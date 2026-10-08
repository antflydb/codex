//! Antfly backend for `GoalStore`. See `state/src/runtime/goals.rs` for the
//! SQLite implementation this mirrors exactly (semantics are ported 1:1).

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::Write;
use codex_protocol::ThreadId;

use super::internal;
use crate::GoalAccountingMode;
use crate::GoalAccountingOutcome;
use crate::GoalUpdate;
use crate::ThreadGoal;
use crate::ThreadGoalStatus;

fn goal_key(thread_id: ThreadId) -> String {
    format!("st:goal:{thread_id}")
}

fn deferral_key(thread_id: ThreadId) -> String {
    format!("st:goaldef:{thread_id}")
}

async fn get(antfly: &Arc<Antfly>, thread_id: ThreadId) -> anyhow::Result<Option<ThreadGoal>> {
    antfly.get_as(goal_key(thread_id)).await.map_err(internal)
}

async fn put(antfly: &Arc<Antfly>, goal: &ThreadGoal) -> anyhow::Result<()> {
    let doc = serde_json::to_value(goal)?;
    antfly
        .write(vec![Write::put(goal_key(goal.thread_id), doc)])
        .await
        .map_err(internal)
}

pub(crate) async fn get_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    get(antfly, thread_id).await
}

pub(crate) async fn replace_thread_goal_snapshot(
    antfly: &Arc<Antfly>,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    let doc = serde_json::to_value(goal)?;
    antfly
        .write(vec![
            Write::put(goal_key(goal.thread_id), doc),
            Write::put(deferral_key(goal.thread_id), serde_json::json!({})),
        ])
        .await
        .map_err(internal)
}

pub(crate) async fn has_thread_goal_continuation_deferral(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    Ok(antfly
        .get(deferral_key(thread_id))
        .await
        .map_err(internal)?
        .is_some())
}

pub(crate) async fn clear_thread_goal_continuation_deferral(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<()> {
    antfly
        .write(vec![Write::delete(deferral_key(thread_id))])
        .await
        .map_err(internal)
}

fn status_after_budget_limit(
    status: ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> ThreadGoalStatus {
    if status == ThreadGoalStatus::Active
        && token_budget.is_some_and(|budget| tokens_used >= budget)
    {
        ThreadGoalStatus::BudgetLimited
    } else {
        status
    }
}

fn fresh_goal(
    thread_id: ThreadId,
    objective: &str,
    status: ThreadGoalStatus,
    token_budget: Option<i64>,
) -> ThreadGoal {
    let now = Utc::now();
    ThreadGoal {
        thread_id,
        goal_id: uuid::Uuid::new_v4().to_string(),
        objective: objective.to_string(),
        status: status_after_budget_limit(status, /* tokens_used */ 0, token_budget),
        token_budget,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: now,
        updated_at: now,
    }
}

pub(crate) async fn replace_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    objective: &str,
    status: ThreadGoalStatus,
    token_budget: Option<i64>,
) -> anyhow::Result<ThreadGoal> {
    let goal = fresh_goal(thread_id, objective, status, token_budget);
    put(antfly, &goal).await?;
    Ok(goal)
}

pub(crate) async fn insert_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    objective: &str,
    status: ThreadGoalStatus,
    token_budget: Option<i64>,
) -> anyhow::Result<Option<ThreadGoal>> {
    let _guard = antfly.lock().await;
    match get(antfly, thread_id).await? {
        Some(existing) if existing.status != ThreadGoalStatus::Complete => Ok(None),
        _ => {
            let goal = fresh_goal(thread_id, objective, status, token_budget);
            put(antfly, &goal).await?;
            Ok(Some(goal))
        }
    }
}

pub(crate) async fn update_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    update: GoalUpdate,
) -> anyhow::Result<Option<ThreadGoal>> {
    let GoalUpdate {
        objective,
        status,
        token_budget,
        expected_goal_id,
    } = update;

    let _guard = antfly.lock().await;
    let Some(mut goal) = get(antfly, thread_id).await? else {
        return Ok(None);
    };
    if let Some(expected) = &expected_goal_id
        && goal.goal_id != *expected
    {
        return Ok(None);
    }

    match (status, token_budget) {
        (Some(new_status), Some(new_budget)) => {
            if let Some(objective) = objective {
                goal.objective = objective;
            }
            let stays_budget_limited = goal.status == ThreadGoalStatus::BudgetLimited
                && matches!(
                    new_status,
                    ThreadGoalStatus::Paused | ThreadGoalStatus::Blocked
                );
            goal.status = if stays_budget_limited {
                goal.status
            } else if new_status == ThreadGoalStatus::Active
                && new_budget.is_some_and(|budget| goal.tokens_used >= budget)
            {
                ThreadGoalStatus::BudgetLimited
            } else {
                new_status
            };
            goal.token_budget = new_budget;
        }
        (Some(new_status), None) => {
            if let Some(objective) = objective {
                goal.objective = objective;
            }
            let stays_budget_limited = goal.status == ThreadGoalStatus::BudgetLimited
                && matches!(
                    new_status,
                    ThreadGoalStatus::Paused | ThreadGoalStatus::Blocked
                );
            goal.status = if stays_budget_limited {
                goal.status
            } else if new_status == ThreadGoalStatus::Active
                && goal
                    .token_budget
                    .is_some_and(|budget| goal.tokens_used >= budget)
            {
                ThreadGoalStatus::BudgetLimited
            } else {
                new_status
            };
        }
        (None, Some(new_budget)) => {
            if let Some(objective) = objective {
                goal.objective = objective;
            }
            goal.token_budget = new_budget;
            if goal.status == ThreadGoalStatus::Active
                && new_budget.is_some_and(|budget| goal.tokens_used >= budget)
            {
                goal.status = ThreadGoalStatus::BudgetLimited;
            }
        }
        (None, None) => {
            let Some(objective) = objective else {
                // Read-only: nothing to change.
                return Ok(Some(goal));
            };
            goal.objective = objective;
        }
    }
    goal.updated_at = Utc::now();
    put(antfly, &goal).await?;
    Ok(Some(goal))
}

async fn update_active_thread_goal_status(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    status: ThreadGoalStatus,
) -> anyhow::Result<Option<ThreadGoal>> {
    let _guard = antfly.lock().await;
    let Some(mut goal) = get(antfly, thread_id).await? else {
        return Ok(None);
    };
    let eligible = goal.status == ThreadGoalStatus::Active
        || (status == ThreadGoalStatus::UsageLimited
            && goal.status == ThreadGoalStatus::BudgetLimited);
    if !eligible {
        return Ok(None);
    }
    goal.status = status;
    goal.updated_at = Utc::now();
    put(antfly, &goal).await?;
    Ok(Some(goal))
}

pub(crate) async fn pause_active_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    update_active_thread_goal_status(antfly, thread_id, ThreadGoalStatus::Paused).await
}

pub(crate) async fn usage_limit_active_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    update_active_thread_goal_status(antfly, thread_id, ThreadGoalStatus::UsageLimited).await
}

pub(crate) async fn delete_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    let _guard = antfly.lock().await;
    let Some(goal) = get(antfly, thread_id).await? else {
        return Ok(None);
    };
    antfly
        .write(vec![Write::delete(goal_key(thread_id))])
        .await
        .map_err(internal)?;
    Ok(Some(goal))
}

pub(crate) async fn account_thread_goal_usage(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    time_delta_seconds: i64,
    token_delta: i64,
    mode: GoalAccountingMode,
    expected_goal_id: Option<&str>,
) -> anyhow::Result<GoalAccountingOutcome> {
    let time_delta_seconds = time_delta_seconds.max(0);
    let token_delta = token_delta.max(0);
    if time_delta_seconds == 0 && token_delta == 0 {
        return Ok(GoalAccountingOutcome::Unchanged(
            get(antfly, thread_id).await?,
        ));
    }

    let _guard = antfly.lock().await;
    let Some(mut goal) = get(antfly, thread_id).await? else {
        return Ok(GoalAccountingOutcome::Unchanged(None));
    };

    let active_or_stopped = matches!(
        goal.status,
        ThreadGoalStatus::Active
            | ThreadGoalStatus::Paused
            | ThreadGoalStatus::Blocked
            | ThreadGoalStatus::UsageLimited
            | ThreadGoalStatus::BudgetLimited
    );
    let status_eligible = match mode {
        GoalAccountingMode::ActiveStatusOnly => goal.status == ThreadGoalStatus::Active,
        GoalAccountingMode::ActiveOnly => {
            matches!(
                goal.status,
                ThreadGoalStatus::Active | ThreadGoalStatus::BudgetLimited
            )
        }
        GoalAccountingMode::ActiveOrComplete => matches!(
            goal.status,
            ThreadGoalStatus::Active | ThreadGoalStatus::BudgetLimited | ThreadGoalStatus::Complete
        ),
        GoalAccountingMode::ActiveOrStopped => active_or_stopped,
    };
    let budget_limit_eligible = match mode {
        GoalAccountingMode::ActiveStatusOnly
        | GoalAccountingMode::ActiveOnly
        | GoalAccountingMode::ActiveOrComplete => goal.status == ThreadGoalStatus::Active,
        GoalAccountingMode::ActiveOrStopped => active_or_stopped,
    };
    let goal_id_matches = expected_goal_id.is_none_or(|expected| goal.goal_id == expected);

    if !status_eligible || !goal_id_matches {
        return Ok(GoalAccountingOutcome::Unchanged(Some(goal)));
    }

    let new_tokens_used = goal.tokens_used + token_delta;
    if budget_limit_eligible
        && goal
            .token_budget
            .is_some_and(|budget| new_tokens_used >= budget)
    {
        goal.status = ThreadGoalStatus::BudgetLimited;
    }
    goal.time_used_seconds += time_delta_seconds;
    goal.tokens_used = new_tokens_used;
    goal.updated_at = Utc::now();
    put(antfly, &goal).await?;
    Ok(GoalAccountingOutcome::Updated(goal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GoalStore;
    use pretty_assertions::assert_eq;

    fn thread_id() -> ThreadId {
        ThreadId::from_string("00000000-0000-0000-0000-000000000123").expect("valid thread id")
    }

    async fn test_store() -> (GoalStore, Arc<Antfly>, std::path::PathBuf) {
        let dir = crate::runtime::test_support::unique_temp_dir();
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let mut config = codex_antfly::AntflyConfig::embedded(dir.join("codex.aflite"));
        config.embedder = None;
        let antfly = Arc::new(codex_antfly::Antfly::new(config));
        (GoalStore::new_antfly(Arc::clone(&antfly)), antfly, dir)
    }

    /// Closes the database (waiting for background work) before removing
    /// its directory.
    async fn cleanup(antfly: &Antfly, dir: std::path::PathBuf) {
        antfly.close().await.expect("close antfly");
        let _ = tokio::fs::remove_dir_all(dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replace_update_and_get_thread_goal() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();

        let goal = store
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                ThreadGoalStatus::Active,
                Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");
        assert_eq!(
            Some(goal.clone()),
            store.get_thread_goal(thread_id).await.unwrap()
        );

        let updated = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Paused),
                    token_budget: Some(Some(200_000)),
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let expected = ThreadGoal {
            status: ThreadGoalStatus::Paused,
            token_budget: Some(200_000),
            updated_at: updated.updated_at,
            ..goal.clone()
        };
        assert_eq!(expected, updated);

        let replaced = store
            .replace_thread_goal(
                thread_id,
                "ship the new result",
                ThreadGoalStatus::Active,
                None,
            )
            .await
            .expect("goal replacement should succeed");
        assert_eq!("ship the new result", replaced.objective);
        assert_eq!(ThreadGoalStatus::Active, replaced.status);
        assert_eq!(None, replaced.token_budget);
        assert_eq!(0, replaced.tokens_used);
        assert_eq!(0, replaced.time_used_seconds);

        assert_eq!(
            Some(replaced),
            store.delete_thread_goal(thread_id).await.unwrap()
        );
        assert_eq!(None, store.get_thread_goal(thread_id).await.unwrap());
        assert_eq!(None, store.delete_thread_goal(thread_id).await.unwrap());

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replace_thread_goal_applies_budget_limit_immediately() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();

        let replaced = store
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                ThreadGoalStatus::Active,
                Some(0),
            )
            .await
            .expect("goal replacement should succeed");

        assert_eq!(ThreadGoalStatus::BudgetLimited, replaced.status);
        assert_eq!(Some(0), replaced.token_budget);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_thread_goal_does_not_replace_existing_goal() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();

        let inserted = store
            .insert_thread_goal(
                thread_id,
                "optimize the benchmark",
                ThreadGoalStatus::Active,
                Some(100_000),
            )
            .await
            .expect("goal insertion should succeed")
            .expect("goal should be inserted");

        let duplicate = store
            .insert_thread_goal(
                thread_id,
                "replace the benchmark",
                ThreadGoalStatus::Active,
                Some(200_000),
            )
            .await
            .expect("duplicate insert should not fail");

        assert_eq!(None, duplicate);
        assert_eq!(
            Some(inserted),
            store.get_thread_goal(thread_id).await.unwrap()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_thread_goal_replaces_a_complete_goal() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();

        store
            .replace_thread_goal(thread_id, "first", ThreadGoalStatus::Complete, None)
            .await
            .expect("goal replacement should succeed");

        let inserted = store
            .insert_thread_goal(thread_id, "second", ThreadGoalStatus::Active, None)
            .await
            .expect("goal insertion should succeed")
            .expect("goal should replace a completed goal");
        assert_eq!("second", inserted.objective);
        assert_eq!(ThreadGoalStatus::Active, inserted.status);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn update_thread_goal_ignores_replaced_goal_version() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();

        let original = store
            .replace_thread_goal(
                thread_id,
                "old objective",
                ThreadGoalStatus::Active,
                Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let replacement = store
            .replace_thread_goal(
                thread_id,
                "new objective",
                ThreadGoalStatus::Active,
                Some(10),
            )
            .await
            .expect("goal replacement should succeed");

        let stale_update = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(original.goal_id),
                },
            )
            .await
            .expect("goal update should succeed");
        assert_eq!(None, stale_update);
        assert_eq!(
            Some(replacement.clone()),
            store
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );

        let fresh_update = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(replacement.goal_id),
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("fresh update should match the replacement goal");
        assert_eq!(ThreadGoalStatus::Complete, fresh_update.status);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pause_active_thread_goal_does_not_clobber_terminal_status() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        let goal = store
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                ThreadGoalStatus::Active,
                Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");

        let paused = store
            .pause_active_thread_goal(thread_id)
            .await
            .expect("active pause should succeed")
            .expect("active goal should be paused");
        let expected = ThreadGoal {
            status: ThreadGoalStatus::Paused,
            updated_at: paused.updated_at,
            ..goal
        };
        assert_eq!(expected, paused);

        let complete = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let pause_result = store
            .pause_active_thread_goal(thread_id)
            .await
            .expect("terminal pause attempt should succeed");
        assert_eq!(None, pause_result);
        assert_eq!(
            Some(complete),
            store
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn usage_limit_active_thread_goal_updates_active_or_budget_limited_goals() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        let goal = store
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                ThreadGoalStatus::Active,
                None,
            )
            .await
            .expect("goal replacement should succeed");

        let usage_limited = store
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("usage limiting should succeed")
            .expect("active goal should become usage limited");
        let expected = ThreadGoal {
            status: ThreadGoalStatus::UsageLimited,
            updated_at: usage_limited.updated_at,
            ..goal
        };
        assert_eq!(expected, usage_limited);

        let second_update = store
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("repeated usage limiting should succeed");
        assert_eq!(None, second_update);

        let budget_limited = store
            .replace_thread_goal(
                thread_id,
                "keep the usage failure visible",
                ThreadGoalStatus::BudgetLimited,
                Some(1),
            )
            .await
            .expect("goal replacement should succeed");
        let usage_limited = store
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("usage limiting should succeed")
            .expect("budget-limited goal should become usage limited");
        let expected = ThreadGoal {
            status: ThreadGoalStatus::UsageLimited,
            updated_at: usage_limited.updated_at,
            ..budget_limited
        };
        assert_eq!(expected, usage_limited);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn usage_accounting_updates_active_goals_and_accounts_budget_limited_in_flight_usage() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                ThreadGoalStatus::Active,
                Some(20),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = store
            .account_thread_goal_usage(thread_id, 7, 5, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("active goal should be updated");
        };
        assert_eq!(ThreadGoalStatus::Active, goal.status);
        assert_eq!(5, goal.tokens_used);
        assert_eq!(7, goal.time_used_seconds);

        let outcome = store
            .account_thread_goal_usage(thread_id, 3, 15, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("budget crossing should update the goal");
        };
        assert_eq!(ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(20, goal.tokens_used);
        assert_eq!(10, goal.time_used_seconds);

        let outcome = store
            .account_thread_goal_usage(thread_id, 5, 5, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("budget-limited goal should still account in-flight active usage");
        };
        assert_eq!(ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(25, goal.tokens_used);
        assert_eq!(15, goal.time_used_seconds);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn active_status_only_usage_accounting_does_not_update_budget_limited_goals() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "stay stopped",
                ThreadGoalStatus::BudgetLimited,
                Some(20),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = store
            .account_thread_goal_usage(thread_id, 5, 5, GoalAccountingMode::ActiveStatusOnly, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = outcome else {
            panic!("budget-limited goal should not be updated");
        };
        assert_eq!(ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(0, goal.tokens_used);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stopped_usage_accounting_promotes_paused_goal_over_budget() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "stop before overrun",
                ThreadGoalStatus::Active,
                Some(20),
            )
            .await
            .expect("goal replacement should succeed");
        store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed");

        let outcome = store
            .account_thread_goal_usage(thread_id, 3, 25, GoalAccountingMode::ActiveOrStopped, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("stopped goal should account final usage");
        };
        assert_eq!(ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(25, goal.tokens_used);
        assert_eq!(3, goal.time_used_seconds);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn usage_accounting_can_finalize_completed_goal_for_completing_turn() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "finish the report",
                ThreadGoalStatus::Complete,
                Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");

        let active_only = store
            .account_thread_goal_usage(thread_id, 30, 200, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = active_only else {
            panic!("completed goal should not be updated by active-only accounting");
        };
        assert_eq!(ThreadGoalStatus::Complete, goal.status);
        assert_eq!(0, goal.tokens_used);

        let completing_turn = store
            .account_thread_goal_usage(
                thread_id,
                30,
                200,
                GoalAccountingMode::ActiveOrComplete,
                None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = completing_turn else {
            panic!("completed goal should be updated for final accounting");
        };
        assert_eq!(ThreadGoalStatus::Complete, goal.status);
        assert_eq!(200, goal.tokens_used);
        assert_eq!(30, goal.time_used_seconds);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn budget_updates_immediately_stop_active_goals_already_over_budget() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                ThreadGoalStatus::Active,
                Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        store
            .account_thread_goal_usage(thread_id, 1, 50, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");

        let lowered = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: None,
                    token_budget: Some(Some(40)),
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(ThreadGoalStatus::BudgetLimited, lowered.status);
        assert_eq!(Some(40), lowered.token_budget);
        assert_eq!(50, lowered.tokens_used);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn activating_goal_already_over_budget_keeps_it_budget_limited() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                ThreadGoalStatus::Active,
                Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        store
            .account_thread_goal_usage(thread_id, 1, 50, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");

        let reactivated = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: Some("stay within budget, with clearer wording".to_string()),
                    status: Some(ThreadGoalStatus::Active),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(ThreadGoalStatus::BudgetLimited, reactivated.status);
        assert_eq!(Some(40), reactivated.token_budget);
        assert_eq!(50, reactivated.tokens_used);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pausing_and_blocking_budget_limited_goal_preserves_terminal_status() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                ThreadGoalStatus::Active,
                Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        let outcome = store
            .account_thread_goal_usage(thread_id, 1, 50, GoalAccountingMode::ActiveOnly, None)
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(budget_limited) = outcome else {
            panic!("budget crossing should update the goal");
        };

        let paused = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        assert_eq!(ThreadGoalStatus::BudgetLimited, paused.status);
        assert_eq!(50, paused.tokens_used);

        let blocked = store
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(ThreadGoalStatus::Blocked),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let expected = ThreadGoal {
            updated_at: blocked.updated_at,
            ..budget_limited
        };
        assert_eq!(expected, blocked);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn continuation_deferral_roundtrips() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();

        assert!(
            !store
                .has_thread_goal_continuation_deferral(thread_id)
                .await
                .unwrap()
        );

        let goal = store
            .replace_thread_goal(thread_id, "track deferral", ThreadGoalStatus::Active, None)
            .await
            .expect("goal replacement should succeed");
        store
            .replace_thread_goal_snapshot(&goal)
            .await
            .expect("snapshot replacement should succeed");
        assert!(
            store
                .has_thread_goal_continuation_deferral(thread_id)
                .await
                .unwrap()
        );

        store
            .clear_thread_goal_continuation_deferral(thread_id)
            .await
            .expect("deferral clear should succeed");
        assert!(
            !store
                .has_thread_goal_continuation_deferral(thread_id)
                .await
                .unwrap()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn usage_accounting_adds_concurrent_token_deltas() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = thread_id();
        store
            .replace_thread_goal(
                thread_id,
                "count every token",
                ThreadGoalStatus::Active,
                Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");

        let first =
            store.account_thread_goal_usage(thread_id, 4, 40, GoalAccountingMode::ActiveOnly, None);
        let second =
            store.account_thread_goal_usage(thread_id, 6, 60, GoalAccountingMode::ActiveOnly, None);
        let (first, second) = tokio::join!(first, second);
        first.expect("first usage accounting should succeed");
        second.expect("second usage accounting should succeed");

        let goal = store
            .get_thread_goal(thread_id)
            .await
            .expect("goal read should succeed")
            .expect("goal should exist");
        assert_eq!(100, goal.tokens_used);
        assert_eq!(10, goal.time_used_seconds);

        cleanup(&antfly, dir).await;
    }
}
