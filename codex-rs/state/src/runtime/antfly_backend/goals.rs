//! Antfly backend for `GoalStore`, backed by the `codex_thread_goals` and
//! `codex_thread_goal_continuation_deferrals` SQL tables (see
//! `codex-antfly/src/schema.rs`, migration 1). See `state/src/runtime/goals.rs`
//! for the SQLite implementation this mirrors exactly (semantics, including
//! the exact SQL, are ported 1:1 with `?` placeholders rewritten to `$n` and
//! `thread_goals`/`thread_goal_continuation_deferrals` rewritten to
//! `codex_thread_goals`/`codex_thread_goal_continuation_deferrals`). Every
//! mutation here is a single atomic SQL statement (`UPDATE ... RETURNING` or
//! `INSERT ... ON CONFLICT ... RETURNING`), so unlike the old key-prefix
//! backend this needs no process-wide lock: Antfly's READ COMMITTED
//! transactions make each statement itself the atomic compare-and-set.

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;

use super::internal;
use crate::GoalAccountingMode;
use crate::GoalAccountingOutcome;
use crate::GoalUpdate;
use crate::ThreadGoal;
use crate::ThreadGoalStatus;
use crate::model::datetime_to_epoch_millis;
use crate::model::epoch_millis_to_datetime;

fn thread_goal_from_row(row: &SqlRow) -> anyhow::Result<ThreadGoal> {
    let thread_id = ThreadId::try_from(row.string("thread_id").map_err(internal)?)?;
    let status_str = row.string("status").map_err(internal)?;
    Ok(ThreadGoal {
        thread_id,
        goal_id: row.string("goal_id").map_err(internal)?,
        objective: row.string("objective").map_err(internal)?,
        status: ThreadGoalStatus::try_from(status_str.as_str())?,
        token_budget: row.opt_i64("token_budget").map_err(internal)?,
        tokens_used: row.i64("tokens_used").map_err(internal)?,
        time_used_seconds: row.i64("time_used_seconds").map_err(internal)?,
        created_at: epoch_millis_to_datetime(row.i64("created_at_ms").map_err(internal)?)?,
        updated_at: epoch_millis_to_datetime(row.i64("updated_at_ms").map_err(internal)?)?,
    })
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

pub(crate) async fn get_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            r#"
SELECT
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
FROM codex_thread_goals
WHERE thread_id = $1
            "#,
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    row.map(|row| thread_goal_from_row(&row)).transpose()
}

pub(crate) async fn replace_thread_goal_snapshot(
    antfly: &Arc<Antfly>,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let result = async {
        tx.execute(
            r#"
INSERT INTO codex_thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = excluded.tokens_used,
    time_used_seconds = excluded.time_used_seconds,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
            "#,
            sql_params![
                goal.thread_id.to_string(),
                goal.goal_id.clone(),
                goal.objective.clone(),
                goal.status.as_str(),
                goal.token_budget,
                goal.tokens_used,
                goal.time_used_seconds,
                datetime_to_epoch_millis(goal.created_at),
                datetime_to_epoch_millis(goal.updated_at),
            ],
        )
        .await?;

        tx.execute(
            r#"
INSERT INTO codex_thread_goal_continuation_deferrals (thread_id)
VALUES ($1)
ON CONFLICT (thread_id) DO NOTHING
            "#,
            sql_params![goal.thread_id.to_string()],
        )
        .await?;
        Ok::<_, codex_antfly::AntflyError>(())
    }
    .await;

    match result {
        Ok(()) => tx.commit().await.map_err(internal),
        Err(err) => {
            let _ = tx.rollback().await;
            Err(internal(err))
        }
    }
}

pub(crate) async fn has_thread_goal_continuation_deferral(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            r#"
SELECT EXISTS(
    SELECT 1
    FROM codex_thread_goal_continuation_deferrals
    WHERE thread_id = $1
) AS present
            "#,
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    match row {
        Some(row) => row.bool("present").map_err(internal),
        None => Ok(false),
    }
}

pub(crate) async fn clear_thread_goal_continuation_deferral(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "DELETE FROM codex_thread_goal_continuation_deferrals WHERE thread_id = $1",
        sql_params![thread_id.to_string()],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn replace_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    objective: &str,
    status: ThreadGoalStatus,
    token_budget: Option<i64>,
) -> anyhow::Result<ThreadGoal> {
    let sql = antfly.sql().await.map_err(internal)?;
    let goal_id = uuid::Uuid::new_v4().to_string();
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
    let row = sql
        .fetch_optional(
            r#"
INSERT INTO codex_thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES ($1, $2, $3, $4, $5, 0, 0, $6, $7)
ON CONFLICT (thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
            sql_params![
                thread_id.to_string(),
                goal_id,
                objective,
                status.as_str(),
                token_budget,
                now_ms,
                now_ms,
            ],
        )
        .await
        .map_err(internal)?;
    let row = row.ok_or_else(|| anyhow::anyhow!("replace_thread_goal: no row returned"))?;
    thread_goal_from_row(&row)
}

pub(crate) async fn insert_thread_goal(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    objective: &str,
    status: ThreadGoalStatus,
    token_budget: Option<i64>,
) -> anyhow::Result<Option<ThreadGoal>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let goal_id = uuid::Uuid::new_v4().to_string();
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
    let row = sql
        .fetch_optional(
            r#"
INSERT INTO codex_thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES ($1, $2, $3, $4, $5, 0, 0, $6, $7)
ON CONFLICT (thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
WHERE codex_thread_goals.status = 'complete'
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
            sql_params![
                thread_id.to_string(),
                goal_id,
                objective,
                status.as_str(),
                token_budget,
                now_ms,
                now_ms,
            ],
        )
        .await
        .map_err(internal)?;
    // The WHERE clause only gates the ON CONFLICT UPDATE action: a fresh
    // insert (no existing row) always returns the new row, and an existing
    // non-complete goal leaves the WHERE false, so RETURNING yields nothing.
    row.map(|row| thread_goal_from_row(&row)).transpose()
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
    let sql = antfly.sql().await.map_err(internal)?;
    let objective = objective.as_deref();
    let expected_goal_id = expected_goal_id.as_deref();
    let now_ms = datetime_to_epoch_millis(Utc::now());

    let rows_affected = match (status, token_budget) {
        (Some(status), Some(token_budget)) => sql
            .execute(
                r#"
UPDATE codex_thread_goals
SET
    objective = COALESCE($1, objective),
    status = CASE
        WHEN status = $2 AND $3 IN ($4, $5) THEN status
        WHEN $3 = 'active' AND $6::bigint IS NOT NULL AND tokens_used >= $6 THEN $7
        ELSE $3
    END,
    token_budget = $6,
    updated_at_ms = $8
WHERE thread_id = $9
  AND ($10::text IS NULL OR goal_id = $10)
                "#,
                sql_params![
                    objective,
                    ThreadGoalStatus::BudgetLimited.as_str(),
                    status.as_str(),
                    ThreadGoalStatus::Paused.as_str(),
                    ThreadGoalStatus::Blocked.as_str(),
                    token_budget,
                    ThreadGoalStatus::BudgetLimited.as_str(),
                    now_ms,
                    thread_id.to_string(),
                    expected_goal_id,
                ],
            )
            .await
            .map_err(internal)?,
        (Some(status), None) => sql
            .execute(
                r#"
UPDATE codex_thread_goals
SET
    objective = COALESCE($1, objective),
    status = CASE
        WHEN status = $2 AND $3 IN ($4, $5) THEN status
        WHEN $3 = 'active' AND token_budget IS NOT NULL AND tokens_used >= token_budget THEN $6
        ELSE $3
    END,
    updated_at_ms = $7
WHERE thread_id = $8
  AND ($9::text IS NULL OR goal_id = $9)
                "#,
                sql_params![
                    objective,
                    ThreadGoalStatus::BudgetLimited.as_str(),
                    status.as_str(),
                    ThreadGoalStatus::Paused.as_str(),
                    ThreadGoalStatus::Blocked.as_str(),
                    ThreadGoalStatus::BudgetLimited.as_str(),
                    now_ms,
                    thread_id.to_string(),
                    expected_goal_id,
                ],
            )
            .await
            .map_err(internal)?,
        (None, Some(token_budget)) => sql
            .execute(
                r#"
UPDATE codex_thread_goals
SET
    objective = COALESCE($1, objective),
    token_budget = $2,
    status = CASE
        WHEN status = 'active' AND $2::bigint IS NOT NULL AND tokens_used >= $2 THEN $3
        ELSE status
    END,
    updated_at_ms = $4
WHERE thread_id = $5
  AND ($6::text IS NULL OR goal_id = $6)
                "#,
                sql_params![
                    objective,
                    token_budget,
                    ThreadGoalStatus::BudgetLimited.as_str(),
                    now_ms,
                    thread_id.to_string(),
                    expected_goal_id,
                ],
            )
            .await
            .map_err(internal)?,
        (None, None) => {
            if let Some(objective) = objective {
                sql.execute(
                    r#"
UPDATE codex_thread_goals
SET
    objective = $1,
    updated_at_ms = $2
WHERE thread_id = $3
  AND ($4::text IS NULL OR goal_id = $4)
                    "#,
                    sql_params![objective, now_ms, thread_id.to_string(), expected_goal_id,],
                )
                .await
                .map_err(internal)?
            } else {
                let goal = get_thread_goal(antfly, thread_id).await?;
                return Ok(match (goal, expected_goal_id) {
                    (Some(goal), Some(expected_goal_id)) if goal.goal_id != expected_goal_id => {
                        None
                    }
                    (goal, _) => goal,
                });
            }
        }
    };

    if rows_affected == 0 {
        return Ok(None);
    }

    get_thread_goal(antfly, thread_id).await
}

async fn update_active_thread_goal_status(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    status: ThreadGoalStatus,
) -> anyhow::Result<Option<ThreadGoal>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let rows_affected = sql
        .execute(
            r#"
UPDATE codex_thread_goals
SET
    status = $1,
    updated_at_ms = $2
WHERE thread_id = $3
  AND (
      status = 'active'
      OR (
          $1 = 'usage_limited'
          AND status = 'budget_limited'
      )
  )
            "#,
            sql_params![status.as_str(), now_ms, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;

    if rows_affected == 0 {
        return Ok(None);
    }

    get_thread_goal(antfly, thread_id).await
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
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            r#"
DELETE FROM codex_thread_goals
WHERE thread_id = $1
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    row.map(|row| thread_goal_from_row(&row)).transpose()
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
            get_thread_goal(antfly, thread_id).await?,
        ));
    }

    let sql = antfly.sql().await.map_err(internal)?;
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let active_or_stopped_status_filter =
        "status IN ('active', 'paused', 'blocked', 'usage_limited', 'budget_limited')";
    let status_filter = match mode {
        GoalAccountingMode::ActiveStatusOnly => "status = 'active'",
        GoalAccountingMode::ActiveOnly => "status IN ('active', 'budget_limited')",
        GoalAccountingMode::ActiveOrComplete => {
            "status IN ('active', 'budget_limited', 'complete')"
        }
        GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
    };
    let budget_limit_status_filter = match mode {
        GoalAccountingMode::ActiveStatusOnly
        | GoalAccountingMode::ActiveOnly
        | GoalAccountingMode::ActiveOrComplete => "status = 'active'",
        GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
    };

    let mut statement = format!(
        r#"
UPDATE codex_thread_goals
SET
    time_used_seconds = time_used_seconds + $1,
    tokens_used = tokens_used + $2,
    status = CASE
        WHEN {budget_limit_status_filter}
            AND token_budget IS NOT NULL
            AND tokens_used + $2 >= token_budget
        THEN $3
        ELSE status
    END,
    updated_at_ms = $4
WHERE thread_id = $5
  AND {status_filter}
        "#
    );
    let mut params = sql_params![
        time_delta_seconds,
        token_delta,
        ThreadGoalStatus::BudgetLimited.as_str(),
        now_ms,
        thread_id.to_string(),
    ];
    if let Some(expected_goal_id) = expected_goal_id {
        statement.push_str(" AND goal_id = $6");
        params.push(expected_goal_id.into());
    }
    statement.push_str(
        r#"
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
        "#,
    );

    let row = sql
        .fetch_optional(&statement, params)
        .await
        .map_err(internal)?;

    let Some(row) = row else {
        return Ok(GoalAccountingOutcome::Unchanged(
            get_thread_goal(antfly, thread_id).await?,
        ));
    };

    let updated = thread_goal_from_row(&row)?;
    Ok(GoalAccountingOutcome::Updated(updated))
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
