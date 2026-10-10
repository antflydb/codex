//! Antfly backend for generic rollout-migration progress, backed by the
//! `codex_rollout_migration_state` and `codex_rollout_migration_skipped_rollouts`
//! SQL tables (see `codex-antfly/src/schema.rs`, migration 1). Mirrors
//! `state/src/runtime/rollout_migration.rs` exactly, including the
//! never-move-backward cursor upsert.

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;

use super::internal;
use crate::RolloutMigrationCursor;
use crate::RolloutMigrationSkippedRollout;
use crate::RolloutMigrationState;

fn state_from_row(row: &SqlRow) -> anyhow::Result<RolloutMigrationState> {
    let thread_created_at = row
        .opt_i64("last_checked_thread_created_at")
        .map_err(internal)?;
    let thread_id = row.opt_string("last_checked_thread_id").map_err(internal)?;
    let last_checked_thread = match (thread_created_at, thread_id) {
        (Some(thread_created_at), Some(thread_id)) => Some(RolloutMigrationCursor {
            thread_created_at,
            thread_id,
        }),
        (None, None) => None,
        _ => {
            return Err(anyhow::anyhow!(
                "rollout migration state has incomplete last checked thread"
            ));
        }
    };
    Ok(RolloutMigrationState {
        last_checked_thread,
    })
}

fn skipped_from_row(row: &SqlRow) -> anyhow::Result<RolloutMigrationSkippedRollout> {
    Ok(RolloutMigrationSkippedRollout {
        rollout_path: row.string("rollout_path").map_err(internal)?,
        rollout_size_bytes: row.i64("rollout_size_bytes").map_err(internal)?,
        rollout_modified_at_ns: row.i64("rollout_modified_at_ns").map_err(internal)?,
        skip_reason: super::text_column(row, "skip_reason").map_err(internal)?,
    })
}

pub(crate) async fn get_rollout_migration_state(
    antfly: &Arc<Antfly>,
    migration_id: &str,
) -> anyhow::Result<Option<RolloutMigrationState>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT last_checked_thread_created_at, last_checked_thread_id \
             FROM codex_rollout_migration_state WHERE migration_id = $1",
            sql_params![migration_id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| state_from_row(&row)).transpose()
}

pub(crate) async fn advance_rollout_migration_state(
    antfly: &Arc<Antfly>,
    migration_id: &str,
    last_checked_thread: Option<&RolloutMigrationCursor>,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let (thread_created_at, thread_id) = last_checked_thread.map_or((None, None), |cursor| {
        (
            Some(cursor.thread_created_at),
            Some(cursor.thread_id.as_str()),
        )
    });
    sql.execute(
        "INSERT INTO codex_rollout_migration_state (
            migration_id,
            last_checked_thread_created_at,
            last_checked_thread_id,
            updated_at
         ) VALUES ($1, $2, $3, $4)
         ON CONFLICT (migration_id) DO UPDATE SET
            last_checked_thread_created_at = excluded.last_checked_thread_created_at,
            last_checked_thread_id = excluded.last_checked_thread_id,
            updated_at = excluded.updated_at
         WHERE excluded.last_checked_thread_created_at IS NOT NULL
           AND (
             codex_rollout_migration_state.last_checked_thread_created_at IS NULL
             OR excluded.last_checked_thread_created_at
                 > codex_rollout_migration_state.last_checked_thread_created_at
             OR (
                 excluded.last_checked_thread_created_at
                     = codex_rollout_migration_state.last_checked_thread_created_at
                 AND excluded.last_checked_thread_id
                     > codex_rollout_migration_state.last_checked_thread_id
             )
           )",
        sql_params![
            migration_id,
            thread_created_at,
            thread_id,
            Utc::now().timestamp()
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn list_rollout_migration_skipped_rollouts(
    antfly: &Arc<Antfly>,
    migration_id: &str,
) -> anyhow::Result<Vec<RolloutMigrationSkippedRollout>> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.fetch_all(
        "SELECT rollout_path, rollout_size_bytes, rollout_modified_at_ns, skip_reason \
         FROM codex_rollout_migration_skipped_rollouts WHERE migration_id = $1",
        sql_params![migration_id],
    )
    .await
    .map_err(internal)?
    .iter()
    .map(skipped_from_row)
    .collect()
}

pub(crate) async fn record_rollout_migration_skip(
    antfly: &Arc<Antfly>,
    migration_id: &str,
    skipped_rollout: &RolloutMigrationSkippedRollout,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_rollout_migration_skipped_rollouts (
            migration_id,
            rollout_path,
            rollout_size_bytes,
            rollout_modified_at_ns,
            skip_reason,
            skipped_at
         ) VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (migration_id, rollout_path) DO UPDATE SET
            rollout_size_bytes = excluded.rollout_size_bytes,
            rollout_modified_at_ns = excluded.rollout_modified_at_ns,
            skip_reason = excluded.skip_reason,
            skipped_at = excluded.skipped_at",
        sql_params![
            migration_id,
            skipped_rollout.rollout_path.as_str(),
            skipped_rollout.rollout_size_bytes,
            skipped_rollout.rollout_modified_at_ns,
            skipped_rollout.skip_reason.as_str(),
            Utc::now().timestamp(),
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn remove_rollout_migration_skip(
    antfly: &Arc<Antfly>,
    migration_id: &str,
    rollout_path: &str,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "DELETE FROM codex_rollout_migration_skipped_rollouts \
         WHERE migration_id = $1 AND rollout_path = $2",
        sql_params![migration_id, rollout_path],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::sql_test_support::AntflyRuntime;
    use super::*;
    use pretty_assertions::assert_eq;

    fn cursor(thread_created_at: i64, thread_id: &str) -> RolloutMigrationCursor {
        RolloutMigrationCursor {
            thread_created_at,
            thread_id: thread_id.to_string(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_cursor_only_moves_forward() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        assert_eq!(None, runtime.get_rollout_migration_state("m").await?);

        runtime.advance_rollout_migration_state("m", None).await?;
        assert_eq!(
            Some(RolloutMigrationState {
                last_checked_thread: None
            }),
            runtime.get_rollout_migration_state("m").await?
        );

        runtime
            .advance_rollout_migration_state("m", Some(&cursor(10, "b")))
            .await?;
        // Older, equal-time-smaller-id, and empty cursors never move it back.
        runtime
            .advance_rollout_migration_state("m", Some(&cursor(5, "z")))
            .await?;
        runtime
            .advance_rollout_migration_state("m", Some(&cursor(10, "a")))
            .await?;
        runtime.advance_rollout_migration_state("m", None).await?;
        assert_eq!(
            Some(cursor(10, "b")),
            runtime
                .get_rollout_migration_state("m")
                .await?
                .and_then(|state| state.last_checked_thread)
        );

        runtime
            .advance_rollout_migration_state("m", Some(&cursor(10, "c")))
            .await?;
        assert_eq!(
            Some(cursor(10, "c")),
            runtime
                .get_rollout_migration_state("m")
                .await?
                .and_then(|state| state.last_checked_thread)
        );
        assert_eq!(None, runtime.get_rollout_migration_state("other").await?);
        harness.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn skipped_rollouts_upsert_list_and_remove() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        let skipped = RolloutMigrationSkippedRollout {
            rollout_path: "/sessions/a.jsonl".to_string(),
            rollout_size_bytes: 10,
            rollout_modified_at_ns: 100,
            skip_reason: "unreadable".to_string(),
        };
        runtime.record_rollout_migration_skip("m", &skipped).await?;
        let changed = RolloutMigrationSkippedRollout {
            rollout_size_bytes: 20,
            rollout_modified_at_ns: 200,
            skip_reason: "{\"error\":\"still unreadable\"}".to_string(),
            ..skipped.clone()
        };
        runtime.record_rollout_migration_skip("m", &changed).await?;
        assert_eq!(
            vec![changed],
            runtime.list_rollout_migration_skipped_rollouts("m").await?
        );
        assert!(
            runtime
                .list_rollout_migration_skipped_rollouts("other")
                .await?
                .is_empty()
        );

        runtime
            .remove_rollout_migration_skip("m", &skipped.rollout_path)
            .await?;
        assert!(
            runtime
                .list_rollout_migration_skipped_rollouts("m")
                .await?
                .is_empty()
        );
        harness.close().await;
        Ok(())
    }
}
