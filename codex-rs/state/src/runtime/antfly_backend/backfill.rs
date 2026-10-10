//! Antfly backend for rollout metadata backfill state, backed by the
//! singleton row of the `codex_backfill_state` SQL table (see
//! `codex-antfly/src/schema.rs`, migration 1). Mirrors
//! `state/src/runtime/backfill.rs` exactly: every operation first ensures
//! the row exists, and claiming is a lease-style compare-and-set `UPDATE`.

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::Sql;
use codex_antfly::sql_params;

use super::internal;
use crate::BackfillState;
use crate::BackfillStatus;

async fn ensure_row(antfly: &Arc<Antfly>) -> anyhow::Result<&Sql> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_backfill_state (id, status, last_watermark, last_success_at, updated_at) \
         VALUES (1, $1, NULL, NULL, $2) ON CONFLICT (id) DO NOTHING",
        sql_params![BackfillStatus::Pending.as_str(), Utc::now().timestamp()],
    )
    .await
    .map_err(internal)?;
    Ok(sql)
}

pub(crate) async fn get_backfill_state(antfly: &Arc<Antfly>) -> anyhow::Result<BackfillState> {
    let sql = ensure_row(antfly).await?;
    let row = sql
        .fetch_optional(
            "SELECT status, last_watermark, last_success_at FROM codex_backfill_state WHERE id = 1",
            vec![],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| anyhow::anyhow!("codex_backfill_state singleton row is missing"))?;
    let status = row.string("status").map_err(internal)?;
    let last_success_at = row
        .opt_i64("last_success_at")
        .map_err(internal)?
        .map(|secs| {
            DateTime::<Utc>::from_timestamp(secs, 0)
                .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp: {secs}"))
        })
        .transpose()?;
    let last_watermark = if row.is_null("last_watermark").map_err(internal)? {
        None
    } else {
        Some(super::text_column(&row, "last_watermark").map_err(internal)?)
    };
    Ok(BackfillState {
        status: BackfillStatus::parse(status.as_str())?,
        last_watermark,
        last_success_at,
    })
}

pub(crate) async fn try_claim_backfill(
    antfly: &Arc<Antfly>,
    lease_seconds: i64,
) -> anyhow::Result<bool> {
    let sql = ensure_row(antfly).await?;
    let now = Utc::now().timestamp();
    let lease_cutoff = now.saturating_sub(lease_seconds.max(0));
    let rows_affected = sql
        .execute(
            "UPDATE codex_backfill_state SET status = $1, updated_at = $2 \
             WHERE id = 1 AND status <> $3 AND (status <> $1 OR updated_at <= $4)",
            sql_params![
                BackfillStatus::Running.as_str(),
                now,
                BackfillStatus::Complete.as_str(),
                lease_cutoff,
            ],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected == 1)
}

pub(crate) async fn mark_backfill_running(antfly: &Arc<Antfly>) -> anyhow::Result<()> {
    let sql = ensure_row(antfly).await?;
    sql.execute(
        "UPDATE codex_backfill_state SET status = $1, updated_at = $2 WHERE id = 1",
        sql_params![BackfillStatus::Running.as_str(), Utc::now().timestamp()],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn checkpoint_backfill(
    antfly: &Arc<Antfly>,
    watermark: &str,
) -> anyhow::Result<()> {
    let sql = ensure_row(antfly).await?;
    sql.execute(
        "UPDATE codex_backfill_state SET status = $1, last_watermark = $2, updated_at = $3 \
         WHERE id = 1",
        sql_params![
            BackfillStatus::Running.as_str(),
            watermark,
            Utc::now().timestamp()
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn mark_backfill_complete(
    antfly: &Arc<Antfly>,
    last_watermark: Option<&str>,
) -> anyhow::Result<()> {
    let sql = ensure_row(antfly).await?;
    let now = Utc::now().timestamp();
    sql.execute(
        "UPDATE codex_backfill_state SET status = $1, \
         last_watermark = COALESCE($2, last_watermark), last_success_at = $3, updated_at = $3 \
         WHERE id = 1",
        sql_params![BackfillStatus::Complete.as_str(), last_watermark, now],
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

    #[tokio::test(flavor = "multi_thread")]
    async fn backfill_state_persists_progress_and_completion() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        assert_eq!(
            BackfillState::default(),
            runtime.get_backfill_state().await?
        );

        runtime.mark_backfill_running().await?;
        runtime
            .checkpoint_backfill("sessions/2026/01/27/rollout-a.jsonl")
            .await?;
        let running = runtime.get_backfill_state().await?;
        assert_eq!(BackfillStatus::Running, running.status);
        assert_eq!(
            Some("sessions/2026/01/27/rollout-a.jsonl".to_string()),
            running.last_watermark
        );
        assert_eq!(None, running.last_success_at);

        runtime
            .mark_backfill_complete(Some("sessions/2026/01/28/rollout-b.jsonl"))
            .await?;
        let completed = runtime.get_backfill_state().await?;
        assert_eq!(BackfillStatus::Complete, completed.status);
        assert_eq!(
            Some("sessions/2026/01/28/rollout-b.jsonl".to_string()),
            completed.last_watermark
        );
        assert!(completed.last_success_at.is_some());

        // A completion without a watermark keeps the previous one.
        runtime.mark_backfill_complete(None).await?;
        assert_eq!(
            Some("sessions/2026/01/28/rollout-b.jsonl".to_string()),
            runtime.get_backfill_state().await?.last_watermark
        );
        harness.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_backfill_state_repairs_a_missing_singleton_row() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let sql = harness.antfly.sql().await?;
        sql.execute("DELETE FROM codex_backfill_state WHERE id = 1", vec![])
            .await?;
        assert_eq!(
            BackfillState::default(),
            harness.runtime.get_backfill_state().await?
        );
        let count = sql
            .fetch_optional(
                "SELECT COUNT(*) AS count FROM codex_backfill_state WHERE id = 1",
                vec![],
            )
            .await?
            .expect("count row")
            .i64("count")?;
        assert_eq!(1, count);
        harness.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backfill_claim_is_singleton_until_stale_and_blocked_when_complete()
    -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        assert!(runtime.try_claim_backfill(3600).await?);
        assert!(!runtime.try_claim_backfill(3600).await?);

        harness
            .antfly
            .sql()
            .await?
            .execute(
                "UPDATE codex_backfill_state SET status = $1, updated_at = $2 WHERE id = 1",
                sql_params![
                    BackfillStatus::Running.as_str(),
                    Utc::now().timestamp().saturating_sub(10_000)
                ],
            )
            .await?;
        assert!(runtime.try_claim_backfill(10).await?);

        runtime.mark_backfill_complete(None).await?;
        assert!(!runtime.try_claim_backfill(3600).await?);
        harness.close().await;
        Ok(())
    }
}
