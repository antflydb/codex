//! Antfly backend for guardian review feedback, backed by the
//! `codex_guardian_review_feedback` SQL table (see `codex-antfly/src/schema.rs`,
//! migration 1). Mirrors `state/src/runtime/guardian_feedback.rs` exactly:
//! the insert and both evictions (per-thread, then global by count and
//! bytes) run in one transaction, and the foreign key on `codex_threads`
//! cascades deletes and rejects captures for deleted threads.
//!
//! Antfly SQL has no BLOB type, so `record` holds the base64 of the opaque
//! bytes and `record_bytes` their raw length, which the global byte budget
//! sums exactly as SQLite summed `length(record)` over the BLOB.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;

use super::internal;
use crate::GuardianReviewRecord;
use crate::MAX_GUARDIAN_REVIEW_BYTES;
use crate::MAX_GUARDIAN_REVIEW_RECORDS;
use crate::MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD;

fn record_from_row(row: &SqlRow) -> anyhow::Result<GuardianReviewRecord> {
    Ok(GuardianReviewRecord {
        id: row.string("id").map_err(internal)?,
        thread_id: ThreadId::from_string(&row.string("thread_id").map_err(internal)?)?,
        record: STANDARD.decode(super::text_column(row, "record").map_err(internal)?)?,
    })
}

pub(crate) async fn record_guardian_review_failure(
    antfly: &Arc<Antfly>,
    record: &GuardianReviewRecord,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        record.record.len() < MAX_GUARDIAN_REVIEW_BYTES,
        "Guardian feedback record exceeds its size limit"
    );
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let result = async {
        tx.execute(
            "INSERT INTO codex_guardian_review_feedback (id, thread_id, record, record_bytes) \
             VALUES ($1, $2, $3, $4)",
            sql_params![
                record.id.clone(),
                record.thread_id.to_string(),
                STANDARD.encode(&record.record),
                record.record.len() as i64,
            ],
        )
        .await?;
        tx.execute(
            "DELETE FROM codex_guardian_review_feedback WHERE id IN \
             (SELECT id FROM codex_guardian_review_feedback WHERE thread_id = $1 \
              ORDER BY id DESC OFFSET $2)",
            sql_params![
                record.thread_id.to_string(),
                MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD as i64
            ],
        )
        .await?;
        tx.execute(
            "DELETE FROM codex_guardian_review_feedback WHERE id IN \
             (SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS n, \
              SUM(record_bytes + 1) OVER (ORDER BY id DESC) AS bytes \
              FROM codex_guardian_review_feedback) AS ranked WHERE n > $1 OR bytes > $2)",
            sql_params![
                MAX_GUARDIAN_REVIEW_RECORDS as i64,
                MAX_GUARDIAN_REVIEW_BYTES as i64
            ],
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

pub(crate) async fn list_guardian_review_records(
    antfly: &Arc<Antfly>,
) -> anyhow::Result<Vec<GuardianReviewRecord>> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.fetch_all(
        "SELECT id, thread_id, record FROM codex_guardian_review_feedback ORDER BY id",
        vec![],
    )
    .await
    .map_err(internal)?
    .iter()
    .map(record_from_row)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::super::sql_test_support::AntflyRuntime;
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test(flavor = "multi_thread")]
    async fn retention_bounds_count_per_thread_and_bytes() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let state = &harness.runtime;
        let mut ids = Vec::new();
        for _ in 0..=MAX_GUARDIAN_REVIEW_RECORDS {
            let id = ThreadId::new();
            harness.insert_thread(id).await;
            let record = GuardianReviewRecord::new(id, b"{}".to_vec());
            state.record_guardian_review_failure(&record).await?;
            ids.push(id);
        }
        assert_eq!(
            state
                .list_guardian_review_records()
                .await?
                .iter()
                .map(|record| record.thread_id)
                .collect::<Vec<_>>(),
            ids[1..],
        );
        let id = ids[0];
        for index in 0..12 {
            state
                .record_guardian_review_failure(&GuardianReviewRecord::new(
                    id,
                    index.to_string().into_bytes(),
                ))
                .await?;
        }
        assert_eq!(
            state
                .list_guardian_review_records()
                .await?
                .into_iter()
                .filter(|record| record.thread_id == id)
                .map(|record| record.record)
                .collect::<Vec<_>>(),
            (4..12)
                .map(|index| index.to_string().into_bytes())
                .collect::<Vec<_>>(),
        );
        let payload = vec![b' '; MAX_GUARDIAN_REVIEW_BYTES / 3];
        for _ in 0..4 {
            state
                .record_guardian_review_failure(&GuardianReviewRecord::new(id, payload.clone()))
                .await?;
        }
        assert_eq!(
            state
                .list_guardian_review_records()
                .await?
                .into_iter()
                .map(|record| (record.thread_id, record.record))
                .collect::<Vec<_>>(),
            vec![(id, payload.clone()), (id, payload)],
        );
        harness.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deleting_an_actor_removes_evidence_and_rejects_late_captures() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let state = &harness.runtime;
        let id = ThreadId::new();
        harness.insert_thread(id).await;
        let record = GuardianReviewRecord::new(id, b"private review context".to_vec());
        state.record_guardian_review_failure(&record).await?;
        assert_eq!(1, state.list_guardian_review_records().await?.len());
        harness
            .antfly
            .sql()
            .await?
            .execute(
                "DELETE FROM codex_threads WHERE id = $1",
                sql_params![id.to_string()],
            )
            .await?;
        assert!(state.list_guardian_review_records().await?.is_empty());
        assert!(state.record_guardian_review_failure(&record).await.is_err());
        harness.close().await;
        Ok(())
    }
}
