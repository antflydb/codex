//! Antfly backend for guardian review feedback. Mirrors
//! `state/src/runtime/guardian_feedback.rs` (SQLite: `guardian_review_feedback`
//! table, `ON DELETE CASCADE` on `thread_id`) exactly, except the cascade:
//! Antfly has no foreign keys, so a thread's guardian records must be deleted
//! explicitly (by whichever wiring hooks a thread delete into
//! `codex_antfly::Antfly`'s `ThreadDataCleanup`-style callback; not done in
//! this module).
//!
//! All records live under one small, capped collection (`st:guardian:`), so
//! every operation scans and rewrites the whole collection rather than
//! maintaining a secondary per-thread index: eviction never lets it grow
//! past [`crate::MAX_GUARDIAN_REVIEW_RECORDS`] rows or
//! [`crate::MAX_GUARDIAN_REVIEW_BYTES`] bytes, so the scan is always cheap.
//!
//! `GuardianReviewRecord::id` is a UUIDv7 (`Uuid::now_v7()`), whose canonical
//! hex string representation sorts lexicographically in creation order, so
//! an ascending key scan over `st:guardian:` is already oldest-first and a
//! reversed scan is newest-first — no separate ordering index is needed.

use std::sync::Arc;

use codex_antfly::Antfly;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::ThreadId;

use super::internal;
use crate::GuardianReviewRecord;
use crate::MAX_GUARDIAN_REVIEW_BYTES;
use crate::MAX_GUARDIAN_REVIEW_RECORDS;
use crate::MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD;

const PREFIX: &str = "st:guardian:";

fn key(id: &str) -> String {
    format!("{PREFIX}{id}")
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Doc {
    id: String,
    thread_id: ThreadId,
    record: Vec<u8>,
}

impl From<&GuardianReviewRecord> for Doc {
    fn from(record: &GuardianReviewRecord) -> Self {
        Self {
            id: record.id.clone(),
            thread_id: record.thread_id,
            record: record.record.clone(),
        }
    }
}

impl From<Doc> for GuardianReviewRecord {
    fn from(doc: Doc) -> Self {
        Self {
            id: doc.id,
            thread_id: doc.thread_id,
            record: doc.record,
        }
    }
}

async fn scan_all(antfly: &Arc<Antfly>) -> anyhow::Result<Vec<Doc>> {
    Ok(antfly
        .scan_as::<Doc>(ScanRequest::prefix(PREFIX))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect())
}

pub(crate) async fn record_guardian_review_failure(
    antfly: &Arc<Antfly>,
    record: &GuardianReviewRecord,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        record.record.len() < MAX_GUARDIAN_REVIEW_BYTES,
        "Guardian feedback record exceeds its size limit"
    );

    let _guard = antfly.lock().await;
    let mut all = scan_all(antfly).await?;
    all.push(Doc::from(record));

    // Per-thread cap: keep only the newest MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD
    // rows for this thread (id desc = newest first); drop the rest.
    let mut same_thread: Vec<&Doc> = all
        .iter()
        .filter(|doc| doc.thread_id == record.thread_id)
        .collect();
    same_thread.sort_by(|a, b| b.id.cmp(&a.id));
    let evicted_per_thread: std::collections::HashSet<String> = same_thread
        .into_iter()
        .skip(MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD)
        .map(|doc| doc.id.clone())
        .collect();
    all.retain(|doc| !evicted_per_thread.contains(&doc.id));

    // Global cap: keep a newest-first prefix bounded by both row count and
    // cumulative byte size (`length(record) + 1` per row, matching the SQL
    // window-function eviction).
    all.sort_by(|a, b| b.id.cmp(&a.id));
    let mut cumulative_bytes: i64 = 0;
    let mut surviving_ids = std::collections::HashSet::new();
    for (rank, doc) in all.iter().enumerate() {
        let rank = rank + 1;
        cumulative_bytes += doc.record.len() as i64 + 1;
        if rank > MAX_GUARDIAN_REVIEW_RECORDS || cumulative_bytes > MAX_GUARDIAN_REVIEW_BYTES as i64
        {
            continue;
        }
        surviving_ids.insert(doc.id.clone());
    }

    let mut writes = vec![Write::put(
        key(&record.id),
        serde_json::to_value(Doc::from(record))?,
    )];
    for id in &evicted_per_thread {
        writes.push(Write::delete(key(id)));
    }
    for doc in &all {
        if doc.id != record.id && !surviving_ids.contains(&doc.id) {
            writes.push(Write::delete(key(&doc.id)));
        }
    }
    // The just-inserted record always has the newest id (Uuid::now_v7 is
    // monotonic with wall-clock), so it is always rank 1 in both orderings
    // above and never evicted by its own insert.
    antfly.write(writes).await.map_err(internal)?;
    Ok(())
}

pub(crate) async fn list_guardian_review_records(
    antfly: &Arc<Antfly>,
) -> anyhow::Result<Vec<GuardianReviewRecord>> {
    Ok(scan_all(antfly)
        .await?
        .into_iter()
        .map(GuardianReviewRecord::from)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn settle() {
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    async fn test_antfly() -> (Arc<Antfly>, std::path::PathBuf) {
        let dir = crate::runtime::test_support::unique_temp_dir();
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let mut config = codex_antfly::AntflyConfig::embedded(dir.join("codex.aflite"));
        config.embedder = None;
        (Arc::new(Antfly::new(config)), dir)
    }

    async fn cleanup(dir: std::path::PathBuf) {
        settle();
        let _ = tokio::fs::remove_dir_all(dir).await;
    }

    fn thread(n: u32) -> ThreadId {
        ThreadId::from_string(&format!("00000000-0000-0000-0000-{n:012}")).expect("valid thread id")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_oversized_records() {
        let (antfly, dir) = test_antfly().await;
        let record = GuardianReviewRecord::new(thread(1), vec![0u8; MAX_GUARDIAN_REVIEW_BYTES]);
        let err = record_guardian_review_failure(&antfly, &record)
            .await
            .expect_err("oversized record should be rejected");
        assert!(err.to_string().contains("size limit"));
        cleanup(dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_is_oldest_first_and_roundtrips() {
        let (antfly, dir) = test_antfly().await;
        let a = GuardianReviewRecord::new(thread(1), b"a".to_vec());
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let b = GuardianReviewRecord::new(thread(2), b"b".to_vec());
        record_guardian_review_failure(&antfly, &a).await.unwrap();
        record_guardian_review_failure(&antfly, &b).await.unwrap();

        let records = list_guardian_review_records(&antfly).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id, a.id);
        assert_eq!(records[1].id, b.id);
        assert_eq!(records[0].record, b"a".to_vec());
        cleanup(dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn per_thread_cap_evicts_oldest_for_that_thread_only() {
        let (antfly, dir) = test_antfly().await;
        let busy_thread = thread(1);
        let other_thread = thread(2);
        record_guardian_review_failure(
            &antfly,
            &GuardianReviewRecord::new(other_thread, b"keep-me".to_vec()),
        )
        .await
        .unwrap();
        for i in 0..(MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD + 3) {
            record_guardian_review_failure(
                &antfly,
                &GuardianReviewRecord::new(busy_thread, format!("record-{i}").into_bytes()),
            )
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let records = list_guardian_review_records(&antfly).await.unwrap();
        let for_busy = records
            .iter()
            .filter(|r| r.thread_id == busy_thread)
            .count();
        let for_other = records
            .iter()
            .filter(|r| r.thread_id == other_thread)
            .count();
        assert_eq!(for_busy, MAX_GUARDIAN_REVIEW_RECORDS_PER_THREAD);
        assert_eq!(for_other, 1);
        // The newest records for the busy thread survive.
        let newest_kept: Vec<String> = records
            .iter()
            .filter(|r| r.thread_id == busy_thread)
            .map(|r| String::from_utf8(r.record.clone()).unwrap())
            .collect();
        assert!(newest_kept.contains(&"record-5".to_string()));
        assert!(!newest_kept.contains(&"record-0".to_string()));
        cleanup(dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn global_cap_evicts_oldest_once_count_exceeded() {
        let (antfly, dir) = test_antfly().await;
        // Spread across many threads so the per-thread cap never kicks in,
        // only the global row-count cap.
        for i in 0..(MAX_GUARDIAN_REVIEW_RECORDS + 5) {
            record_guardian_review_failure(
                &antfly,
                &GuardianReviewRecord::new(thread(i as u32), format!("r{i}").into_bytes()),
            )
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let records = list_guardian_review_records(&antfly).await.unwrap();
        assert_eq!(records.len(), MAX_GUARDIAN_REVIEW_RECORDS);
        // Oldest ones (r0..) should have been evicted; newest (the last
        // inserted) must survive.
        let bodies: Vec<String> = records
            .iter()
            .map(|r| String::from_utf8(r.record.clone()).unwrap())
            .collect();
        assert!(!bodies.contains(&"r0".to_string()));
        assert!(bodies.contains(&format!("r{}", MAX_GUARDIAN_REVIEW_RECORDS + 4)));
        cleanup(dir).await;
    }
}
