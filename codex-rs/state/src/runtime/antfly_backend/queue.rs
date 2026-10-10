//! Antfly backend for the durable, ordered user-submission queue. Mirrors
//! `state/src/runtime/queued_items.rs` (the SQLite implementation), backed by
//! the `codex_queued_items` and `codex_queued_thread_revisions` SQL tables
//! (see `codex-antfly/src/schema.rs`, migration 1).
//!
//! SQLite assigns `queued_thread_revisions.revision` with an `AUTOINCREMENT`
//! primary key plus triggers that fire after every insert/update/delete on
//! `queued_items`. Antfly has neither sequences nor triggers, so every
//! mutation here recomputes `COALESCE(MAX(revision), 0) + 1` across
//! `codex_queued_thread_revisions` and upserts the mutated thread's row with
//! it, inside the same transaction as the `codex_queued_items` write. Two
//! transactions racing on the *same* thread's revision row (or on the same
//! `(thread_id, queue_order)` item) fail with 40001 or 23505; both are
//! retried as a whole-transaction retry (see [`with_retry`]). Two
//! transactions touching *different* threads do not conflict at the
//! database level and may compute equal revision numbers; that is harmless
//! here since `changes_since` only compares a thread's own revision against
//! a previously observed baseline, never claims a total order across
//! threads, matching the single shared counter note in the original
//! key-prefix backend this replaces.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::AntflyError;
use codex_antfly::AntflyResult;
use codex_antfly::sql::Sql;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use uuid::Uuid;

use super::internal;
use crate::MAX_QUEUE_ITEMS;
use crate::QueuedUserSubmissionRecord;
use crate::model::datetime_to_epoch_millis;

const MAX_RETRY_ATTEMPTS: u32 = 10;

fn is_retryable(err: &AntflyError) -> bool {
    err.is_conflict() || err.is_unique_violation()
}

type Attempt<'a, T> = Pin<Box<dyn Future<Output = AntflyResult<T>> + Send + 'a>>;

/// Runs `attempt` inside a fresh transaction, retrying the whole transaction
/// (up to [`MAX_RETRY_ATTEMPTS`] times) on a serialization conflict (40001)
/// or a unique-constraint race (23505) against a concurrent writer.
async fn with_retry<T>(
    sql: &Sql,
    mut attempt: impl for<'a> FnMut(&'a mut SqlTx) -> Attempt<'a, T>,
) -> AntflyResult<T> {
    for _ in 0..MAX_RETRY_ATTEMPTS {
        let mut tx = sql.begin().await?;
        match attempt(&mut tx).await {
            Ok(value) => match tx.commit().await {
                Ok(()) => return Ok(value),
                Err(err) if is_retryable(&err) => continue,
                Err(err) => return Err(err),
            },
            Err(err) => {
                let _ = tx.rollback().await;
                if is_retryable(&err) {
                    continue;
                }
                return Err(err);
            }
        }
    }
    Err(AntflyError::Sql {
        code: Some("40001".to_string()),
        message: "codex_queued_items: exceeded retry attempts after repeated conflicts".to_string(),
    })
}

/// Recomputes the next global revision and upserts it as `thread_id`'s
/// revision, inside the caller's transaction.
async fn bump_revision(tx: &mut SqlTx, thread_id: ThreadId) -> AntflyResult<i64> {
    let row = tx
        .fetch_optional(
            "SELECT COALESCE(MAX(revision), 0) + 1 AS next_revision FROM codex_queued_thread_revisions",
            vec![],
        )
        .await?;
    let revision = match row {
        Some(row) => row.i64("next_revision")?,
        None => 1,
    };
    tx.execute(
        "INSERT INTO codex_queued_thread_revisions (thread_id, revision)
         VALUES ($1, $2)
         ON CONFLICT (thread_id) DO UPDATE SET revision = excluded.revision",
        sql_params![thread_id.to_string(), revision],
    )
    .await?;
    Ok(revision)
}

fn record_from_row(row: &SqlRow) -> anyhow::Result<QueuedUserSubmissionRecord> {
    Ok(QueuedUserSubmissionRecord {
        id: row.string("id").map_err(internal)?,
        thread_id: ThreadId::try_from(row.string("thread_id").map_err(internal)?)?,
        payload: super::text_column(row, "payload_json").map_err(internal)?,
    })
}

pub(crate) async fn change_version(antfly: &Arc<Antfly>) -> anyhow::Result<i64> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT COALESCE(MAX(revision), 0) AS revision FROM codex_queued_thread_revisions",
            vec![],
        )
        .await
        .map_err(internal)?;
    match row {
        Some(row) => row.i64("revision").map_err(internal),
        None => Ok(0),
    }
}

pub(crate) async fn changes_since(
    antfly: &Arc<Antfly>,
    revision: i64,
    thread_ids: &[ThreadId],
) -> anyhow::Result<Vec<(ThreadId, i64)>> {
    if thread_ids.is_empty() {
        return Ok(Vec::new());
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let mut statement = String::from(
        "SELECT thread_id, revision FROM codex_queued_thread_revisions \
         WHERE revision > $1 AND thread_id IN (",
    );
    let mut params = sql_params![revision];
    for (index, thread_id) in thread_ids.iter().enumerate() {
        if index > 0 {
            statement.push_str(", ");
        }
        statement.push_str(&format!("${}", index + 2));
        params.push(thread_id.to_string().into());
    }
    statement.push_str(") ORDER BY revision");
    let rows = sql.fetch_all(&statement, params).await.map_err(internal)?;
    rows.iter()
        .map(|row| {
            let thread_id = ThreadId::try_from(row.string("thread_id").map_err(internal)?)?;
            let revision = row.i64("revision").map_err(internal)?;
            Ok((thread_id, revision))
        })
        .collect()
}

pub(crate) async fn enqueue(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    payload_json: &str,
) -> anyhow::Result<QueuedUserSubmissionRecord> {
    let sql = antfly.sql().await.map_err(internal)?;
    let id = Uuid::now_v7().to_string();
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let max_items = i64::try_from(MAX_QUEUE_ITEMS)?;
    let payload_json = payload_json.to_string();

    // Antfly does not support the `INSERT ... SELECT ...` form SQLite uses to
    // compute `queue_order` and enforce the queue limit in one statement
    // ("This SQL statement or expression is not supported", SQLSTATE 0A000):
    // read both, then insert, retrying the whole transaction on a race (a
    // concurrent enqueue for the same thread can compute the same
    // `queue_order` before either commits, which the unique index on
    // `(thread_id, queue_order)` turns into 23505).
    let row = with_retry(sql, move |tx| {
        let id = id.clone();
        let payload_json = payload_json.clone();
        let thread_id_str = thread_id.to_string();
        Box::pin(async move {
            let count = tx
                .fetch_optional(
                    "SELECT COUNT(*) AS count FROM codex_queued_items WHERE thread_id = $1",
                    sql_params![thread_id_str.clone()],
                )
                .await?
                .map(|row| row.i64("count"))
                .transpose()?
                .unwrap_or(0);
            if count >= max_items {
                return Ok(None);
            }
            let next_order = tx
                .fetch_optional(
                    "SELECT COALESCE(MAX(queue_order), -1) + 1 AS next_order \
                     FROM codex_queued_items WHERE thread_id = $1",
                    sql_params![thread_id_str.clone()],
                )
                .await?
                .map(|row| row.i64("next_order"))
                .transpose()?
                .unwrap_or(0);
            let row = tx
                .fetch_optional(
                    "INSERT INTO codex_queued_items (
                        id, thread_id, payload_json, queue_order,
                        created_at_ms, updated_at_ms
                     ) VALUES ($1, $2, $3, $4, $5, $5)
                     RETURNING id, thread_id, payload_json",
                    sql_params![id, thread_id_str, payload_json, next_order, now_ms],
                )
                .await?;
            if row.is_some() {
                bump_revision(tx, thread_id).await?;
            }
            Ok(row)
        })
    })
    .await
    .map_err(internal)?;

    let Some(row) = row else {
        // Matches the SQLite `INSERT...SELECT...WHERE count < MAX` producing
        // no row, which `thread-store::queue_store` detects by downcasting to
        // this exact error.
        return Err(anyhow::Error::new(sqlx::Error::RowNotFound));
    };
    record_from_row(&row)
}

pub(crate) async fn list_page(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    offset: usize,
    limit: usize,
) -> anyhow::Result<Vec<QueuedUserSubmissionRecord>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT id, thread_id, payload_json
             FROM codex_queued_items
             WHERE thread_id = $1
             ORDER BY queue_order LIMIT $2 OFFSET $3",
            sql_params![
                thread_id.to_string(),
                i64::try_from(limit)?,
                i64::try_from(offset)?
            ],
        )
        .await
        .map_err(internal)?;
    rows.iter().map(record_from_row).collect()
}

pub(crate) async fn update(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    item_id: &str,
    payload_json: &str,
) -> anyhow::Result<Option<QueuedUserSubmissionRecord>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let payload_json = payload_json.to_string();

    let row = with_retry(sql, move |tx| {
        let item_id = item_id.to_string();
        let payload_json = payload_json.clone();
        Box::pin(async move {
            let row = tx
                .fetch_optional(
                    "UPDATE codex_queued_items
                     SET payload_json = $1, updated_at_ms = $2
                     WHERE thread_id = $3 AND id = $4
                     RETURNING id, thread_id, payload_json",
                    sql_params![payload_json, now_ms, thread_id.to_string(), item_id],
                )
                .await?;
            if row.is_some() {
                bump_revision(tx, thread_id).await?;
            }
            Ok(row)
        })
    })
    .await
    .map_err(internal)?;

    row.as_ref().map(record_from_row).transpose()
}

pub(crate) async fn delete(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    item_id: &str,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let deleted = with_retry(sql, move |tx| {
        let item_id = item_id.to_string();
        Box::pin(async move {
            let rows_affected = tx
                .execute(
                    "DELETE FROM codex_queued_items WHERE thread_id = $1 AND id = $2",
                    sql_params![thread_id.to_string(), item_id],
                )
                .await?;
            if rows_affected > 0 {
                bump_revision(tx, thread_id).await?;
            }
            Ok(rows_affected > 0)
        })
    })
    .await
    .map_err(internal)?;
    Ok(deleted)
}

pub(crate) async fn reorder(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    ordered_ids: &[String],
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now_ms = datetime_to_epoch_millis(Utc::now());

    // Validated up front, outside the retry loop: the SQLite implementation
    // rolls back and returns this exact error on mismatch, without retrying.
    let existing_rows = sql
        .fetch_all(
            "SELECT id FROM codex_queued_items WHERE thread_id = $1",
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    let mut expected_ids = existing_rows
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut requested_ids = ordered_ids.to_vec();
    expected_ids.sort();
    requested_ids.sort();
    if expected_ids != requested_ids {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "queue reorder must include every queued submission exactly once",
        )
        .into());
    }

    with_retry(sql, move |tx| {
        let ordered_ids = ordered_ids.to_vec();
        Box::pin(async move {
            let row = tx
                .fetch_optional(
                    "SELECT COALESCE(MAX(queue_order), -1) AS max_queue_order \
                     FROM codex_queued_items WHERE thread_id = $1",
                    sql_params![thread_id.to_string()],
                )
                .await?;
            let max_queue_order = match row {
                Some(row) => row.i64("max_queue_order")?,
                None => -1,
            };
            for (index, item_id) in ordered_ids.iter().enumerate() {
                tx.execute(
                    "UPDATE codex_queued_items SET queue_order = $1, updated_at_ms = $2
                     WHERE thread_id = $3 AND id = $4",
                    sql_params![
                        max_queue_order + index as i64 + 1,
                        now_ms,
                        thread_id.to_string(),
                        item_id.clone(),
                    ],
                )
                .await?;
            }
            bump_revision(tx, thread_id).await?;
            Ok(())
        })
    })
    .await
    .map_err(internal)
}

pub(crate) async fn delete_thread_queue(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    with_retry(sql, move |tx| {
        Box::pin(async move {
            let rows_affected = tx
                .execute(
                    "DELETE FROM codex_queued_items WHERE thread_id = $1",
                    sql_params![thread_id.to_string()],
                )
                .await?;
            if rows_affected > 0 {
                bump_revision(tx, thread_id).await?;
            }
            Ok(rows_affected > 0)
        })
    })
    .await
    .map_err(internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteQueueStore;
    use pretty_assertions::assert_eq;

    async fn test_store() -> (SqliteQueueStore, Arc<Antfly>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
        config.embedder = None;
        let antfly = Arc::new(codex_antfly::Antfly::new(config));
        (
            SqliteQueueStore::new_antfly(Arc::clone(&antfly)),
            antfly,
            dir,
        )
    }

    async fn cleanup(antfly: &Antfly, dir: tempfile::TempDir) {
        antfly.close().await.expect("close antfly");
        drop(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fifo_dispatch_preserves_edits_reordering_and_pagination() {
        let (queue, antfly, dir) = test_store().await;
        let thread_id = ThreadId::new();
        let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
        let second = queue.enqueue(thread_id, r#"{"n":2}"#).await.unwrap();
        let third = queue.enqueue(thread_id, r#"{"n":3}"#).await.unwrap();

        let updated = queue
            .update(thread_id, &first.id, r#"{"n":"edited"}"#)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, updated.id);
        let error = queue
            .reorder(thread_id, std::slice::from_ref(&first.id))
            .await
            .unwrap_err();
        assert_eq!(
            std::io::ErrorKind::InvalidInput,
            error.downcast_ref::<std::io::Error>().unwrap().kind()
        );

        let ordered_ids = vec![third.id, first.id, second.id];
        queue.reorder(thread_id, &ordered_ids).await.unwrap();

        let items = queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 3)
            .await
            .unwrap();
        let page = queue
            .list_page(thread_id, /*offset*/ 1, /*limit*/ 1)
            .await
            .unwrap();
        assert_eq!(vec![items[1].clone()], page);
        assert_eq!(r#"{"n":"edited"}"#, items[1].payload);

        for item in items {
            assert!(queue.delete(thread_id, &item.id).await.unwrap());
        }
        assert!(
            queue
                .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
                .await
                .unwrap()
                .is_empty()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_revisions_identify_changed_threads_after_updates_and_deletions() {
        let (queue, antfly, dir) = test_store().await;
        let thread_id = ThreadId::new();
        let first = queue.enqueue(thread_id, r#"{"first":true}"#).await.unwrap();
        let first_revision = queue
            .changes_since(/*revision*/ 0, &[thread_id])
            .await
            .unwrap()[0]
            .1;
        queue
            .update(thread_id, &first.id, r#"{"updated":true}"#)
            .await
            .unwrap();
        let updated_revision = queue
            .changes_since(first_revision, &[thread_id])
            .await
            .unwrap()[0]
            .1;
        let other_thread_id = ThreadId::new();
        queue
            .enqueue(other_thread_id, r#"{"other":true}"#)
            .await
            .unwrap();
        let newly_loaded_changes = queue
            .changes_since(/*revision*/ 0, &[other_thread_id])
            .await
            .unwrap();
        assert_eq!(
            vec![(thread_id, updated_revision), newly_loaded_changes[0]],
            queue
                .changes_since(first_revision, &[thread_id, other_thread_id])
                .await
                .unwrap()
        );
        assert!(queue.delete(thread_id, &first.id).await.unwrap());
        assert!(
            queue
                .changes_since(updated_revision, &[thread_id])
                .await
                .unwrap()
                .iter()
                .any(|(changed_thread, _)| *changed_thread == thread_id)
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_operations_cannot_mutate_another_threads_messages() {
        let (queue, antfly, dir) = test_store().await;
        let thread_id = ThreadId::new();
        let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
        let other_thread_id = ThreadId::new();
        let other = queue.enqueue(other_thread_id, r#"{"n":2}"#).await.unwrap();
        let other_id = &other.id;

        assert_eq!(
            None,
            queue
                .update(thread_id, other_id, r#"{"n":3}"#)
                .await
                .unwrap()
        );
        assert!(!queue.delete(thread_id, other_id).await.unwrap());
        assert!(
            queue
                .reorder(thread_id, std::slice::from_ref(other_id))
                .await
                .is_err()
        );
        let (items, other_items) = tokio::join!(
            queue.list_page(thread_id, /*offset*/ 0, /*limit*/ 1),
            queue.list_page(other_thread_id, /*offset*/ 0, /*limit*/ 1),
        );
        assert_eq!(
            (vec![first], vec![other]),
            (items.unwrap(), other_items.unwrap())
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deleting_a_thread_removes_its_queue() {
        let (queue, antfly, dir) = test_store().await;
        let thread_id = ThreadId::new();
        queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

        assert!(queue.delete_thread_queue(thread_id).await.unwrap());
        assert!(
            queue
                .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!queue.delete_thread_queue(thread_id).await.unwrap());

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn competing_enqueues_preserve_fifo_queue_order() {
        let (queue, antfly, dir) = test_store().await;
        let thread_id = ThreadId::new();
        let (first, second) = tokio::join!(
            queue.enqueue(thread_id, r#"{"first":true}"#),
            queue.enqueue(thread_id, r#"{"second":true}"#),
        );
        let mut expected = vec![first.unwrap(), second.unwrap()];
        expected.sort_by(|first, second| first.id.cmp(&second.id));
        let mut actual = queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 2)
            .await
            .unwrap();
        actual.sort_by(|first, second| first.id.cmp(&second.id));
        assert_eq!(expected, actual);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_inserts_enforce_the_queue_limit() {
        let (queue, antfly, dir) = test_store().await;
        let thread_id = ThreadId::new();
        for _ in 0..MAX_QUEUE_ITEMS - 1 {
            queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
        }
        let (first, second) = tokio::join!(
            queue.enqueue(thread_id, r#"{"n":2}"#),
            queue.enqueue(thread_id, r#"{"n":3}"#),
        );
        assert_ne!(first.is_ok(), second.is_ok());
        assert_eq!(
            MAX_QUEUE_ITEMS,
            queue
                .list_page(thread_id, /*offset*/ 0, /*limit*/ MAX_QUEUE_ITEMS)
                .await
                .unwrap()
                .len()
        );

        cleanup(&antfly, dir).await;
    }
}
