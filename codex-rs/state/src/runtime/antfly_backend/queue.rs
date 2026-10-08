//! Antfly backend for the durable, ordered user-submission queue. Mirrors
//! `state/src/runtime/queued_items.rs` (the SQLite implementation).
//!
//! Items for one thread live under `st:queue:{thread}:{ordinal}`, in
//! ascending `queue_order` key order. `queue_order` never resets or reuses a
//! value, even across `reorder`, matching the SQLite implementation.
//!
//! `change_version`/`changes_since` are served by a single global monotonic
//! counter (`st:queuerev:global`) plus one last-touched marker per thread
//! (`st:queuerev:t:{thread}`), bumped together under `Antfly::lock` on every
//! mutation. The SQLite implementation uses two independently numbered
//! counters (SQLite's own `PRAGMA data_version` and a trigger-maintained
//! `queued_thread_revisions` sequence); nothing in the contract requires
//! them to be different counters, only monotonic and comparable to their
//! own prior values, so one counter serves both here. `reorder` bumps the
//! counter once per call rather than once per moved item (the SQLite
//! triggers fire once per row); callers only compare revisions with `>`, so
//! this does not change observable behavior.

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::AntflyResult;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

use super::internal;
use crate::MAX_QUEUE_ITEMS;
use crate::QueuedUserSubmissionRecord;
use crate::model::datetime_to_epoch_millis;

const ITEM_PREFIX: &str = "st:queue:";
const GLOBAL_REV_KEY: &str = "st:queuerev:global";
const THREAD_REV_PREFIX: &str = "st:queuerev:t:";

fn items_prefix(thread_id: ThreadId) -> String {
    format!("{ITEM_PREFIX}{thread_id}:")
}

fn item_key(thread_id: ThreadId, queue_order: i64) -> String {
    format!(
        "{ITEM_PREFIX}{thread_id}:{}",
        codex_antfly::keys::ordinal(queue_order as u64)
    )
}

fn thread_rev_key(thread_id: ThreadId) -> String {
    format!("{THREAD_REV_PREFIX}{thread_id}")
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredItem {
    id: String,
    thread_id: String,
    payload_json: String,
    queue_order: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl StoredItem {
    fn into_record(self) -> anyhow::Result<QueuedUserSubmissionRecord> {
        Ok(QueuedUserSubmissionRecord {
            id: self.id,
            thread_id: ThreadId::from_string(&self.thread_id)?,
            payload: self.payload_json,
        })
    }
}

/// Every item for `thread_id`, in ascending `queue_order` (key) order.
async fn load_thread_items(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Vec<(String, StoredItem)>> {
    antfly
        .scan(ScanRequest::prefix(&items_prefix(thread_id)))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|document| {
            let item: StoredItem =
                serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
            Ok((document.key, item))
        })
        .collect()
}

async fn next_global_revision(antfly: &Arc<Antfly>) -> AntflyResult<i64> {
    let current = antfly.get_as::<i64>(GLOBAL_REV_KEY).await?.unwrap_or(0);
    Ok(current + 1)
}

/// Writes that bump the shared queue revision counter and this thread's
/// last-touched marker. Callers append their own item mutations and send
/// everything in one atomic `Antfly::write` call under `Antfly::lock`.
async fn bump_revision_writes(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> AntflyResult<Vec<Write>> {
    let revision = next_global_revision(antfly).await?;
    Ok(vec![
        Write::put(GLOBAL_REV_KEY, serde_json::json!(revision)),
        Write::put(thread_rev_key(thread_id), serde_json::json!(revision)),
    ])
}

pub(crate) async fn change_version(antfly: &Arc<Antfly>) -> anyhow::Result<i64> {
    antfly
        .get_as::<i64>(GLOBAL_REV_KEY)
        .await
        .map_err(internal)
        .map(|value| value.unwrap_or(0))
}

pub(crate) async fn changes_since(
    antfly: &Arc<Antfly>,
    revision: i64,
    thread_ids: &[ThreadId],
) -> anyhow::Result<Vec<(ThreadId, i64)>> {
    if thread_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut changes = Vec::new();
    for &thread_id in thread_ids {
        let Some(thread_revision) = antfly
            .get_as::<i64>(thread_rev_key(thread_id))
            .await
            .map_err(internal)?
        else {
            continue;
        };
        if thread_revision > revision {
            changes.push((thread_id, thread_revision));
        }
    }
    changes.sort_by_key(|(_, revision)| *revision);
    Ok(changes)
}

pub(crate) async fn enqueue(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    payload_json: &str,
) -> anyhow::Result<QueuedUserSubmissionRecord> {
    let _guard = antfly.lock().await;
    let items = load_thread_items(antfly, thread_id).await?;
    if items.len() >= MAX_QUEUE_ITEMS {
        // Matches the SQLite `INSERT...SELECT...WHERE count < MAX` producing
        // no row, which `thread-store::queue_store` detects by downcasting to
        // this exact error.
        return Err(anyhow::Error::new(sqlx::Error::RowNotFound));
    }
    let next_queue_order = items
        .last()
        .map(|(_, item)| item.queue_order + 1)
        .unwrap_or(0);
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let id = uuid::Uuid::now_v7().to_string();
    let record = StoredItem {
        id: id.clone(),
        thread_id: thread_id.to_string(),
        payload_json: payload_json.to_string(),
        queue_order: next_queue_order,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };
    let mut writes = vec![Write::put(
        item_key(thread_id, next_queue_order),
        serde_json::to_value(&record)?,
    )];
    writes.extend(
        bump_revision_writes(antfly, thread_id)
            .await
            .map_err(internal)?,
    );
    antfly.write(writes).await.map_err(internal)?;
    record.into_record()
}

pub(crate) async fn list_page(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    offset: usize,
    limit: usize,
) -> anyhow::Result<Vec<QueuedUserSubmissionRecord>> {
    load_thread_items(antfly, thread_id)
        .await?
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(_, item)| item.into_record())
        .collect()
}

pub(crate) async fn update(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    item_id: &str,
    payload_json: &str,
) -> anyhow::Result<Option<QueuedUserSubmissionRecord>> {
    let _guard = antfly.lock().await;
    let items = load_thread_items(antfly, thread_id).await?;
    let Some((key, mut item)) = items.into_iter().find(|(_, item)| item.id == item_id) else {
        return Ok(None);
    };
    item.payload_json = payload_json.to_string();
    item.updated_at_ms = datetime_to_epoch_millis(Utc::now());
    antfly
        .write(vec![Write::put(key, serde_json::to_value(&item)?)])
        .await
        .map_err(internal)?;
    Ok(Some(item.into_record()?))
}

pub(crate) async fn delete(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    item_id: &str,
) -> anyhow::Result<bool> {
    let _guard = antfly.lock().await;
    let items = load_thread_items(antfly, thread_id).await?;
    let Some((key, _)) = items.into_iter().find(|(_, item)| item.id == item_id) else {
        return Ok(false);
    };
    let mut writes = vec![Write::delete(key)];
    writes.extend(
        bump_revision_writes(antfly, thread_id)
            .await
            .map_err(internal)?,
    );
    antfly.write(writes).await.map_err(internal)?;
    Ok(true)
}

pub(crate) async fn reorder(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    ordered_ids: &[String],
) -> anyhow::Result<()> {
    let _guard = antfly.lock().await;
    let items = load_thread_items(antfly, thread_id).await?;

    let mut current_ids: Vec<&str> = items.iter().map(|(_, item)| item.id.as_str()).collect();
    current_ids.sort_unstable();
    let mut requested_ids: Vec<&str> = ordered_ids.iter().map(String::as_str).collect();
    requested_ids.sort_unstable();
    if current_ids != requested_ids {
        return Err(anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "queue reorder must include every queued submission exactly once",
        )));
    }

    let max_queue_order = items
        .iter()
        .map(|(_, item)| item.queue_order)
        .max()
        .unwrap_or(-1);
    let now_ms = datetime_to_epoch_millis(Utc::now());
    let mut writes = Vec::with_capacity(ordered_ids.len() * 2 + 2);
    for (index, id) in ordered_ids.iter().enumerate() {
        let Some((old_key, mut item)) = items.iter().find(|(_, item)| &item.id == id).cloned()
        else {
            continue;
        };
        let new_queue_order = max_queue_order + 1 + index as i64;
        item.queue_order = new_queue_order;
        item.updated_at_ms = now_ms;
        let new_key = item_key(thread_id, new_queue_order);
        if new_key != old_key {
            writes.push(Write::delete(old_key));
        }
        writes.push(Write::put(new_key, serde_json::to_value(&item)?));
    }
    writes.extend(
        bump_revision_writes(antfly, thread_id)
            .await
            .map_err(internal)?,
    );
    antfly.write(writes).await.map_err(internal)
}

pub(crate) async fn delete_thread_queue(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let _guard = antfly.lock().await;
    let items = load_thread_items(antfly, thread_id).await?;
    if items.is_empty() {
        return Ok(false);
    }
    let mut writes: Vec<Write> = items
        .into_iter()
        .map(|(key, _)| Write::delete(key))
        .collect();
    writes.extend(
        bump_revision_writes(antfly, thread_id)
            .await
            .map_err(internal)?,
    );
    antfly.write(writes).await.map_err(internal)?;
    Ok(true)
}
