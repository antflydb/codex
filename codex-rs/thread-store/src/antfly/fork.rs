//! Reference-backed forks and history reverts, supported for Legacy threads.
//!
//! This store does not implement Paginated history yet, so both operations
//! here only handle `ThreadHistoryMode::Legacy` sources; a Paginated source
//! returns `Unsupported`, leaving room for paginated support to be added
//! separately.
//!
//! A turn boundary is located by scanning the thread's persisted
//! `TurnStarted`/`TurnComplete`/`TurnAborted` events, mirroring the local
//! store's `ThroughTurn`/`BeforeTurn` semantics. For Legacy threads,
//! `prepare_fork`'s model context is simply the full persisted history up to
//! the boundary (not a lineage reference), so `history_base` is always
//! `None` and the returned `PreparedFork` holds a trivial source
//! reservation: the model context is already a self-contained copy by the
//! time this call returns, so nothing further is read from the source.

use std::sync::Arc;

use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;

use super::AntflyThreadStore;
use super::from_value;
use super::internal;
use super::keys;
use crate::ForkBoundary;
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::RevertThreadParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// First index whose item is `TurnStarted{turn_id}`.
fn turn_started_index(items: &[RolloutItem], turn_id: &str) -> Option<usize> {
    items.iter().position(|item| {
        matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(event)) if event.turn_id == turn_id)
    })
}

/// Last index whose item is `TurnStarted{turn_id}`.
fn last_turn_started_index(items: &[RolloutItem], turn_id: &str) -> Option<usize> {
    items.iter().rposition(|item| {
        matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(event)) if event.turn_id == turn_id)
    })
}

/// First index at or after `from` whose item completes (or aborts) `turn_id`.
fn turn_completion_index(items: &[RolloutItem], turn_id: &str, from: usize) -> Option<usize> {
    items
        .iter()
        .enumerate()
        .skip(from)
        .find_map(|(index, item)| {
            let completed = match item {
                RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => event.turn_id == turn_id,
                RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                    event.turn_id.as_deref() == Some(turn_id)
                }
                _ => false,
            };
            completed.then_some(index)
        })
}

/// Index one past the last item visible at `boundary`, within `items`.
fn boundary_end_index(items: &[RolloutItem], boundary: &ForkBoundary) -> ThreadStoreResult<usize> {
    match boundary {
        ForkBoundary::Latest => Ok(items.len()),
        ForkBoundary::ThroughTurn(turn_id) => {
            let start = last_turn_started_index(items, turn_id).ok_or_else(|| {
                ThreadStoreError::InvalidRequest {
                    message: format!("turn not found: {turn_id}"),
                }
            })?;
            let end = turn_completion_index(items, turn_id, start).ok_or_else(|| {
                ThreadStoreError::InvalidRequest {
                    message: format!("lastTurnId '{turn_id}' identifies an in-progress turn"),
                }
            })?;
            Ok(end + 1)
        }
        ForkBoundary::BeforeTurn(turn_id) => {
            turn_started_index(items, turn_id).ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!("turn not found: {turn_id}"),
            })
        }
    }
}

pub(super) async fn prepare_fork(
    store: &AntflyThreadStore,
    params: PrepareForkParams,
) -> ThreadStoreResult<PreparedFork> {
    // Flush a live writer so the boundary search sees its latest items;
    // tolerate there being none.
    match store
        .persist_thread_impl(params.thread_id, PersistContext::Standard)
        .await
    {
        Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
        Err(err) => return Err(err),
    }
    let record =
        store
            .load_record(params.thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })?;
    if !matches!(record.history_mode(), ThreadHistoryMode::Legacy) {
        return Err(ThreadStoreError::Unsupported {
            operation: "prepare_fork",
        });
    }
    let items = store.load_items(params.thread_id).await?;
    let end = boundary_end_index(&items, &params.boundary)?;
    let model_context = Arc::new(items[..end].to_vec());
    Ok(PreparedFork::new(params.thread_id, None, model_context, ()))
}

pub(super) async fn revert_thread(
    store: &AntflyThreadStore,
    params: RevertThreadParams,
) -> ThreadStoreResult<()> {
    if store.has_live_writer(params.thread_id).await {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {} has an active writer", params.thread_id),
        });
    }
    let _guard = store.antfly().lock().await;
    let previous =
        store
            .load_record(params.thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })?;
    if !matches!(previous.history_mode(), ThreadHistoryMode::Legacy) {
        return Err(ThreadStoreError::Unsupported {
            operation: "revert_thread",
        });
    }

    let documents = store
        .antfly()
        .scan(ScanRequest::prefix(&keys::items_prefix(params.thread_id)))
        .await
        .map_err(internal)?;
    let mut entries = Vec::with_capacity(documents.len());
    for document in documents {
        let Some((_, ordinal)) = keys::parse_item(&document.key) else {
            continue;
        };
        let item = document
            .doc
            .get("item")
            .cloned()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("item {} has no payload", document.key),
            })?;
        let item: RolloutItem = from_value(item)?;
        entries.push((document.key, ordinal, item));
    }
    let items: Vec<RolloutItem> = entries.iter().map(|(_, _, item)| item.clone()).collect();
    let boundary = turn_started_index(&items, &params.before_turn_id).ok_or_else(|| {
        ThreadStoreError::InvalidRequest {
            message: format!("turn not found: {}", params.before_turn_id),
        }
    })?;
    let boundary_ordinal = entries[boundary].1;

    let mut record = previous.clone();
    record.next_ordinal = boundary_ordinal;
    if params.multi_agent_version.is_some() {
        record.created.multi_agent_version = params.multi_agent_version;
    }

    let mut writes: Vec<Write> = entries
        .into_iter()
        .filter(|(_, ordinal, _)| *ordinal >= boundary_ordinal)
        .map(|(key, _, _)| Write::delete(key))
        .collect();
    writes.extend(AntflyThreadStore::record_writes(Some(&previous), &record)?);
    store.antfly().write(writes).await.map_err(internal)
}
