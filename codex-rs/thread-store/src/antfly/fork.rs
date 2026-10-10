//! Forks and history reverts.
//!
//! Legacy threads: a turn boundary is located by scanning the persisted
//! `TurnStarted`/`TurnComplete`/`TurnAborted` events; `prepare_fork`'s model
//! context is the full history up to the boundary and `history_base` is
//! `None`.
//!
//! Paginated threads: boundaries come from the projected turns, searched
//! across the lineage like the local store (`ThroughTurn` finds the visible,
//! newest turn; `BeforeTurn` the oldest). The fork references its source
//! through `history_base`, so the child reads inherited history from the
//! source thread, and the model context is the latest-compaction scan cut at
//! that position. A revert truncates the thread at the turn's first ordinal
//! and rebuilds its projection; it is refused while a fork still inherits
//! history past that point.

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Write;
use codex_antfly::schema;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use codex_rollout::RolloutItem;

use super::AntflyThreadStore;
use super::from_value;
use super::history::Segment;
use super::history::resolve_lineage;
use super::internal;
use super::keys;
use super::projection;
use super::projection::TurnDoc;
use crate::ForkBoundary;
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::RevertThreadParams;
use crate::StoredTurnStatus;
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
    if record.history_mode() == ThreadHistoryMode::Paginated {
        return prepare_paginated_fork(store, params).await;
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
    if previous.history_mode() == ThreadHistoryMode::Paginated {
        return revert_paginated(store, previous, params).await;
    }

    let documents = store
        .antfly()
        .documents(schema::HISTORY_ITEMS)
        .scan(codex_antfly::ScanRequest::prefix(
            &keys::history_item_prefix(params.thread_id),
        ))
        .await
        .map_err(internal)?;
    let mut entries = Vec::with_capacity(documents.len());
    for document in documents {
        let Some((_, ordinal)) = keys::parse_history_item_id(&document.key) else {
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

    let mut record = previous;
    record.next_ordinal = boundary_ordinal;
    if params.multi_agent_version.is_some() {
        record.created.multi_agent_version = params.multi_agent_version;
    }

    let deletes: Vec<Write> = entries
        .into_iter()
        .filter(|(_, ordinal, _)| *ordinal >= boundary_ordinal)
        .map(|(key, _, _)| Write::delete(key))
        .collect();
    if !deletes.is_empty() {
        store
            .antfly()
            .documents(schema::HISTORY_ITEMS)
            .write(deletes)
            .await
            .map_err(internal)?;
    }
    store.save_record(&record).await
}

async fn turn_doc(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
    turn_id: &str,
) -> ThreadStoreResult<Option<TurnDoc>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT * FROM codex_thread_turns WHERE thread_id = $1 AND turn_id = $2",
            sql_params![thread_id.to_string(), turn_id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| TurnDoc::from_row(&row)).transpose()
}

/// The segment holding `turn_id` and its projected turn, searching newest
/// first (the visible turn) or oldest first (the source turn).
async fn find_turn(
    store: &AntflyThreadStore,
    segments: &[Segment],
    turn_id: &str,
    newest_first: bool,
) -> ThreadStoreResult<(usize, TurnDoc)> {
    let order: Vec<usize> = if newest_first {
        (0..segments.len()).rev().collect()
    } else {
        (0..segments.len()).collect()
    };
    for index in order {
        let segment = segments[index];
        if let Some(turn) = turn_doc(store, segment.thread_id, turn_id).await?
            && turn.rollout_ordinal >= segment.start_ordinal
            && segment
                .end_ordinal
                .is_none_or(|end| turn.rollout_ordinal < end)
        {
            return Ok((index, turn));
        }
    }
    Err(ThreadStoreError::InvalidRequest {
        message: format!("turn not found: {turn_id}"),
    })
}

/// `history_base` for a fork of the lineage at `boundary`.
async fn history_base_at_boundary(
    store: &AntflyThreadStore,
    segments: &[Segment],
    next_ordinal: u64,
    boundary: &ForkBoundary,
) -> ThreadStoreResult<HistoryPosition> {
    let (segment_index, end) = match boundary {
        ForkBoundary::Latest => (segments.len() - 1, next_ordinal),
        ForkBoundary::ThroughTurn(turn_id) => {
            let (index, turn) = find_turn(store, segments, turn_id, true).await?;
            if turn.status == StoredTurnStatus::InProgress {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("lastTurnId '{turn_id}' identifies an in-progress turn"),
                });
            }
            let end = turn
                .rollout_end_ordinal
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("turn {turn_id} does not have persisted rollout positions"),
                })?;
            (index, end + 1)
        }
        ForkBoundary::BeforeTurn(turn_id) => {
            let (index, turn) = find_turn(store, segments, turn_id, false).await?;
            if turn.rollout_end_ordinal == Some(turn.rollout_ordinal) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("turn {turn_id} does not have a persisted start boundary"),
                });
            }
            (index, turn.rollout_ordinal)
        }
    };
    let segment = segments[segment_index];
    if segment.end_ordinal.is_some_and(|limit| end > limit) {
        return Err(ThreadStoreError::InvalidRequest {
            message: "fork boundary exceeds inherited source history".to_owned(),
        });
    }
    // A cut at the very start of a segment would reference an empty segment;
    // collapse it to the previous segment's end instead.
    if end <= segment.start_ordinal && segment_index > 0 {
        let previous = segments[segment_index - 1];
        return Ok(HistoryPosition {
            thread_id: previous.thread_id,
            end_ordinal_exclusive: segment.start_ordinal.saturating_sub(1),
            end_byte_offset: 0,
        });
    }
    Ok(HistoryPosition {
        thread_id: segment.thread_id,
        end_ordinal_exclusive: end,
        end_byte_offset: 0,
    })
}

/// The model context visible at `base`: the newest compaction at or before
/// the cut plus everything after it, led by the source's `SessionMeta`.
async fn model_context_at(
    store: &AntflyThreadStore,
    source: ThreadId,
    segments: &[Segment],
    base: &HistoryPosition,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let cut = segments
        .iter()
        .position(|segment| segment.thread_id == base.thread_id)
        .unwrap_or(segments.len() - 1);
    let mut scan = ModelContextScan::default();
    let mut session_meta = None;
    'segments: for (index, segment) in segments[..=cut].iter().enumerate().rev() {
        let end = if index == cut {
            Some(base.end_ordinal_exclusive)
        } else {
            segment.end_ordinal
        };
        let items = store.load_items_before(segment.thread_id, end).await?;
        for item in items.into_iter().rev() {
            if let RolloutItem::SessionMeta(_) = &item {
                if session_meta.is_none() {
                    session_meta = Some(item);
                }
                continue 'segments;
            }
            match scan.push(item) {
                ModelContextScanProgress::Continue => {}
                ModelContextScanProgress::Complete => break 'segments,
            }
        }
    }
    let mut items = scan.finish();
    let session_meta = match session_meta {
        Some(meta) => meta,
        // The scan stopped before reaching a SessionMeta; use the source's.
        None => store
            .load_items_before(source, Some(1))
            .await?
            .into_iter()
            .next()
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id: source })?,
    };
    items.insert(0, session_meta);
    Ok(items)
}

async fn prepare_paginated_fork(
    store: &AntflyThreadStore,
    params: PrepareForkParams,
) -> ThreadStoreResult<PreparedFork> {
    let record =
        store
            .load_record(params.thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })?;
    let segments = resolve_lineage(store, params.thread_id).await?;
    let base =
        history_base_at_boundary(store, &segments, record.next_ordinal, &params.boundary).await?;
    let model_context = model_context_at(store, params.thread_id, &segments, &base).await?;
    // Deleting a thread a fork references is refused, so no reservation is
    // needed to keep the base alive.
    Ok(PreparedFork::new(
        params.thread_id,
        Some(base),
        Arc::new(model_context),
        (),
    ))
}

async fn revert_paginated(
    store: &AntflyThreadStore,
    previous: super::record::ThreadRecord,
    params: RevertThreadParams,
) -> ThreadStoreResult<()> {
    let thread_id = params.thread_id;
    let segments = resolve_lineage(store, thread_id).await?;
    let (index, turn) = find_turn(store, &segments, &params.before_turn_id, false).await?;
    if segments[index].thread_id != thread_id {
        // The turn lives in an inherited segment; truncating the source would
        // change other threads' history.
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "turn {} is inherited from another thread and cannot be reverted here",
                params.before_turn_id
            ),
        });
    }
    let cut = turn.rollout_ordinal;
    if let Some((child, _)) = store
        .fork_children(thread_id)
        .await?
        .into_iter()
        .find(|(_, end)| *end > cut)
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "thread {thread_id} cannot be reverted: fork {child} inherits history past turn {}",
                params.before_turn_id
            ),
        });
    }

    let documents = store
        .antfly()
        .documents(schema::HISTORY_ITEMS)
        .scan(codex_antfly::ScanRequest::prefix(
            &keys::history_item_prefix(thread_id),
        ))
        .await
        .map_err(internal)?;
    let mut kept = Vec::new();
    let mut deletes = Vec::new();
    for document in documents {
        let Some((_, ordinal)) = keys::parse_history_item_id(&document.key) else {
            continue;
        };
        if ordinal >= cut {
            deletes.push(Write::delete(document.key));
        } else if let Some(item) = document.doc.get("item").cloned() {
            kept.push((ordinal, from_value::<RolloutItem>(item)?));
        }
    }
    if !deletes.is_empty() {
        store
            .antfly()
            .documents(schema::HISTORY_ITEMS)
            .write(deletes)
            .await
            .map_err(internal)?;
    }
    store.projection_delete(thread_id).await?;

    let mut record = previous;
    record.next_ordinal = cut;
    if params.multi_agent_version.is_some() {
        record.created.multi_agent_version = params.multi_agent_version;
    }
    store.save_record(&record).await?;

    // Rebuild the projection from the surviving items. Ordinals are
    // contiguous from the first kept record.
    if let Some((first, _)) = kept.first() {
        let first = *first;
        let items: Vec<RolloutItem> = kept.into_iter().map(|(_, item)| item).collect();
        projection::apply_batch(
            store,
            thread_id,
            record.created.subagent_history_start_ordinal,
            first,
            &items,
            Utc::now(),
        )
        .await?;
    }
    Ok(())
}
