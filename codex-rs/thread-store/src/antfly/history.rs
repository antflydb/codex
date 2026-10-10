//! Paginated-history reads: lineage resolution, `list_turns`, `list_items`,
//! `list_timeline`, and the reverse-scan "latest model context" used by
//! `load_latest_model_context` and `resume_thread` (spec §0, §2.4, §2.9,
//! §2.10, §2.24).
//!
//! Lineage here is simpler than the local store's: a fork always creates a
//! brand-new [`ThreadId`] (`revert_thread` is unsupported), so one logical
//! thread id maps to exactly one segment and `HistoryPosition::thread_id`
//! names that segment's owning thread directly.

use std::collections::HashSet;

use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadRealtimeItem;
use codex_app_server_protocol::ThreadTimelineEntry;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::realtime::RealtimeItem;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use codex_rollout::RolloutItem;
use serde::Deserialize;
use serde::Serialize;

use super::AntflyThreadStore;
use super::internal;
use super::projection::ItemDoc;
use super::projection::TurnDoc;
use crate::ItemPage;
use crate::ItemSortKey;
use crate::ListItemsParams;
use crate::ListItemsPosition;
use crate::ListTimelineParams;
use crate::ListTurnsParams;
use crate::SortDirection;
use crate::StoredThreadItem;
use crate::StoredTurn;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TimelinePage;
use crate::TurnPage;

/// One immutable range of ordinals contributed by one thread's own storage.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Segment {
    pub(crate) thread_id: ThreadId,
    pub(crate) start_ordinal: u64,
    /// Exclusive upper bound; `None` for the newest (unbounded) segment.
    pub(crate) end_ordinal: Option<u64>,
}

/// Walks `history_base` pointers from `thread_id` back to its root, oldest
/// segment first.
pub(super) async fn resolve_lineage(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<Segment>> {
    let mut segments = Vec::new();
    let mut seen = HashSet::new();
    let mut current = thread_id;
    let mut end = None;
    loop {
        if !seen.insert(current) {
            return Err(ThreadStoreError::Internal {
                message: format!("cyclic paginated history lineage for thread {thread_id}"),
            });
        }
        let record =
            store
                .load_record(current)
                .await?
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!("missing lineage ancestor {current} for thread {thread_id}"),
                })?;
        let base_ordinal = record
            .created
            .history_base
            .map(|base| base.end_ordinal_exclusive)
            .unwrap_or(0);
        segments.push(Segment {
            thread_id: current,
            start_ordinal: base_ordinal + 1,
            end_ordinal: end,
        });
        match record.created.history_base {
            Some(base) => {
                end = Some(base.end_ordinal_exclusive);
                current = base.thread_id;
            }
            None => break,
        }
    }
    segments.reverse();
    Ok(segments)
}

pub(crate) fn segment_index_for_ordinal(segments: &[Segment], ordinal: u64) -> Option<usize> {
    segments.iter().position(|segment| {
        ordinal >= segment.start_ordinal && segment.end_ordinal.is_none_or(|end| ordinal < end)
    })
}

/// Segment visitation order for a listing: ascending walks the cursor's
/// segment through the newest; descending walks it back to the oldest.
fn segment_order(
    len: usize,
    direction: SortDirection,
    cursor_segment: Option<usize>,
) -> Vec<usize> {
    match direction {
        SortDirection::Asc => (cursor_segment.unwrap_or(0)..len).collect(),
        SortDirection::Desc => {
            let end = cursor_segment.unwrap_or_else(|| len.saturating_sub(1));
            (0..=end).rev().collect()
        }
    }
}

pub(super) async fn validate_paginated(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
    include_archived: bool,
    operation: &'static str,
) -> ThreadStoreResult<()> {
    let Some(record) = store.load_record(thread_id).await? else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    if record.archived_at.is_some() && !include_archived {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} is archived"),
        });
    }
    match record.history_mode() {
        ThreadHistoryMode::Legacy => Err(ThreadStoreError::Unsupported { operation }),
        ThreadHistoryMode::Paginated => Ok(()),
    }
}

pub(super) fn validate_page_size(page_size: usize) -> ThreadStoreResult<()> {
    if page_size == 0 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "page size must be positive".to_owned(),
        });
    }
    Ok(())
}

pub(super) fn invalid_cursor(cursor: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    }
}

/// `scope.kind` discriminant for the turns/items cursor (spec §2.24).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(crate) enum CursorScope {
    Turns,
    ItemsByCreatedAtOrdinal,
    ItemsByUpdatedAtOrdinal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoryCursor {
    pub(crate) requested_thread_id: ThreadId,
    pub(crate) rollout_ordinal: u64,
    pub(crate) include_anchor: bool,
    pub(crate) scope: CursorScope,
}

pub(crate) fn parse_history_cursor(
    cursor: Option<&str>,
    requested_thread_id: ThreadId,
    scope: CursorScope,
) -> ThreadStoreResult<Option<HistoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let value: HistoryCursor = serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    if value.requested_thread_id != requested_thread_id || value.scope != scope {
        return Err(invalid_cursor(cursor));
    }
    Ok(Some(value))
}

pub(crate) fn serialize_history_cursor(
    requested_thread_id: ThreadId,
    scope: CursorScope,
    rollout_ordinal: u64,
    include_anchor: bool,
) -> ThreadStoreResult<String> {
    serde_json::to_string(&HistoryCursor {
        requested_thread_id,
        rollout_ordinal,
        include_anchor,
        scope,
    })
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to serialize cursor: {err}"),
    })
}

/// Applies the cursor's `>`/`>=`/`<`/`<=` ordinal bound within one already
/// ascending-by-ordinal candidate list.
fn cursor_bound(ordinal: u64, direction: SortDirection, cursor: Option<&HistoryCursor>) -> bool {
    let Some(cursor) = cursor else {
        return true;
    };
    match (direction, cursor.include_anchor) {
        (SortDirection::Asc, true) => ordinal >= cursor.rollout_ordinal,
        (SortDirection::Asc, false) => ordinal > cursor.rollout_ordinal,
        (SortDirection::Desc, true) => ordinal <= cursor.rollout_ordinal,
        (SortDirection::Desc, false) => ordinal < cursor.rollout_ordinal,
    }
}

async fn scan_turns(
    store: &AntflyThreadStore,
    segment: Segment,
) -> ThreadStoreResult<Vec<TurnDoc>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT * FROM codex_thread_turns WHERE thread_id = $1 AND rollout_ordinal >= $2 \
             AND ($3::bigint IS NULL OR rollout_ordinal < $3) ORDER BY rollout_ordinal ASC",
            sql_params![
                segment.thread_id.to_string(),
                segment.start_ordinal as i64,
                segment.end_ordinal.map(|end| end as i64)
            ],
        )
        .await
        .map_err(internal)?;
    rows.iter().map(TurnDoc::from_row).collect()
}

/// Whether `turn_id` is hidden because a strictly newer segment has its own
/// turn with the same id (revert/re-run dedup, spec §2.24).
async fn hidden_by_newer_segment(
    store: &AntflyThreadStore,
    newer_segments: &[Segment],
    turn_id: &str,
) -> ThreadStoreResult<bool> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    for segment in newer_segments {
        let present = sql
            .fetch_optional(
                "SELECT 1 AS present FROM codex_thread_turns WHERE thread_id = $1 AND turn_id = $2",
                sql_params![segment.thread_id.to_string(), turn_id],
            )
            .await
            .map_err(internal)?
            .is_some();
        if present {
            return Ok(true);
        }
    }
    Ok(false)
}

fn turn_doc_to_stored(
    turn: TurnDoc,
    items: Vec<StoredThreadItem>,
    items_view: StoredTurnItemsView,
) -> StoredTurn {
    StoredTurn {
        turn_id: turn.turn_id,
        root_turn_id: turn.root_turn_id,
        items,
        items_view,
        status: turn.status,
        error: turn.error,
        started_at: turn.started_at,
        completed_at: turn.completed_at,
        duration_ms: turn.duration_ms,
    }
}

async fn load_summary_item(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
) -> ThreadStoreResult<Option<StoredThreadItem>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT * FROM codex_thread_items WHERE thread_id = $1 AND turn_id = $2 AND item_id = $3",
            sql_params![thread_id.to_string(), turn_id, item_id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| ItemDoc::from_row(&row).map(item_doc_to_stored))
        .transpose()
}

fn item_doc_to_stored(item: ItemDoc) -> StoredThreadItem {
    StoredThreadItem {
        turn_id: item.turn_id,
        item_id: item.item_id,
        updated_at_ordinal: item.updated_at_ordinal,
        created_at_ms: item.created_at_ms,
        started_at_ms: item.started_at_ms,
        completed_at_ms: item.completed_at_ms,
        item_json: item.item_json.to_string().into_bytes(),
    }
}

pub(super) async fn list_turns(
    store: &AntflyThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<TurnPage> {
    validate_paginated(
        store,
        params.thread_id,
        params.include_archived,
        "list_turns",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let segments = resolve_lineage(store, params.thread_id).await?;
    let cursor = parse_history_cursor(
        params.cursor.as_deref(),
        params.thread_id,
        CursorScope::Turns,
    )?;
    let cursor_segment = match &cursor {
        Some(cursor) => Some(
            segment_index_for_ordinal(&segments, cursor.rollout_ordinal)
                .ok_or_else(|| invalid_cursor("position outside thread lineage"))?,
        ),
        None => None,
    };
    let order = segment_order(segments.len(), params.sort_direction, cursor_segment);
    let mut rows: Vec<(usize, TurnDoc)> = Vec::new();
    'outer: for segment_index in order {
        let segment = segments[segment_index];
        let segment_cursor = if Some(segment_index) == cursor_segment {
            cursor.as_ref()
        } else {
            None
        };
        let mut candidates = scan_turns(store, segment).await?;
        candidates.retain(|turn| {
            cursor_bound(turn.rollout_ordinal, params.sort_direction, segment_cursor)
        });
        if params.sort_direction == SortDirection::Desc {
            candidates.reverse();
        }
        for turn in candidates {
            if hidden_by_newer_segment(store, &segments[segment_index + 1..], &turn.turn_id).await?
            {
                continue;
            }
            rows.push((segment_index, turn));
            if rows.len() == params.page_size + 1 {
                break 'outer;
            }
        }
    }
    let has_more = rows.len() > params.page_size;
    rows.truncate(params.page_size);
    let backwards_cursor = rows
        .first()
        .map(|(_, turn)| {
            serialize_history_cursor(
                params.thread_id,
                CursorScope::Turns,
                turn.rollout_ordinal,
                true,
            )
        })
        .transpose()?;
    let next_cursor = if has_more {
        rows.last()
            .map(|(_, turn)| {
                serialize_history_cursor(
                    params.thread_id,
                    CursorScope::Turns,
                    turn.rollout_ordinal,
                    false,
                )
            })
            .transpose()?
    } else {
        None
    };
    let mut turns = Vec::with_capacity(rows.len());
    for (segment_index, turn) in rows {
        let items = match params.items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary => {
                let thread_id = segments[segment_index].thread_id;
                let mut items = Vec::new();
                if let Some(item_id) = &turn.first_user_item_id
                    && let Some(item) =
                        load_summary_item(store, thread_id, &turn.turn_id, item_id).await?
                {
                    items.push(item);
                }
                if let Some(item_id) = &turn.final_agent_item_id
                    && let Some(item) =
                        load_summary_item(store, thread_id, &turn.turn_id, item_id).await?
                {
                    items.push(item);
                }
                items.sort_by_key(|item| item.updated_at_ordinal);
                items
            }
        };
        turns.push(turn_doc_to_stored(turn, items, params.items_view));
    }
    Ok(TurnPage {
        turns,
        next_cursor,
        backwards_cursor,
    })
}

async fn scan_items_created(
    store: &AntflyThreadStore,
    segment: Segment,
) -> ThreadStoreResult<Vec<ItemDoc>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT * FROM codex_thread_items WHERE thread_id = $1 AND rollout_ordinal >= $2 \
             AND ($3::bigint IS NULL OR rollout_ordinal < $3) ORDER BY rollout_ordinal ASC",
            sql_params![
                segment.thread_id.to_string(),
                segment.start_ordinal as i64,
                segment.end_ordinal.map(|end| end as i64)
            ],
        )
        .await
        .map_err(internal)?;
    rows.iter().map(ItemDoc::from_row).collect()
}

async fn scan_items_updated(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
    watermark: u64,
) -> ThreadStoreResult<Vec<ItemDoc>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT * FROM codex_thread_items WHERE thread_id = $1 AND updated_at_ordinal > $2 \
             ORDER BY updated_at_ordinal ASC",
            sql_params![thread_id.to_string(), watermark as i64],
        )
        .await
        .map_err(internal)?;
    rows.iter().map(ItemDoc::from_row).collect()
}

async fn find_item_anchor_ordinal(
    store: &AntflyThreadStore,
    segments: &[Segment],
    turn_id: &str,
    item_id: &str,
) -> ThreadStoreResult<Option<u64>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    for segment in segments {
        let row = sql
            .fetch_optional(
                "SELECT rollout_ordinal FROM codex_thread_items WHERE thread_id = $1 AND turn_id = $2 AND item_id = $3",
                sql_params![segment.thread_id.to_string(), turn_id, item_id],
            )
            .await
            .map_err(internal)?;
        if let Some(row) = row {
            return Ok(Some(row.i64("rollout_ordinal").map_err(internal)? as u64));
        }
    }
    Ok(None)
}

fn invalid_item_anchor() -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: "invalid item anchor".to_owned(),
    }
}

pub(super) async fn list_items(
    store: &AntflyThreadStore,
    params: ListItemsParams,
) -> ThreadStoreResult<ItemPage> {
    validate_paginated(
        store,
        params.thread_id,
        params.include_archived,
        "list_items",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let segments = resolve_lineage(store, params.thread_id).await?;

    let (cursor_token, anchor_item_id) = match &params.position {
        Some(ListItemsPosition::Cursor(cursor)) => (Some(cursor.as_str()), None),
        Some(ListItemsPosition::ItemAnchor { item_id }) => (None, Some(item_id.as_str())),
        None => (None, None),
    };
    if anchor_item_id.is_some()
        && (params.sort_key != ItemSortKey::CreatedAtOrdinal
            || params.after_updated_at_ordinal.is_some())
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: "item anchors require creation-order paging without an update watermark"
                .to_owned(),
        });
    }
    if params.after_updated_at_ordinal.is_some() && segments.len() > 1 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "incremental item replay is not supported for forked threads".to_owned(),
        });
    }

    if matches!(params.sort_key, ItemSortKey::UpdatedAtOrdinal) {
        let Some(watermark) = params.after_updated_at_ordinal else {
            return Err(ThreadStoreError::InvalidRequest {
                message: "update-ordinal item sorting requires an update watermark".to_owned(),
            });
        };
        let [segment] = segments.as_slice() else {
            return Err(ThreadStoreError::Internal {
                message: "update-ordinal item paging requires one rollout segment".to_owned(),
            });
        };
        let cursor = parse_history_cursor(
            cursor_token,
            params.thread_id,
            CursorScope::ItemsByUpdatedAtOrdinal,
        )?;
        let mut candidates = scan_items_updated(store, segment.thread_id, watermark).await?;
        if let Some(turn_id) = params.turn_id.as_deref() {
            candidates.retain(|item| item.turn_id == turn_id);
        }
        candidates.retain(|item| {
            cursor_bound(
                item.updated_at_ordinal,
                params.sort_direction,
                cursor.as_ref(),
            )
        });
        if params.sort_direction == SortDirection::Desc {
            candidates.reverse();
        }
        return finish_item_page(
            params.thread_id,
            CursorScope::ItemsByUpdatedAtOrdinal,
            candidates,
            params.page_size,
            |item| item.updated_at_ordinal,
        );
    }

    let cursor = if let Some(item_id) = anchor_item_id {
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: "turnId is required when cursor is an item anchor".to_owned(),
            })?;
        if item_id.is_empty() {
            return Err(invalid_item_anchor());
        }
        let ordinal = find_item_anchor_ordinal(store, &segments, turn_id, item_id)
            .await?
            .ok_or_else(invalid_item_anchor)?;
        Some(HistoryCursor {
            requested_thread_id: params.thread_id,
            rollout_ordinal: ordinal,
            include_anchor: false,
            scope: CursorScope::ItemsByCreatedAtOrdinal,
        })
    } else {
        parse_history_cursor(
            cursor_token,
            params.thread_id,
            CursorScope::ItemsByCreatedAtOrdinal,
        )?
    };
    let cursor_segment = match &cursor {
        Some(cursor) => Some(
            segment_index_for_ordinal(&segments, cursor.rollout_ordinal)
                .ok_or_else(|| invalid_cursor("position outside thread lineage"))?,
        ),
        None => None,
    };
    let order = segment_order(segments.len(), params.sort_direction, cursor_segment);
    let mut rows: Vec<ItemDoc> = Vec::new();
    'outer: for segment_index in order {
        let segment = segments[segment_index];
        let segment_cursor = if Some(segment_index) == cursor_segment {
            cursor.as_ref()
        } else {
            None
        };
        let mut candidates = scan_items_created(store, segment).await?;
        if let Some(turn_id) = params.turn_id.as_deref() {
            candidates.retain(|item| item.turn_id == turn_id);
        }
        if let Some(watermark) = params.after_updated_at_ordinal {
            candidates.retain(|item| item.updated_at_ordinal > watermark);
        }
        candidates.retain(|item| {
            cursor_bound(item.rollout_ordinal, params.sort_direction, segment_cursor)
        });
        if params.sort_direction == SortDirection::Desc {
            candidates.reverse();
        }
        for item in candidates {
            rows.push(item);
            if rows.len() == params.page_size + 1 {
                break 'outer;
            }
        }
    }
    finish_item_page(
        params.thread_id,
        CursorScope::ItemsByCreatedAtOrdinal,
        rows,
        params.page_size,
        |item| item.rollout_ordinal,
    )
}

fn finish_item_page(
    thread_id: ThreadId,
    scope: CursorScope,
    mut rows: Vec<ItemDoc>,
    page_size: usize,
    ordinal_of: impl Fn(&ItemDoc) -> u64,
) -> ThreadStoreResult<ItemPage> {
    let has_more = rows.len() > page_size;
    rows.truncate(page_size);
    let backwards_cursor = rows
        .first()
        .map(|item| serialize_history_cursor(thread_id, scope.clone(), ordinal_of(item), true))
        .transpose()?;
    let next_cursor = if has_more {
        rows.last()
            .map(|item| serialize_history_cursor(thread_id, scope, ordinal_of(item), false))
            .transpose()?
    } else {
        None
    };
    Ok(ItemPage {
        items: rows.into_iter().map(item_doc_to_stored).collect(),
        next_cursor,
        backwards_cursor,
    })
}

/// Timeline cursor (spec §2.24): `{"threadId","position","kind","id"}`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimelineCursor {
    thread_id: ThreadId,
    position: u64,
    kind: u8,
    id: String,
}

fn entry_key(entry: &ThreadTimelineEntry) -> (u64, u8, String) {
    match entry {
        ThreadTimelineEntry::TurnStarted {
            position, turn_id, ..
        } => (*position, 0, turn_id.clone()),
        ThreadTimelineEntry::Item { position, item, .. } => (*position, 1, item.id().to_owned()),
        ThreadTimelineEntry::Realtime { position, item } => (*position, 2, item.id.clone()),
        ThreadTimelineEntry::TurnCompleted {
            position, turn_id, ..
        } => (*position, 3, turn_id.clone()),
    }
}

async fn realtime_in_segment(
    store: &AntflyThreadStore,
    segment: Segment,
) -> ThreadStoreResult<Vec<(u64, RealtimeItem)>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT rollout_ordinal, item_json FROM codex_thread_realtime_items WHERE thread_id = $1 \
             AND rollout_ordinal >= $2 AND ($3::bigint IS NULL OR rollout_ordinal < $3) ORDER BY rollout_ordinal ASC",
            sql_params![
                segment.thread_id.to_string(),
                segment.start_ordinal as i64,
                segment.end_ordinal.map(|end| end as i64)
            ],
        )
        .await
        .map_err(internal)?;
    rows.iter()
        .map(|row: &SqlRow| {
            let ordinal = row.i64("rollout_ordinal").map_err(internal)? as u64;
            let item: RealtimeItem = serde_json::from_value(
                row.json("item_json").map_err(internal)?,
            )
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to deserialize realtime item: {err}"),
            })?;
            Ok((ordinal, item))
        })
        .collect()
}

pub(super) async fn list_timeline(
    store: &AntflyThreadStore,
    params: ListTimelineParams,
) -> ThreadStoreResult<TimelinePage> {
    validate_paginated(store, params.thread_id, false, "thread/timeline/list").await?;
    validate_page_size(params.page_size)?;
    let segments = resolve_lineage(store, params.thread_id).await?;

    let cursor = params
        .cursor
        .as_deref()
        .map(serde_json::from_str::<TimelineCursor>)
        .transpose()
        .map_err(|_| ThreadStoreError::InvalidRequest {
            message: "invalid thread timeline cursor".to_owned(),
        })?
        .map(|cursor| {
            if cursor.thread_id != params.thread_id {
                Err(ThreadStoreError::InvalidRequest {
                    message: "thread timeline cursor belongs to another thread".to_owned(),
                })
            } else {
                Ok(cursor)
            }
        })
        .transpose()?;
    let cursor_key = cursor
        .as_ref()
        .map(|cursor| (cursor.position, cursor.kind, cursor.id.clone()));

    let mut entries: Vec<ThreadTimelineEntry> = Vec::new();
    for segment in segments.iter().rev() {
        let turns = scan_turns(store, *segment).await?;
        for turn in &turns {
            entries.push(ThreadTimelineEntry::TurnStarted {
                position: turn.rollout_ordinal,
                turn_id: turn.turn_id.clone(),
                started_at: turn.started_at,
            });
            if let Some(end_ordinal) = turn.rollout_end_ordinal {
                entries.push(ThreadTimelineEntry::TurnCompleted {
                    position: end_ordinal,
                    turn_id: turn.turn_id.clone(),
                    status: stored_status_to_v2(turn.status),
                    error: turn.error.clone().map(stored_error_to_v2),
                    started_at: turn.started_at,
                    completed_at: turn.completed_at,
                    duration_ms: turn.duration_ms,
                });
            }
        }
        let items = scan_items_created(store, *segment).await?;
        for item in items {
            let thread_item: ThreadItem =
                serde_json::from_value(item.item_json).map_err(|err| {
                    ThreadStoreError::Internal {
                        message: format!("failed to deserialize stored thread item: {err}"),
                    }
                })?;
            entries.push(ThreadTimelineEntry::Item {
                position: item.rollout_ordinal,
                turn_id: item.turn_id,
                item: Box::new(thread_item),
            });
        }
        for (ordinal, item) in realtime_in_segment(store, *segment).await? {
            entries.push(ThreadTimelineEntry::Realtime {
                position: ordinal,
                item: ThreadRealtimeItem::from(item),
            });
        }
    }
    entries.sort_by_key(entry_key);
    if let Some(cursor_key) = &cursor_key {
        entries.retain(|entry| &entry_key(entry) < cursor_key);
    }
    let has_more = entries.len() > params.page_size;
    let start = entries
        .len()
        .saturating_sub(params.page_size.min(entries.len()));
    let page: Vec<ThreadTimelineEntry> = entries[start..].to_vec();
    let next_cursor = if has_more {
        page.first()
            .map(|entry| {
                let (position, kind, id) = entry_key(entry);
                serde_json::to_string(&TimelineCursor {
                    thread_id: params.thread_id,
                    position,
                    kind,
                    id,
                })
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to serialize cursor: {err}"),
                })
            })
            .transpose()?
    } else {
        None
    };

    let page_start_key = page.first().map(entry_key);
    let mut active_realtime_session_at_page_start = None;
    for segment in segments.iter().rev() {
        let mut realtime = realtime_in_segment(store, *segment).await?;
        realtime.sort_by_key(|(ordinal, _)| *ordinal);
        realtime.reverse();
        for (ordinal, item) in realtime {
            let key = (ordinal, 2u8, item.id.clone());
            if let Some(page_start_key) = &page_start_key
                && key >= *page_start_key
            {
                continue;
            }
            match item.content {
                codex_protocol::realtime::RealtimeItemContent::RealtimeSessionStarted => {
                    active_realtime_session_at_page_start = Some(item.realtime_session_id);
                }
                codex_protocol::realtime::RealtimeItemContent::RealtimeSessionClosed { .. } => {}
                _ => continue,
            }
            break;
        }
        if active_realtime_session_at_page_start.is_some() {
            break;
        }
    }

    // `page` is already oldest-first: it is the tail slice of `entries`,
    // which was sorted ascending above. Spec §2.24 returns the timeline in
    // ascending ("rollout") order, so no further reversal is needed here.
    Ok(TimelinePage {
        items: page,
        next_cursor,
        active_realtime_session_at_page_start,
    })
}

fn stored_status_to_v2(status: StoredTurnStatus) -> codex_app_server_protocol::TurnStatus {
    match status {
        StoredTurnStatus::Completed => codex_app_server_protocol::TurnStatus::Completed,
        StoredTurnStatus::Interrupted => codex_app_server_protocol::TurnStatus::Interrupted,
        StoredTurnStatus::Failed => codex_app_server_protocol::TurnStatus::Failed,
        StoredTurnStatus::InProgress => codex_app_server_protocol::TurnStatus::InProgress,
    }
}

fn stored_error_to_v2(error: crate::StoredTurnError) -> codex_app_server_protocol::TurnError {
    codex_app_server_protocol::TurnError {
        message: error.message,
        codex_error_info: error.codex_error_info,
        additional_details: error.additional_details,
        misalignment: None,
    }
}

/// Reverse-scans `thread_id`'s own lineage for the latest model-visible
/// context (spec §2.10): the newest `Compacted` item carrying both
/// `replacement_history` and `window_number`, plus everything newer, or the
/// full history if no such compaction exists. The thread's canonical
/// `SessionMeta` is always prepended.
pub(super) async fn load_latest_model_context_items(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let segments = resolve_lineage(store, thread_id).await?;
    let mut scan = ModelContextScan::default();
    let mut session_meta_item = None;
    'segments: for segment in segments.iter().rev() {
        // Bound the scan to this segment's own visible range: an ancestor
        // thread may have kept receiving turns after the point it was
        // forked from, and those newer-than-the-fork items must not leak
        // into this thread's model context.
        let items = store
            .load_items_before(segment.thread_id, segment.end_ordinal)
            .await?;
        for item in items.into_iter().rev() {
            if let RolloutItem::SessionMeta(_) = &item {
                if segment.thread_id == thread_id {
                    session_meta_item = Some(item);
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
    let session_meta = session_meta_item.ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    items.insert(0, session_meta);
    Ok(items)
}
