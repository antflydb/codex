//! Turn/item/realtime projection maintained alongside raw rollout items for
//! `Paginated` threads (spec §2.25). `build_writes` is called with every
//! batch of newly ordinal-assigned items and returns the extra Antfly writes
//! to fold into the same write call that persists the raw items, so the
//! projection can never diverge from the durable history.
//!
//! This reimplements `codex_app_server_protocol::project_rollout_line`'s
//! consumer (`local/thread_history_materialization.rs` /
//! `local/thread_history.rs`) over Antfly documents instead of SQLite rows.

use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Write;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnMetadata;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus as V2TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::models::MessagePhase;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::AntflyThreadStore;
use super::internal;
use super::keys;
use super::to_value;
use crate::StoredTurnError;
use crate::StoredTurnStatus;
use crate::ThreadStoreResult;

/// Everything stored about one projected turn.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TurnDoc {
    pub(crate) turn_id: String,
    pub(crate) root_turn_id: Option<String>,
    pub(crate) status: StoredTurnStatus,
    pub(crate) error: Option<StoredTurnError>,
    pub(crate) started_at: Option<i64>,
    pub(crate) completed_at: Option<i64>,
    pub(crate) duration_ms: Option<i64>,
    /// Ordinal of the turn's first record. Immutable once set.
    pub(crate) rollout_ordinal: u64,
    /// Ordinal of the record that made the turn terminal, if any.
    pub(crate) rollout_end_ordinal: Option<u64>,
    pub(crate) first_user_item_id: Option<String>,
    pub(crate) final_agent_item_id: Option<String>,
    /// Latest `AgentMessage` item with no phase, used to backfill
    /// `final_agent_item_id` for turns whose assistant text never carried an
    /// explicit `final_answer` phase.
    pub(crate) latest_unphased_agent_item_id: Option<String>,
}

impl TurnDoc {
    fn is_terminal(&self) -> bool {
        self.rollout_end_ordinal.is_some()
    }
}

/// Everything stored about one projected item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ItemDoc {
    pub(crate) turn_id: String,
    pub(crate) item_id: String,
    pub(crate) item_type: String,
    pub(crate) item_json: Value,
    /// Ordinal of the item's first projection. Immutable once set.
    pub(crate) rollout_ordinal: u64,
    pub(crate) updated_at_ordinal: u64,
    pub(crate) created_at_ms: i64,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) completed_at_ms: Option<i64>,
}

fn turn_status_from_v2(status: V2TurnStatus) -> StoredTurnStatus {
    match status {
        V2TurnStatus::Completed => StoredTurnStatus::Completed,
        V2TurnStatus::Interrupted => StoredTurnStatus::Interrupted,
        V2TurnStatus::Failed => StoredTurnStatus::Failed,
        V2TurnStatus::InProgress => StoredTurnStatus::InProgress,
    }
}

fn turn_error_from_v2(error: codex_app_server_protocol::TurnError) -> StoredTurnError {
    StoredTurnError {
        message: error.message,
        codex_error_info: error.codex_error_info,
        additional_details: error.additional_details,
    }
}

/// Mutable projection state for one write batch, backed by lazily-fetched
/// Antfly documents so repeated touches within a batch stay consistent.
struct Builder<'a> {
    store: &'a AntflyThreadStore,
    thread_id: codex_protocol::ThreadId,
    turns: HashMap<String, Option<TurnDoc>>,
    items: HashMap<(String, String), Option<ItemDoc>>,
    writes: Vec<Write>,
}

impl<'a> Builder<'a> {
    fn new(store: &'a AntflyThreadStore, thread_id: codex_protocol::ThreadId) -> Self {
        Self {
            store,
            thread_id,
            turns: HashMap::new(),
            items: HashMap::new(),
            writes: Vec::new(),
        }
    }

    async fn turn(&mut self, turn_id: &str) -> ThreadStoreResult<Option<TurnDoc>> {
        if let Some(existing) = self.turns.get(turn_id) {
            return Ok(existing.clone());
        }
        let loaded = match self
            .store
            .antfly()
            .get(keys::turn_id_key(self.thread_id, turn_id))
            .await
            .map_err(internal)?
        {
            Some(doc) => Some(super::from_value::<TurnDoc>(codex_antfly::strip_reserved(
                doc,
            ))?),
            None => None,
        };
        self.turns.insert(turn_id.to_owned(), loaded.clone());
        Ok(loaded)
    }

    async fn item(&mut self, turn_id: &str, item_id: &str) -> ThreadStoreResult<Option<ItemDoc>> {
        let cache_key = (turn_id.to_owned(), item_id.to_owned());
        if let Some(existing) = self.items.get(&cache_key) {
            return Ok(existing.clone());
        }
        let loaded = match self
            .store
            .antfly()
            .get(keys::item_id_key(self.thread_id, turn_id, item_id))
            .await
            .map_err(internal)?
        {
            Some(doc) => Some(super::from_value::<ItemDoc>(codex_antfly::strip_reserved(
                doc,
            ))?),
            None => None,
        };
        self.items.insert(cache_key, loaded.clone());
        Ok(loaded)
    }

    fn put_turn(&mut self, turn: TurnDoc, became_terminal: bool) -> ThreadStoreResult<()> {
        let doc = to_value(&turn)?;
        self.writes.push(Write::put(
            keys::turn_id_key(self.thread_id, &turn.turn_id),
            doc.clone(),
        ));
        self.writes.push(Write::put(
            keys::turn_by_start(self.thread_id, turn.rollout_ordinal, &turn.turn_id),
            doc.clone(),
        ));
        if became_terminal {
            let end_ordinal = turn.rollout_end_ordinal.unwrap_or(turn.rollout_ordinal);
            self.writes.push(Write::put(
                keys::turn_by_end(self.thread_id, end_ordinal, &turn.turn_id),
                doc,
            ));
        }
        self.turns.insert(turn.turn_id.clone(), Some(turn));
        Ok(())
    }

    fn put_item(
        &mut self,
        item: ItemDoc,
        previous_updated_at_ordinal: Option<u64>,
    ) -> ThreadStoreResult<()> {
        let doc = to_value(&item)?;
        self.writes.push(Write::put(
            keys::item_id_key(self.thread_id, &item.turn_id, &item.item_id),
            doc.clone(),
        ));
        self.writes.push(Write::put(
            keys::item_by_created(
                self.thread_id,
                item.rollout_ordinal,
                &item.turn_id,
                &item.item_id,
            ),
            doc.clone(),
        ));
        if let Some(previous) = previous_updated_at_ordinal
            && previous != item.updated_at_ordinal
        {
            self.writes.push(Write::delete(keys::item_by_updated(
                self.thread_id,
                previous,
                &item.turn_id,
                &item.item_id,
            )));
        }
        self.writes.push(Write::put(
            keys::item_by_updated(
                self.thread_id,
                item.updated_at_ordinal,
                &item.turn_id,
                &item.item_id,
            ),
            doc,
        ));
        let cache_key = (item.turn_id.clone(), item.item_id.clone());
        self.items.insert(cache_key, Some(item));
        Ok(())
    }

    async fn apply_turn_metadata(
        &mut self,
        ordinal: u64,
        metadata: ThreadHistoryTurnMetadata,
    ) -> ThreadStoreResult<()> {
        let existing = self.turn(&metadata.turn_id).await?;
        let status = turn_status_from_v2(metadata.status);
        let is_terminal = matches!(
            status,
            StoredTurnStatus::Completed | StoredTurnStatus::Interrupted | StoredTurnStatus::Failed
        );
        match existing {
            // A terminal turn is final and never reopened.
            Some(existing) if existing.is_terminal() => Ok(()),
            Some(mut turn) => {
                turn.root_turn_id = turn.root_turn_id.or(metadata.root_turn_id);
                turn.status = status;
                turn.error = metadata.error.map(turn_error_from_v2);
                turn.started_at = metadata.started_at.or(turn.started_at);
                turn.completed_at = metadata.completed_at.or(turn.completed_at);
                turn.duration_ms = metadata.duration_ms.or(turn.duration_ms);
                if is_terminal {
                    turn.rollout_end_ordinal = Some(ordinal);
                    if turn.final_agent_item_id.is_none() {
                        turn.final_agent_item_id = turn.latest_unphased_agent_item_id.clone();
                    }
                }
                self.put_turn(turn, is_terminal)
            }
            None => {
                let mut turn = TurnDoc {
                    turn_id: metadata.turn_id,
                    root_turn_id: metadata.root_turn_id,
                    status,
                    error: metadata.error.map(turn_error_from_v2),
                    started_at: metadata.started_at,
                    completed_at: metadata.completed_at,
                    duration_ms: metadata.duration_ms,
                    rollout_ordinal: ordinal,
                    rollout_end_ordinal: is_terminal.then_some(ordinal),
                    first_user_item_id: None,
                    final_agent_item_id: None,
                    latest_unphased_agent_item_id: None,
                };
                if is_terminal {
                    turn.final_agent_item_id = turn.latest_unphased_agent_item_id.clone();
                }
                self.put_turn(turn, is_terminal)
            }
        }
    }

    async fn apply_item_change(
        &mut self,
        ordinal: u64,
        fallback_created_at_ms: i64,
        change: ThreadHistoryItemChange,
    ) -> ThreadStoreResult<()> {
        let item_id = change.item.id().to_owned();
        let item_json = to_value(&change.item)?;
        let item_type = item_json
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let existing = self.item(&change.turn_id, &item_id).await?;
        let previous_updated_at_ordinal = existing.as_ref().map(|item| item.updated_at_ordinal);
        let item = match existing {
            Some(mut item) => {
                item.item_type = item_type;
                item.item_json = item_json;
                item.updated_at_ordinal = ordinal;
                item.started_at_ms = item.started_at_ms.or(change.started_at_ms);
                item.completed_at_ms = item.completed_at_ms.or(change.completed_at_ms);
                item
            }
            None => ItemDoc {
                turn_id: change.turn_id.clone(),
                item_id: item_id.clone(),
                item_type,
                item_json,
                rollout_ordinal: ordinal,
                updated_at_ordinal: ordinal,
                created_at_ms: change.started_at_ms.unwrap_or(fallback_created_at_ms),
                started_at_ms: change.started_at_ms,
                completed_at_ms: change.completed_at_ms,
            },
        };
        self.put_item(item, previous_updated_at_ordinal)?;
        self.backfill_turn_summary_ids(&change.turn_id, &item_id, &change.item)
            .await
    }

    /// Mirrors the simplified, incremental version of the summary-id backfill
    /// described in spec §2.25: the first user message sets
    /// `first_user_item_id` once; an agent message with `final_answer` phase
    /// sets `final_agent_item_id`; both only while the turn is still open.
    /// An unphased agent message is tracked so a turn that terminates without
    /// ever emitting a `final_answer` phase still gets a `final_agent_item_id`.
    async fn backfill_turn_summary_ids(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item: &ThreadItem,
    ) -> ThreadStoreResult<()> {
        let Some(mut turn) = self.turn(turn_id).await? else {
            return Ok(());
        };
        if turn.is_terminal() {
            return Ok(());
        }
        let mut changed = false;
        match item {
            ThreadItem::UserMessage { .. } if turn.first_user_item_id.is_none() => {
                turn.first_user_item_id = Some(item_id.to_owned());
                changed = true;
            }
            ThreadItem::UserMessage { .. } => {}
            ThreadItem::AgentMessage { phase, .. } => match phase {
                Some(MessagePhase::FinalAnswer) => {
                    turn.final_agent_item_id = Some(item_id.to_owned());
                    changed = true;
                }
                None => {
                    turn.latest_unphased_agent_item_id = Some(item_id.to_owned());
                    changed = true;
                }
                Some(_) => {}
            },
            _ => {}
        }
        if changed {
            self.put_turn(turn, /* became_terminal */ false)?;
        }
        Ok(())
    }

    fn put_realtime(
        &mut self,
        ordinal: u64,
        created_at_ms: i64,
        item: &codex_protocol::realtime::RealtimeItem,
    ) -> ThreadStoreResult<()> {
        let item_json = to_value(item)?;
        let item_type = item_json
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let doc = serde_json::json!({
            "item_id": item.id,
            "item_type": item_type,
            "rollout_ordinal": ordinal,
            "created_at_ms": created_at_ms,
            "item_json": item_json,
        });
        self.writes.push(Write::put(
            keys::realtime(self.thread_id, ordinal, &item.id),
            doc,
        ));
        Ok(())
    }
}

/// Builds the extra projection writes for a batch of newly ordinal-assigned
/// items, to be included in the same [`codex_antfly::Antfly::write`] call
/// that persists the raw items so the two never diverge.
///
/// `items` must be in rollout order with ordinals `first_ordinal,
/// first_ordinal + 1, ...`. `now` is used as the fallback timestamp for
/// records whose event does not itself carry one (we are the live writer, so
/// every record in a batch shares one wall-clock moment).
pub(super) async fn build_writes(
    store: &AntflyThreadStore,
    thread_id: codex_protocol::ThreadId,
    subagent_history_start_ordinal: Option<u64>,
    first_ordinal: u64,
    items: &[RolloutItem],
    now: DateTime<Utc>,
) -> ThreadStoreResult<Vec<Write>> {
    let mut builder = Builder::new(store, thread_id);
    let timestamp = now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let now_ms = now.timestamp_millis();
    for (offset, item) in items.iter().enumerate() {
        let ordinal = first_ordinal + offset as u64;
        let inherited = subagent_history_start_ordinal.is_some_and(|start| ordinal < start);
        if inherited {
            continue;
        }
        if let RolloutItem::RealtimeItem(realtime) = item {
            builder.put_realtime(ordinal, now_ms, realtime)?;
            continue;
        }
        let line = RolloutLine {
            timestamp: timestamp.clone(),
            ordinal: Some(ordinal),
            item: item.clone(),
        };
        let changes = project_rollout_line(&line);
        for metadata in changes.changed_turns {
            builder.apply_turn_metadata(ordinal, metadata).await?;
        }
        for change in changes.changed_items {
            builder.apply_item_change(ordinal, now_ms, change).await?;
        }
    }
    Ok(builder.writes)
}
