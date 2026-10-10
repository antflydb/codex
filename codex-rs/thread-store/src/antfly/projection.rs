//! Turn/item/realtime projection maintained alongside raw rollout items for
//! `Paginated` threads (spec §2.25), in `codex_thread_turns` /
//! `codex_thread_items` / `codex_thread_realtime_items` /
//! `codex_thread_history_projection_state`. [`apply_batch`] runs in one SQL
//! transaction per call with every batch of newly ordinal-assigned items, so
//! a read within the same batch always sees that batch's own earlier writes
//! (ordinary read-your-own-writes transaction semantics - no separate cache
//! is needed).
//!
//! This reimplements `codex_app_server_protocol::project_rollout_line`'s
//! consumer (`local/thread_history_materialization.rs` /
//! `local/thread_history.rs`) over SQL rows instead of SQLite rows.

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnMetadata;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus as V2TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::ThreadId;
use codex_protocol::models::MessagePhase;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::AntflyThreadStore;
use super::internal;
use crate::StoredTurnError;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
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

    pub(crate) fn from_row(row: &SqlRow) -> ThreadStoreResult<Self> {
        Ok(Self {
            turn_id: row.string("turn_id").map_err(internal)?,
            root_turn_id: row.opt_string("root_turn_id").map_err(internal)?,
            status: parse_status(&row.string("status").map_err(internal)?)?,
            error: row
                .opt_json("error_json")
                .map_err(internal)?
                .map(serde_json::from_value)
                .transpose()
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("invalid turn error: {err}"),
                })?,
            started_at: row.opt_i64("started_at").map_err(internal)?,
            completed_at: row.opt_i64("completed_at").map_err(internal)?,
            duration_ms: row.opt_i64("duration_ms").map_err(internal)?,
            rollout_ordinal: row.i64("rollout_ordinal").map_err(internal)? as u64,
            rollout_end_ordinal: row
                .opt_i64("rollout_end_ordinal")
                .map_err(internal)?
                .map(|v| v as u64),
            first_user_item_id: row.opt_string("first_user_item_id").map_err(internal)?,
            final_agent_item_id: row.opt_string("final_agent_item_id").map_err(internal)?,
            latest_unphased_agent_item_id: row
                .opt_string("latest_unphased_agent_item_id")
                .map_err(internal)?,
        })
    }
}

fn parse_status(text: &str) -> ThreadStoreResult<StoredTurnStatus> {
    Ok(match text {
        "completed" => StoredTurnStatus::Completed,
        "interrupted" => StoredTurnStatus::Interrupted,
        "failed" => StoredTurnStatus::Failed,
        "in_progress" => StoredTurnStatus::InProgress,
        other => {
            return Err(ThreadStoreError::Internal {
                message: format!("invalid turn status: {other}"),
            });
        }
    })
}

fn status_text(status: StoredTurnStatus) -> &'static str {
    match status {
        StoredTurnStatus::Completed => "completed",
        StoredTurnStatus::Interrupted => "interrupted",
        StoredTurnStatus::Failed => "failed",
        StoredTurnStatus::InProgress => "in_progress",
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

impl ItemDoc {
    pub(crate) fn from_row(row: &SqlRow) -> ThreadStoreResult<Self> {
        Ok(Self {
            turn_id: row.string("turn_id").map_err(internal)?,
            item_id: row.string("item_id").map_err(internal)?,
            item_type: row.string("item_type").map_err(internal)?,
            item_json: row.json("item_json").map_err(internal)?,
            rollout_ordinal: row.i64("rollout_ordinal").map_err(internal)? as u64,
            updated_at_ordinal: row.i64("updated_at_ordinal").map_err(internal)? as u64,
            created_at_ms: row.i64("created_at_ms").map_err(internal)?,
            started_at_ms: row.opt_i64("started_at_ms").map_err(internal)?,
            completed_at_ms: row.opt_i64("completed_at_ms").map_err(internal)?,
        })
    }
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

async fn load_turn(
    tx: &mut SqlTx,
    thread_id: ThreadId,
    turn_id: &str,
) -> ThreadStoreResult<Option<TurnDoc>> {
    let row = tx
        .fetch_optional(
            "SELECT * FROM codex_thread_turns WHERE thread_id = $1 AND turn_id = $2",
            sql_params![thread_id.to_string(), turn_id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| TurnDoc::from_row(&row)).transpose()
}

async fn load_item(
    tx: &mut SqlTx,
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
) -> ThreadStoreResult<Option<ItemDoc>> {
    let row = tx
        .fetch_optional(
            "SELECT * FROM codex_thread_items WHERE thread_id = $1 AND turn_id = $2 AND item_id = $3",
            sql_params![thread_id.to_string(), turn_id, item_id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| ItemDoc::from_row(&row)).transpose()
}

async fn put_turn(tx: &mut SqlTx, thread_id: ThreadId, turn: &TurnDoc) -> ThreadStoreResult<()> {
    let error_json = turn
        .error
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize turn error: {err}"),
        })?;
    tx.execute(
        "INSERT INTO codex_thread_turns ( \
            thread_id, turn_id, rollout_ordinal, rollout_end_ordinal, root_turn_id, status, \
            error_json, started_at, completed_at, duration_ms, first_user_item_id, \
            final_agent_item_id, latest_unphased_agent_item_id \
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (thread_id, turn_id) DO UPDATE SET \
            rollout_end_ordinal = excluded.rollout_end_ordinal, \
            root_turn_id = excluded.root_turn_id, \
            status = excluded.status, \
            error_json = excluded.error_json, \
            started_at = excluded.started_at, \
            completed_at = excluded.completed_at, \
            duration_ms = excluded.duration_ms, \
            first_user_item_id = excluded.first_user_item_id, \
            final_agent_item_id = excluded.final_agent_item_id, \
            latest_unphased_agent_item_id = excluded.latest_unphased_agent_item_id",
        sql_params![
            thread_id.to_string(),
            turn.turn_id.clone(),
            turn.rollout_ordinal as i64,
            turn.rollout_end_ordinal.map(|v| v as i64),
            turn.root_turn_id.clone(),
            status_text(turn.status),
            error_json,
            turn.started_at,
            turn.completed_at,
            turn.duration_ms,
            turn.first_user_item_id.clone(),
            turn.final_agent_item_id.clone(),
            turn.latest_unphased_agent_item_id.clone()
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

async fn put_item(tx: &mut SqlTx, thread_id: ThreadId, item: &ItemDoc) -> ThreadStoreResult<()> {
    tx.execute(
        "INSERT INTO codex_thread_items ( \
            thread_id, turn_id, item_id, rollout_ordinal, updated_at_ordinal, item_type, \
            item_json, created_at_ms, started_at_ms, completed_at_ms \
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         ON CONFLICT (thread_id, turn_id, item_id) DO UPDATE SET \
            updated_at_ordinal = excluded.updated_at_ordinal, \
            item_type = excluded.item_type, \
            item_json = excluded.item_json, \
            started_at_ms = excluded.started_at_ms, \
            completed_at_ms = excluded.completed_at_ms",
        sql_params![
            thread_id.to_string(),
            item.turn_id.clone(),
            item.item_id.clone(),
            item.rollout_ordinal as i64,
            item.updated_at_ordinal as i64,
            item.item_type.clone(),
            item.item_json.clone(),
            item.created_at_ms,
            item.started_at_ms,
            item.completed_at_ms
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

async fn apply_turn_metadata(
    tx: &mut SqlTx,
    thread_id: ThreadId,
    ordinal: u64,
    metadata: ThreadHistoryTurnMetadata,
) -> ThreadStoreResult<()> {
    let existing = load_turn(tx, thread_id, &metadata.turn_id).await?;
    let status = turn_status_from_v2(metadata.status);
    let is_terminal = matches!(
        status,
        StoredTurnStatus::Completed | StoredTurnStatus::Interrupted | StoredTurnStatus::Failed
    );
    let turn = match existing {
        // A terminal turn is final and never reopened.
        Some(existing) if existing.is_terminal() => return Ok(()),
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
            turn
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
            turn
        }
    };
    put_turn(tx, thread_id, &turn).await
}

async fn apply_item_change(
    tx: &mut SqlTx,
    thread_id: ThreadId,
    ordinal: u64,
    fallback_created_at_ms: i64,
    change: ThreadHistoryItemChange,
) -> ThreadStoreResult<()> {
    let item_id = change.item.id().to_owned();
    let item_json =
        serde_json::to_value(&change.item).map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize thread item: {err}"),
        })?;
    let item_type = item_json
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let existing = load_item(tx, thread_id, &change.turn_id, &item_id).await?;
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
    put_item(tx, thread_id, &item).await?;
    backfill_turn_summary_ids(tx, thread_id, &change.turn_id, &item_id, &change.item).await
}

/// Mirrors the simplified, incremental version of the summary-id backfill
/// described in spec §2.25: the first user message sets
/// `first_user_item_id` once; an agent message with `final_answer` phase
/// sets `final_agent_item_id`; both only while the turn is still open.
async fn backfill_turn_summary_ids(
    tx: &mut SqlTx,
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    item: &ThreadItem,
) -> ThreadStoreResult<()> {
    let Some(mut turn) = load_turn(tx, thread_id, turn_id).await? else {
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
        put_turn(tx, thread_id, &turn).await?;
    }
    Ok(())
}

async fn put_realtime(
    tx: &mut SqlTx,
    thread_id: ThreadId,
    ordinal: u64,
    created_at_ms: i64,
    item: &codex_protocol::realtime::RealtimeItem,
) -> ThreadStoreResult<()> {
    let item_json = serde_json::to_value(item).map_err(|err| ThreadStoreError::Internal {
        message: format!("serialize realtime item: {err}"),
    })?;
    let item_type = item_json
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    tx.execute(
        "INSERT INTO codex_thread_realtime_items (thread_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json) \
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (thread_id, item_id) DO NOTHING",
        sql_params![thread_id.to_string(), item.id.clone(), ordinal as i64, created_at_ms, item_type, item_json],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

/// Applies the projection changes for a batch of newly ordinal-assigned
/// items, in one SQL transaction. `items` must be in rollout order with
/// ordinals `first_ordinal, first_ordinal + 1, ...`. `now` is used as the
/// fallback timestamp for records whose event does not itself carry one (we
/// are the live writer, so every record in a batch shares one wall-clock
/// moment).
pub(super) async fn apply_batch(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
    subagent_history_start_ordinal: Option<u64>,
    first_ordinal: u64,
    items: &[RolloutItem],
    now: DateTime<Utc>,
) -> ThreadStoreResult<()> {
    let timestamp = now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let now_ms = now.timestamp_millis();
    let sql = store.antfly().sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let mut next_ordinal = first_ordinal;
    for (offset, item) in items.iter().enumerate() {
        let ordinal = first_ordinal + offset as u64;
        next_ordinal = ordinal + 1;
        let inherited = subagent_history_start_ordinal.is_some_and(|start| ordinal < start);
        if inherited {
            continue;
        }
        if let RolloutItem::RealtimeItem(realtime) = item {
            put_realtime(&mut tx, thread_id, ordinal, now_ms, realtime).await?;
            continue;
        }
        let line = RolloutLine {
            timestamp: timestamp.clone(),
            ordinal: Some(ordinal),
            item: item.clone(),
        };
        let changes = project_rollout_line(&line);
        for metadata in changes.changed_turns {
            apply_turn_metadata(&mut tx, thread_id, ordinal, metadata).await?;
        }
        for change in changes.changed_items {
            apply_item_change(&mut tx, thread_id, ordinal, now_ms, change).await?;
        }
    }
    tx.execute(
        "INSERT INTO codex_thread_history_projection_state (thread_id, next_rollout_ordinal) \
         VALUES ($1, $2) ON CONFLICT (thread_id) DO UPDATE SET next_rollout_ordinal = excluded.next_rollout_ordinal",
        sql_params![thread_id.to_string(), next_ordinal as i64],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)
}
