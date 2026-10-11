//! Antfly backend for thread read receipts, backed by the
//! `codex_thread_read_receipts` SQL table (see `codex-antfly/src/schema.rs`,
//! migration 22). Mirrors `state/src/runtime/thread_read_state.rs`: receipts
//! have no foreign key so rebuilding thread metadata keeps them, and only
//! thread deletion removes them. Publish and update read, decide and write
//! inside one transaction instead of SQLite's conditional upsert.

use std::collections::HashMap;
use std::sync::Arc;

use codex_antfly::Antfly;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use uuid::Uuid;

use super::internal;
use crate::ReadStateOperation;
use crate::ReadStateUpdate;
use crate::ThreadReadState;

struct Receipt {
    first_unread_turn: Option<String>,
    revision: String,
    last_published_turn: Option<String>,
}

/// The thread's receipt, `Some(None)` for a thread without one, or `None`
/// when the thread has no durable metadata row.
async fn receipt(tx: &mut SqlTx, id: ThreadId) -> anyhow::Result<Option<Option<Receipt>>> {
    let Some(row) = tx
        .fetch_optional(
            "SELECT t.id AS id, r.first_unread_turn AS first_unread_turn, r.revision AS revision, \
             r.last_published_turn AS last_published_turn \
             FROM codex_threads t LEFT JOIN codex_thread_read_receipts r ON r.thread_id = t.id \
             WHERE t.id = $1",
            sql_params![id.to_string()],
        )
        .await
        .map_err(internal)?
    else {
        return Ok(None);
    };
    let Some(revision) = row.opt_string("revision").map_err(internal)? else {
        return Ok(Some(None));
    };
    Ok(Some(Some(Receipt {
        first_unread_turn: row.opt_string("first_unread_turn").map_err(internal)?,
        revision,
        last_published_turn: row.opt_string("last_published_turn").map_err(internal)?,
    })))
}

fn read_state(id: ThreadId, receipt: Option<Receipt>) -> ThreadReadState {
    match receipt {
        Some(receipt) => ThreadReadState {
            first_unread_turn: receipt.first_unread_turn,
            revision: receipt.revision,
        },
        // Bind the initial revision to the thread so it cannot edit another
        // thread that has no receipt yet.
        None => ThreadReadState {
            first_unread_turn: None,
            revision: format!("initial:{id}"),
        },
    }
}

async fn upsert(
    tx: &mut SqlTx,
    id: ThreadId,
    first_unread_turn: Option<&str>,
    revision: &str,
    last_published_turn: Option<&str>,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO codex_thread_read_receipts \
         (thread_id, first_unread_turn, revision, last_published_turn) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (thread_id) DO UPDATE SET first_unread_turn = excluded.first_unread_turn, \
         revision = excluded.revision, last_published_turn = excluded.last_published_turn",
        sql_params![
            id.to_string(),
            first_unread_turn.map(str::to_owned),
            revision.to_owned(),
            last_published_turn.map(str::to_owned)
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn thread_read_states(
    antfly: &Arc<Antfly>,
    ids: &[ThreadId],
) -> anyhow::Result<HashMap<ThreadId, ThreadReadState>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let mut states = HashMap::new();
    for &id in ids {
        if let Some(found) = receipt(&mut tx, id).await? {
            states.insert(id, read_state(id, found));
        }
    }
    let _ = tx.rollback().await;
    Ok(states)
}

pub(crate) async fn publish_thread_attention(
    antfly: &Arc<Antfly>,
    id: ThreadId,
    turn_id: &str,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let result = async {
        let Some(existing) = receipt(&mut tx, id).await? else {
            return Ok(false);
        };
        let first_unread_turn = match &existing {
            Some(receipt) if receipt.last_published_turn.as_deref() == Some(turn_id) => {
                return Ok(false);
            }
            Some(receipt) => receipt.first_unread_turn.clone(),
            None => None,
        };
        let first_unread_turn = first_unread_turn.unwrap_or_else(|| turn_id.to_owned());
        upsert(
            &mut tx,
            id,
            Some(&first_unread_turn),
            &Uuid::new_v4().to_string(),
            Some(turn_id),
        )
        .await?;
        Ok(true)
    }
    .await;
    finish(tx, result).await
}

pub(crate) async fn update_thread_read_state(
    antfly: &Arc<Antfly>,
    id: ThreadId,
    expected_revision: &str,
    operation: ReadStateOperation,
) -> anyhow::Result<ReadStateUpdate> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let result = async {
        let Some(existing) = receipt(&mut tx, id).await? else {
            return Ok(ReadStateUpdate::Unavailable);
        };
        let last_published_turn = existing
            .as_ref()
            .and_then(|receipt| receipt.last_published_turn.clone());
        let before = read_state(id, existing);
        if before.revision != expected_revision {
            return Ok(ReadStateUpdate::Conflict(before));
        }
        let first_unread = match operation {
            ReadStateOperation::Read => None,
            ReadStateOperation::Unread => Some(""),
        };
        let revision = Uuid::new_v4().to_string();
        upsert(
            &mut tx,
            id,
            first_unread,
            &revision,
            last_published_turn.as_deref(),
        )
        .await?;
        Ok(ReadStateUpdate::Applied(ThreadReadState {
            first_unread_turn: first_unread.map(str::to_owned),
            revision,
        }))
    }
    .await;
    finish(tx, result).await
}

async fn finish<T>(tx: SqlTx, result: anyhow::Result<T>) -> anyhow::Result<T> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(internal)?;
            Ok(value)
        }
        Err(err) => {
            let _ = tx.rollback().await;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::sql_test_support::AntflyRuntime;
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test(flavor = "multi_thread")]
    async fn attention_is_published_once_and_edits_compare_revisions() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let state = &harness.runtime;
        let id = ThreadId::new();
        let unknown = ThreadId::new();
        assert!(!state.publish_thread_attention(unknown, "turn-1").await?);
        harness.insert_thread(id).await;

        let initial = state.thread_read_states(&[id, unknown]).await?;
        assert_eq!(
            initial,
            HashMap::from([(
                id,
                ThreadReadState {
                    first_unread_turn: None,
                    revision: format!("initial:{id}"),
                },
            )])
        );

        assert!(state.publish_thread_attention(id, "turn-1").await?);
        assert!(!state.publish_thread_attention(id, "turn-1").await?);
        assert!(state.publish_thread_attention(id, "turn-2").await?);
        let unread = state.thread_read_states(&[id]).await?.remove(&id).unwrap();
        assert_eq!(unread.first_unread_turn.as_deref(), Some("turn-1"));

        let stale = state
            .update_thread_read_state(id, &format!("initial:{id}"), ReadStateOperation::Read)
            .await?;
        assert_eq!(stale, ReadStateUpdate::Conflict(unread.clone()));
        let ReadStateUpdate::Applied(read) = state
            .update_thread_read_state(id, &unread.revision, ReadStateOperation::Read)
            .await?
        else {
            panic!("expected the read mark to apply");
        };
        assert_eq!(read.first_unread_turn, None);
        // Marking read keeps the last delivered turn, so replaying it stays quiet.
        assert!(!state.publish_thread_attention(id, "turn-2").await?);

        let ReadStateUpdate::Applied(manual) = state
            .update_thread_read_state(id, &read.revision, ReadStateOperation::Unread)
            .await?
        else {
            panic!("expected the unread mark to apply");
        };
        assert_eq!(manual.first_unread_turn.as_deref(), Some(""));
        assert!(state.publish_thread_attention(id, "turn-3").await?);
        assert_eq!(
            state.thread_read_states(&[id]).await?[&id]
                .first_unread_turn
                .as_deref(),
            Some(""),
        );

        state.delete_threads_strict(&[id]).await?;
        assert_eq!(
            state
                .update_thread_read_state(id, &manual.revision, ReadStateOperation::Read)
                .await?,
            ReadStateUpdate::Unavailable,
        );
        let leftover = harness
            .antfly
            .sql()
            .await?
            .fetch_all(
                "SELECT thread_id FROM codex_thread_read_receipts WHERE thread_id = $1",
                sql_params![id.to_string()],
            )
            .await?;
        assert!(leftover.is_empty());
        harness.close().await;
        Ok(())
    }
}
