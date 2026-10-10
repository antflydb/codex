//! Antfly-backed thread attachments (`codex_thread_attachments`), the same
//! table `codex-thread-store`'s `AntflyThreadStore` uses
//! (`thread-store/src/antfly/attachments.rs`). Mirrors
//! `state/src/runtime/thread_attachments.rs`.

use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::internal;
use crate::AddThreadAttachmentOutcome;
use crate::MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES;
use crate::MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE;
use crate::MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES;
use crate::MAX_THREAD_ATTACHMENT_TYPE_BYTES;
use crate::MAX_THREAD_ATTACHMENTS_PER_THREAD;
use crate::RemoveThreadAttachmentOutcome;
use crate::ThreadAttachment;
use crate::ThreadAttachmentArchiveFilter;
use crate::ThreadAttachmentOwner;
use crate::ThreadAttachmentOwnerPage;
use crate::ThreadAttachmentPage;

fn validate_attachment_identity(attachment_type: &str, identity_key: &str) -> anyhow::Result<()> {
    if attachment_type.trim().is_empty() {
        anyhow::bail!("invalid thread attachment request: attachment type must not be empty");
    }
    if attachment_type.len() > MAX_THREAD_ATTACHMENT_TYPE_BYTES {
        anyhow::bail!(
            "invalid thread attachment request: attachment type exceeds {MAX_THREAD_ATTACHMENT_TYPE_BYTES} bytes"
        );
    }
    if identity_key.trim().is_empty() {
        anyhow::bail!(
            "invalid thread attachment request: attachment identity key must not be empty"
        );
    }
    if identity_key.len() > MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES {
        anyhow::bail!(
            "invalid thread attachment request: attachment identity key exceeds {MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES} bytes"
        );
    }
    Ok(())
}

fn attachment_from_row(row: &SqlRow) -> anyhow::Result<ThreadAttachment> {
    Ok(ThreadAttachment {
        id: row.string("id").map_err(internal)?,
        thread_id: ThreadId::from_string(&row.string("thread_id").map_err(internal)?)?,
        attachment_type: row.string("attachment_type").map_err(internal)?,
        identity_key: row.string("identity_key").map_err(internal)?,
        payload: row.json("payload").map_err(internal)?,
        created_at: row.i64("created_at").map_err(internal)?,
    })
}

pub(crate) async fn copy_thread_attachments(
    antfly: &Antfly,
    source_thread_id: ThreadId,
    destination_thread_id: ThreadId,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let exists = sql
        .fetch_optional(
            "SELECT 1 AS present FROM codex_threads WHERE id = $1",
            sql_params![destination_thread_id.to_string()],
        )
        .await
        .map_err(internal)?
        .is_some();
    if !exists {
        anyhow::bail!("thread not found: {destination_thread_id}");
    }
    let rows = sql
        .fetch_all(
            "SELECT attachment_type, identity_key, payload FROM codex_thread_attachments WHERE thread_id = $1 ORDER BY created_at, id",
            sql_params![source_thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    let created_at = chrono::Utc::now().timestamp();
    let mut tx = sql.begin().await.map_err(internal)?;
    for row in &rows {
        tx.execute(
            "INSERT INTO codex_thread_attachments (id, thread_id, attachment_type, identity_key, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
            sql_params![
                uuid::Uuid::now_v7().to_string(),
                destination_thread_id.to_string(),
                row.string("attachment_type").map_err(internal)?,
                row.string("identity_key").map_err(internal)?,
                row.json("payload").map_err(internal)?,
                created_at
            ],
        )
        .await
        .map_err(internal)?;
    }
    tx.commit().await.map_err(internal)
}

pub(crate) async fn add_thread_attachment(
    antfly: &Antfly,
    thread_id: ThreadId,
    attachment_type: &str,
    identity_key: &str,
    payload: &Value,
) -> anyhow::Result<AddThreadAttachmentOutcome> {
    validate_attachment_identity(attachment_type, identity_key)?;
    let serialized_payload = serde_json::to_string(payload)?;
    if serialized_payload.len() > MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES {
        anyhow::bail!(
            "invalid thread attachment request: attachment payload exceeds {MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES} bytes"
        );
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let thread_id_string = thread_id.to_string();
    let thread_exists = tx
        .fetch_optional(
            "SELECT 1 AS present FROM codex_threads WHERE id = $1",
            sql_params![thread_id_string.clone()],
        )
        .await
        .map_err(internal)?
        .is_some();
    if !thread_exists {
        anyhow::bail!("thread not found: {thread_id}");
    }
    let existing = tx
        .fetch_optional(
            "SELECT id, thread_id, attachment_type, identity_key, payload, created_at FROM codex_thread_attachments \
             WHERE thread_id = $1 AND attachment_type = $2 AND identity_key = $3",
            sql_params![thread_id_string.clone(), attachment_type, identity_key],
        )
        .await
        .map_err(internal)?;
    if let Some(existing) = existing {
        return Ok(AddThreadAttachmentOutcome::Existing(attachment_from_row(
            &existing,
        )?));
    }
    let identity_count = tx
        .fetch_optional(
            "SELECT COUNT(*) AS value FROM codex_thread_attachments WHERE thread_id = $1",
            sql_params![thread_id_string.clone()],
        )
        .await
        .map_err(internal)?
        .map(|row| row.i64("value"))
        .transpose()
        .map_err(internal)?
        .unwrap_or(0);
    if identity_count as usize >= MAX_THREAD_ATTACHMENTS_PER_THREAD {
        anyhow::bail!(
            "invalid thread attachment request: thread attachment identity count exceeds {MAX_THREAD_ATTACHMENTS_PER_THREAD}"
        );
    }
    let attachment = ThreadAttachment {
        id: uuid::Uuid::now_v7().to_string(),
        thread_id,
        attachment_type: attachment_type.to_string(),
        identity_key: identity_key.to_string(),
        payload: payload.clone(),
        created_at: chrono::Utc::now().timestamp(),
    };
    tx.execute(
        "INSERT INTO codex_thread_attachments (id, thread_id, attachment_type, identity_key, payload, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
        sql_params![attachment.id.clone(), thread_id_string, attachment_type, identity_key, serialized_payload, attachment.created_at],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(AddThreadAttachmentOutcome::Created(attachment))
}

pub(crate) async fn remove_thread_attachment(
    antfly: &Antfly,
    thread_id: ThreadId,
    attachment_type: &str,
    identity_key: &str,
) -> anyhow::Result<RemoveThreadAttachmentOutcome> {
    validate_attachment_identity(attachment_type, identity_key)?;
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let thread_id_string = thread_id.to_string();
    let thread_exists = tx
        .fetch_optional(
            "SELECT 1 AS present FROM codex_threads WHERE id = $1",
            sql_params![thread_id_string.clone()],
        )
        .await
        .map_err(internal)?
        .is_some();
    if !thread_exists {
        anyhow::bail!("thread not found: {thread_id}");
    }
    let removed = tx
        .fetch_optional(
            "DELETE FROM codex_thread_attachments WHERE thread_id = $1 AND attachment_type = $2 AND identity_key = $3 \
             RETURNING id, thread_id, attachment_type, identity_key, payload, created_at",
            sql_params![thread_id_string, attachment_type, identity_key],
        )
        .await
        .map_err(internal)?;
    let outcome = match removed {
        Some(row) => RemoveThreadAttachmentOutcome::Removed(attachment_from_row(&row)?),
        None => RemoveThreadAttachmentOutcome::NotFound,
    };
    tx.commit().await.map_err(internal)?;
    Ok(outcome)
}

#[derive(Serialize, Deserialize)]
struct AttachmentThreadsCursor {
    attachment_type: String,
    identity_key: String,
    archived: Option<bool>,
    thread_id: ThreadId,
}

pub(crate) async fn list_thread_attachment_threads(
    antfly: &Antfly,
    attachment_type: &str,
    identity_key: &str,
    archive_filter: ThreadAttachmentArchiveFilter,
    cursor: Option<&str>,
    limit: usize,
) -> anyhow::Result<ThreadAttachmentOwnerPage> {
    validate_attachment_identity(attachment_type, identity_key)?;
    if !(1..=MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE).contains(&limit) {
        anyhow::bail!(
            "invalid thread attachment request: page limit must be between 1 and {MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE}"
        );
    }
    let archived = match archive_filter {
        ThreadAttachmentArchiveFilter::All => None,
        ThreadAttachmentArchiveFilter::NonArchived => Some(false),
        ThreadAttachmentArchiveFilter::Archived => Some(true),
    };
    let anchor = cursor
        .map(serde_json::from_str::<AttachmentThreadsCursor>)
        .transpose()?;
    if let Some(anchor) = &anchor
        && (anchor.attachment_type != attachment_type
            || anchor.identity_key != identity_key
            || anchor.archived != archived)
    {
        anyhow::bail!("invalid thread attachment request: invalid pagination cursor");
    }
    let mut statement = String::from(
        "SELECT a.thread_id AS thread_id, t.archived AS archived FROM codex_thread_attachments a \
         JOIN codex_threads t ON t.id = a.thread_id WHERE a.attachment_type = $1 AND a.identity_key = $2",
    );
    let mut values = sql_params![attachment_type, identity_key];
    if let Some(archived) = archived {
        values.push(archived.into());
        statement.push_str(&format!(" AND t.archived = ${}", values.len()));
    }
    if let Some(anchor) = anchor {
        values.push(anchor.thread_id.to_string().into());
        statement.push_str(&format!(" AND a.thread_id > ${}", values.len()));
    }
    values.push(((limit + 1) as i64).into());
    statement.push_str(&format!(
        " ORDER BY a.thread_id ASC LIMIT ${}",
        values.len()
    ));
    let sql_handle = antfly.sql().await.map_err(internal)?;
    let rows = sql_handle
        .fetch_all(&statement, values)
        .await
        .map_err(internal)?;
    let mut threads = rows
        .iter()
        .map(|row| -> anyhow::Result<ThreadAttachmentOwner> {
            Ok(ThreadAttachmentOwner {
                thread_id: ThreadId::from_string(&row.string("thread_id").map_err(internal)?)?,
                archived: row.bool("archived").map_err(internal)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let next_cursor = if threads.len() > limit {
        threads.truncate(limit);
        threads
            .last()
            .map(|thread| {
                serde_json::to_string(&AttachmentThreadsCursor {
                    attachment_type: attachment_type.to_owned(),
                    identity_key: identity_key.to_owned(),
                    archived,
                    thread_id: thread.thread_id,
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(ThreadAttachmentOwnerPage {
        threads,
        next_cursor,
    })
}

fn parse_attachment_cursor(cursor: &str) -> anyhow::Result<(String, i64, String)> {
    let mut segments = cursor.split('|');
    let (Some(thread_id), Some(created_at), Some(attachment_id), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        anyhow::bail!("invalid thread attachment request: invalid pagination cursor");
    };
    if ThreadId::from_string(thread_id).is_err() || uuid::Uuid::parse_str(attachment_id).is_err() {
        anyhow::bail!("invalid thread attachment request: invalid pagination cursor");
    }
    let created_at = created_at.parse::<i64>()?;
    Ok((thread_id.to_string(), created_at, attachment_id.to_string()))
}

pub(crate) async fn list_thread_attachments(
    antfly: &Antfly,
    thread_id: ThreadId,
    cursor: Option<&str>,
    limit: usize,
) -> anyhow::Result<ThreadAttachmentPage> {
    if !(1..=MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE).contains(&limit) {
        anyhow::bail!(
            "invalid thread attachment request: page limit must be between 1 and {MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE}"
        );
    }
    let thread_id_string = thread_id.to_string();
    let anchor = cursor.map(parse_attachment_cursor).transpose()?;
    if let Some((cursor_thread_id, _, _)) = anchor.as_ref()
        && cursor_thread_id != &thread_id_string
    {
        anyhow::bail!("invalid thread attachment request: invalid pagination cursor");
    }
    let mut statement = String::from(
        "SELECT id, thread_id, attachment_type, identity_key, payload, created_at FROM codex_thread_attachments WHERE thread_id = $1",
    );
    let mut values = sql_params![thread_id_string];
    if let Some((_, created_at, attachment_id)) = anchor {
        values.push(created_at.into());
        values.push(attachment_id.into());
        statement.push_str(&format!(
            " AND (created_at, id) > (${}, ${})",
            values.len() - 1,
            values.len()
        ));
    }
    values.push(((limit + 1) as i64).into());
    statement.push_str(&format!(
        " ORDER BY created_at ASC, id ASC LIMIT ${}",
        values.len()
    ));
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql.fetch_all(&statement, values).await.map_err(internal)?;
    let mut attachments = rows
        .iter()
        .map(attachment_from_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let next_cursor = if attachments.len() > limit {
        attachments.truncate(limit);
        attachments.last().map(|attachment| {
            format!(
                "{}|{}|{}",
                attachment.thread_id, attachment.created_at, attachment.id
            )
        })
    } else {
        None
    };
    Ok(ThreadAttachmentPage {
        attachments,
        next_cursor,
    })
}
