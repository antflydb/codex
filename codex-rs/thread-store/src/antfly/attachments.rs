//! Thread-owned attachments (`codex_thread_attachments`). One row per
//! identity, looked up canonically by `(thread_id, attachment_type,
//! identity_key)` (its UNIQUE constraint), listed per thread by
//! `codex_thread_attachments_thread`, and listed per identity across threads
//! by `codex_thread_attachments_identity`.

use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use codex_state::MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES;
use codex_state::MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE;
use codex_state::MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES;
use codex_state::MAX_THREAD_ATTACHMENT_TYPE_BYTES;
use codex_state::MAX_THREAD_ATTACHMENTS_PER_THREAD;
use serde::Deserialize;
use serde::Serialize;

use super::AntflyThreadStore;
use super::internal;
use crate::AddThreadAttachmentOutcome;
use crate::AddThreadAttachmentParams;
use crate::ListThreadAttachmentThreadsParams;
use crate::ListThreadAttachmentsParams;
use crate::RemoveThreadAttachmentOutcome;
use crate::RemoveThreadAttachmentParams;
use crate::ThreadAttachment;
use crate::ThreadAttachmentArchiveFilter;
use crate::ThreadAttachmentOwner;
use crate::ThreadAttachmentOwnerPage;
use crate::ThreadAttachmentPage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: message.into(),
    }
}

fn attachment_from_row(row: &SqlRow) -> ThreadStoreResult<ThreadAttachment> {
    Ok(ThreadAttachment {
        id: row.string("id").map_err(internal)?,
        thread_id: ThreadId::from_string(&row.string("thread_id").map_err(internal)?).map_err(
            |err| ThreadStoreError::Internal {
                message: format!("invalid thread id: {err}"),
            },
        )?,
        attachment_type: row.string("attachment_type").map_err(internal)?,
        identity_key: row.string("identity_key").map_err(internal)?,
        payload: row.json("payload").map_err(internal)?,
        created_at: row.i64("created_at").map_err(internal)?,
    })
}

fn validate_identity(attachment_type: &str, identity_key: &str) -> ThreadStoreResult<()> {
    if attachment_type.trim().is_empty() {
        return Err(invalid("attachment type must not be empty"));
    }
    if attachment_type.len() > MAX_THREAD_ATTACHMENT_TYPE_BYTES {
        return Err(invalid(format!(
            "attachment type exceeds {MAX_THREAD_ATTACHMENT_TYPE_BYTES} bytes"
        )));
    }
    if identity_key.trim().is_empty() {
        return Err(invalid("attachment identity key must not be empty"));
    }
    if identity_key.len() > MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES {
        return Err(invalid(format!(
            "attachment identity key exceeds {MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

pub(super) async fn add_thread_attachment(
    store: &AntflyThreadStore,
    params: AddThreadAttachmentParams,
) -> ThreadStoreResult<AddThreadAttachmentOutcome> {
    validate_identity(&params.attachment_type, &params.identity_key)?;
    let serialized = serde_json::to_string(&params.payload)
        .map_err(|err| invalid(format!("attachment payload cannot be serialized: {err}")))?;
    if serialized.len() > MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES {
        return Err(invalid(format!(
            "attachment payload exceeds {MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES} bytes"
        )));
    }
    store
        .load_record(params.thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?;

    let sql = store.antfly().sql().await.map_err(internal)?;
    let thread_id = params.thread_id.to_string();
    if let Some(row) = sql
        .fetch_optional(
            "SELECT id, thread_id, attachment_type, identity_key, payload, created_at \
             FROM codex_thread_attachments WHERE thread_id = $1 AND attachment_type = $2 AND identity_key = $3",
            sql_params![thread_id.clone(), params.attachment_type.clone(), params.identity_key.clone()],
        )
        .await
        .map_err(internal)?
    {
        return Ok(AddThreadAttachmentOutcome::Existing(attachment_from_row(&row)?));
    }

    let count = sql
        .fetch_optional(
            "SELECT COUNT(*) AS value FROM codex_thread_attachments WHERE thread_id = $1",
            sql_params![thread_id.clone()],
        )
        .await
        .map_err(internal)?
        .map(|row| row.i64("value"))
        .transpose()
        .map_err(internal)?
        .unwrap_or(0);
    if count as usize >= MAX_THREAD_ATTACHMENTS_PER_THREAD {
        return Err(invalid(format!(
            "thread attachment identity count exceeds {MAX_THREAD_ATTACHMENTS_PER_THREAD}"
        )));
    }

    let attachment = ThreadAttachment {
        id: uuid::Uuid::now_v7().to_string(),
        thread_id: params.thread_id,
        attachment_type: params.attachment_type,
        identity_key: params.identity_key,
        payload: params.payload,
        created_at: chrono::Utc::now().timestamp(),
    };
    sql.execute(
        "INSERT INTO codex_thread_attachments (id, thread_id, attachment_type, identity_key, payload, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        sql_params![
            attachment.id.clone(),
            thread_id,
            attachment.attachment_type.clone(),
            attachment.identity_key.clone(),
            attachment.payload.clone(),
            attachment.created_at
        ],
    )
    .await
    .map_err(internal)?;
    Ok(AddThreadAttachmentOutcome::Created(attachment))
}

pub(super) async fn remove_thread_attachment(
    store: &AntflyThreadStore,
    params: RemoveThreadAttachmentParams,
) -> ThreadStoreResult<RemoveThreadAttachmentOutcome> {
    validate_identity(&params.attachment_type, &params.identity_key)?;
    store
        .load_record(params.thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?;
    let sql = store.antfly().sql().await.map_err(internal)?;
    let removed = sql
        .fetch_optional(
            "DELETE FROM codex_thread_attachments WHERE thread_id = $1 AND attachment_type = $2 AND identity_key = $3 \
             RETURNING id, thread_id, attachment_type, identity_key, payload, created_at",
            sql_params![params.thread_id.to_string(), params.attachment_type, params.identity_key],
        )
        .await
        .map_err(internal)?;
    match removed {
        Some(row) => Ok(RemoveThreadAttachmentOutcome::Removed(attachment_from_row(
            &row,
        )?)),
        None => Ok(RemoveThreadAttachmentOutcome::NotFound),
    }
}

#[derive(Serialize, Deserialize)]
struct OwnerCursor {
    attachment_type: String,
    identity_key: String,
    archived: Option<bool>,
    thread_id: ThreadId,
}

fn archive_filter_value(filter: ThreadAttachmentArchiveFilter) -> Option<bool> {
    match filter {
        ThreadAttachmentArchiveFilter::All => None,
        ThreadAttachmentArchiveFilter::NonArchived => Some(false),
        ThreadAttachmentArchiveFilter::Archived => Some(true),
    }
}

pub(super) async fn list_thread_attachment_threads(
    store: &AntflyThreadStore,
    params: ListThreadAttachmentThreadsParams,
) -> ThreadStoreResult<ThreadAttachmentOwnerPage> {
    validate_identity(&params.attachment_type, &params.identity_key)?;
    if !(1..=MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE).contains(&params.limit) {
        return Err(invalid(format!(
            "page limit must be between 1 and {MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE}"
        )));
    }
    let wanted_archived = archive_filter_value(params.archive_filter);
    let anchor = params
        .cursor
        .as_deref()
        .map(|cursor| -> ThreadStoreResult<ThreadId> {
            let parsed: OwnerCursor =
                serde_json::from_str(cursor).map_err(|_| invalid("invalid pagination cursor"))?;
            if parsed.attachment_type != params.attachment_type
                || parsed.identity_key != params.identity_key
                || parsed.archived != wanted_archived
            {
                return Err(invalid("invalid pagination cursor"));
            }
            Ok(parsed.thread_id)
        })
        .transpose()?;

    let mut sql = String::from(
        "SELECT a.thread_id AS thread_id, t.archived AS archived FROM codex_thread_attachments a \
         JOIN codex_threads t ON t.id = a.thread_id \
         WHERE a.attachment_type = $1 AND a.identity_key = $2",
    );
    let mut values = sql_params![params.attachment_type.clone(), params.identity_key.clone()];
    if let Some(archived) = wanted_archived {
        values.push(archived.into());
        sql.push_str(&format!(" AND t.archived = ${}", values.len()));
    }
    if let Some(anchor) = anchor {
        values.push(anchor.to_string().into());
        sql.push_str(&format!(" AND a.thread_id > ${}", values.len()));
    }
    values.push(((params.limit + 1) as i64).into());
    sql.push_str(&format!(
        " ORDER BY a.thread_id ASC LIMIT ${}",
        values.len()
    ));

    let sql_handle = store.antfly().sql().await.map_err(internal)?;
    let rows = sql_handle.fetch_all(&sql, values).await.map_err(internal)?;
    let mut threads = rows
        .iter()
        .map(|row| -> ThreadStoreResult<ThreadAttachmentOwner> {
            Ok(ThreadAttachmentOwner {
                thread_id: ThreadId::from_string(&row.string("thread_id").map_err(internal)?)
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("invalid thread id: {err}"),
                    })?,
                archived: row.bool("archived").map_err(internal)?,
            })
        })
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    let next_cursor = if threads.len() > params.limit {
        threads.truncate(params.limit);
        threads
            .last()
            .map(|thread| {
                serde_json::to_string(&OwnerCursor {
                    attachment_type: params.attachment_type.clone(),
                    identity_key: params.identity_key.clone(),
                    archived: wanted_archived,
                    thread_id: thread.thread_id,
                })
            })
            .transpose()
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("serialize cursor: {err}"),
            })?
    } else {
        None
    };
    Ok(ThreadAttachmentOwnerPage {
        threads,
        next_cursor,
    })
}

fn parse_list_cursor(cursor: &str, thread_id: ThreadId) -> ThreadStoreResult<(i64, String)> {
    let mut segments = cursor.split('|');
    let (Some(cursor_thread), Some(created_at), Some(attachment_id), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return Err(invalid("invalid pagination cursor"));
    };
    match ThreadId::from_string(cursor_thread) {
        Ok(id) if id == thread_id => {}
        _ => return Err(invalid("invalid pagination cursor")),
    }
    if uuid::Uuid::parse_str(attachment_id).is_err() {
        return Err(invalid("invalid pagination cursor"));
    }
    let created_at = created_at
        .parse::<i64>()
        .map_err(|_| invalid("invalid pagination cursor"))?;
    Ok((created_at, attachment_id.to_string()))
}

pub(super) async fn list_thread_attachments(
    store: &AntflyThreadStore,
    params: ListThreadAttachmentsParams,
) -> ThreadStoreResult<ThreadAttachmentPage> {
    if !(1..=MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE).contains(&params.limit) {
        return Err(invalid(format!(
            "page limit must be between 1 and {MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE}"
        )));
    }
    let anchor = params
        .cursor
        .as_deref()
        .map(|cursor| parse_list_cursor(cursor, params.thread_id))
        .transpose()?;
    let mut sql = String::from(
        "SELECT id, thread_id, attachment_type, identity_key, payload, created_at \
         FROM codex_thread_attachments WHERE thread_id = $1",
    );
    let mut values = sql_params![params.thread_id.to_string()];
    if let Some((created_at, attachment_id)) = anchor {
        values.push(created_at.into());
        values.push(attachment_id.into());
        sql.push_str(&format!(
            " AND (created_at, id) > (${}, ${})",
            values.len() - 1,
            values.len()
        ));
    }
    values.push(((params.limit + 1) as i64).into());
    sql.push_str(&format!(
        " ORDER BY created_at ASC, id ASC LIMIT ${}",
        values.len()
    ));

    let sql_handle = store.antfly().sql().await.map_err(internal)?;
    let rows = sql_handle.fetch_all(&sql, values).await.map_err(internal)?;
    let mut attachments = rows
        .iter()
        .map(attachment_from_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    let next_cursor = if attachments.len() > params.limit {
        attachments.truncate(params.limit);
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

pub(super) async fn copy_thread_attachments(
    store: &AntflyThreadStore,
    source_thread_id: ThreadId,
    destination_thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    store
        .load_record(destination_thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: destination_thread_id,
        })?;
    let sql = store.antfly().sql().await.map_err(internal)?;
    let source = sql
        .fetch_all(
            "SELECT attachment_type, identity_key, payload FROM codex_thread_attachments \
             WHERE thread_id = $1 ORDER BY created_at, id",
            sql_params![source_thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    let created_at = chrono::Utc::now().timestamp();
    let mut tx = sql.begin().await.map_err(internal)?;
    for row in &source {
        tx.execute(
            "INSERT INTO codex_thread_attachments (id, thread_id, attachment_type, identity_key, payload, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
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
