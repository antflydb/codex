//! Thread-owned attachments.
//!
//! Each attachment is stored three times, written atomically together: the
//! canonical row keyed by `(thread, type, identity key)`, a per-thread
//! listing entry keyed by `(thread, created_at, id)`, and an owner entry
//! keyed by `(type, identity key, thread)` used by
//! [`list_thread_attachment_threads`]. The owner entry does not cache a
//! thread's archived state (it would go stale on archive/unarchive), so that
//! listing re-reads each candidate thread's record.

use chrono::Utc;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::ThreadId;
use codex_state::MAX_THREAD_ATTACHMENT_IDENTITY_KEY_BYTES;
use codex_state::MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE;
use codex_state::MAX_THREAD_ATTACHMENT_PAYLOAD_BYTES;
use codex_state::MAX_THREAD_ATTACHMENT_TYPE_BYTES;
use codex_state::MAX_THREAD_ATTACHMENTS_PER_THREAD;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::AntflyThreadStore;
use super::from_value;
use super::internal;
use super::keys;
use super::to_value;
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

/// Records fetched per scan batch while filling an owner page.
const SCAN_BATCH: usize = 200;

/// Serializable mirror of [`ThreadAttachment`], which does not derive serde.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AttachmentDoc {
    id: String,
    thread_id: ThreadId,
    attachment_type: String,
    identity_key: String,
    payload: Value,
    created_at: i64,
}

impl From<AttachmentDoc> for ThreadAttachment {
    fn from(doc: AttachmentDoc) -> Self {
        ThreadAttachment {
            id: doc.id,
            thread_id: doc.thread_id,
            attachment_type: doc.attachment_type,
            identity_key: doc.identity_key,
            payload: doc.payload,
            created_at: doc.created_at,
        }
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

fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: message.into(),
    }
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

fn attachment_writes(doc: &AttachmentDoc) -> ThreadStoreResult<Vec<Write>> {
    let value = to_value(doc)?;
    Ok(vec![
        Write::put(
            keys::attachment(doc.thread_id, &doc.attachment_type, &doc.identity_key),
            value.clone(),
        ),
        Write::put(
            keys::attachment_list_entry(doc.thread_id, doc.created_at, &doc.id),
            value.clone(),
        ),
        Write::put(
            keys::attachment_owner_entry(&doc.attachment_type, &doc.identity_key, doc.thread_id),
            value,
        ),
    ])
}

/// Delete writes for every attachment owned by `thread_id`, for the host to
/// fold into its own atomic batch when deleting a thread.
pub(super) async fn delete_writes_for_thread(
    store: &AntflyThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<Write>> {
    let docs: Vec<AttachmentDoc> = store
        .antfly()
        .scan_as::<AttachmentDoc>(ScanRequest::prefix(&keys::attachments_prefix(thread_id)))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect();
    let mut writes = Vec::with_capacity(docs.len() * 3);
    for doc in docs {
        writes.push(Write::delete(keys::attachment(
            doc.thread_id,
            &doc.attachment_type,
            &doc.identity_key,
        )));
        writes.push(Write::delete(keys::attachment_list_entry(
            doc.thread_id,
            doc.created_at,
            &doc.id,
        )));
        writes.push(Write::delete(keys::attachment_owner_entry(
            &doc.attachment_type,
            &doc.identity_key,
            doc.thread_id,
        )));
    }
    Ok(writes)
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

    let _guard = store.antfly().lock().await;
    store
        .load_record(params.thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?;

    let canonical_key = keys::attachment(
        params.thread_id,
        &params.attachment_type,
        &params.identity_key,
    );
    if let Some(doc) = store
        .antfly()
        .get(canonical_key.clone())
        .await
        .map_err(internal)?
    {
        let existing: AttachmentDoc = from_value(doc)?;
        return Ok(AddThreadAttachmentOutcome::Existing(existing.into()));
    }

    let count = store
        .antfly()
        .scan(ScanRequest::prefix(&keys::attachments_prefix(
            params.thread_id,
        )))
        .await
        .map_err(internal)?
        .len();
    if count >= MAX_THREAD_ATTACHMENTS_PER_THREAD {
        return Err(invalid(format!(
            "thread attachment identity count exceeds {MAX_THREAD_ATTACHMENTS_PER_THREAD}"
        )));
    }

    let attachment = AttachmentDoc {
        id: uuid::Uuid::now_v7().to_string(),
        thread_id: params.thread_id,
        attachment_type: params.attachment_type,
        identity_key: params.identity_key,
        payload: params.payload,
        created_at: Utc::now().timestamp(),
    };
    store
        .antfly()
        .write(attachment_writes(&attachment)?)
        .await
        .map_err(internal)?;
    Ok(AddThreadAttachmentOutcome::Created(attachment.into()))
}

pub(super) async fn remove_thread_attachment(
    store: &AntflyThreadStore,
    params: RemoveThreadAttachmentParams,
) -> ThreadStoreResult<RemoveThreadAttachmentOutcome> {
    validate_identity(&params.attachment_type, &params.identity_key)?;
    let _guard = store.antfly().lock().await;
    store
        .load_record(params.thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?;

    let canonical_key = keys::attachment(
        params.thread_id,
        &params.attachment_type,
        &params.identity_key,
    );
    let Some(doc) = store
        .antfly()
        .get(canonical_key.clone())
        .await
        .map_err(internal)?
    else {
        return Ok(RemoveThreadAttachmentOutcome::NotFound);
    };
    let attachment: AttachmentDoc = from_value(doc)?;
    let writes = vec![
        Write::delete(canonical_key),
        Write::delete(keys::attachment_list_entry(
            attachment.thread_id,
            attachment.created_at,
            &attachment.id,
        )),
        Write::delete(keys::attachment_owner_entry(
            &attachment.attachment_type,
            &attachment.identity_key,
            attachment.thread_id,
        )),
    ];
    store.antfly().write(writes).await.map_err(internal)?;
    Ok(RemoveThreadAttachmentOutcome::Removed(attachment.into()))
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
    let prefix = keys::attachment_list_prefix(params.thread_id);
    let from = match anchor {
        Some((created_at, attachment_id)) => format!(
            "{}\u{0}",
            keys::attachment_list_entry(params.thread_id, created_at, &attachment_id)
        ),
        None => prefix.clone(),
    };
    let to = codex_antfly::keys::prefix_end(&prefix);
    let mut attachments: Vec<AttachmentDoc> = store
        .antfly()
        .scan_as::<AttachmentDoc>(ScanRequest {
            from,
            to,
            limit: Some(params.limit + 1),
        })
        .await
        .map_err(internal)?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect();
    let next_cursor = if attachments.len() > params.limit {
        attachments.truncate(params.limit);
        attachments
            .last()
            .map(|doc| format!("{}|{}|{}", doc.thread_id, doc.created_at, doc.id))
    } else {
        None
    };
    Ok(ThreadAttachmentPage {
        attachments: attachments.into_iter().map(Into::into).collect(),
        next_cursor,
    })
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

    let prefix = keys::attachment_owner_prefix(&params.attachment_type, &params.identity_key);
    let mut from = match anchor {
        Some(thread_id) => format!(
            "{}\u{0}",
            keys::attachment_owner_entry(&params.attachment_type, &params.identity_key, thread_id)
        ),
        None => prefix.clone(),
    };
    let to = codex_antfly::keys::prefix_end(&prefix);
    let mut threads: Vec<ThreadAttachmentOwner> = Vec::new();
    let mut has_more = false;
    'scan: loop {
        let batch: Vec<(String, AttachmentDoc)> = store
            .antfly()
            .scan_as::<AttachmentDoc>(ScanRequest {
                from: from.clone(),
                to: to.clone(),
                limit: Some(SCAN_BATCH),
            })
            .await
            .map_err(internal)?;
        let exhausted = batch.len() < SCAN_BATCH;
        for (key, doc) in batch {
            from = format!("{key}\u{0}");
            let Some(record) = store.load_record(doc.thread_id).await? else {
                continue;
            };
            let archived = record.archived_at.is_some();
            let include = match wanted_archived {
                Some(wanted) => wanted == archived,
                None => true,
            };
            if !include {
                continue;
            }
            if threads.len() == params.limit {
                has_more = true;
                break 'scan;
            }
            threads.push(ThreadAttachmentOwner {
                thread_id: doc.thread_id,
                archived,
            });
        }
        if exhausted {
            break;
        }
    }
    let next_cursor = if has_more {
        threads.last().and_then(|owner| {
            serde_json::to_string(&OwnerCursor {
                attachment_type: params.attachment_type.clone(),
                identity_key: params.identity_key.clone(),
                archived: wanted_archived,
                thread_id: owner.thread_id,
            })
            .ok()
        })
    } else {
        None
    };
    Ok(ThreadAttachmentOwnerPage {
        threads,
        next_cursor,
    })
}

pub(super) async fn copy_thread_attachments(
    store: &AntflyThreadStore,
    source_thread_id: ThreadId,
    destination_thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let _guard = store.antfly().lock().await;
    store
        .load_record(destination_thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: destination_thread_id,
        })?;
    let source: Vec<AttachmentDoc> = store
        .antfly()
        .scan_as::<AttachmentDoc>(ScanRequest::prefix(&keys::attachment_list_prefix(
            source_thread_id,
        )))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect();
    let created_at = Utc::now().timestamp();
    let mut writes = Vec::with_capacity(source.len() * 3);
    for doc in source {
        let copy = AttachmentDoc {
            id: uuid::Uuid::now_v7().to_string(),
            thread_id: destination_thread_id,
            attachment_type: doc.attachment_type,
            identity_key: doc.identity_key,
            payload: doc.payload,
            created_at,
        };
        writes.extend(attachment_writes(&copy)?);
    }
    store.antfly().write(writes).await.map_err(internal)
}
