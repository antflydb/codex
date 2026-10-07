//! Independently persisted thread sections, including the built-in Pinned
//! section (`codex_state::PINNED_THREAD_SECTION_ID`), which always exists and
//! cannot be renamed or deleted.
//!
//! A thread's section membership (`ts:s:...`, see [`super::keys`]) is
//! separate from a section's definition (`ts:scn:...`, this module) so a
//! thread can be moved into an ad hoc section tag that was never created
//! through [`create_thread_section`]; its display name then falls back to
//! the tag itself, matching the store's pre-existing `move_thread_to_section`
//! behavior.

use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_state::PINNED_THREAD_SECTION_ID;
use codex_state::PINNED_THREAD_SECTION_NAME;
use codex_state::ThreadSection;

use super::AntflyThreadStore;
use super::from_value;
use super::internal;
use super::keys;
use super::record::ThreadRecord;
use super::to_value;
use crate::CreateThreadSectionParams;
use crate::DeleteThreadSectionParams;
use crate::ListThreadSectionsParams;
use crate::RenameThreadSectionParams;
use crate::StoredThreadSection;
use crate::StoredThreadSectionsPage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

fn stored(section: ThreadSection) -> StoredThreadSection {
    StoredThreadSection {
        id: section.id,
        name: section.name,
        appearance: section.appearance,
    }
}

fn pinned_section() -> ThreadSection {
    ThreadSection {
        id: PINNED_THREAD_SECTION_ID.to_string(),
        name: PINNED_THREAD_SECTION_NAME.to_string(),
        appearance: None,
    }
}

/// Ensures the built-in Pinned section has a persisted row, so it is
/// discoverable by [`list_thread_sections`] like any other section.
async fn ensure_pinned_section(store: &AntflyThreadStore) -> ThreadStoreResult<()> {
    let key = keys::section_def(PINNED_THREAD_SECTION_ID);
    if store
        .antfly()
        .get(key.clone())
        .await
        .map_err(internal)?
        .is_some()
    {
        return Ok(());
    }
    store
        .antfly()
        .write(vec![Write::put(key, to_value(&pinned_section())?)])
        .await
        .map_err(internal)
}

/// Loads one section's definition, synthesizing the built-in Pinned section
/// without requiring it to already be persisted.
pub(super) async fn load_section(
    store: &AntflyThreadStore,
    id: &str,
) -> ThreadStoreResult<Option<ThreadSection>> {
    if id == PINNED_THREAD_SECTION_ID {
        return Ok(Some(pinned_section()));
    }
    match store
        .antfly()
        .get(keys::section_def(id))
        .await
        .map_err(internal)?
    {
        Some(doc) => Ok(Some(from_value(doc)?)),
        None => Ok(None),
    }
}

pub(super) async fn list_thread_sections(
    store: &AntflyThreadStore,
    params: ListThreadSectionsParams,
) -> ThreadStoreResult<StoredThreadSectionsPage> {
    ensure_pinned_section(store).await?;
    let limit = params.limit.max(1);
    let prefix = keys::SECTION_DEF_PREFIX;
    let from = match &params.cursor {
        Some(cursor) => format!("{}\u{0}", keys::section_def(cursor)),
        None => prefix.to_string(),
    };
    let to = codex_antfly::keys::prefix_end(prefix);
    let mut sections: Vec<ThreadSection> = store
        .antfly()
        .scan_as::<ThreadSection>(ScanRequest {
            from,
            to,
            limit: Some(limit + 1),
        })
        .await
        .map_err(internal)?
        .into_iter()
        .map(|(_, section)| section)
        .collect();
    let next_cursor = if sections.len() > limit {
        sections.truncate(limit);
        sections.last().map(|section| section.id.clone())
    } else {
        None
    };
    Ok(StoredThreadSectionsPage {
        sections: sections.into_iter().map(stored).collect(),
        next_cursor,
    })
}

pub(super) async fn create_thread_section(
    store: &AntflyThreadStore,
    params: CreateThreadSectionParams,
) -> ThreadStoreResult<StoredThreadSection> {
    let section = ThreadSection {
        id: uuid::Uuid::now_v7().to_string(),
        name: params.name,
        appearance: params.appearance,
    };
    store
        .antfly()
        .write(vec![Write::put(
            keys::section_def(&section.id),
            to_value(&section)?,
        )])
        .await
        .map_err(internal)?;
    Ok(stored(section))
}

pub(super) async fn rename_thread_section(
    store: &AntflyThreadStore,
    params: RenameThreadSectionParams,
) -> ThreadStoreResult<Option<StoredThreadSection>> {
    if params.section_id == PINNED_THREAD_SECTION_ID {
        return Err(ThreadStoreError::Internal {
            message: "built-in pinned thread section cannot be renamed".to_owned(),
        });
    }
    let _guard = store.antfly().lock().await;
    let Some(mut section) = load_section(store, &params.section_id).await? else {
        return Ok(None);
    };
    section.name = params.name;
    if let Some(appearance) = params.appearance {
        section.appearance = appearance;
    }
    let mut writes = vec![Write::put(
        keys::section_def(&section.id),
        to_value(&section)?,
    )];
    // Denormalized onto member records so listings do not re-read the
    // section definition per thread.
    let members = store
        .antfly()
        .scan_as::<ThreadRecord>(ScanRequest::prefix(&keys::section_prefix(&section.id)))
        .await
        .map_err(internal)?;
    for (_, member) in &members {
        let mut updated = member.clone();
        updated.section_name = Some(section.name.clone());
        updated.section_appearance = section.appearance.clone();
        writes.extend(AntflyThreadStore::record_writes(Some(member), &updated)?);
    }
    store.antfly().write(writes).await.map_err(internal)?;
    Ok(Some(stored(section)))
}

pub(super) async fn delete_thread_section(
    store: &AntflyThreadStore,
    params: DeleteThreadSectionParams,
) -> ThreadStoreResult<bool> {
    if params.section_id == PINNED_THREAD_SECTION_ID {
        return Err(ThreadStoreError::Internal {
            message: "built-in pinned thread section cannot be deleted".to_owned(),
        });
    }
    let _guard = store.antfly().lock().await;
    let key = keys::section_def(&params.section_id);
    if store
        .antfly()
        .get(key.clone())
        .await
        .map_err(internal)?
        .is_none()
    {
        return Ok(false);
    }
    let mut writes = vec![Write::delete(key)];
    let members = store
        .antfly()
        .scan_as::<ThreadRecord>(ScanRequest::prefix(&keys::section_prefix(
            &params.section_id,
        )))
        .await
        .map_err(internal)?;
    for (_, member) in &members {
        let mut updated = member.clone();
        updated.section = None;
        updated.section_position = None;
        updated.section_entered_at = None;
        updated.section_name = None;
        updated.section_appearance = None;
        writes.extend(AntflyThreadStore::record_writes(Some(member), &updated)?);
    }
    store.antfly().write(writes).await.map_err(internal)?;
    Ok(true)
}
