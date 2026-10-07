//! Host-owned projects.
//!
//! A project's `recency_at_ms` is not stored; it is the max `recency_at` of
//! its non-archived member threads, computed from a scan of `ts:t:` the same
//! way `descendants_of` in [`super::listing`] computes thread lineage. This
//! store does not expect thread counts large enough to need a maintained
//! secondary index for that aggregate.
//!
//! Listing pages by sorting the full project set in memory, then resuming
//! after the project named by the cursor (an opaque project id). This is
//! simpler than, and not wire-compatible with, the local store's
//! value-encoding cursor, which is not required by this store's contract.

use std::collections::BTreeMap;
use std::collections::HashMap;

use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::ThreadId;
use codex_state::ProjectSortKey;
use serde::Deserialize;
use serde::Serialize;

use super::AntflyThreadStore;
use super::from_value;
use super::internal;
use super::keys;
use super::record::ThreadRecord;
use super::to_value;
use crate::CreateProjectParams;
use crate::CreatedProject;
use crate::DeletedProject;
use crate::ListProjectsParams;
use crate::MoveProjectParams;
use crate::ProjectMoveOutcome;
use crate::SortDirection;
use crate::StoredProject;
use crate::StoredProjectRoot;
use crate::StoredProjectsPage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::UpdateProjectParams;
use crate::UpdatedProject;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ProjectDoc {
    id: String,
    name: String,
    roots: Vec<String>,
    metadata: BTreeMap<String, String>,
    position: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: message.into(),
    }
}

fn stored(doc: ProjectDoc, recency_at_ms: Option<i64>) -> StoredProject {
    StoredProject {
        id: doc.id,
        name: doc.name,
        roots: doc
            .roots
            .into_iter()
            .map(|path| StoredProjectRoot { path })
            .collect(),
        metadata: doc.metadata,
        position: doc.position,
        created_at_ms: doc.created_at_ms,
        updated_at_ms: doc.updated_at_ms,
        recency_at_ms,
    }
}

async fn all_projects(store: &AntflyThreadStore) -> ThreadStoreResult<Vec<ProjectDoc>> {
    Ok(store
        .antfly()
        .scan_as::<ProjectDoc>(ScanRequest::prefix(keys::PROJECT_PREFIX))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect())
}

async fn load_project(
    store: &AntflyThreadStore,
    id: &str,
) -> ThreadStoreResult<Option<ProjectDoc>> {
    match store
        .antfly()
        .get(keys::project(id))
        .await
        .map_err(internal)?
    {
        Some(value) => Ok(Some(from_value(value)?)),
        None => Ok(None),
    }
}

/// Every thread currently assigned to `project_id`, via a full scan: there is
/// no maintained secondary index from project to member threads.
async fn project_threads(
    store: &AntflyThreadStore,
    project_id: &str,
) -> ThreadStoreResult<Vec<ThreadRecord>> {
    let records = store
        .antfly()
        .scan_as::<ThreadRecord>(ScanRequest::prefix(keys::THREAD_PREFIX))
        .await
        .map_err(internal)?;
    Ok(records
        .into_iter()
        .filter(|(_, record)| record.project_id().as_deref() == Some(project_id))
        .map(|(_, record)| record)
        .collect())
}

async fn project_recency_ms(
    store: &AntflyThreadStore,
    project_id: &str,
) -> ThreadStoreResult<Option<i64>> {
    let threads = project_threads(store, project_id).await?;
    Ok(threads
        .iter()
        .filter(|record| record.archived_at.is_none())
        .map(|record| record.recency_at().timestamp_millis())
        .max())
}

/// `project_id -> max recency_at_ms over its non-archived threads`, computed
/// with one scan for every project, for `list_projects`.
async fn project_recency_map(store: &AntflyThreadStore) -> ThreadStoreResult<HashMap<String, i64>> {
    let records = store
        .antfly()
        .scan_as::<ThreadRecord>(ScanRequest::prefix(keys::THREAD_PREFIX))
        .await
        .map_err(internal)?;
    let mut map = HashMap::new();
    for (_, record) in records {
        if record.archived_at.is_some() {
            continue;
        }
        let Some(project_id) = record.project_id() else {
            continue;
        };
        let millis = record.recency_at().timestamp_millis();
        map.entry(project_id)
            .and_modify(|value: &mut i64| *value = (*value).max(millis))
            .or_insert(millis);
    }
    Ok(map)
}

pub(super) async fn list_projects(
    store: &AntflyThreadStore,
    params: ListProjectsParams,
) -> ThreadStoreResult<StoredProjectsPage> {
    if params.limit == 0 {
        return Err(invalid("project limit must be positive"));
    }
    let recency_map = project_recency_map(store).await?;
    let mut projects = all_projects(store).await?;
    let recency_of = |doc: &ProjectDoc| recency_map.get(&doc.id).copied();
    sort_projects(
        &mut projects,
        &recency_map,
        params.sort_key,
        params.sort_direction,
    );

    let start = match &params.cursor {
        Some(cursor) => {
            let position = projects
                .iter()
                .position(|doc| &doc.id == cursor)
                .ok_or_else(|| invalid("invalid project cursor: not found"))?;
            position + 1
        }
        None => 0,
    };
    let mut page: Vec<ProjectDoc> = projects.into_iter().skip(start).collect();
    let has_more = page.len() > params.limit;
    page.truncate(params.limit);
    let next_cursor = has_more
        .then(|| page.last().map(|doc| doc.id.clone()))
        .flatten();
    Ok(StoredProjectsPage {
        projects: page
            .into_iter()
            .map(|doc| {
                let recency = recency_of(&doc);
                stored(doc, recency)
            })
            .collect(),
        next_cursor,
    })
}

fn sort_projects(
    projects: &mut [ProjectDoc],
    recency_map: &HashMap<String, i64>,
    sort_key: ProjectSortKey,
    direction: SortDirection,
) {
    match sort_key {
        ProjectSortKey::Position => {
            projects.sort_by(|a, b| a.position.cmp(&b.position).then_with(|| a.id.cmp(&b.id)));
            if direction == SortDirection::Desc {
                projects.reverse();
            }
        }
        ProjectSortKey::RecencyAt => {
            // Nulls (no non-archived member threads) always sort last;
            // `direction` only orders values and ids within each group.
            projects.sort_by(|a, b| {
                let a_recency = recency_map.get(&a.id).copied();
                let b_recency = recency_map.get(&b.id).copied();
                let ordering = match (a_recency, b_recency) {
                    (None, None) => a.id.cmp(&b.id),
                    (None, Some(_)) => return std::cmp::Ordering::Greater,
                    (Some(_), None) => return std::cmp::Ordering::Less,
                    (Some(x), Some(y)) => x.cmp(&y).then_with(|| a.id.cmp(&b.id)),
                };
                if direction == SortDirection::Desc {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
        }
    }
}

pub(super) async fn read_project(
    store: &AntflyThreadStore,
    project_id: String,
) -> ThreadStoreResult<Option<StoredProject>> {
    let Some(doc) = load_project(store, &project_id).await? else {
        return Ok(None);
    };
    let recency = project_recency_ms(store, &project_id).await?;
    Ok(Some(stored(doc, recency)))
}

pub(super) async fn create_project(
    store: &AntflyThreadStore,
    params: CreateProjectParams,
) -> ThreadStoreResult<CreatedProject> {
    let _guard = store.antfly().lock().await;
    let key_key = keys::project_idempotency_key(&params.idempotency_key);
    if let Some(doc) = store
        .antfly()
        .get(key_key.clone())
        .await
        .map_err(internal)?
    {
        let project_id = doc
            .get("project_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "corrupt project idempotency key record".to_owned(),
            })?
            .to_string();
        return match load_project(store, &project_id).await? {
            Some(project) => {
                let recency = project_recency_ms(store, &project_id).await?;
                Ok(CreatedProject {
                    project: stored(project, recency),
                    created: false,
                })
            }
            None => Err(invalid(format!(
                "idempotency key refers to deleted project: {}",
                params.idempotency_key
            ))),
        };
    }

    let mut thread_records = Vec::with_capacity(params.thread_ids.len());
    for thread_id_str in &params.thread_ids {
        let thread_id = ThreadId::from_string(thread_id_str).ok().ok_or_else(|| {
            ThreadStoreError::Internal {
                message: format!("thread not found: {thread_id_str}"),
            }
        })?;
        let record =
            store
                .load_record(thread_id)
                .await?
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!("thread not found: {thread_id_str}"),
                })?;
        thread_records.push(record);
    }
    let recency_at_ms = thread_records
        .iter()
        .filter(|record| record.archived_at.is_none())
        .map(|record| record.recency_at().timestamp_millis())
        .max();

    let projects = all_projects(store).await?;
    let position = projects
        .iter()
        .map(|project| project.position)
        .max()
        .map_or(0, |max| max + 1);
    let now = chrono::Utc::now().timestamp_millis();
    let id = uuid::Uuid::now_v7().to_string();
    let doc = ProjectDoc {
        id: id.clone(),
        name: params.name,
        roots: params.roots.into_iter().map(|root| root.path).collect(),
        metadata: params.metadata,
        position,
        created_at_ms: now,
        updated_at_ms: now,
    };
    let mut writes = vec![
        Write::put(keys::project(&id), to_value(&doc)?),
        Write::put(key_key, serde_json::json!({ "project_id": id })),
    ];
    for record in thread_records {
        let mut updated = record.clone();
        updated.patch.project_id = Some(Some(id.clone()));
        writes.extend(AntflyThreadStore::record_writes(Some(&record), &updated)?);
    }
    store.antfly().write(writes).await.map_err(internal)?;
    Ok(CreatedProject {
        project: stored(doc, recency_at_ms),
        created: true,
    })
}

pub(super) async fn update_project(
    store: &AntflyThreadStore,
    params: UpdateProjectParams,
) -> ThreadStoreResult<Option<UpdatedProject>> {
    let _guard = store.antfly().lock().await;
    let Some(mut doc) = load_project(store, &params.project_id).await? else {
        return Ok(None);
    };
    let next_name = params.name.unwrap_or_else(|| doc.name.clone());
    let next_roots = params
        .roots
        .map(|roots| roots.into_iter().map(|root| root.path).collect::<Vec<_>>())
        .unwrap_or_else(|| doc.roots.clone());
    let next_metadata = params.metadata.unwrap_or_else(|| doc.metadata.clone());
    if next_name == doc.name && next_roots == doc.roots && next_metadata == doc.metadata {
        let recency = project_recency_ms(store, &doc.id).await?;
        return Ok(Some(UpdatedProject {
            project: stored(doc, recency),
            changed: false,
        }));
    }
    doc.name = next_name;
    doc.roots = next_roots;
    doc.metadata = next_metadata;
    doc.updated_at_ms = chrono::Utc::now().timestamp_millis();
    store
        .antfly()
        .write(vec![Write::put(keys::project(&doc.id), to_value(&doc)?)])
        .await
        .map_err(internal)?;
    let recency = project_recency_ms(store, &doc.id).await?;
    Ok(Some(UpdatedProject {
        project: stored(doc, recency),
        changed: true,
    }))
}

pub(super) async fn move_project(
    store: &AntflyThreadStore,
    params: MoveProjectParams,
) -> ThreadStoreResult<Option<ProjectMoveOutcome>> {
    let _guard = store.antfly().lock().await;
    let mut projects = all_projects(store).await?;
    projects.sort_by(|a, b| a.position.cmp(&b.position).then_with(|| a.id.cmp(&b.id)));
    let Some(current_index) = projects
        .iter()
        .position(|project| project.id == params.project_id)
    else {
        return Ok(None);
    };
    if params.before_project_id.as_deref() == Some(params.project_id.as_str()) {
        return Err(invalid(format!(
            "project {} cannot be moved before itself",
            params.project_id
        )));
    }
    let original_order: Vec<String> = projects.iter().map(|project| project.id.clone()).collect();
    let moved = projects.remove(current_index);
    let next_index = match &params.before_project_id {
        Some(before) => projects
            .iter()
            .position(|project| &project.id == before)
            .ok_or_else(|| invalid(format!("before project not found: {before}")))?,
        None => projects.len(),
    };
    projects.insert(next_index, moved);
    let new_order: Vec<String> = projects.iter().map(|project| project.id.clone()).collect();
    if new_order == original_order {
        return Ok(Some(ProjectMoveOutcome::Unchanged));
    }

    let now = chrono::Utc::now().timestamp_millis();
    let mut writes = Vec::with_capacity(projects.len());
    for (index, mut doc) in projects.into_iter().enumerate() {
        let position = i64::try_from(index).unwrap_or(i64::MAX);
        doc.position = position;
        if doc.id == params.project_id {
            doc.updated_at_ms = now;
        }
        writes.push(Write::put(keys::project(&doc.id), to_value(&doc)?));
    }
    store.antfly().write(writes).await.map_err(internal)?;
    Ok(Some(ProjectMoveOutcome::Moved))
}

pub(super) async fn delete_project(
    store: &AntflyThreadStore,
    project_id: String,
) -> ThreadStoreResult<Option<DeletedProject>> {
    let _guard = store.antfly().lock().await;
    if load_project(store, &project_id).await?.is_none() {
        return Ok(None);
    }
    let members = project_threads(store, &project_id).await?;
    let mut active = Vec::new();
    let mut archived = Vec::new();
    let mut writes = vec![Write::delete(keys::project(&project_id))];
    for record in members {
        let mut updated = record.clone();
        updated.patch.project_id = Some(None);
        writes.extend(AntflyThreadStore::record_writes(Some(&record), &updated)?);
        if record.archived_at.is_some() {
            archived.push(record.thread_id().to_string());
        } else {
            active.push(record.thread_id().to_string());
        }
    }
    active.sort();
    archived.sort();
    store.antfly().write(writes).await.map_err(internal)?;
    Ok(Some(DeletedProject {
        affected_active_thread_ids: active,
        affected_archived_thread_ids: archived,
    }))
}
