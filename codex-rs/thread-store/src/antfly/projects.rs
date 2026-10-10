//! Host-owned projects (`codex_projects` / `codex_project_roots` /
//! `codex_project_idempotency_keys`).
//!
//! A project's `recency_at_ms` is not stored; it is `MAX(recency_at_ms)` over
//! its non-archived member threads, computed with one query per read (or one
//! join per listing) instead of a maintained aggregate.

use std::collections::BTreeMap;

use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use codex_state::ProjectSortKey;

use super::AntflyThreadStore;
use super::internal;
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

fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: message.into(),
    }
}

struct ProjectRow {
    id: String,
    name: String,
    metadata: BTreeMap<String, String>,
    position: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

fn project_row(row: &SqlRow) -> ThreadStoreResult<ProjectRow> {
    Ok(ProjectRow {
        id: row.string("id").map_err(internal)?,
        name: row.string("name").map_err(internal)?,
        metadata: serde_json::from_value(row.json("metadata").map_err(internal)?).map_err(
            |err| ThreadStoreError::Internal {
                message: format!("invalid project metadata: {err}"),
            },
        )?,
        position: row.i64("position").map_err(internal)?,
        created_at_ms: row.i64("created_at_ms").map_err(internal)?,
        updated_at_ms: row.i64("updated_at_ms").map_err(internal)?,
    })
}

fn stored(
    row: ProjectRow,
    roots: Vec<StoredProjectRoot>,
    recency_at_ms: Option<i64>,
) -> StoredProject {
    StoredProject {
        id: row.id,
        name: row.name,
        roots,
        metadata: row.metadata,
        position: row.position,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
        recency_at_ms,
    }
}

async fn roots_for(
    store: &AntflyThreadStore,
    project_id: &str,
) -> ThreadStoreResult<Vec<StoredProjectRoot>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT path FROM codex_project_roots WHERE project_id = $1 ORDER BY position ASC",
            sql_params![project_id],
        )
        .await
        .map_err(internal)?;
    rows.iter()
        .map(|row| {
            Ok(StoredProjectRoot {
                path: row.string("path").map_err(internal)?,
            })
        })
        .collect()
}

async fn recency_for(
    store: &AntflyThreadStore,
    project_id: &str,
) -> ThreadStoreResult<Option<i64>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT MAX(recency_at_ms) AS value FROM codex_threads WHERE project_id = $1 AND archived = false",
            sql_params![project_id],
        )
        .await
        .map_err(internal)?;
    Ok(row.and_then(|row| row.opt_i64("value").ok().flatten()))
}

async fn load_project(
    store: &AntflyThreadStore,
    id: &str,
) -> ThreadStoreResult<Option<ProjectRow>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT * FROM codex_projects WHERE id = $1",
            sql_params![id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| project_row(&row)).transpose()
}

pub(super) async fn list_projects(
    store: &AntflyThreadStore,
    params: ListProjectsParams,
) -> ThreadStoreResult<StoredProjectsPage> {
    if params.limit == 0 {
        return Err(invalid("project limit must be positive"));
    }
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT p.id, p.name, p.metadata, p.position, p.created_at_ms, p.updated_at_ms, \
                (SELECT MAX(recency_at_ms) FROM codex_threads WHERE project_id = p.id AND archived = false) AS recency_at_ms \
             FROM codex_projects p",
            vec![],
        )
        .await
        .map_err(internal)?;
    let mut projects: Vec<(ProjectRow, Option<i64>)> = rows
        .iter()
        .map(|row| {
            Ok((
                project_row(row)?,
                row.opt_i64("recency_at_ms").map_err(internal)?,
            ))
        })
        .collect::<ThreadStoreResult<Vec<_>>>()?;

    sort_projects(&mut projects, params.sort_key, params.sort_direction);
    let start = match &params.cursor {
        Some(cursor) => {
            let position = projects
                .iter()
                .position(|(project, _)| &project.id == cursor)
                .ok_or_else(|| invalid("invalid project cursor: not found"))?;
            position + 1
        }
        None => 0,
    };
    let mut page: Vec<(ProjectRow, Option<i64>)> = projects.into_iter().skip(start).collect();
    let has_more = page.len() > params.limit;
    page.truncate(params.limit);
    let next_cursor = has_more
        .then(|| page.last().map(|(project, _)| project.id.clone()))
        .flatten();

    let mut projects = Vec::with_capacity(page.len());
    for (project, recency_at_ms) in page {
        let roots = roots_for(store, &project.id).await?;
        projects.push(stored(project, roots, recency_at_ms));
    }
    Ok(StoredProjectsPage {
        projects,
        next_cursor,
    })
}

fn sort_projects(
    projects: &mut [(ProjectRow, Option<i64>)],
    sort_key: ProjectSortKey,
    direction: SortDirection,
) {
    match sort_key {
        ProjectSortKey::Position => {
            projects.sort_by(|(a, _), (b, _)| {
                a.position.cmp(&b.position).then_with(|| a.id.cmp(&b.id))
            });
            if direction == SortDirection::Desc {
                projects.reverse();
            }
        }
        ProjectSortKey::RecencyAt => {
            projects.sort_by(|(a, a_recency), (b, b_recency)| {
                let ordering = match (a_recency, b_recency) {
                    (None, None) => a.id.cmp(&b.id),
                    (None, Some(_)) => return std::cmp::Ordering::Greater,
                    (Some(_), None) => return std::cmp::Ordering::Less,
                    (Some(x), Some(y)) => x.cmp(y).then_with(|| a.id.cmp(&b.id)),
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
    let Some(project) = load_project(store, &project_id).await? else {
        return Ok(None);
    };
    let roots = roots_for(store, &project_id).await?;
    let recency = recency_for(store, &project_id).await?;
    Ok(Some(stored(project, roots, recency)))
}

pub(super) async fn create_project(
    store: &AntflyThreadStore,
    params: CreateProjectParams,
) -> ThreadStoreResult<CreatedProject> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    if let Some(row) = sql
        .fetch_optional(
            "SELECT project_id FROM codex_project_idempotency_keys WHERE key = $1",
            sql_params![params.idempotency_key.clone()],
        )
        .await
        .map_err(internal)?
    {
        let project_id = row.string("project_id").map_err(internal)?;
        return match load_project(store, &project_id).await? {
            Some(project) => {
                let roots = roots_for(store, &project_id).await?;
                let recency = recency_for(store, &project_id).await?;
                Ok(CreatedProject {
                    project: stored(project, roots, recency),
                    created: false,
                })
            }
            None => Err(invalid(format!(
                "idempotency key refers to deleted project: {}",
                params.idempotency_key
            ))),
        };
    }

    for thread_id_str in &params.thread_ids {
        let thread_id = ThreadId::from_string(thread_id_str).ok().ok_or_else(|| {
            ThreadStoreError::Internal {
                message: format!("thread not found: {thread_id_str}"),
            }
        })?;
        store
            .load_record(thread_id)
            .await?
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("thread not found: {thread_id_str}"),
            })?;
    }

    let id = uuid::Uuid::now_v7().to_string();
    let now = chrono::Utc::now().timestamp_millis();
    let mut tx = sql.begin().await.map_err(internal)?;
    let position = tx
        .fetch_optional("SELECT MAX(position) AS value FROM codex_projects", vec![])
        .await
        .map_err(internal)?
        .and_then(|row| row.opt_i64("value").ok().flatten())
        .map_or(0, |max| max + 1);
    let metadata_json =
        serde_json::to_value(&params.metadata).map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize project metadata: {err}"),
        })?;
    tx.execute(
        "INSERT INTO codex_projects (id, name, metadata, position, created_at_ms, updated_at_ms) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        sql_params![
            id.clone(),
            params.name.clone(),
            metadata_json,
            position,
            now,
            now
        ],
    )
    .await
    .map_err(internal)?;
    let roots: Vec<String> = params.roots.into_iter().map(|root| root.path).collect();
    replace_roots(&mut tx, &id, &roots).await?;
    for thread_id_str in &params.thread_ids {
        tx.execute(
            "UPDATE codex_threads SET project_id = $1 WHERE id = $2",
            sql_params![id.clone(), thread_id_str.clone()],
        )
        .await
        .map_err(internal)?;
    }
    tx.execute(
        "INSERT INTO codex_project_idempotency_keys (key, project_id, created_at_ms) VALUES ($1, $2, $3)",
        sql_params![params.idempotency_key.clone(), id.clone(), now],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;

    let recency_at_ms = recency_for(store, &id).await?;
    let roots = roots
        .into_iter()
        .map(|path| StoredProjectRoot { path })
        .collect();
    let project = ProjectRow {
        id,
        name: params.name,
        metadata: params.metadata,
        position,
        created_at_ms: now,
        updated_at_ms: now,
    };
    Ok(CreatedProject {
        project: stored(project, roots, recency_at_ms),
        created: true,
    })
}

pub(super) async fn update_project(
    store: &AntflyThreadStore,
    params: UpdateProjectParams,
) -> ThreadStoreResult<Option<UpdatedProject>> {
    let Some(current) = load_project(store, &params.project_id).await? else {
        return Ok(None);
    };
    let current_roots = roots_for(store, &params.project_id).await?;
    let next_name = params.name.unwrap_or_else(|| current.name.clone());
    let next_roots = params.roots.unwrap_or_else(|| current_roots.clone());
    let next_metadata = params.metadata.unwrap_or_else(|| current.metadata.clone());
    if next_name == current.name && next_roots == current_roots && next_metadata == current.metadata
    {
        let recency = recency_for(store, &params.project_id).await?;
        return Ok(Some(UpdatedProject {
            project: stored(current, current_roots, recency),
            changed: false,
        }));
    }
    let now = chrono::Utc::now().timestamp_millis();
    let metadata_json =
        serde_json::to_value(&next_metadata).map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize project metadata: {err}"),
        })?;
    let sql = store.antfly().sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    tx.execute(
        "UPDATE codex_projects SET name = $1, metadata = $2, updated_at_ms = $3 WHERE id = $4",
        sql_params![
            next_name.clone(),
            metadata_json,
            now,
            params.project_id.clone()
        ],
    )
    .await
    .map_err(internal)?;
    if next_roots != current_roots {
        let roots: Vec<String> = next_roots.iter().map(|root| root.path.clone()).collect();
        replace_roots(&mut tx, &params.project_id, &roots).await?;
    }
    tx.commit().await.map_err(internal)?;
    let recency = recency_for(store, &params.project_id).await?;
    let project = ProjectRow {
        id: params.project_id,
        name: next_name,
        metadata: next_metadata,
        position: current.position,
        created_at_ms: current.created_at_ms,
        updated_at_ms: now,
    };
    Ok(Some(UpdatedProject {
        project: stored(project, next_roots, recency),
        changed: true,
    }))
}

pub(super) async fn move_project(
    store: &AntflyThreadStore,
    params: MoveProjectParams,
) -> ThreadStoreResult<Option<ProjectMoveOutcome>> {
    let sql = store.antfly().sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let rows = tx
        .fetch_all(
            "SELECT id FROM codex_projects ORDER BY position ASC, id ASC",
            vec![],
        )
        .await
        .map_err(internal)?;
    let mut project_ids = rows
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    let Some(current_index) = project_ids.iter().position(|id| id == &params.project_id) else {
        return Ok(None);
    };
    if params.before_project_id.as_deref() == Some(params.project_id.as_str()) {
        return Err(invalid(format!(
            "project {} cannot be moved before itself",
            params.project_id
        )));
    }
    let original_order = project_ids.clone();
    let moved = project_ids.remove(current_index);
    let next_index = match &params.before_project_id {
        Some(before) => project_ids
            .iter()
            .position(|id| id == before)
            .ok_or_else(|| invalid(format!("before project not found: {before}")))?,
        None => project_ids.len(),
    };
    project_ids.insert(next_index, moved);
    if project_ids == original_order {
        return Ok(Some(ProjectMoveOutcome::Unchanged));
    }
    for (index, id) in project_ids.iter().enumerate() {
        tx.execute(
            "UPDATE codex_projects SET position = $1 WHERE id = $2",
            sql_params![index as i64, id.clone()],
        )
        .await
        .map_err(internal)?;
    }
    tx.execute(
        "UPDATE codex_projects SET updated_at_ms = $1 WHERE id = $2",
        sql_params![
            chrono::Utc::now().timestamp_millis(),
            params.project_id.clone()
        ],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(Some(ProjectMoveOutcome::Moved))
}

pub(super) async fn delete_project(
    store: &AntflyThreadStore,
    project_id: String,
) -> ThreadStoreResult<Option<DeletedProject>> {
    if load_project(store, &project_id).await?.is_none() {
        return Ok(None);
    }
    let sql = store.antfly().sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let active = tx
        .fetch_all(
            "SELECT id FROM codex_threads WHERE project_id = $1 AND archived = false ORDER BY id ASC",
            sql_params![project_id.clone()],
        )
        .await
        .map_err(internal)?
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    let archived = tx
        .fetch_all(
            "SELECT id FROM codex_threads WHERE project_id = $1 AND archived = true ORDER BY id ASC",
            sql_params![project_id.clone()],
        )
        .await
        .map_err(internal)?
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    tx.execute(
        "UPDATE codex_threads SET project_id = NULL WHERE project_id = $1",
        sql_params![project_id.clone()],
    )
    .await
    .map_err(internal)?;
    tx.execute(
        "DELETE FROM codex_projects WHERE id = $1",
        sql_params![project_id.clone()],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(Some(DeletedProject {
        affected_active_thread_ids: active,
        affected_archived_thread_ids: archived,
    }))
}

async fn replace_roots(
    tx: &mut SqlTx,
    project_id: &str,
    roots: &[String],
) -> ThreadStoreResult<()> {
    tx.execute(
        "DELETE FROM codex_project_roots WHERE project_id = $1",
        sql_params![project_id],
    )
    .await
    .map_err(internal)?;
    for (position, path) in roots.iter().enumerate() {
        tx.execute(
            "INSERT INTO codex_project_roots (project_id, position, path) VALUES ($1, $2, $3)",
            sql_params![project_id, position as i64, path.clone()],
        )
        .await
        .map_err(internal)?;
    }
    Ok(())
}
