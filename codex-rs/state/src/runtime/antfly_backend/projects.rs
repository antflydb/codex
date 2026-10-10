//! Antfly-backed projects (`codex_projects` / `codex_project_roots` /
//! `codex_project_idempotency_keys`), the same tables
//! `codex-thread-store`'s `AntflyThreadStore` uses
//! (`thread-store/src/antfly/projects.rs`).
//!
//! Lists the full project set and paginates in Rust rather than
//! reproducing `state/src/runtime/projects.rs`'s exact SQL cursor format
//! (`v1|key|order|value|id`): a cursor only round-trips through the backend
//! that minted it, so the two backends' cursors never need to match byte
//! for byte, only to behave the same way (same filters, same order, same
//! page boundaries).

use std::collections::BTreeMap;

use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;

use super::internal;
use crate::Project;
use crate::ProjectRoot;
use crate::ProjectSortKey;
use crate::ProjectsPage;
use crate::SortDirection;

fn project_row(row: &SqlRow) -> anyhow::Result<Project> {
    Ok(Project {
        id: row.string("id").map_err(internal)?,
        name: row.string("name").map_err(internal)?,
        roots: Vec::new(),
        metadata: serde_json::from_value(row.json("metadata").map_err(internal)?)?,
        position: row.i64("position").map_err(internal)?,
        created_at_ms: row.i64("created_at_ms").map_err(internal)?,
        updated_at_ms: row.i64("updated_at_ms").map_err(internal)?,
        recency_at_ms: None,
    })
}

async fn roots_for(antfly: &Antfly, project_id: &str) -> anyhow::Result<Vec<ProjectRoot>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT path FROM codex_project_roots WHERE project_id = $1 ORDER BY position ASC",
            sql_params![project_id],
        )
        .await
        .map_err(internal)?;
    rows.iter()
        .map(|row| {
            Ok(ProjectRoot {
                path: row.string("path").map_err(internal)?,
            })
        })
        .collect()
}

async fn recency_for(antfly: &Antfly, project_id: &str) -> anyhow::Result<Option<i64>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT MAX(recency_at_ms) AS value FROM codex_threads WHERE project_id = $1 AND archived = false",
            sql_params![project_id],
        )
        .await
        .map_err(internal)?;
    Ok(row.and_then(|row| row.opt_i64("value").ok().flatten()))
}

async fn load_project(antfly: &Antfly, id: &str) -> anyhow::Result<Option<Project>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT id, name, metadata, position, created_at_ms, updated_at_ms FROM codex_projects WHERE id = $1",
            sql_params![id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| project_row(&row)).transpose()
}

async fn hydrate(antfly: &Antfly, mut project: Project) -> anyhow::Result<Project> {
    project.roots = roots_for(antfly, &project.id).await?;
    project.recency_at_ms = recency_for(antfly, &project.id).await?;
    Ok(project)
}

pub(crate) async fn set_thread_project(
    antfly: &Antfly,
    thread_id: &str,
    project_id: Option<&str>,
) -> anyhow::Result<Option<Option<String>>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    if let Some(project_id) = project_id {
        let exists = tx
            .fetch_optional(
                "SELECT 1 AS present FROM codex_projects WHERE id = $1",
                sql_params![project_id],
            )
            .await
            .map_err(internal)?
            .is_some();
        if !exists {
            anyhow::bail!("project not found: {project_id}");
        }
    }
    let previous = tx
        .fetch_optional(
            "SELECT project_id FROM codex_threads WHERE id = $1",
            sql_params![thread_id],
        )
        .await
        .map_err(internal)?;
    let Some(previous) = previous else {
        return Ok(None);
    };
    let previous = previous.opt_string("project_id").map_err(internal)?;
    if previous.as_deref() != project_id {
        let statement = match project_id {
            Some(_) => "UPDATE codex_threads SET project_id = $1 WHERE id = $2",
            None => "UPDATE codex_threads SET project_id = NULL WHERE id = $1",
        };
        let params = match project_id {
            Some(project_id) => sql_params![project_id, thread_id],
            None => sql_params![thread_id],
        };
        tx.execute(statement, params).await.map_err(internal)?;
    }
    tx.commit().await.map_err(internal)?;
    Ok(Some(previous))
}

pub(crate) async fn list_projects(
    antfly: &Antfly,
    cursor: Option<&str>,
    limit: usize,
    sort_key: ProjectSortKey,
    sort_direction: SortDirection,
) -> anyhow::Result<ProjectsPage> {
    anyhow::ensure!(limit > 0, "project limit must be positive");
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT id, name, metadata, position, created_at_ms, updated_at_ms FROM codex_projects",
            vec![],
        )
        .await
        .map_err(internal)?;
    let mut projects = Vec::with_capacity(rows.len());
    for row in &rows {
        projects.push(hydrate(antfly, project_row(row)?).await?);
    }
    sort_projects(&mut projects, sort_key, sort_direction);
    let start = match cursor {
        Some(cursor) => projects
            .iter()
            .position(|project| project.id == cursor)
            .map(|index| index + 1)
            .ok_or_else(|| anyhow::anyhow!("invalid project cursor: not found"))?,
        None => 0,
    };
    let mut page: Vec<Project> = projects.into_iter().skip(start).collect();
    let has_more = page.len() > limit;
    page.truncate(limit);
    let next_cursor = has_more
        .then(|| page.last().map(|project| project.id.clone()))
        .flatten();
    Ok(ProjectsPage {
        projects: page,
        next_cursor,
    })
}

fn sort_projects(projects: &mut [Project], sort_key: ProjectSortKey, direction: SortDirection) {
    match sort_key {
        ProjectSortKey::Position => {
            projects.sort_by(|a, b| a.position.cmp(&b.position).then_with(|| a.id.cmp(&b.id)));
            if direction == SortDirection::Desc {
                projects.reverse();
            }
        }
        ProjectSortKey::RecencyAt => {
            projects.sort_by(|a, b| {
                let ordering = match (a.recency_at_ms, b.recency_at_ms) {
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

pub(crate) async fn get_project(antfly: &Antfly, id: &str) -> anyhow::Result<Option<Project>> {
    let Some(project) = load_project(antfly, id).await? else {
        return Ok(None);
    };
    Ok(Some(hydrate(antfly, project).await?))
}

pub(crate) async fn get_project_by_idempotency_key(
    antfly: &Antfly,
    idempotency_key: &str,
) -> anyhow::Result<Option<Project>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let project_id = sql
        .fetch_optional(
            "SELECT project_id FROM codex_project_idempotency_keys WHERE key = $1",
            sql_params![idempotency_key],
        )
        .await
        .map_err(internal)?;
    let Some(project_id) = project_id else {
        return Ok(None);
    };
    let project_id = project_id.string("project_id").map_err(internal)?;
    let Some(project) = load_project(antfly, &project_id).await? else {
        anyhow::bail!("idempotency key refers to deleted project: {idempotency_key}");
    };
    Ok(Some(hydrate(antfly, project).await?))
}

async fn replace_roots(
    tx: &mut SqlTx,
    project_id: &str,
    roots: &[ProjectRoot],
) -> anyhow::Result<()> {
    tx.execute(
        "DELETE FROM codex_project_roots WHERE project_id = $1",
        sql_params![project_id],
    )
    .await
    .map_err(internal)?;
    for (position, root) in roots.iter().enumerate() {
        tx.execute(
            "INSERT INTO codex_project_roots (project_id, position, path) VALUES ($1, $2, $3)",
            sql_params![project_id, position as i64, root.path.clone()],
        )
        .await
        .map_err(internal)?;
    }
    Ok(())
}

pub(crate) async fn create_project(
    antfly: &Antfly,
    name: String,
    roots: Vec<ProjectRoot>,
    metadata: BTreeMap<String, String>,
    thread_ids: &[String],
    idempotency_key: &str,
) -> anyhow::Result<(Project, bool)> {
    if let Some(project) = get_project_by_idempotency_key(antfly, idempotency_key).await? {
        return Ok((project, false));
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    for thread_id in thread_ids {
        let exists = tx
            .fetch_optional(
                "SELECT 1 AS present FROM codex_threads WHERE id = $1",
                sql_params![thread_id.clone()],
            )
            .await
            .map_err(internal)?
            .is_some();
        if !exists {
            anyhow::bail!("thread not found: {thread_id}");
        }
    }
    let id = uuid::Uuid::now_v7().to_string();
    let now = chrono::Utc::now().timestamp_millis();
    let position = tx
        .fetch_optional("SELECT MAX(position) AS value FROM codex_projects", vec![])
        .await
        .map_err(internal)?
        .and_then(|row| row.opt_i64("value").ok().flatten())
        .map_or(0, |max| max + 1);
    let metadata_json = serde_json::to_value(&metadata)?;
    tx.execute(
        "INSERT INTO codex_projects (id, name, metadata, position, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, $4, $5, $6)",
        sql_params![id.clone(), name.clone(), metadata_json, position, now, now],
    )
    .await
    .map_err(internal)?;
    replace_roots(&mut tx, &id, &roots).await?;
    for thread_id in thread_ids {
        tx.execute(
            "UPDATE codex_threads SET project_id = $1 WHERE id = $2",
            sql_params![id.clone(), thread_id.clone()],
        )
        .await
        .map_err(internal)?;
    }
    tx.execute(
        "INSERT INTO codex_project_idempotency_keys (key, project_id, created_at_ms) VALUES ($1, $2, $3)",
        sql_params![idempotency_key, id.clone(), now],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok((
        Project {
            id,
            name,
            roots,
            metadata,
            position,
            created_at_ms: now,
            updated_at_ms: now,
            recency_at_ms: None,
        },
        true,
    ))
}

pub(crate) async fn update_project(
    antfly: &Antfly,
    id: &str,
    name: Option<String>,
    roots: Option<Vec<ProjectRoot>>,
    metadata: Option<BTreeMap<String, String>>,
) -> anyhow::Result<Option<(Project, bool)>> {
    let Some(current) = get_project(antfly, id).await? else {
        return Ok(None);
    };
    let next_name = name.unwrap_or_else(|| current.name.clone());
    let next_roots = roots.unwrap_or_else(|| current.roots.clone());
    let next_metadata = metadata.unwrap_or_else(|| current.metadata.clone());
    if next_name == current.name && next_roots == current.roots && next_metadata == current.metadata
    {
        return Ok(Some((current, false)));
    }
    let now = chrono::Utc::now().timestamp_millis();
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let metadata_json = serde_json::to_value(&next_metadata)?;
    tx.execute(
        "UPDATE codex_projects SET name = $1, metadata = $2, updated_at_ms = $3 WHERE id = $4",
        sql_params![next_name.clone(), metadata_json, now, id],
    )
    .await
    .map_err(internal)?;
    if next_roots != current.roots {
        replace_roots(&mut tx, id, &next_roots).await?;
    }
    tx.commit().await.map_err(internal)?;
    Ok(Some((
        Project {
            id: id.to_string(),
            name: next_name,
            roots: next_roots,
            metadata: next_metadata,
            position: current.position,
            created_at_ms: current.created_at_ms,
            updated_at_ms: now,
            recency_at_ms: current.recency_at_ms,
        },
        true,
    )))
}

pub(crate) async fn move_project(
    antfly: &Antfly,
    project_id: &str,
    before_project_id: Option<&str>,
) -> anyhow::Result<Option<bool>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let rows = tx
        .fetch_all(
            "SELECT id FROM codex_projects ORDER BY position ASC, id ASC",
            vec![],
        )
        .await
        .map_err(internal)?;
    let mut project_ids: Vec<String> = rows
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<anyhow::Result<_>>()?;
    let Some(current_index) = project_ids.iter().position(|id| id == project_id) else {
        return Ok(None);
    };
    if before_project_id == Some(project_id) {
        anyhow::bail!("project {project_id} cannot be moved before itself");
    }
    let original_order = project_ids.clone();
    let moved = project_ids.remove(current_index);
    let next_index = if let Some(before_project_id) = before_project_id {
        project_ids
            .iter()
            .position(|id| id == before_project_id)
            .ok_or_else(|| anyhow::anyhow!("before project not found: {before_project_id}"))?
    } else {
        project_ids.len()
    };
    project_ids.insert(next_index, moved);
    if project_ids == original_order {
        return Ok(Some(false));
    }
    for (position, id) in project_ids.iter().enumerate() {
        tx.execute(
            "UPDATE codex_projects SET position = $1 WHERE id = $2",
            sql_params![position as i64, id.clone()],
        )
        .await
        .map_err(internal)?;
    }
    tx.execute(
        "UPDATE codex_projects SET updated_at_ms = $1 WHERE id = $2",
        sql_params![chrono::Utc::now().timestamp_millis(), project_id],
    )
    .await
    .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(Some(true))
}

pub(crate) async fn delete_project(
    antfly: &Antfly,
    id: &str,
) -> anyhow::Result<Option<(Vec<String>, Vec<String>)>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let exists = tx
        .fetch_optional(
            "SELECT 1 AS present FROM codex_projects WHERE id = $1",
            sql_params![id],
        )
        .await
        .map_err(internal)?
        .is_some();
    if !exists {
        return Ok(None);
    }
    let active = tx
        .fetch_all("SELECT id FROM codex_threads WHERE project_id = $1 AND archived = false ORDER BY id ASC", sql_params![id])
        .await
        .map_err(internal)?
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let archived = tx
        .fetch_all("SELECT id FROM codex_threads WHERE project_id = $1 AND archived = true ORDER BY id ASC", sql_params![id])
        .await
        .map_err(internal)?
        .iter()
        .map(|row| row.string("id").map_err(internal))
        .collect::<anyhow::Result<Vec<_>>>()?;
    tx.execute(
        "UPDATE codex_threads SET project_id = NULL WHERE project_id = $1",
        sql_params![id],
    )
    .await
    .map_err(internal)?;
    tx.execute("DELETE FROM codex_projects WHERE id = $1", sql_params![id])
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(Some((active, archived)))
}
