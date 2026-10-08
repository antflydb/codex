//! Reads thread metadata written by `AntflyThreadStore`
//! (`codex-thread-store`, `thread-store/src/antfly/`).
//!
//! `codex-state` cannot depend on `codex-thread-store`: that crate already
//! depends on `codex-state` (for `ThreadSection`/`PINNED_THREAD_SECTION_*`),
//! so a reverse dependency would cycle. Instead of sharing Rust types, this
//! module re-parses the `ts:t:{thread_id}` document (a JSON `ThreadRecord`,
//! see `thread-store/src/antfly/record.rs` and `keys.rs`) by the field names
//! those types serialize to. That JSON shape is a documented wire contract
//! between the two crates, not a type dependency; keep the field paths below
//! in sync with `ThreadRecord`/`CreateThreadParams`/`ThreadMetadataPatch` if
//! they change.
//!
//! Only the subset of fields the `StateRuntime` Antfly backend needs is
//! projected here: `get_thread`, `get_thread_memory_mode`,
//! `set_thread_preview_if_empty`, the spawn-edge lookups (which need a
//! thread's `agent_path`), and the memory pipeline's
//! `enabled_thread_metadata` / `claim_stage1_jobs_for_startup` scan.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::ThreadId;
use serde_json::Value;

use super::internal;

const THREAD_PREFIX: &str = "ts:t:";

pub(crate) fn thread_key(thread_id: ThreadId) -> String {
    format!("{THREAD_PREFIX}{thread_id}")
}

/// Projection of a `ts:t:{thread_id}` document.
#[derive(Clone, Debug)]
pub(crate) struct ThreadView {
    pub(crate) thread_id: ThreadId,
    pub(crate) cwd: PathBuf,
    pub(crate) model_provider: String,
    /// Best-effort textual session-source discriminant (see module docs).
    pub(crate) source: String,
    pub(crate) git_branch: Option<String>,
    pub(crate) preview: Option<String>,
    pub(crate) agent_path: Option<String>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) archived_at: Option<DateTime<Utc>>,
}

fn str_at<'a>(doc: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = doc;
    for segment in path {
        current = current.get(segment)?;
    }
    current.as_str()
}

fn datetime_at(doc: &Value, path: &[&str]) -> Option<DateTime<Utc>> {
    str_at(doc, path)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// A thread's effective session-source discriminant, matching the shape
/// `SessionSource` tends to serialize to (a bare string for unit variants,
/// or `{"type": "..."}` for struct/tuple variants).
fn source_discriminant(doc: &Value, path: &[&str]) -> Option<String> {
    let mut current = doc;
    for segment in path {
        current = current.get(segment)?;
    }
    match current {
        Value::String(value) => Some(value.clone()),
        Value::Object(map) => map
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| map.keys().next().cloned()),
        _ => None,
    }
}

impl ThreadView {
    fn from_doc(thread_id: ThreadId, doc: &Value) -> Self {
        // `patch.*` overrides `created.metadata.*`/`created.source` when
        // present, mirroring `ThreadRecord::cwd()`/`model_provider()`/
        // `source()` in `thread-store/src/antfly/record.rs`.
        let cwd = str_at(doc, &["patch", "cwd"])
            .or_else(|| str_at(doc, &["created", "metadata", "cwd"]))
            .map(PathBuf::from)
            .unwrap_or_default();
        let model_provider = str_at(doc, &["patch", "model_provider"])
            .or_else(|| str_at(doc, &["created", "metadata", "model_provider"]))
            .unwrap_or_default()
            .to_string();
        let source = source_discriminant(doc, &["patch", "source"])
            .or_else(|| source_discriminant(doc, &["created", "source"]))
            .unwrap_or_default();
        let git_branch = str_at(doc, &["patch", "git_info", "branch"]).map(str::to_string);
        let preview = str_at(doc, &["patch", "preview"])
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let agent_path = str_at(doc, &["patch", "agent_path"])
            .or_else(|| str_at(doc, &["created", "source", "agent_path"]))
            .map(str::to_string);
        let updated_at = datetime_at(doc, &["patch", "updated_at"])
            .or_else(|| datetime_at(doc, &["materialized_at"]))
            .unwrap_or_else(Utc::now);
        let archived_at = datetime_at(doc, &["archived_at"]);
        Self {
            thread_id,
            cwd,
            model_provider,
            source,
            git_branch,
            preview,
            agent_path,
            updated_at,
            archived_at,
        }
    }
}

/// Loads the raw `ts:t:{thread_id}` document, if the thread exists.
async fn load_doc(antfly: &Arc<Antfly>, thread_id: ThreadId) -> anyhow::Result<Option<Value>> {
    antfly
        .get(thread_key(thread_id))
        .await
        .map_err(internal)
        .map(|doc| doc.map(codex_antfly::strip_reserved))
}

pub(crate) async fn load_thread_view(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadView>> {
    Ok(load_doc(antfly, thread_id)
        .await?
        .map(|doc| ThreadView::from_doc(thread_id, &doc)))
}

/// Full `StateRuntime::get_thread` projection, for callers outside the
/// memory pipeline (app-server request processors, goal continuation,
/// thread deletion) that need the general-purpose `ThreadMetadata` shape.
///
/// Best-effort: a handful of fields (`approval_mode`, `sandbox_policy`,
/// `reasoning_effort`, `thread_source`, `section`, `git_sha`,
/// `git_origin_url`) depend on enum/struct JSON shapes
/// `thread-store`'s types don't document as a stable wire contract, so
/// they degrade to safe defaults (empty string / `None`) rather than a
/// best-guess parse that could silently be wrong. Everything the memory
/// pipeline and spawn-edge/preview/polluted-flag paths actually read
/// (`id`, `cwd`, `model_provider`, `source`, `git_branch`, `preview`,
/// `agent_path`, `updated_at`, `archived_at`) is exact.
pub(crate) async fn get_thread_metadata(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<crate::ThreadMetadata>> {
    let Some(doc) = load_doc(antfly, thread_id).await? else {
        return Ok(None);
    };
    let view = ThreadView::from_doc(thread_id, &doc);
    let created_at = datetime_at(&doc, &["patch", "created_at"])
        .or_else(|| datetime_at(&doc, &["materialized_at"]))
        .unwrap_or(view.updated_at);
    let recency_at = datetime_at(&doc, &["patch", "advance_recency_at"]).unwrap_or(view.updated_at);
    let title = str_at(&doc, &["patch", "title"])
        .unwrap_or_default()
        .to_string();
    let name = str_at(&doc, &["patch", "name"]).map(str::to_string);
    let model = str_at(&doc, &["patch", "model"]).map(str::to_string);
    let cli_version = str_at(&doc, &["patch", "cli_version"])
        .unwrap_or_default()
        .to_string();
    let first_user_message = str_at(&doc, &["patch", "first_user_message"]).map(str::to_string);
    let project_id = str_at(&doc, &["patch", "project_id"]).map(str::to_string);
    let daybreak_enabled = doc
        .get("patch")
        .and_then(|patch| patch.get("daybreak_enabled"))
        .and_then(Value::as_bool);
    let originator = str_at(&doc, &["patch", "originator"])
        .or_else(|| str_at(&doc, &["created", "originator"]))
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let creator_user_id = str_at(&doc, &["patch", "creator_user_id"])
        .or_else(|| str_at(&doc, &["created", "creator_user_id"]))
        .map(str::to_string);
    let creator_account_id = str_at(&doc, &["patch", "creator_account_id"])
        .or_else(|| str_at(&doc, &["created", "creator_account_id"]))
        .map(str::to_string);
    let agent_nickname = str_at(&doc, &["patch", "agent_nickname"]).map(str::to_string);
    let agent_role = str_at(&doc, &["patch", "agent_role"]).map(str::to_string);
    let history_mode = str_at(&doc, &["created", "history_mode"])
        .and_then(|value| value.parse().ok())
        .unwrap_or(codex_protocol::protocol::ThreadHistoryMode::Legacy);

    Ok(Some(crate::ThreadMetadata {
        originator,
        creator_user_id,
        creator_account_id,
        id: thread_id,
        rollout_path: PathBuf::new(),
        created_at,
        updated_at: view.updated_at,
        recency_at,
        source: view.source,
        history_mode,
        thread_source: None,
        agent_nickname,
        agent_role,
        agent_path: view.agent_path,
        model_provider: view.model_provider,
        model,
        reasoning_effort: None,
        cwd: view.cwd,
        cli_version,
        title,
        name,
        preview: view.preview,
        sandbox_policy: String::new(),
        approval_mode: String::new(),
        tokens_used: 0,
        first_user_message,
        archived_at: view.archived_at,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id,
        daybreak_enabled,
        git_sha: None,
        git_branch: view.git_branch,
        git_origin_url: None,
    }))
}

/// Scans every thread record. Used only by the best-effort memory-pipeline
/// startup scan (`claim_stage1_jobs_for_startup`); callers on a hot path
/// should prefer `load_thread_view` for a single thread.
pub(crate) async fn scan_all_threads(antfly: &Arc<Antfly>) -> anyhow::Result<Vec<ThreadView>> {
    let documents = antfly
        .scan(ScanRequest::prefix(THREAD_PREFIX))
        .await
        .map_err(internal)?;
    Ok(documents
        .into_iter()
        .filter_map(|document| {
            let thread_id = document
                .key
                .strip_prefix(THREAD_PREFIX)
                .and_then(|id| ThreadId::from_string(id).ok())?;
            let doc = codex_antfly::strip_reserved(document.doc);
            Some(ThreadView::from_doc(thread_id, &doc))
        })
        .collect())
}

/// Sets `patch.preview` on the thread record if it is currently empty,
/// matching `UPDATE threads SET preview = ? WHERE id = ? AND preview = ''`.
/// Returns `false` (no error) if the thread does not exist, `preview` is
/// blank, or a preview is already set.
pub(crate) async fn set_preview_if_empty(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    preview: &str,
) -> anyhow::Result<bool> {
    let preview = preview.trim();
    if preview.is_empty() {
        return Ok(false);
    }
    let _guard = antfly.lock().await;
    let Some(mut doc) = load_doc(antfly, thread_id).await? else {
        return Ok(false);
    };
    let has_preview = str_at(&doc, &["patch", "preview"]).is_some_and(|value| !value.is_empty());
    if has_preview {
        return Ok(false);
    }
    let Some(patch) = doc.get_mut("patch").and_then(Value::as_object_mut) else {
        return Ok(false);
    };
    patch.insert("preview".to_string(), Value::String(preview.to_string()));
    antfly
        .write(vec![Write::put(thread_key(thread_id), doc)])
        .await
        .map_err(internal)?;
    Ok(true)
}

const THREAD_MODE_PREFIX: &str = "st:mem:polluted:";

fn mode_key(thread_id: ThreadId) -> String {
    format!("{THREAD_MODE_PREFIX}{thread_id}")
}

/// `threads.memory_mode`'s only mutation reachable through the Antfly thread
/// store is `mark_thread_memory_mode_polluted` (the SQLite `'disabled'`
/// state is set exclusively by `LocalThreadStore`-era call sites, which
/// never run against `AntflyThreadStore`). So this backend tracks only a
/// presence marker: absent means `"enabled"` (the SQLite default), present
/// means `"polluted"`.
pub(crate) async fn get_memory_mode(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<String>> {
    if load_doc(antfly, thread_id).await?.is_none() {
        return Ok(None);
    }
    let polluted = antfly
        .get(mode_key(thread_id))
        .await
        .map_err(internal)?
        .is_some();
    Ok(Some(
        if polluted { "polluted" } else { "enabled" }.to_string(),
    ))
}

pub(crate) async fn is_memory_mode_enabled(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    Ok(get_memory_mode(antfly, thread_id).await?.as_deref() == Some("enabled"))
}

/// Marks a thread polluted. Returns whether the marker transitioned from
/// absent to present (mirrors `rows_affected > 0` on the SQLite
/// `WHERE memory_mode != 'polluted'` guard).
pub(crate) async fn mark_memory_mode_polluted(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let _guard = antfly.lock().await;
    if antfly
        .get(mode_key(thread_id))
        .await
        .map_err(internal)?
        .is_some()
    {
        return Ok(false);
    }
    antfly
        .write(vec![Write::put(mode_key(thread_id), serde_json::json!({}))])
        .await
        .map_err(internal)?;
    Ok(true)
}

const EDGE_PARENT_PREFIX: &str = "st:edgep:";
const EDGE_CHILD_PREFIX: &str = "st:edgec:";

fn edge_parent_prefix(parent: ThreadId) -> String {
    format!("{EDGE_PARENT_PREFIX}{parent}:")
}

fn edge_parent_key(parent: ThreadId, child: ThreadId) -> String {
    format!("{}{child}", edge_parent_prefix(parent))
}

fn edge_child_key(child: ThreadId) -> String {
    format!("{EDGE_CHILD_PREFIX}{child}")
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct EdgeDoc {
    parent_thread_id: ThreadId,
    status: crate::DirectionalThreadSpawnEdgeStatus,
}

pub(crate) async fn upsert_spawn_edge(
    antfly: &Arc<Antfly>,
    parent: ThreadId,
    child: ThreadId,
    status: crate::DirectionalThreadSpawnEdgeStatus,
) -> anyhow::Result<()> {
    let _guard = antfly.lock().await;
    // A child has at most one parent edge; replace any previous one (which
    // may have a different parent) the same way the SQLite
    // `ON CONFLICT(child_thread_id) DO UPDATE` upsert does.
    let mut writes = Vec::with_capacity(3);
    if let Some(previous) = antfly
        .get_as::<EdgeDoc>(edge_child_key(child))
        .await
        .map_err(internal)?
        && previous.parent_thread_id != parent
    {
        writes.push(Write::delete(edge_parent_key(
            previous.parent_thread_id,
            child,
        )));
    }
    let doc = serde_json::to_value(EdgeDoc {
        parent_thread_id: parent,
        status,
    })?;
    writes.push(Write::put(edge_child_key(child), doc.clone()));
    writes.push(Write::put(edge_parent_key(parent, child), doc));
    antfly.write(writes).await.map_err(internal)
}

pub(crate) async fn set_spawn_edge_status(
    antfly: &Arc<Antfly>,
    child: ThreadId,
    status: crate::DirectionalThreadSpawnEdgeStatus,
) -> anyhow::Result<()> {
    let _guard = antfly.lock().await;
    let Some(mut edge) = antfly
        .get_as::<EdgeDoc>(edge_child_key(child))
        .await
        .map_err(internal)?
    else {
        return Ok(());
    };
    let parent = edge.parent_thread_id;
    edge.status = status;
    let doc = serde_json::to_value(&edge)?;
    antfly
        .write(vec![
            Write::put(edge_child_key(child), doc.clone()),
            Write::put(edge_parent_key(parent, child), doc),
        ])
        .await
        .map_err(internal)
}

pub(crate) async fn list_spawn_children(
    antfly: &Arc<Antfly>,
    parent: ThreadId,
    status: Option<crate::DirectionalThreadSpawnEdgeStatus>,
) -> anyhow::Result<Vec<ThreadId>> {
    let documents = antfly
        .scan(ScanRequest::prefix(&edge_parent_prefix(parent)))
        .await
        .map_err(internal)?;
    let mut children = Vec::new();
    for document in documents {
        let edge: EdgeDoc = serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
        if status.is_some_and(|status| status != edge.status) {
            continue;
        }
        let Some(child) = document
            .key
            .strip_prefix(&edge_parent_prefix(parent))
            .and_then(|id| ThreadId::from_string(id).ok())
        else {
            continue;
        };
        children.push(child);
    }
    children.sort_by_key(ThreadId::to_string);
    Ok(children)
}

/// Breadth-first walk of the spawn-edge tree under `root`, matching the
/// `WITH RECURSIVE` CTE's `ORDER BY depth ASC, child_thread_id ASC`.
pub(crate) async fn list_spawn_descendants(
    antfly: &Arc<Antfly>,
    root: ThreadId,
    status: Option<crate::DirectionalThreadSpawnEdgeStatus>,
) -> anyhow::Result<Vec<ThreadId>> {
    let mut ordered = Vec::new();
    let mut frontier = vec![root];
    let mut visited = std::collections::HashSet::new();
    visited.insert(root);
    while !frontier.is_empty() {
        let mut next_frontier = Vec::new();
        for parent in frontier {
            // Status filters every level of the walk (a non-matching edge
            // prunes the whole subtree below it), matching the SQLite CTE.
            let children = list_spawn_children(antfly, parent, status).await?;
            for child in children {
                if visited.insert(child) {
                    ordered.push(child);
                    next_frontier.push(child);
                }
            }
        }
        frontier = next_frontier;
    }
    Ok(ordered)
}

/// Resolves exactly one match, erroring like SQLite's `LIMIT 2` ambiguity
/// trap rather than silently picking one.
fn one_match(mut matches: Vec<ThreadId>, agent_path: &str) -> anyhow::Result<Option<ThreadId>> {
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => anyhow::bail!("multiple agents found for canonical path `{agent_path}`"),
    }
}

pub(crate) async fn find_spawn_child_by_path(
    antfly: &Arc<Antfly>,
    parent: ThreadId,
    agent_path: &str,
) -> anyhow::Result<Option<ThreadId>> {
    let mut matches = Vec::new();
    for child in list_spawn_children(antfly, parent, None).await? {
        if let Some(view) = load_thread_view(antfly, child).await?
            && view.agent_path.as_deref() == Some(agent_path)
        {
            matches.push(child);
        }
    }
    matches.sort_by_key(ThreadId::to_string);
    one_match(matches, agent_path)
}

pub(crate) async fn find_spawn_descendant_by_path(
    antfly: &Arc<Antfly>,
    root: ThreadId,
    agent_path: &str,
) -> anyhow::Result<Option<ThreadId>> {
    let mut matches = Vec::new();
    for descendant in list_spawn_descendants(antfly, root, None).await? {
        if let Some(view) = load_thread_view(antfly, descendant).await?
            && view.agent_path.as_deref() == Some(agent_path)
        {
            matches.push(descendant);
        }
    }
    matches.sort_by_key(ThreadId::to_string);
    one_match(matches, agent_path)
}
