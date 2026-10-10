//! A thin projection of `codex_threads` for the memory pipeline
//! (`antfly_backend::memories`), which cannot depend on `codex-thread-store`
//! (that crate already depends on `codex-state`, so a reverse dependency
//! would cycle) and so cannot see `AntflyThreadStore` or its row-parsing
//! types directly.
//!
//! This used to re-parse the key-value `ts:t:{thread_id}` document
//! `AntflyThreadStore` wrote, best-effort, by the field names its types
//! serialize to. Thread metadata is a SQL row now (`codex_threads`, via
//! [`super::threads::get_thread`]), shared with `AntflyThreadStore`, so
//! every field below is exact, not best-effort.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_protocol::ThreadId;

use super::threads;

/// Projection of one thread's `codex_threads` row, for the memory pipeline.
///
/// `model_provider`/`preview`/`agent_path` are not read by the current
/// memory pipeline but are kept for parity with the full projection this
/// module used to carry (and for any caller added later).
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct ThreadView {
    pub(crate) thread_id: ThreadId,
    pub(crate) cwd: PathBuf,
    pub(crate) model_provider: String,
    pub(crate) source: String,
    pub(crate) git_branch: Option<String>,
    pub(crate) preview: Option<String>,
    pub(crate) agent_path: Option<String>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) archived_at: Option<DateTime<Utc>>,
}

impl From<crate::ThreadMetadata> for ThreadView {
    fn from(metadata: crate::ThreadMetadata) -> Self {
        Self {
            thread_id: metadata.id,
            cwd: metadata.cwd,
            model_provider: metadata.model_provider,
            source: metadata.source,
            git_branch: metadata.git_branch,
            preview: metadata.preview,
            agent_path: metadata.agent_path,
            updated_at: metadata.updated_at,
            archived_at: metadata.archived_at,
        }
    }
}

pub(crate) async fn load_thread_view(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadView>> {
    Ok(threads::get_thread(antfly, thread_id)
        .await?
        .map(ThreadView::from))
}

/// Full `StateRuntime::get_thread` projection, for callers outside the
/// memory pipeline (app-server request processors, goal continuation,
/// thread deletion) that need the general-purpose `ThreadMetadata` shape.
pub(crate) async fn get_thread_metadata(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<crate::ThreadMetadata>> {
    threads::get_thread(antfly, thread_id).await
}

/// Scans every thread row. Used only by the best-effort memory-pipeline
/// startup scan (`claim_stage1_jobs_for_startup`); callers on a hot path
/// should prefer `load_thread_view` for a single thread.
pub(crate) async fn scan_all_threads(antfly: &Arc<Antfly>) -> anyhow::Result<Vec<ThreadView>> {
    let sql = antfly.sql().await.map_err(super::internal)?;
    let rows = sql
        .fetch_all("SELECT id FROM codex_threads", vec![])
        .await
        .map_err(super::internal)?;
    let mut views = Vec::with_capacity(rows.len());
    for row in &rows {
        let id = ThreadId::try_from(row.string("id").map_err(super::internal)?)?;
        if let Some(metadata) = threads::get_thread(antfly, id).await? {
            views.push(ThreadView::from(metadata));
        }
    }
    Ok(views)
}

pub(crate) async fn is_memory_mode_enabled(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    threads::is_memory_mode_enabled(antfly, thread_id).await
}

/// Marks a thread polluted. Returns whether the column transitioned
/// (mirrors `rows_affected > 0` on the SQLite `WHERE memory_mode !=
/// 'polluted'` guard).
pub(crate) async fn mark_memory_mode_polluted(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    threads::mark_memory_mode_polluted(antfly, thread_id).await
}
