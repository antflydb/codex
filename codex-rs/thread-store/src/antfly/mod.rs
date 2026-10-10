//! [`ThreadStore`] backed by Antfly: no SQLite databases and no rollout files.
//!
//! Every thread's metadata is one row in `codex_threads`; its persisted
//! rollout items are one document per item in `codex_history_items`, keyed
//! so an ordered prefix scan returns a thread's items in rollout order (see
//! [`keys`]). Item documents carry the visible message text in the shared
//! search field, so `search_threads` is hybrid full-text and semantic search.
//! Sections, projects, attachments, spawn edges, and the paginated-history
//! projection are their own SQL tables (see `codex_antfly::schema`), shared
//! with `StateRuntime`'s Antfly backend: one source of truth for thread
//! state.
//!
//! Live writer state (lazy materialization, pending metadata) is held in
//! memory like the local store; nothing is written for a thread until it is
//! persisted or receives its first durable item.

mod attachments;
mod fork;
mod history;
mod import;
mod keys;
mod listing;
mod occurrences;
mod projection;
mod projects;
mod record;
mod search;
mod sections;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::AntflyError;
use codex_antfly::Write;
use codex_antfly::schema;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionContextWindow;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::into_persisted_rollout_items;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde_json::Value;

use crate::AddThreadAttachmentOutcome;
use crate::AddThreadAttachmentParams;
use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateProjectParams;
use crate::CreateThreadParams;
use crate::CreateThreadSectionParams;
use crate::CreatedProject;
use crate::DeleteThreadParams;
use crate::DeleteThreadSectionParams;
use crate::DeletedProject;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListProjectsParams;
use crate::ListThreadAttachmentThreadsParams;
use crate::ListThreadAttachmentsParams;
use crate::ListThreadSectionsParams;
use crate::ListThreadsParams;
use crate::ListTimelineParams;
use crate::ListTurnsParams;
use crate::LoadThreadHistoryParams;
use crate::MoveProjectParams;
use crate::MoveThreadToSectionParams;
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ProjectMoveOutcome;
use crate::ReadThreadByRolloutPathParams;
use crate::ReadThreadParams;
use crate::RemoveThreadAttachmentOutcome;
use crate::RemoveThreadAttachmentParams;
use crate::RenameThreadSectionParams;
use crate::ResumeThreadParams;
use crate::RevertThreadParams;
use crate::SearchThreadOccurrencesParams;
use crate::SearchThreadsParams;
use crate::StoredModelContext;
use crate::StoredProject;
use crate::StoredProjectsPage;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::StoredThreadSection;
use crate::StoredThreadSectionsPage;
use crate::ThreadAttachmentOwnerPage;
use crate::ThreadAttachmentPage;
use crate::ThreadMetadataPatch;
use crate::ThreadOccurrenceSearchPage;
use crate::ThreadPage;
use crate::ThreadSearchPage;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;
use crate::ThreadStoreResult;
use crate::TimelinePage;
use crate::TurnPage;
use crate::UpdateProjectParams;
use crate::UpdateThreadMetadataParams;
use crate::UpdatedProject;
use record::ThreadRecord;
use record::visible_text;

pub use import::ImportOutcome;
pub use import::ImportSection;
pub use import::ImportThreadParams;

/// Host cleanup run before a thread's data is deleted (for example agent
/// message boards). Failures abort the delete so it can be retried.
pub type ThreadDataCleanup =
    Arc<dyn Fn(Vec<ThreadId>) -> ThreadStoreFuture<'static, ()> + Send + Sync>;

/// A thread with a live writer in this process.
struct LiveThread {
    created: CreateThreadParams,
    /// `Some` once the thread is durable.
    record: Option<ThreadRecord>,
    /// Items held until the thread materializes (its `SessionMeta`).
    pending: Vec<RolloutItem>,
    /// Metadata applied before the thread materialized.
    pending_patch: ThreadMetadataPatch,
}

#[derive(Default)]
struct WriterState {
    live: HashMap<ThreadId, LiveThread>,
    staged_metadata: HashMap<ThreadId, ThreadMetadataPatch>,
}

/// Antfly-backed [`ThreadStore`].
pub struct AntflyThreadStore {
    antfly: Arc<Antfly>,
    state: Arc<tokio::sync::Mutex<WriterState>>,
    cleanup: Option<ThreadDataCleanup>,
    /// The Antfly-backed `StateRuntime` sharing this same `Antfly` handle, so
    /// sessions reach memories/goals/guardian-feedback/shell-snapshot state
    /// the same way they do for `LocalThreadStore` (see
    /// `core/src/session/session.rs`'s `LocalThreadStore` downcast, which
    /// also checks for `AntflyThreadStore` and reads this field).
    state_db: Option<codex_rollout::StateDbHandle>,
}

impl std::fmt::Debug for AntflyThreadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AntflyThreadStore")
            .field("antfly", &self.antfly)
            .finish_non_exhaustive()
    }
}

fn internal(err: AntflyError) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("antfly: {err}"),
    }
}

fn to_value<T: serde::Serialize>(value: &T) -> ThreadStoreResult<Value> {
    serde_json::to_value(value).map_err(|err| ThreadStoreError::Internal {
        message: format!("serialize: {err}"),
    })
}

fn from_value<T: serde::de::DeserializeOwned>(value: Value) -> ThreadStoreResult<T> {
    serde_json::from_value(value).map_err(|err| ThreadStoreError::Internal {
        message: format!("deserialize: {err}"),
    })
}

/// Builds the `SessionMeta` that opens a thread's history.
fn session_meta(params: &CreateThreadParams) -> RolloutItem {
    let meta = SessionMeta {
        session_id: params.session_id,
        id: params.thread_id,
        forked_from_id: params.forked_from_id,
        parent_thread_id: params.parent_thread_id,
        timestamp: Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        cwd: params.metadata.cwd.clone().unwrap_or_default(),
        runtime_workspace_roots: params
            .runtime_workspace_roots
            .as_ref()
            .map(|roots| roots.iter().map(AbsolutePathBuf::to_path_buf).collect()),
        agent_nickname: params.source.get_nickname(),
        agent_role: params.source.get_agent_role(),
        agent_path: params.source.get_agent_path().map(Into::into),
        originator: params.originator.clone(),
        creator_user_id: params.creator_user_id.clone(),
        creator_account_id: params.creator_account_id.clone(),
        cli_version: env!("CARGO_PKG_VERSION").to_string(),
        source: params.source.clone(),
        thread_source: params.thread_source.clone(),
        model_provider: Some(params.metadata.model_provider.clone()),
        base_instructions: Some(params.base_instructions.clone()),
        dynamic_tools: (!params.dynamic_tools.is_empty()).then(|| params.dynamic_tools.clone()),
        selected_capability_roots: params.selected_capability_roots.clone(),
        memory_mode: matches!(params.metadata.memory_mode, ThreadMemoryMode::Disabled)
            .then_some("disabled".to_string()),
        history_mode: params.history_mode,
        history_base: params.history_base,
        subagent_history_start_ordinal: params.subagent_history_start_ordinal,
        multi_agent_version: params.multi_agent_version,
        context_window: Some(SessionContextWindow::new(params.initial_window_id.clone())),
        ..SessionMeta::default()
    };
    RolloutItem::SessionMeta(SessionMetaLine { meta, git: None })
}

impl AntflyThreadStore {
    pub fn new(antfly: Arc<Antfly>) -> Self {
        Self {
            antfly,
            state: Arc::new(tokio::sync::Mutex::new(WriterState::default())),
            cleanup: None,
            state_db: None,
        }
    }

    /// Runs `cleanup` before deleting threads, like the local store's hook.
    pub fn with_thread_data_cleanup(mut self, cleanup: ThreadDataCleanup) -> Self {
        self.cleanup = Some(cleanup);
        self
    }

    /// Attaches the Antfly-backed `StateRuntime` sharing this store's
    /// `Antfly` handle, so sessions can reach memories, goals, guardian
    /// feedback, and other `StateRuntime`-backed features.
    pub fn with_state_db(mut self, state_db: codex_rollout::StateDbHandle) -> Self {
        self.state_db = Some(state_db);
        self
    }

    /// The attached `StateRuntime`, if one was set at construction. Async to
    /// match `LocalThreadStore::state_db`'s signature, so downcast call
    /// sites can treat both stores the same way.
    pub async fn state_db(&self) -> Option<codex_rollout::StateDbHandle> {
        self.state_db.clone()
    }

    pub fn antfly(&self) -> &Arc<Antfly> {
        &self.antfly
    }

    pub(crate) async fn load_record(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Option<ThreadRecord>> {
        let sql = self.antfly.sql().await.map_err(internal)?;
        let row = sql
            .fetch_optional(
                &format!("{} WHERE t.id = $1", record::SELECT_THREAD),
                sql_params![thread_id.to_string()],
            )
            .await
            .map_err(internal)?;
        row.map(|row| ThreadRecord::from_row(&row)).transpose()
    }

    async fn require_record(
        &self,
        thread_id: ThreadId,
        include_archived: bool,
    ) -> ThreadStoreResult<ThreadRecord> {
        match self.load_record(thread_id).await? {
            Some(record) if include_archived || record.archived_at.is_none() => Ok(record),
            _ => Err(ThreadStoreError::ThreadNotFound { thread_id }),
        }
    }

    pub(crate) async fn load_items(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        self.load_items_before(thread_id, None).await
    }

    /// Raw rollout items for `thread_id`, in order, stopping before
    /// `end_ordinal_exclusive` when given. Used by lineage scans over an
    /// ancestor thread that kept receiving items after the ordinal a
    /// descendant forked from.
    pub(crate) async fn load_items_before(
        &self,
        thread_id: ThreadId,
        end_ordinal_exclusive: Option<u64>,
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        let prefix = keys::history_item_prefix(thread_id);
        let to = match end_ordinal_exclusive {
            Some(ordinal) => keys::history_item_id(thread_id, ordinal),
            None => codex_antfly::keys::prefix_end(&prefix),
        };
        let documents = self
            .antfly
            .documents(schema::HISTORY_ITEMS)
            .scan(codex_antfly::ScanRequest {
                from: prefix,
                to,
                limit: None,
            })
            .await
            .map_err(internal)?;
        documents
            .into_iter()
            .map(|document| {
                let item = document.doc.get("item").cloned().ok_or_else(|| {
                    ThreadStoreError::Internal {
                        message: format!("item {} has no payload", document.key),
                    }
                })?;
                from_value(item)
            })
            .collect()
    }

    /// Replaces the whole `codex_threads` row for `record`. The row has no
    /// secondary indexes of its own (listing reads the table directly), so
    /// this is the only write a metadata change needs.
    async fn save_record(&self, record: &ThreadRecord) -> ThreadStoreResult<()> {
        let sql = self.antfly.sql().await.map_err(internal)?;
        let (statement, params) = record::upsert_statement(record.upsert_params()?);
        sql.execute(&statement, params).await.map_err(internal)?;
        if let Some(parent) = record.created.parent_thread_id {
            sql.execute(
                "INSERT INTO codex_thread_spawn_edges (child_thread_id, parent_thread_id, status) \
                 VALUES ($1, $2, 'open') ON CONFLICT (child_thread_id) DO NOTHING",
                sql_params![record.thread_id().to_string(), parent.to_string()],
            )
            .await
            .map_err(internal)?;
        }
        Ok(())
    }

    fn item_write(
        thread_id: ThreadId,
        ordinal: u64,
        item: &RolloutItem,
    ) -> ThreadStoreResult<Write> {
        let mut doc = serde_json::json!({
            "thread_id": thread_id.to_string(),
            "ordinal": ordinal,
            "item": to_value(item)?,
        });
        if let Some((speaker, text)) = visible_text(item)
            && let Some(map) = doc.as_object_mut()
        {
            map.insert(
                codex_antfly::SEARCH_TEXT_FIELD.to_string(),
                Value::String(text),
            );
            map.insert("speaker".to_string(), to_value(&speaker)?);
            if let Some(turn_id) = record::item_turn_id(item) {
                map.insert("turn_id".to_string(), Value::String(turn_id));
            }
        }
        Ok(Write::put(keys::history_item_id(thread_id, ordinal), doc))
    }

    /// Makes a live thread durable, writing its pending items and `extra`.
    async fn materialize(
        &self,
        live: &mut LiveThread,
        extra: Vec<RolloutItem>,
    ) -> ThreadStoreResult<()> {
        let thread_id = live.created.thread_id;
        let mut record = ThreadRecord::new(live.created.clone(), Utc::now());
        record.patch.merge(std::mem::take(&mut live.pending_patch));
        let items: Vec<RolloutItem> = live.pending.drain(..).chain(extra).collect();
        let base = live
            .created
            .history_base
            .map(|base| base.end_ordinal_exclusive)
            .unwrap_or(0);
        let mut writes = Vec::with_capacity(items.len());
        for (offset, item) in items.iter().enumerate() {
            writes.push(Self::item_write(thread_id, base + offset as u64, item)?);
        }
        self.antfly
            .documents(schema::HISTORY_ITEMS)
            .write(writes)
            .await
            .map_err(internal)?;
        if record.history_mode() == ThreadHistoryMode::Paginated {
            projection::apply_batch(
                self,
                thread_id,
                live.created.subagent_history_start_ordinal,
                base,
                &items,
                Utc::now(),
            )
            .await?;
        }
        record.next_ordinal = base + items.len() as u64;
        self.save_record(&record).await?;
        live.record = Some(record);
        Ok(())
    }

    async fn append(
        &self,
        live: &mut LiveThread,
        items: Vec<RolloutItem>,
    ) -> ThreadStoreResult<()> {
        let Some(record) = live.record.as_mut() else {
            return self.materialize(live, items).await;
        };
        let thread_id = record.thread_id();
        let mut writes = Vec::with_capacity(items.len());
        for (offset, item) in items.iter().enumerate() {
            writes.push(Self::item_write(
                thread_id,
                record.next_ordinal + offset as u64,
                item,
            )?);
        }
        self.antfly
            .documents(schema::HISTORY_ITEMS)
            .write(writes)
            .await
            .map_err(internal)?;
        if record.history_mode() == ThreadHistoryMode::Paginated {
            projection::apply_batch(
                self,
                thread_id,
                live.created.subagent_history_start_ordinal,
                record.next_ordinal,
                &items,
                Utc::now(),
            )
            .await?;
        }
        record.next_ordinal += items.len() as u64;
        self.save_record(record).await?;
        Ok(())
    }

    async fn create_thread_impl(&self, params: CreateThreadParams) -> ThreadStoreResult<()> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        if state.live.contains_key(&params.thread_id) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {} already has a live writer", params.thread_id),
            });
        }
        let pending = vec![session_meta(&params)];
        state.live.insert(
            params.thread_id,
            LiveThread {
                created: params,
                record: None,
                pending,
                pending_patch: ThreadMetadataPatch::default(),
            },
        );
        Ok(())
    }

    async fn resume_thread_impl(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreResult<Arc<Vec<RolloutItem>>> {
        let thread_id = params.thread_id;
        let mut state = Arc::clone(&self.state).lock_owned().await;
        if state.live.contains_key(&thread_id) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} already has a live writer"),
            });
        }
        let record = self
            .load_record(thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if record.archived_at.is_some() && !params.include_archived {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} is archived"),
            });
        }
        let explicit_history = params.history.filter(|history| {
            !matches!(
                history.first(),
                Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == thread_id
            )
        });
        let history = match explicit_history {
            Some(history) => history,
            None if record.history_mode() == ThreadHistoryMode::Paginated => {
                Arc::new(history::load_latest_model_context_items(self, thread_id).await?)
            }
            None => Arc::new(self.load_items(thread_id).await?),
        };
        state.live.insert(
            thread_id,
            LiveThread {
                created: record.created.clone(),
                record: Some(record),
                pending: Vec::new(),
                pending_patch: ThreadMetadataPatch::default(),
            },
        );
        Ok(history)
    }

    async fn append_items_impl(&self, params: AppendThreadItemsParams) -> ThreadStoreResult<()> {
        if params.items.is_empty() {
            return Ok(());
        }
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let live =
            state
                .live
                .get_mut(&params.thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                })?;
        let items = into_persisted_rollout_items(params.items, live.created.history_mode);
        if items.is_empty() {
            return Ok(());
        }
        let _guard = self.antfly.lock().await;
        self.append(live, items).await
    }

    async fn persist_thread_impl(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreResult<()> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let live = state
            .live
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        match context {
            // Preparation flushes without materializing an empty thread;
            // subagent spawns are followed by a standard persist.
            PersistContext::ThreadPreparation | PersistContext::SubagentSpawn => Ok(()),
            PersistContext::Standard
            | PersistContext::TurnStart
            | PersistContext::SteeredUserInput => {
                if live.record.is_some() {
                    return Ok(());
                }
                let _guard = self.antfly.lock().await;
                self.materialize(live, Vec::new()).await
            }
        }
    }

    async fn require_live(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        if Arc::clone(&self.state)
            .lock_owned()
            .await
            .live
            .contains_key(&thread_id)
        {
            Ok(())
        } else {
            Err(ThreadStoreError::ThreadNotFound { thread_id })
        }
    }

    /// Whether `thread_id` has a live writer in this process, for operations
    /// that require the caller to close it first (for example `revert_thread`).
    pub(crate) async fn has_live_writer(&self, thread_id: ThreadId) -> bool {
        Arc::clone(&self.state)
            .lock_owned()
            .await
            .live
            .contains_key(&thread_id)
    }

    async fn shutdown_thread_impl(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let live = state
            .live
            .remove(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if live.record.is_none() {
            state.staged_metadata.remove(&thread_id);
        }
        Ok(())
    }

    async fn discard_thread_impl(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        state.staged_metadata.remove(&thread_id);
        state
            .live
            .remove(&thread_id)
            .map(|_| ())
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
    }

    async fn load_history_impl(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        let record = self
            .require_record(params.thread_id, params.include_archived)
            .await?;
        if record.history_mode() == ThreadHistoryMode::Paginated {
            return Err(ThreadStoreError::Unsupported {
                operation: "paginated_threads",
            });
        }
        Ok(StoredThreadHistory {
            revision: None,
            thread_id: params.thread_id,
            items: self.load_items(params.thread_id).await?,
        })
    }

    async fn load_latest_model_context_impl(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreResult<StoredModelContext> {
        let record = self
            .require_record(params.thread_id, params.include_archived)
            .await?;
        let items = if record.history_mode() == ThreadHistoryMode::Paginated {
            history::load_latest_model_context_items(self, params.thread_id).await?
        } else {
            self.load_items(params.thread_id).await?
        };
        Ok(StoredModelContext {
            revision: None,
            thread_id: params.thread_id,
            items,
        })
    }

    async fn read_thread_impl(&self, params: ReadThreadParams) -> ThreadStoreResult<StoredThread> {
        let record = self
            .require_record(params.thread_id, params.include_archived)
            .await?;
        let history = if params.include_history {
            Some(self.load_items(params.thread_id).await?)
        } else {
            None
        };
        Ok(record.to_stored(history))
    }

    async fn read_thread_by_rollout_path_impl(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreResult<StoredThread> {
        let sql = self.antfly.sql().await.map_err(internal)?;
        let path = params.rollout_path.to_string_lossy().to_string();
        let row = sql
            .fetch_optional(
                "SELECT id FROM codex_threads WHERE rollout_path = $1 AND rollout_path <> ''",
                sql_params![path],
            )
            .await
            .map_err(internal)?;
        let thread_id = row
            .map(|row| row.string("id"))
            .transpose()
            .map_err(internal)?
            .and_then(|id| ThreadId::from_string(&id).ok())
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!(
                    "no thread was imported from rollout path {}",
                    params.rollout_path.display()
                ),
            })?;
        self.read_thread_impl(ReadThreadParams {
            thread_id,
            include_archived: params.include_archived,
            include_history: params.include_history,
        })
        .await
    }

    async fn update_thread_metadata_impl(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreResult<StoredThread> {
        let thread_id = params.thread_id;
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let mut patch = state
            .staged_metadata
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();
        patch.merge(params.patch);
        // Rollout paths are a local-store concept.
        patch.rollout_path = None;

        if let Some(live) = state.live.get_mut(&thread_id)
            && live.record.is_none()
        {
            live.pending_patch.merge(patch);
            let mut preview = ThreadRecord::new(live.created.clone(), Utc::now());
            preview.patch = live.pending_patch.clone();
            state.staged_metadata.remove(&thread_id);
            return Ok(preview.to_stored(None));
        }

        let _guard = self.antfly.lock().await;
        let previous = self
            .require_record(thread_id, params.include_archived)
            .await?;
        let mut record = previous.clone();
        record.patch.merge(patch);
        self.save_record(&record).await?;
        if let Some(live) = state.live.get_mut(&thread_id) {
            live.record = Some(record.clone());
        }
        state.staged_metadata.remove(&thread_id);
        Ok(record.to_stored(None))
    }

    async fn set_archived(
        &self,
        thread_id: ThreadId,
        archived: bool,
    ) -> ThreadStoreResult<ThreadRecord> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let _guard = self.antfly.lock().await;
        let previous =
            self.load_record(thread_id)
                .await?
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("no thread found for thread id {thread_id}"),
                })?;
        if archived == previous.archived_at.is_some() {
            if archived {
                return Ok(previous);
            }
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("no archived thread found for thread id {thread_id}"),
            });
        }
        let mut record = previous;
        record.archived_at = archived.then(Utc::now);
        self.save_record(&record).await?;
        if let Some(live) = state.live.get_mut(&thread_id) {
            live.record = Some(record.clone());
        }
        Ok(record)
    }

    /// Deletes every paginated-history projection row of `thread_id` (spec
    /// §2.25). Harmless no-op for Legacy threads.
    pub(crate) async fn projection_delete(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let sql = self.antfly.sql().await.map_err(internal)?;
        for table in [
            "codex_thread_turns",
            "codex_thread_items",
            "codex_thread_realtime_items",
            "codex_thread_history_projection_state",
        ] {
            sql.execute(
                &format!("DELETE FROM {table} WHERE thread_id = $1"),
                sql_params![thread_id.to_string()],
            )
            .await
            .map_err(internal)?;
        }
        Ok(())
    }

    /// Threads whose history starts inside `thread_id`'s, with the exclusive
    /// end ordinal they inherit.
    pub(crate) async fn fork_children(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Vec<(ThreadId, u64)>> {
        let sql = self.antfly.sql().await.map_err(internal)?;
        let rows = sql
            .fetch_all(
                "SELECT id, history_base_end_ordinal FROM codex_threads \
                 WHERE history_base_thread_id = $1",
                sql_params![thread_id.to_string()],
            )
            .await
            .map_err(internal)?;
        rows.into_iter()
            .map(|row| {
                let id =
                    ThreadId::from_string(&row.string("id").map_err(internal)?).map_err(|err| {
                        ThreadStoreError::Internal {
                            message: format!("invalid thread id: {err}"),
                        }
                    })?;
                let end_ordinal = row.i64("history_base_end_ordinal").map_err(internal)? as u64;
                Ok((id, end_ordinal))
            })
            .collect()
    }

    async fn delete_thread_impl(&self, params: DeleteThreadParams) -> ThreadStoreResult<()> {
        let thread_id = params.thread_id;
        if let Some(cleanup) = &self.cleanup {
            cleanup(vec![thread_id]).await?;
        }
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let _guard = self.antfly.lock().await;
        let record = self.load_record(thread_id).await?;
        let items = self
            .antfly
            .documents(schema::HISTORY_ITEMS)
            .scan(codex_antfly::ScanRequest::prefix(
                &keys::history_item_prefix(thread_id),
            ))
            .await
            .map_err(internal)?;
        if record.is_none() && items.is_empty() {
            state.live.remove(&thread_id);
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        }
        if !self.fork_children(thread_id).await?.is_empty() {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "cannot delete thread {thread_id}: forked history still references it"
                ),
            });
        }
        let item_writes: Vec<Write> = items
            .into_iter()
            .map(|document| Write::delete(document.key))
            .collect();
        if !item_writes.is_empty() {
            self.antfly
                .documents(schema::HISTORY_ITEMS)
                .write(item_writes)
                .await
                .map_err(internal)?;
        }
        self.projection_delete(thread_id).await?;
        let sql = self.antfly.sql().await.map_err(internal)?;
        sql.execute(
            "DELETE FROM codex_thread_spawn_edges WHERE parent_thread_id = $1 OR child_thread_id = $1",
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
        // `codex_thread_attachments` and `codex_thread_dynamic_tools` cascade
        // from this delete.
        sql.execute(
            "DELETE FROM codex_threads WHERE id = $1",
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
        state.live.remove(&thread_id);
        state.staged_metadata.remove(&thread_id);
        Ok(())
    }
}

impl ThreadStore for AntflyThreadStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_history_mode(&self) -> ThreadHistoryMode {
        ThreadHistoryMode::Paginated
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.create_thread_impl(params))
    }

    fn stage_pending_thread_metadata(
        &self,
        thread_id: ThreadId,
        patch: ThreadMetadataPatch,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            if patch.rollout_path.is_some() {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "pending thread metadata cannot set a rollout path".to_owned(),
                });
            }
            if patch.is_empty() {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "pending thread metadata cannot be empty".to_owned(),
                });
            }
            let mut state = Arc::clone(&self.state).lock_owned().await;
            if state.staged_metadata.contains_key(&thread_id) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("pending thread metadata already exists: {thread_id}"),
                });
            }
            state.staged_metadata.insert(thread_id, patch);
            Ok(())
        })
    }

    fn read_pending_thread_metadata(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreFuture<'_, Option<ThreadMetadataPatch>> {
        Box::pin(async move {
            Ok(Arc::clone(&self.state)
                .lock_owned()
                .await
                .staged_metadata
                .get(&thread_id)
                .cloned())
        })
    }

    fn remove_pending_thread_metadata(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            Arc::clone(&self.state)
                .lock_owned()
                .await
                .staged_metadata
                .remove(&thread_id);
            Ok(())
        })
    }

    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        Box::pin(self.resume_thread_impl(params))
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.append_items_impl(params))
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.persist_thread_impl(thread_id, context))
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        // Items are written as they are appended; there is nothing to flush.
        Box::pin(self.require_live(thread_id))
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.shutdown_thread_impl(thread_id))
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.discard_thread_impl(thread_id))
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(self.load_history_impl(params))
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(self.load_latest_model_context_impl(params))
    }

    fn supports_paginated_history_lists(&self) -> bool {
        true
    }

    fn list_turns(&self, params: ListTurnsParams) -> ThreadStoreFuture<'_, TurnPage> {
        Box::pin(history::list_turns(self, params))
    }

    fn list_items(&self, params: ListItemsParams) -> ThreadStoreFuture<'_, ItemPage> {
        Box::pin(history::list_items(self, params))
    }

    fn list_timeline(&self, params: ListTimelineParams) -> ThreadStoreFuture<'_, TimelinePage> {
        Box::pin(history::list_timeline(self, params))
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(self.read_thread_impl(params))
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(self.read_thread_by_rollout_path_impl(params))
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(listing::list_threads(self, params))
    }

    fn search_threads(
        &self,
        params: SearchThreadsParams,
    ) -> ThreadStoreFuture<'_, ThreadSearchPage> {
        Box::pin(search::search_threads(self, params))
    }

    fn search_thread_occurrences(
        &self,
        params: SearchThreadOccurrencesParams,
    ) -> ThreadStoreFuture<'_, ThreadOccurrenceSearchPage> {
        Box::pin(occurrences::search_thread_occurrences(self, params))
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move { self.update_thread_metadata_impl(params).await.map(Some) })
    }

    fn move_thread_to_section(
        &self,
        params: MoveThreadToSectionParams,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(sections::move_thread_to_section(self, params))
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { self.set_archived(params.thread_id, true).await.map(|_| ()) })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            self.set_archived(params.thread_id, false)
                .await
                .map(|record| record.to_stored(None))
        })
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.delete_thread_impl(params))
    }

    fn prepare_fork(&self, params: PrepareForkParams) -> ThreadStoreFuture<'_, PreparedFork> {
        Box::pin(fork::prepare_fork(self, params))
    }

    fn revert_thread(&self, params: RevertThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(fork::revert_thread(self, params))
    }

    fn supports_thread_sections(&self) -> bool {
        true
    }

    fn list_thread_sections(
        &self,
        params: ListThreadSectionsParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSectionsPage> {
        Box::pin(sections::list_thread_sections(self, params))
    }

    fn create_thread_section(
        &self,
        params: CreateThreadSectionParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSection> {
        Box::pin(sections::create_thread_section(self, params))
    }

    fn rename_thread_section(
        &self,
        params: RenameThreadSectionParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThreadSection>> {
        Box::pin(sections::rename_thread_section(self, params))
    }

    fn delete_thread_section(
        &self,
        params: DeleteThreadSectionParams,
    ) -> ThreadStoreFuture<'_, bool> {
        Box::pin(sections::delete_thread_section(self, params))
    }

    fn supports_thread_attachments(&self) -> bool {
        true
    }

    fn copy_thread_attachments(
        &self,
        source_thread_id: ThreadId,
        destination_thread_id: ThreadId,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(attachments::copy_thread_attachments(
            self,
            source_thread_id,
            destination_thread_id,
        ))
    }

    fn add_thread_attachment(
        &self,
        params: AddThreadAttachmentParams,
    ) -> ThreadStoreFuture<'_, AddThreadAttachmentOutcome> {
        Box::pin(attachments::add_thread_attachment(self, params))
    }

    fn list_thread_attachments(
        &self,
        params: ListThreadAttachmentsParams,
    ) -> ThreadStoreFuture<'_, ThreadAttachmentPage> {
        Box::pin(attachments::list_thread_attachments(self, params))
    }

    fn list_thread_attachment_threads(
        &self,
        params: ListThreadAttachmentThreadsParams,
    ) -> ThreadStoreFuture<'_, ThreadAttachmentOwnerPage> {
        Box::pin(attachments::list_thread_attachment_threads(self, params))
    }

    fn remove_thread_attachment(
        &self,
        params: RemoveThreadAttachmentParams,
    ) -> ThreadStoreFuture<'_, RemoveThreadAttachmentOutcome> {
        Box::pin(attachments::remove_thread_attachment(self, params))
    }

    fn supports_projects(&self) -> bool {
        true
    }

    fn list_projects(
        &self,
        params: ListProjectsParams,
    ) -> ThreadStoreFuture<'_, StoredProjectsPage> {
        Box::pin(projects::list_projects(self, params))
    }

    fn read_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<StoredProject>> {
        Box::pin(projects::read_project(self, project_id))
    }

    fn create_project(&self, params: CreateProjectParams) -> ThreadStoreFuture<'_, CreatedProject> {
        Box::pin(projects::create_project(self, params))
    }

    fn update_project(
        &self,
        params: UpdateProjectParams,
    ) -> ThreadStoreFuture<'_, Option<UpdatedProject>> {
        Box::pin(projects::update_project(self, params))
    }

    fn move_project(
        &self,
        params: MoveProjectParams,
    ) -> ThreadStoreFuture<'_, Option<ProjectMoveOutcome>> {
        Box::pin(projects::move_project(self, params))
    }

    fn delete_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<DeletedProject>> {
        Box::pin(projects::delete_project(self, project_id))
    }
}
