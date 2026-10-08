//! [`ThreadStore`] backed by Antfly: no SQLite databases and no rollout files.
//!
//! Each thread is one record plus its persisted rollout items, keyed so that
//! ordered scans return items in rollout order and listings newest first (see
//! [`keys`]). Item documents carry the visible message text in the shared
//! search field, so `search_threads` is hybrid full-text and semantic search.
//!
//! Live writer state (lazy materialization, pending metadata) is held in
//! memory like the local store; nothing is written for a thread until it is
//! persisted or receives its first durable item.

mod attachments;
mod fork;
mod history;
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
use codex_antfly::ScanRequest;
use codex_antfly::Write;
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
use serde_json::json;

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
use crate::ThreadSortKey;
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

/// Host cleanup run before a thread's data is deleted (for example agent
/// message boards). Failures abort the delete so it can be retried.
pub type ThreadDataCleanup =
    Arc<dyn Fn(Vec<ThreadId>) -> ThreadStoreFuture<'static, ()> + Send + Sync>;

const SORT_KEYS: [ThreadSortKey; 3] = [
    ThreadSortKey::CreatedAt,
    ThreadSortKey::UpdatedAt,
    ThreadSortKey::RecencyAt,
];

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
        }
    }

    /// Runs `cleanup` before deleting threads, like the local store's hook.
    pub fn with_thread_data_cleanup(mut self, cleanup: ThreadDataCleanup) -> Self {
        self.cleanup = Some(cleanup);
        self
    }

    pub fn antfly(&self) -> &Arc<Antfly> {
        &self.antfly
    }

    pub(crate) async fn load_record(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Option<ThreadRecord>> {
        match self
            .antfly
            .get(keys::thread(thread_id))
            .await
            .map_err(internal)?
        {
            Some(doc) => Ok(Some(from_value(codex_antfly::strip_reserved(doc))?)),
            None => Ok(None),
        }
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
        let prefix = keys::items_prefix(thread_id);
        let to = match end_ordinal_exclusive {
            Some(ordinal) => keys::item(thread_id, ordinal),
            None => codex_antfly::keys::prefix_end(&prefix),
        };
        let documents = self
            .antfly
            .scan(ScanRequest {
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

    /// Writes for `record`, replacing the listing entries of `previous`.
    fn record_writes(
        previous: Option<&ThreadRecord>,
        record: &ThreadRecord,
    ) -> ThreadStoreResult<Vec<Write>> {
        let thread_id = record.thread_id();
        let doc = to_value(record)?;
        let mut writes = vec![Write::put(keys::thread(thread_id), doc.clone())];
        for sort_key in SORT_KEYS {
            let key = keys::index_entry(
                record.archived_at.is_some(),
                sort_key,
                record.sort_millis(sort_key),
                thread_id,
            );
            if let Some(previous) = previous {
                let old = keys::index_entry(
                    previous.archived_at.is_some(),
                    sort_key,
                    previous.sort_millis(sort_key),
                    thread_id,
                );
                if old != key {
                    writes.push(Write::delete(old));
                }
            }
            writes.push(Write::put(key, doc.clone()));
        }
        let old_section = previous.and_then(|previous| {
            Some(keys::section_entry(
                previous.section.as_deref()?,
                previous.section_position.unwrap_or(i64::MAX),
                thread_id,
            ))
        });
        let new_section = record.section.as_deref().map(|section| {
            keys::section_entry(
                section,
                record.section_position.unwrap_or(i64::MAX),
                thread_id,
            )
        });
        if let Some(old) = old_section
            && Some(&old) != new_section.as_ref()
        {
            writes.push(Write::delete(old));
        }
        if let Some(new) = new_section {
            writes.push(Write::put(new, doc));
        }
        if let Some(path) = &record.legacy_rollout_path {
            writes.push(Write::put(
                keys::rollout_path(path),
                json!({"thread_id": thread_id.to_string()}),
            ));
        }
        Ok(writes)
    }

    fn item_write(
        thread_id: ThreadId,
        ordinal: u64,
        item: &RolloutItem,
    ) -> ThreadStoreResult<Write> {
        let mut doc = json!({
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
        Ok(Write::put(keys::item(thread_id, ordinal), doc))
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
        let mut writes = Vec::with_capacity(items.len() + 5);
        for (offset, item) in items.iter().enumerate() {
            writes.push(Self::item_write(thread_id, base + offset as u64, item)?);
        }
        if record.history_mode() == ThreadHistoryMode::Paginated {
            writes.extend(
                projection::build_writes(
                    self,
                    thread_id,
                    live.created.subagent_history_start_ordinal,
                    base,
                    &items,
                    Utc::now(),
                )
                .await?,
            );
        }
        record.next_ordinal = base + items.len() as u64;
        writes.extend(Self::record_writes(None, &record)?);
        self.antfly.write(writes).await.map_err(internal)?;
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
        let mut writes = Vec::with_capacity(items.len() + 1);
        for (offset, item) in items.iter().enumerate() {
            writes.push(Self::item_write(
                thread_id,
                record.next_ordinal + offset as u64,
                item,
            )?);
        }
        if record.history_mode() == ThreadHistoryMode::Paginated {
            writes.extend(
                projection::build_writes(
                    self,
                    thread_id,
                    live.created.subagent_history_start_ordinal,
                    record.next_ordinal,
                    &items,
                    Utc::now(),
                )
                .await?,
            );
        }
        let mut updated = record.clone();
        updated.next_ordinal += items.len() as u64;
        writes.push(Write::put(keys::thread(thread_id), to_value(&updated)?));
        self.antfly.write(writes).await.map_err(internal)?;
        *record = updated;
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
        let mapping = self
            .antfly
            .get(keys::rollout_path(&params.rollout_path))
            .await
            .map_err(internal)?;
        let thread_id = mapping
            .as_ref()
            .and_then(|doc| doc.get("thread_id"))
            .and_then(Value::as_str)
            .and_then(|id| ThreadId::from_string(id).ok())
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
        let writes = Self::record_writes(Some(&previous), &record)?;
        self.antfly.write(writes).await.map_err(internal)?;
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
        let mut record = previous.clone();
        record.archived_at = archived.then(Utc::now);
        let writes = Self::record_writes(Some(&previous), &record)?;
        self.antfly.write(writes).await.map_err(internal)?;
        if let Some(live) = state.live.get_mut(&thread_id) {
            live.record = Some(record.clone());
        }
        Ok(record)
    }

    /// Deletes for every paginated-history projection row of `thread_id`
    /// (spec §2.25). Harmless no-op scans for Legacy threads.
    pub(crate) async fn projection_delete_writes(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Vec<Write>> {
        let mut writes = Vec::new();
        for prefix in [
            keys::turn_id_prefix(thread_id),
            keys::turn_start_prefix(thread_id),
            keys::turn_end_prefix(thread_id),
            keys::item_id_prefix(thread_id),
            keys::item_created_prefix(thread_id),
            keys::item_updated_prefix(thread_id),
            keys::realtime_prefix(thread_id),
        ] {
            let documents = self
                .antfly
                .scan(ScanRequest::prefix(&prefix))
                .await
                .map_err(internal)?;
            writes.extend(
                documents
                    .into_iter()
                    .map(|document| Write::delete(document.key)),
            );
        }
        Ok(writes)
    }

    /// Threads whose history starts inside `thread_id`'s, with the exclusive
    /// end ordinal they inherit.
    pub(crate) async fn fork_children(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Vec<(ThreadId, u64)>> {
        let records = self
            .antfly
            .scan_as::<ThreadRecord>(ScanRequest::prefix(keys::THREAD_PREFIX))
            .await
            .map_err(internal)?;
        Ok(records
            .into_iter()
            .filter_map(|(_, record)| {
                let base = record.created.history_base?;
                (base.thread_id == thread_id && record.thread_id() != thread_id)
                    .then_some((record.thread_id(), base.end_ordinal_exclusive))
            })
            .collect())
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
            .scan(ScanRequest::prefix(&keys::items_prefix(thread_id)))
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
        let mut writes: Vec<Write> = items
            .into_iter()
            .map(|document| Write::delete(document.key))
            .collect();
        writes.push(Write::delete(keys::thread(thread_id)));
        writes.extend(self.projection_delete_writes(thread_id).await?);
        if let Some(record) = &record {
            for sort_key in SORT_KEYS {
                writes.push(Write::delete(keys::index_entry(
                    record.archived_at.is_some(),
                    sort_key,
                    record.sort_millis(sort_key),
                    thread_id,
                )));
            }
            if let Some(section) = record.section.as_deref() {
                writes.push(Write::delete(keys::section_entry(
                    section,
                    record.section_position.unwrap_or(i64::MAX),
                    thread_id,
                )));
            }
            if let Some(path) = &record.legacy_rollout_path {
                writes.push(Write::delete(keys::rollout_path(path)));
            }
        }
        writes.extend(attachments::delete_writes_for_thread(self, thread_id).await?);
        self.antfly.write(writes).await.map_err(internal)?;
        state.live.remove(&thread_id);
        state.staged_metadata.remove(&thread_id);
        Ok(())
    }

    async fn move_thread_to_section_impl(
        &self,
        params: MoveThreadToSectionParams,
    ) -> ThreadStoreResult<()> {
        if params
            .section
            .as_deref()
            .is_some_and(|section| section.trim().is_empty())
        {
            return Err(ThreadStoreError::InvalidRequest {
                message: "section must not be empty".to_owned(),
            });
        }
        if params.section.is_none() && params.before_thread_id.is_some() {
            return Err(ThreadStoreError::InvalidRequest {
                message: "before thread cannot be specified without a section".to_owned(),
            });
        }
        let mut state = Arc::clone(&self.state).lock_owned().await;
        let _guard = self.antfly.lock().await;
        let previous =
            self.load_record(params.thread_id)
                .await?
                .ok_or(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                })?;
        let mut changed: Vec<(ThreadRecord, ThreadRecord)> = Vec::new();
        let mut record = previous.clone();
        match params.section.as_deref() {
            None => {
                record.section = None;
                record.section_position = None;
                record.section_entered_at = None;
                record.section_name = None;
                record.section_appearance = None;
            }
            Some(section) => {
                if let Some(before) = params.before_thread_id
                    && before == params.thread_id
                {
                    return Err(ThreadStoreError::InvalidRequest {
                        message: format!(
                            "thread {} cannot be moved before itself",
                            params.thread_id
                        ),
                    });
                }
                let members = self
                    .antfly
                    .scan_as::<ThreadRecord>(ScanRequest::prefix(&keys::section_prefix(section)))
                    .await
                    .map_err(internal)?;
                let mut ordered: Vec<ThreadRecord> = members
                    .into_iter()
                    .map(|(_, member)| member)
                    .filter(|member| member.thread_id() != params.thread_id)
                    .collect();
                if let Some(before) = params.before_thread_id
                    && !ordered.iter().any(|member| member.thread_id() == before)
                {
                    return Err(ThreadStoreError::InvalidRequest {
                        message: format!("before thread {before} is not in section {section}"),
                    });
                }
                if previous.section.as_deref() != Some(section) {
                    record.section = Some(section.to_owned());
                    record.section_entered_at = Some(Utc::now());
                }
                match sections::load_section(self, section).await? {
                    Some(definition) => {
                        record.section_name = Some(definition.name);
                        record.section_appearance = definition.appearance;
                    }
                    None => {
                        record.section_name = None;
                        record.section_appearance = None;
                    }
                }
                let insert_at = params
                    .before_thread_id
                    .and_then(|before| {
                        ordered
                            .iter()
                            .position(|member| member.thread_id() == before)
                    })
                    .unwrap_or(ordered.len());
                ordered.insert(insert_at, record.clone());
                for (index, member) in ordered.into_iter().enumerate() {
                    let position = i64::try_from(index)
                        .unwrap_or(i64::MAX)
                        .saturating_add(1)
                        .saturating_mul(1_000_000);
                    if member.thread_id() == params.thread_id {
                        record.section_position = Some(position);
                    } else if member.section_position != Some(position) {
                        // Re-read the authoritative record before rewriting it.
                        if let Some(current) = self.load_record(member.thread_id()).await? {
                            let mut moved = current.clone();
                            moved.section_position = Some(position);
                            changed.push((current, moved));
                        }
                    }
                }
            }
        }
        let mut writes = Self::record_writes(Some(&previous), &record)?;
        for (old, new) in &changed {
            writes.extend(Self::record_writes(Some(old), new)?);
        }
        self.antfly.write(writes).await.map_err(internal)?;
        for (_, new) in changed.into_iter().chain([(previous, record)]) {
            if let Some(live) = state.live.get_mut(&new.thread_id()) {
                live.record = Some(new);
            }
        }
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
        Box::pin(self.move_thread_to_section_impl(params))
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
