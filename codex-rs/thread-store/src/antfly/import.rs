//! Bulk import of complete threads, used to migrate local history.
//!
//! Items are written in chunks with sequential ordinals (and, for Paginated
//! threads, their projection rows in the same write). The thread record goes
//! last, so a thread only appears in listings once all of its history is in
//! place; re-running an interrupted import rewrites the same keys.

use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Write;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;

use super::AntflyThreadStore;
use super::internal;
use super::keys;
use super::projection;
use super::record::ThreadRecord;
use super::to_value;
use crate::CreateThreadParams;
use crate::ThreadMetadataPatch;
use crate::ThreadStoreResult;

/// Items written per batch.
const IMPORT_CHUNK: usize = 256;

/// A complete thread to import.
#[derive(Debug)]
pub struct ImportThreadParams {
    pub created: CreateThreadParams,
    /// Full history in order, starting with the thread's `SessionMeta`.
    pub items: Vec<RolloutItem>,
    pub patch: ThreadMetadataPatch,
    pub archived_at: Option<DateTime<Utc>>,
    pub section: Option<ImportSection>,
    /// Where the thread came from, so `read_thread_by_rollout_path` keeps
    /// resolving it.
    pub legacy_rollout_path: Option<PathBuf>,
    /// Overwrite a thread that already exists.
    pub replace: bool,
}

#[derive(Clone, Debug)]
pub struct ImportSection {
    pub id: String,
    pub name: String,
    pub position: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportOutcome {
    Imported,
    AlreadyPresent,
}

impl AntflyThreadStore {
    /// Writes a complete thread. Existing threads are left alone unless
    /// `replace` is set.
    pub async fn import_thread(
        &self,
        params: ImportThreadParams,
    ) -> ThreadStoreResult<ImportOutcome> {
        let thread_id = params.created.thread_id;
        let _guard = self.antfly().lock().await;
        let previous = self.load_record(thread_id).await?;
        if previous.is_some() && !params.replace {
            return Ok(ImportOutcome::AlreadyPresent);
        }
        if previous.is_some() {
            let mut cleanup: Vec<Write> = self
                .antfly()
                .scan(codex_antfly::ScanRequest::prefix(&keys::items_prefix(
                    thread_id,
                )))
                .await
                .map_err(internal)?
                .into_iter()
                .map(|document| Write::delete(document.key))
                .collect();
            cleanup.extend(self.projection_delete_writes(thread_id).await?);
            self.antfly().write(cleanup).await.map_err(internal)?;
        }

        let paginated = params.created.history_mode == ThreadHistoryMode::Paginated;
        let imported_at = Utc::now();
        let mut ordinal = 0u64;
        for chunk in params.items.chunks(IMPORT_CHUNK) {
            let mut writes = Vec::with_capacity(chunk.len() * 2);
            for (offset, item) in chunk.iter().enumerate() {
                writes.push(Self::item_write(thread_id, ordinal + offset as u64, item)?);
            }
            if paginated {
                writes.extend(
                    projection::build_writes(
                        self,
                        thread_id,
                        params.created.subagent_history_start_ordinal,
                        ordinal,
                        chunk,
                        imported_at,
                    )
                    .await?,
                );
            }
            self.antfly().write(writes).await.map_err(internal)?;
            ordinal += chunk.len() as u64;
        }

        let materialized_at = params.patch.created_at.unwrap_or(imported_at);
        let mut record = ThreadRecord::new(params.created, materialized_at);
        record.patch = params.patch;
        record.archived_at = params.archived_at;
        record.next_ordinal = ordinal;
        record.legacy_rollout_path = params.legacy_rollout_path;
        let mut writes = Vec::new();
        if let Some(section) = params.section {
            let definition = keys::section_def(&section.id);
            if self
                .antfly()
                .get(definition.clone())
                .await
                .map_err(internal)?
                .is_none()
            {
                writes.push(Write::put(
                    definition,
                    to_value(&codex_state::ThreadSection {
                        id: section.id.clone(),
                        name: section.name.clone(),
                        appearance: None,
                    })?,
                ));
            }
            record.section = Some(section.id);
            record.section_name = Some(section.name);
            record.section_position = section.position;
            record.section_entered_at = Some(materialized_at);
        }
        writes.extend(Self::record_writes(previous.as_ref(), &record)?);
        self.antfly().write(writes).await.map_err(internal)?;
        Ok(ImportOutcome::Imported)
    }
}
