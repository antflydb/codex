//! Bulk import of complete threads, used to migrate local history.
//!
//! Items are written in chunks with sequential ordinals (and, for Paginated
//! threads, their projection rows in the same chunk). The thread row is
//! written last, so a thread only appears in listings once all of its
//! history is in place; re-running an interrupted import rewrites the same
//! rows.

use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::schema;
use codex_antfly::sql_params;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;

use super::AntflyThreadStore;
use super::internal;
use super::projection;
use super::record::ThreadRecord;
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
            let cleanup = self
                .antfly()
                .documents(schema::HISTORY_ITEMS)
                .scan(codex_antfly::ScanRequest::prefix(
                    &super::keys::history_item_prefix(thread_id),
                ))
                .await
                .map_err(internal)?
                .into_iter()
                .map(|document| codex_antfly::Write::delete(document.key))
                .collect::<Vec<_>>();
            if !cleanup.is_empty() {
                self.antfly()
                    .documents(schema::HISTORY_ITEMS)
                    .write(cleanup)
                    .await
                    .map_err(internal)?;
            }
            self.projection_delete(thread_id).await?;
        }

        let paginated = params.created.history_mode == ThreadHistoryMode::Paginated;
        let imported_at = Utc::now();
        let mut ordinal = 0u64;
        for chunk in params.items.chunks(IMPORT_CHUNK) {
            let mut writes = Vec::with_capacity(chunk.len());
            for (offset, item) in chunk.iter().enumerate() {
                writes.push(Self::item_write(thread_id, ordinal + offset as u64, item)?);
            }
            self.antfly()
                .documents(schema::HISTORY_ITEMS)
                .write(writes)
                .await
                .map_err(internal)?;
            if paginated {
                projection::apply_batch(
                    self,
                    thread_id,
                    params.created.subagent_history_start_ordinal,
                    ordinal,
                    chunk,
                    imported_at,
                )
                .await?;
            }
            ordinal += chunk.len() as u64;
        }

        let materialized_at = params.patch.created_at.unwrap_or(imported_at);
        let mut record = ThreadRecord::new(params.created, materialized_at);
        record.patch = params.patch;
        record.archived_at = params.archived_at;
        record.next_ordinal = ordinal;
        record.legacy_rollout_path = params.legacy_rollout_path;
        if let Some(section) = params.section {
            let sql = self.antfly().sql().await.map_err(internal)?;
            sql.execute(
                "INSERT INTO codex_thread_sections (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
                sql_params![section.id.clone(), section.name.clone()],
            )
            .await
            .map_err(internal)?;
            record.section = Some(section.id);
            record.section_position = section.position;
            record.section_entered_at = Some(materialized_at);
        }
        self.save_record(&record).await?;
        Ok(ImportOutcome::Imported)
    }
}
