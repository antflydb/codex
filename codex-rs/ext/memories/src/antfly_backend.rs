//! Memories backend over Antfly: hybrid search over indexed notes, with the
//! filesystem remaining the source of truth for note content.
//!
//! `list` and `read` keep the local backend's exact semantics (same paths,
//! same validation, same errors): they delegate to [`LocalMemoriesBackend`]
//! unchanged. `add_ad_hoc_note` and `read` additionally index note content
//! into Antfly, best-effort, so later searches can find it; a failed index
//! never fails the user-facing call, since the filesystem stays the source
//! of truth. `search` runs Antfly hybrid (full text + semantic) search over
//! the indexed documents to pick candidate files, then re-applies the local
//! backend's exact line-matching rules (`match_mode`, `context_lines`,
//! `case_sensitive`, `normalized`) to those files, so the structure of a
//! match is unchanged. The two differences from the local backend's search
//! are: the candidate set is "notes indexed so far" rather than every file
//! under the root, and results are ordered by Antfly relevance instead of
//! path order.
//!
//! The memories write pipeline (`codex-rs/memories/write`) writes files
//! directly to disk and does not go through this backend, so pipeline
//! output (rollout summaries, consolidated notes) is only indexed lazily,
//! the next time an agent reads it through the `read` tool.

mod search;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;

use codex_antfly::Antfly;
use codex_antfly::SEARCH_TEXT_FIELD;
use codex_antfly::Write as AntflyWrite;

use crate::backend::AddAdHocMemoryNoteRequest;
use crate::backend::AddAdHocMemoryNoteResponse;
use crate::backend::ListMemoriesRequest;
use crate::backend::ListMemoriesResponse;
use crate::backend::MemoriesBackend;
use crate::backend::MemoriesBackendError;
use crate::backend::ReadMemoryRequest;
use crate::backend::ReadMemoryResponse;
use crate::backend::SearchMemoriesRequest;
use crate::backend::SearchMemoriesResponse;
use crate::local::LocalMemoriesBackend;

const KEY_PREFIX: &str = "mem";
const NOTE_SEGMENT: &str = "note";

#[derive(Debug, Clone)]
pub(crate) struct AntflyMemoriesBackend {
    local: LocalMemoriesBackend,
    antfly: Arc<Antfly>,
    root: PathBuf,
    /// Distinguishes V1 (`memories`) and V2 (`memories_v2`) notes sharing one
    /// Antfly database.
    namespace: String,
}

impl AntflyMemoriesBackend {
    pub(crate) fn new(
        root: impl Into<PathBuf>,
        antfly: Arc<Antfly>,
        namespace: impl Into<String>,
    ) -> Self {
        let root = root.into();
        Self {
            local: LocalMemoriesBackend::from_memory_root(root.clone()),
            antfly,
            root,
            namespace: namespace.into(),
        }
    }

    fn doc_key(&self, path: &str) -> String {
        format!("{KEY_PREFIX}:{}:{NOTE_SEGMENT}:{path}", self.namespace)
    }

    fn note_prefix(&self) -> String {
        format!("{KEY_PREFIX}:{}:{NOTE_SEGMENT}:", self.namespace)
    }

    /// Best-effort: a failed index never fails the caller's read or write,
    /// since the filesystem (not Antfly) is the source of truth.
    async fn index_note(&self, path: &str, content: &str) {
        let doc = serde_json::json!({
            "path": path,
            SEARCH_TEXT_FIELD: content,
        });
        if let Err(err) = self
            .antfly
            .write(vec![AntflyWrite::put(self.doc_key(path), doc)])
            .await
        {
            tracing::warn!(path, %err, "failed to index memory note into Antfly");
        }
    }

    /// Re-reads `relative_path` from disk and indexes it, best-effort. Used
    /// after a successful `read` so pipeline-written files become
    /// searchable without this backend owning their writes.
    async fn index_note_file(&self, relative_path: &str) {
        let absolute = self.root.join(relative_path);
        match tokio::fs::read_to_string(&absolute).await {
            Ok(content) => self.index_note(relative_path, &content).await,
            Err(err) if err.kind() == std::io::ErrorKind::InvalidData => {
                // Not valid UTF-8; nothing to index.
            }
            Err(err) => {
                tracing::warn!(
                    path = relative_path,
                    %err,
                    "failed to read memory note for indexing"
                );
            }
        }
    }
}

impl MemoriesBackend for AntflyMemoriesBackend {
    async fn add_ad_hoc_note(
        &self,
        request: AddAdHocMemoryNoteRequest,
    ) -> Result<AddAdHocMemoryNoteResponse, MemoriesBackendError> {
        let response = self.local.add_ad_hoc_note(request.clone()).await?;
        let path = format!("extensions/ad_hoc/notes/{}", request.filename);
        self.index_note(&path, &request.note).await;
        Ok(response)
    }

    async fn list(
        &self,
        request: ListMemoriesRequest,
    ) -> Result<ListMemoriesResponse, MemoriesBackendError> {
        self.local.list(request).await
    }

    async fn read(
        &self,
        request: ReadMemoryRequest,
    ) -> Result<ReadMemoryResponse, MemoriesBackendError> {
        let response = self.local.read(request.clone()).await?;
        self.index_note_file(&request.path).await;
        Ok(response)
    }

    async fn search(
        &self,
        request: SearchMemoriesRequest,
    ) -> Result<SearchMemoriesResponse, MemoriesBackendError> {
        search::search(self, request).await
    }
}
