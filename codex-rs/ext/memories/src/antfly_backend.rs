//! Memories backend over Antfly: hybrid search over indexed notes, with the
//! filesystem remaining the source of truth for note content.
//!
//! Notes are indexed as documents in `schema::MEMORY_NOTES`
//! (`codex_memory_notes`), one document per note path, with declared fields
//! `namespace`, `path` and `updated_at_ms` plus `search_text`. V1
//! (`memories`) and V2 (`memories_v2`) notes share that one table and are
//! distinguished by `namespace`; every search filters on it so the two
//! versions' notes never mix, matching the old `mem:{ns}:note:{path}`
//! key-prefix separation.
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
//! directly to disk and does not go through this backend. [`reconcile`]
//! brings the index up to date with the files on disk: it runs when a thread
//! starts and, rate-limited, before each search, so pipeline output
//! (rollout summaries, consolidated notes) reaches search and the knowledge
//! graph without waiting for an agent to read it.
//!
//! [`reconcile`]: AntflyMemoriesBackend::reconcile

mod search;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use codex_antfly::Antfly;
use codex_antfly::SEARCH_TEXT_FIELD;
use codex_antfly::ScanRequest;
use codex_antfly::Write as AntflyWrite;
use codex_antfly::schema;

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

#[derive(Debug, Clone)]
pub(crate) struct AntflyMemoriesBackend {
    local: LocalMemoriesBackend,
    antfly: Arc<Antfly>,
    root: PathBuf,
    /// Distinguishes V1 (`memories`) and V2 (`memories_v2`) notes sharing one
    /// `codex_memory_notes` table; every search filters on it.
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

    /// Document id for one note: unique across namespaces since both share
    /// `codex_memory_notes`.
    fn doc_id(&self, path: &str) -> String {
        format!("{}:{path}", self.namespace)
    }

    /// `antfly_search` filter restricting a search to this namespace, and
    /// optionally to one exact note path (used when the caller named a
    /// `path`, which previously scanned a single-key KV prefix).
    fn namespace_filter(&self, path: Option<&str>) -> serde_json::Value {
        let namespace_term = serde_json::json!({"term": {"namespace": self.namespace}});
        match path {
            Some(path) => serde_json::json!({
                "conjuncts": [namespace_term, {"term": {"path": path}}],
            }),
            None => namespace_term,
        }
    }

    /// Best-effort: a failed index never fails the caller's read or write,
    /// since the filesystem (not Antfly) is the source of truth.
    async fn index_note(&self, path: &str, content: &str) {
        let modified_ms = tokio::fs::metadata(self.root.join(path))
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|elapsed| elapsed.as_millis() as i64);
        let doc = self.note_doc(path, content, modified_ms);
        if let Err(err) = self
            .antfly
            .documents(schema::MEMORY_NOTES)
            .write(vec![AntflyWrite::put(self.doc_id(path), doc)])
            .await
        {
            tracing::warn!(path, %err, "failed to index memory note into Antfly");
        }
    }

    fn note_doc(
        &self,
        path: &str,
        content: &str,
        file_modified_ms: Option<i64>,
    ) -> serde_json::Value {
        let mut doc = serde_json::json!({
            "namespace": self.namespace,
            "path": path,
            "updated_at_ms": now_ms(),
            SEARCH_TEXT_FIELD: content,
        });
        if let Some(modified) = file_modified_ms {
            doc["file_modified_ms"] = serde_json::json!(modified);
        }
        doc
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

/// Searches reconcile at most this often per memory root.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
/// Larger files are left out of the index rather than read whole.
const MAX_INDEXED_FILE_BYTES: u64 = 512 * 1024;

static LAST_RECONCILE: LazyLock<Mutex<HashMap<PathBuf, Instant>>> = LazyLock::new(Mutex::default);

/// Every regular file under `root` with its modification time, as
/// `/`-separated relative paths. Hidden entries (such as `.git`) are skipped.
fn note_files(root: &Path) -> Vec<(String, i64)> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            let path = entry.path();
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() || metadata.len() > MAX_INDEXED_FILE_BYTES {
                continue;
            }
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|elapsed| elapsed.as_millis() as i64)
                .unwrap_or_default();
            files.push((relative, modified_ms));
        }
    }
    files
}

impl AntflyMemoriesBackend {
    /// Indexes notes whose files changed since they were indexed and drops
    /// notes whose files are gone. Best effort, like every index write here.
    pub(crate) async fn reconcile(&self) {
        let root = self.root.clone();
        let files = match tokio::task::spawn_blocking(move || note_files(&root)).await {
            Ok(files) => files,
            Err(err) => {
                tracing::warn!(%err, "failed to walk memory notes");
                return;
            }
        };
        let documents = self.antfly.documents(schema::MEMORY_NOTES);
        let prefix = format!("{}:", self.namespace);
        let indexed: HashMap<String, Option<i64>> =
            match documents.scan(ScanRequest::prefix(&prefix)).await {
                Ok(documents) => documents
                    .into_iter()
                    .filter_map(|document| {
                        let path = document.doc.get("path")?.as_str()?.to_string();
                        let modified = document
                            .doc
                            .get("file_modified_ms")
                            .and_then(serde_json::Value::as_i64);
                        Some((path, modified))
                    })
                    .collect(),
                Err(err) => {
                    tracing::warn!(%err, "failed to list indexed memory notes");
                    return;
                }
            };
        let mut writes = Vec::new();
        for (path, modified_ms) in &files {
            if indexed.get(path) == Some(&Some(*modified_ms)) {
                continue;
            }
            let Ok(content) = tokio::fs::read_to_string(self.root.join(path)).await else {
                continue;
            };
            writes.push(AntflyWrite::put(
                self.doc_id(path),
                self.note_doc(path, &content, Some(*modified_ms)),
            ));
        }
        let present: std::collections::HashSet<&str> =
            files.iter().map(|(path, _)| path.as_str()).collect();
        for path in indexed
            .keys()
            .filter(|path| !present.contains(path.as_str()))
        {
            writes.push(AntflyWrite::delete(self.doc_id(path)));
        }
        if writes.is_empty() {
            return;
        }
        if let Err(err) = documents.write(writes).await {
            tracing::warn!(%err, "failed to reconcile memory notes into Antfly");
        }
    }

    /// [`Self::reconcile`], unless this memory root was reconciled recently.
    pub(crate) async fn reconcile_if_stale(&self) {
        let now = Instant::now();
        {
            let Ok(mut last) = LAST_RECONCILE.lock() else {
                return;
            };
            if last
                .get(&self.root)
                .is_some_and(|at| now.duration_since(*at) < RECONCILE_INTERVAL)
            {
                return;
            }
            last.insert(self.root.clone(), now);
        }
        self.reconcile().await;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
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
        self.reconcile_if_stale().await;
        search::search(self, request).await
    }
}
