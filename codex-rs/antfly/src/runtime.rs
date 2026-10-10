use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

use serde_json::Value;
use tokio::sync::OnceCell;

use crate::backend::Backend;
use crate::backend::DenseIndex;
use crate::backend::Document;
use crate::backend::LEGACY_TABLE;
use crate::backend::ScanRequest;
use crate::backend::SearchHit;
use crate::backend::TableSpec;
use crate::backend::Write;
use crate::config::AntflyConfig;
use crate::config::BackendConfig;
use crate::embedded::EmbeddedBackend;
use crate::embedded::LocalDecider;
use crate::error::AntflyError;
use crate::error::AntflyResult;
use crate::remote::RemoteBackend;
use crate::schema;
use crate::sql::Sql;

/// Field that every searchable Codex document puts its text into. The dense
/// index embeds it and full-text search matches it.
pub const SEARCH_TEXT_FIELD: &str = "search_text";
const DENSE_INDEX_NAME: &str = "codex_search_text";

/// Process-wide Antfly handle used by every Codex store.
pub struct Antfly {
    config: AntflyConfig,
    /// Opened on first use so configuration never fails at construction;
    /// open errors surface on the first operation instead.
    backend: OnceCell<Arc<dyn Backend>>,
    decider: OnceCell<Arc<LocalDecider>>,
    schema: OnceCell<()>,
    /// Relational tables, migrated and with document tables created on
    /// first use.
    sql: OnceCell<Sql>,
    /// Serializes read-modify-write sequences across stores in this process.
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for Antfly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Antfly")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn registry() -> &'static Mutex<HashMap<BackendConfig, Weak<Antfly>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<BackendConfig, Weak<Antfly>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

/// Returns the process-wide handle for `config.backend`. An embedded database
/// has a single writer, so every store in the process must share one handle.
/// The backend opens on first use.
pub fn shared(config: &AntflyConfig) -> Arc<Antfly> {
    let mut registry = match registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(existing) = registry.get(&config.backend).and_then(Weak::upgrade) {
        return existing;
    }
    let antfly = Arc::new(Antfly::new(config.clone()));
    registry.insert(config.backend.clone(), Arc::downgrade(&antfly));
    antfly
}

fn api_key(api_key_env: Option<&String>) -> AntflyResult<Option<String>> {
    match api_key_env {
        Some(name) => std::env::var(name)
            .map(Some)
            .map_err(|_| AntflyError::Config(format!("environment variable {name} is not set"))),
        None => Ok(None),
    }
}

fn open_backend(config: &BackendConfig) -> AntflyResult<Arc<dyn Backend>> {
    Ok(match config {
        BackendConfig::Embedded { path } => Arc::new(EmbeddedBackend::open(path)?),
        BackendConfig::Remote {
            url,
            table,
            api_key_env,
            ..
        } => Arc::new(RemoteBackend::new(
            url,
            table,
            api_key(api_key_env.as_ref())?,
        )?),
    })
}

impl Antfly {
    /// A new, unshared handle. Prefer [`shared`].
    pub fn new(config: AntflyConfig) -> Self {
        Self {
            config,
            backend: OnceCell::new(),
            decider: OnceCell::new(),
            schema: OnceCell::new(),
            sql: OnceCell::new(),
            lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Opens the backend now, reporting configuration errors immediately.
    pub fn open(config: AntflyConfig) -> AntflyResult<Self> {
        let backend = open_backend(&config.backend)?;
        Ok(Self::with_backend(config, backend))
    }

    /// Wraps an existing backend, for tests and alternative transports.
    pub fn with_backend(config: AntflyConfig, backend: Arc<dyn Backend>) -> Self {
        let antfly = Self::new(config);
        let _ = antfly.backend.set(backend);
        antfly
    }

    /// The backend, opening it on first use.
    pub async fn backend(&self) -> AntflyResult<&Arc<dyn Backend>> {
        self.backend
            .get_or_try_init(|| async {
                let config = self.config.backend.clone();
                // Opening an embedded database blocks briefly.
                tokio::task::spawn_blocking(move || open_backend(&config))
                    .await
                    .map_err(|_| AntflyError::ExecutorUnavailable)?
            })
            .await
    }

    pub fn config(&self) -> &AntflyConfig {
        &self.config
    }

    /// Holds the process-wide read-modify-write lock.
    pub async fn lock(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.lock).lock_owned().await
    }

    async fn ready(&self) -> AntflyResult<()> {
        self.schema
            .get_or_try_init(|| async {
                let dense = self.config.embedder.as_ref().map(|embedder| DenseIndex {
                    name: DENSE_INDEX_NAME.to_string(),
                    field: SEARCH_TEXT_FIELD.to_string(),
                    model: embedder.model.clone(),
                    dims: embedder.dims,
                });
                self.backend()
                    .await?
                    .ensure_table(TableSpec {
                        name: LEGACY_TABLE.to_string(),
                        schema: Value::Null,
                        dense,
                    })
                    .await
            })
            .await
            .map(|_| ())
    }

    pub async fn write(&self, writes: Vec<Write>) -> AntflyResult<()> {
        self.ready().await?;
        self.backend()
            .await?
            .write(LEGACY_TABLE.to_string(), writes)
            .await
    }

    pub async fn get(&self, key: impl Into<String>) -> AntflyResult<Option<Value>> {
        self.backend()
            .await?
            .get(LEGACY_TABLE.to_string(), key.into())
            .await
    }

    pub async fn get_as<T: serde::de::DeserializeOwned>(
        &self,
        key: impl Into<String>,
    ) -> AntflyResult<Option<T>> {
        match self.get(key).await? {
            Some(doc) => Ok(Some(serde_json::from_value(strip_reserved(doc))?)),
            None => Ok(None),
        }
    }

    pub async fn scan(&self, request: ScanRequest) -> AntflyResult<Vec<Document>> {
        self.backend()
            .await?
            .scan(LEGACY_TABLE.to_string(), request)
            .await
    }

    /// Scans `prefix` and decodes each document as `T`.
    pub async fn scan_as<T: serde::de::DeserializeOwned>(
        &self,
        request: ScanRequest,
    ) -> AntflyResult<Vec<(String, T)>> {
        let documents = self.scan(request).await?;
        documents
            .into_iter()
            .map(|document| {
                Ok((
                    document.key,
                    serde_json::from_value(strip_reserved(document.doc))?,
                ))
            })
            .collect()
    }

    /// Full-text matches for `text` among documents under `prefix`.
    pub async fn search_full_text(
        &self,
        prefix: &str,
        text: &str,
        limit: usize,
    ) -> AntflyResult<Vec<SearchHit>> {
        self.ready().await?;
        self.backend()
            .await?
            .search(
                LEGACY_TABLE.to_string(),
                serde_json::json!({
                    "full_text_search": {"match": {"field": SEARCH_TEXT_FIELD, "text": text}},
                    "filter_prefix": prefix,
                    "limit": limit,
                }),
            )
            .await
    }

    /// Nearest neighbors of `text` among documents under `prefix`, or nothing
    /// when no embedder is configured.
    pub async fn search_semantic(
        &self,
        prefix: &str,
        text: &str,
        limit: usize,
    ) -> AntflyResult<Vec<SearchHit>> {
        if self.config.embedder.is_none() {
            return Ok(Vec::new());
        }
        self.ready().await?;
        self.backend()
            .await?
            .search(
                LEGACY_TABLE.to_string(),
                serde_json::json!({
                    "semantic_search": text,
                    "indexes": [DENSE_INDEX_NAME],
                    "filter_prefix": prefix,
                    "limit": limit,
                }),
            )
            .await
    }

    /// Full-text matches followed by semantic neighbors not already matched.
    /// Semantic failures degrade to full text only.
    pub async fn search_text(
        &self,
        prefix: &str,
        text: &str,
        full_text_limit: usize,
        semantic_limit: usize,
    ) -> AntflyResult<Vec<SearchHit>> {
        let mut hits = self.search_full_text(prefix, text, full_text_limit).await?;
        if semantic_limit == 0 {
            return Ok(hits);
        }
        match self.search_semantic(prefix, text, semantic_limit).await {
            Ok(semantic) => {
                let seen: std::collections::HashSet<String> =
                    hits.iter().map(|hit| hit.key.clone()).collect();
                hits.extend(semantic.into_iter().filter(|hit| !seen.contains(&hit.key)));
            }
            Err(err) => tracing::warn!("antfly semantic search failed, using full text: {err}"),
        }
        Ok(hits)
    }

    pub async fn search(&self, request: Value) -> AntflyResult<Vec<SearchHit>> {
        self.ready().await?;
        self.backend()
            .await?
            .search(LEGACY_TABLE.to_string(), request)
            .await
    }

    fn dense_index(&self) -> Option<DenseIndex> {
        self.config.embedder.as_ref().map(|embedder| DenseIndex {
            name: DENSE_INDEX_NAME.to_string(),
            field: SEARCH_TEXT_FIELD.to_string(),
            model: embedder.model.clone(),
            dims: embedder.dims,
        })
    }

    /// The SQL pool for relational tables. On first use it connects, applies
    /// [`schema::MIGRATIONS`], and creates the [`schema::DOCUMENT_TABLES`].
    pub async fn sql(&self) -> AntflyResult<&Sql> {
        self.sql
            .get_or_try_init(|| async {
                let sql = match &self.config.backend {
                    BackendConfig::Embedded { path } => {
                        // The document backend creates the file and its
                        // directory; open it first.
                        self.backend().await?;
                        Sql::connect_embedded(path.clone(), false).await?
                    }
                    BackendConfig::Remote { sql_url, .. } => {
                        let url = sql_url.as_deref().ok_or_else(|| {
                            AntflyError::Config(
                                "antfly.sql_url is required for a remote backend".to_string(),
                            )
                        })?;
                        Sql::connect_remote(url).await?
                    }
                };
                schema::migrate(&sql, schema::MIGRATIONS).await?;
                let backend = self.backend().await?;
                for table in schema::DOCUMENT_TABLES {
                    backend
                        .ensure_table(TableSpec {
                            name: table.name.to_string(),
                            schema: table.schema_json(),
                            dense: self.dense_index(),
                        })
                        .await?;
                }
                Ok::<_, AntflyError>(sql)
            })
            .await
    }

    /// A document table from [`schema::DOCUMENT_TABLES`].
    pub fn documents(&self, table: &'static str) -> Documents<'_> {
        Documents {
            antfly: self,
            table,
        }
    }

    fn decider(&self) -> AntflyResult<Arc<LocalDecider>> {
        if let Some(decider) = self.decider.get() {
            return Ok(Arc::clone(decider));
        }
        let decider = Arc::new(LocalDecider::new(self.config.models_dir.clone())?);
        let _ = self.decider.set(Arc::clone(&decider));
        Ok(self.decider.get().map(Arc::clone).unwrap_or(decider))
    }

    /// Closes the backend and the decision runtime, resolving once both have
    /// released their resources (including background indexing), so their
    /// files can be removed. Later calls fail.
    pub async fn close(&self) -> AntflyResult<()> {
        let decider = match self.decider.get() {
            Some(decider) => decider.close().await,
            None => Ok(()),
        };
        if let Some(sql) = self.sql.get() {
            sql.close().await;
        }
        if let Some(backend) = self.backend.get() {
            backend.close().await?;
        }
        decider
    }

    /// Loads the decision model so the first approval is not slow.
    pub async fn warm_decider(&self) -> AntflyResult<()> {
        self.decider()?.warm().await
    }

    /// Answers a typed-decision `DecideRequest` with the local runtime.
    pub async fn decide(&self, request: &Value) -> AntflyResult<Value> {
        self.decider()?.decide(request).await
    }
}

/// One document table. Its schema and indexes are created with the SQL
/// tables on first use (see [`Antfly::sql`]).
pub struct Documents<'a> {
    antfly: &'a Antfly,
    table: &'static str,
}

impl Documents<'_> {
    async fn backend(&self) -> AntflyResult<&Arc<dyn Backend>> {
        self.antfly.sql().await?;
        self.antfly.backend().await
    }

    pub fn name(&self) -> &'static str {
        self.table
    }

    pub async fn write(&self, writes: Vec<Write>) -> AntflyResult<()> {
        self.backend()
            .await?
            .write(self.table.to_string(), writes)
            .await
    }

    pub async fn get(&self, key: impl Into<String>) -> AntflyResult<Option<Value>> {
        self.backend()
            .await?
            .get(self.table.to_string(), key.into())
            .await
    }

    pub async fn get_as<T: serde::de::DeserializeOwned>(
        &self,
        key: impl Into<String>,
    ) -> AntflyResult<Option<T>> {
        match self.get(key).await? {
            Some(doc) => Ok(Some(serde_json::from_value(strip_reserved(doc))?)),
            None => Ok(None),
        }
    }

    pub async fn scan(&self, request: ScanRequest) -> AntflyResult<Vec<Document>> {
        self.backend()
            .await?
            .scan(self.table.to_string(), request)
            .await
    }

    /// Executes an Antfly `QueryRequest` against this table.
    pub async fn search(&self, request: Value) -> AntflyResult<Vec<SearchHit>> {
        self.backend()
            .await?
            .search(self.table.to_string(), request)
            .await
    }

    /// Full-text matches followed by semantic neighbors not already matched,
    /// optionally restricted by an Antfly filter query. Semantic failures
    /// degrade to full text only.
    pub async fn search_text(
        &self,
        text: &str,
        filter: Option<Value>,
        full_text_limit: usize,
        semantic_limit: usize,
    ) -> AntflyResult<Vec<SearchHit>> {
        let mut full_text = serde_json::json!({
            "full_text_search": {"match": {"field": SEARCH_TEXT_FIELD, "text": text}},
            "limit": full_text_limit,
        });
        if let Some(filter) = &filter {
            full_text["filter_query"] = filter.clone();
        }
        let mut hits = self.search(full_text).await?;
        if semantic_limit == 0 || self.antfly.config.embedder.is_none() {
            return Ok(hits);
        }
        let mut semantic = serde_json::json!({
            "semantic_search": text,
            "indexes": [DENSE_INDEX_NAME],
            "limit": semantic_limit,
        });
        if let Some(filter) = filter {
            semantic["filter_query"] = filter;
        }
        match self.search(semantic).await {
            Ok(semantic) => {
                let seen: std::collections::HashSet<String> =
                    hits.iter().map(|hit| hit.key.clone()).collect();
                hits.extend(semantic.into_iter().filter(|hit| !seen.contains(&hit.key)));
            }
            Err(err) => tracing::warn!("antfly semantic search failed, using full text: {err}"),
        }
        Ok(hits)
    }
}

/// Removes Antfly-managed fields (`_embeddings`, `_chunks`, ...) and the
/// search text so stored documents decode into store types.
pub fn strip_reserved(mut doc: Value) -> Value {
    if let Some(map) = doc.as_object_mut() {
        map.retain(|key, _| !key.starts_with('_') && key != SEARCH_TEXT_FIELD);
    }
    doc
}
