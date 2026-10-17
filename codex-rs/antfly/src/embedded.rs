use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use antfly_embedded::Database;
use antfly_embedded::Inference;
use antfly_embedded::InferenceOptions;
use antfly_embedded::MIN_THREAD_STACK_SIZE;
use antfly_embedded::OpenOptions;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use tokio::sync::oneshot;

use crate::backend::Backend;
use crate::backend::BackendFuture;
use crate::backend::Document;
use crate::backend::GraphIndex;
use crate::backend::ScanRequest;
use crate::backend::TableSpec;
use crate::backend::Write;
use crate::error::AntflyError;
use crate::error::AntflyResult;

/// Worker threads for blocking `libantfly` calls. `libantfly` serializes
/// calls on one handle, so a small pool only overlaps inference with storage.
const EXECUTOR_THREADS: usize = 2;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Runs closures on threads whose stacks satisfy `libantfly`'s 8 MiB minimum.
/// Tokio worker and blocking-pool threads are too small for it.
pub(crate) struct Executor {
    sender: Mutex<mpsc::Sender<Job>>,
}

impl Executor {
    pub(crate) fn new(name: &str) -> AntflyResult<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..EXECUTOR_THREADS {
            let receiver = Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("{name}-{index}"))
                .stack_size(MIN_THREAD_STACK_SIZE)
                .spawn(move || {
                    loop {
                        let job = match receiver.lock() {
                            Ok(guard) => guard.recv(),
                            Err(_) => return,
                        };
                        match job {
                            Ok(job) => job(),
                            Err(_) => return,
                        }
                    }
                })
                .map_err(|err| AntflyError::Embedded(format!("spawn executor thread: {err}")))?;
        }
        Ok(Self {
            sender: Mutex::new(sender),
        })
    }

    pub(crate) async fn run<T, F>(&self, f: F) -> AntflyResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> AntflyResult<T> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job: Job = Box::new(move || {
            let _ = tx.send(f());
        });
        self.sender
            .lock()
            .map_err(|_| AntflyError::ExecutorUnavailable)?
            .send(job)
            .map_err(|_| AntflyError::ExecutorUnavailable)?;
        rx.await.map_err(|_| AntflyError::ExecutorUnavailable)?
    }
}

/// A `.aflite` database and embedded inference runtime in this process.
pub struct EmbeddedBackend {
    /// `None` only while dropping.
    db: Option<Arc<Database>>,
    executor: Executor,
    known_indexes: Mutex<HashSet<String>>,
    /// `table:desired graph name` → the graph index actually serving it.
    /// Differs while a replacement of an older version is still building.
    serving: Mutex<HashMap<String, String>>,
}

/// How long opening waits for a graph replacement before letting it finish
/// in the background (the old index keeps serving meanwhile).
const REPLACE_WAIT_MS: u64 = 250;
/// Bounded retries for schema calls that return `ANTFLY_BUSY` while replay or
/// promotion is mid-batch.
const SCHEMA_BUSY_RETRIES: u32 = 20;

fn is_kind(err: &antfly_embedded::Error, kind: antfly_embedded::Error) -> bool {
    err.kind() == kind
}

/// Runs a schema call, retrying `ANTFLY_BUSY` with a short backoff.
fn retry_busy<T>(
    mut call: impl FnMut() -> antfly_embedded::Result<T>,
) -> antfly_embedded::Result<T> {
    let mut attempt = 0;
    loop {
        match call() {
            Err(err)
                if is_kind(&err, antfly_embedded::Error::Busy) && attempt < SCHEMA_BUSY_RETRIES =>
            {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_millis(
                    5 * u64::from(attempt.min(20)),
                ));
            }
            result => return result,
        }
    }
}

/// Outcome of reconciling one graph index.
enum GraphState {
    /// The desired index serves.
    Ready,
    /// `old` keeps serving while the replacement builds in the background.
    Replacing { old: String },
    /// Antfly refused the replacement; `old` keeps serving.
    Refused { old: String, error: String },
}

/// Moves curated resolver identities into the namespace `config` declares.
///
/// Antfly replaces a graph whose inline resolver changes `table` or
/// `key_template` only when that resolver has no merges or aliases; curated
/// identities need an explicit `set_resolver_namespace` first. Merge
/// endpoints are mapped to themselves: Codex's template changes (`slug` to
/// `uslug`) render ASCII names to the same keys, so existing redirects stay
/// valid in the new namespace.
fn migrate_resolver_namespaces(db: &Database, config: &Value) -> AntflyResult<()> {
    let Some(resolvers) = config.get("resolvers").and_then(Value::as_array) else {
        return Ok(());
    };
    let identities: Value = serde_json::from_slice(
        &db.resolver_identities_json()
            .map_err(embedded("list resolver identities"))?,
    )?;
    for resolver in resolvers {
        let (Some(name), Some(table), Some(template)) = (
            resolver.get("name").and_then(Value::as_str),
            resolver.get("table").and_then(Value::as_str),
            resolver.get("key_template").and_then(Value::as_str),
        ) else {
            continue;
        };
        let Some(identity) = identities
            .as_array()
            .into_iter()
            .flatten()
            .find(|identity| {
                identity.pointer("/config/name").and_then(Value::as_str) == Some(name)
            })
        else {
            continue;
        };
        let unchanged = identity.pointer("/config/table").and_then(Value::as_str) == Some(table)
            && identity
                .pointer("/config/key_template")
                .and_then(Value::as_str)
                == Some(template);
        let merges = identity
            .get("merges")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if unchanged || merges.is_empty() {
            continue;
        }
        let mut keys = std::collections::BTreeSet::new();
        for merge in &merges {
            for field in ["key", "target"] {
                if let Some(key) = merge.get(field).and_then(Value::as_str) {
                    keys.insert(key.to_string());
                }
            }
        }
        let request = serde_json::to_vec(&json!({
            "table": table,
            "key_template": template,
            "key_map": keys
                .iter()
                .map(|key| json!({"key": key, "target": key}))
                .collect::<Vec<_>>(),
        }))?;
        tracing::info!(
            "antfly: moving resolver {name} and its {} merges to {template}",
            merges.len()
        );
        retry_busy(|| db.set_resolver_namespace_json(name, &request))
            .map_err(embedded("set resolver namespace"))?;
    }
    Ok(())
}

/// Adds `name` (replacing `stale` when given) and reports which index serves.
fn reconcile_graph(
    db: &Database,
    name: &str,
    stale: Option<&str>,
    config: &Value,
) -> AntflyResult<GraphState> {
    let Some(old) = stale else {
        let envelope = serde_json::to_vec(&json!({
            "name": name, "kind": "graph", "config_json": config.to_string(),
        }))?;
        retry_busy(|| db.add_index_json(&envelope)).map_err(embedded("add graph index"))?;
        return Ok(GraphState::Ready);
    };
    // Atomic replacement: the old index serves until the new one is built
    // and swapped in; on timeout the build continues in background
    // maintenance and the next open re-sends the identical request.
    let envelope = serde_json::to_vec(&json!({
        "name": name, "kind": "graph", "config_json": config.to_string(),
        "replaces": old, "wait": {"timeout_ms": REPLACE_WAIT_MS},
    }))?;
    let pending = || GraphState::Replacing {
        old: old.to_string(),
    };
    for attempt in 0..2 {
        match db.add_index_json(&envelope) {
            Ok(()) => return Ok(GraphState::Ready),
            Err(err) if is_kind(&err, antfly_embedded::Error::Busy) => return Ok(pending()),
            Err(err)
                if is_kind(&err, antfly_embedded::Error::Cancelled)
                    || is_kind(&err, antfly_embedded::Error::ReplacementPending) =>
            {
                // Completion can race an interrupted wait; trust the catalog.
                let indexes = names(&db.indexes_json().map_err(embedded("list indexes"))?);
                return Ok(if indexes.contains(name) {
                    GraphState::Ready
                } else {
                    pending()
                });
            }
            Err(err)
                if attempt == 0
                    && is_kind(
                        &err,
                        antfly_embedded::Error::ReplacementConfigurationMismatch,
                    ) =>
            {
                // An earlier build of a different config holds the name.
                tracing::info!("antfly: replacing stale pending graph build {name}: {err}");
                db.cancel_replacement(name)
                    .map_err(embedded("cancel stale replacement"))?;
                let _ = db.run_until_idle();
            }
            Err(err) if is_kind(&err, antfly_embedded::Error::ReplacementFailed) => {
                // Keep serving the old index; clear the failed build so the
                // next open starts a fresh one.
                tracing::warn!("antfly: graph replacement {old} -> {name} failed: {err}");
                db.cancel_replacement(name)
                    .map_err(embedded("cancel failed replacement"))?;
                return Ok(pending());
            }
            Err(err) => return Err(embedded("replace graph index")(err)),
        }
    }
    Ok(pending())
}

impl EmbeddedBackend {
    /// Opens `path`, creating it when missing. Blocks; call during startup.
    /// Enrichments load models from `models_dir`, or Antfly's default
    /// (`~/.antfly/inference/models`) when unset.
    pub fn open(path: &Path, models_dir: Option<&Path>) -> AntflyResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| AntflyError::Embedded(format!("create {parent:?}: {err}")))?;
        }
        let executor = Executor::new("antfly")?;
        let path = path.to_path_buf();
        // Opening also needs a large stack, so do it on an executor thread.
        let (tx, rx) = mpsc::channel();
        let open_path = path.clone();
        let models_dir = models_dir.map(Path::to_path_buf);
        std::thread::Builder::new()
            .name("antfly-open".to_string())
            .stack_size(MIN_THREAD_STACK_SIZE)
            .spawn(move || {
                let mut options = OpenOptions::new()
                    .local_runtime_configured(true)
                    .busy_timeout(BUSY_TIMEOUT);
                if let Some(dir) = &models_dir {
                    options = options.models_dir(dir);
                }
                let result = if open_path.exists() {
                    Database::open(&open_path, &options)
                } else {
                    Database::create(&open_path, &options)
                };
                let _ = tx.send(result);
            })
            .map_err(|err| AntflyError::Embedded(format!("spawn open thread: {err}")))?;
        let db = rx
            .recv()
            .map_err(|_| AntflyError::ExecutorUnavailable)?
            .map_err(|err| AntflyError::Embedded(format!("open {path:?}: {err}")))?;
        Ok(Self {
            db: Some(Arc::new(db)),
            executor,
            known_indexes: Mutex::new(HashSet::new()),
            serving: Mutex::new(HashMap::new()),
        })
    }

    async fn call<T, F>(&self, f: F) -> AntflyResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> AntflyResult<T> + Send + 'static,
    {
        let db = self
            .db
            .as_ref()
            .map(Arc::clone)
            .ok_or(AntflyError::ExecutorUnavailable)?;
        self.executor.run(move || f(&db)).await
    }

    /// Runs `f` against `table`'s handle on an executor thread.
    async fn call_in<T, F>(&self, table: String, f: F) -> AntflyResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> AntflyResult<T> + Send + 'static,
    {
        self.call(move |db| {
            let handle = db
                .open_table(&table)
                .map_err(|err| AntflyError::Embedded(format!("open table {table}: {err}")))?;
            f(&handle)
        })
        .await
    }
}

/// Longest a local inference call waits out `MODEL_RESOURCE_BUSY`.
const BUSY_RETRY_BUDGET: Duration = Duration::from_secs(30);

/// Runs `call`, retrying while the runtime reports a retryable
/// `MODEL_RESOURCE_BUSY`: a model still loading, or another model holding
/// the inference memory. Waits as long as the error's `retry_after_ms` asks
/// (1 s otherwise), up to [`BUSY_RETRY_BUDGET`] in total. Runs on an executor
/// thread, so sleeping blocks only that thread.
fn retry_while_busy<T>(
    mut call: impl FnMut() -> Result<T, antfly_embedded::InferenceError>,
) -> Result<T, antfly_embedded::InferenceError> {
    let started = std::time::Instant::now();
    loop {
        let err = match call() {
            Ok(value) => return Ok(value),
            Err(err) => err,
        };
        let body: Value = serde_json::from_str(&err.body).unwrap_or(Value::Null);
        let busy = body.get("error").and_then(Value::as_str) == Some("MODEL_RESOURCE_BUSY")
            && body.get("retryable").and_then(Value::as_bool) != Some(false);
        let wait = Duration::from_millis(
            body.get("retry_after_ms")
                .and_then(Value::as_u64)
                .unwrap_or(1_000)
                .clamp(100, 5_000),
        );
        if !busy || started.elapsed() + wait > BUSY_RETRY_BUDGET {
            return Err(err);
        }
        std::thread::sleep(wait);
    }
}

/// Returns the cached inference handle, opening it on first use. Must run on
/// an executor thread: opening loads the runtime and needs a large stack.
fn inference_handle(
    slot: &Mutex<Option<Arc<Inference>>>,
    models_dir: Option<&Path>,
) -> AntflyResult<Arc<Inference>> {
    let mut slot = slot.lock().map_err(|_| AntflyError::ExecutorUnavailable)?;
    if let Some(inference) = slot.as_ref() {
        return Ok(Arc::clone(inference));
    }
    let mut options = InferenceOptions::new();
    if let Some(dir) = models_dir {
        options = options.models_dir(dir);
    }
    let inference = Arc::new(
        Inference::open(&options)
            .map_err(|err| AntflyError::Embedded(format!("open inference: {err}")))?,
    );
    *slot = Some(Arc::clone(&inference));
    Ok(inference)
}

fn embedded(context: &str) -> impl FnOnce(antfly_embedded::Error) -> AntflyError + '_ {
    move |err| AntflyError::Embedded(format!("{context}: {err}"))
}

#[derive(Deserialize)]
struct ScanResult {
    #[serde(default)]
    documents: Vec<ScanDocument>,
}

#[derive(Deserialize)]
struct ScanDocument {
    id_b64: String,
    json: String,
}

#[derive(Deserialize)]
struct NamedEntry {
    name: String,
}

fn names(listing: &[u8]) -> HashSet<String> {
    let Ok(value) = serde_json::from_slice::<Value>(listing) else {
        return HashSet::new();
    };
    let entries = match &value {
        Value::Array(entries) => entries.clone(),
        Value::Object(map) => map
            .values()
            .find_map(|value| value.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    entries
        .into_iter()
        .filter_map(|entry| match entry {
            Value::String(name) => Some(name),
            other => serde_json::from_value::<NamedEntry>(other)
                .ok()
                .map(|entry| entry.name),
        })
        .collect()
}

impl EmbeddedBackend {
    /// Adds a graph index when missing, dropping stale versions of it. Its
    /// inline artifact producers and resolvers register with the index.
    async fn ensure_graph(&self, table: &str, graph: GraphIndex) -> AntflyResult<()> {
        let marker = format!("graph:{table}:{}", graph.name);
        if self
            .known_indexes
            .lock()
            .map(|known| known.contains(&marker))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let name = graph.name.clone();
        let prefix = graph.replaces_prefix;
        let config = graph.config;
        let state = self
            .call_in(table.to_string(), move |db| {
                let indexes = names(&db.indexes_json().map_err(embedded("list indexes"))?);
                let mut stale: Vec<&String> = if prefix.is_empty() {
                    Vec::new()
                } else {
                    indexes
                        .iter()
                        .filter(|index| index.starts_with(prefix) && **index != name)
                        .collect()
                };
                stale.sort();
                if indexes.contains(&name) {
                    // Swapped (or added) already; older leftovers retire.
                    for old in stale {
                        retry_busy(|| db.delete_index(old))
                            .map_err(embedded("delete stale graph index"))?;
                    }
                    return Ok(GraphState::Ready);
                }
                // Replace the newest older version; any others are dropped
                // once the replacement has swapped in. A refused replacement
                // leaves the old version serving rather than failing setup.
                let old = stale.last().map(|old| old.as_str());
                let result = match old {
                    Some(_) => migrate_resolver_namespaces(db, &config)
                        .and_then(|()| reconcile_graph(db, &name, old, &config)),
                    None => reconcile_graph(db, &name, old, &config),
                };
                match (result, old) {
                    (Err(err), Some(old)) => Ok(GraphState::Refused {
                        old: old.to_string(),
                        error: err.to_string(),
                    }),
                    (result, _) => result,
                }
            })
            .await?;
        let serving_key = format!("{table}:{}", graph.name);
        match state {
            GraphState::Ready => {
                if let Ok(mut serving) = self.serving.lock() {
                    serving.insert(serving_key, graph.name.clone());
                }
                if let Ok(mut known) = self.known_indexes.lock() {
                    known.insert(marker);
                }
            }
            GraphState::Replacing { old } => {
                tracing::info!(
                    "antfly: graph index {} on {table} is building; {old} serves until it swaps in",
                    graph.name
                );
                if let Ok(mut serving) = self.serving.lock() {
                    serving.insert(serving_key, old);
                }
            }
            GraphState::Refused { old, error } => {
                tracing::warn!(
                    "antfly: could not replace graph index {old} on {table} with {}; {old} keeps serving: {error}",
                    graph.name
                );
                if let Ok(mut serving) = self.serving.lock() {
                    serving.insert(serving_key, old);
                }
            }
        }
        Ok(())
    }
}

impl Backend for EmbeddedBackend {
    fn write(&self, table: String, writes: Vec<Write>) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            if writes.is_empty() {
                return Ok(());
            }
            // Later writes to the same key win; one batch commits atomically.
            let mut inserts = serde_json::Map::new();
            let mut deletes = std::collections::BTreeSet::new();
            for write in writes {
                match write {
                    Write::Put { key, doc } => {
                        deletes.remove(&key);
                        inserts.insert(key, doc);
                    }
                    Write::Delete { key } => {
                        inserts.remove(&key);
                        deletes.insert(key);
                    }
                }
            }
            let request = serde_json::to_vec(&json!({
                "inserts": inserts,
                "deletes": deletes,
                "sync_level": "write",
            }))?;
            self.call_in(table, move |db| {
                db.batch_json(&request)
                    .map(|_| ())
                    .map_err(embedded("batch"))
            })
            .await
        })
    }

    fn get(&self, table: String, key: String) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            self.call_in(table, move |db| match db.lookup_json(key.as_bytes()) {
                Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                // Native errors carry owned detail; classify by code.
                Err(err) if err.kind() == antfly_embedded::Error::NotFound => Ok(None),
                Err(err) => Err(AntflyError::Embedded(format!("lookup: {err}"))),
            })
            .await
        })
    }

    fn scan(&self, table: String, request: ScanRequest) -> BackendFuture<'_, Vec<Document>> {
        Box::pin(async move {
            let body = json!({
                "from_key_b64": BASE64.encode(request.from.as_bytes()),
                "to_key_b64": BASE64.encode(request.to.as_bytes()),
                "inclusive_from": true,
                "exclusive_to": true,
                "include_documents": true,
                "limit": request.limit.unwrap_or(0),
            });
            let bytes = serde_json::to_vec(&body)?;
            let raw = self
                .call_in(table, move |db| {
                    db.scan_json(&bytes).map_err(embedded("scan"))
                })
                .await?;
            let result: ScanResult = serde_json::from_slice(&raw)?;
            let mut documents = Vec::with_capacity(result.documents.len());
            for document in result.documents {
                let key = BASE64
                    .decode(document.id_b64.as_bytes())
                    .map_err(|err| AntflyError::Malformed(format!("scan id: {err}")))?;
                let key = String::from_utf8(key)
                    .map_err(|err| AntflyError::Malformed(format!("scan id: {err}")))?;
                documents.push(Document {
                    key,
                    doc: serde_json::from_str(&document.json)?,
                });
            }
            Ok(documents)
        })
    }

    fn query(&self, table: String, request: Value) -> BackendFuture<'_, Value> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(&request)?;
            let raw = self
                .call_in(table, move |db| {
                    db.search_json(&bytes).map_err(embedded("search"))
                })
                .await?;
            Ok(serde_json::from_slice(&raw)?)
        })
    }

    fn ensure_table(&self, spec: TableSpec) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            let table = spec.name.clone();
            let marker = format!("table:{table}");
            let known = self
                .known_indexes
                .lock()
                .map(|known| known.contains(&marker))
                .unwrap_or(false);
            if !known {
                let schema = serde_json::to_vec(&spec.schema)?;
                let name = table.clone();
                self.call(move |db| {
                    let tables = names(&db.list_tables_json().map_err(embedded("list tables"))?);
                    if !tables.contains(&name) {
                        db.create_table_json(&name, &schema)
                            .map_err(embedded("create table"))?;
                    }
                    Ok(())
                })
                .await?;
                if let Ok(mut known) = self.known_indexes.lock() {
                    known.insert(marker);
                }
            }
            // Graphs serve search features; a graph that cannot be set up
            // must not stop the table's writes.
            for graph in spec.graphs {
                let name = graph.name.clone();
                if let Err(err) = self.ensure_graph(&table, graph).await {
                    tracing::warn!("antfly: graph index {name} on {table} is unavailable: {err}");
                }
            }
            let Some(dense) = spec.dense else {
                return Ok(());
            };
            let marker = format!("dense:{table}:{}", dense.name);
            if self
                .known_indexes
                .lock()
                .map(|known| known.contains(&marker))
                .unwrap_or(false)
            {
                return Ok(());
            }
            let mut embedder = json!({"provider": "antfly", "model": dense.model});
            let mut config = json!({
                "type": "embeddings",
                // Without a field the index reads `embedding` and never
                // sees the enriched text.
                "field": dense.field,
                "metric": "cosine",
            });
            // Without dims Antfly probes the embedder for its native size.
            if let Some(dims) = dense.dims {
                embedder["dimensions"] = json!(dims);
                config["dims"] = json!(dims);
            }
            config["embedder"] = embedder.clone();
            let index = json!({
                "name": dense.name,
                "kind": "dense_vector",
                "config_json": config.to_string(),
            });
            let enrichment_name = format!("{}_embedder", dense.name);
            let enrichment = json!({
                "name": enrichment_name,
                "kind": "embedding",
                "field": dense.field,
                "vector_space": dense.name,
                "producer_json": json!({"type": "embedder", "config": embedder}).to_string(),
            });
            let index_name = dense.name.clone();
            self.call_in(table, move |db| {
                let indexes = names(&db.indexes_json().map_err(embedded("list indexes"))?);
                // A dense index left by a previously configured embedder
                // would keep embedding every write; drop it and its producer.
                for stale in indexes.iter().filter(|name| {
                    name.starts_with(crate::runtime::DENSE_INDEX_PREFIX) && **name != index_name
                }) {
                    db.delete_enrichment("embedding", &format!("{stale}_embedder"))
                        .map_err(embedded("delete stale enrichment"))?;
                    db.delete_index(stale)
                        .map_err(embedded("delete stale index"))?;
                }
                if !indexes.contains(&index_name) {
                    db.add_index_json(serde_json::to_vec(&index)?)
                        .map_err(embedded("add index"))?;
                }
                let enrichments = names(
                    &db.enrichments_json()
                        .map_err(embedded("list enrichments"))?,
                );
                if !enrichments.contains(&enrichment_name) {
                    db.add_enrichment_json(serde_json::to_vec(&enrichment)?)
                        .map_err(embedded("add enrichment"))?;
                }
                Ok(())
            })
            .await?;
            if let Ok(mut known) = self.known_indexes.lock() {
                known.insert(marker);
            }
            Ok(())
        })
    }

    fn serving_graph(&self, table: String, name: String) -> BackendFuture<'_, String> {
        Box::pin(async move {
            let key = format!("{table}:{name}");
            let current = self
                .serving
                .lock()
                .ok()
                .and_then(|serving| serving.get(&key).cloned());
            let Some(old) = current.filter(|serving| *serving != name) else {
                return Ok(name);
            };
            // A background replacement may have swapped in since open.
            let wanted = name.clone();
            let swapped = self
                .call_in(table, move |db| {
                    Ok(names(&db.indexes_json().map_err(embedded("list indexes"))?)
                        .contains(&wanted))
                })
                .await?;
            if !swapped {
                return Ok(old);
            }
            if let Ok(mut serving) = self.serving.lock() {
                serving.insert(key, name.clone());
            }
            Ok(name)
        })
    }

    fn resolve_query(
        &self,
        table: String,
        resolver: String,
        request: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(&request)?;
            let raw = self
                .call_in(table, move |db| {
                    // Busy means a concurrent config change; the whole query
                    // is retried, as Antfly's contract asks.
                    retry_busy(|| db.resolve_query_json(&resolver, &bytes))
                        .map_err(embedded("resolve query"))
                })
                .await?;
            Ok(Some(serde_json::from_slice(&raw)?))
        })
    }

    fn merge_resolver_keys(
        &self,
        table: String,
        resolver: String,
        from: String,
        to: String,
    ) -> BackendFuture<'_, bool> {
        Box::pin(async move {
            let request = serde_json::to_vec(&json!({"key": from, "target": to}))?;
            self.call_in(table, move |db| {
                retry_busy(|| db.merge_resolver_keys_json(&resolver, &request))
                    .map_err(embedded("merge resolver keys"))
            })
            .await?;
            Ok(true)
        })
    }

    fn render_resolver_key(
        &self,
        table: String,
        resolver: String,
        mention: Value,
    ) -> BackendFuture<'_, Option<String>> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(&mention)?;
            let raw = self
                .call_in(table, move |db| {
                    retry_busy(|| db.render_resolver_key_json(&resolver, &bytes))
                        .map_err(embedded("render resolver key"))
                })
                .await?;
            let rendered: Value = serde_json::from_slice(&raw)?;
            Ok(rendered
                .get("key")
                .and_then(Value::as_str)
                .map(str::to_string))
        })
    }

    fn embedded_database(&self) -> Option<Arc<Database>> {
        self.db.as_ref().map(Arc::clone)
    }

    fn close(&self) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            let Some(db) = self.db.as_ref().map(Arc::clone) else {
                return Ok(());
            };
            // Waits for in-flight calls and background work, on a thread
            // with libantfly's required stack.
            self.executor
                .run(move || db.close().map_err(embedded("close")))
                .await
        })
    }
}

/// Typed decisions answered by the embedded inference runtime in this
/// process. Decisions stay local even when storage is remote.
pub struct LocalDecider {
    /// Shared with in-flight jobs; the handle inside is closed on an
    /// executor thread.
    slot: Arc<Mutex<Option<Arc<Inference>>>>,
    models_dir: Option<PathBuf>,
    executor: Executor,
}

impl LocalDecider {
    pub fn new(models_dir: Option<PathBuf>) -> AntflyResult<Self> {
        Ok(Self {
            slot: Arc::new(Mutex::new(None)),
            models_dir,
            executor: Executor::new("antfly-decide")?,
        })
    }

    /// Loads the inference runtime ahead of the first decision.
    pub async fn warm(&self) -> AntflyResult<()> {
        let slot = Arc::clone(&self.slot);
        let models_dir = self.models_dir.clone();
        self.executor
            .run(move || inference_handle(&slot, models_dir.as_deref()).map(|_| ()))
            .await
    }

    /// Closes the inference runtime if it was loaded.
    pub async fn close(&self) -> AntflyResult<()> {
        let inference = self.slot.lock().ok().and_then(|slot| slot.clone());
        let Some(inference) = inference else {
            return Ok(());
        };
        self.executor
            .run(move || {
                inference
                    .close()
                    .map_err(|err| AntflyError::Embedded(format!("close inference: {err}")))
            })
            .await
    }

    /// Answers an `ExtractRequest` (GLiNER entities, relations,
    /// classifications).
    pub async fn extract(&self, request: &Value) -> AntflyResult<Value> {
        let slot = Arc::clone(&self.slot);
        let models_dir = self.models_dir.clone();
        let bytes = serde_json::to_vec(request)?;
        let raw = self
            .executor
            .run(move || {
                let inference = inference_handle(&slot, models_dir.as_deref())?;
                retry_while_busy(|| inference.extract(&bytes))
                    .map_err(|err| AntflyError::Embedded(format!("extract: {err}")))
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Answers a `GenerateRequest` (chat messages to a local generator).
    pub async fn generate(&self, request: &Value) -> AntflyResult<Value> {
        let slot = Arc::clone(&self.slot);
        let models_dir = self.models_dir.clone();
        let bytes = serde_json::to_vec(request)?;
        let raw = self
            .executor
            .run(move || {
                let inference = inference_handle(&slot, models_dir.as_deref())?;
                retry_while_busy(|| inference.generate(&bytes))
                    .map_err(|err| AntflyError::Embedded(format!("generate: {err}")))
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Answers a `DecideRequest`.
    pub async fn decide(&self, request: &Value) -> AntflyResult<Value> {
        let slot = Arc::clone(&self.slot);
        let models_dir = self.models_dir.clone();
        let bytes = serde_json::to_vec(request)?;
        let raw = self
            .executor
            .run(move || {
                let inference = inference_handle(&slot, models_dir.as_deref())?;
                retry_while_busy(|| inference.decide(&bytes))
                    .map_err(|err| AntflyError::Embedded(format!("decide: {err}")))
            })
            .await?;
        Ok(serde_json::from_slice(&raw)?)
    }
}

impl Drop for EmbeddedBackend {
    fn drop(&mut self) {
        // Closing the database calls into libantfly, which needs a large
        // stack; hand the last reference to an executor thread.
        let Some(db) = self.db.take() else {
            return;
        };
        if let Ok(sender) = self.executor.sender.lock() {
            let _ = sender.send(Box::new(move || drop(db)));
        }
    }
}

impl Drop for LocalDecider {
    fn drop(&mut self) {
        let inference = self.slot.lock().ok().and_then(|mut slot| slot.take());
        let Some(inference) = inference else {
            return;
        };
        if let Ok(sender) = self.executor.sender.lock() {
            let _ = sender.send(Box::new(move || drop(inference)));
        }
    }
}
