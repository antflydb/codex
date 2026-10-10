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
use crate::backend::LEGACY_TABLE;
use crate::backend::ScanRequest;
use crate::backend::SearchHit;
use crate::backend::TableSpec;
use crate::backend::Write;
use crate::backend::parse_search_hits;
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
}

impl EmbeddedBackend {
    /// Opens `path`, creating it when missing. Blocks; call during startup.
    pub fn open(path: &Path) -> AntflyResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| AntflyError::Embedded(format!("create {parent:?}: {err}")))?;
        }
        let executor = Executor::new("antfly")?;
        let path = path.to_path_buf();
        // Opening also needs a large stack, so do it on an executor thread.
        let (tx, rx) = mpsc::channel();
        let open_path = path.clone();
        std::thread::Builder::new()
            .name("antfly-open".to_string())
            .stack_size(MIN_THREAD_STACK_SIZE)
            .spawn(move || {
                let options = OpenOptions::new()
                    .local_runtime_configured(true)
                    .busy_timeout(BUSY_TIMEOUT);
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

    /// Runs `f` against `table`'s handle (the root table for
    /// [`LEGACY_TABLE`]) on an executor thread.
    async fn call_in<T, F>(&self, table: String, f: F) -> AntflyResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> AntflyResult<T> + Send + 'static,
    {
        self.call(move |db| {
            if table == LEGACY_TABLE {
                return f(db);
            }
            let handle = db
                .open_table(&table)
                .map_err(|err| AntflyError::Embedded(format!("open table {table}: {err}")))?;
            f(&handle)
        })
        .await
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
                Err(antfly_embedded::Error::NotFound) => Ok(None),
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

    fn search(&self, table: String, request: Value) -> BackendFuture<'_, Vec<SearchHit>> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(&request)?;
            let raw = self
                .call_in(table, move |db| {
                    db.search_json(&bytes).map_err(embedded("search"))
                })
                .await?;
            let body: Value = serde_json::from_slice(&raw)?;
            Ok(parse_search_hits(&body))
        })
    }

    fn ensure_table(&self, spec: TableSpec) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            let table = spec.name.clone();
            if table != LEGACY_TABLE {
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
                        let tables =
                            names(&db.list_tables_json().map_err(embedded("list tables"))?);
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
            let embedder = json!({"provider": "antfly", "model": dense.model});
            let index = json!({
                "name": dense.name,
                "kind": "dense_vector",
                "config_json": json!({
                    "type": "embeddings",
                    // Without a field the index reads `embedding` and never
                    // sees the enriched text.
                    "field": dense.field,
                    "dims": dense.dims,
                    "metric": "cosine",
                    "embedder": embedder,
                })
                .to_string(),
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

    /// Answers a `DecideRequest`.
    pub async fn decide(&self, request: &Value) -> AntflyResult<Value> {
        let slot = Arc::clone(&self.slot);
        let models_dir = self.models_dir.clone();
        let bytes = serde_json::to_vec(request)?;
        let raw = self
            .executor
            .run(move || {
                let inference = inference_handle(&slot, models_dir.as_deref())?;
                inference
                    .decide(&bytes)
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
