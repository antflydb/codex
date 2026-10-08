//! A local embedded database replicated to a remote Antfly instance.
//!
//! Every write is applied locally together with an outbox entry in the same
//! atomic batch. A background task drains the outbox to the remote in order,
//! deleting entries once the remote accepts them, so the local copy keeps
//! working offline and the remote catches up when it is reachable. Reads and
//! searches are local unless `search_remote` is set, in which case searches
//! go to the remote replica (which may hold other machines' data).

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Notify;

use crate::backend::Backend;
use crate::backend::BackendFuture;
use crate::backend::Document;
use crate::backend::ScanRequest;
use crate::backend::SchemaSpec;
use crate::backend::SearchHit;
use crate::backend::Write;
use crate::error::AntflyResult;
use crate::keys;

pub(crate) const OUTBOX_PREFIX: &str = "ob:";
/// Outbox entries shipped per remote batch.
const DRAIN_BATCH: usize = 64;
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// A write as stored in the outbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum OutboxWrite {
    Put { key: String, doc: Value },
    Delete { key: String },
}

impl From<&Write> for OutboxWrite {
    fn from(write: &Write) -> Self {
        match write {
            Write::Put { key, doc } => OutboxWrite::Put {
                key: key.clone(),
                doc: doc.clone(),
            },
            Write::Delete { key } => OutboxWrite::Delete { key: key.clone() },
        }
    }
}

impl From<OutboxWrite> for Write {
    fn from(write: OutboxWrite) -> Self {
        match write {
            OutboxWrite::Put { key, doc } => Write::Put { key, doc },
            OutboxWrite::Delete { key } => Write::Delete { key },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct OutboxEntry {
    writes: Vec<OutboxWrite>,
}

struct Shared {
    local: Arc<dyn Backend>,
    remote: Arc<dyn Backend>,
    pending_schema: std::sync::Mutex<Option<SchemaSpec>>,
}

/// Local-first backend that replicates writes to a remote backend.
pub struct ReplicatedBackend {
    shared: Arc<Shared>,
    /// Wakes the drainer; held separately so the drainer can wait without
    /// keeping the backend alive.
    notify: Arc<Notify>,
    search_remote: bool,
    sequence: AtomicU64,
    drainer: OnceLock<()>,
}

impl ReplicatedBackend {
    pub fn new(local: Arc<dyn Backend>, remote: Arc<dyn Backend>, search_remote: bool) -> Self {
        let start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0);
        Self {
            shared: Arc::new(Shared {
                local,
                remote,
                pending_schema: std::sync::Mutex::new(None),
            }),
            notify: Arc::new(Notify::new()),
            search_remote,
            sequence: AtomicU64::new(start),
            drainer: OnceLock::new(),
        }
    }

    /// Outbox key ordered by write sequence. Sequences start at the open
    /// time in nanoseconds so they keep increasing across restarts.
    fn next_outbox_key(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{OUTBOX_PREFIX}{}", keys::ordinal(sequence))
    }

    /// Starts the drainer on first use, from inside the caller's runtime.
    fn ensure_drainer(&self) {
        self.drainer.get_or_init(|| {
            let shared = Arc::downgrade(&self.shared);
            let notify = Arc::clone(&self.notify);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(drain_forever(shared, notify));
            }
        });
        self.notify.notify_one();
    }

    /// Ships every queued write to the remote now. For tests and shutdown.
    pub async fn flush(&self) -> AntflyResult<usize> {
        drain_once(&self.shared).await
    }
}

/// Ships outbox entries in order until the outbox is empty or the remote
/// fails. Returns how many entries were shipped.
async fn drain_once(shared: &Shared) -> AntflyResult<usize> {
    if let Some(schema) = shared
        .pending_schema
        .lock()
        .ok()
        .and_then(|mut pending| pending.take())
        && let Err(err) = shared.remote.ensure_schema(schema.clone()).await
    {
        if let Ok(mut pending) = shared.pending_schema.lock() {
            *pending = Some(schema);
        }
        return Err(err);
    }
    let mut shipped = 0;
    loop {
        let entries = shared
            .local
            .scan(ScanRequest::prefix(OUTBOX_PREFIX).with_limit(DRAIN_BATCH))
            .await?;
        if entries.is_empty() {
            return Ok(shipped);
        }
        for entry in entries {
            let outbox: OutboxEntry = serde_json::from_value(entry.doc)?;
            let writes: Vec<Write> = outbox.writes.into_iter().map(Write::from).collect();
            shared.remote.write(writes).await?;
            shared.local.write(vec![Write::delete(entry.key)]).await?;
            shipped += 1;
        }
    }
}

async fn drain_forever(shared: std::sync::Weak<Shared>, notify: Arc<Notify>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let Some(strong) = shared.upgrade() else {
            return;
        };
        match drain_once(&strong).await {
            Ok(_) => {
                backoff = Duration::from_secs(1);
                // Wait for new writes, rechecking periodically.
                drop(strong);
                let _ = tokio::time::timeout(MAX_BACKOFF, notify.notified()).await;
            }
            Err(err) => {
                tracing::warn!("antfly replication paused, retrying in {backoff:?}: {err}");
                drop(strong);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

impl Backend for ReplicatedBackend {
    fn write(&self, writes: Vec<Write>) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            if writes.is_empty() {
                return Ok(());
            }
            let entry = OutboxEntry {
                writes: writes.iter().map(OutboxWrite::from).collect(),
            };
            let mut local = writes;
            local.push(Write::put(
                self.next_outbox_key(),
                serde_json::to_value(&entry)?,
            ));
            self.shared.local.write(local).await?;
            self.ensure_drainer();
            Ok(())
        })
    }

    fn get(&self, key: String) -> BackendFuture<'_, Option<Value>> {
        self.shared.local.get(key)
    }

    fn scan(&self, request: ScanRequest) -> BackendFuture<'_, Vec<Document>> {
        self.shared.local.scan(request)
    }

    fn search(&self, request: Value) -> BackendFuture<'_, Vec<SearchHit>> {
        if self.search_remote {
            let shared = Arc::clone(&self.shared);
            Box::pin(async move {
                match shared.remote.search(request.clone()).await {
                    Ok(hits) => Ok(hits),
                    Err(err) => {
                        tracing::warn!("remote search failed, searching locally: {err}");
                        shared.local.search(request).await
                    }
                }
            })
        } else {
            self.shared.local.search(request)
        }
    }

    fn ensure_schema(&self, schema: SchemaSpec) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            self.shared.local.ensure_schema(schema.clone()).await?;
            // The remote may be unreachable; set it up when draining.
            if let Ok(mut pending) = self.shared.pending_schema.lock() {
                *pending = Some(schema);
            }
            self.ensure_drainer();
            Ok(())
        })
    }

    fn close(&self) -> BackendFuture<'_, ()> {
        Box::pin(self.close_backends())
    }
}

impl ReplicatedBackend {
    /// Queued writes stay in the local outbox and ship after the next open.
    async fn close_backends(&self) -> AntflyResult<()> {
        self.notify.notify_one();
        let local = self.shared.local.close().await;
        self.shared.remote.close().await?;
        local
    }
}

impl Drop for ReplicatedBackend {
    fn drop(&mut self) {
        // Wake the drainer so it notices the handle is gone.
        self.notify.notify_one();
    }
}
