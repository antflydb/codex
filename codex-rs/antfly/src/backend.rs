use futures::future::BoxFuture;
use serde_json::Value;

use crate::error::AntflyResult;

pub type BackendFuture<'a, T> = BoxFuture<'a, AntflyResult<T>>;

/// One mutation in an atomic write.
#[derive(Clone, Debug, PartialEq)]
pub enum Write {
    Put { key: String, doc: Value },
    Delete { key: String },
}

impl Write {
    pub fn put(key: impl Into<String>, doc: Value) -> Self {
        Write::Put {
            key: key.into(),
            doc,
        }
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Write::Delete { key: key.into() }
    }
}

/// Half-open key range `[from, to)`, returned in ascending key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRequest {
    pub from: String,
    pub to: String,
    pub limit: Option<usize>,
}

impl ScanRequest {
    /// Every key that starts with `prefix`.
    pub fn prefix(prefix: &str) -> Self {
        Self {
            from: prefix.to_string(),
            to: crate::keys::prefix_end(prefix),
            limit: None,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub key: String,
    pub doc: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    pub key: String,
    pub score: f64,
    pub doc: Option<Value>,
}

/// Storage operations shared by the embedded and remote backends.
///
/// `write` is atomic: either every mutation in the call is applied or none
/// is. Callers that read, modify, and write must serialize those sequences
/// themselves (see [`crate::Antfly::lock`]).
pub trait Backend: Send + Sync {
    fn write(&self, writes: Vec<Write>) -> BackendFuture<'_, ()>;

    fn get(&self, key: String) -> BackendFuture<'_, Option<Value>>;

    fn scan(&self, request: ScanRequest) -> BackendFuture<'_, Vec<Document>>;

    /// Executes an Antfly `QueryRequest` and returns the merged hits.
    fn search(&self, request: Value) -> BackendFuture<'_, Vec<SearchHit>>;

    /// Creates the table, indexes, and enrichments in `schema` when missing.
    fn ensure_schema(&self, schema: SchemaSpec) -> BackendFuture<'_, ()>;
}

/// Dense semantic index over one text field, embedded by Antfly inference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DenseIndex {
    pub name: String,
    pub field: String,
    pub model: String,
    pub dims: u32,
}

/// Backend-neutral description of the indexes Codex needs. Full-text search
/// over every field is always available.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SchemaSpec {
    pub dense: Option<DenseIndex>,
}

/// Extracts hits from a `QueryResponses` or single `QueryResult` body.
pub(crate) fn parse_search_hits(body: &Value) -> Vec<SearchHit> {
    let results: Vec<&Value> = match body.get("responses").and_then(Value::as_array) {
        Some(responses) => responses.iter().collect(),
        None => vec![body],
    };
    let mut hits = Vec::new();
    for result in results {
        let Some(list) = result
            .get("hits")
            .and_then(|hits| hits.get("hits"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for hit in list {
            let Some(key) = hit.get("_id").and_then(Value::as_str) else {
                continue;
            };
            hits.push(SearchHit {
                key: key.to_string(),
                score: hit.get("_score").and_then(Value::as_f64).unwrap_or(0.0),
                doc: hit.get("_source").cloned(),
            });
        }
    }
    hits
}
