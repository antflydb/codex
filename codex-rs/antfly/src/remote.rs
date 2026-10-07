use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use reqwest::StatusCode;
use serde_json::Value;
use serde_json::json;

use crate::backend::Backend;
use crate::backend::BackendFuture;
use crate::backend::Document;
use crate::backend::ScanRequest;
use crate::backend::SchemaSpec;
use crate::backend::SearchHit;
use crate::backend::Write;
use crate::backend::parse_search_hits;
use crate::error::AntflyError;
use crate::error::AntflyResult;

/// A remote Antfly server or Antfly Cloud instance.
///
/// All Codex state lives in one table. `write` uses the table batch endpoint,
/// which applies a request's inserts and deletes together for a single-range
/// table.
pub struct RemoteBackend {
    client: reqwest::Client,
    base_url: String,
    table: String,
    api_key: Option<String>,
    table_ready: AtomicBool,
}

impl RemoteBackend {
    pub fn new(base_url: &str, table: &str, api_key: Option<String>) -> AntflyResult<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|err| AntflyError::Config(format!("http client: {err}")))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            table: table.to_string(),
            api_key,
            table_ready: AtomicBool::new(false),
        })
    }

    fn table_url(&self, suffix: &str) -> String {
        format!(
            "{}/db/v1/tables/{}{suffix}",
            self.base_url,
            encode_path_segment(&self.table)
        )
    }

    fn request(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        let builder = self.client.request(method, url);
        match &self.api_key {
            Some(key) => builder.bearer_auth(key),
            None => builder,
        }
    }

    async fn send(&self, builder: reqwest::RequestBuilder) -> AntflyResult<reqwest::Response> {
        builder
            .send()
            .await
            .map_err(|err| AntflyError::Remote(err.to_string()))
    }

    async fn checked(response: reqwest::Response) -> AntflyResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        Err(AntflyError::Remote(format!("{status}: {body}")))
    }

    async fn ensure_table(&self) -> AntflyResult<()> {
        if self.table_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let response = self
            .send(self.request(reqwest::Method::GET, self.table_url("")))
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            let created = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url(""))
                        .json(&json!({})),
                )
                .await?;
            // A concurrent creator may win; that is fine.
            if created.status() != StatusCode::CONFLICT {
                Self::checked(created).await?;
            }
        } else {
            Self::checked(response).await?;
        }
        self.table_ready.store(true, Ordering::Release);
        Ok(())
    }
}

/// Percent-encodes one URL path segment.
fn encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

impl Backend for RemoteBackend {
    fn write(&self, writes: Vec<Write>) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            if writes.is_empty() {
                return Ok(());
            }
            self.ensure_table().await?;
            // Later writes to the same key win, matching embedded batches.
            let mut inserts = BTreeMap::new();
            let mut deletes = BTreeMap::new();
            for write in writes {
                match write {
                    Write::Put { key, doc } => {
                        deletes.remove(&key);
                        inserts.insert(key, doc);
                    }
                    Write::Delete { key } => {
                        inserts.remove(&key);
                        deletes.insert(key, ());
                    }
                }
            }
            let body = json!({
                "inserts": inserts,
                "deletes": deletes.into_keys().collect::<Vec<_>>(),
                "sync_level": "write",
            });
            let response = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url("/batch"))
                        .json(&body),
                )
                .await?;
            Self::checked(response).await?;
            Ok(())
        })
    }

    fn get(&self, key: String) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            self.ensure_table().await?;
            let url = self.table_url(&format!("/documents/{}", encode_path_segment(&key)));
            let response = self.send(self.request(reqwest::Method::GET, url)).await?;
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let response = Self::checked(response).await?;
            let doc = response
                .json::<Value>()
                .await
                .map_err(|err| AntflyError::Malformed(err.to_string()))?;
            Ok(Some(doc))
        })
    }

    fn scan(&self, request: ScanRequest) -> BackendFuture<'_, Vec<Document>> {
        Box::pin(async move {
            self.ensure_table().await?;
            let body = json!({
                "from": request.from,
                "to": request.to,
                "inclusive_from": true,
                "exclusive_to": true,
                "fields": ["*"],
            });
            let response = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url("/documents"))
                        .json(&body),
                )
                .await?;
            let text = Self::checked(response)
                .await?
                .text()
                .await
                .map_err(|err| AntflyError::Remote(err.to_string()))?;
            let mut documents = Vec::new();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let mut doc: Value = serde_json::from_str(line)?;
                let key = match doc.as_object_mut().and_then(|map| map.remove("_id")) {
                    Some(Value::String(key)) => key,
                    _ => return Err(AntflyError::Malformed("scan line without _id".into())),
                };
                documents.push(Document { key, doc });
                if request.limit.is_some_and(|limit| documents.len() >= limit) {
                    break;
                }
            }
            Ok(documents)
        })
    }

    fn search(&self, request: Value) -> BackendFuture<'_, Vec<SearchHit>> {
        Box::pin(async move {
            self.ensure_table().await?;
            let response = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url("/query"))
                        .json(&request),
                )
                .await?;
            let body = Self::checked(response)
                .await?
                .json::<Value>()
                .await
                .map_err(|err| AntflyError::Malformed(err.to_string()))?;
            Ok(parse_search_hits(&body))
        })
    }

    fn ensure_schema(&self, schema: SchemaSpec) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_table().await?;
            let Some(dense) = schema.dense else {
                return Ok(());
            };
            let url = self.table_url(&format!("/indexes/{}", encode_path_segment(&dense.name)));
            let existing = self
                .send(self.request(reqwest::Method::GET, url.clone()))
                .await?;
            if existing.status().is_success() {
                return Ok(());
            }
            let body = json!({
                "type": "embeddings",
                "field": dense.field,
                "dimension": dense.dims,
                "embedder": {"provider": "antfly", "model": dense.model},
            });
            let response = self
                .send(self.request(reqwest::Method::POST, url).json(&body))
                .await?;
            if response.status() != StatusCode::CONFLICT {
                Self::checked(response).await?;
            }
            Ok(())
        })
    }
}
