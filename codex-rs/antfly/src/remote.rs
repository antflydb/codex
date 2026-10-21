use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Mutex;

use reqwest::StatusCode;
use serde_json::Value;
use serde_json::json;

use crate::backend::Backend;
use crate::backend::BackendFuture;
use crate::backend::Document;
use crate::backend::GraphIndex;
use crate::backend::ScanRequest;
use crate::backend::TableSpec;
use crate::backend::Write;
use crate::error::AntflyError;
use crate::error::AntflyResult;

/// A remote Antfly server or Antfly Cloud instance.
///
/// Document tables are addressed by name and created by
/// [`Backend::ensure_table`]. `write` uses the table batch endpoint, which
/// applies a request's inserts and deletes together for a single-range
/// table.
pub struct RemoteBackend {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    /// Tables known to exist.
    ready_tables: Mutex<HashSet<String>>,
}

impl RemoteBackend {
    pub fn new(base_url: &str, api_key: Option<String>) -> AntflyResult<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|err| AntflyError::Config(format!("http client: {err}")))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            ready_tables: Mutex::new(HashSet::new()),
        })
    }

    fn table_url(&self, table: &str, suffix: &str) -> String {
        format!(
            "{}/db/v1/tables/{}{suffix}",
            self.base_url,
            encode_path_segment(table)
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

    /// Sends a JSON request (`body` for POST), retrying while the server
    /// reports the resolver owner busy (429) or unavailable (503), as
    /// Antfly's curation contract asks. Returns the parsed body; `None` on 404.
    async fn json_call(
        &self,
        method: reqwest::Method,
        url: String,
        body: Option<&Value>,
    ) -> AntflyResult<Option<Value>> {
        let mut delay = RETRY_INITIAL_DELAY;
        let mut attempt = 0;
        loop {
            let mut builder = self.request(method.clone(), url.clone());
            if let Some(body) = body {
                builder = builder.json(body);
            }
            let response = self.send(builder).await?;
            let status = response.status();
            if matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            ) && attempt < RETRY_ATTEMPTS
            {
                attempt += 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX_DELAY);
                continue;
            }
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            return Self::checked(response)
                .await?
                .json::<Value>()
                .await
                .map(Some)
                .map_err(|err| AntflyError::Malformed(err.to_string()));
        }
    }

    fn resolver_url(&self, table: &str, resolver: &str, suffix: &str) -> String {
        self.table_url(
            table,
            &format!("/resolvers/{}{suffix}", encode_path_segment(resolver)),
        )
    }

    async fn checked(response: reqwest::Response) -> AntflyResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        Err(AntflyError::Remote(format!("{status}: {body}")))
    }

    async fn ensure_table_exists(&self, table: &str, schema: &Value) -> AntflyResult<()> {
        if self
            .ready_tables
            .lock()
            .map(|ready| ready.contains(table))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let response = self
            .send(self.request(reqwest::Method::GET, self.table_url(table, "")))
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            let body = json!({ "schema": schema });
            let created = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url(table, ""))
                        .json(&body),
                )
                .await?;
            // A concurrent creator may win; that is fine.
            if created.status() != StatusCode::CONFLICT {
                Self::checked(created).await?;
            }
        } else {
            Self::checked(response).await?;
        }
        if let Ok(mut ready) = self.ready_tables.lock() {
            ready.insert(table.to_string());
        }
        Ok(())
    }

    /// Creates a graph index when missing. Stale versions are left for the
    /// operator: dropping server indexes is not this client's call.
    async fn ensure_graph(&self, table: &str, graph: &GraphIndex) -> AntflyResult<()> {
        let url = self.table_url(
            table,
            &format!("/indexes/{}", encode_path_segment(&graph.name)),
        );
        let existing = self
            .send(self.request(reqwest::Method::GET, url.clone()))
            .await?;
        if existing.status().is_success() {
            return Ok(());
        }
        let mut body = graph.config.clone();
        body["type"] = json!("graph");
        let response = self
            .send(self.request(reqwest::Method::POST, url).json(&body))
            .await?;
        if response.status() != StatusCode::CONFLICT {
            Self::checked(response).await?;
        }
        Ok(())
    }
}

const RETRY_ATTEMPTS: u32 = 8;
const RETRY_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const RETRY_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

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
    fn write(&self, table: String, writes: Vec<Write>) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            if writes.is_empty() {
                return Ok(());
            }
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
                    self.request(reqwest::Method::POST, self.table_url(&table, "/batch"))
                        .json(&body),
                )
                .await?;
            Self::checked(response).await?;
            Ok(())
        })
    }

    fn get(&self, table: String, key: String) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            let url = self.table_url(&table, &format!("/documents/{}", encode_path_segment(&key)));
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

    fn scan(&self, table: String, request: ScanRequest) -> BackendFuture<'_, Vec<Document>> {
        Box::pin(async move {
            let body = json!({
                "from": request.from,
                "to": request.to,
                "inclusive_from": true,
                "exclusive_to": true,
                "fields": ["*"],
            });
            let response = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url(&table, "/documents"))
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

    fn query(&self, table: String, request: Value) -> BackendFuture<'_, Value> {
        Box::pin(async move {
            let response = self
                .send(
                    self.request(reqwest::Method::POST, self.table_url(&table, "/query"))
                        .json(&request),
                )
                .await?;
            Self::checked(response)
                .await?
                .json::<Value>()
                .await
                .map_err(|err| AntflyError::Malformed(err.to_string()))
        })
    }

    fn ensure_table(&self, spec: TableSpec) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            let table = spec.name.clone();
            self.ensure_table_exists(&table, &spec.schema).await?;
            for graph in &spec.graphs {
                self.ensure_graph(&table, graph).await?;
            }
            let Some(dense) = spec.dense else {
                return Ok(());
            };
            let url = self.table_url(
                &table,
                &format!("/indexes/{}", encode_path_segment(&dense.name)),
            );
            let existing = self
                .send(self.request(reqwest::Method::GET, url.clone()))
                .await?;
            if existing.status().is_success() {
                return Ok(());
            }
            let mut body = json!({
                "type": "embeddings",
                "field": dense.field,
                "embedder": {"provider": "antfly", "model": dense.model},
            });
            // Without a dimension the server probes the embedder.
            if let Some(dims) = dense.dims {
                body["dimension"] = json!(dims);
                body["embedder"]["dimensions"] = json!(dims);
            }
            let response = self
                .send(self.request(reqwest::Method::POST, url).json(&body))
                .await?;
            if response.status() != StatusCode::CONFLICT {
                Self::checked(response).await?;
            }
            Ok(())
        })
    }

    fn resolve_query(
        &self,
        table: String,
        resolver: String,
        request: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            self.json_call(
                reqwest::Method::POST,
                self.resolver_url(&table, &resolver, ":resolve"),
                Some(&request),
            )
            .await
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
            let request = json!({"from": from, "to": to});
            let merged = self
                .json_call(
                    reqwest::Method::POST,
                    self.resolver_url(&table, &resolver, "/redirects"),
                    Some(&request),
                )
                .await?;
            Ok(merged.is_some())
        })
    }

    fn curate_entity(
        &self,
        entity_table: String,
        key: String,
        request: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            let mut body = request;
            let action = body
                .as_object_mut()
                .and_then(|fields| fields.remove("action"))
                .and_then(|action| action.as_str().map(str::to_string))
                .ok_or_else(|| AntflyError::Config("curation request needs an action".into()))?;
            let entity = format!("/entities/{}", encode_path_segment(&key));
            let (method, url, body) = match action.as_str() {
                "redirects" => (
                    reqwest::Method::GET,
                    self.table_url(&entity_table, &format!("{entity}/redirects")),
                    None,
                ),
                "merge" | "unmerge" | "suppress" | "unsuppress" => (
                    reqwest::Method::POST,
                    self.table_url(&entity_table, &format!("{entity}:{action}")),
                    Some(body),
                ),
                other => {
                    return Err(AntflyError::Config(format!(
                        "unknown entity curation action `{other}`"
                    )));
                }
            };
            self.json_call(method, url, body.as_ref()).await
        })
    }

    fn propose_curation(
        &self,
        entity_table: String,
        request: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            self.json_call(
                reqwest::Method::POST,
                self.table_url(&entity_table, "/curation/proposals"),
                Some(&request),
            )
            .await
        })
    }

    fn list_curation_proposals(
        &self,
        entity_table: String,
        request: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            let query: Vec<String> = ["state", "action", "cursor", "limit"]
                .iter()
                .filter_map(|name| {
                    let value = request.get(*name)?;
                    let value = value
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| value.to_string());
                    Some(format!("{name}={}", encode_path_segment(&value)))
                })
                .collect();
            let suffix = if query.is_empty() {
                "/curation/proposals".to_string()
            } else {
                format!("/curation/proposals?{}", query.join("&"))
            };
            self.json_call(
                reqwest::Method::GET,
                self.table_url(&entity_table, &suffix),
                None,
            )
            .await
        })
    }

    fn decide_curation_proposal(
        &self,
        entity_table: String,
        id: String,
        request: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            let mut body = request;
            let accept = body
                .as_object_mut()
                .and_then(|fields| fields.remove("accept"))
                .and_then(|accept| accept.as_bool())
                .ok_or_else(|| AntflyError::Config("proposal decision needs `accept`".into()))?;
            let verb = if accept { "accept" } else { "reject" };
            self.json_call(
                reqwest::Method::POST,
                self.table_url(
                    &entity_table,
                    &format!("/curation/proposals/{}:{verb}", encode_path_segment(&id)),
                ),
                Some(&body),
            )
            .await
        })
    }

    fn resolver_curation(
        &self,
        table: String,
        resolver: String,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            self.json_call(
                reqwest::Method::GET,
                self.resolver_url(&table, &resolver, ""),
                None,
            )
            .await
        })
    }

    fn render_resolver(
        &self,
        table: String,
        resolver: String,
        mention: Value,
    ) -> BackendFuture<'_, Option<Value>> {
        Box::pin(async move {
            self.json_call(
                reqwest::Method::POST,
                self.resolver_url(&table, &resolver, ":render"),
                Some(&mention),
            )
            .await
        })
    }

    fn render_resolver_key(
        &self,
        table: String,
        resolver: String,
        mention: Value,
    ) -> BackendFuture<'_, Option<String>> {
        Box::pin(async move {
            let rendered = self.render_resolver(table, resolver, mention).await?;
            Ok(rendered.and_then(|rendered| {
                rendered
                    .get("key")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::body_json;
    use wiremock::matchers::method;
    use wiremock::matchers::path;

    #[tokio::test]
    async fn entity_curation_posts_to_the_entity_action() -> AntflyResult<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/db/v1/tables/codex_concepts/entities/concept%2Fagents:merge",
            ))
            .and(body_json(json!({"into": "concept/agent"})))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"complete": false})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/db/v1/tables/codex_concepts/entities/concept%2Fagents/redirects",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"redirects": []})))
            .expect(1)
            .mount(&server)
            .await;
        let backend = RemoteBackend::new(&server.uri(), None)?;
        let merged = backend
            .curate_entity(
                "codex_concepts".into(),
                "concept/agents".into(),
                json!({"action": "merge", "into": "concept/agent"}),
            )
            .await?;
        assert_eq!(merged, Some(json!({"complete": false})));
        let redirects = backend
            .curate_entity(
                "codex_concepts".into(),
                "concept/agents".into(),
                json!({"action": "redirects"}),
            )
            .await?;
        assert_eq!(redirects, Some(json!({"redirects": []})));
        Ok(())
    }

    #[tokio::test]
    async fn proposals_use_the_curation_routes() -> AntflyResult<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/db/v1/tables/codex_concepts/curation/proposals"))
            .and(body_json(
                json!({"action": "suppress", "key": "concept/note"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "p1"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/db/v1/tables/codex_concepts/curation/proposals"))
            .and(wiremock::matchers::query_param("state", "open"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"proposals": []})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/db/v1/tables/codex_concepts/curation/proposals/p1:reject",
            ))
            .and(body_json(json!({"actor": "user"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"complete": true})))
            .expect(1)
            .mount(&server)
            .await;
        let backend = RemoteBackend::new(&server.uri(), None)?;
        let table = || "codex_concepts".to_string();
        assert_eq!(
            backend
                .propose_curation(
                    table(),
                    json!({"action": "suppress", "key": "concept/note"})
                )
                .await?,
            Some(json!({"id": "p1"}))
        );
        assert_eq!(
            backend
                .list_curation_proposals(table(), json!({"state": "open"}))
                .await?,
            Some(json!({"proposals": []}))
        );
        assert_eq!(
            backend
                .decide_curation_proposal(
                    table(),
                    "p1".into(),
                    json!({"accept": false, "actor": "user"})
                )
                .await?,
            Some(json!({"complete": true}))
        );
        Ok(())
    }

    #[tokio::test]
    async fn resolver_reads_retry_busy_and_map_missing_to_none() -> AntflyResult<()> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/db/v1/tables/codex_knowledge/resolvers/concepts"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/db/v1/tables/codex_knowledge/resolvers/concepts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"redirects": []})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/db/v1/tables/codex_knowledge/resolvers/concepts:render",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"minted_key": "concept/agents", "key": "concept/agent"})),
            )
            .mount(&server)
            .await;
        let backend = RemoteBackend::new(&server.uri(), None)?;
        assert_eq!(
            backend
                .resolver_curation("codex_knowledge".into(), "concepts".into())
                .await?,
            Some(json!({"redirects": []}))
        );
        let mention = json!({"text": "Agents", "label": "concept"});
        assert_eq!(
            backend
                .render_resolver_key("codex_knowledge".into(), "concepts".into(), mention.clone())
                .await?,
            Some("concept/agent".to_string())
        );
        assert_eq!(
            backend
                .render_resolver("codex_memory_notes".into(), "concepts".into(), mention)
                .await?,
            None
        );
        Ok(())
    }
}
