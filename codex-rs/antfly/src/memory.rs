//! Client for Antfly's native memory service (`/memory/v1`): in process on
//! embedded Lite, or over HTTP on a server.

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::Value;
use serde_json::json;

use crate::Antfly;
use crate::error::AntflyError;
use crate::error::AntflyResult;

/// Attempts at a request the server reports busy (429) or unavailable (503).
const RETRY_ATTEMPTS: u32 = 8;
const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(200);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

enum Transport<'a> {
    Embedded(&'a Antfly),
    Http {
        client: reqwest::Client,
        base_url: String,
        api_key: Option<String>,
    },
}

/// One memory space.
pub struct MemorySpace<'a> {
    space: String,
    transport: Transport<'a>,
}

impl<'a> MemorySpace<'a> {
    /// A space in `antfly`'s own database (embedded Lite).
    pub fn embedded(antfly: &'a Antfly, space: &str) -> Self {
        Self {
            space: space.to_string(),
            transport: Transport::Embedded(antfly),
        }
    }

    /// A space on a server (for example `http://127.0.0.1:8080`).
    pub fn remote(base_url: &str, space: &str, api_key: Option<String>) -> AntflyResult<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|err| AntflyError::Config(format!("http client: {err}")))?;
        Ok(Self {
            space: space.to_string(),
            transport: Transport::Http {
                client,
                base_url: base_url.trim_end_matches('/').to_string(),
                api_key,
            },
        })
    }

    /// Runs one memory operation. `idempotency_key` is required for writes.
    pub async fn call(
        &self,
        action: &str,
        id: Option<&str>,
        body: Value,
        idempotency_key: Option<&str>,
    ) -> AntflyResult<Value> {
        match &self.transport {
            Transport::Embedded(antfly) => {
                let mut request = json!({"action": action, "body": body});
                if let Some(id) = id {
                    request["id"] = json!(id);
                }
                if let Some(key) = idempotency_key {
                    request["idempotency_key"] = json!(key);
                }
                antfly.memory(&self.space, request).await?.ok_or_else(|| {
                    AntflyError::Config(
                        "this Antfly backend has no in-process memory service".into(),
                    )
                })
            }
            Transport::Http {
                client,
                base_url,
                api_key,
            } => {
                let (method, url, body) = http_route(base_url, &self.space, action, id, body)?;
                http_call(
                    client,
                    api_key.as_deref(),
                    method,
                    url,
                    body,
                    idempotency_key,
                )
                .await
            }
        }
    }

    /// Creates the space with `config` unless it exists.
    pub async fn ensure(&self, config: Value) -> AntflyResult<()> {
        if self.call("inspect", None, json!({}), None).await.is_ok() {
            return Ok(());
        }
        self.call("create", None, config, Some("create")).await?;
        Ok(())
    }

    /// The space's current revision, which writes must name.
    pub async fn revision(&self) -> AntflyResult<u64> {
        let inspection = self.call("inspect", None, json!({}), None).await?;
        inspection["state"]["revision"]
            .as_u64()
            .ok_or_else(|| AntflyError::Malformed("space inspection has no revision".into()))
    }

    /// One page of an external evaluation run's judge tasks.
    pub async fn evaluation_tasks(
        &self,
        run: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> AntflyResult<Value> {
        let mut body = json!({"limit": limit});
        if let Some(cursor) = cursor {
            body["cursor"] = json!(cursor);
        }
        self.call("evaluation_tasks", Some(run), body, None).await
    }

    /// Submits verdicts for an external evaluation run, attributed to
    /// `actor`. `judge` must be the run's frozen judge, if it has one.
    pub async fn submit_verdicts(
        &self,
        run: &str,
        verdicts: Vec<Value>,
        actor: &str,
        judge: Option<&Value>,
    ) -> AntflyResult<Value> {
        let expected_revision = self.revision().await?;
        let mut body = json!({
            "expected_revision": expected_revision,
            "verdicts": verdicts,
            "actor": actor,
        });
        if let Some(judge) = judge {
            body["model"] = judge.clone();
        }
        let key = format!("verdicts-{run}-{expected_revision}");
        self.call("evaluation_verdict", Some(run), body, Some(&key))
            .await
    }

    /// An evaluation run's status.
    pub async fn evaluation(&self, run: &str) -> AntflyResult<Value> {
        self.call("evaluation_run", Some(run), json!({}), None)
            .await
    }

    /// Uploads labelled `cases` as dataset `dataset` and starts an external
    /// evaluation run `run` over it, freezing `judge` as its judge.
    pub async fn start_external_evaluation(
        &self,
        dataset: &str,
        run: &str,
        cases: &[Value],
        judge: Option<&Value>,
    ) -> AntflyResult<Value> {
        let mut batch: Vec<Value> = Vec::new();
        let mut batch_bytes = 0;
        for case in cases.iter().map(Some).chain(std::iter::once(None)) {
            let size = case.map(|case| case.to_string().len()).unwrap_or(0);
            if !batch.is_empty()
                && (case.is_none() || batch.len() == 32 || batch_bytes + size > 128 * 1024)
            {
                let revision = self.revision().await?;
                self.call(
                    "put_evaluation_dataset",
                    None,
                    json!({"id": dataset, "expected_revision": revision, "cases": batch}),
                    Some(&format!("dataset-{dataset}-{revision}")),
                )
                .await?;
                batch = Vec::new();
                batch_bytes = 0;
            }
            if let Some(case) = case {
                batch.push(case.clone());
                batch_bytes += size;
            }
        }
        let revision = self.revision().await?;
        let mut start = json!({
            "id": run,
            "dataset": dataset,
            "expected_revision": revision,
            "mode": "external",
        });
        if let Some(judge) = judge {
            start["judge"] = judge.clone();
        }
        self.call(
            "start_evaluation",
            None,
            start,
            Some(&format!("start-{run}")),
        )
        .await
    }
}

impl Antfly {
    /// Memory space `space` on this database's backend: in process on Lite,
    /// over HTTP (with the configured bearer token) on a server.
    pub fn memory_space(&self, space: &str) -> AntflyResult<MemorySpace<'_>> {
        match &self.config().backend {
            crate::BackendConfig::Embedded { .. } => Ok(MemorySpace::embedded(self, space)),
            crate::BackendConfig::Remote {
                url, api_key_env, ..
            } => MemorySpace::remote(
                url,
                space,
                api_key_env
                    .as_deref()
                    .and_then(|name| std::env::var(name).ok()),
            ),
        }
    }
}

/// The HTTP method, URL and body for one memory action.
fn http_route(
    base_url: &str,
    space: &str,
    action: &str,
    id: Option<&str>,
    body: Value,
) -> AntflyResult<(reqwest::Method, String, Option<Value>)> {
    use reqwest::Method;
    let space_url = format!("{base_url}/memory/v1/spaces/{}", encode_segment(space));
    let id = || {
        id.map(encode_segment)
            .ok_or_else(|| AntflyError::Config(format!("memory action {action} needs an id")))
    };
    let query = |body: &Value| -> String {
        let pairs: Vec<String> = body
            .as_object()
            .into_iter()
            .flatten()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string());
                format!("{key}={}", encode_segment(&value))
            })
            .collect();
        if pairs.is_empty() {
            String::new()
        } else {
            format!("?{}", pairs.join("&"))
        }
    };
    Ok(match action {
        "create" => (
            Method::POST,
            format!("{base_url}/memory/v1/spaces"),
            Some(json!({"space": space, "config": body})),
        ),
        "inspect" => (Method::GET, space_url, None),
        "put_evaluation_dataset" => (
            Method::POST,
            format!("{space_url}/evaluation-datasets"),
            Some(body),
        ),
        "start_evaluation" => (Method::POST, format!("{space_url}/evaluations"), Some(body)),
        "evaluation_run" => (
            Method::GET,
            format!("{space_url}/evaluations/{}", id()?),
            None,
        ),
        "evaluation_tasks" => (
            Method::GET,
            format!("{space_url}/evaluations/{}/tasks{}", id()?, query(&body)),
            None,
        ),
        "evaluation_results" => (
            Method::GET,
            format!("{space_url}/evaluations/{}/results{}", id()?, query(&body)),
            None,
        ),
        "evaluation_verdict" => (
            Method::POST,
            format!("{space_url}/evaluations/{}/verdicts", id()?),
            Some(body),
        ),
        "observe" => (
            Method::POST,
            format!("{space_url}/observations"),
            Some(body),
        ),
        "list" => (
            Method::GET,
            format!("{space_url}/memories{}", query(&body)),
            None,
        ),
        "supersession_candidates" => (
            Method::POST,
            format!("{space_url}/memories/{}/supersession-candidates", id()?),
            Some(body),
        ),
        other => {
            return Err(AntflyError::Config(format!(
                "memory action {other} has no HTTP mapping here"
            )));
        }
    })
}

async fn http_call(
    client: &reqwest::Client,
    api_key: Option<&str>,
    method: reqwest::Method,
    url: String,
    body: Option<Value>,
    idempotency_key: Option<&str>,
) -> AntflyResult<Value> {
    let mut delay = RETRY_INITIAL_DELAY;
    let mut attempt = 0;
    loop {
        let mut request = client.request(method.clone(), url.clone());
        if let Some(key) = api_key {
            request = request.bearer_auth(key);
        }
        if let Some(key) = idempotency_key {
            request = request.header("Idempotency-Key", key);
        }
        if let Some(body) = &body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|err| AntflyError::Remote(err.to_string()))?;
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
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(AntflyError::Remote(format!("{status}: {body}")));
        }
        return response
            .json::<Value>()
            .await
            .map_err(|err| AntflyError::Malformed(err.to_string()));
    }
}

/// Percent-encodes one URL path segment or query value.
fn encode_segment(segment: &str) -> String {
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
