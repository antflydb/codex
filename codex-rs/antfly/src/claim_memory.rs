//! Codex's claims in Antfly's memory service.
//!
//! When a thread settles, its claims (all but retracted ones) are observed as
//! settled assertions in the [`SPACE`] memory space: the claim id as the
//! assertion id, subject from the claim's working directory, the claim's
//! kind, one evidence record per claim. Codex's supersession links are not
//! written, so the space proposes candidates on its own.
//!
//! With `claim_memory = "enforce"` (the default) those candidates are what
//! settling judges for cross-thread supersession; with `"shadow"` Codex keeps
//! its own text search and the space's candidates are only logged. Either way
//! each current claim's comparison goes to `codex_decision_log` (kind
//! [`SHADOW_KIND`]), and [`report`] summarizes them.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::Value;
use serde_json::json;

use crate::Antfly;
use crate::AntflyResult;
use crate::BackendConfig;
use crate::knowledge::Claim;
use crate::knowledge::ClaimStatus;
use crate::memory::MemorySpace;

/// The memory space that holds Codex's claims.
pub const SPACE: &str = "codex_claims";
/// `codex_decision_log` kind of a candidate comparison; its subject is the
/// Codex claim id.
pub const SHADOW_KIND: &str = "memory_supersession";
const CANDIDATES: usize = 10;
/// Observations per request (the service accepts up to 128).
const OBSERVE_BATCH: usize = 32;
/// Polls for candidate readiness before using what is there.
const READY_POLLS: usize = 50;
const READY_POLL_DELAY: Duration = Duration::from_millis(200);
/// Maintenance steps per poll on Lite, which runs memory work only when
/// asked.
const ADVANCE_STEPS: usize = 20;
/// Output size of the default embedder (EmbeddingGemma 2). Memory spaces need
/// an explicit dimension (antflydb/antfly#1295); other embedders get
/// semantic candidates only with `embedder_dims` set.
const DEFAULT_EMBEDDER_DIMS: u32 = 768;

fn subject(cwd: Option<&str>) -> String {
    match cwd
        .map(|cwd| cwd.trim_end_matches('/'))
        .filter(|cwd| !cwd.is_empty())
    {
        Some(cwd) => format!("cwd:{cwd}"),
        None => "cwd:unknown".to_string(),
    }
}

/// The space configuration, and the candidate mode it supports: semantic
/// when the embedder's dimension is known, else lexical.
fn space_config(antfly: &Antfly) -> (Value, &'static str) {
    let Some(embedder) = &antfly.config().embedder else {
        return (json!({}), "lexical");
    };
    let default = crate::EmbedderConfig::default();
    let dims = embedder
        .dims
        .or((embedder.model == default.model).then_some(DEFAULT_EMBEDDER_DIMS));
    match dims {
        Some(dims) => (
            json!({
                "features": {"semantic_recall": true},
                "models": {"embedder": {
                    "provider": "antfly",
                    "model": embedder.model,
                    "revision": "current",
                    "dimensions": dims,
                }},
            }),
            "semantic",
        ),
        None => (json!({}), "lexical"),
    }
}

/// The claim space, created on first use.
pub(crate) struct ClaimSpace<'a> {
    antfly: &'a Antfly,
    space: MemorySpace<'a>,
    mode: &'static str,
    embedded: bool,
}

/// A claim's supersession candidates from the space.
#[derive(Clone, Debug)]
pub(crate) struct Candidates {
    pub mode: &'static str,
    pub retrieval: Value,
    pub semantic_discovery: Value,
    /// Claim ids, best first.
    pub ids: Vec<String>,
}

impl<'a> ClaimSpace<'a> {
    pub(crate) async fn open(antfly: &'a Antfly) -> AntflyResult<Self> {
        let space = antfly.memory_space(SPACE)?;
        let (config, mode) = space_config(antfly);
        space.ensure(config).await?;
        Ok(Self {
            antfly,
            space,
            mode,
            embedded: matches!(antfly.config().backend, BackendConfig::Embedded { .. }),
        })
    }

    /// Observes `claims` (all but retracted ones). Returns how many.
    pub(crate) async fn mirror(&self, claims: &[Claim]) -> AntflyResult<usize> {
        let observed: Vec<&Claim> = claims
            .iter()
            .filter(|claim| claim.status != ClaimStatus::Retracted)
            .collect();
        for chunk in observed.chunks(OBSERVE_BATCH) {
            observe(&self.space, chunk).await?;
        }
        Ok(observed.len())
    }

    /// The space's supersession candidates for claim `id`, once its indexes
    /// have caught up (or after [`READY_POLLS`]).
    pub(crate) async fn candidates(&self, id: &str) -> AntflyResult<Candidates> {
        let page = candidates(&self.space, id, self.mode, self.embedded).await?;
        Ok(Candidates {
            mode: self.mode,
            retrieval: page["retrieval"].clone(),
            semantic_discovery: page["semantic_discovery"].clone(),
            ids: candidate_ids(&page),
        })
    }

    /// Logs one claim's comparison: the space's candidates, the candidates
    /// Codex judged, and the claims it superseded.
    pub(crate) async fn record(&self, decision: &Decision, found: &Candidates) -> AntflyResult<()> {
        let response = json!({
            "mode": found.mode,
            "retrieval": found.retrieval,
            "semantic_discovery": found.semantic_discovery,
            "memory_candidates": found.ids,
            "codex_candidates": decision.candidates,
            "codex_supersedes": decision.supersedes,
        });
        let result = (!decision.supersedes.is_empty()).then(|| {
            decision
                .supersedes
                .iter()
                .all(|earlier| found.ids.contains(earlier))
        });
        crate::labels::record_decision(
            self.antfly,
            crate::labels::DecisionRecord {
                kind: SHADOW_KIND,
                model: found.mode,
                subject: Some(decision.claim_id.clone()),
                input: &decision.statement,
                response: &response,
                result,
                score: None,
                threshold_bp: None,
            },
        )
        .await
    }
}

/// What Codex decided for one current claim when its thread settled.
#[derive(Clone, Debug, Default)]
pub(crate) struct Decision {
    pub claim_id: String,
    pub statement: String,
    /// Claims it was judged against, most similar first.
    pub candidates: Vec<String>,
    /// Claims it supersedes, from its own thread and from others.
    pub supersedes: Vec<String>,
}

async fn observe(space: &MemorySpace<'_>, claims: &[&Claim]) -> AntflyResult<()> {
    let mut evidence = Vec::with_capacity(claims.len());
    let mut assertions = Vec::with_capacity(claims.len());
    for claim in claims {
        let evidence_id = format!("evidence:{}", claim.id);
        evidence.push(json!({
            "id": evidence_id,
            "text": claim.statement,
            "source_id": claim.thread_id,
            "source_version": claim.id,
        }));
        assertions.push(json!({
            "id": claim.id,
            "subject": subject(claim.cwd.as_deref()),
            "predicate": "states",
            "value": claim.statement,
            "statement": claim.statement,
            "kind": claim.kind.as_str(),
            "stability": "settled",
            "evidence": [{"id": evidence_id}],
        }));
    }
    let body = json!({"evidence": evidence, "assertions": assertions});
    let key = format!(
        "observe-{:016x}",
        crate::knowledge::fnv1a(&body.to_string())
    );
    let operation = space.call("observe", None, body, Some(&key)).await?;
    if operation["phase"] == "failed" {
        tracing::warn!(
            "antfly: claim memory observation failed: {}",
            operation["error_code"]
        );
    }
    Ok(())
}

/// The candidate page for `id`, once the space's indexes have caught up
/// (or after [`READY_POLLS`], logged as not ready).
async fn candidates(
    space: &MemorySpace<'_>,
    id: &str,
    mode: &str,
    embedded: bool,
) -> AntflyResult<Value> {
    let mut poll = 0;
    loop {
        let page = space
            .call(
                "supersession_candidates",
                Some(id),
                json!({"limit": CANDIDATES, "mode": mode}),
                None,
            )
            .await?;
        poll += 1;
        if ready(&page, mode) || poll == READY_POLLS {
            return Ok(page);
        }
        if embedded {
            advance(space).await;
        }
        tokio::time::sleep(READY_POLL_DELAY).await;
    }
}

fn ready(page: &Value, mode: &str) -> bool {
    page["retrieval"] == "indexed" && (mode == "lexical" || page["semantic_discovery"] == "indexed")
}

/// Runs a bounded amount of the space's pending maintenance.
async fn advance(space: &MemorySpace<'_>) {
    for _ in 0..ADVANCE_STEPS {
        let key = format!("advance-{}", uuid_like());
        if space
            .call("advance", Some(""), json!({}), Some(&key))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn uuid_like() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    format!("{nanos:x}")
}

/// Assertion ids of a candidate page, best first.
fn candidate_ids(page: &Value) -> Vec<String> {
    page["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let view = item.get("view").unwrap_or(item);
            let memory = view.get("memory").unwrap_or(view);
            memory["id"]
                .as_str()
                .or(memory["assertion"]["id"].as_str())
                .map(str::to_string)
        })
        .collect()
}

/// How the memory service's candidates compare with Codex's decisions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShadowReport {
    /// Candidate mode of the latest comparisons.
    pub mode: Option<String>,
    /// Settled claims compared (latest comparison per claim).
    pub claims: usize,
    /// Comparisons logged before the space's indexes were ready.
    pub not_ready: usize,
    /// Claims Codex decided to supersede.
    pub supersessions: usize,
    /// Of those, ones among the memory service's candidates.
    pub found: usize,
    pub memory_candidates: usize,
    pub codex_candidates: usize,
    /// Claims both Codex and the memory service proposed.
    pub shared_candidates: usize,
    /// Supersessions within one thread (the extractor's `replaces`), and
    /// how many of them the memory service proposed.
    pub within_thread: usize,
    pub within_thread_found: usize,
    /// Codex supersessions the memory service did not propose.
    pub misses: Vec<Miss>,
}

/// A claim Codex superseded that was not among the memory service's
/// candidates for its replacement.
#[derive(Clone, Debug, PartialEq)]
pub struct Miss {
    pub claim: String,
    pub superseded: String,
    pub within_thread: bool,
    /// The memory service's candidates, best first.
    pub memory_candidates: Vec<String>,
}

/// The thread a claim id belongs to (`<version>:<thread>:...`).
fn thread_of(claim_id: &str) -> Option<&str> {
    claim_id.split(':').nth(1)
}

/// Compares the latest shadow comparison of each claim.
pub async fn report(antfly: &Antfly) -> AntflyResult<ShadowReport> {
    let mut report = ShadowReport::default();
    for (claim, response) in crate::labels::latest_responses(antfly, SHADOW_KIND).await? {
        let ids = |field: &str| -> BTreeSet<String> {
            response[field]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|id| id.as_str().map(str::to_string))
                .collect()
        };
        let memory = ids("memory_candidates");
        let codex = ids("codex_candidates");
        let supersedes = ids("codex_supersedes");
        report.claims += 1;
        report.mode = response["mode"].as_str().map(str::to_string);
        if !ready(&response, response["mode"].as_str().unwrap_or("lexical")) {
            report.not_ready += 1;
        }
        report.supersessions += supersedes.len();
        report.found += supersedes.intersection(&memory).count();
        for earlier in &supersedes {
            let within_thread = thread_of(earlier) == thread_of(&claim);
            let found = memory.contains(earlier);
            if within_thread {
                report.within_thread += 1;
                report.within_thread_found += usize::from(found);
            }
            if !found {
                report.misses.push(Miss {
                    claim: claim.clone(),
                    superseded: earlier.clone(),
                    within_thread,
                    memory_candidates: response["memory_candidates"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|id| id.as_str().map(str::to_string))
                        .collect(),
                });
            }
        }
        report.memory_candidates += memory.len();
        report.codex_candidates += codex.len();
        report.shared_candidates += codex.intersection(&memory).count();
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn subjects_come_from_the_working_directory() {
        assert_eq!(subject(Some("/src/codex/")), "cwd:/src/codex");
        assert_eq!(subject(None), "cwd:unknown");
        assert_eq!(subject(Some("")), "cwd:unknown");
    }

    #[test]
    fn claim_ids_name_their_thread() {
        assert_eq!(
            thread_of("turns:thread-a:00000000000000000000:000"),
            Some("thread-a")
        );
        assert_eq!(thread_of("claim-1"), None);
    }

    #[test]
    fn candidate_ids_read_views_in_order() {
        let page = json!({"items": [
            {"view": {"memory": {"id": "claim-1"}}},
            {"view": {"id": "claim-2"}},
            {"memory": {"assertion": {"id": "claim-3"}}},
        ]});
        assert_eq!(candidate_ids(&page), vec!["claim-1", "claim-2", "claim-3"]);
    }
}
