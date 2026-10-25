//! Shadow mirror of settled claims in Antfly's memory service.
//!
//! When a thread settles, its claims (all but retracted ones) are observed as
//! settled assertions in the [`SPACE`] memory space: subject from the claim's
//! working directory, the claim's kind, one evidence record per claim. For
//! each of the thread's current claims, the space's supersession candidates
//! are then logged to `codex_decision_log` (kind [`SHADOW_KIND`]) next to the
//! candidates Codex searched and the claims it decided to supersede. Codex's
//! supersession links are not written to the space, so its candidates are its
//! own. [`report`] compares the two before claims move to the service.

use std::collections::BTreeMap;
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

/// The memory space that mirrors Codex's claims.
pub const SPACE: &str = "codex_claims";
/// `codex_decision_log` kind of a shadow comparison; its subject is the
/// Codex claim id.
pub const SHADOW_KIND: &str = "memory_supersession";
const CANDIDATES: usize = 10;
/// Observations per request (the service accepts up to 128).
const OBSERVE_BATCH: usize = 32;
/// Polls for candidate readiness before logging what is there.
const READY_POLLS: usize = 50;
const READY_POLL_DELAY: Duration = Duration::from_millis(200);
/// Maintenance steps per poll on Lite, which runs memory work only when
/// asked.
const ADVANCE_STEPS: usize = 20;
/// Output size of the default embedder (EmbeddingGemma 2). Memory spaces need
/// an explicit dimension (antflydb/antfly#1295); other embedders get
/// semantic candidates only with `embedder_dims` set.
const DEFAULT_EMBEDDER_DIMS: u32 = 768;

/// The assertion id of Codex claim `claim_id`.
pub fn memory_id(claim_id: &str) -> String {
    format!("claim-{:016x}", crate::knowledge::fnv1a(claim_id))
}

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

/// What Codex decided for one current claim when its thread settled.
#[derive(Clone, Debug, Default)]
pub(crate) struct Decision {
    pub claim_id: String,
    pub statement: String,
    /// Claims Codex compared it with, most similar first.
    pub candidates: Vec<String>,
    /// Claims it supersedes, from its own thread and from others.
    pub supersedes: Vec<String>,
}

/// Mirrors `claims` and logs the space's candidates for `decisions`.
/// Failures are logged, never returned: this is a shadow.
pub(crate) async fn shadow_settle(antfly: &Antfly, claims: &[Claim], decisions: &[Decision]) {
    if let Err(err) = settle(antfly, claims, decisions).await {
        tracing::warn!("antfly: claim memory shadow failed: {err}");
    }
}

/// Observes `claims` (all but retracted ones) in the space, creating it
/// first if needed. Returns how many were observed.
pub(crate) async fn mirror(antfly: &Antfly, claims: &[Claim]) -> AntflyResult<usize> {
    let space = antfly.memory_space(SPACE)?;
    space.ensure(space_config(antfly).0).await?;
    let observed: Vec<&Claim> = claims
        .iter()
        .filter(|claim| claim.status != ClaimStatus::Retracted)
        .collect();
    for chunk in observed.chunks(OBSERVE_BATCH) {
        observe(&space, chunk).await?;
    }
    Ok(observed.len())
}

async fn settle(antfly: &Antfly, claims: &[Claim], decisions: &[Decision]) -> AntflyResult<()> {
    mirror(antfly, claims).await?;
    let space = antfly.memory_space(SPACE)?;
    let mode = space_config(antfly).1;
    let embedded = matches!(antfly.config().backend, BackendConfig::Embedded { .. });
    // Codex ids by assertion id, for the claims this comparison can name.
    let mut known: BTreeMap<String, String> = BTreeMap::new();
    for id in claims.iter().map(|claim| &claim.id).chain(
        decisions
            .iter()
            .flat_map(|decision| decision.candidates.iter().chain(&decision.supersedes)),
    ) {
        known.insert(memory_id(id), id.clone());
    }
    for decision in decisions {
        let page = candidates(&space, &memory_id(&decision.claim_id), mode, embedded).await?;
        let found: Vec<String> = candidate_ids(&page)
            .into_iter()
            .map(|id| known.get(&id).cloned().unwrap_or(id))
            .collect();
        let response = json!({
            "mode": mode,
            "retrieval": page["retrieval"],
            "semantic_discovery": page["semantic_discovery"],
            "memory_candidates": found,
            "codex_candidates": decision.candidates,
            "codex_supersedes": decision.supersedes,
        });
        let result = (!decision.supersedes.is_empty()).then(|| {
            decision
                .supersedes
                .iter()
                .all(|earlier| found.contains(earlier))
        });
        crate::labels::record_decision(
            antfly,
            crate::labels::DecisionRecord {
                kind: SHADOW_KIND,
                model: mode,
                subject: Some(decision.claim_id.clone()),
                input: &decision.statement,
                response: &response,
                result,
                score: None,
                threshold_bp: None,
            },
        )
        .await?;
    }
    Ok(())
}

async fn observe(space: &MemorySpace<'_>, claims: &[&Claim]) -> AntflyResult<()> {
    let mut evidence = Vec::with_capacity(claims.len());
    let mut assertions = Vec::with_capacity(claims.len());
    for claim in claims {
        let id = memory_id(&claim.id);
        let evidence_id = format!("e-{id}");
        evidence.push(json!({
            "id": evidence_id,
            "text": claim.statement,
            "source_id": claim.thread_id,
            "source_version": id,
        }));
        assertions.push(json!({
            "id": id,
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
