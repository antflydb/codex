//! Knowledge graph: claims (typed propositions distilled from sessions) and
//! memory notes, linked to the concepts they are about.
//!
//! Two graph indexes live on each knowledge table:
//!
//! - **Concepts** (`codex_concepts_<hash>`): a GLiNER extractor enrichment over
//!   `search_text`, with a resolver minting canonical `concept/<slug>` keys.
//!   Each document gets `about` edges to its concepts, and stable
//!   concept→concept relations become edges too. The name hashes the model
//!   and label schema, so changing either rebuilds the index.
//! - **Claim links** (`codex_claim_links`): `supersedes` edges derived from
//!   each claim's `supersedes` field. A field-derived edge type cannot share
//!   an index with an artifact source, and keeping it separate means a concept
//!   rebuild never loses supersession history.
//!
//! Claims are extracted turn by turn (see `codex_antfly_extension`'s claim
//! pipeline). The extractor sees its thread's current claims, so a later turn
//! replaces or retracts an earlier claim from the same thread directly. A
//! claim stays provisional (`settled: false`) until its thread goes idle;
//! settling compares each claim with its most similar settled claims from
//! other threads using a decide-model predicate, so a half-finished session
//! never supersedes established knowledge. Retrieval seeds concepts from the
//! query (GLiNER at query time plus the concepts of direct search hits), then
//! walks `about` edges back to the claims and notes that share them, ranking
//! provisional claims below settled ones.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use crate::Antfly;
use crate::AntflyResult;
use crate::ScanRequest;
use crate::Write;
use crate::backend::GraphIndex;
use crate::schema;

pub const CONCEPT_GRAPH_PREFIX: &str = "codex_concepts";
/// Field-derived claim-to-claim edges. Versioned so a change of edge types
/// replaces the index in place (`replaces_prefix`).
pub const CLAIM_LINKS_INDEX: &str = "codex_claim_links_v2";
const CLAIM_LINKS_PREFIX: &str = "codex_claim_links";
pub const ABOUT_EDGE: &str = "about";
pub const SUPERSEDES_EDGE: &str = "supersedes";
/// A rationale (or lesson, constraint) motivates the decision it explains.
pub const MOTIVATES_EDGE: &str = "motivates";
/// An alternative was rejected in favor of the decision chosen instead.
pub const REJECTED_IN_FAVOR_OF_EDGE: &str = "rejected_in_favor_of";
/// Name of the concept resolver declared inline by every concept graph.
pub const CONCEPT_RESOLVER: &str = "concepts";
/// Query text sent to query-time resolution; the configured extractor
/// windows long input, so this only bounds the request.
const QUERY_RESOLVE_MAX_BYTES: usize = 8 * 1024;
/// Key prefix of concept nodes minted by the resolver.
pub const CONCEPT_PREFIX: &str = "concept/";

/// Concept labels. GLiNER is zero-shot, so these describe ideas rather than
/// code. The set is deliberately this wide: Antfly serves windowed GLiNER2.5
/// extraction only within its measured profile, whose minimum padded
/// sequence was measured with an 11-entity, 6-relation schema; a narrower
/// schema makes short claims fall below it and fail closed.
const CONCEPT_ENTITIES: &[&str] = &[
    "concept",
    "technology",
    "tool",
    "project",
    "organization",
    "person",
    "goal",
    "problem",
    "practice",
    "constraint",
    "metric",
];
const CONCEPT_RELATIONS: &[&str] = &[
    "part_of",
    "alternative_to",
    "uses",
    "depends_on",
    "replaces",
    "causes",
];
/// Bump when the labels, relations, resolver or metric settings change.
const CONCEPT_SCHEMA_VERSION: u32 = 3;
/// Personalized PageRank over the concept graph, seeded per query.
pub const PPR_METRIC: &str = "ppr";
/// `graph_metric` accepts at most this many seed nodes.
const MAX_PPR_SEEDS: usize = 128;
/// GLiNER's single-window extraction profile is measured up to 610 bytes.
const QUERY_EXTRACT_MAX_BYTES: usize = 600;
/// Laya rejects over-long inputs; keep each statement well inside them.
const DECIDE_STATEMENT_MAX_CHARS: usize = 600;
const SUPERSEDE_CANDIDATES: usize = 3;
/// Claims about a concept given to the generator that describes it.
const DESCRIBE_EVIDENCE: usize = 8;
/// Longest description kept, in characters.
const DESCRIPTION_MAX_CHARS: usize = 600;
const DESCRIBE_INSTRUCTIONS: &str = "You write one or two plain sentences describing a concept from a \
software project's knowledge graph, using only the facts given. Say what it is and its role. Do \
not speculate.";
/// `version` of claims written by the per-turn pipeline. Claims from the
/// earlier stage-1 extraction used the memory version (`v1`, `v2`).
pub const TURN_CLAIMS_VERSION: &str = "turns";
/// Every claim key namespace, for deletion.
const CLAIM_VERSIONS: &[&str] = &[TURN_CLAIMS_VERSION, "v1", "v2"];
/// Provisional claims rank at this fraction of their score.
const PROVISIONAL_WEIGHT: f64 = 0.5;

fn fnv1a(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Name of the concept graph index for `extract_model`.
pub fn concept_graph_name(extract_model: &str) -> String {
    let hash = fnv1a(&format!("{extract_model}:{CONCEPT_SCHEMA_VERSION}"));
    format!("{CONCEPT_GRAPH_PREFIX}_{hash:016x}")
}

pub(crate) fn concept_graph_index(extract_model: &str) -> GraphIndex {
    let name = concept_graph_name(extract_model);
    let artifact = format!("{name}_extraction");
    let mut edge_types = vec![json!({"name": ABOUT_EDGE})];
    edge_types.extend(
        CONCEPT_RELATIONS
            .iter()
            .map(|relation| json!({"name": relation})),
    );
    let config = json!({
        "source": {
            "artifact": artifact,
            "path": "$.relations[*]",
            "format": "extraction_relation",
            "mention_edge_type": ABOUT_EDGE,
            "edge": {"type": "{{ _item.type }}", "weight": "{{ _item.score }}"},
        },
        "artifact": {
            "name": artifact,
            "kind": "asset",
            "source": {"type": "field", "value": crate::SEARCH_TEXT_FIELD},
            "content_type": "application/json",
            "producer_json": {"type": "extractor", "config": {
                "provider": "antfly",
                "model": extract_model,
                "schema": {
                    "entities": CONCEPT_ENTITIES,
                    "relations": CONCEPT_RELATIONS
                        .iter()
                        .map(|relation| json!({"type": relation}))
                        .collect::<Vec<_>>(),
                },
                "options": {
                    "include_confidence": true,
                    "include_spans": true,
                    "long_document": {"mode": "window"},
                },
            }},
        },
        "edge_types": edge_types,
        // Seeded per query with `direction: both`, so mass flows from
        // concepts back to the documents about them.
        "metrics": {PPR_METRIC: {"kind": "pagerank"}},
        "resolvers": [{
            "name": CONCEPT_RESOLVER,
            "table": schema::CONCEPTS,
            "source_artifact": artifact,
            "resolution_artifact": format!("{name}_resolution"),
            // Label-free keys: a per-mention label guess in the key would
            // split one idea into several nodes. `uslug` keeps non-ASCII
            // names distinct instead of collapsing them.
            "key_template": "concept/{{ uslug _entity.text }}",
            "min_confidence": 0.5,
            "config_generation": 1,
        }],
    });
    GraphIndex {
        name,
        config,
        replaces_prefix: CONCEPT_GRAPH_PREFIX,
    }
}

pub(crate) fn claim_links_index() -> GraphIndex {
    GraphIndex {
        name: CLAIM_LINKS_INDEX.to_string(),
        config: json!({"edge_types": [
            {"name": SUPERSEDES_EDGE, "field": "supersedes"},
            {"name": MOTIVATES_EDGE, "field": "motivates"},
            {"name": REJECTED_IN_FAVOR_OF_EDGE, "field": "rejected_in_favor_of"},
        ]}),
        replaces_prefix: CLAIM_LINKS_PREFIX,
    }
}

/// The concept key Antfly's resolver mints for `text`: `concept/` plus
/// Antfly's `slug` (lowercase ASCII alphanumerics, other runs collapsed to
/// `_`, possessive `'s` dropped). `None` when nothing alphanumeric remains.
pub fn concept_key(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = String::new();
    let mut pending_sep = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let apostrophe_len = if c == b'\'' {
            1
        } else if c == 0xE2 && i + 2 < bytes.len() && bytes[i + 1] == 0x80 && bytes[i + 2] == 0x99 {
            3
        } else {
            0
        };
        if apostrophe_len > 0 && !out.is_empty() && !pending_sep {
            let s_index = i + apostrophe_len;
            let after_s = s_index + 1;
            let possessive = s_index < bytes.len()
                && bytes[s_index].eq_ignore_ascii_case(&b's')
                && (after_s >= bytes.len() || !bytes[after_s].is_ascii_alphanumeric());
            if possessive {
                i = s_index + 1;
                continue;
            }
        }
        let lower = c.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            if pending_sep && !out.is_empty() {
                out.push('_');
            }
            out.push(lower as char);
            pending_sep = false;
        } else if !out.is_empty() {
            pending_sep = true;
        }
        i += if apostrophe_len == 3 { 3 } else { 1 };
    }
    (!out.is_empty()).then(|| format!("{CONCEPT_PREFIX}{out}"))
}

/// Minimum name similarity for a concept-merge candidate: a neighbor's
/// search score divided by the concept's own score. Queries and stored names
/// are embedded with different roles, so even a name scores below 1.0
/// against itself and raw scores are not comparable across names. On
/// `tests/fixtures/knowledge_eval/concept_pairs.jsonl`, true synonyms score
/// 0.82–0.97, except abbreviations such as "KG" (0.73; configure those as
/// aliases), while hard negatives reach 0.91 ("PageRank" and "Personalized
/// PageRank"). The floor only decides which pairs a model judges.
pub const CONCEPT_SIMILARITY_FLOOR: f64 = 0.80;

/// Per-turn claims by state (see [`Knowledge::claim_counts`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClaimCounts {
    /// Current and settled.
    pub current: u64,
    /// Current, from a thread that has not settled.
    pub provisional: u64,
    pub superseded: u64,
    pub retracted: u64,
}

/// A concept promoted into [`schema::CONCEPTS`] by the resolver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Concept {
    pub key: String,
    pub name: String,
    pub entity_type: Option<String>,
}

/// A name's lexical identity: its alphanumeric runs lowercased, each with a
/// plural `s` dropped, joined without separators. "ThreadStore", "thread
/// store" and "thread-stores" share one; digits are kept, so versions never
/// do.
pub fn concept_identity(name: &str) -> String {
    name.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| {
            let lower = token.to_lowercase();
            match lower.strip_suffix('s') {
                Some(stem)
                    if stem.chars().count() >= 3
                        && !stem.ends_with('s')
                        && stem.chars().all(char::is_alphabetic) =>
                {
                    stem.to_string()
                }
                _ => lower,
            }
        })
        .collect()
}

/// The number runs in `name` ("Qwen3" → `["3"]`, "GLiNER 2.5" → `["2",
/// "5"]`). Names with different numbers name different versions or parts.
pub fn number_tokens(name: &str) -> Vec<String> {
    name.split(|c: char| !c.is_ascii_digit())
        .filter(|run| !run.is_empty())
        .map(str::to_string)
        .collect()
}

/// What a claim asserts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimKind {
    Decision,
    Rationale,
    Preference,
    Lesson,
    Constraint,
    OpenQuestion,
    /// An option considered and not chosen.
    Alternative,
}

impl ClaimKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ClaimKind::Decision => "decision",
            ClaimKind::Rationale => "rationale",
            ClaimKind::Preference => "preference",
            ClaimKind::Lesson => "lesson",
            ClaimKind::Constraint => "constraint",
            ClaimKind::OpenQuestion => "open_question",
            ClaimKind::Alternative => "alternative",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Current,
    Superseded,
    /// A later turn of the same thread said it no longer holds.
    Retracted,
}

/// A claim extracted from one turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnClaim {
    pub kind: ClaimKind,
    pub statement: String,
    pub turn_id: Option<String>,
    /// History ordinal where the turn starts; orders claims within a thread.
    pub turn_ordinal: u64,
    /// Ids of the thread's current claims this one replaces.
    pub replaces: Vec<String>,
    /// Decisions this claim explains (for rationale, lessons, constraints).
    pub motivates: Vec<ClaimRef>,
    /// Decisions chosen instead of this alternative.
    pub rejected_in_favor_of: Vec<ClaimRef>,
}

/// A claim a new claim links to: an existing one, or another claim of the
/// same batch by its position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimRef {
    Existing(String),
    New(usize),
}

/// Claims extracted from a batch of a thread's turns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnClaimBatch {
    pub thread_id: String,
    pub cwd: Option<String>,
    /// When the newest turn in the batch was recorded.
    pub source_updated_at_ms: i64,
    pub claims: Vec<TurnClaim>,
    /// Ids of the thread's current claims that no longer hold.
    pub retracts: Vec<String>,
}

/// A stored claim.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub id: String,
    pub kind: ClaimKind,
    pub statement: String,
    pub status: ClaimStatus,
    pub thread_id: String,
    pub version: String,
    #[serde(default)]
    pub cwd: Option<String>,
    pub source_updated_at_ms: i64,
    pub generated_at_ms: i64,
    /// Ids of earlier claims this one replaces.
    #[serde(default)]
    pub supersedes: Vec<String>,
    #[serde(default)]
    pub superseded_by: Option<String>,
    /// Decisions this claim explains.
    #[serde(default)]
    pub motivates: Vec<String>,
    /// Decisions chosen instead of this alternative.
    #[serde(default)]
    pub rejected_in_favor_of: Vec<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub turn_ordinal: Option<u64>,
    /// False while the claim's thread is still active. Claims from before
    /// the per-turn pipeline were extracted from finished threads.
    #[serde(default = "settled_by_default")]
    pub settled: bool,
}

fn settled_by_default() -> bool {
    true
}

impl Claim {
    fn to_doc(&self) -> AntflyResult<Value> {
        let mut doc = serde_json::to_value(self)?;
        doc[crate::SEARCH_TEXT_FIELD] = json!(self.statement);
        Ok(doc)
    }

    fn from_doc(id: &str, doc: Value) -> Option<Self> {
        let mut doc = doc;
        doc["id"] = json!(id);
        serde_json::from_value(crate::strip_reserved(doc)).ok()
    }
}

/// A claim found by [`Knowledge::related`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RelatedClaim {
    pub claim: Claim,
    /// Shared concepts that led here, as concept keys.
    pub concepts: Vec<String>,
    pub score: f64,
    /// Current claims that explain this one (`motivates` edges into it).
    pub motivated_by: Vec<Claim>,
    /// Alternatives rejected in favor of this one.
    pub alternatives: Vec<Claim>,
}

/// A memory note found by [`Knowledge::related`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RelatedNote {
    pub namespace: String,
    pub path: String,
    pub concepts: Vec<String>,
    pub score: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Related {
    /// Concept keys the search started from.
    pub seeds: Vec<String>,
    pub claims: Vec<RelatedClaim>,
    pub notes: Vec<RelatedNote>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

fn truncate_chars(text: &str, max: usize) -> &str {
    match text.char_indices().nth(max) {
        Some((index, _)) => &text[..index],
        None => text,
    }
}

fn truncate_bytes(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn claim_prefix(version: &str, thread_id: &str) -> String {
    format!(
        "{}:{}:",
        crate::keys::escape(version),
        crate::keys::escape(thread_id)
    )
}

/// Canonical keys from a `resolve_query` response, in mention order.
fn resolved_keys(response: &Value) -> Vec<String> {
    let mut keys = Vec::new();
    for entity in response
        .pointer("/resolution/entities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(key) = entity.pointer("/doc_ref/key").and_then(Value::as_str)
            && !keys.iter().any(|seen: &String| seen == key)
        {
            keys.push(key.to_string());
        }
    }
    keys
}

/// One node of a traversal, with the start node of its path.
struct Reached {
    key: String,
    from: String,
    doc: Option<Value>,
}

fn reached_nodes(response: &Value, query: &str) -> Vec<Reached> {
    let nodes = response
        .get("responses")
        .and_then(Value::as_array)
        .and_then(|responses| responses.first())
        .unwrap_or(response)
        .pointer(&format!("/graph_results/{query}/nodes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    nodes
        .into_iter()
        .filter_map(|node| {
            let key = node.get("key")?.as_str()?.to_string();
            let from = node
                .get("path")
                .and_then(Value::as_array)
                .and_then(|path| path.first())
                .and_then(|start| start.get("key"))
                .and_then(Value::as_str)?
                .to_string();
            Some(Reached {
                key,
                from,
                doc: node.get("document").cloned(),
            })
        })
        .collect()
}

/// Knowledge stored in Antfly. Obtain with [`Antfly::knowledge`].
pub struct Knowledge<'a> {
    pub(crate) antfly: &'a Antfly,
    pub(crate) config: &'a crate::KnowledgeConfig,
}

impl Knowledge<'_> {
    /// The concept graph serving `table`: the configured version, or the
    /// older one it is replacing until the replacement swaps in.
    async fn graph_for(&self, table: &'static str) -> String {
        let wanted = concept_graph_name(&self.config.extract_model);
        match self.antfly.documents(table).serving_graph(&wanted).await {
            Ok(name) => name,
            Err(err) => {
                tracing::warn!("antfly: serving graph lookup failed: {err}");
                wanted
            }
        }
    }

    /// Every claim of `thread_id` from the per-turn pipeline, in turn order.
    pub async fn thread_claims(&self, thread_id: &str) -> AntflyResult<Vec<Claim>> {
        Ok(self
            .antfly
            .documents(schema::KNOWLEDGE)
            .scan(ScanRequest::prefix(&claim_prefix(
                TURN_CLAIMS_VERSION,
                thread_id,
            )))
            .await?
            .into_iter()
            .filter_map(|document| Claim::from_doc(&document.key, document.doc))
            .collect())
    }

    /// Records `batch` as provisional claims. Claims it replaces or retracts
    /// must be current claims of the same thread; other ids are ignored.
    /// Earlier stage-1 claims of the thread are dropped on its first batch.
    pub async fn append_turn_claims(&self, batch: TurnClaimBatch) -> AntflyResult<Vec<Claim>> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let existing = self.thread_claims(&batch.thread_id).await?;
        let mut writes = Vec::new();
        if existing.is_empty() {
            for version in CLAIM_VERSIONS
                .iter()
                .filter(|version| **version != TURN_CLAIMS_VERSION)
            {
                for document in documents
                    .scan(ScanRequest::prefix(&claim_prefix(
                        version,
                        &batch.thread_id,
                    )))
                    .await?
                {
                    writes.push(Write::delete(document.key));
                }
            }
        }
        let thread_ids: BTreeSet<String> = existing.iter().map(|claim| claim.id.clone()).collect();
        let mut current: BTreeMap<String, Claim> = existing
            .into_iter()
            .filter(|claim| claim.status == ClaimStatus::Current)
            .map(|claim| (claim.id.clone(), claim))
            .collect();
        let prefix = claim_prefix(TURN_CLAIMS_VERSION, &batch.thread_id);
        // Batch position → record index, and each record's unresolved links.
        let mut positions: Vec<Option<usize>> = Vec::with_capacity(batch.claims.len());
        let mut links: Vec<(Vec<ClaimRef>, Vec<ClaimRef>)> = Vec::new();
        let generated_at_ms = now_ms();
        let mut per_turn: BTreeMap<u64, usize> = BTreeMap::new();
        let mut records = Vec::with_capacity(batch.claims.len());
        let mut changed: BTreeMap<String, Claim> = BTreeMap::new();
        for claim in batch.claims {
            let statement = claim.statement.trim().to_string();
            if statement.is_empty() {
                positions.push(None);
                continue;
            }
            positions.push(Some(records.len()));
            links.push((claim.motivates, claim.rejected_in_favor_of));
            let index = per_turn.entry(claim.turn_ordinal).or_default();
            let id = format!(
                "{prefix}{}:{:03}",
                crate::keys::ordinal(claim.turn_ordinal),
                *index
            );
            *index += 1;
            let mut supersedes = Vec::new();
            for replaced in claim.replaces {
                if let Some(mut earlier) = current.remove(&replaced) {
                    earlier.status = ClaimStatus::Superseded;
                    earlier.superseded_by = Some(id.clone());
                    supersedes.push(earlier.id.clone());
                    changed.insert(earlier.id.clone(), earlier);
                }
            }
            records.push(Claim {
                id,
                kind: claim.kind,
                statement,
                status: ClaimStatus::Current,
                thread_id: batch.thread_id.clone(),
                version: TURN_CLAIMS_VERSION.to_string(),
                cwd: batch.cwd.clone(),
                source_updated_at_ms: batch.source_updated_at_ms,
                generated_at_ms,
                supersedes,
                superseded_by: None,
                motivates: Vec::new(),
                rejected_in_favor_of: Vec::new(),
                turn_id: claim.turn_id,
                turn_ordinal: Some(claim.turn_ordinal),
                settled: false,
            });
        }
        let resolve = |records: &[Claim], own: usize, refs: Vec<ClaimRef>| -> Vec<String> {
            let mut ids = Vec::new();
            for target in refs {
                let id = match target {
                    ClaimRef::Existing(id) if thread_ids.contains(&id) => Some(id),
                    ClaimRef::Existing(_) => None,
                    ClaimRef::New(position) => positions
                        .get(position)
                        .copied()
                        .flatten()
                        .filter(|index| *index != own)
                        .map(|index| records[index].id.clone()),
                };
                if let Some(id) = id
                    && !ids.contains(&id)
                {
                    ids.push(id);
                }
            }
            ids
        };
        for (index, (motivates, rejected)) in links.into_iter().enumerate() {
            let motivates = resolve(&records, index, motivates);
            let rejected = resolve(&records, index, rejected);
            records[index].motivates = motivates;
            records[index].rejected_in_favor_of = rejected;
        }
        for retracted in batch.retracts {
            if let Some(mut earlier) = current.remove(&retracted) {
                earlier.status = ClaimStatus::Retracted;
                changed.insert(earlier.id.clone(), earlier);
            }
        }
        for claim in changed.values().chain(records.iter()) {
            writes.push(Write::put(claim.id.clone(), claim.to_doc()?));
        }
        if !writes.is_empty() {
            let _guard = self.antfly.lock().await;
            documents.write(writes).await?;
        }
        Ok(records)
    }

    /// Settles `thread_id`'s provisional claims: each current one is checked
    /// against similar settled claims from other threads, which it may
    /// supersede, and then marked settled. Supersession failures are logged
    /// and never fail settling. Returns the number of claims settled.
    pub async fn settle_thread(&self, thread_id: &str) -> AntflyResult<usize> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let pending: Vec<Claim> = self
            .thread_claims(thread_id)
            .await?
            .into_iter()
            .filter(|claim| !claim.settled)
            .collect();
        if pending.is_empty() {
            return Ok(0);
        }
        let mut superseded: BTreeMap<String, String> = BTreeMap::new();
        let mut writes = Vec::with_capacity(pending.len());
        for mut claim in pending.iter().cloned() {
            if claim.status == ClaimStatus::Current {
                match self
                    .supersede_candidates(thread_id, claim.kind, &claim.statement)
                    .await
                {
                    Ok(candidates) => {
                        for earlier in candidates {
                            if superseded.contains_key(&earlier.id) {
                                continue;
                            }
                            match self.supersedes(&earlier.statement, &claim.statement).await {
                                Ok(true) => {
                                    superseded.insert(earlier.id.clone(), claim.id.clone());
                                    claim.supersedes.push(earlier.id);
                                }
                                Ok(false) => {}
                                Err(err) => {
                                    tracing::warn!("antfly: supersession check failed: {err}")
                                }
                            }
                        }
                    }
                    Err(err) => tracing::warn!("antfly: supersession search failed: {err}"),
                }
            }
            claim.settled = true;
            writes.push(Write::put(claim.id.clone(), claim.to_doc()?));
        }
        let _guard = self.antfly.lock().await;
        for (earlier_id, later_id) in &superseded {
            let Some(doc) = documents.get(earlier_id.clone()).await? else {
                continue;
            };
            let Some(mut earlier) = Claim::from_doc(earlier_id, doc) else {
                continue;
            };
            earlier.status = ClaimStatus::Superseded;
            earlier.superseded_by = Some(later_id.clone());
            writes.push(Write::put(earlier.id.clone(), earlier.to_doc()?));
        }
        documents.write(writes).await?;
        Ok(pending.len())
    }

    /// Deletes every claim distilled from `thread_id`.
    pub async fn delete_thread_claims(&self, thread_id: &str) -> AntflyResult<()> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let mut deletes = Vec::new();
        for version in CLAIM_VERSIONS {
            for document in documents
                .scan(ScanRequest::prefix(&claim_prefix(version, thread_id)))
                .await?
            {
                deletes.push(Write::delete(document.key));
            }
        }
        documents.write(deletes).await
    }

    /// Deletes every claim.
    pub async fn clear(&self) -> AntflyResult<()> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let mut deletes = Vec::new();
        for version in CLAIM_VERSIONS {
            let prefix = format!("{}:", crate::keys::escape(version));
            for document in documents.scan(ScanRequest::prefix(&prefix)).await? {
                deletes.push(Write::delete(document.key));
            }
        }
        documents.write(deletes).await
    }

    /// Counts of per-turn claims by state.
    pub async fn claim_counts(&self) -> AntflyResult<ClaimCounts> {
        let mut counts = ClaimCounts::default();
        for document in self
            .antfly
            .documents(schema::KNOWLEDGE)
            .scan(ScanRequest::prefix(&format!(
                "{}:",
                crate::keys::escape(TURN_CLAIMS_VERSION)
            )))
            .await?
        {
            let Some(claim) = Claim::from_doc(&document.key, document.doc) else {
                continue;
            };
            match claim.status {
                ClaimStatus::Current if claim.settled => counts.current += 1,
                ClaimStatus::Current => counts.provisional += 1,
                ClaimStatus::Superseded => counts.superseded += 1,
                ClaimStatus::Retracted => counts.retracted += 1,
            }
        }
        Ok(counts)
    }

    /// Every promoted concept.
    pub async fn concepts(&self) -> AntflyResult<Vec<Concept>> {
        Ok(self
            .antfly
            .documents(schema::CONCEPTS)
            .scan(ScanRequest::prefix(CONCEPT_PREFIX))
            .await?
            .into_iter()
            .filter_map(|document| concept_from(document.key, &document.doc))
            .collect())
    }

    /// Concepts whose names are semantically nearest to `name`, with their
    /// similarity scores, most similar first.
    pub async fn concept_neighbors(
        &self,
        name: &str,
        limit: usize,
    ) -> AntflyResult<Vec<(Concept, f64)>> {
        Ok(self
            .antfly
            .documents(schema::CONCEPTS)
            .nearest(name, limit)
            .await?
            .into_iter()
            .filter_map(|hit| {
                let score = hit.score;
                concept_from(hit.key, hit.doc.as_ref()?).map(|concept| (concept, score))
            })
            .collect())
    }

    /// Merges concept `from` into `to` in every table with a concept graph,
    /// so later and replayed mentions resolve to `to`. `false` when the
    /// backend cannot merge.
    pub async fn merge_concept(&self, from: &str, to: &str) -> AntflyResult<bool> {
        let mut merged = true;
        for table in [schema::KNOWLEDGE, schema::MEMORY_NOTES] {
            merged &= self
                .antfly
                .documents(table)
                .merge_resolver_keys(CONCEPT_RESOLVER, from, to)
                .await?;
        }
        Ok(merged)
    }

    /// The concept key the resolver gives `name`, following aliases and
    /// merges. `None` when the backend cannot render keys.
    pub async fn concept_key_for(&self, name: &str) -> AntflyResult<Option<String>> {
        self.antfly
            .documents(schema::KNOWLEDGE)
            .render_resolver_key(CONCEPT_RESOLVER, json!({"text": name, "label": "concept"}))
            .await
    }

    /// Keys of concepts the consolidation pass has already examined.
    pub async fn reviewed_concepts(&self) -> AntflyResult<BTreeSet<String>> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_all("SELECT key FROM codex_concept_reviews", vec![])
            .await?
            .iter()
            .filter_map(|row| row.string("key").ok())
            .collect())
    }

    /// An examined, unmerged concept with the same lexical identity as
    /// `concept`, other than `concept` itself.
    pub async fn lexical_twin(&self, concept: &Concept) -> AntflyResult<Option<String>> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_optional(
                "SELECT key FROM codex_concept_reviews \
                 WHERE identity = $1 AND merged_into IS NULL AND key <> $2 \
                 ORDER BY key LIMIT 1",
                crate::sql_params![concept_identity(&concept.name), concept.key.clone()],
            )
            .await?
            .and_then(|row| row.string("key").ok()))
    }

    /// Concepts consolidation merged into another.
    pub async fn merged_concept_count(&self) -> AntflyResult<u64> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_optional(
                "SELECT COUNT(*) AS n FROM codex_concept_reviews WHERE merged_into IS NOT NULL",
                vec![],
            )
            .await?
            .and_then(|row| row.i64("n").ok())
            .and_then(|n| u64::try_from(n).ok())
            .unwrap_or_default())
    }

    /// Whether `key` has been examined and was not merged away.
    pub async fn is_canonical_concept(&self, key: &str) -> AntflyResult<bool> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_optional(
                "SELECT key FROM codex_concept_reviews WHERE key = $1 AND merged_into IS NULL",
                crate::sql_params![key],
            )
            .await?
            .is_some())
    }

    /// Records that `concept` was examined, and what it was merged into.
    pub async fn record_concept_review(
        &self,
        concept: &Concept,
        merged_into: Option<&str>,
    ) -> AntflyResult<()> {
        let sql = self.antfly.sql().await?;
        sql.execute(
            "INSERT INTO codex_concept_reviews (key, identity, merged_into, reviewed_at_ms) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (key) DO UPDATE SET \
             identity = excluded.identity, merged_into = excluded.merged_into, \
             reviewed_at_ms = excluded.reviewed_at_ms",
            crate::sql_params![
                concept.key.clone(),
                concept_identity(&concept.name),
                merged_into.map(str::to_string),
                now_ms()
            ],
        )
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> AntflyResult<Option<Claim>> {
        Ok(self
            .antfly
            .documents(schema::KNOWLEDGE)
            .get(id.to_string())
            .await?
            .and_then(|doc| Claim::from_doc(id, doc)))
    }

    /// Current, settled claims of `kind` from threads other than
    /// `thread_id` that are most similar to `statement`.
    async fn supersede_candidates(
        &self,
        thread_id: &str,
        kind: ClaimKind,
        statement: &str,
    ) -> AntflyResult<Vec<Claim>> {
        let filter = json!({"conjuncts": [
            {"term": {"status": "current"}},
            {"term": {"kind": kind.as_str()}},
        ]});
        let hits = self
            .antfly
            .documents(schema::KNOWLEDGE)
            .search_text(
                statement,
                Some(filter),
                SUPERSEDE_CANDIDATES * 3,
                SUPERSEDE_CANDIDATES * 3,
            )
            .await?;
        Ok(hits
            .into_iter()
            .filter_map(|hit| Claim::from_doc(&hit.key, hit.doc?))
            .filter(|claim| claim.thread_id != thread_id && claim.settled)
            .take(SUPERSEDE_CANDIDATES)
            .collect())
    }

    /// Whether `later` should replace `earlier`: it restates the same fact,
    /// or replaces, reverses or updates it. Identical statements (ignoring
    /// case, spacing and a final period) need no model. Otherwise one decide
    /// call asks two questions, and either is enough at the threshold: the
    /// "replaces, reverses or updates" predicate (which catches a changed
    /// choice that Laya otherwise reads as a different fact), and a relation
    /// choice where "same" plus "updates" must win (which catches
    /// restatements the predicate misses).
    pub async fn supersedes(&self, earlier: &str, later: &str) -> AntflyResult<bool> {
        if normalized(earlier) == normalized(later) {
            return Ok(true);
        }
        let request = json!({
            "model": self.antfly.config().decide_model,
            "input": format!(
                "Earlier statement: {}\nLater statement: {}",
                truncate_chars(earlier, DECIDE_STATEMENT_MAX_CHARS),
                truncate_chars(later, DECIDE_STATEMENT_MAX_CHARS),
            ),
            "questions": [{
                "name": "relation",
                "type": "choice",
                "instructions": "How does the later statement relate to the earlier statement?",
                "choices": [
                    {"value": "same", "description": "Asserts the same fact about the same subject, possibly in other words"},
                    {"value": "updates", "description": "Replaces, reverses or updates the earlier statement about the same subject"},
                    {"value": "different", "description": "Asserts a different fact about the same subject"},
                    {"value": "unrelated", "description": "Is about a different subject"},
                ],
            }, {
                "name": "supersedes",
                "type": "predicate",
                "instructions": "The later statement replaces, reverses or updates the earlier statement about the same subject.",
            }],
        });
        let response = self.antfly.decide(&request).await?;
        let malformed = || crate::AntflyError::Malformed(format!("decide response: {response}"));
        let probabilities = relation_probabilities(&response).ok_or_else(malformed)?;
        let predicate = predicate_probability(&response, "supersedes").ok_or_else(malformed)?;
        let threshold = self.config.supersede_threshold_bp;
        let supersedes = predicate * 10_000.0 >= f64::from(threshold)
            || replaces_earlier(&probabilities, threshold);
        let input = request["input"].as_str().unwrap_or_default();
        if let Err(err) = crate::labels::record_decision(
            self.antfly,
            crate::labels::DecisionRecord {
                kind: "supersession",
                model: &self.antfly.config().decide_model,
                subject: None,
                input,
                response: &response,
                result: Some(supersedes),
                threshold_bp: Some(threshold),
            },
        )
        .await
        {
            tracing::debug!("failed to record supersession decision: {err}");
        }
        Ok(supersedes)
    }

    /// Canonical concept keys for the mentions in `text`. Antfly's
    /// query-time resolution runs the concept graph's own extractor and
    /// resolver (aliases, merges), so keys match write time exactly; backends
    /// without it fall back to local GLiNER extraction plus `concept_key`.
    pub async fn extract_concepts(&self, text: &str) -> Vec<String> {
        let request = json!({"text": truncate_bytes(text, QUERY_RESOLVE_MAX_BYTES)});
        match self
            .antfly
            .documents(schema::KNOWLEDGE)
            .resolve_query(CONCEPT_RESOLVER, request)
            .await
        {
            Ok(Some(response)) => return resolved_keys(&response),
            Ok(None) => {}
            Err(err) => {
                tracing::warn!("antfly: query resolution failed, extracting locally: {err}")
            }
        }
        self.extract_concepts_locally(text).await
    }

    /// Local GLiNER extraction (truncated to the single-window profile) with
    /// keys slugged like the resolver's `key_template`. Empty when extraction
    /// is unavailable.
    async fn extract_concepts_locally(&self, text: &str) -> Vec<String> {
        let request = json!({
            "model": self.config.extract_model,
            "schema": {"entities": CONCEPT_ENTITIES},
            "inputs": [{"content": truncate_bytes(text, QUERY_EXTRACT_MAX_BYTES)}],
            "options": {"include_confidence": true},
        });
        let response = match self.antfly.extract(&request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("antfly: concept extraction failed: {err}");
                return Vec::new();
            }
        };
        let mut keys = BTreeSet::new();
        for item in response
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            for entity in item
                .get("entities")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let score = entity.get("score").and_then(Value::as_f64).unwrap_or(1.0);
                if score < 0.5 {
                    continue;
                }
                if let Some(key) = entity
                    .get("text")
                    .and_then(Value::as_str)
                    .and_then(concept_key)
                {
                    keys.insert(key);
                }
            }
        }
        keys.into_iter().collect()
    }

    /// Concepts each of `keys` is `about`, in `table`.
    async fn concepts_of(
        &self,
        table: &'static str,
        keys: &[String],
    ) -> HashMap<String, Vec<String>> {
        let mut out: HashMap<String, Vec<String>> = HashMap::new();
        if keys.is_empty() {
            return out;
        }
        let index = self.graph_for(table).await;
        let request = json!({
            "graph_queries": {"about": {"index": index, "traverse": {
                "start": {"keys": keys},
                "direction": "out", "edge_types": [ABOUT_EDGE], "max_depth": 1,
                "include_paths": true, "limit": 1000,
            }}},
            "limit": 1,
        });
        match self.antfly.documents(table).query(request).await {
            Ok(response) => {
                for node in reached_nodes(&response, "about") {
                    out.entry(node.from).or_default().push(node.key);
                }
            }
            Err(err) => tracing::warn!("antfly: concept lookup failed: {err}"),
        }
        out
    }

    /// Documents in `table` that are `about` any of `concepts`, with every
    /// concept each shares: `deduplicate_nodes: false` returns one path per
    /// (concept, document) pair.
    async fn about_any(
        &self,
        table: &'static str,
        concepts: &[String],
        limit: usize,
    ) -> Vec<(String, Value, Vec<String>)> {
        if concepts.is_empty() {
            return Vec::new();
        }
        let index = self.graph_for(table).await;
        let request = json!({
            "graph_queries": {"docs": {"index": index, "traverse": {
                "start": {"keys": concepts},
                "direction": "in", "edge_types": [ABOUT_EDGE], "max_depth": 1,
                "include_documents": true, "include_paths": true,
                "deduplicate_nodes": false, "limit": limit,
            }}},
            "limit": 1,
        });
        let response = match self.antfly.documents(table).query(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("antfly: concept traversal failed: {err}");
                return Vec::new();
            }
        };
        let mut found: BTreeMap<String, (Option<Value>, BTreeSet<String>)> = BTreeMap::new();
        for node in reached_nodes(&response, "docs") {
            let entry = found.entry(node.key).or_default();
            if entry.0.is_none() {
                entry.0 = node.doc;
            }
            entry.1.insert(node.from);
        }
        found
            .into_iter()
            .filter_map(|(key, (doc, shared))| Some((key, doc?, shared.into_iter().collect())))
            .collect()
    }

    /// Fills each claim's rationale and rejected alternatives from the
    /// claim links graph. Failures leave them empty.
    async fn attach_links(&self, claims: &mut [RelatedClaim]) {
        let keys: Vec<String> = claims.iter().map(|found| found.claim.id.clone()).collect();
        if keys.is_empty() {
            return;
        }
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let index = match documents.serving_graph(CLAIM_LINKS_INDEX).await {
            Ok(index) => index,
            Err(err) => {
                tracing::warn!("antfly: claim links lookup failed: {err}");
                return;
            }
        };
        let request = json!({
            "graph_queries": {"links": {"index": index, "traverse": {
                "start": {"keys": keys},
                "direction": "in",
                "edge_types": [MOTIVATES_EDGE, REJECTED_IN_FAVOR_OF_EDGE],
                "max_depth": 1,
                "include_documents": true, "include_paths": true,
                "deduplicate_nodes": false, "limit": 200,
            }}},
            "limit": 1,
        });
        let response = match documents.query(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("antfly: claim links traversal failed: {err}");
                return;
            }
        };
        for node in reached_nodes(&response, "links") {
            let Some(linked) = node.doc.and_then(|doc| Claim::from_doc(&node.key, doc)) else {
                continue;
            };
            if linked.status != ClaimStatus::Current {
                continue;
            }
            let Some(target) = claims.iter_mut().find(|found| found.claim.id == node.from) else {
                continue;
            };
            let list = if linked.rejected_in_favor_of.contains(&node.from) {
                &mut target.alternatives
            } else if linked.motivates.contains(&node.from) {
                &mut target.motivated_by
            } else {
                continue;
            };
            if !list.iter().any(|claim| claim.id == linked.id) {
                list.push(linked);
            }
        }
    }

    /// Personalized PageRank scores in `table`, seeded at `seeds` and
    /// walking edges both ways. Empty when the metric is unavailable.
    async fn ppr_scores(
        &self,
        table: &'static str,
        seeds: &[String],
        top_k: usize,
    ) -> HashMap<String, f64> {
        if seeds.is_empty() {
            return HashMap::new();
        }
        let index = self.graph_for(table).await;
        let request = json!({
            "graph_metric": {
                "index": index,
                "metric": PPR_METRIC,
                "metric_freshness": "fresh",
                "seed_nodes": &seeds[..seeds.len().min(MAX_PPR_SEEDS)],
                "direction": "both",
                "top_k": top_k,
            },
            "limit": 1,
        });
        let response = match self.antfly.documents(table).query(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("antfly: personalized PageRank failed: {err}");
                return HashMap::new();
            }
        };
        response
            .get("responses")
            .and_then(Value::as_array)
            .and_then(|responses| responses.first())
            .unwrap_or(&response)
            .pointer(&format!("/graph_metric_results/{PPR_METRIC}/scores"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let node = entry.get("node")?.as_str()?;
                let score = entry.get("score")?.as_f64()?;
                (!node.starts_with(CONCEPT_PREFIX) && score > 0.0)
                    .then(|| (node.to_string(), score))
            })
            .collect()
    }

    /// The local generator that describes concepts, when descriptions are on.
    pub fn describe_model(&self) -> Option<&str> {
        self.config.describe_model.as_deref()
    }

    /// Statements of the current claims about the concept `key`, settled
    /// claims first, then the newest.
    pub async fn concept_evidence(&self, key: &str, limit: usize) -> Vec<String> {
        let mut claims: Vec<Claim> = self
            .about_any(schema::KNOWLEDGE, &[key.to_string()], limit * 4)
            .await
            .into_iter()
            .filter_map(|(id, doc, _)| Claim::from_doc(&id, doc))
            .filter(|claim| claim.status == ClaimStatus::Current)
            .collect();
        claims.sort_by(|a, b| {
            b.settled
                .cmp(&a.settled)
                .then(b.source_updated_at_ms.cmp(&a.source_updated_at_ms))
                .then(a.id.cmp(&b.id))
        });
        claims
            .into_iter()
            .take(limit)
            .map(|claim| claim.statement)
            .collect()
    }

    /// Descriptions of the concepts in `keys` that have one.
    pub async fn concept_descriptions(
        &self,
        keys: &[String],
    ) -> AntflyResult<HashMap<String, String>> {
        let table = self.antfly.documents(schema::CONCEPT_DESCRIPTIONS);
        let mut found = HashMap::new();
        for key in keys {
            if let Some(description) = table
                .get(key)
                .await?
                .and_then(|doc| doc.get("description")?.as_str().map(str::to_string))
            {
                found.insert(key.clone(), description);
            }
        }
        Ok(found)
    }

    /// Writes or refreshes descriptions for up to `max` concepts whose
    /// evidence changed since they were last described. Concepts merged into
    /// another are skipped, and so are concepts no current claim is about.
    pub async fn describe_concepts(&self, max: usize) -> AntflyResult<DescribeReport> {
        let mut report = DescribeReport::default();
        let Some(model) = self.config.describe_model.clone() else {
            return Ok(report);
        };
        let merged: BTreeSet<String> = self
            .antfly
            .sql()
            .await?
            .fetch_all(
                "SELECT key FROM codex_concept_reviews WHERE merged_into IS NOT NULL",
                vec![],
            )
            .await?
            .iter()
            .filter_map(|row| row.string("key").ok())
            .collect();
        let table = self.antfly.documents(schema::CONCEPT_DESCRIPTIONS);
        for concept in self.concepts().await? {
            if report.described >= max {
                break;
            }
            if merged.contains(&concept.key) {
                continue;
            }
            report.examined += 1;
            let evidence = self.concept_evidence(&concept.key, DESCRIBE_EVIDENCE).await;
            if evidence.is_empty() {
                report.without_evidence += 1;
                continue;
            }
            let hash = evidence_hash(&model, &concept, &evidence);
            let existing = table.get(&concept.key).await?;
            if existing
                .as_ref()
                .and_then(|doc| doc.get("evidence_hash")?.as_str())
                == Some(hash.as_str())
            {
                report.unchanged += 1;
                continue;
            }
            let request = describe_request(&model, &concept, &evidence);
            let description = match self.antfly.generate(&request).await {
                Ok(response) => generated_text(&response),
                Err(err) => {
                    tracing::warn!("antfly: describing {} failed: {err}", concept.key);
                    None
                }
            };
            let Some(description) = description else {
                report.failed += 1;
                continue;
            };
            let description = truncate_chars(&description, DESCRIPTION_MAX_CHARS);
            table
                .write(vec![Write::put(
                    concept.key.clone(),
                    json!({
                        "canonical_name": concept.name,
                        "description": description,
                        "search_text": format!("{}: {description}", concept.name),
                        "evidence_hash": hash,
                        "model": model,
                        "described_at_ms": now_ms(),
                    }),
                )])
                .await?;
            report.described += 1;
        }
        Ok(report)
    }

    /// Current claims and memory notes related to `query`: seeded by the
    /// concepts in the query and in its direct search hits, then expanded
    /// through every document sharing those concepts. Superseded claims are
    /// left out unless `include_superseded`.
    pub async fn related(
        &self,
        query: &str,
        limit: usize,
        include_superseded: bool,
    ) -> AntflyResult<Related> {
        let claims_table = self.antfly.documents(schema::KNOWLEDGE);
        let mut seeds: BTreeSet<String> = self.extract_concepts(query).await.into_iter().collect();
        let filter = (!include_superseded).then(|| json!({"term": {"status": "current"}}));
        let hits = claims_table
            .search_text(query, filter, limit, limit)
            .await?;
        let hit_keys: Vec<String> = hits.iter().map(|hit| hit.key.clone()).collect();
        for concepts in self
            .concepts_of(schema::KNOWLEDGE, &hit_keys)
            .await
            .into_values()
        {
            seeds.extend(concepts);
        }
        let seeds: Vec<String> = seeds.into_iter().collect();
        let keep = |claim: &Claim| include_superseded || claim.status == ClaimStatus::Current;

        // Candidates: direct hits, documents one `about` hop from a seed, and
        // anything personalized PageRank reaches further out.
        let mut candidates: HashMap<String, (Claim, Vec<String>, f64)> = HashMap::new();
        for (rank, hit) in hits.into_iter().enumerate() {
            let Some(claim) = hit.doc.and_then(|doc| Claim::from_doc(&hit.key, doc)) else {
                continue;
            };
            if keep(&claim) {
                candidates.insert(hit.key, (claim, Vec::new(), 1.0 / (rank as f64 + 1.0)));
            }
        }
        for (key, doc, shared) in self.about_any(schema::KNOWLEDGE, &seeds, limit * 4).await {
            let Some(claim) = Claim::from_doc(&key, doc) else {
                continue;
            };
            if keep(&claim) {
                candidates
                    .entry(key)
                    .or_insert_with(|| (claim, Vec::new(), 0.0))
                    .1 = shared;
            }
        }
        let ppr = self.ppr_scores(schema::KNOWLEDGE, &seeds, limit * 8).await;
        for key in ppr.keys() {
            if candidates.contains_key(key) {
                continue;
            }
            if let Some(claim) = self.get(key).await?
                && keep(&claim)
            {
                candidates.insert(key.clone(), (claim, Vec::new(), 0.0));
            }
        }
        let mut claims: Vec<RelatedClaim> = candidates
            .into_iter()
            .map(|(key, (claim, concepts, direct))| {
                let weight = if claim.settled {
                    1.0
                } else {
                    PROVISIONAL_WEIGHT
                };
                RelatedClaim {
                    score: weight * rank_score(&ppr, &key, concepts.len(), direct),
                    claim,
                    concepts,
                    motivated_by: Vec::new(),
                    alternatives: Vec::new(),
                }
            })
            .collect();
        claims.sort_by(|a, b| {
            b.score.total_cmp(&a.score).then(
                b.claim
                    .source_updated_at_ms
                    .cmp(&a.claim.source_updated_at_ms),
            )
        });
        claims.truncate(limit);
        self.attach_links(&mut claims).await;

        let note_ppr = self
            .ppr_scores(schema::MEMORY_NOTES, &seeds, limit * 8)
            .await;
        let mut notes: Vec<RelatedNote> = self
            .about_any(schema::MEMORY_NOTES, &seeds, limit * 4)
            .await
            .into_iter()
            .filter_map(|(key, doc, shared)| {
                Some(RelatedNote {
                    namespace: doc.get("namespace")?.as_str()?.to_string(),
                    path: doc.get("path")?.as_str()?.to_string(),
                    score: rank_score(&note_ppr, &key, shared.len(), 0.0),
                    concepts: shared,
                })
            })
            .collect();
        notes.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.path.cmp(&b.path)));
        notes.truncate(limit);
        Ok(Related {
            seeds,
            claims,
            notes,
        })
    }
}

/// What [`Knowledge::describe_concepts`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeReport {
    /// Concepts considered (merged concepts are not).
    pub examined: usize,
    pub described: usize,
    /// Concepts whose evidence had not changed since their description.
    pub unchanged: usize,
    /// Concepts no current claim is about.
    pub without_evidence: usize,
    pub failed: usize,
}

/// Identity of what a description was written from, so it is rewritten
/// only when the model, the name or the evidence changes.
fn evidence_hash(model: &str, concept: &Concept, evidence: &[String]) -> String {
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    model.hash(&mut hasher);
    concept.name.hash(&mut hasher);
    concept.entity_type.hash(&mut hasher);
    evidence.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The generate request that describes `concept` from `evidence`.
fn describe_request(model: &str, concept: &Concept, evidence: &[String]) -> Value {
    let label = concept
        .entity_type
        .as_deref()
        .map(|label| format!(" ({label})"))
        .unwrap_or_default();
    let facts = evidence
        .iter()
        .map(|statement| {
            format!(
                "- {}",
                truncate_chars(statement, DECIDE_STATEMENT_MAX_CHARS)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    json!({
        "model": model,
        "max_tokens": 160,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": DESCRIBE_INSTRUCTIONS},
            {"role": "user", "content": format!("Concept: {}{label}\nFacts:\n{facts}", concept.name)},
        ],
    })
}

/// The text of a generate response, without surrounding quotes.
fn generated_text(response: &Value) -> Option<String> {
    let text = response
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)?
        .trim()
        .trim_matches('"')
        .trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// `text` lowercased with whitespace collapsed and a final period dropped.
fn normalized(text: &str) -> String {
    let collapsed = text
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    collapsed.trim_end_matches('.').to_string()
}

/// Choice probabilities of the decide response's `relation` answer.
fn relation_probabilities(response: &Value) -> Option<HashMap<String, f64>> {
    let answer = response
        .get("answers")?
        .as_array()?
        .iter()
        .find(|answer| answer.get("name").and_then(Value::as_str) == Some("relation"))?;
    answer
        .get("probabilities")?
        .as_array()?
        .iter()
        .map(|entry| {
            Some((
                entry.get("value")?.as_str()?.to_string(),
                entry.get("probability")?.as_f64()?,
            ))
        })
        .collect()
}

fn predicate_probability(response: &Value, name: &str) -> Option<f64> {
    response
        .get("answers")?
        .as_array()?
        .iter()
        .find(|answer| answer.get("name").and_then(Value::as_str) == Some(name))?
        .get("probability")?
        .as_f64()
}

fn replaces_earlier(probabilities: &HashMap<String, f64>, threshold_bp: u32) -> bool {
    let p = |value: &str| probabilities.get(value).copied().unwrap_or_default();
    let top = probabilities
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(value, _)| value.as_str());
    let replacing = p("same") + p("updates");
    matches!(top, Some("same" | "updates")) && replacing * 10_000.0 >= f64::from(threshold_bp)
}

fn concept_from(key: String, doc: &Value) -> Option<Concept> {
    let name = doc.get("canonical_name")?.as_str()?.to_string();
    Some(Concept {
        key,
        name,
        entity_type: doc
            .get("entity_type")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Personalized PageRank when it ran, else the count of shared concepts;
/// direct search hits get a small boost either way.
fn rank_score(ppr: &HashMap<String, f64>, key: &str, shared: usize, direct: f64) -> f64 {
    let graph = if ppr.is_empty() {
        shared as f64
    } else {
        ppr.get(key).copied().unwrap_or_default()
    };
    graph + 0.1 * direct
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn concept_keys_match_antfly_slug() {
        assert_eq!(
            concept_key("EmbeddingGemma 2").as_deref(),
            Some("concept/embeddinggemma_2")
        );
        assert_eq!(
            concept_key("  Codex's approval reviewer! ").as_deref(),
            Some("concept/codex_approval_reviewer")
        );
        assert_eq!(
            concept_key("Antfly’s pgwire").as_deref(),
            Some("concept/antfly_pgwire")
        );
        assert_eq!(concept_key("it's").as_deref(), Some("concept/it"));
        assert_eq!(
            concept_key("rock'n'roll").as_deref(),
            Some("concept/rock_n_roll")
        );
        assert_eq!(concept_key("—"), None);
    }

    #[test]
    fn resolved_keys_follow_canonical_doc_refs() {
        let response = json!({"resolution": {"entities": [
            {"local_id": "0", "doc_ref": {"table": "codex_concepts", "key": "concept/qwen3_embedding"}, "decision": "match"},
            {"local_id": "1", "doc_ref": {"table": "codex_concepts", "key": "concept/antfly"}, "decision": "new"},
            {"local_id": "2", "doc_ref": {"table": "codex_concepts", "key": "concept/antfly"}, "decision": "new"},
            {"local_id": "3"},
        ]}});
        assert_eq!(
            resolved_keys(&response),
            vec!["concept/qwen3_embedding", "concept/antfly"]
        );
        assert!(resolved_keys(&json!({})).is_empty());
    }

    #[test]
    fn restatements_and_updates_replace_earlier_claims() {
        assert_eq!(
            normalized("  The importer writes 1,024 items  per batch. "),
            normalized("the importer writes 1,024 items per batch")
        );
        let response = |same: f64, updates: f64, different: f64| {
            json!({"answers": [{"name": "relation", "probabilities": [
                {"value": "same", "probability": same},
                {"value": "updates", "probability": updates},
                {"value": "different", "probability": different},
                {"value": "unrelated", "probability": 1.0 - same - updates - different},
            ]}]})
        };
        let decide =
            |value: Value| relation_probabilities(&value).map(|p| replaces_earlier(&p, 8_000));
        assert_eq!(decide(response(0.85, 0.05, 0.05)), Some(true));
        assert_eq!(decide(response(0.10, 0.75, 0.10)), Some(true));
        // Mass split evenly is not confident enough.
        assert_eq!(decide(response(0.35, 0.30, 0.30)), Some(false));
        // A different fact about the same subject never replaces.
        assert_eq!(decide(response(0.30, 0.25, 0.42)), Some(false));
        assert_eq!(decide(json!({"answers": []})), None);
    }

    #[test]
    fn lexical_identity_and_numbers_separate_variants_from_versions() {
        for (a, b) in [
            ("ThreadStore", "thread store"),
            ("approvals reviewer", "approval reviewer"),
            ("app-server", "app server"),
            ("knowledge-graph", "Knowledge Graph"),
            ("agents", "agent"),
        ] {
            assert_eq!(concept_identity(a), concept_identity(b), "{a} ~ {b}");
        }
        for (a, b) in [
            ("Qwen3", "Qwen2"),
            ("EmbeddingGemma 2", "EmbeddingGemma"),
            ("Antfly", "Antfarm"),
            ("process", "proces"),
            ("status", "stat"),
        ] {
            assert_ne!(concept_identity(a), concept_identity(b), "{a} ~ {b}");
        }
        assert_eq!(number_tokens("GLiNER 2.5"), vec!["2", "5"]);
        assert_eq!(number_tokens("Qwen3 Embedding"), vec!["3"]);
        assert!(number_tokens("Antfly").is_empty());
    }

    #[test]
    fn concept_graph_name_tracks_model() {
        assert_ne!(
            concept_graph_name("fastino/gliner2.5-base-v1"),
            concept_graph_name("fastino/GLiNER2.5-multi-Decide")
        );
        assert!(concept_graph_name("m").starts_with("codex_concepts_"));
    }
}
