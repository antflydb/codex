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
/// Excerpts from memory notes added to a concept's evidence when claims
/// leave room, and the longest each may be.
const NOTE_EXCERPTS: usize = 3;
const NOTE_EXCERPT_MAX_CHARS: usize = 300;
/// Longest description kept, in characters.
const DESCRIPTION_MAX_CHARS: usize = 600;
/// The facts mention the concept but are mostly about other things, and a
/// small model readily moves their properties onto it ("Antfly is half the
/// size of Qwen3" from a claim about EmbeddingGemma); the instructions say
/// to describe the concept itself and attribute each fact correctly.
const DESCRIBE_INSTRUCTIONS: &str = "You describe one concept from a software project's \
knowledge graph. The facts are statements that mention the concept; most are about other \
things too. Write one or two plain sentences about the concept itself: first what kind of thing \
it is, then its role in the project. Attribute each fact to the thing it is actually about; \
never move a property of another concept onto this one. Use only the facts given. If they do \
not say what the concept is, say only how it is used.";
/// Changes whenever [`DESCRIBE_INSTRUCTIONS`] or the request shape changes,
/// so existing descriptions are rewritten.
const DESCRIBE_VERSION: u32 = 2;
/// `version` of claims written by the per-turn pipeline. Claims from the
/// earlier stage-1 extraction used the memory version (`v1`, `v2`).
pub const TURN_CLAIMS_VERSION: &str = "turns";
/// Every claim key namespace, for deletion.
const CLAIM_VERSIONS: &[&str] = &[TURN_CLAIMS_VERSION, "v1", "v2"];
/// Provisional claims rank at this fraction of their score.
const PROVISIONAL_WEIGHT: f64 = 0.5;
/// Weight of claims reached only through a concept link (`is_a`,
/// `part_of`, `alternative_to`) from a seed, not about a seed itself.
const LINKED_WEIGHT: f64 = 0.5;

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
        enrichments: Vec::new(),
    }
}

/// Prefix of the history concept graph's index and enrichments.
pub const HISTORY_CONCEPTS_PREFIX: &str = "codex_history_concepts";
/// Bumped when the history concept graph's configuration changes.
const HISTORY_CONCEPTS_VERSION: u32 = 1;
/// Graph node of a thread, the source of its `about` edges to concepts.
pub const THREAD_NODE_PREFIX: &str = "thread/";

/// Name of the history concept graph index for `extract_model`.
pub fn history_concepts_graph_name(extract_model: &str) -> String {
    let hash = fnv1a(&format!(
        "{extract_model}:{CONCEPT_SCHEMA_VERSION}:{HISTORY_CONCEPTS_VERSION}"
    ));
    format!("{HISTORY_CONCEPTS_PREFIX}_{hash:016x}")
}

/// Concepts of conversation history, per turn: user and agent messages are
/// grouped by thread (an Antfly grouped source, ordered by ordinal and
/// segmented by turn), GLiNER extracts concepts from each turn, and the
/// concept resolver turns them into `about` edges from `thread/<id>` to the
/// same `concept/…` keys claims and notes use. Tool output is left out.
pub(crate) fn history_concepts_index(extract_model: &str) -> GraphIndex {
    let name = history_concepts_graph_name(extract_model);
    let turns = format!("{name}_turns");
    let extraction = format!("{name}_extraction");
    let extractor = json!({"type": "extractor", "config": {
        "provider": "antfly",
        "model": extract_model,
        "schema": {"entities": CONCEPT_ENTITIES, "relations": []},
        "options": {
            "include_confidence": true,
            "include_spans": true,
            "long_document": {"mode": "window"},
        },
    }});
    let enrichments = vec![
        json!({
            "name": turns,
            "kind": "group",
            "source": {
                "group_by": "thread_id",
                "order_by": "ordinal",
                "segment_by": "turn_id",
                "field": crate::SEARCH_TEXT_FIELD,
                "filter": {"term": {"speaker": ["user", "agent"]}},
            },
        }),
        json!({
            "name": extraction,
            "kind": "asset",
            "source_artifact_name": turns,
            "content_type": "application/json",
            "producer_json": extractor.to_string(),
        }),
    ];
    let config = json!({
        "source": {
            "artifact": extraction,
            "path": "$.relations[*]",
            "format": "extraction_relation",
            "mention_edge_type": ABOUT_EDGE,
            "nodes": {"source": format!("{THREAD_NODE_PREFIX}{{{{_doc.value._group_key}}}}")},
            "edge": {"type": "{{ _item.type }}", "edge_id": "{{_doc.key}}:{{_item.type}}"},
            "context": {"doc_fields": ["_group_key", "_segment_key"]},
        },
        "edge_types": [{"name": ABOUT_EDGE}],
        "resolvers": [{
            "name": CONCEPT_RESOLVER,
            "table": schema::CONCEPTS,
            "source_artifact": extraction,
            "resolution_artifact": format!("{name}_resolution"),
            "key_template": "concept/{{ uslug _entity.text }}",
            "min_confidence": 0.5,
            "config_generation": 1,
        }],
    });
    GraphIndex {
        name,
        config,
        replaces_prefix: HISTORY_CONCEPTS_PREFIX,
        enrichments,
    }
}

pub const CONCEPT_LINKS_INDEX: &str = "codex_concept_links_v1";
const CONCEPT_LINKS_PREFIX: &str = "codex_concept_links";
/// Edge kinds between concepts, each the name of a field on
/// [`schema::CONCEPT_LINKS`] documents holding target concept keys.
pub const CONCEPT_LINK_KINDS: &[&str] = &["is_a", "part_of", "alternative_to"];

pub(crate) fn concept_links_index() -> GraphIndex {
    GraphIndex {
        name: CONCEPT_LINKS_INDEX.to_string(),
        config: json!({"edge_types": CONCEPT_LINK_KINDS
            .iter()
            .map(|kind| json!({"name": kind, "field": kind}))
            .collect::<Vec<_>>()}),
        replaces_prefix: CONCEPT_LINKS_PREFIX,
        enrichments: Vec::new(),
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
        enrichments: Vec::new(),
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
    /// Concepts linked to a seed, whose claims were also considered.
    pub linked: Vec<String>,
    pub claims: Vec<RelatedClaim>,
    pub notes: Vec<RelatedNote>,
    /// Threads whose conversation is about a seed.
    pub threads: Vec<RelatedThread>,
}

/// A thread whose turns are about some of the searched concepts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RelatedThread {
    pub thread_id: String,
    /// The seeds its turns are about.
    pub concepts: Vec<String>,
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

/// A traversal start at concept nodes. They live in [`schema::CONCEPTS`],
/// not in the table being queried, so they are addressed by table-qualified
/// identity.
fn concept_nodes(concepts: &[String]) -> Value {
    json!({"identities": concepts
        .iter()
        .map(|key| json!({"table": schema::CONCEPTS, "key": key}))
        .collect::<Vec<_>>()})
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
        // One entity-level call reaches every resolver writing concepts (the
        // claims and memory-note graphs alike), validated before any applies.
        if self
            .curate_concept(from, json!({"action": "merge", "into": to}))
            .await?
            .is_some()
        {
            return Ok(true);
        }
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

    /// Curates concept `key` in every resolver that writes concepts. `None`
    /// when the backend cannot curate entities.
    async fn curate_concept(&self, key: &str, request: Value) -> AntflyResult<Option<Value>> {
        self.antfly
            .curate_entity(schema::CONCEPTS, key, request)
            .await
    }

    /// Undoes the merge of concept `key` (the key it had before the merge),
    /// so its mentions resolve to it again. With `pin_distinct`, it can never
    /// be merged into its former target again. `false` when the backend
    /// cannot curate.
    pub async fn unmerge_concept(&self, key: &str, pin_distinct: bool) -> AntflyResult<bool> {
        if self
            .curate_concept(
                key,
                json!({"action": "unmerge", "pin_distinct": pin_distinct}),
            )
            .await?
            .is_none()
        {
            return Ok(false);
        }
        let sql = self.antfly.sql().await?;
        sql.execute(
            "UPDATE codex_concept_reviews SET merged_into = NULL WHERE key = $1",
            crate::sql_params![key],
        )
        .await?;
        Ok(true)
    }

    /// Suppresses concept `key`: its mentions mint no concept and no `about`
    /// edge, and existing edges are removed on replay. Concepts merged into
    /// `key` are suppressed with it in the same commit (Antfly records them
    /// as a cascade from `key`); they are returned. `actor` and `reason` are
    /// kept as the suppression's provenance. `None` when the backend cannot
    /// curate.
    pub async fn suppress_concept(
        &self,
        key: &str,
        actor: &str,
        reason: Option<&str>,
    ) -> AntflyResult<Option<Vec<String>>> {
        let mut request = json!({"action": "suppress", "absorbed": "suppress", "actor": actor});
        if let Some(reason) = reason {
            request["reason"] = json!(reason);
        }
        Ok(self
            .curate_concept(key, request)
            .await?
            .map(|result| absorbed_keys(&result)))
    }

    /// Reverses [`Knowledge::suppress_concept`] for `key` alone; concepts
    /// suppressed with it stay suppressed and unmerged.
    pub async fn unsuppress_concept(&self, key: &str, actor: &str) -> AntflyResult<bool> {
        Ok(self
            .curate_concept(key, json!({"action": "unsuppress", "actor": actor}))
            .await?
            .is_some())
    }

    /// How consolidation screens concepts for generic words.
    pub fn concept_screening(&self) -> crate::TriageMode {
        self.config.concept_screening
    }

    /// Queues a proposal to suppress concept `key` as a generic word, for
    /// review. Accepting it suppresses what was merged into `key` too.
    /// Proposing a concept again keeps its earlier decision. `false` when
    /// the backend has no proposal queue.
    pub async fn propose_generic(
        &self,
        key: &str,
        reason: &str,
        evidence: Value,
    ) -> AntflyResult<bool> {
        Ok(self
            .antfly
            .propose_curation(
                schema::CONCEPTS,
                json!({
                    "action": "suppress",
                    "key": key,
                    "options": {"absorbed": "suppress"},
                    "evidence": evidence,
                    "actor": GENERIC_SCREEN_ACTOR,
                    "reason": reason,
                }),
            )
            .await?
            .is_some())
    }

    /// Queues a proposal to merge concept `key` into `into`, for review.
    /// Proposing the same pair again keeps its earlier decision. `false`
    /// when the backend has no proposal queue.
    pub async fn propose_merge(
        &self,
        key: &str,
        into: &str,
        reason: &str,
        evidence: Value,
    ) -> AntflyResult<bool> {
        Ok(self
            .antfly
            .propose_curation(
                schema::CONCEPTS,
                json!({
                    "action": "merge",
                    "key": key,
                    "into": into,
                    "evidence": evidence,
                    "actor": CONSOLIDATION_ACTOR,
                    "reason": reason,
                }),
            )
            .await?
            .is_some())
    }

    /// Proposals to suppress concepts, in `state` (`open`, `accepted`,
    /// `rejected` or `superseded`; all when `None`). `None` when the backend
    /// has no proposal queue.
    pub async fn suppress_proposals(
        &self,
        state: Option<&str>,
    ) -> AntflyResult<Option<Vec<ConceptProposal>>> {
        self.proposals(state, Some("suppress")).await
    }

    /// Proposals in `state` for `action` (all when `None`). `None` when the
    /// backend has no proposal queue.
    pub async fn proposals(
        &self,
        state: Option<&str>,
        action: Option<&str>,
    ) -> AntflyResult<Option<Vec<ConceptProposal>>> {
        let mut proposals = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut request = json!({"limit": PROPOSAL_PAGE});
            if let Some(action) = action {
                request["action"] = json!(action);
            }
            if let Some(state) = state {
                request["state"] = json!(state);
            }
            if let Some(cursor) = &cursor {
                request["cursor"] = json!(cursor);
            }
            let Some(page) = self
                .antfly
                .list_curation_proposals(schema::CONCEPTS, request)
                .await?
            else {
                return Ok(None);
            };
            proposals.extend(
                page["proposals"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(ConceptProposal::from_json),
            );
            match page["next_cursor"].as_str() {
                Some(next) if !next.is_empty() => cursor = Some(next.to_string()),
                _ => return Ok(Some(proposals)),
            }
        }
    }

    /// Open proposals from generic screening.
    pub async fn generic_proposals(&self) -> AntflyResult<Vec<ConceptProposal>> {
        Ok(self
            .suppress_proposals(Some("open"))
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter(|proposal| proposal.actor.as_deref() == Some(GENERIC_SCREEN_ACTOR))
            .collect())
    }

    /// Accepts or rejects proposal `id` as `actor`. Accepting applies it;
    /// for a suppression, the concepts suppressed with it are returned.
    /// Rejecting a merge pins the pair distinct, so it is never merged.
    /// `None` when the backend has no proposal queue.
    pub async fn decide_proposal(
        &self,
        proposal: &ConceptProposal,
        accept: bool,
        actor: &str,
    ) -> AntflyResult<Option<Vec<String>>> {
        let mut request = json!({"accept": accept, "actor": actor});
        if !accept && proposal.action == "merge" {
            request["pin_distinct"] = json!(true);
        }
        Ok(self
            .antfly
            .decide_curation_proposal(schema::CONCEPTS, &proposal.id, request)
            .await?
            .map(|result| absorbed_keys(&result)))
    }

    /// Open proposals from Codex (generic screening and consolidation).
    pub async fn open_proposals(&self) -> AntflyResult<Vec<ConceptProposal>> {
        Ok(self
            .proposals(Some("open"), None)
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter(|proposal| {
                matches!(
                    proposal.actor.as_deref(),
                    Some(GENERIC_SCREEN_ACTOR | CONSOLIDATION_ACTOR)
                )
            })
            .collect())
    }

    /// The open suppression proposal for concept `key`, if any.
    pub async fn open_suppress_proposal(&self, key: &str) -> AntflyResult<Option<ConceptProposal>> {
        Ok(self
            .suppress_proposals(Some("open"))
            .await?
            .unwrap_or_default()
            .into_iter()
            .find(|proposal| proposal.key == key))
    }

    /// Redirects, distinct pins and suppressions of the concept resolvers,
    /// as Antfly records them. `None` when the backend cannot report them.
    pub async fn concept_curation(&self) -> AntflyResult<Option<ConceptCuration>> {
        let mut curation = ConceptCuration::default();
        let mut reported = false;
        for table in [
            schema::KNOWLEDGE,
            schema::MEMORY_NOTES,
            schema::HISTORY_ITEMS,
        ] {
            let Some(state) = self
                .antfly
                .documents(table)
                .resolver_curation(CONCEPT_RESOLVER)
                .await?
            else {
                continue;
            };
            reported = true;
            curation.absorb(&state);
        }
        Ok(reported.then_some(curation))
    }

    /// The key the resolver mints for `name` before any redirect: the key
    /// to unmerge or suppress. `None` when the backend cannot render.
    pub async fn concept_minted_key(&self, name: &str) -> AntflyResult<Option<String>> {
        Ok(self
            .antfly
            .documents(schema::KNOWLEDGE)
            .render_resolver(CONCEPT_RESOLVER, json!({"text": name, "label": "concept"}))
            .await?
            .and_then(|rendered| rendered.get("minted_key")?.as_str().map(str::to_string)))
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
        let request = supersession_request(&self.antfly.config().decide_model, earlier, later);
        let response = self.antfly.decide(&request).await?;
        let score = supersession_score(&response)
            .ok_or_else(|| crate::AntflyError::Malformed(format!("decide response: {response}")))?;
        let threshold = self.config.supersede_threshold_bp;
        let supersedes = score * 10_000.0 >= f64::from(threshold);
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
                score: Some(score),
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
        let index = self.graph_for(table).await;
        let mut found: BTreeMap<String, (Option<Value>, BTreeSet<String>)> = BTreeMap::new();
        for (concept, node) in self
            .reached_from_concepts(table, &index, concepts, limit, true)
            .await
        {
            let entry = found.entry(node.key).or_default();
            if entry.0.is_none() {
                entry.0 = node.doc;
            }
            entry.1.insert(concept);
        }
        found
            .into_iter()
            .filter_map(|(key, (doc, shared))| Some((key, doc?, shared.into_iter().collect())))
            .collect()
    }

    /// Nodes one `about` edge into each of `concepts` in graph `index` of
    /// `table`, paired with the concept that reached them. One traversal per
    /// concept: the server traverses incoming edges only with deduplicated
    /// nodes, so a single traversal would lose which concepts a node shares.
    /// Failures yield none.
    async fn reached_from_concepts(
        &self,
        table: &'static str,
        index: &str,
        concepts: &[String],
        limit: usize,
        include_documents: bool,
    ) -> Vec<(String, Reached)> {
        if concepts.is_empty() {
            return Vec::new();
        }
        let queries: serde_json::Map<String, Value> = concepts
            .iter()
            .enumerate()
            .map(|(position, concept)| {
                (
                    format!("c{position}"),
                    json!({"index": index, "traverse": {
                        "start": concept_nodes(std::slice::from_ref(concept)),
                        "direction": "in", "edge_types": [ABOUT_EDGE], "max_depth": 1,
                        "include_documents": include_documents, "include_paths": true,
                        "limit": limit,
                    }}),
                )
            })
            .collect();
        let request = json!({"graph_queries": queries, "limit": 1});
        let response = match self.antfly.documents(table).query(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("antfly: concept traversal failed: {err}");
                return Vec::new();
            }
        };
        concepts
            .iter()
            .enumerate()
            .flat_map(|(position, concept)| {
                reached_nodes(&response, &format!("c{position}"))
                    .into_iter()
                    .map(|node| (concept.clone(), node))
            })
            .collect()
    }

    /// Threads whose turns are about any of `concepts` (history concept
    /// graph), most shared concepts first. Failures yield none.
    pub async fn threads_about(&self, concepts: &[String], limit: usize) -> Vec<RelatedThread> {
        let wanted = history_concepts_graph_name(&self.config.extract_model);
        let documents = self.antfly.documents(schema::HISTORY_ITEMS);
        let index = documents.serving_graph(&wanted).await.unwrap_or(wanted);
        let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (concept, node) in self
            .reached_from_concepts(schema::HISTORY_ITEMS, &index, concepts, limit * 8, false)
            .await
        {
            if let Some(thread_id) = node.key.strip_prefix(THREAD_NODE_PREFIX) {
                found
                    .entry(thread_id.to_string())
                    .or_default()
                    .insert(concept);
            }
        }
        let mut threads: Vec<RelatedThread> = found
            .into_iter()
            .map(|(thread_id, concepts)| RelatedThread {
                thread_id,
                concepts: concepts.into_iter().collect(),
            })
            .collect();
        threads.sort_by(|a, b| {
            b.concepts
                .len()
                .cmp(&a.concepts.len())
                .then(a.thread_id.cmp(&b.thread_id))
        });
        threads.truncate(limit);
        threads
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

    /// Concepts whose descriptions are nearest to `key`'s, with similarity
    /// relative to its own (1.0 = as close as itself), most similar first.
    /// Empty when `key` has no description.
    pub async fn description_neighbors(
        &self,
        key: &str,
        limit: usize,
    ) -> AntflyResult<Vec<(String, f64)>> {
        let table = self.antfly.documents(schema::CONCEPT_DESCRIPTIONS);
        let Some(text) = table
            .get(key)
            .await?
            .and_then(|doc| doc.get("search_text")?.as_str().map(str::to_string))
        else {
            return Ok(Vec::new());
        };
        let hits = table.nearest(&text, limit + 1).await?;
        let own = hits
            .iter()
            .find(|hit| hit.key == key)
            .or(hits.first())
            .map(|hit| hit.score)
            .filter(|score| *score > 0.0)
            .unwrap_or(1.0);
        Ok(hits
            .into_iter()
            .filter(|hit| hit.key != key)
            .map(|hit| (hit.key, hit.score / own))
            .take(limit)
            .collect())
    }

    /// Other concepts mentioned by the current claims about `key`, with how
    /// many of those claims mention each.
    pub async fn co_mentioned(&self, key: &str, limit: usize) -> HashMap<String, usize> {
        let claims: Vec<String> = self
            .about_any(schema::KNOWLEDGE, &[key.to_string()], limit)
            .await
            .into_iter()
            .filter(|(_, doc, _)| doc.get("status").and_then(Value::as_str) == Some("current"))
            .map(|(id, _, _)| id)
            .collect();
        let mut counts: HashMap<String, usize> = HashMap::new();
        for concepts in self
            .concepts_of(schema::KNOWLEDGE, &claims)
            .await
            .into_values()
        {
            for concept in concepts {
                if concept != key {
                    *counts.entry(concept).or_default() += 1;
                }
            }
        }
        counts
    }

    /// Every promoted concept, by key.
    pub async fn concepts_by_key(&self) -> AntflyResult<BTreeMap<String, Concept>> {
        Ok(self
            .concepts()
            .await?
            .into_iter()
            .map(|concept| (concept.key.clone(), concept))
            .collect())
    }

    /// Concepts consolidation merged into another.
    pub async fn merged_concept_keys(&self) -> AntflyResult<BTreeSet<String>> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_all(
                "SELECT key FROM codex_concept_reviews WHERE merged_into IS NOT NULL",
                vec![],
            )
            .await?
            .iter()
            .filter_map(|row| row.string("key").ok())
            .collect())
    }

    /// Unordered concept pairs the relation pass has judged.
    pub async fn related_pairs_reviewed(&self) -> AntflyResult<BTreeSet<(String, String)>> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_all("SELECT a, b FROM codex_concept_relation_reviews", vec![])
            .await?
            .iter()
            .filter_map(|row| Some((row.string("a").ok()?, row.string("b").ok()?)))
            .collect())
    }

    /// Records the relation judged between concepts `a` and `b`: an edge
    /// `from kind to` (both among `a`, `b`), or none.
    pub async fn record_concept_relation(
        &self,
        a: &str,
        b: &str,
        edge: Option<(&str, &str, &str)>,
    ) -> AntflyResult<()> {
        if let Some((from, kind, to)) = edge {
            let table = self.antfly.documents(schema::CONCEPT_LINKS);
            let mut doc = table
                .get(from)
                .await?
                .unwrap_or_else(|| json!({"search_text": from}));
            let targets = doc
                .as_object_mut()
                .map(|object| object.entry(kind.to_string()).or_insert_with(|| json!([])));
            if let Some(Value::Array(targets)) = targets
                && !targets.iter().any(|target| target.as_str() == Some(to))
            {
                targets.push(json!(to));
            }
            doc["linked_at_ms"] = json!(now_ms());
            table.write(vec![Write::put(from.to_string(), doc)]).await?;
        }
        let (a, b) = if a <= b { (a, b) } else { (b, a) };
        let sql = self.antfly.sql().await?;
        sql.execute(
            "INSERT INTO codex_concept_relation_reviews (a, b, relation, reviewed_at_ms) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (a, b) DO UPDATE SET \
             relation = excluded.relation, reviewed_at_ms = excluded.reviewed_at_ms",
            crate::sql_params![
                a,
                b,
                edge.map(|(from, kind, to)| format!("{from} {kind} {to}")),
                now_ms()
            ],
        )
        .await?;
        Ok(())
    }

    /// Removes every link between concepts `a` and `b`, in either
    /// direction, and records the pair as judged unrelated so the relation
    /// pass does not add it back. `false` when there was no link.
    pub async fn unlink_concepts(&self, a: &str, b: &str) -> AntflyResult<bool> {
        let table = self.antfly.documents(schema::CONCEPT_LINKS);
        let mut removed = false;
        for (from, to) in [(a, b), (b, a)] {
            let Some(mut doc) = table.get(from).await? else {
                continue;
            };
            let mut changed = false;
            for kind in CONCEPT_LINK_KINDS {
                if let Some(Value::Array(targets)) = doc.get_mut(*kind) {
                    let before = targets.len();
                    targets.retain(|target| target.as_str() != Some(to));
                    changed |= targets.len() != before;
                }
            }
            if changed {
                table.write(vec![Write::put(from.to_string(), doc)]).await?;
                removed = true;
            }
        }
        self.record_concept_relation(a, b, None).await?;
        Ok(removed)
    }

    /// Forgets which concepts consolidation has examined, keeping its
    /// merges, so every unmerged concept is judged again (with the
    /// descriptions written since). Returns how many were forgotten.
    pub async fn forget_concept_reviews(&self) -> AntflyResult<usize> {
        let sql = self.antfly.sql().await?;
        let forgotten = sql
            .fetch_all(
                "SELECT key FROM codex_concept_reviews WHERE merged_into IS NULL",
                vec![],
            )
            .await?
            .len();
        sql.execute(
            "DELETE FROM codex_concept_reviews WHERE merged_into IS NULL",
            vec![],
        )
        .await?;
        Ok(forgotten)
    }

    /// Every merge consolidation made, as `(merged, into)` concept keys.
    pub async fn concept_merges(&self) -> AntflyResult<Vec<(String, String)>> {
        let sql = self.antfly.sql().await?;
        Ok(sql
            .fetch_all(
                "SELECT key, merged_into FROM codex_concept_reviews \
                 WHERE merged_into IS NOT NULL ORDER BY key",
                vec![],
            )
            .await?
            .iter()
            .filter_map(|row| Some((row.string("key").ok()?, row.string("merged_into").ok()?)))
            .collect())
    }

    /// Every concept link as `(from, kind, to)` concept keys.
    pub async fn concept_links(&self) -> AntflyResult<Vec<(String, String, String)>> {
        let mut links = Vec::new();
        for document in self
            .antfly
            .documents(schema::CONCEPT_LINKS)
            .scan(ScanRequest::prefix(CONCEPT_PREFIX))
            .await?
        {
            for kind in CONCEPT_LINK_KINDS {
                for target in document.doc[*kind].as_array().into_iter().flatten() {
                    if let Some(target) = target.as_str() {
                        links.push((
                            document.key.clone(),
                            (*kind).to_string(),
                            target.to_string(),
                        ));
                    }
                }
            }
        }
        Ok(links)
    }

    /// Concepts one link away from each of `keys`, in either direction.
    pub async fn linked_concepts(&self, keys: &[String]) -> HashMap<String, Vec<String>> {
        let mut linked: HashMap<String, Vec<String>> = HashMap::new();
        if keys.is_empty() {
            return linked;
        }
        let documents = self.antfly.documents(schema::CONCEPT_LINKS);
        let index = match documents.serving_graph(CONCEPT_LINKS_INDEX).await {
            Ok(index) => index,
            Err(err) => {
                tracing::debug!("antfly: concept links lookup failed: {err}");
                return linked;
            }
        };
        let request = json!({
            "graph_queries": {"links": {"index": index, "traverse": {
                "start": {"keys": keys},
                "direction": "both",
                "edge_types": CONCEPT_LINK_KINDS,
                "max_depth": 1,
                "include_documents": false, "include_paths": true,
                "deduplicate_nodes": false, "limit": 200,
            }}},
            "limit": 1,
        });
        let response = match documents.query(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!("antfly: concept links traversal failed: {err}");
                return linked;
            }
        };
        for node in reached_nodes(&response, "links") {
            let neighbors = linked.entry(node.from.clone()).or_default();
            if node.key != node.from && !neighbors.contains(&node.key) {
                neighbors.push(node.key);
            }
        }
        linked
    }

    /// Lines of the memory notes about `concept` that mention it by name,
    /// for concepts that notes mention but no current claim is about.
    pub async fn note_evidence(&self, concept: &Concept, limit: usize) -> Vec<String> {
        let name = concept.name.to_lowercase();
        let mut excerpts = Vec::new();
        for (_, doc, _) in self
            .about_any(
                schema::MEMORY_NOTES,
                std::slice::from_ref(&concept.key),
                limit * 2,
            )
            .await
        {
            let text = doc
                .get(crate::SEARCH_TEXT_FIELD)
                .and_then(Value::as_str)
                .unwrap_or_default();
            for line in note_lines_mentioning(text, &name) {
                let excerpt = truncate_chars(line, NOTE_EXCERPT_MAX_CHARS).to_string();
                if !excerpts.contains(&excerpt) {
                    excerpts.push(excerpt);
                }
                if excerpts.len() >= limit {
                    return excerpts;
                }
            }
        }
        excerpts
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
    /// evidence changed since they were last described. Evidence is the
    /// current claims about the concept, then lines of memory notes that
    /// mention it. Concepts merged into another are skipped, and so are
    /// concepts with no evidence.
    pub async fn describe_concepts(&self, max: usize) -> AntflyResult<DescribeReport> {
        let mut report = DescribeReport::default();
        let Some(mut model) = self.config.describe_model.clone() else {
            return Ok(report);
        };
        let merged = self.merged_concept_keys().await?;
        let table = self.antfly.documents(schema::CONCEPT_DESCRIPTIONS);
        for concept in self.concepts().await? {
            if report.described >= max {
                break;
            }
            if merged.contains(&concept.key) {
                continue;
            }
            report.examined += 1;
            let mut evidence = self.concept_evidence(&concept.key, DESCRIBE_EVIDENCE).await;
            let room = DESCRIBE_EVIDENCE
                .saturating_sub(evidence.len())
                .min(NOTE_EXCERPTS);
            if room > 0 {
                evidence.extend(self.note_evidence(&concept, room).await);
            }
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
            let mut response = self
                .antfly
                .generate(&describe_request(&model, &concept, &evidence))
                .await;
            // A model too large for the inference memory left by extraction
            // and decide models: use the smaller one for the rest of this
            // pass. Its descriptions are rewritten once the configured model
            // loads, since the model is part of the evidence hash.
            if response.is_err() && model != crate::config::FALLBACK_DESCRIBE_MODEL {
                tracing::info!(
                    "antfly: describe model {model} unavailable; falling back to {}",
                    crate::config::FALLBACK_DESCRIBE_MODEL
                );
                model = crate::config::FALLBACK_DESCRIBE_MODEL.to_string();
                let fallback_hash = evidence_hash(&model, &concept, &evidence);
                if existing
                    .as_ref()
                    .and_then(|doc| doc.get("evidence_hash")?.as_str())
                    == Some(fallback_hash.as_str())
                {
                    report.unchanged += 1;
                    continue;
                }
                response = self
                    .antfly
                    .generate(&describe_request(&model, &concept, &evidence))
                    .await;
            }
            let hash = evidence_hash(&model, &concept, &evidence);
            let description = match response {
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
        let linked: Vec<String> = {
            let mut linked: BTreeSet<String> = self
                .linked_concepts(&seeds)
                .await
                .into_values()
                .flatten()
                .collect();
            for seed in &seeds {
                linked.remove(seed);
            }
            linked.into_iter().collect()
        };
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
        // Claims about a linked concept only, at lower weight.
        let mut via_link: BTreeSet<String> = BTreeSet::new();
        for (key, doc, shared) in self.about_any(schema::KNOWLEDGE, &linked, limit * 2).await {
            if candidates.contains_key(&key) {
                continue;
            }
            let Some(claim) = Claim::from_doc(&key, doc) else {
                continue;
            };
            if keep(&claim) {
                via_link.insert(key.clone());
                candidates.insert(key, (claim, shared, 0.0));
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
                } * if via_link.contains(&key) {
                    LINKED_WEIGHT
                } else {
                    1.0
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
        let threads = self.threads_about(&seeds, limit).await;
        Ok(Related {
            seeds,
            linked,
            claims,
            notes,
            threads,
        })
    }
}

/// Lines of a Markdown note that mention `name` (lowercase), with list
/// markers and heading hashes stripped.
fn note_lines_mentioning<'a>(text: &'a str, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
    text.lines()
        .map(|line| line.trim().trim_start_matches(['#', '-', '*', '>']).trim())
        .filter(move |line| !line.is_empty() && line.to_lowercase().contains(name))
}

/// Durable curation of the concept resolvers (see
/// [`Knowledge::concept_curation`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConceptCuration {
    /// Merged concept key → the key it resolves to now.
    pub redirects: BTreeMap<String, String>,
    /// Unordered pairs pinned never to merge, smaller key first.
    pub distinct: BTreeSet<(String, String)>,
    /// Suppressed concept keys.
    pub suppressed: BTreeSet<String>,
    /// Why each suppressed key is suppressed, when Antfly recorded it.
    pub suppression_origins: BTreeMap<String, CurationOrigin>,
}

/// Antfly's provenance for one curation record.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CurationOrigin {
    /// `manual`, `adjudicator`, `review`, `cascade` or `config`.
    pub origin: String,
    pub actor: Option<String>,
    pub reason: Option<String>,
    /// For a cascade, the key whose suppression suppressed this one.
    pub cascade_from: Option<String>,
    /// For a review, the accepted proposal.
    pub proposal_id: Option<String>,
}

impl CurationOrigin {
    fn from_json(value: &Value) -> Self {
        let text = |field: &str| value[field].as_str().map(str::to_string);
        Self {
            origin: text("origin").unwrap_or_default(),
            actor: text("actor"),
            reason: text("reason"),
            cascade_from: text("cascade_from"),
            proposal_id: text("proposal_id"),
        }
    }
}

/// A queued curation proposal for a concept.
#[derive(Clone, Debug, PartialEq)]
pub struct ConceptProposal {
    pub id: String,
    pub action: String,
    pub key: String,
    /// The merge target, for a merge.
    pub into: Option<String>,
    /// `open`, `accepted`, `rejected` or `superseded`.
    pub state: String,
    pub actor: Option<String>,
    pub reason: Option<String>,
}

impl ConceptProposal {
    fn from_json(value: &Value) -> Option<Self> {
        let text = |field: &str| value[field].as_str().map(str::to_string);
        Some(Self {
            id: text("id")?,
            action: text("action")?,
            key: text("key")?,
            into: text("into"),
            state: text("state")?,
            actor: value["origin"]["actor"].as_str().map(str::to_string),
            reason: value["origin"]["reason"].as_str().map(str::to_string),
        })
    }
}

/// Actor recorded on generic screening's proposals and suppressions.
pub const GENERIC_SCREEN_ACTOR: &str = "codex/generic-screen";
/// Actor recorded on consolidation's merge proposals.
pub const CONSOLIDATION_ACTOR: &str = "codex/consolidation";
/// Actor recorded on curation from `codex knowledge`.
pub const USER_ACTOR: &str = "codex/user";
/// Proposals listed per request.
const PROPOSAL_PAGE: usize = 200;

/// Keys an entity action released or suppressed with its target, across
/// resolvers.
fn absorbed_keys(result: &Value) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for resolver in result["results"].as_array().into_iter().flatten() {
        for absorbed in resolver["absorbed"].as_array().into_iter().flatten() {
            if let Some(key) = absorbed["key"].as_str() {
                keys.insert(key.to_string());
            }
        }
    }
    keys.into_iter().collect()
}

impl ConceptCuration {
    /// Adds one resolver's state (`resolver_curation_json`).
    fn absorb(&mut self, state: &Value) {
        let state = state.get("resolver").unwrap_or(state);
        for redirect in state["redirects"].as_array().into_iter().flatten() {
            if let (Some(key), Some(target)) =
                (redirect["key"].as_str(), redirect["target"].as_str())
            {
                self.redirects.insert(key.to_string(), target.to_string());
            }
        }
        let curation = &state["curation"];
        for pair in curation["distinct"].as_array().into_iter().flatten() {
            if let (Some(from), Some(to)) = (pair["from"].as_str(), pair["to"].as_str()) {
                self.distinct.insert(ordered_pair(from, to));
            }
        }
        let suppress = &curation["suppress"];
        for key in suppress["keys"].as_array().into_iter().flatten() {
            if let Some(key) = key.as_str() {
                self.suppressed.insert(key.to_string());
            }
        }
        for entry in suppress["entries"].as_array().into_iter().flatten() {
            if let Some(key) = entry["key"].as_str() {
                self.suppressed.insert(key.to_string());
                self.suppression_origins
                    .insert(key.to_string(), CurationOrigin::from_json(&entry["origin"]));
            }
        }
    }

    /// Whether `a` and `b` are pinned never to merge.
    pub fn is_distinct(&self, a: &str, b: &str) -> bool {
        self.distinct.contains(&ordered_pair(a, b))
    }
}

fn ordered_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
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
    /// Concepts no current claim or memory note is about.
    pub without_evidence: usize,
    pub failed: usize,
}

/// Identity of what a description was written from, so it is rewritten
/// only when the model, the name or the evidence changes.
fn evidence_hash(model: &str, concept: &Concept, evidence: &[String]) -> String {
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    DESCRIBE_VERSION.hash(&mut hasher);
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

/// A concept as a judge sees it: its name, and its description when known.
fn concept_line(name: &str, description: Option<&str>) -> String {
    match description {
        Some(description) => format!(
            "{name} — {}",
            truncate_chars(description, DECIDE_STATEMENT_MAX_CHARS)
        ),
        None => name.to_string(),
    }
}

/// The decide request asking how concepts A and B relate, for merging: one
/// choice whose `same` probability is the merge score. The other choices
/// give the related-but-different mass somewhere to go; asked alone, "are
/// these the same?" rates siblings and versions as high as synonyms.
pub fn concept_merge_request(
    model: &str,
    a: &str,
    a_description: Option<&str>,
    b: &str,
    b_description: Option<&str>,
) -> Value {
    json!({
        "model": model,
        "input": format!(
            "A: {}\nB: {}",
            concept_line(a, a_description),
            concept_line(b, b_description)
        ),
        "questions": [{
            "name": "relation",
            "type": "choice",
            "instructions": "Do A and B name the same thing in this software project?",
            "choices": [
                {"value": "same", "description": "The same thing under another name, spelling or abbreviation"},
                {"value": "kind", "description": "One is a kind, version or instance of the other"},
                {"value": "part", "description": "One is a part, feature or component of the other"},
                {"value": "related", "description": "Different things that are related or used together"},
                {"value": "unrelated", "description": "Different, unrelated things"},
            ],
        }],
    })
}

/// Probability that a [`concept_merge_request`] response names one thing.
pub fn concept_merge_score(response: &Value) -> Option<f64> {
    relation_probabilities(response)?.get("same").copied()
}

/// Directed relations between concepts: `A <relation> B`.
pub const CONCEPT_RELATIONS_KINDS: &[&str] = &["is_a", "part_of", "alternative_to"];

/// The decide request asking how concept A relates to concept B.
pub fn concept_relation_request(
    model: &str,
    a: &str,
    a_description: Option<&str>,
    b: &str,
    b_description: Option<&str>,
) -> Value {
    json!({
        "model": model,
        "input": format!(
            "A: {}\nB: {}",
            concept_line(a, a_description),
            concept_line(b, b_description)
        ),
        "questions": [{
            "name": "relation",
            "type": "choice",
            "instructions": "How is A related to B?",
            "choices": [
                {"value": "a_is_b", "description": "A is a kind, version or instance of B"},
                {"value": "b_is_a", "description": "B is a kind, version or instance of A"},
                {"value": "a_part_b", "description": "A is a part, feature or component of B"},
                {"value": "b_part_a", "description": "B is a part, feature or component of A"},
                {"value": "alternatives", "description": "A and B are alternatives for the same purpose"},
                {"value": "none", "description": "None of these"},
            ],
        }],
    })
}

/// A judged concept edge: `(from, kind, to, probability)`, with `from` and
/// `to` as `"a"` or `"b"`.
pub type ConceptEdge = (&'static str, &'static str, &'static str, f64);

/// A concept relation read from a [`concept_relation_request`] response:
/// `(from, relation, to, probability)` with `from`/`to` as `"a"`/`"b"`,
/// or `None` when "none" is likeliest. Also returns 1 − p(none).
pub fn concept_relation_answer(response: &Value) -> Option<(Option<ConceptEdge>, f64)> {
    let probabilities = relation_probabilities(response)?;
    let related = 1.0 - probabilities.get("none").copied().unwrap_or_default();
    let (top, p) = probabilities
        .iter()
        .max_by(|x, y| x.1.total_cmp(y.1))
        .map(|(value, p)| (value.as_str(), *p))?;
    let edge = match top {
        "a_is_b" => Some(("a", "is_a", "b", p)),
        "b_is_a" => Some(("b", "is_a", "a", p)),
        "a_part_b" => Some(("a", "part_of", "b", p)),
        "b_part_a" => Some(("b", "part_of", "a", p)),
        "alternatives" => Some(("a", "alternative_to", "b", p)),
        _ => None,
    };
    Some((edge, related))
}

/// The decide request asking whether `later` supersedes `earlier`.
pub fn supersession_request(model: &str, earlier: &str, later: &str) -> Value {
    json!({
        "model": model,
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
    })
}

/// How confidently a supersession response says the later statement
/// replaces the earlier one: the `supersedes` predicate, or the mass on
/// `same` and `updates` when one of them is the top relation. A threshold on
/// this score is Codex's supersession rule.
pub fn supersession_score(response: &Value) -> Option<f64> {
    let probabilities = relation_probabilities(response)?;
    let predicate = predicate_probability(response, "supersedes")?;
    Some(predicate.max(replacing_score(&probabilities)))
}

/// `same` + `updates` when one of them is the top relation, else 0.
fn replacing_score(probabilities: &HashMap<String, f64>) -> f64 {
    let p = |value: &str| probabilities.get(value).copied().unwrap_or_default();
    let top = probabilities
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(value, _)| value.as_str());
    if matches!(top, Some("same" | "updates")) {
        p("same") + p("updates")
    } else {
        0.0
    }
}

#[cfg(test)]
fn replaces_earlier(probabilities: &HashMap<String, f64>, threshold_bp: u32) -> bool {
    replacing_score(probabilities) * 10_000.0 >= f64::from(threshold_bp)
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
    fn note_excerpts_are_the_lines_naming_the_concept() {
        let note = "# Embedders\n\n- EmbeddingGemma 2 is the default embedder.\n- Qwen3 was dropped.\n> embeddinggemma 2 detects 768 dimensions\n";
        assert_eq!(
            note_lines_mentioning(note, "embeddinggemma 2").collect::<Vec<_>>(),
            vec![
                "EmbeddingGemma 2 is the default embedder.",
                "embeddinggemma 2 detects 768 dimensions"
            ]
        );
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
