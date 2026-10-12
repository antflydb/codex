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
//! Supersession is judged when claims are written: each new claim is compared
//! with its most similar current claims from other threads using a
//! decide-model predicate. Retrieval seeds concepts from the query (GLiNER at
//! query time plus the concepts of direct search hits), then walks `about`
//! edges back to the claims and notes that share them.

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
pub const CLAIM_LINKS_INDEX: &str = "codex_claim_links";
pub const ABOUT_EDGE: &str = "about";
pub const SUPERSEDES_EDGE: &str = "supersedes";
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
/// Bump when the labels, relations or resolver settings change.
const CONCEPT_SCHEMA_VERSION: u32 = 1;
/// GLiNER's single-window extraction profile is measured up to 610 bytes.
const QUERY_EXTRACT_MAX_BYTES: usize = 600;
/// Laya rejects over-long inputs; keep each statement well inside them.
const DECIDE_STATEMENT_MAX_CHARS: usize = 600;
const SUPERSEDE_CANDIDATES: usize = 3;

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
        "resolvers": [{
            "name": "concepts",
            "table": "codex_concepts",
            "source_artifact": artifact,
            "resolution_artifact": format!("{name}_resolution"),
            // Label-free keys: a per-mention label guess in the key would
            // split one idea into several nodes.
            "key_template": "concept/{{ slug _entity.text }}",
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
        config: json!({"edge_types": [{"name": SUPERSEDES_EDGE, "field": "supersedes"}]}),
        replaces_prefix: "",
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
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Current,
    Superseded,
}

/// A claim to record for a thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewClaim {
    pub kind: ClaimKind,
    pub statement: String,
}

/// Where a thread's claims came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimSource {
    /// Memory pipeline version (`v1`, `v2`); claims are kept per version.
    pub version: String,
    pub thread_id: String,
    pub cwd: Option<String>,
    /// The thread's `updated_at` that the claims were distilled from.
    pub source_updated_at_ms: i64,
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
    fn graph_name(&self) -> String {
        concept_graph_name(&self.config.extract_model)
    }

    /// Replaces `source`'s claims with `claims`. Each new claim is checked
    /// against similar current claims from other threads; ones it
    /// supersedes are marked so. Supersession failures are logged and never
    /// fail the write.
    pub async fn record_thread_claims(
        &self,
        source: &ClaimSource,
        claims: Vec<NewClaim>,
    ) -> AntflyResult<Vec<Claim>> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let prefix = claim_prefix(&source.version, &source.thread_id);
        let existing: BTreeSet<String> = documents
            .scan(ScanRequest::prefix(&prefix))
            .await?
            .into_iter()
            .map(|document| document.key)
            .collect();
        let generated_at_ms = now_ms();
        let mut records = Vec::with_capacity(claims.len());
        let mut superseded: BTreeMap<String, String> = BTreeMap::new();
        for (index, claim) in claims.into_iter().enumerate() {
            let statement = claim.statement.trim().to_string();
            if statement.is_empty() {
                continue;
            }
            let id = format!("{prefix}{index:04}");
            let mut supersedes = Vec::new();
            match self
                .supersede_candidates(source, claim.kind, &statement)
                .await
            {
                Ok(candidates) => {
                    for earlier in candidates {
                        match self.supersedes(&earlier.statement, &statement).await {
                            Ok(true) => {
                                superseded.insert(earlier.id.clone(), id.clone());
                                supersedes.push(earlier.id);
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
            records.push(Claim {
                id,
                kind: claim.kind,
                statement,
                status: ClaimStatus::Current,
                thread_id: source.thread_id.clone(),
                version: source.version.clone(),
                cwd: source.cwd.clone(),
                source_updated_at_ms: source.source_updated_at_ms,
                generated_at_ms,
                supersedes,
                superseded_by: None,
            });
        }
        let kept: BTreeSet<&str> = records.iter().map(|claim| claim.id.as_str()).collect();
        let mut writes = Vec::new();
        for key in existing.iter().filter(|key| !kept.contains(key.as_str())) {
            writes.push(Write::delete(key.clone()));
        }
        for record in &records {
            writes.push(Write::put(record.id.clone(), record.to_doc()?));
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
        Ok(records)
    }

    /// Deletes every claim distilled from `thread_id`, for any version.
    pub async fn delete_thread_claims(&self, version: &str, thread_id: &str) -> AntflyResult<()> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let deletes: Vec<Write> = documents
            .scan(ScanRequest::prefix(&claim_prefix(version, thread_id)))
            .await?
            .into_iter()
            .map(|document| Write::delete(document.key))
            .collect();
        documents.write(deletes).await
    }

    /// Deletes every claim of `version`.
    pub async fn clear(&self, version: &str) -> AntflyResult<()> {
        let documents = self.antfly.documents(schema::KNOWLEDGE);
        let prefix = format!("{}:", crate::keys::escape(version));
        let deletes: Vec<Write> = documents
            .scan(ScanRequest::prefix(&prefix))
            .await?
            .into_iter()
            .map(|document| Write::delete(document.key))
            .collect();
        documents.write(deletes).await
    }

    pub async fn get(&self, id: &str) -> AntflyResult<Option<Claim>> {
        Ok(self
            .antfly
            .documents(schema::KNOWLEDGE)
            .get(id.to_string())
            .await?
            .and_then(|doc| Claim::from_doc(id, doc)))
    }

    async fn supersede_candidates(
        &self,
        source: &ClaimSource,
        kind: ClaimKind,
        statement: &str,
    ) -> AntflyResult<Vec<Claim>> {
        let filter = json!({"conjuncts": [
            {"term": {"status": "current"}},
            {"term": {"kind": kind.as_str()}},
            {"term": {"version": source.version}},
        ]});
        let hits = self
            .antfly
            .documents(schema::KNOWLEDGE)
            .search_text(
                statement,
                Some(filter),
                SUPERSEDE_CANDIDATES,
                SUPERSEDE_CANDIDATES,
            )
            .await?;
        Ok(hits
            .into_iter()
            .filter_map(|hit| Claim::from_doc(&hit.key, hit.doc?))
            .filter(|claim| claim.thread_id != source.thread_id)
            .take(SUPERSEDE_CANDIDATES)
            .collect())
    }

    /// Asks the decide model whether `later` replaces `earlier`.
    async fn supersedes(&self, earlier: &str, later: &str) -> AntflyResult<bool> {
        let request = json!({
            "model": self.antfly.config().decide_model,
            "input": format!(
                "Earlier statement: {}\nLater statement: {}",
                truncate_chars(earlier, DECIDE_STATEMENT_MAX_CHARS),
                truncate_chars(later, DECIDE_STATEMENT_MAX_CHARS),
            ),
            "questions": [{
                "name": "supersedes",
                "type": "predicate",
                "instructions": "The later statement replaces, reverses or updates the earlier statement about the same subject.",
            }],
        });
        let response = self.antfly.decide(&request).await?;
        let probability = response
            .get("answers")
            .and_then(Value::as_array)
            .and_then(|answers| {
                answers
                    .iter()
                    .find(|answer| answer.get("name").and_then(Value::as_str) == Some("supersedes"))
            })
            .and_then(|answer| answer.get("probability"))
            .and_then(Value::as_f64)
            .ok_or_else(|| crate::AntflyError::Malformed(format!("decide response: {response}")))?;
        Ok(probability * 10_000.0 >= f64::from(self.config.supersede_threshold_bp))
    }

    /// Concept keys GLiNER finds in `text` (truncated to the single-window
    /// profile). Empty when extraction is unavailable.
    pub async fn extract_concepts(&self, text: &str) -> Vec<String> {
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
        let request = json!({
            "graph_queries": {"about": {"index": self.graph_name(), "traverse": {
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

    /// Documents in `table` that are `about` any of `concepts`, with the
    /// concepts each shares. A traversal reports each document once, so the
    /// shared set comes from each document's own concepts.
    async fn about_any(
        &self,
        table: &'static str,
        concepts: &[String],
        limit: usize,
    ) -> Vec<(String, Value, Vec<String>)> {
        if concepts.is_empty() {
            return Vec::new();
        }
        let request = json!({
            "graph_queries": {"docs": {"index": self.graph_name(), "traverse": {
                "start": {"keys": concepts},
                "direction": "in", "edge_types": [ABOUT_EDGE], "max_depth": 1,
                "include_documents": true, "include_paths": true, "limit": limit,
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
        let found: BTreeMap<String, Value> = reached_nodes(&response, "docs")
            .into_iter()
            .filter_map(|node| Some((node.key, node.doc?)))
            .collect();
        let keys: Vec<String> = found.keys().cloned().collect();
        let own = self.concepts_of(table, &keys).await;
        let seeds: BTreeSet<&String> = concepts.iter().collect();
        found
            .into_iter()
            .map(|(key, doc)| {
                let shared = own
                    .get(&key)
                    .map(|own| {
                        own.iter()
                            .filter(|concept| seeds.contains(concept))
                            .cloned()
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect()
                    })
                    .unwrap_or_default();
                (key, doc, shared)
            })
            .collect()
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
        let mut direct: HashMap<String, f64> = HashMap::new();
        let filter = (!include_superseded).then(|| json!({"term": {"status": "current"}}));
        let hits = claims_table
            .search_text(query, filter, limit, limit)
            .await?;
        let hit_keys: Vec<String> = hits.iter().map(|hit| hit.key.clone()).collect();
        for (rank, hit) in hits.iter().enumerate() {
            direct.insert(hit.key.clone(), 1.0 / (rank as f64 + 1.0));
        }
        for concepts in self
            .concepts_of(schema::KNOWLEDGE, &hit_keys)
            .await
            .into_values()
        {
            seeds.extend(concepts);
        }
        let seeds: Vec<String> = seeds.into_iter().collect();

        let mut claims: HashMap<String, RelatedClaim> = HashMap::new();
        for hit in hits {
            let Some(claim) = hit.doc.and_then(|doc| Claim::from_doc(&hit.key, doc)) else {
                continue;
            };
            let score = direct.get(&claim.id).copied().unwrap_or_default();
            claims.insert(
                claim.id.clone(),
                RelatedClaim {
                    claim,
                    concepts: Vec::new(),
                    score,
                },
            );
        }
        for (key, doc, shared) in self.about_any(schema::KNOWLEDGE, &seeds, limit * 4).await {
            let Some(claim) = Claim::from_doc(&key, doc) else {
                continue;
            };
            if !include_superseded && claim.status == ClaimStatus::Superseded {
                continue;
            }
            let entry = claims.entry(key).or_insert_with(|| RelatedClaim {
                claim,
                concepts: Vec::new(),
                score: 0.0,
            });
            entry.score += shared.len() as f64;
            entry.concepts = shared;
        }
        let mut claims: Vec<RelatedClaim> = claims.into_values().collect();
        claims.sort_by(|a, b| {
            b.score.total_cmp(&a.score).then(
                b.claim
                    .source_updated_at_ms
                    .cmp(&a.claim.source_updated_at_ms),
            )
        });
        claims.truncate(limit);

        let mut notes: Vec<RelatedNote> = self
            .about_any(schema::MEMORY_NOTES, &seeds, limit * 4)
            .await
            .into_iter()
            .filter_map(|(_, doc, shared)| {
                Some(RelatedNote {
                    namespace: doc.get("namespace")?.as_str()?.to_string(),
                    path: doc.get("path")?.as_str()?.to_string(),
                    score: shared.len() as f64,
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
    fn concept_graph_name_tracks_model() {
        assert_ne!(
            concept_graph_name("fastino/gliner2.5-base-v1"),
            concept_graph_name("fastino/GLiNER2.5-multi-Decide")
        );
        assert!(concept_graph_name("m").starts_with("codex_concepts_"));
    }
}
