//! Concept consolidation: merging promoted concepts that name the same
//! thing, so claims and notes about it meet at one node.
//!
//! The resolver mints concept keys deterministically (`uslug` of the
//! mention), so "ThreadStore" and "thread store", or "Antfly" and "Antfly
//! database", start as separate concepts. Embeddings alone cannot tell
//! synonyms from versions or siblings: with EmbeddingGemma 2, "EmbeddingGemma
//! 2" and "EmbeddingGemma" score 0.986 and "Qwen3" and "Qwen2" 0.957, above
//! most true synonyms, and Laya judges "Antfly" and "Antfarm" the same thing.
//! So each new concept goes through three gates:
//!
//! 1. Lexical identity ([`concept_identity`]): equal identities merge with no
//!    model. Separators, case and plural `s` are ignored; digits are not.
//! 2. Candidates: the nearest examined concept by name embedding, above
//!    `CONCEPT_SIMILARITY_FLOOR`, with the same number tokens.
//! 3. Judgment: candidate pairs go to the memory extraction model in one
//!    request per pass, which decides whether both names mean the same thing.
//!    Each name carries its description when it has one; on the labeled
//!    merge fixture this lifts the judge from 83% to 94% precision and
//!    recall. A concept whose name has no close neighbor can still be paired
//!    through its description's nearest neighbor (synonyms with dissimilar
//!    names, "KG" and "knowledge graph").
//!
//! [`relate`] then links concepts that are different things: `is_a`,
//! `part_of` and `alternative_to` edges, judged by the same model on pairs
//! that are co-mentioned in claims or close by description.
//!
//! Merges use Antfly's durable `merge_resolver_keys`, which redirects the
//! old key and replays affected extractions. Each examined concept is
//! recorded in `codex_concept_reviews` so it is never judged twice.

use std::future::Future;

use codex_antfly::knowledge::Concept;
use codex_antfly::knowledge::Knowledge;
use codex_antfly::knowledge::concept_identity;
use codex_antfly::knowledge::number_tokens;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use codex_antfly::knowledge::CONCEPT_SIMILARITY_FLOOR as SIMILARITY_FLOOR;
const NEIGHBORS: usize = 5;
/// Concepts examined per pass; the rest wait for the next one.
const MAX_CONCEPTS_PER_PASS: usize = 200;
/// Pairs judged per pass (one model request).
const MAX_PAIRS_PER_PASS: usize = 40;
/// Description similarity (relative to the concept's own) above which a
/// description neighbor becomes a merge candidate.
const DESCRIPTION_MERGE_FLOOR: f64 = 0.9;
/// Description similarity above which a pair is asked about relations.
const DESCRIPTION_RELATION_FLOOR: f64 = 0.85;
/// Description neighbors considered per concept.
const DESCRIPTION_NEIGHBORS: usize = 3;

pub(crate) const JUDGE_INSTRUCTIONS: &str = "You consolidate the concept index of a \
knowledge graph built from software engineering sessions. For each numbered pair of names, \
answer `same: true` only when both names refer to the same thing: synonyms, abbreviations, \
alternative spellings, or a name with or without a qualifier that does not change what it \
refers to (\"Antfly\" and \"Antfly database\"). Answer false for different versions \
(\"Qwen3\" and \"Qwen2\"), a part, feature or kind of something (\"PageRank\" and \
\"Personalized PageRank\"), sibling products (\"Codex\" and \"Codex Cloud\"), and things that \
are merely related. When unsure, answer false.";

pub(crate) fn judge_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "pairs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pair": {"type": "integer"},
                        "same": {"type": "boolean"},
                    },
                    "required": ["pair", "same"],
                    "additionalProperties": false,
                },
            },
        },
        "required": ["pairs"],
        "additionalProperties": false,
    })
}

/// Added to [`JUDGE_INSTRUCTIONS`] when names carry descriptions.
pub(crate) const DESCRIBED_JUDGE_NOTE: &str = " Each name may be followed by a short description \
of what it refers to in this project, written from the claims about it; use it to tell what each \
name refers to, but descriptions of different things can sound alike.";

/// A concept as the judge sees it: its name, and its description if any.
pub(crate) fn judged_name(name: &str, description: Option<&str>) -> String {
    match description {
        Some(description) => format!("\"{name}\" ({description})"),
        None => format!("\"{name}\""),
    }
}

/// The judgment request for pairs already rendered with [`judged_name`].
pub(crate) fn judge_input_rendered(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .enumerate()
        .map(|(index, (a, b))| format!("{}. {a} | {b}", index + 1))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) const RELATION_INSTRUCTIONS: &str = "You link the concept index of a knowledge \
graph built from software engineering sessions. For each numbered pair A | B (each name may be \
followed by a description of what it refers to in this project), say how A relates to B: \
`a_is_b` when A is a kind, version or instance of B; `b_is_a` for the reverse; `a_part_b` when A \
is a part, feature or component of B; `b_part_a` for the reverse; `alternatives` when A and B \
are alternatives for the same purpose; `none` otherwise, including when they are merely used \
together. Also answer `none` when A or B is a generic word (\"note\", \"content\", \
\"information\", \"add\") rather than a specific thing in this project. When unsure, answer \
`none`.";

pub(crate) const RELATION_CHOICES: &[&str] = &[
    "a_is_b",
    "b_is_a",
    "a_part_b",
    "b_part_a",
    "alternatives",
    "none",
];

pub(crate) fn relation_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "pairs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pair": {"type": "integer"},
                        "relation": {"type": "string", "enum": RELATION_CHOICES},
                    },
                    "required": ["pair", "relation"],
                    "additionalProperties": false,
                },
            },
        },
        "required": ["pairs"],
        "additionalProperties": false,
    })
}

#[derive(Deserialize)]
struct RelationJudgments {
    pairs: Vec<RelationJudgment>,
}

#[derive(Deserialize)]
struct RelationJudgment {
    pair: usize,
    relation: String,
}

/// The model's relation per pair; pairs it skipped are `none`.
pub(crate) fn parse_relations(answer: &str, count: usize) -> anyhow::Result<Vec<String>> {
    let judgments: RelationJudgments = serde_json::from_str(answer.trim())?;
    let mut relations = vec!["none".to_string(); count];
    for judgment in judgments.pairs {
        if let Some(slot) = judgment
            .pair
            .checked_sub(1)
            .and_then(|index| relations.get_mut(index))
            && RELATION_CHOICES.contains(&judgment.relation.as_str())
        {
            *slot = judgment.relation;
        }
    }
    Ok(relations)
}

#[derive(Deserialize)]
struct Judgments {
    pairs: Vec<Judgment>,
}

#[derive(Deserialize)]
struct Judgment {
    pair: usize,
    same: bool,
}

/// The model's verdict per pair; pairs it skipped count as different.
pub(crate) fn parse_judgments(answer: &str, count: usize) -> anyhow::Result<Vec<bool>> {
    let judgments: Judgments = serde_json::from_str(answer.trim())?;
    let mut same = vec![false; count];
    for judgment in judgments.pairs {
        if let Some(slot) = judgment
            .pair
            .checked_sub(1)
            .and_then(|index| same.get_mut(index))
        {
            *slot = judgment.same;
        }
    }
    Ok(same)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Consolidation {
    /// Concepts examined this pass.
    pub examined: usize,
    /// Merged by lexical identity, without a model.
    pub lexical_merges: usize,
    /// Candidate pairs the model judged.
    pub judged: usize,
    /// Pairs the model judged the same and that were merged.
    pub judged_merges: usize,
}

/// One consolidation pass. `judge` answers whether each pair of names means
/// the same thing.
pub(crate) async fn consolidate<J, F>(
    knowledge: &Knowledge<'_>,
    judge: J,
) -> anyhow::Result<Consolidation>
where
    J: FnOnce(Vec<(String, String)>) -> F,
    F: Future<Output = anyhow::Result<Vec<bool>>>,
{
    let reviewed = knowledge.reviewed_concepts().await?;
    let mut pending: Vec<Concept> = knowledge
        .concepts()
        .await?
        .into_iter()
        .filter(|concept| !reviewed.contains(&concept.key))
        .collect();
    pending.truncate(MAX_CONCEPTS_PER_PASS);
    // Pairs a curator unmerged with a pin are never merged again.
    let curation = knowledge.concept_curation().await?.unwrap_or_default();
    let mut report = Consolidation::default();
    let mut pairs: Vec<(Concept, Concept)> = Vec::new();
    for concept in pending {
        if let Some(twin) = knowledge.lexical_twin(&concept).await?
            && !curation.is_distinct(&concept.key, &twin)
        {
            if knowledge.merge_concept(&concept.key, &twin).await? {
                knowledge
                    .record_concept_review(&concept, Some(&twin))
                    .await?;
                report.lexical_merges += 1;
                report.examined += 1;
            }
            continue;
        }
        let numbers = number_tokens(&concept.name);
        let mut candidate = None;
        let neighbors = knowledge
            .concept_neighbors(&concept.name, NEIGHBORS + 1)
            .await?;
        // Normalize by the concept's own score; without it, by the best.
        let own = neighbors
            .iter()
            .find(|(neighbor, _)| neighbor.key == concept.key)
            .or(neighbors.first())
            .map(|(_, score)| *score)
            .filter(|score| *score > 0.0)
            .unwrap_or(1.0);
        for (neighbor, score) in neighbors {
            if neighbor.key == concept.key
                || score / own < SIMILARITY_FLOOR
                || number_tokens(&neighbor.name) != numbers
                || concept_identity(&neighbor.name) == concept_identity(&concept.name)
                || curation.is_distinct(&concept.key, &neighbor.key)
            {
                continue;
            }
            if knowledge.is_canonical_concept(&neighbor.key).await? {
                candidate = Some(neighbor);
                break;
            }
        }
        if candidate.is_none() {
            let names = knowledge.concepts_by_key().await?;
            for (key, similarity) in knowledge
                .description_neighbors(&concept.key, DESCRIPTION_NEIGHBORS)
                .await?
            {
                let Some(neighbor) = names.get(&key) else {
                    continue;
                };
                if similarity < DESCRIPTION_MERGE_FLOOR
                    || number_tokens(&neighbor.name) != numbers
                    || curation.is_distinct(&concept.key, &key)
                {
                    continue;
                }
                if knowledge.is_canonical_concept(&key).await? {
                    candidate = Some(neighbor.clone());
                    break;
                }
            }
        }
        match candidate {
            // Over this pass's budget: examined next pass.
            Some(_) if pairs.len() >= MAX_PAIRS_PER_PASS => continue,
            // Recorded as canonical now so later concepts in this pass can
            // be compared with it; a merge below updates the record.
            Some(neighbor) => {
                knowledge.record_concept_review(&concept, None).await?;
                pairs.push((concept, neighbor));
            }
            None => knowledge.record_concept_review(&concept, None).await?,
        }
        report.examined += 1;
    }
    if pairs.is_empty() {
        return Ok(report);
    }
    let keys: Vec<String> = pairs
        .iter()
        .flat_map(|(concept, neighbor)| [concept.key.clone(), neighbor.key.clone()])
        .collect();
    let descriptions = knowledge.concept_descriptions(&keys).await?;
    let render = |concept: &Concept| {
        judged_name(
            &concept.name,
            descriptions.get(&concept.key).map(String::as_str),
        )
    };
    let rendered = pairs
        .iter()
        .map(|(concept, neighbor)| (render(concept), render(neighbor)))
        .collect();
    let verdicts = judge(rendered).await?;
    report.judged = pairs.len();
    for ((concept, neighbor), same) in pairs.into_iter().zip(verdicts) {
        if same && knowledge.merge_concept(&concept.key, &neighbor.key).await? {
            knowledge
                .record_concept_review(&concept, Some(&neighbor.key))
                .await?;
            report.judged_merges += 1;
        }
    }
    Ok(report)
}

pub(crate) const GENERIC_INSTRUCTIONS: &str = "You clean the concept index of a knowledge graph \
built from software engineering sessions. Each numbered concept is a name, often followed by a \
short description of what it refers to in this project. Answer `generic: true` only for filler: \
a pronoun, a bare verb, or a single everyday word that could appear in any conversation and names \
nothing in particular, such as \"you\", \"add\", \"delete\", \"note\", \"content\", \
\"information\" or \"summary\". Everything else is not generic: \
- names of tools, technologies, products, files, components, commands or settings, even when short \
(\"DNS\", \"killall\", \"embedder\", \"rollout\"); \
- domain terms of software engineering or of this project, even when made of common words \
(\"agent\", \"benchmarks\", \"dimensions\", \"DNS resolution\", \"sandbox permission\", \
\"elevated permissions\"); \
- any phrase of two or more words that refers to an idea, practice or rule in this project. \
A vague description does not make a concept generic; judge the name. When unsure, answer false.";
/// Concepts screened per pass (one model request).
const MAX_SCREENED_PER_PASS: usize = 40;

pub(crate) fn generic_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "item": {"type": "integer"},
                        "generic": {"type": "boolean"},
                    },
                    "required": ["item", "generic"],
                    "additionalProperties": false,
                },
            },
        },
        "required": ["items"],
        "additionalProperties": false,
    })
}

/// The screening request: numbered concepts rendered with [`judged_name`].
pub(crate) fn generic_input(concepts: &[String]) -> String {
    concepts
        .iter()
        .enumerate()
        .map(|(index, concept)| format!("{}. {concept}", index + 1))
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Deserialize)]
struct GenericJudgments {
    items: Vec<GenericJudgment>,
}

#[derive(Deserialize)]
struct GenericJudgment {
    item: usize,
    generic: bool,
}

/// The model's verdict per concept; concepts it skipped are specific.
pub(crate) fn parse_generic(answer: &str, count: usize) -> anyhow::Result<Vec<bool>> {
    let judgments: GenericJudgments = serde_json::from_str(answer.trim())?;
    let mut generic = vec![false; count];
    for judgment in judgments.items {
        if let Some(slot) = judgment
            .item
            .checked_sub(1)
            .and_then(|index| generic.get_mut(index))
        {
            *slot = judgment.generic;
        }
    }
    Ok(generic)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GenericScreen {
    /// Concepts screened this pass.
    pub screened: usize,
    /// Concepts judged generic.
    pub generic: usize,
    /// Generic concepts suppressed (enforce mode only).
    pub suppressed: usize,
}

/// One screening pass: concepts never screened before go to `judge`, which
/// says per concept whether it is a generic word rather than a specific
/// thing. Each verdict is a `generic_concept` label, and each concept is
/// screened once. In shadow mode (the default) generic ones become Antfly
/// curation proposals, listed by `codex knowledge proposals`; in enforce
/// mode they are suppressed, so their mentions stop minting them and their
/// edges are removed. `codex knowledge keep` reverses one.
pub(crate) async fn screen_generic<J, F>(
    knowledge: &Knowledge<'_>,
    antfly: &codex_antfly::Antfly,
    model: &str,
    judge: J,
) -> anyhow::Result<GenericScreen>
where
    J: FnOnce(Vec<String>) -> F,
    F: Future<Output = anyhow::Result<Vec<bool>>>,
{
    let mode = knowledge.concept_screening();
    if mode == codex_antfly::TriageMode::Off {
        return Ok(GenericScreen::default());
    }
    let Some(curation) = knowledge.concept_curation().await? else {
        // The backend cannot suppress; nothing to act on.
        return Ok(GenericScreen::default());
    };
    let screened =
        codex_antfly::labels::latest_results(antfly, codex_antfly::labels::GENERIC_CONCEPT).await?;
    let merged = knowledge.merged_concept_keys().await?;
    let pending: Vec<Concept> = knowledge
        .concepts()
        .await?
        .into_iter()
        .filter(|concept| {
            !screened.contains_key(&concept.key)
                && !merged.contains(&concept.key)
                && !curation.suppressed.contains(&concept.key)
        })
        .take(MAX_SCREENED_PER_PASS)
        .collect();
    if pending.is_empty() {
        return Ok(GenericScreen::default());
    }
    let keys: Vec<String> = pending.iter().map(|concept| concept.key.clone()).collect();
    let descriptions = knowledge.concept_descriptions(&keys).await?;
    let rendered: Vec<String> = pending
        .iter()
        .map(|concept| {
            judged_name(
                &concept.name,
                descriptions.get(&concept.key).map(String::as_str),
            )
        })
        .collect();
    let verdicts = judge(rendered.clone()).await?;
    let mut report = GenericScreen {
        screened: pending.len(),
        ..GenericScreen::default()
    };
    for ((concept, rendered), generic) in pending.iter().zip(&rendered).zip(verdicts) {
        if generic {
            report.generic += 1;
            let reason = format!("generic word, screened by {model}");
            if mode == codex_antfly::TriageMode::Enforce {
                if knowledge
                    .suppress_concept(
                        &concept.key,
                        codex_antfly::knowledge::GENERIC_SCREEN_ACTOR,
                        Some(&reason),
                    )
                    .await?
                    .is_some()
                {
                    report.suppressed += 1;
                }
            } else {
                knowledge
                    .propose_generic(
                        &concept.key,
                        &reason,
                        json!({"model": model, "judged": rendered}),
                    )
                    .await?;
            }
        }
        codex_antfly::labels::record_decision(
            antfly,
            codex_antfly::labels::DecisionRecord {
                kind: codex_antfly::labels::GENERIC_CONCEPT,
                model,
                subject: Some(concept.key.clone()),
                input: rendered,
                response: &json!({"generic": generic}),
                result: Some(generic),
                score: None,
                threshold_bp: None,
            },
        )
        .await?;
    }
    Ok(report)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Relations {
    /// Concept pairs judged this pass.
    pub judged: usize,
    /// Pairs that became a link.
    pub linked: usize,
}

/// One relation pass. Pairs of described concepts that are co-mentioned in
/// claims, or close by description, go to `relate`, which answers
/// `a_is_b`, `b_is_a`, `a_part_b`, `b_part_a`, `alternatives` or `none`
/// per pair; each pair is judged once.
pub(crate) async fn relate<J, F>(knowledge: &Knowledge<'_>, relate: J) -> anyhow::Result<Relations>
where
    J: FnOnce(Vec<(String, String)>) -> F,
    F: Future<Output = anyhow::Result<Vec<String>>>,
{
    let names = knowledge.concepts_by_key().await?;
    let merged = knowledge.merged_concept_keys().await?;
    let keys: Vec<String> = names
        .keys()
        .filter(|key| !merged.contains(*key))
        .cloned()
        .collect();
    let descriptions = knowledge.concept_descriptions(&keys).await?;
    let reviewed = knowledge.related_pairs_reviewed().await?;
    let mut pairs: Vec<(String, String)> = Vec::new();
    let consider = |a: &str, b: &str, pairs: &mut Vec<(String, String)>| {
        let (a, b) = if a <= b { (a, b) } else { (b, a) };
        let pair = (a.to_string(), b.to_string());
        if a != b
            && descriptions.contains_key(a)
            && descriptions.contains_key(b)
            && !reviewed.contains(&pair)
            && !pairs.contains(&pair)
        {
            pairs.push(pair);
        }
    };
    for key in descriptions.keys() {
        if pairs.len() >= MAX_PAIRS_PER_PASS {
            break;
        }
        for (other, _) in knowledge.co_mentioned(key, 50).await {
            consider(key, &other, &mut pairs);
        }
        for (other, similarity) in knowledge
            .description_neighbors(key, DESCRIPTION_NEIGHBORS)
            .await?
        {
            if similarity >= DESCRIPTION_RELATION_FLOOR {
                consider(key, &other, &mut pairs);
            }
        }
    }
    pairs.truncate(MAX_PAIRS_PER_PASS);
    if pairs.is_empty() {
        return Ok(Relations::default());
    }
    let render = |key: &str| {
        judged_name(
            names
                .get(key)
                .map(|concept| concept.name.as_str())
                .unwrap_or(key),
            descriptions.get(key).map(String::as_str),
        )
    };
    let rendered = pairs.iter().map(|(a, b)| (render(a), render(b))).collect();
    let answers = relate(rendered).await?;
    let mut report = Relations {
        judged: pairs.len(),
        linked: 0,
    };
    for ((a, b), answer) in pairs.iter().zip(answers) {
        let edge = match answer.as_str() {
            "a_is_b" => Some((a.as_str(), "is_a", b.as_str())),
            "b_is_a" => Some((b.as_str(), "is_a", a.as_str())),
            "a_part_b" => Some((a.as_str(), "part_of", b.as_str())),
            "b_part_a" => Some((b.as_str(), "part_of", a.as_str())),
            "alternatives" => Some((a.as_str(), "alternative_to", b.as_str())),
            _ => None,
        };
        knowledge.record_concept_relation(a, b, edge).await?;
        if edge.is_some() {
            report.linked += 1;
        }
    }
    Ok(report)
}

/// Applies configured aliases (`alias → canonical name`): each alias's
/// concept is merged into its canonical name's. Already-applied aliases
/// render to the same key and are skipped.
pub(crate) async fn apply_aliases(
    knowledge: &Knowledge<'_>,
    aliases: &[(String, String)],
) -> anyhow::Result<usize> {
    let mut applied = 0;
    for (alias, canonical) in aliases {
        let (Some(from), Some(to)) = (
            knowledge.concept_key_for(alias).await?,
            knowledge.concept_key_for(canonical).await?,
        ) else {
            continue;
        };
        if from != to && knowledge.merge_concept(&from, &to).await? {
            applied += 1;
        }
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn has_models(owner: &str) -> bool {
        std::env::var_os("HOME").is_some_and(|home| {
            std::path::Path::new(&home)
                .join(".antfly/inference/models")
                .join(owner)
                .exists()
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn consolidation_merges_variants_and_asks_only_about_plausible_pairs()
    -> anyhow::Result<()> {
        if !has_models("google") || !has_models("fastino") {
            return Ok(());
        }
        let _serial = crate::MODEL_TESTS.acquire().await?;
        let dir = tempfile::tempdir()?;
        let config = codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
        let antfly = codex_antfly::Antfly::open(config)?;
        let knowledge = antfly
            .knowledge()
            .ok_or_else(|| anyhow::anyhow!("no knowledge"))?;
        let names = [
            "Antfarm",
            "Antfly",
            "Antfly database",
            "Qwen2",
            "Qwen3",
            "thread store",
            "ThreadStore",
        ];
        let concepts = antfly.documents(codex_antfly::schema::CONCEPTS);
        concepts
            .write(
                names
                    .iter()
                    .map(|name| {
                        let key = codex_antfly::knowledge::concept_key(name).unwrap_or_default();
                        codex_antfly::Write::put(
                            key,
                            json!({"canonical_name": name, "entity_type": "technology"}),
                        )
                    })
                    .collect(),
            )
            .await?;
        // Name embeddings are indexed asynchronously.
        for _ in 0..150 {
            if concepts.nearest("Qwen3", names.len()).await?.len() == names.len() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let asked = std::sync::Mutex::new(Vec::new());
        let report = consolidate(&knowledge, |pairs: Vec<(String, String)>| {
            asked
                .lock()
                .map(|mut asked| asked.extend(pairs.clone()))
                .ok();
            async move {
                Ok(pairs
                    .iter()
                    .map(|(a, b)| {
                        matches!(
                            (a.as_str(), b.as_str()),
                            ("\"Antfly database\"", "\"Antfly\"")
                                | ("\"Antfly\"", "\"Antfly database\"")
                        )
                    })
                    .collect())
            }
        })
        .await?;
        let asked = asked.into_inner().unwrap_or_default();
        assert_eq!(report.lexical_merges, 1, "{report:?}");
        assert_eq!(report.judged_merges, 1, "{report:?} {asked:?}");
        // Different numbers are never asked about.
        assert!(
            !asked
                .iter()
                .any(|(a, b)| a.contains("Qwen") && b.contains("Qwen")),
            "{asked:?}"
        );
        assert_eq!(
            knowledge
                .concept_key_for("Antfly database")
                .await?
                .as_deref(),
            Some("concept/antfly")
        );
        assert_ne!(
            knowledge.concept_key_for("Qwen2").await?,
            knowledge.concept_key_for("Qwen3").await?
        );
        // A second pass has nothing left to examine.
        let again = consolidate(&knowledge, |_| async { Ok(Vec::new()) }).await?;
        assert_eq!(again, Consolidation::default());
        antfly.close().await?;
        Ok(())
    }

    async fn write_concepts(antfly: &codex_antfly::Antfly, names: &[&str]) -> anyhow::Result<()> {
        antfly
            .documents(codex_antfly::schema::CONCEPTS)
            .write(
                names
                    .iter()
                    .map(|name| {
                        let key = codex_antfly::knowledge::concept_key(name).unwrap_or_default();
                        codex_antfly::Write::put(
                            key,
                            json!({"canonical_name": name, "entity_type": "technology"}),
                        )
                    })
                    .collect(),
            )
            .await?;
        Ok(())
    }

    async fn note_is_generic(concepts: Vec<String>) -> anyhow::Result<Vec<bool>> {
        Ok(concepts
            .iter()
            .map(|concept| concept.starts_with("\"note\""))
            .collect())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generic_concepts_are_proposed_once_in_shadow_mode() -> anyhow::Result<()> {
        if !has_models("fastino") {
            return Ok(());
        }
        let _serial = crate::MODEL_TESTS.acquire().await?;
        let dir = tempfile::tempdir()?;
        let config = codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
        let antfly = codex_antfly::Antfly::open(config)?;
        let knowledge = antfly
            .knowledge()
            .ok_or_else(|| anyhow::anyhow!("no knowledge"))?;
        write_concepts(&antfly, &["note", "Antfly"]).await?;
        let report = screen_generic(&knowledge, &antfly, "test", note_is_generic).await?;
        assert_eq!(
            report,
            GenericScreen {
                screened: 2,
                generic: 1,
                suppressed: 0
            }
        );
        let curation = knowledge.concept_curation().await?.unwrap_or_default();
        assert!(curation.suppressed.is_empty(), "{curation:?}");
        let proposals = knowledge.generic_proposals().await?;
        let keys: Vec<&str> = proposals.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(keys, vec!["concept/note"]);
        // Each concept is screened once, and a decided proposal is no
        // longer open.
        let again = screen_generic(&knowledge, &antfly, "test", note_is_generic).await?;
        assert_eq!(again, GenericScreen::default());
        assert_eq!(
            knowledge
                .decide_proposal(&proposals[0].id, false, codex_antfly::knowledge::USER_ACTOR)
                .await?,
            Some(Vec::new())
        );
        assert!(knowledge.generic_proposals().await?.is_empty());
        antfly.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generic_concepts_are_suppressed_in_enforce_mode() -> anyhow::Result<()> {
        if !has_models("fastino") {
            return Ok(());
        }
        let _serial = crate::MODEL_TESTS.acquire().await?;
        let dir = tempfile::tempdir()?;
        let mut config = codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
        if let Some(knowledge) = config.knowledge.as_mut() {
            knowledge.concept_screening = codex_antfly::TriageMode::Enforce;
        }
        let antfly = codex_antfly::Antfly::open(config)?;
        let knowledge = antfly
            .knowledge()
            .ok_or_else(|| anyhow::anyhow!("no knowledge"))?;
        write_concepts(&antfly, &["note", "Antfly"]).await?;
        let report = screen_generic(&knowledge, &antfly, "test", note_is_generic).await?;
        assert_eq!(
            report,
            GenericScreen {
                screened: 2,
                generic: 1,
                suppressed: 1
            }
        );
        let curation = knowledge.concept_curation().await?.unwrap_or_default();
        assert!(curation.suppressed.contains("concept/note"), "{curation:?}");
        assert_eq!(
            curation.suppression_origins["concept/note"]
                .actor
                .as_deref(),
            Some(codex_antfly::knowledge::GENERIC_SCREEN_ACTOR)
        );
        assert!(knowledge.generic_proposals().await?.is_empty());
        antfly.close().await?;
        Ok(())
    }

    #[test]
    fn generic_verdicts_default_to_specific() -> anyhow::Result<()> {
        assert_eq!(
            generic_input(&["\"note\"".to_string(), "\"DNS\"".to_string()]),
            "1. \"note\"\n2. \"DNS\""
        );
        let answer = json!({"items": [{"item": 1, "generic": true}, {"item": 7, "generic": true}]});
        assert_eq!(parse_generic(&answer.to_string(), 2)?, vec![true, false]);
        Ok(())
    }

    #[test]
    fn judgments_default_to_different() -> anyhow::Result<()> {
        let pairs = [
            ("Antfly".to_string(), "Antfly database".to_string()),
            ("Codex".to_string(), "Codex Cloud".to_string()),
            ("PageRank".to_string(), "Personalized PageRank".to_string()),
        ];
        let rendered: Vec<(String, String)> = pairs
            .iter()
            .map(|(a, b)| (judged_name(a, None), judged_name(b, None)))
            .collect();
        assert_eq!(
            judge_input_rendered(&rendered),
            "1. \"Antfly\" | \"Antfly database\"\n2. \"Codex\" | \"Codex Cloud\"\n3. \"PageRank\" | \"Personalized PageRank\""
        );
        let answer = json!({"pairs": [{"pair": 1, "same": true}, {"pair": 2, "same": false}, {"pair": 9, "same": true}]});
        assert_eq!(
            parse_judgments(&answer.to_string(), 3)?,
            vec![true, false, false]
        );
        Ok(())
    }
}
