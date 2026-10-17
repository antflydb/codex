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

/// The judgment request for `pairs`, numbered from 1.
pub(crate) fn judge_input(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .enumerate()
        .map(|(index, (a, b))| format!("{}. \"{a}\" | \"{b}\"", index + 1))
        .collect::<Vec<_>>()
        .join("\n")
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
    let mut report = Consolidation::default();
    let mut pairs: Vec<(Concept, Concept)> = Vec::new();
    for concept in pending {
        if let Some(twin) = knowledge.lexical_twin(&concept).await? {
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
            {
                continue;
            }
            if knowledge.is_canonical_concept(&neighbor.key).await? {
                candidate = Some(neighbor);
                break;
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
    let names = pairs
        .iter()
        .map(|(concept, neighbor)| (concept.name.clone(), neighbor.name.clone()))
        .collect();
    let verdicts = judge(names).await?;
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
                            ("Antfly database", "Antfly") | ("Antfly", "Antfly database")
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
                .any(|(a, b)| a.starts_with("Qwen") && b.starts_with("Qwen")),
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

    #[test]
    fn judgments_default_to_different() -> anyhow::Result<()> {
        let pairs = vec![
            ("Antfly".to_string(), "Antfly database".to_string()),
            ("Codex".to_string(), "Codex Cloud".to_string()),
            ("PageRank".to_string(), "Personalized PageRank".to_string()),
        ];
        assert_eq!(
            judge_input(&pairs),
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
