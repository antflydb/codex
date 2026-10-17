//! Benchmarking decide models on Codex's own labeled questions.
//!
//! Each task replays labeled inputs through a model and keeps the score
//! Codex would threshold, next to the label. [`TaskResult`] then reports
//! ranking quality (AUC) and precision/recall at thresholds, so models can
//! be compared per question and thresholds picked from data.

use std::time::Instant;

use serde_json::Value;

use crate::Antfly;
use crate::knowledge::concept_merge_request;
use crate::knowledge::concept_merge_score;
use crate::knowledge::concept_relation_answer;
use crate::knowledge::concept_relation_request;
use crate::knowledge::supersession_request;
use crate::knowledge::supersession_score;

/// Hand-labeled supersession pairs (`earlier`, `later`, `replaces`).
pub const SUPERSESSION_FIXTURE: &str =
    include_str!("../tests/fixtures/knowledge_eval/supersession.jsonl");

/// Hand-labeled concept pairs: do `a` and `b` name the same thing (`same`)?
/// Each side carries its description, written the way Codex writes them.
pub const CONCEPT_MERGE_FIXTURE: &str =
    include_str!("../tests/fixtures/knowledge_eval/concept_merge.jsonl");

/// Hand-labeled directed concept relations: `a <relation> b`, where
/// relation is `is_a`, `part_of`, `alternative_to` or `none`.
pub const CONCEPT_RELATIONS_FIXTURE: &str =
    include_str!("../tests/fixtures/knowledge_eval/concept_relations.jsonl");

/// One labeled input and the score a model gave it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    pub label: bool,
    pub score: f64,
}

/// One model's results on one task.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskResult {
    pub task: String,
    pub model: String,
    pub samples: Vec<Sample>,
    /// Inputs the model rejected or answered without a score.
    pub failures: usize,
    pub first_error: Option<String>,
    pub latencies_ms: Vec<u64>,
    /// Task-specific results beyond the binary score, one line each.
    pub notes: Vec<String>,
}

/// Precision and recall of `score >= threshold` against the labels.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Operating {
    pub threshold: f64,
    pub true_positives: usize,
    pub false_positives: usize,
    pub false_negatives: usize,
}

impl Operating {
    pub fn precision(&self) -> Option<f64> {
        let predicted = self.true_positives + self.false_positives;
        (predicted > 0).then(|| self.true_positives as f64 / predicted as f64)
    }

    pub fn recall(&self) -> Option<f64> {
        let positives = self.true_positives + self.false_negatives;
        (positives > 0).then(|| self.true_positives as f64 / positives as f64)
    }
}

impl TaskResult {
    pub fn new(task: &str, model: &str) -> Self {
        Self {
            task: task.to_string(),
            model: model.to_string(),
            ..Self::default()
        }
    }

    /// Times one model call and records its score, or the failure.
    pub async fn record<F>(&mut self, label: bool, call: F)
    where
        F: std::future::Future<Output = Result<Option<f64>, String>>,
    {
        let started = Instant::now();
        let outcome = call.await;
        self.record_scored(label, outcome, started.elapsed());
    }

    /// Records a score computed elsewhere, e.g. one of several scores
    /// read from a single model call.
    pub fn record_scored(
        &mut self,
        label: bool,
        outcome: Result<Option<f64>, String>,
        elapsed: std::time::Duration,
    ) {
        self.latencies_ms
            .push(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
        match outcome {
            Ok(Some(score)) => self.samples.push(Sample { label, score }),
            Ok(None) => {
                self.failures += 1;
                self.first_error
                    .get_or_insert_with(|| "response without a score".to_string());
            }
            Err(err) => {
                self.failures += 1;
                self.first_error.get_or_insert(err);
            }
        }
    }

    pub fn positives(&self) -> usize {
        self.samples.iter().filter(|sample| sample.label).count()
    }

    /// Probability that a random positive outscores a random negative (ties
    /// count half); `None` without both classes.
    pub fn auc(&self) -> Option<f64> {
        let positives: Vec<f64> = self
            .samples
            .iter()
            .filter(|s| s.label)
            .map(|s| s.score)
            .collect();
        let negatives: Vec<f64> = self
            .samples
            .iter()
            .filter(|s| !s.label)
            .map(|s| s.score)
            .collect();
        if positives.is_empty() || negatives.is_empty() {
            return None;
        }
        let mut wins = 0.0;
        for p in &positives {
            for n in &negatives {
                wins += if p > n {
                    1.0
                } else if p == n {
                    0.5
                } else {
                    0.0
                };
            }
        }
        Some(wins / (positives.len() * negatives.len()) as f64)
    }

    pub fn at(&self, threshold: f64) -> Operating {
        let mut operating = Operating {
            threshold,
            true_positives: 0,
            false_positives: 0,
            false_negatives: 0,
        };
        for sample in &self.samples {
            match (sample.score >= threshold, sample.label) {
                (true, true) => operating.true_positives += 1,
                (true, false) => operating.false_positives += 1,
                (false, true) => operating.false_negatives += 1,
                (false, false) => {}
            }
        }
        operating
    }

    pub fn median_latency_ms(&self) -> Option<u64> {
        let mut sorted = self.latencies_ms.clone();
        sorted.sort_unstable();
        sorted.get(sorted.len() / 2).copied()
    }
}

/// Scores the supersession fixture with `model`.
pub async fn supersession(antfly: &Antfly, model: &str) -> TaskResult {
    let mut result = TaskResult::new("supersession", model);
    for line in SUPERSESSION_FIXTURE
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let Ok(case) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let (Some(earlier), Some(later), Some(label)) = (
            case["earlier"].as_str(),
            case["later"].as_str(),
            case["replaces"].as_bool(),
        ) else {
            continue;
        };
        let request = supersession_request(model, earlier, later);
        result
            .record(label, async {
                antfly
                    .decide(&request)
                    .await
                    .map(|response| supersession_score(&response))
                    .map_err(|err| err.to_string())
            })
            .await;
    }
    result
}

fn fixture_cases(fixture: &str) -> impl Iterator<Item = Value> + '_ {
    fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
}

/// Scores the concept merge fixture: p(same) against the `same` label,
/// with both descriptions in the input, or names only.
pub async fn concept_merge(antfly: &Antfly, model: &str, descriptions: bool) -> TaskResult {
    let task = if descriptions {
        "concept_merge"
    } else {
        "concept_merge_names"
    };
    let mut result = TaskResult::new(task, model);
    for case in fixture_cases(CONCEPT_MERGE_FIXTURE) {
        let (Some(a), Some(b), Some(label)) = (
            case["a"].as_str(),
            case["b"].as_str(),
            case["same"].as_bool(),
        ) else {
            continue;
        };
        let describe = |side: &str| {
            descriptions
                .then(|| case[format!("{side}_description")].as_str())
                .flatten()
        };
        let request = concept_merge_request(model, a, describe("a"), b, describe("b"));
        result
            .record(label, async {
                antfly
                    .decide(&request)
                    .await
                    .map(|response| concept_merge_score(&response))
                    .map_err(|err| err.to_string())
            })
            .await;
    }
    result
}

/// Scores the concept relation fixture. The binary score is relation
/// detection (1 − p(none) against "related"); the note is the share of
/// related pairs whose top answer has the right direction and kind.
pub async fn concept_relation(antfly: &Antfly, model: &str) -> TaskResult {
    let mut result = TaskResult::new("concept_relation", model);
    let (mut typed, mut related) = (0usize, 0usize);
    for case in fixture_cases(CONCEPT_RELATIONS_FIXTURE) {
        let (Some(a), Some(b), Some(relation)) = (
            case["a"].as_str(),
            case["b"].as_str(),
            case["relation"].as_str(),
        ) else {
            continue;
        };
        let request = concept_relation_request(
            model,
            a,
            case["a_description"].as_str(),
            b,
            case["b_description"].as_str(),
        );
        let answer = antfly.decide(&request).await;
        if relation != "none" {
            related += 1;
            if let Ok(Some((Some((from, kind, to, _)), _))) =
                answer.as_ref().map(concept_relation_answer)
            {
                let expected_alternative = relation == "alternative_to" && kind == relation;
                if expected_alternative || (kind == relation && from == "a" && to == "b") {
                    typed += 1;
                }
            }
        }
        result
            .record(relation != "none", async {
                answer
                    .map(|response| concept_relation_answer(&response).map(|(_, related)| related))
                    .map_err(|err| err.to_string())
            })
            .await;
    }
    result.notes.push(format!(
        "relation and direction right on {typed} of {related} related pairs"
    ));
    result
}

/// One word from a local generator: the first of `choices` its answer
/// starts with.
async fn generator_choice(
    antfly: &Antfly,
    model: &str,
    system: &str,
    user: &str,
    choices: &[&str],
) -> Result<Option<String>, String> {
    let request = serde_json::json!({
        "model": model,
        "max_tokens": 8,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": format!("{user}\nAnswer with exactly one of: {}.", choices.join(", "))},
        ],
    });
    let response = antfly
        .generate(&request)
        .await
        .map_err(|err| err.to_string())?;
    let text = response
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .trim_matches(|c: char| !c.is_alphanumeric() && c != '_')
        .to_lowercase();
    Ok(choices
        .iter()
        .find(|choice| text.starts_with(**choice))
        .map(|choice| (*choice).to_string()))
}

fn generator_side(case: &Value, key: &str) -> String {
    let name = case[key].as_str().unwrap_or_default();
    match case[format!("{key}_description")].as_str() {
        Some(description) => format!("{name} — {description}"),
        None => name.to_string(),
    }
}

/// The merge fixture judged by a local generator that sees descriptions.
pub async fn concept_merge_generator(antfly: &Antfly, model: &str) -> TaskResult {
    const SYSTEM: &str = "You consolidate the concept index of a knowledge graph built from \
software engineering sessions. Decide whether two concept names refer to the same thing. `same` \
only for synonyms, abbreviations, alternative spellings, or a name with or without a qualifier \
that does not change what it refers to. Different versions, a part or kind of something, sibling \
products and merely related things are not the same. When unsure, do not answer `same`.";
    let mut result = TaskResult::new("concept_merge_generator", model);
    for case in fixture_cases(CONCEPT_MERGE_FIXTURE) {
        let label = case["same"].as_bool().unwrap_or(false);
        let user = format!(
            "A: {}\nB: {}",
            generator_side(&case, "a"),
            generator_side(&case, "b")
        );
        result
            .record(label, async {
                generator_choice(antfly, model, SYSTEM, &user, &["same", "different"])
                    .await
                    .map(|choice| choice.map(|choice| if choice == "same" { 1.0 } else { 0.0 }))
            })
            .await;
    }
    result
}

/// The relation fixture judged by a local generator that sees descriptions.
pub async fn concept_relation_generator(antfly: &Antfly, model: &str) -> TaskResult {
    const SYSTEM: &str = "You link the concept index of a knowledge graph built from software \
engineering sessions. Say how A relates to B: `a_is_b` when A is a kind, version or instance of \
B; `b_is_a` for the reverse; `a_part_b` when A is a part, feature or component of B; `b_part_a` \
for the reverse; `alternatives` when A and B are alternatives for the same purpose; `none` \
otherwise, including when they are merely used together.";
    const CHOICES: &[&str] = &[
        "a_is_b",
        "b_is_a",
        "a_part_b",
        "b_part_a",
        "alternatives",
        "none",
    ];
    let mut result = TaskResult::new("concept_relation_generator", model);
    let (mut typed, mut related) = (0usize, 0usize);
    for case in fixture_cases(CONCEPT_RELATIONS_FIXTURE) {
        let expected = case["relation"].as_str().unwrap_or("none").to_string();
        let user = format!(
            "A: {}\nB: {}",
            generator_side(&case, "a"),
            generator_side(&case, "b")
        );
        let answer = generator_choice(antfly, model, SYSTEM, &user, CHOICES).await;
        if expected != "none" {
            related += 1;
            if let Ok(Some(got)) = &answer
                && matches!(
                    (expected.as_str(), got.as_str()),
                    ("is_a", "a_is_b")
                        | ("part_of", "a_part_b")
                        | ("alternative_to", "alternatives")
                )
            {
                typed += 1;
            }
        }
        result
            .record(expected != "none", async {
                answer.map(|got| got.map(|got| if got == "none" { 0.0 } else { 1.0 }))
            })
            .await;
    }
    result.notes.push(format!(
        "relation and direction right on {typed} of {related} related pairs"
    ));
    result
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::Sample;
    use super::TaskResult;

    #[test]
    fn auc_and_operating_points() {
        let mut result = TaskResult::new("t", "m");
        for (label, score) in [(true, 0.9), (true, 0.6), (false, 0.7), (false, 0.2)] {
            result.samples.push(Sample { label, score });
        }
        assert_eq!(result.auc(), Some(0.75));
        let at = result.at(0.65);
        assert_eq!(
            (at.true_positives, at.false_positives, at.false_negatives),
            (1, 1, 1)
        );
        assert_eq!((at.precision(), at.recall()), (Some(0.5), Some(0.5)));
        assert_eq!(result.positives(), 2);
    }
}
