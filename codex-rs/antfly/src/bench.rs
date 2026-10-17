//! Benchmarking decide models on Codex's own labeled questions.
//!
//! Each task replays labeled inputs through a model and keeps the score
//! Codex would threshold, next to the label. [`TaskResult`] then reports
//! ranking quality (AUC) and precision/recall at thresholds, so models can
//! be compared per question and thresholds picked from data.

use std::time::Instant;

use serde_json::Value;

use crate::Antfly;
use crate::knowledge::supersession_request;
use crate::knowledge::supersession_score;

/// Hand-labeled supersession pairs (`earlier`, `later`, `replaces`).
pub const SUPERSESSION_FIXTURE: &str =
    include_str!("../tests/fixtures/knowledge_eval/supersession.jsonl");

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
