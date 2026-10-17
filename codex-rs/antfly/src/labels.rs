//! Label logs for calibrating and benchmarking local models.
//!
//! Every model call that makes a decision on Codex's behalf can become a
//! labeled example later: a typed decision's full response next to the
//! verdict Codex applied ([`record_decision`]), and each turn sent for claim
//! extraction next to how many claims the extractor found in it
//! ([`record_extraction`]), which labels whether the turn held anything
//! durable. Recording is best-effort: callers log failures and carry on.

use std::hash::Hash;
use std::hash::Hasher;

use serde_json::Value;

use crate::Antfly;
use crate::error::AntflyResult;
use crate::sql_params;

/// One typed decision and the verdict Codex applied.
#[derive(Clone, Debug)]
pub struct DecisionRecord<'a> {
    /// What was decided, e.g. `supersession`.
    pub kind: &'a str,
    pub model: &'a str,
    /// What the decision is about (claim ids, say), when known.
    pub subject: Option<String>,
    /// The decide request's input text.
    pub input: &'a str,
    /// The decide response, as returned.
    pub response: &'a Value,
    pub result: Option<bool>,
    /// The probability the verdict was thresholded on.
    pub score: Option<f64>,
    pub threshold_bp: Option<u32>,
}

pub async fn record_decision(antfly: &Antfly, record: DecisionRecord<'_>) -> AntflyResult<()> {
    let now = now_ms();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    record.input.hash(&mut hasher);
    record.model.hash(&mut hasher);
    let id = format!("{}:{now}:{:016x}", record.kind, hasher.finish());
    let sql = antfly.sql().await?;
    sql.execute(
        "INSERT INTO codex_decision_log \
         (id, kind, model, subject, input, response, result, score, threshold_bp, created_at_ms) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO NOTHING",
        sql_params![
            id,
            record.kind,
            record.model,
            record.subject,
            record.input,
            record.response.to_string(),
            record.result,
            record.score,
            record.threshold_bp,
            now
        ],
    )
    .await?;
    Ok(())
}

/// A turn sent for claim extraction.
#[derive(Clone, Debug)]
pub struct ExtractedTurn {
    pub turn_id: Option<String>,
    pub start: u64,
    pub end: u64,
    /// Characters of rendered evidence sent for the turn.
    pub chars: usize,
    /// Claims the extractor attributed to the turn.
    pub claims: usize,
}

/// One extraction request: its turns and what came back.
#[derive(Clone, Debug)]
pub struct ExtractionRecord<'a> {
    pub thread_id: &'a str,
    pub model: &'a str,
    pub turns: Vec<ExtractedTurn>,
    pub claims: usize,
    pub retracts: usize,
}

pub async fn record_extraction(antfly: &Antfly, record: ExtractionRecord<'_>) -> AntflyResult<()> {
    let now = now_ms();
    let batch_turns = record.turns.len();
    let sql = antfly.sql().await?;
    for turn in record.turns {
        sql.execute(
            "INSERT INTO codex_extraction_log \
             (thread_id, turn_start, turn_end, turn_id, chars, claims, batch_turns, \
              batch_claims, batch_retracts, model, extracted_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (thread_id, turn_start) DO UPDATE SET \
             turn_end = excluded.turn_end, turn_id = excluded.turn_id, chars = excluded.chars, \
             claims = excluded.claims, batch_turns = excluded.batch_turns, \
             batch_claims = excluded.batch_claims, batch_retracts = excluded.batch_retracts, \
             model = excluded.model, extracted_at_ms = excluded.extracted_at_ms",
            sql_params![
                record.thread_id,
                count(turn.start),
                count(turn.end),
                turn.turn_id,
                count(turn.chars),
                count(turn.claims),
                count(batch_turns),
                count(record.claims),
                count(record.retracts),
                record.model,
                now
            ],
        )
        .await?;
    }
    Ok(())
}

/// How triage at one threshold would have done against extraction.
#[derive(Clone, Debug, PartialEq)]
pub struct TriagePoint {
    pub threshold: f64,
    /// Turns that produced claims and would still be extracted.
    pub kept_with_claims: usize,
    pub with_claims: usize,
    /// Turns that would be skipped.
    pub skipped: usize,
}

/// Triage verdicts scored against what extraction found for the same turns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TriageReport {
    /// Turns with both a triage score and an extraction result.
    pub turns: usize,
    pub points: Vec<TriagePoint>,
}

/// Scores logged triage verdicts against extraction results at
/// `thresholds`: recall on turns that produced claims, and turns skipped.
pub async fn triage_report(antfly: &Antfly, thresholds: &[f64]) -> AntflyResult<TriageReport> {
    let sql = antfly.sql().await?;
    let mut scores = std::collections::HashMap::new();
    for row in sql
        .fetch_all(
            "SELECT subject, score FROM codex_decision_log WHERE kind = 'triage'",
            vec![],
        )
        .await?
    {
        if let (Some(subject), Some(score)) = (row.opt_string("subject")?, row.opt_f64("score")?) {
            scores.insert(subject, score);
        }
    }
    let mut labeled: Vec<(f64, bool)> = Vec::new();
    for row in sql
        .fetch_all(
            "SELECT thread_id, turn_start, claims FROM codex_extraction_log",
            vec![],
        )
        .await?
    {
        let subject = format!("{}:{}", row.string("thread_id")?, row.i64("turn_start")?);
        if let Some(score) = scores.get(&subject) {
            labeled.push((*score, row.i64("claims")? > 0));
        }
    }
    let with_claims = labeled.iter().filter(|(_, claims)| *claims).count();
    Ok(TriageReport {
        turns: labeled.len(),
        points: thresholds
            .iter()
            .map(|&threshold| TriagePoint {
                threshold,
                kept_with_claims: labeled
                    .iter()
                    .filter(|(score, claims)| *claims && *score >= threshold)
                    .count(),
                with_claims,
                skipped: labeled
                    .iter()
                    .filter(|(score, _)| *score < threshold)
                    .count(),
            })
            .collect(),
    })
}

fn count(value: impl TryInto<i64>) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}
