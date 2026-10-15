//! The extraction request for one batch of turns: the prompt, its strict
//! output schema, and validation of the model's answer against the thread's
//! existing claims.

use std::collections::HashMap;

use codex_antfly::knowledge::Claim;
use codex_antfly::knowledge::ClaimKind;
use codex_antfly::knowledge::ClaimRef;
use codex_antfly::knowledge::TurnClaim;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::transcript::Turn;

pub(crate) const INSTRUCTIONS: &str = include_str!("prompt.md");
/// Existing claims shown to the model, newest first.
const MAX_EXISTING_CLAIMS: usize = 40;
/// Claims accepted from one answer.
const MAX_CLAIMS: usize = 8;
/// Characters of one claim statement.
const MAX_STATEMENT_CHARS: usize = 400;
/// Characters of rendered turns sent in one request.
pub(crate) const MAX_TURN_CHARS: usize = 60_000;

const KINDS: &[&str] = &[
    "decision",
    "rationale",
    "preference",
    "lesson",
    "constraint",
    "open_question",
    "alternative",
];

pub(crate) fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "turn": {"type": "integer"},
                        "kind": {"type": "string", "enum": KINDS},
                        "statement": {"type": "string"},
                        "replaces": {"type": "array", "items": {"type": "string"}},
                        "motivates": {"type": "array", "items": {"type": "string"}},
                        "rejected_in_favor_of": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["turn", "kind", "statement", "replaces", "motivates", "rejected_in_favor_of"],
                    "additionalProperties": false,
                },
            },
            "retracts": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["claims", "retracts"],
        "additionalProperties": false,
    })
}

/// One request's input plus what is needed to interpret its answer.
pub(crate) struct Request {
    pub(crate) input: String,
    /// Short id shown to the model → claim id.
    existing: HashMap<String, String>,
    /// `[turn N]` label → (turn id, start ordinal).
    turns: Vec<(Option<String>, u64)>,
}

/// Builds the request for `turns` (all with evidence) given the thread's
/// current claims.
pub(crate) fn request(cwd: Option<&str>, current: &[Claim], turns: &[&Turn]) -> Request {
    let mut input = String::new();
    if let Some(cwd) = cwd {
        input.push_str(&format!("Working directory: {cwd}\n\n"));
    }
    let mut existing = HashMap::new();
    let mut shown: Vec<&Claim> = current.iter().collect();
    shown.sort_by(|a, b| (a.turn_ordinal, &a.id).cmp(&(b.turn_ordinal, &b.id)));
    let older = shown.len().saturating_sub(MAX_EXISTING_CLAIMS);
    shown.drain(..older);
    if shown.is_empty() {
        input.push_str("Existing claims from this session: none\n\n");
    } else {
        input.push_str("Existing claims from this session:\n");
        for (index, claim) in shown.iter().enumerate() {
            let short = format!("c{}", index + 1);
            input.push_str(&format!(
                "[{short}] ({}) {}\n",
                claim.kind.as_str(),
                claim.statement
            ));
            existing.insert(short, claim.id.clone());
        }
        input.push('\n');
    }
    input.push_str("New turns:\n");
    let mut labels = Vec::with_capacity(turns.len());
    for (index, turn) in turns.iter().enumerate() {
        input.push_str(&format!(
            "\n[turn {}]\n{}\n",
            index + 1,
            head_and_tail(&turn.text, MAX_TURN_CHARS)
        ));
        labels.push((turn.turn_id.clone(), turn.start));
    }
    Request {
        input,
        existing,
        turns: labels,
    }
}

#[derive(Deserialize)]
struct Output {
    claims: Vec<OutputClaim>,
    retracts: Vec<String>,
}

#[derive(Deserialize)]
struct OutputClaim {
    turn: i64,
    kind: String,
    statement: String,
    replaces: Vec<String>,
    motivates: Vec<String>,
    rejected_in_favor_of: Vec<String>,
}

/// `text` cut to about `max` bytes, keeping its start and its end (where a
/// turn's conclusion is).
fn head_and_tail(text: &str, max: usize) -> std::borrow::Cow<'_, str> {
    if text.len() <= max {
        return text.into();
    }
    let mut head = max * 2 / 3;
    while !text.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = text.len() - (max - head);
    while !text.is_char_boundary(tail) {
        tail += 1;
    }
    format!(
        "{}\n[… middle of turn omitted …]\n{}",
        &text[..head],
        &text[tail..]
    )
    .into()
}

fn truncate(text: &str, max: usize) -> &str {
    match text.char_indices().nth(max) {
        Some((index, _)) => &text[..index],
        None => text,
    }
}

impl Request {
    /// The model's answer as claims and retractions. Unknown short ids and
    /// kinds are dropped; an out-of-range turn is attributed to the last one.
    pub(crate) fn parse(&self, answer: &str) -> anyhow::Result<(Vec<TurnClaim>, Vec<String>)> {
        let output: Output = serde_json::from_str(answer.trim())?;
        let parsed: Vec<Option<(ClaimKind, String)>> = output
            .claims
            .iter()
            .map(|claim| {
                let kind: ClaimKind =
                    serde_json::from_value(Value::String(claim.kind.clone())).ok()?;
                let statement = truncate(claim.statement.trim(), MAX_STATEMENT_CHARS).to_string();
                (!statement.is_empty()).then_some((kind, statement))
            })
            .collect();
        // Answer position of each kept claim → its position in the result.
        let mut kept: HashMap<usize, usize> = HashMap::new();
        for (position, claim) in parsed.iter().enumerate() {
            if claim.is_some() && kept.len() < MAX_CLAIMS {
                kept.insert(position, kept.len());
            }
        }
        let refs = |targets: &[String]| -> Vec<ClaimRef> {
            targets
                .iter()
                .filter_map(|target| {
                    if let Some(id) = self.existing.get(target) {
                        return Some(ClaimRef::Existing(id.clone()));
                    }
                    let position: usize = target.strip_prefix('n')?.parse().ok()?;
                    kept.get(&position.checked_sub(1)?)
                        .map(|index| ClaimRef::New(*index))
                })
                .collect()
        };
        let mut claims = Vec::with_capacity(kept.len());
        for (position, (claim, parsed)) in output.claims.iter().zip(parsed).enumerate() {
            let (Some((kind, statement)), true) = (parsed, kept.contains_key(&position)) else {
                continue;
            };
            let index = usize::try_from(claim.turn.saturating_sub(1))
                .unwrap_or(0)
                .min(self.turns.len().saturating_sub(1));
            let Some((turn_id, turn_ordinal)) = self.turns.get(index).cloned() else {
                continue;
            };
            claims.push(TurnClaim {
                kind,
                statement,
                turn_id,
                turn_ordinal,
                replaces: claim
                    .replaces
                    .iter()
                    .filter_map(|short| self.existing.get(short).cloned())
                    .collect(),
                // Only explanations motivate and only alternatives are
                // rejected; the model sometimes links a decision backwards.
                motivates: if matches!(
                    kind,
                    ClaimKind::Rationale | ClaimKind::Lesson | ClaimKind::Constraint
                ) {
                    refs(&claim.motivates)
                } else {
                    Vec::new()
                },
                rejected_in_favor_of: if kind == ClaimKind::Alternative {
                    refs(&claim.rejected_in_favor_of)
                } else {
                    Vec::new()
                },
            });
        }
        let retracts = output
            .retracts
            .iter()
            .filter_map(|short| self.existing.get(short).cloned())
            .collect();
        Ok((claims, retracts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_antfly::knowledge::ClaimStatus;
    use pretty_assertions::assert_eq;

    fn turn(id: &str, start: u64, text: &str) -> Turn {
        Turn {
            turn_id: Some(id.to_string()),
            start,
            end: start + 5,
            finished: true,
            text: text.to_string(),
        }
    }

    fn claim(id: &str, ordinal: u64, statement: &str) -> Claim {
        Claim {
            id: id.to_string(),
            kind: ClaimKind::Decision,
            statement: statement.to_string(),
            status: ClaimStatus::Current,
            thread_id: "t".to_string(),
            version: "turns".to_string(),
            cwd: None,
            source_updated_at_ms: 0,
            generated_at_ms: 0,
            supersedes: Vec::new(),
            superseded_by: None,
            motivates: Vec::new(),
            rejected_in_favor_of: Vec::new(),
            turn_id: None,
            turn_ordinal: Some(ordinal),
            settled: false,
        }
    }

    #[test]
    fn answers_map_short_ids_and_turn_labels_back() -> anyhow::Result<()> {
        let current = vec![
            claim(
                "turns:t:2:000",
                2,
                "The importer writes 256 items per batch.",
            ),
            claim("turns:t:2:001", 2, "Imports run on a debug build."),
        ];
        let turns = [
            turn("turn-a", 10, "User: bigger batches"),
            turn("turn-b", 20, "User: release"),
        ];
        let refs: Vec<&Turn> = turns.iter().collect();
        let request = request(Some("/repo"), &current, &refs);
        assert!(
            request
                .input
                .contains("[c1] (decision) The importer writes 256 items per batch.")
        );
        assert!(request.input.contains("[turn 2]\nUser: release"));

        let answer = json!({
            "claims": [
                {"turn": 1, "kind": "not_a_kind", "statement": "Dropped.", "replaces": [], "motivates": [], "rejected_in_favor_of": []},
                {"turn": 1, "kind": "decision", "statement": "The importer writes 1,024 items per batch.", "replaces": ["c1", "c9"], "motivates": ["n3"], "rejected_in_favor_of": ["n4"]},
                {"turn": 7, "kind": "lesson", "statement": "Release builds import faster.", "replaces": [], "motivates": ["n2", "n1", "c2"], "rejected_in_favor_of": []},
                {"turn": 1, "kind": "alternative", "statement": "Keep 256-item batches.", "replaces": [], "motivates": [], "rejected_in_favor_of": ["n2"]},
            ],
            "retracts": ["c2", "c5"],
        });
        let (claims, retracts) = request.parse(&answer.to_string())?;
        assert_eq!(claims.len(), 3);
        assert_eq!(claims[0].replaces, vec!["turns:t:2:000".to_string()]);
        // A decision neither motivates nor is rejected.
        assert!(claims[0].motivates.is_empty() && claims[0].rejected_in_favor_of.is_empty());
        // `n2` is the decision, which lands first once the invalid claim
        // before it is dropped; `n1` (the dropped claim) resolves to nothing.
        assert_eq!(
            claims[1].motivates,
            vec![
                ClaimRef::New(0),
                ClaimRef::Existing("turns:t:2:001".to_string())
            ]
        );
        assert_eq!(claims[2].kind, ClaimKind::Alternative);
        assert_eq!(claims[2].rejected_in_favor_of, vec![ClaimRef::New(0)]);
        assert_eq!(claims[0].turn_id.as_deref(), Some("turn-a"));
        assert_eq!(
            (claims[1].turn_id.as_deref(), claims[1].turn_ordinal),
            (Some("turn-b"), 20)
        );
        assert_eq!(retracts, vec!["turns:t:2:001".to_string()]);
        Ok(())
    }
}
