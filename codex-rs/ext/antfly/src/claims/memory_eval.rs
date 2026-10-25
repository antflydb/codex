//! Codex as the external judge of an Antfly memory evaluation run: concept
//! identity and typed relations with the existing judges, and claim
//! supersession with a four-way verdict.

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;

use super::concepts;
use super::concepts::MergeVerdict;

pub(crate) const SUPERSESSION_INSTRUCTIONS: &str = "You maintain the claims of a knowledge base \
built from software engineering sessions. Each numbered pair is an EARLIER and a LATER claim \
about the project. Answer `supersedes` when the later claim should replace the earlier one in \
the knowledge base: it updates, reverses, corrects, narrows or adds to the same decision, fact, \
preference or outcome, even if it mostly repeats it. Answer `restates` only when the later \
claim adds nothing: same content, no new or changed detail. Answer `contradicts` when both \
cannot be true but the later one is not clearly the newer word on it. Answer `independent` \
when they are about different things or both can hold. Set `sure: false` when the claims do \
not settle it.";

pub(crate) const SUPERSESSION_VERDICTS: &[&str] =
    &["supersedes", "contradicts", "restates", "independent"];

pub(crate) fn supersession_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "pairs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pair": {"type": "integer"},
                        "verdict": {"type": "string", "enum": SUPERSESSION_VERDICTS},
                        "sure": {"type": "boolean"},
                    },
                    "required": ["pair", "verdict", "sure"],
                    "additionalProperties": false,
                },
            },
        },
        "required": ["pairs"],
        "additionalProperties": false,
    })
}

pub(crate) fn supersession_input(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .enumerate()
        .map(|(index, (earlier, later))| {
            format!("{}. EARLIER: {earlier}\n   LATER: {later}", index + 1)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Deserialize)]
struct SupersessionAnswer {
    pairs: Vec<SupersessionJudgment>,
}

#[derive(Deserialize)]
struct SupersessionJudgment {
    pair: usize,
    verdict: String,
    sure: bool,
}

/// The verdict per pair: `None` when unsure or skipped.
pub(crate) fn parse_supersession(
    answer: &str,
    count: usize,
) -> anyhow::Result<Vec<Option<&'static str>>> {
    let parsed: SupersessionAnswer = serde_json::from_str(answer.trim())?;
    let mut verdicts = vec![None; count];
    for judgment in parsed.pairs {
        let Some(slot) = judgment
            .pair
            .checked_sub(1)
            .and_then(|index| verdicts.get_mut(index))
        else {
            continue;
        };
        if judgment.sure {
            *slot = SUPERSESSION_VERDICTS
                .iter()
                .find(|verdict| **verdict == judgment.verdict)
                .copied();
        }
    }
    Ok(verdicts)
}

/// The judge an external run freezes: Codex, the judging model, and a
/// digest of the judge prompts, so a prompt change starts a new cohort.
pub(crate) fn judge_metadata(model: &str) -> Value {
    let mut digest = Sha256::new();
    for prompt in [
        concepts::JUDGE_INSTRUCTIONS,
        concepts::DESCRIBED_JUDGE_NOTE,
        concepts::RELATION_INSTRUCTIONS,
        SUPERSESSION_INSTRUCTIONS,
    ] {
        digest.update(prompt.as_bytes());
        digest.update([0]);
    }
    let digest = format!("{:x}", digest.finalize());
    json!({"provider": "codex", "model": model, "revision": format!("prompts-{}", &digest[..16])})
}

/// One judge task of an evaluation run.
#[derive(Clone, Debug)]
pub(crate) struct Task {
    pub case_id: String,
    pub case_revision: u64,
    pub input_fingerprint: String,
    pub kind: String,
    pub input: Value,
}

impl Task {
    pub(crate) fn from_json(item: &Value) -> Option<Self> {
        Some(Self {
            case_id: item["case_id"].as_str()?.to_string(),
            case_revision: item["case_revision"].as_u64()?,
            input_fingerprint: item["input_fingerprint"].as_str()?.to_string(),
            kind: item["input"]["task"].as_str()?.to_string(),
            input: item["input"]["input"].clone(),
        })
    }

    /// The verdict for this task: `label` of `None` abstains.
    pub(crate) fn verdict(&self, label: Option<&str>, invalid: bool) -> Value {
        json!({
            "case_id": self.case_id,
            "case_revision": self.case_revision,
            "input_fingerprint": self.input_fingerprint,
            "prediction": {"label": label},
            "invalid_output": invalid,
        })
    }

    /// A concept side rendered as the judges see it.
    pub(crate) fn concept(&self, side: &str) -> String {
        concepts::judged_name(
            self.input[side].as_str().unwrap_or_default(),
            self.input[format!("{side}_description")].as_str(),
        )
    }
}

pub(crate) fn identity_label(verdict: MergeVerdict) -> Option<&'static str> {
    match verdict {
        MergeVerdict::Same => Some("same"),
        MergeVerdict::Different => Some("different"),
        MergeVerdict::Unsure => None,
    }
}

/// Whether a run's frozen judge is `judge`. The service echoes optional
/// fields (`dimensions`) back as null.
pub(crate) fn same_judge(frozen: &Value, judge: &Value) -> bool {
    ["provider", "model", "revision"]
        .iter()
        .all(|key| frozen[key] == judge[key])
}

/// Whether the relation judge's answer for `a | b` is the proposed predicate.
pub(crate) fn relation_label(relation: &str, proposed: &str) -> &'static str {
    let matches = matches!(
        (proposed, relation),
        ("is_a", "a_is_b") | ("part_of", "a_part_b") | ("alternative_to", "alternatives")
    );
    if matches { "related" } else { "unrelated" }
}

/// What a judging pass submitted.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct EvaluationReport {
    pub tasks: usize,
    pub labelled: usize,
    pub abstained: usize,
    pub invalid: usize,
    pub skipped: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn unsure_and_unknown_supersession_verdicts_abstain() -> anyhow::Result<()> {
        let answer = json!({"pairs": [
            {"pair": 1, "verdict": "supersedes", "sure": true},
            {"pair": 2, "verdict": "restates", "sure": false},
            {"pair": 3, "verdict": "replaces", "sure": true},
        ]});
        assert_eq!(
            parse_supersession(&answer.to_string(), 4)?,
            vec![Some("supersedes"), None, None, None]
        );
        Ok(())
    }

    #[test]
    fn frozen_judges_match_on_identity_fields() {
        let judge = judge_metadata("gpt-5-mini");
        let mut frozen = judge.clone();
        frozen["dimensions"] = Value::Null;
        assert!(same_judge(&frozen, &judge));
        assert!(!same_judge(&judge_metadata("gpt-5"), &judge));
    }

    #[test]
    fn relation_answers_map_to_proposed_predicates() {
        assert_eq!(relation_label("a_is_b", "is_a"), "related");
        assert_eq!(relation_label("b_is_a", "is_a"), "unrelated");
        assert_eq!(relation_label("a_part_b", "part_of"), "related");
        assert_eq!(relation_label("alternatives", "alternative_to"), "related");
        assert_eq!(relation_label("none", "part_of"), "unrelated");
    }
}
