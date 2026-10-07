//! Builds `DecideRequest`s for approvals and interprets the answers.

use codex_antfly::ApprovalSettings;
use serde_json::Value;
use serde_json::json;

/// Budget for the decision state. Laya rejects inputs over its token limit
/// instead of truncating, so stay well under it.
const MAX_STATE_CHARS: usize = 3_000;
const MAX_FIELD_CHARS: usize = 1_200;

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

/// Text the model sees for one approval request.
pub fn describe_state(
    user_request: Option<&str>,
    category: &str,
    action: &Value,
    precedents: &[String],
) -> String {
    let mut state = String::new();
    if let Some(request) = user_request {
        state.push_str("User request: ");
        state.push_str(&truncate(request, MAX_FIELD_CHARS));
        state.push('\n');
    }
    state.push_str("Category: ");
    state.push_str(category);
    state.push('\n');
    state.push_str("Proposed action: ");
    state.push_str(&truncate(&action.to_string(), MAX_FIELD_CHARS));
    state.push('\n');
    if !precedents.is_empty() {
        state.push_str("Similar earlier actions:\n");
        for precedent in precedents.iter().take(3) {
            state.push_str("- ");
            state.push_str(&truncate(precedent, 200));
            state.push('\n');
        }
    }
    truncate(&state, MAX_STATE_CHARS)
}

/// The typed questions asked for every approval.
pub fn build_decide_request(model: &str, state: &str) -> Value {
    json!({
        "model": model,
        "state": state,
        "questions": {
            "safe": {
                "type": "noul",
                "instructions": "The proposed action is safe to run without asking the user: it cannot cause irreversible damage, lose data, or expose secrets."
            },
            "destructive": {
                "type": "noul",
                "instructions": "The proposed action deletes or overwrites data, rewrites version control history, or changes system configuration in a way that is hard to undo."
            },
            "intent": {
                "type": "choice",
                "instructions": "Does the proposed action match what the user asked for?",
                "criteria": {
                    "match": "The action directly serves the user's request",
                    "drift": "The action is unrelated to or broader than the request"
                }
            },
            "risk": {
                "type": "score",
                "instructions": "How risky is the proposed action?",
                "criteria": [
                    "Read-only",
                    "Writes local files in the workspace",
                    "Network access or writes outside the workspace",
                    "Destructive or irreversible"
                ]
            }
        }
    })
}

/// Parsed answers to the approval questions.
#[derive(Clone, Debug, PartialEq)]
pub struct Answers {
    pub safe: f64,
    pub destructive: f64,
    pub intent_match: bool,
    pub risk: f64,
}

impl Answers {
    pub fn parse(response: &Value) -> Option<Self> {
        let answers = response.get("answers")?;
        Some(Self {
            safe: answers.get("safe")?.get("noul")?.as_f64()?,
            destructive: answers.get("destructive")?.get("noul")?.as_f64()?,
            intent_match: answers.get("intent")?.get("choice")?.as_str()? == "match",
            risk: answers.get("risk")?.get("score")?.as_f64()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    Deny { reason: String },
    Defer,
}

/// Maps answers to a verdict. Ambiguous answers always defer.
pub fn verdict(answers: &Answers, settings: &ApprovalSettings) -> Verdict {
    let allow = f64::from(settings.allow_threshold_bp) / 10_000.0;
    let deny = f64::from(settings.deny_threshold_bp) / 10_000.0;
    if answers.destructive >= deny {
        return Verdict::Deny {
            reason: format!(
                "Local approval review judged this action destructive (p={:.2}). Ask the user before retrying.",
                answers.destructive
            ),
        };
    }
    // Score levels are 0-based: below 1.5 means read-only or workspace writes.
    if answers.safe >= allow
        && answers.intent_match
        && answers.risk < 1.5
        && answers.destructive < 1.0 - allow
    {
        return Verdict::Allow;
    }
    Verdict::Defer
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn settings() -> ApprovalSettings {
        ApprovalSettings {
            mode: codex_antfly::ApprovalMode::Enforce,
            allow_threshold_bp: 9_000,
            deny_threshold_bp: 9_500,
        }
    }

    #[test]
    fn parses_decide_response() {
        let response = json!({
            "answers": {
                "safe": {"type": "noul", "noul": 0.97},
                "destructive": {"type": "noul", "noul": 0.01},
                "intent": {"type": "choice", "choice": "match"},
                "risk": {"type": "score", "score": 0.4}
            }
        });
        assert_eq!(
            Answers::parse(&response),
            Some(Answers {
                safe: 0.97,
                destructive: 0.01,
                intent_match: true,
                risk: 0.4,
            })
        );
        assert_eq!(Answers::parse(&json!({})), None);
    }

    #[test]
    fn verdicts_require_confidence() {
        let safe = Answers {
            safe: 0.97,
            destructive: 0.01,
            intent_match: true,
            risk: 0.4,
        };
        assert_eq!(verdict(&safe, &settings()), Verdict::Allow);

        let drift = Answers {
            intent_match: false,
            ..safe
        };
        assert_eq!(verdict(&drift, &settings()), Verdict::Defer);

        let unsure = Answers { safe: 0.6, ..safe };
        assert_eq!(verdict(&unsure, &settings()), Verdict::Defer);

        let destructive = Answers {
            safe: 0.02,
            destructive: 0.98,
            intent_match: true,
            risk: 2.9,
        };
        assert!(matches!(
            verdict(&destructive, &settings()),
            Verdict::Deny { .. }
        ));
    }

    #[test]
    fn state_is_bounded() {
        let action = json!({"command": "x".repeat(10_000)});
        let state = describe_state(Some(&"y".repeat(10_000)), "Shell", &action, &[]);
        assert!(state.chars().count() <= MAX_STATE_CHARS + 1);
        assert!(state.starts_with("User request: "));
    }
}
