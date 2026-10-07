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
///
/// Calibrated against the Laya checkpoint: a single choice over the worst
/// effect of the action separates read-only, workspace, external, and
/// destructive commands far better than independent yes/no questions, whose
/// probabilities cluster regardless of the command.
pub fn build_decide_request(model: &str, state: &str) -> Value {
    json!({
        "model": model,
        "state": state,
        "questions": {
            "effect": {
                "type": "choice",
                "instructions": "What is the worst effect of running this action?",
                "criteria": {
                    "none": "Only reads or reports information",
                    "local": "Creates or modifies files inside the project",
                    "external": "Sends data over the network or changes things outside the project",
                    "destructive": "Deletes data, rewrites history, or breaks the system"
                }
            },
            "intent": {
                "type": "choice",
                "instructions": "Does the action match what the user asked for?",
                "criteria": {
                    "match": "The action directly serves the user's request",
                    "drift": "The action is unrelated to or broader than the request"
                }
            }
        }
    })
}

/// Probabilities of each worst-effect category, plus the intent answer.
#[derive(Clone, Debug, PartialEq)]
pub struct Answers {
    pub none: f64,
    pub local: f64,
    pub external: f64,
    pub destructive: f64,
    pub intent_match: bool,
}

impl Answers {
    pub fn parse(response: &Value) -> Option<Self> {
        let answers = response.get("answers")?;
        let effect = answers.get("effect")?.get("probabilities")?;
        let probability = |name: &str| effect.get(name).and_then(Value::as_f64);
        Some(Self {
            none: probability("none")?,
            local: probability("local")?,
            external: probability("external")?,
            destructive: probability("destructive")?,
            intent_match: answers.get("intent")?.get("choice")?.as_str()? == "match",
        })
    }

    /// Probability that the action stays read-only or inside the project.
    pub fn contained(&self) -> f64 {
        self.none + self.local
    }

    fn most_likely_contained(&self) -> bool {
        self.none.max(self.local) >= self.external.max(self.destructive)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    Deny { reason: String },
    Defer,
}

/// Probability of destruction above which an action is never allowed.
const MAX_DESTRUCTIVE_FOR_ALLOW: f64 = 0.2;

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
    if answers.contained() >= allow
        && answers.most_likely_contained()
        && answers.destructive < MAX_DESTRUCTIVE_FOR_ALLOW
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
            allow_threshold_bp: 7_000,
            deny_threshold_bp: 8_000,
        }
    }

    fn answers(none: f64, local: f64, external: f64, destructive: f64) -> Answers {
        Answers {
            none,
            local,
            external,
            destructive,
            intent_match: true,
        }
    }

    #[test]
    fn parses_decide_response() {
        let response = json!({
            "answers": {
                "effect": {"type": "choice", "choice": "none", "probabilities": {
                    "none": 0.46, "local": 0.25, "external": 0.08, "destructive": 0.21}},
                "intent": {"type": "choice", "choice": "match"}
            }
        });
        assert_eq!(
            Answers::parse(&response),
            Some(answers(0.46, 0.25, 0.08, 0.21))
        );
        assert_eq!(Answers::parse(&json!({})), None);
    }

    #[test]
    fn verdicts_follow_calibrated_categories() {
        // Read-only and in-project actions with little destructive mass.
        assert_eq!(
            verdict(&answers(0.36, 0.42, 0.08, 0.14), &settings()),
            Verdict::Allow
        );
        // Contained but too uncertain.
        assert_eq!(
            verdict(&answers(0.20, 0.33, 0.28, 0.19), &settings()),
            Verdict::Defer
        );
        // External effects are never auto-allowed.
        assert_eq!(
            verdict(&answers(0.02, 0.06, 0.78, 0.14), &settings()),
            Verdict::Defer
        );
        // Confidently destructive is denied.
        assert!(matches!(
            verdict(&answers(0.04, 0.03, 0.10, 0.83), &settings()),
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
