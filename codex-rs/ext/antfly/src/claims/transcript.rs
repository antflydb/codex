//! Groups a thread's stored history items into turns and renders each turn
//! as compact, redacted evidence for claim extraction.
//!
//! Works on the stored JSON (`RolloutItem` serialized as `{type, payload}`)
//! so the pipeline does not depend on every item variant's Rust shape.

use codex_secrets::redact_secrets;
use serde_json::Value;

/// Characters kept from one message or tool argument.
const MAX_TEXT_CHARS: usize = 4_000;
/// Characters kept from one tool output.
const MAX_OUTPUT_CHARS: usize = 800;

/// One turn of a thread's history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Turn {
    pub(crate) turn_id: Option<String>,
    /// Ordinal of the turn's first item.
    pub(crate) start: u64,
    /// One past the turn's last item.
    pub(crate) end: u64,
    /// The turn completed or was aborted.
    pub(crate) finished: bool,
    /// Rendered evidence; empty when the turn has nothing worth reading.
    pub(crate) text: String,
}

fn truncate(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((index, _)) => format!("{}…", &text[..index]),
        None => text.to_string(),
    }
}

fn joined_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Host-injected context (environment, instructions, skills) carried as user
/// messages; it describes the setup, not what the user said.
fn is_injected_context(text: &str) -> bool {
    let trimmed = text.trim_start();
    if trimmed.starts_with("# AGENTS.md") {
        return true;
    }
    let Some(rest) = trimmed.strip_prefix('<') else {
        return false;
    };
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_lowercase() || *c == '_' || *c == '-')
        .collect();
    !name.is_empty()
        && rest[name.len()..].starts_with('>')
        && trimmed.contains(&format!("</{name}>"))
}

fn render_response_item(payload: &Value) -> Option<String> {
    let kind = payload.get("type")?.as_str()?;
    match kind {
        "message" => {
            let role = payload.get("role")?.as_str()?;
            let text = joined_text(payload.get("content"));
            let text = text.trim();
            if text.is_empty() {
                return None;
            }
            match role {
                "user" if !is_injected_context(text) => {
                    Some(format!("User: {}", truncate(text, MAX_TEXT_CHARS)))
                }
                "assistant" => Some(format!("Assistant: {}", truncate(text, MAX_TEXT_CHARS))),
                _ => None,
            }
        }
        "function_call" | "custom_tool_call" => {
            let name = payload
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let input = payload
                .get("arguments")
                .or_else(|| payload.get("input"))
                .map(|value| match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            Some(format!(
                "Tool call {name}: {}",
                truncate(&input, MAX_TEXT_CHARS)
            ))
        }
        "local_shell_call" => {
            let command = payload.pointer("/action/command").map(Value::to_string)?;
            Some(format!("Shell: {}", truncate(&command, MAX_TEXT_CHARS)))
        }
        "function_call_output" | "custom_tool_call_output" => {
            let output = payload.get("output")?;
            let text = match output {
                Value::Object(map) => joined_text(map.get("content").or(map.get("text"))),
                other => joined_text(Some(other)),
            };
            let text = text.trim();
            (!text.is_empty()).then(|| format!("Tool output: {}", truncate(text, MAX_OUTPUT_CHARS)))
        }
        _ => None,
    }
}

fn event_turn_id(payload: &Value) -> Option<String> {
    payload
        .get("turn_id")
        .or_else(|| payload.pointer("/turn_attribution/turn_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Splits `items` (ordinal, stored item) into turns, in order. Items before
/// the first turn start their own turn when they contain evidence.
pub(crate) fn turns(items: &[(u64, Value)]) -> Vec<Turn> {
    let mut turns: Vec<Turn> = Vec::new();
    for (ordinal, item) in items {
        let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
        let Some(payload) = item.get("payload") else {
            continue;
        };
        let event = (kind == "event_msg")
            .then(|| payload.get("type").and_then(Value::as_str))
            .flatten();
        match event {
            Some("task_started" | "turn_started") => {
                turns.push(Turn {
                    turn_id: event_turn_id(payload),
                    start: *ordinal,
                    end: ordinal + 1,
                    finished: false,
                    text: String::new(),
                });
                continue;
            }
            Some("task_complete" | "turn_complete" | "turn_aborted") => {
                if let Some(turn) = turns.last_mut().filter(|turn| !turn.finished) {
                    turn.end = ordinal + 1;
                    turn.finished = true;
                }
                continue;
            }
            _ => {}
        }
        let rendered = (kind == "response_item")
            .then(|| render_response_item(payload))
            .flatten();
        let open = turns.last().is_some_and(|turn| !turn.finished);
        if !open {
            if rendered.is_none() {
                // Setup and bookkeeping between turns belong to no turn, but
                // still count as extracted.
                if let Some(turn) = turns.last_mut() {
                    turn.end = ordinal + 1;
                }
                continue;
            }
            turns.push(Turn {
                turn_id: None,
                start: *ordinal,
                end: ordinal + 1,
                finished: false,
                text: String::new(),
            });
        }
        let Some(turn) = turns.last_mut() else {
            continue;
        };
        turn.end = ordinal + 1;
        if let Some(line) = rendered {
            if !turn.text.is_empty() {
                turn.text.push('\n');
            }
            turn.text.push_str(&redact_secrets(line));
        }
    }
    turns
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn response(payload: Value) -> Value {
        json!({"type": "response_item", "payload": payload})
    }

    fn event(payload: Value) -> Value {
        json!({"type": "event_msg", "payload": payload})
    }

    #[test]
    fn groups_items_into_turns_and_keeps_only_evidence() {
        let items = vec![
            (0, json!({"type": "session_meta", "payload": {}})),
            (
                1,
                event(json!({"type": "task_started", "turn_attribution": {"turn_id": "t1"}})),
            ),
            (
                2,
                response(
                    json!({"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "instructions"}]}),
                ),
            ),
            (
                3,
                response(
                    json!({"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<environment_context>\n<cwd>/x</cwd>\n</environment_context>"}]}),
                ),
            ),
            (
                4,
                response(
                    json!({"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Use sk-abcdefghijklmnopqrstuvwxyz0123456789ABCD for the import"}]}),
                ),
            ),
            (5, response(json!({"type": "reasoning", "summary": []}))),
            (
                6,
                response(
                    json!({"type": "custom_tool_call", "name": "exec", "input": "cargo test"}),
                ),
            ),
            (
                7,
                response(
                    json!({"type": "custom_tool_call_output", "output": [{"type": "input_text", "text": "ok"}]}),
                ),
            ),
            (
                8,
                response(
                    json!({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Done."}]}),
                ),
            ),
            (9, event(json!({"type": "task_complete", "turn_id": "t1"}))),
            (10, event(json!({"type": "token_count"}))),
            (11, event(json!({"type": "task_started", "turn_id": "t2"}))),
            (
                12,
                response(
                    json!({"type": "message", "role": "user", "content": [{"type": "input_text", "text": "next"}]}),
                ),
            ),
        ];
        let turns = turns(&items);
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].turn_id.as_deref(), Some("t1"));
        assert_eq!(
            (turns[0].start, turns[0].end, turns[0].finished),
            (1, 11, true)
        );
        assert!(
            !turns[0].text.contains("sk-abcdefghij"),
            "{}",
            turns[0].text
        );
        assert_eq!(
            turns[0].text.lines().collect::<Vec<_>>(),
            vec![
                "User: Use [REDACTED_SECRET] for the import",
                "Tool call exec: cargo test",
                "Tool output: ok",
                "Assistant: Done.",
            ]
        );
        assert_eq!(turns[1].turn_id.as_deref(), Some("t2"));
        assert_eq!(
            (turns[1].start, turns[1].end, turns[1].finished),
            (11, 13, false)
        );
    }
}
