//! Screening turns with the decide model before claim extraction.
//!
//! Most turns hold nothing durable (routine edits, tool output), yet each
//! extraction request costs a model call. Before a batch is sent, the decide
//! model is asked whether each turn records something a future agent should
//! know. In shadow mode the verdict is only logged (`codex_decision_log`,
//! kind `triage`, subject `<thread>:<turn start>`) so it can be scored
//! against what extraction found for the same turn (`codex_extraction_log`);
//! in enforce mode turns below the threshold are not sent.

use codex_antfly::Antfly;
use codex_antfly::TriageMode;
use codex_antfly::labels;
use serde_json::Value;
use serde_json::json;

use super::transcript::Turn;

/// Characters of a turn's evidence the decide model sees: its start, where
/// the user states the request, and its end, where the outcome is.
const HEAD_CHARS: usize = 1_200;
const TAIL_CHARS: usize = 800;

pub(crate) const KIND: &str = "triage";

/// The start and end of `text`, within the decide model's input budget.
fn excerpt(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= HEAD_CHARS + TAIL_CHARS {
        return text.to_string();
    }
    let head: String = chars[..HEAD_CHARS].iter().collect();
    let tail: String = chars[chars.len() - TAIL_CHARS..].iter().collect();
    format!("{head}\n…\n{tail}")
}

/// One choice over what the turn mainly contains. Calibrated against Laya:
/// a yes/no predicate scored near chance, while this choice separated every
/// durable turn from every routine one on a hand-labeled sample.
pub(crate) fn request(model: &str, turn: &Turn) -> Value {
    json!({
        "model": model,
        "input": excerpt(&turn.text),
        "questions": [{
            "name": "content",
            "type": "choice",
            "instructions": "What does this turn of a coding session mainly contain?",
            "choices": [
                {"value": "decision", "description": "A choice made for the work, or why it was made"},
                {"value": "preference", "description": "How the user wants to work"},
                {"value": "lesson", "description": "Something learned from a failure or surprise, or a constraint"},
                {"value": "open_question", "description": "Something left unresolved that matters later"},
                {"value": "routine", "description": "Routine work: running commands, showing output, small edits, explanations"},
            ],
        }],
    })
}

/// Probability that the turn holds something durable: anything but routine.
fn probability(response: &Value) -> Option<f64> {
    let routine = response
        .get("answers")?
        .as_array()?
        .iter()
        .find(|answer| answer.get("name").and_then(Value::as_str) == Some("content"))?
        .get("probabilities")?
        .as_array()?
        .iter()
        .find(|choice| choice.get("value").and_then(Value::as_str) == Some("routine"))?
        .get("probability")?
        .as_f64()?;
    Some(1.0 - routine)
}

/// Screens `turns` and logs each verdict. Returns the turns to extract:
/// all of them unless `mode` is enforce. A turn the model fails on is kept.
pub(crate) async fn screen<'a>(
    antfly: &Antfly,
    thread_id: &str,
    mode: TriageMode,
    threshold_bp: u32,
    turns: Vec<&'a Turn>,
) -> Vec<&'a Turn> {
    if mode == TriageMode::Off {
        return turns;
    }
    let model = antfly.config().decide_model.clone();
    let mut kept = Vec::with_capacity(turns.len());
    for turn in turns {
        let request = request(&model, turn);
        let response = match antfly.decide(&request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!("claim triage failed: {err}");
                kept.push(turn);
                continue;
            }
        };
        let Some(probability) = probability(&response) else {
            kept.push(turn);
            continue;
        };
        let durable = probability * 10_000.0 >= f64::from(threshold_bp);
        if let Err(err) = labels::record_decision(
            antfly,
            labels::DecisionRecord {
                kind: KIND,
                model: &model,
                subject: Some(format!("{thread_id}:{}", turn.start)),
                input: request["input"].as_str().unwrap_or_default(),
                response: &response,
                result: Some(durable),
                score: Some(probability),
                threshold_bp: Some(threshold_bp),
            },
        )
        .await
        {
            tracing::debug!("failed to record claim triage: {err}");
        }
        if durable || mode == TriageMode::Shadow {
            kept.push(turn);
        }
    }
    kept
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::excerpt;
    use super::probability;
    use super::request;
    use crate::claims::transcript::Turn;

    fn turn(text: &str) -> Turn {
        Turn {
            turn_id: None,
            start: 0,
            end: 1,
            finished: true,
            text: text.to_string(),
        }
    }

    /// The decide model answers the triage question, and rates a turn that
    /// settles a decision above routine tool chatter. Needs the Laya model.
    #[test]
    fn decide_model_separates_durable_turns_from_routine_ones()
    -> Result<(), Box<dyn std::error::Error>> {
        let models = dirs_models().join(codex_antfly::DEFAULT_DECIDE_MODEL);
        if !models.exists() {
            return Ok(());
        }
        let dir = tempfile::tempdir()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(16 * 1024 * 1024)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mut config =
                codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
            config.embedder = None;
            let antfly = codex_antfly::Antfly::open(config)?;
            let model = codex_antfly::DEFAULT_DECIDE_MODEL;
            let durable = turn(
                "User: Let's stop using Qwen3 Embedding; switch Codex search to EmbeddingGemma 2, \
                 it ranks our sessions better and Antfly detects its dimensions.\n\
                 Assistant: Switched the default embedder to google/embeddinggemma-2 and noted why.",
            );
            let routine = turn(
                "User: run the tests\nAssistant: Ran cargo test -p codex-antfly: 14 passed.",
            );
            let p_durable = probability(&antfly.decide(&request(model, &durable)).await?)
                .ok_or("no durable probability")?;
            let p_routine = probability(&antfly.decide(&request(model, &routine)).await?)
                .ok_or("no routine probability")?;
            println!("durable {p_durable:.3} routine {p_routine:.3}");
            assert!(p_durable > 0.8 && p_routine < 0.5, "{p_durable} vs {p_routine}");
            antfly.close().await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    fn dirs_models() -> std::path::PathBuf {
        std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_default()
            .join(".antfly/inference/models")
    }

    #[test]
    fn long_turns_keep_their_start_and_end() {
        let text = format!(
            "{}{}{}",
            "a".repeat(1_500),
            "b".repeat(500),
            "c".repeat(900)
        );
        let cut = excerpt(&text);
        assert!(cut.starts_with(&"a".repeat(1_200)));
        assert!(cut.ends_with(&"c".repeat(800)));
        assert!(!cut.contains('b'));
        assert_eq!(excerpt("short turn"), "short turn");
    }
}
