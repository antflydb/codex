//! Records what happened to tool calls after a local approval decision, so
//! decisions can be calibrated against outcomes.

use std::sync::Arc;

use codex_antfly::Write;
use codex_extension_api::ToolCallOutcome;
use codex_extension_api::ToolFinishInput;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolLifecycleFuture;
use serde_json::Value;

use crate::reviewer::LayaApprovalReviewer;

pub(crate) struct OutcomeRecorder {
    reviewer: Arc<LayaApprovalReviewer>,
}

impl OutcomeRecorder {
    pub(crate) fn new(reviewer: Arc<LayaApprovalReviewer>) -> Self {
        Self { reviewer }
    }
}

fn outcome_label(outcome: ToolCallOutcome) -> &'static str {
    match outcome {
        ToolCallOutcome::Completed { .. } => "executed",
        ToolCallOutcome::Blocked => "blocked",
        ToolCallOutcome::Failed {
            handler_executed: true,
        } => "executed_failed",
        ToolCallOutcome::Failed {
            handler_executed: false,
        } => "not_executed",
        _ => "unknown",
    }
}

impl ToolLifecycleContributor for OutcomeRecorder {
    fn on_tool_finish<'a>(&'a self, input: ToolFinishInput<'a>) -> ToolLifecycleFuture<'a> {
        Box::pin(async move {
            let pending = self
                .reviewer
                .pending
                .lock()
                .ok()
                .and_then(|mut pending| pending.remove(input.call_id));
            let Some(mut pending) = pending else {
                return;
            };
            let Some(thread_id) = pending
                .doc
                .get("thread_id")
                .and_then(Value::as_str)
                .and_then(|id| codex_protocol::ThreadId::from_string(id).ok())
            else {
                return;
            };
            let Some(antfly) = self.reviewer.antfly_for(thread_id).await else {
                return;
            };
            if let Some(map) = pending.doc.as_object_mut() {
                map.insert(
                    "outcome".to_string(),
                    Value::String(outcome_label(input.outcome).to_string()),
                );
            }
            if let Err(err) = antfly
                .write(vec![Write::put(pending.key, pending.doc)])
                .await
            {
                tracing::warn!("failed to record approval outcome: {err}");
            }
        })
    }
}
