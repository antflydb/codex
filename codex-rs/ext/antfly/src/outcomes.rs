//! Records what happened to tool calls after a local approval decision, so
//! decisions can be calibrated against outcomes.

use std::sync::Arc;

use codex_antfly::Write;
use codex_antfly::schema;
use codex_extension_api::ApprovalResolutionSource;
use codex_extension_api::ApprovalResolvedInput;
use codex_extension_api::ToolCallOutcome;
use codex_extension_api::ToolFinishInput;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolLifecycleFuture;
use codex_protocol::protocol::NetworkPolicyRuleAction;
use codex_protocol::protocol::ReviewDecision;
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

/// Label for a finished call. A rejected approval ends the handler normally,
/// so the tool outcome alone cannot tell it from a command that ran and
/// failed; `approved` is the resolution seen by `on_approval_resolved`.
pub(crate) fn outcome_label(outcome: ToolCallOutcome, approved: Option<bool>) -> &'static str {
    match (outcome, approved) {
        (_, Some(false)) => "declined",
        (ToolCallOutcome::Completed { success: true }, _) => "executed",
        (ToolCallOutcome::Completed { success: false }, _) => "executed_failed",
        (ToolCallOutcome::Blocked, _) => "blocked",
        (
            ToolCallOutcome::Failed {
                handler_executed: true,
            },
            _,
        ) => "failed",
        (
            ToolCallOutcome::Failed {
                handler_executed: false,
            },
            _,
        ) => "not_executed",
        _ => "unknown",
    }
}

fn decision_text(decision: &ReviewDecision) -> &'static str {
    match decision {
        ReviewDecision::Approved => "approved",
        ReviewDecision::ApprovedExecpolicyAmendment { .. } => "approved_execpolicy_amendment",
        ReviewDecision::ApprovedForSession => "approved_for_session",
        ReviewDecision::ApprovedMcpPolicyAmendment => "approved_mcp_policy_amendment",
        ReviewDecision::NetworkPolicyAmendment { .. } => "network_policy_amendment",
        ReviewDecision::Denied { .. } => "denied",
        ReviewDecision::TimedOut => "timed_out",
        ReviewDecision::Abort => "abort",
    }
}

fn is_approval(decision: &ReviewDecision) -> bool {
    match decision {
        ReviewDecision::Approved
        | ReviewDecision::ApprovedExecpolicyAmendment { .. }
        | ReviewDecision::ApprovedForSession
        | ReviewDecision::ApprovedMcpPolicyAmendment => true,
        ReviewDecision::NetworkPolicyAmendment {
            network_policy_amendment,
        } => network_policy_amendment.action == NetworkPolicyRuleAction::Allow,
        ReviewDecision::Denied { .. } | ReviewDecision::TimedOut | ReviewDecision::Abort => false,
    }
}

fn source_text(source: ApprovalResolutionSource) -> &'static str {
    match source {
        ApprovalResolutionSource::Hook => "hook",
        ApprovalResolutionSource::Reviewer => "reviewer",
        ApprovalResolutionSource::User => "user",
    }
}

impl ToolLifecycleContributor for OutcomeRecorder {
    fn on_approval_resolved(&self, input: ApprovalResolvedInput<'_>) {
        if let Ok(mut pending) = self.reviewer.pending.lock()
            && let Some(pending) = pending.get_mut(input.call_id)
        {
            pending.approved = Some(is_approval(input.decision));
            if let Some(map) = pending.doc.as_object_mut() {
                map.insert(
                    "decision".to_string(),
                    Value::String(decision_text(input.decision).to_string()),
                );
                map.insert(
                    "decided_by".to_string(),
                    Value::String(source_text(input.source).to_string()),
                );
            }
        }
    }

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
                    Value::String(outcome_label(input.outcome, pending.approved).to_string()),
                );
            }
            if let Err(err) = antfly
                .documents(schema::APPROVALS)
                .write(vec![Write::put(pending.id, pending.doc)])
                .await
            {
                tracing::warn!("failed to record approval outcome: {err}");
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use codex_extension_api::ToolCallOutcome;
    use pretty_assertions::assert_eq;

    use super::outcome_label;

    #[test]
    fn declined_approvals_are_not_failures() {
        let failed = ToolCallOutcome::Failed {
            handler_executed: true,
        };
        assert_eq!(outcome_label(failed, Some(false)), "declined");
        assert_eq!(outcome_label(failed, Some(true)), "failed");
        assert_eq!(
            outcome_label(ToolCallOutcome::Completed { success: false }, Some(true)),
            "executed_failed"
        );
        assert_eq!(
            outcome_label(ToolCallOutcome::Completed { success: true }, None),
            "executed"
        );
    }
}
