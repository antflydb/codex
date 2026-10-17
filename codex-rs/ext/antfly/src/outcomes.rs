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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

impl ToolLifecycleContributor for OutcomeRecorder {
    /// Records the resolution right away, so a decision survives even if
    /// the process stops before the tool finishes.
    fn on_approval_resolved(&self, input: ApprovalResolvedInput<'_>) {
        let Ok(mut pending) = self.reviewer.pending.lock() else {
            return;
        };
        let Some(pending) = pending.get_mut(input.call_id) else {
            return;
        };
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
            map.insert("resolved_at_ms".to_string(), Value::from(now_ms()));
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let antfly = Arc::clone(&pending.antfly);
        let write = Write::put(pending.id.clone(), pending.doc.clone());
        pending.resolution_write = Some(runtime.spawn(async move {
            if let Err(err) = antfly.documents(schema::APPROVALS).write(vec![write]).await {
                tracing::warn!("failed to record approval resolution: {err}");
            }
        }));
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
            if let Some(map) = pending.doc.as_object_mut() {
                map.insert(
                    "outcome".to_string(),
                    Value::String(outcome_label(input.outcome, pending.approved).to_string()),
                );
                map.insert("finished_at_ms".to_string(), Value::from(now_ms()));
            }
            if let Some(resolution) = pending.resolution_write.take() {
                let _ = resolution.await;
            }
            if let Err(err) = pending
                .antfly
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
    use std::sync::Arc;
    use std::sync::Weak;

    use codex_antfly::Antfly;
    use codex_antfly::AntflyConfig;
    use codex_antfly::schema;
    use codex_extension_api::ApprovalResolutionSource;
    use codex_extension_api::ApprovalResolvedInput;
    use codex_extension_api::ExtensionData;
    use codex_extension_api::ToolCallOutcome;
    use codex_extension_api::ToolLifecycleContributor;
    use codex_protocol::protocol::ReviewDecision;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::OutcomeRecorder;
    use super::outcome_label;
    use crate::reviewer::LayaApprovalReviewer;
    use crate::reviewer::PendingOutcome;

    /// The user's decision reaches the database when it is made, not only
    /// when the tool finishes, so a restart in between keeps it.
    #[test]
    fn resolutions_are_recorded_before_the_tool_finishes() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(16 * 1024 * 1024)
            .enable_all()
            .build()?;
        let _serial = runtime.block_on(crate::MODEL_TESTS.acquire())?;
        runtime.block_on(async {
            let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
            config.embedder = None;
            let antfly = Arc::new(Antfly::open(config)?);
            let reviewer = Arc::new(LayaApprovalReviewer::new(Weak::new()));
            let doc = json!({"thread_id": "thread-1", "approval_id": "approval-1",
                "search_text": "Proposed action: rm -rf target", "outcome": "pending"});
            reviewer.pending.lock().expect("pending").insert(
                "call-1".to_string(),
                PendingOutcome {
                    id: "approval-1".to_string(),
                    doc,
                    antfly: Arc::clone(&antfly),
                    approved: None,
                    resolution_write: None,
                },
            );
            let recorder = OutcomeRecorder::new(Arc::clone(&reviewer));
            let store = ExtensionData::new("test");
            recorder.on_approval_resolved(ApprovalResolvedInput {
                thread_store: &store,
                turn_store: &store,
                turn_id: "turn-1",
                call_id: "call-1",
                decision: &ReviewDecision::Approved,
                source: ApprovalResolutionSource::User,
            });
            let write = reviewer
                .pending
                .lock()
                .expect("pending")
                .get_mut("call-1")
                .and_then(|pending| pending.resolution_write.take())
                .expect("resolution write started");
            write.await?;
            let stored = antfly
                .documents(schema::APPROVALS)
                .get("approval-1")
                .await?
                .expect("approval stored");
            assert_eq!(stored["decision"], json!("approved"));
            assert_eq!(stored["decided_by"], json!("user"));
            assert_eq!(stored["outcome"], json!("pending"));
            assert!(stored["resolved_at_ms"].as_i64().is_some_and(|at| at > 0));
            antfly.close().await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

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
