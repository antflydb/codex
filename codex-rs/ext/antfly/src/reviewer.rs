use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use codex_antfly::Antfly;
use codex_antfly::ApprovalMode;
use codex_antfly::ApprovalSettings;
use codex_antfly::Write;
use codex_antfly::schema;
use codex_core::ThreadManager;
use codex_core::config::ThreadStoreConfig;
use codex_extension_api::ApprovalDecision;
use codex_extension_api::ApprovalDecisionInput;
use codex_extension_api::ApprovalReviewContributor;
use codex_extension_api::ContextualUserFragment;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionMetrics;
use codex_extension_api::TurnInputContext;
use codex_extension_api::TurnInputContributor;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::user_input::UserInput;
use serde_json::Value;
use serde_json::json;

use crate::request::Answers;
use crate::request::Verdict;
use crate::request::build_decide_request;
use crate::request::describe_state;
use crate::request::verdict;

/// Similar earlier decisions included in the model's context.
const PRECEDENTS: usize = 3;

/// Version of the fields recorded with each decision, so calibration can
/// tell records with the full model context from older ones.
const RECORD_VERSION: u32 = 2;

/// Latest user request in a thread, captured from turn input.
struct LatestUserRequest(String);

/// Decision recorded for a tool call that has not finished yet.
pub(crate) struct PendingOutcome {
    /// Document id in [`schema::APPROVALS`]; the approval's own id.
    pub(crate) id: String,
    pub(crate) doc: Value,
    pub(crate) antfly: Arc<Antfly>,
    /// Whether the approval was granted, once it has resolved.
    pub(crate) approved: Option<bool>,
    /// Write of the resolution, which the outcome write waits for so it
    /// cannot be overtaken.
    pub(crate) resolution_write: Option<tokio::task::JoinHandle<()>>,
}

pub(crate) struct LayaApprovalReviewer {
    thread_manager: Weak<ThreadManager>,
    /// Decisions awaiting a tool outcome, by tool call id.
    pub(crate) pending: Mutex<HashMap<String, PendingOutcome>>,
}

impl LayaApprovalReviewer {
    pub(crate) fn new(thread_manager: Weak<ThreadManager>) -> Self {
        Self {
            thread_manager,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// The Antfly handle for this thread, when the thread store is Antfly.
    pub(crate) async fn antfly_for(
        &self,
        thread_id: codex_protocol::ThreadId,
    ) -> Option<Arc<Antfly>> {
        let manager = self.thread_manager.upgrade()?;
        let thread = manager.get_thread(thread_id).await.ok()?;
        let config = thread.config().await;
        match &config.experimental_thread_store {
            ThreadStoreConfig::Antfly(antfly) => Some(codex_antfly::shared(antfly)),
            _ => None,
        }
    }

    /// Searches every recorded approval for ones whose `search_text`
    /// resembles `action_text`, regardless of thread: precedents are meant
    /// to generalize across the whole local history, not just this thread.
    async fn precedents(antfly: &Antfly, action_text: &str) -> Vec<String> {
        let hits = match antfly
            .documents(schema::APPROVALS)
            .search_text(action_text, /*filter*/ None, PRECEDENTS, PRECEDENTS)
            .await
        {
            Ok(hits) => hits,
            Err(err) => {
                tracing::debug!("approval precedent search failed: {err}");
                return Vec::new();
            }
        };
        hits.into_iter()
            .take(PRECEDENTS)
            .filter_map(|hit| {
                let doc = hit.doc?;
                let summary = doc.get("search_text")?.as_str()?.to_string();
                let outcome = doc
                    .get("outcome")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                Some(format!("{summary} -> {outcome}"))
            })
            .collect()
    }

    async fn review(&self, input: &ApprovalDecisionInput<'_>) -> Option<ApprovalDecision> {
        // Full access needs no review. Requests that require a fresh review
        // (escalated commands, retries) are still assessed and recorded; only
        // what may be applied changes (see `applied_decision`).
        if input.full_access {
            return None;
        }
        let antfly = self.antfly_for(input.thread_id).await?;
        let settings = antfly.config().approvals;
        if settings.mode == ApprovalMode::Off {
            return None;
        }

        let user_request = input
            .thread_store
            .get::<LatestUserRequest>()
            .map(|request| request.0.clone());
        let category = format!("{:?}", input.category);
        let action_text = input.action.to_string();
        let precedents = Self::precedents(&antfly, &action_text).await;
        let state = describe_state(
            user_request.as_deref(),
            &category,
            input.action,
            &precedents,
        );
        let request = build_decide_request(antfly.config().approval_decide_model(), &state);
        let response = match antfly.decide(&request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("local approval review failed: {err}");
                return None;
            }
        };
        let Some(answers) = Answers::parse(&response) else {
            tracing::warn!("local approval review returned unexpected answers: {response}");
            return None;
        };
        let decided = verdict(&answers, &settings);
        tracing::info!(
            approval_id = input.approval_id,
            mode = ?settings.mode,
            none = answers.none,
            local = answers.local,
            external = answers.external,
            destructive = answers.destructive,
            intent_match = answers.intent_match,
            verdict = ?decided,
            "local approval review"
        );

        let applied = applied_decision(&settings, &decided, input.require_fresh_review);
        self.record(
            &antfly,
            input,
            &state,
            &answers,
            &decided,
            applied.is_some(),
        )
        .await;
        applied
    }

    /// Records this decision as a document in [`schema::APPROVALS`], keyed
    /// by the approval's own id. A simple id (rather than a time-ordered key,
    /// as the legacy `approval:{desc ts}:{id}` layout used) is enough:
    /// precedent search ranks by relevance, not recency, and a newest-first
    /// listing (if one is ever added) can sort on the declared
    /// `decided_at_ms` field through SQL or `antfly_search`, same as any
    /// other document table.
    async fn record(
        &self,
        antfly: &Arc<Antfly>,
        input: &ApprovalDecisionInput<'_>,
        state: &str,
        answers: &Answers,
        decided: &Verdict,
        applied: bool,
    ) {
        let now = chrono_millis();
        let id = input.approval_id.to_string();
        let settings = antfly.config().approvals;
        let outcome = match (applied, decided) {
            (true, Verdict::Deny { .. }) => "denied_by_review",
            _ => "pending",
        };
        let doc = json!({
            "thread_id": input.thread_id.to_string(),
            "approval_id": input.approval_id,
            "tool_call_id": input.tool_call_id,
            "category": format!("{:?}", input.category),
            "fresh_review_required": input.require_fresh_review,
            "search_text": state,
            "action": input.action,
            "answers": {
                "none": answers.none,
                "local": answers.local,
                "external": answers.external,
                "destructive": answers.destructive,
                "intent_match": answers.intent_match,
                "intent_match_probability": answers.intent_match_probability,
            },
            "verdict": format!("{decided:?}"),
            "applied": applied,
            "outcome": outcome,
            "decided_at_ms": now,
            // What produced the verdict, for calibration across settings.
            "mode": settings.mode,
            "decide_model": antfly.config().approval_decide_model(),
            "allow_threshold_bp": settings.allow_threshold_bp,
            "deny_threshold_bp": settings.deny_threshold_bp,
            "record_version": RECORD_VERSION,
        });
        if outcome == "pending"
            && let Some(call_id) = input.tool_call_id
            && let Ok(mut pending) = self.pending.lock()
        {
            pending.insert(
                call_id.to_string(),
                PendingOutcome {
                    id: id.clone(),
                    doc: doc.clone(),
                    antfly: Arc::clone(antfly),
                    approved: None,
                    resolution_write: None,
                },
            );
        }
        if let Err(err) = antfly
            .documents(schema::APPROVALS)
            .write(vec![Write::put(id, doc)])
            .await
        {
            tracing::warn!("failed to record approval decision: {err}");
        }
    }
}

fn chrono_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

impl ApprovalReviewContributor for LayaApprovalReviewer {
    fn decide<'a>(
        &'a self,
        input: &'a ApprovalDecisionInput<'_>,
    ) -> ExtensionFuture<'a, Option<ApprovalDecision>> {
        Box::pin(self.review(input))
    }
}

impl TurnInputContributor for LayaApprovalReviewer {
    fn contribute<'a>(
        &'a self,
        input: TurnInputContext<'a>,
        _extension_metrics: Option<Arc<dyn ExtensionMetrics>>,
        _session_store: &'a ExtensionData,
        thread_store: &'a ExtensionData,
        _turn_store: &'a ExtensionData,
    ) -> ExtensionFuture<'a, Vec<Box<dyn ContextualUserFragment + Send>>> {
        Box::pin(async move {
            let text = input
                .user_input
                .iter()
                .filter_map(|item| match item {
                    UserInput::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            if !text.trim().is_empty() {
                thread_store.insert(LatestUserRequest(text));
            }
            Vec::new()
        })
    }
}

/// What a verdict may change. Shadow mode only records. In enforce mode a
/// confident denial is applied. A confident allow is applied only when
/// `apply_allow` is set and the host does not require a fresh review
/// (escalated commands, retries): the host does not trust a fast allow
/// there, so the request goes to the user (or the configured reviewer).
pub(crate) fn applied_decision(
    settings: &ApprovalSettings,
    verdict: &Verdict,
    require_fresh_review: bool,
) -> Option<ApprovalDecision> {
    match (settings.mode, verdict) {
        (ApprovalMode::Enforce, Verdict::Allow)
            if settings.apply_allow && !require_fresh_review =>
        {
            Some(ApprovalDecision::Allow)
        }
        (ApprovalMode::Enforce, Verdict::Deny { reason }) => {
            Some(ApprovalDecision::Reviewed(ReviewDecision::Denied {
                rejection: reason.clone(),
            }))
        }
        _ => None,
    }
}

#[cfg(test)]
mod applied_decision_tests {
    use super::*;

    fn settings(mode: ApprovalMode, apply_allow: bool) -> ApprovalSettings {
        ApprovalSettings {
            mode,
            apply_allow,
            ..ApprovalSettings::default()
        }
    }

    #[test]
    fn enforce_applies_denials_and_allows_only_when_asked() {
        let deny = Verdict::Deny {
            reason: "destructive".to_string(),
        };
        let enforce = settings(ApprovalMode::Enforce, false);
        let enforce_allow = settings(ApprovalMode::Enforce, true);
        let shadow = settings(ApprovalMode::Shadow, true);
        // Allows are advisory unless apply_allow, and never fast without a
        // fresh review.
        assert!(applied_decision(&enforce, &Verdict::Allow, false).is_none());
        assert!(matches!(
            applied_decision(&enforce_allow, &Verdict::Allow, false),
            Some(ApprovalDecision::Allow)
        ));
        assert!(applied_decision(&enforce_allow, &Verdict::Allow, true).is_none());
        for fresh in [false, true] {
            assert!(matches!(
                applied_decision(&enforce, &deny, fresh),
                Some(ApprovalDecision::Reviewed(ReviewDecision::Denied { .. }))
            ));
            assert!(applied_decision(&shadow, &deny, fresh).is_none());
            assert!(applied_decision(&shadow, &Verdict::Allow, fresh).is_none());
        }
    }
}
