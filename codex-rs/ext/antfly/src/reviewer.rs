use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use codex_antfly::Antfly;
use codex_antfly::ApprovalMode;
use codex_antfly::Write;
use codex_antfly::keys;
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

/// Key prefix for recorded approval decisions.
pub(crate) const APPROVAL_PREFIX: &str = "approval:";
/// Similar earlier decisions included in the model's context.
const PRECEDENTS: usize = 3;

/// Latest user request in a thread, captured from turn input.
struct LatestUserRequest(String);

/// Decision recorded for a tool call that has not finished yet.
pub(crate) struct PendingOutcome {
    pub(crate) key: String,
    pub(crate) doc: Value,
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

    async fn precedents(antfly: &Antfly, action_text: &str) -> Vec<String> {
        let hits = match antfly
            .search_text(APPROVAL_PREFIX, action_text, PRECEDENTS, PRECEDENTS)
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
        // Guardian's guards: full access needs no review, and fresh reviews
        // must not be short-circuited by a cached or fast decision.
        if input.full_access || input.require_fresh_review {
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
        let request = build_decide_request(&antfly.config().decide_model, &state);
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

        let applied = match (&settings.mode, &decided) {
            (ApprovalMode::Enforce, Verdict::Allow) => Some(ApprovalDecision::Allow),
            (ApprovalMode::Enforce, Verdict::Deny { reason }) => {
                Some(ApprovalDecision::Reviewed(ReviewDecision::Denied {
                    rejection: reason.clone(),
                }))
            }
            _ => None,
        };
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

    async fn record(
        &self,
        antfly: &Antfly,
        input: &ApprovalDecisionInput<'_>,
        state: &str,
        answers: &Answers,
        decided: &Verdict,
        applied: bool,
    ) {
        let now = chrono_millis();
        let key = keys::join(&[
            "approval",
            &keys::descending(now),
            &keys::escape(input.approval_id),
        ]);
        let outcome = match (applied, decided) {
            (true, Verdict::Deny { .. }) => "denied_by_review",
            _ => "pending",
        };
        let doc = json!({
            "thread_id": input.thread_id.to_string(),
            "approval_id": input.approval_id,
            "tool_call_id": input.tool_call_id,
            "category": format!("{:?}", input.category),
            "search_text": state,
            "action": input.action,
            "answers": {
                "none": answers.none,
                "local": answers.local,
                "external": answers.external,
                "destructive": answers.destructive,
                "intent_match": answers.intent_match,
            },
            "verdict": format!("{decided:?}"),
            "applied": applied,
            "outcome": outcome,
            "decided_at_ms": now,
        });
        if outcome == "pending"
            && let Some(call_id) = input.tool_call_id
            && let Ok(mut pending) = self.pending.lock()
        {
            pending.insert(
                call_id.to_string(),
                PendingOutcome {
                    key: key.clone(),
                    doc: doc.clone(),
                },
            );
        }
        if let Err(err) = antfly.write(vec![Write::put(key, doc)]).await {
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
