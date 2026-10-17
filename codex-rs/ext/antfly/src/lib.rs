//! Local typed-decision review of approval requests, backed by Antfly.
//!
//! When the thread store is Antfly and `approvals.mode` is `shadow` or
//! `enforce`, each approval request is described as text and answered by a
//! local typed-decision model (Laya by default) through `Inference::decide`.
//! Shadow mode only records decisions. Enforce mode allows confident safe
//! actions and denies confident destructive ones; everything else continues
//! to Guardian or the user. Decisions and their observed outcomes are stored
//! in Antfly for calibration and as context for later requests.
//!
//! When the knowledge graph is enabled it also extracts claims from finished
//! turns into it (the claim pipeline in `claims`) and contributes
//! `knowledge.search`, which surfaces those claims and related memory notes
//! (see [`knowledge`]).

mod claims;

/// `codex knowledge`: status, backfill and consolidation over history.
pub use claims::admin as knowledge_admin;
pub mod knowledge;
mod outcomes;
mod request;
mod reviewer;

use std::sync::Arc;
use std::sync::Weak;

use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_extension_api::ExtensionRegistryBuilder;

pub use request::Answers;
pub use request::Verdict;
pub use request::build_decide_request;
pub use request::verdict;

/// Installs the reviewer, the turn-input capture it relies on, the outcome
/// recorder, `knowledge.search` and the per-turn claim pipeline. Register
/// before Guardian so confident local decisions win; when this reviewer
/// declines, Guardian and the user flow run as usual.
pub fn install(
    registry: &mut ExtensionRegistryBuilder<Config>,
    thread_manager: Weak<ThreadManager>,
) {
    let claims = claims::ClaimPipeline::new(thread_manager.clone());
    registry.thread_lifecycle_contributor(Arc::clone(&claims) as _);
    registry.turn_lifecycle_contributor(Arc::clone(&claims) as _);
    registry.config_contributor(claims);
    let reviewer = Arc::new(reviewer::LayaApprovalReviewer::new(thread_manager));
    registry.turn_input_contributor(Arc::clone(&reviewer) as _);
    registry.tool_lifecycle_contributor(Arc::new(outcomes::OutcomeRecorder::new(Arc::clone(
        &reviewer,
    ))));
    registry.approval_review_contributor(reviewer);
    let knowledge = Arc::new(knowledge::KnowledgeExtension);
    registry.thread_lifecycle_contributor(Arc::clone(&knowledge) as _);
    registry.config_contributor(Arc::clone(&knowledge) as _);
    registry.tool_contributor(knowledge);
}

/// Runs unit tests that open a database or load local models one at a
/// time: several loading models at once can exceed inference memory
/// admission and fail spuriously.
#[cfg(test)]
pub(crate) static MODEL_TESTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
