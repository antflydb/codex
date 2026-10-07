//! Local typed-decision review of approval requests, backed by Antfly.
//!
//! When the thread store is Antfly and `approvals.mode` is `shadow` or
//! `enforce`, each approval request is described as text and answered by a
//! local typed-decision model (Laya by default) through `Inference::decide`.
//! Shadow mode only records decisions. Enforce mode allows confident safe
//! actions and denies confident destructive ones; everything else continues
//! to Guardian or the user. Decisions and their observed outcomes are stored
//! in Antfly for calibration and as context for later requests.

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

/// Installs the reviewer, the turn-input capture it relies on, and the
/// outcome recorder. Register before Guardian so confident local decisions
/// win; when this reviewer declines, Guardian and the user flow run as usual.
pub fn install(
    registry: &mut ExtensionRegistryBuilder<Config>,
    thread_manager: Weak<ThreadManager>,
) {
    let reviewer = Arc::new(reviewer::LayaApprovalReviewer::new(thread_manager));
    registry.turn_input_contributor(Arc::clone(&reviewer) as _);
    registry.tool_lifecycle_contributor(Arc::new(outcomes::OutcomeRecorder::new(Arc::clone(
        &reviewer,
    ))));
    registry.approval_review_contributor(reviewer);
}
