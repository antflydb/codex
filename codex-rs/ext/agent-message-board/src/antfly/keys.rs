//! Key layout for one board in Antfly.
//!
//! ```text
//! amb:tomb:{board}                                  deletion tombstone
//! amb:seq:{board}                                   monotonic post sequence
//! amb:ch:{board}:{channel}                          channel record
//! amb:post:{board}:{post}                            post record (canonical)
//! amb:req:{board}:{caller}:{request_id}             idempotency record
//! amb:sub:{board}:{target}:{agent}                  subscription state
//! ```
//!
//! `{channel}` and `{request_id}` are percent-escaped so they cannot contain
//! `:`. `{target}` is a percent-escaped JSON encoding of [`SubscriptionTarget`]
//! for the same reason.

use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::Result;

use crate::SubscriptionTarget;

pub(super) const TOMBSTONE_PREFIX: &str = "amb:tomb:";
pub(super) const SEQUENCE_PREFIX: &str = "amb:seq:";
pub(super) const CHANNEL_PREFIX: &str = "amb:ch:";
pub(super) const POST_PREFIX: &str = "amb:post:";
pub(super) const REQUEST_PREFIX: &str = "amb:req:";
pub(super) const SUBSCRIPTION_PREFIX: &str = "amb:sub:";

pub(super) fn tombstone(board: SessionId) -> String {
    format!("{TOMBSTONE_PREFIX}{board}")
}

pub(super) fn sequence(board: SessionId) -> String {
    format!("{SEQUENCE_PREFIX}{board}")
}

pub(super) fn channel(board: SessionId, name: &str) -> String {
    format!(
        "{CHANNEL_PREFIX}{board}:{}",
        codex_antfly::keys::escape(name)
    )
}

pub(super) fn channel_prefix(board: SessionId) -> String {
    format!("{CHANNEL_PREFIX}{board}:")
}

pub(super) fn post(board: SessionId, id: uuid::Uuid) -> String {
    format!("{POST_PREFIX}{board}:{id}")
}

pub(super) fn post_prefix(board: SessionId) -> String {
    format!("{POST_PREFIX}{board}:")
}

pub(super) fn request(board: SessionId, caller: ThreadId, request_id: &str) -> String {
    format!(
        "{REQUEST_PREFIX}{board}:{caller}:{}",
        codex_antfly::keys::escape(request_id)
    )
}

pub(super) fn request_prefix(board: SessionId) -> String {
    format!("{REQUEST_PREFIX}{board}:")
}

fn target_key(target: &SubscriptionTarget) -> Result<String> {
    let raw = serde_json::to_string(target).map_err(super::storage_error)?;
    Ok(codex_antfly::keys::escape(&raw))
}

pub(super) fn subscription_target_prefix(
    board: SessionId,
    target: &SubscriptionTarget,
) -> Result<String> {
    Ok(format!(
        "{SUBSCRIPTION_PREFIX}{board}:{}:",
        target_key(target)?
    ))
}

pub(super) fn subscription(
    board: SessionId,
    target: &SubscriptionTarget,
    agent: ThreadId,
) -> Result<String> {
    Ok(format!(
        "{}{agent}",
        subscription_target_prefix(board, target)?
    ))
}

pub(super) fn subscription_prefix(board: SessionId) -> String {
    format!("{SUBSCRIPTION_PREFIX}{board}:")
}

/// Recovers the agent from a subscription key's trailing component.
pub(super) fn parse_subscription_agent(key: &str) -> Option<ThreadId> {
    let (_, agent) = key.rsplit_once(':')?;
    ThreadId::from_string(agent).ok()
}
