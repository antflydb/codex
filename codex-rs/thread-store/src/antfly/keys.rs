//! Key layout for Codex threads in an Antfly table.
//!
//! ```text
//! ts:t:{thread}                         thread record
//! ts:i:{thread}:{ordinal}               persisted rollout item, in order
//! ts:x:{a|l}:{c|u|r}:{desc ms}:{thread} listing index (archived or live),
//!                                       newest first; value is the record
//! ts:s:{section}:{position}:{thread}    section membership, in position order
//! ts:rp:{path}                          legacy rollout path -> thread id
//! ts:scn:{id}                           thread section definition
//! ts:att:{thread}:{type}:{key}          canonical thread attachment
//! ts:atl:{thread}:{asc created}:{id}    attachment listing for one thread
//! ts:ato:{type}:{key}:{thread}          attachment owners for one identity
//! ts:proj:{id}                          project record
//! ts:pik:{key}                          project idempotency key -> project id
//! ```

use codex_antfly::keys;
use codex_protocol::ThreadId;

use crate::ThreadSortKey;

pub(crate) const THREAD_PREFIX: &str = "ts:t:";
pub(crate) const ITEM_PREFIX: &str = "ts:i:";
pub(crate) const INDEX_PREFIX: &str = "ts:x:";
pub(crate) const SECTION_PREFIX: &str = "ts:s:";
pub(crate) const ROLLOUT_PATH_PREFIX: &str = "ts:rp:";
pub(crate) const SECTION_DEF_PREFIX: &str = "ts:scn:";
pub(crate) const ATTACHMENT_PREFIX: &str = "ts:att:";
pub(crate) const ATTACHMENT_LIST_PREFIX: &str = "ts:atl:";
pub(crate) const ATTACHMENT_OWNER_PREFIX: &str = "ts:ato:";
pub(crate) const PROJECT_PREFIX: &str = "ts:proj:";
pub(crate) const PROJECT_KEY_PREFIX: &str = "ts:pik:";

pub(crate) fn thread(thread_id: ThreadId) -> String {
    format!("{THREAD_PREFIX}{thread_id}")
}

pub(crate) fn items_prefix(thread_id: ThreadId) -> String {
    format!("{ITEM_PREFIX}{thread_id}:")
}

pub(crate) fn item(thread_id: ThreadId, ordinal: u64) -> String {
    format!("{ITEM_PREFIX}{thread_id}:{}", keys::ordinal(ordinal))
}

/// Parses `(thread, ordinal)` from an item key.
pub(crate) fn parse_item(key: &str) -> Option<(ThreadId, u64)> {
    let rest = key.strip_prefix(ITEM_PREFIX)?;
    let (thread, ordinal) = rest.rsplit_once(':')?;
    Some((ThreadId::from_string(thread).ok()?, ordinal.parse().ok()?))
}

pub(crate) fn sort_code(sort_key: ThreadSortKey) -> &'static str {
    match sort_key {
        ThreadSortKey::CreatedAt => "c",
        ThreadSortKey::UpdatedAt => "u",
        // Section ordering has its own index; recency is the closest proxy.
        ThreadSortKey::RecencyAt | ThreadSortKey::SectionPosition => "r",
    }
}

pub(crate) fn index_prefix(archived: bool, sort_key: ThreadSortKey) -> String {
    format!(
        "{INDEX_PREFIX}{}:{}:",
        if archived { "a" } else { "l" },
        sort_code(sort_key)
    )
}

pub(crate) fn index_entry(
    archived: bool,
    sort_key: ThreadSortKey,
    millis: i64,
    thread_id: ThreadId,
) -> String {
    format!(
        "{}{}:{thread_id}",
        index_prefix(archived, sort_key),
        keys::descending(millis)
    )
}

pub(crate) fn section_prefix(section: &str) -> String {
    format!("{SECTION_PREFIX}{}:", keys::escape(section))
}

pub(crate) fn section_entry(section: &str, position: i64, thread_id: ThreadId) -> String {
    format!(
        "{}{}:{thread_id}",
        section_prefix(section),
        keys::ascending(position)
    )
}

pub(crate) fn rollout_path(path: &std::path::Path) -> String {
    format!(
        "{ROLLOUT_PATH_PREFIX}{}",
        keys::escape(&path.to_string_lossy())
    )
}

pub(crate) fn section_def(id: &str) -> String {
    format!("{SECTION_DEF_PREFIX}{}", keys::escape(id))
}

pub(crate) fn attachment(thread_id: ThreadId, attachment_type: &str, identity_key: &str) -> String {
    format!(
        "{ATTACHMENT_PREFIX}{thread_id}:{}:{}",
        keys::escape(attachment_type),
        keys::escape(identity_key)
    )
}

pub(crate) fn attachments_prefix(thread_id: ThreadId) -> String {
    format!("{ATTACHMENT_PREFIX}{thread_id}:")
}

pub(crate) fn attachment_list_entry(
    thread_id: ThreadId,
    created_at: i64,
    attachment_id: &str,
) -> String {
    format!(
        "{ATTACHMENT_LIST_PREFIX}{thread_id}:{}:{attachment_id}",
        keys::ascending(created_at)
    )
}

pub(crate) fn attachment_list_prefix(thread_id: ThreadId) -> String {
    format!("{ATTACHMENT_LIST_PREFIX}{thread_id}:")
}

pub(crate) fn attachment_owner_entry(
    attachment_type: &str,
    identity_key: &str,
    thread_id: ThreadId,
) -> String {
    format!(
        "{ATTACHMENT_OWNER_PREFIX}{}:{}:{thread_id}",
        keys::escape(attachment_type),
        keys::escape(identity_key)
    )
}

pub(crate) fn attachment_owner_prefix(attachment_type: &str, identity_key: &str) -> String {
    format!(
        "{ATTACHMENT_OWNER_PREFIX}{}:{}:",
        keys::escape(attachment_type),
        keys::escape(identity_key)
    )
}

pub(crate) fn project(id: &str) -> String {
    format!("{PROJECT_PREFIX}{}", keys::escape(id))
}

pub(crate) fn project_idempotency_key(key: &str) -> String {
    format!("{PROJECT_KEY_PREFIX}{}", keys::escape(key))
}
