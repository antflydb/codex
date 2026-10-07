//! Key layout for Codex threads in an Antfly table.
//!
//! ```text
//! ts:t:{thread}                              thread record
//! ts:i:{thread}:{ordinal}                    persisted rollout item, in order
//! ts:x:{a|l}:{c|u|r}:{desc ms}:{thread}      listing index (archived or live),
//!                                             newest first; value is the record
//! ts:s:{section}:{position}:{thread}         section membership, in position order
//! ts:rp:{path}                               legacy rollout path -> thread id
//!
//! Paginated-history projection (§2.25), maintained alongside raw items in the
//! same write so it never diverges:
//!
//! ts:ptid:{thread}:{turn}                    turn doc, direct lookup by turn id
//! ts:pt:{thread}:{start ordinal}:{turn}      turn doc, ordered by start ordinal
//! ts:pte:{thread}:{end ordinal}:{turn}       turn doc, ordered by end ordinal
//!                                             (written once, when a turn becomes terminal)
//! ts:pid:{thread}:{turn}:{item}              item doc, direct lookup
//! ts:pi:{thread}:{created ordinal}:{turn}:{item}   item doc, ordered by creation
//! ts:piu:{thread}:{updated ordinal}:{turn}:{item}  item doc, ordered by last update
//! ts:rt:{thread}:{ordinal}:{item}            realtime item doc, in order
//! ```

use codex_antfly::keys;
use codex_protocol::ThreadId;

use crate::ThreadSortKey;

pub(crate) const THREAD_PREFIX: &str = "ts:t:";
pub(crate) const ITEM_PREFIX: &str = "ts:i:";
pub(crate) const INDEX_PREFIX: &str = "ts:x:";
pub(crate) const SECTION_PREFIX: &str = "ts:s:";
pub(crate) const ROLLOUT_PATH_PREFIX: &str = "ts:rp:";
pub(crate) const TURN_ID_PREFIX: &str = "ts:ptid:";
pub(crate) const TURN_START_PREFIX: &str = "ts:pt:";
pub(crate) const TURN_END_PREFIX: &str = "ts:pte:";
pub(crate) const ITEM_ID_PREFIX: &str = "ts:pid:";
pub(crate) const ITEM_CREATED_PREFIX: &str = "ts:pi:";
pub(crate) const ITEM_UPDATED_PREFIX: &str = "ts:piu:";
pub(crate) const REALTIME_PREFIX: &str = "ts:rt:";

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

pub(crate) fn turn_id_key(thread_id: ThreadId, turn_id: &str) -> String {
    format!("{TURN_ID_PREFIX}{thread_id}:{}", keys::escape(turn_id))
}

pub(crate) fn turn_id_prefix(thread_id: ThreadId) -> String {
    format!("{TURN_ID_PREFIX}{thread_id}:")
}

pub(crate) fn turn_by_start(thread_id: ThreadId, start_ordinal: u64, turn_id: &str) -> String {
    format!(
        "{TURN_START_PREFIX}{thread_id}:{}:{}",
        keys::ordinal(start_ordinal),
        keys::escape(turn_id)
    )
}

pub(crate) fn turn_start_bound(thread_id: ThreadId, ordinal: u64) -> String {
    format!("{TURN_START_PREFIX}{thread_id}:{}", keys::ordinal(ordinal))
}

pub(crate) fn turn_start_prefix(thread_id: ThreadId) -> String {
    format!("{TURN_START_PREFIX}{thread_id}:")
}

pub(crate) fn turn_by_end(thread_id: ThreadId, end_ordinal: u64, turn_id: &str) -> String {
    format!(
        "{TURN_END_PREFIX}{thread_id}:{}:{}",
        keys::ordinal(end_ordinal),
        keys::escape(turn_id)
    )
}

pub(crate) fn turn_end_prefix(thread_id: ThreadId) -> String {
    format!("{TURN_END_PREFIX}{thread_id}:")
}

pub(crate) fn item_id_key(thread_id: ThreadId, turn_id: &str, item_id: &str) -> String {
    format!(
        "{ITEM_ID_PREFIX}{thread_id}:{}:{}",
        keys::escape(turn_id),
        keys::escape(item_id)
    )
}

pub(crate) fn item_id_prefix(thread_id: ThreadId) -> String {
    format!("{ITEM_ID_PREFIX}{thread_id}:")
}

pub(crate) fn item_by_created(
    thread_id: ThreadId,
    created_ordinal: u64,
    turn_id: &str,
    item_id: &str,
) -> String {
    format!(
        "{ITEM_CREATED_PREFIX}{thread_id}:{}:{}:{}",
        keys::ordinal(created_ordinal),
        keys::escape(turn_id),
        keys::escape(item_id)
    )
}

pub(crate) fn item_created_bound(thread_id: ThreadId, ordinal: u64) -> String {
    format!(
        "{ITEM_CREATED_PREFIX}{thread_id}:{}",
        keys::ordinal(ordinal)
    )
}

pub(crate) fn item_created_prefix(thread_id: ThreadId) -> String {
    format!("{ITEM_CREATED_PREFIX}{thread_id}:")
}

pub(crate) fn item_by_updated(
    thread_id: ThreadId,
    updated_ordinal: u64,
    turn_id: &str,
    item_id: &str,
) -> String {
    format!(
        "{ITEM_UPDATED_PREFIX}{thread_id}:{}:{}:{}",
        keys::ordinal(updated_ordinal),
        keys::escape(turn_id),
        keys::escape(item_id)
    )
}

pub(crate) fn item_updated_prefix(thread_id: ThreadId) -> String {
    format!("{ITEM_UPDATED_PREFIX}{thread_id}:")
}

pub(crate) fn realtime(thread_id: ThreadId, ordinal: u64, item_id: &str) -> String {
    format!(
        "{REALTIME_PREFIX}{thread_id}:{}:{}",
        keys::ordinal(ordinal),
        keys::escape(item_id)
    )
}

pub(crate) fn realtime_bound(thread_id: ThreadId, ordinal: u64) -> String {
    format!("{REALTIME_PREFIX}{thread_id}:{}", keys::ordinal(ordinal))
}

pub(crate) fn realtime_prefix(thread_id: ThreadId) -> String {
    format!("{REALTIME_PREFIX}{thread_id}:")
}
