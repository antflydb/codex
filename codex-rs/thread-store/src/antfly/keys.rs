//! Document ids for `codex_history_items` (see `codex_antfly::schema`).
//!
//! Every other piece of thread state (the thread record, sections, projects,
//! attachments, spawn edges, the paginated-history projection) lives in SQL
//! tables addressed by column, not by a synthetic key; only the raw rollout
//! item history is a document table, keyed `{thread_id}:{ordinal}` so a
//! prefix scan returns one thread's items in order.

use codex_protocol::ThreadId;

pub(crate) fn history_item_id(thread_id: ThreadId, ordinal: u64) -> String {
    format!("{thread_id}:{ordinal:020}")
}

pub(crate) fn history_item_prefix(thread_id: ThreadId) -> String {
    format!("{thread_id}:")
}

/// Parses `(thread, ordinal)` back out of a `codex_history_items` id.
pub(crate) fn parse_history_item_id(id: &str) -> Option<(ThreadId, u64)> {
    let (thread, ordinal) = id.rsplit_once(':')?;
    Some((ThreadId::from_string(thread).ok()?, ordinal.parse().ok()?))
}
