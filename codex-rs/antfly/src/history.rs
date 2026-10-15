//! Reads of `codex_history_items`, the raw rollout items of every thread,
//! keyed `{thread_id}:{ordinal:020}` (written by `codex-thread-store`).

use serde_json::Value;

use crate::Antfly;
use crate::AntflyResult;
use crate::ScanRequest;
use crate::schema;

/// Key of `thread_id`'s item at `ordinal`.
pub fn item_key(thread_id: &str, ordinal: u64) -> String {
    format!("{thread_id}:{}", crate::keys::ordinal(ordinal))
}

/// Up to `limit` stored items of `thread_id` from `from_ordinal` on, as
/// `(ordinal, item)` in order. Each item is a serialized `RolloutItem`.
pub async fn items_from(
    antfly: &Antfly,
    thread_id: &str,
    from_ordinal: u64,
    limit: usize,
) -> AntflyResult<Vec<(u64, Value)>> {
    let prefix = format!("{thread_id}:");
    let request = ScanRequest {
        from: item_key(thread_id, from_ordinal),
        to: crate::keys::prefix_end(&prefix),
        limit: Some(limit),
    };
    Ok(antfly
        .documents(schema::HISTORY_ITEMS)
        .scan(request)
        .await?
        .into_iter()
        .filter_map(|document| {
            let ordinal = document.doc.get("ordinal")?.as_u64()?;
            let item = document.doc.get("item")?.clone();
            Some((ordinal, item))
        })
        .collect())
}
