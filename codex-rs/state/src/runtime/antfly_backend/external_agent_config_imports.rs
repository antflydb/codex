//! Antfly backend for external-agent config import history. Mirrors
//! `state/src/runtime/external_agent_config_imports.rs` (the SQLite
//! implementation).

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_antfly::keys;
use serde_json::Value;
use serde_json::json;

use super::internal;
use crate::ExternalAgentConfigImportDetailsRecord;
use crate::ExternalAgentConfigImportFailureRecord;
use crate::ExternalAgentConfigImportHistoryRecord;
use crate::ExternalAgentConfigImportSuccessRecord;
use crate::model::datetime_to_epoch_millis;

const RECORD_PREFIX: &str = "st:extimport:";
const INDEX_PREFIX: &str = "st:extimportx:";

fn record_key(import_id: &str) -> String {
    format!("{RECORD_PREFIX}{}", keys::escape(import_id))
}

fn index_key(completed_at_ms: i64, import_id: &str) -> String {
    format!(
        "{INDEX_PREFIX}{}:{}",
        keys::descending(completed_at_ms),
        keys::escape(import_id)
    )
}

pub(crate) async fn record_external_agent_config_import_completed(
    antfly: &Arc<Antfly>,
    import_id: &str,
    provider_id: Option<&str>,
    successes: &[ExternalAgentConfigImportSuccessRecord],
    failures: &[ExternalAgentConfigImportFailureRecord],
) -> anyhow::Result<()> {
    let completed_at_ms = datetime_to_epoch_millis(Utc::now());
    let doc = json!({
        "import_id": import_id,
        "provider_id": provider_id,
        "completed_at_ms": completed_at_ms,
        "successes": successes,
        "failures": failures,
    });
    let record_key = record_key(import_id);

    let _guard = antfly.lock().await;
    let previous_completed_at_ms = antfly
        .get(record_key.clone())
        .await
        .map_err(internal)?
        .and_then(|previous| previous.get("completed_at_ms").and_then(Value::as_i64));

    let mut writes = vec![Write::put(record_key, doc.clone())];
    if let Some(previous_ms) = previous_completed_at_ms {
        writes.push(Write::delete(index_key(previous_ms, import_id)));
    }
    writes.push(Write::put(index_key(completed_at_ms, import_id), doc));
    antfly.write(writes).await.map_err(internal)
}

pub(crate) async fn external_agent_config_import_details_record(
    antfly: &Arc<Antfly>,
    import_id: &str,
) -> anyhow::Result<Option<ExternalAgentConfigImportDetailsRecord>> {
    let Some(doc) = antfly.get(record_key(import_id)).await.map_err(internal)? else {
        return Ok(None);
    };
    Ok(Some(ExternalAgentConfigImportDetailsRecord {
        successes: serde_json::from_value(doc.get("successes").cloned().unwrap_or_default())?,
        failures: serde_json::from_value(doc.get("failures").cloned().unwrap_or_default())?,
    }))
}

fn history_record_from_doc(doc: Value) -> anyhow::Result<ExternalAgentConfigImportHistoryRecord> {
    Ok(ExternalAgentConfigImportHistoryRecord {
        import_id: doc
            .get("import_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        provider_id: doc
            .get("provider_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        completed_at_ms: doc
            .get("completed_at_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        successes: serde_json::from_value(doc.get("successes").cloned().unwrap_or_default())?,
        failures: serde_json::from_value(doc.get("failures").cloned().unwrap_or_default())?,
    })
}

pub(crate) async fn external_agent_config_import_history_records(
    antfly: &Arc<Antfly>,
) -> anyhow::Result<Vec<ExternalAgentConfigImportHistoryRecord>> {
    // Ascending key order over `st:extimportx:` yields descending
    // `completed_at_ms` (encoded with `keys::descending`) then ascending
    // `import_id`, matching `ORDER BY completed_at_ms DESC, import_id ASC`.
    antfly
        .scan(ScanRequest::prefix(INDEX_PREFIX))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|document| history_record_from_doc(document.doc))
        .collect()
}
