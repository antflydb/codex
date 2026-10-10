//! Antfly backend for external agent config import history, backed by the
//! `codex_external_agent_config_imports` SQL table (see
//! `codex-antfly/src/schema.rs`, migration 1). Mirrors
//! `state/src/runtime/external_agent_config_imports.rs` exactly; `successes`
//! and `failures` are JSONB rather than JSON text.

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;

use super::internal;
use crate::model::datetime_to_epoch_millis;
use crate::runtime::ExternalAgentConfigImportDetailsRecord;
use crate::runtime::ExternalAgentConfigImportFailureRecord;
use crate::runtime::ExternalAgentConfigImportHistoryRecord;
use crate::runtime::ExternalAgentConfigImportSuccessRecord;

pub(crate) async fn record_external_agent_config_import_completed(
    antfly: &Arc<Antfly>,
    import_id: &str,
    provider_id: Option<&str>,
    successes: &[ExternalAgentConfigImportSuccessRecord],
    failures: &[ExternalAgentConfigImportFailureRecord],
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_external_agent_config_imports (
            import_id,
            provider_id,
            completed_at_ms,
            successes,
            failures
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (import_id) DO UPDATE SET
            provider_id = excluded.provider_id,
            completed_at_ms = excluded.completed_at_ms,
            successes = excluded.successes,
            failures = excluded.failures",
        sql_params![
            import_id,
            provider_id,
            datetime_to_epoch_millis(Utc::now()),
            serde_json::to_value(successes)?,
            serde_json::to_value(failures)?,
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn external_agent_config_import_details_record(
    antfly: &Arc<Antfly>,
    import_id: &str,
) -> anyhow::Result<Option<ExternalAgentConfigImportDetailsRecord>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT successes, failures FROM codex_external_agent_config_imports \
             WHERE import_id = $1",
            sql_params![import_id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| {
        Ok(ExternalAgentConfigImportDetailsRecord {
            successes: serde_json::from_value(row.json("successes").map_err(internal)?)?,
            failures: serde_json::from_value(row.json("failures").map_err(internal)?)?,
        })
    })
    .transpose()
}

fn history_record_from_row(row: &SqlRow) -> anyhow::Result<ExternalAgentConfigImportHistoryRecord> {
    Ok(ExternalAgentConfigImportHistoryRecord {
        import_id: row.string("import_id").map_err(internal)?,
        provider_id: row.opt_string("provider_id").map_err(internal)?,
        completed_at_ms: row.i64("completed_at_ms").map_err(internal)?,
        successes: serde_json::from_value(row.json("successes").map_err(internal)?)?,
        failures: serde_json::from_value(row.json("failures").map_err(internal)?)?,
    })
}

pub(crate) async fn external_agent_config_import_history_records(
    antfly: &Arc<Antfly>,
) -> anyhow::Result<Vec<ExternalAgentConfigImportHistoryRecord>> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.fetch_all(
        "SELECT import_id, provider_id, completed_at_ms, successes, failures \
         FROM codex_external_agent_config_imports \
         ORDER BY completed_at_ms DESC, import_id ASC",
        vec![],
    )
    .await
    .map_err(internal)?
    .iter()
    .map(history_record_from_row)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::super::sql_test_support::AntflyRuntime;
    use super::*;
    use pretty_assertions::assert_eq;

    fn success(item_type: &str, source: &str) -> ExternalAgentConfigImportSuccessRecord {
        ExternalAgentConfigImportSuccessRecord {
            item_type: item_type.to_string(),
            cwd: None,
            source: Some(source.to_string()),
            target: Some(source.to_string()),
            title: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn records_completion_by_import_id_and_lists_history() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        runtime
            .record_external_agent_config_import_completed(
                "import-1",
                Some("provider-1"),
                &[success("CONFIG", "settings.json")],
                &[],
            )
            .await?;
        let failure = ExternalAgentConfigImportFailureRecord {
            item_type: "MCP_SERVER_CONFIG".to_string(),
            error_type: None,
            sub_error_type: Some("failed_to_copy_plugin_file".to_string()),
            failure_stage: "import".to_string(),
            message: "failed".to_string(),
            cwd: None,
            source: Some("broken".to_string()),
        };
        runtime
            .record_external_agent_config_import_completed(
                "import-1",
                Some("provider-2"),
                &[
                    success("CONFIG", "settings.json"),
                    success("MCP_SERVER_CONFIG", "github"),
                ],
                std::slice::from_ref(&failure),
            )
            .await?;

        assert_eq!(
            Some(ExternalAgentConfigImportDetailsRecord {
                successes: vec![
                    success("CONFIG", "settings.json"),
                    success("MCP_SERVER_CONFIG", "github"),
                ],
                failures: vec![failure.clone()],
            }),
            runtime
                .external_agent_config_import_details_record("import-1")
                .await?
        );
        assert_eq!(
            None,
            runtime
                .external_agent_config_import_details_record("missing")
                .await?
        );

        // A later completion sorts first (completed_at_ms DESC).
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        runtime
            .record_external_agent_config_import_completed("import-2", None, &[], &[])
            .await?;
        let history = runtime
            .external_agent_config_import_history_records()
            .await?;
        assert_eq!(
            vec!["import-2", "import-1"],
            history
                .iter()
                .map(|record| record.import_id.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(None, history[0].provider_id);
        assert_eq!(Some("provider-2".to_string()), history[1].provider_id);
        assert_eq!(vec![failure], history[1].failures);
        assert!(history[0].completed_at_ms >= history[1].completed_at_ms);
        harness.close().await;
        Ok(())
    }
}
