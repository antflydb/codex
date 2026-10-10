//! Antfly backend for remote-control enrollments, backed by the
//! `codex_remote_control_enrollments` SQL table (see
//! `codex-antfly/src/schema.rs`, migration 1). Mirrors
//! `state/src/runtime/remote_control.rs` exactly, including storing an absent
//! `app_server_client_name` as `''` so it can be part of the primary key,
//! and leaving `remote_control_enabled` untouched when an existing
//! enrollment is re-upserted.

use std::sync::Arc;

use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql_params;

use super::internal;
use crate::runtime::RemoteControlEnrollmentRecord;

const APP_SERVER_CLIENT_NAME_NONE: &str = "";

fn client_name_key(app_server_client_name: Option<&str>) -> &str {
    app_server_client_name.unwrap_or(APP_SERVER_CLIENT_NAME_NONE)
}

fn enrollment_from_row(row: &SqlRow) -> anyhow::Result<RemoteControlEnrollmentRecord> {
    let app_server_client_name = row.string("app_server_client_name").map_err(internal)?;
    Ok(RemoteControlEnrollmentRecord {
        websocket_url: row.string("websocket_url").map_err(internal)?,
        account_id: row.string("account_id").map_err(internal)?,
        app_server_client_name: (!app_server_client_name.is_empty())
            .then_some(app_server_client_name),
        server_id: row.string("server_id").map_err(internal)?,
        environment_id: row.string("environment_id").map_err(internal)?,
        server_name: row.string("server_name").map_err(internal)?,
        remote_control_enabled: row.opt_bool("remote_control_enabled").map_err(internal)?,
    })
}

pub(crate) async fn get_remote_control_enrollment(
    antfly: &Arc<Antfly>,
    websocket_url: &str,
    account_id: &str,
    app_server_client_name: Option<&str>,
) -> anyhow::Result<Option<RemoteControlEnrollmentRecord>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT websocket_url, account_id, app_server_client_name, server_id, \
             environment_id, server_name, remote_control_enabled \
             FROM codex_remote_control_enrollments \
             WHERE websocket_url = $1 AND account_id = $2 AND app_server_client_name = $3",
            sql_params![
                websocket_url,
                account_id,
                client_name_key(app_server_client_name)
            ],
        )
        .await
        .map_err(internal)?;
    row.map(|row| enrollment_from_row(&row)).transpose()
}

pub(crate) async fn upsert_remote_control_enrollment(
    antfly: &Arc<Antfly>,
    enrollment: &RemoteControlEnrollmentRecord,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_remote_control_enrollments (
            websocket_url,
            account_id,
            app_server_client_name,
            server_id,
            environment_id,
            server_name,
            remote_control_enabled,
            updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (websocket_url, account_id, app_server_client_name) DO UPDATE SET
            server_id = excluded.server_id,
            environment_id = excluded.environment_id,
            server_name = excluded.server_name,
            updated_at = excluded.updated_at",
        sql_params![
            enrollment.websocket_url.as_str(),
            enrollment.account_id.as_str(),
            client_name_key(enrollment.app_server_client_name.as_deref()),
            enrollment.server_id.as_str(),
            enrollment.environment_id.as_str(),
            enrollment.server_name.as_str(),
            enrollment.remote_control_enabled,
            Utc::now().timestamp(),
        ],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn set_remote_control_enabled(
    antfly: &Arc<Antfly>,
    websocket_url: &str,
    account_id: &str,
    app_server_client_name: Option<&str>,
    remote_control_enabled: bool,
) -> anyhow::Result<u64> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "UPDATE codex_remote_control_enrollments \
         SET remote_control_enabled = $1, updated_at = $2 \
         WHERE websocket_url = $3 AND account_id = $4 AND app_server_client_name = $5",
        sql_params![
            remote_control_enabled,
            Utc::now().timestamp(),
            websocket_url,
            account_id,
            client_name_key(app_server_client_name),
        ],
    )
    .await
    .map_err(internal)
}

pub(crate) async fn delete_remote_control_enrollment(
    antfly: &Arc<Antfly>,
    websocket_url: &str,
    account_id: &str,
    app_server_client_name: Option<&str>,
) -> anyhow::Result<u64> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "DELETE FROM codex_remote_control_enrollments \
         WHERE websocket_url = $1 AND account_id = $2 AND app_server_client_name = $3",
        sql_params![
            websocket_url,
            account_id,
            client_name_key(app_server_client_name)
        ],
    )
    .await
    .map_err(internal)
}

#[cfg(test)]
mod tests {
    use super::super::sql_test_support::AntflyRuntime;
    use super::*;
    use pretty_assertions::assert_eq;

    fn enrollment(
        websocket_url: &str,
        account_id: &str,
        app_server_client_name: Option<&str>,
        server_id: &str,
        remote_control_enabled: Option<bool>,
    ) -> RemoteControlEnrollmentRecord {
        RemoteControlEnrollmentRecord {
            websocket_url: websocket_url.to_string(),
            account_id: account_id.to_string(),
            app_server_client_name: app_server_client_name.map(str::to_string),
            server_id: server_id.to_string(),
            environment_id: format!("env-{server_id}"),
            server_name: format!("name-{server_id}"),
            remote_control_enabled,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remote_control_enrollment_round_trips_by_target_and_account() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        let url = "wss://example.com/backend-api/wham/remote/control/server";
        let first = enrollment(url, "account-a", None, "srv_e_first", Some(false));
        let second = enrollment(
            url,
            "account-b",
            Some("desktop"),
            "srv_e_second",
            Some(true),
        );
        runtime.upsert_remote_control_enrollment(&first).await?;
        runtime.upsert_remote_control_enrollment(&second).await?;

        assert_eq!(
            Some(first.clone()),
            runtime
                .get_remote_control_enrollment(url, "account-a", None)
                .await?
        );
        assert_eq!(
            Some(second.clone()),
            runtime
                .get_remote_control_enrollment(url, "account-b", Some("desktop"))
                .await?
        );
        assert_eq!(
            None,
            runtime
                .get_remote_control_enrollment(url, "account-b", None)
                .await?
        );

        // Re-upserting replaces the server identity but keeps the preference.
        let replaced = enrollment(url, "account-a", None, "srv_e_replaced", Some(true));
        runtime.upsert_remote_control_enrollment(&replaced).await?;
        assert_eq!(
            Some(RemoteControlEnrollmentRecord {
                remote_control_enabled: Some(false),
                ..replaced
            }),
            runtime
                .get_remote_control_enrollment(url, "account-a", None)
                .await?
        );

        assert_eq!(
            1,
            runtime
                .set_remote_control_enabled(url, "account-a", None, true)
                .await?
        );
        assert_eq!(
            Some(true),
            runtime
                .get_remote_control_enrollment(url, "account-a", None)
                .await?
                .and_then(|enrollment| enrollment.remote_control_enabled)
        );
        assert_eq!(
            0,
            runtime
                .set_remote_control_enabled(url, "account-missing", None, true)
                .await?
        );
        harness.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_remote_control_enrollment_removes_only_matching_entry() -> anyhow::Result<()> {
        let harness = AntflyRuntime::open().await;
        let runtime = &harness.runtime;
        let url = "wss://example.com/backend-api/wham/remote/control/server";
        let first = enrollment(url, "account-a", None, "srv_e_first", None);
        let second = enrollment(
            url,
            "account-a",
            Some("desktop"),
            "srv_e_second",
            Some(true),
        );
        runtime.upsert_remote_control_enrollment(&first).await?;
        runtime.upsert_remote_control_enrollment(&second).await?;

        assert_eq!(
            1,
            runtime
                .delete_remote_control_enrollment(url, "account-a", None)
                .await?
        );
        assert_eq!(
            None,
            runtime
                .get_remote_control_enrollment(url, "account-a", None)
                .await?
        );
        assert_eq!(
            Some(second),
            runtime
                .get_remote_control_enrollment(url, "account-a", Some("desktop"))
                .await?
        );
        assert_eq!(
            0,
            runtime
                .delete_remote_control_enrollment(url, "account-a", None)
                .await?
        );
        harness.close().await;
        Ok(())
    }
}
