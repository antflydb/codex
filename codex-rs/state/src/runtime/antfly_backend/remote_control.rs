//! Antfly backend for remote-control enrollments. Mirrors
//! `state/src/runtime/remote_control.rs` (the SQLite implementation).

use std::sync::Arc;

use codex_antfly::Antfly;
use codex_antfly::Write;
use codex_antfly::keys;

use super::internal;
use crate::RemoteControlEnrollmentRecord;

const PREFIX: &str = "st:remote:";
/// Same sentinel the SQLite implementation uses for a `NULL` client name, so
/// an absent name never collides with an empty-string one.
const CLIENT_NAME_NONE: &str = "";

fn key(websocket_url: &str, account_id: &str, app_server_client_name: Option<&str>) -> String {
    format!(
        "{PREFIX}{}:{}:{}",
        keys::escape(websocket_url),
        keys::escape(account_id),
        keys::escape(app_server_client_name.unwrap_or(CLIENT_NAME_NONE)),
    )
}

pub(crate) async fn get_remote_control_enrollment(
    antfly: &Arc<Antfly>,
    websocket_url: &str,
    account_id: &str,
    app_server_client_name: Option<&str>,
) -> anyhow::Result<Option<RemoteControlEnrollmentRecord>> {
    antfly
        .get_as(key(websocket_url, account_id, app_server_client_name))
        .await
        .map_err(internal)
}

pub(crate) async fn upsert_remote_control_enrollment(
    antfly: &Arc<Antfly>,
    enrollment: &RemoteControlEnrollmentRecord,
) -> anyhow::Result<()> {
    let key = key(
        &enrollment.websocket_url,
        &enrollment.account_id,
        enrollment.app_server_client_name.as_deref(),
    );
    let _guard = antfly.lock().await;
    // The SQLite `ON CONFLICT` clause updates every column except
    // `remote_control_enabled`, which an existing row keeps untouched.
    let existing_enabled = antfly
        .get_as::<RemoteControlEnrollmentRecord>(key.clone())
        .await
        .map_err(internal)?
        .map(|existing| existing.remote_control_enabled);
    let mut record = enrollment.clone();
    if let Some(preserved) = existing_enabled {
        record.remote_control_enabled = preserved;
    }
    antfly
        .write(vec![Write::put(key, serde_json::to_value(&record)?)])
        .await
        .map_err(internal)
}

pub(crate) async fn set_remote_control_enabled(
    antfly: &Arc<Antfly>,
    websocket_url: &str,
    account_id: &str,
    app_server_client_name: Option<&str>,
    remote_control_enabled: bool,
) -> anyhow::Result<u64> {
    let key = key(websocket_url, account_id, app_server_client_name);
    let _guard = antfly.lock().await;
    let Some(mut record) = antfly
        .get_as::<RemoteControlEnrollmentRecord>(key.clone())
        .await
        .map_err(internal)?
    else {
        return Ok(0);
    };
    record.remote_control_enabled = Some(remote_control_enabled);
    antfly
        .write(vec![Write::put(key, serde_json::to_value(&record)?)])
        .await
        .map_err(internal)?;
    Ok(1)
}

pub(crate) async fn delete_remote_control_enrollment(
    antfly: &Arc<Antfly>,
    websocket_url: &str,
    account_id: &str,
    app_server_client_name: Option<&str>,
) -> anyhow::Result<u64> {
    let key = key(websocket_url, account_id, app_server_client_name);
    let _guard = antfly.lock().await;
    if antfly.get(key.clone()).await.map_err(internal)?.is_none() {
        return Ok(0);
    }
    antfly
        .write(vec![Write::delete(key)])
        .await
        .map_err(internal)?;
    Ok(1)
}
