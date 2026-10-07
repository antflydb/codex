//! Exercises the remote backend against a running Antfly server. Set
//! `ANTFLY_TEST_URL` (for example `http://127.0.0.1:8080`) to run.

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_antfly::BackendConfig;
use codex_antfly::EmbedderConfig;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use pretty_assertions::assert_eq;
use serde_json::json;

fn remote() -> Option<Antfly> {
    let url = std::env::var("ANTFLY_TEST_URL").ok()?;
    let table = format!("codex_test_{}", std::process::id());
    Some(Antfly::new(AntflyConfig {
        backend: BackendConfig::Remote {
            url,
            table,
            api_key_env: None,
        },
        models_dir: None,
        embedder: Some(EmbedderConfig::default()),
        decide_model: "laya".to_string(),
        approvals: Default::default(),
    }))
}

#[tokio::test]
async fn remote_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let Some(antfly) = remote() else {
        eprintln!("skipping: ANTFLY_TEST_URL is not set");
        return Ok(());
    };
    antfly
        .write(vec![
            Write::put(
                "item:t1:2",
                json!({"n": 2, "search_text": "raft snapshot races"}),
            ),
            Write::put("item:t1:1", json!({"n": 1, "search_text": "release notes"})),
            Write::put("item:t2:1", json!({"n": 9})),
        ])
        .await?;
    assert_eq!(
        antfly.get("item:t1:1").await?,
        Some(json!({"n": 1, "search_text": "release notes"}))
    );
    assert_eq!(antfly.get("missing").await?, None);
    let ns: Vec<i64> = antfly
        .scan(ScanRequest::prefix("item:t1:"))
        .await?
        .iter()
        .filter_map(|document| document.doc["n"].as_i64())
        .collect();
    assert_eq!(ns, vec![1, 2]);

    let mut keys = Vec::new();
    for _ in 0..50 {
        keys = antfly
            .search_full_text("item:t1:", "snapshot", 5)
            .await?
            .into_iter()
            .map(|hit| hit.key)
            .collect();
        if !keys.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(keys, vec!["item:t1:2".to_string()]);

    antfly.write(vec![Write::delete("item:t1:2")]).await?;
    assert_eq!(antfly.get("item:t1:2").await?, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicated_writes_reach_remote() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    use codex_antfly::Backend;
    use codex_antfly::EmbeddedBackend;
    use codex_antfly::RemoteBackend;
    use codex_antfly::ReplicatedBackend;

    let Ok(url) = std::env::var("ANTFLY_TEST_URL") else {
        eprintln!("skipping: ANTFLY_TEST_URL is not set");
        return Ok(());
    };
    let dir = tempfile::tempdir()?;
    let table = format!("codex_replica_{}", std::process::id());
    let local: Arc<dyn Backend> = Arc::new(EmbeddedBackend::open(&dir.path().join("l.aflite"))?);
    let remote_backend: Arc<dyn Backend> = Arc::new(RemoteBackend::new(&url, &table, None)?);
    // An unreachable remote: writes still succeed locally and queue up.
    let offline: Arc<dyn Backend> =
        Arc::new(RemoteBackend::new("http://127.0.0.1:9", &table, None)?);
    let replicated = ReplicatedBackend::new(Arc::clone(&local), offline, false);
    replicated
        .write(vec![Write::put("doc:1", json!({"n": 1}))])
        .await?;
    assert_eq!(
        replicated.get("doc:1".to_string()).await?,
        Some(json!({"n": 1}))
    );
    assert!(
        replicated.flush().await.is_err(),
        "offline remote must not drain"
    );
    drop(replicated);

    // Reconnect to the real remote; the queued write and a new one drain in order.
    let replicated = ReplicatedBackend::new(Arc::clone(&local), Arc::clone(&remote_backend), false);
    replicated
        .write(vec![
            Write::put("doc:2", json!({"n": 2})),
            Write::delete("doc:1"),
        ])
        .await?;
    replicated.flush().await?;
    assert_eq!(remote_backend.get("doc:1".to_string()).await?, None);
    assert_eq!(
        remote_backend.get("doc:2".to_string()).await?,
        Some(json!({"n": 2}))
    );
    assert!(
        local.scan(ScanRequest::prefix("ob:")).await?.is_empty(),
        "outbox drained"
    );
    drop(replicated);
    drop(local);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    Ok(())
}
