//! Exercises the embedded backend against a real `libantfly`.

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_antfly::keys;
use codex_antfly::schema;
use pretty_assertions::assert_eq;
use serde_json::json;

fn runtime() -> tokio::runtime::Runtime {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => panic!("tokio runtime: {err}"),
    }
}

fn open(dir: &tempfile::TempDir, embedder: bool) -> Antfly {
    let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
    if !embedder {
        config.embedder = None;
    }
    match Antfly::open(config) {
        Ok(antfly) => antfly,
        Err(err) => panic!("open antfly: {err}"),
    }
}

#[test]
fn write_get_scan_delete() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let docs = antfly.documents(schema::HISTORY_ITEMS);
        let writes = (0..5)
            .map(|i| {
                Write::put(
                    keys::join(&["item", "t1", &keys::ordinal(i)]),
                    json!({"n": i}),
                )
            })
            .chain([Write::put("item:t2:00000000000000000000", json!({"n": 99}))])
            .collect();
        docs.write(writes).await?;

        assert_eq!(
            docs.get("item:t1:00000000000000000003").await?,
            Some(json!({"n": 3}))
        );
        assert_eq!(docs.get("missing").await?, None);

        let found = docs.scan(ScanRequest::prefix("item:t1:")).await?;
        let ns: Vec<i64> = found.iter().filter_map(|d| d.doc["n"].as_i64()).collect();
        assert_eq!(ns, vec![0, 1, 2, 3, 4]);

        let limited = docs
            .scan(ScanRequest::prefix("item:t1:").with_limit(2))
            .await?;
        assert_eq!(limited.len(), 2);

        docs.write(vec![
            Write::delete("item:t1:00000000000000000000"),
            Write::put("item:t1:00000000000000000001", json!({"n": 10})),
        ])
        .await?;
        let found = docs.scan(ScanRequest::prefix("item:t1:")).await?;
        let ns: Vec<i64> = found.iter().filter_map(|d| d.doc["n"].as_i64()).collect();
        assert_eq!(ns, vec![10, 2, 3, 4]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn descending_keys_list_newest_first() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let docs = antfly.documents(schema::HISTORY_ITEMS);
        let writes = [100i64, 300, 200]
            .iter()
            .map(|ts| {
                Write::put(
                    keys::join(&["recency", &keys::descending(*ts)]),
                    json!({"ts": ts}),
                )
            })
            .collect();
        docs.write(writes).await?;
        let found = docs.scan(ScanRequest::prefix("recency:")).await?;
        let ts: Vec<i64> = found.iter().filter_map(|d| d.doc["ts"].as_i64()).collect();
        assert_eq!(ts, vec![300, 200, 100]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn full_text_search_filtered_by_declared_field() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let docs = antfly.documents(schema::HISTORY_ITEMS);
        docs.write(vec![
            Write::put(
                "a",
                json!({"thread_id": "t1", "search_text": "fix the flaky raft snapshot test"}),
            ),
            Write::put(
                "b",
                json!({"thread_id": "t1", "search_text": "write release notes for the cli"}),
            ),
            Write::put(
                "c",
                json!({"thread_id": "t2", "search_text": "raft snapshot in another thread"}),
            ),
        ])
        .await?;
        let filter = json!({"term": {"thread_id": "t1"}});
        let mut hits = Vec::new();
        // Indexing is asynchronous; poll briefly.
        for _ in 0..50 {
            hits = docs
                .search_text("raft snapshot", Some(filter.clone()), 10, 0)
                .await?;
            if !hits.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let keys: Vec<&str> = hits.iter().map(|hit| hit.key.as_str()).collect();
        assert_eq!(keys, vec!["a"]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn decide_with_local_model() -> Result<(), Box<dyn std::error::Error>> {
    let Some(home) = std::env::var_os("HOME") else {
        return Ok(());
    };
    let models = std::path::Path::new(&home).join(".antfly/inference/models");
    if !models.join("convaiinnovations/laya").exists() {
        eprintln!("skipping: no laya checkpoint under {models:?}");
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let answer = antfly
            .decide(&json!({
                "model": "convaiinnovations/laya",
                "input": "command: rm -rf /",
                "questions": [{
                    "name": "destructive",
                    "type": "predicate",
                    "instructions": "The command permanently deletes data."
                }]
            }))
            .await?;
        let probability = answer["answers"][0]["probability"].as_f64().unwrap_or(-1.0);
        assert!((0.0..=1.0).contains(&probability), "answer: {answer}");
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn semantic_search_with_local_embedder() -> Result<(), Box<dyn std::error::Error>> {
    let Some(home) = std::env::var_os("HOME") else {
        return Ok(());
    };
    let models = std::path::Path::new(&home).join(".antfly/inference/models/BAAI");
    if !models.exists() {
        eprintln!("skipping: no BAAI embedder under {models:?}");
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, true);
        let docs = antfly.documents(schema::HISTORY_ITEMS);
        docs.write(vec![
            Write::put(
                "a",
                json!({"search_text": "fix the flaky raft snapshot test"}),
            ),
            Write::put(
                "b",
                json!({"search_text": "write release notes for the cli"}),
            ),
        ])
        .await?;
        let mut hits = Vec::new();
        for _ in 0..100 {
            // No lexical overlap: only the semantic leg can match.
            hits = docs
                .search_text("consensus log compaction", None, 10, 1)
                .await?;
            if !hits.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let keys: Vec<&str> = hits.iter().map(|hit| hit.key.as_str()).collect();
        assert_eq!(keys, vec!["a"]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
