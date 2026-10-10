//! Exercises `schema::APPROVALS` against a real embedded `libantfly`: the
//! write-record / search-precedent / update-outcome mechanics
//! `src/reviewer.rs` and `src/outcomes.rs` rely on, without needing the full
//! `ThreadManager` plumbing around them.

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_antfly::Write;
use codex_antfly::schema;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

fn runtime() -> tokio::runtime::Runtime {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => panic!("tokio runtime: {err}"),
    }
}

#[test]
fn approvals_round_trip_by_id_and_record_outcome_updates() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
        config.embedder = None;
        let antfly = Antfly::open(config)?;
        let approvals = antfly.documents(schema::APPROVALS);

        let approval_id = "approval-1";
        let doc = json!({
            "thread_id": "thread-1",
            "approval_id": approval_id,
            "tool_call_id": "call-1",
            "category": "Shell",
            "search_text": "User request: fix the flaky test\nProposed action: rm -rf target/debug",
            "verdict": "Defer",
            "applied": false,
            "outcome": "pending",
            "decided_at_ms": 1_700_000_000_000i64,
        });
        approvals
            .write(vec![Write::put(approval_id, doc.clone())])
            .await?;

        // Document id is the approval id, so it round-trips by key.
        let fetched = approvals.get(approval_id).await?.expect("approval stored");
        assert_eq!(
            fetched.get("outcome"),
            Some(&Value::String("pending".to_string()))
        );

        // Recording an outcome overwrites the same document, in place.
        let mut updated = doc.clone();
        updated["outcome"] = Value::String("executed".to_string());
        approvals
            .write(vec![Write::put(approval_id, updated)])
            .await?;
        let after = approvals
            .get(approval_id)
            .await?
            .expect("approval still stored");
        assert_eq!(
            after.get("outcome"),
            Some(&Value::String("executed".to_string()))
        );

        // Precedent search finds it by the recorded `search_text`.
        let mut hits = Vec::new();
        for _ in 0..50 {
            hits = approvals.search_text("rm -rf target", None, 3, 3).await?;
            if !hits.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(hits.len(), 1, "expected one precedent hit, got {hits:?}");
        assert_eq!(hits[0].key, approval_id);

        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn approvals_search_text_can_filter_by_declared_fields() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
        config.embedder = None;
        let antfly = Antfly::open(config)?;
        let approvals = antfly.documents(schema::APPROVALS);

        approvals
            .write(vec![
                Write::put(
                    "a1",
                    json!({
                        "thread_id": "thread-a",
                        "approval_id": "a1",
                        "category": "Shell",
                        "search_text": "delete the build cache",
                        "verdict": "Allow",
                        "applied": true,
                        "outcome": "executed",
                        "decided_at_ms": 1,
                    }),
                ),
                Write::put(
                    "a2",
                    json!({
                        "thread_id": "thread-b",
                        "approval_id": "a2",
                        "category": "Shell",
                        "search_text": "delete the build cache",
                        "verdict": "Allow",
                        "applied": true,
                        "outcome": "executed",
                        "decided_at_ms": 2,
                    }),
                ),
            ])
            .await?;

        let mut hits = Vec::new();
        for _ in 0..50 {
            hits = approvals
                .search_text(
                    "delete the build cache",
                    Some(json!({"term": {"thread_id": "thread-b"}})),
                    5,
                    5,
                )
                .await?;
            if !hits.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            hits.len(),
            1,
            "expected only thread-b's approval, got {hits:?}"
        );
        assert_eq!(hits[0].key, "a2");

        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
