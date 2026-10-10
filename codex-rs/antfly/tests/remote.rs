//! Exercises the remote backend against a running Antfly server. Set
//! `ANTFLY_TEST_URL` (for example `http://127.0.0.1:8080`) to run, and
//! `ANTFLY_TEST_SQL_URL` (its PostgreSQL listener) for SQL tables.

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_antfly::BackendConfig;
use codex_antfly::EmbedderConfig;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_antfly::schema;
use pretty_assertions::assert_eq;
use serde_json::json;

fn remote() -> Option<Antfly> {
    let url = std::env::var("ANTFLY_TEST_URL").ok()?;
    Some(Antfly::new(AntflyConfig {
        backend: BackendConfig::Remote {
            url,
            sql_url: std::env::var("ANTFLY_TEST_SQL_URL").ok(),
            api_key_env: None,
        },
        models_dir: None,
        embedder: Some(EmbedderConfig::default()),
        decide_model: codex_antfly::DEFAULT_DECIDE_MODEL.to_string(),
        approvals: Default::default(),
    }))
}

#[tokio::test]
async fn remote_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let Some(antfly) = remote() else {
        eprintln!("skipping: ANTFLY_TEST_URL is not set");
        return Ok(());
    };
    // Relational tables over the PostgreSQL listener, then document tables
    // over HTTP (creating the document tables needs the SQL pool first).
    let sql = antfly.sql().await?;
    let run = format!("remote-{}", std::process::id());
    sql.execute(
        "INSERT INTO codex_thread_sections (id, name) VALUES ($1, $2)",
        codex_antfly::sql_params![run.clone(), "Remote"],
    )
    .await?;
    let section = sql
        .fetch_optional(
            "SELECT name FROM codex_thread_sections WHERE id = $1",
            codex_antfly::sql_params![run.clone()],
        )
        .await?
        .ok_or("missing section")?;
    assert_eq!(section.string("name")?, "Remote");

    let docs = antfly.documents(schema::HISTORY_ITEMS);
    let t1 = format!("{run}:t1");
    docs.write(vec![
        Write::put(
            format!("{t1}:2"),
            json!({"thread_id": t1, "ordinal": 2, "search_text": "raft snapshot races"}),
        ),
        Write::put(
            format!("{t1}:1"),
            json!({"thread_id": t1, "ordinal": 1, "search_text": "release notes"}),
        ),
    ])
    .await?;
    assert_eq!(
        docs.get(format!("{t1}:1"))
            .await?
            .and_then(|doc| doc["ordinal"].as_i64()),
        Some(1)
    );
    assert_eq!(docs.get("missing").await?, None);
    let ordinals: Vec<i64> = docs
        .scan(ScanRequest::prefix(&format!("{t1}:")))
        .await?
        .iter()
        .filter_map(|document| document.doc["ordinal"].as_i64())
        .collect();
    assert_eq!(ordinals, vec![1, 2]);

    let filter = json!({"term": {"thread_id": t1}});
    let mut keys = Vec::new();
    for _ in 0..50 {
        keys = docs
            .search_text("snapshot", Some(filter.clone()), 5, 0)
            .await?
            .into_iter()
            .map(|hit| hit.key)
            .collect();
        if !keys.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(keys, vec![format!("{t1}:2")]);

    docs.write(vec![Write::delete(format!("{t1}:2"))]).await?;
    assert_eq!(docs.get(format!("{t1}:2")).await?, None);
    sql.execute(
        "DELETE FROM codex_thread_sections WHERE id = $1",
        codex_antfly::sql_params![run],
    )
    .await?;
    antfly.close().await?;
    Ok(())
}
