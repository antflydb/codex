//! Exercises the remote backend against a running Antfly server. Set
//! `ANTFLY_TEST_URL` (for example `http://127.0.0.1:8080`) to run, and
//! `ANTFLY_TEST_SQL_URL` (its PostgreSQL listener) for SQL tables, and
//! `ANTFLY_TEST_API_KEY` when the server requires authentication.

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
            // Bearer token (an API key's `encoded` value) for servers
            // started with `--auth true`.
            api_key_env: std::env::var_os("ANTFLY_TEST_API_KEY")
                .map(|_| "ANTFLY_TEST_API_KEY".to_string()),
        },
        models_dir: None,
        embedder: Some(EmbedderConfig::default()),
        decide_model: codex_antfly::DEFAULT_DECIDE_MODEL.to_string(),
        approvals: Default::default(),
        approvals_model: None,
        knowledge: Some(Default::default()),
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

/// Concept curation against a live server: entity merges and unmerges with
/// a distinct pin, cascade suppression with provenance, and the proposal
/// queue. Keys carry the process id, so reruns on one server do not collide.
#[tokio::test]
async fn remote_concept_curation() -> Result<(), Box<dyn std::error::Error>> {
    let Some(antfly) = remote() else {
        eprintln!("skipping: ANTFLY_TEST_URL is not set");
        return Ok(());
    };
    let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
    let run = std::process::id();
    let (agents, agent) = (
        format!("concept/agents_{run}"),
        format!("concept/agent_{run}"),
    );
    let (notes, note) = (
        format!("concept/notes_{run}"),
        format!("concept/note_{run}"),
    );
    let user = codex_antfly::knowledge::USER_ACTOR;

    assert!(knowledge.merge_concept(&agents, &agent).await?);
    let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
    assert_eq!(
        curation.redirects.get(&agents),
        Some(&agent),
        "{curation:?}"
    );
    assert!(knowledge.unmerge_concept(&agents, true).await?);
    let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
    assert!(!curation.redirects.contains_key(&agents), "{curation:?}");
    assert!(curation.is_distinct(&agent, &agents), "{curation:?}");

    assert!(knowledge.merge_concept(&notes, &note).await?);
    assert_eq!(
        knowledge
            .suppress_concept(&note, user, Some("generic"))
            .await?,
        Some(vec![notes.clone()])
    );
    let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
    assert!(curation.suppressed.contains(&note), "{curation:?}");
    assert!(curation.suppressed.contains(&notes), "{curation:?}");
    assert_eq!(
        curation.suppression_origins[&notes].cascade_from.as_deref(),
        Some(note.as_str())
    );
    assert!(knowledge.unsuppress_concept(&note, user).await?);

    // Proposals: a merge proposal, rejected, pins the pair distinct.
    let (a, b) = (
        format!("concept/kg_{run}"),
        format!("concept/knowledge_graph_{run}"),
    );
    assert!(
        knowledge
            .propose_merge(&a, &b, "unsure", json!({"a": "KG", "b": "knowledge graph"}))
            .await?
    );
    let open = knowledge.open_proposals().await?;
    let proposal = open
        .iter()
        .find(|proposal| proposal.key == a)
        .ok_or("proposal not listed")?;
    assert_eq!(proposal.into.as_deref(), Some(b.as_str()));
    knowledge.decide_proposal(proposal, false, user).await?;
    assert!(
        !knowledge
            .open_proposals()
            .await?
            .iter()
            .any(|proposal| proposal.key == a)
    );
    let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
    assert!(curation.is_distinct(&a, &b), "{curation:?}");
    antfly.close().await?;
    Ok(())
}

/// History concepts against a live server: a thread's user and agent
/// messages are grouped per turn, GLiNER extracts concepts, and the thread
/// node gets `about` edges to them; tool output is left out.
#[tokio::test]
async fn remote_history_concepts() -> Result<(), Box<dyn std::error::Error>> {
    let Some(antfly) = remote() else {
        eprintln!("skipping: ANTFLY_TEST_URL is not set");
        return Ok(());
    };
    let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
    let thread = format!("history-{}", std::process::id());
    let rows = [
        (
            1,
            "turn-1",
            Some("user"),
            "Switch the embedder to EmbeddingGemma in Antfly.",
        ),
        (
            2,
            "turn-1",
            Some("agent"),
            "I changed the Antfly config to use EmbeddingGemma.",
        ),
        (
            3,
            "turn-1",
            None,
            "cargo build output mentioning Kubernetes",
        ),
        (
            4,
            "turn-2",
            Some("user"),
            "Now flush the DNS cache on macOS.",
        ),
    ];
    antfly
        .documents(schema::HISTORY_ITEMS)
        .write(
            rows.iter()
                .map(|(ordinal, turn, speaker, text)| {
                    let mut doc = json!({"thread_id": thread, "ordinal": ordinal, "turn_id": turn});
                    // Like the thread store: only user and agent messages
                    // carry text and a speaker.
                    if let Some(speaker) = speaker {
                        doc["speaker"] = json!(speaker);
                        doc["search_text"] = json!(text);
                    } else {
                        doc["item"] = json!({"tool_output": text});
                    }
                    Write::put(format!("{thread}:{ordinal}"), doc)
                })
                .collect(),
        )
        .await?;
    let seeds = vec![
        "concept/embeddinggemma".to_string(),
        "concept/kubernetes".to_string(),
    ];
    let mut threads = Vec::new();
    for _ in 0..120 {
        threads = knowledge.threads_about(&seeds, 50).await;
        if threads.iter().any(|found| found.thread_id == thread) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let found = threads
        .iter()
        .find(|found| found.thread_id == thread)
        .ok_or_else(|| format!("thread not reached: {threads:?}"))?;
    assert_eq!(found.concepts, vec!["concept/embeddinggemma".to_string()]);

    // Retrieval reaches documents through incoming `about` edges too.
    let path = format!("{thread}.md");
    antfly
        .documents(schema::MEMORY_NOTES)
        .write(vec![Write::put(
            format!("memories:{path}"),
            json!({"namespace": "memories", "path": path, "updated_at_ms": 1,
                   "search_text": "Antfly embeds Codex threads with EmbeddingGemma."}),
        )])
        .await?;
    let mut related = None;
    for _ in 0..120 {
        let found = knowledge.related("EmbeddingGemma", 50, false).await?;
        if found.notes.iter().any(|note| note.path == path)
            && found.threads.iter().any(|found| found.thread_id == thread)
        {
            related = Some(found);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    related.ok_or("related() never reached the note and the thread")?;
    antfly.close().await?;
    Ok(())
}
