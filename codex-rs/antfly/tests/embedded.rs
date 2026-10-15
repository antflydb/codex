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

/// Whether `<models>/<owner>` holds an installed model, so tests needing
/// real inference skip on machines without it.
fn has_models(owner: &str) -> bool {
    let Some(home) = std::env::var_os("HOME") else {
        return false;
    };
    let models = std::path::Path::new(&home)
        .join(".antfly/inference/models")
        .join(owner);
    if !models.exists() {
        eprintln!("skipping: no {owner} embedder under {models:?}");
        return false;
    }
    true
}

async fn write_semantic_fixture(antfly: &Antfly) -> Result<(), codex_antfly::AntflyError> {
    antfly
        .documents(schema::HISTORY_ITEMS)
        .write(vec![
            Write::put(
                "a",
                json!({"search_text": "fix the flaky raft snapshot test"}),
            ),
            Write::put(
                "b",
                json!({"search_text": "write release notes for the cli"}),
            ),
        ])
        .await
}

/// Polls until the semantic leg finds a document; embedding is asynchronous.
async fn semantic_keys(antfly: &Antfly) -> Result<Vec<String>, codex_antfly::AntflyError> {
    let docs = antfly.documents(schema::HISTORY_ITEMS);
    let mut hits = Vec::new();
    for _ in 0..150 {
        // No lexical overlap: only the semantic leg can match.
        hits = docs
            .search_text("consensus log compaction", None, 10, 1)
            .await?;
        if !hits.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    Ok(hits.into_iter().map(|hit| hit.key).collect())
}

#[test]
fn semantic_search_with_local_embedder() -> Result<(), Box<dyn std::error::Error>> {
    if !has_models("google") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, true);
        write_semantic_fixture(&antfly).await?;
        assert_eq!(semantic_keys(&antfly).await?, vec!["a"]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn switching_embedders_reindexes_existing_documents() -> Result<(), Box<dyn std::error::Error>> {
    if !has_models("BAAI") || !has_models("google") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
        config.embedder = Some(codex_antfly::EmbedderConfig {
            model: "BAAI/bge-small-en-v1.5".to_string(),
            dims: None,
        });
        let antfly = Antfly::open(config)?;
        write_semantic_fixture(&antfly).await?;
        assert_eq!(semantic_keys(&antfly).await?, vec!["a"]);
        antfly.close().await?;

        // The default (EmbeddingGemma 2) embedder gets its own index, filled from the
        // documents already stored, and the bge index is dropped.
        let antfly = open(&dir, true);
        assert_eq!(semantic_keys(&antfly).await?, vec!["a"]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn knowledge_links_claims_and_notes_through_concepts() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::knowledge::ClaimKind;
    use codex_antfly::knowledge::TurnClaim;
    use codex_antfly::knowledge::TurnClaimBatch;
    if !has_models("fastino") || !has_models("convaiinnovations") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let claim = |kind: ClaimKind, statement: &str| TurnClaim {
            kind,
            statement: statement.to_string(),
            turn_id: Some("turn-1".to_string()),
            turn_ordinal: 0,
            replaces: Vec::new(),
        };
        let batch = |thread: &str, at: i64, claims: Vec<TurnClaim>| TurnClaimBatch {
            thread_id: thread.to_string(),
            cwd: None,
            source_updated_at_ms: at,
            claims,
            retracts: Vec::new(),
        };
        knowledge
            .append_turn_claims(batch(
                "thread-a",
                1,
                vec![
                    claim(
                        ClaimKind::Decision,
                        "Use Qwen3 Embedding as the embedder for Codex semantic search in Antfly.",
                    ),
                    claim(
                        ClaimKind::Preference,
                        "Pin Antfly by git revision rather than tracking a branch.",
                    ),
                ],
            ))
            .await?;
        knowledge.settle_thread("thread-a").await?;
        let a_decision = format!("turns:thread-a:{}:000", codex_antfly::keys::ordinal(0));
        // Supersession compares against claims that are already searchable;
        // indexing is asynchronous, so wait for thread A's decision to land.
        for _ in 0..100 {
            let hits = antfly
                .documents(schema::KNOWLEDGE)
                .search_text("Qwen3 Embedding embedder", None, 5, 0)
                .await?;
            if hits.iter().any(|hit| hit.key == a_decision) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let later = knowledge
            .append_turn_claims(batch(
                "thread-b",
                2,
                vec![claim(
                    ClaimKind::Decision,
                    "Use EmbeddingGemma 2 instead of Qwen3 Embedding as the Codex semantic search embedder; Antfly detects the dimensions.",
                )],
            ))
            .await?;
        // A provisional claim never supersedes another thread's claim.
        assert!(later[0].supersedes.is_empty());
        assert!(!later[0].settled);
        knowledge.settle_thread("thread-b").await?;
        let b_decision = later[0].id.clone();
        let later = knowledge
            .get(&b_decision)
            .await?
            .ok_or("thread B's decision missing")?;
        assert!(later.settled);
        assert_eq!(later.supersedes, vec![a_decision.clone()]);
        antfly
            .documents(schema::MEMORY_NOTES)
            .write(vec![Write::put(
                "memories:notes/embedder.md",
                json!({"namespace": "memories", "path": "notes/embedder.md", "updated_at_ms": 3,
                       "search_text": "Codex semantic search embeds with EmbeddingGemma 2 through Antfly; approvals use Laya."}),
            )])
            .await?;
        let mut related = Default::default();
        for _ in 0..150 {
            related = knowledge.related("Which embedder does Codex semantic search use?", 5, false).await?;
            if !related.notes.is_empty() && related.claims.iter().any(|c| !c.concepts.is_empty()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        // Only the current decision is surfaced, linked through shared concepts.
        let ids: Vec<&str> = related.claims.iter().map(|c| c.claim.id.as_str()).collect();
        assert!(ids.contains(&b_decision.as_str()), "{related:#?}");
        assert!(!ids.contains(&a_decision.as_str()), "{related:#?}");
        let current = &related.claims[0];
        assert!(current.concepts.len() > 1, "{related:#?}");
        assert_eq!(
            related.notes.iter().map(|n| n.path.as_str()).collect::<Vec<_>>(),
            vec!["notes/embedder.md"]
        );
        // Query seeds come from Antfly's own resolver, not local slugging.
        let resolved = antfly
            .documents(schema::KNOWLEDGE)
            .resolve_query(
                codex_antfly::knowledge::CONCEPT_RESOLVER,
                json!({"text": "Which embedder does Codex semantic search use?"}),
            )
            .await?
            .ok_or("embedded backend resolves queries")?;
        let keys: Vec<&str> = resolved["resolution"]["entities"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entity| entity["doc_ref"]["key"].as_str())
            .collect();
        assert!(
            !keys.is_empty() && keys.iter().all(|key| key.starts_with("concept/")),
            "{resolved:#}"
        );
        assert!(
            related.seeds.iter().any(|seed| keys.contains(&seed.as_str())),
            "{related:#?} vs {keys:?}"
        );
        // Ranking runs on seeded PageRank in both directions: mass seeded
        // at the claim's concepts must flow back to the claim itself.
        let ppr = antfly
            .documents(schema::KNOWLEDGE)
            .query(json!({
                "graph_metric": {
                    "index": codex_antfly::knowledge::concept_graph_name(
                        codex_antfly::DEFAULT_EXTRACT_MODEL,
                    ),
                    "metric": codex_antfly::knowledge::PPR_METRIC,
                    "metric_freshness": "fresh",
                    "seed_nodes": &current.concepts,
                    "direction": "both",
                    "top_k": 20,
                },
                "limit": 1,
            }))
            .await?;
        let scores = ppr["responses"][0]["graph_metric_results"]
            [codex_antfly::knowledge::PPR_METRIC]["scores"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert!(
            scores.iter().any(|entry| entry["node"] == b_decision.as_str()
                && entry["score"].as_f64().unwrap_or_default() > 0.0),
            "{ppr:#}"
        );
        // History keeps the superseded decision, pointing at its replacement.
        let with_history = knowledge
            .related("Which embedder does Codex semantic search use?", 5, true)
            .await?;
        let earlier = with_history
            .claims
            .iter()
            .find(|c| c.claim.id == a_decision)
            .ok_or("superseded claim missing from history")?;
        assert_eq!(earlier.claim.status, codex_antfly::knowledge::ClaimStatus::Superseded);
        assert_eq!(earlier.claim.superseded_by.as_deref(), Some(b_decision.as_str()));
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn graph_upgrades_replace_the_old_version_without_a_gap() -> Result<(), Box<dyn std::error::Error>>
{
    use codex_antfly::Backend;
    use codex_antfly::EmbeddedBackend;
    use codex_antfly::GraphIndex;
    use codex_antfly::TableSpec;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("codex.aflite");
    let spec = |name: &str| TableSpec {
        name: "graphs".to_string(),
        schema: json!({
            "default_type": "doc",
            "document_schemas": {"doc": {"schema": {"type": "object", "additionalProperties": true}}},
        }),
        dense: None,
        graphs: vec![GraphIndex {
            name: name.to_string(),
            config: json!({"edge_types": [{"name": "links", "field": "links"}]}),
            replaces_prefix: "links_graph_",
        }],
    };
    let traverse = |index: &str| {
        json!({
            "graph_queries": {"q": {"index": index, "traverse": {
                "start": {"keys": ["a"]}, "direction": "out", "edge_types": ["links"],
                "max_depth": 1, "limit": 10}}},
            "limit": 1,
        })
    };
    runtime().block_on(async {
        let backend = EmbeddedBackend::open(&path, None)?;
        backend.ensure_table(spec("links_graph_v1")).await?;
        backend
            .write(
                "graphs".to_string(),
                vec![
                    Write::put("a", json!({"links": ["b"]})),
                    Write::put("b", json!({})),
                ],
            )
            .await?;
        backend.close().await?;

        // A new version on reopen replaces v1; v1 serves until the swap.
        let backend = EmbeddedBackend::open(&path, None)?;
        backend.ensure_table(spec("links_graph_v2")).await?;
        let mut serving = String::new();
        for _ in 0..200 {
            serving = backend
                .serving_graph("graphs".to_string(), "links_graph_v2".to_string())
                .await?;
            assert!(
                serving == "links_graph_v1" || serving == "links_graph_v2",
                "{serving}"
            );
            let hits = backend
                .query("graphs".to_string(), traverse(&serving))
                .await?;
            assert!(hits.to_string().contains("\"b\""), "{serving}: {hits}");
            if serving == "links_graph_v2" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(serving, "links_graph_v2");
        // Reopening with the same version is a no-op and v1 is gone.
        backend.close().await?;
        let backend = EmbeddedBackend::open(&path, None)?;
        backend.ensure_table(spec("links_graph_v2")).await?;
        let old = backend
            .query("graphs".to_string(), traverse("links_graph_v1"))
            .await;
        assert!(old.is_err() || !old?.to_string().contains("\"b\""));
        backend.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn turn_claims_replace_retract_and_track_progress() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::claim_progress;
    use codex_antfly::knowledge::ClaimKind;
    use codex_antfly::knowledge::ClaimStatus;
    use codex_antfly::knowledge::TurnClaim;
    use codex_antfly::knowledge::TurnClaimBatch;
    if !has_models("fastino") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let claim = |ordinal: u64, statement: &str, replaces: Vec<String>| TurnClaim {
            kind: ClaimKind::Decision,
            statement: statement.to_string(),
            turn_id: Some(format!("turn-{ordinal}")),
            turn_ordinal: ordinal,
            replaces,
        };
        let batch = |claims: Vec<TurnClaim>, retracts: Vec<String>| TurnClaimBatch {
            thread_id: "thread-a".to_string(),
            cwd: Some("/repo".to_string()),
            source_updated_at_ms: 10,
            claims,
            retracts,
        };
        let first = knowledge
            .append_turn_claims(batch(
                vec![
                    claim(2, "The importer writes 256 items per batch.", Vec::new()),
                    claim(2, "Imports run on a debug build.", Vec::new()),
                ],
                Vec::new(),
            ))
            .await?;
        assert_eq!(first[0].turn_id.as_deref(), Some("turn-2"));
        assert_eq!(first[0].turn_ordinal, Some(2));
        assert!(!first[0].settled);
        // A later turn replaces one claim and retracts the other; ids from
        // other threads are ignored.
        let second = knowledge
            .append_turn_claims(batch(
                vec![claim(
                    9,
                    "The importer writes 1,024 items per batch.",
                    vec![first[0].id.clone(), "turns:thread-b:x:000".to_string()],
                )],
                vec![first[1].id.clone()],
            ))
            .await?;
        assert_eq!(second[0].supersedes, vec![first[0].id.clone()]);
        let claims = knowledge.thread_claims("thread-a").await?;
        let status = |id: &str| {
            claims
                .iter()
                .find(|claim| claim.id == id)
                .map(|claim| (claim.status, claim.superseded_by.clone()))
        };
        assert_eq!(
            status(&first[0].id),
            Some((ClaimStatus::Superseded, Some(second[0].id.clone())))
        );
        assert_eq!(status(&first[1].id), Some((ClaimStatus::Retracted, None)));
        assert_eq!(status(&second[0].id), Some((ClaimStatus::Current, None)));
        assert_eq!(knowledge.settle_thread("thread-a").await?, 3);
        assert!(
            knowledge
                .thread_claims("thread-a")
                .await?
                .iter()
                .all(|claim| claim.settled)
        );

        // Progress: one owner at a time, advanced and settled under the lease.
        let lease = claim_progress::lease(&antfly, "thread-a", "owner-1", 60_000)
            .await?
            .ok_or("first lease")?;
        assert_eq!(lease.next_ordinal, 0);
        assert!(
            claim_progress::lease(&antfly, "thread-a", "owner-2", 60_000)
                .await?
                .is_none()
        );
        claim_progress::advance(&antfly, "thread-a", "owner-1", 12).await?;
        claim_progress::settle(&antfly, "thread-a", "owner-1", 12).await?;
        assert!(
            claim_progress::advance(&antfly, "thread-a", "owner-2", 99)
                .await
                .is_err()
        );
        claim_progress::release(&antfly, "thread-a", "owner-1", Some(("boom", i64::MAX))).await?;
        // Backing off: nobody may lease until the retry time.
        assert!(
            claim_progress::lease(&antfly, "thread-a", "owner-2", 60_000)
                .await?
                .is_none()
        );

        // The sweep finds threads with unextracted history.
        let sql = antfly.sql().await?;
        for (id, next) in [("thread-new", 5_i64), ("thread-a", 12)] {
            sql.execute(
                "INSERT INTO codex_threads (id, rollout_path, created_at, updated_at, created_at_ms, \
                 updated_at_ms, recency_at, recency_at_ms, source, model_provider, cwd, title, \
                 sandbox_policy, approval_mode, next_ordinal) \
                 VALUES ($1, '', 1, 1, 1000, 1000, 1, 1000, 'cli', 'p', '/repo', 't', 's', 'a', $2)",
                codex_antfly::sql_params![id, next],
            )
            .await?;
        }
        let candidates = claim_progress::sweep_candidates(
            &antfly,
            claim_progress::SweepParams {
                sources: &["cli", "vscode"],
                min_updated_at_ms: 0,
                settle_before_ms: 2_000,
                now_ms: 3_000,
                limit: 10,
            },
        )
        .await?;
        // thread-a is extracted, settled and backing off.
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| (candidate.thread_id.as_str(), candidate.history_end, candidate.progress))
                .collect::<Vec<_>>(),
            vec![("thread-new", 5, None)]
        );
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[test]
fn supersession_collapses_restatements_but_keeps_distinct_facts()
-> Result<(), Box<dyn std::error::Error>> {
    if !has_models("convaiinnovations") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let cases = [
            (
                "The Codex Antfly importer writes 1,024 history items per import batch.",
                "The Codex Antfly importer writes 1,024 history items per batch now that Antfly lifted its 2 MiB transaction limit.",
                true,
            ),
            (
                "Use Qwen3 Embedding as the embedder for Codex semantic search in Antfly.",
                "Use EmbeddingGemma 2 instead of Qwen3 Embedding as the Codex semantic search embedder.",
                true,
            ),
            (
                "Creating hello.txt failed because the elevated-permission request was declined.",
                "The requested elevated permission to create hello.txt was declined, so hello.txt was not created.",
                true,
            ),
            (
                "The Codex Antfly importer writes 1,024 history items per import batch.",
                "The Codex Antfly importer keeps threads of every age.",
                false,
            ),
            (
                "Pin Antfly by git revision rather than tracking a branch.",
                "Use EmbeddingGemma 2 as the Codex semantic search embedder.",
                false,
            ),
        ];
        let mut wrong = Vec::new();
        for (earlier, later, expected) in cases {
            if knowledge.supersedes(earlier, later).await? != expected {
                wrong.push((earlier, later, expected));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
