//! Exercises the embedded backend against a real `libantfly`.

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_antfly::keys;
use codex_antfly::schema;
use pretty_assertions::assert_eq;
use serde_json::json;

/// A test's runtime, holding a lock that runs these tests one at a time.
/// Many load local models (GLiNER, Laya, Gemma, embedders); several at
/// once can exceed inference memory admission and fail spuriously.
struct TestRuntime {
    runtime: tokio::runtime::Runtime,
    _serial: std::sync::MutexGuard<'static, ()>,
}

impl std::ops::Deref for TestRuntime {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

fn runtime() -> TestRuntime {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => TestRuntime {
            runtime,
            _serial: serial,
        },
        Err(err) => panic!("tokio runtime: {err}"),
    }
}

/// A supersession judge that replaces every candidate.
struct AlwaysSupersedes;

impl codex_antfly::knowledge::SupersessionJudge for AlwaysSupersedes {
    fn judge<'a>(
        &'a self,
        pairs: &'a [(String, String)],
    ) -> codex_antfly::BackendFuture<'a, Vec<Option<bool>>> {
        Box::pin(async move { Ok(vec![Some(true); pairs.len()]) })
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
            motivates: Vec::new(),
            rejected_in_favor_of: Vec::new(),
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
        knowledge.settle_thread("thread-a", None).await?;
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
        // The memory service proposes thread A's decision; the judge (here
        // one that always agrees) decides.
        knowledge.settle_thread("thread-b", Some(&AlwaysSupersedes)).await?;
        let b_decision = later[0].id.clone();
        let later = knowledge
            .get(&b_decision)
            .await?
            .ok_or("thread B's decision missing")?;
        assert!(later.settled);
        assert_eq!(later.supersedes, vec![a_decision.clone()]);
        // The memory service shadow logged its candidates for the decision
        // next to Codex's, and found the claim Codex superseded.
        let shadow = codex_antfly::labels::latest_responses(
            &antfly,
            codex_antfly::claim_memory::SHADOW_KIND,
        )
        .await?;
        let logged = shadow.get(&b_decision).ok_or("no shadow comparison")?;
        assert_eq!(logged["retrieval"], "indexed", "{logged}");
        assert_eq!(logged["codex_supersedes"], json!([a_decision]));
        assert!(
            logged["memory_candidates"]
                .as_array()
                .is_some_and(|ids| ids.contains(&json!(a_decision))),
            "{logged}"
        );
        let report = codex_antfly::claim_memory::report(&antfly).await?;
        assert_eq!((report.supersessions, report.found), (1, 1), "{report:?}");
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
            enrichments: Vec::new(),
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
        // A write receipt does not promise the graph has indexed it; what
        // the replacement must preserve is what v1 already serves.
        let mut indexed = false;
        for _ in 0..200 {
            let hits = backend
                .query("graphs".to_string(), traverse("links_graph_v1"))
                .await?;
            if hits.to_string().contains("\"b\"") {
                indexed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(indexed, "v1 never indexed a -> b");
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
        // Deterministically model a reader that selected v1 immediately
        // before activation. The managed adapter must retry against v2.
        let retired = backend
            .query("graphs".to_string(), traverse("links_graph_v1"))
            .await
            .map_err(|err| format!("query graph retired during replacement: {err}"))?;
        assert!(retired.to_string().contains("\"b\""), "{retired}");
        assert!(
            backend
                .query("graphs".to_string(), traverse("unmanaged_missing_graph"))
                .await
                .is_err()
        );
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
            motivates: Vec::new(),
            rejected_in_favor_of: Vec::new(),
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
        assert_eq!(knowledge.settle_thread("thread-a", None).await?, 3);
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

#[test]
fn decisions_carry_their_rationale_and_rejected_alternatives()
-> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::knowledge::ClaimKind;
    use codex_antfly::knowledge::ClaimRef;
    use codex_antfly::knowledge::TurnClaim;
    use codex_antfly::knowledge::TurnClaimBatch;
    if !has_models("fastino") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let claim = |kind: ClaimKind, statement: &str, motivates: Vec<ClaimRef>, rejected: Vec<ClaimRef>| TurnClaim {
            kind,
            statement: statement.to_string(),
            turn_id: Some("turn-1".to_string()),
            turn_ordinal: 4,
            replaces: Vec::new(),
            motivates,
            rejected_in_favor_of: rejected,
        };
        let recorded = knowledge
            .append_turn_claims(TurnClaimBatch {
                thread_id: "thread-links".to_string(),
                cwd: None,
                source_updated_at_ms: 1,
                claims: vec![
                    claim(ClaimKind::Decision, "The Codex Antfly importer writes 1,024 history items per batch.", Vec::new(), Vec::new()),
                    claim(ClaimKind::Rationale, "Larger Codex import batches are possible because Antfly lifted its 2 MiB transaction limit.", vec![ClaimRef::New(0)], Vec::new()),
                    claim(ClaimKind::Alternative, "Keeping 256-item Codex import batches was considered and rejected.", Vec::new(), vec![ClaimRef::New(0)]),
                    claim(ClaimKind::Lesson, "A link to itself or another thread is dropped.", vec![ClaimRef::New(3), ClaimRef::Existing("turns:other:x:000".to_string())], Vec::new()),
                ],
                retracts: Vec::new(),
            })
            .await?;
        let decision = recorded[0].id.clone();
        assert_eq!(recorded[1].motivates, vec![decision.clone()]);
        assert_eq!(recorded[2].rejected_in_favor_of, vec![decision.clone()]);
        assert!(recorded[3].motivates.is_empty());
        let mut found = None;
        for _ in 0..150 {
            let related = knowledge.related("How many history items per Codex import batch?", 5, false).await?;
            if let Some(hit) = related.claims.into_iter().find(|hit| hit.claim.id == decision)
                && !hit.motivated_by.is_empty()
                && !hit.alternatives.is_empty()
            {
                found = Some(hit);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let hit = found.ok_or("decision without its links")?;
        assert_eq!(hit.motivated_by.iter().map(|c| c.id.clone()).collect::<Vec<_>>(), vec![recorded[1].id.clone()]);
        assert_eq!(hit.alternatives.iter().map(|c| c.id.clone()).collect::<Vec<_>>(), vec![recorded[2].id.clone()]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Precision and recall of supersession and of the concept-merge gates on
/// `tests/fixtures/knowledge_eval`. Opt-in (needs models, prints a report):
/// `cargo test -p codex-antfly --test embedded knowledge_eval -- --ignored --nocapture`.
#[test]
#[ignore]
fn knowledge_eval() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::knowledge::CONCEPT_SIMILARITY_FLOOR;
    use codex_antfly::knowledge::concept_identity;
    use codex_antfly::knowledge::number_tokens;
    let fixtures =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/knowledge_eval");
    let read = |name: &str| -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        Ok(std::fs::read_to_string(fixtures.join(name))?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?)
    };
    let supersession = read("supersession.jsonl")?;
    let pairs = read("concept_pairs.jsonl")?;
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, true);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let (mut tp, mut fp, mut fn_) = (0_usize, 0_usize, 0_usize);
        for case in &supersession {
            let earlier = case["earlier"].as_str().unwrap_or_default();
            let later = case["later"].as_str().unwrap_or_default();
            let expected = case["replaces"].as_bool().unwrap_or_default();
            let got = knowledge.supersedes(earlier, later).await?;
            match (got, expected) {
                (true, true) => tp += 1,
                (true, false) => { fp += 1; println!("  false supersession: {earlier:?} -> {later:?}"); }
                (false, true) => { fn_ += 1; println!("  missed supersession: {earlier:?} -> {later:?}"); }
                (false, false) => {}
            }
        }
        println!("supersession: precision {:.2} recall {:.2} ({tp} tp, {fp} fp, {fn_} fn)",
            tp as f64 / (tp + fp).max(1) as f64, tp as f64 / (tp + fn_).max(1) as f64);

        let concepts = antfly.documents(schema::CONCEPTS);
        let mut names: Vec<String> = pairs.iter()
            .flat_map(|pair| [pair["a"].as_str().unwrap_or_default().to_string(), pair["b"].as_str().unwrap_or_default().to_string()])
            .collect();
        names.sort();
        names.dedup();
        concepts.write(names.iter().map(|name| Write::put(format!("concept/{name}"), json!({"canonical_name": name}))).collect()).await?;
        for _ in 0..200 {
            if concepts.nearest(&names[0], names.len()).await?.len() == names.len() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        // Gate outcome per pair: merged without a model, judged, or kept apart.
        let (mut lexical, mut judged_same, mut judged_different, mut apart_same) = (0, 0, 0, 0);
        for pair in &pairs {
            let a = pair["a"].as_str().unwrap_or_default();
            let b = pair["b"].as_str().unwrap_or_default();
            let same = pair["same"].as_bool().unwrap_or_default();
            if concept_identity(a) == concept_identity(b) {
                lexical += 1;
                if !same { println!("  false lexical merge: {a:?} ~ {b:?}"); }
                continue;
            }
            let hits = concepts.nearest(a, names.len()).await?;
            let score = |name: &str| hits.iter().find(|hit| hit.key == format!("concept/{name}")).map(|hit| hit.score);
            let similarity = score(b).zip(score(a)).map(|(pair, own)| pair / own).unwrap_or(0.0);
            let candidate = similarity >= CONCEPT_SIMILARITY_FLOOR && number_tokens(a) == number_tokens(b);
            match (candidate, same) {
                (true, true) => judged_same += 1,
                (true, false) => judged_different += 1,
                (false, true) => { apart_same += 1; println!("  synonym kept apart ({similarity:.2}): {a:?} ~ {b:?}"); }
                (false, false) => {}
            }
        }
        let synonyms = pairs.iter().filter(|pair| pair["same"].as_bool() == Some(true)).count();
        println!("concept gates: {lexical} lexical merges; {} pairs judged ({judged_same} synonyms, {judged_different} not); \
                  {apart_same} synonyms kept apart; synonym recall before judgment {:.2}",
            judged_same + judged_different,
            (lexical + judged_same) as f64 / synonyms.max(1) as f64);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Upgrading a graph whose inline resolver changes key template (`slug` to
/// `uslug`) carries curated merges over: the namespace is migrated first,
/// then the replacement transfers the resolver (antflydb/antfly#1144).
#[test]
fn graph_upgrade_moves_resolver_and_keeps_merges() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::GraphIndex;
    use codex_antfly::TableSpec;
    if !has_models("fastino") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let backend = antfly.backend().await?;
        let graph = |name: &str, template: &str| GraphIndex {
            name: name.to_string(),
            replaces_prefix: "kg_",
            enrichments: Vec::new(),
            config: json!({
                "source": {"artifact": format!("{name}_extraction"), "path": "$.relations[*]",
                           "format": "extraction_relation", "mention_edge_type": "about",
                           "edge": {"type": "{{ _item.type }}", "weight": "{{ _item.score }}"}},
                "artifact": {"name": format!("{name}_extraction"), "kind": "asset",
                             "source": {"type": "field", "value": "search_text"},
                             "content_type": "application/json",
                             "producer_json": {"type": "extractor", "config": {
                                 "provider": "antfly", "model": codex_antfly::DEFAULT_EXTRACT_MODEL,
                                 "schema": {"entities": ["concept","technology","tool","project","organization","person","goal","problem","practice","constraint","metric"],
                                            "relations": [{"type":"part_of"},{"type":"alternative_to"},{"type":"uses"},{"type":"depends_on"},{"type":"replaces"},{"type":"causes"}]},
                                 "options": {"include_confidence": true, "include_spans": true, "long_document": {"mode": "window"}}}}},
                "edge_types": [{"name": "about"}],
                "resolvers": [{"name": "concepts", "table": "repro_concepts",
                               "source_artifact": format!("{name}_extraction"),
                               "resolution_artifact": format!("{name}_resolution"),
                               "key_template": template, "min_confidence": 0.5, "config_generation": 1}],
            }),
        };
        let schema = json!({"document_schemas": {"default": {"schema": {"type": "object", "properties": {"search_text": {"type": "string"}}}}}});
        backend.ensure_table(TableSpec { name: "repro_concepts".into(), schema: schema.clone(), dense: None, graphs: vec![] }).await?;
        backend.ensure_table(TableSpec { name: "repro".into(), schema: schema.clone(), dense: None,
            graphs: vec![graph("kg_v2", "concept/{{ slug _entity.text }}")] }).await?;
        backend.write("repro".into(), vec![Write::put("d1", json!({"search_text": "Codex uses Antfly for semantic search."}))]).await?;
        assert!(backend
            .merge_resolver_keys("repro".into(), "concepts".into(), "concept/agents".into(), "concept/agent".into())
            .await?);
        backend.ensure_table(TableSpec { name: "repro".into(), schema, dense: None,
            graphs: vec![graph("kg_v3", "concept/{{ uslug _entity.text }}")] }).await?;
        let mut serving = String::new();
        for _ in 0..300 {
            serving = backend.serving_graph("repro".into(), "kg_v3".into()).await?;
            if serving == "kg_v3" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(serving, "kg_v3");
        let render = |text: &str| backend.render_resolver_key(
            "repro".into(), "concepts".into(), json!({"text": text, "label": "concept"}));
        // The merge survived the namespace change.
        assert_eq!(render("Agents").await?.as_deref(), Some("concept/agent"));
        // uslug keeps non-ASCII names instead of dropping them.
        assert_eq!(render("Café").await?.as_deref(), Some("concept/café"));
        backend.write("repro".into(), vec![Write::put("d2", json!({"search_text": "Still writable."}))]).await?;
        assert!(backend.get("repro".into(), "d2".into()).await?.is_some());
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Decisions and extractions are logged as labels for calibrating and
/// benchmarking local models.
#[test]
fn label_logs_record_decisions_and_extracted_turns() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::labels;
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let response = json!({"answers": [{"name": "supersedes", "probability": 0.91}]});
        labels::record_decision(
            &antfly,
            labels::DecisionRecord {
                kind: "supersession",
                model: "convaiinnovations/laya",
                subject: None,
                input: "Earlier statement: a\nLater statement: b",
                response: &response,
                result: Some(true),
                score: Some(0.91),
                threshold_bp: Some(8_000),
            },
        )
        .await?;
        let turn = |start: u64, claims: usize| labels::ExtractedTurn {
            turn_id: Some(format!("turn-{start}")),
            start,
            end: start + 4,
            chars: 120,
            claims,
        };
        let record = |claims: Vec<usize>| labels::ExtractionRecord {
            thread_id: "thread-1",
            model: "gpt-5.6-luna",
            turns: claims
                .iter()
                .enumerate()
                .map(|(i, &n)| turn(i as u64 * 4, n))
                .collect(),
            claims: claims.iter().sum(),
            retracts: 0,
        };
        labels::record_extraction(&antfly, record(vec![0, 2])).await?;
        // Re-extracting a turn replaces its row instead of duplicating it.
        labels::record_extraction(&antfly, record(vec![1, 2])).await?;

        let sql = antfly.sql().await?;
        let decisions = sql
            .fetch_all(
                "SELECT kind, model, result, threshold_bp, response FROM codex_decision_log",
                vec![],
            )
            .await?;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].string("kind")?, "supersession");
        assert_eq!(decisions[0].i64("threshold_bp")?, 8_000);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&decisions[0].string("response")?)?,
            response
        );
        let turns = sql
            .fetch_all(
                "SELECT turn_start, claims, batch_claims FROM codex_extraction_log \
                 WHERE thread_id = $1 ORDER BY turn_start",
                codex_antfly::sql_params!["thread-1"],
            )
            .await?;
        let rows: Vec<(i64, i64, i64)> = turns
            .iter()
            .map(|row| {
                Ok((
                    row.i64("turn_start")?,
                    row.i64("claims")?,
                    row.i64("batch_claims")?,
                ))
            })
            .collect::<Result<_, codex_antfly::AntflyError>>()?;
        assert_eq!(rows, vec![(0, 1, 3), (4, 2, 3)]);

        // Triage verdicts join extraction results by `<thread>:<turn start>`.
        for (start, score) in [(0, 0.9), (4, 0.3)] {
            labels::record_decision(
                &antfly,
                labels::DecisionRecord {
                    kind: "triage",
                    model: "convaiinnovations/laya",
                    subject: Some(format!("thread-1:{start}")),
                    input: "turn",
                    response: &json!({}),
                    result: Some(score >= 0.5),
                    score: Some(score),
                    threshold_bp: Some(5_000),
                },
            )
            .await?;
        }
        let report = labels::triage_report(&antfly, &[0.2, 0.5]).await?;
        assert_eq!(report.turns, 2);
        let at = |i: usize| {
            let point = &report.points[i];
            (point.kept_with_claims, point.with_claims, point.skipped)
        };
        // Both turns produced claims; at 0.5 the second would be lost.
        assert_eq!((at(0), at(1)), ((2, 2, 0), (1, 2, 1)));
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// With the default embedder, the memory shadow compares semantic
/// candidates, which find a reworded update.
#[test]
fn claim_memory_shadow_uses_semantic_candidates() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::knowledge::ClaimKind;
    use codex_antfly::knowledge::TurnClaim;
    use codex_antfly::knowledge::TurnClaimBatch;
    if !has_models("google") || !has_models("convaiinnovations") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, true);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let batch = |thread: &str, statement: &str| TurnClaimBatch {
            thread_id: thread.to_string(),
            cwd: Some("/src/codex".to_string()),
            source_updated_at_ms: 1,
            claims: vec![TurnClaim {
                kind: ClaimKind::Decision,
                statement: statement.to_string(),
                turn_id: Some("turn-1".to_string()),
                turn_ordinal: 0,
                replaces: Vec::new(),
                motivates: Vec::new(),
                rejected_in_favor_of: Vec::new(),
            }],
            retracts: Vec::new(),
        };
        let earlier = knowledge
            .append_turn_claims(batch(
                "thread-a",
                "Run the test suite on every pull request in CI.",
            ))
            .await?;
        knowledge.settle_thread("thread-a", None).await?;
        let later = knowledge
            .append_turn_claims(batch(
                "thread-b",
                "Continuous integration now checks each PR with the full tests plus clippy.",
            ))
            .await?;
        knowledge.settle_thread("thread-b", None).await?;
        // Without a judge, a different statement never supersedes.
        let kept = knowledge
            .get(&earlier[0].id)
            .await?
            .ok_or("earlier claim missing")?;
        assert_eq!(kept.status, codex_antfly::knowledge::ClaimStatus::Current);
        let shadow = codex_antfly::labels::latest_responses(
            &antfly,
            codex_antfly::claim_memory::SHADOW_KIND,
        )
        .await?;
        let logged = shadow.get(&later[0].id).ok_or("no shadow comparison")?;
        assert_eq!(logged["mode"], "semantic", "{logged}");
        assert_eq!(logged["semantic_discovery"], "indexed", "{logged}");
        assert!(
            logged["memory_candidates"]
                .as_array()
                .is_some_and(|ids| ids.contains(&json!(earlier[0].id))),
            "{logged}"
        );
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Concepts get descriptions grounded in the claims about them, rewritten
/// only when that evidence changes.
#[test]
fn concepts_are_described_from_their_claims() -> Result<(), Box<dyn std::error::Error>> {
    use codex_antfly::knowledge::ClaimKind;
    use codex_antfly::knowledge::TurnClaim;
    use codex_antfly::knowledge::TurnClaimBatch;
    if !has_models("fastino") || !has_models("ggml-org") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let claim = |statement: &str| TurnClaim {
            kind: ClaimKind::Decision,
            statement: statement.to_string(),
            turn_id: Some("turn-1".to_string()),
            turn_ordinal: 0,
            replaces: Vec::new(),
            motivates: Vec::new(),
            rejected_in_favor_of: Vec::new(),
        };
        knowledge
            .append_turn_claims(TurnClaimBatch {
                thread_id: "thread-a".to_string(),
                cwd: None,
                source_updated_at_ms: 1,
                claims: vec![
                    claim("Codex stores threads and history in an embedded Antfly Lite file through libantfly."),
                    claim("Only one process may write the embedded Antfly Lite file, so Codex runs through the app-server daemon."),
                ],
                retracts: Vec::new(),
            })
            .await?;
        // Concept extraction is asynchronous; wait for a concept the claims
        // are about.
        let mut key = None;
        'wait: for _ in 0..300 {
            for concept in knowledge.concepts().await? {
                if !knowledge.concept_evidence(&concept.key, 8).await.is_empty() {
                    key = Some(concept.key);
                    break 'wait;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let key = key.ok_or("no concept with claims about it")?;
        // Let extraction finish, so the second pass sees no new concepts.
        let mut seen = 0;
        let mut stable = 0;
        for _ in 0..300 {
            let count = knowledge.concepts().await?.len();
            stable = if count == seen { stable + 1 } else { 0 };
            seen = count;
            if stable == 10 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let report = knowledge.describe_concepts(50).await?;
        assert!(report.described >= 1, "{report:?}");
        assert_eq!(report.failed, 0, "{report:?}");
        let descriptions = knowledge.concept_descriptions(std::slice::from_ref(&key)).await?;
        let description = descriptions.get(&key).ok_or("no description")?;
        println!("{key}: {description}");
        assert!(!description.is_empty());

        // Unchanged evidence is not described again.
        let again = knowledge.describe_concepts(50).await?;
        assert_eq!(again.described, 0, "{again:?}");
        assert!(again.unchanged >= 1, "{again:?}");
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Judged concept relations become traversable links, recorded so each
/// pair is judged once.
#[test]
fn concept_links_are_recorded_and_traversable() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let (gemma, embedder, qwen) = (
            "concept/embeddinggemma_2",
            "concept/embedder",
            "concept/qwen3_embedding",
        );
        knowledge
            .record_concept_relation(embedder, gemma, Some((gemma, "is_a", embedder)))
            .await?;
        knowledge
            .record_concept_relation(gemma, qwen, Some((gemma, "alternative_to", qwen)))
            .await?;
        knowledge
            .record_concept_relation(embedder, qwen, None)
            .await?;
        let mut links = knowledge.concept_links().await?;
        links.sort();
        assert_eq!(
            links,
            vec![
                (
                    gemma.to_string(),
                    "alternative_to".to_string(),
                    qwen.to_string()
                ),
                (gemma.to_string(), "is_a".to_string(), embedder.to_string()),
            ]
        );
        let reviewed = knowledge.related_pairs_reviewed().await?;
        assert_eq!(reviewed.len(), 3);
        assert!(reviewed.contains(&(embedder.to_string(), qwen.to_string())));
        // Links are traversed both ways; the graph index is built
        // asynchronously.
        let mut linked = std::collections::HashMap::new();
        for _ in 0..100 {
            linked = knowledge
                .linked_concepts(&[embedder.to_string(), gemma.to_string()])
                .await;
            if linked.get(gemma).is_some_and(|found| found.len() == 2) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(linked.get(embedder), Some(&vec![gemma.to_string()]));
        let mut from_gemma = linked.get(gemma).cloned().unwrap_or_default();
        from_gemma.sort();
        assert_eq!(from_gemma, vec![embedder.to_string(), qwen.to_string()]);
        // Crossing the RPC batch bound preserves each root's neighbors and
        // does not duplicate them when a root appears more than once.
        let many: Vec<String> = [embedder, gemma]
            .repeat(17)
            .into_iter()
            .map(str::to_string)
            .collect();
        let batched = knowledge.linked_concepts(&many).await;
        assert_eq!(batched.len(), linked.len());
        for (key, mut neighbors) in batched {
            neighbors.sort();
            let mut expected = linked.get(&key).ok_or("unexpected concept root")?.clone();
            expected.sort();
            assert_eq!(neighbors, expected);
        }

        // Unlinking removes the edge whichever way it points, and the pair
        // stays judged so it is not linked again.
        assert!(knowledge.unlink_concepts(qwen, gemma).await?);
        assert!(!knowledge.unlink_concepts(qwen, gemma).await?);
        assert_eq!(
            knowledge.concept_links().await?,
            vec![(gemma.to_string(), "is_a".to_string(), embedder.to_string())]
        );
        assert!(
            knowledge
                .related_pairs_reviewed()
                .await?
                .contains(&(gemma.to_string(), qwen.to_string()))
        );
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Concept curation goes through Antfly's entity API: merges, unmerges with
/// a distinct pin, and suppressions are recorded once for every resolver
/// that writes concepts, and reported back.
#[test]
fn concept_curation_merges_unmerges_and_suppresses() -> Result<(), Box<dyn std::error::Error>> {
    if !has_models("fastino") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let (agents, agent, note) = ("concept/agents", "concept/agent", "concept/note");

        assert!(knowledge.merge_concept(agents, agent).await?);
        let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
        assert_eq!(
            curation.redirects.get(agents).map(String::as_str),
            Some(agent),
            "{curation:?}"
        );
        assert_eq!(
            knowledge.concept_key_for("Agents").await?.as_deref(),
            Some(agent)
        );
        // The minted key is the one before the redirect: what a user unmerges.
        assert_eq!(
            knowledge.concept_minted_key("Agents").await?.as_deref(),
            Some(agents)
        );

        assert!(knowledge.unmerge_concept(agents, true).await?);
        let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
        assert!(!curation.redirects.contains_key(agents), "{curation:?}");
        assert!(curation.is_distinct(agent, agents), "{curation:?}");
        assert_eq!(
            knowledge.concept_key_for("Agents").await?.as_deref(),
            Some(agents)
        );

        // Suppressing a merge target suppresses what was merged into it in
        // the same commit, recorded as a cascade.
        let notes = "concept/notes";
        assert!(knowledge.merge_concept(notes, note).await?);
        let user = codex_antfly::knowledge::USER_ACTOR;
        assert_eq!(
            knowledge
                .suppress_concept(note, user, Some("generic"))
                .await?,
            Some(vec![notes.to_string()])
        );
        let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
        assert!(curation.suppressed.contains(note), "{curation:?}");
        assert!(curation.suppressed.contains(notes), "{curation:?}");
        assert!(!curation.redirects.contains_key(notes), "{curation:?}");
        let origin = &curation.suppression_origins[note];
        assert_eq!(
            (origin.actor.as_deref(), origin.reason.as_deref()),
            (Some(user), Some("generic"))
        );
        let cascade = &curation.suppression_origins[notes];
        assert_eq!(
            (cascade.origin.as_str(), cascade.cascade_from.as_deref()),
            ("cascade", Some(note))
        );
        assert!(knowledge.unsuppress_concept(note, user).await?);
        let curation = knowledge.concept_curation().await?.ok_or("no curation")?;
        assert!(!curation.suppressed.contains(note), "{curation:?}");
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// History concepts on Lite: a thread's user and agent messages are grouped
/// per turn, GLiNER extracts concepts, and the thread node gets `about`
/// edges to them, from ordinary writes; tool output is left out.
#[test]
fn history_concepts_link_threads_to_concepts() -> Result<(), Box<dyn std::error::Error>> {
    if !has_models("fastino") {
        return Ok(());
    }
    let dir = tempfile::tempdir()?;
    runtime().block_on(async {
        let antfly = open(&dir, false);
        let knowledge = antfly.knowledge().ok_or("knowledge graph disabled")?;
        let thread = "history-lite";
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
        ];
        antfly
            .documents(schema::HISTORY_ITEMS)
            .write(
                rows.iter()
                    .map(|(ordinal, turn, speaker, text)| {
                        let mut doc =
                            json!({"thread_id": thread, "ordinal": ordinal, "turn_id": turn});
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
            threads = knowledge.threads_about(&seeds, 10).await;
            if !threads.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let found = threads
            .iter()
            .find(|found| found.thread_id == thread)
            .ok_or_else(|| format!("thread not reached: {threads:?}"))?;
        assert_eq!(found.concepts, vec!["concept/embeddinggemma".to_string()]);
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
