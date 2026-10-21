//! `codex knowledge`: status, backfill and consolidation over existing
//! history, driven by the same worker as the live pipeline.
//!
//! The live pipeline only reaches threads that run turns, plus a small
//! startup sweep of recent threads. Imported and older threads are reached
//! here, on request: [`plan`] lists what is pending (and what it would cost)
//! without touching anything, [`run`] extracts and settles it under a request
//! budget, and [`consolidate`] runs a concept-merge pass. Every step uses the
//! pipeline's leases and progress rows, so runs are resumable and never
//! extract a thread twice. Claim extraction stays in Codex rather than in an
//! Antfly enrichment: it needs Codex's model client and auth (Antfly's
//! ChatGPT connector is for interactive generation only), and it is stateful
//! per thread. Antfly's own enrichments (concepts, embeddings, edges) follow
//! the written claims as usual.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_antfly::Antfly;
use codex_antfly::claim_progress;
use codex_antfly::claim_progress::SweepCandidate;
use codex_antfly::claim_progress::SweepParams;
use codex_antfly::knowledge::ClaimCounts;
use codex_core::ThreadManager;
use codex_core::config::Config;

use super::Job;
pub use super::JobOutcome;
use super::ThreadClaims;
use super::Worker;
use super::concepts;
pub use super::concepts::Consolidation;
pub use super::concepts::GenericScreen;
pub use super::concepts::MergeVerdict;
pub use super::concepts::Relations;
use super::now_ms;

/// Threads [`plan`] examines when no limit is given.
pub const DEFAULT_LIMIT: usize = 25;

/// What to backfill.
#[derive(Clone, Debug, Default)]
pub struct BackfillOptions {
    /// Only threads updated at or after this time.
    pub since_ms: Option<i64>,
    /// Exactly these threads, whatever their age or source.
    pub threads: Vec<String>,
    /// At most this many threads, newest first.
    pub limit: usize,
}

/// One thread with history the pipeline has not extracted or settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadPlan {
    pub thread_id: String,
    pub updated_at_ms: i64,
    /// Stored history items not yet extracted.
    pub pending_items: u64,
    /// Turns not yet extracted, for threads with a turn projection. Each
    /// extraction request covers one or more turns, so this bounds the
    /// requests the thread needs.
    pub pending_turns: Option<u64>,
}

fn claims(config: &Config) -> anyhow::Result<ThreadClaims> {
    ThreadClaims::from_config(config, /*source*/ None).ok_or_else(|| {
        anyhow::anyhow!(
            "claim extraction is off: it needs the Antfly thread store with the knowledge \
             graph and claim_extraction on, and memories.generate_memories"
        )
    })
}

async fn plan_one(antfly: &Antfly, candidate: SweepCandidate) -> anyhow::Result<ThreadPlan> {
    let next = candidate
        .progress
        .map(|progress| progress.next_ordinal)
        .unwrap_or(0);
    Ok(ThreadPlan {
        pending_items: candidate.history_end.saturating_sub(next),
        pending_turns: claim_progress::pending_turns(antfly, &candidate.thread_id, next).await?,
        updated_at_ms: candidate.updated_at_ms,
        thread_id: candidate.thread_id,
    })
}

/// Threads to backfill, newest first. Reads only.
pub async fn plan(antfly: &Antfly, options: &BackfillOptions) -> anyhow::Result<Vec<ThreadPlan>> {
    let mut candidates = Vec::new();
    if options.threads.is_empty() {
        let sources: Vec<String> = codex_rollout::INTERACTIVE_SESSION_SOURCES
            .iter()
            .map(ToString::to_string)
            .collect();
        let sources: Vec<&str> = sources.iter().map(String::as_str).collect();
        let now = now_ms();
        candidates = claim_progress::sweep_candidates(
            antfly,
            SweepParams {
                sources: &sources,
                min_updated_at_ms: options.since_ms.unwrap_or(0),
                // History is settled as it is backfilled.
                settle_before_ms: now,
                now_ms: now,
                limit: if options.limit == 0 {
                    DEFAULT_LIMIT
                } else {
                    options.limit
                },
            },
        )
        .await?;
    } else {
        for thread_id in &options.threads {
            match claim_progress::thread_candidate(antfly, thread_id).await? {
                Some(candidate) => candidates.push(candidate),
                None => anyhow::bail!("no thread {thread_id}"),
            }
        }
    }
    let mut plans = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        plans.push(plan_one(antfly, candidate).await?);
    }
    Ok(plans)
}

/// What [`run`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackfillReport {
    /// Threads extracted and settled completely.
    pub completed: usize,
    /// Threads another worker held, or with memory generation off.
    pub skipped: usize,
    /// Threads stopped partway by the request budget; rerun to continue.
    pub partial: usize,
    pub failed: Vec<(String, String)>,
    pub requests: usize,
}

/// Extracts and settles `plans` in order, stopping when `max_requests`
/// extraction requests have been made. `on_thread` sees each outcome.
pub async fn run(
    thread_manager: &Arc<ThreadManager>,
    config: &Config,
    plans: &[ThreadPlan],
    max_requests: Option<usize>,
    mut on_thread: impl FnMut(&ThreadPlan, &JobOutcome),
) -> anyhow::Result<BackfillReport> {
    // Fails unless the thread store is Antfly with claim extraction on.
    let claims = claims(config)?;
    let budget = max_requests.map(|requests| Arc::new(AtomicUsize::new(requests)));
    let worker = Worker::standalone(Arc::downgrade(thread_manager), usize::MAX, budget.clone());
    let mut report = BackfillReport::default();
    for plan in plans {
        let outcome = worker
            .process(Job {
                thread_id: plan.thread_id.clone(),
                claims: claims.clone(),
                // Backfilled history is old; settle it as it is extracted.
                settle: true,
            })
            .await;
        on_thread(plan, &outcome);
        match &outcome {
            JobOutcome::Skipped => report.skipped += 1,
            JobOutcome::NoHost => anyhow::bail!("no live thread to attribute model requests to"),
            JobOutcome::Failed(err) => report.failed.push((plan.thread_id.clone(), err.clone())),
            JobOutcome::Done { requests, complete } => {
                report.requests += requests;
                if *complete {
                    report.completed += 1;
                } else {
                    report.partial += 1;
                }
            }
        }
        if budget
            .as_ref()
            .is_some_and(|budget| budget.load(Ordering::SeqCst) == 0)
        {
            break;
        }
    }
    Ok(report)
}

/// Runs one concept pass now: describes concepts whose evidence changed,
/// merges synonyms, then links related concepts.
pub async fn consolidate(
    thread_manager: &Arc<ThreadManager>,
    config: &Config,
) -> anyhow::Result<(GenericScreen, Consolidation, Relations)> {
    let antfly = match &config.experimental_thread_store {
        codex_core::config::ThreadStoreConfig::Antfly(antfly) => codex_antfly::shared(antfly),
        _ => anyhow::bail!("the thread store is not Antfly"),
    };
    let claims = claims(config)?;
    let knowledge = antfly
        .knowledge()
        .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
    if let Some(aliases) = antfly
        .config()
        .knowledge
        .as_ref()
        .map(|k| &k.concept_aliases)
        && !aliases.is_empty()
    {
        concepts::apply_aliases(&knowledge, aliases).await?;
    }
    let worker = Worker::standalone(Arc::downgrade(thread_manager), usize::MAX, None);
    // The thread id only picks the host; any loaded thread serves.
    let job = Job {
        thread_id: String::new(),
        claims,
        settle: false,
    };
    if let Err(err) = knowledge.describe_concepts(usize::MAX).await {
        tracing::warn!("antfly: describing concepts failed: {err}");
    }
    let screened = concepts::screen_generic(
        &knowledge,
        &antfly,
        &super::extract_model(config),
        |concepts| worker.screen(&job, concepts),
    )
    .await?;
    let merged = concepts::consolidate(&knowledge, |pairs| worker.judge(&job, pairs)).await?;
    let related = concepts::relate(&knowledge, |pairs| worker.relate(&job, pairs)).await?;
    Ok((screened, merged, related))
}

/// Writes descriptions for up to `max` concepts whose evidence changed,
/// with the local describe model.
pub async fn describe(
    antfly: &Antfly,
    max: usize,
) -> anyhow::Result<codex_antfly::knowledge::DescribeReport> {
    let knowledge = antfly
        .knowledge()
        .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
    if knowledge.describe_model().is_none() {
        anyhow::bail!("concept descriptions are off (describe_model is empty)");
    }
    Ok(knowledge.describe_concepts(max).await?)
}

/// Benchmarks `models` on every labeled task Codex has data for: the
/// supersession fixture, logged approvals with the user's decision, and
/// logged extraction turns (at most `limit` of each).
pub async fn benchmark(
    antfly: &Antfly,
    models: &[String],
    limit: usize,
) -> anyhow::Result<Vec<codex_antfly::bench::TaskResult>> {
    let approvals = labeled_approvals(antfly, limit).await?;
    let turns = labeled_turns(antfly, limit).await?;
    let mut results = Vec::new();
    for model in models {
        // Generator models (local Gemma) judge concepts in words, not typed
        // decisions; benchmark them only with the generator variants.
        if model.contains("gemma-4") {
            results.push(codex_antfly::bench::concept_merge_generator(antfly, model).await);
            results.push(codex_antfly::bench::concept_relation_generator(antfly, model).await);
            continue;
        }
        results.push(codex_antfly::bench::supersession(antfly, model).await);
        results.push(codex_antfly::bench::concept_merge(antfly, model, false).await);
        results.push(codex_antfly::bench::concept_merge(antfly, model, true).await);
        results.push(codex_antfly::bench::concept_relation(antfly, model).await);
        let mut deny = codex_antfly::bench::TaskResult::new("approval_deny", model);
        let mut allow = codex_antfly::bench::TaskResult::new("approval_allow", model);
        for approval in &approvals {
            let request = crate::request::build_decide_request(model, &approval.state);
            let started = std::time::Instant::now();
            let answers = antfly
                .decide(&request)
                .await
                .map_err(|err| err.to_string())
                .map(|response| crate::request::Answers::parse(&response));
            let elapsed = started.elapsed();
            let scored = |pick: fn(&crate::request::Answers) -> f64| match &answers {
                Ok(answers) => Ok(answers.as_ref().map(pick)),
                Err(err) => Err(err.clone()),
            };
            deny.record_scored(
                approval.denied,
                scored(|answers| answers.destructive),
                elapsed,
            );
            allow.record_scored(
                approval.allowed_and_ran,
                scored(crate::request::Answers::contained),
                elapsed,
            );
        }
        results.push(deny);
        results.push(allow);
        let mut triage = codex_antfly::bench::TaskResult::new("triage", model);
        for turn in &turns {
            let request = super::triage::request(model, &turn.turn);
            triage
                .record(turn.had_claims, async {
                    antfly
                        .decide(&request)
                        .await
                        .map(|response| super::triage::probability(&response))
                        .map_err(|err| err.to_string())
                })
                .await;
        }
        results.push(triage);
    }
    Ok(results)
}

struct LabeledApproval {
    state: String,
    /// The user declined it.
    denied: bool,
    /// The user approved it and it ran successfully.
    allowed_and_ran: bool,
}

/// Recorded approvals the user resolved, newest first.
async fn labeled_approvals(antfly: &Antfly, limit: usize) -> anyhow::Result<Vec<LabeledApproval>> {
    let mut found: Vec<(i64, LabeledApproval)> = antfly
        .documents(codex_antfly::schema::APPROVALS)
        .scan(codex_antfly::ScanRequest::prefix(""))
        .await?
        .into_iter()
        .filter_map(|document| {
            let doc = document.doc;
            let decision = doc.get("decision")?.as_str()?;
            let state = doc.get("search_text")?.as_str()?.to_string();
            let outcome = doc
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let at = doc
                .get("decided_at_ms")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            Some((
                at,
                LabeledApproval {
                    state,
                    denied: matches!(decision, "denied" | "abort"),
                    allowed_and_ran: decision.starts_with("approved") && outcome == "executed",
                },
            ))
        })
        .collect();
    found.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
    Ok(found
        .into_iter()
        .take(limit)
        .map(|(_, approval)| approval)
        .collect())
}

struct LabeledTurn {
    turn: super::transcript::Turn,
    had_claims: bool,
}

/// Turns sent for extraction, newest first, with their rendered evidence.
async fn labeled_turns(antfly: &Antfly, limit: usize) -> anyhow::Result<Vec<LabeledTurn>> {
    let sql = antfly.sql().await?;
    let rows = sql
        .fetch_all(
            "SELECT thread_id, turn_start, turn_end, claims FROM codex_extraction_log \
             ORDER BY extracted_at_ms DESC LIMIT $1",
            codex_antfly::sql_params![i64::try_from(limit).unwrap_or(i64::MAX)],
        )
        .await?;
    let mut turns = Vec::new();
    for row in rows {
        let thread_id = row.string("thread_id")?;
        let start = u64::try_from(row.i64("turn_start")?).unwrap_or_default();
        let end = u64::try_from(row.i64("turn_end")?).unwrap_or_default();
        let items = codex_antfly::history::items_from(
            antfly,
            &thread_id,
            start,
            usize::try_from(end.saturating_sub(start))
                .unwrap_or(usize::MAX)
                .max(1),
        )
        .await?;
        if let Some(turn) = super::transcript::turns(&items)
            .into_iter()
            .find(|turn| turn.start == start && !turn.text.is_empty())
        {
            turns.push(LabeledTurn {
                turn,
                had_claims: row.i64("claims")? > 0,
            });
        }
    }
    Ok(turns)
}

/// Candidate concept pairs for labeling merges and relations, with every
/// signal the judges could use: name and description similarity (relative
/// to each concept's own score), co-mentions in current claims, lexical
/// identity, number tokens, and both descriptions.
pub async fn export_pairs(
    antfly: &Antfly,
    neighbors: usize,
) -> anyhow::Result<Vec<serde_json::Value>> {
    use codex_antfly::knowledge::concept_identity;
    use codex_antfly::knowledge::number_tokens;
    use std::collections::BTreeMap;
    let knowledge = antfly
        .knowledge()
        .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
    let merged: std::collections::BTreeSet<String> = {
        let sql = antfly.sql().await?;
        sql.fetch_all(
            "SELECT key FROM codex_concept_reviews WHERE merged_into IS NOT NULL",
            vec![],
        )
        .await?
        .iter()
        .filter_map(|row| row.string("key").ok())
        .collect()
    };
    let concepts: BTreeMap<String, codex_antfly::knowledge::Concept> = knowledge
        .concepts()
        .await?
        .into_iter()
        .filter(|concept| !merged.contains(&concept.key))
        .map(|concept| (concept.key.clone(), concept))
        .collect();
    let keys: Vec<String> = concepts.keys().cloned().collect();
    let descriptions = knowledge.concept_descriptions(&keys).await?;
    #[derive(Default)]
    struct Signals {
        name: Option<f64>,
        description: Option<f64>,
        co_mentions: usize,
    }
    let mut pairs: BTreeMap<(String, String), Signals> = BTreeMap::new();
    let ordered = |a: &str, b: &str| {
        if a < b {
            (a.to_string(), b.to_string())
        } else {
            (b.to_string(), a.to_string())
        }
    };
    for concept in concepts.values() {
        let hits = knowledge
            .concept_neighbors(&concept.name, neighbors + 1)
            .await?;
        let own = hits
            .iter()
            .find(|(neighbor, _)| neighbor.key == concept.key)
            .or(hits.first())
            .map(|(_, score)| *score)
            .filter(|score| *score > 0.0)
            .unwrap_or(1.0);
        for (neighbor, score) in hits {
            if neighbor.key != concept.key && concepts.contains_key(&neighbor.key) {
                let signals = pairs
                    .entry(ordered(&concept.key, &neighbor.key))
                    .or_default();
                let relative = score / own;
                signals.name = Some(signals.name.map_or(relative, |s: f64| s.max(relative)));
            }
        }
        for (neighbor, relative) in knowledge
            .description_neighbors(&concept.key, neighbors)
            .await?
        {
            if concepts.contains_key(&neighbor) {
                let signals = pairs.entry(ordered(&concept.key, &neighbor)).or_default();
                signals.description = Some(
                    signals
                        .description
                        .map_or(relative, |s: f64| s.max(relative)),
                );
            }
        }
        for (neighbor, count) in knowledge.co_mentioned(&concept.key, 50).await {
            if concepts.contains_key(&neighbor) && neighbor != concept.key {
                let signals = pairs.entry(ordered(&concept.key, &neighbor)).or_default();
                signals.co_mentions = signals.co_mentions.max(count);
            }
        }
    }
    Ok(pairs
        .into_iter()
        .filter_map(|((a, b), signals)| {
            let (ca, cb) = (concepts.get(&a)?, concepts.get(&b)?);
            Some(serde_json::json!({
                "a": ca.name,
                "b": cb.name,
                "a_key": a,
                "b_key": b,
                "a_type": ca.entity_type,
                "b_type": cb.entity_type,
                "a_description": descriptions.get(&a),
                "b_description": descriptions.get(&b),
                "name_similarity": signals.name,
                "description_similarity": signals.description,
                "co_mentions": signals.co_mentions,
                "lexical_identity": concept_identity(&ca.name) == concept_identity(&cb.name),
                "same_numbers": number_tokens(&ca.name) == number_tokens(&cb.name),
            }))
        })
        .collect())
}

/// Benchmarks the extraction model as the concept judge, through a host
/// session: merges judged on names only and with descriptions, and
/// relations, on the labeled concept fixtures.
pub async fn benchmark_llm(
    thread_manager: &Arc<ThreadManager>,
    config: &Config,
) -> anyhow::Result<Vec<codex_antfly::bench::TaskResult>> {
    use codex_antfly::bench::TaskResult;
    let claims = claims(config)?;
    let model = super::extract_model(config);
    let worker = Worker::standalone(Arc::downgrade(thread_manager), usize::MAX, None);
    let job = Job {
        thread_id: String::new(),
        claims,
        settle: false,
    };
    let cases = |fixture: &str| -> Vec<serde_json::Value> {
        fixture
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    };
    let merges = cases(codex_antfly::bench::CONCEPT_MERGE_FIXTURE);
    let relations = cases(codex_antfly::bench::CONCEPT_RELATIONS_FIXTURE);
    let side = |case: &serde_json::Value, key: &str, described: bool| {
        concepts::judged_name(
            case[key].as_str().unwrap_or_default(),
            described
                .then(|| case[format!("{key}_description")].as_str())
                .flatten(),
        )
    };
    let mut results = Vec::new();
    for described in [false, true] {
        let task = if described {
            "concept_merge_llm"
        } else {
            "concept_merge_llm_names"
        };
        let mut result = TaskResult::new(task, &model);
        let instructions = if described {
            format!(
                "{}{}",
                concepts::JUDGE_INSTRUCTIONS,
                concepts::DESCRIBED_JUDGE_NOTE
            )
        } else {
            concepts::JUDGE_INSTRUCTIONS.to_string()
        };
        for chunk in merges.chunks(40) {
            let pairs: Vec<(String, String)> = chunk
                .iter()
                .map(|case| (side(case, "a", described), side(case, "b", described)))
                .collect();
            let started = std::time::Instant::now();
            let verdicts = worker
                .sample(
                    &job,
                    &instructions,
                    &concepts::judge_input_rendered(&pairs),
                    concepts::judge_schema(),
                )
                .await
                .map_err(|_| "model request failed".to_string())
                .and_then(|answer| {
                    concepts::parse_judgments(&answer, pairs.len()).map_err(|err| err.to_string())
                });
            let each = started.elapsed() / u32::try_from(pairs.len().max(1)).unwrap_or(1);
            for (index, case) in chunk.iter().enumerate() {
                let label = case["same"].as_bool().unwrap_or(false);
                // Unsure answers score 0.5: proposed for review, not merged.
                let score = verdicts
                    .as_ref()
                    .map(|verdicts| Some(verdicts[index].score()))
                    .map_err(Clone::clone);
                result.record_scored(label, score, each);
            }
        }
        results.push(result);
    }
    let mut result = TaskResult::new("concept_relation_llm", &model);
    let (mut typed, mut related) = (0usize, 0usize);
    for chunk in relations.chunks(40) {
        let pairs: Vec<(String, String)> = chunk
            .iter()
            .map(|case| (side(case, "a", true), side(case, "b", true)))
            .collect();
        let started = std::time::Instant::now();
        let answers = worker
            .sample(
                &job,
                concepts::RELATION_INSTRUCTIONS,
                &concepts::judge_input_rendered(&pairs),
                concepts::relation_schema(),
            )
            .await
            .map_err(|_| "model request failed".to_string())
            .and_then(|answer| {
                concepts::parse_relations(&answer, pairs.len()).map_err(|err| err.to_string())
            });
        let each = started.elapsed() / u32::try_from(pairs.len().max(1)).unwrap_or(1);
        for (index, case) in chunk.iter().enumerate() {
            let expected = case["relation"].as_str().unwrap_or("none");
            let answer = answers.as_ref().map(|all| all[index].clone());
            if expected != "none" {
                related += 1;
                if answer
                    .as_deref()
                    .is_ok_and(|got| relation_matches(expected, got))
                {
                    typed += 1;
                }
            }
            result.record_scored(
                expected != "none",
                answer
                    .map(|got| Some(if got == "none" { 0.0 } else { 1.0 }))
                    .map_err(Clone::clone),
                each,
            );
        }
    }
    result.notes.push(format!(
        "relation and direction right on {typed} of {related} related pairs"
    ));
    results.push(result);
    Ok(results)
}

/// Whether a judge's `a_is_b`-style answer matches a fixture's
/// `a <relation> b` label.
fn relation_matches(expected: &str, answer: &str) -> bool {
    matches!(
        (expected, answer),
        ("is_a", "a_is_b") | ("part_of", "a_part_b") | ("alternative_to", "alternatives")
    )
}

/// Pipeline-wide state for `codex knowledge status`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KnowledgeStatus {
    pub progress: claim_progress::ProgressSummary,
    /// Eligible threads with unextracted history or unsettled claims.
    pub pending_threads: usize,
    pub claims: ClaimCounts,
    pub concepts: usize,
    /// Concepts merged into another by consolidation.
    pub merged_concepts: u64,
    /// Shadow triage verdicts scored against extraction results.
    pub triage: codex_antfly::labels::TriageReport,
}

/// Thresholds `codex knowledge status` reports triage at.
pub const TRIAGE_THRESHOLDS: &[f64] = &[0.2, 0.5, 0.8];

pub async fn status(antfly: &Antfly) -> anyhow::Result<KnowledgeStatus> {
    let knowledge = antfly
        .knowledge()
        .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
    let pending = plan(
        antfly,
        &BackfillOptions {
            limit: 100_000,
            ..BackfillOptions::default()
        },
    )
    .await?;
    Ok(KnowledgeStatus {
        progress: claim_progress::summary(antfly).await?,
        pending_threads: pending.len(),
        claims: knowledge.claim_counts().await?,
        concepts: knowledge.concepts().await?.len(),
        merged_concepts: knowledge.merged_concept_count().await?,
        triage: codex_antfly::labels::triage_report(antfly, TRIAGE_THRESHOLDS).await?,
    })
}
