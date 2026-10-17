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

/// Runs one concept consolidation pass now.
pub async fn consolidate(
    thread_manager: &Arc<ThreadManager>,
    config: &Config,
) -> anyhow::Result<Consolidation> {
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
    concepts::consolidate(&knowledge, |pairs| worker.judge(&job, pairs)).await
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

/// Pipeline-wide state for `codex knowledge status`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KnowledgeStatus {
    pub progress: claim_progress::ProgressSummary,
    /// Eligible threads with unextracted history or unsettled claims.
    pub pending_threads: usize,
    pub claims: ClaimCounts,
    pub concepts: usize,
    /// Concepts merged into another by consolidation.
    pub merged_concepts: u64,
}

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
    })
}
