//! Per-turn claim extraction into the knowledge graph.
//!
//! Claims are distilled from finished turns, not whole threads, so knowledge
//! from a session is searchable minutes after it is discussed instead of
//! after the memory pipeline's idle window. For each thread the pipeline:
//!
//! 1. Waits for a quiet period after a turn ends (`claim_debounce_seconds`),
//!    or runs at once when the thread stops.
//! 2. Leases the thread (`codex_claim_progress`), reads its history items
//!    from the extraction watermark on, and groups them into finished turns.
//! 3. Sends batches of turns, with the thread's current claims, to the
//!    memory extraction model, which returns new claims and the existing
//!    ones they replace or retract. The claims are recorded as provisional
//!    and the watermark advances past the batch.
//! 4. Settles the thread's claims once it has been idle for
//!    `claim_settle_minutes` or has stopped: only then are they compared
//!    with other threads' claims and allowed to supersede them.
//!
//! A sweep shortly after the first thread starts picks up threads whose
//! history was never extracted or whose claims were never settled (a crash,
//! an import, a process that exited during the quiet period).

pub mod admin;
mod concepts;
mod extract;
mod transcript;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use codex_antfly::Antfly;
use codex_antfly::ClaimPipelineConfig;
use codex_antfly::claim_progress;
use codex_antfly::history;
use codex_antfly::knowledge::ClaimStatus;
use codex_antfly::knowledge::TurnClaim;
use codex_antfly::knowledge::TurnClaimBatch;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_core::config::ThreadStoreConfig;
use codex_extension_api::ConfigContributor;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadStartInput;
use codex_extension_api::ThreadStopInput;
use codex_extension_api::TurnAbortInput;
use codex_extension_api::TurnLifecycleContributor;
use codex_extension_api::TurnStopInput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use transcript::Turn;

/// History items read per page.
const PAGE_ITEMS: usize = 2_000;
/// Model requests one job may make; a longer backlog continues later.
const MAX_REQUESTS_PER_JOB: usize = 16;
/// How long a lease protects a thread from other workers.
const LEASE_MS: i64 = 15 * 60 * 1_000;
/// First retry delay after a failure; doubles per failure up to a day.
const RETRY_BASE_MS: i64 = 5 * 60 * 1_000;
const RETRY_MAX_MS: i64 = 24 * 60 * 60 * 1_000;
/// Delay after the first thread starts before sweeping, so the sweep does
/// not compete with startup.
const SWEEP_DELAY: Duration = Duration::from_secs(60);
/// Threads examined per sweep.
const SWEEP_LIMIT: usize = 16;
/// Minimum time between concept consolidation passes on one database.
const CONSOLIDATION_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Finished turns that trigger extraction without waiting for quiet.
const MAX_PENDING_TURNS: u32 = 4;

/// What a thread needs for extraction, captured at thread start.
#[derive(Clone)]
struct ThreadClaims {
    antfly: Arc<Antfly>,
    config: Arc<Config>,
    settings: ClaimPipelineConfig,
}

impl ThreadClaims {
    /// Present when the thread store is Antfly with claim extraction on and
    /// the session is one memories are generated from.
    fn from_config(config: &Config, source: Option<&SessionSource>) -> Option<Self> {
        let ThreadStoreConfig::Antfly(antfly) = &config.experimental_thread_store else {
            return None;
        };
        let settings = antfly.knowledge.as_ref()?.claims.clone()?;
        if config.ephemeral || !config.memories.generate_memories {
            return None;
        }
        if let Some(source) = source
            && !codex_rollout::INTERACTIVE_SESSION_SOURCES.contains(source)
        {
            return None;
        }
        Some(Self {
            antfly: codex_antfly::shared(antfly),
            config: Arc::new(config.clone()),
            settings,
        })
    }
}

struct Job {
    thread_id: String,
    claims: ThreadClaims,
    /// Settle the thread's claims after extracting.
    settle: bool,
}

#[derive(Default)]
struct Timers {
    /// Pending debounce or settle timer per thread.
    by_thread: HashMap<String, JoinHandle<()>>,
    /// Finished turns since the thread's last extraction.
    pending_turns: HashMap<String, u32>,
}

pub(crate) struct ClaimPipeline {
    thread_manager: Weak<ThreadManager>,
    /// Lease owner for this process.
    owner: String,
    queue: OnceLock<mpsc::UnboundedSender<Job>>,
    timers: Arc<Mutex<Timers>>,
    /// Databases already swept by this process.
    swept: Mutex<HashSet<usize>>,
}

impl ClaimPipeline {
    pub(crate) fn new(thread_manager: Weak<ThreadManager>) -> Arc<Self> {
        Arc::new(Self {
            thread_manager,
            owner: uuid::Uuid::new_v4().to_string(),
            queue: OnceLock::new(),
            timers: Arc::new(Mutex::new(Timers::default())),
            swept: Mutex::new(HashSet::new()),
        })
    }

    /// The worker's queue, started on first use (inside the runtime).
    fn queue(&self) -> mpsc::UnboundedSender<Job> {
        self.queue
            .get_or_init(|| {
                let (sender, receiver) = mpsc::unbounded_channel();
                let worker = Worker {
                    thread_manager: self.thread_manager.clone(),
                    owner: self.owner.clone(),
                    timers: Arc::clone(&self.timers),
                    queue: sender.clone(),
                    consolidated: Mutex::new(HashMap::new()),
                    per_job_requests: MAX_REQUESTS_PER_JOB,
                    budget: None,
                    schedule_settles: true,
                };
                tokio::spawn(worker.run(receiver));
                sender
            })
            .clone()
    }

    fn enqueue(&self, job: Job) {
        let _ = self.queue().send(job);
    }

    /// Replaces `thread_id`'s pending timer with one that enqueues a job
    /// after `delay`.
    fn schedule(&self, thread_id: &str, claims: ThreadClaims, delay: Duration, settle: bool) {
        let queue = self.queue();
        let job_thread = thread_id.to_string();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = queue.send(Job {
                thread_id: job_thread,
                claims,
                settle,
            });
        });
        if let Ok(mut timers) = self.timers.lock()
            && let Some(previous) = timers.by_thread.insert(thread_id.to_string(), timer)
        {
            previous.abort();
        }
    }

    fn cancel_timer(&self, thread_id: &str) {
        if let Ok(mut timers) = self.timers.lock() {
            if let Some(timer) = timers.by_thread.remove(thread_id) {
                timer.abort();
            }
            timers.pending_turns.remove(thread_id);
        }
    }

    fn turn_finished(&self, thread_store: &ExtensionData) {
        let Some(claims) = thread_store.get::<ThreadClaims>() else {
            return;
        };
        let thread_id = thread_store.level_id().to_string();
        let pending = self
            .timers
            .lock()
            .map(|mut timers| {
                let count = timers.pending_turns.entry(thread_id.clone()).or_default();
                *count += 1;
                *count
            })
            .unwrap_or(1);
        let delay = if pending >= MAX_PENDING_TURNS {
            Duration::ZERO
        } else {
            Duration::from_secs(claims.settings.debounce_seconds)
        };
        self.schedule(&thread_id, (*claims).clone(), delay, false);
    }

    /// Sweeps `claims`' database once per process, after a delay.
    fn sweep_once(&self, claims: &ThreadClaims) {
        let key = Arc::as_ptr(&claims.antfly) as usize;
        let first = self
            .swept
            .lock()
            .map(|mut swept| swept.insert(key))
            .unwrap_or(false);
        if !first {
            return;
        }
        let queue = self.queue();
        let claims = claims.clone();
        tokio::spawn(async move {
            tokio::time::sleep(SWEEP_DELAY).await;
            if let Err(err) = sweep(&queue, &claims).await {
                tracing::warn!("antfly: claim sweep failed: {err}");
            }
        });
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

async fn sweep(
    queue: &mpsc::UnboundedSender<Job>,
    claims: &ThreadClaims,
) -> codex_antfly::AntflyResult<()> {
    if let Some(knowledge) = claims.antfly.knowledge()
        && let Some(config) = claims.antfly.config().knowledge.as_ref()
        && !config.concept_aliases.is_empty()
    {
        match concepts::apply_aliases(&knowledge, &config.concept_aliases).await {
            Ok(applied) if applied > 0 => {
                tracing::info!("antfly: applied {applied} concept aliases")
            }
            Ok(_) => {}
            Err(err) => tracing::warn!("antfly: applying concept aliases failed: {err}"),
        }
    }
    let now = now_ms();
    let sources: Vec<String> = codex_rollout::INTERACTIVE_SESSION_SOURCES
        .iter()
        .map(ToString::to_string)
        .collect();
    let sources: Vec<&str> = sources.iter().map(String::as_str).collect();
    let max_age_ms = claims
        .config
        .memories
        .max_rollout_age_days
        .max(0)
        .saturating_mul(24 * 60 * 60 * 1_000);
    let settle_before_ms =
        now.saturating_sub((claims.settings.settle_minutes as i64).saturating_mul(60_000));
    let candidates = claim_progress::sweep_candidates(
        &claims.antfly,
        claim_progress::SweepParams {
            sources: &sources,
            min_updated_at_ms: now.saturating_sub(max_age_ms),
            settle_before_ms,
            now_ms: now,
            limit: SWEEP_LIMIT,
        },
    )
    .await?;
    for candidate in candidates {
        let _ = queue.send(Job {
            thread_id: candidate.thread_id,
            claims: claims.clone(),
            settle: candidate.updated_at_ms <= settle_before_ms,
        });
    }
    Ok(())
}

struct Worker {
    thread_manager: Weak<ThreadManager>,
    owner: String,
    timers: Arc<Mutex<Timers>>,
    queue: mpsc::UnboundedSender<Job>,
    /// Last consolidation pass per database.
    consolidated: Mutex<HashMap<usize, std::time::Instant>>,
    /// Model requests one job may make before yielding.
    per_job_requests: usize,
    /// Model requests left across all jobs, when capped.
    budget: Option<Arc<AtomicUsize>>,
    /// Whether finished jobs schedule a settle (live pipeline only).
    schedule_settles: bool,
}

/// What processing one job did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobOutcome {
    /// Memory generation is off for the thread, or another worker holds it,
    /// or it is backing off.
    Skipped,
    /// No loaded thread to attribute model requests to.
    NoHost,
    Failed(String),
    /// `requests` model requests made; `complete` when everything ready was
    /// extracted (and settled, if asked), false when a request cap stopped it.
    Done {
        requests: usize,
        complete: bool,
    },
}

/// Why a job stopped early without failing.
enum Stop {
    /// No live thread to attribute model requests to; retry later.
    NoHost,
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for Stop {
    fn from(err: anyhow::Error) -> Self {
        Stop::Failed(err)
    }
}

impl From<codex_antfly::AntflyError> for Stop {
    fn from(err: codex_antfly::AntflyError) -> Self {
        Stop::Failed(err.into())
    }
}

impl Worker {
    async fn run(self, mut receiver: mpsc::UnboundedReceiver<Job>) {
        while let Some(job) = receiver.recv().await {
            self.process(job).await;
        }
    }

    /// A worker for a standalone run (`codex knowledge backfill`): no queue
    /// or timers, `per_job_requests` per thread and an optional total budget.
    pub(crate) fn standalone(
        thread_manager: Weak<ThreadManager>,
        per_job_requests: usize,
        budget: Option<Arc<AtomicUsize>>,
    ) -> Self {
        let (queue, _receiver) = mpsc::unbounded_channel();
        Self {
            thread_manager,
            owner: uuid::Uuid::new_v4().to_string(),
            timers: Arc::new(Mutex::new(Timers::default())),
            queue,
            consolidated: Mutex::new(HashMap::new()),
            per_job_requests,
            budget,
            schedule_settles: false,
        }
    }

    pub(crate) async fn process(&self, job: Job) -> JobOutcome {
        let antfly = Arc::clone(&job.claims.antfly);
        // A thread not yet persisted is picked up by a later trigger or sweep.
        match claim_progress::memory_enabled(&antfly, &job.thread_id).await {
            Ok(true) => {}
            Ok(false) => return JobOutcome::Skipped,
            Err(err) => {
                tracing::warn!(
                    "antfly: memory mode of {} unavailable: {err}",
                    job.thread_id
                );
                return JobOutcome::Failed(err.to_string());
            }
        }
        let progress =
            match claim_progress::lease(&antfly, &job.thread_id, &self.owner, LEASE_MS).await {
                Ok(Some(progress)) => progress,
                Ok(None) => return JobOutcome::Skipped,
                Err(err) => {
                    tracing::warn!("antfly: claim lease for {} failed: {err}", job.thread_id);
                    return JobOutcome::Failed(err.to_string());
                }
            };
        if let Ok(mut timers) = self.timers.lock() {
            timers.pending_turns.remove(&job.thread_id);
        }
        let outcome = self.extract(&job, progress).await;
        if outcome.is_ok() && !job.settle && self.schedule_settles {
            self.schedule_settle(&job);
        }
        let failure = match &outcome {
            Ok(_) | Err(Stop::NoHost) => None,
            Err(Stop::Failed(err)) => {
                tracing::warn!(
                    "antfly: claim extraction for {} failed: {err}",
                    job.thread_id
                );
                let delay = RETRY_BASE_MS
                    .saturating_mul(1_i64 << progress.failures.min(16))
                    .min(RETRY_MAX_MS);
                Some((err.to_string(), now_ms().saturating_add(delay)))
            }
        };
        if let Err(err) = claim_progress::release(
            &antfly,
            &job.thread_id,
            &self.owner,
            failure.as_ref().map(|(error, at)| (error.as_str(), *at)),
        )
        .await
        {
            tracing::warn!(
                "antfly: releasing claim lease for {} failed: {err}",
                job.thread_id
            );
        }
        match outcome {
            Ok((requests, complete)) => JobOutcome::Done { requests, complete },
            Err(Stop::NoHost) => JobOutcome::NoHost,
            Err(Stop::Failed(err)) => JobOutcome::Failed(err.to_string()),
        }
    }

    /// Settles the thread once it has been quiet for `settle_minutes`, unless
    /// a newer turn already has a timer pending (its extraction schedules
    /// the settle in turn).
    fn schedule_settle(&self, job: &Job) {
        let Ok(mut timers) = self.timers.lock() else {
            return;
        };
        if timers
            .by_thread
            .get(&job.thread_id)
            .is_some_and(|timer| !timer.is_finished())
        {
            return;
        }
        let queue = self.queue.clone();
        let next = Job {
            thread_id: job.thread_id.clone(),
            claims: job.claims.clone(),
            settle: true,
        };
        let delay = Duration::from_secs(job.claims.settings.settle_minutes.saturating_mul(60));
        let timer = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = queue.send(next);
        });
        timers.by_thread.insert(job.thread_id.clone(), timer);
    }

    /// A live thread to attribute model requests to: the job's own thread
    /// when loaded, else any loaded thread.
    async fn host(
        &self,
        thread_manager: &ThreadManager,
        thread_id: &str,
    ) -> Option<(ThreadId, Arc<codex_core::CodexThread>)> {
        if let Ok(id) = ThreadId::from_string(thread_id)
            && let Ok(thread) = thread_manager.get_thread(id).await
        {
            return Some((id, thread));
        }
        for id in thread_manager.list_thread_ids().await {
            if let Ok(thread) = thread_manager.get_thread(id).await {
                return Some((id, thread));
            }
        }
        None
    }

    /// Extracts the job's ready turns; `(requests, complete)`.
    async fn extract(
        &self,
        job: &Job,
        progress: claim_progress::ClaimProgress,
    ) -> Result<(usize, bool), Stop> {
        let antfly = &job.claims.antfly;
        let knowledge = antfly
            .knowledge()
            .ok_or_else(|| anyhow::anyhow!("the knowledge graph is disabled"))?;
        let mut next = progress.next_ordinal;
        let mut requests = 0;
        loop {
            let items = history::items_from(antfly, &job.thread_id, next, PAGE_ITEMS).await?;
            if items.is_empty() {
                break;
            }
            let more = items.len() == PAGE_ITEMS;
            let turns = transcript::turns(&items);
            let mut ready: Vec<&Turn> = turns
                .iter()
                .take_while(|turn| turn.finished || (job.settle && !more))
                .collect();
            if ready.is_empty() && more {
                // One turn longer than a page: take the page as it is so a
                // huge turn cannot stall the thread.
                ready = turns.iter().collect();
            }
            if ready.is_empty() {
                break;
            }
            for batch in batches(&ready) {
                let evidence: Vec<&Turn> = batch
                    .iter()
                    .copied()
                    .filter(|turn| !turn.text.is_empty())
                    .collect();
                if !evidence.is_empty() {
                    if requests == self.per_job_requests || !self.take_budget() {
                        return Ok((requests, false));
                    }
                    requests += 1;
                    let current: Vec<_> = knowledge
                        .thread_claims(&job.thread_id)
                        .await?
                        .into_iter()
                        .filter(|claim| claim.status == ClaimStatus::Current)
                        .collect();
                    let cwd = job.claims.config.cwd.as_path().display().to_string();
                    let request = extract::request(Some(&cwd), &current, &evidence);
                    let answer = self
                        .sample(
                            job,
                            extract::INSTRUCTIONS,
                            &request.input,
                            extract::output_schema(),
                        )
                        .await?;
                    let (claims, retracts) = request.parse(&answer)?;
                    let model = extract_model(&job.claims.config);
                    let record = extraction_record(
                        &job.thread_id,
                        &model,
                        &evidence,
                        &claims,
                        retracts.len(),
                    );
                    if let Err(err) = codex_antfly::labels::record_extraction(antfly, record).await
                    {
                        tracing::debug!("failed to record claim extraction: {err}");
                    }
                    if !claims.is_empty() || !retracts.is_empty() {
                        knowledge
                            .append_turn_claims(TurnClaimBatch {
                                thread_id: job.thread_id.clone(),
                                cwd: Some(cwd),
                                source_updated_at_ms: now_ms(),
                                claims,
                                retracts,
                            })
                            .await?;
                    }
                }
                if let Some(last) = batch.last() {
                    next = last.end;
                    claim_progress::advance(antfly, &job.thread_id, &self.owner, next).await?;
                }
            }
            if !more {
                break;
            }
        }
        if job.settle {
            knowledge.settle_thread(&job.thread_id).await?;
            claim_progress::settle(antfly, &job.thread_id, &self.owner, next).await?;
            self.consolidate_if_due(job).await;
        }
        Ok((requests, true))
    }

    /// The extraction model's verdict on whether each pair of concept names
    /// means the same thing.
    pub(crate) async fn judge(
        &self,
        job: &Job,
        pairs: Vec<(String, String)>,
    ) -> anyhow::Result<Vec<bool>> {
        let count = pairs.len();
        let answer = self
            .sample(
                job,
                concepts::JUDGE_INSTRUCTIONS,
                &concepts::judge_input(&pairs),
                concepts::judge_schema(),
            )
            .await
            .map_err(|stop| match stop {
                Stop::NoHost => anyhow::anyhow!("no loaded thread to attribute the request to"),
                Stop::Failed(err) => err,
            })?;
        concepts::parse_judgments(&answer, count)
    }

    /// Takes one request from the budget; false when it is spent.
    fn take_budget(&self) -> bool {
        let Some(budget) = &self.budget else {
            return true;
        };
        budget
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }

    /// One request to the memory extraction model, attributed to a live
    /// thread (see [`Self::host`]).
    async fn sample(
        &self,
        job: &Job,
        instructions: &str,
        input: &str,
        schema: serde_json::Value,
    ) -> Result<String, Stop> {
        let thread_manager = self.thread_manager.upgrade().ok_or(Stop::NoHost)?;
        let (host_id, host) = self
            .host(&thread_manager, &job.thread_id)
            .await
            .ok_or(Stop::NoHost)?;
        Ok(codex_memories_write::sample_structured(
            thread_manager,
            host_id,
            host,
            &job.claims.config,
            instructions.to_string(),
            input,
            schema,
        )
        .await?)
    }

    /// Runs a concept consolidation pass when the last one on this database
    /// was at least [`CONSOLIDATION_INTERVAL`] ago. Failures are logged.
    async fn consolidate_if_due(&self, job: &Job) {
        let key = Arc::as_ptr(&job.claims.antfly) as usize;
        let now = std::time::Instant::now();
        let due = self
            .consolidated
            .lock()
            .map(|mut last| {
                let due = last
                    .get(&key)
                    .is_none_or(|at| now.duration_since(*at) >= CONSOLIDATION_INTERVAL);
                if due {
                    last.insert(key, now);
                }
                due
            })
            .unwrap_or(false);
        if !due {
            return;
        }
        let Some(knowledge) = job.claims.antfly.knowledge() else {
            return;
        };
        let judge = |pairs: Vec<(String, String)>| self.judge(job, pairs);
        match concepts::consolidate(&knowledge, judge).await {
            Ok(report) => tracing::info!("antfly: concept consolidation: {report:?}"),
            Err(err) => tracing::warn!("antfly: concept consolidation failed: {err}"),
        }
    }
}

/// The model claim extraction samples, as configured.
fn extract_model(config: &Config) -> String {
    config
        .memories
        .extract_model
        .clone()
        .unwrap_or_else(|| "provider_default".to_string())
}

/// What one extraction request covered and found, per turn: the label for
/// triaging turns before extraction.
fn extraction_record<'a>(
    thread_id: &'a str,
    model: &'a str,
    evidence: &[&Turn],
    claims: &[TurnClaim],
    retracts: usize,
) -> codex_antfly::labels::ExtractionRecord<'a> {
    codex_antfly::labels::ExtractionRecord {
        thread_id,
        model,
        turns: evidence
            .iter()
            .map(|turn| codex_antfly::labels::ExtractedTurn {
                turn_id: turn.turn_id.clone(),
                start: turn.start,
                end: turn.end,
                chars: turn.text.chars().count(),
                claims: claims
                    .iter()
                    .filter(|claim| claim.turn_ordinal == turn.start)
                    .count(),
            })
            .collect(),
        claims: claims.len(),
        retracts,
    }
}

/// Consecutive turns grouped so each request stays within the input budget.
/// A turn over budget is cut to it.
fn batches<'a>(turns: &[&'a Turn]) -> Vec<Vec<&'a Turn>> {
    let mut batches: Vec<Vec<&'a Turn>> = Vec::new();
    let mut size = 0;
    for turn in turns {
        let len = turn.text.len().min(extract::MAX_TURN_CHARS);
        if batches.is_empty() || size + len > extract::MAX_TURN_CHARS {
            batches.push(Vec::new());
            size = 0;
        }
        size += len;
        if let Some(batch) = batches.last_mut() {
            batch.push(turn);
        }
    }
    batches
}

impl ThreadLifecycleContributor<Config> for ClaimPipeline {
    fn on_thread_start<'a>(
        &'a self,
        input: ThreadStartInput<'a, Config>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if let Some(claims) =
                ThreadClaims::from_config(input.config, Some(input.session_source))
            {
                self.sweep_once(&claims);
                input.thread_store.insert(claims);
            }
        })
    }

    fn on_thread_stop<'a>(&'a self, input: ThreadStopInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let thread_id = input.thread_store.level_id().to_string();
            self.cancel_timer(&thread_id);
            if let Some(claims) = input.thread_store.get::<ThreadClaims>() {
                // The worker is process-wide, not thread-owned: the job
                // outlives this thread's runtime.
                self.enqueue(Job {
                    thread_id,
                    claims: (*claims).clone(),
                    settle: true,
                });
            }
        })
    }
}

impl TurnLifecycleContributor for ClaimPipeline {
    fn on_turn_stop<'a>(&'a self, input: TurnStopInput<'a>) -> ExtensionFuture<'a, ()> {
        self.turn_finished(input.thread_store);
        Box::pin(std::future::ready(()))
    }

    fn on_turn_abort<'a>(&'a self, input: TurnAbortInput<'a>) -> ExtensionFuture<'a, ()> {
        self.turn_finished(input.thread_store);
        Box::pin(std::future::ready(()))
    }
}

impl ConfigContributor<Config> for ClaimPipeline {
    fn on_config_changed(
        &self,
        _session_store: &ExtensionData,
        thread_store: &ExtensionData,
        _previous_config: &Config,
        new_config: &Config,
    ) {
        // The session source is fixed for a thread's life; keep the
        // eligibility decided at start and refresh only the settings.
        if thread_store.get::<ThreadClaims>().is_none() {
            return;
        }
        match ThreadClaims::from_config(new_config, /*source*/ None) {
            Some(claims) => {
                thread_store.insert(claims);
            }
            None => {
                thread_store.remove::<ThreadClaims>();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn turn(start: u64, len: usize) -> Turn {
        Turn {
            turn_id: None,
            start,
            end: start + 1,
            finished: true,
            text: "x".repeat(len),
        }
    }

    #[test]
    fn extraction_records_attribute_claims_to_their_turns() {
        let turns = [turn(0, 10), turn(5, 20), turn(9, 30)];
        let refs: Vec<&Turn> = turns.iter().collect();
        let claim = |turn_ordinal| TurnClaim {
            kind: codex_antfly::knowledge::ClaimKind::Decision,
            statement: "Codex keeps Laya as the decide model.".to_string(),
            turn_id: None,
            turn_ordinal,
            replaces: Vec::new(),
            motivates: Vec::new(),
            rejected_in_favor_of: Vec::new(),
        };
        let record = extraction_record(
            "thread-1",
            "model",
            &refs,
            &[claim(5), claim(5), claim(9)],
            1,
        );
        let claims: Vec<usize> = record.turns.iter().map(|turn| turn.claims).collect();
        assert_eq!(claims, vec![0, 2, 1]);
        assert_eq!(record.turns[1].chars, 20);
        assert_eq!((record.claims, record.retracts), (3, 1));
    }

    #[test]
    fn batches_stay_within_the_input_budget() {
        let turns = [
            turn(0, 30_000),
            turn(1, 25_000),
            turn(2, 10_000),
            turn(3, 200_000),
            turn(4, 0),
        ];
        let refs: Vec<&Turn> = turns.iter().collect();
        let starts: Vec<Vec<u64>> = batches(&refs)
            .iter()
            .map(|batch| batch.iter().map(|turn| turn.start).collect())
            .collect();
        assert_eq!(starts, vec![vec![0, 1], vec![2], vec![3, 4]]);
    }
}
