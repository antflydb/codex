//! Antfly backend for `MemoryStore`. Mirrors `state/src/runtime/memories.rs`
//! (the `stage1_outputs`/`jobs` SQLite tables) 1:1 where practical.
//!
//! Key layout (see `antfly_backend::mod` docs):
//! - `st:mem:{version}:s1:{thread}` — one combined record per thread holding
//!   both its `Stage1Output` (if any) and its stage-1 job/lease state (if
//!   any). SQLite keeps these in separate tables joined by `thread_id`;
//!   Antfly has no join, so this backend keeps them in one document instead
//!   and updates both halves under one `antfly.lock()` critical section.
//! - `st:mem:{version}:p2` — the single global phase-2 consolidation job.
//! - `st:mem:{version}:progress` — `{max_thread_count}` consolidation
//!   progress, preserved across pruning and cleared only by an explicit
//!   reset.
//!
//! `version` namespaces `MemoryVersion::V1`/`V2` the same way
//! `memories_1.sqlite`/`memories_v2_1.sqlite` do, but (matching the SQLite
//! implementation) both versions share the *same* underlying `threads`
//! metadata: `enabled_thread_metadata`/`mark_thread_memory_mode_polluted`
//! always go through `thread_adapter`, which is version-independent.

use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use super::internal;
use super::thread_adapter;
use crate::Phase2JobClaimOutcome;
use crate::Stage1JobClaim;
use crate::Stage1JobClaimOutcome;
use crate::Stage1Output;
use crate::Stage1StartupClaimParams;

const DEFAULT_RETRY_REMAINING: i64 = 3;
const PHASE2_SUCCESS_COOLDOWN_SECONDS: i64 = 6 * 60 * 60;

fn s1_key(version: &str, thread_id: ThreadId) -> String {
    format!("st:mem:{version}:s1:{thread_id}")
}

fn s1_prefix(version: &str) -> String {
    format!("st:mem:{version}:s1:")
}

fn phase2_key(version: &str) -> String {
    format!("st:mem:{version}:p2")
}

fn progress_key(version: &str) -> String {
    format!("st:mem:{version}:progress")
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct JobState {
    status: String, // "running" | "done" | "error" | "pending"
    worker_id: Option<String>,
    ownership_token: Option<String>,
    started_at: Option<i64>,
    finished_at: Option<i64>,
    lease_until: Option<i64>,
    retry_at: Option<i64>,
    retry_remaining: i64,
    last_error: Option<String>,
    input_watermark: Option<i64>,
    last_success_watermark: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stage1OutputDoc {
    source_updated_at: i64,
    raw_memory: String,
    rollout_summary: String,
    rollout_slug: Option<String>,
    generated_at: i64,
    #[serde(default)]
    usage_count: i64,
    #[serde(default)]
    last_usage: Option<i64>,
    #[serde(default)]
    selected_for_phase2: bool,
    #[serde(default)]
    selected_for_phase2_source_updated_at: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Stage1Record {
    output: Option<Stage1OutputDoc>,
    job: Option<JobState>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ConsolidationProgress {
    max_thread_count: u32,
}

fn epoch(dt: DateTime<Utc>) -> i64 {
    dt.timestamp()
}

fn from_epoch(secs: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp: {secs}"))
}

async fn get_record(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
) -> anyhow::Result<Stage1Record> {
    Ok(antfly
        .get_as(s1_key(version, thread_id))
        .await
        .map_err(internal)?
        .unwrap_or_default())
}

async fn put_record(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    record: &Stage1Record,
) -> anyhow::Result<()> {
    antfly
        .write(vec![Write::put(
            s1_key(version, thread_id),
            serde_json::to_value(record)?,
        )])
        .await
        .map_err(internal)
}

pub(crate) async fn clear_memory_data(antfly: &Arc<Antfly>, version: &str) -> anyhow::Result<()> {
    let _guard = antfly.lock().await;
    let documents = antfly
        .scan(ScanRequest::prefix(&s1_prefix(version)))
        .await
        .map_err(internal)?;
    let mut writes: Vec<Write> = documents
        .into_iter()
        .map(|document| Write::delete(document.key))
        .collect();
    writes.push(Write::delete(phase2_key(version)));
    writes.push(Write::put(
        progress_key(version),
        serde_json::to_value(ConsolidationProgress::default())?,
    ));
    antfly.write(writes).await.map_err(internal)
}

pub(crate) async fn record_stage1_output_usage(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_ids: &[ThreadId],
) -> anyhow::Result<usize> {
    if thread_ids.is_empty() {
        return Ok(0);
    }
    let now = Utc::now().timestamp();
    let _guard = antfly.lock().await;
    let mut updated = 0usize;
    let mut writes = Vec::new();
    for &thread_id in thread_ids {
        let mut record = get_record(antfly, version, thread_id).await?;
        let Some(output) = record.output.as_mut() else {
            continue;
        };
        output.usage_count += 1;
        output.last_usage = Some(now);
        writes.push(Write::put(
            s1_key(version, thread_id),
            serde_json::to_value(&record)?,
        ));
        updated += 1;
    }
    if !writes.is_empty() {
        antfly.write(writes).await.map_err(internal)?;
    }
    Ok(updated)
}

async fn stage1_source_needs_update(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    source_updated_at: i64,
) -> anyhow::Result<bool> {
    let record = get_record(antfly, version, thread_id).await?;
    if let Some(output) = &record.output
        && output.source_updated_at >= source_updated_at
    {
        return Ok(false);
    }
    if let Some(job) = &record.job
        && job
            .last_success_watermark
            .is_some_and(|watermark| watermark >= source_updated_at)
    {
        return Ok(false);
    }
    Ok(true)
}

/// Best-effort equivalent of the SQLite `threads` scan in
/// `claim_stage1_jobs_for_startup`. See `thread_adapter` module docs: the
/// source-discriminant match is approximate since `SessionSource`'s exact
/// JSON shape is not shared with this crate.
pub(crate) async fn claim_stage1_jobs_for_startup(
    antfly: &Arc<Antfly>,
    version: &str,
    current_thread_id: ThreadId,
    params: Stage1StartupClaimParams<'_>,
) -> anyhow::Result<Vec<Stage1JobClaim>> {
    let Stage1StartupClaimParams {
        scan_limit,
        max_claimed,
        max_age_days,
        min_rollout_idle_hours,
        allowed_sources,
        lease_seconds,
    } = params;
    if scan_limit == 0 || max_claimed == 0 {
        return Ok(Vec::new());
    }
    let max_age_cutoff = Utc::now() - Duration::days(max_age_days.max(0));
    let idle_cutoff = Utc::now() - Duration::hours(min_rollout_idle_hours.max(0));

    let mut candidates = thread_adapter::scan_all_threads(antfly)
        .await?
        .into_iter()
        .filter(|view| view.archived_at.is_none())
        .filter(|view| view.thread_id != current_thread_id)
        .filter(|view| {
            allowed_sources.is_empty() || allowed_sources.iter().any(|s| s == &view.source)
        })
        .filter(|view| view.updated_at >= max_age_cutoff && view.updated_at <= idle_cutoff)
        .collect::<Vec<_>>();
    candidates.sort_by_key(|view| std::cmp::Reverse(view.updated_at));
    candidates.truncate(scan_limit);

    let mut claimed = Vec::new();
    for view in candidates {
        if claimed.len() >= max_claimed {
            break;
        }
        if !thread_adapter::is_memory_mode_enabled(antfly, view.thread_id).await? {
            continue;
        }
        let source_updated_at = epoch(view.updated_at);
        if !stage1_source_needs_update(antfly, version, view.thread_id, source_updated_at).await? {
            continue;
        }
        if let Stage1JobClaimOutcome::Claimed { ownership_token } = try_claim_stage1_job(
            antfly,
            version,
            view.thread_id,
            current_thread_id,
            source_updated_at,
            lease_seconds,
            max_claimed,
        )
        .await?
        {
            let Some(metadata) = load_thread_metadata(antfly, view.thread_id).await? else {
                continue;
            };
            claimed.push(Stage1JobClaim {
                thread: metadata,
                ownership_token,
            });
        }
    }
    Ok(claimed)
}

/// Builds the `ThreadMetadata` fields `Stage1JobClaim`/`Stage1Output`
/// actually read (`id`, `rollout_path`, `cwd`, `updated_at`, `git_branch`);
/// other `ThreadMetadata` fields are best-effort defaults since callers of
/// this path (the memory extraction pipeline) only read those.
async fn load_thread_metadata(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<Option<crate::ThreadMetadata>> {
    thread_adapter::get_thread_metadata(antfly, thread_id).await
}

pub(crate) async fn delete_thread_memory(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
) -> anyhow::Result<()> {
    let now = Utc::now().timestamp();
    let _guard = antfly.lock().await;
    let record = get_record(antfly, version, thread_id).await?;
    let was_selected = record
        .output
        .as_ref()
        .is_some_and(|output| output.selected_for_phase2);
    let had_output = record.output.is_some();
    let mut writes = vec![Write::delete(s1_key(version, thread_id))];
    if had_output && was_selected {
        writes.extend(enqueue_global_consolidation_writes(antfly, version, now).await?);
    }
    antfly.write(writes).await.map_err(internal)
}

pub(crate) async fn list_stage1_outputs_for_global(
    antfly: &Arc<Antfly>,
    version: &str,
    n: usize,
) -> anyhow::Result<Vec<Stage1Output>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let documents = antfly
        .scan(ScanRequest::prefix(&s1_prefix(version)))
        .await
        .map_err(internal)?;
    let mut rows: Vec<(ThreadId, Stage1OutputDoc)> = Vec::new();
    for document in documents {
        let Some(thread_id) = thread_id_from_s1_key(version, &document.key) else {
            continue;
        };
        let record: Stage1Record =
            serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
        let Some(output) = record.output else {
            continue;
        };
        if output.raw_memory.trim().is_empty() && output.rollout_summary.trim().is_empty() {
            continue;
        }
        rows.push((thread_id, output));
    }
    rows.sort_by(|(a_id, a), (b_id, b)| {
        b.source_updated_at
            .cmp(&a.source_updated_at)
            .then_with(|| b_id.to_string().cmp(&a_id.to_string()))
    });

    let mut outputs = Vec::new();
    for (thread_id, output) in rows {
        if let Some(stage1) = stage1_output_if_thread_enabled(antfly, thread_id, &output).await? {
            outputs.push(stage1);
            if outputs.len() >= n {
                break;
            }
        }
    }
    Ok(outputs)
}

fn thread_id_from_s1_key(version: &str, key: &str) -> Option<ThreadId> {
    key.strip_prefix(&s1_prefix(version))
        .and_then(|id| ThreadId::from_string(id).ok())
}

async fn stage1_output_if_thread_enabled(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    output: &Stage1OutputDoc,
) -> anyhow::Result<Option<Stage1Output>> {
    if !thread_adapter::is_memory_mode_enabled(antfly, thread_id).await? {
        return Ok(None);
    }
    let Some(view) = thread_adapter::load_thread_view(antfly, thread_id).await? else {
        return Ok(None);
    };
    Ok(Some(Stage1Output {
        thread_id,
        rollout_path: std::path::PathBuf::new(),
        source_updated_at: from_epoch(output.source_updated_at)?,
        raw_memory: output.raw_memory.clone(),
        rollout_summary: output.rollout_summary.clone(),
        rollout_slug: output.rollout_slug.clone(),
        cwd: view.cwd,
        git_branch: view.git_branch,
        generated_at: from_epoch(output.generated_at)?,
    }))
}

pub(crate) async fn prune_stage1_outputs_for_retention(
    antfly: &Arc<Antfly>,
    version: &str,
    max_unused_days: i64,
    limit: usize,
) -> anyhow::Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
    let _guard = antfly.lock().await;
    let documents = antfly
        .scan(ScanRequest::prefix(&s1_prefix(version)))
        .await
        .map_err(internal)?;
    let mut candidates: Vec<(ThreadId, i64)> = Vec::new();
    for document in documents {
        let Some(thread_id) = thread_id_from_s1_key(version, &document.key) else {
            continue;
        };
        let record: Stage1Record =
            serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
        let Some(output) = record.output else {
            continue;
        };
        if output.selected_for_phase2 {
            continue;
        }
        let recency = output.last_usage.unwrap_or(output.source_updated_at);
        if recency < cutoff {
            candidates.push((thread_id, recency));
        }
    }
    candidates.sort_by_key(|(id, recency)| (*recency, id.to_string()));
    candidates.truncate(limit);
    let writes: Vec<Write> = candidates
        .iter()
        .map(|(id, _)| Write::delete(s1_key(version, *id)))
        .collect();
    let pruned = writes.len();
    if !writes.is_empty() {
        antfly.write(writes).await.map_err(internal)?;
    }
    Ok(pruned)
}

pub(crate) async fn get_phase2_input_selection(
    antfly: &Arc<Antfly>,
    version: &str,
    n: usize,
    max_unused_days: i64,
) -> anyhow::Result<Vec<Stage1Output>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
    let documents = antfly
        .scan(ScanRequest::prefix(&s1_prefix(version)))
        .await
        .map_err(internal)?;
    let mut candidates: Vec<(ThreadId, Stage1OutputDoc)> = Vec::new();
    for document in documents {
        let Some(thread_id) = thread_id_from_s1_key(version, &document.key) else {
            continue;
        };
        let record: Stage1Record =
            serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
        let Some(output) = record.output else {
            continue;
        };
        if output.raw_memory.trim().is_empty() && output.rollout_summary.trim().is_empty() {
            continue;
        }
        let eligible = match output.last_usage {
            Some(last_usage) => last_usage >= cutoff,
            None => output.source_updated_at >= cutoff,
        };
        if eligible {
            candidates.push((thread_id, output));
        }
    }
    candidates.sort_by(|(a_id, a), (b_id, b)| {
        b.usage_count
            .cmp(&a.usage_count)
            .then_with(|| {
                b.last_usage
                    .unwrap_or(b.source_updated_at)
                    .cmp(&a.last_usage.unwrap_or(a.source_updated_at))
            })
            .then_with(|| b.source_updated_at.cmp(&a.source_updated_at))
            .then_with(|| b_id.to_string().cmp(&a_id.to_string()))
    });
    // Unlike the SQLite implementation's `LIMIT ... OFFSET` paging loop
    // (needed there to bound per-page work while probing the state DB for
    // each candidate), this backend already has every candidate in memory
    // after one scan, so it simply walks the fully sorted list until `n`
    // thread-enabled outputs are found.

    let mut selected = Vec::new();
    for (thread_id, output) in candidates {
        if let Some(stage1) = stage1_output_if_thread_enabled(antfly, thread_id, &output).await? {
            selected.push(stage1);
            if selected.len() >= n {
                break;
            }
        }
    }
    selected.sort_by_key(|output| output.thread_id.to_string());
    Ok(selected)
}

pub(crate) async fn try_claim_stage1_job(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    worker_id: ThreadId,
    source_updated_at: i64,
    lease_seconds: i64,
    max_running_jobs: usize,
) -> anyhow::Result<Stage1JobClaimOutcome> {
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let ownership_token = Uuid::new_v4().to_string();

    let _guard = antfly.lock().await;
    let mut record = get_record(antfly, version, thread_id).await?;
    if let Some(output) = &record.output
        && output.source_updated_at >= source_updated_at
    {
        return Ok(Stage1JobClaimOutcome::SkippedUpToDate);
    }
    if let Some(job) = &record.job
        && job
            .last_success_watermark
            .is_some_and(|watermark| watermark >= source_updated_at)
    {
        return Ok(Stage1JobClaimOutcome::SkippedUpToDate);
    }

    // Global running-job cap across every other thread's stage-1 job,
    // matching the SQLite `(SELECT COUNT(*) FROM jobs WHERE kind=...
    // AND status='running' AND lease_until > now AND job_key != excluded.
    // job_key) < max_running_jobs` guard.
    let running_excluding_self =
        count_running_stage1_jobs(antfly, version, now, Some(thread_id)).await?;

    // Mirrors the SQLite upsert's combined `WHERE` (three ORed escape
    // hatches, each ANDed together): not already running with a fresh
    // lease; no active retry backoff (unless the watermark advanced); and
    // retries remain (unless the watermark advanced).
    let existing_watermark = record
        .job
        .as_ref()
        .and_then(|job| job.input_watermark)
        .unwrap_or(-1);
    let watermark_advanced = source_updated_at > existing_watermark;
    let not_running = record.job.as_ref().is_none_or(|job| {
        job.status != "running" || job.lease_until.is_none_or(|until| until <= now)
    });
    let no_backoff = record
        .job
        .as_ref()
        .is_none_or(|job| job.retry_at.is_none_or(|retry_at| retry_at <= now))
        || watermark_advanced;
    let retries_remain = record
        .job
        .as_ref()
        .is_none_or(|job| job.retry_remaining > 0)
        || watermark_advanced;
    let under_cap = running_excluding_self < max_running_jobs as i64;

    if !(not_running && no_backoff && retries_remain && under_cap) {
        // Claim failed: categorize the skip reason in the same priority
        // order the SQLite fallback `SELECT` uses.
        return Ok(match &record.job {
            Some(job) if job.retry_remaining <= 0 => Stage1JobClaimOutcome::SkippedRetryExhausted,
            Some(job) if job.retry_at.is_some_and(|retry_at| retry_at > now) => {
                Stage1JobClaimOutcome::SkippedRetryBackoff
            }
            Some(job)
                if job.status == "running" && job.lease_until.is_some_and(|until| until > now) =>
            {
                Stage1JobClaimOutcome::SkippedRunning
            }
            _ => Stage1JobClaimOutcome::SkippedRunning,
        });
    }

    let retry_remaining = if watermark_advanced {
        DEFAULT_RETRY_REMAINING
    } else {
        record
            .job
            .as_ref()
            .map(|job| job.retry_remaining)
            .unwrap_or(DEFAULT_RETRY_REMAINING)
    };
    record.job = Some(JobState {
        status: "running".to_string(),
        worker_id: Some(worker_id.to_string()),
        ownership_token: Some(ownership_token.clone()),
        started_at: Some(now),
        finished_at: None,
        lease_until: Some(lease_until),
        retry_at: None,
        retry_remaining,
        last_error: None,
        input_watermark: Some(source_updated_at),
        last_success_watermark: record
            .job
            .as_ref()
            .and_then(|job| job.last_success_watermark),
    });
    put_record(antfly, version, thread_id, &record).await?;
    Ok(Stage1JobClaimOutcome::Claimed { ownership_token })
}

async fn count_running_stage1_jobs(
    antfly: &Arc<Antfly>,
    version: &str,
    now: i64,
    exclude: Option<ThreadId>,
) -> anyhow::Result<i64> {
    let documents = antfly
        .scan(ScanRequest::prefix(&s1_prefix(version)))
        .await
        .map_err(internal)?;
    let mut count = 0i64;
    for document in documents {
        if exclude
            .is_some_and(|excluded| thread_id_from_s1_key(version, &document.key) == Some(excluded))
        {
            continue;
        }
        let record: Stage1Record =
            serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
        if let Some(job) = record.job
            && job.status == "running"
            && job.lease_until.is_some_and(|until| until > now)
        {
            count += 1;
        }
    }
    Ok(count)
}

// Mirrors `MemoryStore::mark_stage1_job_succeeded`'s SQLite signature exactly.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn mark_stage1_job_succeeded(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    ownership_token: &str,
    source_updated_at: i64,
    raw_memory: &str,
    rollout_summary: &str,
    rollout_slug: Option<&str>,
) -> anyhow::Result<bool> {
    let now = Utc::now().timestamp();
    let _guard = antfly.lock().await;
    let mut record = get_record(antfly, version, thread_id).await?;
    let Some(job) = record.job.as_mut() else {
        return Ok(false);
    };
    if job.status != "running" || job.ownership_token.as_deref() != Some(ownership_token) {
        return Ok(false);
    }
    job.status = "done".to_string();
    job.finished_at = Some(now);
    job.lease_until = None;
    job.last_error = None;
    job.last_success_watermark = job.input_watermark;

    let replace_output = record
        .output
        .as_ref()
        .is_none_or(|existing| source_updated_at >= existing.source_updated_at);
    if replace_output {
        let selected_for_phase2 = record
            .output
            .as_ref()
            .is_some_and(|existing| existing.selected_for_phase2);
        let selected_for_phase2_source_updated_at = record
            .output
            .as_ref()
            .and_then(|existing| existing.selected_for_phase2_source_updated_at);
        let usage_count = record
            .output
            .as_ref()
            .map(|existing| existing.usage_count)
            .unwrap_or(0);
        let last_usage = record
            .output
            .as_ref()
            .and_then(|existing| existing.last_usage);
        record.output = Some(Stage1OutputDoc {
            source_updated_at,
            raw_memory: raw_memory.to_string(),
            rollout_summary: rollout_summary.to_string(),
            rollout_slug: rollout_slug.map(str::to_string),
            generated_at: now,
            usage_count,
            last_usage,
            selected_for_phase2,
            selected_for_phase2_source_updated_at,
        });
    }
    let mut writes = vec![Write::put(
        s1_key(version, thread_id),
        serde_json::to_value(&record)?,
    )];
    writes.extend(enqueue_global_consolidation_writes(antfly, version, source_updated_at).await?);
    antfly.write(writes).await.map_err(internal)?;
    Ok(true)
}

pub(crate) async fn mark_stage1_job_succeeded_no_output(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    ownership_token: &str,
) -> anyhow::Result<bool> {
    let now = Utc::now().timestamp();
    let _guard = antfly.lock().await;
    let mut record = get_record(antfly, version, thread_id).await?;
    let Some(job) = record.job.as_mut() else {
        return Ok(false);
    };
    if job.status != "running" || job.ownership_token.as_deref() != Some(ownership_token) {
        return Ok(false);
    }
    job.status = "done".to_string();
    job.finished_at = Some(now);
    job.lease_until = None;
    job.last_error = None;
    job.last_success_watermark = job.input_watermark;
    let source_updated_at = job.input_watermark.unwrap_or(now);
    let had_output = record.output.take().is_some();

    let mut writes = vec![Write::put(
        s1_key(version, thread_id),
        serde_json::to_value(&record)?,
    )];
    if had_output {
        writes
            .extend(enqueue_global_consolidation_writes(antfly, version, source_updated_at).await?);
    }
    antfly.write(writes).await.map_err(internal)?;
    Ok(true)
}

pub(crate) async fn mark_stage1_job_failed(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    ownership_token: &str,
    failure_reason: &str,
    retry_delay_seconds: i64,
) -> anyhow::Result<bool> {
    let now = Utc::now().timestamp();
    let retry_at = now.saturating_add(retry_delay_seconds.max(0));
    let _guard = antfly.lock().await;
    let mut record = get_record(antfly, version, thread_id).await?;
    let Some(job) = record.job.as_mut() else {
        return Ok(false);
    };
    if job.status != "running" || job.ownership_token.as_deref() != Some(ownership_token) {
        return Ok(false);
    }
    job.status = "error".to_string();
    job.finished_at = Some(now);
    job.lease_until = None;
    job.retry_at = Some(retry_at);
    job.retry_remaining -= 1;
    job.last_error = Some(failure_reason.to_string());
    put_record(antfly, version, thread_id, &record).await?;
    Ok(true)
}

async fn enqueue_global_consolidation_writes(
    antfly: &Arc<Antfly>,
    version: &str,
    input_watermark: i64,
) -> anyhow::Result<Vec<Write>> {
    let mut job: JobState = antfly
        .get_as(phase2_key(version))
        .await
        .map_err(internal)?
        .unwrap_or_default();
    if job.status != "running" {
        job.status = "pending".to_string();
    }
    // Bookkeeping only (mirrors the SQLite upsert): phase 2 does not use
    // this watermark as a dirty check, so it is fine for it to not be
    // monotonic across concurrent callers racing outside the lock.
    job.input_watermark = Some(input_watermark.max(job.input_watermark.unwrap_or(i64::MIN)));
    Ok(vec![Write::put(
        phase2_key(version),
        serde_json::to_value(&job)?,
    )])
}

pub(crate) async fn enqueue_global_consolidation(
    antfly: &Arc<Antfly>,
    version: &str,
    input_watermark: i64,
) -> anyhow::Result<()> {
    let _guard = antfly.lock().await;
    let writes = enqueue_global_consolidation_writes(antfly, version, input_watermark).await?;
    antfly.write(writes).await.map_err(internal)
}

pub(crate) async fn try_claim_global_phase2_job(
    antfly: &Arc<Antfly>,
    version: &str,
    worker_id: ThreadId,
    lease_seconds: i64,
) -> anyhow::Result<Phase2JobClaimOutcome> {
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let cooldown_cutoff = now.saturating_sub(PHASE2_SUCCESS_COOLDOWN_SECONDS);
    let ownership_token = Uuid::new_v4().to_string();

    let _guard = antfly.lock().await;
    let existing: Option<JobState> = antfly.get_as(phase2_key(version)).await.map_err(internal)?;
    let Some(mut job) = existing else {
        let job = JobState {
            status: "running".to_string(),
            worker_id: Some(worker_id.to_string()),
            ownership_token: Some(ownership_token.clone()),
            started_at: Some(now),
            finished_at: None,
            lease_until: Some(lease_until),
            retry_at: None,
            retry_remaining: DEFAULT_RETRY_REMAINING,
            last_error: None,
            input_watermark: Some(0),
            last_success_watermark: Some(0),
        };
        antfly
            .write(vec![Write::put(
                phase2_key(version),
                serde_json::to_value(&job)?,
            )])
            .await
            .map_err(internal)?;
        return Ok(Phase2JobClaimOutcome::Claimed {
            ownership_token,
            input_watermark: 0,
        });
    };

    let input_watermark_value = job.input_watermark.unwrap_or(0);
    if job.retry_at.is_some_and(|retry_at| retry_at > now) {
        return Ok(Phase2JobClaimOutcome::SkippedRetryUnavailable);
    }
    if job.status == "running" && job.lease_until.is_some_and(|until| until > now) {
        return Ok(Phase2JobClaimOutcome::SkippedRunning);
    }
    if job.last_error.is_none()
        && job
            .finished_at
            .is_some_and(|finished_at| finished_at > cooldown_cutoff)
    {
        return Ok(Phase2JobClaimOutcome::SkippedCooldown);
    }

    job.status = "running".to_string();
    job.worker_id = Some(worker_id.to_string());
    job.ownership_token = Some(ownership_token.clone());
    job.started_at = Some(now);
    job.finished_at = None;
    job.lease_until = Some(lease_until);
    job.retry_at = None;
    job.last_error = None;
    antfly
        .write(vec![Write::put(
            phase2_key(version),
            serde_json::to_value(&job)?,
        )])
        .await
        .map_err(internal)?;
    Ok(Phase2JobClaimOutcome::Claimed {
        ownership_token,
        input_watermark: input_watermark_value,
    })
}

pub(crate) async fn heartbeat_global_phase2_job(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    lease_seconds: i64,
) -> anyhow::Result<bool> {
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let _guard = antfly.lock().await;
    let Some(mut job) = antfly
        .get_as::<JobState>(phase2_key(version))
        .await
        .map_err(internal)?
    else {
        return Ok(false);
    };
    if job.status != "running" || job.ownership_token.as_deref() != Some(ownership_token) {
        return Ok(false);
    }
    job.lease_until = Some(lease_until);
    antfly
        .write(vec![Write::put(
            phase2_key(version),
            serde_json::to_value(&job)?,
        )])
        .await
        .map_err(internal)?;
    Ok(true)
}

pub(crate) async fn mark_global_phase2_job_succeeded(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    completed_watermark: i64,
    selected_outputs: &[Stage1Output],
) -> anyhow::Result<bool> {
    let now = Utc::now().timestamp();
    let _guard = antfly.lock().await;
    let Some(mut job) = antfly
        .get_as::<JobState>(phase2_key(version))
        .await
        .map_err(internal)?
    else {
        return Ok(false);
    };
    if job.status != "running" || job.ownership_token.as_deref() != Some(ownership_token) {
        return Ok(false);
    }
    job.status = "done".to_string();
    job.finished_at = Some(now);
    job.lease_until = None;
    job.last_error = None;
    job.last_success_watermark = Some(
        job.last_success_watermark
            .unwrap_or(0)
            .max(completed_watermark),
    );

    let selected: std::collections::HashMap<ThreadId, i64> = selected_outputs
        .iter()
        .map(|output| (output.thread_id, output.source_updated_at.timestamp()))
        .collect();

    let documents = antfly
        .scan(ScanRequest::prefix(&s1_prefix(version)))
        .await
        .map_err(internal)?;
    let mut writes = vec![Write::put(phase2_key(version), serde_json::to_value(&job)?)];
    for document in documents {
        let Some(thread_id) = thread_id_from_s1_key(version, &document.key) else {
            continue;
        };
        let mut record: Stage1Record =
            serde_json::from_value(codex_antfly::strip_reserved(document.doc))?;
        let Some(output) = record.output.as_mut() else {
            continue;
        };
        let new_selected = selected.get(&thread_id).copied();
        let should_write = output.selected_for_phase2
            || output.selected_for_phase2_source_updated_at.is_some()
            || new_selected.is_some();
        if !should_write {
            continue;
        }
        output.selected_for_phase2 = new_selected.is_some();
        output.selected_for_phase2_source_updated_at = new_selected;
        writes.push(Write::put(
            s1_key(version, thread_id),
            serde_json::to_value(&record)?,
        ));
    }

    let mut progress: ConsolidationProgress = antfly
        .get_as(progress_key(version))
        .await
        .map_err(internal)?
        .unwrap_or_default();
    progress.max_thread_count = progress.max_thread_count.max(selected_outputs.len() as u32);
    writes.push(Write::put(
        progress_key(version),
        serde_json::to_value(&progress)?,
    ));

    antfly.write(writes).await.map_err(internal)?;
    Ok(true)
}

pub(crate) async fn mark_global_phase2_job_failed(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    failure_reason: &str,
    retry_delay_seconds: i64,
) -> anyhow::Result<bool> {
    mark_global_phase2_job_failed_impl(
        antfly,
        version,
        Some(ownership_token),
        failure_reason,
        retry_delay_seconds,
    )
    .await
}

pub(crate) async fn mark_global_phase2_job_failed_if_unowned(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    failure_reason: &str,
    retry_delay_seconds: i64,
) -> anyhow::Result<bool> {
    let _guard = antfly.lock().await;
    let Some(mut job) = antfly
        .get_as::<JobState>(phase2_key(version))
        .await
        .map_err(internal)?
    else {
        return Ok(false);
    };
    let owned_or_unowned =
        job.ownership_token.as_deref() == Some(ownership_token) || job.ownership_token.is_none();
    if job.status != "running" || !owned_or_unowned {
        return Ok(false);
    }
    apply_phase2_failure(&mut job, failure_reason, retry_delay_seconds);
    antfly
        .write(vec![Write::put(
            phase2_key(version),
            serde_json::to_value(&job)?,
        )])
        .await
        .map_err(internal)?;
    Ok(true)
}

async fn mark_global_phase2_job_failed_impl(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: Option<&str>,
    failure_reason: &str,
    retry_delay_seconds: i64,
) -> anyhow::Result<bool> {
    let _guard = antfly.lock().await;
    let Some(mut job) = antfly
        .get_as::<JobState>(phase2_key(version))
        .await
        .map_err(internal)?
    else {
        return Ok(false);
    };
    if job.status != "running" || job.ownership_token.as_deref() != ownership_token {
        return Ok(false);
    }
    apply_phase2_failure(&mut job, failure_reason, retry_delay_seconds);
    antfly
        .write(vec![Write::put(
            phase2_key(version),
            serde_json::to_value(&job)?,
        )])
        .await
        .map_err(internal)?;
    Ok(true)
}

fn apply_phase2_failure(job: &mut JobState, failure_reason: &str, retry_delay_seconds: i64) {
    let now = Utc::now().timestamp();
    job.status = "error".to_string();
    job.finished_at = Some(now);
    job.lease_until = None;
    job.retry_at = Some(now.saturating_add(retry_delay_seconds.max(0)));
    job.retry_remaining = (job.retry_remaining - 1).max(0);
    job.last_error = Some(failure_reason.to_string());
}

pub(crate) async fn max_consolidated_thread_count(
    antfly: &Arc<Antfly>,
    version: &str,
) -> anyhow::Result<u32> {
    let progress: ConsolidationProgress = antfly
        .get_as(progress_key(version))
        .await
        .map_err(internal)?
        .unwrap_or_default();
    Ok(progress.max_thread_count)
}

/// Marks a thread polluted (via `thread_adapter`'s presence marker) and
/// enqueues phase-2 forgetting when the thread participated in the last
/// successful phase-2 baseline — regardless of whether the marker itself
/// transitioned, mirroring the SQLite implementation exactly (it checks
/// `selected_for_phase2` before the `UPDATE ... WHERE memory_mode !=
/// 'polluted'` and enqueues unconditionally on that earlier read).
pub(crate) async fn mark_thread_memory_mode_polluted(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let now = Utc::now().timestamp();
    let record = get_record(antfly, version, thread_id).await?;
    let selected_for_phase2 = record
        .output
        .is_some_and(|output| output.selected_for_phase2);
    let changed = thread_adapter::mark_memory_mode_polluted(antfly, thread_id).await?;
    if selected_for_phase2 {
        let _guard = antfly.lock().await;
        let writes = enqueue_global_consolidation_writes(antfly, version, now).await?;
        antfly.write(writes).await.map_err(internal)?;
    }
    Ok(changed)
}
