//! Antfly backend for `MemoryStore`, backed by the `codex_stage1_outputs`,
//! `codex_jobs`, and `codex_consolidation_progress` SQL tables (see
//! `codex-antfly/src/schema.rs`, migration 10). See
//! `state/src/runtime/memories.rs` for the SQLite implementation this
//! mirrors (semantics, including the exact SQL, are ported 1:1 with `?`
//! placeholders rewritten to `$n` and `stage1_outputs`/`jobs`/
//! `consolidation_progress` rewritten to `codex_stage1_outputs`/
//! `codex_jobs`/`codex_consolidation_progress`).
//!
//! # Versioning
//!
//! `MemoryStore::new_antfly(antfly, version)` is parameterized by a
//! `"v1"`/`"v2"` tag (see `state/src/runtime/memory_versions.rs`). Under
//! SQLite, v1 and v2 are two entirely separate database files
//! (`memories_1.sqlite` / `memories_v2_1.sqlite`), so the same `thread_id`
//! in each never collides. Antfly has a single `.aflite` file shared by
//! every `MemoryStore`, so migration 10 adds a `version` column to all
//! three tables and makes it part of each primary key (`(version,
//! thread_id)`, `(version, kind, job_key)`, `version`), rather than
//! hard-coding or ignoring the version tag.
//!
//! This is not just defensive: `core/src/stream_events_utils.rs` calls
//! `StateRuntime::memories_for_version(turn_context.config.memories.version)`
//! on every completed turn and (via `record_stage1_output_usage`) writes
//! into whichever version the user's config selects, and
//! `state/src/runtime/memory_versions.rs`'s `clear_all_memory_data`/
//! `delete_versioned_thread_memory` always also touch the v2-tagged store
//! whenever Antfly is configured. So v1 and v2 stage-1 outputs and jobs are
//! both genuinely reachable and must not collide.
//!
//! # Thread lookups
//!
//! Thread state comes straight from `codex_threads`:
//! `enabled_thread_metadata`-equivalent lookups filter
//! `memory_mode = 'enabled'`, and `mark_thread_memory_mode_polluted` writes
//! `codex_threads.memory_mode` directly, matching the SQLite implementation
//! exactly. Only a handful of `ThreadMetadata` fields
//! (`id`/`rollout_path`/`cwd`/`git_branch`/`source`/`updated_at`) are
//! populated for `Stage1JobClaim`; the rest default, since that is all the
//! memory pipeline's callers read (matching the comment this backend
//! carried before this port).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::AntflyError;
use codex_antfly::AntflyResult;
use codex_antfly::sql::Sql;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;
use uuid::Uuid;

use super::internal;
use super::text_column;
use crate::Phase2JobClaimOutcome;
use crate::Stage1JobClaim;
use crate::Stage1JobClaimOutcome;
use crate::Stage1Output;
use crate::Stage1StartupClaimParams;

const JOB_KIND_MEMORY_STAGE1: &str = "memory_stage1";
const JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL: &str = "memory_consolidate_global";
const MEMORY_CONSOLIDATION_JOB_KEY: &str = "global";
const PHASE2_SUCCESS_COOLDOWN_SECONDS: i64 = 6 * 60 * 60;
const DEFAULT_RETRY_REMAINING: i64 = 3;

const MAX_RETRY_ATTEMPTS: u32 = 10;

fn is_retryable(err: &AntflyError) -> bool {
    err.is_conflict() || err.is_unique_violation()
}

type Attempt<'a, T> = Pin<Box<dyn Future<Output = AntflyResult<T>> + Send + 'a>>;

/// Runs `attempt` inside a fresh transaction, retrying the whole transaction
/// (up to [`MAX_RETRY_ATTEMPTS`] times) on a serialization conflict (40001)
/// or a unique-constraint race (23505) against a concurrent writer. Mirrors
/// `antfly_backend::queue::with_retry`.
async fn with_retry<T>(
    sql: &Sql,
    mut attempt: impl for<'a> FnMut(&'a mut SqlTx) -> Attempt<'a, T>,
) -> AntflyResult<T> {
    for _ in 0..MAX_RETRY_ATTEMPTS {
        let mut tx = sql.begin().await?;
        match attempt(&mut tx).await {
            Ok(value) => match tx.commit().await {
                Ok(()) => return Ok(value),
                Err(err) if is_retryable(&err) => continue,
                Err(err) => return Err(err),
            },
            Err(err) => {
                let _ = tx.rollback().await;
                if is_retryable(&err) {
                    continue;
                }
                return Err(err);
            }
        }
    }
    Err(AntflyError::Sql {
        code: Some("40001".to_string()),
        message: "codex_jobs/codex_stage1_outputs: exceeded retry attempts after repeated \
                   conflicts"
            .to_string(),
    })
}

fn epoch(dt: DateTime<Utc>) -> i64 {
    dt.timestamp()
}

fn from_epoch(secs: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp: {secs}"))
}

/// Builds the `ThreadMetadata` fields `Stage1JobClaim`/`Stage1Output`
/// actually read (`id`, `rollout_path`, `cwd`, `updated_at`, `git_branch`);
/// other `ThreadMetadata` fields are best-effort defaults since callers of
/// this path (the memory extraction pipeline) only read those.
fn minimal_thread_metadata(
    thread_id: ThreadId,
    rollout_path: String,
    cwd: String,
    git_branch: Option<String>,
    source: String,
    updated_at: DateTime<Utc>,
) -> crate::ThreadMetadata {
    crate::ThreadMetadata {
        originator: None,
        creator_user_id: None,
        creator_account_id: None,
        id: thread_id,
        rollout_path: std::path::PathBuf::from(rollout_path),
        created_at: updated_at,
        updated_at,
        recency_at: updated_at,
        source,
        history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        agent_path: None,
        model_provider: String::new(),
        model: None,
        reasoning_effort: None,
        cwd: std::path::PathBuf::from(cwd),
        cli_version: String::new(),
        title: String::new(),
        name: None,
        preview: None,
        sandbox_policy: String::new(),
        approval_mode: String::new(),
        tokens_used: 0,
        first_user_message: None,
        archived_at: None,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        daybreak_enabled: None,
        git_sha: None,
        git_branch,
        git_origin_url: None,
    }
}

fn stage1_output_from_joined_row(row: &SqlRow) -> anyhow::Result<Stage1Output> {
    let thread_id = ThreadId::try_from(row.string("thread_id").map_err(internal)?)?;
    let source_updated_at = from_epoch(row.i64("source_updated_at").map_err(internal)?)?;
    let generated_at = from_epoch(row.i64("generated_at").map_err(internal)?)?;
    Ok(Stage1Output {
        thread_id,
        rollout_path: std::path::PathBuf::from(row.string("rollout_path").map_err(internal)?),
        source_updated_at,
        raw_memory: text_column(row, "raw_memory").map_err(internal)?,
        rollout_summary: text_column(row, "rollout_summary").map_err(internal)?,
        rollout_slug: row.opt_string("rollout_slug").map_err(internal)?,
        cwd: std::path::PathBuf::from(row.string("cwd").map_err(internal)?),
        git_branch: row.opt_string("git_branch").map_err(internal)?,
        generated_at,
    })
}

pub(crate) async fn clear_memory_data(antfly: &Arc<Antfly>, version: &str) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let version = version.to_string();
    with_retry(sql, move |tx| {
        let version = version.clone();
        Box::pin(async move {
            tx.execute(
                "DELETE FROM codex_stage1_outputs WHERE version = $1",
                sql_params![version.clone()],
            )
            .await?;
            tx.execute(
                "DELETE FROM codex_jobs WHERE version = $1",
                sql_params![version.clone()],
            )
            .await?;
            tx.execute(
                "INSERT INTO codex_consolidation_progress (version, max_thread_count)
                 VALUES ($1, 0)
                 ON CONFLICT (version) DO UPDATE SET max_thread_count = 0",
                sql_params![version],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(internal)
}

pub(crate) async fn record_stage1_output_usage(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_ids: &[ThreadId],
) -> anyhow::Result<usize> {
    if thread_ids.is_empty() {
        return Ok(0);
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let version = version.to_string();
    let thread_ids: Vec<String> = thread_ids.iter().map(ThreadId::to_string).collect();
    let updated = with_retry(sql, move |tx| {
        let version = version.clone();
        let thread_ids = thread_ids.clone();
        Box::pin(async move {
            let mut updated = 0usize;
            for thread_id in &thread_ids {
                let rows = tx
                    .execute(
                        "UPDATE codex_stage1_outputs
                         SET usage_count = COALESCE(usage_count, 0) + 1, last_usage = $1
                         WHERE version = $2 AND thread_id = $3",
                        sql_params![now, version.clone(), thread_id.clone()],
                    )
                    .await?;
                updated += rows as usize;
            }
            Ok(updated)
        })
    })
    .await
    .map_err(internal)?;
    Ok(updated)
}

async fn stage1_source_needs_update(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    source_updated_at: i64,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let existing_output = sql
        .fetch_optional(
            "SELECT source_updated_at FROM codex_stage1_outputs WHERE version = $1 AND thread_id = $2",
            sql_params![version, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    if let Some(row) = existing_output {
        let existing = row.i64("source_updated_at").map_err(internal)?;
        if existing >= source_updated_at {
            return Ok(false);
        }
    }
    let existing_job = sql
        .fetch_optional(
            "SELECT last_success_watermark FROM codex_jobs
             WHERE version = $1 AND kind = $2 AND job_key = $3",
            sql_params![version, JOB_KIND_MEMORY_STAGE1, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    if let Some(row) = existing_job {
        let watermark = row.opt_i64("last_success_watermark").map_err(internal)?;
        if watermark.is_some_and(|watermark| watermark >= source_updated_at) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Selects and claims stage-1 startup jobs for stale threads directly from
/// `codex_threads`. See `MemoryStore::claim_stage1_jobs_for_startup` for the
/// exact filter/ordering semantics this mirrors.
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
    let sql = antfly.sql().await.map_err(internal)?;
    let worker_id = current_thread_id;
    let max_age_cutoff_ms = (Utc::now() - Duration::days(max_age_days.max(0))).timestamp_millis();
    let idle_cutoff_ms =
        (Utc::now() - Duration::hours(min_rollout_idle_hours.max(0))).timestamp_millis();

    let mut statement = String::from(
        "SELECT id, rollout_path, cwd, git_branch, updated_at_ms, source
         FROM codex_threads
         WHERE archived = false
           AND memory_mode = 'enabled'
           AND id != $1
           AND updated_at_ms >= $2
           AND updated_at_ms <= $3",
    );
    let mut query_params = sql_params![
        current_thread_id.to_string(),
        max_age_cutoff_ms,
        idle_cutoff_ms
    ];
    if !allowed_sources.is_empty() {
        statement.push_str(" AND (");
        for (index, source) in allowed_sources.iter().enumerate() {
            if index > 0 {
                statement.push_str(" OR ");
            }
            statement.push_str(&format!("source = ${}", query_params.len() + 1));
            query_params.push(source.clone().into());
        }
        statement.push(')');
    }
    statement.push_str(" ORDER BY updated_at_ms DESC LIMIT ");
    statement.push_str(&format!("${}", query_params.len() + 1));
    query_params.push(i64::try_from(scan_limit).unwrap_or(i64::MAX).into());

    let rows = sql
        .fetch_all(&statement, query_params)
        .await
        .map_err(internal)?;

    let mut claimed = Vec::new();
    for row in rows {
        if claimed.len() >= max_claimed {
            break;
        }
        let thread_id = ThreadId::try_from(row.string("id").map_err(internal)?)?;
        let updated_at_ms = row.i64("updated_at_ms").map_err(internal)?;
        let updated_at = crate::model::epoch_millis_to_datetime(updated_at_ms)?;
        let source_updated_at = epoch(updated_at);
        if !stage1_source_needs_update(antfly, version, thread_id, source_updated_at).await? {
            continue;
        }
        if let Stage1JobClaimOutcome::Claimed { ownership_token } = try_claim_stage1_job(
            antfly,
            version,
            thread_id,
            worker_id,
            source_updated_at,
            lease_seconds,
            max_claimed,
        )
        .await?
        {
            let rollout_path = row.string("rollout_path").map_err(internal)?;
            let cwd = row.string("cwd").map_err(internal)?;
            let git_branch = row.opt_string("git_branch").map_err(internal)?;
            let source = row.string("source").map_err(internal)?;
            claimed.push(Stage1JobClaim {
                thread: minimal_thread_metadata(
                    thread_id,
                    rollout_path,
                    cwd,
                    git_branch,
                    source,
                    updated_at,
                ),
                ownership_token,
            });
        }
    }
    Ok(claimed)
}

async fn enqueue_global_consolidation_tx(
    tx: &mut SqlTx,
    version: &str,
    input_watermark: i64,
) -> AntflyResult<()> {
    let existing = tx
        .fetch_optional(
            "SELECT status, retry_at, retry_remaining, input_watermark
             FROM codex_jobs WHERE version = $1 AND kind = $2 AND job_key = $3",
            sql_params![
                version,
                JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                MEMORY_CONSOLIDATION_JOB_KEY
            ],
        )
        .await?;
    match existing {
        None => {
            tx.execute(
                "INSERT INTO codex_jobs (
                    version, kind, job_key, status, retry_remaining, input_watermark,
                    last_success_watermark
                 ) VALUES ($1, $2, $3, 'pending', $4, $5, 0)",
                sql_params![
                    version,
                    JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                    MEMORY_CONSOLIDATION_JOB_KEY,
                    DEFAULT_RETRY_REMAINING,
                    input_watermark,
                ],
            )
            .await?;
        }
        Some(row) => {
            let status = row.string("status")?;
            let retry_at = row.opt_i64("retry_at")?;
            let retry_remaining = row.i64("retry_remaining")?;
            let current_watermark = row.opt_i64("input_watermark")?.unwrap_or(0);
            let new_status = if status == "running" {
                "running"
            } else {
                "pending"
            };
            let new_retry_at = if status == "running" { retry_at } else { None };
            let new_retry_remaining = retry_remaining.max(DEFAULT_RETRY_REMAINING);
            let new_watermark = if input_watermark > current_watermark {
                input_watermark
            } else {
                current_watermark + 1
            };
            tx.execute(
                "UPDATE codex_jobs
                 SET status = $1, retry_at = $2, retry_remaining = $3, input_watermark = $4
                 WHERE version = $5 AND kind = $6 AND job_key = $7",
                sql_params![
                    new_status,
                    new_retry_at,
                    new_retry_remaining,
                    new_watermark,
                    version,
                    JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                    MEMORY_CONSOLIDATION_JOB_KEY,
                ],
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn delete_thread_memory(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let version = version.to_string();
    let thread_id_str = thread_id.to_string();
    with_retry(sql, move |tx| {
        let version = version.clone();
        let thread_id_str = thread_id_str.clone();
        Box::pin(async move {
            let existing = tx
                .fetch_optional(
                    "SELECT selected_for_phase2 FROM codex_stage1_outputs
                     WHERE version = $1 AND thread_id = $2",
                    sql_params![version.clone(), thread_id_str.clone()],
                )
                .await?;
            let was_selected = existing
                .as_ref()
                .map(|row| row.bool("selected_for_phase2"))
                .transpose()?
                .unwrap_or(false);
            let deleted = tx
                .execute(
                    "DELETE FROM codex_stage1_outputs WHERE version = $1 AND thread_id = $2",
                    sql_params![version.clone(), thread_id_str.clone()],
                )
                .await?;
            tx.execute(
                "DELETE FROM codex_jobs WHERE version = $1 AND kind = $2 AND job_key = $3",
                sql_params![
                    version.clone(),
                    JOB_KIND_MEMORY_STAGE1,
                    thread_id_str.clone()
                ],
            )
            .await?;
            if deleted > 0 && was_selected {
                enqueue_global_consolidation_tx(tx, &version, now).await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(internal)
}

/// Lists the most recent non-empty stage-1 outputs for global consolidation.
/// A single join with `codex_threads` filters to enabled threads and hydrates
/// `cwd`/`rollout_path`/`git_branch`, so unlike the SQLite implementation
/// (whose `stage1_outputs` and `threads` tables live in separate database
/// files) this needs no in-process pagination/probing loop.
pub(crate) async fn list_stage1_outputs_for_global(
    antfly: &Arc<Antfly>,
    version: &str,
    n: usize,
) -> anyhow::Result<Vec<Stage1Output>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT so.thread_id, so.source_updated_at, so.raw_memory, so.rollout_summary,
                    so.rollout_slug, so.generated_at, t.rollout_path, t.cwd, t.git_branch
             FROM codex_stage1_outputs so
             JOIN codex_threads t ON t.id = so.thread_id
             WHERE so.version = $1 AND t.memory_mode = 'enabled'
               AND (trim(so.raw_memory) <> '' OR trim(so.rollout_summary) <> '')
             ORDER BY so.source_updated_at DESC, so.thread_id DESC
             LIMIT $2",
            sql_params![version, i64::try_from(n).unwrap_or(i64::MAX)],
        )
        .await
        .map_err(internal)?;
    rows.iter().map(stage1_output_from_joined_row).collect()
}

/// Prunes stale, unselected stage-1 outputs. Reads the stalest `limit`
/// candidate ids, then deletes exactly those rows (re-checking
/// `selected_for_phase2 = false` in the same statement) rather than a single
/// `DELETE ... WHERE thread_id IN (SELECT ... ORDER BY ... LIMIT ...)`,
/// since a correlated delete-with-order-by-limit subquery is unproven on the
/// embedded Antfly SQL driver.
pub(crate) async fn prune_stage1_outputs_for_retention(
    antfly: &Arc<Antfly>,
    version: &str,
    max_unused_days: i64,
    limit: usize,
) -> anyhow::Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();

    let mut tx = sql.begin().await.map_err(internal)?;
    let result = async {
        let candidates = tx
            .fetch_all(
                "SELECT thread_id FROM codex_stage1_outputs
                 WHERE version = $1 AND selected_for_phase2 = false
                   AND COALESCE(last_usage, source_updated_at) < $2
                 ORDER BY COALESCE(last_usage, source_updated_at) ASC, source_updated_at ASC,
                          thread_id ASC
                 LIMIT $3",
                sql_params![version, cutoff, i64::try_from(limit).unwrap_or(i64::MAX)],
            )
            .await?;
        if candidates.is_empty() {
            return Ok(0);
        }
        let ids = candidates
            .iter()
            .map(|row| row.string("thread_id"))
            .collect::<AntflyResult<Vec<_>>>()?;

        let mut statement = String::from(
            "DELETE FROM codex_stage1_outputs WHERE version = $1 AND selected_for_phase2 = false \
             AND thread_id IN (",
        );
        let mut params = sql_params![version];
        for (index, id) in ids.iter().enumerate() {
            if index > 0 {
                statement.push_str(", ");
            }
            statement.push_str(&format!("${}", index + 2));
            params.push(id.clone().into());
        }
        statement.push(')');
        let deleted = tx.execute(&statement, params).await?;
        Ok(deleted as usize)
    }
    .await;

    match result {
        Ok(pruned) => tx.commit().await.map_err(internal).map(|()| pruned),
        Err(err) => {
            let _ = tx.rollback().await;
            Err(internal(err))
        }
    }
}

/// Returns the current phase-2 input set. A single ranking join with
/// `codex_threads` replaces the SQLite implementation's page-at-a-time probe
/// loop (needed there only to bound per-page work while checking each
/// candidate's thread against a separate database file).
pub(crate) async fn get_phase2_input_selection(
    antfly: &Arc<Antfly>,
    version: &str,
    n: usize,
    max_unused_days: i64,
) -> anyhow::Result<Vec<Stage1Output>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
    let rows = sql
        .fetch_all(
            "SELECT so.thread_id, so.source_updated_at, so.raw_memory, so.rollout_summary,
                    so.rollout_slug, so.generated_at, t.rollout_path, t.cwd, t.git_branch
             FROM codex_stage1_outputs so
             JOIN codex_threads t ON t.id = so.thread_id
             WHERE so.version = $1 AND t.memory_mode = 'enabled'
               AND (trim(so.raw_memory) <> '' OR trim(so.rollout_summary) <> '')
               AND (
                     (so.last_usage IS NOT NULL AND so.last_usage >= $2)
                     OR (so.last_usage IS NULL AND so.source_updated_at >= $2)
               )
             ORDER BY
                 COALESCE(so.usage_count, 0) DESC,
                 COALESCE(so.last_usage, so.source_updated_at) DESC,
                 so.source_updated_at DESC,
                 so.thread_id DESC
             LIMIT $3",
            sql_params![version, cutoff, i64::try_from(n).unwrap_or(i64::MAX)],
        )
        .await
        .map_err(internal)?;
    let mut selected = rows
        .iter()
        .map(stage1_output_from_joined_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    selected.sort_by_key(|output| output.thread_id.to_string());
    Ok(selected)
}

struct ExistingStage1Job {
    status: String,
    lease_until: Option<i64>,
    retry_at: Option<i64>,
    retry_remaining: i64,
    input_watermark: Option<i64>,
}

fn existing_stage1_job_from_row(row: &SqlRow) -> AntflyResult<ExistingStage1Job> {
    Ok(ExistingStage1Job {
        status: row.string("status")?,
        lease_until: row.opt_i64("lease_until")?,
        retry_at: row.opt_i64("retry_at")?,
        retry_remaining: row.i64("retry_remaining")?,
        input_watermark: row.opt_i64("input_watermark")?,
    })
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
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let max_running_jobs = i64::try_from(max_running_jobs).unwrap_or(i64::MAX);
    let version = version.to_string();
    let thread_id_str = thread_id.to_string();
    let worker_id_str = worker_id.to_string();

    with_retry(sql, move |tx| {
        let version = version.clone();
        let thread_id_str = thread_id_str.clone();
        let worker_id_str = worker_id_str.clone();
        let ownership_token = Uuid::new_v4().to_string();
        Box::pin(async move {
            let existing_output = tx
                .fetch_optional(
                    "SELECT source_updated_at FROM codex_stage1_outputs
                     WHERE version = $1 AND thread_id = $2",
                    sql_params![version.clone(), thread_id_str.clone()],
                )
                .await?;
            if let Some(row) = &existing_output {
                let existing = row.i64("source_updated_at")?;
                if existing >= source_updated_at {
                    return Ok(Stage1JobClaimOutcome::SkippedUpToDate);
                }
            }

            let existing_job_row = tx
                .fetch_optional(
                    "SELECT status, lease_until, retry_at, retry_remaining, input_watermark,
                            last_success_watermark
                     FROM codex_jobs WHERE version = $1 AND kind = $2 AND job_key = $3",
                    sql_params![
                        version.clone(),
                        JOB_KIND_MEMORY_STAGE1,
                        thread_id_str.clone()
                    ],
                )
                .await?;
            if let Some(row) = &existing_job_row {
                let watermark = row.opt_i64("last_success_watermark")?;
                if watermark.is_some_and(|watermark| watermark >= source_updated_at) {
                    return Ok(Stage1JobClaimOutcome::SkippedUpToDate);
                }
            }
            let existing_job = existing_job_row
                .as_ref()
                .map(existing_stage1_job_from_row)
                .transpose()?;

            let running_row = tx
                .fetch_optional(
                    "SELECT COUNT(*) AS running_count FROM codex_jobs
                     WHERE version = $1 AND kind = $2 AND status = 'running'
                       AND lease_until IS NOT NULL AND lease_until > $3 AND job_key != $4",
                    sql_params![
                        version.clone(),
                        JOB_KIND_MEMORY_STAGE1,
                        now,
                        thread_id_str.clone()
                    ],
                )
                .await?;
            let running_excluding_self = running_row
                .map(|row| row.i64("running_count"))
                .transpose()?
                .unwrap_or(0);

            let existing_watermark = existing_job
                .as_ref()
                .and_then(|job| job.input_watermark)
                .unwrap_or(-1);
            let watermark_advanced = source_updated_at > existing_watermark;
            let not_running = existing_job.as_ref().is_none_or(|job| {
                job.status != "running" || job.lease_until.is_none_or(|until| until <= now)
            });
            let no_backoff = watermark_advanced
                || existing_job
                    .as_ref()
                    .is_none_or(|job| job.retry_at.is_none_or(|retry_at| retry_at <= now));
            let retries_remain = watermark_advanced
                || existing_job
                    .as_ref()
                    .is_none_or(|job| job.retry_remaining > 0);
            let under_cap = running_excluding_self < max_running_jobs;

            if !(not_running && no_backoff && retries_remain && under_cap) {
                return Ok(match &existing_job {
                    Some(job) if job.retry_remaining <= 0 => {
                        Stage1JobClaimOutcome::SkippedRetryExhausted
                    }
                    Some(job) if job.retry_at.is_some_and(|retry_at| retry_at > now) => {
                        Stage1JobClaimOutcome::SkippedRetryBackoff
                    }
                    Some(job)
                        if job.status == "running"
                            && job.lease_until.is_some_and(|until| until > now) =>
                    {
                        Stage1JobClaimOutcome::SkippedRunning
                    }
                    _ => Stage1JobClaimOutcome::SkippedRunning,
                });
            }

            let retry_remaining = if watermark_advanced {
                DEFAULT_RETRY_REMAINING
            } else {
                existing_job
                    .as_ref()
                    .map(|job| job.retry_remaining)
                    .unwrap_or(DEFAULT_RETRY_REMAINING)
            };

            tx.execute(
                "INSERT INTO codex_jobs (
                    version, kind, job_key, status, worker_id, ownership_token,
                    started_at, finished_at, lease_until, retry_at, retry_remaining,
                    last_error, input_watermark, last_success_watermark
                 ) VALUES ($1, $2, $3, 'running', $4, $5, $6, NULL, $7, NULL, $8, NULL, $9, NULL)
                 ON CONFLICT (version, kind, job_key) DO UPDATE SET
                     status = 'running',
                     worker_id = excluded.worker_id,
                     ownership_token = excluded.ownership_token,
                     started_at = excluded.started_at,
                     finished_at = NULL,
                     lease_until = excluded.lease_until,
                     retry_at = NULL,
                     retry_remaining = excluded.retry_remaining,
                     last_error = NULL,
                     input_watermark = excluded.input_watermark",
                sql_params![
                    version,
                    JOB_KIND_MEMORY_STAGE1,
                    thread_id_str,
                    worker_id_str,
                    ownership_token.clone(),
                    now,
                    lease_until,
                    retry_remaining,
                    source_updated_at,
                ],
            )
            .await?;

            Ok(Stage1JobClaimOutcome::Claimed { ownership_token })
        })
    })
    .await
    .map_err(internal)
}

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
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let version = version.to_string();
    let thread_id_str = thread_id.to_string();
    let ownership_token = ownership_token.to_string();
    let raw_memory = raw_memory.to_string();
    let rollout_summary = rollout_summary.to_string();
    let rollout_slug = rollout_slug.map(str::to_string);

    with_retry(sql, move |tx| {
        let version = version.clone();
        let thread_id_str = thread_id_str.clone();
        let ownership_token = ownership_token.clone();
        let raw_memory = raw_memory.clone();
        let rollout_summary = rollout_summary.clone();
        let rollout_slug = rollout_slug.clone();
        Box::pin(async move {
            let rows = tx
                .execute(
                    "UPDATE codex_jobs
                     SET status = 'done', finished_at = $1, lease_until = NULL, last_error = NULL,
                         last_success_watermark = input_watermark
                     WHERE version = $2 AND kind = $3 AND job_key = $4
                       AND status = 'running' AND ownership_token = $5",
                    sql_params![
                        now,
                        version.clone(),
                        JOB_KIND_MEMORY_STAGE1,
                        thread_id_str.clone(),
                        ownership_token,
                    ],
                )
                .await?;
            if rows == 0 {
                return Ok(false);
            }

            tx.execute(
                "INSERT INTO codex_stage1_outputs (
                    version, thread_id, source_updated_at, raw_memory, rollout_summary,
                    rollout_slug, generated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (version, thread_id) DO UPDATE SET
                     source_updated_at = excluded.source_updated_at,
                     raw_memory = excluded.raw_memory,
                     rollout_summary = excluded.rollout_summary,
                     rollout_slug = excluded.rollout_slug,
                     generated_at = excluded.generated_at
                 WHERE excluded.source_updated_at >= codex_stage1_outputs.source_updated_at",
                sql_params![
                    version.clone(),
                    thread_id_str,
                    source_updated_at,
                    raw_memory,
                    rollout_summary,
                    rollout_slug,
                    now,
                ],
            )
            .await?;

            enqueue_global_consolidation_tx(tx, &version, source_updated_at).await?;
            Ok(true)
        })
    })
    .await
    .map_err(internal)
}

pub(crate) async fn mark_stage1_job_succeeded_no_output(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    ownership_token: &str,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let version = version.to_string();
    let thread_id_str = thread_id.to_string();
    let ownership_token = ownership_token.to_string();

    with_retry(sql, move |tx| {
        let version = version.clone();
        let thread_id_str = thread_id_str.clone();
        let ownership_token = ownership_token.clone();
        Box::pin(async move {
            let row = tx
                .fetch_optional(
                    "UPDATE codex_jobs
                     SET status = 'done', finished_at = $1, lease_until = NULL, last_error = NULL,
                         last_success_watermark = input_watermark
                     WHERE version = $2 AND kind = $3 AND job_key = $4
                       AND status = 'running' AND ownership_token = $5
                     RETURNING input_watermark",
                    sql_params![
                        now,
                        version.clone(),
                        JOB_KIND_MEMORY_STAGE1,
                        thread_id_str.clone(),
                        ownership_token,
                    ],
                )
                .await?;
            let Some(row) = row else {
                return Ok(false);
            };
            let source_updated_at = row.opt_i64("input_watermark")?.unwrap_or(now);

            let deleted = tx
                .execute(
                    "DELETE FROM codex_stage1_outputs WHERE version = $1 AND thread_id = $2",
                    sql_params![version.clone(), thread_id_str],
                )
                .await?;
            if deleted > 0 {
                enqueue_global_consolidation_tx(tx, &version, source_updated_at).await?;
            }
            Ok(true)
        })
    })
    .await
    .map_err(internal)
}

pub(crate) async fn mark_stage1_job_failed(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
    ownership_token: &str,
    failure_reason: &str,
    retry_delay_seconds: i64,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let retry_at = now.saturating_add(retry_delay_seconds.max(0));
    let rows = sql
        .execute(
            "UPDATE codex_jobs
             SET status = 'error', finished_at = $1, lease_until = NULL, retry_at = $2,
                 retry_remaining = retry_remaining - 1, last_error = $3
             WHERE version = $4 AND kind = $5 AND job_key = $6
               AND status = 'running' AND ownership_token = $7",
            sql_params![
                now,
                retry_at,
                failure_reason,
                version,
                JOB_KIND_MEMORY_STAGE1,
                thread_id.to_string(),
                ownership_token,
            ],
        )
        .await
        .map_err(internal)?;
    Ok(rows > 0)
}

pub(crate) async fn enqueue_global_consolidation(
    antfly: &Arc<Antfly>,
    version: &str,
    input_watermark: i64,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    let version = version.to_string();
    with_retry(sql, move |tx| {
        let version = version.clone();
        Box::pin(
            async move { enqueue_global_consolidation_tx(tx, &version, input_watermark).await },
        )
    })
    .await
    .map_err(internal)
}

pub(crate) async fn try_claim_global_phase2_job(
    antfly: &Arc<Antfly>,
    version: &str,
    worker_id: ThreadId,
    lease_seconds: i64,
) -> anyhow::Result<Phase2JobClaimOutcome> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let cooldown_cutoff = now.saturating_sub(PHASE2_SUCCESS_COOLDOWN_SECONDS);
    let version = version.to_string();
    let worker_id_str = worker_id.to_string();

    with_retry(sql, move |tx| {
        let version = version.clone();
        let worker_id_str = worker_id_str.clone();
        let ownership_token = Uuid::new_v4().to_string();
        Box::pin(async move {
            let existing_row = tx
                .fetch_optional(
                    "SELECT status, lease_until, retry_at, input_watermark, finished_at, last_error
                     FROM codex_jobs WHERE version = $1 AND kind = $2 AND job_key = $3",
                    sql_params![
                        version.clone(),
                        JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                        MEMORY_CONSOLIDATION_JOB_KEY,
                    ],
                )
                .await?;

            let Some(existing_row) = existing_row else {
                tx.execute(
                    "INSERT INTO codex_jobs (
                        version, kind, job_key, status, worker_id, ownership_token,
                        started_at, lease_until, retry_remaining, input_watermark,
                        last_success_watermark
                     ) VALUES ($1, $2, $3, 'running', $4, $5, $6, $7, $8, 0, 0)",
                    sql_params![
                        version.clone(),
                        JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                        MEMORY_CONSOLIDATION_JOB_KEY,
                        worker_id_str,
                        ownership_token.clone(),
                        now,
                        lease_until,
                        DEFAULT_RETRY_REMAINING,
                    ],
                )
                .await?;
                return Ok(Phase2JobClaimOutcome::Claimed {
                    ownership_token,
                    input_watermark: 0,
                });
            };

            let input_watermark_value = existing_row.opt_i64("input_watermark")?.unwrap_or(0);
            let status = existing_row.string("status")?;
            let existing_lease_until = existing_row.opt_i64("lease_until")?;
            let retry_at = existing_row.opt_i64("retry_at")?;
            let finished_at = existing_row.opt_i64("finished_at")?;
            let last_error = existing_row.opt_string("last_error")?;

            if retry_at.is_some_and(|retry_at| retry_at > now) {
                return Ok(Phase2JobClaimOutcome::SkippedRetryUnavailable);
            }
            if status == "running" && existing_lease_until.is_some_and(|until| until > now) {
                return Ok(Phase2JobClaimOutcome::SkippedRunning);
            }
            if last_error.is_none()
                && finished_at.is_some_and(|finished_at| finished_at > cooldown_cutoff)
            {
                return Ok(Phase2JobClaimOutcome::SkippedCooldown);
            }

            tx.execute(
                "UPDATE codex_jobs
                 SET status = 'running', worker_id = $1, ownership_token = $2, started_at = $3,
                     finished_at = NULL, lease_until = $4, retry_at = NULL, last_error = NULL
                 WHERE version = $5 AND kind = $6 AND job_key = $7",
                sql_params![
                    worker_id_str,
                    ownership_token.clone(),
                    now,
                    lease_until,
                    version,
                    JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                    MEMORY_CONSOLIDATION_JOB_KEY,
                ],
            )
            .await?;

            Ok(Phase2JobClaimOutcome::Claimed {
                ownership_token,
                input_watermark: input_watermark_value,
            })
        })
    })
    .await
    .map_err(internal)
}

pub(crate) async fn heartbeat_global_phase2_job(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    lease_seconds: i64,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let lease_until = now.saturating_add(lease_seconds.max(0));
    let rows = sql
        .execute(
            "UPDATE codex_jobs SET lease_until = $1
             WHERE version = $2 AND kind = $3 AND job_key = $4
               AND status = 'running' AND ownership_token = $5",
            sql_params![
                lease_until,
                version,
                JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                MEMORY_CONSOLIDATION_JOB_KEY,
                ownership_token,
            ],
        )
        .await
        .map_err(internal)?;
    Ok(rows > 0)
}

pub(crate) async fn mark_global_phase2_job_succeeded(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    completed_watermark: i64,
    selected_outputs: &[Stage1Output],
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let version = version.to_string();
    let ownership_token = ownership_token.to_string();
    let selected: Vec<(String, i64)> = selected_outputs
        .iter()
        .map(|output| {
            (
                output.thread_id.to_string(),
                output.source_updated_at.timestamp(),
            )
        })
        .collect();
    let selected_count = i64::try_from(selected_outputs.len()).unwrap_or(i64::MAX);

    with_retry(sql, move |tx| {
        let version = version.clone();
        let ownership_token = ownership_token.clone();
        let selected = selected.clone();
        Box::pin(async move {
            let existing = tx
                .fetch_optional(
                    "SELECT last_success_watermark FROM codex_jobs
                     WHERE version = $1 AND kind = $2 AND job_key = $3
                       AND status = 'running' AND ownership_token = $4",
                    sql_params![
                        version.clone(),
                        JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                        MEMORY_CONSOLIDATION_JOB_KEY,
                        ownership_token.clone(),
                    ],
                )
                .await?;
            let Some(existing) = existing else {
                return Ok(false);
            };
            let prior_watermark = existing.opt_i64("last_success_watermark")?.unwrap_or(0);
            let new_watermark = prior_watermark.max(completed_watermark);

            let rows = tx
                .execute(
                    "UPDATE codex_jobs
                     SET status = 'done', finished_at = $1, lease_until = NULL, last_error = NULL,
                         last_success_watermark = $2
                     WHERE version = $3 AND kind = $4 AND job_key = $5
                       AND status = 'running' AND ownership_token = $6",
                    sql_params![
                        now,
                        new_watermark,
                        version.clone(),
                        JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                        MEMORY_CONSOLIDATION_JOB_KEY,
                        ownership_token,
                    ],
                )
                .await?;
            if rows == 0 {
                return Ok(false);
            }

            tx.execute(
                "UPDATE codex_stage1_outputs
                 SET selected_for_phase2 = false, selected_for_phase2_source_updated_at = NULL
                 WHERE version = $1
                   AND (selected_for_phase2 = true OR selected_for_phase2_source_updated_at IS NOT NULL)",
                sql_params![version.clone()],
            )
            .await?;

            for (thread_id_str, source_updated_at) in &selected {
                tx.execute(
                    "UPDATE codex_stage1_outputs
                     SET selected_for_phase2 = true, selected_for_phase2_source_updated_at = $1
                     WHERE version = $2 AND thread_id = $3 AND source_updated_at = $4",
                    sql_params![
                        *source_updated_at,
                        version.clone(),
                        thread_id_str.clone(),
                        *source_updated_at,
                    ],
                )
                .await?;
            }

            tx.execute(
                "INSERT INTO codex_consolidation_progress (version, max_thread_count)
                 VALUES ($1, $2)
                 ON CONFLICT (version) DO UPDATE SET max_thread_count = CASE
                     WHEN excluded.max_thread_count > codex_consolidation_progress.max_thread_count
                         THEN excluded.max_thread_count
                     ELSE codex_consolidation_progress.max_thread_count
                 END",
                sql_params![version, selected_count],
            )
            .await?;

            Ok(true)
        })
    })
    .await
    .map_err(internal)
}

async fn phase2_failed_apply(
    tx: &mut SqlTx,
    version: &str,
    ownership_token: &str,
    failure_reason: &str,
    retry_at: i64,
    now: i64,
    allow_unowned: bool,
) -> AntflyResult<bool> {
    let existing = if allow_unowned {
        tx.fetch_optional(
            "SELECT retry_remaining FROM codex_jobs
             WHERE version = $1 AND kind = $2 AND job_key = $3
               AND status = 'running' AND (ownership_token = $4 OR ownership_token IS NULL)",
            sql_params![
                version,
                JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                MEMORY_CONSOLIDATION_JOB_KEY,
                ownership_token
            ],
        )
        .await?
    } else {
        tx.fetch_optional(
            "SELECT retry_remaining FROM codex_jobs
             WHERE version = $1 AND kind = $2 AND job_key = $3
               AND status = 'running' AND ownership_token = $4",
            sql_params![
                version,
                JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                MEMORY_CONSOLIDATION_JOB_KEY,
                ownership_token
            ],
        )
        .await?
    };
    let Some(existing) = existing else {
        return Ok(false);
    };
    let retry_remaining = (existing.i64("retry_remaining")? - 1).max(0);

    let rows = if allow_unowned {
        tx.execute(
            "UPDATE codex_jobs
             SET status = 'error', finished_at = $1, lease_until = NULL, retry_at = $2,
                 retry_remaining = $3, last_error = $4
             WHERE version = $5 AND kind = $6 AND job_key = $7
               AND status = 'running' AND (ownership_token = $8 OR ownership_token IS NULL)",
            sql_params![
                now,
                retry_at,
                retry_remaining,
                failure_reason,
                version,
                JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                MEMORY_CONSOLIDATION_JOB_KEY,
                ownership_token,
            ],
        )
        .await?
    } else {
        tx.execute(
            "UPDATE codex_jobs
             SET status = 'error', finished_at = $1, lease_until = NULL, retry_at = $2,
                 retry_remaining = $3, last_error = $4
             WHERE version = $5 AND kind = $6 AND job_key = $7
               AND status = 'running' AND ownership_token = $8",
            sql_params![
                now,
                retry_at,
                retry_remaining,
                failure_reason,
                version,
                JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL,
                MEMORY_CONSOLIDATION_JOB_KEY,
                ownership_token,
            ],
        )
        .await?
    };
    Ok(rows > 0)
}

async fn mark_global_phase2_job_failed_common(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    failure_reason: &str,
    retry_delay_seconds: i64,
    allow_unowned: bool,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let retry_at = now.saturating_add(retry_delay_seconds.max(0));
    let version = version.to_string();
    let ownership_token = ownership_token.to_string();
    let failure_reason = failure_reason.to_string();

    with_retry(sql, move |tx| {
        let version = version.clone();
        let ownership_token = ownership_token.clone();
        let failure_reason = failure_reason.clone();
        Box::pin(async move {
            phase2_failed_apply(
                tx,
                &version,
                &ownership_token,
                &failure_reason,
                retry_at,
                now,
                allow_unowned,
            )
            .await
        })
    })
    .await
    .map_err(internal)
}

pub(crate) async fn mark_global_phase2_job_failed(
    antfly: &Arc<Antfly>,
    version: &str,
    ownership_token: &str,
    failure_reason: &str,
    retry_delay_seconds: i64,
) -> anyhow::Result<bool> {
    mark_global_phase2_job_failed_common(
        antfly,
        version,
        ownership_token,
        failure_reason,
        retry_delay_seconds,
        false,
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
    mark_global_phase2_job_failed_common(
        antfly,
        version,
        ownership_token,
        failure_reason,
        retry_delay_seconds,
        true,
    )
    .await
}

pub(crate) async fn max_consolidated_thread_count(
    antfly: &Arc<Antfly>,
    version: &str,
) -> anyhow::Result<u32> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT max_thread_count FROM codex_consolidation_progress WHERE version = $1",
            sql_params![version],
        )
        .await
        .map_err(internal)?;
    let count = row
        .map(|row| row.i64("max_thread_count"))
        .transpose()
        .map_err(internal)?
        .unwrap_or(0);
    Ok(u32::try_from(count)?)
}

/// Marks a thread polluted on `codex_threads.memory_mode` and enqueues
/// phase-2 forgetting when the thread participated in the last successful
/// phase-2 baseline — regardless of whether the marker itself transitioned,
/// mirroring the SQLite implementation exactly (it checks
/// `selected_for_phase2` before the `UPDATE ... WHERE memory_mode !=
/// 'polluted'` and enqueues unconditionally on that earlier read).
pub(crate) async fn mark_thread_memory_mode_polluted(
    antfly: &Arc<Antfly>,
    version: &str,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let now = Utc::now().timestamp();
    let thread_id_str = thread_id.to_string();

    let selected_for_phase2 = sql
        .fetch_optional(
            "SELECT selected_for_phase2 FROM codex_stage1_outputs WHERE version = $1 AND thread_id = $2",
            sql_params![version, thread_id_str.clone()],
        )
        .await
        .map_err(internal)?
        .map(|row| row.bool("selected_for_phase2"))
        .transpose()
        .map_err(internal)?
        .unwrap_or(false);

    let rows = sql
        .execute(
            "UPDATE codex_threads SET memory_mode = 'polluted' WHERE id = $1 AND memory_mode != 'polluted'",
            sql_params![thread_id_str],
        )
        .await
        .map_err(internal)?;

    if selected_for_phase2 {
        enqueue_global_consolidation(antfly, version, now).await?;
    }

    Ok(rows > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use crate::Phase2JobClaimOutcome;
    use crate::Stage1JobClaimOutcome;
    use pretty_assertions::assert_eq;

    async fn test_store() -> (MemoryStore, Arc<Antfly>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
        config.embedder = None;
        let antfly = Arc::new(codex_antfly::Antfly::new(config));
        (
            MemoryStore::new_antfly(Arc::clone(&antfly), "v1"),
            antfly,
            dir,
        )
    }

    async fn cleanup(antfly: &Antfly, dir: tempfile::TempDir) {
        antfly.close().await.expect("close antfly");
        drop(dir);
    }

    /// Inserts a minimal `codex_threads` row satisfying every `NOT NULL`
    /// column without a default (see schema.rs migration 1).
    async fn insert_thread(
        antfly: &Arc<Antfly>,
        thread_id: ThreadId,
        cwd: &str,
        source: &str,
        updated_at_ms: i64,
        memory_mode: &str,
    ) {
        let sql = antfly.sql().await.expect("sql pool");
        sql.execute(
            "INSERT INTO codex_threads (
                id, rollout_path, created_at, updated_at, created_at_ms, updated_at_ms,
                recency_at, recency_at_ms, source, model_provider, cwd, title,
                sandbox_policy, approval_mode, memory_mode, archived
             ) VALUES ($1, $2, $3, $3, $4, $4, $3, $4, $5, 'test-provider', $6, 'test',
                       'read-only', 'on-request', $7, false)",
            sql_params![
                thread_id.to_string(),
                format!("/rollouts/{thread_id}.jsonl"),
                updated_at_ms / 1000,
                updated_at_ms,
                source,
                cwd,
                memory_mode,
            ],
        )
        .await
        .expect("insert test thread");
    }

    fn new_thread_id() -> ThreadId {
        ThreadId::new()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stage1_claim_skips_up_to_date_and_claims_newer_source() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner_a = new_thread_id();
        let owner_b = new_thread_id();

        let claim = store
            .try_claim_stage1_job(thread_id, owner_a, 100, 3600, 64)
            .await
            .expect("claim stage1");
        let token = match claim {
            Stage1JobClaimOutcome::Claimed { ownership_token } => ownership_token,
            other => panic!("unexpected claim outcome: {other:?}"),
        };
        assert!(
            store
                .mark_stage1_job_succeeded(thread_id, &token, 100, "raw", "sum", None)
                .await
                .expect("mark succeeded")
        );

        assert_eq!(
            Stage1JobClaimOutcome::SkippedUpToDate,
            store
                .try_claim_stage1_job(thread_id, owner_b, 100, 3600, 64)
                .await
                .expect("claim up to date")
        );
        assert!(matches!(
            store
                .try_claim_stage1_job(thread_id, owner_b, 101, 3600, 64)
                .await
                .expect("claim newer"),
            Stage1JobClaimOutcome::Claimed { .. }
        ));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stage1_stale_lease_can_be_stolen_but_fresh_lease_is_skipped() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner_a = new_thread_id();
        let owner_b = new_thread_id();

        assert!(matches!(
            store
                .try_claim_stage1_job(thread_id, owner_a, 100, 3600, 64)
                .await
                .unwrap(),
            Stage1JobClaimOutcome::Claimed { .. }
        ));
        assert_eq!(
            Stage1JobClaimOutcome::SkippedRunning,
            store
                .try_claim_stage1_job(thread_id, owner_b, 100, 3600, 64)
                .await
                .unwrap()
        );

        let sql = antfly.sql().await.unwrap();
        sql.execute(
            "UPDATE codex_jobs SET lease_until = 0 WHERE kind = 'memory_stage1'",
            vec![],
        )
        .await
        .unwrap();

        assert!(matches!(
            store
                .try_claim_stage1_job(thread_id, owner_b, 100, 3600, 64)
                .await
                .unwrap(),
            Stage1JobClaimOutcome::Claimed { .. }
        ));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stage1_running_cap_throttles_concurrent_claims() {
        let (store, antfly, dir) = test_store().await;
        let thread_a = new_thread_id();
        let thread_b = new_thread_id();
        insert_thread(&antfly, thread_a, "/ws-a", "cli", 1_000_000, "enabled").await;
        insert_thread(&antfly, thread_b, "/ws-b", "cli", 1_000_001, "enabled").await;
        let owner_a = new_thread_id();
        let owner_b = new_thread_id();

        assert!(matches!(
            store
                .try_claim_stage1_job(thread_a, owner_a, 100, 3600, 1)
                .await
                .unwrap(),
            Stage1JobClaimOutcome::Claimed { .. }
        ));
        assert_eq!(
            Stage1JobClaimOutcome::SkippedRunning,
            store
                .try_claim_stage1_job(thread_b, owner_b, 101, 3600, 1)
                .await
                .unwrap()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stage1_retry_exhaustion_does_not_block_newer_watermark() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner = new_thread_id();

        for _ in 0..DEFAULT_RETRY_REMAINING {
            let claim = store
                .try_claim_stage1_job(thread_id, owner, 100, 3600, 64)
                .await
                .unwrap();
            let Stage1JobClaimOutcome::Claimed { ownership_token } = claim else {
                panic!("expected claim, got {claim:?}");
            };
            assert!(
                store
                    .mark_stage1_job_failed(thread_id, &ownership_token, "boom", 0)
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            Stage1JobClaimOutcome::SkippedRetryExhausted,
            store
                .try_claim_stage1_job(thread_id, owner, 100, 3600, 64)
                .await
                .unwrap()
        );
        assert!(matches!(
            store
                .try_claim_stage1_job(thread_id, owner, 200, 3600, 64)
                .await
                .unwrap(),
            Stage1JobClaimOutcome::Claimed { .. }
        ));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mark_stage1_succeeded_no_output_deletes_output_and_enqueues_phase2() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner = new_thread_id();

        let Stage1JobClaimOutcome::Claimed { ownership_token } = store
            .try_claim_stage1_job(thread_id, owner, 100, 3600, 64)
            .await
            .unwrap()
        else {
            panic!("expected claim")
        };
        assert!(
            store
                .mark_stage1_job_succeeded(thread_id, &ownership_token, 100, "raw", "sum", None)
                .await
                .unwrap()
        );
        assert_eq!(
            1,
            store
                .list_stage1_outputs_for_global(10)
                .await
                .unwrap()
                .len()
        );

        let Stage1JobClaimOutcome::Claimed {
            ownership_token: second_token,
        } = store
            .try_claim_stage1_job(thread_id, owner, 101, 3600, 64)
            .await
            .unwrap()
        else {
            panic!("expected second claim")
        };
        assert!(
            store
                .mark_stage1_job_succeeded_no_output(thread_id, &second_token)
                .await
                .unwrap()
        );
        assert_eq!(
            0,
            store
                .list_stage1_outputs_for_global(10)
                .await
                .unwrap()
                .len()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clear_memory_data_clears_rows_and_preserves_thread_memory_modes() {
        let (store, antfly, dir) = test_store().await;
        let enabled_id = new_thread_id();
        let disabled_id = new_thread_id();
        insert_thread(
            &antfly,
            enabled_id,
            "/ws-enabled",
            "cli",
            1_000_000,
            "enabled",
        )
        .await;
        insert_thread(
            &antfly,
            disabled_id,
            "/ws-disabled",
            "cli",
            1_000_000,
            "disabled",
        )
        .await;
        let owner = new_thread_id();

        let Stage1JobClaimOutcome::Claimed { ownership_token } = store
            .try_claim_stage1_job(enabled_id, owner, 100, 3600, 64)
            .await
            .unwrap()
        else {
            panic!("expected claim")
        };
        store
            .mark_stage1_job_succeeded(enabled_id, &ownership_token, 100, "raw", "sum", None)
            .await
            .unwrap();

        store.clear_memory_data().await.expect("clear memory data");

        assert_eq!(
            0,
            store
                .list_stage1_outputs_for_global(10)
                .await
                .unwrap()
                .len()
        );
        let sql = antfly.sql().await.unwrap();
        let jobs_count = sql
            .fetch_optional("SELECT COUNT(*) AS c FROM codex_jobs", vec![])
            .await
            .unwrap()
            .unwrap()
            .i64("c")
            .unwrap();
        assert_eq!(0, jobs_count);
        let disabled_mode = sql
            .fetch_optional(
                "SELECT memory_mode FROM codex_threads WHERE id = $1",
                sql_params![disabled_id.to_string()],
            )
            .await
            .unwrap()
            .unwrap()
            .string("memory_mode")
            .unwrap();
        assert_eq!("disabled", disabled_mode);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_stage1_output_usage_updates_usage_metadata() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner = new_thread_id();
        let Stage1JobClaimOutcome::Claimed { ownership_token } = store
            .try_claim_stage1_job(thread_id, owner, 100, 3600, 64)
            .await
            .unwrap()
        else {
            panic!("expected claim")
        };
        store
            .mark_stage1_job_succeeded(thread_id, &ownership_token, 100, "raw", "sum", None)
            .await
            .unwrap();

        let missing_id = new_thread_id();
        let updated = store
            .record_stage1_output_usage(&[thread_id, missing_id])
            .await
            .unwrap();
        assert_eq!(1, updated);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn phase2_lifecycle_claim_heartbeat_succeed_and_select_outputs() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner = new_thread_id();
        // A recent timestamp: `get_phase2_input_selection`'s `max_unused_days`
        // cutoff excludes stage1 outputs whose `source_updated_at` (for a
        // never-used memory) falls outside that recency window, matching
        // the SQLite implementation's candidate-selection query.
        let source_updated_at = Utc::now().timestamp();
        let Stage1JobClaimOutcome::Claimed { ownership_token } = store
            .try_claim_stage1_job(thread_id, owner, source_updated_at, 3600, 64)
            .await
            .unwrap()
        else {
            panic!("expected stage1 claim")
        };
        store
            .mark_stage1_job_succeeded(
                thread_id,
                &ownership_token,
                source_updated_at,
                "raw",
                "sum",
                None,
            )
            .await
            .unwrap();

        let Phase2JobClaimOutcome::Claimed {
            ownership_token: phase2_token,
            input_watermark,
        } = store
            .try_claim_global_phase2_job(owner, 3600)
            .await
            .unwrap()
        else {
            panic!("expected phase2 claim")
        };
        assert_eq!(source_updated_at, input_watermark);
        assert!(
            store
                .heartbeat_global_phase2_job(&phase2_token, 3600)
                .await
                .unwrap()
        );

        let selected = store.list_stage1_outputs_for_global(10).await.unwrap();
        assert_eq!(1, selected.len());
        assert!(
            store
                .mark_global_phase2_job_succeeded(&phase2_token, input_watermark, &selected)
                .await
                .unwrap()
        );
        assert_eq!(1, store.max_consolidated_thread_count().await.unwrap());

        let phase2_input = store.get_phase2_input_selection(10, 365).await.unwrap();
        assert_eq!(1, phase2_input.len());
        assert_eq!(thread_id, phase2_input[0].thread_id);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn phase2_failure_fallback_updates_unowned_running_job() {
        let (store, antfly, dir) = test_store().await;
        store.enqueue_global_consolidation(1).await.unwrap();
        let owner = new_thread_id();
        let Phase2JobClaimOutcome::Claimed {
            ownership_token, ..
        } = store
            .try_claim_global_phase2_job(owner, 3600)
            .await
            .unwrap()
        else {
            panic!("expected phase2 claim")
        };
        // Simulate ownership loss by clearing the token directly.
        let sql = antfly.sql().await.unwrap();
        sql.execute(
            "UPDATE codex_jobs SET ownership_token = NULL WHERE kind = 'memory_consolidate_global'",
            vec![],
        )
        .await
        .unwrap();

        assert!(
            store
                .mark_global_phase2_job_failed_if_unowned(&ownership_token, "boom", 60)
                .await
                .unwrap()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mark_thread_memory_mode_polluted_enqueues_phase2_for_selected_threads() {
        let (store, antfly, dir) = test_store().await;
        let thread_id = new_thread_id();
        insert_thread(&antfly, thread_id, "/ws", "cli", 1_000_000, "enabled").await;
        let owner = new_thread_id();
        let Stage1JobClaimOutcome::Claimed { ownership_token } = store
            .try_claim_stage1_job(thread_id, owner, 100, 3600, 64)
            .await
            .unwrap()
        else {
            panic!("expected stage1 claim")
        };
        store
            .mark_stage1_job_succeeded(thread_id, &ownership_token, 100, "raw", "sum", None)
            .await
            .unwrap();
        let Phase2JobClaimOutcome::Claimed {
            ownership_token: phase2_token,
            input_watermark,
        } = store
            .try_claim_global_phase2_job(owner, 3600)
            .await
            .unwrap()
        else {
            panic!("expected phase2 claim")
        };
        let selected = store.list_stage1_outputs_for_global(10).await.unwrap();
        store
            .mark_global_phase2_job_succeeded(&phase2_token, input_watermark, &selected)
            .await
            .unwrap();

        assert!(
            store
                .mark_thread_memory_mode_polluted(thread_id)
                .await
                .unwrap()
        );

        let sql = antfly.sql().await.unwrap();
        sql.execute(
            "UPDATE codex_jobs SET finished_at = $1
             WHERE kind = 'memory_consolidate_global'",
            sql_params![Utc::now().timestamp() - PHASE2_SUCCESS_COOLDOWN_SECONDS - 1],
        )
        .await
        .unwrap();
        assert!(matches!(
            store
                .try_claim_global_phase2_job(owner, 3600)
                .await
                .unwrap(),
            Phase2JobClaimOutcome::Claimed { .. }
        ));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_stage1_outputs_for_global_skips_polluted_threads() {
        let (store, antfly, dir) = test_store().await;
        let enabled_id = new_thread_id();
        let polluted_id = new_thread_id();
        insert_thread(
            &antfly,
            enabled_id,
            "/ws-enabled",
            "cli",
            1_000_000,
            "enabled",
        )
        .await;
        insert_thread(
            &antfly,
            polluted_id,
            "/ws-polluted",
            "cli",
            1_000_000,
            "polluted",
        )
        .await;
        let owner = new_thread_id();
        for id in [enabled_id, polluted_id] {
            let Stage1JobClaimOutcome::Claimed { ownership_token } = store
                .try_claim_stage1_job(id, owner, 100, 3600, 64)
                .await
                .unwrap()
            else {
                panic!("expected claim")
            };
            store
                .mark_stage1_job_succeeded(id, &ownership_token, 100, "raw", "sum", None)
                .await
                .unwrap();
        }

        let outputs = store.list_stage1_outputs_for_global(10).await.unwrap();
        assert_eq!(1, outputs.len());
        assert_eq!(enabled_id, outputs[0].thread_id);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_phase2_input_selection_prioritizes_usage_then_recency() {
        let (store, antfly, dir) = test_store().await;
        let high_usage = new_thread_id();
        let low_usage = new_thread_id();
        insert_thread(&antfly, high_usage, "/ws-high", "cli", 1_000_000, "enabled").await;
        insert_thread(&antfly, low_usage, "/ws-low", "cli", 1_000_000, "enabled").await;
        let owner = new_thread_id();
        for id in [high_usage, low_usage] {
            let Stage1JobClaimOutcome::Claimed { ownership_token } = store
                .try_claim_stage1_job(id, owner, 100, 3600, 64)
                .await
                .unwrap()
            else {
                panic!("expected claim")
            };
            store
                .mark_stage1_job_succeeded(id, &ownership_token, 100, "raw", "sum", None)
                .await
                .unwrap();
        }
        store
            .record_stage1_output_usage(&[high_usage, high_usage, high_usage])
            .await
            .unwrap();
        store
            .record_stage1_output_usage(&[low_usage])
            .await
            .unwrap();

        let selection = store.get_phase2_input_selection(1, 365).await.unwrap();
        assert_eq!(
            vec![high_usage],
            selection.iter().map(|o| o.thread_id).collect::<Vec<_>>()
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prune_stage1_outputs_for_retention_prunes_stale_unselected_rows_only() {
        let (store, antfly, dir) = test_store().await;
        let stale_unselected = new_thread_id();
        let stale_selected = new_thread_id();
        let fresh = new_thread_id();
        for (id, ws) in [
            (stale_unselected, "/ws-stale-unselected"),
            (stale_selected, "/ws-stale-selected"),
            (fresh, "/ws-fresh"),
        ] {
            insert_thread(&antfly, id, ws, "cli", 1_000_000, "enabled").await;
        }
        let owner = new_thread_id();
        let now = Utc::now().timestamp();
        let stale_at = now - Duration::days(60).num_seconds();
        let fresh_at = now - Duration::days(1).num_seconds();
        for (id, source_updated_at) in [
            (stale_unselected, stale_at),
            (stale_selected, stale_at),
            (fresh, fresh_at),
        ] {
            let Stage1JobClaimOutcome::Claimed { ownership_token } = store
                .try_claim_stage1_job(id, owner, source_updated_at, 3600, 64)
                .await
                .unwrap()
            else {
                panic!("expected claim")
            };
            store
                .mark_stage1_job_succeeded(
                    id,
                    &ownership_token,
                    source_updated_at,
                    "raw",
                    "sum",
                    None,
                )
                .await
                .unwrap();
        }
        let sql = antfly.sql().await.unwrap();
        sql.execute(
            "UPDATE codex_stage1_outputs SET selected_for_phase2 = true WHERE thread_id = $1",
            sql_params![stale_selected.to_string()],
        )
        .await
        .unwrap();

        let pruned = store
            .prune_stage1_outputs_for_retention(/*max_unused_days*/ 30, /*limit*/ 100)
            .await
            .unwrap();
        assert_eq!(1, pruned);

        let remaining: Vec<ThreadId> = store
            .list_stage1_outputs_for_global(10)
            .await
            .unwrap()
            .into_iter()
            .map(|output| output.thread_id)
            .collect();
        assert!(remaining.contains(&fresh));
        assert!(remaining.contains(&stale_selected));
        assert!(!remaining.contains(&stale_unselected));

        cleanup(&antfly, dir).await;
    }
}
