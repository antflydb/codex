//! Progress of per-turn claim extraction, one `codex_claim_progress` row per
//! thread.
//!
//! A worker leases a thread before extracting, advances `next_ordinal` after
//! each committed batch of turns, and records `settled_ordinal` once the
//! thread's claims have been settled. Leases expire, so a crashed worker's
//! thread is picked up again; failures back off through `retry_at_ms`.

use crate::Antfly;
use crate::AntflyError;
use crate::AntflyResult;
use crate::sql::SqlValue;
use crate::sql_params;

/// Where extraction stands for one leased thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimProgress {
    /// First history ordinal not yet extracted.
    pub next_ordinal: u64,
    /// Claims from history below this ordinal have been settled.
    pub settled_ordinal: u64,
    /// Consecutive failed attempts.
    pub failures: u32,
}

/// A thread the startup sweep may need to extract or settle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SweepCandidate {
    pub thread_id: String,
    /// The thread's `next_ordinal`: one past its last stored history item.
    pub history_end: u64,
    pub updated_at_ms: i64,
    pub cwd: String,
    /// `None` when the pipeline has never seen the thread.
    pub progress: Option<ClaimProgress>,
}

/// Bounds for [`sweep_candidates`].
#[derive(Clone, Debug)]
pub struct SweepParams<'a> {
    /// Session sources eligible for extraction (interactive ones).
    pub sources: &'a [&'a str],
    /// Threads last updated before this are ignored.
    pub min_updated_at_ms: i64,
    /// Threads idle since before this are settled even with no new history.
    pub settle_before_ms: i64,
    pub now_ms: i64,
    pub limit: usize,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

fn ordinal(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn progress_from(row: &crate::sql::SqlRow, prefix: &str) -> AntflyResult<Option<ClaimProgress>> {
    let Some(next) = row.opt_i64(&format!("{prefix}next_ordinal"))? else {
        return Ok(None);
    };
    Ok(Some(ClaimProgress {
        next_ordinal: ordinal(next),
        settled_ordinal: ordinal(
            row.opt_i64(&format!("{prefix}settled_ordinal"))?
                .unwrap_or(0),
        ),
        failures: u32::try_from(row.opt_i64(&format!("{prefix}failures"))?.unwrap_or(0))
            .unwrap_or(u32::MAX),
    }))
}

/// Whether `thread_id` exists and has memory generation enabled
/// (`memory_mode = 'enabled'`).
pub async fn memory_enabled(antfly: &Antfly, thread_id: &str) -> AntflyResult<bool> {
    let sql = antfly.sql().await?;
    let row = sql
        .fetch_optional(
            "SELECT memory_mode FROM codex_threads WHERE id = $1",
            sql_params![thread_id],
        )
        .await?;
    Ok(match row {
        Some(row) => row.string("memory_mode")? == "enabled",
        None => false,
    })
}

/// Leases `thread_id` for `owner` until `lease_ms` from now. `None` when
/// another owner holds a live lease or the thread is backing off.
pub async fn lease(
    antfly: &Antfly,
    thread_id: &str,
    owner: &str,
    lease_ms: i64,
) -> AntflyResult<Option<ClaimProgress>> {
    let sql = antfly.sql().await?;
    let now = now_ms();
    sql.execute(
        "INSERT INTO codex_claim_progress (thread_id, updated_at_ms) VALUES ($1, $2) \
         ON CONFLICT (thread_id) DO NOTHING",
        sql_params![thread_id, now],
    )
    .await?;
    let row = sql
        .fetch_optional(
            "UPDATE codex_claim_progress \
             SET lease_owner = $2, lease_until_ms = $3, updated_at_ms = $4 \
             WHERE thread_id = $1 \
               AND (lease_owner IS NULL OR lease_owner = $2 OR lease_until_ms < $4) \
               AND (retry_at_ms IS NULL OR retry_at_ms <= $4) \
             RETURNING next_ordinal, settled_ordinal, failures",
            sql_params![thread_id, owner, now.saturating_add(lease_ms), now],
        )
        .await?;
    match row {
        Some(row) => progress_from(&row, ""),
        None => Ok(None),
    }
}

async fn update_leased(
    antfly: &Antfly,
    thread_id: &str,
    owner: &str,
    assignments: &str,
    mut params: Vec<SqlValue>,
) -> AntflyResult<()> {
    let sql = antfly.sql().await?;
    let base = params.len();
    let statement = format!(
        "UPDATE codex_claim_progress SET {assignments}, updated_at_ms = ${} \
         WHERE thread_id = ${} AND lease_owner = ${}",
        base + 1,
        base + 2,
        base + 3,
    );
    params.extend(sql_params![now_ms(), thread_id, owner]);
    if sql.execute(&statement, params).await? == 0 {
        return Err(AntflyError::Malformed(format!(
            "claim extraction lease on {thread_id} was lost"
        )));
    }
    Ok(())
}

/// Records that history below `next_ordinal` has been extracted and clears
/// any failure state.
pub async fn advance(
    antfly: &Antfly,
    thread_id: &str,
    owner: &str,
    next_ordinal: u64,
) -> AntflyResult<()> {
    update_leased(
        antfly,
        thread_id,
        owner,
        "next_ordinal = $1, failures = 0, retry_at_ms = NULL, last_error = NULL",
        sql_params![next_ordinal as i64],
    )
    .await
}

/// Records that claims from history below `settled_ordinal` are settled.
pub async fn settle(
    antfly: &Antfly,
    thread_id: &str,
    owner: &str,
    settled_ordinal: u64,
) -> AntflyResult<()> {
    update_leased(
        antfly,
        thread_id,
        owner,
        "settled_ordinal = $1",
        sql_params![settled_ordinal as i64],
    )
    .await
}

/// Releases the lease. A failure is recorded with a retry time.
pub async fn release(
    antfly: &Antfly,
    thread_id: &str,
    owner: &str,
    failure: Option<(&str, i64)>,
) -> AntflyResult<()> {
    match failure {
        None => {
            update_leased(
                antfly,
                thread_id,
                owner,
                "lease_owner = NULL, lease_until_ms = NULL",
                Vec::new(),
            )
            .await
        }
        Some((error, retry_at_ms)) => {
            update_leased(
                antfly,
                thread_id,
                owner,
                "lease_owner = NULL, lease_until_ms = NULL, failures = failures + 1, \
                 retry_at_ms = $1, last_error = $2",
                sql_params![retry_at_ms, error],
            )
            .await
        }
    }
}

/// Recently updated threads with history the pipeline has not extracted, or
/// claims it has not settled after the thread went idle. Newest first.
pub async fn sweep_candidates(
    antfly: &Antfly,
    params: SweepParams<'_>,
) -> AntflyResult<Vec<SweepCandidate>> {
    if params.sources.is_empty() || params.limit == 0 {
        return Ok(Vec::new());
    }
    let sql = antfly.sql().await?;
    let mut values = sql_params![
        params.min_updated_at_ms,
        params.now_ms,
        params.settle_before_ms,
        params.limit as i64
    ];
    let sources = params
        .sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            values.push(SqlValue::from(*source));
            format!("${}", index + 5)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let statement = format!(
        "SELECT t.id, t.next_ordinal AS history_end, t.updated_at_ms, t.cwd, \
                p.next_ordinal AS p_next_ordinal, p.settled_ordinal AS p_settled_ordinal, \
                p.failures AS p_failures \
         FROM codex_threads t LEFT JOIN codex_claim_progress p ON p.thread_id = t.id \
         WHERE t.archived = false AND t.memory_mode = 'enabled' \
           AND t.updated_at_ms >= $1 AND t.source IN ({sources}) \
           AND (p.thread_id IS NULL \
                OR ((p.retry_at_ms IS NULL OR p.retry_at_ms <= $2) \
                    AND (p.lease_until_ms IS NULL OR p.lease_until_ms < $2) \
                    AND (p.next_ordinal < t.next_ordinal \
                         OR (p.settled_ordinal < p.next_ordinal AND t.updated_at_ms <= $3)))) \
         ORDER BY t.updated_at_ms DESC LIMIT $4"
    );
    sql.fetch_all(&statement, values)
        .await?
        .iter()
        .map(|row| {
            Ok(SweepCandidate {
                thread_id: row.string("id")?,
                history_end: ordinal(row.i64("history_end")?),
                updated_at_ms: row.i64("updated_at_ms")?,
                cwd: row.string("cwd")?,
                progress: progress_from(row, "p_")?,
            })
        })
        .collect()
}

/// The thread `thread_id` as a candidate, whatever its source, age or
/// progress. `None` when it does not exist.
pub async fn thread_candidate(
    antfly: &Antfly,
    thread_id: &str,
) -> AntflyResult<Option<SweepCandidate>> {
    let sql = antfly.sql().await?;
    let row = sql
        .fetch_optional(
            "SELECT t.id, t.next_ordinal AS history_end, t.updated_at_ms, t.cwd, \
                    p.next_ordinal AS p_next_ordinal, p.settled_ordinal AS p_settled_ordinal, \
                    p.failures AS p_failures \
             FROM codex_threads t LEFT JOIN codex_claim_progress p ON p.thread_id = t.id \
             WHERE t.id = $1",
            sql_params![thread_id],
        )
        .await?;
    row.map(|row| {
        Ok(SweepCandidate {
            thread_id: row.string("id")?,
            history_end: ordinal(row.i64("history_end")?),
            updated_at_ms: row.i64("updated_at_ms")?,
            cwd: row.string("cwd")?,
            progress: progress_from(&row, "p_")?,
        })
    })
    .transpose()
}

/// Turns of `thread_id` starting at or after `from_ordinal`, from the
/// paginated projection. `None` for threads without one (Legacy history).
pub async fn pending_turns(
    antfly: &Antfly,
    thread_id: &str,
    from_ordinal: u64,
) -> AntflyResult<Option<u64>> {
    let sql = antfly.sql().await?;
    let projected = sql
        .fetch_optional(
            "SELECT next_rollout_ordinal FROM codex_thread_history_projection_state WHERE thread_id = $1",
            sql_params![thread_id],
        )
        .await?;
    if projected.is_none() {
        return Ok(None);
    }
    let row = sql
        .fetch_optional(
            "SELECT COUNT(*) AS n FROM codex_thread_turns WHERE thread_id = $1 AND rollout_ordinal >= $2",
            sql_params![thread_id, from_ordinal as i64],
        )
        .await?;
    Ok(row.map(|row| ordinal(row.i64("n").unwrap_or_default())))
}

/// Pipeline-wide counts for `codex knowledge status`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProgressSummary {
    /// Threads the pipeline has seen.
    pub threads: u64,
    /// Seen threads whose claims are all settled.
    pub settled: u64,
    /// Threads backing off after failures, with their last error.
    pub failing: Vec<(String, u32, String)>,
}

pub async fn summary(antfly: &Antfly) -> AntflyResult<ProgressSummary> {
    let sql = antfly.sql().await?;
    let mut summary = ProgressSummary::default();
    for row in sql
        .fetch_all(
            "SELECT thread_id, next_ordinal, settled_ordinal, failures, last_error FROM codex_claim_progress",
            Vec::new(),
        )
        .await?
    {
        summary.threads += 1;
        if row.i64("settled_ordinal")? >= row.i64("next_ordinal")? {
            summary.settled += 1;
        }
        let failures = row.i64("failures")?;
        if failures > 0 {
            summary.failing.push((
                row.string("thread_id")?,
                u32::try_from(failures).unwrap_or(u32::MAX),
                row.opt_string("last_error")?.unwrap_or_default(),
            ));
        }
    }
    Ok(summary)
}
