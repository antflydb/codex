//! Antfly-backed thread metadata, listing, spawn edges, and deletion.
//!
//! Mirrors `state/src/runtime/threads.rs`'s SQLite methods one-for-one over
//! `codex_threads` / `codex_thread_spawn_edges`, the same tables
//! `codex-thread-store`'s `AntflyThreadStore` writes (see
//! `thread-store/src/antfly/record.rs`): one source of truth for thread
//! state. `codex_threads` carries every column `ThreadMetadata` needs
//! directly; `section_name`/`section_appearance` are joined in at read time
//! via a correlated subquery against `codex_thread_sections`, like
//! `AntflyThreadStore`'s own `record::SELECT_THREAD`.
//!
//! Antfly SQL dialect notes (see also `codex-antfly::sql`'s doc comment):
//! - `instr(a, b) > 0` has no direct equivalent; `strpos` is not supported
//!   either (`0A000`/`25P02`). Use `a LIKE $n` with the needle escaped in
//!   Rust (`\` -> `\\`, `%` -> `\%`, `_` -> `\_`) and wrapped `%needle%`
//!   (Antfly applies the default backslash escape with no `ESCAPE` clause).
//! - A bound `NULL` parameter carries no type of its own over the embedded
//!   driver and is rejected (`22023`) against a non-JSON column; write a
//!   literal `NULL` instead of a parameter when a value is absent.
//! - `codex_threads.sandbox_policy` has no `DEFAULT`, unlike
//!   `tokens_used`/`has_user_event`; every insert must still supply it.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlValue;
use codex_antfly::sql_params;
use codex_history::RolloutItem;
use codex_protocol::SanitizedGitUrl;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadSource;

use super::internal;
use crate::Anchor;
use crate::DirectionalThreadSpawnEdgeStatus;
use crate::SortDirection;
use crate::SortKey;
use crate::ThreadFilterOptions;
use crate::ThreadMetadata;
use crate::ThreadMetadataBuilder;
use crate::ThreadRelationFilter;
use crate::ThreadsPage;
use crate::extract::enum_to_string;
use crate::model::anchor_from_item;
use crate::runtime::threads::extract_memory_mode;

const SELECT_COLUMNS: &str = "
    t.id, t.rollout_path, t.created_at_ms AS created_at, t.updated_at_ms AS updated_at,
    t.recency_at_ms AS recency_at, t.source, t.originator, t.creator_user_id,
    t.creator_account_id, t.history_mode, t.thread_source, t.agent_nickname, t.agent_role,
    t.agent_path, t.model_provider, t.model, t.reasoning_effort, t.cwd, t.cli_version,
    t.title, t.name, t.preview, t.sandbox_policy, t.approval_mode, t.tokens_used,
    t.first_user_message, t.archived_at,
    t.thread_section_id AS section,
    (SELECT s.name FROM codex_thread_sections s WHERE s.id = t.thread_section_id) AS section_name,
    (SELECT s.appearance FROM codex_thread_sections s WHERE s.id = t.thread_section_id) AS section_appearance,
    t.section_position, t.section_entered_at_ms, t.project_id, t.daybreak_enabled,
    t.git_sha, t.git_branch, t.git_origin_url";

fn epoch_millis_to_datetime(value: i64) -> anyhow::Result<DateTime<Utc>> {
    const MIN_EPOCH_MILLIS: i64 = 1_577_836_800_000;
    let millis = if value < MIN_EPOCH_MILLIS {
        value.saturating_mul(1000)
    } else {
        value
    };
    DateTime::<Utc>::from_timestamp_millis(millis)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp millis: {value}"))
}

fn epoch_seconds_to_datetime(value: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp seconds: {value}"))
}

fn thread_metadata_from_row(row: &SqlRow) -> anyhow::Result<ThreadMetadata> {
    let id = ThreadId::try_from(row.string("id")?)?;
    let section_id = row.opt_string("section")?;
    let section_name = row.opt_string("section_name")?;
    let section_appearance = row.opt_string("section_appearance")?;
    let section = match (section_id, section_name) {
        (Some(id), Some(name)) => Some(crate::ThreadSection::from_row((
            id,
            name,
            section_appearance,
        ))?),
        (None, None) => None,
        (Some(id), None) => anyhow::bail!("thread references an unknown section: {id}"),
        (None, Some(name)) => {
            anyhow::bail!("thread has a section name without a section id: {name}")
        }
    };
    let preview = row.string("preview")?;
    let first_user_message = row.string("first_user_message")?;
    Ok(ThreadMetadata {
        originator: row.opt_string("originator")?,
        creator_user_id: row.opt_string("creator_user_id")?,
        creator_account_id: row.opt_string("creator_account_id")?,
        id,
        rollout_path: PathBuf::from(row.string("rollout_path")?),
        created_at: epoch_millis_to_datetime(row.i64("created_at")?)?,
        updated_at: epoch_millis_to_datetime(row.i64("updated_at")?)?,
        recency_at: epoch_millis_to_datetime(row.i64("recency_at")?)?,
        source: row.string("source")?,
        history_mode: row
            .string("history_mode")?
            .parse()
            .map_err(anyhow::Error::msg)?,
        thread_source: row
            .opt_string("thread_source")?
            .map(|value| value.parse())
            .transpose()
            .map_err(anyhow::Error::msg)?,
        agent_nickname: row.opt_string("agent_nickname")?,
        agent_role: row.opt_string("agent_role")?,
        agent_path: row.opt_string("agent_path")?,
        model_provider: row.string("model_provider")?,
        model: row.opt_string("model")?,
        reasoning_effort: row
            .opt_string("reasoning_effort")?
            .and_then(|value| value.parse().ok()),
        cwd: PathBuf::from(row.string("cwd")?),
        cli_version: row.string("cli_version")?,
        title: row.string("title")?,
        name: row.opt_string("name")?,
        preview: (!preview.is_empty()).then_some(preview),
        sandbox_policy: row.string("sandbox_policy")?,
        approval_mode: row.string("approval_mode")?,
        tokens_used: row.i64("tokens_used")?,
        first_user_message: (!first_user_message.is_empty()).then_some(first_user_message),
        archived_at: row
            .opt_i64("archived_at")?
            .map(epoch_seconds_to_datetime)
            .transpose()?,
        section,
        section_position: row.opt_i64("section_position")?,
        section_entered_at: row
            .opt_i64("section_entered_at_ms")?
            .map(epoch_millis_to_datetime)
            .transpose()?,
        project_id: row.opt_string("project_id")?,
        daybreak_enabled: row.opt_bool("daybreak_enabled")?,
        git_sha: row.opt_string("git_sha")?,
        git_branch: row.opt_string("git_branch")?,
        git_origin_url: row
            .opt_string("git_origin_url")?
            .and_then(|url| SanitizedGitUrl::try_from(url).ok()),
    })
}

pub(crate) async fn get_thread(
    antfly: &Arc<Antfly>,
    id: ThreadId,
) -> anyhow::Result<Option<ThreadMetadata>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            &format!("SELECT {SELECT_COLUMNS} FROM codex_threads t WHERE t.id = $1"),
            sql_params![id.to_string()],
        )
        .await
        .map_err(internal)?;
    row.map(|row| thread_metadata_from_row(&row)).transpose()
}

pub(crate) async fn get_threads(
    antfly: &Arc<Antfly>,
    thread_ids: &[ThreadId],
) -> anyhow::Result<std::collections::HashMap<ThreadId, ThreadMetadata>> {
    let mut threads = std::collections::HashMap::with_capacity(thread_ids.len());
    if thread_ids.is_empty() {
        return Ok(threads);
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let placeholders: Vec<String> = (1..=thread_ids.len())
        .map(|index| format!("${index}"))
        .collect();
    let params: Vec<SqlValue> = thread_ids.iter().map(|id| id.to_string().into()).collect();
    let rows = sql
        .fetch_all(
            &format!(
                "SELECT {SELECT_COLUMNS} FROM codex_threads t WHERE t.id IN ({})",
                placeholders.join(", ")
            ),
            params,
        )
        .await
        .map_err(internal)?;
    for row in &rows {
        if let Ok(metadata) = thread_metadata_from_row(row) {
            threads.insert(metadata.id, metadata);
        }
    }
    Ok(threads)
}

pub(crate) async fn mark_thread_paginated(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    legacy_name: Option<&str>,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    // `codex_threads.name` is nullable with no literal-NULL concern here:
    // `legacy_name` is always bound (possibly as SQL NULL via the macro,
    // which this specific column tolerates since it is read back, never
    // compared against another column in the same statement).
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET history_mode = 'paginated', name = CASE \
                WHEN name IS NULL OR trim(name) = '' THEN $1 \
                WHEN history_mode = 'legacy' AND source = '{\"subagent\":{\"other\":\"guardian\"}}' AND name = $2 THEN COALESCE($3, name) \
                ELSE name END \
             WHERE id = $4",
            sql_params![legacy_name, crate::GUARDIAN_THREAD_TITLE, legacy_name, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn get_thread_memory_mode(
    antfly: &Arc<Antfly>,
    id: ThreadId,
) -> anyhow::Result<Option<String>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT memory_mode FROM codex_threads WHERE id = $1",
            sql_params![id.to_string()],
        )
        .await
        .map_err(internal)?;
    row.map(|row| row.string("memory_mode"))
        .transpose()
        .map_err(internal)
}

pub(crate) async fn is_memory_mode_enabled(
    antfly: &Arc<Antfly>,
    id: ThreadId,
) -> anyhow::Result<bool> {
    Ok(get_thread_memory_mode(antfly, id).await?.as_deref() == Some("enabled"))
}

pub(crate) async fn mark_memory_mode_polluted(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET memory_mode = 'polluted' WHERE id = $1 AND memory_mode <> 'polluted'",
            sql_params![thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn set_thread_preview_if_empty(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    preview: &str,
) -> anyhow::Result<bool> {
    let preview = preview.trim();
    if preview.is_empty() {
        return Ok(false);
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET preview = $1 WHERE id = $2 AND preview = ''",
            sql_params![preview, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn set_thread_daybreak_enabled(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    daybreak_enabled: bool,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET daybreak_enabled = $1 WHERE id = $2",
            sql_params![daybreak_enabled, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn set_thread_memory_mode(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    memory_mode: &str,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET memory_mode = $1 WHERE id = $2",
            sql_params![memory_mode, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn update_thread_title(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    title: &str,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET title = $1 WHERE id = $2",
            sql_params![title, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn update_thread_name(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    name: Option<&str>,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    // See the module's dialect notes: a bound `NULL` is rejected against
    // `codex_threads.name` (TEXT), so a cleared name is a literal.
    let (statement, params): (&str, Vec<SqlValue>) = match name {
        Some(name) => (
            "UPDATE codex_threads SET name = $1 WHERE id = $2",
            sql_params![name, thread_id.to_string()],
        ),
        None => (
            "UPDATE codex_threads SET name = NULL WHERE id = $1",
            sql_params![thread_id.to_string()],
        ),
    };
    let rows_affected = sql.execute(statement, params).await.map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn touch_thread_updated_at(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    updated_at: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET updated_at = $1, updated_at_ms = $2 WHERE id = $3",
            sql_params![
                updated_at.timestamp(),
                updated_at.timestamp_millis(),
                thread_id.to_string()
            ],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

/// Mirrors the SQLite `MAX(?, MAX(?, recency_at_ms + 1) / 1000)` monotonic
/// bump: `recency_at_ms` never goes backwards and is unique per touch.
pub(crate) async fn touch_thread_recency_at(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    recency_at: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let recency_at_seconds = recency_at.timestamp();
    let recency_at_millis = recency_at.timestamp_millis();
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET \
                recency_at = GREATEST($1, GREATEST($2, recency_at_ms + 1) / 1000), \
                recency_at_ms = GREATEST($2, recency_at_ms + 1) \
             WHERE id = $3",
            sql_params![recency_at_seconds, recency_at_millis, thread_id.to_string()],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

pub(crate) async fn update_thread_git_info(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    git_sha: Option<Option<&str>>,
    git_branch: Option<Option<&str>>,
    git_origin_url: Option<Option<&SanitizedGitUrl>>,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET \
                git_sha = CASE WHEN $1 THEN $2 ELSE git_sha END, \
                git_branch = CASE WHEN $3 THEN $4 ELSE git_branch END, \
                git_origin_url = CASE WHEN $5 THEN $6 ELSE git_origin_url END \
             WHERE id = $7",
            sql_params![
                git_sha.is_some(),
                git_sha.flatten(),
                git_branch.is_some(),
                git_branch.flatten(),
                git_origin_url.is_some(),
                git_origin_url.flatten().map(SanitizedGitUrl::as_str),
                thread_id.to_string()
            ],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected > 0)
}

fn thread_spawn_parent_thread_id_from_source_str(source: &str) -> Option<ThreadId> {
    let parsed: Option<SessionSource> = serde_json::from_str(source)
        .or_else(|_| {
            serde_json::from_value::<SessionSource>(serde_json::Value::String(source.to_string()))
        })
        .ok();
    parsed?.parent_thread_id()
}

async fn insert_thread_spawn_edge_if_absent(
    antfly: &Arc<Antfly>,
    parent_thread_id: ThreadId,
    child_thread_id: ThreadId,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_thread_spawn_edges (child_thread_id, parent_thread_id, status) VALUES ($1, $2, $3) \
         ON CONFLICT (child_thread_id) DO NOTHING",
        sql_params![child_thread_id.to_string(), parent_thread_id.to_string(), DirectionalThreadSpawnEdgeStatus::Open.as_ref()],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

async fn insert_thread_spawn_edge_from_source_if_absent(
    antfly: &Arc<Antfly>,
    child_thread_id: ThreadId,
    source: &str,
) -> anyhow::Result<()> {
    let Some(parent_thread_id) = thread_spawn_parent_thread_id_from_source_str(source) else {
        return Ok(());
    };
    insert_thread_spawn_edge_if_absent(antfly, parent_thread_id, child_thread_id).await
}

/// `codex_threads` columns an upsert must always supply (either `$n` or a
/// literal `NULL` where the value is absent). Same order as the bind list
/// in [`upsert_thread_with_creation_memory_mode`] / [`insert_thread_if_absent`].
fn metadata_preview(metadata: &ThreadMetadata) -> &str {
    metadata
        .preview
        .as_deref()
        .or(metadata.first_user_message.as_deref())
        .unwrap_or_default()
}

/// Builds `($1, $2::bool, NULL, ...)`-style value expressions: `None` values
/// become the literal `NULL` (see the module's dialect notes), everything
/// else a bound `$n`.
struct ValueList {
    exprs: Vec<String>,
    params: Vec<SqlValue>,
}

impl ValueList {
    fn new() -> Self {
        Self {
            exprs: Vec::new(),
            params: Vec::new(),
        }
    }

    fn push(&mut self, value: impl Into<SqlValue>) -> &mut Self {
        let value = value.into();
        if matches!(value, SqlValue::Null) {
            self.exprs.push("NULL".to_string());
        } else {
            self.params.push(value);
            self.exprs.push(format!("${}", self.params.len()));
        }
        self
    }
}

async fn upsert_thread_with_creation_memory_mode(
    antfly: &Arc<Antfly>,
    metadata: &ThreadMetadata,
    creation_memory_mode: Option<&str>,
) -> anyhow::Result<()> {
    let preview = metadata_preview(metadata);
    let mut values = ValueList::new();
    values
        .push(metadata.id.to_string())
        .push(metadata.rollout_path.display().to_string())
        .push(metadata.created_at.timestamp())
        .push(metadata.updated_at.timestamp())
        .push(metadata.recency_at.timestamp())
        .push(metadata.created_at.timestamp_millis())
        .push(metadata.updated_at.timestamp_millis())
        .push(metadata.recency_at.timestamp_millis())
        .push(metadata.source.clone())
        .push(metadata.originator.clone())
        .push(metadata.creator_user_id.clone())
        .push(metadata.creator_account_id.clone())
        .push(metadata.history_mode.as_str())
        .push(metadata.thread_source.as_ref().map(ThreadSource::as_str))
        .push(metadata.agent_nickname.clone())
        .push(metadata.agent_role.clone())
        .push(metadata.agent_path.clone())
        .push(metadata.model_provider.clone())
        .push(metadata.model.clone())
        .push(metadata.reasoning_effort.as_ref().map(enum_to_string))
        .push(metadata.cwd.display().to_string())
        .push(metadata.cli_version.clone())
        .push(metadata.title.clone())
        .push(metadata.name.clone())
        .push(preview.to_string())
        .push(metadata.sandbox_policy.clone())
        .push(metadata.approval_mode.clone())
        .push(metadata.tokens_used)
        .push(metadata.first_user_message.clone().unwrap_or_default())
        .push(metadata.archived_at.is_some())
        .push(metadata.archived_at.map(|at| at.timestamp()))
        .push(metadata.section.as_ref().map(|section| section.id.clone()))
        .push(metadata.section_position)
        .push(metadata.section_entered_at.map(|at| at.timestamp_millis()))
        .push(metadata.git_sha.clone())
        .push(metadata.git_branch.clone())
        .push(
            metadata
                .git_origin_url
                .as_ref()
                .map(SanitizedGitUrl::as_str),
        )
        .push(creation_memory_mode.unwrap_or("enabled"))
        .push(metadata.project_id.clone())
        .push(metadata.daybreak_enabled);
    let columns = [
        "id",
        "rollout_path",
        "created_at",
        "updated_at",
        "recency_at",
        "created_at_ms",
        "updated_at_ms",
        "recency_at_ms",
        "source",
        "originator",
        "creator_user_id",
        "creator_account_id",
        "history_mode",
        "thread_source",
        "agent_nickname",
        "agent_role",
        "agent_path",
        "model_provider",
        "model",
        "reasoning_effort",
        "cwd",
        "cli_version",
        "title",
        "name",
        "preview",
        "sandbox_policy",
        "approval_mode",
        "tokens_used",
        "first_user_message",
        "archived",
        "archived_at",
        "thread_section_id",
        "section_position",
        "section_entered_at_ms",
        "git_sha",
        "git_branch",
        "git_origin_url",
        "memory_mode",
        "project_id",
        "daybreak_enabled",
    ];
    // Paginated history is a one-way promotion; stale legacy metadata must
    // not downgrade it. `preview`/`originator`/`creator_*`/`git_*` keep the
    // existing non-empty/non-null value over a reconciled rollout upsert.
    let assignments = "
        rollout_path = excluded.rollout_path, created_at = excluded.created_at, updated_at = excluded.updated_at,
        created_at_ms = excluded.created_at_ms, updated_at_ms = excluded.updated_at_ms,
        source = excluded.source,
        originator = COALESCE(codex_threads.originator, excluded.originator),
        creator_user_id = COALESCE(codex_threads.creator_user_id, excluded.creator_user_id),
        creator_account_id = COALESCE(codex_threads.creator_account_id, excluded.creator_account_id),
        history_mode = CASE WHEN codex_threads.history_mode = 'paginated' THEN codex_threads.history_mode ELSE excluded.history_mode END,
        thread_source = excluded.thread_source, agent_nickname = excluded.agent_nickname,
        agent_role = excluded.agent_role, agent_path = excluded.agent_path, model_provider = excluded.model_provider,
        model = excluded.model, reasoning_effort = excluded.reasoning_effort, cwd = excluded.cwd,
        cli_version = excluded.cli_version, title = excluded.title,
        preview = CASE WHEN excluded.preview <> '' THEN excluded.preview ELSE codex_threads.preview END,
        sandbox_policy = excluded.sandbox_policy, approval_mode = excluded.approval_mode,
        tokens_used = excluded.tokens_used, first_user_message = excluded.first_user_message,
        archived = excluded.archived, archived_at = excluded.archived_at,
        git_sha = COALESCE(codex_threads.git_sha, excluded.git_sha),
        git_branch = COALESCE(codex_threads.git_branch, excluded.git_branch),
        git_origin_url = COALESCE(codex_threads.git_origin_url, excluded.git_origin_url)";
    let statement = format!(
        "INSERT INTO codex_threads ({cols}) VALUES ({vals}) ON CONFLICT (id) DO UPDATE SET {assignments}",
        cols = columns.join(", "),
        vals = values.exprs.join(", "),
    );
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(&statement, values.params)
        .await
        .map_err(internal)?;
    insert_thread_spawn_edge_from_source_if_absent(antfly, metadata.id, &metadata.source).await?;
    Ok(())
}

pub(crate) async fn upsert_thread(
    antfly: &Arc<Antfly>,
    metadata: &ThreadMetadata,
) -> anyhow::Result<()> {
    upsert_thread_with_creation_memory_mode(antfly, metadata, None).await
}

pub(crate) async fn insert_thread_if_absent(
    antfly: &Arc<Antfly>,
    metadata: &ThreadMetadata,
) -> anyhow::Result<bool> {
    let preview = metadata_preview(metadata);
    let mut values = ValueList::new();
    values
        .push(metadata.id.to_string())
        .push(metadata.rollout_path.display().to_string())
        .push(metadata.created_at.timestamp())
        .push(metadata.updated_at.timestamp())
        .push(metadata.recency_at.timestamp())
        .push(metadata.created_at.timestamp_millis())
        .push(metadata.updated_at.timestamp_millis())
        .push(metadata.recency_at.timestamp_millis())
        .push(metadata.source.clone())
        .push(metadata.originator.clone())
        .push(metadata.creator_user_id.clone())
        .push(metadata.creator_account_id.clone())
        .push(metadata.history_mode.as_str())
        .push(metadata.thread_source.as_ref().map(ThreadSource::as_str))
        .push(metadata.agent_nickname.clone())
        .push(metadata.agent_role.clone())
        .push(metadata.agent_path.clone())
        .push(metadata.model_provider.clone())
        .push(metadata.model.clone())
        .push(metadata.reasoning_effort.as_ref().map(enum_to_string))
        .push(metadata.cwd.display().to_string())
        .push(metadata.cli_version.clone())
        .push(metadata.title.clone())
        .push(metadata.name.clone())
        .push(preview.to_string())
        .push(metadata.sandbox_policy.clone())
        .push(metadata.approval_mode.clone())
        .push(metadata.tokens_used)
        .push(metadata.first_user_message.clone().unwrap_or_default())
        .push(metadata.archived_at.is_some())
        .push(metadata.archived_at.map(|at| at.timestamp()))
        .push(metadata.section.as_ref().map(|section| section.id.clone()))
        .push(metadata.section_position)
        .push(metadata.section_entered_at.map(|at| at.timestamp_millis()))
        .push(metadata.git_sha.clone())
        .push(metadata.git_branch.clone())
        .push(
            metadata
                .git_origin_url
                .as_ref()
                .map(SanitizedGitUrl::as_str),
        )
        .push("enabled")
        .push(metadata.project_id.clone())
        .push(metadata.daybreak_enabled);
    let columns = [
        "id",
        "rollout_path",
        "created_at",
        "updated_at",
        "recency_at",
        "created_at_ms",
        "updated_at_ms",
        "recency_at_ms",
        "source",
        "originator",
        "creator_user_id",
        "creator_account_id",
        "history_mode",
        "thread_source",
        "agent_nickname",
        "agent_role",
        "agent_path",
        "model_provider",
        "model",
        "reasoning_effort",
        "cwd",
        "cli_version",
        "title",
        "name",
        "preview",
        "sandbox_policy",
        "approval_mode",
        "tokens_used",
        "first_user_message",
        "archived",
        "archived_at",
        "thread_section_id",
        "section_position",
        "section_entered_at_ms",
        "git_sha",
        "git_branch",
        "git_origin_url",
        "memory_mode",
        "project_id",
        "daybreak_enabled",
    ];
    let statement = format!(
        "INSERT INTO codex_threads ({cols}) VALUES ({vals}) ON CONFLICT (id) DO NOTHING",
        cols = columns.join(", "),
        vals = values.exprs.join(", "),
    );
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(&statement, values.params)
        .await
        .map_err(internal)?;
    insert_thread_spawn_edge_from_source_if_absent(antfly, metadata.id, &metadata.source).await?;
    Ok(rows_affected > 0)
}

pub(crate) async fn apply_rollout_items(
    antfly: &Arc<Antfly>,
    builder: &ThreadMetadataBuilder,
    items: &[RolloutItem],
    default_provider: &str,
    new_thread_memory_mode: Option<&str>,
    updated_at_override: Option<DateTime<Utc>>,
) -> anyhow::Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let existing_metadata = get_thread(antfly, builder.id).await?;
    let mut metadata = existing_metadata
        .clone()
        .unwrap_or_else(|| builder.build(default_provider));
    metadata.rollout_path = builder.rollout_path.clone();
    for item in items {
        crate::extract::apply_rollout_item(&mut metadata, item, default_provider);
    }
    if let Some(existing_metadata) = existing_metadata.as_ref() {
        metadata.prefer_existing_git_info(existing_metadata);
    }
    if let Some(updated_at) = updated_at_override {
        metadata.updated_at = updated_at;
    } else if let Some(updated_at) =
        crate::paths::file_modified_time_utc(builder.rollout_path.as_path()).await
    {
        metadata.updated_at = updated_at;
    }
    if existing_metadata.is_none() {
        upsert_thread_with_creation_memory_mode(antfly, &metadata, new_thread_memory_mode).await?;
    } else {
        upsert_thread(antfly, &metadata).await?;
    }
    if let Some(memory_mode) = extract_memory_mode(items) {
        set_thread_memory_mode(antfly, builder.id, memory_mode.as_str()).await?;
    }
    Ok(())
}

pub(crate) async fn mark_archived(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    rollout_path: &Path,
    archived_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let Some(mut metadata) = get_thread(antfly, thread_id).await? else {
        return Ok(());
    };
    metadata.archived_at = Some(archived_at);
    metadata.rollout_path = rollout_path.to_path_buf();
    if let Some(updated_at) = crate::paths::file_modified_time_utc(rollout_path).await {
        metadata.updated_at = updated_at;
    }
    upsert_thread(antfly, &metadata).await
}

pub(crate) async fn mark_unarchived(
    antfly: &Arc<Antfly>,
    thread_id: ThreadId,
    rollout_path: &Path,
) -> anyhow::Result<()> {
    let Some(mut metadata) = get_thread(antfly, thread_id).await? else {
        return Ok(());
    };
    metadata.archived_at = None;
    metadata.rollout_path = rollout_path.to_path_buf();
    if let Some(updated_at) = crate::paths::file_modified_time_utc(rollout_path).await {
        metadata.updated_at = updated_at;
    }
    upsert_thread(antfly, &metadata).await
}

pub(crate) async fn find_rollout_path_by_id(
    antfly: &Arc<Antfly>,
    id: ThreadId,
    archived_only: Option<bool>,
) -> anyhow::Result<Option<PathBuf>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut statement = "SELECT rollout_path FROM codex_threads WHERE id = $1".to_string();
    let mut params = sql_params![id.to_string()];
    match archived_only {
        Some(true) => statement.push_str(" AND archived = true"),
        Some(false) => statement.push_str(" AND archived = false"),
        None => {}
    }
    let row = sql
        .fetch_optional(&statement, std::mem::take(&mut params))
        .await
        .map_err(internal)?;
    row.map(|row| row.string("rollout_path"))
        .transpose()
        .map(|path| path.map(PathBuf::from))
        .map_err(internal)
}

pub(crate) async fn replace_rollout_path_if_current(
    antfly: &Arc<Antfly>,
    id: ThreadId,
    expected: &Path,
    replacement: &Path,
) -> anyhow::Result<bool> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows_affected = sql
        .execute(
            "UPDATE codex_threads SET rollout_path = $1 WHERE id = $2 AND rollout_path = $3",
            sql_params![
                replacement.display().to_string(),
                id.to_string(),
                expected.display().to_string()
            ],
        )
        .await
        .map_err(internal)?;
    Ok(rows_affected == 1)
}

/// Appends `value` to `params` and returns its `$n` placeholder.
fn bind(params: &mut Vec<SqlValue>, value: impl Into<SqlValue>) -> String {
    params.push(value.into());
    format!("${}", params.len())
}

/// A `LIKE` pattern matching `term` as a literal substring, case-sensitively
/// (see the module's dialect notes: `strpos` is unsupported). `%`, `_`, and
/// `\` are escaped so the pattern cannot act as a glob.
fn contains_pattern(term: &str) -> String {
    let mut pattern = String::with_capacity(term.len() + 2);
    pattern.push('%');
    for ch in term.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern.push('%');
    pattern
}

fn sort_column(sort_key: SortKey) -> &'static str {
    match sort_key {
        SortKey::CreatedAt => "t.created_at_ms",
        SortKey::UpdatedAt => "t.updated_at_ms",
        SortKey::RecencyAt => "t.recency_at_ms",
        SortKey::SectionPosition => "t.section_position",
    }
}

fn build_list_query(
    filters: ThreadFilterOptions<'_>,
    relation_filter: Option<ThreadRelationFilter>,
    limit: usize,
) -> (String, Vec<SqlValue>) {
    let mut values: Vec<SqlValue> = Vec::new();
    let mut sql = String::new();
    let include_relation_parent = relation_filter.is_some();

    match relation_filter {
        Some(ThreadRelationFilter::DescendantsOf(ancestor)) => {
            let placeholder = bind(&mut values, ancestor.to_string());
            sql.push_str(&format!(
                "WITH RECURSIVE subtree(child_thread_id, parent_thread_id) AS ( \
                    SELECT child_thread_id, parent_thread_id FROM codex_thread_spawn_edges WHERE parent_thread_id = {placeholder} \
                    UNION \
                    SELECT edge.child_thread_id, edge.parent_thread_id FROM codex_thread_spawn_edges edge \
                    JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id \
                ) "
            ));
            sql.push_str(&format!(
                "SELECT {SELECT_COLUMNS}, subtree.parent_thread_id AS parent_thread_id FROM subtree \
                 JOIN codex_threads t ON t.id = subtree.child_thread_id"
            ));
        }
        Some(ThreadRelationFilter::DirectChildrenOf(parent)) => {
            let placeholder = bind(&mut values, parent.to_string());
            sql.push_str(&format!(
                "SELECT {SELECT_COLUMNS}, e.parent_thread_id AS parent_thread_id FROM codex_thread_spawn_edges e \
                 JOIN codex_threads t ON t.id = e.child_thread_id AND e.parent_thread_id = {placeholder}"
            ));
        }
        None => {
            sql.push_str(&format!("SELECT {SELECT_COLUMNS} FROM codex_threads t"));
        }
    }

    let archived_placeholder = bind(&mut values, filters.archived_only);
    sql.push_str(&format!(" WHERE t.archived = {archived_placeholder}"));
    let include_empty_preview = include_relation_parent;
    if !filters.archived_only && !include_empty_preview && !matches!(filters.section, Some(Some(_)))
    {
        sql.push_str(" AND t.preview <> ''");
    }
    match filters.section {
        Some(Some(section)) => {
            let placeholder = bind(&mut values, section.to_string());
            sql.push_str(&format!(" AND t.thread_section_id = {placeholder}"));
        }
        Some(None) => sql.push_str(" AND t.thread_section_id IS NULL"),
        None => {}
    }
    match filters.project_id {
        Some(Some(project_id)) => {
            let placeholder = bind(&mut values, project_id.to_string());
            sql.push_str(&format!(" AND t.project_id = {placeholder}"));
        }
        Some(None) => sql.push_str(" AND t.project_id IS NULL"),
        None => {}
    }
    if !filters.allowed_sources.is_empty() {
        let placeholders: Vec<String> = filters
            .allowed_sources
            .iter()
            .map(|source| bind(&mut values, source.clone()))
            .collect();
        sql.push_str(&format!(" AND t.source IN ({})", placeholders.join(", ")));
    }
    if let Some(providers) = filters.model_providers
        && !providers.is_empty()
    {
        let placeholders: Vec<String> = providers
            .iter()
            .map(|provider| bind(&mut values, provider.clone()))
            .collect();
        sql.push_str(&format!(
            " AND t.model_provider IN ({})",
            placeholders.join(", ")
        ));
    }
    match filters.cwd_filters {
        Some([]) => sql.push_str(" AND 1 = 0"),
        Some(cwds) => {
            let placeholders: Vec<String> = cwds
                .iter()
                .map(|cwd| bind(&mut values, cwd.display().to_string()))
                .collect();
            sql.push_str(&format!(" AND t.cwd IN ({})", placeholders.join(", ")));
        }
        None => {}
    }
    if let Some(term) = filters.search_term {
        let pattern = contains_pattern(term);
        let p1 = bind(&mut values, pattern.clone());
        let p2 = bind(&mut values, pattern.clone());
        let p3 = bind(&mut values, pattern);
        sql.push_str(&format!(
            " AND (COALESCE(t.name, '') LIKE {p1} OR t.title LIKE {p2} OR t.preview LIKE {p3})"
        ));
    }
    let include_thread_id_tiebreaker = include_relation_parent
        || matches!(
            filters.sort_key,
            SortKey::CreatedAt | SortKey::RecencyAt | SortKey::SectionPosition
        );
    if let Some(anchor) = filters.anchor {
        let anchor_ts = anchor.ts.timestamp_millis();
        let column = sort_column(filters.sort_key);
        let op = match filters.sort_direction {
            SortDirection::Asc => ">",
            SortDirection::Desc => "<",
        };
        let value_placeholder = bind(&mut values, anchor_ts);
        if include_thread_id_tiebreaker && let Some(anchor_id) = anchor.id {
            let id_placeholder = bind(&mut values, anchor_id.to_string());
            sql.push_str(&format!(
                " AND ({column} {op} {value_placeholder} OR ({column} = {value_placeholder} AND t.id {op} {id_placeholder}))"
            ));
        } else {
            sql.push_str(&format!(" AND {column} {op} {value_placeholder}"));
        }
    }
    let order_column = sort_column(filters.sort_key);
    let order_direction = match filters.sort_direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    sql.push_str(&format!(" ORDER BY {order_column} {order_direction}"));
    if include_thread_id_tiebreaker {
        sql.push_str(&format!(", t.id {order_direction}"));
    }
    let limit_placeholder = bind(&mut values, limit as i64);
    sql.push_str(&format!(" LIMIT {limit_placeholder}"));
    (sql, values)
}

pub(crate) async fn list_threads_matching(
    antfly: &Arc<Antfly>,
    page_size: usize,
    filters: ThreadFilterOptions<'_>,
    relation_filter: Option<ThreadRelationFilter>,
) -> anyhow::Result<ThreadsPage> {
    let limit = page_size.saturating_add(1);
    let sort_key = filters.sort_key;
    let (statement, values) = build_list_query(filters, relation_filter, limit);
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql.fetch_all(&statement, values).await.map_err(internal)?;
    let mut items = Vec::with_capacity(rows.len());
    let mut parent_thread_ids = std::collections::HashMap::new();
    for row in &rows {
        let item = thread_metadata_from_row(row)?;
        if relation_filter.is_some()
            && let Some(parent_thread_id) = row.opt_string("parent_thread_id")?
        {
            parent_thread_ids.insert(item.id, ThreadId::try_from(parent_thread_id)?);
        }
        items.push(item);
    }
    let num_scanned_rows = items.len();
    let next_anchor = if items.len() > page_size {
        if let Some(overflow_item) = items.pop() {
            parent_thread_ids.remove(&overflow_item.id);
        }
        items
            .last()
            .and_then(|item| anchor_from_item(item, sort_key, relation_filter.is_some()))
    } else {
        None
    };
    Ok(ThreadsPage {
        items,
        parent_thread_ids,
        next_anchor,
        num_scanned_rows,
    })
}

pub(crate) async fn list_thread_ids(
    antfly: &Arc<Antfly>,
    limit: usize,
    anchor: Option<&Anchor>,
    sort_key: SortKey,
    allowed_sources: &[String],
    model_providers: Option<&[String]>,
    archived_only: bool,
) -> anyhow::Result<Vec<ThreadId>> {
    let filters = ThreadFilterOptions {
        archived_only,
        allowed_sources,
        model_providers,
        cwd_filters: None,
        section: None,
        project_id: None,
        anchor,
        sort_key,
        sort_direction: SortDirection::Desc,
        search_term: None,
    };
    let (statement, values) = build_list_query(filters, None, limit);
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql.fetch_all(&statement, values).await.map_err(internal)?;
    rows.iter()
        .map(|row| Ok(ThreadId::try_from(row.string("id").map_err(internal)?)?))
        .collect()
}

pub(crate) async fn find_thread_by_exact_title(
    antfly: &Arc<Antfly>,
    title: &str,
    allowed_sources: &[String],
    model_providers: Option<&[String]>,
    archived_only: bool,
    cwd: Option<&Path>,
) -> anyhow::Result<Option<ThreadMetadata>> {
    let filters = ThreadFilterOptions {
        archived_only,
        allowed_sources,
        model_providers,
        cwd_filters: None,
        section: None,
        project_id: None,
        anchor: None,
        sort_key: SortKey::UpdatedAt,
        sort_direction: SortDirection::Desc,
        search_term: None,
    };
    let (mut statement, mut values) = build_list_query(filters, None, 1);
    // `build_list_query` always ends with `ORDER BY ... LIMIT $n`; splice
    // the extra filters in just before it.
    let order_by = statement
        .find(" ORDER BY")
        .ok_or_else(|| anyhow::anyhow!("list query is missing its ORDER BY clause"))?;
    let mut extra = String::new();
    let title_placeholder = bind(&mut values, title.to_string());
    extra.push_str(&format!(" AND t.title = {title_placeholder}"));
    if let Some(cwd) = cwd {
        let cwd_placeholder = bind(&mut values, cwd.display().to_string());
        extra.push_str(&format!(" AND t.cwd = {cwd_placeholder}"));
    }
    statement.insert_str(order_by, &extra);
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(&statement, values)
        .await
        .map_err(internal)?;
    row.map(|row| thread_metadata_from_row(&row)).transpose()
}

fn one_thread_id_from_rows(
    ids: Vec<ThreadId>,
    agent_path: &str,
) -> anyhow::Result<Option<ThreadId>> {
    let mut ids = ids;
    match ids.len() {
        0 => Ok(None),
        1 => Ok(ids.pop()),
        _ => anyhow::bail!("multiple agents found for canonical path `{agent_path}`"),
    }
}

pub(crate) async fn upsert_thread_spawn_edge(
    antfly: &Arc<Antfly>,
    parent_thread_id: ThreadId,
    child_thread_id: ThreadId,
    status: DirectionalThreadSpawnEdgeStatus,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_thread_spawn_edges (child_thread_id, parent_thread_id, status) VALUES ($1, $2, $3) \
         ON CONFLICT (child_thread_id) DO UPDATE SET parent_thread_id = excluded.parent_thread_id, status = excluded.status",
        sql_params![child_thread_id.to_string(), parent_thread_id.to_string(), status.as_ref()],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn set_thread_spawn_edge_status(
    antfly: &Arc<Antfly>,
    child_thread_id: ThreadId,
    status: DirectionalThreadSpawnEdgeStatus,
) -> anyhow::Result<()> {
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "UPDATE codex_thread_spawn_edges SET status = $1 WHERE child_thread_id = $2",
        sql_params![status.as_ref(), child_thread_id.to_string()],
    )
    .await
    .map_err(internal)?;
    Ok(())
}

pub(crate) async fn list_thread_spawn_children_matching(
    antfly: &Arc<Antfly>,
    parent_thread_id: ThreadId,
    status: Option<DirectionalThreadSpawnEdgeStatus>,
) -> anyhow::Result<Vec<ThreadId>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut statement =
        "SELECT child_thread_id FROM codex_thread_spawn_edges WHERE parent_thread_id = $1"
            .to_string();
    let mut values = sql_params![parent_thread_id.to_string()];
    if let Some(status) = status {
        values.push(status.as_ref().to_string().into());
        statement.push_str(&format!(" AND status = ${}", values.len()));
    }
    statement.push_str(" ORDER BY child_thread_id");
    let rows = sql.fetch_all(&statement, values).await.map_err(internal)?;
    rows.iter()
        .map(|row| {
            Ok(ThreadId::try_from(
                row.string("child_thread_id").map_err(internal)?,
            )?)
        })
        .collect()
}

pub(crate) async fn list_thread_spawn_descendants_matching(
    antfly: &Arc<Antfly>,
    root_thread_id: ThreadId,
    status: Option<DirectionalThreadSpawnEdgeStatus>,
) -> anyhow::Result<Vec<ThreadId>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let mut statement = String::from(
        "WITH RECURSIVE subtree(child_thread_id, depth) AS ( \
            SELECT child_thread_id, 1 FROM codex_thread_spawn_edges WHERE parent_thread_id = $1",
    );
    let mut values = sql_params![root_thread_id.to_string()];
    if let Some(status) = status {
        values.push(status.as_ref().to_string().into());
        let p = values.len();
        statement.push_str(&format!(" AND status = ${p}"));
        statement.push_str(
            " UNION ALL SELECT edge.child_thread_id, subtree.depth + 1 FROM codex_thread_spawn_edges edge \
             JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id WHERE status = ",
        );
        statement.push_str(&format!("${p}"));
    } else {
        statement.push_str(
            " UNION ALL SELECT edge.child_thread_id, subtree.depth + 1 FROM codex_thread_spawn_edges edge \
             JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id",
        );
    }
    statement
        .push_str(") SELECT child_thread_id FROM subtree ORDER BY depth ASC, child_thread_id ASC");
    let rows = sql.fetch_all(&statement, values).await.map_err(internal)?;
    rows.iter()
        .map(|row| {
            Ok(ThreadId::try_from(
                row.string("child_thread_id").map_err(internal)?,
            )?)
        })
        .collect()
}

pub(crate) async fn find_spawn_child_by_path(
    antfly: &Arc<Antfly>,
    parent_thread_id: ThreadId,
    agent_path: &str,
) -> anyhow::Result<Option<ThreadId>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT t.id FROM codex_thread_spawn_edges e JOIN codex_threads t ON t.id = e.child_thread_id \
             WHERE e.parent_thread_id = $1 AND t.agent_path = $2 ORDER BY t.id LIMIT 2",
            sql_params![parent_thread_id.to_string(), agent_path],
        )
        .await
        .map_err(internal)?;
    let ids = rows
        .iter()
        .map(|row| Ok(ThreadId::try_from(row.string("id").map_err(internal)?)?))
        .collect::<anyhow::Result<Vec<_>>>()?;
    one_thread_id_from_rows(ids, agent_path)
}

pub(crate) async fn find_spawn_descendant_by_path(
    antfly: &Arc<Antfly>,
    root_thread_id: ThreadId,
    agent_path: &str,
) -> anyhow::Result<Option<ThreadId>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "WITH RECURSIVE subtree(child_thread_id) AS ( \
                SELECT child_thread_id FROM codex_thread_spawn_edges WHERE parent_thread_id = $1 \
                UNION ALL \
                SELECT edge.child_thread_id FROM codex_thread_spawn_edges edge JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id \
             ) SELECT t.id FROM subtree JOIN codex_threads t ON t.id = subtree.child_thread_id WHERE t.agent_path = $2 ORDER BY t.id LIMIT 2",
            sql_params![root_thread_id.to_string(), agent_path],
        )
        .await
        .map_err(internal)?;
    let ids = rows
        .iter()
        .map(|row| Ok(ThreadId::try_from(row.string("id").map_err(internal)?)?))
        .collect::<anyhow::Result<Vec<_>>>()?;
    one_thread_id_from_rows(ids, agent_path)
}

/// Deletes a set of threads and everything thread-scoped (spec §5): spawn
/// edges, the paginated-history projection, and the `codex_threads` rows
/// themselves (whose FKs cascade `codex_thread_dynamic_tools` /
/// `codex_thread_attachments` / `codex_guardian_review_feedback`). Goals,
/// the queue, memory jobs/outputs, and logs are owned by other stores; this
/// deletes their rows by SQL without importing their modules, mirroring
/// `delete_threads_strict`'s own cross-store cleanup.
pub(crate) async fn delete_threads_strict(
    antfly: &Arc<Antfly>,
    thread_ids: &[ThreadId],
) -> anyhow::Result<u64> {
    if thread_ids.is_empty() {
        return Ok(0);
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let ids: Vec<String> = thread_ids.iter().map(ThreadId::to_string).collect();
    let mut rows_affected = 0u64;
    for id in &ids {
        // `codex_thread_goal_continuation_deferrals` cascades from
        // `codex_thread_goals` (`ON DELETE CASCADE`).
        for table in [
            "codex_thread_turns",
            "codex_thread_items",
            "codex_thread_realtime_items",
            "codex_thread_history_projection_state",
            "codex_stage1_outputs",
            "codex_thread_goals",
            "codex_queued_items",
        ] {
            sql.execute(
                &format!("DELETE FROM {table} WHERE thread_id = $1"),
                sql_params![id.clone()],
            )
            .await
            .map_err(internal)?;
        }
        // Best-effort: deletes a per-thread memory job if the memory
        // pipeline keys `codex_jobs.job_key` by thread id for its
        // thread-scoped kinds (e.g. "stage1").
        sql.execute(
            "DELETE FROM codex_jobs WHERE job_key = $1",
            sql_params![id.clone()],
        )
        .await
        .map_err(internal)?;
        // Bumps the queue revision like the queue store's own writes do,
        // so a poller watching this thread's queue observes one final
        // change (now empty) instead of going stale silently.
        sql.execute(
            "INSERT INTO codex_queued_thread_revisions (thread_id, revision) VALUES ($1, 1) \
             ON CONFLICT (thread_id) DO UPDATE SET revision = codex_queued_thread_revisions.revision + 1",
            sql_params![id.clone()],
        )
        .await
        .map_err(internal)?;
        sql.execute(
            "DELETE FROM codex_thread_spawn_edges WHERE parent_thread_id = $1 OR child_thread_id = $1",
            sql_params![id.clone()],
        )
        .await
        .map_err(internal)?;
        rows_affected += sql
            .execute(
                "DELETE FROM codex_threads WHERE id = $1",
                sql_params![id.clone()],
            )
            .await
            .map_err(internal)?;
    }
    Ok(rows_affected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SortKey;
    use crate::StateRuntime;
    use crate::ThreadRelationFilter;
    use crate::runtime::test_support::test_thread_metadata;
    use crate::runtime::test_support::unique_temp_dir;
    use pretty_assertions::assert_eq;

    async fn test_runtime() -> (std::sync::Arc<StateRuntime>, Arc<Antfly>, PathBuf) {
        let dir = unique_temp_dir();
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let mut config = codex_antfly::AntflyConfig::embedded(dir.join("codex.aflite"));
        config.embedder = None;
        let antfly = Arc::new(Antfly::new(config));
        let runtime = StateRuntime::init_antfly(Arc::clone(&antfly), "test-provider".to_string())
            .await
            .expect("init antfly state runtime");
        (runtime, antfly, dir)
    }

    async fn cleanup(antfly: &Antfly, dir: PathBuf) {
        antfly.close().await.expect("close antfly");
        let _ = tokio::fs::remove_dir_all(dir).await;
    }

    fn thread_id(seed: u8) -> ThreadId {
        ThreadId::from_string(&format!("00000000-0000-0000-0000-{seed:012}"))
            .expect("valid thread id")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_and_get_thread_round_trips() {
        let (runtime, antfly, dir) = test_runtime().await;
        let id = thread_id(1);
        let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("upsert thread");

        let loaded = runtime
            .get_thread(id)
            .await
            .expect("get thread")
            .expect("thread exists");
        assert_eq!(loaded.id, id);
        assert_eq!(loaded.model_provider, "test-provider");
        assert_eq!(loaded.preview, Some("hello".to_string()));
        assert_eq!(loaded.cwd, PathBuf::from("/work/repo"));

        assert!(
            runtime
                .set_thread_memory_mode(id, "polluted")
                .await
                .expect("set memory mode")
        );
        assert_eq!(
            runtime
                .get_thread_memory_mode(id)
                .await
                .expect("get memory mode"),
            Some("polluted".to_string())
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_threads_pages_newest_first() {
        let (runtime, antfly, dir) = test_runtime().await;
        let mut ids = Vec::new();
        for seed in 0..5u8 {
            let id = thread_id(seed);
            let mut metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
            metadata.updated_at += chrono::Duration::seconds(seed as i64);
            metadata.recency_at = metadata.updated_at;
            metadata.created_at = metadata.updated_at;
            runtime
                .upsert_thread(&metadata)
                .await
                .expect("upsert thread");
            ids.push(id);
        }

        let page = runtime
            .list_threads(
                2,
                ThreadFilterOptions {
                    archived_only: false,
                    allowed_sources: &[],
                    model_providers: None,
                    cwd_filters: None,
                    section: None,
                    project_id: None,
                    anchor: None,
                    sort_key: SortKey::UpdatedAt,
                    sort_direction: SortDirection::Desc,
                    search_term: None,
                },
            )
            .await
            .expect("list threads");
        assert_eq!(
            page.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![ids[4], ids[3]]
        );
        assert!(page.next_anchor.is_some());

        let next = runtime
            .list_threads(
                2,
                ThreadFilterOptions {
                    archived_only: false,
                    allowed_sources: &[],
                    model_providers: None,
                    cwd_filters: None,
                    section: None,
                    project_id: None,
                    anchor: page.next_anchor.as_ref(),
                    sort_key: SortKey::UpdatedAt,
                    sort_direction: SortDirection::Desc,
                    search_term: None,
                },
            )
            .await
            .expect("list threads page 2");
        assert_eq!(
            next.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![ids[2], ids[1]]
        );

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_edges_track_children_and_descendants() {
        let (runtime, antfly, dir) = test_runtime().await;
        let parent = thread_id(10);
        let child = thread_id(11);
        let grandchild = thread_id(12);
        for id in [parent, child, grandchild] {
            let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
            runtime
                .upsert_thread(&metadata)
                .await
                .expect("upsert thread");
        }
        runtime
            .upsert_thread_spawn_edge(parent, child, DirectionalThreadSpawnEdgeStatus::Open)
            .await
            .expect("upsert spawn edge");
        runtime
            .upsert_thread_spawn_edge(child, grandchild, DirectionalThreadSpawnEdgeStatus::Open)
            .await
            .expect("upsert spawn edge");

        assert_eq!(
            runtime
                .list_thread_spawn_children(parent)
                .await
                .expect("list children"),
            vec![child]
        );
        let mut descendants = runtime
            .list_thread_spawn_descendants(parent)
            .await
            .expect("list descendants");
        descendants.sort_by_key(ToString::to_string);
        let mut expected = vec![child, grandchild];
        expected.sort_by_key(ToString::to_string);
        assert_eq!(descendants, expected);

        runtime
            .set_thread_spawn_edge_status(child, DirectionalThreadSpawnEdgeStatus::Closed)
            .await
            .expect("set spawn edge status");
        assert_eq!(
            runtime
                .list_thread_spawn_children_with_status(
                    parent,
                    DirectionalThreadSpawnEdgeStatus::Open
                )
                .await
                .expect("list open children"),
            Vec::<ThreadId>::new()
        );

        let page = runtime
            .list_threads_by_relation(
                10,
                ThreadRelationFilter::DirectChildrenOf(parent),
                ThreadFilterOptions {
                    archived_only: false,
                    allowed_sources: &[],
                    model_providers: None,
                    cwd_filters: None,
                    section: None,
                    project_id: None,
                    anchor: None,
                    sort_key: SortKey::UpdatedAt,
                    sort_direction: SortDirection::Desc,
                    search_term: None,
                },
            )
            .await
            .expect("list by relation");
        assert_eq!(
            page.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![child]
        );
        assert_eq!(page.parent_thread_ids.get(&child), Some(&parent));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sections_create_move_and_list() {
        let (runtime, antfly, dir) = test_runtime().await;
        let a = thread_id(20);
        let b = thread_id(21);
        for id in [a, b] {
            let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
            runtime
                .upsert_thread(&metadata)
                .await
                .expect("upsert thread");
        }
        let section = runtime
            .create_thread_section("Work", None)
            .await
            .expect("create section");
        assert!(
            runtime
                .move_thread_to_section(a, Some(&section.id), None)
                .await
                .expect("move a")
        );
        assert!(
            runtime
                .move_thread_to_section(b, Some(&section.id), Some(a))
                .await
                .expect("move b before a")
        );

        let page = runtime
            .list_threads(
                10,
                ThreadFilterOptions {
                    archived_only: false,
                    allowed_sources: &[],
                    model_providers: None,
                    cwd_filters: None,
                    section: Some(Some(section.id.as_str())),
                    project_id: None,
                    anchor: None,
                    sort_key: SortKey::SectionPosition,
                    sort_direction: SortDirection::Asc,
                    search_term: None,
                },
            )
            .await
            .expect("list section");
        assert_eq!(
            page.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![b, a]
        );

        assert!(
            runtime
                .delete_thread_section(&section.id)
                .await
                .expect("delete section")
        );
        let ordering = runtime
            .get_thread_section_ordering(&[a, b])
            .await
            .expect("get ordering");
        assert_eq!(ordering.get(&a).map(|(position, _)| *position), Some(None));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn projects_crud_round_trips() {
        let (runtime, antfly, dir) = test_runtime().await;
        let id = thread_id(30);
        let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("upsert thread");

        let created = runtime
            .create_project(
                "Antfly".to_string(),
                vec![crate::ProjectRoot {
                    path: "/repo".to_string(),
                }],
                Default::default(),
                &[id.to_string()],
                "key-1",
            )
            .await
            .expect("create project");
        assert!(created.created);

        let repeat = runtime
            .create_project(
                "Ignored".to_string(),
                Vec::new(),
                Default::default(),
                &[],
                "key-1",
            )
            .await
            .expect("repeat create project");
        assert!(!repeat.created);
        assert_eq!(repeat.project.id, created.project.id);

        let read = runtime
            .get_project(&created.project.id)
            .await
            .expect("get project")
            .expect("project exists");
        assert_eq!(read.name, "Antfly");

        let thread_after = runtime
            .get_thread(id)
            .await
            .expect("get thread")
            .expect("thread exists");
        assert_eq!(thread_after.project_id, Some(created.project.id.clone()));

        let updated = runtime
            .update_project(
                &created.project.id,
                Some("Antfly Renamed".to_string()),
                None,
                None,
            )
            .await
            .expect("update project")
            .expect("project exists");
        assert!(updated.1);
        assert_eq!(updated.0.name, "Antfly Renamed");

        let deleted = runtime
            .delete_project(&created.project.id)
            .await
            .expect("delete project")
            .expect("project existed");
        assert_eq!(deleted.0, vec![id.to_string()]);

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attachments_add_list_remove() {
        let (runtime, antfly, dir) = test_runtime().await;
        let id = thread_id(40);
        let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("upsert thread");

        let payload = serde_json::json!({"k": "v"});
        let outcome = runtime
            .add_thread_attachment(id, "note", "ident-1", &payload)
            .await
            .expect("add attachment");
        assert!(matches!(
            outcome,
            crate::AddThreadAttachmentOutcome::Created(_)
        ));

        let page = runtime
            .list_thread_attachments(id, None, 10)
            .await
            .expect("list attachments");
        assert_eq!(page.attachments.len(), 1);

        let removed = runtime
            .remove_thread_attachment(id, "note", "ident-1")
            .await
            .expect("remove attachment");
        assert!(matches!(
            removed,
            crate::RemoveThreadAttachmentOutcome::Removed(_)
        ));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn git_info_and_paginated_promotion_round_trip() {
        let (runtime, antfly, dir) = test_runtime().await;
        let id = thread_id(60);
        let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("upsert thread");

        // A bound `NULL` (no value yet for git_branch/git_origin_url) inside
        // a `CASE WHEN $n THEN $m ELSE col END` must not hit the embedded
        // driver's `22023` ("parameter or row value does not match the
        // required type") error the module's dialect notes describe for a
        // literal `VALUES` position.
        assert!(
            runtime
                .update_thread_git_info(id, Some(Some("abc123")), None, None)
                .await
                .expect("update git info")
        );
        let loaded = runtime
            .get_thread(id)
            .await
            .expect("get thread")
            .expect("thread exists");
        assert_eq!(loaded.git_sha, Some("abc123".to_string()));
        assert_eq!(loaded.git_branch, None);

        assert!(
            runtime
                .mark_thread_paginated(id, Some("Legacy Title"))
                .await
                .expect("mark thread paginated")
        );
        let loaded = runtime
            .get_thread(id)
            .await
            .expect("get thread")
            .expect("thread exists");
        assert_eq!(
            loaded.history_mode,
            codex_protocol::protocol::ThreadHistoryMode::Paginated
        );
        assert_eq!(loaded.name, Some("Legacy Title".to_string()));

        cleanup(&antfly, dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_threads_strict_removes_everything() {
        let (runtime, antfly, dir) = test_runtime().await;
        let id = thread_id(50);
        let metadata = test_thread_metadata(dir.as_path(), id, PathBuf::from("/work/repo"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("upsert thread");
        runtime
            .add_thread_attachment(id, "note", "ident-1", &serde_json::json!({}))
            .await
            .expect("add attachment");

        let rows_affected = runtime
            .delete_threads_strict(&[id])
            .await
            .expect("delete thread");
        assert_eq!(rows_affected, 1);
        assert_eq!(runtime.get_thread(id).await.expect("get thread"), None);
        let page = runtime.list_thread_attachments(id, None, 10).await;
        assert!(page.is_err() || page.unwrap().attachments.is_empty());

        cleanup(&antfly, dir).await;
    }
}
