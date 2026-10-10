//! The durable thread record (`codex_threads`) and its projection to
//! [`StoredThread`].
//!
//! `ThreadRecord` keeps the same `created`/`patch` shape the key-value
//! implementation used (so every getter below, and every call site that
//! reads them, is unchanged): `patch.X` always wins over `created.X` when
//! both are present. [`ThreadRecord::from_row`] reconstructs a record from a
//! `codex_threads` row by writing the column's *effective* value into
//! `patch.X` (even for fields the row never patched), so `created.X` is only
//! ever a placeholder for fields no getter reads directly (dynamic tools,
//! base instructions, the initial window id, ...) - those are never needed
//! again once the thread's `SessionMeta` rollout item is durable.

use std::path::PathBuf;

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use codex_antfly::sql::SqlRow;
use codex_antfly::sql::SqlValue;
use codex_protocol::ThreadId;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::TurnItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TokenUsage;
use codex_rollout::RolloutItem;
use codex_state::ThreadSectionAppearance;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use crate::CreateThreadParams;
use crate::ExtraConfig;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadMetadataPatch;
use crate::ThreadPersistenceMetadata;
use crate::ThreadSortKey;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Every column `codex_threads` carries that a thread record reads or
/// writes, aliased `t`, with the section name/appearance and spawn-edge
/// parent joined in (mirrors `threads.sandbox_policy`'s subquery pattern in
/// `state/src/runtime/threads.rs::push_thread_select_columns`).
pub(crate) const SELECT_THREAD: &str = "
SELECT
    t.id, t.rollout_path, t.created_at, t.updated_at, t.recency_at,
    t.created_at_ms, t.updated_at_ms, t.recency_at_ms,
    t.source, t.thread_source, t.model_provider, t.model, t.reasoning_effort,
    t.cwd, t.title, t.name, t.preview, t.first_user_message,
    t.approval_mode, t.archived, t.archived_at,
    t.git_sha, t.git_branch, t.git_origin_url, t.cli_version,
    t.agent_nickname, t.agent_role, t.agent_path,
    t.memory_mode, t.history_mode,
    t.thread_section_id, t.section_position, t.section_entered_at_ms,
    (SELECT s.name FROM codex_thread_sections s WHERE s.id = t.thread_section_id) AS section_name,
    (SELECT s.appearance FROM codex_thread_sections s WHERE s.id = t.thread_section_id) AS section_appearance,
    t.project_id, t.originator, t.daybreak_enabled,
    t.creator_user_id, t.creator_account_id, t.extra,
    t.forked_from_id,
    (SELECT e.parent_thread_id FROM codex_thread_spawn_edges e WHERE e.child_thread_id = t.id) AS parent_thread_id,
    t.history_base_thread_id, t.history_base_end_ordinal,
    t.subagent_history_start_ordinal, t.multi_agent_version, t.next_ordinal
FROM codex_threads t";

/// Columns written by [`ThreadRecord::upsert_params`], in the same order as
/// the `$n` placeholders [`upsert_sql`] builds.
const UPSERT_COLUMNS: &[&str] = &[
    "id",
    "rollout_path",
    "created_at",
    "updated_at",
    "created_at_ms",
    "updated_at_ms",
    "recency_at",
    "recency_at_ms",
    "source",
    "thread_source",
    "model_provider",
    "model",
    "reasoning_effort",
    "cwd",
    "title",
    "name",
    "preview",
    "first_user_message",
    "sandbox_policy",
    "approval_mode",
    "archived",
    "archived_at",
    "git_sha",
    "git_branch",
    "git_origin_url",
    "cli_version",
    "agent_nickname",
    "agent_role",
    "agent_path",
    "memory_mode",
    "history_mode",
    "thread_section_id",
    "section_position",
    "section_entered_at_ms",
    "project_id",
    "originator",
    "daybreak_enabled",
    "creator_user_id",
    "creator_account_id",
    "extra",
    "forked_from_id",
    "history_base_thread_id",
    "history_base_end_ordinal",
    "subagent_history_start_ordinal",
    "multi_agent_version",
    "next_ordinal",
];

/// Builds the `INSERT ... ON CONFLICT (id) DO UPDATE SET ...` statement over
/// [`UPSERT_COLUMNS`] for one call's `values` (same order, from
/// [`ThreadRecord::upsert_params`]), and the parameters to bind. Every write
/// replaces the whole row: a thread record has no secondary indexes of its
/// own anymore (listing reads `codex_threads` directly), so there is nothing
/// else to keep in sync.
///
/// A `NULL` value is written as the literal `NULL` rather than a bound
/// parameter, out of caution: a bound `NULL` carries no type of its own,
/// which has been a source of `22023` ("a parameter or row value does not
/// match the required type") errors elsewhere. A literal has no such
/// ambiguity.
pub(crate) fn upsert_statement(values: Vec<SqlValue>) -> (String, Vec<SqlValue>) {
    let mut value_exprs = Vec::with_capacity(values.len());
    let mut bound: Vec<SqlValue> = Vec::with_capacity(values.len());
    for value in values {
        if matches!(value, SqlValue::Null) {
            value_exprs.push("NULL".to_string());
        } else {
            bound.push(value);
            value_exprs.push(format!("${}", bound.len()));
        }
    }
    let assignments: Vec<String> = UPSERT_COLUMNS
        .iter()
        .skip(1) // id
        .map(|column| format!("{column} = excluded.{column}"))
        .collect();
    let sql = format!(
        "INSERT INTO codex_threads ({cols}) VALUES ({vals}) ON CONFLICT (id) DO UPDATE SET {sets}",
        cols = UPSERT_COLUMNS.join(", "),
        vals = value_exprs.join(", "),
        sets = assignments.join(", "),
    );
    (sql, bound)
}

/// `serde_json::to_value`, unwrapped to a bare string for string-shaped
/// enums and to compact JSON text otherwise (mirrors
/// `codex-state`'s private `extract::enum_to_string`, so a value this store
/// writes and `StateRuntime`'s SQLite/Antfly paths write for the same enum
/// land on the same text).
fn enum_to_string<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(text)) => text,
        Ok(other) => other.to_string(),
        Err(_) => String::new(),
    }
}

/// Inverse of [`enum_to_string`]: tries the text as JSON first (object/array
/// variants serialize that way), then as a bare JSON string (unit variants).
fn enum_from_str<T: serde::de::DeserializeOwned>(text: &str) -> Option<T> {
    serde_json::from_str(text)
        .ok()
        .or_else(|| serde_json::from_value(Value::String(text.to_string())).ok())
}

/// The text `codex_threads.source` stores for a [`SessionSource`], shared by
/// `listing.rs`'s `allowed_sources` filter so it matches what
/// [`ThreadRecord::upsert_params`] writes.
pub(crate) fn source_text(source: &SessionSource) -> String {
    enum_to_string(source)
}

fn opt_enum_to_string<T: Serialize>(value: Option<&T>) -> Option<String> {
    value.map(enum_to_string)
}

fn opt_enum_from_str<T: serde::de::DeserializeOwned>(text: Option<String>) -> Option<T> {
    text.and_then(|text| enum_from_str(text.as_str()))
}

/// Everything persisted about one thread except its history items.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ThreadRecord {
    pub(crate) created: CreateThreadParams,
    /// Every metadata patch applied so far, merged in order.
    pub(crate) patch: ThreadMetadataPatch,
    /// When the thread first became durable.
    pub(crate) materialized_at: DateTime<Utc>,
    pub(crate) archived_at: Option<DateTime<Utc>>,
    /// Ordinal the next persisted item receives.
    pub(crate) next_ordinal: u64,
    pub(crate) section: Option<String>,
    pub(crate) section_position: Option<i64>,
    pub(crate) section_entered_at: Option<DateTime<Utc>>,
    /// Denormalized from the section definition at read time (a join, not a
    /// stored column); `None` for a section id with no persisted definition
    /// (an ad hoc section tag).
    #[serde(default)]
    pub(crate) section_name: Option<String>,
    #[serde(default)]
    pub(crate) section_appearance: Option<ThreadSectionAppearance>,
    /// Rollout path this thread was imported from, if any.
    pub(crate) legacy_rollout_path: Option<PathBuf>,
}

impl ThreadRecord {
    pub(crate) fn new(created: CreateThreadParams, now: DateTime<Utc>) -> Self {
        Self {
            created,
            patch: ThreadMetadataPatch::default(),
            materialized_at: now,
            archived_at: None,
            next_ordinal: 0,
            section: None,
            section_position: None,
            section_entered_at: None,
            section_name: None,
            section_appearance: None,
            legacy_rollout_path: None,
        }
    }

    pub(crate) fn thread_id(&self) -> ThreadId {
        self.created.thread_id
    }

    pub(crate) fn history_mode(&self) -> ThreadHistoryMode {
        self.created.history_mode
    }

    pub(crate) fn created_at(&self) -> DateTime<Utc> {
        self.patch.created_at.unwrap_or(self.materialized_at)
    }

    pub(crate) fn updated_at(&self) -> DateTime<Utc> {
        self.patch.updated_at.unwrap_or_else(|| self.created_at())
    }

    pub(crate) fn recency_at(&self) -> DateTime<Utc> {
        self.patch
            .advance_recency_at
            .unwrap_or_else(|| self.updated_at())
    }

    /// Millisecond timestamp used for `sort_key` listings.
    pub(crate) fn sort_millis(&self, sort_key: ThreadSortKey) -> i64 {
        match sort_key {
            ThreadSortKey::CreatedAt => self.created_at(),
            ThreadSortKey::UpdatedAt => self.updated_at(),
            ThreadSortKey::RecencyAt | ThreadSortKey::SectionPosition => self.recency_at(),
        }
        .timestamp_millis()
    }

    pub(crate) fn preview(&self) -> String {
        self.patch.preview.clone().unwrap_or_default()
    }

    pub(crate) fn title(&self) -> String {
        self.patch.title.clone().unwrap_or_default()
    }

    pub(crate) fn name(&self) -> Option<String> {
        self.patch.name.clone().flatten()
    }

    pub(crate) fn project_id(&self) -> Option<String> {
        self.patch.project_id.clone().flatten()
    }

    pub(crate) fn cwd(&self) -> PathBuf {
        self.patch
            .cwd
            .clone()
            .or_else(|| self.created.metadata.cwd.clone())
            .unwrap_or_default()
    }

    pub(crate) fn model_provider(&self) -> String {
        self.patch
            .model_provider
            .clone()
            .unwrap_or_else(|| self.created.metadata.model_provider.clone())
    }

    pub(crate) fn memory_mode(&self) -> ThreadMemoryMode {
        self.patch
            .memory_mode
            .unwrap_or(self.created.metadata.memory_mode)
    }

    pub(crate) fn source(&self) -> SessionSource {
        self.patch
            .source
            .clone()
            .unwrap_or_else(|| self.created.source.clone())
    }

    pub(crate) fn originator(&self) -> Option<String> {
        self.patch.originator.clone().or_else(|| {
            (!self.created.originator.is_empty()).then(|| self.created.originator.clone())
        })
    }

    pub(crate) fn creator_user_id(&self) -> Option<String> {
        self.patch
            .creator_user_id
            .clone()
            .or_else(|| self.created.creator_user_id.clone())
    }

    pub(crate) fn creator_account_id(&self) -> Option<String> {
        self.patch
            .creator_account_id
            .clone()
            .or_else(|| self.created.creator_account_id.clone())
    }

    pub(crate) fn thread_source(&self) -> Option<ThreadSource> {
        self.patch
            .thread_source
            .clone()
            .unwrap_or_else(|| self.created.thread_source.clone())
    }

    pub(crate) fn agent_nickname(&self) -> Option<String> {
        self.patch
            .agent_nickname
            .clone()
            .unwrap_or_else(|| self.created.source.get_nickname())
    }

    pub(crate) fn agent_role(&self) -> Option<String> {
        self.patch
            .agent_role
            .clone()
            .unwrap_or_else(|| self.created.source.get_agent_role())
    }

    pub(crate) fn agent_path(&self) -> Option<String> {
        self.patch
            .agent_path
            .clone()
            .unwrap_or_else(|| self.created.source.get_agent_path().map(Into::into))
    }

    pub(crate) fn cli_version(&self) -> String {
        self.patch
            .cli_version
            .clone()
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
    }

    pub(crate) fn approval_mode(&self) -> AskForApproval {
        self.patch
            .approval_mode
            .unwrap_or(AskForApproval::OnRequest)
    }

    pub(crate) fn permission_profile(&self) -> PermissionProfile {
        self.patch
            .permission_profile
            .clone()
            .unwrap_or_else(PermissionProfile::read_only)
    }

    pub(crate) fn token_usage(&self) -> Option<TokenUsage> {
        self.patch.token_usage.clone()
    }

    pub(crate) fn first_user_message(&self) -> Option<String> {
        self.patch.first_user_message.clone()
    }

    pub(crate) fn daybreak_enabled(&self) -> Option<bool> {
        self.patch.daybreak_enabled
    }

    pub(crate) fn git_sha(&self) -> Option<String> {
        self.patch
            .git_info
            .as_ref()
            .and_then(|info| info.sha.clone())
            .flatten()
    }

    pub(crate) fn git_branch(&self) -> Option<String> {
        self.patch
            .git_info
            .as_ref()
            .and_then(|info| info.branch.clone())
            .flatten()
    }

    pub(crate) fn git_origin_url(&self) -> Option<codex_protocol::SanitizedGitUrl> {
        self.patch
            .git_info
            .as_ref()
            .and_then(|info| info.origin_url.clone())
            .flatten()
    }

    pub(crate) fn to_stored(&self, history: Option<Vec<RolloutItem>>) -> StoredThread {
        let thread_id = self.thread_id();
        let created = &self.created;
        StoredThread {
            originator: self.originator(),
            thread_id,
            extra_config: created.extra_config.clone(),
            rollout_path: None,
            forked_from_id: created.forked_from_id,
            parent_thread_id: created.parent_thread_id,
            preview: self.preview(),
            name: self.name(),
            model_provider: self.model_provider(),
            model: self.patch.model.clone(),
            reasoning_effort: self.patch.reasoning_effort.clone().flatten(),
            created_at: self.created_at(),
            updated_at: self.updated_at(),
            recency_at: self.recency_at(),
            archived_at: self.archived_at,
            section: self.section.clone().map(|id| {
                let (name, appearance) = if id == codex_state::PINNED_THREAD_SECTION_ID {
                    (codex_state::PINNED_THREAD_SECTION_NAME.to_string(), None)
                } else {
                    (
                        self.section_name.clone().unwrap_or_else(|| id.clone()),
                        self.section_appearance.clone(),
                    )
                };
                codex_state::ThreadSection {
                    id,
                    name,
                    appearance,
                }
            }),
            section_position: self.section_position,
            section_entered_at: self.section_entered_at,
            project_id: self.project_id(),
            daybreak_enabled: self.daybreak_enabled(),
            cwd: self.cwd(),
            cli_version: self.cli_version(),
            source: self.source(),
            history_mode: created.history_mode,
            thread_source: self.thread_source(),
            agent_nickname: self.agent_nickname(),
            agent_role: self.agent_role(),
            agent_path: self.agent_path(),
            git_info: git_info_from_record(self),
            approval_mode: self.approval_mode(),
            permission_profile: self.permission_profile(),
            token_usage: self.token_usage(),
            first_user_message: self.first_user_message(),
            history: history.map(|items| StoredThreadHistory {
                revision: None,
                thread_id,
                items,
            }),
        }
    }

    /// Params for [`upsert_sql`], in [`UPSERT_COLUMNS`] order.
    pub(crate) fn upsert_params(&self) -> ThreadStoreResult<Vec<SqlValue>> {
        let extra = json!({
            "has_extra_config": self.created.extra_config.is_some(),
            "permission_profile": self.permission_profile_json()?,
            "token_usage": self.token_usage(),
        });
        let history_base = self.created.history_base;
        Ok(vec![
            self.thread_id().to_string().into(),
            self.legacy_rollout_path
                .as_ref()
                .map(|path| path.to_string_lossy().to_string())
                .unwrap_or_default()
                .into(),
            self.created_at().timestamp().into(),
            self.updated_at().timestamp().into(),
            self.created_at().timestamp_millis().into(),
            self.updated_at().timestamp_millis().into(),
            self.recency_at().timestamp().into(),
            self.recency_at().timestamp_millis().into(),
            enum_to_string(&self.source()).into(),
            opt_enum_to_string(self.thread_source().as_ref()).into(),
            self.model_provider().into(),
            self.patch.model.clone().into(),
            opt_enum_to_string(self.patch.reasoning_effort.clone().flatten().as_ref()).into(),
            self.cwd().to_string_lossy().to_string().into(),
            self.title().into(),
            self.name().into(),
            self.preview().into(),
            self.first_user_message().unwrap_or_default().into(),
            // Not an `AntflyThreadStore` concept; `codex_threads.sandbox_policy`
            // has no DEFAULT (unlike `tokens_used`/`has_user_event`), so it must
            // still be written on every upsert.
            "".to_string().into(),
            enum_to_string(&self.approval_mode()).into(),
            self.archived_at.is_some().into(),
            self.archived_at.map(|at| at.timestamp()).into(),
            self.git_sha().into(),
            self.git_branch().into(),
            self.git_origin_url()
                .map(|url| url.as_str().to_string())
                .into(),
            self.cli_version().into(),
            self.agent_nickname().into(),
            self.agent_role().into(),
            self.agent_path().into(),
            enum_to_string(&self.memory_mode()).into(),
            enum_to_string(&self.created.history_mode).into(),
            self.section.clone().into(),
            self.section_position.into(),
            self.section_entered_at
                .map(|at| at.timestamp_millis())
                .into(),
            self.project_id().into(),
            self.originator().into(),
            self.daybreak_enabled().into(),
            self.creator_user_id().into(),
            self.creator_account_id().into(),
            extra.into(),
            self.created.forked_from_id.map(|id| id.to_string()).into(),
            history_base.map(|base| base.thread_id.to_string()).into(),
            history_base
                .map(|base| base.end_ordinal_exclusive as i64)
                .into(),
            self.created
                .subagent_history_start_ordinal
                .map(|ordinal| ordinal as i64)
                .into(),
            opt_enum_to_string(self.created.multi_agent_version.as_ref()).into(),
            (self.next_ordinal as i64).into(),
        ])
    }

    fn permission_profile_json(&self) -> ThreadStoreResult<Value> {
        serde_json::to_value(self.permission_profile()).map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize permission profile: {err}"),
        })
    }

    /// Reconstructs a record from one [`SELECT_THREAD`] row. Every
    /// overridable field lands in `patch` at its *effective* value (see the
    /// module docs); `created` carries only the fields no getter reads
    /// through `patch` (fork lineage, spawn parent, immutable identity).
    pub(crate) fn from_row(row: &SqlRow) -> ThreadStoreResult<Self> {
        let internal = |err: codex_antfly::AntflyError| ThreadStoreError::Internal {
            message: format!("antfly: {err}"),
        };
        let thread_id =
            ThreadId::from_string(&row.string("id").map_err(internal)?).map_err(|err| {
                ThreadStoreError::Internal {
                    message: format!("invalid thread id: {err}"),
                }
            })?;
        let history_mode: ThreadHistoryMode = row
            .string("history_mode")
            .map_err(internal)?
            .parse()
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("invalid history_mode: {err}"),
            })?;
        let forked_from_id = row
            .opt_string("forked_from_id")
            .map_err(internal)?
            .and_then(|id| ThreadId::from_string(&id).ok());
        let parent_thread_id = row
            .opt_string("parent_thread_id")
            .map_err(internal)?
            .and_then(|id| ThreadId::from_string(&id).ok());
        let history_base = match (
            row.opt_string("history_base_thread_id").map_err(internal)?,
            row.opt_i64("history_base_end_ordinal").map_err(internal)?,
        ) {
            (Some(id), Some(end_ordinal_exclusive)) => {
                ThreadId::from_string(&id).ok().map(|id| HistoryPosition {
                    thread_id: id,
                    end_ordinal_exclusive: end_ordinal_exclusive as u64,
                    end_byte_offset: 0,
                })
            }
            _ => None,
        };
        let rollout_path = row.string("rollout_path").map_err(internal)?;
        let extra = row
            .opt_json("extra")
            .map_err(internal)?
            .unwrap_or(Value::Null);
        let has_extra_config = extra
            .get("has_extra_config")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let permission_profile: Option<PermissionProfile> = extra
            .get("permission_profile")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok());
        let token_usage: Option<TokenUsage> = extra
            .get("token_usage")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok());

        let created = CreateThreadParams {
            creator_user_id: row.opt_string("creator_user_id").map_err(internal)?,
            creator_account_id: row.opt_string("creator_account_id").map_err(internal)?,
            session_id: thread_id.into(),
            thread_id,
            extra_config: has_extra_config.then_some(ExtraConfig {}),
            forked_from_id,
            parent_thread_id,
            source: SessionSource::default(),
            thread_source: opt_enum_from_str(row.opt_string("thread_source").map_err(internal)?),
            originator: row
                .opt_string("originator")
                .map_err(internal)?
                .unwrap_or_default(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: opt_enum_from_str(
                row.opt_string("multi_agent_version").map_err(internal)?,
            ),
            history_mode,
            history_base,
            subagent_history_start_ordinal: row
                .opt_i64("subagent_history_start_ordinal")
                .map_err(internal)?
                .map(|ordinal| ordinal as u64),
            initial_window_id: String::new(),
            runtime_workspace_roots: None,
            metadata: ThreadPersistenceMetadata {
                cwd: None,
                model_provider: String::new(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        };

        let git_info = GitInfoPatchValue {
            sha: row.opt_string("git_sha").map_err(internal)?,
            branch: row.opt_string("git_branch").map_err(internal)?,
            origin_url: row
                .opt_string("git_origin_url")
                .map_err(internal)?
                .and_then(|url| codex_protocol::SanitizedGitUrl::try_from(url).ok()),
        };
        let patch = ThreadMetadataPatch {
            name: Some(row.opt_string("name").map_err(internal)?),
            rollout_path: None,
            preview: Some(row.string("preview").map_err(internal)?),
            title: Some(row.string("title").map_err(internal)?),
            model_provider: Some(row.string("model_provider").map_err(internal)?),
            model: row.opt_string("model").map_err(internal)?,
            reasoning_effort: Some(opt_enum_from_str(
                row.opt_string("reasoning_effort").map_err(internal)?,
            )),
            created_at: Some(epoch_ms_to_datetime(
                row.i64("created_at_ms").map_err(internal)?,
            )?),
            updated_at: Some(epoch_ms_to_datetime(
                row.i64("updated_at_ms").map_err(internal)?,
            )?),
            advance_recency_at: Some(epoch_ms_to_datetime(
                row.i64("recency_at_ms").map_err(internal)?,
            )?),
            source: opt_enum_from_str(Some(row.string("source").map_err(internal)?)),
            creator_user_id: row.opt_string("creator_user_id").map_err(internal)?,
            creator_account_id: row.opt_string("creator_account_id").map_err(internal)?,
            originator: row.opt_string("originator").map_err(internal)?,
            thread_source: Some(opt_enum_from_str(
                row.opt_string("thread_source").map_err(internal)?,
            )),
            agent_nickname: Some(row.opt_string("agent_nickname").map_err(internal)?),
            agent_role: Some(row.opt_string("agent_role").map_err(internal)?),
            agent_path: Some(row.opt_string("agent_path").map_err(internal)?),
            cwd: Some(PathBuf::from(row.string("cwd").map_err(internal)?)),
            cli_version: Some(row.string("cli_version").map_err(internal)?),
            approval_mode: enum_from_str(&row.string("approval_mode").map_err(internal)?),
            permission_profile,
            token_usage,
            first_user_message: Some(row.string("first_user_message").map_err(internal)?)
                .filter(|value| !value.is_empty()),
            git_info: Some(git_info.into_patch()),
            memory_mode: enum_from_str(&row.string("memory_mode").map_err(internal)?),
            project_id: Some(row.opt_string("project_id").map_err(internal)?),
            daybreak_enabled: row.opt_bool("daybreak_enabled").map_err(internal)?,
        };

        Ok(Self {
            created,
            patch,
            materialized_at: epoch_ms_to_datetime(row.i64("created_at_ms").map_err(internal)?)?,
            archived_at: row
                .opt_i64("archived_at")
                .map_err(internal)?
                .map(epoch_s_to_datetime)
                .transpose()?,
            next_ordinal: row.i64("next_ordinal").map_err(internal)?.max(0) as u64,
            section: row.opt_string("thread_section_id").map_err(internal)?,
            section_position: row.opt_i64("section_position").map_err(internal)?,
            section_entered_at: row
                .opt_i64("section_entered_at_ms")
                .map_err(internal)?
                .map(epoch_ms_to_datetime)
                .transpose()?,
            section_name: row.opt_string("section_name").map_err(internal)?,
            section_appearance: row
                .opt_string("section_appearance")
                .map_err(internal)?
                .and_then(|text| serde_json::from_str(&text).ok()),
            legacy_rollout_path: (!rollout_path.is_empty()).then(|| PathBuf::from(rollout_path)),
        })
    }
}

/// `GitInfoPatch`'s three clearable fields, filled from flat SQL columns.
struct GitInfoPatchValue {
    sha: Option<String>,
    branch: Option<String>,
    origin_url: Option<codex_protocol::SanitizedGitUrl>,
}

impl GitInfoPatchValue {
    fn into_patch(self) -> crate::GitInfoPatch {
        crate::GitInfoPatch {
            sha: Some(self.sha),
            branch: Some(self.branch),
            origin_url: Some(self.origin_url),
        }
    }
}

fn epoch_ms_to_datetime(value: i64) -> ThreadStoreResult<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("invalid unix timestamp millis: {value}"),
        })
}

fn epoch_s_to_datetime(value: i64) -> ThreadStoreResult<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value, 0).ok_or_else(|| ThreadStoreError::Internal {
        message: format!("invalid unix timestamp seconds: {value}"),
    })
}

fn git_info_from_record(record: &ThreadRecord) -> Option<GitInfo> {
    let sha = record.git_sha();
    let branch = record.git_branch();
    let origin_url = record.git_origin_url();
    if sha.is_none() && branch.is_none() && origin_url.is_none() {
        return None;
    }
    Some(GitInfo {
        commit_hash: sha.as_deref().map(codex_git_utils::GitSha::new),
        branch,
        repository_url: origin_url,
    })
}

/// Who wrote a piece of visible thread text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Speaker {
    User,
    Agent,
}

/// User- or agent-visible message text carried by a rollout item, if any.
/// Legacy threads carry `UserMessage`/`AgentMessage` events; paginated threads
/// carry completed turn items.
pub(crate) fn visible_text(item: &RolloutItem) -> Option<(Speaker, String)> {
    let RolloutItem::EventMsg(event) = item else {
        return None;
    };
    match event {
        EventMsg::UserMessage(user) => Some((Speaker::User, user.message.clone())),
        EventMsg::AgentMessage(agent) => Some((Speaker::Agent, agent.message.clone())),
        EventMsg::ItemCompleted(completed) => match &completed.item {
            TurnItem::UserMessage(user) => {
                let text = user
                    .content
                    .iter()
                    .filter_map(|input| match input {
                        codex_protocol::user_input::UserInput::Text { text, .. } => {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                (!text.is_empty()).then_some((Speaker::User, text))
            }
            TurnItem::AgentMessage(agent) => {
                let text = agent
                    .content
                    .iter()
                    .map(|content| match content {
                        AgentMessageContent::Text { text } => text.as_str(),
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                (!text.is_empty()).then_some((Speaker::Agent, text))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Turn id carried by a completed item, if any.
pub(crate) fn item_turn_id(item: &RolloutItem) -> Option<String> {
    match item {
        RolloutItem::EventMsg(EventMsg::ItemCompleted(completed)) => {
            Some(completed.turn_id.clone())
        }
        _ => None,
    }
}
