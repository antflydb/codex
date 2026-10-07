//! The durable thread record and its projection to [`StoredThread`].

use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::TurnItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::user_input::UserInput;
use codex_rollout::RolloutItem;
use serde::Deserialize;
use serde::Serialize;

use crate::CreateThreadParams;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadMetadataPatch;
use crate::ThreadSortKey;

/// Everything Antfly stores about one thread except its items.
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

    pub(crate) fn source(&self) -> codex_protocol::protocol::SessionSource {
        self.patch
            .source
            .clone()
            .unwrap_or_else(|| self.created.source.clone())
    }

    /// Text matched by `list_threads` search terms.
    pub(crate) fn searchable_summary(&self) -> String {
        [
            self.name(),
            self.patch.title.clone(),
            self.patch.preview.clone(),
            self.patch.first_user_message.clone(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n")
    }

    pub(crate) fn to_stored(&self, history: Option<Vec<RolloutItem>>) -> StoredThread {
        let thread_id = self.thread_id();
        let patch = &self.patch;
        let created = &self.created;
        StoredThread {
            originator: patch
                .originator
                .clone()
                .or_else(|| (!created.originator.is_empty()).then(|| created.originator.clone())),
            thread_id,
            extra_config: created.extra_config.clone(),
            rollout_path: None,
            forked_from_id: created.forked_from_id,
            parent_thread_id: created.parent_thread_id,
            preview: self.preview(),
            name: self.name(),
            model_provider: self.model_provider(),
            model: patch.model.clone(),
            reasoning_effort: patch.reasoning_effort.clone().flatten(),
            created_at: self.created_at(),
            updated_at: self.updated_at(),
            recency_at: self.recency_at(),
            archived_at: self.archived_at,
            section: self.section.clone().map(|id| codex_state::ThreadSection {
                name: if id == codex_state::PINNED_THREAD_SECTION_ID {
                    codex_state::PINNED_THREAD_SECTION_NAME.to_string()
                } else {
                    id.clone()
                },
                id,
                appearance: None,
            }),
            section_position: self.section_position,
            section_entered_at: self.section_entered_at,
            project_id: self.project_id(),
            daybreak_enabled: patch.daybreak_enabled,
            cwd: self.cwd(),
            cli_version: patch
                .cli_version
                .clone()
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
            source: self.source(),
            history_mode: created.history_mode,
            thread_source: patch
                .thread_source
                .clone()
                .unwrap_or_else(|| created.thread_source.clone()),
            agent_nickname: patch
                .agent_nickname
                .clone()
                .unwrap_or_else(|| created.source.get_nickname()),
            agent_role: patch
                .agent_role
                .clone()
                .unwrap_or_else(|| created.source.get_agent_role()),
            agent_path: patch
                .agent_path
                .clone()
                .unwrap_or_else(|| created.source.get_agent_path().map(Into::into)),
            git_info: git_info_from_patch(patch),
            approval_mode: patch.approval_mode.unwrap_or(AskForApproval::OnRequest),
            permission_profile: patch
                .permission_profile
                .clone()
                .unwrap_or_else(PermissionProfile::read_only),
            token_usage: patch.token_usage.clone(),
            first_user_message: patch.first_user_message.clone(),
            history: history.map(|items| StoredThreadHistory {
                revision: None,
                thread_id,
                items,
            }),
        }
    }
}

fn git_info_from_patch(patch: &ThreadMetadataPatch) -> Option<codex_protocol::protocol::GitInfo> {
    let git_info = patch.git_info.as_ref()?;
    let sha = git_info.sha.clone().flatten();
    let branch = git_info.branch.clone().flatten();
    let origin_url = git_info.origin_url.clone().flatten();
    if sha.is_none() && branch.is_none() && origin_url.is_none() {
        return None;
    }
    Some(codex_protocol::protocol::GitInfo {
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
                        UserInput::Text { text, .. } => Some(text.as_str()),
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
