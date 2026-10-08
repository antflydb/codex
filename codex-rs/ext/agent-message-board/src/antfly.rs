//! Antfly-backed boards. The board (tree) ID scopes every read and write.
//!
//! Unlike the SQLite backend, which relies on correlated subqueries for
//! summaries, this backend maintains `message_count`/`last_message_id` on
//! each channel document and `reply_count`/`latest_reply` on each root post
//! document, updated atomically alongside the post that changes them.
//! Listings and search still scan every post under the board prefix and
//! filter/sort in process, like [`crate::InMemoryAgentMessageBoard`]; this
//! keeps semantics easy to match exactly at the cost of being linear in the
//! board's post count per call.
//!
//! All mutations run under [`codex_antfly::Antfly::lock`], the process-wide
//! read-modify-write lock, so a single `write()` at the end of each mutation
//! is atomic and consistent with any concurrent mutation to the same board
//! (including [`AntflyAgentMessageBoard::delete_boards`]).

mod keys;
mod paging;
mod queries;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use crate::ChannelSummary;
use crate::CreateChannelRequest;
use crate::MessageBoardHost;
use crate::PostContent;
use crate::PostDestination;
use crate::PostMetadata;
use crate::PostPreview;
use crate::PostRequest;
use crate::SubscriptionChange;
use crate::SubscriptionRequest;
use crate::SubscriptionState;
use crate::SubscriptionTarget;
use caseless::default_case_fold_str;
use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::ScanRequest;
use codex_antfly::Write;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

const MAX_POST_BYTES: usize = 64 * 1024;
const MAX_CHANNEL_BYTES: usize = 128;
const MAX_READ_CHARS: usize = 20_000;

#[derive(Clone)]
pub struct AntflyAgentMessageBoard {
    identity: SessionId,
    antfly: Arc<Antfly>,
    host: Arc<dyn MessageBoardHost>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChannelDoc {
    name: String,
    search: String,
    created_at: DateTime<Utc>,
    created_by: AgentPath,
    message_count: usize,
    last_message_id: Option<Uuid>,
    last_message_timestamp_micros: Option<i64>,
}

impl ChannelDoc {
    fn new(name: &str, author: AgentPath, now: DateTime<Utc>) -> Self {
        Self {
            name: name.to_string(),
            search: default_case_fold_str(name),
            created_at: now,
            created_by: author,
            message_count: 0,
            last_message_id: None,
            last_message_timestamp_micros: None,
        }
    }

    fn record_post(&mut self, id: Uuid, now: DateTime<Utc>) {
        self.message_count += 1;
        let micros = now.timestamp_micros();
        let newest = self
            .last_message_timestamp_micros
            .is_none_or(|last| micros >= last);
        if newest {
            self.last_message_id = Some(id);
            self.last_message_timestamp_micros = Some(micros);
        }
    }

    fn summary(&self) -> ChannelSummary {
        ChannelSummary {
            channel_name: self.name.clone(),
            created_at: self.created_at,
            created_by: self.created_by.clone(),
            message_count: self.message_count,
            last_message_id: self.last_message_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PostDoc {
    metadata: PostMetadata,
    text: String,
    search: String,
    /// Strict insertion order, used to break timestamp ties the same way a
    /// SQLite `rowid`/`AUTOINCREMENT` sequence would.
    seq: u64,
    /// Meaningful only when `metadata.message_id == metadata.thread_id`.
    reply_count: usize,
    latest_reply_id: Option<Uuid>,
    latest_reply_timestamp_micros: Option<i64>,
}

impl PostDoc {
    fn is_root(&self) -> bool {
        self.metadata.message_id == self.metadata.thread_id
    }

    fn record_reply(&mut self, id: Uuid, now: DateTime<Utc>) {
        self.reply_count += 1;
        let micros = now.timestamp_micros();
        let newest = self
            .latest_reply_timestamp_micros
            .is_none_or(|last| micros >= last);
        if newest {
            self.latest_reply_id = Some(id);
            self.latest_reply_timestamp_micros = Some(micros);
        }
    }

    fn preview(&self, max_chars: usize) -> PostPreview {
        let n_chars = self.text.chars().count();
        PostPreview {
            metadata: self.metadata.clone(),
            text_preview: self.text.chars().take(max_chars).collect(),
            n_chars,
            truncated: n_chars > max_chars,
        }
    }

    /// `(timestamp, seq)`, matching the SQL backend's `ORDER BY timestamp,seq`.
    fn sort_key(&self) -> (i64, u64) {
        (self.metadata.created_at.timestamp_micros(), self.seq)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequestDoc {
    post_id: Uuid,
    request: PostRequest,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct SubscriptionDoc {
    change: SubscriptionChange,
}

/// Antfly documents must be JSON objects, so the monotonic post sequence is
/// wrapped rather than stored as a bare number.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct SequenceDoc {
    value: u64,
}

impl AntflyAgentMessageBoard {
    pub fn open(antfly: Arc<Antfly>, identity: SessionId, host: Arc<dyn MessageBoardHost>) -> Self {
        Self {
            identity,
            antfly,
            host,
        }
    }

    /// Permanently removes boards owned by these roots, including their
    /// posts, channels and subscriptions. Safe to retry; independent of
    /// whether the feature is currently enabled. Mirrors
    /// [`crate::LocalAgentMessageBoard::delete_boards`].
    pub async fn delete_boards(antfly: &Arc<Antfly>, roots: &[SessionId]) -> Result<()> {
        if roots.is_empty() {
            return Ok(());
        }
        let _guard = antfly.lock().await;
        let mut writes = Vec::new();
        for root in roots {
            writes.push(Write::put(keys::tombstone(*root), serde_json::json!({})));
            writes.push(Write::delete(keys::sequence(*root)));
            for prefix in [
                keys::channel_prefix(*root),
                keys::post_prefix(*root),
                keys::request_prefix(*root),
                keys::subscription_prefix(*root),
            ] {
                let documents = antfly
                    .scan(ScanRequest::prefix(&prefix))
                    .await
                    .map_err(storage_error)?;
                writes.extend(
                    documents
                        .into_iter()
                        .map(|document| Write::delete(document.key)),
                );
            }
        }
        antfly.write(writes).await.map_err(storage_error)?;
        Ok(())
    }

    async fn require_not_deleted(&self) -> Result<()> {
        if self
            .antfly
            .get(keys::tombstone(self.identity))
            .await
            .map_err(storage_error)?
            .is_some()
        {
            return Err(invalid(
                "the message board's root has been permanently deleted",
            ));
        }
        Ok(())
    }

    async fn load_channel(&self, name: &str) -> Result<Option<ChannelDoc>> {
        self.antfly
            .get_as::<ChannelDoc>(keys::channel(self.identity, name))
            .await
            .map_err(storage_error)
    }

    async fn require_channel(&self, name: &str) -> Result<ChannelDoc> {
        self.load_channel(name)
            .await?
            .ok_or_else(|| invalid("channel not found in this board"))
    }

    async fn load_post(&self, id: Uuid) -> Result<PostDoc> {
        self.antfly
            .get_as::<PostDoc>(keys::post(self.identity, id))
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid("post not found in this board"))
    }

    async fn next_seq(&self) -> Result<u64> {
        let current = self
            .antfly
            .get_as::<SequenceDoc>(keys::sequence(self.identity))
            .await
            .map_err(storage_error)?
            .map_or(0, |doc| doc.value);
        Ok(current + 1)
    }

    async fn subscription_state(
        &self,
        target: &SubscriptionTarget,
        agent: ThreadId,
    ) -> Result<Option<SubscriptionChange>> {
        let key = keys::subscription(self.identity, target, agent)?;
        Ok(self
            .antfly
            .get_as::<SubscriptionDoc>(key)
            .await
            .map_err(storage_error)?
            .map(|doc| doc.change))
    }

    /// Subscribes unless an explicit unsubscribe is on record, matching the
    /// SQL backend's "insert or ignore ... where not exists an opt-out".
    async fn subscribe_write(
        &self,
        target: &SubscriptionTarget,
        agent: ThreadId,
        writes: &mut Vec<Write>,
    ) -> Result<()> {
        if self.subscription_state(target, agent).await? == Some(SubscriptionChange::Unsubscribe) {
            return Ok(());
        }
        let key = keys::subscription(self.identity, target, agent)?;
        let doc = serde_json::to_value(SubscriptionDoc {
            change: SubscriptionChange::Subscribe,
        })
        .map_err(storage_error)?;
        writes.push(Write::put(key, doc));
        Ok(())
    }

    async fn subscribers(&self, target: &SubscriptionTarget) -> Result<HashSet<ThreadId>> {
        let prefix = keys::subscription_target_prefix(self.identity, target)?;
        let documents = self
            .antfly
            .scan(ScanRequest::prefix(&prefix))
            .await
            .map_err(storage_error)?;
        let mut out = HashSet::new();
        for document in documents {
            let Some(agent) = keys::parse_subscription_agent(&document.key) else {
                continue;
            };
            let doc: SubscriptionDoc =
                serde_json::from_value(codex_antfly::strip_reserved(document.doc))
                    .map_err(storage_error)?;
            if doc.change == SubscriptionChange::Subscribe {
                out.insert(agent);
            }
        }
        Ok(out)
    }

    async fn scan_posts(&self) -> Result<Vec<PostDoc>> {
        let documents = self
            .antfly
            .scan_as::<PostDoc>(ScanRequest::prefix(&keys::post_prefix(self.identity)))
            .await
            .map_err(storage_error)?;
        Ok(documents.into_iter().map(|(_, doc)| doc).collect())
    }

    async fn scan_channels(&self) -> Result<Vec<ChannelDoc>> {
        let documents = self
            .antfly
            .scan_as::<ChannelDoc>(ScanRequest::prefix(&keys::channel_prefix(self.identity)))
            .await
            .map_err(storage_error)?;
        Ok(documents.into_iter().map(|(_, doc)| doc).collect())
    }

    async fn existing_request(
        &self,
        caller: ThreadId,
        request: &PostRequest,
    ) -> Result<Option<PostMetadata>> {
        let key = keys::request(self.identity, caller, &request.request_id);
        let Some(stored) = self
            .antfly
            .get_as::<RequestDoc>(key)
            .await
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        if stored.request != *request {
            return Err(invalid("request ID was already used for a different post"));
        }
        Ok(Some(self.load_post(stored.post_id).await?.metadata))
    }

    pub async fn create_channel(
        &self,
        caller: ThreadId,
        request: CreateChannelRequest,
    ) -> Result<ChannelSummary> {
        validate_channel(&request.channel_name)?;
        let author = self.host.agent_path(caller).await?;
        let now = self.host.current_time(caller).await?;
        let _guard = self.antfly.lock().await;
        self.require_not_deleted().await?;
        if self.load_channel(&request.channel_name).await?.is_some() {
            return Err(invalid("channel already exists"));
        }
        let doc = ChannelDoc::new(&request.channel_name, author, now);
        let mut writes = vec![Write::put(
            keys::channel(self.identity, &request.channel_name),
            serde_json::to_value(&doc).map_err(storage_error)?,
        )];
        if request.subscription == SubscriptionChange::Subscribe {
            self.subscribe_write(
                &SubscriptionTarget::Channel(request.channel_name.clone()),
                caller,
                &mut writes,
            )
            .await?;
        }
        self.antfly.write(writes).await.map_err(storage_error)?;
        Ok(doc.summary())
    }

    /// Once started, finishes the accepted write and fanout even if the tool
    /// caller disconnects, matching the SQLite and in-memory backends.
    pub async fn post(&self, caller: ThreadId, request: PostRequest) -> Result<PostMetadata> {
        let board = self.clone();
        tokio::spawn(async move { board.post_inner(caller, request).await })
            .await
            .map_err(storage_error)?
    }

    async fn post_inner(&self, caller: ThreadId, request: PostRequest) -> Result<PostMetadata> {
        if request.text.len() > MAX_POST_BYTES
            || request.text.is_empty()
            || request.request_id.is_empty()
            || request.request_id.len() > 512
            || request.agents_to_notify.len() > 256
        {
            return Err(invalid(
                "post text, request ID or recipient count exceeds the board limits",
            ));
        }
        let author = self.host.agent_path(caller).await?;
        if let Some(post) = self.existing_request(caller, &request).await? {
            return Ok(post);
        }
        let mut recipients = HashSet::new();
        for path in &request.agents_to_notify {
            recipients.insert(self.host.resolve_agent(path.clone()).await?);
        }
        let now = self.host.current_time(caller).await?;

        let _guard = self.antfly.lock().await;
        self.require_not_deleted().await?;
        if let Some(post) = self.existing_request(caller, &request).await? {
            return Ok(post);
        }

        let id = Uuid::now_v7();
        let mut writes = Vec::new();
        let (channel_name, root, target, mut channel_doc) = match &request.destination {
            PostDestination::Channel(channel) => {
                let doc = self.require_channel(channel).await?;
                (
                    channel.clone(),
                    id,
                    SubscriptionTarget::Channel(channel.clone()),
                    doc,
                )
            }
            PostDestination::NewChannel(channel) => {
                validate_channel(channel)?;
                if self.load_channel(channel).await?.is_some() {
                    return Err(invalid("channel already exists"));
                }
                let doc = ChannelDoc::new(channel, author.clone(), now);
                self.subscribe_write(
                    &SubscriptionTarget::Channel(channel.clone()),
                    caller,
                    &mut writes,
                )
                .await?;
                (
                    channel.clone(),
                    id,
                    SubscriptionTarget::Channel(channel.clone()),
                    doc,
                )
            }
            PostDestination::Thread(root) => {
                let root_post = self.load_post(*root).await?;
                if root_post.metadata.thread_id != *root {
                    return Err(invalid("thread_id must identify a top-level post"));
                }
                let channel_name = root_post.metadata.channel_name.clone();
                let channel_doc = self.require_channel(&channel_name).await?;
                let mut root_post = root_post;
                root_post.record_reply(id, now);
                writes.push(Write::put(
                    keys::post(self.identity, *root),
                    serde_json::to_value(&root_post).map_err(storage_error)?,
                ));
                (
                    channel_name,
                    *root,
                    SubscriptionTarget::Thread(*root),
                    channel_doc,
                )
            }
        };
        channel_doc.record_post(id, now);
        writes.push(Write::put(
            keys::channel(self.identity, &channel_name),
            serde_json::to_value(&channel_doc).map_err(storage_error)?,
        ));

        let mut recipient_ids = self.subscribers(&target).await?;
        recipient_ids.extend(recipients.drain());
        recipient_ids.remove(&caller);

        let post_doc = PostDoc {
            metadata: PostMetadata {
                message_id: id,
                channel_name: channel_name.clone(),
                author: author.clone(),
                thread_id: root,
                created_at: now,
            },
            text: request.text.clone(),
            search: default_case_fold_str(&request.text),
            seq: self.next_seq().await?,
            reply_count: 0,
            latest_reply_id: None,
            latest_reply_timestamp_micros: None,
        };
        writes.push(Write::put(
            keys::post(self.identity, id),
            serde_json::to_value(&post_doc).map_err(storage_error)?,
        ));
        writes.push(Write::put(
            keys::sequence(self.identity),
            serde_json::to_value(SequenceDoc {
                value: post_doc.seq,
            })
            .map_err(storage_error)?,
        ));
        writes.push(Write::put(
            keys::request(self.identity, caller, &request.request_id),
            serde_json::to_value(RequestDoc {
                post_id: id,
                request: request.clone(),
            })
            .map_err(storage_error)?,
        ));
        // Participation subscribes by default, without overriding an explicit opt-out.
        self.subscribe_write(&SubscriptionTarget::Thread(root), caller, &mut writes)
            .await?;

        self.antfly.write(writes).await.map_err(storage_error)?;

        // A committed post succeeds even if a best-effort notice cannot be delivered.
        let notice = post_doc.preview(/*max_chars*/ 150);
        futures::stream::iter(recipient_ids)
            .for_each_concurrent(/*limit*/ 16, |recipient| {
                let notice = notice.clone();
                async move {
                    if let Err(error) = self.host.notify(recipient, notice).await {
                        tracing::warn!(%recipient, %error, "Failed to deliver message-board notification");
                    }
                }
            })
            .await;
        Ok(post_doc.metadata)
    }

    pub async fn set_subscription(
        &self,
        caller: ThreadId,
        request: SubscriptionRequest,
    ) -> Result<SubscriptionState> {
        let caller_path = self.host.agent_path(caller).await?;
        let target_path = request.target_agent.unwrap_or(caller_path);
        let target_agent = self.host.resolve_agent(target_path.clone()).await?;

        let _guard = self.antfly.lock().await;
        self.require_not_deleted().await?;
        let (channel_name, root, last) = match &request.target {
            SubscriptionTarget::Channel(name) => {
                let doc = self.require_channel(name).await?;
                (name.clone(), None, doc.last_message_id)
            }
            SubscriptionTarget::Thread(root) => {
                let post = self.load_post(*root).await?;
                if post.metadata.thread_id != *root {
                    return Err(invalid("thread_id must identify a top-level post"));
                }
                let last = post.latest_reply_id.unwrap_or(*root);
                (post.metadata.channel_name, Some(*root), Some(last))
            }
        };
        let key = keys::subscription(self.identity, &request.target, target_agent)?;
        let doc = serde_json::to_value(SubscriptionDoc {
            change: request.change,
        })
        .map_err(storage_error)?;
        self.antfly
            .write(vec![Write::put(key, doc)])
            .await
            .map_err(storage_error)?;
        Ok(SubscriptionState {
            channel_name,
            thread_id: root,
            target_agent: target_path,
            enabled: request.change == SubscriptionChange::Subscribe,
            last_message_id: last,
        })
    }

    pub async fn read_post(
        &self,
        caller: ThreadId,
        request: crate::ReadPostRequest,
    ) -> Result<PostContent> {
        self.host.agent_path(caller).await?;
        let post = self.load_post(request.message_id).await?;
        let n_chars = post.text.chars().count();
        let offset = (request.offset_chars as usize).min(n_chars);
        let text: String = post
            .text
            .chars()
            .skip(offset)
            .take((request.limit_chars.get() as usize).min(MAX_READ_CHARS))
            .collect();
        let next_offset_chars = offset + text.chars().count();
        Ok(PostContent {
            metadata: post.metadata,
            text,
            n_chars,
            next_offset_chars,
        })
    }
}

fn validate_channel(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > MAX_CHANNEL_BYTES
        || name.trim() != name
        || name.chars().any(char::is_control)
    {
        return Err(invalid(
            "channel names must contain 1–128 bytes without edge whitespace or control characters",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> CodexErr {
    CodexErr::InvalidRequest(message.into())
}

fn storage_error(error: impl std::fmt::Display) -> CodexErr {
    CodexErr::Io(std::io::Error::other(error.to_string()))
}
