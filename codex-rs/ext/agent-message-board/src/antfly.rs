//! Antfly-backed boards. The board (tree) ID scopes every row.
//!
//! Boards are relational: channels, posts, subscriptions and opt-outs each
//! live in their own `codex_message_board_*` table (see
//! `codex_antfly::schema::MIGRATIONS` version 20), queried with the same
//! shape of SQL as [`crate::LocalAgentMessageBoard`]'s SQLite schema so the
//! two backends agree on behavior. `seq` breaks timestamp ties the same way
//! SQLite's `AUTOINCREMENT` rowid did; Antfly has no sequences, so it is
//! assigned as `COALESCE(MAX(seq) WHERE board=?, 0) + 1` in the same
//! transaction that inserts the post, and that transaction is retried on a
//! 40001 conflict (two posts to the same board racing for the next `seq`).
//!
//! Every mutation also runs under [`codex_antfly::Antfly::lock`], the
//! process-wide read-modify-write lock, so within one process a conflict can
//! only come from another process writing the same remote Antfly database;
//! the retry loop exists for that case.

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
use codex_antfly::AntflyError;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
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
/// Bounded attempts at the seq-assignment transaction before giving up.
const MAX_WRITE_ATTEMPTS: u32 = 5;

#[derive(Clone)]
pub struct AntflyAgentMessageBoard {
    identity: SessionId,
    antfly: Arc<Antfly>,
    host: Arc<dyn MessageBoardHost>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StoredPost {
    metadata: PostMetadata,
    text: String,
}

/// A storage error that should retry the whole transaction (40001: another
/// writer committed first) versus one that should surface to the caller.
enum Attempt<T> {
    Ok(T),
    Conflict,
    Failed(CodexErr),
}

fn sql_attempt<T>(result: codex_antfly::AntflyResult<T>) -> Attempt<T> {
    match result {
        Ok(value) => Attempt::Ok(value),
        // 40001: a concurrent transaction committed first. 23505: the
        // manually assigned `seq` (or, more rarely, `request_id`) collided
        // with a row a concurrent transaction just committed; retrying picks
        // a fresh `seq`, or for `request_id` finds the now-committed post and
        // returns it, same as a clean idempotent retry.
        Err(err) if err.is_conflict() || err.is_unique_violation() => Attempt::Conflict,
        Err(err) => Attempt::Failed(storage_error(err)),
    }
}

/// Runs `body` (a transaction) under the board's write lock, retrying when it
/// signals a 40001 conflict.
macro_rules! try_attempt {
    ($expr:expr) => {
        match sql_attempt($expr) {
            Attempt::Ok(value) => value,
            Attempt::Conflict => return Attempt::Conflict,
            Attempt::Failed(err) => return Attempt::Failed(err),
        }
    };
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
        let sql = antfly.sql().await.map_err(storage_error)?;
        let mut tx = sql.begin().await.map_err(storage_error)?;
        for root in roots {
            let board = root.to_string();
            tx.execute(
                "INSERT INTO codex_message_board_tombstones (board) VALUES ($1) \
                 ON CONFLICT (board) DO NOTHING",
                sql_params![board.clone()],
            )
            .await
            .map_err(storage_error)?;
            for statement in [
                "DELETE FROM codex_message_board_subscriptions WHERE board=$1",
                "DELETE FROM codex_message_board_subscription_opt_outs WHERE board=$1",
                "DELETE FROM codex_message_board_posts WHERE board=$1",
                "DELETE FROM codex_message_board_channels WHERE board=$1",
            ] {
                tx.execute(statement, sql_params![board.clone()])
                    .await
                    .map_err(storage_error)?;
            }
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }

    async fn require_not_deleted(&self, tx: &mut SqlTx) -> codex_antfly::AntflyResult<()> {
        let deleted = tx
            .fetch_optional(
                "SELECT 1 AS present FROM codex_message_board_tombstones WHERE board=$1",
                sql_params![self.identity.to_string()],
            )
            .await?
            .is_some();
        if deleted {
            return Err(AntflyError::Malformed(
                "the message board's root has been permanently deleted".to_string(),
            ));
        }
        Ok(())
    }

    /// Begins a transaction and rejects it if this board was deleted.
    /// `require_not_deleted`'s error is reinterpreted as an ordinary invalid
    /// request, never a retryable conflict.
    async fn begin_write(&self) -> Result<SqlTx> {
        let sql = self.antfly.sql().await.map_err(storage_error)?;
        let mut tx = sql.begin().await.map_err(storage_error)?;
        if let Err(err) = self.require_not_deleted(&mut tx).await {
            let _ = tx.rollback().await;
            return Err(invalid(err.to_string()));
        }
        Ok(tx)
    }

    async fn channel_exists(&self, tx: &mut SqlTx, name: &str) -> codex_antfly::AntflyResult<bool> {
        Ok(tx
            .fetch_optional(
                "SELECT 1 AS present FROM codex_message_board_channels WHERE board=$1 AND name=$2",
                sql_params![self.identity.to_string(), name],
            )
            .await?
            .is_some())
    }

    /// Antfly's SQL engine rejects a non-aggregate scalar correlated
    /// subquery in the `SELECT` list (`42703`, "a referenced column does
    /// not exist", confirmed against `codex_message_board_posts`); an
    /// `EXISTS`, `COUNT`, or `MAX`-wrapped correlated subquery works fine.
    /// `message_count` therefore stays a correlated `COUNT(*)`, and
    /// `last_message_id` is a separate, uncorrelated query instead of a
    /// third select-list subquery.
    async fn channel_summary(
        &self,
        tx: &mut SqlTx,
        name: &str,
    ) -> codex_antfly::AntflyResult<Option<ChannelSummary>> {
        let Some(row) = tx
            .fetch_optional(
                "SELECT c.created_at AS created_at, c.author AS author,
                 (SELECT COUNT(*) FROM codex_message_board_posts p
                  WHERE p.board=c.board AND p.channel=c.name) AS message_count
                 FROM codex_message_board_channels c WHERE c.board=$1 AND c.name=$2",
                sql_params![self.identity.to_string(), name],
            )
            .await?
        else {
            return Ok(None);
        };
        let created_at = DateTime::parse_from_rfc3339(&row.string("created_at")?)
            .map_err(|err| codex_antfly::AntflyError::Malformed(err.to_string()))?
            .with_timezone(&Utc);
        let created_by = AgentPath::try_from(row.string("author")?)
            .map_err(codex_antfly::AntflyError::Malformed)?;
        let last_message_id = tx
            .fetch_optional(
                "SELECT id FROM codex_message_board_posts WHERE board=$1 AND channel=$2 \
                 ORDER BY timestamp_us DESC, seq DESC LIMIT 1",
                sql_params![self.identity.to_string(), name],
            )
            .await?
            .map(|row| row.string("id"))
            .transpose()?
            .map(|id| Uuid::parse_str(&id))
            .transpose()
            .map_err(|err| codex_antfly::AntflyError::Malformed(err.to_string()))?;
        Ok(Some(ChannelSummary {
            channel_name: name.to_string(),
            created_at,
            created_by,
            message_count: row.i64("message_count")? as usize,
            last_message_id,
        }))
    }

    async fn require_channel_summary(&self, tx: &mut SqlTx, name: &str) -> Result<ChannelSummary> {
        self.channel_summary(tx, name)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid("channel not found in this board"))
    }

    async fn load_post(
        &self,
        tx: &mut SqlTx,
        id: Uuid,
    ) -> codex_antfly::AntflyResult<Option<StoredPost>> {
        let Some(row) = tx
            .fetch_optional(
                "SELECT payload FROM codex_message_board_posts WHERE board=$1 AND id=$2",
                sql_params![self.identity.to_string(), id.to_string()],
            )
            .await?
        else {
            return Ok(None);
        };
        let payload = row.json("payload")?;
        serde_json::from_value(payload)
            .map(Some)
            .map_err(|err| codex_antfly::AntflyError::Malformed(err.to_string()))
    }

    async fn require_post(&self, tx: &mut SqlTx, id: Uuid) -> Result<StoredPost> {
        self.load_post(tx, id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid("post not found in this board"))
    }

    async fn existing_post(
        &self,
        tx: &mut SqlTx,
        request_id: &str,
        request: &PostRequest,
    ) -> Result<Option<StoredPost>> {
        let Some(row) = tx
            .fetch_optional(
                "SELECT payload, request FROM codex_message_board_posts \
                 WHERE board=$1 AND request_id=$2",
                sql_params![self.identity.to_string(), request_id],
            )
            .await
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let stored: PostRequest =
            serde_json::from_value(row.json("request").map_err(storage_error)?)
                .map_err(storage_error)?;
        if stored != *request {
            return Err(invalid("request ID was already used for a different post"));
        }
        let payload: StoredPost =
            serde_json::from_value(row.json("payload").map_err(storage_error)?)
                .map_err(storage_error)?;
        Ok(Some(payload))
    }

    /// Inserts unless an explicit opt-out is on record, matching the SQL
    /// backend's "insert or ignore ... where not exists an opt-out".
    ///
    /// Antfly's SQL engine does not support `INSERT ... SELECT ... WHERE NOT
    /// EXISTS (...)` (SQLSTATE 0A000, "this SQL statement or expression is
    /// not supported"), which SQLite accepts as one atomic statement, so
    /// this checks and inserts as two statements in the same transaction
    /// instead. Both run under [`Antfly::lock`], so within one process this
    /// is as atomic as the single-statement form; only a second process
    /// writing the same remote database between the two statements could
    /// race it.
    async fn subscribe_write(
        &self,
        tx: &mut SqlTx,
        target: &SubscriptionTarget,
        agent: ThreadId,
    ) -> codex_antfly::AntflyResult<()> {
        let target_value = target_key(target)?;
        let opted_out = tx
            .fetch_optional(
                "SELECT 1 AS present FROM codex_message_board_subscription_opt_outs \
                 WHERE board=$1 AND target=$2 AND agent=$3",
                sql_params![
                    self.identity.to_string(),
                    target_value.clone(),
                    agent.to_string()
                ],
            )
            .await?
            .is_some();
        if opted_out {
            return Ok(());
        }
        tx.execute(
            "INSERT INTO codex_message_board_subscriptions (board, target, agent) \
             VALUES ($1,$2,$3) ON CONFLICT (board, target, agent) DO NOTHING",
            sql_params![self.identity.to_string(), target_value, agent.to_string()],
        )
        .await?;
        Ok(())
    }

    async fn subscribers(
        &self,
        tx: &mut SqlTx,
        target: &SubscriptionTarget,
    ) -> codex_antfly::AntflyResult<HashSet<ThreadId>> {
        let target_value = target_key(target)?;
        let rows = tx
            .fetch_all(
                "SELECT agent FROM codex_message_board_subscriptions WHERE board=$1 AND target=$2",
                sql_params![self.identity.to_string(), target_value],
            )
            .await?;
        rows.iter()
            .map(|row| {
                row.string("agent").and_then(|agent| {
                    ThreadId::from_string(&agent)
                        .map_err(|err| codex_antfly::AntflyError::Malformed(err.to_string()))
                })
            })
            .collect()
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
        let mut tx = self.begin_write().await?;
        let inserted = tx
            .execute(
                "INSERT INTO codex_message_board_channels \
                 (board, name, name_search, created_at, timestamp_us, author) \
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (board, name) DO NOTHING",
                sql_params![
                    self.identity.to_string(),
                    request.channel_name.clone(),
                    default_case_fold_str(&request.channel_name),
                    now.to_rfc3339(),
                    now.timestamp_micros(),
                    author.to_string()
                ],
            )
            .await
            .map_err(storage_error)?;
        if inserted == 0 {
            let _ = tx.rollback().await;
            return Err(invalid("channel already exists"));
        }
        if request.subscription == SubscriptionChange::Subscribe {
            self.subscribe_write(
                &mut tx,
                &SubscriptionTarget::Channel(request.channel_name.clone()),
                caller,
            )
            .await
            .map_err(storage_error)?;
        }
        let summary = self
            .require_channel_summary(&mut tx, &request.channel_name)
            .await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(summary)
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
        let request_id = format!("{caller}:{}", request.request_id);

        // Fast idempotency path, outside the write lock.
        {
            let sql = self.antfly.sql().await.map_err(storage_error)?;
            let mut tx = sql.begin().await.map_err(storage_error)?;
            if let Some(post) = self.existing_post(&mut tx, &request_id, &request).await? {
                let _ = tx.rollback().await;
                return Ok(post.metadata);
            }
            let _ = tx.rollback().await;
        }

        let mut recipients = HashSet::new();
        for path in &request.agents_to_notify {
            recipients.insert(self.host.resolve_agent(path.clone()).await?);
        }
        let now = self.host.current_time(caller).await?;

        for attempt in 0..MAX_WRITE_ATTEMPTS {
            let _guard = self.antfly.lock().await;
            match self
                .post_attempt(caller, &author, &request, &request_id, now, &recipients)
                .await
            {
                Attempt::Ok((metadata, recipient_ids, notice)) => {
                    drop(_guard);
                    // A committed post succeeds even if a best-effort notice
                    // cannot be delivered.
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
                    return Ok(metadata);
                }
                Attempt::Conflict if attempt + 1 < MAX_WRITE_ATTEMPTS => continue,
                Attempt::Conflict => {
                    return Err(invalid(
                        "message board write conflicted with a concurrent post; retry",
                    ));
                }
                Attempt::Failed(err) => return Err(err),
            }
        }
        unreachable!("the loop above always returns")
    }

    async fn post_attempt(
        &self,
        caller: ThreadId,
        author: &AgentPath,
        request: &PostRequest,
        request_id: &str,
        now: DateTime<Utc>,
        recipients: &HashSet<ThreadId>,
    ) -> Attempt<(PostMetadata, HashSet<ThreadId>, PostPreview)> {
        let sql = try_attempt!(self.antfly.sql().await);
        let mut tx = try_attempt!(sql.begin().await);
        if let Err(err) = self.require_not_deleted(&mut tx).await {
            let _ = tx.rollback().await;
            return Attempt::Failed(invalid(err.to_string()));
        }
        match self.existing_post(&mut tx, request_id, request).await {
            Ok(Some(post)) => {
                let _ = tx.rollback().await;
                let notice = preview(post.clone(), /*max_chars*/ 150);
                return Attempt::Ok((post.metadata, HashSet::new(), notice));
            }
            Ok(None) => {}
            Err(err) => {
                let _ = tx.rollback().await;
                return Attempt::Failed(err);
            }
        }

        let id = Uuid::now_v7();
        let (channel_name, root) = match &request.destination {
            PostDestination::Channel(channel) => {
                match self.channel_exists(&mut tx, channel).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = tx.rollback().await;
                        return Attempt::Failed(invalid("channel not found in this board"));
                    }
                    Err(err) => return sql_attempt(Err(err)),
                }
                (channel.clone(), id)
            }
            PostDestination::NewChannel(channel) => {
                if let Err(err) = validate_channel(channel) {
                    let _ = tx.rollback().await;
                    return Attempt::Failed(err);
                }
                let inserted = try_attempt!(
                    tx.execute(
                        "INSERT INTO codex_message_board_channels \
                         (board, name, name_search, created_at, timestamp_us, author) \
                         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (board, name) DO NOTHING",
                        sql_params![
                            self.identity.to_string(),
                            channel.clone(),
                            default_case_fold_str(channel),
                            now.to_rfc3339(),
                            now.timestamp_micros(),
                            author.to_string()
                        ],
                    )
                    .await
                );
                if inserted == 0 {
                    let _ = tx.rollback().await;
                    return Attempt::Failed(invalid("channel already exists"));
                }
                try_attempt!(
                    self.subscribe_write(
                        &mut tx,
                        &SubscriptionTarget::Channel(channel.clone()),
                        caller,
                    )
                    .await
                );
                (channel.clone(), id)
            }
            PostDestination::Thread(root) => {
                let root_post = match self.load_post(&mut tx, *root).await {
                    Ok(Some(post)) => post,
                    Ok(None) => {
                        let _ = tx.rollback().await;
                        return Attempt::Failed(invalid("post not found in this board"));
                    }
                    Err(err) => return sql_attempt(Err(err)),
                };
                if root_post.metadata.thread_id != *root {
                    let _ = tx.rollback().await;
                    return Attempt::Failed(invalid("thread_id must identify a top-level post"));
                }
                (root_post.metadata.channel_name, *root)
            }
        };
        let target = match &request.destination {
            PostDestination::Thread(_) => SubscriptionTarget::Thread(root),
            _ => SubscriptionTarget::Channel(channel_name.clone()),
        };

        let mut recipient_ids = try_attempt!(self.subscribers(&mut tx, &target).await);
        recipient_ids.extend(recipients.iter().copied());
        recipient_ids.remove(&caller);

        let seq_row = try_attempt!(
            tx.fetch_optional(
                "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq \
                 FROM codex_message_board_posts WHERE board=$1",
                sql_params![self.identity.to_string()],
            )
            .await
        );
        let seq = match seq_row.map(|row| row.i64("next_seq")).transpose() {
            Ok(Some(seq)) => seq,
            Ok(None) => 1,
            Err(err) => return sql_attempt(Err(err)),
        };

        let post = StoredPost {
            metadata: PostMetadata {
                message_id: id,
                channel_name: channel_name.clone(),
                author: author.clone(),
                thread_id: root,
                created_at: now,
            },
            text: request.text.clone(),
        };
        let payload = match serde_json::to_value(&post) {
            Ok(value) => value,
            Err(err) => {
                let _ = tx.rollback().await;
                return Attempt::Failed(storage_error(err));
            }
        };
        let request_value = match serde_json::to_value(request) {
            Ok(value) => value,
            Err(err) => {
                let _ = tx.rollback().await;
                return Attempt::Failed(storage_error(err));
            }
        };
        try_attempt!(
            tx.execute(
                "INSERT INTO codex_message_board_posts \
                 (board, id, channel, root, is_root, author, timestamp_us, seq, \
                  body_search, payload, request_id, request) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                sql_params![
                    self.identity.to_string(),
                    id.to_string(),
                    channel_name,
                    root.to_string(),
                    id == root,
                    author.to_string(),
                    now.timestamp_micros(),
                    seq,
                    default_case_fold_str(&request.text),
                    payload,
                    request_id.to_string(),
                    request_value
                ],
            )
            .await
        );
        // Participation subscribes by default, without overriding an explicit opt-out.
        try_attempt!(
            self.subscribe_write(&mut tx, &SubscriptionTarget::Thread(root), caller)
                .await
        );

        match tx.commit().await {
            Ok(()) => {}
            Err(err) => return sql_attempt(Err(err)),
        }

        let notice = preview(post.clone(), /*max_chars*/ 150);
        Attempt::Ok((post.metadata, recipient_ids, notice))
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
        let mut tx = self.begin_write().await?;
        let (channel_name, root, last) = match &request.target {
            SubscriptionTarget::Channel(name) => {
                let summary = self.require_channel_summary(&mut tx, name).await?;
                (name.clone(), None, summary.last_message_id)
            }
            SubscriptionTarget::Thread(root) => {
                let post = self.require_post(&mut tx, *root).await?;
                if post.metadata.thread_id != *root {
                    return Err(invalid("thread_id must identify a top-level post"));
                }
                let last = tx
                    .fetch_optional(
                        "SELECT id FROM codex_message_board_posts WHERE board=$1 AND root=$2 \
                         ORDER BY timestamp_us DESC, seq DESC LIMIT 1",
                        sql_params![self.identity.to_string(), root.to_string()],
                    )
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| invalid("post not found in this board"))?
                    .string("id")
                    .map_err(storage_error)?;
                (
                    post.metadata.channel_name,
                    Some(*root),
                    Some(Uuid::parse_str(&last).map_err(storage_error)?),
                )
            }
        };
        let target_value = target_key(&request.target).map_err(storage_error)?;
        let statements: [&str; 2] = match request.change {
            SubscriptionChange::Subscribe => [
                "DELETE FROM codex_message_board_subscription_opt_outs \
                 WHERE board=$1 AND target=$2 AND agent=$3",
                "INSERT INTO codex_message_board_subscriptions (board, target, agent) \
                 VALUES ($1,$2,$3) ON CONFLICT (board, target, agent) DO NOTHING",
            ],
            SubscriptionChange::Unsubscribe => [
                "DELETE FROM codex_message_board_subscriptions \
                 WHERE board=$1 AND target=$2 AND agent=$3",
                "INSERT INTO codex_message_board_subscription_opt_outs (board, target, agent) \
                 VALUES ($1,$2,$3) ON CONFLICT (board, target, agent) DO NOTHING",
            ],
        };
        for statement in statements {
            tx.execute(
                statement,
                sql_params![
                    self.identity.to_string(),
                    target_value.clone(),
                    target_agent.to_string()
                ],
            )
            .await
            .map_err(storage_error)?;
        }
        tx.commit().await.map_err(storage_error)?;
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
        let sql = self.antfly.sql().await.map_err(storage_error)?;
        let mut tx = sql.begin().await.map_err(storage_error)?;
        let post = self.require_post(&mut tx, request.message_id).await?;
        let _ = tx.rollback().await;
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

fn preview(post: StoredPost, max_chars: usize) -> PostPreview {
    let n_chars = post.text.chars().count();
    let text_preview = post.text.chars().take(max_chars).collect();
    PostPreview {
        metadata: post.metadata,
        text_preview,
        n_chars,
        truncated: n_chars > max_chars,
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

fn target_key(target: &SubscriptionTarget) -> codex_antfly::AntflyResult<String> {
    serde_json::to_string(target)
        .map_err(|err| codex_antfly::AntflyError::Malformed(err.to_string()))
}
