//! Trait adapter and read queries, translating
//! [`crate::local::queries`]'s SQLite statements into Antfly's PostgreSQL
//! dialect: `$n` parameters, no `MATERIALIZED` CTE hint, and a `LIKE`
//! pattern instead of `instr`/`strpos` substring matching (`strpos` is not
//! supported: `antfly SQL failed (25P02): The transaction is aborted; roll
//! back before continuing`, the engine's generic report of a failed
//! statement, confirmed by isolating the call; `LIKE` with a manually
//! backslash-escaped needle reproduces `instr`'s plain substring semantics
//! and antflydb/antfly has no `ESCAPE` clause support, so escaping happens
//! in Rust and relies on `\` being the default `LIKE` escape character).

use caseless::default_case_fold_str;
use codex_antfly::sql::SqlValue;
use codex_antfly::sql_params;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::Result;
use futures::future::BoxFuture;

use super::AntflyAgentMessageBoard;
use super::MAX_READ_CHARS;
use super::invalid;
use super::paging::Window;
use super::paging::decode_posts;
use super::paging::direction;
use super::preview;
use super::storage_error;
use crate::AgentMessageBoard;
use crate::ChannelQuery;
use crate::ChannelSummary;
use crate::CreateChannelRequest;
use crate::Page;
use crate::PostContent;
use crate::PostMetadata;
use crate::PostPreview;
use crate::PostQuery;
use crate::PostRequest;
use crate::ReadPostRequest;
use crate::ReadThreadRequest;
use crate::SubscriptionRequest;
use crate::SubscriptionState;
use crate::ThreadPage;
use crate::ThreadQuery;
use crate::ThreadSort;
use crate::ThreadSummary;

/// Builds a parameterized `$n` SQL statement incrementally.
struct Builder {
    sql: String,
    params: Vec<SqlValue>,
}

impl Builder {
    fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    fn push(&mut self, text: &str) -> &mut Self {
        self.sql.push_str(text);
        self
    }

    fn bind(&mut self, value: impl Into<SqlValue>) -> &mut Self {
        self.params.push(value.into());
        self.sql.push_str(&format!("${}", self.params.len()));
        self
    }
}

/// A `LIKE` pattern matching `needle` as a plain substring, anywhere in the
/// column: `\`, `%` and `_` in `needle` are backslash-escaped so they are
/// not treated as `LIKE` wildcards or the escape character itself.
fn substring_pattern(needle: &str) -> String {
    let mut escaped = String::with_capacity(needle.len() + 2);
    escaped.push('%');
    for ch in needle.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped.push('%');
    escaped
}

impl AgentMessageBoard for AntflyAgentMessageBoard {
    fn identity(&self) -> SessionId {
        self.identity
    }

    fn create_channel(
        &self,
        caller: ThreadId,
        request: CreateChannelRequest,
    ) -> BoxFuture<'_, Result<ChannelSummary>> {
        Box::pin(AntflyAgentMessageBoard::create_channel(
            self, caller, request,
        ))
    }

    fn post(&self, caller: ThreadId, request: PostRequest) -> BoxFuture<'_, Result<PostMetadata>> {
        Box::pin(AntflyAgentMessageBoard::post(self, caller, request))
    }

    fn set_subscription(
        &self,
        caller: ThreadId,
        request: SubscriptionRequest,
    ) -> BoxFuture<'_, Result<SubscriptionState>> {
        Box::pin(AntflyAgentMessageBoard::set_subscription(
            self, caller, request,
        ))
    }

    fn read_post(
        &self,
        caller: ThreadId,
        request: ReadPostRequest,
    ) -> BoxFuture<'_, Result<PostContent>> {
        Box::pin(AntflyAgentMessageBoard::read_post(self, caller, request))
    }

    fn list_channels(
        &self,
        caller: ThreadId,
        query: ChannelQuery,
    ) -> BoxFuture<'_, Result<Page<ChannelSummary>>> {
        Box::pin(async move {
            self.host.agent_path(caller).await?;
            let sql = self.antfly.sql().await.map_err(storage_error)?;
            let mut tx = sql.begin().await.map_err(storage_error)?;
            let window = Window::new(&query.page)?;
            let order = direction(query.direction);
            let mut builder = Builder::new(
                "SELECT c.name AS name FROM codex_message_board_channels c WHERE c.board=",
            );
            builder.bind(self.identity.to_string());
            builder.push(" AND c.name_search LIKE ");
            builder.bind(substring_pattern(&default_case_fold_str(
                &query.query.unwrap_or_default(),
            )));
            builder
                .push(
                    " ORDER BY COALESCE(
                    (SELECT MAX(p.timestamp_us) FROM codex_message_board_posts p
                     WHERE p.board=c.board AND p.channel=c.name),
                    c.timestamp_us) ",
                )
                .push(order)
                .push(", c.name ")
                .push(order)
                .push(" LIMIT ");
            builder.bind((window.limit + 1) as i64);
            builder.push(" OFFSET ");
            builder.bind(window.offset());
            let rows = tx
                .fetch_all(&builder.sql, builder.params)
                .await
                .map_err(storage_error)?;
            let mut channels = Vec::with_capacity(rows.len());
            for row in rows {
                let name = row.string("name").map_err(storage_error)?;
                channels.push(self.require_channel_summary(&mut tx, &name).await?);
            }
            let _ = tx.rollback().await;
            window.finish(channels)
        })
    }

    fn list_threads(
        &self,
        caller: ThreadId,
        query: ThreadQuery,
    ) -> BoxFuture<'_, Result<Page<ThreadSummary>>> {
        Box::pin(async move {
            self.host.agent_path(caller).await?;
            let sql = self.antfly.sql().await.map_err(storage_error)?;
            let mut tx = sql.begin().await.map_err(storage_error)?;
            let exists = tx
                .fetch_optional(
                    "SELECT 1 AS present FROM codex_message_board_channels WHERE board=$1 AND name=$2",
                    sql_params![self.identity.to_string(), query.channel_name.clone()],
                )
                .await
                .map_err(storage_error)?
                .is_some();
            if !exists {
                let _ = tx.rollback().await;
                return Err(invalid("channel not found in this board"));
            }
            let window = Window::new(&query.page)?;
            let order = direction(query.direction);
            let mut builder = Builder::new(
                "WITH page AS (SELECT p.board AS board, p.id AS id, p.payload AS payload, p.seq AS seq, ",
            );
            match query.sort {
                ThreadSort::Created => {
                    builder.push("p.timestamp_us");
                }
                ThreadSort::Activity => {
                    builder.push(
                        "(SELECT MAX(r.timestamp_us) FROM codex_message_board_posts r \
                         WHERE r.board=p.board AND r.root=p.id)",
                    );
                }
            }
            builder.push(" AS sort_timestamp FROM codex_message_board_posts p WHERE p.board=");
            builder.bind(self.identity.to_string());
            builder.push(" AND p.channel=");
            builder.bind(query.channel_name);
            builder
                .push(" AND p.is_root=true ORDER BY sort_timestamp ")
                .push(order)
                .push(", p.seq ")
                .push(order)
                .push(" LIMIT ");
            builder.bind((window.limit + 1) as i64);
            builder.push(" OFFSET ");
            builder.bind(window.offset());
            builder
                .push(
                    ") SELECT p.id AS id, p.payload AS payload,
                     (SELECT COUNT(*) FROM codex_message_board_posts r
                      WHERE r.board=p.board AND r.root=p.id AND r.is_root=false) AS reply_count
                     FROM page p ORDER BY p.sort_timestamp ",
                )
                .push(order)
                .push(", p.seq ")
                .push(order);
            let rows = tx
                .fetch_all(&builder.sql, builder.params)
                .await
                .map_err(storage_error)?;
            let chars = (query.max_chars_per_post.get() as usize)
                .min(MAX_READ_CHARS / (2 * rows.len().min(window.limit).max(1)));
            let mut threads = Vec::with_capacity(rows.len());
            for row in rows {
                let root: super::StoredPost =
                    serde_json::from_value(row.json("payload").map_err(storage_error)?)
                        .map_err(storage_error)?;
                let id = root.metadata.message_id;
                let reply_count = row.i64("reply_count").map_err(storage_error)? as usize;
                // A non-aggregate scalar correlated subquery in the `SELECT`
                // list fails in Antfly's SQL engine (42703; see
                // `AntflyAgentMessageBoard::channel_summary`), so the latest
                // reply is a separate, uncorrelated query per thread instead
                // of a subquery column on the statement above.
                let latest: Option<super::StoredPost> = if reply_count == 0 {
                    None
                } else {
                    let root_id = row.string("id").map_err(storage_error)?;
                    tx.fetch_optional(
                        "SELECT payload AS payload FROM codex_message_board_posts \
                         WHERE board=$1 AND root=$2 AND is_root=false \
                         ORDER BY timestamp_us DESC, seq DESC LIMIT 1",
                        sql_params![self.identity.to_string(), root_id],
                    )
                    .await
                    .map_err(storage_error)?
                    .map(|row| row.json("payload"))
                    .transpose()
                    .map_err(storage_error)?
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(storage_error)?
                };
                let activity = latest.as_ref().map_or(root.metadata.created_at, |latest| {
                    latest.metadata.created_at.max(root.metadata.created_at)
                });
                threads.push(ThreadSummary {
                    thread_id: id,
                    root_post: preview(root, chars),
                    reply_count,
                    last_activity_at: activity,
                    latest_reply: latest.map(|post| preview(post, chars)),
                });
            }
            let _ = tx.rollback().await;
            window.finish(threads)
        })
    }

    fn search_posts(
        &self,
        caller: ThreadId,
        query: PostQuery,
    ) -> BoxFuture<'_, Result<Page<PostPreview>>> {
        Box::pin(async move {
            self.host.agent_path(caller).await?;
            let sql = self.antfly.sql().await.map_err(storage_error)?;
            let mut tx = sql.begin().await.map_err(storage_error)?;
            let after = if let Some(id) = query.after_message_id {
                let row = tx
                    .fetch_optional(
                        "SELECT timestamp_us AS timestamp_us, seq AS seq \
                         FROM codex_message_board_posts WHERE board=$1 AND id=$2",
                        sql_params![self.identity.to_string(), id.to_string()],
                    )
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| invalid("post not found in this board"))?;
                Some((
                    row.i64("timestamp_us").map_err(storage_error)?,
                    row.i64("seq").map_err(storage_error)?,
                ))
            } else {
                None
            };
            let window = Window::new(&query.page)?;
            let mut builder = Builder::new(
                "SELECT payload AS payload FROM codex_message_board_posts WHERE board=",
            );
            builder.bind(self.identity.to_string());
            if let Some(channel) = query.channel_name {
                builder.push(" AND channel=").bind(channel);
            }
            if let Some(author) = query.author {
                builder.push(" AND author=").bind(author.to_string());
            }
            if let Some(text) = query.query {
                builder.push(" AND body_search LIKE ");
                builder.bind(substring_pattern(&default_case_fold_str(&text)));
            }
            if let Some((timestamp, seq)) = after {
                builder.push(" AND (timestamp_us>");
                builder.bind(timestamp);
                builder.push(" OR (timestamp_us=");
                builder.bind(timestamp);
                builder.push(" AND seq>");
                builder.bind(seq);
                builder.push("))");
            }
            builder.push(" ORDER BY timestamp_us DESC, seq DESC LIMIT ");
            builder.bind((window.limit + 1) as i64);
            builder.push(" OFFSET ");
            builder.bind(window.offset());
            let rows = tx
                .fetch_all(&builder.sql, builder.params)
                .await
                .map_err(storage_error)?;
            let _ = tx.rollback().await;
            let payloads: Result<Vec<serde_json::Value>> = rows
                .into_iter()
                .map(|row| row.json("payload").map_err(storage_error))
                .collect();
            let posts = decode_posts(payloads?)?;
            let chars = (query.max_chars_per_post.get() as usize)
                .min(MAX_READ_CHARS / posts.len().min(window.limit).max(1));
            window.finish(posts.into_iter().map(|post| preview(post, chars)).collect())
        })
    }

    fn read_thread(
        &self,
        caller: ThreadId,
        request: ReadThreadRequest,
    ) -> BoxFuture<'_, Result<ThreadPage>> {
        Box::pin(async move {
            self.host.agent_path(caller).await?;
            let sql = self.antfly.sql().await.map_err(storage_error)?;
            let mut tx = sql.begin().await.map_err(storage_error)?;
            let root = self.require_post(&mut tx, request.thread_id).await?;
            if root.metadata.thread_id != request.thread_id {
                let _ = tx.rollback().await;
                return Err(invalid("thread_id must identify a top-level post"));
            }
            let window = Window::new(&request.page)?;
            let rows = tx
                .fetch_all(
                    "SELECT payload AS payload FROM codex_message_board_posts \
                     WHERE board=$1 AND root=$2 AND is_root=false \
                     ORDER BY timestamp_us DESC, seq DESC LIMIT $3 OFFSET $4",
                    sql_params![
                        self.identity.to_string(),
                        request.thread_id.to_string(),
                        (window.limit + 1) as i64,
                        window.offset()
                    ],
                )
                .await
                .map_err(storage_error)?;
            let _ = tx.rollback().await;
            let payloads: Result<Vec<serde_json::Value>> = rows
                .into_iter()
                .map(|row| row.json("payload").map_err(storage_error))
                .collect();
            let posts = decode_posts(payloads?)?;
            let chars = (request.max_chars_per_post.get() as usize)
                .min(MAX_READ_CHARS / (posts.len().min(window.limit) + 1));
            Ok(ThreadPage {
                root_post: preview(root, chars),
                replies: window
                    .finish(posts.into_iter().map(|post| preview(post, chars)).collect())?,
            })
        })
    }
}
