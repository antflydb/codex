//! Trait adapter and read queries. Writes live in the parent module; this
//! file scans, filters, sorts and pages, like
//! [`crate::in_memory::queries`] but reading fresh from Antfly each call.

use caseless::default_case_fold_str;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::Result;
use futures::future::BoxFuture;

use super::AntflyAgentMessageBoard;
use super::ChannelDoc;
use super::MAX_READ_CHARS;
use super::PostDoc;
use super::invalid;
use super::paging::Window;
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
use crate::SortDirection;
use crate::SubscriptionRequest;
use crate::SubscriptionState;
use crate::ThreadPage;
use crate::ThreadQuery;
use crate::ThreadSort;
use crate::ThreadSummary;

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
            let search = default_case_fold_str(&query.query.unwrap_or_default());
            let mut channels: Vec<ChannelDoc> = self
                .scan_channels()
                .await?
                .into_iter()
                .filter(|channel| channel.search.contains(&search))
                .collect();
            channels.sort_by(|a, b| activity_key(a).cmp(&activity_key(b)));
            if query.direction == SortDirection::NewestFirst {
                channels.reverse();
            }
            let window = Window::new(&query.page)?;
            let (offset, limit) = (window.offset(), window.limit);
            window.finish(
                channels
                    .into_iter()
                    .skip(offset)
                    .take(limit + 1)
                    .map(|channel| channel.summary())
                    .collect(),
            )
        })
    }

    fn list_threads(
        &self,
        caller: ThreadId,
        query: ThreadQuery,
    ) -> BoxFuture<'_, Result<Page<ThreadSummary>>> {
        Box::pin(async move {
            self.host.agent_path(caller).await?;
            self.require_channel(&query.channel_name).await?;
            let mut roots: Vec<PostDoc> = self
                .scan_posts()
                .await?
                .into_iter()
                .filter(|post| post.is_root() && post.metadata.channel_name == query.channel_name)
                .collect();
            roots.sort_by_key(|post| {
                let timestamp = match query.sort {
                    ThreadSort::Created => post.sort_key().0,
                    ThreadSort::Activity => post
                        .latest_reply_timestamp_micros
                        .unwrap_or(post.sort_key().0),
                };
                (timestamp, post.seq)
            });
            if query.direction == SortDirection::NewestFirst {
                roots.reverse();
            }
            let window = Window::new(&query.page)?;
            let page: Vec<PostDoc> = roots
                .into_iter()
                .skip(window.offset())
                .take(window.limit + 1)
                .collect();
            let chars = (query.max_chars_per_post.get() as usize)
                .min(MAX_READ_CHARS / (2 * page.len().min(window.limit).max(1)));
            let mut threads = Vec::with_capacity(page.len());
            for root in &page {
                let latest = match root.latest_reply_id {
                    Some(id) => Some(self.load_post(id).await?),
                    None => None,
                };
                let activity = latest.as_ref().map_or(root.metadata.created_at, |latest| {
                    latest.metadata.created_at.max(root.metadata.created_at)
                });
                threads.push(ThreadSummary {
                    thread_id: root.metadata.message_id,
                    root_post: root.preview(chars),
                    reply_count: root.reply_count,
                    last_activity_at: activity,
                    latest_reply: latest.map(|latest| latest.preview(chars)),
                });
            }
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
            let after = match query.after_message_id {
                Some(id) => Some(self.load_post(id).await?.sort_key()),
                None => None,
            };
            let search = query.query.as_deref().map(default_case_fold_str);
            let mut posts: Vec<PostDoc> = self
                .scan_posts()
                .await?
                .into_iter()
                .filter(|post| {
                    query
                        .channel_name
                        .as_deref()
                        .is_none_or(|channel| channel == post.metadata.channel_name)
                        && query
                            .author
                            .as_ref()
                            .is_none_or(|author| *author == post.metadata.author)
                        && search
                            .as_ref()
                            .is_none_or(|needle| post.search.contains(needle))
                        && after.is_none_or(|after| post.sort_key() > after)
                })
                .collect();
            // search_posts always orders newest-first, ignoring the requested direction.
            posts.sort_by_key(|post| std::cmp::Reverse(post.sort_key()));
            let window = Window::new(&query.page)?;
            let page: Vec<PostDoc> = posts
                .into_iter()
                .skip(window.offset())
                .take(window.limit + 1)
                .collect();
            let chars = (query.max_chars_per_post.get() as usize)
                .min(MAX_READ_CHARS / page.len().min(window.limit).max(1));
            window.finish(page.into_iter().map(|post| post.preview(chars)).collect())
        })
    }

    fn read_thread(
        &self,
        caller: ThreadId,
        request: ReadThreadRequest,
    ) -> BoxFuture<'_, Result<ThreadPage>> {
        Box::pin(async move {
            self.host.agent_path(caller).await?;
            let root = self.load_post(request.thread_id).await?;
            if root.metadata.thread_id != request.thread_id {
                return Err(invalid("thread_id must identify a top-level post"));
            }
            let mut replies: Vec<PostDoc> = self
                .scan_posts()
                .await?
                .into_iter()
                .filter(|post| {
                    post.metadata.thread_id == request.thread_id
                        && post.metadata.message_id != request.thread_id
                })
                .collect();
            replies.sort_by_key(|post| std::cmp::Reverse(post.sort_key()));
            let window = Window::new(&request.page)?;
            let page: Vec<PostDoc> = replies
                .into_iter()
                .skip(window.offset())
                .take(window.limit + 1)
                .collect();
            let chars = (request.max_chars_per_post.get() as usize)
                .min(MAX_READ_CHARS / (page.len().min(window.limit) + 1));
            Ok(ThreadPage {
                root_post: root.preview(chars),
                replies: window
                    .finish(page.into_iter().map(|post| post.preview(chars)).collect())?,
            })
        })
    }
}

/// `(activity, name)` ordering key for channel listing: the channel's
/// newest post timestamp, falling back to its creation time.
fn activity_key(channel: &ChannelDoc) -> (i64, &str) {
    (
        channel
            .last_message_timestamp_micros
            .unwrap_or_else(|| channel.created_at.timestamp_micros()),
        channel.name.as_str(),
    )
}
