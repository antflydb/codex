//! End-to-end checks of [`AntflyAgentMessageBoard`] against an embedded
//! `libantfly`. Mirrors the scenarios in `tests/local_board.rs` closely
//! enough to show the two backends agree, without depending on that
//! integration test's SQLite-only harness.

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use tokio::sync::Mutex;

use super::AntflyAgentMessageBoard;
use crate::AgentMessageBoard;
use crate::ChannelQuery;
use crate::CreateChannelRequest;
use crate::MessageBoardHost;
use crate::NotificationDelivery;
use crate::PageRequest;
use crate::PostDestination;
use crate::PostPreview;
use crate::PostQuery;
use crate::PostRequest;
use crate::ReadPostRequest;
use crate::ReadThreadRequest;
use crate::SortDirection;
use crate::SubscriptionChange;
use crate::SubscriptionRequest;
use crate::SubscriptionTarget;
use crate::ThreadQuery;
use crate::ThreadSort;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct FakeHost {
    agents: HashMap<ThreadId, AgentPath>,
    clock: Mutex<DateTime<Utc>>,
    notifications: Mutex<Vec<(ThreadId, PostPreview)>>,
}

impl FakeHost {
    fn new(agents: impl IntoIterator<Item = (ThreadId, AgentPath)>) -> Arc<Self> {
        Arc::new(Self {
            agents: agents.into_iter().collect(),
            clock: Mutex::new(Utc::now()),
            notifications: Mutex::new(Vec::new()),
        })
    }

    /// Advances the clock so successive posts have a strict happens-before
    /// order even when the wall clock's resolution would not guarantee it.
    async fn advance(&self) {
        let mut clock = self.clock.lock().await;
        *clock += chrono::Duration::milliseconds(1);
    }

    async fn notified(&self) -> Vec<(ThreadId, PostPreview)> {
        self.notifications.lock().await.clone()
    }
}

impl MessageBoardHost for FakeHost {
    fn agent_path(&self, caller: ThreadId) -> BoxFuture<'_, Result<AgentPath>> {
        Box::pin(async move {
            self.agents
                .get(&caller)
                .cloned()
                .ok_or_else(|| CodexErr::InvalidRequest("unknown agent".into()))
        })
    }

    fn resolve_agent(&self, path: AgentPath) -> BoxFuture<'_, Result<ThreadId>> {
        Box::pin(async move {
            self.agents
                .iter()
                .find(|(_, candidate)| **candidate == path)
                .map(|(id, _)| *id)
                .ok_or_else(|| CodexErr::InvalidRequest("unknown agent path".into()))
        })
    }

    fn current_time(&self, _caller: ThreadId) -> BoxFuture<'_, Result<DateTime<Utc>>> {
        Box::pin(async move { Ok(*self.clock.lock().await) })
    }

    fn notify(
        &self,
        recipient: ThreadId,
        post: PostPreview,
    ) -> BoxFuture<'_, Result<NotificationDelivery>> {
        Box::pin(async move {
            self.notifications.lock().await.push((recipient, post));
            Ok(NotificationDelivery::Accepted)
        })
    }
}

fn antfly(dir: &tempfile::TempDir) -> Arc<Antfly> {
    let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
    config.embedder = None;
    Arc::new(Antfly::new(config))
}

fn post_request(request_id: &str, destination: PostDestination, text: &str) -> PostRequest {
    PostRequest {
        request_id: request_id.to_string(),
        destination,
        text: text.to_string(),
        agents_to_notify: Vec::new(),
    }
}

fn page(limit: u32) -> PageRequest {
    PageRequest {
        cursor: None,
        limit: NonZeroU32::new(limit).unwrap_or(NonZeroU32::MIN),
    }
}

/// Closes the database (waiting for background work) before its directory
/// is removed.
async fn settle(antfly: Arc<Antfly>, dir: tempfile::TempDir) {
    if let Err(err) = antfly.close().await {
        panic!("close antfly: {err}");
    }
    drop(antfly);
    drop(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posting_updates_channel_and_thread_summaries_and_notifies_subscribers() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let root_id = ThreadId::new();
    let alice = ThreadId::new();
    let bob = ThreadId::new();
    let root_path = AgentPath::root();
    let alice_path = root_path.join("alice").map_err(CodexErr::InvalidRequest)?;
    let bob_path = root_path.join("bob").map_err(CodexErr::InvalidRequest)?;
    let host = FakeHost::new([
        (root_id, root_path),
        (alice, alice_path.clone()),
        (bob, bob_path.clone()),
    ]);
    let identity = SessionId::from(root_id);
    let board = AntflyAgentMessageBoard::open(Arc::clone(&antfly), identity, host.clone());

    assert_eq!(board.identity(), identity);

    let channel = board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Subscribe,
            },
        )
        .await?;
    assert_eq!(channel.message_count, 0);
    assert_eq!(channel.created_by, alice_path.clone());

    // Bob subscribes to the channel so he is notified of new roots.
    board
        .set_subscription(
            bob,
            SubscriptionRequest {
                target: SubscriptionTarget::Channel("general".to_string()),
                target_agent: None,
                change: SubscriptionChange::Subscribe,
            },
        )
        .await?;

    host.advance().await;
    let root = board
        .post(
            alice,
            post_request(
                "root-1",
                PostDestination::Channel("general".to_string()),
                "hello from alice",
            ),
        )
        .await?;
    assert_eq!(root.author, alice_path);

    let notified = host.notified().await;
    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].0, bob);
    assert_eq!(notified[0].1.metadata.message_id, root.message_id);

    let channel = board
        .list_channels(
            bob,
            ChannelQuery {
                query: None,
                direction: SortDirection::NewestFirst,
                page: page(10),
            },
        )
        .await?
        .results
        .into_iter()
        .next()
        .ok_or("expected a channel")?;
    assert_eq!(channel.message_count, 1);
    assert_eq!(channel.last_message_id, Some(root.message_id));

    host.advance().await;
    let reply = board
        .post(
            bob,
            post_request(
                "reply-1",
                PostDestination::Thread(root.thread_id),
                "hi alice",
            ),
        )
        .await?;

    // Bob's reply notifies alice: she is subscribed to the thread by having
    // posted its root, and is not removed for being the channel's creator.
    let notified = host.notified().await;
    assert_eq!(notified.len(), 2);
    assert_eq!(notified[1].0, alice);
    assert_eq!(notified[1].1.metadata.message_id, reply.message_id);

    let threads = board
        .list_threads(
            alice,
            ThreadQuery {
                channel_name: "general".to_string(),
                sort: ThreadSort::Activity,
                direction: SortDirection::NewestFirst,
                page: page(10),
                max_chars_per_post: NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN),
            },
        )
        .await?
        .results;
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].reply_count, 1);
    assert_eq!(
        threads[0]
            .latest_reply
            .as_ref()
            .map(|post| post.metadata.message_id),
        Some(reply.message_id)
    );

    let thread_page = board
        .read_thread(
            alice,
            ReadThreadRequest {
                thread_id: root.thread_id,
                page: page(10),
                max_chars_per_post: NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN),
            },
        )
        .await?;
    assert_eq!(thread_page.root_post.metadata.message_id, root.message_id);
    assert_eq!(thread_page.replies.results.len(), 1);
    assert_eq!(
        thread_page.replies.results[0].metadata.message_id,
        reply.message_id
    );

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posting_is_idempotent_on_request_id_and_rejects_mismatched_retries() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let root_id = ThreadId::new();
    let alice = ThreadId::new();
    let host = FakeHost::new([
        (root_id, AgentPath::root()),
        (
            alice,
            AgentPath::root()
                .join("alice")
                .map_err(CodexErr::InvalidRequest)?,
        ),
    ]);
    let identity = SessionId::from(root_id);
    let board = AntflyAgentMessageBoard::open(Arc::clone(&antfly), identity, host);

    board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Unsubscribe,
            },
        )
        .await?;

    let first = board
        .post(
            alice,
            post_request(
                "dup",
                PostDestination::Channel("general".to_string()),
                "first",
            ),
        )
        .await?;
    let retry = board
        .post(
            alice,
            post_request(
                "dup",
                PostDestination::Channel("general".to_string()),
                "first",
            ),
        )
        .await?;
    assert_eq!(first.message_id, retry.message_id);

    let mismatched = board
        .post(
            alice,
            post_request(
                "dup",
                PostDestination::Channel("general".to_string()),
                "different text",
            ),
        )
        .await;
    assert!(mismatched.is_err());

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_unsubscribe_survives_participation_and_search_filters_posts() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let root_id = ThreadId::new();
    let alice = ThreadId::new();
    let bob = ThreadId::new();
    let host = FakeHost::new([
        (root_id, AgentPath::root()),
        (
            alice,
            AgentPath::root()
                .join("alice")
                .map_err(CodexErr::InvalidRequest)?,
        ),
        (
            bob,
            AgentPath::root()
                .join("bob")
                .map_err(CodexErr::InvalidRequest)?,
        ),
    ]);
    let identity = SessionId::from(root_id);
    let board = AntflyAgentMessageBoard::open(Arc::clone(&antfly), identity, host.clone());

    board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Subscribe,
            },
        )
        .await?;
    host.advance().await;
    let root = board
        .post(
            alice,
            post_request(
                "root",
                PostDestination::Channel("general".to_string()),
                "alpha bravo",
            ),
        )
        .await?;

    // Bob explicitly unsubscribes from the thread before ever posting in it.
    board
        .set_subscription(
            bob,
            SubscriptionRequest {
                target: SubscriptionTarget::Thread(root.thread_id),
                target_agent: None,
                change: SubscriptionChange::Unsubscribe,
            },
        )
        .await?;

    host.advance().await;
    // Bob now participates by replying; participation must not override his
    // explicit opt-out, so a later reply from someone else must not notify him.
    board
        .post(
            bob,
            post_request(
                "bob-reply",
                PostDestination::Thread(root.thread_id),
                "charlie delta",
            ),
        )
        .await?;

    host.advance().await;
    board
        .post(
            alice,
            post_request(
                "alice-reply-2",
                PostDestination::Thread(root.thread_id),
                "echo foxtrot",
            ),
        )
        .await?;

    let notified_bob = host
        .notified()
        .await
        .into_iter()
        .filter(|(recipient, _)| *recipient == bob)
        .count();
    assert_eq!(
        notified_bob, 0,
        "bob's explicit unsubscribe must be preserved"
    );

    let matches = board
        .search_posts(
            alice,
            PostQuery {
                channel_name: None,
                query: Some("BRAVO".to_string()),
                after_message_id: None,
                author: None,
                page: page(10),
                max_chars_per_post: NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN),
            },
        )
        .await?
        .results;
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].metadata.message_id, root.message_id);

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_post_windows_text_and_delete_boards_tombstones_future_writes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let root_id = ThreadId::new();
    let alice = ThreadId::new();
    let host = FakeHost::new([
        (root_id, AgentPath::root()),
        (
            alice,
            AgentPath::root()
                .join("alice")
                .map_err(CodexErr::InvalidRequest)?,
        ),
    ]);
    let identity = SessionId::from(root_id);
    let board = AntflyAgentMessageBoard::open(Arc::clone(&antfly), identity, host);

    board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Unsubscribe,
            },
        )
        .await?;
    let root = board
        .post(
            alice,
            post_request(
                "root",
                PostDestination::Channel("general".to_string()),
                "0123456789",
            ),
        )
        .await?;

    let chunk = board
        .read_post(
            alice,
            ReadPostRequest {
                message_id: root.message_id,
                offset_chars: 2,
                limit_chars: NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
            },
        )
        .await?;
    assert_eq!(chunk.text, "234");
    assert_eq!(chunk.n_chars, 10);
    assert_eq!(chunk.next_offset_chars, 5);

    AntflyAgentMessageBoard::delete_boards(&antfly, &[identity]).await?;

    let after_delete = board
        .post(
            alice,
            post_request(
                "post-after-delete",
                PostDestination::Channel("general".to_string()),
                "should fail",
            ),
        )
        .await;
    assert!(after_delete.is_err());

    // Deleting again must be safe to retry.
    AntflyAgentMessageBoard::delete_boards(&antfly, &[identity]).await?;
    // An empty root list is also a safe no-op.
    AntflyAgentMessageBoard::delete_boards(&antfly, &[]).await?;

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_channels_orders_by_activity_then_name_and_filters_by_substring() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let root_id = ThreadId::new();
    let alice = ThreadId::new();
    let host = FakeHost::new([
        (root_id, AgentPath::root()),
        (
            alice,
            AgentPath::root()
                .join("alice")
                .map_err(CodexErr::InvalidRequest)?,
        ),
    ]);
    let identity = SessionId::from(root_id);
    let board = AntflyAgentMessageBoard::open(Arc::clone(&antfly), identity, host.clone());

    for name in ["alpha", "beta-notes", "gamma"] {
        board
            .create_channel(
                alice,
                CreateChannelRequest {
                    channel_name: name.to_string(),
                    subscription: SubscriptionChange::Unsubscribe,
                },
            )
            .await?;
        host.advance().await;
    }
    // Bump "alpha" to the most recent activity.
    board
        .post(
            alice,
            post_request("bump", PostDestination::Channel("alpha".to_string()), "hi"),
        )
        .await?;

    let names: Vec<String> = board
        .list_channels(
            alice,
            ChannelQuery {
                query: None,
                direction: SortDirection::NewestFirst,
                page: page(10),
            },
        )
        .await?
        .results
        .into_iter()
        .map(|channel| channel.channel_name)
        .collect();
    assert_eq!(names, vec!["alpha", "gamma", "beta-notes"]);

    let filtered: HashSet<String> = board
        .list_channels(
            alice,
            ChannelQuery {
                query: Some("NOTE".to_string()),
                direction: SortDirection::OldestFirst,
                page: page(10),
            },
        )
        .await?
        .results
        .into_iter()
        .map(|channel| channel.channel_name)
        .collect();
    assert_eq!(filtered, HashSet::from(["beta-notes".to_string()]));

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channel_creation_rejects_duplicates_and_invalid_names() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let root_id = ThreadId::new();
    let alice = ThreadId::new();
    let host = FakeHost::new([
        (root_id, AgentPath::root()),
        (
            alice,
            AgentPath::root()
                .join("alice")
                .map_err(CodexErr::InvalidRequest)?,
        ),
    ]);
    let identity = SessionId::from(root_id);
    let board = AntflyAgentMessageBoard::open(Arc::clone(&antfly), identity, host);

    board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Unsubscribe,
            },
        )
        .await?;
    let duplicate = board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Unsubscribe,
            },
        )
        .await;
    assert!(duplicate.is_err());

    let invalid_name = board
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: " padded ".to_string(),
                subscription: SubscriptionChange::Unsubscribe,
            },
        )
        .await;
    assert!(invalid_name.is_err());

    settle(antfly, dir).await;
    Ok(())
}

/// Smoke-checks that an id round-trips; mostly guards against accidental key
/// collisions between unrelated boards sharing one Antfly database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_boards_in_one_database_do_not_see_each_others_data() -> TestResult {
    let dir = tempfile::tempdir()?;
    let antfly = antfly(&dir);
    let alice = ThreadId::new();
    let board_a_id = SessionId::from(ThreadId::new());
    let board_b_id = SessionId::from(ThreadId::new());
    let host = FakeHost::new([(alice, AgentPath::root())]);
    let board_a = AntflyAgentMessageBoard::open(Arc::clone(&antfly), board_a_id, host.clone());
    let board_b = AntflyAgentMessageBoard::open(Arc::clone(&antfly), board_b_id, host);

    board_a
        .create_channel(
            alice,
            CreateChannelRequest {
                channel_name: "general".to_string(),
                subscription: SubscriptionChange::Unsubscribe,
            },
        )
        .await?;

    let board_b_channels = board_b
        .list_channels(
            alice,
            ChannelQuery {
                query: None,
                direction: SortDirection::NewestFirst,
                page: page(10),
            },
        )
        .await?;
    assert!(board_b_channels.results.is_empty());

    settle(antfly, dir).await;
    Ok(())
}
