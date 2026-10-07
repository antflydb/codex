//! End-to-end checks of [`AntflyThreadStore`] against an embedded `libantfly`.
//! Calls into `libantfly` happen on the store's own large-stack executor.

use std::sync::Arc;
use std::time::Duration;

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_app_server_protocol::ThreadTimelineEntry;
use codex_protocol::ThreadId;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::MessagePhase;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::user_input::UserInput;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;

use super::AntflyThreadStore;
use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ItemSortKey;
use crate::ListItemsParams;
use crate::ListItemsPosition;
use crate::ListThreadsParams;
use crate::ListTimelineParams;
use crate::ListTurnsParams;
use crate::LoadThreadHistoryParams;
use crate::MoveThreadToSectionParams;
use crate::PersistContext;
use crate::ReadThreadParams;
use crate::SearchThreadOccurrencesParams;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadMetadataPatch;
use crate::ThreadPersistenceMetadata;
use crate::ThreadSortKey;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::UpdateThreadMetadataParams;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn create_params(thread_id: ThreadId) -> CreateThreadParams {
    CreateThreadParams {
        creator_user_id: None,
        creator_account_id: None,
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "test_originator".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        initial_window_id: uuid::Uuid::now_v7().to_string(),
        runtime_workspace_roots: None,
        metadata: ThreadPersistenceMetadata {
            cwd: Some("/work/repo".into()),
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}

fn user_message(text: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: text.to_string(),
        ..Default::default()
    }))
}

fn agent_message(text: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
        message: text.to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    }))
}

fn paginated_params(thread_id: ThreadId) -> CreateThreadParams {
    CreateThreadParams {
        history_mode: ThreadHistoryMode::Paginated,
        ..create_params(thread_id)
    }
}

fn turn_started(turn_id: &str, started_at: i64) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_attribution: None,
        turn_id: turn_id.to_string(),
        root_turn_id: None,
        trace_id: None,
        started_at: Some(started_at),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_complete(turn_id: &str, started_at: i64, completed_at: i64) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        root_turn_id: None,
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(started_at),
        completed_at: Some(completed_at),
        duration_ms: Some((completed_at - started_at) * 1000),
        time_to_first_token_ms: None,
    }))
}

fn user_message_item(id: &str, text: &str) -> TurnItem {
    TurnItem::UserMessage(UserMessageItem {
        id: id.to_string(),
        client_id: None,
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
    })
}

fn agent_message_item(id: &str, text: &str, phase: Option<MessagePhase>) -> TurnItem {
    TurnItem::AgentMessage(AgentMessageItem {
        id: id.to_string(),
        content: vec![AgentMessageContent::Text {
            text: text.to_string(),
        }],
        phase,
        memory_citation: None,
        delivery: None,
        questions: None,
    })
}

fn item_completed(
    thread_id: ThreadId,
    turn_id: &str,
    item: TurnItem,
    started_at_ms: i64,
    completed_at_ms: i64,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item,
        started_at_ms: Some(started_at_ms),
        completed_at_ms,
    }))
}

fn store(dir: &tempfile::TempDir) -> AntflyThreadStore {
    let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
    config.embedder = None;
    AntflyThreadStore::new(Arc::new(Antfly::new(config)))
}

async fn start_thread(
    store: &AntflyThreadStore,
    preview: &str,
    messages: &[RolloutItem],
) -> Result<ThreadId, ThreadStoreError> {
    let thread_id = ThreadId::new();
    store.create_thread(create_params(thread_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: messages.to_vec(),
        })
        .await?;
    store
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id,
            patch: ThreadMetadataPatch {
                preview: Some(preview.to_string()),
                updated_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
            include_archived: false,
        })
        .await?;
    Ok(thread_id)
}

fn list_params() -> ListThreadsParams {
    ListThreadsParams {
        page_size: 10,
        cursor: None,
        sort_key: ThreadSortKey::UpdatedAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: Vec::new(),
        model_providers: None,
        cwd_filters: None,
        section: None,
        project_id: None,
        archived: false,
        search_term: None,
        relation_filter: None,
        use_state_db_only: false,
    }
}

async fn settle(store: AntflyThreadStore, dir: tempfile::TempDir) {
    drop(store);
    // The backend closes on its executor; let it finish before the dir goes.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_round_trips_through_antfly() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);

    // A created thread is not durable until it is persisted or appended to.
    let empty = ThreadId::new();
    store.create_thread(create_params(empty)).await?;
    assert!(matches!(
        store
            .read_thread(ReadThreadParams {
                thread_id: empty,
                include_archived: true,
                include_history: false,
            })
            .await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    store.shutdown_thread(empty).await?;

    let thread_id = start_thread(
        &store,
        "fix the raft snapshot test",
        &[
            user_message("please fix the raft snapshot test"),
            agent_message("I updated the snapshot fixture."),
        ],
    )
    .await?;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;

    let read = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: true,
        })
        .await?;
    assert_eq!(read.preview, "fix the raft snapshot test");
    assert_eq!(read.cwd, std::path::PathBuf::from("/work/repo"));
    let history = read
        .history
        .map(|history| history.items)
        .unwrap_or_default();
    assert_eq!(history.len(), 3, "session meta plus two messages");
    assert!(matches!(history[0], RolloutItem::SessionMeta(_)));

    // Shutdown then resume returns the same history.
    store.shutdown_thread(thread_id).await?;
    let resumed = store
        .resume_thread(crate::ResumeThreadParams {
            thread_id,
            rollout_path: None,
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: create_params(thread_id).metadata,
        })
        .await?;
    assert_eq!(resumed.len(), 3);
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![user_message("and add a regression test")],
        })
        .await?;
    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await?;
    assert_eq!(history.items.len(), 4);

    // Archive moves it between listings; unarchive moves it back.
    let listed = store.list_threads(list_params()).await?;
    assert_eq!(
        listed.items.iter().map(|t| t.thread_id).collect::<Vec<_>>(),
        vec![thread_id]
    );
    store
        .archive_thread(ArchiveThreadParams { thread_id })
        .await?;
    assert!(store.list_threads(list_params()).await?.items.is_empty());
    let archived = store
        .list_threads(ListThreadsParams {
            archived: true,
            ..list_params()
        })
        .await?;
    assert_eq!(archived.items.len(), 1);
    let restored = store
        .unarchive_thread(ArchiveThreadParams { thread_id })
        .await?;
    assert!(restored.archived_at.is_none());

    store.shutdown_thread(thread_id).await?;
    store
        .delete_thread(DeleteThreadParams { thread_id })
        .await?;
    assert!(matches!(
        store.delete_thread(DeleteThreadParams { thread_id }).await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_pages_newest_first_and_filters() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let mut ids = Vec::new();
    for index in 0..5 {
        let id = start_thread(&store, &format!("thread {index}"), &[user_message("hi")]).await?;
        ids.push(id);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let first = store
        .list_threads(ListThreadsParams {
            page_size: 2,
            ..list_params()
        })
        .await?;
    assert_eq!(
        first.items.iter().map(|t| t.thread_id).collect::<Vec<_>>(),
        vec![ids[4], ids[3]]
    );
    let second = store
        .list_threads(ListThreadsParams {
            page_size: 2,
            cursor: first.next_cursor.clone(),
            ..list_params()
        })
        .await?;
    assert_eq!(
        second.items.iter().map(|t| t.thread_id).collect::<Vec<_>>(),
        vec![ids[2], ids[1]]
    );
    let ascending = store
        .list_threads(ListThreadsParams {
            page_size: 2,
            sort_direction: SortDirection::Asc,
            ..list_params()
        })
        .await?;
    assert_eq!(
        ascending
            .items
            .iter()
            .map(|t| t.thread_id)
            .collect::<Vec<_>>(),
        vec![ids[0], ids[1]]
    );
    let searched = store
        .list_threads(ListThreadsParams {
            search_term: Some("thread 3".to_string()),
            ..list_params()
        })
        .await?;
    assert_eq!(
        searched
            .items
            .iter()
            .map(|t| t.thread_id)
            .collect::<Vec<_>>(),
        vec![ids[3]]
    );

    store
        .move_thread_to_section(MoveThreadToSectionParams {
            thread_id: ids[1],
            section: Some("work".to_string()),
            before_thread_id: None,
        })
        .await?;
    store
        .move_thread_to_section(MoveThreadToSectionParams {
            thread_id: ids[2],
            section: Some("work".to_string()),
            before_thread_id: Some(ids[1]),
        })
        .await?;
    let section = store
        .list_threads(ListThreadsParams {
            sort_key: ThreadSortKey::SectionPosition,
            sort_direction: SortDirection::Asc,
            section: Some(Some("work".to_string())),
            ..list_params()
        })
        .await?;
    assert_eq!(
        section
            .items
            .iter()
            .map(|t| t.thread_id)
            .collect::<Vec<_>>(),
        vec![ids[2], ids[1]]
    );
    for id in &ids {
        store.shutdown_thread(*id).await?;
    }
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_threads_finds_message_text() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let raft = start_thread(
        &store,
        "raft work",
        &[user_message(
            "the raft snapshot install races with compaction",
        )],
    )
    .await?;
    let other = start_thread(&store, "docs", &[user_message("write the release notes")]).await?;
    let mut results = Vec::new();
    for _ in 0..50 {
        results = store
            .search_threads(SearchThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::UpdatedAt,
                sort_direction: SortDirection::Desc,
                allowed_sources: Vec::new(),
                archived: false,
                search_term: "snapshot".to_string(),
            })
            .await?
            .items;
        if !results.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        results
            .iter()
            .map(|r| r.thread.thread_id)
            .collect::<Vec<_>>(),
        vec![raft]
    );
    assert!(results[0].snippet.contains("snapshot"));
    store.shutdown_thread(raft).await?;
    store.shutdown_thread(other).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_history_mode_is_paginated() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    assert_eq!(store.default_history_mode(), ThreadHistoryMode::Paginated);
    assert!(store.supports_paginated_history_lists());
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paginated_projection_builds_turns_and_items() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = ThreadId::new();
    store.create_thread(paginated_params(thread_id)).await?;

    // One full turn: started, a user item, an in-progress agent item (no
    // phase) that a later duplicate completion then supersedes with the
    // final phased answer, then the turn completes.
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1", 1_000),
                item_completed(
                    thread_id,
                    "turn-1",
                    user_message_item("item-user", "please fix the widget counter"),
                    1_000,
                    1_000,
                ),
                item_completed(
                    thread_id,
                    "turn-1",
                    agent_message_item("item-agent", "working on it", None),
                    1_001,
                    1_001,
                ),
            ],
        })
        .await?;

    // Duplicate completion of the same item id: updated_at_ordinal moves,
    // rollout_ordinal (creation) and created_at_ms must not.
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                item_completed(
                    thread_id,
                    "turn-1",
                    agent_message_item(
                        "item-agent",
                        "fixed the widget counter",
                        Some(MessagePhase::FinalAnswer),
                    ),
                    1_002,
                    1_003,
                ),
                turn_complete("turn-1", 1_000, 1_003),
            ],
        })
        .await?;

    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await?;
    assert_eq!(turns.turns.len(), 1);
    let turn = &turns.turns[0];
    assert_eq!(turn.turn_id, "turn-1");
    assert_eq!(turn.status, StoredTurnStatus::Completed);
    assert_eq!(turn.started_at, Some(1_000));
    assert_eq!(turn.completed_at, Some(1_003));
    // Summary is [first_user_item, final_agent_item], ordinal order.
    assert_eq!(turn.items.len(), 2);
    assert_eq!(turn.items[0].item_id, "item-user");
    assert_eq!(turn.items[1].item_id, "item-agent");

    let items = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: None,
            include_archived: false,
            position: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await?;
    assert_eq!(items.items.len(), 2);
    let agent_item = items
        .items
        .iter()
        .find(|item| item.item_id == "item-agent")
        .expect("agent item present");
    assert!(agent_item.updated_at_ordinal > items.items[0].updated_at_ordinal);

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_turns_and_items_page_both_directions_with_anchor() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = ThreadId::new();
    store.create_thread(paginated_params(thread_id)).await?;

    let mut items = Vec::new();
    for index in 0..5i64 {
        let turn_id = format!("turn-{index}");
        items.push(turn_started(&turn_id, 1_000 + index));
        items.push(item_completed(
            thread_id,
            &turn_id,
            user_message_item(&format!("u{index}"), &format!("message {index}")),
            1_000 + index,
            1_000 + index,
        ));
        items.push(item_completed(
            thread_id,
            &turn_id,
            agent_message_item(
                &format!("a{index}"),
                &format!("reply {index}"),
                Some(MessagePhase::FinalAnswer),
            ),
            1_000 + index,
            1_000 + index,
        ));
        items.push(turn_complete(&turn_id, 1_000 + index, 1_000 + index));
    }
    store
        .append_items(AppendThreadItemsParams { thread_id, items })
        .await?;

    let turn_ids = |page: &crate::TurnPage| {
        page.turns
            .iter()
            .map(|turn| turn.turn_id.clone())
            .collect::<Vec<_>>()
    };

    let first = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 2,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await?;
    assert_eq!(turn_ids(&first), vec!["turn-0", "turn-1"]);
    assert!(first.next_cursor.is_some());

    let second = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: first.next_cursor.clone(),
            page_size: 2,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await?;
    assert_eq!(turn_ids(&second), vec!["turn-2", "turn-3"]);

    let desc_first = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 2,
            sort_direction: SortDirection::Desc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await?;
    assert_eq!(turn_ids(&desc_first), vec!["turn-4", "turn-3"]);

    // Paging backwards from `second`'s backwards_cursor returns to `first`.
    let back = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: second.backwards_cursor.clone(),
            page_size: 2,
            sort_direction: SortDirection::Desc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await?;
    assert_eq!(turn_ids(&back), vec!["turn-2", "turn-1"]);

    // Item anchor: the item after "u2" within turn-2, in creation order.
    let anchored = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: Some("turn-2".to_string()),
            include_archived: false,
            position: Some(ListItemsPosition::ItemAnchor {
                item_id: "u2".to_string(),
            }),
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await?;
    assert_eq!(anchored.items.len(), 1);
    assert_eq!(anchored.items[0].item_id, "a2");

    // An anchor outside the requested turn is rejected.
    let bad_anchor = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: Some("turn-2".to_string()),
            include_archived: false,
            position: Some(ListItemsPosition::ItemAnchor {
                item_id: "u3".to_string(),
            }),
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await;
    assert!(matches!(
        bad_anchor,
        Err(ThreadStoreError::InvalidRequest { .. })
    ));

    // An item anchor without a turn id is rejected.
    let missing_turn = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: None,
            include_archived: false,
            position: Some(ListItemsPosition::ItemAnchor {
                item_id: "u2".to_string(),
            }),
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await;
    assert!(matches!(
        missing_turn,
        Err(ThreadStoreError::InvalidRequest { .. })
    ));

    let timeline = store
        .list_timeline(ListTimelineParams {
            thread_id,
            cursor: None,
            page_size: 100,
        })
        .await?;
    let positions: Vec<u64> = timeline
        .items
        .iter()
        .map(|entry| match entry {
            ThreadTimelineEntry::TurnStarted { position, .. }
            | ThreadTimelineEntry::Item { position, .. }
            | ThreadTimelineEntry::Realtime { position, .. }
            | ThreadTimelineEntry::TurnCompleted { position, .. } => *position,
        })
        .collect();
    let mut sorted_positions = positions.clone();
    sorted_positions.sort_unstable();
    assert_eq!(positions, sorted_positions, "timeline must be ascending");

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_thread_occurrences_finds_matches_with_utf16_ranges() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = ThreadId::new();
    store.create_thread(paginated_params(thread_id)).await?;

    let user_text = "please check the Widget count";
    let agent_text = "The widget count is 42.";
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1", 1_000),
                item_completed(
                    thread_id,
                    "turn-1",
                    user_message_item("u1", user_text),
                    1_000,
                    1_000,
                ),
                item_completed(
                    thread_id,
                    "turn-1",
                    agent_message_item("a1", agent_text, Some(MessagePhase::FinalAnswer)),
                    1_001,
                    1_002,
                ),
                turn_complete("turn-1", 1_000, 1_002),
            ],
        })
        .await?;

    let page = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id,
            search_term: "widget".to_string(),
            cursor: None,
            page_size: 10,
        })
        .await?;
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].item_id, "u1");
    assert_eq!(page.items[1].item_id, "a1");

    let match_start = user_text.to_lowercase().find("widget").unwrap();
    let expected_utf16_start = user_text[..match_start].encode_utf16().count() as u32;
    assert_eq!(
        page.items[0].snippet_match_range.start,
        expected_utf16_start
    );
    assert_eq!(
        page.items[0].snippet_match_range.end - page.items[0].snippet_match_range.start,
        "widget".encode_utf16().count() as u32
    );
    assert!(page.items[0].snippet.contains("Widget"));
    assert!(page.items[1].snippet.contains("widget"));

    // page_size of 1 should page across the two matching items via a cursor.
    let limited = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id,
            search_term: "widget".to_string(),
            cursor: None,
            page_size: 1,
        })
        .await?;
    assert_eq!(limited.items.len(), 1);
    assert_eq!(limited.items[0].item_id, "u1");
    let next = limited.next_cursor.clone().expect("more matches remain");
    let continued = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id,
            search_term: "widget".to_string(),
            cursor: Some(next),
            page_size: 1,
        })
        .await?;
    assert_eq!(continued.items.len(), 1);
    assert_eq!(continued.items[0].item_id, "a1");
    assert!(continued.next_cursor.is_none());

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_history_rejects_paginated_threads_use_model_context_instead() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = ThreadId::new();
    store.create_thread(paginated_params(thread_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![turn_started("turn-1", 1_000)],
        })
        .await?;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;

    let rejected = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await;
    assert!(matches!(
        rejected,
        Err(ThreadStoreError::Unsupported {
            operation: "paginated_threads"
        })
    ));

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await?;
    assert!(matches!(context.items[0], RolloutItem::SessionMeta(_)));
    assert!(
        context
            .items
            .iter()
            .any(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(_))))
    );

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_paginated_thread_returns_latest_model_context() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = ThreadId::new();
    store.create_thread(paginated_params(thread_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1", 1_000),
                item_completed(
                    thread_id,
                    "turn-1",
                    user_message_item("u1", "hello"),
                    1_000,
                    1_000,
                ),
                turn_complete("turn-1", 1_000, 1_001),
            ],
        })
        .await?;
    store.shutdown_thread(thread_id).await?;

    let resumed = store
        .resume_thread(crate::ResumeThreadParams {
            thread_id,
            rollout_path: None,
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: paginated_params(thread_id).metadata,
        })
        .await?;
    assert!(matches!(resumed[0], RolloutItem::SessionMeta(_)));
    assert!(
        resumed
            .iter()
            .any(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnComplete(_))))
    );

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_threads_still_use_full_replay_history() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = start_thread(
        &store,
        "legacy preview",
        &[user_message("hello from legacy"), agent_message("hi back")],
    )
    .await?;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;

    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await?;
    assert_eq!(history.items.len(), 3);
    assert!(matches!(history.items[0], RolloutItem::SessionMeta(_)));

    // Paginated-only reads are unsupported for a Legacy thread.
    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await;
    assert!(matches!(
        turns,
        Err(ThreadStoreError::Unsupported {
            operation: "list_turns"
        })
    ));

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forked_lineage_reads_see_ancestor_and_own_turns() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);

    // Parent: one completed turn, then persisted so its projection and items
    // are durable before the child's `history_base` is computed.
    let parent_id = ThreadId::new();
    store.create_thread(paginated_params(parent_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: parent_id,
            items: vec![
                turn_started("parent-turn", 1_000),
                item_completed(
                    parent_id,
                    "parent-turn",
                    user_message_item("p-u", "parent question"),
                    1_000,
                    1_000,
                ),
                turn_complete("parent-turn", 1_000, 1_001),
            ],
        })
        .await?;
    store
        .persist_thread(parent_id, PersistContext::Standard)
        .await?;
    let parent_record = store
        .load_record(parent_id)
        .await?
        .expect("parent is durable");
    let history_base = codex_protocol::protocol::HistoryPosition {
        thread_id: parent_id,
        end_ordinal_exclusive: parent_record.next_ordinal,
        end_byte_offset: 0,
    };

    // Child: forked from the parent's end, with its own new turn.
    let child_id = ThreadId::new();
    store
        .create_thread(CreateThreadParams {
            history_base: Some(history_base),
            forked_from_id: Some(parent_id),
            parent_thread_id: Some(parent_id),
            ..paginated_params(child_id)
        })
        .await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![
                turn_started("child-turn", 2_000),
                item_completed(
                    child_id,
                    "child-turn",
                    user_message_item("c-u", "child question"),
                    2_000,
                    2_000,
                ),
                turn_complete("child-turn", 2_000, 2_001),
            ],
        })
        .await?;
    store
        .persist_thread(child_id, PersistContext::Standard)
        .await?;

    // Listing the child's turns walks back into the parent's segment.
    let turns = store
        .list_turns(ListTurnsParams {
            thread_id: child_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await?;
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["parent-turn", "child-turn"]
    );

    // Items across both segments, in creation order.
    let items = store
        .list_items(ListItemsParams {
            thread_id: child_id,
            turn_id: None,
            include_archived: false,
            position: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await?;
    assert_eq!(
        items
            .items
            .iter()
            .map(|item| item.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["p-u", "c-u"]
    );

    // The latest model context starts with the child's own canonical
    // SessionMeta, followed by the parent's inherited suffix and the
    // child's own records, without the parent's SessionMeta line.
    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child_id,
            include_archived: false,
        })
        .await?;
    match &context.items[0] {
        RolloutItem::SessionMeta(meta) => assert_eq!(meta.meta.id, child_id),
        other => panic!("expected child session meta first, got {other:?}"),
    }
    assert_eq!(
        context
            .items
            .iter()
            .filter(|item| matches!(item, RolloutItem::SessionMeta(_)))
            .count(),
        1,
        "the parent's own session meta line must not leak into the child's context"
    );
    assert!(context.items.iter().any(|item| matches!(
        item,
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) if event.turn_id == "parent-turn"
    )));
    assert!(context.items.iter().any(|item| matches!(
        item,
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) if event.turn_id == "child-turn"
    )));

    // Occurrence search on the child also walks into the parent's segment,
    // with a turn cursor that resolves back through `list_turns`.
    let occurrences = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id: child_id,
            search_term: "question".to_string(),
            cursor: None,
            page_size: 10,
        })
        .await?;
    assert_eq!(
        occurrences
            .items
            .iter()
            .map(|item| item.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["p-u", "c-u"]
    );

    store.shutdown_thread(parent_id).await?;
    store.shutdown_thread(child_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_thread_removes_paginated_projection_rows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = ThreadId::new();
    store.create_thread(paginated_params(thread_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1", 1_000),
                item_completed(
                    thread_id,
                    "turn-1",
                    user_message_item("u1", "hello"),
                    1_000,
                    1_000,
                ),
                turn_complete("turn-1", 1_000, 1_001),
            ],
        })
        .await?;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    store.shutdown_thread(thread_id).await?;

    store
        .delete_thread(DeleteThreadParams { thread_id })
        .await?;

    for prefix in [
        super::keys::turn_id_prefix(thread_id),
        super::keys::turn_start_prefix(thread_id),
        super::keys::turn_end_prefix(thread_id),
        super::keys::item_id_prefix(thread_id),
        super::keys::item_created_prefix(thread_id),
        super::keys::item_updated_prefix(thread_id),
        super::keys::realtime_prefix(thread_id),
    ] {
        let remaining = store
            .antfly()
            .scan(codex_antfly::ScanRequest::prefix(&prefix))
            .await?;
        assert!(
            remaining.is_empty(),
            "leftover projection rows under {prefix}"
        );
    }

    settle(store, dir).await;
    Ok(())
}
