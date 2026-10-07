//! End-to-end checks of [`AntflyThreadStore`] against an embedded `libantfly`.
//! Calls into `libantfly` happen on the store's own large-stack executor.

use std::sync::Arc;
use std::time::Duration;

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;

use super::AntflyThreadStore;
use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ListThreadsParams;
use crate::LoadThreadHistoryParams;
use crate::MoveThreadToSectionParams;
use crate::PersistContext;
use crate::ReadThreadParams;
use crate::SearchThreadsParams;
use crate::SortDirection;
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
