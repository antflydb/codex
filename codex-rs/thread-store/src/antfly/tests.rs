//! End-to-end checks of [`AntflyThreadStore`] against an embedded `libantfly`.
//! Calls into `libantfly` happen on the store's own large-stack executor.

use std::sync::Arc;
use std::time::Duration;

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ModeKind;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;

use super::AntflyThreadStore;
use crate::AddThreadAttachmentOutcome;
use crate::AddThreadAttachmentParams;
use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateProjectParams;
use crate::CreateThreadParams;
use crate::CreateThreadSectionParams;
use crate::DeleteThreadParams;
use crate::DeleteThreadSectionParams;
use crate::ForkBoundary;
use crate::ListProjectsParams;
use crate::ListThreadAttachmentThreadsParams;
use crate::ListThreadAttachmentsParams;
use crate::ListThreadSectionsParams;
use crate::ListThreadsParams;
use crate::LoadThreadHistoryParams;
use crate::MoveProjectParams;
use crate::MoveThreadToSectionParams;
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::ProjectMoveOutcome;
use crate::ProjectSortKey;
use crate::ReadThreadParams;
use crate::RemoveThreadAttachmentOutcome;
use crate::RemoveThreadAttachmentParams;
use crate::RenameThreadSectionParams;
use crate::RevertThreadParams;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::StoredProjectRoot;
use crate::ThreadAttachmentArchiveFilter;
use crate::ThreadMetadataPatch;
use crate::ThreadPersistenceMetadata;
use crate::ThreadSortKey;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::UpdateProjectParams;
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

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_attribution: None,
        turn_id: turn_id.to_string(),
        root_turn_id: None,
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: ModeKind::default(),
    }))
}

fn turn_complete(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        root_turn_id: None,
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
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
async fn thread_sections_seed_pinned_and_support_crud() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    assert!(store.supports_thread_sections());

    let page = store
        .list_thread_sections(ListThreadSectionsParams {
            cursor: None,
            limit: 10,
        })
        .await?;
    assert!(page.sections.iter().any(|section| {
        section.id == codex_state::PINNED_THREAD_SECTION_ID
            && section.name == codex_state::PINNED_THREAD_SECTION_NAME
    }));

    let created = store
        .create_thread_section(CreateThreadSectionParams {
            name: "Work".to_string(),
            appearance: None,
        })
        .await?;
    assert_eq!(created.name, "Work");

    let renamed = store
        .rename_thread_section(RenameThreadSectionParams {
            section_id: created.id.clone(),
            name: "Work Stuff".to_string(),
            appearance: None,
        })
        .await?
        .expect("section exists");
    assert_eq!(renamed.name, "Work Stuff");

    assert!(matches!(
        store
            .rename_thread_section(RenameThreadSectionParams {
                section_id: codex_state::PINNED_THREAD_SECTION_ID.to_string(),
                name: "x".to_string(),
                appearance: None,
            })
            .await,
        Err(ThreadStoreError::Internal { .. })
    ));
    assert!(matches!(
        store
            .delete_thread_section(DeleteThreadSectionParams {
                section_id: codex_state::PINNED_THREAD_SECTION_ID.to_string(),
            })
            .await,
        Err(ThreadStoreError::Internal { .. })
    ));
    assert_eq!(
        store
            .rename_thread_section(RenameThreadSectionParams {
                section_id: "missing".to_string(),
                name: "x".to_string(),
                appearance: None,
            })
            .await?,
        None
    );

    let thread_id = start_thread(&store, "sectioned", &[user_message("hi")]).await?;
    store
        .move_thread_to_section(MoveThreadToSectionParams {
            thread_id,
            section: Some(created.id.clone()),
            before_thread_id: None,
        })
        .await?;
    let read = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?;
    let section = read.section.expect("section set");
    assert_eq!(section.id, created.id);
    assert_eq!(section.name, "Work Stuff");

    let deleted = store
        .delete_thread_section(DeleteThreadSectionParams {
            section_id: created.id.clone(),
        })
        .await?;
    assert!(deleted);
    let read_after = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?;
    assert!(read_after.section.is_none());

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_attachments_add_list_remove_copy() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    assert!(store.supports_thread_attachments());

    let thread_a = start_thread(&store, "a", &[user_message("hi")]).await?;
    let thread_b = start_thread(&store, "b", &[user_message("hi")]).await?;

    assert!(matches!(
        store
            .add_thread_attachment(AddThreadAttachmentParams {
                thread_id: thread_a,
                attachment_type: String::new(),
                identity_key: "k".to_string(),
                payload: serde_json::json!({}),
            })
            .await,
        Err(ThreadStoreError::InvalidRequest { .. })
    ));

    let missing_thread = ThreadId::new();
    assert!(matches!(
        store
            .add_thread_attachment(AddThreadAttachmentParams {
                thread_id: missing_thread,
                attachment_type: "ref".to_string(),
                identity_key: "doc".to_string(),
                payload: serde_json::json!({}),
            })
            .await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));

    let outcome = store
        .add_thread_attachment(AddThreadAttachmentParams {
            thread_id: thread_a,
            attachment_type: "ref".to_string(),
            identity_key: "doc-1".to_string(),
            payload: serde_json::json!({"n": 1}),
        })
        .await?;
    let first = match outcome {
        AddThreadAttachmentOutcome::Created(attachment) => attachment,
        AddThreadAttachmentOutcome::Existing(_) => panic!("expected Created"),
    };

    let repeat = store
        .add_thread_attachment(AddThreadAttachmentParams {
            thread_id: thread_a,
            attachment_type: "ref".to_string(),
            identity_key: "doc-1".to_string(),
            payload: serde_json::json!({"n": 2}),
        })
        .await?;
    match repeat {
        AddThreadAttachmentOutcome::Existing(attachment) => {
            assert_eq!(attachment.id, first.id);
            assert_eq!(attachment.payload, serde_json::json!({"n": 1}));
        }
        AddThreadAttachmentOutcome::Created(_) => panic!("expected Existing"),
    }

    let page = store
        .list_thread_attachments(ListThreadAttachmentsParams {
            thread_id: thread_a,
            cursor: None,
            limit: 10,
        })
        .await?;
    assert_eq!(page.attachments.len(), 1);
    assert!(page.next_cursor.is_none());

    let owners = store
        .list_thread_attachment_threads(ListThreadAttachmentThreadsParams {
            attachment_type: "ref".to_string(),
            identity_key: "doc-1".to_string(),
            archive_filter: ThreadAttachmentArchiveFilter::All,
            cursor: None,
            limit: 10,
        })
        .await?;
    assert_eq!(owners.threads.len(), 1);
    assert_eq!(owners.threads[0].thread_id, thread_a);
    assert!(!owners.threads[0].archived);

    store
        .add_thread_attachment(AddThreadAttachmentParams {
            thread_id: thread_a,
            attachment_type: "ref".to_string(),
            identity_key: "doc-2".to_string(),
            payload: serde_json::json!({"n": 3}),
        })
        .await?;
    store.copy_thread_attachments(thread_a, thread_b).await?;
    let copied = store
        .list_thread_attachments(ListThreadAttachmentsParams {
            thread_id: thread_b,
            cursor: None,
            limit: 10,
        })
        .await?;
    assert_eq!(copied.attachments.len(), 2);
    assert!(copied.attachments.iter().all(|a| a.id != first.id));

    let removed = store
        .remove_thread_attachment(RemoveThreadAttachmentParams {
            thread_id: thread_a,
            attachment_type: "ref".to_string(),
            identity_key: "doc-1".to_string(),
        })
        .await?;
    assert!(matches!(removed, RemoveThreadAttachmentOutcome::Removed(_)));
    let removed_again = store
        .remove_thread_attachment(RemoveThreadAttachmentParams {
            thread_id: thread_a,
            attachment_type: "ref".to_string(),
            identity_key: "doc-1".to_string(),
        })
        .await?;
    assert!(matches!(
        removed_again,
        RemoveThreadAttachmentOutcome::NotFound
    ));

    store.shutdown_thread(thread_a).await?;
    store.shutdown_thread(thread_b).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_thread_removes_attachments() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    let thread_id = start_thread(&store, "a", &[user_message("hi")]).await?;
    store
        .add_thread_attachment(AddThreadAttachmentParams {
            thread_id,
            attachment_type: "ref".to_string(),
            identity_key: "doc".to_string(),
            payload: serde_json::json!({}),
        })
        .await?;
    store.shutdown_thread(thread_id).await?;
    store
        .delete_thread(DeleteThreadParams { thread_id })
        .await?;
    let page = store
        .list_thread_attachments(ListThreadAttachmentsParams {
            thread_id,
            cursor: None,
            limit: 10,
        })
        .await?;
    assert!(page.attachments.is_empty());
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projects_crud_round_trips() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);
    assert!(store.supports_projects());

    let thread_id = start_thread(&store, "proj-thread", &[user_message("hi")]).await?;

    let unknown_thread = ThreadId::new();
    assert!(matches!(
        store
            .create_project(CreateProjectParams {
                name: "Bad".to_string(),
                roots: Vec::new(),
                metadata: Default::default(),
                thread_ids: vec![unknown_thread.to_string()],
                idempotency_key: "bad-key".to_string(),
            })
            .await,
        Err(ThreadStoreError::Internal { .. })
    ));

    let created = store
        .create_project(CreateProjectParams {
            name: "Antfly".to_string(),
            roots: vec![StoredProjectRoot {
                path: "/repo".to_string(),
            }],
            metadata: Default::default(),
            thread_ids: vec![thread_id.to_string()],
            idempotency_key: "key-1".to_string(),
        })
        .await?;
    assert!(created.created);
    assert_eq!(created.project.position, 0);

    let repeat = store
        .create_project(CreateProjectParams {
            name: "Ignored".to_string(),
            roots: Vec::new(),
            metadata: Default::default(),
            thread_ids: Vec::new(),
            idempotency_key: "key-1".to_string(),
        })
        .await?;
    assert!(!repeat.created);
    assert_eq!(repeat.project.id, created.project.id);

    let read = store
        .read_project(created.project.id.clone())
        .await?
        .expect("project exists");
    assert_eq!(read.name, "Antfly");

    let thread_after = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?;
    assert_eq!(thread_after.project_id, Some(created.project.id.clone()));

    let listed = store
        .list_projects(ListProjectsParams {
            cursor: None,
            limit: 10,
            sort_key: ProjectSortKey::Position,
            sort_direction: SortDirection::Asc,
        })
        .await?;
    assert_eq!(listed.projects.len(), 1);

    let updated = store
        .update_project(UpdateProjectParams {
            project_id: created.project.id.clone(),
            name: Some("Antfly Renamed".to_string()),
            roots: None,
            metadata: None,
        })
        .await?
        .expect("project exists");
    assert!(updated.changed);
    assert_eq!(updated.project.name, "Antfly Renamed");

    let no_op = store
        .update_project(UpdateProjectParams {
            project_id: created.project.id.clone(),
            name: Some("Antfly Renamed".to_string()),
            roots: None,
            metadata: None,
        })
        .await?
        .expect("project exists");
    assert!(!no_op.changed);

    let second = store
        .create_project(CreateProjectParams {
            name: "Second".to_string(),
            roots: Vec::new(),
            metadata: Default::default(),
            thread_ids: Vec::new(),
            idempotency_key: "key-2".to_string(),
        })
        .await?;

    let moved = store
        .move_project(MoveProjectParams {
            project_id: second.project.id.clone(),
            before_project_id: Some(created.project.id.clone()),
        })
        .await?
        .expect("project exists");
    assert_eq!(moved, ProjectMoveOutcome::Moved);

    let deleted = store
        .delete_project(created.project.id.clone())
        .await?
        .expect("project exists");
    assert_eq!(
        deleted.affected_active_thread_ids,
        vec![thread_id.to_string()]
    );
    assert!(deleted.affected_archived_thread_ids.is_empty());

    let thread_final = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?;
    assert_eq!(thread_final.project_id, None);

    store.shutdown_thread(thread_id).await?;
    settle(store, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_fork_and_revert_thread_legacy() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = store(&dir);

    let thread_id = ThreadId::new();
    store.create_thread(create_params(thread_id)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                turn_started("turn-1"),
                user_message("first question"),
                agent_message("first answer"),
                turn_complete("turn-1"),
                turn_started("turn-2"),
                user_message("second question"),
                agent_message("second answer"),
                turn_complete("turn-2"),
            ],
        })
        .await?;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;

    let latest = store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::Latest,
        })
        .await?;
    assert_eq!(latest.model_context.len(), 9);
    assert!(latest.history_base.is_none());

    let before_turn_2 = store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::BeforeTurn("turn-2".to_string()),
        })
        .await?;
    assert_eq!(before_turn_2.model_context.len(), 5);

    let through_turn_1 = store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::ThroughTurn("turn-1".to_string()),
        })
        .await?;
    assert_eq!(through_turn_1.model_context.len(), 5);

    assert!(matches!(
        store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::BeforeTurn("missing-turn".to_string()),
            })
            .await,
        Err(ThreadStoreError::InvalidRequest { .. })
    ));

    // revert_thread requires the live writer closed first.
    assert!(matches!(
        store
            .revert_thread(RevertThreadParams {
                thread_id,
                before_turn_id: "turn-2".to_string(),
                multi_agent_version: None,
            })
            .await,
        Err(ThreadStoreError::InvalidRequest { .. })
    ));
    store.shutdown_thread(thread_id).await?;

    store
        .revert_thread(RevertThreadParams {
            thread_id,
            before_turn_id: "turn-2".to_string(),
            multi_agent_version: None,
        })
        .await?;

    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await?;
    assert_eq!(history.items.len(), 5);

    // An in-progress turn cannot be the inclusive end of a `ThroughTurn` fork.
    let unfinished = ThreadId::new();
    store.create_thread(create_params(unfinished)).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: unfinished,
            items: vec![turn_started("turn-x"), user_message("q")],
        })
        .await?;
    store
        .persist_thread(unfinished, PersistContext::Standard)
        .await?;
    assert!(matches!(
        store
            .prepare_fork(PrepareForkParams {
                thread_id: unfinished,
                boundary: ForkBoundary::ThroughTurn("turn-x".to_string()),
            })
            .await,
        Err(ThreadStoreError::InvalidRequest { .. })
    ));
    store.shutdown_thread(unfinished).await?;

    // Paginated threads are out of scope for this store's fork/revert support.
    let mut paginated_params = create_params(ThreadId::new());
    paginated_params.history_mode = ThreadHistoryMode::Paginated;
    let paginated_id = paginated_params.thread_id;
    store.create_thread(paginated_params).await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: paginated_id,
            items: vec![user_message("hi")],
        })
        .await?;
    store
        .persist_thread(paginated_id, PersistContext::Standard)
        .await?;
    assert!(matches!(
        store
            .prepare_fork(PrepareForkParams {
                thread_id: paginated_id,
                boundary: ForkBoundary::Latest,
            })
            .await,
        Err(ThreadStoreError::Unsupported { .. })
    ));
    store.shutdown_thread(paginated_id).await?;
    assert!(matches!(
        store
            .revert_thread(RevertThreadParams {
                thread_id: paginated_id,
                before_turn_id: "x".to_string(),
                multi_agent_version: None,
            })
            .await,
        Err(ThreadStoreError::Unsupported { .. })
    ));

    settle(store, dir).await;
    Ok(())
}
