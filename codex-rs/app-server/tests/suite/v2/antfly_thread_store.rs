//! Regression coverage for app-server thread operations backed by the
//! Antfly thread store (`experimental_thread_store = { type = "antfly" }`).
//!
//! The important failure mode is accidentally creating local SQLite or
//! rollout JSONL persistence while the Antfly store is configured: Codex
//! must create no SQLite files (and no `sessions`/`archived_sessions`
//! rollout directories) under `codex_home` when this backend is selected.
//! An embedded Antfly `.aflite` file is expected and is not a failure.

use std::future::Future;
use std::path::Path;

use anyhow::Result;
use anyhow::anyhow;
use app_test_support::MockResponsesConfig;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server::in_process;
use codex_app_server::in_process::InProcessClientHandle;
use codex_app_server::in_process::InProcessServerEvent;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadDeleteParams;
use codex_app_server_protocol::ThreadDeleteResponse;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput as V2UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_config::NoopThreadConfigLoader;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Runs `test` on a thread/runtime with a bigger-than-default stack.
///
/// Session construction's call chain (through embedded Antfly's inference
/// resource-policy setup) is deep enough to overflow Tokio's default 2 MiB
/// worker stack under `#[tokio::test(flavor = "multi_thread")]`, the same
/// class of issue `core/tests/common/test_codex.rs` and
/// `tui/src/app/tests.rs` already work around for other deep call chains in
/// this codebase. Production binaries never hit this: `arg0` always builds
/// its runtime with a bumped `thread_stack_size` (see
/// `codex_async_utils::THREAD_STACK_SIZE_BYTES`); only this crate's default
/// `#[tokio::test]` runtime does not.
fn run_test_with_large_stack<F, Fut>(name: &str, test: F) -> Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    const WORKER_THREADS: usize = 2;
    const TEST_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(WORKER_THREADS)
                .thread_stack_size(TEST_STACK_SIZE_BYTES)
                .enable_all()
                .build()?;
            runtime.block_on(Box::pin(test()))
        })?;

    handle
        .join()
        .map_err(|_| anyhow!("{name} thread panicked"))?
}

#[test]
fn antfly_thread_store_creates_no_sqlite_files() -> Result<()> {
    run_test_with_large_stack(
        "antfly-thread-store-test",
        antfly_thread_store_creates_no_sqlite_files_inner,
    )
}

async fn antfly_thread_store_creates_no_sqlite_files_inner() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_antfly_config_toml(codex_home.path(), &server.uri())?;

    let mut client = start_in_process_server(codex_home.path()).await?;

    let response = client
        .request(ClientRequest::ThreadStart {
            request_id: RequestId::Integer(1),
            params: ThreadStartParams::default(),
        })
        .await?
        .expect("thread/start should succeed");
    let ThreadStartResponse { thread, .. } =
        serde_json::from_value(response).expect("thread/start response should parse");

    client
        .request(ClientRequest::TurnStart {
            request_id: RequestId::Integer(2),
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                client_user_message_id: None,
                input: vec![V2UserInput::Text {
                    text: "Hello".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?
        .expect("turn/start should succeed");

    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let Some(event) = client.next_event().await else {
                anyhow::bail!("in-process app-server stopped before turn/completed");
            };
            if let InProcessServerEvent::ServerNotification(notification) = event
                && let ServerNotification::TurnCompleted(completed) = notification.as_ref()
                && completed.thread_id == thread.id
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    let response = client
        .request(ClientRequest::ThreadList {
            request_id: RequestId::Integer(3),
            params: ThreadListParams {
                excluded_thread_ids: None,
                originators: None,
                cursor: None,
                limit: Some(10),
                sort_key: None,
                sort_direction: None,
                model_providers: Some(Vec::new()),
                source_kinds: None,
                archived: None,
                section_id: None,
                project_id: None,
                cwd: None,
                use_state_db_only: false,
                search_term: None,
                parent_thread_id: None,
                ancestor_thread_id: None,
            },
        })
        .await?
        .expect("thread/list should succeed");
    let ThreadListResponse { data, .. } =
        serde_json::from_value(response).expect("thread/list response should parse");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].id, thread.id);

    let response = client
        .request(ClientRequest::ThreadDelete {
            request_id: RequestId::Integer(4),
            params: ThreadDeleteParams {
                thread_id: thread.id.clone(),
            },
        })
        .await?
        .expect("thread/delete should succeed");
    let _: ThreadDeleteResponse = serde_json::from_value(response)?;

    client.shutdown().await?;
    // The embedded Antfly backend closes its database on a background
    // thread; give it a moment before the tempdir drops out from under it.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    assert_no_sqlite_artifacts(codex_home.path())?;

    Ok(())
}

fn assert_no_sqlite_artifacts(codex_home: &Path) -> Result<()> {
    assert!(
        !codex_home.join("sessions").exists(),
        "the Antfly thread store should not create local rollout sessions"
    );
    assert!(
        !codex_home.join("archived_sessions").exists(),
        "the Antfly thread store should not create archived rollout sessions"
    );

    let sqlite_artifacts = std::fs::read_dir(codex_home)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.ends_with(".sqlite")
                        || name.ends_with(".sqlite-shm")
                        || name.ends_with(".sqlite-wal")
                })
        })
        .collect::<Vec<_>>();
    assert!(
        sqlite_artifacts.is_empty(),
        "the Antfly thread store should not create sqlite artifacts: {sqlite_artifacts:?}"
    );

    Ok(())
}

async fn start_in_process_server(codex_home: &Path) -> Result<InProcessClientHandle> {
    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(codex_home.to_path_buf())
            .fallback_cwd(Some(codex_home.to_path_buf()))
            .loader_overrides(loader_overrides.clone())
            .build()
            .await?,
    );

    Ok(start_in_process_client(config, loader_overrides).await?)
}

async fn start_in_process_client(
    config: Arc<Config>,
    loader_overrides: LoaderOverrides,
) -> std::io::Result<InProcessClientHandle> {
    in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        embedded_network_policy: Default::default(),
        thread_config_loader: Arc::new(NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        config_warnings: Vec::new(),
        session_source: SessionSource::Cli,
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: "codex-app-server-tests".to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: None,
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await
}

fn create_antfly_config_toml(codex_home: &Path, server_uri: &str) -> std::io::Result<()> {
    // No embedder: full-text search alone is enough to exercise the backend
    // without a model download, and no remote `url` selects the embedded
    // `.aflite` backend under `codex_home`.
    MockResponsesConfig::new(server_uri)
        .with_root_config(
            "experimental_thread_store = { type = \"antfly\", semantic_search = false }",
        )
        .write(codex_home)
}
