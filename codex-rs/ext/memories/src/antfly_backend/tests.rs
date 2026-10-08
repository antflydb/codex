//! End-to-end checks of [`AntflyMemoriesBackend`] against an embedded
//! `libantfly`. `add_ad_hoc_note`/`list`/`read` are checked against plain
//! filesystem expectations (the local backend's contract); `search` is
//! checked against Antfly hybrid search over indexed notes.

use std::sync::Arc;
use std::time::Duration;

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use pretty_assertions::assert_eq;

use super::AntflyMemoriesBackend;
use crate::backend::AddAdHocMemoryNoteRequest;
use crate::backend::ListMemoriesRequest;
use crate::backend::MemoriesBackend;
use crate::backend::MemoriesBackendError;
use crate::backend::ReadMemoryRequest;
use crate::backend::SearchMatchMode;
use crate::backend::SearchMemoriesRequest;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn backend(dir: &tempfile::TempDir) -> (AntflyMemoriesBackend, Arc<Antfly>) {
    let mut config = AntflyConfig::embedded(dir.path().join("codex.aflite"));
    config.embedder = None;
    let antfly = Arc::new(Antfly::new(config));
    let root = dir.path().join("memories");
    (
        AntflyMemoriesBackend::new(root, Arc::clone(&antfly), "memories"),
        antfly,
    )
}

fn search_request(queries: &[&str]) -> SearchMemoriesRequest {
    SearchMemoriesRequest {
        queries: queries.iter().map(ToString::to_string).collect(),
        match_mode: SearchMatchMode::Any,
        path: None,
        cursor: None,
        context_lines: 0,
        case_sensitive: false,
        normalized: false,
        max_results: 50,
    }
}

/// Antfly's full-text index is populated by a background worker, so a
/// search run immediately after a write can race it. Polls instead of
/// asserting once, matching how a real caller would retry a search.
async fn search_eventually(
    backend: &AntflyMemoriesBackend,
    request: SearchMemoriesRequest,
    min_matches: usize,
) -> Result<crate::backend::SearchMemoriesResponse, MemoriesBackendError> {
    let mut last = backend.search(request.clone()).await?;
    for _ in 0..40 {
        if last.matches.len() >= min_matches {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        last = backend.search(request.clone()).await?;
    }
    Ok(last)
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
async fn add_ad_hoc_note_writes_the_file_and_becomes_searchable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (backend, antfly) = backend(&dir);

    let filename = "2025-01-02T03-04-05-space-whales.md";
    backend
        .add_ad_hoc_note(AddAdHocMemoryNoteRequest {
            filename: filename.to_string(),
            note: "Space whales migrate through the asteroid belt in spring.".to_string(),
        })
        .await?;

    // The local semantics still apply: the file exists on disk at the
    // documented ad-hoc notes path.
    let on_disk = tokio::fs::read_to_string(
        dir.path()
            .join("memories/extensions/ad_hoc/notes")
            .join(filename),
    )
    .await?;
    assert_eq!(
        on_disk,
        "Space whales migrate through the asteroid belt in spring."
    );

    let response = search_eventually(&backend, search_request(&["whales"]), 1).await?;
    assert_eq!(response.matches.len(), 1);
    assert!(response.matches[0].path.ends_with(filename));
    assert!(response.matches[0].content.contains("Space whales"));

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reading_a_pipeline_written_file_indexes_it_for_later_search() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (backend, antfly) = backend(&dir);

    // Simulate the memories write pipeline writing a file directly to disk,
    // bypassing this backend entirely.
    let summaries_dir = dir.path().join("memories/rollout_summaries");
    tokio::fs::create_dir_all(&summaries_dir).await?;
    tokio::fs::write(
        summaries_dir.join("thread-1.md"),
        "The user prefers tabs over spaces in generated code.",
    )
    .await?;

    // Not indexed yet: search finds nothing.
    let before = backend.search(search_request(&["tabs"])).await?;
    assert!(before.matches.is_empty());

    // Reading the file (as the memory `read` tool would) indexes it.
    let read = backend
        .read(ReadMemoryRequest {
            path: "rollout_summaries/thread-1.md".to_string(),
            line_offset: 1,
            max_lines: None,
            max_tokens: 0,
        })
        .await?;
    assert_eq!(
        read.content,
        "The user prefers tabs over spaces in generated code."
    );

    let after = search_eventually(&backend, search_request(&["tabs"]), 1).await?;
    assert_eq!(after.matches.len(), 1);
    assert_eq!(after.matches[0].path, "rollout_summaries/thread-1.md");

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_keeps_local_filesystem_semantics() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (backend, antfly) = backend(&dir);
    tokio::fs::create_dir_all(dir.path().join("memories")).await?;
    tokio::fs::write(dir.path().join("memories/notes.md"), "hello").await?;
    tokio::fs::write(dir.path().join("memories/.hidden.md"), "secret").await?;

    let listing = backend
        .list(ListMemoriesRequest {
            path: None,
            cursor: None,
            max_results: 10,
        })
        .await?;
    let paths: Vec<&str> = listing
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(paths, vec!["notes.md"]);

    let missing = backend
        .list(ListMemoriesRequest {
            path: Some("does-not-exist".to_string()),
            cursor: None,
            max_results: 10,
        })
        .await;
    assert!(matches!(
        missing,
        Err(MemoriesBackendError::NotFound { .. })
    ));

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_validates_input_before_touching_antfly() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (backend, antfly) = backend(&dir);
    tokio::fs::create_dir_all(dir.path().join("memories")).await?;

    let empty_query = backend
        .search(SearchMemoriesRequest {
            queries: vec![" ".to_string()],
            match_mode: SearchMatchMode::Any,
            path: None,
            cursor: None,
            context_lines: 0,
            case_sensitive: false,
            normalized: false,
            max_results: 10,
        })
        .await;
    assert!(matches!(empty_query, Err(MemoriesBackendError::EmptyQuery)));

    let bad_window = backend
        .search(SearchMemoriesRequest {
            queries: vec!["anything".to_string()],
            match_mode: SearchMatchMode::AllWithinLines { line_count: 0 },
            path: None,
            cursor: None,
            context_lines: 0,
            case_sensitive: false,
            normalized: false,
            max_results: 10,
        })
        .await;
    assert!(matches!(
        bad_window,
        Err(MemoriesBackendError::InvalidMatchWindow)
    ));

    let bad_path = backend
        .search(SearchMemoriesRequest {
            queries: vec!["anything".to_string()],
            match_mode: SearchMatchMode::Any,
            path: Some("nope".to_string()),
            cursor: None,
            context_lines: 0,
            case_sensitive: false,
            normalized: false,
            max_results: 10,
        })
        .await;
    assert!(matches!(
        bad_path,
        Err(MemoriesBackendError::NotFound { .. })
    ));

    settle(antfly, dir).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_respects_match_mode_and_context_lines() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (backend, antfly) = backend(&dir);

    backend
        .add_ad_hoc_note(AddAdHocMemoryNoteRequest {
            filename: "2025-01-02T03-04-05-recipe.md".to_string(),
            note: "line one\nflour and sugar\nmore context\nbutter and sugar\nline five"
                .to_string(),
        })
        .await?;

    let any_mode = search_eventually(&backend, search_request(&["flour", "butter"]), 2).await?;
    assert_eq!(any_mode.matches.len(), 2);

    let same_line = search_eventually(
        &backend,
        SearchMemoriesRequest {
            match_mode: SearchMatchMode::AllOnSameLine,
            context_lines: 1,
            ..search_request(&["flour", "sugar"])
        },
        1,
    )
    .await?;
    assert_eq!(same_line.matches.len(), 1);
    assert_eq!(same_line.matches[0].match_line_number, 2);
    assert!(same_line.matches[0].content.contains("line one"));
    assert!(same_line.matches[0].content.contains("more context"));

    settle(antfly, dir).await;
    Ok(())
}
