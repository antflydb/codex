//! Hybrid search over notes indexed in Antfly, with local-backend line
//! matching applied to the resulting candidate files.

use std::collections::HashSet;

use serde_json::Value;

use codex_antfly::schema;

use super::AntflyMemoriesBackend;
use crate::MAX_SEARCH_RESULTS;
use crate::backend::ListMemoriesRequest;
use crate::backend::MemoriesBackend;
use crate::backend::MemoriesBackendError;
use crate::backend::SearchMatchMode;
use crate::backend::SearchMemoriesRequest;
use crate::backend::SearchMemoriesResponse;
use crate::local::SearchMatcher;
use crate::local::search_file;

/// Hybrid-search candidate files considered before line matching narrows them down.
const FULL_TEXT_CANDIDATE_LIMIT: usize = 200;
const SEMANTIC_CANDIDATE_LIMIT: usize = 50;

pub(super) async fn search(
    backend: &AntflyMemoriesBackend,
    request: SearchMemoriesRequest,
) -> Result<SearchMemoriesResponse, MemoriesBackendError> {
    let queries = request
        .queries
        .iter()
        .map(|query| query.trim().to_string())
        .collect::<Vec<_>>();
    if queries.is_empty() || queries.iter().any(String::is_empty) {
        return Err(MemoriesBackendError::EmptyQuery);
    }
    if matches!(
        request.match_mode,
        SearchMatchMode::AllWithinLines { line_count: 0 }
    ) {
        return Err(MemoriesBackendError::InvalidMatchWindow);
    }

    let max_results = request.max_results.min(MAX_SEARCH_RESULTS);
    let start_index = match request.cursor.as_deref() {
        Some(cursor) => cursor.parse::<usize>().map_err(|_| {
            MemoriesBackendError::invalid_cursor(cursor, "must be a non-negative integer")
        })?,
        None => 0,
    };
    if let Some(path) = &request.path {
        // Reuses the local backend's path validation (existence, symlink and
        // hidden-component rules) instead of duplicating it here.
        backend
            .local
            .list(ListMemoriesRequest {
                path: Some(path.clone()),
                cursor: None,
                max_results: 1,
            })
            .await?;
    }

    let matcher = SearchMatcher::new(
        queries.clone(),
        request.match_mode.clone(),
        request.case_sensitive,
        request.normalized,
    )?;

    let filter = backend.namespace_filter(request.path.as_deref());
    let search_term = queries.join(" ");
    let hits = backend
        .antfly
        .documents(schema::MEMORY_NOTES)
        .search_text(
            &search_term,
            Some(filter),
            FULL_TEXT_CANDIDATE_LIMIT,
            SEMANTIC_CANDIDATE_LIMIT,
        )
        .await
        .map_err(|err| MemoriesBackendError::Io(std::io::Error::other(err.to_string())))?;

    let mut matches = Vec::new();
    let mut seen = HashSet::new();
    for hit in hits {
        let Some(doc) = hit.doc else { continue };
        let Some(path) = doc.get("path").and_then(Value::as_str) else {
            continue;
        };
        if !seen.insert(path.to_string()) {
            continue;
        }
        let absolute = backend.root.join(path);
        match search_file(
            &backend.root,
            &absolute,
            &matcher,
            request.context_lines,
            &mut matches,
        )
        .await
        {
            Ok(()) => {}
            // The index can lag behind the filesystem (a note was moved or
            // removed after indexing); skip it rather than failing the search.
            Err(MemoriesBackendError::Io(_)) => continue,
            Err(err) => return Err(err),
        }
    }

    if start_index > matches.len() {
        return Err(MemoriesBackendError::invalid_cursor(
            start_index.to_string(),
            "exceeds result count",
        ));
    }
    let end_index = start_index.saturating_add(max_results).min(matches.len());
    let next_cursor = (end_index < matches.len()).then(|| end_index.to_string());
    let truncated = next_cursor.is_some();
    Ok(SearchMemoriesResponse {
        queries,
        match_mode: request.match_mode,
        path: request.path,
        matches: matches.drain(start_index..end_index).collect(),
        next_cursor,
        truncated,
    })
}
