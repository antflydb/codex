//! Thread search over persisted message text.
//!
//! `search_threads` takes every full-text match plus the closest semantic
//! neighbors, then orders and pages the matching threads like `list_threads`.
//! Occurrence search stays a literal match, as its contract requires.

use std::collections::HashMap;

use codex_antfly::schema;
use codex_protocol::ThreadId;
use serde_json::Value;

use super::AntflyThreadStore;
use super::internal;
use super::listing::ListCursor;
use super::record::ThreadRecord;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::StoredThreadSearchResult;
use crate::ThreadSearchPage;
use crate::ThreadSortKey;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Full-text hits considered per query.
const FULL_TEXT_LIMIT: usize = 1_000;
/// Semantic neighbors added beyond exact matches.
const SEMANTIC_LIMIT: usize = 20;
const CONTEXT_BEFORE_CHARS: usize = 48;
const CONTEXT_AFTER_CHARS: usize = 96;

/// Byte range of the first case-insensitive literal match of `needle`.
fn find_case_insensitive(haystack: &str, needle: &str) -> Option<(usize, usize)> {
    let needle = needle.to_lowercase();
    if needle.is_empty() {
        return None;
    }
    // Lowercasing can change byte lengths, so compare char by char.
    let chars: Vec<(usize, char)> = haystack.char_indices().collect();
    let needle_chars: Vec<char> = needle.chars().collect();
    'start: for start in 0..chars.len() {
        let mut matched = 0;
        let mut index = start;
        while matched < needle_chars.len() {
            let Some((_, ch)) = chars.get(index) else {
                continue 'start;
            };
            for lower in ch.to_lowercase() {
                if needle_chars.get(matched) != Some(&lower) {
                    continue 'start;
                }
                matched += 1;
            }
            index += 1;
        }
        let begin = chars[start].0;
        let end = chars
            .get(index)
            .map_or(haystack.len(), |(offset, _)| *offset);
        return Some((begin, end));
    }
    None
}

/// Excerpt around the first match of `term`, using the local store's
/// 48/96-character context and ellipses. Without a literal match (a semantic
/// neighbor) the excerpt is the start of the text.
pub(crate) fn excerpt(text: &str, term: &str) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let (start, end) = find_case_insensitive(&text, term).unwrap_or((0, 0));
    let before: Vec<(usize, char)> = text[..start].char_indices().collect();
    let snippet_start = if before.len() > CONTEXT_BEFORE_CHARS {
        before[before.len() - CONTEXT_BEFORE_CHARS].0
    } else {
        0
    };
    let snippet_end = text[end..]
        .char_indices()
        .nth(CONTEXT_AFTER_CHARS)
        .map_or(text.len(), |(offset, _)| end + offset);
    let mut snippet = text[snippet_start..snippet_end].trim().to_string();
    if snippet_start > 0 {
        snippet = format!("... {snippet}");
    }
    if snippet_end < text.len() {
        snippet.push_str(" ...");
    }
    snippet
}

fn sort_value(record: &ThreadRecord, sort_key: ThreadSortKey) -> i64 {
    record.sort_millis(sort_key)
}

pub(super) async fn search_threads(
    store: &AntflyThreadStore,
    params: SearchThreadsParams,
) -> ThreadStoreResult<ThreadSearchPage> {
    let term = params.search_term.trim();
    if term.is_empty() {
        return Err(ThreadStoreError::InvalidRequest {
            message: "thread/search requires search_term".to_owned(),
        });
    }
    if params.sort_key == ThreadSortKey::SectionPosition {
        return Err(ThreadStoreError::InvalidRequest {
            message: "thread/search does not support section-position sorting".to_owned(),
        });
    }
    let cursor = params
        .cursor
        .as_deref()
        .map(|token| ListCursor::parse(token, params.sort_key))
        .transpose()?;
    let hits = store
        .antfly()
        .documents(schema::HISTORY_ITEMS)
        .search_text(term, None, FULL_TEXT_LIMIT, SEMANTIC_LIMIT)
        .await
        .map_err(internal)?;

    // First hit per thread supplies its snippet.
    let mut snippets: HashMap<ThreadId, String> = HashMap::new();
    let mut order: Vec<ThreadId> = Vec::new();
    for hit in hits {
        let Some(doc) = &hit.doc else {
            continue;
        };
        let Some(thread_id) = doc
            .get("thread_id")
            .and_then(Value::as_str)
            .and_then(|id| ThreadId::from_string(id).ok())
        else {
            continue;
        };
        if snippets.contains_key(&thread_id) {
            continue;
        }
        let text = doc
            .get(codex_antfly::SEARCH_TEXT_FIELD)
            .and_then(Value::as_str)
            .unwrap_or_default();
        let snippet = excerpt(text, term);
        if snippet.is_empty() {
            continue;
        }
        snippets.insert(thread_id, snippet);
        order.push(thread_id);
    }

    let mut matched: Vec<ThreadRecord> = Vec::new();
    for thread_id in order {
        let Some(record) = store.load_record(thread_id).await? else {
            continue;
        };
        if record.archived_at.is_some() != params.archived {
            continue;
        }
        if !params.allowed_sources.is_empty() && !params.allowed_sources.contains(&record.source())
        {
            continue;
        }
        matched.push(record);
    }
    matched.sort_by(|a, b| {
        let key = |record: &ThreadRecord| {
            (
                sort_value(record, params.sort_key),
                record.thread_id().to_string(),
            )
        };
        match params.sort_direction {
            SortDirection::Asc => key(a).cmp(&key(b)),
            SortDirection::Desc => key(b).cmp(&key(a)),
        }
    });
    if let Some(cursor) = &cursor
        && let Some(position) = matched
            .iter()
            .position(|record| ListCursor::for_record(record, params.sort_key) == *cursor)
    {
        matched.drain(..=position);
    }
    let page_size = params.page_size.max(1);
    let has_more = matched.len() > page_size;
    matched.truncate(page_size);
    let next_cursor = if has_more {
        matched
            .last()
            .map(|record| ListCursor::for_record(record, params.sort_key).encode(params.sort_key))
    } else {
        None
    };
    Ok(ThreadSearchPage {
        items: matched
            .into_iter()
            .map(|record| {
                let snippet = snippets.remove(&record.thread_id()).unwrap_or_default();
                StoredThreadSearchResult {
                    thread: record.to_stored(None),
                    snippet,
                }
            })
            .collect(),
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn excerpt_matches_local_rules() {
        assert_eq!(excerpt("fix the raft test", "RAFT"), "fix the raft test");
        let long = format!("{} needle {}", "a ".repeat(100), "b ".repeat(100));
        let snippet = excerpt(&long, "needle");
        assert!(snippet.starts_with("... "));
        assert!(snippet.ends_with(" ..."));
        assert!(snippet.contains("needle"));
    }

    #[test]
    fn case_insensitive_find_handles_unicode() {
        assert_eq!(find_case_insensitive("Straße ÜBER", "über"), Some((8, 13)));
        assert_eq!(find_case_insensitive("abc", "zzz"), None);
    }
}
