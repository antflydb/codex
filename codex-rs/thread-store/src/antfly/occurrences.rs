//! `search_thread_occurrences` (spec §2.27): literal, case-insensitive
//! substring search over visible user/agent text within one paginated
//! thread's projected items, walking lineage segments oldest to newest.
//!
//! The matching and snippet rules are ported verbatim from
//! `local/thread_history/search.rs` so the two stores agree on byte/UTF-16
//! offsets; only the item source (Antfly projection docs instead of SQLite
//! rows) differs.

use std::borrow::Cow;
use std::collections::HashMap;

use codex_antfly::ScanRequest;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::strip_user_message_prefix;
use pulldown_cmark::Event;
use pulldown_cmark::Parser;
use pulldown_cmark::TagEnd;
use serde::Deserialize;
use serde::Serialize;

use super::AntflyThreadStore;
use super::history::CursorScope;
use super::history::Segment;
use super::history::resolve_lineage;
use super::history::segment_index_for_ordinal;
use super::history::serialize_history_cursor;
use super::history::validate_paginated;
use super::internal;
use super::keys;
use super::projection::ItemDoc;
use super::projection::TurnDoc;
use crate::SearchTextRange;
use crate::SearchThreadOccurrencesParams;
use crate::StoredThreadOccurrence;
use crate::ThreadOccurrenceSearchPage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

const SNIPPET_CONTEXT_BEFORE_CHARS: usize = 48;
const SNIPPET_CONTEXT_AFTER_CHARS: usize = 96;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchCursor {
    thread_id: ThreadId,
    search_term: String,
    next_rollout_ordinal: u64,
    next_occurrence_index: usize,
}

struct Candidate {
    turn_id: String,
    item_id: String,
    rollout_ordinal: u64,
    item: ThreadItem,
}

fn invalid_cursor(cursor: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    }
}

fn parse_cursor(
    cursor: Option<&str>,
    thread_id: ThreadId,
    search_term: &str,
) -> ThreadStoreResult<Option<SearchCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let value: SearchCursor = serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    if value.thread_id != thread_id || value.search_term != search_term {
        return Err(invalid_cursor(cursor));
    }
    Ok(Some(value))
}

async fn candidates_in_segment(
    store: &AntflyThreadStore,
    segment: Segment,
    from_ordinal: u64,
) -> ThreadStoreResult<Vec<Candidate>> {
    let from = keys::item_created_bound(segment.thread_id, from_ordinal.max(segment.start_ordinal));
    let to = match segment.end_ordinal {
        Some(end) => keys::item_created_bound(segment.thread_id, end),
        None => codex_antfly::keys::prefix_end(&keys::item_created_prefix(segment.thread_id)),
    };
    let rows = store
        .antfly()
        .scan_as::<ItemDoc>(ScanRequest {
            from,
            to,
            limit: None,
        })
        .await
        .map_err(internal)?;
    let mut turns_cache: HashMap<String, Option<TurnDoc>> = HashMap::new();
    let mut candidates = Vec::new();
    for (_, doc) in rows {
        let is_final_agent = {
            let turn = match turns_cache.get(&doc.turn_id) {
                Some(turn) => turn.clone(),
                None => {
                    let turn = store
                        .antfly()
                        .get_as::<TurnDoc>(keys::turn_id_key(segment.thread_id, &doc.turn_id))
                        .await
                        .map_err(internal)?;
                    turns_cache.insert(doc.turn_id.clone(), turn.clone());
                    turn
                }
            };
            turn.is_some_and(|turn| {
                turn.final_agent_item_id.as_deref() == Some(doc.item_id.as_str())
            })
        };
        let is_user_message = doc.item_type == "userMessage";
        let is_partial_answer = doc.item_type == "agentMessage"
            && doc
                .item_json
                .get("phase")
                .and_then(serde_json::Value::as_str)
                == Some("partial_answer");
        if !(is_user_message || is_partial_answer || is_final_agent) {
            continue;
        }
        let item: ThreadItem =
            serde_json::from_value(doc.item_json).map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to deserialize stored thread item: {err}"),
            })?;
        candidates.push(Candidate {
            turn_id: doc.turn_id,
            item_id: doc.item_id,
            rollout_ordinal: doc.rollout_ordinal,
            item,
        });
    }
    candidates.sort_by_key(|candidate| candidate.rollout_ordinal);
    Ok(candidates)
}

fn searchable_text(item: &ThreadItem) -> Option<Cow<'_, str>> {
    match item {
        ThreadItem::UserMessage { content, .. } => {
            let mut text_parts = content
                .iter()
                .filter_map(|input| match input {
                    UserInput::Text { text, .. } => Some(strip_user_message_prefix(text)),
                    _ => None,
                })
                .filter(|text| !text.is_empty())
                .peekable();
            let first = text_parts.next()?;
            match text_parts.next() {
                None => Some(Cow::Borrowed(first)),
                Some(second) => {
                    let mut parts = vec![first, second];
                    parts.extend(text_parts);
                    Some(Cow::Owned(parts.concat()))
                }
            }
        }
        ThreadItem::AgentMessage { text, .. } => {
            let text = markdown_to_search_text(text);
            (!text.is_empty()).then_some(Cow::Owned(text))
        }
        _ => None,
    }
}

fn markdown_to_search_text(markdown: &str) -> String {
    let mut text = String::new();
    for event in Parser::new(markdown.trim()) {
        match event {
            Event::Text(value)
            | Event::Code(value)
            | Event::Html(value)
            | Event::InlineHtml(value) => {
                text.push_str(&value);
            }
            Event::SoftBreak | Event::HardBreak | Event::Rule => text.push(' '),
            Event::End(
                TagEnd::Paragraph
                | TagEnd::Heading(_)
                | TagEnd::BlockQuote
                | TagEnd::CodeBlock
                | TagEnd::List(_)
                | TagEnd::Item
                | TagEnd::Table
                | TagEnd::TableHead
                | TagEnd::TableRow
                | TagEnd::TableCell,
            ) => text.push(' '),
            _ => {}
        }
    }
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

struct LiteralMatcher {
    lowercase_needle: String,
}

impl LiteralMatcher {
    fn new(needle: &str) -> Self {
        Self {
            lowercase_needle: needle.to_lowercase(),
        }
    }

    fn find_ranges(&self, text: &str, limit: usize) -> Vec<std::ops::Range<usize>> {
        let lowercase_text = text.to_lowercase();
        let mut spans = Vec::with_capacity(text.chars().count());
        let mut lowercase_start = 0;
        for (original_start, character) in text.char_indices() {
            let lowercase_end =
                lowercase_start + character.to_lowercase().map(char::len_utf8).sum::<usize>();
            spans.push((
                lowercase_start..lowercase_end,
                original_start..original_start + character.len_utf8(),
            ));
            lowercase_start = lowercase_end;
        }

        let mut start_span = 0;
        let mut end_span = 0;
        lowercase_text
            .match_indices(self.lowercase_needle.as_str())
            .take(limit)
            .filter_map(|(start, matched)| {
                let end = start.saturating_add(matched.len());
                while spans.get(start_span)?.0.end <= start {
                    start_span += 1;
                }
                while spans.get(end_span)?.0.end <= end.saturating_sub(1) {
                    end_span += 1;
                }
                let original_start = spans.get(start_span)?.1.start;
                let original_end = spans.get(end_span)?.1.end;
                Some(original_start..original_end)
            })
            .collect()
    }
}

fn utf16_len(text: &str) -> u32 {
    u32::try_from(text.encode_utf16().count()).unwrap_or(u32::MAX)
}

fn char_start_before(text: &str, byte_index: usize, chars_before: usize) -> usize {
    text[..byte_index]
        .char_indices()
        .rev()
        .nth(chars_before)
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn char_end_after(text: &str, byte_index: usize, chars_after: usize) -> usize {
    text[byte_index..]
        .char_indices()
        .nth(chars_after)
        .map(|(offset, _)| byte_index.saturating_add(offset))
        .unwrap_or(text.len())
}

fn occurrence_in_item(
    turn_id: &str,
    item_id: &str,
    text: &str,
    matched: std::ops::Range<usize>,
    turn_cursor: &str,
) -> StoredThreadOccurrence {
    let snippet_start = char_start_before(text, matched.start, SNIPPET_CONTEXT_BEFORE_CHARS);
    let snippet_end = char_end_after(text, matched.end, SNIPPET_CONTEXT_AFTER_CHARS);
    let leading_ellipsis = snippet_start > 0;
    let trailing_ellipsis = snippet_end < text.len();
    let mut snippet = String::new();
    if leading_ellipsis {
        snippet.push_str("... ");
    }
    snippet.push_str(&text[snippet_start..snippet_end]);
    if trailing_ellipsis {
        snippet.push_str(" ...");
    }
    let snippet_match_start =
        if leading_ellipsis { 4 } else { 0 } + utf16_len(&text[snippet_start..matched.start]);
    let match_len = utf16_len(&text[matched]);

    StoredThreadOccurrence {
        turn_id: turn_id.to_owned(),
        item_id: item_id.to_owned(),
        snippet,
        snippet_match_range: SearchTextRange {
            start: snippet_match_start,
            end: snippet_match_start.saturating_add(match_len),
        },
        turn_cursor: turn_cursor.to_owned(),
    }
}

/// The visible (newest-segment) start ordinal of `turn_id`, searching newest
/// to oldest, for `turn_cursor` on items from an ancestor segment.
async fn find_visible_turn_ordinal(
    store: &AntflyThreadStore,
    segments: &[Segment],
    turn_id: &str,
) -> ThreadStoreResult<u64> {
    for segment in segments.iter().rev() {
        if let Some(turn) = store
            .antfly()
            .get_as::<TurnDoc>(keys::turn_id_key(segment.thread_id, turn_id))
            .await
            .map_err(internal)?
        {
            return Ok(turn.rollout_ordinal);
        }
    }
    Err(ThreadStoreError::InvalidRequest {
        message: format!("turn not found: {turn_id}"),
    })
}

pub(super) async fn search_thread_occurrences(
    store: &AntflyThreadStore,
    params: SearchThreadOccurrencesParams,
) -> ThreadStoreResult<ThreadOccurrenceSearchPage> {
    if params.search_term.trim().is_empty() {
        return Err(ThreadStoreError::InvalidRequest {
            message: "thread/searchOccurrences requires search_term".to_owned(),
        });
    }
    if params.page_size == 0 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "thread/searchOccurrences requires page_size greater than zero".to_owned(),
        });
    }
    validate_paginated(
        store,
        params.thread_id,
        /* include_archived */ true,
        "thread/searchOccurrences",
    )
    .await?;
    let cursor = parse_cursor(
        params.cursor.as_deref(),
        params.thread_id,
        &params.search_term,
    )?;
    let matcher = LiteralMatcher::new(params.search_term.as_str());
    let segments = resolve_lineage(store, params.thread_id).await?;
    let cursor_segment = cursor
        .as_ref()
        .map(|cursor| {
            segment_index_for_ordinal(&segments, cursor.next_rollout_ordinal)
                .ok_or_else(|| invalid_cursor("position outside thread lineage"))
        })
        .transpose()?;

    let mut items = Vec::with_capacity(params.page_size);
    let mut effective_turn_ordinals: HashMap<String, u64> = HashMap::new();
    for (segment_index, segment) in segments
        .iter()
        .enumerate()
        .skip(cursor_segment.unwrap_or(0))
    {
        let from_ordinal = if Some(segment_index) == cursor_segment {
            cursor
                .as_ref()
                .map_or(segment.start_ordinal, |cursor| cursor.next_rollout_ordinal)
        } else {
            segment.start_ordinal
        };
        let candidates = candidates_in_segment(store, *segment, from_ordinal).await?;

        let mut matching: Vec<(Candidate, String, Vec<std::ops::Range<usize>>, usize)> = Vec::new();
        let mut matching_occurrences = 0usize;
        for candidate in candidates {
            let Some(text) = searchable_text(&candidate.item) else {
                continue;
            };
            let first_occurrence_index = cursor
                .as_ref()
                .filter(|cursor| cursor.next_rollout_ordinal == candidate.rollout_ordinal)
                .map_or(0, |cursor| cursor.next_occurrence_index);
            let remaining = params
                .page_size
                .saturating_add(1)
                .saturating_sub(items.len())
                .saturating_sub(matching_occurrences);
            let matches = matcher.find_ranges(
                text.as_ref(),
                first_occurrence_index.saturating_add(remaining),
            );
            if matches.len() <= first_occurrence_index {
                continue;
            }
            matching_occurrences += matches.len() - first_occurrence_index;
            let text = text.into_owned();
            matching.push((candidate, text, matches, first_occurrence_index));
            if matching_occurrences
                == params
                    .page_size
                    .saturating_add(1)
                    .saturating_sub(items.len())
            {
                break;
            }
        }

        for (candidate, text, matches, first_occurrence_index) in matching {
            let turn_rollout_ordinal = match effective_turn_ordinals.get(&candidate.turn_id) {
                Some(ordinal) => *ordinal,
                None => {
                    let ordinal =
                        find_visible_turn_ordinal(store, &segments, &candidate.turn_id).await?;
                    effective_turn_ordinals.insert(candidate.turn_id.clone(), ordinal);
                    ordinal
                }
            };
            let turn_cursor = serialize_history_cursor(
                params.thread_id,
                CursorScope::Turns,
                turn_rollout_ordinal,
                true,
            )?;
            for (occurrence_index, matched) in
                matches.into_iter().enumerate().skip(first_occurrence_index)
            {
                if items.len() == params.page_size {
                    return Ok(ThreadOccurrenceSearchPage {
                        items,
                        next_cursor: Some(
                            serde_json::to_string(&SearchCursor {
                                thread_id: params.thread_id,
                                search_term: params.search_term,
                                next_rollout_ordinal: candidate.rollout_ordinal,
                                next_occurrence_index: occurrence_index,
                            })
                            .map_err(|err| {
                                ThreadStoreError::Internal {
                                    message: format!("failed to serialize cursor: {err}"),
                                }
                            })?,
                        ),
                    });
                }
                items.push(occurrence_in_item(
                    &candidate.turn_id,
                    &candidate.item_id,
                    text.as_str(),
                    matched,
                    turn_cursor.as_str(),
                ));
            }
        }
    }

    Ok(ThreadOccurrenceSearchPage {
        items,
        next_cursor: None,
    })
}
