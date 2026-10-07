//! `list_threads` over the ordered listing and section indexes.

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use codex_antfly::ScanRequest;
use codex_protocol::ThreadId;

use super::AntflyThreadStore;
use super::internal;
use super::keys;
use super::record::ThreadRecord;
use crate::ListThreadsParams;
use crate::SortDirection;
use crate::ThreadPage;
use crate::ThreadRelationFilter;
use crate::ThreadSortKey;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Records fetched per index scan while filling a page.
const SCAN_BATCH: usize = 200;

/// Listing cursor in the local store's format: `{rfc3339}|{thread_id}`, or
/// `{position}|{thread_id}` for section ordering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ListCursor {
    pub(crate) value: i64,
    pub(crate) thread_id: ThreadId,
}

impl ListCursor {
    pub(crate) fn for_record(record: &ThreadRecord, sort_key: ThreadSortKey) -> Self {
        Self {
            value: match sort_key {
                ThreadSortKey::SectionPosition => record.section_position.unwrap_or(i64::MAX),
                other => record.sort_millis(other),
            },
            thread_id: record.thread_id(),
        }
    }

    pub(crate) fn encode(&self, sort_key: ThreadSortKey) -> String {
        if sort_key == ThreadSortKey::SectionPosition {
            return format!("{}|{}", self.value, self.thread_id);
        }
        let timestamp = DateTime::<Utc>::from_timestamp_millis(self.value)
            .unwrap_or_default()
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        format!("{timestamp}|{}", self.thread_id)
    }

    pub(crate) fn parse(token: &str, sort_key: ThreadSortKey) -> ThreadStoreResult<Self> {
        let invalid = || ThreadStoreError::InvalidRequest {
            message: format!("invalid cursor: {token}"),
        };
        let (value, thread_id) = token.split_once('|').ok_or_else(invalid)?;
        let thread_id = ThreadId::from_string(thread_id).map_err(|_| invalid())?;
        let value = if sort_key == ThreadSortKey::SectionPosition {
            value.parse().map_err(|_| invalid())?
        } else {
            DateTime::parse_from_rfc3339(value)
                .map_err(|_| invalid())?
                .timestamp_millis()
        };
        Ok(Self { value, thread_id })
    }

    /// The index key this cursor points at, for resuming a descending scan.
    fn index_key(&self, archived: bool, sort_key: ThreadSortKey, section: Option<&str>) -> String {
        match (sort_key, section) {
            (ThreadSortKey::SectionPosition, Some(section)) => {
                keys::section_entry(section, self.value, self.thread_id)
            }
            _ => keys::index_entry(archived, sort_key, self.value, self.thread_id),
        }
    }
}

fn normalize(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let trimmed = text.trim_end_matches('/');
    PathBuf::from(if trimmed.is_empty() { "/" } else { trimmed })
}

struct Filter<'a> {
    params: &'a ListThreadsParams,
    descendants: Option<HashSet<ThreadId>>,
    cwds: Option<Vec<PathBuf>>,
}

impl Filter<'_> {
    fn matches(&self, record: &ThreadRecord) -> bool {
        let params = self.params;
        if params.archived != record.archived_at.is_some() {
            return false;
        }
        let in_section_listing = matches!(params.section, Some(Some(_)));
        if !params.archived && !in_section_listing && record.preview().is_empty() {
            return false;
        }
        if !params.allowed_sources.is_empty() && !params.allowed_sources.contains(&record.source())
        {
            return false;
        }
        if let Some(providers) = &params.model_providers
            && !providers.is_empty()
            && !providers.contains(&record.model_provider())
        {
            return false;
        }
        if let Some(cwds) = &self.cwds
            && !cwds.contains(&normalize(&record.cwd()))
        {
            return false;
        }
        if let Some(section) = &params.section
            && section.as_deref() != record.section.as_deref()
        {
            return false;
        }
        if let Some(project) = &params.project_id
            && project.as_deref() != record.project_id().as_deref()
        {
            return false;
        }
        if let Some(term) = &params.search_term
            && !record.searchable_summary().contains(term.as_str())
        {
            return false;
        }
        match &params.relation_filter {
            Some(ThreadRelationFilter::DirectChildrenOf(parent)) => {
                record.created.parent_thread_id == Some(*parent)
            }
            Some(ThreadRelationFilter::DescendantsOf(_)) => self
                .descendants
                .as_ref()
                .is_some_and(|descendants| descendants.contains(&record.thread_id())),
            None => true,
        }
    }
}

/// Every thread transitively spawned from `ancestor`, excluding it.
async fn descendants_of(
    store: &AntflyThreadStore,
    ancestor: ThreadId,
) -> ThreadStoreResult<HashSet<ThreadId>> {
    let records = store
        .antfly()
        .scan_as::<ThreadRecord>(ScanRequest::prefix(keys::THREAD_PREFIX))
        .await
        .map_err(internal)?;
    let parents: Vec<(ThreadId, Option<ThreadId>)> = records
        .iter()
        .map(|(_, record)| (record.thread_id(), record.created.parent_thread_id))
        .collect();
    let mut subtree = HashSet::from([ancestor]);
    loop {
        let mut discovered = false;
        for (thread_id, parent) in &parents {
            if parent.is_some_and(|parent| subtree.contains(&parent)) {
                discovered |= subtree.insert(*thread_id);
            }
        }
        if !discovered {
            break;
        }
    }
    subtree.remove(&ancestor);
    Ok(subtree)
}

pub(super) async fn list_threads(
    store: &AntflyThreadStore,
    params: ListThreadsParams,
) -> ThreadStoreResult<ThreadPage> {
    let section = match (&params.section, params.sort_key) {
        (Some(Some(section)), _) => Some(section.clone()),
        (_, ThreadSortKey::SectionPosition) => {
            return Err(ThreadStoreError::InvalidRequest {
                message: "section-position sorting requires a section filter".to_owned(),
            });
        }
        _ => None,
    };
    let section_ordered = params.sort_key == ThreadSortKey::SectionPosition;
    let cursor = params
        .cursor
        .as_deref()
        .map(|token| ListCursor::parse(token, params.sort_key))
        .transpose()?;
    let descendants = match &params.relation_filter {
        Some(ThreadRelationFilter::DescendantsOf(ancestor)) => {
            Some(descendants_of(store, *ancestor).await?)
        }
        _ => None,
    };
    let filter = Filter {
        cwds: params
            .cwd_filters
            .as_ref()
            .map(|cwds| cwds.iter().map(|cwd| normalize(cwd)).collect()),
        params: &params,
        descendants,
    };
    let page_size = params.page_size.max(1);

    // Index order: section listings ascend by position; time listings are
    // stored newest first.
    let prefix = match (&section, section_ordered) {
        (Some(section), true) => keys::section_prefix(section),
        _ => keys::index_prefix(params.archived, params.sort_key),
    };
    let index_descends = !section_ordered;
    let wants_descending = params.sort_direction == SortDirection::Desc;

    let mut matched: Vec<ThreadRecord> = Vec::new();
    let mut has_more = false;
    if index_descends == wants_descending {
        // The index order is the requested order: resume after the cursor.
        let mut from = match &cursor {
            Some(cursor) => format!(
                "{}\u{0}",
                cursor.index_key(params.archived, params.sort_key, section.as_deref())
            ),
            None => prefix.clone(),
        };
        let to = codex_antfly::keys::prefix_end(&prefix);
        'scan: loop {
            let batch = store
                .antfly()
                .scan_as::<ThreadRecord>(ScanRequest {
                    from: from.clone(),
                    to: to.clone(),
                    limit: Some(SCAN_BATCH),
                })
                .await
                .map_err(internal)?;
            let exhausted = batch.len() < SCAN_BATCH;
            for (key, record) in batch {
                from = format!("{key}\u{0}");
                if !filter.matches(&record) {
                    continue;
                }
                if matched.len() == page_size {
                    has_more = true;
                    break 'scan;
                }
                matched.push(record);
            }
            if exhausted {
                break;
            }
        }
    } else {
        // Reverse of the index order: read everything, then page.
        let mut all: Vec<ThreadRecord> = store
            .antfly()
            .scan_as::<ThreadRecord>(ScanRequest::prefix(&prefix))
            .await
            .map_err(internal)?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        all.reverse();
        let mut started = cursor.is_none();
        for record in all {
            if !started {
                started = cursor.as_ref().is_some_and(|cursor| {
                    ListCursor::for_record(&record, params.sort_key) == *cursor
                });
                continue;
            }
            if !filter.matches(&record) {
                continue;
            }
            if matched.len() == page_size {
                has_more = true;
                break;
            }
            matched.push(record);
        }
    }

    let next_cursor = if has_more {
        matched
            .last()
            .map(|record| ListCursor::for_record(record, params.sort_key).encode(params.sort_key))
    } else {
        None
    };
    Ok(ThreadPage {
        items: matched
            .iter()
            .map(|record| record.to_stored(None))
            .collect(),
        next_cursor,
    })
}
