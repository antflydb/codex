//! `list_threads` over `codex_threads` with SQL keyset pagination.

use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use codex_antfly::sql::SqlValue;
use codex_protocol::ThreadId;

use super::AntflyThreadStore;
use super::internal;
use super::record::ThreadRecord;
use crate::ListThreadsParams;
use crate::SortDirection;
use crate::ThreadPage;
use crate::ThreadRelationFilter;
use crate::ThreadSortKey;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Listing cursor: `{rfc3339}|{thread_id}`, or `{position}|{thread_id}` for
/// section ordering.
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
}

fn normalize(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let trimmed = text.trim_end_matches('/');
    PathBuf::from(if trimmed.is_empty() { "/" } else { trimmed })
}

fn sort_column(sort_key: ThreadSortKey) -> &'static str {
    match sort_key {
        ThreadSortKey::CreatedAt => "created_at_ms",
        ThreadSortKey::UpdatedAt => "updated_at_ms",
        ThreadSortKey::RecencyAt => "recency_at_ms",
        ThreadSortKey::SectionPosition => "section_position",
    }
}

/// Appends `value` to `params` and returns its `$n` placeholder.
fn bind(params: &mut Vec<SqlValue>, value: impl Into<SqlValue>) -> String {
    params.push(value.into());
    format!("${}", params.len())
}

/// A `LIKE` pattern matching `term` as a literal substring, case-sensitively
/// (Antfly's embedded SQL engine does not support `strpos`, the dialect's
/// suggested `instr` translation: `antfly SQL failed (0A000): This SQL
/// statement or expression is not supported`). `%`, `_`, and `\` are escaped
/// so the pattern cannot act as a glob.
fn contains_pattern(term: &str) -> String {
    let mut pattern = String::with_capacity(term.len() + 2);
    pattern.push('%');
    for ch in term.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern.push('%');
    pattern
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
    let in_section_listing = section.is_some();
    let cursor = params
        .cursor
        .as_deref()
        .map(|token| ListCursor::parse(token, params.sort_key))
        .transpose()?;
    let page_size = params.page_size.max(1);

    let mut values: Vec<SqlValue> = Vec::new();
    let mut sql = String::from(super::record::SELECT_THREAD);
    sql.push(' ');

    if let Some(ThreadRelationFilter::DescendantsOf(ancestor)) = params.relation_filter {
        let placeholder = bind(&mut values, ancestor.to_string());
        sql = format!(
            "WITH RECURSIVE subtree(child_thread_id) AS ( \
                SELECT child_thread_id FROM codex_thread_spawn_edges WHERE parent_thread_id = {placeholder} \
                UNION \
                SELECT edge.child_thread_id FROM codex_thread_spawn_edges edge \
                JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id \
            ) {sql} JOIN subtree ON subtree.child_thread_id = t.id "
        );
    } else if let Some(ThreadRelationFilter::DirectChildrenOf(parent)) = params.relation_filter {
        let placeholder = bind(&mut values, parent.to_string());
        sql.push_str(&format!(
            "JOIN codex_thread_spawn_edges e ON e.child_thread_id = t.id AND e.parent_thread_id = {placeholder} "
        ));
    }

    let archived_placeholder = bind(&mut values, params.archived);
    sql.push_str(&format!("WHERE t.archived = {archived_placeholder}"));
    if !params.archived && !in_section_listing {
        sql.push_str(" AND t.preview <> ''");
    }
    match &params.section {
        Some(Some(section)) => {
            let placeholder = bind(&mut values, section.clone());
            sql.push_str(&format!(" AND t.thread_section_id = {placeholder}"));
        }
        Some(None) => sql.push_str(" AND t.thread_section_id IS NULL"),
        None => {}
    }
    match &params.project_id {
        Some(Some(project)) => {
            let placeholder = bind(&mut values, project.clone());
            sql.push_str(&format!(" AND t.project_id = {placeholder}"));
        }
        Some(None) => sql.push_str(" AND t.project_id IS NULL"),
        None => {}
    }
    if !params.allowed_sources.is_empty() {
        let placeholders: Vec<String> = params
            .allowed_sources
            .iter()
            .map(|source| bind(&mut values, super::record::source_text(source)))
            .collect();
        sql.push_str(&format!(" AND t.source IN ({})", placeholders.join(", ")));
    }
    if let Some(providers) = &params.model_providers
        && !providers.is_empty()
    {
        let placeholders: Vec<String> = providers
            .iter()
            .map(|provider| bind(&mut values, provider.clone()))
            .collect();
        sql.push_str(&format!(
            " AND t.model_provider IN ({})",
            placeholders.join(", ")
        ));
    }
    if let Some(cwds) = &params.cwd_filters {
        if cwds.is_empty() {
            sql.push_str(" AND 1 = 0");
        } else {
            let placeholders: Vec<String> = cwds
                .iter()
                .map(|cwd| bind(&mut values, normalize(cwd).to_string_lossy().to_string()))
                .collect();
            sql.push_str(&format!(" AND t.cwd IN ({})", placeholders.join(", ")));
        }
    }
    if let Some(term) = &params.search_term {
        let pattern = contains_pattern(term);
        let p1 = bind(&mut values, pattern.clone());
        let p2 = bind(&mut values, pattern.clone());
        let p3 = bind(&mut values, pattern.clone());
        let p4 = bind(&mut values, pattern);
        sql.push_str(&format!(
            " AND (COALESCE(t.name, '') LIKE {p1} \
                OR t.title LIKE {p2} \
                OR t.preview LIKE {p3} \
                OR t.first_user_message LIKE {p4})"
        ));
    }

    let order_column: String =
        if section.is_some() && params.sort_key == ThreadSortKey::SectionPosition {
            "t.section_position".to_string()
        } else {
            format!("t.{}", sort_column(params.sort_key))
        };
    let op = match params.sort_direction {
        SortDirection::Asc => ">",
        SortDirection::Desc => "<",
    };
    if let Some(cursor) = &cursor {
        let value_param = bind(&mut values, cursor.value);
        let id_param = bind(&mut values, cursor.thread_id.to_string());
        sql.push_str(&format!(
            " AND ({order_column} {op} {value_param} OR ({order_column} = {value_param} AND t.id {op} {id_param}))"
        ));
    }
    let direction = match params.sort_direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    let limit_placeholder = bind(&mut values, (page_size + 1) as i64);
    sql.push_str(&format!(
        " ORDER BY {order_column} {direction}, t.id {direction} LIMIT {limit_placeholder}"
    ));

    let sql_handle = store.antfly().sql().await.map_err(internal)?;
    let rows = sql_handle.fetch_all(&sql, values).await.map_err(internal)?;
    let mut matched: Vec<ThreadRecord> = rows
        .iter()
        .map(ThreadRecord::from_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    let has_more = matched.len() > page_size;
    matched.truncate(page_size);

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
