//! Offset pagination and row decoding, matching
//! [`crate::local::paging`]'s cursor format and SQL helpers so cursors stay
//! opaque but structurally familiar, and so query results decode the same
//! way, across backends.

use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use codex_protocol::error::Result;

use super::StoredPost;
use super::invalid;
use super::storage_error;
use crate::Page;
use crate::PageRequest;
use crate::SortDirection;

pub(super) struct Window {
    offset: u32,
    pub(super) limit: usize,
}

impl Window {
    pub(super) fn new(page: &PageRequest) -> Result<Self> {
        let offset = match &page.cursor {
            Some(cursor) => {
                let mut bytes = [0; 4];
                let len = BASE64_URL_SAFE_NO_PAD
                    .decode_slice(cursor, &mut bytes)
                    .map_err(|_| invalid("invalid cursor"))?;
                if len != bytes.len() {
                    return Err(invalid("invalid cursor"));
                }
                u32::from_be_bytes(bytes)
            }
            None => 0,
        };
        Ok(Self {
            offset,
            limit: (page.limit.get() as usize).min(50),
        })
    }

    pub(super) fn offset(&self) -> i64 {
        i64::from(self.offset)
    }

    pub(super) fn finish<T>(self, mut results: Vec<T>) -> Result<Page<T>> {
        let has_more = results.len() > self.limit;
        results.truncate(self.limit);
        let next_cursor = if has_more {
            let offset = self
                .offset
                .checked_add(results.len() as u32)
                .ok_or_else(|| invalid("cursor offset exceeds the board limit"))?;
            Some(BASE64_URL_SAFE_NO_PAD.encode(offset.to_be_bytes()))
        } else {
            None
        };
        Ok(Page {
            results,
            next_cursor,
        })
    }
}

/// `ASC`/`DESC`, safe to interpolate directly since it never comes from user
/// input.
pub(super) fn direction(direction: SortDirection) -> &'static str {
    match direction {
        SortDirection::NewestFirst => "DESC",
        SortDirection::OldestFirst => "ASC",
    }
}

/// Decodes a `payload` JSONB column into [`StoredPost`]s, in row order.
pub(super) fn decode_posts(rows: Vec<serde_json::Value>) -> Result<Vec<StoredPost>> {
    rows.into_iter()
        .map(|value| serde_json::from_value(value).map_err(storage_error))
        .collect()
}
