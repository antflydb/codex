//! Offset pagination, matching the local and in-memory backends' cursor
//! format so cursors stay opaque but structurally familiar across backends.

use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use codex_protocol::error::Result;

use super::invalid;
use crate::Page;
use crate::PageRequest;

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

    pub(super) fn offset(&self) -> usize {
        self.offset as usize
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
