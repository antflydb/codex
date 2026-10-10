//! Antfly-backed thread sections (`codex_thread_sections`) and their
//! per-thread ordering (`codex_threads.thread_section_id` /
//! `section_position` / `section_entered_at_ms`), the same tables
//! `codex-thread-store`'s `AntflyThreadStore` uses
//! (`thread-store/src/antfly/sections.rs`). Mirrors
//! `state/src/runtime/thread_sections.rs` and
//! `state/src/runtime/thread_section_order.rs`.
//!
//! Section ordering ports `thread_section_order.rs`'s sparse-position
//! algorithm verbatim (binary-search a gap between neighbors; renumber with
//! `1_000_000`-spaced positions and retry once no gap remains).

use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use codex_antfly::Antfly;
use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;

use super::internal;
use crate::PINNED_THREAD_SECTION_ID;
use crate::ThreadSection;
use crate::ThreadSectionAppearance;
use crate::ThreadSectionsPage;

const SECTION_POSITION_GAP: i64 = 1_000_000;

fn section_from_row(row: &codex_antfly::sql::SqlRow) -> anyhow::Result<ThreadSection> {
    let id = row.string("id").map_err(internal)?;
    let name = row.string("name").map_err(internal)?;
    let appearance = row.opt_string("appearance").map_err(internal)?;
    ThreadSection::from_row((id, name, appearance))
}

pub(crate) async fn create_thread_section(
    antfly: &Antfly,
    name: &str,
    appearance: Option<ThreadSectionAppearance>,
) -> anyhow::Result<ThreadSection> {
    let section = ThreadSection {
        id: uuid::Uuid::now_v7().to_string(),
        name: name.to_string(),
        appearance,
    };
    let appearance_json = section
        .appearance
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let sql = antfly.sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_thread_sections (id, name, appearance) VALUES ($1, $2, $3)",
        sql_params![section.id.clone(), section.name.clone(), appearance_json],
    )
    .await
    .map_err(internal)?;
    Ok(section)
}

pub(crate) async fn rename_thread_section(
    antfly: &Antfly,
    id: &str,
    name: &str,
    appearance: Option<Option<ThreadSectionAppearance>>,
) -> anyhow::Result<Option<ThreadSection>> {
    if id == PINNED_THREAD_SECTION_ID {
        anyhow::bail!("built-in pinned thread section cannot be renamed");
    }
    let replace_appearance = appearance.is_some();
    let appearance_json = appearance
        .flatten()
        .map(|appearance| serde_json::to_string(&appearance))
        .transpose()?;
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "UPDATE codex_thread_sections SET name = $1, appearance = CASE WHEN $2 THEN $3 ELSE appearance END \
             WHERE id = $4 RETURNING id, name, appearance",
            sql_params![name, replace_appearance, appearance_json, id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| section_from_row(&row)).transpose()
}

pub(crate) async fn delete_thread_section(antfly: &Antfly, id: &str) -> anyhow::Result<bool> {
    if id == PINNED_THREAD_SECTION_ID {
        anyhow::bail!("built-in pinned thread section cannot be deleted");
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    tx.execute(
        "UPDATE codex_threads SET section_position = NULL, section_entered_at_ms = NULL WHERE thread_section_id = $1",
        sql_params![id],
    )
    .await
    .map_err(internal)?;
    let deleted = tx
        .execute(
            "DELETE FROM codex_thread_sections WHERE id = $1",
            sql_params![id],
        )
        .await
        .map_err(internal)?
        > 0;
    tx.commit().await.map_err(internal)?;
    Ok(deleted)
}

pub(crate) async fn get_thread_section_ordering(
    antfly: &Antfly,
    thread_ids: &[ThreadId],
) -> anyhow::Result<HashMap<ThreadId, (Option<i64>, Option<DateTime<Utc>>)>> {
    let mut result = HashMap::new();
    if thread_ids.is_empty() {
        return Ok(result);
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let placeholders: Vec<String> = (1..=thread_ids.len())
        .map(|index| format!("${index}"))
        .collect();
    let params: Vec<codex_antfly::sql::SqlValue> =
        thread_ids.iter().map(|id| id.to_string().into()).collect();
    let rows = sql
        .fetch_all(&format!("SELECT id, section_position, section_entered_at_ms FROM codex_threads WHERE id IN ({})", placeholders.join(", ")), params)
        .await
        .map_err(internal)?;
    for row in &rows {
        let id = ThreadId::try_from(row.string("id").map_err(internal)?)?;
        let section_position = row.opt_i64("section_position").map_err(internal)?;
        let section_entered_at = row
            .opt_i64("section_entered_at_ms")
            .map_err(internal)?
            .map(|millis| {
                DateTime::<Utc>::from_timestamp_millis(millis)
                    .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp millis: {millis}"))
            })
            .transpose()?;
        result.insert(id, (section_position, section_entered_at));
    }
    Ok(result)
}

pub(crate) async fn get_thread_section(
    antfly: &Antfly,
    id: &str,
) -> anyhow::Result<Option<ThreadSection>> {
    let sql = antfly.sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "SELECT id, name, appearance FROM codex_thread_sections WHERE id = $1",
            sql_params![id],
        )
        .await
        .map_err(internal)?;
    row.map(|row| section_from_row(&row)).transpose()
}

pub(crate) async fn list_thread_sections(
    antfly: &Antfly,
    cursor: Option<&str>,
    limit: usize,
) -> anyhow::Result<ThreadSectionsPage> {
    let page_size = limit.max(1);
    let sql = antfly.sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT id, name, appearance FROM codex_thread_sections WHERE ($1::text IS NULL OR id > $1) ORDER BY id LIMIT $2",
            sql_params![cursor, (page_size as i64) + 1],
        )
        .await
        .map_err(internal)?;
    let mut sections = rows
        .iter()
        .map(section_from_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let next_cursor = if sections.len() > page_size {
        sections.truncate(page_size);
        sections.last().map(|section| section.id.clone())
    } else {
        None
    };
    Ok(ThreadSectionsPage {
        sections,
        next_cursor,
    })
}

pub(crate) async fn move_thread_to_section(
    antfly: &Antfly,
    thread_id: ThreadId,
    section: Option<&str>,
    before_thread_id: Option<ThreadId>,
) -> anyhow::Result<bool> {
    if section.is_none() && before_thread_id.is_some() {
        anyhow::bail!("before thread cannot be specified without a section");
    }
    let sql = antfly.sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let thread_id_str = thread_id.to_string();
    let current_section = tx
        .fetch_optional(
            "SELECT thread_section_id FROM codex_threads WHERE id = $1",
            sql_params![thread_id_str.clone()],
        )
        .await
        .map_err(internal)?;
    let Some(current_section) = current_section else {
        return Ok(false);
    };
    let current_section = current_section
        .opt_string("thread_section_id")
        .map_err(internal)?;
    let Some(section) = section else {
        tx.execute(
            "UPDATE codex_threads SET thread_section_id = NULL, section_position = NULL, section_entered_at_ms = NULL WHERE id = $1",
            sql_params![thread_id_str],
        )
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        return Ok(true);
    };

    if section != PINNED_THREAD_SECTION_ID
        && tx
            .fetch_optional(
                "SELECT 1 AS present FROM codex_thread_sections WHERE id = $1",
                sql_params![section],
            )
            .await
            .map_err(internal)?
            .is_none()
    {
        anyhow::bail!("section {section} does not exist");
    }

    let before_thread_id = before_thread_id.map(|id| id.to_string());
    if before_thread_id.as_deref() == Some(thread_id_str.as_str()) {
        anyhow::bail!("thread {thread_id} cannot be moved before itself");
    }
    if let Some(before_thread_id) = before_thread_id.as_deref() {
        let before_section = tx
            .fetch_optional(
                "SELECT thread_section_id FROM codex_threads WHERE id = $1",
                sql_params![before_thread_id],
            )
            .await
            .map_err(internal)?
            .ok_or_else(|| {
                anyhow::anyhow!("before thread {before_thread_id} is not in section {section}")
            })?
            .opt_string("thread_section_id")
            .map_err(internal)?;
        if before_section.as_deref() != Some(section) {
            anyhow::bail!("before thread {before_thread_id} is not in section {section}");
        }
    }

    let position = section_move_position(
        &mut tx,
        section,
        &thread_id_str,
        before_thread_id.as_deref(),
    )
    .await?;
    if current_section.as_deref() == Some(section) {
        tx.execute(
            "UPDATE codex_threads SET section_position = $1 WHERE id = $2",
            sql_params![position, thread_id_str],
        )
        .await
        .map_err(internal)?;
    } else {
        tx.execute(
            "UPDATE codex_threads SET thread_section_id = $1, section_position = $2, section_entered_at_ms = $3 WHERE id = $4",
            sql_params![section, position, Utc::now().timestamp_millis(), thread_id_str],
        )
        .await
        .map_err(internal)?;
    }
    tx.commit().await.map_err(internal)?;
    Ok(true)
}

async fn section_move_position(
    tx: &mut SqlTx,
    section: &str,
    thread_id: &str,
    before_thread_id: Option<&str>,
) -> anyhow::Result<i64> {
    let mut renumbered = false;
    loop {
        let position = if let Some(before_thread_id) = before_thread_id {
            let upper = tx
                .fetch_optional(
                    "SELECT section_position FROM codex_threads WHERE id = $1 AND thread_section_id = $2",
                    sql_params![before_thread_id, section],
                )
                .await
                .map_err(internal)?
                .ok_or_else(|| anyhow::anyhow!("before thread {before_thread_id} is not in section {section}"))?
                .opt_i64("section_position")
                .map_err(internal)?
                .ok_or_else(|| anyhow::anyhow!("before thread {before_thread_id} is not in section {section}"))?;
            let lower = tx
                .fetch_optional(
                    "SELECT MAX(section_position) AS value FROM codex_threads WHERE thread_section_id = $1 AND section_position < $2 AND id <> $3",
                    sql_params![section, upper, thread_id],
                )
                .await
                .map_err(internal)?
                .and_then(|row| row.opt_i64("value").ok().flatten());
            match lower {
                Some(lower) if (upper as i128) - (lower as i128) > 1 => Some(
                    i64::try_from(lower as i128 + (upper as i128 - lower as i128) / 2)
                        .unwrap_or(lower),
                ),
                Some(_) => None,
                None if upper > 1 => Some(upper / 2),
                None => None,
            }
        } else {
            let max_position = tx
                .fetch_optional(
                    "SELECT MAX(section_position) AS value FROM codex_threads WHERE thread_section_id = $1 AND id <> $2",
                    sql_params![section, thread_id],
                )
                .await
                .map_err(internal)?
                .and_then(|row| row.opt_i64("value").ok().flatten());
            max_position
                .unwrap_or_default()
                .checked_add(SECTION_POSITION_GAP)
        };

        if let Some(position) = position {
            return Ok(position);
        }
        if renumbered {
            anyhow::bail!("section {section} has no remaining thread positions");
        }
        renumber_section_positions(tx, section, Some(thread_id)).await?;
        renumbered = true;
    }
}

async fn renumber_section_positions(
    tx: &mut SqlTx,
    section: &str,
    excluded_thread_id: Option<&str>,
) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE codex_threads SET section_position = ranked.position \
         FROM ( \
             SELECT id, ROW_NUMBER() OVER (ORDER BY section_position ASC, id ASC) * $1 AS position \
             FROM codex_threads WHERE thread_section_id = $2 AND ($3::text IS NULL OR id <> $3) \
         ) AS ranked \
         WHERE codex_threads.id = ranked.id",
        sql_params![SECTION_POSITION_GAP, section, excluded_thread_id],
    )
    .await
    .map_err(internal)?;
    Ok(())
}
