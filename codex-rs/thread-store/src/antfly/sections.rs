//! Independently persisted thread sections (`codex_thread_sections`),
//! including the built-in Pinned section (`codex_state::PINNED_THREAD_SECTION_ID`),
//! which the schema migration seeds and which can never be renamed or
//! deleted.
//!
//! A thread's section membership (`codex_threads.thread_section_id` /
//! `section_position` / `section_entered_at_ms`) is columns on the thread
//! row, not a copy of the section's name/appearance: `codex_threads`'s
//! `SELECT_THREAD` joins those in at read time (see `record::SELECT_THREAD`),
//! so renaming a section needs no fan-out write to its members.
//!
//! Section ordering ports `state/src/runtime/thread_section_order.rs`'s
//! sparse-position algorithm verbatim (binary-search a gap between
//! neighbors; renumber with `1_000_000`-spaced positions and retry once no
//! gap remains), so `AntflyThreadStore` and `StateRuntime` order a section
//! identically.

use codex_antfly::sql::SqlTx;
use codex_antfly::sql_params;
use codex_state::PINNED_THREAD_SECTION_ID;
use codex_state::ThreadSection;

use super::AntflyThreadStore;
use super::internal;
use crate::CreateThreadSectionParams;
use crate::DeleteThreadSectionParams;
use crate::ListThreadSectionsParams;
use crate::MoveThreadToSectionParams;
use crate::RenameThreadSectionParams;
use crate::StoredThreadSection;
use crate::StoredThreadSectionsPage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

const SECTION_POSITION_GAP: i64 = 1_000_000;

fn stored(section: ThreadSection) -> StoredThreadSection {
    StoredThreadSection {
        id: section.id,
        name: section.name,
        appearance: section.appearance,
    }
}

fn section_from_row(row: &codex_antfly::sql::SqlRow) -> ThreadStoreResult<ThreadSection> {
    Ok(ThreadSection {
        id: row.string("id").map_err(internal)?,
        name: row.string("name").map_err(internal)?,
        appearance: row
            .opt_string("appearance")
            .map_err(internal)?
            .map(|text| serde_json::from_str(&text))
            .transpose()
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("invalid section appearance: {err}"),
            })?,
    })
}

pub(super) async fn list_thread_sections(
    store: &AntflyThreadStore,
    params: ListThreadSectionsParams,
) -> ThreadStoreResult<StoredThreadSectionsPage> {
    let limit = params.limit.max(1);
    let sql = store.antfly().sql().await.map_err(internal)?;
    let rows = sql
        .fetch_all(
            "SELECT id, name, appearance FROM codex_thread_sections \
             WHERE $1::text IS NULL OR id > $1 ORDER BY id LIMIT $2",
            sql_params![params.cursor.clone(), (limit as i64) + 1],
        )
        .await
        .map_err(internal)?;
    let mut sections = rows
        .iter()
        .map(section_from_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    let next_cursor = if sections.len() > limit {
        sections.truncate(limit);
        sections.last().map(|section| section.id.clone())
    } else {
        None
    };
    Ok(StoredThreadSectionsPage {
        sections: sections.into_iter().map(stored).collect(),
        next_cursor,
    })
}

pub(super) async fn create_thread_section(
    store: &AntflyThreadStore,
    params: CreateThreadSectionParams,
) -> ThreadStoreResult<StoredThreadSection> {
    let section = ThreadSection {
        id: uuid::Uuid::now_v7().to_string(),
        name: params.name,
        appearance: params.appearance,
    };
    let appearance_json = section
        .appearance
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize section appearance: {err}"),
        })?;
    let sql = store.antfly().sql().await.map_err(internal)?;
    sql.execute(
        "INSERT INTO codex_thread_sections (id, name, appearance) VALUES ($1, $2, $3)",
        sql_params![section.id.clone(), section.name.clone(), appearance_json],
    )
    .await
    .map_err(internal)?;
    Ok(stored(section))
}

pub(super) async fn rename_thread_section(
    store: &AntflyThreadStore,
    params: RenameThreadSectionParams,
) -> ThreadStoreResult<Option<StoredThreadSection>> {
    if params.section_id == PINNED_THREAD_SECTION_ID {
        return Err(ThreadStoreError::Internal {
            message: "built-in pinned thread section cannot be renamed".to_owned(),
        });
    }
    let replace_appearance = params.appearance.is_some();
    let appearance_json = params
        .appearance
        .flatten()
        .map(|appearance| serde_json::to_string(&appearance))
        .transpose()
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("serialize section appearance: {err}"),
        })?;
    let sql = store.antfly().sql().await.map_err(internal)?;
    let row = sql
        .fetch_optional(
            "UPDATE codex_thread_sections \
             SET name = $1, appearance = CASE WHEN $2 THEN $3 ELSE appearance END \
             WHERE id = $4 RETURNING id, name, appearance",
            sql_params![
                params.name,
                replace_appearance,
                appearance_json,
                params.section_id
            ],
        )
        .await
        .map_err(internal)?;
    row.map(|row| section_from_row(&row).map(stored))
        .transpose()
}

pub(super) async fn delete_thread_section(
    store: &AntflyThreadStore,
    params: DeleteThreadSectionParams,
) -> ThreadStoreResult<bool> {
    if params.section_id == PINNED_THREAD_SECTION_ID {
        return Err(ThreadStoreError::Internal {
            message: "built-in pinned thread section cannot be deleted".to_owned(),
        });
    }
    let sql = store.antfly().sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    tx.execute(
        "UPDATE codex_threads SET section_position = NULL, section_entered_at_ms = NULL \
         WHERE thread_section_id = $1",
        sql_params![params.section_id.clone()],
    )
    .await
    .map_err(internal)?;
    let deleted = tx
        .execute(
            "DELETE FROM codex_thread_sections WHERE id = $1",
            sql_params![params.section_id.clone()],
        )
        .await
        .map_err(internal)?
        > 0;
    tx.commit().await.map_err(internal)?;
    Ok(deleted)
}

pub(super) async fn move_thread_to_section(
    store: &AntflyThreadStore,
    params: MoveThreadToSectionParams,
) -> ThreadStoreResult<()> {
    if params
        .section
        .as_deref()
        .is_some_and(|section| section.trim().is_empty())
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: "section must not be empty".to_owned(),
        });
    }
    if params.section.is_none() && params.before_thread_id.is_some() {
        return Err(ThreadStoreError::InvalidRequest {
            message: "before thread cannot be specified without a section".to_owned(),
        });
    }
    let sql = store.antfly().sql().await.map_err(internal)?;
    let mut tx = sql.begin().await.map_err(internal)?;
    let thread_id = params.thread_id.to_string();
    let current_section = tx
        .fetch_optional(
            "SELECT thread_section_id FROM codex_threads WHERE id = $1",
            sql_params![thread_id.clone()],
        )
        .await
        .map_err(internal)?
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?
        .opt_string("thread_section_id")
        .map_err(internal)?;

    let Some(section) = params.section.as_deref() else {
        tx.execute(
            "UPDATE codex_threads SET thread_section_id = NULL, section_position = NULL, \
             section_entered_at_ms = NULL WHERE id = $1",
            sql_params![thread_id],
        )
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        return Ok(());
    };

    if let Some(before) = params.before_thread_id
        && before == params.thread_id
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {} cannot be moved before itself", params.thread_id),
        });
    }
    // Unlike `StateRuntime`'s SQLite-backed sections, a thread can move into
    // an ad hoc section tag that was never created through
    // `create_thread_section`; `codex_threads.thread_section_id` has a
    // foreign key into `codex_thread_sections`, though, so that tag still
    // needs a row (named after itself, like the key-value store's fallback
    // display name) for the move to satisfy it.
    if section != PINNED_THREAD_SECTION_ID {
        tx.execute(
            "INSERT INTO codex_thread_sections (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            sql_params![section, section],
        )
        .await
        .map_err(internal)?;
    }
    if let Some(before) = params.before_thread_id {
        let before_section = tx
            .fetch_optional(
                "SELECT thread_section_id FROM codex_threads WHERE id = $1",
                sql_params![before.to_string()],
            )
            .await
            .map_err(internal)?
            .ok_or(ThreadStoreError::InvalidRequest {
                message: format!("before thread {before} is not in section {section}"),
            })?
            .opt_string("thread_section_id")
            .map_err(internal)?;
        if before_section.as_deref() != Some(section) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("before thread {before} is not in section {section}"),
            });
        }
    }

    let position = section_move_position(
        &mut tx,
        section,
        &thread_id,
        params.before_thread_id.map(|id| id.to_string()).as_deref(),
    )
    .await?;
    if current_section.as_deref() == Some(section) {
        tx.execute(
            "UPDATE codex_threads SET section_position = $1 WHERE id = $2",
            sql_params![position, thread_id],
        )
        .await
        .map_err(internal)?;
    } else {
        tx.execute(
            "UPDATE codex_threads SET thread_section_id = $1, section_position = $2, \
             section_entered_at_ms = $3 WHERE id = $4",
            sql_params![
                section,
                position,
                chrono::Utc::now().timestamp_millis(),
                thread_id
            ],
        )
        .await
        .map_err(internal)?;
    }
    tx.commit().await.map_err(internal)
}

async fn section_move_position(
    tx: &mut SqlTx,
    section: &str,
    thread_id: &str,
    before_thread_id: Option<&str>,
) -> ThreadStoreResult<i64> {
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
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("before thread {before_thread_id} is not in section {section}"),
                })?
                .opt_i64("section_position")
                .map_err(internal)?
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("before thread {before_thread_id} is not in section {section}"),
                })?;
            let lower = tx
                .fetch_optional(
                    "SELECT MAX(section_position) AS value FROM codex_threads \
                     WHERE thread_section_id = $1 AND section_position < $2 AND id <> $3",
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
                    "SELECT MAX(section_position) AS value FROM codex_threads \
                     WHERE thread_section_id = $1 AND id <> $2",
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
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("section {section} has no remaining thread positions"),
            });
        }
        renumber_section_positions(tx, section, Some(thread_id)).await?;
        renumbered = true;
    }
}

async fn renumber_section_positions(
    tx: &mut SqlTx,
    section: &str,
    excluded_thread_id: Option<&str>,
) -> ThreadStoreResult<()> {
    tx.execute(
        "UPDATE codex_threads SET section_position = ranked.position \
         FROM ( \
             SELECT id, ROW_NUMBER() OVER (ORDER BY section_position ASC, id ASC) * $1 AS position \
             FROM codex_threads \
             WHERE thread_section_id = $2 AND ($3::text IS NULL OR id <> $3) \
         ) AS ranked \
         WHERE codex_threads.id = ranked.id",
        sql_params![SECTION_POSITION_GAP, section, excluded_thread_id],
    )
    .await
    .map_err(internal)?;
    Ok(())
}
