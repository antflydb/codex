//! Reads an existing `CODEX_HOME`: rollout files and, read-only, the SQLite
//! thread metadata. Nothing here creates or modifies local state.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::SessionMeta;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_thread_store::GitInfoPatch;
use codex_thread_store::ThreadMetadataPatch;
use sqlx::Row;

/// The opening `SessionMeta` of one rollout file: a single history segment.
#[derive(Clone)]
pub struct RolloutHeader {
    pub path: PathBuf,
    pub archived: bool,
    pub meta: SessionMeta,
}

impl RolloutHeader {
    /// The segment id other segments reference in `history_base`: the
    /// rollout id in the file name, or the thread id for legacy names.
    pub fn rollout_id(&self) -> ThreadId {
        codex_rollout::rollout_id_from_path(&self.path).unwrap_or(self.meta.id)
    }
}

/// Every `rollout-*.jsonl[.zst]` under `sessions/` and `archived_sessions/`.
pub fn discover_rollouts(codex_home: &Path) -> Vec<(PathBuf, bool)> {
    let mut found = Vec::new();
    for (dir, archived) in [("sessions", false), ("archived_sessions", true)] {
        let mut stack = vec![codex_home.join(dir)];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                if name.starts_with("rollout-")
                    && (name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
                {
                    found.push((path, archived));
                }
            }
        }
    }
    found.sort();
    found
}

/// Reads only a rollout's opening `SessionMeta`. Returns `None` for a file
/// that does not start with one.
pub async fn read_header(path: &Path, archived: bool) -> std::io::Result<Option<RolloutHeader>> {
    let mut reader = codex_rollout::open_rollout_line_reader(path).await?;
    while let Some(text) = reader.next_line().await? {
        if text.trim().is_empty() {
            continue;
        }
        let Ok(line) = codex_rollout::parse_rollout_line(&text) else {
            return Ok(None);
        };
        return Ok(match line.item {
            RolloutItem::SessionMeta(session) => Some(RolloutHeader {
                path: path.to_path_buf(),
                archived,
                meta: session.meta,
            }),
            _ => None,
        });
    }
    Ok(None)
}

/// Reads every line after a rollout's opening `SessionMeta`, keeping
/// ordinals. Unparseable lines are skipped, as the local store does.
pub async fn read_lines(path: &Path) -> std::io::Result<Vec<RolloutLine>> {
    let mut reader = codex_rollout::open_rollout_line_reader(path).await?;
    let mut lines = Vec::new();
    let mut first = true;
    while let Some(text) = reader.next_line().await? {
        if text.trim().is_empty() {
            continue;
        }
        let Ok(line) = codex_rollout::parse_rollout_line(&text) else {
            continue;
        };
        if first {
            first = false;
            if matches!(line.item, RolloutItem::SessionMeta(_)) {
                continue;
            }
        }
        lines.push(line);
    }
    Ok(lines)
}

/// Display metadata the local store keeps only in SQLite.
#[derive(Clone, Debug, Default)]
pub struct Overlay {
    pub rollout_path: Option<PathBuf>,
    pub archived_at: Option<DateTime<Utc>>,
    pub section_id: Option<String>,
    pub section_position: Option<i64>,
    pub section_entered_at: Option<DateTime<Utc>>,
    pub patch: ThreadMetadataPatch,
}

/// A section definition from the local store.
#[derive(Clone, Debug)]
pub struct SectionDef {
    pub id: String,
    pub name: String,
}

fn millis(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

/// Reads thread display metadata from `state_*.sqlite` without creating or
/// migrating it. A missing database yields no overlays.
pub async fn read_overlays(
    codex_home: &Path,
) -> anyhow::Result<(HashMap<ThreadId, Overlay>, Vec<SectionDef>)> {
    let Some(path) = latest_state_db(codex_home) else {
        return Ok((HashMap::new(), Vec::new()));
    };
    let sqlite = codex_state::SqliteConfig::from_sqlite_home(
        codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(codex_home)?,
    );
    let pool = sqlite.open_read_only_pool(&path, None).await?;
    let columns: Vec<String> = sqlx::query("SELECT name FROM pragma_table_info('threads')")
        .fetch_all(&pool)
        .await?
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect();
    let has = |name: &str| columns.iter().any(|column| column == name);
    let optional = |name: &str| {
        if has(name) {
            name.to_string()
        } else {
            format!("NULL AS {name}")
        }
    };
    let select = format!(
        "SELECT id, rollout_path, title, preview, first_user_message, model_provider, cwd, \
         archived, archived_at, git_sha, git_branch, {}, {}, {}, {}, {}, {}, {}, {}, {} \
         FROM threads",
        optional("name"),
        optional("model"),
        optional("reasoning_effort"),
        optional("created_at_ms"),
        optional("updated_at_ms"),
        optional("recency_at_ms"),
        optional("thread_section_id"),
        optional("section_position"),
        optional("project_id"),
    );
    let mut overlays = HashMap::new();
    // Built only from fixed column names.
    for row in sqlx::query(sqlx::AssertSqlSafe(select))
        .fetch_all(&pool)
        .await?
    {
        let Ok(id) = row.try_get::<String, _>("id") else {
            continue;
        };
        let Ok(thread_id) = ThreadId::from_string(&id) else {
            continue;
        };
        let archived = row.try_get::<i64, _>("archived").unwrap_or(0) != 0;
        let archived_at = archived.then(|| {
            row.try_get::<Option<i64>, _>("archived_at")
                .ok()
                .flatten()
                .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0))
                .unwrap_or_else(Utc::now)
        });
        let get = |name: &str| row.try_get::<Option<String>, _>(name).ok().flatten();
        let get_i64 = |name: &str| row.try_get::<Option<i64>, _>(name).ok().flatten();
        let git_sha = non_empty(get("git_sha"));
        let git_branch = non_empty(get("git_branch"));
        let git_info = (git_sha.is_some() || git_branch.is_some()).then(|| GitInfoPatch {
            sha: git_sha.map(Some),
            branch: git_branch.map(Some),
            ..Default::default()
        });
        let patch = ThreadMetadataPatch {
            name: non_empty(get("name")).map(Some),
            preview: non_empty(get("preview")),
            title: non_empty(get("title")),
            first_user_message: non_empty(get("first_user_message")),
            model_provider: non_empty(get("model_provider")),
            model: non_empty(get("model")),
            reasoning_effort: get("reasoning_effort")
                .and_then(|effort| effort.parse::<ReasoningEffort>().ok())
                .map(Some),
            cwd: non_empty(get("cwd")).map(PathBuf::from),
            created_at: millis(get_i64("created_at_ms")),
            updated_at: millis(get_i64("updated_at_ms")),
            advance_recency_at: millis(get_i64("recency_at_ms")),
            project_id: non_empty(get("project_id")).map(Some),
            git_info,
            ..Default::default()
        };
        let section_id = non_empty(get("thread_section_id"));
        overlays.insert(
            thread_id,
            Overlay {
                rollout_path: get("rollout_path").map(PathBuf::from),
                archived_at,
                section_position: section_id.as_ref().and(get_i64("section_position")),
                section_entered_at: None,
                section_id,
                patch,
            },
        );
    }
    let sections = sqlx::query("SELECT id, name FROM thread_sections")
        .fetch_all(&pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| {
                    Some(SectionDef {
                        id: row.try_get("id").ok()?,
                        name: row.try_get("name").ok()?,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    pool.close().await;
    Ok((overlays, sections))
}

/// The newest `state_N.sqlite` in `codex_home`.
fn latest_state_db(codex_home: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(codex_home).ok()?;
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let version: u32 = name
                .strip_prefix("state_")?
                .strip_suffix(".sqlite")?
                .parse()
                .ok()?;
            Some((version, entry.path()))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, path)| path)
}
