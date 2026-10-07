//! Turns local rollouts into self-contained threads to import.
//!
//! A local thread's history can span several rollout files: forks and
//! reverts start a new segment whose `history_base` points into an older
//! one. Imported threads flatten that lineage into one history so they do
//! not depend on rollout ids that are not threads. Planning reads only each
//! file's `SessionMeta`; a thread's lines are read when it is materialized,
//! so memory stays bounded by the largest single thread.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::ThreadMetadataPatch;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_utils_absolute_path::AbsolutePathBuf;

use crate::source::Overlay;
use crate::source::RolloutHeader;
use crate::source::SectionDef;
use crate::source::read_lines;

/// A thread ready to write into Antfly.
#[derive(Debug)]
pub struct ImportedThread {
    pub created: CreateThreadParams,
    /// Full history in order, starting with the thread's `SessionMeta`.
    pub items: Vec<RolloutItem>,
    pub patch: ThreadMetadataPatch,
    pub archived_at: Option<DateTime<Utc>>,
    pub section: Option<ImportedSection>,
    pub legacy_rollout_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct ImportedSection {
    pub id: String,
    pub name: String,
    pub position: Option<i64>,
}

/// Why a rollout was not imported.
#[derive(Debug)]
pub struct Skipped {
    pub path: PathBuf,
    pub reason: String,
}

/// One thread's lineage and metadata; its lines are read on demand.
#[derive(Clone)]
pub struct ThreadPlan {
    /// Segments from the tip back to the root, each with the exclusive end
    /// of its visible ordinals: where its child forked (`None` for the tip).
    chain: Vec<(RolloutHeader, Option<u64>)>,
    pub overlay: Overlay,
    pub section_name: Option<String>,
}

impl ThreadPlan {
    pub fn thread_id(&self) -> ThreadId {
        self.tip().meta.id
    }

    pub fn tip(&self) -> &RolloutHeader {
        &self.chain[0].0
    }

    pub fn history_mode(&self) -> codex_protocol::protocol::ThreadHistoryMode {
        self.tip().meta.history_mode
    }

    pub fn archived(&self) -> bool {
        self.overlay.archived_at.is_some() || self.tip().archived
    }

    /// Last activity, for `--since` filtering.
    pub fn updated_at(&self) -> Option<DateTime<Utc>> {
        self.overlay.patch.updated_at.or_else(|| {
            let modified = std::fs::metadata(&self.tip().path).ok()?.modified().ok()?;
            Some(DateTime::<Utc>::from(modified))
        })
    }

    /// Reads the lineage and builds the self-contained thread.
    pub async fn materialize(&self) -> std::io::Result<ImportedThread> {
        let mut items = Vec::new();
        // Oldest ancestor first; each segment's end is where its child forked.
        for (segment, end) in self.chain.iter().rev() {
            let end = *end;
            let lines = read_lines(&segment.path).await?;
            items.extend(visible_items(&segment.meta, lines, end));
        }

        let tip = self.tip();
        let mut meta = tip.meta.clone();
        meta.history_base = None;
        meta.forked_from_ordinal_exclusive = None;
        if self.chain.len() > 1 {
            // Ordinals change when ancestors are inlined.
            meta.subagent_history_start_ordinal = None;
        }
        let created = create_params(&meta);
        items.insert(
            0,
            RolloutItem::SessionMeta(SessionMetaLine { meta, git: None }),
        );
        let overlay = self.overlay.clone();
        Ok(ImportedThread {
            created,
            items,
            archived_at: overlay.archived_at.or_else(|| tip.archived.then(Utc::now)),
            section: overlay.section_id.map(|id| ImportedSection {
                name: self.section_name.clone().unwrap_or_else(|| id.clone()),
                id,
                position: overlay.section_position,
            }),
            patch: overlay.patch,
            legacy_rollout_path: tip.path.clone(),
        })
    }
}

fn create_params(meta: &SessionMeta) -> CreateThreadParams {
    CreateThreadParams {
        creator_user_id: meta.creator_user_id.clone(),
        creator_account_id: meta.creator_account_id.clone(),
        session_id: meta.session_id,
        thread_id: meta.id,
        extra_config: None,
        forked_from_id: meta.forked_from_id,
        parent_thread_id: meta.parent_thread_id,
        source: meta.source.clone(),
        thread_source: meta.thread_source.clone(),
        originator: meta.originator.clone(),
        base_instructions: meta.base_instructions.clone().unwrap_or_default(),
        dynamic_tools: meta.dynamic_tools.clone().unwrap_or_default(),
        selected_capability_roots: meta.selected_capability_roots.clone(),
        multi_agent_version: meta.multi_agent_version,
        history_mode: meta.history_mode,
        history_base: None,
        subagent_history_start_ordinal: meta.subagent_history_start_ordinal,
        initial_window_id: meta
            .context_window
            .as_ref()
            .map(|window| window.window_id.clone())
            .unwrap_or_default(),
        runtime_workspace_roots: meta.runtime_workspace_roots.as_ref().map(|roots| {
            roots
                .iter()
                .filter_map(|root| AbsolutePathBuf::from_absolute_path(root).ok())
                .collect()
        }),
        metadata: ThreadPersistenceMetadata {
            cwd: Some(meta.cwd.clone()),
            model_provider: meta.model_provider.clone().unwrap_or_default(),
            memory_mode: if meta.memory_mode.as_deref() == Some("disabled") {
                ThreadMemoryMode::Disabled
            } else {
                ThreadMemoryMode::Enabled
            },
        },
    }
}

/// Lines of a segment below `end` (exclusive), following the local lineage
/// rule: a forked segment's own rows start after its `SessionMeta`.
fn visible_items(
    meta: &SessionMeta,
    lines: Vec<RolloutLine>,
    end: Option<u64>,
) -> Vec<RolloutItem> {
    let start = meta
        .history_base
        .map(|base| base.end_ordinal_exclusive + 1)
        .unwrap_or(1);
    lines
        .into_iter()
        .filter(|line| match line.ordinal {
            Some(ordinal) => ordinal >= start && end.is_none_or(|end| ordinal < end),
            // Legacy lines carry no ordinals and are all visible.
            None => true,
        })
        .map(|line| line.item)
        .collect()
}

/// Builds a plan per thread. A thread's current rollout is the one SQLite
/// points at, or else the newest rollout of that thread that no other
/// rollout of the same thread uses as its base.
pub fn plan(
    headers: Vec<RolloutHeader>,
    overlays: &HashMap<ThreadId, Overlay>,
    sections: &[SectionDef],
) -> (Vec<ThreadPlan>, Vec<Skipped>) {
    let mut by_rollout: HashMap<ThreadId, usize> = HashMap::new();
    let mut by_thread: HashMap<ThreadId, Vec<usize>> = HashMap::new();
    for (index, header) in headers.iter().enumerate() {
        by_rollout.insert(header.rollout_id(), index);
        by_thread.entry(header.meta.id).or_default().push(index);
    }
    let section_names: HashMap<&str, &str> = sections
        .iter()
        .map(|section| (section.id.as_str(), section.name.as_str()))
        .collect();

    let mut plans = Vec::new();
    let mut skipped = Vec::new();
    for (thread_id, candidates) in by_thread {
        let overlay = overlays.get(&thread_id).cloned().unwrap_or_default();
        let tip = overlay
            .rollout_path
            .as_ref()
            .and_then(|path| {
                candidates
                    .iter()
                    .copied()
                    .find(|index| headers[*index].path.as_path() == path.as_path())
            })
            .or_else(|| {
                let bases: HashSet<ThreadId> = candidates
                    .iter()
                    .filter_map(|index| {
                        headers[*index].meta.history_base.map(|base| base.thread_id)
                    })
                    .collect();
                candidates
                    .iter()
                    .copied()
                    .filter(|index| !bases.contains(&headers[*index].rollout_id()))
                    .max_by_key(|index| headers[*index].path.clone())
            });
        let Some(tip) = tip else {
            continue;
        };

        let mut chain = vec![(headers[tip].clone(), None)];
        let mut seen = HashSet::from([tip]);
        let mut broken = None;
        while let Some(base) = chain
            .last()
            .and_then(|(header, _)| header.meta.history_base)
        {
            match by_rollout.get(&base.thread_id).copied() {
                Some(parent) if seen.insert(parent) => {
                    chain.push((headers[parent].clone(), Some(base.end_ordinal_exclusive)));
                }
                Some(_) => {
                    broken = Some("history lineage has a cycle".to_string());
                    break;
                }
                None => {
                    broken = Some(format!("missing base rollout {}", base.thread_id));
                    break;
                }
            }
        }
        if let Some(reason) = broken {
            skipped.push(Skipped {
                path: headers[tip].path.clone(),
                reason,
            });
            continue;
        }
        let section_name = overlay
            .section_id
            .as_deref()
            .and_then(|id| section_names.get(id))
            .map(|name| (*name).to_string());
        plans.push(ThreadPlan {
            chain,
            overlay,
            section_name,
        });
    }
    plans.sort_by_key(|plan| plan.thread_id().to_string());
    (plans, skipped)
}
