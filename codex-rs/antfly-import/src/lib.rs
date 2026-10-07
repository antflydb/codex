//! Imports an existing local `CODEX_HOME` (rollout files plus SQLite thread
//! metadata, read-only) into the Antfly thread store.

mod plan;
mod source;

use std::path::Path;

pub use plan::ImportedSection;
pub use plan::ImportedThread;
pub use plan::Skipped;
pub use plan::ThreadPlan;
pub use source::Overlay;
pub use source::RolloutHeader;
pub use source::discover_rollouts;
pub use source::read_header;
pub use source::read_lines;
pub use source::read_overlays;

/// Lineage and metadata for every local thread, read without loading
/// history.
pub struct ImportPlan {
    pub threads: Vec<ThreadPlan>,
    pub skipped: Vec<Skipped>,
    pub rollout_files: usize,
}

/// Reads `codex_home` without modifying it.
pub async fn load(codex_home: &Path) -> anyhow::Result<ImportPlan> {
    let files = discover_rollouts(codex_home);
    let rollout_files = files.len();
    let mut headers = Vec::with_capacity(files.len());
    let mut skipped = Vec::new();
    for (path, archived) in files {
        match read_header(&path, archived).await {
            Ok(Some(header)) => headers.push(header),
            Ok(None) => skipped.push(Skipped {
                path,
                reason: "does not start with a SessionMeta".to_string(),
            }),
            Err(err) => skipped.push(Skipped {
                path,
                reason: format!("unreadable: {err}"),
            }),
        }
    }
    let (overlays, sections) = read_overlays(codex_home).await?;
    let (threads, mut plan_skipped) = plan::plan(headers, &overlays, &sections);
    skipped.append(&mut plan_skipped);
    Ok(ImportPlan {
        threads,
        skipped,
        rollout_files,
    })
}
