use std::path::PathBuf;

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::Utc;
use clap::Parser;
use codex_protocol::protocol::ThreadHistoryMode;

/// Import local Codex threads into Antfly.
#[derive(Parser)]
struct Args {
    /// Codex home to read (default: ~/.codex). It is never modified.
    #[arg(long)]
    codex_home: Option<PathBuf>,
    /// Only threads active on or after this date (YYYY-MM-DD).
    #[arg(long)]
    since: Option<NaiveDate>,
    /// Import at most this many threads, newest first.
    #[arg(long)]
    limit: Option<usize>,
    /// Report what would be imported without writing anything.
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let codex_home = match args.codex_home {
        Some(path) => path,
        None => PathBuf::from(std::env::var("HOME")?).join(".codex"),
    };
    let plan = codex_antfly_import::load(&codex_home).await?;
    let since: Option<DateTime<Utc>> = args
        .since
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|time| time.and_utc());
    let mut threads: Vec<_> = plan
        .threads
        .iter()
        .filter(|thread| {
            since.is_none_or(|since| thread.updated_at().is_some_and(|updated| updated >= since))
        })
        .collect();
    threads.sort_by_key(|thread| std::cmp::Reverse(thread.updated_at()));
    if let Some(limit) = args.limit {
        threads.truncate(limit);
    }
    let paginated = threads
        .iter()
        .filter(|thread| thread.history_mode() == ThreadHistoryMode::Paginated)
        .count();
    println!(
        "{} rollout files, {} threads; selected {} ({paginated} paginated, {} archived); {} skipped",
        plan.rollout_files,
        plan.threads.len(),
        threads.len(),
        threads.iter().filter(|thread| thread.archived()).count(),
        plan.skipped.len(),
    );
    for skipped in plan.skipped.iter().take(10) {
        println!("  skipped {}: {}", skipped.path.display(), skipped.reason);
    }
    if args.dry_run {
        // Materialize a few to validate lineage flattening without writing.
        for thread in threads.iter().take(5) {
            let imported = thread.materialize().await?;
            println!(
                "  {} {:?} {} items{}",
                imported.created.thread_id,
                imported.created.history_mode,
                imported.items.len(),
                imported
                    .patch
                    .preview
                    .as_deref()
                    .map(|preview| format!(": {}", preview.chars().take(60).collect::<String>()))
                    .unwrap_or_default()
            );
        }
        return Ok(());
    }
    anyhow::bail!("writing to Antfly is not wired up yet; use --dry-run")
}
