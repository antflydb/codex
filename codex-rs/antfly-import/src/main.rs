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
    /// Destination database (default: <codex-home>/antfly.aflite).
    #[arg(long)]
    to: Option<PathBuf>,
    /// Import into a remote Antfly instance instead (base URL).
    #[arg(long)]
    url: Option<String>,
    /// PostgreSQL URL of the remote instance's SQL listener (required with
    /// --url), e.g. postgres://codex:secret@host:5432/default.
    #[arg(long)]
    sql_url: Option<String>,
    /// Environment variable holding the remote bearer token.
    #[arg(long)]
    api_key_env: Option<String>,
    /// Skip dense indexing; full-text search still works.
    #[arg(long)]
    no_semantic: bool,
    /// Overwrite threads that were already imported.
    #[arg(long)]
    replace: bool,
    /// After importing, search the destination and print matching threads.
    #[arg(long)]
    search: Option<String>,
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
    let config = codex_antfly::AntflyConfig::from_toml(codex_antfly::AntflyTomlSettings {
        codex_home: codex_home.clone(),
        path: if args.url.is_some() {
            None
        } else {
            args.to.clone()
        },
        url: args.url.clone(),
        sql_url: args.sql_url.clone(),
        api_key_env: args.api_key_env.clone(),
        semantic_search: args.no_semantic.then_some(false),
        ..Default::default()
    })?;
    let store = codex_thread_store::AntflyThreadStore::new(codex_antfly::shared(&config));
    let total = threads.len();
    let (mut imported, mut present, mut failed) = (0usize, 0usize, 0usize);
    for (index, thread) in threads.iter().enumerate() {
        let result = async {
            let thread = thread.materialize().await?;
            let outcome = store
                .import_thread(codex_thread_store::ImportThreadParams {
                    created: thread.created,
                    items: thread.items,
                    patch: thread.patch,
                    archived_at: thread.archived_at,
                    section: thread
                        .section
                        .map(|section| codex_thread_store::ImportSection {
                            id: section.id,
                            name: section.name,
                            position: section.position,
                        }),
                    legacy_rollout_path: Some(thread.legacy_rollout_path),
                    replace: args.replace,
                })
                .await?;
            anyhow::Ok(outcome)
        }
        .await;
        match result {
            Ok(codex_thread_store::ImportOutcome::Imported) => imported += 1,
            Ok(codex_thread_store::ImportOutcome::AlreadyPresent) => present += 1,
            Err(err) => {
                failed += 1;
                eprintln!("  failed {}: {err}", thread.thread_id());
            }
        }
        if (index + 1) % 25 == 0 || index + 1 == total {
            println!(
                "  {}/{total} (imported {imported}, already present {present}, failed {failed})",
                index + 1
            );
        }
    }
    println!("done: imported {imported}, already present {present}, failed {failed}");
    if let Some(term) = &args.search {
        use codex_thread_store::ThreadStore;
        let page = store
            .search_threads(codex_thread_store::SearchThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: codex_thread_store::ThreadSortKey::UpdatedAt,
                sort_direction: codex_thread_store::SortDirection::Desc,
                allowed_sources: Vec::new(),
                archived: false,
                search_term: term.clone(),
            })
            .await?;
        println!("search {term:?}: {} threads", page.items.len());
        for result in page.items {
            println!(
                "  {} {}\n      {}",
                result.thread.thread_id,
                result.thread.preview.chars().take(70).collect::<String>(),
                result.snippet
            );
        }
    }
    if failed > 0 {
        anyhow::bail!("{failed} threads failed to import");
    }
    Ok(())
}
