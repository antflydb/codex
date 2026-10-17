//! `codex knowledge`: inspect and backfill the Antfly knowledge graph.
//!
//! The live claim pipeline extracts claims from turns as they finish. This
//! command reaches the history it never saw (imported and older threads)
//! on request, with a dry run and a request budget, and runs concept
//! consolidation on demand. It opens the embedded database itself, so the
//! app-server daemon must not hold it.

use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use chrono::NaiveDate;
use codex_antfly_extension::knowledge_admin;
use codex_antfly_extension::knowledge_admin::BackfillOptions;
use codex_antfly_extension::knowledge_admin::JobOutcome;
use codex_config::LoaderOverrides;
use codex_core::config::Config;
use codex_core::config::ThreadStoreConfig;
use codex_home::CodexHomeUserInstructionsProvider;
use codex_utils_cli::CliConfigOverrides;

use crate::cloud_config;

/// Inspect and backfill the Antfly knowledge graph (claims and concepts).
#[derive(Debug, clap::Parser)]
pub struct KnowledgeCli {
    #[clap(flatten)]
    pub config_overrides: CliConfigOverrides,

    #[command(subcommand)]
    pub subcommand: KnowledgeSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum KnowledgeSubcommand {
    /// Show claim extraction progress, claims and concepts.
    Status,
    /// Extract and settle claims from history the live pipeline never saw.
    Backfill(BackfillArgs),
    /// Merge concepts that name the same thing now.
    Consolidate,
    /// Describe concepts from the claims about them, with the local
    /// describe model.
    Describe(DescribeArgs),
}

#[derive(Debug, clap::Args)]
pub struct DescribeArgs {
    /// Describe at most this many concepts; rerun to continue.
    #[arg(long, default_value_t = 50)]
    limit: usize,
}

#[derive(Debug, clap::Args)]
pub struct BackfillArgs {
    /// Only threads updated on or after this date (YYYY-MM-DD).
    #[arg(long)]
    since: Option<NaiveDate>,
    /// Only these threads (repeatable), whatever their age or source.
    #[arg(long = "thread")]
    threads: Vec<String>,
    /// At most this many threads, newest first.
    #[arg(long, default_value_t = knowledge_admin::DEFAULT_LIMIT)]
    limit: usize,
    /// Stop after this many extraction requests; rerun to continue.
    #[arg(long)]
    max_requests: Option<usize>,
    /// List what would be extracted, and its cost, without doing it.
    #[arg(long)]
    dry_run: bool,
}

impl KnowledgeCli {
    pub async fn run(
        self,
        loader_overrides: LoaderOverrides,
        arg0_paths: codex_arg0::Arg0DispatchPaths,
    ) -> Result<()> {
        let mut config =
            cloud_config::load_config(&self.config_overrides, loader_overrides).await?;
        // The host session needs these to start, as `codex exec` does.
        config.codex_self_exe = arg0_paths.codex_self_exe;
        config.codex_linux_sandbox_exe = arg0_paths.codex_linux_sandbox_exe;
        let ThreadStoreConfig::Antfly(antfly_config) = &config.experimental_thread_store else {
            anyhow::bail!(
                "the knowledge graph needs the Antfly thread store \
                 ([experimental_thread_store] type = \"antfly\")"
            );
        };
        let antfly = codex_antfly::shared(antfly_config);
        match self.subcommand {
            KnowledgeSubcommand::Status => print_status(&antfly).await,
            KnowledgeSubcommand::Backfill(args) => backfill(&antfly, config, args).await,
            KnowledgeSubcommand::Describe(args) => {
                let report = knowledge_admin::describe(&antfly, args.limit)
                    .await
                    .context(
                        "describing concepts (is the app-server daemon holding the database?)",
                    )?;
                println!(
                    "examined {} concepts: {} described, {} unchanged, {} without current claims, {} failed",
                    report.examined,
                    report.described,
                    report.unchanged,
                    report.without_evidence,
                    report.failed
                );
                Ok(())
            }
            KnowledgeSubcommand::Consolidate => {
                let (thread_manager, host) = start_host(&config).await?;
                let report = knowledge_admin::consolidate(&thread_manager, &config).await;
                stop_host(&thread_manager, host).await;
                let report = report?;
                println!(
                    "examined {} concepts: {} merged by spelling, {} of {} judged pairs merged",
                    report.examined, report.lexical_merges, report.judged_merges, report.judged
                );
                Ok(())
            }
        }
    }
}

async fn print_status(antfly: &codex_antfly::Antfly) -> Result<()> {
    let status = knowledge_admin::status(antfly)
        .await
        .context("reading the knowledge graph (is the app-server daemon holding the database?)")?;
    println!(
        "threads: {} seen, {} settled, {} with history pending",
        status.progress.threads, status.progress.settled, status.pending_threads
    );
    println!(
        "claims: {} current, {} provisional, {} superseded, {} retracted",
        status.claims.current,
        status.claims.provisional,
        status.claims.superseded,
        status.claims.retracted
    );
    println!(
        "concepts: {} ({} merged into another)",
        status.concepts, status.merged_concepts
    );
    for (thread, failures, error) in &status.progress.failing {
        println!("  failing {thread} ({failures}x): {error}");
    }
    Ok(())
}

async fn backfill(antfly: &codex_antfly::Antfly, config: Config, args: BackfillArgs) -> Result<()> {
    let options = BackfillOptions {
        since_ms: args
            .since
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|time| time.and_utc().timestamp_millis()),
        threads: args.threads,
        limit: args.limit,
    };
    let plans = knowledge_admin::plan(antfly, &options)
        .await
        .context("reading the knowledge graph (is the app-server daemon holding the database?)")?;
    let turns: u64 = plans.iter().filter_map(|plan| plan.pending_turns).sum();
    let unknown = plans
        .iter()
        .filter(|plan| plan.pending_turns.is_none())
        .count();
    println!(
        "{} threads, {} history items, {turns} turns{} pending",
        plans.len(),
        plans.iter().map(|plan| plan.pending_items).sum::<u64>(),
        if unknown > 0 {
            format!(" (+{unknown} threads without a turn count)")
        } else {
            String::new()
        }
    );
    if args.dry_run {
        for plan in &plans {
            let updated = chrono::DateTime::from_timestamp_millis(plan.updated_at_ms)
                .map(|time| time.format("%Y-%m-%d").to_string())
                .unwrap_or_default();
            let turns = plan
                .pending_turns
                .map(|turns| format!("{turns} turns"))
                .unwrap_or_else(|| "turns unknown".to_string());
            println!(
                "  {} {updated} {} items, {turns}",
                plan.thread_id, plan.pending_items
            );
        }
        println!(
            "each extraction request covers one or more turns (up to 60,000 characters), \
             so at most {turns} requests{}",
            if unknown > 0 {
                " plus the uncounted threads"
            } else {
                ""
            }
        );
        return Ok(());
    }
    if plans.is_empty() {
        return Ok(());
    }
    let (thread_manager, host) = start_host(&config).await?;
    let report = knowledge_admin::run(
        &thread_manager,
        &config,
        &plans,
        args.max_requests,
        |plan, outcome| {
            let summary = match outcome {
                JobOutcome::Skipped => {
                    "skipped (held elsewhere, backing off, or memory off)".to_string()
                }
                JobOutcome::NoHost => "no host thread".to_string(),
                JobOutcome::Failed(err) => format!("failed: {err}"),
                JobOutcome::Done {
                    requests,
                    complete: true,
                } => format!("done ({requests} requests)"),
                JobOutcome::Done {
                    requests,
                    complete: false,
                } => {
                    format!("partial ({requests} requests; budget spent)")
                }
            };
            println!("  {} {summary}", plan.thread_id);
        },
    )
    .await;
    stop_host(&thread_manager, host).await;
    let report = report?;
    println!(
        "{} threads done, {} partial, {} skipped, {} failed; {} extraction requests",
        report.completed,
        report.partial,
        report.skipped,
        report.failed.len(),
        report.requests
    );
    if report.partial > 0 {
        println!("rerun to continue where the budget stopped");
    }
    Ok(())
}

/// An ephemeral thread whose session attributes the model requests, as the
/// live pipeline attributes them to a loaded thread.
async fn start_host(
    config: &Config,
) -> Result<(Arc<codex_core::ThreadManager>, codex_core::NewThread)> {
    let mut host_config = config.clone();
    host_config.ephemeral = true;
    let user_instructions_provider = Arc::new(CodexHomeUserInstructionsProvider::new(
        config.codex_home.clone(),
    ));
    let extensions = codex_extension_api::ExtensionRegistryBuilder::new().build();
    codex_core::start_standalone_thread(
        host_config,
        /*state_db*/ None,
        Arc::new(extensions),
        user_instructions_provider,
    )
    .await
    .context(
        "starting a session for model requests (is the app-server daemon holding the database?)",
    )
}

async fn stop_host(thread_manager: &codex_core::ThreadManager, host: codex_core::NewThread) {
    let _ = host.thread.shutdown_and_wait().await;
    let _ = thread_manager.remove_thread(&host.thread_id).await;
}
