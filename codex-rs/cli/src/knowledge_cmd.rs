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
    /// Describe, merge and link concepts now.
    Consolidate(ConsolidateArgs),
    /// Describe concepts from the claims about them, with the local
    /// describe model.
    Describe(DescribeArgs),
    /// Compare decide models on the labeled questions Codex has logged.
    Benchmark(BenchmarkArgs),
    /// Write candidate concept pairs (JSONL) for labeling merges and
    /// relations.
    ExportPairs(ExportPairsArgs),
    /// List the links between concepts (is_a, part_of, alternative_to).
    Links,
    /// List the concepts consolidation merged, and what into.
    Merges,
    /// Remove the links between two concepts, by name or `concept/` key,
    /// and keep them from being linked again.
    Unlink(UnlinkArgs),
    /// Undo a merge: the named concept (by name or `concept/` key) gets its
    /// own key back, and is pinned so it is never merged into the same
    /// concept again.
    Unmerge(UnmergeArgs),
    /// Suppress a concept that is noise (a generic word): its mentions stop
    /// minting it and its graph edges are removed.
    Ignore(ConceptArg),
    /// Keep a concept: undo `ignore`, or reject a proposal to ignore it.
    #[command(visible_alias = "keep")]
    Unignore(ConceptArg),
    /// List ignored concepts, and why they are ignored.
    Ignored,
    /// List open proposals: concepts generic screening proposes to ignore,
    /// and pairs consolidation was unsure were the same.
    Proposals,
    /// Accept the open proposal for a concept (ignore it, or merge it).
    Accept(ConceptArg),
    /// Reject the open proposal for a concept; a rejected merge pins the
    /// pair distinct.
    Reject(ConceptArg),
}

#[derive(Debug, clap::Args)]
pub struct UnmergeArgs {
    concept: String,
    /// Allow consolidation to merge the pair again later.
    #[arg(long)]
    allow_remerge: bool,
}

#[derive(Debug, clap::Args)]
pub struct ConceptArg {
    concept: String,
}

#[derive(Debug, clap::Args)]
pub struct ConsolidateArgs {
    /// Judge every unmerged concept again, including ones examined before
    /// they had descriptions; runs passes until none are left.
    #[arg(long)]
    rejudge: bool,
}

#[derive(Debug, clap::Args)]
pub struct UnlinkArgs {
    a: String,
    b: String,
}

#[derive(Debug, clap::Args)]
pub struct ExportPairsArgs {
    /// Name and description neighbors considered per concept.
    #[arg(long, default_value_t = 5)]
    neighbors: usize,
    /// Output file; standard output when omitted.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct BenchmarkArgs {
    /// Decide model to evaluate (repeatable); defaults to the configured
    /// decide and approval models.
    #[arg(long = "model")]
    models: Vec<String>,
    /// At most this many logged approvals and extraction turns.
    #[arg(long, default_value_t = 200)]
    limit: usize,
    /// Also benchmark the extraction model as the concept judge (a few
    /// model requests on your account).
    #[arg(long)]
    llm_judge: bool,
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
            KnowledgeSubcommand::Unlink(args) => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let names = knowledge
                    .concepts_by_key()
                    .await
                    .context("reading concepts")?;
                let resolve = |given: &str| -> Option<String> {
                    if names.contains_key(given) {
                        return Some(given.to_string());
                    }
                    names
                        .values()
                        .find(|concept| concept.name.eq_ignore_ascii_case(given))
                        .map(|concept| concept.key.clone())
                };
                let (Some(a), Some(b)) = (resolve(&args.a), resolve(&args.b)) else {
                    anyhow::bail!("no concept named {:?} or {:?}", args.a, args.b);
                };
                if knowledge.unlink_concepts(&a, &b).await? {
                    println!("unlinked {} and {}", args.a, args.b);
                } else {
                    println!(
                        "{} and {} were not linked; they won't be linked later",
                        args.a, args.b
                    );
                }
                Ok(())
            }
            KnowledgeSubcommand::Merges => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let names = knowledge
                    .concepts_by_key()
                    .await
                    .context("reading concepts")?;
                let name = |key: &str| {
                    names
                        .get(key)
                        .map(|concept| concept.name.clone())
                        .unwrap_or_else(|| key.to_string())
                };
                // Antfly's redirects are authoritative; Codex's own records
                // stand in when the backend cannot report them.
                let merges: Vec<(String, String)> = match knowledge.concept_curation().await? {
                    Some(curation) => curation.redirects.into_iter().collect(),
                    None => knowledge.concept_merges().await?,
                };
                for (merged, into) in merges {
                    println!("{} -> {}", name(&merged), name(&into));
                }
                Ok(())
            }
            KnowledgeSubcommand::Ignored => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let Some(curation) = knowledge.concept_curation().await? else {
                    println!("this Antfly backend can't suppress concepts");
                    return Ok(());
                };
                let keys: Vec<String> = curation.suppressed.iter().cloned().collect();
                print_concepts(&knowledge, &keys, |key, name| {
                    let Some(origin) = curation.suppression_origins.get(key) else {
                        return "ignored".to_string();
                    };
                    let screened = origin.actor.as_deref()
                        == Some(codex_antfly::knowledge::GENERIC_SCREEN_ACTOR);
                    match (origin.origin.as_str(), &origin.cascade_from) {
                        ("cascade", Some(from)) => format!("ignored with {}", name(from)),
                        ("review", _) => "screened as generic, accepted".to_string(),
                        _ if screened => "screened as generic".to_string(),
                        ("config", _) => "ignored by configuration".to_string(),
                        _ => "ignored".to_string(),
                    }
                })
                .await
            }
            KnowledgeSubcommand::Proposals => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let proposals = knowledge.open_proposals().await?;
                if proposals.is_empty() {
                    println!("no open proposals");
                    return Ok(());
                }
                let keys: Vec<String> = proposals.iter().map(|p| p.key.clone()).collect();
                let into: std::collections::HashMap<String, String> = proposals
                    .iter()
                    .filter_map(|p| Some((p.key.clone(), p.into.clone()?)))
                    .collect();
                print_concepts(&knowledge, &keys, |key, name| match into.get(key) {
                    Some(target) => format!("maybe the same as {}", name(target)),
                    None => "looks generic".to_string(),
                })
                .await?;
                println!(
                    "\naccept with `codex knowledge accept <concept>`, reject with `codex knowledge reject <concept>`"
                );
                Ok(())
            }
            KnowledgeSubcommand::Accept(args) => decide(&antfly, &args.concept, true).await,
            KnowledgeSubcommand::Reject(args) => decide(&antfly, &args.concept, false).await,
            KnowledgeSubcommand::Unmerge(args) => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let key = concept_key_arg(&knowledge, &args.concept).await?;
                if knowledge.unmerge_concept(&key, !args.allow_remerge).await? {
                    println!(
                        "unmerged {}{}",
                        args.concept,
                        if args.allow_remerge {
                            ""
                        } else {
                            "; it won't be merged into the same concept again"
                        }
                    );
                } else {
                    println!("this Antfly backend can't unmerge concepts");
                }
                Ok(())
            }
            KnowledgeSubcommand::Ignore(args) => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let key = concept_key_arg(&knowledge, &args.concept).await?;
                let user = codex_antfly::knowledge::USER_ACTOR;
                // Accepting an open proposal keeps the screening's provenance.
                let absorbed = match knowledge.open_suppress_proposal(&key).await? {
                    Some(proposal) => knowledge.decide_proposal(&proposal, true, user).await?,
                    None => knowledge.suppress_concept(&key, user, None).await?,
                };
                let Some(absorbed) = absorbed else {
                    println!("this Antfly backend can't suppress concepts");
                    return Ok(());
                };
                println!(
                    "ignoring {}; its graph edges are removed on replay",
                    args.concept
                );
                if !absorbed.is_empty() {
                    println!(
                        "also ignoring what was merged into it: {}",
                        absorbed.join(", ")
                    );
                }
                Ok(())
            }
            KnowledgeSubcommand::Unignore(args) => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let key = concept_key_arg(&knowledge, &args.concept).await?;
                let user = codex_antfly::knowledge::USER_ACTOR;
                if let Some(proposal) = knowledge.open_suppress_proposal(&key).await? {
                    knowledge.decide_proposal(&proposal, false, user).await?;
                    println!("keeping {}; it won't be proposed again", args.concept);
                    return Ok(());
                }
                let suppressed = knowledge
                    .concept_curation()
                    .await?
                    .is_some_and(|curation| curation.suppressed.contains(&key));
                if !suppressed {
                    println!("{} is not ignored", args.concept);
                } else if knowledge.unsuppress_concept(&key, user).await? {
                    println!("keeping {}", args.concept);
                } else {
                    println!("this Antfly backend can't suppress concepts");
                }
                Ok(())
            }
            KnowledgeSubcommand::Links => {
                let knowledge = antfly
                    .knowledge()
                    .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                let names = knowledge
                    .concepts_by_key()
                    .await
                    .context("reading concepts")?;
                let name = |key: &str| {
                    names
                        .get(key)
                        .map(|concept| concept.name.clone())
                        .unwrap_or_else(|| key.to_string())
                };
                for (from, kind, to) in knowledge.concept_links().await? {
                    println!("{} {kind} {}", name(&from), name(&to));
                }
                Ok(())
            }
            KnowledgeSubcommand::ExportPairs(args) => {
                let pairs = knowledge_admin::export_pairs(&antfly, args.neighbors)
                    .await
                    .context(
                        "exporting concept pairs (is the app-server daemon holding the database?)",
                    )?;
                let lines: Vec<String> = pairs.iter().map(ToString::to_string).collect();
                let text = lines.join("\n") + "\n";
                match &args.out {
                    Some(path) => std::fs::write(path, text)
                        .with_context(|| format!("writing {}", path.display()))?,
                    None => print!("{text}"),
                }
                eprintln!("{} candidate pairs", pairs.len());
                Ok(())
            }
            KnowledgeSubcommand::Benchmark(args) => {
                let mut models = args.models;
                if models.is_empty() {
                    models.push(antfly.config().decide_model.clone());
                    let approvals = antfly.config().approval_decide_model().to_string();
                    if !models.contains(&approvals) {
                        models.push(approvals);
                    }
                }
                let mut results = knowledge_admin::benchmark(&antfly, &models, args.limit)
                    .await
                    .context("benchmarking (is the app-server daemon holding the database?)")?;
                if args.llm_judge {
                    let (thread_manager, host) = start_host(&config).await?;
                    let judged = knowledge_admin::benchmark_llm(&thread_manager, &config).await;
                    stop_host(&thread_manager, host).await;
                    results.extend(judged?);
                }
                print_benchmark(&results);
                Ok(())
            }
            KnowledgeSubcommand::Describe(args) => {
                let report = knowledge_admin::describe(&antfly, args.limit)
                    .await
                    .context(
                        "describing concepts (is the app-server daemon holding the database?)",
                    )?;
                println!(
                    "examined {} concepts: {} described, {} unchanged, {} without claims or notes, {} failed",
                    report.examined,
                    report.described,
                    report.unchanged,
                    report.without_evidence,
                    report.failed
                );
                Ok(())
            }
            KnowledgeSubcommand::Consolidate(args) => {
                if args.rejudge {
                    let knowledge = antfly
                        .knowledge()
                        .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
                    let forgotten = knowledge.forget_concept_reviews().await.context(
                        "resetting concept reviews (is the app-server daemon holding the database?)",
                    )?;
                    println!("judging {forgotten} unmerged concepts again");
                }
                let (thread_manager, host) = start_host(&config).await?;
                // One pass examines a bounded number of concepts; a rejudge
                // runs passes until none are left.
                let mut outcome = Ok(());
                for _ in 0..if args.rejudge { 100 } else { 1 } {
                    match knowledge_admin::consolidate(&thread_manager, &config).await {
                        Ok((screened, report, relations)) => {
                            println!(
                                "screened {} concepts: {} look generic, {} ignored",
                                screened.screened, screened.generic, screened.suppressed
                            );
                            if screened.generic > screened.suppressed {
                                println!("review them with `codex knowledge proposals`");
                            }
                            println!(
                                "examined {} concepts: {} merged by spelling, {} of {} judged pairs merged, {} proposed",
                                report.examined,
                                report.lexical_merges,
                                report.judged_merges,
                                report.judged,
                                report.proposed_merges
                            );
                            if report.proposed_merges > 0 {
                                println!("review proposed merges with `codex knowledge proposals`");
                            }
                            println!(
                                "judged {} concept pairs for relations: {} linked",
                                relations.judged, relations.linked
                            );
                            if screened.screened == 0
                                && report.examined == 0
                                && relations.judged == 0
                            {
                                break;
                            }
                        }
                        Err(err) => {
                            outcome = Err(err);
                            break;
                        }
                    }
                }
                stop_host(&thread_manager, host).await;
                outcome?;
                Ok(())
            }
        }
    }
}

/// Accepts or rejects the open proposal for `concept`.
async fn decide(antfly: &codex_antfly::Antfly, concept: &str, accept: bool) -> Result<()> {
    let knowledge = antfly
        .knowledge()
        .ok_or_else(|| anyhow::anyhow!("the knowledge graph is off"))?;
    let key = concept_key_arg(&knowledge, concept).await?;
    let open = knowledge.open_proposals().await?;
    let Some(proposal) = open.iter().find(|proposal| proposal.key == key) else {
        println!("no open proposal for {concept}");
        return Ok(());
    };
    let user = codex_antfly::knowledge::USER_ACTOR;
    knowledge.decide_proposal(proposal, accept, user).await?;
    let what = match (proposal.action.as_str(), accept) {
        ("merge", true) => "merged",
        ("merge", false) => "kept apart; the pair won't be merged",
        ("suppress", true) => "ignored",
        (_, true) => "applied",
        (_, false) => "kept; it won't be proposed again",
    };
    println!("{concept}: {what}");
    Ok(())
}

/// Prints `keys` as `name (why): description`, `why` given the key and a
/// key-to-name lookup.
async fn print_concepts(
    knowledge: &codex_antfly::knowledge::Knowledge<'_>,
    keys: &[String],
    why: impl Fn(&str, &dyn Fn(&str) -> String) -> String,
) -> Result<()> {
    let names = knowledge
        .concepts_by_key()
        .await
        .context("reading concepts")?;
    let name = |key: &str| {
        names
            .get(key)
            .map(|concept| concept.name.clone())
            .unwrap_or_else(|| key.to_string())
    };
    let descriptions = knowledge.concept_descriptions(keys).await?;
    for key in keys {
        let why = why(key, &name);
        match descriptions.get(key) {
            Some(description) => println!("{} ({why}): {description}", name(key)),
            None => println!("{} ({why})", name(key)),
        }
    }
    Ok(())
}

/// The concept key a user means: a `concept/` key as given, otherwise the
/// key the resolver mints for the name before any merge redirect, so a
/// merged-away concept can still be named.
async fn concept_key_arg(
    knowledge: &codex_antfly::knowledge::Knowledge<'_>,
    given: &str,
) -> Result<String> {
    if given.starts_with(codex_antfly::knowledge::CONCEPT_PREFIX) {
        return Ok(given.to_string());
    }
    if let Some(key) = knowledge
        .concept_minted_key(given)
        .await
        .context("rendering the concept key (is the app-server daemon holding the database?)")?
    {
        return Ok(key);
    }
    knowledge
        .concepts()
        .await?
        .into_iter()
        .find(|concept| concept.name.eq_ignore_ascii_case(given))
        .map(|concept| concept.key)
        .ok_or_else(|| anyhow::anyhow!("no concept named {given:?}"))
}

fn print_benchmark(results: &[codex_antfly::bench::TaskResult]) {
    let percent = |value: Option<f64>| {
        value
            .map(|value| format!("{:.0}%", value * 100.0))
            .unwrap_or_else(|| "-".to_string())
    };
    for result in results {
        println!(
            "{} on {}: {} samples ({} positive), AUC {}, median {} ms{}",
            result.model,
            result.task,
            result.samples.len(),
            result.positives(),
            result
                .auc()
                .map(|auc| format!("{auc:.2}"))
                .unwrap_or_else(|| "-".to_string()),
            result.median_latency_ms().unwrap_or_default(),
            if result.failures > 0 {
                format!(
                    ", {} failed ({})",
                    result.failures,
                    result.first_error.as_deref().unwrap_or("")
                )
            } else {
                String::new()
            }
        );
        for note in &result.notes {
            println!("  {note}");
        }
        for threshold in [0.5, 0.8] {
            let at = result.at(threshold);
            println!(
                "  at {threshold:.1}: precision {} recall {}",
                percent(at.precision()),
                percent(at.recall())
            );
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
    if status.triage.turns > 0 {
        println!("triage: {} extracted turns scored", status.triage.turns);
        for point in &status.triage.points {
            println!(
                "  at {:.1}: keeps {} of {} turns with claims, skips {}",
                point.threshold, point.kept_with_claims, point.with_claims, point.skipped
            );
        }
    }
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
