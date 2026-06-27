use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};
use quant_stack_core::alpha::{self, AlphaEvalConfig};
use quant_stack_core::report_model;
use serde_json::Value;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::Duration as StdDuration;
use tracing::{info, warn};

mod us_daily;

#[derive(Parser)]
#[command(name = "quant-stack", about = "Unified quant stack orchestration")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create or verify the unified alpha/report-model schema.
    Migrate {
        #[arg(long, default_value = "data/strategy_backtest_history.duckdb")]
        db: PathBuf,
        #[arg(long)]
        check: bool,
    },
    /// Alpha maturity and execution bulletin commands.
    Alpha {
        #[command(subcommand)]
        command: AlphaCommand,
    },
    /// Structured report model commands.
    Report {
        #[command(subcommand)]
        command: ReportCommand,
    },
    /// Unified daily orchestration wrapper.
    Daily(DailyArgs),
    /// US equities full daily pipeline: data, split, agents, report, delivery.
    UsDaily(us_daily::UsDailyArgs),
}

#[derive(Subcommand)]
enum AlphaCommand {
    /// Evaluate policy maturity and emit the daily alpha bulletin.
    Evaluate(AlphaEvaluateArgs),
}

#[derive(Subcommand)]
enum ReportCommand {
    /// Materialize report model JSON files from the history DB.
    Model(ReportModelArgs),
    /// Enforce shared report model status on an existing final report.
    Stamp(ReportStampArgs),
}

#[derive(Parser, Debug, Clone)]
struct AlphaEvaluateArgs {
    #[arg(long)]
    date: String,
    #[arg(long, default_value = "us,cn")]
    markets: String,
    #[arg(long, default_value_t = 30)]
    lookback_days: i64,
    #[arg(long, default_value = "data/strategy_backtest_history.duckdb")]
    history_db: PathBuf,
    #[arg(long, default_value = "reports/review_dashboard/strategy_backtest")]
    output_root: PathBuf,
    #[arg(long, default_value = "quant-research-v1/data/quant.duckdb")]
    us_db: PathBuf,
    #[arg(long, default_value = "quant-research-cn/data/quant_cn_report.duckdb")]
    cn_db: PathBuf,
    #[arg(long, default_value_t = 3)]
    us_horizon_days: i64,
    #[arg(long, default_value_t = 2)]
    cn_horizon_days: i64,
    #[arg(long)]
    auto_select: bool,
    #[arg(long)]
    emit_bulletin: bool,
    #[arg(long)]
    no_project_copies: bool,
}

#[derive(Parser, Debug, Clone)]
struct ReportModelArgs {
    #[arg(long)]
    date: String,
    #[arg(long, default_value = "us,cn")]
    markets: String,
    #[arg(long, default_value = "post")]
    session: String,
    #[arg(long, default_value = "data/strategy_backtest_history.duckdb")]
    history_db: PathBuf,
    #[arg(long, default_value = "reports")]
    reports_dir: PathBuf,
}

#[derive(Parser, Debug, Clone)]
struct ReportStampArgs {
    #[arg(long)]
    date: String,
    #[arg(long)]
    market: String,
    #[arg(long, default_value = "post")]
    session: String,
    #[arg(long)]
    report: PathBuf,
    #[arg(long, default_value = ".")]
    stack_root: PathBuf,
}

#[derive(Parser, Debug, Clone)]
struct DailyArgs {
    #[arg(long)]
    date: String,
    #[arg(long, default_value = "us,cn")]
    markets: String,
    #[arg(long, default_value = "post")]
    session: String,
    #[arg(long, default_value_t = 30)]
    lookback_days: i64,
    #[arg(long)]
    run_producers: bool,
    #[arg(long)]
    skip_fetch: bool,
    #[arg(long)]
    with_narrative: bool,
    #[arg(long)]
    send_reports: bool,
    #[arg(long, value_enum, default_value_t = DeliveryMode::Test, env = "QUANT_DELIVERY_MODE")]
    delivery_mode: DeliveryMode,
    #[arg(long, env = "QUANT_TEST_RECIPIENT")]
    test_recipient: Option<String>,
    #[arg(long)]
    delivery_dry_run: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, default_value = ".")]
    stack_root: PathBuf,
    #[arg(long, default_value = "data/strategy_backtest_history.duckdb")]
    history_db: PathBuf,
    #[arg(long, default_value = "reports/review_dashboard/strategy_backtest")]
    output_root: PathBuf,
    #[arg(long, default_value = "quant-research-v1/data/quant.duckdb")]
    us_db: PathBuf,
    #[arg(long, default_value = "quant-research-cn/data/quant_cn_report.duckdb")]
    cn_db: PathBuf,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum DeliveryMode {
    Test,
    Prod,
}

impl DeliveryMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            DeliveryMode::Test => "test",
            DeliveryMode::Prod => "prod",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DailyPipelineState {
    CnAkshareReady,
    ProducersReady,
    CnFactorImported,
    CnPreAlphaRendered,
    AlphaEvaluated,
    ReportModelWritten,
    CnBulletinRendered,
    CnPayloadsFinalized,
    CnChartsGenerated,
    CnNarrativeRendered,
    NarrativeExternal,
    DeliveryReady,
    Delivered,
    CnReviewMaintenance,
}

impl DailyPipelineState {
    fn as_str(self) -> &'static str {
        match self {
            Self::CnAkshareReady => "cn_akshare_ready",
            Self::ProducersReady => "producers_ready",
            Self::CnFactorImported => "cn_factor_imported",
            Self::CnPreAlphaRendered => "cn_pre_alpha_rendered",
            Self::AlphaEvaluated => "alpha_evaluated",
            Self::ReportModelWritten => "report_model_written",
            Self::CnBulletinRendered => "cn_bulletin_rendered",
            Self::CnPayloadsFinalized => "cn_payloads_finalized",
            Self::CnChartsGenerated => "cn_charts_generated",
            Self::CnNarrativeRendered => "cn_narrative_rendered",
            Self::NarrativeExternal => "narrative_external",
            Self::DeliveryReady => "delivery_ready",
            Self::Delivered => "delivered",
            Self::CnReviewMaintenance => "cn_review_maintenance",
        }
    }
}

struct ChildProcessGuard {
    child: Child,
    label: &'static str,
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        info!(label = self.label, "stopped background process");
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("quant_stack=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Migrate { db, check } => {
            alpha::migrate(&db, check)?;
            println!(
                "schema {}: {}",
                if check { "checked" } else { "migrated" },
                db.display()
            );
        }
        Commands::Alpha { command } => match command {
            AlphaCommand::Evaluate(args) => {
                let output_root = args.output_root.clone();
                let bulletin = run_alpha_evaluate(args)?;
                println!(
                    "alpha evaluate complete: {} -> {}",
                    bulletin.as_of,
                    output_root.join(&bulletin.as_of).display()
                );
            }
        },
        Commands::Report { command } => match command {
            ReportCommand::Model(args) => {
                let markets = parse_markets(&args.markets);
                let n = report_model::write_models_from_history(
                    &args.history_db,
                    &args.date,
                    &markets,
                    &args.session,
                    &args.reports_dir,
                )?;
                println!("report models written: {n}");
            }
            ReportCommand::Stamp(args) => {
                let stack_root = args
                    .stack_root
                    .canonicalize()
                    .unwrap_or_else(|_| args.stack_root.clone());
                enforce_shared_report_model_status(
                    &stack_root,
                    &args.market,
                    &args.date,
                    &args.session,
                    &args.report,
                )?;
                println!("report stamped: {}", args.report.display());
            }
        },
        Commands::Daily(args) => run_daily(args)?,
        Commands::UsDaily(args) => us_daily::run(args)?,
    }
    Ok(())
}

fn run_alpha_evaluate(args: AlphaEvaluateArgs) -> Result<quant_stack_core::alpha::AlphaBulletin> {
    let as_of = NaiveDate::parse_from_str(&args.date, "%Y-%m-%d")
        .with_context(|| format!("invalid --date {}", args.date))?;
    let config = AlphaEvalConfig {
        as_of,
        markets: parse_markets(&args.markets),
        lookback_days: args.lookback_days,
        auto_select: args.auto_select,
        emit_bulletin: args.emit_bulletin,
        history_db: args.history_db,
        output_root: args.output_root,
        us_db: args.us_db,
        cn_db: args.cn_db,
        us_horizon_days: args.us_horizon_days,
        cn_horizon_days: args.cn_horizon_days,
        write_project_copies: !args.no_project_copies,
    };
    alpha::evaluate(&config)
}

fn run_daily(args: DailyArgs) -> Result<()> {
    let markets = parse_markets(&args.markets);
    let has_cn = markets.iter().any(|m| m == "cn");
    let stack_root = args
        .stack_root
        .canonicalize()
        .unwrap_or_else(|_| args.stack_root.clone());
    if !args.dry_run {
        std::env::set_current_dir(&stack_root)
            .with_context(|| format!("failed to enter stack root {}", stack_root.display()))?;
    }
    if args.send_reports
        && args.delivery_mode == DeliveryMode::Test
        && args.test_recipient.is_none()
        && std::env::var("QUANT_TEST_RECIPIENT")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .is_none()
    {
        anyhow::bail!(
            "test delivery requires --test-recipient or QUANT_TEST_RECIPIENT; refusing to fall back to production recipients"
        );
    }
    let _cn_bridge = if has_cn && args.run_producers && !args.dry_run {
        let bridge = ensure_cn_akshare_bridge(&stack_root)?;
        enter_daily_state(DailyPipelineState::CnAkshareReady, &args, &markets);
        bridge
    } else {
        None
    };
    if has_cn && !args.dry_run {
        ensure_cn_binary(&stack_root)?;
    }
    if args.run_producers {
        run_producers(
            &stack_root,
            &args.date,
            &args.session,
            &markets,
            args.dry_run,
            args.skip_fetch,
        )?;
    }
    enter_daily_state(DailyPipelineState::ProducersReady, &args, &markets);

    if has_cn {
        prepare_cn_for_alpha(&stack_root, &args, &markets)?;
    }

    let alpha_args = AlphaEvaluateArgs {
        date: args.date.clone(),
        markets: args.markets.clone(),
        lookback_days: args.lookback_days,
        history_db: args.history_db.clone(),
        output_root: args.output_root.clone(),
        us_db: args.us_db.clone(),
        cn_db: args.cn_db.clone(),
        us_horizon_days: 3,
        cn_horizon_days: 2,
        auto_select: true,
        emit_bulletin: true,
        no_project_copies: true,
    };
    if args.dry_run {
        println!(
            "dry-run: would evaluate alpha and write report model for {}",
            args.date
        );
        enter_daily_state(DailyPipelineState::AlphaEvaluated, &args, &markets);
        enter_daily_state(DailyPipelineState::ReportModelWritten, &args, &markets);
    } else {
        run_alpha_evaluate(alpha_args)?;
        enter_daily_state(DailyPipelineState::AlphaEvaluated, &args, &markets);
        let written = report_model::write_models_from_history(
            &args.history_db,
            &args.date,
            &markets,
            &args.session,
            &stack_root.join("reports"),
        )?;
        enter_daily_state(DailyPipelineState::ReportModelWritten, &args, &markets);
        println!("daily core complete: report models written={written}");
    }

    if has_cn {
        finalize_cn_report(&stack_root, &args, &markets)?;
    }

    if args.with_narrative && !has_cn {
        enter_daily_state(DailyPipelineState::NarrativeExternal, &args, &markets);
        warn!("--with-narrative requested; LLM narrator cutover is intentionally left to the report-model phase. Use legacy run_agents wrappers during transition.");
    }
    if args.send_reports {
        enter_daily_state(DailyPipelineState::DeliveryReady, &args, &markets);
        send_reports(&stack_root, &args.date, &args.session, &markets, &args)?;
        enter_daily_state(DailyPipelineState::Delivered, &args, &markets);
    }
    if has_cn {
        run_cn_review_maintenance(&stack_root, &args, &markets)?;
    }
    Ok(())
}

fn enter_daily_state(state: DailyPipelineState, args: &DailyArgs, markets: &[String]) {
    info!(
        state = state.as_str(),
        date = %args.date,
        session = %args.session,
        markets = %markets.join(","),
        delivery_mode = args.delivery_mode.as_str(),
        dry_run = args.dry_run,
        "daily pipeline state"
    );
}

fn run_producers(
    stack_root: &Path,
    date: &str,
    session: &str,
    markets: &[String],
    dry_run: bool,
    cn_skip_fetch: bool,
) -> Result<()> {
    if markets.iter().any(|m| m == "us") {
        let mut cmd = ProcessCommand::new("python3");
        cmd.arg(stack_root.join("quant-research-v1/scripts/run_daily.py"))
            .arg("--date")
            .arg(date)
            .arg("--session")
            .arg(session)
            .current_dir(stack_root.join("quant-research-v1"));
        run_or_print("us producer", cmd, dry_run)?;
    }
    if markets.iter().any(|m| m == "cn") {
        let cn_root = stack_root.join("quant-research-cn");
        let mut cmd = cn_quant_command(&cn_root);
        cmd.arg("run").arg("--date").arg(date);
        if cn_skip_fetch {
            cmd.arg("--skip-fetch");
        }
        cmd.current_dir(&cn_root);
        run_or_print("cn producer", cmd, dry_run)?;
    }
    Ok(())
}

fn ensure_cn_binary(stack_root: &Path) -> Result<()> {
    let cn_root = stack_root.join("quant-research-cn");
    let binary = cn_root.join("target/release/quant-cn");
    if binary.is_file() {
        return Ok(());
    }
    let mut cmd = ProcessCommand::new("cargo");
    cmd.arg("build").arg("--release").current_dir(&cn_root);
    run_or_print("cn release build", cmd, false)
}

fn cn_quant_command(cn_root: &Path) -> ProcessCommand {
    let binary = cn_root.join("target/release/quant-cn");
    if binary.is_file() {
        ProcessCommand::new(binary)
    } else {
        let mut cmd = ProcessCommand::new("cargo");
        cmd.arg("run").arg("--quiet").arg("--release").arg("--");
        cmd
    }
}

fn ensure_cn_akshare_bridge(stack_root: &Path) -> Result<Option<ChildProcessGuard>> {
    if check_akshare_bridge() {
        return Ok(None);
    }
    let cn_root = stack_root.join("quant-research-cn");
    let log_dir = cn_root.join("reports/logs");
    fs::create_dir_all(&log_dir)?;
    let log_file = File::options()
        .create(true)
        .append(true)
        .open(log_dir.join("akshare_bridge.log"))?;
    let log_err = log_file.try_clone()?;
    let child = ProcessCommand::new("python3")
        .arg("-m")
        .arg("uvicorn")
        .arg("akshare_bridge:app")
        .arg("--host")
        .arg("0.0.0.0")
        .arg("--port")
        .arg("8321")
        .current_dir(cn_root.join("bridge"))
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_err))
        .spawn()
        .context("failed to start CN AKShare bridge")?;
    thread::sleep(StdDuration::from_secs(3));
    if check_akshare_bridge() {
        Ok(Some(ChildProcessGuard {
            child,
            label: "cn_akshare_bridge",
        }))
    } else {
        let mut guard = ChildProcessGuard {
            child,
            label: "cn_akshare_bridge",
        };
        let _ = guard.child.kill();
        let _ = guard.child.wait();
        warn!("CN AKShare bridge failed health check; continuing without AKShare bridge");
        Ok(None)
    }
}

fn check_akshare_bridge() -> bool {
    ProcessCommand::new("curl")
        .arg("-sf")
        .arg("http://localhost:8321/health")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn prepare_cn_for_alpha(stack_root: &Path, args: &DailyArgs, markets: &[String]) -> Result<()> {
    let cn_root = stack_root.join("quant-research-cn");
    let factor_lab_root = stack_root.join("factor-lab");
    if args.dry_run {
        println!("dry-run: would import CN Factor Lab factors and refresh CN report ledgers");
        enter_daily_state(DailyPipelineState::CnFactorImported, args, markets);
        enter_daily_state(DailyPipelineState::CnPreAlphaRendered, args, markets);
        return Ok(());
    }

    if factor_lab_root.is_dir() {
        let mut import = ProcessCommand::new("python3");
        import
            .arg("-m")
            .arg("src.mining.export_to_pipeline")
            .arg("--market")
            .arg("cn")
            .arg("--date")
            .arg(&args.date)
            .current_dir(&factor_lab_root);
        run_or_warn("cn factor import", import);
    } else {
        warn!(path = %factor_lab_root.display(), "factor-lab root missing; skipping CN factor import");
    }
    enter_daily_state(DailyPipelineState::CnFactorImported, args, markets);

    let mut render = cn_quant_command(&cn_root);
    render
        .arg("render")
        .arg("--date")
        .arg(&args.date)
        .current_dir(&cn_root);
    run_or_print("cn pre-alpha render", render, false)?;
    cleanup_legacy_cn_report_artifacts(&cn_root, &args.date, &cn_slot(&args.session))?;
    enter_daily_state(DailyPipelineState::CnPreAlphaRendered, args, markets);
    Ok(())
}

fn finalize_cn_report(stack_root: &Path, args: &DailyArgs, markets: &[String]) -> Result<()> {
    let cn_root = stack_root.join("quant-research-cn");
    if args.dry_run {
        println!(
            "dry-run: would refresh CN report ledgers/charts and remove legacy CN report artifacts"
        );
        enter_daily_state(DailyPipelineState::CnBulletinRendered, args, markets);
        enter_daily_state(DailyPipelineState::CnPayloadsFinalized, args, markets);
        enter_daily_state(DailyPipelineState::CnChartsGenerated, args, markets);
        if args.with_narrative || args.send_reports {
            enter_daily_state(DailyPipelineState::CnNarrativeRendered, args, markets);
        }
        return Ok(());
    }

    let mut render = cn_quant_command(&cn_root);
    render
        .arg("render")
        .arg("--date")
        .arg(&args.date)
        .current_dir(&cn_root);
    run_or_print("cn bulletin render", render, false)?;
    enter_daily_state(DailyPipelineState::CnBulletinRendered, args, markets);

    cleanup_legacy_cn_report_artifacts(&cn_root, &args.date, &cn_slot(&args.session))?;
    enter_daily_state(DailyPipelineState::CnPayloadsFinalized, args, markets);

    generate_cn_charts(&cn_root, &args.date, &cn_slot(&args.session));
    enter_daily_state(DailyPipelineState::CnChartsGenerated, args, markets);

    if args.with_narrative || args.send_reports {
        info!("legacy CN agents disabled; Main Strategy V2 replaces the CN daily report");
        enter_daily_state(DailyPipelineState::CnNarrativeRendered, args, markets);
    }
    Ok(())
}

fn cleanup_legacy_cn_report_artifacts(cn_root: &Path, date: &str, slot: &str) -> Result<()> {
    let reports = cn_root.join("reports");
    for section in ["macro", "structural", "events"] {
        remove_file_if_exists(&reports.join(format!("{date}_payload_{section}.md")))?;
        if slot != "daily" {
            remove_file_if_exists(&reports.join(format!("{date}_payload_{section}_{slot}.md")))?;
        }
    }
    remove_file_if_exists(&reports.join(format!("{date}_factor_lab_appendix.md")))?;
    if slot != "daily" {
        remove_file_if_exists(&reports.join(format!("{date}_factor_lab_appendix_{slot}.md")))?;
        remove_file_if_exists(&reports.join(format!("{date}_report_zh.md")))?;
    }
    remove_dir_if_exists(&reports.join(format!("agents-{date}")))?;
    if slot != "daily" {
        remove_dir_if_exists(&reports.join(format!("agents-{date}-{slot}")))?;
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn generate_cn_charts(cn_root: &Path, date: &str, slot: &str) {
    let mut cmd = ProcessCommand::new("python3");
    cmd.arg("scripts/generate_charts.py")
        .arg("--date")
        .arg(date)
        .current_dir(cn_root);
    run_or_warn("cn chart generation", cmd);
    if slot == "daily" {
        return;
    }
    let chart_dir = cn_root.join("reports/charts").join(date);
    let slot_dir = chart_dir.join(slot);
    if fs::create_dir_all(&slot_dir).is_err() {
        return;
    }
    let Ok(entries) = fs::read_dir(&chart_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) == Some("png") {
            if let Some(file_name) = path.file_name() {
                let _ = fs::copy(&path, slot_dir.join(file_name));
            }
        }
    }
}

fn run_cn_review_maintenance(
    stack_root: &Path,
    args: &DailyArgs,
    markets: &[String],
) -> Result<()> {
    let timing = std::env::var("QUANT_CN_REVIEW_BACKFILL_TIMING")
        .unwrap_or_else(|_| "post-email".to_string());
    if matches!(timing.as_str(), "skip" | "off" | "disabled") {
        return Ok(());
    }
    if args.dry_run {
        println!("dry-run: would run CN review maintenance");
        enter_daily_state(DailyPipelineState::CnReviewMaintenance, args, markets);
        return Ok(());
    }
    let days = std::env::var("QUANT_CN_REVIEW_BACKFILL_DAYS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(7);
    let as_of = NaiveDate::parse_from_str(&args.date, "%Y-%m-%d")
        .with_context(|| format!("invalid --date {}", args.date))?;
    let from = as_of - chrono::Duration::days(days);
    let cn_root = stack_root.join("quant-research-cn");
    let mut cmd = cn_quant_command(&cn_root);
    cmd.arg("review-backfill")
        .arg("--date-from")
        .arg(from.to_string())
        .arg("--date-to")
        .arg(&args.date)
        .current_dir(&cn_root);
    run_or_warn("cn review maintenance", cmd);
    enter_daily_state(DailyPipelineState::CnReviewMaintenance, args, markets);
    Ok(())
}

fn enforce_shared_report_model_status(
    stack_root: &Path,
    market: &str,
    date: &str,
    session: &str,
    report: &Path,
) -> Result<()> {
    if !report.is_file() {
        anyhow::bail!(
            "final report missing for shared report model status enforcement: {}",
            report.display()
        );
    }
    let block = shared_report_model_status_block(stack_root, market, date, session)?;
    let text = fs::read_to_string(report)?;
    fs::write(report, upsert_shared_report_model_status(&text, &block))?;
    Ok(())
}

fn upsert_shared_report_model_status(text: &str, block: &str) -> String {
    const HEADING: &str = "## Shared Report Model Status";
    let normalized_block = block.trim_end();
    if let Some(start) = text.find(HEADING) {
        if let Some(separator) = text[start..].find("\n---\n") {
            let end = start + separator + "\n---\n".len();
            let prefix = &text[..start];
            let tail = text[end..].trim_start_matches('\n');
            return format!("{prefix}{normalized_block}\n\n{tail}");
        }
    }
    format!("{normalized_block}\n\n{text}")
}

fn shared_report_model_status_block(
    stack_root: &Path,
    market: &str,
    date: &str,
    session: &str,
) -> Result<String> {
    let model_path = find_shared_report_model(stack_root, market, date)?;
    let text = fs::read_to_string(&model_path)
        .with_context(|| format!("shared report model missing: {}", model_path.display()))?;
    let model: Value = serde_json::from_str(&text)
        .with_context(|| format!("invalid shared report model JSON: {}", model_path.display()))?;
    let alpha = model
        .get("alpha_bulletin")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("shared report model missing alpha_bulletin"))?;
    let ev_status = alpha
        .get("ev_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let selected_policy = alpha
        .get("selected_policy")
        .and_then(Value::as_str)
        .unwrap_or("none");
    let tactical_policy = alpha
        .get("tactical_policy")
        .and_then(Value::as_str)
        .unwrap_or("none");
    let evaluated_through = alpha
        .get("evaluated_through")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let count = |key: &str| {
        alpha
            .get(key)
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0)
    };
    let probation_symbols = section_symbols(alpha, "probation_alpha");
    Ok(format!(
        "## Shared Report Model Status\n\
         - market: `{}`\n\
         - as_of: `{}`\n\
         - session: `{}` (stable alpha model source: `post`)\n\
         - ev_status: `{}`\n\
         - selected_policy: `{}`\n\
         - tactical_policy: `{}`\n\
         - evaluated_through: `{}`\n\
         - section_counts: execution={} probation={} tactical={} options={} recall={} blocked={}\n\
         - probation_symbols: `{}` (trial-only max 0.25R/0.5R, not a formal Fresh Entry)\n\
         - rule: final report must not create formal Fresh Entry Tickets unless `execution_alpha` is non-empty in this shared model; `probation_alpha` is trial-only sizing, not formal execution.\n\
         \n\
         ---\n",
        market,
        date,
        session,
        ev_status,
        selected_policy,
        tactical_policy,
        evaluated_through,
        count("execution_alpha"),
        count("probation_alpha"),
        count("tactical_alpha"),
        count("options_alpha"),
        count("recall_alpha"),
        count("blocked_alpha"),
        probation_symbols,
    ))
}

fn section_symbols(alpha: &serde_json::Map<String, Value>, key: &str) -> String {
    let symbols: Vec<String> = alpha
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("symbol").and_then(Value::as_str))
        .take(5)
        .map(ToString::to_string)
        .collect();
    if symbols.is_empty() {
        "none".to_string()
    } else {
        symbols.join(", ")
    }
}

fn find_shared_report_model(stack_root: &Path, market: &str, date: &str) -> Result<PathBuf> {
    let candidates = [
        stack_root
            .join("reports")
            .join(format!("{date}_report_model_{market}_post.json")),
        stack_root
            .join("reports/review_dashboard/strategy_backtest")
            .join(date)
            .join(format!("report_model_{market}_post.json")),
    ];
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "shared report model missing for {market} {date}; checked root reports and review_dashboard"
            )
        })
}

fn cn_slot(session: &str) -> String {
    match session {
        "pre" | "morning" => "morning".to_string(),
        "post" | "evening" => "evening".to_string(),
        "daily" => "daily".to_string(),
        other => other.to_string(),
    }
}

fn run_or_print(label: &str, mut cmd: ProcessCommand, dry_run: bool) -> Result<()> {
    if dry_run {
        println!("dry-run {label}: {:?}", cmd);
        return Ok(());
    }
    info!(label, command = ?cmd, "running producer");
    let status = cmd
        .status()
        .with_context(|| format!("failed to spawn {label}"))?;
    if !status.success() {
        anyhow::bail!("{label} failed with status {status}");
    }
    Ok(())
}

fn run_or_warn(label: &str, mut cmd: ProcessCommand) {
    info!(label, command = ?cmd, "running optional step");
    match cmd.status() {
        Ok(status) if status.success() => {}
        Ok(status) => warn!(label, %status, "optional step failed"),
        Err(err) => warn!(label, error = %err, "optional step failed to spawn"),
    }
}

fn send_reports(
    stack_root: &Path,
    date: &str,
    session: &str,
    markets: &[String],
    args: &DailyArgs,
) -> Result<()> {
    let stack_root = stack_root
        .canonicalize()
        .unwrap_or_else(|_| stack_root.to_path_buf());
    if markets.iter().any(|m| m == "us") {
        let us_session = match session {
            "pre" | "morning" => "pre",
            "post" | "evening" | "daily" => "post",
            other => other,
        };
        send_daily_strategy_report(&stack_root, date, us_session, "us", args, false)?;
    }

    if markets.iter().any(|m| m == "cn") {
        let slot = match session {
            "pre" | "morning" => "morning",
            "post" | "evening" => "evening",
            "daily" => "daily",
            other => other,
        };
        send_daily_strategy_report(&stack_root, date, slot, "cn", args, false)?;
    }
    Ok(())
}

fn send_daily_strategy_report(
    stack_root: &Path,
    date: &str,
    slot: &str,
    market: &str,
    args: &DailyArgs,
    skip_generate: bool,
) -> Result<()> {
    let mut cmd = ProcessCommand::new("python3");
    cmd.arg("scripts/send_production_decision_report.py")
        .arg("--date")
        .arg(date)
        .arg("--session")
        .arg(slot)
        .arg("--market")
        .arg(market)
        .arg("--delivery-mode")
        .arg(args.delivery_mode.as_str())
        .current_dir(stack_root);
    if skip_generate {
        cmd.arg("--skip-generate");
    }
    if let Some(recipient) = args.test_recipient.as_deref() {
        cmd.arg("--test-recipient").arg(recipient);
    }
    if args.delivery_dry_run {
        cmd.arg("--delivery-dry-run");
    }
    if args.dry_run {
        cmd.arg("--dry-run");
    }
    run_or_print("daily strategy report delivery", cmd, false)
}

fn parse_markets(markets: &str) -> Vec<String> {
    markets
        .split(',')
        .map(|m| m.trim().to_lowercase())
        .filter(|m| !m.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "quant_stack_cli_{name}_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn write_model(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            path,
            r#"{
  "as_of": "2026-05-07",
  "market": "cn",
  "session": "post",
  "alpha_bulletin": {
    "ev_status": "failed",
    "selected_policy": null,
    "tactical_policy": null,
    "evaluated_through": "2026-05-05",
    "execution_alpha": [],
    "tactical_alpha": [],
    "options_alpha": [{"symbol": "510300"}],
    "recall_alpha": [],
    "blocked_alpha": [{"symbol": "000001.SZ"}]
  }
}"#,
        )
        .unwrap();
    }

    #[test]
    fn shared_report_model_status_falls_back_to_review_dashboard() {
        let root = temp_root("model_fallback");
        let model = root
            .join("reports/review_dashboard/strategy_backtest/2026-05-07")
            .join("report_model_cn_post.json");
        write_model(&model);

        let block = shared_report_model_status_block(&root, "cn", "2026-05-07", "evening").unwrap();

        assert!(block.contains("## Shared Report Model Status"));
        assert!(block.contains("- market: `cn`"));
        assert!(block.contains("- ev_status: `failed`"));
        assert!(block.contains("execution=0"));
        assert!(block.contains("options=1"));
        assert!(block.contains("blocked=1"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enforce_shared_report_model_status_prepends_final_report() {
        let root = temp_root("report_enforce");
        write_model(
            &root
                .join("reports")
                .join("2026-05-07_report_model_cn_post.json"),
        );
        let report = root.join("report.md");
        fs::write(&report, "# 市场日报\n\n正文").unwrap();

        enforce_shared_report_model_status(&root, "cn", "2026-05-07", "evening", &report).unwrap();
        let text = fs::read_to_string(&report).unwrap();

        assert!(text.starts_with("## Shared Report Model Status"));
        assert!(text.contains("# 市场日报"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enforce_shared_report_model_status_replaces_existing_block() {
        let root = temp_root("report_replace");
        write_model(
            &root
                .join("reports")
                .join("2026-05-07_report_model_cn_post.json"),
        );
        let report = root.join("report.md");
        fs::write(
            &report,
            "## Shared Report Model Status\n- old: true\n\n---\n\n# 市场日报\n\n正文",
        )
        .unwrap();

        enforce_shared_report_model_status(&root, "cn", "2026-05-07", "evening", &report).unwrap();
        let text = fs::read_to_string(&report).unwrap();

        assert!(text.starts_with("## Shared Report Model Status"));
        assert!(!text.contains("old: true"));
        assert!(text.contains("probation=0"));
        assert!(text.contains("# 市场日报"));
        let _ = fs::remove_dir_all(root);
    }
}
