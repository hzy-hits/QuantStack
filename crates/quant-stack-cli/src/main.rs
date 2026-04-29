use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};
use quant_stack_core::alpha::{self, AlphaEvalConfig};
use quant_stack_core::report_model;
use std::fs::{self, File};
use std::io::Read;
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
        no_project_copies: false,
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
        cmd.arg("run").arg("--date").arg(date).current_dir(&cn_root);
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
        println!("dry-run: would import CN Factor Lab factors and render pre-alpha payloads");
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
    enter_daily_state(DailyPipelineState::CnPreAlphaRendered, args, markets);
    Ok(())
}

fn finalize_cn_report(stack_root: &Path, args: &DailyArgs, markets: &[String]) -> Result<()> {
    let cn_root = stack_root.join("quant-research-cn");
    if args.dry_run {
        println!("dry-run: would render CN bulletin payloads, charts, and final agent report");
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

    append_cn_factor_prior(stack_root, &args.date)?;
    annotate_and_snapshot_cn_payloads(&cn_root, &args.date, &cn_slot(&args.session))?;
    verify_cn_payloads(&cn_root, &args.date)?;
    enter_daily_state(DailyPipelineState::CnPayloadsFinalized, args, markets);

    generate_cn_charts(&cn_root, &args.date, &cn_slot(&args.session));
    enter_daily_state(DailyPipelineState::CnChartsGenerated, args, markets);

    if args.with_narrative || args.send_reports {
        run_cn_agents(stack_root, args)?;
        enter_daily_state(DailyPipelineState::CnNarrativeRendered, args, markets);
    }
    Ok(())
}

fn append_cn_factor_prior(stack_root: &Path, date: &str) -> Result<()> {
    let cn_root = stack_root.join("quant-research-cn");
    let factor_lab_root = stack_root.join("factor-lab");
    let structural = cn_root
        .join("reports")
        .join(format!("{date}_payload_structural.md"));
    if !structural.is_file() || !factor_lab_root.is_dir() {
        return Ok(());
    }

    let output = ProcessCommand::new("python3")
        .arg("scripts/run_strategy.py")
        .arg("--market")
        .arg("cn")
        .arg("--today")
        .arg("--date")
        .arg(date)
        .current_dir(&factor_lab_root)
        .output();

    let mut section = String::new();
    section.push_str("\n## Factor Lab research prior / recall lead\n\n");
    section.push_str("以下是 Factor Lab research prior / recall lead，不是独立交易指令。\n");
    section.push_str("它不能决定 Headline Gate、今日市场主方向或主书排序；只有通过主系统方向、execution gate、流动性和追价过滤后，才能进入主书。\n\n");
    match output {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            section.push_str("状态: FRESH。可作为研究附录展示，但不得覆盖主系统结论。\n\n");
            section.push_str(&sanitize_factor_lab_text(&text));
            section.push('\n');
        }
        Ok(out) => {
            warn!(status = %out.status, "Factor Lab strategy output failed");
            section
                .push_str("状态: UNAVAILABLE。候选输出失败或缺少交易日信息，忽略其方向性结论。\n");
        }
        Err(err) => {
            warn!(error = %err, "failed to run Factor Lab strategy output");
            section
                .push_str("状态: UNAVAILABLE。候选输出失败或缺少交易日信息，忽略其方向性结论。\n");
        }
    }
    section.push_str("\n请在最终研报中完整展示上述清单，但明确标注为研究附录，不得让其主导 headline 或主书排序。\n");

    let mut current = String::new();
    File::open(&structural)?.read_to_string(&mut current)?;
    if !current.contains("## Factor Lab research prior / recall lead") {
        current.push_str(&section);
        fs::write(structural, current)?;
    }
    Ok(())
}

fn sanitize_factor_lab_text(text: &str) -> String {
    let mut out = text
        .replace(
            "## Factor Lab Independent Trading Signal",
            "## Factor Lab research prior / recall lead",
        )
        .replace(
            "## Factor Lab Research Candidates",
            "## Factor Lab research prior / recall lead",
        )
        .replace("怎么操作:", "研究观察:")
        .replace("买入价", "参考价")
        .replace("止损", "风控线")
        .replace("止盈", "观察上沿")
        .replace("仓位", "研究权重")
        .replace("买入", "研究关注");
    out = out
        .lines()
        .map(|line| {
            if line.contains("明天开盘") && line.contains("只") {
                "研究候选清单如下（仅 recall lead，进入主书前仍需 gate）".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    out.trim().to_string()
}

fn annotate_and_snapshot_cn_payloads(cn_root: &Path, date: &str, slot: &str) -> Result<()> {
    let reports = cn_root.join("reports");
    let label = match slot {
        "morning" => "盘前",
        "evening" => "盘后",
        other => other,
    };
    let meaning = if slot == "morning" {
        "盘前报告：价格/资金数据按最新可用收盘解释，重点是隔夜事件、今日触发条件、开盘后确认与撤销规则；不要把它写成收盘复盘。"
    } else {
        "盘后报告：应以今日收盘、全天资金流、事件兑现和早盘假设复盘为主；不要把早盘条件原样复制。"
    };
    let block = format!(
        "## 报告时段\n- Slot: {slot} / {label}\n- 报告日期: {date}\n- 解释: {meaning}\n\n---\n\n"
    );
    for section in ["macro", "structural", "events"] {
        let src = reports.join(format!("{date}_payload_{section}.md"));
        if !src.is_file() {
            continue;
        }
        let mut text = fs::read_to_string(&src)?;
        if !text
            .get(..text.len().min(1000))
            .unwrap_or("")
            .contains("## 报告时段")
        {
            text = format!("{block}{text}");
            fs::write(&src, &text)?;
        }
        if slot != "daily" {
            fs::copy(
                &src,
                reports.join(format!("{date}_payload_{section}_{slot}.md")),
            )?;
        }
    }
    Ok(())
}

fn verify_cn_payloads(cn_root: &Path, date: &str) -> Result<()> {
    for section in ["macro", "structural", "events"] {
        let path = cn_root
            .join("reports")
            .join(format!("{date}_payload_{section}.md"));
        let metadata = fs::metadata(&path)
            .with_context(|| format!("missing CN payload {}", path.display()))?;
        if metadata.len() == 0 {
            anyhow::bail!("empty CN payload {}", path.display());
        }
    }
    Ok(())
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

fn run_cn_agents(stack_root: &Path, args: &DailyArgs) -> Result<()> {
    let cn_root = stack_root.join("quant-research-cn");
    let slot = cn_slot(&args.session);
    let previous = find_previous_cn_report(&cn_root, &args.date, &slot);
    let mut cmd = ProcessCommand::new("bash");
    cmd.arg("scripts/run_agents.sh")
        .arg(&args.date)
        .arg(&slot)
        .current_dir(&cn_root)
        .env("SEND_EMAIL", "0")
        .env("QUANT_DELIVERY_MODE", args.delivery_mode.as_str())
        .env("QUANT_STACK_ROOT", stack_root);
    if let Some(path) = previous {
        cmd.arg(path);
    }
    if let Some(recipient) = args.test_recipient.as_deref() {
        cmd.env("QUANT_TEST_RECIPIENT", recipient);
    }
    run_or_print("cn agents", cmd, args.dry_run)
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

fn find_previous_cn_report(cn_root: &Path, date: &str, slot: &str) -> Option<PathBuf> {
    let reports_dir = cn_root.join("reports");
    let current_rank = slot_rank(slot);
    let mut candidates = Vec::new();
    let entries = fs::read_dir(reports_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        let Some((report_date, report_slot)) = parse_cn_report_name(name) else {
            continue;
        };
        let key = (report_date.as_str(), slot_rank(&report_slot));
        if key < (date, current_rank) {
            candidates.push((report_date, slot_rank(&report_slot), path));
        }
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    candidates.pop().map(|(_, _, path)| path)
}

fn parse_cn_report_name(name: &str) -> Option<(String, String)> {
    if !name.ends_with(".md") || name.len() < 23 {
        return None;
    }
    let date = name.get(0..10)?.to_string();
    if !date.chars().all(|c| c.is_ascii_digit() || c == '-') {
        return None;
    }
    if name == format!("{date}_report_zh.md") {
        return Some((date, "evening".to_string()));
    }
    let prefix = format!("{date}_report_zh_");
    name.strip_prefix(&prefix)
        .and_then(|rest| rest.strip_suffix(".md"))
        .map(|slot| (date, slot.to_string()))
}

fn slot_rank(slot: &str) -> i32 {
    match slot {
        "morning" => 0,
        "evening" | "daily" => 1,
        _ => 1,
    }
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
        let mut cmd = ProcessCommand::new("uv");
        cmd.arg("run")
            .arg("python")
            .arg("scripts/send_report.py")
            .arg("--send")
            .arg("--date")
            .arg(date)
            .arg("--session")
            .arg(us_session)
            .arg("--lang")
            .arg("zh")
            .arg("--delivery-mode")
            .arg(args.delivery_mode.as_str())
            .current_dir(stack_root.join("quant-research-v1"));
        if let Some(recipient) = args.test_recipient.as_deref() {
            cmd.arg("--test-recipient").arg(recipient);
        }
        if args.delivery_dry_run || args.dry_run {
            cmd.arg("--dry-run");
        }
        run_or_print("us delivery", cmd, false)?;
    }

    if markets.iter().any(|m| m == "cn") {
        let slot = match session {
            "pre" | "morning" => "morning",
            "post" | "evening" => "evening",
            "daily" => "daily",
            other => other,
        };
        let cn_root = stack_root.join("quant-research-cn");
        let report_path = if slot == "daily" {
            cn_root.join("reports").join(format!("{date}_report_zh.md"))
        } else {
            cn_root
                .join("reports")
                .join(format!("{date}_report_zh_{slot}.md"))
        };
        let subject = if slot == "daily" {
            format!("A股量化研究日报 — {date}")
        } else if slot == "morning" {
            format!("A股量化研究盘前日报 — {date}")
        } else if slot == "evening" {
            format!("A股量化研究盘后日报 — {date}")
        } else {
            format!("A股量化研究{slot}日报 — {date}")
        };
        let mut cmd = ProcessCommand::new("python3");
        cmd.arg("scripts/send_email.py")
            .arg(report_path)
            .arg("--subject")
            .arg(subject)
            .arg("--delivery-mode")
            .arg(args.delivery_mode.as_str())
            .current_dir(&cn_root);
        let chart_dir = cn_chart_dir(&cn_root, date, slot);
        if chart_dir.is_dir() {
            cmd.arg("--charts").arg(chart_dir);
        }
        if let Some(recipient) = args.test_recipient.as_deref() {
            cmd.arg("--test-recipient").arg(recipient);
        }
        if args.delivery_dry_run || args.dry_run {
            cmd.arg("--dry-run");
        }
        run_or_print("cn delivery", cmd, false)?;
    }
    Ok(())
}

fn cn_chart_dir(cn_root: &Path, date: &str, slot: &str) -> PathBuf {
    let slot_dir = cn_root.join("reports").join("charts").join(date).join(slot);
    if slot != "daily" && slot_dir.is_dir() {
        return slot_dir;
    }
    cn_root.join("reports").join("charts").join(date)
}

fn parse_markets(markets: &str) -> Vec<String> {
    markets
        .split(',')
        .map(|m| m.trim().to_lowercase())
        .filter(|m| !m.is_empty())
        .collect()
}
