use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};
use quant_stack_core::alpha::{self, AlphaEvalConfig};
use quant_stack_core::report_model;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use tracing::{info, warn};

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
enum DeliveryMode {
    Test,
    Prod,
}

impl DeliveryMode {
    fn as_str(self) -> &'static str {
        match self {
            DeliveryMode::Test => "test",
            DeliveryMode::Prod => "prod",
        }
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
    let stack_root = args
        .stack_root
        .canonicalize()
        .unwrap_or_else(|_| args.stack_root.clone());
    if args.run_producers {
        run_producers(
            &stack_root,
            &args.date,
            &args.session,
            &markets,
            args.dry_run,
        )?;
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
    } else {
        run_alpha_evaluate(alpha_args)?;
        let written = report_model::write_models_from_history(
            &args.history_db,
            &args.date,
            &markets,
            &args.session,
            &stack_root.join("reports"),
        )?;
        println!("daily core complete: report models written={written}");
    }

    if args.with_narrative {
        warn!("--with-narrative requested; LLM narrator cutover is intentionally left to the report-model phase. Use legacy run_agents wrappers during transition.");
    }
    if args.send_reports {
        send_reports(&stack_root, &args.date, &args.session, &markets, &args)?;
    }
    Ok(())
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
        let mut cmd = ProcessCommand::new("cargo");
        cmd.arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(stack_root.join("quant-research-cn/Cargo.toml"))
            .arg("--")
            .arg("run")
            .arg("--date")
            .arg(date)
            .current_dir(stack_root);
        run_or_print("cn producer", cmd, dry_run)?;
    }
    Ok(())
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
