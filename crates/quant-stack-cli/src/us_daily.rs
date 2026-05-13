use crate::DeliveryMode;
use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use quant_stack_core::alpha::{self, AlphaEvalConfig};
use quant_stack_core::report_model;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

const DATA_TIMEOUT_SECS: u64 = 3600;
const EMAIL_TIMEOUT_SECS: u64 = 120;
const LOCK_FILE: &str = "/tmp/quant-research-pipeline.lock";

#[derive(Parser, Debug, Clone)]
pub(crate) struct UsDailyArgs {
    /// Report date. Defaults to current New York market date.
    #[arg(value_name = "YYYY-MM-DD")]
    pub(crate) date: Option<String>,
    /// Compatibility flag for scripts/run_full.sh: run the pre-market session.
    #[arg(long)]
    pub(crate) premarket: bool,
    /// Explicit session. `--premarket` overrides this to `pre`.
    #[arg(long, default_value = "post")]
    pub(crate) session: String,
    /// Reuse existing payload inputs and skip the Python data producer.
    #[arg(long)]
    pub(crate) skip_data: bool,
    #[arg(long, value_enum, default_value_t = DeliveryMode::Test, env = "QUANT_DELIVERY_MODE")]
    pub(crate) delivery_mode: DeliveryMode,
    #[arg(long, env = "QUANT_TEST_RECIPIENT")]
    pub(crate) test_recipient: Option<String>,
    /// Resolve recipients and render the email, but skip the Gmail API call.
    #[arg(long)]
    pub(crate) delivery_dry_run: bool,
    /// Print commands without running them.
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// Repository root containing quant-research-v1 and factor-lab.
    #[arg(long, default_value = ".")]
    pub(crate) stack_root: PathBuf,
    /// Shared alpha/report-model history database.
    #[arg(long, default_value = "data/strategy_backtest_history.duckdb")]
    pub(crate) history_db: PathBuf,
    /// Shared alpha dashboard output root.
    #[arg(long, default_value = "reports/review_dashboard/strategy_backtest")]
    pub(crate) output_root: PathBuf,
    #[arg(long, default_value_t = 30)]
    pub(crate) lookback_days: i64,
    /// Disable the legacy single retry. Useful for local debugging.
    #[arg(long)]
    pub(crate) no_retry: bool,
    #[arg(long, default_value_t = 60)]
    pub(crate) retry_delay_secs: u64,
    #[arg(
        long,
        default_value = "13502448752hzy@gmail.com",
        env = "QUANT_ALERT_TO"
    )]
    pub(crate) alert_to: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum UsPipelineState {
    PreflightReady,
    LockAcquired,
    PreviousReportResolved,
    DataProducerComplete,
    FactorLabRefreshComplete,
    FactorLabImportComplete,
    SharedAlphaEvaluated,
    SharedReportModelWritten,
    AgentsComplete,
    ReportReady,
    DeliveryReady,
    Delivered,
}

impl UsPipelineState {
    fn as_str(self) -> &'static str {
        match self {
            Self::PreflightReady => "preflight_ready",
            Self::LockAcquired => "lock_acquired",
            Self::PreviousReportResolved => "previous_report_resolved",
            Self::DataProducerComplete => "data_producer_complete",
            Self::FactorLabRefreshComplete => "factor_lab_refresh_complete",
            Self::FactorLabImportComplete => "factor_lab_import_complete",
            Self::SharedAlphaEvaluated => "shared_alpha_evaluated",
            Self::SharedReportModelWritten => "shared_report_model_written",
            Self::AgentsComplete => "agents_complete",
            Self::ReportReady => "report_ready",
            Self::DeliveryReady => "delivery_ready",
            Self::Delivered => "delivered",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum UsPipelineStep {
    Preflight,
    DataPipeline,
    FactorLab,
    AgentAnalysis,
    EmailSend,
}

impl UsPipelineStep {
    fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::DataPipeline => "data pipeline",
            Self::FactorLab => "factor lab",
            Self::AgentAnalysis => "agent analysis",
            Self::EmailSend => "email send",
        }
    }
}

#[derive(Debug)]
struct UsPipelineFailure {
    step: UsPipelineStep,
    error: anyhow::Error,
}

impl UsPipelineFailure {
    fn new(step: UsPipelineStep, error: anyhow::Error) -> Self {
        Self { step, error }
    }
}

struct UsPipelineContext {
    date: String,
    session: String,
    stack_root: PathBuf,
    project_dir: PathBuf,
    factor_lab_root: PathBuf,
    history_db: PathBuf,
    output_root: PathBuf,
    lookback_days: i64,
    delivery_mode: DeliveryMode,
    test_recipient: Option<String>,
    skip_data: bool,
    delivery_dry_run: bool,
    dry_run: bool,
    alert_to: String,
    python_bin: String,
    claude_bin: String,
    codex_bin: String,
}

struct PipelineLock {
    path: PathBuf,
}

impl PipelineLock {
    fn acquire(path: impl Into<PathBuf>, dry_run: bool) -> Result<Option<Self>> {
        let path = path.into();
        if dry_run {
            println!("dry-run: would acquire lock {}", path.display());
            return Ok(None);
        }
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "{}", std::process::id())?;
                Ok(Some(Self { path }))
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let pid_text = fs::read_to_string(&path).unwrap_or_default();
                let pid = pid_text.trim().parse::<u32>().ok();
                if pid.is_some_and(pid_is_alive) {
                    bail!("US pipeline already running (PID {}).", pid.unwrap());
                }
                warn!(
                    lock = %path.display(),
                    stale_pid = pid_text.trim(),
                    "removing stale US pipeline lock"
                );
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove stale lock {}", path.display()))?;
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                writeln!(file, "{}", std::process::id())?;
                Ok(Some(Self { path }))
            }
            Err(err) => Err(err).with_context(|| format!("failed to acquire {}", path.display())),
        }
    }
}

impl Drop for PipelineLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn run(args: UsDailyArgs) -> Result<()> {
    let ctx = UsPipelineContext::new(args.clone())?;
    ensure_test_recipient(&ctx)?;
    if let Err(error) = ensure_preflight(&ctx) {
        send_failure_alert(&ctx, UsPipelineStep::Preflight, 0, &error.to_string());
        return Err(error).context("preflight");
    }
    enter_state(&ctx, UsPipelineState::PreflightReady, 0);
    let _lock = match PipelineLock::acquire(LOCK_FILE, ctx.dry_run) {
        Ok(lock) => lock,
        Err(error) => {
            send_failure_alert(&ctx, UsPipelineStep::Preflight, 0, &error.to_string());
            return Err(error).context("lock acquisition");
        }
    };
    enter_state(&ctx, UsPipelineState::LockAcquired, 0);

    let first = run_attempt(&ctx, 1);
    if first.is_ok() {
        return Ok(());
    }
    let first = first.err().unwrap();
    println!();
    println!(
        "US PIPELINE FAILED at step: {}: {}",
        first.step.as_str(),
        first.error
    );
    send_failure_alert(
        &ctx,
        first.step,
        1,
        &format!(
            "First attempt failed. Retrying in {} seconds.\n\n{}",
            args.retry_delay_secs, first.error
        ),
    );

    if args.no_retry || ctx.dry_run {
        return Err(first.error).with_context(|| first.step.as_str().to_string());
    }

    println!("Retrying in {} seconds...", args.retry_delay_secs);
    thread::sleep(Duration::from_secs(args.retry_delay_secs));
    println!();
    println!("==========================================");
    println!("  US PIPELINE RETRY ATTEMPT");
    println!("==========================================");

    match run_attempt(&ctx, 2) {
        Ok(()) => {
            println!("US pipeline recovered on retry.");
            send_recovered_alert(&ctx, first.step);
            Ok(())
        }
        Err(second) => {
            println!();
            println!(
                "US PIPELINE FAILED AGAIN at step: {}: {}",
                second.step.as_str(),
                second.error
            );
            send_failure_alert(
                &ctx,
                second.step,
                2,
                &format!(
                    "Both attempts failed. Manual intervention required.\nFirst failure: {}\nSecond failure: {}",
                    first.step.as_str(),
                    second.step.as_str()
                ),
            );
            Err(second.error).with_context(|| second.step.as_str().to_string())
        }
    }
}

impl UsPipelineContext {
    fn new(args: UsDailyArgs) -> Result<Self> {
        let stack_root = resolve_stack_root(&args.stack_root)?;
        let project_dir = stack_root.join("quant-research-v1");
        if !project_dir.is_dir() {
            bail!(
                "quant-research-v1 project not found under {}",
                stack_root.display()
            );
        }
        let factor_lab_root = resolve_factor_lab_root(&stack_root, &project_dir)?;
        let date = match args.date {
            Some(d) => {
                NaiveDate::parse_from_str(&d, "%Y-%m-%d")
                    .with_context(|| format!("invalid date {d}; expected YYYY-MM-DD"))?;
                d
            }
            None => default_us_market_date()?,
        };
        let session = if args.premarket {
            "pre".to_string()
        } else {
            normalize_us_session(&args.session)?
        };
        Ok(Self {
            date,
            session,
            stack_root: stack_root.clone(),
            project_dir,
            factor_lab_root,
            history_db: absolutize_under(&stack_root, &args.history_db),
            output_root: absolutize_under(&stack_root, &args.output_root),
            lookback_days: args.lookback_days,
            delivery_mode: args.delivery_mode,
            test_recipient: args.test_recipient,
            skip_data: args.skip_data,
            delivery_dry_run: args.delivery_dry_run,
            dry_run: args.dry_run,
            alert_to: args.alert_to,
            python_bin: env::var("PYTHON_BIN").unwrap_or_else(|_| "python3".to_string()),
            claude_bin: env::var("CLAUDE_BIN").unwrap_or_else(|_| "claude".to_string()),
            codex_bin: env::var("CODEX_BIN").unwrap_or_else(|_| "codex".to_string()),
        })
    }
}

fn run_attempt(ctx: &UsPipelineContext, attempt: u8) -> std::result::Result<(), UsPipelineFailure> {
    println!("==========================================");
    println!(
        "US Quant Research Pipeline - {} ({}) [attempt {}]",
        ctx.date, ctx.session, attempt
    );
    println!("Delivery mode: {}", ctx.delivery_mode.as_str());
    println!("==========================================");

    let previous_report = find_previous_report(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::Preflight, e))?;
    enter_state(ctx, UsPipelineState::PreviousReportResolved, attempt);
    if let Some(prev) = previous_report.as_deref() {
        println!("Previous report found: {}", prev.display());
    } else {
        println!("No previous report found.");
    }

    run_data_pipeline(ctx).map_err(|e| UsPipelineFailure::new(UsPipelineStep::DataPipeline, e))?;
    enter_state(ctx, UsPipelineState::DataProducerComplete, attempt);

    run_factor_lab_refresh(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::FactorLab, e))?;
    enter_state(ctx, UsPipelineState::FactorLabRefreshComplete, attempt);

    run_factor_lab_import(ctx).map_err(|e| UsPipelineFailure::new(UsPipelineStep::FactorLab, e))?;
    enter_state(ctx, UsPipelineState::FactorLabImportComplete, attempt);

    run_shared_alpha_gate(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::DataPipeline, e))?;
    enter_state(ctx, UsPipelineState::SharedAlphaEvaluated, attempt);
    materialize_us_report_model(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::DataPipeline, e))?;
    enter_state(ctx, UsPipelineState::SharedReportModelWritten, attempt);

    disable_legacy_agents(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::AgentAnalysis, e))?;
    enter_state(ctx, UsPipelineState::AgentsComplete, attempt);

    render_replacement_report(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::AgentAnalysis, e))?;
    let report = legacy_us_report_path(ctx);
    if ctx.dry_run {
        println!("dry-run: would materialize US report {}", report.display());
    } else {
        ensure_nonempty(&report)
            .map_err(|e| UsPipelineFailure::new(UsPipelineStep::AgentAnalysis, e))?;
        println!(
            "US report: {} ({} bytes)",
            report.display(),
            file_size(&report).unwrap_or(0)
        );
    }
    enter_state(ctx, UsPipelineState::ReportReady, attempt);

    enter_state(ctx, UsPipelineState::DeliveryReady, attempt);
    send_report(ctx).map_err(|e| UsPipelineFailure::new(UsPipelineStep::EmailSend, e))?;
    enter_state(ctx, UsPipelineState::Delivered, attempt);

    println!("==========================================");
    println!("US pipeline complete - {} ({})", ctx.date, ctx.session);
    println!("==========================================");
    Ok(())
}

fn enter_state(ctx: &UsPipelineContext, state: UsPipelineState, attempt: u8) {
    info!(
        state = state.as_str(),
        date = %ctx.date,
        session = %ctx.session,
        attempt,
        delivery_mode = ctx.delivery_mode.as_str(),
        dry_run = ctx.dry_run,
        "us pipeline state"
    );
}

fn ensure_test_recipient(ctx: &UsPipelineContext) -> Result<()> {
    if ctx.delivery_mode == DeliveryMode::Test
        && ctx
            .test_recipient
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        && env::var("QUANT_TEST_RECIPIENT")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .is_none()
    {
        bail!("test delivery requires --test-recipient or QUANT_TEST_RECIPIENT; refusing to fall back to production recipients");
    }
    Ok(())
}

fn ensure_preflight(ctx: &UsPipelineContext) -> Result<()> {
    for bin in ["uv", "timeout"] {
        if !command_exists(bin) {
            bail!("'{bin}' not found in PATH");
        }
    }
    if !command_exists(&ctx.python_bin) {
        bail!("'{}' not found in PATH", ctx.python_bin);
    }
    if !ctx.dry_run && !command_exists(&ctx.claude_bin) && !command_exists(&ctx.codex_bin) {
        bail!(
            "neither '{}' nor '{}' is available in PATH",
            ctx.claude_bin,
            ctx.codex_bin
        );
    }
    Ok(())
}

fn run_data_pipeline(ctx: &UsPipelineContext) -> Result<()> {
    if ctx.skip_data {
        println!();
        println!("[1/7] Skip US data producer (--skip-data)");
        return Ok(());
    }
    if ctx.dry_run {
        println!("dry-run: would run US data producer");
        return Ok(());
    }
    println!();
    println!("[1/7] US data producer ({})", ctx.session);
    let mut cmd = timeout_command(DATA_TIMEOUT_SECS, "uv");
    cmd.arg("run")
        .arg("python")
        .arg("scripts/run_daily.py")
        .arg("--date")
        .arg(&ctx.date)
        .arg("--session")
        .arg(&ctx.session)
        .env("PYTHONUNBUFFERED", "1")
        .current_dir(&ctx.project_dir);
    run_command("us data producer", cmd, false)
}

fn run_factor_lab_refresh(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    if ctx.session == "post" {
        println!("[2/7] US Factor Lab same-day refresh");
        let mut cmd = ProcessCommand::new("bash");
        cmd.arg("scripts/daily_factors.sh")
            .arg("--market")
            .arg("us")
            .env("FACTOR_LAB_US_EXPECTED_DATE", &ctx.date)
            .current_dir(&ctx.factor_lab_root);
        run_optional("factor lab refresh", cmd, ctx.dry_run);
    } else {
        println!("[2/7] Skip US Factor Lab same-day refresh for pre-market session");
    }
    Ok(())
}

fn run_factor_lab_import(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[3/7] Import US Factor Lab factors");
    let mut cmd = ProcessCommand::new(&ctx.python_bin);
    cmd.arg("-m")
        .arg("src.mining.export_to_pipeline")
        .arg("--market")
        .arg("us")
        .arg("--date")
        .arg(&ctx.date)
        .current_dir(&ctx.factor_lab_root);
    run_optional("factor lab import", cmd, ctx.dry_run);
    Ok(())
}

fn run_shared_alpha_gate(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[3.5/7] Shared alpha gate + report model");
    if ctx.dry_run {
        println!("dry-run: would run shared US alpha gate");
        return Ok(());
    }
    let as_of = NaiveDate::parse_from_str(&ctx.date, "%Y-%m-%d")
        .with_context(|| format!("invalid date {}", ctx.date))?;
    let config = AlphaEvalConfig {
        as_of,
        markets: vec!["us".to_string()],
        lookback_days: ctx.lookback_days,
        auto_select: true,
        emit_bulletin: true,
        history_db: ctx.history_db.clone(),
        output_root: ctx.output_root.clone(),
        us_db: us_report_db(ctx),
        cn_db: ctx
            .stack_root
            .join("quant-research-cn/data/quant_cn_report.duckdb"),
        us_horizon_days: 3,
        cn_horizon_days: 2,
        write_project_copies: false,
    };
    alpha::evaluate(&config)?;
    Ok(())
}

fn us_report_db(ctx: &UsPipelineContext) -> PathBuf {
    let session_db = ctx
        .project_dir
        .join("data")
        .join(format!("quant_report_{}_{}.duckdb", ctx.date, ctx.session));
    if session_db.is_file() {
        session_db
    } else {
        ctx.project_dir.join("data/quant.duckdb")
    }
}

fn materialize_us_report_model(ctx: &UsPipelineContext) -> Result<()> {
    if ctx.dry_run {
        println!("dry-run: would materialize US shared report model");
        return Ok(());
    }
    let written = report_model::write_models_from_history(
        &ctx.history_db,
        &ctx.date,
        &["us".to_string()],
        "post",
        &ctx.project_dir.join("reports"),
    )?;
    if written == 0 {
        bail!(
            "shared report model missing for US {} in {}",
            ctx.date,
            ctx.history_db.display()
        );
    }
    Ok(())
}

fn disable_legacy_agents(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[6/7] Legacy US agents disabled; Main Strategy V2 replaces the US daily report");
    cleanup_legacy_us_report_artifacts(ctx)
}

fn legacy_us_report_path(ctx: &UsPipelineContext) -> PathBuf {
    ctx.project_dir
        .join("reports")
        .join(format!("{}_report_zh_{}.md", ctx.date, ctx.session))
}

fn run_daily_report_command(
    ctx: &UsPipelineContext,
    label: &str,
    skip_generate: bool,
    delivery_dry_run: bool,
) -> Result<()> {
    let mut cmd = timeout_command(EMAIL_TIMEOUT_SECS, &ctx.python_bin);
    cmd.arg(
        ctx.stack_root
            .join("scripts/send_production_decision_report.py"),
    );
    cmd.arg("--date")
        .arg(&ctx.date)
        .arg("--session")
        .arg(&ctx.session)
        .arg("--market")
        .arg("us")
        .arg("--delivery-mode")
        .arg(ctx.delivery_mode.as_str())
        .current_dir(&ctx.stack_root);
    if skip_generate {
        cmd.arg("--skip-generate");
    }
    if let Some(recipient) = ctx
        .test_recipient
        .as_deref()
        .filter(|s| !s.trim().is_empty())
    {
        cmd.arg("--test-recipient").arg(recipient);
    }
    if delivery_dry_run {
        cmd.arg("--delivery-dry-run");
    }
    if ctx.dry_run {
        cmd.arg("--dry-run");
    }
    run_command(label, cmd, ctx.dry_run)
}

fn render_replacement_report(ctx: &UsPipelineContext) -> Result<()> {
    run_daily_report_command(ctx, "us final market report render", false, true)?;
    cleanup_legacy_us_report_artifacts(ctx)
}

fn send_report(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[7/7] Send US report");
    run_daily_report_command(ctx, "us daily report delivery", true, ctx.delivery_dry_run)?;
    cleanup_legacy_us_report_artifacts(ctx)
}

fn cleanup_legacy_us_report_artifacts(ctx: &UsPipelineContext) -> Result<()> {
    let reports = ctx.project_dir.join("reports");
    remove_file_if_exists(&reports.join(format!("{}_payload_{}.md", ctx.date, ctx.session)))?;
    remove_file_if_exists(&reports.join(format!("{}_payload.md", ctx.date)))?;
    for section in ["macro", "structural", "news"] {
        remove_file_if_exists(&reports.join(format!(
            "{}_payload_{}_{}.md",
            ctx.date, section, ctx.session
        )))?;
        remove_file_if_exists(&reports.join(format!("{}_payload_{}.md", ctx.date, section)))?;
    }
    remove_file_if_exists(&reports.join(format!(
        "{}_factor_lab_appendix_{}.md",
        ctx.date, ctx.session
    )))?;
    remove_file_if_exists(&reports.join(format!("{}_factor_lab_appendix.md", ctx.date)))?;
    remove_dir_if_exists(
        &ctx.project_dir
            .join(format!("run-agents-{}-{}", ctx.date, ctx.session)),
    )?;
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

fn send_failure_alert(ctx: &UsPipelineContext, step: UsPipelineStep, attempt: u8, detail: &str) {
    let subject = format!(
        "[Quant Pipeline FAILED] {} ({}) - {} (attempt {})",
        ctx.date,
        ctx.session,
        step.as_str(),
        attempt
    );
    let body = format!(
        "Pipeline failed at step: {}\nDate: {}\nSession: {}\nAttempt: {}\n\nError detail:\n{}\n",
        step.as_str(),
        ctx.date,
        ctx.session,
        attempt,
        detail
    );
    send_alert(ctx, &subject, &body);
}

fn send_recovered_alert(ctx: &UsPipelineContext, first_step: UsPipelineStep) {
    let subject = format!("[Quant Pipeline RECOVERED] {} ({})", ctx.date, ctx.session);
    let body = format!(
        "Pipeline recovered on retry.\nDate: {}\nSession: {}\nFirst failure was at step: {}\n",
        ctx.date,
        ctx.session,
        first_step.as_str()
    );
    send_alert(ctx, &subject, &body);
}

fn send_alert(ctx: &UsPipelineContext, subject: &str, body: &str) {
    if ctx.dry_run {
        println!("dry-run: would send alert '{subject}' to {}", ctx.alert_to);
        return;
    }
    let mut cmd = ProcessCommand::new("uv");
    cmd.arg("run")
        .arg("python")
        .arg("scripts/send_alert.py")
        .arg("--to")
        .arg(&ctx.alert_to)
        .arg("--subject")
        .arg(subject)
        .arg("--body")
        .arg(body)
        .current_dir(&ctx.project_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Err(err) = run_command("us failure alert", cmd, false) {
        warn!(error = %err, "failed to send US pipeline alert");
    }
}

fn find_previous_report(ctx: &UsPipelineContext) -> Result<Option<PathBuf>> {
    let reports_dir = ctx.project_dir.join("reports");
    let current_rank = session_rank(&ctx.session)?;
    let mut reports: Vec<((String, u8), PathBuf)> = Vec::new();
    for entry in fs::read_dir(&reports_dir)
        .with_context(|| format!("failed to read {}", reports_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.contains("_report_codex") || !name.ends_with(".md") {
            continue;
        }
        let Some((report_date, rest)) = name.split_once("_report_zh_") else {
            continue;
        };
        let report_session = rest.trim_end_matches(".md");
        let Ok(rank) = session_rank(report_session) else {
            continue;
        };
        if (report_date, rank) < (ctx.date.as_str(), current_rank) {
            reports.push(((report_date.to_string(), rank), path));
        }
    }
    reports.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(reports.pop().map(|(_, path)| path))
}

fn timeout_command<S: AsRef<std::ffi::OsStr>>(seconds: u64, program: S) -> ProcessCommand {
    let mut cmd = ProcessCommand::new("timeout");
    cmd.arg(seconds.to_string()).arg(program);
    cmd
}

fn run_command(label: &str, mut cmd: ProcessCommand, dry_run: bool) -> Result<()> {
    if dry_run {
        println!("dry-run {label}: {:?}", cmd);
        return Ok(());
    }
    info!(label, command = ?cmd, "running US pipeline command");
    let status = cmd
        .status()
        .with_context(|| format!("failed to spawn {label}"))?;
    if !status.success() {
        bail!("{label} failed with status {status}");
    }
    Ok(())
}

fn run_optional(label: &str, cmd: ProcessCommand, dry_run: bool) {
    if let Err(err) = run_command(label, cmd, dry_run) {
        warn!(label, error = %err, "non-fatal US pipeline command failed");
        println!("  {label} failed (non-fatal): {err}");
    }
}

fn ensure_nonempty(path: &Path) -> Result<()> {
    let meta = fs::metadata(path).with_context(|| format!("{} not found", path.display()))?;
    if meta.len() == 0 {
        bail!("{} is empty", path.display());
    }
    Ok(())
}

fn file_size(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|m| m.len())
}

fn resolve_stack_root(input: &Path) -> Result<PathBuf> {
    let base = input.canonicalize().unwrap_or_else(|_| input.to_path_buf());
    if base.join("quant-research-v1").is_dir() {
        return Ok(base);
    }
    if base.file_name().and_then(|s| s.to_str()) == Some("quant-research-v1") {
        return Ok(base
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| anyhow!("cannot infer stack root from {}", base.display()))?);
    }
    Ok(base)
}

fn absolutize_under(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn resolve_factor_lab_root(stack_root: &Path, project_dir: &Path) -> Result<PathBuf> {
    let candidates = [
        env::var("FACTOR_LAB_ROOT").ok().map(PathBuf::from),
        env::var("QUANT_STACK_ROOT")
            .ok()
            .map(|p| PathBuf::from(p).join("factor-lab")),
        Some(stack_root.join("factor-lab")),
        project_dir.parent().map(|p| p.join("factor-lab")),
        project_dir
            .parent()
            .and_then(Path::parent)
            .map(|p| p.join("python/factor-lab")),
    ];
    for candidate in candidates.into_iter().flatten() {
        if candidate.is_dir() {
            return candidate
                .canonicalize()
                .with_context(|| format!("failed to canonicalize {}", candidate.display()));
        }
    }
    bail!("factor-lab repo not found. Set FACTOR_LAB_ROOT or QUANT_STACK_ROOT.")
}

fn default_us_market_date() -> Result<String> {
    let output = ProcessCommand::new("date")
        .arg("+%Y-%m-%d")
        .env("TZ", "America/New_York")
        .output()
        .context("failed to resolve New York date")?;
    if !output.status.success() {
        bail!("date command failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn normalize_us_session(session: &str) -> Result<String> {
    match session {
        "pre" | "premarket" | "morning" => Ok("pre".to_string()),
        "post" | "postmarket" | "evening" | "daily" => Ok("post".to_string()),
        other => bail!("unsupported US session '{other}', expected pre or post"),
    }
}

fn session_rank(session: &str) -> Result<u8> {
    match normalize_us_session(session)?.as_str() {
        "pre" => Ok(0),
        "post" => Ok(1),
        _ => unreachable!(),
    }
}

fn command_exists(name: &str) -> bool {
    let path = Path::new(name);
    if path.components().count() > 1 {
        return path.is_file();
    }
    env::var_os("PATH")
        .map(|paths| env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
        .unwrap_or(false)
}

fn pid_is_alive(pid: u32) -> bool {
    ProcessCommand::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
