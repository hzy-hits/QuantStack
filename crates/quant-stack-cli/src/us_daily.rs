use crate::DeliveryMode;
use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

const DATA_TIMEOUT_SECS: u64 = 3600;
const SPLIT_TIMEOUT_SECS: u64 = 60;
const AGENT_TIMEOUT_SECS: u64 = 2400;
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
    PayloadReady,
    PayloadSplit,
    FactorLabInjected,
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
            Self::PayloadReady => "payload_ready",
            Self::PayloadSplit => "payload_split",
            Self::FactorLabInjected => "factor_lab_injected",
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
    PayloadSplit,
    AgentAnalysis,
    EmailSend,
}

impl UsPipelineStep {
    fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::DataPipeline => "data pipeline",
            Self::FactorLab => "factor lab",
            Self::PayloadSplit => "split payload",
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
    project_dir: PathBuf,
    factor_lab_root: PathBuf,
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
            project_dir,
            factor_lab_root,
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

    let payload = ctx
        .project_dir
        .join("reports")
        .join(format!("{}_payload_{}.md", ctx.date, ctx.session));
    ensure_nonempty(&payload)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::DataPipeline, e))?;
    println!("Payload: {} bytes", file_size(&payload).unwrap_or(0));
    enter_state(ctx, UsPipelineState::PayloadReady, attempt);

    split_payload(ctx).map_err(|e| UsPipelineFailure::new(UsPipelineStep::PayloadSplit, e))?;
    validate_split_payloads(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::PayloadSplit, e))?;
    enter_state(ctx, UsPipelineState::PayloadSplit, attempt);

    inject_factor_lab_candidates(ctx)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::FactorLab, e))?;
    enter_state(ctx, UsPipelineState::FactorLabInjected, attempt);

    run_agents(ctx, previous_report.as_deref())
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::AgentAnalysis, e))?;
    enter_state(ctx, UsPipelineState::AgentsComplete, attempt);

    let report = ctx
        .project_dir
        .join("reports")
        .join(format!("{}_report_zh_{}.md", ctx.date, ctx.session));
    ensure_nonempty(&report)
        .map_err(|e| UsPipelineFailure::new(UsPipelineStep::AgentAnalysis, e))?;
    println!(
        "Chinese report: {} ({} bytes)",
        report.display(),
        file_size(&report).unwrap_or(0)
    );
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

fn split_payload(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[4/7] Split payload");
    let mut cmd = timeout_command(SPLIT_TIMEOUT_SECS, "uv");
    cmd.arg("run")
        .arg("python")
        .arg("scripts/split_payload.py")
        .arg("--date")
        .arg(&ctx.date)
        .arg("--session")
        .arg(&ctx.session)
        .current_dir(&ctx.project_dir);
    run_command("payload split", cmd, ctx.dry_run)
}

fn validate_split_payloads(ctx: &UsPipelineContext) -> Result<()> {
    if ctx.dry_run {
        return Ok(());
    }
    for section in ["macro", "structural", "news"] {
        ensure_nonempty(&ctx.project_dir.join("reports").join(format!(
            "{}_payload_{}_{}.md",
            ctx.date, section, ctx.session
        )))?;
    }
    Ok(())
}

fn inject_factor_lab_candidates(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[5/7] Inject Factor Lab research candidates");
    if ctx.dry_run {
        println!("dry-run: would append Factor Lab candidates to structural payload");
        return Ok(());
    }

    let structural = ctx.project_dir.join("reports").join(format!(
        "{}_payload_structural_{}.md",
        ctx.date, ctx.session
    ));
    if !structural.is_file() {
        warn!(path = %structural.display(), "structural payload missing; skip factor lab injection");
        return Ok(());
    }

    let output = run_factor_lab_strategy(ctx).unwrap_or_else(|err| {
        warn!(error = %err, "factor lab signal injection failed");
        String::new()
    });
    let (status, trade_date, age_days) = factor_lab_status(&ctx.date, &output);
    let mut file = OpenOptions::new()
        .append(true)
        .open(&structural)
        .with_context(|| format!("failed to open {}", structural.display()))?;
    writeln!(file)?;
    writeln!(file, "## Factor Lab Research Candidates")?;
    writeln!(file)?;
    writeln!(file, "以下是 Factor Lab 的研究候选，不是独立交易指令。")?;
    writeln!(
        file,
        "它不能决定 Headline Gate、今日大盘结论或主书方向；只有通过主系统方向、execution gate、流动性和追价过滤后，才能进入主书。"
    )?;
    match status {
        FactorLabStatus::Fresh => {
            if let Some(trade_date) = trade_date {
                writeln!(
                    file,
                    "状态: FRESH。候选输出交易日 {trade_date}，可作为研究附录展示，但不得覆盖主系统结论。"
                )?;
            } else {
                writeln!(
                    file,
                    "状态: FRESH。未发现明显日期滞后，可作为研究附录展示，但不得覆盖主系统结论。"
                )?;
            }
        }
        FactorLabStatus::Stale => {
            writeln!(
                file,
                "状态: STALE。候选输出使用的最新交易日为 {}，较报告日 {} 滞后 {} 天。只允许放在附录，不得作为主报告确认信号。",
                trade_date.unwrap_or_else(|| "unknown".to_string()),
                ctx.date,
                age_days.unwrap_or(999)
            )?;
        }
        FactorLabStatus::Unavailable => {
            writeln!(
                file,
                "状态: UNAVAILABLE。候选输出失败或缺少交易日信息，忽略其方向性结论。"
            )?;
        }
    }
    writeln!(file, "每只股票附带参考价、风控线、观察上沿和研究权重。")?;
    writeln!(file)?;
    if !output.trim().is_empty() {
        writeln!(file, "{}", output.trim_end())?;
    }
    writeln!(file)?;
    writeln!(
        file,
        "最终研报只需保留状态说明和紧凑表格，不要复述整段“使用方式”说明；它不得主导 headline 或主书排序。"
    )?;
    println!(
        "Factor Lab candidates injected into {}",
        structural.display()
    );
    Ok(())
}

fn run_factor_lab_strategy(ctx: &UsPipelineContext) -> Result<String> {
    let output = ProcessCommand::new(&ctx.python_bin)
        .arg("scripts/run_strategy.py")
        .arg("--market")
        .arg("us")
        .arg("--today")
        .arg("--date")
        .arg(&ctx.date)
        .current_dir(&ctx.factor_lab_root)
        .output()
        .context("failed to spawn Factor Lab strategy")?;
    if !output.status.success() {
        bail!(
            "Factor Lab strategy failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_agents(ctx: &UsPipelineContext, previous_report: Option<&Path>) -> Result<()> {
    println!();
    println!("[6/7] Run US analysis agents");
    let mut cmd = timeout_command(
        AGENT_TIMEOUT_SECS,
        ctx.project_dir.join("scripts/run_agents.sh"),
    );
    cmd.arg(&ctx.date).arg(&ctx.session);
    if let Some(prev) = previous_report {
        cmd.arg(prev);
    }
    cmd.current_dir(&ctx.project_dir);
    run_command("us agents", cmd, ctx.dry_run)
}

fn send_report(ctx: &UsPipelineContext) -> Result<()> {
    println!();
    println!("[7/7] Send US report");
    let mut cmd = timeout_command(EMAIL_TIMEOUT_SECS, "uv");
    cmd.arg("run")
        .arg("python")
        .arg("scripts/send_report.py")
        .arg("--send")
        .arg("--date")
        .arg(&ctx.date)
        .arg("--session")
        .arg(&ctx.session)
        .arg("--lang")
        .arg("zh")
        .arg("--delivery-mode")
        .arg(ctx.delivery_mode.as_str())
        .current_dir(&ctx.project_dir);
    if let Some(recipient) = ctx
        .test_recipient
        .as_deref()
        .filter(|s| !s.trim().is_empty())
    {
        cmd.arg("--test-recipient").arg(recipient);
    }
    if ctx.delivery_dry_run || ctx.dry_run {
        cmd.arg("--dry-run");
    }
    run_command("us delivery", cmd, ctx.dry_run)
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FactorLabStatus {
    Fresh,
    Stale,
    Unavailable,
}

fn factor_lab_status(as_of: &str, output: &str) -> (FactorLabStatus, Option<String>, Option<i64>) {
    if output.trim().is_empty() {
        return (FactorLabStatus::Unavailable, None, None);
    }
    let trade_date = extract_factor_lab_trade_date(output);
    let Some(trade_date) = trade_date else {
        return (FactorLabStatus::Fresh, None, None);
    };
    let age_days = NaiveDate::parse_from_str(as_of, "%Y-%m-%d")
        .ok()
        .zip(NaiveDate::parse_from_str(&trade_date, "%Y-%m-%d").ok())
        .map(|(as_of, trade)| (as_of - trade).num_days());
    if age_days.unwrap_or(999) <= 3 {
        (FactorLabStatus::Fresh, Some(trade_date), age_days)
    } else {
        (FactorLabStatus::Stale, Some(trade_date), age_days)
    }
}

fn extract_factor_lab_trade_date(output: &str) -> Option<String> {
    let marker = "数据截止:";
    let idx = output.find(marker)?;
    let after = output[idx + marker.len()..].trim_start();
    let date = after.get(0..10)?;
    if NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok() {
        Some(date.to_string())
    } else {
        None
    }
}
