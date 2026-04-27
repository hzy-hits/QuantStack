use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::info;

const SESSION: &str = "daily";
const LOOKBACK_DAYS: i64 = 90;
const SLIPPAGE_PCT: f64 = 0.18;
const WIN_THRESHOLD_PCT: f64 = 0.50;
const LOSS_THRESHOLD_PCT: f64 = -1.00;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectionStatus {
    Selected,
    Ignored,
}

impl SelectionStatus {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "selected" => Self::Selected,
            _ => Self::Ignored,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportLane {
    CoreBook,
    ThemeRotation,
    TacticalContinuation,
    RangeCore,
    Radar,
    Other,
}

impl ReportLane {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_uppercase().as_str() {
            "CORE BOOK" => Self::CoreBook,
            "THEME ROTATION" => Self::ThemeRotation,
            "TACTICAL CONTINUATION" => Self::TacticalContinuation,
            "RANGE CORE" => Self::RangeCore,
            "RADAR" => Self::Radar,
            _ => Self::Other,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::CoreBook => "CORE BOOK",
            Self::ThemeRotation => "THEME ROTATION",
            Self::TacticalContinuation => "TACTICAL CONTINUATION",
            Self::RangeCore => "RANGE CORE",
            Self::Radar => "RADAR",
            Self::Other => "OTHER",
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::CoreBook => "core_book",
            Self::ThemeRotation => "theme_rotation",
            Self::TacticalContinuation => "tactical_continuation",
            Self::RangeCore => "range_core",
            Self::Radar => "radar",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Bullish,
    Bearish,
    Neutral,
}

impl Direction {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "bullish" | "long" => Self::Bullish,
            "bearish" | "short" => Self::Bearish,
            _ => Self::Neutral,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    Executable,
    WaitPullback,
    DoNotChase,
    Other,
}

impl ExecutionMode {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "executable" => Self::Executable,
            "wait_pullback" => Self::WaitPullback,
            "do_not_chase" => Self::DoNotChase,
            _ => Self::Other,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Executable => "executable",
            Self::WaitPullback => "wait_pullback",
            Self::DoNotChase => "do_not_chase",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrategyFamily {
    EarningsSetup,
    EarlyAccumulation,
    ContinuationBreakout,
    ShadowOptionEdge,
    ThemeRotation,
    StructuralCore,
}

impl StrategyFamily {
    fn classify(decision: &Decision) -> Self {
        let p_upside = decision.detail_f64("p_upside", 0.50);
        let shadow_prob = decision.detail_f64("shadow_option_alpha_prob", 0.50);
        let ret_20d = decision.detail_f64("ret_20d", 0.0);

        match (
            p_upside >= 0.70,
            decision.setup_score >= 0.62 && decision.fade_risk <= 0.35,
            decision.continuation_score >= 0.62 && ret_20d >= 12.0,
            shadow_prob >= 0.58 && decision.fade_risk <= 0.40,
            decision.report_lane,
        ) {
            (true, _, _, _, _) => Self::EarningsSetup,
            (_, true, _, _, _) => Self::EarlyAccumulation,
            (_, _, true, _, _) => Self::ContinuationBreakout,
            (_, _, _, true, _) => Self::ShadowOptionEdge,
            (_, _, _, _, ReportLane::ThemeRotation) => Self::ThemeRotation,
            _ => Self::StructuralCore,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::EarningsSetup => "earnings_setup",
            Self::EarlyAccumulation => "early_accumulation",
            Self::ContinuationBreakout => "continuation_breakout",
            Self::ShadowOptionEdge => "shadow_option_edge",
            Self::ThemeRotation => "theme_rotation",
            Self::StructuralCore => "structural_core",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeIntent {
    Trade,
    Setup,
    Observe,
    Avoid,
}

impl TradeIntent {
    fn from_decision(decision: &Decision) -> Self {
        match (
            decision.selection_status,
            decision.direction,
            decision.report_lane,
            decision.execution_mode,
        ) {
            (
                SelectionStatus::Selected,
                Direction::Bullish,
                ReportLane::CoreBook,
                ExecutionMode::Executable,
            ) => Self::Trade,
            (
                SelectionStatus::Selected,
                Direction::Bullish,
                ReportLane::CoreBook,
                ExecutionMode::WaitPullback,
            ) => Self::Setup,
            (
                SelectionStatus::Selected,
                Direction::Bullish,
                ReportLane::CoreBook,
                ExecutionMode::DoNotChase,
            ) => Self::Avoid,
            (SelectionStatus::Selected, Direction::Bearish, _, _) => Self::Avoid,
            (SelectionStatus::Selected, Direction::Bullish, _, _) => Self::Observe,
            _ => Self::Observe,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "TRADE",
            Self::Setup => "SETUP",
            Self::Observe => "OBSERVE",
            Self::Avoid => "AVOID",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionRule {
    NextOpenOrPullback,
    PullbackOnly,
    ObserveOnly,
    Blocked,
}

impl ExecutionRule {
    fn from_intent(intent: TradeIntent) -> Self {
        match intent {
            TradeIntent::Trade => Self::NextOpenOrPullback,
            TradeIntent::Setup => Self::PullbackOnly,
            TradeIntent::Observe => Self::ObserveOnly,
            TradeIntent::Avoid => Self::Blocked,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::NextOpenOrPullback => "next_open_or_pullback",
            Self::PullbackOnly => "pullback_only",
            Self::ObserveOnly => "observe_only",
            Self::Blocked => "blocked",
        }
    }

    fn planned_entry(self, decision: &Decision) -> Option<f64> {
        match self {
            Self::NextOpenOrPullback | Self::PullbackOnly if decision.reference_close > 0.0 => {
                let pullback = decision.pullback_trigger_pct.max(0.5);
                Some(decision.reference_close * (1.0 - pullback / 100.0))
            }
            _ => None,
        }
    }

    fn simulate(self, decision: &Decision, future: &[FutureBar]) -> Fill {
        match (self, self.planned_entry(decision), future.first()) {
            (Self::ObserveOnly, _, _) => Fill::not_planned(FillStatus::NotPlanned, "observe_only"),
            (Self::Blocked, _, _) => Fill::not_planned(FillStatus::NotPlanned, "blocked"),
            (_, _, None) => Fill::not_planned(FillStatus::Pending, "pending_future_bar"),
            (Self::PullbackOnly, Some(entry), Some(_)) => future
                .iter()
                .find(|bar| bar.low <= entry)
                .map(|bar| Fill::filled(FillStatus::FilledPullback, bar.trade_date, entry))
                .unwrap_or_else(|| Fill::not_planned(FillStatus::NoFill, "pullback_not_touched")),
            (Self::NextOpenOrPullback, Some(entry), Some(first)) => {
                let open_gap = pct_change(first.open, decision.reference_close);
                match open_gap <= decision.max_chase_gap_pct.max(0.8) {
                    true => Fill::filled(FillStatus::FilledOpen, first.trade_date, first.open),
                    false if first.low <= entry => {
                        Fill::filled(FillStatus::FilledPullback, first.trade_date, entry)
                    }
                    false => Fill::not_planned(FillStatus::NoFill, "gap_above_limit_no_pullback"),
                }
            }
            _ => Fill::not_planned(FillStatus::NotPlanned, "missing_planned_entry"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillStatus {
    Pending,
    NotPlanned,
    NoFill,
    FilledOpen,
    FilledPullback,
}

impl FillStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::NotPlanned => "not_planned",
            Self::NoFill => "no_fill",
            Self::FilledOpen => "filled_open",
            Self::FilledPullback => "filled_pullback",
        }
    }
}

#[derive(Debug)]
struct Fill {
    status: FillStatus,
    date: Option<NaiveDate>,
    price: Option<f64>,
    reason: &'static str,
}

impl Fill {
    fn filled(status: FillStatus, date: NaiveDate, price: f64) -> Self {
        Self {
            status,
            date: Some(date),
            price: Some(price),
            reason: "filled",
        }
    }

    fn not_planned(status: FillStatus, reason: &'static str) -> Self {
        Self {
            status,
            date: None,
            price: None,
            reason,
        }
    }

    #[cfg(test)]
    fn is_filled(&self) -> bool {
        matches!(
            self.status,
            FillStatus::FilledOpen | FillStatus::FilledPullback
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutcomeLabel {
    Pending,
    Observe,
    Avoid,
    NoFill,
    Won,
    Lost,
    RightButExitFlat,
    TailRisk,
    Flat,
}

impl OutcomeLabel {
    fn from_trade(intent: TradeIntent, fill: &Fill, result: &TradeResult) -> Self {
        match (intent, fill.status, result.realized_ret_pct) {
            (TradeIntent::Observe, _, _) => Self::Observe,
            (TradeIntent::Avoid, _, _) => Self::Avoid,
            (_, FillStatus::Pending, _) => Self::Pending,
            (_, FillStatus::NoFill | FillStatus::NotPlanned, _) => Self::NoFill,
            (_, _, Some(ret)) if ret >= WIN_THRESHOLD_PCT => Self::Won,
            (_, _, Some(ret)) if ret <= LOSS_THRESHOLD_PCT => Self::Lost,
            (_, _, Some(_)) if result.max_favorable_pct.unwrap_or(0.0) >= 2.0 => {
                Self::RightButExitFlat
            }
            (_, _, Some(_)) if result.max_adverse_pct.unwrap_or(0.0) <= -3.0 => Self::TailRisk,
            (_, _, Some(_)) => Self::Flat,
            _ => Self::Pending,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Observe => "observe",
            Self::Avoid => "avoid",
            Self::NoFill => "no_fill",
            Self::Won => "won",
            Self::Lost => "lost",
            Self::RightButExitFlat => "right_but_exit_flat",
            Self::TailRisk => "tail_risk",
            Self::Flat => "flat",
        }
    }
}

#[derive(Debug)]
struct Decision {
    report_date: NaiveDate,
    symbol: String,
    selection_status: SelectionStatus,
    selection_status_raw: String,
    report_lane: ReportLane,
    direction: Direction,
    signal_confidence: String,
    execution_mode: ExecutionMode,
    max_chase_gap_pct: f64,
    pullback_trigger_pct: f64,
    setup_score: f64,
    continuation_score: f64,
    fade_risk: f64,
    reference_close: f64,
    details: Option<Value>,
    flow_conflict_flag: bool,
}

impl Decision {
    fn detail_f64(&self, key: &str, default: f64) -> f64 {
        self.details
            .as_ref()
            .and_then(|v| v.get(key))
            .and_then(|v| v.as_f64())
            .unwrap_or(default)
    }
}

#[derive(Debug, Clone)]
struct FutureBar {
    trade_date: NaiveDate,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Debug, Default)]
struct TradeResult {
    exit_date: Option<NaiveDate>,
    exit_price: Option<f64>,
    realized_ret_pct: Option<f64>,
    max_favorable_pct: Option<f64>,
    max_adverse_pct: Option<f64>,
}

impl TradeResult {
    fn from_fill(fill: &Fill, future: &[FutureBar]) -> Self {
        let Some(fill_date) = fill.date else {
            return Self::default();
        };
        let Some(fill_price) = fill.price.filter(|p| *p > 0.0) else {
            return Self::default();
        };
        let active_bars: Vec<&FutureBar> = future
            .iter()
            .filter(|bar| bar.trade_date >= fill_date)
            .collect();
        let exit_bar = active_bars
            .iter()
            .find(|bar| bar.trade_date > fill_date)
            .copied();
        let exit_price = exit_bar.map(|bar| bar.close);
        let realized_ret_pct = exit_price.map(|exit| pct_change(exit, fill_price) - SLIPPAGE_PCT);
        let max_favorable_pct = finite_fold(
            active_bars
                .iter()
                .map(|bar| pct_change(bar.high, fill_price)),
            f64::NEG_INFINITY,
            f64::max,
        );
        let max_adverse_pct = finite_fold(
            active_bars
                .iter()
                .map(|bar| pct_change(bar.low, fill_price)),
            f64::INFINITY,
            f64::min,
        );
        Self {
            exit_date: exit_bar.map(|bar| bar.trade_date),
            exit_price,
            realized_ret_pct,
            max_favorable_pct,
            max_adverse_pct,
        }
    }
}

#[derive(Debug)]
struct PaperTrade {
    decision: Decision,
    strategy_family: StrategyFamily,
    strategy_key: String,
    execution_rule: ExecutionRule,
    intent: TradeIntent,
    planned_entry: Option<f64>,
    fill: Fill,
    result: TradeResult,
    shadow_alpha_prob: f64,
    downside_stress: f64,
    stale_chase_risk: f64,
    label: OutcomeLabel,
    detail: Value,
}

#[derive(Debug, Default)]
struct EvStats {
    strategy_family: String,
    samples: usize,
    planned: usize,
    fills: usize,
    wins: usize,
    losses: usize,
    win_sum: f64,
    loss_sum_abs: f64,
    tail_sum_abs: f64,
    downside_sum: f64,
    stale_sum: f64,
}

#[derive(Debug)]
struct EvRow {
    samples: i64,
    ev_pct: f64,
    risk_unit_pct: f64,
    ev_per_risk: f64,
    ev_norm_score: f64,
    eligible: bool,
    fail_reasons: String,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    ensure_schema(db)?;
    let cutoff = as_of - Duration::days(LOOKBACK_DAYS);
    let decisions = load_decisions(db, cutoff, as_of)?;
    db.execute(
        "DELETE FROM paper_trades
         WHERE report_date >= CAST(? AS DATE)
           AND report_date <= CAST(? AS DATE)
           AND session = ?",
        duckdb::params![cutoff.to_string(), as_of.to_string(), SESSION],
    )?;

    let mut trades = Vec::new();
    for decision in decisions {
        trades.push(build_trade(db, decision, as_of)?);
    }
    store_paper_trades(db, &trades, as_of)?;
    let ev_rows = store_strategy_ev(db, &trades, as_of)?;
    write_current_analytics(db, &trades, as_of)?;

    info!(
        trades = trades.len(),
        strategies = ev_rows,
        "paper_trade_ev complete"
    );
    Ok(trades.len())
}

fn ensure_schema(db: &Connection) -> Result<()> {
    db.execute_batch(crate::storage::schema::CREATE_TABLES)?;
    Ok(())
}

fn build_trade(db: &Connection, decision: Decision, as_of: NaiveDate) -> Result<PaperTrade> {
    let strategy_family = StrategyFamily::classify(&decision);
    let intent = TradeIntent::from_decision(&decision);
    let execution_rule = ExecutionRule::from_intent(intent);
    let strategy_key = strategy_key(&decision, strategy_family, execution_rule);
    let shadow_alpha_prob = decision.detail_f64("shadow_option_alpha_prob", 0.50);
    let downside_stress = decision.detail_f64("downside_stress", 0.50);
    let stale_chase_risk = decision.detail_f64("stale_chase_risk", 0.35);
    let planned_entry = execution_rule.planned_entry(&decision);
    let future = load_future_bars(db, &decision.symbol, decision.report_date, as_of)?;
    let fill = execution_rule.simulate(&decision, &future);
    let result = TradeResult::from_fill(&fill, &future);
    let label = OutcomeLabel::from_trade(intent, &fill, &result);

    let detail = json!({
        "strategy_family": strategy_family.as_str(),
        "strategy_key": strategy_key,
        "execution_rule": execution_rule.as_str(),
        "action_intent": intent.as_str(),
        "signal_confidence": decision.signal_confidence,
        "report_bucket": decision.report_lane.as_str(),
        "execution_mode": decision.execution_mode.as_str(),
        "setup_score": round3(decision.setup_score),
        "continuation_score": round3(decision.continuation_score),
        "fade_risk": round3(decision.fade_risk),
        "shadow_alpha_prob": round3(shadow_alpha_prob),
        "downside_stress": round3(downside_stress),
        "stale_chase_risk": round3(stale_chase_risk),
        "flow_conflict_flag": decision.flow_conflict_flag,
        "fill_reason": fill.reason,
        "slippage_pct": SLIPPAGE_PCT,
        "data_scope": "paper_trade_ev_walk_forward",
    });

    Ok(PaperTrade {
        decision,
        strategy_family,
        strategy_key,
        execution_rule,
        intent,
        planned_entry,
        fill,
        result,
        shadow_alpha_prob,
        downside_stress,
        stale_chase_risk,
        label,
        detail,
    })
}

fn load_decisions(db: &Connection, cutoff: NaiveDate, as_of: NaiveDate) -> Result<Vec<Decision>> {
    type RawDecision = (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        f64,
        f64,
        f64,
        f64,
        f64,
        f64,
        Option<String>,
        f64,
    );

    let mut stmt = db.prepare(
        "SELECT
            CAST(d.report_date AS VARCHAR),
            d.symbol,
            d.selection_status,
            COALESCE(d.report_bucket, ''),
            COALESCE(d.signal_direction, ''),
            COALESCE(d.signal_confidence, ''),
            COALESCE(d.execution_mode, ''),
            COALESCE(d.max_chase_gap_pct, 0),
            COALESCE(d.pullback_trigger_pct, 0),
            COALESCE(d.setup_score, 0),
            COALESCE(d.continuation_score, 0),
            COALESCE(d.fade_risk, 0),
            COALESCE(d.reference_close, 0),
            d.details_json,
            COALESCE(f.value, 0) AS flow_conflict_flag
         FROM report_decisions d
         LEFT JOIN analytics f
           ON f.ts_code = d.symbol
          AND f.as_of = d.report_date
          AND f.module = 'flow_audit'
          AND f.metric = 'flow_conflict_flag'
         WHERE d.session = ?
           AND d.report_date >= CAST(? AS DATE)
           AND d.report_date <= CAST(? AS DATE)
           AND d.reference_close > 0",
    )?;
    let rows = stmt.query_map(
        duckdb::params![SESSION, cutoff.to_string(), as_of.to_string()],
        |row| {
            Ok::<RawDecision, duckdb::Error>((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2).unwrap_or_default(),
                row.get::<_, String>(3).unwrap_or_default(),
                row.get::<_, String>(4).unwrap_or_default(),
                row.get::<_, String>(5).unwrap_or_default(),
                row.get::<_, String>(6).unwrap_or_default(),
                row.get::<_, f64>(7).unwrap_or(0.0),
                row.get::<_, f64>(8).unwrap_or(0.0),
                row.get::<_, f64>(9).unwrap_or(0.0),
                row.get::<_, f64>(10).unwrap_or(0.0),
                row.get::<_, f64>(11).unwrap_or(0.0),
                row.get::<_, f64>(12).unwrap_or(0.0),
                row.get::<_, Option<String>>(13).ok().flatten(),
                row.get::<_, f64>(14).unwrap_or(0.0),
            ))
        },
    )?;

    let mut out = Vec::new();
    for row in rows.filter_map(|r| r.ok()) {
        let (
            report_date,
            symbol,
            selection_status,
            report_bucket,
            signal_direction,
            signal_confidence,
            execution_mode,
            max_chase_gap_pct,
            pullback_trigger_pct,
            setup_score,
            continuation_score,
            fade_risk,
            reference_close,
            details_raw,
            flow_conflict_flag,
        ) = row;
        out.push(Decision {
            report_date: parse_sql_date(&report_date)?,
            symbol,
            selection_status: SelectionStatus::parse(&selection_status),
            selection_status_raw: selection_status,
            report_lane: ReportLane::parse(&report_bucket),
            direction: Direction::parse(&signal_direction),
            signal_confidence,
            execution_mode: ExecutionMode::parse(&execution_mode),
            max_chase_gap_pct,
            pullback_trigger_pct,
            setup_score,
            continuation_score,
            fade_risk,
            reference_close,
            details: details_raw.and_then(|raw| serde_json::from_str(&raw).ok()),
            flow_conflict_flag: flow_conflict_flag >= 0.5,
        });
    }
    Ok(out)
}

fn store_paper_trades(db: &Connection, trades: &[PaperTrade], as_of: NaiveDate) -> Result<usize> {
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO paper_trades (
            report_date, session, symbol, selection_status, strategy_family,
            strategy_key, execution_rule, action_intent, evaluation_date,
            reference_close, planned_entry, fill_date, fill_price, exit_date, exit_price,
            fill_status, realized_ret_pct, max_favorable_pct, max_adverse_pct,
            shadow_alpha_prob, downside_stress, stale_chase_risk, flow_conflict_flag,
            label, detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;
    for trade in trades {
        insert.execute(duckdb::params![
            trade.decision.report_date.to_string(),
            SESSION,
            trade.decision.symbol,
            trade.decision.selection_status_raw,
            trade.strategy_family.as_str(),
            trade.strategy_key,
            trade.execution_rule.as_str(),
            trade.intent.as_str(),
            as_of.to_string(),
            trade.decision.reference_close,
            trade.planned_entry,
            trade.fill.date.map(|v| v.to_string()),
            trade.fill.price,
            trade.result.exit_date.map(|v| v.to_string()),
            trade.result.exit_price,
            trade.fill.status.as_str(),
            trade.result.realized_ret_pct,
            trade.result.max_favorable_pct,
            trade.result.max_adverse_pct,
            trade.shadow_alpha_prob,
            trade.downside_stress,
            trade.stale_chase_risk,
            trade.decision.flow_conflict_flag,
            trade.label.as_str(),
            trade.detail.to_string(),
        ])?;
    }
    Ok(trades.len())
}

fn store_strategy_ev(db: &Connection, trades: &[PaperTrade], as_of: NaiveDate) -> Result<usize> {
    db.execute(
        "DELETE FROM strategy_ev WHERE as_of = CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
    )?;
    let mut by_key: HashMap<String, EvStats> = HashMap::new();
    for trade in trades
        .iter()
        .filter(|t| t.decision.report_date < as_of && t.intent == TradeIntent::Trade)
    {
        let stats = by_key.entry(trade.strategy_key.clone()).or_default();
        stats.strategy_family = trade.strategy_family.as_str().to_string();
        stats.samples += 1;
        stats.planned += 1;
        stats.downside_sum += trade.downside_stress;
        stats.stale_sum += trade.stale_chase_risk;
        if let Some(ret) = trade.result.realized_ret_pct {
            stats.fills += 1;
            match ret {
                r if r >= WIN_THRESHOLD_PCT => {
                    stats.wins += 1;
                    stats.win_sum += r;
                }
                r if r <= LOSS_THRESHOLD_PCT => {
                    stats.losses += 1;
                    stats.loss_sum_abs += r.abs();
                }
                _ => {}
            }
        }
        if let Some(max_adverse) = trade.result.max_adverse_pct {
            stats.tail_sum_abs += max_adverse.min(0.0).abs();
        }
    }

    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO strategy_ev (
            as_of, strategy_key, strategy_family, samples, planned_trades, fills,
            wins, losses, fill_rate, win_rate_raw, win_rate_bayes, avg_win_pct,
            avg_loss_pct, avg_tail_loss_pct, avg_downside_stress, ev_pct,
            risk_unit_pct, ev_per_risk, ev_norm_score,
            eligible, fail_reasons, detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    let mut count = 0usize;
    for (key, stats) in by_key {
        let fills = stats.fills;
        let fill_rate = ratio(stats.fills, stats.planned);
        let win_rate_raw = ratio(stats.wins, fills);
        let win_rate_bayes = (stats.wins as f64 + 2.0) / (fills as f64 + 4.0);
        let avg_win = average_or(stats.win_sum, stats.wins, 1.2);
        let avg_loss = average_or(stats.loss_sum_abs, stats.losses, 1.0);
        let avg_tail = average_or(stats.tail_sum_abs, fills, 1.0);
        let avg_downside = average_or(stats.downside_sum, stats.samples, 0.5);
        let avg_stale = average_or(stats.stale_sum, stats.samples, 0.35);
        let tail_penalty =
            0.20 * (avg_tail - avg_loss).max(0.0) + 0.55 * avg_downside + 0.35 * avg_stale;
        let ev_pct = win_rate_bayes * avg_win
            - (1.0 - win_rate_bayes) * avg_loss
            - SLIPPAGE_PCT
            - tail_penalty;
        let (risk_unit_pct, ev_per_risk, ev_norm_score) =
            normalized_ev_metrics(ev_pct, avg_loss, avg_tail, avg_downside, avg_stale);

        let fail = ev_fail_reasons(stats.samples, fills, fill_rate, ev_pct, avg_tail);
        let eligible = fail.is_empty();
        let detail = json!({
            "ev_formula": "p_win_bayes*avg_win - (1-p_win_bayes)*avg_loss - slippage - tail_penalty",
            "normalized_ev_formula": "ev_per_risk = ev_pct / risk_unit_pct; ev_norm_score = sigmoid(ev_per_risk) mapped to 0-100",
            "slippage_pct": SLIPPAGE_PCT,
            "avg_stale_chase_risk": round3(avg_stale),
            "tail_penalty_pct": round3(tail_penalty),
            "risk_unit_pct": round3(risk_unit_pct),
            "ev_per_risk": round3(ev_per_risk),
            "ev_norm_score": round3(ev_norm_score),
            "min_samples": 8,
            "min_fills": 4,
            "min_fill_rate": 0.35,
            "min_ev_pct": 0.15,
            "state_machine": "Decision->StrategyFamily->TradeIntent->ExecutionRule->Fill->Outcome->EV",
        })
        .to_string();

        insert.execute(duckdb::params![
            as_of.to_string(),
            key,
            stats.strategy_family,
            stats.samples as i64,
            stats.planned as i64,
            stats.fills as i64,
            stats.wins as i64,
            stats.losses as i64,
            fill_rate,
            win_rate_raw,
            win_rate_bayes,
            avg_win,
            avg_loss,
            avg_tail,
            avg_downside,
            ev_pct,
            risk_unit_pct,
            ev_per_risk,
            ev_norm_score,
            eligible,
            fail.join(","),
            detail,
        ])?;
        count += 1;
    }
    Ok(count)
}

fn write_current_analytics(db: &Connection, trades: &[PaperTrade], as_of: NaiveDate) -> Result<()> {
    db.execute(
        "DELETE FROM analytics WHERE as_of = CAST(? AS DATE) AND module = 'paper_trade_ev'",
        duckdb::params![as_of.to_string()],
    )?;
    let ev_map = load_ev_map(db, as_of)?;
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, 'paper_trade_ev', ?, ?, ?)",
    )?;
    for trade in trades.iter().filter(|t| t.decision.report_date == as_of) {
        let ev = ev_map.get(&trade.strategy_key);
        let ev_pct = ev.map(|row| row.ev_pct).unwrap_or(0.0);
        let risk_unit_pct = ev.map(|row| row.risk_unit_pct).unwrap_or(0.0);
        let ev_per_risk = ev.map(|row| row.ev_per_risk).unwrap_or(0.0);
        let ev_norm_score = ev.map(|row| row.ev_norm_score).unwrap_or(0.0);
        let eligible = ev.map(|row| row.eligible).unwrap_or(false);
        let samples = ev.map(|row| row.samples as f64).unwrap_or(0.0);
        let detail = json!({
            "strategy_family": trade.strategy_family.as_str(),
            "strategy_key": trade.strategy_key,
            "execution_rule": trade.execution_rule.as_str(),
            "action_intent": trade.intent.as_str(),
            "planned_entry": trade.planned_entry.map(round2),
            "ev_pct": round3(ev_pct),
            "risk_unit_pct": round3(risk_unit_pct),
            "ev_per_risk": round3(ev_per_risk),
            "ev_norm_score": round3(ev_norm_score),
            "strategy_samples": samples,
            "eligible": eligible,
            "fail_reasons": ev.map(|row| row.fail_reasons.clone()).unwrap_or_else(|| "no_history".to_string()),
            "shadow_alpha_prob": round3(trade.shadow_alpha_prob),
            "downside_stress": round3(trade.downside_stress),
            "stale_chase_risk": round3(trade.stale_chase_risk),
            "flow_conflict_flag": trade.decision.flow_conflict_flag,
        })
        .to_string();
        for (metric, value) in [
            ("ev_pct", ev_pct),
            ("risk_unit_pct", risk_unit_pct),
            ("ev_per_risk", ev_per_risk),
            ("ev_norm_score", ev_norm_score),
            ("strategy_samples", samples),
            ("eligible", if eligible { 1.0 } else { 0.0 }),
        ] {
            insert.execute(duckdb::params![
                &trade.decision.symbol,
                as_of.to_string(),
                metric,
                value,
                &detail
            ])?;
        }
    }
    Ok(())
}

fn load_ev_map(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, EvRow>> {
    let mut stmt = db.prepare(
        "SELECT strategy_key, COALESCE(samples, 0), COALESCE(ev_pct, 0),
                COALESCE(risk_unit_pct, 0), COALESCE(ev_per_risk, 0),
                COALESCE(ev_norm_score, 0), eligible, COALESCE(fail_reasons, '')
         FROM strategy_ev
         WHERE as_of = CAST(? AS DATE)",
    )?;
    let rows = stmt.query_map(duckdb::params![as_of.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            EvRow {
                samples: row.get::<_, i64>(1).unwrap_or(0),
                ev_pct: row.get::<_, f64>(2).unwrap_or(0.0),
                risk_unit_pct: row.get::<_, f64>(3).unwrap_or(0.0),
                ev_per_risk: row.get::<_, f64>(4).unwrap_or(0.0),
                ev_norm_score: row.get::<_, f64>(5).unwrap_or(0.0),
                eligible: row.get::<_, bool>(6).unwrap_or(false),
                fail_reasons: row.get::<_, String>(7).unwrap_or_default(),
            },
        ))
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

fn load_future_bars(
    db: &Connection,
    symbol: &str,
    report_date: NaiveDate,
    as_of: NaiveDate,
) -> Result<Vec<FutureBar>> {
    let mut stmt = db.prepare(
        "SELECT CAST(trade_date AS VARCHAR), open, high, low, close
         FROM prices
         WHERE ts_code = ?
           AND trade_date > CAST(? AS DATE)
           AND trade_date <= CAST(? AS DATE)
         ORDER BY trade_date
         LIMIT 2",
    )?;
    let rows = stmt.query_map(
        duckdb::params![symbol, report_date.to_string(), as_of.to_string()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1).unwrap_or(0.0),
                row.get::<_, f64>(2).unwrap_or(0.0),
                row.get::<_, f64>(3).unwrap_or(0.0),
                row.get::<_, f64>(4).unwrap_or(0.0),
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows.filter_map(|r| r.ok()) {
        out.push(FutureBar {
            trade_date: parse_sql_date(&row.0)?,
            open: row.1,
            high: row.2,
            low: row.3,
            close: row.4,
        });
    }
    Ok(out)
}

fn strategy_key(
    decision: &Decision,
    family: StrategyFamily,
    execution_rule: ExecutionRule,
) -> String {
    let shadow_bucket = match decision.detail_f64("downside_stress", 0.5) {
        v if v >= 0.60 => "shadow_high",
        v if v >= 0.35 => "shadow_mid",
        _ => "shadow_low",
    };
    let setup_bucket = match decision.setup_score {
        v if v >= 0.65 => "setup_strong",
        v if v >= 0.50 => "setup_mixed",
        _ => "setup_weak",
    };
    format!(
        "{}|{}|{}|{}|{}",
        family.as_str(),
        decision.report_lane.key(),
        execution_rule.as_str(),
        shadow_bucket,
        setup_bucket
    )
}

fn ev_fail_reasons(
    samples: usize,
    fills: usize,
    fill_rate: f64,
    ev_pct: f64,
    avg_tail: f64,
) -> Vec<&'static str> {
    [
        (samples < 8, "samples_lt_8"),
        (fills < 4, "fills_lt_4"),
        (fill_rate < 0.35, "fill_rate_lt_35pct"),
        (ev_pct <= 0.15, "ev_not_positive_enough"),
        (avg_tail > 5.5, "tail_loss_gt_5_5pct"),
    ]
    .into_iter()
    .filter_map(|(failed, reason)| failed.then_some(reason))
    .collect()
}

fn finite_fold<I, F>(values: I, init: f64, f: F) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
    F: Fn(f64, f64) -> f64,
{
    let mut acc = init;
    let mut seen = false;
    for value in values {
        if value.is_finite() {
            acc = f(acc, value);
            seen = true;
        }
    }
    seen.then_some(acc)
}

fn ratio(num: usize, den: usize) -> f64 {
    match den {
        0 => 0.0,
        _ => num as f64 / den as f64,
    }
}

fn average_or(sum: f64, n: usize, default: f64) -> f64 {
    match n {
        0 => default,
        _ => sum / n as f64,
    }
}

fn normalized_ev_metrics(
    ev_pct: f64,
    avg_loss_pct: f64,
    avg_tail_loss_pct: f64,
    avg_downside_stress: f64,
    avg_stale_chase_risk: f64,
) -> (f64, f64, f64) {
    let risk_unit_pct = avg_loss_pct.max(avg_tail_loss_pct).max(0.50)
        + 0.60 * avg_downside_stress
        + 0.35 * avg_stale_chase_risk
        + SLIPPAGE_PCT;
    let ev_per_risk = ev_pct / risk_unit_pct.max(0.25);
    let ev_norm_score = 100.0 / (1.0 + (-3.0 * ev_per_risk).exp());
    (risk_unit_pct, ev_per_risk, ev_norm_score)
}

fn pct_change(target: f64, base: f64) -> f64 {
    match (target > 0.0, base > 0.0) {
        (true, true) => (target / base - 1.0) * 100.0,
        _ => 0.0,
    }
}

fn parse_sql_date(raw: &str) -> Result<NaiveDate> {
    let trimmed = raw.trim();
    let date_part = trimmed.get(0..10).unwrap_or(trimmed);
    Ok(NaiveDate::parse_from_str(date_part, "%Y-%m-%d")?)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision() -> Decision {
        Decision {
            report_date: NaiveDate::from_ymd_opt(2026, 4, 24).unwrap(),
            symbol: "603444.SH".to_string(),
            selection_status: SelectionStatus::Selected,
            selection_status_raw: "selected".to_string(),
            report_lane: ReportLane::CoreBook,
            direction: Direction::Bullish,
            signal_confidence: "HIGH".to_string(),
            execution_mode: ExecutionMode::Executable,
            max_chase_gap_pct: 2.5,
            pullback_trigger_pct: 1.1,
            setup_score: 0.69,
            continuation_score: 0.66,
            fade_risk: 0.29,
            reference_close: 401.97,
            details: Some(json!({"p_upside": 0.98, "downside_stress": 0.45})),
            flow_conflict_flag: false,
        }
    }

    #[test]
    fn earnings_setup_family_wins_over_plain_momentum() {
        let d = decision();
        let family = StrategyFamily::classify(&d);
        assert_eq!(family, StrategyFamily::EarningsSetup);
        assert!(strategy_key(&d, family, ExecutionRule::NextOpenOrPullback)
            .contains("earnings_setup|core_book"));
    }

    #[test]
    fn decision_state_maps_to_trade_rule() {
        let d = decision();
        let intent = TradeIntent::from_decision(&d);
        assert_eq!(intent, TradeIntent::Trade);
        assert_eq!(
            ExecutionRule::from_intent(intent),
            ExecutionRule::NextOpenOrPullback
        );
    }

    #[test]
    fn non_core_selected_is_observe_not_trade() {
        let mut d = decision();
        d.report_lane = ReportLane::ThemeRotation;
        let intent = TradeIntent::from_decision(&d);
        assert_eq!(intent, TradeIntent::Observe);
        assert_eq!(
            ExecutionRule::from_intent(intent),
            ExecutionRule::ObserveOnly
        );
    }

    #[test]
    fn pullback_only_fills_only_when_low_touches_entry() {
        let d = decision();
        let bars = vec![FutureBar {
            trade_date: NaiveDate::from_ymd_opt(2026, 4, 27).unwrap(),
            open: 405.0,
            high: 410.0,
            low: 397.0,
            close: 402.0,
        }];
        let fill = ExecutionRule::PullbackOnly.simulate(&d, &bars);
        assert!(fill.is_filled());
        assert_eq!(fill.status, FillStatus::FilledPullback);
    }
}
