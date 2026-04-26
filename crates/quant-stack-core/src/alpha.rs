use anyhow::Result;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;

mod bulletin;
mod policy;
mod source;
mod storage;

pub use bulletin::render_market_bulletin_md;
pub use storage::migrate;

#[derive(Debug, Clone)]
pub struct AlphaEvalConfig {
    pub as_of: NaiveDate,
    pub markets: Vec<String>,
    pub lookback_days: i64,
    pub auto_select: bool,
    pub emit_bulletin: bool,
    pub history_db: PathBuf,
    pub output_root: PathBuf,
    pub us_db: PathBuf,
    pub cn_db: PathBuf,
    pub us_horizon_days: i64,
    pub cn_horizon_days: i64,
    pub write_project_copies: bool,
}

#[derive(Debug, Clone)]
struct MarketConfig {
    market: String,
    db_path: PathBuf,
    horizon_days: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyCandidate {
    pub market: String,
    pub policy_id: String,
    pub policy_label: String,
    pub horizon_days: i64,
    pub lookback_days: i64,
    pub fills: usize,
    pub active_buckets: usize,
    pub avg_trade_pct: Option<f64>,
    pub median_trade_pct: Option<f64>,
    pub strict_win_rate: Option<f64>,
    pub max_drawdown_pct: Option<f64>,
    pub top1_winner_contribution: Option<f64>,
    pub stability_score: f64,
    pub eligible: bool,
    pub fail_reasons: Vec<String>,
    pub selected: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradeRow {
    pub market: String,
    pub report_date: Option<String>,
    pub evaluation_date: Option<String>,
    pub symbol: String,
    pub selection_status: Option<String>,
    pub rank_order: Option<i64>,
    pub report_bucket: Option<String>,
    pub signal_direction: Option<String>,
    pub signal_confidence: Option<String>,
    pub headline_mode: Option<String>,
    pub execution_mode: Option<String>,
    pub composite_score: Option<f64>,
    pub rr_ratio: Option<f64>,
    pub primary_reason: Option<String>,
    pub details_json: Option<String>,
    pub action_intent: Option<String>,
    pub executable: Option<bool>,
    pub return_pct: Option<f64>,
    pub best_possible_ret_pct: Option<f64>,
    pub stale_chase: Option<bool>,
    pub no_fill_reason: Option<String>,
    pub label: Option<String>,
    pub calibration_bucket: Option<String>,
    pub policy_id: String,
    pub policy_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulletinItem {
    pub market: String,
    pub symbol: String,
    pub section: String,
    pub policy_id: String,
    pub policy_label: String,
    pub report_bucket: Option<String>,
    pub signal_direction: Option<String>,
    pub signal_confidence: Option<String>,
    pub headline_mode: Option<String>,
    pub execution_mode: Option<String>,
    pub reason: String,
    pub blockers: Vec<String>,
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaBulletin {
    pub as_of: String,
    pub evaluated_through: BTreeMap<String, String>,
    pub selected_policies: BTreeMap<String, Option<String>>,
    #[serde(default)]
    pub tactical_policies: BTreeMap<String, Option<String>>,
    pub stability: BTreeMap<String, Vec<PolicyCandidate>>,
    pub execution_alpha: Vec<BulletinItem>,
    #[serde(default)]
    pub tactical_alpha: Vec<BulletinItem>,
    pub options_alpha: Vec<BulletinItem>,
    pub recall_alpha: Vec<BulletinItem>,
    pub blocked_alpha: Vec<BulletinItem>,
}

#[derive(Debug, Clone)]
struct SelectionRow {
    market: String,
    selected_policy_id: Option<String>,
    previous_policy_id: Option<String>,
    stability_score: Option<f64>,
    challenger_policy_id: Option<String>,
    challenger_score: Option<f64>,
    selection_reason: String,
}

pub fn evaluate(config: &AlphaEvalConfig) -> Result<AlphaBulletin> {
    let requested: HashSet<String> = config.markets.iter().map(|m| m.to_lowercase()).collect();
    let markets = vec![
        MarketConfig {
            market: "us".to_string(),
            db_path: config.us_db.clone(),
            horizon_days: config.us_horizon_days,
        },
        MarketConfig {
            market: "cn".to_string(),
            db_path: config.cn_db.clone(),
            horizon_days: config.cn_horizon_days,
        },
    ]
    .into_iter()
    .filter(|market| requested.is_empty() || requested.contains(&market.market))
    .collect::<Vec<_>>();

    let mut evaluated_through = BTreeMap::new();
    let mut candidates_by_market: BTreeMap<String, Vec<PolicyCandidate>> = BTreeMap::new();
    let mut current_by_market: BTreeMap<String, Vec<TradeRow>> = BTreeMap::new();
    let mut options_by_market: BTreeMap<String, Vec<BulletinItem>> = BTreeMap::new();
    let mut selected_policies = BTreeMap::new();
    let mut selection_rows = Vec::new();
    let mut evaluated_trade_rows = Vec::new();
    let mut selected_trade_rows = Vec::new();

    for market in markets {
        let (rows, eval_through) = source::load_evaluated_trades(
            &market.db_path,
            &market.market,
            config.as_of,
            config.lookback_days,
            market.horizon_days,
        )?;
        evaluated_through.insert(market.market.clone(), eval_through);
        let mut candidates = policy::build_policy_candidates(
            &rows,
            &market.market,
            market.horizon_days,
            config.lookback_days,
        );
        let previous =
            policy::load_previous_champion(&config.history_db, &market.market, config.as_of)?;
        let (selected, reason) = if config.auto_select {
            policy::select_champion(&candidates, previous.as_deref(), 0.15)
        } else {
            (None, "auto-select disabled".to_string())
        };
        policy::mark_selected(&mut candidates, selected.as_deref());
        selected_policies.insert(market.market.clone(), selected.clone());
        current_by_market.insert(
            market.market.clone(),
            source::load_current_candidates(
                &market.db_path,
                &market.market,
                config.as_of,
                market.horizon_days,
            )?,
        );
        options_by_market.insert(
            market.market.clone(),
            source::load_options_alpha_candidates(&market.db_path, &market.market, config.as_of)?,
        );

        let selected_candidate = candidates.iter().find(|c| c.selected);
        let challenger = candidates.iter().filter(|c| c.eligible).max_by(|a, b| {
            a.stability_score
                .partial_cmp(&b.stability_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        selection_rows.push(SelectionRow {
            market: market.market.clone(),
            selected_policy_id: selected.clone(),
            previous_policy_id: previous,
            stability_score: selected_candidate.map(|c| c.stability_score),
            challenger_policy_id: challenger.map(|c| c.policy_id.clone()),
            challenger_score: challenger.map(|c| c.stability_score),
            selection_reason: reason,
        });
        evaluated_trade_rows.extend(rows.iter().cloned());
        if let Some(policy) = selected {
            selected_trade_rows.extend(rows.iter().filter(|row| row.policy_id == policy).cloned());
        }
        candidates_by_market.insert(market.market.clone(), candidates);
    }

    let bulletin = bulletin::build_bulletin(
        config.as_of,
        evaluated_through,
        selected_policies,
        &candidates_by_market,
        &current_by_market,
        &options_by_market,
    );

    let output_dir = config.output_root.join(config.as_of.to_string());
    fs::create_dir_all(&output_dir)?;
    fs::write(
        output_dir.join("strategy_backtest_report.md"),
        storage::strategy_report_md(&bulletin, &candidates_by_market),
    )?;
    storage::write_result_tables(
        &output_dir.join("strategy_backtest.duckdb"),
        config.as_of,
        &bulletin,
        &candidates_by_market,
        &current_by_market,
        &selection_rows,
        &evaluated_trade_rows,
        &selected_trade_rows,
        "post",
    )?;
    storage::write_result_tables(
        &config.history_db,
        config.as_of,
        &bulletin,
        &candidates_by_market,
        &current_by_market,
        &selection_rows,
        &evaluated_trade_rows,
        &selected_trade_rows,
        "post",
    )?;
    if config.emit_bulletin {
        storage::write_bulletin_files(&output_dir, &bulletin)?;
        if config.write_project_copies {
            storage::write_project_bulletin_copies(config, &bulletin)?;
        }
    }
    Ok(bulletin)
}
