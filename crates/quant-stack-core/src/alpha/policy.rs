use super::{
    source::{is_fill, mean, median, round_opt, round_value, table_exists},
    PolicyCandidate, TradeRow,
};
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::{params, Connection};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Debug, Clone, Copy)]
struct Thresholds {
    min_fills: usize,
    min_active_buckets: usize,
    min_avg_trade_pct: f64,
    min_median_trade_pct: f64,
    min_strict_win_rate: f64,
    min_max_drawdown_pct: f64,
    max_top1_winner_contribution: f64,
}

fn thresholds(market: &str) -> Thresholds {
    match market {
        "cn" => Thresholds {
            min_fills: 50,
            min_active_buckets: 15,
            min_avg_trade_pct: 0.30,
            min_median_trade_pct: 0.0,
            min_strict_win_rate: 0.43,
            min_max_drawdown_pct: -8.0,
            max_top1_winner_contribution: 0.25,
        },
        _ => Thresholds {
            min_fills: 20,
            min_active_buckets: 10,
            min_avg_trade_pct: 0.40,
            min_median_trade_pct: 0.0,
            min_strict_win_rate: 0.45,
            min_max_drawdown_pct: -25.0,
            max_top1_winner_contribution: 0.45,
        },
    }
}

fn max_drawdown_pct(daily_returns: &[(String, f64)]) -> Option<f64> {
    if daily_returns.is_empty() {
        return None;
    }
    let mut equity = 0.0;
    let mut peak = 0.0;
    let mut max_dd = 0.0;
    for (_, ret) in daily_returns {
        equity += ret;
        if equity > peak {
            peak = equity;
        }
        let dd = equity - peak;
        if dd < max_dd {
            max_dd = dd;
        }
    }
    Some(max_dd)
}

fn top1_winner_contribution(returns: &[f64]) -> Option<f64> {
    let winners: Vec<f64> = returns.iter().copied().filter(|ret| *ret > 0.0).collect();
    let total = winners.iter().sum::<f64>();
    if total <= 0.0 {
        None
    } else {
        winners.into_iter().reduce(f64::max).map(|max| max / total)
    }
}

pub(super) fn build_policy_candidates(
    rows: &[TradeRow],
    market: &str,
    horizon_days: i64,
    lookback_days: i64,
) -> Vec<PolicyCandidate> {
    let mut grouped: BTreeMap<String, Vec<TradeRow>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry(row.policy_id.clone())
            .or_default()
            .push(row.clone());
    }
    grouped
        .into_iter()
        .map(|(policy_id, policy_rows)| {
            let label = policy_rows
                .first()
                .map(|row| row.policy_label.clone())
                .unwrap_or_else(|| policy_id.clone());
            evaluate_policy(
                market,
                &policy_id,
                &label,
                &policy_rows,
                horizon_days,
                lookback_days,
            )
        })
        .collect()
}

fn evaluate_policy(
    market: &str,
    policy_id: &str,
    policy_label: &str,
    rows: &[TradeRow],
    horizon_days: i64,
    lookback_days: i64,
) -> PolicyCandidate {
    let thresholds = thresholds(market);
    let fills: Vec<&TradeRow> = rows.iter().filter(|row| is_fill(row)).collect();
    let returns: Vec<f64> = fills.iter().filter_map(|row| row.return_pct).collect();
    let active_dates: BTreeSet<String> = fills
        .iter()
        .filter_map(|row| row.report_date.clone())
        .collect();
    let mut daily: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for row in fills {
        if let (Some(report_date), Some(ret)) = (&row.report_date, row.return_pct) {
            daily.entry(report_date.clone()).or_default().push(ret);
        }
    }
    let daily_returns: Vec<(String, f64)> = daily
        .into_iter()
        .filter_map(|(date, vals)| mean(&vals).map(|avg| (date, avg)))
        .collect();
    let avg_trade = mean(&returns);
    let median_trade = median(&returns);
    let win_rate = if returns.is_empty() {
        None
    } else {
        Some(returns.iter().filter(|ret| **ret > 0.0).count() as f64 / returns.len() as f64)
    };
    let max_dd = max_drawdown_pct(&daily_returns);
    let top1 = top1_winner_contribution(&returns);
    let fail_reasons = stability_fail_reasons(
        returns.len(),
        active_dates.len(),
        avg_trade,
        median_trade,
        win_rate,
        max_dd,
        top1,
        thresholds,
    );
    let mut fail_reasons = fail_reasons;
    fail_reasons.extend(policy_scope_fail_reasons(policy_id));
    let score = stability_score(returns.len(), avg_trade, win_rate, max_dd, top1, thresholds);
    PolicyCandidate {
        market: market.to_string(),
        policy_id: policy_id.to_string(),
        policy_label: policy_label.to_string(),
        horizon_days,
        lookback_days,
        fills: returns.len(),
        active_buckets: active_dates.len(),
        avg_trade_pct: round_opt(avg_trade, 6),
        median_trade_pct: round_opt(median_trade, 6),
        strict_win_rate: round_opt(win_rate, 6),
        max_drawdown_pct: round_opt(max_dd, 6),
        top1_winner_contribution: round_opt(top1, 6),
        stability_score: round_value(score, 6),
        eligible: fail_reasons.is_empty(),
        fail_reasons,
        selected: false,
    }
}

fn stability_fail_reasons(
    fills: usize,
    active_buckets: usize,
    avg_trade_pct: Option<f64>,
    median_trade_pct: Option<f64>,
    strict_win_rate: Option<f64>,
    max_drawdown: Option<f64>,
    top1_contribution: Option<f64>,
    thresholds: Thresholds,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if fills < thresholds.min_fills {
        reasons.push(format!("fills<{}", thresholds.min_fills));
    }
    if active_buckets < thresholds.min_active_buckets {
        reasons.push(format!("active_buckets<{}", thresholds.min_active_buckets));
    }
    if avg_trade_pct.map_or(true, |v| v <= thresholds.min_avg_trade_pct) {
        reasons.push(format!("avg_trade_pct<={}", thresholds.min_avg_trade_pct));
    }
    if median_trade_pct.map_or(true, |v| v < thresholds.min_median_trade_pct) {
        reasons.push(format!(
            "median_trade_pct<{}",
            thresholds.min_median_trade_pct
        ));
    }
    if strict_win_rate.map_or(true, |v| v <= thresholds.min_strict_win_rate) {
        reasons.push(format!(
            "strict_win_rate<={}",
            thresholds.min_strict_win_rate
        ));
    }
    if max_drawdown.map_or(true, |v| v <= thresholds.min_max_drawdown_pct) {
        reasons.push(format!(
            "max_drawdown_pct<={}",
            thresholds.min_max_drawdown_pct
        ));
    }
    match top1_contribution {
        None => reasons.push("top1_winner_contribution=NA".to_string()),
        Some(v) if v > thresholds.max_top1_winner_contribution => reasons.push(format!(
            "top1_winner_contribution>{}",
            thresholds.max_top1_winner_contribution
        )),
        _ => {}
    }
    reasons
}

fn policy_scope_fail_reasons(policy_id: &str) -> Vec<String> {
    let parts: Vec<&str> = policy_id.split(':').collect();
    if parts.len() < 6 {
        return vec!["policy_scope_unparseable".to_string()];
    }
    let mut reasons = Vec::new();
    if parts[1] != "core" {
        reasons.push("policy_bucket_not_core".to_string());
    }
    if !matches!(parts[2], "long" | "short") {
        reasons.push("policy_direction_not_tradeable".to_string());
    }
    if parts[3] != "high_mod" {
        reasons.push("policy_confidence_not_high_mod".to_string());
    }
    if parts[4] != "executable_now" {
        reasons.push("policy_execution_not_now".to_string());
    }
    reasons
}

fn stability_score(
    fills: usize,
    avg_trade_pct: Option<f64>,
    strict_win_rate: Option<f64>,
    max_drawdown: Option<f64>,
    top1_contribution: Option<f64>,
    thresholds: Thresholds,
) -> f64 {
    if fills == 0 || avg_trade_pct.is_none() || strict_win_rate.is_none() {
        return 0.0;
    }
    let fill_factor = ((fills as f64 / thresholds.min_fills.max(1) as f64).sqrt()).min(2.0);
    let edge = avg_trade_pct.unwrap_or(0.0).max(0.0);
    let win = strict_win_rate.unwrap_or(0.0).max(0.0);
    let concentration = 1.0 - top1_contribution.unwrap_or(1.0).clamp(0.0, 1.0);
    let dd_penalty = max_drawdown
        .filter(|dd| *dd < 0.0)
        .map(|dd| (1.0 + dd / 100.0).max(0.1))
        .unwrap_or(1.0);
    edge * win * fill_factor * concentration * dd_penalty
}

pub(super) fn load_previous_champion(
    history_db: &Path,
    market: &str,
    as_of: NaiveDate,
) -> Result<Option<String>> {
    if !history_db.exists() {
        return Ok(None);
    }
    let con = Connection::open(history_db)?;
    if !table_exists(&con, "playbook_selection")? {
        return Ok(None);
    }
    let mut stmt = con.prepare(
        "SELECT selected_policy_id
         FROM playbook_selection
         WHERE market = ?
           AND as_of < CAST(? AS DATE)
           AND selected_policy_id IS NOT NULL
         ORDER BY as_of DESC
         LIMIT 1",
    )?;
    let mut rows = stmt.query(params![market, as_of.to_string()])?;
    if let Some(row) = rows.next()? {
        Ok(row.get::<_, Option<String>>(0)?)
    } else {
        Ok(None)
    }
}

pub(super) fn select_champion(
    candidates: &[PolicyCandidate],
    previous_policy_id: Option<&str>,
    challenger_margin: f64,
) -> (Option<String>, String) {
    let eligible: Vec<&PolicyCandidate> = candidates.iter().filter(|c| c.eligible).collect();
    if eligible.is_empty() {
        return (None, "no eligible policy passed stability gate".to_string());
    }
    let challenger = eligible
        .iter()
        .max_by(|a, b| {
            a.stability_score
                .partial_cmp(&b.stability_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.fills.cmp(&b.fills))
        })
        .copied()
        .unwrap();
    if let Some(previous_id) = previous_policy_id {
        if let Some(previous) = eligible
            .iter()
            .copied()
            .find(|c| c.policy_id == previous_id)
        {
            if challenger.policy_id == previous_id {
                return (
                    Some(previous_id.to_string()),
                    "incumbent remains top eligible policy".to_string(),
                );
            }
            if challenger.stability_score <= previous.stability_score * (1.0 + challenger_margin) {
                return (
                    Some(previous_id.to_string()),
                    "incumbent held; challenger did not clear 15% score margin".to_string(),
                );
            }
            return (
                Some(challenger.policy_id.clone()),
                "challenger replaced incumbent after clearing 15% score margin".to_string(),
            );
        }
    }
    (
        Some(challenger.policy_id.clone()),
        "selected highest stability score among eligible policies".to_string(),
    )
}

pub(super) fn mark_selected(candidates: &mut [PolicyCandidate], selected_policy_id: Option<&str>) {
    for candidate in candidates {
        candidate.selected = selected_policy_id == Some(candidate.policy_id.as_str());
    }
}
