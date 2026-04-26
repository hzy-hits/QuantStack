use super::{
    bulletin::render_market_bulletin_md,
    source::{is_fill, table_exists},
    AlphaBulletin, AlphaEvalConfig, PolicyCandidate, SelectionRow, TradeRow,
};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use duckdb::{params, Connection};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

fn ensure_result_schema(con: &Connection) -> Result<()> {
    con.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS playbook_candidates (
            as_of DATE, evaluated_through DATE, market VARCHAR, policy_id VARCHAR,
            policy_label VARCHAR, horizon_days INTEGER, lookback_days INTEGER,
            fills INTEGER, active_buckets INTEGER, avg_trade_pct DOUBLE,
            median_trade_pct DOUBLE, strict_win_rate DOUBLE, max_drawdown_pct DOUBLE,
            top1_winner_contribution DOUBLE, stability_score DOUBLE, eligible BOOLEAN,
            fail_reasons VARCHAR, selected BOOLEAN, created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS playbook_selection (
            as_of DATE, evaluated_through DATE, market VARCHAR, selected_policy_id VARCHAR,
            previous_policy_id VARCHAR, stability_score DOUBLE, challenger_policy_id VARCHAR,
            challenger_score DOUBLE, selection_reason VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS alpha_bulletin (
            as_of DATE, market VARCHAR, section VARCHAR, symbol VARCHAR, policy_id VARCHAR,
            reason VARCHAR, blockers_json VARCHAR, payload_json VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS selected_trades (
            as_of DATE, market VARCHAR, policy_id VARCHAR, report_date DATE, evaluation_date DATE,
            symbol VARCHAR, return_pct DOUBLE, label VARCHAR, fill_quality VARCHAR, source_json VARCHAR
        );
        CREATE TABLE IF NOT EXISTS bucket_curve (
            as_of DATE, market VARCHAR, policy_id VARCHAR, bucket_date DATE, trade_count INTEGER,
            avg_return_pct DOUBLE, cumulative_return_pct DOUBLE, drawdown_pct DOUBLE
        );
        CREATE TABLE IF NOT EXISTS algo_candidates (
            as_of DATE, market VARCHAR, source VARCHAR, strategy_id VARCHAR, policy_id VARCHAR,
            symbol VARCHAR, report_date DATE, candidate_date DATE, selection_status VARCHAR,
            report_bucket VARCHAR, signal_direction VARCHAR, signal_confidence VARCHAR,
            execution_mode VARCHAR, rank_order INTEGER, composite_score DOUBLE, details_json VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS candidate_outcomes (
            as_of DATE, market VARCHAR, policy_id VARCHAR, symbol VARCHAR, report_date DATE,
            evaluation_date DATE, horizon_days INTEGER, return_pct DOUBLE, label VARCHAR,
            data_ready BOOLEAN, source_json VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS alpha_maturity_daily (
            as_of DATE, evaluated_through DATE, market VARCHAR, policy_id VARCHAR,
            policy_label VARCHAR, maturity_level VARCHAR, horizon_days INTEGER, lookback_days INTEGER,
            fills INTEGER, active_buckets INTEGER, avg_trade_pct DOUBLE, median_trade_pct DOUBLE,
            strict_win_rate DOUBLE, max_drawdown_pct DOUBLE, top1_winner_contribution DOUBLE,
            stability_score DOUBLE, eligible BOOLEAN, fail_reasons VARCHAR, selected BOOLEAN,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS execution_gate_results (
            as_of DATE, market VARCHAR, symbol VARCHAR, policy_id VARCHAR, gate_status VARCHAR,
            section VARCHAR, blockers_json VARCHAR, reason VARCHAR, payload_json VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS daily_alpha_bulletin (
            as_of DATE, market VARCHAR, section VARCHAR, symbol VARCHAR, policy_id VARCHAR,
            reason VARCHAR, blockers_json VARCHAR, payload_json VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS daily_report_model (
            as_of DATE, market VARCHAR, session VARCHAR, model_json VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        ",
    )?;
    Ok(())
}

pub fn migrate(path: &Path, check_only: bool) -> Result<()> {
    if check_only && !path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let con = Connection::open(path)?;
    ensure_result_schema(&con)?;
    if check_only {
        for table in [
            "algo_candidates",
            "candidate_outcomes",
            "alpha_maturity_daily",
            "execution_gate_results",
            "daily_alpha_bulletin",
            "daily_report_model",
        ] {
            if !table_exists(&con, table)? {
                anyhow::bail!("missing table after migration: {table}");
            }
        }
    }
    Ok(())
}

pub(super) fn write_result_tables(
    path: &Path,
    as_of: NaiveDate,
    bulletin: &AlphaBulletin,
    candidates_by_market: &BTreeMap<String, Vec<PolicyCandidate>>,
    current_by_market: &BTreeMap<String, Vec<TradeRow>>,
    selection_rows: &[SelectionRow],
    evaluated_trade_rows: &[TradeRow],
    selected_trade_rows: &[TradeRow],
    session: &str,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let con = Connection::open(path)?;
    ensure_result_schema(&con)?;
    let as_of_s = as_of.to_string();
    for table in [
        "playbook_candidates",
        "playbook_selection",
        "alpha_bulletin",
        "selected_trades",
        "bucket_curve",
        "algo_candidates",
        "candidate_outcomes",
        "alpha_maturity_daily",
        "execution_gate_results",
        "daily_alpha_bulletin",
        "daily_report_model",
    ] {
        con.execute(
            &format!("DELETE FROM {table} WHERE as_of = ?"),
            params![as_of_s],
        )?;
    }

    for (market, candidates) in candidates_by_market {
        for candidate in candidates {
            let fail_reasons = serde_json::to_string(&candidate.fail_reasons)?;
            con.execute(
                "INSERT INTO playbook_candidates
                 (as_of, evaluated_through, market, policy_id, policy_label, horizon_days,
                  lookback_days, fills, active_buckets, avg_trade_pct, median_trade_pct,
                  strict_win_rate, max_drawdown_pct, top1_winner_contribution, stability_score,
                  eligible, fail_reasons, selected)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    as_of_s,
                    bulletin.evaluated_through.get(market).map(String::as_str),
                    market,
                    candidate.policy_id,
                    candidate.policy_label,
                    candidate.horizon_days,
                    candidate.lookback_days,
                    candidate.fills as i64,
                    candidate.active_buckets as i64,
                    candidate.avg_trade_pct,
                    candidate.median_trade_pct,
                    candidate.strict_win_rate,
                    candidate.max_drawdown_pct,
                    candidate.top1_winner_contribution,
                    candidate.stability_score,
                    candidate.eligible,
                    fail_reasons,
                    candidate.selected,
                ],
            )?;
            let maturity_level = if candidate.selected {
                "L3 Execution-eligible"
            } else if candidate.eligible {
                "L2 Shadow"
            } else if candidate.fills > 0 {
                "L1 Recall"
            } else {
                "L0 Research"
            };
            con.execute(
                "INSERT INTO alpha_maturity_daily
                 (as_of, evaluated_through, market, policy_id, policy_label, maturity_level,
                  horizon_days, lookback_days, fills, active_buckets, avg_trade_pct,
                  median_trade_pct, strict_win_rate, max_drawdown_pct, top1_winner_contribution,
                  stability_score, eligible, fail_reasons, selected)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    as_of_s,
                    bulletin.evaluated_through.get(market).map(String::as_str),
                    market,
                    candidate.policy_id,
                    candidate.policy_label,
                    maturity_level,
                    candidate.horizon_days,
                    candidate.lookback_days,
                    candidate.fills as i64,
                    candidate.active_buckets as i64,
                    candidate.avg_trade_pct,
                    candidate.median_trade_pct,
                    candidate.strict_win_rate,
                    candidate.max_drawdown_pct,
                    candidate.top1_winner_contribution,
                    candidate.stability_score,
                    candidate.eligible,
                    fail_reasons,
                    candidate.selected,
                ],
            )?;
        }
    }

    for row in selection_rows {
        con.execute(
            "INSERT INTO playbook_selection
             (as_of, evaluated_through, market, selected_policy_id, previous_policy_id,
              stability_score, challenger_policy_id, challenger_score, selection_reason)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                as_of_s,
                bulletin
                    .evaluated_through
                    .get(&row.market)
                    .map(String::as_str),
                row.market,
                row.selected_policy_id,
                row.previous_policy_id,
                row.stability_score,
                row.challenger_policy_id,
                row.challenger_score,
                row.selection_reason,
            ],
        )?;
    }

    let mut candidate_rows_written = 0usize;
    for rows in current_by_market.values() {
        for row in rows {
            candidate_rows_written += con.execute(
                "INSERT INTO algo_candidates
                 (as_of, market, source, strategy_id, policy_id, symbol, report_date, candidate_date,
                  selection_status, report_bucket, signal_direction, signal_confidence,
                  execution_mode, rank_order, composite_score, details_json)
                 VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    &as_of_s,
                    &row.market,
                    "report_decisions",
                    &row.policy_id,
                    &row.policy_id,
                    &row.symbol,
                    row.report_date.as_deref(),
                    row.report_date.as_deref(),
                    row.selection_status.as_deref(),
                    row.report_bucket.as_deref(),
                    row.signal_direction.as_deref(),
                    row.signal_confidence.as_deref(),
                    row.execution_mode.as_deref(),
                    row.rank_order,
                    row.composite_score,
                    row.details_json.as_deref(),
                ],
            )?;
        }
    }
    if candidate_rows_written == 0 {
        for item in bulletin
            .execution_alpha
            .iter()
            .chain(bulletin.tactical_alpha.iter())
            .chain(bulletin.options_alpha.iter())
            .chain(bulletin.recall_alpha.iter())
            .chain(bulletin.blocked_alpha.iter())
        {
            con.execute(
                "INSERT INTO algo_candidates
                 (as_of, market, source, strategy_id, policy_id, symbol, candidate_date,
                  report_bucket, signal_direction, signal_confidence, execution_mode, details_json)
                 VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?)",
                params![
                    &as_of_s,
                    &item.market,
                    "daily_alpha_bulletin",
                    &item.policy_id,
                    &item.policy_id,
                    &item.symbol,
                    &as_of_s,
                    item.report_bucket.as_deref(),
                    item.signal_direction.as_deref(),
                    item.signal_confidence.as_deref(),
                    item.execution_mode.as_deref(),
                    serde_json::to_string(item)?,
                ],
            )?;
        }
    }

    for trade in evaluated_trade_rows {
        con.execute(
            "INSERT INTO candidate_outcomes
             (as_of, market, policy_id, symbol, report_date, evaluation_date, horizon_days,
              return_pct, label, data_ready, source_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                as_of_s,
                &trade.market,
                &trade.policy_id,
                &trade.symbol,
                trade.report_date.as_deref(),
                trade.evaluation_date.as_deref(),
                parse_policy_horizon(&trade.policy_id),
                trade.return_pct,
                trade.label.as_deref(),
                trade.return_pct.is_some(),
                serde_json::to_string(trade)?,
            ],
        )?;
    }

    for trade in selected_trade_rows {
        con.execute(
            "INSERT INTO selected_trades
             (as_of, market, policy_id, report_date, evaluation_date, symbol, return_pct,
              label, fill_quality, source_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                as_of_s,
                &trade.market,
                &trade.policy_id,
                trade.report_date.as_deref(),
                trade.evaluation_date.as_deref(),
                &trade.symbol,
                trade.return_pct,
                trade.label.as_deref(),
                trade
                    .no_fill_reason
                    .clone()
                    .unwrap_or_else(|| if is_fill(trade) {
                        "filled"
                    } else {
                        "not_filled"
                    }
                    .to_string()),
                serde_json::to_string(trade)?,
            ],
        )?;
    }

    for item in bulletin
        .execution_alpha
        .iter()
        .chain(bulletin.tactical_alpha.iter())
        .chain(bulletin.options_alpha.iter())
        .chain(bulletin.recall_alpha.iter())
        .chain(bulletin.blocked_alpha.iter())
    {
        let blockers_json = serde_json::to_string(&item.blockers)?;
        let payload_json = serde_json::to_string(item)?;
        con.execute(
            "INSERT INTO alpha_bulletin
             (as_of, market, section, symbol, policy_id, reason, blockers_json, payload_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                as_of_s,
                item.market,
                item.section,
                item.symbol,
                item.policy_id,
                item.reason,
                blockers_json,
                payload_json,
            ],
        )?;
        con.execute(
            "INSERT INTO daily_alpha_bulletin
             (as_of, market, section, symbol, policy_id, reason, blockers_json, payload_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                as_of_s,
                item.market,
                item.section,
                item.symbol,
                item.policy_id,
                item.reason,
                blockers_json,
                payload_json,
            ],
        )?;
        con.execute(
            "INSERT INTO execution_gate_results
             (as_of, market, symbol, policy_id, gate_status, section, blockers_json, reason, payload_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                as_of_s,
                item.market,
                item.symbol,
                item.policy_id,
                if item.section == "execution_alpha" { "pass" } else { "blocked" },
                item.section,
                serde_json::to_string(&item.blockers)?,
                item.reason,
                serde_json::to_string(item)?,
            ],
        )?;
    }

    write_report_models_to_db(&con, as_of, bulletin, session)?;
    con.execute_batch("CHECKPOINT")?;
    Ok(())
}

fn parse_policy_horizon(policy_id: &str) -> Option<i64> {
    policy_id
        .rsplit_once(":h")
        .and_then(|(_, h)| h.parse::<i64>().ok())
}

fn write_report_models_to_db(
    con: &Connection,
    as_of: NaiveDate,
    bulletin: &AlphaBulletin,
    session: &str,
) -> Result<()> {
    for market in ["us", "cn"] {
        let model = crate::report_model::build_report_model(bulletin, market, session);
        con.execute(
            "INSERT INTO daily_report_model (as_of, market, session, model_json)
             VALUES (?, ?, ?, ?)",
            params![
                as_of.to_string(),
                market,
                session,
                serde_json::to_string_pretty(&model)?,
            ],
        )?;
    }
    Ok(())
}

pub(super) fn write_bulletin_files(output_dir: &Path, bulletin: &AlphaBulletin) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::write(
        output_dir.join("alpha_bulletin.json"),
        serde_json::to_string_pretty(bulletin)?,
    )?;
    for market in ["us", "cn"] {
        fs::write(
            output_dir.join(format!("alpha_bulletin_{market}.md")),
            render_market_bulletin_md(bulletin, market),
        )?;
        let model = crate::report_model::build_report_model(bulletin, market, "post");
        fs::write(
            output_dir.join(format!("report_model_{market}_post.json")),
            serde_json::to_string_pretty(&model)?,
        )?;
    }
    Ok(())
}

pub(super) fn write_project_bulletin_copies(
    config: &AlphaEvalConfig,
    bulletin: &AlphaBulletin,
) -> Result<()> {
    let stack_root = std::env::current_dir().context("current dir")?;
    let copies = [
        (
            "us",
            stack_root
                .join("quant-research-v1")
                .join("reports/review_dashboard/strategy_backtest")
                .join(&bulletin.as_of),
        ),
        (
            "cn",
            stack_root
                .join("quant-research-cn")
                .join("reports/review_dashboard/strategy_backtest")
                .join(&bulletin.as_of),
        ),
    ];
    if !config.write_project_copies {
        return Ok(());
    }
    for (market, path) in copies {
        fs::create_dir_all(&path)?;
        fs::write(
            path.join(format!("alpha_bulletin_{market}.md")),
            render_market_bulletin_md(bulletin, market),
        )?;
    }
    Ok(())
}

pub(super) fn strategy_report_md(
    bulletin: &AlphaBulletin,
    candidates_by_market: &BTreeMap<String, Vec<PolicyCandidate>>,
) -> String {
    let mut lines = vec![
        format!("# Strategy Backtest Gate - {}", bulletin.as_of),
        String::new(),
        "| Market | Evaluated through | Selected policy | Eligible / Total |".to_string(),
        "|---|---|---|---:|".to_string(),
    ];
    for market in ["us", "cn"] {
        let candidates = candidates_by_market
            .get(market)
            .cloned()
            .unwrap_or_default();
        let eligible = candidates.iter().filter(|c| c.eligible).count();
        let selected = bulletin
            .selected_policies
            .get(market)
            .and_then(|v| v.as_deref())
            .unwrap_or("none");
        lines.push(format!(
            "| {} | {} | `{}` | {} / {} |",
            market.to_uppercase(),
            bulletin
                .evaluated_through
                .get(market)
                .map(String::as_str)
                .unwrap_or("-"),
            selected,
            eligible,
            candidates.len()
        ));
    }
    lines.push(String::new());
    for market in ["us", "cn"] {
        lines.push(format!("## {} Candidate Policies", market.to_uppercase()));
        lines.push(String::new());
        let mut candidates = candidates_by_market
            .get(market)
            .cloned()
            .unwrap_or_default();
        candidates.sort_by(|a, b| {
            b.selected
                .cmp(&a.selected)
                .then(b.eligible.cmp(&a.eligible))
                .then(
                    b.stability_score
                        .partial_cmp(&a.stability_score)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
        });
        if candidates.is_empty() {
            lines.push("No evaluated policies found.".to_string());
            lines.push(String::new());
            continue;
        }
        lines.push("| Selected | Eligible | Policy | Fills | Active buckets | Avg % | Median % | Win | Max DD % | Top1 | Score | Fails |".to_string());
        lines.push("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|".to_string());
        for c in candidates.into_iter().take(30) {
            lines.push(format!(
                "| {} | {} | `{}` | {} | {} | {:?} | {:?} | {:?} | {:?} | {:?} | {} | {} |",
                if c.selected { "yes" } else { "" },
                if c.eligible { "yes" } else { "no" },
                c.policy_id,
                c.fills,
                c.active_buckets,
                c.avg_trade_pct,
                c.median_trade_pct,
                c.strict_win_rate,
                c.max_drawdown_pct,
                c.top1_winner_contribution,
                c.stability_score,
                c.fail_reasons.join(", ")
            ));
        }
        lines.push(String::new());
    }
    format!("{}\n", lines.join("\n").trim_end())
}
