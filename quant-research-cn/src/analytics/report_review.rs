use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use tracing::info;

use crate::config::Settings;
use crate::filtering::notable::{build_review_decisions, NotableItem, ReviewDecision};

const SESSION: &str = "daily";
const LOOKBACK_DAYS: i64 = 45;

struct DecisionRow {
    report_date: NaiveDate,
    symbol: String,
    selection_status: String,
    signal_direction: String,
    execution_mode: String,
    max_chase_gap_pct: Option<f64>,
    reference_close: Option<f64>,
}

struct FutureBar {
    trade_date: NaiveDate,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
}

struct OutcomeRow {
    report_date: NaiveDate,
    symbol: String,
    selection_status: String,
    evaluation_date: NaiveDate,
    signal_direction: String,
    execution_mode: String,
    next_open_ret_pct: Option<f64>,
    next_close_ret_pct: Option<f64>,
    best_up_2d_pct: Option<f64>,
    best_down_2d_pct: Option<f64>,
    gap_vs_chase_limit: Option<f64>,
}

pub fn materialize_report_review(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
    notable: &[NotableItem],
) -> Result<usize> {
    let selected_symbols: Vec<String> = notable.iter().map(|item| item.ts_code.clone()).collect();
    let decisions = build_review_decisions(db, cfg, as_of, &selected_symbols)?;
    if decisions.is_empty() {
        return Ok(0);
    }

    let stored = store_report_decisions(db, as_of, &decisions)?;
    let outcomes = compute_report_outcomes(db, as_of)?;
    let postmortem = compute_alpha_postmortem(db, as_of)?;
    let algorithm_postmortem =
        crate::analytics::algorithm_postmortem::materialize_algorithm_postmortem(db, as_of)?;
    info!(
        %as_of,
        stored,
        outcomes,
        postmortem,
        algorithm_postmortem,
        "report review refreshed"
    );
    Ok(stored)
}

fn store_report_decisions(
    db: &Connection,
    as_of: NaiveDate,
    decisions: &[ReviewDecision],
) -> Result<usize> {
    let date_str = as_of.to_string();
    db.execute(
        "DELETE FROM report_decisions WHERE report_date = CAST(? AS DATE) AND session = ?",
        duckdb::params![date_str.clone(), SESSION],
    )?;
    let mut stmt = db.prepare(
        "INSERT OR REPLACE INTO report_decisions (
            report_date, session, symbol, selection_status, rank_order,
            report_bucket, signal_direction, signal_confidence, composite_score,
            execution_mode, execution_score, max_chase_gap_pct,
            pullback_trigger_pct, setup_score, continuation_score,
            fade_risk, reference_close, details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    for row in decisions {
        stmt.execute(duckdb::params![
            date_str,
            SESSION,
            row.symbol,
            row.selection_status,
            row.rank_order,
            row.report_bucket,
            row.signal_direction,
            row.signal_confidence,
            row.composite_score,
            row.execution_mode,
            row.execution_score,
            nullable_f64(row.max_chase_gap_pct),
            nullable_f64(row.pullback_trigger_pct),
            nullable_f64(row.setup_score),
            nullable_f64(row.continuation_score),
            nullable_f64(row.fade_risk),
            row.reference_close,
            row.details_json,
        ])?;
    }

    Ok(decisions.len())
}

fn compute_report_outcomes(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let cutoff = as_of - Duration::days(LOOKBACK_DAYS);
    db.execute(
        "DELETE FROM report_outcomes WHERE evaluation_date = CAST(? AS DATE) AND session = ?",
        duckdb::params![as_of.to_string(), SESSION],
    )?;
    let mut stmt = db.prepare(
        "SELECT CAST(report_date AS VARCHAR), symbol, selection_status, signal_direction, execution_mode,
                max_chase_gap_pct, reference_close
         FROM report_decisions
         WHERE session = ?
           AND report_date >= CAST(? AS DATE)
           AND report_date < CAST(? AS DATE)
         ORDER BY report_date, rank_order",
    )?;

    let rows: Vec<DecisionRow> = stmt
        .query_map(
            duckdb::params![SESSION, cutoff.to_string(), as_of.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<f64>>(5)?,
                    row.get::<_, Option<f64>>(6)?,
                ))
            },
        )?
        .filter_map(|r| match r {
            Ok((
                report_date,
                symbol,
                selection_status,
                signal_direction,
                execution_mode,
                max_chase_gap_pct,
                reference_close,
            )) => parse_sql_date(report_date)
                .ok()
                .map(|parsed_date| DecisionRow {
                    report_date: parsed_date,
                    symbol,
                    selection_status,
                    signal_direction,
                    execution_mode,
                    max_chase_gap_pct,
                    reference_close,
                }),
            Err(_) => None,
        })
        .collect();

    let mut inserted = 0usize;
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO report_outcomes (
            report_date, session, symbol, selection_status, evaluation_date,
            next_trade_date, second_trade_date, reference_close, next_open, next_close,
            best_high_2d, worst_low_2d, next_open_ret_pct, next_close_ret_pct,
            best_up_2d_pct, best_down_2d_pct, gap_vs_chase_limit, data_ready
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    for row in rows {
        let Some(reference_close) = row.reference_close.filter(|v| *v > 0.0) else {
            continue;
        };
        let future = load_future_bars(db, &row.symbol, row.report_date)?;
        if future.is_empty() {
            continue;
        }

        let first = &future[0];
        let second = future.get(1);
        let best_high = future
            .iter()
            .filter_map(|bar| bar.high)
            .fold(None, |acc: Option<f64>, v| {
                Some(acc.map_or(v, |m| m.max(v)))
            });
        let worst_low = future
            .iter()
            .filter_map(|bar| bar.low)
            .fold(None, |acc: Option<f64>, v| {
                Some(acc.map_or(v, |m| m.min(v)))
            });

        let next_open_ret_pct = pct_change(first.open, reference_close);
        let next_close_ret_pct = pct_change(first.close, reference_close);
        let best_up_2d_pct = pct_change(best_high, reference_close);
        let best_down_2d_pct = pct_change(worst_low, reference_close);
        let gap_vs_chase_limit = match (next_open_ret_pct, row.max_chase_gap_pct) {
            (Some(gap), Some(limit)) if limit > 0.0 => Some(gap / limit),
            _ => None,
        };

        insert_stmt.execute(duckdb::params![
            row.report_date.to_string(),
            SESSION,
            row.symbol,
            row.selection_status,
            as_of.to_string(),
            first.trade_date.to_string(),
            second.map(|bar| bar.trade_date.to_string()),
            reference_close,
            first.open,
            first.close,
            best_high,
            worst_low,
            next_open_ret_pct,
            next_close_ret_pct,
            best_up_2d_pct,
            best_down_2d_pct,
            gap_vs_chase_limit,
            true,
        ])?;
        inserted += 1;
    }

    Ok(inserted)
}

fn compute_alpha_postmortem(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let cutoff = as_of - Duration::days(LOOKBACK_DAYS);
    db.execute(
        "DELETE FROM alpha_postmortem WHERE evaluation_date = CAST(? AS DATE) AND session = ?",
        duckdb::params![as_of.to_string(), SESSION],
    )?;
    let mut stmt = db.prepare(
        "SELECT
            CAST(d.report_date AS VARCHAR),
            d.symbol,
            d.selection_status,
            CAST(o.evaluation_date AS VARCHAR),
            d.signal_direction,
            d.execution_mode,
            o.next_open_ret_pct,
            o.next_close_ret_pct,
            o.best_up_2d_pct,
            o.best_down_2d_pct,
            o.gap_vs_chase_limit
         FROM report_decisions d
         INNER JOIN report_outcomes o
           ON o.report_date = d.report_date
          AND o.session = d.session
          AND o.symbol = d.symbol
          AND o.selection_status = d.selection_status
         WHERE d.session = ?
           AND d.report_date >= CAST(? AS DATE)
           AND o.data_ready = TRUE",
    )?;

    let rows: Vec<OutcomeRow> = stmt
        .query_map(duckdb::params![SESSION, cutoff.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<f64>>(6)?,
                row.get::<_, Option<f64>>(7)?,
                row.get::<_, Option<f64>>(8)?,
                row.get::<_, Option<f64>>(9)?,
                row.get::<_, Option<f64>>(10)?,
            ))
        })?
        .filter_map(|r| match r {
            Ok((
                report_date,
                symbol,
                selection_status,
                evaluation_date,
                signal_direction,
                execution_mode,
                next_open_ret_pct,
                next_close_ret_pct,
                best_up_2d_pct,
                best_down_2d_pct,
                gap_vs_chase_limit,
            )) => {
                let parsed_report = parse_sql_date(report_date).ok()?;
                let parsed_eval = parse_sql_date(evaluation_date).ok()?;
                Some(OutcomeRow {
                    report_date: parsed_report,
                    symbol,
                    selection_status,
                    evaluation_date: parsed_eval,
                    signal_direction,
                    execution_mode,
                    next_open_ret_pct,
                    next_close_ret_pct,
                    best_up_2d_pct,
                    best_down_2d_pct,
                    gap_vs_chase_limit,
                })
            }
            Err(_) => None,
        })
        .collect();

    let mut inserted = 0usize;
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO alpha_postmortem (
            report_date, session, symbol, selection_status, evaluation_date,
            label, review_note, factor_feedback_action, factor_feedback_weight,
            best_ret_pct, next_open_ret_pct, next_close_ret_pct, gap_vs_chase_limit
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    for row in rows {
        let directional_best = directional_best_ret(
            &row.signal_direction,
            row.best_up_2d_pct,
            row.best_down_2d_pct,
        );
        let directional_close =
            directional_close_ret(&row.signal_direction, row.next_close_ret_pct);
        let (label, review_note, action, weight) =
            classify_postmortem(&row, directional_best, directional_close);

        insert_stmt.execute(duckdb::params![
            row.report_date.to_string(),
            SESSION,
            row.symbol,
            row.selection_status,
            row.evaluation_date.to_string(),
            label,
            review_note,
            action,
            weight,
            directional_best,
            row.next_open_ret_pct,
            row.next_close_ret_pct,
            row.gap_vs_chase_limit,
        ])?;
        inserted += 1;
    }

    Ok(inserted)
}

fn load_future_bars(
    db: &Connection,
    symbol: &str,
    report_date: NaiveDate,
) -> Result<Vec<FutureBar>> {
    let mut stmt = db.prepare(
        "SELECT CAST(trade_date AS VARCHAR), open, high, low, close
         FROM prices
         WHERE ts_code = ?
           AND trade_date > CAST(? AS DATE)
         ORDER BY trade_date
         LIMIT 2",
    )?;

    let rows = stmt.query_map(duckdb::params![symbol, report_date.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<f64>>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, Option<f64>>(4)?,
        ))
    })?;

    Ok(rows
        .filter_map(|r| match r {
            Ok((trade_date, open, high, low, close)) => {
                parse_sql_date(trade_date)
                    .ok()
                    .map(|parsed_date| FutureBar {
                        trade_date: parsed_date,
                        open,
                        high,
                        low,
                        close,
                    })
            }
            Err(_) => None,
        })
        .collect())
}

fn pct_change(target: Option<f64>, base: f64) -> Option<f64> {
    target
        .filter(|v| *v > 0.0 && base > 0.0)
        .map(|v| ((v / base) - 1.0) * 100.0)
}

fn directional_best_ret(
    direction: &str,
    best_up_2d_pct: Option<f64>,
    best_down_2d_pct: Option<f64>,
) -> Option<f64> {
    match direction {
        "bearish" => best_down_2d_pct.map(|v| -v),
        _ => best_up_2d_pct,
    }
}

fn directional_close_ret(direction: &str, next_close_ret_pct: Option<f64>) -> Option<f64> {
    match direction {
        "bearish" => next_close_ret_pct.map(|v| -v),
        _ => next_close_ret_pct,
    }
}

fn classify_postmortem(
    row: &OutcomeRow,
    directional_best: Option<f64>,
    directional_close: Option<f64>,
) -> (String, String, Option<String>, Option<f64>) {
    let best = directional_best.unwrap_or(0.0);
    let close = directional_close.unwrap_or(0.0);
    let gap_ratio = row.gap_vs_chase_limit.unwrap_or(0.0);
    let gap_invalid = row.signal_direction == "bullish" && gap_ratio >= 1.0;

    if row.selection_status == "ignored" {
        if best >= 2.5 {
            return (
                "missed_alpha".to_string(),
                format!("未入选标的后续仍沿原方向走出 {:.1}%", best),
                Some("boost_recall".to_string()),
                Some(clamp_weight(best / 4.0)),
            );
        }
        return (
            "ignored_ok".to_string(),
            "未入选标的后续优势有限".to_string(),
            None,
            None,
        );
    }

    if gap_invalid {
        return (
            "alpha_already_paid".to_string(),
            format!("次日开盘已超过追价上限，gap_vs_limit={:.2}x", gap_ratio),
            Some("penalize_stale_chase".to_string()),
            Some(clamp_weight(gap_ratio)),
        );
    }

    if row.execution_mode != "executable" && best >= 2.0 {
        return (
            "good_signal_bad_timing".to_string(),
            format!("方向没错，但执行模式={}，可做空间不足", row.execution_mode),
            Some("penalize_stale_chase".to_string()),
            Some(clamp_weight(best / 4.0)),
        );
    }

    if best >= 2.0 && close >= -1.0 {
        return (
            "captured".to_string(),
            format!("入选信号后续沿原方向延续 {:.1}%", best),
            Some("reward_capture".to_string()),
            Some(clamp_weight(best / 5.0)),
        );
    }

    if close <= -1.5 || best < 0.5 {
        return (
            "false_positive".to_string(),
            "入选信号后续没有形成可执行优势".to_string(),
            Some("penalize_false_positive".to_string()),
            Some(clamp_weight((-close).max(1.0) / 3.0)),
        );
    }

    (
        "flat_edge".to_string(),
        "方向与执行都没有形成足够清晰的优势".to_string(),
        None,
        None,
    )
}

fn clamp_weight(raw: f64) -> f64 {
    raw.clamp(0.5, 1.5)
}

fn nullable_f64(v: f64) -> Option<f64> {
    if v.abs() < 1e-12 {
        None
    } else {
        Some(v)
    }
}

fn parse_sql_date(raw: String) -> Result<NaiveDate> {
    let trimmed = raw.trim();
    let date_part = trimmed.get(0..10).unwrap_or(trimmed);
    Ok(NaiveDate::parse_from_str(date_part, "%Y-%m-%d")?)
}
