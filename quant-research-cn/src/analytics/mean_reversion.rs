use crate::analytics::rv::{infer_censor_side, CensorSide};
/// Mean-reversion signal — identifies oversold/overbought positions for regime-adaptive scoring.
///
/// Signals:
///   - MA distance (20D, 60D): how far price is from moving average
///   - RSI-14: standard momentum oscillator (< 30 oversold, > 70 overbought)
///   - Bollinger Band position: where price sits within 2σ bands
///
/// Output: reversion_score ∈ [0, 1] and reversion_direction (bullish_reversion / bearish_reversion)
///   High reversion_score = strong mean-reversion signal (price at extreme, likely to revert)
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use std::collections::HashMap;
use tracing::info;

/// Compute mean-reversion metrics for all stocks with sufficient price history.
pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();

    // RSI14 and 20D Bollinger only need a short recent window. SMA60 is kept as a
    // contextual average when available, but it must not suppress RSI coverage in
    // the report DB where many active candidates only carry a recent snapshot.
    let sql = "
        WITH bars AS (
            SELECT ts_code, trade_date, close,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        ),
        stock_bars AS (
            SELECT ts_code, rn, close
            FROM bars WHERE rn <= 65
        ),
        agg AS (
            SELECT ts_code,
                   MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
                   AVG(CASE WHEN rn <= 20 THEN close END) AS sma20,
                   AVG(CASE WHEN rn <= 60 THEN close END) AS sma60,
                   STDDEV_POP(CASE WHEN rn <= 20 THEN close END) AS std20,
                   COUNT(CASE WHEN rn <= 60 THEN 1 END) AS n_bars
            FROM stock_bars
            GROUP BY ts_code
            HAVING n_bars >= 20
        )
        SELECT ts_code, close_now, sma20, sma60, std20, n_bars
        FROM agg
        WHERE close_now IS NOT NULL AND sma20 IS NOT NULL AND std20 > 0
    ";

    let mut stmt = db.prepare(sql)?;
    let rows: Vec<(String, f64, f64, f64, f64, i64)> = stmt
        .query_map(duckdb::params![date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3).unwrap_or(0.0),
                row.get::<_, f64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Load RSI data. For A-shares, use pct_chg + Wilder smoothing as the
    // report-facing RSI; the legacy raw-close simple RSI is kept for audit.
    let rsi_map = compute_rsi_batch(db, &date_str, 14)?;

    db.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS mean_reversion_stage (
            ts_code VARCHAR,
            as_of VARCHAR,
            module VARCHAR,
            metric VARCHAR,
            value DOUBLE,
            detail VARCHAR
        );
        DELETE FROM mean_reversion_stage;",
    )?;

    // Cross-sectional z-scores for MA distances
    let ma20_dists: Vec<f64> = rows
        .iter()
        .map(|(_, close, sma20, _, _, _)| (close - sma20) / sma20)
        .collect();
    let (ma20_mean, ma20_std) = cross_stats(&ma20_dists);

    let mut symbol_count = 0usize;
    let mut row_count = 0usize;

    {
        let mut appender = db.appender("mean_reversion_stage")?;

        for (ts_code, close, sma20, sma60, std20, n_bars) in &rows {
            // MA distance (z-scored cross-sectionally)
            let ma20_pct = (close - sma20) / sma20;
            let ma20_z = if ma20_std > 1e-10 {
                ((ma20_pct - ma20_mean) / ma20_std).clamp(-3.0, 3.0)
            } else {
                0.0
            };

            let ma60_pct = if sma60.abs() > 1e-10 {
                (close - sma60) / sma60
            } else {
                0.0
            };

            // Bollinger Band position: (close - lower) / (upper - lower)
            let bb_upper = sma20 + 2.0 * std20;
            let bb_lower = sma20 - 2.0 * std20;
            let bb_width = bb_upper - bb_lower;
            let bb_position = if bb_width > 1e-10 {
                ((close - bb_lower) / bb_width).clamp(0.0, 1.0)
            } else {
                0.5
            };

            // RSI
            let rsi_stats = rsi_map.get(ts_code.as_str()).copied().unwrap_or_default();
            let rsi = rsi_stats.exec;

            // Signed reversion score: DIRECTION matters, not just extremeness.
            // Positive = oversold (expect up), Negative = overbought (expect down)
            // Range: [-1, +1], where magnitude = conviction strength
            //
            // Previous bug: unsigned score mixed oversold+overbought -> IC was negative
            // because "extreme stocks continue their direction" (momentum at tails).
            // Fix: score the REVERSAL direction, so IC should be positive.
            let rsi_signal = (50.0 - rsi) / 50.0; // +1 when RSI=0 (oversold), -1 when RSI=100
            let bb_signal = 0.5 - bb_position; // +0.5 at lower band, -0.5 at upper band
            let ma_signal = -ma20_z / 3.0; // positive when below MA (oversold)

            // Weighted signed combination: [-1, +1]
            let reversion_score =
                (0.35 * rsi_signal + 0.35 * bb_signal * 2.0 + 0.30 * ma_signal).clamp(-1.0, 1.0);

            // Direction from sign
            let reversion_direction = if reversion_score > 0.2 {
                "bullish_reversion" // oversold -> expect up
            } else if reversion_score < -0.2 {
                "bearish_reversion" // overbought -> expect down
            } else {
                "neutral"
            };

            let detail = serde_json::json!({
                "ma20_pct": round3(ma20_pct * 100.0),
                "ma60_pct": round3(ma60_pct * 100.0),
                "ma20_z": round3(ma20_z),
                "rsi_14": round1(rsi),
                "rsi_14_wilder": round1(rsi_stats.wilder_pct),
                "rsi_14_simple_pct": round1(rsi_stats.simple_pct),
                "rsi_14_raw_simple": round1(rsi_stats.raw_simple),
                "rsi_limit_censor_count_14": rsi_stats.limit_censor_count as i64,
                "rsi_latest_pct_chg": round3(rsi_stats.latest_pct_chg),
                "rsi_method": "wilder_pct_chg_with_limit_censor_audit",
                "bb_position": round3(bb_position),
                "bb_width_pct": round3(bb_width / sma20 * 100.0),
                "direction": reversion_direction,
                "n_bars": n_bars,
            });
            let detail_str = detail.to_string();

            for (metric, value) in [
                ("reversion_score", reversion_score),
                ("rsi_14", rsi),
                ("rsi_14_wilder", rsi_stats.wilder_pct),
                ("rsi_14_simple_pct", rsi_stats.simple_pct),
                ("rsi_14_raw_simple", rsi_stats.raw_simple),
                (
                    "rsi_limit_censor_count_14",
                    rsi_stats.limit_censor_count as f64,
                ),
                ("bb_position", bb_position),
                ("ma20_z", ma20_z),
                (
                    "reversion_direction",
                    if reversion_direction == "bullish_reversion" {
                        1.0
                    } else if reversion_direction == "bearish_reversion" {
                        -1.0
                    } else {
                        0.0
                    },
                ),
            ] {
                appender.append_row(duckdb::params![
                    ts_code,
                    &date_str,
                    "mean_reversion",
                    metric,
                    value,
                    &detail_str
                ])?;
                row_count += 1;
            }

            symbol_count += 1;
        }
    }

    db.execute(
        "DELETE FROM analytics WHERE as_of = CAST(? AS DATE) AND module = 'mean_reversion'",
        duckdb::params![date_str],
    )?;
    db.execute_batch(
        "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
         SELECT ts_code, CAST(as_of AS DATE), module, metric, value, detail
         FROM mean_reversion_stage",
    )?;

    info!(
        symbols = symbol_count,
        rows = row_count,
        "mean_reversion complete"
    );
    Ok(row_count)
}

#[derive(Debug, Clone, Copy)]
struct RsiStats {
    raw_simple: f64,
    simple_pct: f64,
    wilder_pct: f64,
    exec: f64,
    limit_censor_count: usize,
    latest_pct_chg: f64,
}

impl Default for RsiStats {
    fn default() -> Self {
        Self {
            raw_simple: 50.0,
            simple_pct: 50.0,
            wilder_pct: 50.0,
            exec: 50.0,
            limit_censor_count: 0,
            latest_pct_chg: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
struct RsiBar {
    ts_code: String,
    name: String,
    close: f64,
    prev_close: Option<f64>,
    high: f64,
    low: f64,
    pct_chg: Option<f64>,
}

/// Compute RSI-14 for all stocks as a batch.
///
/// A-share daily RSI is intentionally based on exchange pct_chg with Wilder
/// smoothing. The previous close-difference/simple-average RSI is retained as
/// `raw_simple` only, because raw price units and one-bar jumps made execution
/// reports look much hotter than the actual smoothed momentum state.
fn compute_rsi_batch(
    db: &Connection,
    date_str: &str,
    period: usize,
) -> Result<HashMap<String, RsiStats>> {
    let lookback = period + 65; // enough history for Wilder smoothing
    let sql = format!(
        "WITH bars AS (
            SELECT p.ts_code,
                   p.trade_date,
                   p.close,
                   p.high,
                   p.low,
                   p.pct_chg,
                   COALESCE(sb.name, '') AS name,
                   LAG(p.close) OVER (PARTITION BY p.ts_code ORDER BY p.trade_date) AS prev_close,
                   ROW_NUMBER() OVER (PARTITION BY p.ts_code ORDER BY p.trade_date DESC) AS rn
            FROM prices p
            LEFT JOIN stock_basic sb ON sb.ts_code = p.ts_code
            WHERE p.trade_date <= CAST(? AS DATE)
        )
        SELECT ts_code, name, close, prev_close, high, low, pct_chg
        FROM bars
        WHERE rn <= {lookback} AND (pct_chg IS NOT NULL OR prev_close IS NOT NULL)
        ORDER BY ts_code, trade_date ASC"
    );

    let mut stmt = db.prepare(&sql)?;
    let rows: Vec<RsiBar> = stmt
        .query_map(duckdb::params![date_str], |row| {
            Ok(RsiBar {
                ts_code: row.get::<_, String>(0)?,
                name: row.get::<_, String>(1).unwrap_or_default(),
                close: row.get::<_, f64>(2).unwrap_or(0.0),
                prev_close: row.get::<_, Option<f64>>(3).unwrap_or(None),
                high: row.get::<_, f64>(4).unwrap_or(0.0),
                low: row.get::<_, f64>(5).unwrap_or(0.0),
                pct_chg: row.get::<_, Option<f64>>(6).unwrap_or(None),
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Group by ts_code and compute RSI
    let mut grouped: HashMap<String, Vec<RsiBar>> = HashMap::new();
    for row in rows {
        grouped.entry(row.ts_code.clone()).or_default().push(row);
    }

    let mut result = HashMap::new();
    for (ts_code, bars) in &grouped {
        if bars.len() < period {
            continue;
        }
        let pct_returns: Vec<f64> = bars.iter().filter_map(|bar| return_pct(bar)).collect();
        let raw_changes: Vec<f64> = bars
            .iter()
            .filter_map(|bar| bar.prev_close.map(|prev| bar.close - prev))
            .collect();
        if pct_returns.len() < period || raw_changes.len() < period {
            continue;
        }

        let recent_bars = bars.iter().rev().take(period);
        let limit_censor_count = recent_bars
            .filter(|bar| {
                matches!(
                    infer_censor_side(
                        &bar.ts_code,
                        &bar.name,
                        return_pct(bar).unwrap_or(0.0),
                        bar.high,
                        bar.low,
                        bar.close,
                    ),
                    CensorSide::Left | CensorSide::Right
                )
            })
            .count();

        let raw_simple = simple_rsi(&raw_changes[raw_changes.len() - period..]);
        let simple_pct = simple_rsi(&pct_returns[pct_returns.len() - period..]);
        let wilder_pct = wilder_rsi(&pct_returns, period);
        let latest_pct_chg = pct_returns.last().copied().unwrap_or(0.0);

        result.insert(
            ts_code.clone(),
            RsiStats {
                raw_simple,
                simple_pct,
                wilder_pct,
                exec: wilder_pct,
                limit_censor_count,
                latest_pct_chg,
            },
        );
    }

    Ok(result)
}

fn return_pct(bar: &RsiBar) -> Option<f64> {
    if let Some(pct) = bar.pct_chg {
        if pct.is_finite() {
            return Some(pct);
        }
    }
    let prev = bar.prev_close?;
    if prev.abs() < 1e-10 || !bar.close.is_finite() {
        return None;
    }
    Some((bar.close / prev - 1.0) * 100.0)
}

fn simple_rsi(changes: &[f64]) -> f64 {
    if changes.is_empty() {
        return 50.0;
    }
    let period = changes.len() as f64;
    let avg_gain = changes.iter().filter(|&&d| d > 0.0).sum::<f64>() / period;
    let avg_loss = changes
        .iter()
        .filter(|&&d| d < 0.0)
        .map(|d| d.abs())
        .sum::<f64>()
        / period;
    rsi_from_avg(avg_gain, avg_loss)
}

fn wilder_rsi(changes: &[f64], period: usize) -> f64 {
    if changes.len() < period || period == 0 {
        return 50.0;
    }
    let mut avg_gain = changes[..period].iter().filter(|&&d| d > 0.0).sum::<f64>() / period as f64;
    let mut avg_loss = changes[..period]
        .iter()
        .filter(|&&d| d < 0.0)
        .map(|d| d.abs())
        .sum::<f64>()
        / period as f64;
    for change in &changes[period..] {
        let gain = if *change > 0.0 { *change } else { 0.0 };
        let loss = if *change < 0.0 { change.abs() } else { 0.0 };
        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;
    }
    rsi_from_avg(avg_gain, avg_loss)
}

fn rsi_from_avg(avg_gain: f64, avg_loss: f64) -> f64 {
    if avg_loss < 1e-10 {
        100.0
    } else {
        let rs = avg_gain / avg_loss;
        (100.0 - 100.0 / (1.0 + rs)).clamp(0.0, 100.0)
    }
}

fn cross_stats(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 1.0);
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    (mean, variance.sqrt().max(1e-10))
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wilder_rsi_dampens_one_day_spike_vs_simple_window() {
        let mut returns = vec![
            -1.2, -1.0, -0.8, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0.1, -0.2, 0.1, -0.1, 0.2, 4.12,
            0.56, 0.89, -1.77, 0.9, 0.45, -0.56, -0.56, 1.01, -1.0, 0.23, -0.45, -0.45, 5.22,
        ];
        let simple = simple_rsi(&returns[returns.len() - 14..]);
        let wilder = wilder_rsi(&returns, 14);
        assert!(simple > 70.0);
        assert!(wilder < simple - 5.0);

        returns.push(-1.0);
        let updated = wilder_rsi(&returns, 14);
        assert!(updated < wilder);
    }

    #[test]
    fn simple_rsi_handles_no_losses_as_hot() {
        let returns = vec![0.1; 14];
        assert_eq!(simple_rsi(&returns), 100.0);
    }
}
