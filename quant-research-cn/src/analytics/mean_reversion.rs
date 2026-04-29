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

    // Load RSI data (need close prices for gain/loss calculation)
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
            let rsi = rsi_map.get(ts_code.as_str()).copied().unwrap_or(50.0);

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
                "bb_position": round3(bb_position),
                "bb_width_pct": round3(bb_width / sma20 * 100.0),
                "direction": reversion_direction,
                "n_bars": n_bars,
            });
            let detail_str = detail.to_string();

            for (metric, value) in [
                ("reversion_score", reversion_score),
                ("rsi_14", rsi),
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

/// Compute RSI-14 for all stocks as a batch.
fn compute_rsi_batch(
    db: &Connection,
    date_str: &str,
    period: usize,
) -> Result<std::collections::HashMap<String, f64>> {
    let lookback = period + 5; // extra margin
    let sql = format!(
        "WITH bars AS (
            SELECT ts_code, trade_date, close,
                   LAG(close) OVER (PARTITION BY ts_code ORDER BY trade_date) AS prev_close,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        )
        SELECT ts_code, close, prev_close, rn
        FROM bars
        WHERE rn <= {lookback} AND prev_close IS NOT NULL
        ORDER BY ts_code, rn ASC"
    );

    let mut stmt = db.prepare(&sql)?;
    let rows: Vec<(String, f64, f64, i64)> = stmt
        .query_map(duckdb::params![date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Group by ts_code and compute RSI
    let mut grouped: std::collections::HashMap<String, Vec<(f64, f64)>> =
        std::collections::HashMap::new();
    for (ts_code, close, prev_close, _rn) in &rows {
        grouped
            .entry(ts_code.clone())
            .or_default()
            .push((*close, *prev_close));
    }

    let mut result = std::collections::HashMap::new();
    for (ts_code, changes) in &grouped {
        if changes.len() < period {
            continue;
        }
        // Use last `period` changes (already ordered by rn ASC = most recent first → reversed)
        // changes are ordered rn ASC (most recent first), take the most recent `period`
        let recent: Vec<f64> = changes.iter().take(period).map(|(c, p)| c - p).collect();

        let avg_gain = recent.iter().filter(|&&d| d > 0.0).sum::<f64>() / period as f64;
        let avg_loss = recent
            .iter()
            .filter(|&&d| d < 0.0)
            .map(|d| d.abs())
            .sum::<f64>()
            / period as f64;

        let rsi = if avg_loss < 1e-10 {
            100.0
        } else {
            let rs = avg_gain / avg_loss;
            100.0 - 100.0 / (1.0 + rs)
        };

        result.insert(ts_code.clone(), rsi);
    }

    Ok(result)
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
