/// Breakout detection — identifies trend inception from consolidation.
///
/// Signals:
///   - Bollinger squeeze: bandwidth compression ratio (low = coiled, ready to break)
///   - Volume surge: today's volume vs 20D average (>2x = breakout confirmation)
///   - Range breakout: close above N-day high or below N-day low
///   - Volatility expansion: GK realized vol expanding after compression
///
/// Output: breakout_score ∈ [0, 1]
///   High score = volatility compressed + volume surging + price breaking range
///   This captures the START of a new trend, not the middle.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::info;

/// Compute breakout metrics for all stocks with sufficient history.
pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();

    let sql = "
        WITH bars AS (
            SELECT ts_code, trade_date, open, high, low, close, vol, pct_chg,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        ),
        agg AS (
            SELECT ts_code,
                -- Current price & volume
                MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
                MAX(CASE WHEN rn = 1 THEN vol END) AS vol_now,
                MAX(CASE WHEN rn = 1 THEN high END) AS high_now,
                MAX(CASE WHEN rn = 1 THEN low END) AS low_now,

                -- 20D stats (use pct_chg for volatility, not close levels)
                AVG(CASE WHEN rn <= 20 THEN close END) AS sma20,
                STDDEV_POP(CASE WHEN rn <= 20 THEN pct_chg END) AS std20_ret,
                AVG(CASE WHEN rn BETWEEN 2 AND 21 THEN vol END) AS avg_vol_20,

                -- 5D return volatility (recent)
                STDDEV_POP(CASE WHEN rn <= 5 THEN pct_chg END) AS std5_ret,
                AVG(CASE WHEN rn <= 5 THEN close END) AS sma5,

                -- N-day high/low for range breakout
                MAX(CASE WHEN rn BETWEEN 2 AND 21 THEN high END) AS high_20d,
                MIN(CASE WHEN rn BETWEEN 2 AND 21 THEN low END) AS low_20d,
                MAX(CASE WHEN rn BETWEEN 2 AND 11 THEN high END) AS high_10d,
                MIN(CASE WHEN rn BETWEEN 2 AND 11 THEN low END) AS low_10d,

                -- 60D return volatility for squeeze reference
                AVG(CASE WHEN rn <= 60 THEN close END) AS sma60,
                STDDEV_POP(CASE WHEN rn <= 60 THEN pct_chg END) AS std60_ret,

                COUNT(CASE WHEN rn <= 20 THEN 1 END) AS n20,
                COUNT(CASE WHEN rn <= 60 THEN 1 END) AS n60
            FROM bars
            WHERE rn <= 65
            GROUP BY ts_code
            HAVING n20 >= 15 AND close_now IS NOT NULL AND avg_vol_20 > 0
        )
        SELECT ts_code, close_now, vol_now, high_now, low_now,
               sma20, std20_ret, avg_vol_20,
               std5_ret, sma5,
               high_20d, low_20d, high_10d, low_10d,
               sma60, std60_ret, n60
        FROM agg
        WHERE std20_ret > 0
    ";

    let mut stmt = db.prepare(sql)?;
    let rows: Vec<_> = stmt
        .query_map(duckdb::params![date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,   // ts_code
                row.get::<_, f64>(1)?,      // close_now
                row.get::<_, f64>(2)?,      // vol_now
                row.get::<_, f64>(3)?,      // high_now
                row.get::<_, f64>(4)?,      // low_now
                row.get::<_, f64>(5)?,      // sma20
                row.get::<_, f64>(6)?,      // std20
                row.get::<_, f64>(7)?,      // avg_vol_20
                row.get::<_, f64>(8).unwrap_or(0.0),  // std5
                row.get::<_, f64>(9).unwrap_or(0.0),  // sma5
                row.get::<_, f64>(10).unwrap_or(0.0), // high_20d
                row.get::<_, f64>(11).unwrap_or(0.0), // low_20d
                row.get::<_, f64>(12).unwrap_or(0.0), // high_10d
                row.get::<_, f64>(13).unwrap_or(0.0), // low_10d
                row.get::<_, f64>(14).unwrap_or(0.0), // sma60
                row.get::<_, f64>(15).unwrap_or(0.0), // std60
                row.get::<_, i64>(16).unwrap_or(0),   // n60
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let insert_sql = "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail) VALUES (?, ?, 'breakout', ?, ?, ?)";
    let mut insert_stmt = db.prepare(insert_sql)?;
    let mut count = 0usize;

    for (ts_code, close, vol, high, _low, sma20, std20_ret, avg_vol_20,
         std5_ret, _sma5, high_20d, low_20d, high_10d, low_10d,
         _sma60, std60_ret, n60) in &rows
    {
        // ── 1. Squeeze: return volatility compression (not close-level std)
        // squeeze_ratio = recent 20D return vol / historical 60D return vol
        let squeeze_ratio = if *n60 >= 40 && std60_ret.abs() > 1e-10 {
            std20_ret / std60_ret
        } else {
            1.0 // no reference = no squeeze
        };
        // Squeeze score: lower ratio = more compressed = higher score
        // squeeze_ratio < 0.5 → very compressed (score ~1.0)
        // squeeze_ratio > 1.0 → already expanded (score ~0.0)
        let squeeze_score = (1.0 - squeeze_ratio).clamp(0.0, 1.0);

        // ── 2. Volume surge: today vs 20D average
        let vol_ratio = if *avg_vol_20 > 0.0 { vol / avg_vol_20 } else { 1.0 };
        // vol_ratio > 2.0 → strong surge (score 1.0)
        // vol_ratio = 1.0 → normal (score 0.0)
        let volume_score = ((vol_ratio - 1.0) / 1.5).clamp(0.0, 1.0);

        // ── 3. Range breakout: close vs recent high/low (symmetric: both use close)
        let break_high_20 = if *high_20d > 0.0 { *close > *high_20d } else { false };
        let break_low_20 = if *low_20d > 0.0 { *close < *low_20d } else { false };
        let break_high_10 = if *high_10d > 0.0 { *close > *high_10d } else { false };
        let break_low_10 = if *low_10d > 0.0 { *close < *low_10d } else { false };

        let range_score = if break_high_20 || break_low_20 {
            1.0
        } else if break_high_10 || break_low_10 {
            0.6
        } else {
            0.0
        };

        // ── 4. Volatility expansion: 5D return vol expanding vs 20D return vol
        let vol_expansion = if *std20_ret > 1e-10 {
            let ratio_5_20 = std5_ret / std20_ret;
            // ratio > 1.5 = expanding, ratio < 0.7 = still compressed
            ((ratio_5_20 - 0.7) / 0.8).clamp(0.0, 1.0)
        } else {
            0.0
        };

        // ── Composite breakout score
        // Key insight: the BEST breakout = compressed THEN expanding + volume + range break
        // squeeze alone is not enough (could stay compressed)
        // We want: high squeeze + high volume + range break
        let breakout_score = (0.30 * squeeze_score
            + 0.25 * volume_score
            + 0.25 * range_score
            + 0.20 * vol_expansion)
            .min(1.0);

        // Direction
        let breakout_direction = if break_high_20 || break_high_10 {
            "bullish_breakout"
        } else if break_low_20 || break_low_10 {
            "bearish_breakout"
        } else if squeeze_score > 0.5 {
            "coiled" // compressed but no direction yet
        } else {
            "none"
        };

        let detail = serde_json::json!({
            "squeeze_ratio": round3(squeeze_ratio),
            "squeeze_score": round3(squeeze_score),
            "vol_ratio": round2(vol_ratio),
            "volume_score": round3(volume_score),
            "break_high_20d": break_high_20,
            "break_low_20d": break_low_20,
            "break_high_10d": break_high_10,
            "break_low_10d": break_low_10,
            "range_score": round3(range_score),
            "vol_expansion": round3(vol_expansion),
            "std20_ret_pct": round3(*std20_ret),
            "direction": breakout_direction,
        });
        let detail_str = detail.to_string();

        insert_stmt.execute(duckdb::params![ts_code, date_str, "breakout_score", breakout_score, detail_str])?;
        insert_stmt.execute(duckdb::params![ts_code, date_str, "breakout_direction",
            if breakout_direction == "bullish_breakout" { 1.0 }
            else if breakout_direction == "bearish_breakout" { -1.0 }
            else { 0.0 },
            detail_str])?;

        count += 1;
    }

    info!(rows = count, "breakout complete");
    Ok(count)
}

fn round3(v: f64) -> f64 { (v * 1000.0).round() / 1000.0 }
fn round2(v: f64) -> f64 { (v * 100.0).round() / 100.0 }
