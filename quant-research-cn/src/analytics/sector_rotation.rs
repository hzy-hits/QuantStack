/// Sector rotation analytics — computes industry-level momentum, flow, and rotation signals.
///
/// Uses Tushare industry classification (from stock_basic.industry field) to group stocks,
/// then computes per-industry aggregate metrics:
///   - sector_return_5d: average 5-day return of constituent stocks
///   - sector_return_20d: average 20-day return of constituent stocks
///   - sector_flow_z: average information_score of constituent stocks
///   - sector_momentum: z-score of sector return relative to market
///   - rotation_score: composite of momentum + flow + reversal signals
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::info;

const MODULE: &str = "sector_rotation";

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();

    // Compute per-industry metrics using stock_basic.industry + prices + analytics
    let sql = "
        WITH ranked AS (
            SELECT ts_code, trade_date, close, pct_chg,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        ),
        stock_returns AS (
            SELECT p.ts_code,
                   CASE WHEN p5.close > 0 THEN (p.close / p5.close - 1.0) * 100.0 ELSE NULL END AS ret_5d,
                   CASE WHEN p20.close > 0 THEN (p.close / p20.close - 1.0) * 100.0 ELSE NULL END AS ret_20d,
                   p.pct_chg AS ret_1d
            FROM ranked p
            LEFT JOIN ranked p5 ON p.ts_code = p5.ts_code AND p5.rn = 6
            LEFT JOIN ranked p20 ON p.ts_code = p20.ts_code AND p20.rn = 21
            WHERE p.rn = 1
        ),
        sector_stats AS (
            SELECT
                sb.industry,
                COUNT(*) AS n_stocks,
                AVG(sr.ret_5d) AS avg_ret_5d,
                AVG(sr.ret_20d) AS avg_ret_20d,
                AVG(sr.ret_1d) AS avg_ret_1d,
                AVG(a.value) AS avg_flow_score,
                STDDEV_POP(sr.ret_5d) AS std_ret_5d
            FROM stock_basic sb
            JOIN stock_returns sr ON sb.ts_code = sr.ts_code
            LEFT JOIN analytics a ON sb.ts_code = a.ts_code
                AND a.as_of = ? AND a.module = 'flow' AND a.metric = 'information_score'
            WHERE sb.industry IS NOT NULL AND sb.industry != ''
                AND sr.ret_5d IS NOT NULL
            GROUP BY sb.industry
            HAVING COUNT(*) >= 3
        ),
        market_avg AS (
            SELECT AVG(avg_ret_5d) AS mkt_ret, STDDEV_POP(avg_ret_5d) AS mkt_std
            FROM sector_stats
        )
        SELECT
            ss.industry,
            ss.n_stocks,
            ss.avg_ret_5d,
            ss.avg_ret_20d,
            ss.avg_ret_1d,
            COALESCE(ss.avg_flow_score, 0) AS avg_flow_score,
            CASE WHEN ma.mkt_std > 0.01
                 THEN (ss.avg_ret_5d - ma.mkt_ret) / ma.mkt_std
                 ELSE 0 END AS momentum_z
        FROM sector_stats ss, market_avg ma
        ORDER BY momentum_z DESC
    ";

    let mut stmt = db.prepare(sql)?;
    let rows: Vec<(String, i64, f64, f64, f64, f64, f64)> = stmt
        .query_map(duckdb::params![date_str, date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1).unwrap_or(0),
                row.get::<_, f64>(2).unwrap_or(0.0),
                row.get::<_, f64>(3).unwrap_or(0.0),
                row.get::<_, f64>(4).unwrap_or(0.0),
                row.get::<_, f64>(5).unwrap_or(0.0),
                row.get::<_, f64>(6).unwrap_or(0.0),
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        info!("no sector data available (stock_basic.industry empty?)");
        return Ok(0);
    }

    // Write to analytics table
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let mut count = 0usize;
    for (industry, n_stocks, ret_5d, ret_20d, ret_1d, flow, mom_z) in &rows {
        let ts_code = format!("_SECTOR_{}", industry);
        let detail = format!(
            r#"{{"industry":"{}","n_stocks":{},"ret_5d":{:.2},"ret_20d":{:.2},"ret_1d":{:.2},"flow_score":{:.3},"momentum_z":{:.2}}}"#,
            industry, n_stocks, ret_5d, ret_20d, ret_1d, flow, mom_z
        );

        // Sector 5D return
        insert_stmt.execute(duckdb::params![
            &ts_code, date_str, MODULE, "ret_5d", ret_5d, &detail,
        ])?;

        // Sector 20D return
        insert_stmt.execute(duckdb::params![
            &ts_code, date_str, MODULE, "ret_20d", ret_20d, None::<String>,
        ])?;

        // Sector momentum z-score
        insert_stmt.execute(duckdb::params![
            &ts_code, date_str, MODULE, "momentum_z", mom_z, None::<String>,
        ])?;

        // Sector avg flow score
        insert_stmt.execute(duckdb::params![
            &ts_code, date_str, MODULE, "flow_score", flow, None::<String>,
        ])?;

        // Rotation score: 0.5*momentum_z + 0.3*flow + 0.2*reversal_penalty
        // Reversal penalty: sectors with extreme 5D gains have mean-reversion risk
        let reversal_penalty = if *ret_5d > 10.0 { -0.3 } else if *ret_5d < -10.0 { 0.3 } else { 0.0 };
        let rotation_score = (0.5 * mom_z.clamp(-3.0, 3.0) / 3.0
            + 0.3 * flow
            + 0.2 * reversal_penalty)
            .clamp(-1.0, 1.0);

        insert_stmt.execute(duckdb::params![
            &ts_code, date_str, MODULE, "rotation_score", rotation_score, None::<String>,
        ])?;

        count += 1;
    }

    // Also store the top-5 and bottom-5 sectors as a market-level summary
    let n = rows.len();
    let top5: Vec<String> = rows.iter().take(5).map(|r| format!("{}({:.1}%)", r.0, r.2)).collect();
    let bot5: Vec<String> = rows.iter().rev().take(5).map(|r| format!("{}({:.1}%)", r.0, r.2)).collect();

    let summary = format!(
        r#"{{"n_sectors":{},"top5":[{}],"bottom5":[{}]}}"#,
        n,
        top5.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(","),
        bot5.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(","),
    );

    insert_stmt.execute(duckdb::params![
        "_MARKET", date_str, MODULE, "summary", n as f64, &summary,
    ])?;

    info!(sectors = count, "sector rotation computed");
    Ok(count)
}
