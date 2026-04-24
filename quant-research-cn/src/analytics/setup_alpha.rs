use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "setup_alpha";

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let sql = "
        WITH ranked AS (
            SELECT ts_code, trade_date, close, high, low, vol, pct_chg,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        ),
        agg AS (
            SELECT
                ts_code,
                MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
                MAX(CASE WHEN rn BETWEEN 2 AND 21 THEN high END) AS high_20d,
                MIN(CASE WHEN rn BETWEEN 2 AND 21 THEN low END) AS low_20d,
                AVG(CASE WHEN rn <= 5 THEN vol END) AS avg_vol_5,
                AVG(CASE WHEN rn BETWEEN 6 AND 20 THEN vol END) AS avg_vol_base,
                STDDEV_POP(CASE WHEN rn <= 5 THEN pct_chg END) AS std5_ret,
                STDDEV_POP(CASE WHEN rn <= 20 THEN pct_chg END) AS std20_ret,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d_ago,
                COUNT(CASE WHEN rn <= 20 THEN 1 END) AS n20
            FROM ranked
            WHERE rn <= 25
            GROUP BY ts_code
            HAVING n20 >= 15 AND close_now IS NOT NULL
        )
        SELECT
            a.ts_code,
            a.close_now,
            a.high_20d,
            a.low_20d,
            a.avg_vol_5,
            a.avg_vol_base,
            a.std5_ret,
            a.std20_ret,
            CASE WHEN a.close_5d_ago > 0 THEN (a.close_now / a.close_5d_ago - 1.0) * 100.0 ELSE 0 END AS ret_5d,
            COALESCE(f.value, 0) AS information_score,
            COALESCE(s.value, 0) AS downside_stress
        FROM agg a
        LEFT JOIN analytics f
          ON a.ts_code = f.ts_code
         AND f.as_of = ?
         AND f.module = 'flow'
         AND f.metric = 'information_score'
        LEFT JOIN analytics s
          ON a.ts_code = s.ts_code
         AND s.as_of = ?
         AND s.module = 'shadow_fast'
         AND s.metric = 'downside_stress'
    ";

    let mut stmt = db.prepare(sql)?;
    let rows: Vec<_> = stmt
        .query_map(duckdb::params![date_str, date_str, date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1).unwrap_or(0.0),
                row.get::<_, f64>(2).unwrap_or(0.0),
                row.get::<_, f64>(3).unwrap_or(0.0),
                row.get::<_, f64>(4).unwrap_or(0.0),
                row.get::<_, f64>(5).unwrap_or(0.0),
                row.get::<_, f64>(6).unwrap_or(0.0),
                row.get::<_, f64>(7).unwrap_or(0.0),
                row.get::<_, f64>(8).unwrap_or(0.0),
                row.get::<_, f64>(9).unwrap_or(0.0),
                row.get::<_, f64>(10).unwrap_or(0.0),
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;
    let mut count = 0usize;

    for (
        ts_code,
        close_now,
        high_20d,
        low_20d,
        avg_vol_5,
        avg_vol_base,
        std5_ret,
        std20_ret,
        ret_5d,
        information_score,
        downside_stress,
    ) in rows
    {
        let range = (high_20d - low_20d).max(1e-6);
        let close_location = ((close_now - low_20d) / range).clamp(0.0, 1.0);
        let compression_score = if std20_ret.abs() > 1e-6 {
            (1.0 - std5_ret / std20_ret).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let volume_build = if avg_vol_base > 1e-6 {
            ((avg_vol_5 / avg_vol_base) - 0.9).clamp(0.0, 0.8) / 0.8
        } else {
            0.0
        };

        let bullish_pressure = ((close_location - 0.55) / 0.45).clamp(0.0, 1.0);
        let bearish_pressure = ((0.45 - close_location) / 0.45).clamp(0.0, 1.0);
        let setup_direction = if bullish_pressure > bearish_pressure + 0.10 && ret_5d > 0.0 {
            1.0
        } else if bearish_pressure > bullish_pressure + 0.10 && ret_5d < 0.0 {
            -1.0
        } else {
            0.0
        };
        let directional_pressure = if setup_direction > 0.0 {
            bullish_pressure
        } else if setup_direction < 0.0 {
            bearish_pressure
        } else {
            (close_location - 0.5).abs() * 1.4
        }
        .clamp(0.0, 1.0);
        let shadow_room = (1.0 - downside_stress).clamp(0.0, 1.0);

        let setup_score = (0.30 * compression_score
            + 0.25 * directional_pressure
            + 0.20 * volume_build
            + 0.15 * information_score.clamp(0.0, 1.0)
            + 0.10 * shadow_room)
            .clamp(0.0, 1.0);

        let detail = json!({
            "direction": if setup_direction > 0.0 { "bullish" } else if setup_direction < 0.0 { "bearish" } else { "neutral" },
            "compression_score": round3(compression_score),
            "close_location_20d": round3(close_location),
            "volume_build": round3(volume_build),
            "ret_5d": round2(ret_5d),
            "information_score": round3(information_score),
            "downside_stress": round3(downside_stress),
        })
        .to_string();

        for (metric, value) in [
            ("setup_score", setup_score),
            ("setup_direction", setup_direction),
            ("close_location_20d", close_location),
            ("compression_score", compression_score),
            ("volume_build", volume_build),
        ] {
            insert.execute(duckdb::params![
                ts_code,
                date_str,
                MODULE,
                metric,
                value,
                detail.clone()
            ])?;
        }
        count += 1;
    }

    info!(rows = count, "setup_alpha complete");
    Ok(count)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
