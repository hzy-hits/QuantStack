use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "setup_alpha";

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    crate::analytics::price_features::ensure(db, as_of)?;
    let date_str = as_of.to_string();
    let sql = "
        WITH aux AS (
            SELECT
                ts_code,
                MAX(CASE WHEN module = 'flow' AND metric = 'information_score' THEN value END) AS information_score,
                MAX(CASE WHEN module = 'shadow_fast' AND metric = 'downside_stress' THEN value END) AS downside_stress
            FROM analytics
            WHERE as_of = CAST(? AS DATE)
              AND (
                   (module = 'flow' AND metric = 'information_score')
                OR (module = 'shadow_fast' AND metric = 'downside_stress')
              )
            GROUP BY ts_code
        )
        SELECT
            p.ts_code,
            p.close_now,
            p.high_20d,
            p.low_20d,
            p.avg_vol_5,
            p.avg_vol_base,
            p.std5_ret,
            p.std20_ret,
            COALESCE(p.ret_5d, 0) AS ret_5d,
            COALESCE(aux.information_score, 0) AS information_score,
            COALESCE(aux.downside_stress, 0) AS downside_stress
        FROM price_features p
        LEFT JOIN aux ON p.ts_code = aux.ts_code
        WHERE p.as_of = CAST(? AS DATE)
          AND p.n_obs >= 20
          AND p.close_now IS NOT NULL
    ";

    let mut stmt = db.prepare(sql)?;
    let rows: Vec<_> = stmt
        .query_map(duckdb::params![date_str, date_str], |row| {
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

    db.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS setup_alpha_stage (
            ts_code VARCHAR,
            as_of VARCHAR,
            module VARCHAR,
            metric VARCHAR,
            value DOUBLE,
            detail VARCHAR
        );
        DELETE FROM setup_alpha_stage;",
    )?;
    let mut count = 0usize;

    {
        let mut appender = db.appender("setup_alpha_stage")?;
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
                appender.append_row(duckdb::params![
                    &ts_code, &date_str, MODULE, metric, value, &detail
                ])?;
                count += 1;
            }
        }
    }

    db.execute(
        "DELETE FROM analytics WHERE as_of = CAST(? AS DATE) AND module = ?",
        duckdb::params![date_str.clone(), MODULE],
    )?;
    db.execute_batch(
        "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
         SELECT ts_code, CAST(as_of AS DATE), module, metric, value, detail
         FROM setup_alpha_stage",
    )?;

    info!(rows = count, "setup_alpha complete");
    Ok(count)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
