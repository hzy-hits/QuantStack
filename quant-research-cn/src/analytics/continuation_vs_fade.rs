use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "continuation_vs_fade";

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    crate::analytics::price_features::ensure(db, as_of)?;
    let date_str = as_of.to_string();
    let sql = "
        WITH aux AS (
            SELECT
                ts_code,
                MAX(CASE WHEN module = 'breakout' AND metric = 'breakout_score' THEN value END) AS breakout_score,
                MAX(CASE WHEN module = 'breakout' AND metric = 'breakout_direction' THEN value END) AS breakout_direction,
                MAX(CASE WHEN module = 'flow' AND metric = 'information_score' THEN value END) AS information_score,
                MAX(CASE WHEN module = 'setup_alpha' AND metric = 'setup_score' THEN value END) AS setup_score,
                MAX(CASE WHEN module = 'setup_alpha' AND metric = 'setup_direction' THEN value END) AS setup_direction,
                MAX(CASE WHEN module = 'mean_reversion' AND metric = 'bb_position' THEN value END) AS bb_position,
                MAX(CASE WHEN module = 'shadow_fast' AND metric = 'shadow_iv_30d' THEN value END) AS shadow_iv_30d,
                MAX(CASE WHEN module = 'shadow_fast' AND metric = 'downside_stress' THEN value END) AS downside_stress
            FROM analytics
            WHERE as_of = CAST(? AS DATE)
              AND (
                   (module = 'breakout' AND metric IN ('breakout_score', 'breakout_direction'))
                OR (module = 'flow' AND metric = 'information_score')
                OR (module = 'setup_alpha' AND metric IN ('setup_score', 'setup_direction'))
                OR (module = 'mean_reversion' AND metric = 'bb_position')
                OR (module = 'shadow_fast' AND metric IN ('shadow_iv_30d', 'downside_stress'))
              )
            GROUP BY ts_code
        )
        SELECT
            p.ts_code,
            COALESCE(p.ret_5d, 0) AS ret_5d,
            COALESCE(aux.breakout_score, 0) AS breakout_score,
            COALESCE(aux.breakout_direction, 0) AS breakout_direction,
            COALESCE(aux.information_score, 0) AS information_score,
            COALESCE(aux.setup_score, 0) AS setup_score,
            COALESCE(aux.setup_direction, 0) AS setup_direction,
            COALESCE(aux.bb_position, 0.5) AS bb_position,
            COALESCE(aux.shadow_iv_30d, 0) AS shadow_iv_30d,
            COALESCE(aux.downside_stress, 0) AS downside_stress
        FROM price_features p
        LEFT JOIN aux ON p.ts_code = aux.ts_code
        WHERE p.as_of = CAST(? AS DATE)
          AND p.n_obs >= 6
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
                row.get::<_, f64>(7).unwrap_or(0.5),
                row.get::<_, f64>(8).unwrap_or(0.0),
                row.get::<_, f64>(9).unwrap_or(0.0),
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    db.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS continuation_vs_fade_stage (
            ts_code VARCHAR,
            as_of VARCHAR,
            module VARCHAR,
            metric VARCHAR,
            value DOUBLE,
            detail VARCHAR
        );
        DELETE FROM continuation_vs_fade_stage;",
    )?;
    let mut count = 0usize;

    {
        let mut appender = db.appender("continuation_vs_fade_stage")?;
        for (
            ts_code,
            ret_5d,
            breakout_score,
            breakout_direction,
            information_score,
            setup_score,
            setup_direction,
            bb_position,
            shadow_iv_30d,
            downside_stress,
        ) in rows
        {
            let trend_pressure = (ret_5d.abs() / 8.0).clamp(0.0, 1.0);
            let continuation_direction = if setup_direction.abs() > 0.1 {
                setup_direction.signum()
            } else if breakout_direction.abs() > 0.1 {
                breakout_direction.signum()
            } else if ret_5d > 0.0 {
                1.0
            } else if ret_5d < 0.0 {
                -1.0
            } else {
                0.0
            };

            let stretch = if continuation_direction > 0.0 {
                ((bb_position - 0.85) / 0.15).clamp(0.0, 1.0)
            } else if continuation_direction < 0.0 {
                ((0.15 - bb_position) / 0.15).clamp(0.0, 1.0)
            } else {
                (bb_position - 0.5).abs() * 1.5
            };
            let vol_risk = (shadow_iv_30d / 35.0).clamp(0.0, 1.0);

            let continuation_score = (0.30 * setup_score
                + 0.25 * breakout_score
                + 0.20 * information_score.clamp(0.0, 1.0)
                + 0.15 * trend_pressure
                + 0.10 * (1.0 - stretch))
                .clamp(0.0, 1.0);
            let fade_risk = (0.40 * stretch
                + 0.25 * downside_stress.clamp(0.0, 1.0)
                + 0.20 * vol_risk
                + 0.15 * ((trend_pressure - 0.65) / 0.35).clamp(0.0, 1.0))
            .clamp(0.0, 1.0);

            let regime = if continuation_score >= 0.58 && fade_risk < 0.50 {
                "continue"
            } else if fade_risk >= 0.58 {
                "fade"
            } else {
                "neutral"
            };

            let detail = json!({
                "direction": if continuation_direction > 0.0 { "bullish" } else if continuation_direction < 0.0 { "bearish" } else { "neutral" },
                "regime": regime,
                "ret_5d": round2(ret_5d),
                "breakout_score": round3(breakout_score),
                "information_score": round3(information_score),
                "setup_score": round3(setup_score),
                "bb_position": round3(bb_position),
                "shadow_iv_30d": round2(shadow_iv_30d),
                "downside_stress": round3(downside_stress),
            })
            .to_string();

            for (metric, value) in [
                ("continuation_score", continuation_score),
                ("fade_risk", fade_risk),
                ("continuation_direction", continuation_direction),
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
         FROM continuation_vs_fade_stage",
    )?;

    info!(rows = count, "continuation_vs_fade complete");
    Ok(count)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
