use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "continuation_vs_fade";

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let history_start = (as_of - Duration::days(45)).to_string();
    let sql = "
        WITH ranked AS (
            SELECT ts_code, trade_date, close,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
              AND trade_date >= CAST(? AS DATE)
        ),
        latest AS (
            SELECT
                ts_code,
                MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d_ago,
                COUNT(*) AS n_obs
            FROM ranked
            WHERE rn <= 6
            GROUP BY ts_code
            HAVING n_obs >= 6 AND close_now IS NOT NULL
        )
        SELECT
            l.ts_code,
            CASE WHEN l.close_5d_ago > 0 THEN (l.close_now / l.close_5d_ago - 1.0) * 100.0 ELSE 0 END AS ret_5d,
            COALESCE(b.value, 0) AS breakout_score,
            COALESCE(bd.value, 0) AS breakout_direction,
            COALESCE(f.value, 0) AS information_score,
            COALESCE(sa.value, 0) AS setup_score,
            COALESCE(sd.value, 0) AS setup_direction,
            COALESCE(mr.value, 0.5) AS bb_position,
            COALESCE(si.value, 0) AS shadow_iv_30d,
            COALESCE(ds.value, 0) AS downside_stress
        FROM latest l
        LEFT JOIN analytics b
          ON l.ts_code = b.ts_code AND b.as_of = ? AND b.module = 'breakout' AND b.metric = 'breakout_score'
        LEFT JOIN analytics bd
          ON l.ts_code = bd.ts_code AND bd.as_of = ? AND bd.module = 'breakout' AND bd.metric = 'breakout_direction'
        LEFT JOIN analytics f
          ON l.ts_code = f.ts_code AND f.as_of = ? AND f.module = 'flow' AND f.metric = 'information_score'
        LEFT JOIN analytics sa
          ON l.ts_code = sa.ts_code AND sa.as_of = ? AND sa.module = 'setup_alpha' AND sa.metric = 'setup_score'
        LEFT JOIN analytics sd
          ON l.ts_code = sd.ts_code AND sd.as_of = ? AND sd.module = 'setup_alpha' AND sd.metric = 'setup_direction'
        LEFT JOIN analytics mr
          ON l.ts_code = mr.ts_code AND mr.as_of = ? AND mr.module = 'mean_reversion' AND mr.metric = 'bb_position'
        LEFT JOIN analytics si
          ON l.ts_code = si.ts_code AND si.as_of = ? AND si.module = 'shadow_fast' AND si.metric = 'shadow_iv_30d'
        LEFT JOIN analytics ds
          ON l.ts_code = ds.ts_code AND ds.as_of = ? AND ds.module = 'shadow_fast' AND ds.metric = 'downside_stress'
    ";

    let mut stmt = db.prepare(sql)?;
    let rows: Vec<_> = stmt
        .query_map(
            duckdb::params![
                date_str,
                history_start,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
            ],
            |row| {
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
            },
        )?
        .filter_map(|r| r.ok())
        .collect();

    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;
    let mut count = 0usize;

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

    info!(rows = count, "continuation_vs_fade complete");
    Ok(count)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
