use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "open_execution_gate";

fn regime_sensitive_thresholds(
    regime: i32,
    setup_score: f64,
    continuation_score: f64,
) -> (f64, f64, f64) {
    let (ret_penalty_base, wait_pullback_base, do_not_chase_base) = match regime {
        0 => (12.0, 8.0, 14.0), // trending: allow continuation names more room
        2 => (9.5, 6.5, 11.5),  // noisy: moderate relief
        1 => (8.0, 5.5, 9.5),   // mean-reverting: stay tighter
        _ => (8.5, 6.0, 10.5),
    };

    let structure_relief = (1.8 * (continuation_score - 0.52).max(0.0)
        + 1.2 * (setup_score - 0.50).max(0.0))
    .clamp(0.0, 2.5);

    let ret_penalty_denom =
        (ret_penalty_base + 2.5 * structure_relief).clamp(ret_penalty_base, 16.0);
    let wait_pullback_threshold =
        (wait_pullback_base + 1.5 * structure_relief).clamp(wait_pullback_base, 11.5);
    let do_not_chase_threshold =
        (do_not_chase_base + 2.5 * structure_relief).clamp(do_not_chase_base, 17.5);

    (
        ret_penalty_denom,
        wait_pullback_threshold,
        do_not_chase_threshold,
    )
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let history_start = (as_of - Duration::days(90)).to_string();
    let sql = "
        WITH ranked AS (
            SELECT ts_code, trade_date, close, high, low,
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
                MAX(CASE WHEN rn = 21 THEN close END) AS close_20d_ago,
                AVG(CASE WHEN rn <= 14 AND close > 0 THEN (high - low) / close * 100.0 END) AS atr_pct_14
            FROM ranked
            WHERE rn <= 25
            GROUP BY ts_code
        )
        SELECT
            l.ts_code,
            l.close_now,
            CASE WHEN l.close_5d_ago > 0 THEN (l.close_now / l.close_5d_ago - 1.0) * 100.0 ELSE 0 END AS ret_5d,
            CASE WHEN l.close_20d_ago > 0 THEN (l.close_now / l.close_20d_ago - 1.0) * 100.0 ELSE 0 END AS ret_20d,
            COALESCE(l.atr_pct_14, 0) AS atr_pct_14,
            COALESCE(sa.value, 0) AS setup_score,
            COALESCE(cf.value, 0) AS continuation_score,
            COALESCE(fr.value, 0) AS fade_risk,
            COALESCE(si.value, 0) AS shadow_iv_30d,
            COALESCE(ds.value, 0) AS downside_stress,
            CAST(COALESCE(rg.value, 2) AS INTEGER) AS regime
        FROM latest l
        LEFT JOIN analytics sa
          ON l.ts_code = sa.ts_code AND sa.as_of = ? AND sa.module = 'setup_alpha' AND sa.metric = 'setup_score'
        LEFT JOIN analytics cf
          ON l.ts_code = cf.ts_code AND cf.as_of = ? AND cf.module = 'continuation_vs_fade' AND cf.metric = 'continuation_score'
        LEFT JOIN analytics fr
          ON l.ts_code = fr.ts_code AND fr.as_of = ? AND fr.module = 'continuation_vs_fade' AND fr.metric = 'fade_risk'
        LEFT JOIN analytics si
          ON l.ts_code = si.ts_code AND si.as_of = ? AND si.module = 'shadow_fast' AND si.metric = 'shadow_iv_30d'
        LEFT JOIN analytics ds
          ON l.ts_code = ds.ts_code AND ds.as_of = ? AND ds.module = 'shadow_fast' AND ds.metric = 'downside_stress'
        LEFT JOIN analytics rg
          ON l.ts_code = rg.ts_code AND rg.as_of = ? AND rg.module = 'momentum' AND rg.metric = 'regime'
        WHERE l.close_now IS NOT NULL
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
                date_str
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
                    row.get::<_, f64>(7).unwrap_or(0.0),
                    row.get::<_, f64>(8).unwrap_or(0.0),
                    row.get::<_, f64>(9).unwrap_or(0.0),
                    row.get::<_, i32>(10).unwrap_or(2),
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
        close_now,
        ret_5d,
        ret_20d,
        atr_pct_14,
        setup_score,
        continuation_score,
        fade_risk,
        shadow_iv_30d,
        downside_stress,
        regime,
    ) in rows
    {
        let (ret_penalty_denom, wait_pullback_threshold, do_not_chase_threshold) =
            regime_sensitive_thresholds(regime, setup_score, continuation_score);
        let max_chase_gap_pct = (0.45 * atr_pct_14
            + 1.10 * continuation_score
            + 0.70 * (1.0 - downside_stress.clamp(0.0, 1.0)))
        .clamp(0.8, 4.5);
        let pullback_trigger_pct = (0.45 * max_chase_gap_pct).clamp(0.4, 2.5);
        let execution_score = (0.45 * continuation_score
            + 0.25 * setup_score
            + 0.15 * (1.0 - downside_stress.clamp(0.0, 1.0))
            + 0.15 * (1.0 - (ret_5d.abs() / ret_penalty_denom).clamp(0.0, 1.0)))
        .clamp(0.0, 1.0);

        let execution_mode = if fade_risk > 0.65
            || ret_5d.abs() > do_not_chase_threshold
            || (shadow_iv_30d > 28.0 && downside_stress > 0.60)
        {
            "do_not_chase"
        } else if execution_score < 0.48 || ret_5d.abs() > wait_pullback_threshold {
            "wait_pullback"
        } else {
            "executable"
        };

        let pullback_price = if close_now > 0.0 {
            close_now * (1.0 - pullback_trigger_pct / 100.0)
        } else {
            0.0
        };

        let detail = json!({
            "execution_mode": execution_mode,
            "ret_5d": round2(ret_5d),
            "ret_20d": round2(ret_20d),
            "atr_pct_14": round2(atr_pct_14),
            "setup_score": round3(setup_score),
            "continuation_score": round3(continuation_score),
            "fade_risk": round3(fade_risk),
            "shadow_iv_30d": round2(shadow_iv_30d),
            "downside_stress": round3(downside_stress),
            "pullback_price": round2(pullback_price),
            "ret_penalty_denom": round2(ret_penalty_denom),
            "wait_pullback_threshold": round2(wait_pullback_threshold),
            "do_not_chase_threshold": round2(do_not_chase_threshold),
            "regime": regime,
        })
        .to_string();

        for (metric, value) in [
            ("execution_score", execution_score),
            ("max_chase_gap_pct", max_chase_gap_pct),
            ("pullback_trigger_pct", pullback_trigger_pct),
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

    info!(rows = count, "open_execution_gate complete");
    Ok(count)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::regime_sensitive_thresholds;

    #[test]
    fn trending_regime_allows_more_extension_than_mean_reversion() {
        let (_ret_denom_trend, wait_trend, stop_trend) = regime_sensitive_thresholds(0, 0.72, 0.74);
        let (_ret_denom_mr, wait_mr, stop_mr) = regime_sensitive_thresholds(1, 0.72, 0.74);

        assert!(wait_trend > wait_mr);
        assert!(stop_trend > stop_mr);
    }

    #[test]
    fn stronger_setup_and_continuation_raise_thresholds() {
        let (ret_denom_low, wait_low, stop_low) = regime_sensitive_thresholds(0, 0.40, 0.45);
        let (ret_denom_high, wait_high, stop_high) = regime_sensitive_thresholds(0, 0.75, 0.78);

        assert!(ret_denom_high > ret_denom_low);
        assert!(wait_high > wait_low);
        assert!(stop_high > stop_low);
    }
}
