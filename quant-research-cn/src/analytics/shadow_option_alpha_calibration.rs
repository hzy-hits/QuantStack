use std::collections::HashMap;

use anyhow::Result;
use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use serde_json::json;
use tracing::info;

const MODULE: &str = "shadow_option_alpha";
const LOOKBACK_DAYS: i64 = 90;
const PRIOR_N: f64 = 16.0;

#[derive(Debug, Clone, Default)]
struct BucketStats {
    n: usize,
    captured: usize,
    missed_alpha: usize,
    stale_chase: usize,
    false_positive: usize,
    latest_sample_date: Option<String>,
}

#[derive(Debug)]
struct CurrentRow {
    ts_code: String,
    ret_5d: f64,
    shadow_iv_30d: f64,
    downside_stress: f64,
    shadow_put_90_3m: f64,
    shadow_touch_90_3m: f64,
    shadow_skew_90_3m: f64,
    setup_score: f64,
    setup_direction: f64,
    continuation_score: f64,
    continuation_direction: f64,
    fade_risk: f64,
    execution_score: f64,
    max_chase_gap_pct: f64,
    pullback_trigger_pct: f64,
    execution_mode: String,
    pullback_price: Option<f64>,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let history = load_bucket_stats(db, as_of);
    let overall = history.get("_overall").cloned().unwrap_or_default();
    let rows = load_current_rows(db, as_of)?;

    db.execute(
        "DELETE FROM analytics WHERE as_of = CAST(? AS DATE) AND module = ?",
        duckdb::params![date_str.clone(), MODULE],
    )?;
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let mut count = 0usize;
    for row in rows {
        let shadow_rank_score = compute_shadow_rank_score(&row);
        let (bucket, bucket_id) = calibration_bucket(
            shadow_rank_score,
            &row.execution_mode,
            row.setup_score,
            row.continuation_score,
            row.fade_risk,
        );
        let stats = history
            .get(&bucket)
            .filter(|stats| stats.n >= 8)
            .unwrap_or(&overall);
        let stats_source = if history.get(&bucket).map(|s| s.n >= 8).unwrap_or(false) {
            "bucket"
        } else if overall.n > 0 {
            "overall"
        } else {
            "prior"
        };

        let capture_rate = shrunk_rate(stats.captured, stats.n, 0.45);
        let missed_rate = shrunk_rate(stats.missed_alpha, stats.n, 0.20);
        let stale_rate = shrunk_rate(stats.stale_chase, stats.n, 0.25);
        let false_rate = shrunk_rate(stats.false_positive, stats.n, 0.20);

        let shadow_alpha_prob = (0.30 * shadow_rank_score.max(0.0).clamp(0.0, 1.0)
            + 0.22 * row.setup_score.clamp(0.0, 1.0)
            + 0.20 * row.continuation_score.clamp(0.0, 1.0)
            + 0.14 * row.execution_score.clamp(0.0, 1.0)
            + 0.14 * capture_rate
            + 0.06 * missed_rate
            - 0.12 * false_rate)
            .clamp(0.0, 1.0);
        let stale_chase_risk = compute_stale_chase_risk(&row, stale_rate);
        let entry_quality_score = (0.42 * row.execution_score.clamp(0.0, 1.0)
            + 0.24 * shadow_alpha_prob
            + 0.18 * (1.0 - row.fade_risk.clamp(0.0, 1.0))
            + 0.16 * (1.0 - stale_chase_risk))
            .clamp(0.0, 1.0);
        let diagnostic = diagnostic_label(
            &row.execution_mode,
            shadow_alpha_prob,
            stale_chase_risk,
            entry_quality_score,
        );
        let ci = wilson_interval(stats.captured, stats.n);

        let detail = json!({
            "diagnostic": diagnostic,
            "diagnostic_zh": diagnostic_zh(diagnostic),
            "calibration_bucket": bucket,
            "calibration_bucket_id": bucket_id,
            "stats_source": stats_source,
            "sample_count": stats.n,
            "capture_rate": plain_rate(stats.captured, stats.n),
            "capture_rate_interval": ci.map(|(lo, hi)| vec![round4(lo), round4(hi)]),
            "missed_alpha_rate": plain_rate(stats.missed_alpha, stats.n),
            "stale_chase_rate": plain_rate(stats.stale_chase, stats.n),
            "false_positive_rate": plain_rate(stats.false_positive, stats.n),
            "latest_sample_date": stats.latest_sample_date.clone(),
            "shadow_rank_score": round3(shadow_rank_score),
            "shadow_alpha_prob": round3(shadow_alpha_prob),
            "stale_chase_risk": round3(stale_chase_risk),
            "entry_quality_score": round3(entry_quality_score),
            "shadow_iv_30d": round2(row.shadow_iv_30d),
            "downside_stress": round3(row.downside_stress),
            "shadow_put_90_3m": round4(row.shadow_put_90_3m),
            "shadow_touch_90_3m": round4(row.shadow_touch_90_3m),
            "shadow_skew_90_3m": round2(row.shadow_skew_90_3m),
            "setup_score": round3(row.setup_score),
            "continuation_score": round3(row.continuation_score),
            "fade_risk": round3(row.fade_risk),
            "execution_score": round3(row.execution_score),
            "execution_mode": row.execution_mode,
            "max_chase_gap_pct": round2(row.max_chase_gap_pct),
            "pullback_trigger_pct": round2(row.pullback_trigger_pct),
            "pullback_price": row.pullback_price.map(round2),
        })
        .to_string();

        for (metric, value) in [
            ("shadow_alpha_prob", shadow_alpha_prob),
            ("stale_chase_risk", stale_chase_risk),
            ("entry_quality_score", entry_quality_score),
            ("calibration_bucket", bucket_id as f64),
        ] {
            insert.execute(duckdb::params![
                &row.ts_code,
                &date_str,
                MODULE,
                metric,
                value,
                detail.clone()
            ])?;
        }
        count += 1;
    }

    info!(rows = count, "shadow_option_alpha calibration complete");
    Ok(count)
}

fn load_bucket_stats(db: &Connection, as_of: NaiveDate) -> HashMap<String, BucketStats> {
    let cutoff = (as_of - Duration::days(LOOKBACK_DAYS)).to_string();
    let mut out: HashMap<String, BucketStats> = HashMap::new();
    let sql = "
        SELECT
            COALESCE(CAST(p.evaluation_date AS VARCHAR), '') AS evaluation_date,
            COALESCE(p.label, '') AS label,
            COALESCE(CAST(json_extract(d.details_json, '$.shadow_rank_score') AS DOUBLE), 0.0) AS shadow_rank_score,
            COALESCE(d.execution_mode, '') AS execution_mode,
            COALESCE(d.setup_score, 0.0) AS setup_score,
            COALESCE(d.continuation_score, 0.0) AS continuation_score,
            COALESCE(d.fade_risk, 0.0) AS fade_risk
        FROM alpha_postmortem p
        INNER JOIN report_decisions d
          ON d.report_date = p.report_date
         AND d.session = p.session
         AND d.symbol = p.symbol
         AND d.selection_status = p.selection_status
        WHERE p.evaluation_date >= CAST(? AS DATE)
          AND p.evaluation_date < CAST(? AS DATE)";

    let mut stmt = match db.prepare(sql) {
        Ok(stmt) => stmt,
        Err(_) => return out,
    };
    let rows = match stmt.query_map(duckdb::params![cutoff, as_of.to_string()], |row| {
        Ok((
            row.get::<_, String>(0).unwrap_or_default(),
            row.get::<_, String>(1).unwrap_or_default(),
            row.get::<_, f64>(2).unwrap_or(0.0),
            row.get::<_, String>(3).unwrap_or_default(),
            row.get::<_, f64>(4).unwrap_or(0.0),
            row.get::<_, f64>(5).unwrap_or(0.0),
            row.get::<_, f64>(6).unwrap_or(0.0),
        ))
    }) {
        Ok(rows) => rows,
        Err(_) => return out,
    };

    for row in rows.filter_map(|r| r.ok()) {
        let (sample_date, label, shadow_rank_score, execution_mode, setup, cont, fade) = row;
        let group = label_group(&label);
        if group == "unresolved" {
            continue;
        }
        let (bucket, _) = calibration_bucket(shadow_rank_score, &execution_mode, setup, cont, fade);
        add_label(out.entry(bucket).or_default(), group, &sample_date);
        add_label(
            out.entry("_overall".to_string()).or_default(),
            group,
            &sample_date,
        );
    }
    out
}

fn load_current_rows(db: &Connection, as_of: NaiveDate) -> Result<Vec<CurrentRow>> {
    let date_str = as_of.to_string();
    let sql = "
        WITH ranked AS (
            SELECT ts_code, trade_date, close,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        ),
        latest AS (
            SELECT
                ts_code,
                MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d_ago
            FROM ranked
            WHERE rn <= 6
            GROUP BY ts_code
            HAVING close_now IS NOT NULL
        )
        SELECT
            l.ts_code,
            CASE WHEN l.close_5d_ago > 0 THEN (l.close_now / l.close_5d_ago - 1.0) * 100.0 ELSE 0 END AS ret_5d,
            COALESCE(s30.value, 0.0) AS shadow_iv_30d,
            COALESCE(ds.value, 0.0) AS downside_stress,
            COALESCE(sp90.value, 0.0) AS shadow_put_90_3m,
            COALESCE(st90.value, 0.0) AS shadow_touch_90_3m,
            COALESCE(ssk.value, 0.0) AS shadow_skew_90_3m,
            COALESCE(sa.value, 0.0) AS setup_score,
            COALESCE(sd.value, 0.0) AS setup_direction,
            COALESCE(cs.value, 0.0) AS continuation_score,
            COALESCE(cd.value, 0.0) AS continuation_direction,
            COALESCE(fr.value, 0.0) AS fade_risk,
            COALESCE(es.value, 0.0) AS execution_score,
            COALESCE(mcg.value, 0.0) AS max_chase_gap_pct,
            COALESCE(ptp.value, 0.0) AS pullback_trigger_pct,
            es.detail AS execution_detail
        FROM latest l
        LEFT JOIN analytics s30 ON l.ts_code = s30.ts_code AND s30.as_of = ? AND s30.module = 'shadow_fast' AND s30.metric = 'shadow_iv_30d'
        LEFT JOIN analytics ds ON l.ts_code = ds.ts_code AND ds.as_of = ? AND ds.module = 'shadow_fast' AND ds.metric = 'downside_stress'
        LEFT JOIN analytics sp90 ON l.ts_code = sp90.ts_code AND sp90.as_of = ? AND sp90.module = 'shadow_full' AND sp90.metric = 'shadow_put_90_3m'
        LEFT JOIN analytics st90 ON l.ts_code = st90.ts_code AND st90.as_of = ? AND st90.module = 'shadow_full' AND st90.metric = 'shadow_touch_90_3m'
        LEFT JOIN analytics ssk ON l.ts_code = ssk.ts_code AND ssk.as_of = ? AND ssk.module = 'shadow_full' AND ssk.metric = 'shadow_skew_90_3m'
        LEFT JOIN analytics sa ON l.ts_code = sa.ts_code AND sa.as_of = ? AND sa.module = 'setup_alpha' AND sa.metric = 'setup_score'
        LEFT JOIN analytics sd ON l.ts_code = sd.ts_code AND sd.as_of = ? AND sd.module = 'setup_alpha' AND sd.metric = 'setup_direction'
        LEFT JOIN analytics cs ON l.ts_code = cs.ts_code AND cs.as_of = ? AND cs.module = 'continuation_vs_fade' AND cs.metric = 'continuation_score'
        LEFT JOIN analytics cd ON l.ts_code = cd.ts_code AND cd.as_of = ? AND cd.module = 'continuation_vs_fade' AND cd.metric = 'continuation_direction'
        LEFT JOIN analytics fr ON l.ts_code = fr.ts_code AND fr.as_of = ? AND fr.module = 'continuation_vs_fade' AND fr.metric = 'fade_risk'
        LEFT JOIN analytics es ON l.ts_code = es.ts_code AND es.as_of = ? AND es.module = 'open_execution_gate' AND es.metric = 'execution_score'
        LEFT JOIN analytics mcg ON l.ts_code = mcg.ts_code AND mcg.as_of = ? AND mcg.module = 'open_execution_gate' AND mcg.metric = 'max_chase_gap_pct'
        LEFT JOIN analytics ptp ON l.ts_code = ptp.ts_code AND ptp.as_of = ? AND ptp.module = 'open_execution_gate' AND ptp.metric = 'pullback_trigger_pct'
        WHERE l.ts_code NOT LIKE '688%'";

    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map(
        duckdb::params![
            &date_str, &date_str, &date_str, &date_str, &date_str, &date_str, &date_str, &date_str,
            &date_str, &date_str, &date_str, &date_str, &date_str, &date_str,
        ],
        |row| {
            let execution_detail = row.get::<_, Option<String>>(15)?;
            let detail = execution_detail
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
            let execution_mode = detail
                .as_ref()
                .and_then(|obj| obj.get("execution_mode"))
                .and_then(|v| v.as_str())
                .unwrap_or("executable")
                .to_string();
            let pullback_price = detail
                .as_ref()
                .and_then(|obj| obj.get("pullback_price"))
                .and_then(|v| v.as_f64());

            Ok(CurrentRow {
                ts_code: row.get::<_, String>(0)?,
                ret_5d: row.get::<_, f64>(1).unwrap_or(0.0),
                shadow_iv_30d: row.get::<_, f64>(2).unwrap_or(0.0),
                downside_stress: row.get::<_, f64>(3).unwrap_or(0.0),
                shadow_put_90_3m: row.get::<_, f64>(4).unwrap_or(0.0),
                shadow_touch_90_3m: row.get::<_, f64>(5).unwrap_or(0.0),
                shadow_skew_90_3m: row.get::<_, f64>(6).unwrap_or(0.0),
                setup_score: row.get::<_, f64>(7).unwrap_or(0.0),
                setup_direction: row.get::<_, f64>(8).unwrap_or(0.0),
                continuation_score: row.get::<_, f64>(9).unwrap_or(0.0),
                continuation_direction: row.get::<_, f64>(10).unwrap_or(0.0),
                fade_risk: row.get::<_, f64>(11).unwrap_or(0.0),
                execution_score: row.get::<_, f64>(12).unwrap_or(0.0),
                max_chase_gap_pct: row.get::<_, f64>(13).unwrap_or(0.0),
                pullback_trigger_pct: row.get::<_, f64>(14).unwrap_or(0.0),
                execution_mode,
                pullback_price,
            })
        },
    )?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

fn label_group(label: &str) -> &'static str {
    match label {
        "captured" => "captured",
        "missed_alpha" => "missed_alpha",
        "alpha_already_paid" | "good_signal_bad_timing" | "stale_chase" => "stale_chase",
        "false_positive" => "false_positive",
        _ => "unresolved",
    }
}

fn add_label(stats: &mut BucketStats, label: &str, sample_date: &str) {
    stats.n += 1;
    match label {
        "captured" => stats.captured += 1,
        "missed_alpha" => stats.missed_alpha += 1,
        "stale_chase" => stats.stale_chase += 1,
        "false_positive" => stats.false_positive += 1,
        _ => {}
    }
    if !sample_date.is_empty()
        && stats
            .latest_sample_date
            .as_ref()
            .map(|existing| sample_date > existing.as_str())
            .unwrap_or(true)
    {
        stats.latest_sample_date = Some(sample_date.to_string());
    }
}

fn calibration_bucket(
    shadow_rank_score: f64,
    execution_mode: &str,
    setup_score: f64,
    continuation_score: f64,
    fade_risk: f64,
) -> (String, i32) {
    let shadow_band = if shadow_rank_score >= 0.55 {
        ("shadow_high", 3)
    } else if shadow_rank_score >= 0.25 {
        ("shadow_mid", 2)
    } else if shadow_rank_score <= -0.25 {
        ("shadow_bearish", 0)
    } else {
        ("shadow_low", 1)
    };
    let execution_band = match execution_mode {
        "do_not_chase" => ("do_not_chase", 0),
        "wait_pullback" => ("wait_pullback", 1),
        _ => ("executable", 2),
    };
    let structure_band = if fade_risk >= 0.58 {
        ("fade_high", 0)
    } else if setup_score >= 0.55 || continuation_score >= 0.55 {
        ("setup_strong", 2)
    } else {
        ("setup_mixed", 1)
    };
    (
        format!(
            "{}|{}|{}",
            shadow_band.0, execution_band.0, structure_band.0
        ),
        shadow_band.1 * 100 + execution_band.1 * 10 + structure_band.1,
    )
}

fn compute_shadow_rank_score(row: &CurrentRow) -> f64 {
    let has_shadow = row.shadow_iv_30d > 0.0
        || row.shadow_put_90_3m > 0.0
        || row.shadow_touch_90_3m > 0.0
        || row.shadow_skew_90_3m > 0.0;
    if !has_shadow {
        return 0.0;
    }
    let stability = (1.0 - row.downside_stress).clamp(0.0, 1.0);
    let vol_reasonable = (1.0 - (row.shadow_iv_30d / 45.0)).clamp(0.0, 1.0);
    let touch_comfort = if row.shadow_touch_90_3m > 0.0 {
        (1.0 - row.shadow_touch_90_3m).clamp(0.0, 1.0)
    } else {
        stability
    };
    let skew_comfort = if row.shadow_skew_90_3m > 0.0 {
        (1.0 - row.shadow_skew_90_3m / 10.0).clamp(0.0, 1.0)
    } else {
        stability
    };
    let put_cost_comfort = if row.shadow_put_90_3m > 0.0 {
        (1.0 - row.shadow_put_90_3m / 0.25).clamp(0.0, 1.0)
    } else {
        stability
    };
    let alpha_score = (0.30 * stability
        + 0.20 * vol_reasonable
        + 0.20 * touch_comfort
        + 0.15 * skew_comfort
        + 0.15 * put_cost_comfort)
        .clamp(0.0, 1.0);

    let direction = if row.setup_direction.abs() > 0.1 {
        row.setup_direction.signum()
    } else if row.continuation_direction.abs() > 0.1 {
        row.continuation_direction.signum()
    } else if row.ret_5d > 0.0 {
        1.0
    } else if row.ret_5d < 0.0 {
        -1.0
    } else {
        0.0
    };

    if direction > 0.0 {
        alpha_score
    } else if direction < 0.0 {
        -0.60 * alpha_score
    } else {
        0.0
    }
}

fn compute_stale_chase_risk(row: &CurrentRow, stale_rate: f64) -> f64 {
    let mode_risk = match row.execution_mode.as_str() {
        "do_not_chase" => 0.80,
        "wait_pullback" => 0.48,
        _ => 0.20,
    };
    let ret_stretch = (row.ret_5d.abs() / 12.0).clamp(0.0, 1.0);
    (0.34 * mode_risk
        + 0.24 * row.fade_risk.clamp(0.0, 1.0)
        + 0.18 * row.downside_stress.clamp(0.0, 1.0)
        + 0.14 * ret_stretch
        + 0.10 * stale_rate)
        .clamp(0.0, 1.0)
}

fn diagnostic_label(
    execution_mode: &str,
    shadow_alpha_prob: f64,
    stale_chase_risk: f64,
    entry_quality_score: f64,
) -> &'static str {
    if execution_mode == "do_not_chase" || stale_chase_risk >= 0.62 {
        "chase_invalid"
    } else if execution_mode == "wait_pullback"
        || stale_chase_risk >= 0.44
        || entry_quality_score < 0.52
    {
        "pullback_only"
    } else if shadow_alpha_prob >= 0.50 && entry_quality_score >= 0.55 {
        "can_accept"
    } else {
        "pullback_only"
    }
}

fn diagnostic_zh(label: &str) -> &'static str {
    match label {
        "can_accept" => "能承接",
        "pullback_only" => "只等回踩",
        "chase_invalid" => "追价失效",
        _ => "只等回踩",
    }
}

fn shrunk_rate(successes: usize, n: usize, prior: f64) -> f64 {
    if n == 0 {
        prior
    } else {
        (successes as f64 + prior * PRIOR_N) / (n as f64 + PRIOR_N)
    }
}

fn plain_rate(successes: usize, n: usize) -> Option<f64> {
    if n == 0 {
        None
    } else {
        Some(round4(successes as f64 / n as f64))
    }
}

fn wilson_interval(successes: usize, n: usize) -> Option<(f64, f64)> {
    if n == 0 {
        return None;
    }
    let z = 1.96_f64;
    let n_f = n as f64;
    let p = successes as f64 / n_f;
    let denom = 1.0 + z * z / n_f;
    let centre = p + z * z / (2.0 * n_f);
    let margin = z * ((p * (1.0 - p) + z * z / (4.0 * n_f)) / n_f).sqrt();
    Some((
        ((centre - margin) / denom).clamp(0.0, 1.0),
        ((centre + margin) / denom).clamp(0.0, 1.0),
    ))
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

fn round4(v: f64) -> f64 {
    (v * 10000.0).round() / 10000.0
}

#[cfg(test)]
mod tests {
    use super::{calibration_bucket, diagnostic_label};

    #[test]
    fn do_not_chase_bucket_is_chase_invalid() {
        let (bucket, bucket_id) = calibration_bucket(0.62, "do_not_chase", 0.70, 0.68, 0.30);
        assert!(bucket.contains("shadow_high"));
        assert!(bucket.contains("do_not_chase"));
        assert_eq!(bucket_id, 302);
        assert_eq!(
            diagnostic_label("do_not_chase", 0.68, 0.40, 0.61),
            "chase_invalid"
        );
    }

    #[test]
    fn executable_supported_bucket_can_accept() {
        let (bucket, bucket_id) = calibration_bucket(0.58, "executable", 0.62, 0.60, 0.22);
        assert_eq!(bucket, "shadow_high|executable|setup_strong");
        assert_eq!(bucket_id, 322);
        assert_eq!(
            diagnostic_label("executable", 0.57, 0.24, 0.59),
            "can_accept"
        );
    }
}
