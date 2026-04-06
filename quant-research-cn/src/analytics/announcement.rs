/// Announcement risk — A-share equivalent of US earnings_risk.
///
/// 业绩预告 types → surprise categories → Beta-Binomial posterior
///
/// P(5D excess return > 0 | forecast_type) via Beta-Binomial
///
/// Type mapping:
///   预增/扭亏     → strong positive
///   略增/续盈     → mild positive
///   略减/续亏     → mild negative
///   预减/首亏     → strong negative
///   不确定        → neutral (skip)
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::info;

use super::bayes::BetaBinomial;

const MODULE: &str = "announcement";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SurpriseCategory {
    StrongPositive, // 0
    MildPositive,   // 1
    Neutral,        // 2
    MildNegative,   // 3
    StrongNegative, // 4
}

impl SurpriseCategory {
    fn as_i32(self) -> i32 {
        match self {
            Self::StrongPositive => 0,
            Self::MildPositive => 1,
            Self::Neutral => 2,
            Self::MildNegative => 3,
            Self::StrongNegative => 4,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::StrongPositive => "strong_positive",
            Self::MildPositive => "mild_positive",
            Self::Neutral => "neutral",
            Self::MildNegative => "mild_negative",
            Self::StrongNegative => "strong_negative",
        }
    }
}

pub fn classify_forecast_type(forecast_type: &str) -> SurpriseCategory {
    match forecast_type {
        "预增" | "扭亏" => SurpriseCategory::StrongPositive,
        "略增" | "续盈" => SurpriseCategory::MildPositive,
        "略减" | "续亏" => SurpriseCategory::MildNegative,
        "预减" | "首亏" => SurpriseCategory::StrongNegative,
        _ => SurpriseCategory::Neutral,
    }
}

/// Estimate win/loss from p_change range.
///
/// If both min and max are positive → win (positive direction).
/// If both negative → loss. Mixed or missing → uncertain (skip).
fn estimate_outcome(p_change_min: Option<f64>, p_change_max: Option<f64>) -> Option<bool> {
    match (p_change_min, p_change_max) {
        (Some(lo), Some(hi)) => {
            if lo > 0.0 && hi > 0.0 {
                Some(true) // both positive → win
            } else if lo < 0.0 && hi < 0.0 {
                Some(false) // both negative → loss
            } else {
                // Mixed sign → uncertain, use midpoint
                let mid = (lo + hi) / 2.0;
                if mid.abs() < 1e-10 {
                    None // truly ambiguous
                } else {
                    Some(mid > 0.0)
                }
            }
        }
        (Some(v), None) | (None, Some(v)) => Some(v > 0.0),
        (None, None) => None,
    }
}

struct ForecastRow {
    ts_code: String,
    _ann_date: String,
    forecast_type: String,
    p_change_min: Option<f64>,
    p_change_max: Option<f64>,
}

pub fn compute(db: &Connection, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let lookback = (as_of - chrono::Duration::days(30)).to_string();

    // ── Step 1: Query recent forecasts (last 30 days) ──────────────
    let mut stmt = db.prepare(
        "SELECT ts_code, CAST(ann_date AS VARCHAR) AS ann_date, forecast_type, p_change_min, p_change_max
         FROM forecast
         WHERE ann_date >= ? AND ann_date <= ?
         ORDER BY ann_date DESC",
    )?;

    let forecasts: Vec<ForecastRow> = stmt
        .query_map([&lookback, &date_str], |row| {
            Ok(ForecastRow {
                ts_code: row.get::<_, String>(0)?,
                _ann_date: row.get::<_, String>(1)?,
                forecast_type: row.get::<_, String>(2)?,
                p_change_min: row.get::<_, Option<f64>>(3)?,
                p_change_max: row.get::<_, Option<f64>>(4)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    if forecasts.is_empty() {
        info!("no recent forecasts found, skipping announcement_risk");
        return Ok(0);
    }

    // ── Step 2: Historical calibration per category ────────────────
    // Query all forecasts from last 2 years to build per-category priors.
    // Use p_change direction as a proxy for 5D outcome when actual price
    // history is insufficient.
    let hist_start = (as_of - chrono::Duration::days(730)).to_string();
    let mut hist_stmt = db.prepare(
        "SELECT forecast_type, p_change_min, p_change_max
         FROM forecast
         WHERE ann_date >= ? AND ann_date <= ?",
    )?;

    // Accumulate wins/losses per category
    let mut cat_wins = [0usize; 5]; // indexed by SurpriseCategory as_i32
    let mut cat_losses = [0usize; 5];

    let hist_rows: Vec<(String, Option<f64>, Option<f64>)> = hist_stmt
        .query_map([&hist_start, &date_str], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    for (ft, pmin, pmax) in &hist_rows {
        let cat = classify_forecast_type(ft);
        if cat == SurpriseCategory::Neutral {
            continue;
        }
        let idx = cat.as_i32() as usize;
        match estimate_outcome(*pmin, *pmax) {
            Some(true) => cat_wins[idx] += 1,
            Some(false) => cat_losses[idx] += 1,
            None => {} // ambiguous, skip
        }
    }

    info!(
        hist_rows = hist_rows.len(),
        "announcement historical calibration loaded"
    );

    // ── Step 3: Compute posteriors and write analytics ─────────────
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let mut count = 0usize;
    for f in &forecasts {
        let cat = classify_forecast_type(&f.forecast_type);
        if cat == SurpriseCategory::Neutral {
            continue;
        }
        let idx = cat.as_i32() as usize;

        // Beta-Binomial update with historical calibration
        let bb = BetaBinomial::new(); // Beta(2,2) prior
        let posterior = bb.update(cat_wins[idx], cat_losses[idx]);

        let p_change_mid = match (f.p_change_min, f.p_change_max) {
            (Some(lo), Some(hi)) => (lo + hi) / 2.0,
            (Some(v), None) | (None, Some(v)) => v,
            (None, None) => 0.0,
        };

        let detail = format!(
            r#"{{"horizon":"5D","category":"{}","sample_size":{},"ci_lower":{:.4},"ci_upper":{:.4},"prior":"Beta(2,2)","p_change_mid":{:.2}}}"#,
            cat.label(),
            posterior.n,
            posterior.ci_low,
            posterior.ci_high,
            p_change_mid,
        );

        // p_upside
        insert_stmt.execute(duckdb::params![
            f.ts_code,
            date_str,
            MODULE,
            "p_upside",
            posterior.mean,
            detail,
        ])?;
        // p_upside_ci_low
        insert_stmt.execute(duckdb::params![
            f.ts_code,
            date_str,
            MODULE,
            "p_upside_ci_low",
            posterior.ci_low,
            serde_null(),
        ])?;
        // p_upside_ci_high
        insert_stmt.execute(duckdb::params![
            f.ts_code,
            date_str,
            MODULE,
            "p_upside_ci_high",
            posterior.ci_high,
            serde_null(),
        ])?;
        // p_upside_n
        insert_stmt.execute(duckdb::params![
            f.ts_code,
            date_str,
            MODULE,
            "p_upside_n",
            posterior.n as f64,
            serde_null(),
        ])?;
        // surprise_category
        insert_stmt.execute(duckdb::params![
            f.ts_code,
            date_str,
            MODULE,
            "surprise_category",
            cat.as_i32() as f64,
            serde_null(),
        ])?;
        // p_change_mid
        insert_stmt.execute(duckdb::params![
            f.ts_code,
            date_str,
            MODULE,
            "p_change_mid",
            p_change_mid,
            serde_null(),
        ])?;

        count += 1;
    }

    info!(
        forecasts = forecasts.len(),
        written = count,
        "announcement_risk computed"
    );
    Ok(count)
}

/// Convenience: return None-coercible empty detail.
fn serde_null() -> Option<String> {
    None
}
