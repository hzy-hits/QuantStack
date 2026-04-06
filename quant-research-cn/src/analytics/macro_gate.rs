/// Macro gate matrix — Axiom 4 overlay.
///
/// 3×3 matrix: benchmark realized vol × yield spread (LPR - Shibor)
///
/// Multiplies composite scores to gate out noise in extreme macro environments.
///
/// Additional: market options stress z-score from opt_daily activity.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::{info, warn};

use crate::config::Settings;

const MODULE: &str = "macro_gate";

/// Use "_MARKET" as ts_code for market-wide macro gate rows.
const MARKET_CODE: &str = "_MARKET";

/// Volatility regime
#[derive(Debug, Clone, Copy)]
pub enum VolRegime {
    Calm,     // < 20% annualized
    Elevated, // 20-35%
    Panic,    // >= 35%
}

impl VolRegime {
    fn as_i32(self) -> i32 {
        match self {
            Self::Calm => 0,
            Self::Elevated => 1,
            Self::Panic => 2,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Calm => "calm",
            Self::Elevated => "elevated",
            Self::Panic => "panic",
        }
    }
}

/// Yield curve regime
#[derive(Debug, Clone, Copy)]
pub enum YieldCurve {
    Normal,   // LPR - Shibor > 1.5  (positive spread, normal)
    Flat,     // 0.5 - 1.5           (converging)
    Steep,    // < 0.5               (Shibor very low → easing)
}

impl YieldCurve {
    fn as_i32(self) -> i32 {
        match self {
            Self::Normal => 0,
            Self::Flat => 1,
            Self::Steep => 2,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Flat => "flat",
            Self::Steep => "steep",
        }
    }
}

/// Gate multiplier lookup — 3×3 vol × yield_curve matrix.
///
/// Steep curve (easing) is favorable for equities; Panic vol is unfavorable.
pub fn gate_multiplier(vol: VolRegime, curve: YieldCurve) -> f64 {
    match (vol, curve) {
        (VolRegime::Calm, YieldCurve::Steep)    => 1.1,
        (VolRegime::Calm, YieldCurve::Normal)    => 1.0,
        (VolRegime::Calm, YieldCurve::Flat)      => 0.9,
        (VolRegime::Elevated, YieldCurve::Steep) => 1.0,
        (VolRegime::Elevated, YieldCurve::Normal) => 0.9,
        (VolRegime::Elevated, YieldCurve::Flat)  => 0.8,
        (VolRegime::Panic, YieldCurve::Steep)    => 0.8,
        (VolRegime::Panic, YieldCurve::Normal)   => 0.7,
        (VolRegime::Panic, YieldCurve::Flat)     => 0.6,
    }
}

/// Asset class multipliers in different vol regimes
pub fn asset_class_multiplier(asset_class: &str, vol: VolRegime) -> f64 {
    match (asset_class, vol) {
        ("tech" | "growth", VolRegime::Calm)     => 1.2,
        ("tech" | "growth", VolRegime::Elevated) => 0.9,
        ("tech" | "growth", VolRegime::Panic)    => 0.7,
        ("consumer" | "dividend", VolRegime::Calm)     => 0.9,
        ("consumer" | "dividend", VolRegime::Elevated) => 1.0,
        ("consumer" | "dividend", VolRegime::Panic)    => 1.1,
        _ => 1.0,
    }
}

/// Classify annualized volatility into regime buckets.
fn classify_vol_regime(vol_ann: f64) -> VolRegime {
    if vol_ann < 20.0 {
        VolRegime::Calm
    } else if vol_ann < 35.0 {
        VolRegime::Elevated
    } else {
        VolRegime::Panic
    }
}

/// Classify yield spread (LPR - Shibor) into curve buckets.
///
/// In China: Shibor very low relative to LPR → easing → "Steep" (wide spread).
/// Spread > 1.5 = Normal, 0.5-1.5 = Flat, < 0.5 = Steep (or inverted/easing).
fn classify_yield_curve(spread: f64) -> YieldCurve {
    if spread > 1.5 {
        YieldCurve::Normal
    } else if spread > 0.5 {
        YieldCurve::Flat
    } else {
        YieldCurve::Steep
    }
}

/// Compute 20-day realized volatility (annualized %) from daily pct_chg.
fn realized_vol(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let daily_std = var.sqrt();
    // Annualize: pct_chg is already in percentage points, so std is in % points.
    // Multiply by sqrt(252) to annualize.
    daily_std * (252.0_f64).sqrt()
}

pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    let benchmark = &cfg.universe.benchmark;

    // ── Step 1: Compute 20D realized volatility of benchmark ──────
    let mut vol_stmt = db.prepare(
        "SELECT pct_chg FROM prices
         WHERE ts_code = ? AND trade_date <= ?
         ORDER BY trade_date DESC
         LIMIT 20",
    )?;

    let returns: Vec<f64> = vol_stmt
        .query_map(duckdb::params![benchmark, date_str], |row| {
            row.get::<_, Option<f64>>(0)
        })?
        .filter_map(|r| r.ok().flatten())
        .collect();

    let vol_ann = if returns.len() >= 5 {
        realized_vol(&returns)
    } else {
        warn!(
            n = returns.len(),
            benchmark = benchmark,
            "insufficient price data for vol, defaulting to Calm"
        );
        15.0 // default: Calm regime
    };

    let vol_regime = classify_vol_regime(vol_ann);
    info!(
        vol_ann = format!("{:.2}", vol_ann),
        regime = vol_regime.label(),
        n = returns.len(),
        "benchmark realized vol computed"
    );

    // ── Step 2: Query Shibor and LPR from macro_cn ────────────────
    // Shibor overnight: SHIBOR_ON (daily), fallback to M0009970 (monthly cn_m)
    // LPR 1yr: LPR_1Y (from shibor_lpr), fallback to M0062063 (monthly cn_m)
    let shibor = query_latest_macro(db, "SHIBOR_ON", &date_str)
        .or_else(|| query_latest_macro(db, "M0009970", &date_str));
    let lpr = query_latest_macro(db, "LPR_1Y", &date_str)
        .or_else(|| query_latest_macro(db, "M0062063", &date_str));

    let (spread, yield_curve) = match (lpr, shibor) {
        (Some(l), Some(s)) => {
            let sp = l - s;
            info!(lpr = l, shibor = s, spread = format!("{:.3}", sp), "yield spread");
            (sp, classify_yield_curve(sp))
        }
        _ => {
            warn!("macro_cn data unavailable for Shibor/LPR, defaulting to Normal");
            (2.0, YieldCurve::Normal) // safe default
        }
    };

    // ── Step 3: Gate multiplier ───────────────────────────────────
    let mult = gate_multiplier(vol_regime, yield_curve);
    info!(
        multiplier = format!("{:.2}", mult),
        vol = vol_regime.label(),
        curve = yield_curve.label(),
        "macro gate computed"
    );

    // ── Step 4: Market options stress z-score ─────────────────────
    let opt_stress = compute_opt_stress(db, &date_str);

    // ── Step 5: Write analytics rows ──────────────────────────────
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let detail = format!(
        r#"{{"benchmark":"{}","vol_ann":{:.2},"vol_regime":"{}","spread":{:.3},"yield_curve":"{}","n_returns":{}}}"#,
        benchmark,
        vol_ann,
        vol_regime.label(),
        spread,
        yield_curve.label(),
        returns.len(),
    );

    // gate_multiplier
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "gate_multiplier",
        mult,
        detail,
    ])?;

    // vol_regime
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "vol_regime",
        vol_regime.as_i32() as f64,
        serde_null(),
    ])?;

    // yield_curve
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "yield_curve",
        yield_curve.as_i32() as f64,
        serde_null(),
    ])?;

    // realized_vol_ann
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "realized_vol_ann",
        vol_ann,
        serde_null(),
    ])?;

    // market_opt_stress
    if let Some(stress) = opt_stress {
        insert_stmt.execute(duckdb::params![
            MARKET_CODE,
            date_str,
            MODULE,
            "market_opt_stress",
            stress,
            serde_null(),
        ])?;
    }

    info!("macro_gate analytics written");
    Ok(1)
}

/// Query the latest value for a macro series on or before `as_of`.
fn query_latest_macro(db: &Connection, series_id: &str, as_of: &str) -> Option<f64> {
    let result = db.prepare(
        "SELECT value FROM macro_cn
         WHERE series_id = ? AND date <= ?
         ORDER BY date DESC
         LIMIT 1",
    );
    match result {
        Ok(mut stmt) => {
            let rows: Vec<Option<f64>> = stmt
                .query_map(duckdb::params![series_id, as_of], |row| {
                    row.get::<_, Option<f64>>(0)
                })
                .ok()?
                .filter_map(|r| r.ok())
                .collect();
            rows.into_iter().next().flatten()
        }
        Err(e) => {
            warn!(series_id = series_id, err = %e, "macro_cn query failed");
            None
        }
    }
}

/// Compute z-score of today's options market activity vs. last 20 days.
///
/// Uses total (vol * amount) from opt_daily as a proxy for options stress.
/// Returns None if data is unavailable.
fn compute_opt_stress(db: &Connection, as_of: &str) -> Option<f64> {
    // Query last 21 days of total options activity (including today)
    let result = db.prepare(
        "SELECT CAST(trade_date AS VARCHAR) AS trade_date, SUM(vol * amount) AS activity
         FROM opt_daily
         WHERE trade_date <= ?
         GROUP BY trade_date
         ORDER BY trade_date DESC
         LIMIT 21",
    );

    let mut stmt = match result {
        Ok(s) => s,
        Err(e) => {
            warn!(err = %e, "opt_daily query failed, skipping opt stress");
            return None;
        }
    };

    let rows: Vec<(String, f64)> = stmt
        .query_map(duckdb::params![as_of], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
            ))
        })
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    if rows.len() < 5 {
        warn!(n = rows.len(), "insufficient opt_daily data for stress z-score");
        return None;
    }

    // First row is today (or most recent), rest are historical
    let today_activity = rows[0].1;
    let hist: Vec<f64> = rows[1..].iter().map(|r| r.1).collect();

    if hist.is_empty() {
        return None;
    }

    let n = hist.len() as f64;
    let mean = hist.iter().sum::<f64>() / n;
    let var = if hist.len() > 1 {
        hist.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0)
    } else {
        0.0
    };
    let std = var.sqrt();

    if std < 1e-10 {
        return Some(0.0);
    }

    let z = ((today_activity - mean) / std).clamp(-3.0, 3.0);
    info!(
        today = format!("{:.0}", today_activity),
        mean = format!("{:.0}", mean),
        z = format!("{:.2}", z),
        "options stress z-score"
    );
    Some(z)
}

fn serde_null() -> Option<String> {
    None
}
