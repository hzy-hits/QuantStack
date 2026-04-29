/// Shadow option analytics for A-shares.
///
/// Design:
///   1. `shadow_fast` runs cross-sectionally for all stocks using market option
///      curves + stock-specific realized/idiosyncratic vol proxies.
///   2. `shadow_full` prices a small shortlist only (notable candidates,
///      watchlist, and event names), producing put/floor/touch metrics.
///
/// This avoids fitting a heavy "full option model" for every stock while still
/// allowing shadow volatility to participate in filtering and reporting.
use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::json;
use tracing::{info, warn};

use crate::analytics::rv::yang_zhang_vol;
use crate::config::Settings;

pub const MODULE_FAST: &str = "shadow_fast";
pub const MODULE_FULL: &str = "shadow_full";

const OPTION_FAMILIES: [(&str, &str); 9] = [
    ("OP510050", "上证50ETF"),
    ("OP510300", "沪深300ETF"),
    ("OP159919", "沪深300ETF(深)"),
    ("OP510500", "中证500ETF"),
    ("OP159922", "中证500ETF(深)"),
    ("OP159915", "创业板ETF"),
    ("OP159901", "深证100ETF"),
    ("OP588000", "科创50ETF"),
    ("OP588080", "科创50ETF(易方达)"),
];

#[derive(Debug, Clone)]
struct MarketCurve {
    family: String,
    label: String,
    source_trade_date: Option<NaiveDate>,
    iv_30d: Option<f64>,
    iv_60d: Option<f64>,
    iv_90d: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct EventRisk {
    p_upside: Option<f64>,
    p_drop: Option<f64>,
    unlock_days: Option<f64>,
    float_ratio: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct StockFeature {
    ts_code: String,
    market: String,
    industry: String,
    bars: Vec<(f64, f64, f64, f64)>,
    returns: Vec<f64>,
    vol_5d: f64,
    vol_20d: f64,
    vol_60d: f64,
    latest_close: f64,
    event: EventRisk,
}

#[derive(Clone)]
struct MarketChainRow {
    call_put: String,
    strike: f64,
    settle: f64,
    oi: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ShadowFullMetrics {
    pub put_90_3m: Option<f64>,
    pub put_80_3m: Option<f64>,
    pub touch_90_3m: Option<f64>,
    pub floor_1sigma_3m: Option<f64>,
    pub floor_2sigma_3m: Option<f64>,
    pub skew_90_3m: Option<f64>,
}

pub fn compute_fast(db: &Connection, _cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let date_str = as_of.to_string();
    db.execute(
        "DELETE FROM analytics WHERE as_of = ? AND module = ?",
        duckdb::params![date_str, MODULE_FAST],
    )?;

    let curves = compute_market_curves(db, as_of)?;
    write_market_curves(db, as_of, &curves)?;

    let memberships = load_index_memberships(db, as_of)?;
    let proxy_returns = load_proxy_returns(db, as_of)?;
    let proxy_realized = compute_proxy_realized_vols(&proxy_returns);
    let industry_multiplier =
        compute_industry_multipliers(db, as_of, proxy_realized.get("000300.SH").copied());
    let features = load_stock_features(db, as_of)?;

    if features.is_empty() {
        warn!("shadow_fast: no stock features loaded");
        return Ok(0);
    }

    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let fallback_market = proxy_realized.get("000300.SH").copied().unwrap_or(24.0) * 1.10;
    let risk_free = query_shibor_rate(db, &date_str).unwrap_or(1.5) / 100.0;
    let mut written = 0usize;

    for feature in features.values() {
        if feature.latest_close <= 0.0 || feature.returns.len() < 20 {
            continue;
        }

        let family = choose_proxy_family(feature, &memberships, &curves);
        let curve = curves
            .get(&family)
            .or_else(|| curves.get("OP510300"))
            .or_else(|| curves.values().next());
        let Some(curve) = curve else { continue };

        let benchmark_code = benchmark_for_family(&curve.family);
        let benchmark_returns = proxy_returns.get(benchmark_code);
        let beta = benchmark_returns
            .and_then(|bench| compute_beta(&feature.returns, bench))
            .unwrap_or(1.0)
            .clamp(0.0, 2.5);
        let idio_vol = benchmark_returns
            .and_then(|bench| compute_idio_vol(&feature.returns, bench, beta))
            .unwrap_or((0.65 * feature.vol_20d + 0.35 * feature.vol_60d).max(8.0))
            .clamp(6.0, 95.0);
        let sector_mult = industry_multiplier
            .get(feature.industry.as_str())
            .copied()
            .unwrap_or(1.0);
        let har_vol = blend_har_vol(feature.vol_5d, feature.vol_20d, feature.vol_60d);
        let jump_addon = compute_jump_addon(feature);
        let event_addon = compute_event_addon(&feature.event);
        let market_30 = curve.iv_30d.unwrap_or(fallback_market);
        let market_60 = curve.iv_60d.unwrap_or(market_30 * 0.97);
        let market_90 = curve.iv_90d.unwrap_or(market_60 * 0.96);

        let shadow_30 = shadow_iv_from_components(
            market_30,
            beta,
            sector_mult,
            har_vol,
            idio_vol,
            jump_addon,
            event_addon,
            30,
        );
        let shadow_60 = shadow_iv_from_components(
            market_60,
            beta,
            sector_mult,
            har_vol,
            idio_vol,
            jump_addon,
            event_addon,
            60,
        );
        let shadow_90 = shadow_iv_from_components(
            market_90,
            beta,
            sector_mult,
            har_vol,
            idio_vol,
            jump_addon,
            event_addon,
            90,
        );
        let downside_stress = compute_downside_stress(
            shadow_30,
            market_30,
            jump_addon,
            event_addon,
            &feature.event,
        );

        let detail = json!({
            "proxy_family": curve.family,
            "proxy_label": curve.label,
            "proxy_trade_date": curve.source_trade_date.map(|d| d.to_string()),
            "benchmark_code": benchmark_code,
            "beta": round3(beta),
            "market_iv_30d": round2(market_30),
            "market_iv_60d": round2(market_60),
            "market_iv_90d": round2(market_90),
            "har_vol": round2(har_vol),
            "idio_vol": round2(idio_vol),
            "sector_multiplier": round3(sector_mult),
            "jump_addon": round2(jump_addon),
            "event_addon": round2(event_addon),
            "risk_free": round4(risk_free),
        })
        .to_string();

        for (metric, value) in [
            ("shadow_iv_30d", shadow_30),
            ("shadow_iv_60d", shadow_60),
            ("shadow_iv_90d", shadow_90),
            ("downside_stress", downside_stress),
        ] {
            insert.execute(duckdb::params![
                feature.ts_code,
                date_str,
                MODULE_FAST,
                metric,
                value,
                detail.clone()
            ])?;
        }

        written += 1;
    }

    info!(
        rows = written,
        curves = curves.len(),
        "shadow_fast complete"
    );
    Ok(written)
}

pub fn enrich_symbols_full(db: &Connection, as_of: NaiveDate, symbols: &[String]) -> Result<usize> {
    let target: BTreeSet<String> = symbols.iter().cloned().collect();
    if target.is_empty() {
        return Ok(0);
    }

    let date_str = as_of.to_string();
    let risk_free = query_shibor_rate(db, &date_str).unwrap_or(1.5) / 100.0;

    let mut existing_fast: HashMap<String, HashMap<String, (f64, Option<String>)>> = HashMap::new();
    let mut stmt = db.prepare(
        "SELECT ts_code, metric, value, detail
         FROM analytics
         WHERE as_of = ? AND module = ?",
    )?;
    let rows = stmt.query_map(duckdb::params![date_str, MODULE_FAST], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    for row in rows.flatten() {
        if target.contains(&row.0) {
            existing_fast
                .entry(row.0)
                .or_default()
                .insert(row.1, (row.2, row.3));
        }
    }

    if existing_fast.is_empty() {
        return Ok(0);
    }

    let mut prices = HashMap::new();
    let mut price_stmt = db.prepare(
        "WITH ranked AS (
             SELECT ts_code, close,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
             FROM prices
             WHERE trade_date <= CAST(? AS DATE)
         )
         SELECT ts_code, close
         FROM ranked
         WHERE rn = 1",
    )?;
    let price_rows = price_stmt.query_map(duckdb::params![date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<f64>>(1)?.unwrap_or(0.0),
        ))
    })?;
    for row in price_rows.flatten() {
        if target.contains(&row.0) {
            prices.insert(row.0, row.1);
        }
    }

    let mut delete =
        db.prepare("DELETE FROM analytics WHERE ts_code = ? AND as_of = ? AND module = ?")?;
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let mut written = 0usize;
    for symbol in target {
        let Some(metrics) = existing_fast.get(&symbol) else {
            continue;
        };
        let close = prices.get(&symbol).copied().unwrap_or(0.0);
        if close <= 0.0 {
            continue;
        }

        let atm_30 = metrics.get("shadow_iv_30d").map(|v| v.0).unwrap_or(0.0);
        let atm_90 = metrics.get("shadow_iv_90d").map(|v| v.0).unwrap_or(atm_30);
        let downside_stress = metrics.get("downside_stress").map(|v| v.0).unwrap_or(0.0);
        if atm_30 <= 0.0 || atm_90 <= 0.0 {
            continue;
        }

        let down90 = (atm_90 * (1.0 + 0.18 * downside_stress)).clamp(8.0, 140.0) / 100.0;
        let down80 = (atm_90 * (1.0 + 0.30 * downside_stress)).clamp(8.0, 160.0) / 100.0;
        let atm_sigma = atm_90 / 100.0;
        let strike_90 = close * 0.90;
        let strike_80 = close * 0.80;
        let t = 0.25;

        let put_90 = black_scholes_put(close, strike_90, t, risk_free, down90);
        let put_80 = black_scholes_put(close, strike_80, t, risk_free, down80);
        let touch_90 = prob_touch(strike_90, close, risk_free, down90, t);
        let floor_1sigma = close * (-atm_sigma * t.sqrt()).exp();
        let floor_2sigma = close * (-2.0 * atm_sigma * t.sqrt()).exp();
        let skew_90 = ((down90 - atm_sigma) * 100.0).max(0.0);

        delete.execute(duckdb::params![symbol, date_str, MODULE_FULL])?;
        let detail = json!({
            "pricing_horizon_days": 90,
            "risk_free": round4(risk_free),
            "atm_iv_90d": round2(atm_90),
            "downside_iv_90pct_put": round2(down90 * 100.0),
            "downside_iv_80pct_put": round2(down80 * 100.0),
        })
        .to_string();

        for (metric, value) in [
            ("shadow_put_90_3m", put_90),
            ("shadow_put_80_3m", put_80),
            ("shadow_touch_90_3m", touch_90),
            ("shadow_floor_1sigma_3m", floor_1sigma),
            ("shadow_floor_2sigma_3m", floor_2sigma),
            ("shadow_skew_90_3m", skew_90),
        ] {
            insert.execute(duckdb::params![
                symbol,
                date_str,
                MODULE_FULL,
                metric,
                value,
                detail.clone()
            ])?;
        }
        written += 1;
    }

    info!(rows = written, "shadow_full shortlist pricing complete");
    Ok(written)
}

pub fn load_full_metrics(
    db: &Connection,
    as_of: NaiveDate,
    symbols: &[String],
) -> HashMap<String, ShadowFullMetrics> {
    let targets: HashSet<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let mut out = HashMap::new();
    if targets.is_empty() {
        return out;
    }

    let date_str = as_of.to_string();
    let sql = "SELECT ts_code, metric, value
               FROM analytics
               WHERE as_of = ? AND module = ?";
    let Ok(mut stmt) = db.prepare(sql) else {
        return out;
    };

    let Ok(rows) = stmt.query_map(duckdb::params![date_str, MODULE_FULL], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
        ))
    }) else {
        return out;
    };

    for row in rows.flatten() {
        if !targets.contains(row.0.as_str()) {
            continue;
        }
        let entry = out.entry(row.0).or_insert_with(ShadowFullMetrics::default);
        match row.1.as_str() {
            "shadow_put_90_3m" => entry.put_90_3m = Some(row.2),
            "shadow_put_80_3m" => entry.put_80_3m = Some(row.2),
            "shadow_touch_90_3m" => entry.touch_90_3m = Some(row.2),
            "shadow_floor_1sigma_3m" => entry.floor_1sigma_3m = Some(row.2),
            "shadow_floor_2sigma_3m" => entry.floor_2sigma_3m = Some(row.2),
            "shadow_skew_90_3m" => entry.skew_90_3m = Some(row.2),
            _ => {}
        }
    }

    out
}

fn compute_market_curves(
    db: &Connection,
    as_of: NaiveDate,
) -> Result<HashMap<String, MarketCurve>> {
    let mut curves = HashMap::new();
    for (family, label) in OPTION_FAMILIES {
        if let Some(curve) = compute_family_curve(db, as_of, family, label)? {
            curves.insert(family.to_string(), curve);
        }
    }
    Ok(curves)
}

fn compute_family_curve(
    db: &Connection,
    as_of: NaiveDate,
    family: &str,
    label: &str,
) -> Result<Option<MarketCurve>> {
    let date_str = as_of.to_string();
    let mut stmt = db.prepare(
        "SELECT
             CAST(od.trade_date AS VARCHAR) AS trade_date,
             ob.call_put,
             ob.exercise_price,
             CAST(ob.maturity_date AS VARCHAR) AS maturity_date,
             od.settle,
             od.oi
         FROM opt_daily od
         JOIN opt_basic ob ON od.ts_code = ob.ts_code
         WHERE od.trade_date = (SELECT MAX(trade_date) FROM opt_daily WHERE trade_date <= ?)
           AND ob.opt_code LIKE ?
           AND ob.exercise_price IS NOT NULL
           AND ob.maturity_date IS NOT NULL
           AND od.settle IS NOT NULL
           AND od.settle > 0
         ORDER BY ob.maturity_date, ob.exercise_price",
    )?;

    #[derive(Clone)]
    struct OptRow {
        trade_date: NaiveDate,
        maturity: NaiveDate,
        chain: MarketChainRow,
    }

    let pattern = format!("{family}%");
    let rows: Vec<OptRow> = stmt
        .query_map(duckdb::params![date_str, pattern], |row| {
            let trade_date = row.get::<_, String>(0)?;
            let maturity = row.get::<_, String>(3)?;
            Ok(OptRow {
                trade_date: NaiveDate::parse_from_str(&trade_date, "%Y-%m-%d").unwrap_or(as_of),
                maturity: NaiveDate::parse_from_str(&maturity, "%Y-%m-%d").unwrap_or(as_of),
                chain: MarketChainRow {
                    call_put: row.get::<_, String>(1)?,
                    strike: row.get::<_, f64>(2)?,
                    settle: row.get::<_, f64>(4)?,
                    oi: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                },
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        return Ok(None);
    }

    let source_trade_date = rows.first().map(|r| r.trade_date);
    let mut by_maturity: HashMap<NaiveDate, Vec<MarketChainRow>> = HashMap::new();
    for row in rows {
        by_maturity.entry(row.maturity).or_default().push(row.chain);
    }

    let risk_free = query_shibor_rate(db, &date_str).unwrap_or(1.5) / 100.0;
    let mut points = Vec::new();
    for (maturity, chain) in by_maturity {
        let days = (maturity - as_of).num_days();
        if days < 7 {
            continue;
        }
        if let Some(iv) = compute_single_term_iv(&chain, days, risk_free) {
            points.push((days as f64, iv));
        }
    }
    points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    if points.is_empty() {
        return Ok(None);
    }

    Ok(Some(MarketCurve {
        family: family.to_string(),
        label: label.to_string(),
        source_trade_date,
        iv_30d: interpolate_curve(&points, 30.0),
        iv_60d: interpolate_curve(&points, 60.0),
        iv_90d: interpolate_curve(&points, 90.0),
    }))
}

fn compute_single_term_iv(
    chain: &[MarketChainRow],
    days_to_exp: i64,
    risk_free: f64,
) -> Option<f64> {
    let t_years = days_to_exp as f64 / 365.0;
    if t_years < 0.01 {
        return None;
    }

    let mut calls = Vec::new();
    let mut puts = Vec::new();
    for opt in chain {
        if opt.call_put == "C" {
            calls.push((opt.strike, opt.settle, opt.oi));
        } else if opt.call_put == "P" {
            puts.push((opt.strike, opt.settle, opt.oi));
        }
    }
    calls.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    puts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    if calls.len() < 2 || puts.len() < 2 {
        return None;
    }

    let mut min_diff = f64::MAX;
    let mut forward = 0.0;
    for c in &calls {
        for p in &puts {
            if (c.0 - p.0).abs() < 1e-8 {
                let diff = (c.1 - p.1).abs();
                if diff < min_diff {
                    min_diff = diff;
                    forward = c.0 + (risk_free * t_years).exp() * (c.1 - p.1);
                }
            }
        }
    }
    if forward <= 0.0 {
        let total_oi: f64 = calls.iter().map(|c| c.2).sum();
        forward = if total_oi > 0.0 {
            calls.iter().map(|c| c.0 * c.2).sum::<f64>() / total_oi
        } else {
            calls[calls.len() / 2].0
        };
    }

    let mut all_strikes: Vec<f64> = calls
        .iter()
        .map(|c| c.0)
        .chain(puts.iter().map(|p| p.0))
        .collect();
    all_strikes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    all_strikes.dedup_by(|a, b| (*a - *b).abs() < 1e-8);
    let k0 = all_strikes
        .iter()
        .filter(|&&k| k <= forward)
        .last()
        .copied()
        .unwrap_or(forward);

    let mut strike_prices: Vec<(f64, f64)> = Vec::new();
    for p in &puts {
        if p.0 < k0 {
            strike_prices.push((p.0, p.1));
        }
    }
    if let Some(atm_call) = calls.iter().find(|c| (c.0 - k0).abs() < 1e-8) {
        if let Some(atm_put) = puts.iter().find(|p| (p.0 - k0).abs() < 1e-8) {
            strike_prices.push((k0, (atm_call.1 + atm_put.1) / 2.0));
        }
    }
    for c in &calls {
        if c.0 > k0 {
            strike_prices.push((c.0, c.1));
        }
    }
    strike_prices.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    if strike_prices.len() < 3 {
        return None;
    }

    let ert = (risk_free * t_years).exp();
    let mut sigma2 = 0.0;
    for i in 0..strike_prices.len() {
        let (k, q) = strike_prices[i];
        let dk = if i == 0 {
            strike_prices[1].0 - k
        } else if i == strike_prices.len() - 1 {
            k - strike_prices[i - 1].0
        } else {
            (strike_prices[i + 1].0 - strike_prices[i - 1].0) / 2.0
        };
        sigma2 += (dk / (k * k)) * ert * q;
    }
    sigma2 = (2.0 / t_years) * sigma2 - (1.0 / t_years) * (forward / k0 - 1.0).powi(2);
    if sigma2 <= 0.0 {
        return None;
    }
    Some((sigma2.sqrt() * 100.0).clamp(8.0, 120.0))
}

fn interpolate_curve(points: &[(f64, f64)], target_days: f64) -> Option<f64> {
    if points.is_empty() {
        return None;
    }
    if points.len() == 1 {
        return Some(points[0].1);
    }
    if target_days <= points[0].0 {
        return Some(points[0].1);
    }
    if target_days >= points[points.len() - 1].0 {
        return Some(points[points.len() - 1].1);
    }
    for w in points.windows(2) {
        let (d1, v1) = w[0];
        let (d2, v2) = w[1];
        if target_days >= d1 && target_days <= d2 {
            let wgt = (target_days - d1) / (d2 - d1).max(1e-6);
            return Some(v1 + wgt * (v2 - v1));
        }
    }
    None
}

fn write_market_curves(
    db: &Connection,
    as_of: NaiveDate,
    curves: &HashMap<String, MarketCurve>,
) -> Result<()> {
    let date_str = as_of.to_string();
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;
    for curve in curves.values() {
        let ts_code = format!("_SHADOW_CURVE_{}", curve.family);
        let detail = json!({
            "family": curve.family,
            "label": curve.label,
            "source_trade_date": curve.source_trade_date.map(|d| d.to_string()),
        })
        .to_string();
        for (metric, value) in [
            ("shadow_iv_30d", curve.iv_30d.unwrap_or(0.0)),
            ("shadow_iv_60d", curve.iv_60d.unwrap_or(0.0)),
            ("shadow_iv_90d", curve.iv_90d.unwrap_or(0.0)),
        ] {
            insert.execute(duckdb::params![
                ts_code,
                date_str,
                MODULE_FAST,
                metric,
                value,
                detail.clone()
            ])?;
        }
    }
    Ok(())
}

fn load_index_memberships(
    db: &Connection,
    as_of: NaiveDate,
) -> Result<HashMap<String, HashSet<String>>> {
    let mut out = HashMap::new();
    for index_code in ["000016.SH", "000300.SH", "000905.SH"] {
        let sql = "SELECT con_code
                   FROM index_weight
                   WHERE index_code = ?
                     AND trade_date = (
                         SELECT MAX(trade_date) FROM index_weight
                         WHERE index_code = ? AND trade_date <= CAST(? AS DATE)
                     )";
        let mut stmt = db.prepare(sql)?;
        let rows = stmt.query_map(
            duckdb::params![index_code, index_code, as_of.to_string()],
            |row| row.get::<_, String>(0),
        )?;
        let set: HashSet<String> = rows.filter_map(|r| r.ok()).collect();
        out.insert(index_code.to_string(), set);
    }
    Ok(out)
}

fn load_proxy_returns(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, Vec<f64>>> {
    let lookback = (as_of - chrono::Duration::days(120)).to_string();
    let mut stmt = db.prepare(
        "SELECT ts_code, pct_chg
         FROM prices
         WHERE ts_code IN ('000016.SH', '000300.SH', '000905.SH', '399006.SZ')
           AND trade_date >= CAST(? AS DATE)
           AND trade_date <= CAST(? AS DATE)
         ORDER BY ts_code, trade_date",
    )?;
    let rows = stmt.query_map(duckdb::params![lookback, as_of.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<f64>>(1)?.unwrap_or(0.0) / 100.0,
        ))
    })?;

    let mut out: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows.flatten() {
        out.entry(row.0).or_default().push(row.1);
    }
    Ok(out)
}

fn compute_proxy_realized_vols(returns: &HashMap<String, Vec<f64>>) -> HashMap<String, f64> {
    returns
        .iter()
        .map(|(code, rets)| {
            (
                code.clone(),
                annualized_std(rets).unwrap_or(20.0).clamp(8.0, 80.0),
            )
        })
        .collect()
}

fn compute_industry_multipliers(
    db: &Connection,
    as_of: NaiveDate,
    benchmark_rv: Option<f64>,
) -> HashMap<String, f64> {
    let benchmark_rv = benchmark_rv.unwrap_or(24.0).max(6.0);
    let lookback = (as_of - chrono::Duration::days(40)).to_string();
    let sql = "SELECT sb.industry, p.trade_date, AVG(COALESCE(p.pct_chg, 0)) / 100.0
               FROM prices p
               JOIN stock_basic sb ON p.ts_code = sb.ts_code
               WHERE p.trade_date >= CAST(? AS DATE)
                 AND p.trade_date <= CAST(? AS DATE)
                 AND sb.list_status = 'L'
                 AND sb.industry IS NOT NULL
                 AND sb.industry != ''
               GROUP BY sb.industry, p.trade_date
               ORDER BY sb.industry, p.trade_date";
    let Ok(mut stmt) = db.prepare(sql) else {
        return HashMap::new();
    };
    let Ok(rows) = stmt.query_map(duckdb::params![lookback, as_of.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, f64>(2).unwrap_or(0.0),
        ))
    }) else {
        return HashMap::new();
    };

    let mut industry_returns: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows.flatten() {
        industry_returns.entry(row.0).or_default().push(row.1);
    }
    industry_returns
        .into_iter()
        .map(|(industry, rets)| {
            let rv = annualized_std(&rets).unwrap_or(benchmark_rv);
            let mult = (rv / benchmark_rv).clamp(0.80, 1.35);
            (industry, mult)
        })
        .collect()
}

fn load_stock_features(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, StockFeature>> {
    let lookback = (as_of - chrono::Duration::days(120)).to_string();
    let mut stmt = db.prepare(
        "WITH ranked AS (
             SELECT p.ts_code, p.trade_date, p.open, p.high, p.low, p.close, p.pct_chg,
                    ROW_NUMBER() OVER (PARTITION BY p.ts_code ORDER BY p.trade_date DESC) AS rn
             FROM prices p
             JOIN stock_basic sb ON p.ts_code = sb.ts_code
             WHERE p.trade_date >= CAST(? AS DATE)
               AND p.trade_date <= CAST(? AS DATE)
               AND sb.list_status = 'L'
         )
         SELECT sb.ts_code, COALESCE(sb.market, ''), COALESCE(sb.industry, ''),
                r.open, r.high, r.low, r.close, COALESCE(r.pct_chg, 0)
         FROM ranked r
         JOIN stock_basic sb ON r.ts_code = sb.ts_code
         WHERE r.rn <= 65
         ORDER BY sb.ts_code, r.trade_date",
    )?;
    let rows = stmt.query_map(duckdb::params![lookback, as_of.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
            row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
            row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
            row.get::<_, Option<f64>>(6)?.unwrap_or(0.0),
            row.get::<_, Option<f64>>(7)?.unwrap_or(0.0) / 100.0,
        ))
    })?;

    let event_map = load_event_risk_map(db, as_of)?;
    let mut features: HashMap<String, StockFeature> = HashMap::new();
    for row in rows.flatten() {
        let entry = features
            .entry(row.0.clone())
            .or_insert_with(|| StockFeature {
                ts_code: row.0.clone(),
                market: row.1.clone(),
                industry: row.2.clone(),
                bars: Vec::new(),
                returns: Vec::new(),
                vol_5d: 0.0,
                vol_20d: 0.0,
                vol_60d: 0.0,
                latest_close: 0.0,
                event: event_map.get(&row.0).cloned().unwrap_or_default(),
            });
        entry.market = row.1;
        entry.industry = row.2;
        entry.bars.push((row.3, row.4, row.5, row.6));
        entry.returns.push(row.7);
        entry.latest_close = row.6;
    }

    for feature in features.values_mut() {
        feature.vol_5d = vol_from_recent_bars(&feature.bars, 5);
        feature.vol_20d = vol_from_recent_bars(&feature.bars, 20);
        feature.vol_60d = vol_from_recent_bars(&feature.bars, 60);
    }

    Ok(features)
}

fn load_event_risk_map(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, EventRisk>> {
    let date_str = as_of.to_string();
    let mut stmt = db.prepare(
        "SELECT ts_code, module, metric, value
         FROM analytics
         WHERE as_of = ?
           AND module IN ('announcement', 'unlock')
           AND metric IN ('p_upside', 'p_drop', 'days_to_unlock', 'float_ratio')",
    )?;
    let rows = stmt.query_map(duckdb::params![date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, f64>(3)?,
        ))
    })?;

    let mut out = HashMap::new();
    for row in rows.flatten() {
        let entry = out.entry(row.0).or_insert_with(EventRisk::default);
        match (row.1.as_str(), row.2.as_str()) {
            ("announcement", "p_upside") => entry.p_upside = Some(row.3),
            ("unlock", "p_drop") => entry.p_drop = Some(row.3),
            ("unlock", "days_to_unlock") => entry.unlock_days = Some(row.3),
            ("unlock", "float_ratio") => entry.float_ratio = Some(row.3),
            _ => {}
        }
    }
    Ok(out)
}

fn choose_proxy_family(
    feature: &StockFeature,
    memberships: &HashMap<String, HashSet<String>>,
    curves: &HashMap<String, MarketCurve>,
) -> String {
    let ts_code = feature.ts_code.as_str();
    if memberships
        .get("000016.SH")
        .map(|s| s.contains(ts_code))
        .unwrap_or(false)
        && curves.contains_key("OP510050")
    {
        return "OP510050".to_string();
    }
    if feature.market.contains("创业板") || ts_code.starts_with("300") || ts_code.starts_with("301")
    {
        for family in ["OP159915", "OP510300", "OP159919"] {
            if curves.contains_key(family) {
                return family.to_string();
            }
        }
    }
    if ts_code.starts_with("688") {
        for family in ["OP588000", "OP588080", "OP510300"] {
            if curves.contains_key(family) {
                return family.to_string();
            }
        }
    }
    if memberships
        .get("000905.SH")
        .map(|s| s.contains(ts_code))
        .unwrap_or(false)
    {
        for family in ["OP510500", "OP159922", "OP510300"] {
            if curves.contains_key(family) {
                return family.to_string();
            }
        }
    }
    if memberships
        .get("000300.SH")
        .map(|s| s.contains(ts_code))
        .unwrap_or(false)
    {
        for family in ["OP510300", "OP159919", "OP510050"] {
            if curves.contains_key(family) {
                return family.to_string();
            }
        }
    }
    for family in [
        "OP510300", "OP159919", "OP510500", "OP159922", "OP510050", "OP159901",
    ] {
        if curves.contains_key(family) {
            return family.to_string();
        }
    }
    "OP510300".to_string()
}

fn benchmark_for_family(family: &str) -> &'static str {
    match family {
        "OP510050" => "000016.SH",
        "OP510500" | "OP159922" => "000905.SH",
        "OP159915" => "399006.SZ",
        _ => "000300.SH",
    }
}

fn blend_har_vol(vol_5d: f64, vol_20d: f64, vol_60d: f64) -> f64 {
    let base = 0.20 * vol_5d.max(0.0) + 0.55 * vol_20d.max(0.0) + 0.25 * vol_60d.max(0.0);
    base.clamp(8.0, 95.0)
}

fn shadow_iv_from_components(
    market_iv: f64,
    beta: f64,
    sector_mult: f64,
    har_vol: f64,
    idio_vol: f64,
    jump_addon: f64,
    event_addon: f64,
    tenor_days: u32,
) -> f64 {
    let beta_abs = beta.abs().clamp(0.45, 1.80);
    let market_component = market_iv * beta_abs;
    let sector_component = market_iv * sector_mult;
    let base_var = 0.45 * market_component.powi(2)
        + 0.15 * sector_component.powi(2)
        + 0.20 * har_vol.powi(2)
        + 0.20 * idio_vol.powi(2);
    let tenor_weight = match tenor_days {
        0..=30 => 1.0,
        31..=60 => 0.82,
        _ => 0.68,
    };
    let addon = tenor_weight * (jump_addon + event_addon);
    (base_var.sqrt() + addon).clamp(10.0, 140.0)
}

fn compute_jump_addon(feature: &StockFeature) -> f64 {
    let vol_jump = (feature.vol_5d - feature.vol_20d).max(0.0) * 0.35;
    let worst_neg = feature
        .returns
        .iter()
        .copied()
        .fold(0.0_f64, |acc, r| acc.min(r));
    let tail_addon = ((-worst_neg * 100.0) - 2.0).max(0.0) * 0.40;
    (vol_jump + tail_addon).clamp(0.0, 16.0)
}

fn compute_event_addon(event: &EventRisk) -> f64 {
    let mut addon = 0.0;
    if let Some(p_upside) = event.p_upside {
        addon += (0.55 - p_upside).max(0.0) * 16.0;
    }
    if let Some(p_drop) = event.p_drop {
        let proximity = match event.unlock_days.unwrap_or(999.0) {
            d if d <= 5.0 => 1.0,
            d if d <= 15.0 => 0.7,
            d if d <= 30.0 => 0.4,
            _ => 0.2,
        };
        let size = match event.float_ratio.unwrap_or(0.0) {
            r if r > 5.0 => 1.0,
            r if r > 2.0 => 0.7,
            _ => 0.4,
        };
        addon += p_drop.max(0.0) * 10.0 * proximity * size;
    }
    addon.clamp(0.0, 18.0)
}

fn compute_downside_stress(
    shadow_30: f64,
    market_30: f64,
    jump_addon: f64,
    event_addon: f64,
    event: &EventRisk,
) -> f64 {
    let vol_gap = ((shadow_30 - market_30).max(0.0) / 18.0).min(1.0);
    let jump = (jump_addon / 12.0).min(1.0);
    let evt = (event_addon / 12.0).min(1.0);
    let bearish_evt = event
        .p_upside
        .map(|p| (0.5 - p).max(0.0) * 4.0)
        .unwrap_or(0.0)
        .min(1.0);
    (0.45 * vol_gap + 0.25 * jump + 0.20 * evt + 0.10 * bearish_evt).clamp(0.0, 1.0)
}

fn compute_beta(stock_returns: &[f64], bench_returns: &[f64]) -> Option<f64> {
    let n = stock_returns.len().min(bench_returns.len());
    if n < 10 {
        return None;
    }
    let s = &stock_returns[stock_returns.len() - n..];
    let b = &bench_returns[bench_returns.len() - n..];
    let s_mean = s.iter().sum::<f64>() / n as f64;
    let b_mean = b.iter().sum::<f64>() / n as f64;
    let cov = s
        .iter()
        .zip(b.iter())
        .map(|(sr, br)| (sr - s_mean) * (br - b_mean))
        .sum::<f64>()
        / n as f64;
    let var_b = b.iter().map(|br| (br - b_mean).powi(2)).sum::<f64>() / n as f64;
    if var_b < 1e-10 {
        return None;
    }
    Some(cov / var_b)
}

fn compute_idio_vol(stock_returns: &[f64], bench_returns: &[f64], beta: f64) -> Option<f64> {
    let n = stock_returns.len().min(bench_returns.len());
    if n < 10 {
        return None;
    }
    let residuals: Vec<f64> = stock_returns[stock_returns.len() - n..]
        .iter()
        .zip(bench_returns[bench_returns.len() - n..].iter())
        .map(|(sr, br)| sr - beta * br)
        .collect();
    annualized_std(&residuals)
}

fn vol_from_recent_bars(bars: &[(f64, f64, f64, f64)], window: usize) -> f64 {
    if bars.len() < window {
        return 0.0;
    }
    let recent = &bars[bars.len() - window..];
    yang_zhang_vol(recent, 252.0)
}

fn annualized_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() as f64);
    Some((var.max(0.0) * 252.0).sqrt() * 100.0)
}

fn black_scholes_put(s: f64, k: f64, t: f64, r: f64, sigma: f64) -> f64 {
    if s <= 0.0 || k <= 0.0 || t <= 0.0 || sigma <= 0.0 {
        return 0.0;
    }
    let d1 = ((s / k).ln() + (r + 0.5 * sigma * sigma) * t) / (sigma * t.sqrt());
    let d2 = d1 - sigma * t.sqrt();
    k * (-r * t).exp() * norm_cdf(-d2) - s * norm_cdf(-d1)
}

fn prob_touch(k: f64, s: f64, mu: f64, sigma: f64, t: f64) -> f64 {
    if k >= s || sigma <= 0.0 || t <= 0.0 {
        return 1.0;
    }
    let a = (k / s).ln();
    let b = (mu - 0.5 * sigma * sigma) * t;
    let c = sigma * t.sqrt();
    if c <= 0.0 {
        return 0.0;
    }
    let p = norm_cdf((a - b) / c)
        + ((2.0 * a * (mu - 0.5 * sigma * sigma)) / (sigma * sigma)).exp() * norm_cdf((a + b) / c);
    p.clamp(0.0, 1.0)
}

fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + statrs_like_erf(x / 2.0_f64.sqrt()))
}

fn statrs_like_erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let ax = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * ax);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
            + 0.254829592)
            * t
            * (-ax * ax).exp();
    sign * y
}

fn query_shibor_rate(db: &Connection, date_str: &str) -> Option<f64> {
    db.prepare(
        "SELECT value FROM macro_cn
         WHERE series_id = 'SHIBOR_1M' AND date <= ?
         ORDER BY date DESC LIMIT 1",
    )
    .ok()?
    .query_map(duckdb::params![date_str], |row| {
        row.get::<_, Option<f64>>(0)
    })
    .ok()?
    .flatten()
    .flatten()
    .next()
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

fn round4(x: f64) -> f64 {
    (x * 10000.0).round() / 10000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            ApiConfig, AssetClassConfig, DataConfig, EnrichmentConfig, FilterConfig, MacroConfig,
            OutputConfig, ReportingConfig, RuntimeConfig, ScanConfig, Settings, SignalsConfig,
            UniverseConfig,
        },
        storage,
    };

    fn test_settings() -> Settings {
        Settings {
            api: ApiConfig {
                tushare_token: String::new(),
                deepseek_key: String::new(),
            },
            runtime: RuntimeConfig {
                timezone: "Asia/Shanghai".to_string(),
                random_seed: 1,
            },
            universe: UniverseConfig {
                benchmark: "000300.SH".to_string(),
                scan: ScanConfig {
                    csi300: true,
                    csi500: false,
                    csi1000: false,
                    sse50: false,
                },
                asset_classes: AssetClassConfig {
                    sector_etfs: false,
                    bond_etfs: false,
                    commodity_etfs: false,
                    cross_border: false,
                },
                watchlist: Vec::new(),
                filters: FilterConfig {
                    min_avg_volume_shares: 0,
                    min_price: 0.0,
                },
            },
            output: OutputConfig {
                max_notable_items: 10,
                min_notable_items: 1,
            },
            data: DataConfig {
                db_path: String::new(),
                raw_db_path: String::new(),
                research_db_path: String::new(),
                report_db_path: String::new(),
                dev_db_path: String::new(),
                use_dev_for_research: false,
                constituent_refresh_days: 7,
            },
            signals: SignalsConfig {
                momentum_windows: vec![5, 20],
                atr_period: 14,
                ma_filter_window: 120,
                flow_ewma_halflife: 10,
                unlock_lookahead_days: 30,
            },
            reporting: ReportingConfig {
                anthropic_model: String::new(),
                anthropic_temperature: 0.0,
                max_tokens: 0,
                recipients: Vec::new(),
            },
            r#macro: MacroConfig::default(),
            enrichment: EnrichmentConfig::default(),
        }
    }

    fn seed_shadow_fixture(db: &Connection, as_of: NaiveDate) -> Result<()> {
        storage::init_schema(db)?;
        db.execute(
            "INSERT INTO macro_cn (date, series_id, series_name, value)
             VALUES (?, 'SHIBOR_1M', 'Shibor 1M', 1.5)",
            duckdb::params![as_of.to_string()],
        )?;
        for (code, name, industry, market) in [
            ("000001.SZ", "平安银行", "银行", "主板"),
            ("600000.SH", "浦发银行", "银行", "主板"),
        ] {
            db.execute(
                "INSERT INTO stock_basic
                 (ts_code, symbol, name, area, industry, market, list_date, list_status)
                 VALUES (?, ?, ?, 'CN', ?, ?, '20000101', 'L')",
                duckdb::params![
                    code,
                    code.split('.').next().unwrap_or(code),
                    name,
                    industry,
                    market
                ],
            )?;
        }
        for index_code in ["000016.SH", "000300.SH", "000905.SH", "399006.SZ"] {
            for symbol in ["000001.SZ", "600000.SH"] {
                db.execute(
                    "INSERT INTO index_weight (index_code, con_code, trade_date, weight)
                     VALUES (?, ?, ?, 50.0)",
                    duckdb::params![index_code, symbol, as_of.to_string()],
                )?;
            }
        }
        for i in 0..70 {
            let d = as_of - chrono::Duration::days(69 - i);
            let wave = (i as f64 / 5.0).sin();
            for (idx, code) in ["000016.SH", "000300.SH", "000905.SH", "399006.SZ"]
                .iter()
                .enumerate()
            {
                let close = 1000.0 + i as f64 * (1.0 + idx as f64 * 0.1) + wave * 3.0;
                let pre_close = close - 1.0;
                db.execute(
                    "INSERT INTO prices
                     (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount, adj_factor)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 100000, 1000000, 1.0)",
                    duckdb::params![
                        *code,
                        d.to_string(),
                        close - 0.4,
                        close + 1.0,
                        close - 1.0,
                        close,
                        pre_close,
                        close - pre_close,
                        (close / pre_close - 1.0) * 100.0,
                    ],
                )?;
            }
            for (idx, code) in ["000001.SZ", "600000.SH"].iter().enumerate() {
                let close = 10.0 + i as f64 * 0.03 + wave * 0.05 + idx as f64;
                let pre_close = close - 0.02;
                db.execute(
                    "INSERT INTO prices
                     (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount, adj_factor)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 100000, 1000000, 1.0)",
                    duckdb::params![
                        *code,
                        d.to_string(),
                        close - 0.03,
                        close + 0.12,
                        close - 0.10,
                        close,
                        pre_close,
                        close - pre_close,
                        (close / pre_close - 1.0) * 100.0,
                    ],
                )?;
            }
        }
        for (maturity, tenor_scale) in [
            (as_of + chrono::Duration::days(60), 1.0),
            (as_of + chrono::Duration::days(120), 1.35),
        ] {
            for (strike, call_settle, put_settle) in [
                (90.0, 12.0 * tenor_scale, 1.0 * tenor_scale),
                (100.0, 5.0 * tenor_scale, 5.0 * tenor_scale),
                (110.0, 1.0 * tenor_scale, 12.0 * tenor_scale),
            ] {
                for (call_put, settle) in [("C", call_settle), ("P", put_settle)] {
                    let ts_code = format!(
                        "MOCK{}{}{}",
                        maturity.format("%m%d"),
                        call_put,
                        strike as i32
                    );
                    db.execute(
                        "INSERT INTO opt_basic
                         (ts_code, name, call_put, exercise_price, maturity_date, list_date, delist_date, opt_code, per_unit, exercise_type)
                         VALUES (?, 'mock', ?, ?, ?, ?, ?, 'OP510300MOCK', 10000, 'E')",
                        duckdb::params![
                            ts_code,
                            call_put,
                            strike,
                            maturity.to_string(),
                            (as_of - chrono::Duration::days(30)).to_string(),
                            maturity.to_string(),
                        ],
                    )?;
                    db.execute(
                        "INSERT INTO opt_daily
                         (ts_code, trade_date, exchange, pre_settle, pre_close, open, high, low, close, settle, vol, amount, oi)
                         VALUES (?, ?, 'SSE', ?, ?, ?, ?, ?, ?, ?, 1000, 1000000, 1000)",
                        duckdb::params![
                            ts_code,
                            as_of.to_string(),
                            settle,
                            settle,
                            settle,
                            settle,
                            settle,
                            settle,
                            settle,
                        ],
                    )?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn shadow_option_pipeline_runs_on_small_fixture() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let cfg = test_settings();
        let as_of = NaiveDate::from_ymd_opt(2026, 4, 29).unwrap();
        seed_shadow_fixture(&db, as_of)?;

        let curves = compute_market_curves(&db, as_of)?;
        assert!(!curves.is_empty(), "expected fixture option curve");

        let features = load_stock_features(&db, as_of)?;
        assert_eq!(features.len(), 2);

        let fast = compute_fast(&db, &cfg, as_of)?;
        assert_eq!(fast, 2);

        let shortlist = vec!["000001.SZ".to_string()];
        let full = enrich_symbols_full(&db, as_of, &shortlist)?;
        assert_eq!(full, 1);

        let loaded = load_full_metrics(&db, as_of, &shortlist);
        assert!(loaded
            .values()
            .any(|metrics| metrics.put_90_3m.unwrap_or(0.0) > 0.0));
        Ok(())
    }

    #[test]
    #[ignore = "integration smoke uses local full DuckDB snapshot and is too slow for default cargo test"]
    fn smoke_shadow_option_pipeline() {
        let cfg = Settings::load("config.yaml").expect("config");
        let as_of = NaiveDate::from_ymd_opt(2026, 4, 14).unwrap();
        let smoke_db = std::env::temp_dir().join(format!(
            "quant_cn_shadow_smoke_{}.duckdb",
            std::process::id()
        ));
        let smoke_db_wal = smoke_db.with_extension("duckdb.wal");
        let _ = std::fs::remove_file(&smoke_db);
        let _ = std::fs::remove_file(&smoke_db_wal);
        let smoke_db_str = smoke_db.to_str().expect("utf-8 smoke db path");
        storage::copy_database(cfg.data.raw_path(), smoke_db_str).expect("stage smoke research");
        let db = storage::open(smoke_db_str).expect("db");

        let curves = compute_market_curves(&db, as_of).expect("curves");
        assert!(!curves.is_empty(), "expected non-empty option curves");

        let features = load_stock_features(&db, as_of).expect("features");
        assert!(
            features.len() > 1000,
            "expected stock features, got {}",
            features.len()
        );

        let fast = compute_fast(&db, &cfg, as_of).expect("shadow_fast");
        assert!(fast > 1000, "expected broad cross-section, got {}", fast);

        let fast_rows: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM analytics
                 WHERE as_of = ? AND module = ? AND metric = 'shadow_iv_30d'",
                duckdb::params![as_of.to_string(), MODULE_FAST],
                |row| row.get::<_, i64>(0),
            )
            .expect("count fast rows");
        assert!(
            fast_rows > 1000,
            "expected shadow_fast rows, got {}",
            fast_rows
        );

        let shortlist = vec![
            "603601.SH".to_string(),
            "000001.SZ".to_string(),
            "300750.SZ".to_string(),
        ];
        let full = enrich_symbols_full(&db, as_of, &shortlist).expect("shadow_full");
        assert!(full >= 1, "expected shortlist pricing, got {}", full);

        let loaded = load_full_metrics(&db, as_of, &shortlist);
        assert!(
            loaded.values().any(|m| m.put_90_3m.unwrap_or(0.0) > 0.0),
            "expected non-empty shadow_full metrics"
        );
        drop(db);
        let _ = std::fs::remove_file(smoke_db);
        let _ = std::fs::remove_file(smoke_db_wal);
    }
}
