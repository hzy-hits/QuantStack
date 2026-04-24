/// Flow analytics — information_score (6-component composite).
///
/// Compensates for missing single-stock options data in A-shares by combining
/// six orthogonal flow/positioning signals into a single [0,1] score.
///
/// information_score = percentile_rank(adaptive_weighted_sum(
///   0.32 * large_flow       — institutional moneyflow imbalance
///   0.23 * margin           — leverage positioning delta
///   0.15 * block            — block-trade premium-weighted flow
///   0.10 * insider          — insider trades + repurchases
///   0.08 * market_vol       — iVIX-like option activity * beta
///   0.12 * tape             — realized price/volume abnormality
/// ) * event_clock_multiplier)
///
/// Removed: northbound (Tushare data all NULL), hot/龙虎榜 (needs >2000 credits).
///
/// Each component is z-scored cross-sectionally (z20 = 20-day window,
/// z60 = 60-day window), clamped to [-3, 3]. Missing values default to 0.
use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::{info, warn};

use crate::config::Settings;

// ── Component weights (6 active components) ─────────────────────────────────
// Removed: northbound_z (Tushare northbound_flow returns all NULL),
//          hot_z (top_list/龙虎榜 needs >2000 Tushare credits, always empty).
// event_clock is a MULTIPLIER, not a weighted component.
const W_LARGE_FLOW: f64 = 0.32; // institutional moneyflow imbalance (81% coverage)
const W_MARGIN: f64 = 0.23; // leverage positioning delta (59% coverage)
const W_BLOCK: f64 = 0.15; // block-trade premium-weighted flow (sparse but high signal)
const W_INSIDER: f64 = 0.10; // shareholder trades + repurchases
const W_MARKET_VOL: f64 = 0.08; // iVIX-like option activity * beta
const W_TAPE: f64 = 0.12; // realized price/volume abnormality (86% coverage)

// Margin
const MG_W_RZYE: f64 = 0.50;
const MG_W_NETFLOW: f64 = 0.35;
const MG_W_RQYE: f64 = 0.15;

// Insider
const INS_W_HOLDER: f64 = 0.60;
const INS_W_REPO: f64 = 0.40;

// Event clock
const EVT_W_DISCLOSURE: f64 = 0.50;
const EVT_W_FORECAST: f64 = 0.30;
const EVT_W_UNLOCK: f64 = 0.20;

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Z-score with clamp to [-3, 3].
pub fn zscore_clamped(value: f64, mean: f64, std: f64) -> f64 {
    if std < 1e-10 {
        return 0.0;
    }
    ((value - mean) / std).clamp(-3.0, 3.0)
}

/// EWMA (exponentially weighted moving average).
pub fn ewma(values: &[f64], halflife: f64) -> Vec<f64> {
    let alpha = 1.0 - (0.5f64).powf(1.0 / halflife);
    let mut result = Vec::with_capacity(values.len());
    let mut ema = values.first().copied().unwrap_or(0.0);
    for &v in values {
        ema = alpha * v + (1.0 - alpha) * ema;
        result.push(ema);
    }
    result
}

/// Compute cross-sectional mean and std from a map of raw values.
fn cross_sectional_stats(vals: &HashMap<String, f64>) -> (f64, f64) {
    if vals.is_empty() {
        return (0.0, 1.0);
    }
    let n = vals.len() as f64;
    let mean = vals.values().sum::<f64>() / n;
    let var = vals.values().map(|v| (v - mean).powi(2)).sum::<f64>() / n.max(1.0);
    let std = var.sqrt();
    (mean, if std < 1e-10 { 1.0 } else { std })
}

/// Cross-sectionally z-score a map in place, returning the z-scores.
fn zscore_map(raw: &HashMap<String, f64>) -> HashMap<String, f64> {
    let (mean, std) = cross_sectional_stats(raw);
    raw.iter()
        .map(|(k, &v)| (k.clone(), zscore_clamped(v, mean, std)))
        .collect()
}

/// Approximate inverse standard normal (probit) for p in (0, 1).
/// Abramowitz & Stegun formula 26.2.23, accuracy ~4.5e-4.
fn probit(p: f64) -> f64 {
    if p <= 0.0 {
        return -3.0;
    }
    if p >= 1.0 {
        return 3.0;
    }
    if p < 0.5 {
        return -probit(1.0 - p);
    }
    let t = (-2.0 * (1.0 - p).ln()).sqrt();
    t - (2.515517 + 0.802853 * t + 0.010328 * t * t)
        / (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t)
}

/// Percentile-rank a map of values, returning probit-transformed scores
/// in [-3, 3]. Uses inverse-normal (probit) transform for scale
/// compatibility with other z-scored components (E|x| ≈ 0.8).
/// Ties get the same (averaged) rank to ensure deterministic output.
fn percentile_rank_map(raw: &HashMap<String, f64>) -> HashMap<String, f64> {
    if raw.is_empty() {
        return HashMap::new();
    }
    let mut entries: Vec<(String, f64)> = raw.iter().map(|(k, &v)| (k.clone(), v)).collect();
    entries.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let n = entries.len() as f64;

    // Compute averaged ranks for ties, then probit-transform
    let mut result = HashMap::with_capacity(entries.len());
    let mut i = 0;
    while i < entries.len() {
        // Find the run of entries with the same value
        let mut j = i + 1;
        while j < entries.len() && (entries[j].1 - entries[i].1).abs() < 1e-12 {
            j += 1;
        }
        // Average rank for this tie group: mean of positions [i..j)
        let avg_rank = (i + j - 1) as f64 / 2.0;
        let pct = (avg_rank + 0.5) / n;
        let z = probit(pct).clamp(-3.0, 3.0);
        for entry in &entries[i..j] {
            result.insert(entry.0.clone(), z);
        }
        i = j;
    }
    result
}

// ═════════════════════════════════════════════════════════════════════════════
// Main entry point
// ═════════════════════════════════════════════════════════════════════════════

pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let _halflife = cfg.signals.flow_ewma_halflife;

    // 1. Large-order flow from moneyflow
    let large_flow_z = compute_large_flow(db, as_of)?;
    info!(symbols = large_flow_z.len(), "large_flow component done");

    // 2. Margin (融资融券)
    let margin_z = compute_margin(db, as_of)?;
    info!(symbols = margin_z.len(), "margin component done");

    // 3. Block trade premium flow
    let block_z = compute_block(db, as_of)?;
    info!(symbols = block_z.len(), "block component done");

    // 4. Insider (shareholder trades + repurchases)
    let insider_z = compute_insider(db, as_of)?;
    info!(symbols = insider_z.len(), "insider component done");

    // 7. Event clock (disclosure proximity, recent forecast, upcoming unlock)
    let event_raw = compute_event_clock(db, as_of)?;
    info!(symbols = event_raw.len(), "event_clock component done");

    // 8. Market-level option activity * beta
    let mkt_vol_z = compute_market_vol(db, as_of)?;
    info!(symbols = mkt_vol_z.len(), "market_vol component done");

    // 9. Tape abnormality (price/volume — available for ALL stocks)
    let tape_z = compute_tape(db, as_of)?;
    info!(symbols = tape_z.len(), "tape component done");

    // ── Collect all symbols that appear in any WEIGHTED component ────────────
    // event_raw is a multiplier only — symbols appearing ONLY in event_raw
    // should NOT enter the composite (they'd get raw_composite=0 + arbitrary percentile).
    let mut all_symbols: std::collections::HashSet<String> = std::collections::HashSet::new();
    for map in [
        &large_flow_z,
        &margin_z,
        &block_z,
        &insider_z,
        &mkt_vol_z,
        &tape_z,
    ] {
        all_symbols.extend(map.keys().cloned());
    }

    if all_symbols.is_empty() {
        warn!(
            "flow::compute — no symbols with any flow data for {}",
            as_of
        );
        return Ok(0);
    }

    // ── Adaptive-weight composite + percentile rank ──────────────────────────
    let date_str = as_of.to_string();

    // Clean previous flow analytics for this date
    db.execute(
        "DELETE FROM analytics WHERE as_of = ? AND module = 'flow'",
        [&date_str],
    )?;

    // Phase 1: compute adaptive-weighted raw composite for each symbol
    // Components with data get proportionally more weight; dead components contribute nothing
    struct SymScore {
        sym: String,
        raw_composite: f64,
        active_count: u8,
        lf: f64,
        tp: f64,
        mg: f64,
        bl: f64,
        ins: f64,
        ev: f64,
        mv: f64,
    }

    // event_clock is now a multiplier, not in this list

    let mut scored: Vec<SymScore> = Vec::with_capacity(all_symbols.len());

    for sym in &all_symbols {
        let lf = *large_flow_z.get(sym).unwrap_or(&0.0);
        let tp = *tape_z.get(sym).unwrap_or(&0.0);
        let mg = *margin_z.get(sym).unwrap_or(&0.0);
        let bl = *block_z.get(sym).unwrap_or(&0.0);
        let ins = *insider_z.get(sym).unwrap_or(&0.0);
        let ev = *event_raw.get(sym).unwrap_or(&0.0);
        let mv = *mkt_vol_z.get(sym).unwrap_or(&0.0);

        // Adaptive weighting: only include components where the stock has data
        // event_clock is a MULTIPLIER, not a weighted component
        let mut components: Vec<(f64, f64)> = Vec::new(); // (weight, |z|)
        if large_flow_z.contains_key(sym) {
            components.push((W_LARGE_FLOW, lf.abs()));
        }
        if margin_z.contains_key(sym) {
            components.push((W_MARGIN, mg.abs()));
        }
        if block_z.contains_key(sym) {
            components.push((W_BLOCK, bl.abs()));
        }
        if insider_z.contains_key(sym) {
            components.push((W_INSIDER, ins.abs()));
        }
        if mkt_vol_z.contains_key(sym) {
            components.push((W_MARKET_VOL, mv.abs()));
        }
        if tape_z.contains_key(sym) {
            components.push((W_TAPE, tp.abs()));
        }

        let active_count = components.len() as u8;
        let total_weight: f64 = components.iter().map(|(w, _)| w).sum();

        let raw_composite = if total_weight > 1e-10 {
            let normalized: f64 = components.iter().map(|(w, z)| (w / total_weight) * z).sum();
            // Coverage penalty: fewer active components → less reliable
            let coverage_factor = match active_count {
                0 => 0.0,
                1 => 0.5,
                2 => 0.7,
                3 => 0.85,
                _ => 1.0,
            };
            // Event clock as multiplier: stocks near events get amplified
            // ev is [0, 1], so multiplier is [1.0, 1.35]
            let event_mult = 1.0 + 0.35 * ev;
            normalized * coverage_factor * event_mult
        } else {
            0.0
        };

        scored.push(SymScore {
            sym: sym.clone(),
            raw_composite,
            active_count,
            lf,
            tp,
            mg,
            bl,
            ins,
            ev,
            mv,
        });
    }

    // Phase 2: percentile rank → information_score in [0, 1]
    // Sort by raw_composite ascending, assign percentile
    scored.sort_by(|a, b| {
        a.raw_composite
            .partial_cmp(&b.raw_composite)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let n_total = scored.len() as f64;

    let mut insert = db.prepare(
        "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, 'flow', ?, ?, ?)",
    )?;

    let mut n_rows: usize = 0;

    for (rank, s) in scored.iter().enumerate() {
        let percentile = (rank as f64 + 0.5) / n_total; // uniform [0, 1]

        let detail = format!(
            r#"{{"active":{}, "raw":{:.4}}}"#,
            s.active_count, s.raw_composite
        );

        // Write percentile-ranked information_score
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "information_score",
            percentile,
            &detail
        ])?;
        n_rows += 1;

        // Write component z-scores (no detail needed)
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "large_flow_z",
            s.lf,
            None::<String>
        ])?;
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "tape_z",
            s.tp,
            None::<String>
        ])?;
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "margin_z",
            s.mg,
            None::<String>
        ])?;
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "block_z",
            s.bl,
            None::<String>
        ])?;
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "insider_z",
            s.ins,
            None::<String>
        ])?;
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "event_clock",
            s.ev,
            None::<String>
        ])?;
        insert.execute(duckdb::params![
            &s.sym,
            &date_str,
            "market_vol_z",
            s.mv,
            None::<String>
        ])?;
        n_rows += 7;
    }

    info!(
        symbols = all_symbols.len(),
        rows = n_rows,
        "information_score complete"
    );
    Ok(n_rows)
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 1: Large-order flow (moneyflow)
// ═════════════════════════════════════════════════════════════════════════════

/// |z20( (buy_elg+buy_lg - sell_elg-sell_lg) / total_amount )|
///
/// Query the latest day's moneyflow and compute the large-order imbalance
/// ratio, then z-score cross-sectionally using the last 20 days of data.
fn compute_large_flow(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let lookback_start = (as_of - chrono::Duration::days(30)).to_string(); // ~20 trading days

    // Get the per-symbol large imbalance ratio for each day in the window
    let mut stmt = db.prepare(
        "SELECT ts_code, CAST(trade_date AS VARCHAR) AS trade_date,
                COALESCE(buy_elg_amount, 0) + COALESCE(buy_lg_amount, 0)
              - COALESCE(sell_elg_amount, 0) - COALESCE(sell_lg_amount, 0)
                AS large_net,
                COALESCE(buy_sm_amount, 0) + COALESCE(sell_sm_amount, 0)
              + COALESCE(buy_md_amount, 0) + COALESCE(sell_md_amount, 0)
              + COALESCE(buy_lg_amount, 0) + COALESCE(sell_lg_amount, 0)
              + COALESCE(buy_elg_amount, 0) + COALESCE(sell_elg_amount, 0)
                AS total_amount
         FROM moneyflow
         WHERE trade_date >= ? AND trade_date <= ?
         ORDER BY ts_code, trade_date",
    )?;

    // Collect per-symbol time series of imbalance ratios
    let mut series: HashMap<String, Vec<(String, f64)>> = HashMap::new();

    let rows = stmt.query_map([&lookback_start, &date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, f64>(3)?,
        ))
    })?;

    for r in rows {
        let (code, date, large_net, total) = r?;
        let ratio = if total.abs() < 1e-6 {
            0.0
        } else {
            large_net / total
        };
        series.entry(code).or_default().push((date, ratio));
    }

    // For each symbol, compute 20-day mean/std of its own time series,
    // then z-score the latest value. Minimum 5 observations required
    // for reliable z-scoring; with fewer, use raw ratio directly.
    let mut raw_z: HashMap<String, f64> = HashMap::new();

    for (code, ts) in &series {
        if ts.is_empty() {
            continue;
        }
        let latest_val = ts.last().unwrap().1;
        if ts.len() < 5 {
            // Insufficient history for z-score — use raw ratio directly
            raw_z.insert(code.clone(), latest_val.clamp(-3.0, 3.0));
        } else {
            let n = ts.len() as f64;
            let mean = ts.iter().map(|(_, v)| v).sum::<f64>() / n;
            let var = ts.iter().map(|(_, v)| (v - mean).powi(2)).sum::<f64>() / n.max(1.0);
            let std = var.sqrt();
            raw_z.insert(code.clone(), zscore_clamped(latest_val, mean, std));
        }
    }

    // Percentile rank instead of cross-sectional z-score.
    // Large-flow data is bimodal (most stocks either net-buy or net-sell),
    // so zscore_map degenerates to binary ±1.11. Percentile ranking
    // produces a uniform distribution with better signal differentiation.
    Ok(percentile_rank_map(&raw_z))
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 3: Margin (融资融券)
// ═════════════════════════════════════════════════════════════════════════════

/// |0.50*z20(delta5 rzye/circ_mv) + 0.35*z20((rzmre-rzche)/circ_mv)
///  + 0.15*z20(delta5 rqye/circ_mv)|
fn compute_margin(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let lookback = (as_of - chrono::Duration::days(30)).to_string();

    // Part A: 5-day delta of rzye / circ_mv
    let mut stmt_a = db.prepare(
        "WITH ranked AS (
            SELECT m.ts_code, m.trade_date,
                   m.rzye / NULLIF(d.circ_mv, 0) AS rzye_ratio,
                   ROW_NUMBER() OVER (PARTITION BY m.ts_code ORDER BY m.trade_date DESC) AS rn
            FROM margin_detail m
            JOIN daily_basic d ON m.ts_code = d.ts_code AND m.trade_date = d.trade_date
            WHERE m.trade_date >= ? AND m.trade_date <= ?
        )
        SELECT a.ts_code,
               COALESCE(a.rzye_ratio, 0) - COALESCE(b.rzye_ratio, 0) AS delta5
        FROM ranked a
        LEFT JOIN ranked b ON a.ts_code = b.ts_code AND b.rn = 6
        WHERE a.rn = 1",
    )?;

    let mut rzye_delta: HashMap<String, f64> = HashMap::new();
    for r in stmt_a.query_map([&lookback, &date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })? {
        let (code, d) = r?;
        rzye_delta.insert(code, d);
    }

    // Part B: (rzmre - rzche) / circ_mv — net margin inflow today
    let mut stmt_b = db.prepare(
        "SELECT m.ts_code,
                (COALESCE(m.rzmre, 0) - COALESCE(m.rzche, 0)) / NULLIF(d.circ_mv, 0) AS net_flow
         FROM margin_detail m
         JOIN daily_basic d ON m.ts_code = d.ts_code AND m.trade_date = d.trade_date
         WHERE m.trade_date = (
             SELECT MAX(trade_date) FROM margin_detail WHERE trade_date <= ?
         )",
    )?;

    let mut margin_net: HashMap<String, f64> = HashMap::new();
    for r in stmt_b.query_map([&date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
    })? {
        let (code, v) = r?;
        margin_net.insert(code, v.unwrap_or(0.0));
    }

    // Part C: 5-day delta of rqye / circ_mv (short-selling balance)
    let mut stmt_c = db.prepare(
        "WITH ranked AS (
            SELECT m.ts_code, m.trade_date,
                   COALESCE(m.rqye, 0) / NULLIF(d.circ_mv, 0) AS rqye_ratio,
                   ROW_NUMBER() OVER (PARTITION BY m.ts_code ORDER BY m.trade_date DESC) AS rn
            FROM margin_detail m
            JOIN daily_basic d ON m.ts_code = d.ts_code AND m.trade_date = d.trade_date
            WHERE m.trade_date >= ? AND m.trade_date <= ?
        )
        SELECT a.ts_code,
               COALESCE(a.rqye_ratio, 0) - COALESCE(b.rqye_ratio, 0) AS delta5
        FROM ranked a
        LEFT JOIN ranked b ON a.ts_code = b.ts_code AND b.rn = 6
        WHERE a.rn = 1",
    )?;

    let mut rqye_delta: HashMap<String, f64> = HashMap::new();
    for r in stmt_c.query_map([&lookback, &date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })? {
        let (code, d) = r?;
        rqye_delta.insert(code, d);
    }

    // Z-score each
    let rzye_z = zscore_map(&rzye_delta);
    let net_z = zscore_map(&margin_net);
    let rqye_z = zscore_map(&rqye_delta);

    // Combine
    let mut all_syms: std::collections::HashSet<String> = std::collections::HashSet::new();
    all_syms.extend(rzye_z.keys().cloned());
    all_syms.extend(net_z.keys().cloned());
    all_syms.extend(rqye_z.keys().cloned());

    let mut result: HashMap<String, f64> = HashMap::new();
    for sym in all_syms {
        let a = rzye_z.get(&sym).copied().unwrap_or(0.0);
        let b = net_z.get(&sym).copied().unwrap_or(0.0);
        let c = rqye_z.get(&sym).copied().unwrap_or(0.0);
        result.insert(sym, MG_W_RZYE * a + MG_W_NETFLOW * b + MG_W_RQYE * c);
    }

    Ok(result)
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 4: Block trade premium-weighted flow
// ═════════════════════════════════════════════════════════════════════════════

/// |z20( sum_5d(block_trade.amount * (block_trade.price/prices.close - 1)) / circ_mv )|
fn compute_block(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let lookback_5d = (as_of - chrono::Duration::days(10)).to_string(); // ~5 trading days
    let lookback_30d = (as_of - chrono::Duration::days(45)).to_string(); // ~20 trading days for z

    // Sum over last 5 trading days: amount * (block_price / close_price - 1) / circ_mv
    // This captures premium/discount-weighted conviction
    let mut stmt = db.prepare(
        "WITH block_prem AS (
            SELECT bt.ts_code, bt.trade_date,
                   SUM(bt.amount * (bt.price / NULLIF(p.close, 0) - 1.0)) AS prem_flow
            FROM block_trade bt
            JOIN prices p ON bt.ts_code = p.ts_code AND bt.trade_date = p.trade_date
            WHERE bt.trade_date >= ? AND bt.trade_date <= ?
            GROUP BY bt.ts_code, bt.trade_date
        )
        SELECT bp.ts_code,
               SUM(bp.prem_flow) / NULLIF(d.circ_mv, 0) AS ratio
        FROM block_prem bp
        JOIN daily_basic d ON bp.ts_code = d.ts_code
            AND d.trade_date = (SELECT MAX(trade_date) FROM daily_basic
                                WHERE ts_code = bp.ts_code AND trade_date <= ?)
        WHERE bp.trade_date >= ?
        GROUP BY bp.ts_code, d.circ_mv",
    )?;

    let mut raw: HashMap<String, f64> = HashMap::new();
    for r in stmt.query_map([&lookback_30d, &date_str, &date_str, &lookback_5d], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
    })? {
        let (code, v) = r?;
        raw.insert(code, v.unwrap_or(0.0));
    }

    Ok(zscore_map(&raw))
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 4: Insider (shareholder trades + repurchases)
// ═════════════════════════════════════════════════════════════════════════════

/// |0.6*z60(sum_20d stk_holdertrade.change_ratio) + 0.4*z60(repurchase.amount/total_mv)|
fn compute_insider(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let lookback_20d = (as_of - chrono::Duration::days(30)).to_string();
    let lookback_60d = (as_of - chrono::Duration::days(90)).to_string();

    // Part A: sum of change_ratio over last 20 days from stk_holdertrade
    // Positive change_ratio = buying (增持), negative = selling (减持)
    let mut stmt_a = db.prepare(
        "SELECT ts_code,
                SUM(COALESCE(change_ratio, 0)) AS total_change
         FROM stk_holdertrade
         WHERE ann_date >= ? AND ann_date <= ?
         GROUP BY ts_code",
    )?;

    let mut holder_raw: HashMap<String, f64> = HashMap::new();
    for r in stmt_a.query_map([&lookback_20d, &date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })? {
        let (code, v) = r?;
        holder_raw.insert(code, v);
    }

    // Part B: repurchase.amount / total_mv — announced in last 60 days
    let mut stmt_b = db.prepare(
        "SELECT r.ts_code,
                SUM(COALESCE(r.amount, 0)) / NULLIF(d.total_mv, 0) AS repo_ratio
         FROM repurchase r
         JOIN daily_basic d ON r.ts_code = d.ts_code
             AND d.trade_date = (SELECT MAX(trade_date) FROM daily_basic
                                 WHERE ts_code = r.ts_code AND trade_date <= ?)
         WHERE r.ann_date >= ? AND r.ann_date <= ?
         GROUP BY r.ts_code, d.total_mv",
    )?;

    let mut repo_raw: HashMap<String, f64> = HashMap::new();
    for r in stmt_b.query_map([&date_str, &lookback_60d, &date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
    })? {
        let (code, v) = r?;
        repo_raw.insert(code, v.unwrap_or(0.0));
    }

    // Z-score each (using wider population — these are sparse signals)
    let holder_z = zscore_map(&holder_raw);
    let repo_z = zscore_map(&repo_raw);

    let mut all_syms: std::collections::HashSet<String> = std::collections::HashSet::new();
    all_syms.extend(holder_z.keys().cloned());
    all_syms.extend(repo_z.keys().cloned());

    let mut result: HashMap<String, f64> = HashMap::new();
    for sym in all_syms {
        let h = holder_z.get(&sym).copied().unwrap_or(0.0);
        let r = repo_z.get(&sym).copied().unwrap_or(0.0);
        result.insert(sym, INS_W_HOLDER * h + INS_W_REPO * r);
    }

    Ok(result)
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 7: Event clock (indicator-based, not z-scored)
// ═════════════════════════════════════════════════════════════════════════════

/// 0.5*1(days_to_disclosure<=5) + 0.3*1(forecast in last 10d) + 0.2*1(unlock<=5d & ratio>5%)
///
/// Returns raw [0, 1] values — NOT z-scored (binary indicators).
fn compute_event_clock(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let future_5d = (as_of + chrono::Duration::days(5)).to_string();
    let past_10d = (as_of - chrono::Duration::days(10)).to_string();

    let mut result: HashMap<String, f64> = HashMap::new();

    // Part A: disclosure_date — upcoming within 5 calendar days
    // Use actual_date or pre_date for the expected disclosure date
    let mut stmt_a = db.prepare(
        "SELECT DISTINCT ts_code
         FROM disclosure_date
         WHERE (actual_date >= ? AND actual_date <= ?)
            OR (actual_date IS NULL AND pre_date >= ? AND pre_date <= ?)",
    )?;
    for r in stmt_a.query_map([&date_str, &future_5d, &date_str, &future_5d], |row| {
        Ok(row.get::<_, String>(0)?)
    })? {
        let code = r?;
        *result.entry(code).or_insert(0.0) += EVT_W_DISCLOSURE;
    }

    // Part B: forecast announced in last 10 days
    let mut stmt_b = db.prepare(
        "SELECT DISTINCT ts_code
         FROM forecast
         WHERE ann_date >= ? AND ann_date <= ?",
    )?;
    for r in stmt_b.query_map([&past_10d, &date_str], |row| Ok(row.get::<_, String>(0)?))? {
        let code = r?;
        *result.entry(code).or_insert(0.0) += EVT_W_FORECAST;
    }

    // Part C: share unlock within 5 days with ratio > 5%
    let mut stmt_c = db.prepare(
        "SELECT DISTINCT ts_code
         FROM share_unlock
         WHERE float_date >= ? AND float_date <= ?
           AND COALESCE(float_ratio, 0) > 5.0",
    )?;
    for r in stmt_c.query_map([&date_str, &future_5d], |row| Ok(row.get::<_, String>(0)?))? {
        let code = r?;
        *result.entry(code).or_insert(0.0) += EVT_W_UNLOCK;
    }

    Ok(result)
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 8: Market-level option activity * beta
// ═════════════════════════════════════════════════════════════════════════════

/// Market volatility component: iVIX-like from ETF options + activity proxy.
///
/// Two-layer approach:
///   Layer 1: VIX-style model-free implied variance from opt_daily settle prices
///            + opt_basic strike/expiry (if available with sufficient data)
///   Layer 2: Fallback to activity proxy (vol/oi z-score) when option data sparse
///
/// Result scaled by each stock's beta to CSI 300.
fn compute_market_vol(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let lookback = (as_of - chrono::Duration::days(30)).to_string();

    // ── Layer 1: Try iVIX-like from option chain ─────────────────────────────
    let ivix = compute_ivix(db, &date_str);

    // ── Layer 2: Activity-based z-score (fallback / complement) ──────────────
    let mut stmt_opt = db.prepare(
        "SELECT trade_date,
                SUM(COALESCE(vol, 0)) AS total_vol,
                SUM(COALESCE(oi, 0)) AS total_oi
         FROM opt_daily
         WHERE trade_date >= ? AND trade_date <= ?
         GROUP BY trade_date
         ORDER BY trade_date",
    )?;

    let mut opt_series: Vec<f64> = Vec::new();
    for r in stmt_opt.query_map([&lookback, &date_str], |row| {
        Ok((row.get::<_, f64>(1)?, row.get::<_, f64>(2)?))
    })? {
        let (vol, oi) = r?;
        let activity = if oi > 1e-6 { vol / oi } else { 0.0 };
        opt_series.push(activity);
    }

    if opt_series.is_empty() && ivix.is_none() {
        return Ok(HashMap::new());
    }

    let opt_z = if !opt_series.is_empty() {
        let n = opt_series.len() as f64;
        let opt_mean = opt_series.iter().sum::<f64>() / n;
        let opt_var = opt_series
            .iter()
            .map(|v| (v - opt_mean).powi(2))
            .sum::<f64>()
            / n.max(1.0);
        let opt_std = opt_var.sqrt();
        let latest_opt = *opt_series.last().unwrap();
        zscore_clamped(latest_opt, opt_mean, opt_std)
    } else {
        0.0
    };

    // Blend: if iVIX available, use 0.6*ivix_z + 0.4*activity_z; else pure activity
    // iVIX is already annualized % — normalize to z-like by comparing to typical range
    let blended_z = if let Some(ivix_val) = ivix {
        // Typical A-share iVIX range: 15-35. Center at 22, scale ~5.
        let ivix_z = zscore_clamped(ivix_val, 22.0, 5.0);
        info!(
            ivix = format!("{:.2}", ivix_val),
            ivix_z = format!("{:.2}", ivix_z),
            activity_z = format!("{:.2}", opt_z),
            "iVIX blended"
        );
        0.6 * ivix_z + 0.4 * opt_z
    } else {
        opt_z
    };

    // ── Per-stock beta to benchmark ──────────────────────────────────────────
    // Simple approximation: covariance(stock, index) / var(index) using returns
    // over the 20-day window. Use CSI300 (000300.SH) as benchmark.
    let lookback_beta = (as_of - chrono::Duration::days(40)).to_string(); // a bit extra

    // Get benchmark returns
    let mut stmt_bench = db.prepare(
        "SELECT CAST(trade_date AS VARCHAR) AS trade_date, pct_chg
         FROM prices
         WHERE ts_code = '000300.SH' AND trade_date >= ? AND trade_date <= ?
         ORDER BY trade_date",
    )?;
    let mut bench_returns: Vec<(String, f64)> = Vec::new();
    for r in stmt_bench.query_map([&lookback_beta, &date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
    })? {
        let (date, pct) = r?;
        bench_returns.push((date, pct.unwrap_or(0.0)));
    }

    if bench_returns.len() < 5 {
        // Not enough benchmark data — return empty
        return Ok(HashMap::new());
    }

    let bench_map: HashMap<String, f64> = bench_returns.iter().cloned().collect();
    let bench_vals: Vec<f64> = bench_returns.iter().map(|(_, v)| *v).collect();
    let b_n = bench_vals.len() as f64;
    let b_mean = bench_vals.iter().sum::<f64>() / b_n;
    let b_var = bench_vals.iter().map(|v| (v - b_mean).powi(2)).sum::<f64>() / b_n.max(1.0);

    if b_var < 1e-12 {
        return Ok(HashMap::new());
    }

    // Get stock returns for the same window
    let mut stmt_stk = db.prepare(
        "SELECT ts_code, CAST(trade_date AS VARCHAR) AS trade_date, pct_chg
         FROM prices
         WHERE trade_date >= ? AND trade_date <= ?
           AND ts_code != '000300.SH'
         ORDER BY ts_code, trade_date",
    )?;

    let mut stk_returns: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for r in stmt_stk.query_map([&lookback_beta, &date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<f64>>(2)?,
        ))
    })? {
        let (code, date, pct) = r?;
        stk_returns
            .entry(code)
            .or_default()
            .push((date, pct.unwrap_or(0.0)));
    }

    // Compute beta for each stock, then scale by opt_z
    let mut result: HashMap<String, f64> = HashMap::new();
    for (code, returns) in &stk_returns {
        // Match dates with benchmark
        let mut pairs: Vec<(f64, f64)> = Vec::new();
        for (date, stk_ret) in returns {
            if let Some(&bench_ret) = bench_map.get(date) {
                pairs.push((*stk_ret, bench_ret));
            }
        }

        if pairs.len() < 5 {
            continue;
        }

        let pn = pairs.len() as f64;
        let s_mean = pairs.iter().map(|(s, _)| s).sum::<f64>() / pn;
        let b_mean_local = pairs.iter().map(|(_, b)| b).sum::<f64>() / pn;
        let cov = pairs
            .iter()
            .map(|(s, b)| (s - s_mean) * (b - b_mean_local))
            .sum::<f64>()
            / pn;
        let var_b = pairs
            .iter()
            .map(|(_, b)| (b - b_mean_local).powi(2))
            .sum::<f64>()
            / pn;

        let beta = if var_b > 1e-12 {
            (cov / var_b).clamp(0.0, 3.0)
        } else {
            1.0
        };
        // Clamp directly — do NOT z-score, because blended_z is a scalar
        // that would be normalized away. beta*blended_z preserves market stress level.
        result.insert(code.clone(), (beta * blended_z).clamp(-3.0, 3.0));
    }

    Ok(result)
}

// ═════════════════════════════════════════════════════════════════════════════
// iVIX-like: model-free implied variance from ETF option chain
// ═════════════════════════════════════════════════════════════════════════════

/// Compute a VIX-style model-free implied vol from 300ETF options.
/// Uses settle prices from opt_daily + strike/expiry from opt_basic.
///
/// Returns annualized implied vol (%) or None if data insufficient.
fn compute_ivix(db: &Connection, date_str: &str) -> Option<f64> {
    // Query: join opt_daily (latest date) with opt_basic for 300ETF family
    let result = db.prepare(
        "SELECT ob.call_put, ob.exercise_price,
                CAST(ob.maturity_date AS VARCHAR) AS maturity_date,
                od.settle, od.oi
         FROM opt_daily od
         JOIN opt_basic ob ON od.ts_code = ob.ts_code
         WHERE od.trade_date = (SELECT MAX(trade_date) FROM opt_daily WHERE trade_date <= ?)
           AND ob.opt_code LIKE 'OP510300%'
           AND ob.exercise_price IS NOT NULL
           AND ob.maturity_date IS NOT NULL
           AND od.settle IS NOT NULL AND od.settle > 0
         ORDER BY ob.maturity_date, ob.exercise_price",
    );

    let mut stmt = match result {
        Ok(s) => s,
        Err(e) => {
            warn!(err = %e, "ivix query failed");
            return None;
        }
    };

    struct OptRow {
        call_put: String,
        strike: f64,
        maturity: String,
        settle: f64,
        oi: f64,
    }

    let rows: Vec<OptRow> = stmt
        .query_map(duckdb::params![date_str], |row| {
            Ok(OptRow {
                call_put: row.get::<_, String>(0)?,
                strike: row.get::<_, f64>(1)?,
                maturity: row.get::<_, String>(2)?,
                settle: row.get::<_, f64>(3)?,
                oi: row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
            })
        })
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    if rows.len() < 6 {
        // Need at least a few strikes on each side
        return None;
    }

    // Group by maturity, find nearest expiry with >= 7 days
    let today = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
    let mut by_maturity: HashMap<String, Vec<&OptRow>> = HashMap::new();
    for r in &rows {
        by_maturity.entry(r.maturity.clone()).or_default().push(r);
    }

    // Find nearest expiry with T >= 7 days and enough strikes
    let mut best_maturity: Option<(String, i64)> = None;
    for (mat_str, opts) in &by_maturity {
        let mat_date = chrono::NaiveDate::parse_from_str(mat_str, "%Y-%m-%d").ok()?;
        let days = (mat_date - today).num_days();
        if days < 7 || opts.len() < 4 {
            continue;
        }
        match &best_maturity {
            None => best_maturity = Some((mat_str.clone(), days)),
            Some((_, best_days)) => {
                if days < *best_days {
                    best_maturity = Some((mat_str.clone(), days));
                }
            }
        }
    }

    let (mat_key, days_to_exp) = best_maturity?;
    let chain = by_maturity.get(&mat_key)?;
    let t_years = days_to_exp as f64 / 365.0;

    if t_years < 0.01 {
        return None;
    }

    // Risk-free rate from Shibor (approximate)
    let r = query_shibor_rate(db, date_str).unwrap_or(1.5) / 100.0;

    // Separate calls and puts, sort by strike
    let mut calls: Vec<(f64, f64, f64)> = Vec::new(); // (strike, settle, oi)
    let mut puts: Vec<(f64, f64, f64)> = Vec::new();

    for opt in chain {
        if opt.call_put == "C" {
            calls.push((opt.strike, opt.settle, opt.oi));
        } else if opt.call_put == "P" {
            puts.push((opt.strike, opt.settle, opt.oi));
        }
    }

    calls.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    puts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    if calls.len() < 2 || puts.len() < 2 {
        return None;
    }

    // Find forward price via put-call parity at the strike where |C-P| is minimized
    let mut min_diff = f64::MAX;
    let mut forward = 0.0;

    for c in &calls {
        for p in &puts {
            if (c.0 - p.0).abs() < 1e-6 {
                // Same strike: F = K + e^(rT) * (C - P)
                let diff = (c.1 - p.1).abs();
                if diff < min_diff {
                    min_diff = diff;
                    forward = c.0 + (r * t_years).exp() * (c.1 - p.1);
                }
            }
        }
    }

    if forward <= 0.0 {
        // Fallback: use average of call strikes weighted by OI as approximate forward
        let total_oi: f64 = calls.iter().map(|c| c.2).sum();
        if total_oi > 0.0 {
            forward = calls.iter().map(|c| c.0 * c.2).sum::<f64>() / total_oi;
        } else {
            forward = calls[calls.len() / 2].0;
        }
    }

    // K0 = largest strike <= F (VIX methodology)
    let mut all_strikes: Vec<f64> = calls
        .iter()
        .map(|c| c.0)
        .chain(puts.iter().map(|p| p.0))
        .collect();
    all_strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
    all_strikes.dedup();

    let k0 = all_strikes
        .iter()
        .filter(|&&k| k <= forward)
        .last()
        .copied()
        .unwrap_or(forward);

    // VIX-style variance: σ²(T) = (2/T) × Σ[ΔK/K² × e^(rT) × Q(K)] - (1/T)(F/K₀ - 1)²
    // Q(K) = put price for K < K₀, call price for K > K₀, average for K = K₀
    let ert = (r * t_years).exp();
    let mut sigma2 = 0.0;

    // Build sorted list of all strikes with their OTM prices
    let mut strike_prices: Vec<(f64, f64)> = Vec::new(); // (K, Q(K))

    for p in &puts {
        if p.0 < k0 {
            strike_prices.push((p.0, p.1));
        }
    }
    // ATM: average of call and put
    if let Some(atm_call) = calls.iter().find(|c| (c.0 - k0).abs() < 1e-6) {
        if let Some(atm_put) = puts.iter().find(|p| (p.0 - k0).abs() < 1e-6) {
            strike_prices.push((k0, (atm_call.1 + atm_put.1) / 2.0));
        }
    }
    for c in &calls {
        if c.0 > k0 {
            strike_prices.push((c.0, c.1));
        }
    }

    strike_prices.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    if strike_prices.len() < 3 {
        return None;
    }

    for i in 0..strike_prices.len() {
        let (k, q) = strike_prices[i];
        // ΔK: half the distance between adjacent strikes
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

    let ivix = sigma2.sqrt() * 100.0; // annualized %

    info!(
        ivix = format!("{:.2}", ivix),
        forward = format!("{:.2}", forward),
        k0 = format!("{:.2}", k0),
        n_strikes = strike_prices.len(),
        days = days_to_exp,
        "iVIX computed from 300ETF options"
    );

    Some(ivix)
}

/// Query latest Shibor 1M rate for risk-free approximation.
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
    .filter_map(|r| r.ok())
    .next()
    .flatten()
}

// ═════════════════════════════════════════════════════════════════════════════
// Component 9: Tape abnormality (price/volume — available for ALL stocks)
// ═════════════════════════════════════════════════════════════════════════════

/// 0.40*z(turnover_spike) + 0.30*z(volume_ratio) + 0.30*z(price_shock)
///
/// This component uses daily_basic and prices data which is available for ALL
/// stocks, providing baseline coverage when sparse flow signals are absent.
fn compute_tape(db: &Connection, as_of: NaiveDate) -> Result<HashMap<String, f64>> {
    let date_str = as_of.to_string();
    let lookback = (as_of - chrono::Duration::days(30)).to_string();

    // ── Sub-signal A: turnover_rate spike (today vs 20d avg) ────────────────
    let mut stmt_tr = db.prepare(
        "WITH tr_data AS (
            SELECT ts_code, trade_date, turnover_rate,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM daily_basic
            WHERE trade_date >= ? AND trade_date <= ?
              AND turnover_rate IS NOT NULL AND turnover_rate > 0
        ),
        tr_avg AS (
            SELECT ts_code,
                   AVG(turnover_rate) AS avg_tr,
                   COUNT(*) AS n
            FROM tr_data
            WHERE rn <= 20
            GROUP BY ts_code
            HAVING COUNT(*) >= 3
        )
        SELECT t.ts_code,
               t.turnover_rate / NULLIF(a.avg_tr, 0) AS turnover_ratio
        FROM tr_data t
        JOIN tr_avg a ON t.ts_code = a.ts_code
        WHERE t.rn = 1",
    )?;

    let mut turnover_raw: HashMap<String, f64> = HashMap::new();
    for r in stmt_tr.query_map([&lookback, &date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
    })? {
        let (code, v) = r?;
        turnover_raw.insert(code, v.unwrap_or(1.0));
    }

    // ── Sub-signal B: volume_ratio (already computed by Tushare) ────────────
    let mut stmt_vr = db.prepare(
        "SELECT ts_code, volume_ratio
         FROM daily_basic
         WHERE trade_date = (SELECT MAX(trade_date) FROM daily_basic WHERE trade_date <= ?)
           AND volume_ratio IS NOT NULL AND volume_ratio > 0",
    )?;

    let mut vol_ratio_raw: HashMap<String, f64> = HashMap::new();
    for r in stmt_vr.query_map([&date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })? {
        let (code, v) = r?;
        vol_ratio_raw.insert(code, v);
    }

    // ── Sub-signal C: price shock (|pct_chg| today) ────────────────────────
    let mut stmt_ps = db.prepare(
        "SELECT ts_code, ABS(COALESCE(pct_chg, 0)) AS abs_chg
         FROM prices
         WHERE trade_date = (SELECT MAX(trade_date) FROM prices WHERE trade_date <= ?)
           AND pct_chg IS NOT NULL",
    )?;

    let mut price_shock_raw: HashMap<String, f64> = HashMap::new();
    for r in stmt_ps.query_map([&date_str], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
    })? {
        let (code, v) = r?;
        price_shock_raw.insert(code, v);
    }

    // ── Z-score each sub-signal cross-sectionally ──────────────────────────
    let tr_z = zscore_map(&turnover_raw);
    let vr_z = zscore_map(&vol_ratio_raw);
    let ps_z = zscore_map(&price_shock_raw);

    // ── Combine ────────────────────────────────────────────────────────────
    let mut all_syms: std::collections::HashSet<String> = std::collections::HashSet::new();
    all_syms.extend(tr_z.keys().cloned());
    all_syms.extend(vr_z.keys().cloned());
    all_syms.extend(ps_z.keys().cloned());

    let mut result: HashMap<String, f64> = HashMap::new();
    for sym in all_syms {
        let t = tr_z.get(&sym).copied().unwrap_or(0.0);
        let v = vr_z.get(&sym).copied().unwrap_or(0.0);
        let p = ps_z.get(&sym).copied().unwrap_or(0.0);
        result.insert(sym, 0.40 * t + 0.30 * v + 0.30 * p);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zscore_clamped() {
        assert!((zscore_clamped(10.0, 5.0, 2.5) - 2.0).abs() < 1e-10);
        // Clamp test
        assert_eq!(zscore_clamped(100.0, 0.0, 1.0), 3.0);
        assert_eq!(zscore_clamped(-100.0, 0.0, 1.0), -3.0);
        // Zero std
        assert_eq!(zscore_clamped(5.0, 5.0, 0.0), 0.0);
    }

    #[test]
    fn test_sigmoid() {
        assert!((sigmoid(0.0) - 0.5).abs() < 1e-10);
        assert!(sigmoid(10.0) > 0.999);
        assert!(sigmoid(-10.0) < 0.001);
    }

    #[test]
    fn test_ewma() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = ewma(&vals, 2.0);
        assert_eq!(result.len(), 5);
        // First value should be close to 1.0
        assert!((result[0] - 1.0).abs() < 0.1);
        // EWMA should trend upward
        for i in 1..result.len() {
            assert!(result[i] > result[i - 1]);
        }
    }

    #[test]
    fn test_cross_sectional_stats() {
        let mut m: HashMap<String, f64> = HashMap::new();
        m.insert("A".to_string(), 1.0);
        m.insert("B".to_string(), 3.0);
        m.insert("C".to_string(), 5.0);
        let (mean, std) = cross_sectional_stats(&m);
        assert!((mean - 3.0).abs() < 1e-10);
        // std = sqrt((4+0+4)/3) = sqrt(8/3) ~ 1.633
        assert!((std - (8.0_f64 / 3.0).sqrt()).abs() < 1e-10);
    }

    #[test]
    fn test_weight_sum() {
        // event_clock is now a multiplier, not a weighted component
        let total = W_LARGE_FLOW + W_MARGIN + W_BLOCK + W_INSIDER + W_MARKET_VOL + W_TAPE;
        assert!(
            (total - 1.0).abs() < 1e-10,
            "weights must sum to 1.0, got {}",
            total
        );
    }
}
