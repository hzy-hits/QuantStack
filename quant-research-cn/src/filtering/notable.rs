/// Two-pass notable item filter — Regime-Adaptive Scoring + Convergence Classification.
///
/// Pass 1: full universe → regime-adaptive composite → top 120 candidates
/// Pass 2: multi-source convergence → classify HIGH / MODERATE / WATCH / LOW
///
/// Three strategy modes based on per-stock regime:
///   Trending     → weight momentum high, reversion low
///   MeanReverting → weight reversion high, momentum low
///   Noisy        → weight breakout high, balanced others
///
/// Classification: convergence-based (like US pipeline)
///   HIGH = 2+ independent signal sources aligned + no conflicts
///   Not a fixed threshold — adapts to available evidence
use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde::Serialize;

use crate::config::Settings;

// ── Fixed weights (regime-independent) ────────────────────────────────────
const W_MAGNITUDE: f64 = 0.10;
const W_INFORMATION: f64 = 0.15;  // flow
const W_EVENT: f64 = 0.12;       // announcements
const W_CROSS_ASSET: f64 = 0.08;
const W_LAB: f64 = 0.35;         // Factor Lab rolling best-factor signal
// Remaining 0.20 is split among momentum/reversion/breakout by regime

// ── Regime-adaptive weights for the 0.20 directional budget ───────────────
// (momentum, reversion, breakout) — must sum to 0.20
const REGIME_WEIGHTS_TRENDING: (f64, f64, f64) = (0.13, 0.01, 0.06);
const REGIME_WEIGHTS_MEAN_REV: (f64, f64, f64) = (0.03, 0.12, 0.05);
const REGIME_WEIGHTS_NOISY: (f64, f64, f64) = (0.06, 0.06, 0.08);

const PASS1_CUTOFF: usize = 120;

#[derive(Debug, Clone, Serialize)]
pub struct NotableItem {
    pub ts_code: String,
    pub name: String,
    pub composite_score: f64,
    pub magnitude_score: f64,
    pub event_score: f64,
    pub momentum_score: f64,
    pub flow_score: f64,
    pub cross_asset_score: f64,
    pub report_bucket: String,
    pub report_reason: String,
    pub signal: Signal,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct Signal {
    pub confidence: String, // HIGH / MODERATE / WATCH / LOW
    pub direction: String,  // bullish / bearish / neutral
    pub horizon: String,    // 5D / 20D
}

/// Internal intermediate representation for scoring.
struct Candidate {
    ts_code: String,
    // Raw metrics from existing modules
    ret_5d: f64,
    ret_20d: f64,
    trend_prob: f64,
    trend_prob_n: f64,
    information_score: f64,
    p_upside: f64,
    surprise_category: f64,
    p_drop: f64,
    unlock_days: f64,
    float_ratio: f64,
    // New: regime + mean_reversion + breakout
    regime: i32,             // 0=Trending, 1=MeanReverting, 2=Noisy, -1=unknown
    reversion_score: f64,
    reversion_direction: f64, // 1=bullish, -1=bearish, 0=neutral
    breakout_score: f64,
    breakout_direction: f64,  // 1=bullish, -1=bearish, 0=none
    rsi_14: f64,
    bb_position: f64,
    lab_factor: f64,         // Factor Lab composite (0 if no promoted factors)
    // Computed scores
    magnitude_score: f64,
    momentum_score: f64,
    reversion_s: f64,
    breakout_s: f64,
    information_s: f64,
    event_score: f64,
    cross_asset_score: f64,
    composite: f64,
}

pub fn build_notable_items(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
) -> Result<Vec<NotableItem>> {
    let date_str = as_of.to_string();
    let max_items = cfg.output.max_notable_items;

    // ── Load macro gate multiplier ──────────────────────────────────────────
    let gate_mult = load_gate_multiplier(db, &date_str);

    // ── Pass 1: Score full universe with regime-adaptive weights ───────────
    let mut candidates = load_candidates(db, &date_str)?;

    if candidates.is_empty() {
        tracing::warn!("no candidates with analytics data for {}", date_str);
        return Ok(Vec::new());
    }

    // Compute magnitude z-scores cross-sectionally
    let ret5d_vals: Vec<f64> = candidates.iter().map(|c| c.ret_5d.abs()).collect();
    let (mag_mean, mag_std) = cross_stats(&ret5d_vals);

    // Compute cross-asset score from sector fund flow
    let sector_flow = load_sector_alignment(db, &date_str);

    for c in &mut candidates {
        // Magnitude: |5D return| z-score → [0, 1]
        c.magnitude_score = zscore_to_score(c.ret_5d.abs(), mag_mean, mag_std);

        // Momentum: deviation from coin flip, weighted by sample size
        let n_weight = (c.trend_prob_n / 20.0).min(1.0);
        c.momentum_score = ((c.trend_prob - 0.5).abs() * 2.0 * n_weight).min(1.0);

        // Mean-reversion: now signed [-1, +1]. Use absolute value for composite weight,
        // sign is used in convergence classification for direction.
        c.reversion_s = c.reversion_score.abs();

        // Breakout: already computed in analytics, just pass through
        c.breakout_s = c.breakout_score;

        // Information: already [0, 1]
        c.information_s = c.information_score;

        // Event: announcement + unlock
        c.event_score = compute_event_score(c);

        // Cross-asset: sector fund flow alignment
        c.cross_asset_score = sector_flow;

        // ── Regime-adaptive weighting ──────────────────────────────────────
        let (w_mom, w_rev, w_brk) = match c.regime {
            0 => REGIME_WEIGHTS_TRENDING,
            1 => REGIME_WEIGHTS_MEAN_REV,
            _ => REGIME_WEIGHTS_NOISY, // includes unknown (-1)
        };

        c.composite = (W_MAGNITUDE * c.magnitude_score
            + W_INFORMATION * c.information_s
            + w_mom * c.momentum_score
            + w_rev * c.reversion_s
            + w_brk * c.breakout_s
            + W_EVENT * c.event_score
            + W_CROSS_ASSET * c.cross_asset_score
            + W_LAB * c.lab_factor.abs().min(1.0))
            * gate_mult;

        c.composite = c.composite.clamp(0.0, 1.0);
    }

    // Sort by composite descending
    candidates.sort_by(|a, b| b.composite.partial_cmp(&a.composite).unwrap());
    candidates.truncate(PASS1_CUTOFF);

    // ── Pass 2: Convergence-based classification ──────────────────────────
    // Instead of a fixed threshold, we count independent signal sources
    for c in &mut candidates {
        let mut confirmation_bonus = 0.0f64;

        // Bonus: information + magnitude convergence
        if c.information_s > 0.5 && c.magnitude_score > 0.5 {
            confirmation_bonus += 0.05;
        }

        // Bonus: strong announcement
        if c.surprise_category >= 0.0 && (c.p_upside > 0.7 || c.p_upside < 0.3) {
            confirmation_bonus += 0.05;
        }

        // Bonus: large unlock imminent
        if c.unlock_days >= 0.0 && c.unlock_days <= 5.0 && c.float_ratio > 5.0 {
            confirmation_bonus += 0.04;
        }

        // Bonus: strong reversion signal (RSI extreme + BB extreme)
        if c.reversion_score > 0.6 {
            confirmation_bonus += 0.04;
        }

        // Bonus: breakout with volume confirmation
        if c.breakout_score > 0.5 {
            confirmation_bonus += 0.04;
        }

        c.composite = (c.composite + confirmation_bonus).min(1.0);
    }

    candidates.sort_by(|a, b| b.composite.partial_cmp(&a.composite).unwrap());
    candidates.truncate(max_items);

    // ── Load stock names ────────────────────────────────────────────────────
    let names = load_stock_names(db);

    // ── Convert to NotableItems with convergence classification ────────────
    let items: Vec<NotableItem> = candidates
        .into_iter()
        .map(move |c| {
            let (confidence, direction) = classify_convergence(&c);
            let (report_bucket, report_reason) =
                classify_report_lane(&c, &confidence, &direction);

            let regime_label = match c.regime {
                0 => "trending",
                1 => "mean_reverting",
                2 => "noisy",
                _ => "unknown",
            };

            NotableItem {
                ts_code: c.ts_code.clone(),
                name: names.get(&c.ts_code).cloned().unwrap_or_default(),
                composite_score: c.composite,
                magnitude_score: c.magnitude_score,
                event_score: c.event_score,
                momentum_score: c.momentum_score,
                flow_score: c.information_s,
                cross_asset_score: c.cross_asset_score,
                report_bucket,
                report_reason,
                signal: Signal {
                    confidence,
                    direction,
                    horizon: "5D".to_string(),
                },
                detail: serde_json::json!({
                    "ret_5d": c.ret_5d,
                    "ret_20d": c.ret_20d,
                    "trend_prob": c.trend_prob,
                    "trend_prob_n": c.trend_prob_n,
                    "information_score": c.information_score,
                    "reversion_score": c.reversion_score,
                    "reversion_direction": c.reversion_direction,
                    "breakout_score": c.breakout_score,
                    "breakout_direction": c.breakout_direction,
                    "rsi_14": c.rsi_14,
                    "bb_position": c.bb_position,
                    "regime": regime_label,
                    "p_upside": if c.surprise_category >= 0.0 { Some(c.p_upside) } else { None::<f64> },
                    "p_drop": if c.unlock_days >= 0.0 { Some(c.p_drop) } else { None::<f64> },
                    "unlock_days": if c.unlock_days >= 0.0 { Some(c.unlock_days) } else { None::<f64> },
                    "float_ratio": if c.unlock_days >= 0.0 { Some(c.float_ratio) } else { None::<f64> },
                    "gate_multiplier": gate_mult,
                }),
            }
        })
        .collect();

    tracing::info!(
        max = max_items,
        pass1 = PASS1_CUTOFF,
        found = items.len(),
        gate = gate_mult,
        "notable items filtered"
    );

    Ok(items)
}

// ── Convergence-based classification ──────────────────────────────────────

/// Count independent signal sources and classify by convergence.
/// Signal sources: momentum, reversion, breakout, flow, event
/// HIGH = 2+ independent sources aligned in same direction + no conflicts
fn classify_convergence(c: &Candidate) -> (String, String) {
    // Identify active directional signals and their directions
    // Each source: (name, direction_sign, strength)
    let mut sources: Vec<(&str, f64, f64)> = Vec::new();

    // 1. Momentum signal (trend_prob deviation)
    if c.momentum_score > 0.15 && c.trend_prob_n > 5.0 {
        let dir = if c.trend_prob > 0.55 { 1.0 } else if c.trend_prob < 0.45 { -1.0 } else { 0.0 };
        if dir != 0.0 {
            sources.push(("momentum", dir, c.momentum_score));
        }
    }

    // 2. Mean-reversion signal
    if c.reversion_score > 0.4 {
        sources.push(("reversion", c.reversion_direction, c.reversion_score));
    }

    // 3. Breakout signal
    if c.breakout_score > 0.35 {
        sources.push(("breakout", c.breakout_direction, c.breakout_score));
    }

    // 4. Flow/information signal (strong institutional conviction)
    // Direction from reversion or breakout direction (NOT ret_5d, to avoid circular dependency)
    if c.information_s > 0.7 {
        // Use reversion_direction or breakout_direction as independent direction hint
        let dir = if c.reversion_direction != 0.0 {
            c.reversion_direction
        } else if c.breakout_direction != 0.0 {
            c.breakout_direction
        } else {
            0.0
        };
        if dir != 0.0 {
            sources.push(("flow", dir, c.information_s));
        }
    }

    // 5. Event signal
    if c.surprise_category >= 0.0 && (c.p_upside - 0.5).abs() > 0.15 {
        let dir = if c.p_upside > 0.55 { 1.0 } else { -1.0 };
        sources.push(("event", dir, (c.p_upside - 0.5).abs() * 2.0));
    }

    // 6. Unlock (bearish only)
    if c.unlock_days >= 0.0 && c.unlock_days <= 10.0 && c.float_ratio > 3.0 {
        sources.push(("unlock", -1.0, c.p_drop.min(1.0)));
    }

    // 7. Factor Lab composite (independent signal from daily factor mining)
    if c.lab_factor.abs() > 0.1 {
        let dir = if c.lab_factor > 0.0 { 1.0 } else { -1.0 };
        sources.push(("lab_factor", dir, c.lab_factor.abs().min(1.0)));
    }

    // Note: magnitude is NOT a source — it's price-derived like momentum,
    // so including it would overcount price-based evidence.

    // Filter to sources with clear direction
    let directional: Vec<_> = sources.iter().filter(|(_, d, _)| *d != 0.0).collect();

    if directional.is_empty() {
        return ("LOW".to_string(), "neutral".to_string());
    }

    // Determine consensus direction (weighted by strength)
    let weighted_dir: f64 = directional.iter().map(|(_, d, s)| d * s).sum();
    // Tie → neutral, not bearish
    let consensus_dir = if weighted_dir > 0.0 {
        1.0
    } else if weighted_dir < 0.0 {
        -1.0
    } else {
        return ("WATCH".to_string(), "neutral".to_string());
    };

    // Count aligned vs conflicting
    let aligned: Vec<_> = directional
        .iter()
        .filter(|(_, d, _)| (*d as f64).signum() == (consensus_dir as f64).signum())
        .collect();
    let conflicting: Vec<_> = directional
        .iter()
        .filter(|(_, d, _)| (*d as f64).signum() != (consensus_dir as f64).signum())
        .collect();

    let n_aligned = aligned.len();
    let n_conflicting = conflicting.len();
    let has_strong_conflict = conflicting.iter().any(|(_, _, s)| *s > 0.5);

    // Classify
    let confidence = if n_aligned >= 2 && n_conflicting == 0 && c.composite > 0.35 {
        "HIGH"
    } else if n_aligned >= 2 && !has_strong_conflict && c.composite > 0.25 {
        "MODERATE"
    } else if n_aligned >= 1 && !has_strong_conflict {
        "WATCH"
    } else {
        "LOW"
    };

    let direction = if consensus_dir > 0.0 {
        "bullish"
    } else {
        "bearish"
    };

    (confidence.to_string(), direction.to_string())
}

fn classify_report_lane(
    c: &Candidate,
    confidence: &str,
    direction: &str,
) -> (String, String) {
    let directional = direction != "neutral";

    if confidence == "HIGH" || (confidence == "MODERATE" && directional && c.composite >= 0.50) {
        let reason = if confidence == "HIGH" {
            "高置信且方向明确，适合作为主报告主书信号"
        } else {
            "中等置信但综合分与方向一致性足够，可作为主报告主线代表"
        };
        return ("CORE BOOK".to_string(), reason.to_string());
    }

    let theme_like = directional
        && (
            confidence == "MODERATE"
                || confidence == "WATCH"
                || c.information_s >= 0.70
                || c.event_score >= 0.20
                || c.magnitude_score >= 0.70
        );
    if theme_like {
        return (
            "THEME ROTATION".to_string(),
            "更适合作为主题轮动或资金主线观察，不宜直接当作主书结论".to_string(),
        );
    }

    (
        "RADAR".to_string(),
        "信号边缘或冲突较多，保留在雷达层持续跟踪".to_string(),
    )
}

// ── Data loading ────────────────────────────────────────────────────────────

fn load_candidates(db: &Connection, date_str: &str) -> Result<Vec<Candidate>> {
    let sql = "
        WITH ranked AS (
            SELECT ts_code, trade_date, close, pct_chg, vol,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        ),
        latest_prices AS (
            SELECT p.ts_code,
                   p.pct_chg AS ret_1d,
                   p.vol AS vol_today,
                   p5.close AS close_5d_ago,
                   p20.close AS close_20d_ago,
                   p.close AS close_today
            FROM ranked p
            LEFT JOIN ranked p5 ON p.ts_code = p5.ts_code AND p5.rn = 6
            INNER JOIN ranked p20 ON p.ts_code = p20.ts_code AND p20.rn = 21
            WHERE p.rn = 1
              AND p20.close IS NOT NULL
        )
        SELECT
            lp.ts_code,
            CASE WHEN lp.close_5d_ago > 0 THEN (lp.close_today / lp.close_5d_ago - 1.0) * 100.0 ELSE 0 END AS ret_5d,
            CASE WHEN lp.close_20d_ago > 0 THEN (lp.close_today / lp.close_20d_ago - 1.0) * 100.0 ELSE 0 END AS ret_20d,
            COALESCE(a_tp.value, 0.5) AS trend_prob,
            COALESCE(a_tn.value, 0) AS trend_prob_n,
            COALESCE(a_is.value, 0) AS information_score,
            COALESCE(a_pu.value, 0.5) AS p_upside,
            COALESCE(a_sc.value, -1) AS surprise_category,
            COALESCE(a_pd.value, 0) AS p_drop,
            COALESCE(a_ud.value, -1) AS unlock_days,
            COALESCE(a_fr.value, 0) AS float_ratio,
            COALESCE(a_rg.value, 2) AS regime,
            COALESCE(a_rs.value, 0) AS reversion_score,
            COALESCE(a_rd.value, 0) AS reversion_direction,
            COALESCE(a_bs.value, 0) AS breakout_score,
            COALESCE(a_bd.value, 0) AS breakout_direction,
            COALESCE(a_rsi.value, 50) AS rsi_14,
            COALESCE(a_bb.value, 0.5) AS bb_position,
            COALESCE(a_lf.value, 0) AS lab_factor
        FROM latest_prices lp
        LEFT JOIN analytics a_tp ON lp.ts_code = a_tp.ts_code AND a_tp.as_of = ? AND a_tp.module = 'momentum' AND a_tp.metric = 'trend_prob'
        LEFT JOIN analytics a_tn ON lp.ts_code = a_tn.ts_code AND a_tn.as_of = ? AND a_tn.module = 'momentum' AND a_tn.metric = 'trend_prob_n'
        LEFT JOIN analytics a_is ON lp.ts_code = a_is.ts_code AND a_is.as_of = ? AND a_is.module = 'flow' AND a_is.metric = 'information_score'
        LEFT JOIN analytics a_pu ON lp.ts_code = a_pu.ts_code AND a_pu.as_of = ? AND a_pu.module = 'announcement' AND a_pu.metric = 'p_upside'
        LEFT JOIN analytics a_sc ON lp.ts_code = a_sc.ts_code AND a_sc.as_of = ? AND a_sc.module = 'announcement' AND a_sc.metric = 'surprise_category'
        LEFT JOIN analytics a_pd ON lp.ts_code = a_pd.ts_code AND a_pd.as_of = ? AND a_pd.module = 'unlock' AND a_pd.metric = 'p_drop'
        LEFT JOIN analytics a_ud ON lp.ts_code = a_ud.ts_code AND a_ud.as_of = ? AND a_ud.module = 'unlock' AND a_ud.metric = 'days_to_unlock'
        LEFT JOIN analytics a_fr ON lp.ts_code = a_fr.ts_code AND a_fr.as_of = ? AND a_fr.module = 'unlock' AND a_fr.metric = 'float_ratio'
        LEFT JOIN analytics a_rg ON lp.ts_code = a_rg.ts_code AND a_rg.as_of = ? AND a_rg.module = 'momentum' AND a_rg.metric = 'regime'
        LEFT JOIN analytics a_rs ON lp.ts_code = a_rs.ts_code AND a_rs.as_of = ? AND a_rs.module = 'mean_reversion' AND a_rs.metric = 'reversion_score'
        LEFT JOIN analytics a_rd ON lp.ts_code = a_rd.ts_code AND a_rd.as_of = ? AND a_rd.module = 'mean_reversion' AND a_rd.metric = 'reversion_direction'
        LEFT JOIN analytics a_bs ON lp.ts_code = a_bs.ts_code AND a_bs.as_of = ? AND a_bs.module = 'breakout' AND a_bs.metric = 'breakout_score'
        LEFT JOIN analytics a_bd ON lp.ts_code = a_bd.ts_code AND a_bd.as_of = ? AND a_bd.module = 'breakout' AND a_bd.metric = 'breakout_direction'
        LEFT JOIN analytics a_lf ON lp.ts_code = a_lf.ts_code AND a_lf.as_of = ? AND a_lf.module = 'lab_factor' AND a_lf.metric = 'lab_composite'
        LEFT JOIN analytics a_rsi ON lp.ts_code = a_rsi.ts_code AND a_rsi.as_of = ? AND a_rsi.module = 'mean_reversion' AND a_rsi.metric = 'rsi_14'
        LEFT JOIN analytics a_bb ON lp.ts_code = a_bb.ts_code AND a_bb.as_of = ? AND a_bb.module = 'mean_reversion' AND a_bb.metric = 'bb_position'
    ";

    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map(
        duckdb::params![
            date_str, // ranked CTE
            date_str, date_str, date_str, date_str, date_str, date_str, date_str, date_str, // original analytics
            date_str, date_str, date_str, date_str, date_str, date_str, date_str, date_str, // new analytics + lab_factor
        ],
        |row| {
            Ok(Candidate {
                ts_code: row.get::<_, String>(0)?,
                ret_5d: row.get::<_, f64>(1).unwrap_or(0.0),
                ret_20d: row.get::<_, f64>(2).unwrap_or(0.0),
                trend_prob: row.get::<_, f64>(3).unwrap_or(0.5),
                trend_prob_n: row.get::<_, f64>(4).unwrap_or(0.0),
                information_score: row.get::<_, f64>(5).unwrap_or(0.0),
                p_upside: row.get::<_, f64>(6).unwrap_or(0.5),
                surprise_category: row.get::<_, f64>(7).unwrap_or(-1.0),
                p_drop: row.get::<_, f64>(8).unwrap_or(0.0),
                unlock_days: row.get::<_, f64>(9).unwrap_or(-1.0),
                float_ratio: row.get::<_, f64>(10).unwrap_or(0.0),
                regime: row.get::<_, f64>(11).unwrap_or(2.0) as i32,
                reversion_score: row.get::<_, f64>(12).unwrap_or(0.0),
                reversion_direction: row.get::<_, f64>(13).unwrap_or(0.0),
                breakout_score: row.get::<_, f64>(14).unwrap_or(0.0),
                breakout_direction: row.get::<_, f64>(15).unwrap_or(0.0),
                rsi_14: row.get::<_, f64>(16).unwrap_or(50.0),
                bb_position: row.get::<_, f64>(17).unwrap_or(0.5),
                lab_factor: row.get::<_, f64>(18).unwrap_or(0.0),
                // Scores filled later
                magnitude_score: 0.0,
                momentum_score: 0.0,
                reversion_s: 0.0,
                breakout_s: 0.0,
                information_s: 0.0,
                event_score: 0.0,
                cross_asset_score: 0.0,
                composite: 0.0,
            })
        },
    )?;

    let mut candidates = Vec::new();
    for row in rows {
        if let Ok(c) = row {
            candidates.push(c);
        }
    }
    Ok(candidates)
}

fn load_stock_names(db: &Connection) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Ok(mut stmt) = db.prepare("SELECT ts_code, name FROM stock_basic") {
        if let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1).unwrap_or_default(),
            ))
        }) {
            for row in rows.flatten() {
                map.insert(row.0, row.1);
            }
        }
    }
    map
}

fn load_gate_multiplier(db: &Connection, date_str: &str) -> f64 {
    let result = db.query_row(
        "SELECT value FROM analytics WHERE ts_code = '_MARKET' AND as_of = ? AND module = 'macro_gate' AND metric = 'gate_multiplier'",
        duckdb::params![date_str],
        |row| row.get::<_, f64>(0),
    );
    result.unwrap_or(1.0)
}

fn load_sector_alignment(db: &Connection, date_str: &str) -> f64 {
    let result = db.query_row(
        "SELECT CAST(COUNT(CASE WHEN main_net_in > 0 THEN 1 END) AS DOUBLE) / NULLIF(COUNT(*), 0)
         FROM sector_fund_flow WHERE trade_date = ?",
        duckdb::params![date_str],
        |row| row.get::<_, Option<f64>>(0),
    );
    match result {
        Ok(Some(v)) => v,
        _ => 0.5,
    }
}

// ── Scoring helpers ─────────────────────────────────────────────────────────

fn compute_event_score(c: &Candidate) -> f64 {
    let mut score = 0.0f64;

    if c.surprise_category >= 0.0 {
        score += 0.6 * ((c.p_upside - 0.5).abs() * 2.0).min(1.0);
    }

    if c.unlock_days >= 0.0 {
        let proximity = if c.unlock_days <= 5.0 {
            1.0
        } else if c.unlock_days <= 15.0 {
            0.5
        } else {
            0.2
        };
        let size_factor = if c.float_ratio > 5.0 {
            1.0
        } else if c.float_ratio > 1.0 {
            0.6
        } else {
            0.3
        };
        score += 0.4 * c.p_drop * proximity * size_factor;
    }

    score.min(1.0)
}

fn zscore_to_score(value: f64, mean: f64, std: f64) -> f64 {
    if std < 1e-10 {
        return 0.0;
    }
    let z = ((value - mean) / std).abs().min(3.0);
    z / 3.0
}

fn cross_stats(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 1.0);
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    (mean, variance.sqrt().max(1e-10))
}
