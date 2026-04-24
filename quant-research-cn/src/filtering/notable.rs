/// Two-pass notable item filter — Regime-Adaptive Scoring + Convergence Classification.
///
/// Pass 1: full universe → regime-adaptive composite → recall-aware top-N candidates
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
use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde::Serialize;

use crate::analytics::headline_gate::{summarize_headline_gate, HeadlineSignalSummary};
use crate::analytics::shadow_calibration::{
    summarize_shadow_calibration, ShadowCalibrationSummary, BASE_SHADOW_PASS1_RESERVE,
    BASE_SHADOW_WEIGHT, POSITIVE_SHADOW_THRESHOLD,
};
use crate::analytics::shadow_option;
use crate::config::Settings;

// ── Fixed weights (regime-independent) ────────────────────────────────────
const W_MAGNITUDE: f64 = 0.10;
const W_INFORMATION: f64 = 0.18; // flow
const W_EVENT: f64 = 0.14; // announcements
const W_CROSS_ASSET: f64 = 0.10;
const W_LAB: f64 = 0.18; // Factor Lab is recall/research input, not a second book
                         // Remaining 0.20 is split among momentum/reversion/breakout by regime.
                         // Shadow/setup/execution signals are added separately below.

// ── Regime-adaptive weights for the 0.20 directional budget ───────────────
// (momentum, reversion, breakout) — must sum to 0.20
const REGIME_WEIGHTS_TRENDING: (f64, f64, f64) = (0.13, 0.01, 0.06);
const REGIME_WEIGHTS_MEAN_REV: (f64, f64, f64) = (0.03, 0.12, 0.05);
const REGIME_WEIGHTS_NOISY: (f64, f64, f64) = (0.06, 0.06, 0.08);

const PASS1_CUTOFF_BASE: usize = 120;
const PASS1_CUTOFF_MAX: usize = 180;
const EXECUTION_ACTIONABLE_MIN_SCORE: f64 = 0.50;
const RANGE_CORE_BUCKET: &str = "RANGE CORE";
const TACTICAL_CONTINUATION_BUCKET: &str = "TACTICAL CONTINUATION";
const UNCERTAIN_TACTICAL_LIMIT_MAX: usize = 4;
const UNCERTAIN_RANGE_CORE_LIMIT_MAX: usize = 2;
const HIGH_CONFIDENCE_COMPOSITE_MIN: f64 = 0.32;
const MODERATE_CONFIDENCE_COMPOSITE_MIN: f64 = 0.22;
const STRUCTURAL_PROMOTION_COMPOSITE_MIN: f64 = 0.28;
const STRUCTURAL_PROMOTION_EXECUTION_MIN: f64 = 0.53;
const STRUCTURAL_PROMOTION_SETUP_MIN: f64 = 0.44;
const STRUCTURAL_PROMOTION_CONTINUATION_MIN: f64 = 0.40;
const TACTICAL_CONTINUATION_EXECUTION_MIN: f64 = 0.48;
const TACTICAL_CONTINUATION_WATCH_EXECUTION_MIN: f64 = 0.58;
const TACTICAL_CONTINUATION_WATCH_COMPOSITE_MIN: f64 = 0.32;
const TACTICAL_CONTINUATION_SETUP_MIN: f64 = 0.44;
const TACTICAL_CONTINUATION_SCORE_MIN: f64 = 0.40;
const TACTICAL_CONTINUATION_FADE_MAX: f64 = 0.62;
const TACTICAL_CONTINUATION_CONFIRM_BREAKOUT_MIN: f64 = 0.42;
const TACTICAL_CONTINUATION_CONFIRM_INFO_MIN: f64 = 0.52;
const TACTICAL_CONTINUATION_CONFIRM_SHADOW_MIN: f64 = 0.52;
const TACTICAL_CONTINUATION_MEAN_REV_SETUP_MIN: f64 = 0.46;
const TACTICAL_CONTINUATION_MEAN_REV_SCORE_MIN: f64 = 0.42;
const TACTICAL_CONTINUATION_MEAN_REV_EXECUTION_MIN: f64 = 0.55;
const TACTICAL_CONTINUATION_MEAN_REV_FADE_MAX: f64 = 0.36;
const TACTICAL_CONTINUATION_NOISY_SETUP_MIN: f64 = 0.58;
const TACTICAL_CONTINUATION_NOISY_SCORE_MIN: f64 = 0.50;
const TACTICAL_CONTINUATION_NOISY_EXECUTION_MIN: f64 = 0.57;
const TACTICAL_CONTINUATION_NOISY_FADE_MAX: f64 = 0.32;
const RANGE_CORE_EXECUTION_MIN: f64 = 0.56;
const RANGE_CORE_COMPOSITE_MIN: f64 = 0.40;
const RANGE_CORE_SETUP_MIN: f64 = 0.44;
const RANGE_CORE_CONTINUATION_MIN: f64 = 0.40;
const RANGE_CORE_INFO_MIN: f64 = 0.70;
const RANGE_CORE_EVENT_MIN: f64 = 0.20;
const RANGE_CORE_FADE_MAX: f64 = 0.38;
const UNCERTAIN_TACTICAL_PRIORITY_REGIME_BONUS: f64 = 0.12;
const UNCERTAIN_TACTICAL_PRIORITY_EVENT_BONUS: f64 = 0.04;

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

#[derive(Debug, Clone)]
pub struct ReviewDecision {
    pub symbol: String,
    pub selection_status: String,
    pub rank_order: i64,
    pub report_bucket: String,
    pub signal_direction: String,
    pub signal_confidence: String,
    pub composite_score: f64,
    pub execution_mode: String,
    pub execution_score: f64,
    pub max_chase_gap_pct: f64,
    pub pullback_trigger_pct: f64,
    pub setup_score: f64,
    pub continuation_score: f64,
    pub fade_risk: f64,
    pub reference_close: Option<f64>,
    pub details_json: String,
}

/// Internal intermediate representation for scoring.
#[derive(Debug, Clone)]
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
    regime: i32, // 0=Trending, 1=MeanReverting, 2=Noisy, -1=unknown
    reversion_score: f64,
    reversion_direction: f64, // 1=bullish, -1=bearish, 0=neutral
    breakout_score: f64,
    breakout_direction: f64, // 1=bullish, -1=bearish, 0=none
    rsi_14: f64,
    bb_position: f64,
    lab_factor: f64, // Factor Lab composite (0 if no promoted factors)
    lab_trade_date: Option<NaiveDate>,
    lab_is_fresh: bool,
    shadow_iv_30d: f64,
    shadow_iv_60d: f64,
    shadow_iv_90d: f64,
    downside_stress: f64,
    shadow_proxy: Option<String>,
    shadow_put_90_3m: f64,
    shadow_touch_90_3m: f64,
    shadow_floor_1sigma_3m: Option<f64>,
    shadow_floor_2sigma_3m: Option<f64>,
    shadow_skew_90_3m: f64,
    shadow_alpha_score: f64,
    shadow_rank_score: f64,
    setup_score: f64,
    setup_direction: f64,
    continuation_score: f64,
    fade_risk: f64,
    continuation_direction: f64,
    execution_score: f64,
    max_chase_gap_pct: f64,
    pullback_trigger_pct: f64,
    pullback_price: Option<f64>,
    execution_mode: String,
    calibrated_shadow_alpha_prob: f64,
    stale_chase_risk: f64,
    entry_quality_score: f64,
    calibration_bucket_id: f64,
    shadow_option_alpha_detail: Option<serde_json::Value>,
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

struct PreparedCandidates {
    candidates: Vec<Candidate>,
    gate_mult: f64,
    fresh_lab: usize,
    stale_lab: usize,
    full_shadow_symbols: Vec<String>,
}

struct AnalyticsAsOfDates {
    prices: String,
    flow: String,
    announcement: String,
    unlock: String,
    momentum: String,
    mean_reversion: String,
    breakout: String,
    lab_factor: String,
    shadow_fast: String,
    shadow_full: String,
    setup_alpha: String,
    continuation_vs_fade: String,
    open_execution_gate: String,
    shadow_option_alpha: String,
    macro_gate: String,
    sector_flow: String,
}

pub fn materialize_shadow_full(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let prepared = prepare_candidates(db, cfg, as_of)?;
    if prepared.candidates.is_empty() {
        tracing::warn!(%as_of, "shadow_full skipped: no notable candidates");
        return Ok(0);
    }

    let written = shadow_option::enrich_symbols_full(db, as_of, &prepared.full_shadow_symbols)?;
    tracing::info!(
        %as_of,
        shortlist = prepared.full_shadow_symbols.len(),
        rows = written,
        "shadow_full materialized during analysis stage"
    );
    Ok(written)
}

pub fn build_notable_items(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
) -> Result<Vec<NotableItem>> {
    let base_report_limit = cfg.output.max_notable_items;
    let shadow_calibration = summarize_shadow_calibration(db, as_of);
    let report_limit = effective_report_limit(base_report_limit, &shadow_calibration);
    let selection_pool_limit = expanded_report_pool_limit(report_limit, &shadow_calibration);
    let PreparedCandidates {
        mut candidates,
        gate_mult,
        fresh_lab,
        stale_lab,
        ..
    } = prepare_candidates(db, cfg, as_of)?;

    if candidates.is_empty() {
        tracing::warn!("no candidates with analytics data for {}", as_of);
        return Ok(Vec::new());
    }

    candidates.truncate(selection_pool_limit);
    let uncertain_selection = plan_uncertain_headline_candidates(&candidates);

    // ── Load stock names ────────────────────────────────────────────────────
    let names = load_stock_names(db);

    // ── Convert to NotableItems with convergence classification ────────────
    let mut items: Vec<NotableItem> = candidates
        .into_iter()
        .map(move |c| {
            let (confidence, direction) = classify_convergence(&c);
            let tactical_continuation_candidate =
                is_tactical_continuation_candidate(&c, &confidence, &direction);
            let tactical_priority = tactical_priority_score(&c);
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
                    "lab_factor": c.lab_factor,
                    "lab_trade_date": c.lab_trade_date.map(|d| d.to_string()),
                    "lab_is_fresh": c.lab_is_fresh,
                    "shadow_iv_30d": c.shadow_iv_30d,
                    "shadow_iv_60d": c.shadow_iv_60d,
                    "shadow_iv_90d": c.shadow_iv_90d,
                    "downside_stress": c.downside_stress,
                    "shadow_proxy": c.shadow_proxy,
                    "shadow_alpha_score": c.shadow_alpha_score,
                    "shadow_rank_score": c.shadow_rank_score,
                    "shadow_live_weight": shadow_calibration.recommended_weight,
                    "shadow_live_reserve": shadow_calibration.recommended_reserve,
                    "shadow_recall_gap": shadow_calibration.recall_gap,
                    "shadow_quality_gap": shadow_calibration.quality_gap,
                    "setup_score": c.setup_score,
                    "setup_direction": signed_direction_label(c.setup_direction),
                    "continuation_score": c.continuation_score,
                    "fade_risk": c.fade_risk,
                    "continuation_direction": signed_direction_label(c.continuation_direction),
                    "tactical_continuation_candidate": tactical_continuation_candidate,
                    "tactical_priority_score": tactical_priority,
                    "execution_score": c.execution_score,
                    "max_chase_gap_pct": c.max_chase_gap_pct,
                    "pullback_trigger_pct": c.pullback_trigger_pct,
                    "pullback_price": c.pullback_price,
                    "execution_mode": c.execution_mode,
                    "shadow_option_alpha_prob": c.calibrated_shadow_alpha_prob,
                    "stale_chase_risk": c.stale_chase_risk,
                    "entry_quality_score": c.entry_quality_score,
                    "shadow_option_calibration_bucket_id": c.calibration_bucket_id,
                    "shadow_option_alpha": c.shadow_option_alpha_detail,
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

    let item_symbols: Vec<String> = items.iter().map(|item| item.ts_code.clone()).collect();
    let full_shadow = shadow_option::load_full_metrics(db, as_of, &item_symbols);
    for item in &mut items {
        if let Some(metrics) = full_shadow.get(&item.ts_code) {
            if let Some(obj) = item.detail.as_object_mut() {
                obj.insert(
                    "shadow_put_90_3m".to_string(),
                    serde_json::json!(metrics.put_90_3m),
                );
                obj.insert(
                    "shadow_put_80_3m".to_string(),
                    serde_json::json!(metrics.put_80_3m),
                );
                obj.insert(
                    "shadow_touch_90_3m".to_string(),
                    serde_json::json!(metrics.touch_90_3m),
                );
                obj.insert(
                    "shadow_floor_1sigma_3m".to_string(),
                    serde_json::json!(metrics.floor_1sigma_3m),
                );
                obj.insert(
                    "shadow_floor_2sigma_3m".to_string(),
                    serde_json::json!(metrics.floor_2sigma_3m),
                );
                obj.insert(
                    "shadow_skew_90_3m".to_string(),
                    serde_json::json!(metrics.skew_90_3m),
                );
            }
        }
    }

    let headline_gate = summarize_headline_gate(
        db,
        as_of,
        &items
            .iter()
            .map(|item| HeadlineSignalSummary {
                direction: item.signal.direction.clone(),
                trend_prob: item.detail.get("trend_prob").and_then(|v| v.as_f64()),
                report_bucket: item.report_bucket.clone(),
            })
            .collect::<Vec<_>>(),
    );

    if headline_gate.mode == "uncertain" {
        let headline_reason = headline_gate
            .reasons
            .first()
            .cloned()
            .unwrap_or_else(|| "方向性优势不足".to_string());
        let mut tactical_slots_remaining = uncertain_selection.tactical_limit;
        let mut range_core_slots_remaining = uncertain_selection.range_core_limit;
        for item in &mut items {
            let tactical_candidate = uncertain_selection.tactical_symbols.contains(&item.ts_code);
            let range_core_candidate = uncertain_selection
                .range_core_symbols
                .contains(&item.ts_code);
            apply_uncertain_headline_policy(
                &mut item.report_bucket,
                &mut item.report_reason,
                &headline_reason,
                tactical_candidate,
                &mut tactical_slots_remaining,
                range_core_candidate,
                &mut range_core_slots_remaining,
            );
        }
        tracing::info!(
            tactical_candidates = uncertain_selection.tactical_symbols.len(),
            range_core_candidates = uncertain_selection.range_core_symbols.len(),
            tactical_assigned = items
                .iter()
                .filter(|item| item.report_bucket == TACTICAL_CONTINUATION_BUCKET)
                .count(),
            range_core_assigned = items
                .iter()
                .filter(|item| item.report_bucket == RANGE_CORE_BUCKET)
                .count(),
            tactical_symbols = ?uncertain_selection.tactical_symbols,
            range_core_symbols = ?uncertain_selection.range_core_symbols,
            "uncertain tactical/range-core selection applied"
        );
    }

    items = downselect_report_items(items, report_limit);

    tracing::info!(
        max = report_limit,
        pool = selection_pool_limit,
        pass1 = effective_pass1_cutoff(&shadow_calibration),
        found = items.len(),
        gate = gate_mult,
        base_shadow_weight = BASE_SHADOW_WEIGHT,
        shadow_positive_threshold = POSITIVE_SHADOW_THRESHOLD,
        headline_mode = %headline_gate.mode,
        fresh_lab,
        stale_lab,
        "notable items filtered"
    );

    Ok(items)
}

pub fn build_review_decisions(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
    selected_symbols: &[String],
) -> Result<Vec<ReviewDecision>> {
    let shadow_calibration = summarize_shadow_calibration(db, as_of);
    let max_items = effective_report_limit(cfg.output.max_notable_items, &shadow_calibration);
    let PreparedCandidates { candidates, .. } = prepare_candidates(db, cfg, as_of)?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let selected_set: HashSet<&str> = selected_symbols.iter().map(|s| s.as_str()).collect();
    let top_slice = candidates.iter().take(max_items);
    let headline_gate = summarize_headline_gate(
        db,
        as_of,
        &top_slice
            .map(|c| {
                let (confidence, direction) = classify_convergence(c);
                let (report_bucket, _) = classify_report_lane(c, &confidence, &direction);
                HeadlineSignalSummary {
                    direction,
                    trend_prob: Some(c.trend_prob),
                    report_bucket,
                }
            })
            .collect::<Vec<_>>(),
    );
    let headline_reason = headline_gate
        .reasons
        .first()
        .cloned()
        .unwrap_or_else(|| "方向性优势不足".to_string());
    let reference_close = load_reference_close_map(db, &as_of.to_string());
    let uncertain_selection = if headline_gate.mode == "uncertain" {
        Some(plan_uncertain_headline_candidates(&candidates))
    } else {
        None
    };
    let mut tactical_slots_remaining = uncertain_selection
        .as_ref()
        .map(|plan| plan.tactical_limit)
        .unwrap_or(0);
    let mut range_core_slots_remaining = uncertain_selection
        .as_ref()
        .map(|plan| plan.range_core_limit)
        .unwrap_or(0);

    let mut decisions = Vec::with_capacity(candidates.len());
    for (idx, c) in candidates.into_iter().enumerate() {
        let (confidence, direction) = classify_convergence(&c);
        let (mut report_bucket, mut report_reason) =
            classify_report_lane(&c, &confidence, &direction);
        let tactical_candidate = uncertain_selection
            .as_ref()
            .map(|plan| plan.tactical_symbols.contains(&c.ts_code))
            .unwrap_or(false);
        let range_core_candidate = uncertain_selection
            .as_ref()
            .map(|plan| plan.range_core_symbols.contains(&c.ts_code))
            .unwrap_or(false);

        if headline_gate.mode == "uncertain" {
            apply_uncertain_headline_policy(
                &mut report_bucket,
                &mut report_reason,
                &headline_reason,
                tactical_candidate,
                &mut tactical_slots_remaining,
                range_core_candidate,
                &mut range_core_slots_remaining,
            );
        }

        decisions.push(ReviewDecision {
            symbol: c.ts_code.clone(),
            selection_status: if selected_set.contains(c.ts_code.as_str()) {
                "selected".to_string()
            } else {
                "ignored".to_string()
            },
            rank_order: (idx + 1) as i64,
            report_bucket,
            signal_direction: direction,
            signal_confidence: confidence,
            composite_score: c.composite,
            execution_mode: c.execution_mode.clone(),
            execution_score: c.execution_score,
            max_chase_gap_pct: c.max_chase_gap_pct,
            pullback_trigger_pct: c.pullback_trigger_pct,
            setup_score: c.setup_score,
            continuation_score: c.continuation_score,
            fade_risk: c.fade_risk,
            reference_close: reference_close.get(&c.ts_code).copied(),
            details_json: serde_json::json!({
                "headline_mode": headline_gate.mode,
                "headline_reason": report_reason,
                "trend_prob": c.trend_prob,
                "trend_prob_n": c.trend_prob_n,
                "ret_5d": c.ret_5d,
                "ret_20d": c.ret_20d,
                "lab_is_fresh": c.lab_is_fresh,
                "shadow_alpha_score": c.shadow_alpha_score,
                "shadow_rank_score": c.shadow_rank_score,
                "shadow_live_weight": shadow_calibration.recommended_weight,
                "shadow_live_reserve": shadow_calibration.recommended_reserve,
                "shadow_recall_gap": shadow_calibration.recall_gap,
                "shadow_quality_gap": shadow_calibration.quality_gap,
                "shadow_iv_30d": c.shadow_iv_30d,
                "shadow_iv_60d": c.shadow_iv_60d,
                "shadow_iv_90d": c.shadow_iv_90d,
                "downside_stress": c.downside_stress,
                "shadow_put_90_3m": c.shadow_put_90_3m,
                "shadow_touch_90_3m": c.shadow_touch_90_3m,
                "shadow_floor_1sigma_3m": c.shadow_floor_1sigma_3m,
                "shadow_floor_2sigma_3m": c.shadow_floor_2sigma_3m,
                "shadow_skew_90_3m": c.shadow_skew_90_3m,
                "setup_score": c.setup_score,
                "continuation_score": c.continuation_score,
                "execution_score": c.execution_score,
                "execution_mode": c.execution_mode,
                "shadow_option_alpha_prob": c.calibrated_shadow_alpha_prob,
                "stale_chase_risk": c.stale_chase_risk,
                "entry_quality_score": c.entry_quality_score,
                "shadow_option_calibration_bucket_id": c.calibration_bucket_id,
                "shadow_option_alpha": c.shadow_option_alpha_detail,
            })
            .to_string(),
        });
    }

    Ok(decisions)
}

fn prepare_candidates(
    db: &Connection,
    cfg: &Settings,
    as_of: NaiveDate,
) -> Result<PreparedCandidates> {
    let effective_dates = resolve_analytics_as_of_dates(db, as_of);

    // ── Load macro gate multiplier ──────────────────────────────────────────
    let gate_mult = load_gate_multiplier(db, &effective_dates.macro_gate);
    let shadow_calibration = summarize_shadow_calibration(db, as_of);
    tracing::info!(
        %as_of,
        selected = shadow_calibration.selected_reviewed,
        ignored = shadow_calibration.ignored_reviewed,
        selected_positive = shadow_calibration.selected_positive,
        ignored_positive = shadow_calibration.ignored_positive,
        recall_gap = shadow_calibration.recall_gap,
        quality_gap = shadow_calibration.quality_gap,
        shadow_weight = shadow_calibration.recommended_weight,
        shadow_reserve = shadow_calibration.recommended_reserve,
        "shadow calibration summary loaded"
    );

    // ── Pass 1: Score full universe with regime-adaptive weights ───────────
    let mut candidates = load_candidates(db, &effective_dates)?;
    if candidates.is_empty() {
        return Ok(PreparedCandidates {
            candidates,
            gate_mult,
            fresh_lab: 0,
            stale_lab: 0,
            full_shadow_symbols: Vec::new(),
        });
    }

    let mut fresh_lab = 0usize;
    let mut stale_lab = 0usize;
    for c in &mut candidates {
        let is_fresh = c
            .lab_trade_date
            .map(|trade_date| {
                let age_days = as_of.signed_duration_since(trade_date).num_days();
                (0..=3).contains(&age_days)
            })
            .unwrap_or(false);
        c.lab_is_fresh = is_fresh;
        if is_fresh {
            fresh_lab += 1;
        } else {
            stale_lab += 1;
            c.lab_factor = 0.0;
        }
    }

    // Compute magnitude z-scores cross-sectionally
    let ret5d_vals: Vec<f64> = candidates.iter().map(|c| c.ret_5d.abs()).collect();
    let (mag_mean, mag_std) = cross_stats(&ret5d_vals);

    // Compute cross-asset score from sector fund flow
    let sector_flow = load_sector_alignment(db, &effective_dates.sector_flow);

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
        c.shadow_alpha_score = compute_shadow_alpha_score(c);
        c.shadow_rank_score = compute_shadow_rank_score(c);

        // ── Regime-adaptive weighting ──────────────────────────────────────
        let (w_mom, w_rev, w_brk) = match c.regime {
            0 => REGIME_WEIGHTS_TRENDING,
            1 => REGIME_WEIGHTS_MEAN_REV,
            _ => REGIME_WEIGHTS_NOISY, // includes unknown (-1)
        };

        let execution_penalty = match c.execution_mode.as_str() {
            "do_not_chase" => 0.10,
            "wait_pullback" => 0.04,
            _ => 0.0,
        };

        c.composite = (W_MAGNITUDE * c.magnitude_score
            + W_INFORMATION * c.information_s
            + w_mom * c.momentum_score
            + w_rev * c.reversion_s
            + w_brk * c.breakout_s
            + W_EVENT * c.event_score
            + W_CROSS_ASSET * c.cross_asset_score
            + W_LAB * c.lab_factor.abs().min(1.0)
            + shadow_calibration.recommended_weight * c.shadow_rank_score
            + 0.08 * c.setup_score
            + 0.06 * c.continuation_score
            + 0.05 * c.execution_score
            - 0.08 * c.fade_risk
            - execution_penalty)
            * gate_mult;

        c.composite = c.composite.clamp(0.0, 1.0);
    }

    let full_shadow_event_symbols: Vec<String> = candidates
        .iter()
        .filter(|c| {
            c.event_score >= 0.20
                || (c.unlock_days >= 0.0 && c.unlock_days <= 10.0)
                || (c.downside_stress >= 0.55 && c.shadow_iv_30d >= 18.0)
        })
        .map(|c| c.ts_code.clone())
        .collect();

    let mut shadow_focus: Vec<Candidate> = candidates
        .iter()
        .filter(|c| {
            c.shadow_rank_score >= POSITIVE_SHADOW_THRESHOLD
                && c.execution_mode != "do_not_chase"
                && (c.setup_direction > 0.0
                    || c.continuation_direction > 0.0
                    || c.breakout_direction > 0.0)
                && (c.setup_score >= 0.45
                    || c.continuation_score >= 0.45
                    || c.execution_score >= 0.52)
        })
        .cloned()
        .collect();
    shadow_focus.sort_by(|a, b| {
        b.shadow_alpha_score
            .partial_cmp(&a.shadow_alpha_score)
            .unwrap()
            .then_with(|| b.execution_score.partial_cmp(&a.execution_score).unwrap())
            .then_with(|| b.setup_score.partial_cmp(&a.setup_score).unwrap())
            .then_with(|| b.composite.partial_cmp(&a.composite).unwrap())
    });

    // Sort by composite descending
    candidates.sort_by(|a, b| b.composite.partial_cmp(&a.composite).unwrap());

    let pass1_cutoff = effective_pass1_cutoff(&shadow_calibration);
    let reserve = shadow_focus.len().min(
        shadow_calibration
            .recommended_reserve
            .max(BASE_SHADOW_PASS1_RESERVE),
    );
    let primary_keep = pass1_cutoff.saturating_sub(reserve);
    let mut selected: Vec<Candidate> = Vec::with_capacity(pass1_cutoff);
    let mut seen: HashSet<String> = HashSet::new();

    for c in candidates.into_iter() {
        if selected.len() >= primary_keep {
            break;
        }
        if seen.insert(c.ts_code.clone()) {
            selected.push(c);
        }
    }

    for c in shadow_focus.into_iter() {
        if selected.len() >= pass1_cutoff {
            break;
        }
        if seen.insert(c.ts_code.clone()) {
            selected.push(c);
        }
    }

    selected.sort_by(|a, b| b.composite.partial_cmp(&a.composite).unwrap());
    candidates = selected;

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

        if c.setup_score > 0.60 && c.continuation_score > 0.55 {
            confirmation_bonus += 0.05;
        }

        if c.execution_score > 0.58 && c.execution_mode == "executable" {
            confirmation_bonus += 0.04;
        }

        if c.fade_risk > 0.65 {
            confirmation_bonus -= 0.06;
        }

        if c.execution_mode == "wait_pullback" {
            confirmation_bonus -= 0.03;
        } else if c.execution_mode == "do_not_chase" {
            confirmation_bonus -= 0.08;
        }

        c.composite = (c.composite + confirmation_bonus - front_rank_penalty(c)).clamp(0.0, 1.0);
    }

    candidates.sort_by(|a, b| b.composite.partial_cmp(&a.composite).unwrap());

    let mut full_shadow_symbols: Vec<String> =
        candidates.iter().map(|c| c.ts_code.clone()).collect();
    full_shadow_symbols.extend(cfg.universe.watchlist.iter().cloned());
    full_shadow_symbols.extend(full_shadow_event_symbols);

    Ok(PreparedCandidates {
        candidates,
        gate_mult,
        fresh_lab,
        stale_lab,
        full_shadow_symbols,
    })
}

// ── Convergence-based classification ──────────────────────────────────────

fn effective_source_shrinkage(weights: &[f64]) -> f64 {
    if weights.len() <= 1 {
        return 1.0;
    }

    let cleaned: Vec<f64> = weights
        .iter()
        .copied()
        .filter(|w| *w > 0.0 && w.is_finite())
        .collect();
    if cleaned.len() <= 1 {
        return 1.0;
    }

    let total: f64 = cleaned.iter().sum();
    if total <= 0.0 {
        return 0.60;
    }

    let sum_sq: f64 = cleaned
        .iter()
        .map(|w| {
            let p = *w / total;
            p * p
        })
        .sum();
    if sum_sq <= 0.0 {
        return 1.0;
    }

    let n_total = cleaned.len() as f64;
    let n_effective = 1.0 / sum_sq.max(1e-12);
    (n_effective / n_total).sqrt().clamp(0.60, 1.0)
}

fn has_structural_follow_through(c: &Candidate) -> bool {
    (c.setup_direction > 0.0 && c.setup_score >= STRUCTURAL_PROMOTION_SETUP_MIN)
        || (c.continuation_direction > 0.0
            && c.continuation_score >= STRUCTURAL_PROMOTION_CONTINUATION_MIN)
}

fn has_continuation_confirmation(c: &Candidate) -> bool {
    (c.breakout_direction > 0.0 && c.breakout_score >= TACTICAL_CONTINUATION_CONFIRM_BREAKOUT_MIN)
        || c.information_s >= TACTICAL_CONTINUATION_CONFIRM_INFO_MIN
        || c.shadow_rank_score >= TACTICAL_CONTINUATION_CONFIRM_SHADOW_MIN
}

fn is_chinext_symbol(ts_code: &str) -> bool {
    ts_code.starts_with("300") || ts_code.starts_with("301")
}

fn tactical_priority_score(c: &Candidate) -> f64 {
    let regime_bonus = match c.regime {
        1 if c.execution_score >= TACTICAL_CONTINUATION_MEAN_REV_EXECUTION_MIN
            && c.fade_risk <= TACTICAL_CONTINUATION_MEAN_REV_FADE_MAX =>
        {
            UNCERTAIN_TACTICAL_PRIORITY_REGIME_BONUS
        }
        0 if c.setup_score >= 0.55 && c.continuation_score >= 0.50 => 0.04,
        2 if c.setup_score >= TACTICAL_CONTINUATION_NOISY_SETUP_MIN
            && c.continuation_score >= TACTICAL_CONTINUATION_NOISY_SCORE_MIN
            && c.execution_score >= TACTICAL_CONTINUATION_NOISY_EXECUTION_MIN
            && c.fade_risk <= TACTICAL_CONTINUATION_NOISY_FADE_MAX =>
        {
            0.02
        }
        _ => 0.0,
    };
    let event_bonus = if c.event_score >= 0.45 || c.p_upside >= 0.80 {
        UNCERTAIN_TACTICAL_PRIORITY_EVENT_BONUS
    } else {
        0.0
    };

    (0.20 * c.composite
        + 0.25 * c.execution_score
        + 0.20 * c.setup_score
        + 0.20 * c.continuation_score
        + 0.10 * c.information_s
        + regime_bonus
        + event_bonus
        - 0.18 * c.fade_risk)
        .clamp(0.0, 2.0)
}

fn range_core_priority_score(c: &Candidate) -> f64 {
    (0.34 * c.composite
        + 0.24 * c.execution_score
        + 0.16 * c.setup_score
        + 0.14 * c.continuation_score
        + 0.08 * c.information_s
        + 0.04 * c.event_score
        - 0.16 * c.fade_risk)
        .clamp(0.0, 2.0)
}

fn dynamic_uncertain_tactical_limit(eligible_count: usize) -> usize {
    match eligible_count {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=5 => 3,
        _ => UNCERTAIN_TACTICAL_LIMIT_MAX,
    }
}

fn dynamic_uncertain_range_core_limit(eligible_count: usize) -> usize {
    match eligible_count {
        0 => 0,
        1..=3 => 1,
        _ => UNCERTAIN_RANGE_CORE_LIMIT_MAX,
    }
}

fn shadow_recall_pressure(summary: &ShadowCalibrationSummary) -> f64 {
    (0.70 * summary.ignored_positive_missed_rate + 0.60 * summary.recall_gap).clamp(0.0, 1.0)
}

fn effective_pass1_cutoff(summary: &ShadowCalibrationSummary) -> usize {
    let recall_pressure = shadow_recall_pressure(summary);
    let dynamic_extra = (36.0 * recall_pressure + 90.0 * summary.recall_gap).round() as usize;
    PASS1_CUTOFF_BASE
        .saturating_add(dynamic_extra)
        .clamp(PASS1_CUTOFF_BASE, PASS1_CUTOFF_MAX)
}

fn effective_report_limit(base_report_limit: usize, summary: &ShadowCalibrationSummary) -> usize {
    let recall_pressure = shadow_recall_pressure(summary);
    let dynamic_extra = (8.0 * recall_pressure + 40.0 * summary.recall_gap).round() as usize;
    base_report_limit
        .saturating_add(dynamic_extra)
        .clamp(base_report_limit, base_report_limit.saturating_add(8))
}

fn expanded_report_pool_limit(report_limit: usize, summary: &ShadowCalibrationSummary) -> usize {
    let recall_pressure = shadow_recall_pressure(summary);
    let base_extra = 8usize;
    let dynamic_extra = (16.0 * recall_pressure + 30.0 * summary.recall_gap).round() as usize;
    report_limit
        .saturating_add(base_extra + dynamic_extra)
        .clamp(report_limit, effective_pass1_cutoff(summary))
}

fn lane_priority(report_bucket: &str) -> usize {
    match report_bucket {
        "CORE BOOK" => 0,
        RANGE_CORE_BUCKET => 1,
        TACTICAL_CONTINUATION_BUCKET => 2,
        "THEME ROTATION" => 3,
        "RADAR" => 4,
        _ => 5,
    }
}

fn confidence_priority(confidence: &str) -> usize {
    match confidence {
        "HIGH" => 0,
        "MODERATE" => 1,
        "WATCH" => 2,
        "LOW" => 3,
        _ => 4,
    }
}

fn detail_num(item: &NotableItem, key: &str) -> f64 {
    item.detail.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

fn final_report_priority_score(item: &NotableItem) -> f64 {
    let tactical_priority = detail_num(item, "tactical_priority_score");
    let execution_score = detail_num(item, "execution_score");
    let setup_score = detail_num(item, "setup_score");
    let continuation_score = detail_num(item, "continuation_score");
    let fade_risk = detail_num(item, "fade_risk");
    let lane_bonus = match item.report_bucket.as_str() {
        "CORE BOOK" => 0.08,
        RANGE_CORE_BUCKET => 0.10,
        TACTICAL_CONTINUATION_BUCKET => 0.12,
        "THEME ROTATION" => 0.03,
        _ => 0.0,
    };

    (0.38 * item.composite_score
        + 0.18 * item.flow_score
        + 0.14 * item.event_score
        + 0.12 * item.magnitude_score
        + 0.12 * execution_score
        + 0.10 * setup_score
        + 0.10 * continuation_score
        + 0.14 * tactical_priority
        + lane_bonus
        - 0.12 * fade_risk)
        .clamp(0.0, 2.5)
}

fn downselect_report_items(mut items: Vec<NotableItem>, max_items: usize) -> Vec<NotableItem> {
    if items.len() <= max_items {
        return items;
    }

    items.sort_by(|a, b| {
        lane_priority(&a.report_bucket)
            .cmp(&lane_priority(&b.report_bucket))
            .then_with(|| {
                confidence_priority(&a.signal.confidence)
                    .cmp(&confidence_priority(&b.signal.confidence))
            })
            .then_with(|| {
                final_report_priority_score(b)
                    .partial_cmp(&final_report_priority_score(a))
                    .unwrap()
            })
            .then_with(|| b.composite_score.partial_cmp(&a.composite_score).unwrap())
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    items.truncate(max_items);
    items
}

#[derive(Debug, Clone)]
struct UncertainHeadlineSelectionPlan {
    tactical_symbols: HashSet<String>,
    range_core_symbols: HashSet<String>,
    tactical_limit: usize,
    range_core_limit: usize,
}

fn select_uncertain_tactical_symbols_from_candidates(candidates: &[Candidate]) -> HashSet<String> {
    let mut tactical_ranked: Vec<(String, f64, f64, f64)> = candidates
        .iter()
        .filter_map(|c| {
            let (confidence, direction) = classify_convergence(c);
            if !is_tactical_continuation_candidate(c, &confidence, &direction) {
                return None;
            }
            Some((
                c.ts_code.clone(),
                tactical_priority_score(c),
                c.execution_score,
                c.composite,
            ))
        })
        .collect();

    tactical_ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap()
            .then_with(|| b.2.partial_cmp(&a.2).unwrap())
            .then_with(|| b.3.partial_cmp(&a.3).unwrap())
    });

    let tactical_limit = dynamic_uncertain_tactical_limit(tactical_ranked.len());
    tactical_ranked
        .into_iter()
        .take(tactical_limit)
        .map(|(symbol, _, _, _)| symbol)
        .collect()
}

fn is_uncertain_range_core_candidate(
    c: &Candidate,
    confidence: &str,
    direction: &str,
    report_bucket: &str,
) -> bool {
    if is_chinext_symbol(&c.ts_code) {
        return false;
    }
    if report_bucket != "CORE BOOK" || direction != "bullish" {
        return false;
    }
    if confidence != "HIGH" && confidence != "MODERATE" {
        return false;
    }
    if c.execution_mode == "do_not_chase"
        || c.execution_score < RANGE_CORE_EXECUTION_MIN
        || c.fade_risk > RANGE_CORE_FADE_MAX
        || c.composite < RANGE_CORE_COMPOSITE_MIN
    {
        return false;
    }

    let structural_tailwind = c.setup_direction > 0.0 && c.setup_score >= RANGE_CORE_SETUP_MIN;
    let continuation_tailwind =
        c.continuation_direction > 0.0 && c.continuation_score >= RANGE_CORE_CONTINUATION_MIN;
    let context_tailwind =
        c.information_s >= RANGE_CORE_INFO_MIN || c.event_score >= RANGE_CORE_EVENT_MIN;

    (structural_tailwind && continuation_tailwind)
        || (structural_tailwind && context_tailwind)
        || (continuation_tailwind && context_tailwind)
}

fn plan_uncertain_headline_candidates(candidates: &[Candidate]) -> UncertainHeadlineSelectionPlan {
    let tactical_symbols = select_uncertain_tactical_symbols_from_candidates(candidates);
    let mut range_core_ranked: Vec<(String, f64, f64, f64)> = candidates
        .iter()
        .filter_map(|c| {
            if tactical_symbols.contains(&c.ts_code) {
                return None;
            }
            let (confidence, direction) = classify_convergence(c);
            let (report_bucket, _) = classify_report_lane(c, &confidence, &direction);
            if !is_uncertain_range_core_candidate(c, &confidence, &direction, &report_bucket) {
                return None;
            }
            Some((
                c.ts_code.clone(),
                range_core_priority_score(c),
                c.execution_score,
                c.composite,
            ))
        })
        .collect();

    range_core_ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap()
            .then_with(|| b.2.partial_cmp(&a.2).unwrap())
            .then_with(|| b.3.partial_cmp(&a.3).unwrap())
    });

    let range_core_limit = dynamic_uncertain_range_core_limit(range_core_ranked.len());
    let range_core_symbols = range_core_ranked
        .into_iter()
        .take(range_core_limit)
        .map(|(symbol, _, _, _)| symbol)
        .collect::<HashSet<_>>();

    UncertainHeadlineSelectionPlan {
        tactical_limit: tactical_symbols.len(),
        range_core_limit: range_core_symbols.len(),
        tactical_symbols,
        range_core_symbols,
    }
}

/// Count independent signal sources and classify by convergence.
/// Signal sources: momentum, reversion, breakout, flow, event
/// HIGH = 2+ independent sources aligned in same direction + no conflicts
fn classify_convergence(c: &Candidate) -> (String, String) {
    // Identify active directional signals and their directions
    // Each source: (name, direction_sign, strength)
    let mut sources: Vec<(&str, f64, f64)> = Vec::new();

    // 1. Momentum signal (trend_prob deviation)
    if c.momentum_score > 0.15 && c.trend_prob_n > 5.0 {
        let dir = if c.trend_prob > 0.55 {
            1.0
        } else if c.trend_prob < 0.45 {
            -1.0
        } else {
            0.0
        };
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

    // 8.5 Shadow option confirmation: only acts as a confirmer when the
    // structure/continuation side already provides a direction.
    if c.shadow_rank_score > 0.58
        && c.execution_mode == "executable"
        && c.execution_score >= EXECUTION_ACTIONABLE_MIN_SCORE
    {
        let dir = if c.setup_direction != 0.0 {
            c.setup_direction
        } else if c.continuation_direction != 0.0 {
            c.continuation_direction
        } else if c.breakout_direction != 0.0 {
            c.breakout_direction
        } else {
            0.0
        };
        if dir != 0.0 {
            sources.push(("shadow_option", dir, c.shadow_rank_score));
        }
    }

    // 8. Setup alpha (pre-breakout structure)
    if c.setup_score > 0.45 && c.setup_direction != 0.0 {
        sources.push(("setup_alpha", c.setup_direction, c.setup_score));
    }

    // 9. Continuation vs fade
    if c.continuation_score > 0.50 && c.continuation_direction != 0.0 {
        sources.push((
            "continuation",
            c.continuation_direction,
            c.continuation_score,
        ));
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
    let evidence_weights: Vec<f64> = directional.iter().map(|(_, _, s)| *s).collect();
    let confidence_shrinkage = effective_source_shrinkage(&evidence_weights);
    let effective_composite = c.composite * confidence_shrinkage;

    let structural_promotion = consensus_dir > 0.0
        && n_aligned >= 1
        && !has_strong_conflict
        && effective_composite >= STRUCTURAL_PROMOTION_COMPOSITE_MIN
        && c.execution_mode != "do_not_chase"
        && c.execution_score >= STRUCTURAL_PROMOTION_EXECUTION_MIN
        && has_structural_follow_through(c)
        && has_continuation_confirmation(c);

    // Classify
    let mut confidence = if n_aligned >= 2
        && n_conflicting == 0
        && effective_composite > HIGH_CONFIDENCE_COMPOSITE_MIN
    {
        "HIGH"
    } else if (n_aligned >= 2
        && !has_strong_conflict
        && effective_composite > MODERATE_CONFIDENCE_COMPOSITE_MIN)
        || structural_promotion
    {
        "MODERATE"
    } else if n_aligned >= 1 && !has_strong_conflict {
        "WATCH"
    } else {
        "LOW"
    };

    if confidence_shrinkage < 0.70 {
        confidence = match confidence {
            "HIGH" => "MODERATE",
            "MODERATE" => "WATCH",
            other => other,
        };
    }

    if c.execution_mode == "wait_pullback" && confidence == "HIGH" {
        confidence = "MODERATE";
    } else if c.execution_mode == "do_not_chase" {
        confidence = match confidence {
            "HIGH" => "WATCH",
            "MODERATE" => "WATCH",
            "WATCH" => "LOW",
            other => other,
        };
    }

    if shadow_execution_blocked(c) {
        confidence = match confidence {
            "HIGH" => "WATCH",
            "MODERATE" => "WATCH",
            other => other,
        };
    }

    let direction = if consensus_dir > 0.0 {
        "bullish"
    } else {
        "bearish"
    };

    (confidence.to_string(), direction.to_string())
}

fn classify_report_lane(c: &Candidate, confidence: &str, direction: &str) -> (String, String) {
    let directional = direction != "neutral";
    let shadow_blocked = shadow_execution_blocked(c);

    if c.execution_mode == "do_not_chase" {
        return (
            "RADAR".to_string(),
            if shadow_blocked {
                "影子期权层虽然仍有支撑，但执行门槛没有通过；当前属于高波追价区，只保留雷达观察"
                    .to_string()
            } else {
                "方向可能存在，但当前已进入高波追价区；仅保留雷达观察，不给机械追价".to_string()
            },
        );
    }

    if c.execution_mode == "wait_pullback" {
        return (
            "THEME ROTATION".to_string(),
            if !directional {
                "结构仍在修复，但方向性不足；只保留在轮动层等待更清晰的回踩确认".to_string()
            } else if shadow_blocked {
                format!(
                    "影子期权与结构方向仍在，但执行门槛未过；只允许留在轮动层等回踩，最大追价参考 {:.1}%",
                    c.max_chase_gap_pct
                )
            } else {
                format!(
                    "结构和方向存在，但次日更适合等回踩再介入；最大追价参考 {:.1}%",
                    c.max_chase_gap_pct
                )
            },
        );
    }

    if direction == "bearish" {
        return (
            "RADAR".to_string(),
            "当前账户按多头视角管理；偏空信号只作为风险回避与仓位收缩参考，不作为可执行主书"
                .to_string(),
        );
    }

    if direction == "bullish" && is_chinext_symbol(&c.ts_code) {
        return (
            "THEME ROTATION".to_string(),
            "创业板标的不进入可执行主书或战术层；即使结构成立，也只保留为主题轮动观察".to_string(),
        );
    }

    if shadow_blocked {
        return (
            "RADAR".to_string(),
            "影子期权支撑尚在，但执行评分不足，不应把它写成可直接做的信号".to_string(),
        );
    }

    if confidence == "HIGH" || (confidence == "MODERATE" && directional && c.composite >= 0.50) {
        let reason = if confidence == "HIGH" {
            "高置信且方向明确，适合作为主报告主书信号"
        } else {
            "中等置信但综合分与方向一致性足够，可作为主报告主线代表"
        };
        return ("CORE BOOK".to_string(), reason.to_string());
    }

    let theme_like = directional
        && (confidence == "MODERATE"
            || confidence == "WATCH"
            || c.information_s >= 0.70
            || c.event_score >= 0.20
            || c.magnitude_score >= 0.70);
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

fn is_tactical_continuation_candidate(c: &Candidate, confidence: &str, direction: &str) -> bool {
    if is_chinext_symbol(&c.ts_code) {
        return false;
    }
    if direction != "bullish" {
        return false;
    }
    let watch_override = confidence == "WATCH"
        && c.execution_score >= TACTICAL_CONTINUATION_WATCH_EXECUTION_MIN
        && c.composite >= TACTICAL_CONTINUATION_WATCH_COMPOSITE_MIN
        && has_structural_follow_through(c)
        && has_continuation_confirmation(c);
    if confidence != "HIGH" && confidence != "MODERATE" && !watch_override {
        return false;
    }
    if c.execution_mode == "do_not_chase"
        || c.execution_score < TACTICAL_CONTINUATION_EXECUTION_MIN
        || c.fade_risk > TACTICAL_CONTINUATION_FADE_MAX
    {
        return false;
    }

    let continuation_tailwind =
        c.continuation_direction > 0.0 && c.continuation_score >= TACTICAL_CONTINUATION_SCORE_MIN;
    let structural_tailwind =
        c.setup_direction > 0.0 && c.setup_score >= TACTICAL_CONTINUATION_SETUP_MIN;
    let confirmation_tailwind = has_continuation_confirmation(c);
    let noisy_relaunch_tailwind = c.regime == 2
        && c.setup_score >= TACTICAL_CONTINUATION_NOISY_SETUP_MIN
        && c.continuation_score >= TACTICAL_CONTINUATION_NOISY_SCORE_MIN
        && c.execution_score >= TACTICAL_CONTINUATION_NOISY_EXECUTION_MIN
        && c.fade_risk <= TACTICAL_CONTINUATION_NOISY_FADE_MAX;
    let mean_reversion_relaunch_tailwind = c.regime == 1
        && c.execution_score >= TACTICAL_CONTINUATION_MEAN_REV_EXECUTION_MIN
        && c.fade_risk <= TACTICAL_CONTINUATION_MEAN_REV_FADE_MAX
        && ((c.setup_score >= TACTICAL_CONTINUATION_MEAN_REV_SETUP_MIN
            && c.continuation_score >= TACTICAL_CONTINUATION_MEAN_REV_SCORE_MIN)
            || (c.setup_score >= 0.46
                && c.continuation_score >= 0.52
                && has_continuation_confirmation(c)));
    let regime_tailwind = c.regime == 0
        || (c.regime == 2
            && (c.breakout_score >= TACTICAL_CONTINUATION_CONFIRM_BREAKOUT_MIN
                || noisy_relaunch_tailwind))
        || mean_reversion_relaunch_tailwind;

    continuation_tailwind && (structural_tailwind || confirmation_tailwind) && regime_tailwind
}

fn apply_uncertain_headline_policy(
    report_bucket: &mut String,
    report_reason: &mut String,
    headline_reason: &str,
    tactical_candidate: bool,
    tactical_slots_remaining: &mut usize,
    range_core_candidate: bool,
    range_core_slots_remaining: &mut usize,
) {
    if *report_bucket == "RADAR" {
        return;
    }

    if tactical_candidate && *tactical_slots_remaining > 0 {
        *report_bucket = TACTICAL_CONTINUATION_BUCKET.to_string();
        *report_reason = format!(
            "Headline Gate=uncertain：{}；市场不 headline 单边，但该标的 continuation/执行评分仍过线，仅保留战术续涨名额（小仓位、硬止损、不得机械追高）",
            headline_reason
        );
        *tactical_slots_remaining -= 1;
        return;
    }

    if *report_bucket == "CORE BOOK" {
        if range_core_candidate && *range_core_slots_remaining > 0 {
            *report_bucket = RANGE_CORE_BUCKET.to_string();
            *report_reason = format!(
                "Headline Gate=uncertain：{}；市场未到趋势主书，但该标的综合/执行仍过线，保留为区间主书（条件式做多、轻仓、等确认，不代表全面转多）",
                headline_reason
            );
            *range_core_slots_remaining -= 1;
            return;
        }

        *report_bucket = "THEME ROTATION".to_string();
        *report_reason = if tactical_candidate {
            format!(
                "Headline Gate=uncertain：{}；该标的具备续涨条件，但战术名额已满，回退为主题轮动观察",
                headline_reason
            )
        } else if range_core_candidate {
            format!(
                "Headline Gate=uncertain：{}；该标的本可保留区间主书，但 range-core 名额已满，回退为主题轮动观察",
                headline_reason
            )
        } else {
            format!(
                "Headline Gate=uncertain：{}；主书降级为主题轮动观察",
                headline_reason
            )
        };
    } else if *report_bucket == "THEME ROTATION" {
        *report_reason = if tactical_candidate {
            format!(
                "Headline Gate=uncertain：{}；该标的续涨逻辑成立，但战术名额已满，只保留条件式观察",
                headline_reason
            )
        } else {
            "Headline Gate=uncertain：只保留条件式观察，不形成主书方向".to_string()
        };
    }
}

fn shadow_execution_blocked(c: &Candidate) -> bool {
    c.shadow_rank_score >= POSITIVE_SHADOW_THRESHOLD
        && (c.execution_mode != "executable" || c.execution_score < EXECUTION_ACTIONABLE_MIN_SCORE)
}

fn front_rank_penalty(c: &Candidate) -> f64 {
    let shadow_blocked = shadow_execution_blocked(c);
    match c.execution_mode.as_str() {
        "do_not_chase" => {
            let mut penalty = 0.22;
            if shadow_blocked {
                penalty += 0.12;
            }
            penalty
        }
        "wait_pullback" => {
            let mut penalty = 0.08;
            if shadow_blocked {
                penalty += 0.06;
            }
            penalty
        }
        _ => {
            if shadow_blocked {
                0.10
            } else if c.execution_score < 0.45 {
                0.04
            } else {
                0.0
            }
        }
    }
}

// ── Data loading ────────────────────────────────────────────────────────────

fn load_candidates(
    db: &Connection,
    effective_dates: &AnalyticsAsOfDates,
) -> Result<Vec<Candidate>> {
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
            COALESCE(a_lf.value, 0) AS lab_factor,
            a_lf.detail AS lab_detail,
            COALESCE(a_s30.value, 0) AS shadow_iv_30d,
            COALESCE(a_s60.value, 0) AS shadow_iv_60d,
            COALESCE(a_s90.value, 0) AS shadow_iv_90d,
            COALESCE(a_sds.value, 0) AS downside_stress,
            a_s30.detail AS shadow_detail,
            COALESCE(a_sp90.value, 0) AS shadow_put_90_3m,
            COALESCE(a_st90.value, 0) AS shadow_touch_90_3m,
            COALESCE(a_sf1.value, 0) AS shadow_floor_1sigma_3m,
            COALESCE(a_sf2.value, 0) AS shadow_floor_2sigma_3m,
            COALESCE(a_ssk.value, 0) AS shadow_skew_90_3m,
            COALESCE(a_ss.value, 0) AS setup_score,
            COALESCE(a_sd.value, 0) AS setup_direction,
            COALESCE(a_cs.value, 0) AS continuation_score,
            COALESCE(a_fade.value, 0) AS fade_risk,
            COALESCE(a_cd.value, 0) AS continuation_direction,
            COALESCE(a_es.value, 0) AS execution_score,
            COALESCE(a_mcg.value, 0) AS max_chase_gap_pct,
            COALESCE(a_ptp.value, 0) AS pullback_trigger_pct,
            a_es.detail AS execution_detail,
            COALESCE(a_soap.value, 0) AS shadow_option_alpha_prob,
            COALESCE(a_scr.value, 0) AS stale_chase_risk,
            COALESCE(a_eq.value, 0) AS entry_quality_score,
            COALESCE(a_cb.value, 0) AS calibration_bucket_id,
            a_soap.detail AS shadow_option_alpha_detail
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
        LEFT JOIN analytics a_s30 ON lp.ts_code = a_s30.ts_code AND a_s30.as_of = ? AND a_s30.module = 'shadow_fast' AND a_s30.metric = 'shadow_iv_30d'
        LEFT JOIN analytics a_s60 ON lp.ts_code = a_s60.ts_code AND a_s60.as_of = ? AND a_s60.module = 'shadow_fast' AND a_s60.metric = 'shadow_iv_60d'
        LEFT JOIN analytics a_s90 ON lp.ts_code = a_s90.ts_code AND a_s90.as_of = ? AND a_s90.module = 'shadow_fast' AND a_s90.metric = 'shadow_iv_90d'
        LEFT JOIN analytics a_sds ON lp.ts_code = a_sds.ts_code AND a_sds.as_of = ? AND a_sds.module = 'shadow_fast' AND a_sds.metric = 'downside_stress'
        LEFT JOIN analytics a_sp90 ON lp.ts_code = a_sp90.ts_code AND a_sp90.as_of = ? AND a_sp90.module = 'shadow_full' AND a_sp90.metric = 'shadow_put_90_3m'
        LEFT JOIN analytics a_st90 ON lp.ts_code = a_st90.ts_code AND a_st90.as_of = ? AND a_st90.module = 'shadow_full' AND a_st90.metric = 'shadow_touch_90_3m'
        LEFT JOIN analytics a_sf1 ON lp.ts_code = a_sf1.ts_code AND a_sf1.as_of = ? AND a_sf1.module = 'shadow_full' AND a_sf1.metric = 'shadow_floor_1sigma_3m'
        LEFT JOIN analytics a_sf2 ON lp.ts_code = a_sf2.ts_code AND a_sf2.as_of = ? AND a_sf2.module = 'shadow_full' AND a_sf2.metric = 'shadow_floor_2sigma_3m'
        LEFT JOIN analytics a_ssk ON lp.ts_code = a_ssk.ts_code AND a_ssk.as_of = ? AND a_ssk.module = 'shadow_full' AND a_ssk.metric = 'shadow_skew_90_3m'
        LEFT JOIN analytics a_ss ON lp.ts_code = a_ss.ts_code AND a_ss.as_of = ? AND a_ss.module = 'setup_alpha' AND a_ss.metric = 'setup_score'
        LEFT JOIN analytics a_sd ON lp.ts_code = a_sd.ts_code AND a_sd.as_of = ? AND a_sd.module = 'setup_alpha' AND a_sd.metric = 'setup_direction'
        LEFT JOIN analytics a_cs ON lp.ts_code = a_cs.ts_code AND a_cs.as_of = ? AND a_cs.module = 'continuation_vs_fade' AND a_cs.metric = 'continuation_score'
        LEFT JOIN analytics a_fade ON lp.ts_code = a_fade.ts_code AND a_fade.as_of = ? AND a_fade.module = 'continuation_vs_fade' AND a_fade.metric = 'fade_risk'
        LEFT JOIN analytics a_cd ON lp.ts_code = a_cd.ts_code AND a_cd.as_of = ? AND a_cd.module = 'continuation_vs_fade' AND a_cd.metric = 'continuation_direction'
        LEFT JOIN analytics a_es ON lp.ts_code = a_es.ts_code AND a_es.as_of = ? AND a_es.module = 'open_execution_gate' AND a_es.metric = 'execution_score'
        LEFT JOIN analytics a_mcg ON lp.ts_code = a_mcg.ts_code AND a_mcg.as_of = ? AND a_mcg.module = 'open_execution_gate' AND a_mcg.metric = 'max_chase_gap_pct'
        LEFT JOIN analytics a_ptp ON lp.ts_code = a_ptp.ts_code AND a_ptp.as_of = ? AND a_ptp.module = 'open_execution_gate' AND a_ptp.metric = 'pullback_trigger_pct'
        LEFT JOIN analytics a_soap ON lp.ts_code = a_soap.ts_code AND a_soap.as_of = ? AND a_soap.module = 'shadow_option_alpha' AND a_soap.metric = 'shadow_alpha_prob'
        LEFT JOIN analytics a_scr ON lp.ts_code = a_scr.ts_code AND a_scr.as_of = ? AND a_scr.module = 'shadow_option_alpha' AND a_scr.metric = 'stale_chase_risk'
        LEFT JOIN analytics a_eq ON lp.ts_code = a_eq.ts_code AND a_eq.as_of = ? AND a_eq.module = 'shadow_option_alpha' AND a_eq.metric = 'entry_quality_score'
        LEFT JOIN analytics a_cb ON lp.ts_code = a_cb.ts_code AND a_cb.as_of = ? AND a_cb.module = 'shadow_option_alpha' AND a_cb.metric = 'calibration_bucket'
        WHERE lp.ts_code NOT LIKE '688%'
    ";

    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map(
        duckdb::params![
            &effective_dates.prices, // ranked CTE
            // momentum / flow / event / unlock / regime / reversion / breakout (13)
            &effective_dates.momentum,
            &effective_dates.momentum,
            &effective_dates.flow,
            &effective_dates.announcement,
            &effective_dates.announcement,
            &effective_dates.unlock,
            &effective_dates.unlock,
            &effective_dates.unlock,
            &effective_dates.momentum,
            &effective_dates.mean_reversion,
            &effective_dates.mean_reversion,
            &effective_dates.breakout,
            &effective_dates.breakout,
            // factor lab (1)
            &effective_dates.lab_factor,
            // RSI / BB (2)
            &effective_dates.mean_reversion,
            &effective_dates.mean_reversion,
            // shadow_fast (4)
            &effective_dates.shadow_fast,
            &effective_dates.shadow_fast,
            &effective_dates.shadow_fast,
            &effective_dates.shadow_fast,
            // shadow_full (5)
            &effective_dates.shadow_full,
            &effective_dates.shadow_full,
            &effective_dates.shadow_full,
            &effective_dates.shadow_full,
            &effective_dates.shadow_full,
            // setup / continuation / execution (8)
            &effective_dates.setup_alpha,
            &effective_dates.setup_alpha,
            &effective_dates.continuation_vs_fade,
            &effective_dates.continuation_vs_fade,
            &effective_dates.continuation_vs_fade,
            &effective_dates.open_execution_gate,
            &effective_dates.open_execution_gate,
            &effective_dates.open_execution_gate,
            &effective_dates.shadow_option_alpha,
            &effective_dates.shadow_option_alpha,
            &effective_dates.shadow_option_alpha,
            &effective_dates.shadow_option_alpha,
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
                lab_trade_date: row
                    .get::<_, Option<String>>(19)?
                    .and_then(|detail| serde_json::from_str::<serde_json::Value>(&detail).ok())
                    .and_then(|obj| {
                        obj.get("trade_date")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
                lab_is_fresh: false,
                shadow_iv_30d: row.get::<_, f64>(20).unwrap_or(0.0),
                shadow_iv_60d: row.get::<_, f64>(21).unwrap_or(0.0),
                shadow_iv_90d: row.get::<_, f64>(22).unwrap_or(0.0),
                downside_stress: row.get::<_, f64>(23).unwrap_or(0.0),
                shadow_proxy: row
                    .get::<_, Option<String>>(24)?
                    .and_then(|detail| serde_json::from_str::<serde_json::Value>(&detail).ok())
                    .and_then(|obj| {
                        obj.get("proxy_label")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    }),
                shadow_put_90_3m: row.get::<_, f64>(25).unwrap_or(0.0),
                shadow_touch_90_3m: row.get::<_, f64>(26).unwrap_or(0.0),
                shadow_floor_1sigma_3m: row.get::<_, Option<f64>>(27)?,
                shadow_floor_2sigma_3m: row.get::<_, Option<f64>>(28)?,
                shadow_skew_90_3m: row.get::<_, f64>(29).unwrap_or(0.0),
                shadow_alpha_score: 0.0,
                shadow_rank_score: 0.0,
                setup_score: row.get::<_, f64>(30).unwrap_or(0.0),
                setup_direction: row.get::<_, f64>(31).unwrap_or(0.0),
                continuation_score: row.get::<_, f64>(32).unwrap_or(0.0),
                fade_risk: row.get::<_, f64>(33).unwrap_or(0.0),
                continuation_direction: row.get::<_, f64>(34).unwrap_or(0.0),
                execution_score: row.get::<_, f64>(35).unwrap_or(0.0),
                max_chase_gap_pct: row.get::<_, f64>(36).unwrap_or(0.0),
                pullback_trigger_pct: row.get::<_, f64>(37).unwrap_or(0.0),
                pullback_price: row
                    .get::<_, Option<String>>(38)?
                    .and_then(|detail| serde_json::from_str::<serde_json::Value>(&detail).ok())
                    .and_then(|obj| obj.get("pullback_price").and_then(|v| v.as_f64())),
                execution_mode: row
                    .get::<_, Option<String>>(38)?
                    .and_then(|detail| serde_json::from_str::<serde_json::Value>(&detail).ok())
                    .and_then(|obj| {
                        obj.get("execution_mode")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| "executable".to_string()),
                calibrated_shadow_alpha_prob: row.get::<_, f64>(39).unwrap_or(0.0),
                stale_chase_risk: row.get::<_, f64>(40).unwrap_or(0.0),
                entry_quality_score: row.get::<_, f64>(41).unwrap_or(0.0),
                calibration_bucket_id: row.get::<_, f64>(42).unwrap_or(0.0),
                shadow_option_alpha_detail: row
                    .get::<_, Option<String>>(43)?
                    .and_then(|detail| serde_json::from_str::<serde_json::Value>(&detail).ok()),
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

fn load_reference_close_map(db: &Connection, date_str: &str) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    if let Ok(mut stmt) = db.prepare(
        "WITH ranked AS (
            SELECT
                ts_code,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_date DESC
                ) AS rn
            FROM prices
            WHERE trade_date <= CAST(? AS DATE)
        )
        SELECT ts_code, close
        FROM ranked
        WHERE rn = 1",
    ) {
        if let Ok(rows) = stmt.query_map(duckdb::params![date_str], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?))
        }) {
            for row in rows.flatten() {
                if let Some(close) = row.1 {
                    map.insert(row.0, close);
                }
            }
        }
    }
    map
}

fn resolve_market_as_of(db: &Connection, as_of: NaiveDate) -> NaiveDate {
    db.query_row(
        "SELECT CAST(MAX(trade_date) AS VARCHAR) FROM prices WHERE trade_date <= CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
    .and_then(|raw| {
        let trimmed = raw.trim();
        let date_part = trimmed.get(0..10).unwrap_or(trimmed);
        NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
    })
    .unwrap_or(as_of)
}

fn resolve_module_as_of(db: &Connection, as_of: NaiveDate, module: &str) -> NaiveDate {
    db.query_row(
        "SELECT CAST(MAX(as_of) AS VARCHAR) FROM analytics WHERE as_of <= CAST(? AS DATE) AND module = ?",
        duckdb::params![as_of.to_string(), module],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
    .and_then(|raw| {
        let trimmed = raw.trim();
        let date_part = trimmed.get(0..10).unwrap_or(trimmed);
        NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
    })
    .unwrap_or(as_of)
}

fn resolve_sector_flow_as_of(db: &Connection, as_of: NaiveDate) -> NaiveDate {
    db.query_row(
        "SELECT CAST(MAX(trade_date) AS VARCHAR) FROM sector_fund_flow WHERE trade_date <= CAST(? AS DATE)",
        duckdb::params![as_of.to_string()],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
    .and_then(|raw| {
        let trimmed = raw.trim();
        let date_part = trimmed.get(0..10).unwrap_or(trimmed);
        NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
    })
    .unwrap_or(as_of)
}

fn resolve_analytics_as_of_dates(db: &Connection, as_of: NaiveDate) -> AnalyticsAsOfDates {
    AnalyticsAsOfDates {
        prices: resolve_market_as_of(db, as_of).to_string(),
        flow: resolve_module_as_of(db, as_of, "flow").to_string(),
        announcement: resolve_module_as_of(db, as_of, "announcement").to_string(),
        unlock: resolve_module_as_of(db, as_of, "unlock").to_string(),
        momentum: resolve_module_as_of(db, as_of, "momentum").to_string(),
        mean_reversion: resolve_module_as_of(db, as_of, "mean_reversion").to_string(),
        breakout: resolve_module_as_of(db, as_of, "breakout").to_string(),
        lab_factor: resolve_module_as_of(db, as_of, "lab_factor").to_string(),
        shadow_fast: resolve_module_as_of(db, as_of, "shadow_fast").to_string(),
        shadow_full: resolve_module_as_of(db, as_of, "shadow_full").to_string(),
        setup_alpha: resolve_module_as_of(db, as_of, "setup_alpha").to_string(),
        continuation_vs_fade: resolve_module_as_of(db, as_of, "continuation_vs_fade").to_string(),
        open_execution_gate: resolve_module_as_of(db, as_of, "open_execution_gate").to_string(),
        shadow_option_alpha: resolve_module_as_of(db, as_of, "shadow_option_alpha").to_string(),
        macro_gate: resolve_module_as_of(db, as_of, "macro_gate").to_string(),
        sector_flow: resolve_sector_flow_as_of(db, as_of).to_string(),
    }
}

fn signed_direction_label(v: f64) -> &'static str {
    if v > 0.0 {
        "bullish"
    } else if v < 0.0 {
        "bearish"
    } else {
        "neutral"
    }
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

fn compute_shadow_alpha_score(c: &Candidate) -> f64 {
    let has_shadow_fast = c.shadow_iv_30d > 0.0 || c.shadow_iv_60d > 0.0 || c.shadow_iv_90d > 0.0;
    let has_shadow_full = c.shadow_put_90_3m > 0.0
        || c.shadow_touch_90_3m > 0.0
        || c.shadow_floor_1sigma_3m.unwrap_or(0.0) > 0.0
        || c.shadow_floor_2sigma_3m.unwrap_or(0.0) > 0.0
        || c.shadow_skew_90_3m > 0.0;

    if !has_shadow_fast && !has_shadow_full {
        return 0.0;
    }

    let stability = (1.0 - c.downside_stress).clamp(0.0, 1.0);
    let touch_comfort = if c.shadow_touch_90_3m > 0.0 {
        (1.0 - c.shadow_touch_90_3m).clamp(0.0, 1.0)
    } else {
        stability
    };
    let skew_comfort = if c.shadow_skew_90_3m > 0.0 {
        (1.0 - (c.shadow_skew_90_3m / 10.0)).clamp(0.0, 1.0)
    } else {
        stability
    };
    let tail_gap_comfort = match (c.shadow_floor_1sigma_3m, c.shadow_floor_2sigma_3m) {
        (Some(f1), Some(f2)) if f1 > 0.0 && f2 > 0.0 => {
            let tail_gap = ((f1 - f2).abs() / f1.max(1.0)).clamp(0.0, 1.0);
            (1.0 - tail_gap).clamp(0.0, 1.0)
        }
        _ => stability,
    };
    let put_cost_comfort = if c.shadow_put_90_3m > 0.0 {
        (1.0 - (c.shadow_put_90_3m / 0.25)).clamp(0.0, 1.0)
    } else {
        stability
    };

    (0.35 * stability
        + 0.25 * touch_comfort
        + 0.20 * skew_comfort
        + 0.10 * tail_gap_comfort
        + 0.10 * put_cost_comfort)
        .clamp(0.0, 1.0)
}

fn compute_shadow_rank_score(c: &Candidate) -> f64 {
    let directional_context = if c.setup_direction != 0.0 {
        c.setup_direction
    } else if c.continuation_direction != 0.0 {
        c.continuation_direction
    } else if c.breakout_direction != 0.0 {
        c.breakout_direction
    } else if c.reversion_direction != 0.0 {
        c.reversion_direction
    } else if c.surprise_category >= 0.0 && (c.p_upside - 0.5).abs() > 0.10 {
        if c.p_upside > 0.5 {
            1.0
        } else {
            -1.0
        }
    } else {
        0.0
    };

    if directional_context > 0.0 {
        c.shadow_alpha_score
    } else if directional_context < 0.0 {
        -0.60 * c.shadow_alpha_score
    } else {
        0.0
    }
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

#[cfg(test)]
mod tests {
    use super::{
        apply_uncertain_headline_policy, classify_convergence, classify_report_lane,
        effective_pass1_cutoff, effective_report_limit, expanded_report_pool_limit,
        is_tactical_continuation_candidate, plan_uncertain_headline_candidates,
        select_uncertain_tactical_symbols_from_candidates, Candidate, RANGE_CORE_BUCKET,
        TACTICAL_CONTINUATION_BUCKET, UNCERTAIN_TACTICAL_LIMIT_MAX,
    };
    use crate::analytics::shadow_calibration::ShadowCalibrationSummary;

    fn sample_candidate() -> Candidate {
        Candidate {
            ts_code: "000001.SZ".to_string(),
            ret_5d: 7.2,
            ret_20d: 14.5,
            trend_prob: 0.58,
            trend_prob_n: 20.0,
            information_score: 0.72,
            p_upside: 0.0,
            surprise_category: -1.0,
            p_drop: 0.0,
            unlock_days: -1.0,
            float_ratio: 0.0,
            regime: 0,
            reversion_score: 0.0,
            reversion_direction: 0.0,
            breakout_score: 0.46,
            breakout_direction: 1.0,
            rsi_14: 62.0,
            bb_position: 0.78,
            lab_factor: 0.0,
            lab_trade_date: None,
            lab_is_fresh: false,
            shadow_iv_30d: 0.0,
            shadow_iv_60d: 0.0,
            shadow_iv_90d: 0.0,
            downside_stress: 0.24,
            shadow_proxy: None,
            shadow_put_90_3m: 0.0,
            shadow_touch_90_3m: 0.0,
            shadow_floor_1sigma_3m: None,
            shadow_floor_2sigma_3m: None,
            shadow_skew_90_3m: 0.0,
            shadow_alpha_score: 0.66,
            shadow_rank_score: 0.61,
            setup_score: 0.58,
            setup_direction: 1.0,
            continuation_score: 0.67,
            fade_risk: 0.34,
            continuation_direction: 1.0,
            execution_score: 0.59,
            max_chase_gap_pct: 3.2,
            pullback_trigger_pct: 1.4,
            pullback_price: Some(10.5),
            execution_mode: "wait_pullback".to_string(),
            calibrated_shadow_alpha_prob: 0.0,
            stale_chase_risk: 0.0,
            entry_quality_score: 0.0,
            calibration_bucket_id: 0.0,
            shadow_option_alpha_detail: None,
            magnitude_score: 0.62,
            momentum_score: 0.38,
            reversion_s: 0.0,
            breakout_s: 0.47,
            information_s: 0.74,
            event_score: 0.0,
            cross_asset_score: 0.18,
            composite: 0.64,
        }
    }

    fn sample_shadow_summary() -> ShadowCalibrationSummary {
        ShadowCalibrationSummary {
            ignored_positive_missed_rate: 0.30,
            recall_gap: 0.06,
            ..ShadowCalibrationSummary::default()
        }
    }

    #[test]
    fn tactical_candidate_requires_bullish_continuation_and_execution_quality() {
        let base = sample_candidate();
        assert!(is_tactical_continuation_candidate(
            &base, "MODERATE", "bullish"
        ));

        let mut chinext = sample_candidate();
        chinext.ts_code = "300999.SZ".to_string();
        assert!(!is_tactical_continuation_candidate(
            &chinext, "MODERATE", "bullish"
        ));

        let mut stretched = sample_candidate();
        stretched.execution_mode = "do_not_chase".to_string();
        assert!(!is_tactical_continuation_candidate(
            &stretched, "MODERATE", "bullish"
        ));

        let mut mean_reversion = sample_candidate();
        mean_reversion.regime = 1;
        mean_reversion.breakout_score = 0.30;
        mean_reversion.execution_score = 0.52;
        mean_reversion.fade_risk = 0.42;
        assert!(!is_tactical_continuation_candidate(
            &mean_reversion,
            "MODERATE",
            "bullish"
        ));
    }

    #[test]
    fn structural_follow_through_can_promote_watch_to_moderate() {
        let mut candidate = sample_candidate();
        candidate.momentum_score = 0.0;
        candidate.breakout_score = 0.0;
        candidate.breakout_direction = 0.0;
        candidate.reversion_direction = 1.0;
        candidate.reversion_score = 0.0;
        candidate.information_s = 0.88;
        candidate.setup_score = 0.48;
        candidate.continuation_score = 0.43;
        candidate.execution_score = 0.61;
        candidate.execution_mode = "executable".to_string();
        candidate.shadow_rank_score = 0.54;
        candidate.composite = 0.36;

        let (confidence, direction) = classify_convergence(&candidate);
        assert!(confidence == "MODERATE" || confidence == "HIGH");
        assert_eq!(direction, "bullish");
    }

    #[test]
    fn tactical_candidate_can_keep_strong_watch_names_alive() {
        let mut candidate = sample_candidate();
        candidate.setup_score = 0.47;
        candidate.continuation_score = 0.44;
        candidate.execution_score = 0.61;
        candidate.composite = 0.35;

        assert!(is_tactical_continuation_candidate(
            &candidate, "WATCH", "bullish"
        ));
    }

    #[test]
    fn mean_reversion_relaunch_can_qualify_for_tactical_continuation() {
        let mut candidate = sample_candidate();
        candidate.regime = 1;
        candidate.setup_score = 0.63;
        candidate.continuation_score = 0.54;
        candidate.execution_score = 0.62;
        candidate.fade_risk = 0.24;
        candidate.breakout_score = 0.20;
        candidate.breakout_direction = 0.0;
        candidate.information_s = 0.76;

        assert!(is_tactical_continuation_candidate(
            &candidate, "MODERATE", "bullish"
        ));
    }

    #[test]
    fn uncertain_tactical_selection_prioritizes_high_quality_relaunches() {
        let mut noisy = sample_candidate();
        noisy.ts_code = "NOISY.SZ".to_string();
        noisy.regime = 2;
        noisy.breakout_score = 0.10;
        noisy.breakout_direction = 0.0;
        noisy.setup_score = 0.69;
        noisy.continuation_score = 0.57;
        noisy.execution_score = 0.64;
        noisy.fade_risk = 0.17;
        noisy.information_s = 0.85;
        noisy.composite = 0.52;

        let mut mean_rev = sample_candidate();
        mean_rev.ts_code = "MEANREV.SZ".to_string();
        mean_rev.regime = 1;
        mean_rev.breakout_score = 0.15;
        mean_rev.breakout_direction = 0.0;
        mean_rev.setup_score = 0.76;
        mean_rev.continuation_score = 0.55;
        mean_rev.execution_score = 0.60;
        mean_rev.fade_risk = 0.26;
        mean_rev.information_s = 0.69;
        mean_rev.event_score = 0.58;
        mean_rev.composite = 0.42;

        let mut mean_rev_2 = sample_candidate();
        mean_rev_2.ts_code = "MEANREV2.SZ".to_string();
        mean_rev_2.regime = 1;
        mean_rev_2.breakout_score = 0.18;
        mean_rev_2.breakout_direction = 0.0;
        mean_rev_2.setup_score = 0.62;
        mean_rev_2.continuation_score = 0.61;
        mean_rev_2.execution_score = 0.64;
        mean_rev_2.fade_risk = 0.24;
        mean_rev_2.information_s = 0.93;
        mean_rev_2.composite = 0.41;

        let selected = select_uncertain_tactical_symbols_from_candidates(&[
            noisy.clone(),
            mean_rev.clone(),
            mean_rev_2.clone(),
        ]);

        assert_eq!(selected.len(), 3);
        assert!(selected.contains("MEANREV.SZ"));
        assert!(selected.contains("MEANREV2.SZ"));
        assert!(selected.contains("NOISY.SZ"));
    }

    #[test]
    fn uncertain_headline_plan_can_preserve_range_core_slots() {
        let mut core_a = sample_candidate();
        core_a.ts_code = "COREA.SZ".to_string();
        core_a.execution_mode = "executable".to_string();
        core_a.composite = 0.56;
        core_a.execution_score = 0.68;
        core_a.setup_score = 0.61;
        core_a.continuation_score = 0.35;
        core_a.continuation_direction = 0.0;
        core_a.fade_risk = 0.18;
        core_a.information_s = 0.82;
        core_a.event_score = 0.24;

        let mut core_b = core_a.clone();
        core_b.ts_code = "COREB.SZ".to_string();
        core_b.composite = 0.51;
        core_b.execution_score = 0.63;
        core_b.information_s = 0.78;

        let plan = plan_uncertain_headline_candidates(&[core_a, core_b]);
        assert!(plan.range_core_limit >= 1);
        assert!(plan.range_core_symbols.contains("COREA.SZ"));
    }

    #[test]
    fn chinext_bullish_name_is_demoted_out_of_actionable_buckets() {
        let mut candidate = sample_candidate();
        candidate.ts_code = "300363.SZ".to_string();
        candidate.execution_mode = "executable".to_string();

        let (bucket, reason) = classify_report_lane(&candidate, "HIGH", "bullish");
        assert_eq!(bucket, "THEME ROTATION");
        assert!(reason.contains("创业板"));
    }

    #[test]
    fn uncertain_policy_caps_tactical_slots() {
        let mut bucket_a = "THEME ROTATION".to_string();
        let mut reason_a = String::new();
        let mut slots = UNCERTAIN_TACTICAL_LIMIT_MAX;
        let mut range_slots = 0usize;
        apply_uncertain_headline_policy(
            &mut bucket_a,
            &mut reason_a,
            "edge weak",
            true,
            &mut slots,
            false,
            &mut range_slots,
        );
        assert_eq!(bucket_a, TACTICAL_CONTINUATION_BUCKET);
        assert_eq!(slots, UNCERTAIN_TACTICAL_LIMIT_MAX - 1);

        let mut bucket_b = "CORE BOOK".to_string();
        let mut reason_b = String::new();
        let mut exhausted_slots = 0usize;
        let mut exhausted_range_slots = 0usize;
        apply_uncertain_headline_policy(
            &mut bucket_b,
            &mut reason_b,
            "edge weak",
            true,
            &mut exhausted_slots,
            false,
            &mut exhausted_range_slots,
        );
        assert_eq!(bucket_b, "THEME ROTATION");
        assert!(reason_b.contains("战术名额已满"));
    }

    #[test]
    fn uncertain_policy_can_preserve_range_core_bucket() {
        let mut bucket = "CORE BOOK".to_string();
        let mut reason = String::new();
        let mut tactical_slots = 0usize;
        let mut range_slots = 1usize;
        apply_uncertain_headline_policy(
            &mut bucket,
            &mut reason,
            "edge weak",
            false,
            &mut tactical_slots,
            true,
            &mut range_slots,
        );
        assert_eq!(bucket, RANGE_CORE_BUCKET);
        assert_eq!(range_slots, 0);
        assert!(reason.contains("区间主书"));
    }

    #[test]
    fn recall_pressure_expands_pass1_and_final_limits() {
        let summary = sample_shadow_summary();
        let report_limit = effective_report_limit(30, &summary);
        let pool_limit = expanded_report_pool_limit(report_limit, &summary);

        assert!(effective_pass1_cutoff(&summary) > 120);
        assert!(report_limit > 30);
        assert!(pool_limit > report_limit);
        assert!(pool_limit <= effective_pass1_cutoff(&summary));
    }
}
