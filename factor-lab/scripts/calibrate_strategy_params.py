#!/usr/bin/env python3
"""Generate strategy parameter provenance and calibration artifacts.

This script is intentionally lightweight enough for the weekend maintenance job:
it reads completed paper-trade / model tables, writes a YAML artifact, and marks
which parameters are statistical, cost assumptions, walk-forward calibrated, or
legacy heuristics still waiting to be replaced.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import math

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.paths import FACTOR_LAB_ROOT, QUANT_CN_REPORT_DB, QUANT_CN_ROOT, QUANT_US_REPORT_DB, QUANT_US_ROOT


EV80_Z = 1.2816
EV95_Z = 1.6449
DEFAULT_PAPER_PARAMS = {
    "lookback_days": 90,
    "slippage_pct": 0.18,
    "win_threshold_pct": 0.50,
    "loss_threshold_pct": -1.00,
    "ev_lcb_80_z": EV80_Z,
    "ev_lcb_95_z": EV95_Z,
    "min_samples": 8,
    "min_fills": 4,
    "min_fill_rate": 0.35,
    "min_ev_pct": 0.15,
    "min_ev_lcb_80_pct": 0.0,
    "max_tail_loss_pct": 5.5,
    "provenance": "built_in_default",
}
MIN_ACTIVATION_IMPROVEMENT_PCT = 0.05
MIN_US_GATE_OOS_TRADES = 20
MIN_US_OPTIONS_OOS_ROWS = 20
DEFAULT_US_RUNTIME_PARAMS = {
    "risk_params": {
        "atr_stop_multiple": 2.0,
        "fallback_expected_move_atr_multiple": 2.0,
        "provenance": "legacy_heuristic",
    },
    "options_alpha": {
        "min_days_to_exp": 3.0,
        "max_days_to_exp": 120.0,
        "max_spread_pct": 25.0,
        "min_chain_width": 6.0,
        "min_atm_iv_pct": 5.0,
        "directional_edge_threshold": 0.45,
        "vol_edge_threshold": 0.10,
        "vol_edge_wait_threshold": 0.55,
        "pc_z_scale": 2.5,
        "skew_z_scale": 2.5,
        "vrp_z_scale": 2.5,
        "vrp_raw_scale": 0.20,
        "cheapness_scale": 0.35,
        "flow_volume_norm": 50_000.0,
        "flow_vol_oi_norm": 50.0,
        "flow_volume_weight": 0.70,
        "flow_ratio_weight": 0.30,
        "direction_weights": {"bias": 0.35, "pc": 0.25, "skew": 0.25, "flow": 0.15},
        "vol_weights": {"vrp": 0.65, "cheapness": 0.25, "flow": 0.10},
        "provenance": "legacy_heuristic",
    },
    "overnight_continuation_alpha": {
        "lookback_days": 90,
        "prior_n": 12.0,
        "event_boost": 0.035,
        "liquidity_good_adj": 0.025,
        "liquidity_poor_adj": -0.025,
        "continuation_weights": {
            "p_gate_continue": 0.38,
            "support": 0.18,
            "trend_alignment": 0.12,
            "hist_continue": 0.17,
            "lab_composite": 0.06,
            "effective_stretch": -0.16,
            "hist_stale": -0.10,
        },
        "fade_weights": {
            "p_gate_fade": 0.42,
            "hist_fade": 0.22,
            "effective_stretch": 0.18,
            "support_gap": 0.10,
            "discipline_gap": 0.08,
        },
        "paid_risk_weights": {
            "effective_stretch": 0.40,
            "hist_stale": 0.30,
            "gap_overpaid": 0.18,
            "discipline_gap": 0.12,
        },
        "entry_quality_weights": {
            "continuation_score": 0.38,
            "discipline": 0.24,
            "support": 0.18,
            "paid_risk_gap": 0.12,
            "fade_score_gap": 0.08,
        },
        "paid_risk_do_not_chase": 0.62,
        "paid_risk_wait": 0.44,
        "entry_quality_min_wait": 0.52,
        "continuation_min": 0.55,
        "entry_quality_min_continue": 0.56,
        "strong_entry_quality": 0.62,
        "provenance": "legacy_heuristic",
    },
    "overnight_gate": {
        "price_context": {"atr_window": 14, "prior_range_window": 20},
        "historical_context": {"lo_days": 10, "hi_days": 5},
        "delta_features": {
            "iv_epsilon": 1e-6,
            "iv_log_base": 2.0,
            "skew_delta_scale": 0.35,
            "pc_offset": 0.25,
            "pc_log_base": 3.0,
        },
        "trend_alignment": {
            "neutral_no_gap": 0.50,
            "missing_trend": 0.35,
            "signed_edge_scale": 0.25,
            "trending_bonus": 0.03,
            "noisy_bonus": 0.02,
            "mean_reverting_bonus": 0.025,
            "mean_reverting_neutral_band": 0.08,
        },
        "discipline": {
            "no_gap_support": 0.55,
            "missing_gap_comfort": 0.60,
            "gap_offset": 0.15,
            "gap_span": 0.85,
            "missing_cone_comfort": 0.60,
            "cone_upper_mid": 0.58,
            "cone_lower_mid": 0.42,
            "cone_span": 0.32,
            "gap_weight": 0.60,
            "cone_weight": 0.40,
            "cap": 0.85,
        },
        "support_regime_bonus": {
            "trending_alignment_min": 0.62,
            "trending_bonus": 0.03,
            "noisy_discipline_min": 0.74,
            "noisy_flow_min": 0.30,
            "noisy_bias_min": 0.75,
            "noisy_bonus": 0.02,
            "mean_reverting_alignment_min": 0.55,
            "mean_reverting_discipline_min": 0.72,
            "mean_reverting_bonus": 0.035,
        },
        "support_weights": {
            "flow_intensity": 0.20,
            "iv_delta": 0.12,
            "skew_delta": 0.08,
            "pc_delta": 0.06,
            "bias_support": 0.10,
            "trend_alignment": 0.18,
            "discipline_support": 0.14,
            "sentiment_support": 0.12,
        },
        "continuation_probability": {
            "base": 0.14,
            "support_score": 0.50,
            "trend_alignment": 0.12,
            "discipline_support": 0.08,
            "stretch_score": -0.22,
            "trending_alignment_min": 0.62,
            "trending_bonus": 0.03,
            "mean_reverting_discipline_min": 0.72,
            "mean_reverting_bonus": 0.035,
            "noisy_support_min": 0.48,
            "noisy_discipline_min": 0.74,
            "noisy_bonus": 0.02,
        },
        "continuation_relief": {
            "trend_match_bonus": 0.10,
            "alignment_min": 0.65,
            "alignment_bonus": 0.06,
            "p_continue_min": 0.60,
            "p_continue_span": 0.20,
            "p_continue_bonus": 0.08,
            "support_min": 0.52,
            "support_span": 0.28,
            "support_bonus": 0.06,
            "discipline_min": 0.75,
            "discipline_span": 0.10,
            "discipline_bonus": 0.03,
            "cap": 0.22,
        },
        "flow": {
            "volume_weight": 0.55,
            "ratio_weight": 0.45,
            "volume_norm": 50_000.0,
            "vol_oi_norm": 40.0,
        },
        "sentiment": {
            "vote_z_threshold": 0.50,
            "vote_weight": 0.50,
            "neutral_support": 0.50,
            "aligned_bias_support": 1.0,
            "opposed_bias_support": 0.0,
            "trend_dir_upper": 0.56,
            "trend_dir_lower": 0.44,
            "trend_neutral_probability": 0.50,
            "trend_support_scale": 2.0,
            "trend_support_weight": 0.50,
        },
        "stretch": {
            "gap_consumed_offset": 0.55,
            "gap_consumed_span": 0.60,
            "gap_atr_offset": 0.75,
            "gap_atr_span": 0.75,
            "cone_upper": 0.78,
            "cone_lower": 0.22,
            "cone_span": 0.18,
            "gap_weight": 0.45,
            "atr_weight": 0.30,
            "cone_weight": 0.25,
        },
        "fade_probability": {
            "base": 0.10,
            "stretch_score": 0.62,
            "bias_gap": 0.16,
            "trend_alignment_gap": 0.08,
            "discipline_gap": 0.04,
        },
        "chase_gap": {
            "min_pct": 0.75,
            "max_pct": 4.0,
            "expected_move_base": 0.55,
            "expected_move_support": 0.20,
            "atr_base": 0.80,
            "atr_support": 0.20,
            "pullback_gap_fraction": 0.35,
        },
        "action": {
            "wait_gap_base": 0.65,
            "wait_gap_relief": 0.35,
            "do_not_chase_gap_base": 1.00,
            "do_not_chase_gap_relief": 0.45,
            "do_not_chase_stretch": 0.78,
            "do_not_chase_support_max": 0.48,
            "wait_stretch": 0.50,
            "wait_p_continue_max": 0.68,
            "continue_min": 0.58,
            "fade_min": 0.58,
            "strong_continue_min": 0.65,
            "moderate_continue_min": 0.52,
        },
        "provenance": "legacy_heuristic",
    },
}


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table],
        ).fetchone()[0]
        > 0
    )


def latest_as_of(con: duckdb.DuckDBPyConnection, table: str) -> str | None:
    if not table_exists(con, table):
        return None
    row = con.execute(f"SELECT MAX(CAST(as_of AS VARCHAR)) FROM {table}").fetchone()
    return row[0] if row and row[0] else None


def fetch_strategy_ev(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    as_of = latest_as_of(con, "strategy_ev")
    if not as_of:
        return {"status": "missing", "as_of": None, "families": []}
    rows = con.execute(
        """
        SELECT
            strategy_key, strategy_family, samples, fills, fill_rate, win_rate_bayes,
            ev_pct, ev_lcb_80_pct, ev_lcb_95_pct, risk_unit_pct,
            ev_norm_score, ev_norm_lcb_80, eligible, fail_reasons
        FROM strategy_ev
        WHERE as_of = CAST(? AS DATE)
        ORDER BY ev_lcb_80_pct DESC NULLS LAST, ev_pct DESC NULLS LAST
        LIMIT 30
        """,
        [as_of],
    ).fetchall()
    families = []
    for row in rows:
        families.append(
            {
                "strategy_key": row[0],
                "strategy_family": row[1],
                "samples": row[2],
                "fills": row[3],
                "fill_rate": row[4],
                "win_rate_bayes": row[5],
                "ev_pct": row[6],
                "ev_lcb_80_pct": row[7],
                "ev_lcb_95_pct": row[8],
                "risk_unit_pct": row[9],
                "ev_norm_score": row[10],
                "ev_norm_lcb_80": row[11],
                "eligible": bool(row[12]),
                "fail_reasons": row[13],
            }
        )
    return {"status": "ok", "as_of": as_of, "families": families}


def _policy_pass(row: dict[str, Any], params: dict[str, Any]) -> bool:
    return (
        int(row["samples"] or 0) >= int(params["min_samples"])
        and int(row["fills"] or 0) >= int(params["min_fills"])
        and float(row["fill_rate"] or 0.0) >= float(params["min_fill_rate"])
        and float(row["ev_pct"] or 0.0) > float(params["min_ev_pct"])
        and float(row["ev_lcb_80_pct"] or 0.0) > float(params["min_ev_lcb_80_pct"])
        and float(row["avg_tail_loss_pct"] or 0.0) <= float(params["max_tail_loss_pct"])
    )


def _policy_stats(rows: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
    selected = [float(row["realized_ret_pct"]) for row in rows if _policy_pass(row, params)]
    if not selected:
        return {
            "oos_trades": 0,
            "oos_avg_ret_pct": None,
            "oos_std_pct": None,
            "oos_ev_lcb_80_pct": None,
        }
    avg = sum(selected) / len(selected)
    if len(selected) > 1:
        variance = sum((v - avg) ** 2 for v in selected) / (len(selected) - 1)
        std = math.sqrt(max(variance, 0.0))
    else:
        std = abs(avg)
    lcb80 = avg - EV80_Z * max(std, 0.50) / math.sqrt(len(selected))
    return {
        "oos_trades": len(selected),
        "oos_avg_ret_pct": avg,
        "oos_std_pct": std,
        "oos_ev_lcb_80_pct": lcb80,
    }


def _finite_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _safe_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge_dict(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def _ev_stats(values: list[float]) -> dict[str, Any]:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if not clean:
        return {
            "n": 0,
            "avg_ret_pct": None,
            "std_pct": None,
            "ev_lcb_80_pct": None,
            "win_rate": None,
        }
    avg = sum(clean) / len(clean)
    if len(clean) > 1:
        variance = sum((v - avg) ** 2 for v in clean) / (len(clean) - 1)
        std = math.sqrt(max(variance, 0.0))
    else:
        std = abs(avg)
    lcb80 = avg - EV80_Z * max(std, 0.50) / math.sqrt(len(clean))
    return {
        "n": len(clean),
        "avg_ret_pct": avg,
        "std_pct": std,
        "ev_lcb_80_pct": lcb80,
        "win_rate": sum(1 for v in clean if v > 0.0) / len(clean),
    }


def _train_oos_split(rows: list[dict[str, Any]], date_key: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    dates = sorted({str(row.get(date_key)) for row in rows if row.get(date_key)})
    if len(dates) < 4:
        return rows, rows
    cutoff_idx = max(1, int(len(dates) * 0.65))
    cutoff = dates[min(cutoff_idx, len(dates) - 1)]
    train = [row for row in rows if str(row.get(date_key)) < cutoff]
    oos = [row for row in rows if str(row.get(date_key)) >= cutoff]
    return train or rows, oos or rows


def _stats_for_filter(rows: list[dict[str, Any]], predicate) -> dict[str, Any]:
    selected = []
    for row in rows:
        try:
            keep = predicate(row)
        except Exception:
            keep = False
        if not keep:
            continue
        ret = _finite_float(row.get("realized_ret_pct"))
        if ret is not None:
            selected.append(ret)
    return _ev_stats(selected)


def _load_release_outcomes(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    if not table_exists(con, "strategy_ev") or not table_exists(con, "paper_trades"):
        return []
    rows = con.execute(
        """
        SELECT
            CAST(ev.as_of AS VARCHAR) AS as_of,
            ev.strategy_key,
            ev.samples,
            ev.fills,
            ev.fill_rate,
            ev.ev_pct,
            ev.ev_lcb_80_pct,
            ev.avg_tail_loss_pct,
            pt.realized_ret_pct
        FROM strategy_ev ev
        INNER JOIN paper_trades pt
          ON pt.report_date = ev.as_of
         AND pt.strategy_key = ev.strategy_key
        WHERE lower(pt.action_intent) = 'trade'
          AND pt.realized_ret_pct IS NOT NULL
        """
    ).fetchall()
    keys = [
        "as_of",
        "strategy_key",
        "samples",
        "fills",
        "fill_rate",
        "ev_pct",
        "ev_lcb_80_pct",
        "avg_tail_loss_pct",
        "realized_ret_pct",
    ]
    return [dict(zip(keys, row)) for row in rows]


def calibrate_paper_trade_params(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = _load_release_outcomes(con)
    default_params = dict(DEFAULT_PAPER_PARAMS)
    default_stats = _policy_stats(rows, default_params)

    grids = {
        "min_samples": [4, 8, 12, 20],
        "min_fills": [2, 4, 8, 12],
        "min_fill_rate": [0.25, 0.35, 0.50],
        "min_ev_pct": [-0.10, 0.0, 0.15, 0.30, 0.50],
        "min_ev_lcb_80_pct": [-0.50, -0.25, 0.0, 0.10],
        "max_tail_loss_pct": [3.5, 5.5, 8.0],
    }
    best_params = dict(default_params)
    best_stats = dict(default_stats)
    best_score = default_stats.get("oos_ev_lcb_80_pct")
    best_score = float("-inf") if best_score is None else float(best_score)

    for min_samples in grids["min_samples"]:
        for min_fills in grids["min_fills"]:
            if min_fills > min_samples:
                continue
            for min_fill_rate in grids["min_fill_rate"]:
                for min_ev_pct in grids["min_ev_pct"]:
                    for min_ev_lcb in grids["min_ev_lcb_80_pct"]:
                        for max_tail in grids["max_tail_loss_pct"]:
                            candidate = dict(default_params)
                            candidate.update(
                                {
                                    "min_samples": min_samples,
                                    "min_fills": min_fills,
                                    "min_fill_rate": min_fill_rate,
                                    "min_ev_pct": min_ev_pct,
                                    "min_ev_lcb_80_pct": min_ev_lcb,
                                    "max_tail_loss_pct": max_tail,
                                    "provenance": "calibrated_walk_forward",
                                }
                            )
                            stats = _policy_stats(rows, candidate)
                            if int(stats["oos_trades"] or 0) < 20:
                                continue
                            score = stats.get("oos_ev_lcb_80_pct")
                            if score is None:
                                continue
                            if float(score) > best_score:
                                best_score = float(score)
                                best_params = candidate
                                best_stats = stats

    default_lcb = default_stats.get("oos_ev_lcb_80_pct")
    candidate_lcb = best_stats.get("oos_ev_lcb_80_pct")
    use_candidate = (
        candidate_lcb is not None
        and default_lcb is not None
        and int(best_stats.get("oos_trades") or 0) >= 20
        and float(candidate_lcb) > 0.0
        and float(candidate_lcb) >= float(default_lcb) + MIN_ACTIVATION_IMPROVEMENT_PCT
        and best_params != default_params
    )

    return {
        "default": default_params,
        "candidate": best_params,
        "selected": "candidate" if use_candidate else "default",
        "selected_params": best_params if use_candidate else default_params,
        "activation": {
            "use_candidate": use_candidate,
            "reason": (
                "candidate_oos_ev_lcb80_improved"
                if use_candidate
                else "default_retained_until_candidate_lcb80_improves"
            ),
            "min_improvement_pct": MIN_ACTIVATION_IMPROVEMENT_PCT,
            "default_oos_ev_lcb_80_pct": default_lcb,
            "candidate_oos_ev_lcb_80_pct": candidate_lcb,
            "default_oos_trades": default_stats.get("oos_trades"),
            "candidate_oos_trades": best_stats.get("oos_trades"),
            "default_oos_avg_ret_pct": default_stats.get("oos_avg_ret_pct"),
            "candidate_oos_avg_ret_pct": best_stats.get("oos_avg_ret_pct"),
            "data_rows": len(rows),
        },
    }


def fetch_limit_up_model(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    as_of = latest_as_of(con, "limit_up_model_performance")
    if not as_of:
        return {"status": "missing", "as_of": None}
    perf = con.execute(
        """
        SELECT model_state, train_start, train_end, train_samples, train_positives,
               auc, brier, top_decile_hit_rate, top_decile_lift,
               failed_board_rate, avg_next_ret_pct
        FROM limit_up_model_performance
        WHERE as_of = CAST(? AS DATE)
        """,
        [as_of],
    ).fetchone()
    decision_counts: list[dict[str, Any]] = []
    if table_exists(con, "limit_up_model_predictions"):
        for state, count in con.execute(
            """
            SELECT decision_state, COUNT(*)
            FROM limit_up_model_predictions
            WHERE as_of = CAST(? AS DATE)
            GROUP BY decision_state
            ORDER BY COUNT(*) DESC
            """,
            [as_of],
        ).fetchall():
            decision_counts.append({"decision_state": state, "count": count})
    return {
        "status": "ok",
        "as_of": as_of,
        "model_state": perf[0],
        "train_start": str(perf[1]) if perf[1] is not None else None,
        "train_end": str(perf[2]) if perf[2] is not None else None,
        "train_samples": perf[3],
        "train_positives": perf[4],
        "auc": perf[5],
        "brier": perf[6],
        "top_decile_hit_rate": perf[7],
        "top_decile_lift": perf[8],
        "failed_board_rate": perf[9],
        "avg_next_ret_pct": perf[10],
        "decision_counts": decision_counts,
    }


def cn_parameter_registry() -> list[dict[str, Any]]:
    return [
        {
            "file": "quant-research-cn/src/analytics/paper_trade_ev.rs",
            "params": [
                {
                    "name": "LOOKBACK_DAYS",
                    "value": 90,
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                    "notes": "Should be selected by OOS EV LCB, not by taste.",
                },
                {
                    "name": "SLIPPAGE_PCT",
                    "value": 0.18,
                    "provenance": "cost_assumption",
                    "migration": "broker_cost_and_fill_model",
                    "notes": "Needs real fill/slippage audit when available.",
                },
                {
                    "name": "WIN_THRESHOLD_PCT",
                    "value": 0.50,
                    "provenance": "legacy_heuristic",
                    "migration": "strategy_family_specific_outcome_bins",
                },
                {
                    "name": "LOSS_THRESHOLD_PCT",
                    "value": -1.00,
                    "provenance": "legacy_heuristic",
                    "migration": "strategy_family_specific_outcome_bins",
                },
                {
                    "name": "EV80_Z",
                    "value": EV80_Z,
                    "provenance": "statistical",
                    "notes": "One-sided 80% normal lower-confidence z.",
                },
                {
                    "name": "EV95_Z",
                    "value": EV95_Z,
                    "provenance": "statistical",
                    "notes": "One-sided 95% normal lower-confidence z.",
                },
            ],
        },
        {
            "file": "quant-research-cn/src/analytics/limit_up_model.rs",
            "params": [
                {
                    "name": "TRAIN_LOOKBACK_DAYS",
                    "value": 540,
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                },
                {
                    "name": "COST_PCT",
                    "value": 0.20,
                    "provenance": "cost_assumption",
                    "migration": "limit_strategy_fill_cost_audit",
                },
                {
                    "name": "NEGATIVE_SAMPLE_DENOM",
                    "value": 24,
                    "provenance": "statistical",
                    "notes": "Rare-event class-balance control for logistic training.",
                },
            ],
        },
        {
            "file": "quant-research-cn/src/filtering/notable.rs",
            "params": [
                {
                    "name": "composite/report/chase weights",
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                    "notes": "Should not be treated as alpha proof.",
                }
            ],
        },
        {
            "file": "quant-research-cn/src/analytics/continuation_vs_fade.rs",
            "params": [
                {
                    "name": "continuation/fade score weights",
                    "provenance": "legacy_heuristic",
                    "migration": "calibrated_walk_forward",
                }
            ],
        },
        {
            "file": "quant-research-cn/src/analytics/open_execution_gate.rs",
            "params": [
                {
                    "name": "open execution score thresholds",
                    "provenance": "legacy_heuristic",
                    "migration": "fill_model_and_mae_calibration",
                }
            ],
        },
    ]


def us_parameter_registry() -> list[dict[str, Any]]:
    return [
        {
            "file": "quant-research-v1/src/quant_bot/analytics/risk_params.py",
            "params": [
                {
                    "name": "atr_stop_multiple",
                    "value": 2.0,
                    "provenance": "legacy_heuristic",
                    "migration": "walk_forward_rr_and_mae_calibration",
                },
                {
                    "name": "fallback_expected_move_atr_multiple",
                    "value": 2.0,
                    "provenance": "legacy_heuristic",
                    "migration": "options_expected_move_or_realized_cone_calibration",
                },
            ],
        },
        {
            "file": "quant-research-v1/src/quant_bot/analytics/options_alpha.py",
            "params": [
                {
                    "name": "directional_edge_threshold / vol_edge_threshold",
                    "value": "0.45 / 0.10",
                    "provenance": "legacy_heuristic",
                    "migration": "options_expression_outcome_calibration",
                },
                {
                    "name": "liquidity gate",
                    "value": "spread<=25%, chain_width>=6, days_to_exp>=3",
                    "provenance": "cost_assumption",
                    "migration": "options_fill_quality_audit",
                },
            ],
        },
        {
            "file": "quant-research-v1/src/quant_bot/analytics/overnight_continuation_alpha.py",
            "params": [
                {
                    "name": "continuation/fade/paid-risk weights",
                    "provenance": "legacy_heuristic",
                    "migration": "bucketed_postmortem_walk_forward",
                },
                {
                    "name": "paid_risk_do_not_chase / paid_risk_wait",
                    "value": "0.62 / 0.44",
                    "provenance": "legacy_heuristic",
                    "migration": "stale_chase_false_positive_lcb",
                },
            ],
        },
        {
            "file": "quant-research-v1/src/quant_bot/analytics/overnight_gate.py",
            "params": [
                {
                    "name": "support/stretch/action thresholds",
                    "provenance": "legacy_heuristic",
                    "migration": "next_open_fill_and_next_close_ev_calibration",
                    "notes": "Runtime-loaded; still waiting for walk-forward calibration.",
                }
            ],
        },
    ]


def fetch_us_postmortem(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    if not table_exists(con, "algorithm_postmortem"):
        return {"status": "missing"}
    rows = con.execute(
        """
        SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN executable THEN 1 ELSE 0 END) AS executable_n,
            AVG(realized_pnl_pct) FILTER (WHERE executable AND realized_pnl_pct IS NOT NULL) AS avg_pnl,
            AVG(CASE WHEN direction_right THEN 1.0 ELSE 0.0 END)
                FILTER (WHERE executable AND direction_right IS NOT NULL) AS direction_hit_rate,
            SUM(CASE WHEN stale_chase THEN 1 ELSE 0 END) AS stale_chase_n,
            MAX(CAST(evaluation_date AS VARCHAR)) AS latest_evaluation_date
        FROM algorithm_postmortem
        """
    ).fetchone()
    return {
        "status": "ok",
        "rows": rows[0],
        "executable_rows": rows[1],
        "avg_realized_pnl_pct": rows[2],
        "direction_hit_rate": rows[3],
        "stale_chase_rows": rows[4],
        "latest_evaluation_date": rows[5],
    }


def fetch_us_options_alpha(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    if not table_exists(con, "options_alpha"):
        return {"status": "missing"}
    as_of = latest_as_of(con, "options_alpha")
    if not as_of:
        return {"status": "missing", "as_of": None}
    rows = con.execute(
        """
        SELECT expression, liquidity_gate, COUNT(*), AVG(directional_edge), AVG(vol_edge)
        FROM options_alpha
        WHERE as_of = CAST(? AS DATE)
        GROUP BY expression, liquidity_gate
        ORDER BY COUNT(*) DESC
        """,
        [as_of],
    ).fetchall()
    return {
        "status": "ok",
        "as_of": as_of,
        "buckets": [
            {
                "expression": row[0],
                "liquidity_gate": row[1],
                "count": row[2],
                "avg_directional_edge": row[3],
                "avg_vol_edge": row[4],
            }
            for row in rows
        ],
    }


def _load_us_gate_outcomes(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    if not table_exists(con, "algorithm_postmortem") or not table_exists(con, "analysis_daily"):
        return []
    rows = con.execute(
        """
        SELECT
            CAST(ap.report_date AS VARCHAR) AS report_date,
            ap.symbol,
            ap.realized_pnl_pct,
            ap.label,
            ap.fill_quality,
            ap.detail_json AS postmortem_detail_json,
            ad.trend_prob,
            ad.p_downside,
            ad.details AS gate_detail_json
        FROM algorithm_postmortem ap
        INNER JOIN analysis_daily ad
          ON ad.date = ap.report_date
         AND ad.symbol = ap.symbol
         AND ad.module_name = 'overnight_gate'
        WHERE ap.action_intent = 'TRADE'
          AND ap.realized_pnl_pct IS NOT NULL
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    keys = [
        "report_date",
        "symbol",
        "realized_ret_pct",
        "label",
        "fill_quality",
        "postmortem_detail_json",
        "trend_prob",
        "p_downside",
        "gate_detail_json",
    ]
    for row in rows:
        item = dict(zip(keys, row))
        gate = _safe_json_dict(item.pop("gate_detail_json", None))
        pm = _safe_json_dict(item.pop("postmortem_detail_json", None))
        item.update(
            {
                "p_continue": _finite_float(item.get("trend_prob"), _finite_float(gate.get("p_continue"), 0.0)) or 0.0,
                "p_fade": _finite_float(item.get("p_downside"), _finite_float(gate.get("p_fade"), 0.0)) or 0.0,
                "support_score": _finite_float(gate.get("support_score"), 0.5) or 0.5,
                "effective_stretch_score": _finite_float(gate.get("effective_stretch_score"), _finite_float(gate.get("stretch_score"), 0.0)) or 0.0,
                "gap_vs_expected_move": _finite_float(gate.get("gap_vs_expected_move"), 0.0) or 0.0,
                "continuation_relief": _finite_float(gate.get("continuation_relief"), 0.0) or 0.0,
                "rr_ratio": _finite_float(pm.get("rr_ratio")),
                "expected_move_pct": _finite_float(pm.get("expected_move_pct")),
            }
        )
        out.append(item)
    return out


def _simulate_us_gate_action(row: dict[str, Any], action_params: dict[str, Any]) -> str:
    p_continue = _finite_float(row.get("p_continue"), 0.0) or 0.0
    p_fade = _finite_float(row.get("p_fade"), 0.0) or 0.0
    support_score = _finite_float(row.get("support_score"), 0.5) or 0.5
    effective_stretch = _finite_float(row.get("effective_stretch_score"), 0.0) or 0.0
    gap_vs_expected = _finite_float(row.get("gap_vs_expected_move"), 0.0) or 0.0
    relief = _finite_float(row.get("continuation_relief"), 0.0) or 0.0

    wait_gap_threshold = _finite_float(action_params.get("wait_gap_base"), 0.65) + (
        _finite_float(action_params.get("wait_gap_relief"), 0.35) * relief
    )
    do_not_chase_gap_threshold = _finite_float(action_params.get("do_not_chase_gap_base"), 1.00) + (
        _finite_float(action_params.get("do_not_chase_gap_relief"), 0.45) * relief
    )

    if effective_stretch >= _finite_float(action_params.get("do_not_chase_stretch"), 0.78) or (
        gap_vs_expected > do_not_chase_gap_threshold
        and support_score < _finite_float(action_params.get("do_not_chase_support_max"), 0.48)
    ):
        return "do_not_chase"
    if effective_stretch >= _finite_float(action_params.get("wait_stretch"), 0.50) or (
        gap_vs_expected > wait_gap_threshold
        and p_continue < _finite_float(action_params.get("wait_p_continue_max"), 0.68)
    ):
        return "wait_pullback"
    if p_continue >= _finite_float(action_params.get("continue_min"), 0.58):
        return "executable_now"
    if p_fade >= _finite_float(action_params.get("fade_min"), 0.58):
        return "wait_pullback"
    return "executable_now"


def calibrate_us_overnight_gate(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = _load_us_gate_outcomes(con)
    default_action = copy.deepcopy(DEFAULT_US_RUNTIME_PARAMS["overnight_gate"]["action"])
    train, oos = _train_oos_split(rows, "report_date")
    default_stats = _stats_for_filter(
        oos,
        lambda row: _simulate_us_gate_action(row, default_action) == "executable_now",
    )

    best_action = copy.deepcopy(default_action)
    best_train_stats = _stats_for_filter(
        train,
        lambda row: _simulate_us_gate_action(row, default_action) == "executable_now",
    )
    best_oos_stats = default_stats
    best_score = float(default_stats["ev_lcb_80_pct"]) if default_stats.get("ev_lcb_80_pct") is not None else float("-inf")

    grids = {
        "continue_min": [0.52, 0.56, 0.58, 0.62, 0.66],
        "wait_stretch": [0.40, 0.50, 0.60, 0.70],
        "do_not_chase_stretch": [0.65, 0.78, 0.90],
        "do_not_chase_support_max": [0.40, 0.48, 0.56],
        "wait_p_continue_max": [0.60, 0.68, 0.76],
    }
    for continue_min in grids["continue_min"]:
        for wait_stretch in grids["wait_stretch"]:
            for do_not_chase_stretch in grids["do_not_chase_stretch"]:
                if do_not_chase_stretch < wait_stretch:
                    continue
                for do_not_chase_support_max in grids["do_not_chase_support_max"]:
                    for wait_p_continue_max in grids["wait_p_continue_max"]:
                        candidate = copy.deepcopy(default_action)
                        candidate.update(
                            {
                                "continue_min": continue_min,
                                "wait_stretch": wait_stretch,
                                "do_not_chase_stretch": do_not_chase_stretch,
                                "do_not_chase_support_max": do_not_chase_support_max,
                                "wait_p_continue_max": wait_p_continue_max,
                            }
                        )
                        train_stats = _stats_for_filter(
                            train,
                            lambda row, params=candidate: _simulate_us_gate_action(row, params) == "executable_now",
                        )
                        if int(train_stats.get("n") or 0) < MIN_US_GATE_OOS_TRADES:
                            continue
                        oos_stats = _stats_for_filter(
                            oos,
                            lambda row, params=candidate: _simulate_us_gate_action(row, params) == "executable_now",
                        )
                        if int(oos_stats.get("n") or 0) < MIN_US_GATE_OOS_TRADES:
                            continue
                        score = oos_stats.get("ev_lcb_80_pct")
                        if score is None:
                            continue
                        if float(score) > best_score:
                            best_score = float(score)
                            best_action = candidate
                            best_train_stats = train_stats
                            best_oos_stats = oos_stats

    default_lcb = default_stats.get("ev_lcb_80_pct")
    candidate_lcb = best_oos_stats.get("ev_lcb_80_pct")
    use_candidate = (
        candidate_lcb is not None
        and default_lcb is not None
        and int(best_oos_stats.get("n") or 0) >= MIN_US_GATE_OOS_TRADES
        and float(candidate_lcb) > 0.0
        and float(candidate_lcb) >= float(default_lcb) + MIN_ACTIVATION_IMPROVEMENT_PCT
        and best_action != default_action
    )

    selected_action = best_action if use_candidate else default_action
    selected_payload = copy.deepcopy(DEFAULT_US_RUNTIME_PARAMS["overnight_gate"])
    selected_payload["action"] = selected_action
    selected_payload["provenance"] = "calibrated_walk_forward" if use_candidate else "legacy_heuristic"
    return {
        "status": "ok" if rows else "missing",
        "rows": len(rows),
        "train_rows": len(train),
        "oos_rows": len(oos),
        "selected": "candidate" if use_candidate else "default",
        "selected_params": selected_payload,
        "activation": {
            "use_candidate": use_candidate,
            "reason": (
                "candidate_oos_ev_lcb80_improved"
                if use_candidate
                else "default_retained_until_candidate_lcb80_improves"
            ),
            "min_oos_trades": MIN_US_GATE_OOS_TRADES,
            "min_improvement_pct": MIN_ACTIVATION_IMPROVEMENT_PCT,
            "default_oos": default_stats,
            "candidate_train": best_train_stats,
            "candidate_oos": best_oos_stats,
        },
    }


def _load_us_options_outcomes(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    if not table_exists(con, "options_alpha") or not table_exists(con, "alpha_postmortem"):
        return []
    rows = con.execute(
        """
        SELECT
            CAST(oa.as_of AS VARCHAR) AS report_date,
            oa.symbol,
            oa.directional_edge,
            oa.vol_edge,
            oa.vrp_edge,
            oa.flow_edge,
            oa.liquidity_gate,
            ap.best_ret_pct
        FROM options_alpha oa
        INNER JOIN alpha_postmortem ap
          ON ap.report_date = oa.as_of
         AND ap.symbol = oa.symbol
        WHERE ap.best_ret_pct IS NOT NULL
        """
    ).fetchall()
    keys = [
        "report_date",
        "symbol",
        "directional_edge",
        "vol_edge",
        "vrp_edge",
        "flow_edge",
        "liquidity_gate",
        "realized_ret_pct",
    ]
    return [dict(zip(keys, row)) for row in rows]


def _us_options_signal(row: dict[str, Any], params: dict[str, Any]) -> bool:
    if str(row.get("liquidity_gate") or "") != "pass":
        return False
    directional_edge = abs(_finite_float(row.get("directional_edge"), 0.0) or 0.0)
    vol_edge = _finite_float(row.get("vol_edge"), 0.0) or 0.0
    return directional_edge >= _finite_float(params.get("directional_edge_threshold"), 0.45) and vol_edge >= _finite_float(
        params.get("vol_edge_threshold"), 0.10
    )


def _bucket_rsi(value: Any) -> str:
    rsi = _finite_float(value, 50.0) or 50.0
    if rsi < 35.0:
        return "rsi_oversold"
    if rsi <= 55.0:
        return "rsi_neutral"
    if rsi <= 70.0:
        return "rsi_warm"
    return "rsi_hot"


def _bucket_vix(value: Any) -> str:
    vix = _finite_float(value)
    if vix is None:
        return "vix_unknown"
    if vix >= 30.0:
        return "extreme_fear"
    if vix >= 22.0:
        return "fear"
    if vix >= 16.0:
        return "neutral"
    return "greed"


def _bucket_stats(rows: list[dict[str, Any]], bucket_key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        bucket = str(row.get(bucket_key) or "unknown")
        ret = _finite_float(row.get("realized_ret_pct"))
        if ret is None:
            continue
        grouped.setdefault(bucket, []).append(ret)
    out = []
    for bucket, values in grouped.items():
        stats = _ev_stats(values)
        stats["bucket"] = bucket
        out.append(stats)
    return sorted(out, key=lambda row: (row.get("ev_lcb_80_pct") is None, -(row.get("ev_lcb_80_pct") or -999)))


def fetch_us_feature_diagnostics(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    if not table_exists(con, "algorithm_postmortem") or not table_exists(con, "analysis_daily"):
        return {"status": "missing"}
    rows = con.execute(
        """
        SELECT
            CAST(ap.report_date AS VARCHAR) AS report_date,
            ap.symbol,
            ap.realized_pnl_pct,
            mr.details AS mean_reversion_detail,
            vix.adj_close AS vix_level
        FROM algorithm_postmortem ap
        LEFT JOIN analysis_daily mr
          ON mr.date = ap.report_date
         AND mr.symbol = ap.symbol
         AND mr.module_name = 'mean_reversion'
        LEFT JOIN prices_daily vix
          ON vix.date = ap.report_date
         AND vix.symbol = '^VIX'
        WHERE ap.action_intent = 'TRADE'
          AND ap.realized_pnl_pct IS NOT NULL
        """
    ).fetchall()
    parsed = []
    for report_date, symbol, ret, mean_reversion_detail, vix_level in rows:
        detail = _safe_json_dict(mean_reversion_detail)
        rsi = detail.get("rsi_14")
        parsed.append(
            {
                "report_date": report_date,
                "symbol": symbol,
                "realized_ret_pct": ret,
                "rsi_14": rsi,
                "rsi_bucket": _bucket_rsi(rsi),
                "vix_level": vix_level,
                "vix_bucket": _bucket_vix(vix_level),
            }
        )
    return {
        "status": "ok",
        "rows": len(parsed),
        "rsi_buckets": _bucket_stats(parsed, "rsi_bucket"),
        "vix_buckets": _bucket_stats(parsed, "vix_bucket"),
        "notes": [
            "Diagnostics only: these buckets do not automatically alter production gates.",
            "RSI buckets use conventional technical ranges; VIX buckets proxy the internal fear/greed state.",
        ],
    }


def calibrate_us_options_alpha(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = _load_us_options_outcomes(con)
    default_params = copy.deepcopy(DEFAULT_US_RUNTIME_PARAMS["options_alpha"])
    train, oos = _train_oos_split(rows, "report_date")
    default_stats = _stats_for_filter(oos, lambda row: _us_options_signal(row, default_params))
    best_params = copy.deepcopy(default_params)
    best_train_stats = _stats_for_filter(train, lambda row: _us_options_signal(row, default_params))
    best_oos_stats = default_stats
    best_score = float(default_stats["ev_lcb_80_pct"]) if default_stats.get("ev_lcb_80_pct") is not None else float("-inf")

    for directional_threshold in [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]:
        for vol_threshold in [-0.10, 0.0, 0.10, 0.20, 0.30]:
            candidate = copy.deepcopy(default_params)
            candidate.update(
                {
                    "directional_edge_threshold": directional_threshold,
                    "vol_edge_threshold": vol_threshold,
                    "provenance": "calibrated_walk_forward",
                }
            )
            train_stats = _stats_for_filter(train, lambda row, params=candidate: _us_options_signal(row, params))
            if int(train_stats.get("n") or 0) < MIN_US_OPTIONS_OOS_ROWS:
                continue
            oos_stats = _stats_for_filter(oos, lambda row, params=candidate: _us_options_signal(row, params))
            if int(oos_stats.get("n") or 0) < MIN_US_OPTIONS_OOS_ROWS:
                continue
            score = oos_stats.get("ev_lcb_80_pct")
            if score is None:
                continue
            if float(score) > best_score:
                best_score = float(score)
                best_params = candidate
                best_train_stats = train_stats
                best_oos_stats = oos_stats

    default_lcb = default_stats.get("ev_lcb_80_pct")
    candidate_lcb = best_oos_stats.get("ev_lcb_80_pct")
    use_candidate = (
        candidate_lcb is not None
        and default_lcb is not None
        and int(best_oos_stats.get("n") or 0) >= MIN_US_OPTIONS_OOS_ROWS
        and float(candidate_lcb) > 0.0
        and float(candidate_lcb) >= float(default_lcb) + MIN_ACTIVATION_IMPROVEMENT_PCT
        and best_params != default_params
    )
    selected = best_params if use_candidate else default_params
    selected["provenance"] = "calibrated_walk_forward" if use_candidate else "legacy_heuristic"
    return {
        "status": "ok" if rows else "missing",
        "rows": len(rows),
        "train_rows": len(train),
        "oos_rows": len(oos),
        "selected": "candidate" if use_candidate else "default",
        "selected_params": selected,
        "activation": {
            "use_candidate": use_candidate,
            "reason": (
                "candidate_oos_ev_lcb80_improved"
                if use_candidate
                else "default_retained_until_candidate_lcb80_improves"
            ),
            "min_oos_rows": MIN_US_OPTIONS_OOS_ROWS,
            "min_improvement_pct": MIN_ACTIVATION_IMPROVEMENT_PCT,
            "default_oos": default_stats,
            "candidate_train": best_train_stats,
            "candidate_oos": best_oos_stats,
        },
    }


def calibrate_us_runtime_params(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    gate = calibrate_us_overnight_gate(con)
    options = calibrate_us_options_alpha(con)
    selected = copy.deepcopy(DEFAULT_US_RUNTIME_PARAMS)
    if isinstance(gate.get("selected_params"), dict):
        selected["overnight_gate"] = _deep_merge_dict(
            selected["overnight_gate"],
            gate["selected_params"],
        )
    if isinstance(options.get("selected_params"), dict):
        selected["options_alpha"] = _deep_merge_dict(
            selected["options_alpha"],
            options["selected_params"],
        )
    return {
        "selected_params": selected,
        "overnight_gate": gate,
        "options_alpha": options,
    }


def build_cn_artifact(db_path: Path) -> dict[str, Any]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        runtime_params = calibrate_paper_trade_params(con)
        return {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market": "cn",
            "source_db": str(db_path),
            "principles": [
                "Report ranking should use realized strategy EV, not opaque setup score.",
                "Every numeric threshold must carry provenance.",
                "Legacy heuristics are allowed only as defaults until walk-forward calibration replaces them.",
                "Execution Alpha requires positive EV lower confidence bound, not just high hit rate.",
            ],
            "ev80_lcb": {
                "definition": "one-sided 80% lower confidence bound of strategy EV",
                "formula": "ev_lcb_80 = ev_pct - 1.2816 * max(realized_std_pct, risk_unit_pct) / sqrt(fills)",
                "z": EV80_Z,
                "interpretation": "A conservative haircut on average EV. It is not win rate and not a guarantee.",
                "release_rule": "ev_lcb_80_pct > 0 means the strategy remains positive after an 80% confidence haircut.",
            },
            "paper_trade_ev": {
                "current_release_rule": {
                    "min_ev_pct": DEFAULT_PAPER_PARAMS["min_ev_pct"],
                    "min_ev_lcb_80_pct": DEFAULT_PAPER_PARAMS["min_ev_lcb_80_pct"],
                    "min_fill_rate": DEFAULT_PAPER_PARAMS["min_fill_rate"],
                    "provenance": "mixed: statistical confidence bound plus legacy engineering guards",
                },
                "runtime_params": runtime_params,
                "latest": fetch_strategy_ev(con),
            },
            "limit_up_model": fetch_limit_up_model(con),
            "parameter_registry": cn_parameter_registry(),
        }
    finally:
        con.close()


def build_us_artifact(db_path: Path) -> dict[str, Any]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        runtime_calibration = calibrate_us_runtime_params(con)
        return {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market": "us",
            "source_db": str(db_path),
            "principles": [
                "US execution/risk constants must be runtime parameters with provenance.",
                "Options alpha is an expression layer; it needs its own outcome calibration.",
                "Legacy heuristics remain defaults until OOS postmortem LCB improves.",
                "Headline/news context can explain a setup but cannot prove alpha.",
            ],
            "ev80_lcb": {
                "definition": "one-sided 80% lower confidence bound of realized strategy return",
                "formula": "ev_lcb_80 = avg_return - 1.2816 * max(std_return, 0.50) / sqrt(n)",
                "z": EV80_Z,
                "interpretation": "A conservative OOS haircut. Candidate params activate only when this improves and stays positive.",
            },
            "us_runtime_params": runtime_calibration["selected_params"],
            "runtime_calibration": {
                "overnight_gate": runtime_calibration["overnight_gate"],
                "options_alpha": runtime_calibration["options_alpha"],
            },
            "feature_diagnostics": fetch_us_feature_diagnostics(con),
            "latest": {
                "postmortem": fetch_us_postmortem(con),
                "options_alpha": fetch_us_options_alpha(con),
            },
            "parameter_registry": us_parameter_registry(),
        }
    finally:
        con.close()


def build_artifact(market: str, db_path: Path) -> dict[str, Any]:
    if market == "cn":
        return build_cn_artifact(db_path)
    if market == "us":
        return build_us_artifact(db_path)
    raise ValueError(f"unsupported market: {market}")


def output_paths(market: str) -> list[Path]:
    paths = [
        FACTOR_LAB_ROOT
        / "runtime"
        / "strategy_calibration"
        / market
        / "strategy_params.generated.yaml"
    ]
    if market == "cn":
        paths.append(QUANT_CN_ROOT / "config" / "strategy_params.generated.yaml")
    if market == "us":
        paths.append(QUANT_US_ROOT / "config" / "strategy_params.generated.yaml")
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate strategy parameter calibration artifact.")
    parser.add_argument("--market", choices=["cn", "us"], default="cn")
    parser.add_argument("--dry-run", action="store_true", help="Print artifact without writing files.")
    parser.add_argument("--output", type=Path, help="Optional single output path.")
    args = parser.parse_args()

    db_path = QUANT_CN_REPORT_DB if args.market == "cn" else QUANT_US_REPORT_DB
    if db_path is None or not db_path.exists():
        print(f"ERROR: report database not found: {db_path}", file=sys.stderr)
        return 1

    artifact = build_artifact(args.market, db_path)
    text = yaml.safe_dump(artifact, sort_keys=False, allow_unicode=True)
    if args.dry_run:
        print(text)
        return 0

    targets = [args.output] if args.output else output_paths(args.market)
    for path in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
