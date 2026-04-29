"""Runtime strategy parameters for the US research producer.

The defaults here are deliberately conservative and transparent.  Weekend
calibration can write ``config/strategy_params.generated.yaml`` or the shared
Factor Lab artifact, and runtime code will use it when present.
"""
from __future__ import annotations

import copy
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_US_STRATEGY_PARAMS: dict[str, Any] = {
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
        "direction_weights": {
            "bias": 0.35,
            "pc": 0.25,
            "skew": 0.25,
            "flow": 0.15,
        },
        "vol_weights": {
            "vrp": 0.65,
            "cheapness": 0.25,
            "flow": 0.10,
        },
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


def _repo_paths() -> tuple[Path, Path]:
    us_root = Path(__file__).resolve().parents[3]
    stack_root = us_root.parent
    return us_root, stack_root


def _candidate_paths() -> list[Path]:
    us_root, stack_root = _repo_paths()
    paths: list[Path] = []
    env_path = os.environ.get("QUANT_US_STRATEGY_PARAMS")
    if env_path:
        paths.append(Path(env_path).expanduser())
    paths.extend(
        [
            us_root / "config" / "strategy_params.generated.yaml",
            us_root / "config" / "strategy_params.yaml",
            stack_root / "factor-lab" / "runtime" / "strategy_calibration" / "us" / "strategy_params.generated.yaml",
        ]
    )
    return paths


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def _runtime_payload(raw: dict[str, Any]) -> dict[str, Any]:
    for key in ("us_runtime_params", "runtime_params", "strategy_params"):
        value = raw.get(key)
        if isinstance(value, dict):
            return value
    return raw


@lru_cache(maxsize=1)
def load_us_strategy_params() -> dict[str, Any]:
    params = copy.deepcopy(DEFAULT_US_STRATEGY_PARAMS)
    for path in _candidate_paths():
        if not path.exists():
            continue
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        params = _deep_merge(params, _runtime_payload(raw))
        params["_source"] = str(path)
        return params
    params["_source"] = "built_in_default"
    return params


def get_us_strategy_param_section(section: str) -> dict[str, Any]:
    value = load_us_strategy_params().get(section)
    return value if isinstance(value, dict) else {}
