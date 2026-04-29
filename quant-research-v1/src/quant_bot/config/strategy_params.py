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
