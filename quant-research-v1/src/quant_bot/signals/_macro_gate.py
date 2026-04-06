"""Macro gate -- 3x3 VIX x 10Y-2Y matrix with per-asset, per-source multipliers."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

def _source_bucket(signal_type: str) -> str:
    if signal_type == "event_driven":
        return "event_driven"
    if "momentum" in signal_type:
        return "momentum"
    if "options" in signal_type or "proxy" in signal_type:
        return "options"
    return "mixed"


def _pair(long_mult: float, short_mult: float) -> dict[str, float]:
    return {"long": long_mult, "short": short_mult}


def _macro_series(market_context: dict, series_id: str) -> float | None:
    """Pull a FRED series from market_context["macro"] by series_id."""
    macro = market_context.get("macro", {})
    for payload in macro.values():
        if payload.get("series_id") == series_id and payload.get("value") is not None:
            return float(payload["value"])
    return None


def _vix_bucket(vix_level: float | None) -> str:
    if vix_level is None:
        return "unknown"
    if vix_level < 20:
        return "calm"
    if vix_level < 30:
        return "elevated"
    return "panic"


def _curve_bucket(spread_10y2y: float | None) -> str:
    if spread_10y2y is None:
        return "unknown"
    if spread_10y2y < 0:
        return "inverted"
    if spread_10y2y < 0.75:
        return "low_positive"
    return "high_positive"


NEUTRAL_ASSET_MULTIPLIERS = {
    "default": _pair(1.00, 1.00),
    "broad_equity": _pair(1.00, 1.00),
    "growth": _pair(1.00, 1.00),
    "defensive": _pair(1.00, 1.00),
    "energy": _pair(1.00, 1.00),
    "defense": _pair(1.00, 1.00),
    "duration": _pair(1.00, 1.00),
    "credit": _pair(1.00, 1.00),
    "gold": _pair(1.00, 1.00),
    "vol": _pair(1.00, 1.00),
}

MACRO_GATE_MATRIX: dict[tuple[str, str], dict[str, Any]] = {
    # ── calm VIX ──────────────────────────────────────────────────────────
    ("calm", "inverted"): {
        "regime": "late_cycle_complacency",
        # Calm VIX with inverted curve: equity tape is complacent vs macro.
        "asset": {
            "broad_equity": _pair(0.95, 1.00),
            "growth": _pair(0.93, 1.02),
            "duration": _pair(1.05, 0.95),
            "credit": _pair(0.95, 1.02),
            "vol": _pair(0.95, 1.00),
        },
        "source": {"event_driven": 1.00, "momentum": 0.98, "options": 1.00, "mixed": 1.00},
    },
    ("calm", "low_positive"): {
        "regime": "neutral",
        "asset": {},
        "source": {"event_driven": 1.00, "momentum": 1.00, "options": 1.00, "mixed": 1.00},
    },
    ("calm", "high_positive"): {
        "regime": "reflation_risk_on",
        # Steep curve + calm vol = friendlier risk-on backdrop.
        "asset": {
            "broad_equity": _pair(1.05, 0.95),
            "growth": _pair(1.08, 0.92),
            "energy": _pair(1.08, 0.92),
            "defensive": _pair(0.95, 1.05),
            "duration": _pair(0.92, 1.05),
            "credit": _pair(1.05, 0.95),
            "vol": _pair(0.90, 1.05),
        },
        "source": {"event_driven": 1.00, "momentum": 1.05, "options": 1.00, "mixed": 1.00},
    },
    # ── elevated VIX ──────────────────────────────────────────────────────
    ("elevated", "inverted"): {
        "regime": "recession_watch",
        # Classic late-cycle slowdown: stress rising, curve already warning.
        "asset": {
            "broad_equity": _pair(0.85, 1.05),
            "growth": _pair(0.75, 1.10),
            "defensive": _pair(1.08, 0.95),
            "defense": _pair(1.05, 0.95),
            "duration": _pair(1.12, 0.90),
            "credit": _pair(0.82, 1.08),
            "gold": _pair(1.05, 0.95),
            "vol": _pair(1.10, 0.95),
        },
        "source": {"event_driven": 1.02, "momentum": 0.93, "options": 0.98, "mixed": 1.00},
    },
    ("elevated", "low_positive"): {
        "regime": "flight_to_safety_watch",
        # High-ish vol, non-inverted low curve: flight to safety or shock.
        "asset": {
            "broad_equity": _pair(0.88, 1.05),
            "growth": _pair(0.80, 1.08),
            "defensive": _pair(1.08, 0.95),
            "defense": _pair(1.05, 0.95),
            "duration": _pair(1.05, 0.95),
            "credit": _pair(0.85, 1.08),
            "gold": _pair(1.07, 0.95),
            "vol": _pair(1.12, 0.95),
        },
        "source": {"event_driven": 1.05, "momentum": 0.95, "options": 1.00, "mixed": 1.00},
    },
    ("elevated", "high_positive"): {
        "regime": "policy_uncertainty",
        # Rising vol + steep curve = fiscal/policy/term-premium uncertainty.
        "asset": {
            "broad_equity": _pair(0.80, 1.03),
            "growth": _pair(0.75, 1.05),
            "defensive": _pair(0.95, 1.00),
            "energy": _pair(0.95, 1.00),
            "defense": _pair(0.95, 1.00),
            "duration": _pair(0.90, 1.05),
            "credit": _pair(0.78, 1.05),
            "gold": _pair(1.05, 0.95),
            "vol": _pair(1.10, 0.95),
        },
        "source": {"event_driven": 0.98, "momentum": 0.92, "options": 0.95, "mixed": 0.95},
    },
    # ── panic VIX ─────────────────────────────────────────────────────────
    ("panic", "inverted"): {
        "regime": "recession_scare",
        # Cleanest recession-fear quadrant: growth hit hardest.
        "asset": {
            "broad_equity": _pair(0.70, 1.08),
            "growth": _pair(0.60, 1.12),
            "defensive": _pair(1.12, 0.92),
            "defense": _pair(1.05, 0.95),
            "energy": _pair(0.90, 1.05),
            "duration": _pair(1.15, 0.88),
            "credit": _pair(0.70, 1.10),
            "gold": _pair(1.10, 0.92),
            "vol": _pair(1.20, 0.90),
        },
        "source": {"event_driven": 1.03, "momentum": 0.90, "options": 0.95, "mixed": 0.98},
    },
    ("panic", "low_positive"): {
        "regime": "panic_flight_to_safety",
        # Panic vol + non-inverted curve: true safety rush or geopolitical shock.
        # This is the quadrant that fixes the old scalar gate bug.
        "asset": {
            "broad_equity": _pair(0.75, 1.05),
            "growth": _pair(0.65, 1.10),
            "defensive": _pair(1.15, 0.90),
            "defense": _pair(1.10, 0.90),
            "energy": _pair(1.05, 0.95),
            "duration": _pair(1.05, 0.95),
            "credit": _pair(0.72, 1.10),
            "gold": _pair(1.12, 0.90),
            "vol": _pair(1.22, 0.88),
        },
        "source": {"event_driven": 1.08, "momentum": 0.92, "options": 1.00, "mixed": 1.00},
    },
    ("panic", "high_positive"): {
        "regime": "systemic_uncertainty",
        # Only quadrant where broad dampening is correct across the board.
        "asset": {
            "default": _pair(0.90, 0.90),
            "broad_equity": _pair(0.65, 0.95),
            "growth": _pair(0.60, 0.95),
            "defensive": _pair(0.85, 0.95),
            "defense": _pair(0.90, 0.95),
            "energy": _pair(0.90, 0.95),
            "duration": _pair(0.85, 0.95),
            "credit": _pair(0.65, 0.95),
            "gold": _pair(1.05, 0.95),
            "vol": _pair(1.15, 0.90),
        },
        "source": {"event_driven": 0.95, "momentum": 0.88, "options": 0.92, "mixed": 0.90},
    },
}


def _apply_asset_overrides(
    base: dict[str, dict[str, float]],
    overrides: dict[str, dict[str, float]],
) -> None:
    for bucket, pair in overrides.items():
        current = base.setdefault(bucket, _pair(1.00, 1.00))
        current["long"] = round(current["long"] * pair["long"], 3)
        current["short"] = round(current["short"] * pair["short"], 3)


def _shock_overlays(market_context: dict) -> list[dict[str, Any]]:
    """Deterministic cross-asset overlays from tape, not headline parsing."""
    overlays: list[dict[str, Any]] = []

    vix_level = market_context.get("vix", {}).get("level")
    spy = market_context.get("major_indices", {}).get("SPY", {}).get("ret_1d_pct")
    qqq = market_context.get("major_indices", {}).get("QQQ", {}).get("ret_1d_pct")
    xle = market_context.get("sectors", {}).get("XLE", {}).get("ret_1d_pct")
    crude = market_context.get("commodities", {}).get("CL=F", {}).get("ret_1d_pct")
    tlt = market_context.get("rates_credit", {}).get("TLT", {}).get("ret_1d_pct")

    # Commodity shock: oil-led stress sharpens energy/defense, weakens bond hedge
    if (
        vix_level is not None and vix_level >= 25
        and spy is not None and spy < 0
        and qqq is not None and qqq < 0
        and ((crude is not None and crude >= 2.5) or (xle is not None and xle >= 1.5))
    ):
        overlays.append({
            "name": "commodity_shock",
            "asset": {
                "energy": _pair(1.15, 0.90),
                "defense": _pair(1.08, 0.92),
                "growth": _pair(0.95, 1.02),
                "duration": _pair(0.92, 1.02),
            },
            "source": {"event_driven": 1.05},
        })

    # Duration failure: stocks and Treasuries both sell off
    if (
        vix_level is not None and vix_level >= 25
        and spy is not None and spy <= -0.75
        and tlt is not None and tlt <= -0.50
    ):
        overlays.append({
            "name": "duration_failure",
            "asset": {
                "duration": _pair(0.85, 1.05),
                "gold": _pair(1.03, 0.98),
            },
            "source": {},
        })

    return overlays


def _macro_gate(market_context: dict) -> dict[str, Any]:
    """
    Regime-aware macro gate returning per-asset, per-source multipliers.

    Uses VIX from price data first (market_context["vix"]["level"]),
    falls back to FRED VIXCLS. Uses T10Y2Y from FRED macro data.
    """
    vix_level = market_context.get("vix", {}).get("level")
    if vix_level is None:
        vix_level = _macro_series(market_context, "VIXCLS")

    spread_10y2y = _macro_series(market_context, "T10Y2Y")

    vix_b = _vix_bucket(vix_level)
    spread_b = _curve_bucket(spread_10y2y)

    if vix_b == "unknown" or spread_b == "unknown":
        return {
            "regime": "unknown",
            "inputs": {"vix_level": vix_level, "spread_10y2y": spread_10y2y},
            "matrix_cell": {"vix": vix_b, "spread": spread_b},
            "asset": deepcopy(NEUTRAL_ASSET_MULTIPLIERS),
            "source": {"event_driven": 1.00, "momentum": 1.00, "options": 1.00, "mixed": 1.00},
            "overlays": [],
        }

    cell = MACRO_GATE_MATRIX[(vix_b, spread_b)]
    asset = deepcopy(NEUTRAL_ASSET_MULTIPLIERS)
    _apply_asset_overrides(asset, cell["asset"])

    source = {"event_driven": 1.00, "momentum": 1.00, "options": 1.00, "mixed": 1.00}
    source.update(cell["source"])

    overlay_names: list[str] = []
    for overlay in _shock_overlays(market_context):
        overlay_names.append(overlay["name"])
        _apply_asset_overrides(asset, overlay.get("asset", {}))
        for source_key, mult in overlay.get("source", {}).items():
            source[source_key] = round(source.get(source_key, 1.00) * mult, 3)

    return {
        "regime": cell["regime"],
        "inputs": {"vix_level": vix_level, "spread_10y2y": spread_10y2y},
        "matrix_cell": {"vix": vix_b, "spread": spread_b},
        "asset": asset,
        "source": source,
        "overlays": overlay_names,
    }


def _resolve_macro_multiplier(
    gate_map: dict[str, Any],
    asset_bucket: str,
    direction: str,
    signal_type: str,
) -> float:
    """Resolve the final scalar multiplier for one item from the gate dict."""
    asset_mult = gate_map["asset"].get(
        asset_bucket, gate_map["asset"]["default"]
    ).get(direction, 1.00)
    source_mult = gate_map["source"].get(_source_bucket(signal_type), 1.00)
    # Clamp so macro never fully overwhelms or fabricates a signal
    applied = asset_mult * source_mult
    return max(0.60, min(1.35, applied))
