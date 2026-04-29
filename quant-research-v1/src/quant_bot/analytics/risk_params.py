"""
Risk parameters: entry, stop, target, R:R ratio.
Computed from ATR and probability cone.
"""
from __future__ import annotations

from typing import Any

from quant_bot.config.strategy_params import get_us_strategy_param_section


def compute_risk_params(item: dict) -> dict[str, Any]:
    """
    For a notable item with momentum and options data, compute:

    entry  = current price
    stop   = price - 2 * ATR  (2-ATR trailing stop for longs; inverted for shorts)
    target = price + expected_move  (from options cone or ATR-based)
    rr_ratio = |target - entry| / |entry - stop|
    half_life = from cointegration OU fit if available

    Returns: {entry, stop, target, rr_ratio, half_life} or empty dict on insufficient data.
    """
    price = item.get("price")
    atr = item.get("atr")
    if price is None or atr is None or price <= 0 or atr <= 0:
        return {}
    params = get_us_strategy_param_section("risk_params")
    stop_atr_multiple = float(params.get("atr_stop_multiple", 2.0))
    fallback_move_atr_multiple = float(params.get("fallback_expected_move_atr_multiple", 2.0))

    # Direction from signal classification
    sig = item.get("signal", {})
    direction = sig.get("direction", "long")

    # Expected move: prefer options-implied, fall back to ATR-based
    expected_move_pct = None
    opts = item.get("options") or {}
    if opts.get("expected_move_pct"):
        expected_move_pct = opts["expected_move_pct"]
    elif (item.get("momentum") or {}).get("expected_move_pct"):
        expected_move_pct = item["momentum"]["expected_move_pct"]

    if expected_move_pct is None:
        expected_move_pct = (fallback_move_atr_multiple * atr / price) * 100.0

    expected_move_usd = price * expected_move_pct / 100.0

    execution_gate = item.get("execution_gate") or {}
    execution_mode = execution_gate.get("action", "executable_now")
    pullback_price = execution_gate.get("pullback_price")

    # Compute entry, stop, target based on direction.
    # If the overnight gate says "wait" or "do not chase", anchor the
    # entry to the pullback trigger instead of the stale prior close.
    entry_price = price
    if execution_mode in {"wait_pullback", "do_not_chase"} and pullback_price:
        try:
            entry_price = float(pullback_price)
        except (TypeError, ValueError):
            entry_price = price

    entry = round(entry_price, 2)
    stop_distance = stop_atr_multiple * atr

    if direction == "short":
        stop = round(entry_price + stop_distance, 2)
        target = round(entry_price - expected_move_usd, 2)
    else:
        # Default to long
        stop = round(entry_price - stop_distance, 2)
        target = round(entry_price + expected_move_usd, 2)

    # R:R ratio = reward / risk
    risk = abs(entry - stop)
    reward = abs(target - entry)
    rr_ratio = round(reward / risk, 2) if risk > 0 else None

    result: dict[str, Any] = {
        "entry": entry,
        "stop": stop,
        "target": target,
        "rr_ratio": rr_ratio,
        "stop_distance_atr": stop_atr_multiple,
        "expected_move_pct": round(expected_move_pct, 2),
        "execution_mode": execution_mode,
        "reference_price": execution_gate.get("ref_price"),
        "gap_pct": execution_gate.get("gap_pct"),
        "param_source": params.get("provenance", "built_in_default"),
    }

    # Half-life from cointegration if available
    price_signals = item.get("price_signals") or {}
    coint = price_signals.get("cointegration")
    if coint:
        # coint may be a list of pairs; take the one with shortest half_life
        if isinstance(coint, list):
            hl_values = [p.get("half_life_days") for p in coint if p.get("half_life_days")]
            if hl_values:
                result["half_life"] = round(min(hl_values), 1)
        elif isinstance(coint, dict) and coint.get("half_life_days"):
            result["half_life"] = round(coint["half_life_days"], 1)

    return result
