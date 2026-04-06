"""
Contradiction matrix: detect when signal sources disagree.

4 sources: A=momentum/price, B=options/sentiment, C=fundamental/valuation, D=event/catalyst

Patterns:
  A up + B up + C=cheap + D=catalyst   -> HIGH conviction (all agree)
  A up + B down                        -> contradiction (momentum up but options bearish)
  A up + C=expensive                   -> momentum chasing risk
  A=none + C=cheap + D=catalyst        -> value play
"""
from __future__ import annotations

from typing import Any


def _direction_label(val: float | None, up_thresh: float, down_thresh: float) -> str:
    """Classify a numeric value into up/down/neutral."""
    if val is None:
        return "neutral"
    if val > up_thresh:
        return "up"
    if val < down_thresh:
        return "down"
    return "neutral"


def _momentum_direction(item: dict) -> str:
    """Source A: momentum/price direction from trend_prob and returns."""
    mom = item.get("momentum") or {}
    tp = mom.get("trend_prob") or mom.get("p_upside")
    if tp is not None:
        return _direction_label(tp, 0.55, 0.45)
    # Fallback to 5D return
    ret_5d = item.get("ret_5d_pct")
    if ret_5d is not None:
        return _direction_label(ret_5d, 2.0, -2.0)
    return "neutral"


def _options_direction(item: dict) -> str:
    """Source B: options/sentiment direction from sentiment z-scores and options data."""
    sentiment = item.get("sentiment") or {}
    opts = item.get("options") or {}

    # Use sentiment z-scores if available
    pc_z = sentiment.get("pc_ratio_z")
    skew_z = sentiment.get("skew_z")
    vrp = sentiment.get("vrp")

    score = 0.0
    signals = 0

    # Negative pc_ratio_z = more calls vs puts = bullish
    if pc_z is not None:
        if pc_z < -1.0:
            score += 1.0
        elif pc_z < -0.5:
            score += 0.5
        elif pc_z > 1.0:
            score -= 1.0
        elif pc_z > 0.5:
            score -= 0.5
        signals += 1

    # Low skew_z = less put premium = bullish
    if skew_z is not None:
        if skew_z < -1.0:
            score += 0.5
        elif skew_z > 1.0:
            score -= 0.5
        signals += 1

    # Positive VRP = IV > RV = options overpricing risk = slightly bearish
    if vrp is not None:
        if vrp > 0.1:
            score -= 0.3
        elif vrp < -0.1:
            score += 0.3
        signals += 1

    # Options bias signal as tiebreaker
    bias = opts.get("bias_signal")
    if bias == "bullish":
        score += 0.3
    elif bias == "bearish":
        score -= 0.3

    if signals == 0 and bias is None:
        return "neutral"

    if score > 0.3:
        return "up"
    if score < -0.3:
        return "down"
    return "neutral"


def _valuation_direction(value_score: dict | None) -> str:
    """Source C: fundamental/valuation from value_scores pe_pct."""
    if value_score is None:
        return "neutral"
    # valuation_score: 0 = cheapest in sector, 1 = most expensive
    vs = value_score.get("valuation_score")
    if vs is None:
        return "neutral"
    if vs < 0.3:
        return "cheap"
    if vs > 0.7:
        return "expensive"
    return "neutral"


def _catalyst_direction(item: dict) -> str:
    """Source D: event/catalyst presence."""
    events = item.get("events") or []
    news = item.get("news") or []
    filings = item.get("sec_filings") or []

    has_catalyst = len(events) > 0 or len(news) > 0 or len(filings) > 0
    return "catalyst" if has_catalyst else "none"


def detect_contradictions(
    item: dict,
    value_score: dict | None = None,
) -> dict[str, Any]:
    """
    Analyze signal agreement/contradiction for a notable item.

    Returns: {
        sources: {A: str, B: str, C: str, D: str},
        contradictions: list[str],
        pattern: str,
        conviction_modifier: float,
    }
    """
    a = _momentum_direction(item)
    b = _options_direction(item)
    c = _valuation_direction(value_score)
    d = _catalyst_direction(item)

    sources = {"A_momentum": a, "B_options": b, "C_valuation": c, "D_catalyst": d}
    contradictions: list[str] = []
    modifier = 1.0

    # --- Contradiction detection ---

    # A vs B: momentum vs options disagreement
    if a != "neutral" and b != "neutral":
        if (a == "up" and b == "down") or (a == "down" and b == "up"):
            contradictions.append(
                f"Momentum ({a}) contradicts options/sentiment ({b})"
            )
            modifier *= 0.6

    # A vs C: momentum vs valuation
    if a == "up" and c == "expensive":
        contradictions.append(
            "Momentum chasing risk: price trending up but valuation expensive"
        )
        modifier *= 0.8
    elif a == "down" and c == "cheap":
        contradictions.append(
            "Falling knife risk: valuation cheap but momentum still down"
        )
        modifier *= 0.85

    # B vs C: options bullish but expensive, or options bearish but cheap
    if b == "up" and c == "expensive":
        contradictions.append(
            "Options bullish but valuation already stretched"
        )
        modifier *= 0.85
    elif b == "down" and c == "cheap":
        contradictions.append(
            "Options bearish despite cheap valuation -- potential forced selling"
        )
        modifier *= 0.9

    # --- Pattern classification ---
    if not contradictions:
        if a == "up" and b == "up" and c in ("cheap", "neutral") and d == "catalyst":
            pattern = "aligned_bullish"
        elif a == "down" and b == "down" and c in ("expensive", "neutral") and d == "catalyst":
            pattern = "aligned_bearish"
        elif a == "neutral" and c == "cheap" and d == "catalyst":
            pattern = "value_play"
        elif a == "neutral" and b == "neutral":
            pattern = "no_signal"
        else:
            pattern = "partial_alignment"
    else:
        if len(contradictions) >= 2:
            pattern = "major_contradiction"
        else:
            pattern = "minor_contradiction"

    return {
        "sources": sources,
        "contradictions": contradictions,
        "pattern": pattern,
        "conviction_modifier": round(modifier, 2),
    }
