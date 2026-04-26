"""
Signal classification -- weighted multi-source directional scoring.

Each source emits:
  - signed_score in [-1, 1]: negative = bearish, positive = bullish
  - quality in [0, 1]: how trustworthy/liquid/fresh the signal is

Direction = sign of quality-weighted sum of signed scores.
Confidence = f(agreement among high-quality independent sources).

Five signal sources:
  1. options   -- IV skew, put/call ratio, bias signal, liquidity quality
  2. momentum  -- trend_prob, regime, 5D/20D returns, z-score
  3. event     -- earnings surprise, 8-K filings, index changes
  4. reversion -- mean-reversion extremes (RSI, Bollinger, SMA deviation)
  5. breakout  -- volatility squeeze + range/volume breakout detection

Macro regime acts as a gate/weight modifier, not an equal vote.
Gate is a 3x3 matrix (VIX bucket x 10Y-2Y bucket) with per-asset-class
and per-source multipliers, replacing the old scalar dampener.

Proxy-derived options (e.g. CL=F using USO's chain) are tracked as
non-independent sources and receive a 50% quality haircut.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Optional

from ._sources import (
    _options_signal, _momentum_signal, _event_signal,
    _reversion_signal, _breakout_signal,
)
from ._asset_buckets import _asset_bucket_for_item
from ._divergence import _cross_asset_divergence
from ._macro_gate import (
    _macro_gate,
    _resolve_macro_multiplier,
    NEUTRAL_ASSET_MULTIPLIERS,
)


# ── Evidence shrinkage ───────────────────────────────────────────────────────

def _effective_source_stats(sources: list[dict[str, Any]]) -> dict[str, float]:
    """Estimate effective independent evidence count from source contribution concentration."""
    active = [
        max(abs(float(src.get("signed_score") or 0.0)) * float(src.get("quality") or 0.0), 0.0)
        for src in sources
        if src.get("independent") and abs(float(src.get("signed_score") or 0.0)) >= 0.1
    ]
    n_total = len(active)
    if n_total <= 1:
        return {"n_total": float(n_total), "n_effective": float(n_total), "shrinkage": 1.0}

    total_weight = sum(active)
    if total_weight <= 0:
        return {"n_total": float(n_total), "n_effective": 1.0, "shrinkage": 0.6}

    probs = [w / total_weight for w in active]
    n_effective = 1.0 / max(sum(p * p for p in probs), 1e-12)
    shrinkage = max(0.60, min(1.0, (n_effective / n_total) ** 0.5))
    return {
        "n_total": float(n_total),
        "n_effective": round(float(n_effective), 3),
        "shrinkage": round(float(shrinkage), 3),
    }


# ── Main classifier ─────────────────────────────────────────────────────────

def classify_signal(
    item: dict,
    market_context: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Classify a notable item into a signal with direction, confidence, and quality.

    Uses regime-aware macro gate (3x3 VIX x 10Y-2Y matrix) and
    proxy-aware source independence tracking.
    """
    # Extract source signals
    opts = item.get("options", {}) or {}
    proxy_source = opts.get("_proxy_source") or opts.get("proxy_source")
    opts_signed, opts_quality = _options_signal(opts)
    mom_signed, mom_quality = _momentum_signal(item.get("momentum", {}))
    ev_signed, ev_quality = _event_signal(
        item.get("events", []),
        item.get("earnings_risk", {}),
    )
    rev_signed, rev_quality = _reversion_signal(item.get("mean_reversion", {}))
    bo_signed, bo_quality = _breakout_signal(item.get("breakout", {}))

    # Factor Lab composite
    lab = item.get("lab_factor", {})
    lab_val = lab.get("lab_composite", 0.0) if lab else 0.0
    lab_is_confirming = bool(lab.get("is_confirming")) if lab else False
    lab_signed = float(lab_val) if lab_is_confirming and abs(lab_val or 0) > 0.1 else 0.0
    lab_quality = min(abs(lab_val or 0), 1.0) if lab_signed != 0.0 else 0.0

    sources = [
        {
            "name": "proxy_confirmation" if proxy_source else "options",
            "kind": "options",
            "independent": not bool(proxy_source),
            "signed_score": opts_signed,
            "quality": opts_quality,
            "proxy_source": proxy_source,
        },
        {
            "name": "momentum",
            "kind": "momentum",
            "independent": True,
            "signed_score": mom_signed,
            "quality": mom_quality,
        },
        {
            "name": "event",
            "kind": "event",
            "independent": True,
            "signed_score": ev_signed,
            "quality": ev_quality,
        },
        {
            "name": "reversion",
            "kind": "reversion",
            "independent": True,
            "signed_score": rev_signed,
            "quality": rev_quality,
        },
        {
            "name": "breakout",
            "kind": "breakout",
            "independent": True,
            "signed_score": bo_signed,
            "quality": bo_quality,
        },
        {
            "name": "lab_factor",
            "kind": "lab_factor",
            "independent": True,
            "signed_score": lab_signed,
            "quality": lab_quality,
        },
    ]

    # ── Regime-aware quality scaling ─────────────────────────────────────────
    # In mean-reverting regimes, trust reversion signals more and momentum less.
    # In trending regimes, trust momentum more and reversion less.
    # This prevents "conflict" when the regime clearly favors one strategy.
    mom = item.get("momentum") or {}
    stock_regime = mom.get("regime", "noisy")

    regime_quality_scale = {
        "trending":       {"momentum": 1.2, "reversion": 0.3, "breakout": 0.8},
        "mean_reverting":  {"momentum": 0.3, "reversion": 1.2, "breakout": 0.8},
        "noisy":          {"momentum": 0.6, "reversion": 0.6, "breakout": 1.2},
    }
    scales = regime_quality_scale.get(stock_regime, regime_quality_scale["noisy"])

    for src in sources:
        kind = src["kind"]
        if kind in scales:
            src["quality"] *= scales[kind]

    # Raw directional score (before macro gate)
    # Only sources with a directional opinion (|score| >= 0.1) contribute to denominator.
    # Neutral sources (no opinion) should not dilute the directional signal.
    directional_sources = [src for src in sources if abs(src["signed_score"]) >= 0.1]
    if directional_sources:
        weighted_sum = sum(src["signed_score"] * src["quality"] for src in directional_sources)
        total_quality = sum(src["quality"] for src in directional_sources)
        raw_direction_score = weighted_sum / total_quality if total_quality > 0 else 0.0
    else:
        raw_direction_score = 0.0

    # Classify source alignment
    active_sources = [src for src in sources if src["quality"] > 0.2]
    aligned = []
    conflicting = []
    neutral_sources = []

    for src in active_sources:
        s = src["signed_score"]
        if abs(s) < 0.1:
            neutral_sources.append(src["name"])
        elif (s > 0) == (raw_direction_score > 0):
            aligned.append(src["name"])
        else:
            conflicting.append(src["name"])

    # Provisional signal type for source-bucket lookup
    if len(conflicting) > 0:
        provisional_signal_type = "divergence"
    elif "event" in aligned and len(aligned) <= 2:
        provisional_signal_type = "event_driven"
    elif "momentum" in aligned:
        provisional_signal_type = "momentum"
    elif "options" in aligned or "proxy_confirmation" in aligned:
        provisional_signal_type = "options"
    else:
        provisional_signal_type = "mixed"

    # Macro gate: regime-aware, per-asset, per-source
    gate_map = _macro_gate(market_context) if market_context else {
        "regime": "unknown",
        "inputs": {"vix_level": None, "spread_10y2y": None},
        "matrix_cell": {"vix": "unknown", "spread": "unknown"},
        "asset": deepcopy(NEUTRAL_ASSET_MULTIPLIERS),
        "source": {"event_driven": 1.00, "momentum": 1.00, "options": 1.00, "mixed": 1.00},
        "overlays": [],
    }

    asset_bucket = _asset_bucket_for_item(item)

    # Provisional direction for gate resolution
    if abs(raw_direction_score) < 0.1:
        provisional_direction = "neutral"
    elif raw_direction_score > 0:
        provisional_direction = "long"
    else:
        provisional_direction = "short"

    # Resolve macro multiplier
    if provisional_direction == "neutral":
        applied_gate = 1.0
    else:
        applied_gate = _resolve_macro_multiplier(
            gate_map,
            asset_bucket=asset_bucket,
            direction=provisional_direction,
            signal_type=provisional_signal_type,
        )

    gated_direction_score = raw_direction_score * applied_gate

    # Cross-asset divergence check
    cross_div = _cross_asset_divergence(item, market_context)
    if cross_div["divergence_detected"]:
        gated_direction_score *= 0.85

    source_stats = _effective_source_stats(active_sources)
    confidence_shrinkage = float(source_stats.get("shrinkage", 1.0))
    shrunk_direction_score = gated_direction_score * confidence_shrinkage

    # Final direction from shrunk score
    if abs(shrunk_direction_score) < 0.1:
        direction = "neutral"
    elif shrunk_direction_score > 0:
        direction = "long"
    else:
        direction = "short"

    # Confidence -- uses gated score and independent source count
    independent_aligned = [
        src["name"] for src in active_sources
        if src["independent"] and src["name"] in aligned
    ]
    n_independent_aligned = len(independent_aligned)
    n_conflicting = len(conflicting)
    has_high_quality_conflict = any(
        src["quality"] > 0.5
        for src in active_sources
        if src["name"] in conflicting
    )

    if n_independent_aligned >= 2 and n_conflicting == 0 and abs(shrunk_direction_score) >= 0.25:
        confidence = "HIGH"
    elif n_independent_aligned >= 1 and not has_high_quality_conflict and abs(shrunk_direction_score) > 0.15:
        confidence = "MODERATE"
    elif n_conflicting > 0 and has_high_quality_conflict:
        confidence = "LOW"
    elif abs(shrunk_direction_score) < 0.1:
        confidence = "NO_SIGNAL"
    else:
        confidence = "LOW"

    # ── Exhaustion downgrade: HIGH → MODERATE when structure contradicts ──
    # This prevents the "label says HIGH but analysis says damaged" problem.
    # Exhaustion flags indicate the signal's structural integrity is degrading
    # even though source alignment still looks good on paper.
    exhaustion_flags = []
    if confidence == "HIGH":
        mom = item.get("momentum") or {}
        opts = item.get("options") or {}
        execution_gate = item.get("execution_gate") or {}

        # Momentum deceleration: trend is decaying or reversing
        accel = mom.get("momentum_accel")
        if accel is not None:
            if accel < 0:
                exhaustion_flags.append("momentum_reversing")
            elif 0 < accel < 0.3:
                exhaustion_flags.append("momentum_decelerating")

        # Cone position: price near probability boundary
        cp68 = opts.get("cone_position_68")
        if cp68 is not None:
            if direction == "long" and cp68 > 0.85:
                exhaustion_flags.append("cone_top")
            elif direction == "short" and cp68 < 0.15:
                exhaustion_flags.append("cone_bottom")

        # Cross-asset divergence already detected above
        if cross_div.get("divergence_detected"):
            exhaustion_flags.append("cross_asset_divergence")

        if execution_gate.get("action") == "do_not_chase":
            exhaustion_flags.append("gap_overshoot")
        elif execution_gate.get("action") == "wait_pullback":
            exhaustion_flags.append("needs_pullback")

        gap_vs_expected_move = execution_gate.get("gap_vs_expected_move")
        if gap_vs_expected_move is not None:
            try:
                if float(gap_vs_expected_move) > 1.0:
                    exhaustion_flags.append("expected_move_consumed")
            except (TypeError, ValueError):
                pass

        # Mean-reverting regime: momentum-ONLY signals in MR regime are
        # less trustworthy. But if reversion is the aligned source, this is
        # the RIGHT strategy for this regime — don't penalize.
        if mom.get("regime") == "mean_reverting" and "momentum" in aligned and "reversion" not in aligned:
            exhaustion_flags.append("mean_reverting_regime")

        if exhaustion_flags:
            confidence = "MODERATE"

    # Signal type label
    if confidence == "NO_SIGNAL":
        signal_type = "no_signal"
    elif n_conflicting > 0:
        signal_type = "divergence"
    elif "event" in aligned and len(aligned) <= 2:
        signal_type = "event_driven"
    elif "momentum" in aligned:
        signal_type = f"{'bullish' if direction == 'long' else 'bearish'}_momentum"
    elif "options" in aligned:
        signal_type = f"{'bullish' if direction == 'long' else 'bearish'}_options"
    elif "proxy_confirmation" in aligned:
        signal_type = f"{'bullish' if direction == 'long' else 'bearish'}_proxy_confirmation"
    elif "reversion" in aligned:
        signal_type = f"{'bullish' if direction == 'long' else 'bearish'}_reversion"
    elif "breakout" in aligned:
        signal_type = f"{'bullish' if direction == 'long' else 'bearish'}_breakout"
    else:
        signal_type = f"{'bullish' if direction == 'long' else 'bearish'}_mixed"

    # Per-source detail
    source_details = {}
    for src in sources:
        if src["quality"] > 0:
            detail = {
                "signed_score": round(src["signed_score"], 3),
                "quality": round(src["quality"], 2),
                "direction": (
                    "bullish" if src["signed_score"] > 0.1
                    else "bearish" if src["signed_score"] < -0.1
                    else "neutral"
                ),
                "independent": src["independent"],
            }
            if src.get("proxy_source"):
                detail["proxy_source"] = src["proxy_source"]
            source_details[src["name"]] = detail

    return {
        "signal_type": signal_type,
        "confidence": confidence,
        "direction": direction,
        "direction_score": round(shrunk_direction_score, 3),
        "direction_score_pre_shrink": round(gated_direction_score, 3),
        "raw_direction_score": round(raw_direction_score, 3),
        "macro_gate": round(applied_gate, 2),
        "confidence_shrinkage": round(confidence_shrinkage, 3),
        "effective_independent_sources": source_stats.get("n_effective", 0.0),
        "total_independent_sources": source_stats.get("n_total", 0.0),
        "macro_regime": gate_map["regime"],
        "macro_matrix_cell": gate_map.get("matrix_cell"),
        "macro_asset_bucket": asset_bucket,
        "macro_overlays": gate_map.get("overlays", []),
        "sources_aligned": aligned,
        "sources_independent_aligned": independent_aligned,
        "sources_conflicting": conflicting,
        "sources_neutral": neutral_sources,
        "source_details": source_details,
        "cross_asset_divergence": cross_div if cross_div.get("divergence_detected") else None,
        "exhaustion_downgrade": exhaustion_flags if exhaustion_flags else None,
    }


def classify_all(
    notable_items: list[dict],
    market_context: Optional[dict] = None,
) -> list[dict]:
    """
    Classify all notable items and merge signal into each item.
    Returns the same list with 'signal' key added to each item.
    """
    for item in notable_items:
        item["signal"] = classify_signal(item, market_context)
    return notable_items


def _execution_action(item: dict[str, Any]) -> str:
    risk = item.get("risk_params") or {}
    gate = item.get("execution_gate") or {}
    if gate:
        return str(risk.get("execution_mode") or gate.get("action") or "unknown")
    return "unknown"


def _main_action_intent(action: str, passes: bool) -> str:
    if passes:
        return "TRADE"
    if action == "do_not_chase":
        return "AVOID"
    if action == "wait_pullback":
        return "WAIT"
    return "OBSERVE"


def main_signal_gate(item: dict[str, Any], headline_gate: dict[str, Any] | None) -> dict[str, Any]:
    """Gate directional evidence into a true main signal.

    ``classify_signal`` answers "is directional evidence aligned?".  This gate
    answers the stricter question "may this be treated as the report's main
    signal/actionable book?".
    """
    signal = item.get("signal") or {}
    headline_mode = str((headline_gate or {}).get("mode") or "unknown").lower()
    report_bucket = str(item.get("report_bucket") or "").lower()
    action = _execution_action(item)
    direction = str(signal.get("direction") or "neutral").lower()
    confidence = str(signal.get("confidence") or "NO_SIGNAL").upper()
    direction_score = abs(float(signal.get("direction_score") or 0.0))
    rr_ratio = (item.get("risk_params") or {}).get("rr_ratio")

    blockers: list[str] = []
    if report_bucket != "core":
        blockers.append(f"report_lane_{report_bucket or 'unknown'}")
    if confidence not in {"HIGH", "MODERATE"}:
        blockers.append(f"confidence_{confidence.lower()}")
    if direction not in {"long", "short"}:
        blockers.append("neutral_direction")
    if direction_score < 0.20:
        blockers.append("thin_direction_score")
    if action != "executable_now":
        blockers.append(f"execution_{action}")
    if signal.get("exhaustion_downgrade"):
        blockers.append("exhaustion_downgrade")
    try:
        if rr_ratio is not None and float(rr_ratio) < 1.5:
            blockers.append("rr_below_1_5")
    except (TypeError, ValueError):
        pass

    passes = not blockers
    role = "main_signal" if passes else "directional_observation"
    if confidence in {"LOW", "NO_SIGNAL"}:
        role = "notability_only"

    return {
        "status": "pass" if passes else "blocked",
        "role": role,
        "action_intent": _main_action_intent(action, passes),
        "headline_mode": headline_mode,
        "report_bucket": report_bucket or None,
        "execution_action": action,
        "blockers": blockers,
    }


def apply_main_signal_gate(bundle: dict[str, Any]) -> dict[str, Any]:
    """Annotate every notable item with the strict main-signal gate result."""
    headline_gate = bundle.get("headline_gate") or {}
    counts = {"main_signal": 0, "directional_observation": 0, "notability_only": 0}
    for item in bundle.get("notable_items", []):
        gate = main_signal_gate(item, headline_gate)
        item["main_signal_gate"] = gate
        item.setdefault("signal", {})["main_signal_gate"] = gate
        counts[gate["role"]] = counts.get(gate["role"], 0) + 1
    bundle["main_signal_summary"] = {
        "headline_mode": headline_gate.get("mode"),
        "counts": counts,
        "rule": "Main signal requires core lane, HIGH/MODERATE directional evidence, executable_now, sufficient RR, and no exhaustion blocker. Headline mode is advisory context only.",
    }
    return bundle
