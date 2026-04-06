"""Options scoring: IV level, IV delta, flow intensity, and composite options score."""
from __future__ import annotations

import math
from typing import Any

from ._common import _clamp01, _safe, _weighted_average_available, _parse_unusual


def _flow_intensity(unusual_activity: list[dict]) -> float:
    """
    Score 0-1 based on unusual options flow magnitude.
    Captures what IV/skew miss: raw conviction via volume.
    """
    if not unusual_activity:
        return 0.0

    total_vol = sum(u.get("volume", 0) for u in unusual_activity)
    max_ratio = max((u.get("vol_oi_ratio") or 0) for u in unusual_activity)
    n_strikes = len(unusual_activity)

    # Volume component: log scale -- 1k=0.4, 10k=0.7, 50k+=1.0
    vol_score = min(math.log10(max(total_vol, 1)) / math.log10(50000), 1.0)

    # Max vol/OI ratio: 5x=0.3, 20x=0.7, 50x+=1.0
    ratio_score = min(max_ratio / 50.0, 1.0) if max_ratio else 0.0

    # Breadth: more strikes with unusual activity = more conviction
    breadth_score = min(n_strikes / 5.0, 1.0)

    return 0.5 * vol_score + 0.3 * ratio_score + 0.2 * breadth_score


def _compute_iv_delta_metrics(
    cur_snap: dict | None,
    hist_snap: dict | None,
    cur_analysis: dict | None,
    hist_analysis: dict | None,
) -> dict[str, Any]:
    """
    Compute 7-day repricing metrics from options snapshot + analysis.
    Returns dict with iv_delta score and component scores.
    """
    result: dict[str, Any] = {
        "iv_delta": None,
        "iv_change_score": None,
        "iv_change_pct_pts": None,
        "pc_change_score": None,
        "pc_delta": None,
        "skew_change_score": None,
        "skew_delta": None,
        "history_status": "missing_7d_snapshot",
        "history_as_of": None,
    }

    if not cur_snap or not hist_snap:
        return result

    hist_as_of = hist_snap.get("hist_as_of") or hist_snap.get("as_of")
    result["history_as_of"] = str(hist_as_of) if hist_as_of else None
    result["history_status"] = "available"

    # ATM IV change (percentage-point and relative)
    cur_iv = cur_snap.get("atm_iv")
    hist_iv = hist_snap.get("atm_iv")
    if cur_iv is not None and hist_iv is not None and float(hist_iv) > 0:
        cur_iv_f = float(cur_iv)
        hist_iv_f = float(hist_iv)
        iv_change_pct_pts = cur_iv_f - hist_iv_f
        result["iv_change_pct_pts"] = round(iv_change_pct_pts, 2)
        # Absolute: +20 vol points saturates
        iv_change_abs_score = _clamp01(abs(iv_change_pct_pts) / 20.0)
        # Relative: 2x IV saturates
        iv_change_log = abs(math.log((cur_iv_f + 1e-6) / max(hist_iv_f, 1e-6)))
        iv_change_rel_score = _clamp01(iv_change_log / math.log(2.0))
        result["iv_change_score"] = max(iv_change_abs_score, iv_change_rel_score)

    # Put/call ratio change (log-ratio to handle small denominators)
    cur_pc = cur_snap.get("put_call_vol_ratio")
    hist_pc = hist_snap.get("put_call_vol_ratio")
    if cur_pc is not None and hist_pc is not None:
        cur_pc_f = float(cur_pc)
        hist_pc_f = float(hist_pc)
        result["pc_delta"] = round(cur_pc_f - hist_pc_f, 3)
        result["pc_change_score"] = _clamp01(
            abs(math.log((cur_pc_f + 0.25) / (hist_pc_f + 0.25))) / math.log(3.0)
        )

    # Skew change (from analysis tables)
    cur_skew = float(cur_analysis["iv_skew"]) if cur_analysis and cur_analysis.get("iv_skew") is not None else None
    hist_skew = float(hist_analysis["iv_skew"]) if hist_analysis and hist_analysis.get("iv_skew") is not None else None
    if cur_skew is not None and hist_skew is not None:
        result["skew_delta"] = round(cur_skew - hist_skew, 4)
        result["skew_change_score"] = _clamp01(abs(cur_skew - hist_skew) / 0.35)

    # Aggregate iv_delta (weighted average of available components)
    result["iv_delta"] = _weighted_average_available([
        (0.5, result["iv_change_score"]),
        (0.3, result["pc_change_score"]),
        (0.2, result["skew_change_score"]),
    ])

    return result


def _compute_flow_signal(
    cur_unusual: list[dict],
    hist_unusual: list[dict],
) -> dict[str, Any]:
    """
    Compute flow intensity with novelty awareness.
    flow_intensity = 0.7 * flow_level + 0.3 * flow_delta
    """
    flow_level = _flow_intensity(cur_unusual)
    hist_flow_level = _flow_intensity(hist_unusual)

    if not hist_unusual:
        return {
            "flow_level": flow_level,
            "flow_delta": 0.0,
            "flow_burst": 0.0,
            "strike_novelty": 0.0,
            "flow_intensity": flow_level,
            "flow_history_available": False,
        }

    # Flow burst: new flow above historical baseline
    flow_burst = _clamp01(max(flow_level - hist_flow_level, 0.0) / 0.5)

    # Strike novelty: fraction of current strikes that are new
    def _strike_key(u: dict) -> tuple:
        return (u.get("type", ""), round(float(u.get("strike", 0)), 2))

    current_keys = {_strike_key(u) for u in cur_unusual} if cur_unusual else set()
    hist_keys = {_strike_key(u) for u in hist_unusual} if hist_unusual else set()
    strike_novelty = (
        len(current_keys - hist_keys) / len(current_keys)
        if current_keys else 0.0
    )

    flow_delta = 0.7 * flow_burst + 0.3 * strike_novelty
    flow_intensity_val = 0.7 * flow_level + 0.3 * flow_delta

    return {
        "flow_level": flow_level,
        "flow_delta": flow_delta,
        "flow_burst": flow_burst,
        "strike_novelty": strike_novelty,
        "flow_intensity": flow_intensity_val,
        "flow_history_available": True,
    }


def score_options(
    sym: str,
    opt: dict | None,
    opt_7d: dict | None,
    oa: dict | None,
    oa_7d: dict | None,
    atr: float,
    ac: float,
) -> tuple[float, float, dict]:
    """
    Compute options score for a symbol.

    Returns:
        (options_score, iv_ratio_level, opts_payload)
    """
    options_score = 0.0
    iv_ratio_level = 0.0
    opts_payload: dict[str, Any] = {}

    proxy_source = opt.get("_proxy_source") if opt else None
    is_proxy_options = bool(proxy_source)

    iv_is_broken = False
    pc_ratio = None
    atm_iv = 0.0

    if opt:
        atm_iv = _safe(opt.get("atm_iv"), 0.0)
        exp_move = _safe(opt.get("expected_move_pct"), 0.0)
        pc_ratio = _safe(opt.get("put_call_vol_ratio"))
        # Detect broken IV (weekend calibration artifacts, missing data)
        iv_is_broken = atm_iv < 5.0  # annualized IV < 5% is implausible
        # IV ratio level (existing metric, renamed for clarity)
        if atr > 0 and ac > 0 and not iv_is_broken:
            hist_daily_vol_pct = (atr / ac) * 100.0
            iv_daily_equiv = atm_iv / math.sqrt(252) if atm_iv else 0.0
            if hist_daily_vol_pct > 0:
                iv_ratio_level = min(iv_daily_equiv / hist_daily_vol_pct, 2.0) / 2.0
        opts_payload = {
            "atm_iv_pct": atm_iv,
            "expected_move_pct": exp_move,
            "put_call_ratio": pc_ratio,
            "options_data_origin": proxy_source or sym,
            "options_independent": not is_proxy_options,
            "options_kind": "proxy" if is_proxy_options else "direct",
        }
        if is_proxy_options:
            opts_payload["_proxy_source"] = proxy_source
            opts_payload["proxy_source"] = proxy_source
            opts_payload["proxy_note"] = (
                f"Uses {proxy_source} options as a proxy; not independent confirmation."
            )

    # Enrich with enhanced analysis (probability cone, skew, bias)
    if oa:
        opts_payload["probability_cone"] = {
            "expiry": oa.get("expiry"),
            "days_to_exp": oa.get("days_to_exp"),
            "range_68": [_safe(oa.get("range_68_low")), _safe(oa.get("range_68_high"))],
            "range_95": [_safe(oa.get("range_95_low")), _safe(oa.get("range_95_high"))],
        }
        # Cone position: where current price sits within the probability cone
        # 0.0 = at lower bound, 1.0 = at upper bound
        r68 = opts_payload["probability_cone"]["range_68"]
        if r68[0] is not None and r68[1] is not None and r68[1] > r68[0]:
            opts_payload["cone_position_68"] = round(
                (ac - r68[0]) / (r68[1] - r68[0]), 3
            )
        opts_payload["iv_skew"] = _safe(oa.get("iv_skew"))
        opts_payload["bias_signal"] = oa.get("bias_signal")
        opts_payload["liquidity_score"] = oa.get("liquidity_score")
        opts_payload["chain_width"] = oa.get("chain_width")
        opts_payload["avg_spread_pct"] = _safe(oa.get("avg_spread_pct"))

    # IV delta metrics (7-day repricing)
    delta_metrics = _compute_iv_delta_metrics(opt, opt_7d, oa, oa_7d)
    iv_delta = delta_metrics.get("iv_delta")

    # Flow intensity with novelty awareness
    cur_unusual = _parse_unusual(oa.get("unusual_strikes") if oa else None)
    hist_unusual = _parse_unusual(oa_7d.get("unusual_strikes") if oa_7d else None)
    flow_metrics = _compute_flow_signal(cur_unusual, hist_unusual)
    flow_intensity_val = flow_metrics["flow_intensity"]

    # Unusual activity in payload
    if cur_unusual:
        opts_payload["unusual_activity"] = cur_unusual

    # Compute options score with proxy awareness
    flow_used: float
    if opt and is_proxy_options:
        # Proxy: exclude flow, 50% haircut on IV-only score
        flow_used = 0.0
        if iv_delta is None:
            options_score = 0.5 * iv_ratio_level
        else:
            options_score = 0.5 * (0.4 * iv_ratio_level + 0.3 * iv_delta) / 0.7
    elif opt:
        # Direct options: full scoring
        flow_used = flow_intensity_val
        if iv_is_broken:
            # IV data is implausible (< 5% annualized) -- score from flow only
            # Use P/C ratio extremity as a flow-like signal
            pc_extremity = 0.0
            if pc_ratio is not None:
                if pc_ratio < 0.15:
                    pc_extremity = 0.9
                elif pc_ratio < 0.3:
                    pc_extremity = 0.7
                elif pc_ratio < 0.5:
                    pc_extremity = 0.4
                elif pc_ratio > 3.0:
                    pc_extremity = 0.9
                elif pc_ratio > 2.0:
                    pc_extremity = 0.7
                elif pc_ratio > 1.5:
                    pc_extremity = 0.4
            options_score = max(0.5 * pc_extremity + 0.5 * flow_used, flow_used)
        elif iv_delta is None:
            # Fallback: renormalize level + flow (like old 0.6/0.4 blend)
            options_score = (0.4 * iv_ratio_level + 0.3 * flow_used) / 0.7
        else:
            options_score = 0.4 * iv_ratio_level + 0.3 * iv_delta + 0.3 * flow_used
            # Extreme repricing floor: large IV jump with confirmation
            if (
                (delta_metrics.get("iv_change_score") or 0.0) >= 0.85
                and (
                    (delta_metrics.get("pc_change_score") or 0.0) >= 0.50
                    or (delta_metrics.get("skew_change_score") or 0.0) >= 0.50
                    or (flow_metrics.get("flow_delta") or 0.0) >= 0.35
                )
            ):
                options_score = max(options_score, 0.65)
    else:
        flow_used = 0.0

    # Expand opts_payload with new scoring fields
    if opt and iv_is_broken:
        opts_payload["iv_data_quality"] = "broken"
        opts_payload["iv_data_quality_note"] = (
            f"ATM IV {atm_iv:.1f}% is implausibly low; "
            f"options_score uses P/C ratio + flow instead of IV."
        )
    opts_payload["iv_ratio_level"] = round(iv_ratio_level, 3)
    opts_payload["iv_delta"] = round(iv_delta, 3) if iv_delta is not None else None
    opts_payload["iv_change_pct_pts_7d"] = delta_metrics.get("iv_change_pct_pts")
    opts_payload["put_call_ratio_delta_7d"] = delta_metrics.get("pc_delta")
    opts_payload["skew_delta_7d"] = delta_metrics.get("skew_delta")
    opts_payload["flow_level"] = round(flow_metrics["flow_level"], 3)
    opts_payload["flow_delta"] = round(flow_metrics["flow_delta"], 3)
    opts_payload["flow_intensity"] = round(flow_intensity_val, 3)
    opts_payload["flow_intensity_used"] = round(flow_used, 3)
    opts_payload["history_status"] = delta_metrics.get("history_status")
    opts_payload["history_as_of"] = delta_metrics.get("history_as_of")
    if is_proxy_options:
        opts_payload["options_score_modifier"] = 0.5
        opts_payload["flow_intensity_note"] = (
            f"Raw flow comes from {proxy_source}; excluded from {sym} scoring "
            f"because ETF flow is not a clean proxy for underlying positioning."
        )

    return options_score, iv_ratio_level, opts_payload
