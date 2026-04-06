"""Signal extraction functions: options, momentum, event, reversion, breakout."""
from __future__ import annotations


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _options_signal(opts: dict) -> tuple[float, float]:
    """
    Derive directional signal from options data.
    Returns (signed_score, quality).

    Proxy-derived options (tagged with _proxy_source) get:
    - No unusual flow amplification (ETF flow ≠ underlying positioning)
    - No flow_intensity quality boost
    - 50% quality haircut
    """
    if not opts:
        return 0.0, 0.0

    proxy_source = opts.get("_proxy_source") or opts.get("proxy_source")
    is_proxy = bool(proxy_source)

    score = 0.0
    signals = 0
    total_weight = 0.0

    # Detect broken IV (weekend calibration artifacts)
    atm_iv = opts.get("atm_iv_pct", 0.0) or 0.0
    iv_broken = atm_iv < 5.0 and atm_iv >= 0.0

    # IV skew: <0.9 bullish, >1.15 bearish
    # Skip when IV is broken — skew from bad IV data is noise
    skew = opts.get("iv_skew")
    if skew is not None and not iv_broken:
        if skew < 0.85:
            score += 0.8
        elif skew < 0.90:
            score += 0.4
        elif skew > 1.20:
            score -= 0.8
        elif skew > 1.15:
            score -= 0.4
        signals += 1
        total_weight += 1.0

    # Put/call volume ratio: <0.5 bullish, >1.5 bearish
    pc = opts.get("put_call_ratio")
    if pc is not None:
        if pc < 0.3:
            score += 0.7
        elif pc < 0.7:
            score += 0.3
        elif pc > 2.0:
            score -= 0.7
        elif pc > 1.3:
            score -= 0.3
        signals += 1
        total_weight += 1.0

    # Bias signal (tiebreaker)
    bias = opts.get("bias_signal")
    if bias == "bullish":
        score += 0.2
    elif bias == "bearish":
        score -= 0.2

    # Unusual flow — skip for proxy options
    unusual = opts.get("unusual_activity", [])
    if unusual and isinstance(unusual, list) and not is_proxy:
        total_vol = sum(u.get("volume", 0) for u in unusual)
        max_ratio = max((u.get("vol_oi_ratio") or 0) for u in unusual)
        call_vol = sum(u["volume"] for u in unusual if u.get("type") == "call")
        put_vol = sum(u["volume"] for u in unusual if u.get("type") == "put")
        if total_vol > 500 and max_ratio > 5:
            flow_direction = 0.3 if call_vol > put_vol * 2 else (-0.3 if put_vol > call_vol * 2 else 0.0)
            intensity = min(total_vol / 20000.0, 1.0)
            score += flow_direction * intensity
            signals += 1
            total_weight += 1.0

    if signals == 0:
        return 0.0, 0.0

    signed = _clamp(score / max(total_weight, 1.0))

    # Quality from liquidity
    liq = opts.get("liquidity_score", "poor")
    if liq == "good":
        quality = 0.9
    elif liq == "fair":
        quality = 0.6
    else:
        quality = 0.3

    # Flow intensity quality boost — only for direct options
    flow_intensity = opts.get("flow_intensity", 0.0)
    if not is_proxy and flow_intensity > 0.5:
        quality = min(quality + 0.2, 1.0)
    elif not is_proxy and flow_intensity > 0.3:
        quality = min(quality + 0.1, 1.0)

    # Proxy options: 50% quality haircut
    if is_proxy:
        quality *= 0.5

    return signed, quality


def _momentum_signal(mom: dict) -> tuple[float, float]:
    """
    Derive directional signal from momentum analysis.
    Returns (signed_score, quality).
    """
    if not mom:
        return 0.0, 0.0

    score = 0.0
    signals = 0

    # trend_prob: deviation from 0.5
    tp = mom.get("trend_prob")
    if tp is not None:
        deviation = (tp - 0.5) * 4.0  # scale: 0.55 → 0.2, 0.60 → 0.4
        score += _clamp(deviation, -0.5, 0.5)
        signals += 1

    # 5D return direction (short-term momentum)
    mom_5d = mom.get("mom_5d") or mom.get("ret_5d_pct")
    if mom_5d is not None:
        if abs(mom_5d) > 1.0:
            score += _clamp(mom_5d / 10.0, -0.5, 0.5)
            signals += 1

    # 20D return (medium-term trend)
    mom_20d = mom.get("mom_20d") or mom.get("ret_20d_pct")
    if mom_20d is not None:
        if abs(mom_20d) > 2.0:
            score += _clamp(mom_20d / 20.0, -0.3, 0.3)
            signals += 1

    # z-score extremity (unsigned — just amplifies confidence)
    z = mom.get("z_score")
    if z is not None and abs(z) > 1.5:
        if score != 0:
            score += 0.2 * (1 if score > 0 else -1)

    if signals == 0:
        return 0.0, 0.0

    signed = _clamp(score / max(signals, 1.0))

    strength = mom.get("strength_bucket", "moderate")
    quality_map = {"strong": 0.8, "moderate": 0.6, "weak": 0.4, "inconclusive": 0.2}
    quality = quality_map.get(strength, 0.5)

    regime = mom.get("regime", "noisy")
    if regime == "trending":
        quality = min(quality + 0.15, 1.0)
    elif regime == "mean_reverting":
        quality = max(quality - 0.15, 0.1)

    return signed, quality


def _event_signal(events: list[dict], earnings_risk: dict) -> tuple[float, float]:
    """
    Derive directional signal from events.
    Returns (signed_score, quality).
    """
    if not events and not earnings_risk:
        return 0.0, 0.0

    score = 0.0
    quality = 0.0

    for ev in events:
        ev_type = ev.get("type", "")

        if ev_type == "earnings":
            surprise = ev.get("surprise_pct")
            if surprise is not None:
                score += _clamp(surprise / 15.0, -1.0, 1.0) * 0.6
                quality = max(quality, 0.7)
            else:
                quality = max(quality, 0.3)

        elif ev_type == "8-K_filing":
            quality = max(quality, 0.4)

        elif ev_type.startswith("index_"):
            days_ago = ev.get("days_ago", 30)
            recency = max(0.0, 1.0 - days_ago / 14.0)
            if "add" in ev_type:
                score += 0.5 * (0.5 + 0.5 * recency)
                quality = max(quality, 0.6)
            else:
                score -= 0.4 * (0.5 + 0.5 * recency)
                quality = max(quality, 0.5)

    if earnings_risk:
        er_p_upside = earnings_risk.get("p_upside")
        if er_p_upside is not None and not earnings_risk.get("surprise_unknown", False):
            deviation = (er_p_upside - 0.5) * 2.0
            score += _clamp(deviation, -0.4, 0.4)
            quality = max(quality, 0.5)

    signed = _clamp(score)
    return signed, min(quality, 1.0)


def _reversion_signal(mr: dict) -> tuple[float, float]:
    """
    Derive directional signal from mean-reversion analysis.
    Returns (signed_score, quality).

    bullish_reversion → positive score (oversold bounce expected)
    bearish_reversion → negative score (overbought pullback expected)
    """
    if not mr:
        return 0.0, 0.0

    direction = mr.get("reversion_direction", "neutral")
    rev_score = mr.get("reversion_score", 0.0) or 0.0

    if direction == "neutral" or rev_score < 0.2:
        return 0.0, 0.0

    # Signed score: bullish reversion = +, bearish reversion = -
    if direction == "bullish_reversion":
        signed = _clamp(rev_score, 0.0, 1.0)
    elif direction == "bearish_reversion":
        signed = _clamp(-rev_score, -1.0, 0.0)
    else:
        return 0.0, 0.0

    # Quality based on score strength
    strength = mr.get("strength_bucket", "inconclusive")
    quality_map = {"strong": 0.8, "moderate": 0.6, "weak": 0.3, "inconclusive": 0.1}
    quality = quality_map.get(strength, 0.3)

    return signed, quality


def _breakout_signal(bo: dict) -> tuple[float, float]:
    """
    Derive directional signal from breakout detection.
    Returns (signed_score, quality).

    bullish_breakout → positive score
    bearish_breakout → negative score
    coiled → neutral (no directional opinion, but contributes to attention)
    """
    if not bo:
        return 0.0, 0.0

    direction = bo.get("breakout_direction", "none")
    bo_score = bo.get("breakout_score", 0.0) or 0.0

    if direction == "none" or bo_score < 0.15:
        return 0.0, 0.0

    # Coiled = no direction yet, just volatility compression
    if direction == "coiled":
        return 0.0, 0.0

    # Signed score: bullish breakout = +, bearish breakout = -
    if direction == "bullish_breakout":
        signed = _clamp(bo_score, 0.0, 1.0)
    elif direction == "bearish_breakout":
        signed = _clamp(-bo_score, -1.0, 0.0)
    else:
        return 0.0, 0.0

    # Quality based on score strength
    strength = bo.get("strength_bucket", "inconclusive")
    quality_map = {"strong": 0.85, "moderate": 0.6, "weak": 0.3, "inconclusive": 0.1}
    quality = quality_map.get(strength, 0.3)

    return signed, quality
