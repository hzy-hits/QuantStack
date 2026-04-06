"""
Phase 3 — Composite scoring for DYP screen candidates.

Takes candidates that passed Step 1-2 (eligibility + DYP gate) and computes:
  - Technical Score (TS): 50MA, 200MA, 52W drawdown, RSI(14)
  - Safety Score (SS): dividend stability, cut detection, years, interval consistency
  - Composite: DYP 35% + TS 35% + SS 30%

Labels: STRONG_DIP, MODERATE_DIP, FAIR
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import duckdb
import structlog

log = structlog.get_logger()


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _compute_rsi(closes: list[float], period: int = 14) -> float | None:
    """Compute RSI from a list of close prices (oldest first)."""
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_technical_score(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of: date,
) -> dict:
    """
    Compute Technical Score (0-100) for a single symbol.
    Returns dict with ts, sub-components, and raw values.
    """
    # Fetch ~300 days of close prices for MA/RSI/drawdown
    start = as_of - timedelta(days=400)
    rows = con.execute("""
        SELECT date, close FROM prices_daily
        WHERE symbol = ? AND date >= ? AND date <= ?
        ORDER BY date ASC
    """, [symbol, start, as_of]).fetchall()

    if len(rows) < 50:
        return {"ts": 0, "pct_below_50ma": None, "pct_below_200ma": None,
                "drawdown_52w_pct": None, "rsi14": None, "insufficient_data": True}

    closes = [r[1] for r in rows]
    current_price = closes[-1]

    # SMA 50
    sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
    # SMA 200
    sma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else None
    # 52-week high (252 trading days)
    high_252 = max(closes[-252:]) if len(closes) >= 20 else None
    # RSI 14
    rsi14 = _compute_rsi(closes, 14)

    # Sub-scores
    score_50ma = 0.0
    pct_below_50ma = None
    if sma50 and current_price < sma50:
        pct_below_50ma = (sma50 - current_price) / sma50
        score_50ma = 25 * _clamp(pct_below_50ma / 0.10, 0, 1)

    score_200ma = 0.0
    pct_below_200ma = None
    if sma200 and current_price < sma200:
        pct_below_200ma = (sma200 - current_price) / sma200
        score_200ma = 25 * _clamp(pct_below_200ma / 0.20, 0, 1)

    score_52w_dd = 0.0
    drawdown_52w_pct = None
    if high_252 and high_252 > 0:
        drawdown_52w_pct = (high_252 - current_price) / high_252
        score_52w_dd = 25 * _clamp(drawdown_52w_pct / 0.35, 0, 1)

    score_rsi = 0.0
    if rsi14 is not None:
        if rsi14 < 30:
            score_rsi = 25
        elif rsi14 < 40:
            score_rsi = 12
        elif rsi14 > 65:
            score_rsi = -10

    ts = _clamp(score_50ma + score_200ma + score_52w_dd + score_rsi)

    return {
        "ts": round(ts, 1),
        "pct_below_50ma": round(pct_below_50ma * 100, 2) if pct_below_50ma else 0.0,
        "pct_below_200ma": round(pct_below_200ma * 100, 2) if pct_below_200ma else 0.0,
        "drawdown_52w_pct": round(drawdown_52w_pct * 100, 2) if drawdown_52w_pct else 0.0,
        "rsi14": round(rsi14, 1) if rsi14 is not None else None,
        "insufficient_data": False,
    }


def compute_safety_score(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of: date,
) -> dict:
    """
    Compute Dividend Safety Score (0-100) for a single symbol.
    Uses only the dividends table (no external fundamentals needed).
    """
    rows = con.execute("""
        SELECT ex_date, cash_amount, is_special
        FROM dividends
        WHERE symbol = ? AND is_special = FALSE AND ex_date <= ?
        ORDER BY ex_date ASC
    """, [symbol, as_of]).fetchall()

    flags: list[str] = []

    if len(rows) < 4:
        flags.append("insufficient_dividend_history")
        return {"ss": 0, "stability_score": 0, "cut_score": 0, "years_score": 0,
                "interval_score": 0, "latest_div_change_pct": None,
                "consecutive_years": 0, "amount_cv": None, "interval_cv": None,
                "flags": flags}

    amounts = [r[1] for r in rows]
    dates = [r[0] for r in rows]

    # ── Stability (35 pts): CV of recent amounts ──
    recent = amounts[-min(12, len(amounts)):]
    mean_amt = sum(recent) / len(recent) if recent else 0
    if mean_amt > 0 and len(recent) >= 2:
        variance = sum((a - mean_amt) ** 2 for a in recent) / (len(recent) - 1)
        std_amt = math.sqrt(variance)
        amount_cv = std_amt / mean_amt
    else:
        amount_cv = 1.0
    stability_score = 35 * _clamp(1 - amount_cv / 0.35, 0, 1)

    # ── Cut detection (30 pts) ──
    latest_div_change_pct = None
    cut_score = 30.0  # default: no cut
    if len(amounts) >= 2:
        prev = amounts[-2]
        curr = amounts[-1]
        if prev > 0:
            latest_div_change_pct = (curr - prev) / prev * 100
            if latest_div_change_pct < -10:
                cut_score = 0.0
                flags.append("recent_cut_gt_10pct")

    # ── Consecutive years (20 pts) ──
    years_with_div = set()
    for d in dates:
        years_with_div.add(d.year)
    # Count consecutive years ending at as_of.year
    consecutive = 0
    for y in range(as_of.year, as_of.year - 20, -1):
        if y in years_with_div:
            consecutive += 1
        else:
            break
    years_score = 20 * _clamp(consecutive / 10, 0, 1)

    # ── Interval consistency (15 pts) ──
    if len(dates) >= 3:
        gaps = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]
        # Use recent gaps only
        recent_gaps = gaps[-min(8, len(gaps)):]
        mean_gap = sum(recent_gaps) / len(recent_gaps)
        if mean_gap > 0 and len(recent_gaps) >= 2:
            gap_var = sum((g - mean_gap) ** 2 for g in recent_gaps) / (len(recent_gaps) - 1)
            interval_cv = math.sqrt(gap_var) / mean_gap
        else:
            interval_cv = 1.0
    else:
        interval_cv = 1.0
    interval_score = 15 * _clamp(1 - interval_cv / 0.25, 0, 1)

    # Additional flags
    if len(dates) >= 2:
        days_since_last = (as_of - dates[-1]).days
        # Expected cadence from mean gap
        if len(gaps) >= 2 and mean_gap > 0 and days_since_last > mean_gap * 1.5:
            flags.append("payment_overdue_vs_cadence")

    if amount_cv > 0.30:
        flags.append("high_amount_volatility")
    if interval_cv > 0.20:
        flags.append("irregular_payout_schedule")

    # Check for recent special dividend
    has_recent_special = con.execute("""
        SELECT COUNT(*) FROM dividends
        WHERE symbol = ? AND is_special = TRUE
          AND ex_date > ? - INTERVAL '365 days' AND ex_date <= ?
    """, [symbol, as_of, as_of]).fetchone()[0]
    if has_recent_special > 0:
        flags.append("recent_special_dividend")

    ss = round(stability_score + cut_score + years_score + interval_score, 1)

    return {
        "ss": ss,
        "stability_score": round(stability_score, 1),
        "cut_score": round(cut_score, 1),
        "years_score": round(years_score, 1),
        "interval_score": round(interval_score, 1),
        "latest_div_change_pct": round(latest_div_change_pct, 1) if latest_div_change_pct is not None else None,
        "consecutive_years": consecutive,
        "amount_cv": round(amount_cv, 3) if amount_cv is not None else None,
        "interval_cv": round(interval_cv, 3) if interval_cv is not None else None,
        "flags": flags,
    }


def score_dyp_candidates(
    con: duckdb.DuckDBPyConnection,
    candidates: list[dict],
    as_of: date,
) -> list[dict]:
    """
    Score DYP candidates with TS + SS, compute composite, assign labels.
    Candidates come from run_dyp_screen() with dyp, current_yield_pct, etc.
    Returns enriched list sorted by composite descending.
    """
    scored = []
    for c in candidates:
        sym = c["symbol"]
        dyp = c["dyp"]

        ts_data = compute_technical_score(con, sym, as_of)
        ss_data = compute_safety_score(con, sym, as_of)

        ts = ts_data["ts"]
        ss = ss_data["ss"]
        composite = round(0.35 * dyp + 0.35 * ts + 0.30 * ss, 1)

        # Labels — STRONG_DIP capped if any red safety flag
        red_flags = [f for f in ss_data.get("flags", [])
                     if f in ("recent_cut_gt_10pct", "payment_overdue_vs_cadence")]
        if composite >= 70 and dyp >= 80 and not red_flags:
            label = "STRONG_DIP"
        elif composite >= 55 and dyp >= 70:
            label = "MODERATE_DIP"
        elif composite >= 40:
            label = "FAIR"
        else:
            label = "BELOW_THRESHOLD"

        scored.append({
            **c,
            "technical_score": ts,
            "safety_score": ss,
            "composite_score": composite,
            "label": label,
            # TS detail
            "pct_below_50ma": ts_data.get("pct_below_50ma"),
            "pct_below_200ma": ts_data.get("pct_below_200ma"),
            "drawdown_52w_pct": ts_data.get("drawdown_52w_pct"),
            "rsi14": ts_data.get("rsi14"),
            # SS detail
            "latest_div_change_pct": ss_data.get("latest_div_change_pct"),
            "consecutive_years": ss_data.get("consecutive_years"),
            "amount_cv": ss_data.get("amount_cv"),
            "interval_cv": ss_data.get("interval_cv"),
            "flags": ss_data.get("flags", []),
        })

    scored.sort(key=lambda x: x["composite_score"], reverse=True)
    log.info("dyp_scoring_done", total=len(scored),
             strong=sum(1 for s in scored if s["label"] == "STRONG_DIP"),
             moderate=sum(1 for s in scored if s["label"] == "MODERATE_DIP"),
             fair=sum(1 for s in scored if s["label"] == "FAIR"))
    return scored
