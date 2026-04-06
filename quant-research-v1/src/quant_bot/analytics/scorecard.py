"""
Algorithm scorecard: compare prior predictions to actual outcomes.
Builds trust calibration for the LLM.

Looks at analysis_daily signals from the last N trading days and checks
whether the predicted direction materialized in 5D forward returns.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import duckdb
import structlog

log = structlog.get_logger()


def compute_scorecard(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    lookback_days: int = 20,
) -> dict[str, Any]:
    """
    Look at momentum_risk signals from the last `lookback_days` trading days.
    For each:
    1. Load the signal direction (p_upside > 0.5 = bullish, else bearish)
    2. Load the actual 5D forward return from prices_daily
    3. Was the prediction correct? (direction match)

    Also check cointegration predictions:
    - Cointegration spread_zscore > 2: did spread revert within half_life?

    Returns: {
        momentum_accuracy: {calls, correct, accuracy},
        by_confidence: [{bucket, calls, correct, accuracy}],
        recent_misses: [{date, symbol, predicted, actual_ret}],
        cointegration_accuracy: {calls, correct, accuracy},
    }
    """
    result: dict[str, Any] = {
        "momentum_accuracy": {"calls": 0, "correct": 0, "accuracy": None},
        "by_confidence": [],
        "recent_misses": [],
        "cointegration_accuracy": {"calls": 0, "correct": 0, "accuracy": None},
    }

    cutoff = as_of - timedelta(days=lookback_days * 2)  # extra buffer for weekends
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    as_of_str = as_of.strftime("%Y-%m-%d")

    # ── Momentum signal accuracy ────────────────────────────────────────────
    try:
        rows = con.execute("""
            SELECT a.symbol, a.date, a.p_upside, a.strength_bucket,
                   p_fwd.adj_close AS fwd_price,
                   p_cur.adj_close AS cur_price
            FROM analysis_daily a
            INNER JOIN prices_daily p_cur
                ON p_cur.symbol = a.symbol AND p_cur.date = a.date
            INNER JOIN (
                SELECT symbol, date, adj_close,
                       LAG(date, 5) OVER (PARTITION BY symbol ORDER BY date) AS date_5ago
                FROM prices_daily
            ) p_fwd
                ON p_fwd.symbol = a.symbol
                AND p_fwd.date_5ago = a.date
            WHERE a.module_name = 'momentum_risk'
              AND a.date >= ?
              AND a.date < ?
              AND a.p_upside IS NOT NULL
              AND p_cur.adj_close IS NOT NULL
              AND p_fwd.adj_close IS NOT NULL
        """, [cutoff_str, as_of_str]).fetchall()
    except Exception as e:
        log.warning("scorecard_momentum_query_failed", error=str(e))
        rows = []

    if not rows:
        log.info("scorecard_no_historical_signals")
        return result

    total_calls = 0
    total_correct = 0
    bucket_stats: dict[str, dict] = {}
    misses: list[dict] = []

    for sym, sig_date, p_upside, strength, fwd_price, cur_price in rows:
        if cur_price is None or fwd_price is None or cur_price <= 0:
            continue

        predicted_up = p_upside > 0.5
        actual_ret = (fwd_price / cur_price - 1.0) * 100.0
        actual_up = actual_ret > 0

        correct = predicted_up == actual_up
        total_calls += 1
        if correct:
            total_correct += 1
        else:
            misses.append({
                "date": str(sig_date),
                "symbol": sym,
                "predicted": "up" if predicted_up else "down",
                "actual_ret_5d_pct": round(actual_ret, 2),
                "p_upside": round(float(p_upside), 3),
            })

        # By strength bucket
        bucket = strength or "unknown"
        bs = bucket_stats.setdefault(bucket, {"calls": 0, "correct": 0})
        bs["calls"] += 1
        if correct:
            bs["correct"] += 1

    result["momentum_accuracy"] = {
        "calls": total_calls,
        "correct": total_correct,
        "accuracy": round(total_correct / total_calls, 3) if total_calls > 0 else None,
    }

    result["by_confidence"] = [
        {
            "bucket": bucket,
            "calls": stats["calls"],
            "correct": stats["correct"],
            "accuracy": round(stats["correct"] / stats["calls"], 3) if stats["calls"] > 0 else None,
        }
        for bucket, stats in sorted(bucket_stats.items())
    ]

    # Keep only most recent misses (limit 10)
    result["recent_misses"] = sorted(misses, key=lambda m: m["date"], reverse=True)[:10]

    # ── Cointegration accuracy ──────────────────────────────────────────────
    try:
        coint_rows = con.execute("""
            SELECT c.symbol_a, c.symbol_b, c.computed_at,
                   c.spread_zscore, c.half_life_days,
                   pa_cur.adj_close AS price_a_cur,
                   pb_cur.adj_close AS price_b_cur,
                   pa_fwd.adj_close AS price_a_fwd,
                   pb_fwd.adj_close AS price_b_fwd,
                   c.beta
            FROM cointegrated_pairs c
            INNER JOIN prices_daily pa_cur
                ON pa_cur.symbol = c.symbol_a AND pa_cur.date = c.computed_at
            INNER JOIN prices_daily pb_cur
                ON pb_cur.symbol = c.symbol_b AND pb_cur.date = c.computed_at
            INNER JOIN (
                SELECT symbol, date, adj_close,
                       LAG(date, 5) OVER (PARTITION BY symbol ORDER BY date) AS date_5ago
                FROM prices_daily
            ) pa_fwd
                ON pa_fwd.symbol = c.symbol_a AND pa_fwd.date_5ago = c.computed_at
            INNER JOIN (
                SELECT symbol, date, adj_close,
                       LAG(date, 5) OVER (PARTITION BY symbol ORDER BY date) AS date_5ago
                FROM prices_daily
            ) pb_fwd
                ON pb_fwd.symbol = c.symbol_b AND pb_fwd.date_5ago = c.computed_at
            WHERE c.computed_at >= ?
              AND c.computed_at < ?
              AND c.fdr_significant = TRUE
              AND ABS(c.spread_zscore) > 1.5
        """, [cutoff_str, as_of_str]).fetchall()
    except Exception as e:
        log.warning("scorecard_coint_query_failed", error=str(e))
        coint_rows = []

    coint_calls = 0
    coint_correct = 0
    for row in coint_rows:
        sym_a, sym_b, comp_date, z_score, half_life, pa_c, pb_c, pa_f, pb_f, beta = row
        if any(v is None or v <= 0 for v in [pa_c, pb_c, pa_f, pb_f]):
            continue
        if beta is None:
            continue

        # Spread = log(A) - beta * log(B)
        import math
        spread_cur = math.log(pa_c) - beta * math.log(pb_c)
        spread_fwd = math.log(pa_f) - beta * math.log(pb_f)

        # z_score > 0 means spread above mean -> predict it decreases
        predicted_decrease = z_score > 0
        actual_decrease = spread_fwd < spread_cur

        coint_calls += 1
        if predicted_decrease == actual_decrease:
            coint_correct += 1

    result["cointegration_accuracy"] = {
        "calls": coint_calls,
        "correct": coint_correct,
        "accuracy": round(coint_correct / coint_calls, 3) if coint_calls > 0 else None,
    }

    log.info(
        "scorecard_computed",
        momentum_calls=total_calls,
        momentum_accuracy=result["momentum_accuracy"]["accuracy"],
        coint_calls=coint_calls,
    )

    return result
