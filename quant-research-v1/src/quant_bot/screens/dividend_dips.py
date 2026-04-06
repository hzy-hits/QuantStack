"""
Dividend Yield Percentile (DYP) screen — Phase 2 engine.

Step 1: Eligibility filter (history length + recent dividend)
Step 2: DYP gate (current yield percentile vs own N-year history)

All heavy lifting is DuckDB SQL. Python just orchestrates queries
and returns results as a list of dicts.
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import structlog

log = structlog.get_logger()


def run_dyp_screen(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    *,
    dyp_threshold: int = 70,
    min_history_days: int = 730,
    lookback_years: int = 5,
    max_results: int = 30,
) -> list[dict]:
    """
    Run the two-step DYP screen and return qualifying candidates.

    Step 1: Filter to symbols with sufficient dividend history
            (>= min_history_days of price data, dividend in last 365d,
             >= 8 regular dividend payments).
    Step 2: Compute DYP = percentile rank of current TTM yield
            within own lookback_years history. Gate at dyp_threshold.
            Uses raw close (not adj_close) as denominator to avoid
            split-adjustment distortion in historical yield.

    Returns list of dicts sorted by DYP descending, capped at max_results.
    """
    if not symbols:
        return []

    placeholders = ", ".join(["?"] * len(symbols))

    # ── Step 1: Eligibility ──────────────────────────────────────────────────
    eligible_query = f"""
    WITH price_span AS (
        SELECT symbol,
               MIN(date) AS first_date,
               MAX(date) AS last_date,
               DATEDIFF('day', MIN(date), MAX(date)) AS history_days
        FROM prices_daily
        WHERE symbol IN ({placeholders})
        GROUP BY symbol
        HAVING history_days >= ?
    ),
    div_activity AS (
        SELECT symbol,
               MAX(ex_date) AS last_ex_date,
               COUNT(*) AS div_count
        FROM dividends
        WHERE symbol IN ({placeholders})
          AND is_special = FALSE
        GROUP BY symbol
        HAVING last_ex_date >= ? AND div_count >= 8
    )
    SELECT p.symbol, p.history_days, d.last_ex_date, d.div_count
    FROM price_span p
    JOIN div_activity d ON p.symbol = d.symbol
    """

    one_year_ago = as_of - timedelta(days=365)
    params_step1 = list(symbols) + [min_history_days] + list(symbols) + [one_year_ago]

    eligible_rows = con.execute(eligible_query, params_step1).fetchall()
    eligible_syms = [row[0] for row in eligible_rows]
    eligible_meta = {row[0]: {"history_days": row[1], "last_ex_date": row[2], "div_count": row[3]}
                     for row in eligible_rows}

    if not eligible_syms:
        log.info("dyp_screen_no_eligible")
        return []

    log.info("dyp_step1_eligible", count=len(eligible_syms))

    # ── Step 2: DYP computation ──────────────────────────────────────────────
    # Uses raw close price (not adj_close) as denominator.
    # Per-symbol latest date avoids dropping symbols with stale bars.
    ep = ", ".join(["?"] * len(eligible_syms))
    lookback_start = as_of - timedelta(days=lookback_years * 365)

    dyp_query = f"""
    WITH per_symbol_latest AS (
        SELECT symbol, MAX(date) AS latest_date
        FROM prices_daily
        WHERE symbol IN ({ep}) AND date <= ?
        GROUP BY symbol
    ),
    ttm_yields AS (
        SELECT
            p.symbol,
            p.date,
            p.close,
            COALESCE((
                SELECT SUM(d.cash_amount)
                FROM dividends d
                WHERE d.symbol = p.symbol
                  AND d.ex_date > p.date - INTERVAL '365 days'
                  AND d.ex_date <= p.date
                  AND d.is_special = FALSE
            ), 0.0) AS ttm_dividend
        FROM prices_daily p
        WHERE p.symbol IN ({ep})
          AND p.date >= ?
          AND p.date <= (SELECT MAX(latest_date) FROM per_symbol_latest)
    ),
    yields AS (
        SELECT symbol, date, close, ttm_dividend,
               CASE WHEN close > 0 THEN ttm_dividend / close * 100.0 ELSE 0.0 END AS yield_pct
        FROM ttm_yields
        WHERE ttm_dividend > 0
    ),
    ranked AS (
        SELECT
            symbol, date, close, ttm_dividend, yield_pct,
            PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY yield_pct) * 100 AS dyp
        FROM yields
    ),
    current AS (
        SELECT r.*
        FROM ranked r
        JOIN per_symbol_latest psl ON r.symbol = psl.symbol AND r.date = psl.latest_date
    )
    SELECT symbol, dyp, yield_pct, ttm_dividend, close
    FROM current
    WHERE dyp >= ?
    ORDER BY dyp DESC
    LIMIT ?
    """

    # Params: eligible_syms for per_symbol_latest + as_of,
    #         eligible_syms for ttm_yields + lookback_start,
    #         dyp_threshold, max_results
    params_step2 = (
        list(eligible_syms) + [as_of]
        + list(eligible_syms) + [lookback_start]
        + [dyp_threshold, max_results]
    )
    rows = con.execute(dyp_query, params_step2).fetchall()

    results = []
    for row in rows:
        sym = row[0]
        meta = eligible_meta.get(sym, {})
        results.append({
            "symbol": sym,
            "dyp": round(row[1], 1),
            "current_yield_pct": round(row[2], 2),
            "ttm_dividend": round(row[3], 4),
            "current_price": round(row[4], 2),
            "history_days": meta.get("history_days"),
            "last_ex_date": str(meta.get("last_ex_date", "")),
            "div_count": meta.get("div_count"),
        })

    log.info("dyp_step2_passed", count=len(results), threshold=dyp_threshold)
    return results
