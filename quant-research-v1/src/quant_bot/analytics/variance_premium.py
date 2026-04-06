"""
Variance Risk Premium: IV**2 - RV**2

VRP is typically POSITIVE -- investors pay a premium for vol protection.
Elevated VRP = market fears more than realized.  Negative VRP = unusual.

For first 6 months: raw VRP level only (no z-score standardization).
After 6 months of accumulated data: z-score against rolling 6-month mean.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import duckdb
import numpy as np
import structlog

log = structlog.get_logger()


def compute_vrp(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    rv_window: int = 20,
) -> dict[str, dict]:
    """
    For each symbol:
    1. Get ATM IV from options_analysis (nearest expiry, most recent as_of)
       - atm_iv is stored as decimal (0.35 = 35%)
    2. Compute 20D realized vol from prices_daily:
       - log_returns = ln(P_t / P_{t-1})
       - rv = std(log_returns[-rv_window:]) * sqrt(252)
    3. VRP = IV**2 - RV**2

    Returns dict[symbol -> {vrp, iv_ann, rv_ann}]
    """
    as_of_str = as_of.strftime("%Y-%m-%d")

    # Load ATM IV per symbol (nearest expiry for today)
    try:
        iv_df = con.execute("""
            SELECT oa.symbol, oa.atm_iv
            FROM options_analysis oa
            INNER JOIN (
                SELECT symbol, MIN(days_to_exp) AS min_exp
                FROM options_analysis
                WHERE as_of = ?
                GROUP BY symbol
            ) nearest ON oa.symbol = nearest.symbol
                      AND oa.days_to_exp = nearest.min_exp
                      AND oa.as_of = ?
        """, [as_of_str, as_of_str]).fetchdf()
    except Exception:
        iv_df = None

    if iv_df is None or iv_df.empty:
        log.info("vrp_no_iv_data", as_of=as_of_str)
        return {}

    iv_map: dict[str, float] = {}
    for _, row in iv_df.iterrows():
        iv_val = row["atm_iv"]
        if iv_val is not None and math.isfinite(float(iv_val)) and float(iv_val) > 0:
            iv_map[row["symbol"]] = float(iv_val)

    if not iv_map:
        return {}

    # Load recent prices for RV computation
    # Need rv_window + 1 trading days of prices
    cutoff = (as_of - timedelta(days=rv_window * 2 + 10)).strftime("%Y-%m-%d")
    syms_with_iv = list(iv_map.keys())

    try:
        price_df = con.execute(f"""
            SELECT symbol, date, adj_close
            FROM prices_daily
            WHERE symbol IN ({','.join('?' * len(syms_with_iv))})
              AND date BETWEEN ? AND ?
            ORDER BY symbol, date
        """, syms_with_iv + [cutoff, as_of_str]).fetchdf()
    except Exception:
        log.warning("vrp_price_query_failed")
        return {}

    if price_df.empty:
        return {}

    results: dict[str, dict] = {}

    for sym in syms_with_iv:
        sdf = price_df[price_df["symbol"] == sym].sort_values("date")
        if len(sdf) < rv_window + 1:
            continue

        prices = sdf["adj_close"].to_numpy().astype(float)
        prices = prices[~np.isnan(prices)]
        if len(prices) < rv_window + 1:
            continue

        # Use the last rv_window+1 prices to get rv_window returns
        recent = prices[-(rv_window + 1):]
        log_ret = np.diff(np.log(np.maximum(recent, 1e-9)))

        rv_daily = float(np.std(log_ret, ddof=1))
        rv_ann = rv_daily * math.sqrt(252)

        iv_ann = iv_map[sym]  # already annualized decimal

        # VRP = IV**2 - RV**2
        vrp = iv_ann ** 2 - rv_ann ** 2

        if not math.isfinite(vrp):
            continue

        results[sym] = {
            "vrp": round(vrp, 6),
            "iv_ann": round(iv_ann, 4),
            "rv_ann": round(rv_ann, 4),
        }

    log.info("vrp_computed", symbols=len(results))
    return results


def store_vrp(
    con: duckdb.DuckDBPyConnection,
    results: dict[str, dict],
    as_of: date,
) -> int:
    """Store VRP results into options_sentiment table (VRP columns only).

    Rows are INSERT OR REPLACE so that subsequent sentiment_ewma store
    can fill in the remaining columns without conflict.
    """
    if not results:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")
    count = 0
    for sym, data in results.items():
        con.execute("""
            INSERT OR REPLACE INTO options_sentiment
                (symbol, as_of, vrp, iv_ann, rv_ann)
            VALUES (?, ?, ?, ?, ?)
        """, [sym, as_of_str, data["vrp"], data["iv_ann"], data["rv_ann"]])
        count += 1

    con.commit()
    log.info("vrp_stored", rows=count)
    return count
