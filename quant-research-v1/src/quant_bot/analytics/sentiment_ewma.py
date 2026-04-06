"""
EWMA z-score on Put/Call ratio and IV skew.
Replaces OU process -- more robust with limited history, no parametric assumptions.

z = (X_today - EWMA_mean) / EWMA_std

EWMA with span=20 gives ~10-day effective memory.
Works from Day 1 -- first few days have wider confidence intervals.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger()


def compute_sentiment_ewma(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    ewma_span: int = 20,
) -> dict[str, dict]:
    """
    For each symbol:
    1. Load historical put_call_vol_ratio from options_snapshot
    2. Load historical iv_skew from options_analysis
    3. Compute EWMA mean and std on each series (pandas .ewm(span=20))
    4. z = (today - ewma_mean) / ewma_std

    Positive pc_ratio_z -> more puts than usual -> bearish sentiment
    Positive skew_z -> more downside fear than usual

    Returns dict[symbol -> {pc_ratio_z, skew_z, pc_ratio_raw, skew_raw}]
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    # Load enough history for EWMA (~60 trading days is generous)
    lookback_start = (as_of - timedelta(days=120)).strftime("%Y-%m-%d")

    # ── Load P/C ratio history from options_snapshot ──
    # Use nearest-expiry row per (symbol, as_of) date
    try:
        pc_df = con.execute("""
            SELECT o.symbol, o.as_of, o.put_call_vol_ratio
            FROM options_snapshot o
            INNER JOIN (
                SELECT symbol, as_of, MIN(days_to_exp) AS min_exp
                FROM options_snapshot
                WHERE as_of BETWEEN ? AND ?
                GROUP BY symbol, as_of
            ) nearest ON o.symbol = nearest.symbol
                      AND o.as_of = nearest.as_of
                      AND o.days_to_exp = nearest.min_exp
            ORDER BY o.symbol, o.as_of
        """, [lookback_start, as_of_str]).fetchdf()
    except Exception:
        pc_df = pd.DataFrame()

    # ── Load IV skew history from options_analysis ──
    try:
        skew_df = con.execute("""
            SELECT oa.symbol, oa.as_of, oa.iv_skew
            FROM options_analysis oa
            INNER JOIN (
                SELECT symbol, as_of, MIN(days_to_exp) AS min_exp
                FROM options_analysis
                WHERE as_of BETWEEN ? AND ?
                GROUP BY symbol, as_of
            ) nearest ON oa.symbol = nearest.symbol
                      AND oa.as_of = nearest.as_of
                      AND oa.days_to_exp = nearest.min_exp
            ORDER BY oa.symbol, oa.as_of
        """, [lookback_start, as_of_str]).fetchdf()
    except Exception:
        skew_df = pd.DataFrame()

    # ── Compute EWMA z-scores per symbol ──
    results: dict[str, dict] = {}

    # Build per-symbol series
    pc_by_sym: dict[str, pd.DataFrame] = {}
    if not pc_df.empty:
        for sym, grp in pc_df.groupby("symbol"):
            pc_by_sym[str(sym)] = grp[["as_of", "put_call_vol_ratio"]].dropna(
                subset=["put_call_vol_ratio"]
            ).sort_values("as_of")

    skew_by_sym: dict[str, pd.DataFrame] = {}
    if not skew_df.empty:
        for sym, grp in skew_df.groupby("symbol"):
            skew_by_sym[str(sym)] = grp[["as_of", "iv_skew"]].dropna(
                subset=["iv_skew"]
            ).sort_values("as_of")

    all_syms = set(pc_by_sym.keys()) | set(skew_by_sym.keys())

    for sym in all_syms:
        if sym not in symbols:
            continue

        entry: dict = {
            "pc_ratio_z": None,
            "skew_z": None,
            "pc_ratio_raw": None,
            "skew_raw": None,
        }

        # P/C ratio z-score
        pc_series = pc_by_sym.get(sym)
        if pc_series is not None and len(pc_series) >= 3:
            vals = pc_series["put_call_vol_ratio"].astype(float)
            # Compute EWMA on data up to yesterday (exclude today) to avoid look-ahead bias
            hist_vals = vals.iloc[:-1]
            ewm_mean = hist_vals.ewm(span=ewma_span, min_periods=3).mean()
            ewm_std = hist_vals.ewm(span=ewma_span, min_periods=3).std()

            today_val = float(vals.iloc[-1])
            yesterday_mean = float(ewm_mean.iloc[-1]) if len(ewm_mean) > 0 else float("nan")
            yesterday_std = float(ewm_std.iloc[-1]) if len(ewm_std) > 0 else float("nan")

            entry["pc_ratio_raw"] = round(today_val, 4)

            if (
                math.isfinite(yesterday_mean)
                and math.isfinite(yesterday_std)
                and yesterday_std > 1e-9
            ):
                z = (today_val - yesterday_mean) / yesterday_std
                if math.isfinite(z):
                    entry["pc_ratio_z"] = round(z, 4)

        # IV skew z-score
        skew_series = skew_by_sym.get(sym)
        if skew_series is not None and len(skew_series) >= 3:
            vals = skew_series["iv_skew"].astype(float)
            # Compute EWMA on data up to yesterday (exclude today) to avoid look-ahead bias
            hist_vals = vals.iloc[:-1]
            ewm_mean = hist_vals.ewm(span=ewma_span, min_periods=3).mean()
            ewm_std = hist_vals.ewm(span=ewma_span, min_periods=3).std()

            today_val = float(vals.iloc[-1])
            yesterday_mean = float(ewm_mean.iloc[-1]) if len(ewm_mean) > 0 else float("nan")
            yesterday_std = float(ewm_std.iloc[-1]) if len(ewm_std) > 0 else float("nan")

            entry["skew_raw"] = round(today_val, 4)

            if (
                math.isfinite(yesterday_mean)
                and math.isfinite(yesterday_std)
                and yesterday_std > 1e-9
            ):
                z = (today_val - yesterday_mean) / yesterday_std
                if math.isfinite(z):
                    entry["skew_z"] = round(z, 4)

        # Only include if we got at least one z-score
        if entry["pc_ratio_z"] is not None or entry["skew_z"] is not None:
            results[sym] = entry

    log.info("sentiment_ewma_computed", symbols=len(results))
    return results


def store_sentiment(
    con: duckdb.DuckDBPyConnection,
    results: dict[str, dict],
    as_of: date,
) -> int:
    """Store EWMA sentiment z-scores into options_sentiment table.

    Uses UPDATE to fill in z-score columns for rows already created by
    store_vrp.  If a row doesn't exist yet (symbol had no IV data for VRP),
    INSERT a new row with z-scores only.
    """
    if not results:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")
    count = 0
    for sym, data in results.items():
        # Try UPDATE first (row may exist from store_vrp)
        con.execute("""
            UPDATE options_sentiment
            SET pc_ratio_z = ?,
                skew_z = ?,
                pc_ratio_raw = ?,
                skew_raw = ?
            WHERE symbol = ? AND as_of = ?
        """, [
            data.get("pc_ratio_z"),
            data.get("skew_z"),
            data.get("pc_ratio_raw"),
            data.get("skew_raw"),
            sym,
            as_of_str,
        ])

        # Check if a row was updated; if not, insert one
        row_count = con.execute("""
            SELECT COUNT(*) FROM options_sentiment
            WHERE symbol = ? AND as_of = ?
        """, [sym, as_of_str]).fetchone()[0]

        if row_count == 0:
            con.execute("""
                INSERT INTO options_sentiment
                    (symbol, as_of, pc_ratio_z, skew_z, pc_ratio_raw, skew_raw)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                sym, as_of_str,
                data.get("pc_ratio_z"),
                data.get("skew_z"),
                data.get("pc_ratio_raw"),
                data.get("skew_raw"),
            ])
        count += 1

    con.commit()
    log.info("sentiment_ewma_stored", rows=count)
    return count
