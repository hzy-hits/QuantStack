"""Options data loading: current snapshot, analysis, 7-day history, and proxy mapping."""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import structlog

from quant_bot.data_ingestion.options import OPTIONS_PROXY_MAP

log = structlog.get_logger()


def load_options_current(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> tuple[dict, dict]:
    """
    Load current options snapshot and analysis.
    Returns (opts_map, opts_analysis_map) keyed by symbol.
    """
    as_of_str = as_of.strftime("%Y-%m-%d")

    # Snapshot
    try:
        opts = con.execute("""
            SELECT o.symbol, o.atm_iv, o.expected_move_pct, o.put_call_vol_ratio
            FROM options_snapshot o
            INNER JOIN (
                SELECT symbol, MIN(days_to_exp) AS min_exp
                FROM options_snapshot
                WHERE as_of = ?
                GROUP BY symbol
            ) nearest ON o.symbol = nearest.symbol
                      AND o.days_to_exp = nearest.min_exp
                      AND o.as_of = ?
        """, [as_of_str, as_of_str]).fetchdf()
        opts_map = {r["symbol"]: r.to_dict() for _, r in opts.iterrows()} if not opts.empty else {}
    except Exception:
        opts_map = {}

    # Enhanced analysis (probability cone, skew, bias, unusual activity)
    try:
        opts_analysis = con.execute("""
            SELECT oa.*
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
        opts_analysis_map = {r["symbol"]: r.to_dict() for _, r in opts_analysis.iterrows()} if not opts_analysis.empty else {}
    except Exception:
        opts_analysis_map = {}

    return opts_map, opts_analysis_map


def load_options_history(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    hist_target_str: str,
    hist_lo_str: str,
    hist_hi_str: str,
) -> tuple[dict, dict]:
    """
    Load 7-day historical options snapshot and analysis for IV delta scoring.
    Returns (opts_hist_map, opts_analysis_hist_map) keyed by symbol.
    """
    # Historical snapshot
    try:
        opts_hist = con.execute("""
            WITH hist_dates AS (
                SELECT symbol, as_of AS hist_as_of,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol
                           ORDER BY ABS(DATE_DIFF('day', as_of, CAST(? AS DATE))) ASC,
                                    as_of DESC
                       ) AS rn
                FROM options_snapshot
                WHERE as_of BETWEEN ? AND ?
            ),
            hist_snapshot AS (
                SELECT h.symbol, d.hist_as_of, h.atm_iv, h.expected_move_pct,
                       h.put_call_vol_ratio,
                       ROW_NUMBER() OVER (
                           PARTITION BY h.symbol
                           ORDER BY h.days_to_exp ASC
                       ) AS rn
                FROM options_snapshot h
                JOIN (SELECT * FROM hist_dates WHERE rn = 1) d
                  ON h.symbol = d.symbol AND h.as_of = d.hist_as_of
            )
            SELECT symbol, hist_as_of, atm_iv, expected_move_pct, put_call_vol_ratio
            FROM hist_snapshot WHERE rn = 1
        """, [hist_target_str, hist_lo_str, hist_hi_str]).fetchdf()
        opts_hist_map = {r["symbol"]: r.to_dict() for _, r in opts_hist.iterrows()} if not opts_hist.empty else {}
    except Exception:
        opts_hist_map = {}

    # Historical analysis
    try:
        opts_analysis_hist = con.execute("""
            WITH hist_dates AS (
                SELECT symbol, as_of AS hist_as_of,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol
                           ORDER BY ABS(DATE_DIFF('day', as_of, CAST(? AS DATE))) ASC,
                                    as_of DESC
                       ) AS rn
                FROM options_analysis
                WHERE as_of BETWEEN ? AND ?
            ),
            hist_analysis AS (
                SELECT h.symbol, d.hist_as_of, h.atm_iv, h.iv_skew,
                       h.put_call_vol_ratio, h.unusual_strikes,
                       ROW_NUMBER() OVER (
                           PARTITION BY h.symbol
                           ORDER BY h.days_to_exp ASC
                       ) AS rn
                FROM options_analysis h
                JOIN (SELECT * FROM hist_dates WHERE rn = 1) d
                  ON h.symbol = d.symbol AND h.as_of = d.hist_as_of
            )
            SELECT symbol, hist_as_of, atm_iv, iv_skew, put_call_vol_ratio, unusual_strikes
            FROM hist_analysis WHERE rn = 1
        """, [hist_target_str, hist_lo_str, hist_hi_str]).fetchdf()
        opts_analysis_hist_map = {r["symbol"]: r.to_dict() for _, r in opts_analysis_hist.iterrows()} if not opts_analysis_hist.empty else {}
    except Exception:
        opts_analysis_hist_map = {}

    return opts_hist_map, opts_analysis_hist_map


def apply_proxy_mapping(
    opts_map: dict,
    opts_analysis_map: dict,
    opts_hist_map: dict,
    opts_analysis_hist_map: dict,
) -> None:
    """
    Apply OPTIONS_PROXY_MAP: map ineligible symbols to their ETF proxy.
    Mutates all four maps in place.
    """
    for ineligible_sym, proxy_sym in OPTIONS_PROXY_MAP.items():
        if ineligible_sym not in opts_map and proxy_sym in opts_map:
            proxy_data = dict(opts_map[proxy_sym])
            proxy_data["_proxy_source"] = proxy_sym
            opts_map[ineligible_sym] = proxy_data
        if ineligible_sym not in opts_analysis_map and proxy_sym in opts_analysis_map:
            proxy_data = dict(opts_analysis_map[proxy_sym])
            proxy_data["_proxy_source"] = proxy_sym
            opts_analysis_map[ineligible_sym] = proxy_data
        # Also proxy historical data
        if ineligible_sym not in opts_hist_map and proxy_sym in opts_hist_map:
            proxy_data = dict(opts_hist_map[proxy_sym])
            proxy_data["_proxy_source"] = proxy_sym
            opts_hist_map[ineligible_sym] = proxy_data
        if ineligible_sym not in opts_analysis_hist_map and proxy_sym in opts_analysis_hist_map:
            proxy_data = dict(opts_analysis_hist_map[proxy_sym])
            proxy_data["_proxy_source"] = proxy_sym
            opts_analysis_hist_map[ineligible_sym] = proxy_data
