"""Event-type data loading: momentum, earnings risk, earnings events, filings, index changes, news."""
from __future__ import annotations

import json
from datetime import date, timedelta

import duckdb
import structlog

log = structlog.get_logger()


def load_momentum_analysis(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """Load momentum risk analysis results as {symbol: row_dict}."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    mom = con.execute("""
        SELECT symbol, trend_prob, p_upside, p_downside,
               daily_risk_usd, expected_move_pct,
               z_score, p_value_bonf, strength_bucket, regime, details
        FROM analysis_daily
        WHERE date = ? AND module_name = 'momentum_risk'
    """, [as_of_str]).fetchdf()
    return {r["symbol"]: r for _, r in mom.iterrows()} if not mom.empty else {}


def load_mean_reversion(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """Load mean-reversion analysis results as {symbol: row_dict}."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        mr = con.execute("""
            SELECT symbol, z_score, strength_bucket, regime, details
            FROM analysis_daily
            WHERE date = ? AND module_name = 'mean_reversion'
        """, [as_of_str]).fetchdf()
        return {r["symbol"]: r for _, r in mr.iterrows()} if not mr.empty else {}
    except Exception:
        return {}


def load_breakout(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """Load breakout analysis results as {symbol: row_dict}."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        bo = con.execute("""
            SELECT symbol, strength_bucket, regime, details
            FROM analysis_daily
            WHERE date = ? AND module_name = 'breakout'
        """, [as_of_str]).fetchdf()
        return {r["symbol"]: r for _, r in bo.iterrows()} if not bo.empty else {}
    except Exception:
        return {}


def load_lab_factor(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """Load Factor Lab composite as {symbol: row_dict}."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        df = con.execute("""
            SELECT symbol, trend_prob, details
            FROM analysis_daily
            WHERE date = ? AND module_name = 'lab_factor'
        """, [as_of_str]).fetchdf()
        if df.empty:
            return {}
        result = {}
        for _, r in df.iterrows():
            details = json.loads(r["details"]) if r["details"] else {}
            result[r["symbol"]] = {
                "lab_composite": r["trend_prob"],  # stored in trend_prob column
                "details": details,
            }
        log.info("lab_factor_loaded", symbols=len(result))
        return result
    except Exception:
        return {}


def load_earnings_risk(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """Load earnings risk analysis results as {symbol: row_dict}."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    earn_risk = con.execute("""
        SELECT symbol, p_upside, p_downside, expected_move_pct, strength_bucket, details
        FROM analysis_daily
        WHERE date = ? AND module_name = 'earnings_risk'
    """, [as_of_str]).fetchdf()
    return {r["symbol"]: r for _, r in earn_risk.iterrows()} if not earn_risk.empty else {}


def load_earnings_events(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, list]:
    """Load earnings events within +-7 day window as {symbol: [event_dicts]}."""
    earn_window_start = (as_of - timedelta(days=3)).strftime("%Y-%m-%d")
    earn_window_end = (as_of + timedelta(days=7)).strftime("%Y-%m-%d")
    earnings = con.execute("""
        SELECT symbol, report_date, estimate_eps, actual_eps, surprise_pct
        FROM earnings_calendar
        WHERE report_date BETWEEN ? AND ?
    """, [earn_window_start, earn_window_end]).fetchdf()
    earn_map: dict[str, list] = {}
    if not earnings.empty:
        for _, r in earnings.iterrows():
            earn_map.setdefault(r["symbol"], []).append(r.to_dict())
    return earn_map


def load_filings(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, list]:
    """Load recent SEC 8-K filings as {symbol: [filing_dicts]}."""
    try:
        filings = con.execute("""
            SELECT symbol, form_type, filed_date, items, description, filing_url
            FROM sec_filings
            WHERE filed_date >= ?
            ORDER BY filed_date DESC
        """, [(as_of - timedelta(days=7)).strftime("%Y-%m-%d")]).fetchdf()
        filing_map: dict[str, list] = {}
        if not filings.empty:
            for _, r in filings.iterrows():
                filing_map.setdefault(r["symbol"], []).append({
                    "form": r["form_type"],
                    "filed": str(r["filed_date"]),
                    "items": json.loads(r["items"]) if r["items"] else [],
                    "description": r["description"],
                    "url": r["filing_url"],
                })
        return filing_map
    except Exception:
        return {}


def load_index_changes(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, list[dict]]:
    """Load index constituent changes (last 60 days) as {symbol: [change_dicts]}."""
    try:
        ic = con.execute("""
            SELECT index_symbol, symbol, change_type, change_date
            FROM index_changes
            WHERE change_date >= ?
            ORDER BY change_date DESC
        """, [(as_of - timedelta(days=60)).strftime("%Y-%m-%d")]).fetchdf()
        index_change_map: dict[str, list[dict]] = {}
        if not ic.empty:
            for _, r in ic.iterrows():
                index_change_map.setdefault(r["symbol"], []).append({
                    "index": r["index_symbol"],
                    "type": r["change_type"],   # 'add' | 'remove'
                    "date": str(r["change_date"])[:10],
                })
        return index_change_map
    except Exception:
        return {}


def load_news(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, list]:
    """Load news per symbol (last 3 days) as {symbol: [news_dicts]}."""
    try:
        news = con.execute("""
            SELECT symbol, headline, summary, source, published_at
            FROM news_items
            WHERE published_at >= ?
            ORDER BY published_at DESC
        """, [(as_of - timedelta(days=3)).strftime("%Y-%m-%d 00:00:00")]).fetchdf()
        news_map: dict[str, list] = {}
        if not news.empty:
            for _, r in news.iterrows():
                news_map.setdefault(r["symbol"], []).append({
                    "headline": r["headline"],
                    "summary": r["summary"][:200] if r["summary"] else "",
                    "source": r["source"],
                    "published_at": str(r["published_at"]),
                })
        return news_map
    except Exception:
        return {}
