"""Fetch company profile + valuation metrics from Finnhub — weekly refresh."""
from __future__ import annotations

import time
from datetime import date, datetime

import duckdb
import requests
import structlog

log = structlog.get_logger()

# Finnhub free tier: 60 API calls / minute (NOT per second).
# 1.05s between calls = ~57 calls/min, safely under the limit.
# ~750 symbols * 2 calls * 1.05s ≈ 26 min.
_RATE_LIMIT_SLEEP = 1.05


def _profile_fresh(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of: date,
    refresh_days: int,
) -> bool:
    """Return True if company_profile for symbol was fetched within refresh_days."""
    try:
        row = con.execute(
            "SELECT MAX(as_of) FROM company_profile WHERE symbol = ?",
            [symbol],
        ).fetchone()
    except duckdb.CatalogException:
        return False
    if row is None or row[0] is None:
        return False
    last_date = row[0]
    if isinstance(last_date, str):
        last_date = date.fromisoformat(last_date)
    return (as_of - last_date).days < refresh_days


def _fetch_profile(symbol: str, api_key: str) -> dict | None:
    """GET /stock/profile2 — returns company metadata or None."""
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={"symbol": symbol, "token": api_key},
            timeout=15,
        )
        if resp.status_code == 429:
            log.warning("finnhub_rate_limited", symbol=symbol)
            time.sleep(5)
            return None
        resp.raise_for_status()
        data = resp.json()
        if not data or not data.get("ticker"):
            return None
        return data
    except Exception as e:
        log.warning("finnhub_profile_error", symbol=symbol, error=str(e))
        return None


def _fetch_metrics(symbol: str, api_key: str) -> dict | None:
    """GET /stock/metric?metric=all — returns valuation metrics or None."""
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/stock/metric",
            params={"symbol": symbol, "metric": "all", "token": api_key},
            timeout=15,
        )
        if resp.status_code == 429:
            log.warning("finnhub_rate_limited", symbol=symbol)
            time.sleep(5)
            return None
        resp.raise_for_status()
        data = resp.json()
        return data.get("metric", {})
    except Exception as e:
        log.warning("finnhub_metric_error", symbol=symbol, error=str(e))
        return None


def _safe_float(val) -> float | None:
    """Convert to float, returning None for missing/invalid values."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (ValueError, TypeError):
        return None


def fetch_fundamentals(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    api_key: str,
    refresh_days: int = 7,
) -> int:
    """
    Fetch company profile + valuation metrics for symbols from Finnhub.

    Two API calls per symbol (profile2 + metric), rate-limited to 57/min (Finnhub free tier: 60/min).
    Stores in company_profile table. Skips symbols with fresh data.

    Returns count of symbols successfully fetched.
    """
    if not api_key:
        log.warning("fundamentals_skip_no_api_key")
        return 0

    # Filter to symbols that need refresh
    stale = [s for s in symbols if not _profile_fresh(con, s, as_of, refresh_days)]
    if not stale:
        log.info("fundamentals_all_fresh", total=len(symbols))
        return 0

    log.info("fundamentals_fetching", stale=len(stale), total=len(symbols),
             est_minutes=round(len(stale) * 2 * _RATE_LIMIT_SLEEP / 60, 1))

    fetched = 0
    batch_rows = []

    for i, sym in enumerate(stale):
        # Fetch profile
        profile = _fetch_profile(sym, api_key)
        time.sleep(_RATE_LIMIT_SLEEP)

        # Fetch metrics
        metrics = _fetch_metrics(sym, api_key)
        time.sleep(_RATE_LIMIT_SLEEP)

        if profile is None and metrics is None:
            continue

        row = (
            sym,
            as_of,
            profile.get("name") if profile else None,
            profile.get("finnhubIndustry") if profile else None,  # sector-level
            profile.get("finnhubIndustry") if profile else None,  # industry (same field)
            _safe_float(profile.get("marketCapitalization")) if profile else None,
            # Metrics
            _safe_float(metrics.get("peTTM")) if metrics else None,
            _safe_float(metrics.get("peNormalizedAnnual")) if metrics else None,
            _safe_float(metrics.get("psTTM")) if metrics else None,
            _safe_float(metrics.get("pbQuarterly")) if metrics else None,
            _safe_float(metrics.get("currentEv/freeCashFlowTTM")) if metrics else None,
            _safe_float(metrics.get("roeTTM")) if metrics else None,
            _safe_float(metrics.get("freeCashFlowPerShareTTM")) if metrics else None,
            _safe_float(metrics.get("revenueGrowthQuarterlyYoy")) if metrics else None,
            _safe_float(metrics.get("targetMedianPrice")) if metrics else None,
            # analyst_count: use number of recommendations if available
            int(metrics.get("analystRecommendationCount", 0) or 0) if metrics else None,
            _safe_float(metrics.get("recommendationMean")) if metrics else None,
        )
        batch_rows.append(row)
        fetched += 1

        # Batch insert every 50 symbols to avoid losing progress
        if len(batch_rows) >= 50:
            _upsert_batch(con, batch_rows)
            batch_rows = []

        if (i + 1) % 25 == 0:
            log.info("fundamentals_progress", done=i + 1, total=len(stale),
                     fetched=fetched)

    # Final batch
    if batch_rows:
        _upsert_batch(con, batch_rows)

    log.info("fundamentals_done", fetched=fetched, stale=len(stale))
    return fetched


def _upsert_batch(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """Insert or replace a batch of company_profile rows."""
    con.begin()
    try:
        con.executemany(
            """INSERT OR REPLACE INTO company_profile
            (symbol, as_of, company_name, sector, industry, market_cap,
             pe_ttm, pe_fwd, ps_ratio, pb_ratio, ev_ebitda,
             roe, fcf_yield, revenue_growth,
             analyst_target, analyst_count, recommendation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
