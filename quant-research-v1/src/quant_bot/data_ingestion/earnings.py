"""Fetch earnings calendar and EPS surprise data from Finnhub."""
from __future__ import annotations

import time
from datetime import date, timedelta

import duckdb
import httpx
import polars as pl
import structlog

log = structlog.get_logger()

FINNHUB_BASE = "https://finnhub.io/api/v1"
RATE_LIMIT_DELAY = 1.0 / 55  # stay under 60 req/s


def _get(client: httpx.Client, endpoint: str, params: dict) -> dict | list:
    resp = client.get(f"{FINNHUB_BASE}{endpoint}", params=params, timeout=10)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT_DELAY)
    return resp.json()


def fetch_earnings_history(
    client: httpx.Client,
    symbol: str,
    api_key: str,
) -> list[dict]:
    """Fetch historical EPS actuals vs estimates for one symbol."""
    data = _get(client, "/stock/earnings", {"symbol": symbol, "token": api_key, "limit": 20})
    if not isinstance(data, list):
        return []

    records = []
    for item in data:
        try:
            records.append({
                "symbol": symbol,
                "report_date": date.fromisoformat(item["period"]),
                "fiscal_period": item.get("period", ""),
                "estimate_eps": float(item.get("estimate") or 0),
                "actual_eps": float(item.get("actual") or 0),
                "surprise_pct": float(item.get("surprisePercent") or 0),
            })
        except (KeyError, ValueError, TypeError):
            continue
    return records


def fetch_earnings_calendar(
    client: httpx.Client,
    api_key: str,
    days_ahead: int = 14,
) -> list[dict]:
    """Fetch upcoming earnings events."""
    today = date.today()
    end = today + timedelta(days=days_ahead)
    data = _get(
        client,
        "/calendar/earnings",
        {"from": str(today), "to": str(end), "token": api_key},
    )
    if not isinstance(data, dict):
        return []

    records = []
    for item in data.get("earningsCalendar", []):
        try:
            records.append({
                "symbol": item["symbol"],
                "report_date": date.fromisoformat(item["date"]),
                "fiscal_period": item.get("quarter", ""),
                "estimate_eps": float(item.get("epsEstimate") or 0),
                "actual_eps": None,
                "surprise_pct": None,
            })
        except (KeyError, ValueError, TypeError):
            continue
    return records


def upsert_earnings(con: duckdb.DuckDBPyConnection, records: list[dict]) -> int:
    if not records:
        return 0

    rows = []
    for r in records:
        rows.append({
            "symbol": r["symbol"],
            "report_date": r["report_date"],
            "fiscal_period": r.get("fiscal_period") or "",
            "estimate_eps": r.get("estimate_eps"),
            "actual_eps": r.get("actual_eps"),
            "surprise_pct": r.get("surprise_pct"),
        })

    df = pl.DataFrame(rows).with_columns(pl.col("report_date").cast(pl.Date))
    con.register("earn_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO earnings_calendar
            (symbol, report_date, fiscal_period, estimate_eps, actual_eps, surprise_pct)
        SELECT symbol, report_date, fiscal_period, estimate_eps, actual_eps, surprise_pct
        FROM earn_updates
    """)
    con.unregister("earn_updates")
    con.commit()
    return len(rows)


def fetch_and_store_earnings(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    api_key: str,
    init: bool = False,
) -> int:
    total = 0
    with httpx.Client() as client:
        # Historical EPS on init or weekly refresh
        if init:
            for sym in symbols:
                try:
                    records = fetch_earnings_history(client, sym, api_key)
                    total += upsert_earnings(con, records)
                    log.info("earnings_history_fetched", symbol=sym, rows=len(records))
                except Exception as e:
                    log.warning("earnings_history_error", symbol=sym, error=str(e))

        # Always fetch upcoming calendar
        try:
            upcoming = fetch_earnings_calendar(client, api_key)
            total += upsert_earnings(con, upcoming)
            log.info("earnings_calendar_fetched", rows=len(upcoming))
        except Exception as e:
            log.warning("earnings_calendar_error", error=str(e))

    return total
