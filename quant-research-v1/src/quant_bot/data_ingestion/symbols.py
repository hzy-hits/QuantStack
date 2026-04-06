"""Fetch all US stock symbols from Finnhub — single API call, weekly refresh."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import requests
import structlog

log = structlog.get_logger()


def _symbols_fresh(con: duckdb.DuckDBPyConnection, refresh_days: int) -> bool:
    """Return True if us_symbols was refreshed within refresh_days."""
    try:
        row = con.execute(
            "SELECT MAX(fetched_at) FROM us_symbols"
        ).fetchone()
    except duckdb.CatalogException:
        return False
    if row is None or row[0] is None:
        return False
    last_ts = row[0]
    if isinstance(last_ts, str):
        last_ts = datetime.fromisoformat(last_ts)
    if hasattr(last_ts, "date"):
        last_date = last_ts.date()
    else:
        last_date = last_ts
    return (date.today() - last_date).days < refresh_days


def fetch_us_symbols(
    con: duckdb.DuckDBPyConnection,
    api_key: str,
    refresh_days: int = 7,
) -> list[dict]:
    """
    Fetch all US stock symbols from Finnhub GET /stock/symbol?exchange=US.

    Stores in us_symbols table. Returns list of dicts with symbol info.
    Skips fetch if data is less than refresh_days old.
    """
    if _symbols_fresh(con, refresh_days):
        rows = con.execute(
            "SELECT symbol, name, type, exchange, mic FROM us_symbols"
        ).fetchall()
        log.info("us_symbols_from_cache", count=len(rows))
        return [
            {"symbol": r[0], "name": r[1], "type": r[2],
             "exchange": r[3], "mic": r[4]}
            for r in rows
        ]

    log.info("us_symbols_fetching_from_finnhub")
    resp = requests.get(
        "https://finnhub.io/api/v1/stock/symbol",
        params={"exchange": "US", "token": api_key},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, list) or len(data) == 0:
        log.warning("us_symbols_empty_response")
        return []

    now = datetime.utcnow().isoformat()
    rows_to_insert = []
    for item in data:
        sym = item.get("symbol", "").strip()
        if not sym:
            continue
        rows_to_insert.append((
            sym,
            item.get("description", ""),
            item.get("type", ""),
            item.get("exchange", "US"),
            item.get("mic", ""),
            now,
        ))

    con.begin()
    try:
        con.execute("DELETE FROM us_symbols")
        con.executemany(
            "INSERT INTO us_symbols (symbol, name, type, exchange, mic, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows_to_insert,
        )
        con.commit()
    except Exception:
        con.rollback()
        raise

    log.info("us_symbols_fetched", count=len(rows_to_insert))
    return [
        {"symbol": r[0], "name": r[1], "type": r[2],
         "exchange": r[3], "mic": r[4]}
        for r in rows_to_insert
    ]
