"""Fetch macro data from FRED."""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import polars as pl
import structlog
from fredapi import Fred

log = structlog.get_logger()


def fetch_and_store_macro(
    con: duckdb.DuckDBPyConnection,
    fred_key: str,
    series_ids: list[str],
    init: bool = False,
) -> int:
    fred = Fred(api_key=fred_key)
    today = date.today()
    start = (today - timedelta(days=730)) if init else (today - timedelta(days=10))

    records = []
    for sid in series_ids:
        try:
            s = fred.get_series(sid, observation_start=str(start), observation_end=str(today))
            for dt, val in s.items():
                if val is not None and not (val != val):  # skip NaN
                    records.append({
                        "date": dt.date(),
                        "series_id": sid,
                        "value": float(val),
                    })
            log.info("fred_fetched", series=sid, rows=len(s.dropna()))
        except Exception as e:
            log.warning("fred_error", series=sid, error=str(e))

    if not records:
        return 0

    df = pl.DataFrame(records).with_columns(pl.col("date").cast(pl.Date))
    con.register("macro_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO macro_daily (date, series_id, value)
        SELECT date, series_id, value FROM macro_updates
    """)
    con.unregister("macro_updates")
    con.commit()
    return len(records)
