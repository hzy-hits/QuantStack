"""Fetch EOD prices: yfinance (adj_close) + Finnhub (OHLCV fallback)."""
from __future__ import annotations

import time
from datetime import date, timedelta

import duckdb
import polars as pl
import yfinance as yf
import structlog

log = structlog.get_logger()


def fetch_prices_yfinance(
    symbols: list[str],
    start: date,
    end: date,
) -> pl.DataFrame:
    """
    Fetch OHLCV + adj_close from yfinance for all symbols at once.
    Returns a Polars DataFrame with columns:
        symbol, date, open, high, low, close, volume, adj_close
    """
    tickers = " ".join(symbols)
    raw = yf.download(
        tickers,
        start=str(start),
        end=str(end),
        auto_adjust=False,
        progress=False,
        group_by="ticker" if len(symbols) > 1 else "column",
    )

    if raw.empty:
        return pl.DataFrame()

    records = []
    for sym in symbols:
        try:
            if len(symbols) == 1:
                df = raw
            else:
                df = raw[sym]

            df = df.dropna(how="all")
            for dt, row in df.iterrows():
                record = {
                    "symbol": sym,
                    "date": dt.date(),
                    "open": float(row.get("Open", 0) or 0),
                    "high": float(row.get("High", 0) or 0),
                    "low": float(row.get("Low", 0) or 0),
                    "close": float(row.get("Close", 0) or 0),
                    "volume": float(row.get("Volume", 0) or 0),
                    "adj_close": float(row.get("Adj Close", 0) or 0),
                }
                # Normalize float NaN to None so DuckDB receives NULL
                for key, value in record.items():
                    if isinstance(value, float) and value != value:
                        record[key] = None
                # Skip rows with no valid close (e.g. pre-market futures with partial data)
                if record["close"] is None:
                    continue
                if record["volume"] is not None:
                    record["volume"] = int(record["volume"])
                records.append(record)
        except Exception as e:
            log.warning("yfinance_symbol_error", symbol=sym, error=str(e))

    symbols_fetched = {r["symbol"] for r in records}
    coverage_pct = len(symbols_fetched) / len(symbols) * 100 if symbols else 0
    if coverage_pct < 50:
        log.warning("yfinance_low_coverage", fetched=len(symbols_fetched),
                     requested=len(symbols), pct=f"{coverage_pct:.0f}%")

    if not records:
        return pl.DataFrame()

    return pl.DataFrame(records).with_columns([
        pl.col("date").cast(pl.Date),
    ])


def upsert_prices(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Upsert price rows into prices_daily. Returns count inserted/updated."""
    if df.is_empty():
        return 0

    con.register("price_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO prices_daily
            (symbol, date, open, high, low, close, volume, adj_close)
        SELECT symbol, date, open, high, low, close, volume, adj_close
        FROM price_updates
    """)
    con.unregister("price_updates")
    return len(df)


def fetch_and_store_prices(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    init: bool = False,
) -> int:
    """
    Main entry point. If init=True, fetch 2 years of history.
    Otherwise fetch last 5 trading days (incremental).
    Returns total rows upserted.
    """
    today = date.today()
    if init:
        start = today - timedelta(days=730)
        log.info("prices_init_fetch", symbols=len(symbols), start=str(start))
    else:
        start = today - timedelta(days=7)
        log.info("prices_incremental_fetch", symbols=len(symbols))

    # Batch into groups of 50 to avoid yfinance timeouts
    total = 0
    batch_size = 50
    con.begin()
    try:
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            df = fetch_prices_yfinance(batch, start, today)
            if not df.is_empty():
                n = upsert_prices(con, df)
                total += n
                log.info("prices_batch_done", batch=i // batch_size + 1, rows=n)
            time.sleep(0.5)  # be polite to yfinance
        con.commit()
    except Exception:
        con.rollback()
        raise

    if total == 0:
        log.warning("prices_no_data", symbols=len(symbols),
                     hint="yfinance returned no data for any symbol")

    return total
