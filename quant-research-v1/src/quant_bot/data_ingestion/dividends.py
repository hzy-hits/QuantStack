"""Fetch dividend history from yfinance and store in DuckDB."""
from __future__ import annotations

import time
from datetime import date, timedelta

import duckdb
import polars as pl
import yfinance as yf
import structlog

log = structlog.get_logger()


def fetch_dividends_yfinance(
    symbols: list[str],
    start: date,
    end: date,
) -> pl.DataFrame:
    """
    Batch-fetch dividend data using yf.download with actions=True.
    Returns Polars DataFrame: symbol, ex_date, cash_amount.
    """
    tickers = " ".join(symbols)
    raw = yf.download(
        tickers,
        start=str(start),
        end=str(end),
        actions=True,
        auto_adjust=False,
        progress=False,
        group_by="ticker" if len(symbols) > 1 else "column",
    )

    if raw.empty:
        return pl.DataFrame()

    records: list[dict] = []
    for sym in symbols:
        try:
            if len(symbols) == 1:
                df = raw
            else:
                df = raw[sym]

            if "Dividends" not in df.columns:
                continue

            divs = df["Dividends"].dropna()
            divs = divs[divs > 0]

            for dt, amount in divs.items():
                records.append({
                    "symbol": sym,
                    "ex_date": dt.date(),
                    "cash_amount": float(amount),
                })
        except Exception as e:
            log.warning("dividend_symbol_error", symbol=sym, error=str(e))

    if not records:
        return pl.DataFrame()

    return pl.DataFrame(records).with_columns([
        pl.col("ex_date").cast(pl.Date),
    ])


def flag_special_dividends_from_db(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    multiplier: float = 3.0,
) -> None:
    """
    Recompute is_special flags for the given symbols using full DB history.
    A dividend is special if it exceeds multiplier × trailing average
    of all prior dividends for the same symbol.
    """
    if not symbols:
        return

    placeholders = ", ".join(["?"] * len(symbols))
    rows = con.execute(f"""
        SELECT symbol, ex_date, cash_amount
        FROM dividends
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, ex_date
    """, symbols).fetchall()

    if not rows:
        return

    # Build per-symbol flag map
    updates: list[tuple] = []
    current_sym = None
    running_sum = 0.0
    running_count = 0

    for sym, ex_date, amount in rows:
        if sym != current_sym:
            current_sym = sym
            running_sum = 0.0
            running_count = 0

        is_special = False
        if running_count >= 2:
            trailing_avg = running_sum / running_count
            is_special = amount > trailing_avg * multiplier

        updates.append((is_special, sym, ex_date))
        running_sum += amount
        running_count += 1

    # Batch update
    con.executemany(
        "UPDATE dividends SET is_special = ? WHERE symbol = ? AND ex_date = ?",
        updates,
    )
    con.commit()
    log.info("special_dividends_flagged", symbols=len(symbols), updated=len(updates))


def upsert_dividends(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Upsert dividend rows into dividends table. Returns count."""
    if df.is_empty():
        return 0

    # Upsert without is_special — that gets recomputed from full history
    insert_df = df.select(["symbol", "ex_date", "cash_amount"])
    con.register("div_updates", insert_df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO dividends
            (symbol, ex_date, cash_amount)
        SELECT symbol, ex_date, cash_amount
        FROM div_updates
    """)
    con.unregister("div_updates")
    return len(df)


def fetch_and_store_dividends(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    init: bool = False,
    lookback_years: int = 5,
    special_dividend_multiplier: float = 3.0,
) -> int:
    """
    Main entry point for dividend ingestion.
    init=True fetches `lookback_years` of history; otherwise last 90 days.
    After upsert, recomputes special dividend flags from full stored history.
    """
    today = date.today()
    if init:
        start = today - timedelta(days=lookback_years * 365)
        log.info("dividends_init_fetch", symbols=len(symbols), start=str(start))
    else:
        start = today - timedelta(days=90)
        log.info("dividends_incremental_fetch", symbols=len(symbols))

    total = 0
    touched_symbols: list[str] = []
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        df = fetch_dividends_yfinance(batch, start, today)
        if not df.is_empty():
            n = upsert_dividends(con, df)
            total += n
            touched_symbols.extend(df["symbol"].unique().to_list())
            log.info("dividends_batch_done", batch=i // batch_size + 1, rows=n)
        time.sleep(0.5)

    con.commit()

    # Recompute special flags from full history for all touched symbols
    if touched_symbols:
        unique_touched = sorted(set(touched_symbols))
        flag_special_dividends_from_db(con, unique_touched, special_dividend_multiplier)

    return total
