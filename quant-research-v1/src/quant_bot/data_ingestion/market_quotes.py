"""Fetch free session quote snapshots for US pre/post-market execution gates."""
from __future__ import annotations

import json
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Any

import duckdb
import polars as pl
import structlog
import yfinance as yf

log = structlog.get_logger()

DEFAULT_MAX_WORKERS = 8
DEFAULT_MAX_SYMBOLS = 180


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) and out > 0 else None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _mapping_value(obj: Any, *keys: str) -> Any:
    for key in keys:
        try:
            value = obj.get(key)
        except AttributeError:
            value = getattr(obj, key, None)
        except Exception:
            value = None
        if value is not None:
            return value
    return None


def _fast_info_dict(ticker: Any) -> dict[str, Any]:
    try:
        fast = ticker.fast_info
        if isinstance(fast, dict):
            return dict(fast)
        return {key: _mapping_value(fast, key) for key in getattr(fast, "keys", lambda: [])()}
    except Exception:
        return {}


def _history_last_price(ticker: Any, as_of: date) -> tuple[float | None, datetime | None]:
    try:
        hist = ticker.history(period="1d", interval="1m", prepost=True, auto_adjust=False)
    except Exception:
        return None, None
    if hist is None or getattr(hist, "empty", True):
        return None, None
    try:
        close = _safe_float(hist["Close"].dropna().iloc[-1])
        idx = hist["Close"].dropna().index[-1]
    except Exception:
        return None, None
    quote_time = None
    try:
        if hasattr(idx, "to_pydatetime"):
            quote_time = idx.to_pydatetime()
    except Exception:
        quote_time = None
    try:
        if quote_time is not None and quote_time.date() != as_of:
            return None, None
    except Exception:
        return None, None
    return close, quote_time


def _active_price(
    *,
    session: str,
    regular_market_price: float | None,
    premarket_price: float | None,
    postmarket_price: float | None,
    last_price: float | None,
) -> tuple[float | None, str | None]:
    if session == "pre":
        if premarket_price is not None:
            return premarket_price, "premarket_price"
        if last_price is not None:
            return last_price, "last_price"
    if session == "post":
        if postmarket_price is not None:
            return postmarket_price, "postmarket_price"
        if last_price is not None:
            return last_price, "last_price"
    if regular_market_price is not None:
        return regular_market_price, "regular_market_price"
    return last_price, "last_price" if last_price is not None else None


def fetch_market_quote(symbol: str, as_of: date, session: str) -> dict[str, Any] | None:
    """Fetch one Yahoo/yfinance quote snapshot.

    The output is deliberately a snapshot, not a minute-bar backfill. It gives
    execution gates a current pre/post-market reference without changing the
    daily OHLCV research layer.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return None

    ticker = yf.Ticker(symbol)
    fast = _fast_info_dict(ticker)
    info: dict[str, Any] = {}
    try:
        info = ticker.get_info() or {}
    except Exception:
        info = {}

    hist_last, hist_time = _history_last_price(ticker, as_of)
    quote_time = hist_time or datetime.now(timezone.utc)
    if quote_time.tzinfo is not None:
        quote_time = quote_time.astimezone(timezone.utc).replace(tzinfo=None)

    regular_market_price = _first_float(
        info.get("regularMarketPrice"),
        info.get("currentPrice"),
        _mapping_value(fast, "regularMarketPrice", "regular_market_price"),
    )
    premarket_price = _first_float(info.get("preMarketPrice"))
    postmarket_price = _first_float(info.get("postMarketPrice"))
    last_price = _first_float(
        hist_last,
        info.get("regularMarketPrice"),
        info.get("currentPrice"),
        _mapping_value(fast, "lastPrice", "last_price"),
    )
    previous_close = _first_float(
        info.get("regularMarketPreviousClose"),
        info.get("previousClose"),
        _mapping_value(fast, "regularMarketPreviousClose", "previous_close"),
    )
    active_price, active_source = _active_price(
        session=session,
        regular_market_price=regular_market_price,
        premarket_price=premarket_price,
        postmarket_price=postmarket_price,
        last_price=last_price,
    )
    if active_price is None:
        return None

    raw = {
        "regularMarketPrice": info.get("regularMarketPrice"),
        "preMarketPrice": info.get("preMarketPrice"),
        "postMarketPrice": info.get("postMarketPrice"),
        "currentPrice": info.get("currentPrice"),
        "previousClose": info.get("previousClose"),
        "fast_info_keys": sorted(fast.keys()),
    }
    return {
        "symbol": symbol,
        "as_of": as_of,
        "session": session,
        "quote_time": quote_time,
        "regular_market_price": regular_market_price,
        "premarket_price": premarket_price,
        "postmarket_price": postmarket_price,
        "last_price": last_price,
        "previous_close": previous_close,
        "active_price": active_price,
        "active_price_source": active_source,
        "currency": info.get("currency") or _mapping_value(fast, "currency"),
        "source": "yfinance_yahoo_delayed",
        "raw_json": json.dumps(raw, sort_keys=True, default=str),
    }


def fetch_market_quotes(
    symbols: list[str],
    as_of: date,
    session: str,
    *,
    max_workers: int | None = None,
    max_symbols: int | None = None,
) -> pl.DataFrame:
    session = "pre" if session in {"pre", "premarket", "morning"} else "post"
    unique_symbols = sorted({s.strip().upper() for s in symbols if str(s).strip()})
    if not unique_symbols:
        return pl.DataFrame()

    max_symbols = max_symbols or int(os.getenv("US_MARKET_QUOTES_MAX_SYMBOLS", DEFAULT_MAX_SYMBOLS))
    fetch_symbols = unique_symbols[:max_symbols]
    workers = max_workers or int(os.getenv("US_MARKET_QUOTES_MAX_WORKERS", DEFAULT_MAX_WORKERS))

    rows: list[dict[str, Any]] = []
    failures = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(fetch_market_quote, symbol, as_of, session): symbol
            for symbol in fetch_symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                row = future.result()
            except Exception as exc:  # noqa: BLE001
                failures += 1
                log.debug("market_quote_fetch_error", symbol=symbol, error=str(exc))
                continue
            if row:
                rows.append(row)
            else:
                failures += 1

    if failures:
        log.info(
            "market_quote_fetch_summary",
            requested=len(fetch_symbols),
            rows=len(rows),
            failures=failures,
            source="yfinance_yahoo_delayed",
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows, infer_schema_length=None).with_columns(pl.col("as_of").cast(pl.Date))


def upsert_market_quotes(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0
    con.register("market_quote_updates", df.to_arrow())
    con.execute(
        """
        INSERT OR REPLACE INTO market_quotes
            (symbol, as_of, session, quote_time,
             regular_market_price, premarket_price, postmarket_price,
             last_price, previous_close, active_price, active_price_source,
             currency, source, raw_json)
        SELECT symbol, as_of, session, quote_time,
               regular_market_price, premarket_price, postmarket_price,
               last_price, previous_close, active_price, active_price_source,
               currency, source, raw_json
        FROM market_quote_updates
        """
    )
    con.unregister("market_quote_updates")
    return len(df)


def fetch_and_store_market_quotes(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    session: str,
) -> int:
    df = fetch_market_quotes(symbols, as_of, session)
    return upsert_market_quotes(con, df)
