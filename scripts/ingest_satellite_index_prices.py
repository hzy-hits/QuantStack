"""Ingest international satellite-pool index prices into the US DuckDB.

The AI Infra 卫星资产池 spans Taiwan, Japan, Korea, Europe, and Israel.
The daily report needs trailing-return context for the corresponding equity
benchmarks so an operator can see, e.g., "Taiwan small-caps up 4% but our
COWOS suppliers up 6%". We piggy-back on the existing US DuckDB
(`quant-research-v1/data/quant.duckdb`) because it already stores
yfinance-sourced OHLCV in `prices_daily`.

Run example:

    python3 scripts/ingest_satellite_index_prices.py --as-of 2026-05-13

The script is idempotent. If yfinance returns nothing for a symbol (often
^N225/^KS11 on the public endpoint), the script reports the gap and exits
non-zero only if every symbol fails.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class SatelliteIndex:
    canonical: str
    yfinance_symbol: str
    label: str
    region: str


CANONICAL_SATELLITE: tuple[SatelliteIndex, ...] = (
    SatelliteIndex("^TWII", "^TWII", "TAIEX (台湾加权)", "Taiwan"),
    SatelliteIndex("^N225", "^N225", "Nikkei 225 (日经225)", "Japan"),
    SatelliteIndex("^KS11", "^KS11", "KOSPI (韩国综指)", "Korea"),
    SatelliteIndex("^AEX", "^AEX", "AEX (荷兰AEX)", "Europe-NL"),
    SatelliteIndex("EWJ", "EWJ", "EWJ (Japan ETF)", "Japan"),
    SatelliteIndex("EWY", "EWY", "EWY (Korea ETF)", "Korea"),
    SatelliteIndex("EWT", "EWT", "EWT (Taiwan ETF)", "Taiwan"),
    SatelliteIndex("EWN", "EWN", "EWN (Netherlands ETF)", "Europe-NL"),
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--us-db",
        type=Path,
        default=Path("quant-research-v1/data/quant.duckdb"),
        help="US DuckDB where prices_daily lives.",
    )
    parser.add_argument(
        "--as-of",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Upper-bound trade date (inclusive). Defaults to today.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=730,
        help="Trailing days to fetch via yfinance.",
    )
    return parser.parse_args()


def _ensure_prices_daily(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prices_daily (
            symbol VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adj_close DOUBLE,
            PRIMARY KEY (symbol, date)
        )
        """
    )


def _fetch_history(symbol: str, start: date, end: date):
    try:
        import yfinance as yf  # type: ignore[import-not-found]
    except ImportError as exc:
        print(f"error: yfinance not installed ({exc})", file=sys.stderr)
        return None
    try:
        ticker = yf.Ticker(symbol)
        # yfinance treats `end` as exclusive; pad by one day.
        df = ticker.history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=False,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"error: yfinance fetch failed for {symbol}: {exc}", file=sys.stderr)
        return None
    return df


def _upsert(con: duckdb.DuckDBPyConnection, spec: SatelliteIndex, df, as_of: date) -> int:
    rows: list[tuple] = []
    for ts, record in df.iterrows():
        trade_date = ts.date() if hasattr(ts, "date") else ts
        if trade_date > as_of:
            continue
        close = float(record.get("Close")) if record.get("Close") is not None else None
        adj = record.get("Adj Close")
        adj_close = float(adj) if adj is not None and not _is_nan(adj) else close
        rows.append(
            (
                spec.canonical,
                trade_date,
                _maybe_float(record.get("Open")),
                _maybe_float(record.get("High")),
                _maybe_float(record.get("Low")),
                close,
                _maybe_int(record.get("Volume")),
                adj_close,
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO prices_daily (symbol, date, open, high, low, close, volume, adj_close)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _is_nan(value) -> bool:
    try:
        return value != value  # NaN != NaN
    except Exception:
        return False


def _maybe_float(value):
    if value is None or _is_nan(value):
        return None
    return float(value)


def _maybe_int(value):
    if value is None or _is_nan(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> int:
    args = _parse_args()
    as_of = args.as_of or date.today()
    start = date.fromordinal(max(1, as_of.toordinal() - args.lookback_days))
    if not args.us_db.exists():
        print(f"error: US db missing at {args.us_db}", file=sys.stderr)
        return 2

    con = duckdb.connect(str(args.us_db))
    try:
        _ensure_prices_daily(con)
        per_symbol: list[tuple[str, int]] = []
        failures: list[str] = []
        for spec in CANONICAL_SATELLITE:
            df = _fetch_history(spec.yfinance_symbol, start, as_of)
            if df is None or getattr(df, "empty", True):
                failures.append(spec.canonical)
                continue
            inserted = _upsert(con, spec, df, as_of)
            per_symbol.append((spec.canonical, inserted))
    finally:
        con.close()

    print(
        f"Satellite index ingest complete as-of {as_of.isoformat()}: "
        + ", ".join(f"{code}={count}" for code, count in per_symbol)
        + (f"; failed: {','.join(failures)}" if failures else "")
    )
    if failures and not per_symbol:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
