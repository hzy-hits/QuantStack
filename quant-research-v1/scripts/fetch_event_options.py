#!/usr/bin/env python3
"""Fetch event-watch option chains into DuckDB.

This is a targeted repair tool for event trades whose symbols were not in the
daily candidate pool. It persists CBOE delayed bid/ask legs, snapshots, and
analysis so spread tickets can use real leg quotes instead of last-price marks.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quant_bot.data_ingestion.options import (  # noqa: E402
    fetch_options_snapshot_with_quotes,
    upsert_options,
    upsert_options_analysis,
    upsert_options_chain_quotes,
)
from quant_bot.storage.db import init_schema  # noqa: E402


DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "quant.duckdb"


def _previous_weekday(day: date) -> date:
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def default_as_of(now: datetime | None = None) -> date:
    """Use the latest completed US trading date for delayed quote persistence."""
    et_now = now.astimezone(ZoneInfo("America/New_York")) if now else datetime.now(ZoneInfo("America/New_York"))
    if et_now.weekday() >= 5:
        return _previous_weekday(et_now.date())
    if et_now.hour < 16 or (et_now.hour == 16 and et_now.minute < 15):
        return _previous_weekday(et_now.date())
    return et_now.date()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["BA", "BABA"], help="Option symbols to fetch.")
    parser.add_argument("--as-of", default=None, help="Persistence date, usually latest completed US session.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB path to update.")
    parser.add_argument("--max-expiries", type=int, default=4, help="Number of nearest expiries to persist.")
    parser.add_argument(
        "--target-expiry",
        default="2026-05-22",
        help="Expiry to print spread diagnostics for after persistence.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print diagnostics without writing DuckDB.",
    )
    return parser.parse_args()


def _spread_pairs(symbol: str) -> list[tuple[float, float]]:
    if symbol.upper() == "BA":
        return [(235.0, 250.0), (230.0, 245.0), (240.0, 255.0)]
    if symbol.upper() == "BABA":
        return [(145.0, 155.0)]
    return []


def _row_by_strike(df: pl.DataFrame, strike: float) -> dict | None:
    rows = df.filter(pl.col("strike") == strike).to_dicts()
    return rows[0] if rows else None


def print_spread_diagnostics(quotes: pl.DataFrame, expiry: str) -> None:
    if quotes.is_empty():
        print("No chain quotes fetched.")
        return
    target = quotes.filter((pl.col("expiry") == expiry) & (pl.col("option_type") == "call"))
    if target.is_empty():
        print(f"No call quotes found for target expiry {expiry}.")
        return

    print(f"\nSpread diagnostics for {expiry}")
    print(
        "symbol,underlying,spread,long_bid,long_ask,long_mid,short_bid,short_ask,"
        "short_mid,natural_debit,mid_debit,breakeven_mid,max_profit_mid,rr_mid"
    )
    for symbol in sorted(set(target.get_column("symbol").to_list())):
        sub = target.filter(pl.col("symbol") == symbol)
        current_prices = [x for x in sub.get_column("current_price").to_list() if x is not None]
        current_price = float(current_prices[0]) if current_prices else None
        for long_strike, short_strike in _spread_pairs(symbol):
            long_row = _row_by_strike(sub, long_strike)
            short_row = _row_by_strike(sub, short_strike)
            if not long_row or not short_row:
                continue
            natural = float(long_row["ask"] or 0.0) - float(short_row["bid"] or 0.0)
            mid = float(long_row["mid"] or 0.0) - float(short_row["mid"] or 0.0)
            width = short_strike - long_strike
            max_profit = width - mid
            breakeven = long_strike + mid
            rr = max_profit / mid if mid > 0 else None
            print(
                f"{symbol},{current_price:.2f},{long_strike:.0f}/{short_strike:.0f},"
                f"{float(long_row['bid'] or 0.0):.2f},{float(long_row['ask'] or 0.0):.2f},"
                f"{float(long_row['mid'] or 0.0):.3f},"
                f"{float(short_row['bid'] or 0.0):.2f},{float(short_row['ask'] or 0.0):.2f},"
                f"{float(short_row['mid'] or 0.0):.3f},"
                f"{natural:.3f},{mid:.3f},{breakeven:.3f},{max_profit:.3f},"
                f"{rr:.3f}" if rr is not None else "-"
            )


def main() -> None:
    args = parse_args()
    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date() if args.as_of else default_as_of()
    symbols = sorted({symbol.strip().upper() for symbol in args.symbols if symbol.strip()})
    if not symbols:
        raise SystemExit("No symbols provided.")

    print(
        f"Fetching CBOE delayed option chains: symbols={','.join(symbols)} "
        f"as_of={as_of.isoformat()} max_expiries={args.max_expiries}"
    )
    snapshot_df, analysis_df, quote_df = fetch_options_snapshot_with_quotes(
        symbols,
        as_of,
        max_expiries=max(1, int(args.max_expiries)),
    )
    print(
        f"Fetched rows: snapshot={len(snapshot_df)} analysis={len(analysis_df)} "
        f"chain_quotes={len(quote_df)}"
    )

    if not args.dry_run:
        args.db.parent.mkdir(parents=True, exist_ok=True)
        init_schema(args.db)
        con = duckdb.connect(str(args.db))
        try:
            n_snapshot = upsert_options(con, snapshot_df)
            n_analysis = upsert_options_analysis(con, analysis_df)
            n_quotes = upsert_options_chain_quotes(con, quote_df)
            con.execute("CHECKPOINT")
            print(
                f"Persisted to {args.db}: snapshot={n_snapshot} "
                f"analysis={n_analysis} chain_quotes={n_quotes}"
            )
        finally:
            con.close()

    print_spread_diagnostics(quote_df, args.target_expiry)


if __name__ == "__main__":
    main()
