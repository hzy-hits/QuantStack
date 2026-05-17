"""Daily yfinance ingest for wedge instruments used by the bubble hedge radar.

These are not AI-universe candidates — they're the *wedge* and *housing /
banks* hedge instruments that score_bubble_hedge_radar.py needs to read the
Hedge-Wedge-Confirm-Press framework. Pulling them separately so the AI book
producer stays focused on AI infra.

Symbols:
- Rates: TLT IEF SHY TBT ^TNX
- Credit: HYG JNK LQD
- Banks: XLF BMO RY TD BNS CM
- Housing: XHB ITB
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"

WEDGE_SYMBOLS = (
    "TLT", "IEF", "SHY", "TBT", "^TNX",
    "HYG", "JNK", "LQD",
    "XLF", "BMO", "RY", "TD", "BNS", "CM",
    "XHB", "ITB",
    # ^MOVE = ICE BofA MOVE index (Treasury-implied volatility) — the most
    # direct wedge gauge. ^VIX is already fetched by the main US pipeline.
    "^MOVE",
)


def _ingest(us_db: Path, as_of: date, lookback_days: int = 365) -> int:
    try:
        import yfinance as yf
    except ImportError:
        print("error: yfinance not installed", file=sys.stderr)
        return 1
    if not us_db.exists():
        print(f"error: US db missing at {us_db}", file=sys.stderr)
        return 2
    con = duckdb.connect(str(us_db))
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS prices_daily (symbol VARCHAR, date DATE, "
            "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT, adj_close DOUBLE, "
            "PRIMARY KEY (symbol, date))"
        )
        start = as_of - timedelta(days=lookback_days)
        summary: list[tuple[str, int]] = []
        for sym in WEDGE_SYMBOLS:
            try:
                df = yf.Ticker(sym).history(
                    start=start.isoformat(),
                    end=(as_of + timedelta(days=1)).isoformat(),
                    auto_adjust=False,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"warn: {sym} fetch failed: {exc}", file=sys.stderr)
                summary.append((sym, 0))
                continue
            if df.empty:
                summary.append((sym, 0))
                continue
            inserted = 0
            for ts, row in df.iterrows():
                d = ts.date() if hasattr(ts, "date") else ts
                if d > as_of:
                    continue
                close = float(row["Close"]) if row["Close"] is not None else None
                adj = float(row["Adj Close"]) if "Adj Close" in row and row["Adj Close"] is not None else close
                con.execute(
                    "INSERT OR REPLACE INTO prices_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        sym, d.isoformat(),
                        float(row["Open"]) if row["Open"] is not None else None,
                        float(row["High"]) if row["High"] is not None else None,
                        float(row["Low"]) if row["Low"] is not None else None,
                        close,
                        int(row["Volume"]) if row["Volume"] is not None else None,
                        adj,
                    ],
                )
                inserted += 1
            summary.append((sym, inserted))
    finally:
        con.close()
    msg = ", ".join(f"{s}={n}" for s, n in summary)
    print(f"wedge ingest complete as-of {as_of}: {msg}")
    failures = sum(1 for _, n in summary if n == 0)
    return 1 if failures == len(summary) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(), default=None)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--lookback-days", type=int, default=365)
    args = parser.parse_args()
    as_of = args.as_of or date.today()
    return _ingest(args.us_db, as_of, args.lookback_days)


if __name__ == "__main__":
    sys.exit(main())
