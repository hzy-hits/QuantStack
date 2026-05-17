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
        def _num(value) -> float | None:
            # yfinance can emit NaN; normalise NaN/None → None so a malformed
            # row never raises and aborts the remaining symbols.
            try:
                if value is None:
                    return None
                f = float(value)
                return None if f != f else f  # NaN check
            except (TypeError, ValueError):
                return None

        for sym in WEDGE_SYMBOLS:
            # One try per symbol — covers the fetch AND row conversion/upsert
            # so a bad row from yfinance can't kill the rest of the book.
            try:
                df = yf.Ticker(sym).history(
                    start=start.isoformat(),
                    end=(as_of + timedelta(days=1)).isoformat(),
                    auto_adjust=False,
                )
                if df.empty:
                    summary.append((sym, 0))
                    continue
                inserted = 0
                for ts, row in df.iterrows():
                    d = ts.date() if hasattr(ts, "date") else ts
                    if d > as_of:
                        continue
                    close = _num(row.get("Close"))
                    adj = _num(row.get("Adj Close")) if "Adj Close" in row else close
                    vol = _num(row.get("Volume"))
                    con.execute(
                        "INSERT OR REPLACE INTO prices_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            sym, d.isoformat(),
                            _num(row.get("Open")), _num(row.get("High")),
                            _num(row.get("Low")), close,
                            int(vol) if vol is not None else None,
                            adj if adj is not None else close,
                        ],
                    )
                    inserted += 1
                summary.append((sym, inserted))
            except Exception as exc:  # noqa: BLE001
                print(f"warn: {sym} ingest failed: {exc}", file=sys.stderr)
                summary.append((sym, 0))
                continue
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
