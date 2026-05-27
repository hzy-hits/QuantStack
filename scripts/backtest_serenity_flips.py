"""Backtest Serenity stance flips → forward 5d/10d/20d returns.

For each flip recorded in serenity_stance_flips, joins to prices_daily
(the symbol's own price) and computes forward return at multiple horizons.
Reports by transition type (bullish→neutral, neutral→bearish, etc.).

Refuses to draw conclusions when sample size is too small (default
min_per_bucket=10). When < min, prints "accumulating" with progress.

Run: python3 scripts/backtest_serenity_flips.py [--min-per-bucket 10]
"""
from __future__ import annotations

import argparse
import statistics as stat
from collections import defaultdict
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
US_DB = ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def fwd_return(con: duckdb.DuckDBPyConnection, symbol: str, base_date: date,
               horizon_days: int) -> float | None:
    rows = con.execute("""
        SELECT date, close FROM prices_daily
        WHERE symbol = ? AND close IS NOT NULL AND date >= ?
        ORDER BY date LIMIT ?
    """, [symbol, base_date.isoformat(), horizon_days + 2]).fetchall()
    if len(rows) <= horizon_days:
        return None
    base_close = float(rows[0][1])
    end_close = float(rows[horizon_days][1])
    return (end_close / base_close - 1) * 100


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-per-bucket", type=int, default=10)
    args = ap.parse_args()

    con = duckdb.connect(str(US_DB), read_only=True)
    flips = con.execute("""
        SELECT flipped_at, ticker, previous_stance, current_stance,
               change_type, priority_score
        FROM serenity_stance_flips
        ORDER BY flipped_at DESC
    """).fetchall()
    print(f"=== Serenity stance flip backtest ===")
    print(f"  total flips on record: {len(flips)}")
    if not flips:
        print("  no data yet — let daily cron accumulate flips for 7-30 days then re-run")
        con.close(); return

    buckets = defaultdict(list)
    skipped_no_price = 0
    for flipped_at, ticker, prev, cur, change_type, prio in flips:
        if not prev or not cur: continue
        key = f"{prev}→{cur}"
        flip_date = flipped_at.date() if hasattr(flipped_at, "date") else flipped_at
        ret_5d = fwd_return(con, ticker, flip_date, 5)
        ret_10d = fwd_return(con, ticker, flip_date, 10)
        ret_20d = fwd_return(con, ticker, flip_date, 20)
        if all(v is None for v in (ret_5d, ret_10d, ret_20d)):
            skipped_no_price += 1; continue
        buckets[key].append({"ret_5d": ret_5d, "ret_10d": ret_10d, "ret_20d": ret_20d})
    con.close()
    if skipped_no_price:
        print(f"  skipped {skipped_no_price} (forward window not yet complete)")
    print()
    for key in sorted(buckets, key=lambda k: -len(buckets[k])):
        rows = buckets[key]
        if len(rows) < args.min_per_bucket:
            print(f"  {key:30}: N={len(rows)}  [accumulating, need {args.min_per_bucket-len(rows)} more]")
            continue
        for h in ("5d", "10d", "20d"):
            vals = [r[f"ret_{h}"] for r in rows if r[f"ret_{h}"] is not None]
            if not vals: continue
            avg = stat.mean(vals)
            med = stat.median(vals)
            win = sum(1 for v in vals if v > 0) / len(vals) * 100
            print(f"  {key:30} {h:>3} N={len(vals):3} avg={avg:+6.2f}% median={med:+6.2f}% win={win:.0f}%")
        print()


if __name__ == "__main__":
    main()
