"""Detect Serenity stance flips by parsing view_change.previous_stance →
current_stance in today's serenity_picks snapshot.

Writes any real flips (status != 'unchanged' AND previous != current) to
serenity_stance_flips table. Each row is independent — no diff against
yesterday's snapshot needed because view_change carries the transition.

Used downstream by:
  - render_serenity_crosscheck_section (today's flips shown in daily report)
  - backtest_serenity_flips.py (once 30+ flips accumulate)

Schema:
  serenity_stance_flips(
    flipped_at TIMESTAMP,
    ticker, previous_stance, current_stance,
    previous_view, current_view, change_type,
    priority_score, latest_return_pct, ai_chain_segment,
    detected_at TIMESTAMP, fetched_at TIMESTAMP,
    PRIMARY KEY (flipped_at, ticker)
  )

Run: python3 scripts/detect_serenity_flips.py [--date 2026-05-27]
"""
from __future__ import annotations

import argparse
import ast
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
US_DB = ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS serenity_stance_flips (
            flipped_at TIMESTAMP,
            ticker VARCHAR,
            previous_stance VARCHAR,
            current_stance VARCHAR,
            previous_view VARCHAR,
            current_view VARCHAR,
            change_type VARCHAR,
            priority_score DOUBLE,
            latest_return_pct DOUBLE,
            ai_chain_segment VARCHAR,
            detected_at TIMESTAMP DEFAULT current_timestamp,
            fetched_at TIMESTAMP,
            PRIMARY KEY (flipped_at, ticker)
        )
    """)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    args = ap.parse_args()
    target_fetched = datetime.combine(date.fromisoformat(args.date), datetime.min.time())

    con = duckdb.connect(str(US_DB))
    init_schema(con)
    rows = con.execute("""
        SELECT ticker, stance, current_view, view_change,
               priority_score, latest_return_pct, ai_chain_segment
        FROM serenity_picks
        WHERE fetched_at = ?
    """, [target_fetched]).fetchall()
    if not rows:
        print(f"  no serenity_picks for {args.date} — run fetch_serenity_picks first")
        con.close(); return

    inserted = 0
    skipped = 0
    for r in rows:
        ticker, cur_stance, cur_view, vc_raw, prio, ret, seg = r
        if not vc_raw:
            skipped += 1; continue
        try:
            vc = ast.literal_eval(vc_raw)
        except (ValueError, SyntaxError):
            skipped += 1; continue
        prev_stance = vc.get("previous_stance")
        prev_view = vc.get("previous_view")
        status = vc.get("status") or "unchanged"
        change_type = vc.get("change_type") or "none"
        # Only count real transitions
        if status == "unchanged" and (not prev_stance or prev_stance == cur_stance) \
           and (not prev_view or prev_view == cur_view):
            continue
        # Real flip — parse changed_at
        flipped_at = vc.get("changed_at")
        if not flipped_at:
            continue
        try:
            flipped_ts = datetime.fromisoformat(flipped_at)
            if flipped_ts.tzinfo:
                flipped_ts = flipped_ts.replace(tzinfo=None)  # naive for DB PK consistency
        except ValueError:
            continue
        try:
            con.execute("""
                INSERT OR REPLACE INTO serenity_stance_flips
                (flipped_at, ticker, previous_stance, current_stance,
                 previous_view, current_view, change_type, priority_score,
                 latest_return_pct, ai_chain_segment, detected_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, ?)
            """, [
                flipped_ts, ticker, prev_stance, cur_stance,
                prev_view, cur_view, change_type,
                float(prio) if prio is not None else None,
                float(ret) if ret is not None else None,
                seg, target_fetched,
            ])
            inserted += 1
        except (duckdb.Error, TypeError, ValueError) as e:
            print(f"  [warn] {ticker}: {e}", file=sys.stderr)

    total = con.execute("SELECT COUNT(*) FROM serenity_stance_flips").fetchone()[0]
    by_change = con.execute("""
        SELECT change_type, COUNT(*) FROM serenity_stance_flips
        GROUP BY change_type ORDER BY 2 DESC
    """).fetchall()
    print(f"  flips detected today: {inserted} (skipped {skipped} unchanged)")
    print(f"  serenity_stance_flips cumulative: {total} rows")
    for ct, n in by_change:
        print(f"    {ct}: {n}")
    con.close()


if __name__ == "__main__":
    main()
