"""Refresh CBOE chains for 8 cash-settled indices + compute GEX + render.

Designed to run every 30 min during US market hours (9:30 ET - 16:00 ET).
One invocation does the full pipeline:
  1. Fetch fresh CBOE chain for each index (uses existing
     fetch_options_snapshot_with_quotes — already handles caret/W-suffix)
  2. Upsert into options_chain_quotes (PK includes as_of so each session
     keeps its own snapshot)
  3. Run compute_index_gex.py + render_index_dashboard.py
  4. Print summary line for cron log

CBOE CDN data is delayed ~15min, so "intraday" here means a 30-min
snapshot, not real-time. For true real-time you'd need OPRA / Polygon /
IBKR feeds.

Usage:
  python3 scripts/intraday/refresh_index_chains.py
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[2]
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.data_ingestion.options import (  # noqa: E402
    fetch_options_snapshot_with_quotes,
    upsert_options_chain_quotes,
)

DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"

INDEX_SYMBOLS = [
    "^SPX", "^NDX", "^XSP", "^XND", "^MRUT", "^RUT", "^XEO", "^VIX",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=None, help="Override as_of (default: today)")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="Skip CBOE refresh, only re-run GEX + render on existing snapshot")
    args = ap.parse_args()

    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()
    snapshot_time = datetime.utcnow()
    print(f"=== refresh_index_chains  as_of={as_of}  snapshot_time={snapshot_time.isoformat(timespec='seconds')}")

    if not args.skip_fetch:
        print(f"--- fetching {len(INDEX_SYMBOLS)} indices from CBOE ---")
        snap_df, ana_df, quote_df = fetch_options_snapshot_with_quotes(INDEX_SYMBOLS, as_of)
        if len(quote_df) == 0:
            print("  no quotes returned — CBOE blacklist or transient failure; exiting")
            return
        # Persist chain quotes
        con = duckdb.connect(str(DB_PATH))
        n_quotes = upsert_options_chain_quotes(con, quote_df)
        print(f"  upserted {n_quotes:,} chain quotes")
        con.execute("CHECKPOINT")
        con.close()
    else:
        print("--- skip-fetch: using existing chain snapshot ---")

    # Compute GEX
    print("--- computing index_gex_snapshots ---")
    gex_cmd = [
        sys.executable, str(STACK_ROOT / "scripts" / "intraday" / "compute_index_gex.py"),
        "--as-of", as_of.isoformat(),
        "--snapshot-time", snapshot_time.isoformat(),
    ]
    result = subprocess.run(gex_cmd, cwd=STACK_ROOT, text=True,
                            capture_output=True, timeout=120, check=False)
    if result.returncode != 0:
        print(f"  GEX compute failed: {result.stderr[-300:]}")
    else:
        # Strip the per-row debug lines, keep just the summary
        last_lines = result.stdout.strip().splitlines()[-3:]
        for ln in last_lines:
            print(f"  {ln}")

    # Render dashboard
    print("--- rendering dashboard ---")
    render_cmd = [
        sys.executable, str(STACK_ROOT / "scripts" / "intraday" / "render_index_dashboard.py"),
        "--as-of", as_of.isoformat(),
    ]
    result = subprocess.run(render_cmd, cwd=STACK_ROOT, text=True,
                            capture_output=True, timeout=60, check=False)
    if result.returncode == 0:
        print(f"  {result.stdout.strip()}")
    else:
        print(f"  render failed: {result.stderr[-300:]}")

    print(f"=== refresh complete  as_of={as_of}")


if __name__ == "__main__":
    main()
