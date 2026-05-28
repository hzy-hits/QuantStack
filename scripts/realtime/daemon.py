"""Orchestrator: poll csv → detect → advise → notify.

Runs as one long-lived daemon. Combines csv_watcher + setup_detector +
llm_advisor + telegram in a single process so you only manage 1 pid.

Usage:
  python3 scripts/realtime/daemon.py --csv-path C:/TOS/snapshot.csv

The daemon:
  1. Polls the CSV every poll_seconds (default 2s) and upserts to DuckDB
  2. Every detect_seconds (default 30s) runs setup_detector
  3. On each fired setup → calls llm_advisor → sends Telegram
  4. Logs everything to stdout (point cron/systemd at it)

Press Ctrl-C to stop.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(STACK_ROOT / "scripts" / "realtime"))

# Import from sibling modules
from csv_watcher import _process_csv, _ensure_schema  # noqa: E402
from setup_detector import detect_once  # noqa: E402
from llm_advisor import advise  # noqa: E402
from notify_telegram import send_advisor, send_message  # noqa: E402

DB_PATH = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-path", required=True)
    ap.add_argument("--poll-seconds", type=float, default=2.0,
                    help="CSV poll interval (default 2s)")
    ap.add_argument("--detect-seconds", type=float, default=30.0,
                    help="Setup detection cadence (default 30s)")
    ap.add_argument("--no-llm", action="store_true",
                    help="Detect + log only; skip LLM advisor + Telegram")
    ap.add_argument("--no-telegram", action="store_true",
                    help="Run LLM but don't send to Telegram (for testing)")
    args = ap.parse_args()
    csv_path = Path(args.csv_path)

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    _ensure_schema(con)

    print(f"[daemon] starting · csv={csv_path} poll={args.poll_seconds}s detect={args.detect_seconds}s")
    print(f"[daemon] llm={'OFF' if args.no_llm else 'ON'}  telegram={'OFF' if args.no_telegram else 'ON'}")

    last_mtime = 0.0
    last_detect = 0.0
    n_setups_today = 0
    n_csv_upserts = 0

    # Notify on startup
    if not args.no_telegram:
        try:
            send_message(f"🚀 Realtime advisor started · {datetime.utcnow().isoformat(timespec='seconds')}Z")
        except SystemExit as e:
            print(f"[daemon] telegram config missing — running without notifications: {e}")
            args.no_telegram = True

    try:
        while True:
            now = time.time()
            # 1. Poll CSV
            try:
                if csv_path.exists():
                    mtime = csv_path.stat().st_mtime
                    if mtime != last_mtime:
                        n = _process_csv(csv_path, con)
                        if n > 0:
                            n_csv_upserts += 1
                            con.execute("CHECKPOINT")
                        last_mtime = mtime
            except OSError as e:
                print(f"[daemon] csv read err: {e}")

            # 2. Periodic detection
            if now - last_detect >= args.detect_seconds:
                last_detect = now
                try:
                    setups = detect_once(con)
                    for s in setups:
                        n_setups_today += 1
                        sd = asdict(s)
                        print(f"[daemon] SETUP #{n_setups_today} {sd['type']} {sd['symbol']}: {sd['summary']}")
                        if args.no_llm:
                            continue
                        # 3. LLM advise
                        advisor = advise(sd)
                        if not advisor:
                            print(f"[daemon] advisor returned None for {sd['type']}")
                            continue
                        print(f"[daemon] advisor regime={advisor.get('regime_now')} "
                              f"ideas={len(advisor.get('trade_ideas') or [])}")
                        # 4. Notify
                        if not args.no_telegram:
                            ok = send_advisor(sd, advisor)
                            print(f"[daemon] telegram {'sent' if ok else 'FAILED'}")
                except (duckdb.Error, KeyError, ValueError) as e:
                    print(f"[daemon] detect/advise loop error: {e}")
                    traceback.print_exc()

            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print(f"\n[daemon] stopped · csv_upserts={n_csv_upserts} setups_today={n_setups_today}")
        if not args.no_telegram:
            send_message(f"🛑 Realtime advisor stopped · {n_setups_today} setups today")
    finally:
        con.close()


if __name__ == "__main__":
    main()
