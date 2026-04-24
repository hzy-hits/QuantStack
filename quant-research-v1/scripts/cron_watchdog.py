#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent
SRC_ROOT = PROJECT_DIR / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from quant_bot.orchestration.watchdog import run_watchdog


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan managed cron jobs across repos and retrigger missed runs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect due tasks without launching a rerun.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="How many recent local calendar days to inspect for missed runs.",
    )
    parser.add_argument(
        "--max-auto-triggers",
        type=int,
        default=1,
        help="Maximum watchdog launches per task/date before requiring manual intervention.",
    )
    args = parser.parse_args()

    messages = run_watchdog(
        PROJECT_DIR,
        dry_run=args.dry_run,
        lookback_days=args.lookback_days,
        max_auto_triggers=args.max_auto_triggers,
    )
    for msg in messages:
        print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
