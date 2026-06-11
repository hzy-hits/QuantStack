"""Catch-up runner for missed scheduled tasks.

WSL2 suspends when idle / when Windows sleeps, and cron does NOT replay
missed jobs on resume — a research task scheduled for 10:00 is simply lost
if the machine was asleep then. (Observed 2026-05-19: cron stopped logging
after 09:42; the 10:00 / 11:xx / 12:xx research tasks never ran.)

Run this every ~15 minutes. It detects tasks that were due earlier today
but never succeeded, and runs them late via ops/run_task.sh. run_task's
flock makes a late run and a normal cron run mutually exclusive.

Scope: only the light, cadence-flexible groups (research / factor /
paper). The heavy market-pipeline runs (cn.* / us.* /
weekly.*) are deliberately excluded — running those hours late is worse
than skipping (stale market timing, late emails, 4h jobs colliding).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from tasklib import (
    cron_matches as _matches,
    order_by_dependency,
    parse_cron_field as _parse_field,
)

STACK_ROOT = Path(__file__).resolve().parents[1]
TASKS_YAML = STACK_ROOT / "ops" / "tasks.yaml"
STATE_DIR = STACK_ROOT / "ops" / "state"
RUN_TASK = STACK_ROOT / "ops" / "run_task.sh"
CST = timezone(timedelta(hours=8))

CATCHUP_GROUPS = {"research", "factor", "paper"}
GRACE_MINUTES = 20   # ignore a slot younger than this — let normal cron try first


def most_recent_fire(expr: str, now: datetime) -> datetime | None:
    """Latest datetime <= now that matches expr, searching back 24h."""
    cur = now.replace(second=0, microsecond=0)
    for _ in range(1440):
        if _matches(expr, cur):
            return cur
        cur -= timedelta(minutes=1)
    return None


def _last_success(task_id: str) -> datetime | None:
    path = STATE_DIR / f"{task_id}.last_success.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return datetime.fromisoformat(data["finished_at"])
    except (OSError, ValueError, KeyError):
        return None


def find_missed(now: datetime) -> list[tuple[str, datetime]]:
    """[(task_id, due_time)] for catch-up-eligible tasks that missed today."""
    registry = yaml.safe_load(TASKS_YAML.read_text(encoding="utf-8"))
    tasks = registry.get("tasks") or {}
    missed: list[tuple[str, datetime]] = []
    for task_id, task in tasks.items():
        if not isinstance(task, dict) or task.get("group") not in CATCHUP_GROUPS:
            continue
        schedule = str(task.get("schedule") or "")
        if len(schedule.split()) != 5:        # skip @reboot / malformed
            continue
        fire = most_recent_fire(schedule, now)
        if fire is None or fire.date() != now.date():
            continue                          # only catch up TODAY's missed slots
        if (now - fire) < timedelta(minutes=GRACE_MINUTES):
            continue                          # too fresh — let cron handle it
        last = _last_success(task_id)
        if last is None or last < fire:
            missed.append((task_id, fire))
    # depends_on targets first, so a blocked dependent finds its dependency
    # already replayed in the same catch-up pass.
    return order_by_dependency(missed, tasks)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be caught up, run nothing")
    args = parser.parse_args()

    now = datetime.now(CST)
    missed = find_missed(now)
    if not missed:
        print(f"catch-up {now:%Y-%m-%d %H:%M} CST: nothing missed")
        return 0

    print(f"catch-up {now:%Y-%m-%d %H:%M} CST: {len(missed)} missed task(s)")
    failures = 0
    for task_id, fire in missed:
        print(f"  {task_id}: due {fire:%H:%M}, not run "
              f"{'(dry-run)' if args.dry_run else '→ running late'}")
        if args.dry_run:
            continue
        result = subprocess.run([str(RUN_TASK), task_id], cwd=str(STACK_ROOT))
        if result.returncode != 0:
            failures += 1
            print(f"  {task_id}: run_task exited {result.returncode}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
