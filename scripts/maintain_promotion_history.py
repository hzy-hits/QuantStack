"""Append daily promotion-plan rows to a long-running history ledger.

Each day the readiness → promotion pipeline emits
`reports/review_dashboard/ai_infra_promotion_plan/<date>/promotion_plan.csv`.
This script accumulates those rows into a single ledger so a researcher can
look back and ask:

- When did this ticker first appear as `promote_now`?
- How long did `evidence_partial` linger before becoming `ready_for_promotion`?
- Which `blocked_by_counterevidence` names eventually cleared?

The ledger lives at
`ai_infra/reports/promotion_history.csv` (or a path supplied via
`--history-csv`). The script is idempotent on `(as_of, primary_ticker)` —
re-running for the same date will refresh those rows in place rather than
duplicating them.

Default safety: append-only with backup. If the ledger does not exist we
create it with a header row.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAN_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_promotion_plan"
DEFAULT_HISTORY = STACK_ROOT / "ai_infra" / "reports" / "promotion_history.csv"

HISTORY_FIELDS = (
    "as_of",
    "primary_ticker",
    "ticker_field",
    "company",
    "asset_pool",
    "market_country",
    "bfs_depth",
    "module",
    "priority_tier",
    "readiness_tier",
    "readiness_score",
    "recommendation",
    "rationale",
    "counterevidence",
)


def _plan_path_default(as_of: str) -> Path:
    return DEFAULT_PLAN_ROOT / as_of / "promotion_plan.csv"


def _load_history(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return {
            ((row.get("as_of") or "").strip(), (row.get("primary_ticker") or "").strip().upper()): row
            for row in csv.DictReader(handle)
        }


def _load_plan(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _to_history_row(plan_row: dict[str, str], as_of: str) -> dict[str, str]:
    return {
        "as_of": as_of,
        "primary_ticker": (plan_row.get("primary_ticker") or "").upper(),
        "ticker_field": plan_row.get("ticker_field") or "",
        "company": plan_row.get("company") or "",
        "asset_pool": plan_row.get("asset_pool") or "",
        "market_country": plan_row.get("market_country") or "",
        "bfs_depth": plan_row.get("bfs_depth") or "",
        "module": plan_row.get("module") or "",
        "priority_tier": plan_row.get("priority_tier") or "",
        "readiness_tier": plan_row.get("readiness_tier") or "",
        "readiness_score": plan_row.get("readiness_score") or "",
        "recommendation": plan_row.get("recommendation") or "",
        "rationale": plan_row.get("rationale") or "",
        "counterevidence": plan_row.get("counterevidence") or "",
    }


def _write_history(path: Path, rows: dict[tuple[str, str], dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(
        rows.values(),
        key=lambda r: (r.get("as_of") or "", r.get("primary_ticker") or ""),
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDS)
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow({key: row.get(key, "") for key in HISTORY_FIELDS})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--plan", type=Path, default=None)
    parser.add_argument("--history-csv", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    plan_path = args.plan or _plan_path_default(as_of)
    if not plan_path.exists():
        print(f"error: plan not found at {plan_path}", file=sys.stderr)
        return 2

    plan_rows = _load_plan(plan_path)
    if not plan_rows:
        print(f"plan {plan_path} is empty; nothing to record")
        return 0

    history = _load_history(args.history_csv)
    if args.history_csv.exists() and not args.no_backup:
        backup = args.history_csv.with_suffix(args.history_csv.suffix + ".bak")
        shutil.copy2(args.history_csv, backup)

    new_count = 0
    updated_count = 0
    for plan_row in plan_rows:
        primary = (plan_row.get("primary_ticker") or "").strip().upper()
        if not primary:
            continue
        key = (as_of, primary)
        row = _to_history_row(plan_row, as_of)
        if key in history:
            if history[key] != row:
                updated_count += 1
            history[key] = row
        else:
            history[key] = row
            new_count += 1

    _write_history(args.history_csv, history)
    print(
        f"Promotion history updated: {args.history_csv}; new={new_count}, updated={updated_count}, total_rows={len(history)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
