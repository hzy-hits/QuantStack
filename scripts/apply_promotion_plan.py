"""Apply a confirmed promotion plan to `expansion_candidates_promoted_v1.csv`.

Closed-loop step:

    source-review queue
      -> readiness ledger (score_source_review_readiness.py)
      -> promotion plan (derive_promotion_plan_from_readiness.py)
      -> apply_promotion_plan.py (this script, *human-confirmed*)
      -> ai_infra/reports/expansion_candidates_promoted_v1.csv

Safety contract:

- Default mode is `--dry-run`. The script prints rows that *would* be appended
  but never writes anything.
- Writing requires explicit `--confirm`. Without it the script refuses.
- Only `recommendation == "promote_now"` rows are eligible.
- If `--tickers` is provided, only those tickers from the plan are considered.
  This is the recommended workflow: operator hand-picks the subset.
- Symbols already present in the promoted CSV are skipped — the file is append-only.
- Backup: the prior CSV is copied to `.bak` next to it before write.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROMOTED = STACK_ROOT / "ai_infra" / "reports" / "expansion_candidates_promoted_v1.csv"
DEFAULT_PLAN_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_promotion_plan"

PROMOTED_FIELDS = (
    "as_of",
    "symbol",
    "company_name",
    "market",
    "ai_module",
    "source_url",
    "source_type",
    "source_date",
    "confidence",
    "evidence_state",
    "financial_translation",
    "universe_row",
)

ELIGIBLE_RECOMMENDATION = "promote_now"


def _resolve_market(market_country: str, asset_pool: str) -> str:
    if "中国" in asset_pool or market_country in {"A股主板", "中国"}:
        return "CN"
    if "美国" in asset_pool or market_country == "US":
        return "US"
    if "卫星" in asset_pool:
        return "Satellite"
    return market_country or "Unknown"


def _plan_path_default(as_of: str) -> Path:
    return DEFAULT_PLAN_ROOT / as_of / "promotion_plan.csv"


def _existing_symbols(path: Path) -> set[str]:
    if not path.exists():
        return set()
    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            sym = (row.get("symbol") or "").strip().upper()
            if sym:
                seen.add(sym)
    return seen


def _load_plan(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _to_row(plan_row: dict[str, str], as_of: str) -> dict[str, str]:
    market = _resolve_market(plan_row.get("market_country") or "", plan_row.get("asset_pool") or "")
    return {
        "as_of": as_of,
        "symbol": plan_row.get("primary_ticker") or plan_row.get("ticker_field") or "",
        "company_name": plan_row.get("company") or "",
        "market": market,
        "ai_module": plan_row.get("module") or "",
        "source_url": "_pending_evidence_card_link_",
        "source_type": "promotion_plan_derived",
        "source_date": as_of,
        "confidence": "medium",
        "evidence_state": "promoted_via_readiness_loop_pending_evidence_card",
        "financial_translation": plan_row.get("rationale") or "",
        "universe_row": "",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument(
        "--plan",
        type=Path,
        default=None,
        help="Path to promotion_plan.csv. Defaults to the plan under DEFAULT_PLAN_ROOT/<as-of>.",
    )
    parser.add_argument(
        "--promoted",
        type=Path,
        default=DEFAULT_PROMOTED,
        help="Path to expansion_candidates_promoted_v1.csv.",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="If supplied, only include these primary tickers from the plan. Recommended for human-in-the-loop runs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print rows that would be appended. Default behaviour (no --confirm).",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required to actually write rows to the promoted CSV.",
    )
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    plan_path = args.plan or _plan_path_default(as_of)
    if not plan_path.exists():
        print(f"error: plan not found at {plan_path}", file=sys.stderr)
        return 2
    if not args.promoted.parent.exists():
        print(f"error: promoted CSV parent dir missing: {args.promoted.parent}", file=sys.stderr)
        return 2

    plan_rows = _load_plan(plan_path)
    eligible = [row for row in plan_rows if row.get("recommendation") == ELIGIBLE_RECOMMENDATION]
    if args.tickers:
        wanted = {t.strip().upper() for t in args.tickers if t.strip()}
        eligible = [row for row in eligible if (row.get("primary_ticker") or "").upper() in wanted]

    existing = _existing_symbols(args.promoted)
    to_append: list[dict[str, str]] = []
    skipped: list[str] = []
    for row in eligible:
        symbol = (row.get("primary_ticker") or "").strip().upper()
        if not symbol:
            continue
        if symbol in existing:
            skipped.append(symbol)
            continue
        to_append.append(_to_row(row, as_of))

    print(f"Plan: {plan_path}")
    print(f"Promoted CSV: {args.promoted}")
    print(f"Eligible (promote_now): {len(eligible)}")
    print(f"Already promoted: {sorted(skipped) if skipped else '-'}")
    print(f"To append: {[row['symbol'] for row in to_append]}")

    if not to_append:
        return 0

    if not args.confirm:
        print()
        print("Dry run — pass --confirm to write rows.")
        return 0

    # Backup, then append.
    if args.promoted.exists():
        backup = args.promoted.with_suffix(args.promoted.suffix + ".bak")
        shutil.copy2(args.promoted, backup)
        print(f"Backup created: {backup}")

    file_exists = args.promoted.exists()
    with args.promoted.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROMOTED_FIELDS)
        if not file_exists or args.promoted.stat().st_size == 0:
            writer.writeheader()
        for row in to_append:
            writer.writerow(row)
    print(f"Appended {len(to_append)} rows to {args.promoted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
