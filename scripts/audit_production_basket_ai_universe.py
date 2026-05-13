"""Audit production_basket entries against the AI infra universe contract.

Reads `us_opportunity_ranker.json` and `cn_opportunity_ranker.json` under a
review-dashboard date directory and asserts:

1. The `ai_infra_gate.contract` field is `ai_infra_universe_only`.
2. Every row in `production_basket` carries `ai_infra_universe: True`.
3. Every row in `production_basket` has a non-empty `ai_infra_current_pool`
   (i.e. came from the source-reviewed universe ledger, not a fall-through).

The script exits non-zero on any violation so it can wire into ops cron and
review_packet generation.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DASHBOARD = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"


def _load(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _check_payload(market: str, payload: dict) -> list[str]:
    errors: list[str] = []
    gate = payload.get("ai_infra_gate") or {}
    contract = gate.get("contract")
    if contract != "ai_infra_universe_only":
        errors.append(f"{market}: ai_infra_gate.contract={contract!r} (want ai_infra_universe_only)")

    basket = payload.get("production_basket") or []
    if not isinstance(basket, list):
        errors.append(f"{market}: production_basket is not a list ({type(basket).__name__})")
        return errors

    for row in basket:
        symbol = row.get("symbol") or "<unknown>"
        if not row.get("ai_infra_universe"):
            errors.append(f"{market}: {symbol} in production_basket but ai_infra_universe is not True")
        if not row.get("ai_infra_current_pool"):
            errors.append(f"{market}: {symbol} has no ai_infra_current_pool tag")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True, help="Report date, e.g. 2026-05-13")
    parser.add_argument(
        "--dashboard-root",
        type=Path,
        default=DEFAULT_DASHBOARD,
        help="Root of main_strategy_v2 review dashboard outputs.",
    )
    args = parser.parse_args()

    date_dir = args.dashboard_root / args.as_of
    if not date_dir.exists():
        print(f"audit: dashboard date directory missing: {date_dir}", file=sys.stderr)
        return 2

    errors: list[str] = []
    found_any = False
    for market, filename in (("US", "us_opportunity_ranker.json"), ("CN", "cn_opportunity_ranker.json")):
        payload = _load(date_dir / filename)
        if payload is None:
            errors.append(f"{market}: missing {filename}")
            continue
        found_any = True
        errors.extend(_check_payload(market, payload))

    if not found_any:
        print("audit: no ranker payloads found", file=sys.stderr)
        return 2

    if errors:
        print("Production basket AI-universe audit FAILED:")
        for line in errors:
            print(f"  - {line}")
        return 1

    print(f"Production basket AI-universe audit OK for {args.as_of}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
