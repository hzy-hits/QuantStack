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


def _check_payload(market: str, payload: dict, *, strict: bool = False) -> list[str]:
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

    if strict:
        all_rows = payload.get("all_rows") or []
        if not isinstance(all_rows, list):
            errors.append(f"{market}: all_rows is not a list ({type(all_rows).__name__})")
        else:
            for row in all_rows:
                symbol = row.get("symbol") or "<unknown>"
                if not row.get("ai_infra_universe"):
                    errors.append(
                        f"{market}: {symbol} in all_rows (watch/research-only) bypassed the AI universe gate"
                    )
    return errors


def _basket_coverage(payload: dict) -> dict[str, dict[str, int]]:
    """Summarize the basket by asset_pool tag and BFS depth.

    The methodology emphasises core/satellite/candidate buckets; this matrix
    makes the mix observable to the operator without hand-reading the JSON.
    """
    basket = payload.get("production_basket") or []
    all_rows = payload.get("all_rows") or []
    by_pool: dict[str, int] = {}
    by_depth: dict[str, int] = {}
    for row in basket:
        pool = row.get("ai_infra_current_pool") or row.get("ai_infra_asset_pool") or "unknown"
        by_pool[pool] = by_pool.get(pool, 0) + 1
        depth = row.get("ai_infra_bfs_depth") or "unknown"
        by_depth[depth] = by_depth.get(depth, 0) + 1
    return {
        "basket_count": {"value": len(basket)},
        "all_rows_count": {"value": len(all_rows)},
        "by_current_pool": by_pool,
        "by_bfs_depth": by_depth,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True, help="Report date, e.g. 2026-05-13")
    parser.add_argument(
        "--dashboard-root",
        type=Path,
        default=DEFAULT_DASHBOARD,
        help="Root of main_strategy_v2 review dashboard outputs.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also require every all_rows entry (watch/research-only) to be ai_infra_universe=True.",
    )
    args = parser.parse_args()

    date_dir = args.dashboard_root / args.as_of
    if not date_dir.exists():
        print(f"audit: dashboard date directory missing: {date_dir}", file=sys.stderr)
        return 2

    errors: list[str] = []
    found_any = False
    coverage: dict[str, dict[str, dict[str, int]]] = {}
    for market, filename in (("US", "us_opportunity_ranker.json"), ("CN", "cn_opportunity_ranker.json")):
        payload = _load(date_dir / filename)
        if payload is None:
            errors.append(f"{market}: missing {filename}")
            continue
        found_any = True
        errors.extend(_check_payload(market, payload, strict=args.strict))
        coverage[market] = _basket_coverage(payload)

    if not found_any:
        print("audit: no ranker payloads found", file=sys.stderr)
        return 2

    if errors:
        print("Production basket AI-universe audit FAILED:")
        for line in errors:
            print(f"  - {line}")
        return 1

    print(f"Production basket AI-universe audit OK for {args.as_of}")
    for market, matrix in coverage.items():
        basket = matrix["basket_count"]["value"]
        all_rows = matrix["all_rows_count"]["value"]
        pools = ", ".join(f"{name}={count}" for name, count in sorted(matrix["by_current_pool"].items()))
        depths = ", ".join(f"{name}={count}" for name, count in sorted(matrix["by_bfs_depth"].items()))
        print(f"  {market}: basket={basket} / all_rows={all_rows}")
        if pools:
            print(f"    pools: {pools}")
        if depths:
            print(f"    depths: {depths}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
