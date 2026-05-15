"""Derive the production AI infra universe from the research JSONL.

The research universe (`ai_infra/data/global_universe_v2.jsonl`) holds every
BFS candidate, including 待原文核验 / 证据不足 rows that should not be
executed. The production universe is the subset whose `evidence_state`
contains 原文已证明 or 合理推论 — i.e. names that have cleared the G0-G2
review gate per `company-financials-market-options-methodology.md`.

This script writes the derivative to
`ai_infra/reports/production_universe_v1.jsonl` (and a Markdown summary) so the
operator can see exactly which names are eligible for the production basket.
It does NOT mutate the research universe.

Wire into cron after `source_review_readiness` so promotion lands first.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AI_INFRA_ROOT = STACK_ROOT / "ai_infra"
DEFAULT_OUT_JSONL = DEFAULT_AI_INFRA_ROOT / "reports" / "production_universe_v1.jsonl"
DEFAULT_OUT_MD = DEFAULT_AI_INFRA_ROOT / "reports" / "production_universe_v1.md"

SRC_PATH = STACK_ROOT / "quant-research-v1" / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from quant_bot.analytics import ai_infra_universe as gate  # type: ignore  # noqa: E402


def _render_summary(rows: list[dict], as_of: str) -> str:
    by_market: dict[str, int] = {}
    by_depth: dict[str, int] = {}
    by_pool: dict[str, int] = {}
    for row in rows:
        m = row.get("market_country") or "UNK"
        by_market[m] = by_market.get(m, 0) + 1
        d = row.get("bfs_depth") or "UNK"
        by_depth[d] = by_depth.get(d, 0) + 1
        p = row.get("current_pool") or "UNK"
        by_pool[p] = by_pool.get(p, 0) + 1
    lines = [
        f"# Production AI Infra Universe — {as_of}",
        "",
        f"Total production-grade rows: **{len(rows)}**",
        "",
        "Filter: `evidence_state` contains `原文已证明` or `合理推论`. Other",
        "names stay in the research universe (radar-only) until they clear",
        "the G0-G2 gate.",
        "",
        "## By market",
        "",
    ]
    for m, n in sorted(by_market.items()):
        lines.append(f"- {m}: {n}")
    lines.extend(["", "## By BFS depth", ""])
    for d, n in sorted(by_depth.items()):
        lines.append(f"- {d}: {n}")
    lines.extend(["", "## By current pool", ""])
    for p, n in sorted(by_pool.items()):
        lines.append(f"- {p}: {n}")
    lines.extend(["", "## Tickers (alphabetical)", ""])
    tickers = sorted({str(r.get("ticker") or "") for r in rows if r.get("ticker")})
    for t in tickers:
        lines.append(f"- {t}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ai-infra-root", type=Path, default=DEFAULT_AI_INFRA_ROOT)
    parser.add_argument("--out-jsonl", type=Path, default=DEFAULT_OUT_JSONL)
    parser.add_argument("--out-md", type=Path, default=DEFAULT_OUT_MD)
    parser.add_argument(
        "--as-of",
        default=date.today().isoformat(),
        help="Tag the markdown summary with this date.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary to stdout; do not touch files.",
    )
    args = parser.parse_args()

    all_records = gate.load_records(args.ai_infra_root)
    production_rows = [
        r
        for r in all_records
        if not gate.is_excluded_record(r) and gate.is_production_grade(r)
    ]

    summary = _render_summary(production_rows, args.as_of)
    if args.dry_run:
        print(summary)
        print(f"(dry-run) would write {len(production_rows)} rows to {args.out_jsonl}")
        return 0

    args.out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with args.out_jsonl.open("w", encoding="utf-8") as fh:
        for row in production_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    args.out_md.write_text(summary, encoding="utf-8")
    print(
        f"production universe: wrote {len(production_rows)} rows to "
        f"{args.out_jsonl} (research total = {len(all_records)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
