"""Derive a promotion plan from the source-review readiness ledger.

This closes the loop methodology asks for:

    source-review queue → readiness gates → promotion recommendation

It does **not** mutate `global_universe_v2.jsonl` or
`expansion_candidates_promoted_v1.csv`. The output is a human-reviewable plan
that bundles the readiness verdict with the queue's primary-source checklist
and counterevidence, so a researcher can sign off before any universe change.

Recommendations map readiness tiers to actions:

| Readiness tier             | Recommendation       |
|----------------------------|----------------------|
| ready_for_promotion        | promote_now          |
| evidence_partial           | watch_with_review    |
| pending_human_review       | research_only        |
| blocked_by_counterevidence | reject_until_resolved|
| g0_blocked                 | gate_g0_no_promotion |
| unscored                   | needs_template_fill  |

Outputs are written to
`reports/review_dashboard/ai_infra_promotion_plan/<date>/promotion_plan.{csv,md}`.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_READINESS_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_source_review_readiness"
DEFAULT_QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_promotion_plan"


RECOMMENDATION_BY_TIER = {
    "ready_for_promotion": "promote_now",
    "evidence_partial": "watch_with_review",
    "pending_human_review": "research_only",
    "blocked_by_counterevidence": "reject_until_resolved",
    "g0_blocked": "gate_g0_no_promotion",
    "unscored": "needs_template_fill",
}

RECOMMENDATION_ORDER = (
    "promote_now",
    "watch_with_review",
    "research_only",
    "reject_until_resolved",
    "gate_g0_no_promotion",
    "needs_template_fill",
)


@dataclass(frozen=True)
class PromotionLine:
    rank: int | None
    primary_ticker: str
    ticker_field: str
    company: str
    asset_pool: str
    market_country: str
    bfs_depth: str
    module: str
    priority_tier: str
    readiness_tier: str
    readiness_score: float
    recommendation: str
    rationale: str
    primary_sources_to_find: str
    upgrade_conditions: str
    counterevidence: str

    def as_dict(self) -> dict[str, str]:
        return {
            "rank": str(self.rank) if self.rank is not None else "",
            "primary_ticker": self.primary_ticker,
            "ticker_field": self.ticker_field,
            "company": self.company,
            "asset_pool": self.asset_pool,
            "market_country": self.market_country,
            "bfs_depth": self.bfs_depth,
            "module": self.module,
            "priority_tier": self.priority_tier,
            "readiness_tier": self.readiness_tier,
            "readiness_score": f"{self.readiness_score:.3f}",
            "recommendation": self.recommendation,
            "rationale": self.rationale,
            "primary_sources_to_find": self.primary_sources_to_find,
            "upgrade_conditions": self.upgrade_conditions,
            "counterevidence": self.counterevidence,
        }


def _primary_ticker(field: str) -> str:
    aliases = [piece.strip() for piece in (field or "").split("/") if piece.strip()]
    if not aliases:
        return field.strip()
    for alias in aliases:
        if alias.isupper() and "." not in alias and len(alias) <= 5:
            return alias
    return aliases[0]


def _rationale_for(tier: str, evidence_state: str, counterevidence: str) -> str:
    if tier == "ready_for_promotion":
        return f"evidence_state contains 原文已证明: {evidence_state[:120]}"
    if tier == "evidence_partial":
        return f"partial evidence (合理推论) — watch until 原文已证明: {evidence_state[:120]}"
    if tier == "pending_human_review":
        return "template filled but evidence_state still pending; needs primary-source pull"
    if tier == "blocked_by_counterevidence":
        return f"≥3 unresolved counter-evidence items: {counterevidence[:160]}"
    if tier == "g0_blocked":
        return "G0 gate: no primary_sources_to_find listed"
    return "scorer could not classify; queue row missing fields"


def build_plan(readiness_csv: Path, queue_csv: Path) -> list[PromotionLine]:
    with readiness_csv.open("r", encoding="utf-8") as handle:
        readiness_rows = list(csv.DictReader(handle))
    with queue_csv.open("r", encoding="utf-8") as handle:
        queue_rows = {row.get("rank") or "": row for row in csv.DictReader(handle)}

    plan: list[PromotionLine] = []
    for row in readiness_rows:
        tier = row.get("readiness_tier") or "unscored"
        recommendation = RECOMMENDATION_BY_TIER.get(tier, "needs_template_fill")
        try:
            rank_int: int | None = int(row.get("rank") or "")
        except ValueError:
            rank_int = None
        try:
            score = float(row.get("evidence_score") or 0.0)
        except ValueError:
            score = 0.0
        queue_row = queue_rows.get(row.get("rank") or "") or {}
        ticker_field = row.get("ticker") or ""
        plan.append(
            PromotionLine(
                rank=rank_int,
                primary_ticker=_primary_ticker(ticker_field),
                ticker_field=ticker_field,
                company=row.get("company") or "",
                asset_pool=row.get("asset_pool") or "",
                market_country=row.get("market_country") or "",
                bfs_depth=row.get("bfs_depth") or "",
                module=row.get("module") or queue_row.get("module") or "",
                priority_tier=queue_row.get("priority_tier") or row.get("priority_tier") or "",
                readiness_tier=tier,
                readiness_score=score,
                recommendation=recommendation,
                rationale=_rationale_for(tier, row.get("evidence_state") or "", row.get("counterevidence") or ""),
                primary_sources_to_find=queue_row.get("primary_sources_to_find") or "",
                upgrade_conditions=queue_row.get("upgrade_conditions") or "",
                counterevidence=row.get("counterevidence") or "",
            )
        )
    plan.sort(
        key=lambda p: (
            RECOMMENDATION_ORDER.index(p.recommendation) if p.recommendation in RECOMMENDATION_ORDER else 99,
            p.rank if p.rank is not None else 9_999,
        )
    )
    return plan


def render_markdown(plan: list[PromotionLine], as_of: str) -> str:
    counts = Counter(line.recommendation for line in plan)
    lines: list[str] = [
        f"# AI Infra Promotion Plan — {as_of}",
        "",
        "- 数据源: readiness ledger + source verification queue。",
        "- 状态: **建议**，不直接写入 `global_universe_v2.jsonl` 或 `expansion_candidates_promoted_v1.csv`。",
        "- 用法: 操作员对照 evidence card 草稿和原文链接，在通过 G0-G4 之后才执行 promote/reject。",
        "",
        "## Recommendation Summary",
        "",
        "| Recommendation | Count |",
        "|---|---:|",
    ]
    for rec in RECOMMENDATION_ORDER:
        lines.append(f"| {rec} | {counts.get(rec, 0)} |")
    lines.append("")

    for rec in RECOMMENDATION_ORDER:
        section = [line for line in plan if line.recommendation == rec]
        if not section:
            continue
        lines += [
            f"## {rec} ({len(section)})",
            "",
            "| Rank | Ticker | Company | Pool | Depth | Tier | Module | Rationale |",
            "|---:|---|---|---|---|---|---|---|",
        ]
        for line in section[:80]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(line.rank) if line.rank is not None else "-",
                        line.primary_ticker or "-",
                        line.company or "-",
                        line.asset_pool or "-",
                        line.bfs_depth or "-",
                        f"{line.readiness_tier} ({line.readiness_score:.2f})",
                        line.module or "-",
                        line.rationale[:140],
                    ]
                )
                + " |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def write_outputs(plan: list[PromotionLine], out_dir: Path, as_of: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "promotion_plan.csv"
    md_path = out_dir / "promotion_plan.md"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(PromotionLine.__annotations__.keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for line in plan:
            writer.writerow(line.as_dict())
    md_path.write_text(render_markdown(plan, as_of), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument(
        "--readiness-root",
        type=Path,
        default=DEFAULT_READINESS_ROOT,
    )
    parser.add_argument("--readiness-csv", type=Path, default=None)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    readiness_csv = args.readiness_csv or (args.readiness_root / as_of / "source_review_readiness.csv")
    if not readiness_csv.exists():
        print(f"error: readiness ledger not found at {readiness_csv}", file=sys.stderr)
        return 2
    if not args.queue.exists():
        print(f"error: queue not found at {args.queue}", file=sys.stderr)
        return 2

    plan = build_plan(readiness_csv, args.queue)
    out_dir = args.output_root / as_of
    write_outputs(plan, out_dir, as_of)
    counts = Counter(line.recommendation for line in plan)
    summary = ", ".join(f"{rec}={counts.get(rec, 0)}" for rec in RECOMMENDATION_ORDER)
    print(f"Promotion plan written: {out_dir / 'promotion_plan.md'}; {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
