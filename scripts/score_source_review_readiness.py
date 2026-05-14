"""Score source-review readiness against the AI Infra evidence-card gates.

The methodology in `ai_infra/docs/company-financials-market-options-methodology.md`
defines hard gates G0-G4 for promoting a name from `needs_human_source_review`
to a source-confirmed evidence card:

- G0: No primary source listed → cap at `pending_original_source_verification`.
- G1: AI narrative only, no revenue/order/customer/product evidence → not core pool.
- Promotion requires: evidence_state contains `原文已证明`, primary sources
  enumerated, metrics_to_verify enumerated, counterevidence acknowledged, and
  upgrade conditions written.

This script reads `ai_infra/reports/source_verification_queue_v1.csv`, scores
each row, and writes a readiness ledger to
`ai_infra/reports/source_review_readiness_v1.{csv,md}`.

The script never mutates the source queue. It produces a derivative ledger
that downstream tools (review packet, dashboards, daily report) can consume.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_source_review_readiness"


REQUIRED_FIELDS = (
    "primary_sources_to_find",
    "metrics_to_verify",
    "upgrade_conditions",
    "downgrade_conditions",
    "evidence_state",
    "counterevidence",
)


READINESS_TIERS = (
    "ready_for_promotion",
    "evidence_partial",
    "pending_human_review",
    "blocked_by_counterevidence",
    "g0_blocked",
    "unscored",
)


@dataclass(frozen=True)
class ReadinessRow:
    rank: int | None
    ticker: str
    company: str
    asset_pool: str
    market_country: str
    bfs_depth: str
    priority_tier: str
    module: str
    verification_status: str
    evidence_signals: str
    evidence_score: float
    readiness_tier: str
    missing_fields: tuple[str, ...]
    evidence_state: str
    counterevidence: str

    def as_dict(self) -> dict[str, object]:
        return {
            "rank": self.rank if self.rank is not None else "",
            "ticker": self.ticker,
            "company": self.company,
            "asset_pool": self.asset_pool,
            "market_country": self.market_country,
            "bfs_depth": self.bfs_depth,
            "priority_tier": self.priority_tier,
            "module": self.module,
            "verification_status": self.verification_status,
            "evidence_signals": self.evidence_signals,
            "evidence_score": f"{self.evidence_score:.3f}",
            "readiness_tier": self.readiness_tier,
            "missing_fields": ";".join(self.missing_fields),
            "evidence_state": self.evidence_state,
            "counterevidence": self.counterevidence,
        }


def _field_filled(value: str | None) -> bool:
    if not value:
        return False
    stripped = value.strip()
    if not stripped:
        return False
    return stripped not in {"-", "—", "待核验", "TBD", "tbd", "TODO", "todo", "?"}


def _split_counterevidence(value: str) -> list[str]:
    if not value:
        return []
    raw = value.replace("，", ",").replace("；", ",").replace(";", ",")
    return [piece.strip() for piece in raw.split(",") if piece.strip() and piece.strip() not in {"-", "—"}]


def _classify(row: dict[str, str]) -> ReadinessRow:
    evidence_state = (row.get("evidence_state") or "").strip()
    counter = (row.get("counterevidence") or "").strip()
    primary_sources = (row.get("primary_sources_to_find") or "").strip()
    metrics = (row.get("metrics_to_verify") or "").strip()
    upgrade = (row.get("upgrade_conditions") or "").strip()
    downgrade = (row.get("downgrade_conditions") or "").strip()

    missing: list[str] = []
    for field in REQUIRED_FIELDS:
        if not _field_filled(row.get(field)):
            missing.append(field)

    has_primary = _field_filled(primary_sources)
    has_metrics = _field_filled(metrics)
    has_upgrade = _field_filled(upgrade)
    has_downgrade = _field_filled(downgrade)
    has_counter = _field_filled(counter)

    proved = "原文已证明" in evidence_state
    partial = "合理推论" in evidence_state
    pending = "待原文核验" in evidence_state or "待核验" in evidence_state

    counter_items = _split_counterevidence(counter)

    # Weight: primary 30, metrics 15, upgrade 15, downgrade 10, counter 10, evidence_state 20.
    score = 0.0
    score += 0.30 if has_primary else 0.0
    score += 0.15 if has_metrics else 0.0
    score += 0.15 if has_upgrade else 0.0
    score += 0.10 if has_downgrade else 0.0
    score += 0.10 if has_counter else 0.0
    if proved:
        score += 0.20
    elif partial:
        score += 0.10
    elif pending:
        score += 0.05

    # G0: no primary source → cap status and block promotion entirely.
    if not has_primary:
        tier = "g0_blocked"
    elif proved and has_metrics and has_upgrade and has_counter:
        # Promotion requires evidence_state proves something AND all gating fields filled.
        tier = "ready_for_promotion"
    elif len(counter_items) >= 3 and not proved:
        # Multiple unresolved counter-evidence items dominate any partial signal.
        tier = "blocked_by_counterevidence"
    elif partial or proved:
        tier = "evidence_partial"
    elif pending and has_metrics and has_upgrade:
        tier = "pending_human_review"
    else:
        tier = "unscored"

    signals: list[str] = []
    if proved:
        signals.append("evidence_proved")
    if partial:
        signals.append("evidence_partial")
    if pending:
        signals.append("evidence_pending")
    if not has_primary:
        signals.append("no_primary_source")
    if not has_metrics:
        signals.append("no_metrics_to_verify")
    if not has_upgrade:
        signals.append("no_upgrade_conditions")
    if not has_counter:
        signals.append("no_counterevidence")
    if len(counter_items) >= 3:
        signals.append(f"counter_items_{len(counter_items)}")

    rank_value: int | None
    try:
        rank_value = int(row.get("rank") or "")
    except ValueError:
        rank_value = None

    return ReadinessRow(
        rank=rank_value,
        ticker=row.get("ticker") or "",
        company=row.get("company") or "",
        asset_pool=row.get("asset_pool") or "",
        market_country=row.get("market_country") or "",
        bfs_depth=row.get("bfs_depth") or "",
        priority_tier=row.get("priority_tier") or "",
        module=row.get("module") or "",
        verification_status=row.get("verification_status") or "",
        evidence_signals=",".join(signals) if signals else "-",
        evidence_score=round(score, 3),
        readiness_tier=tier,
        missing_fields=tuple(missing),
        evidence_state=evidence_state,
        counterevidence=counter,
    )


def score_queue(queue_path: Path) -> list[ReadinessRow]:
    with queue_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [_classify(row) for row in reader]


def classify_row(row: dict[str, str]) -> ReadinessRow:
    """Public wrapper around `_classify` so other modules can score a single
    queue row without subprocessing the full ledger pipeline."""
    return _classify(row)


def tier_and_score(row: dict[str, str]) -> tuple[str, float]:
    """Convenience helper for callers that only need (readiness_tier, score)."""
    classified = _classify(row)
    return classified.readiness_tier, classified.evidence_score


def _render_markdown(rows: list[ReadinessRow], queue_path: Path) -> str:
    tier_counts = Counter(row.readiness_tier for row in rows)
    pool_counts = Counter(row.asset_pool for row in rows)

    lines: list[str] = [
        "# AI Infra Source Review Readiness Ledger",
        "",
        f"- 数据源: `{queue_path.relative_to(STACK_ROOT) if queue_path.is_absolute() and queue_path.is_relative_to(STACK_ROOT) else queue_path}`",
        f"- 总数: {len(rows)}",
        "",
        "## Tier Summary",
        "",
        "| Tier | Count |",
        "|---|---:|",
    ]
    for tier in READINESS_TIERS:
        lines.append(f"| {tier} | {tier_counts.get(tier, 0)} |")

    lines += [
        "",
        "## Asset Pool Summary",
        "",
        "| Asset Pool | Count |",
        "|---|---:|",
    ]
    for pool, count in sorted(pool_counts.items()):
        lines.append(f"| {pool or '-'} | {count} |")

    for tier in READINESS_TIERS:
        section_rows = [row for row in rows if row.readiness_tier == tier]
        if not section_rows:
            continue
        section_rows.sort(key=lambda r: (r.rank if r.rank is not None else 9_999))
        lines += [
            "",
            f"## {tier} ({len(section_rows)})",
            "",
            "| Rank | Ticker | Company | Pool | Depth | Score | Module | Signals | Missing |",
            "|---:|---|---|---|---|---:|---|---|---|",
        ]
        for row in section_rows[:80]:
            lines.append(
                f"| {row.rank if row.rank is not None else '-'} "
                f"| {row.ticker or '-'} "
                f"| {row.company or '-'} "
                f"| {row.asset_pool or '-'} "
                f"| {row.bfs_depth or '-'} "
                f"| {row.evidence_score:.2f} "
                f"| {row.module or '-'} "
                f"| {row.evidence_signals} "
                f"| {';'.join(row.missing_fields) or '-'} |"
            )
    return "\n".join(lines) + "\n"


def write_outputs(rows: list[ReadinessRow], out_csv: Path, out_md: Path, queue_path: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "rank",
                "ticker",
                "company",
                "asset_pool",
                "market_country",
                "bfs_depth",
                "priority_tier",
                "module",
                "verification_status",
                "evidence_signals",
                "evidence_score",
                "readiness_tier",
                "missing_fields",
                "evidence_state",
                "counterevidence",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())
    out_md.write_text(_render_markdown(rows, queue_path), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument(
        "--as-of",
        default=None,
        help="Date stamp for the output directory (defaults to current date in Asia/Shanghai).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Parent directory; outputs land in <output-root>/<as-of>/.",
    )
    parser.add_argument("--out-csv", type=Path, default=None)
    parser.add_argument("--out-md", type=Path, default=None)
    args = parser.parse_args()

    if not args.queue.exists():
        print(f"error: queue not found at {args.queue}", file=sys.stderr)
        return 2

    if args.as_of is None:
        from datetime import datetime, timezone, timedelta

        cst = datetime.now(timezone(timedelta(hours=8)))
        as_of = cst.date().isoformat()
    else:
        as_of = args.as_of

    out_csv = args.out_csv or (args.output_root / as_of / "source_review_readiness.csv")
    out_md = args.out_md or (args.output_root / as_of / "source_review_readiness.md")

    rows = score_queue(args.queue)
    write_outputs(rows, out_csv, out_md, args.queue)
    tier_counts = Counter(row.readiness_tier for row in rows)
    summary = ", ".join(f"{tier}={tier_counts.get(tier, 0)}" for tier in READINESS_TIERS)
    print(f"Source-review readiness ledger written: {out_md}; {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
