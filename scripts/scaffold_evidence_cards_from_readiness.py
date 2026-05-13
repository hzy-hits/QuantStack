"""Auto-draft AI Infra evidence cards from the readiness ledger.

This closes one step of the manual workflow described in
`ai_infra/docs/research-checklist.md` and `source-evidence-template.md`.

For every row in the readiness ledger that is `ready_for_promotion` or
`evidence_partial`, we render a pre-filled Markdown evidence card with:

- Basic info (company, ticker, BFS depth, module, dependency path).
- A source checklist seeded from `primary_sources_to_find`.
- A blank but typed evidence table aligned to the queue's
  `metrics_to_verify`.
- A conclusion-layer scaffold (原文已证明 / 合理推论 / 待原文核验 / 主要反证).
- Refutation block populated from the queue's counterevidence.
- Next-step bullets from `upgrade_conditions` and `downgrade_conditions`.

This is a *draft*, not a finished evidence card. Operators still need to fill
in the original-source quotes and adjust the conclusion layer before the row
graduates to production.

Outputs land under
`reports/review_dashboard/ai_infra_evidence_card_drafts/<date>/<ticker>.md`
plus a single `INDEX.md` listing the generated drafts.
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_READINESS_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_source_review_readiness"
DEFAULT_QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"
DEFAULT_DRAFT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_infra_evidence_card_drafts"

ELIGIBLE_TIERS = {"ready_for_promotion", "evidence_partial"}

# Evidence rows align to source-evidence-template.md. We always keep the same
# anchors so reviewers see a stable schema even when metrics_to_verify is sparse.
EVIDENCE_ANCHORS = (
    "Revenue / segment revenue",
    "Gross margin / operating margin",
    "CapEx / inventory / FCF",
    "Backlog / RPO / orders",
    "ASP / shipment / capacity",
    "Customer / product evidence",
)


@dataclass(frozen=True)
class DraftSpec:
    primary_ticker: str
    ticker_field: str
    company: str
    asset_pool: str
    market_country: str
    bfs_depth: str
    module: str
    readiness_tier: str
    readiness_score: float
    evidence_state: str
    counterevidence: str
    dependency_path: str
    primary_sources_to_find: str
    metrics_to_verify: str
    upgrade_conditions: str
    downgrade_conditions: str
    rank: int | None
    priority_tier: str


def _primary_ticker(field: str) -> str:
    aliases = [piece.strip() for piece in (field or "").split("/") if piece.strip()]
    if not aliases:
        return field.strip()
    for alias in aliases:
        if alias.isupper() and "." not in alias and len(alias) <= 5:
            return alias
    return aliases[0]


def _safe_filename(ticker: str) -> str:
    keep = "_-."
    return "".join(ch if ch.isalnum() or ch in keep else "_" for ch in ticker)


def _split_items(raw: str) -> list[str]:
    if not raw:
        return []
    normalized = raw.replace("，", ",").replace("；", ",").replace(";", ",").replace("、", ",")
    return [piece.strip() for piece in normalized.split(",") if piece.strip() and piece.strip() not in {"-", "—"}]


def load_readiness(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_queue(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    by_key: dict[tuple[str, str], dict[str, str]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            by_key[(row.get("rank") or "", row.get("ticker") or "")] = row
    return by_key


def collect_specs(readiness_rows: list[dict[str, str]], queue: dict[tuple[str, str], dict[str, str]]) -> list[DraftSpec]:
    specs: list[DraftSpec] = []
    for row in readiness_rows:
        if row.get("readiness_tier") not in ELIGIBLE_TIERS:
            continue
        rank_value = row.get("rank") or ""
        ticker_field = row.get("ticker") or ""
        queue_row = queue.get((rank_value, ticker_field)) or {}
        try:
            rank_int: int | None = int(rank_value) if rank_value else None
        except ValueError:
            rank_int = None
        try:
            readiness_score = float(row.get("evidence_score") or 0.0)
        except ValueError:
            readiness_score = 0.0
        specs.append(
            DraftSpec(
                primary_ticker=_primary_ticker(ticker_field),
                ticker_field=ticker_field,
                company=row.get("company") or "",
                asset_pool=row.get("asset_pool") or "",
                market_country=row.get("market_country") or "",
                bfs_depth=row.get("bfs_depth") or "",
                module=row.get("module") or "",
                readiness_tier=row.get("readiness_tier") or "",
                readiness_score=readiness_score,
                evidence_state=row.get("evidence_state") or "",
                counterevidence=row.get("counterevidence") or "",
                dependency_path=queue_row.get("dependency_path") or "",
                primary_sources_to_find=queue_row.get("primary_sources_to_find") or "",
                metrics_to_verify=queue_row.get("metrics_to_verify") or "",
                upgrade_conditions=queue_row.get("upgrade_conditions") or "",
                downgrade_conditions=queue_row.get("downgrade_conditions") or "",
                rank=rank_int,
                priority_tier=queue_row.get("priority_tier") or row.get("priority_tier") or "",
            )
        )
    return specs


def render_card(spec: DraftSpec, as_of: str) -> str:
    sources = _split_items(spec.primary_sources_to_find)
    metrics = _split_items(spec.metrics_to_verify)
    counters = _split_items(spec.counterevidence)
    upgrades = _split_items(spec.upgrade_conditions)
    downgrades = _split_items(spec.downgrade_conditions)

    lines: list[str] = [
        f"# Evidence Card Draft — {spec.company} ({spec.primary_ticker})",
        "",
        f"- Generated: {as_of}",
        f"- Readiness tier: `{spec.readiness_tier}` (score {spec.readiness_score:.2f})",
        f"- Source queue rank: {spec.rank if spec.rank is not None else '-'} | priority: {spec.priority_tier or '-'}",
        "- 状态: **草稿**。需要填入原文 quote、链接、口径，再提交人工 review。",
        "",
        "## 基本信息",
        "",
        "| 字段 | 内容 |",
        "| --- | --- |",
        f"| 研究主题 | AI Infra BFS — {spec.module or '-'} |",
        f"| 公司 / 证券代码 | {spec.company} / {spec.ticker_field} |",
        f"| 资产池 / 区域 | {spec.asset_pool or '-'} / {spec.market_country or '-'} |",
        f"| 产业链位置 | {spec.bfs_depth or '-'} — {spec.module or '-'} |",
        "| 报告期 | _待填: 最新季报 / 年报_ |",
        "| 原始来源类型 | 年报 / 季报 / earnings release / earnings call / investor presentation / 监管公告 / 技术资料 |",
        "| 原始来源链接 | _待填: 直接贴 SEC EDGAR / 公司 IR / 交易所 URL_ |",
        "| 发布日期 | _待填_ |",
        "",
    ]

    if spec.dependency_path:
        lines += [
            "## BFS 依赖路径",
            "",
            f"- {spec.dependency_path}",
            "",
        ]

    lines += [
        "## 原文 Source Checklist",
        "",
        f"参考队列原文要求：{spec.primary_sources_to_find or '_待补_'}",
        "",
    ]
    if sources:
        for source in sources:
            lines.append(f"- [ ] {source}")
    else:
        lines += [
            "- [ ] 10-K / 20-F",
            "- [ ] 10-Q / quarterly results",
            "- [ ] Earnings call transcript",
            "- [ ] Investor presentation",
            "- [ ] Product / capacity page",
        ]
    lines.append("")

    lines += [
        "## 原文证据",
        "",
        "| 指标 | 原文位置 | 原文能证明什么 | 不能证明什么 | 口径备注 |",
        "| --- | --- | --- | --- | --- |",
    ]
    seen_anchors: set[str] = set()
    for anchor in EVIDENCE_ANCHORS:
        seen_anchors.add(anchor)
        lines.append(f"| {anchor} |  |  |  |  |")
    # Add any metrics_to_verify that don't map onto the anchor labels — they
    # are usually company-specific (e.g. "HBM revenue mix").
    for metric in metrics:
        if metric in seen_anchors:
            continue
        lines.append(f"| {metric} |  |  |  |  |")
    lines.append("")

    lines += [
        "## 结论分层",
        "",
        "| 层级 | 内容 |",
        "| --- | --- |",
    ]
    if "原文已证明" in spec.evidence_state:
        lines.append(f"| 原文已证明 | {spec.evidence_state} |")
    else:
        lines.append("| 原文已证明 | _待填_ |")
    if "合理推论" in spec.evidence_state:
        lines.append(f"| 合理推论 | {spec.evidence_state} |")
    else:
        lines.append("| 合理推论 | _待填_ |")
    if "待原文核验" in spec.evidence_state or "待核验" in spec.evidence_state:
        lines.append(f"| 待原文核验 | {spec.evidence_state} |")
    else:
        lines.append("| 待原文核验 | _待填_ |")
    if counters:
        lines.append(f"| 主要反证 | {'; '.join(counters)} |")
    else:
        lines.append("| 主要反证 | _待填_ |")
    lines.append("")

    lines += [
        "## 反证与下调条件",
        "",
        f"- counterevidence 队列字段: {spec.counterevidence or '_待补_'}",
    ]
    if downgrades:
        for item in downgrades:
            lines.append(f"- downgrade trigger: {item}")
    lines.append("")

    lines += [
        "## 升级条件 (promotion gate)",
        "",
    ]
    if upgrades:
        for item in upgrades:
            lines.append(f"- {item}")
    else:
        lines += [
            "- 原文证据覆盖 revenue / margin / customer / capacity 至少三项。",
            "- evidence_state 升级为 `原文已证明`。",
        ]
    lines.append("")

    lines += [
        "## 研究判断 (评分 1-5)",
        "",
        "| 维度 | 评分 | 依据 |",
        "| --- | --- | --- |",
        "| AI 需求相关度 |  |  |",
        "| 供给瓶颈 |  |  |",
        "| 议价权 |  |  |",
        "| 持续性 |  |  |",
        "| 财务传导 |  |  |",
        "| 技术护城河 |  |  |",
        "| 估值空间 |  |  |",
        "| 反证清晰度 |  |  |",
        "",
        "## 下一步核验",
        "",
        "1. _待补: 找最新季报里 AI/data center segment 拆分。_",
        "2. _待补: 找客户披露或交叉验证。_",
        "3. _待补: 写出 promotion / watch / reject 判断。_",
        "",
    ]
    return "\n".join(lines) + "\n"


def render_index(specs: list[DraftSpec], as_of: str) -> str:
    lines: list[str] = [
        f"# Evidence Card Draft Index — {as_of}",
        "",
        f"- Total drafts: {len(specs)}",
        "- 入口口径: readiness_tier ∈ {ready_for_promotion, evidence_partial}",
        "- 每张草稿是 *起点*；操作员需要补原文证据再做 promote/reject 决定。",
        "",
        "| Ticker | Company | Pool | Depth | Tier | Score | Module |",
        "|---|---|---|---|---|---:|---|",
    ]
    for spec in sorted(specs, key=lambda s: (s.rank if s.rank is not None else 9_999)):
        lines.append(
            "| "
            + " | ".join(
                [
                    spec.primary_ticker or "-",
                    spec.company or "-",
                    spec.asset_pool or "-",
                    spec.bfs_depth or "-",
                    spec.readiness_tier,
                    f"{spec.readiness_score:.2f}",
                    spec.module or "-",
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_drafts(specs: list[DraftSpec], out_dir: Path, as_of: str) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for spec in specs:
        filename = f"{_safe_filename(spec.primary_ticker)}.md"
        path = out_dir / filename
        path.write_text(render_card(spec, as_of), encoding="utf-8")
        written.append(path)
    (out_dir / "INDEX.md").write_text(render_index(specs, as_of), encoding="utf-8")
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument(
        "--readiness-csv",
        type=Path,
        default=None,
        help="Direct path to source_review_readiness.csv. If omitted, looks under the readiness root.",
    )
    parser.add_argument(
        "--readiness-root",
        type=Path,
        default=DEFAULT_READINESS_ROOT,
        help="Parent of <date>/source_review_readiness.csv outputs.",
    )
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DRAFT_ROOT)
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()

    if args.readiness_csv is not None:
        readiness_path = args.readiness_csv
    else:
        readiness_path = args.readiness_root / as_of / "source_review_readiness.csv"
    if not readiness_path.exists():
        print(f"error: readiness ledger not found at {readiness_path}", file=sys.stderr)
        return 2
    if not args.queue.exists():
        print(f"error: queue not found at {args.queue}", file=sys.stderr)
        return 2

    readiness_rows = load_readiness(readiness_path)
    queue = load_queue(args.queue)
    specs = collect_specs(readiness_rows, queue)
    if not specs:
        print("No ready_for_promotion or evidence_partial rows found; nothing to draft.")
        return 0
    out_dir = args.output_root / as_of
    written = write_drafts(specs, out_dir, as_of)
    print(f"Evidence card drafts written: {len(written)} files under {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
