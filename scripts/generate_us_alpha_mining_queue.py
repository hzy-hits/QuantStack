#!/usr/bin/env python3
"""Generate a US alpha-mining queue from the local AI Infra universe."""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from pathlib import Path


DEFAULT_DB = Path("data/ai_infra_universe.sqlite")
DEFAULT_REPORTS_DIR = Path("reports")
DEFAULT_EVIDENCE_DIR = Path("evidence/us_alpha")


FIELDS = [
    "rank",
    "priority",
    "cluster",
    "ticker",
    "company",
    "bfs_depth",
    "module",
    "current_pool",
    "mcap_bucket",
    "total_score",
    "score_bucket",
    "why_it_matters",
    "primary_questions",
    "sources_to_find",
    "upgrade_conditions",
    "downgrade_conditions",
    "existing_evidence_card",
    "evidence_state",
    "counterevidence",
]


CLUSTER_RULES = [
    (
        re.compile(
            r"NeoCloud|AI cloud|SMB cloud|GPU-as-a-Service|AI data center developer|data center developer|AI hosting|powered land|powered data center|HPC transition|GPU cloud",
            re.I,
        ),
        "neocloud_powered_land",
        "NeoCloud and powered land have high upside but must pass credit and utilization tests.",
        "Are contracts take-or-pay? Is utilization high? Do debt, leases, depreciation, and interest stay manageable?",
        "10-K/20-F/10-Q; S-1 if relevant; debt and lease footnotes; customer contracts; power/site disclosures.",
        "Backlog converts to cash revenue, utilization is high, customer quality is clear, and leverage is manageable.",
        "Customer concentration, weak contract terms, GPU residual risk, power delays, debt/interest, or negative FCF dominate.",
    ),
    (
        re.compile(r"optic|laser|CPO|photonic|InP|800G|1\.6T|AEC|SerDes|connectivity|retimer|network", re.I),
        "optics_connectivity",
        "AI clusters need more east-west bandwidth; this bucket tests whether optics/connectivity suppliers capture D2-D3 value.",
        "What share of revenue is datacenter/AI? Are 800G/1.6T/CPO/AEC products qualified and ramping? Is margin rising despite price pressure?",
        "10-K/10-Q; latest earnings release; investor presentation; product qualification pages; customer/hyperscaler references.",
        "Original sources show AI datacenter mix, qualified products, durable customer demand, and margin support.",
        "Revenue is telecom recovery, one-customer ramp, ASP compression, or CPO timing slips.",
    ),
    (
        re.compile(r"thermal|cooling|switchgear|electrical|power", re.I),
        "power_thermal",
        "AI data centers are constrained by power delivery and heat removal; this bucket tests time-to-power bottlenecks.",
        "How much backlog/revenue is data-center related? Are lead times tight? Does gross margin improve with AI demand?",
        "10-K/10-Q; earnings release; backlog/order disclosures; investor presentation; product pages.",
        "Backlog, book-to-bill, data center mix, lead time, and margin all improve from AI/DC demand.",
        "Orders are pulled forward, data centers slip, cooling/power products commoditize, or customer pricing pressure rises.",
    ),
    (
        re.compile(r"test|ATE|probe|socket|inspection|metrology", re.I),
        "test_metrology",
        "AI accelerators, HBM, and advanced packaging raise test, probe, and inspection complexity.",
        "Is growth tied to HBM/GPU/advanced packaging rather than broad semi beta? Are orders and margins improving?",
        "10-K/10-Q; earnings release; investor presentation; product/application pages; customer segment commentary.",
        "Company filings tie revenue/orders to HBM, AI accelerators, advanced packaging, or high-complexity probe/test.",
        "Revenue is broad WFE/test cycle recovery with no AI/HBM mix disclosure, or test time declines.",
    ),
    (
        re.compile(r"memory interface|EDA|IP|custom silicon|ASIC|storage|SSD|NAND|HDD", re.I),
        "ip_storage_eda",
        "AI ASIC, memory bandwidth, and data pipelines can create value in IP, EDA, and storage layers.",
        "Is the revenue connected to AI chips, HBM/CXL/DDR, enterprise AI storage, or just generic tech spend?",
        "10-K/10-Q; earnings release; investor presentation; product roadmap; customer/design-win commentary.",
        "Original sources show AI/HPC design wins, royalties, enterprise AI storage demand, or memory-interface pull-through.",
        "Exposure is generic software/storage/EDA beta, customer ROI is weak, or revenue timing is one-off.",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate US alpha-mining queue and evidence card stubs.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--limit", type=int, default=30)
    return parser.parse_args()


def depth_numbers(depth: str) -> list[int]:
    return [int(value) for value in re.findall(r"D(\d+)", depth)]


def max_depth(depth: str) -> int:
    values = depth_numbers(depth)
    return max(values) if values else 99


def cluster_for(row: dict[str, str]) -> tuple[str, str, str, str, str, str]:
    text = " ".join([row.get("module", ""), row.get("dependency_path", ""), row.get("overseas_bottleneck", "")])
    for pattern, cluster, why, questions, sources, upgrade, downgrade in CLUSTER_RULES:
        if pattern.search(text):
            return cluster, why, questions, sources, upgrade, downgrade
    return (
        "other_watchlist",
        "This remains in the US watchlist but needs clearer dependency evidence before it can become alpha work.",
        "What direct D1-D3 dependency does original-source evidence prove?",
        "10-K/10-Q; earnings release; investor presentation; official product/customer pages.",
        "Original sources prove direct AI Infra dependency and visible financial translation.",
        "Dependency remains a narrative mapping or counterevidence dominates.",
    )


def priority_for(row: dict[str, str]) -> str:
    score = int(row["total_score"])
    high = max_depth(row["bfs_depth"])
    pool = row["current_pool"]
    mcap = row["mcap_bucket"]
    if "排除" in pool:
        return "P4_excluded"
    if "Mega" not in mcap and high <= 3 and score >= 93 and "候选" in pool:
        return "P0_us_alpha"
    if "Mega" not in mcap and high <= 4 and score >= 75:
        return "P1_verify"
    if high <= 4 and score >= 75:
        return "P2_large_cap_context"
    return "P3_radar"


def existing_card(ticker: str) -> str:
    matches = sorted(Path("evidence").glob(f"**/*{ticker}*.md"))
    for match in matches:
        if match.name == "README.md":
            continue
        return str(match)
    return ""


def load_rows(db_path: Path) -> list[dict[str, str]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              c.ticker, c.company, c.market_country, c.asset_pool, c.mcap_bucket,
              c.bfs_depth, c.module, c.current_pool,
              s.total_score, s.score_bucket,
              r.evidence_state, r.counterevidence,
              d.dependency_path, d.dependency_edge, d.overseas_bottleneck
            FROM companies c
            JOIN scores s USING(ticker)
            JOIN research_signals r USING(ticker)
            JOIN dependency_edges d USING(ticker)
            WHERE c.asset_pool = '美国资产池'
              AND c.current_pool NOT LIKE '%排除%'
            ORDER BY s.total_score DESC, c.ticker
            """
        )
    ]
    conn.close()
    return rows


def enrich_rows(rows: list[dict[str, str]], limit: int) -> list[dict[str, str]]:
    enriched = []
    for row in rows:
        priority = priority_for(row)
        if priority == "P3_radar":
            continue
        cluster, why, questions, sources, upgrade, downgrade = cluster_for(row)
        row = dict(row)
        row.update(
            {
                "priority": priority,
                "cluster": cluster,
                "why_it_matters": why,
                "primary_questions": questions,
                "sources_to_find": sources,
                "upgrade_conditions": upgrade,
                "downgrade_conditions": downgrade,
                "existing_evidence_card": existing_card(row["ticker"]),
            }
        )
        enriched.append(row)
    priority_order = {"P0_us_alpha": 0, "P1_verify": 1, "P2_large_cap_context": 2, "P4_excluded": 4}
    enriched.sort(key=lambda r: (priority_order.get(r["priority"], 9), -int(r["total_score"]), r["ticker"]))
    for idx, row in enumerate(enriched[:limit], start=1):
        row["rank"] = str(idx)
    return enriched[:limit]


def write_csv(rows: list[dict[str, str]], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def write_md(rows: list[dict[str, str]], path: Path) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["priority"]] = counts.get(row["priority"], 0) + 1

    lines = [
        "# US Alpha Mining Queue v1",
        "",
        "状态：executable mining queue, pending original-source verification",
        "边界：这是研究优先级队列，不是买入清单、投资建议、目标价或仓位建议。",
        "",
        "## 使用方式",
        "",
        "先做 `P0_us_alpha`，每家公司只回答一个问题：原文是否证明它能从 D1-D3 AI Infra 瓶颈中拿到收入、毛利或现金流？",
        "",
        "## Priority Counts",
        "",
        "| priority | count |",
        "| --- | ---: |",
    ]
    for priority in ["P0_us_alpha", "P1_verify", "P2_large_cap_context"]:
        lines.append(f"| {priority} | {counts.get(priority, 0)} |")

    lines.extend(
        [
            "",
            "## Queue",
            "",
            "| rank | priority | cluster | ticker | company | BFS | module | score | card |",
            "| ---: | --- | --- | --- | --- | --- | --- | ---: | --- |",
        ]
    )
    for row in rows:
        card = row.get("existing_evidence_card") or ""
        lines.append(
            f"| {row['rank']} | {row['priority']} | {row['cluster']} | {row['ticker']} | {row['company']} | "
            f"{row['bfs_depth']} | {row['module']} | {row['total_score']} | {card} |"
        )

    lines.extend(["", "## Cluster Playbooks", ""])
    seen = set()
    for row in rows:
        if row["cluster"] in seen:
            continue
        seen.add(row["cluster"])
        lines.extend(
            [
                f"### {row['cluster']}",
                "",
                f"- Why: {row['why_it_matters']}",
                f"- Questions: {row['primary_questions']}",
                f"- Sources: {row['sources_to_find']}",
                f"- Upgrade: {row['upgrade_conditions']}",
                f"- Downgrade: {row['downgrade_conditions']}",
                "",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def card_body(row: dict[str, str]) -> str:
    return f"""# {row['ticker']} {row['company']} US alpha evidence card

状态：draft evidence card, pending original-source verification
边界：研究线索，不是投资建议、买卖建议、目标价或仓位建议。

## 基本信息

| 字段 | 内容 |
| --- | --- |
| ticker | {row['ticker']} |
| company | {row['company']} |
| BFS depth | {row['bfs_depth']} |
| module | {row['module']} |
| cluster | {row['cluster']} |
| current pool | {row['current_pool']} |
| total score | {row['total_score']} |

## 为什么挖

{row['why_it_matters']}

## 第一轮要回答的问题

{row['primary_questions']}

## 必找原文

{row['sources_to_find']}

## 原文证据

| 指标 | 原文位置 | 原文已证明什么 | 不能证明什么 | 备注 |
| --- | --- | --- | --- | --- |
| Revenue / segment revenue |  |  |  |  |
| AI/datacenter mix |  |  |  |  |
| Orders / backlog / customer demand |  |  |  |  |
| Gross margin / operating margin |  |  |  |  |
| Customer concentration |  |  |  |  |
| CapEx / inventory / cash flow |  |  |  |  |

## 升级 / 降级条件

| 方向 | 条件 |
| --- | --- |
| 升级 | {row['upgrade_conditions']} |
| 降级 | {row['downgrade_conditions']} |

## 当前反证

{row['counterevidence']}

## 结论分层

| 层级 | 内容 |
| --- | --- |
| 原文已证明 |  |
| 合理推论 |  |
| 待原文核验 | {row['evidence_state']} |
| 主要反证 | {row['counterevidence']} |
| 当前动作 | 先补原文，不做组合动作 |
"""


def write_cards(rows: list[dict[str, str]], evidence_dir: Path) -> None:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    readme_lines = [
        "# US alpha evidence cards",
        "",
        "这里放美股 D2-D3 / D3 alpha sleeve 的 evidence card。已有 batch1 卡片的不重复生成，优先沿用原卡。",
        "",
        "| ticker | company | priority | cluster | card |",
        "| --- | --- | --- | --- | --- |",
    ]

    for row in rows:
        if row["priority"] not in {"P0_us_alpha", "P1_verify"}:
            continue
        card = row.get("existing_evidence_card")
        if not card:
            safe_ticker = re.sub(r"[^A-Za-z0-9._-]+", "-", row["ticker"])
            safe_company = re.sub(r"[^A-Za-z0-9._-]+", "-", row["company"]).strip("-")
            path = evidence_dir / f"{row['rank'].zfill(3)}-{safe_ticker}-{safe_company}.md"
            if not path.exists():
                path.write_text(card_body(row), encoding="utf-8")
            card = str(path)
        readme_lines.append(
            f"| {row['ticker']} | {row['company']} | {row['priority']} | {row['cluster']} | {card} |"
        )

    (evidence_dir / "README.md").write_text("\n".join(readme_lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    args.reports_dir.mkdir(parents=True, exist_ok=True)
    rows = enrich_rows(load_rows(args.db), args.limit)
    write_csv(rows, args.reports_dir / "us_alpha_mining_queue_v1.csv")
    write_md(rows, args.reports_dir / "us_alpha_mining_queue_v1.md")
    write_cards(rows, args.evidence_dir)
    print(f"Generated {len(rows)} US alpha mining rows")
    print(args.reports_dir / "us_alpha_mining_queue_v1.csv")
    print(args.reports_dir / "us_alpha_mining_queue_v1.md")
    print(args.evidence_dir)


if __name__ == "__main__":
    main()
