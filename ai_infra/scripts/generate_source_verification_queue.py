#!/usr/bin/env python3
"""Generate a source-verification queue from the AI Infra universe database."""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


DEFAULT_DB = Path("data/ai_infra_universe.sqlite")
DEFAULT_REPORTS_DIR = Path("reports")


OUTPUT_FIELDS = [
    "rank",
    "priority_tier",
    "ticker",
    "company",
    "market_country",
    "asset_pool",
    "bfs_depth",
    "module",
    "current_pool",
    "total_score",
    "score_bucket",
    "verification_status",
    "source_priority",
    "primary_sources_to_find",
    "metrics_to_verify",
    "upgrade_conditions",
    "downgrade_conditions",
    "evidence_state",
    "counterevidence",
    "dependency_path",
    "dependency_edge",
    "etf_clue",
    "smart_money_clue",
    "market_context_notes",
]


MODULE_RULES = [
    (
        re.compile(r"power|thermal|cooling|liquid|UPS|PDU|grid|switchgear|transformer", re.I),
        {
            "source": "annual report / quarterly results / backlog/orders / investor presentation / product page",
            "metrics": "data center orders; backlog; book-to-bill; liquid cooling attach; margin; lead time; capacity expansion",
            "upgrade": "Original sources prove data-center AI demand, long lead-time constraints, backlog growth, and margin expansion.",
            "downgrade": "Orders are pulled forward, projects delayed, customer pricing pressure rises, or grid permission is the real bottleneck.",
        },
    ),
    (
        re.compile(r"\bGPU\b|CUDA|\bTPU\b|accelerator", re.I),
        {
            "source": "10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap",
            "metrics": "data center compute revenue; accelerator roadmap; networking/rack-scale systems; supply constraints; gross margin; customer concentration",
            "upgrade": "Original sources prove sustained AI compute demand, platform pull-through, and supply-chain transmission into D2/D3.",
            "downgrade": "Growth slows, supply constraints ease into oversupply, margins compress, or workload shifts weaken the platform moat.",
        },
    ),
    (
        re.compile(r"Custom ASIC|\bASIC\b|\bEDA\b|\bIP\b|interface IP|custom silicon", re.I),
        {
            "source": "10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap",
            "metrics": "data center revenue; custom silicon revenue; design wins; NRE vs mass production; IP royalty; customer concentration",
            "upgrade": "Original sources prove sustained AI compute or ASIC demand and supply-chain pull-through to D2/D3.",
            "downgrade": "Revenue is concentrated, one-off NRE, software moat blocks adoption, or customer disclosures are too thin.",
        },
    ),
    (
        re.compile(r"Optic|laser|CPO|SiPh|photonic|InP|800G|1.6T|AEC|SerDes|connectivity|retimer|network", re.I),
        {
            "source": "10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference",
            "metrics": "datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin",
            "upgrade": "Company filings show AI datacenter/datacom growth, qualified 800G/1.6T or CPO products, and margin durability.",
            "downgrade": "Growth is telecom cycle or one customer ramp, ASP declines dominate, or CPO timing is pushed out.",
        },
    ),
    (
        re.compile(r"\btest(?:er|ing|s)?\b|\bATE\b|probe|socket|inspection|metrology", re.I),
        {
            "source": "annual report / quarterly results / earnings call / investor presentation / product and application pages",
            "metrics": "AI/HBM tester demand; SoC vs memory tester mix; probe/socket orders; inspection/metrology AI packaging exposure; margin",
            "upgrade": "Original sources prove HBM, AI accelerator, or advanced packaging complexity is increasing test or inspection revenue.",
            "downgrade": "Revenue is broad semi-cycle recovery with no AI/HBM/advanced packaging mix or order evidence.",
        },
    ),
    (
        re.compile(r"CoWoS|advanced packaging|Packaging|bonding|molding|Dicing|grinding|thinning|OSAT|substrate|ABF|PCB|CCL", re.I),
        {
            "source": "annual report / quarterly results / presentation / capacity expansion announcement / product page",
            "metrics": "AI/HPC advanced packaging revenue; backlog/orders; capacity; substrate layer/spec upgrade; gross margin",
            "upgrade": "Original sources tie orders/revenue to HBM, CoWoS, AI server, advanced package, ABF, or high-speed PCB demand.",
            "downgrade": "Revenue is mainly consumer/auto/legacy PCB or broad semi beta with no AI/HPC customer evidence.",
        },
    ),
    (
        re.compile(r"server|rack|ODM|OEM", re.I),
        {
            "source": "annual report / quarterly results / customer concentration note / inventory and margin disclosure",
            "metrics": "AI server revenue; rack-scale shipment; inventory; gross margin; customer concentration; liquid-cooled rack mix",
            "upgrade": "AI server/rack revenue and margins are disclosed, backlog converts to revenue, and inventory risk is controlled.",
            "downgrade": "Revenue grows but margin compresses, inventory rises, or business remains low-margin assembly without bottleneck power.",
        },
    ),
    (
        re.compile(r"HBM|DRAM|memory", re.I),
        {
            "source": "annual report / quarterly results / earnings call / investor presentation / product roadmap",
            "metrics": "HBM revenue or mix; capacity plan; ASP/margin; customer qualification; HBM3E/HBM4 roadmap",
            "upgrade": "Company filings prove AI/HBM revenue mix, capacity tightness, improving margin, and multi-customer demand.",
            "downgrade": "HBM exposure is not disclosed, demand is commodity DRAM recovery, or capacity additions point to fast oversupply.",
        },
    ),
    (
        re.compile(r"NeoCloud|cloud|GPU-as-a-Service|data center|IDC", re.I),
        {
            "source": "10-K/20-F / quarterly results / debt filings / customer contracts / power and datacenter disclosures",
            "metrics": "backlog quality; contracted MW; utilization; depreciation; interest expense; customer concentration; FCF",
            "upgrade": "Backlog is take-or-pay or high-quality, utilization is high, cash collection is visible, and leverage is manageable.",
            "downgrade": "Revenue depends on leverage, customer concentration, weak contract terms, GPU residual risk, or negative FCF spiral.",
        },
    ),
    (
        re.compile(r"material|chemical|gas|wafer|vacuum|clean|UPW|SOI|InP|GaAs|SiC", re.I),
        {
            "source": "annual report / segment disclosure / capacity expansion note / technical product page",
            "metrics": "AI/HBM/CoWoS/SiPh material exposure; customer qualification; ASP; utilization; margin; capacity",
            "upgrade": "Material specification upgrade is proven to be required by HBM, CoWoS, CPO, advanced node, or AI power devices.",
            "downgrade": "Exposure is broad semi cycle or consumer/industrial demand with no clear D0-D2 blocking path.",
        },
    ),
]


DEFAULT_RULE = {
    "source": "annual report / quarterly results / earnings call / investor presentation / company product page",
    "metrics": "AI-related revenue; orders/backlog; capacity; gross margin; customer concentration; technical roadmap",
    "upgrade": "Original sources prove direct D1-D3 dependency and visible financial translation.",
    "downgrade": "Dependency remains a narrative mapping, lacks revenue/order evidence, or counterevidence dominates.",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate AI Infra original-source verification queue.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite universe database.")
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR, help="Reports directory.")
    return parser.parse_args()


def depth_numbers(depth: str) -> list[int]:
    return [int(value) for value in re.findall(r"D(\d+)", depth)]


def depth_range(depth: str) -> tuple[int, int]:
    values = depth_numbers(depth)
    if not values:
        return (99, 99)
    return min(values), max(values)


def priority_tier(row: dict[str, str]) -> str:
    low, high = depth_range(row["bfs_depth"])
    score = int(row["total_score"])
    if "排除" in row["current_pool"] or row["score_bucket"] == "exclude":
        return "P4_excluded_or_hold"
    if high <= 3 and score >= 90:
        return "P0_first_batch"
    if high <= 3 and score >= 75:
        return "P1_d1_d3_followup"
    if high <= 4 and score >= 65:
        return "P2_radar_if_blocks_d2"
    if low >= 4:
        return "P3_deep_radar"
    return "P3_low_priority"


def source_priority(row: dict[str, str]) -> str:
    tier = priority_tier(row)
    if tier == "P0_first_batch":
        return "Find latest annual report, latest quarterly results, earnings call transcript, investor presentation, and official product/capacity pages first."
    if tier == "P1_d1_d3_followup":
        return "Find latest annual/quarterly filings and one official source proving the dependency path."
    if tier == "P2_radar_if_blocks_d2":
        return "Verify whether this D3-D4/D4 node can actually block D0-D2 before deeper work."
    if tier == "P4_excluded_or_hold":
        return "Only revisit if universe rules change or original sources contradict exclusion."
    return "Low-priority radar; defer until higher-confidence D1-D3 evidence is processed."


def rule_for(row: dict[str, str]) -> dict[str, str]:
    module_text = row.get("module", "")
    for pattern, rule in MODULE_RULES:
        if pattern.search(module_text):
            return rule

    full_text = " ".join(
        [
            module_text,
            row.get("dependency_path", ""),
            row.get("dependency_edge", ""),
            row.get("overseas_bottleneck", ""),
            row.get("up_downstream", ""),
        ]
    )
    for pattern, rule in MODULE_RULES:
        if pattern.search(full_text):
            return rule
    return DEFAULT_RULE


def load_rows(db_path: Path) -> list[dict[str, str]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              c.ticker,
              c.company,
              c.market_country,
              c.asset_pool,
              c.bfs_depth,
              c.module,
              c.current_pool,
              s.total_score,
              s.score_bucket,
              r.verification_status,
              r.evidence_state,
              r.counterevidence,
              r.etf_clue,
              r.smart_money_clue,
              d.dependency_path,
              d.dependency_edge,
              d.overseas_bottleneck,
              d.up_downstream
            FROM companies c
            JOIN scores s USING (ticker)
            JOIN research_signals r USING (ticker)
            JOIN dependency_edges d USING (ticker)
            ORDER BY s.total_score DESC, c.asset_pool, c.ticker
            """
        )
    ]
    conn.close()
    return rows


def build_queue(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    tier_order = {
        "P0_first_batch": 0,
        "P1_d1_d3_followup": 1,
        "P2_radar_if_blocks_d2": 2,
        "P3_deep_radar": 3,
        "P3_low_priority": 3,
        "P4_excluded_or_hold": 4,
    }
    queue: list[dict[str, str]] = []
    for row in rows:
        rule = rule_for(row)
        item = dict(row)
        item["priority_tier"] = priority_tier(row)
        item["source_priority"] = source_priority(row)
        item["primary_sources_to_find"] = rule["source"]
        item["metrics_to_verify"] = rule["metrics"]
        item["upgrade_conditions"] = rule["upgrade"]
        item["downgrade_conditions"] = rule["downgrade"]
        queue.append(item)
    queue.sort(key=lambda r: (tier_order[r["priority_tier"]], -int(r["total_score"]), r["asset_pool"], r["ticker"]))
    for index, item in enumerate(queue, 1):
        item["rank"] = str(index)
    return queue


def write_csv(path: Path, queue: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(queue)


def select_batch1(queue: list[dict[str, str]], per_asset: int = 8) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    for asset_pool in ["中国资产池", "美国资产池", "卫星资产池"]:
        selected.extend(
            [
                row
                for row in queue
                if row["priority_tier"] == "P0_first_batch" and row["asset_pool"] == asset_pool
            ][:per_asset]
        )
    return selected


def md_escape(value: object, limit: int = 140) -> str:
    text = str(value).replace("\n", " ").replace("|", "\\|")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def md_table(headers: list[str], rows: list[list[object]]) -> str:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(md_escape(value) for value in row) + " |")
    return "\n".join(output)


def company_rows(rows: list[dict[str, str]]) -> list[list[object]]:
    return [
        [
            row["rank"],
            row["ticker"],
            row["company"],
            row["market_country"],
            row["asset_pool"],
            row["bfs_depth"],
            row["module"],
            row["total_score"],
            row["primary_sources_to_find"],
            row["metrics_to_verify"],
        ]
        for row in rows
    ]


def write_markdown(path: Path, queue: list[dict[str, str]]) -> None:
    tiers = defaultdict(list)
    for row in queue:
        tiers[row["priority_tier"]].append(row)
    tier_counts = Counter(row["priority_tier"] for row in queue)
    asset_counts = Counter(row["asset_pool"] for row in queue)
    p0_by_asset = {
        asset: [row for row in tiers["P0_first_batch"] if row["asset_pool"] == asset][:12]
        for asset in ["中国资产池", "美国资产池", "卫星资产池"]
    }
    batch1 = select_batch1(queue)

    lines = [
        "# AI Infra Source Verification Queue v1",
        "",
        "状态：原文核验任务队列，所有公司仍为 `pending_original_source_verification`。",
        "",
        "边界：这是研究优先级和证据核验清单，不是投资建议、买卖建议、目标价或仓位建议。",
        "",
        "## 总览",
        "",
        f"- 总记录数：{len(queue)}",
        f"- P0 第一批：{tier_counts.get('P0_first_batch', 0)}",
        f"- P1 跟进批：{tier_counts.get('P1_d1_d3_followup', 0)}",
        f"- P2/P3 雷达：{tier_counts.get('P2_radar_if_blocks_d2', 0) + tier_counts.get('P3_deep_radar', 0) + tier_counts.get('P3_low_priority', 0)}",
        f"- P4 排除/暂缓：{tier_counts.get('P4_excluded_or_hold', 0)}",
        "",
        "### Tier 分布",
        "",
        md_table(["Tier", "数量"], [[k, v] for k, v in tier_counts.most_common()]),
        "",
        "### 资产池分布",
        "",
        md_table(["资产池", "数量"], [[k, v] for k, v in asset_counts.most_common()]),
        "",
        "## P0 第一批核验",
        "",
        "P0 选择逻辑：BFS 最深不超过 D3、总分至少 90、且不在排除池。先用这些公司建立原文证据卡片和核验节奏。",
        "",
        "### Batch 1 建议先做",
        "",
        "为了避免一次铺太散，第一轮先从每个资产池各取 8 家 P0 公司做证据卡，覆盖中国、美国和卫星市场。",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows(batch1),
        ),
        "",
        "### 中国资产池 P0",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows(p0_by_asset["中国资产池"]),
        ),
        "",
        "### 美国资产池 P0",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows(p0_by_asset["美国资产池"]),
        ),
        "",
        "### 卫星资产池 P0",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows(p0_by_asset["卫星资产池"]),
        ),
        "",
        "## 核验工作法",
        "",
        "每家公司先做一张证据卡：",
        "",
        "- `原文来源`: annual report / quarterly results / earnings call / investor presentation / company product page / exchange filing。",
        "- `已证明事实`: 只写原文可证明的收入、订单、backlog、产能、毛利率、客户、技术路线。",
        "- `合理推论`: 明确写出从哪条原文事实推出来，不能混同为已证明。",
        "- `主要反证`: 客户集中、价格战、供给过剩、融资压力、技术路线变化、毛利率不跟随收入等。",
        "- `结论动作`: 升级 / 保持候选 / 降为雷达 / 排除。",
        "",
        "## P1 跟进批 Top 30",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows(tiers["P1_d1_d3_followup"][:30]),
        ),
        "",
        "## P2/P3 雷达 Top 30",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows((tiers["P2_radar_if_blocks_d2"] + tiers["P3_deep_radar"] + tiers["P3_low_priority"])[:30]),
        ),
        "",
        "## P4 排除或暂缓",
        "",
        md_table(
            ["Rank", "Ticker", "Company", "Market", "Asset Pool", "BFS", "Module", "Score", "Sources", "Metrics"],
            company_rows(tiers["P4_excluded_or_hold"][:40]),
        ),
        "",
        "## 建议下一步",
        "",
        "1. 先从每个资产池各抽 5-8 个 P0 公司做 evidence card。",
        "2. 每张卡只接受公司原文、交易所公告、监管文件、官网技术资料或上下游交叉披露。",
        "3. 完成第一批后再接 ETF holdings、免费价格数据、SEC 13F/N-PORT。",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    reports_dir = args.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)
    rows = load_rows(args.db)
    queue = build_queue(rows)
    write_csv(reports_dir / "source_verification_queue_v1.csv", queue)
    write_csv(reports_dir / "source_verification_batch1.csv", select_batch1(queue))
    write_markdown(reports_dir / "source_verification_queue_v1.md", queue)
    print(f"Generated {len(queue)} verification tasks.")
    print(f"CSV: {reports_dir / 'source_verification_queue_v1.csv'}")
    print(f"Markdown: {reports_dir / 'source_verification_queue_v1.md'}")


if __name__ == "__main__":
    main()
