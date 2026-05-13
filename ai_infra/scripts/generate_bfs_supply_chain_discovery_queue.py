#!/usr/bin/env python3
"""Generate BFS supply-chain discovery queues and a ChatGPT Pro prompt.

The goal is not to recommend trades. This script turns the existing universe
seed into repeatable research tasks for finding additional AI-infra suppliers
through filings, annual reports, investor presentations, and cross-disclosures.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


DEFAULT_INPUT = Path("data/global_universe_v2.jsonl")
DEFAULT_REPORTS_DIR = Path("reports")
DEFAULT_NOTES_DIR = Path("notes")

THEME_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("compute_gpu_asic", ("gpu", "cuda", "asic", "xpu", "tpu", "trainium", "compute")),
    ("hbm_memory_storage", ("hbm", "dram", "nand", "ssd", "memory", "storage", "cxl")),
    ("advanced_packaging_substrate", ("cowos", "packaging", "substrate", "abf", "bonding", "molding", "osat")),
    ("testing_metrology", ("test", "probe", "socket", "inspection", "metrology", "ate")),
    ("networking_fabric", ("ethernet", "infiniband", "network", "retimer", "serdes", "cxl", "pcie", "fabric")),
    ("optics_photonics_cpo", ("optical", "optics", "photonics", "laser", "cpo", "800g", "1.6t", "silicon photonics")),
    ("power_cooling_grid", ("power", "cooling", "thermal", "liquid", "ups", "switchgear", "grid", "electrical")),
    ("neocloud_data_center", ("neocloud", "cloud", "data center", "gpu-as-a-service", "hosting", "colo")),
    ("materials_equipment", ("material", "chemical", "gas", "vacuum", "wafer", "wfe", "upw")),
]

SOURCE_TARGETS_BY_THEME = {
    "compute_gpu_asic": "10-K/20-F, annual report, product roadmap, platform BOM, hyperscaler customer disclosures",
    "hbm_memory_storage": "annual report, quarterly earnings, HBM roadmap, capacity expansion, customer qualification notes",
    "advanced_packaging_substrate": "annual report, packaging capacity notes, substrate capacity, equipment orders, customer cross-disclosures",
    "testing_metrology": "annual report, product application pages, orders/backlog, tester/probe/socket revenue mix",
    "networking_fabric": "annual report, product pages, switch/NIC/retimer/AEC roadmap, hyperscaler network disclosures",
    "optics_photonics_cpo": "annual report, OFC/OCP product pages, 800G/1.6T/CPO qualification, laser capacity disclosures",
    "power_cooling_grid": "annual report, backlog, book-to-bill, lead-time commentary, data center customer disclosures",
    "neocloud_data_center": "10-K/20-F/S-1, RPO/backlog footnotes, lease/debt footnotes, power/MW disclosures",
    "materials_equipment": "annual report, segment disclosure, product purity/specification pages, customer qualification notes",
    "other": "annual report, quarterly results, investor presentation, official product and customer pages",
}

EXTRACTION_KEYWORDS = {
    "default": [
        "AI", "accelerated computing", "data center", "hyperscale", "HPC",
        "customer", "supplier", "backlog", "capacity", "qualification",
        "lead time", "gross margin", "capex", "free cash flow",
    ],
    "compute_gpu_asic": ["GPU", "accelerator", "ASIC", "chiplet", "CoWoS", "HBM", "CUDA", "TPU"],
    "hbm_memory_storage": ["HBM3E", "HBM4", "server DRAM", "enterprise SSD", "QLC", "CXL"],
    "advanced_packaging_substrate": ["CoWoS", "2.5D", "hybrid bonding", "TCB", "ABF", "interposer", "substrate"],
    "testing_metrology": ["probe card", "wafer probe", "known good die", "memory test", "SoC test", "inspection"],
    "networking_fabric": ["800G", "1.6T", "Ethernet", "InfiniBand", "NVLink", "retimer", "AEC", "SerDes"],
    "optics_photonics_cpo": ["CPO", "LPO", "silicon photonics", "InP", "EML", "DFB", "external light source"],
    "power_cooling_grid": ["liquid cooling", "CDU", "cold plate", "UPS", "PDU", "switchgear", "transformer", "MW"],
    "neocloud_data_center": ["RPO", "revenue backlog", "lease liabilities", "MW", "GPU fleet", "utilization"],
    "materials_equipment": ["photoresist", "slurry", "etch gas", "vacuum", "UPW", "wafer", "SOI", "InP"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate AI-infra BFS supply-chain discovery tasks.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input JSONL universe seed.")
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR, help="Output reports directory.")
    parser.add_argument("--notes-dir", type=Path, default=DEFAULT_NOTES_DIR, help="Output notes directory.")
    parser.add_argument("--max-prompt-seeds", type=int, default=90, help="Maximum seed rows embedded in the Pro prompt.")
    return parser.parse_args()


def read_jsonl(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSONL") from exc
            rows.append({str(k): "" if v is None else str(v) for k, v in obj.items()})
    return rows


def classify_theme(row: dict[str, str]) -> str:
    text = " ".join([
        row.get("module", ""),
        row.get("dependency_path", ""),
        row.get("dependency_edge", ""),
        row.get("overseas_bottleneck", ""),
    ]).lower()
    for theme, keywords in THEME_RULES:
        if any(keyword in text for keyword in keywords):
            return theme
    return "other"


def parse_depth_rank(depth: str) -> int:
    depth = depth.upper()
    ranks = []
    for marker, rank in [("D0", 0), ("D1", 1), ("D2", 2), ("D3", 3), ("D4", 4), ("D5", 5)]:
        if marker in depth:
            ranks.append(rank)
    return min(ranks) if ranks else 9


def priority_for(row: dict[str, str], theme: str) -> str:
    depth_rank = parse_depth_rank(row.get("bfs_depth", ""))
    pool = row.get("current_pool", "")
    if depth_rank <= 3 and ("核心" in pool or "候选" in pool):
        return "P0_expand_now"
    if depth_rank <= 3:
        return "P1_expand_after_p0"
    if theme in {"power_cooling_grid", "materials_equipment", "neocloud_data_center"}:
        return "P2_radar_if_blocks_d2"
    return "P3_deep_radar"


def region_bucket(row: dict[str, str]) -> str:
    market = row.get("market_country", "")
    pool = row.get("asset_pool", "")
    if "US" in market or "美国" in pool:
        return "US"
    if any(key in market for key in ["日本", "韩国", "台湾", "欧洲", "以色列"]):
        return "satellite_non_us"
    if "中国" in pool or "A股" in market or "H股" in market:
        return "china_hk"
    return "other"


def build_task(row: dict[str, str], index: int) -> dict[str, str]:
    theme = classify_theme(row)
    keywords = EXTRACTION_KEYWORDS["default"] + EXTRACTION_KEYWORDS.get(theme, [])
    ticker = row.get("ticker", "")
    company = row.get("company", "")
    module = row.get("module", "")
    return {
        "task_id": f"DISC-{index:04d}",
        "priority": priority_for(row, theme),
        "theme": theme,
        "region_bucket": region_bucket(row),
        "seed_ticker": ticker,
        "seed_company": company,
        "market_country": row.get("market_country", ""),
        "asset_pool": row.get("asset_pool", ""),
        "bfs_depth": row.get("bfs_depth", ""),
        "module": module,
        "current_pool": row.get("current_pool", ""),
        "source_targets": SOURCE_TARGETS_BY_THEME.get(theme, SOURCE_TARGETS_BY_THEME["other"]),
        "extraction_keywords": "; ".join(dict.fromkeys(keywords)),
        "expansion_goal": (
            "Find upstream suppliers, downstream customers, peer suppliers, equipment/material bottlenecks, "
            "and cross-disclosed companies that can be mapped into D1-D5 BFS with evidence status."
        ),
        "agent_prompt": (
            f"Seed: {ticker} {company}. Module: {module}. Read primary filings/product pages and extract named "
            "customers, suppliers, equipment, materials, capacity constraints, backlog drivers, and direct competitors. "
            "Return only source-backed candidates or clearly mark pending verification."
        ),
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "task_id", "priority", "theme", "region_bucket", "seed_ticker", "seed_company",
        "market_country", "asset_pool", "bfs_depth", "module", "current_pool",
        "source_targets", "extraction_keywords", "expansion_goal", "agent_prompt",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    out.extend("| " + " | ".join(cell.replace("|", "/") for cell in row) + " |" for row in rows)
    return "\n".join(out)


def write_report(path: Path, tasks: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_theme = Counter(task["theme"] for task in tasks)
    by_region = Counter(task["region_bucket"] for task in tasks)
    by_priority = Counter(task["priority"] for task in tasks)
    top = tasks[:40]
    lines = [
        "# BFS Supply-Chain Discovery Queue v1",
        "",
        "Status: generated queue; research-priority only; not investment advice.",
        "",
        "## Summary",
        "",
        f"- Total seed tasks: {len(tasks)}",
        f"- Themes: {dict(by_theme)}",
        f"- Regions: {dict(by_region)}",
        f"- Priorities: {dict(by_priority)}",
        "",
        "## Top Tasks",
        "",
        markdown_table(
            ["task_id", "priority", "theme", "region", "seed", "depth", "module"],
            [[
                t["task_id"], t["priority"], t["theme"], t["region_bucket"],
                f'{t["seed_ticker"]} {t["seed_company"]}', t["bfs_depth"], t["module"][:70],
            ] for t in top],
        ),
        "",
        "## Agent Output Contract",
        "",
        "Each agent run must output:",
        "",
        "1. `new_candidate`: company/ticker/exchange/country.",
        "2. `bfs_depth`: D1-D5 and the edge from seed to candidate.",
        "3. `evidence_state`: proven / reasonable inference / pending verification / refutation.",
        "4. `primary_sources_to_check`: filing, annual report, product page, customer/supplier cross-disclosure.",
        "5. `why_it_can_block_d0_d2`: only required if D4-D5 candidate is upgraded.",
        "6. `counterevidence`: what would invalidate the mapping.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def compact_seed_lines(tasks: list[dict[str, str]], max_rows: int) -> list[str]:
    selected = [
        task for task in tasks
        if task["priority"] in {"P0_expand_now", "P1_expand_after_p0"}
        and task["region_bucket"] in {"US", "satellite_non_us"}
    ][:max_rows]
    return [
        f"- {t['region_bucket']} | {t['seed_ticker']} | {t['seed_company']} | {t['bfs_depth']} | "
        f"{t['theme']} | {t['module']} | {t['current_pool']}"
        for t in selected
    ]


def write_pro_prompt(path: Path, tasks: list[dict[str, str]], max_seed_rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    seed_lines = compact_seed_lines(tasks, max_seed_rows)
    theme_counts = Counter(task["theme"] for task in tasks)
    prompt = f"""# ChatGPT Pro Prompt: BFS Supply-Chain Discovery Agent Design

你是 AI Infra 产业链研究助手。目标不是投资建议，不给买卖建议、不做目标价，而是帮我把现有 AI Infra universe 从 seed companies 扩展成 source-backed supply-chain discovery system。

## 项目框架

从 D0 LLM 需求源头出发做 dependency BFS：

- D0: OpenAI / Anthropic / Google DeepMind / Gemini / Meta / xAI 等 LLM demand。
- D1: GPU/TPU/ASIC/cloud/software stack。
- D2: HBM、CoWoS、leading-edge foundry、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- D3: HBM test/equipment/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer/AEC、液冷组件、电力设备关键部件。
- D4-D5: 材料、气体、化学品、真空、洁净、能源、电网、融资、监管，只做 radar，除非能证明反向卡住 D0-D2。

研究重点：美国公司 + 日本/韩国/台湾/欧洲/以色列公司。不要重复泛泛 AI 概念股，要沿供应链边找更多可交易或可观察公司。

## 现有 seed universe 摘要

主题计数：{dict(theme_counts)}

Seed companies:
{chr(10).join(seed_lines)}

## 任务 A：从 seed 出发扩展供应链

请按以下主题分别扩展更多美国、日本、韩国、台湾、欧洲、以色列候选公司：

1. compute_gpu_asic
2. hbm_memory_storage
3. advanced_packaging_substrate
4. testing_metrology
5. networking_fabric
6. optics_photonics_cpo
7. power_cooling_grid
8. neocloud_data_center
9. materials_equipment

对每个主题输出：

- 已有 seed 中最重要的 10 个起点；
- 还缺哪些供应链子环节；
- 应该如何从 annual report / 10-K / 20-F / earnings call / product pages / customer cross-disclosures 中找新公司；
- 新候选公司清单：ticker、exchange/country、BFS depth、dependency edge、为什么相关、需要核验的原文、主要反证；
- 明确标记哪些只是 radar，哪些可以进入 D1-D3 候选。

## 任务 B：自动化 agent pipeline 设计

请设计一个本地脚本/agent 系统，用于“读财报挖供应链公司”。要求：

1. 输入：seed universe JSONL，每条有 ticker/company/market_country/bfs_depth/module/dependency_path/current_pool。
2. 数据源优先级：company annual report、10-K/20-F/10-Q、earnings release/call transcript、investor presentation、company product pages、customer/supplier cross-disclosures、SEC/交易所公告。
3. Pipeline 阶段：
   - security master / ticker normalization；
   - filing/source discovery；
   - PDF/HTML/text extraction；
   - entity extraction：customers、suppliers、competitors、equipment、materials、capacity、backlog、capex、RPO、lead time；
   - dependency edge classifier；
   - evidence card generator；
   - dedupe/entity linking；
   - candidate scoring；
   - refutation dashboard update。
4. 输出：SQLite schema、JSONL schema、CSV queue、Markdown evidence card。
5. 每个 agent 的 prompt：filing-reader、entity-linker、dependency-classifier、evidence-card-writer、refutation-reviewer。
6. 给出伪代码或 Python 标准库 MVP 设计，不接 IBKR、不自动交易。

## 任务 C：搜索/核验 query 模板

请给出用于每个主题的可执行搜索 query 模板，例如：

- `site:company.com annual report AI data center HBM supplier`
- `10-K customer concentration AI data center backlog`
- `investor presentation CoWoS substrate capacity`
- `OFC 1.6T CPO silicon photonics customer qualification`

## 证据规则

所有输出必须分为：

- 原文已证明；
- 合理推论；
- 待原文核验；
- 主要反证。

不要把媒体、模型记忆、券商摘要当成事实。不要输出投资建议、买入/卖出、目标价或实际仓位。
"""
    path.write_text(prompt, encoding="utf-8")


def main() -> None:
    args = parse_args()
    rows = read_jsonl(args.input)
    tasks = [build_task(row, index + 1) for index, row in enumerate(rows)]
    tasks.sort(key=lambda task: (task["priority"], task["region_bucket"], task["theme"], task["seed_ticker"]))

    csv_path = args.reports_dir / "bfs_supply_chain_discovery_queue_v1.csv"
    report_path = args.reports_dir / "bfs_supply_chain_discovery_queue_v1.md"
    prompt_path = args.notes_dir / "2026-05-13-chatgpt-pro-bfs-supply-chain-discovery-prompt.md"

    write_csv(csv_path, tasks)
    write_report(report_path, tasks)
    write_pro_prompt(prompt_path, tasks, args.max_prompt_seeds)

    print(f"Seed rows: {len(rows)}")
    print(f"Discovery tasks: {len(tasks)}")
    print(f"CSV: {csv_path}")
    print(f"Report: {report_path}")
    print(f"ChatGPT Pro prompt: {prompt_path}")


if __name__ == "__main__":
    main()
