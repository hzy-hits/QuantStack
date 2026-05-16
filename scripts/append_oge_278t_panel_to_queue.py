"""Append the Trump OGE 278-T (Q1 2026) disclosed-rotation panel to the queue.

Operator input (2026-05-16): the latest OGE Form 278-T periodic disclosure
shows Q1 2026 buys rotating into "AI + US industrials + defense + finance".

This is a WEAK, LAGGED signal (filing surfaces ~6 weeks late; dollar amounts
are disclosed ranges). Operator decision: track it as a `smart_money_clue`
tag only — it never feeds the evidence gate and never generates R.

Two operations:
1. Append the 18 names not already tracked. Off-mandate names (defense /
   banks / crypto / pure AI-software) go into a quarantined pool label
   `宏观轮动观察池` with tier `P4_off_mandate_macro_signal` so they cannot be
   confused with AI-infra BFS candidates. They sit in the queue at
   `待原文核验` and — being absent from global_universe_v2.jsonl — physically
   cannot reach the production basket.
2. Annotate the 8 already-tracked names' `smart_money_clue` with the OGE tag.

A .bak copy is written before any change. Idempotent.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
QUEUE = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"

FIELDS = (
    "rank", "priority_tier", "ticker", "company", "market_country", "asset_pool",
    "bfs_depth", "module", "current_pool", "total_score", "score_bucket",
    "verification_status", "source_priority", "primary_sources_to_find",
    "metrics_to_verify", "upgrade_conditions", "downgrade_conditions",
    "evidence_state", "counterevidence", "dependency_path", "dependency_edge",
    "etf_clue", "smart_money_clue", "market_context_notes",
)

TPL_SOURCE_PRIORITY = "Find latest annual report, latest quarterly results, earnings call transcript, investor presentation, and official product/capacity pages first."
TPL_VERIFICATION_DEFAULT = "pending_original_source_verification"
TPL_SOURCES = "annual report / quarterly results / earnings call / investor presentation / 10-K / 10-Q"

OGE_TAG = "OGE-278T Q1-2026 川普披露 net-buy"

# Already tracked (universe or queue) — only get the OGE clue annotation.
ALREADY_TRACKED = ["NVDA", "AVGO", "SNPS", "CDNS", "DELL", "BLK", "SNDK", "INTC"]

# ── Group A: genuine AI-infra supply-chain gaps (proper BFS framing) ──────────
GROUP_A_AI_INFRA = [
    {
        "ticker": "JBL", "company": "Jabil",
        "bfs_depth": "D4", "module": "AI server / data-center rack EMS & advanced manufacturing",
        "dependency_path": "Hyperscaler AI capex → AI server / rack assembly → Jabil EMS",
        "dependency_edge": "BOM边+产能边",
        "evidence_state": "待原文核验: AI/数据中心 EMS 收入拆分、机柜级订单、产能",
        "counterevidence": "EMS 毛利率薄、客户集中、消费/汽车业务周期混杂",
        "metrics_to_verify": "AI/data-center EMS revenue split, rack-level order book, margin, customer concentration",
        "etf_clue": "XLK / contract-manufacturing",
    },
    {
        "ticker": "TXN", "company": "Texas Instruments",
        "bfs_depth": "D4-D5", "module": "Analog / power management for AI servers & data-center power",
        "dependency_path": "AI server power delivery → analog / PMIC / power → Texas Instruments",
        "dependency_edge": "BOM边",
        "evidence_state": "待原文核验: 数据中心/企业 analog 收入、AI 服务器电源内容增量",
        "counterevidence": "模拟周期、AI 占比小、汽车/工业为收入主体、边缘 AI-infra",
        "metrics_to_verify": "data-center analog revenue, AI server power content, capex cycle",
        "etf_clue": "SOXX/SMH/XLK",
    },
]

# ── Group B: AI application / demand-side (D0, off the hardware BFS spine) ────
GROUP_B_AI_APP = [
    {
        "ticker": "NOW", "company": "ServiceNow",
        "module": "Enterprise AI workflow application — demand-side",
        "etf_clue": "IGV / software", "biz": "企业 AI 工作流软件",
    },
    {
        "ticker": "ADBE", "company": "Adobe",
        "module": "Creative / document AI application — demand-side",
        "etf_clue": "IGV / software", "biz": "创意/文档 AI 软件",
    },
    {
        "ticker": "WDAY", "company": "Workday",
        "module": "HR / finance AI application — demand-side",
        "etf_clue": "IGV / software", "biz": "HR/财务 AI 软件",
    },
    {
        "ticker": "PLTR", "company": "Palantir",
        "module": "AI analytics / defense software — demand-side",
        "etf_clue": "IGV / defense-tech", "biz": "AI 分析/防务软件",
    },
]

# ── Group C: off-mandate macro rotation (quarantined pool) ───────────────────
GROUP_C_OFF_MANDATE = [
    {"ticker": "BA", "company": "Boeing", "biz": "民航 + 防务航空", "etf_clue": "ITA / XAR"},
    {"ticker": "GE", "company": "GE Aerospace", "biz": "航空发动机", "etf_clue": "ITA / XAR"},
    {"ticker": "TDG", "company": "TransDigm", "biz": "航空零部件", "etf_clue": "ITA / XAR"},
    {"ticker": "AXON", "company": "Axon Enterprise", "biz": "公共安全/防务科技", "etf_clue": "ITA / XAR"},
    {"ticker": "MSI", "company": "Motorola Solutions", "biz": "公共安全通信", "etf_clue": "ITA / XLK"},
    {"ticker": "GS", "company": "Goldman Sachs", "biz": "投行/资管", "etf_clue": "XLF",
     "wedge_note": "银行 — 与 bubble-hedge wedge 层相关"},
    {"ticker": "PNC", "company": "PNC Financial", "biz": "区域银行", "etf_clue": "XLF / KRE",
     "wedge_note": "区域银行 — 与 bubble-hedge wedge 层相关"},
    {"ticker": "BAC", "company": "Bank of America", "biz": "大型银行", "etf_clue": "XLF",
     "wedge_note": "银行 — 与 bubble-hedge wedge 层相关"},
    {"ticker": "HOOD", "company": "Robinhood", "biz": "互联网券商/fintech", "etf_clue": "XLF / fintech"},
    {"ticker": "COIN", "company": "Coinbase", "biz": "加密交易所", "etf_clue": "crypto-equity"},
    {"ticker": "MSTR", "company": "Strategy (MicroStrategy)", "biz": "比特币代理标的", "etf_clue": "crypto-equity"},
    {"ticker": "MARA", "company": "Marathon Digital", "biz": "比特币矿企", "etf_clue": "crypto-equity"},
]


def _build_candidates() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for c in GROUP_A_AI_INFRA:
        rows.append({
            "priority_tier": "P3_deep_radar",
            "market_country": "US", "asset_pool": "美国资产池",
            "current_pool": "雷达池", "total_score": "60", "score_bucket": "radar_review",
            "upgrade_conditions": "AI/数据中心收入拆分明确且高增,通过 source review",
            "downgrade_conditions": "AI 占比稀薄、周期下行、毛利率压缩",
            "smart_money_clue": f"13F semi/EMS; {OGE_TAG}",
            "market_context_notes": "OGE-278T Q1-2026 川普加仓主线之一;AI-infra 供应链相关,进雷达核验",
            "primary_sources_to_find": TPL_SOURCES,
            **c,
        })
    for c in GROUP_B_AI_APP:
        rows.append({
            "priority_tier": "P4_off_mandate_macro_signal",
            "market_country": "US", "asset_pool": "美国资产池",
            "bfs_depth": "D0(应用层)", "current_pool": "宏观轮动观察池",
            "total_score": "0", "score_bucket": "off_mandate_watch",
            "dependency_path": f"AI token 需求侧 — {c['biz']};非硬件 BFS spine",
            "dependency_edge": "需求边(非供应链瓶颈)",
            "evidence_state": f"待原文核验: OGE-278T Q1-2026 川普 net-buy({c['biz']},滞后信号、金额区间估算)",
            "counterevidence": "AI 软件应用层,非 AI-infra 硬件供应链;不可进生产池;仅作信号跟踪",
            "metrics_to_verify": "AI-attributable revenue split (if any); 仅信号跟踪,不做深度核验",
            "upgrade_conditions": "—(off-mandate,不晋级)",
            "downgrade_conditions": "—",
            "smart_money_clue": OGE_TAG,
            "market_context_notes": "川普调仓主线 AI+工业链+国防+金融;本基金只在 AI-infra 硬件链内行动,此处仅记录信号",
            "primary_sources_to_find": "—(信号跟踪,不做 source review)",
            **{k: v for k, v in c.items() if k in FIELDS},
        })
    for c in GROUP_C_OFF_MANDATE:
        wedge = c.get("wedge_note", "")
        notes = "川普调仓主线 AI+工业链+国防+金融;本基金只在 AI-infra 内行动,此处仅记录信号"
        if wedge:
            notes = f"{notes};{wedge}"
        rows.append({
            "priority_tier": "P4_off_mandate_macro_signal",
            "ticker": c["ticker"], "company": c["company"],
            "market_country": "US", "asset_pool": "美国资产池",
            "bfs_depth": "—(off-BFS)", "module": c["biz"],
            "current_pool": "宏观轮动观察池",
            "total_score": "0", "score_bucket": "off_mandate_watch",
            "dependency_path": "非 AI-infra 供应链 — OGE-278T Q1-2026 政策/宏观轮动信号",
            "dependency_edge": "—",
            "evidence_state": f"待原文核验: OGE-278T Q1-2026 川普 net-buy({c['biz']},滞后信号、金额区间估算)",
            "counterevidence": "出 AI-infra mandate;不可进生产池;仅作政策/宏观轮动跟踪,不生成 R",
            "metrics_to_verify": "—(off-mandate,仅信号跟踪)",
            "upgrade_conditions": "—(off-mandate,不晋级)",
            "downgrade_conditions": "—",
            "etf_clue": c.get("etf_clue", ""),
            "smart_money_clue": OGE_TAG,
            "market_context_notes": notes,
            "primary_sources_to_find": "—(信号跟踪,不做 source review)",
        })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, default=QUEUE)
    parser.add_argument("--no-backup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.queue.exists():
        print(f"error: queue not found at {args.queue}", file=sys.stderr)
        return 2

    with args.queue.open("r", encoding="utf-8") as handle:
        existing_rows = list(csv.DictReader(handle))
    existing_tickers: set[str] = set()
    for row in existing_rows:
        for piece in (row.get("ticker") or "").split("/"):
            t = piece.strip().upper()
            if t:
                existing_tickers.add(t)
    max_rank = max(
        (int(r["rank"]) for r in existing_rows if r.get("rank") and r["rank"].isdigit()),
        default=0,
    )

    # 1. Append new rows.
    new_rows: list[dict[str, str]] = []
    skipped: list[str] = []
    for candidate in _build_candidates():
        ticker = candidate["ticker"].upper()
        if ticker in existing_tickers:
            skipped.append(ticker)
            continue
        max_rank += 1
        row = {key: "" for key in FIELDS}
        row.update({
            "rank": str(max_rank),
            "verification_status": TPL_VERIFICATION_DEFAULT,
            "source_priority": TPL_SOURCE_PRIORITY,
        })
        row.update({k: v for k, v in candidate.items() if k in FIELDS})
        new_rows.append(row)
        existing_tickers.add(ticker)

    # 2. Annotate already-tracked rows' smart_money_clue.
    annotated: list[str] = []
    for row in existing_rows:
        tickers = {t.strip().upper() for t in (row.get("ticker") or "").split("/")}
        if tickers & set(ALREADY_TRACKED):
            clue = row.get("smart_money_clue") or ""
            if OGE_TAG not in clue:
                row["smart_money_clue"] = f"{clue} | {OGE_TAG}".strip(" |")
                annotated.append(sorted(tickers & set(ALREADY_TRACKED))[0])

    print(f"OGE-278T panel: {len(_build_candidates())} candidates | "
          f"already in queue: {len(skipped)} | to append: {len(new_rows)} | "
          f"annotated existing: {len(annotated)}")
    if args.dry_run:
        for row in new_rows:
            print(f"  + #{row['rank']} {row['ticker']:7} {row['priority_tier']:28} "
                  f"{row['current_pool']:14} {row['module']}")
        if annotated:
            print(f"  ~ annotated: {', '.join(sorted(set(annotated)))}")
        if skipped:
            print(f"  skipped: {', '.join(sorted(set(skipped)))}")
        return 0

    if not new_rows and not annotated:
        print("nothing to do")
        return 0

    if not args.no_backup:
        backup = args.queue.with_suffix(args.queue.suffix + ".bak")
        shutil.copy2(args.queue, backup)
        print(f"backup: {backup}")

    # Full rewrite (annotations require it).
    with args.queue.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in existing_rows:
            writer.writerow({k: row.get(k, "") for k in FIELDS})
        for row in new_rows:
            writer.writerow(row)
    print(f"appended {len(new_rows)} rows; annotated {len(annotated)} existing; skipped {len(skipped)}")
    for row in new_rows:
        print(f"  + #{row['rank']} {row['ticker']:7} {row['current_pool']:14} {row['module']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
