"""Append the InP / CPO / silicon-photonics deep-dive candidates to the queue.

Operator deep-dive (2026-05-15) on the AI optical-interconnect profit pool —
the migration from pluggable optics toward InP laser sources, silicon-photonics
foundry capacity, CPO external light sources, and connector/fiber layers.

Cross-checked against global_universe_v2.jsonl + source_verification_queue_v1:
LITE / COHR / TSEM / IQE.L / AIXA.DE / SIVE.ST / AAOI / MTSI / SMTC / CRDO /
STM / FN / MRVL / AVGO / TER / FORM / CAMT / NVMI / ONTO / 2360.TW and the CN
optical names (300308/300502) are already tracked. These 8 are NOT — they fill
the InP-substrate, SiPho-foundry, CPO-ELS, and connector/fiber gaps.

Methodology guardrails:
- Every row enters at verification_status = pending_original_source_verification.
- evidence_state = 待原文核验 — these are radar candidates, NOT production. They
  must clear source review + the G0-G2 evidence gate before any R.
- This script never touches global_universe_v2.jsonl.
- A .bak copy is written before any change.
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
TPL_SOURCES_EQUIP = "annual report / quarterly results / earnings call / investor presentation / product roadmap"

CANDIDATES: list[dict[str, str]] = [
    {
        "ticker": "AXTI", "company": "AXT Inc.",
        "priority_tier": "P2_radar_if_blocks_d2",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D2-D3", "module": "InP crystal / substrate for AI optical lasers (800G/1.6T/CPO CW laser)",
        "current_pool": "雷达池", "total_score": "78", "score_bucket": "radar_review",
        "dependency_path": "AI cluster optical interconnect → InP laser/EML/CW laser → InP epi → InP substrate",
        "dependency_edge": "BOM边+产能边",
        "evidence_state": "待原文核验: InP收入连续环比增速、6-inch InP客户qualification、Tongmei扩产可销售产能、毛利率",
        "counterevidence": "估值脆(PE负)、融资稀释、中国出口许可/供应链风险、2027-2028 InP产能转过剩可能",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "InP segment revenue QoQ, gross margin, 6-inch InP customer qualification, Tongmei capacity ramp, customer concentration, export-license exposure",
        "upgrade_conditions": "InP收入连续两季度环比高增 + 6-inch InP进入客户qualification + 毛利率回正",
        "downgrade_conditions": "InP从短缺转过剩、出口/客户认证受阻、融资大幅稀释",
        "etf_clue": "SOXX/SMH (small weight) / compound-semi baskets",
        "smart_money_clue": "13F small-cap semi; 期权活跃 / short interest 高",
        "market_context_notes": "最纯 InP substrate 公开标的，高弹性高风险；反证：LightCounting 称 2025 InP laser 加产或缓解短缺",
    },
    {
        "ticker": "POET", "company": "POET Technologies",
        "priority_tier": "P3_deep_radar",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D3-D4", "module": "External light source (Blazar) + optical interposer for CPO",
        "current_pool": "雷达池", "total_score": "62", "score_bucket": "radar_review",
        "dependency_path": "CPO switch/optical engine → external light source + optical interposer → POET Blazar",
        "dependency_edge": "技术边+客户边",
        "evidence_state": "待原文核验: Blazar量产客户、hyperscaler/switch-ASIC qualification、optical interposer从demo到volume、现金流runway",
        "counterevidence": "商业化未验证、博客TAM偏marketing、融资压力、可能只是CPO概念无可交付产能",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "Blazar production design wins, qualification status, interposer volume revenue, cash runway, dilution risk",
        "upgrade_conditions": "Blazar/interposer 进入 hyperscaler 或 switch-ASIC 厂 qualification + 实际收入确认",
        "downgrade_conditions": "持续亏损无量产、融资大幅稀释、CPO 延后",
        "etf_clue": "small-cap photonics / 无干净 ETF",
        "smart_money_clue": "retail/options heavy; 高波动期权型",
        "market_context_notes": "高波动期权型标的；ELS 方向纯度高但商业化最不确定",
    },
    {
        "ticker": "GFS", "company": "GlobalFoundries",
        "priority_tier": "P2_radar_if_blocks_d2",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D3", "module": "Silicon photonics PIC foundry platform (Fotonix / AMF)",
        "current_pool": "雷达池", "total_score": "72", "score_bucket": "radar_review",
        "dependency_path": "SiPho PIC demand → silicon photonics foundry capacity → GlobalFoundries",
        "dependency_edge": "产能边+技术边",
        "evidence_state": "待原文核验: SiPho收入占总收入比、客户名单、SiPho毛利率vs普通foundry、AMF收购产能路线",
        "counterevidence": "SiPho纯度低(大盘foundry)、2028后GFS/ST/TSMC/Intel都加入稀释稀缺性、成熟制程周期",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "silicon photonics revenue share, customer list, SiPho gross margin, AMF integration, 2026 doubling claim vs official guidance",
        "upgrade_conditions": "公司原始指引确认 SiPho 收入翻倍 + 高毛利 + 客户多元化",
        "downgrade_conditions": "SiPho 仍是边缘收入、foundry 价格战、成熟制程下行",
        "etf_clue": "SOXX/SMH/XLK",
        "smart_money_clue": "13F semi large-cap",
        "market_context_notes": "SiPho foundry 中低纯度大盘；产业链验证作用 > 弹性；TrendForce 称 2026 收入翻倍待官方核验",
    },
    {
        "ticker": "GLW", "company": "Corning",
        "priority_tier": "P3_deep_radar",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D4", "module": "Optical fiber + cable + connectivity for AI data center",
        "current_pool": "雷达池", "total_score": "70", "score_bucket": "radar_review",
        "dependency_path": "AI datacenter buildout → optical fiber/cable/connectivity → Corning Optical Communications",
        "dependency_edge": "BOM边+产能边",
        "evidence_state": "待原文核验: NVIDIA光连接多年合作收入贡献、Optical Communications AI收入拆分、毛利率",
        "counterevidence": "弹性低(大盘)、显示玻璃/其他业务周期混杂、AI占比有限",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "Optical Communications AI/datacenter revenue split, NVIDIA partnership revenue contribution, gross margin trend",
        "upgrade_conditions": "AI/datacenter optical 收入拆分明确且高增 + NVIDIA 合作进入收入",
        "downgrade_conditions": "光通信增速回落、显示业务拖累、AI 占比始终偏低",
        "etf_clue": "XLK / industrials / optical infra",
        "smart_money_clue": "13F large-cap; 被动权重",
        "market_context_notes": "光互连基础设施层大盘复利；验证 'AI 不是只买 transceiver，还要买 fiber/connectivity'",
    },
    {
        "ticker": "AEHR", "company": "Aehr Test Systems",
        "priority_tier": "P3_deep_radar",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D4", "module": "Burn-in / test systems for photonics & power semiconductor devices",
        "current_pool": "雷达池", "total_score": "58", "score_bucket": "radar_review",
        "dependency_path": "High-speed optical device yield/reliability → burn-in & test → Aehr test systems",
        "dependency_edge": "设备边+技术边",
        "evidence_state": "待原文核验: photonics/optical test收入占比、客户名单、订单可见度",
        "counterevidence": "收入小、客户高度集中、SiC test业务周期、AI photonics占比未证实",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "photonics test revenue share, customer concentration, order backlog, AI optical vs SiC mix",
        "upgrade_conditions": "photonics/optical test 收入占比上升 + 多客户订单可见度",
        "downgrade_conditions": "纯 SiC 周期下行、客户集中风险、AI photonics 敞口未兑现",
        "etf_clue": "SOXX (tiny) / semi-cap-equipment",
        "smart_money_clue": "small-cap; 期权 / short heavy",
        "market_context_notes": "光器件测试/burn-in 小盘；需证明 AI photonics 敞口而非纯 SiC test",
    },
    {
        "ticker": "APH", "company": "Amphenol",
        "priority_tier": "P3_deep_radar",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D4", "module": "Connectors + fiber management + interconnect for AI data center",
        "current_pool": "雷达池", "total_score": "66", "score_bucket": "radar_review",
        "dependency_path": "CPO/AI networking → fiber/connector content increase → Amphenol interconnect",
        "dependency_edge": "BOM边",
        "evidence_state": "待原文核验: IT-datacom/AI收入拆分、CPO fiber/连接器单机内容增量",
        "counterevidence": "弹性低、业务极分散、并购驱动增长难拆 AI 纯度",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "IT datacom segment AI revenue, CPO connector content per system, organic vs M&A growth",
        "upgrade_conditions": "IT-datacom AI 收入拆分明确 + CPO 连接器单机内容上升",
        "downgrade_conditions": "AI 占比稀薄、增长靠并购、连接器价格竞争",
        "etf_clue": "XLK / industrials",
        "smart_money_clue": "13F large-cap",
        "market_context_notes": "连接器大盘；CPO 使 fiber/connector 单机内容上升的受益者",
    },
    {
        "ticker": "TEL", "company": "TE Connectivity",
        "priority_tier": "P3_deep_radar",
        "market_country": "US", "asset_pool": "美国资产池",
        "bfs_depth": "D4", "module": "Connectors + interconnect for AI data center networking",
        "current_pool": "雷达池", "total_score": "62", "score_bucket": "radar_review",
        "dependency_path": "AI networking → connector/interconnect content → TE Connectivity",
        "dependency_edge": "BOM边",
        "evidence_state": "待原文核验: datacom/AI连接器收入、CPO fiber内容增量、数据中心segment增速",
        "counterevidence": "弹性低、工业/汽车周期混杂、AI占比有限",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "datacom connector revenue, AI datacenter exposure, CPO fiber content per system",
        "upgrade_conditions": "datacom/AI 连接器收入明确高增 + CPO 内容增量",
        "downgrade_conditions": "工业/汽车周期拖累、AI 占比偏低",
        "etf_clue": "XLK / industrials",
        "smart_money_clue": "13F large-cap",
        "market_context_notes": "连接器大盘，AI 纯度低于 APH；作为 APH 的对照标的",
    },
    {
        "ticker": "5802.T", "company": "Sumitomo Electric Industries",
        "priority_tier": "P3_deep_radar",
        "market_country": "日本", "asset_pool": "海外卫星池",
        "bfs_depth": "D2", "module": "InP substrate + compound semiconductor materials (2nd-source)",
        "current_pool": "雷达池", "total_score": "64", "score_bucket": "radar_review",
        "dependency_path": "AI optical laser demand → InP substrate → Sumitomo Electric compound-semi materials",
        "dependency_edge": "BOM边+产能边",
        "evidence_state": "待原文核验: InP substrate扩产计划、AI光通信材料收入、产能与AXT/JX相对份额",
        "counterevidence": "大型综合集团业务极分散、InP占比极小、纯度难拆、日元/出口风险",
        "primary_sources_to_find": TPL_SOURCES_EQUIP,
        "metrics_to_verify": "InP substrate capacity expansion, optical materials revenue, share vs AXT/JX Advanced Metals",
        "upgrade_conditions": "InP substrate 扩产明确 + 光通信材料收入可拆分高增",
        "downgrade_conditions": "InP 占比始终极小、集团周期拖累、无法拆出 AI 纯度",
        "etf_clue": "EWJ / Japan industrials",
        "smart_money_clue": "日股大盘；被动",
        "market_context_notes": "InP substrate 第二供应源；集团纯度极低，用于验证 'InP 不止 AXT 一家'",
    },
]


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

    new_rows: list[dict[str, str]] = []
    skipped: list[str] = []
    for candidate in CANDIDATES:
        primary = candidate["ticker"].split("/")[0].strip().upper()
        if primary in existing_tickers:
            skipped.append(primary)
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
        existing_tickers.add(primary)

    print(f"InP/CPO/photonics panel: {len(CANDIDATES)} candidates | "
          f"already in queue: {len(skipped)} | to append: {len(new_rows)}")
    if args.dry_run or not new_rows:
        for row in new_rows:
            print(f"  + #{row['rank']} {row['ticker']:9} {row['priority_tier']:24} "
                  f"{row['bfs_depth']:6} {row['module']}")
        if skipped:
            print(f"  skipped (already present): {', '.join(skipped)}")
        if not args.dry_run:
            print("nothing to add")
        return 0

    if not args.no_backup:
        backup = args.queue.with_suffix(args.queue.suffix + ".bak")
        shutil.copy2(args.queue, backup)
        print(f"backup: {backup}")

    with args.queue.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        for row in new_rows:
            writer.writerow(row)
    print(f"appended {len(new_rows)} rows; skipped {len(skipped)}")
    for row in new_rows:
        print(f"  + #{row['rank']} {row['ticker']:9} {row['priority_tier']:24} {row['module']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
