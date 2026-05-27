"""AI Infra Satellite Pool section (Phase B.13).

Extracted from scripts/generate_main_strategy_v2_report.py — clean
self-contained section. Imports _READINESS_TIER_ORDER from
sections.audits_calendars (already extracted in B.10).
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text
from sections.audits_calendars import _READINESS_TIER_ORDER


def render_satellite_pool_report_section(payload: dict[str, Any], *, limit_per_region: int = 12) -> list[str]:
    report = payload.get("satellite_pool_report") or {}
    rows = report.get("rows") or []
    queue_path = report.get("queue_path") or "ai_infra/reports/source_verification_queue_v1.csv"
    lines = [
        "## AI Infra Satellite Pool (TW/JP/KR/EU/IL)",
        "",
        f"- 数据源: `{queue_path}`；状态: `{report.get('status') or 'unknown'}`；总数: {report.get('total_rows') or 0}",
        "- 范畴: 卫星资产池映射到 D1-D5 全球 AI infra 供应链；研究权重高，但需通过 IBKR/ADR 才能交易。",
        "- 这张表只回答两件事:哪些卫星名字进了 source review 队列、现在 evidence 写到几成。它不是买入许可。",
        "",
    ]

    region_counts = report.get("region_counts") or {}
    if region_counts:
        lines += [
            "### Region Coverage",
            "",
            "| Region | Count |",
            "|---|---:|",
        ]
        for region in sorted(region_counts, key=lambda r: (-region_counts[r], r)):
            lines.append(f"| {region or '-'} | {region_counts[region]} |")
        lines.append("")

    depth_counts = report.get("depth_counts") or {}
    if depth_counts:
        lines += [
            "### BFS Depth Coverage",
            "",
            "| Depth | Count |",
            "|---|---:|",
        ]
        for depth in sorted(depth_counts):
            lines.append(f"| {depth or '-'} | {depth_counts[depth]} |")
        lines.append("")

    readiness_counts = report.get("readiness_counts") or {}
    if readiness_counts:
        chunks = [
            f"{tier}={readiness_counts.get(tier, 0)}"
            for tier in _READINESS_TIER_ORDER
            if readiness_counts.get(tier, 0)
        ]
        lines += [
            "### Readiness Distribution",
            "",
            f"- {'; '.join(chunks) or 'all rows unscored'}",
            "",
        ]

    by_region: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_region.setdefault(row.get("region") or "Unknown", []).append(row)
    for region in sorted(by_region, key=lambda r: (-len(by_region[r]), r)):
        region_rows = by_region[region]
        lines += [
            f"### {region} ({len(region_rows)})",
            "",
            "| Rank | Ticker | Company | Depth | Module | Readiness | Tape | Market Context | Priority |",
            "|---:|---|---|---|---|---|---|---|---|",
        ]
        for entry in region_rows[:limit_per_region]:
            readiness = entry.get("readiness_tier") or "unscored"
            score = entry.get("readiness_score")
            score_text = f" ({score:.2f})" if isinstance(score, (int, float)) else ""
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(entry.get("rank") if entry.get("rank") is not None else "-"),
                        entry.get("primary_ticker") or entry.get("ticker") or "-",
                        clean_table_text(entry.get("company") or "-", 24),
                        entry.get("bfs_depth") or "-",
                        clean_table_text(entry.get("module") or "-", 28),
                        f"{readiness}{score_text}",
                        clean_table_text(entry.get("ema_summary") or "no_data", 42),
                        clean_table_text(entry.get("market_context_notes") or "-", 42),
                        entry.get("priority_tier") or "-",
                    ]
                )
                + " |"
            )
        lines.append("")
    return lines
