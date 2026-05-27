"""AI Supercycle evidence + supply chain + value radar sections (Phase B.9).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Three clean evidence-ledger sections that all
just render payload rows; no DB access or complex deps.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text, fmt_num


def render_ai_supercycle_evidence_section(payload: dict[str, Any], market: str | None = None, *, limit: int = 14) -> list[str]:
    ledger = payload.get("ai_supercycle_evidence_ledger") or {}
    rows = ledger.get("rows") or []
    if market:
        rows = [row for row in rows if str(row.get("market") or "").upper() == market.upper()]
    title = "AI Supercycle Evidence Ledger" if not market else f"{market.upper()} AI Supercycle Evidence"
    if market and market.upper() == "US":
        contract_note = (
            "这张表只回答一件事：美股候选票和 AI 大周期的关系有什么可审计证据。"
            "`source_linked_supply_evidence` 才能当供应链证据；`negative_supply_evidence` 是订单/供应关系风险，必须先澄清；"
            "`theme_news_only` 只能说明主题方向，不能冒充供应关系。"
        )
    elif market and market.upper() == "CN":
        contract_note = (
            "这张表只回答一件事：A股候选票和 AI 大周期的关系有什么可审计证据。"
            "`price_flow_first_no_current_news` 只能说明价格/资金先动，不能冒充公告确认的供应链关系。"
        )
    else:
        contract_note = (
            "这张表只回答一件事：候选票和 AI 大周期的关系有什么可审计证据。"
            "`source_linked_supply_evidence` 才能当供应链证据；`negative_supply_evidence` 是订单/供应关系风险，必须先澄清；"
            "`theme_news_only` 和 A股 `price_flow_first_no_current_news` 只能说明主题/盘面方向，不能冒充供应关系。"
        )
    lines = [
        f"## {title}",
        "",
        contract_note,
        "",
    ]
    if not rows:
        lines += ["- No AI supercycle evidence rows.", ""]
        return lines
    lines += [
        "| Market | Symbol | Layer | Evidence | Score | Role / bottleneck | Text |",
        "|---|---|---|---|---:|---|---|",
    ]
    for row in rows[:limit]:
        role = row.get("supply_chain_role") or row.get("bottleneck_focus") or "-"
        lines.append(
            f"| {row.get('market')} | {row.get('symbol')} | {row.get('layer')} | "
            f"{row.get('evidence_state')} | {fmt_num(row.get('evidence_score'), 2)} | "
            f"{clean_table_text(role, 70)} | {clean_table_text(row.get('evidence_text'), 130)} |"
        )
    lines.append("")
    return lines


def render_ai_supply_chain_relationships_section(payload: dict[str, Any], *, limit: int = 12) -> list[str]:
    ledger = payload.get("ai_supply_chain_relationships") or {}
    summary = ledger.get("summary") or {}
    rows = ledger.get("rows") or []
    lines = [
        "## AI Supply Chain Relationship Ledger",
        "",
        "这是供应链关系底稿，只收带 source_url/source_type/confidence 的关系。它解决的是“这家公司到底和 AI 卡点有没有可审计关系”，不直接生成交易 R。",
        "",
        f"- source-linked relationships: {summary.get('source_linked', 0)} / {summary.get('rows', 0)}",
        "",
    ]
    if not rows:
        lines += [f"- data_required: {ledger.get('data_required') or 'relationship ledger missing'}", ""]
        return lines
    lines += [
        "| Primary | Counterparty | Layer | Type | Confidence | Source | Role / bottleneck |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows[:limit]:
        source = row.get("source_name") or row.get("source_url") or "-"
        lines.append(
            f"| {row.get('primary_symbol') or '-'} | {row.get('counterparty_symbol') or '-'} | "
            f"{row.get('layer') or '-'} | {row.get('relationship_type') or '-'} | "
            f"{row.get('confidence') or '-'} | {clean_table_text(source, 45)} | "
            f"{clean_table_text(row.get('supply_chain_role') or row.get('bottleneck_focus'), 100)} |"
        )
    lines.append("")
    return lines


def render_ai_supercycle_value_radar_section(
    payload: dict[str, Any],
    market: str | None = None,
    *,
    limit: int = 16,
) -> list[str]:
    radar = payload.get("ai_supercycle_value_radar") or {}
    rows = radar.get("rows") or []
    if market:
        rows = [row for row in rows if str(row.get("market") or "").upper() == market.upper()]
    title = "AI Supercycle 10x Value Radar" if not market else f"{market.upper()} AI Supercycle 10x Value Radar"
    tail = {
        "US": "没有供应链证据的票只能进 research watch。",
        "CN": "只有 tape、没有公告/供应关系的票只能进 research watch。",
    }.get((market or "").upper(),
          "A 股若只有 tape、没有公告/供应关系,只能进 research watch。")
    note = (
        "长期研究雷达,不是当日交易指令。排序优先 AI 卡点层、公司级证据、小中市值可选性、"
        f"以及增长/估值数据是否到位 —— {tail}"
    )
    lines = [
        f"## {title}",
        "",
        note,
        "",
    ]
    if not rows:
        lines += ["- No value-radar rows.", ""]
        return lines
    lines += [
        "| Rank | Market | Symbol | Layer | Priority | Score | Size | Evidence | Lab | Valuation | Next work |",
        "|---:|---|---|---|---|---:|---|---|---:|---|---|",
    ]
    for idx, row in enumerate(rows[:limit], start=1):
        lines.append(
            f"| {idx} | {row.get('market')} | {row.get('symbol')} | {row.get('layer')} | "
            f"{row.get('research_priority')} | {fmt_num(row.get('value_radar_score'), 2)} | "
            f"{clean_table_text(row.get('size_reason'), 45)} | {row.get('evidence_state')} | "
            f"{fmt_num(row.get('lab_quality_score'), 1)} | "
            f"{clean_table_text(row.get('valuation_snapshot'), 55)} | "
            f"{clean_table_text('; '.join(row.get('blockers') or []) or row.get('next_due_diligence'), 90)} |"
        )
    lines.append("")
    return lines
