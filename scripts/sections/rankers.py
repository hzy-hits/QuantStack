"""Ranker + sector + lab + layer attribution sections (Phase B.12).

Extracted from scripts/generate_main_strategy_v2_report.py — 5 self-
contained section renderers, all only depending on lib.fmt.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text, fmt_num, fmt_pct, round_or_none


def render_us_opportunity_ranker_section(payload: dict[str, Any]) -> list[str]:
    ranker = payload.get("us_opportunity_ranker") or {}
    rows = ranker.get("top_rows") or []
    if not rows:
        return ["## 美股生产排序 / US Production Ranker", "", "- No US production ranker rows.", ""]
    lines = [
        "## 美股生产排序 / US Production Ranker",
        "",
        "当前 US 主执行层是 `us_theme_cluster_momentum`；AI supercycle layer/priority 进入排序，但供应链关系必须有新闻/公告/财报证据才写成正式理由。价格、新闻、期权/flow 联合排序，期权只做股票决策证据。",
        "",
        "| Rank | Symbol | Sleeve | Layer | Evidence | Tier | Action | Score | Joint | Headline | Options/Flow | R:R | Trend |",
        "|---:|---|---|---|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:12]:
        headline = round_or_none(row.get("headline_risk"))
        lines.append(
            f"| {row.get('rank')} | {row.get('symbol')} | {row.get('alpha_sleeve_id') or 'rank_only'} | "
            f"{row.get('supercycle_layer') or row.get('theme_id') or '-'} | "
            f"{row.get('supplier_evidence_state') or '-'} | "
            f"{row.get('production_tier')} | {row.get('production_action')} | "
            f"{fmt_num(row.get('rank_score'))} | "
            f"{fmt_num(row.get('joint_signal_score'), 0)} | "
            f"{fmt_num(None if headline is None else headline * 100.0, 0)} | "
            f"{fmt_num(row.get('flow_options_quality'), 0)} | "
            f"{fmt_num(row.get('rr_ratio'))} | {row.get('trend_regime') or '-'} |"
        )
    event_rows = [row for row in ranker.get("all_rows") or [] if row.get("production_tier") == "event_risk_watch"]
    if event_rows:
        lines += ["", "Event/news 0R watch:"]
        for row in event_rows[:6]:
            lines.append(f"- {row.get('symbol')}: {row.get('latest_headline') or row.get('headline_flags') or 'headline risk'}")
    lines.append("")
    return lines


def render_cn_opportunity_ranker_section(payload: dict[str, Any]) -> list[str]:
    ranker = payload.get("cn_opportunity_ranker") or {}
    rows = ranker.get("top_rows") or []
    if not rows:
        return ["## A 股生产排序 / CN Production Ranker", "", "- No CN production ranker rows.", ""]
    lines = [
        "## A 股生产排序 / CN Production Ranker",
        "",
        "`cn_tape_leadership_continuation` 是强市场主执行层；`cn_oversold_ev_positive` 和 `cn_observed_lifecycle_prob` 只做 secondary/弱市场工具。A 股排序先看价格、成交、资金流和板块联动；当前叙事优先 AI infra 与矿产/能源/重工，日常消费排除，互联网/软件降优先级。",
        "",
        "| Rank | Symbol | Name | Source | Tier | Action | Score | ExpR | LCBR | Obs n | Price | Flow | Headline | Knife | Entry |",
        "|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:12]:
        headline = round_or_none(row.get("headline_risk"))
        lines.append(
            f"| {row.get('rank')} | {row.get('symbol')} | {row.get('name') or '-'} | "
            f"{row.get('alpha_sleeve_id') or row.get('observed_lifecycle_sleeve_id') or 'rank_only'} | "
            f"{row.get('production_tier')} | {row.get('production_action')} | "
            f"{fmt_num(row.get('rank_score'))} | "
            f"{fmt_num(row.get('expected_r_t3'))} | "
            f"{fmt_num(row.get('lcb80_r_t3'))} | "
            f"{row.get('observed_probability_n') or '-'} | "
            f"{fmt_num(row.get('price_first_signal_score'), 0)} | "
            f"{fmt_num(row.get('informed_flow_score'), 0)} | "
            f"{fmt_num(None if headline is None else headline * 100.0, 0)} | "
            f"{fmt_num(row.get('falling_knife_score'), 0)} | "
            f"{row.get('observation_entry_zone') or '-'} |"
        )
    event_rows = [
        row
        for row in ranker.get("all_rows") or []
        if str(row.get("production_tier") or "") == "event_risk_watch"
    ]
    if event_rows:
        lines += ["", "Event-risk demotions:", ""]
        for row in event_rows[:8]:
            lines.append(
                f"- {row.get('symbol')} {row.get('name') or ''}: {row.get('latest_headline') or '-'}"
            )
    lines.append("")
    return lines


def render_cn_sector_narrative_section(payload: dict[str, Any]) -> list[str]:
    rows = (payload.get("cn") or {}).get("sector_narrative_screen") or []
    lines = [
        "## A 股板块叙事筛选 / CN Sector Narrative Screen",
        "",
        "技术量化先筛板块再筛个股：日常消费板块硬排除；AI infra、光通信/CPO、半导体封测材料、电力/电网和矿产/能源/重工给正向叙事分；互联网/软件只有明确 AI-infra 证据才提升。",
        "",
    ]
    if not rows:
        lines += ["- No sector leadership rows after narrative exclusions.", ""]
        return lines
    lines += [
        "| Rank | Sector | Narrative / Layer | Score | Names | Leaders | 5D | 1D | Breadth | Vol | Flow | Main flow | Why |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for idx, row in enumerate(rows[:15], start=1):
        lines.append(
            "| {rank} | {sector} | {narrative} | {score} | {names} | {leaders} | {ret5} | {pct} | {breadth} | {vol} | {flow} | {main_flow} | {why} |".format(
                rank=idx,
                sector=row.get("industry") or "-",
                narrative=f"{row.get('narrative_group') or '-'}/{row.get('supercycle_layer') or '-'}",
                score=fmt_num(row.get("sector_score"), 2),
                names=row.get("names") or 0,
                leaders=row.get("leader_count") or 0,
                ret5=fmt_pct(row.get("sector_ret_5d_pct")),
                pct=fmt_pct(row.get("sector_pct_chg")),
                breadth=fmt_pct((round_or_none(row.get("breadth")) or 0.0) * 100.0),
                vol=fmt_num(row.get("avg_amount_ratio"), 2),
                flow=fmt_num(row.get("avg_flow_intensity"), 4),
                main_flow=fmt_num(row.get("sector_main_net_in"), 2),
                why=clean_table_text(row.get("narrative_reason") or "", 70),
            )
        )
    lines.append("")
    return lines


def render_ai_lab_quality_index_section(payload: dict[str, Any], *, limit: int = 10) -> list[str]:
    index = payload.get("ai_lab_quality_index") or {}
    rows = index.get("rows") or []
    lines = [
        "## AI Lab Quality Index",
        "",
        "这是大模型/云/应用分发层的研究质量索引契约，目标是用 NeurIPS / ICML / ICLR / CVPR 的工业 lab 论文和开源栈质量做量化输入。当前如果没有 publication dataset，就只显示 data_required，不把 lab 名气硬塞进交易分数。",
        "",
    ]
    if not rows:
        lines += ["- No AI lab index seed rows.", ""]
        return lines
    lines += [
        "| Symbol | Company | Labs | Layer | Papers | Score | Status | Data requirement |",
        "|---|---|---|---|---:|---:|---|---|",
    ]
    for row in rows[:limit]:
        lines.append(
            f"| {row.get('symbol')} | {row.get('company')} | "
            f"{clean_table_text(', '.join(str(item) for item in row.get('labs') or []), 80)} | "
            f"{row.get('supercycle_layer') or '-'} | {row.get('paper_count_total') or '-'} | "
            f"{fmt_num(row.get('lab_quality_score'), 1)} | {row.get('data_status') or '-'} | "
            f"{clean_table_text(row.get('data_requirement') or '-', 120)} |"
        )
    lines.append("")
    return lines


def render_ai_supercycle_layer_attribution_section(
    payload: dict[str, Any],
    market: str | None = None,
    *,
    limit: int = 18,
) -> list[str]:
    attribution = payload.get("ai_supercycle_layer_attribution") or {}
    rows = attribution.get("rows") or []
    if market:
        rows = [row for row in rows if str(row.get("market") or "").upper() == market.upper()]
    summary = attribution.get("summary") or {}
    status = attribution.get("status") or {}
    title = "AI Supercycle Layer Attribution" if not market else f"{market.upper()} AI Supercycle Layer Attribution"
    if market and market.upper() == "US":
        note = "这张表回答“哪一层过去真的有过 sleeve alpha”，不是今日买入许可。美股来自 theme basket 历史收益。"
    elif market and market.upper() == "CN":
        note = "这张表回答“哪一层过去真的有过 sleeve alpha”，不是今日买入许可。A股来自 price/flow/tape leadership 历史收益，新闻仍只做滞后标签。"
    else:
        note = "这张表回答“哪一层过去真的有过 sleeve alpha”，不是今日买入许可。US 来自 theme basket 历史收益；CN 来自 price/flow/tape leadership 历史收益，新闻仍只做滞后标签。"
    if market:
        status_line = f"- status: {market.upper()}={status.get(market.upper()) or '-'}"
    else:
        status_line = f"- status: US={status.get('US') or '-'}; CN={status.get('CN') or '-'}"
    lines = [
        f"## {title}",
        "",
        note,
        "",
        f"- rows: {len(rows)}; positive LCB80 layers: {sum(1 for row in rows if (round_or_none(row.get('lcb80_pct')) or -999.0) > 0)}",
        status_line,
        "",
    ]
    if not rows:
        lines += ["- No layer attribution rows.", ""]
        return lines
    lines += [
        "| Market | Layer | Source | N | Active | Avg | LCB80 | Win | Confirm | Labels |",
        "|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows[:limit]:
        confirm = f"full {row.get('full_confirm', 0)}, proxy {row.get('proxy_confirm', 0)}"
        labels = ", ".join(str(item) for item in row.get("labels") or [])
        lines.append(
            f"| {row.get('market')} | {row.get('layer')} | {row.get('source')} | "
            f"{row.get('n') or 0} | {row.get('active_dates') or 0} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0.0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{confirm} | {clean_table_text(labels, 90)} |"
        )
    lines.append("")
    return lines
