"""CN standalone daily report renderer (Phase C.1 of REFACTOR_PLAN.md).

Extracted byte-identical from scripts/generate_main_strategy_v2_report.py.

Uses lazy import of the main monolith via the _m() proxy for callables
not yet extracted to lib/sections (market_actions / market_watch_rows /
render_market_action_table / render_cn_probability_picks_section have
wrappers in main).
"""
from __future__ import annotations

import importlib
from typing import Any

from lib.fmt import fmt_num, fmt_pct, fmt_r, narrative_label, round_or_none
from sections.realized_horizon import render_realized_horizon_edge_section
from sections.regime_views import render_risk_regime_section
from sections.left_side import render_cn_left_side_watch_section
from sections.audits_calendars import (
    render_earnings_calendar_section,
    render_source_review_calendar_section,
)
from sections.ai_supercycle import (
    render_ai_supercycle_evidence_section,
    render_ai_supercycle_value_radar_section,
)
from sections.selection_rationale import render_market_selection_rationale
from sections.cn_ranked_watch import render_cn_ranked_watch_radar_section

_main = None


def _m():
    global _main
    if _main is None:
        _main = importlib.import_module("generate_main_strategy_v2_report")
    return _main


def render_cn_standalone_report(payload: dict[str, Any]) -> str:
    m = _m()
    as_of = payload["as_of"]
    actions = m.market_actions(payload, "CN")
    summary = ((payload.get("production_decision_summary") or {}).get("summary") or {})
    sector_rows = (payload.get("cn") or {}).get("sector_narrative_screen") or []
    cn_r = fmt_r(summary.get('cn_r'))
    lines = [
        f"# A股量化日报 - {as_of}",
        "",
        f"今天 A 股给出 {len(actions)} 个执行候选,合计 {cn_r}。选股顺序是板块和资金先行,再落到个股 —— AI infra、矿产/能源/重工是主线,日常消费这次不纳入。",
        "",
    ]
    lines += m.render_cn_probability_picks_section(payload)
    lines += render_realized_horizon_edge_section(payload, "CN")
    lines += ["## 今天先看哪些板块", ""]
    if sector_rows:
        lines += [
            "| Rank | 板块 | 叙事/层 | 5D | 1D | 广度 | 领涨数 | 资金/成交 |",
            "|---:|---|---|---:|---:|---:|---:|---|",
        ]
        for idx, row in enumerate(sector_rows[:10], start=1):
            flow = f"vol {fmt_num(row.get('avg_amount_ratio'), 2)}, flow {fmt_num(row.get('avg_flow_intensity'), 4)}"
            lines.append(
                f"| {idx} | {row.get('industry') or '-'} | "
                f"{narrative_label(row.get('narrative_group'))} / {row.get('supercycle_layer') or '-'} | "
                f"{fmt_pct(row.get('sector_ret_5d_pct'))} | {fmt_pct(row.get('sector_pct_chg'))} | "
                f"{fmt_pct((round_or_none(row.get('breadth')) or 0.0) * 100.0)} | "
                f"{row.get('leader_count') or 0} | {flow} |"
            )
        lines.append("")
    elif actions:
        lines += ["- 已有 AI-infra 生产 universe 个股进入执行候选；板块层面今天没有形成新的行业级共振。", ""]
    else:
        lines += ["- 今天没有 AI-infra 板块同时通过叙事、tape 和可执行 sleeve gate。", ""]
    lines += [
        "## 可交易名单",
        "",
        "执行名单来自 AI-infra universe、source-review/关系账本和已推广 sleeve 的交集；broad-market 信号只作为背景。",
        "",
    ]
    lines += m.render_market_action_table(actions)
    lines += render_market_selection_rationale(payload, actions, "CN")
    lines += render_cn_ranked_watch_radar_section(payload)
    lines += render_cn_left_side_watch_section(payload)
    lines += render_risk_regime_section(payload, regime_key="cn_risk_regime")
    lines += render_ai_supercycle_evidence_section(payload, "CN", limit=10)
    lines += render_ai_supercycle_value_radar_section(payload, "CN", limit=8)
    lines += [
        "## 只观察或不碰",
        "",
    ]
    lines += m.render_market_watch_table(m.market_watch_rows(payload, "CN"))
    lines += render_earnings_calendar_section(payload, "CN")
    lines += render_source_review_calendar_section(payload, "CN")
    lines += m.render_benchmark_attribution_section(payload, "CN")
    lines += m.render_ai_book_attribution_section(payload, "CN")
    lines += [
        "## 风险口径",
        "",
        f"- CN long R: {fmt_r(summary.get('cn_r'))}",
        f"- Beta hedge: {fmt_r(summary.get('beta_hedge_r'))}",
        f"- Net beta after hedge: {fmt_r(summary.get('net_beta_r'))}",
        "- A 股新闻几乎都是滞后信号 —— 这里只把它当风险标签,真正决定入选的是价格、成交、资金和板块联动。",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"
