"""US standalone daily report renderer (Phase C.0 of REFACTOR_PLAN.md).

Extracted byte-identical from scripts/generate_main_strategy_v2_report.py.

Uses lazy import of the main monolith via a small proxy so any callable
not yet extracted to lib/sections still resolves correctly without
triggering circular imports. (The main monolith imports
`render_us_standalone_report` from this module at the bottom of its
own load sequence, so by the time the function is actually invoked,
the main module is fully initialized.)
"""
from __future__ import annotations

import importlib
from typing import Any

from lib.fmt import fmt_pct, fmt_r
from sections.iv_view import render_iv_view_section
# render_us_left_side_section is a wrapper in main (injects
# US_MEAN_REVERSION_ROOT); reach it via lazy proxy m.render_us_left_side_section.
from sections.options_radar import (
    render_options_anomaly_section,
    render_options_tenor_section,
)
from sections.probability_picks import render_us_probability_picks_section
from sections.realized_horizon import render_realized_horizon_edge_section
from sections.regime_views import render_fear_greed_section, render_risk_regime_section
from sections.serenity import render_serenity_crosscheck_section
from sections.market_regime import render_market_regime_score_section
from sections.top10_daily import render_us_top10_daily_section
from sections.audits_calendars import (
    render_earnings_calendar_section,
    render_source_review_calendar_section,
)
from sections.ai_supercycle import (
    render_ai_supercycle_evidence_section,
    render_ai_supercycle_value_radar_section,
    render_ai_supply_chain_relationships_section,
)
from sections.selection_rationale import render_market_selection_rationale

_main = None


def _m():
    global _main
    if _main is None:
        _main = importlib.import_module("generate_main_strategy_v2_report")
    return _main


def render_us_standalone_report(payload: dict[str, Any]) -> str:
    m = _m()
    as_of = payload["as_of"]
    actions = m.market_actions(payload, "US")
    summary = ((payload.get("production_decision_summary") or {}).get("summary") or {})
    us = payload.get("us") or {}
    us_r = fmt_r(summary.get('us_r'))
    if actions:
        headline = (
            f"今天美股共 {len(actions)} 个执行候选,合计 {us_r}。主线按主题 basket 跑,强主题不再压成纯 watch。"
            "期权/flow 用来交叉验证股票 timing 和风险。"
        )
    else:
        headline = (
            f"今天美股没有股票执行候选,合计 {us_r}。下面保留 ranker、新闻和期权定位,用于观察下一次开盘。"
        )
    lines = [
        f"# 美股量化日报 - {as_of}",
        "",
        headline,
        "",
    ]
    lines += m.render_us_execution_gate_notice(payload)
    lines += render_realized_horizon_edge_section(payload, "US")
    us_gate = (
        ((payload.get("production_decision_summary") or {}).get("summary") or {}).get("us_execution_gate")
        or m.evaluate_us_execution_gate(payload)
    )
    lines += render_us_probability_picks_section(payload, actions=actions, us_gate=us_gate)
    lines += render_us_top10_daily_section(payload, actions=actions)
    lines += ["## 可交易名单", ""]
    lines += m.render_market_action_table(actions)
    lines += render_market_selection_rationale(payload, actions, "US")
    lines += m.render_us_left_side_section(payload)
    lines += render_iv_view_section(payload)
    lines += render_risk_regime_section(payload)
    lines += render_fear_greed_section(payload)
    lines += render_market_regime_score_section(payload)
    lines += render_serenity_crosscheck_section(payload)
    lines += render_options_anomaly_section(payload)
    lines += render_options_tenor_section(payload)
    lines += m.render_bubble_hedge_section(payload)
    lines += [
        "## 主题和证据",
        "",
        f"- Current candidate date: {us.get('current_date') or '-'}",
        f"- Options rows available: {us.get('options_coverage_rows', 0)}",
        f"- US stock bridge LCB80: {fmt_pct(((us.get('metrics') or {}).get('v2_stock_only_net') or {}).get('lcb80_pct'))}",
        "- 主题 basket 需要持续复核：如果广度收缩、期权/flow 退潮或新闻风险升高，股票 R 应该下调或退出。",
        "",
    ]
    lines += render_ai_supercycle_evidence_section(payload, "US", limit=10)
    lines += render_ai_supply_chain_relationships_section(payload, limit=8)
    lines += m.render_ai_lab_quality_index_section(payload, limit=8)
    lines += render_ai_supercycle_value_radar_section(payload, "US", limit=8)
    lines += m.render_ai_supercycle_layer_attribution_section(payload, "US", limit=10)
    lines += [
        "## 只观察或不碰",
        "",
    ]
    lines += m.render_market_watch_table(m.market_watch_rows(payload, "US"))
    lines += render_earnings_calendar_section(payload, "US")
    lines += render_source_review_calendar_section(payload, "US")
    lines += m.render_benchmark_attribution_section(payload, "US")
    lines += m.render_ai_book_attribution_section(payload, "US")
    lines += [
        "## 风险口径",
        "",
        f"- US long R: {fmt_r(summary.get('us_r'))}",
        f"- Beta hedge: {fmt_r(summary.get('beta_hedge_r'))}",
        f"- Net beta after hedge: {fmt_r(summary.get('net_beta_r'))}",
        "- Options remain auxiliary: real bid/ask option PnL ledger is diagnostic, not the stock-trade blocker.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"
