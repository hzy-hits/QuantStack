"""Combined US+CN backtest report renderer (Phase C.2 of REFACTOR_PLAN.md).

Extracted byte-identical from scripts/generate_main_strategy_v2_report.py.

Uses full-lazy `_m()` proxy to the monolith for everything — render_report
touches ~30 helper functions, most still in main, so lazy delegation is
the lowest-risk extraction pattern.

Module-level constants (CN_TAPE_SLEEVE_ID etc) are also reached via _m()
so we don't need to duplicate them.
"""
from __future__ import annotations

import importlib
from typing import Any

from lib.fmt import fmt_pct
from sections.us_stock_decision_stack import render_us_stock_decision_stack_section

_main = None


def _m():
    global _main
    if _main is None:
        _main = importlib.import_module("generate_main_strategy_v2_report")
    return _main


def render_report(payload: dict[str, Any]) -> str:
    m = _m()
    us = payload["us"]
    cn = payload["cn"]
    limit_up = payload["limit_up"]
    as_of = payload["as_of"]
    start = payload["start"]
    us_v2 = us["metrics"]["v2"]
    us_legacy = us["metrics"]["legacy"]
    cn_v2 = cn["metrics"]["v2"]
    decision_summary = (payload.get("production_decision_summary") or {}).get("summary") or {}
    cn_tape_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == m.CN_TAPE_SLEEVE_ID
        for row in cn.get("current") or []
    )
    cn_ai_infra_ea_count = sum(
        1
        for row in cn.get("current") or []
        if row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == m.CN_AI_INFRA_PRODUCTION_SLEEVE
    )
    cn_evidence_label = (
        "CN tape leadership active; oversold evidence kept as secondary context"
        if cn_tape_ea
        else f"CN `{m.CN_AI_INFRA_PRODUCTION_SLEEVE}` current_EA {cn_ai_infra_ea_count}; broad-market signals out of scope"
        if cn_ai_infra_ea_count > 0
        else f"CN oversold_contrarian LCB80 {fmt_pct(cn_v2.get('lcb80_pct'))}"
    )
    conclusion = (
        f"今日生产动作：{decision_summary.get('headline') or 'no production action today'} "
        f"证据口径：US stock-net LCB80 {fmt_pct((us.get('metrics') or {}).get('v2_stock_only_net', {}).get('lcb80_pct'))}; "
        f"{cn_evidence_label}. "
        "0R 区：rank-only、事件风险、ST/退市类、非 AI-infra broad-market 信号、未闭环期权。"
    )

    lines: list[str] = [
        f"# Main Strategy V2 Backtest - {as_of}",
        "",
        f"Range: {start} to {as_of}.",
        "",
    ]
    lines += m.render_production_decision_summary(payload)
    lines += render_us_stock_decision_stack_section(payload)
    lines += m.render_fear_greed_section(payload)
    lines += m.render_earnings_calendar_section(payload, "US", limit=18)
    lines += m.render_earnings_calendar_section(payload, "CN", limit=18)
    lines += m.render_source_review_calendar_section(payload, "US", limit=12)
    lines += m.render_source_review_calendar_section(payload, "CN", limit=12)
    lines += m.render_satellite_pool_report_section(payload, limit_per_region=10)
    lines += m.render_benchmark_attribution_section(payload, "SATELLITE")
    lines += [
        "## 一句话结论",
        "",
        conclusion,
        "",
        "## 赚钱优先裁决 / Profit Guardrails",
        "",
        f"当前为 `{m.MAIN_STRATEGY_MODE}` 模式。净 EV、LCB80、回撤、新鲜期、样本覆盖和执行数据只影响仓位/优先级，不再把 A 股或美股机会硬拦成 0R。",
        "",
    ]
    lines += m.render_profit_guardrails(payload.get("profit_guardrails") or [])
    lines += m.render_profit_readiness_section(payload)
    lines += m.render_pipeline_requirements_audit_section(payload)
    lines += m.render_ai_supercycle_evidence_section(payload, limit=16)
    lines += m.render_ai_supply_chain_relationships_section(payload, limit=12)
    lines += m.render_ai_lab_quality_index_section(payload, limit=10)
    lines += m.render_ai_supercycle_value_radar_section(payload, limit=16)
    lines += m.render_ai_supercycle_layer_attribution_section(payload, limit=18)
    lines += [
        "## 策略方向裁决 / Strategy Direction",
        "",
        "这不是永久固化的配置，而是每天滚动重排的机会快照：哪个策略族有当前 setup、该给多大股票 R、哪些风险只作为提示。",
        "",
    ]
    lines += m.render_strategy_direction_table(payload.get("strategy_direction") or [])
    lines += m.render_adjustment_rules()
    lines += m.render_portfolio_risk_overlay_section(payload)
    lines += m.render_option_shadow_ledger_section(payload)
    lines += [
        "## 美股 V2 vs legacy",
        "",
        "US rule: `us_theme_cluster_momentum` is the main trend sleeve when a basket has breadth, price/volume and options/flow confirmation. Single-name V2 rows need their own fresh promotion; options/flow are decision evidence, not the traded instrument.",
        "",
    ]
    lines += m.render_metrics_table(
        [
            us["metrics"]["v2"],
            us["metrics"]["v2_stock_only_net"],
            us["metrics"]["v2_options_confirmed"],
            us["metrics"]["legacy"],
        ]
    )
    lines += [
        "",
        f"- Current US candidate date: {us.get('current_date') or '-'}",
        f"- Options rows available for latest screen: {us.get('options_coverage_rows', 0)}",
        f"- Stock-only bridge: subtracts {m.US_STOCK_ROUNDTRIP_COST_PCT:.2f}% roundtrip cost from the underlying 3-session result; this supports stock trades when the production ranker emits Execution Alpha.",
        "- HIGH/MOD single-name legacy rows stay ranked watch unless they are pulled into a promoted theme basket.",
        "",
    ]
    lines += m.render_us_opportunity_ranker_section(payload)
    lines += m.render_gamma_spring_section(payload)
    lines += m.render_missed_alpha_radar(us.get("missed_alpha_radar") or [])
    lines += [
        "## 策略新鲜期 / Freshness",
        "",
        "主策略不是永久身份。这里用滚动 7/14/30/45/60 日窗口重新计算 EV/LCB 作为机会新鲜度提示；不再作为硬拦截。",
        "",
    ]
    lines += m.render_freshness_table("US freshness", us.get("freshness") or {})
    lines += m.render_freshness_table("CN freshness", cn.get("freshness") or {})
    lines += [
        "## A 股 V2",
        "",
        "V2 rule: oversold_contrarian with real T+1/T+2 exits and Tobit limit-censored volatility as risk unit. For A-shares, fear/high-vol is often the contrarian edge context, so it clips size and enforces pullback-only execution instead of copying the US trend blocker.",
        "",
    ]
    lines += m.render_metrics_table([cn["metrics"]["v2"], cn["metrics"]["v2_all_oversold_diagnostic"]])
    lines += [
        "",
        f"- Current CN candidate date: {cn.get('current_date') or '-'}",
        "- A-share T+1 note: same-day exit is not counted as a valid realized exit; current-day rows can remain pending.",
        "",
    ]
    lines += m.render_cn_sector_narrative_section(payload)
    lines += m.render_cn_opportunity_ranker_section(payload)
    lines += m.render_cn_lifecycle_section(cn)
    lines += [
        "## 最近候选表现",
        "",
        "| Market | Symbol | Name | State | Policy | Note |",
        "|---|---|---|---|---|---|",
    ]
    for row in m.recent_outcomes(us, cn):
        lines.append(
            f"| {row['market'].upper()} | {row.get('symbol')} | {row.get('name') or '-'} | "
            f"{row.get('state')} | {row.get('policy')} | {row.get('reason')} |"
        )
    lines += [
        "",
        "## 当前可执行 / 只观察 / 被拦截",
        "",
        "### US",
        "",
    ]
    lines += m.render_current_table(us.get("current") or [], "us")
    lines += ["### CN", ""]
    lines += m.render_current_table(cn.get("current") or [], "cn")
    lines += [
        "## 下一步需要的数据",
        "",
        "- US: keep real option expression history with selected legs so V2 options have true bid/ask leg PnL coverage.",
        "- US: persist `options_chain_quotes` daily so option shadow ledger can move from proxy to true bid/ask leg PnL.",
        "- Portfolio: keep sector/industry tags, stock/index/futures price history, hedge fills and residual beta attribution complete enough for long alpha + beta hedge sizing.",
        "- CN: keep source-reviewed AI-infra universe, relationship ledger, K-line/flow features, lifecycle labels, and execution fills complete enough for sleeve sizing.",
        "- CN: keep fill_date/exit_date/max_favorable/max_adverse in `strategy_model_dataset`; lifecycle gate now depends on T+1/T+3/T+5 bucket evidence.",
        "- CN: shadow option fields remain risk haircuts only until there is a real listed option/futures expression and executable quote history.",
        "- CN: keep Tobit volatility and market fear/high-vol fields in candidate exports; they drive risk unit and admission.",
    ]
    return "\n".join(lines).rstrip() + "\n"
