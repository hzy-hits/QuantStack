"""FactorLab research brief renderer (Phase C.2 part 2 of REFACTOR_PLAN.md).

Extracted byte-identical from scripts/generate_main_strategy_v2_report.py.

Smaller and more self-contained than render_report; only needs 2 helpers
from main (render_strategy_direction_table, render_adjustment_rules) +
3 constants, reached via lazy `_m()`.
"""
from __future__ import annotations

import importlib
from typing import Any

from lib.fmt import fmt_pct

_main = None


def _m():
    global _main
    if _main is None:
        _main = importlib.import_module("generate_main_strategy_v2_report")
    return _main


def render_factorlab_brief(payload: dict[str, Any]) -> str:
    m = _m()
    as_of = payload["as_of"]
    us = payload["us"]
    cn = payload["cn"]
    lifecycle = (cn.get("lifecycle") or {}).get("policy") or {}
    direction_lines = m.render_strategy_direction_table(payload.get("strategy_direction") or [])
    cn_tape_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == m.CN_TAPE_SLEEVE_ID
        for row in cn.get("current") or []
    )
    cn_ai_infra_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == m.CN_AI_INFRA_PRODUCTION_SLEEVE
        for row in cn.get("current") or []
    )
    cn_current_line = (
        f"- CN current execution: `{m.CN_TAPE_SLEEVE_ID}` active; A-share execution is right-side AI-infra tape leadership today"
        if cn_tape_ea
        else f"- CN current execution: `{m.CN_AI_INFRA_PRODUCTION_SLEEVE}` active; source-reviewed AI-infra production names control A-share execution today"
        if cn_ai_infra_ea
        else f"- CN oversold_contrarian LCB80: {fmt_pct(cn['metrics']['v2'].get('lcb80_pct'))}; freshness={cn.get('freshness', {}).get('v2', {}).get('state')}"
    )
    return "\n".join(
        [
            f"# FactorLab Main Strategy Research Brief - {as_of}",
            "",
            "## Research Question",
            "",
            f"当前主策略不应固定为 LOW、HIGH/MOD、趋势突破或均值回归。现在按 `{m.MAIN_STRATEGY_MODE}` 模式处理：当前 setup 优先进入机会池，EV/LCB/freshness 只决定排序和仓位提示。",
            "",
            "## Current Evidence Snapshot",
            "",
            f"- US V2 LOW/core/trending LCB80: {fmt_pct(us['metrics']['v2'].get('lcb80_pct'))}; freshness={us.get('freshness', {}).get('v2', {}).get('state')}",
            f"- US V2 stock-only net LCB80: {fmt_pct(us['metrics'].get('v2_stock_only_net', {}).get('lcb80_pct'))}; freshness={us.get('freshness', {}).get('v2_stock_only_net', {}).get('state')}",
            f"- US legacy HIGH/MOD LCB80: {fmt_pct(us['metrics']['legacy'].get('lcb80_pct'))}; freshness={us.get('freshness', {}).get('legacy', {}).get('state')}",
            cn_current_line,
            f"- CN lifecycle: best={lifecycle.get('best_bucket') or '-'}, max_hold=T+{lifecycle.get('max_hold_days') or '-'}, rule={lifecycle.get('follow_through_rule') or '-'}",
            "",
            "## Profit Objective",
            "",
            "赚钱目标优先于策略标签：FactorLab 必须把 post-cost、capital-weighted PnL、风险单位收益、最大回撤、换手/滑点和可成交性作为机会排序特征，而不是硬门槛。",
            "",
            "Promotion ladder: watch -> stock trade -> normal size；rolling LCB80、T+1、basket drawdown 和期权/flow 辅助证据只改变尺寸和优先级。",
            "",
            "A 股和美股分开裁决：美股 noisy/mean-reverting、A 股恐惧/高波都作为入场方式和仓位提示，不再作为阻断器。",
            "",
            "US bridge rule: 期权表达历史不足不拦股票；stock-only net-after-cost 决定股票交易，期权/flow 用来辅助排序和风险折扣。",
            "",
            "## Strategy Direction Board",
            "",
            *direction_lines,
            *m.render_adjustment_rules(),
            "## FactorLab Tasks",
            "",
            "1. 生成候选主策略族：trend_breakout、oversold_contrarian、event_second_day、early_accumulation、shadow_option_edge。",
            "2. 对每族输出 rolling 7/14/30/60D EV、LCB80、样本数、最大回撤、成交率、top1 concentration。",
            "3. 给出 freshness half-life：最近多长窗口还有 setup；LCB 只作为强弱读数。",
            "4. 给出主策略切换规则：什么时候从趋势切到均值回归，什么时候只降尺寸。",
            "5. 输出 next experiment：需要新增哪些特征或执行数据才能扩大机会尺寸。",
            "6. 在组合层报告 long alpha、beta hedge、net beta、行业暴露、相关簇、VaR95 和风险归因。",
            "7. 对 US options shadow ledger 分开评估 leg_quotes 与 proxy_bs 的 post-cost PnL、LCB80 和滑点敏感性；A 股 shadow option 仅作为风险折扣输入。",
            "",
            "## Guardrails",
            "",
            "- 不能因为 HIGH/MOD、CORE、结构核心这些标签本身而给正常仓位；只有生产交易层才能给股票 R。",
            "- 没有 T+1/T+2 真实退出的 A股结果不能算胜率。",
            "- 非 AI-infra broad-market 信号不能进入 AI-infra 生产 R，也不能阻拦 AI-infra sleeve。",
        ]
    ).rstrip() + "\n"
