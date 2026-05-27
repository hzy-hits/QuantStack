"""Audits + calendars + portfolio + attribution sections (Phase B.10).

Batch of 8 small/medium sections — all clean (only lib.fmt deps):
  - render_profit_readiness_section
  - render_pipeline_requirements_audit_section
  - render_portfolio_risk_overlay_section
  - render_option_shadow_ledger_section
  - render_earnings_calendar_section (+ _fmt_eps helper)
  - render_source_review_calendar_section (+ _READINESS_TIER_ORDER)
  - render_ai_book_attribution_section
  - render_benchmark_attribution_section
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from lib.fmt import (
    as_iso, clean_table_text, fmt_bool, fmt_num, fmt_pct, fmt_r, round_or_none,
)


_READINESS_TIER_ORDER = (
    "ready_for_promotion",
    "evidence_partial",
    "pending_human_review",
    "blocked_by_counterevidence",
    "g0_blocked",
    "unscored",
)


def _fmt_eps(value: Any) -> str:
    parsed = round_or_none(value, 4)
    if parsed is None:
        return "-"
    return f"{parsed:.4g}"


def render_profit_readiness_section(payload: dict[str, Any]) -> list[str]:
    readiness = payload.get("profit_readiness") or {}
    lines = [
        "## 赚钱落地缺口 / Profit Readiness",
        "",
        "这里专门回答“还差什么才能把研究 edge 变成可控实盘 PnL”。",
        "",
        "| Priority | Area | State | Allowed now | Blocker | Next step |",
        "|---:|---|---|---|---|---|",
    ]
    for row in readiness.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('allowed_now')} | {row.get('blocker')} | {row.get('next_step')} |"
        )
    lines.append("")
    return lines


def render_pipeline_requirements_audit_section(payload: dict[str, Any]) -> list[str]:
    audit = payload.get("pipeline_requirements_audit") or {}
    summary = audit.get("summary") or {}
    lines = [
        "## 管线需求审计 / Pipeline Requirements Audit",
        "",
        "这里专门回答“这套管线有没有实际用”：fail 表示可以观察/排序，但不能把它写成可执行 alpha。",
        "",
        f"- Fail count: `{summary.get('fail_count', 0)}`",
        f"- Top blocker: {summary.get('top_blocker') or '-'}",
        f"- Production bias: {summary.get('production_bias') or '-'}",
        "",
        "| Priority | Area | State | Evidence | Next change |",
        "|---:|---|---|---|---|",
    ]
    for row in audit.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('evidence')} | {row.get('next_change')} |"
        )
    lines.append("")
    return lines


def render_portfolio_risk_overlay_section(payload: dict[str, Any]) -> list[str]:
    overlay = payload.get("portfolio_risk_overlay") or {}
    summary = overlay.get("summary") or {}
    lines = [
        "## 组合风险覆盖 / Portfolio Risk Overlay",
        "",
        "这里不是重新选股，也不是硬拦截器；它把当前机会映射成 long stock alpha、beta hedge、剩余 beta 和风险归因。",
        "",
        f"- Current opportunity candidates: {summary.get('candidate_count', 0)}",
        f"- Long alpha R: {fmt_num(summary.get('long_alpha_r'), 4)}",
        f"- Planned beta hedge R: {fmt_num(summary.get('beta_hedge_r'), 4)}",
        f"- Net beta R after hedge: {fmt_num(summary.get('net_beta_r'), 4)}",
        f"- VaR95 R proxy: {fmt_num(summary.get('var95_r_proxy'), 4)}",
        f"- Hedged VaR95 R proxy: {fmt_num(summary.get('hedged_var95_r_proxy'), 4)}",
        f"- Warning references only: total {fmt_num(summary.get('total_cap_r'), 2)}R, sector {fmt_num(summary.get('sector_cap_r'), 2)}R, correlation cluster {fmt_num(summary.get('corr_cluster_cap_r'), 2)}R",
        "",
    ]
    hedge_book = summary.get("hedge_book") or []
    if hedge_book:
        lines.append(
            "- Hedge book: "
            + "; ".join(
                f"{row.get('market')} short {row.get('instrument')} {fmt_num(row.get('hedge_notional_r'), 4)}R ({row.get('names')} names)"
                for row in hedge_book
            )
        )
        lines.append("")
    attribution = summary.get("risk_attribution") or {}
    if attribution:
        lines += [
            f"- Risk attribution: single-name max {fmt_r(attribution.get('single_name_max_r'))}; sector max {fmt_r(attribution.get('sector_max_r'))}; corr-cluster max {fmt_r(attribution.get('correlation_cluster_max_r'))}; hedge offset {fmt_r(attribution.get('hedge_offset_r'))}; idiosyncratic alpha proxy {fmt_r(attribution.get('idiosyncratic_alpha_r'))}",
            "",
        ]
    if summary.get("hedge_basis_risk"):
        lines += [
            f"- Hedge basis risk: hedged VaR proxy is +{fmt_num(attribution.get('basis_risk_delta_r'), 4)}R above unhedged; hedge lowers market beta, not single-name/basis risk.",
            "",
        ]
    rows = overlay.get("rows") or []
    if not rows:
        lines += ["- No current candidates found for opportunity sizing.", ""]
        return lines
    lines += [
        "| Market | Symbol | State | Sector | Base R | Long R | Hedge | Beta | Net beta R | Auto | Shadow haircut | Reasons |",
        "|---|---|---|---|---:|---:|---|---:|---:|---|---:|---|",
    ]
    for row in rows[:20]:
        lines.append(
            f"| {row.get('market')} | {row.get('symbol')} | {row.get('state')} | {row.get('sector')} | "
            f"{fmt_num(row.get('base_r'), 4)} | {fmt_num(row.get('final_r'), 4)} | "
            f"{row.get('hedge_instrument') or '-'} {fmt_num(row.get('hedge_notional_r'), 4)}R | {fmt_num(row.get('hedge_beta'), 2)} | "
            f"{fmt_r(row.get('net_beta_r'))} | {fmt_bool(bool(row.get('auto_eligible')))} | "
            f"{fmt_num(row.get('shadow_option_haircut'), 2)} | {', '.join(row.get('risk_reasons') or [])} |"
        )
    lines.append("")
    return lines


def render_option_shadow_ledger_section(payload: dict[str, Any]) -> list[str]:
    ledger = payload.get("option_shadow_ledger") or {}
    overall = ((ledger.get("summary") or {}).get("overall_long") or {})
    real = ((ledger.get("summary") or {}).get("real_bid_ask_options") or {})
    all_real = ((ledger.get("summary") or {}).get("all_options_alpha_real_bid_ask") or {})
    lines = [
        "## US Option Shadow PnL Ledger",
        "",
        "美股期权/flow 只作为股票决策辅助证据：真实 `options_chain_quotes` 的 entry/exit bid/ask 双腿 PnL 只用于诊断期权表达质量，不是股票交易的硬 blocker。",
        "",
        f"- Real bid/ask leg rows: {ledger.get('real_bid_ask_resolved_count', 0)}",
        f"- All options_alpha real bid/ask rows: {ledger.get('all_real_bid_ask_resolved_count', 0)} resolved / {ledger.get('all_real_bid_ask_unresolved_count', 0)} unresolved",
        f"- Proxy rows: {ledger.get('proxy_resolved_count', 0)}",
        f"- Stock proxy rows: {ledger.get('stock_proxy_resolved_count', 0)}",
        f"- Unresolved rows: {ledger.get('unresolved_count', 0)}",
        f"- Rows with persisted legs: {ledger.get('rows_with_legs', 0)}",
        f"- Real bid/ask LCB80: {fmt_pct(real.get('lcb80_pct'))}",
        f"- All options_alpha real bid/ask LCB80: {fmt_pct(all_real.get('lcb80_pct'))}",
        f"- Long-expression n: {overall.get('n', 0)}",
        f"- Long-expression LCB80: {fmt_pct(overall.get('lcb80_pct'))}",
        "",
    ]
    rows = ledger.get("rows") or []
    if not rows:
        lines += ["- No V2 US rows available for option shadow marking.", ""]
        return lines
    lines += [
        "| Date | Symbol | Expression | Pricing mode | Real bid/ask | Underlying | Option return | Reason |",
        "|---|---|---|---|---|---:|---:|---|",
    ]
    for row in rows[:20]:
        lines.append(
            f"| {row.get('report_date')} | {row.get('symbol')} | {row.get('expression')} | "
            f"{row.get('pricing_mode')} | {fmt_bool(bool(row.get('real_bid_ask_resolved')))} | "
            f"{fmt_pct(row.get('underlying_return_pct'))} | "
            f"{fmt_pct(row.get('return_pct'))} | {row.get('reason')} |"
        )
    lines.append("")
    return lines


def render_earnings_calendar_section(payload: dict[str, Any], market: str, *, limit: int = 20) -> list[str]:
    calendar = (payload.get("earnings_calendar") or {}).get(market.lower()) or {}
    rows = calendar.get("rows") or []
    title = "美股财报日历" if market.upper() == "US" else "A股财报披露日历"
    lines = [
        f"## {title}",
        "",
        f"- 窗口: {calendar.get('window') or '-'}；状态: `{calendar.get('status') or 'unknown'}`；"
        f"范围: `{calendar.get('scope') or 'unknown'}`（{calendar.get('focus_symbol_count') or 0} 个报告内代码）。",
        "- 财报日期是催化剂/风险时钟,不构成把 watch 升级成交易票的理由。",
        "",
    ]
    if market.upper() == "US":
        lines += [
            "| 类型 | 代码 | 名称 | 日期 | 财期 | EPS预估 | EPS实际 | Surprise |",
            "|---|---|---|---|---|---:|---:|---:|",
        ]
        if rows:
            for row in rows[:limit]:
                lines.append(
                    f"| {row.get('focus') or '-'} | {row.get('symbol') or '-'} | "
                    f"{clean_table_text(row.get('display_name') or row.get('name') or '-', 42)} | "
                    f"{as_iso(row.get('report_date')) or '-'} | {row.get('fiscal_period') or '-'} | "
                    f"{_fmt_eps(row.get('estimate_eps'))} | {_fmt_eps(row.get('actual_eps'))} | "
                    f"{fmt_pct(row.get('surprise_pct'))} |"
                )
        else:
            lines.append("| - | - | 无未来/近期重点财报 | - | - | - | - | - |")
    else:
        lines += [
            "| 类型 | 代码 | 名称 | 报告期 | 预约日 | 实际日 |",
            "|---|---|---|---|---|---|",
        ]
        if rows:
            for row in rows[:limit]:
                lines.append(
                    f"| {row.get('focus') or '-'} | {row.get('symbol') or '-'} | "
                    f"{clean_table_text(row.get('display_name') or row.get('name') or '-', 36)} | "
                    f"{as_iso(row.get('fiscal_period')) or '-'} | {row.get('pre_date') or '-'} | "
                    f"{row.get('actual_date') or '-'} |"
                )
        else:
            lines.append("| - | - | 无今日/近期重点披露 | - | - | - |")
    lines.append("")
    return lines


def render_ai_book_attribution_section(payload: dict[str, Any], market: str) -> list[str]:
    book = ((payload.get("benchmark_attribution") or {}).get("ai_book") or {}).get(market.lower()) or {}
    title = "US AI Book vs Benchmark" if market.upper() == "US" else "A股 AI Book vs Benchmark"
    rows = book.get("rows") or []
    basket_size = book.get("basket_size") or 0
    lines = [
        f"## {title}",
        "",
        f"- 状态: `{book.get('status') or 'unknown'}`；equal-weight 篮子规模 {basket_size}；window 取 20d / 60d 滚动。",
        "- 日度收益对 benchmark 的回归(alpha/beta/IR),样本期短,只能当 sanity check —— 不是完整的风险归因。",
        "",
    ]
    if not rows:
        if basket_size == 0:
            lines += ["- 当前 production basket 为空，无 AI book attribution 行。", ""]
        else:
            lines += ["- 缺少 benchmark / book 价格数据，无法计算。", ""]
        return lines
    lines += [
        "| Benchmark | Window | N | Active Return | Daily Alpha | Beta | IR |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        active = row.get("active_return_pct")
        alpha = row.get("alpha_daily_pct")
        beta_val = row.get("beta")
        info = row.get("information_ratio")
        lines.append(
            f"| {row.get('benchmark_label') or row.get('benchmark')} | "
            f"{row.get('window')} | {row.get('n') or 0} | "
            f"{fmt_pct(active) if active is not None else '-'} | "
            f"{fmt_pct(alpha) if alpha is not None else '-'} | "
            f"{fmt_num(beta_val) if beta_val is not None else '-'} | "
            f"{fmt_num(info) if info is not None else '-'} |"
        )
    lines.append("")

    risk = book.get("risk") or {}
    if risk:
        dd20 = risk.get("max_drawdown_20d_pct")
        dd60 = risk.get("max_drawdown_60d_pct")
        atr = risk.get("avg_atr20_pct")
        corr20 = risk.get("pairwise_corr_20d") or {}
        corr60 = risk.get("pairwise_corr_60d") or {}
        lines += [
            "### Risk block",
            "",
            f"- Max drawdown 20d / 60d: {fmt_pct(dd20) if dd20 is not None else '-'} / {fmt_pct(dd60) if dd60 is not None else '-'}",
            f"- 篮子成员 ATR20 (close-to-close) 均值: {fmt_pct(atr) if atr is not None else '-'}",
            f"- 篮子内 20d 配对相关: mean {fmt_num(corr20.get('mean'))}, max {fmt_num(corr20.get('max'))}, min {fmt_num(corr20.get('min'))}, n_pairs {corr20.get('n_pairs') or 0}",
            f"- 篮子内 60d 配对相关: mean {fmt_num(corr60.get('mean'))}, max {fmt_num(corr60.get('max'))}, min {fmt_num(corr60.get('min'))}, n_pairs {corr60.get('n_pairs') or 0}",
            "",
        ]
    return lines


def render_benchmark_attribution_section(payload: dict[str, Any], market: str, *, limit: int = 10) -> list[str]:
    data = (payload.get("benchmark_attribution") or {}).get(market.lower()) or {}
    rows = data.get("rows") or []
    if market.upper() == "US":
        title = "US Benchmark Snapshot"
        note = (
            "benchmark 提供 macro/beta context 和归因基线,自身不会进 production candidate。"
            " 主要看 AI book 相对 SPY/QQQ/SMH 或对应指数的方向。"
        )
    elif market.upper() == "CN":
        title = "A股 Benchmark Snapshot"
        note = (
            "benchmark 只做 macro/beta context 和归因基线,不进 production candidate。"
            " A股 attribution 主要看 AI book 相对 沪深300/创业板指/深成指/上证指数 的方向。"
        )
    else:
        title = "Satellite Benchmark Snapshot (TW/JP/KR/EU)"
        note = (
            "卫星 benchmark 覆盖 TSMC/HBM/CoWoS/ABF/AEX 设备这条卫星资产池,提供 macro context。"
            " ^TWII/^N225/^KS11/^AEX 是本地指数；EWT/EWJ/EWY/EWN 是 US-listed ETF 镜像。"
            " 不能作为 production candidate。"
        )
    missing = data.get("missing") or []
    lines = [
        f"## {title}",
        "",
        note,
        "",
    ]
    if missing:
        lines.append(f"- 缺数据: {', '.join(missing)}")
        lines.append("")
    lines += [
        "| Symbol | Latest Date | Close | 1D | 5D | 20D | 60D | YTD |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    if rows:
        for row in rows[:limit]:
            if row.get("status") != "ok":
                lines.append(
                    f"| {row.get('label') or row.get('symbol')} | - | - | - | - | - | - | - |"
                )
                continue
            lines.append(
                f"| {row.get('label') or row.get('symbol')} | {row.get('latest_date') or '-'} | "
                f"{fmt_num(row.get('latest_close'), 2)} | {fmt_pct(row.get('ret_1d_pct'))} | "
                f"{fmt_pct(row.get('ret_5d_pct'))} | {fmt_pct(row.get('ret_20d_pct'))} | "
                f"{fmt_pct(row.get('ret_60d_pct'))} | {fmt_pct(row.get('ret_ytd_pct'))} |"
            )
    else:
        lines.append("| - | - | - | - | - | - | - | - |")
    lines.append("")
    return lines


def render_source_review_calendar_section(
    payload: dict[str, Any],
    market: str,
    *,
    limit: int = 12,
) -> list[str]:
    calendar = (payload.get("source_review_calendar") or {}).get(market.lower()) or {}
    rows = calendar.get("rows") or []
    title = "AI Infra Source Review Calendar (US)" if market.upper() == "US" else "AI Infra Source Review Calendar (A股)"
    queue_path = calendar.get("queue_path") or "ai_infra/reports/source_verification_queue_v1.csv"
    tier_counts: Counter[str] = Counter()
    for row in rows:
        tier_counts[row.get("readiness_tier") or "unscored"] += 1
    summary_chunks = [f"{tier}={tier_counts.get(tier, 0)}" for tier in _READINESS_TIER_ORDER if tier_counts.get(tier, 0)]
    summary_text = "; ".join(summary_chunks) if summary_chunks else "all rows unscored"
    lines = [
        f"## {title}",
        "",
        f"- 数据源: `{queue_path}`；状态: `{calendar.get('status') or 'unknown'}`；"
        f"范围: `{calendar.get('scope') or 'unknown'}` (focus 命中 {calendar.get('focus_match_count') or 0} / {calendar.get('focus_symbol_count') or 0})。",
        f"- Readiness 分布: {summary_text}",
        "- `ready_for_promotion` 意味着 evidence card 模板写齐、evidence_state 含「原文已证明」;其他 tier 仍要人工核验。没有 evidence card 的名字不会晋级为 production candidate。",
        "",
        "| Tier | Ticker | Company | Depth | Module | Readiness | Tape (EMA21/50) | Market Context |",
        "|---|---|---|---|---|---|---|---|",
    ]
    if rows:
        for row in rows[:limit]:
            ticker = row.get("primary_ticker") or row.get("ticker") or "-"
            readiness = row.get("readiness_tier") or "unscored"
            readiness_score = row.get("readiness_score")
            score_text = f" ({readiness_score:.2f})" if isinstance(readiness_score, (int, float)) else ""
            lines.append(
                "| "
                + " | ".join(
                    [
                        row.get("priority_tier") or "-",
                        ticker,
                        clean_table_text(row.get("company") or "-", 26),
                        row.get("bfs_depth") or "-",
                        clean_table_text(row.get("module") or "-", 28),
                        f"{readiness}{score_text}",
                        clean_table_text(row.get("ema_summary") or "no_data", 48),
                        clean_table_text(row.get("market_context_notes") or "-", 44),
                    ]
                )
                + " |"
            )
    else:
        lines.append("| - | - | 无待核验候选 | - | - | - | - | - |")
    lines.append("")
    return lines
