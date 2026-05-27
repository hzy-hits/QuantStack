"""Pure table renderers (Phase B.11).

Extracted from scripts/generate_main_strategy_v2_report.py — 7 self-
contained markdown-table renderers used in the daily report. All only
depend on lib.fmt; no DB, no other section dependencies.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text, fmt_num, fmt_pct


def render_metrics_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Strategy | n | Active days | Avg | Median | Win | EV LCB80 | Trade Sharpe | Daily Sharpe | Max DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['n']} | {row['active_dates']} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('median_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_pct(row.get('lcb80_pct'))} | {fmt_num(row.get('trade_sharpe'), 2)} | "
            f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_pct(row.get('max_drawdown_pct'))} |"
        )
    return lines


def render_current_table(rows: list[dict[str, Any]], market: str) -> list[str]:
    if not rows:
        return [f"- {market.upper()}: none.", ""]
    if market == "us":
        lines = [
            "| State | Symbol | Rank | Tier | Action | Buy/Review | Stop | Target | Option expression | Trend | Time exit | Why |",
            "|---|---|---:|---|---|---:|---:|---:|---|---|---|---|",
        ]
        for row in rows[:12]:
            lines.append(
                f"| {row['state']} | {row['symbol']} | {row.get('production_rank') or '-'} | "
                f"{row.get('production_tier') or '-'} | {row.get('production_action') or '-'} | "
                f"{fmt_num(row.get('entry'))} | "
                f"{fmt_num(row.get('stop'))} | {fmt_num(row.get('target'))} | "
                f"{row.get('option_expression') or '-'} | {row.get('trend_regime') or '-'} | "
                f"{row.get('time_exit')} | {row.get('reason')} |"
            )
        lines.append("")
        return lines

    lines = [
        "| State | Code | Name | Rank | Tier | Action | ExpR | LCBR | Obs n | Observation entry | Handling line | First target | EV | EV80 | Evidence context | Log overlay | Lifecycle action | Time exit | T+1 risk |",
        "|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|",
    ]
    for row in rows[:16]:
        lines.append(
            f"| {row['state']} | {row['symbol']} | {row.get('name') or '-'} | "
            f"{row.get('production_rank') or '-'} | {row.get('production_tier') or '-'} | "
            f"{row.get('production_action') or '-'} | "
            f"{fmt_num(row.get('expected_r_t3'))} | {fmt_num(row.get('lcb80_r_t3'))} | "
            f"{row.get('observed_probability_n') or '-'} | "
            f"{row.get('observation_entry_zone') or '-'} | {fmt_num(row.get('handling_line'))} | "
            f"{fmt_num(row.get('first_target'))} | {fmt_pct(row.get('ev_pct'))} | "
            f"{fmt_pct(row.get('ev_lcb80_pct'))} | {row.get('gate_summary') or '-'} | "
            f"{row.get('log_denoise_overlay') or '-'} | "
            f"{row.get('lifecycle_action') or '-'} | "
            f"{row.get('time_exit')} | {row.get('t1_risk')} |"
        )
    lines.append("")
    return lines


def render_missed_alpha_radar(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## US Missed Alpha / Winner Hold Radar",
        "",
        "这些是 missed-alpha / winner-hold 机会提示。追高、低 R:R、noisy/mean-reverting 只作为入场方式提示；如果价格给 pullback/retest，可以进入股票交易复核。",
        "",
    ]
    if not rows:
        lines += ["- No missed-alpha radar rows today.", ""]
        return lines
    lines += [
        "| State | Symbol | Confidence | Fresh entry | Hold overlay | Pullback/retest | Stop | R:R | Trend | Blockers |",
        "|---|---|---|---|---|---:|---:|---:|---|---|",
    ]
    for row in rows[:30]:
        blockers = ", ".join(str(item) for item in (row.get("blockers") or []) if item)
        lines.append(
            f"| {row.get('state')} | {row.get('symbol')} | {row.get('signal_confidence') or '-'} | "
            f"{row.get('fresh_entry_action')} | {row.get('hold_action')} | "
            f"{fmt_num(row.get('pullback_price') or row.get('entry'))} | {fmt_num(row.get('stop'))} | "
            f"{fmt_num(row.get('rr_ratio'))} | {row.get('trend_regime') or '-'} | {blockers or row.get('reason') or '-'} |"
        )
    lines.append("")
    return lines


def render_limit_table(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- No limit-up radar rows.", ""]
    lines = [
        "| State | Code | Name | p_limit_up | p_touch_limit | 5D | 20D | Raw touch | EV after cost | Top decile | Model state |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:20]:
        lines.append(
            f"| Limit-Up Radar | {row.get('symbol')} | {row.get('name') or '-'} | "
            f"{fmt_pct((row.get('p_limit_up') or 0) * 100.0)} | "
            f"{fmt_pct((row.get('p_touch_limit') or 0) * 100.0)} | "
            f"{fmt_pct(row.get('ret_5d'))} | {fmt_pct(row.get('ret_20d'))} | "
            f"{fmt_pct((row.get('raw_p_touch_limit') or 0) * 100.0)} | "
            f"{fmt_pct(row.get('ev_after_cost_pct'))} | {row.get('probability_decile')} | "
            f"{row.get('model_state')} |"
        )
    lines.append("")
    return lines


def render_freshness_table(title: str, freshness: dict[str, Any]) -> list[str]:
    lines = [f"### {title}", ""]
    rows = [
        ("V2", freshness.get("v2") or {}),
    ]
    if freshness.get("v2_stock_only_net"):
        rows.append(("V2 stock-only net", freshness.get("v2_stock_only_net") or {}))
    rows.append(("Legacy baseline", freshness.get("legacy") or {}))
    lines += [
        "| Strategy | Freshness state | Effective window | Rule | 7D LCB80 | 14D LCB80 | 30D LCB80 |",
        "|---|---|---:|---|---:|---:|---:|",
    ]
    for label, data in rows:
        by_window = {row.get("window_days"): row for row in data.get("windows") or []}
        effective = data.get("freshness_days")
        lines.append(
            f"| {label} | {data.get('state') or '-'} | {effective or '-'} | {data.get('rule') or '-'} | "
            f"{fmt_pct((by_window.get(7) or {}).get('lcb80_pct'))} | "
            f"{fmt_pct((by_window.get(14) or {}).get('lcb80_pct'))} | "
            f"{fmt_pct((by_window.get(30) or {}).get('lcb80_pct'))} |"
        )
    lines.append("")
    return lines


def render_cn_lifecycle_table(rows: list[dict[str, Any]], title: str) -> list[str]:
    lines = [
        f"### {title}",
        "",
        "| Bucket | n | Active days | Avg | Win | EV LCB80 | Avg MFE | Avg MAE | Avg hold |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if not rows:
        return lines + ["| - | 0 | 0 | - | - | - | - | - | - |", ""]
    for row in rows:
        lines.append(
            f"| {row.get('bucket')} | {row.get('n', 0)} | {row.get('active_dates', 0)} | "
            f"{fmt_pct(row.get('avg_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_pct(row.get('lcb80_pct'))} | {fmt_pct(row.get('avg_mfe_pct'))} | "
            f"{fmt_pct(row.get('avg_mae_pct'))} | {fmt_num(row.get('avg_hold_days'))} |"
        )
    lines.append("")
    return lines


def render_market_watch_table(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- 没有需要额外点名的 watch-only 标的。", ""]
    lines = [
        "| Symbol | State | Why |",
        "|---|---|---|",
    ]
    for row in rows[:10]:
        name = f" {row.get('name')}" if row.get("name") else ""
        lines.append(
            f"| {row.get('symbol')}{name} | {row.get('state') or '-'} | "
            f"{clean_table_text(row.get('reason'), 120)} |"
        )
    lines.append("")
    return lines
