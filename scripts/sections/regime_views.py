"""Regime / fear-greed / bubble-hedge / execution-gate views (Phase B.7).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. 4 small context-rendering sections that share
no logic with each other but live in the same module since they're all
"environment / regime" section renderers.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_num


def render_risk_regime_section(payload: dict[str, Any], regime_key: str = "risk_regime") -> list[str]:
    """Render the Hedge/Wedge/Confirm/Press gate state — the hard R gate.

    regime_key="cn_risk_regime" renders the CN-native regime (创业板/北向/
    两融 signals); the default renders the US regime (MOVE/VIX/SMH).
    """
    is_cn = "cn" in regime_key
    regime = payload.get(regime_key) or {}
    state = str(regime.get("state") or "hedge")
    mult = float(regime.get("r_multiplier", 1.0))
    state_label = {
        "hedge": "HEDGE — 常驻基线",
        "wedge": "WEDGE — 楔子咬合",
        "confirm": "CONFIRM — 破位预警",
        "press": "PRESS — 确认压制",
        "capitulation": "CAPITULATION — 抛售衰竭",
    }.get(state, state)
    title = ("## CN 风控引擎 — A股 Hedge / Wedge / Confirm / Press（硬 gate）"
             if is_cn else
             "## 风控引擎 — Hedge / Wedge / Confirm / Press（硬 gate）")
    book = "A股" if is_cn else "AI-infra"
    lines = [
        title,
        "",
        f"**当前状态：{state_label}** ｜ {book}新加仓 R 乘数 `{mult:.2f}x`"
        + ("（新加仓冻结）" if not regime.get("new_adds_allowed", True) else ""),
        "",
        f"- 判定：{regime.get('rationale') or '—'}",
        f"- 对冲指引：{regime.get('hedge_directive') or '—'}",
        f"- Victim 动作：{regime.get('victim_action') or '—'}",
    ]
    if regime.get("artifact_missing"):
        art = "cn_risk_regime" if is_cn else "bubble_hedge"
        lines.append(f"- ⚠️ {art} 工件缺失，gate 退化为 1.0x。")
    sig = regime.get("signals") or {}
    if sig and is_cn:
        lines.append(
            "- A股信号："
            f"创业板>EMA50={sig.get('gem_above_ema50')} ｜ "
            f"创业板>EMA20={sig.get('gem_above_ema20')} ｜ "
            f"沪深300>EMA50={sig.get('hs300_above_ema50')} ｜ "
            f"北向20d={fmt_num(sig.get('north_20d_sum'), 0)} ｜ "
            f"两融20d={fmt_num(sig.get('margin_chg_20d_pct'), 1)}%"
        )
        lines.append(
            f"- 楔子层(共用)：美债 MOVE={fmt_num(sig.get('us_move_level'), 1)}"
            f"(20d {fmt_num(sig.get('us_move_chg_20d'), 1)}%) — 经北向传导"
        )
    elif sig:
        lines.append(
            "- 波动信号："
            f"MOVE={fmt_num(sig.get('move_level'), 1)}"
            f"(20d {fmt_num(sig.get('move_chg_20d'), 1)}%) ｜ "
            f"VIX={fmt_num(sig.get('vix_level'), 1)} ｜ "
            f"MOVE/VIX={fmt_num(sig.get('move_vix_ratio'), 2)} ｜ "
            f"TLT20d={fmt_num(sig.get('tlt_ret_20d_pct'), 2)}%"
        )
        lines.append(
            "- 趋势信号："
            f"SMH↔TLT corr={fmt_num(sig.get('smh_tlt_corr_20d'), 2)} ｜ "
            f"F&G={fmt_num(sig.get('fear_greed_score'), 0)} ｜ "
            f"SMH>EMA50={sig.get('smh_above_ema50')} ｜ "
            f"trendline_break={sig.get('trendline_break')}"
        )
    lines.append("")
    return lines


def render_bubble_hedge_section(payload: dict[str, Any], *, victim_top_n: int = 3) -> list[str]:
    bubble = payload.get("bubble_hedge") or {}
    if not bubble:
        return [
            "## Bubble Hedge — Wedge / Victim / Confirmation",
            "",
            "- 工件未生成。运行 `scripts/score_bubble_hedge_radar.py` 后再看。",
            "",
        ]
    lines = [
        "## Bubble Hedge — Wedge / Victim / Confirmation",
        "",
        "Hedge-Wedge-Confirm-Press 框架对 AI book 的风险口径。",
        "**不替代量化决策**；只是告诉操作员当前在哪个阶段。",
        "",
    ]
    for note in bubble.get("guidance") or []:
        lines.append(f"- {note}")
    lines.append("")

    confirm = bubble.get("confirmation") or {}
    lines += [
        f"- SMH {confirm.get('smh_close')} | EMA20 {confirm.get('smh_ema20')} / EMA50 {confirm.get('smh_ema50')} / EMA200 {confirm.get('smh_ema200')}",
        f"- 站上 EMA20/EMA50/EMA200: {confirm.get('smh_above_ema20')}/{confirm.get('smh_above_ema50')}/{confirm.get('smh_above_ema200')} | "
        f"SMH↔TLT 20d corr: {confirm.get('ai_book_vs_tlt_corr_20d')} | trendline break: {confirm.get('trendline_break')}",
        "",
    ]
    victims = bubble.get("victims") or []
    if victims:
        lines += [
            "### Victim shortlist (高分 = 越脆弱)",
            "",
            "| Symbol | Company | Module | px vs EMA50 | β vs TLT | Score | Reasons |",
            "|---|---|---|---:|---:|---:|---|",
        ]
        for v in victims[:victim_top_n]:
            ema50 = v.get("px_vs_ema50_pct")
            beta = v.get("beta_vs_tlt_20d")
            lines.append(
                f"| {v.get('symbol')} | {(v.get('company') or '')[:24]} | "
                f"{(v.get('module') or '')[:28]} | "
                f"{f'{ema50:+.1f}%' if ema50 is not None else '-'} | "
                f"{f'{beta:+.2f}' if beta is not None else '-'} | "
                f"{v.get('convex_score', 0):.1f} | "
                f"{', '.join((v.get('reasons') or [])[:3])} |"
            )
        lines.append("")
    lines.append("详细 wedge layer 见 `reports/review_dashboard/bubble_hedge_radar/<date>/bubble_hedge.md`。")
    lines.append("")
    return lines


def render_fear_greed_section(payload: dict[str, Any]) -> list[str]:
    fg = payload.get("fear_greed") or {}
    if not fg:
        return []
    score = fg.get("score")
    rating = fg.get("rating") or "-"
    source = fg.get("source") or "?"
    components = fg.get("components") or {}
    is_cnn = source == "cnn"
    title = (
        "## CNN Fear & Greed — 仅作 macro/crowding 上下文"
        if is_cnn
        else "## Internal Fear/Greed proxy — CNN 官方源不可用时的 macro/crowding 代理"
    )
    source_line = (
        "- 数据源: `cnn` official feed"
        if is_cnn
        else "- 数据源: `proxy` (CNN official feed 未成功；使用 VIX + SPY EMA50 + SPY 5d 三因子代理)"
    )
    reading_label = "CNN 当前读数" if is_cnn else "内部 proxy 当前读数"
    lines = [
        title,
        "",
        source_line,
        f"- {reading_label}: **{score:.1f} / 100** → **{rating}**",
        "- macro/crowding 层的信号用于读环境；ticker 执行状态仍以 AI book 和执行汇总为准。",
        "",
    ]
    if is_cnn:
        history = []
        for key, label in (
            ("previous_close", "前一日"),
            ("previous_1_week", "一周前"),
            ("previous_1_month", "一月前"),
            ("previous_1_year", "一年前"),
        ):
            value = fg.get(key)
            if isinstance(value, dict):
                value = value.get("score")
            if value is not None:
                try:
                    history.append(f"{label}={float(value):.1f}")
                except (TypeError, ValueError):
                    continue
        if history:
            lines.append(f"- 历史读数: {'; '.join(history)}")
            lines.append("")
    if components:
        lines += [
            "| 分量 | 数值 | 解释 |",
            "|---|---|---|",
        ]
        if "vix" in components:
            entry = components["vix"]
            lines.append(
                f"| VIX | level {entry.get('level')} (percentile {entry.get('percentile_252d')}%) "
                f"| score {entry.get('score')} (低 VIX = 贪婪) |"
            )
        if "spy_vs_ema50" in components:
            entry = components["spy_vs_ema50"]
            lines.append(
                f"| SPY vs EMA50 | dist {entry.get('distance_pct')}% | "
                f"score {entry.get('score')} (≥ EMA50 = 贪婪) |"
            )
        if "spy_5d_return" in components:
            entry = components["spy_5d_return"]
            lines.append(
                f"| SPY 5d return | {entry.get('value_pct')}% (percentile {entry.get('percentile_252d')}%) | "
                f"score {entry.get('score')} |"
            )
        for key in ("market_momentum_sp500", "stock_price_strength", "stock_price_breadth",
                    "put_call_options", "market_volatility_vix", "safe_haven_demand", "junk_bond_demand"):
            entry = components.get(key)
            if not isinstance(entry, dict):
                continue
            current = entry.get("score") or entry.get("rating")
            if current is None:
                continue
            lines.append(f"| {key} | {current} | CNN 子分量 |")
        lines.append("")
    return lines


def render_us_execution_gate_notice(payload: dict[str, Any]) -> list[str]:
    gate = ((payload.get("production_decision_summary") or {}).get("summary") or {}).get("us_execution_gate")
    if not isinstance(gate, dict) or gate.get("allowed"):
        return []
    reasons = gate.get("reasons") or []
    lines = [
        "## US Production Gate",
        "",
        "- 今日美股执行 R = 0；ranker、新闻和期权异动进入观察区。",
    ]
    for reason in reasons[:3]:
        lines.append(f"- {reason}")
    lines.append("")
    return lines
