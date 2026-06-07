"""US stock decision stack report section.

The US pipeline is intentionally layered: AI evidence admits the stock universe,
trend + IV/HV drives the main timing context, Gamma Spring v3 manages exposure,
and portfolio risk converts the idea into final stock R.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from lib.fmt import fmt_num, fmt_r


def _sym(value: Any) -> str:
    return str(value or "").upper().strip()


def _ratio_pct(value: Any, digits: int = 0) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed * 100:+.{digits}f}%"


def _iv_hv_label(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    if 0.90 <= parsed <= 1.35:
        return f"{parsed:.2f} healthy"
    if parsed < 0.90:
        return f"{parsed:.2f} low/unpriced-tail"
    return f"{parsed:.2f} high/event"


def _gamma_action_label(signal: Any) -> str:
    text = str(signal or "hold_context_only")
    mapping = {
        "hold_context_only": "hold context",
        "hold_observe_after_breakout": "hold after break",
        "breakout_hold_ok": "hold after break",
        "gamma_v2_entry_alpha": "entry alpha",
        "reduce_or_tighten_stop": "reduce/tighten",
        "no_add_tighten_stop": "no add/tighten",
        "do_not_chase_above_wall": "do not chase",
    }
    return mapping.get(text, text)


def _management_counts(gamma_rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    counts = Counter(str(row.get("management_signal") or "hold_context_only") for row in gamma_rows)
    reduce_count = counts["reduce_or_tighten_stop"]
    no_add_count = counts["no_add_tighten_stop"] + counts["do_not_chase_above_wall"]
    hold_count = sum(
        value
        for key, value in counts.items()
        if key not in {"reduce_or_tighten_stop", "no_add_tighten_stop", "do_not_chase_above_wall"}
    )
    return reduce_count, no_add_count, hold_count


def render_us_stock_decision_stack_section(payload: dict[str, Any], *, limit: int = 8) -> list[str]:
    actions = [
        row
        for row in ((payload.get("production_decision_summary") or {}).get("actionable") or [])
        if _sym(row.get("market")) == "US"
    ]
    gamma_rows = list((payload.get("gamma_spring") or {}).get("rows") or [])
    gamma_lookup = {_sym(row.get("symbol")): row for row in gamma_rows if _sym(row.get("symbol"))}
    ranker_lookup = {
        _sym(row.get("symbol")): row
        for row in ((payload.get("us_opportunity_ranker") or {}).get("all_rows") or [])
        if _sym(row.get("symbol"))
    }
    options = payload.get("options_verdicts") or {}
    reduce_count, no_add_count, hold_count = _management_counts(gamma_rows)
    lines = [
        "## US Stock Decision Stack / 美股买卖管理总线",
        "",
        "- 设计: 主策略不是单一神奇指标,而是分层总线: AI universe 准入 -> Gamma Spring v3 GEX 区间状态机 -> trend + IV/HV timing -> portfolio/beta 风险预算。",
        "- Gamma v3 定位: 美股选股/入场主引擎之一,在已准入 AI universe 内可生成 `us_gamma_v2_alpha` 兼容 production candidate;同时继续负责仓位上限、收紧止损、不追高和突破后持有观察。",
        "- 风控边界: dealer sign 仍是 OI change / volume / skew proxy,所以 Gamma v3 可以给入场 alpha,但不能绕过 source evidence、headline risk、组合 R 和 beta hedge。",
        "",
        "| Layer | Production role | Today status | Can create candidate? | Can change stock R? |",
        "|---|---|---|---|---|",
        "| AI universe / source evidence | 候选准入 | 只允许 AI universe / 晋级票进入 production | yes, only after promotion | evidence multiplier |",
        f"| Gamma Spring v3 | 选股/入场 alpha 主引擎 | {len(gamma_rows)} rows; reduce/tighten {reduce_count}, no-add/chase {no_add_count}, hold {hold_count} | yes, inside admitted universe | create entry / cap R / tighten stop |",
        "| Trend + IV/HV healthy band | timing 确认 / volatility health | IV/HV 0.90-1.35 配合 trend 是健康带 | yes, inside admitted universe | entry quality / hold / skip |",
        "| Portfolio / beta overlay | 组合风险预算 | final R、hedge、net beta 统一收口 | no | final R / hedge |",
        "",
    ]
    if not actions:
        lines += ["- 今日无 US production action; Gamma v3 仍保留为观察/风险管理 context。", ""]
        return lines

    lines += [
        "| Symbol | Stock R | Timing source | IV/HV | Gamma v3 | Dealer px | Wall | Mgmt |",
        "|---|---:|---|---:|---:|---:|---|---|",
    ]
    for action in actions[:limit]:
        sym = _sym(action.get("symbol"))
        ranked = ranker_lookup.get(sym) or {}
        gamma = gamma_lookup.get(sym) or {}
        dealer = (
            ranked.get("gamma_v2_dealer_pressure_proxy")
            if ranked.get("gamma_v2_dealer_pressure_proxy") is not None
            else gamma.get("dealer_pressure_proxy")
        )
        wall = ranked.get("gamma_v2_wall_transition") or gamma.get("wall_transition") or "-"
        mgmt = ranked.get("gamma_v2_management_signal") or gamma.get("management_signal")
        gamma_display = (
            f"alpha {fmt_num(ranked.get('gamma_v2_alpha_score'), 0)}"
            if ranked.get("gamma_v2_alpha_score") is not None
            else f"{fmt_num(gamma.get('gamma_v2_multiplier'), 2)}x"
        )
        opt = options.get(sym) or {}
        lines.append(
            "| {sym} | {r} | {source} | {ivhv} | {mult} | {dealer} | {wall} | {mgmt} |".format(
                sym=sym or "-",
                r=fmt_r(action.get("size_r")),
                source=str(action.get("source") or "-"),
                ivhv=_iv_hv_label(opt.get("iv_hv")),
                mult=gamma_display,
                dealer=_ratio_pct(dealer, 0),
                wall=wall,
                mgmt=_gamma_action_label(mgmt),
            )
        )
    lines.append("")
    return lines
