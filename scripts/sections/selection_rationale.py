"""Selection rationale + ranker priority helpers (Phase B.18).

Extracted from scripts/generate_main_strategy_v2_report.py — contains:
  - Constants: RANKER_TRADE_TIERS
  - Helpers: ranker_row_priority, best_ranker_rows_by_symbol,
             actionable_ranked_row, trade_orientation,
             promotion_metric_summary, quant_reason, news_reason,
             history_reason
  - Sections: render_actionable_selection_rationale,
              render_market_selection_rationale, render_cn_actionable_evidence

human_risk_plan is late-imported from main (still depends on
US/CN_DEFAULT_TIME_EXIT constants that haven't been extracted yet).
"""
from __future__ import annotations

import json
from typing import Any

from lib.fmt import (
    clean_table_text, fmt_num, fmt_pct, fmt_r, fmt_rate_pct, narrative_label,
    round_or_none, symbol_key as _symbol_key,
)
from sleeves.cn_tape_leadership import CN_TAPE_SLEEVE_ID
from sleeves.us_theme_cluster import US_THEME_SLEEVE_ID


# Sleeve constants used by trade_orientation. Import matches main script:
#   CN_ALPHA_FACTORY_EXECUTION_SLEEVE = "cn_oversold_ev_positive"
#   CN_OBSERVED_LIFECYCLE_SLEEVE = cn_observed_lifecycle_prob.OBSERVED_LIFECYCLE_SLEEVE
from quant_bot.analytics import cn_observed_lifecycle_prob
CN_ALPHA_FACTORY_EXECUTION_SLEEVE = "cn_oversold_ev_positive"
CN_OBSERVED_LIFECYCLE_SLEEVE = cn_observed_lifecycle_prob.OBSERVED_LIFECYCLE_SLEEVE


RANKER_TRADE_TIERS = {
    "top_probe",
    "secondary_probe",
    "top_stock_trade",
    "secondary_stock_trade",
    "observed_lifecycle_trade",
    "observed_lifecycle_secondary_trade",
}


def ranker_row_priority(row: dict[str, Any]) -> tuple[int, int, float, int]:
    tier = str(row.get("production_tier") or "")
    sleeve = str(row.get("alpha_sleeve_id") or "")
    rank = int(round_or_none(row.get("rank")) or 999999)
    return (
        1 if tier in RANKER_TRADE_TIERS else 0,
        1 if sleeve else 0,
        round_or_none(row.get("rank_score")) or 0.0,
        -rank,
    )


def best_ranker_rows_by_symbol(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = _symbol_key(row.get("symbol"))
        if not symbol:
            continue
        existing = by_symbol.get(symbol)
        if existing is None or ranker_row_priority(row) > ranker_row_priority(existing):
            by_symbol[symbol] = row
    return by_symbol


def actionable_ranked_row(payload: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    market = str(action.get("market") or "").upper()
    key = "cn_opportunity_ranker" if market == "CN" else "us_opportunity_ranker"
    rows = (payload.get(key) or {}).get("all_rows") or []
    return best_ranker_rows_by_symbol(rows).get(_symbol_key(action.get("symbol")), {})


def trade_orientation(market: str, ranked: dict[str, Any]) -> str:
    source = str(ranked.get("alpha_sleeve_id") or ranked.get("observed_lifecycle_sleeve_id") or "")
    if market == "US" and source == US_THEME_SLEEVE_ID:
        return "右侧主题动量"
    if market == "CN" and source == CN_TAPE_SLEEVE_ID:
        return "右侧强势延续"
    if market == "CN" and (
        source in {CN_ALPHA_FACTORY_EXECUTION_SLEEVE, CN_OBSERVED_LIFECYCLE_SLEEVE}
        or str(ranked.get("production_tier") or "").startswith("observed_lifecycle")
    ):
        return "左侧价值/超跌"
    return "右侧确认优先"


def promotion_metric_summary(payload: dict[str, Any], market: str, sleeve_id: str | None) -> str | None:
    sleeve = str(sleeve_id or "")
    if not sleeve:
        return None
    best: dict[str, Any] | None = None
    best_payload: dict[str, Any] | None = None
    for row in (payload.get("promotion_contract") or {}).get("rows") or []:
        if str(row.get("market") or "").lower() != market.lower():
            continue
        if str(row.get("sleeve_id") or "") != sleeve:
            continue
        raw = row.get("gates_snapshot_json")
        parsed: dict[str, Any] = {}
        if raw:
            try:
                parsed = json.loads(str(raw))
            except (TypeError, json.JSONDecodeError):
                parsed = {}
        metrics = parsed.get("metrics") or parsed.get("calibration") or {}
        if metrics:
            best = row
            best_payload = parsed
            if str(row.get("status") or "").lower() != "promoted":
                break
    if best is None or best_payload is None:
        return None
    metrics = best_payload.get("metrics") or best_payload.get("calibration") or {}
    blockers = best_payload.get("blockers") or []
    status = str(best.get("status") or "-")
    parts = [
        f"{sleeve} 历史样本 n={metrics.get('n', '-')}",
        f"活跃日 {metrics.get('active_dates', '-')}",
        f"LCB80 {fmt_pct(metrics.get('lcb80_pct'))}",
    ]
    if metrics.get("win_rate") is not None:
        parts.append(f"胜率 {fmt_rate_pct(metrics.get('win_rate'))}")
    if status.lower() != "promoted":
        parts.append("还不是独立长期策略，今天必须靠右侧 tape/主题确认")
    if blockers:
        parts.append(f"旧gate风险 {','.join(str(item) for item in blockers[:3])}")
    return "; ".join(parts)


def quant_reason(market: str, ranked: dict[str, Any]) -> str:
    if market == "CN":
        style = trade_orientation(market, ranked)
        volume = ranked.get("volume_ratio")
        if volume is None:
            volume = ranked.get("flow_volume_confirmation")
        layer = ranked.get("supercycle_layer") or ranked.get("narrative_group") or "neutral"
        role = ranked.get("supply_chain_role") or ""
        if style.startswith("右侧"):
            return (
                f"量化: {narrative_label(ranked.get('narrative_group') or 'neutral')} / {layer}; "
                f"5D {fmt_pct(ranked.get('ret_5d'))}, 1D {fmt_pct(ranked.get('pct_chg'))}, "
                f"price {fmt_num(ranked.get('price_first_signal_score'), 0)}, "
                f"flow {fmt_num(ranked.get('informed_flow_score'), 0)}, "
                f"vol {fmt_num(volume, 2)}"
                + (f", role {clean_table_text(role, 55)}" if role else "")
            )
        value_bits = []
        pe = round_or_none(ranked.get("pe_ttm"))
        pb = round_or_none(ranked.get("pb"))
        if pe is not None and pe > 0:
            value_bits.append(f"PE_TTM {fmt_num(pe, 1)}")
        if pb is not None and pb > 0:
            value_bits.append(f"PB {fmt_num(pb, 2)}")
        if not value_bits:
            value_bits.append("估值字段缺失, 不冒充价值证据")
        return (
            f"量化: 左侧只看 value+oversold; {', '.join(value_bits)}; "
            f"20D {fmt_pct(ranked.get('ret_20d'))}, RSI {fmt_num(ranked.get('rsi_14'), 1)}, "
            f"LCBR {fmt_num(ranked.get('lcb80_r_t3'))}"
        )
    layer = ranked.get("supercycle_layer") or ranked.get("theme_id") or "theme"
    role = ranked.get("supply_chain_role") or ""
    return (
        f"量化: AI supercycle主题动量 / {layer}; "
        f"联合分数 {fmt_num(ranked.get('joint_signal_score'), 0)}, "
        f"期权/flow {fmt_num(ranked.get('flow_options_quality'), 0)}, "
        f"R:R {fmt_num(ranked.get('rr_ratio'), 2)}"
        + (f", role {clean_table_text(role, 55)}" if role else "")
    )


def news_reason(market: str, ranked: dict[str, Any]) -> str:
    headline_risk = round_or_none(ranked.get("headline_risk"))
    latest = ranked.get("latest_headline")
    risk_text = fmt_num((headline_risk or 0.0) * 100.0, 0) if headline_risk is not None else "-"
    if market == "US" and ranked.get("ai_evidence_headline"):
        state = ranked.get("supplier_evidence_state") or "theme_news_only"
        return (
            f"新闻: AI证据={state}, 风险分 {risk_text}; "
            f"{ranked.get('ai_evidence_source') or '-'}: "
            f"{clean_table_text(ranked.get('ai_evidence_text') or ranked.get('ai_evidence_headline'), 110)}"
        )
    if latest:
        return f"新闻: 风险分 {risk_text}; 最新标题={clean_table_text(latest, 90)}"
    if market == "CN":
        return f"新闻: 无明确阻断新闻；A股新闻只做滞后风险标签, 不作为入选主因"
    return f"新闻: 风险分 {risk_text}; 当前没有阻断性事件"


def history_reason(market: str, ranked: dict[str, Any], payload: dict[str, Any]) -> str:
    sleeve = str(ranked.get("alpha_sleeve_id") or ranked.get("observed_lifecycle_sleeve_id") or "")
    if market == "CN":
        if ranked.get("expected_r_t3") is not None or ranked.get("lcb80_r_t3") is not None:
            return (
                f"历史: observed lifecycle ExpR {fmt_num(ranked.get('expected_r_t3'))}, "
                f"LCBR {fmt_num(ranked.get('lcb80_r_t3'))}, n {ranked.get('observed_probability_n') or '-'}"
            )
        if sleeve == CN_TAPE_SLEEVE_ID:
            layer = str(ranked.get("supercycle_layer") or "")
            for row in (payload.get("ai_supercycle_layer_attribution") or {}).get("rows") or []:
                row_source = str(row.get("sleeve_id") or row.get("source") or "")
                if (
                    str(row.get("market") or "").upper() == "CN"
                    and row_source == CN_TAPE_SLEEVE_ID
                    and str(row.get("layer") or "") == layer
                ):
                    return (
                        f"历史: CN tape layer {layer or '-'} n={row.get('n')}; "
                        f"avg {fmt_pct(row.get('avg_pct'))}, LCB80 {fmt_pct(row.get('lcb80_pct'))}, "
                        f"win {fmt_rate_pct(row.get('win_rate'))}"
                    )
        sleeve_summary = promotion_metric_summary(payload, "cn", sleeve)
        if sleeve_summary:
            return f"历史: {sleeve_summary}"
        metrics = ((payload.get("cn") or {}).get("metrics") or {}).get("v2") or {}
        return (
            f"历史: sleeve={ranked.get('alpha_sleeve_id') or '-'}, "
            f"CN EV-positive参考 LCB80 {fmt_pct(metrics.get('lcb80_pct'))}, "
            f"win {fmt_rate_pct(metrics.get('win_rate'))}"
        )
    sleeve_summary = promotion_metric_summary(payload, "us", sleeve)
    if sleeve_summary:
        return f"历史: {sleeve_summary}"
    metrics = ((payload.get("us") or {}).get("metrics") or {}).get("v2_stock_only_net") or {}
    return (
        f"历史: US stock bridge LCB80 {fmt_pct(metrics.get('lcb80_pct'))}, "
        f"win {fmt_rate_pct(metrics.get('win_rate'))}"
    )


def render_actionable_selection_rationale(payload: dict[str, Any], actions: list[dict[str, Any]]) -> list[str]:
    if not actions:
        return []
    lines = [
        "",
        "### 入选三理由 / Selection Rationale",
        "",
        "每只可交易标的都要交代交易方式 + 三条证据。右侧只跟强趋势 / 强板块,左侧仅在价值或历史赔率也站得住的超跌里出现。",
        "",
        "| Market | Symbol | Style | Quant data | News/event | History/evidence |",
        "|---|---|---|---|---|---|",
    ]
    for action in actions[:18]:
        market = str(action.get("market") or "").upper()
        ranked = actionable_ranked_row(payload, action)
        symbol = action.get("symbol") or ranked.get("symbol") or "-"
        lines.append(
            f"| {market or '-'} | {symbol} | {trade_orientation(market, ranked)} | "
            f"{clean_table_text(quant_reason(market, ranked), 160)} | "
            f"{clean_table_text(news_reason(market, ranked), 150)} | "
            f"{clean_table_text(history_reason(market, ranked, payload), 150)} |"
        )
    lines.append("")
    return lines


def render_market_selection_rationale(payload: dict[str, Any], actions: list[dict[str, Any]], market: str) -> list[str]:
    # Late import to avoid circular dep (human_risk_plan still in main)
    from generate_main_strategy_v2_report import human_risk_plan

    market_actions = [row for row in actions if str(row.get("market") or "").upper() == market.upper()]
    if not market_actions:
        return []
    lines = ["## 逐票复核", ""]
    verdicts = payload.get("options_verdicts") or {}
    for action in market_actions[:14]:
        ranked = actionable_ranked_row(payload, action)
        symbol = action.get("symbol") or ranked.get("symbol") or "-"
        name = f" {action.get('name')}" if action.get("name") else ""
        style = trade_orientation(market.upper(), ranked)
        entry = clean_table_text(action.get("entry"), 80)
        risk = clean_table_text(human_risk_plan(action.get("risk_plan")), 110)
        size_txt = fmt_r(action.get("size_r"))
        lines += [
            f"- **{symbol}{name}** — {style}。"
            f"{clean_table_text(quant_reason(market.upper(), ranked), 170)};"
            f"{clean_table_text(news_reason(market.upper(), ranked), 160)};"
            f"{clean_table_text(history_reason(market.upper(), ranked, payload), 170)}。"
            f"参考入口 `{entry}`,风控 `{risk}`,本期 {size_txt}。",
        ]
        verdict = (verdicts.get(str(symbol).upper()) or {}).get("verdict")
        if verdict:
            lines.append(f"  - 期权侧:{verdict}")
    lines.append("")
    return lines


def render_cn_actionable_evidence(payload: dict[str, Any], actions: list[dict[str, Any]]) -> list[str]:
    cn_symbols = [_symbol_key(row.get("symbol")) for row in actions if row.get("market") == "CN"]
    if not cn_symbols:
        return []
    ranker_rows = (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or []
    ranker_by_symbol = best_ranker_rows_by_symbol(ranker_rows)
    lines = [
        "",
        "### A股执行候选证据 / CN Action Evidence",
        "",
        "这部分解释每只 A 股为什么能给 R。A 股新闻只做滞后标签；这里优先展示叙事归属、价格状态、成交/资金、历史相似生命周期和退出规则。",
        "",
        "| Symbol | Source / Tier | Entry / Handle / Target | ExpR / LCBR / n | Price state | Flow / volume | Risk notes |",
        "|---|---|---|---|---|---|---|",
    ]
    for action in [row for row in actions if row.get("market") == "CN"][:12]:
        symbol = _symbol_key(action.get("symbol"))
        row = ranker_by_symbol.get(symbol, {})
        source = row.get("alpha_sleeve_id") or row.get("observed_lifecycle_sleeve_id") or row.get("execution_source") or "-"
        narrative = row.get("narrative_group") or "-"
        tier = row.get("production_tier") or action.get("tier") or "-"
        entry = row.get("observation_entry_zone") or action.get("entry") or "-"
        handle = row.get("handling_line") or "-"
        target = row.get("first_target") or "-"
        old_ev = ""
        if row.get("ev_pct") is not None or row.get("ev_lcb80_pct") is not None:
            old_ev = f"; oldEV {fmt_pct(row.get('ev_pct'))}/{fmt_pct(row.get('ev_lcb80_pct'))}"
        price_state = (
            f"5D {fmt_pct(row.get('ret_5d'))}; 20D {fmt_pct(row.get('ret_20d'))}; "
            f"RSI {fmt_num(row.get('rsi_14'), 1)}; price {fmt_num(row.get('price_first_signal_score'), 0)}"
        )
        flow_state = (
            f"flow {fmt_num(row.get('informed_flow_score'), 0)}; "
            f"large_z {fmt_num(row.get('flow_large_flow_z'))}; "
            f"vol_confirm {fmt_num(row.get('flow_volume_confirmation'))}; "
            f"tape_z {fmt_num(row.get('flow_tape_z'))}"
        )
        risk_notes = (
            f"knife {fmt_num(row.get('falling_knife_score'), 0)}; "
            f"narrative {narrative}/{row.get('supercycle_layer') or '-'}; "
            f"{row.get('supply_chain_role') or row.get('narrative_reason') or ''}; "
            f"state {row.get('alpha_state') or '-'}; "
            f"{row.get('observed_lifecycle_reason') or row.get('reason') or '-'}{old_ev}; "
            f"{row.get('time_exit') or action.get('risk_plan') or '-'}"
        )
        lines.append(
            f"| {action.get('symbol')} {action.get('name') or ''} | "
            f"{source} ({narrative}) / {tier} | "
            f"{clean_table_text(f'{entry} / {handle} / {target}', 80)} | "
            f"{fmt_num(row.get('expected_r_t3'))} / {fmt_num(row.get('lcb80_r_t3'))} / {row.get('observed_probability_n') or '-'} | "
            f"{clean_table_text(price_state, 100)} | "
            f"{clean_table_text(flow_state, 100)} | "
            f"{clean_table_text(risk_notes, 160)} |"
        )
    lines.append("")
    return lines
