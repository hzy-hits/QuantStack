"""🎯 今日只看这些 — Production stocks + long-dated tenor context (Phase B.3).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Renders the top US daily picks: ≤6 production stocks
+ ≤4 long-dated tenor anomaly rows (0R context).

Takes `actions` kwarg (precomputed market_actions(payload, "US") from main)
to avoid circular dependency.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import display_tenor_name, fmt_r, report_safe_options_context, round_or_none, symbol_key


def render_us_top10_daily_section(
    payload: dict[str, Any],
    *,
    actions: list[dict[str, Any]],
) -> list[str]:
    """Production stock focus plus option-flow context.

    放在美股日报最顶部(intro 之后,可交易名单之前)。下面的完整可交易
    名单 / 逐票复核 / IV 视图等保留作为深挖。
    """
    ranker_rows = (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []
    verdicts = payload.get("options_verdicts") or {}
    tenor_signals = payload.get("options_tenor_signals") or []
    actionable_syms = {str(a.get("symbol") or "").upper() for a in actions}

    lines: list[str] = ["## 🎯 今日只看这些", ""]

    # ---- Production stocks only ----
    ranker_by_sym = {symbol_key(row.get("symbol")): row for row in ranker_rows}
    top_stocks: list[dict[str, Any]] = []
    seen: set[str] = set()
    for action in actions:
        sym = symbol_key(action.get("symbol"))
        if not sym or sym in seen:
            continue
        r = dict(ranker_by_sym.get(sym) or {})
        r.setdefault("symbol", sym)
        r["_production_size_r"] = action.get("size_r")
        top_stocks.append(r)
        seen.add(sym)
        if len(top_stocks) == 6:
            break

    lines += [
        "### 🟢 Production 股票",
        "",
        "| # | Symbol | rank_score | Sleeve | IV rank | 今天的位置 |",
        "|---:|---|---:|---|---:|---|",
    ]
    if not top_stocks:
        lines.append("| - | - | - | - | - | 今日股票执行为 0R；ranker 和期权定位保留观察 |")
    for i, r in enumerate(top_stocks, 1):
        sym = str(r.get("symbol") or "").upper()
        v = verdicts.get(sym) or {}
        iv_rk = round_or_none(v.get("iv_rank_pct"))
        iv_s = f"{iv_rk:.0f}%" if iv_rk is not None else "-"
        sleeve = (r.get("alpha_sleeve_id") or "-").replace("us_", "").replace("_", " ")
        is_actionable = sym in actionable_syms
        marker = "✓ 可买" if is_actionable else "观察"
        # one-line context: tier + actionability + IV vibe
        iv_note = ""
        if iv_rk is not None:
            if iv_rk <= 20:
                iv_note = "IV 历史低位,只作方向成本 context"
            elif iv_rk >= 80:
                iv_note = "IV 高位,买股别买期权"
            else:
                iv_note = "IV 中性"
        else:
            iv_note = "期权数据缺"
        broad = (r.get("score_components") or {}).get("broad_signal")
        broad_s = f"broad {broad:.0f}" if broad is not None else ""
        ctx = f"{marker};size {fmt_r(r.get('_production_size_r'))};{iv_note}" + (f";{broad_s}" if broad_s else "")
        lines.append(
            f"| {i} | **{sym}** | {r.get('rank_score', '-')} | {sleeve[:24]} | "
            f"{iv_s} | {ctx} |"
        )

    # ---- 4 long-dated option-flow context rows ----
    long_tenor = [
        s for s in tenor_signals
        if s.get("pattern") in {"insider_tilt_long_dated_calls", "bullish_conviction_stack"}
    ]
    long_tenor.sort(key=lambda s: -float(s.get("score") or 0.0))
    leaps_picks: list[dict[str, Any]] = []
    seen_l: set[str] = set()
    for s in long_tenor:
        sym = str(s.get("symbol") or "").upper()
        if not sym or sym in seen_l:
            continue
        leaps_picks.append(s)
        seen_l.add(sym)
        if len(leaps_picks) == 4:
            break

    lines += [
        "",
        "### 🎯 远月 OTM 异动(0R context)",
        "",
        "| # | Symbol | Pattern | Score | IV rank | 异动证据 |",
        "|---:|---|---|---:|---:|---|",
    ]
    pattern_zh = {
        "insider_tilt_long_dated_calls": "远月 call 堆积",
        "bullish_conviction_stack": "多周期看涨堆积",
    }
    for i, s in enumerate(leaps_picks, 1):
        sym = str(s.get("symbol") or "").upper()
        v = verdicts.get(sym) or {}
        iv_rk = round_or_none(v.get("iv_rank_pct"))
        iv_s = f"{iv_rk:.0f}%" if iv_rk is not None else "-"
        pat = pattern_zh.get(s.get("pattern"), s.get("pattern") or "-")
        ev = s.get("evidence") or {}
        if "tenors" in ev and "ratios" in ev:
            ratios = ev.get("ratios") or []
            tenors = ev.get("tenors") or []
            ev_txt = " / ".join(
                f"{display_tenor_name(t)}={float(r):.1f}x" for t, r in zip(tenors, ratios)
            )
        else:
            parts = [f"{k}={v}" for k, v in ev.items()]
            ev_txt = "; ".join(parts)
        score = float(s.get("score") or 0.0)
        cross_action = " ⭐ 同时在 Top 6 股票" if sym in {str(t.get('symbol') or '').upper() for t in top_stocks} else ""
        lines.append(
            f"| {i} | **{sym}** | {pat} | {score:.1f} | {iv_s} | "
            f"{report_safe_options_context(ev_txt, 60)}{cross_action} |"
        )

    if not leaps_picks and not top_stocks:
        lines.append("- 今日 ranker + tenor 信号都为空。")

    lines += [
        "",
        f"_完整 ranker 表、逐票复核、IV 视图、左侧观察池等深挖内容见下文；本节期权异动为 0R context。_",
        "",
    ]
    return lines
