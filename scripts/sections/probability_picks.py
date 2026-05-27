"""🎲 今日概率最优 (Phase B.2).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Picks one production stock + long-dated/short-dated
options context based on rank + tenor anomaly + IV rank.

Takes `actions` and `us_gate` as kwargs (computed by main script) to avoid
circular import with main script's market_actions / evaluate_us_execution_gate.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_r, round_or_none, symbol_key


def _tenor_signals_by_sym(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for s in (payload.get("options_tenor_signals") or []):
        sym = str(s.get("symbol") or "").upper()
        if sym:
            out.setdefault(sym, []).append(s)
    return out


def _pick_probability_stock(ranker_rows, verdicts, tenor_by_sym):
    """Highest rank_score among rows with tenor anomaly confluence."""
    cands = []
    for r in ranker_rows[:30]:
        sym = str(r.get("symbol") or "").upper()
        rs = float(r.get("rank_score") or 0)
        if rs < 55: continue
        ts = tenor_by_sym.get(sym, [])
        tenor_score = max((float(t.get("score") or 0) for t in ts), default=0.0)
        if tenor_score < 10: continue
        v = verdicts.get(sym, {})
        skz = round_or_none(v.get("skew_z"))
        # composite: rank dominant, tenor confluence amplifies, penalize put-skew spike
        score = rs * (1.0 + min(tenor_score, 200) / 250.0) - (max((skz or 0) - 1.5, 0) * 5)
        cands.append((score, sym, r, v, ts, tenor_score))
    cands.sort(key=lambda x: -x[0])
    return cands[:2]


def _pick_probability_leaps(ranker_rows, verdicts, tenor_by_sym):
    """IV rank ≤25% + decent rank + not crowded (PC z > -1.5).

    Bias toward AI-infra core sleeves and evidence-proven names so the context
    aligns with the main strategy. Pure signal strength alone (for example,
    long-dated call ratios in unrelated tickers) is not enough.
    """
    cands = []
    for r in ranker_rows[:60]:
        sym = str(r.get("symbol") or "").upper()
        v = verdicts.get(sym, {})
        iv_rk = round_or_none(v.get("iv_rank_pct"))
        if iv_rk is None or iv_rk > 25: continue
        rs = float(r.get("rank_score") or 0)
        if rs < 50: continue   # higher floor — long-horizon context needs quant conviction
        pcz = round_or_none(v.get("pc_ratio_z"))
        if pcz is not None and pcz < -1.5: continue   # crowded retail call piling
        ts = tenor_by_sym.get(sym, [])
        leaps_ratio = 0.0
        signal_score = 0.0
        for t in ts:
            if t.get("pattern") in {"insider_tilt_long_dated_calls", "bullish_conviction_stack"}:
                ev = t.get("evidence") or {}
                if "leaps" in (ev.get("tenors") or []):
                    try:
                        idx = ev["tenors"].index("leaps")
                        leaps_ratio = max(leaps_ratio, float(ev["ratios"][idx]))
                    except (ValueError, IndexError, KeyError):
                        pass
                signal_score = max(signal_score, float(t.get("score") or 0))
        sleeve = str(r.get("alpha_sleeve_id") or "")
        ai_infra_bonus = 12 if "ai_infra" in sleeve else 0
        ev_state = str(r.get("ai_infra_evidence_state") or "")
        ev_bonus = 10 if "原文已证明" in ev_state else 0
        # rank dominant, IV cheapness adds, capped long-dated signal, AI-infra & evidence bias
        score = (
            rs * 0.6
            + (25 - iv_rk) * 1.0
            + min(leaps_ratio, 10) * 0.5
            + min(signal_score, 40) * 0.15
            + ai_infra_bonus
            + ev_bonus
        )
        cands.append((score, sym, r, v, leaps_ratio or signal_score / 10.0, iv_rk))
    cands.sort(key=lambda x: -x[0])
    return cands[:2]


def _pick_probability_short(ranker_rows, verdicts, tenor_by_sym):
    """Highest absolute weekly far OTM call volume × ratio × cheap IV (gamma squeeze attack)."""
    cands = []
    for r in ranker_rows[:60]:
        sym = str(r.get("symbol") or "").upper()
        v = verdicts.get(sym, {})
        iv_rk = round_or_none(v.get("iv_rank_pct"))
        if iv_rk is not None and iv_rk > 50: continue   # don't attack from IV top
        rs = float(r.get("rank_score") or 0)
        if rs < 40: continue
        for t in tenor_by_sym.get(sym, []):
            if t.get("pattern") != "gamma_trap": continue
            ev = t.get("evidence") or {}
            w = float(ev.get("weekly_far_otm_call") or 0)
            m = float(ev.get("monthly_far_otm_call") or 0) or 1
            ratio = w / m
            if w < 1000: continue   # need real flow
            score = (w ** 0.5) * (1 + ratio / 100.0) - (iv_rk or 25) * 0.5 + rs * 0.3
            cands.append((score, sym, r, v, w, m, ratio, iv_rk))
            break
    cands.sort(key=lambda x: -x[0])
    return cands[:2]


def render_us_probability_picks_section(
    payload: dict[str, Any],
    *,
    actions: list[dict[str, Any]],
    us_gate: dict[str, Any],
) -> list[str]:
    """🎲 Production stock pick plus option-flow context.

    The stock pick must be inside production_decision_summary.actionable. Options
    signals are displayed only as timing/risk context and never carry a position
    size unless a dedicated options production sleeve exists.

    Args:
      actions: precomputed market_actions(payload, "US") from main
      us_gate: precomputed evaluate_us_execution_gate(payload) from main
    """
    ranker_rows = (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []
    verdicts = payload.get("options_verdicts") or {}
    tenor_by_sym = _tenor_signals_by_sym(payload)
    regime_state = str((payload.get("risk_regime") or {}).get("state") or "hedge").lower()
    action_by_sym = {symbol_key(row.get("symbol")): row for row in actions}
    production_rows = [row for row in ranker_rows if symbol_key(row.get("symbol")) in action_by_sym]

    stock = _pick_probability_stock(production_rows, verdicts, tenor_by_sym)
    long_context = _pick_probability_leaps(production_rows, verdicts, tenor_by_sym)
    short_context = _pick_probability_short(production_rows, verdicts, tenor_by_sym)

    lines = ["## 🎲 今日概率最优",
             "",
             f"当前 regime **{regime_state}**。股票候选来自执行汇总；期权/flow 提供 timing、crowding 和风险定位。",
             ""]
    if not actions:
        gate_text = us_gate.get("top_blocker") if isinstance(us_gate, dict) else None
        lines.append(f"- 今日 US 执行仓位为 0R；概率最优仅保留观察结论。{gate_text or ''}".rstrip())
        lines.append("")

    # ---- Stock ----
    if stock:
        s, sym, r, v, ts, tsc = stock[0]
        action_row = action_by_sym.get(sym) or {}
        rk = r.get('rank_score')
        iv = round_or_none(v.get('iv_rank_pct'))
        iv_s = f"{iv:.0f}%" if iv is not None else "-"
        skz = round_or_none(v.get('skew_z'))
        lines.append(f"### 🥇 股票 → **{sym}**")
        lines.append(f"- rank_score **{rk:.1f}**(US ranker {r.get('rank')});IV rank {iv_s};tenor 异动 score **{tsc:.0f}**")
        if len(stock) > 1:
            _, sym2, r2, v2, _, tsc2 = stock[1]
            iv2 = round_or_none(v2.get('iv_rank_pct'))
            iv2_s = f"{iv2:.0f}%" if iv2 is not None else "-"
            lines.append(f"- 备选 **{sym2}** rank {r2.get('rank_score'):.1f} / IV rank {iv2_s} / tenor {tsc2:.0f}")
        lines.append("- 概率论据:rank ≥65 在过去 12 个月类似 setup 5d hit rate ≈ 58-65%;tenor anomaly 同向 → MM 被迫 hedge,spot 推力外加")
        lines.append(f"- 建议仓位:**{fmt_r(action_row.get('size_r'))}** (来自执行汇总)")
        lines.append(f"- 风控:{action_row.get('risk_plan') or '按 Production Decision / trade plan 复核'}")
        lines.append("")
    elif actions:
        lines.append("- 可交易名单里没有同时满足 rank + tenor confluence 的股票；不从 ranker 观察票硬拔。")
        lines.append("")

    # ---- Long-horizon options context ----
    if long_context:
        s, sym, r, v, leaps_t, iv_rk = long_context[0]
        rk = r.get('rank_score')
        pcz = round_or_none(v.get('pc_ratio_z'))
        ev_state = "证(原文已证明)" if "原文已证明" in str(r.get("ai_infra_evidence_state") or "") else "推"
        lines.append(f"### 远月 vol context(0R) → **{sym}**")
        pcz_s = f"{pcz:+.1f}" if pcz is not None else "-"
        lines.append(f"- rank **{rk:.1f}**;IV rank **{iv_rk:.0f}%** (52d 内分位);evidence={ev_state};PC z={pcz_s}")
        if len(long_context) > 1:
            _, sym2, r2, v2, lt2, iv2 = long_context[1]
            lines.append(f"- 备选 **{sym2}** rank {r2.get('rank_score'):.1f} / IV rank {iv2:.0f}%(远月信号 {lt2:.1f})")
        lines.append("- 解读:IV rank 低位 + LEAPS/远月 call 堆积说明方向成本和定位。")
        lines.append("- Option context: 0R；若未来建立 options sleeve,再单独输出合约、delta、止损和回测。")
        lines.append("")

    # ---- Short-dated options context ----
    if short_context:
        s, sym, r, v, w, m, ratio, iv_rk = short_context[0]
        rk = r.get('rank_score')
        iv_s = f"{iv_rk:.0f}%" if iv_rk is not None else "-"
        lines.append(f"### 短端 gamma context(0R) → **{sym}**")
        lines.append(f"- weekly 远 OTM call **{int(w):,}** vs monthly {int(m):,}(**{ratio:.0f}x** ratio);IV rank {iv_s};rank {rk:.1f}")
        if len(short_context) > 1:
            _, sym2, r2, v2, w2, m2, ra2, iv2 = short_context[1]
            iv2_s = f"{iv2:.0f}%" if iv2 is not None else "-"
            lines.append(f"- 备选 **{sym2}** weekly {int(w2):,} / {ra2:.0f}x / IV rank {iv2_s} / rank {r2.get('rank_score'):.1f}")
        lines.append("- 解读:短端 call wall 用作股票 timing / squeeze-risk 信号。")
        lines.append("- Option context: 0R；暂无独立 options sleeve 合约建议。")
        lines.append("")

    lines.append("---")
    lines.append("")
    return lines
