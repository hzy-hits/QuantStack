"""🎲 A 股个股概率最优 + 隐含 vol surface (Phase B.8).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Takes actions kwarg from main to avoid circular
import with main's market_actions.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text, fmt_r, symbol_key as _symbol_key


def _pick_probability_cn_stock(ranker_rows, shadow_by_sym):
    """Best CN stock: rank + flow (informed+tushare) + narrative + shadow IV."""
    cands = []
    for r in ranker_rows[:25]:
        sym = str(r.get("symbol") or "").upper()
        rs = float(r.get("rank_score") or 0)
        if rs < 60: continue
        sc = r.get("score_components") or {}
        inflow = float(r.get("informed_flow_score") or 0)
        tushare = float(sc.get("tushare_flow") or 0)
        narrative = float(r.get("narrative_fit_score") or sc.get("narrative_fit") or 0)
        sh = shadow_by_sym.get(sym, {})
        touch = sh.get("touch_90")
        touch_pen = ((touch or 0.5) * 25) if touch is not None else 12
        score = rs * 0.5 + (inflow + tushare) * 0.12 + narrative * 0.15 - touch_pen
        cands.append((score, sym, r, sh))
    cands.sort(key=lambda x: -x[0])
    return cands[:2]


def render_cn_probability_picks_section(
    payload: dict[str, Any],
    *,
    actions: list[dict[str, Any]],
) -> list[str]:
    """🎲 A 股个股概率最优 + 隐含 vol surface (shadow_full)."""
    rows = (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or []
    shadow = payload.get("cn_shadow_full") or {}
    eff_date = (shadow.get("_effective_date") or {}).get("effective_date", "-")
    action_by_sym = {_symbol_key(row.get("symbol")): row for row in actions}
    production_rows = [row for row in rows if _symbol_key(row.get("symbol")) in action_by_sym]
    picks = _pick_probability_cn_stock(production_rows, shadow)

    lines = [
        "## 🎲 A 股概率最优 (个股)",
        "",
        "打分口径: 在可交易名单内按 rank_score、资金流、题材匹配和 shadow_full 隐含下行重排；非执行名单的高分票放在研究观察区。",
        "",
    ]
    if not picks:
        if actions:
            lines += ["- 今日可交易名单里无满足概率最优阈值(rank ≥60)的名字；其他 ranker 高分票维持 0R 观察。", ""]
        else:
            lines += ["- 今日 A 股 Production Decision 没有可交易名单；概率最优不生成 R。", ""]
    else:
        s, sym, r, sh = picks[0]
        action_row = action_by_sym.get(sym) or {}
        rs = r.get("rank_score") or 0
        name = r.get("name") or "-"
        sc = r.get("score_components") or {}
        inflow = r.get("informed_flow_score") or 0
        tushare = sc.get("tushare_flow") or 0
        nar = r.get("narrative_fit_score") or sc.get("narrative_fit") or 0
        atm_iv = sh.get("atm_iv_90d")
        touch = sh.get("touch_90")
        skew = sh.get("skew_90")
        lines.append(f"### 🥇 个股 → **{sym} {name}**")
        lines.append(f"- rank **{rs:.1f}**;informed_flow **{inflow:.0f}**;tushare_flow **{tushare:.0f}**;题材匹配 {nar:.0f}")
        if atm_iv is not None:
            t_s = f"{touch*100:.0f}%" if touch is not None else "-"
            sk_s = f"{skew:+.2f}" if skew is not None else "-"
            lines.append(f"- 隐含 vol(shadow_full ETF 代理): 90d ATM IV {atm_iv:.1f}%;3 个月触及 -10% 概率 **{t_s}**;skew {sk_s}")
        else:
            lines.append("- 隐含 vol 数据未覆盖该股(shadow_full 当日缺)")
        if len(picks) > 1:
            s2, sym2, r2, _ = picks[1]
            lines.append(f"- 备选 **{sym2} {r2.get('name')}** rank {r2.get('rank_score'):.1f} / informed {r2.get('informed_flow_score'):.0f}")
        rationale = []
        if rs >= 70: rationale.append("rank ≥70 历史 5d hit rate 60%+")
        if (inflow + tushare) >= 150: rationale.append("flow 双高 = 主力资金 + 龙虎榜联手")
        if touch is not None and touch < 0.4: rationale.append(f"隐含下行触及概率 {touch*100:.0f}% < 50% 中性")
        if rationale:
            lines.append("- 概率论据:" + ";".join(rationale))
        lines.append(f"- 建议仓位:**{fmt_r(action_row.get('size_r'))}** (来自执行汇总)")
        lines.append(f"- 风控:{action_row.get('risk_plan') or '跌破 EMA21 / 板块退潮 / 北向单日流出 ≥50 亿 任一触发减仓'}")
        lines.append("")

    lines += [
        f"### 个股隐含 vol surface (shadow_full,as of {eff_date})",
        "",
        "ETF 期权曲线代理出的每只 A 股 3 个月隐含波动率 + 下行触及概率。**touch_90 ≥ 60%** = 市场在用 ETF put 大量定价下行,谨慎追多;**≤ 35%** = 下行担忧低,追多空间更大。",
        "",
        "| Symbol | Name | 90d ATM IV | -10% touch | skew | 解读 |",
        "|---|---|---:|---:|---:|---|",
    ]
    shown = 0
    for r in rows[:15]:
        sym = str(r.get("symbol") or "").upper()
        sh = shadow.get(sym) or {}
        atm = sh.get("atm_iv_90d")
        if atm is None: continue
        tp = sh.get("touch_90")
        sk = sh.get("skew_90")
        if tp is not None and tp >= 0.6:
            verdict = "⚠️ 下行已被大量定价"
        elif tp is not None and tp <= 0.35:
            verdict = "✓ 下行担忧低"
        else:
            verdict = "中性"
        tp_s = f"{tp*100:.0f}%" if tp is not None else "-"
        sk_s = f"{sk:+.2f}" if sk is not None else "-"
        lines.append(f"| {sym} | {clean_table_text(r.get('name') or '-', 10)} | {atm:.1f}% | {tp_s} | {sk_s} | {verdict} |")
        shown += 1
        if shown >= 10: break
    if shown == 0:
        lines.append("| - | shadow_full 今天没有覆盖任何头部名字 | - | - | - | - |")
    lines += ["", "---", ""]
    return lines
