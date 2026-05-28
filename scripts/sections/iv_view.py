"""US 期权 IV 视图 — non-executable options context (Phase B.4).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Per-stock IV / VRP / skew table with IV rank
based action hints (LEAPS context / sell-vol context / wait).

Includes the build_options_verdicts loader (DB heavy) + _iv_action_hint
classifier + render_iv_view_section renderer.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from lib.fmt import round_or_none


def build_options_verdicts(us_db: Path, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    """Compact per-stock options read — what the options market already knows.

    Options are evidence for the *stock* decision, never a traded instrument
    here. For each basket name we translate four readings into plain language:

    - positioning : pc_ratio_z (call-heavy vs put-heavy order flow)
    - expected move : iv_ann, plus vrp (IV cheap or rich vs realised vol)
    - downside fear : skew_z (elevated put skew = crowd prices asymmetry)
    - conviction horizon : where chain volume sits (weekly = tactical,
      long-dated = structural positioning context)
    """
    out: dict[str, dict[str, Any]] = {}
    syms = sorted({str(s).upper() for s in symbols if s})
    if not us_db.exists() or not syms:
        return out
    placeholders = ", ".join("?" for _ in syms)
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        effective_date = as_of.isoformat()
        try:
            latest_row = con.execute(
                "SELECT MAX(as_of) FROM options_sentiment WHERE as_of <= ?",
                [as_of.isoformat()],
            ).fetchone()
            if latest_row and latest_row[0]:
                effective_date = str(latest_row[0])
        except duckdb.Error:
            pass
        sentiment: dict[str, dict[str, Any]] = {}
        try:
            for row in con.execute(
                f"SELECT symbol, pc_ratio_z, skew_z, vrp, iv_ann, rv_ann "
                f"FROM options_sentiment WHERE as_of = ? AND symbol IN ({placeholders})",
                [effective_date, *syms],
            ).fetchall():
                sentiment[str(row[0]).upper()] = {
                    "pc_ratio_z": row[1], "skew_z": row[2], "vrp": row[3],
                    "iv_ann": row[4], "rv_ann": row[5],
                }
        except duckdb.Error:
            sentiment = {}
        # IV rank: percentile of today's IV vs each symbol's last ≤252 trading
        # days. Today the table only has ~52 distinct dates (CBOE collection
        # started 2026-03-10), so the lookback caps at whatever exists; the
        # output records n_obs so the operator knows the lookback depth.
        iv_rank: dict[str, dict[str, Any]] = {}
        try:
            for row in con.execute(
                f"WITH hist AS ("
                f"  SELECT symbol, iv_ann FROM options_sentiment "
                f"  WHERE symbol IN ({placeholders}) AND iv_ann IS NOT NULL "
                f"    AND as_of <= ? AND as_of >= (CAST(? AS DATE) - INTERVAL 365 DAY)"
                f") "
                f"SELECT symbol, COUNT(*) AS n, MIN(iv_ann) AS mn, MAX(iv_ann) AS mx "
                f"FROM hist GROUP BY symbol",
                [*syms, effective_date, effective_date],
            ).fetchall():
                iv_rank[str(row[0]).upper()] = {
                    "n_obs": int(row[1] or 0),
                    "iv_min": float(row[2] or 0.0),
                    "iv_max": float(row[3] or 0.0),
                }
            # Second pass: percent_rank of current IV among history.
            for row in con.execute(
                f"WITH hist AS ("
                f"  SELECT symbol, as_of, iv_ann, "
                f"    PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY iv_ann) AS pr "
                f"  FROM options_sentiment "
                f"  WHERE symbol IN ({placeholders}) AND iv_ann IS NOT NULL "
                f"    AND as_of <= ? AND as_of >= (CAST(? AS DATE) - INTERVAL 365 DAY)"
                f") SELECT symbol, pr FROM hist WHERE as_of = ?",
                [*syms, effective_date, effective_date, effective_date],
            ).fetchall():
                sym_u = str(row[0]).upper()
                if sym_u in iv_rank:
                    iv_rank[sym_u]["pct_rank"] = float(row[1]) if row[1] is not None else None
        except duckdb.Error:
            iv_rank = {}
        tenor_vol: dict[str, dict[str, float]] = {}
        try:
            for row in con.execute(
                f"SELECT symbol, "
                f"  SUM(CASE WHEN days_to_exp <= 21 THEN volume ELSE 0 END) AS short_v, "
                f"  SUM(CASE WHEN days_to_exp >= 121 THEN volume ELSE 0 END) AS long_v, "
                f"  SUM(volume) AS total_v "
                f"FROM options_chain_quotes "
                f"WHERE as_of = ? AND symbol IN ({placeholders}) AND volume IS NOT NULL "
                f"GROUP BY symbol",
                [effective_date, *syms],
            ).fetchall():
                tenor_vol[str(row[0]).upper()] = {
                    "short": float(row[1] or 0.0),
                    "long": float(row[2] or 0.0),
                    "total": float(row[3] or 0.0),
                }
        except duckdb.Error:
            tenor_vol = {}
    finally:
        con.close()

    for sym in syms:
        sent = sentiment.get(sym) or {}
        tenor = tenor_vol.get(sym) or {}
        parts: list[str] = []

        pcz = round_or_none(sent.get("pc_ratio_z"))
        if pcz is None:
            parts.append("定位 n/a")
        elif pcz <= -1.0:
            parts.append("call 偏多(看涨定位)")
        elif pcz >= 1.0:
            parts.append("put 偏空(看跌定位)")
        else:
            parts.append("定位中性")

        iv = round_or_none(sent.get("iv_ann"))
        vrp = round_or_none(sent.get("vrp"))
        if iv is not None:
            em = f"IV {iv * 100:.0f}%"
            if vrp is not None:
                if vrp <= -0.05:
                    em += "·便宜(IV<实际波动)"
                elif vrp >= 0.05:
                    em += "·贵(IV>实际波动)"
                else:
                    em += "·合理"
            parts.append(em)

        skz = round_or_none(sent.get("skew_z"))
        if skz is not None:
            if skz >= 1.0:
                parts.append("下行恐惧高(put skew 抬升)")
            elif skz <= -1.0:
                parts.append("下行恐惧低(skew 平淡)")
            else:
                parts.append("下行恐惧中")

        total = tenor.get("total") or 0.0
        if total > 0:
            long_share = (tenor.get("long") or 0.0) / total
            short_share = (tenor.get("short") or 0.0) / total
            if long_share >= 0.15:
                parts.append("信仰久期长(远月堆积)")
            elif short_share >= 0.70:
                parts.append("信仰久期短(战术为主)")
            else:
                parts.append("信仰久期中")

        if parts:
            out[sym] = {"verdict": " | ".join(parts), **sent}
            rk = iv_rank.get(sym) or {}
            if rk:
                out[sym]["iv_rank_pct"] = (rk.get("pct_rank") or 0.0) * 100.0 if rk.get("pct_rank") is not None else None
                out[sym]["iv_rank_n"] = rk.get("n_obs")
    return out


def _iv_action_hint(vrp: float | None, iv_ann: float | None, skew_z: float | None,
                    pcz: float | None, long_share: float, short_share: float,
                    regime_state: str, iv_rank_pct: float | None = None) -> str:
    """Translate options readings into non-executable context language.

    iv_rank_pct overrides simpler VRP-only checks when available: an IV at the
    20th percentile of its own past year marks cheap directional vol context;
    an IV at the 80th percentile marks expensive-vol context.
    """
    if iv_ann is None:
        return "—"
    iv_pct = iv_ann * 100
    panic_regime = regime_state in {"press", "capitulation"}
    if iv_rank_pct is not None:
        if iv_rank_pct <= 20 and long_share >= 0.10:
            if panic_regime:
                return "🎯 远月方向 context(IV rank 低位+恐慌+久期长,0R)"
            return f"💎 远月方向 context(IV rank {iv_rank_pct:.0f}%,0R)"
        if iv_rank_pct >= 80 and (skew_z is None or skew_z >= 0.5):
            return f"⏸ 等回落(IV rank {iv_rank_pct:.0f}% 高位)"
        if iv_rank_pct >= 70 and short_share >= 0.50:
            return f"✂️ 高 IV context(IV rank {iv_rank_pct:.0f}%,不写卖方指令)"
    if vrp is not None and vrp <= -0.05 and long_share >= 0.10:
        if panic_regime and (skew_z is None or skew_z >= 0.5):
            return "🎯 远月方向 context(IV 便宜+恐慌+久期长,0R)"
        return "💎 远月方向 context(IV<HV,远月堆积,0R)"
    if vrp is not None and vrp >= 0.05 and short_share >= 0.50:
        return "✂️ 高 IV context(IV 贵+短端火,不写卖方指令)"
    if iv_pct >= 55 and (skew_z is None or skew_z >= 1.0):
        return "⏸ 等回落(IV 高位+put skew 抬升)"
    if vrp is not None and vrp <= -0.03:
        return "🟢 IV 偏便宜,方向成本 context(0R)"
    if vrp is not None and vrp >= 0.05:
        return "🔴 IV 偏贵,高 IV 风险 context(不写卖方指令)"
    return "观望"


def render_iv_view_section(payload: dict[str, Any], *, limit: int = 6) -> list[str]:
    """Per-stock IV / VRP / skew table with non-executable context hints."""
    verdicts = payload.get("options_verdicts") or {}
    if not verdicts:
        return [
            "## US 期权 IV 视图",
            "",
            "- 今日 `options_verdicts` 为空(可能 CBOE 拉取失败或 trade_plan 为空)。",
            "",
        ]

    regime_state = str((payload.get("risk_regime") or {}).get("state") or "hedge").lower()

    rows = []
    max_n = 0
    for sym, v in verdicts.items():
        iv = round_or_none(v.get("iv_ann"))
        rv = round_or_none(v.get("rv_ann"))
        vrp = round_or_none(v.get("vrp"))
        pcz = round_or_none(v.get("pc_ratio_z"))
        skz = round_or_none(v.get("skew_z"))
        iv_rk = round_or_none(v.get("iv_rank_pct"))
        iv_n = v.get("iv_rank_n") or 0
        if iv_n > max_n:
            max_n = int(iv_n)
        verdict_txt = str(v.get("verdict") or "")
        long_share = 0.15 if "信仰久期长" in verdict_txt else (0.0 if "信仰久期短" in verdict_txt else 0.08)
        short_share = 0.70 if "信仰久期短" in verdict_txt else (0.0 if "信仰久期长" in verdict_txt else 0.40)
        hint = _iv_action_hint(vrp, iv, skz, pcz, long_share, short_share, regime_state, iv_rank_pct=iv_rk)
        rows.append({
            "sym": sym, "iv": iv, "rv": rv, "vrp": vrp,
            "pcz": pcz, "skz": skz, "hint": hint,
            "verdict": verdict_txt, "iv_rk": iv_rk, "iv_n": iv_n,
        })

    rows.sort(key=lambda r: (
        0 if r["iv_rk"] is not None else 1,
        r["iv_rk"] if r["iv_rk"] is not None else 999.0,
        r["vrp"] if r["vrp"] is not None else 999.0,
    ))

    cheap_vol_context = [r for r in rows if "远月方向 context" in r["hint"]]
    expensive_vol_context = [r for r in rows if "高 IV context" in r["hint"] or "卖方" in r["hint"]]

    rank_phrase = (
        f"IV rank lookback 现在是 {max_n} 个交易日(目标 252,CBOE 收集起点 2026-03-10,随时间逼近 1Y)"
        if max_n > 0 else "IV rank 历史不足,本次仅按 VRP 排序"
    )
    rank_label = f"IV rank (N≤{max_n}d)" if max_n > 0 else "IV rank"
    lines = [
        "## US 期权 IV 视图",
        "",
        f"按 IV rank 升序排,最便宜的 IV 在前。用途是判断方向成本、crowding 和股票 timing。",
        f"{rank_phrase}。当前 tape **{regime_state}**;rank ≤20% 是历史低位方向成本 context,≥80% 是高 IV 风险 context。",
        "",
    ]
    if cheap_vol_context:
        names = " / ".join(r["sym"] for r in cheap_vol_context[:3])
        lines.append(f"- 🎯 **低 IV 方向 context ({len(cheap_vol_context)})**: {names}；0R")
    if expensive_vol_context:
        names = " / ".join(r["sym"] for r in expensive_vol_context[:3])
        lines.append(f"- ✂️ **高 IV 风险 context ({len(expensive_vol_context)})**: {names}；偏风险提示")
    if cheap_vol_context or expensive_vol_context:
        lines.append("")

    lines += [
        f"| Symbol | IV 30d | HV30 | VRP | {rank_label} | PC z | Skew z | Context |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in rows[:limit]:
        iv_s = f"{r['iv']*100:.0f}%" if r["iv"] is not None else "-"
        rv_s = f"{r['rv']*100:.0f}%" if r["rv"] is not None else "-"
        vrp_s = f"{r['vrp']*100:+.1f}pp" if r["vrp"] is not None else "-"
        rk_s = f"{r['iv_rk']:.0f}%" if r["iv_rk"] is not None else "-"
        pcz_s = f"{r['pcz']:+.1f}" if r["pcz"] is not None else "-"
        skz_s = f"{r['skz']:+.1f}" if r["skz"] is not None else "-"
        lines.append(
            f"| {r['sym']} | {iv_s} | {rv_s} | {vrp_s} | {rk_s} | {pcz_s} | {skz_s} | "
            f"{r['hint']} |"
        )
    lines.append("")
    return lines
