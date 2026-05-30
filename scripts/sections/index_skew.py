"""US 指数级 skew 视图 — index-level options skew / tail-risk context.

Complements the per-stock IV view (scripts/sections/iv_view.py): that table
covers individual basket names; this one covers the *index* surfaces
(^SPX / ^NDX / SPY / QQQ / ^XSP / ^VIX), where skew encodes market-wide
tail-risk pricing rather than single-name positioning.

Two readings per index:
  - skew term structure : iv_skew (OTM-put IV / OTM-call IV) at the front
    tenor vs ~30d. A rising term structure = the crowd pays up for
    longer-dated downside protection (tail hedging building).
  - skew z-score        : skew_z from options_sentiment where available
    (SPY / QQQ), i.e. how stretched today's skew is vs its own history.

All numbers are read straight from the DB — non-executable context (0R),
never a traded instrument.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from lib.fmt import round_or_none

# Index surfaces worth a market-wide skew read, in display order.
INDEX_SKEW_SYMBOLS = ["^SPX", "^NDX", "SPY", "QQQ", "^XSP", "^VIX"]
_TERM_TARGET_DTE = 30  # tenor (days) we treat as the "term" skew point


def build_index_skew(us_db: Path, as_of: date) -> dict[str, dict[str, Any]]:
    """Per-index skew term structure + z-score, from existing options tables."""
    out: dict[str, dict[str, Any]] = {}
    if not us_db.exists():
        return out
    syms = INDEX_SKEW_SYMBOLS
    placeholders = ", ".join("?" for _ in syms)
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        # Effective date: latest options_analysis snapshot on/before as_of.
        effective_date = as_of.isoformat()
        row = con.execute(
            "SELECT MAX(as_of) FROM options_analysis WHERE as_of <= ?",
            [as_of.isoformat()],
        ).fetchone()
        if row and row[0]:
            effective_date = str(row[0])

        # Skew term structure: front tenor (min dte) + the tenor closest to 30d.
        per_tenor: dict[str, list[tuple[int, float, float]]] = {}
        try:
            for r in con.execute(
                f"SELECT symbol, days_to_exp, iv_skew, atm_iv "
                f"FROM options_analysis "
                f"WHERE as_of = ? AND symbol IN ({placeholders}) "
                f"  AND iv_skew IS NOT NULL AND days_to_exp IS NOT NULL "
                f"ORDER BY symbol, days_to_exp",
                [effective_date, *syms],
            ).fetchall():
                per_tenor.setdefault(str(r[0]).upper(), []).append(
                    (int(r[1]), float(r[2]), float(r[3] or 0.0))
                )
        except duckdb.Error:
            per_tenor = {}

        # skew_z + vrp context where the sentiment surface has it (SPY/QQQ).
        sentiment: dict[str, dict[str, Any]] = {}
        try:
            sdate = effective_date
            srow = con.execute(
                "SELECT MAX(as_of) FROM options_sentiment WHERE as_of <= ?",
                [as_of.isoformat()],
            ).fetchone()
            if srow and srow[0]:
                sdate = str(srow[0])
            for r in con.execute(
                f"SELECT symbol, skew_z, skew_raw, vrp, iv_ann "
                f"FROM options_sentiment WHERE as_of = ? AND symbol IN ({placeholders})",
                [sdate, *syms],
            ).fetchall():
                sentiment[str(r[0]).upper()] = {
                    "skew_z": r[1], "skew_raw": r[2], "vrp": r[3], "iv_ann": r[4],
                }
        except duckdb.Error:
            sentiment = {}
    finally:
        con.close()

    for sym in syms:
        tenors = per_tenor.get(sym) or []
        sent = sentiment.get(sym) or {}
        if not tenors and not sent:
            continue
        rec: dict[str, Any] = {"as_of": effective_date}
        if tenors:
            front_dte, front_skew, front_atm = tenors[0]
            # tenor closest to the 30d target.
            term_dte, term_skew, _ = min(tenors, key=lambda t: abs(t[0] - _TERM_TARGET_DTE))
            rec.update({
                "front_dte": front_dte,
                "front_skew": front_skew,
                "front_atm_iv": front_atm,
                "term_dte": term_dte,
                "term_skew": term_skew,
                "skew_slope": term_skew - front_skew,
            })
        rec["skew_z"] = sent.get("skew_z")
        rec["skew_raw"] = sent.get("skew_raw")
        rec["vrp"] = sent.get("vrp")
        rec["iv_ann"] = sent.get("iv_ann")
        out[sym] = rec
    return out


def _slope_label(slope: float | None) -> str:
    if slope is None:
        return ""
    if slope >= 0.20:
        return "陡峭上行(尾部对冲需求building)"
    if slope >= 0.05:
        return "温和上行"
    if slope <= -0.10:
        return "倒挂(近端恐慌>远端)"
    return "平坦"


def render_index_skew_section(payload: dict[str, Any]) -> list[str]:
    """Index-level skew term-structure table (SPX/NDX/SPY/QQQ/XSP/VIX)."""
    data = payload.get("index_skew") or {}
    if not data:
        return [
            "## US 指数级 skew(尾部风险)",
            "",
            "- 今日 `index_skew` 为空(可能 CBOE 指数期权拉取失败)。",
            "",
        ]
    lines = [
        "## US 指数级 skew(尾部风险)",
        "",
        "指数期权 skew = OTM put IV / OTM call IV。>1 表示市场为下行多付权利金。"
        "比较**近端 vs ~30d** 的 skew 期限结构:上行 = 尾部对冲需求在累积;skew z 衡量"
        "今日 skew 相对自身历史的拉伸度。全段为 0R context,不是交易标的。",
        "",
        "| Index | 近端 skew (dte) | ~30d skew (dte) | 期限结构 | skew z | VRP |",
        "|---|---:|---:|---|---:|---:|",
    ]
    for sym in INDEX_SKEW_SYMBOLS:
        rec = data.get(sym)
        if not rec:
            continue
        fs = rec.get("front_skew")
        fd = rec.get("front_dte")
        ts = rec.get("term_skew")
        td = rec.get("term_dte")
        slope = rec.get("skew_slope")
        skz = round_or_none(rec.get("skew_z"))
        vrp = round_or_none(rec.get("vrp"))
        front_s = f"{fs:.2f} ({fd}d)" if fs is not None else "-"
        term_s = f"{ts:.2f} ({td}d)" if ts is not None else "-"
        slope_s = _slope_label(slope) + (f" {slope:+.2f}" if slope is not None else "")
        skz_s = f"{skz:+.2f}" if skz is not None else "-"
        vrp_s = f"{vrp * 100:+.1f}pp" if vrp is not None else "-"
        lines.append(
            f"| {sym} | {front_s} | {term_s} | {slope_s} | {skz_s} | {vrp_s} |"
        )
    lines.append("")
    return lines
