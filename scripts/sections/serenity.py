"""Serenity Analysis cross-check section (Phase B.0).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Reads serenity_picks + serenity_stance_flips
tables and renders a 4-block cross-check section for the US daily report.
"""
from __future__ import annotations

import ast
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from lib.db_helpers import _connect_ro, table_exists


def build_serenity_crosscheck(us_db: Path, as_of: date, ranker_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Cross-check our US ranker + universe vs Serenity Analysis picks.

    Three dimensions:
      1. 24h fresh: Serenity 'last_mentioned_at' within past 24h (real recency)
      2. Stance flips: view_change.status != 'unchanged' (rarely fires —
         Serenity bumps view_change.changed_at every daily rebuild even when
         stance unchanged; only true stance/view transitions count)
      3. Disagreements: our rank ≥70 vs Serenity neutral/bearish (涨过头警报),
         and Serenity priority ≥80 bullish + present in our universe but
         our rank < 50 (missed)
    """
    out: dict[str, Any] = {"available": False}
    if not us_db.exists():
        return out
    con = _connect_ro(us_db)
    try:
        if not table_exists(con, "serenity_picks"):
            return out
        latest = con.execute(
            "SELECT MAX(fetched_at) FROM serenity_picks WHERE fetched_at <= ?",
            [datetime.combine(as_of, datetime.max.time())],
        ).fetchone()
        if not latest or not latest[0]:
            return out
        eff_fetched = latest[0]
        rows = con.execute("""
            SELECT ticker, ai_chain_segment, stance, current_view, confidence,
                   priority_score, latest_return_pct, last_mentioned_at, view_change,
                   ret_1w, ret_1m, ret_6m, ret_1y
            FROM serenity_picks WHERE fetched_at = ?
        """, [eff_fetched]).fetchall()
    finally:
        con.close()
    if not rows:
        return out
    # Count accumulated flips for progress indicator
    try:
        con2 = _connect_ro(us_db)
        if table_exists(con2, "serenity_stance_flips"):
            out["flips_accumulated"] = con2.execute(
                "SELECT COUNT(*) FROM serenity_stance_flips"
            ).fetchone()[0]
        con2.close()
    except duckdb.Error:
        pass
    serenity: dict[str, dict[str, Any]] = {}
    for r in rows:
        sym = (r[0] or "").upper()
        if not sym: continue
        vc = {}
        if r[8]:
            try: vc = ast.literal_eval(r[8])
            except (ValueError, SyntaxError): pass
        serenity[sym] = {
            "segment": r[1], "stance": r[2], "view": r[3], "confidence": r[4],
            "priority": r[5], "ret": r[6], "last_mentioned": r[7], "vc": vc,
            "ret_1w": r[9], "ret_1m": r[10], "ret_6m": r[11], "ret_1y": r[12],
        }
    # 24h fresh
    now = datetime.now(timezone(timedelta(hours=8)))
    fresh = []
    for sym, s in serenity.items():
        lm = s.get("last_mentioned")
        if not lm: continue
        try:
            lm_tz = lm.replace(tzinfo=timezone(timedelta(hours=8))) if lm.tzinfo is None else lm
            if (now - lm_tz).total_seconds() < 86400:
                fresh.append((sym, lm, s))
        except (AttributeError, TypeError): pass
    fresh.sort(key=lambda x: x[1], reverse=True)
    # Stance flips
    flips = []
    for sym, s in serenity.items():
        vc = s["vc"]
        if not vc: continue
        if vc.get("status") and vc.get("status") != "unchanged":
            flips.append((sym, vc.get("previous_stance"), vc.get("current_stance"),
                          vc.get("previous_view"), vc.get("current_view"), s["ret"]))
        elif vc.get("previous_stance") and vc.get("previous_stance") != s["stance"]:
            flips.append((sym, vc.get("previous_stance"), s["stance"],
                          vc.get("previous_view"), vc.get("current_view"), s["ret"]))
    # Disagreements
    ranker_map = {str(r.get("symbol") or "").upper(): float(r.get("rank_score") or 0)
                  for r in ranker_rows}
    overhead = []  # our rank ≥70, Serenity neutral/bearish (涨过头)
    underrated = []  # Serenity priority ≥80 bullish, our rank <50
    for sym, s in serenity.items():
        rk = ranker_map.get(sym)
        if rk is None: continue
        if rk >= 70 and s["stance"] in ("neutral", "bearish"):
            overhead.append((sym, rk, s["stance"], s["priority"], s["ret"], s["segment"]))
        if rk < 50 and s["stance"] == "bullish" and (s["priority"] or 0) >= 80:
            underrated.append((sym, rk, s["priority"], s["ret"], s["segment"]))
    overhead.sort(key=lambda x: -x[4])  # by accumulated return DESC (most-runs first)
    underrated.sort(key=lambda x: -(x[2] or 0))
    out.update({
        "available": True,
        "fetched_at": eff_fetched.isoformat() if hasattr(eff_fetched, "isoformat") else str(eff_fetched),
        "total_picks": len(serenity),
        "fresh_24h": fresh,
        "stance_flips": flips,
        "overhead_warnings": overhead,
        "underrated_warnings": underrated,
    })
    return out


def render_serenity_crosscheck_section(payload: dict[str, Any]) -> list[str]:
    sc = payload.get("serenity_crosscheck") or {}
    lines = ["## 🪞 Serenity 第三方视角 (cross-check, 仅观察不影响 ranker)", ""]
    if not sc.get("available"):
        lines += ["- serenity_picks 表当日无数据(跑 `python3 scripts/fetch_serenity_picks.py` 抓取)。", ""]
        return lines
    lines += [
        f"- 数据来源:Serenity Analysis (analysissite.vercel.app),共 {sc['total_picks']} 票",
        f"- 拉取时间:{sc['fetched_at']}",
    ]
    flips_progress = sc.get("flips_accumulated")
    if flips_progress is not None:
        if flips_progress < 30:
            lines.append(f"- Stance flips 累积进度:**{flips_progress} / 30** (够 30 个可跑 `backtest_serenity_flips.py`)")
        else:
            lines.append(f"- Stance flips 累积 **{flips_progress}** — 可运行 `python3 scripts/backtest_serenity_flips.py` 看 fwd 表现")
    lines.append("")
    # 1. Stance flips
    flips = sc.get("stance_flips") or []
    if flips:
        lines += [f"### ⚡ Stance 翻转 ({len(flips)})", "",
                  "| Symbol | prev → now | view 变化 | 累计 |",
                  "|---|:---:|---|---:|"]
        for s in flips[:3]:
            sym, ps, ns, pv, nv, ret = s
            arrow = f"{ps or '?'} → **{ns or '?'}**"
            v_arrow = f"{pv or '?'} → {nv or '?'}" if pv != nv else "—"
            lines.append(f"| **{sym}** | {arrow} | {v_arrow} | {ret:+.0f}% |")
        lines.append("")
    # 2. Fresh 24h mentions
    fresh = sc.get("fresh_24h") or []
    if fresh:
        lines += [f"### 🕒 过去 24h 被 Serenity 提到 ({len(fresh)})", "",
                  "| Symbol | stance | priority | 累计 | 1w | segment |",
                  "|---|:---:|---:|---:|---:|:---:|"]
        for sym, lm, s in fresh[:3]:
            ret_1w = f"{s.get('ret_1w', 0):+.1f}%" if s.get('ret_1w') is not None else "-"
            emoji = {"bullish":"🟢", "neutral":"⚪", "bearish":"🔴"}.get(s["stance"], "?")
            lines.append(
                f"| **{sym}** | {emoji} {s['stance']} | {s['priority']:.0f} | "
                f"{s['ret']:+.0f}% | {ret_1w} | {s['segment'] or '-'} |"
            )
        lines.append("")
    # 3. Overhead warnings (we rank ≥70, Serenity neutral/bearish)
    overhead = sc.get("overhead_warnings") or []
    if overhead:
        lines += [f"### ⚠️ 涨过头警报:我们 rank ≥ 70 但 Serenity 已转 neutral/bearish ({len(overhead)})", "",
                  "| Symbol | 我们 rank | Serenity | prio | 累计 | segment |",
                  "|---|---:|:---:|---:|---:|:---:|"]
        for sym, rk, st, prio, ret, seg in overhead[:3]:
            emoji = {"neutral":"⚪", "bearish":"🔴"}.get(st, "?")
            lines.append(f"| **{sym}** | {rk:.1f} | {emoji} {st} | {prio:.0f} | {ret:+.0f}% | {seg or '-'} |")
        lines.append("")
    # 4. Underrated (Serenity loves, we don't rank high)
    underrated = sc.get("underrated_warnings") or []
    if underrated:
        lines += [f"### 🔍 Serenity 高 prio bullish 但我们 rank < 50 ({len(underrated)})", "",
                  "| Symbol | 我们 rank | Serenity prio | 累计 | segment |",
                  "|---|---:|---:|---:|:---:|"]
        for sym, rk, prio, ret, seg in underrated[:3]:
            lines.append(f"| **{sym}** | {rk:.1f} | {prio:.0f} | {ret:+.0f}% | {seg or '-'} |")
        lines.append("")
    if not (flips or fresh or overhead or underrated):
        lines += ["- 今天 4 个维度都无信号。", ""]
    return lines
