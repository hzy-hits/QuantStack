"""SPX × P/C 4-quadrant Market Regime Score (Phase B.1).

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. Builds MRS from SPY 60d price + P/C history
and renders the section to the US daily report.
"""
from __future__ import annotations

import math
import statistics as stat
from datetime import date
from pathlib import Path
from typing import Any

from lib.db_helpers import _connect_ro


def build_market_regime_score(us_db: Path, as_of: date) -> dict[str, Any]:
    """SPX 价格 × P/C 4 象限 + 综合 MRS 评分.

    Empirical (44d sample, SPY 2026-03~2026-05):
      Quadrant I  (SPX↑ P/C↓): N=12 fwd_5d +1.86%, win 100%
      Quadrant II (SPX↑ P/C↑): N=19 fwd_5d +1.66%, win 79%
      Quadrant III(SPX↓ P/C↑): N=10 fwd_5d +0.24%, win 40%   ← worst
      Quadrant IV (SPX↓ P/C↓): N= 3 fwd_5d +1.61%, win 67% (capitulation)

      MRS bucket    N    fwd_5d  win
      >0.4         10    +1.61%  100%
      0.1~0.4      15    +2.02%   80%
      -0.1~0.1      4    +0.32%   75%
      -0.4~-0.1     9    +1.42%   67%
      <-0.4         6    +0.07%   33%

    Formula:
      MRS = 0.5 tanh(r5d/σ_r) + 0.3 tanh(-Δpc5d/σ_dpc) + 0.2 tanh(-(pc-pc_avg)/σ_pc)
    """
    out: dict[str, Any] = {"as_of": as_of.isoformat(), "available": False}
    if not us_db.exists():
        return out
    con = _connect_ro(us_db)
    try:
        rows = con.execute("""
            WITH p AS (
              SELECT date, close FROM prices_daily
              WHERE symbol='SPY' AND close IS NOT NULL AND date <= CAST(? AS DATE)
              ORDER BY date DESC LIMIT 120
            ), s AS (
              SELECT as_of, pc_ratio_raw FROM options_sentiment
              WHERE symbol='SPY' AND pc_ratio_raw IS NOT NULL
                AND as_of <= CAST(? AS DATE)
              ORDER BY as_of DESC LIMIT 120
            )
            SELECT p.date, p.close, s.pc_ratio_raw
            FROM p INNER JOIN s ON p.date = s.as_of
            ORDER BY p.date
        """, [as_of.isoformat(), as_of.isoformat()]).fetchall()
    finally:
        con.close()
    if len(rows) < 10:
        out["state"] = f"insufficient_history({len(rows)}d)"
        return out
    dates = [r[0] for r in rows]
    close = [float(r[1]) for r in rows]
    pc = [float(r[2]) for r in rows]
    N = len(rows)
    if N < 6:
        out["state"] = "need_5d_window"
        return out
    i = N - 1
    r1 = (close[i] / close[i-1] - 1) * 100
    r5 = (close[i] / close[i-5] - 1) * 100
    dpc1 = pc[i] - pc[i-1]
    dpc5 = pc[i] - pc[i-5]
    if len(close) < 6:
        return out
    r5_series = [(close[k] / close[k-5] - 1) * 100 for k in range(5, N)]
    dpc5_series = [pc[k] - pc[k-5] for k in range(5, N)]
    sigma_r = stat.stdev(r5_series) if len(r5_series) > 1 else 1.0
    sigma_dpc = stat.stdev(dpc5_series) if len(dpc5_series) > 1 else 0.1
    sigma_pc = stat.stdev(pc) if len(pc) > 1 else 0.1
    pc_avg = stat.mean(pc)
    momentum = math.tanh(r5 / sigma_r) if sigma_r else 0.0
    fear_change = math.tanh(-dpc5 / sigma_dpc) if sigma_dpc else 0.0
    fear_level = math.tanh(-(pc[i] - pc_avg) / sigma_pc) if sigma_pc else 0.0
    mrs = 0.5 * momentum + 0.3 * fear_change + 0.2 * fear_level
    if r5 > 0 and dpc5 < 0: quad, quad_label = "I", "SPX↑ P/C↓ 确认bull"
    elif r5 > 0 and dpc5 >= 0: quad, quad_label = "II", "SPX↑ P/C↑ 涨中加保护"
    elif r5 <= 0 and dpc5 > 0: quad, quad_label = "III", "SPX↓ P/C↑ 确认bear"
    else: quad, quad_label = "IV", "SPX↓ P/C↓ capitulation"
    if mrs > 0.4: bucket, bucket_hist = "强看涨", "fwd_5d +1.61% / 胜率 100% (N=10)"
    elif mrs > 0.1: bucket, bucket_hist = "看涨", "fwd_5d +2.02% / 胜率 80% (N=15)"
    elif mrs > -0.1: bucket, bucket_hist = "中性", "fwd_5d +0.32% / 胜率 75% (N=4)"
    elif mrs > -0.4: bucket, bucket_hist = "看跌", "fwd_5d +1.42% / 胜率 67% (N=9)"
    else: bucket, bucket_hist = "强看跌", "fwd_5d +0.07% / 胜率 33% (N=6)"
    out.update({
        "available": True,
        "data_date": dates[i].isoformat(),
        "spy_close": close[i],
        "r5d_pct": r5, "r1d_pct": r1,
        "pc_now": pc[i], "pc_5d_ago": pc[i-5], "dpc_5d": dpc5, "dpc_1d": dpc1,
        "pc_avg_60d": pc_avg,
        "mrs": mrs,
        "momentum_term": momentum,
        "fear_change_term": fear_change,
        "fear_level_term": fear_level,
        "quadrant": quad, "quadrant_label": quad_label,
        "mrs_bucket": bucket, "bucket_history": bucket_hist,
        "sample_n": len(rows),
    })
    return out


def render_market_regime_score_section(payload: dict[str, Any]) -> list[str]:
    mrs = payload.get("market_regime_score") or {}
    lines = ["## 📊 SPX × P/C 市场体感 (MRS)", ""]
    if not mrs.get("available"):
        lines += [f"- 数据不足:{mrs.get('state','unknown')}", ""]
        return lines
    quad = mrs["quadrant"]
    quad_label = mrs["quadrant_label"]
    pc_now = mrs["pc_now"]
    dpc = mrs["dpc_5d"]
    r5 = mrs["r5d_pct"]
    mrs_val = mrs["mrs"]
    bucket = mrs["mrs_bucket"]
    bh = mrs["bucket_history"]
    lines += [
        f"- 数据日 **{mrs['data_date']}**(基于 SPY,N={mrs['sample_n']}d 历史)",
        f"- SPY ${mrs['spy_close']:.2f},5d 涨幅 **{r5:+.2f}%**;P/C **{pc_now:.3f}**(5d 变化 **{dpc:+.3f}**)",
        f"- 象限 **{quad} ({quad_label})**",
        f"- **MRS = {mrs_val:+.3f}** → **{bucket}**;历史类似 setup → {bh}",
        f"  - 拆分:momentum {mrs['momentum_term']:+.2f} × 0.5 + fear变化 {mrs['fear_change_term']:+.2f} × 0.3 + fear水平 {mrs['fear_level_term']:+.2f} × 0.2",
        "",
        "**4 象限 forward 5d 胜率(SPY 44d 样本)**:",
        "",
        "| 象限 | 描述 | N | fwd 5d | 胜率 |",
        "|:---:|---|---:|---:|---:|",
        "| **I** | SPX↑ P/C↓(确认 bull) | 12 | +1.86% | **100%** |",
        "| **II** | SPX↑ P/C↑(涨中加保护) | 19 | +1.66% | 79% |",
        "| **III** | SPX↓ P/C↑(确认 bear) | 10 | +0.24% | **40%** |",
        "| **IV** | SPX↓ P/C↓(capitulation) | 3 | +1.61% | 67% |",
        "",
        "_样本仅 44 天,pattern 一致但需更长 history 严格 validate。_",
        "",
    ]
    return lines
