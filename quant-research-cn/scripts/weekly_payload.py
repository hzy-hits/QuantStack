#!/usr/bin/env python3
"""Generate A-share weekly summary payload from DuckDB.

Aggregates the past week's daily data into a concise weekly payload for agent analysis.

Usage:
    python scripts/weekly_payload.py                      # current week
    python scripts/weekly_payload.py --date 2026-03-15    # week ending on this date
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

DB_PATH = "data/quant_cn.duckdb"


def _fmt(v, d=2):
    if v is None:
        return "N/A"
    return f"{v:+.{d}f}" if d else f"{v:.0f}"


def _pct(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    return (cur / prev - 1.0) * 100.0


def generate_weekly_payload(as_of: date) -> str:
    con = duckdb.connect(DB_PATH, read_only=True)

    # Week boundaries
    weekday = as_of.weekday()
    if weekday >= 5:
        week_end = as_of - timedelta(days=weekday - 4)
    else:
        week_end = as_of
    week_start = week_end - timedelta(days=4)

    ws = week_start.isoformat()
    we = week_end.isoformat()
    prev_fri = (week_start - timedelta(days=3)).isoformat()

    lines = [
        f"# A股周度研究总结 — {ws} 至 {we}",
        "",
        f"> **生成时间:** {datetime.now().isoformat(timespec='minutes')}",
        "> **周期:** 周度聚合",
        "> **注意:** 所有数据由程序计算，agent负责叙事。非投资建议。",
        "",
        "---",
        "",
    ]

    # ── 1. 主要指数周度表现 ──────────────────────────────────────────────────
    lines += ["## 1. 主要指数 — 周度表现", ""]
    benchmarks = [
        ("000300.SH", "沪深300"),
        ("000016.SH", "上证50"),
        ("399006.SZ", "创业板指"),
        ("000905.SH", "中证500"),
        ("000852.SH", "中证1000"),
    ]
    lines += [
        "| 指数 | 周一收盘 | 周五收盘 | 周涨跌% | 周最高 | 周最低 |",
        "|------|---------|---------|---------|--------|--------|",
    ]
    for ts_code, name in benchmarks:
        rows = con.execute("""
            SELECT trade_date, close, high, low FROM prices
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
        """, [ts_code, ws, we]).fetchall()
        if not rows:
            lines.append(f"| {name} | N/A | N/A | N/A | N/A | N/A |")
            continue
        mon = rows[0][1]
        fri = rows[-1][1]
        hi = max(r[2] for r in rows if r[2])
        lo = min(r[3] for r in rows if r[3])
        ret = _pct(fri, mon)
        lines.append(f"| {name} | {_fmt(mon)} | {_fmt(fri)} | {_fmt(ret)}% | {_fmt(hi)} | {_fmt(lo)} |")
    lines += [""]

    # ── 2. 行业板块排名 ──────────────────────────────────────────────────────
    lines += ["## 2. 行业板块 — 周度涨跌排名", ""]
    sectors = con.execute("""
        SELECT sector_name,
               SUM(pct_chg) AS total_chg,
               SUM(main_net_in) AS total_flow
        FROM sector_fund_flow
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY sector_name
        ORDER BY total_chg DESC
    """, [ws, we]).fetchall()
    if sectors:
        lines += ["| 行业 | 周涨跌% | 主力净流入(万) |",
                  "|------|---------|---------------|"]
        for name, chg, flow in sectors:
            lines.append(f"| {name} | {_fmt(chg)}% | {_fmt(flow, 0)} |")
    lines += [""]

    # ── 3. 北向资金 ───────────────────────────────────────────────────────────
    lines += ["## 3. 北向资金 — 周度汇总", ""]
    nb = con.execute("""
        SELECT trade_date, net_amount
        FROM northbound_flow
        WHERE source = 'total' AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date ASC
    """, [ws, we]).fetchall()
    if nb:
        total_nb = sum(r[1] for r in nb if r[1])
        lines += [f"周度净流入: **{total_nb/1e4:.1f}亿元**", ""]
        lines += ["| 日期 | 净买入(万) |", "|------|-----------|"]
        for dt, amt in nb:
            lines.append(f"| {dt} | {_fmt(amt, 0)} |")
    lines += [""]

    # ── 4. 融资余额 ───────────────────────────────────────────────────────────
    lines += ["## 4. 融资余额变化", ""]
    margin = con.execute("""
        SELECT CAST(trade_date AS VARCHAR),
               SUM(COALESCE(rzye, 0)) / 1e8 AS total_yi
        FROM margin_detail
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """, [ws, we]).fetchall()
    if margin and len(margin) >= 2:
        start_bal = margin[0][1]
        end_bal = margin[-1][1]
        delta = end_bal - start_bal
        lines += [
            f"周初余额: {start_bal:.0f}亿 → 周末余额: {end_bal:.0f}亿 (Δ{delta:+.0f}亿)",
        ]
    lines += [""]

    # ── 5. 涨跌幅排行 ─────────────────────────────────────────────────────────
    lines += ["## 5. 个股周涨跌排行", ""]
    movers = con.execute("""
        WITH weekly AS (
            SELECT p1.ts_code,
                   p2.close / NULLIF(p1.close, 0) - 1.0 AS ret
            FROM (
                SELECT ts_code, close FROM prices
                WHERE trade_date = (SELECT MAX(trade_date) FROM prices WHERE trade_date <= ?)
                  AND close IS NOT NULL AND close > 0
            ) p1
            JOIN (
                SELECT ts_code, close FROM prices
                WHERE trade_date = (SELECT MAX(trade_date) FROM prices WHERE trade_date <= ?)
                  AND close IS NOT NULL
            ) p2 ON p1.ts_code = p2.ts_code
        )
        SELECT ts_code, ret * 100 AS pct FROM weekly ORDER BY ret DESC
    """, [prev_fri, we]).fetchall()
    if movers:
        lines += ["### 涨幅前10", ""]
        lines += ["| 代码 | 周涨跌% |", "|------|---------|"]
        for tc, pct in movers[:10]:
            lines.append(f"| {tc} | {_fmt(pct)}% |")
        lines += ["", "### 跌幅前10", ""]
        lines += ["| 代码 | 周涨跌% |", "|------|---------|"]
        for tc, pct in movers[-10:][::-1]:
            lines.append(f"| {tc} | {_fmt(pct)}% |")
    lines += [""]

    # ── 6. HMM Regime ─────────────────────────────────────────────────────────
    lines += ["## 6. HMM市场状态 — 本周变化", ""]
    hmm = con.execute("""
        SELECT as_of, value, detail
        FROM analytics
        WHERE module = 'hmm' AND metric = 'p_bull'
          AND as_of >= ? AND as_of <= ?
        ORDER BY as_of ASC
    """, [ws, we]).fetchall()
    if hmm:
        lines += ["| 日期 | P(bull) |", "|------|---------|"]
        for dt, val, _ in hmm:
            lines.append(f"| {dt} | {_fmt(val, 3)} |")
    lines += [""]

    # ── 7. 业绩预告 ───────────────────────────────────────────────────────────
    lines += ["## 7. 本周业绩预告", ""]
    forecasts = con.execute("""
        SELECT ts_code, ann_date, forecast_type, summary
        FROM forecast
        WHERE ann_date >= ? AND ann_date <= ?
        ORDER BY ann_date DESC
        LIMIT 20
    """, [ws, we]).fetchall()
    if forecasts:
        lines += ["| 代码 | 公告日 | 类型 | 摘要 |", "|------|--------|------|------|"]
        for tc, dt, tp, summary in forecasts:
            s = (summary or "")[:40]
            lines.append(f"| {tc} | {dt} | {tp or '?'} | {s} |")
    else:
        lines += ["本周无业绩预告。"]
    lines += [""]

    # ── 8. 下周限售解禁 ───────────────────────────────────────────────────────
    lines += ["## 8. 下周限售解禁预览", ""]
    next_week_start = (week_end + timedelta(days=3)).isoformat()
    next_week_end = (week_end + timedelta(days=7)).isoformat()
    unlocks = con.execute("""
        SELECT ts_code, float_date,
               float_share / 1e4 AS float_wan
        FROM share_unlock
        WHERE float_date >= ? AND float_date <= ?
          AND float_share > 0
        ORDER BY float_share DESC
        LIMIT 10
    """, [next_week_start, next_week_end]).fetchall()
    if unlocks:
        lines += ["| 代码 | 解禁日 | 解禁量(万股) |", "|------|--------|-------------|"]
        for tc, dt, shares in unlocks:
            lines.append(f"| {tc} | {dt} | {_fmt(shares, 0)} |")
    else:
        lines += ["下周无重大限售解禁。"]
    lines += ["", "---", ""]

    con.close()
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate A-share weekly payload")
    parser.add_argument("--date", default=None, help="Week ending date")
    args = parser.parse_args()

    if args.date:
        parts = args.date.split("-")
        as_of = date(int(parts[0]), int(parts[1]), int(parts[2]))
    else:
        from datetime import datetime as dt
        import zoneinfo
        as_of = dt.now(zoneinfo.ZoneInfo("Asia/Shanghai")).date()

    payload = generate_weekly_payload(as_of)
    output = Path(f"reports/{as_of}_weekly_payload.md")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload)
    print(f"Weekly payload: {output} ({len(payload)} bytes)")


if __name__ == "__main__":
    main()
