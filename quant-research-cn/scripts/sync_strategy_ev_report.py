#!/usr/bin/env python3
"""Insert the deterministic CN strategy-EV ledger into the final report.

The LLM merge report is allowed to summarize the payload, but it must not be the
only place where strategy lifecycle state is exposed. This script is deliberately
post-merge and deterministic: it reads DuckDB tables populated by
paper_trade_ev.rs and inserts/replaces one compact section in the final report.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


SECTION_TITLE = "## 策略EV账本"


def fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def compact_key(key: str) -> str:
    parts = key.split("|")
    if len(parts) >= 5:
        return f"{parts[0]} / {parts[2]} / {parts[3]} / {parts[4]}"
    return key


def load_bulletin_status(reports_dir: Path, date: str) -> tuple[str, str, str]:
    path = reports_dir / "review_dashboard" / "strategy_backtest" / date / "alpha_bulletin.json"
    if not path.exists():
        return "pending", "none", "bulletin missing"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return "error", "none", f"bulletin unreadable: {exc}"
    ev_status = (data.get("ev_status") or {}).get("cn") or data.get("ev_status") or "unknown"
    selected = (data.get("selected_policies") or {}).get("cn") or "none"
    evaluated = (data.get("evaluated_through") or {}).get("cn") or "-"
    return str(ev_status), str(selected), str(evaluated)


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def query_rows(
    db_path: Path, date: str
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], tuple[int, int, int] | None, str]:
    if not db_path.exists():
        return [], [], (0, 0, 0), f"db missing: {db_path}"
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return [], [], (0, 0, 0), f"db unreadable: {exc}"

    missing = [table for table in ("strategy_ev", "paper_trades") if not table_exists(con, table)]
    if missing:
        return [], [], (0, 0, 0), f"missing table(s): {', '.join(missing)}"

    strategies = con.execute(
        """
        SELECT strategy_key, samples, fills, win_rate_bayes, ev_pct, risk_unit_pct,
               ev_per_risk, ev_norm_score, eligible, COALESCE(fail_reasons, '')
        FROM strategy_ev
        WHERE as_of = CAST(? AS DATE)
        ORDER BY eligible DESC, ev_norm_score DESC, samples DESC
        LIMIT 8
        """,
        [date],
    ).fetchall()
    candidates = con.execute(
        """
        SELECT p.symbol, COALESCE(s.name, ''), p.strategy_family, p.action_intent,
               p.execution_rule, p.fill_status, p.label, p.planned_entry,
               ev.ev_pct, ev.ev_norm_score, ev.samples, ev.eligible, COALESCE(ev.fail_reasons, '')
        FROM paper_trades p
        LEFT JOIN stock_basic s ON s.ts_code = p.symbol
        LEFT JOIN strategy_ev ev ON ev.as_of = CAST(? AS DATE) AND ev.strategy_key = p.strategy_key
        WHERE p.report_date = CAST(? AS DATE) AND p.action_intent = 'TRADE'
        ORDER BY COALESCE(ev.eligible, FALSE) DESC, COALESCE(ev.ev_norm_score, -999) DESC, p.symbol
        LIMIT 12
        """,
        [date, date],
    ).fetchall()
    counts = con.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN action_intent = 'TRADE' THEN 1 ELSE 0 END) AS trade_count,
            SUM(CASE WHEN action_intent = 'OBSERVE' THEN 1 ELSE 0 END) AS observe_count
        FROM paper_trades
        WHERE report_date = CAST(? AS DATE)
        """,
        [date],
    ).fetchone()
    return strategies, candidates, counts, "ok"


def build_section(reports_dir: Path, db_path: Path, date: str) -> str:
    ev_status, selected, evaluated = load_bulletin_status(reports_dir, date)
    strategies, candidates, counts, db_status = query_rows(db_path, date)
    total, trade_count, observe_count = counts or (0, 0, 0)
    local_eligible = sum(1 for row in strategies if bool(row[8]))

    lines: list[str] = [
        SECTION_TITLE,
        "",
        (
            "这段是系统账本，直接来自 DuckDB，不由 agent 自由改写。"
            "Stable Champion（共享稳定冠军）是 30D rolling 门禁选出的冠军策略；它没有通过时，只表示"
            "“尚未升级为 Execution Alpha”，不等于本地 paper-EV 没有结果。"
        ),
        "",
        f"- Stable Champion: ev_status=`{ev_status}`，selected_policy=`{selected}`，evaluated_through=`{evaluated}`。",
        f"- Local Paper EV: db_status=`{db_status}`，策略族 {len(strategies)} 条，本地 paper-EV eligible {local_eligible} 条；候选生命周期 total={total or 0}, paper_trade={trade_count or 0}, observe={observe_count or 0}。",
        "- 同日 A 股 T+1 未完成时，当前候选会显示 `pending`；这是未到可评价窗口，不是没生成信号。",
        "",
    ]

    if strategies:
        lines += [
            "### 策略族 EV",
            "",
            "| 策略族 | n | fills | p_win | EV% | risk unit | EV/risk | EV norm | eligible | fail |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
        for key, samples, fills, p_win, ev_pct, risk_unit, ev_per_risk, ev_norm, eligible, fail in strategies:
            lines.append(
                f"| `{compact_key(str(key))}` | {samples} | {fills} | {fmt(p_win, 3)} | "
                f"{fmt(ev_pct)} | {fmt(risk_unit)} | {fmt(ev_per_risk, 3)} | "
                f"{fmt(ev_norm, 1)} | {'yes' if eligible else 'no'} | {fail or '-'} |"
            )
        lines.append("")

    if candidates:
        lines += [
            "### 当前候选生命周期",
            "",
            "| 代码 | 名称 | 策略族 | 执行规则 | fill | label | 计划价 | EV% | EV norm | n | paper-EV | blocker |",
            "|---|---|---|---|---|---|---:|---:|---:|---:|---|---|",
        ]
        for (
            symbol,
            name,
            family,
            _intent,
            execution_rule,
            fill_status,
            label,
            planned_entry,
            ev_pct,
            ev_norm,
            samples,
            eligible,
            fail,
        ) in candidates:
            lines.append(
                f"| {symbol} | {name or '-'} | {family} | {execution_rule} | {fill_status} | {label} | "
                f"{fmt(planned_entry)} | {fmt(ev_pct)} | {fmt(ev_norm, 1)} | {samples or '-'} | "
                f"{'pass' if eligible else 'no'} | {fail or '-'} |"
            )
        lines.append("")

    lines.append(
        "报告解释规则：`paper-EV pass` 可以进入 Setup/复核层；只有 stable champion 也通过，才允许写成 Execution Alpha。"
    )
    return "\n".join(lines).rstrip() + "\n"


def replace_or_insert(report_text: str, section: str) -> str:
    if SECTION_TITLE in report_text:
        start = report_text.index(SECTION_TITLE)
        next_heading = report_text.find("\n## ", start + len(SECTION_TITLE))
        if next_heading == -1:
            return report_text[:start].rstrip() + "\n\n" + section
        return report_text[:start].rstrip() + "\n\n" + section + "\n" + report_text[next_heading + 1 :].lstrip()

    marker = "\n## 今日市场"
    if marker in report_text:
        return report_text.replace(marker, "\n" + section + marker, 1)

    marker = "\n## 交易地图"
    if marker in report_text:
        return report_text.replace(marker, "\n" + section + marker, 1)

    return report_text.rstrip() + "\n\n" + section


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync CN Strategy EV ledger into final markdown report.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--db", default="data/quant_cn_report.duckdb", type=Path)
    parser.add_argument("--reports-dir", default="reports", type=Path)
    args = parser.parse_args()

    if not args.report.exists():
        raise SystemExit(f"report not found: {args.report}")
    section = build_section(args.reports_dir, args.db, args.date)
    text = args.report.read_text(encoding="utf-8")
    args.report.write_text(replace_or_insert(text, section), encoding="utf-8")
    print(f"Strategy EV section synced into {args.report}")


if __name__ == "__main__":
    main()
