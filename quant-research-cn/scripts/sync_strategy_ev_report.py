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
import re
from pathlib import Path
from typing import Any

import duckdb


SECTION_TITLE = "## Alpha状态"
LEGACY_SECTION_TITLES = ("## 策略EV账本",)


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


def compact_key_zh(key: str) -> str:
    label = {
        "early_accumulation": "早期吸筹",
        "structural_core": "结构核心",
        "continuation_breakout": "突破延续",
        "shadow_option_edge": "影子期权",
        "next_open_or_pullback": "次日开盘或回踩",
        "shadow_low": "低波动",
        "shadow_mid": "中波动",
        "shadow_high": "高波动",
        "setup_strong": "强蓄势",
        "setup_mixed": "混合蓄势",
        "setup_weak": "弱蓄势",
    }
    return " / ".join(label.get(part, part) for part in compact_key(key).split(" / "))


def fail_zh(fail: str) -> str:
    if not fail:
        return "-"
    labels = {
        "samples_lt_8": "样本少于8",
        "fills_lt_4": "成交样本少于4",
        "fill_rate_lt_35pct": "成交率不足35%",
        "ev_not_positive_enough": "历史EV不够",
        "tail_loss_gt_5_5pct": "尾部亏损偏大",
    }
    return "、".join(labels.get(part, part) for part in fail.split(",") if part)


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
    eligible_strategies = [row for row in strategies if bool(row[8])]
    eligible_candidates = [row for row in candidates if bool(row[11])]
    best_strategy = eligible_strategies[0] if eligible_strategies else (strategies[0] if strategies else None)

    if ev_status == "passed":
        execution_state = "30日稳定门禁已放行，可按执行层规则筛出正式候选。"
    elif ev_status == "pending":
        execution_state = "30日稳定门禁还没跑完，不能把候选写成正式执行信号。"
    elif ev_status == "failed":
        execution_state = "30日稳定门禁未放行，所以今天没有正式执行信号。"
    else:
        execution_state = f"30日稳定门禁状态为 `{ev_status}`，需要人工复核。"

    if best_strategy is not None:
        best_key, samples, fills, p_win, ev_pct, risk_unit, ev_per_risk, ev_norm, eligible, fail = best_strategy
        best_line = (
            f"本地纸面回放里，最好的一族是「{compact_key_zh(str(best_key))}」："
            f"样本 {samples}、成交 {fills}、历史EV {fmt(ev_pct)}%、EV得分 {fmt(ev_norm, 1)}，"
            f"{'可进入复核层' if eligible else '仍未过本地EV'}。"
        )
    else:
        best_line = "本地纸面回放暂无可用策略族。"

    candidate_codes = "、".join(str(row[0]) for row in eligible_candidates[:6]) or "无"

    lines: list[str] = [
        SECTION_TITLE,
        "",
        f"结论：{execution_state}",
        "",
        f"- 今天不是没有候选：系统记录了 {total or 0} 个候选，其中 {trade_count or 0} 个进入纸面交易、{observe_count or 0} 个只观察。",
        f"- 今天不是正式买入清单：30日门禁截至 {evaluated} 仍未选出可稳定复用的执行策略。",
        f"- 本地有正向线索：{best_line}",
        f"- 可复核但未升级执行的代码：{candidate_codes}。",
        "- A股同日候选显示 pending 是正常的：T+1 还没到可评价窗口，不代表信号没生成。",
        "",
    ]

    if eligible_candidates:
        lines += [
            "### 复核名单",
            "",
            "| 代码 | 名称 | 系统分组 | 计划观察价 | 历史EV | 样本 | 状态 |",
            "|---|---|---|---:|---:|---:|---|",
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
        ) in eligible_candidates:
            lines.append(
                f"| {symbol} | {name or '-'} | {family} | {fmt(planned_entry)} | "
                f"{fmt(ev_pct)}% | {samples or '-'} | 回踩/盘口复核，不是执行信号 |"
            )
        lines.append("")

    if strategies:
        lines += [
            "### 系统审计",
            "",
            "| 策略族 | 样本 | 成交 | 历史EV | EV得分 | 结论 |",
            "|---|---:|---:|---:|---:|---|",
        ]
        for key, samples, fills, p_win, ev_pct, risk_unit, ev_per_risk, ev_norm, eligible, fail in strategies[:6]:
            lines.append(
                f"| {compact_key_zh(str(key))} | {samples} | {fills} | {fmt(ev_pct)}% | "
                f"{fmt(ev_norm, 1)} | {'复核层通过' if eligible else fail_zh(fail)} |"
            )
        lines.append("")

    lines.append(
        f"内部审计：db_status=`{db_status}`，selected_policy=`{selected}`。这两个字段只用于排错，不应被当成交易结论。"
    )
    return "\n".join(lines).rstrip() + "\n"


def build_scorecard_section(reports_dir: Path, db_path: Path, date: str) -> str:
    ev_status, _selected, evaluated = load_bulletin_status(reports_dir, date)
    strategies, candidates, counts, db_status = query_rows(db_path, date)
    total, trade_count, observe_count = counts or (0, 0, 0)
    eligible_candidates = [row for row in candidates if bool(row[11])]
    eligible_strategies = [row for row in strategies if bool(row[8])]
    best_strategy = eligible_strategies[0] if eligible_strategies else (strategies[0] if strategies else None)

    if ev_status == "failed":
        gate_sentence = f"30日稳定门禁已评估到 {evaluated}，但没有放行正式执行策略。"
    elif ev_status == "pending":
        gate_sentence = "30日稳定门禁尚未完成评估，当前不能把候选写成正式执行信号。"
    elif ev_status == "passed":
        gate_sentence = "30日稳定门禁已放行，正式执行候选仍需满足当日执行约束。"
    else:
        gate_sentence = f"30日稳定门禁状态为 `{ev_status}`，需要人工复核。"

    if best_strategy is not None:
        key, samples, fills, _p_win, ev_pct, _risk_unit, _ev_per_risk, ev_norm, eligible, fail = best_strategy
        best_sentence = (
            f"本地纸面回放最好的一族是「{compact_key_zh(str(key))}」，"
            f"样本 {samples}、成交 {fills}、历史EV {fmt(ev_pct)}%、EV得分 {fmt(ev_norm, 1)}；"
            f"{'可进入复核层' if eligible else fail_zh(fail)}。"
        )
    else:
        best_sentence = "本地纸面回放暂无可用策略族。"

    codes = "、".join(str(row[0]) for row in eligible_candidates[:6]) or "无"
    return "\n".join(
        [
            "## 信号记分卡",
            (
                "本期没有到期的上一期正式执行信号需要复盘。"
                f"系统今天记录 {total or 0} 个候选，其中 {trade_count or 0} 个进入纸面交易、{observe_count or 0} 个只观察。"
                f"{gate_sentence}"
                f"{best_sentence}"
                f"可复核但未升级执行的代码：{codes}。"
                "因此今天的问题不是“没有股票”，而是“有候选，但还没有通过历史EV和稳定门禁成为正式执行信号”。"
            ),
        ]
    ).rstrip() + "\n"


def replace_h2_section(report_text: str, title: str, section: str) -> str:
    if title not in report_text:
        return report_text
    start = report_text.index(title)
    next_heading = report_text.find("\n## ", start + len(title))
    if next_heading == -1:
        return report_text[:start].rstrip() + "\n\n" + section
    return report_text[:start].rstrip() + "\n\n" + section + "\n" + report_text[next_heading + 1 :].lstrip()


def replace_h3_section(report_text: str, title: str, section: str) -> str:
    if title not in report_text:
        return report_text
    start = report_text.index(title)
    next_heading = report_text.find("\n### ", start + len(title))
    next_h2 = report_text.find("\n## ", start + len(title))
    candidates = [pos for pos in (next_heading, next_h2) if pos != -1]
    end = min(candidates) if candidates else -1
    if end == -1:
        return report_text[:start].rstrip() + "\n\n" + section
    return report_text[:start].rstrip() + "\n\n" + section + "\n" + report_text[end + 1 :].lstrip()


def needs_internal_cleanup(text: str) -> bool:
    banned = (
        "Recall→EV",
        "Recall → EV",
        "Stable Alpha Bulletin",
        "Stable Champion",
        "EV unknown",
        "no stable champion policy",
        "CORE BOOK",
        "TACTICAL CONTINUATION",
    )
    return any(term in text for term in banned)


def sanitize_report_language(report_text: str, reports_dir: Path, db_path: Path, date: str) -> str:
    if needs_internal_cleanup(report_text):
        report_text = replace_h2_section(
            report_text,
            "## 信号记分卡",
            build_scorecard_section(reports_dir, db_path, date),
        )
        report_text = replace_h3_section(
            report_text,
            "### 做多",
            "\n".join(
                [
                    "### 做多",
                    "",
                    "本期无可执行做多。原因不是没有候选，而是30日稳定门禁未放行；当前正EV候选只能进入回踩/盘口复核层。Headline=uncertain、gate_multiplier=0.70 时，个股线索不能上升成市场主线。",
                ]
            )
            + "\n",
        )

    replacements = {
        "Recall→EV→Gate 转化链断裂": "候选已召回，但历史EV/30日稳定门禁未放行",
        "Recall → EV → Gate 转化链断裂": "候选已召回，但历史EV/30日稳定门禁未放行",
        "CN Stable Alpha Bulletin": "30日稳定门禁",
        "Stable Alpha Bulletin": "30日稳定门禁",
        "Stable Champion Policy=none": "30日稳定门禁未放行",
        "EV unknown: no stable champion policy": "30日稳定门禁未放行",
        "CORE BOOK": "主候选池",
        "TACTICAL CONTINUATION": "战术延续",
        "Blocked Chase": "追价阻断",
        "Setup Alpha": "复核层Alpha",
        "paper-EV": "本地历史EV",
        "本地 本地历史EV": "本地历史EV",
        "Post-Event": "事件次日",
        "payload": "系统",
        "RANGE/THEME": "区间/主题",
        "事件次日 次日": "事件次日",
        "(CORE)": "(主候选)",
        "（CORE）": "（主候选）",
        "(THEME": "(主题观察",
        "（THEME": "（主题观察",
        "(Recall": "(召回候选",
        "（Recall": "（召回候选",
    }
    for old, new in replacements.items():
        report_text = report_text.replace(old, new)
    report_text = re.sub(r"\(CORE([,，])", r"(主候选\1", report_text)
    report_text = re.sub(r"（CORE([,，])", r"（主候选\1", report_text)
    report_text = report_text.replace("本地 本地历史EV", "本地历史EV")
    return report_text


def replace_or_insert(report_text: str, section: str) -> str:
    for title in (SECTION_TITLE, *LEGACY_SECTION_TITLES):
        if title not in report_text:
            continue
        start = report_text.index(title)
        next_heading = report_text.find("\n## ", start + len(title))
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
    text = sanitize_report_language(text, args.reports_dir, args.db, args.date)
    args.report.write_text(replace_or_insert(text, section), encoding="utf-8")
    print(f"Strategy EV section synced into {args.report}")


if __name__ == "__main__":
    main()
