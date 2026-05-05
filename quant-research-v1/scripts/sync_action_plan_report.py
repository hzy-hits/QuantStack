#!/usr/bin/env python3
"""Sync the deterministic Action Plan Ledger into the final Chinese report."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


SYNC_INTRO = (
    "系统自动生成的价格计划，不是订单。正式执行仍以 Stable Alpha / Execution Alpha "
    "放行为准；未放行的名字只能复核、等待或回避。"
)


def _section(text: str, heading: str, next_headings: tuple[str, ...]) -> str:
    start = text.find(heading)
    if start < 0:
        return ""
    body_start = start + len(heading)
    end = len(text)
    for marker in next_headings:
        idx = text.find(marker, body_start)
        if idx >= 0:
            end = min(end, idx)
    return text[body_start:end].strip()


def _parse_table(section: str) -> list[dict[str, str]]:
    lines = [line.strip() for line in section.splitlines()]
    rows: list[dict[str, str]] = []
    headers: list[str] = []
    for line in lines:
        if not line.startswith("|") or "---" in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if not headers:
            headers = cells
            continue
        if len(cells) != len(headers):
            continue
        rows.append(dict(zip(headers, cells, strict=False)))
    return rows


def _fmt(value: str) -> str:
    value = value.strip()
    return value if value and value.upper() != "N/A" else "-"


def _direction(value: str) -> str:
    value = value.strip().lower()
    if value == "long":
        return "多头观察"
    if value == "short":
        return "回避/下行观察"
    if value == "neutral":
        return "中性"
    return value or "-"


def _time_exit(value: str) -> str:
    value = value.strip()
    if value == "3 sessions / next catalyst":
        return "3个交易日/下一催化剂前"
    return value or "-"


def _reason(value: str) -> str:
    value = value.strip()
    if not value:
        return "-"
    value = re.sub(
        r"positive-EV recall policy \(fills=(\d+), avg=([^)]+?), EV LCB=([^)]+?)\)",
        r"正EV复核策略（成交样本=\1，均值=\2，EV下界=\3）",
        value,
    )
    replacements = {
        "event known; require second-day acceptance": "事件已公开；只看第二日承接",
        "event known": "事件已公开",
        "require second-day acceptance": "只看第二日承接",
        "extended but confirmation still supports follow-through": "涨幅已扩展；仍需承接确认",
        "move already paid / stale chase risk": "涨幅已兑现/追高风险",
        "stale chase / already paid": "追高风险/已兑现",
        "R:R below execution floor": "R:R低于执行线",
        "not a setup-alpha candidate": "不属于复核型机会",
        "review only": "仅复核",
        "stable EV gate not passed": "稳定EV门禁未通过",
    }
    parts = [part.strip() for part in value.split(";") if part.strip()]
    translated = [replacements.get(part, part) for part in parts]
    return "；".join(translated) if translated else "-"


def _render_rows(level: str, rows: list[dict[str, str]], limit: int | None = None) -> list[str]:
    rendered: list[str] = []
    for row in rows[:limit]:
        rendered.append(
            "| {level} | {name} | {direction} | {entry} | {stop} | {target} | {rr} | {time_exit} | {reason} |".format(
                level=level,
                name=_fmt(row.get("Symbol / Company", "")),
                direction=_direction(row.get("Direction", "")),
                entry=_fmt(row.get("Entry / Review", "")),
                stop=_fmt(row.get("Stop / Invalid", "")),
                target=_fmt(row.get("Target", "")),
                rr=_fmt(row.get("R:R", "")),
                time_exit=_time_exit(row.get("Time exit", "")),
                reason=_reason(row.get("State reason", "")),
            )
        )
    return rendered


def build_action_plan_snippet(structural_text: str, max_blocked: int = 8) -> str:
    ledger = _section(
        structural_text,
        "## Action Plan Ledger",
        ("## Setup Alpha / Anti-Chase", "\n## Notable Items"),
    )
    if not ledger:
        return ""

    gate_rows = _parse_table(
        _section(ledger, "### Gate-Pass Plans", ("### Setup / Wait Plans", "### Blocked / No-Chase Plans"))
    )
    setup_rows = _parse_table(
        _section(ledger, "### Setup / Wait Plans", ("### Blocked / No-Chase Plans",))
    )
    blocked_rows = _parse_table(
        _section(ledger, "### Blocked / No-Chase Plans", ("\n---",))
    )

    if not gate_rows and not setup_rows and not blocked_rows:
        return ""

    lines = [
        "### 价格计划",
        "",
        SYNC_INTRO,
        "",
        "| 层级 | 代码 / 公司 | 方向 | 入场/复核 | 失效/止损 | 目标 | R:R | 时间退出 | 状态原因 |",
        "|------|-------------|------|-----------|-----------|------|-----|----------|----------|",
    ]
    lines.extend(_render_rows("可执行", gate_rows))
    lines.extend(_render_rows("复核/等待", setup_rows))
    lines.extend(_render_rows("不追/阻断", blocked_rows, max_blocked))
    return "\n".join(lines).strip()


def sync_action_plan(report_path: Path, structural_path: Path, max_blocked: int = 8) -> bool:
    report = report_path.read_text(encoding="utf-8")
    structural = structural_path.read_text(encoding="utf-8")
    snippet = build_action_plan_snippet(structural, max_blocked=max_blocked)
    if not snippet:
        return False

    # Backward-compatible cleanup for older generated reports that used hidden
    # markers, then remove any prior deterministic price-plan section.
    report = re.sub(
        r"\n?<!-- ACTION_PLAN_SYNC_START -->.*?<!-- ACTION_PLAN_SYNC_END -->\n?",
        "\n",
        report,
        flags=re.DOTALL,
    )
    report = re.sub(
        rf"\n?### 价格计划\n\n{re.escape(SYNC_INTRO)}.*?(?=\n### |\n## |\Z)",
        "\n",
        report,
        flags=re.DOTALL,
    )
    block = f"{snippet}\n\n"

    setup_idx = report.find("### Setup Alpha")
    if setup_idx >= 0:
        report = report[:setup_idx] + block + report[setup_idx:]
    else:
        trade_idx = report.find("## 交易地图")
        if trade_idx >= 0:
            next_section = report.find("\n## ", trade_idx + 1)
            insert_at = next_section if next_section >= 0 else len(report)
            report = report[:insert_at].rstrip() + "\n\n" + block + report[insert_at:].lstrip()
        else:
            report = report.rstrip() + "\n\n" + block

    report_path.write_text(report, encoding="utf-8")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync US action plan ledger into final report")
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--structural", required=True, type=Path)
    parser.add_argument("--max-blocked", type=int, default=8)
    args = parser.parse_args()

    if sync_action_plan(args.report, args.structural, max_blocked=args.max_blocked):
        print(f"Synced Action Plan Ledger into {args.report}")
    else:
        print("No Action Plan Ledger found; skipped")


if __name__ == "__main__":
    main()
