from __future__ import annotations

import re
from typing import Any


SIGNAL_HEADING = "## Factor Lab Research Candidates"
SIGNAL_HEADING_RE = re.compile(
    r"(?ms)^## Factor Lab (?:Research Candidates|Independent Trading Signal)\s*\n.*$"
)
REPORT_HEADING = "**Factor Lab research prior / recall lead**"
REPORT_SECTION_RE = re.compile(
    r"(?ms)^\*\*Factor Lab (?:选股|research prior / recall lead)\*\*\n\n.*?(?=\n---\n\n\*\*|\n## |\Z)"
)
MERGE_FACTOR_SECTION_RE = re.compile(
    r"(?ms)^### Factor Lab\n.*?(?=^## |\Z)"
)
ROW_RE = re.compile(
    r"^\s*(\d+)\s+([A-Z0-9._-]+)\s+(.+?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)%\s*$"
)
UNAVAILABLE_RE = re.compile(r"(?mi)^\s*状态:\s*UNAVAILABLE\b.*$")


def extract_factor_lab_signal(structural_text: str) -> str | None:
    match = SIGNAL_HEADING_RE.search(structural_text.strip())
    if not match:
        return None
    return match.group(0).strip()


def _parse_factor_lab_signal(signal_block: str) -> dict[str, Any] | None:
    lines = [line.rstrip() for line in signal_block.splitlines()]
    if not lines:
        return None

    parsed: dict[str, Any] = {
        "status_line": "",
        "trade_date_line": "",
        "factor_name": "",
        "hold_days_line": "",
        "cleaning_line": "",
        "rows": [],
    }

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("状态:") and not parsed["status_line"]:
            parsed["status_line"] = stripped
        elif stripped.startswith("数据截止:") and not parsed["trade_date_line"]:
            parsed["trade_date_line"] = stripped
        elif (stripped.startswith("依据:") or stripped.startswith("因子:")) and not parsed["factor_name"]:
            parsed["factor_name"] = re.sub(r"^(依据|因子):", "", stripped, count=1).strip()
        elif (
            stripped.startswith("2. 持有")
            or stripped.startswith("4. 参考持有窗口")
        ) and not parsed["hold_days_line"]:
            parsed["hold_days_line"] = stripped
        elif stripped.startswith("数据清洗:") and not parsed["cleaning_line"]:
            parsed["cleaning_line"] = stripped

        row_match = ROW_RE.match(line)
        if row_match:
            rank, symbol, name, entry, stop, target, weight = row_match.groups()
            parsed["rows"].append(
                {
                    "rank": int(rank),
                    "symbol": symbol,
                    "name": name.strip(),
                    "entry": entry,
                    "stop": stop,
                    "target": target,
                    "weight": f"{weight}%",
                }
            )

    if not parsed["rows"]:
        return None
    return parsed


def render_factor_lab_report_section(signal_block: str) -> str:
    unavailable_match = UNAVAILABLE_RE.search(signal_block)
    if unavailable_match:
        return (
            f"{REPORT_HEADING}\n\n"
            f"{unavailable_match.group(0).strip()} "
            "本期不展示 Factor Lab research prior / recall lead 候选表，也不把其方向性结论写入正文。"
        ).strip()

    parsed = _parse_factor_lab_signal(signal_block)
    if parsed:
        summary_bits = [
            "research prior / recall lead，不是交易指令；不改变主系统结论；未通过主系统 gate 的票只能观察。"
        ]
        if parsed["status_line"]:
            summary_bits.append(parsed["status_line"])
        if parsed["trade_date_line"]:
            summary_bits.append(parsed["trade_date_line"])
        if parsed["factor_name"]:
            summary_bits.append(f"当前因子：`{parsed['factor_name']}`。")
        if parsed["hold_days_line"]:
            summary_bits.append(parsed["hold_days_line"])
        if parsed["cleaning_line"]:
            summary_bits.append(parsed["cleaning_line"])

        lines = [
            REPORT_HEADING,
            "",
            " ".join(summary_bits),
            "",
            "| 代码 | 名称 | 参考价 | 风控线 | 观察上沿 | 研究权重 | 备注 |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
        for row in parsed["rows"]:
            lines.append(
                f"| `{row['symbol']}` | {row['name']} | {row['entry']} | {row['stop']} | {row['target']} | {row['weight']} | 强度#{row['rank']} |"
            )
        return "\n".join(lines).strip()

    body = re.sub(
        r"(?ms)^## Factor Lab (?:Research Candidates|Independent Trading Signal)\s*\n?",
        "",
        signal_block.strip(),
        count=1,
    ).strip()
    body = body.split("Traceback (most recent call last):", 1)[0].strip()
    body = re.sub(r"(?m)^最终研报只需保留状态说明.*$", "", body).strip()
    body = re.sub(r"(?m)^每只股票附带.*$", "", body).strip()
    return f"{REPORT_HEADING}\n\n{body}".strip()


def _insert_factor_lab_section(report_text: str, replacement: str) -> str:
    for marker in ("\n---\n\n**接下来看什么**", "\n## Factor Lab 因子实验报告"):
        idx = report_text.find(marker)
        if idx != -1:
            return report_text[:idx].rstrip() + "\n\n---\n\n" + replacement + report_text[idx:]
    return report_text.rstrip() + "\n\n---\n\n" + replacement


def sync_factor_lab_signal_section(report_text: str, structural_text: str) -> str:
    signal_block = extract_factor_lab_signal(structural_text)
    if not signal_block:
        return report_text if report_text.endswith("\n") else report_text + "\n"

    replacement = render_factor_lab_report_section(signal_block)
    if REPORT_SECTION_RE.search(report_text):
        synced = REPORT_SECTION_RE.sub(replacement.rstrip() + "\n\n", report_text, count=1)
    elif MERGE_FACTOR_SECTION_RE.search(report_text):
        synced = MERGE_FACTOR_SECTION_RE.sub(replacement.rstrip() + "\n\n", report_text, count=1)
    else:
        synced = _insert_factor_lab_section(report_text, replacement)
    return synced.rstrip() + "\n"
