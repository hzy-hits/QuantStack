from __future__ import annotations

import re


SIGNAL_HEADING = "## Factor Lab Independent Trading Signal"
REPORT_HEADING = "**Factor Lab 选股**"
REPORT_SECTION_RE = re.compile(
    r"(?ms)^\*\*Factor Lab 选股\*\*\n\n.*?(?=\n---\n\n\*\*|\n## |\Z)"
)


def extract_factor_lab_signal(structural_text: str) -> str | None:
    match = re.search(r"(?ms)^## Factor Lab Independent Trading Signal\s*\n.*$", structural_text.strip())
    if not match:
        return None
    return match.group(0).strip()


def render_factor_lab_report_section(signal_block: str) -> str:
    body = re.sub(
        r"(?ms)^## Factor Lab Independent Trading Signal\s*\n?",
        "",
        signal_block.strip(),
        count=1,
    ).strip()
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
        synced = REPORT_SECTION_RE.sub(replacement, report_text, count=1)
    else:
        synced = _insert_factor_lab_section(report_text, replacement)
    return synced.rstrip() + "\n"
