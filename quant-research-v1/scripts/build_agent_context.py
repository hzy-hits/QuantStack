#!/usr/bin/env python3
"""Build compact agent contexts from large payload markdown files."""
from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path


ITEM_RE = re.compile(r"(?ms)^### \d+\. .*?(?=^### \d+\. |^## |\Z)")
SPECIAL_SECTION_HEADINGS = (
    "## Factor Lab Research Candidates",
    "## Factor Lab Independent Trading Signal",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _take_until_heading(text: str, stop_headings: list[str]) -> str:
    end = len(text)
    for heading in stop_headings:
        idx = text.find(heading)
        if idx != -1:
            end = min(end, idx)
    return text[:end].rstrip()


def _split_items(text: str) -> tuple[str, list[str], str]:
    matches = list(ITEM_RE.finditer(text))
    if not matches:
        return text.strip(), [], ""
    header = text[:matches[0].start()].strip()
    sections = [m.group(0).strip() for m in matches]
    trailing = text[matches[-1].end():].strip()
    return header, sections, trailing


def _extract_preserved_sections(text: str) -> list[str]:
    preserved: list[str] = []
    for heading in SPECIAL_SECTION_HEADINGS:
        idx = text.find(heading)
        if idx != -1:
            preserved.append(text[idx:].strip())
    return preserved


def _symbol(section: str) -> str:
    match = re.search(r"^### \d+\.\s+([A-Z0-9._-]+)", section, re.M)
    return match.group(1) if match else "UNKNOWN"


def _bucket(section: str) -> str:
    if "**HIGH**" in section:
        return "HIGH"
    if "**MODERATE**" in section:
        return "MODERATE"
    if "**WATCH**" in section or " WATCH " in section:
        return "WATCH"
    if "**LOW**" in section:
        return "LOW"
    if "no signal" in section.lower():
        return "NO_SIGNAL"
    return "OTHER"


def _lane(section: str) -> str:
    upper = section.upper()
    if "CORE BOOK" in upper:
        return "CORE"
    if "TACTICAL CONTINUATION" in upper:
        return "TACTICAL_CONTINUATION"
    if "TACTICAL EVENT TAPE" in upper:
        return "EVENT_TAPE"
    if "APPENDIX / RADAR" in upper:
        return "APPENDIX"
    return "OTHER"


def _compact_items(text: str, *, max_items: int, title: str) -> tuple[str, list[str]]:
    header, sections, trailing = _split_items(text)
    if not sections:
        return text.strip(), []

    chosen: list[str] = []
    used: set[str] = set()
    lane_caps = {"CORE": 7, "TACTICAL_CONTINUATION": 2, "EVENT_TAPE": 2, "APPENDIX": 1}
    lane_order = ["CORE", "TACTICAL_CONTINUATION", "EVENT_TAPE", "APPENDIX", "OTHER"]
    bucket_order = ["HIGH", "MODERATE", "WATCH", "LOW", "OTHER", "NO_SIGNAL"]
    selected_lane_counts: Counter[str] = Counter()

    for lane in lane_order:
        for bucket in bucket_order:
            for section in sections:
                if len(chosen) >= max_items:
                    break
                symbol = _symbol(section)
                if symbol in used:
                    continue
                if _lane(section) != lane or _bucket(section) != bucket:
                    continue
                lane_cap = lane_caps.get(lane, max_items)
                if selected_lane_counts[lane] >= lane_cap:
                    continue
                chosen.append(section)
                used.add(symbol)
                selected_lane_counts[lane] += 1
            if len(chosen) >= max_items:
                break
        if len(chosen) >= max_items:
            break

    if len(chosen) < max_items:
        lane_priority = {lane: idx for idx, lane in enumerate(lane_order)}
        bucket_priority = {bucket: idx for idx, bucket in enumerate(bucket_order)}
        remaining = sorted(
            [section for section in sections if _symbol(section) not in used],
            key=lambda section: (
                lane_priority.get(_lane(section), len(lane_priority)),
                bucket_priority.get(_bucket(section), len(bucket_priority)),
                _symbol(section),
            ),
        )
        for section in remaining[: max_items - len(chosen)]:
            chosen.append(section)
            used.add(_symbol(section))

    lane_counts = Counter(_lane(section) for section in sections)
    counts = Counter(_bucket(section) for section in sections)
    omitted = len(sections) - len(chosen)
    summary_lines = [
        f"## {title}摘要",
        f"- 总条目: {len(sections)}",
        f"- Report lanes: CORE {lane_counts.get('CORE', 0)} | TACTICAL_CONTINUATION {lane_counts.get('TACTICAL_CONTINUATION', 0)} | EVENT_TAPE {lane_counts.get('EVENT_TAPE', 0)} | APPENDIX {lane_counts.get('APPENDIX', 0)} | OTHER {lane_counts.get('OTHER', 0)}",
        f"- HIGH: {counts.get('HIGH', 0)} | MODERATE: {counts.get('MODERATE', 0)} | WATCH: {counts.get('WATCH', 0)} | LOW: {counts.get('LOW', 0)} | no-signal: {counts.get('NO_SIGNAL', 0)}",
        f"- 本上下文保留: {len(chosen)} 条，按 CORE -> TACTICAL_CONTINUATION -> EVENT_TAPE -> APPENDIX 优先；其余 {omitted} 条仅计入摘要，不再逐条展开",
        f"- 保留代码: {', '.join(_symbol(section) for section in chosen)}",
    ]

    preserved_sections = _extract_preserved_sections(trailing)
    compact = "\n\n".join(
        part for part in [header, "\n".join(summary_lines), *chosen, *preserved_sections] if part
    )
    return compact.strip(), [_symbol(section) for section in chosen]


def build_contexts(reports_dir: Path, date: str, session: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    macro_path = reports_dir / f"{date}_payload_macro_{session}.md"
    structural_path = reports_dir / f"{date}_payload_structural_{session}.md"
    news_path = reports_dir / f"{date}_payload_news_{session}.md"
    if not macro_path.exists():
        macro_path = reports_dir / f"{date}_payload_macro.md"
    if not structural_path.exists():
        structural_path = reports_dir / f"{date}_payload_structural.md"
    if not news_path.exists():
        news_path = reports_dir / f"{date}_payload_news.md"

    macro_text = _read(macro_path)
    structural_text = _read(structural_path)
    news_text = _read(news_path)

    macro_compact = _take_until_heading(
        macro_text,
        stop_headings=["## Universe Summary", "## Data Coverage", "## Charts"],
    )
    (out_dir / "macro.md").write_text(macro_compact + "\n", encoding="utf-8")

    structural_compact, selected_symbols = _compact_items(
        structural_text,
        max_items=12,
        title="结构信号",
    )
    (out_dir / "structural.md").write_text(structural_compact + "\n", encoding="utf-8")

    news_header, news_sections, _ = _split_items(news_text)
    selected_news = [section for section in news_sections if _symbol(section) in set(selected_symbols)]
    if len(selected_news) < min(10, len(news_sections)):
        for section in news_sections:
            if len(selected_news) >= 12:
                break
            if section in selected_news:
                continue
            if _bucket(section) == "NO_SIGNAL":
                continue
            selected_news.append(section)
    news_counts = Counter(_bucket(section) for section in news_sections)
    news_lane_counts = Counter(_lane(section) for section in news_sections)
    omitted_news = len(news_sections) - len(selected_news)
    news_summary = "\n".join([
        "## 事件新闻摘要",
        f"- 总条目: {len(news_sections)}",
        f"- Report lanes: CORE {news_lane_counts.get('CORE', 0)} | TACTICAL_CONTINUATION {news_lane_counts.get('TACTICAL_CONTINUATION', 0)} | EVENT_TAPE {news_lane_counts.get('EVENT_TAPE', 0)} | APPENDIX {news_lane_counts.get('APPENDIX', 0)} | OTHER {news_lane_counts.get('OTHER', 0)}",
        f"- HIGH: {news_counts.get('HIGH', 0)} | MODERATE: {news_counts.get('MODERATE', 0)} | WATCH: {news_counts.get('WATCH', 0)} | LOW: {news_counts.get('LOW', 0)} | no-signal: {news_counts.get('NO_SIGNAL', 0)}",
        f"- 本上下文保留: {len(selected_news)} 条；其余 {omitted_news} 条仅保留在原始payload中",
        f"- 保留代码: {', '.join(_symbol(section) for section in selected_news)}",
    ])
    news_compact = "\n\n".join(part for part in [news_header, news_summary, *selected_news] if part)
    (out_dir / "news.md").write_text(news_compact + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--session", choices=["post", "pre"], default="post")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    build_contexts(Path(args.reports_dir), args.date, args.session, Path(args.out_dir))


if __name__ == "__main__":
    main()
