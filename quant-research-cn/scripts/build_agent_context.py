#!/usr/bin/env python3
"""Build compact A-share agent contexts from large payload markdown files."""
from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path


ITEM_RE = re.compile(r"(?ms)^#### .+?(?=^#### |\Z)")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _split_items(text: str) -> tuple[str, list[str]]:
    matches = list(ITEM_RE.finditer(text))
    if not matches:
        return text.strip(), []
    return text[:matches[0].start()].strip(), [m.group(0).strip() for m in matches]


def _symbol(section: str) -> str:
    match = re.search(r"^####\s+([0-9A-Z._-]+)", section, re.M)
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
    if "RANGE CORE" in upper:
        return "RANGE_CORE"
    if "TACTICAL CONTINUATION" in upper:
        return "TACTICAL_CONTINUATION"
    if "THEME ROTATION" in upper:
        return "THEME_ROTATION"
    if "RADAR" in upper:
        return "RADAR"
    return "OTHER"


def _compact_items(
    text: str,
    *,
    title: str,
    max_items: int,
) -> tuple[str, list[str], dict[str, str]]:
    header, sections = _split_items(text)
    if not sections:
        return text.strip(), [], {}

    chosen: list[str] = []
    used: set[str] = set()
    symbol_lanes: dict[str, str] = {}
    lane_caps = {"CORE": 4, "RANGE_CORE": 4, "TACTICAL_CONTINUATION": 6, "THEME_ROTATION": 6, "RADAR": 2}
    lane_order = ["CORE", "RANGE_CORE", "TACTICAL_CONTINUATION", "THEME_ROTATION", "RADAR", "OTHER"]
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
                symbol_lanes[symbol] = lane
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
            symbol = _symbol(section)
            chosen.append(section)
            used.add(symbol)
            symbol_lanes[symbol] = _lane(section)

    lane_counts = Counter(_lane(section) for section in sections)
    counts = Counter(_bucket(section) for section in sections)
    omitted = len(sections) - len(chosen)
    summary = "\n".join([
        f"## {title}摘要",
        f"- 总条目: {len(sections)}",
        f"- 报告层级: CORE {lane_counts.get('CORE', 0)} | RANGE_CORE {lane_counts.get('RANGE_CORE', 0)} | TACTICAL_CONTINUATION {lane_counts.get('TACTICAL_CONTINUATION', 0)} | THEME_ROTATION {lane_counts.get('THEME_ROTATION', 0)} | RADAR {lane_counts.get('RADAR', 0)} | OTHER {lane_counts.get('OTHER', 0)}",
        f"- HIGH: {counts.get('HIGH', 0)} | MODERATE: {counts.get('MODERATE', 0)} | WATCH: {counts.get('WATCH', 0)} | LOW: {counts.get('LOW', 0)} | no-signal: {counts.get('NO_SIGNAL', 0)}",
        f"- 保留: {len(chosen)} 条，优先级为 CORE -> RANGE_CORE -> TACTICAL_CONTINUATION -> THEME_ROTATION -> RADAR；其余 {omitted} 条只保留在统计摘要中",
        f"- 保留代码: {', '.join(_symbol(section) for section in chosen)}",
    ])

    compact = "\n\n".join(part for part in [header, summary, *chosen] if part)
    return compact.strip(), [_symbol(section) for section in chosen], symbol_lanes


def _payload_path(reports_dir: Path, date: str, section: str, slot: str | None) -> Path:
    candidates = []
    if slot:
        candidates.append(reports_dir / f"{date}_payload_{section}_{slot}.md")
    candidates.append(reports_dir / f"{date}_payload_{section}.md")
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def build_contexts(reports_dir: Path, date: str, out_dir: Path, slot: str | None = None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    macro_text = _read(_payload_path(reports_dir, date, "macro", slot))
    structural_text = _read(_payload_path(reports_dir, date, "structural", slot))
    events_text = _read(_payload_path(reports_dir, date, "events", slot))

    (out_dir / "macro.md").write_text(macro_text + "\n", encoding="utf-8")

    structural_compact, selected_symbols, symbol_lanes = _compact_items(
        structural_text,
        title="结构信号",
        max_items=18,
    )
    (out_dir / "structural.md").write_text(structural_compact + "\n", encoding="utf-8")

    events_header, event_sections = _split_items(events_text)
    selected_events = [section for section in event_sections if _symbol(section) in set(selected_symbols)]
    if len(selected_events) < min(10, len(event_sections)):
        for section in event_sections:
            if len(selected_events) >= 12:
                break
            if section in selected_events:
                continue
            if _bucket(section) == "NO_SIGNAL":
                continue
            selected_events.append(section)
    event_counts = Counter(_bucket(section) for section in event_sections)
    omitted_events = len(event_sections) - len(selected_events)
    core_syms = [sym for sym, lane in symbol_lanes.items() if lane == "CORE"]
    range_core_syms = [sym for sym, lane in symbol_lanes.items() if lane == "RANGE_CORE"]
    tactical_syms = [sym for sym, lane in symbol_lanes.items() if lane == "TACTICAL_CONTINUATION"]
    theme_syms = [sym for sym, lane in symbol_lanes.items() if lane == "THEME_ROTATION"]
    radar_syms = [sym for sym, lane in symbol_lanes.items() if lane == "RADAR"]
    event_summary = "\n".join([
        "## 事件催化摘要",
        f"- 总条目: {len(event_sections)}",
        f"- 结构优先级: CORE {', '.join(core_syms) if core_syms else '(无)'}",
        f"- 结构优先级: RANGE_CORE {', '.join(range_core_syms) if range_core_syms else '(无)'}",
        f"- 结构优先级: TACTICAL_CONTINUATION {', '.join(tactical_syms) if tactical_syms else '(无)'}",
        f"- 结构优先级: THEME_ROTATION {', '.join(theme_syms) if theme_syms else '(无)'}",
        f"- 结构优先级: RADAR {', '.join(radar_syms) if radar_syms else '(无)'}",
        f"- HIGH: {event_counts.get('HIGH', 0)} | MODERATE: {event_counts.get('MODERATE', 0)} | WATCH: {event_counts.get('WATCH', 0)} | LOW: {event_counts.get('LOW', 0)} | no-signal: {event_counts.get('NO_SIGNAL', 0)}",
        f"- 保留: {len(selected_events)} 条；其余 {omitted_events} 条只保留在统计摘要中",
        f"- 保留代码: {', '.join(_symbol(section) for section in selected_events) if selected_events else '(事件payload无逐标的分段，保留全文)'}",
    ])
    events_compact = "\n\n".join(part for part in [events_header, event_summary, *selected_events] if part)
    (out_dir / "events.md").write_text(events_compact + "\n", encoding="utf-8")

    merge_crosscheck = "\n\n---\n\n".join([macro_text.strip(), structural_compact.strip(), events_compact.strip()])
    (out_dir / "merge_crosscheck.md").write_text(merge_crosscheck + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--slot", choices=["morning", "evening"], default=None)
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    build_contexts(Path(args.reports_dir), args.date, Path(args.out_dir), args.slot)


if __name__ == "__main__":
    main()
