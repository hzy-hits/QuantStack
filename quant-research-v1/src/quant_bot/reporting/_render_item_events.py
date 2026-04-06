"""Render per-item events (earnings, 8-K filings, index changes) and news."""
from __future__ import annotations

from typing import Any

from ._render_fmt import _fmt_pct, _fmt_val


def render_item_events(item: dict, compact: bool = False) -> list[str]:
    """Return lines for the events + news portion of a single notable item."""
    lines: list[str] = []
    if not compact:
        lines += _earnings_events(item)
    lines += _news(item)
    lines += ["---", ""]
    return lines


# -- private helpers --------------------------------------------------------


def _earnings_events(item: dict) -> list[str]:
    events = item.get("events", [])
    if not events:
        return []
    lines = ["**Earnings / Events:**", ""]
    for ev in events:
        if ev.get("type") == "earnings":
            d = ev.get("days_offset", 0)
            when = f"in {d}D" if d >= 0 else f"{abs(d)}D ago"
            lines.append(f"- Earnings {when}")
            if ev.get("estimate_eps") is not None:
                lines.append(f"  - EPS estimate: {_fmt_val(ev.get('estimate_eps'), 2)}")
            if ev.get("actual_eps") is not None:
                lines.append(f"  - EPS actual: {_fmt_val(ev.get('actual_eps'), 2)}")
            if ev.get("surprise_pct") is not None:
                lines.append(f"  - Surprise: {_fmt_pct(ev.get('surprise_pct'), 1)}")
        elif ev.get("type") == "8-K_filing":
            for f in ev.get("filings", []):
                lines.append(f"- SEC 8-K filed {f.get('filed', '?')}: {f.get('description', '?')}")
                for it in f.get("items", []):
                    lines.append(f"  - {it}")
        elif ev.get("type", "").startswith("index_"):
            action = "ADDED TO" if "add" in ev["type"] else "REMOVED FROM"
            lines.append(
                f"- {action} {ev.get('index', '?')} on {ev.get('date', '?')} "
                f"({ev.get('days_ago', '?')}D ago)"
            )
    lines.append("")
    return lines


def _news(item: dict) -> list[str]:
    news = item.get("news", [])
    if not news:
        return []
    lines = ["**Recent News:**", ""]
    for n in news:
        ts = str(n.get("published_at", ""))[:10]
        lines.append(f"- [{ts}] **{n.get('source', '?')}** \u2014 {n.get('headline', '')}")
        if n.get("summary"):
            lines.append(f"  > {n['summary'][:200]}")
    lines.append("")
    return lines
