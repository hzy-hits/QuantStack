"""
Render the computed payload into a structured raw Markdown file.

This file is the program's final output. It goes to whatever agent the user
chooses — Claude Code, Codex, OpenClaw, GPT-4, or a human directly.

The agent's job: read this file, write the narrative report.
The program's job: compute everything, present it clearly.

Design rule: every number here was computed deterministically upstream.
The agent adds prose. The agent adds no new numbers.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ._render_charts import render_charts
from ._render_context import render_header_and_context
from ._render_extremes import render_options_extremes
from ._render_fmt import _plural
from ._render_insights import (
    render_item_contradictions,
    render_item_news_quality,
    render_item_risk_params,
    render_portfolio_risk,
    render_report_postmortem,
    render_scorecard,
    render_shared_catalysts,
)
from ._render_item_data import render_item_data
from ._render_item_events import render_item_events
from ._render_item_header import render_item_header
from ._render_screens import render_coverage, render_dividend_screen, render_universe_summary


def render_payload_md(bundle: dict, output_path: Path, chart_paths: list | None = None) -> None:
    """
    Write the full computed payload as structured Markdown.
    This is the file the agent reads.
    """
    lines: list[str] = []

    lines += render_header_and_context(bundle)
    lines += render_report_postmortem(bundle)
    lines += render_scorecard(bundle)
    lines += render_portfolio_risk(bundle)
    lines += render_shared_catalysts(bundle)
    lines += render_options_extremes(bundle)
    lines += _render_notable_items(bundle)
    lines += render_dividend_screen(bundle)
    lines += render_universe_summary(bundle)
    lines += render_coverage(bundle)
    lines += render_charts(chart_paths, output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def _render_notable_items(bundle: dict) -> list[str]:
    """Render the full notable-items section with report-lane grouping."""
    all_items = bundle.get("notable_items", [])
    headline_mode = str((bundle.get("headline_gate") or {}).get("mode") or "unknown").lower()

    lines: list[str] = [
        "## Notable Items",
        "",
        f"Top {len(all_items)} from the full-universe scan.",
        "",
        "*Items are split into Core Book, Tactical Continuation, Tactical Event Tape, and Appendix / Radar so anomaly detection does not overwhelm the main report.*",
        "*Signal direction uses weighted multi-source scoring (options + momentum + events), while report lane reflects tradability and confirmation quality.*",
        "",
    ]
    if headline_mode != "trend":
        lines += [
            f"**Execution guard:** Headline Gate is `{headline_mode.upper()}`. Do not turn any lane below into a buy list.",
            "For final reports, use observation / pullback-review / invalidation language only; do not write Entry/Stop/Target tables, 'today only do', or chase instructions.",
            "",
        ]

    bucket_specs = [
        (
            "core",
            "Core Book",
            "Primary report lane: liquid large-cap equities, ETFs, or watchlist names with enough confirmation to matter for the main narrative.",
        ),
        (
            "tactical_continuation",
            "Tactical Continuation",
            "Continuation setups that still have edge, but only as smaller tactical positions with hard stops and tighter execution discipline.",
        ),
        (
            "event_tape",
            "Tactical Event Tape",
            "High-volatility event names and anomaly bursts. Tactical only; these can be smaller, noisier, or less liquid than the core book.",
        ),
        (
            "appendix",
            "Appendix / Radar",
            "Residual anomaly scan and lower-priority names. Use for market color and follow-up work, not as the main report thesis.",
        ),
    ]

    item_idx = 1
    for bucket, title, description in bucket_specs:
        bucket_items = _sort_bucket_items(
            [x for x in all_items if x.get("report_bucket") == bucket],
            bucket=bucket,
        )
        if not bucket_items:
            continue

        high = len([x for x in bucket_items if x.get("signal", {}).get("confidence") == "HIGH"])
        mod = len([x for x in bucket_items if x.get("signal", {}).get("confidence") == "MODERATE"])
        low = len([x for x in bucket_items if x.get("signal", {}).get("confidence") in ("LOW", "NO_SIGNAL", None)])

        lines += [
            "---",
            "",
            f"### {title}",
            "",
            f"{_plural(len(bucket_items), 'item')}. Signal confidence: {high} HIGH, {mod} MODERATE, {low} LOW/NONE.",
            f"*{description}*",
            "",
        ]

        for item in bucket_items:
            item = {**item, "_headline_mode": headline_mode}
            compact = bucket == "appendix" or item.get("signal", {}).get("confidence") in ("LOW", "NO_SIGNAL", None)
            lines += render_item_header(item_idx, item, compact=compact)
            if not compact:
                if headline_mode == "trend":
                    lines += render_item_risk_params(item)
                else:
                    lines += _render_nontrend_execution_guard(item, headline_mode)
                lines += render_item_contradictions(item)
            lines += render_item_data(item, compact=compact)
            if not compact:
                lines += render_item_news_quality(item)
            lines += render_item_events(item, compact=compact)
            item_idx += 1

    return lines


def _sort_bucket_items(items: list[dict], *, bucket: str) -> list[dict]:
    """Sort items within a report lane by confidence first, then lane score."""
    confidence_rank = {"HIGH": 0, "MODERATE": 1, "LOW": 2, "NO_SIGNAL": 3, None: 3}

    def _primary_score(item: dict) -> float:
        if bucket == "core":
            return float(
                item.get("selection_rank_score")
                or item.get("report_score")
                or item.get("score", 0.0)
                or 0.0
            )
        return float(
            item.get("selection_rank_score")
            or item.get("report_score")
            or item.get("score", 0.0)
            or 0.0
        )

    return sorted(
        items,
        key=lambda item: (
            confidence_rank.get((item.get("signal") or {}).get("confidence"), 3),
            -_primary_score(item),
            -float(item.get("report_score", item.get("score", 0.0)) or 0.0),
            item.get("symbol", ""),
        ),
    )


def _render_nontrend_execution_guard(item: dict, headline_mode: str) -> list[str]:
    """Render execution discipline without order-shaped risk parameters."""
    gate = item.get("execution_gate") or {}
    risk = item.get("risk_params") or {}
    pullback = gate.get("pullback_price") or risk.get("entry")
    action = gate.get("action") or risk.get("execution_mode") or "unknown"
    lines = [
        "**Execution guard (non-trend gate):**",
        "",
        f"- Headline Gate `{headline_mode.upper()}`: this item is a review candidate, not an order surface.",
        f"- Execution state: {action}; pullback/review reference: {_fmt_price(pullback)}.",
        "- Final report must not print Entry/Stop/Target, 'today only do', or max-chase instructions for this item.",
        "",
    ]
    return lines


def _fmt_price(value: Any) -> str:
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"
