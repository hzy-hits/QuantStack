"""Render chart section with image embeds."""
from __future__ import annotations

from pathlib import Path


# Map chart filenames to descriptive titles
_CHART_TITLES = {
    "sector_performance.png": "Sector Performance",
    "notable_items.png": "Top Notable Items by Score",
    "cross_asset.png": "Cross-Asset Dashboard",
    "dyp_candidates.png": "Dividend Yield Dip Candidates",
    "index_trends.png": "Major Index Trends (60D)",
    "vix_trend.png": "VIX \u2014 Market Fear Gauge",
    "top_movers.png": "Top Movers \u2014 Winners & Losers",
}


def render_charts(chart_paths: list | None, output_path: Path) -> list[str]:
    """Return lines for the charts section, or empty if no charts."""
    if not chart_paths:
        return []

    lines: list[str] = [
        "## Charts",
        "",
    ]
    for cp in chart_paths:
        p = Path(cp)
        title = _CHART_TITLES.get(p.name, p.stem.replace("_", " ").title())
        # Use relative path from reports/ directory
        rel_path = p
        try:
            rel_path = p.relative_to(output_path.parent)
        except ValueError:
            pass
        lines.append(f"### {title}")
        lines.append(f"![{title}]({rel_path})")
        lines.append("")

    lines += ["---", ""]
    return lines
