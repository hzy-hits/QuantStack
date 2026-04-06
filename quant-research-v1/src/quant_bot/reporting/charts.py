"""
Generate daily report charts as PNG files.

Charts:
  1. Sector Performance — 11 sector ETFs, 1D return horizontal bar
  2. Top Notable Items — top items by notability score, colored by confidence
  3. Cross-Asset Heatmap — indices, commodities, bonds, volatility returns matrix
  4. DYP Screen Candidates — stacked bar of DYP/TS/SS score breakdown
  5. Index Price Trends — SPY/QQQ/IWM 60-day normalized price lines
  6. VIX Trend — VIX 60-day line with 20-day SMA
  7. Top Movers — biggest 1D winners and losers from notable items
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import numpy as np

# ── Style ────────────────────────────────────────────────────────────────────
DARK_BG = "#1a1a2e"
PANEL_BG = "#16213e"
TEXT_COLOR = "#e0e0e0"
GRID_COLOR = "#2a2a4a"
RED = "#ff4757"
GREEN = "#2ed573"
BLUE = "#3742fa"
GOLD = "#ffa502"
CYAN = "#18dcff"
MUTED_RED = "#c44569"
MUTED_GREEN = "#6ab04c"

SECTOR_LABELS = {
    "XLK": "Tech", "XLF": "Financials", "XLE": "Energy", "XLV": "Healthcare",
    "XLI": "Industrials", "XLU": "Utilities", "XLRE": "Real Estate",
    "XLY": "Cons. Disc.", "XLP": "Cons. Staples", "XLB": "Materials", "XLC": "Comm. Svc.",
}

DIP_LABEL_COLORS = {
    "STRONG_DIP": "#2ed573",
    "MODERATE_DIP": "#ffa502",
    "FAIR": "#7f8fa6",
    "BELOW_THRESHOLD": "#535c68",
}


def _apply_dark_style():
    plt.rcParams.update({
        "figure.facecolor": DARK_BG,
        "axes.facecolor": PANEL_BG,
        "axes.edgecolor": GRID_COLOR,
        "axes.labelcolor": TEXT_COLOR,
        "text.color": TEXT_COLOR,
        "xtick.color": TEXT_COLOR,
        "ytick.color": TEXT_COLOR,
        "grid.color": GRID_COLOR,
        "grid.alpha": 0.3,
        "font.size": 11,
        "font.family": "sans-serif",
    })


def _save(fig, path: Path, dpi: int = 150):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(path), dpi=dpi, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


# ── Chart 1: Sector Performance ──────────────────────────────────────────────

def chart_sector_performance(bundle: dict, output_dir: Path) -> Path | None:
    """Horizontal bar chart of sector ETF 1D returns."""
    ctx = bundle.get("market_context", {})
    sectors = ctx.get("sectors", {})
    if not sectors:
        return None

    _apply_dark_style()

    # Sort by return
    items = []
    for sym, data in sectors.items():
        ret = data.get("ret_1d_pct")
        if ret is not None:
            label = SECTOR_LABELS.get(sym, sym)
            items.append((label, ret))

    if not items:
        return None

    items.sort(key=lambda x: x[1])
    labels, values = zip(*items)

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = [GREEN if v >= 0 else RED for v in values]
    bars = ax.barh(labels, values, color=colors, edgecolor="none", height=0.7)

    # Value labels
    for bar, val in zip(bars, values):
        x = bar.get_width()
        offset = 0.05 if x >= 0 else -0.05
        ha = "left" if x >= 0 else "right"
        ax.text(x + offset, bar.get_y() + bar.get_height() / 2,
                f"{val:+.2f}%", va="center", ha=ha, fontsize=9, color=TEXT_COLOR)

    ax.set_xlabel("1-Day Return (%)")
    ax.set_title("Sector Performance", fontsize=14, fontweight="bold", pad=12)
    ax.axvline(x=0, color=TEXT_COLOR, linewidth=0.5, alpha=0.3)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # SPY reference line
    spy_ret = (ctx.get("major_indices", {}).get("SPY") or {}).get("ret_1d_pct")
    if spy_ret is not None:
        ax.axvline(x=spy_ret, color=CYAN, linewidth=1, linestyle="--", alpha=0.6)
        ax.text(spy_ret, len(items) - 0.5, f"  SPY {spy_ret:+.2f}%",
                fontsize=8, color=CYAN, va="bottom")

    path = output_dir / "sector_performance.png"
    _save(fig, path)
    return path


# ── Chart 2: Top Notable Items ───────────────────────────────────────────────

CONFIDENCE_COLORS = {
    "HIGH": "#ff4757",
    "MODERATE": "#ffa502",
    "LOW": "#7f8fa6",
    "NO_SIGNAL": "#535c68",
    None: "#535c68",
}

def chart_notable_items(bundle: dict, output_dir: Path, max_items: int = 20) -> Path | None:
    """Horizontal bar chart of top notable items, colored by signal confidence."""
    items = bundle.get("notable_items", [])
    if not items:
        return None

    _apply_dark_style()

    # Take top N by score
    top = sorted(items, key=lambda x: x.get("score", 0), reverse=True)[:max_items]
    top.reverse()  # bottom-to-top for horizontal bars

    symbols = [it["symbol"] for it in top]
    scores = [it.get("score", 0) for it in top]
    confidences = [it.get("signal", {}).get("confidence") for it in top]
    directions = [it.get("signal", {}).get("direction", "neutral") for it in top]
    colors = [CONFIDENCE_COLORS.get(c, "#535c68") for c in confidences]

    fig, ax = plt.subplots(figsize=(9, max(5, len(top) * 0.35)))
    bars = ax.barh(symbols, scores, color=colors, edgecolor="none", height=0.7)

    # Direction arrows
    for bar, direction, score in zip(bars, directions, scores):
        arrow = "▲" if direction == "bullish" else "▼" if direction == "bearish" else "◆"
        color = GREEN if direction == "bullish" else RED if direction == "bearish" else TEXT_COLOR
        ax.text(score + 0.005, bar.get_y() + bar.get_height() / 2,
                f" {arrow} {score:.3f}", va="center", fontsize=8, color=color)

    ax.set_xlabel("Notability Score")
    ax.set_title("Top Notable Items", fontsize=14, fontweight="bold", pad=12)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=CONFIDENCE_COLORS["HIGH"], label="HIGH"),
        Patch(facecolor=CONFIDENCE_COLORS["MODERATE"], label="MODERATE"),
        Patch(facecolor=CONFIDENCE_COLORS["LOW"], label="LOW / NONE"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=8,
              facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)

    path = output_dir / "notable_items.png"
    _save(fig, path)
    return path


# ── Chart 3: Cross-Asset Returns Heatmap ─────────────────────────────────────

def chart_cross_asset_heatmap(bundle: dict, output_dir: Path) -> Path | None:
    """Heatmap of 1D returns across asset classes."""
    ctx = bundle.get("market_context", {})
    universe_summary = bundle.get("universe_summary", {})

    _apply_dark_style()

    # Collect all cross-asset data
    rows: list[tuple[str, str, float]] = []  # (category, symbol, ret_1d)

    # Indices
    for sym, data in ctx.get("major_indices", {}).items():
        ret = data.get("ret_1d_pct")
        if ret is not None:
            rows.append(("Indices", sym, ret))

    # Sectors (top 3 + bottom 3 only for compactness)
    sector_items = []
    for sym, data in ctx.get("sectors", {}).items():
        ret = data.get("ret_1d_pct")
        if ret is not None:
            sector_items.append((sym, ret))
    sector_items.sort(key=lambda x: x[1])
    for sym, ret in sector_items[:3] + sector_items[-3:]:
        label = SECTOR_LABELS.get(sym, sym)
        rows.append(("Sectors", f"{label} ({sym})", ret))

    # Commodities
    for sym, data in ctx.get("commodities", {}).items():
        ret = data.get("ret_1d_pct")
        if ret is not None:
            rows.append(("Commodities", sym, ret))

    # Rates/Credit
    for sym, data in ctx.get("rates_credit", {}).items():
        ret = data.get("ret_1d_pct")
        if ret is not None:
            rows.append(("Rates/Credit", sym, ret))

    if not rows:
        return None

    # Build grid
    categories = []
    labels = []
    values = []
    seen_cats = []
    for cat, sym, ret in rows:
        categories.append(cat)
        labels.append(sym)
        values.append(ret)
        if cat not in seen_cats:
            seen_cats.append(cat)

    fig, ax = plt.subplots(figsize=(6, max(5, len(rows) * 0.32)))

    # Color mapping: diverging red-green
    max_abs = max(abs(v) for v in values) if values else 1
    norm = mcolors.TwoSlopeNorm(vmin=-max_abs, vcenter=0, vmax=max_abs)
    cmap = mcolors.LinearSegmentedColormap.from_list("rg", [RED, PANEL_BG, GREEN])

    y_pos = list(range(len(rows)))
    bar_colors = [cmap(norm(v)) for v in values]
    bars = ax.barh(y_pos, values, color=bar_colors, edgecolor="none", height=0.7)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=9)

    # Value labels
    for i, (bar, val) in enumerate(zip(bars, values)):
        x = bar.get_width()
        offset = 0.03 * max_abs if x >= 0 else -0.03 * max_abs
        ha = "left" if x >= 0 else "right"
        ax.text(x + offset, i, f"{val:+.2f}%", va="center", ha=ha, fontsize=8, color=TEXT_COLOR)

    # Category separators
    prev_cat = None
    for i, cat in enumerate(categories):
        if cat != prev_cat and prev_cat is not None:
            ax.axhline(y=i - 0.5, color=GRID_COLOR, linewidth=0.8, linestyle="-")
        prev_cat = cat

    # Category labels on right
    cat_positions = {}
    for i, cat in enumerate(categories):
        if cat not in cat_positions:
            cat_positions[cat] = []
        cat_positions[cat].append(i)
    for cat, positions in cat_positions.items():
        mid = (positions[0] + positions[-1]) / 2
        ax.text(max_abs * 1.3, mid, cat, va="center", ha="left",
                fontsize=9, fontweight="bold", color=GOLD, alpha=0.8)

    ax.axvline(x=0, color=TEXT_COLOR, linewidth=0.5, alpha=0.3)
    ax.set_xlabel("1-Day Return (%)")
    ax.set_title("Cross-Asset Dashboard", fontsize=14, fontweight="bold", pad=12)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    path = output_dir / "cross_asset.png"
    _save(fig, path, dpi=150)
    return path


# ── Chart 4: DYP Screen Candidates ──────────────────────────────────────────

def chart_dyp_candidates(bundle: dict, output_dir: Path, max_items: int = 15) -> Path | None:
    """Stacked horizontal bar of DYP/TS/SS score breakdown for dip candidates."""
    dip_screen = bundle.get("dividend_dip_screen", {})
    items = dip_screen.get("items", [])
    if not items:
        return None

    _apply_dark_style()

    top = items[:max_items]
    top.reverse()  # bottom-to-top

    symbols = [it["symbol"] for it in top]
    dyp_vals = [it.get("dyp", 0) * 0.35 for it in top]  # weighted
    ts_vals = [it.get("technical_score", 0) * 0.35 for it in top]
    ss_vals = [it.get("safety_score", 0) * 0.30 for it in top]
    composites = [it.get("composite_score", 0) for it in top]
    labels_list = [it.get("label", "—") for it in top]

    fig, ax = plt.subplots(figsize=(9, max(4, len(top) * 0.4)))

    y = np.arange(len(symbols))
    h = 0.6

    bars_dyp = ax.barh(y, dyp_vals, height=h, color="#3742fa", label="DYP (35%)")
    bars_ts = ax.barh(y, ts_vals, height=h, left=dyp_vals, color="#ffa502", label="TS (35%)")
    bars_ss = ax.barh(y, ss_vals, height=h,
                      left=[d + t for d, t in zip(dyp_vals, ts_vals)],
                      color="#2ed573", label="SS (30%)")

    ax.set_yticks(y)
    ax.set_yticklabels(symbols, fontsize=10)

    # Composite score + label
    for i, (comp, label) in enumerate(zip(composites, labels_list)):
        color = DIP_LABEL_COLORS.get(label, TEXT_COLOR)
        ax.text(comp + 0.5, i, f" {comp:.0f} {label}",
                va="center", fontsize=8, color=color, fontweight="bold")

    ax.set_xlabel("Composite Score (weighted)")
    ax.set_title("Dividend Yield Dip Candidates", fontsize=14, fontweight="bold", pad=12)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="lower right", fontsize=8,
              facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)

    # Yield annotation on left
    for i, it in enumerate(top):
        yld = it.get("current_yield_pct", 0)
        ax.text(-1, i, f"{yld:.1f}%", va="center", ha="right",
                fontsize=8, color=CYAN, alpha=0.8)
    ax.text(-1, len(top), "Yield", va="center", ha="right",
            fontsize=8, color=CYAN, fontweight="bold", alpha=0.8)

    path = output_dir / "dyp_candidates.png"
    _save(fig, path)
    return path


# ── Chart 5: Index Price Trends ──────────────────────────────────────────────

INDEX_COLORS = {"SPY": CYAN, "QQQ": GOLD, "IWM": "#ff6b81", "DIA": "#7bed9f"}

def chart_index_trends(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    output_dir: Path,
    lookback_days: int = 60,
) -> Path | None:
    """Line chart of major index prices over recent period, normalized to 100."""
    _apply_dark_style()

    start = as_of - timedelta(days=int(lookback_days * 1.5))  # extra for weekends
    symbols = ["SPY", "QQQ", "IWM", "DIA"]

    fig, ax = plt.subplots(figsize=(10, 5))
    has_data = False

    for sym in symbols:
        rows = con.execute("""
            SELECT date, adj_close FROM prices_daily
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, [sym, start, as_of]).fetchall()

        if len(rows) < 10:
            continue

        dates = [r[0] for r in rows]
        prices = [r[1] for r in rows]
        # Normalize to 100 at start
        base = prices[0]
        if base and base > 0:
            normed = [p / base * 100 for p in prices]
            color = INDEX_COLORS.get(sym, TEXT_COLOR)
            ax.plot(dates, normed, color=color, linewidth=1.8, label=sym)
            # End label
            ax.text(dates[-1], normed[-1], f"  {sym} {normed[-1]:.1f}",
                    fontsize=8, color=color, va="center")
            has_data = True

    if not has_data:
        plt.close(fig)
        return None

    ax.axhline(y=100, color=TEXT_COLOR, linewidth=0.5, alpha=0.2, linestyle="--")
    ax.set_ylabel("Normalized Price (base=100)")
    ax.set_title(f"Major Indices — {lookback_days}D Trend", fontsize=14, fontweight="bold", pad=12)
    ax.legend(loc="upper left", fontsize=9,
              facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
    ax.grid(axis="both", alpha=0.15)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    fig.autofmt_xdate(rotation=30)

    path = output_dir / "index_trends.png"
    _save(fig, path)
    return path


# ── Chart 6: VIX Trend ──────────────────────────────────────────────────────

def chart_vix_trend(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    output_dir: Path,
    lookback_days: int = 90,
) -> Path | None:
    """VIX line chart with 20-day SMA and fear zone shading."""
    _apply_dark_style()

    start = as_of - timedelta(days=int(lookback_days * 1.5))
    rows = con.execute("""
        SELECT date, close FROM prices_daily
        WHERE symbol = '^VIX' AND date >= ? AND date <= ?
        ORDER BY date ASC
    """, [start, as_of]).fetchall()

    if len(rows) < 20:
        return None

    dates = [r[0] for r in rows]
    closes = [r[1] for r in rows]

    # 20-day SMA
    sma20 = []
    for i in range(len(closes)):
        if i < 19:
            sma20.append(None)
        else:
            sma20.append(sum(closes[i - 19:i + 1]) / 20)

    fig, ax = plt.subplots(figsize=(10, 4.5))

    # Fear zone shading
    ax.axhspan(20, 30, color=GOLD, alpha=0.06)
    ax.axhspan(30, 80, color=RED, alpha=0.06)
    ax.text(dates[0], 21, " Elevated", fontsize=7, color=GOLD, alpha=0.5)
    ax.text(dates[0], 31, " Fear", fontsize=7, color=RED, alpha=0.5)

    # VIX line
    ax.plot(dates, closes, color=RED, linewidth=1.5, label="VIX", alpha=0.9)
    ax.fill_between(dates, 0, closes, color=RED, alpha=0.08)

    # SMA
    valid_dates = [d for d, v in zip(dates, sma20) if v is not None]
    valid_sma = [v for v in sma20 if v is not None]
    if valid_sma:
        ax.plot(valid_dates, valid_sma, color=CYAN, linewidth=1.2,
                linestyle="--", label="20D SMA", alpha=0.7)

    # Current level annotation
    ax.text(dates[-1], closes[-1], f"  {closes[-1]:.1f}",
            fontsize=10, color=RED, fontweight="bold", va="center")

    ax.set_ylabel("VIX Level")
    ax.set_title("VIX — Market Fear Gauge", fontsize=14, fontweight="bold", pad=12)
    ax.legend(loc="upper left", fontsize=9,
              facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
    ax.grid(axis="both", alpha=0.15)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(bottom=max(0, min(closes) - 2), top=max(closes) + 5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    fig.autofmt_xdate(rotation=30)

    path = output_dir / "vix_trend.png"
    _save(fig, path)
    return path


# ── Chart 7: Top Movers ─────────────────────────────────────────────────────

def chart_top_movers(bundle: dict, output_dir: Path, n: int = 8) -> Path | None:
    """Butterfly bar chart — top winners vs losers from notable items."""
    items = bundle.get("notable_items", [])
    if not items:
        return None

    _apply_dark_style()

    with_ret = [(it["symbol"], it.get("ret_1d_pct", 0) or 0) for it in items]
    with_ret.sort(key=lambda x: x[1])

    losers = [x for x in with_ret if x[1] < 0][:n]
    winners = [x for x in with_ret if x[1] > 0][-n:]
    winners.reverse()

    if not losers and not winners:
        return None

    max_rows = max(len(losers), len(winners), 1)
    fig, (ax_lose, ax_win) = plt.subplots(1, 2, figsize=(10, max(4, max_rows * 0.4)),
                                           sharey=False)

    # Losers (left panel)
    if losers:
        syms_l = [x[0] for x in losers]
        vals_l = [x[1] for x in losers]
        ax_lose.barh(syms_l, vals_l, color=RED, edgecolor="none", height=0.6)
        for i, (s, v) in enumerate(losers):
            ax_lose.text(v - 0.1, i, f"{v:+.1f}%", va="center", ha="right", fontsize=8, color=TEXT_COLOR)
    ax_lose.set_title("Biggest Losers", fontsize=11, fontweight="bold", color=RED)
    ax_lose.invert_xaxis()
    ax_lose.spines["top"].set_visible(False)
    ax_lose.spines["right"].set_visible(False)
    ax_lose.spines["left"].set_visible(False)
    ax_lose.grid(axis="x", alpha=0.15)

    # Winners (right panel)
    if winners:
        syms_w = [x[0] for x in winners]
        vals_w = [x[1] for x in winners]
        ax_win.barh(syms_w, vals_w, color=GREEN, edgecolor="none", height=0.6)
        for i, (s, v) in enumerate(winners):
            ax_win.text(v + 0.1, i, f"{v:+.1f}%", va="center", ha="left", fontsize=8, color=TEXT_COLOR)
    ax_win.set_title("Biggest Winners", fontsize=11, fontweight="bold", color=GREEN)
    ax_win.spines["top"].set_visible(False)
    ax_win.spines["left"].set_visible(False)
    ax_win.spines["right"].set_visible(False)
    ax_win.grid(axis="x", alpha=0.15)

    fig.suptitle("Top Movers — Notable Items", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()

    path = output_dir / "top_movers.png"
    _save(fig, path)
    return path


# ── Main entry point ─────────────────────────────────────────────────────────

def generate_daily_charts(
    bundle: dict,
    output_dir: Path,
    con: duckdb.DuckDBPyConnection | None = None,
    as_of: date | None = None,
) -> list[Path]:
    """
    Generate all daily charts and return list of paths to created PNGs.
    Skips charts gracefully if data is missing.

    Pass `con` and `as_of` to enable time-series charts (index trends, VIX).
    """
    charts: list[Path] = []

    # Bundle-only charts (no DB needed)
    bundle_generators = [
        chart_sector_performance,
        chart_notable_items,
        chart_cross_asset_heatmap,
        chart_dyp_candidates,
        chart_top_movers,
    ]

    for gen in bundle_generators:
        try:
            path = gen(bundle, output_dir)
            if path:
                charts.append(path)
        except Exception as e:
            import structlog
            structlog.get_logger().warning("chart_generation_failed",
                                           chart=gen.__name__, error=str(e))

    # Time-series charts (need DB connection)
    if con is not None and as_of is not None:
        ts_generators = [
            (chart_index_trends, [con, as_of, output_dir]),
            (chart_vix_trend, [con, as_of, output_dir]),
        ]
        for gen, args in ts_generators:
            try:
                path = gen(*args)
                if path:
                    charts.append(path)
            except Exception as e:
                import structlog
                structlog.get_logger().warning("chart_generation_failed",
                                               chart=gen.__name__, error=str(e))

    return charts
