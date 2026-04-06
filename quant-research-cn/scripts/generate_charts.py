#!/usr/bin/env python3
"""Generate daily report charts for A-share pipeline.

Charts:
  1. Index Proxy Trends — top liquid stocks + benchmark proxy, 60-day normalized
  2. Notable Items — top 30 by composite score (from analytics)
  3. Sector Fund Flow — sector-level net inflow or concept board performance
  4. Information Score — component decomposition for top items
  5. Fund Flow Overview — margin balance trend
  6. Volatility — 20-day rolling realized vol of a liquid benchmark proxy

Usage:
    python scripts/generate_charts.py [--date 2026-03-12] [--db data/quant_cn.duckdb]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
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

# Chinese font
_CJK_FONT_FOUND = False
for font in ["Noto Sans SC", "WenQuanYi Micro Hei", "Noto Sans CJK SC", "SimHei", "PingFang SC"]:
    try:
        matplotlib.font_manager.findfont(font, fallback_to_default=False)
        plt.rcParams["font.sans-serif"] = [font, "DejaVu Sans"]
        _CJK_FONT_FOUND = True
        break
    except Exception:
        continue
if not _CJK_FONT_FOUND:
    import warnings
    warnings.warn("No CJK font found — Chinese text will render as boxes")
plt.rcParams["axes.unicode_minus"] = False


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
    })


def _save(fig, path: Path, dpi: int = 150):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(path), dpi=dpi, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def _parse_date(s: str) -> date:
    s = s.strip()
    if len(s) == 10 and "-" in s:
        parts = s.split("-")
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    elif len(s) == 8:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    raise ValueError(f"Cannot parse date: {s}")


# ── Chart 1: Index Proxy Trends ─────────────────────────────────────────────

# Use liquid large-caps as proxy for market segments
INDEX_PROXY = {
    "601318.SH": ("Ping An / Finance", CYAN),
    "600519.SH": ("Moutai / Consumer", GOLD),
    "300750.SZ": ("CATL / NewEnergy", "#ff6b81"),
    "688981.SH": ("SMIC / Tech", "#7bed9f"),
}


def chart_index_trends(con, as_of: date, output_dir: Path, lookback: int = 60) -> Path | None:
    _apply_dark_style()
    start = as_of - timedelta(days=int(lookback * 1.6))

    fig, ax = plt.subplots(figsize=(10, 5))
    has_data = False

    for ts_code, (label, color) in INDEX_PROXY.items():
        rows = con.execute("""
            SELECT CAST(trade_date AS VARCHAR), close FROM prices
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
        """, [ts_code, str(start), str(as_of)]).fetchall()

        if len(rows) < 10:
            continue

        dates = [_parse_date(r[0]) for r in rows]
        prices = [r[1] for r in rows]
        base = prices[0]
        if not base or base <= 0:
            continue

        normed = [p / base * 100 for p in prices]
        ax.plot(dates, normed, color=color, linewidth=1.8, label=label)
        ax.text(dates[-1], normed[-1], f"  {normed[-1]:.1f}",
                fontsize=8, color=color, va="center")
        has_data = True

    if not has_data:
        plt.close(fig)
        return None

    ax.axhline(y=100, color=TEXT_COLOR, linewidth=0.5, alpha=0.2, linestyle="--")
    ax.set_ylabel("Normalized Price (base=100)")
    ax.set_title(f"Market Proxy — {lookback}D Trend", fontsize=14, fontweight="bold", pad=12)
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


# ── Chart 2: Notable Items ──────────────────────────────────────────────────

CONFIDENCE_COLORS = {"HIGH": RED, "MODERATE": GOLD, "WATCH": CYAN, "LOW": "#7f8fa6"}
DIRECTION_ARROWS = {
    "bullish": ("▲", GREEN),
    "bearish": ("▼", RED),
    "neutral": ("◆", TEXT_COLOR),
}


def chart_notable_items(con, as_of: date, output_dir: Path) -> Path | None:
    """Top items by information_score from analytics table."""
    _apply_dark_style()

    rows = con.execute("""
        SELECT ts_code, value FROM analytics
        WHERE module = 'flow' AND metric = 'information_score'
          AND CAST(as_of AS VARCHAR) = ?
        ORDER BY value DESC
        LIMIT 30
    """, [str(as_of)]).fetchall()

    if not rows:
        return None

    # Get momentum trend_prob for direction hint
    trend_probs = {}
    tp_rows = con.execute("""
        SELECT ts_code, value FROM analytics
        WHERE module = 'momentum' AND metric = 'trend_prob'
          AND CAST(as_of AS VARCHAR) = ?
    """, [str(as_of)]).fetchall()
    for r in tp_rows:
        trend_probs[r[0]] = r[1]

    rows = list(reversed(rows))
    symbols = [r[0] for r in rows]
    scores = [r[1] or 0 for r in rows]

    # Classify
    directions = []
    confidences = []
    for sym, score in zip(symbols, scores):
        tp = trend_probs.get(sym, 0.5)
        if tp > 0.55:
            directions.append("bullish")
        elif tp < 0.45:
            directions.append("bearish")
        else:
            directions.append("neutral")

        if score > 0.7:
            confidences.append("HIGH")
        elif score > 0.5:
            confidences.append("MODERATE")
        elif score > 0.3:
            confidences.append("WATCH")
        else:
            confidences.append("LOW")

    colors = [CONFIDENCE_COLORS.get(c, "#7f8fa6") for c in confidences]

    fig, ax = plt.subplots(figsize=(9, max(6, len(rows) * 0.32)))
    bars = ax.barh(symbols, scores, color=colors, edgecolor="none", height=0.7)

    for bar, direction, score in zip(bars, directions, scores):
        arrow, acolor = DIRECTION_ARROWS.get(direction, ("◆", TEXT_COLOR))
        ax.text(score + 0.003, bar.get_y() + bar.get_height() / 2,
                f" {arrow} {score:.3f}", va="center", fontsize=7, color=acolor)

    ax.set_xlabel("Information Score")
    ax.set_title("Signal Strength — Top 30", fontsize=14, fontweight="bold", pad=12)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=CONFIDENCE_COLORS["HIGH"], label="HIGH"),
        Patch(facecolor=CONFIDENCE_COLORS["MODERATE"], label="MODERATE"),
        Patch(facecolor=CONFIDENCE_COLORS["WATCH"], label="WATCH"),
        Patch(facecolor=CONFIDENCE_COLORS["LOW"], label="LOW"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=8,
              facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)

    path = output_dir / "notable_items.png"
    _save(fig, path)
    return path


# ── Chart 3: Sector Fund Flow ───────────────────────────────────────────────

def chart_sector_flow(con, as_of: date, output_dir: Path) -> Path | None:
    _apply_dark_style()

    # Try sector_fund_flow first
    rows = con.execute("""
        SELECT sector_name, pct_chg, main_net_in
        FROM sector_fund_flow
        WHERE CAST(trade_date AS VARCHAR) =
            (SELECT MAX(CAST(trade_date AS VARCHAR)) FROM sector_fund_flow
             WHERE trade_date <= ?)
        ORDER BY main_net_in ASC
    """, [str(as_of)]).fetchall()

    if not rows or len(rows) < 3:
        # Fall back: concept boards
        rows = con.execute("""
            SELECT board_name, pct_chg, amount
            FROM concept_board
            WHERE CAST(trade_date AS VARCHAR) =
                (SELECT MAX(CAST(trade_date AS VARCHAR)) FROM concept_board
                 WHERE trade_date <= ?)
            ORDER BY pct_chg DESC
            LIMIT 30
        """, [str(as_of)]).fetchall()

        if not rows or len(rows) < 3:
            return None
        else:
            names = [r[0] for r in rows]
            values = [r[1] or 0 for r in rows]
            items = list(zip(names, values))
            items.sort(key=lambda x: x[1])
            names = [x[0] for x in items]
            values = [x[1] for x in items]
            fig, ax = plt.subplots(figsize=(9, max(6, len(items) * 0.3)))
            colors = [GREEN if v >= 0 else RED for v in values]
            ax.barh(names, values, color=colors, edgecolor="none", height=0.7)
            ax.set_xlabel("涨跌幅 (%)")
            ax.set_title("概念板块 — 今日涨跌", fontsize=14, fontweight="bold", pad=12)
    else:
        names = [r[0] for r in rows]
        net_in = [r[2] or 0 for r in rows]
        fig, ax = plt.subplots(figsize=(9, max(6, len(names) * 0.35)))
        colors = [GREEN if v >= 0 else RED for v in net_in]
        ax.barh(names, net_in, color=colors, edgecolor="none", height=0.7)
        ax.set_xlabel("主力净流入 (万元)")
        ax.set_title("行业资金流向 — 主力净流入", fontsize=14, fontweight="bold", pad=12)

    ax.axvline(x=0, color=TEXT_COLOR, linewidth=0.5, alpha=0.3)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    path = output_dir / "sector_flow.png"
    _save(fig, path)
    return path


# ── Chart 4: Info Score Components ──────────────────────────────────────────

COMPONENT_COLORS = {
    "large_flow_z": CYAN,
    "northbound_z": GOLD,
    "margin_z": "#ff6b81",
    "block_z": "#7bed9f",
    "hot_z": RED,
    "insider_z": BLUE,
    "event_clock": "#a29bfe",
    "market_vol_z": "#636e72",
}


def chart_info_score_breakdown(con, as_of: date, output_dir: Path, top_n: int = 15) -> Path | None:
    _apply_dark_style()

    # Get top items by information_score
    top = con.execute("""
        SELECT ts_code FROM analytics
        WHERE module = 'flow' AND metric = 'information_score'
          AND CAST(as_of AS VARCHAR) = ?
        ORDER BY value DESC LIMIT ?
    """, [str(as_of), top_n]).fetchall()

    if not top:
        return None

    ts_codes = [r[0] for r in top]
    components = list(COMPONENT_COLORS.keys())

    data = {}
    for tc in ts_codes:
        data[tc] = {}
        for comp in components:
            rows = con.execute("""
                SELECT value FROM analytics
                WHERE ts_code = ? AND module = 'flow' AND metric = ?
                  AND CAST(as_of AS VARCHAR) = ?
            """, [tc, comp, str(as_of)]).fetchall()
            data[tc][comp] = abs(rows[0][0]) if rows and rows[0][0] else 0.0

    fig, ax = plt.subplots(figsize=(10, max(5, top_n * 0.38)))

    y = np.arange(len(ts_codes))
    ts_codes_rev = list(reversed(ts_codes))

    # Weights matching flow.rs
    weights = {
        "large_flow_z": 0.30, "northbound_z": 0.18, "margin_z": 0.15,
        "block_z": 0.10, "hot_z": 0.08, "insider_z": 0.07,
        "event_clock": 0.07, "market_vol_z": 0.05,
    }

    lefts = np.zeros(len(ts_codes))
    for comp in components:
        w = weights.get(comp, 0.1)
        vals = [data[tc].get(comp, 0) * w for tc in ts_codes_rev]
        color = COMPONENT_COLORS.get(comp, TEXT_COLOR)
        label = comp.replace("_z", "").replace("_", " ")
        ax.barh(y, vals, left=lefts, height=0.6, label=label, color=color, edgecolor="none")
        lefts += np.array(vals)

    ax.set_yticks(y)
    ax.set_yticklabels(ts_codes_rev, fontsize=9)
    ax.set_xlabel("Weighted Signal Strength")
    ax.set_title("Info Score Breakdown — Top Items", fontsize=14, fontweight="bold", pad=12)
    ax.grid(axis="x", alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="lower right", fontsize=7, ncol=2,
              facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)

    path = output_dir / "info_score_breakdown.png"
    _save(fig, path)
    return path


# ── Chart 5: Margin Balance Trend ───────────────────────────────────────────

def chart_fund_flow_trend(con, as_of: date, output_dir: Path, lookback: int = 60) -> Path | None:
    _apply_dark_style()
    start = as_of - timedelta(days=int(lookback * 1.6))

    # Margin balance trend (most reliable flow data we have)
    margin_rows = con.execute("""
        SELECT CAST(trade_date AS VARCHAR),
               SUM(COALESCE(rzye, 0)) / 1e8 as total_rzye_yi
        FROM margin_detail
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """, [str(start), str(as_of)]).fetchall()

    # Northbound flow
    nb_rows = con.execute("""
        SELECT CAST(trade_date AS VARCHAR), net_amount
        FROM northbound_flow
        WHERE source = 'total'
          AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date ASC
    """, [str(start), str(as_of)]).fetchall()

    if (not margin_rows or len(margin_rows) < 3) and (not nb_rows or len(nb_rows) < 3):
        return None

    fig, ax1 = plt.subplots(figsize=(10, 5))
    has_data = False

    if nb_rows and len(nb_rows) >= 3:
        dates_nb = [_parse_date(r[0]) for r in nb_rows]
        vals_nb = [r[1] / 1e4 if r[1] else 0 for r in nb_rows]
        colors_nb = [GREEN if v >= 0 else RED for v in vals_nb]
        ax1.bar(dates_nb, vals_nb, color=colors_nb, alpha=0.7, width=0.8, label="Northbound Net(B)")
        ax1.set_ylabel("Northbound Net Buy (100M)", color=CYAN)
        has_data = True

    if margin_rows and len(margin_rows) >= 3:
        if has_data:
            ax2 = ax1.twinx()
        else:
            ax2 = ax1
        dates_m = [_parse_date(r[0]) for r in margin_rows]
        vals_m = [r[1] for r in margin_rows]
        ax2.plot(dates_m, vals_m, color=GOLD, linewidth=2, label="Margin Balance(B)")
        ax2.set_ylabel("Margin Balance (100M)", color=GOLD)
        ax2.tick_params(axis="y", labelcolor=GOLD)
        has_data = True

    if not has_data:
        plt.close(fig)
        return None

    ax1.set_title("Fund Flow Trend", fontsize=14, fontweight="bold", pad=12)
    ax1.grid(axis="y", alpha=0.15)
    ax1.spines["top"].set_visible(False)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    fig.autofmt_xdate(rotation=30)

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    try:
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8,
                   facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
    except Exception:
        ax1.legend(loc="upper left", fontsize=8,
                   facecolor=PANEL_BG, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)

    path = output_dir / "fund_flow_trend.png"
    _save(fig, path)
    return path


# ── Chart 6: Realized Volatility ────────────────────────────────────────────

def chart_volatility(con, as_of: date, output_dir: Path, lookback: int = 120) -> Path | None:
    """Use a liquid large-cap (601318 中国平安) as market vol proxy."""
    _apply_dark_style()
    start = as_of - timedelta(days=int(lookback * 1.6))

    # Try multiple liquid stocks
    for proxy, label in [("601318.SH", "沪深大盘"), ("600519.SH", "沪深大盘"), ("000001.SZ", "沪深大盘")]:
        rows = con.execute("""
            SELECT CAST(trade_date AS VARCHAR), pct_chg FROM prices
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
        """, [proxy, str(start), str(as_of)]).fetchall()

        if len(rows) >= 30:
            break
    else:
        return None

    dates = [_parse_date(r[0]) for r in rows]
    returns = [r[1] / 100.0 if r[1] else 0 for r in rows]

    window = 20
    vol20 = []
    for i in range(len(returns)):
        if i < window - 1:
            vol20.append(None)
        else:
            chunk = returns[i - window + 1:i + 1]
            std = np.std(chunk)
            vol20.append(std * np.sqrt(252) * 100)

    valid_dates = [d for d, v in zip(dates, vol20) if v is not None]
    valid_vol = [v for v in vol20 if v is not None]

    if not valid_vol:
        return None

    fig, ax = plt.subplots(figsize=(10, 4.5))

    ax.axhspan(0, 15, color=GREEN, alpha=0.04)
    ax.axhspan(15, 25, color=GOLD, alpha=0.04)
    ax.axhspan(25, 60, color=RED, alpha=0.04)
    ax.text(valid_dates[0], 8, "  Low", fontsize=7, color=GREEN, alpha=0.5)
    ax.text(valid_dates[0], 18, "  Mid", fontsize=7, color=GOLD, alpha=0.5)
    ax.text(valid_dates[0], 30, "  High", fontsize=7, color=RED, alpha=0.5)

    ax.plot(valid_dates, valid_vol, color=CYAN, linewidth=1.5)
    ax.fill_between(valid_dates, 0, valid_vol, color=CYAN, alpha=0.1)

    if valid_vol:
        ax.text(valid_dates[-1], valid_vol[-1], f"  {valid_vol[-1]:.1f}%",
                fontsize=10, color=CYAN, fontweight="bold", va="center")

    ax.set_ylabel("Annualized Vol (%)")
    ax.set_title(f"20D Rolling Vol — {proxy}", fontsize=14, fontweight="bold", pad=12)
    ax.grid(axis="both", alpha=0.15)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(bottom=max(0, min(valid_vol) - 2), top=max(valid_vol) + 5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    fig.autofmt_xdate(rotation=30)

    path = output_dir / "volatility.png"
    _save(fig, path)
    return path


# ── Main ─────────────────────────────────────────────────────────────────────

def generate_all_charts(db_path: str, as_of: date, output_dir: Path) -> list[Path]:
    con = duckdb.connect(db_path, read_only=True)
    charts: list[Path] = []

    generators = [
        ("index_trends", lambda: chart_index_trends(con, as_of, output_dir)),
        ("notable_items", lambda: chart_notable_items(con, as_of, output_dir)),
        ("sector_flow", lambda: chart_sector_flow(con, as_of, output_dir)),
        ("info_score", lambda: chart_info_score_breakdown(con, as_of, output_dir)),
        ("fund_flow", lambda: chart_fund_flow_trend(con, as_of, output_dir)),
        ("volatility", lambda: chart_volatility(con, as_of, output_dir)),
    ]

    for name, gen in generators:
        try:
            path = gen()
            if path:
                charts.append(path)
                print(f"  [OK] {name} -> {path} ({path.stat().st_size / 1024:.0f} KB)")
            else:
                print(f"  [SKIP] {name} -- insufficient data")
        except Exception as e:
            print(f"  [FAIL] {name} -- {e}")

    con.close()
    return charts


def main():
    parser = argparse.ArgumentParser(description="Generate A-share charts")
    parser.add_argument("--date", default=None, help="Date (YYYY-MM-DD)")
    parser.add_argument("--db", default="data/quant_cn.duckdb", help="DuckDB path")
    args = parser.parse_args()

    if args.date:
        parts = args.date.split("-")
        as_of = date(int(parts[0]), int(parts[1]), int(parts[2]))
    else:
        from datetime import datetime
        import zoneinfo
        as_of = datetime.now(zoneinfo.ZoneInfo("Asia/Shanghai")).date()

    output_dir = Path("reports") / "charts" / str(as_of)
    print(f"Generating charts for {as_of} -> {output_dir}")
    charts = generate_all_charts(args.db, as_of, output_dir)
    print(f"\n{len(charts)} charts generated.")


if __name__ == "__main__":
    main()
