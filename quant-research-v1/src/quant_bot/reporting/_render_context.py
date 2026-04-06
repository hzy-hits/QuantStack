"""Render header, meta, market context, macro snapshot, headlines, polymarket."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from ._render_fmt import _fmt_pct, _fmt_val

# Monthly FRED series — show "monthly" cadence tag and don't treat lag as staleness
_MONTHLY_SERIES = {"CPIAUCSL", "UNRATE", "FEDFUNDS"}


def render_header_and_context(bundle: dict) -> list[str]:
    """Return all lines from header through end of market-context section."""
    lines: list[str] = []
    meta = bundle.get("meta", {})
    trade_date = meta.get("trade_date", str(date.today()))
    generated_at = meta.get("generated_at", datetime.now().isoformat(timespec="minutes"))

    # ── HEADER ───────────────────────────────────────────────────────────────
    lines += [
        f"# Quant Research Payload \u2014 {trade_date}",
        "",
        f"> **Generated:** {generated_at}",
        "> **For the agent:** This file contains all computed data for today's research report.",
        "> Every number was computed deterministically. Your job is to synthesize the narrative.",
        "> Do not invent numbers. Do not add tickers not listed here. Reduce confidence where data is flagged as missing or stale.",
        "> **Precision rule:** Model probabilities carry estimation error — never cite P=1.00 or P=0.00; use P\u22481.00 or 'near-certainty'.",
        "> **Macro data lag:** Monthly series (CPI, unemployment, Fed Funds) typically lag 1\u20132 months. Always cite the reference period.",
        "> Not financial advice. Research only.",
        "",
        "---",
        "",
    ]

    # ── META ─────────────────────────────────────────────────────────────────
    lines += [
        "## Meta",
        "",
        f"- **Trade date:** {trade_date}",
        f"- **Generated at:** {generated_at}",
        f"- **Benchmark:** {meta.get('benchmark', 'SPY')}",
        f"- **Universe scanned:** {meta.get('total_universe_size', '?')} symbols",
        f"- **Notable items selected:** {meta.get('notable_items_count', '?')}",
        "",
        "**Data freshness:**",
    ]
    for k, v in meta.get("data_freshness", {}).items():
        lines.append(f"- {k}: {v}")
    lines += ["", "---", ""]

    # ── MARKET CONTEXT ───────────────────────────────────────────────────────
    ctx = bundle.get("market_context", {})
    regime = ctx.get("regime", {})
    major_indices = ctx.get("major_indices", {})
    spy_ret_1d = (major_indices.get("SPY") or {}).get("ret_1d_pct")

    lines += [
        "## Market Context",
        "",
        f"**Regime:** {regime.get('label', '?').upper()} \u2014 "
        f"prevalence {regime.get('regime_prevalence_pct', 0)*100:.0f}% ({regime.get('prevalence_label', '?')})",
        "",
    ]

    counts = regime.get("counts", {})
    if counts:
        lines.append(f"Regime breakdown ({regime.get('total_analyzed', '?')} symbols analyzed):")
        for label, cnt in counts.items():
            lines.append(f"- {label}: {cnt}")
        lines.append("")

    lines += [
        f"**SPY today:** {_fmt_pct(spy_ret_1d)}",
        "",
    ]

    # HMM regime overlay
    lines += _render_hmm_regime(bundle)

    # Macro
    lines += _render_macro(ctx, trade_date)

    # Market headlines
    lines += _render_headlines(ctx)

    # Polymarket
    lines += _render_polymarket(ctx)

    lines += ["---", ""]
    return lines


# -- private helpers --------------------------------------------------------


def _render_hmm_regime(bundle: dict) -> list[str]:
    """Render the HMM market-level regime overlay section."""
    hmm = bundle.get("hmm_regime")
    if not hmm:
        return []

    regime = hmm["regime"]
    p_bull = hmm["p_bull"]
    p_bear = hmm["p_bear"]
    trans = hmm["transition_matrix"]
    days = hmm["days_in_current_regime"]
    converged = hmm["model_converged"]
    n_obs = hmm.get("n_observations", "?")
    bull_means = hmm["state_means"]["bull"]
    bear_means = hmm["state_means"]["bear"]

    # 1-step-ahead forecast (from HMM module, if available)
    p_bull_tomorrow = hmm.get("p_bull_tomorrow")
    p_ret_pos_tomorrow = hmm.get("p_ret_positive_tomorrow")

    lines = [
        "### HMM Market Regime",
        "",
        f"Current state inference: **{regime.upper()}** "
        f"(posterior: P={p_bull:.3f} bull / {p_bear:.3f} bear)",
    ]

    # Show 1-step-ahead forecast if available
    if p_bull_tomorrow is not None:
        lines.append(
            f"Next-day forecast: P(bull state tomorrow)={p_bull_tomorrow:.3f}"
        )
    if p_ret_pos_tomorrow is not None:
        lines.append(
            f"Model-implied P(SPY up tomorrow)={p_ret_pos_tomorrow:.3f}"
        )

    lines += [
        f"Transition matrix: P(bull\u2192bull)={trans[0][0]:.3f}, P(bear\u2192bear)={trans[1][1]:.3f}",
        f"Days in current regime: {days}",
        f"Bull state: mean_return={bull_means['ret_mean_pct']:+.4f}%, mean_VIX={bull_means['vix_mean']:.1f}",
        f"Bear state: mean_return={bear_means['ret_mean_pct']:+.4f}%, mean_VIX={bear_means['vix_mean']:.1f}",
        f"Model: {n_obs} obs, converged={converged}",
    ]

    # Calibration metrics (from forecast resolution, if available)
    cal = hmm.get("calibration")
    if cal and cal.get("n", 0) > 0:
        brier = cal.get("brier_score")
        hit = cal.get("hit_rate")
        n = cal["n"]
        brier_str = f"{brier:.3f}" if brier is not None else "N/A"
        hit_str = f"{hit*100:.1f}%" if hit is not None else "N/A"
        lines.append(
            f"Calibration (last {n} forecasts): Brier={brier_str}, "
            f"hit_rate={hit_str} (n={n})"
        )
        lines.append(
            "*Brier: 0=perfect, 0.25=coin flip. "
            "Hit rate: direction accuracy of P(SPY up)>0.5.*"
        )
    else:
        lines.append(
            "*Calibration: insufficient resolved forecasts. "
            "Treat regime labels as descriptive, not predictive.*"
        )

    lines += [
        "",
        "*HMM is a market-level overlay (SPY+VIX). "
        "Per-symbol autocorrelation regime is separate. "
        "Posterior probabilities carry estimation error — do not treat as exact.*",
        "",
    ]
    return lines


def _render_macro(ctx: dict, trade_date_str: str = "") -> list[str]:
    macro = ctx.get("macro", {})
    if not macro:
        return []

    try:
        trade_dt = date.fromisoformat(trade_date_str) if trade_date_str else date.today()
    except ValueError:
        trade_dt = date.today()

    lines = [
        "### Macro Snapshot",
        "",
        "| Indicator | Value | Ref Period | Cadence |",
        "|-----------|-------|------------|---------|",
    ]
    for label, data in macro.items():
        as_of = data.get("as_of", "?")
        sid = data.get("series_id", "")
        cadence = "monthly" if sid in _MONTHLY_SERIES else "daily"
        lines.append(
            f"| {label} | {_fmt_val(data.get('value'), 4)} | {as_of} | {cadence} |"
        )

    lines += [
        "",
        "*Monthly series (CPI, unemployment, Fed Funds) report reference-period data "
        "that typically lags 1\u20132 months. Cite the Ref Period, not the trade date, "
        "when using these figures.*",
        "",
    ]
    return lines


def _render_headlines(ctx: dict) -> list[str]:
    headlines = ctx.get("market_headlines", [])
    if not headlines:
        return []
    lines = ["### Market Headlines (last 3 days)", ""]
    for h in headlines:
        lines.append(f"- **{h.get('source', '?')}** \u2014 {h.get('headline', '')}")
    lines += [""]
    return lines


def _render_polymarket(ctx: dict) -> list[str]:
    poly = ctx.get("polymarket_events", [])
    if not poly:
        return []
    lines = [
        "### Polymarket Crowd Probabilities (macro events)",
        "",
        "*Minimum $10K volume, fetched within last 2 days. "
        "Probabilities are snapshots — prices change continuously.*",
        "",
        "| Question | P(Yes) | Δ | P(No) | Volume USD | End Date | Fetched (UTC) |",
        "|----------|--------|---|-------|------------|----------|---------------|",
    ]
    for e in poly:
        q = e.get("question", "")[:80]
        end_date = e.get("end_date", "?") or "?"
        fetched_raw = e.get("fetched_at", "?") or "?"
        # Truncate fetched_at to minute precision for readability
        if fetched_raw and fetched_raw != "?" and len(fetched_raw) > 16:
            fetched = fetched_raw[:16]
        else:
            fetched = fetched_raw
        # Format Δ with sign
        delta = e.get("p_yes_delta")
        if delta is not None:
            delta_str = f"{delta:+.3f}"
        else:
            delta_str = "—"
        lines.append(
            f"| {q} | {_fmt_val(e.get('p_yes'), 3)} | "
            f"{delta_str} | "
            f"{_fmt_val(e.get('p_no'), 3)} | "
            f"${e.get('volume_usd', 0):,.0f} | "
            f"{end_date} | {fetched} |"
        )

    lines += [""]
    return lines
