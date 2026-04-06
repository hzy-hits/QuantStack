"""Render options positioning extremes (bullish + bearish tables)."""
from __future__ import annotations

from typing import Any

from ._render_fmt import _fmt_pct, _fmt_val


def render_options_extremes(bundle: dict) -> list[str]:
    """Return lines for the options-extremes section, or empty if none."""
    extremes = bundle.get("options_extremes", {})
    ext_bull = extremes.get("bullish", [])
    ext_bear = extremes.get("bearish", [])
    if not ext_bull and not ext_bear:
        return []

    lines: list[str] = [
        "## Options Positioning Extremes",
        "",
        "Top 5 most extreme bullish and bearish options positioning from the notable universe.",
        "*Ranked by options directional score. P/C ratio < 0.5 = bullish flow, > 1.5 = bearish flow.*",
        "*IV Skew < 1.0 = calls priced richer than puts (bullish), > 1.0 = puts priced richer (bearish).*",
        "",
    ]

    if ext_bull:
        lines += _extremes_table("Extreme Bullish Options", ext_bull)

    if ext_bear:
        lines += _extremes_table("Extreme Bearish Options", ext_bear)

    lines += ["---", ""]
    return lines


def _extremes_table(title: str, entries: list[dict]) -> list[str]:
    lines = [
        f"### {title}",
        "",
        "| # | Symbol | P/C Ratio | IV Skew | Bias | Unusual Flow | Ret 1D | Confidence |",
        "|---|--------|-----------|---------|------|--------------|--------|------------|",
    ]
    for i, e in enumerate(entries, 1):
        pc_str = _fmt_val(e.get("put_call_ratio"), 2) if e.get("put_call_ratio") is not None else "\u2014"
        skew_str = _fmt_val(e.get("iv_skew"), 3) if e.get("iv_skew") is not None else "\u2014"
        bias = (e.get("bias_signal") or "\u2014").upper()
        flow = e.get("unusual_flow_summary") or "\u2014"
        ret = _fmt_pct(e.get("ret_1d_pct"))
        conf = e.get("confidence", "\u2014")
        iv_note = " \u26a0\ufe0f" if e.get("iv_data_quality") == "broken" else ""
        lines.append(
            f"| {i} | **{e['symbol']}** | {pc_str} | {skew_str}{iv_note} | {bias} | {flow} | {ret} | {conf} |"
        )
    lines += [""]
    return lines
