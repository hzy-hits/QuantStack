"""Realized horizon edge section (Phase B.14).

Extracted from scripts/generate_main_strategy_v2_report.py — renders
the close-to-close horizon performance table for US or CN actionables.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_pct, round_or_none


def _horizon_edge_row(summary: dict[str, Any], market: str, mode: str = "contract_gated") -> dict[str, Any]:
    return ((summary.get("by_mode_market") or {}).get(f"{mode}:{market.upper()}") or {})


def _horizon_verdict(market: str, horizon_rows: dict[str, Any]) -> str:
    def ok(h: str) -> bool:
        stats = horizon_rows.get(h) or {}
        wavg = round_or_none(stats.get("weighted_avg"))
        med = round_or_none(stats.get("median"))
        win = round_or_none(stats.get("win_rate"))
        n = int(stats.get("n") or 0)
        return n >= 10 and (wavg or 0.0) > 0 and (med or 0.0) > 0 and (win or 0.0) >= 0.5

    if market.upper() == "US":
        if ok("3") or ok("5"):
            return "US horizon edge is positive beyond 1D; still keep next-session review and let winners earn hold time."
        return "US edge is tactical: next-session review only; no mechanical 3D/5D hold."
    if ok("5"):
        return "CN 5D edge is currently positive; hold to T+5 only for names that still pass T+1/T+3 follow-through."
    if ok("3"):
        return "CN T+3 edge is usable; T+5 requires fresh follow-through confirmation."
    return "CN edge is short-cycle: T+1/T+3 review, no blind T+5 hold."


def render_realized_horizon_edge_section(payload: dict[str, Any], market: str) -> list[str]:
    summary = payload.get("report_action_backtest_summary") or {}
    data = _horizon_edge_row(summary, market)
    horizons = data.get("horizons") or {}
    if not horizons:
        return []
    lines = [
        f"## {market.upper()} Realized Horizon Edge",
        "",
        "- 来自最近日报 actionables 的 close-to-close 回测，用来决定默认复核/持有周期。",
        "",
        "| Horizon | N | R-weighted | Median | Win | Verdict |",
        "|---:|---:|---:|---:|---:|---|",
    ]
    for horizon in ("1", "3", "5", "10"):
        stats = horizons.get(horizon) or {}
        if not stats:
            continue
        wavg = round_or_none(stats.get("weighted_avg"))
        med = round_or_none(stats.get("median"))
        win = round_or_none(stats.get("win_rate"))
        good = (wavg or 0.0) > 0 and (med or 0.0) > 0 and (win or 0.0) >= 0.5
        verdict = "usable" if good else "review-only"
        lines.append(
            f"| {horizon}D | {stats.get('n') or 0} | {fmt_pct((wavg or 0.0) * 100.0)} | "
            f"{fmt_pct((med or 0.0) * 100.0)} | {fmt_pct((win or 0.0) * 100.0)} | {verdict} |"
        )
    lines += ["", f"- 执行结论: {_horizon_verdict(market, horizons)}", ""]
    return lines
