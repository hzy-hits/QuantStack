"""EMA 21/50 tape overlay standalone sheet (Phase B.17)."""
from __future__ import annotations

from typing import Any

from lib.fmt import fmt_pct


def render_ema_tape_overlay_markdown(overlay: dict[str, dict[str, Any]], as_of: str) -> str:
    """Render `payload["ema_tape_overlay"]` as a standalone tape sheet.

    Sorted by cross_state (bull/tangled/bear), then by EMA21 5d slope desc so
    the strongest "bull; rising" names sit at the top. The methodology limits
    K-line to tape/crowding/risk; this artifact is for reviewer eyeballs, not
    for evidence of supply-chain relationships.
    """
    rows: list[tuple[str, dict[str, Any]]] = []
    for symbol, entry in overlay.items():
        metrics = entry.get("metrics")
        if not metrics:
            continue
        rows.append((symbol, entry))

    cross_rank = {"bull": 0, "tangled": 1, "bear": 2}

    def _slope(entry: dict[str, Any]) -> float:
        return entry.get("metrics", {}).get("slope_21d_5d_pct") or 0.0

    rows.sort(
        key=lambda pair: (
            cross_rank.get(pair[1]["metrics"].get("cross_state") or "tangled", 3),
            -_slope(pair[1]),
            pair[0],
        )
    )

    lines: list[str] = [
        f"# AI Infra EMA 21/50 Tape Overlay - {as_of}",
        "",
        "- 数据源: AI universe + source-review queue tickers.",
        "- 排序: cross_state (bull → tangled → bear)，再按 EMA21 5d slope 降序。",
        "- K-line 反映 tape / crowding / 风险情绪,看不到基本面和供应链 —— 不要拿它当证据。",
        "",
        "| Symbol | Market | As-of | Cross | Recent Cross | Slope 5d | Close vs EMA21 | Close vs EMA50 |",
        "|---|---|---|---|---|---:|---:|---:|",
    ]
    if not rows:
        lines += ["| - | - | - | - | - | - | - | - |", ""]
        return "\n".join(lines) + "\n"
    for symbol, entry in rows:
        metrics = entry.get("metrics") or {}
        cross = metrics.get("cross_state") or "-"
        recent = metrics.get("recent_cross") or "-"
        slope = metrics.get("slope_21d_5d_pct")
        d21 = metrics.get("dist_close_ema21_pct")
        d50 = metrics.get("dist_close_ema50_pct")
        lines.append(
            "| "
            + " | ".join(
                [
                    symbol,
                    entry.get("market") or "-",
                    metrics.get("as_of") or "-",
                    cross,
                    recent,
                    fmt_pct(slope) if slope is not None else "-",
                    fmt_pct(d21) if d21 is not None else "-",
                    fmt_pct(d50) if d50 is not None else "-",
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"
