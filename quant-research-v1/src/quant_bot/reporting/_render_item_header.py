"""Render per-item header: signal badge, source table, sub-scores, exhaustion flags."""
from __future__ import annotations

from typing import Any

from ._render_fmt import _fmt_val


def render_item_header(i: int, item: dict, compact: bool = False) -> list[str]:
    """Return lines for the header portion of a single notable item."""
    sym = item.get("symbol", "?")
    score = item.get("score", 0)
    reason = item.get("primary_reason", "?")
    sub = item.get("sub_scores", {})

    # Signal classification header
    sig = item.get("signal", {})
    sig_conf = sig.get("confidence", "\u2014")
    sig_dir = sig.get("direction", "\u2014")
    sig_type = sig.get("signal_type", "\u2014")
    sig_score = sig.get("direction_score", 0)

    badge = _signal_badge(sig_conf, sig_dir)
    macro_parts = _macro_parts(sig)
    selection_lines = _selection_lines(item)

    header_line = f"### {i}. {sym} [{badge}]"
    signal_line = f"**Signal:** {sig_type} | **Direction score:** {sig_score:+.3f} | **Macro:** {macro_parts}"

    if compact:
        return [header_line, signal_line, *selection_lines, ""]

    lines: list[str] = [header_line, "", signal_line]
    lines += selection_lines

    # Source alignment detail
    lines += _source_alignment(sig)

    # Per-source scores
    lines += _source_details_table(sig)

    lines += [
        "",
        f"**Notability score:** {score:.4f} | **Primary driver:** {reason}",
        f"**Sub-scores:** magnitude={sub.get('magnitude', 0):.2f} | "
        f"event={sub.get('event', 0):.2f} | "
        f"momentum={sub.get('momentum', 0):.2f} | "
        f"options={sub.get('options', 0):.2f} | "
        f"cross_asset={sub.get('cross_asset', 0):.2f}",
    ]

    # Exhaustion flags
    lines += _exhaustion_flags(item, sig)
    lines.append("")
    return lines


# -- private helpers --------------------------------------------------------


def _signal_badge(conf: str, direction: str) -> str:
    if conf == "HIGH":
        return f"\U0001f534 **{conf}** \u2014 {direction.upper()}"
    elif conf == "MODERATE":
        return f"\U0001f7e1 **{conf}** \u2014 {direction.upper()}"
    elif conf == "LOW":
        return f"\u26aa **{conf}**"
    return "\u2014 no signal"


def _macro_parts(sig: dict) -> str:
    macro_regime = sig.get("macro_regime", "neutral")
    macro_gate_val = sig.get("macro_gate", 1.0)
    parts = f"regime={macro_regime}"
    if macro_gate_val != 1.0:
        parts += f", gate=\u00d7{macro_gate_val:.2f}"
    asset_bucket = sig.get("macro_asset_bucket")
    if asset_bucket:
        parts += f", bucket={asset_bucket}"
    overlays = sig.get("macro_overlays", [])
    if overlays:
        parts += f", overlays={'+'.join(overlays)}"
    return parts


def _source_alignment(sig: dict) -> list[str]:
    aligned = sig.get("sources_aligned", [])
    conflicting = sig.get("sources_conflicting", [])
    if not aligned and not conflicting:
        return []
    parts = []
    if aligned:
        parts.append(f"aligned: {', '.join(aligned)}")
    if conflicting:
        parts.append(f"**conflicting: {', '.join(conflicting)}**")
    return [f"**Sources:** {' | '.join(parts)}"]


def _source_details_table(sig: dict) -> list[str]:
    src_details = sig.get("source_details", {})
    if not src_details:
        return []
    lines = [
        "",
        "| Source | Direction | Score | Quality |",
        "|--------|-----------|-------|---------|",
    ]
    for src_name, sd in src_details.items():
        lines.append(
            f"| {src_name} | {sd['direction']} | {sd['signed_score']:+.3f} | {sd['quality']:.2f} |"
        )
    return lines


def _exhaustion_flags(item: dict, sig: dict) -> list[str]:
    flags: list[str] = []
    cross_div = sig.get("cross_asset_divergence")
    if cross_div:
        ref_parts = ", ".join(f"{k} {v:+.1f}%" for k, v in cross_div.get("ref_returns", {}).items())
        flags.append(f"DIVERGENCE (stock {cross_div.get('item_ret_1d', 0):+.1f}% vs {ref_parts})")
    mom_accel = (item.get("momentum") or {}).get("momentum_accel")
    if mom_accel is not None:
        if 0 < mom_accel < 0.3:
            flags.append(f"DECEL (5D/20D={mom_accel:.2f})")
        elif mom_accel < 0:
            flags.append(f"REVERSING (5D/20D={mom_accel:.2f})")
    cp68 = (item.get("options") or {}).get("cone_position_68")
    if cp68 is not None:
        if cp68 > 0.85:
            flags.append(f"CONE_TOP ({cp68:.2f})")
        elif cp68 < 0.15:
            flags.append(f"CONE_BOTTOM ({cp68:.2f})")
    # Show exhaustion downgrade from classify.py (HIGH → MODERATE)
    downgrade = sig.get("exhaustion_downgrade")
    if downgrade:
        flags.append(f"DOWNGRADED from HIGH ({', '.join(downgrade)})")
    if not flags:
        return []
    return [f"**Exhaustion Flags:** {' | '.join(flags)}"]


def _selection_lines(item: dict) -> list[str]:
    selection = item.get("selection") or {}
    fundamentals = item.get("fundamentals") or {}
    lane = item.get("report_bucket") or selection.get("lane")
    tradability = selection.get("tradability_score")
    market_cap = fundamentals.get("market_cap_musd")
    avg_dollar_volume = item.get("avg_dollar_volume_20d")
    execution_gate = item.get("execution_gate") or {}
    headline_mode = str(item.get("_headline_mode") or "unknown").lower()

    confirmations = []
    if selection.get("named_core"):
        confirmations.append("core universe")
    if selection.get("has_liquid_options"):
        confirmations.append("liquid options")
    if selection.get("has_lab_factor"):
        confirmations.append("lab factor")

    lines = [
        (
            f"**Report lane:** {_lane_label(lane)}"
            f" | **Tradability:** {_fmt_val(tradability, 3)}"
            f" | **Mkt cap:** {_fmt_market_cap(market_cap)}"
            f" | **ADV20:** {_fmt_dollar_volume(avg_dollar_volume)}"
        )
    ]
    if execution_gate:
        action = execution_gate.get("action", "executable_now")
        gap_vs_move = execution_gate.get("gap_vs_expected_move")
        lines.append(
            f"**Execution read:** {_execution_sentence(action, headline_mode=headline_mode)}"
            f" | **Gap / implied move:** {_fmt_val(gap_vs_move, 2)}x"
        )
    main_gate = item.get("main_signal_gate") or (item.get("signal") or {}).get("main_signal_gate")
    if main_gate:
        blockers = main_gate.get("blockers") or []
        blocker_text = ", ".join(blockers[:3]) if blockers else "none"
        lines.append(
            f"**Main signal gate:** {str(main_gate.get('status') or 'unknown').upper()}"
            f" | **Role:** {main_gate.get('role') or 'unknown'}"
            f" | **Intent:** {main_gate.get('action_intent') or 'OBSERVE'}"
            f" | **Blockers:** {blocker_text}"
        )
    if confirmations:
        lines.append(f"**Confirmation quality:** {', '.join(confirmations)}")
    penalties = selection.get("penalties") or []
    if penalties:
        lines.append(f"**Why not higher priority:** {', '.join(_humanize_penalty(p) for p in penalties)}")
    return lines


def _lane_label(lane: str | None) -> str:
    labels = {
        "core": "CORE BOOK",
        "tactical_continuation": "TACTICAL CONTINUATION",
        "event_tape": "TACTICAL EVENT TAPE",
        "appendix": "APPENDIX / RADAR",
    }
    return labels.get(lane or "", "UNASSIGNED")


def _fmt_market_cap(musd) -> str:
    if musd is None:
        return "\u2014"
    musd = float(musd)
    if musd >= 1_000:
        return f"${musd / 1_000:.1f}B"
    return f"${musd:.0f}M"


def _fmt_dollar_volume(value) -> str:
    if value is None:
        return "\u2014"
    value = float(value)
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:.0f}"


def _humanize_penalty(code: str) -> str:
    mapping = {
        "low_price": "price below core floor",
        "low_dollar_volume": "dollar volume below core floor",
        "small_cap": "market cap below core floor",
        "poor_options": "poor options liquidity",
        "weak_secondary_confirmation": "weak secondary confirmation",
        "needs_pullback": "needs pullback after overnight stretch",
        "overnight_stretch": "overnight move already consumed too much edge",
    }
    return mapping.get(code, code.replace("_", " "))


def _execution_label(action: str | None) -> str:
    mapping = {
        "executable_now": "EXECUTABLE",
        "wait_pullback": "WAIT PULLBACK",
        "do_not_chase": "DO NOT CHASE",
    }
    return mapping.get(action or "", "NEUTRAL")


def _execution_sentence(action: str | None, *, headline_mode: str = "unknown") -> str:
    if headline_mode != "trend":
        return "non-trend gate: observation only; do not convert this into an order"
    mapping = {
        "executable_now": "still actionable at current levels",
        "wait_pullback": "conditional only; do not enter at the current gap, wait for pullback",
        "do_not_chase": "edge looks spent after the overnight stretch; stand down here",
    }
    return mapping.get(action or "", "no execution read")
