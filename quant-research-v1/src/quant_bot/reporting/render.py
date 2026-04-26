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
    meta = bundle.setdefault("meta", {})
    if not meta.get("session"):
        stem = output_path.stem.lower()
        if stem.endswith("_post"):
            meta["session"] = "post"
            meta.setdefault("session_label", "post-market")
        elif stem.endswith("_pre"):
            meta["session"] = "pre"
            meta.setdefault("session_label", "pre-market")

    lines: list[str] = []

    lines += render_header_and_context(bundle)
    lines += render_report_postmortem(bundle)
    lines += render_scorecard(bundle)
    lines += render_alpha_bulletin(bundle, output_path)
    lines += render_portfolio_risk(bundle)
    lines += render_shared_catalysts(bundle)
    lines += render_options_extremes(bundle)
    lines += render_setup_alpha_summary(bundle)
    lines += _render_notable_items(bundle)
    lines += render_dividend_screen(bundle)
    lines += render_universe_summary(bundle)
    lines += render_coverage(bundle)
    lines += render_charts(chart_paths, output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def render_alpha_bulletin(bundle: dict, output_path: Path) -> list[str]:
    """Include the daily stable-alpha bulletin when the gate has emitted it."""
    trade_date = str((bundle.get("meta") or {}).get("trade_date") or "").strip()
    if not trade_date:
        return []

    candidates = [
        output_path.parent / "review_dashboard" / "strategy_backtest" / trade_date / "alpha_bulletin_us.md",
        Path("reports") / "review_dashboard" / "strategy_backtest" / trade_date / "alpha_bulletin_us.md",
    ]
    project_root = Path(__file__).resolve().parents[3]
    stack_root = project_root.parent
    candidates.extend(
        [
            project_root / "reports" / "review_dashboard" / "strategy_backtest" / trade_date / "alpha_bulletin_us.md",
            stack_root / "reports" / "review_dashboard" / "strategy_backtest" / trade_date / "alpha_bulletin_us.md",
        ]
    )

    seen: set[Path] = set()
    for path in candidates:
        normalized = path.resolve() if path.exists() else path
        if normalized in seen:
            continue
        seen.add(normalized)
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        return [text, "", "---", ""]
    return []


def render_setup_alpha_summary(bundle: dict) -> list[str]:
    """Summarize early/pullback setups separately from stale chase risk."""
    items = bundle.get("notable_items") or []
    if not items:
        return []

    views = [_setup_alpha_view(item) for item in items]
    groups = {
        "early_accumulation": [v for v in views if v["bucket"] == "early_accumulation"],
        "pullback_reset": [v for v in views if v["bucket"] == "pullback_reset"],
        "breakout_acceptance": [v for v in views if v["bucket"] == "breakout_acceptance"],
        "post_event_second_day": [v for v in views if v["bucket"] == "post_event_second_day"],
        "blocked_chase": [v for v in views if v["bucket"] == "blocked_chase"],
    }

    lines: list[str] = [
        "## Setup Alpha / Anti-Chase",
        "",
        "This section is computed before narrative writing. It separates not-yet-overheated setups from names where the move already consumed too much expected value.",
        "",
        "| Bucket | Count | Use in report |",
        "|--------|------:|---------------|",
        f"| Early accumulation | {len(groups['early_accumulation'])} | Setup Alpha candidate; needs confirmation, not a chase. |",
        f"| Pullback / reset | {len(groups['pullback_reset'])} | Wait for the stated pullback/review level before upgrade. |",
        f"| Breakout acceptance | {len(groups['breakout_acceptance'])} | Price moved, but confirmation still supports follow-through; do not treat as stale chase by default. |",
        f"| Post-event second day | {len(groups['post_event_second_day'])} | Event is known; require second-day acceptance instead of day-one chase. |",
        f"| Blocked chase / priced-in | {len(groups['blocked_chase'])} | Do not promote to Execution Alpha unless it resets. |",
        "",
        "**Rules:** Execution Alpha still requires main signal pass + execution gate + R:R. Headline Gate is context only; anti-chase is an execution constraint. Price extension is allowed only when trend/event/options confirmation pays for it.",
        "",
    ]

    lines += _render_setup_alpha_bucket(
        "Early Accumulation",
        groups["early_accumulation"],
        empty="No clean early accumulation setups in the current notable set.",
    )
    lines += _render_setup_alpha_bucket(
        "Pullback / Reset",
        groups["pullback_reset"],
        empty="No pullback/reset candidates with enough support.",
    )
    lines += _render_setup_alpha_bucket(
        "Breakout Acceptance",
        groups["breakout_acceptance"],
        empty="No extended names earned breakout-acceptance status.",
    )
    lines += _render_setup_alpha_bucket(
        "Post-Event Second Day",
        groups["post_event_second_day"],
        empty="No post-event names passed the anti-chase filter.",
    )
    lines += _render_setup_alpha_bucket(
        "Blocked Chase / Priced-In",
        groups["blocked_chase"],
        empty="No names triggered the stale-chase filter.",
        limit=8,
    )
    lines += ["---", ""]
    return lines


def _setup_alpha_view(item: dict[str, Any]) -> dict[str, Any]:
    gate = item.get("execution_gate") or {}
    selection = item.get("selection") or {}
    options = item.get("options") or {}
    sub = item.get("sub_scores") or {}
    signal = item.get("signal") or {}
    risk = item.get("risk_params") or {}

    action = (
        gate.get("action")
        or selection.get("execution_action")
        or risk.get("execution_mode")
        or "unknown"
    )
    ret_1d = _to_float(item.get("ret_1d_pct"))
    ret_5d = _to_float(item.get("ret_5d_pct"))
    ret_21d = _to_float(item.get("ret_21d_pct"))
    expected_move = _to_float(options.get("expected_move_pct")) or _to_float((item.get("momentum") or {}).get("expected_move_pct"))
    move_consumed = abs(ret_1d) / expected_move if expected_move and expected_move > 0 else None
    stretch = _to_float(gate.get("effective_stretch_score"))
    if stretch is None:
        stretch = _to_float(gate.get("stretch_score"), 0.0)
    cone = _to_float(options.get("cone_position_68"))
    from_high = _to_float(item.get("pct_from_52w_high"))
    event_score = _to_float(sub.get("event"), 0.0) or 0.0
    lab_score = _to_float(sub.get("lab_factor"), 0.0) or 0.0
    options_score = _to_float(sub.get("options"), 0.0) or 0.0
    support = _to_float(gate.get("support_score"), 0.0) or 0.0
    support_score = max(event_score, lab_score, options_score, support)

    priced_in_score = _priced_in_score(
        action=action,
        ret_1d=ret_1d,
        ret_5d=ret_5d,
        ret_21d=ret_21d,
        move_consumed=move_consumed,
        stretch=stretch,
        cone=cone,
        from_high=from_high,
        direction=str(signal.get("direction") or "").lower(),
    )
    long_bias = str(signal.get("direction") or "").lower() in {"long", "bullish"}
    not_hot = priced_in_score < 0.62
    breakout_supported = (
        long_bias
        and action not in {"do_not_chase"}
        and support_score >= 0.58
        and (ret_5d >= 8.0 or ret_21d >= 15.0 or (move_consumed is not None and move_consumed >= 0.65))
        and ret_5d <= 18.0
        and ret_21d <= 35.0
        and priced_in_score < 0.82
        and (move_consumed is None or move_consumed <= 1.20)
    )

    if action == "do_not_chase" or priced_in_score >= 0.82:
        bucket = "blocked_chase"
        reason = "move already paid / stale chase risk"
    elif action in {"wait_pullback", "pullback_only"}:
        bucket = "pullback_reset"
        reason = "execution gate requires pullback"
    elif breakout_supported:
        bucket = "breakout_acceptance"
        reason = "extended but confirmation still supports follow-through"
    elif priced_in_score >= 0.72:
        bucket = "blocked_chase"
        reason = "extension lacks enough confirmation"
    elif (
        long_bias
        and not_hot
        and -3.0 <= ret_1d <= 3.5
        and -4.0 <= ret_5d <= 8.0
        and ret_21d <= 15.0
        and max(event_score, lab_score, options_score, support) >= 0.45
    ):
        bucket = "early_accumulation"
        reason = "support building before price stretch"
    elif event_score >= 0.65 and not_hot and abs(ret_1d) <= 8.0:
        bucket = "post_event_second_day"
        reason = "event known; require second-day acceptance"
    else:
        bucket = "other"
        reason = "not a setup-alpha candidate"

    return {
        "symbol": item.get("symbol") or "-",
        "bucket": bucket,
        "reason": reason,
        "lane": item.get("report_bucket") or "-",
        "confidence": signal.get("confidence") or "-",
        "action": action,
        "price": _to_float(item.get("price")),
        "pullback": _to_float(gate.get("pullback_price")) or _to_float(risk.get("entry")),
        "ret_1d": ret_1d,
        "ret_5d": ret_5d,
        "ret_21d": ret_21d,
        "expected_move": expected_move,
        "move_consumed": move_consumed,
        "priced_in_score": priced_in_score,
        "support": support_score,
        "score": _to_float(item.get("selection_rank_score")) or _to_float(item.get("report_score")) or _to_float(item.get("score"), 0.0) or 0.0,
    }


def _render_setup_alpha_bucket(
    title: str,
    rows: list[dict[str, Any]],
    *,
    empty: str,
    limit: int = 5,
) -> list[str]:
    lines = [f"### {title}", ""]
    if not rows:
        return lines + [f"- {empty}", ""]

    ordered = sorted(
        rows,
        key=lambda row: (
            -float(row.get("support") or 0.0),
            float(row.get("priced_in_score") or 0.0),
            -float(row.get("score") or 0.0),
            str(row.get("symbol") or ""),
        ),
    )[:limit]
    lines += [
        "| Symbol | Lane | Exec | 1D | 5D | 21D | Move consumed | Priced-in | Review level | Reason |",
        "|--------|------|------|----|----|-----|---------------|-----------|--------------|--------|",
    ]
    for row in ordered:
        lines.append(
            "| {symbol} | {lane} | {action} | {ret_1d} | {ret_5d} | {ret_21d} | {move_consumed} | {priced_in} | {pullback} | {reason} |".format(
                symbol=row["symbol"],
                lane=_lane_label(row.get("lane")),
                action=row.get("action") or "-",
                ret_1d=_fmt_pct(row.get("ret_1d")),
                ret_5d=_fmt_pct(row.get("ret_5d")),
                ret_21d=_fmt_pct(row.get("ret_21d")),
                move_consumed=_fmt_x(row.get("move_consumed")),
                priced_in=_fmt_val(row.get("priced_in_score"), 2),
                pullback=_fmt_price(row.get("pullback")),
                reason=row.get("reason") or "-",
            )
        )
    lines.append("")
    return lines


def _priced_in_score(
    *,
    action: str,
    ret_1d: float,
    ret_5d: float,
    ret_21d: float,
    move_consumed: float | None,
    stretch: float | None,
    cone: float | None,
    from_high: float | None,
    direction: str,
) -> float:
    score = 0.0
    if action == "do_not_chase":
        score = max(score, 0.90)
    if action in {"wait_pullback", "pullback_only"}:
        score = max(score, 0.48)
    if stretch is not None:
        score = max(score, float(stretch))
    if move_consumed is not None:
        score = max(score, min(abs(float(move_consumed)) / 1.35, 1.0))
    if ret_21d >= 20.0:
        score = max(score, min((ret_21d - 20.0) / 45.0, 1.0))
    if ret_5d >= 12.0:
        score = max(score, min((ret_5d - 12.0) / 24.0, 1.0))
    if from_high is not None and from_high >= -1.0 and ret_21d >= 12.0:
        score = max(score, 0.70)
    if cone is not None:
        if direction in {"long", "bullish"} and cone >= 0.82:
            score = max(score, min((cone - 0.72) / 0.24, 1.0))
        elif direction in {"short", "bearish"} and cone <= 0.18:
            score = max(score, min((0.28 - cone) / 0.24, 1.0))
    return round(max(0.0, min(score, 1.0)), 3)


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_pct(value: Any) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:+.1f}%"


def _fmt_val(value: Any, decimals: int = 2) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.{decimals}f}"


def _fmt_x(value: Any) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.2f}x"


def _lane_label(value: Any) -> str:
    text = str(value or "-").replace("_", " ")
    return text.upper() if text != "-" else "-"


def _render_notable_items(bundle: dict) -> list[str]:
    """Render the full notable-items section with report-lane grouping."""
    all_items = bundle.get("notable_items", [])
    headline_mode = str((bundle.get("headline_gate") or {}).get("mode") or "unknown").lower()
    report_session = str((bundle.get("meta") or {}).get("session") or "").lower()

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
            f"**Headline context:** Headline Gate is `{headline_mode.upper()}`. Treat this as source-quality / regime context, not an execution blocker.",
            "Execution eligibility below is controlled by the main signal and execution gates.",
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
            if report_session:
                item["_report_session"] = report_session
            compact = bucket == "appendix" or item.get("signal", {}).get("confidence") in ("LOW", "NO_SIGNAL", None)
            lines += render_item_header(item_idx, item, compact=compact)
            if not compact:
                main_gate = _main_signal_gate(item)
                if (main_gate or {}).get("status") == "pass":
                    lines += render_item_risk_params(item)
                else:
                    lines += _render_execution_guard(item, headline_mode)
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


def _main_signal_gate(item: dict) -> dict[str, Any] | None:
    gate = item.get("main_signal_gate") or (item.get("signal") or {}).get("main_signal_gate")
    return gate if isinstance(gate, dict) else None


def _render_execution_guard(item: dict, headline_mode: str) -> list[str]:
    """Render execution discipline without order-shaped risk parameters."""
    gate = item.get("execution_gate") or {}
    risk = item.get("risk_params") or {}
    main_gate = _main_signal_gate(item) or {}
    pullback = gate.get("pullback_price") or risk.get("entry")
    action = gate.get("action") or risk.get("execution_mode") or "unknown"
    report_session = str(item.get("_report_session") or "").lower()
    status = str(main_gate.get("status") or "missing").upper()
    role = main_gate.get("role") or "unknown"
    intent = main_gate.get("action_intent") or "OBSERVE"
    blockers = main_gate.get("blockers") or []
    blocker_text = ", ".join(blockers[:3]) if blockers else "none"
    lines = [
        "**Execution guard:**",
        "",
        f"- Main Signal Gate `{status}`: this item is a review candidate, not an order surface.",
        f"- Headline Gate `{headline_mode.upper()}` is advisory context only.",
        f"- Role: {role}; intent: {intent}; blockers: {blocker_text}.",
        (
            "- Execution state: post-market close report; overnight/pre-open gap diagnostics are hidden and must not be used as current execution instructions."
            if report_session == "post"
            else f"- Execution state: {action}; pullback/review reference: {_fmt_price(pullback)}."
        ),
        "- Final report must not print Entry/Stop/Target, 'today only do', or max-chase instructions for this item.",
        "",
    ]
    return lines


def _fmt_price(value: Any) -> str:
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"
