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

import json
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
    _annotate_alpha_policy_context(bundle, output_path)

    lines: list[str] = []

    lines += render_header_and_context(bundle)
    lines += render_report_postmortem(bundle)
    lines += render_scorecard(bundle)
    lines += render_alpha_bulletin(bundle, output_path)
    lines += render_my_book_overlay(bundle, output_path)
    lines += render_strategy_ev_guidance(bundle)
    lines += render_portfolio_risk(bundle)
    lines += render_shared_catalysts(bundle)
    lines += render_options_extremes(bundle)
    lines += render_action_plan_summary(bundle)
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

    candidates = _alpha_bulletin_paths(output_path, trade_date, "alpha_bulletin_us.md")
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


def _alpha_bulletin_paths(output_path: Path, trade_date: str, filename: str) -> list[Path]:
    project_root = Path(__file__).resolve().parents[3]
    stack_root = project_root.parent
    return [
        output_path.parent / "review_dashboard" / "strategy_backtest" / trade_date / filename,
        Path("reports") / "review_dashboard" / "strategy_backtest" / trade_date / filename,
        project_root / "reports" / "review_dashboard" / "strategy_backtest" / trade_date / filename,
        stack_root / "reports" / "review_dashboard" / "strategy_backtest" / trade_date / filename,
    ]


def render_my_book_overlay(bundle: dict, output_path: Path) -> list[str]:
    """Include the user's portfolio permission overlay when available."""
    trade_date = str((bundle.get("meta") or {}).get("trade_date") or "").strip()
    if not trade_date:
        return []

    candidates = _review_dashboard_paths(output_path, trade_date, "my_book_overlay", "my_book_overlay_us.md")
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
        return [text, ""]
    return []


def _review_dashboard_paths(output_path: Path, trade_date: str, folder: str, filename: str) -> list[Path]:
    project_root = Path(__file__).resolve().parents[3]
    stack_root = project_root.parent
    return [
        output_path.parent / "review_dashboard" / folder / trade_date / filename,
        Path("reports") / "review_dashboard" / folder / trade_date / filename,
        project_root / "reports" / "review_dashboard" / folder / trade_date / filename,
        stack_root / "reports" / "review_dashboard" / folder / trade_date / filename,
    ]


def _load_alpha_bulletin_json(bundle: dict, output_path: Path) -> dict[str, Any] | None:
    trade_date = str((bundle.get("meta") or {}).get("trade_date") or "").strip()
    if not trade_date:
        return None
    for path in _alpha_bulletin_paths(output_path, trade_date, "alpha_bulletin.json"):
        if not path.exists():
            continue
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(loaded, dict):
            return loaded
    return None


def _annotate_alpha_policy_context(bundle: dict, output_path: Path) -> None:
    bulletin = _load_alpha_bulletin_json(bundle, output_path)
    if not bulletin:
        return
    bundle["_alpha_bulletin"] = bulletin
    ev_status = str((bulletin.get("ev_status") or {}).get("us") or "unknown")
    selected_policy = (bulletin.get("selected_policies") or {}).get("us")
    research_policy = (bulletin.get("research_policies") or {}).get("us")
    metrics_by_policy = {
        str(row.get("policy_id")): row
        for row in ((bulletin.get("stability") or {}).get("us") or [])
        if row.get("policy_id")
    }
    status_priority = {
        "execution_alpha": 50,
        "tactical_alpha": 40,
        "positive_ev_recall": 35,
        "recall_alpha": 25,
        "blocked_alpha": 10,
    }
    by_symbol: dict[str, dict[str, Any]] = {}

    for section in ["blocked_alpha", "recall_alpha", "tactical_alpha", "execution_alpha"]:
        for row in bulletin.get(section, []) or []:
            if row.get("market") != "us":
                continue
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            reason = str(row.get("reason") or "")
            status = "positive_ev_recall" if section == "recall_alpha" and "positive-EV research policy" in reason else section
            context = {
                "status": status,
                "section": section,
                "ev_status": ev_status,
                "selected_policy": selected_policy,
                "research_policy": research_policy,
                "policy_id": row.get("policy_id"),
                "policy_label": row.get("policy_label"),
                "reason": reason,
                "blockers": row.get("blockers") or [],
            }
            context["policy_metrics"] = metrics_by_policy.get(str(row.get("policy_id") or ""))
            current = by_symbol.get(symbol)
            if not current or status_priority.get(status, 0) > status_priority.get(str(current.get("status")), 0):
                by_symbol[symbol] = context

    default_context = {
        "status": "unclassified",
        "section": None,
        "ev_status": ev_status,
        "selected_policy": selected_policy,
        "research_policy": research_policy,
        "policy_id": None,
        "policy_label": None,
        "reason": "not present in stable-alpha bulletin",
        "blockers": [],
        "policy_metrics": None,
    }
    for item in bundle.get("notable_items") or []:
        symbol = str(item.get("symbol") or "").upper()
        item["stable_alpha_context"] = by_symbol.get(symbol, default_context.copy())


def render_strategy_ev_guidance(bundle: dict) -> list[str]:
    """Show policy EV evidence so HIGH/MODERATE strength cannot masquerade as EV."""
    bulletin = bundle.get("_alpha_bulletin")
    if not isinstance(bulletin, dict):
        return []
    rows = ((bulletin.get("stability") or {}).get("us") or [])
    if not rows:
        return []

    selected_policy = (bulletin.get("selected_policies") or {}).get("us")
    research_policy = (bulletin.get("research_policies") or {}).get("us")
    ev_status = str((bulletin.get("ev_status") or {}).get("us") or "unknown")
    selected_rows: list[dict[str, Any]] = []
    for role, predicate in [
        ("selected champion", lambda row: row.get("policy_id") == selected_policy),
        ("positive-EV recall", lambda row: row.get("policy_id") == research_policy),
        ("profit trend low", lambda row: row.get("policy_id") == "us:core:long:low:executable_now:trending:h3"),
        ("profit trend high/mod", lambda row: row.get("policy_id") == "us:core:long:high_mod:executable_now:trending:h3"),
        ("legacy non-trend core", lambda row: str(row.get("policy_id") or "").startswith("us:core:long:high_mod:executable_now:") and ":trending:" not in str(row.get("policy_id") or "")),
    ]:
        for row in rows:
            if predicate(row):
                selected_rows.append({**row, "_role": role})
                break
    seen = {row.get("policy_id") for row in selected_rows}
    for row in sorted(rows, key=lambda r: float(r.get("stability_score") or 0.0), reverse=True):
        if row.get("policy_id") in seen:
            continue
        selected_rows.append({**row, "_role": "top challenger"})
        seen.add(row.get("policy_id"))
        if len(selected_rows) >= 5:
            break

    if not selected_rows:
        return []

    lines = [
        "## Strategy EV Guidance",
        "",
        "This section is computed from the stable-alpha gate before narrative writing. Signal confidence is recall strength; policy EV decides whether a strategy family may be treated as execution-grade.",
        "",
        f"- Stable gate status: `{ev_status}`",
        f"- Selected execution policy: `{selected_policy or 'none'}`",
        f"- Positive-EV research policy: `{research_policy or 'none'}`",
        "- Report rule: the US money policy is core + long + executable + trending, selected by rolling EV evidence. LOW/HIGH/MODERATE labels alone never create a trade, and options remain shadow-only until the option ledger has positive LCB80.",
        "",
        "| Role | Policy | Fills | Avg trade | EV LCB | Win | Max DD | Top1 contrib | Report action |",
        "|------|--------|------:|----------:|-------:|----:|-------:|-------------:|---------------|",
    ]
    for row in selected_rows:
        action = _policy_report_action(row, selected_policy, research_policy)
        lines.append(
            "| {role} | `{policy}` | {fills} | {avg} | {lcb} | {win} | {dd} | {top1} | {action} |".format(
                role=row.get("_role") or "-",
                policy=row.get("policy_id") or "-",
                fills=int(row.get("fills") or 0),
                avg=_fmt_pct(row.get("avg_trade_pct")),
                lcb=_fmt_pct(row.get("ev_lower_confidence_pct")),
                win=_fmt_pct((_to_float(row.get("strict_win_rate"), None) or 0.0) * 100.0 if row.get("strict_win_rate") is not None else None),
                dd=_fmt_pct(row.get("max_drawdown_pct")),
                top1=_fmt_pct((_to_float(row.get("top1_winner_contribution"), None) or 0.0) * 100.0 if row.get("top1_winner_contribution") is not None else None),
                action=action,
            )
        )
    lines += ["", "---", ""]
    return lines


def _policy_report_action(row: dict[str, Any], selected_policy: str | None, research_policy: str | None) -> str:
    policy_id = row.get("policy_id")
    if selected_policy and policy_id == selected_policy and row.get("eligible"):
        return "Execution Alpha allowed for matching current candidates"
    if research_policy and policy_id == research_policy:
        return "Positive EV Setup / Recall only; needs promotion before execution"
    reasons = row.get("fail_reasons") or []
    if "avg_trade_pct<=0.4" in reasons or "ev_not_positive_enough" in reasons:
        return "Do not promote; historical EV is weak"
    if reasons:
        return "Research only: " + ", ".join(str(x) for x in reasons[:3])
    return "Research only"


def render_action_plan_summary(bundle: dict) -> list[str]:
    """Render a deterministic plan ledger so narrative agents cannot hide the plan."""
    items = bundle.get("notable_items") or []
    if not items:
        return []

    views = [_action_plan_view(item) for item in items]
    gate_pass = [v for v in views if v["state"] == "gate_pass_plan"]
    setup = [v for v in views if v["state"] == "setup_wait_plan"]
    blocked = [v for v in views if v["state"] == "blocked_no_chase"]

    lines: list[str] = [
        "## Action Plan Ledger",
        "",
        "This section is computed before narrative writing. It is the fresh-entry ticket ledger, not a portfolio liquidation instruction. No ticket means no new trade; existing profitable positions must be handled by My Book / Winner Hold Overlay, not killed by the fresh-entry gate.",
        "",
        "| State | Count | Report use |",
        "|-------|------:|------------|",
        f"| Fresh-entry ticket | {len(gate_pass)} | May be discussed as a new-trade ticket only if stable-alpha gate also supports it. |",
        f"| Positive EV / setup ticket | {len(setup)} | Review, 0.10R stock probe, pullback, or second-day confirmation only; no options money. |",
        f"| Blocked / no-ticket | {len(blocked)} | No new buy and no add. If already held, route to Winner Hold Overlay instead of automatic full exit. |",
        "",
    ]
    lines += _render_action_plan_table(
        "Fresh Entry Tickets",
        gate_pass,
        empty="No current notable item has a fresh-entry ticket with stable-alpha support and a complete price plan.",
        limit=6,
    )
    lines += _render_action_plan_table(
        "Positive EV / Setup Tickets",
        setup,
        empty="No setup/wait plans in the current notable set.",
        limit=8,
    )
    lines += _render_action_plan_table(
        "Blocked / No-Ticket",
        blocked,
        empty="No blocked/no-ticket rows in the current notable set.",
        limit=8,
    )
    lines += ["---", ""]
    return lines


def _action_plan_view(item: dict[str, Any]) -> dict[str, Any]:
    signal = item.get("signal") or {}
    risk = item.get("risk_params") or {}
    gate = item.get("execution_gate") or {}
    main_gate = _main_signal_gate(item) or {}
    blockers = main_gate.get("blockers") or []
    action = gate.get("action") or risk.get("execution_mode") or "unknown"
    rr = _to_float(risk.get("rr_ratio"), None)
    entry = _to_float(risk.get("entry"), None) or _to_float(gate.get("pullback_price"), None) or _to_float(item.get("price"), None)
    stop = _to_float(risk.get("stop"), None)
    target = _to_float(risk.get("target"), None)
    expected_move = _to_float(risk.get("expected_move_pct"), None) or _to_float((item.get("options") or {}).get("expected_move_pct"), None)
    gate_status = str(main_gate.get("status") or "").lower()
    gate_role = str(main_gate.get("role") or "").lower()
    bucket = str(item.get("report_bucket") or "").lower()
    confidence = str(signal.get("confidence") or "-")
    priced = _setup_alpha_view(item)
    priced_in = _to_float(priced.get("priced_in_score"), 0.0) or 0.0
    alpha_context = item.get("stable_alpha_context") or {}
    alpha_status = str(alpha_context.get("status") or "")
    stable_ev_status = str(alpha_context.get("ev_status") or "")
    policy_metrics = alpha_context.get("policy_metrics") or {}

    hard_blocked = bool(blockers) or gate_status in {"blocked", "fail", "failed"}
    rr_blocked = rr is not None and rr < 1.20
    chase_blocked = action == "do_not_chase" or priced_in >= 0.82
    stable_blocks_execution = (
        bool(alpha_context)
        and stable_ev_status == "failed"
        and alpha_status not in {"execution_alpha", "positive_ev_recall"}
    )
    positive_ev_recall = alpha_status == "positive_ev_recall"

    if (
        alpha_status == "execution_alpha"
        and gate_status == "pass"
        and gate_role == "main_signal"
        and rr is not None
        and rr >= 1.20
        and not chase_blocked
        and action not in {"wait_pullback", "pullback_only"}
    ):
        state = "gate_pass_plan"
        reason = "fresh-entry ticket passed with complete entry/stop/target"
    elif positive_ev_recall and not chase_blocked and not rr_blocked:
        state = "setup_wait_plan"
        reason = _positive_ev_reason(policy_metrics)
    elif hard_blocked or rr_blocked or chase_blocked:
        state = "blocked_no_chase"
        reason = ", ".join(str(x) for x in blockers[:3]) if blockers else (priced.get("reason") or "no execution plan")
        if rr_blocked:
            reason = "R:R below execution floor" if not reason else f"{reason}; R:R below execution floor"
        if chase_blocked:
            reason = "stale chase / already paid" if not reason else f"{reason}; stale chase / already paid"
        if positive_ev_recall:
            reason = f"{_positive_ev_reason(policy_metrics)}; {reason}"
        elif stable_blocks_execution:
            reason = "stable EV gate not passed" if not reason else f"{reason}; stable EV gate not passed"
    elif stable_blocks_execution:
        state = "blocked_no_chase"
        reason = "stable EV gate not passed"
    elif (
        action in {"wait_pullback", "pullback_only"}
        or priced.get("bucket") in {"early_accumulation", "pullback_reset", "breakout_acceptance", "post_event_second_day"}
    ):
        state = "setup_wait_plan"
        reason = priced.get("reason") or "needs pullback or confirmation"
    else:
        state = "blocked_no_chase"
        reason = ", ".join(str(x) for x in blockers[:3]) if blockers else (priced.get("reason") or "no execution plan")

    return {
        "symbol": item.get("symbol") or "-",
        "company_name": (item.get("fundamentals") or {}).get("company_name") or "",
        "state": state,
        "lane": bucket or "-",
        "direction": signal.get("direction") or "-",
        "confidence": confidence,
        "entry": entry,
        "stop": stop,
        "target": target,
        "rr": rr,
        "expected_move": expected_move,
        "priced_in": priced_in,
        "alpha_rank": _alpha_context_rank(alpha_context),
        "action": action,
        "time_exit": "3 sessions / next catalyst",
        "reason": reason or "-",
        "score": _to_float(item.get("selection_rank_score")) or _to_float(item.get("report_score")) or _to_float(item.get("score"), 0.0) or 0.0,
    }


def _render_action_plan_table(
    title: str,
    rows: list[dict[str, Any]],
    *,
    empty: str,
    limit: int,
) -> list[str]:
    lines = [f"### {title}", ""]
    if not rows:
        return lines + [f"- {empty}", ""]
    ordered = sorted(
        rows,
        key=lambda row: (
            -float(row.get("alpha_rank") or 0.0),
            -float(row.get("score") or 0.0),
            float(row.get("priced_in") or 0.0),
            str(row.get("symbol") or ""),
        ),
    )[:limit]
    lines += [
        "| Symbol / Company | Direction | Confidence | Entry / Review | Stop / Invalid | Target | R:R | Exp move | Time exit | State reason |",
        "|------------------|-----------|------------|----------------|----------------|--------|-----|----------|-----------|--------------|",
    ]
    for row in ordered:
        lines.append(
            "| {symbol} | {direction} | {confidence} | {entry} | {stop} | {target} | {rr} | {expected_move} | {time_exit} | {reason} |".format(
                symbol=_symbol_company_label(row),
                direction=str(row.get("direction") or "-"),
                confidence=str(row.get("confidence") or "-"),
                entry=_fmt_price(row.get("entry")),
                stop=_fmt_price(row.get("stop")),
                target=_fmt_price(row.get("target")),
                rr=_fmt_val(row.get("rr"), 2),
                expected_move=_fmt_pct(row.get("expected_move")),
                time_exit=row.get("time_exit") or "-",
                reason=row.get("reason") or "-",
            )
        )
    lines.append("")
    return lines


def _symbol_company_label(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "-")
    company = str(row.get("company_name") or "").strip()
    return f"{symbol} / {company}" if company else symbol


def _positive_ev_reason(policy_metrics: dict[str, Any]) -> str:
    if not policy_metrics:
        return "positive-EV recall policy; review only"
    return (
        "positive-EV recall policy "
        f"(fills={int(policy_metrics.get('fills') or 0)}, "
        f"avg={_fmt_pct(policy_metrics.get('avg_trade_pct'))}, "
        f"EV LCB={_fmt_pct(policy_metrics.get('ev_lower_confidence_pct'))}); review only"
    )


def _alpha_context_rank(context: dict[str, Any]) -> int:
    status = str((context or {}).get("status") or "")
    return {
        "execution_alpha": 5,
        "positive_ev_recall": 4,
        "tactical_alpha": 3,
        "recall_alpha": 2,
        "blocked_alpha": 1,
    }.get(status, 0)


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
        "**Rules:** Fresh entry still requires a valid ticket, execution gate, R:R, and Strategy EV support. Headline Gate is context only; anti-chase is an execution constraint. Price extension is allowed only when trend/event/options confirmation pays for it. A blocked fresh-entry ticket is not an automatic sell signal for an existing winner; route that case through My Book / Winner Hold Overlay.",
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
    parsed = _to_float(value, None)
    if parsed is None:
        return "-"
    return f"{parsed:+.1f}%"


def _fmt_val(value: Any, decimals: int = 2) -> str:
    parsed = _to_float(value, None)
    if parsed is None:
        return "-"
    return f"{parsed:.{decimals}f}"


def _fmt_x(value: Any) -> str:
    parsed = _to_float(value, None)
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
            "Fresh-entry eligibility below is controlled by ticket, execution, R:R, Strategy EV, and anti-chase gates. Existing winners require a separate hold/trim/exit decision.",
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
    """Sort items within a report lane by EV/report status before raw confidence."""
    confidence_rank = {"HIGH": 0, "MODERATE": 1, "LOW": 2, "NO_SIGNAL": 3, None: 3}
    alpha_rank = {
        "execution_alpha": 0,
        "positive_ev_recall": 1,
        "tactical_alpha": 2,
        "recall_alpha": 3,
        "blocked_alpha": 5,
        "unclassified": 4,
        None: 4,
    }

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
            alpha_rank.get((item.get("stable_alpha_context") or {}).get("status"), 4),
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
    alpha_context = item.get("stable_alpha_context") or {}
    stable_status = alpha_context.get("status") or "unclassified"
    stable_reason = alpha_context.get("reason") or "not present in stable-alpha bulletin"
    lines = [
        "**Execution guard:**",
        "",
        f"- Main Signal Gate `{status}`: this item is a review candidate, not an order surface.",
        f"- Stable Alpha context `{stable_status}`: {stable_reason}.",
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
