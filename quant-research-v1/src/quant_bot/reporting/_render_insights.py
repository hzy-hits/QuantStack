"""Render Phase 4 algorithmic insights: scorecard, portfolio risk, contradictions, shared catalysts."""
from __future__ import annotations

from typing import Any

from ._render_fmt import _fmt_pct, _fmt_val, _plural


def render_report_postmortem(bundle: dict) -> list[str]:
    """Render the execution-aware report review loop."""
    review = bundle.get("report_review")
    if not review or not review.get("selected_reviewed"):
        return []

    selected_counts = review.get("selected_counts", {})
    ignored_counts = review.get("ignored_counts", {})
    feedback_counts = review.get("factor_feedback_counts", {})

    lines = [
        "## Report Postmortem (last 20 trading days)",
        "",
        "*Did the report publish while alpha was still available, or after the move was already paid? Which ignored names kept running anyway?*",
        "",
        f"- **Primary issue:** {review.get('primary_issue_label') or 'N/A'}"
        + (
            f" | secondary: {review.get('secondary_issue_label')}"
            if review.get("secondary_issue_label")
            else ""
        ),
        f"- **Verdict:** {review.get('verdict') or 'N/A'}",
        f"- **Implication for today:** {review.get('today_implication') or 'N/A'}",
        "",
        "| Quick read | Value |",
        "|------------|-------|",
        f"| Capture rate | {_fmt_val(review.get('capture_rate'), 2) if review.get('capture_rate') is not None else 'N/A'} |",
        f"| Ignored-alpha rate | {_fmt_val(review.get('ignored_alpha_rate'), 2) if review.get('ignored_alpha_rate') is not None else 'N/A'} |",
        f"| Stale-chase rate | {_fmt_val(review.get('stale_chase_rate'), 2) if review.get('stale_chase_rate') is not None else 'N/A'} |",
        f"| False-positive rate | {_fmt_val(review.get('false_positive_rate'), 2) if review.get('false_positive_rate') is not None else 'N/A'} |",
        f"| Flat-edge rate | {_fmt_val(review.get('flat_edge_rate'), 2) if review.get('flat_edge_rate') is not None else 'N/A'} |",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Selected ideas reviewed | {review.get('selected_reviewed', 0)} |",
        f"| Ignored candidates reviewed | {review.get('ignored_reviewed', 0)} |",
        f"| Avg move already consumed at next open | {_fmt_val(review.get('avg_move_consumed_ratio'), 3)}x expected move |",
        f"| Avg remaining move to target | {_fmt_pct(review.get('avg_alpha_remaining_pct'))} |",
        "",
        "**Selected ideas:**",
        "",
        "| Captured | Already paid | Good signal, bad timing | False positive | Flat / mixed |",
        "|----------|--------------|-------------------------|----------------|--------------|",
        f"| {selected_counts.get('captured', 0)} | {selected_counts.get('alpha_already_paid', 0)} | {selected_counts.get('good_signal_bad_timing', 0)} | {selected_counts.get('false_positive', 0)} | {selected_counts.get('flat_edge', 0)} |",
        "",
        "**Ignored candidates:**",
        "",
        "| Missed alpha | Ignored OK |",
        "|--------------|------------|",
        f"| {ignored_counts.get('missed_alpha', 0)} | {ignored_counts.get('ignored_ok', 0)} |",
        "",
    ]

    if feedback_counts:
        lines += [
            "**Factor Lab feedback hooks:**",
            "",
            "| Action | Count |",
            "|--------|-------|",
        ]
        safe_feedback_counts = {
            str(action): count
            for action, count in feedback_counts.items()
            if action not in (None, "")
        }
        for action, count in sorted(safe_feedback_counts.items(), key=lambda kv: kv[0]):
            lines.append(f"| {action} | {count} |")
        lines.append("")

    recent_stale = review.get("recent_stale", [])
    if recent_stale:
        lines += [
            "**Recent timing failures:**",
            "",
            "| Date | Symbol | Execution | Move consumed | 3D follow-through |",
            "|------|--------|-----------|---------------|------------------|",
        ]
        for row in recent_stale[:3]:
            lines.append(
                f"| {row['date']} | {row['symbol']} | {row.get('execution_mode') or '—'} | "
                f"{_fmt_val(row.get('move_consumed_ratio'), 3)}x | {_fmt_pct(row.get('best_ret_pct'))} |"
            )
        lines.append("")

    recent_missed = review.get("recent_missed", [])
    if recent_missed:
        lines += [
            "**Recent ignored names that kept running:**",
            "",
            "| Date | Symbol | Confidence | 3D follow-through | Feedback |",
            "|------|--------|------------|------------------|----------|",
        ]
        for row in recent_missed[:3]:
            lines.append(
                f"| {row['date']} | {row['symbol']} | {row.get('confidence') or '—'} | "
                f"{_fmt_pct(row.get('best_ret_pct'))} | {row.get('feedback_action') or '—'} |"
            )
        lines.append("")

    recent_captured = review.get("recent_captured", [])
    if recent_captured:
        lines += [
            "**Recent captured names:**",
            "",
            "| Date | Symbol | Confidence | 3D follow-through |",
            "|------|--------|------------|------------------|",
        ]
        for row in recent_captured[:3]:
            lines.append(
                f"| {row['date']} | {row['symbol']} | {row.get('confidence') or '—'} | "
                f"{_fmt_pct(row.get('best_ret_pct'))} |"
            )
        lines.append("")

    lines += ["---", ""]
    return lines


def render_scorecard(bundle: dict) -> list[str]:
    """Render the algorithm scorecard section."""
    scorecard = bundle.get("scorecard")
    if not scorecard:
        return []

    mom = scorecard.get("momentum_accuracy", {})
    if not mom.get("calls"):
        return []

    lines = [
        "## Algorithm Scorecard (last 20 trading days)",
        "",
        "*How well did the algorithm's directional predictions match actual 5D returns?*",
        "",
    ]

    # Overall momentum accuracy
    lines += [
        "### Momentum Signal Accuracy",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total calls | {mom['calls']} |",
        f"| Correct (direction match) | {mom['correct']} |",
        f"| Accuracy | {_fmt_val(mom.get('accuracy'), 3) if mom.get('accuracy') is not None else 'N/A'} |",
        "",
    ]

    # By confidence bucket
    by_conf = scorecard.get("by_confidence", [])
    if by_conf:
        lines += [
            "**By strength bucket:**",
            "",
            "| Bucket | Calls | Correct | Accuracy |",
            "|--------|-------|---------|----------|",
        ]
        for bc in by_conf:
            acc = _fmt_val(bc.get("accuracy"), 3) if bc.get("accuracy") is not None else "N/A"
            lines.append(f"| {bc['bucket']} | {bc['calls']} | {bc['correct']} | {acc} |")
        lines.append("")

    # Cointegration accuracy
    coint = scorecard.get("cointegration_accuracy", {})
    if coint.get("calls"):
        lines += [
            "### Cointegration Spread Reversion Accuracy",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Spread reversion calls | {coint['calls']} |",
            f"| Correct | {coint['correct']} |",
            f"| Accuracy | {_fmt_val(coint.get('accuracy'), 3) if coint.get('accuracy') is not None else 'N/A'} |",
            "",
        ]

    # Recent misses
    misses = scorecard.get("recent_misses", [])
    if misses:
        lines += [
            "### Recent Misses (learn from errors)",
            "",
            "| Date | Symbol | Predicted | Actual 5D | P(upside) |",
            "|------|--------|-----------|-----------|-----------|",
        ]
        for m in misses[:5]:
            lines.append(
                f"| {m['date']} | {m['symbol']} | {m['predicted']} | "
                f"{_fmt_pct(m.get('actual_ret_5d_pct'))} | {_fmt_val(m.get('p_upside'), 3)} |"
            )
        lines.append("")

    lines += ["---", ""]
    return lines


def render_portfolio_risk(bundle: dict) -> list[str]:
    """Render portfolio-level risk summary."""
    pr = bundle.get("portfolio_risk")
    if not pr:
        return []

    lines = [
        "## Portfolio Risk Summary (HIGH signals)",
        "",
    ]

    lines += [
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Net directional tilt | {pr['net_tilt']:+.3f} ({'net long' if pr['net_tilt'] > 0.1 else 'net short' if pr['net_tilt'] < -0.1 else 'balanced'}) |",
        f"| Concentration (Herfindahl) | {_fmt_val(pr.get('herfindahl'), 3)} (1.0=all same sector) |",
        f"| Independent bets | {pr.get('n_independent_bets', 0)} |",
        "",
    ]

    # Sector concentration
    sector_conc = pr.get("sector_concentration", {})
    if sector_conc:
        lines += [
            "**Sector/theme breakdown:**",
            "",
        ]
        for sector, count in sorted(sector_conc.items(), key=lambda x: -x[1]):
            lines.append(f"- {sector}: {count}")
        lines.append("")

    # Natural hedges
    hedges = pr.get("natural_hedges", [])
    if hedges:
        lines += [
            "**Natural hedges found** (opposing directions in correlated cluster):",
            "",
        ]
        for h in hedges:
            lines.append(f"- Long {h['long']} vs Short {h['short']} (cluster {h['cluster_id']})")
        lines.append("")

    lines += ["---", ""]
    return lines


def render_shared_catalysts(bundle: dict) -> list[str]:
    """Render shared catalyst warnings."""
    catalysts = bundle.get("shared_catalysts", [])
    if not catalysts:
        return []

    lines = [
        "## Shared Catalysts",
        "",
        "*These news items appear across 3+ symbols -- they are market-level events, not idiosyncratic.*",
        "",
    ]
    for c in catalysts:
        syms = ", ".join(c["symbols"])
        headline = c["headline"][:100]
        lines.append(f"- **{headline}** -- affects: {syms}")
    lines += ["", "---", ""]
    return lines


def render_item_contradictions(item: dict) -> list[str]:
    """Render contradiction analysis for a single item. Called from item rendering."""
    contra = item.get("contradictions")
    if not contra:
        return []

    sources = contra.get("sources", {})
    pattern = contra.get("pattern", "unknown")
    modifier = contra.get("conviction_modifier", 1.0)
    contradiction_list = contra.get("contradictions", [])

    lines: list[str] = []

    # Only show if there's something interesting
    if pattern == "no_signal" and not contradiction_list:
        return []

    lines.append("**Contradiction Analysis:**")
    lines.append("")

    # Source direction summary
    src_parts = []
    for key, val in sources.items():
        label = key.replace("_", " ")
        src_parts.append(f"{label}={val}")
    lines.append(f"Sources: {' | '.join(src_parts)}")

    lines.append(f"Pattern: **{pattern}** | Conviction modifier: {modifier:.2f}")

    if contradiction_list:
        for c in contradiction_list:
            lines.append(f"- WARNING: {c}")

    lines.append("")
    return lines


def render_item_risk_params(item: dict) -> list[str]:
    """Render risk parameters for a single item. Called from item rendering."""
    rp = item.get("risk_params")
    if not rp:
        return []

    lines = [
        "**Risk Parameters (algorithmic):**",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Entry | ${_fmt_val(rp.get('entry'), 2)} |",
        f"| Stop (2-ATR) | ${_fmt_val(rp.get('stop'), 2)} |",
        f"| Target | ${_fmt_val(rp.get('target'), 2)} |",
        f"| R:R Ratio | {_fmt_val(rp.get('rr_ratio'), 2)} |",
        f"| Expected move | {_fmt_pct(rp.get('expected_move_pct'))} |",
        f"| Execution plan | {_execution_plan_phrase(rp.get('execution_mode'))} |",
    ]
    if rp.get("reference_price") is not None:
        lines.append(f"| Reference price | ${_fmt_val(rp.get('reference_price'), 2)} |")
    if rp.get("gap_pct") is not None:
        lines.append(f"| Ref gap vs close | {_fmt_pct(rp.get('gap_pct'))} |")
    if rp.get("half_life"):
        lines.append(f"| Mean-reversion half-life | {_fmt_val(rp['half_life'], 1)} days |")
    lines += [""]

    return lines


def _execution_plan_phrase(mode: str | None) -> str:
    mapping = {
        "executable_now": "Entry can still be framed near current levels",
        "wait_pullback": "Use pullback levels rather than chasing the gap",
        "do_not_chase": "Treat the move as stretched; stand down unless price resets",
    }
    return mapping.get(mode or "", "No execution plan")


def render_item_news_quality(item: dict) -> list[str]:
    """Render news quality metadata for a single item."""
    nq = item.get("news_quality")
    if not nq or nq.get("count", 0) == 0:
        return []

    lines: list[str] = []
    if nq.get("deduped_count", 0) < nq.get("count", 0):
        lines.append(
            f"*News: {nq['count']} items, {nq['deduped_count']} unique after dedup. "
            f"Avg freshness: {_fmt_val(nq.get('avg_freshness'), 2)}*"
        )
    elif nq.get("avg_freshness") is not None:
        lines.append(
            f"*News: {nq['count']} items. "
            f"Avg freshness: {_fmt_val(nq.get('avg_freshness'), 2)}*"
        )
    if nq.get("has_shared_catalyst"):
        lines.append("*Some news is shared across multiple symbols (market-level catalyst).*")
    if lines:
        lines.append("")
    return lines
