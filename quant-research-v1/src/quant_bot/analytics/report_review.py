"""
Report-decision postmortem loop.

Turns prior report selections into a structured review ledger so the system can
separate:
  - captured alpha
  - stale / already-paid setups
  - false positives
  - missed alpha from ignored candidates

This review data is also tagged with factor-feedback actions so Factor Lab can
use it during factor selection and health review.
"""
from __future__ import annotations

from datetime import date, timedelta
import json
from statistics import mean
from typing import Any

import duckdb
import structlog

from quant_bot.analytics.algorithm_postmortem import (
    build_algorithm_postmortem_summary,
    materialize_algorithm_postmortem,
)

log = structlog.get_logger()


_ISSUE_LABELS = {
    "missed_alpha": "漏掉 alpha",
    "late": "追晚了",
    "wrong": "判断错了",
    "thin_edge": "edge 太薄",
    "capturing": "抓到了 alpha",
    "mixed": "问题混合",
    "insufficient": "样本不足",
}


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _normalize_direction(v: Any) -> str:
    raw = str(v or "").strip().lower()
    if raw in {"long", "bull", "bullish", "up"}:
        return "long"
    if raw in {"short", "bear", "bearish", "down"}:
        return "short"
    return "neutral"


def _direction_sign(direction: Any) -> int:
    norm = _normalize_direction(direction)
    if norm == "long":
        return 1
    if norm == "short":
        return -1
    return 0


def _signed_return(entry: float | None, future: float | None, direction: Any) -> float | None:
    if entry in (None, 0) or future is None:
        return None
    sign = _direction_sign(direction)
    if sign == 0:
        return None
    return round(sign * ((future / entry) - 1.0) * 100.0, 3)


def _score_threshold(expected_move_pct: float | None) -> float:
    if expected_move_pct and expected_move_pct > 0:
        return max(0.75, expected_move_pct * 0.35)
    return 0.75


def _classify_review_issue(
    *,
    selected_reviewed: int,
    ignored_reviewed: int,
    capture_rate: float | None,
    stale_chase_rate: float | None,
    ignored_alpha_rate: float | None,
    false_positive_rate: float | None,
    flat_edge_rate: float | None,
    selected_counts: dict[str, int],
    ignored_counts: dict[str, int],
) -> tuple[str, str | None]:
    total_reviewed = selected_reviewed + ignored_reviewed
    if total_reviewed < 30 or selected_reviewed < 10:
        return "insufficient", None

    capture_rate = capture_rate or 0.0
    stale_chase_rate = stale_chase_rate or 0.0
    ignored_alpha_rate = ignored_alpha_rate or 0.0
    false_positive_rate = false_positive_rate or 0.0
    flat_edge_rate = flat_edge_rate or 0.0

    issue_scores: list[tuple[str, float]] = []
    if ignored_counts.get("missed_alpha", 0) >= 10:
        issue_scores.append(("missed_alpha", ignored_alpha_rate + 0.20))
    if (
        selected_counts.get("alpha_already_paid", 0)
        + selected_counts.get("good_signal_bad_timing", 0)
    ) >= 5:
        issue_scores.append(("late", stale_chase_rate + 0.10))
    if selected_counts.get("false_positive", 0) >= 5:
        issue_scores.append(("wrong", false_positive_rate + 0.12))
    if selected_counts.get("flat_edge", 0) >= 10:
        issue_scores.append(("thin_edge", flat_edge_rate + 0.08))
    if selected_counts.get("captured", 0) >= 5:
        issue_scores.append(("capturing", capture_rate + 0.05))

    if not issue_scores:
        return "mixed", None

    issue_scores.sort(key=lambda item: item[1], reverse=True)
    primary_issue = issue_scores[0][0]
    secondary_issue = None
    if len(issue_scores) > 1 and issue_scores[1][0] != primary_issue:
        secondary_issue = issue_scores[1][0]
    return primary_issue, secondary_issue


def _review_verdict(
    *,
    primary_issue: str,
    secondary_issue: str | None,
    selected_reviewed: int,
    ignored_reviewed: int,
    capture_rate: float | None,
    stale_chase_rate: float | None,
    ignored_alpha_rate: float | None,
    false_positive_rate: float | None,
    flat_edge_rate: float | None,
) -> tuple[str, str]:
    capture_rate = capture_rate or 0.0
    stale_chase_rate = stale_chase_rate or 0.0
    ignored_alpha_rate = ignored_alpha_rate or 0.0
    false_positive_rate = false_positive_rate or 0.0
    flat_edge_rate = flat_edge_rate or 0.0

    if primary_issue == "missed_alpha":
        verdict = (
            "The bigger problem is missing follow-through than calling direction wrong. "
            f"Ignored-alpha rate is about {ignored_alpha_rate:.0%} versus capture rate about {capture_rate:.0%}."
        )
        implication = (
            "Today, be explicit when the system is under-catching continuation: avoid sounding bullish on names "
            "that already moved, and look harder at still-actionable laggards."
        )
    elif primary_issue == "late":
        verdict = (
            "The bigger problem is arriving after the move is already paid. "
            f"Stale-chase rate is about {stale_chase_rate:.0%} while capture rate is about {capture_rate:.0%}."
        )
        implication = (
            "Today, if a name already consumed most of its expected move, call it late rather than actionable."
        )
    elif primary_issue == "wrong":
        verdict = (
            "The bigger problem is signal quality, not timing. "
            f"False-positive rate is about {false_positive_rate:.0%}, which is too high for strong language."
        )
        implication = (
            "Today, downgrade weak evidence and prefer abstain rules over forcing a trade."
        )
    elif primary_issue == "thin_edge":
        verdict = (
            "The bigger problem is thin edge: many selected names are not outright wrong, but the payoff is too flat. "
            f"Flat-edge rate is about {flat_edge_rate:.0%}."
        )
        implication = (
            "Today, insist on remaining alpha and clean R:R. If the setup only looks right on paper, say no trade."
        )
    elif primary_issue == "capturing":
        verdict = (
            "Recent reviewed ideas are capturing more alpha than they are missing. "
            f"Capture rate is about {capture_rate:.0%}, with stale-chase rate about {stale_chase_rate:.0%}."
        )
        implication = (
            "Today, the main job is execution discipline, not rewriting the whole directional model."
        )
    elif primary_issue == "insufficient":
        verdict = (
            f"Only {selected_reviewed + ignored_reviewed} mature outcomes are available, so the review loop is still sample-light."
        )
        implication = (
            "Today, treat the review block as weak evidence and avoid strong claims about whether the system is late or wrong."
        )
    else:
        verdict = (
            "No single failure mode dominates. The book is mixing missed follow-through, thin edge, and timing slippage."
        )
        implication = (
            "Today, write the failure mode explicitly instead of pretending the system has one clean problem."
        )

    if secondary_issue and secondary_issue != primary_issue:
        verdict = (
            f"{verdict} Secondary issue: {_ISSUE_LABELS.get(secondary_issue, secondary_issue)}."
        )

    return verdict, implication


def store_report_decisions(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    session: str,
    ranked_items: list[dict[str, Any]],
    selected_symbols: set[str],
    headline_gate: dict[str, Any] | None = None,
) -> int:
    """Persist the final ranked decision set for later postmortem review."""
    if not ranked_items:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")
    headline_mode = (headline_gate or {}).get("mode")
    con.execute(
        "DELETE FROM alpha_postmortem WHERE report_date = ? AND session = ?",
        [as_of_str, session],
    )
    con.execute(
        "DELETE FROM report_outcomes WHERE report_date = ? AND session = ?",
        [as_of_str, session],
    )
    con.execute(
        "DELETE FROM report_decisions WHERE report_date = ? AND session = ?",
        [as_of_str, session],
    )
    rows: list[list[Any]] = []

    for idx, item in enumerate(ranked_items, start=1):
        symbol = item.get("symbol")
        if not symbol:
            continue

        selection = item.get("selection") or {}
        risk = item.get("risk_params") or {}
        gate = item.get("execution_gate") or {}
        signal = item.get("signal") or {}

        price = _safe_float(item.get("price"))
        entry_price = _safe_float(risk.get("entry")) or price
        reference_price = (
            _safe_float(risk.get("reference_price"))
            or _safe_float(gate.get("ref_price"))
            or price
        )

        details = {
            "sub_scores": item.get("sub_scores"),
            "selection_penalties": selection.get("penalties"),
            "execution_gate": gate,
            "overnight_alpha": item.get("overnight_alpha"),
            "main_signal_gate": item.get("main_signal_gate") or (signal.get("main_signal_gate") if isinstance(signal, dict) else None),
            "headline_gate_reasons": (headline_gate or {}).get("reasons"),
        }

        rows.append([
            as_of_str,
            session,
            symbol,
            "selected" if symbol in selected_symbols else "ignored",
            idx,
            item.get("report_bucket"),
            _normalize_direction(signal.get("direction")),
            signal.get("confidence"),
            headline_mode,
            _safe_float(item.get("score")),
            _safe_float(item.get("report_score")),
            _safe_float(selection.get("tradability_score")),
            selection.get("lane_reason"),
            risk.get("execution_mode") or gate.get("action"),
            entry_price,
            reference_price,
            _safe_float(risk.get("stop")),
            _safe_float(risk.get("target")),
            _safe_float(risk.get("rr_ratio")),
            _safe_float(risk.get("expected_move_pct"))
            or _safe_float((item.get("options") or {}).get("expected_move_pct"))
            or _safe_float((item.get("momentum") or {}).get("expected_move_pct")),
            _safe_float(risk.get("gap_pct")) or _safe_float(gate.get("gap_pct")),
            item.get("primary_reason"),
            json.dumps(details, ensure_ascii=True, default=str),
        ])

    con.executemany(
        """
        INSERT OR REPLACE INTO report_decisions (
            report_date, session, symbol, selection_status, rank_order,
            report_bucket, signal_direction, signal_confidence, headline_mode,
            composite_score, report_score, tradability_score, lane_reason,
            execution_mode, entry_price, reference_price, stop_price,
            target_price, rr_ratio, expected_move_pct, gap_pct,
            primary_reason, details_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    return len(rows)


def compute_report_outcomes(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    lookback_days: int = 20,
) -> int:
    """Resolve recent report decisions into realized next-session outcomes."""
    cutoff = as_of - timedelta(days=lookback_days * 3)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    as_of_str = as_of.strftime("%Y-%m-%d")

    rows = con.execute(
        """
        WITH future_prices AS (
            SELECT
                d.report_date,
                d.session,
                d.symbol,
                d.selection_status,
                p.date AS price_date,
                p.open AS open_price,
                COALESCE(p.close, p.adj_close) AS close_price,
                ROW_NUMBER() OVER (
                    PARTITION BY d.report_date, d.session, d.symbol, d.selection_status
                    ORDER BY p.date
                ) AS future_rn
            FROM report_decisions d
            INNER JOIN prices_daily p
                ON p.symbol = d.symbol
            WHERE d.report_date >= ?
              AND d.report_date <= ?
              AND p.date <= ?
              AND (
                  (d.session = 'pre' AND p.date >= d.report_date)
                  OR (d.session <> 'pre' AND p.date > d.report_date)
              )
        )
        SELECT
            d.report_date,
            d.session,
            d.symbol,
            d.selection_status,
            d.signal_direction,
            d.execution_mode,
            d.entry_price,
            d.reference_price,
            d.target_price,
            d.expected_move_pct,
            MAX(CASE WHEN fp.future_rn = 1 THEN fp.price_date END) AS next_trade_date,
            MAX(CASE WHEN fp.future_rn = 1 THEN fp.open_price END) AS next_open,
            MAX(CASE WHEN fp.future_rn = 1 THEN fp.close_price END) AS next_close,
            MAX(CASE WHEN fp.future_rn = 3 THEN fp.close_price END) AS hold_3d_close
        FROM report_decisions d
        LEFT JOIN future_prices fp
          ON fp.report_date = d.report_date
         AND fp.session = d.session
         AND fp.symbol = d.symbol
         AND fp.selection_status = d.selection_status
        WHERE d.report_date >= ?
          AND d.report_date <= ?
        GROUP BY
            d.report_date, d.session, d.symbol, d.selection_status,
            d.signal_direction, d.execution_mode, d.entry_price,
            d.reference_price, d.target_price, d.expected_move_pct
        """,
        [cutoff_str, as_of_str, as_of_str, cutoff_str, as_of_str],
    ).fetchall()

    if not rows:
        return 0

    inserts: list[list[Any]] = []
    for row in rows:
        (
            report_date,
            session,
            symbol,
            selection_status,
            signal_direction,
            _execution_mode,
            entry_price,
            reference_price,
            target_price,
            expected_move_pct,
            next_trade_date,
            next_open,
            next_close,
            hold_3d_close,
        ) = row

        entry = _safe_float(entry_price) or _safe_float(reference_price)
        ref = _safe_float(reference_price) or entry
        next_open = _safe_float(next_open)
        next_close = _safe_float(next_close)
        hold_3d_close = _safe_float(hold_3d_close)
        target_price = _safe_float(target_price)
        expected_move_pct = _safe_float(expected_move_pct)

        next_open_ret = _signed_return(entry, next_open, signal_direction)
        next_close_ret = _signed_return(entry, next_close, signal_direction)
        hold_3d_ret = _signed_return(entry, hold_3d_close, signal_direction)
        ref_gap_pct = _signed_return(ref, next_open, signal_direction)
        move_consumed_ratio = (
            round(abs(ref_gap_pct) / expected_move_pct, 3)
            if ref_gap_pct is not None and expected_move_pct and expected_move_pct > 0
            else None
        )
        alpha_remaining_pct = _signed_return(next_open, target_price, signal_direction)
        data_ready = bool(next_trade_date and next_open is not None and next_close is not None)

        inserts.append([
            report_date,
            session,
            symbol,
            selection_status,
            as_of_str,
            next_trade_date,
            entry,
            ref,
            next_open,
            next_close,
            hold_3d_close,
            next_open_ret,
            next_close_ret,
            hold_3d_ret,
            ref_gap_pct,
            move_consumed_ratio,
            next_open_ret,
            alpha_remaining_pct,
            data_ready,
        ])

    con.executemany(
        """
        INSERT OR REPLACE INTO report_outcomes (
            report_date, session, symbol, selection_status, evaluation_date,
            next_trade_date, entry_price, reference_price, next_open, next_close,
            hold_3d_close, next_open_ret_pct, next_close_ret_pct, hold_3d_ret_pct,
            ref_gap_pct, move_consumed_ratio, execution_slippage_pct,
            alpha_remaining_pct, data_ready
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        inserts,
    )
    return len(inserts)


def _factor_feedback(
    label: str,
    confidence: str | None,
) -> tuple[str | None, float | None]:
    scale = {
        "HIGH": 1.0,
        "MODERATE": 0.7,
        "LOW": 0.4,
        "NO_SIGNAL": 0.2,
        None: 0.5,
    }.get(confidence, 0.5)

    if label == "missed_alpha":
        return "boost_recall", round(1.0 * scale, 3)
    if label in {"alpha_already_paid", "good_signal_bad_timing"}:
        return "penalize_stale_chase", round(0.9 * scale, 3)
    if label == "false_positive":
        return "penalize_false_positive", round(1.1 * scale, 3)
    if label == "captured":
        return "reward_capture", round(0.5 * scale, 3)
    return None, None


def compute_alpha_postmortem(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    lookback_days: int = 20,
) -> int:
    """Classify report outcomes into review labels and Factor Lab feedback hooks."""
    cutoff = as_of - timedelta(days=lookback_days * 3)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    as_of_str = as_of.strftime("%Y-%m-%d")

    rows = con.execute(
        """
        SELECT
            d.report_date,
            d.session,
            d.symbol,
            d.selection_status,
            d.signal_direction,
            d.signal_confidence,
            d.execution_mode,
            d.expected_move_pct,
            o.evaluation_date,
            o.data_ready,
            o.next_open_ret_pct,
            o.next_close_ret_pct,
            o.hold_3d_ret_pct,
            o.move_consumed_ratio,
            o.execution_slippage_pct,
            o.alpha_remaining_pct
        FROM report_decisions d
        INNER JOIN report_outcomes o
          ON o.report_date = d.report_date
         AND o.session = d.session
         AND o.symbol = d.symbol
         AND o.selection_status = d.selection_status
        WHERE d.report_date >= ?
          AND d.report_date <= ?
        """,
        [cutoff_str, as_of_str],
    ).fetchall()

    if not rows:
        return 0

    inserts: list[list[Any]] = []
    for row in rows:
        (
            report_date,
            session,
            symbol,
            selection_status,
            signal_direction,
            signal_confidence,
            execution_mode,
            expected_move_pct,
            evaluation_date,
            data_ready,
            next_open_ret_pct,
            next_close_ret_pct,
            hold_3d_ret_pct,
            move_consumed_ratio,
            execution_slippage_pct,
            alpha_remaining_pct,
        ) = row

        best_ret = max(
            [x for x in [_safe_float(next_close_ret_pct), _safe_float(hold_3d_ret_pct)] if x is not None],
            default=None,
        )
        threshold = _score_threshold(_safe_float(expected_move_pct))
        stale = (
            execution_mode in {"wait_pullback", "do_not_chase"}
            or (_safe_float(move_consumed_ratio) or 0.0) >= 0.75
        )

        label = "unresolved"
        note = "Outcome not yet mature enough for review."
        if data_ready:
            if selection_status == "ignored":
                if _direction_sign(signal_direction) == 0:
                    label = "ignored_ok"
                    note = "Ignored candidate had no directional signal."
                elif best_ret is not None and best_ret >= threshold:
                    label = "missed_alpha"
                    note = "Ignored candidate still delivered a directional follow-through."
                else:
                    label = "ignored_ok"
                    note = "Ignored candidate did not leave enough realized follow-through."
            else:
                if best_ret is None:
                    label = "flat_edge"
                    note = "Selected idea has not shown enough realized follow-through yet."
                elif stale and best_ret > 0:
                    if execution_mode == "do_not_chase" or (_safe_float(move_consumed_ratio) or 0.0) >= 1.0:
                        label = "alpha_already_paid"
                        note = "The report found the right move, but most of it was already consumed by the open."
                    else:
                        label = "good_signal_bad_timing"
                        note = "Direction was right, but execution timing degraded the edge."
                elif best_ret >= max(0.5, threshold * 0.6):
                    label = "captured"
                    note = "Selected setup kept moving in the called direction after the report."
                elif best_ret <= -max(0.5, threshold * 0.35):
                    label = "false_positive"
                    note = "Selected setup failed to follow through after publication."
                else:
                    label = "flat_edge"
                    note = "Selected idea was directionally mixed or too small to matter."

        feedback_action, feedback_weight = _factor_feedback(label, signal_confidence)
        inserts.append([
            report_date,
            session,
            symbol,
            selection_status,
            evaluation_date or as_of_str,
            label,
            note,
            feedback_action,
            feedback_weight,
            best_ret,
            _safe_float(move_consumed_ratio),
            _safe_float(execution_slippage_pct),
            _safe_float(alpha_remaining_pct),
        ])

    con.executemany(
        """
        INSERT OR REPLACE INTO alpha_postmortem (
            report_date, session, symbol, selection_status, evaluation_date, label,
            review_note, factor_feedback_action, factor_feedback_weight, best_ret_pct,
            move_consumed_ratio, execution_slippage_pct, alpha_remaining_pct
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        inserts,
    )
    return len(inserts)


def build_report_review(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    session: str,
    lookback_days: int = 20,
) -> dict[str, Any]:
    """Build render-ready review summary for the current session."""
    cutoff = as_of - timedelta(days=lookback_days * 3)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    as_of_str = as_of.strftime("%Y-%m-%d")

    rows = con.execute(
        """
        SELECT
            p.report_date,
            p.symbol,
            p.selection_status,
            p.label,
            p.review_note,
            p.factor_feedback_action,
            p.factor_feedback_weight,
            d.signal_confidence,
            d.execution_mode,
            o.move_consumed_ratio,
            o.execution_slippage_pct,
            o.alpha_remaining_pct,
            o.next_open_ret_pct,
            o.next_close_ret_pct,
            o.hold_3d_ret_pct
        FROM alpha_postmortem p
        INNER JOIN report_decisions d
          ON d.report_date = p.report_date
         AND d.session = p.session
         AND d.symbol = p.symbol
         AND d.selection_status = p.selection_status
        INNER JOIN report_outcomes o
          ON o.report_date = p.report_date
         AND o.session = p.session
         AND o.symbol = p.symbol
         AND o.selection_status = p.selection_status
        WHERE p.session = ?
          AND p.report_date >= ?
          AND p.report_date <= ?
          AND o.data_ready = TRUE
        ORDER BY p.report_date DESC, p.selection_status, p.symbol
        """,
        [session, cutoff_str, as_of_str],
    ).fetchdf()

    if rows.empty:
        return {}

    entries = rows.to_dict("records")
    selected = [r for r in entries if r["selection_status"] == "selected"]
    ignored = [r for r in entries if r["selection_status"] == "ignored"]

    def _count(items: list[dict[str, Any]], label: str) -> int:
        return len([x for x in items if x["label"] == label])

    def _avg(items: list[dict[str, Any]], field: str) -> float | None:
        vals = [_safe_float(x.get(field)) for x in items]
        vals = [v for v in vals if v is not None]
        return round(mean(vals), 3) if vals else None

    stale_items = [
        r for r in selected
        if r["label"] in {"alpha_already_paid", "good_signal_bad_timing"}
    ]
    false_positive_rate = (
        round(_count(selected, "false_positive") / len(selected), 3) if selected else None
    )
    flat_edge_rate = (
        round(_count(selected, "flat_edge") / len(selected), 3) if selected else None
    )
    feedback_counts: dict[str, int] = {}
    for row in entries:
        action = row.get("factor_feedback_action")
        if action:
            feedback_counts[action] = feedback_counts.get(action, 0) + 1

    def _example(items: list[dict[str, Any]], *, sort_key: str, top_n: int = 3) -> list[dict[str, Any]]:
        ranked = sorted(
            items,
            key=lambda row: (_safe_float(row.get(sort_key)) or 0.0, row["report_date"]),
            reverse=True,
        )
        out = []
        for row in ranked[:top_n]:
            out.append({
                "date": str(row["report_date"]),
                "symbol": row["symbol"],
                "label": row["label"],
                "confidence": row.get("signal_confidence"),
                "execution_mode": row.get("execution_mode"),
                "best_ret_pct": _safe_float(row.get("hold_3d_ret_pct"))
                if _safe_float(row.get("hold_3d_ret_pct")) is not None
                else _safe_float(row.get("next_close_ret_pct")),
                "move_consumed_ratio": _safe_float(row.get("move_consumed_ratio")),
                "feedback_action": row.get("factor_feedback_action"),
            })
        return out

    capture_rate = round(_count(selected, "captured") / len(selected), 3) if selected else None
    stale_chase_rate = round(len(stale_items) / len(selected), 3) if selected else None
    ignored_alpha_rate = round(_count(ignored, "missed_alpha") / len(ignored), 3) if ignored else None
    selected_counts = {
        "captured": _count(selected, "captured"),
        "alpha_already_paid": _count(selected, "alpha_already_paid"),
        "good_signal_bad_timing": _count(selected, "good_signal_bad_timing"),
        "false_positive": _count(selected, "false_positive"),
        "flat_edge": _count(selected, "flat_edge"),
    }
    ignored_counts = {
        "missed_alpha": _count(ignored, "missed_alpha"),
        "ignored_ok": _count(ignored, "ignored_ok"),
    }
    primary_issue, secondary_issue = _classify_review_issue(
        selected_reviewed=len(selected),
        ignored_reviewed=len(ignored),
        capture_rate=capture_rate,
        stale_chase_rate=stale_chase_rate,
        ignored_alpha_rate=ignored_alpha_rate,
        false_positive_rate=false_positive_rate,
        flat_edge_rate=flat_edge_rate,
        selected_counts=selected_counts,
        ignored_counts=ignored_counts,
    )
    verdict, implication = _review_verdict(
        primary_issue=primary_issue,
        secondary_issue=secondary_issue,
        selected_reviewed=len(selected),
        ignored_reviewed=len(ignored),
        capture_rate=capture_rate,
        stale_chase_rate=stale_chase_rate,
        ignored_alpha_rate=ignored_alpha_rate,
        false_positive_rate=false_positive_rate,
        flat_edge_rate=flat_edge_rate,
    )

    return {
        "lookback_days": lookback_days,
        "selected_reviewed": len(selected),
        "ignored_reviewed": len(ignored),
        "capture_rate": capture_rate,
        "stale_chase_rate": stale_chase_rate,
        "ignored_alpha_rate": ignored_alpha_rate,
        "false_positive_rate": false_positive_rate,
        "flat_edge_rate": flat_edge_rate,
        "avg_move_consumed_ratio": _avg(selected, "move_consumed_ratio"),
        "avg_alpha_remaining_pct": _avg(selected, "alpha_remaining_pct"),
        "selected_counts": selected_counts,
        "ignored_counts": ignored_counts,
        "factor_feedback_counts": feedback_counts,
        "primary_issue": primary_issue,
        "primary_issue_label": _ISSUE_LABELS.get(primary_issue, primary_issue),
        "secondary_issue": secondary_issue,
        "secondary_issue_label": _ISSUE_LABELS.get(secondary_issue, secondary_issue) if secondary_issue else None,
        "verdict": verdict,
        "today_implication": implication,
        "recent_stale": _example(stale_items, sort_key="move_consumed_ratio"),
        "recent_missed": _example(
            [r for r in ignored if r["label"] == "missed_alpha"],
            sort_key="hold_3d_ret_pct",
        ),
        "recent_captured": _example(
            [r for r in selected if r["label"] == "captured"],
            sort_key="hold_3d_ret_pct",
        ),
    }


def refresh_report_review(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    session: str,
    lookback_days: int = 20,
) -> dict[str, Any]:
    """Resolve outcomes, classify them, and build the render-ready review block."""
    n_outcomes = compute_report_outcomes(con, as_of, lookback_days=lookback_days)
    n_postmortem = compute_alpha_postmortem(con, as_of, lookback_days=lookback_days)
    n_algorithm_postmortem = materialize_algorithm_postmortem(
        con,
        as_of,
        lookback_days=lookback_days,
    )
    review = build_report_review(con, as_of, session, lookback_days=lookback_days)
    algorithm_summary = build_algorithm_postmortem_summary(
        con,
        as_of,
        session,
        lookback_days=lookback_days,
    )
    if algorithm_summary:
        review["algorithm_postmortem"] = algorithm_summary
    log.info(
        "report_review_refreshed",
        session=session,
        outcomes=n_outcomes,
        postmortem=n_postmortem,
        algorithm_postmortem=n_algorithm_postmortem,
        selected_reviewed=review.get("selected_reviewed", 0),
        ignored_reviewed=review.get("ignored_reviewed", 0),
    )
    return review
