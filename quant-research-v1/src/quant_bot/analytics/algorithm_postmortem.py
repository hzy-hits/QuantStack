"""Execution-aware report algorithm postmortem.

The older ``alpha_postmortem`` table reviews whether ranked candidates moved in
the suggested direction.  This module reviews the algorithm as a trading
decision: what action did the report imply, was it fillable, and what happened
from the actual next open instead of the best later excursion.
"""
from __future__ import annotations

from datetime import date, timedelta
import json
from statistics import mean
from typing import Any

import duckdb


DDL = """
CREATE TABLE IF NOT EXISTS algorithm_postmortem (
    report_date              DATE NOT NULL,
    session                  VARCHAR NOT NULL,
    symbol                   VARCHAR NOT NULL,
    selection_status         VARCHAR NOT NULL,
    evaluation_date          DATE NOT NULL,
    action_label             VARCHAR NOT NULL,
    action_source            VARCHAR,
    direction                VARCHAR,
    direction_right          BOOLEAN,
    executable               BOOLEAN,
    fill_price               DOUBLE,
    exit_price               DOUBLE,
    realized_pnl_pct         DOUBLE,
    best_possible_ret_pct    DOUBLE,
    stale_chase              BOOLEAN,
    no_fill_reason           VARCHAR,
    label                    VARCHAR NOT NULL,
    feedback_action          VARCHAR,
    feedback_weight          DOUBLE,
    report_bucket            VARCHAR,
    headline_mode            VARCHAR,
    action_intent            VARCHAR,
    calibration_bucket       VARCHAR,
    regime_bucket            VARCHAR,
    fill_quality             VARCHAR,
    detail_json              VARCHAR,
    PRIMARY KEY (report_date, session, symbol, selection_status)
);
"""

_EXTRA_COLUMNS = {
    "report_bucket": "VARCHAR",
    "headline_mode": "VARCHAR",
    "action_intent": "VARCHAR",
    "calibration_bucket": "VARCHAR",
    "regime_bucket": "VARCHAR",
    "fill_quality": "VARCHAR",
}


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)
    for column, col_type in _EXTRA_COLUMNS.items():
        try:
            con.execute(f"ALTER TABLE algorithm_postmortem ADD COLUMN IF NOT EXISTS {column} {col_type}")
        except duckdb.Error:
            pass


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    if out != out:
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
    direction = _normalize_direction(direction)
    if direction == "long":
        return 1
    if direction == "short":
        return -1
    return 0


def _signed_return(entry: float | None, exit_price: float | None, direction: Any) -> float | None:
    if entry in (None, 0) or exit_price is None:
        return None
    sign = _direction_sign(direction)
    if sign == 0:
        return None
    return round(sign * ((exit_price / entry) - 1.0) * 100.0, 3)


def _score_threshold(expected_move_pct: float | None) -> float:
    if expected_move_pct and expected_move_pct > 0:
        return max(0.75, expected_move_pct * 0.35)
    return 0.75


def _load_details(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        out = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return {}
    return out if isinstance(out, dict) else {}


def _is_actionable_lane(report_bucket: Any, headline_mode: Any) -> bool:
    lane = str(report_bucket or "").strip().lower().replace("_", " ")
    headline = str(headline_mode or "").strip().lower()
    if lane and lane not in {"core", "core book"}:
        return False
    if headline in {"range", "uncertain"}:
        return False
    return True


def _action_intent(action_label: str) -> str:
    if action_label == "TRADE_NOW":
        return "TRADE"
    if action_label == "OBSERVE":
        return "OBSERVE"
    if action_label in {"DO_NOT_CHASE", "RISK_AVOID"}:
        return "AVOID"
    return "WAIT"


def _regime_bucket(details: dict[str, Any], headline_mode: Any) -> str:
    gate = details.get("execution_gate") if isinstance(details.get("execution_gate"), dict) else {}
    overnight_alpha = (
        details.get("overnight_alpha") if isinstance(details.get("overnight_alpha"), dict) else {}
    )
    for value in (
        gate.get("trend_regime"),
        gate.get("regime"),
        overnight_alpha.get("regime"),
        headline_mode,
    ):
        text = str(value or "").strip().lower()
        if text:
            return text
    return "unknown"


def _calibration_bucket(
    *,
    report_bucket: Any,
    headline_mode: Any,
    regime_bucket: str,
    execution_mode: Any,
    action_intent: str,
) -> str:
    lane = str(report_bucket or "unknown").strip().lower().replace("_", " ")
    headline = str(headline_mode or "unknown").strip().lower()
    execution = str(execution_mode or "unknown").strip().lower()
    return (
        f"headline={headline}|lane={lane}|regime={regime_bucket}|"
        f"execution={execution}|intent={action_intent.lower()}"
    )


def _fill_quality(
    *,
    action_intent: str,
    data_ready: bool,
    executable: bool,
    stale_chase: bool,
    realized_pnl_pct: float | None,
    threshold: float,
    no_fill_reason: str | None,
) -> str:
    if action_intent != "TRADE":
        return no_fill_reason or "not_trade"
    if not data_ready:
        return "unresolved"
    if stale_chase:
        return "stale_chase"
    if not executable:
        return no_fill_reason or "no_fill"
    if realized_pnl_pct is not None and realized_pnl_pct >= max(0.35, threshold * 0.35):
        return "captured"
    if realized_pnl_pct is not None and realized_pnl_pct <= -max(0.50, threshold * 0.35):
        return "bad_fill"
    return "flat_fill"


def _action_from_row(
    *,
    selection_status: str,
    report_bucket: str | None,
    direction: str,
    execution_mode: str | None,
    rr_ratio: float | None,
    headline_mode: str | None,
    move_consumed_ratio: float | None,
    details: dict[str, Any],
) -> tuple[str, str]:
    """Infer the report's actionable intent from persisted report metadata."""
    if selection_status != "selected":
        return "WAIT", "not_selected"
    if direction == "neutral":
        return "WAIT", "neutral_signal"
    if not _is_actionable_lane(report_bucket, headline_mode):
        return "OBSERVE", "report_layer"

    mode = str(execution_mode or "").strip().lower()
    gate = details.get("execution_gate") if isinstance(details.get("execution_gate"), dict) else {}
    gate_action = str(gate.get("action") or "").strip().lower()
    if mode in {"do_not_chase", "avoid", "risk_avoid"} or gate_action == "do_not_chase":
        return "DO_NOT_CHASE", "execution_gate"
    if mode in {"wait_pullback", "pullback_only", "conditional"} or gate_action == "wait_pullback":
        return "WAIT_PULLBACK", "execution_gate"

    if rr_ratio is not None and rr_ratio < 1.0:
        return "RISK_AVOID", "rr_below_1"
    if headline_mode == "uncertain" and (rr_ratio is None or rr_ratio < 2.0):
        return "WAIT_PULLBACK", "uncertain_headline"
    if move_consumed_ratio is not None and move_consumed_ratio >= 1.0:
        return "DO_NOT_CHASE", "move_consumed"

    if mode in {"executable_now", "executable", "trade_now", "buy_now", "sell_now"}:
        return "TRADE_NOW", "execution_gate"
    return "TRADE_NOW", "selected_default"


def _feedback(label: str) -> tuple[str | None, float | None]:
    if label == "won_and_executable":
        return "reward_executable_capture", 0.5
    if label in {"false_positive_executable", "wrong_way_executable"}:
        return "penalize_false_positive", 1.1
    if label in {"stale_chase", "right_but_no_fill"}:
        return "penalize_stale_chase", 0.9
    if label == "missed_alpha":
        return "boost_recall", 1.0
    if label == "correct_avoid":
        return "reward_avoid", 0.3
    return None, None


def _classify(
    *,
    selection_status: str,
    action_label: str,
    data_ready: bool,
    direction_right: bool | None,
    stale_chase: bool,
    realized_pnl_pct: float | None,
    best_possible_ret_pct: float | None,
    threshold: float,
) -> str:
    if not data_ready:
        return "unresolved"

    best = best_possible_ret_pct
    realized = realized_pnl_pct

    if selection_status == "ignored":
        if direction_right and best is not None and best >= threshold:
            return "missed_alpha"
        return "correct_ignore"

    if action_label == "OBSERVE":
        if direction_right and best is not None and best >= threshold:
            return "observed_alpha"
        return "correct_observe"

    if action_label == "TRADE_NOW":
        if stale_chase:
            return "stale_chase"
        if realized is not None and realized >= max(0.35, threshold * 0.35):
            return "won_and_executable"
        if realized is not None and realized <= -max(0.50, threshold * 0.35):
            return "false_positive_executable"
        if direction_right and best is not None and best >= threshold:
            return "right_but_poor_exit"
        return "flat_no_edge"

    if action_label in {"WAIT_PULLBACK", "DO_NOT_CHASE", "RISK_AVOID", "WAIT"}:
        if direction_right and best is not None and best >= threshold:
            return "stale_chase" if stale_chase else "right_but_no_fill"
        return "correct_avoid"

    return "flat_no_edge"


def materialize_algorithm_postmortem(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    lookback_days: int = 20,
) -> int:
    """Persist execution-aware algorithm outcomes for recent report decisions."""
    _ensure_schema(con)
    cutoff = as_of - timedelta(days=lookback_days * 3)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    as_of_str = as_of.strftime("%Y-%m-%d")
    con.execute(
        """
        DELETE FROM algorithm_postmortem
        WHERE report_date >= ?
          AND report_date <= ?
        """,
        [cutoff_str, as_of_str],
    )

    rows = con.execute(
        """
        SELECT
            d.report_date,
            d.session,
            d.symbol,
            d.selection_status,
            d.report_bucket,
            d.signal_direction,
            d.signal_confidence,
            d.headline_mode,
            d.execution_mode,
            d.rr_ratio,
            d.expected_move_pct,
            d.gap_pct,
            d.details_json,
            o.evaluation_date,
            o.next_trade_date,
            o.entry_price,
            o.reference_price,
            o.next_open,
            o.next_close,
            o.hold_3d_close,
            o.next_open_ret_pct,
            o.next_close_ret_pct,
            o.hold_3d_ret_pct,
            o.move_consumed_ratio,
            o.alpha_remaining_pct,
            o.data_ready,
            p.label AS alpha_label
        FROM report_decisions d
        INNER JOIN report_outcomes o
          ON o.report_date = d.report_date
         AND o.session = d.session
         AND o.symbol = d.symbol
         AND o.selection_status = d.selection_status
        LEFT JOIN alpha_postmortem p
          ON p.report_date = d.report_date
         AND p.session = d.session
         AND p.symbol = d.symbol
         AND p.selection_status = d.selection_status
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
            report_bucket,
            signal_direction,
            signal_confidence,
            headline_mode,
            execution_mode,
            rr_ratio,
            expected_move_pct,
            gap_pct,
            details_json,
            evaluation_date,
            next_trade_date,
            entry_price,
            reference_price,
            next_open,
            next_close,
            hold_3d_close,
            next_open_ret_pct,
            next_close_ret_pct,
            hold_3d_ret_pct,
            move_consumed_ratio,
            alpha_remaining_pct,
            data_ready,
            alpha_label,
        ) = row

        del entry_price, reference_price, next_open_ret_pct, next_trade_date
        direction = _normalize_direction(signal_direction)
        expected_move = _safe_float(expected_move_pct)
        threshold = _score_threshold(expected_move)
        details = _load_details(details_json)
        move_consumed = _safe_float(move_consumed_ratio)
        action_label, action_source = _action_from_row(
            selection_status=str(selection_status or ""),
            report_bucket=report_bucket,
            direction=direction,
            execution_mode=execution_mode,
            rr_ratio=_safe_float(rr_ratio),
            headline_mode=headline_mode,
            move_consumed_ratio=move_consumed,
            details=details,
        )

        signed_candidates = [
            _safe_float(next_close_ret_pct),
            _safe_float(hold_3d_ret_pct),
        ]
        best_possible = max([x for x in signed_candidates if x is not None], default=None)
        direction_right = (
            best_possible is not None
            and _direction_sign(direction) != 0
            and best_possible >= threshold
        )
        stale_chase = action_label == "DO_NOT_CHASE" or (
            action_label in {"TRADE_NOW", "WAIT_PULLBACK"}
            and (
                (move_consumed is not None and move_consumed >= 0.75)
                or (
                    _safe_float(alpha_remaining_pct) is not None
                    and _safe_float(alpha_remaining_pct) <= 0
                )
            )
        )

        executable = False
        fill_price = None
        exit_price = None
        realized_pnl = None
        no_fill_reason = None
        if action_label == "TRADE_NOW" and data_ready and _direction_sign(direction) != 0:
            fill_price = _safe_float(next_open)
            exit_price = _safe_float(hold_3d_close) or _safe_float(next_close)
            executable = fill_price is not None and exit_price is not None
            if executable:
                realized_pnl = _signed_return(fill_price, exit_price, direction)
            else:
                no_fill_reason = "missing_next_open_or_exit"
        elif action_label == "WAIT_PULLBACK":
            no_fill_reason = "pullback_not_observable"
        elif action_label == "DO_NOT_CHASE":
            no_fill_reason = "do_not_chase"
        elif action_label == "RISK_AVOID":
            no_fill_reason = "risk_reward_rejected"
        elif action_label == "OBSERVE":
            no_fill_reason = "report_bucket_observation"
        else:
            no_fill_reason = "not_actionable"

        label = _classify(
            selection_status=str(selection_status or ""),
            action_label=action_label,
            data_ready=bool(data_ready),
            direction_right=direction_right,
            stale_chase=stale_chase,
            realized_pnl_pct=realized_pnl,
            best_possible_ret_pct=best_possible,
            threshold=threshold,
        )
        feedback_action, feedback_weight = _feedback(label)
        intent = _action_intent(action_label)
        regime_bucket = _regime_bucket(details, headline_mode)
        calibration_bucket = _calibration_bucket(
            report_bucket=report_bucket,
            headline_mode=headline_mode,
            regime_bucket=regime_bucket,
            execution_mode=execution_mode,
            action_intent=intent,
        )
        fill_quality = _fill_quality(
            action_intent=intent,
            data_ready=bool(data_ready),
            executable=executable,
            stale_chase=stale_chase,
            realized_pnl_pct=realized_pnl,
            threshold=threshold,
            no_fill_reason=no_fill_reason,
        )
        detail = {
            "alpha_postmortem_label": alpha_label,
            "report_bucket": report_bucket,
            "signal_confidence": signal_confidence,
            "headline_mode": headline_mode,
            "execution_mode": execution_mode,
            "action_intent": intent,
            "calibration_bucket": calibration_bucket,
            "regime_bucket": regime_bucket,
            "fill_quality": fill_quality,
            "rr_ratio": _safe_float(rr_ratio),
            "expected_move_pct": expected_move,
            "gap_pct": _safe_float(gap_pct),
            "threshold_pct": threshold,
            "move_consumed_ratio": move_consumed,
            "alpha_remaining_pct": _safe_float(alpha_remaining_pct),
        }

        inserts.append([
            report_date,
            session,
            symbol,
            selection_status,
            evaluation_date or as_of_str,
            action_label,
            action_source,
            direction,
            direction_right,
            executable,
            fill_price,
            exit_price,
            realized_pnl,
            best_possible,
            stale_chase,
            no_fill_reason,
            label,
            feedback_action,
            feedback_weight,
            report_bucket,
            headline_mode,
            intent,
            calibration_bucket,
            regime_bucket,
            fill_quality,
            json.dumps(detail, ensure_ascii=True, default=str),
        ])

    con.executemany(
        """
        INSERT OR REPLACE INTO algorithm_postmortem (
            report_date, session, symbol, selection_status, evaluation_date,
            action_label, action_source, direction, direction_right, executable,
            fill_price, exit_price, realized_pnl_pct, best_possible_ret_pct,
            stale_chase, no_fill_reason, label, feedback_action, feedback_weight,
            report_bucket, headline_mode, action_intent, calibration_bucket,
            regime_bucket, fill_quality,
            detail_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        inserts,
    )
    return len(inserts)


def build_algorithm_postmortem_summary(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    session: str,
    lookback_days: int = 20,
) -> dict[str, Any]:
    """Build a compact report-ready summary of algorithm action outcomes."""
    _ensure_schema(con)
    cutoff = as_of - timedelta(days=lookback_days * 3)
    rows = con.execute(
        """
        SELECT
            report_date,
            symbol,
            selection_status,
            action_label,
            executable,
            realized_pnl_pct,
            best_possible_ret_pct,
            stale_chase,
            label,
            no_fill_reason,
            action_intent,
            calibration_bucket,
            regime_bucket,
            fill_quality
        FROM algorithm_postmortem
        WHERE session = ?
          AND report_date >= ?
          AND report_date <= ?
          AND label <> 'unresolved'
        ORDER BY report_date DESC, symbol
        """,
        [session, cutoff.strftime("%Y-%m-%d"), as_of.strftime("%Y-%m-%d")],
    ).fetchall()
    if not rows:
        return {}

    entries = [
        {
            "date": str(r[0]),
            "symbol": r[1],
            "selection_status": r[2],
            "action_label": r[3],
            "executable": bool(r[4]),
            "realized_pnl_pct": _safe_float(r[5]),
            "best_possible_ret_pct": _safe_float(r[6]),
            "stale_chase": bool(r[7]),
            "label": r[8],
            "no_fill_reason": r[9],
            "action_intent": r[10],
            "calibration_bucket": r[11],
            "regime_bucket": r[12],
            "fill_quality": r[13],
        }
        for r in rows
    ]
    selected = [r for r in entries if r["selection_status"] == "selected"]
    executable = [r for r in selected if r["executable"]]
    wins = [r for r in executable if (r["realized_pnl_pct"] or 0.0) > 0]
    stale = [r for r in selected if r["label"] == "stale_chase"]
    no_fill_right = [r for r in selected if r["label"] == "right_but_no_fill"]
    missed = [r for r in entries if r["label"] == "missed_alpha"]
    realized_vals = [r["realized_pnl_pct"] for r in executable if r["realized_pnl_pct"] is not None]

    def _count(label: str) -> int:
        return len([r for r in entries if r["label"] == label])

    def _examples(items: list[dict[str, Any]], score_field: str, n: int = 3) -> list[dict[str, Any]]:
        ranked = sorted(items, key=lambda r: (r.get(score_field) or 0.0, r["date"]), reverse=True)
        return ranked[:n]

    def _calibration_rows() -> list[dict[str, Any]]:
        buckets: dict[str, list[dict[str, Any]]] = {}
        for row in entries:
            key = row.get("calibration_bucket") or "unknown"
            buckets.setdefault(str(key), []).append(row)
        out: list[dict[str, Any]] = []
        for key, bucket_rows in buckets.items():
            trade_rows = [r for r in bucket_rows if r.get("action_intent") == "TRADE"]
            trade_wins = [r for r in trade_rows if r["label"] == "won_and_executable"]
            stale_rows = [r for r in bucket_rows if r["label"] == "stale_chase"]
            missed_rows = [r for r in bucket_rows if r["label"] == "missed_alpha"]
            out.append(
                {
                    "bucket": key,
                    "n": len(bucket_rows),
                    "trade_n": len(trade_rows),
                    "trade_win_rate": round(len(trade_wins) / len(trade_rows), 3)
                    if trade_rows
                    else None,
                    "stale_rate": round(len(stale_rows) / len(bucket_rows), 3),
                    "missed_rate": round(len(missed_rows) / len(bucket_rows), 3),
                }
            )
        return sorted(out, key=lambda r: (r["n"], r["trade_n"]), reverse=True)[:8]

    return {
        "lookback_days": lookback_days,
        "reviewed": len(entries),
        "selected_reviewed": len(selected),
        "executable_reviewed": len(executable),
        "executable_win_rate": round(len(wins) / len(executable), 3) if executable else None,
        "avg_realized_pnl_pct": round(mean(realized_vals), 3) if realized_vals else None,
        "stale_chase_count": len(stale),
        "right_but_no_fill_count": len(no_fill_right),
        "missed_alpha_count": len(missed),
        "label_counts": {
            "won_and_executable": _count("won_and_executable"),
            "false_positive_executable": _count("false_positive_executable"),
            "right_but_no_fill": _count("right_but_no_fill"),
            "stale_chase": _count("stale_chase"),
            "missed_alpha": _count("missed_alpha"),
            "correct_avoid": _count("correct_avoid"),
            "observed_alpha": _count("observed_alpha"),
            "correct_observe": _count("correct_observe"),
            "flat_no_edge": _count("flat_no_edge"),
        },
        "calibration_buckets": _calibration_rows(),
        "recent_stale_or_no_fill": _examples(stale + no_fill_right, "best_possible_ret_pct"),
        "recent_executable_losses": _examples(
            [r for r in executable if (r["realized_pnl_pct"] or 0.0) < 0],
            "realized_pnl_pct",
        ),
        "recent_missed": _examples(missed, "best_possible_ret_pct"),
    }
