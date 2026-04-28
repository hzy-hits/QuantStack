#!/usr/bin/env python3
"""Daily stability gate for US/CN execution-alpha playbooks.

This script turns the existing report-review ledgers into a daily alpha
bulletin.  It does not train a model; it evaluates deterministic playbook
families, applies market-specific stability gates, and emits Markdown snippets
that the US/CN payload renderers can include directly.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "strategy_backtest"
HISTORY_DB = STACK_ROOT / "data" / "strategy_backtest_history.duckdb"
ONE_SIDED_95_Z = 1.64


STABILITY_THRESHOLDS = {
    "us": {
        "min_fills": 20,
        "min_active_buckets": 10,
        "min_avg_trade_pct": 0.40,
        "min_median_trade_pct": 0.0,
        "min_strict_win_rate": 0.45,
        "min_max_drawdown_pct": -25.0,
        "max_top1_winner_contribution": 0.45,
    },
    "cn": {
        "min_fills": 50,
        "min_active_buckets": 15,
        "min_avg_trade_pct": 0.30,
        "min_median_trade_pct": 0.0,
        "min_strict_win_rate": 0.43,
        "min_max_drawdown_pct": -8.0,
        "max_top1_winner_contribution": 0.25,
    },
}


class MarketConfig:
    def __init__(self, market: str, db_path: Path, horizon_days: int) -> None:
        self.market = market
        self.db_path = db_path
        self.horizon_days = horizon_days


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run stable playbook gate and emit alpha bulletin.")
    parser.add_argument("--date", required=True, help="Report date, YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--auto-select", action="store_true", help="Select daily champion policy.")
    parser.add_argument("--emit-bulletin", action="store_true", help="Write JSON and Markdown bulletin files.")
    parser.add_argument("--history-db", type=Path, default=HISTORY_DB)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--us-db", type=Path, default=STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb")
    parser.add_argument("--cn-db", type=Path, default=STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb")
    parser.add_argument("--us-horizon-days", type=int, default=3)
    parser.add_argument("--cn-horizon-days", type=int, default=2)
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def safe_json_loads(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def round_or_none(value: Any, digits: int = 4) -> float | None:
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(fval) or math.isinf(fval):
        return None
    return round(fval, digits)


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    except duckdb.Error:
        return set()
    return {str(row[1]) for row in rows}


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def sql_col(
    alias: str,
    column: str,
    columns: set[str],
    output: str | None = None,
    *,
    default: str = "NULL",
) -> str:
    out = output or column
    if column in columns:
        return f"{alias}.{column} AS {out}"
    return f"{default} AS {out}"


def sql_coalesce(
    refs: Iterable[tuple[str, str, set[str]]],
    output: str,
    *,
    default: str = "NULL",
) -> str:
    parts = [f"{alias}.{column}" for alias, column, columns in refs if column in columns]
    if not parts:
        return f"{default} AS {output}"
    if len(parts) == 1:
        return f"{parts[0]} AS {output}"
    return f"COALESCE({', '.join(parts)}) AS {output}"


def normalize_bucket(value: Any) -> str:
    text = str(value or "unknown").strip().lower().replace("-", " ").replace("_", " ")
    mapping = {
        "core": "core",
        "core book": "core",
        "range core": "range_core",
        "tactical continuation": "tactical_continuation",
        "event tape": "event_tape",
        "tactical event tape": "event_tape",
        "theme rotation": "theme_rotation",
        "appendix": "appendix",
        "appendix radar": "appendix",
        "radar": "radar",
    }
    return mapping.get(text, text.replace(" ", "_") or "unknown")


def normalize_direction(value: Any) -> str:
    text = str(value or "neutral").strip().lower()
    if text in {"long", "bull", "bullish", "up"}:
        return "long"
    if text in {"short", "bear", "bearish", "down"}:
        return "short"
    return "neutral"


def normalize_confidence(value: Any) -> str:
    text = str(value or "unknown").strip().upper()
    if text in {"HIGH", "MODERATE"}:
        return "high_mod"
    if text == "WATCH":
        return "watch"
    if text in {"LOW", "NO_SIGNAL", "NONE"}:
        return "low"
    return text.lower() or "unknown"


def normalize_execution(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    if text in {"trade", "trade_now", "executable", "executable_now", "main_signal"}:
        return "executable_now"
    if text in {"wait", "wait_pullback", "pullback"}:
        return "wait_pullback"
    if text in {"avoid", "do_not_chase", "stale_chase"}:
        return "do_not_chase"
    if text in {"observe", "observation", "directional_observation"}:
        return "observe"
    return text or "unknown"


def main_signal_gate(details: dict[str, Any]) -> dict[str, Any]:
    gate = details.get("main_signal_gate")
    return gate if isinstance(gate, dict) else {}


def row_policy(row: dict[str, Any], market: str, horizon_days: int) -> dict[str, str]:
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    bucket = normalize_bucket(row.get("report_bucket") or gate.get("report_bucket"))
    direction = normalize_direction(row.get("signal_direction") or row.get("direction"))
    confidence = normalize_confidence(row.get("signal_confidence"))
    execution = normalize_execution(
        row.get("execution_mode")
        or row.get("action_intent")
        or gate.get("execution_action")
        or gate.get("execution_mode")
        or gate.get("action_intent")
    )
    policy_id = f"{market}:{bucket}:{direction}:{confidence}:{execution}:h{horizon_days}"
    label = (
        f"{market.upper()} {bucket.replace('_', ' ')} {direction} "
        f"{confidence.replace('_', '/')} {execution.replace('_', ' ')} {horizon_days}D"
    )
    return {
        "policy_id": policy_id,
        "policy_label": label,
        "bucket": bucket,
        "direction": direction,
        "confidence": confidence,
        "execution": execution,
    }


def completed_cutoff(as_of: date, horizon_days: int) -> date:
    return as_of - timedelta(days=max(horizon_days, 0))


def load_evaluated_trades(
    db_path: Path,
    market: str,
    as_of: date,
    lookback_days: int,
    horizon_days: int,
) -> tuple[list[dict[str, Any]], str]:
    cutoff = completed_cutoff(as_of, horizon_days)
    start = cutoff - timedelta(days=lookback_days)
    evaluated_through = cutoff.isoformat()

    if not db_path.exists():
        return [], evaluated_through

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows: list[dict[str, Any]] = []
        if table_exists(con, "algorithm_postmortem"):
            rows = load_algorithm_postmortem_rows(con, start, cutoff, as_of)
        if (
            (not rows or not any(is_fill(row) for row in rows))
            and table_exists(con, "report_decisions")
            and table_exists(con, "report_outcomes")
        ):
            rows = load_report_outcome_rows(con, start, cutoff, as_of)
        if not rows:
            return [], evaluated_through
    finally:
        con.close()

    for row in rows:
        row["market"] = market
        policy = row_policy(row, market, horizon_days)
        row.update(policy)
        row["return_pct"] = round_or_none(row.get("return_pct"), 6)
    max_date = max((to_iso_date(row.get("report_date")) for row in rows), default=None)
    if max_date:
        evaluated_through = min(evaluated_through, max_date)
    return rows, evaluated_through


def load_algorithm_postmortem_rows(
    con: duckdb.DuckDBPyConnection,
    start: date,
    cutoff: date,
    as_of: date,
) -> list[dict[str, Any]]:
    a_cols = table_columns(con, "algorithm_postmortem")
    d_cols = table_columns(con, "report_decisions") if table_exists(con, "report_decisions") else set()
    join = (
        """
        LEFT JOIN report_decisions d
          ON a.report_date = d.report_date
         AND a.session = d.session
         AND a.symbol = d.symbol
         AND a.selection_status = d.selection_status
        """
        if d_cols
        else ""
    )
    select_parts = [
        sql_col("a", "report_date", a_cols),
        sql_col("a", "evaluation_date", a_cols),
        sql_col("a", "symbol", a_cols),
        sql_col("a", "selection_status", a_cols),
        sql_coalesce([("d", "rank_order", d_cols)], "rank_order"),
        sql_coalesce([("d", "report_bucket", d_cols), ("a", "report_bucket", a_cols)], "report_bucket"),
        sql_coalesce(
            [("d", "signal_direction", d_cols), ("a", "direction", a_cols)],
            "signal_direction",
        ),
        sql_coalesce([("d", "signal_confidence", d_cols)], "signal_confidence"),
        sql_coalesce([("d", "execution_mode", d_cols)], "execution_mode"),
        sql_coalesce([("d", "composite_score", d_cols)], "composite_score"),
        sql_col("a", "action_intent", a_cols),
        sql_col("a", "executable", a_cols, default="FALSE"),
        sql_col("a", "realized_pnl_pct", a_cols, "return_pct"),
        sql_col("a", "best_possible_ret_pct", a_cols),
        sql_col("a", "stale_chase", a_cols, default="FALSE"),
        sql_col("a", "no_fill_reason", a_cols),
        sql_col("a", "label", a_cols),
        sql_col("a", "calibration_bucket", a_cols),
        sql_coalesce([("d", "details_json", d_cols), ("a", "detail_json", a_cols)], "details_json"),
    ]
    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM algorithm_postmortem a
        {join}
        WHERE a.report_date >= ? AND a.report_date <= ?
          AND (a.evaluation_date IS NULL OR a.evaluation_date <= ?)
        ORDER BY a.report_date, a.symbol
    """
    return rows_as_dicts(con, sql, [start.isoformat(), cutoff.isoformat(), as_of.isoformat()])


def load_report_outcome_rows(
    con: duckdb.DuckDBPyConnection,
    start: date,
    cutoff: date,
    as_of: date,
) -> list[dict[str, Any]]:
    d_cols = table_columns(con, "report_decisions")
    o_cols = table_columns(con, "report_outcomes")
    return_expr = "NULL"
    if "hold_3d_ret_pct" in o_cols:
        return_expr = "o.hold_3d_ret_pct"
    elif "next_close_ret_pct" in o_cols:
        return_expr = """
            CASE
              WHEN lower(COALESCE(d.signal_direction, '')) IN ('short', 'bearish') THEN -o.next_close_ret_pct
              ELSE o.next_close_ret_pct
            END
        """
    elif "best_up_2d_pct" in o_cols:
        return_expr = "o.best_up_2d_pct"

    select_parts = [
        sql_col("d", "report_date", d_cols),
        sql_col("o", "evaluation_date", o_cols),
        sql_col("d", "symbol", d_cols),
        sql_col("d", "selection_status", d_cols),
        sql_col("d", "rank_order", d_cols),
        sql_col("d", "report_bucket", d_cols),
        sql_col("d", "signal_direction", d_cols),
        sql_col("d", "signal_confidence", d_cols),
        sql_col("d", "execution_mode", d_cols),
        sql_col("d", "composite_score", d_cols),
        "NULL AS action_intent",
        "TRUE AS executable",
        f"{return_expr} AS return_pct",
        "NULL AS best_possible_ret_pct",
        "FALSE AS stale_chase",
        "NULL AS no_fill_reason",
        "NULL AS label",
        "NULL AS calibration_bucket",
        sql_col("d", "details_json", d_cols),
    ]
    data_ready_predicate = "AND COALESCE(o.data_ready, TRUE)" if "data_ready" in o_cols else ""
    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM report_decisions d
        INNER JOIN report_outcomes o
          ON d.report_date = o.report_date
         AND d.session = o.session
         AND d.symbol = o.symbol
         AND d.selection_status = o.selection_status
        WHERE d.report_date >= ? AND d.report_date <= ?
          AND (o.evaluation_date IS NULL OR o.evaluation_date <= ?)
          {data_ready_predicate}
        ORDER BY d.report_date, d.symbol
    """
    return rows_as_dicts(con, sql, [start.isoformat(), cutoff.isoformat(), as_of.isoformat()])


def to_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def is_fill(row: dict[str, Any]) -> bool:
    if row.get("return_pct") is None:
        return False
    executable = row.get("executable")
    if executable is None:
        return True
    if isinstance(executable, bool):
        return executable
    return str(executable).lower() in {"true", "1", "t", "yes"}


def max_drawdown_pct(daily_returns: list[tuple[str, float]]) -> float | None:
    if not daily_returns:
        return None
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for _, ret in daily_returns:
        equity += ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def top1_winner_contribution(returns: list[float]) -> float | None:
    winners = [ret for ret in returns if ret > 0]
    if not winners:
        return None
    total = sum(winners)
    if total <= 0:
        return None
    return max(winners) / total


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def ev_evidence_metrics(returns: list[float]) -> dict[str, Any]:
    """Return statistical evidence for positive EV.

    This is not a trading model. It is an auditable evidence layer over realized
    walk-forward returns: if the lower confidence bound is still below zero, the
    strategy has not statistically earned an execution upgrade.
    """
    n = len(returns)
    if n == 0:
        return {
            "return_std_pct": None,
            "ev_probability_positive": None,
            "ev_lower_confidence_pct": None,
            "fills_required_for_95_lcb": None,
        }
    mean = statistics.fmean(returns)
    if n < 2:
        return {
            "return_std_pct": None,
            "ev_probability_positive": 1.0 if mean > 0 else 0.0,
            "ev_lower_confidence_pct": None,
            "fills_required_for_95_lcb": None,
        }
    std = statistics.stdev(returns)
    if std <= 0:
        prob = 1.0 if mean > 0 else 0.0
        lower = mean
        required = n if lower > 0 else None
    else:
        se = std / math.sqrt(n)
        prob = normal_cdf(mean / se) if se > 0 else (1.0 if mean > 0 else 0.0)
        lower = mean - ONE_SIDED_95_Z * se
        required = math.ceil((ONE_SIDED_95_Z * std / mean) ** 2) if mean > 0 else None
    return {
        "return_std_pct": round_or_none(std, 6),
        "ev_probability_positive": round_or_none(prob, 6),
        "ev_lower_confidence_pct": round_or_none(lower, 6),
        "fills_required_for_95_lcb": required,
    }


def evaluate_policy(
    market: str,
    policy_id: str,
    policy_label: str,
    rows: list[dict[str, Any]],
    horizon_days: int,
    lookback_days: int,
) -> dict[str, Any]:
    thresholds = STABILITY_THRESHOLDS[market]
    fills = [row for row in rows if is_fill(row)]
    returns = [float(row["return_pct"]) for row in fills if row.get("return_pct") is not None]
    active_dates = {to_iso_date(row.get("report_date")) for row in fills if row.get("report_date") is not None}
    daily: dict[str, list[float]] = {}
    for row, ret in zip(fills, returns, strict=False):
        report_date = to_iso_date(row.get("report_date"))
        if report_date:
            daily.setdefault(report_date, []).append(ret)
    daily_returns = sorted((key, statistics.fmean(vals)) for key, vals in daily.items())

    avg_trade = statistics.fmean(returns) if returns else None
    median_trade = statistics.median(returns) if returns else None
    win_rate = (sum(1 for ret in returns if ret > 0) / len(returns)) if returns else None
    max_dd = max_drawdown_pct(daily_returns)
    top1 = top1_winner_contribution(returns)
    evidence = ev_evidence_metrics(returns)
    fail_reasons = stability_fail_reasons(
        fills=len(returns),
        active_buckets=len(active_dates),
        avg_trade_pct=avg_trade,
        median_trade_pct=median_trade,
        strict_win_rate=win_rate,
        max_drawdown=max_dd,
        top1_contribution=top1,
        thresholds=thresholds,
    )
    fail_reasons.extend(policy_scope_fail_reasons(policy_id))
    eligible = not fail_reasons
    score = stability_score(
        fills=len(returns),
        avg_trade_pct=avg_trade,
        strict_win_rate=win_rate,
        max_drawdown=max_dd,
        top1_contribution=top1,
        thresholds=thresholds,
    )
    return {
        "market": market,
        "policy_id": policy_id,
        "policy_label": policy_label,
        "horizon_days": horizon_days,
        "lookback_days": lookback_days,
        "fills": len(returns),
        "active_buckets": len(active_dates),
        "avg_trade_pct": round_or_none(avg_trade, 6),
        "return_std_pct": evidence["return_std_pct"],
        "median_trade_pct": round_or_none(median_trade, 6),
        "strict_win_rate": round_or_none(win_rate, 6),
        "max_drawdown_pct": round_or_none(max_dd, 6),
        "top1_winner_contribution": round_or_none(top1, 6),
        "ev_probability_positive": evidence["ev_probability_positive"],
        "ev_lower_confidence_pct": evidence["ev_lower_confidence_pct"],
        "fills_required_for_95_lcb": evidence["fills_required_for_95_lcb"],
        "stability_score": round_or_none(score, 6) or 0.0,
        "eligible": eligible,
        "fail_reasons": fail_reasons,
        "selected": False,
    }


def stability_fail_reasons(
    *,
    fills: int,
    active_buckets: int,
    avg_trade_pct: float | None,
    median_trade_pct: float | None,
    strict_win_rate: float | None,
    max_drawdown: float | None,
    top1_contribution: float | None,
    thresholds: dict[str, float],
) -> list[str]:
    reasons: list[str] = []
    if fills < thresholds["min_fills"]:
        reasons.append(f"fills<{thresholds['min_fills']}")
    if active_buckets < thresholds["min_active_buckets"]:
        reasons.append(f"active_buckets<{thresholds['min_active_buckets']}")
    if avg_trade_pct is None or avg_trade_pct <= thresholds["min_avg_trade_pct"]:
        reasons.append(f"avg_trade_pct<={thresholds['min_avg_trade_pct']}")
    if median_trade_pct is None or median_trade_pct < thresholds["min_median_trade_pct"]:
        reasons.append(f"median_trade_pct<{thresholds['min_median_trade_pct']}")
    if strict_win_rate is None or strict_win_rate <= thresholds["min_strict_win_rate"]:
        reasons.append(f"strict_win_rate<={thresholds['min_strict_win_rate']}")
    if max_drawdown is None or max_drawdown <= thresholds["min_max_drawdown_pct"]:
        reasons.append(f"max_drawdown_pct<={thresholds['min_max_drawdown_pct']}")
    if top1_contribution is None:
        reasons.append("top1_winner_contribution=NA")
    elif top1_contribution > thresholds["max_top1_winner_contribution"]:
        reasons.append(f"top1_winner_contribution>{thresholds['max_top1_winner_contribution']}")
    return reasons


def policy_scope_fail_reasons(policy_id: str) -> list[str]:
    parts = policy_id.split(":")
    if len(parts) < 6:
        return ["policy_scope_unparseable"]
    _, bucket, direction, confidence, execution, *_ = parts
    reasons: list[str] = []
    if bucket != "core":
        reasons.append("policy_bucket_not_core")
    if direction not in {"long", "short"}:
        reasons.append("policy_direction_not_tradeable")
    if confidence != "high_mod":
        reasons.append("policy_confidence_not_high_mod")
    if execution != "executable_now":
        reasons.append("policy_execution_not_now")
    return reasons


def stability_score(
    *,
    fills: int,
    avg_trade_pct: float | None,
    strict_win_rate: float | None,
    max_drawdown: float | None,
    top1_contribution: float | None,
    thresholds: dict[str, float],
) -> float:
    if fills <= 0 or avg_trade_pct is None or strict_win_rate is None:
        return 0.0
    fill_factor = min(2.0, math.sqrt(fills / max(float(thresholds["min_fills"]), 1.0)))
    edge = max(avg_trade_pct, 0.0)
    win = max(strict_win_rate, 0.0)
    concentration = 1.0 - min(max(top1_contribution or 1.0, 0.0), 1.0)
    dd_penalty = 1.0
    if max_drawdown is not None and max_drawdown < 0:
        dd_penalty = max(0.1, 1.0 + max_drawdown / 100.0)
    return edge * win * fill_factor * concentration * dd_penalty


def build_policy_candidates(
    rows: list[dict[str, Any]],
    market: str,
    horizon_days: int,
    lookback_days: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    labels: dict[str, str] = {}
    for row in rows:
        policy_id = str(row["policy_id"])
        grouped.setdefault(policy_id, []).append(row)
        labels[policy_id] = str(row.get("policy_label") or policy_id)
    return [
        evaluate_policy(market, policy_id, labels[policy_id], policy_rows, horizon_days, lookback_days)
        for policy_id, policy_rows in sorted(grouped.items())
    ]


def load_previous_champion(history_db: Path, market: str, as_of: date) -> str | None:
    if not history_db.exists():
        return None
    con = duckdb.connect(str(history_db), read_only=True)
    try:
        if not table_exists(con, "playbook_selection"):
            return None
        row = con.execute(
            """
            SELECT selected_policy_id
            FROM playbook_selection
            WHERE market = ? AND as_of < ? AND selected_policy_id IS NOT NULL
            ORDER BY as_of DESC
            LIMIT 1
            """,
            [market, as_of.isoformat()],
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    finally:
        con.close()


def select_champion(
    candidates: list[dict[str, Any]],
    previous_policy_id: str | None,
    *,
    challenger_margin: float = 0.15,
) -> tuple[str | None, str]:
    eligible = [c for c in candidates if c.get("eligible")]
    if not eligible:
        return None, "no eligible policy passed stability gate"
    challenger = max(eligible, key=lambda c: (float(c.get("stability_score") or 0.0), c["fills"]))
    if previous_policy_id:
        previous = next(
            (c for c in eligible if c.get("policy_id") == previous_policy_id),
            None,
        )
        if previous:
            prev_score = float(previous.get("stability_score") or 0.0)
            challenge_score = float(challenger.get("stability_score") or 0.0)
            if challenger["policy_id"] == previous_policy_id:
                return previous_policy_id, "incumbent remains top eligible policy"
            if challenge_score <= prev_score * (1.0 + challenger_margin):
                return previous_policy_id, "incumbent held; challenger did not clear 15% score margin"
            return str(challenger["policy_id"]), "challenger replaced incumbent after clearing 15% score margin"
    return str(challenger["policy_id"]), "selected highest stability score among eligible policies"


def mark_selected(candidates: list[dict[str, Any]], selected_policy_id: str | None) -> None:
    for candidate in candidates:
        candidate["selected"] = bool(selected_policy_id and candidate["policy_id"] == selected_policy_id)


def load_current_candidates(db_path: Path, market: str, as_of: date, horizon_days: int) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "report_decisions"):
            return []
        d_cols = table_columns(con, "report_decisions")
        select_parts = [
            sql_col("d", "report_date", d_cols),
            sql_col("d", "symbol", d_cols),
            sql_col("d", "selection_status", d_cols),
            sql_col("d", "rank_order", d_cols),
            sql_col("d", "report_bucket", d_cols),
            sql_col("d", "signal_direction", d_cols),
            sql_col("d", "signal_confidence", d_cols),
            sql_col("d", "headline_mode", d_cols),
            sql_col("d", "execution_mode", d_cols),
            sql_col("d", "composite_score", d_cols),
            sql_col("d", "rr_ratio", d_cols),
            sql_col("d", "primary_reason", d_cols),
            sql_col("d", "details_json", d_cols),
        ]
        order_expr = "COALESCE(d.rank_order, 999999)" if "rank_order" in d_cols else "999999"
        rows = rows_as_dicts(
            con,
            f"""
            SELECT {", ".join(select_parts)}
            FROM report_decisions d
            WHERE d.report_date = ?
            ORDER BY {order_expr}, d.symbol
            """,
            [as_of.isoformat()],
        )
    finally:
        con.close()
    for row in rows:
        row["market"] = market
        row["return_pct"] = None
        row.update(row_policy(row, market, horizon_days))
    return rows


def load_options_alpha_candidates(db_path: Path, market: str, as_of: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    if market == "us":
        return load_us_options_alpha_candidates(db_path, as_of)
    if market == "cn":
        return load_cn_shadow_options_alpha_candidates(db_path, as_of)
    return []


def load_us_options_alpha_candidates(db_path: Path, as_of: date) -> list[dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "options_alpha"):
            return []
        rows = rows_as_dicts(
            con,
            """
            SELECT symbol, directional_edge, vol_edge, vrp_edge, flow_edge,
                   liquidity_gate, expression, reason, detail_json
            FROM options_alpha
            WHERE as_of = CAST(? AS DATE)
              AND expression IN ('stock_long', 'call_spread', 'put_spread')
              AND liquidity_gate = 'pass'
            ORDER BY
              ABS(COALESCE(directional_edge, 0)) + ABS(COALESCE(vol_edge, 0)) DESC,
              symbol
            LIMIT 20
            """,
            [as_of.isoformat()],
        )
    finally:
        con.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        detail = safe_json_loads(row.get("detail_json"))
        out.append(
            {
                "market": "us",
                "symbol": row.get("symbol"),
                "section": "options_alpha",
                "source": "real_options",
                "expression": row.get("expression"),
                "reason": row.get("reason") or "real-options edge candidate",
                "blockers": [],
                "details": {
                    "directional_edge": round_or_none(row.get("directional_edge")),
                    "vol_edge": round_or_none(row.get("vol_edge")),
                    "vrp_edge": round_or_none(row.get("vrp_edge")),
                    "flow_edge": round_or_none(row.get("flow_edge")),
                    "liquidity_gate": row.get("liquidity_gate"),
                    "option_context": detail,
                },
            }
        )
    return out


def load_cn_shadow_options_alpha_candidates(db_path: Path, as_of: date) -> list[dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "analytics"):
            return []
        rows = rows_as_dicts(
            con,
            """
            WITH base AS (
                SELECT
                    a.ts_code,
                    MAX(CASE WHEN a.metric = 'shadow_alpha_prob' THEN a.value END) AS shadow_alpha_prob,
                    MAX(CASE WHEN a.metric = 'entry_quality_score' THEN a.value END) AS entry_quality_score,
                    MAX(CASE WHEN a.metric = 'stale_chase_risk' THEN a.value END) AS stale_chase_risk,
                    MAX(CASE WHEN a.metric = 'calibration_bucket' THEN a.value END) AS calibration_bucket,
                    MAX(a.detail) AS detail_json
                FROM analytics a
                WHERE a.as_of = CAST(? AS DATE)
                  AND a.module = 'shadow_option_alpha'
                GROUP BY a.ts_code
            ),
            fast AS (
                SELECT
                    ts_code,
                    MAX(CASE WHEN metric = 'shadow_iv_30d' THEN value END) AS shadow_iv_30d,
                    MAX(CASE WHEN metric = 'downside_stress' THEN value END) AS downside_stress
                FROM analytics
                WHERE as_of = CAST(? AS DATE)
                  AND module = 'shadow_fast'
                GROUP BY ts_code
            ),
            full_metrics AS (
                SELECT
                    ts_code,
                    MAX(CASE WHEN metric = 'shadow_touch_90_3m' THEN value END) AS shadow_touch_90_3m,
                    MAX(CASE WHEN metric = 'shadow_skew_90_3m' THEN value END) AS shadow_skew_90_3m
                FROM analytics
                WHERE as_of = CAST(? AS DATE)
                  AND module = 'shadow_full'
                GROUP BY ts_code
            )
            SELECT
                base.ts_code AS symbol,
                base.shadow_alpha_prob,
                base.entry_quality_score,
                base.stale_chase_risk,
                base.calibration_bucket,
                base.detail_json,
                fast.shadow_iv_30d,
                fast.downside_stress,
                full_metrics.shadow_touch_90_3m,
                full_metrics.shadow_skew_90_3m
            FROM base
            LEFT JOIN fast ON fast.ts_code = base.ts_code
            LEFT JOIN full_metrics ON full_metrics.ts_code = base.ts_code
            WHERE COALESCE(base.shadow_alpha_prob, 0) >= 0.30
              AND COALESCE(base.entry_quality_score, 0) >= 0.38
              AND COALESCE(base.stale_chase_risk, 1) <= 0.40
            ORDER BY
              COALESCE(base.shadow_alpha_prob, 0) + COALESCE(base.entry_quality_score, 0)
              - COALESCE(base.stale_chase_risk, 0) DESC,
              base.ts_code
            LIMIT 20
            """,
            [as_of.isoformat(), as_of.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        detail = safe_json_loads(row.get("detail_json"))
        alpha_prob = round_or_none(row.get("shadow_alpha_prob"))
        entry_quality = round_or_none(row.get("entry_quality_score"))
        stale_risk = round_or_none(row.get("stale_chase_risk"))
        expression = (
            "stock_long_shadow_confirmed"
            if (alpha_prob or 0) >= 0.65 and (entry_quality or 0) >= 0.58 and (stale_risk or 1) <= 0.45
            else "wait"
        )
        out.append(
            {
                "market": "cn",
                "symbol": row.get("symbol"),
                "section": "options_alpha",
                "source": "shadow_options",
                "expression": expression,
                "reason": "A-share shadow-option risk/convexity check; not a real single-name option trade",
                "blockers": [] if expression != "wait" else ["wait for equity gate / pullback confirmation"],
                "details": {
                    "shadow_alpha_prob": alpha_prob,
                    "entry_quality_score": entry_quality,
                    "stale_chase_risk": stale_risk,
                    "shadow_iv_30d": round_or_none(row.get("shadow_iv_30d")),
                    "downside_stress": round_or_none(row.get("downside_stress")),
                    "shadow_touch_90_3m": round_or_none(row.get("shadow_touch_90_3m")),
                    "shadow_skew_90_3m": round_or_none(row.get("shadow_skew_90_3m")),
                    "calibration_bucket": round_or_none(row.get("calibration_bucket")),
                    "shadow_context": detail,
                },
            }
        )
    return out


def has_factor_lab_prior(row: dict[str, Any]) -> bool:
    details = safe_json_loads(row.get("details_json"))
    haystack = " ".join(
        str(x or "")
        for x in [
            row.get("primary_reason"),
            row.get("report_bucket"),
            details.get("factor_lab"),
            details.get("lab_factor"),
            details.get("lab_is_fresh"),
            details.get("shadow_alpha_score"),
            details.get("shadow_rank_score"),
        ]
    ).lower()
    return any(token in haystack for token in ["factor lab", "lab_", "lab factor", "shadow_", "true"])


def gate_passes(row: dict[str, Any]) -> bool:
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    row_passes = (
        normalize_bucket(row.get("report_bucket") or gate.get("report_bucket")) == "core"
        and normalize_confidence(row.get("signal_confidence")) == "high_mod"
        and normalize_direction(row.get("signal_direction") or gate.get("direction")) in {"long", "short"}
        and normalize_execution(
            row.get("execution_mode")
            or gate.get("execution_action")
            or gate.get("execution_mode")
            or gate.get("action_intent")
        )
        == "executable_now"
    )
    if gate:
        hard_blockers = [
            str(x)
            for x in gate.get("blockers", [])
            if not str(x).lower().startswith("headline_gate_")
            and "headline gate" not in str(x).lower()
        ]
        if hard_blockers:
            return False
        return (gate.get("status") == "pass" and gate.get("role") == "main_signal") or row_passes
    return row_passes


def tactical_gate_passes(row: dict[str, Any]) -> bool:
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    execution = row.get("execution_mode") or gate.get("execution_action") or gate.get("execution_mode")
    return (
        normalize_bucket(row.get("report_bucket") or gate.get("report_bucket")) == "theme_rotation"
        and normalize_confidence(row.get("signal_confidence")) == "high_mod"
        and normalize_direction(row.get("signal_direction") or gate.get("direction")) in {"long", "short"}
        and normalize_execution(execution) == "executable_now"
    )


def select_tactical_policy(candidates: list[dict[str, Any]]) -> str | None:
    tactical = [
        c
        for c in candidates
        if ":theme_rotation:" in str(c.get("policy_id") or "")
        and c.get("fail_reasons") == ["policy_bucket_not_core"]
        and (c.get("stability_score") is not None)
    ]
    if not tactical:
        return None
    return str(max(tactical, key=lambda c: float(c.get("stability_score") or 0.0))["policy_id"])


def candidate_blockers(row: dict[str, Any], selected_policy_id: str | None) -> list[str]:
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    blockers = [
        str(x)
        for x in gate.get("blockers", [])
        if not str(x).lower().startswith("headline_gate_")
        and "headline gate" not in str(x).lower()
    ] if gate else []
    if not selected_policy_id:
        blockers.append("EV unknown: no stable champion policy")
    elif row.get("policy_id") != selected_policy_id:
        blockers.append("EV unknown: outside selected champion policy")
    if normalize_bucket(row.get("report_bucket")) in {"radar", "appendix", "theme_rotation"}:
        blockers.append("strategy/out-of-scope")
    if normalize_execution(row.get("execution_mode") or gate.get("execution_mode")) == "wait_pullback":
        blockers.append("no fill risk")
    if normalize_execution(row.get("execution_mode") or gate.get("execution_mode")) == "do_not_chase":
        blockers.append("stale chase")
    rr = round_or_none(row.get("rr_ratio"))
    if rr is not None and rr < 1.5:
        blockers.append("RR insufficient")
    if not blockers and not gate_passes(row):
        blockers.append("main signal gate blocked")
    return dedupe(blockers)


def dedupe(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def bulletin_item(row: dict[str, Any], section: str, reason: str, blockers: list[str] | None = None) -> dict[str, Any]:
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    headline = str(row.get("headline_mode") or gate.get("headline_mode") or "").strip().lower() or None
    return {
        "market": row.get("market"),
        "symbol": row.get("symbol"),
        "section": section,
        "policy_id": row.get("policy_id"),
        "policy_label": row.get("policy_label"),
        "report_bucket": row.get("report_bucket"),
        "signal_direction": row.get("signal_direction"),
        "signal_confidence": row.get("signal_confidence"),
        "headline_mode": headline,
        "execution_mode": row.get("execution_mode"),
        "reason": reason,
        "blockers": blockers or [],
        "details": {
            "rank_order": row.get("rank_order"),
            "selection_status": row.get("selection_status"),
            "main_signal_gate": gate,
            "headline_context": {"mode": headline, "role": "context_only"},
        },
    }


def build_bulletin(
    as_of: date,
    evaluated_through: dict[str, str],
    selected_policies: dict[str, str | None],
    candidates_by_market: dict[str, list[dict[str, Any]]],
    current_by_market: dict[str, list[dict[str, Any]]],
    options_by_market: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    execution: list[dict[str, Any]] = []
    tactical: list[dict[str, Any]] = []
    options_alpha: list[dict[str, Any]] = []
    recall: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    options_by_market = options_by_market or {}
    tactical_policies = {
        market: select_tactical_policy(candidates)
        for market, candidates in candidates_by_market.items()
    }
    ev_status = {
        market: "passed" if selected_policies.get(market) else "failed"
        for market in candidates_by_market
    }

    for market, current_rows in current_by_market.items():
        selected_policy_id = selected_policies.get(market)
        tactical_policy_id = tactical_policies.get(market)
        for row in current_rows:
            blockers = candidate_blockers(row, selected_policy_id)
            if selected_policy_id and row.get("policy_id") == selected_policy_id and gate_passes(row):
                execution.append(
                    bulletin_item(
                        row,
                        "execution_alpha",
                        "selected champion policy with passing execution gate; headline context is advisory",
                    )
                )
            elif tactical_policy_id and row.get("policy_id") == tactical_policy_id and tactical_gate_passes(row):
                tactical.append(
                    bulletin_item(
                        row,
                        "tactical_alpha",
                        "stable theme-rotation policy; tactical follow-through only, not CORE BOOK execution alpha",
                        ["strategy/out-of-scope for core execution", "use pullback/liquidity confirmation"],
                    )
                )
            elif has_factor_lab_prior(row) or (
                selected_policy_id and row.get("policy_id") == selected_policy_id
            ):
                recall.append(
                    bulletin_item(
                        row,
                        "recall_alpha",
                        "Factor Lab research prior / recall lead; not promoted to Execution Alpha",
                        blockers,
                    )
                )
            else:
                blocked.append(
                    bulletin_item(
                        row,
                        "blocked_alpha",
                        "; ".join(blockers) if blockers else "outside execution-alpha scope",
                        blockers,
                    )
                )
        options_alpha.extend(options_by_market.get(market, []))

    stability = {
        market: [
            {
                "policy_id": c["policy_id"],
                "policy_label": c["policy_label"],
                "eligible": c["eligible"],
                "selected": c["selected"],
                "stability_score": c["stability_score"],
                "fills": c["fills"],
                "active_buckets": c["active_buckets"],
                "avg_trade_pct": c["avg_trade_pct"],
                "return_std_pct": c.get("return_std_pct"),
                "strict_win_rate": c["strict_win_rate"],
                "max_drawdown_pct": c["max_drawdown_pct"],
                "top1_winner_contribution": c["top1_winner_contribution"],
                "ev_probability_positive": c.get("ev_probability_positive"),
                "ev_lower_confidence_pct": c.get("ev_lower_confidence_pct"),
                "fills_required_for_95_lcb": c.get("fills_required_for_95_lcb"),
                "fail_reasons": c["fail_reasons"],
            }
            for c in candidates
        ]
        for market, candidates in candidates_by_market.items()
    }

    return {
        "as_of": as_of.isoformat(),
        "evaluated_through": evaluated_through,
        "ev_status": ev_status,
        "selected_policies": selected_policies,
        "tactical_policies": tactical_policies,
        "stability": stability,
        "execution_alpha": execution,
        "tactical_alpha": tactical,
        "options_alpha": options_alpha,
        "recall_alpha": recall,
        "blocked_alpha": blocked,
    }


def render_market_bulletin_md(bulletin: dict[str, Any], market: str) -> str:
    market_upper = market.upper()
    selected = bulletin["selected_policies"].get(market)
    tactical = bulletin.get("tactical_policies", {}).get(market)
    evaluated = bulletin["evaluated_through"].get(market, "unknown")
    ev_status = bulletin.get("ev_status", {}).get(
        market, "passed" if selected else "failed"
    )
    ev_note = {
        "passed": "stable champion selected; Execution Alpha may be emitted only for matching current candidates",
        "failed": "stable gate evaluated; no champion policy passed, so Setup/Recall names remain review-only",
        "pending": "stable gate not evaluated yet; do not treat pending as no champion or EV failure",
    }.get(ev_status, "stable gate status unknown; do not promote candidates without explicit pass")
    lines = [
        f"## {market_upper} Stable Alpha Bulletin",
        "",
        f"- as_of: {bulletin['as_of']}",
        f"- evaluated_through: {evaluated}",
        f"- ev_status: `{ev_status}`",
        f"- selected_policy: `{selected or 'none'}`",
        f"- tactical_policy: `{tactical or 'none'}`",
        f"- ev_note: {ev_note}",
        "- headline: advisory context only, not an execution blocker",
        "",
    ]
    for title, key, empty in [
        (
            "Equity Execution Alpha",
            "execution_alpha",
            "None. No current candidate passed both the stability champion and execution gates.",
        ),
        (
            "Tactical / Theme Rotation Alpha",
            "tactical_alpha",
            "None. No stable non-core theme-rotation candidate passed the tactical screen.",
        ),
        (
            "Options / Shadow Options Alpha",
            "options_alpha",
            "None. No real-options or shadow-options candidate passed the daily options-alpha screen.",
        ),
        (
            "Recall Alpha",
            "recall_alpha",
            "None. No Factor Lab research prior / recall lead requires follow-up.",
        ),
        (
            "Blocked / Out-of-scope Alpha",
            "blocked_alpha",
            "None. No blocked current candidates were found.",
        ),
    ]:
        lines += [f"### {title}", ""]
        items = [item for item in bulletin[key] if item.get("market") == market]
        if not items:
            lines += [f"- {empty}", ""]
            continue
        for item in items[:20]:
            if key == "options_alpha":
                blockers = item.get("blockers") or []
                blocker_text = f" Blockers: {', '.join(blockers)}." if blockers else ""
                details = item.get("details") or {}
                if item.get("source") == "real_options":
                    edge_text = (
                        f" directional_edge `{details.get('directional_edge')}`; "
                        f"vol_edge `{details.get('vol_edge')}`; vrp_edge `{details.get('vrp_edge')}`; "
                        f"flow_edge `{details.get('flow_edge')}`."
                    )
                else:
                    edge_text = (
                        f" shadow_alpha_prob `{details.get('shadow_alpha_prob')}`; "
                        f"entry_quality `{details.get('entry_quality_score')}`; "
                        f"stale_chase_risk `{details.get('stale_chase_risk')}`."
                    )
                lines.append(
                    f"- `{item.get('symbol')}` — {item.get('reason')}. "
                    f"Expression `{item.get('expression')}`; source `{item.get('source')}`;{edge_text}{blocker_text}"
                )
                continue
            blockers = item.get("blockers") or []
            blocker_text = f" Blockers: {', '.join(blockers)}." if blockers else ""
            headline = item.get("headline_mode")
            headline_text = f" Headline `{headline}` is context only." if headline else ""
            lines.append(
                f"- `{item.get('symbol')}` — {item.get('reason')}. "
                f"Policy `{item.get('policy_id')}`; lane `{item.get('report_bucket')}`; "
                f"confidence `{item.get('signal_confidence')}`.{headline_text}{blocker_text}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def ensure_result_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS playbook_candidates (
            as_of DATE,
            evaluated_through DATE,
            market VARCHAR,
            policy_id VARCHAR,
            policy_label VARCHAR,
            horizon_days INTEGER,
            lookback_days INTEGER,
            fills INTEGER,
            active_buckets INTEGER,
            avg_trade_pct DOUBLE,
            return_std_pct DOUBLE,
            median_trade_pct DOUBLE,
            strict_win_rate DOUBLE,
            max_drawdown_pct DOUBLE,
            top1_winner_contribution DOUBLE,
            ev_probability_positive DOUBLE,
            ev_lower_confidence_pct DOUBLE,
            fills_required_for_95_lcb INTEGER,
            stability_score DOUBLE,
            eligible BOOLEAN,
            fail_reasons VARCHAR,
            selected BOOLEAN,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    for column, column_type in [
        ("return_std_pct", "DOUBLE"),
        ("ev_probability_positive", "DOUBLE"),
        ("ev_lower_confidence_pct", "DOUBLE"),
        ("fills_required_for_95_lcb", "INTEGER"),
    ]:
        con.execute(f"ALTER TABLE playbook_candidates ADD COLUMN IF NOT EXISTS {column} {column_type}")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS playbook_selection (
            as_of DATE,
            evaluated_through DATE,
            market VARCHAR,
            selected_policy_id VARCHAR,
            previous_policy_id VARCHAR,
            stability_score DOUBLE,
            challenger_policy_id VARCHAR,
            challenger_score DOUBLE,
            selection_reason VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS alpha_bulletin (
            as_of DATE,
            market VARCHAR,
            section VARCHAR,
            symbol VARCHAR,
            policy_id VARCHAR,
            reason VARCHAR,
            blockers_json VARCHAR,
            payload_json VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS selected_trades (
            as_of DATE,
            market VARCHAR,
            policy_id VARCHAR,
            report_date DATE,
            evaluation_date DATE,
            symbol VARCHAR,
            return_pct DOUBLE,
            label VARCHAR,
            fill_quality VARCHAR,
            source_json VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS bucket_curve (
            as_of DATE,
            market VARCHAR,
            policy_id VARCHAR,
            bucket_date DATE,
            trade_count INTEGER,
            avg_return_pct DOUBLE,
            cumulative_return_pct DOUBLE,
            drawdown_pct DOUBLE
        )
        """
    )


def write_result_tables(
    db_path: Path,
    as_of: date,
    evaluated_through: dict[str, str],
    candidates_by_market: dict[str, list[dict[str, Any]]],
    selection_rows: list[dict[str, Any]],
    selected_trade_rows: list[dict[str, Any]],
    bulletin: dict[str, Any],
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        ensure_result_schema(con)
        as_of_s = as_of.isoformat()
        for table in [
            "playbook_candidates",
            "playbook_selection",
            "alpha_bulletin",
            "selected_trades",
            "bucket_curve",
        ]:
            con.execute(f"DELETE FROM {table} WHERE as_of = ?", [as_of_s])

        for market, candidates in candidates_by_market.items():
            for c in candidates:
                con.execute(
                    """
                    INSERT INTO playbook_candidates
                    (as_of, evaluated_through, market, policy_id, policy_label, horizon_days,
                     lookback_days, fills, active_buckets, avg_trade_pct, return_std_pct,
                     median_trade_pct, strict_win_rate, max_drawdown_pct, top1_winner_contribution,
                     ev_probability_positive, ev_lower_confidence_pct, fills_required_for_95_lcb,
                     stability_score, eligible, fail_reasons, selected)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        as_of_s,
                        evaluated_through.get(market),
                        market,
                        c["policy_id"],
                        c["policy_label"],
                        c["horizon_days"],
                        c["lookback_days"],
                        c["fills"],
                        c["active_buckets"],
                        c["avg_trade_pct"],
                        c.get("return_std_pct"),
                        c["median_trade_pct"],
                        c["strict_win_rate"],
                        c["max_drawdown_pct"],
                        c["top1_winner_contribution"],
                        c.get("ev_probability_positive"),
                        c.get("ev_lower_confidence_pct"),
                        c.get("fills_required_for_95_lcb"),
                        c["stability_score"],
                        c["eligible"],
                        json.dumps(c["fail_reasons"], ensure_ascii=False),
                        c["selected"],
                    ],
                )

        for row in selection_rows:
            con.execute(
                """
                INSERT INTO playbook_selection
                (as_of, evaluated_through, market, selected_policy_id, previous_policy_id,
                 stability_score, challenger_policy_id, challenger_score, selection_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    as_of_s,
                    evaluated_through.get(row["market"]),
                    row["market"],
                    row.get("selected_policy_id"),
                    row.get("previous_policy_id"),
                    row.get("stability_score"),
                    row.get("challenger_policy_id"),
                    row.get("challenger_score"),
                    row.get("selection_reason"),
                ],
            )

        for row in selected_trade_rows:
            con.execute(
                """
                INSERT INTO selected_trades
                (as_of, market, policy_id, report_date, evaluation_date, symbol, return_pct,
                 label, fill_quality, source_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    as_of_s,
                    row.get("market"),
                    row.get("policy_id"),
                    to_iso_date(row.get("report_date")),
                    to_iso_date(row.get("evaluation_date")),
                    row.get("symbol"),
                    row.get("return_pct"),
                    row.get("label"),
                    row.get("no_fill_reason") or ("filled" if is_fill(row) else "not_filled"),
                    json.dumps(row, default=str, ensure_ascii=False),
                ],
            )

        for row in build_bucket_curve_rows(as_of, selected_trade_rows):
            con.execute(
                """
                INSERT INTO bucket_curve
                (as_of, market, policy_id, bucket_date, trade_count, avg_return_pct,
                 cumulative_return_pct, drawdown_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    as_of_s,
                    row["market"],
                    row["policy_id"],
                    row["bucket_date"],
                    row["trade_count"],
                    row["avg_return_pct"],
                    row["cumulative_return_pct"],
                    row["drawdown_pct"],
                ],
            )

        for section in ["execution_alpha", "tactical_alpha", "options_alpha", "recall_alpha", "blocked_alpha"]:
            for item in bulletin[section]:
                con.execute(
                    """
                    INSERT INTO alpha_bulletin
                    (as_of, market, section, symbol, policy_id, reason, blockers_json, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        as_of_s,
                        item.get("market"),
                        section,
                        item.get("symbol"),
                        item.get("policy_id"),
                        item.get("reason"),
                        json.dumps(item.get("blockers") or [], ensure_ascii=False),
                        json.dumps(item, ensure_ascii=False, default=str),
                    ],
                )
        con.execute("CHECKPOINT")
    finally:
        con.close()


def build_bucket_curve_rows(as_of: date, trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, list[float]]] = {}
    for trade in trades:
        if not is_fill(trade):
            continue
        key = (str(trade.get("market")), str(trade.get("policy_id")))
        report_date = to_iso_date(trade.get("report_date"))
        if not report_date:
            continue
        grouped.setdefault(key, {}).setdefault(report_date, []).append(float(trade["return_pct"]))

    rows: list[dict[str, Any]] = []
    for (market, policy_id), by_date in grouped.items():
        cumulative = 0.0
        peak = 0.0
        for bucket_date, returns in sorted(by_date.items()):
            avg_ret = statistics.fmean(returns)
            cumulative += avg_ret
            peak = max(peak, cumulative)
            rows.append(
                {
                    "as_of": as_of.isoformat(),
                    "market": market,
                    "policy_id": policy_id,
                    "bucket_date": bucket_date,
                    "trade_count": len(returns),
                    "avg_return_pct": round_or_none(avg_ret, 6),
                    "cumulative_return_pct": round_or_none(cumulative, 6),
                    "drawdown_pct": round_or_none(cumulative - peak, 6),
                }
            )
    return rows


def write_bulletin_files(
    output_dir: Path,
    bulletin: dict[str, Any],
    *,
    write_project_copies: bool = True,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "alpha_bulletin.json").write_text(
        json.dumps(bulletin, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    market_text: dict[str, str] = {}
    for market in ["us", "cn"]:
        text = render_market_bulletin_md(bulletin, market)
        market_text[market] = text
        (output_dir / f"alpha_bulletin_{market}.md").write_text(text, encoding="utf-8")

    if write_project_copies:
        copies = {
            "us": STACK_ROOT / "quant-research-v1" / "reports" / "review_dashboard" / "strategy_backtest" / bulletin["as_of"],
            "cn": STACK_ROOT / "quant-research-cn" / "reports" / "review_dashboard" / "strategy_backtest" / bulletin["as_of"],
        }
        for market, path in copies.items():
            path.mkdir(parents=True, exist_ok=True)
            (path / f"alpha_bulletin_{market}.md").write_text(market_text[market], encoding="utf-8")


def strategy_report_md(
    as_of: date,
    evaluated_through: dict[str, str],
    candidates_by_market: dict[str, list[dict[str, Any]]],
    selected_policies: dict[str, str | None],
    ev_status: dict[str, str] | None = None,
) -> str:
    ev_status = ev_status or {
        market: "passed" if selected_policies.get(market) else "failed"
        for market in ["us", "cn"]
    }
    lines = [
        f"# Strategy Backtest Gate — {as_of.isoformat()}",
        "",
        "| Market | Evaluated through | EV status | Selected policy | Eligible / Total |",
        "|---|---|---|---|---:|",
    ]
    for market in ["us", "cn"]:
        candidates = candidates_by_market.get(market, [])
        eligible = sum(1 for c in candidates if c.get("eligible"))
        lines.append(
            f"| {market.upper()} | {evaluated_through.get(market, '-')} | "
            f"`{ev_status.get(market, 'unknown')}` | "
            f"`{selected_policies.get(market) or 'none'}` | {eligible} / {len(candidates)} |"
        )
    lines.append("")
    for market in ["us", "cn"]:
        lines += [f"## {market.upper()} Candidate Policies", ""]
        candidates = sorted(
            candidates_by_market.get(market, []),
            key=lambda c: (not c.get("selected"), not c.get("eligible"), -float(c.get("stability_score") or 0.0)),
        )
        if not candidates:
            lines += ["No evaluated policies found.", ""]
            continue
        lines += [
            "| Selected | Eligible | Policy | Fills | Active buckets | Avg % | P(EV>0) | EV LCB % | n for LCB>0 | Median % | Win | Max DD % | Top1 | Score | Fails |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
        for c in candidates[:30]:
            def fmt(value: Any) -> str:
                return "-" if value is None else str(value)

            lines.append(
                f"| {'yes' if c['selected'] else ''} | {'yes' if c['eligible'] else 'no'} | "
                f"`{c['policy_id']}` | {c['fills']} | {c['active_buckets']} | "
                f"{fmt(c['avg_trade_pct'])} | {fmt(c.get('ev_probability_positive'))} | "
                f"{fmt(c.get('ev_lower_confidence_pct'))} | {fmt(c.get('fills_required_for_95_lcb'))} | "
                f"{fmt(c['median_trade_pct'])} | {fmt(c['strict_win_rate'])} | {fmt(c['max_drawdown_pct'])} | "
                f"{fmt(c['top1_winner_contribution'])} | "
                f"{fmt(c['stability_score'])} | {', '.join(c['fail_reasons'])} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run(args: argparse.Namespace) -> dict[str, Any]:
    as_of = parse_iso_date(args.date)
    configs = [
        MarketConfig("us", args.us_db, args.us_horizon_days),
        MarketConfig("cn", args.cn_db, args.cn_horizon_days),
    ]

    evaluated_through: dict[str, str] = {}
    candidates_by_market: dict[str, list[dict[str, Any]]] = {}
    current_by_market: dict[str, list[dict[str, Any]]] = {}
    options_by_market: dict[str, list[dict[str, Any]]] = {}
    selected_policies: dict[str, str | None] = {}
    selection_rows: list[dict[str, Any]] = []
    selected_trade_rows: list[dict[str, Any]] = []

    for cfg in configs:
        rows, eval_through = load_evaluated_trades(
            cfg.db_path,
            cfg.market,
            as_of,
            args.lookback_days,
            cfg.horizon_days,
        )
        evaluated_through[cfg.market] = eval_through
        candidates = build_policy_candidates(rows, cfg.market, cfg.horizon_days, args.lookback_days)
        previous = load_previous_champion(args.history_db, cfg.market, as_of)
        selected = None
        reason = "auto-select disabled"
        if args.auto_select:
            selected, reason = select_champion(candidates, previous)
        mark_selected(candidates, selected)
        selected_policies[cfg.market] = selected
        candidates_by_market[cfg.market] = candidates
        current_by_market[cfg.market] = load_current_candidates(
            cfg.db_path,
            cfg.market,
            as_of,
            cfg.horizon_days,
        )
        options_by_market[cfg.market] = load_options_alpha_candidates(
            cfg.db_path,
            cfg.market,
            as_of,
        )

        selected_candidate = next((c for c in candidates if c.get("selected")), None)
        challenger = max(
            [c for c in candidates if c.get("eligible")],
            key=lambda c: float(c.get("stability_score") or 0.0),
            default=None,
        )
        selection_rows.append(
            {
                "market": cfg.market,
                "selected_policy_id": selected,
                "previous_policy_id": previous,
                "stability_score": (selected_candidate or {}).get("stability_score"),
                "challenger_policy_id": (challenger or {}).get("policy_id"),
                "challenger_score": (challenger or {}).get("stability_score"),
                "selection_reason": reason,
            }
        )
        if selected:
            selected_trade_rows.extend([row for row in rows if row.get("policy_id") == selected])

    bulletin = build_bulletin(
        as_of,
        evaluated_through,
        selected_policies,
        candidates_by_market,
        current_by_market,
        options_by_market,
    )

    output_dir = args.output_root / as_of.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_text = strategy_report_md(
        as_of,
        evaluated_through,
        candidates_by_market,
        selected_policies,
        bulletin.get("ev_status", {}),
    )
    (output_dir / "strategy_backtest_report.md").write_text(report_text, encoding="utf-8")
    write_result_tables(
        output_dir / "strategy_backtest.duckdb",
        as_of,
        evaluated_through,
        candidates_by_market,
        selection_rows,
        selected_trade_rows,
        bulletin,
    )
    write_result_tables(
        args.history_db,
        as_of,
        evaluated_through,
        candidates_by_market,
        selection_rows,
        selected_trade_rows,
        bulletin,
    )
    if args.emit_bulletin:
        write_bulletin_files(
            output_dir,
            bulletin,
            write_project_copies=args.output_root.resolve() == OUTPUT_ROOT.resolve(),
        )
    return bulletin


def main() -> None:
    args = parse_args()
    try:
        bulletin = run(args)
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"strategy backtest gate failed: {exc}", file=sys.stderr)
        raise
    print(f"Wrote strategy gate outputs for {bulletin['as_of']} to {args.output_root / bulletin['as_of']}")


if __name__ == "__main__":
    main()
