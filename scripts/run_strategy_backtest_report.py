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
ONE_SIDED_80_Z = 1.2816
US_TRADEABLE_OPTION_EXPRESSIONS = {"stock_long", "call_spread", "put_spread"}


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
    parser.add_argument("--lookback-days", type=int, default=60)
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


def nested_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def round_or_none(value: Any, digits: int = 4) -> float | None:
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(fval) or math.isinf(fval):
        return None
    return round(fval, digits)


def float_or(value: Any, default: float = 0.0) -> float:
    parsed = round_or_none(value, 8)
    return default if parsed is None else float(parsed)


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


def sql_in_placeholders(values: list[Any]) -> str:
    return ",".join("?" for _ in values)


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


def trend_regime_from_details(details: dict[str, Any]) -> str:
    gate = details.get("execution_gate") or {}
    if isinstance(gate, dict):
        regime = gate.get("trend_regime") or gate.get("regime")
        if regime:
            return str(regime).strip().lower()
    momentum = details.get("momentum") or {}
    if isinstance(momentum, dict) and momentum.get("regime"):
        return str(momentum.get("regime")).strip().lower()
    return "unknown"


def is_cn_oversold_ev_positive_row(row: dict[str, Any]) -> bool:
    family = str(row.get("strategy_family") or "").strip().lower()
    action = str(row.get("action_intent") or "").strip().upper()
    alpha_state = str(row.get("alpha_state") or "").strip().lower()
    ev_lcb80 = round_or_none(row.get("ev_lcb_80_pct"))
    return (
        family == "oversold_contrarian"
        and action == "TRADE"
        and (alpha_state == "positive_ev_setup" or (ev_lcb80 is not None and ev_lcb80 > 0.0))
    )


def cn_strategy_execution_mode(row: dict[str, Any]) -> str:
    features = safe_json_loads(row.get("features_json"))
    return str(features.get("execution_mode") or row.get("execution_mode") or row.get("execution_rule") or "").strip().lower()


def cn_strategy_hard_blocked(row: dict[str, Any]) -> bool:
    return cn_strategy_execution_mode(row) in {"blocked", "no_trade", "skip", "avoid"}


def row_policy(row: dict[str, Any], market: str, horizon_days: int) -> dict[str, str]:
    if market == "cn" and row.get("strategy_family"):
        family = str(row.get("strategy_family") or "unknown").strip().lower() or "unknown"
        action = str(row.get("action_intent") or "").strip().upper()
        direction = "long"
        if family == "oversold_contrarian":
            confidence = "ev_positive" if is_cn_oversold_ev_positive_row(row) else "ev_unproven"
        elif family == "structural_core":
            confidence = "legacy"
        else:
            confidence = "research"
        execution = "planned_entry" if action == "TRADE" else normalize_execution(row.get("execution_mode") or row.get("execution_rule"))
        regime = "na"
        policy_id = f"{market}:{family}:{direction}:{confidence}:{execution}:{regime}:h{horizon_days}"
        label = f"CN {family.replace('_', ' ')} {confidence.replace('_', ' ')} {execution.replace('_', ' ')} {horizon_days}D"
        return {
            "policy_id": policy_id,
            "policy_label": label,
            "bucket": family,
            "direction": direction,
            "confidence": confidence,
            "execution": execution,
            "trend_regime": regime,
        }

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
    regime = trend_regime_from_details(details) if market == "us" else "na"
    policy_id = f"{market}:{bucket}:{direction}:{confidence}:{execution}:{regime}:h{horizon_days}"
    label = (
        f"{market.upper()} {bucket.replace('_', ' ')} {direction} "
        f"{confidence.replace('_', '/')} {execution.replace('_', ' ')} "
        f"{regime.replace('_', ' ')} {horizon_days}D"
    )
    return {
        "policy_id": policy_id,
        "policy_label": label,
        "bucket": bucket,
        "direction": direction,
        "confidence": confidence,
        "execution": execution,
        "trend_regime": regime,
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
        if (
            market == "cn"
            and table_exists(con, "strategy_model_dataset")
        ):
            rows = load_cn_strategy_model_rows(con, start, cutoff, as_of)
        if (
            market == "us"
            and table_exists(con, "report_decisions")
            and table_exists(con, "report_outcomes")
        ):
            rows = load_report_outcome_rows(con, start, cutoff, as_of)
        if not rows and table_exists(con, "paper_trades"):
            rows = load_paper_trade_rows(con, start, cutoff, as_of)
        if (not rows or not any(is_fill(row) for row in rows)) and table_exists(con, "algorithm_postmortem"):
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


def load_paper_trade_rows(
    con: duckdb.DuckDBPyConnection,
    start: date,
    cutoff: date,
    as_of: date,
) -> list[dict[str, Any]]:
    p_cols = table_columns(con, "paper_trades")
    d_cols = table_columns(con, "report_decisions") if table_exists(con, "report_decisions") else set()
    if "realized_ret_pct" not in p_cols or "report_date" not in p_cols:
        return []
    join = (
        """
        LEFT JOIN report_decisions d
          ON p.report_date = d.report_date
         AND p.session = d.session
         AND p.symbol = d.symbol
         AND p.selection_status = d.selection_status
        """
        if d_cols
        else ""
    )
    select_parts = [
        sql_col("p", "report_date", p_cols),
        sql_coalesce([("p", "exit_date", p_cols), ("p", "evaluation_date", p_cols)], "evaluation_date"),
        sql_col("p", "symbol", p_cols),
        sql_col("p", "selection_status", p_cols),
        sql_coalesce([("d", "rank_order", d_cols)], "rank_order"),
        sql_coalesce([("d", "report_bucket", d_cols)], "report_bucket"),
        sql_coalesce([("d", "signal_direction", d_cols)], "signal_direction", default="'long'"),
        sql_coalesce([("d", "signal_confidence", d_cols)], "signal_confidence"),
        sql_coalesce([("d", "headline_mode", d_cols)], "headline_mode"),
        sql_coalesce([("d", "execution_mode", d_cols)], "execution_mode"),
        sql_coalesce([("d", "composite_score", d_cols)], "composite_score"),
        sql_coalesce([("d", "rr_ratio", d_cols)], "rr_ratio"),
        sql_coalesce([("d", "primary_reason", d_cols)], "primary_reason"),
        sql_col("p", "action_intent", p_cols),
        sql_col("p", "fill_status", p_cols),
        sql_col("p", "realized_ret_pct", p_cols, "return_pct"),
        sql_col("p", "max_favorable_pct", p_cols, "best_possible_ret_pct"),
        sql_col("p", "label", p_cols),
        sql_coalesce([("d", "details_json", d_cols), ("p", "detail_json", p_cols)], "details_json"),
    ]
    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM paper_trades p
        {join}
        WHERE p.report_date >= ? AND p.report_date <= ?
          AND (p.exit_date IS NULL OR p.exit_date <= ?)
        ORDER BY p.report_date, p.symbol
    """
    rows = rows_as_dicts(con, sql, [start.isoformat(), cutoff.isoformat(), as_of.isoformat()])
    for row in rows:
        details = safe_json_loads(row.get("details_json"))
        row["report_bucket"] = row.get("report_bucket") or details.get("report_bucket")
        row["signal_confidence"] = row.get("signal_confidence") or details.get("signal_confidence")
        row["execution_mode"] = row.get("execution_mode") or details.get("execution_mode")
        row["signal_direction"] = row.get("signal_direction") or details.get("direction") or "long"
        fill_status = str(row.get("fill_status") or "").lower()
        row["executable"] = bool(
            row.get("return_pct") is not None
            and fill_status in {"filled_open", "filled_pullback"}
            and str(row.get("action_intent") or "").upper() == "TRADE"
        )
    return rows


def load_cn_strategy_model_rows(
    con: duckdb.DuckDBPyConnection,
    start: date,
    cutoff: date,
    as_of: date,
) -> list[dict[str, Any]]:
    cols = table_columns(con, "strategy_model_dataset")
    if "realized_ret_pct" not in cols or "strategy_family" not in cols:
        return []
    rows = rows_as_dicts(
        con,
        """
        SELECT
            m.report_date,
            m.evaluation_date,
            m.symbol,
            m.selection_status,
            m.strategy_family,
            m.strategy_key,
            m.execution_rule,
            m.action_intent,
            m.alpha_state,
            m.realized_ret_pct AS return_pct,
            m.ev_pct,
            m.ev_lcb_80_pct,
            m.ev_lcb_95_pct,
            m.risk_unit_pct,
            m.ev_norm_score,
            m.ev_norm_lcb_80,
            m.detail_json AS details_json,
            m.features_json
        FROM strategy_model_dataset m
        WHERE m.report_date >= CAST(? AS DATE)
          AND m.report_date <= CAST(? AS DATE)
          AND (m.evaluation_date IS NULL OR m.evaluation_date <= CAST(? AS DATE))
          AND m.action_intent = 'TRADE'
          AND m.realized_ret_pct IS NOT NULL
        ORDER BY m.report_date, m.symbol
        """,
        [start.isoformat(), cutoff.isoformat(), as_of.isoformat()],
    )
    for row in rows:
        row["executable"] = True
    return rows


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
        lower = mean - ONE_SIDED_80_Z * se
        required = math.ceil((ONE_SIDED_80_Z * std / mean) ** 2) if mean > 0 else None
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
    fail_reasons.extend(policy_scope_fail_reasons(policy_id, market))
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


def policy_scope_fail_reasons(policy_id: str, market: str) -> list[str]:
    parts = policy_id.split(":")
    if len(parts) < 6:
        return ["policy_scope_unparseable"]
    _, bucket, direction, confidence, execution, *rest = parts
    regime = rest[0] if len(rest) >= 2 else "unknown"
    reasons: list[str] = []
    if market == "us":
        if bucket != "core":
            reasons.append("policy_bucket_not_core")
        if direction != "long":
            reasons.append("policy_direction_not_profit_long")
        if confidence not in {"low", "high_mod"}:
            reasons.append("policy_confidence_not_profit_scope")
        if execution != "executable_now":
            reasons.append("policy_execution_not_now")
        if regime != "trending":
            reasons.append("policy_regime_not_trending")
        return reasons
    if market == "cn":
        if bucket != "oversold_contrarian":
            reasons.append("policy_family_not_oversold_contrarian")
        if direction != "long":
            reasons.append("policy_direction_not_profit_long")
        if confidence != "ev_positive":
            reasons.append("policy_ev_lcb80_not_positive")
        if execution != "planned_entry":
            reasons.append("policy_execution_not_planned_entry")
        return reasons
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
        if market == "cn" and table_exists(con, "strategy_model_dataset"):
            rows = load_cn_current_strategy_candidates(con, as_of)
            for row in rows:
                row["market"] = market
                row["return_pct"] = None
                row.update(row_policy(row, market, horizon_days))
            return rows
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
        latest_row = con.execute(
            """
            SELECT MAX(report_date)
            FROM report_decisions
            WHERE report_date <= CAST(? AS DATE)
            """,
            [as_of.isoformat()],
        ).fetchone()
        latest = latest_row[0] if latest_row else None
        if latest is None:
            return []
        rows = rows_as_dicts(
            con,
            f"""
            SELECT {", ".join(select_parts)}
            FROM report_decisions d
            WHERE d.report_date = CAST(? AS DATE)
            ORDER BY {order_expr}, d.symbol
            """,
            [to_iso_date(latest)],
        )
    finally:
        con.close()
    for row in rows:
        row["market"] = market
        row["return_pct"] = None
        row.update(row_policy(row, market, horizon_days))
    return rows


def load_cn_current_strategy_candidates(con: duckdb.DuckDBPyConnection, as_of: date) -> list[dict[str, Any]]:
    latest_row = con.execute(
        "SELECT MAX(report_date) FROM strategy_model_dataset WHERE report_date <= CAST(? AS DATE)",
        [as_of.isoformat()],
    ).fetchone()
    latest = latest_row[0] if latest_row else None
    if latest is None:
        return []
    latest_iso = to_iso_date(latest)
    rows = rows_as_dicts(
        con,
        """
        SELECT
            m.report_date,
            m.evaluation_date,
            m.symbol,
            COALESCE(sb.name, '') AS name,
            m.selection_status,
            m.strategy_family,
            m.strategy_key,
            m.execution_rule,
            m.action_intent,
            m.alpha_state,
            m.ev_pct,
            m.ev_lcb_80_pct,
            m.ev_lcb_95_pct,
            m.risk_unit_pct,
            m.ev_norm_score,
            m.ev_norm_lcb_80,
            m.detail_json AS details_json,
            m.features_json
        FROM strategy_model_dataset m
        LEFT JOIN stock_basic sb ON sb.ts_code = m.symbol
        WHERE m.report_date = CAST(? AS DATE)
          AND m.evaluation_date = (
              SELECT MAX(evaluation_date)
              FROM strategy_model_dataset
              WHERE report_date = CAST(? AS DATE)
          )
          AND m.selection_status IN ('selected', 'exploration')
        ORDER BY
          CASE m.alpha_state
            WHEN 'positive_ev_setup' THEN 0
            WHEN 'blocked_negative_ev' THEN 2
            WHEN 'blocked_tail_risk' THEN 3
            ELSE 1
          END,
          CASE m.action_intent WHEN 'TRADE' THEN 0 WHEN 'SETUP' THEN 1 WHEN 'OBSERVE' THEN 2 ELSE 3 END,
          COALESCE(m.ev_norm_lcb_80, m.ev_norm_score, -999) DESC,
          m.symbol
        """,
        [latest_iso, latest_iso],
    )
    for row in rows:
        confidence = "EV_POSITIVE" if is_cn_oversold_ev_positive_row(row) else "EV_UNPROVEN"
        if str(row.get("strategy_family") or "").lower() == "structural_core":
            confidence = "LEGACY"
        row["report_bucket"] = row.get("strategy_family")
        row["signal_direction"] = "long"
        row["signal_confidence"] = confidence
        row["execution_mode"] = cn_strategy_execution_mode(row) or row.get("execution_rule")
        row["primary_reason"] = (
            "CN oversold_contrarian EV-positive planned-entry policy"
            if confidence == "EV_POSITIVE"
            else "CN strategy candidate outside EV-positive money policy"
        )
    return rows


def load_options_alpha_candidates(db_path: Path, market: str, as_of: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    if market == "us":
        return load_us_options_alpha_candidates(db_path, as_of)
    if market == "cn":
        return load_cn_shadow_options_alpha_candidates(db_path, as_of)
    return []


def load_recent_alpha_pulse(db_path: Path, market: str, as_of: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    if market == "us":
        return load_us_recent_alpha_pulse(db_path, as_of)
    if market == "cn":
        return load_cn_recent_alpha_pulse(db_path, as_of)
    return []


def load_learning_queue(db_path: Path, market: str, as_of: date) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "alpha_postmortem"):
            return []
        queue_labels = ["missed_alpha", "false_positive", "alpha_already_paid", "good_signal_bad_timing", "captured"]
        placeholders = sql_in_placeholders(queue_labels)
        summary_rows = rows_as_dicts(
            con,
            f"""
            SELECT
                label,
                COUNT(*) AS n,
                AVG(best_ret_pct) AS avg_best_ret_pct,
                AVG(CASE WHEN best_ret_pct > 0 THEN 1.0 ELSE 0.0 END) AS positive_best_rate,
                AVG(factor_feedback_weight) AS avg_feedback_weight
            FROM alpha_postmortem
            WHERE report_date <= CAST(? AS DATE)
              AND report_date >= CAST(? AS DATE) - INTERVAL 5 DAY
              AND label IN ({placeholders})
            GROUP BY 1
            ORDER BY
              CASE label
                WHEN 'missed_alpha' THEN 0
                WHEN 'false_positive' THEN 1
                WHEN 'alpha_already_paid' THEN 2
                WHEN 'good_signal_bad_timing' THEN 3
                WHEN 'captured' THEN 4
                ELSE 5
              END
            """,
            [as_of.isoformat(), as_of.isoformat(), *queue_labels],
        )
        examples = rows_as_dicts(
            con,
            f"""
            SELECT
                report_date,
                symbol,
                label,
                best_ret_pct,
                factor_feedback_action,
                factor_feedback_weight,
                review_note
            FROM alpha_postmortem
            WHERE report_date <= CAST(? AS DATE)
              AND report_date >= CAST(? AS DATE) - INTERVAL 5 DAY
              AND label IN ({placeholders})
            ORDER BY
              CASE label
                WHEN 'missed_alpha' THEN 0
                WHEN 'false_positive' THEN 1
                WHEN 'alpha_already_paid' THEN 2
                WHEN 'good_signal_bad_timing' THEN 3
                WHEN 'captured' THEN 4
                ELSE 5
              END,
              ABS(COALESCE(factor_feedback_weight, 0)) DESC,
              ABS(COALESCE(best_ret_pct, 0)) DESC,
              report_date DESC,
              symbol
            LIMIT 40
            """,
            [as_of.isoformat(), as_of.isoformat(), *queue_labels],
        )
    finally:
        con.close()

    examples_by_label: dict[str, list[dict[str, Any]]] = {}
    for row in examples:
        label = str(row.get("label") or "")
        examples_by_label.setdefault(label, []).append(
            {
                **row,
                "report_date": to_iso_date(row.get("report_date")),
                "best_ret_pct": round_or_none(row.get("best_ret_pct")),
                "factor_feedback_weight": round_or_none(row.get("factor_feedback_weight")),
            }
        )

    rows: list[dict[str, Any]] = []
    for row in summary_rows:
        label = str(row.get("label") or "")
        rows.append(
            {
                "market": market,
                "symbol": f"__{label.upper()}__",
                "section": "learning_queue",
                "label": label,
                "reason": learning_queue_task(label),
                "blockers": [],
                "details": {
                    "n": int(row.get("n") or 0),
                    "avg_best_ret_pct": round_or_none(row.get("avg_best_ret_pct")),
                    "positive_best_rate": round_or_none(row.get("positive_best_rate")),
                    "avg_feedback_weight": round_or_none(row.get("avg_feedback_weight")),
                    "examples": examples_by_label.get(label, [])[:6],
                },
            }
        )
    return rows


def learning_queue_task(label: str) -> str:
    return {
        "missed_alpha": "Find recall features that appeared before missed winners; do not invent unrelated factors.",
        "false_positive": "Find blockers that would have filtered losing promoted names before entry.",
        "alpha_already_paid": "Improve entry timing and anti-chase exits; this is not a new-factor task.",
        "good_signal_bad_timing": "Move from direction discovery to pullback/exit timing rules.",
        "captured": "Codify what repeated in captured winners and check whether it adds marginal Sharpe.",
    }.get(label, "Review this label before assigning Factor Lab work.")


def load_us_recent_alpha_pulse(db_path: Path, as_of: date) -> list[dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not (table_exists(con, "report_decisions") and table_exists(con, "report_outcomes")):
            return []
        date_rows = rows_as_dicts(
            con,
            """
            SELECT DISTINCT d.report_date
            FROM report_decisions d
            INNER JOIN report_outcomes o
              ON d.report_date = o.report_date
             AND d.session = o.session
             AND d.symbol = o.symbol
             AND d.selection_status = o.selection_status
            WHERE d.report_date <= CAST(? AS DATE)
              AND d.selection_status = 'selected'
              AND COALESCE(o.data_ready, TRUE)
              AND lower(COALESCE(d.signal_direction, '')) IN ('long', 'short')
              AND o.next_close_ret_pct IS NOT NULL
            ORDER BY d.report_date DESC
            LIMIT 2
            """,
            [as_of.isoformat()],
        )
        dates = [to_iso_date(row.get("report_date")) for row in date_rows if to_iso_date(row.get("report_date"))]
        if not dates:
            return []
        placeholders = sql_in_placeholders(dates)
        signed_next = """
            CASE
              WHEN lower(COALESCE(d.signal_direction, '')) = 'short' THEN -o.next_close_ret_pct
              ELSE o.next_close_ret_pct
            END
        """
        summary_rows = rows_as_dicts(
            con,
            f"""
            SELECT
                d.report_date,
                d.session,
                d.report_bucket,
                COUNT(*) AS n,
                AVG({signed_next}) AS avg_next_pct,
                AVG(CASE WHEN {signed_next} > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
                SUM(CASE WHEN {signed_next} >= 5.0 THEN 1 ELSE 0 END) AS big_winners,
                SUM(CASE WHEN {signed_next} <= -3.0 THEN 1 ELSE 0 END) AS big_drags
            FROM report_decisions d
            INNER JOIN report_outcomes o
              ON d.report_date = o.report_date
             AND d.session = o.session
             AND d.symbol = o.symbol
             AND d.selection_status = o.selection_status
            WHERE d.report_date IN ({placeholders})
              AND d.selection_status = 'selected'
              AND COALESCE(o.data_ready, TRUE)
              AND lower(COALESCE(d.signal_direction, '')) IN ('long', 'short')
              AND o.next_close_ret_pct IS NOT NULL
            GROUP BY 1, 2, 3
            ORDER BY 1 DESC, 2, 3
            """,
            dates,
        )
        leader_rows = rows_as_dicts(
            con,
            f"""
            SELECT
                d.report_date,
                d.session,
                d.symbol,
                d.report_bucket,
                d.signal_direction,
                d.signal_confidence,
                {signed_next} AS signed_next_pct
            FROM report_decisions d
            INNER JOIN report_outcomes o
              ON d.report_date = o.report_date
             AND d.session = o.session
             AND d.symbol = o.symbol
             AND d.selection_status = o.selection_status
            WHERE d.report_date IN ({placeholders})
              AND d.selection_status = 'selected'
              AND COALESCE(o.data_ready, TRUE)
              AND lower(COALESCE(d.signal_direction, '')) IN ('long', 'short')
              AND o.next_close_ret_pct IS NOT NULL
            ORDER BY signed_next_pct DESC, d.symbol
            LIMIT 8
            """,
            dates,
        )
        drag_rows = rows_as_dicts(
            con,
            f"""
            SELECT
                d.report_date,
                d.session,
                d.symbol,
                d.report_bucket,
                d.signal_direction,
                d.signal_confidence,
                {signed_next} AS signed_next_pct
            FROM report_decisions d
            INNER JOIN report_outcomes o
              ON d.report_date = o.report_date
             AND d.session = o.session
             AND d.symbol = o.symbol
             AND d.selection_status = o.selection_status
            WHERE d.report_date IN ({placeholders})
              AND d.selection_status = 'selected'
              AND COALESCE(o.data_ready, TRUE)
              AND lower(COALESCE(d.signal_direction, '')) IN ('long', 'short')
              AND o.next_close_ret_pct IS NOT NULL
            ORDER BY signed_next_pct ASC, d.symbol
            LIMIT 6
            """,
            dates,
        )
    finally:
        con.close()

    core_rows = [row for row in summary_rows if normalize_bucket(row.get("report_bucket")) == "core"]
    total_n = sum(int(row.get("n") or 0) for row in core_rows)
    weighted_core = (
        sum(float(row.get("avg_next_pct") or 0.0) * int(row.get("n") or 0) for row in core_rows) / total_n
        if total_n
        else None
    )
    top_gain = max((float(row.get("signed_next_pct") or 0.0) for row in leader_rows), default=0.0)
    if weighted_core is not None and weighted_core > 0.5:
        verdict = "recent US alpha pulse is positive, but it is concentrated; promote only clean core/options or stock-only leaders"
    elif top_gain >= 5.0:
        verdict = "recent US alpha pulse exists, but the core basket is noisy; use the pulse as a selector, not a basket trade"
    else:
        verdict = "recent US pulse is weak; require fresh confirmation before adding risk"
    return [
        {
            "market": "us",
            "symbol": "__US__",
            "section": "recent_alpha_pulse",
            "reason": verdict,
            "blockers": [],
            "details": {
                "basis": "last two report dates with data-ready T+1 outcomes; 3D hold may still be incomplete",
                "dates": dates,
                "core_weighted_avg_next_pct": round_or_none(weighted_core),
                "lane_summary": [
                    {
                        **row,
                        "report_date": to_iso_date(row.get("report_date")),
                        "avg_next_pct": round_or_none(row.get("avg_next_pct")),
                        "win_rate": round_or_none(row.get("win_rate")),
                    }
                    for row in summary_rows
                ],
                "leaders": [
                    {**row, "report_date": to_iso_date(row.get("report_date")), "signed_next_pct": round_or_none(row.get("signed_next_pct"))}
                    for row in leader_rows
                ],
                "drags": [
                    {**row, "report_date": to_iso_date(row.get("report_date")), "signed_next_pct": round_or_none(row.get("signed_next_pct"))}
                    for row in drag_rows
                ],
            },
        }
    ]


def load_cn_recent_alpha_pulse(db_path: Path, as_of: date) -> list[dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return []
        date_rows = rows_as_dicts(
            con,
            """
            SELECT DISTINCT report_date
            FROM strategy_model_dataset
            WHERE report_date <= CAST(? AS DATE)
              AND strategy_family = 'oversold_contrarian'
              AND action_intent = 'TRADE'
            ORDER BY report_date DESC
            LIMIT 2
            """,
            [as_of.isoformat()],
        )
        dates = [to_iso_date(row.get("report_date")) for row in date_rows if to_iso_date(row.get("report_date"))]
        if not dates:
            return []
        placeholders = sql_in_placeholders(dates)
        summary_rows = rows_as_dicts(
            con,
            f"""
            SELECT
                report_date,
                alpha_state,
                fill_status,
                COUNT(*) AS n,
                AVG(realized_ret_pct) AS avg_realized_pct,
                AVG(max_favorable_pct) AS avg_best_pct,
                AVG(
                    CASE
                      WHEN realized_ret_pct IS NULL THEN NULL
                      WHEN realized_ret_pct > 0 THEN 1.0
                      ELSE 0.0
                    END
                ) AS win_rate
            FROM strategy_model_dataset
            WHERE report_date IN ({placeholders})
              AND strategy_family = 'oversold_contrarian'
              AND action_intent = 'TRADE'
              AND alpha_state IN ('positive_ev_setup', 'research_setup', 'blocked_negative_ev')
            GROUP BY 1, 2, 3
            ORDER BY 1 DESC, 2, 3
            """,
            dates,
        )
        best_rows = rows_as_dicts(
            con,
            f"""
            SELECT
                report_date,
                symbol,
                alpha_state,
                fill_status,
                realized_ret_pct,
                max_favorable_pct,
                ev_lcb_80_pct,
                ev_norm_lcb_80
            FROM strategy_model_dataset
            WHERE report_date IN ({placeholders})
              AND strategy_family = 'oversold_contrarian'
              AND action_intent = 'TRADE'
              AND alpha_state IN ('positive_ev_setup', 'research_setup')
              AND max_favorable_pct IS NOT NULL
            ORDER BY max_favorable_pct DESC, symbol
            LIMIT 8
            """,
            dates,
        )
        pending_rows = rows_as_dicts(
            con,
            """
            SELECT
                report_date,
                symbol,
                alpha_state,
                fill_status,
                ev_lcb_80_pct,
                ev_norm_lcb_80,
                p_fill,
                mu_ret_pct,
                tail_loss_pct
            FROM strategy_model_dataset
            WHERE report_date = CAST(? AS DATE)
              AND strategy_family = 'oversold_contrarian'
              AND action_intent = 'TRADE'
              AND alpha_state IN ('positive_ev_setup', 'research_setup')
              AND fill_status = 'pending'
            ORDER BY
              CASE alpha_state WHEN 'positive_ev_setup' THEN 0 ELSE 1 END,
              COALESCE(ev_norm_lcb_80, -999) DESC,
              symbol
            LIMIT 8
            """,
            [dates[0]],
        )
    finally:
        con.close()

    filled = [row for row in summary_rows if str(row.get("fill_status") or "") == "filled_open"]
    total_n = sum(int(row.get("n") or 0) for row in filled)
    avg_best = (
        sum(float(row.get("avg_best_pct") or 0.0) * int(row.get("n") or 0) for row in filled) / total_n
        if total_n
        else None
    )
    avg_realized_rows = [row for row in filled if row.get("avg_realized_pct") is not None]
    total_realized_n = sum(int(row.get("n") or 0) for row in avg_realized_rows)
    avg_realized = (
        sum(float(row.get("avg_realized_pct") or 0.0) * int(row.get("n") or 0) for row in avg_realized_rows) / total_realized_n
        if total_realized_n
        else None
    )
    if avg_realized is not None and avg_realized > 0.5:
        verdict = "recent CN shadow-option pulse has realized alpha; keep execution/exit discipline ahead of more factor discovery"
    elif avg_best is not None and avg_best > 1.0:
        verdict = "recent CN shadow-option pulse has intraday/short-horizon convexity, but exits are not yet closed"
    else:
        verdict = "recent CN pulse is not confirmed; keep pending names as setup only"
    return [
        {
            "market": "cn",
            "symbol": "__CN__",
            "section": "recent_alpha_pulse",
            "reason": verdict,
            "blockers": [],
            "details": {
                "basis": "last two oversold_contrarian report dates; realized return can be pending while max_favorable shows short convexity",
                "dates": dates,
                "filled_avg_realized_pct": round_or_none(avg_realized),
                "filled_avg_best_pct": round_or_none(avg_best),
                "state_summary": [
                    {
                        **row,
                        "report_date": to_iso_date(row.get("report_date")),
                        "avg_realized_pct": round_or_none(row.get("avg_realized_pct")),
                        "avg_best_pct": round_or_none(row.get("avg_best_pct")),
                        "win_rate": round_or_none(row.get("win_rate")),
                    }
                    for row in summary_rows
                ],
                "best_realized_or_favorable": [
                    {
                        **row,
                        "report_date": to_iso_date(row.get("report_date")),
                        "realized_ret_pct": round_or_none(row.get("realized_ret_pct")),
                        "max_favorable_pct": round_or_none(row.get("max_favorable_pct")),
                        "ev_lcb_80_pct": round_or_none(row.get("ev_lcb_80_pct")),
                        "ev_norm_lcb_80": round_or_none(row.get("ev_norm_lcb_80")),
                    }
                    for row in best_rows
                ],
                "pending_watch": [
                    {
                        **row,
                        "report_date": to_iso_date(row.get("report_date")),
                        "ev_lcb_80_pct": round_or_none(row.get("ev_lcb_80_pct")),
                        "ev_norm_lcb_80": round_or_none(row.get("ev_norm_lcb_80")),
                        "p_fill": round_or_none(row.get("p_fill")),
                        "mu_ret_pct": round_or_none(row.get("mu_ret_pct")),
                        "tail_loss_pct": round_or_none(row.get("tail_loss_pct")),
                    }
                    for row in pending_rows
                ],
            },
        }
    ]


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
            ORDER BY
              CASE
                WHEN liquidity_gate = 'pass' AND expression IN ('call_spread', 'stock_long', 'put_spread') THEN 0
                WHEN liquidity_gate = 'pass' THEN 1
                ELSE 2
              END,
              ABS(COALESCE(directional_edge, 0)) + ABS(COALESCE(vol_edge, 0)) DESC,
              symbol
            LIMIT 1000
            """,
            [as_of.isoformat()],
        )
    finally:
        con.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        detail = safe_json_loads(row.get("detail_json"))
        expression = str(row.get("expression") or "").lower()
        liquidity = str(row.get("liquidity_gate") or "").lower()
        blockers: list[str] = []
        if liquidity != "pass":
            blockers.append(row.get("reason") or f"options liquidity gate {liquidity or 'missing'}")
        elif expression not in US_TRADEABLE_OPTION_EXPRESSIONS:
            blockers.append(row.get("reason") or f"options expression {expression or 'missing'}")
        out.append(
            {
                "market": "us",
                "symbol": row.get("symbol"),
                "section": "options_alpha",
                "source": "real_options",
                "expression": row.get("expression"),
                "reason": row.get("reason") or "real-options edge candidate",
                "blockers": blockers,
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


def is_tradeable_us_options_alpha(item: dict[str, Any]) -> bool:
    expression = str(item.get("expression") or "").lower()
    details = item.get("details") or {}
    liquidity = str(details.get("liquidity_gate") or "").lower()
    return expression in US_TRADEABLE_OPTION_EXPRESSIONS and liquidity == "pass"


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


def long_options_expression_pass(item: dict[str, Any] | None) -> tuple[bool, str]:
    if not item:
        return True, "options expression missing; use stock-only expression"
    expression = str(item.get("expression") or "").lower()
    details = item.get("details") or {}
    liquidity = str(details.get("liquidity_gate") or "").lower()
    directional = round_or_none(details.get("directional_edge"))
    vol_edge = round_or_none(details.get("vol_edge"))
    if liquidity and liquidity != "pass":
        return True, f"options liquidity {liquidity}; use stock-only expression"
    if expression == "call_spread" and (directional or 0.0) > 0.0 and (vol_edge or 0.0) > 0.0:
        return True, "call_spread direction+vol edge passed"
    if expression == "stock_long" and (directional or 0.0) > 0.0:
        return True, "stock_long direction edge passed; options not cheap enough"
    if expression == "put_spread" or (directional is not None and directional < -0.35):
        return False, f"expression {expression} conflicts with a long equity ticket"
    if expression in {"wait", "blocked"}:
        return True, f"expression {expression} is not a long option expression; use stock-only"
    return True, "options direction/vol edge weak; use stock-only expression"


def is_us_core_options_cross_candidate(row: dict[str, Any]) -> bool:
    selection = str(row.get("selection_status") or "selected").strip().lower()
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    execution = normalize_execution(
        row.get("execution_mode")
        or gate.get("execution_action")
        or gate.get("execution_mode")
        or gate.get("action_intent")
    )
    return (
        selection in {"selected", "ignored", "trade", "active"}
        and normalize_bucket(row.get("report_bucket") or gate.get("report_bucket")) == "core"
        and normalize_direction(row.get("signal_direction") or gate.get("direction")) in {"long", "short"}
        and execution == "executable_now"
    )


def us_core_options_cross(row: dict[str, Any], item: dict[str, Any] | None) -> dict[str, Any]:
    direction = normalize_direction(row.get("signal_direction"))
    if not item:
        return {
            "tier": "stock_only_unconfirmed",
            "execution_pass": True,
            "expression": "missing",
            "liquidity_gate": "missing",
            "directional_edge": None,
            "vol_edge": None,
            "flow_edge": None,
            "reason": "options row missing; keep the core equity ticket stock-only and do not use options",
            "blockers": ["options_missing_for_expression"],
        }

    details = item.get("details") or {}
    expression = str(item.get("expression") or "").lower()
    liquidity = str(details.get("liquidity_gate") or "").lower()
    directional = round_or_none(details.get("directional_edge"))
    vol_edge = round_or_none(details.get("vol_edge"))
    flow_edge = round_or_none(details.get("flow_edge"))
    reason = str(item.get("reason") or "").strip()

    base = {
        "expression": expression or "missing",
        "liquidity_gate": liquidity or "missing",
        "directional_edge": directional,
        "vol_edge": vol_edge,
        "flow_edge": flow_edge,
    }

    if liquidity != "pass":
        return {
            **base,
            "tier": "stock_only_unconfirmed",
            "execution_pass": True,
            "reason": f"options liquidity failed ({reason or liquidity or 'unknown'}); use stock-only expression",
            "blockers": [reason or "options_liquidity_failed"],
        }

    dir_val = directional or 0.0
    vol_val = vol_edge or 0.0
    if direction == "long":
        if expression == "call_spread" and dir_val > 0.0 and vol_val > 0.0:
            return {
                **base,
                "tier": "core_options_confirmed",
                "execution_pass": True,
                "reason": "core long and call-spread tape agree; options expression may be used",
                "blockers": [],
            }
        if expression == "stock_long" and dir_val > 0.0:
            return {
                **base,
                "tier": "core_stock_only_options_overpaid",
                "execution_pass": True,
                "reason": "core long is confirmed directionally, but listed options look overpaid; use stock-only",
                "blockers": ["options_overpaid_for_spread"],
            }
        if expression == "put_spread" or dir_val < -0.35:
            return {
                **base,
                "tier": "core_options_conflict",
                "execution_pass": False,
                "reason": "core long conflicts with bearish options tape; do not promote without manual override",
                "blockers": ["core/options direction conflict"],
            }
        return {
            **base,
            "tier": "stock_only_unconfirmed",
            "execution_pass": True,
            "reason": f"core long remains a stock-only ticket; options expression is {expression or 'missing'} ({reason or 'not clean'})",
            "blockers": [reason or "options_not_clean_for_long"],
        }

    if direction == "short":
        if expression == "put_spread" and dir_val < 0.0 and vol_val > 0.0:
            return {
                **base,
                "tier": "core_options_confirmed_short",
                "execution_pass": True,
                "reason": "core short and put-spread tape agree; options expression may be used",
                "blockers": [],
            }
        if expression in {"call_spread", "stock_long"} or dir_val > 0.35:
            return {
                **base,
                "tier": "core_options_conflict",
                "execution_pass": False,
                "reason": "core short conflicts with bullish options tape; do not promote without manual override",
                "blockers": ["core/options direction conflict"],
            }
        return {
            **base,
            "tier": "stock_only_unconfirmed",
            "execution_pass": True,
            "reason": f"core short remains a stock-only ticket; options expression is {expression or 'missing'} ({reason or 'not clean'})",
            "blockers": [reason or "options_not_clean_for_short"],
        }

    return {
        **base,
        "tier": "core_options_conflict",
        "execution_pass": False,
        "reason": "core/options cross requires a long or short core direction",
        "blockers": ["core direction missing"],
    }


def core_options_cross_bulletin_item(row: dict[str, Any], cross: dict[str, Any]) -> dict[str, Any]:
    item = bulletin_item(
        row,
        "core_options_cross",
        str(cross.get("reason") or "core/options cross classified"),
        list(cross.get("blockers") or []),
    )
    item["tier"] = cross.get("tier")
    item["expression"] = cross.get("expression")
    item["details"]["core_options_cross"] = cross
    return item


def fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    return "-" if parsed is None else f"{parsed:+.{digits}f}%"


def fmt_rate(value: Any, digits: int = 1) -> str:
    parsed = round_or_none(value, 6)
    return "-" if parsed is None else f"{parsed * 100.0:.{digits}f}%"


def render_recent_alpha_pulse_md(bulletin: dict[str, Any], market: str) -> list[str]:
    items = [item for item in bulletin.get("recent_alpha_pulse", []) if item.get("market") == market]
    if not items:
        return [
            "### Recent Alpha Pulse",
            "",
            "- No recent outcome pulse was available from the local ledgers.",
            "",
        ]
    item = items[0]
    details = item.get("details") or {}
    lines = [
        "### Recent Alpha Pulse",
        "",
        f"- Verdict: {item.get('reason')}",
        f"- Basis: {details.get('basis')}",
        "",
    ]
    if market == "us":
        lines += [
            "| Date | Session | Lane | N | Avg T+1 | Win | Big winners | Big drags |",
            "|------|---------|------|--:|--------:|----:|------------:|----------:|",
        ]
        for row in (details.get("lane_summary") or [])[:10]:
            lines.append(
                "| {date} | {session} | {lane} | {n} | {avg} | {win} | {winners} | {drags} |".format(
                    date=row.get("report_date") or "-",
                    session=row.get("session") or "-",
                    lane=row.get("report_bucket") or "-",
                    n=int(row.get("n") or 0),
                    avg=fmt_pct(row.get("avg_next_pct")),
                    win=fmt_rate(row.get("win_rate")),
                    winners=int(row.get("big_winners") or 0),
                    drags=int(row.get("big_drags") or 0),
                )
            )
        lines += [
            "",
            "- Leaders: " + _pulse_symbol_list(details.get("leaders"), value_key="signed_next_pct"),
            "- Drags: " + _pulse_symbol_list(details.get("drags"), value_key="signed_next_pct"),
            "",
        ]
        return lines

    lines += [
        "| Date | State | Fill | N | Avg realized | Avg best | Win |",
        "|------|-------|------|--:|-------------:|---------:|----:|",
    ]
    for row in (details.get("state_summary") or [])[:12]:
        lines.append(
            "| {date} | {state} | {fill} | {n} | {realized} | {best} | {win} |".format(
                date=row.get("report_date") or "-",
                state=row.get("alpha_state") or "-",
                fill=row.get("fill_status") or "-",
                n=int(row.get("n") or 0),
                realized=fmt_pct(row.get("avg_realized_pct")),
                best=fmt_pct(row.get("avg_best_pct")),
                win=fmt_rate(row.get("win_rate")),
            )
        )
    lines += [
        "",
        "- Best realized/favorable: "
        + _pulse_symbol_list(details.get("best_realized_or_favorable"), value_key="max_favorable_pct"),
        "- Pending watch: "
        + _pulse_symbol_list(details.get("pending_watch"), value_key="ev_norm_lcb_80", pct=False),
        "",
    ]
    return lines


def _pulse_symbol_list(rows: Any, *, value_key: str, pct: bool = True, limit: int = 6) -> str:
    if not isinstance(rows, list) or not rows:
        return "-"
    parts: list[str] = []
    for row in rows[:limit]:
        symbol = str(row.get("symbol") or "-")
        date_s = str(row.get("report_date") or "")
        value = fmt_pct(row.get(value_key)) if pct else str(row.get(value_key) if row.get(value_key) is not None else "-")
        parts.append(f"`{symbol}` {value} ({date_s})")
    return ", ".join(parts)


def md_cell(value: Any) -> str:
    text = str(value if value is not None else "-").replace("\n", " ").replace("|", "/").strip()
    return text or "-"


def render_execution_candidates_md(bulletin: dict[str, Any], market: str) -> list[str]:
    items = [item for item in bulletin.get("execution_candidates", []) if item.get("market") == market]
    lines = ["### Execution Candidates", ""]
    if not items:
        return [
            *lines,
            "- None. No current candidate has enough pulse/execution evidence for today's action list.",
            "",
        ]

    priority = {
        "execute_option_confirmed_probe": 0,
        "execute_stock_only_probe": 1,
        "planned_entry_probe": 2,
        "stock_only_probe_if_other_gates_pass": 3,
        "wait_pullback_probe": 4,
        "do_not_promote_conflict": 5,
        "research_shadow_probe": 6,
        "setup_watch_only": 7,
        "observe_or_wait": 7,
        "wait_reset": 8,
        "do_not_chase_wait_reset": 9,
        "observe_only": 11,
    }
    tier_priority = {
        "core_options_confirmed": 0,
        "core_options_confirmed_short": 0,
        "core_stock_only_options_overpaid": 1,
        "core_options_conflict": 2,
        "positive_ev_setup": 3,
        "research_setup": 4,
        "stock_only_unconfirmed": 5,
    }
    items = sorted(
        items,
        key=lambda item: (
            priority.get(str(item.get("action") or ""), 99),
            1 if "outside selected stable EV policy" in (item.get("blockers") or []) else 0,
            tier_priority.get(str(item.get("tier") or ""), 99),
            str(item.get("symbol") or ""),
        ),
    )
    lines += [
        "| Symbol | Action | Tier | Expression | Why | Blockers |",
        "|--------|--------|------|------------|-----|----------|",
    ]
    for item in items[:14]:
        details = (item.get("details") or {}).get("execution_candidate") or {}
        why = str(item.get("reason") or "")
        if market == "us":
            metrics = [
                f"paid_risk={details.get('paid_risk')}",
                f"rr={details.get('rr_ratio')}",
            ]
            why = f"{why}; " + ", ".join(metric for metric in metrics if not metric.endswith("=None"))
        elif market == "cn":
            metrics = [
                f"shadow_prob={details.get('shadow_alpha_prob')}",
                f"entry={details.get('entry_quality_score')}",
                f"stale={details.get('stale_chase_risk')}",
            ]
            why = f"{why}; " + ", ".join(metric for metric in metrics if not metric.endswith("=None"))
        blockers = item.get("blockers") or []
        lines.append(
            "| {symbol} | `{action}` | `{tier}` | `{expression}` | {why} | {blockers} |".format(
                symbol=f"`{md_cell(item.get('symbol'))}`",
                action=md_cell(item.get("action")),
                tier=md_cell(item.get("tier")),
                expression=md_cell(item.get("expression")),
                why=md_cell(why),
                blockers=md_cell(", ".join(blockers) if blockers else "none"),
            )
        )
    lines.append("")
    return lines


def render_learning_queue_md(bulletin: dict[str, Any], market: str) -> list[str]:
    items = [item for item in bulletin.get("learning_queue", []) if item.get("market") == market]
    lines = ["### Learning Queue", ""]
    if not items:
        return [
            *lines,
            "- None. No recent postmortem labels require Factor Lab follow-up.",
            "",
        ]

    label_priority = {
        "missed_alpha": 0,
        "false_positive": 1,
        "alpha_already_paid": 2,
        "good_signal_bad_timing": 3,
        "captured": 4,
    }
    items = sorted(
        items,
        key=lambda item: (
            label_priority.get(str(item.get("label") or ""), 99),
            -int((item.get("details") or {}).get("n") or 0),
        ),
    )
    lines += [
        "| Label | N | Avg best | Positive best | Task | Examples |",
        "|-------|--:|---------:|--------------:|------|----------|",
    ]
    for item in items[:8]:
        details = item.get("details") or {}
        examples: list[str] = []
        for example in (details.get("examples") or [])[:4]:
            examples.append(
                "`{symbol}` {best} ({date})".format(
                    symbol=md_cell(example.get("symbol")),
                    best=fmt_pct(example.get("best_ret_pct")),
                    date=md_cell(example.get("report_date")),
                )
            )
        lines.append(
            "| `{label}` | {n} | {avg_best} | {positive_best} | {task} | {examples} |".format(
                label=md_cell(item.get("label")),
                n=int(details.get("n") or 0),
                avg_best=fmt_pct(details.get("avg_best_ret_pct")),
                positive_best=fmt_rate(details.get("positive_best_rate")),
                task=md_cell(item.get("reason")),
                examples=md_cell(", ".join(examples) if examples else "-"),
            )
        )
    lines.append("")
    return lines


def recent_pulse_for_market(
    recent_alpha_pulse_by_market: dict[str, list[dict[str, Any]]],
    market: str,
) -> dict[str, Any] | None:
    rows = recent_alpha_pulse_by_market.get(market) or []
    return rows[0] if rows else None


def recent_pulse_is_positive(pulse: dict[str, Any] | None, market: str) -> bool:
    details = (pulse or {}).get("details") or {}
    if market == "us":
        return float_or(details.get("core_weighted_avg_next_pct"), 0.0) > 0.5
    if market == "cn":
        return (
            float_or(details.get("filled_avg_realized_pct"), 0.0) > 0.5
            or float_or(details.get("filled_avg_best_pct"), 0.0) > 1.0
        )
    return False


def build_us_execution_candidate_item(
    row: dict[str, Any],
    cross: dict[str, Any] | None,
    *,
    selected_policy_id: str | None,
    recent_pulse: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not is_us_core_options_cross_candidate(row):
        return None
    cross = cross or us_core_options_cross(row, None)
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    overnight = details.get("overnight_alpha") if isinstance(details.get("overnight_alpha"), dict) else {}
    execution_gate = details.get("execution_gate") if isinstance(details.get("execution_gate"), dict) else {}
    cross_tier = str(cross.get("tier") or "")
    expression = str(cross.get("expression") or "-")
    policy_id = str(row.get("policy_id") or "")
    if policy_id != (selected_policy_id or "") and cross_tier == "stock_only_unconfirmed":
        return None
    paid_risk = max(
        float_or(overnight.get("alpha_already_paid_risk"), 0.0),
        float_or(overnight.get("fade_or_paid_risk"), 0.0),
        float_or(execution_gate.get("effective_stretch_score"), 0.0),
    )
    rr = round_or_none(row.get("rr_ratio"), 4)
    blockers: list[str] = []
    notes: list[str] = []

    if not recent_pulse_is_positive(recent_pulse, "us"):
        blockers.append("recent_alpha_pulse_not_positive")
    if cross_tier == "core_options_conflict":
        blockers.append("core/options direction conflict")
    if selected_policy_id and policy_id != selected_policy_id:
        blockers.append("outside selected stable EV policy")
    if str(gate.get("status") or "").lower() not in {"pass", ""}:
        notes.extend(str(x) for x in (gate.get("blockers") or [])[:2])
    if paid_risk >= 0.45 or str(execution_gate.get("action") or "").lower() == "do_not_chase":
        blockers.append("alpha_already_paid_or_chase_risk")
    if rr is not None and rr < 1.2:
        blockers.append("rr_below_probe_floor")

    if cross_tier in {"core_options_confirmed", "core_options_confirmed_short"}:
        action = "execute_option_confirmed_probe"
    elif cross_tier == "core_stock_only_options_overpaid":
        action = "execute_stock_only_probe"
        notes.append("options overpaid; no option money")
    elif cross_tier == "stock_only_unconfirmed":
        action = "stock_only_probe_if_other_gates_pass"
        notes.append("options unconfirmed; do not use options")
    else:
        action = "observe_only"

    if blockers:
        action = "observe_or_wait"
        if "core/options direction conflict" in blockers:
            action = "do_not_promote_conflict"
        elif "alpha_already_paid_or_chase_risk" in blockers:
            action = "wait_reset"

    item = bulletin_item(
        row,
        "execution_candidates",
        "; ".join(notes) if notes else str(cross.get("reason") or "US execution candidate classified"),
        dedupe(blockers),
    )
    item["action"] = action
    item["tier"] = cross_tier or "unclassified"
    item["expression"] = expression
    item["details"]["execution_candidate"] = {
        "recent_pulse_positive": recent_pulse_is_positive(recent_pulse, "us"),
        "cross": cross,
        "paid_risk": round_or_none(paid_risk),
        "rr_ratio": rr,
        "stable_policy_selected": selected_policy_id,
        "policy_id": policy_id,
    }
    return item


def build_cn_execution_candidate_item(
    row: dict[str, Any],
    *,
    recent_pulse: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if str(row.get("strategy_family") or "").lower() != "oversold_contrarian":
        return None
    if str(row.get("action_intent") or "").upper() != "TRADE":
        return None
    alpha_state = str(row.get("alpha_state") or "")
    if alpha_state not in {"positive_ev_setup", "research_setup"}:
        return None
    features = safe_json_loads(row.get("features_json"))
    detail = safe_json_loads(row.get("details_json"))
    shadow = nested_get(features, "details", "shadow_option_alpha", default={})
    shadow_prob = float_or(features.get("shadow_alpha_prob") or shadow.get("shadow_alpha_prob"), 0.0)
    entry_quality = float_or(
        features.get("entry_quality_score")
        or nested_get(features, "details", "entry_quality_score")
        or shadow.get("entry_quality_score")
        or features.get("setup_score"),
        0.0,
    )
    stale_risk = float_or(features.get("stale_chase_risk") or shadow.get("stale_chase_risk"), 1.0)
    execution_mode = str(features.get("execution_mode") or row.get("execution_mode") or row.get("execution_rule") or "").lower()
    ev_lcb = round_or_none(row.get("ev_lcb_80_pct"))
    ev_norm = round_or_none(row.get("ev_norm_lcb_80"))

    blockers: list[str] = []
    if not recent_pulse_is_positive(recent_pulse, "cn"):
        blockers.append("recent_shadow_pulse_not_confirmed")
    if execution_mode == "do_not_chase" or stale_risk >= 0.55:
        blockers.append("stale_chase_or_do_not_chase")
    if alpha_state != "positive_ev_setup" and shadow_prob < 0.30:
        blockers.append("shadow_alpha_prob<0.30")
    if entry_quality < 0.30:
        blockers.append("entry_quality_low")

    if alpha_state == "positive_ev_setup" and execution_mode == "wait_pullback":
        action = "wait_pullback_probe"
    elif alpha_state == "positive_ev_setup":
        action = "planned_entry_probe"
    elif not blockers and shadow_prob >= 0.30 and entry_quality >= 0.38 and stale_risk <= 0.40:
        action = "research_shadow_probe"
    else:
        action = "setup_watch_only"
    if "stale_chase_or_do_not_chase" in blockers:
        action = "do_not_chase_wait_reset"

    item = {
        "market": "cn",
        "symbol": row.get("symbol"),
        "section": "execution_candidates",
        "policy_id": row.get("policy_id"),
        "policy_label": row.get("policy_label"),
        "report_bucket": row.get("report_bucket"),
        "signal_direction": row.get("signal_direction"),
        "signal_confidence": row.get("signal_confidence"),
        "headline_mode": None,
        "execution_mode": execution_mode,
        "action": action,
        "tier": alpha_state,
        "expression": "shadow_option_stock",
        "reason": "CN shadow-option execution candidate; exit discipline matters more than new factor discovery",
        "blockers": dedupe(blockers),
        "details": {
            "execution_candidate": {
                "recent_pulse_positive": recent_pulse_is_positive(recent_pulse, "cn"),
                "alpha_state": alpha_state,
                "ev_lcb_80_pct": ev_lcb,
                "ev_norm_lcb_80": ev_norm,
                "shadow_alpha_prob": round_or_none(shadow_prob),
                "entry_quality_score": round_or_none(entry_quality),
                "stale_chase_risk": round_or_none(stale_risk),
                "exit_rule": "next_open_or_pullback; trim fast into max_favorable; time stop if no follow-through within 2 sessions",
                "detail": detail,
            }
        },
    }
    return item


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
    market = str(row.get("market") or "").lower()
    if market == "cn" and row.get("strategy_family"):
        return is_cn_oversold_ev_positive_row(row) and not cn_strategy_hard_blocked(row)
    if market == "us":
        trend_regime = trend_regime_from_details(details)
        return (
            normalize_bucket(row.get("report_bucket") or gate.get("report_bucket")) == "core"
            and normalize_confidence(row.get("signal_confidence")) in {"low", "high_mod"}
            and normalize_direction(row.get("signal_direction") or gate.get("direction")) == "long"
            and normalize_execution(
                row.get("execution_mode")
                or gate.get("execution_action")
                or gate.get("execution_mode")
                or gate.get("action_intent")
            )
            == "executable_now"
            and trend_regime == "trending"
        )
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


def select_positive_ev_research_policy(candidates: list[dict[str, Any]]) -> str | None:
    """Surface policies that pass EV evidence but miss only report-scope labels.

    These are not Execution Alpha champions. They are statistically interesting
    recall/setup policies that would otherwise disappear behind legacy scope
    labels such as LOW confidence.
    """
    scope_only = {
        "policy_bucket_not_core",
        "policy_confidence_not_high_mod",
        "policy_not_v2_low_confidence",
        "policy_confidence_not_profit_scope",
        "policy_execution_not_now",
    }
    research: list[dict[str, Any]] = []
    for c in candidates:
        reasons = set(c.get("fail_reasons") or [])
        if not reasons or not reasons.issubset(scope_only):
            continue
        if "policy_direction_not_tradeable" in reasons:
            continue
        if c.get("ev_lower_confidence_pct") is None or float(c.get("ev_lower_confidence_pct") or 0.0) <= 0.0:
            continue
        if c.get("fills", 0) <= 0:
            continue
        research.append(c)
    if not research:
        return None
    return str(max(research, key=lambda c: (float(c.get("ev_lower_confidence_pct") or 0.0), float(c.get("stability_score") or 0.0)))["policy_id"])


def candidate_blockers(row: dict[str, Any], selected_policy_id: str | None) -> list[str]:
    details = safe_json_loads(row.get("details_json"))
    gate = main_signal_gate(details)
    market = str(row.get("market") or "").lower()
    blockers = [
        str(x)
        for x in gate.get("blockers", [])
        if not str(x).lower().startswith("headline_gate_")
        and "headline gate" not in str(x).lower()
    ] if gate else []
    if market == "us" and row.get("policy_id") == selected_policy_id:
        blockers = [
            blocker
            for blocker in blockers
            if str(blocker).lower() not in {"confidence_low", "signal_confidence_low"}
        ]
    if not selected_policy_id:
        blockers.append("stable EV gate not passed")
    elif row.get("policy_id") != selected_policy_id:
        blockers.append("outside selected stable EV policy")
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
    recent_alpha_pulse_by_market: dict[str, list[dict[str, Any]]] | None = None,
    ev_status_override: dict[str, str] | None = None,
    learning_queue_by_market: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    execution: list[dict[str, Any]] = []
    tactical: list[dict[str, Any]] = []
    options_alpha: list[dict[str, Any]] = []
    core_options_cross: list[dict[str, Any]] = []
    execution_candidates: list[dict[str, Any]] = []
    recent_alpha_pulse: list[dict[str, Any]] = []
    learning_queue: list[dict[str, Any]] = []
    recall: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    options_by_market = options_by_market or {}
    recent_alpha_pulse_by_market = recent_alpha_pulse_by_market or {}
    learning_queue_by_market = learning_queue_by_market or {}
    tactical_policies = {
        market: select_tactical_policy(candidates)
        for market, candidates in candidates_by_market.items()
    }
    research_policies = {
        market: select_positive_ev_research_policy(candidates)
        for market, candidates in candidates_by_market.items()
    }
    ev_status = ev_status_override or {
        market: "passed" if selected_policies.get(market) else "failed"
        for market in candidates_by_market
    }

    for market, current_rows in current_by_market.items():
        selected_policy_id = selected_policies.get(market)
        tactical_policy_id = tactical_policies.get(market)
        research_policy_id = research_policies.get(market)
        recent_pulse = recent_pulse_for_market(recent_alpha_pulse_by_market, market)
        option_lookup = {
            str(item.get("symbol") or "").upper(): item
            for item in options_by_market.get(market, [])
            if item.get("source") == "real_options"
        }
        for row in current_rows:
            symbol_key = str(row.get("symbol") or "").upper()
            option_item = option_lookup.get(symbol_key)
            cross_context = (
                us_core_options_cross(row, option_item)
                if market == "us" and is_us_core_options_cross_candidate(row)
                else None
            )
            if cross_context is not None:
                core_options_cross.append(core_options_cross_bulletin_item(row, cross_context))
            execution_candidate = (
                build_us_execution_candidate_item(
                    row,
                    cross_context,
                    selected_policy_id=selected_policy_id,
                    recent_pulse=recent_pulse,
                )
                if market == "us"
                else build_cn_execution_candidate_item(row, recent_pulse=recent_pulse)
                if market == "cn"
                else None
            )
            if execution_candidate is not None:
                execution_candidates.append(execution_candidate)
            blockers = candidate_blockers(row, selected_policy_id)
            if selected_policy_id and row.get("policy_id") == selected_policy_id and gate_passes(row):
                options_ok = True
                options_reason = ""
                if market == "us":
                    if cross_context is None:
                        cross_context = us_core_options_cross(row, option_item)
                    options_ok = bool(cross_context.get("execution_pass"))
                    options_reason = str(cross_context.get("reason") or "")
                    if execution_candidate and execution_candidate.get("blockers"):
                        options_ok = False
                        options_reason = str(execution_candidate.get("reason") or options_reason)
                elif market == "cn":
                    execution_action = str((execution_candidate or {}).get("action") or "")
                    execution_candidate_blockers = list((execution_candidate or {}).get("blockers") or [])
                    if execution_candidate_blockers or execution_action not in {"planned_entry_probe", "wait_pullback_probe"}:
                        options_ok = False
                        options_reason = str(
                            (execution_candidate or {}).get("reason")
                            or "CN shadow-option setup failed entry/chase/exit execution discipline"
                        )
                if options_ok:
                    reason = (
                        "Execution Alpha: CN oversold_contrarian has EV LCB80>0 and planned-entry constraints passed"
                        if market == "cn"
                        else f"Execution Alpha: V2 main strategy with EV LCB80>0 and execution constraints passed; {options_reason}"
                    )
                    execution.append(
                        bulletin_item(
                            row,
                            "execution_alpha",
                            reason,
                        )
                    )
                else:
                    cross_blockers = list((cross_context or {}).get("blockers") or [])
                    execution_candidate_blockers = list((execution_candidate or {}).get("blockers") or [])
                    recall_reason = (
                        "Positive EV Setup: CN shadow-option setup has positive EV evidence, but entry/chase/exit gates do not allow execution today"
                        if market == "cn"
                        else "Positive EV Setup: V2 main strategy has positive EV evidence, but core/options cross is conflicted or incomplete"
                    )
                    recall.append(
                        bulletin_item(
                            row,
                            "recall_alpha",
                            recall_reason,
                            dedupe(
                                [
                                    *blockers,
                                    *cross_blockers,
                                    *execution_candidate_blockers,
                                    *([options_reason] if market == "us" and options_reason else []),
                                ]
                            ),
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
            elif research_policy_id and row.get("policy_id") == research_policy_id:
                recall.append(
                    bulletin_item(
                        row,
                        "recall_alpha",
                        "positive-EV research policy; not Execution Alpha because report-scope/confidence gate is not promoted",
                        blockers,
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
        market_options = options_by_market.get(market, [])
        if market == "us":
            options_alpha.extend([item for item in market_options if is_tradeable_us_options_alpha(item)])
        else:
            options_alpha.extend(market_options)
        recent_alpha_pulse.extend(recent_alpha_pulse_by_market.get(market, []))
        learning_queue.extend(learning_queue_by_market.get(market, []))

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
        "research_policies": research_policies,
        "stability": stability,
        "execution_alpha": execution,
        "tactical_alpha": tactical,
        "options_alpha": options_alpha,
        "core_options_cross": core_options_cross,
        "execution_candidates": execution_candidates,
        "recent_alpha_pulse": recent_alpha_pulse,
        "learning_queue": learning_queue,
        "recall_alpha": recall,
        "blocked_alpha": blocked,
    }


def render_market_bulletin_md(bulletin: dict[str, Any], market: str) -> str:
    market_upper = market.upper()
    selected = bulletin["selected_policies"].get(market)
    tactical = bulletin.get("tactical_policies", {}).get(market)
    research = bulletin.get("research_policies", {}).get(market)
    evaluated = bulletin["evaluated_through"].get(market, "unknown")
    ev_status = bulletin.get("ev_status", {}).get(
        market, "passed" if selected else "failed"
    )
    ev_note = {
        "passed": "stable profit policy selected; Execution Alpha still needs a matching current candidate and expression pass",
        "failed": "stable gate evaluated; no profit policy passed, so Positive EV Setup / Legacy names remain review-only",
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
        f"- positive_ev_research_policy: `{research or 'none'}`",
        f"- ev_note: {ev_note}",
        "- headline: advisory context only, not an execution blocker",
        "",
    ]
    lines += render_recent_alpha_pulse_md(bulletin, market)
    lines += render_execution_candidates_md(bulletin, market)
    lines += render_learning_queue_md(bulletin, market)
    section_specs = [
        (
            "Equity Execution Alpha",
            "execution_alpha",
            "None. No current candidate passed both the stability champion and execution gates.",
        ),
        (
            "Core + Options Cross",
            "core_options_cross",
            "None. No current US core candidate had a matching options-alpha row to classify.",
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
            "Positive EV Setup",
            "recall_alpha",
            "None. No positive-EV setup / Factor Lab recall lead requires follow-up.",
        ),
        (
            "Legacy / Blocked Alpha",
            "blocked_alpha",
            "None. No blocked current candidates were found.",
        ),
    ]
    if market != "us":
        section_specs = [spec for spec in section_specs if spec[1] != "core_options_cross"]

    for title, key, empty in section_specs:
        lines += [f"### {title}", ""]
        items = [item for item in bulletin[key] if item.get("market") == market]
        if not items:
            lines += [f"- {empty}", ""]
            continue
        for item in items[:20]:
            if key == "core_options_cross":
                cross = ((item.get("details") or {}).get("core_options_cross") or {})
                blockers = item.get("blockers") or []
                blocker_text = f" Blockers: {', '.join(blockers)}." if blockers else ""
                lines.append(
                    f"- `{item.get('symbol')}` - Tier `{item.get('tier') or cross.get('tier')}`; "
                    f"direction `{item.get('signal_direction')}`; expression `{item.get('expression') or cross.get('expression')}`; "
                    f"dir_edge `{cross.get('directional_edge')}`; vol_edge `{cross.get('vol_edge')}`; "
                    f"flow_edge `{cross.get('flow_edge')}`. {item.get('reason')}.{blocker_text}"
                )
                continue
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

        for section in [
            "recent_alpha_pulse",
            "execution_candidates",
            "learning_queue",
            "execution_alpha",
            "core_options_cross",
            "tactical_alpha",
            "options_alpha",
            "recall_alpha",
            "blocked_alpha",
        ]:
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
    recent_alpha_pulse_by_market: dict[str, list[dict[str, Any]]] = {}
    learning_queue_by_market: dict[str, list[dict[str, Any]]] = {}
    selected_policies: dict[str, str | None] = {}
    ev_status: dict[str, str] = {}
    selection_rows: list[dict[str, Any]] = []
    selected_trade_rows: list[dict[str, Any]] = []

    for cfg in configs:
        try:
            rows, eval_through = load_evaluated_trades(
                cfg.db_path,
                cfg.market,
                as_of,
                args.lookback_days,
                cfg.horizon_days,
            )
        except duckdb.Error as exc:
            eval_through = completed_cutoff(as_of, cfg.horizon_days).isoformat()
            candidates_by_market[cfg.market] = []
            current_by_market[cfg.market] = []
            options_by_market[cfg.market] = []
            recent_alpha_pulse_by_market[cfg.market] = []
            learning_queue_by_market[cfg.market] = []
            selected_policies[cfg.market] = None
            ev_status[cfg.market] = "pending"
            selection_rows.append(
                {
                    "market": cfg.market,
                    "selected_policy_id": None,
                    "previous_policy_id": load_previous_champion(args.history_db, cfg.market, as_of),
                    "stability_score": None,
                    "challenger_policy_id": None,
                    "challenger_score": None,
                    "selection_reason": f"market data unavailable; stable gate pending: {type(exc).__name__}: {str(exc).splitlines()[0][:180]}",
                }
            )
            evaluated_through[cfg.market] = eval_through
            continue
        evaluated_through[cfg.market] = eval_through
        candidates = build_policy_candidates(rows, cfg.market, cfg.horizon_days, args.lookback_days)
        previous = load_previous_champion(args.history_db, cfg.market, as_of)
        selected = None
        reason = "auto-select disabled"
        if args.auto_select:
            selected, reason = select_champion(candidates, previous)
        mark_selected(candidates, selected)
        selected_policies[cfg.market] = selected
        ev_status[cfg.market] = "passed" if selected else "failed"
        candidates_by_market[cfg.market] = candidates
        try:
            current_by_market[cfg.market] = load_current_candidates(
                cfg.db_path,
                cfg.market,
                as_of,
                cfg.horizon_days,
            )
        except duckdb.Error:
            current_by_market[cfg.market] = []
        try:
            options_by_market[cfg.market] = load_options_alpha_candidates(
                cfg.db_path,
                cfg.market,
                as_of,
            )
        except duckdb.Error:
            options_by_market[cfg.market] = []
        try:
            recent_alpha_pulse_by_market[cfg.market] = load_recent_alpha_pulse(
                cfg.db_path,
                cfg.market,
                as_of,
            )
        except duckdb.Error:
            recent_alpha_pulse_by_market[cfg.market] = []
        try:
            learning_queue_by_market[cfg.market] = load_learning_queue(
                cfg.db_path,
                cfg.market,
                as_of,
            )
        except duckdb.Error:
            learning_queue_by_market[cfg.market] = []

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
        as_of=as_of,
        evaluated_through=evaluated_through,
        selected_policies=selected_policies,
        candidates_by_market=candidates_by_market,
        current_by_market=current_by_market,
        options_by_market=options_by_market,
        recent_alpha_pulse_by_market=recent_alpha_pulse_by_market,
        ev_status_override=ev_status,
        learning_queue_by_market=learning_queue_by_market,
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
