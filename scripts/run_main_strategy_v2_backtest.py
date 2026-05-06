#!/usr/bin/env python3
"""Backtest and report the EV-first Main Strategy V2 policy.

This is a deterministic reporting layer over the existing US report ledgers,
CN paper-trade EV tables, and CN limit-up model tables. It does not replace the
daily model producers; it classifies the produced rows into the V2 strategy
states requested for review:

* Execution Alpha
* Positive EV Setup
* Limit-Up Radar
* Legacy / Blocked
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_START = "2026-03-01"
LCB80_Z = 1.2816
LCB95_Z = 1.6449
US_STOCK_ROUNDTRIP_COST_PCT = 0.15
PORTFOLIO_TOTAL_R_CAP = 1.50
PORTFOLIO_VAR95_R_CAP = 1.00
SECTOR_R_CAP = 0.50
CORR_CLUSTER_R_CAP = 0.75
CORR_CLUSTER_THRESHOLD = 0.65
OPTION_CONTRACT_MULTIPLIER = 100.0
OPTION_COMMISSION_PER_LEG = 0.65
CN_MAX_LIFECYCLE_HOLD_DAYS = 5
CN_LIFECYCLE_BUCKET_ORDER = ["T+1", "T+2", "T+3", "T+4-T+5", "T+6-T+10", ">T+10", "pending"]
CN_MANUAL_MICRO_PROBE_R = 0.05
CN_EXECUTION_ALPHA_STATE = "positive_ev_setup"


@dataclass
class StrategyMetrics:
    label: str
    n: int
    active_dates: int
    avg_pct: float | None
    median_pct: float | None
    win_rate: float | None
    lcb80_pct: float | None
    lcb95_pct: float | None
    max_drawdown_pct: float | None
    total_pct: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "n": self.n,
            "active_dates": self.active_dates,
            "avg_pct": round_or_none(self.avg_pct),
            "median_pct": round_or_none(self.median_pct),
            "win_rate": round_or_none(self.win_rate),
            "lcb80_pct": round_or_none(self.lcb80_pct),
            "lcb95_pct": round_or_none(self.lcb95_pct),
            "max_drawdown_pct": round_or_none(self.max_drawdown_pct),
            "total_pct": round_or_none(self.total_pct),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Main Strategy V2 backtest report.")
    parser.add_argument("--date", default=None, help="Report date. Defaults to latest available DB date.")
    parser.add_argument("--start", default=DEFAULT_START, help="Backtest start date.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--us-db", type=Path, default=STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb")
    parser.add_argument("--cn-db", type=Path, default=STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb")
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def round_or_none(value: Any, digits: int = 6) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:+.{digits}f}%"


def fmt_num(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def fmt_bool(value: bool) -> str:
    return "yes" if value else "no"


def safe_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def nested_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def latest_date_in_db(path: Path, specs: Iterable[tuple[str, str]]) -> date | None:
    if not path.exists():
        return None
    con = duckdb.connect(str(path), read_only=True)
    try:
        latest: date | None = None
        for table, col in specs:
            if not table_exists(con, table):
                continue
            row = con.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
            value = row[0] if row else None
            if value is None:
                continue
            parsed = parse_date(as_iso(value) or "")
            latest = parsed if latest is None else max(latest, parsed)
        return latest
    finally:
        con.close()


def infer_report_date(us_db: Path, cn_db: Path) -> date:
    dates = [
        latest_date_in_db(us_db, [("report_decisions", "report_date"), ("options_alpha", "as_of")]),
        latest_date_in_db(
            cn_db,
            [
                ("report_decisions", "report_date"),
                ("strategy_ev", "as_of"),
                ("limit_up_model_predictions", "as_of"),
            ],
        ),
    ]
    present = [d for d in dates if d is not None]
    if not present:
        return date.today()
    return max(present)


def compute_metrics(label: str, rows: list[dict[str, Any]], return_key: str = "return_pct") -> StrategyMetrics:
    returns: list[float] = []
    by_date: dict[str, list[float]] = {}
    for row in rows:
        ret = round_or_none(row.get(return_key))
        if ret is None:
            continue
        returns.append(float(ret))
        report_date = as_iso(row.get("report_date"))
        if report_date:
            by_date.setdefault(report_date, []).append(float(ret))

    if not returns:
        return StrategyMetrics(label, 0, 0, None, None, None, None, None, None, None)

    avg = statistics.fmean(returns)
    median = statistics.median(returns)
    win = sum(1 for value in returns if value > 0) / len(returns)
    if len(returns) == 1:
        lcb80 = avg
        lcb95 = avg
    else:
        std = statistics.stdev(returns)
        se = std / math.sqrt(len(returns))
        lcb80 = avg - LCB80_Z * se
        lcb95 = avg - LCB95_Z * se

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for key in sorted(by_date):
        cumulative += statistics.fmean(by_date[key])
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)

    return StrategyMetrics(
        label=label,
        n=len(returns),
        active_dates=len(by_date),
        avg_pct=avg,
        median_pct=median,
        win_rate=win,
        lcb80_pct=lcb80,
        lcb95_pct=lcb95,
        max_drawdown_pct=max_dd,
        total_pct=sum(returns),
    )


def rows_with_return_cost(rows: list[dict[str, Any]], cost_pct: float) -> list[dict[str, Any]]:
    adjusted: list[dict[str, Any]] = []
    for row in rows:
        copied = dict(row)
        ret = round_or_none(copied.get("return_pct"))
        copied["gross_return_pct"] = ret
        copied["roundtrip_cost_pct"] = cost_pct
        copied["return_pct"] = None if ret is None else ret - cost_pct
        adjusted.append(copied)
    return adjusted


def rolling_freshness(
    label: str,
    rows: list[dict[str, Any]],
    as_of: date,
    *,
    min_n: int,
    windows: tuple[int, ...] = (7, 14, 30, 45, 60),
) -> dict[str, Any]:
    """Estimate whether a strategy edge is fresh, decaying, or stale.

    This is intentionally simple and auditable: it recomputes the same LCB80
    evidence on trailing calendar windows. The output is a research guide, not a
    model-selection oracle.
    """
    window_rows: list[dict[str, Any]] = []
    passed: list[int] = []
    for days in windows:
        start = as_of - timedelta(days=days)
        subset = [
            row
            for row in rows
            if (as_iso(row.get("report_date")) and parse_date(as_iso(row.get("report_date")) or "1900-01-01") >= start)
        ]
        metrics = compute_metrics(f"{label} trailing {days}D", subset).to_dict()
        ok = metrics["n"] >= min_n and (metrics.get("lcb80_pct") or 0.0) > 0.0
        if ok:
            passed.append(days)
        window_rows.append({"window_days": days, "passed": ok, **metrics})

    if 14 in passed:
        state = "fresh"
        freshness_days = 14
        rule = "edge survives the short rolling window; keep daily validation"
    elif 30 in passed:
        state = "usable_but_monitor"
        freshness_days = 30
        rule = "edge survives the monthly window but not the short window; size down and recheck"
    elif passed:
        state = "decaying_or_slow"
        freshness_days = max(passed)
        rule = "edge only survives a long window; do not assume it is fresh"
    else:
        state = "expired_or_unproven"
        freshness_days = None
        rule = "latest rolling LCB80 is not positive with enough samples"

    return {
        "label": label,
        "state": state,
        "freshness_days": freshness_days,
        "rule": rule,
        "windows": window_rows,
    }


def is_stable_positive(metrics: StrategyMetrics, *, min_n: int, min_dates: int) -> bool:
    return (
        metrics.n >= min_n
        and metrics.active_dates >= min_dates
        and (metrics.avg_pct or 0.0) > 0.0
        and (metrics.lcb80_pct or 0.0) > 0.0
    )


def option_expression_pass(row: dict[str, Any] | None) -> tuple[bool, str]:
    if not row:
        return False, "options expression missing"
    expression = str(row.get("expression") or "").lower()
    liquidity = str(row.get("liquidity_gate") or "").lower()
    directional = float(row.get("directional_edge") or 0.0)
    vol_edge = float(row.get("vol_edge") or 0.0)
    if liquidity != "pass":
        return False, f"option liquidity {liquidity or 'missing'}"
    if expression == "call_spread" and directional > 0 and vol_edge > 0:
        return True, "call_spread: direction and vol edges pass"
    if expression == "stock_long" and directional > 0:
        return True, "stock_long: direction edge positive, listed options not attractive"
    if expression in {"wait", "blocked", "put_spread"}:
        return False, f"expression {expression} is not a long expression"
    return False, "direction/vol edge did not pass"


def us_trend_regime(row: dict[str, Any]) -> str:
    details = safe_json_loads(row.get("details_json"))
    return str(
        nested_get(details, "execution_gate", "trend_regime")
        or nested_get(details, "execution_gate", "regime")
        or "unknown"
    ).lower()


def us_signal_blockers(row: dict[str, Any]) -> list[str]:
    details = safe_json_loads(row.get("details_json"))
    out: list[str] = []
    main_gate = details.get("main_signal_gate")
    if isinstance(main_gate, dict):
        blockers = main_gate.get("blockers") or []
        if isinstance(blockers, list):
            out.extend(str(item) for item in blockers if item)
    overnight = details.get("overnight_alpha")
    if isinstance(overnight, dict):
        reasons = overnight.get("reason_codes") or []
        if isinstance(reasons, list):
            out.extend(str(item) for item in reasons if item)
        if overnight.get("alpha_already_paid_risk"):
            out.append("alpha_already_paid_risk")
    primary = str(row.get("primary_reason") or "")
    for marker in ["rr_below_1_5", "stale_chase", "exhaustion_downgrade", "move already paid"]:
        if marker in primary:
            out.append(marker)
    return list(dict.fromkeys(out))


def us_pullback_price(row: dict[str, Any]) -> float | None:
    details = safe_json_loads(row.get("details_json"))
    return round_or_none(nested_get(details, "execution_gate", "pullback_price"), 4)


def us_missed_alpha_candidate(row: dict[str, Any]) -> bool:
    if str(row.get("state") or "") == "Execution Alpha":
        return False
    if str(row.get("policy") or "") not in {"legacy HIGH/MOD core", "LOW core executable trending"}:
        return False
    blockers = {str(item) for item in (row.get("blockers") or [])}
    blocker_text = " ".join(blockers).lower()
    rr = round_or_none(row.get("rr_ratio"))
    trend = str(row.get("trend_regime") or "")
    return bool(
        "stale" in blocker_text
        or "already_paid" in blocker_text
        or "paid" in blocker_text
        or "exhaustion" in blocker_text
        or (rr is not None and rr < 1.5)
        or trend in {"noisy", "mean_reverting"}
    )


def is_us_v2_policy(row: dict[str, Any]) -> bool:
    return (
        str(row.get("report_bucket") or "").lower() == "core"
        and str(row.get("signal_direction") or "").lower() == "long"
        and str(row.get("signal_confidence") or "").upper() == "LOW"
        and str(row.get("execution_mode") or "").lower() == "executable_now"
        and us_trend_regime(row) == "trending"
    )


def is_us_legacy_policy(row: dict[str, Any]) -> bool:
    return (
        str(row.get("report_bucket") or "").lower() == "core"
        and str(row.get("signal_direction") or "").lower() == "long"
        and str(row.get("signal_confidence") or "").upper() in {"HIGH", "MODERATE"}
        and str(row.get("execution_mode") or "").lower() == "executable_now"
    )


def load_us_rows(db_path: Path, start: date, as_of: date) -> tuple[list[dict[str, Any]], str]:
    if not db_path.exists():
        return [], "missing"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "report_decisions") or not table_exists(con, "report_outcomes"):
            return [], "missing report_decisions/report_outcomes"
        rows = rows_as_dicts(
            con,
            """
            SELECT
                d.report_date, d.symbol, d.selection_status, d.rank_order,
                d.report_bucket, d.signal_direction, d.signal_confidence,
                d.headline_mode, d.execution_mode, d.entry_price, d.reference_price,
                d.stop_price, d.target_price, d.rr_ratio, d.expected_move_pct,
                d.primary_reason, d.details_json,
                o.evaluation_date,
                o.hold_3d_ret_pct AS return_pct
            FROM report_decisions d
            JOIN report_outcomes o
              ON o.report_date = d.report_date
             AND o.session = d.session
             AND o.symbol = d.symbol
             AND o.selection_status = d.selection_status
            WHERE d.report_date >= CAST(? AS DATE)
              AND d.report_date <= CAST(? AS DATE)
              AND COALESCE(o.data_ready, TRUE)
            ORDER BY d.report_date, d.rank_order, d.symbol
            """,
            [start.isoformat(), as_of.isoformat()],
        )
        return rows, "ok"
    finally:
        con.close()


def load_us_current_rows(db_path: Path, as_of: date) -> tuple[list[dict[str, Any]], date | None, str]:
    if not db_path.exists():
        return [], None, "missing"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "report_decisions"):
            return [], None, "missing report_decisions"
        latest_row = con.execute(
            "SELECT MAX(report_date) FROM report_decisions WHERE report_date <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()
        latest = latest_row[0] if latest_row else None
        if latest is None:
            return [], None, "no rows"
        latest_date = parse_date(as_iso(latest) or "")
        rows = rows_as_dicts(
            con,
            """
            SELECT report_date, symbol, selection_status, rank_order, report_bucket,
                   signal_direction, signal_confidence, headline_mode, execution_mode,
                   entry_price, reference_price, stop_price, target_price, rr_ratio,
                   expected_move_pct, primary_reason, details_json
            FROM report_decisions
            WHERE report_date = CAST(? AS DATE)
            ORDER BY COALESCE(rank_order, 999999), symbol
            """,
            [latest_date.isoformat()],
        )
        return rows, latest_date, "ok"
    finally:
        con.close()


def load_us_options(db_path: Path, as_of: date) -> dict[str, dict[str, Any]]:
    if not db_path.exists():
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "options_alpha"):
            return {}
        latest_row = con.execute(
            "SELECT MAX(as_of) FROM options_alpha WHERE as_of <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()
        latest = latest_row[0] if latest_row else None
        if latest is None:
            return {}
        rows = rows_as_dicts(
            con,
            """
            SELECT symbol, as_of, directional_edge, vol_edge, vrp_edge, flow_edge,
                   liquidity_gate, expression, reason, detail_json
            FROM options_alpha
            WHERE as_of = CAST(? AS DATE)
            """,
            [as_iso(latest)],
        )
        return {str(row["symbol"]).upper(): row for row in rows}
    finally:
        con.close()


def load_us_options_range(db_path: Path, start: date, as_of: date) -> dict[tuple[str, str], dict[str, Any]]:
    if not db_path.exists():
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "options_alpha"):
            return {}
        rows = rows_as_dicts(
            con,
            """
            SELECT symbol, as_of, directional_edge, vol_edge, vrp_edge, flow_edge,
                   liquidity_gate, expression, reason, detail_json
            FROM options_alpha
            WHERE as_of >= CAST(? AS DATE)
              AND as_of <= CAST(? AS DATE)
            """,
            [start.isoformat(), as_of.isoformat()],
        )
        return {
            (as_iso(row.get("as_of")) or "", str(row.get("symbol") or "").upper()): row
            for row in rows
        }
    finally:
        con.close()


def build_us_missed_alpha_radar(current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows too extended for fresh entry, but useful as hold/retest radar."""
    confidence_rank = {"HIGH": 3, "MODERATE": 2, "LOW": 1}
    def radar_priority(row: dict[str, Any]) -> tuple[int, int, int, float, str]:
        blockers = " ".join(str(item) for item in (row.get("blockers") or [])).lower()
        rr = round_or_none(row.get("rr_ratio"))
        low_rr = int("rr_below" in blockers or (rr is not None and rr < 1.5))
        has_stop = int(row.get("stop") is not None)
        confidence = confidence_rank.get(str(row.get("signal_confidence") or "").upper(), 0)
        rr_sort = float(rr if rr is not None else 99.0)
        return (-low_rr, -has_stop, -confidence, rr_sort, str(row.get("symbol") or ""))

    by_symbol: dict[str, dict[str, Any]] = {}
    for row in current:
        if not us_missed_alpha_candidate(row):
            continue
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        blockers = row.get("blockers") or []
        radar_row = {
            "state": "Missed Alpha Radar",
            "symbol": symbol,
            "signal_confidence": row.get("signal_confidence"),
            "fresh_entry_action": "no_fresh_buy_chase_blocked",
            "hold_action": "valid_to_hold_if_already_owned_and_trailing_stop_intact",
            "retest_plan": "wait pullback/retest; add only if a fresh V2 ticket appears",
            "entry": row.get("entry"),
            "pullback_price": row.get("pullback_price"),
            "stop": row.get("stop"),
            "target": row.get("target"),
            "rr_ratio": row.get("rr_ratio"),
            "trend_regime": row.get("trend_regime"),
            "blockers": blockers,
            "reason": row.get("primary_reason") or row.get("reason"),
        }
        existing = by_symbol.get(symbol)
        if not existing:
            by_symbol[symbol] = radar_row
            continue
        old_rank = confidence_rank.get(str(existing.get("signal_confidence") or "").upper(), 0)
        new_rank = confidence_rank.get(str(radar_row.get("signal_confidence") or "").upper(), 0)
        if (new_rank, float(radar_row.get("rr_ratio") or 0.0)) > (old_rank, float(existing.get("rr_ratio") or 0.0)):
            by_symbol[symbol] = radar_row
    return sorted(
        by_symbol.values(),
        key=radar_priority,
    )[:50]


def summarize_us(db_path: Path, start: date, as_of: date) -> dict[str, Any]:
    rows, status = load_us_rows(db_path, start, as_of)
    options = load_us_options(db_path, as_of)
    options_history = load_us_options_range(db_path, start, as_of)
    v2_rows = [row for row in rows if is_us_v2_policy(row)]
    v2_option_rows = [
        row
        for row in v2_rows
        if option_expression_pass(
            options_history.get((as_iso(row.get("report_date")) or "", str(row.get("symbol") or "").upper()))
        )[0]
    ]
    legacy_rows = [row for row in rows if is_us_legacy_policy(row)]
    v2_stock_net_rows = rows_with_return_cost(v2_rows, US_STOCK_ROUNDTRIP_COST_PCT)

    current_rows, current_date, current_status = load_us_current_rows(db_path, as_of)
    v2_metrics = compute_metrics("US V2 LOW/core/executable/trending", v2_rows)
    v2_stock_net_metrics = compute_metrics(
        f"US V2 stock-only net after {US_STOCK_ROUNDTRIP_COST_PCT:.2f}% roundtrip cost",
        v2_stock_net_rows,
    )
    v2_option_metrics = compute_metrics("US V2 with long options expression", v2_option_rows)
    legacy_metrics = compute_metrics("US legacy HIGH/MOD structural core", legacy_rows)
    freshness = {
        "v2": rolling_freshness("US V2 LOW/core/executable/trending", v2_rows, as_of, min_n=8),
        "v2_stock_only_net": rolling_freshness(
            f"US V2 stock-only net after {US_STOCK_ROUNDTRIP_COST_PCT:.2f}% roundtrip cost",
            v2_stock_net_rows,
            as_of,
            min_n=8,
        ),
        "legacy": rolling_freshness("US legacy HIGH/MOD structural core", legacy_rows, as_of, min_n=8),
    }
    v2_stable = is_stable_positive(v2_metrics, min_n=20, min_dates=10)

    current: list[dict[str, Any]] = []
    for row in current_rows:
        symbol = str(row.get("symbol") or "").upper()
        opt_row = options.get(symbol)
        opt_pass, opt_reason = option_expression_pass(opt_row)
        is_v2 = is_us_v2_policy(row)
        is_legacy = is_us_legacy_policy(row)
        if is_v2 and v2_stable and opt_pass:
            state = "Execution Alpha"
            reason = "V2 policy, LCB80>0, trending regime, options expression passes"
        elif is_v2 and (v2_metrics.avg_pct or 0.0) > 0.0 and (v2_metrics.lcb80_pct or 0.0) > 0.0:
            state = "Positive EV Setup"
            reason = f"V2 EV is positive but execution blocked by {opt_reason if not opt_pass else 'stable constraints'}"
        elif is_legacy:
            state = "Legacy / Blocked"
            reason = "legacy HIGH/MOD baseline; not the main strategy, and EV must be audited before use"
        else:
            continue
        current.append(
            {
                "market": "us",
                "as_of": as_iso(row.get("report_date")),
                "symbol": symbol,
                "name": "",
                "state": state,
                "policy": "LOW core executable trending" if is_v2 else "legacy HIGH/MOD core",
                "entry": round_or_none(row.get("entry_price") or row.get("reference_price"), 4),
                "stop": round_or_none(row.get("stop_price"), 4),
                "target": round_or_none(row.get("target_price"), 4),
                "rr_ratio": round_or_none(row.get("rr_ratio"), 4),
                "time_exit": "3 sessions / next catalyst",
                "option_expression": (opt_row or {}).get("expression"),
                "option_reason": opt_reason,
                "trend_regime": us_trend_regime(row),
                "signal_confidence": row.get("signal_confidence"),
                "execution_mode": row.get("execution_mode"),
                "primary_reason": row.get("primary_reason"),
                "blockers": us_signal_blockers(row),
                "pullback_price": us_pullback_price(row),
                "reason": reason,
            }
        )

    missed_alpha = build_us_missed_alpha_radar(current)
    return {
        "status": status,
        "current_status": current_status,
        "current_date": current_date.isoformat() if current_date else None,
        "metrics": {
            "v2": v2_metrics.to_dict(),
            "v2_stock_only_net": v2_stock_net_metrics.to_dict(),
            "v2_options_confirmed": v2_option_metrics.to_dict(),
            "legacy": legacy_metrics.to_dict(),
        },
        "freshness": freshness,
        "v2_stable": v2_stable,
        "options_coverage_rows": len(options),
        "current": current,
        "missed_alpha_radar": missed_alpha,
    }


def load_cn_strategy_rows(db_path: Path, start: date, as_of: date) -> tuple[list[dict[str, Any]], str]:
    if not db_path.exists():
        return [], "missing"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return [], "missing strategy_model_dataset"
        rows = rows_as_dicts(
            con,
            """
            SELECT
                m.report_date, m.evaluation_date, m.symbol,
                COALESCE(sb.name, '') AS name,
                m.selection_status, m.strategy_family, m.strategy_key,
                m.execution_rule, m.action_intent, m.alpha_state,
                m.reference_close, m.planned_entry, m.fill_status,
                m.fill_date, m.fill_price, m.exit_date, m.exit_price,
                m.realized_ret_pct AS return_pct,
                m.max_favorable_pct, m.max_adverse_pct, m.ev_pct, m.ev_lcb_80_pct,
                m.ev_lcb_95_pct, m.risk_unit_pct, m.ev_norm_score,
                m.ev_norm_lcb_80, m.detail_json, m.features_json
            FROM strategy_model_dataset m
            LEFT JOIN stock_basic sb ON sb.ts_code = m.symbol
            WHERE m.report_date >= CAST(? AS DATE)
              AND m.report_date <= CAST(? AS DATE)
              AND m.action_intent = 'TRADE'
              AND m.realized_ret_pct IS NOT NULL
            ORDER BY m.report_date, m.symbol
            """,
            [start.isoformat(), as_of.isoformat()],
        )
        return rows, "ok"
    finally:
        con.close()


def load_cn_current_rows(db_path: Path, as_of: date) -> tuple[list[dict[str, Any]], date | None, str]:
    if not db_path.exists():
        return [], None, "missing"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return [], None, "missing strategy_model_dataset"
        latest_row = con.execute(
            "SELECT MAX(report_date) FROM strategy_model_dataset WHERE report_date <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()
        latest = latest_row[0] if latest_row else None
        if latest is None:
            return [], None, "no rows"
        latest_date = parse_date(as_iso(latest) or "")
        has_strategy_ev = table_exists(con, "strategy_ev")
        ev_select = (
            """
                ev.samples AS strategy_samples, ev.fills AS strategy_fills,
                ev.eligible AS strategy_ev_eligible, ev.fail_reasons AS strategy_fail_reasons,
            """
            if has_strategy_ev
            else """
                NULL::INTEGER AS strategy_samples, NULL::INTEGER AS strategy_fills,
                NULL::BOOLEAN AS strategy_ev_eligible, NULL::VARCHAR AS strategy_fail_reasons,
            """
        )
        ev_join = (
            """
            LEFT JOIN strategy_ev ev
              ON ev.as_of = m.evaluation_date
             AND ev.strategy_key = m.strategy_key
            """
            if has_strategy_ev
            else ""
        )
        rows = rows_as_dicts(
            con,
            f"""
            SELECT
                m.report_date, m.evaluation_date, m.symbol,
                COALESCE(sb.name, '') AS name,
                COALESCE(sb.industry, '') AS industry,
                m.selection_status, m.strategy_family, m.strategy_key,
                m.execution_rule, m.action_intent, m.alpha_state,
                {ev_select}
                m.reference_close, m.planned_entry, m.fill_status,
                m.ev_pct, m.ev_lcb_80_pct, m.ev_lcb_95_pct, m.risk_unit_pct,
                m.ev_norm_score, m.ev_norm_lcb_80, m.detail_json, m.features_json
            FROM strategy_model_dataset m
            LEFT JOIN stock_basic sb ON sb.ts_code = m.symbol
            {ev_join}
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
            [latest_date.isoformat(), latest_date.isoformat()],
        )
        return rows, latest_date, "ok"
    finally:
        con.close()


def cn_price_plan(row: dict[str, Any]) -> dict[str, Any]:
    entry = round_or_none(row.get("planned_entry") or row.get("reference_close"), 4)
    risk = round_or_none(row.get("risk_unit_pct"), 4)
    if entry is None or risk is None or risk <= 0:
        return {
            "observation_entry_zone": fmt_num(entry),
            "handling_line": "-",
            "first_target": "-",
            "risk_unit_pct": risk,
        }
    low = entry
    high = entry * (1.0 + min(risk, 8.0) * 0.25 / 100.0)
    handling = entry * (1.0 - risk / 100.0)
    target = entry * (1.0 + risk / 100.0)
    return {
        "observation_entry_zone": f"{fmt_num(low)}-{fmt_num(high)}",
        "handling_line": round_or_none(handling, 4),
        "first_target": round_or_none(target, 4),
        "risk_unit_pct": risk,
    }


def cn_row_holding_days(row: dict[str, Any]) -> int | None:
    fill = as_iso(row.get("fill_date"))
    exit_ = as_iso(row.get("exit_date"))
    if not fill or not exit_:
        return None
    try:
        return max(0, (parse_date(exit_) - parse_date(fill)).days)
    except ValueError:
        return None


def cn_lifecycle_bucket(days: int | None) -> str:
    if days is None:
        return "pending"
    if days <= 1:
        return "T+1"
    if days == 2:
        return "T+2"
    if days == 3:
        return "T+3"
    if days <= 5:
        return "T+4-T+5"
    if days <= 10:
        return "T+6-T+10"
    return ">T+10"


def cn_feature_value(row: dict[str, Any], key: str) -> Any:
    features = safe_json_loads(row.get("features_json"))
    if key in features:
        return features.get(key)
    detail = safe_json_loads(row.get("detail_json"))
    return detail.get(key)


def cn_feature_float(row: dict[str, Any], key: str) -> float | None:
    return round_or_none(cn_feature_value(row, key))


def cn_risk_bucket(value: float | None, *, low: float, high: float) -> str:
    if value is None:
        return "unknown"
    if value < low:
        return "low"
    if value < high:
        return "mid"
    return "high"


def cn_lifecycle_summary(label: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = compute_metrics(label, rows).to_dict()
    holds = [cn_row_holding_days(row) for row in rows]
    holds_clean = [float(v) for v in holds if v is not None]
    mfe = [float(v) for row in rows if (v := round_or_none(row.get("max_favorable_pct"))) is not None]
    mae = [float(v) for row in rows if (v := round_or_none(row.get("max_adverse_pct"))) is not None]
    metrics.update(
        {
            "avg_hold_days": round_or_none(statistics.fmean(holds_clean)) if holds_clean else None,
            "avg_mfe_pct": round_or_none(statistics.fmean(mfe)) if mfe else None,
            "avg_mae_pct": round_or_none(statistics.fmean(mae)) if mae else None,
        }
    )
    return metrics


def cn_lifecycle_group(rows: list[dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(key_name) or "unknown")
        grouped.setdefault(key, []).append(row)
    order = CN_LIFECYCLE_BUCKET_ORDER if key_name == "hold_bucket" else sorted(grouped)
    out = []
    for key in order:
        if key in grouped:
            out.append({"bucket": key, **cn_lifecycle_summary(key, grouped[key])})
    return out


def build_cn_lifecycle_policy(hold_buckets: list[dict[str, Any]]) -> dict[str, Any]:
    upper_by_bucket = {
        "T+1": 1,
        "T+2": 2,
        "T+3": 3,
        "T+4-T+5": 5,
        "T+6-T+10": 10,
        ">T+10": 10,
    }
    eligible = [
        row
        for row in hold_buckets
        if int(row.get("n") or 0) >= 20 and (row.get("lcb80_pct") or 0.0) > 0.0 and row.get("bucket") != "pending"
    ]
    if eligible:
        best = max(eligible, key=lambda row: float(row.get("lcb80_pct") or -999.0))
        max_hold = min(CN_MAX_LIFECYCLE_HOLD_DAYS, max(upper_by_bucket.get(str(row.get("bucket")), 1) for row in eligible))
        state = "positive_lifecycle"
    else:
        best = max(hold_buckets, key=lambda row: float(row.get("lcb80_pct") or -999.0), default={})
        max_hold = 1
        state = "unproven_lifecycle"
    return {
        "state": state,
        "best_bucket": best.get("bucket"),
        "best_bucket_lcb80_pct": best.get("lcb80_pct"),
        "max_hold_days": max_hold,
        "first_review": "T+1 first sellable session; no same-day exit is counted",
        "follow_through_rule": "T+3 no +1R / no volume follow-through -> exit review",
        "time_stop": f"hard review/exit by T+{max_hold}",
        "entry_rule": "only EV LCB80>0 oversold_contrarian buckets; no chase above planned-entry zone",
        "risk_rule": "Tobit risk unit sets handling line; fear/high-vol clips size, not the edge definition",
    }


def build_cn_lifecycle_research(
    v2_rows: list[dict[str, Any]],
    all_oversold_rows: list[dict[str, Any]],
    as_of: date,
) -> dict[str, Any]:
    def enrich(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            copied = dict(row)
            hold_days = cn_row_holding_days(row)
            copied["hold_days"] = hold_days
            copied["hold_bucket"] = cn_lifecycle_bucket(hold_days)
            copied["execution_mode_bucket"] = str(cn_feature_value(row, "execution_mode") or "unknown")
            copied["fade_bucket"] = cn_risk_bucket(cn_feature_float(row, "fade_risk"), low=0.35, high=0.70)
            copied["stale_bucket"] = cn_risk_bucket(cn_feature_float(row, "stale_chase_risk"), low=0.35, high=0.65)
            copied["flow_bucket"] = "flow_conflict" if str(cn_feature_value(row, "flow_conflict_flag")).lower() == "true" else "flow_clean"
            out.append(copied)
        return out

    v2_enriched = enrich(v2_rows)
    all_enriched = enrich(all_oversold_rows)
    hold_buckets = cn_lifecycle_group(v2_enriched, "hold_bucket")
    return {
        "as_of": as_of.isoformat(),
        "scope": "CN oversold_contrarian lifecycle from strategy_model_dataset; same-day exits are not counted",
        "policy": build_cn_lifecycle_policy(hold_buckets),
        "summary": {
            "v2_ev_positive": cn_lifecycle_summary("V2 EV-positive oversold_contrarian", v2_enriched),
            "all_oversold_diagnostic": cn_lifecycle_summary("All oversold_contrarian diagnostic", all_enriched),
        },
        "by_hold_bucket": hold_buckets,
        "all_oversold_by_hold_bucket": cn_lifecycle_group(all_enriched, "hold_bucket"),
        "by_execution_mode": cn_lifecycle_group(v2_enriched, "execution_mode_bucket"),
        "by_fade_bucket": cn_lifecycle_group(v2_enriched, "fade_bucket"),
        "by_stale_bucket": cn_lifecycle_group(v2_enriched, "stale_bucket"),
        "by_flow_bucket": cn_lifecycle_group(v2_enriched, "flow_bucket"),
    }


def cn_lifecycle_time_exit(policy: dict[str, Any]) -> str:
    max_hold = policy.get("max_hold_days") or CN_MAX_LIFECYCLE_HOLD_DAYS
    return f"T+1 review; T+3 no +1R follow-through -> exit; hard max T+{max_hold}"


def cn_lifecycle_action(row: dict[str, Any], state: str, policy: dict[str, Any]) -> str:
    lcb80 = round_or_none(row.get("ev_lcb_80_pct"))
    execution_mode = str(cn_feature_value(row, "execution_mode") or "")
    fade = cn_feature_float(row, "fade_risk")
    if state == "Legacy / Blocked" or lcb80 is None or lcb80 <= 0:
        return "watch_only_no_new_trade"
    if state == "Positive EV Setup":
        if execution_mode == "do_not_chase" or (fade is not None and fade >= 0.70):
            return "watch_only_until_evidence_gate_passes; no open chase"
        return "watch_only_until_evidence_gate_passes"
    if execution_mode == "do_not_chase" or (fade is not None and fade >= 0.70):
        return "manual_probe_only_after_pullback; no open chase"
    return "planned_entry_probe; manage by T+1/T+3/T+max rule"


def cn_current_ev_gate_passes(row: dict[str, Any]) -> bool:
    alpha_state = str(row.get("alpha_state") or "")
    strategy_ev_eligible = row.get("strategy_ev_eligible")
    if alpha_state != CN_EXECUTION_ALPHA_STATE:
        return False
    if strategy_ev_eligible is False:
        return False
    return True


def cn_current_gate_summary(row: dict[str, Any]) -> str:
    alpha_state = str(row.get("alpha_state") or "-")
    samples = row.get("strategy_samples")
    fills = row.get("strategy_fills")
    eligible = row.get("strategy_ev_eligible")
    fail = str(row.get("strategy_fail_reasons") or "").strip()
    parts = [f"alpha_state={alpha_state}"]
    if samples is not None or fills is not None:
        parts.append(f"samples={samples if samples is not None else '-'}")
        parts.append(f"fills={fills if fills is not None else '-'}")
    if eligible is not None:
        parts.append(f"eligible={'yes' if eligible else 'no'}")
    if fail:
        parts.append(f"fail={fail}")
    return "; ".join(parts)


def summarize_cn(db_path: Path, start: date, as_of: date) -> dict[str, Any]:
    rows, status = load_cn_strategy_rows(db_path, start, as_of)
    v2_all_rows = [row for row in rows if row.get("strategy_family") == "oversold_contrarian"]
    v2_rows = [
        row
        for row in v2_all_rows
        if row.get("alpha_state") == "positive_ev_setup" or (round_or_none(row.get("ev_lcb_80_pct")) or 0.0) > 0.0
    ]
    legacy_rows = [row for row in rows if row.get("strategy_family") == "structural_core"]
    v2_metrics = compute_metrics("CN V2 oversold_contrarian EV-positive buckets", v2_rows)
    v2_all_metrics = compute_metrics("CN oversold_contrarian all buckets diagnostic", v2_all_rows)
    legacy_metrics = compute_metrics("CN legacy structural_core/high_mod baseline", legacy_rows)
    freshness = {
        "v2": rolling_freshness("CN V2 oversold_contrarian EV-positive buckets", v2_rows, as_of, min_n=20),
        "legacy": rolling_freshness("CN legacy structural_core/high_mod baseline", legacy_rows, as_of, min_n=8),
    }
    lifecycle = build_cn_lifecycle_research(v2_rows, v2_all_rows, as_of)
    lifecycle_policy = lifecycle.get("policy") or {}

    current_rows, current_date, current_status = load_cn_current_rows(db_path, as_of)
    current: list[dict[str, Any]] = []
    for row in current_rows:
        family = str(row.get("strategy_family") or "")
        action = str(row.get("action_intent") or "")
        lcb80 = round_or_none(row.get("ev_lcb_80_pct"))
        ev_pct = round_or_none(row.get("ev_pct"))
        features = safe_json_loads(row.get("features_json"))
        market_high_vol = round_or_none(features.get("market_p_high_vol"))
        execution_mode = str(features.get("execution_mode") or "")
        is_v2 = family == "oversold_contrarian" and action == "TRADE"
        is_legacy = family == "structural_core"
        if is_v2 and lcb80 is not None and lcb80 > 0:
            ev_gate_passes = cn_current_ev_gate_passes(row)
            hard_blocked = execution_mode in {"blocked", "no_trade", "skip", "avoid"}
            pullback_only = (market_high_vol or 0.0) >= 0.85 or execution_mode == "do_not_chase"
            gate_summary = cn_current_gate_summary(row)
            if not ev_gate_passes:
                state = "Positive EV Setup"
                reason = f"oversold EV LCB80>0, but not Execution Alpha because the evidence gate has not passed ({gate_summary})"
            elif hard_blocked:
                state = "Positive EV Setup"
                reason = f"oversold EV LCB80>0, but a hard A-share execution blocker keeps this in review sizing ({gate_summary})"
            elif pullback_only:
                state = "Execution Alpha"
                reason = f"oversold EV LCB80>0 and evidence gate passed; A-share fear/high-vol is edge context, with pullback-only clipped size ({gate_summary})"
            else:
                state = "Execution Alpha"
                reason = f"oversold EV LCB80>0 with evidence and execution stress passing ({gate_summary})"
        elif is_v2:
            state = "Legacy / Blocked"
            reason = "oversold candidate but EV LCB80 is not positive for its bucket"
        elif is_legacy:
            state = "Legacy / Blocked"
            reason = "legacy structural_core baseline; no longer the default main strategy"
        else:
            continue
        plan = cn_price_plan(row)
        current.append(
            {
                "market": "cn",
                "as_of": as_iso(row.get("report_date")),
                "symbol": row.get("symbol"),
                "name": row.get("name") or "-",
                "industry": row.get("industry") or "",
                "state": state,
                "policy": family,
                "observation_entry_zone": plan["observation_entry_zone"],
                "handling_line": plan["handling_line"],
                "first_target": plan["first_target"],
                "risk_unit_pct": plan["risk_unit_pct"],
                "ev_pct": ev_pct,
                "ev_lcb80_pct": lcb80,
                "time_exit": cn_lifecycle_time_exit(lifecycle_policy),
                "lifecycle_action": cn_lifecycle_action(row, state, lifecycle_policy),
                "max_hold_days": lifecycle_policy.get("max_hold_days"),
                "t1_risk": "A-share T+1: cannot exit same day after fill; gap/limit-down risk remains.",
                "market_p_high_vol": market_high_vol,
                "execution_mode": execution_mode,
                "alpha_state": row.get("alpha_state"),
                "strategy_samples": row.get("strategy_samples"),
                "strategy_fills": row.get("strategy_fills"),
                "strategy_ev_eligible": row.get("strategy_ev_eligible"),
                "strategy_fail_reasons": row.get("strategy_fail_reasons"),
                "gate_summary": cn_current_gate_summary(row),
                "reason": reason,
            }
        )

    return {
        "status": status,
        "current_status": current_status,
        "current_date": current_date.isoformat() if current_date else None,
        "metrics": {
            "v2": v2_metrics.to_dict(),
            "v2_all_oversold_diagnostic": v2_all_metrics.to_dict(),
            "legacy": legacy_metrics.to_dict(),
        },
        "freshness": freshness,
        "lifecycle": lifecycle,
        "current": current,
    }


def summarize_limit_up(db_path: Path, start: date, as_of: date) -> dict[str, Any]:
    if not db_path.exists():
        return {"status": "missing", "performance": {}, "current": []}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "limit_up_model_predictions"):
            return {"status": "missing limit_up_model_predictions", "performance": {}, "current": []}
        perf: dict[str, Any] = {}
        if table_exists(con, "limit_up_model_performance"):
            row = con.execute(
                """
                SELECT
                    COUNT(*) AS days,
                    AVG(top_decile_hit_rate),
                    AVG(top_decile_lift),
                    AVG(failed_board_rate),
                    AVG(avg_next_ret_pct),
                    MAX(as_of)
                FROM limit_up_model_performance
                WHERE as_of >= CAST(? AS DATE)
                  AND as_of <= CAST(? AS DATE)
                """,
                [start.isoformat(), as_of.isoformat()],
            ).fetchone()
            if row:
                perf = {
                    "days": row[0],
                    "avg_top_decile_hit_rate": round_or_none(row[1]),
                    "avg_top_decile_lift": round_or_none(row[2]),
                    "avg_failed_board_rate": round_or_none(row[3]),
                    "avg_next_ret_pct": round_or_none(row[4]),
                    "latest_performance_date": as_iso(row[5]),
                }
        latest_row = con.execute(
            "SELECT MAX(as_of) FROM limit_up_model_predictions WHERE as_of <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()
        latest = latest_row[0] if latest_row else None
        if latest is None:
            return {"status": "no predictions", "performance": perf, "current": []}
        current = rows_as_dicts(
            con,
            """
            SELECT
                p.as_of, p.symbol, COALESCE(sb.name, json_extract_string(p.detail_json, '$.name'), '') AS name,
                COALESCE(sb.industry, json_extract_string(p.detail_json, '$.industry'), '') AS industry,
                p.board_scope, p.p_limit_up, p.p_touch_limit, p.p_failed_board,
                p.ev_after_cost_pct, p.ev_lcb_80_pct, p.probability_decile,
                p.model_state, p.decision_state
            FROM limit_up_model_predictions p
            LEFT JOIN stock_basic sb ON sb.ts_code = p.symbol
            WHERE p.as_of = CAST(? AS DATE)
            ORDER BY p.probability_decile DESC, p.p_limit_up DESC, p.ev_after_cost_pct DESC, p.symbol
            LIMIT 20
            """,
            [as_iso(latest)],
        )
        for row in current:
            row["state"] = "Limit-Up Radar"
            row["reason"] = "daily model only; requires 9:25/9:35 auction/open confirmation before execution"
        return {"status": "ok", "performance": perf, "current_date": as_iso(latest), "current": current}
    finally:
        con.close()


def recent_outcomes(us: dict[str, Any], cn: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for market, current in [("us", us.get("current") or []), ("cn", cn.get("current") or [])]:
        for row in current[:8]:
            rows.append(
                {
                    "market": market,
                    "symbol": row.get("symbol"),
                    "name": row.get("name") or "",
                    "state": row.get("state"),
                    "policy": row.get("policy"),
                    "reason": row.get("reason"),
                }
            )
    return rows


def render_metrics_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Strategy | n | Active days | Avg | Median | Win | EV LCB80 | Max DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['n']} | {row['active_dates']} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('median_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_pct(row.get('lcb80_pct'))} | {fmt_pct(row.get('max_drawdown_pct'))} |"
        )
    return lines


def render_current_table(rows: list[dict[str, Any]], market: str) -> list[str]:
    if not rows:
        return [f"- {market.upper()}: none.", ""]
    if market == "us":
        lines = [
            "| State | Symbol | Buy/Review | Stop | Target | Option expression | Trend | Time exit | Why |",
            "|---|---|---:|---:|---:|---|---|---|---|",
        ]
        for row in rows[:12]:
            lines.append(
                f"| {row['state']} | {row['symbol']} | {fmt_num(row.get('entry'))} | "
                f"{fmt_num(row.get('stop'))} | {fmt_num(row.get('target'))} | "
                f"{row.get('option_expression') or '-'} | {row.get('trend_regime') or '-'} | "
                f"{row.get('time_exit')} | {row.get('reason')} |"
            )
        lines.append("")
        return lines

    lines = [
        "| State | Code | Name | Observation entry | Handling line | First target | EV | EV80 | Evidence gate | Lifecycle action | Time exit | T+1 risk |",
        "|---|---|---|---:|---:|---:|---:|---:|---|---|---|---|",
    ]
    for row in rows[:16]:
        lines.append(
            f"| {row['state']} | {row['symbol']} | {row.get('name') or '-'} | "
            f"{row.get('observation_entry_zone') or '-'} | {fmt_num(row.get('handling_line'))} | "
            f"{fmt_num(row.get('first_target'))} | {fmt_pct(row.get('ev_pct'))} | "
            f"{fmt_pct(row.get('ev_lcb80_pct'))} | {row.get('gate_summary') or '-'} | "
            f"{row.get('lifecycle_action') or '-'} | "
            f"{row.get('time_exit')} | {row.get('t1_risk')} |"
        )
    lines.append("")
    return lines


def render_missed_alpha_radar(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## US Missed Alpha / Winner Hold Radar",
        "",
        "这些不是新买入清单。它们是被 fresh-entry gate 以追高、低 R:R、noisy/mean-reverting 等理由拦住，但如果已经持有则不该被机械清掉的延续票。动作是 hold runner / wait pullback-retest；加仓仍必须等新的 V2 ticket。",
        "",
    ]
    if not rows:
        lines += ["- No missed-alpha radar rows today.", ""]
        return lines
    lines += [
        "| State | Symbol | Confidence | Fresh entry | Hold overlay | Pullback/retest | Stop | R:R | Trend | Blockers |",
        "|---|---|---|---|---|---:|---:|---:|---|---|",
    ]
    for row in rows[:30]:
        blockers = ", ".join(str(item) for item in (row.get("blockers") or []) if item)
        lines.append(
            f"| {row.get('state')} | {row.get('symbol')} | {row.get('signal_confidence') or '-'} | "
            f"{row.get('fresh_entry_action')} | {row.get('hold_action')} | "
            f"{fmt_num(row.get('pullback_price') or row.get('entry'))} | {fmt_num(row.get('stop'))} | "
            f"{fmt_num(row.get('rr_ratio'))} | {row.get('trend_regime') or '-'} | {blockers or row.get('reason') or '-'} |"
        )
    lines.append("")
    return lines


def render_limit_table(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- No limit-up radar rows.", ""]
    lines = [
        "| State | Code | Name | p_limit_up | p_touch_limit | p_failed_board | EV after cost | Top decile | Model state |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:12]:
        lines.append(
            f"| Limit-Up Radar | {row.get('symbol')} | {row.get('name') or '-'} | "
            f"{fmt_pct((row.get('p_limit_up') or 0) * 100.0)} | "
            f"{fmt_pct((row.get('p_touch_limit') or 0) * 100.0)} | "
            f"{fmt_pct((row.get('p_failed_board') or 0) * 100.0)} | "
            f"{fmt_pct(row.get('ev_after_cost_pct'))} | {row.get('probability_decile')} | "
            f"{row.get('model_state')} |"
        )
    lines.append("")
    return lines


def render_freshness_table(title: str, freshness: dict[str, Any]) -> list[str]:
    lines = [f"### {title}", ""]
    rows = [
        ("V2", freshness.get("v2") or {}),
    ]
    if freshness.get("v2_stock_only_net"):
        rows.append(("V2 stock-only net", freshness.get("v2_stock_only_net") or {}))
    rows.append(("Legacy baseline", freshness.get("legacy") or {}))
    lines += [
        "| Strategy | Freshness state | Effective window | Rule | 7D LCB80 | 14D LCB80 | 30D LCB80 |",
        "|---|---|---:|---|---:|---:|---:|",
    ]
    for label, data in rows:
        by_window = {row.get("window_days"): row for row in data.get("windows") or []}
        effective = data.get("freshness_days")
        lines.append(
            f"| {label} | {data.get('state') or '-'} | {effective or '-'} | {data.get('rule') or '-'} | "
            f"{fmt_pct((by_window.get(7) or {}).get('lcb80_pct'))} | "
            f"{fmt_pct((by_window.get(14) or {}).get('lcb80_pct'))} | "
            f"{fmt_pct((by_window.get(30) or {}).get('lcb80_pct'))} |"
        )
    lines.append("")
    return lines


def render_cn_lifecycle_table(rows: list[dict[str, Any]], title: str) -> list[str]:
    lines = [
        f"### {title}",
        "",
        "| Bucket | n | Active days | Avg | Win | EV LCB80 | Avg MFE | Avg MAE | Avg hold |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if not rows:
        return lines + ["| - | 0 | 0 | - | - | - | - | - | - |", ""]
    for row in rows:
        lines.append(
            f"| {row.get('bucket')} | {row.get('n', 0)} | {row.get('active_dates', 0)} | "
            f"{fmt_pct(row.get('avg_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_pct(row.get('lcb80_pct'))} | {fmt_pct(row.get('avg_mfe_pct'))} | "
            f"{fmt_pct(row.get('avg_mae_pct'))} | {fmt_num(row.get('avg_hold_days'))} |"
        )
    lines.append("")
    return lines


def render_cn_lifecycle_section(cn: dict[str, Any]) -> list[str]:
    lifecycle = cn.get("lifecycle") or {}
    policy = lifecycle.get("policy") or {}
    summary = lifecycle.get("summary") or {}
    v2 = summary.get("v2_ev_positive") or {}
    all_rows = summary.get("all_oversold_diagnostic") or {}
    lines = [
        "## A 股生命周期研究 / CN Lifecycle",
        "",
        "A 股主线不是美股式 30D 持有。这里只用 `oversold_contrarian` 的 EV-positive 子桶做交易生命周期，所有同日退出都不算胜率；全体超跌只作为诊断，不能绕过 EV gate。",
        "",
        f"- Lifecycle state: `{policy.get('state') or '-'}`",
        f"- Best bucket: `{policy.get('best_bucket') or '-'}`; bucket LCB80 {fmt_pct(policy.get('best_bucket_lcb80_pct'))}",
        f"- Max hold: `T+{policy.get('max_hold_days') or '-'}`",
        f"- V2 EV-positive: n `{v2.get('n', 0)}`, avg {fmt_pct(v2.get('avg_pct'))}, LCB80 {fmt_pct(v2.get('lcb80_pct'))}",
        f"- All oversold diagnostic: n `{all_rows.get('n', 0)}`, avg {fmt_pct(all_rows.get('avg_pct'))}, LCB80 {fmt_pct(all_rows.get('lcb80_pct'))}",
        f"- Exit rule: {policy.get('first_review')}; {policy.get('follow_through_rule')}; {policy.get('time_stop')}",
        "",
    ]
    lines += render_cn_lifecycle_table(lifecycle.get("by_hold_bucket") or [], "EV-positive Hold Buckets")
    lines += render_cn_lifecycle_table(lifecycle.get("all_oversold_by_hold_bucket") or [], "All Oversold Diagnostic Hold Buckets")
    lines += render_cn_lifecycle_table(lifecycle.get("by_execution_mode") or [], "EV-positive By Execution Mode")
    return lines


def count_current_states(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        state = str(row.get("state") or "unknown")
        counts[state] = counts.get(state, 0) + 1
    return counts


def metrics_gate_passes(
    metrics: dict[str, Any],
    *,
    min_n: int,
    min_dates: int,
    max_drawdown_floor_pct: float,
) -> bool:
    return (
        int(metrics.get("n") or 0) >= min_n
        and int(metrics.get("active_dates") or 0) >= min_dates
        and (metrics.get("avg_pct") or 0.0) > 0.0
        and (metrics.get("lcb80_pct") or 0.0) > 0.0
        and (metrics.get("max_drawdown_pct") or 0.0) >= max_drawdown_floor_pct
    )


def build_profit_guardrails(us: dict[str, Any], cn: dict[str, Any], limit_up: dict[str, Any]) -> list[dict[str, Any]]:
    us_counts = count_current_states(us.get("current") or [])
    cn_counts = count_current_states(cn.get("current") or [])
    us_v2 = us["metrics"]["v2"]
    us_stock = us["metrics"].get("v2_stock_only_net") or {}
    us_opt = us["metrics"]["v2_options_confirmed"]
    cn_v2 = cn["metrics"]["v2"]
    us_fresh = (us.get("freshness") or {}).get("v2") or {}
    us_stock_fresh = (us.get("freshness") or {}).get("v2_stock_only_net") or {}
    cn_fresh = (cn.get("freshness") or {}).get("v2") or {}
    cn_lifecycle_policy = (cn.get("lifecycle") or {}).get("policy") or {}
    cn_lifecycle_ok = cn_lifecycle_policy.get("state") == "positive_lifecycle"
    us_metric_ok = metrics_gate_passes(us_v2, min_n=20, min_dates=10, max_drawdown_floor_pct=-8.0)
    us_stock_ok = metrics_gate_passes(us_stock, min_n=20, min_dates=8, max_drawdown_floor_pct=-8.0)
    cn_metric_ok = metrics_gate_passes(cn_v2, min_n=200, min_dates=10, max_drawdown_floor_pct=-8.0)

    if us_counts.get("Execution Alpha", 0) > 0 and us_metric_ok and int(us_opt.get("n") or 0) >= 20:
        us_state = "tradeable_small"
        us_size = "0.25R/name; 1R basket cap"
    elif (
        us_stock_ok
        and us_stock_fresh.get("state") in {"fresh", "usable_but_monitor"}
        and (us_counts.get("Execution Alpha", 0) + us_counts.get("Positive EV Setup", 0)) > 0
    ):
        us_state = "conditional_stock_probe"
        us_size = "0.10R/name; 0.50R basket cap; stock-only"
    elif (us_v2.get("lcb80_pct") or 0.0) > 0.0:
        us_state = "paper_or_watch_only"
        us_size = "0R auto"
    else:
        us_state = "blocked"
        us_size = "0R"

    if cn_counts.get("Execution Alpha", 0) > 0 and cn_metric_ok and cn_fresh.get("state") == "fresh" and cn_lifecycle_ok:
        cn_state = "conditional_small"
        cn_size = "0.25R/name; 1R basket cap; planned-entry only"
    elif cn_counts.get("Positive EV Setup", 0) > 0 and cn_metric_ok and cn_lifecycle_ok:
        cn_state = "review_only"
        cn_size = "0R auto; research/watch only"
    else:
        cn_state = "blocked_or_watch"
        cn_size = "0R auto"

    limit_perf = limit_up.get("performance") or {}
    return [
        {
            "market": "US",
            "profit_state": us_state,
            "max_auto_size": us_size,
            "why": (
                f"V2 LCB80 {fmt_pct(us_v2.get('lcb80_pct'))}, stock-net LCB80 {fmt_pct(us_stock.get('lcb80_pct'))}, "
                f"freshness={us_fresh.get('state') or '-'}, stock-net freshness={us_stock_fresh.get('state') or '-'}, "
                f"current Execution Alpha={us_counts.get('Execution Alpha', 0)}, "
                f"option-confirmed n={us_opt.get('n', 0)}"
            ),
            "kill_switch": "Stock probe is a bridge, not option validation; disable if stock-net rolling 30D LCB80 <= 0, no current V2 setup, or basket drawdown breaches -0.5R.",
        },
        {
            "market": "CN",
            "profit_state": cn_state,
            "max_auto_size": cn_size,
            "why": (
                f"V2 LCB80 {fmt_pct(cn_v2.get('lcb80_pct'))}, freshness={cn_fresh.get('state') or '-'}, "
                f"lifecycle={cn_lifecycle_policy.get('best_bucket') or '-'} / T+{cn_lifecycle_policy.get('max_hold_days') or '-'}, "
                f"current Execution Alpha={cn_counts.get('Execution Alpha', 0)}, "
                f"Positive EV Setup={cn_counts.get('Positive EV Setup', 0)}"
            ),
            "kill_switch": "Fear/high-vol clips size instead of blocking contrarian edge; disable if rolling 14D LCB80 <= 0, candidate LCB80 <= 0, lifecycle bucket LCB80 <= 0, or hard liquidity/limit blocker appears.",
        },
        {
            "market": "Limit-Up",
            "profit_state": "radar_only",
            "max_auto_size": "0R",
            "why": (
                f"top-decile lift={fmt_num(limit_perf.get('avg_top_decile_lift'))}, "
                f"avg EV after cost={fmt_pct(limit_perf.get('avg_next_ret_pct'))}; no 9:25/9:35 confirmation"
            ),
            "kill_switch": "Cannot promote to money strategy without auction/open confirmation and positive post-cost live EV.",
        },
    ]


def render_profit_guardrails(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Market | Profit state | Max auto size | Why | Kill switch |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row.get('market')} | {row.get('profit_state')} | {row.get('max_auto_size')} | "
            f"{row.get('why')} | {row.get('kill_switch')} |"
        )
    lines.append("")
    return lines


def load_my_book_overlay(as_of: date) -> dict[str, Any]:
    path = STACK_ROOT / "reports" / "review_dashboard" / "my_book_overlay" / as_of.isoformat() / "my_book_overlay.json"
    if not path.exists():
        return {"status": "missing", "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "path": str(path), "error": str(exc)}
    payload["status"] = "ok"
    payload["path"] = str(path)
    return payload


def load_cn_book_overlay(as_of: date) -> dict[str, Any]:
    path = STACK_ROOT / "reports" / "review_dashboard" / "cn_book_overlay" / as_of.isoformat() / "cn_book_overlay.json"
    if not path.exists():
        return {"status": "missing", "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "path": str(path), "error": str(exc)}
    payload["status"] = "ok"
    payload["path"] = str(path)
    return payload


def _overlay_final_r(overlay: dict[str, Any], market: str) -> float:
    return sum(
        float(row.get("final_r") or 0.0)
        for row in overlay.get("rows") or []
        if str(row.get("market") or "").upper() == market.upper()
    )


def _overlay_manual_probe_r(overlay: dict[str, Any], market: str) -> float:
    return sum(
        float(row.get("manual_probe_r") or 0.0)
        for row in overlay.get("rows") or []
        if str(row.get("market") or "").upper() == market.upper()
    )


def _row_symbols(rows: list[dict[str, Any]], *, state: str | None = None, market: str | None = None) -> str:
    selected = []
    for row in rows:
        if state and row.get("state") != state:
            continue
        if market and str(row.get("market") or "").upper() != market.upper():
            continue
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            selected.append(symbol)
    return ", ".join(selected[:8]) if selected else "-"


def build_profit_readiness(payload: dict[str, Any]) -> dict[str, Any]:
    as_of = parse_date(payload["as_of"])
    us = payload.get("us") or {}
    cn = payload.get("cn") or {}
    limit_up = payload.get("limit_up") or {}
    overlay = payload.get("portfolio_risk_overlay") or {}
    option_ledger = payload.get("option_shadow_ledger") or {}
    my_book = load_my_book_overlay(as_of)
    cn_book = load_cn_book_overlay(as_of)
    my_summary = my_book.get("summary") or {}
    cn_book_summary = cn_book.get("summary") or {}

    cn_guard = _guardrail_by_market(payload.get("profit_guardrails") or [], "CN")
    us_guard = _guardrail_by_market(payload.get("profit_guardrails") or [], "US")
    limit_guard = _guardrail_by_market(payload.get("profit_guardrails") or [], "Limit-Up")
    cn_current = cn.get("current") or []
    us_current = us.get("current") or []
    risk_rows = overlay.get("rows") or []
    cn_risk_rows = [row for row in risk_rows if str(row.get("market") or "").upper() == "CN"]
    cn_zero_reasons = sorted(
        {
            reason
            for row in cn_risk_rows
            if float(row.get("final_r") or 0.0) <= 0.0 or float(row.get("manual_probe_r") or 0.0) > 0.0
            for reason in (row.get("risk_reasons") or [])
        }
    )
    cn_final_r = _overlay_final_r(overlay, "CN")
    cn_manual_r = _overlay_manual_probe_r(overlay, "CN")
    cn_next_step = (
        "Do not chase open. Micro-probe is permitted only when the portfolio overlay assigns manual_probe_r > 0; otherwise keep it as watch/research and wait for evidence-gate upgrade."
        if cn_manual_r <= 0
        else "Do not chase open. If using micro-probe, cap at 0.05R, require planned-entry/pullback fill, and record fill/exit in CN live ledger."
    )
    option_summary = (option_ledger.get("summary") or {}).get("overall_long") or {}
    limit_perf = limit_up.get("performance") or {}

    rows = [
        {
            "area": "CN main alpha",
            "state": "manual_micro_probe_ready" if cn_manual_r > 0 else ("research_edge_ready_execution_not_ready" if cn_final_r <= 0 else "probe_ready"),
            "allowed_now": cn_guard.get("max_auto_size") or "0R",
            "evidence": (
                f"LCB80 {fmt_pct((cn.get('metrics') or {}).get('v2', {}).get('lcb80_pct'))}; "
                f"lifecycle {((cn.get('lifecycle') or {}).get('policy') or {}).get('best_bucket') or '-'}; "
                f"current EA={count_current_states(cn_current).get('Execution Alpha', 0)}"
            ),
            "blocker": (
                f"portfolio final CN R={fmt_num(cn_final_r, 4)}; manual probe R={fmt_num(cn_manual_r, 4)}; "
                f"risk reasons={', '.join(cn_zero_reasons) if cn_zero_reasons else 'none'}; "
                f"current={_row_symbols(cn_current, state='Execution Alpha')}"
            ),
            "next_step": cn_next_step,
            "priority": 1,
        },
        {
            "area": "US stock probe",
            "state": us_guard.get("profit_state") or "unknown",
            "allowed_now": us_guard.get("max_auto_size") or "0R",
            "evidence": (
                f"stock-net LCB80 {fmt_pct((us.get('metrics') or {}).get('v2_stock_only_net', {}).get('lcb80_pct'))}; "
                f"current Positive EV={count_current_states(us_current).get('Positive EV Setup', 0)}"
            ),
            "blocker": (
                f"My Book open={my_summary.get('open_positions', '-')}, cap="
                f"{(my_book.get('policy') or {}).get('single_name_position_cap', '-')}; "
                f"time_stop={my_summary.get('time_stop_positions', '-')}; "
                f"runners={my_summary.get('runner_positions', '-')}; "
                f"exit_reduce_losers={my_summary.get('exit_or_reduce_loser_positions', '-')}"
            ),
            "next_step": "Fresh buys still require a V2 ticket. Existing profitable names use Winner Hold Overlay: no full exit at +1R/+2R unless trailing stop or invalidation breaks.",
            "priority": 2,
        },
        {
            "area": "US options",
            "state": "not_money_ready",
            "allowed_now": "0R options",
            "evidence": f"option-confirmed n={option_summary.get('n', 0)}, LCB80 {fmt_pct(option_summary.get('lcb80_pct'))}",
            "blocker": f"resolved rows={option_ledger.get('resolved_count', 0)}, unresolved rows={option_ledger.get('unresolved_count', 0)}",
            "next_step": "Persist options_chain_quotes and options_alpha expression rows daily until option ledger has n>=20 and LCB80>0 after bid/ask costs.",
            "priority": 3,
        },
        {
            "area": "Limit-up",
            "state": limit_guard.get("profit_state") or "radar_only",
            "allowed_now": limit_guard.get("max_auto_size") or "0R",
            "evidence": f"top-decile lift={fmt_num(limit_perf.get('avg_top_decile_lift'))}, avg next-ret {fmt_pct(limit_perf.get('avg_next_ret_pct'))}",
            "blocker": "missing 9:25/9:35 auction/open confirmation and live post-cost execution ledger",
            "next_step": "Add auction gain, auction turnover, 9:35 volume ratio, sector co-move, seal strength, open-board count before any money promotion.",
            "priority": 4,
        },
        {
            "area": "Live execution ledger",
            "state": "cn_overlay_ready" if cn_book.get("status") == "ok" else "missing_for_cn_and_partial_for_us",
            "allowed_now": "manual review",
            "evidence": (
                f"My Book overlay status={my_book.get('status')}; CN Book status={cn_book.get('status')}; "
                f"CN manual-ready={cn_book_summary.get('manual_micro_probe_ready', '-')}; "
                f"portfolio gross R={fmt_num((overlay.get('summary') or {}).get('gross_r_after_caps'), 4)}"
            ),
            "blocker": (
                "CN live fills CSV missing" if cn_book.get("source_status") != "ok" else "live fills overlay exists; still needs realized exit updates"
            ),
            "next_step": "Set QUANT_CN_ACTIVITY_CSV with live fills; record order time, fill price, slippage, T+1 availability, and realized exit.",
            "priority": 5,
        },
    ]

    money_ready = [
        row
        for row in rows
        if row["area"] in {"CN main alpha", "US stock probe"}
        and "0R" not in str(row.get("allowed_now") or "")
        and "not_ready" not in str(row.get("state") or "")
    ]
    return {
        "as_of": payload["as_of"],
        "summary": {
            "money_ready_lines": len(money_ready),
            "highest_priority_blocker": rows[0]["blocker"],
            "today_bias": "CN research edge first, US stock probe only after book cleanup; options and limit-up stay shadow/radar",
        },
        "rows": sorted(rows, key=lambda row: int(row.get("priority") or 99)),
    }


def render_profit_readiness(payload: dict[str, Any]) -> str:
    readiness = payload.get("profit_readiness") or {}
    summary = readiness.get("summary") or {}
    lines = [
        f"# Profit Readiness - {payload['as_of']}",
        "",
        "This report translates research edges into money-readiness blockers. It does not guarantee profit; it shows what still prevents research EV from becoming controlled live PnL.",
        "",
        f"- Money-ready lines: `{summary.get('money_ready_lines', 0)}`",
        f"- Today bias: {summary.get('today_bias') or '-'}",
        f"- Highest priority blocker: {summary.get('highest_priority_blocker') or '-'}",
        "",
        "| Priority | Area | State | Allowed now | Evidence | Blocker | Next step |",
        "|---:|---|---|---|---|---|---|",
    ]
    for row in readiness.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('allowed_now')} | {row.get('evidence')} | {row.get('blocker')} | {row.get('next_step')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_profit_readiness_section(payload: dict[str, Any]) -> list[str]:
    readiness = payload.get("profit_readiness") or {}
    lines = [
        "## 赚钱落地缺口 / Profit Readiness",
        "",
        "这里专门回答“还差什么才能把研究 edge 变成可控实盘 PnL”。",
        "",
        "| Priority | Area | State | Allowed now | Blocker | Next step |",
        "|---:|---|---|---|---|---|",
    ]
    for row in readiness.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('allowed_now')} | {row.get('blocker')} | {row.get('next_step')} |"
        )
    lines.append("")
    return lines


def _guardrail_by_market(rows: list[dict[str, Any]], market: str) -> dict[str, Any]:
    target = market.upper()
    for row in rows:
        if str(row.get("market") or "").upper() == target:
            return row
    return {}


def _freshness_summary(data: dict[str, Any]) -> tuple[str, Any]:
    return str(data.get("state") or "-"), data.get("freshness_days")


def _current_summary(rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    counts = count_current_states(rows)
    return (
        counts.get("Execution Alpha", 0),
        counts.get("Positive EV Setup", 0),
        counts.get("Legacy / Blocked", 0),
    )


def build_strategy_direction(
    us: dict[str, Any],
    cn: dict[str, Any],
    limit_up: dict[str, Any],
    profit_guardrails: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    us_guard = _guardrail_by_market(profit_guardrails, "US")
    cn_guard = _guardrail_by_market(profit_guardrails, "CN")
    limit_guard = _guardrail_by_market(profit_guardrails, "Limit-Up")
    us_ea, us_pev, us_blocked = _current_summary(us.get("current") or [])
    cn_ea, cn_pev, cn_blocked = _current_summary(cn.get("current") or [])

    cn_v2 = cn["metrics"]["v2"]
    cn_lifecycle_policy = (cn.get("lifecycle") or {}).get("policy") or {}
    cn_v2_fresh, cn_v2_days = _freshness_summary((cn.get("freshness") or {}).get("v2") or {})
    us_stock = us["metrics"].get("v2_stock_only_net") or {}
    us_stock_fresh, us_stock_days = _freshness_summary((us.get("freshness") or {}).get("v2_stock_only_net") or {})
    us_option = us["metrics"].get("v2_options_confirmed") or {}
    us_v2_fresh, us_v2_days = _freshness_summary((us.get("freshness") or {}).get("v2") or {})
    cn_legacy = cn["metrics"]["legacy"]
    cn_legacy_fresh, cn_legacy_days = _freshness_summary((cn.get("freshness") or {}).get("legacy") or {})
    us_legacy = us["metrics"]["legacy"]
    us_legacy_fresh, us_legacy_days = _freshness_summary((us.get("freshness") or {}).get("legacy") or {})
    limit_perf = limit_up.get("performance") or {}

    rows = [
        {
            "market": "CN",
            "strategy_family": "oversold_contrarian",
            "direction": "fear/high-vol oversold reversal",
            "role": "primary",
            "tier": cn_guard.get("profit_state") or "blocked_or_watch",
            "max_size": cn_guard.get("max_auto_size") or "0R",
            "post_cost_lcb80_pct": cn_v2.get("lcb80_pct"),
            "avg_pct": cn_v2.get("avg_pct"),
            "n": cn_v2.get("n"),
            "active_dates": cn_v2.get("active_dates"),
            "max_drawdown_pct": cn_v2.get("max_drawdown_pct"),
            "freshness_state": cn_v2_fresh,
            "freshness_days": cn_v2_days,
            "current_execution_alpha": cn_ea,
            "current_positive_ev_setup": cn_pev,
            "current_blocked": cn_blocked,
            "reason": (
                "strongest current post-cost evidence; high fear/vol is edge context, not a US-style blocker; "
                f"lifecycle best={cn_lifecycle_policy.get('best_bucket') or '-'}, max=T+{cn_lifecycle_policy.get('max_hold_days') or '-'}"
            ),
            "kill_switch": cn_guard.get("kill_switch") or "",
        },
        {
            "market": "US",
            "strategy_family": "low_core_trending_stock_only",
            "direction": "LOW/core/executable trend continuation as stock probe",
            "role": "secondary_probe",
            "tier": us_guard.get("profit_state") or "paper_or_watch_only",
            "max_size": us_guard.get("max_auto_size") or "0R auto",
            "post_cost_lcb80_pct": us_stock.get("lcb80_pct"),
            "avg_pct": us_stock.get("avg_pct"),
            "n": us_stock.get("n"),
            "active_dates": us_stock.get("active_dates"),
            "max_drawdown_pct": us_stock.get("max_drawdown_pct"),
            "freshness_state": us_stock_fresh,
            "freshness_days": us_stock_days,
            "current_execution_alpha": us_ea,
            "current_positive_ev_setup": us_pev,
            "current_blocked": us_blocked,
            "reason": "stock-only net bridge is positive after cost; options expression still needs shadow PnL history",
            "kill_switch": us_guard.get("kill_switch") or "",
        },
        {
            "market": "US",
            "strategy_family": "low_core_trending_options_expression",
            "direction": "same V2 signal expressed through call_spread/stock_long",
            "role": "shadow_validation",
            "tier": "shadow_only",
            "max_size": "0R options",
            "post_cost_lcb80_pct": us_option.get("lcb80_pct"),
            "avg_pct": us_option.get("avg_pct"),
            "n": us_option.get("n"),
            "active_dates": us_option.get("active_dates"),
            "max_drawdown_pct": us_option.get("max_drawdown_pct"),
            "freshness_state": us_v2_fresh,
            "freshness_days": us_v2_days,
            "current_execution_alpha": us_ea,
            "current_positive_ev_setup": us_pev,
            "current_blocked": us_blocked,
            "reason": "required for full US Execution Alpha, but historical option-confirmed PnL is not populated yet",
            "kill_switch": "Promote only after option-confirmed n>=20, LCB80>0, liquidity pass, and live slippage review.",
        },
        {
            "market": "CN",
            "strategy_family": "limit_up_model",
            "direction": "daily limit-up ignition radar",
            "role": "radar",
            "tier": limit_guard.get("profit_state") or "radar_only",
            "max_size": limit_guard.get("max_auto_size") or "0R",
            "post_cost_lcb80_pct": None,
            "avg_pct": limit_perf.get("avg_next_ret_pct"),
            "n": limit_perf.get("days"),
            "active_dates": limit_perf.get("days"),
            "max_drawdown_pct": None,
            "freshness_state": "radar_only",
            "freshness_days": None,
            "current_execution_alpha": 0,
            "current_positive_ev_setup": 0,
            "current_blocked": 0,
            "reason": "top-decile lift is useful for a watchlist, but auction/open confirmation is missing",
            "kill_switch": limit_guard.get("kill_switch") or "",
            "top_decile_lift": limit_perf.get("avg_top_decile_lift"),
        },
        {
            "market": "CN",
            "strategy_family": "legacy_structural_core",
            "direction": "legacy A-share structural core baseline",
            "role": "blocked_baseline",
            "tier": "blocked",
            "max_size": "0R",
            "post_cost_lcb80_pct": cn_legacy.get("lcb80_pct"),
            "avg_pct": cn_legacy.get("avg_pct"),
            "n": cn_legacy.get("n"),
            "active_dates": cn_legacy.get("active_dates"),
            "max_drawdown_pct": cn_legacy.get("max_drawdown_pct"),
            "freshness_state": cn_legacy_fresh,
            "freshness_days": cn_legacy_days,
            "current_execution_alpha": 0,
            "current_positive_ev_setup": 0,
            "current_blocked": cn_blocked,
            "reason": "baseline only; lower confidence bound is negative",
            "kill_switch": "Cannot promote while full-period and rolling evidence remain negative.",
        },
        {
            "market": "US",
            "strategy_family": "legacy_high_mod_core",
            "direction": "legacy HIGH/MOD core baseline",
            "role": "blocked_baseline",
            "tier": "blocked",
            "max_size": "0R",
            "post_cost_lcb80_pct": us_legacy.get("lcb80_pct"),
            "avg_pct": us_legacy.get("avg_pct"),
            "n": us_legacy.get("n"),
            "active_dates": us_legacy.get("active_dates"),
            "max_drawdown_pct": us_legacy.get("max_drawdown_pct"),
            "freshness_state": us_legacy_fresh,
            "freshness_days": us_legacy_days,
            "current_execution_alpha": 0,
            "current_positive_ev_setup": 0,
            "current_blocked": us_blocked,
            "reason": "baseline only; full-period lower confidence bound is negative",
            "kill_switch": "Cannot promote while full-period EV LCB80 is negative.",
        },
    ]
    role_order = {
        "primary": 0,
        "secondary_probe": 1,
        "shadow_validation": 2,
        "radar": 3,
        "blocked_baseline": 4,
    }
    rows.sort(
        key=lambda row: (
            role_order.get(str(row.get("role")), 99),
            -(float(row.get("post_cost_lcb80_pct") or -999.0)),
        )
    )
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def render_strategy_direction_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Rank | Role | Market | Strategy family | Tier | Max size | LCB80 | Freshness | Current | Why |",
        "|---:|---|---|---|---|---|---:|---|---|---|",
    ]
    for row in rows:
        current = (
            f"EA={row.get('current_execution_alpha', 0)}, "
            f"PEV={row.get('current_positive_ev_setup', 0)}, "
            f"blocked={row.get('current_blocked', 0)}"
        )
        freshness_days = row.get("freshness_days")
        freshness = f"{row.get('freshness_state') or '-'}"
        if freshness_days:
            freshness += f" / {freshness_days}D"
        lines.append(
            f"| {row.get('rank')} | {row.get('role')} | {row.get('market')} | "
            f"{row.get('strategy_family')} | {row.get('tier')} | {row.get('max_size')} | "
            f"{fmt_pct(row.get('post_cost_lcb80_pct'))} | {freshness} | {current} | {row.get('reason')} |"
        )
    lines.append("")
    return lines


def render_adjustment_rules() -> list[str]:
    return [
        "## Adjustment Rules",
        "",
        "- This board is a rolling decision snapshot, not a permanent allocation. Re-rank daily after outcomes refresh.",
        "- CN oversold_contrarian stays primary while 14D LCB80 > 0, candidate LCB80 > 0, and planned-entry execution remains possible; cut to `probe` if freshness slips to 30D-only, and cut to `0R` if 14D LCB80 <= 0 or hard liquidity/limit blockers appear.",
        "- US stock-only stays a secondary probe while stock-net 30D LCB80 > 0 and current V2 setups exist; promote only after more active days and live slippage are acceptable, demote to `0R` if stock-net LCB80 <= 0 or basket drawdown breaches -0.5R.",
        "- US options remain shadow until option-confirmed n >= 20, LCB80 > 0, liquidity pass, and realized option/slippage review pass.",
        "- Limit-up remains radar until 9:25/9:35 auction/open features exist and post-cost live EV is positive.",
        "- Legacy can return only through the same EV/freshness gate; HIGH/MOD or structural_core labels alone never promote.",
        "",
    ]


def render_strategy_direction(payload: dict[str, Any]) -> str:
    rows = payload.get("strategy_direction") or []
    primary = next((row for row in rows if row.get("role") == "primary"), {})
    secondary = next((row for row in rows if row.get("role") == "secondary_probe"), {})
    radar = next((row for row in rows if row.get("role") == "radar"), {})
    lines = [
        f"# Strategy Direction Board - {payload['as_of']}",
        "",
        "## Current Decision Snapshot",
        "",
        (
            f"Primary: {primary.get('market', '-')} {primary.get('strategy_family', '-')} "
            f"({primary.get('tier', '-')}); secondary: {secondary.get('market', '-')} "
            f"{secondary.get('strategy_family', '-')} ({secondary.get('tier', '-')}); "
            f"radar: {radar.get('strategy_family', '-')} stays {radar.get('tier', '-')}. "
            "This is the current ranked state, not a fixed strategy allocation."
        ),
        "",
        "## Daily Board",
        "",
    ]
    lines += render_strategy_direction_table(rows)
    lines += render_adjustment_rules()
    lines += [
        "## Daily Questions",
        "",
        "1. Which family has the best post-cost LCB80 today?",
        "2. Is the edge fresh, decaying, or expired?",
        "3. What tier is allowed now: 0R, probe, conditional, or normal?",
        "",
        "## Promotion Ladder",
        "",
        "- `0R`: negative/unknown EV or missing execution evidence.",
        "- `probe`: positive after-cost evidence but incomplete expression or short freshness.",
        "- `conditional`: positive LCB80, fresh enough, current setup exists, and execution constraints define a capped entry plan.",
        "- `normal`: reserved for larger samples, stable freshness, and live/slippage evidence.",
        "",
        "## Kill Switches",
        "",
    ]
    for row in rows:
        if row.get("role") in {"primary", "secondary_probe", "shadow_validation", "radar"}:
            lines.append(f"- {row.get('market')} {row.get('strategy_family')}: {row.get('kill_switch')}")
    return "\n".join(lines).rstrip() + "\n"


def _mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _std(values: list[float]) -> float | None:
    return statistics.stdev(values) if len(values) >= 2 else None


def _corr(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 20:
        return None
    x = a[-n:]
    y = b[-n:]
    mx = statistics.fmean(x)
    my = statistics.fmean(y)
    vx = sum((v - mx) ** 2 for v in x)
    vy = sum((v - my) ** 2 for v in y)
    if vx <= 0 or vy <= 0:
        return None
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    return max(-1.0, min(1.0, cov / math.sqrt(vx * vy)))


def _returns_from_closes(values: list[float]) -> list[float]:
    returns: list[float] = []
    for prev, cur in zip(values, values[1:], strict=False):
        if prev and prev > 0 and cur is not None:
            returns.append(cur / prev - 1.0)
    return returns


def load_us_sectors(db_path: Path, symbols: list[str], as_of: date) -> dict[str, str]:
    if not db_path.exists() or not symbols:
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "universe_constituents"):
            return {}
        placeholders = ",".join("?" for _ in symbols)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT symbol, COALESCE(sector, 'Unknown') AS sector
            FROM (
                SELECT symbol, sector,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetched_date DESC) AS rn
                FROM universe_constituents
                WHERE fetched_date <= CAST(? AS DATE)
                  AND symbol IN ({placeholders})
            )
            WHERE rn = 1
            """,
            [as_of.isoformat(), *symbols],
        )
        return {str(row["symbol"]).upper(): row.get("sector") or "Unknown" for row in rows}
    finally:
        con.close()


def load_return_series(db_path: Path, market: str, symbols: list[str], as_of: date, lookback: int = 90) -> dict[str, list[float]]:
    if not db_path.exists() or not symbols:
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if market == "us":
            table, sym_col, date_col, close_col = "prices_daily", "symbol", "date", "close"
        else:
            table, sym_col, date_col, close_col = "prices", "ts_code", "trade_date", "close"
        if not table_exists(con, table):
            return {}
        start = as_of - timedelta(days=lookback * 2)
        placeholders = ",".join("?" for _ in symbols)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT {sym_col} AS symbol, {date_col} AS d, {close_col} AS close
            FROM {table}
            WHERE {date_col} >= CAST(? AS DATE)
              AND {date_col} <= CAST(? AS DATE)
              AND {sym_col} IN ({placeholders})
              AND {close_col} IS NOT NULL
            ORDER BY symbol, d
            """,
            [start.isoformat(), as_of.isoformat(), *symbols],
        )
    finally:
        con.close()
    closes: dict[str, list[float]] = {}
    for row in rows:
        closes.setdefault(str(row["symbol"]).upper(), []).append(float(row["close"]))
    return {symbol: _returns_from_closes(values)[-lookback:] for symbol, values in closes.items()}


def load_cn_shadow_option_risk(db_path: Path, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not db_path.exists() or not symbols:
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "analytics"):
            return {}
        placeholders = ",".join("?" for _ in symbols)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT ts_code, module, metric, value, detail
            FROM analytics
            WHERE as_of = CAST(? AS DATE)
              AND ts_code IN ({placeholders})
              AND (
                    (module = 'shadow_fast' AND metric IN ('downside_stress', 'shadow_iv_30d'))
                 OR (module = 'shadow_option_alpha' AND metric IN ('entry_quality_score', 'stale_chase_risk', 'shadow_alpha_prob'))
              )
            """,
            [as_of.isoformat(), *symbols],
        )
    finally:
        con.close()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row["ts_code"]).upper()
        out.setdefault(symbol, {})[row["metric"]] = round_or_none(row.get("value"))
        if row.get("detail") and "detail_json" not in out[symbol]:
            out[symbol]["detail_json"] = row.get("detail")
    return out


def _candidate_key(row: dict[str, Any]) -> str:
    return f"{row.get('market')}:{row.get('symbol')}"


def cn_manual_micro_probe_allowed(row: dict[str, Any]) -> bool:
    return (
        str(row.get("market") or "").upper() == "CN"
        and row.get("state") == "Execution Alpha"
        and str(row.get("strategy_family") or "") == "oversold_contrarian"
        and (row.get("ev_lcb80_pct") or 0.0) > 0.0
        and int(row.get("max_hold_days") or 99) <= CN_MAX_LIFECYCLE_HOLD_DAYS
        and "manual_probe" in str(row.get("lifecycle_action") or "")
    )


def _cluster_ids(rows: list[dict[str, Any]], corr: dict[tuple[str, str], float]) -> dict[str, int]:
    parent = {row["key"]: row["key"] for row in rows}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for (a, b), value in corr.items():
        if value >= CORR_CLUSTER_THRESHOLD:
            union(a, b)
    roots: dict[str, int] = {}
    out: dict[str, int] = {}
    for key in parent:
        root = find(key)
        if root not in roots:
            roots[root] = len(roots) + 1
        out[key] = roots[root]
    return out


def build_portfolio_risk_overlay(
    us: dict[str, Any],
    cn: dict[str, Any],
    limit_up: dict[str, Any],
    profit_guardrails: list[dict[str, Any]],
    us_db: Path,
    cn_db: Path,
    as_of: date,
) -> dict[str, Any]:
    us_guard = _guardrail_by_market(profit_guardrails, "US")
    cn_guard = _guardrail_by_market(profit_guardrails, "CN")
    cn_profit_state = str(cn_guard.get("profit_state") or "")
    cn_allows_auto = cn_profit_state == "conditional_small"
    rows: list[dict[str, Any]] = []

    for row in cn.get("current") or []:
        if row.get("state") not in {"Execution Alpha", "Positive EV Setup"}:
            continue
        if (row.get("ev_lcb80_pct") or 0.0) <= 0.0:
            continue
        base_r = 0.25 if row.get("state") == "Execution Alpha" else 0.10
        risk_reasons: list[str] = []
        if not cn_allows_auto:
            base_r = 0.0
            risk_reasons.append(f"profit_guardrail_{cn_profit_state or 'missing'}")
        rows.append(
            {
                "key": f"CN:{row.get('symbol')}",
                "market": "CN",
                "symbol": row.get("symbol"),
                "name": row.get("name") or "",
                "state": row.get("state"),
                "strategy_family": row.get("policy"),
                "sector": row.get("industry") or "Unknown",
                "ev_lcb80_pct": row.get("ev_lcb80_pct"),
                "observation_entry_zone": row.get("observation_entry_zone"),
                "handling_line": row.get("handling_line"),
                "first_target": row.get("first_target"),
                "lifecycle_action": row.get("lifecycle_action"),
                "time_exit": row.get("time_exit"),
                "max_hold_days": row.get("max_hold_days"),
                "base_r": base_r,
                "final_r": base_r,
                "manual_probe_r": 0.0,
                "auto_eligible": base_r > 0.0,
                "risk_reasons": risk_reasons,
                "shadow_option_haircut": 1.0,
            }
        )

    us_profit_state = str(us_guard.get("profit_state") or "")
    us_allows_probe = us_profit_state.startswith("conditional") or us_profit_state.startswith("tradeable")
    for row in us.get("current") or []:
        if row.get("state") not in {"Execution Alpha", "Positive EV Setup"}:
            continue
        base_r = 0.25 if row.get("state") == "Execution Alpha" else 0.10
        risk_reasons = []
        if not us_allows_probe:
            base_r = 0.0
            risk_reasons.append(f"profit_guardrail_{us_profit_state or 'missing'}")
        rows.append(
            {
                "key": f"US:{row.get('symbol')}",
                "market": "US",
                "symbol": row.get("symbol"),
                "name": row.get("name") or "",
                "state": row.get("state"),
                "strategy_family": row.get("policy"),
                "sector": "Unknown",
                "base_r": base_r,
                "final_r": base_r,
                "manual_probe_r": 0.0,
                "auto_eligible": base_r > 0.0,
                "risk_reasons": risk_reasons,
                "shadow_option_haircut": 1.0,
            }
        )

    us_symbols = [str(row["symbol"]).upper() for row in rows if row["market"] == "US"]
    cn_symbols = [str(row["symbol"]).upper() for row in rows if row["market"] == "CN"]
    sectors = load_us_sectors(us_db, us_symbols, as_of)
    for row in rows:
        if row["market"] == "US":
            row["sector"] = sectors.get(str(row["symbol"]).upper(), "Unknown")

    shadow = load_cn_shadow_option_risk(cn_db, cn_symbols, as_of)
    for row in rows:
        if row["market"] != "CN":
            continue
        metrics = shadow.get(str(row["symbol"]).upper(), {})
        row["shadow_option"] = metrics
        entry_quality = metrics.get("entry_quality_score")
        stale = metrics.get("stale_chase_risk")
        downside = metrics.get("downside_stress")
        haircut = 1.0
        if (entry_quality is not None and entry_quality < 0.45) or (stale is not None and stale >= 0.62) or (
            downside is not None and downside >= 0.85
        ):
            haircut = 0.0
            row["risk_reasons"].append("cn_shadow_option_zero")
        elif (entry_quality is not None and entry_quality < 0.55) or (stale is not None and stale >= 0.44) or (
            downside is not None and downside >= 0.70
        ):
            haircut = 0.5
            row["risk_reasons"].append("cn_shadow_option_half")
        elif not metrics:
            row["risk_reasons"].append("cn_shadow_option_missing")
        row["shadow_option_haircut"] = haircut
        row["final_r"] *= haircut
        if row["final_r"] <= 0.0 and cn_manual_micro_probe_allowed(row):
            row["manual_probe_r"] = CN_MANUAL_MICRO_PROBE_R
            row["final_r"] = CN_MANUAL_MICRO_PROBE_R
            row["auto_eligible"] = False
            row["risk_reasons"].append("cn_manual_micro_probe_override")
            row["risk_reasons"].append("planned_entry_or_pullback_only")

    # Sector caps.
    for market in ["CN", "US"]:
        cap = SECTOR_R_CAP
        sectors_seen = sorted({row["sector"] for row in rows if row["market"] == market})
        for sector in sectors_seen:
            group = [row for row in rows if row["market"] == market and row["sector"] == sector]
            total = sum(float(row["final_r"]) for row in group)
            if total > cap and total > 0:
                scale = cap / total
                for row in group:
                    row["final_r"] *= scale
                    row["risk_reasons"].append(f"sector_cap_{sector}")

    returns = {}
    returns.update({f"US:{k}": v for k, v in load_return_series(us_db, "us", us_symbols, as_of).items()})
    returns.update({f"CN:{k}": v for k, v in load_return_series(cn_db, "cn", cn_symbols, as_of).items()})
    corr: dict[tuple[str, str], float] = {}
    for i, left in enumerate(rows):
        for right in rows[i + 1 :]:
            value = _corr(returns.get(left["key"], []), returns.get(right["key"], []))
            if value is not None:
                corr[(left["key"], right["key"])] = value
    clusters = _cluster_ids(rows, corr)
    for row in rows:
        row["corr_cluster_id"] = clusters.get(row["key"])
    for cluster_id in sorted(set(clusters.values())):
        group = [row for row in rows if row.get("corr_cluster_id") == cluster_id]
        total = sum(float(row["final_r"]) for row in group)
        if total > CORR_CLUSTER_R_CAP and total > 0:
            scale = CORR_CLUSTER_R_CAP / total
            for row in group:
                row["final_r"] *= scale
                row["risk_reasons"].append(f"corr_cluster_cap_{cluster_id}")

    gross_r = sum(float(row["final_r"]) for row in rows)
    if gross_r > PORTFOLIO_TOTAL_R_CAP and gross_r > 0:
        scale = PORTFOLIO_TOTAL_R_CAP / gross_r
        for row in rows:
            row["final_r"] *= scale
            row["risk_reasons"].append("total_portfolio_cap")

    variance = 0.0
    for i, left in enumerate(rows):
        for j, right in enumerate(rows):
            if i == j:
                c = 1.0
            else:
                a, b = left["key"], right["key"]
                c = corr.get((a, b), corr.get((b, a), 0.20))
            variance += float(left["final_r"]) * float(right["final_r"]) * c
    var95 = 1.65 * math.sqrt(max(variance, 0.0)) if rows else 0.0
    var_scale = 1.0
    if var95 > PORTFOLIO_VAR95_R_CAP and var95 > 0:
        var_scale = PORTFOLIO_VAR95_R_CAP / var95
        for row in rows:
            row["final_r"] *= var_scale
            row["risk_reasons"].append("var95_cap")
        var95 = PORTFOLIO_VAR95_R_CAP

    for row in rows:
        row["base_r"] = round_or_none(row["base_r"], 4)
        row["final_r"] = round_or_none(row["final_r"], 4)
        row["risk_reasons"] = row["risk_reasons"] or ["pass"]

    return {
        "as_of": as_of.isoformat(),
        "rows": rows,
        "summary": {
            "candidate_count": len(rows),
            "gross_r_after_caps": round_or_none(sum(float(row["final_r"] or 0.0) for row in rows), 4),
            "var95_r_proxy": round_or_none(var95, 4),
            "var_scale": round_or_none(var_scale, 4),
            "sector_cap_r": SECTOR_R_CAP,
            "corr_cluster_cap_r": CORR_CLUSTER_R_CAP,
            "total_cap_r": PORTFOLIO_TOTAL_R_CAP,
            "limit_up_budget_r": 0.0 if limit_up.get("current") else 0.0,
        },
    }


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_call_price(s: float, k: float, t: float, sigma: float, r: float = 0.04) -> float | None:
    if s <= 0 or k <= 0 or t <= 0 or sigma <= 0:
        return None
    vol_t = sigma * math.sqrt(t)
    if vol_t <= 0:
        return max(s - k, 0.0)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / vol_t
    d2 = d1 - vol_t
    return s * _norm_cdf(d1) - k * math.exp(-r * t) * _norm_cdf(d2)


def _spread_fraction(value: Any) -> float:
    parsed = round_or_none(value)
    if parsed is None:
        return 0.20
    return max(0.0, min(parsed if parsed <= 1.0 else parsed / 100.0, 0.95))


def _option_analysis_row(con: duckdb.DuckDBPyConnection, symbol: str, as_of: str, expiry: str | None) -> dict[str, Any] | None:
    if not table_exists(con, "options_analysis"):
        return None
    params: list[Any] = [symbol, as_of]
    expiry_clause = ""
    if expiry:
        expiry_clause = "AND expiry = ?"
        params.append(expiry)
    rows = rows_as_dicts(
        con,
        f"""
        SELECT *
        FROM options_analysis
        WHERE symbol = ?
          AND as_of = CAST(? AS DATE)
          {expiry_clause}
        ORDER BY days_to_exp ASC
        LIMIT 1
        """,
        params,
    )
    return rows[0] if rows else None


def _quote_leg(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    as_of: str,
    expiry: str,
    target_strike: float,
) -> dict[str, Any] | None:
    if not table_exists(con, "options_chain_quotes"):
        return None
    rows = rows_as_dicts(
        con,
        """
        SELECT *
        FROM options_chain_quotes
        WHERE symbol = ?
          AND as_of = CAST(? AS DATE)
          AND expiry = ?
          AND option_type = 'call'
          AND bid IS NOT NULL
          AND ask IS NOT NULL
          AND bid > 0
          AND ask > 0
        ORDER BY ABS(strike - ?) ASC
        LIMIT 1
        """,
        [symbol, as_of, expiry, target_strike],
    )
    return rows[0] if rows else None


def _call_spread_from_quotes(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    entry_date: str,
    exit_date: str,
    expiry: str,
    entry_price: float,
) -> tuple[float | None, dict[str, Any]]:
    long_entry = _quote_leg(con, symbol, entry_date, expiry, entry_price)
    short_entry = _quote_leg(con, symbol, entry_date, expiry, entry_price * 1.05)
    if not long_entry or not short_entry:
        return None, {"reason": "missing_entry_leg_quotes"}
    long_exit = _quote_leg(con, symbol, exit_date, expiry, float(long_entry["strike"]))
    short_exit = _quote_leg(con, symbol, exit_date, expiry, float(short_entry["strike"]))
    if not long_exit or not short_exit:
        return None, {"reason": "missing_exit_leg_quotes"}
    entry_debit = float(long_entry["ask"]) - float(short_entry["bid"])
    exit_value = float(long_exit["bid"]) - float(short_exit["ask"])
    if entry_debit <= 0:
        return None, {"reason": "non_positive_entry_debit"}
    commission_pct = (2.0 * OPTION_COMMISSION_PER_LEG) / (entry_debit * OPTION_CONTRACT_MULTIPLIER) * 100.0
    ret = (exit_value - entry_debit) / entry_debit * 100.0 - commission_pct
    return ret, {
        "pricing_mode": "leg_quotes",
        "long_strike": long_entry["strike"],
        "short_strike": short_entry["strike"],
        "entry_debit": round_or_none(entry_debit, 4),
        "exit_value": round_or_none(exit_value, 4),
        "commission_pct": round_or_none(commission_pct, 4),
    }


def _call_spread_proxy(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    entry_date: str,
    exit_date: str,
    expiry: str | None,
    entry_price: float,
    underlying_ret_pct: float,
) -> tuple[float | None, dict[str, Any]]:
    entry = _option_analysis_row(con, symbol, entry_date, expiry)
    if not entry:
        return None, {"reason": "missing_entry_options_analysis"}
    exit_row = _option_analysis_row(con, symbol, exit_date, str(entry.get("expiry") or expiry or ""))
    s0 = round_or_none(entry.get("current_price")) or entry_price
    s1 = s0 * (1.0 + underlying_ret_pct / 100.0)
    sigma0 = round_or_none(entry.get("atm_iv")) or 0.0
    sigma1 = round_or_none((exit_row or {}).get("atm_iv")) or sigma0
    dte0 = max(int(entry.get("days_to_exp") or 3), 1)
    dte1 = max(dte0 - 3, 1)
    k1 = s0
    k2 = s0 * 1.05
    long0 = _bs_call_price(s0, k1, dte0 / 365.0, sigma0)
    short0 = _bs_call_price(s0, k2, dte0 / 365.0, sigma0)
    long1 = _bs_call_price(s1, k1, dte1 / 365.0, sigma1)
    short1 = _bs_call_price(s1, k2, dte1 / 365.0, sigma1)
    if None in {long0, short0, long1, short1}:
        return None, {"reason": "proxy_bs_failed"}
    spread = _spread_fraction(entry.get("avg_spread_pct"))
    theoretical_debit = float(long0) - float(short0)
    theoretical_exit = float(long1) - float(short1)
    entry_debit = theoretical_debit * (1.0 + spread / 2.0)
    exit_value = theoretical_exit * max(0.0, 1.0 - spread / 2.0)
    if entry_debit <= 0:
        return None, {"reason": "non_positive_proxy_debit"}
    commission_pct = (2.0 * OPTION_COMMISSION_PER_LEG) / (entry_debit * OPTION_CONTRACT_MULTIPLIER) * 100.0
    ret = (exit_value - entry_debit) / entry_debit * 100.0 - commission_pct
    return ret, {
        "pricing_mode": "proxy_bs",
        "expiry": entry.get("expiry"),
        "long_strike": round_or_none(k1, 4),
        "short_strike": round_or_none(k2, 4),
        "entry_debit": round_or_none(entry_debit, 4),
        "exit_value": round_or_none(exit_value, 4),
        "avg_spread_fraction": round_or_none(spread, 4),
        "commission_pct": round_or_none(commission_pct, 4),
    }


def build_option_shadow_ledger(us_db: Path, start: date, as_of: date) -> dict[str, Any]:
    rows, status = load_us_rows(us_db, start, as_of)
    v2_rows = [row for row in rows if is_us_v2_policy(row)]
    options_history = load_us_options_range(us_db, start, as_of)
    con = duckdb.connect(str(us_db), read_only=True) if us_db.exists() else None
    ledger: list[dict[str, Any]] = []
    try:
        for row in v2_rows:
            symbol = str(row.get("symbol") or "").upper()
            entry_date = as_iso(row.get("report_date")) or ""
            exit_date = as_iso(row.get("evaluation_date")) or entry_date
            alpha = options_history.get((entry_date, symbol))
            expression = str((alpha or {}).get("expression") or "missing")
            ret = round_or_none(row.get("return_pct"))
            entry_price = round_or_none(row.get("entry_price") or row.get("reference_price")) or 0.0
            detail = safe_json_loads((alpha or {}).get("detail_json"))
            expiry = str(detail.get("expiry") or "") or None
            ledger_row = {
                "report_date": entry_date,
                "evaluation_date": exit_date,
                "symbol": symbol,
                "expression": expression,
                "underlying_return_pct": ret,
                "pricing_mode": "unresolved",
                "resolved": False,
                "long_expression": expression in {"call_spread", "stock_long"},
                "return_pct": None,
                "reason": "",
                "detail": {},
            }
            if expression == "stock_long" and ret is not None:
                ledger_row.update(
                    {
                        "pricing_mode": "stock_long",
                        "resolved": True,
                        "return_pct": ret - US_STOCK_ROUNDTRIP_COST_PCT,
                        "reason": "stock_long net after roundtrip cost",
                        "detail": {"roundtrip_cost_pct": US_STOCK_ROUNDTRIP_COST_PCT},
                    }
                )
            elif expression == "call_spread" and ret is not None and entry_price > 0 and con is not None:
                quote_ret, quote_detail = (None, {"reason": "missing_chain_quote_table"})
                if expiry:
                    quote_ret, quote_detail = _call_spread_from_quotes(con, symbol, entry_date, exit_date, expiry, entry_price)
                if quote_ret is not None:
                    ledger_row.update(
                        {
                            "pricing_mode": "leg_quotes",
                            "resolved": True,
                            "return_pct": quote_ret,
                            "reason": "call spread marked from bid/ask leg quotes",
                            "detail": quote_detail,
                        }
                    )
                else:
                    proxy_ret, proxy_detail = _call_spread_proxy(con, symbol, entry_date, exit_date, expiry, entry_price, ret)
                    if proxy_ret is not None:
                        ledger_row.update(
                            {
                                "pricing_mode": "proxy_bs",
                                "resolved": True,
                                "return_pct": proxy_ret,
                                "reason": "call spread proxy from options_analysis IV and spread",
                                "detail": {"quote_attempt": quote_detail, **proxy_detail},
                            }
                        )
                    else:
                        ledger_row["reason"] = str(proxy_detail.get("reason") or quote_detail.get("reason"))
                        ledger_row["detail"] = {"quote_attempt": quote_detail, "proxy_attempt": proxy_detail}
            else:
                ledger_row["reason"] = "non-long expression or missing options alpha"
            ledger.append(ledger_row)
    finally:
        if con is not None:
            con.close()

    resolved_long = [
        {"report_date": row["report_date"], "return_pct": row["return_pct"]}
        for row in ledger
        if row.get("resolved") and row.get("long_expression") and row.get("return_pct") is not None
    ]
    summary = {"overall_long": compute_metrics("US option shadow long expressions", resolved_long).to_dict()}
    for key in ["pricing_mode", "expression"]:
        groups = sorted({str(row.get(key)) for row in ledger if row.get("resolved") and row.get("return_pct") is not None})
        summary[f"by_{key}"] = {
            group: compute_metrics(
                f"{key}={group}",
                [
                    {"report_date": row["report_date"], "return_pct": row["return_pct"]}
                    for row in ledger
                    if row.get(key) == group and row.get("resolved") and row.get("return_pct") is not None
                ],
            ).to_dict()
            for group in groups
        }
    return {
        "status": status,
        "rows": ledger,
        "summary": summary,
        "resolved_count": sum(1 for row in ledger if row.get("resolved")),
        "unresolved_count": sum(1 for row in ledger if not row.get("resolved")),
    }


def render_portfolio_risk_overlay(payload: dict[str, Any]) -> str:
    overlay = payload.get("portfolio_risk_overlay") or {}
    summary = overlay.get("summary") or {}
    lines = [
        f"# Portfolio Risk Overlay - {payload['as_of']}",
        "",
        f"- Candidates: {summary.get('candidate_count', 0)}",
        f"- Final gross R: {fmt_num(summary.get('gross_r_after_caps'), 4)}",
        f"- VaR95 R proxy: {fmt_num(summary.get('var95_r_proxy'), 4)}",
        "",
        "| Market | Symbol | State | Sector | Base R | Final R | Manual R | Auto | Shadow haircut | Reasons |",
        "|---|---|---|---|---:|---:|---:|---|---:|---|",
    ]
    for row in overlay.get("rows") or []:
        lines.append(
            f"| {row.get('market')} | {row.get('symbol')} | {row.get('state')} | {row.get('sector')} | "
            f"{fmt_num(row.get('base_r'), 4)} | {fmt_num(row.get('final_r'), 4)} | "
            f"{fmt_num(row.get('manual_probe_r'), 4)} | {fmt_bool(bool(row.get('auto_eligible')))} | "
            f"{fmt_num(row.get('shadow_option_haircut'), 2)} | {', '.join(row.get('risk_reasons') or [])} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_option_shadow_ledger(payload: dict[str, Any]) -> str:
    ledger = payload.get("option_shadow_ledger") or {}
    overall = ((ledger.get("summary") or {}).get("overall_long") or {})
    lines = [
        f"# US Option Shadow Ledger - {payload['as_of']}",
        "",
        f"- Resolved rows: {ledger.get('resolved_count', 0)}",
        f"- Unresolved rows: {ledger.get('unresolved_count', 0)}",
        f"- Overall long-expression LCB80: {fmt_pct(overall.get('lcb80_pct'))}",
        "",
        "| Date | Symbol | Expression | Pricing mode | Return | Reason |",
        "|---|---|---|---|---:|---|",
    ]
    for row in (ledger.get("rows") or [])[:40]:
        lines.append(
            f"| {row.get('report_date')} | {row.get('symbol')} | {row.get('expression')} | "
            f"{row.get('pricing_mode')} | {fmt_pct(row.get('return_pct'))} | {row.get('reason')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_portfolio_risk_overlay_section(payload: dict[str, Any]) -> list[str]:
    overlay = payload.get("portfolio_risk_overlay") or {}
    summary = overlay.get("summary") or {}
    lines = [
        "## 组合风险覆盖 / Portfolio Risk Overlay",
        "",
        "这里不是重新选股，而是把已经通过策略裁决的候选再过一层组合约束：单票 R、行业暴露、相关簇、组合 VaR95，以及 A 股 shadow option 风险折扣。A 股 shadow option 只做风险输入，不当作可交易期权 PnL。",
        "",
        f"- Candidates after strategy gate: {summary.get('candidate_count', 0)}",
        f"- Final gross R after caps: {fmt_num(summary.get('gross_r_after_caps'), 4)}",
        f"- VaR95 R proxy: {fmt_num(summary.get('var95_r_proxy'), 4)}",
        f"- Caps: total {fmt_num(summary.get('total_cap_r'), 2)}R, sector {fmt_num(summary.get('sector_cap_r'), 2)}R, correlation cluster {fmt_num(summary.get('corr_cluster_cap_r'), 2)}R",
        "",
    ]
    rows = overlay.get("rows") or []
    if not rows:
        lines += ["- No current candidates receive risk budget after V2 guardrails.", ""]
        return lines
    lines += [
        "| Market | Symbol | State | Sector | Base R | Final R | Manual R | Auto | Shadow haircut | Reasons |",
        "|---|---|---|---|---:|---:|---:|---|---:|---|",
    ]
    for row in rows[:20]:
        lines.append(
            f"| {row.get('market')} | {row.get('symbol')} | {row.get('state')} | {row.get('sector')} | "
            f"{fmt_num(row.get('base_r'), 4)} | {fmt_num(row.get('final_r'), 4)} | "
            f"{fmt_num(row.get('manual_probe_r'), 4)} | {fmt_bool(bool(row.get('auto_eligible')))} | "
            f"{fmt_num(row.get('shadow_option_haircut'), 2)} | {', '.join(row.get('risk_reasons') or [])} |"
        )
    lines.append("")
    return lines


def render_option_shadow_ledger_section(payload: dict[str, Any]) -> list[str]:
    ledger = payload.get("option_shadow_ledger") or {}
    overall = ((ledger.get("summary") or {}).get("overall_long") or {})
    lines = [
        "## US Option Shadow PnL Ledger",
        "",
        "美股期权表达不再只停留在口头建议：已有真实 `options_chain_quotes` 时按 bid/ask legs 记账；历史没有 leg quote 时用 `options_analysis` 的 IV、期限和 spread 做 Black-Scholes proxy。只有 ledger 样本、LCB80、滑点一起过线后，期权才可能从 shadow 升级。",
        "",
        f"- Resolved rows: {ledger.get('resolved_count', 0)}",
        f"- Unresolved rows: {ledger.get('unresolved_count', 0)}",
        f"- Long-expression n: {overall.get('n', 0)}",
        f"- Long-expression LCB80: {fmt_pct(overall.get('lcb80_pct'))}",
        "",
    ]
    rows = ledger.get("rows") or []
    if not rows:
        lines += ["- No V2 US rows available for option shadow marking.", ""]
        return lines
    lines += [
        "| Date | Symbol | Expression | Pricing mode | Underlying | Option return | Reason |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for row in rows[:20]:
        lines.append(
            f"| {row.get('report_date')} | {row.get('symbol')} | {row.get('expression')} | "
            f"{row.get('pricing_mode')} | {fmt_pct(row.get('underlying_return_pct'))} | "
            f"{fmt_pct(row.get('return_pct'))} | {row.get('reason')} |"
        )
    lines.append("")
    return lines


def render_cn_lifecycle_research(payload: dict[str, Any]) -> str:
    lines = [
        f"# CN Oversold Lifecycle Research - {payload['as_of']}",
        "",
        f"Range: {payload['start']} to {payload['as_of']}.",
        "",
    ]
    lines += render_cn_lifecycle_section(payload.get("cn") or {})
    lines += [
        "## Daily Usage",
        "",
        "- `oversold_contrarian` rows with candidate EV LCB80 > 0 can enter Positive EV Setup; Execution Alpha additionally requires `alpha_state=positive_ev_setup` and a passing `strategy_ev.eligible` evidence gate.",
        "- Fear/high-vol is a size clipper and pullback requirement, not a US-style trend blocker.",
        "- `do_not_chase` means no open chase: wait for planned entry / pullback, then manage by T+1/T+3/T+max.",
        "- All non-EV-positive oversold rows remain watch-only diagnostics.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def render_report(payload: dict[str, Any]) -> str:
    us = payload["us"]
    cn = payload["cn"]
    limit_up = payload["limit_up"]
    as_of = payload["as_of"]
    start = payload["start"]
    us_v2 = us["metrics"]["v2"]
    us_legacy = us["metrics"]["legacy"]
    cn_v2 = cn["metrics"]["v2"]
    cn_legacy = cn["metrics"]["legacy"]
    conclusion = (
        f"V2 replaces legacy as the main strategy: US LOW/core/trending has LCB80 "
        f"{fmt_pct(us_v2.get('lcb80_pct'))} vs legacy {fmt_pct(us_legacy.get('lcb80_pct'))}; "
        f"CN oversold_contrarian has LCB80 {fmt_pct(cn_v2.get('lcb80_pct'))} vs structural_core "
        f"{fmt_pct(cn_legacy.get('lcb80_pct'))}; limit-up stays Radar until auction/open data arrives."
    )

    lines: list[str] = [
        f"# Main Strategy V2 Backtest - {as_of}",
        "",
        f"Range: {start} to {as_of}.",
        "",
        "## 一句话结论",
        "",
        conclusion,
        "",
        "## 赚钱优先裁决 / Profit Guardrails",
        "",
        "主策略名不是交易许可。真钱只看净 EV、LCB80、回撤、新鲜期、样本覆盖和执行数据；不满足时宁可少做，只保留纸面交易或观察。",
        "",
    ]
    lines += render_profit_guardrails(payload.get("profit_guardrails") or [])
    lines += render_profit_readiness_section(payload)
    lines += [
        "## 策略方向裁决 / Strategy Direction",
        "",
        "这不是永久固化的配置，而是每天滚动重排的裁决快照：哪个策略族的 post-cost LCB80 最高，edge 还新不新鲜，现在允许几档仓位。",
        "",
    ]
    lines += render_strategy_direction_table(payload.get("strategy_direction") or [])
    lines += render_adjustment_rules()
    lines += render_portfolio_risk_overlay_section(payload)
    lines += render_option_shadow_ledger_section(payload)
    lines += [
        "## 美股 V2 vs legacy",
        "",
        "V2 rule: LOW confidence + core + executable_now + trending regime. Execution Alpha additionally needs a long options expression (`call_spread` or `stock_long`). HIGH/MOD core is retained only as legacy baseline.",
        "",
    ]
    lines += render_metrics_table(
        [
            us["metrics"]["v2"],
            us["metrics"]["v2_stock_only_net"],
            us["metrics"]["v2_options_confirmed"],
            us["metrics"]["legacy"],
        ]
    )
    lines += [
        "",
        f"- Current US candidate date: {us.get('current_date') or '-'}",
        f"- Options rows available for latest screen: {us.get('options_coverage_rows', 0)}",
        f"- Stock-only bridge: subtracts {US_STOCK_ROUNDTRIP_COST_PCT:.2f}% roundtrip cost from the underlying 3-session result; this can only support a tiny probe until options expression PnL has enough history.",
        "- Why not HIGH/MOD: legacy HIGH/MOD is no longer the main policy and must be blocked when EV LCB80 is not positive.",
        "",
    ]
    lines += render_missed_alpha_radar(us.get("missed_alpha_radar") or [])
    lines += [
        "## 策略新鲜期 / Freshness",
        "",
        "主策略不是永久身份。这里用滚动 7/14/30/45/60 日窗口重新计算 EV LCB80；短窗口通过才叫 fresh，只有长窗口通过则按衰减处理。",
        "",
    ]
    lines += render_freshness_table("US freshness", us.get("freshness") or {})
    lines += render_freshness_table("CN freshness", cn.get("freshness") or {})
    lines += [
        "## A 股 V2 vs legacy",
        "",
        "V2 rule: oversold_contrarian with real T+1/T+2 exits and Tobit limit-censored volatility as risk unit. For A-shares, fear/high-vol is often the contrarian edge context, so it clips size and enforces pullback-only execution instead of copying the US trend blocker. structural_core/high_mod is baseline only.",
        "",
    ]
    lines += render_metrics_table([cn["metrics"]["v2"], cn["metrics"]["v2_all_oversold_diagnostic"], cn["metrics"]["legacy"]])
    lines += [
        "",
        f"- Current CN candidate date: {cn.get('current_date') or '-'}",
        "- A-share T+1 note: same-day exit is not counted as a valid realized exit; current-day rows can remain pending.",
        "",
    ]
    lines += render_cn_lifecycle_section(cn)
    lines += [
        "## 涨停模型雷达表现",
        "",
    ]
    perf = limit_up.get("performance") or {}
    lines += [
        f"- Performance days: {perf.get('days', 0)}",
        f"- Avg top-decile hit rate: {fmt_pct((perf.get('avg_top_decile_hit_rate') or 0) * 100.0) if perf else '-'}",
        f"- Avg top-decile lift: {fmt_num(perf.get('avg_top_decile_lift'))}",
        f"- Avg failed-board rate: {fmt_pct((perf.get('avg_failed_board_rate') or 0) * 100.0) if perf else '-'}",
        f"- Avg EV after cost / next return proxy: {fmt_pct(perf.get('avg_next_ret_pct'))}",
        "- Rule: daily `limit_up_model` rows are always Limit-Up Radar without 9:25 / 9:35 confirmation.",
        "",
        "## 最近候选表现",
        "",
        "| Market | Symbol | Name | State | Policy | Note |",
        "|---|---|---|---|---|---|",
    ]
    for row in recent_outcomes(us, cn):
        lines.append(
            f"| {row['market'].upper()} | {row.get('symbol')} | {row.get('name') or '-'} | "
            f"{row.get('state')} | {row.get('policy')} | {row.get('reason')} |"
        )
    lines += [
        "",
        "## 当前可执行 / 只观察 / 被拦截",
        "",
        "### US",
        "",
    ]
    lines += render_current_table(us.get("current") or [], "us")
    lines += ["### CN", ""]
    lines += render_current_table(cn.get("current") or [], "cn")
    lines += ["### Limit-Up Radar", ""]
    lines += render_limit_table(limit_up.get("current") or [])
    lines += [
        "## 下一步需要的数据",
        "",
        "- US: keep real options expression history so V2 option-confirmed PnL has more than late-April coverage.",
        "- US: persist `options_chain_quotes` daily so option shadow ledger can move from proxy to true bid/ask leg PnL.",
        "- Portfolio: keep sector/industry tags and price history complete enough for exposure, correlation cluster, and VaR caps.",
        "- CN: add 9:25 auction gain, auction turnover, 9:35 volume ratio, sector co-move count, first touch time, seal strength, and open-board count.",
        "- CN: keep fill_date/exit_date/max_favorable/max_adverse in `strategy_model_dataset`; lifecycle gate now depends on T+1/T+3/T+5 bucket evidence.",
        "- CN: shadow option fields remain risk haircuts only until there is a real listed option/futures expression and executable quote history.",
        "- CN: keep Tobit volatility and market fear/high-vol fields in candidate exports; they drive risk unit and admission.",
    ]
    return "\n".join(lines).rstrip() + "\n"


def render_factorlab_brief(payload: dict[str, Any]) -> str:
    as_of = payload["as_of"]
    us = payload["us"]
    cn = payload["cn"]
    lifecycle = (cn.get("lifecycle") or {}).get("policy") or {}
    direction_lines = render_strategy_direction_table(payload.get("strategy_direction") or [])
    return "\n".join(
        [
            f"# FactorLab Main Strategy Research Brief - {as_of}",
            "",
            "## Research Question",
            "",
            "当前主策略不应固定为 LOW、HIGH/MOD、趋势突破或均值回归。请把主策略当成一个有有效期的策略族选择问题：哪一族在当前市场状态下有正 EV、正 LCB80、可执行约束通过，并且 rolling freshness 没有衰减？",
            "",
            "## Current Evidence Snapshot",
            "",
            f"- US V2 LOW/core/trending LCB80: {fmt_pct(us['metrics']['v2'].get('lcb80_pct'))}; freshness={us.get('freshness', {}).get('v2', {}).get('state')}",
            f"- US V2 stock-only net LCB80: {fmt_pct(us['metrics'].get('v2_stock_only_net', {}).get('lcb80_pct'))}; freshness={us.get('freshness', {}).get('v2_stock_only_net', {}).get('state')}",
            f"- US legacy HIGH/MOD LCB80: {fmt_pct(us['metrics']['legacy'].get('lcb80_pct'))}; freshness={us.get('freshness', {}).get('legacy', {}).get('state')}",
            f"- CN oversold_contrarian LCB80: {fmt_pct(cn['metrics']['v2'].get('lcb80_pct'))}; freshness={cn.get('freshness', {}).get('v2', {}).get('state')}",
            f"- CN structural_core LCB80: {fmt_pct(cn['metrics']['legacy'].get('lcb80_pct'))}; freshness={cn.get('freshness', {}).get('legacy', {}).get('state')}",
            f"- CN lifecycle: best={lifecycle.get('best_bucket') or '-'}, max_hold=T+{lifecycle.get('max_hold_days') or '-'}, rule={lifecycle.get('follow_through_rule') or '-'}",
            "",
            "## Profit Objective",
            "",
            "赚钱目标优先于策略标签：FactorLab 必须按 post-cost、capital-weighted PnL、风险单位收益、最大回撤、换手/滑点和可成交性来选主策略。",
            "",
            "Promotion ladder: paper-only -> manual probe -> small auto size -> normal size；任何一层 rolling LCB80 转负、T+1/期权执行证据不足、或 basket drawdown 触发，都要自动降级。",
            "",
            "A 股和美股分开裁决：美股趋势策略把 noisy/mean-reverting 当阻断；A 股 oversold_contrarian 把恐惧/高波当可能的 edge 来源，只用来压仓位、限制追价和触发 T+1 风险检查。",
            "",
            "US bridge rule: 在期权表达历史不足前，单独跑 stock-only net-after-cost 探针回测；通过也只能给极小仓股票 probe，不能替代 option-confirmed Execution Alpha。",
            "",
            "## Strategy Direction Board",
            "",
            *direction_lines,
            *render_adjustment_rules(),
            "## FactorLab Tasks",
            "",
            "1. 生成候选主策略族：trend_breakout、oversold_contrarian、event_second_day、early_accumulation、shadow_option_edge、legacy_structural_core。",
            "2. 对每族输出 rolling 7/14/30/60D EV、LCB80、样本数、最大回撤、成交率、top1 concentration。",
            "3. 给出 freshness half-life：最近多长窗口仍保持 LCB80>0；若只有长窗口有效，标为 decaying。",
            "4. 给出主策略切换规则：什么时候从趋势切到均值回归，什么时候两者都只观察。",
            "5. 输出 next experiment：需要新增哪些特征或执行数据才能升级为 Execution Alpha。",
            "6. 在组合层报告行业暴露、相关簇、VaR95、单票/篮子 R cap 触发原因。",
            "7. 对 US options shadow ledger 分开评估 leg_quotes 与 proxy_bs 的 post-cost PnL、LCB80 和滑点敏感性；A 股 shadow option 仅作为风险折扣输入。",
            "",
            "## Guardrails",
            "",
            "- 不能因为 HIGH/MOD、CORE、结构核心这些标签本身而升级主策略。",
            "- 没有 T+1/T+2 真实退出的 A股结果不能算胜率。",
            "- 涨停模型在没有 9:25/9:35 数据前只能是 Radar。",
        ]
    ).rstrip() + "\n"


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_summary (
                as_of DATE, market VARCHAR, strategy VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS current_candidates (
                as_of DATE, market VARCHAR, state VARCHAR, symbol VARCHAR, name VARCHAR,
                policy VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS limit_up_radar (
                as_of DATE, symbol VARCHAR, name VARCHAR, p_limit_up DOUBLE,
                p_touch_limit DOUBLE, p_failed_board DOUBLE, ev_after_cost_pct DOUBLE,
                payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_direction (
                as_of DATE, rank INTEGER, market VARCHAR, role VARCHAR,
                strategy_family VARCHAR, tier VARCHAR, max_size VARCHAR,
                post_cost_lcb80_pct DOUBLE, freshness_state VARCHAR,
                freshness_days INTEGER, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_risk_overlay (
                as_of DATE, market VARCHAR, symbol VARCHAR, state VARCHAR,
                strategy_family VARCHAR, sector VARCHAR, base_r DOUBLE,
                final_r DOUBLE, shadow_option_haircut DOUBLE, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS option_shadow_ledger (
                as_of DATE, report_date DATE, evaluation_date DATE, symbol VARCHAR,
                expression VARCHAR, pricing_mode VARCHAR, resolved BOOLEAN,
                underlying_return_pct DOUBLE, return_pct DOUBLE, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cn_lifecycle_research (
                as_of DATE, scope VARCHAR, bucket_type VARCHAR, bucket VARCHAR,
                n INTEGER, avg_pct DOUBLE, lcb80_pct DOUBLE,
                win_rate DOUBLE, avg_mfe_pct DOUBLE, avg_mae_pct DOUBLE,
                avg_hold_days DOUBLE, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS profit_readiness (
                as_of DATE, priority INTEGER, area VARCHAR, state VARCHAR,
                allowed_now VARCHAR, evidence VARCHAR, blocker VARCHAR,
                next_step VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute("DELETE FROM strategy_summary WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM current_candidates WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM limit_up_radar WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM strategy_direction WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM portfolio_risk_overlay WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM option_shadow_ledger WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM cn_lifecycle_research WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM profit_readiness WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        for market in ["us", "cn"]:
            metrics = payload[market]["metrics"]
            for strategy, data in metrics.items():
                con.execute(
                    "INSERT INTO strategy_summary VALUES (CAST(? AS DATE), ?, ?, ?)",
                    [payload["as_of"], market, strategy, json.dumps(data, ensure_ascii=False)],
                )
            for row in payload[market].get("current") or []:
                con.execute(
                    "INSERT INTO current_candidates VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?)",
                    [
                        payload["as_of"],
                        market,
                        row.get("state"),
                        row.get("symbol"),
                        row.get("name") or "",
                        row.get("policy"),
                        json.dumps(row, ensure_ascii=False, default=str),
                    ],
                )
        for row in payload["limit_up"].get("current") or []:
            con.execute(
                "INSERT INTO limit_up_radar VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("symbol"),
                    row.get("name") or "",
                    row.get("p_limit_up"),
                    row.get("p_touch_limit"),
                    row.get("p_failed_board"),
                    row.get("ev_after_cost_pct"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in payload.get("strategy_direction") or []:
            con.execute(
                "INSERT INTO strategy_direction VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("rank"),
                    row.get("market"),
                    row.get("role"),
                    row.get("strategy_family"),
                    row.get("tier"),
                    row.get("max_size"),
                    row.get("post_cost_lcb80_pct"),
                    row.get("freshness_state"),
                    row.get("freshness_days"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("portfolio_risk_overlay") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO portfolio_risk_overlay VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("symbol"),
                    row.get("state"),
                    row.get("strategy_family"),
                    row.get("sector"),
                    row.get("base_r"),
                    row.get("final_r"),
                    row.get("shadow_option_haircut"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("option_shadow_ledger") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO option_shadow_ledger VALUES (CAST(? AS DATE), CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("report_date"),
                    row.get("evaluation_date"),
                    row.get("symbol"),
                    row.get("expression"),
                    row.get("pricing_mode"),
                    bool(row.get("resolved")),
                    row.get("underlying_return_pct"),
                    row.get("return_pct"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        lifecycle = (payload.get("cn") or {}).get("lifecycle") or {}
        for bucket_type, rows in [
            ("v2_hold_bucket", lifecycle.get("by_hold_bucket") or []),
            ("all_hold_bucket", lifecycle.get("all_oversold_by_hold_bucket") or []),
            ("execution_mode", lifecycle.get("by_execution_mode") or []),
            ("fade_bucket", lifecycle.get("by_fade_bucket") or []),
            ("stale_bucket", lifecycle.get("by_stale_bucket") or []),
            ("flow_bucket", lifecycle.get("by_flow_bucket") or []),
        ]:
            for row in rows:
                con.execute(
                    "INSERT INTO cn_lifecycle_research VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        payload["as_of"],
                        lifecycle.get("scope"),
                        bucket_type,
                        row.get("bucket"),
                        row.get("n"),
                        row.get("avg_pct"),
                        row.get("lcb80_pct"),
                        row.get("win_rate"),
                        row.get("avg_mfe_pct"),
                        row.get("avg_mae_pct"),
                        row.get("avg_hold_days"),
                        json.dumps(row, ensure_ascii=False, default=str),
                    ],
                )
        for row in (payload.get("profit_readiness") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO profit_readiness VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("priority"),
                    row.get("area"),
                    row.get("state"),
                    row.get("allowed_now"),
                    row.get("evidence"),
                    row.get("blocker"),
                    row.get("next_step"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        con.execute("CHECKPOINT")
    finally:
        con.close()


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    as_of = parse_date(args.date) if args.date else infer_report_date(args.us_db, args.cn_db)
    start = parse_date(args.start)
    us = summarize_us(args.us_db, start, as_of)
    cn = summarize_cn(args.cn_db, start, as_of)
    limit_up = summarize_limit_up(args.cn_db, start, as_of)
    profit_guardrails = build_profit_guardrails(us, cn, limit_up)
    strategy_direction = build_strategy_direction(us, cn, limit_up, profit_guardrails)
    portfolio_risk_overlay = build_portfolio_risk_overlay(
        us,
        cn,
        limit_up,
        profit_guardrails,
        args.us_db,
        args.cn_db,
        as_of,
    )
    option_shadow_ledger = build_option_shadow_ledger(args.us_db, start, as_of)
    payload = {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "us": us,
        "cn": cn,
        "limit_up": limit_up,
        "profit_guardrails": profit_guardrails,
        "strategy_direction": strategy_direction,
        "portfolio_risk_overlay": portfolio_risk_overlay,
        "option_shadow_ledger": option_shadow_ledger,
    }
    payload["profit_readiness"] = build_profit_readiness(payload)
    return payload


def run(args: argparse.Namespace) -> dict[str, Any]:
    payload = build_payload(args)
    output_dir = args.output_root / payload["as_of"]
    output_dir.mkdir(parents=True, exist_ok=True)
    report_md = render_report(payload)
    (output_dir / "main_strategy_v2_backtest.md").write_text(report_md, encoding="utf-8")
    (output_dir / "main_strategy_v2_backtest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "strategy_direction.md").write_text(render_strategy_direction(payload), encoding="utf-8")
    (output_dir / "strategy_direction.json").write_text(
        json.dumps(payload.get("strategy_direction") or [], ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "portfolio_risk_overlay.md").write_text(render_portfolio_risk_overlay(payload), encoding="utf-8")
    (output_dir / "portfolio_risk_overlay.json").write_text(
        json.dumps(payload.get("portfolio_risk_overlay") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "option_shadow_ledger.md").write_text(render_option_shadow_ledger(payload), encoding="utf-8")
    (output_dir / "option_shadow_ledger.json").write_text(
        json.dumps(payload.get("option_shadow_ledger") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "cn_lifecycle_research.md").write_text(render_cn_lifecycle_research(payload), encoding="utf-8")
    (output_dir / "cn_lifecycle_research.json").write_text(
        json.dumps((payload.get("cn") or {}).get("lifecycle") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "profit_readiness.md").write_text(render_profit_readiness(payload), encoding="utf-8")
    (output_dir / "profit_readiness.json").write_text(
        json.dumps(payload.get("profit_readiness") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    write_duckdb(output_dir / "main_strategy_v2_backtest.duckdb", payload)
    factorlab_dir = STACK_ROOT / "factor-lab" / "reports"
    if factorlab_dir.is_dir():
        factorlab_dir.mkdir(parents=True, exist_ok=True)
        (factorlab_dir / f"main_strategy_v2_research_{payload['as_of']}.md").write_text(
            render_factorlab_brief(payload),
            encoding="utf-8",
        )
    return payload


def main() -> None:
    args = parse_args()
    payload = run(args)
    print(
        "Main Strategy V2 backtest written: "
        f"{args.output_root / payload['as_of'] / 'main_strategy_v2_backtest.md'}"
    )


if __name__ == "__main__":
    main()
