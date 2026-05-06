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
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import cn_observed_lifecycle_prob, cn_opportunity_ranker, us_opportunity_ranker  # noqa: E402

DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_START = "2026-03-01"
LCB80_Z = 1.2816
LCB95_Z = 1.6449
MAIN_STRATEGY_MODE = "opportunity"
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
CN_ALPHA_FACTORY_EXECUTION_SLEEVE = "cn_oversold_ev_positive"
CN_OBSERVED_LIFECYCLE_SLEEVE = cn_observed_lifecycle_prob.OBSERVED_LIFECYCLE_SLEEVE
US_ALPHA_FACTORY_EXECUTION_SLEEVE = "us_v2_stock_probe"
CN_LOG_DENOISE_METRICS = [
    "log_return_20d_pct",
    "denoise_residual_zscore",
    "denoised_log_slope_10d_pct",
    "log_return_vol_norm_20d",
    "fft_signal_to_noise",
    "haar_noise_energy",
]


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
    std_pct: float | None = None
    trade_sharpe: float | None = None
    daily_sharpe: float | None = None

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
            "std_pct": round_or_none(self.std_pct),
            "trade_sharpe": round_or_none(self.trade_sharpe),
            "daily_sharpe": round_or_none(self.daily_sharpe),
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


def sharpe_ratio(values: list[float], *, annualize: bool = False) -> float | None:
    if len(values) < 2:
        return None
    std = statistics.stdev(values)
    if std <= 1e-12:
        return None
    ratio = statistics.fmean(values) / std
    if annualize:
        ratio *= math.sqrt(252.0)
    return ratio


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
    std_pct = None
    if len(returns) == 1:
        lcb80 = avg
        lcb95 = avg
    else:
        std = statistics.stdev(returns)
        std_pct = std
        se = std / math.sqrt(len(returns))
        lcb80 = avg - LCB80_Z * se
        lcb95 = avg - LCB95_Z * se

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    daily_returns: list[float] = []
    for key in sorted(by_date):
        daily_return = statistics.fmean(by_date[key])
        daily_returns.append(daily_return)
        cumulative += daily_return
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
        std_pct=std_pct,
        trade_sharpe=sharpe_ratio(returns),
        daily_sharpe=sharpe_ratio(daily_returns, annualize=True),
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
        ok = metrics["n"] > 0
        if ok:
            passed.append(days)
        window_rows.append({"window_days": days, "passed": ok, **metrics})

    if 14 in passed:
        state = "fresh"
        freshness_days = 14
        rule = "recent opportunity window has live rows; use metrics as sizing context"
    elif 30 in passed:
        state = "usable_but_monitor"
        freshness_days = 30
        rule = "monthly opportunity window has rows; size from current setup quality"
    elif passed:
        state = "decaying_or_slow"
        freshness_days = max(passed)
        rule = "older opportunity window has rows; keep it in the opportunity set"
    else:
        state = "expired_or_unproven"
        freshness_days = None
        rule = "no recent rows in the rolling windows"

    return {
        "label": label,
        "state": state,
        "freshness_days": freshness_days,
        "rule": rule,
        "windows": window_rows,
    }


def is_stable_positive(metrics: StrategyMetrics, *, min_n: int, min_dates: int) -> bool:
    return metrics.n > 0 and metrics.active_dates > 0


def option_expression_pass(row: dict[str, Any] | None) -> tuple[bool, str]:
    if not row:
        return True, "options expression missing; stock/probe opportunity still allowed"
    expression = str(row.get("expression") or "").lower()
    liquidity = str(row.get("liquidity_gate") or "").lower()
    directional = float(row.get("directional_edge") or 0.0)
    vol_edge = float(row.get("vol_edge") or 0.0)
    if liquidity != "pass":
        return True, f"option liquidity {liquidity or 'missing'}; use stock/probe expression"
    if expression == "call_spread" and directional > 0 and vol_edge > 0:
        return True, "call_spread: direction and vol edges pass"
    if expression == "stock_long" and directional > 0:
        return True, "stock_long: direction edge positive, listed options not attractive"
    if expression in {"wait", "blocked", "put_spread"}:
        return True, f"expression {expression} is not a long expression; use stock/probe expression"
    return True, "direction/vol edge weak; use stock/probe expression"


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


def us_alpha_factory_sleeve_id(row: dict[str, Any]) -> str | None:
    if is_us_v2_policy(row) or str(row.get("policy") or "") == "LOW core executable trending":
        return US_ALPHA_FACTORY_EXECUTION_SLEEVE
    return None


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
            "fresh_entry_action": "pullback_retest_opportunity",
            "hold_action": "hold_or_add_tiny_if_retest_confirms",
            "retest_plan": "wait pullback/retest; small probe allowed if price confirms",
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
        alpha_sleeve_id = us_alpha_factory_sleeve_id(row)
        if alpha_sleeve_id:
            state = "Execution Alpha"
            reason = f"Alpha Factory sleeve {alpha_sleeve_id} current member; {opt_reason}"
        elif is_legacy:
            state = "Ranked Watch"
            reason = "legacy HIGH/MOD baseline is rank-only until promoted by Alpha Factory"
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
                "alpha_sleeve_id": alpha_sleeve_id,
                "alpha_factory_role": "execution_sleeve" if alpha_sleeve_id else "rank_only",
                "entry": round_or_none(row.get("entry_price") or row.get("reference_price"), 4),
                "stop": round_or_none(row.get("stop_price"), 4),
                "target": round_or_none(row.get("target_price"), 4),
                "rr_ratio": round_or_none(row.get("rr_ratio"), 4),
                "expected_move_pct": round_or_none(row.get("expected_move_pct"), 4),
                "time_exit": "3 sessions / next catalyst",
                "option_expression": (
                    (opt_row or {}).get("expression")
                    if str((opt_row or {}).get("expression") or "").lower() not in {"blocked", "wait", "put_spread"}
                    else "stock_probe"
                ),
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


def load_cn_log_denoise_features(
    db_path: Path,
    as_of: date,
    symbols: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    requested = {symbol for symbol in (symbols or []) if symbol}
    if not db_path.exists():
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "analytics"):
            rows = []
            latest = None
        else:
            latest_row = con.execute(
                """
                SELECT MAX(as_of)
                FROM analytics
                WHERE module = 'momentum'
                  AND metric IN ({})
                  AND as_of <= CAST(? AS DATE)
                """.format(",".join(["?"] * len(CN_LOG_DENOISE_METRICS))),
                [*CN_LOG_DENOISE_METRICS, as_of.isoformat()],
            ).fetchone()
            latest = latest_row[0] if latest_row else None
            rows = (
                rows_as_dicts(
                    con,
                    """
                    SELECT ts_code, metric, value
                    FROM analytics
                    WHERE module = 'momentum'
                      AND as_of = CAST(? AS DATE)
                      AND metric IN ({})
                    """.format(",".join(["?"] * len(CN_LOG_DENOISE_METRICS))),
                    [as_iso(latest), *CN_LOG_DENOISE_METRICS],
                )
                if latest is not None
                else []
            )
    finally:
        con.close()

    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("ts_code") or "")
        metric = str(row.get("metric") or "")
        if not symbol or not metric:
            continue
        bucket = out.setdefault(symbol, {"feature_as_of": as_iso(latest)})
        bucket[metric] = round_or_none(row.get("value"))

    missing = requested - set(out)
    if requested and missing:
        try:
            from quant_bot.analytics.cn_log_denoise_backtest import compute_log_features, load_prices
        except ImportError:
            return out
        try:
            prices = load_prices(db_path, sorted(missing), as_of - timedelta(days=140), as_of)
        except duckdb.Error:
            return out
        features = compute_log_features(prices)
        if not features.empty:
            features = features.sort_values(["symbol", "feature_date"]).drop_duplicates("symbol", keep="last")
            for record in features.to_dict(orient="records"):
                symbol = str(record.get("symbol") or "")
                if not symbol:
                    continue
                out[symbol] = {
                    "feature_as_of": as_iso(record.get("feature_date")),
                    "feature_source": "price_fallback",
                    **{metric: round_or_none(record.get(metric)) for metric in CN_LOG_DENOISE_METRICS},
                }
    return out


def cn_log_denoise_report_action(
    features: dict[str, Any],
    *,
    state: str,
    gate_summary: str,
) -> str:
    log20 = round_or_none(features.get("log_return_20d_pct"))
    residual_z = round_or_none(features.get("denoise_residual_zscore"))
    fft_snr = round_or_none(features.get("fft_signal_to_noise"))
    haar_noise = round_or_none(features.get("haar_noise_energy"))
    if all(value is None for value in [log20, residual_z, fft_snr, haar_noise]):
        return "log:no coverage"

    action_allowed = state in {"Execution Alpha", "Positive EV Setup"}
    parts: list[str] = []
    if residual_z is not None and residual_z <= -1.5:
        if action_allowed:
            parts.append("action_overlay residual_z<=-1.5")
        else:
            parts.append(f"residual_z<=-1.5 but wait gate ({gate_summary})")
    if log20 is not None and log20 <= -20.0:
        parts.append("setup_overlay log20<=-20")
    if haar_noise is not None and haar_noise <= 0.60:
        parts.append("risk_note low_haar_not_bullish")
    if not parts and fft_snr is not None and fft_snr >= 0.50:
        parts.append("context_only fft_snr>=0.50")
    if not parts:
        parts.append("neutral_log_context")
    return "; ".join(parts)


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


def dedupe_cn_strategy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One report-date/symbol can have multiple strategy-key variants; keep the best EV row."""
    best: dict[tuple[str, str], tuple[float, float, dict[str, Any]]] = {}
    for row in rows:
        report_date = as_iso(row.get("report_date")) or ""
        symbol = str(row.get("symbol") or "").upper()
        if not report_date or not symbol:
            continue
        score = (
            round_or_none(row.get("ev_lcb_80_pct")) if round_or_none(row.get("ev_lcb_80_pct")) is not None else -999.0,
            round_or_none(row.get("ev_pct")) if round_or_none(row.get("ev_pct")) is not None else -999.0,
        )
        key = (report_date, symbol)
        existing = best.get(key)
        if existing is None or score > (existing[0], existing[1]):
            best[key] = (score[0], score[1], row)
    return [row for _, _, row in best.values()]


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
        if row.get("bucket") != "pending"
    ]
    if eligible:
        best = max(eligible, key=lambda row: float(row.get("lcb80_pct") or -999.0))
        max_hold = min(CN_MAX_LIFECYCLE_HOLD_DAYS, max(upper_by_bucket.get(str(row.get("bucket")), 1) for row in eligible))
        state = "opportunity_lifecycle"
    else:
        best = max(hold_buckets, key=lambda row: float(row.get("lcb80_pct") or -999.0), default={})
        max_hold = 1
        state = "opportunity_lifecycle_pending"
    return {
        "state": state,
        "best_bucket": best.get("bucket"),
        "best_bucket_lcb80_pct": best.get("lcb80_pct"),
        "max_hold_days": max_hold,
        "first_review": "T+1 first sellable session; no same-day exit is counted",
        "follow_through_rule": "T+3 no +1R / no volume follow-through -> exit review",
        "time_stop": f"hard review/exit by T+{max_hold}",
        "entry_rule": "oversold_contrarian opportunity buckets stay live; metrics only tune size and urgency",
        "risk_rule": "Tobit risk unit sets handling line; fear/high-vol changes entry style after sleeve membership",
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
    v2_dedup = dedupe_cn_strategy_rows(v2_enriched)
    all_dedup = dedupe_cn_strategy_rows(all_enriched)
    hold_buckets = cn_lifecycle_group(v2_enriched, "hold_bucket")
    hold_buckets_dedup = cn_lifecycle_group(v2_dedup, "hold_bucket")
    return {
        "as_of": as_of.isoformat(),
        "scope": "CN oversold_contrarian lifecycle from strategy_model_dataset; same-day exits are not counted",
        "policy": build_cn_lifecycle_policy(hold_buckets),
        "summary": {
            "v2_ev_positive": cn_lifecycle_summary("V2 EV-positive oversold_contrarian", v2_enriched),
            "v2_ev_positive_dedup": cn_lifecycle_summary("V2 EV-positive oversold_contrarian deduped by date/symbol", v2_dedup),
            "all_oversold_diagnostic": cn_lifecycle_summary("All oversold_contrarian diagnostic", all_enriched),
            "all_oversold_diagnostic_dedup": cn_lifecycle_summary("All oversold_contrarian diagnostic deduped by date/symbol", all_dedup),
        },
        "by_hold_bucket": hold_buckets,
        "by_hold_bucket_dedup": hold_buckets_dedup,
        "all_oversold_by_hold_bucket": cn_lifecycle_group(all_enriched, "hold_bucket"),
        "all_oversold_by_hold_bucket_dedup": cn_lifecycle_group(all_dedup, "hold_bucket"),
        "by_execution_mode": cn_lifecycle_group(v2_enriched, "execution_mode_bucket"),
        "by_execution_mode_dedup": cn_lifecycle_group(v2_dedup, "execution_mode_bucket"),
        "by_fade_bucket": cn_lifecycle_group(v2_enriched, "fade_bucket"),
        "by_stale_bucket": cn_lifecycle_group(v2_enriched, "stale_bucket"),
        "by_flow_bucket": cn_lifecycle_group(v2_enriched, "flow_bucket"),
    }


def cn_lifecycle_time_exit(policy: dict[str, Any]) -> str:
    max_hold = policy.get("max_hold_days") or CN_MAX_LIFECYCLE_HOLD_DAYS
    return f"T+1 review; T+3 no +1R follow-through -> exit; hard max T+{max_hold}"


def cn_alpha_factory_sleeve_id(row: dict[str, Any]) -> str | None:
    family = str(row.get("strategy_family") or row.get("policy") or "")
    action = str(row.get("action_intent") or "")
    if not action and str(row.get("state") or "") == "Execution Alpha":
        action = "TRADE"
    alpha_state = str(row.get("alpha_state") or "")
    lcb80 = round_or_none(row.get("ev_lcb_80_pct") if row.get("ev_lcb_80_pct") is not None else row.get("ev_lcb80_pct"))
    if (
        family == "oversold_contrarian"
        and action == "TRADE"
        and (alpha_state == CN_EXECUTION_ALPHA_STATE or (lcb80 is not None and lcb80 > 0.0))
    ):
        return CN_ALPHA_FACTORY_EXECUTION_SLEEVE
    return None


def cn_lifecycle_action(row: dict[str, Any], state: str, policy: dict[str, Any]) -> str:
    execution_mode = str(cn_feature_value(row, "execution_mode") or "")
    fade = cn_feature_float(row, "fade_risk")
    if state in {"Ranked Watch", "Event Risk Watch", "Falling Knife Watch"}:
        return "rank_only_no_new_trade"
    if state == "Positive EV Setup":
        if execution_mode == "do_not_chase" or (fade is not None and fade >= 0.70):
            return "small_probe_after_pullback"
        return "small_probe_allowed"
    if execution_mode == "do_not_chase" or (fade is not None and fade >= 0.70):
        return "manual_probe_after_pullback"
    return "planned_entry_probe; manage by T+1/T+3/T+max rule"


def cn_current_ev_gate_passes(row: dict[str, Any]) -> bool:
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
    v2_rows = [row for row in v2_all_rows if cn_alpha_factory_sleeve_id(row) == CN_ALPHA_FACTORY_EXECUTION_SLEEVE]
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
    current_symbols = sorted({str(row.get("symbol") or "") for row in current_rows if row.get("symbol")})
    current_log_features = load_cn_log_denoise_features(db_path, current_date or as_of, current_symbols)
    observed_lifecycle = cn_observed_lifecycle_prob.build_probability_payload(
        db_path=db_path,
        start=start,
        as_of=current_date or as_of,
        current_rows=current_rows,
    )
    observed_by_symbol = {
        str(symbol or "").upper(): values
        for symbol, values in (observed_lifecycle.get("by_symbol") or {}).items()
    }
    current: list[dict[str, Any]] = []
    for row in current_rows:
        symbol = str(row.get("symbol") or "").upper()
        family = str(row.get("strategy_family") or "")
        action = str(row.get("action_intent") or "")
        lcb80 = round_or_none(row.get("ev_lcb_80_pct"))
        ev_pct = round_or_none(row.get("ev_pct"))
        features = safe_json_loads(row.get("features_json"))
        market_high_vol = round_or_none(features.get("market_p_high_vol"))
        execution_mode = str(features.get("execution_mode") or "")
        is_v2 = family == "oversold_contrarian" and action == "TRADE"
        is_legacy = family == "structural_core"
        alpha_sleeve_id = cn_alpha_factory_sleeve_id(row)
        if is_v2 and alpha_sleeve_id:
            gate_summary = cn_current_gate_summary(row)
            state = "Execution Alpha"
            reason = f"Alpha Factory sleeve {alpha_sleeve_id} current member; production ranker sets size/tier ({gate_summary})"
        elif family == "oversold_contrarian":
            gate_summary = cn_current_gate_summary(row)
            state = "Ranked Watch"
            reason = f"oversold rank candidate only; not in Alpha Factory execution sleeve ({gate_summary})"
        elif is_legacy:
            state = "Ranked Watch"
            reason = "legacy structural_core is rank-only until promoted by Alpha Factory"
        else:
            continue
        gate_summary = cn_current_gate_summary(row)
        log_features = current_log_features.get(str(row.get("symbol") or ""), {})
        observed_features = observed_by_symbol.get(symbol, {})
        log_overlay = cn_log_denoise_report_action(log_features, state=state, gate_summary=gate_summary)
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
                "action_intent": action,
                "alpha_sleeve_id": alpha_sleeve_id,
                "alpha_factory_role": "execution_sleeve" if alpha_sleeve_id else "rank_only",
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
                "gate_summary": gate_summary,
                "log_feature_as_of": log_features.get("feature_as_of"),
                "log_return_20d_pct": log_features.get("log_return_20d_pct"),
                "denoise_residual_zscore": log_features.get("denoise_residual_zscore"),
                "fft_signal_to_noise": log_features.get("fft_signal_to_noise"),
                "haar_noise_energy": log_features.get("haar_noise_energy"),
                "log_denoise_overlay": log_overlay,
                "p_win_t1": observed_features.get("p_win_t1"),
                "p_hit_1r_t3": observed_features.get("p_hit_1r_t3"),
                "p_stop_t3": observed_features.get("p_stop_t3"),
                "expected_r_t3": observed_features.get("expected_r_t3"),
                "lcb80_r_t3": observed_features.get("lcb80_r_t3"),
                "observed_probability_n": observed_features.get("observed_probability_n"),
                "observed_probability_t3_n": observed_features.get("observed_probability_t3_n"),
                "observed_probability_bucket": observed_features.get("observed_probability_bucket"),
                "observed_probability_source": observed_features.get("observed_probability_source"),
                "observed_lifecycle_tier": observed_features.get("observed_lifecycle_tier"),
                "observed_lifecycle_qualified": observed_features.get("observed_lifecycle_qualified"),
                "observed_lifecycle_sleeve_id": observed_features.get("observed_lifecycle_sleeve_id"),
                "observed_lifecycle_reason": observed_features.get("observed_lifecycle_reason"),
                "suggested_hold_days": observed_features.get("suggested_hold_days"),
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
        "observed_lifecycle_prob": observed_lifecycle,
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
        "| Strategy | n | Active days | Avg | Median | Win | EV LCB80 | Trade Sharpe | Daily Sharpe | Max DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['n']} | {row['active_dates']} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('median_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_pct(row.get('lcb80_pct'))} | {fmt_num(row.get('trade_sharpe'), 2)} | "
            f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_pct(row.get('max_drawdown_pct'))} |"
        )
    return lines


def render_current_table(rows: list[dict[str, Any]], market: str) -> list[str]:
    if not rows:
        return [f"- {market.upper()}: none.", ""]
    if market == "us":
        lines = [
            "| State | Symbol | Rank | Tier | Action | Buy/Review | Stop | Target | Option expression | Trend | Time exit | Why |",
            "|---|---|---:|---|---|---:|---:|---:|---|---|---|---|",
        ]
        for row in rows[:12]:
            lines.append(
                f"| {row['state']} | {row['symbol']} | {row.get('production_rank') or '-'} | "
                f"{row.get('production_tier') or '-'} | {row.get('production_action') or '-'} | "
                f"{fmt_num(row.get('entry'))} | "
                f"{fmt_num(row.get('stop'))} | {fmt_num(row.get('target'))} | "
                f"{row.get('option_expression') or '-'} | {row.get('trend_regime') or '-'} | "
                f"{row.get('time_exit')} | {row.get('reason')} |"
            )
        lines.append("")
        return lines

    lines = [
        "| State | Code | Name | Rank | Tier | Action | ExpR | LCBR | Obs n | Observation entry | Handling line | First target | EV | EV80 | Evidence context | Log overlay | Lifecycle action | Time exit | T+1 risk |",
        "|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|",
    ]
    for row in rows[:16]:
        lines.append(
            f"| {row['state']} | {row['symbol']} | {row.get('name') or '-'} | "
            f"{row.get('production_rank') or '-'} | {row.get('production_tier') or '-'} | "
            f"{row.get('production_action') or '-'} | "
            f"{fmt_num(row.get('expected_r_t3'))} | {fmt_num(row.get('lcb80_r_t3'))} | "
            f"{row.get('observed_probability_n') or '-'} | "
            f"{row.get('observation_entry_zone') or '-'} | {fmt_num(row.get('handling_line'))} | "
            f"{fmt_num(row.get('first_target'))} | {fmt_pct(row.get('ev_pct'))} | "
            f"{fmt_pct(row.get('ev_lcb80_pct'))} | {row.get('gate_summary') or '-'} | "
            f"{row.get('log_denoise_overlay') or '-'} | "
            f"{row.get('lifecycle_action') or '-'} | "
            f"{row.get('time_exit')} | {row.get('t1_risk')} |"
        )
    lines.append("")
    return lines


def render_missed_alpha_radar(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## US Missed Alpha / Winner Hold Radar",
        "",
        "这些是 missed-alpha / winner-hold 机会提示。追高、低 R:R、noisy/mean-reverting 只作为入场方式提示；如果价格给 pullback/retest，可以进入小仓机会复核。",
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
    v2_dedup = summary.get("v2_ev_positive_dedup") or {}
    all_rows = summary.get("all_oversold_diagnostic") or {}
    all_dedup = summary.get("all_oversold_diagnostic_dedup") or {}
    lines = [
        "## A 股生命周期研究 / CN Lifecycle",
        "",
        "A 股主线不是美股式 30D 持有。当前执行池只认 Alpha Factory 已证明的 `cn_oversold_ev_positive`；其他 `oversold_contrarian` 子桶保留为 ranked watch。同一日期同一股票可能有多个 strategy_key 变体，去重口径按最高 EV LCB80 保留一条。",
        "",
        f"- Lifecycle state: `{policy.get('state') or '-'}`",
        f"- Best bucket: `{policy.get('best_bucket') or '-'}`; bucket LCB80 {fmt_pct(policy.get('best_bucket_lcb80_pct'))}",
        f"- Max hold: `T+{policy.get('max_hold_days') or '-'}`",
        f"- V2 EV-positive: n `{v2.get('n', 0)}`, avg {fmt_pct(v2.get('avg_pct'))}, LCB80 {fmt_pct(v2.get('lcb80_pct'))}",
        f"- V2 EV-positive dedup: n `{v2_dedup.get('n', 0)}`, avg {fmt_pct(v2_dedup.get('avg_pct'))}, LCB80 {fmt_pct(v2_dedup.get('lcb80_pct'))}",
        f"- All oversold diagnostic: n `{all_rows.get('n', 0)}`, avg {fmt_pct(all_rows.get('avg_pct'))}, LCB80 {fmt_pct(all_rows.get('lcb80_pct'))}",
        f"- All oversold diagnostic dedup: n `{all_dedup.get('n', 0)}`, avg {fmt_pct(all_dedup.get('avg_pct'))}, LCB80 {fmt_pct(all_dedup.get('lcb80_pct'))}",
        f"- Exit rule: {policy.get('first_review')}; {policy.get('follow_through_rule')}; {policy.get('time_stop')}",
        "- CN hold overlay: execution sleeve names get T+1 review, T+3 runner check, and T+5 max-hold review; non-sleeve rows stay rank-only.",
        "",
    ]
    lines += render_cn_lifecycle_table(lifecycle.get("by_hold_bucket") or [], "EV-positive Hold Buckets")
    lines += render_cn_lifecycle_table(lifecycle.get("by_hold_bucket_dedup") or [], "EV-positive Hold Buckets Deduped By Date/Symbol")
    lines += render_cn_lifecycle_table(lifecycle.get("all_oversold_by_hold_bucket") or [], "All Oversold Diagnostic Hold Buckets")
    lines += render_cn_lifecycle_table(lifecycle.get("all_oversold_by_hold_bucket_dedup") or [], "All Oversold Diagnostic Hold Buckets Deduped By Date/Symbol")
    lines += render_cn_lifecycle_table(lifecycle.get("by_execution_mode") or [], "EV-positive By Execution Mode")
    lines += render_cn_lifecycle_table(lifecycle.get("by_execution_mode_dedup") or [], "EV-positive By Execution Mode Deduped")
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
    return int(metrics.get("n") or 0) > 0 and int(metrics.get("active_dates") or 0) > 0


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
    cn_lifecycle_ok = str(cn_lifecycle_policy.get("state") or "").startswith("opportunity_lifecycle")
    us_metric_ok = metrics_gate_passes(us_v2, min_n=20, min_dates=10, max_drawdown_floor_pct=-8.0)
    us_stock_ok = metrics_gate_passes(us_stock, min_n=20, min_dates=8, max_drawdown_floor_pct=-8.0)
    cn_metric_ok = metrics_gate_passes(cn_v2, min_n=200, min_dates=10, max_drawdown_floor_pct=-8.0)
    cn_current_rows = cn.get("current") or []
    cn_alpha_factory_ea = sum(
        1
        for row in cn_current_rows
        if row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == CN_ALPHA_FACTORY_EXECUTION_SLEEVE
    )
    cn_observed_ea = sum(
        1
        for row in cn_current_rows
        if row.get("state") == "Execution Alpha" and row.get("observed_lifecycle_sleeve_id") == CN_OBSERVED_LIFECYCLE_SLEEVE
    )

    if us_counts.get("Execution Alpha", 0) > 0 and us_metric_ok:
        us_state = "tradeable_small"
        us_size = "0.25R/name; 1R basket cap"
    elif (
        us_stock_ok
        and us_stock_fresh.get("state") in {"fresh", "usable_but_monitor"}
        and (us_counts.get("Execution Alpha", 0) + us_counts.get("Positive EV Setup", 0)) > 0
    ):
        us_state = "conditional_stock_probe"
        us_size = "0.10R/name; 0.50R basket cap; stock-only"
    elif (us_counts.get("Execution Alpha", 0) + us_counts.get("Positive EV Setup", 0)) > 0:
        us_state = "opportunity_probe"
        us_size = "0.05R/name; 0.25R basket cap; stock-only"
    else:
        us_state = "no_current_setup"
        us_size = "0R"

    if cn_alpha_factory_ea > 0 and cn_metric_ok and cn_lifecycle_ok:
        cn_state = "conditional_small"
        cn_size = "0.25R/name; 1R basket cap; planned-entry only"
    elif cn_observed_ea > 0 and cn_lifecycle_ok:
        cn_state = "observed_lifecycle_probe"
        cn_size = "0.05R/name max; 0.40R observed basket cap; planned-entry only"
    elif cn_counts.get("Positive EV Setup", 0) > 0 and cn_lifecycle_ok:
        cn_state = "opportunity_probe"
        cn_size = "0.05R/name; 0.25R basket cap; planned-entry only"
    else:
        cn_state = "no_current_setup"
        cn_size = "0R"

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
            "kill_switch": "Production ranker controls which V2 sleeve names can receive probe size today.",
        },
        {
            "market": "CN",
            "profit_state": cn_state,
            "max_auto_size": cn_size,
            "why": (
                f"V2 LCB80 {fmt_pct(cn_v2.get('lcb80_pct'))}, freshness={cn_fresh.get('state') or '-'}, "
                f"lifecycle={cn_lifecycle_policy.get('best_bucket') or '-'} / T+{cn_lifecycle_policy.get('max_hold_days') or '-'}, "
                f"current Execution Alpha={cn_counts.get('Execution Alpha', 0)}, alpha_factory_EA={cn_alpha_factory_ea}, "
                f"observed_EA={cn_observed_ea}, "
                f"Positive EV Setup={cn_counts.get('Positive EV Setup', 0)}"
            ),
            "kill_switch": f"Only `{CN_ALPHA_FACTORY_EXECUTION_SLEEVE}` or `{CN_OBSERVED_LIFECYCLE_SLEEVE}` plus production probe tier can receive new money.",
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
    cn_ea_count = count_current_states(cn_current).get("Execution Alpha", 0)
    if cn_ea_count <= 0:
        cn_state = "no_current_execution_sleeve"
        cn_next_step = "Fix current-pool generation/probability layer: today's CN rows do not map to `cn_oversold_ev_positive`, so production size is correctly 0R."
    elif cn_manual_r > 0:
        cn_state = "manual_micro_probe_ready"
        cn_next_step = "Do not chase open. If using micro-probe, cap at 0.05R, require planned-entry/pullback fill, and record fill/exit in CN live ledger."
    else:
        cn_state = "probe_ready"
        cn_next_step = "Use planned-entry/pullback for current execution-sleeve names; record fills/exits in CN live ledger."
    option_summary = (option_ledger.get("summary") or {}).get("overall_long") or {}
    limit_perf = limit_up.get("performance") or {}

    rows = [
        {
            "area": "CN main alpha",
            "state": cn_state,
            "allowed_now": cn_guard.get("max_auto_size") or "0R",
            "evidence": (
                f"LCB80 {fmt_pct((cn.get('metrics') or {}).get('v2', {}).get('lcb80_pct'))}; "
                f"lifecycle {((cn.get('lifecycle') or {}).get('policy') or {}).get('best_bucket') or '-'}; "
                f"current EA={cn_ea_count}"
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
                f"current EA={count_current_states(us_current).get('Execution Alpha', 0)}"
            ),
            "blocker": (
                f"My Book open={my_summary.get('open_positions', '-')}, cap="
                f"{(my_book.get('policy') or {}).get('single_name_position_cap', '-')}; "
                f"time_stop={my_summary.get('time_stop_positions', '-')}; "
                f"runners={my_summary.get('runner_positions', '-')}; "
                f"exit_reduce_losers={my_summary.get('exit_or_reduce_loser_positions', '-')}"
            ),
            "next_step": "Only `us_v2_stock_probe` rows can receive probe size; legacy rows stay ranked watch. Winner Hold Overlay still controls existing profitable names.",
            "priority": 2,
        },
        {
            "area": "US options",
            "state": "opportunity_proxy_ready",
            "allowed_now": "manual tiny options/stock proxy only",
            "evidence": f"option-confirmed n={option_summary.get('n', 0)}, LCB80 {fmt_pct(option_summary.get('lcb80_pct'))}",
            "blocker": f"resolved rows={option_ledger.get('resolved_count', 0)}, unresolved rows={option_ledger.get('unresolved_count', 0)}",
            "next_step": "Persist options_chain_quotes and options_alpha expression rows daily, but absence of that ledger no longer blocks stock opportunity probes.",
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
            "today_bias": (
                "US stock probe only; CN has no current execution-sleeve member; options and limit-up stay shadow/radar"
                if cn_ea_count <= 0
                else "CN execution sleeve plus US stock probe; options and limit-up stay shadow/radar"
            ),
        },
        "rows": sorted(rows, key=lambda row: int(row.get("priority") or 99)),
    }


def _current_state_count(rows: list[dict[str, Any]], state: str) -> int:
    return sum(1 for row in rows if row.get("state") == state)


def build_pipeline_requirements_audit(payload: dict[str, Any]) -> dict[str, Any]:
    cn = payload.get("cn") or {}
    us = payload.get("us") or {}
    cn_current = cn.get("current") or []
    us_current = us.get("current") or []
    cn_ranker_rows = (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or []
    us_ranker_rows = (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []
    option_ledger = payload.get("option_shadow_ledger") or {}
    readiness_rows = (payload.get("profit_readiness") or {}).get("rows") or []
    live_row = next((row for row in readiness_rows if row.get("area") == "Live execution ledger"), {})

    cn_ea = _current_state_count(cn_current, "Execution Alpha")
    us_ea = _current_state_count(us_current, "Execution Alpha")
    cn_sleeve_rows = [row for row in cn_ranker_rows if row.get("alpha_sleeve_id") == CN_ALPHA_FACTORY_EXECUTION_SLEEVE]
    cn_observed_rows = [row for row in cn_ranker_rows if row.get("observed_lifecycle_qualified")]
    cn_event_rows = [row for row in cn_ranker_rows if row.get("production_tier") == "event_risk_watch"]
    us_event_rows = [row for row in us_ranker_rows if row.get("production_tier") == "event_risk_watch"]
    cn_probability_keys = {
        "p_win_t1",
        "p_hit_1r_t3",
        "p_stop_t3",
        "expected_r_t3",
        "lcb80_r_t3",
        "observed_probability_bucket",
    }
    has_cn_observed_probability = any(cn_probability_keys & set(row) for row in [*cn_current, *cn_ranker_rows])
    rows = [
        {
            "priority": 1,
            "area": "CN current-pool to sleeve bridge",
            "state": "fail_no_current_execution_sleeve" if cn_ea <= 0 else "pass_current_execution_sleeve",
            "evidence": (
                f"current_total={len(cn_current)}, current_EA={cn_ea}, "
                f"ranker_sleeve_rows={len(cn_sleeve_rows)}, "
                f"observed_qualified={len(cn_observed_rows)}, "
                f"historical_lcb80={fmt_pct((cn.get('metrics') or {}).get('v2', {}).get('lcb80_pct'))}"
            ),
            "requirement": "Current producer must either emit `cn_oversold_ev_positive`, qualify via observed lifecycle probability, or explicitly declare no-trade.",
            "next_change": "Keep daily reconciliation: current candidates vs Alpha Factory sleeve and observed probability sleeve, with top missing reasons.",
        },
        {
            "priority": 2,
            "area": "CN observed probability layer",
            "state": "pass_observed_probability" if has_cn_observed_probability else "fail_missing_observed_probability",
            "evidence": "current/ranker rows do not expose p_win_t1, p_hit_1r_t3, p_stop_t3, expected_r_t3, lcb80_r_t3"
            if not has_cn_observed_probability
            else f"observed probability fields present; qualified={len(cn_observed_rows)}",
            "requirement": "Ranker action must be driven by observed historical analog probabilities, not only strategy labels.",
            "next_change": "Build `cn_observed_lifecycle_prob`: state vector -> nearest historical buckets -> expected R / LCB / hold-days.",
        },
        {
            "priority": 3,
            "area": "CN news/event binding",
            "state": "partial_event_watch_active" if cn_event_rows else "partial_no_event_flags_today",
            "evidence": f"event_risk_watch={len(cn_event_rows)}; top_event={((cn_event_rows[0] if cn_event_rows else {}) or {}).get('symbol') or '-'}",
            "requirement": "Every current symbol needs structured headline risk attached before any production action.",
            "next_change": "Keep `news_enriched` primary; fail visible if a ranked current row has stale/missing news coverage.",
        },
        {
            "priority": 4,
            "area": "US production ranker",
            "state": "pass_stock_probe_ready" if us_ea > 0 else "fail_no_us_execution_rows",
            "evidence": f"current_total={len(us_current)}, current_EA={us_ea}, event_risk_watch={len(us_event_rows)}",
            "requirement": "`rank_score/headline_risk/options_quality/production_action` must exist for US current rows.",
            "next_change": "Keep legacy HIGH/MOD rank-only unless Alpha Factory promotes it; improve missing R:R/options quality coverage.",
        },
        {
            "priority": 5,
            "area": "Options execution ledger",
            "state": "fail_no_resolved_option_pnl" if int(option_ledger.get("resolved_count") or 0) <= 0 else "pass_option_pnl_present",
            "evidence": f"resolved={option_ledger.get('resolved_count', 0)}, unresolved={option_ledger.get('unresolved_count', 0)}",
            "requirement": "Option recommendations need bid/ask leg PnL before options become production alpha.",
            "next_change": "Persist options_chain_quotes and option leg selections daily; keep stock probe separate until then.",
        },
        {
            "priority": 6,
            "area": "Live execution ledger",
            "state": "fail_missing_cn_live_fills" if "missing" in str(live_row.get("blocker") or "").lower() else "partial_live_ledger",
            "evidence": live_row.get("blocker") or "-",
            "requirement": "Every production action must be reconciled to fill/slippage/exit, especially CN T+1.",
            "next_change": "Set QUANT_CN_ACTIVITY_CSV and store realized fill/exit into the daily review DB.",
        },
    ]
    failing = [row for row in rows if str(row.get("state") or "").startswith("fail")]
    return {
        "as_of": payload["as_of"],
        "summary": {
            "fail_count": len(failing),
            "top_blocker": (failing[0] if failing else rows[0]).get("area"),
            "production_bias": "US only today; CN is research/ranked-watch until current-pool bridge and observed probability layer are fixed"
            if cn_ea <= 0
            else "CN and US both have execution rows; size still controlled by ranker and live ledger",
        },
        "rows": rows,
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


def render_pipeline_requirements_audit(payload: dict[str, Any]) -> str:
    audit = payload.get("pipeline_requirements_audit") or {}
    summary = audit.get("summary") or {}
    lines = [
        f"# Pipeline Requirements Audit - {payload['as_of']}",
        "",
        "This is the production-contract audit for the current pipeline. A fail here means the report may rank names, but should not pretend the row is executable.",
        "",
        f"- Fail count: `{summary.get('fail_count', 0)}`",
        f"- Top blocker: {summary.get('top_blocker') or '-'}",
        f"- Production bias: {summary.get('production_bias') or '-'}",
        "",
        "| Priority | Area | State | Evidence | Requirement | Next change |",
        "|---:|---|---|---|---|---|",
    ]
    for row in audit.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('evidence')} | {row.get('requirement')} | {row.get('next_change')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_pipeline_requirements_audit_section(payload: dict[str, Any]) -> list[str]:
    audit = payload.get("pipeline_requirements_audit") or {}
    summary = audit.get("summary") or {}
    lines = [
        "## 管线需求审计 / Pipeline Requirements Audit",
        "",
        "这里专门回答“这套管线有没有实际用”：fail 表示可以观察/排序，但不能把它写成可执行 alpha。",
        "",
        f"- Fail count: `{summary.get('fail_count', 0)}`",
        f"- Top blocker: {summary.get('top_blocker') or '-'}",
        f"- Production bias: {summary.get('production_bias') or '-'}",
        "",
        "| Priority | Area | State | Evidence | Next change |",
        "|---:|---|---|---|---|",
    ]
    for row in audit.get("rows") or []:
        lines.append(
            f"| {row.get('priority')} | {row.get('area')} | {row.get('state')} | "
            f"{row.get('evidence')} | {row.get('next_change')} |"
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
            "tier": cn_guard.get("profit_state") or "opportunity_probe",
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
            "tier": us_guard.get("profit_state") or "opportunity_probe",
            "max_size": us_guard.get("max_auto_size") or "0.05R/name; stock-only",
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
            "role": "options_proxy",
            "tier": "manual_proxy",
            "max_size": "manual tiny only",
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
            "reason": "option ledger is useful but no longer blocks stock opportunity probes",
            "kill_switch": "Use tiny/manual options only until real option fills exist.",
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
            "role": "rank_only_baseline",
            "tier": "ranked_watch",
            "max_size": "0R until Alpha Factory promotes sleeve",
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
            "reason": "baseline retained for comparison only; not a production entry sleeve",
            "kill_switch": "No new money entry from legacy CN baseline without Alpha Factory promotion.",
        },
        {
            "market": "US",
            "strategy_family": "legacy_high_mod_core",
            "direction": "legacy HIGH/MOD core baseline",
            "role": "rank_only_baseline",
            "tier": "ranked_watch",
            "max_size": "0R until Alpha Factory promotes sleeve",
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
            "reason": "legacy retained for comparison only; not a production entry sleeve",
            "kill_switch": "No new money entry from legacy US baseline without Alpha Factory promotion.",
        },
    ]
    role_order = {
        "primary": 0,
        "secondary_probe": 1,
        "options_proxy": 2,
        "radar": 3,
        "rank_only_baseline": 4,
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
            f"legacy={row.get('current_blocked', 0)}"
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
        f"- Mode: `{MAIN_STRATEGY_MODE}`. Execution rows must come from Alpha Factory-proven sleeves plus the production ranker tier.",
        "- CN broad oversold stays ranked watch unless it is in `cn_oversold_ev_positive`.",
        "- US stock-only V2 can probe through `us_v2_stock_probe`; legacy HIGH/MOD is ranked watch only.",
        "- US options can be tracked/proxied manually at tiny size; missing option ledger no longer blocks stock opportunities.",
        "- Limit-up remains radar by data availability, but strong names stay on the opportunity board.",
        "- Legacy families are comparison baselines, not fresh-entry production sleeves.",
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
    cn_allows_auto = cn_profit_state in {"conditional_small", "opportunity_probe"}
    rows: list[dict[str, Any]] = []

    for row in cn.get("current") or []:
        if row.get("state") not in {"Execution Alpha", "Positive EV Setup"}:
            continue
        production_tier = str(row.get("production_tier") or "")
        if production_tier == "observed_lifecycle_probe":
            base_r = 0.05
        elif production_tier == "observed_lifecycle_secondary":
            base_r = 0.03
        elif production_tier == "observed_lifecycle_micro_probe":
            base_r = 0.02
        else:
            base_r = 0.25 if row.get("state") == "Execution Alpha" else 0.10
        risk_reasons: list[str] = []
        if production_tier.startswith("observed_lifecycle"):
            risk_reasons.append("observed_lifecycle_tiny_size")
        if not cn_allows_auto:
            base_r = min(base_r, 0.05)
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
                "production_tier": row.get("production_tier"),
                "execution_source": row.get("execution_source"),
                "expected_r_t3": row.get("expected_r_t3"),
                "lcb80_r_t3": row.get("lcb80_r_t3"),
                "observed_probability_n": row.get("observed_probability_n"),
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
    us_allows_probe = (
        us_profit_state.startswith("conditional")
        or us_profit_state.startswith("tradeable")
        or us_profit_state == "opportunity_probe"
    )
    for row in us.get("current") or []:
        if row.get("state") not in {"Execution Alpha", "Positive EV Setup"}:
            continue
        base_r = 0.25 if row.get("state") == "Execution Alpha" else 0.10
        risk_reasons = []
        if not us_allows_probe:
            base_r = 0.05
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
            haircut = 1.0
            row["risk_reasons"].append("cn_shadow_option_warning")
        elif (entry_quality is not None and entry_quality < 0.55) or (stale is not None and stale >= 0.44) or (
            downside is not None and downside >= 0.70
        ):
            haircut = 1.0
            row["risk_reasons"].append("cn_shadow_option_half_warning")
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
                for row in group:
                    row["risk_reasons"].append(f"sector_cap_warning_{sector}")

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
            for row in group:
                row["risk_reasons"].append(f"corr_cluster_cap_warning_{cluster_id}")

    gross_r = sum(float(row["final_r"]) for row in rows)
    if gross_r > PORTFOLIO_TOTAL_R_CAP and gross_r > 0:
        for row in rows:
            row["risk_reasons"].append("total_portfolio_cap_warning")

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
        for row in rows:
            row["risk_reasons"].append("var95_cap_warning")

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
        "这里不是重新选股，也不是硬拦截器；它把当前机会映射成小仓 probe，并给出单票 R、行业暴露、相关簇、组合 VaR95、A 股 shadow option 风险提示。",
        "",
        f"- Current opportunity candidates: {summary.get('candidate_count', 0)}",
        f"- Final gross R: {fmt_num(summary.get('gross_r_after_caps'), 4)}",
        f"- VaR95 R proxy: {fmt_num(summary.get('var95_r_proxy'), 4)}",
        f"- Warning references only: total {fmt_num(summary.get('total_cap_r'), 2)}R, sector {fmt_num(summary.get('sector_cap_r'), 2)}R, correlation cluster {fmt_num(summary.get('corr_cluster_cap_r'), 2)}R",
        "",
    ]
    rows = overlay.get("rows") or []
    if not rows:
        lines += ["- No current candidates found for opportunity sizing.", ""]
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
        "美股期权表达不再只停留在口头建议：已有真实 `options_chain_quotes` 时按 bid/ask legs 记账；历史没有 leg quote 时用 `options_analysis` 的 IV、期限和 spread 做 Black-Scholes proxy。Ledger 现在是诊断和复盘工具，不再阻断股票机会。",
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
        f"- Mode: `{MAIN_STRATEGY_MODE}`. Only Alpha Factory sleeve `{CN_ALPHA_FACTORY_EXECUTION_SLEEVE}` can produce execution rows.",
        "- Broad oversold rows stay in ranked watch; fear/high-vol and no-chase tune entry style only after sleeve membership exists.",
        "- Weak/non-sleeve rows cannot bypass the production ranker into new money entries.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def render_us_opportunity_ranker_section(payload: dict[str, Any]) -> list[str]:
    ranker = payload.get("us_opportunity_ranker") or {}
    rows = ranker.get("top_rows") or []
    if not rows:
        return ["## 美股生产排序 / US Production Ranker", "", "- No US production ranker rows.", ""]
    lines = [
        "## 美股生产排序 / US Production Ranker",
        "",
        "`us_v2_stock_probe` 是当前唯一可执行 Alpha Factory sleeve；legacy report bucket 只做 ranked watch。排序输出 `rank_score/headline_risk/flow_options_quality/production_action`，不再靠旧 HIGH/MOD 桶直接给动作。",
        "",
        "| Rank | Symbol | Sleeve | Tier | Action | Score | Headline | Options/Flow | R:R | Trend |",
        "|---:|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in rows[:12]:
        headline = round_or_none(row.get("headline_risk"))
        lines.append(
            f"| {row.get('rank')} | {row.get('symbol')} | {row.get('alpha_sleeve_id') or 'rank_only'} | "
            f"{row.get('production_tier')} | {row.get('production_action')} | "
            f"{fmt_num(row.get('rank_score'))} | "
            f"{fmt_num(None if headline is None else headline * 100.0, 0)} | "
            f"{fmt_num(row.get('flow_options_quality'), 0)} | "
            f"{fmt_num(row.get('rr_ratio'))} | {row.get('trend_regime') or '-'} |"
        )
    event_rows = [row for row in ranker.get("all_rows") or [] if row.get("production_tier") == "event_risk_watch"]
    if event_rows:
        lines += ["", "Event/news 0R watch:"]
        for row in event_rows[:6]:
            lines.append(f"- {row.get('symbol')}: {row.get('latest_headline') or row.get('headline_flags') or 'headline risk'}")
    lines.append("")
    return lines


def render_cn_opportunity_ranker_section(payload: dict[str, Any]) -> list[str]:
    ranker = payload.get("cn_opportunity_ranker") or {}
    rows = ranker.get("top_rows") or []
    if not rows:
        return ["## A 股生产排序 / CN Production Ranker", "", "- No CN production ranker rows.", ""]
    lines = [
        "## A 股生产排序 / CN Production Ranker",
        "",
        "`cn_oversold_ev_positive` 是 Alpha Factory 已证明 sleeve；`cn_observed_lifecycle_prob` 是当前新增的历史相似观测概率 sleeve。新闻事件风险、falling-knife、资金流、执行质量共同决定排序和 0R/可 probe 动作。",
        "",
        "| Rank | Symbol | Name | Source | Tier | Action | Score | ExpR | LCBR | Obs n | Headline | Knife | Flow | Entry |",
        "|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:12]:
        headline = round_or_none(row.get("headline_risk"))
        lines.append(
            f"| {row.get('rank')} | {row.get('symbol')} | {row.get('name') or '-'} | "
            f"{row.get('alpha_sleeve_id') or row.get('observed_lifecycle_sleeve_id') or 'rank_only'} | "
            f"{row.get('production_tier')} | {row.get('production_action')} | "
            f"{fmt_num(row.get('rank_score'))} | "
            f"{fmt_num(row.get('expected_r_t3'))} | "
            f"{fmt_num(row.get('lcb80_r_t3'))} | "
            f"{row.get('observed_probability_n') or '-'} | "
            f"{fmt_num(None if headline is None else headline * 100.0, 0)} | "
            f"{fmt_num(row.get('falling_knife_score'), 0)} | "
            f"{fmt_num(row.get('flow_information_score'))} | "
            f"{row.get('observation_entry_zone') or '-'} |"
        )
    event_rows = [
        row
        for row in ranker.get("all_rows") or []
        if str(row.get("production_tier") or "") == "event_risk_watch"
    ]
    if event_rows:
        lines += ["", "Event-risk demotions:", ""]
        for row in event_rows[:8]:
            lines.append(
                f"- {row.get('symbol')} {row.get('name') or ''}: {row.get('latest_headline') or '-'}"
            )
    lines.append("")
    return lines


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
        f"当前为 `{MAIN_STRATEGY_MODE}` 模式。净 EV、LCB80、回撤、新鲜期、样本覆盖和执行数据只影响仓位/优先级，不再把 A 股或美股机会硬拦成 0R。",
        "",
    ]
    lines += render_profit_guardrails(payload.get("profit_guardrails") or [])
    lines += render_profit_readiness_section(payload)
    lines += render_pipeline_requirements_audit_section(payload)
    lines += [
        "## 策略方向裁决 / Strategy Direction",
        "",
        "这不是永久固化的配置，而是每天滚动重排的机会快照：哪个策略族有当前 setup、该给多大 probe、哪些风险只作为提示。",
        "",
    ]
    lines += render_strategy_direction_table(payload.get("strategy_direction") or [])
    lines += render_adjustment_rules()
    lines += render_portfolio_risk_overlay_section(payload)
    lines += render_option_shadow_ledger_section(payload)
    lines += [
        "## 美股 V2 vs legacy",
        "",
        "V2 rule: LOW confidence + core + executable_now + trending regime. `us_v2_stock_probe` is the executable sleeve; options expression chooses implementation quality, while HIGH/MOD core remains ranked watch until promoted.",
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
        "- HIGH/MOD: legacy baseline is ranked watch only until Alpha Factory promotes a sleeve.",
        "",
    ]
    lines += render_us_opportunity_ranker_section(payload)
    lines += render_missed_alpha_radar(us.get("missed_alpha_radar") or [])
    lines += [
        "## 策略新鲜期 / Freshness",
        "",
        "主策略不是永久身份。这里用滚动 7/14/30/45/60 日窗口重新计算 EV/LCB 作为机会新鲜度提示；不再作为硬拦截。",
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
    lines += render_cn_opportunity_ranker_section(payload)
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
            f"当前主策略不应固定为 LOW、HIGH/MOD、趋势突破或均值回归。现在按 `{MAIN_STRATEGY_MODE}` 模式处理：当前 setup 优先进入机会池，EV/LCB/freshness 只决定排序和仓位提示。",
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
            "赚钱目标优先于策略标签：FactorLab 必须把 post-cost、capital-weighted PnL、风险单位收益、最大回撤、换手/滑点和可成交性作为机会排序特征，而不是硬门槛。",
            "",
            "Promotion ladder: watch -> manual probe -> small opportunity size -> normal size；rolling LCB80、T+1/期权执行证据、basket drawdown 只改变尺寸和优先级。",
            "",
            "A 股和美股分开裁决：美股 noisy/mean-reverting、A 股恐惧/高波都作为入场方式和仓位提示，不再作为阻断器。",
            "",
            "US bridge rule: 期权表达历史不足时，stock-only net-after-cost 仍然可以给股票 probe；期权 ledger 用来复盘和决定是否扩大。",
            "",
            "## Strategy Direction Board",
            "",
            *direction_lines,
            *render_adjustment_rules(),
            "## FactorLab Tasks",
            "",
            "1. 生成候选主策略族：trend_breakout、oversold_contrarian、event_second_day、early_accumulation、shadow_option_edge、legacy_structural_core。",
            "2. 对每族输出 rolling 7/14/30/60D EV、LCB80、样本数、最大回撤、成交率、top1 concentration。",
            "3. 给出 freshness half-life：最近多长窗口还有 setup；LCB 只作为强弱读数。",
            "4. 给出主策略切换规则：什么时候从趋势切到均值回归，什么时候只降尺寸。",
            "5. 输出 next experiment：需要新增哪些特征或执行数据才能扩大机会尺寸。",
            "6. 在组合层报告行业暴露、相关簇、VaR95、单票/篮子 R warning 原因。",
            "7. 对 US options shadow ledger 分开评估 leg_quotes 与 proxy_bs 的 post-cost PnL、LCB80 和滑点敏感性；A 股 shadow option 仅作为风险折扣输入。",
            "",
            "## Guardrails",
            "",
            "- 不能因为 HIGH/MOD、CORE、结构核心这些标签本身而给正常仓位；但小机会 probe 可以保留。",
            "- 没有 T+1/T+2 真实退出的 A股结果不能算胜率。",
            "- 涨停模型在没有 9:25/9:35 数据前只能是 Radar。",
        ]
    ).rstrip() + "\n"


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS cn_opportunity_ranker")
        con.execute("DROP TABLE IF EXISTS us_opportunity_ranker")
        con.execute("DROP TABLE IF EXISTS pipeline_requirements_audit")
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
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_requirements_audit (
                as_of DATE, priority INTEGER, area VARCHAR, state VARCHAR,
                evidence VARCHAR, requirement VARCHAR, next_change VARCHAR,
                payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cn_opportunity_ranker (
                as_of DATE, rank INTEGER, symbol VARCHAR, name VARCHAR,
                industry VARCHAR, rank_score DOUBLE, alpha_sleeve_id VARCHAR,
                observed_lifecycle_sleeve_id VARCHAR, execution_source VARCHAR,
                alpha_factory_role VARCHAR, production_tier VARCHAR,
                production_action VARCHAR, expected_r_t3 DOUBLE,
                lcb80_r_t3 DOUBLE, p_win_t1 DOUBLE, p_hit_1r_t3 DOUBLE,
                p_stop_t3 DOUBLE, observed_probability_n INTEGER,
                headline_risk DOUBLE,
                falling_knife_score DOUBLE, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS us_opportunity_ranker (
                as_of DATE, rank INTEGER, symbol VARCHAR, rank_score DOUBLE,
                alpha_sleeve_id VARCHAR, alpha_factory_role VARCHAR,
                production_tier VARCHAR, production_action VARCHAR,
                headline_risk DOUBLE, options_quality DOUBLE,
                flow_options_quality DOUBLE, rr_ratio DOUBLE, payload_json VARCHAR
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
        con.execute("DELETE FROM pipeline_requirements_audit WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM cn_opportunity_ranker WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM us_opportunity_ranker WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
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
            ("v2_hold_bucket_dedup", lifecycle.get("by_hold_bucket_dedup") or []),
            ("all_hold_bucket", lifecycle.get("all_oversold_by_hold_bucket") or []),
            ("all_hold_bucket_dedup", lifecycle.get("all_oversold_by_hold_bucket_dedup") or []),
            ("execution_mode", lifecycle.get("by_execution_mode") or []),
            ("execution_mode_dedup", lifecycle.get("by_execution_mode_dedup") or []),
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
        for row in (payload.get("pipeline_requirements_audit") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO pipeline_requirements_audit VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("priority"),
                    row.get("area"),
                    row.get("state"),
                    row.get("evidence"),
                    row.get("requirement"),
                    row.get("next_change"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or []:
            con.execute(
                "INSERT INTO cn_opportunity_ranker VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("rank"),
                    row.get("symbol"),
                    row.get("name") or "",
                    row.get("industry") or "",
                    row.get("rank_score"),
                    row.get("alpha_sleeve_id"),
                    row.get("observed_lifecycle_sleeve_id"),
                    row.get("execution_source"),
                    row.get("alpha_factory_role"),
                    row.get("production_tier"),
                    row.get("production_action"),
                    row.get("expected_r_t3"),
                    row.get("lcb80_r_t3"),
                    row.get("p_win_t1"),
                    row.get("p_hit_1r_t3"),
                    row.get("p_stop_t3"),
                    row.get("observed_probability_n"),
                    row.get("headline_risk"),
                    row.get("falling_knife_score"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []:
            con.execute(
                "INSERT INTO us_opportunity_ranker VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("rank"),
                    row.get("symbol"),
                    row.get("rank_score"),
                    row.get("alpha_sleeve_id"),
                    row.get("alpha_factory_role"),
                    row.get("production_tier"),
                    row.get("production_action"),
                    row.get("headline_risk"),
                    row.get("options_quality"),
                    row.get("flow_options_quality"),
                    row.get("rr_ratio"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        con.execute("CHECKPOINT")
    finally:
        con.close()


def apply_us_ranker_to_current(us: dict[str, Any], ranker: dict[str, Any]) -> None:
    by_symbol = {
        str(row.get("symbol") or "").upper(): row
        for row in ranker.get("all_rows") or []
        if row.get("symbol")
    }
    for row in us.get("current") or []:
        symbol = str(row.get("symbol") or "").upper()
        ranked = by_symbol.get(symbol)
        if not ranked:
            continue
        row["production_rank"] = ranked.get("rank")
        row["production_rank_score"] = ranked.get("rank_score")
        row["production_tier"] = ranked.get("production_tier")
        row["production_action"] = ranked.get("production_action")
        row["headline_risk"] = ranked.get("headline_risk")
        row["options_quality"] = ranked.get("options_quality")
        row["flow_options_quality"] = ranked.get("flow_options_quality")
        row["latest_headline"] = ranked.get("latest_headline")
        row["alpha_sleeve_id"] = ranked.get("alpha_sleeve_id")
        row["alpha_factory_role"] = ranked.get("alpha_factory_role")
        row["execution_source"] = ranked.get("execution_source")
        row["observed_lifecycle_sleeve_id"] = ranked.get("observed_lifecycle_sleeve_id")
        row["observed_lifecycle_qualified"] = ranked.get("observed_lifecycle_qualified")
        row["observed_lifecycle_tier"] = ranked.get("observed_lifecycle_tier")
        row["observed_lifecycle_reason"] = ranked.get("observed_lifecycle_reason")
        row["expected_r_t3"] = ranked.get("expected_r_t3")
        row["lcb80_r_t3"] = ranked.get("lcb80_r_t3")
        row["p_win_t1"] = ranked.get("p_win_t1")
        row["p_hit_1r_t3"] = ranked.get("p_hit_1r_t3")
        row["p_stop_t3"] = ranked.get("p_stop_t3")
        row["observed_probability_n"] = ranked.get("observed_probability_n")
        row["suggested_hold_days"] = ranked.get("suggested_hold_days")
        tier = str(ranked.get("production_tier") or "")
        if tier == "event_risk_watch":
            row["state"] = "Event Risk Watch"
            row["execution_mode"] = "negative_headline_no_probe"
            row["reason"] = f"production ranker demoted to 0R: {ranked.get('latest_headline') or 'event/news risk'}"
        elif ranked.get("alpha_sleeve_id") != US_ALPHA_FACTORY_EXECUTION_SLEEVE:
            row["state"] = "Ranked Watch"
            row["execution_mode"] = "rank_only_no_new_trade"
            row["reason"] = "production ranker kept at 0R: not an Alpha Factory execution sleeve member"
        elif tier in {"top_probe", "secondary_probe"}:
            row["state"] = "Execution Alpha"
            row["reason"] = (
                f"Alpha Factory sleeve {US_ALPHA_FACTORY_EXECUTION_SLEEVE}; "
                f"production tier={tier}, action={ranked.get('production_action')}"
            )
        elif tier == "active_watch":
            row["state"] = "Ranked Watch"
            row["execution_mode"] = ranked.get("production_action") or "prepare_order_but_wait_for_price"
            row["reason"] = "V2 sleeve member, but production rank is watch-only today"


def apply_cn_ranker_to_current(cn: dict[str, Any], ranker: dict[str, Any]) -> None:
    by_symbol = {
        str(row.get("symbol") or "").upper(): row
        for row in ranker.get("all_rows") or []
        if row.get("symbol")
    }
    for row in cn.get("current") or []:
        symbol = str(row.get("symbol") or "").upper()
        ranked = by_symbol.get(symbol)
        if not ranked:
            continue
        row["production_rank"] = ranked.get("rank")
        row["production_rank_score"] = ranked.get("rank_score")
        row["production_tier"] = ranked.get("production_tier")
        row["production_action"] = ranked.get("production_action")
        row["headline_risk"] = ranked.get("headline_risk")
        row["falling_knife_score"] = ranked.get("falling_knife_score")
        row["latest_headline"] = ranked.get("latest_headline")
        row["alpha_sleeve_id"] = ranked.get("alpha_sleeve_id")
        row["alpha_factory_role"] = ranked.get("alpha_factory_role")
        tier = str(ranked.get("production_tier") or "")
        if tier == "event_risk_watch":
            row["state"] = "Event Risk Watch"
            row["execution_mode"] = "negative_headline_no_probe"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = f"production ranker demoted to 0R: {ranked.get('latest_headline') or 'negative headline risk'}"
        elif tier == "falling_knife_watch":
            row["state"] = "Falling Knife Watch"
            row["execution_mode"] = "wait_for_flow_reversal"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = "production ranker demoted to 0R: falling-knife risk without enough confirmation"
        elif tier in {"observed_lifecycle_probe", "observed_lifecycle_secondary", "observed_lifecycle_micro_probe"}:
            row["state"] = "Execution Alpha"
            row["execution_mode"] = ranked.get("production_action") or "planned_entry_observed_probe"
            row["lifecycle_action"] = ranked.get("production_action") or row.get("lifecycle_action")
            row["reason"] = (
                f"Observed lifecycle probability sleeve {CN_OBSERVED_LIFECYCLE_SLEEVE}; "
                f"ExpR={fmt_num(ranked.get('expected_r_t3'))}, LCBR={fmt_num(ranked.get('lcb80_r_t3'))}, "
                f"n={ranked.get('observed_probability_n')}"
            )
        elif ranked.get("alpha_sleeve_id") != CN_ALPHA_FACTORY_EXECUTION_SLEEVE:
            row["state"] = "Ranked Watch"
            row["execution_mode"] = "rank_only_no_new_trade"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = "production ranker kept at 0R: no Alpha Factory sleeve and no qualified observed lifecycle probability"
        elif tier in {"top_probe", "secondary_probe"}:
            row["state"] = "Execution Alpha"
            row["lifecycle_action"] = ranked.get("production_action") or row.get("lifecycle_action")
            row["reason"] = (
                f"Alpha Factory sleeve {CN_ALPHA_FACTORY_EXECUTION_SLEEVE}; "
                f"production tier={tier}, action={ranked.get('production_action')}"
            )
        elif tier in {"active_watch", "bench_ranked"}:
            row["state"] = "Ranked Watch"
            row["execution_mode"] = ranked.get("production_action") or "watch_for_rotation"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = f"Alpha Factory sleeve member, but rank tier {tier} is watch-only today"


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    as_of = parse_date(args.date) if args.date else infer_report_date(args.us_db, args.cn_db)
    start = parse_date(args.start)
    us = summarize_us(args.us_db, start, as_of)
    cn = summarize_cn(args.cn_db, start, as_of)
    limit_up = summarize_limit_up(args.cn_db, start, as_of)
    us_ranker = us_opportunity_ranker.build_ranker_payload(
        as_of=as_of,
        candidates=us.get("current") or [],
        candidate_status="from_main_strategy_v2_current",
        us_db=args.us_db,
        source_report="main_strategy_v2_payload",
        top=30,
    )
    apply_us_ranker_to_current(us, us_ranker)
    cn_ranker = cn_opportunity_ranker.build_ranker_payload(
        as_of=as_of,
        candidates=cn.get("current") or [],
        candidate_status="from_main_strategy_v2_current",
        cn_db=args.cn_db,
        source_report="main_strategy_v2_payload",
        top=30,
    )
    apply_cn_ranker_to_current(cn, cn_ranker)
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
        "us_opportunity_ranker": us_ranker,
        "cn_opportunity_ranker": cn_ranker,
        "profit_guardrails": profit_guardrails,
        "strategy_direction": strategy_direction,
        "portfolio_risk_overlay": portfolio_risk_overlay,
        "option_shadow_ledger": option_shadow_ledger,
    }
    payload["profit_readiness"] = build_profit_readiness(payload)
    payload["pipeline_requirements_audit"] = build_pipeline_requirements_audit(payload)
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
    (output_dir / "pipeline_requirements_audit.md").write_text(
        render_pipeline_requirements_audit(payload),
        encoding="utf-8",
    )
    (output_dir / "pipeline_requirements_audit.json").write_text(
        json.dumps(payload.get("pipeline_requirements_audit") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "us_opportunity_ranker.md").write_text(
        us_opportunity_ranker.render_markdown(payload.get("us_opportunity_ranker") or {}),
        encoding="utf-8",
    )
    (output_dir / "us_opportunity_ranker.json").write_text(
        json.dumps(payload.get("us_opportunity_ranker") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    us_opportunity_ranker.write_duckdb(
        output_dir / "us_opportunity_ranker.duckdb",
        (payload.get("us_opportunity_ranker") or {}).get("all_rows") or [],
        parse_date(payload["as_of"]),
    )
    (output_dir / "cn_opportunity_ranker.md").write_text(
        cn_opportunity_ranker.render_markdown(payload.get("cn_opportunity_ranker") or {}),
        encoding="utf-8",
    )
    (output_dir / "cn_opportunity_ranker.json").write_text(
        json.dumps(payload.get("cn_opportunity_ranker") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    cn_opportunity_ranker.write_duckdb(
        output_dir / "cn_opportunity_ranker.duckdb",
        (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or [],
        parse_date(payload["as_of"]),
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
