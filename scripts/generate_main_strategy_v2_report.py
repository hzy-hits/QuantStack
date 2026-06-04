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
import csv
import json
import math
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import yaml


STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = STACK_ROOT / "scripts"
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from lib import hedge as hedge_lib  # noqa: E402
from lib.convexity import assert_no_anticonvex, classify_convexity  # noqa: E402
from score_risk_regime_engine import classify_regime as classify_risk_regime  # noqa: E402

_CONVEXITY_SHORT = {"convex": "凸", "linear": "线性", "anti_convex": "反凸", "none": "-"}

# Auto-maintained virtual holdings ledger. The operator does not feed
# positions in — we simulate "what was held going into today" from
# yesterday's actionable list (i.e. what the report told them to do).
# Used to render a held→target delta in the Actionable table's Action
# column so PRESS day (R=0.35x) shows "trim from 0.125R to 0.044R" and
# not just the new target size.
VIRTUAL_HOLDINGS_PATH = STACK_ROOT / "ai_infra" / "data" / "virtual_holdings.json"


def _load_virtual_holdings() -> dict[str, dict[str, float]]:
    """{"US": {sym: held_r}, "CN": {sym: held_r}}. Empty when file absent."""
    if not VIRTUAL_HOLDINGS_PATH.exists():
        return {"US": {}, "CN": {}}
    try:
        data = json.loads(VIRTUAL_HOLDINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"US": {}, "CN": {}}
    out: dict[str, dict[str, float]] = {"US": {}, "CN": {}}
    for key in ("us", "cn"):
        section = data.get(key) or {}
        if not isinstance(section, dict):
            continue
        for sym, val in section.items():
            try:
                out[key.upper()][str(sym).upper().strip()] = float(val)
            except (TypeError, ValueError):
                continue
    return out


def _persist_virtual_holdings(
    actionable: list[dict[str, Any]],
    current_book: dict[str, dict[str, float]],
    as_of: str | None,
) -> None:
    """Write today's actionable sizes back to the ledger.

    Today's targets become tomorrow's "held". Symbols not in today's
    actionable are KEPT at their previous held size (orphaned positions
    persist until manually cleared in the JSON). The ledger's `as_of`
    only moves forward — running the report for a historical date is a
    no-op so old runs cannot stomp live state.
    """
    if not as_of:
        return
    try:
        prior_as_of = (
            json.loads(VIRTUAL_HOLDINGS_PATH.read_text(encoding="utf-8")).get("as_of")
            if VIRTUAL_HOLDINGS_PATH.exists() else None
        )
    except (OSError, json.JSONDecodeError):
        prior_as_of = None
    if prior_as_of and prior_as_of > as_of:
        return  # historical re-run; don't stomp newer state
    book = {"US": dict(current_book.get("US") or {}),
            "CN": dict(current_book.get("CN") or {})}
    for row in actionable:
        market = str(row.get("market") or "").upper()
        if market not in ("US", "CN"):
            continue
        sym = _symbol_key(row.get("symbol"))
        if not sym:
            continue
        try:
            size = float(row.get("size_r") or 0.0)
        except (TypeError, ValueError):
            continue
        book[market][sym] = size
    payload = {"as_of": as_of, "us": book["US"], "cn": book["CN"]}
    VIRTUAL_HOLDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    VIRTUAL_HOLDINGS_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


# position_delta_text moved to lib/fmt.py (Phase B.19)
from lib.fmt import position_delta_text  # noqa: E402


# _connect_ro extracted to scripts/lib/db_helpers.py (Phase A.0)
from lib.db_helpers import _connect_ro  # noqa: E402
from quant_bot.analytics import cn_observed_lifecycle_prob, cn_opportunity_ranker, us_opportunity_ranker  # noqa: E402
from sleeves.cn_tape_leadership import (  # noqa: E402
    CN_TAPE_SLEEVE_ID,
    query_cn_sector_narrative_screen,
    query_cn_tape_current_candidates,
    query_cn_tape_leadership_returns,
)
from sleeves.promotions import (  # noqa: E402
    BOOTSTRAP_PROMOTED_SLEEVES,
    UnpromotedSleeveError,
    assert_sleeve_promoted,
    is_sleeve_promoted,
    latest_alpha_factory_db,
    load_promoted_sleeves,
    with_trend_mainline_overrides,
)
from sleeves.us_theme_cluster import (  # noqa: E402
    US_THEME_SLEEVE_ID,
    query_us_theme_cluster_returns,
    query_us_theme_current_candidates,
)

DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_START = "2026-03-01"
LCB80_Z = 1.2816
LCB95_Z = 1.6449
MAIN_STRATEGY_MODE = "opportunity"
US_STOCK_ROUNDTRIP_COST_PCT = 0.15
PORTFOLIO_TOTAL_R_CAP = 1.50
PORTFOLIO_VAR95_R_CAP = 1.00
CN_BASKET_R_CAP = 1.20
US_BASKET_R_CAP = 0.50
US_SINGLE_NAME_R_CAP = 0.125
SECTOR_R_CAP = 0.50
# CN daily report surfaces only the top-N reranked A-share names (operator
# directive 2026-05-18 — keep the A-share daily decision tight).
CN_DAILY_TOP_N = 5
CORR_CLUSTER_R_CAP = 0.75
CORR_CLUSTER_THRESHOLD = 0.65
CN_BETA_HEDGE_RATIO = hedge_lib.CN_BETA_HEDGE_RATIO
US_BETA_HEDGE_RATIO = hedge_lib.US_BETA_HEDGE_RATIO
CN_MARKET_BETA_FLOOR = hedge_lib.CN_MARKET_BETA_FLOOR
US_MARKET_BETA_FLOOR = hedge_lib.US_MARKET_BETA_FLOOR
CN_HEDGE_BENCHMARKS = hedge_lib.CN_HEDGE_BENCHMARKS
US_HEDGE_BENCHMARKS = hedge_lib.US_HEDGE_BENCHMARKS
OPTION_CONTRACT_MULTIPLIER = 100.0
OPTION_COMMISSION_PER_LEG = 0.65
CN_MAX_LIFECYCLE_HOLD_DAYS = 5
US_DEFAULT_TIME_EXIT = "next session review; no mechanical 3D-5D hold"
CN_DEFAULT_TIME_EXIT = "T+1/T+3 review; T+5 only if horizon edge remains positive"
CN_LIFECYCLE_BUCKET_ORDER = ["T+1", "T+2", "T+3", "T+4-T+5", "T+6-T+10", ">T+10", "pending"]
CN_EXECUTION_ALPHA_STATE = "positive_ev_setup"
CN_ALPHA_FACTORY_EXECUTION_SLEEVE = "cn_oversold_ev_positive"
CN_AI_INFRA_PRODUCTION_SLEEVE = "ai_infra_production_core"
CN_ALPHA_FACTORY_EXECUTION_SLEEVES = {
    CN_ALPHA_FACTORY_EXECUTION_SLEEVE,
    CN_TAPE_SLEEVE_ID,
    CN_AI_INFRA_PRODUCTION_SLEEVE,
}
CN_OBSERVED_LIFECYCLE_SLEEVE = cn_observed_lifecycle_prob.OBSERVED_LIFECYCLE_SLEEVE
US_ALPHA_FACTORY_EXECUTION_SLEEVE = "us_v2_stock_probe"
US_ALPHA_FACTORY_EXECUTION_SLEEVES = {US_ALPHA_FACTORY_EXECUTION_SLEEVE, US_THEME_SLEEVE_ID}
DEFAULT_US_THEME_SEED_MAP = STACK_ROOT / "data" / "us_theme_seed_map.yaml"
DEFAULT_AI_LAB_QUALITY_SEED = STACK_ROOT / "data" / "ai_lab_quality_seed.yaml"
DEFAULT_AI_LAB_PUBLICATIONS = STACK_ROOT / "data" / "ai_lab_publications.csv"
DEFAULT_AI_SUPPLY_CHAIN_RELATIONSHIPS = STACK_ROOT / "data" / "ai_supply_chain_relationships.yaml"
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
    parser.add_argument("--promotion-db", type=Path, default=None, help="Optional alpha_factory_backtest.duckdb containing promoted_sleeves.")
    parser.add_argument(
        "--ai-infra-mode",
        choices=["off", "enforce", "expand", "enforce_expand"],
        default=None,
        help="Override AI-infra universe filtering/expansion for the rankers.",
    )
    return parser.parse_args()


# parse_date moved to lib.fmt (Phase A.1, imported below)


def load_promotion_contract(promotion_db: Path | None, as_of: date) -> dict[str, Any]:
    contract_path = promotion_db or latest_alpha_factory_db(STACK_ROOT, as_of.isoformat())
    rows = load_promoted_sleeves(contract_path)
    source = str(contract_path) if contract_path else ""
    if not rows:
        rows = list(BOOTSTRAP_PROMOTED_SLEEVES)
        source = "bootstrap_existing_production_contract"
    rows = with_trend_mainline_overrides(rows)
    return {
        "source": source,
        "rows": rows,
        "promoted": sorted(f"{market}:{sleeve}" for market, sleeve in is_promoted_pairs(rows)),
    }


def is_promoted_pairs(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for row in rows:
        if str(row.get("status") or "").lower() == "promoted":
            out.add((str(row.get("market") or "").lower(), str(row.get("sleeve_id") or "")))
    return out


# Format helpers extracted to scripts/lib/fmt.py (Phase A.1)
from lib.fmt import (  # noqa: E402
    parse_date, as_iso, round_or_none, fmt_pct, fmt_num, fmt_bool, safe_json_loads,
)


# placeholders extracted to scripts/lib/db_helpers.py (Phase A.0)
from lib.db_helpers import placeholders  # noqa: E402


def nested_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


# table_exists, rows_as_dicts extracted to scripts/lib/db_helpers.py (Phase A.0)
from lib.db_helpers import table_exists, rows_as_dicts  # noqa: E402


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


STRATEGY_BACKTEST_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "strategy_backtest"


def load_strategy_alpha_bulletin(as_of: date | str) -> dict[str, Any]:
    as_of_s = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    path = STRATEGY_BACKTEST_ROOT / as_of_s / "alpha_bulletin.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def build_us_market_data_status(db_path: Path, as_of: date) -> dict[str, Any]:
    status: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "prices_daily_latest_date": None,
        "options_analysis_latest_as_of": None,
        "options_chain_latest_as_of": None,
        "options_sentiment_latest_as_of": None,
        "market_quotes_latest_as_of": None,
        "market_quotes_latest_session": None,
        "market_quotes_latest_quote_time": None,
        "stock_data_current": None,
        "state": "unknown",
    }
    if not db_path.exists():
        status.update({"stock_data_current": False, "state": "missing_us_db"})
        return status
    con = _connect_ro(db_path)
    try:
        if not table_exists(con, "prices_daily"):
            status.update({"stock_data_current": False, "state": "missing_prices_daily"})
            return status
        row = con.execute("SELECT MAX(date) FROM prices_daily WHERE close IS NOT NULL").fetchone()
        latest = parse_date(as_iso(row[0]) or "") if row and row[0] is not None else None
        if table_exists(con, "options_analysis"):
            row = con.execute(
                "SELECT MAX(as_of) FROM options_analysis WHERE as_of <= CAST(? AS DATE)",
                [as_of.isoformat()],
            ).fetchone()
            value = parse_date(as_iso(row[0]) or "") if row and row[0] is not None else None
            status["options_analysis_latest_as_of"] = value.isoformat() if value else None
        if table_exists(con, "options_chain_quotes"):
            row = con.execute(
                "SELECT MAX(as_of) FROM options_chain_quotes WHERE as_of <= CAST(? AS DATE)",
                [as_of.isoformat()],
            ).fetchone()
            value = parse_date(as_iso(row[0]) or "") if row and row[0] is not None else None
            status["options_chain_latest_as_of"] = value.isoformat() if value else None
        if table_exists(con, "options_sentiment"):
            row = con.execute(
                "SELECT MAX(as_of) FROM options_sentiment WHERE as_of <= CAST(? AS DATE)",
                [as_of.isoformat()],
            ).fetchone()
            value = parse_date(as_iso(row[0]) or "") if row and row[0] is not None else None
            status["options_sentiment_latest_as_of"] = value.isoformat() if value else None
        if table_exists(con, "market_quotes"):
            row = con.execute(
                """
                SELECT as_of, session, quote_time
                FROM market_quotes
                WHERE as_of <= CAST(? AS DATE)
                ORDER BY as_of DESC, quote_time DESC
                LIMIT 1
                """,
                [as_of.isoformat()],
            ).fetchone()
            if row:
                quote_as_of = parse_date(as_iso(row[0]) or "") if row[0] is not None else None
                status["market_quotes_latest_as_of"] = quote_as_of.isoformat() if quote_as_of else None
                status["market_quotes_latest_session"] = row[1]
                status["market_quotes_latest_quote_time"] = as_iso(row[2])
    finally:
        con.close()
    status["prices_daily_latest_date"] = latest.isoformat() if latest else None
    status["is_previous_session"] = bool(latest and latest < as_of)
    status["effective_us_market_date"] = latest.isoformat() if latest else None
    # 5-day grace window: covers weekends + US market holidays + the CN-time
    # window where today's session hasn't opened yet (e.g. 8am 中国时间
    # looking at US market that closed yesterday EDT). Anything older than
    # 5 calendar days is genuinely stale and gets the original gate behavior.
    from datetime import timedelta as _td
    grace_days = 5
    status["stock_data_current"] = bool(latest and latest >= (as_of - _td(days=grace_days)))
    if latest is None:
        status["state"] = "no_stock_prices"
    elif latest < as_of - _td(days=grace_days):
        status["state"] = "stale_stock_prices_or_market_holiday"
    elif latest < as_of:
        status["state"] = "previous_session"
    else:
        status["state"] = "current"
    return status


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
        return True, "options expression missing; stock trade opportunity still allowed"
    expression = str(row.get("expression") or "").lower()
    liquidity = str(row.get("liquidity_gate") or "").lower()
    directional = float(row.get("directional_edge") or 0.0)
    vol_edge = float(row.get("vol_edge") or 0.0)
    if liquidity != "pass":
        return True, f"option liquidity {liquidity or 'missing'}; use stock expression"
    if expression == "call_spread" and directional > 0 and vol_edge > 0:
        return True, "call_spread: direction and vol edges pass"
    if expression == "stock_long" and directional > 0:
        return True, "stock_long: direction edge positive, listed options not attractive"
    if expression in {"wait", "blocked", "put_spread"}:
        return True, f"expression {expression} is not a long expression; use stock expression"
    return True, "direction/vol edge weak; use stock expression"


def option_expression_is_long_proxy(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    expression = str(row.get("expression") or "").lower()
    directional = float(row.get("directional_edge") or 0.0)
    vol_edge = float(row.get("vol_edge") or 0.0)
    if expression == "stock_long":
        return directional > 0
    if expression == "call_spread":
        return directional > 0 and vol_edge > 0
    return False


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
    con = _connect_ro(db_path)
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
    con = _connect_ro(db_path)
    try:
        if not table_exists(con, "report_decisions"):
            return [], None, "missing report_decisions"
        latest_row = con.execute(
            """
            SELECT MAX(report_date)
            FROM report_decisions
            WHERE report_date <= CAST(? AS DATE)
              AND symbol NOT LIKE '%=%'
            """,
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
    con = _connect_ro(db_path)
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
    con = _connect_ro(db_path)
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


def _earnings_focus_us(as_of: date, report_date: str | None, actual_eps: Any) -> str:
    if not report_date:
        return "关注"
    try:
        event_date = parse_date(report_date[:10])
    except ValueError:
        return "关注"
    delta = (event_date - as_of).days
    if delta == -1 and actual_eps is not None:
        return "昨夜/昨日已披露"
    if delta == 0 and actual_eps is not None:
        return "今日已披露"
    if delta == 0:
        return "今日待披露"
    if 0 < delta <= 7:
        return "未来7日"
    if -1 <= delta < 0:
        return "近期已披露"
    return "关注"


def build_us_earnings_calendar(
    db_path: Path,
    as_of: date,
    *,
    focus_symbols: Iterable[str] | None = None,
    window_days: int = 7,
    limit: int = 40,
) -> dict[str, Any]:
    """Read the US earnings calendar for the final daily report."""
    if not db_path.exists():
        return {"status": "missing_db", "rows": []}
    con = _connect_ro(db_path)
    try:
        if not table_exists(con, "earnings_calendar"):
            return {"status": "missing_table", "rows": []}
        has_symbols = table_exists(con, "us_symbols")
        has_profile = table_exists(con, "company_profile")
        name_select = "'' AS name"
        joins = ""
        params: list[Any] = []
        if has_profile:
            params.append(as_of.isoformat())
            joins += """
            LEFT JOIN (
                SELECT symbol, company_name
                FROM (
                    SELECT symbol, company_name,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of DESC) AS rn
                    FROM company_profile
                    WHERE as_of <= CAST(? AS DATE)
                )
                WHERE rn = 1
            ) cp ON cp.symbol = e.symbol
            """
        if has_symbols:
            joins += "LEFT JOIN us_symbols us ON us.symbol = e.symbol\n"
        if has_profile and has_symbols:
            name_select = "COALESCE(NULLIF(cp.company_name, ''), NULLIF(us.name, ''), '') AS name"
        elif has_profile:
            name_select = "COALESCE(NULLIF(cp.company_name, ''), '') AS name"
        elif has_symbols:
            name_select = "COALESCE(NULLIF(us.name, ''), '') AS name"

        focus = sorted({str(symbol).upper() for symbol in (focus_symbols or []) if str(symbol).strip()})
        symbol_filter = ""
        if focus:
            symbol_filter = f"AND e.symbol IN ({placeholders(focus)})"

        params.extend([as_of.isoformat(), as_of.isoformat(), int(window_days), *focus, int(limit)])
        rows = rows_as_dicts(
            con,
            f"""
            SELECT e.symbol, {name_select}, CAST(e.report_date AS VARCHAR) AS report_date,
                   COALESCE(e.fiscal_period, '') AS fiscal_period,
                   e.estimate_eps, e.actual_eps, e.surprise_pct
            FROM earnings_calendar e
            {joins}
            WHERE e.report_date >= CAST(? AS DATE) - INTERVAL 1 DAY
              AND e.report_date <= CAST(? AS DATE) + (? * INTERVAL 1 DAY)
              {symbol_filter}
            ORDER BY e.report_date,
                     CASE WHEN e.actual_eps IS NULL THEN 1 ELSE 0 END,
                     e.symbol
            LIMIT ?
            """,
            params,
        )
        for row in rows:
            row["market"] = "US"
            row["focus"] = _earnings_focus_us(as_of, as_iso(row.get("report_date")), row.get("actual_eps"))
            row["display_name"] = str(row.get("name") or "")
            row["estimate_eps"] = round_or_none(row.get("estimate_eps"), 4)
            row["actual_eps"] = round_or_none(row.get("actual_eps"), 4)
            row["surprise_pct"] = round_or_none(row.get("surprise_pct"), 2)
        return {
            "status": "ok",
            "scope": "focused_report_symbols" if focus else "all_symbols",
            "focus_symbol_count": len(focus),
            "window": f"{(as_of - timedelta(days=1)).isoformat()} to {(as_of + timedelta(days=window_days)).isoformat()}",
            "rows": rows,
        }
    except Exception as exc:
        return {"status": f"error: {exc}", "rows": []}
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
            "hold_action": "hold_or_add_only_if_retest_confirms",
            "retest_plan": "wait pullback/retest; stock trade allowed only if price confirms",
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


def demote_unpromoted_current_rows(market: str, rows: list[dict[str, Any]], promoted_rows: list[dict[str, Any]]) -> None:
    for row in rows:
        sleeve_id = str(row.get("alpha_sleeve_id") or "")
        if not sleeve_id:
            continue
        if is_sleeve_promoted(market=market, sleeve_id=sleeve_id, promoted_rows=promoted_rows):
            continue
        row["blocked_alpha_sleeve_id"] = sleeve_id
        row["alpha_sleeve_id"] = None
        row["alpha_factory_role"] = "rank_only"
        row["state"] = "Ranked Watch"
        row["execution_mode"] = "rank_only_no_new_trade"
        row["reason"] = f"promotion contract rejected {market}:{sleeve_id}; 0R until Alpha Factory writes promoted_sleeves"


def current_row_priority(row: dict[str, Any]) -> tuple[int, float]:
    sleeve_id = str(row.get("alpha_sleeve_id") or "")
    if sleeve_id in {CN_TAPE_SLEEVE_ID, US_THEME_SLEEVE_ID}:
        return (4, round_or_none(row.get("tape_score") or row.get("theme_score")) or 0.0)
    if str(row.get("state") or "") == "Execution Alpha" and sleeve_id:
        return (3, round_or_none(row.get("ev_lcb80_pct") or row.get("rr_ratio")) or 0.0)
    if row.get("observed_lifecycle_qualified"):
        return (2, round_or_none(row.get("lcb80_r_t3")) or 0.0)
    return (1, round_or_none(row.get("rank_score")) or 0.0)


def merge_current_rows_by_symbol(rows: list[dict[str, Any]], additions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one current production row per symbol, preferring trend-mainline sleeves."""
    merged = list(rows)
    index: dict[str, int] = {}
    for idx, row in enumerate(merged):
        symbol = str(row.get("symbol") or "").upper()
        if symbol and symbol not in index:
            index[symbol] = idx
    for row in additions:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        if symbol not in index:
            index[symbol] = len(merged)
            merged.append(row)
            continue
        old_idx = index[symbol]
        if current_row_priority(row) >= current_row_priority(merged[old_idx]):
            prior = merged[old_idx]
            row = dict(row)
            row["secondary_context"] = {
                "policy": prior.get("policy"),
                "alpha_sleeve_id": prior.get("alpha_sleeve_id"),
                "state": prior.get("state"),
                "reason": prior.get("reason"),
            }
            merged[old_idx] = row
    return merged


def summarize_us(
    db_path: Path,
    start: date,
    as_of: date,
    promoted_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows, status = load_us_rows(db_path, start, as_of)
    options = load_us_options(db_path, as_of)
    options_history = load_us_options_range(db_path, start, as_of)
    v2_rows = [row for row in rows if is_us_v2_policy(row)]
    v2_option_rows = [
        row
        for row in v2_rows
        if option_expression_is_long_proxy(
            options_history.get((as_iso(row.get("report_date")) or "", str(row.get("symbol") or "").upper()))
        )
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
                    else "stock_only"
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

    current = merge_current_rows_by_symbol([], current)
    current = merge_current_rows_by_symbol(
        current,
        query_us_theme_current_candidates(
            db_path,
            as_of,
            DEFAULT_US_THEME_SEED_MAP,
        ),
    )
    demote_unpromoted_current_rows("us", current, promoted_rows or [])
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
    con = _connect_ro(db_path)
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
    con = _connect_ro(db_path)
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


def build_cn_earnings_calendar(
    db_path: Path,
    as_of: date,
    *,
    focus_symbols: Iterable[str] | None = None,
    window_days: int = 7,
    limit: int = 40,
) -> dict[str, Any]:
    """Read the A-share disclosure calendar for the final daily report."""
    if not db_path.exists():
        return {"status": "missing_db", "rows": []}
    con = _connect_ro(db_path)
    try:
        if not table_exists(con, "disclosure_date"):
            return {"status": "missing_table", "rows": []}
        stock_join = "LEFT JOIN stock_basic sb ON sb.ts_code = d.ts_code" if table_exists(con, "stock_basic") else ""
        stock_name = "COALESCE(sb.name, '')" if stock_join else "''"
        focus = sorted({str(symbol).upper() for symbol in (focus_symbols or []) if str(symbol).strip()})
        symbol_filter = f"AND d.ts_code IN ({placeholders(focus)})" if focus else ""
        params: list[Any] = [
            as_of.isoformat(),
            as_of.isoformat(),
            as_of.isoformat(),
            as_of.isoformat(),
            as_of.isoformat(),
            int(window_days),
            as_of.isoformat(),
            as_of.isoformat(),
            as_of.isoformat(),
            as_of.isoformat(),
            as_of.isoformat(),
            int(window_days),
            as_of.isoformat(),
            *focus,
            int(limit),
        ]
        rows = rows_as_dicts(
            con,
            f"""
            SELECT
                CASE
                  WHEN TRY_CAST(d.actual_date AS DATE) = CAST(? AS DATE) THEN '今日已披露'
                  WHEN TRY_CAST(d.actual_date AS DATE) = CAST(? AS DATE) - INTERVAL 1 DAY THEN '昨夜/昨日已披露'
                  WHEN TRY_CAST(d.actual_date AS DATE) IS NULL
                       AND TRY_CAST(d.pre_date AS DATE) = CAST(? AS DATE) THEN '今日预约'
                  WHEN TRY_CAST(d.actual_date AS DATE) IS NULL
                       AND TRY_CAST(d.pre_date AS DATE) > CAST(? AS DATE)
                       AND TRY_CAST(d.pre_date AS DATE) <= CAST(? AS DATE) + (? * INTERVAL 1 DAY) THEN '未来7日'
                  WHEN TRY_CAST(d.actual_date AS DATE) IS NULL
                       AND TRY_CAST(d.pre_date AS DATE) < CAST(? AS DATE)
                       AND TRY_CAST(d.pre_date AS DATE) >= CAST(? AS DATE) - INTERVAL 14 DAY THEN '延期/待披露'
                  ELSE '关注'
                END AS focus,
                d.ts_code AS symbol,
                {stock_name} AS name,
                CAST(d.end_date AS VARCHAR) AS fiscal_period,
                COALESCE(d.pre_date, '-') AS pre_date,
                COALESCE(d.actual_date, '-') AS actual_date,
                COALESCE(d.modify_date, '-') AS modify_date
            FROM disclosure_date d
            {stock_join}
            WHERE d.end_date = (SELECT MAX(end_date) FROM disclosure_date)
              AND (
                TRY_CAST(d.actual_date AS DATE) IN (CAST(? AS DATE), CAST(? AS DATE) - INTERVAL 1 DAY)
                OR (
                    TRY_CAST(d.actual_date AS DATE) IS NULL
                    AND TRY_CAST(d.pre_date AS DATE) <= CAST(? AS DATE) + (? * INTERVAL 1 DAY)
                    AND TRY_CAST(d.pre_date AS DATE) >= CAST(? AS DATE) - INTERVAL 14 DAY
                )
              )
              {symbol_filter}
            ORDER BY
              CASE focus
                WHEN '今日已披露' THEN 0
                WHEN '昨夜/昨日已披露' THEN 1
                WHEN '今日预约' THEN 2
                WHEN '延期/待披露' THEN 3
                WHEN '未来7日' THEN 4
                ELSE 9
              END,
              COALESCE(TRY_CAST(d.actual_date AS DATE), TRY_CAST(d.pre_date AS DATE), DATE '9999-12-31'),
              d.ts_code
            LIMIT ?
            """,
            params,
        )
        for row in rows:
            row["market"] = "CN"
            row["name_zh"] = str(row.get("name") or "")
            row["display_name"] = row["name_zh"]
        return {
            "status": "ok",
            "scope": "focused_report_symbols" if focus else "all_symbols",
            "focus_symbol_count": len(focus),
            "window": f"{(as_of - timedelta(days=1)).isoformat()} to {(as_of + timedelta(days=window_days)).isoformat()}",
            "rows": rows,
        }
    except Exception as exc:
        return {"status": f"error: {exc}", "rows": []}
    finally:
        con.close()


SOURCE_REVIEW_QUEUE_PATH = STACK_ROOT / "ai_infra" / "reports" / "source_verification_queue_v1.csv"


def _safe_relative_path(path: Path) -> str:
    if path.is_absolute():
        try:
            return str(path.relative_to(STACK_ROOT))
        except ValueError:
            return str(path)
    return str(path)

# priority_tier values seen in source_verification_queue_v1.csv, ordered most → least urgent.
_SOURCE_REVIEW_TIER_ORDER = {
    "P0_first_batch": 0,
    "P0": 0,
    "P1": 1,
    "P2": 2,
    "P3": 3,
}


def _compute_ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out: list[float] = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def _ema_tape_metrics(closes_with_dates: list[tuple[date, float]]) -> dict[str, Any] | None:
    """Return EMA21/50 / cross / slope / distance metrics for a price series."""
    if len(closes_with_dates) < 55:
        return None
    closes = [c[1] for c in closes_with_dates]
    ema21 = _compute_ema(closes, 21)
    ema50 = _compute_ema(closes, 50)
    last_close = closes[-1]
    last_ema21 = ema21[-1]
    last_ema50 = ema50[-1]

    # Cross state
    if abs(last_ema21 - last_ema50) / last_ema50 < 0.005:
        cross_state = "tangled"
    elif last_ema21 > last_ema50:
        cross_state = "bull"
    else:
        cross_state = "bear"

    # Recent cross (within last 5 sessions)
    recent_cross: str | None = None
    for i in range(max(1, len(ema21) - 5), len(ema21)):
        prev_diff = ema21[i - 1] - ema50[i - 1]
        curr_diff = ema21[i] - ema50[i]
        if prev_diff < 0 < curr_diff:
            recent_cross = "bull_cross"
            break
        if prev_diff > 0 > curr_diff:
            recent_cross = "bear_cross"
            break

    # 5d slope on EMA21 expressed as percent
    slope_5d_pct: float | None = None
    if len(ema21) >= 6 and ema21[-6] > 0:
        slope_5d_pct = (last_ema21 - ema21[-6]) / ema21[-6] * 100.0

    return {
        "as_of": closes_with_dates[-1][0].isoformat(),
        "close": round(last_close, 4),
        "ema21": round(last_ema21, 4),
        "ema50": round(last_ema50, 4),
        "cross_state": cross_state,
        "recent_cross": recent_cross,
        "slope_21d_5d_pct": round(slope_5d_pct, 3) if slope_5d_pct is not None else None,
        "dist_close_ema21_pct": round((last_close / last_ema21 - 1.0) * 100.0, 3) if last_ema21 else None,
        "dist_close_ema50_pct": round((last_close / last_ema50 - 1.0) * 100.0, 3) if last_ema50 else None,
    }


def _ema_summary_label(metrics: dict[str, Any] | None) -> str:
    if metrics is None:
        return "no_data"
    parts: list[str] = [metrics.get("cross_state") or "?"]
    if metrics.get("recent_cross"):
        parts.append(metrics["recent_cross"])
    slope = metrics.get("slope_21d_5d_pct")
    if slope is not None:
        if slope > 0.5:
            parts.append("rising")
        elif slope < -0.5:
            parts.append("falling")
        else:
            parts.append("flat")
    dist = metrics.get("dist_close_ema21_pct")
    if dist is not None:
        parts.append(f"px {dist:+.1f}% vs EMA21")
    return "; ".join(parts)


def build_ema_tape_overlay(
    us_db: Path,
    cn_db: Path,
    symbols: Iterable[str],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    """Compute EMA21/50 tape metrics for a set of AI-universe symbols.

    The methodology allows K-line / EMA signals as tape/crowding/risk
    context only — never as basic-fundamental evidence. The overlay routes
    symbols to the right DuckDB by suffix: `*.SH`/`*.SZ` → CN db, everything
    else → US db (which also holds yfinance-sourced satellite ADRs/indices).
    """
    out: dict[str, dict[str, Any]] = {}
    us_symbols: list[str] = []
    cn_symbols: list[str] = []
    for symbol in symbols:
        if not symbol:
            continue
        text = str(symbol).upper().strip()
        if text.endswith((".SH", ".SZ")):
            cn_symbols.append(text)
        else:
            us_symbols.append(text)

    if us_symbols:
        series = _load_benchmark_closes(us_db, "us", us_symbols, as_of, lookback_days=160)
        for symbol in us_symbols:
            metrics = _ema_tape_metrics(series.get(symbol) or [])
            out[symbol] = {
                "market": "US",
                "metrics": metrics,
                "summary": _ema_summary_label(metrics),
            }
    if cn_symbols:
        series = _load_benchmark_closes(cn_db, "cn", cn_symbols, as_of, lookback_days=160)
        for symbol in cn_symbols:
            metrics = _ema_tape_metrics(series.get(symbol) or [])
            out[symbol] = {
                "market": "CN",
                "metrics": metrics,
                "summary": _ema_summary_label(metrics),
            }
    return out


# B.17: ema_tape_overlay + cn_lifecycle → scripts/sections/{ema_tape,cn_lifecycle}.py
from sections.ema_tape import render_ema_tape_overlay_markdown  # noqa: E402
from sections.cn_lifecycle import render_cn_lifecycle_section  # noqa: E402
def _ema_lookup(overlay: dict[str, dict[str, Any]] | None, ticker_field: str) -> dict[str, Any] | None:
    if not overlay:
        return None
    for alias in (ticker_field or "").split("/"):
        key = alias.strip().upper()
        if key in overlay:
            return overlay[key]
    return None


def _readiness_tier(row: dict[str, str]) -> tuple[str, float]:
    """Single source of truth: delegate to `score_source_review_readiness`.

    Previously this function was an inline copy of the canonical scorer with a
    "keep in sync" warning, which guaranteed drift. After the 2026-05-14 Codex
    review flagged the duplication, we import the public `tier_and_score`
    helper instead so a single change in the scorer reaches the daily report.
    """
    # Local import keeps the rest of this module independent of the script's
    # CLI side-effects on module load.
    from score_source_review_readiness import tier_and_score
    return tier_and_score(row)


def _source_review_market_matches(row: dict[str, str], market: str) -> bool:
    asset_pool = row.get("asset_pool") or ""
    country = (row.get("market_country") or "").strip()
    if market.upper() == "CN":
        return "中国" in asset_pool or country in {"A股主板", "中国"}
    # US standalone report covers US asset pool plus international satellites tracked
    # through US ADRs / IBKR access.
    return "美国" in asset_pool or "卫星" in asset_pool


def build_source_review_calendar(
    *,
    focus_symbols: Iterable[str] | None = None,
    queue_path: Path | None = None,
    limit: int = 60,
    ema_overlay: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read the curated AI-infra source-verification queue and bucket rows by market.

    The queue lives in `ai_infra/reports/source_verification_queue_v1.csv` and is
    maintained by the ai_infra research workbench. The daily report only consumes
    it; it never modifies the file.
    """
    path = queue_path or SOURCE_REVIEW_QUEUE_PATH
    if not path.exists():
        return {"us": {"status": "missing_queue", "rows": []}, "cn": {"status": "missing_queue", "rows": []}}

    focus = {str(symbol).upper() for symbol in (focus_symbols or []) if str(symbol).strip()}
    try:
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            raw_rows = list(reader)
    except Exception as exc:
        return {
            "us": {"status": f"error: {exc}", "rows": []},
            "cn": {"status": f"error: {exc}", "rows": []},
        }

    def _project(row: dict[str, str]) -> dict[str, Any]:
        rank_value = row.get("rank")
        try:
            rank_int: int | None = int(rank_value) if rank_value else None
        except ValueError:
            rank_int = None
        score_value = row.get("total_score")
        try:
            score_float: float | None = float(score_value) if score_value else None
        except ValueError:
            score_float = None
        ticker_aliases = [piece.strip() for piece in (row.get("ticker") or "").split("/") if piece.strip()]
        readiness_tier, readiness_score = _readiness_tier(row)
        ema_entry = _ema_lookup(ema_overlay, row.get("ticker") or "")
        return {
            "rank": rank_int,
            "priority_tier": row.get("priority_tier") or "",
            "readiness_tier": readiness_tier,
            "readiness_score": readiness_score,
            "ema_summary": ema_entry.get("summary") if ema_entry else "no_data",
            "ema_metrics": ema_entry.get("metrics") if ema_entry else None,
            "ticker": row.get("ticker") or "",
            "primary_ticker": ticker_aliases[0] if ticker_aliases else row.get("ticker") or "",
            "company": row.get("company") or "",
            "market_country": row.get("market_country") or "",
            "asset_pool": row.get("asset_pool") or "",
            "bfs_depth": row.get("bfs_depth") or "",
            "module": row.get("module") or "",
            "current_pool": row.get("current_pool") or "",
            "verification_status": row.get("verification_status") or "",
            "total_score": score_float,
            "score_bucket": row.get("score_bucket") or "",
            "primary_sources_to_find": row.get("primary_sources_to_find") or "",
            "metrics_to_verify": row.get("metrics_to_verify") or "",
            "upgrade_conditions": row.get("upgrade_conditions") or "",
            "downgrade_conditions": row.get("downgrade_conditions") or "",
            "evidence_state": row.get("evidence_state") or "",
            "counterevidence": row.get("counterevidence") or "",
            "market_context_notes": row.get("market_context_notes") or "",
            "dependency_path": row.get("dependency_path") or "",
            "in_focus": any(alias.upper() in focus for alias in ticker_aliases),
        }

    def _key(projected: dict[str, Any]) -> tuple[int, int, int]:
        tier_rank = _SOURCE_REVIEW_TIER_ORDER.get(projected.get("priority_tier") or "", 9)
        focus_rank = 0 if projected.get("in_focus") else 1
        return (focus_rank, tier_rank, projected.get("rank") or 9_999)

    buckets: dict[str, dict[str, Any]] = {}
    for market in ("US", "CN"):
        filtered = [
            _project(row)
            for row in raw_rows
            if _source_review_market_matches(row, market)
        ]
        filtered.sort(key=_key)
        focus_hits = sum(1 for row in filtered if row["in_focus"])
        buckets[market.lower()] = {
            "status": "ok",
            "scope": "focused_report_symbols" if focus else "all_symbols",
            "focus_symbol_count": len(focus),
            "focus_match_count": focus_hits,
            "total_rows": len(filtered),
            "rows": filtered[:limit],
            "queue_path": _safe_relative_path(path),
        }
    return buckets


SATELLITE_REGION_LABELS = {
    "台湾": "Taiwan",
    "日本": "Japan",
    "韩国": "Korea",
    "欧洲": "Europe",
    "以色列/US": "Israel/US",
    "以色列": "Israel",
    "新加坡": "Singapore",
    "香港": "Hong Kong",
}


def build_satellite_pool_report(
    *,
    queue_path: Path | None = None,
    ema_overlay: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Surface 卫星资产池 (TW/JP/KR/EU/IL) AI-infra source-review entries.

    AGENTS.md and ai_infra docs treat the satellite asset pool as a distinct
    research bucket — same BFS framework, different geography and regulatory
    risk. The main daily report previously folded these names into the US side;
    this function gives them an explicit ledger for the operator.
    """
    path = queue_path or SOURCE_REVIEW_QUEUE_PATH
    if not path.exists():
        return {"status": "missing_queue", "rows": []}
    try:
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            raw_rows = list(reader)
    except Exception as exc:
        return {"status": f"error: {exc}", "rows": []}

    satellite_rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if "卫星" not in (row.get("asset_pool") or ""):
            continue
        tier, score = _readiness_tier(row)
        ticker_aliases = [piece.strip() for piece in (row.get("ticker") or "").split("/") if piece.strip()]
        primary_ticker = ticker_aliases[0] if ticker_aliases else (row.get("ticker") or "")
        region_raw = (row.get("market_country") or "").strip()
        try:
            rank_int = int(row.get("rank") or "") if row.get("rank") else None
        except ValueError:
            rank_int = None
        ema_entry = _ema_lookup(ema_overlay, row.get("ticker") or "")
        satellite_rows.append(
            {
                "rank": rank_int,
                "priority_tier": row.get("priority_tier") or "",
                "ticker": row.get("ticker") or "",
                "primary_ticker": primary_ticker,
                "ema_summary": ema_entry.get("summary") if ema_entry else "no_data",
                "ema_metrics": ema_entry.get("metrics") if ema_entry else None,
                "aliases": ticker_aliases,
                "company": row.get("company") or "",
                "region_raw": region_raw,
                "region": SATELLITE_REGION_LABELS.get(region_raw, region_raw or "Unknown"),
                "bfs_depth": row.get("bfs_depth") or "",
                "module": row.get("module") or "",
                "current_pool": row.get("current_pool") or "",
                "verification_status": row.get("verification_status") or "",
                "evidence_state": row.get("evidence_state") or "",
                "counterevidence": row.get("counterevidence") or "",
                "market_context_notes": row.get("market_context_notes") or "",
                "primary_sources_to_find": row.get("primary_sources_to_find") or "",
                "metrics_to_verify": row.get("metrics_to_verify") or "",
                "upgrade_conditions": row.get("upgrade_conditions") or "",
                "downgrade_conditions": row.get("downgrade_conditions") or "",
                "dependency_path": row.get("dependency_path") or "",
                "dependency_edge": row.get("dependency_edge") or "",
                "readiness_tier": tier,
                "readiness_score": score,
            }
        )

    satellite_rows.sort(
        key=lambda r: (
            r.get("region") or "",
            _SOURCE_REVIEW_TIER_ORDER.get(r.get("priority_tier") or "", 9),
            r.get("rank") or 9_999,
        )
    )

    region_counts: Counter[str] = Counter()
    depth_counts: Counter[str] = Counter()
    readiness_counts: Counter[str] = Counter()
    for entry in satellite_rows:
        region_counts[entry["region"]] += 1
        depth_counts[entry.get("bfs_depth") or "-"] += 1
        readiness_counts[entry.get("readiness_tier") or "unscored"] += 1

    return {
        "status": "ok",
        "queue_path": _safe_relative_path(path),
        "total_rows": len(satellite_rows),
        "region_counts": dict(region_counts),
        "depth_counts": dict(depth_counts),
        "readiness_counts": dict(readiness_counts),
        "rows": satellite_rows,
    }


# B.13: satellite_pool moved to scripts/sections/satellite_pool.py
from sections.satellite_pool import render_satellite_pool_report_section  # noqa: E402
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
    con = _connect_ro(db_path)
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
            return "buy_after_pullback_review"
        return "buy_planned_entry_review"
    if execution_mode == "do_not_chase" or (fade is not None and fade >= 0.70):
        return "buy_after_pullback"
    return "buy_planned_entry; manage by T+1/T+3/T+max rule"


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


def summarize_cn(
    db_path: Path,
    start: date,
    as_of: date,
    promoted_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows, status = load_cn_strategy_rows(db_path, start, as_of)
    v2_all_rows = [row for row in rows if row.get("strategy_family") == "oversold_contrarian"]
    v2_rows = [row for row in v2_all_rows if cn_alpha_factory_sleeve_id(row) == CN_ALPHA_FACTORY_EXECUTION_SLEEVE]
    v2_metrics = compute_metrics("CN V2 oversold_contrarian EV-positive buckets", v2_rows)
    v2_all_metrics = compute_metrics("CN oversold_contrarian all buckets diagnostic", v2_all_rows)
    freshness = {
        "v2": rolling_freshness("CN V2 oversold_contrarian EV-positive buckets", v2_rows, as_of, min_n=20),
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
        alpha_sleeve_id = cn_alpha_factory_sleeve_id(row)
        if is_v2 and alpha_sleeve_id:
            gate_summary = cn_current_gate_summary(row)
            state = "Execution Alpha"
            reason = f"Alpha Factory sleeve {alpha_sleeve_id} current member; production ranker sets size/tier ({gate_summary})"
        elif family == "oversold_contrarian":
            gate_summary = cn_current_gate_summary(row)
            state = "Ranked Watch"
            reason = f"oversold rank candidate only; not in Alpha Factory execution sleeve ({gate_summary})"
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

    current = merge_current_rows_by_symbol([], current)
    current = merge_current_rows_by_symbol(
        current,
        query_cn_tape_current_candidates(db_path, current_date or as_of),
    )
    demote_unpromoted_current_rows("cn", current, promoted_rows or [])
    sector_screen = query_cn_sector_narrative_screen(db_path, current_date or as_of)

    return {
        "status": status,
        "current_status": current_status,
        "current_date": current_date.isoformat() if current_date else None,
        "metrics": {
            "v2": v2_metrics.to_dict(),
            "v2_all_oversold_diagnostic": v2_all_metrics.to_dict(),
        },
        "freshness": freshness,
        "lifecycle": lifecycle,
        "observed_lifecycle_prob": observed_lifecycle,
        "sector_narrative_screen": sector_screen,
        "current": current,
    }


def summarize_limit_up(db_path: Path, start: date, as_of: date) -> dict[str, Any]:
    if not db_path.exists():
        return {"status": "missing", "performance": {}, "current": []}
    con = _connect_ro(db_path)
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
                p.model_state, p.decision_state,
                CAST(json_extract_string(p.detail_json, '$.raw_model_score.p_touch_limit') AS DOUBLE) AS raw_p_touch_limit,
                CAST(json_extract_string(p.detail_json, '$.raw_model_score.p_limit_up') AS DOUBLE) AS raw_p_limit_up,
                CAST(json_extract_string(p.detail_json, '$.features[0]') AS DOUBLE) AS ret_1d,
                CAST(json_extract_string(p.detail_json, '$.features[2]') AS DOUBLE) AS ret_5d,
                CAST(json_extract_string(p.detail_json, '$.features[3]') AS DOUBLE) AS ret_20d,
                CAST(json_extract_string(p.detail_json, '$.features[12]') AS DOUBLE) AS industry_hot_ratio,
                CAST(json_extract_string(p.detail_json, '$.features[13]') AS DOUBLE) AS industry_limit_rate
            FROM limit_up_model_predictions p
            LEFT JOIN stock_basic sb ON sb.ts_code = p.symbol
            WHERE p.as_of = CAST(? AS DATE)
            ORDER BY
                p.probability_decile DESC,
                CASE WHEN p.decision_state = 'limit_up_candidate' THEN 1 ELSE 0 END DESC,
                ret_5d DESC NULLS LAST,
                ret_20d DESC NULLS LAST,
                raw_p_touch_limit DESC NULLS LAST,
                raw_p_limit_up DESC NULLS LAST,
                p.ev_lcb_80_pct DESC NULLS LAST,
                p.symbol
            LIMIT 20
            """,
            [as_iso(latest)],
        )
        for row in current:
            row["state"] = "Limit-Up Radar"
            row["reason"] = "broad-market diagnostic only; not part of the AI-infra production sleeve"
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


# B.11: pure-table renderers moved to scripts/sections/tables.py
from sections.tables import (  # noqa: E402
    render_metrics_table, render_current_table, render_missed_alpha_radar,
    render_limit_table, render_freshness_table, render_cn_lifecycle_table,
    render_market_watch_table,
)
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
        if row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") in CN_ALPHA_FACTORY_EXECUTION_SLEEVES
    )
    cn_observed_ea = sum(
        1
        for row in cn_current_rows
        if row.get("state") == "Execution Alpha" and row.get("observed_lifecycle_sleeve_id") == CN_OBSERVED_LIFECYCLE_SLEEVE
    )

    if us_counts.get("Execution Alpha", 0) > 0 and us_metric_ok:
        us_state = "stock_trade"
        us_size = "0.05-0.125R/name; 0.50R basket cap; next-session review"
    elif (
        us_stock_ok
        and us_stock_fresh.get("state") in {"fresh", "usable_but_monitor"}
        and (us_counts.get("Execution Alpha", 0) + us_counts.get("Positive EV Setup", 0)) > 0
    ):
        us_state = "conditional_stock_trade"
        us_size = "0.05-0.125R/name; 0.50R basket cap; stock-only; next-session review"
    elif (us_counts.get("Execution Alpha", 0) + us_counts.get("Positive EV Setup", 0)) > 0:
        us_state = "opportunity_stock_trade"
        us_size = "0.05R/name; 0.25R basket cap; stock-only; setup/watch unless EV gate passes"
    else:
        us_state = "no_current_setup"
        us_size = "0R"

    if cn_alpha_factory_ea > 0 and cn_metric_ok and cn_lifecycle_ok:
        cn_state = "stock_trade"
        cn_size = "0.35R/name; 1.20R basket cap; planned-entry only; T+1/T+3 review"
    elif cn_observed_ea > 0 and cn_lifecycle_ok:
        cn_state = "observed_lifecycle_trade"
        cn_size = "0.14R top / 0.10R secondary; 1.00R observed basket cap; planned-entry only; no blind T+5 hold"
    elif cn_counts.get("Positive EV Setup", 0) > 0 and cn_lifecycle_ok:
        cn_state = "opportunity_stock_trade"
        cn_size = "0.10R/name; 0.50R basket cap; planned-entry only; T+1/T+3 review"
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
                f"options auxiliary expression n={us_opt.get('n', 0)}"
            ),
            "kill_switch": "Production ranker controls which V2 sleeve names can receive stock trade size today; options are auxiliary evidence only.",
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
            "kill_switch": (
                f"Only `{CN_AI_INFRA_PRODUCTION_SLEEVE}`, `{CN_ALPHA_FACTORY_EXECUTION_SLEEVE}` or "
                f"`{CN_OBSERVED_LIFECYCLE_SLEEVE}` plus production trade tier can receive new money."
            ),
        },
        {
            "market": "Broad A-share radar",
            "profit_state": "out_of_scope_for_ai_infra",
            "max_auto_size": "0R",
            "why": (
                f"top-decile lift={fmt_num(limit_perf.get('avg_top_decile_lift'))}, "
                f"avg EV after cost={fmt_pct(limit_perf.get('avg_next_ret_pct'))}; not an AI-infra production sleeve"
            ),
            "kill_switch": "Keep broad-market limit-up rows out of AI-infra sizing; build a separate broad A-share strategy if needed.",
        },
    ]


# B.16: strategy_direction + guardrails → scripts/sections/strategy_direction.py
from sections.strategy_direction import (  # noqa: E402
    render_strategy_direction_table, render_adjustment_rules,
    render_strategy_direction as _render_strategy_direction_impl,
    render_profit_guardrails,
)

def render_strategy_direction(payload):
    return _render_strategy_direction_impl(payload, strategy_mode=MAIN_STRATEGY_MODE)

# fmt_r + clean_table_text extracted to scripts/lib/fmt.py (Phase A.1)
from lib.fmt import fmt_r, clean_table_text  # noqa: E402


# report_safe_options_context moved to lib.fmt (Phase A.1 extension)
from lib.fmt import report_safe_options_context  # noqa: E402


# B.6: options anomaly+tenor section + helpers moved to scripts/sections/options_radar.py
from sections.options_radar import (  # noqa: E402
    _parse_float, _format_option_float, _format_tenor_ratio_text, _tenor_ratio_at,
    _tenor_ref_call_text, _options_tenor_reading, _options_symbol,
    _options_symbol_aliases, _options_related_context, _options_related_guidance,
    _options_tenor_related_guidance,
    render_options_tenor_section, render_options_anomaly_section,
)
# ACTION_LABELS + action_label moved to lib/fmt.py
from lib.fmt import ACTION_LABELS, action_label  # noqa: E402
# B.18: selection_rationale + reason helpers → scripts/sections/selection_rationale.py
# narrative_label moved to lib/fmt.py
from lib.fmt import narrative_label  # noqa: E402
from sections.selection_rationale import (  # noqa: E402
    RANKER_TRADE_TIERS, ranker_row_priority, best_ranker_rows_by_symbol,
    actionable_ranked_row, trade_orientation, promotion_metric_summary,
    quant_reason, news_reason, history_reason,
    render_actionable_selection_rationale, render_market_selection_rationale,
    render_cn_actionable_evidence,
)
def human_risk_plan(value: Any) -> str:
    text = str(value or "-")
    replacements = {
        "handle": "防守线",
        "target": "目标",
        "stop": "止损",
        "3 sessions / next catalyst": "次日复核；不机械持有3-5日",
        US_DEFAULT_TIME_EXIT: "次日复核；不机械持有3-5日",
        CN_DEFAULT_TIME_EXIT: "T+1/T+3复核；只有horizon edge仍为正才持有到T+5",
        "T+1 review; T+5 hard exit unless trend extends": "T+1复核；趋势不延续则T+5硬退出",
        "T+1 review": "T+1复核",
        "T+3 no +1R follow-through -> exit": "T+3没有+1R跟随就退出",
        "hard max T+5": "最晚T+5退出",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Strip the redundant boilerplate that duplicates the explicit
    # stop/target prices already printed earlier in the same string
    # (e.g. "; plan entry=latest close; 止损=-6%; 目标=+10%; review …")
    cut_markers = ("; plan entry=", "; entry=latest close", "; plan entry=latest close")
    for marker in cut_markers:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]
    return text.strip("; ").strip()


def human_trigger_text(market: str, row: dict[str, Any]) -> str:
    trigger = str(row.get("trigger") or "")
    if market.upper() == "CN":
        if "cn_tape_leadership_continuation" in trigger:
            return trigger.replace("cn_tape_leadership_continuation", "右侧强势延续")
        if "cn_observed_lifecycle_prob" in trigger:
            return trigger.replace("cn_observed_lifecycle_prob", "历史相似左侧赔率")
    if market.upper() == "US" and "us_theme_cluster_momentum" in trigger:
        return trigger.replace("us_theme_cluster_momentum", "主题basket动量")
    return trigger or "-"


# fmt_rate_pct + symbol_key extracted to scripts/lib/fmt.py (Phase A.1)
from lib.fmt import fmt_rate_pct  # noqa: E402
from lib.fmt import symbol_key as _symbol_key  # keep old name for backward compat  # noqa: E402


def _is_cn_main_board(symbol: Any) -> bool:
    """True for CN main-board (主板) tickers only.

    The daily actionable list is meant to be *operable* — main board has the
    normal ±10% limit and needs no special account permission. STAR (科创板
    688/689), ChiNext (创业板 300/301) and BSE (北交所 8xx/4xx) are excluded:
    they may still sit in the universe / ranker, just not the daily top-5.
    """
    code = str(symbol or "").split(".")[0].strip()
    return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003"))


def _ranker_lookup(payload: dict[str, Any], market: str) -> dict[str, dict[str, Any]]:
    key = "cn_opportunity_ranker" if market.upper() == "CN" else "us_opportunity_ranker"
    ranker = payload.get(key) or {}
    out: dict[str, dict[str, Any]] = {}
    for row in [*(ranker.get("all_rows") or []), *(ranker.get("top_rows") or [])]:
        symbol = _symbol_key(row.get("symbol"))
        if symbol and symbol not in out:
            out[symbol] = row
    return out


def _row_source(row: dict[str, Any], ranked: dict[str, Any]) -> str:
    return str(
        ranked.get("alpha_sleeve_id")
        or ranked.get("observed_lifecycle_sleeve_id")
        or row.get("execution_source")
        or row.get("strategy_family")
        or "rank_only"
    )


def _decision_trigger(market: str, row: dict[str, Any], ranked: dict[str, Any], guard: dict[str, Any]) -> str:
    source = _row_source(row, ranked)
    if market.upper() == "CN":
        exp_r = ranked.get("expected_r_t3", row.get("expected_r_t3"))
        lcb_r = ranked.get("lcb80_r_t3", row.get("lcb80_r_t3"))
        obs_n = ranked.get("observed_probability_n", row.get("observed_probability_n"))
        headline = round_or_none(ranked.get("headline_risk"))
        knife = ranked.get("falling_knife_score")
        parts = [source]
        if exp_r is not None or lcb_r is not None:
            parts.append(f"ExpR {fmt_num(exp_r)} / LCBR {fmt_num(lcb_r)} / n {fmt_num(obs_n, 0)}")
        if headline is not None:
            parts.append(f"headline {fmt_num(headline * 100.0, 0)}")
        if knife is not None:
            parts.append(f"knife {fmt_num(knife, 0)}")
        return "; ".join(parts)

    score = ranked.get("rank_score")
    headline = round_or_none(ranked.get("headline_risk"))
    quality = ranked.get("flow_options_quality")
    latest = ranked.get("latest_headline")
    parts = [source]
    if score is not None:
        parts.append(f"score {fmt_num(score)}")
    if headline is not None:
        parts.append(f"headline {fmt_num(headline * 100.0, 0)}")
    if quality is not None:
        parts.append(f"flow/options {fmt_num(quality, 0)}")
    if latest:
        parts.append(clean_table_text(latest, 70))
    if len(parts) == 1 and guard.get("why"):
        parts.append(str(guard.get("why")))
    return "; ".join(parts)


def evaluate_us_execution_gate(payload: dict[str, Any]) -> dict[str, Any]:
    """Single production contract for US stock execution rows.

    US actionables require the stable strategy gate to have an explicit passed
    policy, and the stock price tape must be current for the report date. Options
    may still render as context, but cannot rescue execution R when this gate is
    closed.
    """
    bulletin = payload.get("strategy_alpha_bulletin") or {}
    ev_status = str((bulletin.get("ev_status") or {}).get("us") or "unknown").lower()
    selected_policy = (bulletin.get("selected_policies") or {}).get("us")
    evaluated_through = (bulletin.get("evaluated_through") or {}).get("us")
    data_status = payload.get("us_market_data_status") or {}
    stock_current = data_status.get("stock_data_current")
    latest_stock_date = data_status.get("prices_daily_latest_date")
    as_of = payload.get("as_of")
    reasons: list[str] = []

    if bulletin and (ev_status != "passed" or not selected_policy):
        reasons.append(
            "US stable alpha gate not passed "
            f"(ev_status={ev_status}, selected_policy={selected_policy or 'none'}, "
            f"evaluated_through={evaluated_through or '-'})"
        )
    if stock_current is False:
        reasons.append(
            "US stock tape is stale or market is closed "
            f"(latest prices_daily={latest_stock_date or '-'}, report_date={as_of or '-'})"
        )

    return {
        "allowed": not reasons,
        "ev_status": ev_status,
        "selected_policy": selected_policy,
        "evaluated_through": evaluated_through,
        "stock_data_current": stock_current,
        "latest_stock_date": latest_stock_date,
        "reasons": reasons,
        "top_blocker": "; ".join(reasons) if reasons else None,
    }


def _has_ai_infra_context(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in (
            "alpha_sleeve_id",
            "observed_lifecycle_sleeve_id",
            "execution_source",
            "supercycle_layer",
            "ai_infra_module",
            "ai_infra_evidence_state",
            "evidence_state",
            "role",
        )
    ).lower()
    return (
        "ai" in text
        or "cpo" in text
        or "hbm" in text
        or "osat" in text
        or "data center" in text
        or "数据中心" in text
        or "光模块" in text
    )


def _source_review_symbols(payload: dict[str, Any], market: str) -> set[str]:
    section = (payload.get("source_review_calendar") or {}).get(market.lower()) or {}
    symbols: set[str] = set()
    for row in section.get("rows") or []:
        raw = row.get("primary_ticker") or row.get("ticker") or row.get("symbol")
        for token in str(raw or "").replace("/", ",").split(","):
            sym = _symbol_key(token)
            if sym:
                symbols.add(sym)
    return symbols


def build_production_decision_summary(payload: dict[str, Any]) -> dict[str, Any]:
    overlay = payload.get("portfolio_risk_overlay") or {}
    overlay_rows = overlay.get("rows") or []
    cn_lookup = _ranker_lookup(payload, "CN")
    us_lookup = _ranker_lookup(payload, "US")
    guard_by_market = {
        str(row.get("market") or "").upper(): row for row in payload.get("profit_guardrails") or []
    }
    market_order = {"CN": 0, "US": 1}

    us_gate = evaluate_us_execution_gate(payload)
    actionable: list[dict[str, Any]] = []
    blocked_execution: list[dict[str, Any]] = []
    us_trade_plan = payload.get("us_trade_plan") or {}
    gamma_v2_lookup = {
        _symbol_key(row.get("symbol")): row
        for row in (payload.get("gamma_spring") or {}).get("rows") or []
        if _symbol_key(row.get("symbol"))
    }
    holdings = _load_virtual_holdings()
    for row in overlay_rows:
        final_r = round_or_none(row.get("final_r"))
        if final_r is None or final_r <= 0.0:
            continue
        market = str(row.get("market") or "").upper()
        ranked = (cn_lookup if market == "CN" else us_lookup).get(_symbol_key(row.get("symbol")), {})
        symbol_key = _symbol_key(row.get("symbol"))
        if market == "US" and not us_gate["allowed"]:
            blocked_execution.append(
                {
                    "market": "US",
                    "symbol": row.get("symbol"),
                    "name": row.get("name") or ranked.get("name") or "",
                    "state": "execution_blocked_0r",
                    "reason": clean_table_text(us_gate["top_blocker"], 160),
                }
            )
            continue
        trade_plan = us_trade_plan.get(symbol_key) if market == "US" else {}
        action = ranked.get("production_action") or row.get("lifecycle_action") or row.get("state") or "-"
        tier = ranked.get("production_tier") or row.get("production_tier") or row.get("state") or "-"
        entry = (
            ranked.get("observation_entry_zone")
            or ranked.get("entry")
            or row.get("observation_entry_zone")
            or row.get("entry")
            or (trade_plan or {}).get("entry")
            or ("planned-entry/pullback" if market == "CN" else "stock trade")
        )
        if market == "CN":
                risk_plan = (
                    f"handle {ranked.get('handling_line') or row.get('handling_line') or '-'}; "
                    f"target {ranked.get('first_target') or row.get('first_target') or '-'}; "
                    f"{ranked.get('time_exit') or row.get('time_exit') or CN_DEFAULT_TIME_EXIT}"
                )
        else:
            stop_value = (
                ranked.get("stop")
                or ranked.get("stop_price")
                or row.get("stop")
                or row.get("stop_price")
                or (trade_plan or {}).get("stop")
            )
            target_value = (
                ranked.get("target")
                or ranked.get("target_price")
                or row.get("target")
                or row.get("target_price")
                or (trade_plan or {}).get("target")
            )
            plan_suffix = ""
            if trade_plan:
                if trade_plan.get("status") == "ok":
                    plan_suffix = f"; plan {trade_plan.get('rule')} ({trade_plan.get('latest_date')})"
                elif not stop_value and not target_value:
                    plan_suffix = f"; plan blocker {trade_plan.get('rule')}"
            ranked_gamma_score = round_or_none(ranked.get("gamma_v2_alpha_score"))
            gamma_v2 = gamma_v2_lookup.get(symbol_key) or {}
            gamma_suffix = ""
            if ranked_gamma_score is not None:
                gamma_suffix = (
                    "; gamma_v2_alpha "
                    f"{ranked.get('gamma_v2_management_signal') or '-'} "
                    f"score={fmt_num(ranked_gamma_score)} "
                    f"dealer={fmt_rate_pct(ranked.get('gamma_v2_dealer_pressure_proxy'))} "
                    f"wall={ranked.get('gamma_v2_wall_transition') or '-'}"
                )
            elif gamma_v2:
                gamma_suffix = (
                    "; gamma_v2 "
                    f"{gamma_v2.get('management_signal') or '-'} "
                    f"dealer={fmt_rate_pct(gamma_v2.get('dealer_pressure_proxy'))} "
                    f"wall={gamma_v2.get('wall_transition') or '-'} "
                    f"mult={fmt_num(gamma_v2.get('gamma_v2_multiplier'))}"
                )
            risk_plan = (
                f"stop {fmt_num(stop_value)}; "
                f"target {fmt_num(target_value)}; "
                f"{ranked.get('time_exit') or row.get('time_exit') or US_DEFAULT_TIME_EXIT}"
                f"{plan_suffix}"
                f"{gamma_suffix}"
            )
        evidence_state = (
            ranked.get("ai_infra_evidence_state")
            or ranked.get("evidence_state")
            or row.get("ai_infra_evidence_state")
            or row.get("evidence_state")
            or ""
        )
        held_r = (holdings.get(market) or {}).get(symbol_key, 0.0)
        delta_text = position_delta_text(held_r, final_r)
        actionable.append(
            {
                "market": market,
                "symbol": row.get("symbol"),
                "name": row.get("name") or ranked.get("name") or "",
                "action": action,
                "convexity": classify_convexity(action),
                "size_r": final_r,
                "held_r": held_r,
                "position_delta_text": delta_text,
                "tier": tier,
                "source": _row_source(row, ranked),
                "entry": entry,
                "risk_plan": risk_plan,
                "hedge": row.get("hedge_instrument"),
                "hedge_notional_r": row.get("hedge_notional_r"),
                "net_beta_r": row.get("net_beta_r"),
                "trigger": _decision_trigger(market, row, ranked, guard_by_market.get(market, {})),
                "evidence_state": evidence_state,
                "ai_infra_evidence_state": ranked.get("ai_infra_evidence_state") or row.get("ai_infra_evidence_state") or "",
            }
        )
    actionable.sort(
        key=lambda row: (
            market_order.get(str(row.get("market") or ""), 9),
            -(round_or_none(row.get("size_r")) or 0.0),
            str(row.get("symbol") or ""),
        )
    )

    # CN daily directive: surface only the top 5 reranked A-share names, and
    # only MAIN-BOARD (主板) names — STAR/ChiNext need account permission and
    # run a ±20% limit, so they are not "可操作" for the daily list. The list
    # is already conviction-sorted (size_r desc); STAR/ChiNext names stay in
    # the ranker / watch views, they just drop off the daily actionable top-5.
    cn_ranked = [r for r in actionable if str(r.get("market") or "").upper() == "CN"]
    non_cn = [r for r in actionable if str(r.get("market") or "").upper() != "CN"]
    cn_main = [r for r in cn_ranked if _is_cn_main_board(r.get("symbol"))]
    actionable = cn_main[:CN_DAILY_TOP_N] + non_cn

    watch: list[dict[str, Any]] = list(blocked_execution)
    cn_source_review_symbols = _source_review_symbols(payload, "CN")
    for market, lookup in (("CN", cn_lookup), ("US", us_lookup)):
        rows = sorted(lookup.values(), key=lambda row: int(row.get("rank") or 9999))
        if market == "CN":
            if cn_source_review_symbols:
                rows = [row for row in rows if _symbol_key(row.get("symbol")) in cn_source_review_symbols]
            else:
                rows = [row for row in rows if _has_ai_infra_context(row)]
        event_rows = [
            row
            for row in rows
            if str(row.get("production_tier") or "") in {"event_risk_watch", "falling_knife_watch"}
        ]
        ranked_watch = [
            row
            for row in rows
            if str(row.get("production_action") or "").startswith("rank_only")
            or str(row.get("production_tier") or "").endswith("_watch")
        ]
        for row in [*event_rows, *ranked_watch][:6]:
            reason = (
                row.get("latest_headline")
                or row.get("size_hint")
                or row.get("reason")
                or row.get("production_action")
                or "watch only"
            )
            watch.append(
                {
                    "market": market,
                    "symbol": row.get("symbol"),
                    "name": row.get("name") or "",
                    "state": row.get("production_tier") or row.get("state") or "watch",
                    "reason": clean_table_text(reason, 120),
                }
            )
    watch = watch[:12]

    readiness_rows = (payload.get("profit_readiness") or {}).get("rows") or []
    readiness_by_area = {str(row.get("area") or ""): row for row in readiness_rows}
    option_ledger = payload.get("option_shadow_ledger") or {}
    event_symbols = [
        f"{row.get('symbol')}{(' ' + row.get('name')) if row.get('name') else ''}"
        for row in watch
        if row.get("state") == "event_risk_watch"
    ]
    special_symbols = [
        f"{row.get('symbol')}{(' ' + row.get('name')) if row.get('name') else ''}"
        for row in watch
        if row.get("state") == "special_treatment_watch"
    ]
    no_trade = [
        {
            "area": "US execution gate",
            "status": "pass" if us_gate["allowed"] else "0R",
            "reason": us_gate["top_blocker"] or "stable alpha gate and stock tape are current",
        },
        {
            "area": "US options",
            "status": "auxiliary signal only",
            "reason": (
                f"real bid/ask option PnL={option_ledger.get('real_bid_ask_resolved_count', 0)}, "
                f"proxy={option_ledger.get('proxy_resolved_count', 0)}, "
                f"unresolved={option_ledger.get('unresolved_count', 0)}; options inform stock ranking/risk only"
            ),
        },
        {
            "area": "CN/US rank-only rows",
            "status": "0R",
            "reason": "ranked watch is observation only; no sleeve/probability tier means no new money",
        },
        {
            "area": "Event-risk names",
            "status": "0R",
            "reason": ", ".join(event_symbols[:6]) if event_symbols else "none today",
        },
        {
            "area": "ST/restructuring names",
            "status": "0R",
            "reason": ", ".join(special_symbols[:6]) if special_symbols else "none today",
        },
        {
            "area": "Live execution ledger",
            "status": "not closed",
            "reason": (readiness_by_area.get("Live execution ledger") or {}).get("blocker") or "live fills ledger incomplete",
        },
    ]

    cn_actions = [row for row in actionable if row.get("market") == "CN"]
    us_actions = [row for row in actionable if row.get("market") == "US"]
    gross_r = sum(float(row.get("size_r") or 0.0) for row in actionable)
    cn_r = sum(float(row.get("size_r") or 0.0) for row in cn_actions)
    us_r = sum(float(row.get("size_r") or 0.0) for row in us_actions)
    top_blocker = ((payload.get("pipeline_requirements_audit") or {}).get("summary") or {}).get("top_blocker")
    if not us_gate["allowed"]:
        top_blocker = us_gate["top_blocker"] or top_blocker
    summary = {
        "headline": (
            f"CN stock basket {len(cn_actions)} names ({fmt_r(cn_r)}), "
            f"US stock trades {len(us_actions)} names ({fmt_r(us_r)}); "
            "options are auxiliary signals; disabled CN signal families do not create R."
        ),
        "gross_r": round_or_none(gross_r, 4),
        "cn_action_count": len(cn_actions),
        "us_action_count": len(us_actions),
        "cn_r": round_or_none(cn_r, 4),
        "us_r": round_or_none(us_r, 4),
        "watch_count": len(watch),
        "no_trade_count": len(no_trade),
        "portfolio_var95_r": (overlay.get("summary") or {}).get("var95_r_proxy"),
        "beta_hedge_r": (overlay.get("summary") or {}).get("beta_hedge_r"),
        "net_beta_r": (overlay.get("summary") or {}).get("net_beta_r"),
        "hedged_var95_r": (overlay.get("summary") or {}).get("hedged_var95_r_proxy"),
        "risk_attribution": (overlay.get("summary") or {}).get("risk_attribution"),
        "top_blocker": top_blocker,
        "us_execution_gate": us_gate,
    }
    # Today's actionable becomes tomorrow's virtual "held". Symbols not in
    # today's list keep their prior held size (orphan positions persist
    # until the operator clears them in virtual_holdings.json).
    _persist_virtual_holdings(actionable, holdings, payload.get("as_of"))
    return {
        "as_of": payload.get("as_of"),
        "summary": summary,
        "actionable": actionable,
        "watch": watch,
        "no_trade": no_trade,
    }


FEAR_GREED_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "fear_greed"
OPTIONS_ANOMALY_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_options_anomaly_radar"
OPTIONS_TENOR_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_options_tenor_radar"
BUBBLE_HEDGE_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "bubble_hedge_radar"
REPORT_ACTION_BACKTEST_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2_report_backtest"


def load_bubble_hedge_payload(as_of: str) -> dict[str, Any] | None:
    path = BUBBLE_HEDGE_ROOT / as_of / "bubble_hedge.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def load_report_action_backtest_summary(as_of: str) -> dict[str, Any] | None:
    path = REPORT_ACTION_BACKTEST_ROOT / as_of / "report_action_backtest_summary.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


# B.14: realized_horizon_edge section + helpers → scripts/sections/realized_horizon.py
from sections.realized_horizon import (  # noqa: E402
    _horizon_edge_row, _horizon_verdict, render_realized_horizon_edge_section,
)
# B.7: regime/fear_greed/bubble_hedge/execution_gate sections moved to scripts/sections/regime_views.py
from sections.regime_views import (  # noqa: E402
    render_risk_regime_section, render_bubble_hedge_section,
    render_fear_greed_section, render_us_execution_gate_notice,
)
def _latest_dated_subdir(root: Path, as_of: str) -> str | None:
    """Latest YYYY-MM-DD subdir under root with name <= as_of, else None.

    US options data lags 1 trading day (CBOE snapshot is the prior
    session's close), so the radar's output dir is dated the latest US
    trade day — not today's CST report date. Without this fallback the
    daily report shows "n/a" for the options reads every morning.
    """
    if not root.exists():
        return None
    candidates = sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and len(d.name) == 10 and d.name <= as_of
    )
    return candidates[-1] if candidates else None


def load_options_anomaly_payload(as_of: str) -> list[dict[str, Any]]:
    path = OPTIONS_ANOMALY_ROOT / as_of / "options_anomaly.csv"
    if not path.exists():
        fallback = _latest_dated_subdir(OPTIONS_ANOMALY_ROOT, as_of)
        if fallback is None:
            return []
        path = OPTIONS_ANOMALY_ROOT / fallback / "options_anomaly.csv"
        if not path.exists():
            return []
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_options_tenor_signals(as_of: str) -> list[dict[str, Any]]:
    path = OPTIONS_TENOR_ROOT / as_of / "options_tenor_signals.jsonl"
    if not path.exists():
        fallback = _latest_dated_subdir(OPTIONS_TENOR_ROOT, as_of)
        if fallback is None:
            return []
        path = OPTIONS_TENOR_ROOT / fallback / "options_tenor_signals.jsonl"
        if not path.exists():
            return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


# _display_tenor_name moved to lib.fmt as display_tenor_name (Phase A.1 extension)
from lib.fmt import display_tenor_name as _display_tenor_name  # noqa: E402


def load_fear_greed_payload(as_of: str) -> dict[str, Any] | None:
    path = FEAR_GREED_ROOT / as_of / "fear_greed.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


# build_market_regime_score + render_market_regime_score_section extracted to
# scripts/sections/market_regime.py (Phase B.1 of REFACTOR_PLAN.md)
from sections.market_regime import build_market_regime_score, render_market_regime_score_section  # noqa: E402


def render_production_decision_summary(payload: dict[str, Any]) -> list[str]:
    decision = payload.get("production_decision_summary") or build_production_decision_summary(payload)
    summary = decision.get("summary") or {}
    actions = decision.get("actionable") or []
    lines = [
        "## 今日交易决策 / Production Decision",
        "",
        f"- 今日动作: {summary.get('headline') or '-'}",
        "- R 口径: 全部是归一化风险单位，不换算 RMB；`1R` 表示一份标准风险预算，`0.05R` 表示该预算的 5%。",
        f"- 归一化 R 占用: long alpha {fmt_r(summary.get('gross_r'))}; CN {fmt_r(summary.get('cn_r'))}; US {fmt_r(summary.get('us_r'))}; beta hedge {fmt_r(summary.get('beta_hedge_r'))}; net beta {fmt_r(summary.get('net_beta_r'))}; hedged VaR95 proxy {fmt_r(summary.get('hedged_var95_r'))}",
        f"- 当前缺口: {summary.get('top_blocker') or '-'}",
        "",
        "### 可交易 / Actionable",
        "",
        "| Market | Symbol | Name | Action | Size | Hedge | Net beta | Tier | Entry | Risk / Exit | Trigger |",
        "|---|---|---|---|---:|---|---:|---|---|---|---|",
    ]
    if actions:
        for row in actions[:14]:
            action_text = str(row.get('action') or '-') + (row.get('position_delta_text') or '')
            lines.append(
                f"| {row.get('market')} | {row.get('symbol')} | {row.get('name') or '-'} | "
                f"{action_text} | {fmt_r(row.get('size_r'))} | "
                f"{row.get('hedge') or '-'} {fmt_num(row.get('hedge_notional_r'), 4)}R | {fmt_r(row.get('net_beta_r'))} | {row.get('tier')} | "
                f"{clean_table_text(row.get('entry'), 70)} | {clean_table_text(row.get('risk_plan'), 100)} | "
                f"{clean_table_text(row.get('trigger'), 120)} |"
            )
    else:
        lines.append("| - | - | - | no production action today | 0R | - | 0R | - | - | - | - |")

    lines += render_actionable_selection_rationale(payload, actions)
    lines += render_cn_actionable_evidence(payload, actions)

    lines += [
        "",
        "### 只能观察 / Watch",
        "",
        "| Market | Symbol | Name | State | Reason |",
        "|---|---|---|---|---|",
    ]
    watch_rows = decision.get("watch") or []
    if watch_rows:
        for row in watch_rows[:12]:
            lines.append(
                f"| {row.get('market')} | {row.get('symbol')} | {row.get('name') or '-'} | "
                f"{row.get('state')} | {clean_table_text(row.get('reason'), 120)} |"
            )
    else:
        lines.append("| - | - | - | - | no watch-only rows highlighted |")

    lines += [
        "",
        "### 禁止碰 / 0R / 未闭环",
        "",
        "| Area | Status | Reason |",
        "|---|---|---|",
    ]
    for row in decision.get("no_trade") or []:
        lines.append(
            f"| {row.get('area')} | {row.get('status')} | {clean_table_text(row.get('reason'), 140)} |"
        )
    lines.append("")
    return lines


def build_cn_shadow_full(cn_db: Path, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    """Per-stock implied vol surface for A 股 — derived from ETF option curves.

    A 股 has no individual-stock options, so quant-research-cn's shadow_full
    module uses 50ETF/300ETF/创业板ETF option curves to back out a per-stock
    proxy: ATM IV at 90d, 80% / 90% put prices, touch probability, skew.

    Returns: {ticker: {"atm_iv_90d": pct, "touch_90_3m": prob_0_1,
                       "skew_90_3m": float, "put_90_3m": price,
                       "put_80_3m": price, "floor_1sigma": float}}
    """
    out: dict[str, dict[str, Any]] = {}
    if not cn_db.exists() or not symbols:
        return out
    syms = sorted({str(s).upper() for s in symbols if s})
    if not syms:
        return out
    placeholders = ", ".join("?" for _ in syms)
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        # Find latest available shadow_full date ≤ as_of
        latest = con.execute(
            "SELECT MAX(as_of) FROM analytics WHERE module='shadow_full' AND as_of <= ?",
            [as_of.isoformat()],
        ).fetchone()
        if not latest or not latest[0]:
            return out
        eff_date = str(latest[0])
        rows = con.execute(
            f"SELECT ts_code, metric, value, detail FROM analytics "
            f"WHERE module='shadow_full' AND as_of = ? AND ts_code IN ({placeholders})",
            [eff_date, *syms],
        ).fetchall()
        for ts, metric, value, detail in rows:
            sym = str(ts).upper()
            rec = out.setdefault(sym, {})
            metric_key = str(metric).replace("shadow_", "").replace("_3m", "")
            rec[metric_key] = float(value) if value is not None else None
            if detail:
                try:
                    d = json.loads(detail)
                    if "atm_iv_90d" in d and "atm_iv_90d" not in rec:
                        rec["atm_iv_90d"] = d["atm_iv_90d"]
                    if "downside_iv_90pct_put" in d:
                        rec["iv_90pct_put"] = d["downside_iv_90pct_put"]
                    if "downside_iv_80pct_put" in d:
                        rec["iv_80pct_put"] = d["downside_iv_80pct_put"]
                except (json.JSONDecodeError, TypeError):
                    pass
        out["_effective_date"] = {"effective_date": eff_date}
    finally:
        con.close()
    return out


# B.4: build_options_verdicts + _iv_action_hint + render_iv_view_section moved to
# scripts/sections/iv_view.py
from sections.iv_view import build_options_verdicts  # noqa: E402
from sections.index_skew import build_index_skew  # noqa: E402
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


def _allowed_now_has_money(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return bool(text) and not text.startswith("0r")


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
    limit_guard = _guardrail_by_market(payload.get("profit_guardrails") or [], "Broad A-share radar")
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
    cn_tape_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == CN_TAPE_SLEEVE_ID
        for row in cn_current
    )
    cn_ai_infra_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == CN_AI_INFRA_PRODUCTION_SLEEVE
        for row in cn_current
    )
    if cn_ea_count <= 0:
        cn_state = "no_current_execution_sleeve"
        cn_next_step = "Fix current-pool generation/probability layer: today's CN rows do not map to `cn_oversold_ev_positive`, so production size is correctly 0R."
    elif cn_manual_r > 0:
        cn_state = "manual_stock_trade_review"
        cn_next_step = "Do not chase open. Use the explicit portfolio R plan, require planned-entry/pullback fill, and record fill/exit in CN live ledger."
    else:
        cn_state = "stock_trade_ready"
        cn_next_step = "Trade only planned-entry/pullback stock entries for current execution-sleeve names; record fills/exits in CN live ledger."
    option_summary = (option_ledger.get("summary") or {}).get("overall_long") or {}
    real_option_summary = (option_ledger.get("summary") or {}).get("real_bid_ask_options") or {}
    limit_perf = limit_up.get("performance") or {}

    rows = [
        {
            "area": "CN main alpha",
            "state": cn_state,
            "allowed_now": cn_guard.get("max_auto_size") or "0R",
            "evidence": (
                f"active sleeve={CN_TAPE_SLEEVE_ID if cn_tape_ea else CN_AI_INFRA_PRODUCTION_SLEEVE if cn_ai_infra_ea else 'oversold/observed lifecycle'}; "
                f"oversold LCB80 secondary {fmt_pct((cn.get('metrics') or {}).get('v2', {}).get('lcb80_pct'))}; "
                f"lifecycle {((cn.get('lifecycle') or {}).get('policy') or {}).get('best_bucket') or '-'}; current EA={cn_ea_count}"
            ),
            "blocker": (
                f"portfolio final CN R={fmt_num(cn_final_r, 4)}; override stock R={fmt_num(cn_manual_r, 4)}; "
                f"risk reasons={', '.join(cn_zero_reasons) if cn_zero_reasons else 'none'}; "
                f"current={_row_symbols(cn_current, state='Execution Alpha')}"
            ),
            "next_step": cn_next_step,
            "priority": 1,
        },
        {
            "area": "US stock trade",
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
            "next_step": "Only the current US V2 stock execution sleeve can receive stock trade size; legacy rows stay ranked watch. Winner Hold Overlay still controls existing profitable names.",
            "priority": 2,
        },
        {
            "area": "US options",
            "state": "auxiliary_signal_only",
            "allowed_now": "no option orders; use options/flow only as stock ranking and risk evidence",
            "evidence": (
                f"real bid/ask n={real_option_summary.get('n', 0)}, "
                f"LCB80 {fmt_pct(real_option_summary.get('lcb80_pct'))}; "
                f"proxy/stock n={option_summary.get('n', 0)}"
            ),
            "blocker": (
                f"real_bid_ask={option_ledger.get('real_bid_ask_resolved_count', 0)}, "
                f"proxy={option_ledger.get('proxy_resolved_count', 0)}, "
                f"stock_proxy={option_ledger.get('stock_proxy_resolved_count', 0)}, "
                f"unresolved={option_ledger.get('unresolved_count', 0)}"
            ),
            "next_step": "Use options quality as a stock decision feature; do not block stock trades because option leg PnL is missing.",
            "priority": 3,
        },
        {
            "area": "Broad A-share radar",
            "state": limit_guard.get("profit_state") or "out_of_scope_for_ai_infra",
            "allowed_now": limit_guard.get("max_auto_size") or "0R",
            "evidence": f"top-decile lift={fmt_num(limit_perf.get('avg_top_decile_lift'))}, avg next-ret {fmt_pct(limit_perf.get('avg_next_ret_pct'))}",
            "blocker": "not part of the AI-infra mandate; does not block AI-infra sleeve sizing",
            "next_step": "Keep it in a separate broad A-share research report if the strategy is needed later.",
            "priority": 4,
        },
        {
            "area": "Live execution ledger",
            "state": "cn_overlay_ready" if cn_book.get("status") == "ok" else "missing_for_cn_and_partial_for_us",
            "allowed_now": "manual review",
            "evidence": (
                f"My Book overlay status={my_book.get('status')}; CN Book status={cn_book.get('status')}; "
                f"CN live-ready={cn_book_summary.get('manual_micro_probe_ready', '-')}; "
                f"long alpha R={fmt_num((overlay.get('summary') or {}).get('long_alpha_r'), 4)}; "
                f"beta hedge R={fmt_num((overlay.get('summary') or {}).get('beta_hedge_r'), 4)}"
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
        if row["area"] in {"CN main alpha", "US stock trade"}
        and _allowed_now_has_money(row.get("allowed_now"))
        and "not_ready" not in str(row.get("state") or "")
    ]
    return {
        "as_of": payload["as_of"],
        "summary": {
            "money_ready_lines": len(money_ready),
            "highest_priority_blocker": rows[0]["blocker"],
            "today_bias": (
                "US stock trade only; CN has no current execution-sleeve member; broad-market diagnostics stay out of AI-infra sizing"
                if cn_ea_count <= 0
                else "CN execution sleeve plus US stock trade; broad-market diagnostics stay out of AI-infra sizing"
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
    cn_event_rows = [row for row in cn_ranker_rows if row.get("production_tier") == "event_risk_watch"]
    us_event_rows = [row for row in us_ranker_rows if row.get("production_tier") == "event_risk_watch"]
    cn_source_rows = ((payload.get("source_review_calendar") or {}).get("cn") or {}).get("rows") or []
    ready_source_rows = [row for row in cn_source_rows if row.get("readiness_tier") == "ready_for_promotion"]
    rows = [
        {
            "priority": 1,
            "area": "CN AI-infra production sleeve",
            "state": "fail_no_cn_ai_infra_execution" if cn_ea <= 0 else "pass_cn_ai_infra_execution",
            "evidence": (
                f"current_total={len(cn_current)}, current_EA={cn_ea}, "
                f"ranker_rows={len(cn_ranker_rows)}, "
                f"source_review_rows={len(cn_source_rows)}, "
                f"ready_source_rows={len(ready_source_rows)}"
            ),
            "requirement": "A-share R only comes from AI-infra names that have source-reviewed evidence and a promoted execution sleeve.",
            "next_change": "Map ready source-review CN names into the promoted AI-infra execution sleeve; broad-market limit-up data is out of scope and must not block AI-infra R.",
        },
        {
            "priority": 2,
            "area": "CN source-review promotion",
            "state": "partial_source_review_only" if ready_source_rows else "fail_no_ready_cn_source_review",
            "evidence": f"ready_for_promotion={len(ready_source_rows)}, total_cn_source_rows={len(cn_source_rows)}",
            "requirement": "Ready source-review rows still need explicit promotion into universe/relationship ledger before ranker size.",
            "next_change": "Promote verified A-share AI-infra rows with evidence cards, then let the production ranker size only those names.",
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
            "state": "pass_stock_trade_ready" if us_ea > 0 else "fail_no_us_execution_rows",
            "evidence": f"current_total={len(us_current)}, current_EA={us_ea}, event_risk_watch={len(us_event_rows)}",
            "requirement": "`rank_score/headline_risk/options_quality/production_action` must exist for US current rows.",
            "next_change": "Keep legacy HIGH/MOD rank-only unless Alpha Factory promotes it; improve missing R:R/options quality coverage.",
        },
        {
            "priority": 5,
            "area": "Options auxiliary signal",
            "state": "pass_auxiliary_not_stock_blocker",
            "evidence": (
                f"real_bid_ask={option_ledger.get('real_bid_ask_resolved_count', 0)}, "
                f"proxy={option_ledger.get('proxy_resolved_count', 0)}, "
                f"stock_proxy={option_ledger.get('stock_proxy_resolved_count', 0)}, "
                f"unresolved={option_ledger.get('unresolved_count', 0)}"
            ),
            "requirement": "Options/flow data are auxiliary stock-ranking and risk evidence, not the traded instrument.",
            "next_change": "Use options quality to score stock entries; missing option leg PnL must not block stock trades.",
        },
        {
            "priority": 6,
            "area": "Live execution ledger",
            "state": "not_applicable_no_cn_actions"
            if cn_ea <= 0
            else ("fail_missing_cn_live_fills" if "missing" in str(live_row.get("blocker") or "").lower() else "partial_live_ledger"),
            "evidence": "no CN production actions today" if cn_ea <= 0 else (live_row.get("blocker") or "-"),
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
            "production_bias": "US only today; CN stays source-review/research until AI-infra names are promoted into a production sleeve"
            if cn_ea <= 0
            else "CN and US both have execution rows; size still controlled by ranker and live ledger",
        },
        "rows": rows,
    }


# B.15: long-form wrappers → scripts/sections/long_form.py
from sections.long_form import (  # noqa: E402
    render_profit_readiness, render_pipeline_requirements_audit,
    render_portfolio_risk_overlay, render_option_shadow_ledger,
    render_ai_supercycle_evidence, render_ai_supply_chain_relationships,
    render_ai_supercycle_layer_attribution, render_ai_lab_quality_index,
    render_ai_supercycle_value_radar,
)
from sections.audits_calendars import (  # noqa: E402
    _fmt_eps, _READINESS_TIER_ORDER,
    render_profit_readiness_section, render_pipeline_requirements_audit_section,
    render_portfolio_risk_overlay_section, render_option_shadow_ledger_section,
    render_earnings_calendar_section, render_ai_book_attribution_section,
    render_benchmark_attribution_section, render_source_review_calendar_section,
)
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
    us_ea, us_pev, us_blocked = _current_summary(us.get("current") or [])
    cn_ea, cn_pev, cn_blocked = _current_summary(cn.get("current") or [])
    cn_tape_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == CN_TAPE_SLEEVE_ID
        for row in cn.get("current") or []
    )
    cn_ai_infra_ea = any(
        row.get("state") == "Execution Alpha" and row.get("alpha_sleeve_id") == CN_AI_INFRA_PRODUCTION_SLEEVE
        for row in cn.get("current") or []
    )

    cn_v2 = cn["metrics"]["v2"]
    cn_lifecycle_policy = (cn.get("lifecycle") or {}).get("policy") or {}
    cn_v2_fresh, cn_v2_days = _freshness_summary((cn.get("freshness") or {}).get("v2") or {})
    us_stock = us["metrics"].get("v2_stock_only_net") or {}
    us_stock_fresh, us_stock_days = _freshness_summary((us.get("freshness") or {}).get("v2_stock_only_net") or {})
    us_option = us["metrics"].get("v2_options_confirmed") or {}
    us_v2_fresh, us_v2_days = _freshness_summary((us.get("freshness") or {}).get("v2") or {})
    us_legacy = us["metrics"]["legacy"]
    us_legacy_fresh, us_legacy_days = _freshness_summary((us.get("freshness") or {}).get("legacy") or {})

    rows = [
        {
            "market": "CN",
            "strategy_family": (
                CN_TAPE_SLEEVE_ID if cn_tape_ea else CN_AI_INFRA_PRODUCTION_SLEEVE if cn_ai_infra_ea else "oversold_contrarian"
            ),
            "direction": (
                "AI-infra right-side tape leadership"
                if cn_tape_ea
                else "source-reviewed AI-infra production universe"
                if cn_ai_infra_ea
                else "fear/high-vol oversold reversal"
            ),
            "role": "primary",
            "tier": cn_guard.get("profit_state") or "opportunity_stock_trade",
            "max_size": cn_guard.get("max_auto_size") or "0R",
            "post_cost_lcb80_pct": None if cn_tape_ea or cn_ai_infra_ea else cn_v2.get("lcb80_pct"),
            "avg_pct": None if cn_tape_ea or cn_ai_infra_ea else cn_v2.get("avg_pct"),
            "n": cn_ea if cn_tape_ea or cn_ai_infra_ea else cn_v2.get("n"),
            "active_dates": None if cn_tape_ea or cn_ai_infra_ea else cn_v2.get("active_dates"),
            "max_drawdown_pct": None if cn_tape_ea or cn_ai_infra_ea else cn_v2.get("max_drawdown_pct"),
            "freshness_state": cn_v2_fresh,
            "freshness_days": cn_v2_days,
            "current_execution_alpha": cn_ea,
            "current_positive_ev_setup": cn_pev,
            "current_blocked": cn_blocked,
            "reason": (
                "current A-share execution is AI-infra price/flow/sector leadership; news remains a lagging risk label"
                if cn_tape_ea
                else "current A-share execution comes from the source-reviewed AI-infra production universe; price/flow ranker controls tiers and broad-market diagnostics stay out"
                if cn_ai_infra_ea
                else
                "strongest current post-cost evidence; high fear/vol is edge context, not a US-style blocker; "
                f"lifecycle best={cn_lifecycle_policy.get('best_bucket') or '-'}, max=T+{cn_lifecycle_policy.get('max_hold_days') or '-'}"
            ),
            "kill_switch": cn_guard.get("kill_switch") or "",
        },
        {
            "market": "US",
            "strategy_family": "low_core_trending_stock_only",
            "direction": "LOW/core/executable trend continuation as stock trade",
            "role": "secondary_stock_trade",
            "tier": us_guard.get("profit_state") or "opportunity_stock_trade",
            "max_size": us_guard.get("max_auto_size") or "0.10R/name; stock-only",
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
            "reason": "stock-only net bridge is positive after cost; options/flow quality is auxiliary ranking evidence",
            "kill_switch": us_guard.get("kill_switch") or "",
        },
        {
            "market": "US",
            "strategy_family": "low_core_trending_options_expression",
            "direction": "options/flow quality used to score stock entries",
            "role": "options_auxiliary",
            "tier": "auxiliary_signal_only",
            "max_size": "0R options; stock sizing controlled above",
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
            "reason": "option ledger is diagnostic only; it must not block stock trades",
            "kill_switch": "No option orders from this report; use options data only as stock decision evidence.",
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
        "secondary_stock_trade": 1,
        "secondary_probe": 1,
        "options_auxiliary": 2,
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


def _mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _std(values: list[float]) -> float | None:
    return statistics.stdev(values) if len(values) >= 2 else None


def _corr(a: list[float], b: list[float]) -> float | None:
    return hedge_lib.corr(a, b)


def _beta(asset: list[float], benchmark: list[float]) -> float | None:
    return hedge_lib.beta(asset, benchmark)


def _returns_from_closes(values: list[float]) -> list[float]:
    return hedge_lib.returns_from_closes(values)


def load_us_sectors(db_path: Path, symbols: list[str], as_of: date) -> dict[str, str]:
    if not db_path.exists() or not symbols:
        return {}
    con = _connect_ro(db_path)
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
    con = _connect_ro(db_path)
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


US_REPORT_BENCHMARKS = ("SPY", "QQQ", "SMH", "IWM", "DIA")
CN_REPORT_BENCHMARKS = ("000300.SH", "399006.SZ", "399001.SZ", "000001.SH", "000016.SH", "399905.SZ")
SATELLITE_REPORT_BENCHMARKS = ("^TWII", "^N225", "^KS11", "^AEX", "EWT", "EWJ", "EWY", "EWN")
BENCHMARK_LABELS = {
    "SPY": "SPY (S&P 500)",
    "QQQ": "QQQ (Nasdaq 100)",
    "SMH": "SMH (Semiconductors)",
    "IWM": "IWM (Russell 2000)",
    "DIA": "DIA (Dow 30)",
    "000300.SH": "000300.SH (沪深300)",
    "399006.SZ": "399006.SZ (创业板指)",
    "399001.SZ": "399001.SZ (深成指)",
    "000001.SH": "000001.SH (上证指数)",
    "000016.SH": "000016.SH (上证50)",
    "399905.SZ": "399905.SZ (中证500)",
    "^TWII": "^TWII (TAIEX 台湾加权)",
    "^N225": "^N225 (Nikkei 225 日经)",
    "^KS11": "^KS11 (KOSPI 韩国综指)",
    "^AEX": "^AEX (荷兰 AEX)",
    "EWT": "EWT (Taiwan ETF)",
    "EWJ": "EWJ (Japan ETF)",
    "EWY": "EWY (Korea ETF)",
    "EWN": "EWN (Netherlands ETF)",
}


def _load_benchmark_closes(
    db_path: Path,
    market: str,
    symbols: Iterable[str],
    as_of: date,
    lookback_days: int = 320,
) -> dict[str, list[tuple[date, float]]]:
    syms = [str(symbol).upper() for symbol in symbols if symbol]
    if not db_path.exists() or not syms:
        return {}
    if market.lower() == "us":
        table, sym_col, date_col, close_col = "prices_daily", "symbol", "date", "close"
    else:
        table, sym_col, date_col, close_col = "prices", "ts_code", "trade_date", "close"
    con = _connect_ro(db_path)
    try:
        if not table_exists(con, table):
            return {}
        start = as_of - timedelta(days=lookback_days * 2)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT {sym_col} AS symbol, {date_col} AS d, {close_col} AS close
            FROM {table}
            WHERE {date_col} >= CAST(? AS DATE)
              AND {date_col} <= CAST(? AS DATE)
              AND {sym_col} IN ({','.join('?' for _ in syms)})
              AND {close_col} IS NOT NULL
            ORDER BY symbol, d
            """,
            [start.isoformat(), as_of.isoformat(), *syms],
        )
    finally:
        con.close()
    series: dict[str, list[tuple[date, float]]] = {}
    for row in rows:
        symbol = str(row["symbol"]).upper()
        trade_date = row["d"]
        if not isinstance(trade_date, date):
            try:
                trade_date = date.fromisoformat(str(trade_date))
            except ValueError:
                continue
        series.setdefault(symbol, []).append((trade_date, float(row["close"])))
    return series


def _trailing_return_pct(closes: list[tuple[date, float]], periods: int) -> float | None:
    if not closes or len(closes) <= periods:
        return None
    latest = closes[-1][1]
    prior = closes[-1 - periods][1]
    if not prior:
        return None
    return (latest / prior - 1.0) * 100.0


def _ytd_return_pct(closes: list[tuple[date, float]], as_of: date) -> float | None:
    if not closes:
        return None
    year_start = date(as_of.year, 1, 1)
    base = next((close for d, close in closes if d >= year_start), None)
    if base is None or not base:
        return None
    latest = closes[-1][1]
    return (latest / base - 1.0) * 100.0


def _daily_returns(closes: list[tuple[date, float]]) -> dict[date, float]:
    """Convert ordered (date, close) tuples to date -> simple daily return."""
    out: dict[date, float] = {}
    for prev, cur in zip(closes, closes[1:], strict=False):
        if not prev[1] or prev[1] <= 0:
            continue
        out[cur[0]] = cur[1] / prev[1] - 1.0
    return out


def _aligned_pairs(
    series_a: dict[date, float],
    series_b: dict[date, float],
    *,
    window: int,
) -> tuple[list[float], list[float]]:
    common = sorted(set(series_a) & set(series_b))
    if not common:
        return [], []
    common = common[-window:]
    xs = [series_a[d] for d in common]
    ys = [series_b[d] for d in common]
    return xs, ys


def _compute_alpha_beta(
    book_returns: dict[date, float],
    benchmark_returns: dict[date, float],
    *,
    window: int,
) -> dict[str, float | None]:
    xs, ys = _aligned_pairs(book_returns, benchmark_returns, window=window)
    n = len(xs)
    if n < max(10, window // 4):
        return {
            "n": n,
            "alpha_daily_pct": None,
            "beta": None,
            "active_return_pct": None,
            "information_ratio": None,
        }
    mean_book = sum(xs) / n
    mean_bench = sum(ys) / n
    var_bench = sum((y - mean_bench) ** 2 for y in ys)
    cov = sum((xs[i] - mean_book) * (ys[i] - mean_bench) for i in range(n))
    beta_value: float | None = None
    alpha_daily: float | None = None
    if var_bench > 0:
        beta_value = max(-5.0, min(5.0, cov / var_bench))
        alpha_daily = mean_book - beta_value * mean_bench
    active_returns = [xs[i] - ys[i] for i in range(n)]
    mean_active = sum(active_returns) / n
    var_active = sum((r - mean_active) ** 2 for r in active_returns)
    tracking_error = (var_active / n) ** 0.5 if n > 0 else 0.0
    info_ratio: float | None = None
    if tracking_error > 0:
        info_ratio = mean_active / tracking_error
    return {
        "n": n,
        "alpha_daily_pct": round(alpha_daily * 100, 4) if alpha_daily is not None else None,
        "beta": round(beta_value, 3) if beta_value is not None else None,
        "active_return_pct": round(mean_active * 100, 4),
        "information_ratio": round(info_ratio, 3) if info_ratio is not None else None,
    }


def _max_drawdown_pct(returns: list[float]) -> float | None:
    if not returns:
        return None
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for r in returns:
        equity *= 1.0 + r
        peak = max(peak, equity)
        if peak > 0:
            worst = min(worst, equity / peak - 1.0)
    return round(worst * 100.0, 3)


def _atr_proxy(closes: list[tuple[date, float]], window: int) -> float | None:
    if len(closes) < window + 1:
        return None
    ranges: list[float] = []
    for prev, cur in zip(closes[-window - 1:], closes[-window:], strict=False):
        prev_close = prev[1]
        cur_close = cur[1]
        if not prev_close:
            continue
        ranges.append(abs(cur_close - prev_close) / prev_close * 100.0)
    if not ranges:
        return None
    return round(sum(ranges) / len(ranges), 3)


def build_us_stock_trade_plan(
    db_path: Path,
    symbols: Iterable[str],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    """Mechanical stock entry/stop/target fallback for US production rows.

    The US theme cluster sleeve already uses a simple stock plan:
    entry = latest close, stop = -6%, target = +10%. The AI-infra production
    ranker can promote a name without carrying those price fields, so the final
    report fills them from the same local convention rather than printing
    `stop -; target -`.
    """
    clean_symbols = sorted({_symbol_key(symbol) for symbol in symbols if _symbol_key(symbol)})
    series = _load_benchmark_closes(db_path, "us", clean_symbols, as_of, lookback_days=120)
    plans: dict[str, dict[str, Any]] = {}
    for symbol in clean_symbols:
        closes = series.get(symbol) or []
        if not closes:
            plans[symbol] = {
                "status": "missing_price",
                "entry": None,
                "stop": None,
                "target": None,
                "latest_date": None,
                "rule": "missing US prices_daily close; no mechanical stock plan",
            }
            continue
        latest_date, entry = closes[-1]
        plans[symbol] = {
            "status": "ok",
            "entry": round_or_none(entry, 4),
            "stop": round_or_none(entry * 0.94, 4),
            "target": round_or_none(entry * 1.10, 4),
            "latest_date": latest_date.isoformat(),
            "atr20_pct": _atr_proxy(closes, window=20),
                "rule": "entry=latest close; stop=-6%; target=+10%; next-session review; no mechanical 3D-5D hold",
            }
    return plans


def _pairwise_corr(series_by_symbol: dict[str, dict[date, float]], window: int) -> dict[str, float | None]:
    symbols = list(series_by_symbol)
    if len(symbols) < 2:
        return {"mean": None, "max": None, "min": None, "n_pairs": 0}
    import math

    values: list[float] = []
    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            a = series_by_symbol[symbols[i]]
            b = series_by_symbol[symbols[j]]
            common = sorted(set(a) & set(b))
            common = common[-window:]
            n = len(common)
            if n < max(10, window // 4):
                continue
            xs = [a[d] for d in common]
            ys = [b[d] for d in common]
            mx = sum(xs) / n
            my = sum(ys) / n
            vx = sum((x - mx) ** 2 for x in xs)
            vy = sum((y - my) ** 2 for y in ys)
            if vx <= 0 or vy <= 0:
                continue
            cov = sum((xs[k] - mx) * (ys[k] - my) for k in range(n))
            values.append(max(-1.0, min(1.0, cov / math.sqrt(vx * vy))))
    if not values:
        return {"mean": None, "max": None, "min": None, "n_pairs": 0}
    return {
        "mean": round(sum(values) / len(values), 3),
        "max": round(max(values), 3),
        "min": round(min(values), 3),
        "n_pairs": len(values),
    }


def _ai_book_return_series(
    db_path: Path,
    market: str,
    basket_symbols: list[str],
    as_of: date,
) -> dict[date, float]:
    """Equal-weight average of daily simple returns across basket symbols."""
    series = _load_benchmark_closes(db_path, market, basket_symbols, as_of, lookback_days=180)
    per_symbol_returns: list[dict[date, float]] = []
    for symbol, closes in series.items():
        if not closes:
            continue
        per_symbol_returns.append(_daily_returns(closes))
    if not per_symbol_returns:
        return {}
    common_dates: set[date] = set()
    for returns in per_symbol_returns:
        if not common_dates:
            common_dates = set(returns)
        else:
            common_dates &= set(returns)
    if not common_dates:
        # Fallback: union with NaN handling — average over symbols that have a quote.
        union_dates = set().union(*[set(r) for r in per_symbol_returns])
        averaged: dict[date, float] = {}
        for d in sorted(union_dates):
            values = [r[d] for r in per_symbol_returns if d in r]
            if values:
                averaged[d] = sum(values) / len(values)
        return averaged
    averaged: dict[date, float] = {}
    for d in sorted(common_dates):
        values = [returns[d] for returns in per_symbol_returns]
        averaged[d] = sum(values) / len(values)
    return averaged


def build_benchmark_attribution(
    us_db: Path,
    cn_db: Path,
    as_of: date,
    *,
    us_basket: list[str] | None = None,
    cn_basket: list[str] | None = None,
) -> dict[str, Any]:
    """Snapshot trailing returns for canonical US/CN benchmarks plus AI book attribution.

    When `us_basket` / `cn_basket` are provided, also compute the equal-weight
    AI book daily-return series and report rolling alpha/beta/IR vs each
    benchmark over 20- and 60-day windows.
    """
    out: dict[str, Any] = {"as_of": as_of.isoformat(), "us": {}, "cn": {}, "satellite": {}}

    for market, db_path, symbols, key in (
        ("us", us_db, US_REPORT_BENCHMARKS, "us"),
        ("cn", cn_db, CN_REPORT_BENCHMARKS, "cn"),
        # Satellite indices live in the US DuckDB (yfinance-sourced just like SPY/QQQ).
        ("us", us_db, SATELLITE_REPORT_BENCHMARKS, "satellite"),
    ):
        series = _load_benchmark_closes(db_path, market, symbols, as_of)
        rows: list[dict[str, Any]] = []
        missing: list[str] = []
        for symbol in symbols:
            closes = series.get(symbol)
            if not closes:
                missing.append(symbol)
                rows.append(
                    {
                        "symbol": symbol,
                        "label": BENCHMARK_LABELS.get(symbol, symbol),
                        "status": "missing_data",
                        "latest_close": None,
                        "latest_date": None,
                        "ret_1d_pct": None,
                        "ret_5d_pct": None,
                        "ret_20d_pct": None,
                        "ret_60d_pct": None,
                        "ret_ytd_pct": None,
                    }
                )
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "label": BENCHMARK_LABELS.get(symbol, symbol),
                    "status": "ok",
                    "latest_close": round(closes[-1][1], 4),
                    "latest_date": closes[-1][0].isoformat(),
                    "ret_1d_pct": _trailing_return_pct(closes, 1),
                    "ret_5d_pct": _trailing_return_pct(closes, 5),
                    "ret_20d_pct": _trailing_return_pct(closes, 20),
                    "ret_60d_pct": _trailing_return_pct(closes, 60),
                    "ret_ytd_pct": _ytd_return_pct(closes, as_of),
                }
            )
        out[key] = {
            "status": "ok" if rows else "missing_db",
            "rows": rows,
            "missing": missing,
        }

    # AI book attribution: alpha/beta/IR vs each US/CN benchmark.
    out["ai_book"] = {}
    for market_key, db_path, basket, benchmark_symbols in (
        ("us", us_db, us_basket or [], US_REPORT_BENCHMARKS),
        ("cn", cn_db, cn_basket or [], CN_REPORT_BENCHMARKS),
    ):
        if not basket:
            out["ai_book"][market_key] = {"status": "no_basket", "rows": [], "basket_size": 0}
            continue
        book_returns = _ai_book_return_series(db_path, market_key, basket, as_of)
        if not book_returns:
            out["ai_book"][market_key] = {
                "status": "missing_data",
                "rows": [],
                "basket_size": len(basket),
                "basket_symbols": basket,
            }
            continue
        bench_series = _load_benchmark_closes(db_path, market_key, list(benchmark_symbols), as_of, lookback_days=180)
        rows: list[dict[str, Any]] = []
        for symbol in benchmark_symbols:
            closes = bench_series.get(symbol)
            if not closes:
                continue
            bench_returns = _daily_returns(closes)
            for window_label, window_days in (("20d", 20), ("60d", 60)):
                metrics = _compute_alpha_beta(book_returns, bench_returns, window=window_days)
                rows.append(
                    {
                        "benchmark": symbol,
                        "benchmark_label": BENCHMARK_LABELS.get(symbol, symbol),
                        "window": window_label,
                        "n": metrics["n"],
                        "active_return_pct": metrics["active_return_pct"],
                        "alpha_daily_pct": metrics["alpha_daily_pct"],
                        "beta": metrics["beta"],
                        "information_ratio": metrics["information_ratio"],
                    }
                )
        # Risk block: max drawdown / ATR / pairwise correlation across basket.
        sorted_dates = sorted(book_returns)
        book_returns_20 = [book_returns[d] for d in sorted_dates[-20:]]
        book_returns_60 = [book_returns[d] for d in sorted_dates[-60:]]
        per_symbol_series_full = _load_benchmark_closes(db_path, market_key, basket, as_of, lookback_days=120)
        per_symbol_returns: dict[str, dict[date, float]] = {}
        atr_inputs: list[tuple[str, float | None]] = []
        for symbol in basket:
            closes = per_symbol_series_full.get(symbol) or []
            if closes:
                per_symbol_returns[symbol] = _daily_returns(closes)
                atr_inputs.append((symbol, _atr_proxy(closes, window=20)))
        atr_values = [val for _, val in atr_inputs if val is not None]
        risk = {
            "max_drawdown_20d_pct": _max_drawdown_pct(book_returns_20),
            "max_drawdown_60d_pct": _max_drawdown_pct(book_returns_60),
            "avg_atr20_pct": round(sum(atr_values) / len(atr_values), 3) if atr_values else None,
            "pairwise_corr_20d": _pairwise_corr(per_symbol_returns, window=20),
            "pairwise_corr_60d": _pairwise_corr(per_symbol_returns, window=60),
        }
        out["ai_book"][market_key] = {
            "status": "ok" if rows else "missing_benchmark",
            "rows": rows,
            "basket_size": len(basket),
            "basket_symbols": basket,
            "risk": risk,
        }
    return out


def load_cn_future_return_series(db_path: Path, symbols: Iterable[str], as_of: date, lookback: int = 90) -> dict[str, list[float]]:
    clean_symbols = [str(symbol).upper() for symbol in symbols if symbol]
    if not db_path.exists() or not clean_symbols:
        return {}
    con = _connect_ro(db_path)
    try:
        if not table_exists(con, "fut_daily"):
            return {}
        start = as_of - timedelta(days=lookback * 3)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT ts_code AS symbol, trade_date AS d, close
            FROM fut_daily
            WHERE trade_date >= CAST(? AS DATE)
              AND trade_date <= CAST(? AS DATE)
              AND ts_code IN ({','.join('?' for _ in clean_symbols)})
              AND close IS NOT NULL
            ORDER BY symbol, d
            """,
            [start.isoformat(), as_of.isoformat(), *clean_symbols],
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
    con = _connect_ro(db_path)
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


def _select_beta_hedge(
    market: str,
    asset_returns: list[float],
    benchmarks: dict[str, list[float]],
) -> tuple[str, float | None, float | None]:
    return hedge_lib.select_beta_hedge(market, asset_returns, benchmarks)


def _portfolio_var95_proxy(exposures: dict[str, float], return_lookup: dict[str, list[float]]) -> float:
    if not exposures:
        return 0.0
    items = list(exposures.items())
    variance = 0.0
    for left_key, left_r in items:
        for right_key, right_r in items:
            if left_key == right_key:
                corr_value = 1.0
            else:
                corr_value = _corr(return_lookup.get(left_key, []), return_lookup.get(right_key, []))
                if corr_value is None:
                    corr_value = 0.20
            variance += float(left_r) * float(right_r) * corr_value
    return 1.65 * math.sqrt(max(variance, 0.0))


def _scale_overlay_rows(
    rows: list[dict[str, Any]],
    *,
    cap: float,
    reason: str,
    market: str | None = None,
) -> None:
    eligible = [
        row
        for row in rows
        if (market is None or str(row.get("market") or "").upper() == market)
        and float(row.get("final_r") or 0.0) > 0.0
    ]
    total = sum(float(row.get("final_r") or 0.0) for row in eligible)
    if total <= cap or total <= 0.0:
        return
    scale = cap / total
    for row in eligible:
        row["final_r"] = float(row.get("final_r") or 0.0) * scale
        row.setdefault("risk_reasons", []).append(reason)


CAPITULATION_RADAR_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "capitulation_radar"


def load_capitulation_payload(as_of: str) -> dict[str, Any] | None:
    path = CAPITULATION_RADAR_ROOT / as_of / "capitulation_radar.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


CN_RISK_REGIME_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "cn_risk_regime"


def load_cn_risk_regime_payload(as_of: str) -> dict[str, Any]:
    """CN-native regime artifact (score_cn_risk_regime.py). Missing → hedge 1.0x."""
    path = CN_RISK_REGIME_ROOT / as_of / "cn_risk_regime.json"
    if not path.exists():
        return {"state": "hedge", "r_multiplier": 1.0, "new_adds_allowed": True,
                "hedge_directive": "—", "victim_action": "—",
                "rationale": "cn_risk_regime 工件缺失，CN gate 退化为 1.0x。",
                "signals": {}, "artifact_missing": True}
    try:
        out = json.loads(path.read_text(encoding="utf-8"))
        out["artifact_missing"] = False
        return out
    except (OSError, json.JSONDecodeError):
        return {"state": "hedge", "r_multiplier": 1.0, "new_adds_allowed": True,
                "hedge_directive": "—", "victim_action": "—",
                "rationale": "cn_risk_regime 工件损坏，CN gate 退化为 1.0x。",
                "signals": {}, "artifact_missing": True}


def build_risk_regime(
    bubble_hedge: dict[str, Any] | None,
    capitulation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify the Hedge/Wedge/Confirm/Press/Capitulation regime.

    Computed inline (not via cron artifact) so the gate is always fresh for the
    emailed report regardless of when score_risk_regime_engine.py last ran.
    """
    if not bubble_hedge:
        # bubble_hedge missing → classify with empty wedge/confirmation, but
        # still honour a present capitulation payload (a capitulation day must
        # not be misreported as plain hedge just because bubble_hedge is late).
        decision = classify_risk_regime([], {}, [], capitulation=capitulation)
        out = decision.as_dict()
        out["artifact_missing"] = True
        if decision.state == "hedge":
            out["rationale"] = (
                "bubble_hedge 工件缺失，风控 gate 退化为 1.0x（不缩减）。"
            )
        return out
    decision = classify_risk_regime(
        bubble_hedge.get("wedge") or [],
        bubble_hedge.get("confirmation") or {},
        bubble_hedge.get("victims") or [],
        capitulation=capitulation,
    )
    out = decision.as_dict()
    out["artifact_missing"] = False
    return out


def build_portfolio_risk_overlay(
    us: dict[str, Any],
    cn: dict[str, Any],
    limit_up: dict[str, Any],
    profit_guardrails: list[dict[str, Any]],
    us_db: Path,
    cn_db: Path,
    as_of: date,
    risk_regime: dict[str, Any] | None = None,
    cn_risk_regime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    us_guard = _guardrail_by_market(profit_guardrails, "US")
    cn_guard = _guardrail_by_market(profit_guardrails, "CN")
    cn_profit_state = str(cn_guard.get("profit_state") or "")
    cn_allows_auto = cn_profit_state in {"stock_trade", "observed_lifecycle_trade", "opportunity_stock_trade"}
    rows: list[dict[str, Any]] = []

    for row in cn.get("current") or []:
        if row.get("state") not in {"Execution Alpha", "Positive EV Setup"}:
            continue
        production_tier = str(row.get("production_tier") or "")
        if production_tier == "observed_lifecycle_trade":
            base_r = 0.14
        elif production_tier == "observed_lifecycle_secondary_trade":
            base_r = 0.10
        elif production_tier == "secondary_stock_trade":
            base_r = 0.15
        elif production_tier == "top_stock_trade":
            base_r = 0.35
        else:
            base_r = 0.35 if row.get("state") == "Execution Alpha" else 0.15
        risk_reasons: list[str] = []
        if production_tier.startswith("observed_lifecycle"):
            risk_reasons.append("observed_lifecycle_stock_trade")
        if not cn_allows_auto:
            base_r = min(base_r, 0.10)
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
    us_allows_stock_trade = (
        us_profit_state.startswith("conditional")
        or us_profit_state.startswith("tradeable")
        or us_profit_state in {"stock_trade", "opportunity_stock_trade"}
    )
    for row in us.get("current") or []:
        if row.get("state") not in {"Execution Alpha", "Positive EV Setup"}:
            continue
        base_r = 0.125 if row.get("state") == "Execution Alpha" else 0.05
        risk_reasons = []
        risk_reasons.append("us_horizon_cap_next_session_review")
        if not us_allows_stock_trade:
            base_r = min(base_r, 0.05)
            risk_reasons.append(f"profit_guardrail_{us_profit_state or 'missing'}")
        strategy_family = row.get("policy")
        if str(strategy_family or "") == US_THEME_SLEEVE_ID:
            base_r = min(base_r, US_SINGLE_NAME_R_CAP)
            risk_reasons.append("theme_momentum_3d5d_decay_cap")
        rows.append(
            {
                "key": f"US:{row.get('symbol')}",
                "market": "US",
                "symbol": row.get("symbol"),
                "name": row.get("name") or "",
                "state": row.get("state"),
                "strategy_family": strategy_family,
                "sector": "Unknown",
                "time_exit": row.get("time_exit") or US_DEFAULT_TIME_EXIT,
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

    _scale_overlay_rows(rows, cap=CN_BASKET_R_CAP, market="CN", reason="cn_basket_cap_scaled")
    _scale_overlay_rows(rows, cap=US_BASKET_R_CAP, market="US", reason="us_basket_cap_scaled")
    _scale_overlay_rows(rows, cap=PORTFOLIO_TOTAL_R_CAP, reason="total_portfolio_cap_scaled")

    # Hedge/Wedge/Confirm/Press gate — scale the book AFTER the basket caps
    # (the cap re-scaler would otherwise normalise the gate away). Each market
    # is gated by its OWN regime: US rows by the US engine, CN rows by the
    # CN-native engine (CN A-shares run their own tape/flow cycle — only the
    # rates wedge is shared).
    regime_state = str((risk_regime or {}).get("state") or "hedge")
    regime_mult = float((risk_regime or {}).get("r_multiplier", 1.0))
    cn_regime_state = str((cn_risk_regime or {}).get("state") or "hedge")
    cn_regime_mult = float((cn_risk_regime or {}).get("r_multiplier", 1.0))
    for row in rows:
        is_cn = str(row.get("market") or "").upper() == "CN"
        mult = cn_regime_mult if is_cn else regime_mult
        state = cn_regime_state if is_cn else regime_state
        if mult != 1.0:
            row["base_r"] = float(row["base_r"]) * mult
            row["final_r"] = float(row["final_r"]) * mult
            row["auto_eligible"] = row["final_r"] > 0.0
            label = "cn_risk_regime" if is_cn else "risk_regime"
            row["risk_reasons"].append(f"{label}_{state}_x{mult:.2f}")

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
    us_benchmarks = load_return_series(us_db, "us", list(US_HEDGE_BENCHMARKS), as_of)
    cn_benchmarks = load_cn_future_return_series(cn_db, CN_HEDGE_BENCHMARKS, as_of)
    benchmark_lookup = {"US": us_benchmarks, "CN": cn_benchmarks}
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

    hedge_book: dict[str, dict[str, Any]] = {}
    return_lookup = dict(returns)
    exposures: dict[str, float] = {}
    hedge_exposures: dict[str, float] = {}
    for row in rows:
        final_r = float(row.get("final_r") or 0.0)
        exposures[row["key"]] = final_r
        market = str(row.get("market") or "").upper()
        benchmarks = benchmark_lookup.get(market) or {}
        instrument, beta, beta_corr = _select_beta_hedge(market, returns.get(row["key"], []), benchmarks)
        hedge_ratio = CN_BETA_HEDGE_RATIO if market == "CN" else US_BETA_HEDGE_RATIO
        beta_floor = CN_MARKET_BETA_FLOOR if market == "CN" else US_MARKET_BETA_FLOOR
        beta_for_size = 1.0 if beta is None else max(max(beta, 0.0), beta_floor)
        hedge_notional_r = min(final_r * 0.90, final_r * beta_for_size * hedge_ratio) if final_r > 0 else 0.0
        row["hedge_instrument"] = instrument
        row["hedge_direction"] = "short_index_future" if market == "CN" else "short_index_etf"
        row["hedge_beta"] = round_or_none(beta_for_size, 4)
        row["hedge_beta_corr"] = round_or_none(beta_corr, 4)
        if beta is None:
            row["hedge_beta_source"] = "fallback_beta_1"
        elif max(beta, 0.0) < beta_floor:
            row["hedge_beta_source"] = "market_beta_floor"
        else:
            row["hedge_beta_source"] = "return_beta"
        row["hedge_notional_r"] = round_or_none(hedge_notional_r, 4)
        row["net_beta_r"] = round_or_none(max(final_r - hedge_notional_r, 0.0), 4)
        if hedge_notional_r > 0:
            row["risk_reasons"].append(f"beta_hedge_{instrument}")
            hedge_key = f"{market}:HEDGE:{instrument}"
            hedge_exposures[hedge_key] = hedge_exposures.get(hedge_key, 0.0) + hedge_notional_r
            if instrument in benchmarks:
                return_lookup[hedge_key] = benchmarks[instrument]
            book_key = f"{market}:{instrument}"
            bucket = hedge_book.setdefault(
                book_key,
                {
                    "market": market,
                    "instrument": instrument,
                    "direction": row["hedge_direction"],
                    "hedge_notional_r": 0.0,
                    "names": 0,
                },
            )
            bucket["hedge_notional_r"] += hedge_notional_r
            bucket["names"] += 1
    for hedge_key, hedge_r in hedge_exposures.items():
        exposures[hedge_key] = -hedge_r

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
    hedged_var95 = _portfolio_var95_proxy(exposures, return_lookup)
    var_scale = 1.0
    if var95 > PORTFOLIO_VAR95_R_CAP and var95 > 0:
        for row in rows:
            row["risk_reasons"].append("var95_cap_warning")

    sector_exposure: dict[str, float] = {}
    cluster_exposure: dict[int, float] = {}
    for row in rows:
        sector_key = f"{row.get('market')}:{row.get('sector') or 'Unknown'}"
        sector_exposure[sector_key] = sector_exposure.get(sector_key, 0.0) + float(row.get("final_r") or 0.0)
        cluster_id = row.get("corr_cluster_id")
        if cluster_id is not None:
            cluster_exposure[int(cluster_id)] = cluster_exposure.get(int(cluster_id), 0.0) + float(row.get("final_r") or 0.0)
    beta_hedge_r = sum(float(row.get("hedge_notional_r") or 0.0) for row in rows)
    net_beta_r = sum(float(row.get("net_beta_r") or 0.0) for row in rows)
    hedge_rows = [
        {
            **bucket,
            "hedge_notional_r": round_or_none(float(bucket.get("hedge_notional_r") or 0.0), 4),
        }
        for bucket in sorted(hedge_book.values(), key=lambda item: (str(item.get("market")), str(item.get("instrument"))))
    ]
    risk_attribution = {
        "single_name_max_r": round_or_none(max((float(row.get("final_r") or 0.0) for row in rows), default=0.0), 4),
        "sector_max_r": round_or_none(max(sector_exposure.values(), default=0.0), 4),
        "correlation_cluster_max_r": round_or_none(max(cluster_exposure.values(), default=0.0), 4),
        "long_alpha_r": round_or_none(gross_r, 4),
        "beta_hedge_r": round_or_none(beta_hedge_r, 4),
        "net_beta_r": round_or_none(net_beta_r, 4),
        "idiosyncratic_alpha_r": round_or_none(max(gross_r - beta_hedge_r, 0.0), 4),
        "hedge_offset_r": round_or_none(beta_hedge_r, 4),
        "basis_risk_delta_r": round_or_none(max(hedged_var95 - var95, 0.0), 4),
    }

    for row in rows:
        row["base_r"] = round_or_none(row["base_r"], 4)
        row["final_r"] = round_or_none(row["final_r"], 4)
        row["hedge_notional_r"] = round_or_none(row.get("hedge_notional_r"), 4)
        row["net_beta_r"] = round_or_none(row.get("net_beta_r"), 4)
        row["risk_reasons"] = row["risk_reasons"] or ["pass"]

    return {
        "as_of": as_of.isoformat(),
        "rows": rows,
        "summary": {
            "candidate_count": len(rows),
            "gross_r_after_caps": round_or_none(sum(float(row["final_r"] or 0.0) for row in rows), 4),
            "long_alpha_r": round_or_none(gross_r, 4),
            "beta_hedge_r": round_or_none(beta_hedge_r, 4),
            "net_beta_r": round_or_none(net_beta_r, 4),
            "var95_r_proxy": round_or_none(var95, 4),
            "hedged_var95_r_proxy": round_or_none(hedged_var95, 4),
            "hedge_basis_risk": bool(hedged_var95 > var95),
            "var_scale": round_or_none(var_scale, 4),
            "sector_cap_r": SECTOR_R_CAP,
            "corr_cluster_cap_r": CORR_CLUSTER_R_CAP,
            "total_cap_r": PORTFOLIO_TOTAL_R_CAP,
            "limit_up_budget_r": 0.0 if limit_up.get("current") else 0.0,
            "hedge_book": hedge_rows,
            "risk_attribution": risk_attribution,
            "risk_regime_state": regime_state,
            "risk_regime_multiplier": regime_mult,
            "cn_risk_regime_state": cn_regime_state,
            "cn_risk_regime_multiplier": cn_regime_mult,
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
    option_type: str,
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
          AND option_type = ?
          AND bid IS NOT NULL
          AND ask IS NOT NULL
          AND bid > 0
          AND ask > 0
        ORDER BY ABS(strike - ?) ASC
        LIMIT 1
        """,
        [symbol, as_of, expiry, option_type, target_strike],
    )
    return rows[0] if rows else None


def _option_spread_from_quotes(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    entry_date: str,
    exit_date: str,
    expiry: str,
    entry_price: float,
    expression: str,
) -> tuple[float | None, dict[str, Any]]:
    if expression == "call_spread":
        option_type = "call"
        long_target = entry_price
        short_target = entry_price * 1.05
    elif expression == "put_spread":
        option_type = "put"
        long_target = entry_price
        short_target = entry_price * 0.95
    else:
        return None, {"reason": "unsupported_real_option_expression"}

    long_entry = _quote_leg(con, symbol, entry_date, expiry, option_type, long_target)
    short_entry = _quote_leg(con, symbol, entry_date, expiry, option_type, short_target)
    if not long_entry or not short_entry:
        return None, {"reason": "missing_entry_leg_quotes"}
    long_exit = _quote_leg(con, symbol, exit_date, expiry, option_type, float(long_entry["strike"]))
    short_exit = _quote_leg(con, symbol, exit_date, expiry, option_type, float(short_entry["strike"]))
    if not long_exit or not short_exit:
        return None, {"reason": "missing_exit_leg_quotes"}
    entry_debit = float(long_entry["ask"]) - float(short_entry["bid"])
    exit_value = float(long_exit["bid"]) - float(short_exit["ask"])
    if entry_debit <= 0:
        return None, {"reason": "non_positive_entry_debit"}
    commission_pct = (2.0 * OPTION_COMMISSION_PER_LEG) / (entry_debit * OPTION_CONTRACT_MULTIPLIER) * 100.0
    ret = (exit_value - entry_debit) / entry_debit * 100.0 - commission_pct
    legs = [
        {
            "leg_role": "long",
            "side": "buy_to_open_sell_to_close",
            "option_type": option_type,
            "expiry": expiry,
            "strike": long_entry["strike"],
            "entry_contract": long_entry.get("contract_symbol"),
            "exit_contract": long_exit.get("contract_symbol"),
            "entry_bid": round_or_none(long_entry.get("bid"), 4),
            "entry_ask": round_or_none(long_entry.get("ask"), 4),
            "exit_bid": round_or_none(long_exit.get("bid"), 4),
            "exit_ask": round_or_none(long_exit.get("ask"), 4),
            "entry_mark": round_or_none(long_entry.get("ask"), 4),
            "exit_mark": round_or_none(long_exit.get("bid"), 4),
        },
        {
            "leg_role": "short",
            "side": "sell_to_open_buy_to_close",
            "option_type": option_type,
            "expiry": expiry,
            "strike": short_entry["strike"],
            "entry_contract": short_entry.get("contract_symbol"),
            "exit_contract": short_exit.get("contract_symbol"),
            "entry_bid": round_or_none(short_entry.get("bid"), 4),
            "entry_ask": round_or_none(short_entry.get("ask"), 4),
            "exit_bid": round_or_none(short_exit.get("bid"), 4),
            "exit_ask": round_or_none(short_exit.get("ask"), 4),
            "entry_mark": round_or_none(short_entry.get("bid"), 4),
            "exit_mark": round_or_none(short_exit.get("ask"), 4),
        },
    ]
    return ret, {
        "pricing_mode": "leg_quotes",
        "option_type": option_type,
        "expiry": expiry,
        "long_strike": long_entry["strike"],
        "short_strike": short_entry["strike"],
        "entry_debit": round_or_none(entry_debit, 4),
        "exit_value": round_or_none(exit_value, 4),
        "commission_pct": round_or_none(commission_pct, 4),
        "legs": legs,
    }


def _option_exit_quote_date(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    entry_date: str,
    as_of: date,
    *,
    sessions: int = 3,
) -> str | None:
    if not table_exists(con, "options_chain_quotes"):
        return None
    rows = con.execute(
        """
        SELECT DISTINCT as_of
        FROM options_chain_quotes
        WHERE symbol = ?
          AND as_of > CAST(? AS DATE)
          AND as_of <= CAST(? AS DATE)
        ORDER BY as_of
        """,
        [symbol, entry_date, as_of.isoformat()],
    ).fetchall()
    if not rows:
        return None
    idx = min(max(sessions - 1, 0), len(rows) - 1)
    return as_iso(rows[idx][0])


def build_real_option_bidask_ledger(
    con: duckdb.DuckDBPyConnection,
    start: date,
    as_of: date,
) -> list[dict[str, Any]]:
    if not table_exists(con, "options_alpha") or not table_exists(con, "options_chain_quotes"):
        return []
    alpha_rows = rows_as_dicts(
        con,
        """
        SELECT symbol, as_of, expression, detail_json
        FROM options_alpha
        WHERE as_of >= CAST(? AS DATE)
          AND as_of <= CAST(? AS DATE)
          AND expression IN ('call_spread', 'put_spread')
        ORDER BY as_of, symbol
        """,
        [start.isoformat(), as_of.isoformat()],
    )
    ledger: list[dict[str, Any]] = []
    for alpha in alpha_rows:
        symbol = str(alpha.get("symbol") or "").upper()
        entry_date = as_iso(alpha.get("as_of")) or ""
        expression = str(alpha.get("expression") or "")
        detail = safe_json_loads(alpha.get("detail_json"))
        expiry = str(detail.get("expiry") or "") or None
        entry_price = round_or_none(detail.get("current_price")) or 0.0
        exit_date = _option_exit_quote_date(con, symbol, entry_date, as_of)
        row = {
            "source": "options_alpha_all",
            "report_date": entry_date,
            "evaluation_date": exit_date,
            "symbol": symbol,
            "expression": expression,
            "pricing_mode": "unresolved",
            "resolved": False,
            "real_bid_ask_resolved": False,
            "return_pct": None,
            "reason": "",
            "detail": {},
            "legs": [],
        }
        if not expiry:
            row["reason"] = "missing_expiry"
        elif not exit_date:
            row["reason"] = "missing_future_exit_quote_date"
        elif entry_price <= 0:
            row["reason"] = "missing_entry_underlying_price"
        else:
            ret, quote_detail = _option_spread_from_quotes(
                con,
                symbol,
                entry_date,
                exit_date,
                expiry,
                entry_price,
                expression,
            )
            if ret is None:
                row["reason"] = str(quote_detail.get("reason") or "missing_real_bid_ask_leg_quotes")
                row["detail"] = quote_detail
            else:
                row.update(
                    {
                        "pricing_mode": "leg_quotes",
                        "resolved": True,
                        "real_bid_ask_resolved": True,
                        "return_pct": ret,
                        "reason": f"{expression} marked from real bid/ask legs",
                        "detail": quote_detail,
                        "legs": quote_detail.get("legs") or [],
                    }
                )
        ledger.append(row)
    return ledger


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
    all_real_bid_ask_ledger: list[dict[str, Any]] = []
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
                "real_bid_ask_resolved": False,
                "long_expression": expression in {"call_spread", "stock_long"},
                "return_pct": None,
                "reason": "",
                "detail": {},
                "legs": [],
            }
            if expression == "stock_long" and ret is not None:
                ledger_row.update(
                    {
                        "pricing_mode": "stock_long",
                        "resolved": True,
                        "real_bid_ask_resolved": False,
                        "return_pct": ret - US_STOCK_ROUNDTRIP_COST_PCT,
                        "reason": "stock_long net after roundtrip cost",
                        "detail": {"roundtrip_cost_pct": US_STOCK_ROUNDTRIP_COST_PCT},
                    }
                )
            elif expression in {"call_spread", "put_spread"} and ret is not None and entry_price > 0 and con is not None:
                quote_ret, quote_detail = (None, {"reason": "missing_chain_quote_table"})
                if expiry:
                    quote_ret, quote_detail = _option_spread_from_quotes(
                        con,
                        symbol,
                        entry_date,
                        exit_date,
                        expiry,
                        entry_price,
                        expression,
                    )
                if quote_ret is not None:
                    ledger_row.update(
                        {
                            "pricing_mode": "leg_quotes",
                            "resolved": True,
                            "real_bid_ask_resolved": True,
                            "return_pct": quote_ret,
                            "reason": f"{expression} marked from bid/ask leg quotes",
                            "detail": quote_detail,
                            "legs": quote_detail.get("legs") or [],
                        }
                    )
                elif expression == "call_spread":
                    proxy_ret, proxy_detail = _call_spread_proxy(con, symbol, entry_date, exit_date, expiry, entry_price, ret)
                    if proxy_ret is not None:
                        ledger_row.update(
                            {
                                "pricing_mode": "proxy_bs",
                                "resolved": True,
                                "real_bid_ask_resolved": False,
                                "return_pct": proxy_ret,
                                "reason": "call spread proxy from options_analysis IV and spread",
                                "detail": {"quote_attempt": quote_detail, **proxy_detail},
                            }
                        )
                    else:
                        ledger_row["reason"] = str(proxy_detail.get("reason") or quote_detail.get("reason"))
                        ledger_row["detail"] = {"quote_attempt": quote_detail, "proxy_attempt": proxy_detail}
                else:
                    ledger_row["reason"] = str(quote_detail.get("reason") or "missing_real_bid_ask_leg_quotes")
                    ledger_row["detail"] = {"quote_attempt": quote_detail}
            else:
                ledger_row["reason"] = "non-long expression or missing options alpha"
            ledger.append(ledger_row)
        if con is not None:
            all_real_bid_ask_ledger = build_real_option_bidask_ledger(con, start, as_of)
    finally:
        if con is not None:
            con.close()

    resolved_long = [
        {"report_date": row["report_date"], "return_pct": row["return_pct"]}
        for row in ledger
        if row.get("resolved") and row.get("long_expression") and row.get("return_pct") is not None
    ]
    real_bid_ask_rows = [
        {"report_date": row["report_date"], "return_pct": row["return_pct"]}
        for row in ledger
        if row.get("real_bid_ask_resolved") and row.get("return_pct") is not None
    ]
    summary = {
        "overall_long": compute_metrics("US option shadow long expressions", resolved_long).to_dict(),
        "real_bid_ask_options": compute_metrics("US real bid/ask option spread ledger", real_bid_ask_rows).to_dict(),
        "all_options_alpha_real_bid_ask": compute_metrics(
            "US options_alpha all real bid/ask spreads",
            [
                {"report_date": row["report_date"], "return_pct": row["return_pct"]}
                for row in all_real_bid_ask_ledger
                if row.get("real_bid_ask_resolved") and row.get("return_pct") is not None
            ],
        ).to_dict(),
    }
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
        "real_bid_ask_rows": all_real_bid_ask_ledger,
        "summary": summary,
        "resolved_count": sum(1 for row in ledger if row.get("resolved")),
        "real_bid_ask_resolved_count": sum(1 for row in ledger if row.get("real_bid_ask_resolved")),
        "all_real_bid_ask_resolved_count": sum(1 for row in all_real_bid_ask_ledger if row.get("real_bid_ask_resolved")),
        "all_real_bid_ask_unresolved_count": sum(1 for row in all_real_bid_ask_ledger if not row.get("real_bid_ask_resolved")),
        "proxy_resolved_count": sum(1 for row in ledger if row.get("pricing_mode") == "proxy_bs"),
        "stock_proxy_resolved_count": sum(1 for row in ledger if row.get("pricing_mode") == "stock_long"),
        "unresolved_count": sum(1 for row in ledger if not row.get("resolved")),
        "unresolved_by_reason": dict(Counter(str(row.get("reason") or "unknown") for row in ledger if not row.get("resolved"))),
        "rows_with_legs": sum(1 for row in ledger if row.get("legs")) + sum(1 for row in all_real_bid_ask_ledger if row.get("legs")),
    }


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
        f"- Mode: `{MAIN_STRATEGY_MODE}`. `{CN_ALPHA_FACTORY_EXECUTION_SLEEVE}` and `{CN_OBSERVED_LIFECYCLE_SLEEVE}` can produce stock-trade execution rows.",
        "- Broad oversold rows stay in ranked watch; fear/high-vol and no-chase tune entry style only after sleeve/probability membership exists.",
        "- Weak/non-sleeve rows cannot bypass the production ranker into new money entries.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


# B.12: rankers/sector/lab/attribution sections → scripts/sections/rankers.py
from sections.rankers import (  # noqa: E402
    render_us_opportunity_ranker_section, render_cn_opportunity_ranker_section,
    render_cn_sector_narrative_section, render_ai_lab_quality_index_section,
    render_ai_supercycle_layer_attribution_section,
)
def _evidence_state_score(state: str, raw_score: Any = None) -> float:
    if state == "negative_supply_evidence":
        return 0.08
    base = {
        "source_linked_supply_evidence": 0.90,
        "theme_news_only": 0.58,
        "price_flow_first_no_current_news": 0.48,
        "lagging_news_risk_label": 0.42,
        "needs_primary_confirmation": 0.16,
        "missing_recent_news": 0.0,
    }.get(state, 0.20)
    parsed = round_or_none(raw_score)
    if parsed is None:
        return base
    return max(base, min(1.0, parsed))


def build_ai_supply_chain_relationships(
    path: Path = DEFAULT_AI_SUPPLY_CHAIN_RELATIONSHIPS,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "missing",
            "data_required": str(path),
            "summary": {"rows": 0, "source_linked": 0, "official_sources": 0},
            "rows": [],
        }
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows: list[dict[str, Any]] = []
    for item in raw.get("relationships") or []:
        if not isinstance(item, dict):
            continue
        symbols = sorted(
            {
                str(symbol or "").upper()
                for symbol in [
                    item.get("primary_symbol"),
                    item.get("counterparty_symbol"),
                    item.get("customer_symbol"),
                    *(item.get("symbols") or []),
                ]
                if str(symbol or "").strip()
            }
        )
        source_url = str(item.get("source_url") or "").strip()
        source_type = str(item.get("source_type") or "").strip()
        confidence = str(item.get("confidence") or "").lower()
        source_linked = bool(source_url and source_type and confidence in {"high", "medium"})
        rows.append(
            {
                "relationship_id": item.get("relationship_id"),
                "as_of": as_iso(item.get("as_of")),
                "market": str(item.get("market") or "").upper() or "US",
                "primary_symbol": str(item.get("primary_symbol") or "").upper(),
                "counterparty_symbol": str(item.get("counterparty_symbol") or "").upper(),
                "customer_symbol": str(item.get("customer_symbol") or "").upper(),
                "symbols": symbols,
                "layer": item.get("layer"),
                "relationship_type": item.get("relationship_type"),
                "supply_chain_role": item.get("supply_chain_role"),
                "bottleneck_focus": item.get("bottleneck_focus"),
                "source_name": item.get("source_name"),
                "source_type": source_type,
                "source_url": source_url,
                "source_date": as_iso(item.get("source_date")),
                "confidence": confidence or "unknown",
                "evidence_state": "source_linked_relationship" if source_linked else "invalid_missing_source",
                "source_linked": source_linked,
                "notes": item.get("notes"),
            }
        )
    return {
        "status": "loaded",
        "source_file": str(path),
        "contract": raw.get("contract")
        or "Only source-linked relationships may be used as supply-chain confirmation.",
        "summary": {
            "rows": len(rows),
            "source_linked": sum(1 for row in rows if row.get("source_linked")),
            "official_sources": sum(
                1 for row in rows if str(row.get("source_type") or "").startswith("official")
            ),
        },
        "rows": rows,
    }


def ai_relationship_lookup(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for row in (payload.get("ai_supply_chain_relationships") or {}).get("rows") or []:
        if not row.get("source_linked"):
            continue
        for symbol in row.get("symbols") or []:
            key = str(symbol or "").upper()
            if key:
                lookup.setdefault(key, []).append(row)
    return lookup


def _layer_priority(layer: str) -> int:
    order = {
        "ai_labs_cloud_models": 1,
        "ai_compute_accelerators": 2,
        "ai_memory_storage": 3,
        "ai_networking_optical_cpo": 3,
        "ai_datacenter_edge_infra": 4,
        "ai_chip_equipment_materials_packaging": 5,
        "ai_power_nuclear_grid": 6,
        "ai_power_grid": 6,
        "space_connectivity_datacenter": 7,
        "hard_assets_energy_heavy": 8,
    }
    return order.get(layer, 9)


def build_ai_supercycle_evidence_ledger(payload: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    relationship_lookup = ai_relationship_lookup(payload)
    seen_us_symbols: set[str] = set()
    for row in (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []:
        layer = str(row.get("supercycle_layer") or "").strip()
        if not layer:
            continue
        symbol = str(row.get("symbol") or "").upper()
        seen_us_symbols.add(symbol)
        relationships = relationship_lookup.get(symbol, [])
        state = str(row.get("supplier_evidence_state") or "missing_recent_news")
        relationship_text = ""
        relationship_source = None
        relationship_date = None
        relationship_url = None
        if relationships:
            rel = relationships[0]
            relationship_source = rel.get("source_name")
            relationship_date = rel.get("source_date")
            relationship_url = rel.get("source_url")
            relationship_text = (
                f"{rel.get('relationship_type') or 'AI supply-chain relationship'}: "
                f"{rel.get('supply_chain_role') or rel.get('bottleneck_focus') or ''}"
            )
            if state != "negative_supply_evidence":
                state = "source_linked_supply_evidence"
        rows.append(
            {
                "market": "US",
                "symbol": row.get("symbol"),
                "name": row.get("name") or "",
                "layer": layer,
                "priority": row.get("supercycle_priority"),
                "supply_chain_role": row.get("supply_chain_role"),
                "bottleneck_focus": row.get("bottleneck_focus"),
                "evidence_state": state,
                "evidence_score": round_or_none(_evidence_state_score(state, row.get("ai_evidence_score")), 4),
                "evidence_source": relationship_source or row.get("ai_evidence_source"),
                "evidence_date": relationship_date or row.get("ai_evidence_date"),
                "evidence_text": relationship_text
                or row.get("ai_evidence_text")
                or row.get("ai_evidence_headline")
                or row.get("latest_headline"),
                "evidence_url": relationship_url or row.get("ai_evidence_url"),
                "relationship_evidence_count": len(relationships),
                "relationship_sources": [
                    rel.get("source_name") or rel.get("source_url") for rel in relationships[:4]
                ],
                "candidate_tier": row.get("production_tier"),
                "action": row.get("production_action"),
                "rank": row.get("rank"),
                "rank_score": row.get("rank_score"),
                "price_flow_summary": f"joint {fmt_num(row.get('joint_signal_score'), 0)}, options/flow {fmt_num(row.get('flow_options_quality'), 0)}",
                "contract_note": "Supplier/customer relationship is confirmed only when evidence_state=source_linked_supply_evidence and the text is company-specific.",
            }
        )
    for rel in (payload.get("ai_supply_chain_relationships") or {}).get("rows") or []:
        if not rel.get("source_linked") or str(rel.get("market") or "").upper() != "US":
            continue
        symbol = str(rel.get("primary_symbol") or "").upper()
        if not symbol or symbol in seen_us_symbols:
            continue
        layer = str(rel.get("layer") or "").strip()
        rows.append(
            {
                "market": "US",
                "symbol": symbol,
                "name": "",
                "layer": layer,
                "priority": _layer_priority(layer),
                "supply_chain_role": rel.get("supply_chain_role"),
                "bottleneck_focus": rel.get("bottleneck_focus"),
                "evidence_state": "source_linked_supply_evidence",
                "evidence_score": 0.90,
                "evidence_source": rel.get("source_name"),
                "evidence_date": rel.get("source_date"),
                "evidence_text": (
                    f"{rel.get('relationship_type') or 'AI supply-chain relationship'}: "
                    f"{rel.get('supply_chain_role') or rel.get('bottleneck_focus') or ''}"
                ),
                "evidence_url": rel.get("source_url"),
                "relationship_evidence_count": 1,
                "relationship_sources": [rel.get("source_name") or rel.get("source_url")],
                "candidate_tier": "relationship_research_seed",
                "action": "research_only_no_trade",
                "rank": None,
                "rank_score": None,
                "price_flow_summary": "source-linked relationship seed; no current tape/ranker confirmation",
                "contract_note": "Relationship-only rows feed long-term research radar only and cannot create production R.",
            }
        )
    for row in (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or []:
        layer = str(row.get("supercycle_layer") or "").strip()
        if not layer or layer in {"neutral", "excluded_consumer"}:
            continue
        latest = row.get("latest_headline")
        state = "lagging_news_risk_label" if latest else "price_flow_first_no_current_news"
        rows.append(
            {
                "market": "CN",
                "symbol": row.get("symbol"),
                "name": row.get("name") or "",
                "layer": layer,
                "priority": row.get("supercycle_priority"),
                "supply_chain_role": row.get("supply_chain_role"),
                "bottleneck_focus": row.get("bottleneck_focus"),
                "evidence_state": state,
                "evidence_score": round_or_none(_evidence_state_score(state), 4),
                "evidence_source": "price/flow/tape" if not latest else "lagging_news",
                "evidence_date": row.get("latest_headline_date"),
                "evidence_text": latest
                or (
                    f"price {fmt_num(row.get('price_first_signal_score'), 0)}, "
                    f"flow {fmt_num(row.get('informed_flow_score'), 0)}, "
                    f"5D {fmt_pct(row.get('ret_5d'))}, industry {row.get('industry') or '-'}"
                ),
                "evidence_url": None,
                "candidate_tier": row.get("production_tier"),
                "action": row.get("production_action"),
                "rank": row.get("rank"),
                "rank_score": row.get("rank_score"),
                "price_flow_summary": (
                    f"price {fmt_num(row.get('price_first_signal_score'), 0)}, "
                    f"flow {fmt_num(row.get('informed_flow_score'), 0)}, "
                    f"tape {fmt_num(row.get('tape_score'), 2)}"
                ),
                "contract_note": "A-share news is lagging evidence; supplier links need announcements/filings before final narrative.",
            }
        )
    rows.sort(
        key=lambda item: (
            int(item.get("priority") or 9),
            -(round_or_none(item.get("evidence_score")) or 0.0),
            -(round_or_none(item.get("rank_score")) or 0.0),
            str(item.get("market") or ""),
            str(item.get("symbol") or ""),
        )
    )
    by_layer: dict[str, dict[str, Any]] = {}
    for row in rows:
        layer = str(row.get("layer") or "-")
        bucket = by_layer.setdefault(
            layer,
            {"layer": layer, "rows": 0, "source_linked": 0, "theme_or_price": 0, "missing": 0},
        )
        bucket["rows"] += 1
        state = str(row.get("evidence_state") or "")
        if state == "source_linked_supply_evidence":
            bucket["source_linked"] += 1
        elif state in {"theme_news_only", "price_flow_first_no_current_news", "lagging_news_risk_label"}:
            bucket["theme_or_price"] += 1
        else:
            bucket["missing"] += 1
    return {
        "as_of": payload.get("as_of"),
        "generated_at": payload.get("generated_at"),
        "summary": {
            "rows": len(rows),
            "source_linked": sum(1 for row in rows if row.get("evidence_state") == "source_linked_supply_evidence"),
            "needs_primary_confirmation": sum(
                1 for row in rows if row.get("evidence_state") in {"needs_primary_confirmation", "missing_recent_news"}
            ),
            "layers": sorted(by_layer.values(), key=lambda item: (-item["source_linked"], -item["rows"], item["layer"])),
        },
        "rows": rows,
    }


# B.9: ai_supercycle evidence/supply/value sections moved to scripts/sections/ai_supercycle.py
from sections.ai_supercycle import (  # noqa: E402
    render_ai_supercycle_evidence_section,
    render_ai_supply_chain_relationships_section,
    render_ai_supercycle_value_radar_section,
)
def _layer_metrics_rows(
    rows: list[dict[str, Any]],
    *,
    market: str,
    source: str,
    layer_key: str,
    label_key: str | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        layer = str(row.get(layer_key) or "").strip()
        if not layer or layer in {"neutral", "excluded_consumer"}:
            continue
        grouped.setdefault(layer, []).append(row)
    out: list[dict[str, Any]] = []
    for layer, layer_rows in grouped.items():
        metrics = compute_metrics(f"{market} {layer}", layer_rows).to_dict()
        labels = sorted({str(row.get(label_key) or "").strip() for row in layer_rows if label_key and row.get(label_key)})
        full_confirm = sum(1 for row in layer_rows if row.get("confirm_quality") == "full_confirm")
        proxy_confirm = sum(1 for row in layer_rows if row.get("confirm_quality") in {"proxy_confirm", "price_volume_proxy"})
        out.append(
            {
                "market": market,
                "layer": layer,
                "source": source,
                "labels": labels[:8],
                "n": metrics.get("n"),
                "active_dates": metrics.get("active_dates"),
                "avg_pct": metrics.get("avg_pct"),
                "median_pct": metrics.get("median_pct"),
                "win_rate": metrics.get("win_rate"),
                "lcb80_pct": metrics.get("lcb80_pct"),
                "lcb95_pct": metrics.get("lcb95_pct"),
                "trade_sharpe": metrics.get("trade_sharpe"),
                "daily_sharpe": metrics.get("daily_sharpe"),
                "full_confirm": full_confirm,
                "proxy_confirm": proxy_confirm,
                "sample_note": "Layer attribution is historical sleeve evidence, not same-day trade permission.",
            }
        )
    out.sort(
        key=lambda row: (
            -(round_or_none(row.get("lcb80_pct")) or -999.0),
            -(int(row.get("n") or 0)),
            str(row.get("market")),
            str(row.get("layer")),
        )
    )
    return out


def build_ai_supercycle_layer_attribution(us_db: Path, cn_db: Path, start: date, as_of: date) -> dict[str, Any]:
    us_rows, us_status = query_us_theme_cluster_returns(us_db, start, as_of, DEFAULT_US_THEME_SEED_MAP)
    cn_rows, cn_status = query_cn_tape_leadership_returns(cn_db, start, as_of)
    rows: list[dict[str, Any]] = []
    rows.extend(
        _layer_metrics_rows(
            us_rows,
            market="US",
            source=US_THEME_SLEEVE_ID,
            layer_key="supercycle_layer",
            label_key="theme_label",
        )
    )
    rows.extend(
        _layer_metrics_rows(
            cn_rows,
            market="CN",
            source=CN_TAPE_SLEEVE_ID,
            layer_key="supercycle_layer",
            label_key="industry",
        )
    )
    return {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": {"US": us_status, "CN": cn_status},
        "summary": {
            "rows": len(rows),
            "positive_lcb80": sum(1 for row in rows if (round_or_none(row.get("lcb80_pct")) or -999.0) > 0),
            "markets": sorted({row.get("market") for row in rows}),
            "contract": "Historical attribution by AI-supercycle layer; promotion still requires sleeve gates and current evidence.",
        },
        "rows": rows,
    }


def _load_ai_lab_publication_counts(publication_path: Path) -> dict[str, dict[str, Any]]:
    if not publication_path.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with publication_path.open("r", encoding="utf-8", newline="") as handle:
        for raw in csv.DictReader(handle):
            symbol = str(raw.get("symbol") or "").strip().upper()
            conference = str(raw.get("conference") or "").strip()
            if not symbol or not conference:
                continue
            accepted = int(round_or_none(raw.get("accepted_count"), 0) or 0)
            oral = int(round_or_none(raw.get("oral_spotlight_count"), 0) or 0)
            year = str(raw.get("year") or "").strip()
            bucket = out.setdefault(
                symbol,
                {"publication_counts": {}, "oral_spotlight_count": 0, "sources": set(), "years": set()},
            )
            bucket["publication_counts"][conference] = bucket["publication_counts"].get(conference, 0) + max(0, accepted)
            bucket["oral_spotlight_count"] += max(0, oral)
            if raw.get("source"):
                bucket["sources"].add(str(raw.get("source")).strip())
            if year:
                bucket["years"].add(year)
    for bucket in out.values():
        bucket["sources"] = sorted(bucket["sources"])
        bucket["years"] = sorted(bucket["years"])
    return out


def build_ai_lab_quality_index(
    seed_path: Path = DEFAULT_AI_LAB_QUALITY_SEED,
    publication_path: Path = DEFAULT_AI_LAB_PUBLICATIONS,
) -> dict[str, Any]:
    if not seed_path.exists():
        return {
            "status": "missing_seed",
            "seed_path": str(seed_path),
            "conference_scope": ["NeurIPS", "ICML", "ICLR", "CVPR"],
            "rows": [],
        }
    payload = yaml.safe_load(seed_path.read_text(encoding="utf-8")) or {}
    publication_counts = _load_ai_lab_publication_counts(publication_path)
    rows: list[dict[str, Any]] = []
    for item in payload.get("companies") or []:
        symbol = str(item.get("symbol") or "").strip().upper()
        loaded = publication_counts.get(symbol) or {}
        counts = loaded.get("publication_counts") or item.get("publication_counts") or {}
        total_count = sum(int(value or 0) for value in counts.values()) if isinstance(counts, dict) else 0
        oral_count = int(loaded.get("oral_spotlight_count") or 0)
        data_status = "scored" if total_count else str(item.get("data_status") or "data_required")
        rows.append(
            {
                "symbol": symbol or item.get("symbol"),
                "company": item.get("company"),
                "labs": item.get("labs") or [],
                "stack_aliases": item.get("stack_aliases") or [],
                "supercycle_layer": item.get("supercycle_layer"),
                "conference_scope": payload.get("conference_scope") or ["NeurIPS", "ICML", "ICLR", "CVPR"],
                "publication_counts": counts,
                "oral_spotlight_count": oral_count,
                "publication_years": loaded.get("years") or [],
                "publication_sources": loaded.get("sources") or [],
                "paper_count_total": total_count if total_count else None,
                "lab_quality_score": None
                if total_count == 0
                else round_or_none(min(100.0, math.log1p(total_count) * 18.0 + oral_count * 2.0), 4),
                "data_status": data_status,
                "data_requirement": (
                    "Load accepted-paper affiliation data for NeurIPS/ICML/ICLR/CVPR and map author affiliations to labs."
                    if data_status == "data_required"
                    else ""
                ),
            }
        )
    return {
        "status": "data_required" if any(row.get("data_status") == "data_required" for row in rows) else "scored",
        "seed_path": str(seed_path),
        "publication_path": str(publication_path),
        "publication_dataset_loaded": bool(publication_counts),
        "conference_scope": payload.get("conference_scope") or ["NeurIPS", "ICML", "ICLR", "CVPR"],
        "index_formula": payload.get("index_formula"),
        "rows": rows,
    }


def _load_us_company_profiles(us_db: Path, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not us_db.exists():
        return {}
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        if not table_exists(con, "company_profile"):
            return {}
        rows = rows_as_dicts(
            con,
            f"""
            WITH latest AS (
                SELECT symbol, MAX(as_of) AS as_of
                FROM company_profile
                WHERE as_of <= CAST(? AS DATE)
                  AND symbol IN ({placeholders(symbols)})
                GROUP BY symbol
            )
            SELECT c.*
            FROM company_profile c
            JOIN latest l ON l.symbol = c.symbol AND l.as_of = c.as_of
            """,
            [as_of.isoformat(), *symbols],
        )
        return {str(row.get("symbol") or "").upper(): row for row in rows}
    finally:
        con.close()


def _size_optionality_score(market: str, market_cap: Any) -> tuple[float, str]:
    cap = round_or_none(market_cap)
    if cap is None or cap <= 0:
        return 0.35, "market_cap_missing"
    if market.upper() == "CN":
        # TuShare total_mv/circ_mv are in RMB 10k. Convert to RMB bn.
        cap_bn = cap / 100000.0
        if cap_bn <= 5:
            return 1.0, f"{fmt_num(cap_bn, 1)}bn RMB micro/small cap"
        if cap_bn <= 15:
            return 0.82, f"{fmt_num(cap_bn, 1)}bn RMB small/mid cap"
        if cap_bn <= 50:
            return 0.58, f"{fmt_num(cap_bn, 1)}bn RMB mid cap"
        return 0.24, f"{fmt_num(cap_bn, 1)}bn RMB large cap"
    # US company_profile market_cap is stored in USD mn in this dataset.
    cap_bn = cap / 1000.0
    if cap_bn <= 2.5:
        return 1.0, f"{fmt_num(cap_bn, 1)}bn USD micro/small cap"
    if cap_bn <= 10:
        return 0.84, f"{fmt_num(cap_bn, 1)}bn USD small/mid cap"
    if cap_bn <= 50:
        return 0.58, f"{fmt_num(cap_bn, 1)}bn USD mid cap"
    if cap_bn <= 200:
        return 0.36, f"{fmt_num(cap_bn, 1)}bn USD large cap"
    return 0.14, f"{fmt_num(cap_bn, 1)}bn USD mega cap"


def _growth_score(value: Any) -> tuple[float, str]:
    growth = round_or_none(value)
    if growth is None:
        return 0.35, "growth_missing"
    if growth >= 80:
        return 1.0, f"revenue_growth {fmt_pct(growth)}"
    if growth >= 35:
        return 0.82, f"revenue_growth {fmt_pct(growth)}"
    if growth >= 15:
        return 0.60, f"revenue_growth {fmt_pct(growth)}"
    if growth >= 0:
        return 0.38, f"revenue_growth {fmt_pct(growth)}"
    return 0.12, f"revenue_growth {fmt_pct(growth)}"


def _layer_bottleneck_score(layer: str) -> float:
    if layer in {"ai_networking_optical_cpo", "ai_chip_equipment_materials_packaging", "ai_power_grid", "ai_power_nuclear_grid"}:
        return 1.0
    if layer in {"ai_memory_storage", "ai_compute_accelerators", "ai_datacenter_edge_infra"}:
        return 0.85
    if layer in {"space_connectivity_datacenter", "hard_assets_energy_heavy", "ai_electronics_components"}:
        return 0.68
    return 0.40


def _priority_score(value: Any) -> float:
    try:
        priority = int(value)
    except (TypeError, ValueError):
        priority = 5
    return max(0.0, 1.0 - (max(1, priority) - 1) / 8.0)


def build_ai_supercycle_value_radar(payload: dict[str, Any], us_db: Path, as_of: date) -> dict[str, Any]:
    evidence_rows = (payload.get("ai_supercycle_evidence_ledger") or {}).get("rows") or []
    us_symbols = sorted({str(row.get("symbol") or "").upper() for row in evidence_rows if row.get("market") == "US"})
    us_profiles = _load_us_company_profiles(us_db, us_symbols, as_of)
    cn_rows = {
        str(row.get("symbol") or "").upper(): row
        for row in (payload.get("cn_opportunity_ranker") or {}).get("all_rows") or []
        if row.get("symbol")
    }
    us_ranker = {
        str(row.get("symbol") or "").upper(): row
        for row in (payload.get("us_opportunity_ranker") or {}).get("all_rows") or []
        if row.get("symbol")
    }
    lab_lookup = {
        str(row.get("symbol") or "").upper(): row
        for row in (payload.get("ai_lab_quality_index") or {}).get("rows") or []
        if row.get("symbol")
    }
    radar_rows: list[dict[str, Any]] = []
    for row in evidence_rows:
        symbol = str(row.get("symbol") or "").upper()
        market = str(row.get("market") or "").upper()
        if not symbol or market not in {"US", "CN"}:
            continue
        layer = str(row.get("layer") or "")
        evidence_score = round_or_none(row.get("evidence_score")) or 0.0
        rank_score = (round_or_none(row.get("rank_score")) or 0.0) / 100.0
        profile = us_profiles.get(symbol) if market == "US" else cn_rows.get(symbol, {})
        market_cap = None
        valuation_bits: list[str] = []
        if market == "US":
            market_cap = (profile or {}).get("market_cap")
            growth, growth_reason = _growth_score((profile or {}).get("revenue_growth"))
            for key in ["pe_ttm", "ps_ratio", "pb_ratio"]:
                if (profile or {}).get(key) is not None:
                    valuation_bits.append(f"{key}={fmt_num((profile or {}).get(key), 2)}")
        else:
            market_cap = (profile or {}).get("total_mv") or (profile or {}).get("circ_mv")
            growth = max(0.35, rank_score)
            growth_reason = "CN growth data missing; use price/flow rank as proxy only"
            for key in ["pe_ttm", "pb"]:
                if (profile or {}).get(key) is not None:
                    valuation_bits.append(f"{key}={fmt_num((profile or {}).get(key), 2)}")
        size_score, size_reason = _size_optionality_score(market, market_cap)
        bottleneck_score = _layer_bottleneck_score(layer)
        lab_row = lab_lookup.get(symbol) if market == "US" else None
        lab_quality_score = round_or_none((lab_row or {}).get("lab_quality_score"))
        lab_score = max(0.0, min((lab_quality_score or 0.0) / 100.0, 1.0))
        score = (
            0.19 * _priority_score(row.get("priority"))
            + 0.20 * evidence_score
            + 0.18 * bottleneck_score
            + 0.16 * size_score
            + 0.09 * growth
            + 0.07 * rank_score
            + 0.11 * lab_score
        )
        score = round_or_none(score * 100.0, 2)
        evidence_state = str(row.get("evidence_state") or "")
        if evidence_state == "negative_supply_evidence":
            research_priority = "avoid_until_resolved"
        elif market == "US" and evidence_state != "source_linked_supply_evidence":
            research_priority = "evidence_first"
        elif evidence_state in {"missing_recent_news", "needs_primary_confirmation"}:
            research_priority = "evidence_first"
        elif (score or 0) >= 75 and size_score >= 0.55:
            research_priority = "deep_dive_now"
        elif (score or 0) >= 62:
            research_priority = "watchlist_deep_dive"
        else:
            research_priority = "theme_watch"
        blockers = []
        if evidence_state == "negative_supply_evidence":
            blockers.append("negative supply/order evidence; wait for primary-source resolution")
        if evidence_state != "source_linked_supply_evidence" and market == "US":
            blockers.append("needs source-linked company-specific evidence")
        if market == "CN":
            blockers.append("supplier/customer relation not confirmed; A-share tape only")
        if market == "US" and layer == "ai_labs_cloud_models" and lab_quality_score is None:
            blockers.append("top-conference lab publication dataset missing")
        if not valuation_bits:
            blockers.append("valuation fields missing")
        radar_rows.append(
            {
                "market": market,
                "symbol": symbol,
                "name": row.get("name") or (profile or {}).get("company_name") or "",
                "layer": layer,
                "priority": row.get("priority"),
                "value_radar_score": score,
                "research_priority": research_priority,
                "supply_chain_role": row.get("supply_chain_role"),
                "bottleneck_focus": row.get("bottleneck_focus"),
                "evidence_state": evidence_state,
                "evidence_score": row.get("evidence_score"),
                "market_cap": round_or_none(market_cap, 4),
                "size_optionality": round_or_none(size_score, 4),
                "size_reason": size_reason,
                "growth_score": round_or_none(growth, 4),
                "growth_reason": growth_reason,
                "lab_quality_score": lab_quality_score,
                "lab_quality_status": (lab_row or {}).get("data_status"),
                "lab_quality_sources": (lab_row or {}).get("publication_sources") or [],
                "valuation_snapshot": ", ".join(valuation_bits) if valuation_bits else "valuation_missing",
                "rank_score": row.get("rank_score"),
                "evidence_text": row.get("evidence_text"),
                "blockers": blockers,
                "next_due_diligence": (
                    "Verify supplier/customer link, revenue exposure, customer concentration, gross margin durability, capex cycle, dilution risk and valuation."
                ),
                "contract_note": "This is a long-term 10x research radar, not a trading order or same-day R signal.",
            }
        )
    radar_rows.sort(
        key=lambda item: (
            {
                "deep_dive_now": 0,
                "watchlist_deep_dive": 1,
                "evidence_first": 2,
                "theme_watch": 3,
                "avoid_until_resolved": 4,
            }.get(
                str(item.get("research_priority")), 9
            ),
            -(round_or_none(item.get("value_radar_score")) or 0.0),
            str(item.get("market")),
            str(item.get("symbol")),
        )
    )
    return {
        "as_of": payload.get("as_of"),
        "generated_at": payload.get("generated_at"),
        "summary": {
            "rows": len(radar_rows),
            "deep_dive_now": sum(1 for row in radar_rows if row.get("research_priority") == "deep_dive_now"),
            "evidence_first": sum(1 for row in radar_rows if row.get("research_priority") == "evidence_first"),
            "avoid_until_resolved": sum(
                1 for row in radar_rows if row.get("research_priority") == "avoid_until_resolved"
            ),
            "contract": "10x radar ranks research priority only; it cannot create production R.",
        },
        "rows": radar_rows,
    }


def market_actions(payload: dict[str, Any], market: str) -> list[dict[str, Any]]:
    decision = payload.get("production_decision_summary") or build_production_decision_summary(payload)
    return [
        row
        for row in decision.get("actionable") or []
        if str(row.get("market") or "").upper() == market.upper()
    ]


def market_watch_rows(payload: dict[str, Any], market: str) -> list[dict[str, Any]]:
    decision = payload.get("production_decision_summary") or build_production_decision_summary(payload)
    return [
        row
        for row in decision.get("watch") or []
        if str(row.get("market") or "").upper() == market.upper()
    ]


def calendar_focus_symbols(*groups: Iterable[dict[str, Any]]) -> set[str]:
    symbols: set[str] = set()
    for group in groups:
        for row in group or []:
            symbol = str(row.get("symbol") or row.get("ts_code") or "").strip().upper()
            if symbol:
                symbols.add(symbol)
    return symbols


def _evidence_state_for_action(row: dict[str, Any]) -> str:
    """Return a short evidence-state badge for the production action table.

    Sources (in priority order): explicit `evidence_state` on the action row,
    the AI-infra universe `ai_infra_evidence_state` (set by the universe gate),
    or fallback to `unknown` so an operator can immediately see when the row
    lacks source-review evidence and treat it as research-only sizing.
    """
    state = str(row.get("evidence_state") or row.get("ai_infra_evidence_state") or "").strip()
    verif = str(row.get("ai_infra_verification_status") or row.get("verification_status") or "").strip()
    # An entry can read "合理推论" (which passes the pool gate) yet still be a
    # placeholder manual queue-unlock whose AI exposure was never source-verified
    # (verification_status=pending_original_source_verification / evidence text
    # like "操作员从 queue 批量补回 ... 解锁"). Don't let it masquerade as a clean
    # pick — append a ⚠ so the table (and the narrator that reads it) treats it
    # as research-grade, not source-confirmed.
    pending = (
        verif == "pending_original_source_verification"
        or "queue 批量补回" in state
        or "queue ingest" in state
        or ("操作员" in state and "解锁" in state)
    )
    if not state:
        return "unknown"
    if "原文已证明" in state:
        base = "原文已证明"
    elif "合理推论" in state:
        base = "合理推论"
    elif "待原文核验" in state or "待核验" in state:
        base = "待原文核验"
    else:
        base = state[:24]
    if pending and base not in {"待原文核验", "unknown"}:
        return f"{base} ⚠待核验"
    return base


def render_market_action_table(actions: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Symbol | Action | 凸性 | Size | Entry | Evidence | Risk / Exit | Hedge | Trigger |",
        "|---|---|---|---:|---|---|---|---|---|",
    ]
    if not actions:
        lines.append("| - | no trade | - | 0R | - | - | - | - | no current execution candidate |")
        lines.append("")
        return lines
    for row in actions:
        hedge = f"{row.get('hedge') or '-'} {fmt_num(row.get('hedge_notional_r'), 4)}R"
        market = str(row.get("market") or "")
        convexity = _CONVEXITY_SHORT.get(
            str(row.get("convexity") or classify_convexity(row.get("action"))), "-"
        )
        action_cell = action_label(row.get('action')) + (row.get('position_delta_text') or '')
        lines.append(
            f"| {row.get('symbol')} {row.get('name') or ''} | "
            f"{action_cell} | {convexity} | {fmt_r(row.get('size_r'))} | "
            f"{clean_table_text(row.get('entry'), 60)} | "
            f"{_evidence_state_for_action(row)} | "
            f"{clean_table_text(human_risk_plan(row.get('risk_plan')), 90)} | "
            f"{hedge} | {clean_table_text(human_trigger_text(market, row), 110)} |"
        )
    lines.append("")
    return lines


from sections.gamma_spring import build_gamma_spring_snapshot, render_gamma_spring_section  # noqa: E402
from sections.iv_view import _iv_action_hint, render_iv_view_section  # noqa: E402

# B.5: left-side sections moved to scripts/sections/left_side.py
from sections.left_side import (  # noqa: E402
    REGIME_TILT_TABLE, regime_left_right_tilt, render_regime_tilt_header,
    cn_left_side_watch_rows, render_cn_left_side_watch_section,
    render_us_left_side_section as _render_us_left_side_section_impl,
)
US_MEAN_REVERSION_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_mean_reversion_radar"

def render_us_left_side_section(payload, *, limit=None):
    return _render_us_left_side_section_impl(
        payload, us_mean_reversion_root=US_MEAN_REVERSION_ROOT, limit=limit,
    )
# B.8: cn_probability_picks moved to scripts/sections/cn_probability_picks.py
from sections.cn_probability_picks import (  # noqa: E402
    _pick_probability_cn_stock,
    render_cn_probability_picks_section as _render_cn_probability_picks_section_impl,
)

def render_cn_probability_picks_section(payload):
    return _render_cn_probability_picks_section_impl(
        payload, actions=market_actions(payload, 'CN'),
    )
# C.1: render_cn_standalone_report moved to scripts/reports/cn_daily.py
from reports.cn_daily import render_cn_standalone_report  # noqa: E402,F401


# B.0: serenity (re-added — was accidentally removed during B.2/B.3 surgery)
from sections.serenity import build_serenity_crosscheck, render_serenity_crosscheck_section  # noqa: E402

# B.2: probability_picks moved to scripts/sections/probability_picks.py
from sections.probability_picks import render_us_probability_picks_section  # noqa: E402

# B.3: top10_daily moved to scripts/sections/top10_daily.py
from sections.top10_daily import render_us_top10_daily_section  # noqa: E402

# C.0: render_us_standalone_report moved to scripts/reports/us_daily.py
from reports.us_daily import render_us_standalone_report  # noqa: E402,F401


# C.2: render_report moved to scripts/reports/combined.py
from reports.combined import render_report  # noqa: E402,F401
# C.2 part 2: render_factorlab_brief moved to scripts/reports/factorlab.py
from reports.factorlab import render_factorlab_brief  # noqa: E402,F401


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS cn_opportunity_ranker")
        con.execute("DROP TABLE IF EXISTS us_opportunity_ranker")
        con.execute("DROP TABLE IF EXISTS pipeline_requirements_audit")
        con.execute("DROP TABLE IF EXISTS portfolio_risk_overlay")
        con.execute("DROP TABLE IF EXISTS ai_supercycle_evidence")
        con.execute("DROP TABLE IF EXISTS ai_lab_quality_index")
        con.execute("DROP TABLE IF EXISTS ai_supercycle_value_radar")
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
                final_r DOUBLE, hedge_instrument VARCHAR, hedge_notional_r DOUBLE,
                hedge_beta DOUBLE, net_beta_r DOUBLE, shadow_option_haircut DOUBLE,
                payload_json VARCHAR
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
            CREATE TABLE IF NOT EXISTS option_shadow_ledger_legs (
                as_of DATE, report_date DATE, evaluation_date DATE, symbol VARCHAR,
                expression VARCHAR, pricing_mode VARCHAR, leg_role VARCHAR,
                side VARCHAR, option_type VARCHAR, expiry VARCHAR, strike DOUBLE,
                entry_bid DOUBLE, entry_ask DOUBLE, exit_bid DOUBLE, exit_ask DOUBLE,
                entry_mark DOUBLE, exit_mark DOUBLE, entry_contract VARCHAR,
                exit_contract VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS option_real_bid_ask_ledger (
                as_of DATE, report_date DATE, evaluation_date DATE, symbol VARCHAR,
                expression VARCHAR, pricing_mode VARCHAR, resolved BOOLEAN,
                return_pct DOUBLE, reason VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS option_real_bid_ask_legs (
                as_of DATE, report_date DATE, evaluation_date DATE, symbol VARCHAR,
                expression VARCHAR, pricing_mode VARCHAR, leg_role VARCHAR,
                side VARCHAR, option_type VARCHAR, expiry VARCHAR, strike DOUBLE,
                entry_bid DOUBLE, entry_ask DOUBLE, exit_bid DOUBLE, exit_ask DOUBLE,
                entry_mark DOUBLE, exit_mark DOUBLE, entry_contract VARCHAR,
                exit_contract VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS us_gamma_spring (
                as_of DATE, effective_date DATE, previous_effective_date DATE,
                symbol VARCHAR, current_price DOUBLE, gamma_center DOUBLE,
                max_gamma_wall DOUBLE, max_positive_wall DOUBLE,
                max_negative_wall DOUBLE, net_gex DOUBLE,
                dealer_pressure_proxy DOUBLE, dealer_pressure_bucket VARCHAR,
                wall_transition VARCHAR, wall_transition_score DOUBLE,
                gamma_v2_multiplier DOUBLE, management_signal VARCHAR,
                state VARCHAR, payload_json VARCHAR
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
            CREATE TABLE IF NOT EXISTS ai_supercycle_evidence (
                as_of DATE, market VARCHAR, symbol VARCHAR, name VARCHAR,
                layer VARCHAR, priority INTEGER, supply_chain_role VARCHAR,
                bottleneck_focus VARCHAR, evidence_state VARCHAR,
                evidence_score DOUBLE, evidence_source VARCHAR, evidence_date DATE,
                candidate_tier VARCHAR, action VARCHAR, rank INTEGER,
                rank_score DOUBLE, evidence_text VARCHAR, evidence_url VARCHAR,
                payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_lab_quality_index (
                as_of DATE, symbol VARCHAR, company VARCHAR, supercycle_layer VARCHAR,
                data_status VARCHAR, paper_count_total INTEGER,
                lab_quality_score DOUBLE, data_requirement VARCHAR,
                payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_supply_chain_relationships (
                as_of DATE, relationship_id VARCHAR, market VARCHAR,
                primary_symbol VARCHAR, counterparty_symbol VARCHAR,
                customer_symbol VARCHAR, layer VARCHAR, relationship_type VARCHAR,
                source_name VARCHAR, source_type VARCHAR, source_date DATE,
                confidence VARCHAR, evidence_state VARCHAR, source_url VARCHAR,
                payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_supercycle_value_radar (
                as_of DATE, market VARCHAR, symbol VARCHAR, name VARCHAR,
                layer VARCHAR, priority INTEGER, research_priority VARCHAR,
                value_radar_score DOUBLE, evidence_state VARCHAR,
                evidence_score DOUBLE, market_cap DOUBLE, size_optionality DOUBLE,
                size_reason VARCHAR, growth_score DOUBLE, growth_reason VARCHAR,
                lab_quality_score DOUBLE, lab_quality_status VARCHAR,
                valuation_snapshot VARCHAR, blockers VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_supercycle_layer_attribution (
                as_of DATE, market VARCHAR, layer VARCHAR, source VARCHAR,
                n INTEGER, active_dates INTEGER, avg_pct DOUBLE,
                lcb80_pct DOUBLE, win_rate DOUBLE, full_confirm INTEGER,
                proxy_confirm INTEGER, labels VARCHAR, payload_json VARCHAR
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
        con.execute("DELETE FROM option_shadow_ledger_legs WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM option_real_bid_ask_ledger WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM option_real_bid_ask_legs WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM us_gamma_spring WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM cn_lifecycle_research WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM profit_readiness WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM pipeline_requirements_audit WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM ai_supercycle_evidence WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM ai_lab_quality_index WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM ai_supply_chain_relationships WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM ai_supercycle_value_radar WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
        con.execute("DELETE FROM ai_supercycle_layer_attribution WHERE as_of = CAST(? AS DATE)", [payload["as_of"]])
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
                "INSERT INTO portfolio_risk_overlay VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("symbol"),
                    row.get("state"),
                    row.get("strategy_family"),
                    row.get("sector"),
                    row.get("base_r"),
                    row.get("final_r"),
                    row.get("hedge_instrument"),
                    row.get("hedge_notional_r"),
                    row.get("hedge_beta"),
                    row.get("net_beta_r"),
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
            for leg in row.get("legs") or []:
                con.execute(
                    """
                    INSERT INTO option_shadow_ledger_legs
                    VALUES (CAST(? AS DATE), CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        payload["as_of"],
                        row.get("report_date"),
                        row.get("evaluation_date"),
                        row.get("symbol"),
                        row.get("expression"),
                        row.get("pricing_mode"),
                        leg.get("leg_role"),
                        leg.get("side"),
                        leg.get("option_type"),
                        leg.get("expiry"),
                        leg.get("strike"),
                        leg.get("entry_bid"),
                        leg.get("entry_ask"),
                        leg.get("exit_bid"),
                        leg.get("exit_ask"),
                        leg.get("entry_mark"),
                        leg.get("exit_mark"),
                        leg.get("entry_contract"),
                        leg.get("exit_contract"),
                        json.dumps(leg, ensure_ascii=False, default=str),
                    ],
                )
        for row in (payload.get("option_shadow_ledger") or {}).get("real_bid_ask_rows") or []:
            con.execute(
                "INSERT INTO option_real_bid_ask_ledger VALUES (CAST(? AS DATE), CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("report_date"),
                    row.get("evaluation_date"),
                    row.get("symbol"),
                    row.get("expression"),
                    row.get("pricing_mode"),
                    bool(row.get("real_bid_ask_resolved")),
                    row.get("return_pct"),
                    row.get("reason"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
            for leg in row.get("legs") or []:
                con.execute(
                    """
                    INSERT INTO option_real_bid_ask_legs
                    VALUES (CAST(? AS DATE), CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        payload["as_of"],
                        row.get("report_date"),
                        row.get("evaluation_date"),
                        row.get("symbol"),
                        row.get("expression"),
                        row.get("pricing_mode"),
                        leg.get("leg_role"),
                        leg.get("side"),
                        leg.get("option_type"),
                        leg.get("expiry"),
                        leg.get("strike"),
                        leg.get("entry_bid"),
                        leg.get("entry_ask"),
                        leg.get("exit_bid"),
                        leg.get("exit_ask"),
                        leg.get("entry_mark"),
                        leg.get("exit_mark"),
                        leg.get("entry_contract"),
                        leg.get("exit_contract"),
                        json.dumps(leg, ensure_ascii=False, default=str),
                    ],
                )
        gamma_spring = payload.get("gamma_spring") or {}
        for row in gamma_spring.get("rows") or []:
            con.execute(
                """
                INSERT INTO us_gamma_spring
                VALUES (CAST(? AS DATE), TRY_CAST(? AS DATE), TRY_CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["as_of"],
                    gamma_spring.get("effective_date"),
                    gamma_spring.get("previous_effective_date"),
                    row.get("symbol"),
                    row.get("spot"),
                    row.get("center_strike"),
                    row.get("max_wall_strike"),
                    row.get("max_positive_wall"),
                    row.get("max_negative_wall"),
                    row.get("net_gex_1pct"),
                    row.get("dealer_pressure_proxy"),
                    row.get("dealer_pressure_bucket"),
                    row.get("wall_transition"),
                    row.get("wall_transition_score"),
                    row.get("gamma_v2_multiplier"),
                    row.get("management_signal"),
                    row.get("state"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("ai_supercycle_evidence_ledger") or {}).get("rows") or []:
            con.execute(
                """
                INSERT INTO ai_supercycle_evidence
                VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRY_CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("symbol"),
                    row.get("name") or "",
                    row.get("layer"),
                    row.get("priority"),
                    row.get("supply_chain_role"),
                    row.get("bottleneck_focus"),
                    row.get("evidence_state"),
                    row.get("evidence_score"),
                    row.get("evidence_source"),
                    row.get("evidence_date"),
                    row.get("candidate_tier"),
                    row.get("action"),
                    row.get("rank"),
                    row.get("rank_score"),
                    row.get("evidence_text"),
                    row.get("evidence_url"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("ai_lab_quality_index") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO ai_lab_quality_index VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("symbol"),
                    row.get("company"),
                    row.get("supercycle_layer"),
                    row.get("data_status"),
                    row.get("paper_count_total"),
                    row.get("lab_quality_score"),
                    row.get("data_requirement"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("ai_supply_chain_relationships") or {}).get("rows") or []:
            con.execute(
                """
                INSERT INTO ai_supply_chain_relationships
                VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, TRY_CAST(? AS DATE), ?, ?, ?, ?)
                """,
                [
                    payload["as_of"],
                    row.get("relationship_id"),
                    row.get("market"),
                    row.get("primary_symbol"),
                    row.get("counterparty_symbol"),
                    row.get("customer_symbol"),
                    row.get("layer"),
                    row.get("relationship_type"),
                    row.get("source_name"),
                    row.get("source_type"),
                    row.get("source_date"),
                    row.get("confidence"),
                    row.get("evidence_state"),
                    row.get("source_url"),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("ai_supercycle_value_radar") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO ai_supercycle_value_radar VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("symbol"),
                    row.get("name") or "",
                    row.get("layer"),
                    row.get("priority"),
                    row.get("research_priority"),
                    row.get("value_radar_score"),
                    row.get("evidence_state"),
                    row.get("evidence_score"),
                    row.get("market_cap"),
                    row.get("size_optionality"),
                    row.get("size_reason"),
                    row.get("growth_score"),
                    row.get("growth_reason"),
                    row.get("lab_quality_score"),
                    row.get("lab_quality_status"),
                    row.get("valuation_snapshot"),
                    "; ".join(row.get("blockers") or []),
                    json.dumps(row, ensure_ascii=False, default=str),
                ],
            )
        for row in (payload.get("ai_supercycle_layer_attribution") or {}).get("rows") or []:
            con.execute(
                "INSERT INTO ai_supercycle_layer_attribution VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("layer"),
                    row.get("source"),
                    row.get("n"),
                    row.get("active_dates"),
                    row.get("avg_pct"),
                    row.get("lcb80_pct"),
                    row.get("win_rate"),
                    row.get("full_confirm"),
                    row.get("proxy_confirm"),
                    ", ".join(row.get("labels") or []),
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
    by_symbol = best_ranker_rows_by_symbol(ranker.get("all_rows") or [])
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
        elif ranked.get("alpha_sleeve_id") not in US_ALPHA_FACTORY_EXECUTION_SLEEVES:
            row["state"] = "Ranked Watch"
            row["execution_mode"] = "rank_only_no_new_trade"
            row["reason"] = "production ranker kept at 0R: not an Alpha Factory execution sleeve member"
        elif tier in {"top_probe", "secondary_probe", "top_stock_trade", "secondary_stock_trade"}:
            row["state"] = "Execution Alpha"
            row["reason"] = (
                f"Alpha Factory sleeve {ranked.get('alpha_sleeve_id')}; "
                f"production tier={tier}, action={ranked.get('production_action')}"
            )
        elif tier == "active_watch":
            row["state"] = "Ranked Watch"
            row["execution_mode"] = ranked.get("production_action") or "prepare_order_but_wait_for_price"
            row["reason"] = "V2 sleeve member, but production rank is watch-only today"


def apply_cn_ranker_to_current(cn: dict[str, Any], ranker: dict[str, Any]) -> None:
    by_symbol = best_ranker_rows_by_symbol(ranker.get("all_rows") or [])
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
        elif tier == "special_treatment_watch":
            row["state"] = "Special Treatment Watch"
            row["execution_mode"] = "special_treatment_no_probe"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = "production ranker demoted to 0R: ST/restructuring payoff needs a dedicated sleeve"
        elif tier in {
            "observed_lifecycle_probe",
            "observed_lifecycle_secondary",
            "observed_lifecycle_micro_probe",
            "observed_lifecycle_trade",
            "observed_lifecycle_secondary_trade",
        }:
            row["state"] = "Execution Alpha"
            row["execution_mode"] = ranked.get("production_action") or "buy_planned_entry_observed"
            row["lifecycle_action"] = ranked.get("production_action") or row.get("lifecycle_action")
            row["reason"] = (
                f"Observed lifecycle probability sleeve {CN_OBSERVED_LIFECYCLE_SLEEVE}; "
                f"ExpR={fmt_num(ranked.get('expected_r_t3'))}, LCBR={fmt_num(ranked.get('lcb80_r_t3'))}, "
                f"n={ranked.get('observed_probability_n')}"
            )
        elif ranked.get("alpha_sleeve_id") not in CN_ALPHA_FACTORY_EXECUTION_SLEEVES:
            row["state"] = "Ranked Watch"
            row["execution_mode"] = "rank_only_no_new_trade"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = "production ranker kept at 0R: no Alpha Factory sleeve and no qualified observed lifecycle probability"
        elif tier in {"top_probe", "secondary_probe", "top_stock_trade", "secondary_stock_trade"}:
            row["state"] = "Execution Alpha"
            row["lifecycle_action"] = ranked.get("production_action") or row.get("lifecycle_action")
            row["reason"] = (
                f"Alpha Factory sleeve {ranked.get('alpha_sleeve_id')}; "
                f"production tier={tier}, action={ranked.get('production_action')}"
            )
        elif tier in {"active_watch", "bench_ranked"}:
            row["state"] = "Ranked Watch"
            row["execution_mode"] = ranked.get("production_action") or "watch_for_rotation"
            row["lifecycle_action"] = "rank_only_no_new_trade"
            row["reason"] = f"Alpha Factory sleeve member, but rank tier {tier} is watch-only today"


def ranker_rows_as_current_rows(market: str, ranker: dict[str, Any]) -> list[dict[str, Any]]:
    trade_tiers = {"top_probe", "secondary_probe", "top_stock_trade", "secondary_stock_trade"}
    trade_tiers |= {"observed_lifecycle_trade", "observed_lifecycle_secondary_trade"}
    event_tiers = {"event_risk_watch", "falling_knife_watch", "special_treatment_watch"}
    out: list[dict[str, Any]] = []
    for ranked in ranker.get("all_rows") or []:
        row = dict(ranked)
        tier = str(row.get("production_tier") or "")
        action = str(row.get("production_action") or "")
        row["production_rank"] = row.get("rank")
        row["production_rank_score"] = row.get("rank_score")
        row["execution_mode"] = action or row.get("execution_mode")
        row["lifecycle_action"] = action or row.get("lifecycle_action")
        row.setdefault("policy", row.get("alpha_sleeve_id") or "ai_infra_ranker")
        if market.upper() == "CN":
            for key in ["p_win_t1", "p_hit_1r_t3", "p_stop_t3", "expected_r_t3", "lcb80_r_t3"]:
                row.setdefault(key, None)
        if tier in trade_tiers:
            row["state"] = "Execution Alpha"
        elif tier in event_tiers:
            row["state"] = "Event Risk Watch"
        else:
            row["state"] = "Ranked Watch"
        if not row.get("reason"):
            if row["state"] == "Execution Alpha":
                row["reason"] = (
                    f"{market.upper()} AI-infra ranker promoted {row.get('symbol')} via "
                    f"{row.get('alpha_sleeve_id') or row.get('execution_source') or tier}; action={action}"
                )
            else:
                row["reason"] = row.get("size_hint") or action or "AI-infra ranked watch"
        out.append(row)
    return out


def assert_convexity_discipline(payload: dict[str, Any]) -> None:
    """Hard guardrail — the report must never instruct an anti-convex expression.

    Selling premium / shorting vol / leveraged range-trading is forbidden by
    the convexity doctrine (Agents.md §6). This collects every executable
    instruction the report emits and refuses to render if any is anti-convex.
    """
    expressions: list[str] = []
    summary = payload.get("production_decision_summary") or {}
    for row in summary.get("actionable") or []:
        expressions.append(str(row.get("action") or ""))
    regime = payload.get("risk_regime") or {}
    # hedge_directive AND victim_action both carry option instructions the
    # report renders verbatim — both must clear the anti-convex guardrail.
    expressions.append(str(regime.get("hedge_directive") or ""))
    expressions.append(str(regime.get("victim_action") or ""))
    assert_no_anticonvex(expressions)


def assert_promoted_execution_rows(payload: dict[str, Any]) -> None:
    promoted_rows = (payload.get("promotion_contract") or {}).get("rows") or []
    trade_tiers = {"top_probe", "secondary_probe", "top_stock_trade", "secondary_stock_trade"}
    trade_tiers |= {"observed_lifecycle_trade", "observed_lifecycle_secondary_trade"}
    for market in ["us", "cn"]:
        for row in (payload.get(market) or {}).get("current") or []:
            sleeve_id = row.get("alpha_sleeve_id")
            if row.get("state") == "Execution Alpha" and sleeve_id:
                assert_sleeve_promoted(market=market, sleeve_id=str(sleeve_id), promoted_rows=promoted_rows)
        ranker_key = f"{market}_opportunity_ranker"
        for row in (payload.get(ranker_key) or {}).get("all_rows") or []:
            sleeve_id = row.get("alpha_sleeve_id")
            if sleeve_id and row.get("production_tier") in trade_tiers:
                assert_sleeve_promoted(market=market, sleeve_id=str(sleeve_id), promoted_rows=promoted_rows)


def apply_ai_supercycle_layer_guardrails(payload: dict[str, Any]) -> dict[str, Any]:
    """Demote execution rows when the layer-level payoff evidence is not positive."""
    attribution_rows = (payload.get("ai_supercycle_layer_attribution") or {}).get("rows") or []
    history: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in attribution_rows:
        key = (
            str(row.get("market") or "").upper(),
            str(row.get("sleeve_id") or row.get("source") or ""),
            str(row.get("layer") or ""),
        )
        history[key] = row

    blocked: list[dict[str, Any]] = []
    for row in (payload.get("cn") or {}).get("current") or []:
        if row.get("state") != "Execution Alpha" or row.get("alpha_sleeve_id") != CN_TAPE_SLEEVE_ID:
            continue
        layer = str(row.get("supercycle_layer") or "")
        attr = history.get(("CN", CN_TAPE_SLEEVE_ID, layer))
        lcb80 = round_or_none((attr or {}).get("lcb80_pct"))
        n = (attr or {}).get("n")
        if attr and lcb80 is not None and lcb80 > 0:
            continue
        blocker = (
            f"layer_history_rejected: {layer or '-'} "
            f"n={n or '-'}, LCB80={fmt_pct(lcb80) if lcb80 is not None else '-'}"
        )
        blocked.append(
            {
                "market": "CN",
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "sleeve_id": CN_TAPE_SLEEVE_ID,
                "layer": layer,
                "n": n,
                "lcb80_pct": lcb80,
                "reason": blocker,
            }
        )
        row["state"] = "Ranked Watch"
        row["production_tier"] = "ranked_watch"
        row["execution_mode"] = "rank_only_layer_history_blocked"
        row["lifecycle_action"] = "rank_only_layer_history_blocked"
        row["alpha_factory_role"] = "rank_only"
        row["layer_history_blocked"] = True
        row["layer_history_blocker"] = blocker
        row["reason"] = (
            "AI-infra tape is strong, but execution R is blocked until the "
            f"CN layer history turns positive: {blocker}"
        )
    return {
        "contract": "cn_tape_execution_requires_positive_layer_lcb80",
        "rows": blocked,
    }


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    as_of = parse_date(args.date) if args.date else infer_report_date(args.us_db, args.cn_db)
    start = parse_date(args.start)
    promotion_db = getattr(args, "promotion_db", None)
    uses_default_data = (
        Path(args.us_db).resolve() == (STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb").resolve()
        and Path(args.cn_db).resolve() == (STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb").resolve()
    )
    if promotion_db is None and not uses_default_data:
        bootstrap_rows = with_trend_mainline_overrides(list(BOOTSTRAP_PROMOTED_SLEEVES))
        promotion_contract = {
            "source": "bootstrap_non_default_test_db",
            "rows": bootstrap_rows,
            "promoted": sorted(f"{market}:{sleeve}" for market, sleeve in is_promoted_pairs(bootstrap_rows)),
        }
    else:
        promotion_contract = load_promotion_contract(promotion_db, as_of)
    promoted_rows = promotion_contract.get("rows") or []
    us = summarize_us(args.us_db, start, as_of, promoted_rows)
    cn = summarize_cn(args.cn_db, start, as_of, promoted_rows)
    limit_up = summarize_limit_up(args.cn_db, start, as_of)
    ranker_ai_infra_mode = getattr(args, "ai_infra_mode", None) or (
        "enforce_expand" if uses_default_data else "off"
    )
    # Compute regime first so it can drive broad_signal weight inside the ranker.
    bubble_hedge_payload = load_bubble_hedge_payload(as_of.isoformat())
    capitulation_payload = load_capitulation_payload(as_of.isoformat())
    risk_regime = build_risk_regime(bubble_hedge_payload, capitulation_payload)
    cn_risk_regime = load_cn_risk_regime_payload(as_of.isoformat())
    us_regime_state = str((risk_regime or {}).get("state") or "hedge").lower()
    us_ranker = us_opportunity_ranker.build_ranker_payload(
        as_of=as_of,
        candidates=us.get("current") or [],
        candidate_status="from_main_strategy_v2_current",
        us_db=args.us_db,
        source_report="main_strategy_v2_payload",
        top=80,
        ai_infra_root=STACK_ROOT / "ai_infra",
        ai_infra_mode=ranker_ai_infra_mode,
        regime_state=us_regime_state,
    )
    us["raw_current_count"] = len(us.get("current") or [])
    us["current"] = ranker_rows_as_current_rows("US", us_ranker)
    cn_ranker = cn_opportunity_ranker.build_ranker_payload(
        as_of=as_of,
        candidates=cn.get("current") or [],
        candidate_status="from_main_strategy_v2_current",
        cn_db=args.cn_db,
        source_report="main_strategy_v2_payload",
        top=80,
        ai_infra_root=STACK_ROOT / "ai_infra",
        ai_infra_mode=ranker_ai_infra_mode,
    )
    cn["raw_current_count"] = len(cn.get("current") or [])
    cn["current"] = ranker_rows_as_current_rows("CN", cn_ranker)
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
        risk_regime=risk_regime,
        cn_risk_regime=cn_risk_regime,
    )
    option_shadow_ledger = build_option_shadow_ledger(args.us_db, start, as_of)
    us_calendar_focus = calendar_focus_symbols(
        us.get("current") or [],
        us.get("missed_alpha_radar") or [],
        (us_ranker or {}).get("all_rows") or [],
    )
    cn_calendar_focus = calendar_focus_symbols(
        cn.get("current") or [],
        (cn_ranker or {}).get("all_rows") or [],
        (limit_up or {}).get("current") or [],
    )
    earnings_calendar = {
        "us": build_us_earnings_calendar(args.us_db, as_of, focus_symbols=us_calendar_focus),
        "cn": build_cn_earnings_calendar(args.cn_db, as_of, focus_symbols=cn_calendar_focus),
    }
    # EMA21/50 tape overlay covering production basket + source-review focus tickers.
    ema_overlay_symbols: set[str] = set()
    for ticker in {*us_calendar_focus, *cn_calendar_focus}:
        if ticker:
            ema_overlay_symbols.add(str(ticker).upper())
    try:
        with SOURCE_REVIEW_QUEUE_PATH.open("r", encoding="utf-8") as queue_handle:
            for row in csv.DictReader(queue_handle):
                for alias in (row.get("ticker") or "").split("/"):
                    token = alias.strip().upper()
                    if token:
                        ema_overlay_symbols.add(token)
    except FileNotFoundError:
        pass
    ema_overlay = build_ema_tape_overlay(args.us_db, args.cn_db, ema_overlay_symbols, as_of)

    source_review_calendar = build_source_review_calendar(
        focus_symbols=sorted({*us_calendar_focus, *cn_calendar_focus}),
        ema_overlay=ema_overlay,
    )
    us_basket_symbols = [
        str(row.get("symbol") or "").upper()
        for row in ((us_ranker or {}).get("production_basket") or [])
        if row.get("symbol")
    ]
    us_trade_plan_symbols = sorted(
        {
            str(row.get("symbol") or "").upper()
            for row in [
                *((us_ranker or {}).get("all_rows") or []),
                *((us_ranker or {}).get("production_basket") or []),
                *(us.get("current") or []),
            ]
            if row.get("symbol")
        }
    )
    us_trade_plan = build_us_stock_trade_plan(args.us_db, us_trade_plan_symbols, as_of)
    options_verdicts = build_options_verdicts(args.us_db, us_trade_plan_symbols, as_of)
    gamma_spring_symbols = sorted({*us_trade_plan_symbols, "SPY", "QQQ", "SMH"})
    gamma_spring = build_gamma_spring_snapshot(args.us_db, gamma_spring_symbols, as_of)
    index_skew = build_index_skew(args.us_db, as_of)
    cn_basket_symbols = [
        str(row.get("symbol") or "").upper()
        for row in ((cn_ranker or {}).get("production_basket") or [])
        if row.get("symbol")
    ]
    # For shadow_full we want a wider universe — top 25 of CN ranker
    cn_shadow_symbols = sorted({
        str(row.get("symbol") or "").upper()
        for row in ((cn_ranker or {}).get("all_rows") or [])[:25]
        if row.get("symbol")
    })
    cn_shadow_full = build_cn_shadow_full(args.cn_db, cn_shadow_symbols, as_of)
    benchmark_attribution = build_benchmark_attribution(
        args.us_db,
        args.cn_db,
        as_of,
        us_basket=us_basket_symbols,
        cn_basket=cn_basket_symbols,
    )
    satellite_pool_report = build_satellite_pool_report(ema_overlay=ema_overlay)
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
        "earnings_calendar": earnings_calendar,
        "source_review_calendar": source_review_calendar,
        "benchmark_attribution": benchmark_attribution,
        "satellite_pool_report": satellite_pool_report,
        "ema_tape_overlay": ema_overlay,
        "strategy_alpha_bulletin": load_strategy_alpha_bulletin(as_of),
        "us_market_data_status": build_us_market_data_status(args.us_db, as_of),
        "report_action_backtest_summary": load_report_action_backtest_summary(as_of.isoformat()),
        "us_trade_plan": us_trade_plan,
        "options_verdicts": options_verdicts,
        "gamma_spring": gamma_spring,
        "index_skew": index_skew,
        "cn_shadow_full": cn_shadow_full,
        "market_regime_score": build_market_regime_score(args.us_db, as_of),
        "serenity_crosscheck": build_serenity_crosscheck(
            args.us_db, as_of, (us_ranker or {}).get("all_rows") or []
        ),
        "fear_greed": load_fear_greed_payload(as_of.isoformat()),
        "options_anomaly_rows": load_options_anomaly_payload(as_of.isoformat()),
        "options_tenor_signals": load_options_tenor_signals(as_of.isoformat()),
        "bubble_hedge": bubble_hedge_payload,
        "risk_regime": risk_regime,
        "cn_risk_regime": cn_risk_regime,
        "promotion_contract": promotion_contract,
    }
    assert_promoted_execution_rows(payload)
    payload["ai_supply_chain_relationships"] = build_ai_supply_chain_relationships()
    payload["ai_supercycle_evidence_ledger"] = build_ai_supercycle_evidence_ledger(payload)
    payload["ai_lab_quality_index"] = build_ai_lab_quality_index()
    payload["ai_supercycle_value_radar"] = build_ai_supercycle_value_radar(payload, args.us_db, as_of)
    payload["ai_supercycle_layer_attribution"] = build_ai_supercycle_layer_attribution(args.us_db, args.cn_db, start, as_of)
    payload["ai_supercycle_layer_guardrails"] = apply_ai_supercycle_layer_guardrails(payload)
    payload["profit_guardrails"] = build_profit_guardrails(us, cn, limit_up)
    payload["strategy_direction"] = build_strategy_direction(us, cn, limit_up, payload["profit_guardrails"])
    payload["portfolio_risk_overlay"] = build_portfolio_risk_overlay(
        us,
        cn,
        limit_up,
        payload["profit_guardrails"],
        args.us_db,
        args.cn_db,
        as_of,
        risk_regime=risk_regime,
        cn_risk_regime=cn_risk_regime,
    )
    payload["profit_readiness"] = build_profit_readiness(payload)
    payload["pipeline_requirements_audit"] = build_pipeline_requirements_audit(payload)
    payload["production_decision_summary"] = build_production_decision_summary(payload)
    assert_convexity_discipline(payload)
    return payload


def run(args: argparse.Namespace) -> dict[str, Any]:
    payload = build_payload(args)
    output_dir = args.output_root / payload["as_of"]
    output_dir.mkdir(parents=True, exist_ok=True)
    report_md = render_report(payload)
    (output_dir / "main_strategy_v2_backtest.md").write_text(report_md, encoding="utf-8")
    (output_dir / "cn_daily_report.md").write_text(render_cn_standalone_report(payload), encoding="utf-8")
    (output_dir / "us_daily_report.md").write_text(render_us_standalone_report(payload), encoding="utf-8")
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
    (output_dir / "gamma_spring.md").write_text(
        "\n".join(render_gamma_spring_section(payload)),
        encoding="utf-8",
    )
    (output_dir / "gamma_spring.json").write_text(
        json.dumps(payload.get("gamma_spring") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "earnings_calendar.md").write_text(
        "\n".join(
            render_earnings_calendar_section(payload, "US", limit=60)
            + render_earnings_calendar_section(payload, "CN", limit=60)
        ),
        encoding="utf-8",
    )
    (output_dir / "earnings_calendar.json").write_text(
        json.dumps(payload.get("earnings_calendar") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "source_review_calendar.md").write_text(
        "\n".join(
            render_source_review_calendar_section(payload, "US", limit=60)
            + render_source_review_calendar_section(payload, "CN", limit=60)
        ),
        encoding="utf-8",
    )
    (output_dir / "source_review_calendar.json").write_text(
        json.dumps(payload.get("source_review_calendar") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "benchmark_attribution.md").write_text(
        "\n".join(
            render_benchmark_attribution_section(payload, "US", limit=10)
            + render_benchmark_attribution_section(payload, "CN", limit=10)
        ),
        encoding="utf-8",
    )
    (output_dir / "benchmark_attribution.json").write_text(
        json.dumps(payload.get("benchmark_attribution") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "satellite_pool_report.md").write_text(
        "\n".join(render_satellite_pool_report_section(payload, limit_per_region=40)),
        encoding="utf-8",
    )
    (output_dir / "satellite_pool_report.json").write_text(
        json.dumps(payload.get("satellite_pool_report") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "ema_tape_overlay.md").write_text(
        render_ema_tape_overlay_markdown(payload.get("ema_tape_overlay") or {}, payload["as_of"]),
        encoding="utf-8",
    )
    (output_dir / "ema_tape_overlay.json").write_text(
        json.dumps(payload.get("ema_tape_overlay") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "fear_greed.md").write_text(
        "\n".join(render_fear_greed_section(payload)) if payload.get("fear_greed") else "_Fear & Greed unavailable._",
        encoding="utf-8",
    )
    (output_dir / "fear_greed.json").write_text(
        json.dumps(payload.get("fear_greed") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "ai_supercycle_evidence.md").write_text(render_ai_supercycle_evidence(payload), encoding="utf-8")
    (output_dir / "ai_supercycle_evidence.json").write_text(
        json.dumps(payload.get("ai_supercycle_evidence_ledger") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "ai_supply_chain_relationships.md").write_text(
        render_ai_supply_chain_relationships(payload), encoding="utf-8"
    )
    (output_dir / "ai_supply_chain_relationships.json").write_text(
        json.dumps(payload.get("ai_supply_chain_relationships") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "ai_lab_quality_index.md").write_text(render_ai_lab_quality_index(payload), encoding="utf-8")
    (output_dir / "ai_lab_quality_index.json").write_text(
        json.dumps(payload.get("ai_lab_quality_index") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "ai_supercycle_value_radar.md").write_text(render_ai_supercycle_value_radar(payload), encoding="utf-8")
    (output_dir / "ai_supercycle_value_radar.json").write_text(
        json.dumps(payload.get("ai_supercycle_value_radar") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "ai_supercycle_layer_attribution.md").write_text(
        render_ai_supercycle_layer_attribution(payload), encoding="utf-8"
    )
    (output_dir / "ai_supercycle_layer_attribution.json").write_text(
        json.dumps(payload.get("ai_supercycle_layer_attribution") or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
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
