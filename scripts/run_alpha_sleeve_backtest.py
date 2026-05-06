#!/usr/bin/env python3
"""Build an alpha-sleeve scorecard across the existing quant-stack ledgers.

This report is intentionally a control-plane layer. It does not invent a new
trading model; it asks which existing return streams have enough evidence,
which ones are just diagnostics, and which data gaps prevent a sleeve from
becoming money-ready.
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
from typing import Any

import duckdb


SCRIPT_DIR = Path(__file__).resolve().parent
STACK_ROOT = SCRIPT_DIR.parents[0]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_main_strategy_v2_backtest as v2  # noqa: E402

try:
    import pandas as pd
except ImportError:  # pragma: no cover - the repo already depends on pandas for factor-lab.
    pd = None  # type: ignore[assignment]


DEFAULT_START = "2026-03-01"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "alpha_factory"
DEFAULT_FACTOR_LAB_DB = STACK_ROOT / "factor-lab" / "data" / "factor_lab.duckdb"
FACTOR_LAB_CORR_BLOCK_THRESHOLD = 0.85
FACTOR_LAB_MIN_PROB_POSITIVE = 0.80
FACTOR_LAB_GATE_MODE = "opportunity"
FACTOR_LAB_AUTO_PROD_CONTRACTS = {
    "daily_price_overlay": "action_overlay",
}


@dataclass
class Sleeve:
    sleeve_id: str
    market: str
    label: str
    signal_rule: str
    horizon: str
    data_status: str
    money_status: str
    notes: str
    rows: list[dict[str, Any]]
    source_factor_id: str | None = None

    def metrics_dict(self) -> dict[str, Any]:
        metrics = v2.compute_metrics(self.label, self.rows).to_dict()
        metrics.update(
            {
                "sleeve_id": self.sleeve_id,
                "market": self.market,
                "signal_rule": self.signal_rule,
                "horizon": self.horizon,
                "data_status": self.data_status,
                "money_status": self.money_status,
                "top5_pnl_share": v2.round_or_none(top5_pnl_share(self.rows)),
                "mean_daily_breadth": v2.round_or_none(mean_daily_breadth(self.rows)),
                "notes": self.notes,
            }
        )
        if self.source_factor_id:
            metrics["factor_id"] = self.source_factor_id
        return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run alpha sleeve scorecard backtest.")
    parser.add_argument("--date", default=None, help="Report date. Defaults to latest available DB date.")
    parser.add_argument("--start", default=DEFAULT_START, help="Backtest start date.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--us-db", type=Path, default=STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb")
    parser.add_argument("--cn-db", type=Path, default=STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb")
    parser.add_argument("--factor-lab-db", type=Path, default=DEFAULT_FACTOR_LAB_DB)
    parser.add_argument("--min-money-n", type=int, default=20)
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def fmt_pct(value: Any, digits: int = 2) -> str:
    return v2.fmt_pct(value, digits)


def fmt_num(value: Any, digits: int = 2) -> str:
    return v2.fmt_num(value, digits)


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return v2.table_exists(con, table)


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    return v2.rows_as_dicts(con, sql, params)


def latest_report_date(us_db: Path, cn_db: Path) -> date:
    return v2.infer_report_date(us_db, cn_db)


def top5_pnl_share(rows: list[dict[str, Any]]) -> float | None:
    returns = [float(ret) for row in rows if (ret := v2.round_or_none(row.get("return_pct"))) is not None]
    positives = [ret for ret in returns if ret > 0]
    if not positives:
        return None
    denom = sum(positives)
    if denom <= 1e-12:
        return None
    return sum(sorted(positives, reverse=True)[:5]) / denom


def mean_daily_breadth(rows: list[dict[str, Any]]) -> float | None:
    by_date: dict[str, set[str]] = {}
    for row in rows:
        report_date = v2.as_iso(row.get("report_date"))
        symbol = str(row.get("symbol") or "")
        if report_date and symbol and v2.round_or_none(row.get("return_pct")) is not None:
            by_date.setdefault(report_date, set()).add(symbol)
    if not by_date:
        return None
    return statistics.fmean(len(symbols) for symbols in by_date.values())


def daily_series(rows: list[dict[str, Any]]) -> dict[str, float]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        report_date = v2.as_iso(row.get("report_date"))
        ret = v2.round_or_none(row.get("return_pct"))
        if report_date and ret is not None:
            grouped.setdefault(report_date, []).append(float(ret))
    return {key: statistics.fmean(values) for key, values in sorted(grouped.items()) if values}


def lcb80_pct(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    std = statistics.stdev(values)
    return statistics.fmean(values) - v2.LCB80_Z * std / math.sqrt(len(values))


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def probabilistic_positive_mean(metrics: dict[str, Any]) -> float | None:
    n = int(metrics.get("n") or 0)
    avg = v2.round_or_none(metrics.get("avg_pct"))
    std = v2.round_or_none(metrics.get("std_pct"))
    if n < 2 or avg is None or std is None or std <= 1e-12:
        return None
    return normal_cdf(float(avg) / (float(std) / math.sqrt(n)))


def deflated_lcb80_pct(metrics: dict[str, Any], n_trials: int) -> float | None:
    n = int(metrics.get("n") or 0)
    avg = v2.round_or_none(metrics.get("avg_pct"))
    std = v2.round_or_none(metrics.get("std_pct"))
    if n < 2 or avg is None or std is None or std <= 1e-12:
        return v2.round_or_none(metrics.get("lcb80_pct"))
    trial_z = math.sqrt(2.0 * math.log(max(int(n_trials or 1), 1)))
    return float(avg) - (v2.LCB80_Z + trial_z) * float(std) / math.sqrt(n)


def rolling_oos_min_lcb80_pct(rows: list[dict[str, Any]], windows: int = 3) -> float | None:
    series = daily_series(rows)
    if len(series) < windows * 5:
        return None
    ordered = [series[key] for key in sorted(series)]
    chunk_size = math.ceil(len(ordered) / windows)
    lcbs = [
        lcb80_pct(ordered[idx : idx + chunk_size])
        for idx in range(0, len(ordered), chunk_size)
    ]
    clean = [value for value in lcbs if value is not None]
    return min(clean) if len(clean) == windows else None


def pearson_corr(a: dict[str, float], b: dict[str, float]) -> float | None:
    keys = sorted(set(a) & set(b))
    if len(keys) < 3:
        return None
    xs = [a[key] for key in keys]
    ys = [b[key] for key in keys]
    mx = statistics.fmean(xs)
    my = statistics.fmean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=False))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x <= 1e-12 or den_y <= 1e-12:
        return None
    return num / (den_x * den_y)


def money_status_for(
    *,
    metrics: dict[str, Any],
    role: str,
    data_status: str,
    min_n: int,
) -> str:
    n = int(metrics.get("n") or 0)
    if data_status.startswith("missing") or data_status.startswith("no_"):
        return "no_data"
    if role == "baseline":
        return "baseline_only"
    if role == "shadow":
        return "shadow_only"
    if role == "radar":
        return "radar_only"
    if role == "research":
        return "research_only"
    if role == "overlay":
        return "report_overlay"
    if role == "probe":
        return "stock_probe_only"
    if n <= 0:
        return "no_data"
    return "money_candidate"


def make_sleeve(
    *,
    sleeve_id: str,
    market: str,
    label: str,
    signal_rule: str,
    horizon: str,
    data_status: str,
    role: str,
    notes: str,
    rows: list[dict[str, Any]],
    min_money_n: int,
) -> Sleeve:
    metrics = v2.compute_metrics(label, rows).to_dict()
    return Sleeve(
        sleeve_id=sleeve_id,
        market=market,
        label=label,
        signal_rule=signal_rule,
        horizon=horizon,
        data_status=data_status,
        money_status=money_status_for(metrics=metrics, role=role, data_status=data_status, min_n=min_money_n),
        notes=notes,
        rows=rows,
    )


def factor_lab_role(report_contract: str, money_readiness: str) -> str:
    contract = str(report_contract or "research_only")
    readiness = str(money_readiness or "research_only")
    if contract == "fresh_buy_gate" and readiness in {"money_ready", "money_candidate"}:
        return "money"
    if contract in {"action_overlay", "setup_overlay", "risk_warning", "hold_overlay"}:
        return "overlay"
    return "research"


def resolve_factor_lab_production_contract(
    report_contract: str,
    money_readiness: str,
    sleeve_id: str,
) -> tuple[str, str, str | None]:
    """Promoted daily-price factors are production overlays unless explicitly downgraded.

    Older Factor Lab rows defaulted to `research_only` for parser compatibility.
    Once such a factor has promoted sleeve returns, treat the daily-price sleeve
    as an executable overlay input; Alpha Factory records weak evidence as
    opportunity flags instead of blocking the sleeve.
    """
    contract = str(report_contract or "research_only").strip().lower()
    readiness = str(money_readiness or "research_only").strip().lower()
    sleeve = str(sleeve_id or "").strip().lower()
    if contract == "research_only" and sleeve in FACTOR_LAB_AUTO_PROD_CONTRACTS:
        promoted_contract = FACTOR_LAB_AUTO_PROD_CONTRACTS[sleeve]
        return promoted_contract, "money_candidate", f"auto_prod_contract={promoted_contract}"
    return contract, readiness, None


def factor_lab_money_status(
    *,
    label: str,
    rows: list[dict[str, Any]],
    report_contract: str,
    money_readiness: str,
    n_trials: int,
    min_money_n: int,
) -> tuple[str, str]:
    role = factor_lab_role(report_contract, money_readiness)
    metrics = v2.compute_metrics(label, rows).to_dict()
    double_cost = v2.compute_metrics(label + " double-cost", rows, return_key="double_cost_return_pct").to_dict()
    top_share = top5_pnl_share(rows)
    prob_positive = probabilistic_positive_mean(metrics)
    deflated_lcb = deflated_lcb80_pct(metrics, n_trials)
    rolling_lcb = rolling_oos_min_lcb80_pct(rows)
    opportunity_flags: list[str] = []
    if int(metrics.get("n") or 0) < min_money_n:
        opportunity_flags.append("sample_thin")
    if (metrics.get("lcb80_pct") or 0.0) <= 0.0:
        opportunity_flags.append("lcb80<=0")
    if top_share is not None and top_share > 0.30:
        opportunity_flags.append("top5_pnl_share>30%")
    if (double_cost.get("lcb80_pct") or 0.0) <= 0.0:
        opportunity_flags.append("double_cost_lcb80<=0")
    if prob_positive is not None and prob_positive < FACTOR_LAB_MIN_PROB_POSITIVE:
        opportunity_flags.append("prob_positive<80%")
    if deflated_lcb is not None and deflated_lcb <= 0.0:
        opportunity_flags.append("deflated_lcb80<=0")
    if rolling_lcb is not None and rolling_lcb <= 0.0:
        opportunity_flags.append("rolling_oos_min_lcb80<=0")
    note = (
        f"contract={report_contract}; readiness={money_readiness}; mode={FACTOR_LAB_GATE_MODE}; "
        f"double_cost_lcb80={fmt_pct(double_cost.get('lcb80_pct'))}; "
        f"top5_pnl_share={fmt_pct((top_share or 0.0) * 100.0) if top_share is not None else '-'}; "
        f"n_trials={max(int(n_trials or 1), 1)}; "
        f"prob_positive={fmt_pct(prob_positive * 100.0) if prob_positive is not None else '-'}; "
        f"deflated_lcb80={fmt_pct(deflated_lcb)}; "
        f"rolling_oos_min_lcb80={fmt_pct(rolling_lcb)}"
    )
    if opportunity_flags:
        note = f"{note}; opportunity_flags={','.join(opportunity_flags)}"
    if role == "research":
        return "research_only", note
    if role == "overlay":
        return "report_overlay", note
    return "money_candidate", note


def factor_lab_trial_counts_by_market(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    if table_exists(con, "factor_experiment_ledger"):
        rows = rows_as_dicts(
            con,
            """
            SELECT market, COUNT(DISTINCT experiment_id) AS n_trials
            FROM factor_experiment_ledger
            WHERE market IS NOT NULL
              AND CAST(ts AS DATE) <= CAST(? AS DATE)
            GROUP BY market
            """,
            [as_of.isoformat()],
        )
        counts.update(
            {
                str(row.get("market") or ""): max(int(row.get("n_trials") or 0), 1)
                for row in rows
                if row.get("market")
            }
        )

    if table_exists(con, "factor_registry"):
        rows = rows_as_dicts(
            con,
            """
            SELECT market, COUNT(*) AS n_trials
            FROM factor_registry
            WHERE market IS NOT NULL
            GROUP BY market
            """,
            [],
        )
        for row in rows:
            market = str(row.get("market") or "")
            if not market:
                continue
            counts[market] = max(counts.get(market, 1), int(row.get("n_trials") or 1))

    return counts


def load_factor_lab_sleeves(
    factor_lab_db: Path,
    start: date,
    as_of: date,
    min_money_n: int,
) -> list[Sleeve]:
    if not factor_lab_db.exists():
        return []
    con = duckdb.connect(str(factor_lab_db), read_only=True)
    try:
        if not table_exists(con, "factor_sleeve_returns"):
            return []
        trial_counts = factor_lab_trial_counts_by_market(con, as_of)
        rows = rows_as_dicts(
            con,
            """
            SELECT return_date, market, factor_id, sleeve_id, factor_name,
                   report_contract, money_readiness, direction, bucket,
                   gross_return_pct, daily_return_pct, cost_adjusted_return_pct,
                   cost_pct, n_names
            FROM factor_sleeve_returns
            WHERE return_date >= CAST(? AS DATE)
              AND return_date <= CAST(? AS DATE)
              AND bucket = 'top_quintile_long'
            ORDER BY market, factor_id, return_date
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()

    grouped: dict[str, list[dict[str, Any]]] = {}
    meta: dict[str, dict[str, Any]] = {}
    for row in rows:
        factor_id = str(row.get("factor_id") or "")
        if not factor_id:
            continue
        grouped.setdefault(factor_id, []).append(
            {
                "report_date": v2.as_iso(row.get("return_date")),
                "symbol": factor_id,
                "return_pct": v2.round_or_none(row.get("cost_adjusted_return_pct")),
                "gross_return_pct": v2.round_or_none(row.get("gross_return_pct")),
                "double_cost_return_pct": (
                    v2.round_or_none(row.get("daily_return_pct"))
                    - 2.0 * v2.round_or_none(row.get("cost_pct"))
                    if v2.round_or_none(row.get("daily_return_pct")) is not None
                    and v2.round_or_none(row.get("cost_pct")) is not None
                    else None
                ),
                "n_names": row.get("n_names"),
            }
        )
        meta[factor_id] = row

    sleeves: list[Sleeve] = []
    for factor_id, factor_rows in grouped.items():
        info = meta[factor_id]
        name = str(info.get("factor_name") or factor_id)
        market = str(info.get("market") or "")
        registry_contract = str(info.get("report_contract") or "research_only")
        registry_readiness = str(info.get("money_readiness") or "research_only")
        sleeve_contract = str(info.get("sleeve_id") or "")
        contract, readiness, auto_prod_note = resolve_factor_lab_production_contract(
            registry_contract,
            registry_readiness,
            sleeve_contract,
        )
        n_trials = trial_counts.get(market, 1)
        money_status, note = factor_lab_money_status(
            label=f"Factor Lab {name} top-quintile sleeve",
            rows=factor_rows,
            report_contract=contract,
            money_readiness=readiness,
            n_trials=n_trials,
            min_money_n=min_money_n,
        )
        if auto_prod_note:
            note = f"{note}; legacy_contract={registry_contract}; {auto_prod_note}"
        sleeves.append(
            Sleeve(
                sleeve_id=(
                    f"factor_lab_{factor_id}"
                    if factor_id.startswith(f"{market}_")
                    else f"factor_lab_{market}_{factor_id}"
                ),
                market=market,
                label=f"Factor Lab {name}",
                signal_rule=(
                    f"promoted factor {info.get('sleeve_id')}; contract={contract}; "
                    "oriented long-only top quintile, 5D forward return averaged to daily"
                ),
                horizon="5D forward, daily averaged",
                data_status="factor_sleeve_returns",
                money_status=money_status,
                notes=note,
                rows=factor_rows,
                source_factor_id=factor_id,
            )
        )
    return sleeves


def query_us_sec_filing_returns(us_db: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    if not us_db.exists():
        return []
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        if not table_exists(con, "sec_filings") or not table_exists(con, "prices_daily"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH events AS (
                SELECT symbol, accession_number, filed_date,
                       LOWER(COALESCE(description, '') || ' ' || COALESCE(items, '')) AS event_text
                FROM sec_filings
                WHERE filed_date >= CAST(? AS DATE)
                  AND filed_date <= CAST(? AS DATE)
                  AND form_type = '8-K'
            ),
            joined AS (
                SELECT e.symbol, e.accession_number, e.filed_date, e.event_text,
                       p.date AS price_date, p.adj_close AS close,
                       ROW_NUMBER() OVER (
                           PARTITION BY e.symbol, e.accession_number
                           ORDER BY p.date
                       ) AS rn
                FROM events e
                JOIN prices_daily p
                  ON p.symbol = e.symbol
                 AND p.date > e.filed_date
                 AND p.adj_close > 0
            ),
            entry AS (
                SELECT symbol, accession_number, filed_date, event_text, price_date AS entry_date, close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT symbol, accession_number, price_date AS exit_date, close AS exit_close
                FROM joined
                WHERE rn = 4
            )
            SELECT e.filed_date AS report_date,
                   e.symbol,
                   e.accession_number,
                   e.event_text,
                   e.entry_date,
                   x.exit_date,
                   (x.exit_close / e.entry_close - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x
              ON x.symbol = e.symbol
             AND x.accession_number = e.accession_number
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()


def query_cn_forecast_returns(cn_db: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    if not cn_db.exists():
        return []
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not table_exists(con, "forecast") or not table_exists(con, "prices"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH events AS (
                SELECT ts_code, ann_date, forecast_type, p_change_min, p_change_max
                FROM forecast
                WHERE ann_date >= CAST(? AS DATE)
                  AND ann_date <= CAST(? AS DATE)
            ),
            joined AS (
                SELECT e.ts_code, e.ann_date, e.forecast_type, e.p_change_min, e.p_change_max,
                       p.trade_date AS price_date, p.close,
                       ROW_NUMBER() OVER (
                           PARTITION BY e.ts_code, e.ann_date, e.forecast_type
                           ORDER BY p.trade_date
                       ) AS rn
                FROM events e
                JOIN prices p
                  ON p.ts_code = e.ts_code
                 AND p.trade_date > e.ann_date
                 AND p.close > 0
            ),
            entry AS (
                SELECT ts_code, ann_date, forecast_type, p_change_min, p_change_max,
                       price_date AS entry_date, close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT ts_code, ann_date, forecast_type, price_date AS exit_date, close AS exit_close
                FROM joined
                WHERE rn = 6
            )
            SELECT e.ann_date AS report_date,
                   e.ts_code AS symbol,
                   e.forecast_type,
                   e.p_change_min,
                   e.p_change_max,
                   e.entry_date,
                   x.exit_date,
                   (x.exit_close / e.entry_close - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x
              ON x.ts_code = e.ts_code
             AND x.ann_date = e.ann_date
             AND x.forecast_type = e.forecast_type
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()


def query_cn_cb_returns(cn_db: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    if not cn_db.exists():
        return []
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not table_exists(con, "cb_daily"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH signals AS (
                SELECT ts_code, trade_date, close AS signal_close, cb_value, cb_over_rate
                FROM cb_daily
                WHERE trade_date >= CAST(? AS DATE)
                  AND trade_date <= CAST(? AS DATE)
                  AND close > 0
                  AND cb_over_rate IS NOT NULL
            ),
            joined AS (
                SELECT s.ts_code, s.trade_date, s.cb_value, s.cb_over_rate,
                       p.trade_date AS price_date, p.close,
                       ROW_NUMBER() OVER (
                           PARTITION BY s.ts_code, s.trade_date
                           ORDER BY p.trade_date
                       ) AS rn
                FROM signals s
                JOIN cb_daily p
                  ON p.ts_code = s.ts_code
                 AND p.trade_date > s.trade_date
                 AND p.close > 0
            ),
            entry AS (
                SELECT ts_code, trade_date, cb_value, cb_over_rate,
                       price_date AS entry_date, close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT ts_code, trade_date, price_date AS exit_date, close AS exit_close
                FROM joined
                WHERE rn = 6
            )
            SELECT e.trade_date AS report_date,
                   e.ts_code AS symbol,
                   e.cb_value,
                   e.cb_over_rate,
                   e.entry_date,
                   x.exit_date,
                   (x.exit_close / e.entry_close - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x
              ON x.ts_code = e.ts_code
             AND x.trade_date = e.trade_date
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()


def load_cn_log_overlay_rows(cn_db: Path, start: date, as_of: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    if pd is None:
        return [], [], "missing pandas"
    try:
        import run_cn_log_denoise_backtest as cn_log
    except ImportError as exc:
        return [], [], f"missing cn log script: {exc}"

    strategy = cn_log.load_strategy_rows(cn_db, start, as_of)
    if strategy.empty:
        return [], [], "no strategy rows"
    symbols = sorted(strategy["symbol"].dropna().astype(str).unique())
    prices = cn_log.load_prices(cn_db, symbols, start - timedelta(days=140), as_of)
    features = cn_log.compute_log_features(prices)
    merged = cn_log.merge_strategy_with_features(strategy, features)
    if "feature_lag_days" in merged.columns:
        merged = merged[pd.to_numeric(merged["feature_lag_days"], errors="coerce") <= 7]
    merged = cn_log.add_holding_days(cn_log.dedupe_strategy_rows(merged))
    all_oversold = merged[merged["strategy_family"] == "oversold_contrarian"].copy()
    ev_positive = all_oversold[
        (all_oversold["alpha_state"] == "positive_ev_setup")
        | (pd.to_numeric(all_oversold["ev_lcb_80_pct"], errors="coerce") > 0)
    ].copy()
    residual = ev_positive[pd.to_numeric(ev_positive["denoise_residual_zscore"], errors="coerce") <= -1.5].copy()
    deep_log = all_oversold[pd.to_numeric(all_oversold["log_return_20d_pct"], errors="coerce") <= -20.0].copy()
    return records_from_df(residual), records_from_df(deep_log), "ok"


def records_from_df(frame: Any) -> list[dict[str, Any]]:
    if pd is None or frame is None or frame.empty:
        return []
    out = frame.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    out = out.replace({float("nan"): None})
    records: list[dict[str, Any]] = []
    for row in out.to_dict(orient="records"):
        records.append(
            {
                "report_date": v2.as_iso(row.get("report_date")),
                "symbol": row.get("symbol"),
                "return_pct": v2.round_or_none(row.get("return_pct")),
            }
        )
    return records


def build_sleeves(
    us_db: Path,
    cn_db: Path,
    factor_lab_db: Path,
    start: date,
    as_of: date,
    min_money_n: int,
) -> list[Sleeve]:
    sleeves: list[Sleeve] = []

    us_rows, us_status = v2.load_us_rows(us_db, start, as_of)
    us_v2 = v2.rows_with_return_cost([row for row in us_rows if v2.is_us_v2_policy(row)], v2.US_STOCK_ROUNDTRIP_COST_PCT)
    us_legacy = [row for row in us_rows if v2.is_us_legacy_policy(row)]
    option_ledger = v2.build_option_shadow_ledger(us_db, start, as_of) if us_db.exists() else {"rows": []}
    option_rows = [
        {"report_date": row.get("report_date"), "symbol": row.get("symbol"), "return_pct": row.get("return_pct")}
        for row in option_ledger.get("rows", [])
        if row.get("resolved") and row.get("long_expression") and row.get("return_pct") is not None
    ]
    filing_rows = query_us_sec_filing_returns(us_db, start, as_of)
    material_filing_rows = [
        row
        for row in filing_rows
        if "item 1.01" in str(row.get("event_text") or "")
        or "material definitive agreement" in str(row.get("event_text") or "")
    ]

    sleeves.append(
        make_sleeve(
            sleeve_id="us_v2_stock_probe",
            market="us",
            label="US V2 stock-only probe net",
            signal_rule="LOW/core/executable_now/trending, underlying 3-session return minus stock cost",
            horizon="3 sessions",
            data_status=us_status,
            role="probe",
            notes="Bridge sleeve while options expression ledger is still thin.",
            rows=us_v2,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="us_legacy_high_mod",
            market="us",
            label="US legacy HIGH/MOD baseline",
            signal_rule="legacy core long HIGH/MODERATE executable_now",
            horizon="3 sessions",
            data_status=us_status,
            role="baseline",
            notes="Baseline only; not a fresh-entry policy.",
            rows=us_legacy,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="us_option_shadow_long",
            market="us",
            label="US option shadow long expressions",
            signal_rule="V2 rows with stock_long/call_spread expression marked by bid/ask or proxy",
            horizon="3 sessions",
            data_status=f"resolved={option_ledger.get('resolved_count', 0)} unresolved={option_ledger.get('unresolved_count', 0)}",
            role="shadow",
            notes="Shadow-only until resolved sample, LCB80, liquidity, and live slippage pass.",
            rows=option_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="us_sec_8k_material_agreement",
            market="us",
            label="US SEC 8-K material-agreement diagnostic",
            signal_rule="8-K Item 1.01 / material definitive agreement, next tradable close to 3-session exit",
            horizon="3 sessions",
            data_status="diagnostic_only_sec_summary",
            role="research",
            notes="Needs document parser/payoff table before becoming event alpha.",
            rows=material_filing_rows,
            min_money_n=min_money_n,
        )
    )

    cn_rows, cn_status = v2.load_cn_strategy_rows(cn_db, start, as_of)
    cn_oversold_all = [row for row in cn_rows if row.get("strategy_family") == "oversold_contrarian"]
    cn_oversold_ev = [
        row
        for row in cn_oversold_all
        if row.get("alpha_state") == "positive_ev_setup" or (v2.round_or_none(row.get("ev_lcb_80_pct")) or 0.0) > 0.0
    ]
    cn_legacy = [row for row in cn_rows if row.get("strategy_family") == "structural_core"]
    residual_rows, deep_log_rows, log_status = load_cn_log_overlay_rows(cn_db, start, as_of)
    forecast_rows = query_cn_forecast_returns(cn_db, start, as_of)
    positive_forecast = [
        row
        for row in forecast_rows
        if str(row.get("forecast_type") or "") in {"预增", "扭亏", "略增", "续盈"}
    ]
    negative_forecast = [
        row
        for row in forecast_rows
        if str(row.get("forecast_type") or "") in {"预减", "首亏", "略减", "续亏"}
    ]
    cb_rows = query_cn_cb_returns(cn_db, start, as_of)
    cb_low_premium = [row for row in cb_rows if (v2.round_or_none(row.get("cb_over_rate")) or 999.0) <= 5.0]
    cb_high_premium = [row for row in cb_rows if (v2.round_or_none(row.get("cb_over_rate")) or -999.0) >= 30.0]

    sleeves.append(
        make_sleeve(
            sleeve_id="cn_oversold_ev_positive",
            market="cn",
            label="CN oversold_contrarian EV-positive",
            signal_rule="oversold_contrarian with positive EV LCB80 / positive_ev_setup",
            horizon="T+1 to T+5 lifecycle",
            data_status=cn_status,
            role="money",
            notes="Current CN mainline; still clipped by T+1, high-vol, no-chase, and book overlay.",
            rows=cn_oversold_ev,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_oversold_residual_z_action",
            market="cn",
            label="CN EV+ residual_z <= -1.5 action overlay",
            signal_rule="EV-positive oversold plus causal log residual stretch <= -1.5",
            horizon="T+1 to T+5 lifecycle",
            data_status=log_status,
            role="overlay",
            notes="Report action overlay in opportunity mode; EV fields are context, not a hard gate.",
            rows=residual_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_oversold_deep_log20_setup",
            market="cn",
            label="CN all oversold log20 <= -20 setup overlay",
            signal_rule="all oversold_contrarian with causal 20D log return <= -20%",
            horizon="T+1 to T+5 lifecycle",
            data_status=log_status,
            role="overlay",
            notes="Setup overlay in opportunity mode; use for pullback/retest priority.",
            rows=deep_log_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_legacy_structural_core",
            market="cn",
            label="CN legacy structural_core baseline",
            signal_rule="legacy structural_core/high_mod baseline",
            horizon="T+1/T+2 report outcome",
            data_status=cn_status,
            role="baseline",
            notes="Baseline only; not the default main strategy.",
            rows=cn_legacy,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_forecast_positive_event",
            market="cn",
            label="CN positive earnings forecast event diagnostic",
            signal_rule="预增/扭亏/略增/续盈, next tradable close to T+5 exit",
            horizon="T+5",
            data_status="diagnostic_only_forecast_table",
            role="research",
            notes="Needs announcement timestamp, eligibility, and event payoff table before money use.",
            rows=positive_forecast,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_forecast_negative_raw_long",
            market="cn",
            label="CN negative forecast raw-long avoid diagnostic",
            signal_rule="预减/首亏/略减/续亏, raw long return for avoid/risk study",
            horizon="T+5",
            data_status="diagnostic_only_forecast_table",
            role="research",
            notes="Risk/avoid diagnostic; A-share long-only report should not short from this.",
            rows=negative_forecast,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_cb_low_premium_rv",
            market="cn",
            label="CN convertible low-premium RV diagnostic",
            signal_rule="cb_daily cb_over_rate <= 5%, next tradable close to T+5 exit",
            horizon="T+5",
            data_status="diagnostic_only_cb_daily",
            role="research",
            notes="Needs terms, forced redemption, putback, conversion window, and liquidity fields before money use.",
            rows=cb_low_premium,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_cb_high_premium_raw_long",
            market="cn",
            label="CN convertible high-premium raw-long risk diagnostic",
            signal_rule="cb_daily cb_over_rate >= 30%, raw long return for risk study",
            horizon="T+5",
            data_status="diagnostic_only_cb_daily",
            role="research",
            notes="Risk/avoid diagnostic; high premium can be sentiment exposure, not alpha proof.",
            rows=cb_high_premium,
            min_money_n=min_money_n,
        )
    )
    sleeves.extend(load_factor_lab_sleeves(factor_lab_db, start, as_of, min_money_n))
    return sleeves


def build_correlation_payload(sleeves: list[Sleeve]) -> dict[str, Any]:
    series = {s.sleeve_id: daily_series(s.rows) for s in sleeves}
    matrix: list[dict[str, Any]] = []
    pair_values: list[float] = []
    for left in sleeves:
        for right in sleeves:
            corr = 1.0 if left.sleeve_id == right.sleeve_id else pearson_corr(series[left.sleeve_id], series[right.sleeve_id])
            matrix.append(
                {
                    "sleeve_a": left.sleeve_id,
                    "sleeve_b": right.sleeve_id,
                    "corr": v2.round_or_none(corr),
                    "overlap_days": len(set(series[left.sleeve_id]) & set(series[right.sleeve_id])),
                }
            )
            if left.sleeve_id < right.sleeve_id and corr is not None:
                pair_values.append(abs(corr))
    avg_abs_corr = statistics.fmean(pair_values) if pair_values else None
    n = len([s for s in sleeves if daily_series(s.rows)])
    n_eff = None
    if n > 0:
        rho = avg_abs_corr if avg_abs_corr is not None else 0.0
        n_eff = n / (1.0 + (n - 1) * rho)
    return {
        "matrix": matrix,
        "avg_abs_corr": v2.round_or_none(avg_abs_corr),
        "n_eff_all": v2.round_or_none(n_eff),
    }


def combo_rows_from_daily(series_by_id: dict[str, dict[str, float]], ids: list[str]) -> list[dict[str, Any]]:
    all_dates = sorted(set().union(*(set(series_by_id.get(sid, {})) for sid in ids))) if ids else []
    rows: list[dict[str, Any]] = []
    for dt in all_dates:
        values = [series_by_id[sid][dt] for sid in ids if dt in series_by_id.get(sid, {})]
        if values:
            rows.append({"report_date": dt, "symbol": "combo", "return_pct": statistics.fmean(values)})
    return rows


def enrich_relationship_metrics(metrics: list[dict[str, Any]], sleeves: list[Sleeve], correlations: dict[str, Any]) -> None:
    matrix = correlations.get("matrix", [])
    max_corr: dict[str, float | None] = {row["sleeve_id"]: None for row in metrics}
    for row in matrix:
        left = row.get("sleeve_a")
        right = row.get("sleeve_b")
        corr = v2.round_or_none(row.get("corr"))
        if left == right or corr is None:
            continue
        for sid in (left, right):
            prior = max_corr.get(sid)
            max_corr[sid] = abs(corr) if prior is None else max(prior, abs(corr))

    series_by_id = {s.sleeve_id: daily_series(s.rows) for s in sleeves}
    eligible_ids = [
        row["sleeve_id"]
        for row in metrics
        if row["money_status"] in {"money_candidate", "stock_probe_only"}
    ]

    for row in metrics:
        sid = row["sleeve_id"]
        row["max_abs_corr"] = v2.round_or_none(max_corr.get(sid))
        base_ids = [item for item in eligible_ids if item != sid]
        with_ids = list(dict.fromkeys([*base_ids, sid]))
        base_metrics = v2.compute_metrics("base blend", combo_rows_from_daily(series_by_id, base_ids)).to_dict()
        with_metrics = v2.compute_metrics("with sleeve blend", combo_rows_from_daily(series_by_id, with_ids)).to_dict()
        base_sharpe = base_metrics.get("daily_sharpe")
        with_sharpe = with_metrics.get("daily_sharpe")
        if with_sharpe is None:
            row["marginal_daily_sharpe_delta"] = None
        elif base_sharpe is None:
            row["marginal_daily_sharpe_delta"] = v2.round_or_none(with_sharpe)
        else:
            row["marginal_daily_sharpe_delta"] = v2.round_or_none(float(with_sharpe) - float(base_sharpe))


def apply_factor_lab_relationship_gates(metrics: list[dict[str, Any]]) -> None:
    """Annotate Factor Lab relationship risks without suppressing opportunities."""
    for row in metrics:
        sleeve_id = str(row.get("sleeve_id") or "")
        if not sleeve_id.startswith("factor_lab_") or row.get("money_status") != "money_candidate":
            continue

        blockers: list[str] = []
        max_corr = v2.round_or_none(row.get("max_abs_corr"))
        marginal = v2.round_or_none(row.get("marginal_daily_sharpe_delta"))
        if max_corr is not None and max_corr >= FACTOR_LAB_CORR_BLOCK_THRESHOLD:
            blockers.append(f"corr>={FACTOR_LAB_CORR_BLOCK_THRESHOLD:.2f}")
        if marginal is not None and marginal <= 0:
            blockers.append("marginal_sharpe<=0")

        if blockers:
            row["notes"] = f"{row.get('notes') or ''}; portfolio_flags={','.join(blockers)}".strip("; ")


def sync_sleeve_statuses_from_metrics(sleeves: list[Sleeve], metrics: list[dict[str, Any]]) -> None:
    by_id = {row["sleeve_id"]: row for row in metrics}
    for sleeve in sleeves:
        row = by_id.get(sleeve.sleeve_id)
        if not row:
            continue
        sleeve.money_status = str(row.get("money_status") or sleeve.money_status)
        sleeve.notes = str(row.get("notes") or sleeve.notes)


def build_combo_payload(sleeves: list[Sleeve]) -> dict[str, Any]:
    eligible = [
        sleeve
        for sleeve in sleeves
        if sleeve.money_status in {"money_candidate", "stock_probe_only"}
    ]
    per_sleeve = {s.sleeve_id: daily_series(s.rows) for s in eligible}
    all_dates = sorted(set().union(*(set(values) for values in per_sleeve.values()))) if per_sleeve else []
    combo_rows: list[dict[str, Any]] = []
    for dt in all_dates:
        values = [series[dt] for series in per_sleeve.values() if dt in series]
        if values:
            combo_rows.append({"report_date": dt, "symbol": "combo", "return_pct": statistics.fmean(values)})
    metrics = v2.compute_metrics("Equal-weight viable sleeve daily blend", combo_rows).to_dict()
    return {
        "eligible_sleeves": [s.sleeve_id for s in eligible],
        "metrics": metrics,
        "daily_returns": daily_series(combo_rows),
    }


def render_metrics_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Sleeve | Market | n | Days | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | Max corr | Marginal Sharpe | Top5 PnL | Mean breadth | Money status | Data status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['market']} | {row['n']} | {row['active_dates']} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_pct((row.get('win_rate') or 0.0) * 100.0) if row.get('win_rate') is not None else '-'} | "
            f"{fmt_num(row.get('trade_sharpe'), 2)} | {fmt_num(row.get('daily_sharpe'), 2)} | "
            f"{fmt_num(row.get('max_abs_corr'), 2)} | {fmt_num(row.get('marginal_daily_sharpe_delta'), 2)} | "
            f"{fmt_pct((row.get('top5_pnl_share') or 0.0) * 100.0) if row.get('top5_pnl_share') is not None else '-'} | "
            f"{fmt_num(row.get('mean_daily_breadth'), 1)} | {row['money_status']} | {row['data_status']} |"
        )
    return lines + [""]


def render_factor_lab_table(rows: list[dict[str, Any]], correlations: dict[str, Any]) -> list[str]:
    factor_rows = [row for row in rows if str(row.get("sleeve_id") or "").startswith("factor_lab_")]
    if not factor_rows:
        return []
    lines = [
        "## Factor Lab Promoted Factor Sleeves",
        "",
        "| Factor sleeve | Market | n | LCB80 | Daily Sharpe | Max corr | N_eff context | Marginal Sharpe | Money status | Notes |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in factor_rows:
        lines.append(
            f"| {row['label']} | {row['market']} | {row['n']} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_num(row.get('max_abs_corr'), 2)} | "
            f"{fmt_num(correlations.get('n_eff_all'), 2)} | {fmt_num(row.get('marginal_daily_sharpe_delta'), 2)} | "
            f"{row['money_status']} | {row.get('notes') or '-'} |"
        )
    return lines + [""]


def render_corr_matrix(sleeves: list[dict[str, Any]], correlations: dict[str, Any]) -> list[str]:
    ids = [row["sleeve_id"] for row in sleeves]
    labels = {row["sleeve_id"]: row["sleeve_id"] for row in sleeves}
    corr_map = {
        (row["sleeve_a"], row["sleeve_b"]): row.get("corr")
        for row in correlations.get("matrix", [])
    }
    lines = ["| Sleeve | " + " | ".join(ids) + " |", "|---|" + "|".join(["---:"] * len(ids)) + "|"]
    for left in ids:
        cells = [labels[left]]
        for right in ids:
            cells.append(fmt_num(corr_map.get((left, right)), 2))
        lines.append("| " + " | ".join(cells) + " |")
    return lines + [""]


def render_report(payload: dict[str, Any]) -> str:
    metrics = payload["sleeves"]
    money = [row for row in metrics if row["money_status"] in {"money_candidate", "stock_probe_only", "report_overlay"}]
    blocked = [
        row
        for row in metrics
        if row["money_status"] in {
            "blocked_negative_or_unproven",
            "blocked_concentrated_pnl",
            "blocked_double_cost_lcb80",
            "blocked_prob_sharpe",
            "blocked_deflated_sharpe",
            "blocked_rolling_oos",
            "blocked_factor_lab_portfolio_gate",
            "no_data",
        }
    ]
    combo = payload["combo"]["metrics"]
    if money:
        conclusion = (
            f"{len(money)} sleeves have positive money/report evidence; equal-weight viable daily blend "
            f"LCB80 {fmt_pct(combo.get('lcb80_pct'))}, daily Sharpe {fmt_num(combo.get('daily_sharpe'), 2)}."
        )
    else:
        conclusion = "No current opportunity sleeve has usable rows; research-only diagnostics stay informational."
    lines = [
        f"# Alpha Factory Sleeve Backtest - {payload['as_of']}",
        "",
        f"**One-line conclusion:** {conclusion}",
        "",
        "## Coverage",
        "",
        f"- Range: `{payload['start']}` to `{payload['as_of']}`.",
        f"- Sleeves evaluated: `{len(metrics)}`.",
        f"- Average absolute sleeve correlation: `{fmt_num(payload['correlations'].get('avg_abs_corr'), 2)}`.",
        f"- Effective independent sleeve count N_eff: `{fmt_num(payload['correlations'].get('n_eff_all'), 2)}`.",
        f"- Viable money/probe blend sleeves: `{', '.join(payload['combo'].get('eligible_sleeves') or []) or '-'}`.",
        "",
        "## Sleeve Scorecard",
        "",
    ]
    lines += render_metrics_table(metrics)
    lines += render_factor_lab_table(metrics, payload["correlations"])
    lines += [
        "## Correlation Budget",
        "",
        "Correlation is computed on same-date average sleeve returns. Sparse event diagnostics with few overlapping dates should be read as research context, not optimized weights.",
        "",
    ]
    lines += render_corr_matrix(metrics, payload["correlations"])
    lines += [
        "## Equal-Weight Viable Sleeve Blend",
        "",
        "This blend excludes report overlays so the CN mainline is not double-counted against its own explanatory filters.",
        "The blend is a short-window diagnostic, not a production allocation; size still follows each sleeve's money_status and live execution ledger.",
        "",
        "| n | Active days | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | Max DD |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| {combo.get('n', 0)} | {combo.get('active_dates', 0)} | {fmt_pct(combo.get('avg_pct'))} | "
        f"{fmt_pct(combo.get('lcb80_pct'))} | "
        f"{fmt_pct((combo.get('win_rate') or 0.0) * 100.0) if combo.get('win_rate') is not None else '-'} | "
        f"{fmt_num(combo.get('trade_sharpe'), 2)} | {fmt_num(combo.get('daily_sharpe'), 2)} | "
        f"{fmt_pct(combo.get('max_drawdown_pct'))} |",
        "",
        "## Money Readiness",
        "",
    ]
    if money:
        for row in money:
            lines.append(
                f"- `{row['sleeve_id']}`: {row['money_status']}; LCB80 {fmt_pct(row.get('lcb80_pct'))}; "
                f"n={row.get('n')}; note={row.get('notes')}"
            )
    else:
        lines.append("- No money-ready sleeve.")
    if blocked:
        lines.append("")
        lines.append("Blocked / no-data sleeves:")
        for row in blocked:
            lines.append(f"- `{row['sleeve_id']}`: {row['money_status']}; data={row['data_status']}; note={row['notes']}")
    lines += [
        "",
        "## Data Gaps",
        "",
        "- US filings: `sec_filings` is only form/item metadata. Tender/merger/CEF alpha needs a document parser and payoff table.",
        "- CN events: `forecast` exists, but cash-choice/tender/absorption-merger terms are not yet normalized into event payoff rows.",
        "- CN convertibles: `cb_daily` has price/value/premium, but lacks forced-redemption, putback, conversion-window, rating, and remaining-size fields.",
        "- Microstructure: no auction/order-book replay is present here, so 1-5D residual stat-arb and limit-up execution stay research/radar.",
        "- Options: US option ledger must move from proxy to true bid/ask leg PnL with enough resolved rows before money use.",
        "",
        "## Commands",
        "",
        "```bash",
        f"python scripts/run_alpha_sleeve_backtest.py --date {payload['as_of']} --start {payload['start']}",
        f"python scripts/run_main_strategy_v2_backtest.py --date {payload['as_of']} --start {payload['start']}",
        f"python scripts/run_cn_log_denoise_backtest.py --date {payload['as_of']} --start {payload['start']}",
        "```",
        "",
    ]
    return "\n".join(lines)


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE alpha_sleeve_metrics (
                as_of DATE, start_date DATE, sleeve_id VARCHAR, market VARCHAR, label VARCHAR,
                signal_rule VARCHAR, horizon VARCHAR, n INTEGER, active_dates INTEGER,
                avg_pct DOUBLE, median_pct DOUBLE, win_rate DOUBLE, lcb80_pct DOUBLE,
                lcb95_pct DOUBLE, trade_sharpe DOUBLE, daily_sharpe DOUBLE,
                max_drawdown_pct DOUBLE, top5_pnl_share DOUBLE, mean_daily_breadth DOUBLE,
                max_abs_corr DOUBLE, marginal_daily_sharpe_delta DOUBLE,
                money_status VARCHAR, data_status VARCHAR, notes VARCHAR
            )
            """
        )
        metric_rows = []
        for row in payload["sleeves"]:
            metric_rows.append(
                [
                    payload["as_of"],
                    payload["start"],
                    row.get("sleeve_id"),
                    row.get("market"),
                    row.get("label"),
                    row.get("signal_rule"),
                    row.get("horizon"),
                    row.get("n"),
                    row.get("active_dates"),
                    row.get("avg_pct"),
                    row.get("median_pct"),
                    row.get("win_rate"),
                    row.get("lcb80_pct"),
                    row.get("lcb95_pct"),
                    row.get("trade_sharpe"),
                    row.get("daily_sharpe"),
                    row.get("max_drawdown_pct"),
                    row.get("top5_pnl_share"),
                    row.get("mean_daily_breadth"),
                    row.get("max_abs_corr"),
                    row.get("marginal_daily_sharpe_delta"),
                    row.get("money_status"),
                    row.get("data_status"),
                    row.get("notes"),
                ]
            )
        con.executemany(
            "INSERT INTO alpha_sleeve_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metric_rows,
        )
        con.execute(
            """
            CREATE TABLE alpha_sleeve_correlation (
                as_of DATE, sleeve_a VARCHAR, sleeve_b VARCHAR, corr DOUBLE, overlap_days INTEGER
            )
            """
        )
        con.executemany(
            "INSERT INTO alpha_sleeve_correlation VALUES (?, ?, ?, ?, ?)",
            [
                [payload["as_of"], row["sleeve_a"], row["sleeve_b"], row.get("corr"), row.get("overlap_days")]
                for row in payload["correlations"]["matrix"]
            ],
        )
        con.execute(
            """
            CREATE TABLE alpha_sleeve_daily_returns (
                as_of DATE, sleeve_id VARCHAR, return_date DATE, return_pct DOUBLE
            )
            """
        )
        daily_rows = []
        for sleeve_id, series in payload["daily_returns"].items():
            for dt, ret in series.items():
                daily_rows.append([payload["as_of"], sleeve_id, dt, ret])
        con.executemany("INSERT INTO alpha_sleeve_daily_returns VALUES (?, ?, ?, ?)", daily_rows)
    finally:
        con.close()


def alpha_factory_status_for_factor_lab(row: dict[str, Any]) -> tuple[str, list[str]]:
    status = str(row.get("money_status") or "research_only")
    if status == "money_candidate":
        return "pass", []
    if status == "report_overlay":
        return "overlay_allowed", []
    if status == "research_only":
        return "research_only", ["report_contract_research_only"]
    if status == "research_only_sample_thin":
        return "blocked", ["sample_thin"]
    if status == "blocked_negative_or_unproven":
        return "blocked", ["lcb80<=0"]
    if status == "blocked_concentrated_pnl":
        return "blocked", ["top5_pnl_share>30%"]
    if status == "blocked_double_cost_lcb80":
        return "blocked", ["double_cost_lcb80<=0"]
    if status == "blocked_prob_sharpe":
        return "blocked", ["prob_positive<80%"]
    if status == "blocked_deflated_sharpe":
        return "blocked", ["deflated_lcb80<=0"]
    if status == "blocked_rolling_oos":
        return "blocked", ["rolling_oos_min_lcb80<=0"]
    if status == "blocked_factor_lab_portfolio_gate":
        notes = str(row.get("notes") or "")
        marker = "blockers="
        if marker in notes:
            parsed = notes.split(marker, 1)[1].split(";", 1)[0]
            blockers = [item.strip() for item in parsed.split(",") if item.strip()]
            return "blocked", blockers or ["portfolio_gate"]
        return "blocked", ["portfolio_gate"]
    return status, []


def _note_metric(notes: str, name: str) -> float | None:
    marker = f"{name}="
    if marker not in notes:
        return None
    value = notes.split(marker, 1)[1].split(";", 1)[0].strip()
    if value in {"-", ""}:
        return None
    if value.endswith("%"):
        value = value[:-1]
    return v2.round_or_none(value)


def write_factor_lab_money_gate_audit(factor_lab_db: Path, payload: dict[str, Any]) -> None:
    factor_rows = [
        row
        for row in payload.get("sleeves", [])
        if str(row.get("sleeve_id") or "").startswith("factor_lab_")
        and row.get("factor_id")
    ]
    if not factor_rows or not factor_lab_db.exists():
        return

    con = duckdb.connect(str(factor_lab_db))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS factor_money_gate_daily (
                as_of DATE NOT NULL,
                market VARCHAR NOT NULL,
                factor_id VARCHAR NOT NULL,
                sleeve_id VARCHAR NOT NULL,
                report_contract VARCHAR DEFAULT 'research_only',
                money_readiness VARCHAR DEFAULT 'research_only',
                alpha_factory_status VARCHAR NOT NULL,
                money_status VARCHAR NOT NULL,
                n INTEGER,
                lcb80_pct DOUBLE,
                double_cost_lcb80_pct DOUBLE,
                top5_pnl_share DOUBLE,
                max_abs_corr DOUBLE,
                marginal_daily_sharpe_delta DOUBLE,
                blockers_json VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (as_of, market, factor_id)
            )
            """
        )
        audit_rows = []
        registry_updates = []
        for row in factor_rows:
            notes = str(row.get("notes") or "")
            report_contract = "research_only"
            money_readiness = "research_only"
            if "contract=" in notes:
                report_contract = notes.split("contract=", 1)[1].split(";", 1)[0].strip() or report_contract
            if "readiness=" in notes:
                money_readiness = notes.split("readiness=", 1)[1].split(";", 1)[0].strip() or money_readiness
            alpha_status, blockers = alpha_factory_status_for_factor_lab(row)
            if alpha_status in {"overlay_allowed", "pass"} and report_contract != "research_only":
                registry_updates.append(
                    [
                        report_contract,
                        money_readiness,
                        row.get("market"),
                        row.get("factor_id"),
                    ]
                )
            audit_rows.append(
                [
                    payload["as_of"],
                    row.get("market"),
                    row.get("factor_id"),
                    row.get("sleeve_id"),
                    report_contract,
                    money_readiness,
                    alpha_status,
                    row.get("money_status"),
                    row.get("n"),
                    row.get("lcb80_pct"),
                    _note_metric(notes, "double_cost_lcb80"),
                    row.get("top5_pnl_share"),
                    row.get("max_abs_corr"),
                    row.get("marginal_daily_sharpe_delta"),
                    json.dumps(blockers, ensure_ascii=True, sort_keys=True),
                    notes,
                ]
            )
        con.executemany(
            """
            INSERT OR REPLACE INTO factor_money_gate_daily (
                as_of, market, factor_id, sleeve_id, report_contract,
                money_readiness, alpha_factory_status, money_status, n,
                lcb80_pct, double_cost_lcb80_pct, top5_pnl_share, max_abs_corr,
                marginal_daily_sharpe_delta, blockers_json, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            audit_rows,
        )
        if registry_updates:
            try:
                con.executemany(
                    """
                    UPDATE factor_registry
                    SET report_contract=?,
                        money_readiness=?
                    WHERE market=?
                      AND factor_id=?
                      AND status='promoted'
                    """,
                    registry_updates,
                )
            except duckdb.Error:
                pass
    finally:
        con.close()


def run(args: argparse.Namespace) -> dict[str, Any]:
    start = parse_date(args.start)
    as_of = parse_date(args.date) if args.date else latest_report_date(args.us_db, args.cn_db)
    sleeves = build_sleeves(args.us_db, args.cn_db, args.factor_lab_db, start, as_of, args.min_money_n)
    metrics = [sleeve.metrics_dict() for sleeve in sleeves]
    correlations = build_correlation_payload(sleeves)
    enrich_relationship_metrics(metrics, sleeves, correlations)
    apply_factor_lab_relationship_gates(metrics)
    sync_sleeve_statuses_from_metrics(sleeves, metrics)
    combo = build_combo_payload(sleeves)
    payload = {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "sleeves": metrics,
        "correlations": correlations,
        "combo": combo,
        "daily_returns": {s.sleeve_id: daily_series(s.rows) for s in sleeves},
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    output_dir = args.output_root / payload["as_of"]
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "alpha_factory_backtest.md").write_text(render_report(payload), encoding="utf-8")
    (output_dir / "alpha_factory_backtest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    write_duckdb(output_dir / "alpha_factory_backtest.duckdb", payload)
    write_factor_lab_money_gate_audit(args.factor_lab_db, payload)
    return payload


def main() -> None:
    args = parse_args()
    payload = run(args)
    print(
        "Alpha Factory sleeve backtest written: "
        f"{args.output_root / payload['as_of'] / 'alpha_factory_backtest.md'}"
    )


if __name__ == "__main__":
    main()
