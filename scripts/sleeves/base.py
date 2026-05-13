"""Shared alpha sleeve dataclasses and metric helpers."""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

import run_main_strategy_v2_backtest as v2


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
        return "stock_trade"
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
