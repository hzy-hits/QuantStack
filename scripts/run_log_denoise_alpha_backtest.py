#!/usr/bin/env python3
"""Backtest causal log-price denoise filters on US daily report signals.

The goal is to test whether percentage-scale K-line features can recover
"missed alpha" rows that the fresh-entry gate blocks as stale/chase/noisy.
This script recomputes features from historical prices as of each report row,
so it does not depend on newly-added momentum details being present in the old
report JSON.
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))

from quant_bot.analytics.momentum_risk import _log_price_features  # noqa: E402


DEFAULT_START = "2026-03-01"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "log_denoise_alpha"
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
ROUNDTRIP_COST_PCT = 0.15
LCB80_Z = 1.2816
HORIZONS = (3, 5, 10, 20, 30)


@dataclass
class Metric:
    label: str
    horizon: int
    n: int
    active_dates: int
    avg_pct: float | None
    median_pct: float | None
    win_rate: float | None
    lcb80_pct: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "horizon": self.horizon,
            "n": self.n,
            "active_dates": self.active_dates,
            "avg_pct": round_or_none(self.avg_pct, 4),
            "median_pct": round_or_none(self.median_pct, 4),
            "win_rate": round_or_none(self.win_rate, 4),
            "lcb80_pct": round_or_none(self.lcb80_pct, 4),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest US log-denoise missed-alpha filters.")
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--date", default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--cost-pct", type=float, default=ROUNDTRIP_COST_PCT)
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


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


def safe_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
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


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cur = con.execute(sql, params or [])
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def infer_as_of(db_path: Path) -> date:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute("SELECT MAX(report_date) FROM report_decisions").fetchone()
        if not row or row[0] is None:
            return date.today()
        return parse_date(as_iso(row[0]) or "")
    finally:
        con.close()


def load_decisions(db_path: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "report_decisions"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH ranked AS (
                SELECT
                    report_date, session, symbol, selection_status, rank_order,
                    report_bucket, signal_direction, signal_confidence,
                    headline_mode, execution_mode, entry_price, reference_price,
                    stop_price, target_price, rr_ratio, expected_move_pct,
                    primary_reason, details_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY report_date, symbol
                        ORDER BY
                            CASE WHEN session = 'post' THEN 0 ELSE 1 END,
                            CASE WHEN selection_status = 'selected' THEN 0 ELSE 1 END,
                            COALESCE(rank_order, 999999)
                    ) AS rn
                FROM report_decisions
                WHERE report_date >= CAST(? AS DATE)
                  AND report_date <= CAST(? AS DATE)
            )
            SELECT * EXCLUDE (rn)
            FROM ranked
            WHERE rn = 1
            ORDER BY report_date, symbol
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()


def load_price_bars(db_path: Path, symbols: list[str], start: date, as_of: date) -> dict[str, list[dict[str, Any]]]:
    if not symbols:
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "prices_daily"):
            return {}
        placeholders = ",".join("?" for _ in symbols)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT symbol, date, open, close, adj_close
            FROM prices_daily
            WHERE symbol IN ({placeholders})
              AND date >= CAST(? AS DATE)
              AND date <= CAST(? AS DATE)
              AND close IS NOT NULL
            ORDER BY symbol, date
            """,
            [*symbols, (start - timedelta(days=460)).isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()

    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row["symbol"]).upper()
        close = round_or_none(row.get("close"))
        if close is None or close <= 0:
            continue
        open_px = round_or_none(row.get("open")) or close
        adj_close = round_or_none(row.get("adj_close")) or close
        out.setdefault(symbol, []).append(
            {
                "date": parse_date(as_iso(row["date"]) or ""),
                "open": float(open_px),
                "close": float(close),
                "adj_close": float(adj_close),
            }
        )
    return out


def trend_regime(row: dict[str, Any]) -> str:
    details = safe_json_loads(row.get("details_json"))
    return str(
        nested_get(details, "execution_gate", "trend_regime")
        or nested_get(details, "execution_gate", "regime")
        or nested_get(details, "momentum", "regime")
        or "unknown"
    ).lower()


def blockers(row: dict[str, Any]) -> list[str]:
    details = safe_json_loads(row.get("details_json"))
    out: list[str] = []
    main_gate = details.get("main_signal_gate")
    if isinstance(main_gate, dict):
        raw = main_gate.get("blockers") or []
        if isinstance(raw, list):
            out.extend(str(item) for item in raw if item)
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
    rr = round_or_none(row.get("rr_ratio"))
    if rr is not None and rr < 1.5:
        out.append("rr_below_1_5")
    return list(dict.fromkeys(out))


def is_core_long(row: dict[str, Any]) -> bool:
    return (
        str(row.get("report_bucket") or "").lower() == "core"
        and str(row.get("signal_direction") or "").lower() == "long"
    )


def is_v2_strict(row: dict[str, Any]) -> bool:
    return (
        is_core_long(row)
        and str(row.get("signal_confidence") or "").upper() == "LOW"
        and str(row.get("execution_mode") or "").lower() == "executable_now"
        and trend_regime(row) == "trending"
    )


def is_legacy(row: dict[str, Any]) -> bool:
    return (
        is_core_long(row)
        and str(row.get("signal_confidence") or "").upper() in {"HIGH", "MODERATE"}
        and str(row.get("execution_mode") or "").lower() == "executable_now"
    )


def is_missed_like(row: dict[str, Any]) -> bool:
    if not is_core_long(row):
        return False
    text = " ".join(blockers(row)).lower()
    primary = str(row.get("primary_reason") or "").lower()
    regime = trend_regime(row)
    rr = round_or_none(row.get("rr_ratio"))
    return bool(
        "stale" in text
        or "paid" in text
        or "exhaustion" in text
        or "chase" in text
        or (rr is not None and rr < 1.5)
        or regime in {"noisy", "mean_reverting"}
        or primary in {"momentum", "magnitude", "cross_asset"}
    )


def attach_features_and_returns(
    decisions: list[dict[str, Any]],
    bars_by_symbol: dict[str, list[dict[str, Any]]],
    cost_pct: float,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    date_cache = {
        symbol: [bar["date"] for bar in bars]
        for symbol, bars in bars_by_symbol.items()
    }
    for row in decisions:
        symbol = str(row.get("symbol") or "").upper()
        bars = bars_by_symbol.get(symbol) or []
        dates = date_cache.get(symbol) or []
        report_date = parse_date(as_iso(row.get("report_date")) or "")
        session = str(row.get("session") or "").lower()
        if session == "pre":
            feature_idx = bisect.bisect_left(dates, report_date) - 1
        else:
            feature_idx = bisect.bisect_right(dates, report_date) - 1
        if feature_idx < 31:
            continue
        entry_idx = feature_idx + 1
        if entry_idx >= len(bars):
            continue

        closes = np.array([bar["adj_close"] for bar in bars[: feature_idx + 1]], dtype=float)
        features = _log_price_features(closes)
        entry = bars[entry_idx]["open"] or bars[entry_idx]["close"]
        if not entry or entry <= 0:
            continue

        out = dict(row)
        out["symbol"] = symbol
        out["report_date"] = report_date.isoformat()
        out["feature_cutoff_date"] = bars[feature_idx]["date"].isoformat()
        out["entry_date"] = bars[entry_idx]["date"].isoformat()
        out["entry_price"] = entry
        out["trend_regime"] = trend_regime(row)
        out["blockers"] = blockers(row)
        out["is_core_long"] = is_core_long(row)
        out["is_v2_strict"] = is_v2_strict(row)
        out["is_legacy"] = is_legacy(row)
        out["is_missed_like"] = is_missed_like(row)
        for key, value in features.items():
            out[key] = round_or_none(value, 6)
        for horizon in HORIZONS:
            exit_idx = entry_idx + horizon - 1
            key = f"ret_{horizon}d_net_pct"
            gross_key = f"ret_{horizon}d_gross_pct"
            if exit_idx >= len(bars):
                out[key] = None
                out[gross_key] = None
                continue
            exit_px = bars[exit_idx]["close"]
            gross = (exit_px / entry - 1.0) * 100.0
            out[gross_key] = round_or_none(gross, 6)
            out[key] = round_or_none(gross - cost_pct, 6)
        enriched.append(out)
    return enriched


def denoise_pass(row: dict[str, Any], slope_min: float, snr_min: float, noise_max: float) -> bool:
    slope = round_or_none(row.get("denoised_log_slope_10d_pct"))
    snr = round_or_none(row.get("fft_signal_to_noise"))
    noise = round_or_none(row.get("haar_noise_energy"))
    return bool(
        slope is not None
        and snr is not None
        and noise is not None
        and slope > slope_min
        and snr >= snr_min
        and noise <= noise_max
    )


def metric(label: str, rows: list[dict[str, Any]], horizon: int) -> Metric:
    key = f"ret_{horizon}d_net_pct"
    values: list[float] = []
    by_date: dict[str, list[float]] = {}
    for row in rows:
        value = round_or_none(row.get(key))
        if value is None:
            continue
        values.append(value)
        by_date.setdefault(str(row.get("report_date")), []).append(value)
    if not values:
        return Metric(label, horizon, 0, 0, None, None, None, None)
    avg = statistics.fmean(values)
    med = statistics.median(values)
    win = sum(1 for value in values if value > 0) / len(values)
    if len(values) == 1:
        lcb80 = avg
    else:
        lcb80 = avg - LCB80_Z * statistics.stdev(values) / math.sqrt(len(values))
    return Metric(label, horizon, len(values), len(by_date), avg, med, win, lcb80)


def metric_pack(label: str, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {f"{h}d": metric(label, rows, h).to_dict() for h in HORIZONS}


def split_cutoff(rows: list[dict[str, Any]], horizon: int | None = None) -> date:
    if horizon is None:
        resolved = rows
    else:
        key = f"ret_{horizon}d_net_pct"
        resolved = [row for row in rows if round_or_none(row.get(key)) is not None]
    dates = sorted({parse_date(str(row["report_date"])) for row in resolved})
    if not dates:
        return date.today()
    idx = min(len(dates) - 1, max(0, int(len(dates) * 0.70) - 1))
    return dates[idx]


def grid_search(rows: list[dict[str, Any]], train_cutoff: date, horizon: int = 20) -> list[dict[str, Any]]:
    slope_grid = [0.0, 0.03, 0.05, 0.08, 0.10, 0.15]
    snr_grid = [0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
    noise_grid = [0.25, 0.35, 0.45, 0.55, 0.65]
    out: list[dict[str, Any]] = []
    for slope in slope_grid:
        for snr in snr_grid:
            for noise in noise_grid:
                passed = [row for row in rows if denoise_pass(row, slope, snr, noise)]
                train = [row for row in passed if parse_date(str(row["report_date"])) <= train_cutoff]
                test = [row for row in passed if parse_date(str(row["report_date"])) > train_cutoff]
                train_m = metric("train", train, horizon).to_dict()
                test_m = metric("test", test, horizon).to_dict()
                if train_m["n"] < 8:
                    continue
                out.append(
                    {
                        "slope_min": slope,
                        "fft_snr_min": snr,
                        "haar_noise_max": noise,
                        "train": train_m,
                        "test": test_m,
                    }
                )
    return sorted(
        out,
        key=lambda row: (
            row["train"].get("lcb80_pct") if row["train"].get("lcb80_pct") is not None else -999.0,
            row["train"].get("avg_pct") if row["train"].get("avg_pct") is not None else -999.0,
            row["test"].get("n") or 0,
        ),
        reverse=True,
    )


def choose_research_filter(best_by_horizon: dict[int, dict[str, Any] | None]) -> tuple[int | None, dict[str, Any] | None]:
    """Pick a diagnostic filter; robust OOS filters rank first, then 5D/3D average evidence."""
    robust: list[tuple[float, int, dict[str, Any]]] = []
    positive_avg: list[tuple[float, int, dict[str, Any]]] = []
    for horizon, row in best_by_horizon.items():
        if not row:
            continue
        test = row.get("test") or {}
        train = row.get("train") or {}
        test_n = int(test.get("n") or 0)
        train_n = int(train.get("n") or 0)
        test_lcb = round_or_none(test.get("lcb80_pct"))
        train_lcb = round_or_none(train.get("lcb80_pct"))
        test_avg = round_or_none(test.get("avg_pct"))
        if test_n >= 8 and train_n >= 8 and test_lcb is not None and train_lcb is not None and test_lcb > 0 and train_lcb > 0:
            robust.append((test_lcb, -horizon, row))
        if horizon in {3, 5, 10} and test_n >= 8 and test_avg is not None and test_avg > 0:
            positive_avg.append((test_avg, -horizon, row))
    if robust:
        best = sorted(robust, reverse=True)[0]
        return -best[1], best[2]
    if positive_avg:
        best = sorted(positive_avg, reverse=True)[0]
        return -best[1], best[2]
    for horizon in (5, 3, 10, 20, 30):
        if best_by_horizon.get(horizon):
            return horizon, best_by_horizon[horizon]
    return None, None


def select_current(rows: list[dict[str, Any]], slope: float, snr: float, noise: float, as_of: date) -> list[dict[str, Any]]:
    latest = max((parse_date(str(row["report_date"])) for row in rows), default=as_of)
    current = [
        row
        for row in rows
        if parse_date(str(row["report_date"])) == latest
        and is_core_long(row)
        and denoise_pass(row, slope, snr, noise)
    ]
    return sorted(current, key=lambda row: (not row.get("is_missed_like"), str(row["symbol"])))[:30]


def compact_row(row: dict[str, Any]) -> dict[str, Any]:
    keep = [
        "report_date",
        "session",
        "symbol",
        "selection_status",
        "report_bucket",
        "signal_confidence",
        "execution_mode",
        "rr_ratio",
        "primary_reason",
        "trend_regime",
        "denoised_log_slope_10d_pct",
        "fft_signal_to_noise",
        "haar_noise_energy",
        "ret_10d_net_pct",
        "ret_20d_net_pct",
        "ret_30d_net_pct",
        "is_v2_strict",
        "is_legacy",
        "is_missed_like",
        "blockers",
    ]
    return {key: row.get(key) for key in keep}


def render_metric_table(metrics: dict[str, dict[str, Any]], label: str) -> list[str]:
    lines = [
        f"### {label}",
        "",
        "| Horizon | n | active dates | avg | LCB80 | win | median |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for horizon in ["3d", "5d", "10d", "20d", "30d"]:
        row = metrics.get(horizon) or {}
        lines.append(
            f"| {horizon.upper()} | {row.get('n', 0)} | {row.get('active_dates', 0)} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_num((row.get('win_rate') or 0) * 100, 1)}% | {fmt_pct(row.get('median_pct'))} |"
        )
    lines.append("")
    return lines


def render_report(payload: dict[str, Any]) -> str:
    best = payload.get("best_grid") or {}
    current = payload.get("current_candidates") or []
    conclusion = payload.get("conclusion")
    lines = [
        f"# Log-Denoise Alpha Backtest - {payload['as_of']}",
        "",
        f"**One-line conclusion:** {conclusion}",
        "",
        "This is a research backtest, not a live-money gate. Features are recomputed from historical K lines as of each report row; returns are next-session open to future close, net of stock roundtrip cost.",
        "",
        "## Setup",
        "",
        f"- Range: `{payload['start']}` to `{payload['as_of']}`",
        f"- Train/test cutoff: `{payload['train_cutoff']}`",
        f"- Roundtrip cost: `{payload['cost_pct']:.2f}%`",
        f"- Dedupe: one row per report date and symbol, `post` preferred over `pre`",
        f"- Rows enriched: `{payload['row_count']}`; core-long rows `{payload['core_long_count']}`; missed-like rows `{payload['missed_like_count']}`",
        "",
        "## Selected Research Filter",
        "",
    ]
    if best:
        lines += [
            f"- Selection horizon: `{payload.get('best_grid_horizon')}D`",
            f"- `denoised_log_slope_10d_pct > {best['slope_min']}`",
            f"- `fft_signal_to_noise >= {best['fft_snr_min']}`",
            f"- `haar_noise_energy <= {best['haar_noise_max']}`",
            "",
            "| Split | n | avg | LCB80 | win |",
            "|---|---:|---:|---:|---:|",
            (
                f"| Train | {best['train'].get('n', 0)} | {fmt_pct(best['train'].get('avg_pct'))} | "
                f"{fmt_pct(best['train'].get('lcb80_pct'))} | {fmt_num((best['train'].get('win_rate') or 0) * 100, 1)}% |"
            ),
            (
                f"| Test | {best['test'].get('n', 0)} | {fmt_pct(best['test'].get('avg_pct'))} | "
                f"{fmt_pct(best['test'].get('lcb80_pct'))} | {fmt_num((best['test'].get('win_rate') or 0) * 100, 1)}% |"
            ),
            "",
        ]
    else:
        lines += ["No threshold set had enough train samples.", ""]

    lines += [
        "## Horizon Grid Check",
        "",
        "| Horizon | train n | train avg | train LCB80 | test n | test avg | test LCB80 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for horizon in ["3", "5", "10", "20", "30"]:
        row = (payload.get("best_by_horizon") or {}).get(horizon)
        if not row:
            lines.append(f"| {horizon}D | 0 | - | - | 0 | - | - |")
            continue
        train = row.get("train") or {}
        test = row.get("test") or {}
        lines.append(
            f"| {horizon}D | {train.get('n', 0)} | {fmt_pct(train.get('avg_pct'))} | "
            f"{fmt_pct(train.get('lcb80_pct'))} | {test.get('n', 0)} | "
            f"{fmt_pct(test.get('avg_pct'))} | {fmt_pct(test.get('lcb80_pct'))} |"
        )
    lines.append("")

    lines += render_metric_table(payload["metrics"]["v2_strict"], "Current V2 Strict Baseline")
    lines += render_metric_table(payload["metrics"]["legacy_core_long"], "Legacy HIGH/MOD Core Baseline")
    lines += render_metric_table(payload["metrics"]["denoise_core_long"], "Denoise Filter On Core Long")
    lines += render_metric_table(payload["metrics"]["denoise_missed_like"], "Denoise Filter On Missed-Like Rows")

    lines += [
        "## MU / INTC Case Study",
        "",
        "| Date | Symbol | Conf | RR | Regime | slope10 | FFT S/N | Haar noise | 10D | 20D | 30D | Blocked? |",
        "|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload.get("mu_intc_rows", [])[:30]:
        blocked = "yes" if row.get("is_missed_like") and not row.get("is_v2_strict") else "no"
        lines.append(
            f"| {row.get('report_date')} | {row.get('symbol')} | {row.get('signal_confidence') or '-'} | "
            f"{fmt_num(row.get('rr_ratio'), 2)} | {row.get('trend_regime') or '-'} | "
            f"{fmt_num(row.get('denoised_log_slope_10d_pct'), 3)} | "
            f"{fmt_num(row.get('fft_signal_to_noise'), 2)} | "
            f"{fmt_num(row.get('haar_noise_energy'), 2)} | "
            f"{fmt_pct(row.get('ret_10d_net_pct'))} | {fmt_pct(row.get('ret_20d_net_pct'))} | "
            f"{fmt_pct(row.get('ret_30d_net_pct'))} | {blocked} |"
        )
    lines += ["", "## Current Denoise Radar", ""]
    if current:
        lines += [
            "| Symbol | State | Conf | RR | Regime | slope10 | FFT S/N | Haar noise | Fresh-entry read |",
            "|---|---|---|---:|---|---:|---:|---:|---|",
        ]
        for row in current:
            state = "missed-like hold radar" if row.get("is_missed_like") and not row.get("is_v2_strict") else "core-long trend"
            fresh = "no fresh buy override; hold/retest evidence only" if row.get("is_missed_like") else "candidate needs EV gate"
            lines.append(
                f"| {row.get('symbol')} | {state} | {row.get('signal_confidence') or '-'} | "
                f"{fmt_num(row.get('rr_ratio'), 2)} | {row.get('trend_regime') or '-'} | "
                f"{fmt_num(row.get('denoised_log_slope_10d_pct'), 3)} | "
                f"{fmt_num(row.get('fft_signal_to_noise'), 2)} | "
                f"{fmt_num(row.get('haar_noise_energy'), 2)} | {fresh} |"
            )
    else:
        lines.append("No current core-long row passes the best research filter.")
    lines += [
        "",
        "## Policy Read",
        "",
        "- If test LCB80 is positive with enough rows, use this first as `Winner Hold Overlay` / `Missed Alpha Radar` evidence.",
        "- Do not let it bypass `No ticket, no trade` for fresh entries until a walk-forward EV ledger survives more dates.",
        "- If it fails out-of-sample, keep the fields as diagnostics only and do not add another gate.",
        "",
    ]
    return "\n".join(lines)


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE summary (key VARCHAR, value VARCHAR)")
        con.executemany(
            "INSERT INTO summary VALUES (?, ?)",
            [(key, json.dumps(value, ensure_ascii=False, default=str)) for key, value in payload.items() if key != "rows"],
        )
        con.execute(
            """
            CREATE TABLE grid_results (
                horizon INTEGER,
                slope_min DOUBLE,
                fft_snr_min DOUBLE,
                haar_noise_max DOUBLE,
                train_n INTEGER,
                train_avg_pct DOUBLE,
                train_lcb80_pct DOUBLE,
                test_n INTEGER,
                test_avg_pct DOUBLE,
                test_lcb80_pct DOUBLE
            )
            """
        )
        grid_rows = [
            (
                row.get("horizon"),
                row["slope_min"],
                row["fft_snr_min"],
                row["haar_noise_max"],
                row["train"].get("n"),
                row["train"].get("avg_pct"),
                row["train"].get("lcb80_pct"),
                row["test"].get("n"),
                row["test"].get("avg_pct"),
                row["test"].get("lcb80_pct"),
            )
            for row in payload.get("grid_results", [])
        ]
        if grid_rows:
            con.executemany(
                "INSERT INTO grid_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                grid_rows,
            )
        con.execute(
            """
            CREATE TABLE candidate_rows (
                report_date DATE,
                symbol VARCHAR,
                signal_confidence VARCHAR,
                rr_ratio DOUBLE,
                trend_regime VARCHAR,
                denoised_log_slope_10d_pct DOUBLE,
                fft_signal_to_noise DOUBLE,
                haar_noise_energy DOUBLE,
                ret_10d_net_pct DOUBLE,
                ret_20d_net_pct DOUBLE,
                ret_30d_net_pct DOUBLE,
                is_v2_strict BOOLEAN,
                is_legacy BOOLEAN,
                is_missed_like BOOLEAN
            )
            """
        )
        candidate_rows = [
            (
                row.get("report_date"),
                row.get("symbol"),
                row.get("signal_confidence"),
                row.get("rr_ratio"),
                row.get("trend_regime"),
                row.get("denoised_log_slope_10d_pct"),
                row.get("fft_signal_to_noise"),
                row.get("haar_noise_energy"),
                row.get("ret_10d_net_pct"),
                row.get("ret_20d_net_pct"),
                row.get("ret_30d_net_pct"),
                row.get("is_v2_strict"),
                row.get("is_legacy"),
                row.get("is_missed_like"),
            )
            for row in payload.get("rows", [])
        ]
        if candidate_rows:
            con.executemany(
                "INSERT INTO candidate_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                candidate_rows,
            )
    finally:
        con.close()


def main() -> int:
    args = parse_args()
    start = parse_date(args.start)
    as_of = parse_date(args.date) if args.date else infer_as_of(args.us_db)

    decisions = load_decisions(args.us_db, start, as_of)
    symbols = sorted({str(row.get("symbol") or "").upper() for row in decisions if row.get("symbol")})
    bars = load_price_bars(args.us_db, symbols, start, as_of)
    rows = attach_features_and_returns(decisions, bars, args.cost_pct)
    core_long = [row for row in rows if row.get("is_core_long")]
    missed_like = [row for row in rows if row.get("is_missed_like")]
    grids_by_horizon: dict[int, list[dict[str, Any]]] = {}
    best_by_horizon: dict[int, dict[str, Any] | None] = {}
    cutoffs_by_horizon: dict[int, date] = {}
    for horizon in HORIZONS:
        cutoff = split_cutoff(missed_like, horizon=horizon)
        cutoffs_by_horizon[horizon] = cutoff
        grids_by_horizon[horizon] = grid_search(missed_like, cutoff, horizon=horizon)
        best_by_horizon[horizon] = grids_by_horizon[horizon][0] if grids_by_horizon[horizon] else None
    best_horizon, best = choose_research_filter(best_by_horizon)
    train_cutoff = cutoffs_by_horizon.get(best_horizon or 20, split_cutoff(missed_like, horizon=20))
    if best:
        slope = best["slope_min"]
        snr = best["fft_snr_min"]
        noise = best["haar_noise_max"]
    else:
        slope, snr, noise = 0.0, 1.0, 0.45

    denoise_core = [row for row in core_long if denoise_pass(row, slope, snr, noise)]
    denoise_missed = [row for row in missed_like if denoise_pass(row, slope, snr, noise)]
    v2 = [row for row in rows if row.get("is_v2_strict")]
    legacy = [row for row in rows if row.get("is_legacy")]
    current = select_current(rows, slope, snr, noise, as_of)
    mu_intc = [
        row
        for row in denoise_missed
        if row.get("symbol") in {"MU", "INTC"}
    ]
    mu_intc = sorted(mu_intc, key=lambda row: (str(row.get("report_date")), str(row.get("symbol"))), reverse=True)

    test = best.get("test") if best else {}
    train = best.get("train") if best else {}
    test_n = int(test.get("n") or 0)
    test_lcb = round_or_none(test.get("lcb80_pct"))
    train_lcb = round_or_none(train.get("lcb80_pct"))
    if test_n >= 8 and test_lcb is not None and train_lcb is not None and test_lcb > 0 and train_lcb > 0:
        conclusion = (
            "log-denoise filter has positive train/test LCB80; promote first to hold/retest radar, "
            "not fresh-entry auto-buy."
        )
    elif test_n >= 8 and (test.get("avg_pct") or 0.0) > 0:
        conclusion = (
            "log-denoise filter improves short-horizon OOS average returns but LCB80 is not robust; keep as shadow evidence."
        )
    else:
        conclusion = "log-denoise filter is not proven out-of-sample; keep diagnostics only."

    payload = {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "train_cutoff": train_cutoff.isoformat(),
        "cost_pct": args.cost_pct,
        "row_count": len(rows),
        "core_long_count": len(core_long),
        "missed_like_count": len(missed_like),
        "best_grid_horizon": best_horizon,
        "best_grid": best,
        "best_by_horizon": {
            str(horizon): best_by_horizon[horizon]
            for horizon in HORIZONS
        },
        "grid_results": [
            {"horizon": horizon, **row}
            for horizon in HORIZONS
            for row in grids_by_horizon[horizon][:20]
        ][:100],
        "metrics": {
            "v2_strict": metric_pack("v2_strict", v2),
            "legacy_core_long": metric_pack("legacy_core_long", legacy),
            "denoise_core_long": metric_pack("denoise_core_long", denoise_core),
            "denoise_missed_like": metric_pack("denoise_missed_like", denoise_missed),
        },
        "current_candidates": [compact_row(row) for row in current],
        "mu_intc_rows": [compact_row(row) for row in mu_intc],
        "rows": [compact_row(row) for row in denoise_missed],
        "conclusion": conclusion,
    }

    output_dir = args.output_root / as_of.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    md = render_report(payload)
    (output_dir / "log_denoise_alpha_backtest.md").write_text(md, encoding="utf-8")
    (output_dir / "log_denoise_alpha_backtest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    write_duckdb(output_dir / "log_denoise_alpha_backtest.duckdb", payload)
    print(f"Log-denoise alpha backtest written: {output_dir / 'log_denoise_alpha_backtest.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
