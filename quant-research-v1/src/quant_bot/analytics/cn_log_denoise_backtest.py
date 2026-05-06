#!/usr/bin/env python3
"""Backtest CN log-price denoise diagnostics against paper-trade outcomes.

The CN analytics table may not yet contain the latest log/FFT/Haar momentum
rows for historical report dates, so this script recomputes the same causal
features from the prices table and joins them to strategy_model_dataset. The
goal is deliberately narrower than a new strategy: decide which diagnostics
deserve to be wired into the daily report action layer.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import duckdb
import numpy as np
import pandas as pd


STACK_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_START = "2026-03-01"
LCB80_Z = 1.2816
LCB95_Z = 1.6449


@dataclass
class MetricRow:
    label: str
    n: int
    active_dates: int
    avg_pct: float | None
    median_pct: float | None
    win_rate: float | None
    lcb80_pct: float | None
    lcb95_pct: float | None
    avg_mfe_pct: float | None
    avg_mae_pct: float | None
    avg_hold_days: float | None
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
            "avg_mfe_pct": round_or_none(self.avg_mfe_pct),
            "avg_mae_pct": round_or_none(self.avg_mae_pct),
            "avg_hold_days": round_or_none(self.avg_hold_days),
            "std_pct": round_or_none(self.std_pct),
            "trade_sharpe": round_or_none(self.trade_sharpe),
            "daily_sharpe": round_or_none(self.daily_sharpe),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run CN log/FFT/Haar denoise evidence backtest.")
    parser.add_argument("--date", default=None, help="Report date. Defaults to latest strategy_model_dataset date.")
    parser.add_argument("--start", default=DEFAULT_START, help="Backtest start date.")
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-samples", type=int, default=20)
    parser.add_argument("--max-feature-lag-days", type=int, default=7)
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def as_iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def round_or_none(value: Any, digits: int = 6) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def fmt_num(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:+.{digits}f}%"


def fmt_win(value: Any) -> str:
    parsed = round_or_none(value, 4)
    if parsed is None:
        return "-"
    return f"{parsed * 100.0:.1f}%"


def safe_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value or pd.isna(value):
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


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


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def latest_strategy_date(db_path: Path) -> date:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return date.today()
        value = con.execute("SELECT MAX(report_date) FROM strategy_model_dataset").fetchone()[0]
        return parse_date(as_iso(value) or date.today().isoformat())
    finally:
        con.close()


def infer_feature_value(row: pd.Series, key: str) -> Any:
    features = safe_json_loads(row.get("features_json"))
    if key in features:
        return features.get(key)
    detail = safe_json_loads(row.get("detail_json"))
    return detail.get(key)


def load_strategy_rows(db_path: Path, start: date, as_of: date) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return pd.DataFrame()
        return con.execute(
            """
            SELECT
                m.report_date, m.evaluation_date, m.session, m.symbol,
                COALESCE(sb.name, '') AS name,
                COALESCE(sb.industry, '') AS industry,
                m.selection_status, m.strategy_family, m.strategy_key,
                m.execution_rule, m.action_intent, m.alpha_state,
                m.reference_close, m.planned_entry, m.fill_status,
                m.fill_date, m.fill_price, m.exit_date, m.exit_price,
                m.realized_ret_pct AS return_pct,
                m.max_favorable_pct, m.max_adverse_pct,
                m.ev_pct, m.ev_lcb_80_pct, m.ev_lcb_95_pct,
                m.risk_unit_pct, m.ev_norm_score, m.ev_norm_lcb_80,
                m.features_json, m.detail_json
            FROM strategy_model_dataset m
            LEFT JOIN stock_basic sb ON sb.ts_code = m.symbol
            WHERE m.report_date >= CAST(? AS DATE)
              AND m.report_date <= CAST(? AS DATE)
              AND m.action_intent = 'TRADE'
              AND m.realized_ret_pct IS NOT NULL
              AND m.strategy_family IN (
                'oversold_contrarian',
                'structural_core',
                'continuation_breakout',
                'early_accumulation'
              )
            ORDER BY m.report_date, m.symbol, m.strategy_family
            """,
            [start.isoformat(), as_of.isoformat()],
        ).fetchdf()
    finally:
        con.close()


def load_current_rows(db_path: Path, as_of: date) -> tuple[pd.DataFrame, date | None]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return pd.DataFrame(), None
        latest = con.execute(
            "SELECT MAX(report_date) FROM strategy_model_dataset WHERE report_date <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()[0]
        if latest is None:
            return pd.DataFrame(), None
        latest_date = parse_date(as_iso(latest) or as_of.isoformat())
        rows = con.execute(
            """
            SELECT
                m.report_date, m.evaluation_date, m.session, m.symbol,
                COALESCE(sb.name, '') AS name,
                COALESCE(sb.industry, '') AS industry,
                m.selection_status, m.strategy_family, m.strategy_key,
                m.execution_rule, m.action_intent, m.alpha_state,
                m.reference_close, m.planned_entry, m.fill_status,
                m.ev_pct, m.ev_lcb_80_pct, m.ev_lcb_95_pct,
                m.risk_unit_pct, m.ev_norm_score, m.ev_norm_lcb_80,
                m.features_json, m.detail_json
            FROM strategy_model_dataset m
            LEFT JOIN stock_basic sb ON sb.ts_code = m.symbol
            WHERE m.report_date = CAST(? AS DATE)
              AND m.selection_status IN ('selected', 'exploration')
            ORDER BY
              CASE m.alpha_state WHEN 'positive_ev_setup' THEN 0 ELSE 1 END,
              COALESCE(m.ev_lcb_80_pct, -999) DESC,
              m.symbol
            """,
            [latest_date.isoformat()],
        ).fetchdf()
        return rows, latest_date
    finally:
        con.close()


def load_prices(db_path: Path, symbols: list[str], start: date, as_of: date) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("CREATE TEMP TABLE needed_symbols(ts_code VARCHAR)")
        con.executemany("INSERT INTO needed_symbols VALUES (?)", [(symbol,) for symbol in symbols])
        lookback_start = start - timedelta(days=140)
        return con.execute(
            """
            SELECT p.ts_code, p.trade_date, p.close
            FROM prices p
            INNER JOIN needed_symbols s ON s.ts_code = p.ts_code
            WHERE p.trade_date >= CAST(? AS DATE)
              AND p.trade_date <= CAST(? AS DATE)
              AND p.close IS NOT NULL
              AND p.close > 0
            ORDER BY p.ts_code, p.trade_date
            """,
            [lookback_start.isoformat(), as_of.isoformat()],
        ).fetchdf()
    finally:
        con.close()


def mean_np(values: np.ndarray) -> float | None:
    if values.size == 0:
        return None
    out = float(np.mean(values))
    return out if math.isfinite(out) else None


def std_np(values: np.ndarray) -> float | None:
    if values.size == 0:
        return None
    sigma = float(np.std(values))
    if math.isfinite(sigma) and sigma > 1e-12:
        return sigma
    return None


def tail_slope(values: np.ndarray, window: int) -> float | None:
    if values.size < window or window < 2:
        return None
    y = values[-window:]
    if not np.isfinite(y).all():
        return None
    x = np.arange(window, dtype=float)
    dx = x - float(np.mean(x))
    dy = y - float(np.mean(y))
    den = float(np.sum(dx * dx))
    if den <= 0:
        return None
    return float(np.sum(dx * dy) / den)


def ema(values: np.ndarray, span: int) -> np.ndarray:
    if values.size == 0:
        return values
    alpha = 2.0 / (span + 1.0)
    out = np.empty_like(values, dtype=float)
    prev = float(values[0])
    out[0] = prev
    for idx in range(1, values.size):
        prev = alpha * float(values[idx]) + (1.0 - alpha) * prev
        out[idx] = prev
    return out


def spectral_energy(log_rets: np.ndarray, window: int = 32) -> tuple[float | None, float | None, float | None]:
    if log_rets.size < 8:
        return None, None, None
    n = min(window, log_rets.size)
    if n < 8:
        return None, None, None
    tail = np.array(log_rets[-n:], dtype=float)
    if not np.isfinite(tail).all():
        return None, None, None
    tail = tail - float(np.mean(tail))
    powers = np.abs(np.fft.rfft(tail)[1:]) ** 2
    if powers.size == 0:
        return None, None, None
    total = float(np.sum(powers))
    if total <= 1e-18:
        return 0.0, 0.0, None
    split = max(powers.size // 3, 1)
    low = float(np.sum(powers[:split]))
    high = float(np.sum(powers[split:]))
    return low / total, high / total, low / (high + 1e-12)


def haar_energy(log_rets: np.ndarray, window: int = 32) -> tuple[float | None, float | None]:
    if log_rets.size < 8:
        return None, None
    n = min(window, log_rets.size)
    n = 1 << (n.bit_length() - 1)
    if n < 8:
        return None, None
    cur = np.array(log_rets[-n:], dtype=float)
    if not np.isfinite(cur).all():
        return None, None
    cur = cur - float(np.mean(cur))
    total = float(np.sum(cur * cur))
    if total <= 1e-18:
        return 0.0, 0.0
    inv_sqrt2 = 1.0 / math.sqrt(2.0)
    detail_energies: list[float] = []
    while cur.size >= 2:
        pairs = cur.reshape(-1, 2)
        avg = (pairs[:, 0] + pairs[:, 1]) * inv_sqrt2
        detail = (pairs[:, 0] - pairs[:, 1]) * inv_sqrt2
        detail_energies.append(float(np.sum(detail * detail)))
        cur = avg
    noise = sum(detail_energies[:2])
    trend = sum(detail_energies[2:])
    return trend / total, noise / total


def compute_log_features(prices: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    if prices.empty:
        return pd.DataFrame()
    work = prices.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"])
    for symbol, group in work.groupby("ts_code", sort=False):
        group = group.sort_values("trade_date")
        closes = group["close"].astype(float).to_numpy()
        dates = group["trade_date"].to_list()
        log_prices = np.log(np.maximum(closes, 1e-9))
        log_rets_full = np.diff(log_prices)
        ema_log = ema(log_prices, 5)
        residuals = log_prices - ema_log
        for idx, trade_date in enumerate(dates):
            log_rets = log_rets_full[:idx]
            hist_log = log_prices[: idx + 1]
            hist_ema = ema_log[: idx + 1]
            log_return_1d = (hist_log[-1] - hist_log[-2]) * 100.0 if hist_log.size > 1 else None
            log_return_5d = (hist_log[-1] - hist_log[-6]) * 100.0 if hist_log.size > 5 else None
            log_return_20d = (hist_log[-1] - hist_log[-21]) * 100.0 if hist_log.size > 20 else None
            trend_slope_20 = tail_slope(hist_log, 20)
            denoised_slope_10 = tail_slope(hist_ema, 10)
            vol_norm = None
            if log_rets.size >= 20:
                sigma = std_np(log_rets[-20:])
                if sigma is not None:
                    vol_norm = float(log_rets[-1] / sigma)
            residual_z = None
            if idx + 1 >= 20:
                sigma = std_np(residuals[idx - 19 : idx + 1])
                if sigma is not None:
                    residual_z = float(residuals[idx] / sigma)
            fft_low, fft_high, fft_snr = spectral_energy(log_rets, 32)
            haar_trend, haar_noise = haar_energy(log_rets, 32)
            records.append(
                {
                    "symbol": symbol,
                    "feature_date": trade_date,
                    "log_return_1d_pct": log_return_1d,
                    "log_return_5d_pct": log_return_5d,
                    "log_return_20d_pct": log_return_20d,
                    "log_trend_slope_20d_pct": None if trend_slope_20 is None else trend_slope_20 * 100.0,
                    "denoised_log_slope_10d_pct": None if denoised_slope_10 is None else denoised_slope_10 * 100.0,
                    "log_return_vol_norm_20d": vol_norm,
                    "denoise_residual_zscore": residual_z,
                    "fft_low_freq_power": fft_low,
                    "fft_high_freq_power": fft_high,
                    "fft_signal_to_noise": fft_snr,
                    "haar_trend_energy": haar_trend,
                    "haar_noise_energy": haar_noise,
                    "log_feature_window": min(32, int(log_rets.size)),
                }
            )
    return pd.DataFrame.from_records(records)


def merge_strategy_with_features(rows: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    if rows.empty or features.empty:
        return rows.copy()
    rows_work = rows.copy()
    rows_work["report_date_dt"] = pd.to_datetime(rows_work["report_date"]).astype("datetime64[ns]")
    features_work = features.copy()
    features_work["feature_date_dt"] = pd.to_datetime(features_work["feature_date"]).astype("datetime64[ns]")
    merged_parts: list[pd.DataFrame] = []
    for symbol, group in rows_work.groupby("symbol", sort=False):
        feat = features_work[features_work["symbol"] == symbol].sort_values("feature_date_dt")
        if feat.empty:
            copied = group.copy()
            copied["feature_date"] = pd.NaT
            merged_parts.append(copied)
            continue
        left = group.sort_values("report_date_dt")
        right = feat.drop(columns=["symbol"]).sort_values("feature_date_dt")
        merged = pd.merge_asof(
            left,
            right,
            left_on="report_date_dt",
            right_on="feature_date_dt",
            direction="backward",
        )
        merged_parts.append(merged)
    out = pd.concat(merged_parts, ignore_index=True) if merged_parts else rows_work
    out["feature_lag_days"] = (
        pd.to_datetime(out["report_date_dt"]) - pd.to_datetime(out.get("feature_date_dt"))
    ).dt.days
    return out


def dedupe_strategy_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    work = rows.copy()
    work["_ev_lcb_sort"] = pd.to_numeric(work.get("ev_lcb_80_pct"), errors="coerce").fillna(-999.0)
    work["_ev_sort"] = pd.to_numeric(work.get("ev_pct"), errors="coerce").fillna(-999.0)
    work = work.sort_values(["report_date", "symbol", "_ev_lcb_sort", "_ev_sort"], ascending=[True, True, False, False])
    work = work.drop_duplicates(["report_date", "symbol"], keep="first")
    return work.drop(columns=["_ev_lcb_sort", "_ev_sort"], errors="ignore").reset_index(drop=True)


def add_holding_days(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty or "fill_date" not in rows.columns or "exit_date" not in rows.columns:
        return rows.copy()
    out = rows.copy()
    fill = pd.to_datetime(out["fill_date"], errors="coerce")
    exit_ = pd.to_datetime(out["exit_date"], errors="coerce")
    out["hold_days"] = (exit_ - fill).dt.days.clip(lower=0)
    return out


def metric_for_df(label: str, rows: pd.DataFrame) -> MetricRow:
    if rows.empty:
        return MetricRow(label, 0, 0, None, None, None, None, None, None, None, None)
    values = pd.to_numeric(rows.get("return_pct"), errors="coerce").dropna().astype(float)
    if values.empty:
        return MetricRow(label, 0, 0, None, None, None, None, None, None, None, None)
    returns = values.to_list()
    avg = statistics.fmean(returns)
    median = statistics.median(returns)
    win = sum(1 for ret in returns if ret > 0) / len(returns)
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
    resolved_rows = rows.loc[values.index].copy()
    report_dates = pd.to_datetime(resolved_rows["report_date"])
    active_dates = int(report_dates.dt.date.nunique())
    daily_returns = (
        resolved_rows.assign(_return=values)
        .groupby(report_dates.dt.date)["_return"]
        .mean()
        .astype(float)
        .to_list()
    )
    mfe = pd.to_numeric(resolved_rows.get("max_favorable_pct"), errors="coerce").dropna()
    mae = pd.to_numeric(resolved_rows.get("max_adverse_pct"), errors="coerce").dropna()
    hold = pd.to_numeric(resolved_rows.get("hold_days"), errors="coerce").dropna()
    return MetricRow(
        label=label,
        n=len(returns),
        active_dates=active_dates,
        avg_pct=avg,
        median_pct=median,
        win_rate=win,
        lcb80_pct=lcb80,
        lcb95_pct=lcb95,
        avg_mfe_pct=float(mfe.mean()) if not mfe.empty else None,
        avg_mae_pct=float(mae.mean()) if not mae.empty else None,
        avg_hold_days=float(hold.mean()) if not hold.empty else None,
        std_pct=std_pct,
        trade_sharpe=sharpe_ratio(returns),
        daily_sharpe=sharpe_ratio(daily_returns, annualize=True),
    )


def bucket_log20(value: Any) -> str:
    parsed = round_or_none(value)
    if parsed is None:
        return "unknown"
    if parsed <= -20:
        return "<=-20%"
    if parsed <= -10:
        return "-20%..-10%"
    if parsed <= -5:
        return "-10%..-5%"
    if parsed <= 0:
        return "-5%..0%"
    return ">0%"


def bucket_slope(value: Any) -> str:
    parsed = round_or_none(value)
    if parsed is None:
        return "unknown"
    if parsed < -0.20:
        return "<-0.20%/d"
    if parsed < 0:
        return "-0.20..0"
    if parsed < 0.20:
        return "0..0.20"
    return ">=0.20%/d"


def bucket_snr(value: Any) -> str:
    parsed = round_or_none(value)
    if parsed is None:
        return "unknown"
    if parsed < 0.35:
        return "<0.35"
    if parsed < 0.60:
        return "0.35..0.60"
    if parsed < 1.00:
        return "0.60..1.00"
    return ">=1.00"


def bucket_noise(value: Any) -> str:
    parsed = round_or_none(value)
    if parsed is None:
        return "unknown"
    if parsed < 0.45:
        return "<45%"
    if parsed < 0.60:
        return "45%..60%"
    if parsed < 0.75:
        return "60%..75%"
    return ">=75%"


def bucket_z(value: Any) -> str:
    parsed = round_or_none(value)
    if parsed is None:
        return "unknown"
    if parsed <= -1.5:
        return "<=-1.5"
    if parsed < -0.5:
        return "-1.5..-0.5"
    if parsed < 0.5:
        return "-0.5..0.5"
    if parsed < 1.5:
        return "0.5..1.5"
    return ">=1.5"


def add_feature_buckets(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["log20_bucket"] = out["log_return_20d_pct"].map(bucket_log20)
    out["denoised_slope_bucket"] = out["denoised_log_slope_10d_pct"].map(bucket_slope)
    out["fft_snr_bucket"] = out["fft_signal_to_noise"].map(bucket_snr)
    out["haar_noise_bucket"] = out["haar_noise_energy"].map(bucket_noise)
    out["vol_norm_bucket"] = out["log_return_vol_norm_20d"].map(bucket_z)
    out["residual_z_bucket"] = out["denoise_residual_zscore"].map(bucket_z)
    return out


def metric_records_by_bucket(scope: str, rows: pd.DataFrame, feature: str, baseline: MetricRow) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if rows.empty or feature not in rows.columns:
        return records
    order = sorted(rows[feature].dropna().astype(str).unique())
    for bucket in order:
        group = rows[rows[feature].astype(str) == bucket]
        metric = metric_for_df(bucket, group).to_dict()
        metric.update(
            {
                "scope": scope,
                "feature": feature,
                "bucket": bucket,
                "baseline_lcb80_pct": round_or_none(baseline.lcb80_pct),
                "lcb80_delta_pct": (
                    None
                    if metric.get("lcb80_pct") is None or baseline.lcb80_pct is None
                    else round_or_none(metric["lcb80_pct"] - baseline.lcb80_pct)
                ),
            }
        )
        records.append(metric)
    return records


def classify_policy(metric: dict[str, Any], baseline: MetricRow, min_samples: int) -> str:
    n = int(metric.get("n") or 0)
    lcb80 = metric.get("lcb80_pct")
    baseline_lcb = baseline.lcb80_pct
    if n < min_samples:
        return "diagnostic_only_sample_thin"
    if lcb80 is None or baseline_lcb is None:
        return "diagnostic_only_missing_lcb"
    if lcb80 <= 0:
        return "risk_warning_or_size_clip"
    if lcb80 >= baseline_lcb + 0.30:
        return "promote_to_report_overlay"
    if lcb80 <= baseline_lcb - 0.30:
        return "risk_warning_or_size_clip"
    return "display_as_context"


def build_policy_records(
    ev_rows: pd.DataFrame,
    all_rows: pd.DataFrame,
    ev_baseline: MetricRow,
    all_baseline: MetricRow,
    min_samples: int,
) -> list[dict[str, Any]]:
    policies: list[tuple[str, str, pd.DataFrame]] = []

    def add(label: str, scope: str, frame: pd.DataFrame, mask: pd.Series) -> None:
        policies.append((label, scope, frame[mask.fillna(False)]))

    ev = ev_rows
    all_ = all_rows
    add("EV+ baseline", "ev_positive", ev, pd.Series(True, index=ev.index))
    add("EV+ and Haar noise <= 60%", "ev_positive", ev, pd.to_numeric(ev["haar_noise_energy"], errors="coerce") <= 0.60)
    add("EV+ and Haar noise <= 65%", "ev_positive", ev, pd.to_numeric(ev["haar_noise_energy"], errors="coerce") <= 0.65)
    add("EV+ and Haar noise >= 75%", "ev_positive", ev, pd.to_numeric(ev["haar_noise_energy"], errors="coerce") >= 0.75)
    add("EV+ and FFT S/N >= 0.50", "ev_positive", ev, pd.to_numeric(ev["fft_signal_to_noise"], errors="coerce") >= 0.50)
    add("EV+ and FFT S/N >= 0.80", "ev_positive", ev, pd.to_numeric(ev["fft_signal_to_noise"], errors="coerce") >= 0.80)
    add(
        "EV+ and denoised slope >= 0",
        "ev_positive",
        ev,
        pd.to_numeric(ev["denoised_log_slope_10d_pct"], errors="coerce") >= 0,
    )
    add(
        "EV+ and denoised slope < 0",
        "ev_positive",
        ev,
        pd.to_numeric(ev["denoised_log_slope_10d_pct"], errors="coerce") < 0,
    )
    add(
        "EV+ residual stretch <= -1.5",
        "ev_positive",
        ev,
        pd.to_numeric(ev["denoise_residual_zscore"], errors="coerce") <= -1.5,
    )
    add(
        "EV+ downside vol shock <= -1.5",
        "ev_positive",
        ev,
        pd.to_numeric(ev["log_return_vol_norm_20d"], errors="coerce") <= -1.5,
    )
    add(
        "EV+ low-noise recovery",
        "ev_positive",
        ev,
        (pd.to_numeric(ev["haar_noise_energy"], errors="coerce") <= 0.65)
        & (pd.to_numeric(ev["denoised_log_slope_10d_pct"], errors="coerce") >= 0),
    )
    add(
        "EV+ noisy downtrend",
        "ev_positive",
        ev,
        (pd.to_numeric(ev["haar_noise_energy"], errors="coerce") >= 0.75)
        & (pd.to_numeric(ev["denoised_log_slope_10d_pct"], errors="coerce") < 0),
    )
    add(
        "All oversold deep log20 <= -20%",
        "all_oversold",
        all_,
        pd.to_numeric(all_["log_return_20d_pct"], errors="coerce") <= -20,
    )
    add(
        "All oversold Haar noise <= 60%",
        "all_oversold",
        all_,
        pd.to_numeric(all_["haar_noise_energy"], errors="coerce") <= 0.60,
    )
    add(
        "All oversold noisy downtrend",
        "all_oversold",
        all_,
        (pd.to_numeric(all_["haar_noise_energy"], errors="coerce") >= 0.75)
        & (pd.to_numeric(all_["denoised_log_slope_10d_pct"], errors="coerce") < 0),
    )

    out: list[dict[str, Any]] = []
    for label, scope, frame in policies:
        baseline = ev_baseline if scope == "ev_positive" else all_baseline
        metric = metric_for_df(label, frame).to_dict()
        metric.update(
            {
                "scope": scope,
                "baseline_lcb80_pct": round_or_none(baseline.lcb80_pct),
                "lcb80_delta_pct": (
                    None
                    if metric.get("lcb80_pct") is None or baseline.lcb80_pct is None
                    else round_or_none(metric["lcb80_pct"] - baseline.lcb80_pct)
                ),
            }
        )
        metric["decision"] = (
            "baseline"
            if label == "EV+ baseline"
            else classify_policy(metric, baseline, min_samples)
        )
        out.append(metric)
    return out


def current_report_read(row: pd.Series) -> str:
    lcb = round_or_none(row.get("ev_lcb_80_pct"))
    if lcb is None or lcb <= 0:
        return "blocked_or_baseline; log fields are review context only"
    lag = round_or_none(row.get("feature_lag_days"))
    if lag is None:
        return "no causal log feature coverage"
    if lag > 7:
        return "stale log feature; do not use for action"
    slope = round_or_none(row.get("denoised_log_slope_10d_pct"))
    noise = round_or_none(row.get("haar_noise_energy"))
    snr = round_or_none(row.get("fft_signal_to_noise"))
    if slope is not None and noise is not None and slope >= 0 and noise <= 0.65:
        return "log-denoise supports planned-entry/hold review; not a chase signal"
    if slope is not None and noise is not None and slope < 0 and noise >= 0.75:
        return "noisy downtrend; require pullback evidence and size clip"
    if snr is not None and snr >= 0.80:
        return "trend component visible; display as context"
    return "neutral diagnostic"


def records_from_df(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    work = df.copy()
    if limit is not None:
        work = work.head(limit)
    for col in work.columns:
        if pd.api.types.is_datetime64_any_dtype(work[col]):
            work[col] = work[col].dt.strftime("%Y-%m-%d")
    work = work.replace({np.nan: None})
    return work.to_dict(orient="records")


def render_metric_table(rows: list[dict[str, Any]], title: str, *, show_decision: bool = False) -> list[str]:
    if not rows:
        return [f"### {title}", "", "_No rows._", ""]
    headers = "| Label | n | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | MFE | MAE | Delta |"
    sep = "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    if show_decision:
        headers = "| Label | n | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | Delta | Decision |"
        sep = "|---|---:|---:|---:|---:|---:|---:|---:|---|"
    lines = [f"### {title}", "", headers, sep]
    for row in rows:
        if show_decision:
            lines.append(
                "| "
                f"{row.get('label') or row.get('bucket')} | {int(row.get('n') or 0)} | "
                f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
                f"{fmt_win(row.get('win_rate'))} | {fmt_num(row.get('trade_sharpe'), 2)} | "
                f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_pct(row.get('lcb80_delta_pct'))} | "
                f"{row.get('decision') or '-'} |"
            )
        else:
            lines.append(
                "| "
                f"{row.get('label') or row.get('bucket')} | {int(row.get('n') or 0)} | "
                f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
                f"{fmt_win(row.get('win_rate'))} | {fmt_num(row.get('trade_sharpe'), 2)} | "
                f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_pct(row.get('avg_mfe_pct'))} | "
                f"{fmt_pct(row.get('avg_mae_pct'))} | {fmt_pct(row.get('lcb80_delta_pct'))} |"
            )
    return lines + [""]


def render_feature_bucket_table(rows: list[dict[str, Any]], title: str) -> list[str]:
    if not rows:
        return [f"### {title}", "", "_No rows._", ""]
    lines = [
        f"### {title}",
        "",
        "| Feature | Bucket | n | Avg | LCB80 | Win | Trade Sharpe | Daily Sharpe | Delta |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.get('feature')} | {row.get('bucket')} | {int(row.get('n') or 0)} | "
            f"{fmt_pct(row.get('avg_pct'))} | {fmt_pct(row.get('lcb80_pct'))} | "
            f"{fmt_win(row.get('win_rate'))} | {fmt_num(row.get('trade_sharpe'), 2)} | "
            f"{fmt_num(row.get('daily_sharpe'), 2)} | {fmt_pct(row.get('lcb80_delta_pct'))} |"
        )
    return lines + [""]


def render_current_table(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["## Current Candidate Log-Denoise Read", "", "_No current rows._", ""]
    lines = [
        "## Current Candidate Log-Denoise Read",
        "",
        "| Symbol | Name | Family | Alpha | EV LCB80 | log20 | slope10 | FFT S/N | Haar noise | Feature date | Read |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.get('symbol')} | {row.get('name') or ''} | {row.get('strategy_family')} | "
            f"{row.get('alpha_state')} | {fmt_pct(row.get('ev_lcb_80_pct'))} | "
            f"{fmt_pct(row.get('log_return_20d_pct'))} | "
            f"{fmt_num(row.get('denoised_log_slope_10d_pct'), 3)} | "
            f"{fmt_num(row.get('fft_signal_to_noise'), 2)} | "
            f"{fmt_num((row.get('haar_noise_energy') or 0) * 100.0 if row.get('haar_noise_energy') is not None else None, 1)}% | "
            f"{as_iso(row.get('feature_date')) or '-'} | {row.get('report_read')} |"
        )
    return lines + [""]


def render_report(payload: dict[str, Any]) -> str:
    policy_rows = payload["policy_candidates"]
    promotes = [row for row in policy_rows if row.get("decision") == "promote_to_report_overlay"]
    warnings = [row for row in policy_rows if row.get("decision") == "risk_warning_or_size_clip"]
    if promotes:
        first = promotes[0]
        overlay = "setup overlay" if first.get("scope") == "all_oversold" else "action overlay"
        article = "an" if overlay.startswith(("action", "EV")) else "a"
        conclusion = (
            f"Promote {first['label']} into the CN report as {article} {overlay}; "
            "keep log/FFT/Haar out of the fresh-buy gate."
        )
    elif warnings:
        conclusion = (
            "No log/FFT/Haar filter deserves fresh-buy promotion yet; use the weak buckets as risk warnings and size clips."
        )
    else:
        conclusion = "Log/FFT/Haar is report context only until larger EV-positive samples resolve."

    lines = [
        f"# CN Log-Denoise Evidence Backtest - {payload['as_of']}",
        "",
        f"**One-line conclusion:** {conclusion}",
        "",
        "## Data Coverage",
        "",
        f"- Range: `{payload['start']}` to `{payload['as_of']}`.",
        f"- Resolved strategy rows loaded: `{payload['coverage']['resolved_rows_loaded']}`.",
        f"- Deduped rows with causal log features: `{payload['coverage']['usable_dedup_rows']}`.",
        f"- Price feature max date: `{payload['coverage']['price_max_date'] or '-'}`.",
        f"- Max allowed feature lag: `{payload['coverage']['max_feature_lag_days']}` calendar days.",
        "",
    ]
    lines += render_metric_table(payload["baselines"], "Strategy Baselines")
    lines += render_metric_table(payload["policy_candidates"], "Report Integration Candidates", show_decision=True)
    lines += render_feature_bucket_table(payload["ev_positive_feature_buckets"], "EV-Positive Oversold Feature Buckets")
    lines += render_feature_bucket_table(payload["all_oversold_feature_buckets"], "All Oversold Feature Buckets")
    lines += render_current_table(payload["current_candidates"])
    lines += [
        "## Wiring Recommendation",
        "",
        "- Always display `log20`, `denoised_slope10`, `FFT S/N`, `Haar noise`, and `feature_date` for CN candidates with feature coverage.",
        "- Do not make these fields a standalone buy gate. They should only modify report language, planned-entry confidence, and size/risk warnings after the EV gate already passes.",
        "- If a promoted overlay exists above, wire it into the CN `lifecycle_action` text first, not into automatic R sizing.",
        "- Buckets classified as `risk_warning_or_size_clip` should show `wait_pullback / no chase / reduce probe size` in the report.",
        "",
    ]
    return "\n".join(lines)


def write_duckdb(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    try:
        tables = {
            "cn_log_denoise_baselines": pd.DataFrame(payload["baselines"]),
            "cn_log_denoise_policy_candidates": pd.DataFrame(payload["policy_candidates"]),
            "cn_log_denoise_ev_positive_buckets": pd.DataFrame(payload["ev_positive_feature_buckets"]),
            "cn_log_denoise_all_oversold_buckets": pd.DataFrame(payload["all_oversold_feature_buckets"]),
            "cn_log_denoise_current_candidates": pd.DataFrame(payload["current_candidates"]),
        }
        for name, df in tables.items():
            con.register("tmp_df", df)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM tmp_df")
            con.unregister("tmp_df")
    finally:
        con.close()


def main() -> None:
    args = parse_args()
    as_of = parse_date(args.date) if args.date else latest_strategy_date(args.cn_db)
    start = parse_date(args.start)
    output_dir = args.output_root / as_of.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)

    strategy_rows = load_strategy_rows(args.cn_db, start, as_of)
    current_rows, current_date = load_current_rows(args.cn_db, as_of)
    symbols = sorted(
        set(strategy_rows.get("symbol", pd.Series(dtype=str)).dropna().astype(str))
        | set(current_rows.get("symbol", pd.Series(dtype=str)).dropna().astype(str))
    )
    prices = load_prices(args.cn_db, symbols, start, as_of)
    features = compute_log_features(prices)
    price_max_date = as_iso(prices["trade_date"].max()) if not prices.empty else None

    merged = merge_strategy_with_features(strategy_rows, features)
    merged = add_holding_days(merged)
    merged = add_feature_buckets(merged)
    usable = merged[pd.to_numeric(merged["feature_lag_days"], errors="coerce") <= args.max_feature_lag_days].copy()
    usable_dedup = dedupe_strategy_rows(usable)

    oversold = usable_dedup[usable_dedup["strategy_family"] == "oversold_contrarian"].copy()
    ev_positive = oversold[pd.to_numeric(oversold["ev_lcb_80_pct"], errors="coerce") > 0].copy()
    structural = usable_dedup[usable_dedup["strategy_family"] == "structural_core"].copy()
    breakout = usable_dedup[usable_dedup["strategy_family"] == "continuation_breakout"].copy()
    early = usable_dedup[usable_dedup["strategy_family"] == "early_accumulation"].copy()

    baseline_all = metric_for_df("All oversold_contrarian", oversold)
    baseline_ev = metric_for_df("EV-positive oversold_contrarian", ev_positive)
    baselines = [
        baseline_all.to_dict(),
        baseline_ev.to_dict(),
        metric_for_df("Structural core baseline", structural).to_dict(),
        metric_for_df("Continuation breakout diagnostic", breakout).to_dict(),
        metric_for_df("Early accumulation diagnostic", early).to_dict(),
    ]
    policy_candidates = build_policy_records(ev_positive, oversold, baseline_ev, baseline_all, args.min_samples)

    bucket_features = [
        "log20_bucket",
        "denoised_slope_bucket",
        "fft_snr_bucket",
        "haar_noise_bucket",
        "vol_norm_bucket",
        "residual_z_bucket",
    ]
    ev_buckets: list[dict[str, Any]] = []
    all_buckets: list[dict[str, Any]] = []
    for feature in bucket_features:
        ev_buckets.extend(metric_records_by_bucket("ev_positive", ev_positive, feature, baseline_ev))
        all_buckets.extend(metric_records_by_bucket("all_oversold", oversold, feature, metric_for_df("All oversold", oversold)))
    ev_buckets = sorted(
        [row for row in ev_buckets if int(row.get("n") or 0) > 0],
        key=lambda row: (str(row.get("feature")), str(row.get("bucket"))),
    )
    all_buckets = sorted(
        [row for row in all_buckets if int(row.get("n") or 0) > 0],
        key=lambda row: (str(row.get("feature")), str(row.get("bucket"))),
    )

    current_merged = merge_strategy_with_features(current_rows, features)
    current_merged = dedupe_strategy_rows(current_merged)
    if not current_merged.empty:
        current_merged["report_read"] = current_merged.apply(current_report_read, axis=1)
        current_merged = current_merged.sort_values(
            ["alpha_state", "ev_lcb_80_pct", "symbol"],
            ascending=[True, False, True],
        )
    current_records = records_from_df(
        current_merged[
            [
                "report_date",
                "symbol",
                "name",
                "strategy_family",
                "selection_status",
                "action_intent",
                "alpha_state",
                "ev_lcb_80_pct",
                "log_return_20d_pct",
                "denoised_log_slope_10d_pct",
                "fft_signal_to_noise",
                "haar_noise_energy",
                "feature_date",
                "feature_lag_days",
                "report_read",
            ]
        ]
        if not current_merged.empty
        else current_merged,
        limit=25,
    )

    payload = {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "current_report_date": current_date.isoformat() if current_date else None,
        "coverage": {
            "resolved_rows_loaded": int(len(strategy_rows)),
            "dedup_rows": int(len(dedupe_strategy_rows(merged))),
            "usable_dedup_rows": int(len(usable_dedup)),
            "price_rows_loaded": int(len(prices)),
            "price_max_date": price_max_date,
            "max_feature_lag_days": args.max_feature_lag_days,
            "min_samples": args.min_samples,
        },
        "baselines": baselines,
        "policy_candidates": policy_candidates,
        "ev_positive_feature_buckets": ev_buckets,
        "all_oversold_feature_buckets": all_buckets,
        "current_candidates": current_records,
    }

    md = render_report(payload)
    (output_dir / "cn_log_denoise_backtest.md").write_text(md, encoding="utf-8")
    (output_dir / "cn_log_denoise_backtest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_duckdb(output_dir / "cn_log_denoise_backtest.duckdb", payload)
    print(f"CN log-denoise backtest written: {output_dir / 'cn_log_denoise_backtest.md'}")


if __name__ == "__main__":
    main()
