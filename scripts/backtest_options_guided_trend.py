#!/usr/bin/env python3
"""Backtest whether options context improves AI-infra stock trend timing.

This is a research backtest, not a production candidate gate. It keeps stock
membership inside the AI Infra universe and only asks whether IV/HV plus the
time-weighted gamma-spring field improves next-day stock trend selection.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import ai_infra_universe  # noqa: E402


DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "options_guided_trend_backtest"
BENCHMARKS = ("SPY", "QQQ", "SMH")
TRADING_DAYS = 252
CONTRACT_MULTIPLIER = 100.0


@dataclass(frozen=True)
class TrendPoint:
    symbol: str
    as_of: date
    close: float
    next_close: float
    next_date: date
    fwd_return: float
    mom5: float
    mom20: float
    ema20_gap: float
    trend_score: float


@dataclass(frozen=True)
class IvPoint:
    symbol: str
    as_of: date
    iv_ann: float | None
    rv_ann: float | None
    iv_hv: float | None
    iv_rank: float | None
    hist_n: int
    vrp: float | None
    pc_z: float | None
    skew_z: float | None
    bucket: str


@dataclass(frozen=True)
class GammaPoint:
    symbol: str
    as_of: date
    spot: float
    center: float
    displacement: float
    well_width: float
    net_ratio: float
    abs_gex: float
    stiffness_rank: float
    damping: float
    state: str


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stdev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def _z_scores(values: dict[str, float]) -> dict[str, float]:
    vals = list(values.values())
    mu = _mean(vals)
    sigma = _stdev(vals)
    if sigma <= 0:
        return {key: 0.0 for key in values}
    return {key: (value - mu) / sigma for key, value in values.items()}


def _fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = _finite(value)
    if parsed is None:
        return "-"
    return f"{parsed * 100:+.{digits}f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    parsed = _finite(value)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def _equity(returns: Iterable[float]) -> list[float]:
    out: list[float] = []
    cur = 1.0
    for ret in returns:
        cur *= 1.0 + ret
        out.append(cur)
    return out


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row["net_return"]) for row in rows if row.get("net_return") is not None]
    gross = [float(row["gross_return"]) for row in rows if row.get("gross_return") is not None]
    if not returns:
        return {"n_days": 0}
    eq = _equity(returns)
    n = len(returns)
    total = eq[-1] - 1.0
    ann = (eq[-1] ** (TRADING_DAYS / n) - 1.0) if eq[-1] > 0 else None
    avg = _mean(returns)
    std = _stdev(returns)
    sharpe = avg / std * math.sqrt(TRADING_DAYS) if std > 0 else None
    peak = eq[0]
    max_dd = 0.0
    for value in eq:
        peak = max(peak, value)
        max_dd = min(max_dd, value / peak - 1.0)
    return {
        "n_days": n,
        "total_return": total,
        "annualized_return": ann,
        "avg_daily_return": avg,
        "avg_gross_daily_return": _mean(gross),
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "hit_rate": sum(1 for ret in returns if ret > 0) / n,
        "avg_names": _mean([float(row.get("n_names") or 0.0) for row in rows]),
        "avg_turnover": _mean([float(row.get("turnover") or 0.0) for row in rows]),
    }


def _bucket_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "avg": _mean(values),
        "median": statistics.median(values),
        "hit_rate": sum(1 for value in values if value > 0) / len(values),
        "best": max(values),
        "worst": min(values),
    }


def load_prices(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float]]]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT symbol, date, close
        FROM prices_daily
        WHERE symbol IN ({placeholders})
          AND date BETWEEN ? AND ?
          AND close IS NOT NULL
        ORDER BY symbol, date
        """,
        [*symbols, start.isoformat(), end.isoformat()],
    ).fetchall()
    out: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for symbol, date_value, close in rows:
        d = date_value if isinstance(date_value, date) else date.fromisoformat(str(date_value))
        parsed = _finite(close)
        if parsed is not None and parsed > 0:
            out[str(symbol).upper()].append((d, parsed))
    return dict(out)


def _ema(values: list[float], span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (span + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1.0 - alpha) * out[-1])
    return out


def build_trend_points(
    prices: dict[str, list[tuple[date, float]]],
    trading_dates: list[date],
    symbols: list[str],
) -> dict[tuple[date, str], TrendPoint]:
    raw_by_date: dict[date, dict[str, dict[str, float]]] = defaultdict(dict)
    for symbol in symbols:
        series = prices.get(symbol) or []
        if len(series) < 25:
            continue
        dates = [row[0] for row in series]
        closes = [row[1] for row in series]
        date_idx = {d: idx for idx, d in enumerate(dates)}
        ema20 = _ema(closes, 20)
        for d in trading_dates[:-1]:
            idx = date_idx.get(d)
            if idx is None or idx < 20 or idx + 1 >= len(series):
                continue
            next_date, next_close = series[idx + 1]
            if next_date <= d or closes[idx] <= 0:
                continue
            raw_by_date[d][symbol] = {
                "close": closes[idx],
                "next_close": next_close,
                "next_ord": float(next_date.toordinal()),
                "fwd_return": next_close / closes[idx] - 1.0,
                "mom5": closes[idx] / closes[idx - 5] - 1.0 if closes[idx - 5] > 0 else 0.0,
                "mom20": closes[idx] / closes[idx - 20] - 1.0 if closes[idx - 20] > 0 else 0.0,
                "ema20_gap": closes[idx] / ema20[idx] - 1.0 if ema20[idx] > 0 else 0.0,
            }

    out: dict[tuple[date, str], TrendPoint] = {}
    for d, by_symbol in raw_by_date.items():
        mom5_z = _z_scores({s: row["mom5"] for s, row in by_symbol.items()})
        mom20_z = _z_scores({s: row["mom20"] for s, row in by_symbol.items()})
        ema_z = _z_scores({s: row["ema20_gap"] for s, row in by_symbol.items()})
        for symbol, row in by_symbol.items():
            score = 0.25 * mom5_z[symbol] + 0.55 * mom20_z[symbol] + 0.20 * ema_z[symbol]
            out[(d, symbol)] = TrendPoint(
                symbol=symbol,
                as_of=d,
                close=row["close"],
                next_close=row["next_close"],
                next_date=date.fromordinal(int(row["next_ord"])),
                fwd_return=row["fwd_return"],
                mom5=row["mom5"],
                mom20=row["mom20"],
                ema20_gap=row["ema20_gap"],
                trend_score=score,
            )
    return out


def load_iv_points(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[tuple[date, str], IvPoint]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT symbol, as_of, iv_ann, rv_ann, vrp, pc_ratio_z, skew_z
        FROM options_sentiment
        WHERE symbol IN ({placeholders})
          AND as_of BETWEEN ? AND ?
          AND iv_ann IS NOT NULL
          AND rv_ann IS NOT NULL
          AND rv_ann > 0
        ORDER BY symbol, as_of
        """,
        [*symbols, start.isoformat(), end.isoformat()],
    ).fetchall()
    histories: dict[str, list[float]] = defaultdict(list)
    out: dict[tuple[date, str], IvPoint] = {}
    for symbol, date_value, iv_ann, rv_ann, vrp, pc_z, skew_z in rows:
        sym = str(symbol).upper()
        d = date_value if isinstance(date_value, date) else date.fromisoformat(str(date_value))
        iv = _finite(iv_ann)
        rv = _finite(rv_ann)
        if iv is None or rv is None or rv <= 0:
            continue
        hist = histories[sym]
        hist.append(iv)
        iv_rank = None
        if len(hist) >= 10:
            iv_rank = sum(1 for value in hist if value <= iv) / len(hist)
        iv_hv = iv / rv
        bucket = "normal_iv"
        if iv_hv <= 0.90 or (iv_rank is not None and iv_rank <= 0.25):
            bucket = "low_iv"
        elif iv_hv >= 1.35 or (iv_rank is not None and iv_rank >= 0.75):
            bucket = "high_iv"
        out[(d, sym)] = IvPoint(
            symbol=sym,
            as_of=d,
            iv_ann=iv,
            rv_ann=rv,
            iv_hv=iv_hv,
            iv_rank=iv_rank,
            hist_n=len(hist),
            vrp=_finite(vrp),
            pc_z=_finite(pc_z),
            skew_z=_finite(skew_z),
            bucket=bucket,
        )
    return out


def _classify_gamma(row: dict[str, float]) -> str:
    if row["stiffness_rank"] <= 0.20 and row["abs_gex"] < 1_000_000:
        return "LOW_STIFFNESS"
    if row["net_ratio"] >= 0.25:
        if abs(row["displacement"]) <= max(0.01, row["well_width"]):
            return "PINNED_GAMMA_WELL"
        return "GAMMA_REVERSION_BAND"
    if row["net_ratio"] <= -0.25:
        return "NEGATIVE_GAMMA_ACCELERATOR"
    return "MIXED_GAMMA_FIELD"


def load_gamma_points(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    start: date,
    end: date,
    *,
    max_dte: int,
    half_life_days: float,
) -> dict[tuple[date, str], GammaPoint]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT symbol, as_of, days_to_exp, current_price, option_type,
               strike, gamma, open_interest, volume
        FROM options_chain_quotes
        WHERE symbol IN ({placeholders})
          AND as_of BETWEEN ? AND ?
          AND days_to_exp BETWEEN 0 AND ?
          AND current_price IS NOT NULL
          AND option_type IN ('call', 'put')
          AND strike IS NOT NULL
          AND gamma IS NOT NULL
          AND open_interest IS NOT NULL
          AND open_interest > 0
        ORDER BY as_of, symbol
        """,
        [*symbols, start.isoformat(), end.isoformat(), int(max_dte)],
    ).fetchall()

    groups: dict[tuple[date, str], list[tuple[Any, ...]]] = defaultdict(list)
    for symbol, as_of, dte, current_price, option_type, strike, gamma, open_interest, volume in rows:
        d = as_of if isinstance(as_of, date) else date.fromisoformat(str(as_of))
        groups[(d, str(symbol).upper())].append(
            (dte, current_price, option_type, strike, gamma, open_interest, volume)
        )

    rough: dict[tuple[date, str], dict[str, float]] = {}
    for key, raw_rows in groups.items():
        spots = [_finite(row[1]) for row in raw_rows]
        spots = [value for value in spots if value is not None and value > 0]
        if not spots:
            continue
        spot = statistics.median(spots)
        by_strike: dict[float, dict[str, float]] = defaultdict(
            lambda: {"abs": 0.0, "signed": 0.0, "volume": 0.0}
        )
        total_abs = 0.0
        net = 0.0
        volume_gex = 0.0
        for dte, _current_price, option_type, strike, gamma, open_interest, volume in raw_rows:
            k = _finite(strike)
            g = _finite(gamma)
            oi = int(open_interest or 0)
            vol = int(volume or 0)
            dte_f = max(0.0, float(dte or 0.0))
            if k is None or k <= 0 or g is None or g <= 0 or oi <= 0:
                continue
            time_weight = math.exp(-dte_f / max(1.0, half_life_days))
            gex = g * oi * CONTRACT_MULTIPLIER * spot * spot * 0.01 * time_weight
            flow = g * max(vol, 0) * CONTRACT_MULTIPLIER * spot * spot * 0.01 * time_weight
            sign = 1.0 if str(option_type).lower() == "call" else -1.0
            signed = sign * gex
            by_strike[k]["abs"] += abs(gex)
            by_strike[k]["signed"] += signed
            by_strike[k]["volume"] += abs(flow)
            total_abs += abs(gex)
            net += signed
            volume_gex += abs(flow)
        if total_abs <= 0:
            continue
        center = sum(k * row["abs"] for k, row in by_strike.items()) / total_abs
        variance = sum(row["abs"] * ((k - center) / center) ** 2 for k, row in by_strike.items()) / total_abs
        rough[key] = {
            "spot": spot,
            "center": center,
            "displacement": (spot - center) / center if center > 0 else 0.0,
            "well_width": variance ** 0.5,
            "net_ratio": net / total_abs,
            "abs_gex": total_abs,
            "damping": volume_gex / total_abs if total_abs > 0 else 0.0,
        }

    by_date: dict[date, list[tuple[tuple[date, str], dict[str, float]]]] = defaultdict(list)
    for key, row in rough.items():
        by_date[key[0]].append((key, row))

    out: dict[tuple[date, str], GammaPoint] = {}
    for d, items in by_date.items():
        ordered = sorted(items, key=lambda item: item[1]["abs_gex"])
        denom = max(1, len(ordered) - 1)
        for idx, (key, row) in enumerate(ordered):
            row["stiffness_rank"] = idx / denom
        for key, row in items:
            state = _classify_gamma(row)
            out[key] = GammaPoint(
                symbol=key[1],
                as_of=d,
                spot=row["spot"],
                center=row["center"],
                displacement=row["displacement"],
                well_width=row["well_width"],
                net_ratio=row["net_ratio"],
                abs_gex=row["abs_gex"],
                stiffness_rank=row["stiffness_rank"],
                damping=row["damping"],
                state=state,
            )
    return out


def iv_multiplier(point: IvPoint | None) -> float:
    if point is None:
        return 1.0
    if point.bucket == "low_iv":
        return 1.15
    if point.bucket == "high_iv":
        return 0.75
    return 1.0


def gamma_multiplier(point: GammaPoint | None, trend: TrendPoint) -> float:
    if point is None:
        return 1.0
    if point.state == "PINNED_GAMMA_WELL":
        return 0.70
    if point.state == "GAMMA_REVERSION_BAND":
        # Positive gamma pulls price toward center. For long stock timing,
        # below-center displacement is supportive; above-center is resistance.
        return 1.15 if point.displacement < -0.01 else 0.80
    if point.state == "NEGATIVE_GAMMA_ACCELERATOR":
        return 1.20 if trend.mom5 > 0 and trend.mom20 > 0 else 0.75
    return 1.0


def gamma_risk_multiplier(point: GammaPoint | None, trend: TrendPoint) -> float:
    """Less opinionated gamma policy found by the first diagnostic sweep.

    The raw spring-score policy penalizes every positive-gamma "pinned" name.
    In the short local sample that throws away profitable AI momentum. This
    variant only uses gamma as a risk brake: negative gamma is fragile, and
    far-above-center positive gamma gets a small resistance haircut.
    """
    del trend
    if point is None:
        return 1.0
    if point.net_ratio <= -0.25:
        return 0.80
    if point.net_ratio >= 0.25 and point.displacement > 0.05:
        return 0.90
    return 1.0


def _soft_tanh_gate(value: float, width: float) -> float:
    """Smooth 0..1 gate; 0.5 at value=0, asymptotic away from threshold."""
    return 0.5 * (1.0 + math.tanh(value / max(width, 1e-6)))


def gamma_tanh_risk_multiplier(point: GammaPoint | None, trend: TrendPoint) -> float:
    """Continuous gamma risk brake.

    This is the same idea as ``gamma_risk_multiplier`` but without hard
    thresholds. It uses tanh gates so a name just across the line is only
    lightly penalized and the penalty saturates instead of jumping.
    """
    del trend
    if point is None:
        return 1.0
    negative_pressure = _soft_tanh_gate((-point.net_ratio) - 0.20, 0.12)
    positive_gamma = _soft_tanh_gate(point.net_ratio - 0.20, 0.12)
    above_center = _soft_tanh_gate(point.displacement - 0.05, 0.03)
    positive_resistance = positive_gamma * above_center
    penalty = 0.18 * negative_pressure + 0.10 * positive_resistance
    return max(0.72, min(1.0, 1.0 - penalty))


def _score(
    trend: TrendPoint,
    iv: IvPoint | None,
    gamma: GammaPoint | None,
    mode: str,
) -> float:
    score = trend.trend_score
    if mode in {"ivhv_guided", "full_guided"}:
        score *= iv_multiplier(iv)
    if mode == "full_guided":
        score *= gamma_multiplier(gamma, trend)
    if mode == "gamma_risk_guided":
        score *= iv_multiplier(iv)
        score *= gamma_risk_multiplier(gamma, trend)
    if mode == "gamma_tanh_guided":
        score *= iv_multiplier(iv)
        score *= gamma_tanh_risk_multiplier(gamma, trend)
    return score


def _selection_reason(trend: TrendPoint, iv: IvPoint | None, gamma: GammaPoint | None) -> str:
    parts = [f"trend={trend.trend_score:.2f}", f"mom20={trend.mom20:.2%}"]
    if iv is not None and iv.iv_hv is not None:
        rank = f",rank={iv.iv_rank:.0%}" if iv.iv_rank is not None else ""
        parts.append(f"{iv.bucket},IV/HV={iv.iv_hv:.2f}{rank}")
    if gamma is not None:
        parts.append(f"{gamma.state},x={gamma.displacement:.1%},net={gamma.net_ratio:.0%}")
    return "; ".join(parts)


def simulate_strategy(
    dates: list[date],
    trend_points: dict[tuple[date, str], TrendPoint],
    iv_points: dict[tuple[date, str], IvPoint],
    gamma_points: dict[tuple[date, str], GammaPoint],
    *,
    symbols: list[str],
    mode: str,
    eligibility: str,
    top_n: int,
    cost_bps: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    previous_weights: dict[str, float] = {}
    for d in dates[:-1]:
        candidates: list[tuple[float, TrendPoint, IvPoint | None, GammaPoint | None]] = []
        for symbol in symbols:
            trend = trend_points.get((d, symbol))
            if trend is None or trend.trend_score <= 0:
                continue
            iv = iv_points.get((d, symbol))
            gamma = gamma_points.get((d, symbol))
            if eligibility in {"iv", "gamma"} and iv is None:
                continue
            if eligibility == "gamma" and gamma is None:
                continue
            candidates.append((_score(trend, iv, gamma, mode), trend, iv, gamma))
        candidates.sort(key=lambda row: row[0], reverse=True)
        selected = candidates[:top_n]
        weights = {row[1].symbol: 1.0 / len(selected) for row in selected} if selected else {}
        gross = sum(weights[row[1].symbol] * row[1].fwd_return for row in selected) if selected else 0.0
        turnover_symbols = set(previous_weights) | set(weights)
        turnover = sum(abs(weights.get(sym, 0.0) - previous_weights.get(sym, 0.0)) for sym in turnover_symbols)
        net = gross - turnover * cost_bps / 10_000.0
        next_date = selected[0][1].next_date if selected else None
        daily_rows.append(
            {
                "date": d.isoformat(),
                "next_date": next_date.isoformat() if next_date else None,
                "mode": mode,
                "eligibility": eligibility,
                "n_names": len(selected),
                "gross_return": gross,
                "net_return": net,
                "turnover": turnover,
                "symbols": ",".join(row[1].symbol for row in selected),
            }
        )
        for rank, (score, trend, iv, gamma) in enumerate(selected, 1):
            trade_rows.append(
                {
                    "date": d.isoformat(),
                    "next_date": trend.next_date.isoformat(),
                    "mode": mode,
                    "eligibility": eligibility,
                    "rank": rank,
                    "symbol": trend.symbol,
                    "score": score,
                    "weight": weights[trend.symbol],
                    "fwd_return": trend.fwd_return,
                    "trend_score": trend.trend_score,
                    "mom5": trend.mom5,
                    "mom20": trend.mom20,
                    "ema20_gap": trend.ema20_gap,
                    "iv_bucket": iv.bucket if iv else None,
                    "iv_hv": iv.iv_hv if iv else None,
                    "iv_rank": iv.iv_rank if iv else None,
                    "gamma_state": gamma.state if gamma else None,
                    "gamma_displacement": gamma.displacement if gamma else None,
                    "gamma_net_ratio": gamma.net_ratio if gamma else None,
                    "reason": _selection_reason(trend, iv, gamma),
                }
            )
        previous_weights = weights
    return daily_rows, trade_rows


def simulate_benchmark(
    dates: list[date],
    prices: dict[str, list[tuple[date, float]]],
    symbol: str,
) -> list[dict[str, Any]]:
    series = dict(prices.get(symbol) or [])
    rows = []
    for d, next_d in zip(dates, dates[1:]):
        if d not in series or next_d not in series or series[d] <= 0:
            continue
        ret = series[next_d] / series[d] - 1.0
        rows.append(
            {
                "date": d.isoformat(),
                "next_date": next_d.isoformat(),
                "mode": f"benchmark_{symbol}",
                "eligibility": "benchmark",
                "n_names": 1,
                "gross_return": ret,
                "net_return": ret,
                "turnover": 0.0,
                "symbols": symbol,
            }
        )
    return rows


def build_factor_diagnostics(
    dates: list[date],
    symbols: list[str],
    trend_points: dict[tuple[date, str], TrendPoint],
    iv_points: dict[tuple[date, str], IvPoint],
    gamma_points: dict[tuple[date, str], GammaPoint],
) -> dict[str, Any]:
    iv_buckets: dict[str, list[float]] = defaultdict(list)
    gamma_buckets: dict[str, list[float]] = defaultdict(list)
    multiplier_buckets: dict[str, list[float]] = defaultdict(list)
    for d in dates[:-1]:
        for symbol in symbols:
            trend = trend_points.get((d, symbol))
            if trend is None or trend.trend_score <= 0:
                continue
            iv = iv_points.get((d, symbol))
            gamma = gamma_points.get((d, symbol))
            if iv is not None:
                iv_buckets[iv.bucket].append(trend.fwd_return)
            if gamma is not None:
                gamma_buckets[gamma.state].append(trend.fwd_return)
                mult = iv_multiplier(iv) * gamma_multiplier(gamma, trend)
                if mult > 1.05:
                    bucket = "boosted"
                elif mult < 0.95:
                    bucket = "deweighted"
                else:
                    bucket = "neutral"
                multiplier_buckets[bucket].append(trend.fwd_return)
    return {
        "iv_bucket_forward_returns": {key: _bucket_stats(vals) for key, vals in sorted(iv_buckets.items())},
        "gamma_state_forward_returns": {key: _bucket_stats(vals) for key, vals in sorted(gamma_buckets.items())},
        "combined_multiplier_forward_returns": {key: _bucket_stats(vals) for key, vals in sorted(multiplier_buckets.items())},
    }


def _summary_table(summary: dict[str, Any], keys: list[str]) -> list[str]:
    lines = [
        "| Strategy | Days | Total | Ann. | Sharpe | Max DD | Hit | Avg names | Avg turnover |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key in keys:
        stats = summary.get(key) or {}
        lines.append(
            f"| {key} | {stats.get('n_days', 0)} | {_fmt_pct(stats.get('total_return'))} | "
            f"{_fmt_pct(stats.get('annualized_return'))} | {_fmt_num(stats.get('sharpe'))} | "
            f"{_fmt_pct(stats.get('max_drawdown'))} | {_fmt_pct(stats.get('hit_rate'))} | "
            f"{_fmt_num(stats.get('avg_names'))} | {_fmt_num(stats.get('avg_turnover'))} |"
        )
    return lines


def _bucket_table(title: str, data: dict[str, Any]) -> list[str]:
    lines = [f"## {title}", "", "| Bucket | N | Avg next day | Median | Hit | Best | Worst |", "|---|---:|---:|---:|---:|---:|---:|"]
    if not data:
        lines.append("| - | 0 | - | - | - | - | - |")
        lines.append("")
        return lines
    for key, stats in data.items():
        lines.append(
            f"| {key} | {stats.get('n', 0)} | {_fmt_pct(stats.get('avg'))} | "
            f"{_fmt_pct(stats.get('median'))} | {_fmt_pct(stats.get('hit_rate'))} | "
            f"{_fmt_pct(stats.get('best'))} | {_fmt_pct(stats.get('worst'))} |"
        )
    lines.append("")
    return lines


def render_markdown(payload: dict[str, Any]) -> str:
    cfg = payload["config"]
    summary = payload["summary"]
    lines = [
        f"# Options-Guided Stock Trend Backtest - {cfg['start']}..{cfg['end']}",
        "",
        "Research question: does options context improve stock trend timing inside the AI Infra production universe?",
        "",
        "Model contract:",
        "",
        "- Stock price is treated as a sampled continuous path in log-price space; daily close-to-close is the tested return.",
        "- Options are treated as a discrete strike x expiry x open-interest grid, then projected into a stock-level field.",
        "- Time enters the gamma field as `time_weight = exp(-DTE / half_life_days)`; Greeks already contain time-to-expiry, this extra term emphasizes near-expiry pinning and roll-off.",
        "- Positive signed gamma creates a spring around the weighted center strike; negative signed gamma is an inverted spring / acceleration regime.",
        "- `gamma_tanh_guided` replaces fixed gamma haircuts with smooth `tanh` penalties for negative-gamma pressure and far-above-center positive-gamma resistance.",
        "- Signals use date t data and measure t close to next trading close. This is stock PnL only, not option-leg PnL.",
        "",
        f"Parameters: top_n={cfg['top_n']}, cost_bps={cfg['cost_bps']}, max_dte={cfg['max_dte']}, gamma_half_life_days={cfg['gamma_half_life_days']}.",
        "",
        "## IV/HV Window",
        "",
    ]
    lines.extend(_summary_table(summary, ["trend_only_iv_matched", "ivhv_guided", "benchmark_SPY", "benchmark_QQQ", "benchmark_SMH"]))
    lines += [
        "",
        "## Gamma Window",
        "",
    ]
    lines.extend(_summary_table(summary, ["trend_only_gamma_matched", "ivhv_gamma_matched", "full_guided", "gamma_risk_guided", "gamma_tanh_guided", "benchmark_SPY_gamma_window", "benchmark_QQQ_gamma_window", "benchmark_SMH_gamma_window"]))
    lines += [
        "",
        "## Interpretation",
        "",
        payload.get("verdict") or "-",
        "",
    ]
    diag = payload.get("diagnostics") or {}
    lines.extend(_bucket_table("IV/HV Bucket Diagnostics", diag.get("iv_bucket_forward_returns") or {}))
    lines.extend(_bucket_table("Gamma State Diagnostics", diag.get("gamma_state_forward_returns") or {}))
    lines.extend(_bucket_table("Combined Multiplier Diagnostics", diag.get("combined_multiplier_forward_returns") or {}))
    lines += [
        "## Data Caveats",
        "",
        f"- options_sentiment coverage: {payload['data_coverage'].get('options_sentiment')}",
        f"- options_chain_quotes coverage: {payload['data_coverage'].get('options_chain_quotes')}",
        "- Gamma window is short in the current local database, so treat it as an early falsification test, not a production proof.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def _verdict(summary: dict[str, Any]) -> str:
    iv_base = summary.get("trend_only_iv_matched") or {}
    iv_guided = summary.get("ivhv_guided") or {}
    gamma_base = summary.get("trend_only_gamma_matched") or {}
    full = summary.get("full_guided") or {}
    parts = []
    if iv_base.get("n_days") and iv_guided.get("n_days"):
        delta = (iv_guided.get("sharpe") or 0.0) - (iv_base.get("sharpe") or 0.0)
        parts.append(
            f"- IV/HV overlay Sharpe delta: {delta:+.2f} "
            f"({iv_guided.get('sharpe'):.2f} vs {iv_base.get('sharpe'):.2f})."
        )
    if gamma_base.get("n_days") and full.get("n_days"):
        delta = (full.get("sharpe") or 0.0) - (gamma_base.get("sharpe") or 0.0)
        parts.append(
            f"- Full IV/HV + time-weighted Gamma Spring Sharpe delta: {delta:+.2f} "
            f"({full.get('sharpe'):.2f} vs {gamma_base.get('sharpe'):.2f})."
        )
    risk = summary.get("gamma_risk_guided") or {}
    iv_gamma = summary.get("ivhv_gamma_matched") or {}
    if risk.get("n_days") and iv_gamma.get("n_days"):
        delta = (risk.get("sharpe") or 0.0) - (iv_gamma.get("sharpe") or 0.0)
        parts.append(
            f"- Gamma risk-only policy Sharpe delta vs IV/HV-only: {delta:+.2f} "
            f"({risk.get('sharpe'):.2f} vs {iv_gamma.get('sharpe'):.2f})."
        )
    tanh = summary.get("gamma_tanh_guided") or {}
    if tanh.get("n_days") and iv_gamma.get("n_days"):
        delta = (tanh.get("sharpe") or 0.0) - (iv_gamma.get("sharpe") or 0.0)
        parts.append(
            f"- Gamma tanh-soft policy Sharpe delta vs IV/HV-only: {delta:+.2f} "
            f"({tanh.get('sharpe'):.2f} vs {iv_gamma.get('sharpe'):.2f})."
        )
    if not parts:
        return "Data is insufficient for a verdict."
    return "\n".join(parts)


def _coverage(con: duckdb.DuckDBPyConnection, table: str) -> str:
    try:
        rows = con.execute(f"SELECT MIN(as_of), MAX(as_of), COUNT(*), COUNT(DISTINCT symbol) FROM {table}").fetchone()
    except duckdb.Error:
        return "missing"
    if not rows or rows[0] is None:
        return "empty"
    return f"{rows[0]}..{rows[1]}, rows={rows[2]}, symbols={rows[3]}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--start", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(), default=date(2026, 3, 10))
    parser.add_argument("--end", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(), default=None)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--max-dte", type=int, default=45)
    parser.add_argument("--gamma-half-life-days", type=float, default=21.0)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = sorted(ai_infra_universe.records_by_symbol("US", pool="production"))
    end = args.end
    con = duckdb.connect(str(args.us_db), read_only=True)
    try:
        if end is None:
            row = con.execute("SELECT MAX(date) FROM prices_daily WHERE symbol = 'SPY'").fetchone()
            end = row[0] if row and isinstance(row[0], date) else date.fromisoformat(str(row[0]))
        price_start = args.start - timedelta(days=80)
        all_symbols = sorted(set(symbols) | set(BENCHMARKS))
        prices = load_prices(con, all_symbols, price_start, end)
        trading_dates = [d for d, _close in prices.get("SPY", []) if args.start <= d <= end]
        trend_points = build_trend_points(prices, trading_dates, symbols)
        iv_points = load_iv_points(con, symbols, args.start, end)
        gamma_points = load_gamma_points(
            con,
            symbols,
            args.start,
            end,
            max_dte=args.max_dte,
            half_life_days=args.gamma_half_life_days,
        )
        coverage = {
            "options_sentiment": _coverage(con, "options_sentiment"),
            "options_chain_quotes": _coverage(con, "options_chain_quotes"),
        }
    finally:
        con.close()

    iv_dates = sorted(
        d for d in trading_dates
        if any((d, symbol) in trend_points and (d, symbol) in iv_points for symbol in symbols)
    )
    gamma_dates = sorted(
        d for d in trading_dates
        if any((d, symbol) in trend_points and (d, symbol) in iv_points and (d, symbol) in gamma_points for symbol in symbols)
    )

    all_daily: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    strategies = [
        ("trend_only_iv_matched", "trend_only", "iv", iv_dates),
        ("ivhv_guided", "ivhv_guided", "iv", iv_dates),
        ("trend_only_gamma_matched", "trend_only", "gamma", gamma_dates),
        ("ivhv_gamma_matched", "ivhv_guided", "gamma", gamma_dates),
        ("full_guided", "full_guided", "gamma", gamma_dates),
        ("gamma_risk_guided", "gamma_risk_guided", "gamma", gamma_dates),
        ("gamma_tanh_guided", "gamma_tanh_guided", "gamma", gamma_dates),
    ]
    summary: dict[str, Any] = {}
    for label, mode, eligibility, dates_for_run in strategies:
        daily, trades = simulate_strategy(
            dates_for_run,
            trend_points,
            iv_points,
            gamma_points,
            symbols=symbols,
            mode=mode,
            eligibility=eligibility,
            top_n=args.top_n,
            cost_bps=args.cost_bps,
        )
        for row in daily:
            row["strategy"] = label
        for row in trades:
            row["strategy"] = label
        all_daily.extend(daily)
        all_trades.extend(trades)
        summary[label] = _stats(daily)

    for bench in BENCHMARKS:
        rows = simulate_benchmark(iv_dates, prices, bench)
        for row in rows:
            row["strategy"] = f"benchmark_{bench}"
        all_daily.extend(rows)
        summary[f"benchmark_{bench}"] = _stats(rows)
        gamma_rows = simulate_benchmark(gamma_dates, prices, bench)
        for row in gamma_rows:
            row["strategy"] = f"benchmark_{bench}_gamma_window"
        all_daily.extend(gamma_rows)
        summary[f"benchmark_{bench}_gamma_window"] = _stats(gamma_rows)

    diagnostics = build_factor_diagnostics(gamma_dates or iv_dates, symbols, trend_points, iv_points, gamma_points)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "start": args.start.isoformat(),
            "end": end.isoformat(),
            "top_n": args.top_n,
            "cost_bps": args.cost_bps,
            "max_dte": args.max_dte,
            "gamma_half_life_days": args.gamma_half_life_days,
            "universe": "AI Infra production US",
            "symbols": symbols,
            "iv_window_days": len(iv_dates),
            "gamma_window_days": len(gamma_dates),
        },
        "summary": summary,
        "diagnostics": diagnostics,
        "data_coverage": coverage,
    }
    payload["verdict"] = _verdict(summary)

    if not args.no_write:
        output_dir = args.output_root / end.isoformat()
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "options_guided_trend_backtest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        (output_dir / "options_guided_trend_backtest.md").write_text(
            render_markdown(payload),
            encoding="utf-8",
        )
        daily_fields = [
            "strategy", "date", "next_date", "mode", "eligibility", "n_names",
            "gross_return", "net_return", "turnover", "symbols",
        ]
        with (output_dir / "options_guided_trend_daily.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=daily_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_daily)
        trade_fields = [
            "strategy", "date", "next_date", "mode", "eligibility", "rank", "symbol",
            "score", "weight", "fwd_return", "trend_score", "mom5", "mom20", "ema20_gap",
            "iv_bucket", "iv_hv", "iv_rank", "gamma_state", "gamma_displacement",
            "gamma_net_ratio", "reason",
        ]
        with (output_dir / "options_guided_trend_trades.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=trade_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_trades)
        print(f"wrote {output_dir / 'options_guided_trend_backtest.md'}")

    print(render_markdown(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
