#!/usr/bin/env python3
"""Backtest one-ticker stock timing with free OnclickMedia options history.

This is a small external-data probe. It downloads/caches historical stock
prices and near-weekly option chains, projects the discrete option grid into
stock-level IV/HV and gamma-spring features, then tests close-to-next-close
stock exposure. It is not an options trading backtest.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "onclick_options_guided_trend_backtest"
BASE_URL = "https://api.onclickmedia.com"
TRADING_DAYS = 252
CONTRACT_MULTIPLIER = 100.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", default="NVDA")
    parser.add_argument("--start", type=lambda s: date.fromisoformat(s), default=date(2025, 1, 13))
    parser.add_argument("--end", type=lambda s: date.fromisoformat(s), default=date(2025, 3, 14))
    parser.add_argument("--min-dte", type=int, default=4)
    parser.add_argument("--max-dte", type=int, default=11)
    parser.add_argument("--hv-window", type=int, default=20)
    parser.add_argument("--gamma-half-life-days", type=float, default=7.0)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.15)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def _fetch_json(path: str, params: dict[str, str], *, retries: int = 3) -> tuple[Any, str]:
    query = urllib.parse.urlencode(params)
    url = f"{BASE_URL}{path}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "quant-stack-onclick-backtest/1.0"})
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read().decode("utf-8")
            return json.loads(raw), url
        except Exception as exc:  # pragma: no cover - network probe script
            last_error = exc
            time.sleep(0.75 * (attempt + 1))
    raise RuntimeError(f"failed fetching {url}: {last_error}")


def _cached_json(cache_path: Path, path: str, params: dict[str, str], *, refresh: bool, sleep_seconds: float) -> tuple[Any, str, bool]:
    if cache_path.exists() and not refresh:
        return json.loads(cache_path.read_text(encoding="utf-8")), f"cache:{cache_path}", True
    rows, url = _fetch_json(path, params)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    return rows, url, False


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stdev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def _weighted_mean(items: list[tuple[float, float]]) -> float | None:
    cleaned = [(value, max(weight, 0.0)) for value, weight in items if math.isfinite(value) and math.isfinite(weight)]
    denom = sum(weight for _value, weight in cleaned)
    if denom <= 0:
        values = [value for value, _weight in cleaned]
        return _mean(values) if values else None
    return sum(value * weight for value, weight in cleaned) / denom


def _ema(values: list[float], span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (span + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1.0 - alpha) * out[-1])
    return out


def split_adjust_price_series(raw_series: list[tuple[date, float]]) -> tuple[list[tuple[date, float]], list[dict[str, Any]]]:
    """Adjust obvious split jumps so long moving averages are comparable.

    OnclickMedia stock-data is not always split-adjusted. NVDA, for example,
    reports 1208.88 on 2024-06-07 and 121.79 on 2024-06-10. For EMA200, that
    contaminates the average. This lightweight adjustment keeps all closes on
    the latest price scale by propagating detected split ratios backward.
    """
    if not raw_series:
        return [], []
    raw_series = sorted(raw_series)
    adjusted_reversed: list[tuple[date, float]] = []
    events: list[dict[str, Any]] = []
    factor = 1.0
    adjusted_reversed.append((raw_series[-1][0], raw_series[-1][1]))
    for idx in range(len(raw_series) - 2, -1, -1):
        cur_date, cur_close = raw_series[idx]
        next_date, next_close = raw_series[idx + 1]
        if cur_close > 0 and next_close > 0:
            ratio = next_close / cur_close
            if ratio <= 0.35 or ratio >= 3.0:
                factor *= ratio
                events.append(
                    {
                        "from": cur_date.isoformat(),
                        "to": next_date.isoformat(),
                        "raw_prev": cur_close,
                        "raw_next": next_close,
                        "ratio": ratio,
                    }
                )
        adjusted_reversed.append((cur_date, cur_close * factor))
    adjusted_reversed.reverse()
    events.reverse()
    return adjusted_reversed, events


def _equity(returns: list[float]) -> list[float]:
    out: list[float] = []
    cur = 1.0
    for ret in returns:
        cur *= 1.0 + ret
        out.append(cur)
    return out


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row["net_return"]) for row in rows if row.get("net_return") is not None]
    if not returns:
        return {"n_days": 0}
    invested = [
        float(row["net_return"])
        for row in rows
        if row.get("net_return") is not None and float(row.get("exposure") or 0.0) > 0.0
    ]
    eq = _equity(returns)
    n = len(returns)
    avg = _mean(returns)
    std = _stdev(returns)
    peak = eq[0]
    max_dd = 0.0
    for value in eq:
        peak = max(peak, value)
        max_dd = min(max_dd, value / peak - 1.0)
    return {
        "n_days": n,
        "total_return": eq[-1] - 1.0,
        "annualized_return": (eq[-1] ** (TRADING_DAYS / n) - 1.0) if eq[-1] > 0 else None,
        "avg_daily_return": avg,
        "sharpe": avg / std * math.sqrt(TRADING_DAYS) if std > 0 else None,
        "max_drawdown": max_dd,
        "invested_days": len(invested),
        "hit_rate": sum(1 for ret in invested if ret > 0) / len(invested) if invested else None,
        "avg_exposure": _mean([float(row.get("exposure") or 0.0) for row in rows]),
        "avg_turnover": _mean([float(row.get("turnover") or 0.0) for row in rows]),
    }


def _fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = _float(value)
    if parsed is None:
        return "-"
    return f"{parsed * 100:+.{digits}f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    parsed = _float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def load_stock_rows(ticker: str, start: date, end: date, cache_dir: Path, *, refresh: bool, sleep_seconds: float) -> tuple[list[dict[str, Any]], str, bool]:
    rows, url, cached = _cached_json(
        cache_dir / f"stock_{ticker}_{start}_{end}.json",
        "/stock-data/",
        {
            "ticker": ticker,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "data": "all",
            "output": "json",
        },
        refresh=refresh,
        sleep_seconds=sleep_seconds,
    )
    return list(rows or []), url, cached


def target_expiration(as_of: date, *, min_dte: int, max_dte: int) -> date:
    days_to_friday = (4 - as_of.weekday()) % 7
    if days_to_friday < min_dte:
        days_to_friday += 7
    if days_to_friday > max_dte:
        # Fall back to the closest Friday inside a permissive weekly window.
        days_to_friday = max(min_dte, min(days_to_friday, max_dte))
    return as_of + timedelta(days=days_to_friday)


def normalize_chain_rows(rows: list[dict[str, Any]], *, ticker: str, as_of: date, expiration: date, current_price: float) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        greeks = row.get("greeks") or {}
        option_type = str(row.get("type") or "").lower()
        strike = _float(row.get("strike"))
        if option_type not in {"call", "put"} or strike is None:
            continue
        out.append(
            {
                "symbol": ticker,
                "as_of": as_of.isoformat(),
                "expiry": str(row.get("expiration") or expiration.isoformat()),
                "days_to_exp": (expiration - as_of).days,
                "current_price": current_price,
                "contract_symbol": row.get("contract_id"),
                "option_type": option_type,
                "strike": strike,
                "bid": _float(row.get("bid")),
                "bid_size": _float(row.get("bid_size")),
                "ask": _float(row.get("ask")),
                "ask_size": _float(row.get("ask_size")),
                "mid": _float(row.get("mark")),
                "last_price": _float(row.get("last")),
                "volume": _int(row.get("volume")),
                "open_interest": _int(row.get("open_interest")),
                "implied_volatility": _float(greeks.get("implied_volatility")),
                "delta": _float(greeks.get("delta")),
                "gamma": _float(greeks.get("gamma")),
                "theta": _float(greeks.get("theta")),
                "vega": _float(greeks.get("vega")),
                "rho": _float(greeks.get("rho")),
                "source": "onclickmedia_free_backtest",
            }
        )
    return out


def fetch_option_chain(
    ticker: str,
    as_of: date,
    expiration: date,
    current_price: float,
    cache_dir: Path,
    *,
    refresh: bool,
    sleep_seconds: float,
) -> tuple[list[dict[str, Any]], dict[str, str], int]:
    normalized: list[dict[str, Any]] = []
    urls: dict[str, str] = {}
    cache_hits = 0
    for option_type in ("call", "put"):
        try:
            rows, url, cached = _cached_json(
                cache_dir / f"options_{ticker}_{as_of}_{expiration}_{option_type}.json",
                "/options/",
                {
                    "ticker": ticker,
                    "date": as_of.isoformat(),
                    "expiration": expiration.isoformat(),
                    "type": option_type,
                    "data": "all date",
                    "output": "json-v1",
                },
                refresh=refresh,
                sleep_seconds=sleep_seconds,
            )
        except RuntimeError as exc:
            if "HTTP Error 404" not in str(exc):
                raise
            urls[option_type] = f"missing:{exc}"
            return [], urls, cache_hits
        urls[option_type] = url
        cache_hits += int(cached)
        normalized.extend(
            normalize_chain_rows(list(rows or []), ticker=ticker, as_of=as_of, expiration=expiration, current_price=current_price)
        )
    return normalized, urls, cache_hits


def realized_vol(price_series: list[tuple[date, float]], idx: int, window: int) -> float | None:
    if idx < window:
        return None
    returns = [
        math.log(price_series[i][1] / price_series[i - 1][1])
        for i in range(idx - window + 1, idx + 1)
        if price_series[i][1] > 0 and price_series[i - 1][1] > 0
    ]
    if len(returns) < max(5, window // 2):
        return None
    return statistics.pstdev(returns) * math.sqrt(TRADING_DAYS)


def classify_gamma(net_ratio: float, abs_gex: float, displacement: float, well_width: float) -> str:
    if abs_gex < 1_000_000:
        return "LOW_STIFFNESS"
    if net_ratio >= 0.25:
        if abs(displacement) <= max(0.01, well_width):
            return "PINNED_GAMMA_WELL"
        return "GAMMA_REVERSION_BAND"
    if net_ratio <= -0.25:
        return "NEGATIVE_GAMMA_ACCELERATOR"
    return "MIXED_GAMMA_FIELD"


def option_features(
    rows: list[dict[str, Any]],
    *,
    half_life_days: float,
    prev_rows: list[dict[str, Any]] | None = None,
    prev_feature: dict[str, Any] | None = None,
    prev_spot: float | None = None,
) -> dict[str, Any] | None:
    if not rows:
        return None
    spot = _float(rows[0].get("current_price"))
    if spot is None or spot <= 0:
        return None
    atm_iv_items: list[tuple[float, float]] = []
    call_oi = 0.0
    put_oi = 0.0
    put_skew_items: list[tuple[float, float]] = []
    call_skew_items: list[tuple[float, float]] = []
    by_strike: dict[float, dict[str, float]] = defaultdict(lambda: {"abs": 0.0, "signed": 0.0, "volume": 0.0})
    total_abs = 0.0
    net = 0.0
    volume_gex = 0.0
    prev_by_contract = {str(row.get("contract_symbol")): row for row in (prev_rows or []) if row.get("contract_symbol")}
    oi_change_signed_gex = 0.0
    oi_change_abs_gex = 0.0
    volume_signed_gex = 0.0
    volume_abs_gex = 0.0
    oi_change_contracts = 0
    ask_pressure = 0.0
    bid_pressure = 0.0
    for row in rows:
        strike = _float(row.get("strike"))
        gamma = _float(row.get("gamma"))
        iv = _float(row.get("implied_volatility"))
        oi = float(row.get("open_interest") or 0.0)
        volume = float(row.get("volume") or 0.0)
        option_type = str(row.get("option_type") or "").lower()
        dte = max(0.0, float(row.get("days_to_exp") or 0.0))
        if strike is None or strike <= 0:
            continue
        moneyness = strike / spot
        if iv is not None and iv > 0 and 0.95 <= moneyness <= 1.05:
            atm_iv_items.append((iv, max(oi, volume, 1.0)))
        if iv is not None and iv > 0 and option_type == "put" and 0.90 <= moneyness <= 0.98:
            put_skew_items.append((iv, max(oi, volume, 1.0)))
        if iv is not None and iv > 0 and option_type == "call" and 1.02 <= moneyness <= 1.10:
            call_skew_items.append((iv, max(oi, volume, 1.0)))
        if option_type == "call":
            call_oi += oi
        elif option_type == "put":
            put_oi += oi
        if gamma is None or gamma <= 0 or oi <= 0 or option_type not in {"call", "put"}:
            continue
        time_weight = math.exp(-dte / max(1.0, half_life_days))
        gex_unit = gamma * CONTRACT_MULTIPLIER * spot * spot * 0.01 * time_weight
        gex = gex_unit * oi
        flow = gex_unit * max(volume, 0.0)
        sign = 1.0 if option_type == "call" else -1.0
        signed = sign * gex
        by_strike[strike]["abs"] += abs(gex)
        by_strike[strike]["signed"] += signed
        by_strike[strike]["volume"] += abs(flow)
        total_abs += abs(gex)
        net += signed
        volume_gex += abs(flow)
        volume_signed_gex += sign * flow
        volume_abs_gex += abs(flow)

        prev_row = prev_by_contract.get(str(row.get("contract_symbol")))
        if prev_row is not None:
            prev_oi = float(prev_row.get("open_interest") or 0.0)
            oi_change = oi - prev_oi
            if oi_change:
                oi_change_contracts += 1
                oi_change_signed_gex += sign * oi_change * gex_unit
                oi_change_abs_gex += abs(oi_change) * gex_unit

        bid_size = max(_float(row.get("bid_size")) or 0.0, 0.0)
        ask_size = max(_float(row.get("ask_size")) or 0.0, 0.0)
        ask_pressure += ask_size * abs(flow)
        bid_pressure += bid_size * abs(flow)
    atm_iv = _weighted_mean(atm_iv_items)
    if total_abs > 0:
        center = sum(strike * item["abs"] for strike, item in by_strike.items()) / total_abs
        variance = sum(item["abs"] * ((strike - center) / center) ** 2 for strike, item in by_strike.items()) / total_abs
        displacement = (spot - center) / center if center > 0 else 0.0
        well_width = variance ** 0.5
        net_ratio = net / total_abs
        gamma_state = classify_gamma(net_ratio, total_abs, displacement, well_width)
        max_abs_wall = max(by_strike.items(), key=lambda item: item[1]["abs"])[0]
        positive_items = [(strike, item) for strike, item in by_strike.items() if item["signed"] > 0]
        negative_items = [(strike, item) for strike, item in by_strike.items() if item["signed"] < 0]
        max_positive_wall = max(positive_items, key=lambda item: item[1]["signed"])[0] if positive_items else None
        max_negative_wall = min(negative_items, key=lambda item: item[1]["signed"])[0] if negative_items else None
    else:
        center = None
        displacement = None
        well_width = None
        net_ratio = None
        gamma_state = "NO_GAMMA"
        max_abs_wall = None
        max_positive_wall = None
        max_negative_wall = None
    put_skew = _weighted_mean(put_skew_items)
    call_skew = _weighted_mean(call_skew_items)
    skew_ratio = put_skew / call_skew if put_skew and call_skew and call_skew > 0 else None
    oi_change_net_ratio = oi_change_signed_gex / oi_change_abs_gex if oi_change_abs_gex > 0 else None
    volume_net_ratio = volume_signed_gex / volume_abs_gex if volume_abs_gex > 0 else None
    quote_pressure_ratio = None
    if ask_pressure + bid_pressure > 0:
        quote_pressure_ratio = (bid_pressure - ask_pressure) / (bid_pressure + ask_pressure)

    skew_pressure = 0.0
    if skew_ratio is not None:
        skew_pressure = math.tanh((skew_ratio - 1.05) / 0.20)
    dealer_pressure_proxy = max(
        -1.0,
        min(
            1.0,
            0.50 * (oi_change_net_ratio or 0.0)
            + 0.25 * (volume_net_ratio or 0.0)
            + 0.15 * (quote_pressure_ratio or 0.0)
            - 0.25 * skew_pressure,
        ),
    )

    wall_transition = "NO_TRANSITION"
    wall_transition_score = 0.0
    if prev_feature and prev_spot is not None and center is not None:
        prev_center = _float(prev_feature.get("gamma_center"))
        prev_wall = _float(prev_feature.get("max_abs_wall"))
        if prev_center is not None and prev_center > 0:
            prev_side = prev_spot - prev_center
            cur_side = spot - center
            if prev_side <= 0 < cur_side:
                wall_transition = "CENTER_CROSS_UP"
                wall_transition_score += 0.45
            elif prev_side >= 0 > cur_side:
                wall_transition = "CENTER_CROSS_DOWN"
                wall_transition_score -= 0.45
        if prev_wall is not None and prev_wall > 0 and max_abs_wall is not None:
            prev_wall_side = prev_spot - prev_wall
            cur_wall_side = spot - max_abs_wall
            if prev_wall_side <= 0 < cur_wall_side:
                wall_transition = "WALL_BREAK_UP" if wall_transition == "NO_TRANSITION" else wall_transition + "+WALL_BREAK_UP"
                wall_transition_score += 0.35
            elif prev_wall_side >= 0 > cur_wall_side:
                wall_transition = "WALL_BREAK_DOWN" if wall_transition == "NO_TRANSITION" else wall_transition + "+WALL_BREAK_DOWN"
                wall_transition_score -= 0.35
    wall_transition_score = max(-1.0, min(1.0, wall_transition_score))

    return {
        "iv_ann": atm_iv,
        "put_call_oi": put_oi / call_oi if call_oi > 0 else None,
        "skew_ratio": skew_ratio,
        "gamma_center": center,
        "gamma_displacement": displacement,
        "gamma_well_width": well_width,
        "gamma_net_ratio": net_ratio,
        "abs_gex": total_abs,
        "max_abs_wall": max_abs_wall,
        "max_positive_wall": max_positive_wall,
        "max_negative_wall": max_negative_wall,
        "oi_change_net_ratio": oi_change_net_ratio,
        "oi_change_abs_gex": oi_change_abs_gex,
        "oi_change_contracts": oi_change_contracts,
        "volume_net_ratio": volume_net_ratio,
        "quote_pressure_ratio": quote_pressure_ratio,
        "dealer_pressure_proxy": dealer_pressure_proxy,
        "wall_transition": wall_transition,
        "wall_transition_score": wall_transition_score,
        "damping": volume_gex / total_abs if total_abs > 0 else None,
        "gamma_state": gamma_state,
        "n_contracts": len(rows),
        "total_oi": call_oi + put_oi,
    }


def soft_tanh_gate(value: float, width: float) -> float:
    return 0.5 * (1.0 + math.tanh(value / max(width, 1e-6)))


def gamma_tanh_multiplier(feature: dict[str, Any] | None) -> float:
    if not feature:
        return 1.0
    net_ratio = _float(feature.get("gamma_net_ratio"))
    displacement = _float(feature.get("gamma_displacement"))
    if net_ratio is None or displacement is None:
        return 1.0
    negative_pressure = soft_tanh_gate((-net_ratio) - 0.20, 0.12)
    positive_gamma = soft_tanh_gate(net_ratio - 0.20, 0.12)
    above_center = soft_tanh_gate(displacement - 0.05, 0.03)
    positive_resistance = positive_gamma * above_center
    penalty = 0.18 * negative_pressure + 0.10 * positive_resistance
    return max(0.72, min(1.0, 1.0 - penalty))


def gamma_v2_multiplier(row: dict[str, Any]) -> float:
    """Dealer-pressure and wall-transition risk brake.

    This is a proxy layer, not true dealer positioning. It only reduces stock
    exposure when the option grid says pressure or wall transition is adverse.
    """
    dealer = _float(row.get("dealer_pressure_proxy")) or 0.0
    transition_score = _float(row.get("wall_transition_score")) or 0.0
    displacement = _float(row.get("gamma_displacement")) or 0.0
    net_ratio = _float(row.get("gamma_net_ratio")) or 0.0
    penalty = 0.0
    if dealer < -0.25:
        penalty += 0.18 * soft_tanh_gate((-dealer) - 0.25, 0.18)
    if transition_score < -0.10:
        penalty += 0.20 * soft_tanh_gate((-transition_score) - 0.10, 0.18)
    if net_ratio >= 0.25 and displacement > 0.05:
        penalty += 0.08 * soft_tanh_gate(displacement - 0.05, 0.03)
    if net_ratio <= -0.25 and dealer < 0:
        penalty += 0.10 * soft_tanh_gate((-net_ratio) - 0.25, 0.15)
    return max(0.60, min(1.0, 1.0 - penalty))


def gamma_v2_alpha_fields(row: dict[str, Any]) -> dict[str, Any]:
    dealer = _float(row.get("dealer_pressure_proxy")) or 0.0
    transition_score = _float(row.get("wall_transition_score")) or 0.0
    displacement = _float(row.get("gamma_displacement")) or 0.0
    net_ratio = _float(row.get("gamma_net_ratio")) or 0.0
    state_part = 0.62 if net_ratio >= 0.25 else 0.42 if net_ratio <= -0.25 else 0.52
    dealer_part = max(0.0, min(1.0, 0.5 + 0.5 * dealer))
    wall_part = max(0.0, min(1.0, 0.5 + transition_score))
    score = max(0.0, min(1.0, 0.46 * dealer_part + 0.34 * wall_part + 0.20 * state_part))
    do_not_chase = net_ratio >= 0.25 and displacement > 0.08
    if do_not_chase:
        score = min(score, 0.58)
    entry_signal = score >= 0.64 and dealer >= -0.05 and transition_score >= -0.05 and not do_not_chase
    management_signal = (
        "gamma_v2_entry_alpha" if entry_signal else
        "do_not_chase_above_wall" if do_not_chase else
        "reduce_or_tighten_stop" if transition_score <= -0.25 else
        "no_add_tighten_stop" if dealer <= -0.25 else
        "hold_context_only"
    )
    return {
        "gamma_v2_alpha_score": round(score * 100.0, 2),
        "gamma_v2_entry_signal": entry_signal,
        "gamma_v2_management_signal": management_signal,
    }


def dealer_pressure_bucket(value: float | None) -> str:
    if value is None:
        return "missing_pressure"
    if value <= -0.25:
        return "negative_pressure"
    if value >= 0.25:
        return "positive_pressure"
    return "neutral_pressure"


def iv_bucket(iv_hv: float | None, iv_rank: float | None) -> str:
    if iv_hv is None:
        return "missing_iv"
    if iv_hv <= 0.90 or (iv_rank is not None and iv_rank <= 0.25):
        return "low_iv"
    if iv_hv >= 1.35 or (iv_rank is not None and iv_rank >= 0.75):
        return "high_iv"
    return "normal_iv"


def iv_timing_multiplier(bucket: str) -> float:
    if bucket == "low_iv":
        return 1.0
    if bucket == "high_iv":
        return 0.60
    if bucket == "normal_iv":
        return 0.90
    return 0.75


def ivhv_band_exposure(row: dict[str, Any]) -> float:
    iv_hv = _float(row.get("iv_hv"))
    if iv_hv is None:
        return 0.0
    return 1.0 if 0.90 <= iv_hv <= 1.35 else 0.0


def trend_signal(closes: list[float], idx: int, ema20: list[float]) -> tuple[float, bool, dict[str, float]]:
    if idx < 20 or closes[idx] <= 0 or closes[idx - 5] <= 0 or closes[idx - 20] <= 0 or ema20[idx] <= 0:
        return 0.0, False, {"mom5": 0.0, "mom20": 0.0, "ema20_gap": 0.0}
    mom5 = closes[idx] / closes[idx - 5] - 1.0
    mom20 = closes[idx] / closes[idx - 20] - 1.0
    ema20_gap = closes[idx] / ema20[idx] - 1.0
    score = 0.35 * mom5 + 0.45 * mom20 + 0.20 * ema20_gap
    is_on = score > 0 and mom20 > 0 and closes[idx] >= ema20[idx]
    return score, is_on, {"mom5": mom5, "mom20": mom20, "ema20_gap": ema20_gap}


def simulate_strategy(
    rows: list[dict[str, Any]],
    *,
    strategy: str,
    cost_bps: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    previous_exposure = 0.0
    for row in rows:
        if strategy == "buy_hold":
            exposure = 1.0
        elif strategy == "ema200_below_buy":
            exposure = 1.0 if row.get("ema200_ready") and row["close"] < row["ema200"] else 0.0
        elif strategy == "ema200_above_buy":
            exposure = 1.0 if row.get("ema200_ready") and row["close"] >= row["ema200"] else 0.0
        elif strategy == "trend_only":
            exposure = 1.0 if row["trend_on"] else 0.0
        elif strategy == "ivhv_timing":
            exposure = (1.0 if row["trend_on"] else 0.0) * iv_timing_multiplier(row["iv_bucket"])
        elif strategy == "ivhv_band_timing":
            exposure = (1.0 if row["trend_on"] else 0.0) * ivhv_band_exposure(row)
        elif strategy == "ivhv_band_gamma_tanh":
            exposure = (1.0 if row["trend_on"] else 0.0) * ivhv_band_exposure(row) * row["gamma_tanh_multiplier"]
        elif strategy == "ivhv_band_gamma_v2":
            exposure = (1.0 if row["trend_on"] else 0.0) * ivhv_band_exposure(row) * row["gamma_v2_multiplier"]
        elif strategy == "gamma_v2_entry_alpha":
            exposure = 1.0 if row.get("gamma_v2_entry_signal") else 0.0
        elif strategy == "ivhv_gamma_tanh":
            exposure = (1.0 if row["trend_on"] else 0.0) * iv_timing_multiplier(row["iv_bucket"]) * row["gamma_tanh_multiplier"]
        elif strategy == "gamma_tanh_only":
            exposure = (1.0 if row["trend_on"] else 0.0) * row["gamma_tanh_multiplier"]
        else:
            raise ValueError(f"unknown strategy: {strategy}")
        turnover = abs(exposure - previous_exposure)
        net_return = exposure * row["fwd_return"] - turnover * cost_bps / 10_000.0
        item = dict(row)
        item.update({"strategy": strategy, "exposure": exposure, "turnover": turnover, "net_return": net_return})
        out.append(item)
        previous_exposure = exposure
    return out


def bucket_diagnostics(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "missing")].append(float(row["fwd_return"]))
    out: dict[str, dict[str, Any]] = {}
    for bucket, values in sorted(grouped.items()):
        out[bucket] = {
            "n": len(values),
            "avg_next": _mean(values),
            "median_next": statistics.median(values),
            "hit_rate": sum(1 for value in values if value > 0) / len(values),
            "best": max(values),
            "worst": min(values),
        }
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def summary_table(summary: dict[str, Any]) -> list[str]:
    lines = [
        "| Strategy | Days | Invested | Total | Ann. | Sharpe | Max DD | Invested hit | Avg exposure | Avg turnover |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key in ("buy_hold", "ema200_below_buy", "ema200_above_buy", "trend_only", "gamma_tanh_only", "gamma_v2_entry_alpha", "ivhv_timing", "ivhv_gamma_tanh", "ivhv_band_timing", "ivhv_band_gamma_tanh", "ivhv_band_gamma_v2"):
        stats = summary.get(key) or {}
        lines.append(
            f"| {key} | {stats.get('n_days', 0)} | {stats.get('invested_days', 0)} | {_fmt_pct(stats.get('total_return'))} | "
            f"{_fmt_pct(stats.get('annualized_return'))} | {_fmt_num(stats.get('sharpe'))} | "
            f"{_fmt_pct(stats.get('max_drawdown'))} | {_fmt_pct(stats.get('hit_rate'))} | "
            f"{_fmt_num(stats.get('avg_exposure'))} | {_fmt_num(stats.get('avg_turnover'))} |"
        )
    return lines


def diagnostic_table(title: str, data: dict[str, dict[str, Any]]) -> list[str]:
    lines = [f"## {title}", "", "| Bucket | N | Avg next | Median | Hit | Best | Worst |", "|---|---:|---:|---:|---:|---:|---:|"]
    for bucket, stats in data.items():
        lines.append(
            f"| {bucket} | {stats['n']} | {_fmt_pct(stats['avg_next'])} | {_fmt_pct(stats['median_next'])} | "
            f"{_fmt_pct(stats['hit_rate'])} | {_fmt_pct(stats['best'])} | {_fmt_pct(stats['worst'])} |"
        )
    lines.append("")
    return lines


def render_markdown(payload: dict[str, Any]) -> str:
    cfg = payload["config"]
    summary = payload["summary"]
    lines = [
        f"# OnclickMedia Options-Guided Trend Probe - {cfg['ticker']} {cfg['start']}..{cfg['end']}",
        "",
        "Scope: one-stock timing probe using free historical options chains. PnL is stock close-to-next-close only; options data only scales stock exposure.",
        "",
        "Contracts:",
        "",
        f"- Expiry selection: nearest Friday with DTE >= {cfg['min_dte']} and normally <= {cfg['max_dte']}.",
        "- IV/HV: OI-weighted near-ATM IV divided by 20-day realized HV.",
        "- Gamma Spring: signed call/put GEX projected to center strike, net gamma ratio and tanh risk brake.",
        "- Gamma v2: OI-change / volume / quote-size / skew dealer-pressure proxy plus center/wall transition risk brake.",
        "- No leverage: low IV keeps full trend exposure; high IV cuts exposure; gamma tanh only cuts exposure.",
        "",
        "## Results",
        "",
    ]
    lines.extend(summary_table(summary))
    lines += [
        "",
        "## Verdict",
        "",
        payload.get("verdict") or "-",
        "",
    ]
    lines.extend(diagnostic_table("IV/HV Bucket Diagnostics - All Days", payload["diagnostics"]["iv_bucket_forward_returns"]))
    lines.extend(diagnostic_table("IV/HV Bucket Diagnostics - Trend-On Days", payload["diagnostics"]["iv_bucket_trend_on_forward_returns"]))
    lines.extend(diagnostic_table("Gamma State Diagnostics - All Days", payload["diagnostics"]["gamma_state_forward_returns"]))
    lines.extend(diagnostic_table("Gamma State Diagnostics - Trend-On Days", payload["diagnostics"]["gamma_state_trend_on_forward_returns"]))
    lines.extend(diagnostic_table("Dealer Pressure Proxy Diagnostics - Trend-On Days", payload["diagnostics"]["dealer_pressure_trend_on_forward_returns"]))
    lines.extend(diagnostic_table("Wall Transition Diagnostics - Trend-On Days", payload["diagnostics"]["wall_transition_trend_on_forward_returns"]))
    lines += [
        "## Data",
        "",
        f"- Source: OnclickMedia free API, {cfg['n_option_fetch_dates']} option dates, {cfg['n_contract_rows']} normalized contract rows.",
        f"- Stock rows: {cfg['n_stock_rows']}; skipped option dates: {cfg['skipped_option_dates']}.",
        "- This is a small external-data smoke backtest, not enough for production validation.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def verdict(summary: dict[str, Any]) -> str:
    trend = summary.get("trend_only") or {}
    iv = summary.get("ivhv_timing") or {}
    gamma = summary.get("ivhv_gamma_tanh") or {}
    band = summary.get("ivhv_band_timing") or {}
    v2 = summary.get("ivhv_band_gamma_v2") or {}
    gamma_entry = summary.get("gamma_v2_entry_alpha") or {}
    parts: list[str] = []
    if trend.get("n_days") and iv.get("n_days"):
        parts.append(
            f"- IV/HV timing vs pure trend Sharpe delta: {(iv.get('sharpe') or 0.0) - (trend.get('sharpe') or 0.0):+.2f} "
            f"({_fmt_num(iv.get('sharpe'))} vs {_fmt_num(trend.get('sharpe'))})."
        )
    if iv.get("n_days") and gamma.get("n_days"):
        parts.append(
            f"- IV/HV + gamma tanh vs IV/HV Sharpe delta: {(gamma.get('sharpe') or 0.0) - (iv.get('sharpe') or 0.0):+.2f} "
            f"({_fmt_num(gamma.get('sharpe'))} vs {_fmt_num(iv.get('sharpe'))})."
        )
    if trend.get("n_days") and gamma.get("n_days"):
        parts.append(
            f"- IV/HV + gamma tanh vs pure trend total-return delta: {(gamma.get('total_return') or 0.0) - (trend.get('total_return') or 0.0):+.2%}."
        )
    if trend.get("n_days") and band.get("n_days"):
        parts.append(
            f"- IV/HV healthy-band timing vs pure trend Sharpe delta: {(band.get('sharpe') or 0.0) - (trend.get('sharpe') or 0.0):+.2f} "
            f"({_fmt_num(band.get('sharpe'))} vs {_fmt_num(trend.get('sharpe'))}, invested_days={band.get('invested_days', 0)})."
        )
    if band.get("n_days") and v2.get("n_days"):
        parts.append(
            f"- Gamma v2 dealer-pressure/wall-transition delta vs IV/HV band: {(v2.get('sharpe') or 0.0) - (band.get('sharpe') or 0.0):+.2f} "
            f"({_fmt_num(v2.get('sharpe'))} vs {_fmt_num(band.get('sharpe'))})."
        )
    if gamma_entry.get("n_days") and trend.get("n_days"):
        parts.append(
            f"- Gamma v2 entry alpha vs pure trend Sharpe delta: {(gamma_entry.get('sharpe') or 0.0) - (trend.get('sharpe') or 0.0):+.2f} "
            f"({_fmt_num(gamma_entry.get('sharpe'))} vs {_fmt_num(trend.get('sharpe'))}, invested_days={gamma_entry.get('invested_days', 0)})."
        )
    return "\n".join(parts) if parts else "Insufficient rows."


def main() -> int:
    args = parse_args()
    ticker = args.ticker.upper()
    run_dir = args.output_root / ticker / f"{args.start}_{args.end}"
    cache_dir = run_dir / "cache"
    run_dir.mkdir(parents=True, exist_ok=True)

    stock_start = args.start - timedelta(days=max(360, args.hv_window * 4))
    stock_end = args.end + timedelta(days=10)
    stock_rows, stock_url, stock_cached = load_stock_rows(
        ticker,
        stock_start,
        stock_end,
        cache_dir,
        refresh=args.refresh,
        sleep_seconds=args.sleep_seconds,
    )
    raw_price_series: list[tuple[date, float]] = []
    for row in stock_rows:
        d = date.fromisoformat(str(row.get("date")))
        close = _float(row.get("close"))
        if close is not None and close > 0:
            raw_price_series.append((d, close))
    price_series, split_events = split_adjust_price_series(raw_price_series)
    price_series.sort()
    if len(price_series) < 25:
        raise SystemExit(f"insufficient stock rows for {ticker}")

    dates = [d for d, _close in price_series]
    closes = [close for _d, close in price_series]
    idx_by_date = {d: idx for idx, d in enumerate(dates)}
    ema20 = _ema(closes, 20)
    ema200 = _ema(closes, 200)

    feature_rows: list[dict[str, Any]] = []
    normalized_contracts: list[dict[str, Any]] = []
    iv_history: list[float] = []
    option_fetch_dates = 0
    skipped_option_dates = 0
    option_cache_hits = 0
    urls: dict[str, Any] = {"stock": stock_url, "option_examples": []}
    prev_option_rows: list[dict[str, Any]] | None = None
    prev_option_feature: dict[str, Any] | None = None
    prev_option_spot: float | None = None

    for as_of in dates:
        if as_of < args.start or as_of > args.end:
            continue
        idx = idx_by_date[as_of]
        if idx + 1 >= len(price_series):
            continue
        close = closes[idx]
        next_date, next_close = price_series[idx + 1]
        rv_ann = realized_vol(price_series, idx, args.hv_window)
        score, trend_on, trend_parts = trend_signal(closes, idx, ema20)
        expiration = target_expiration(as_of, min_dte=args.min_dte, max_dte=args.max_dte)
        rows, option_urls, cache_hits = fetch_option_chain(
            ticker,
            as_of,
            expiration,
            close,
            cache_dir,
            refresh=args.refresh,
            sleep_seconds=args.sleep_seconds,
        )
        option_cache_hits += cache_hits
        if len(urls["option_examples"]) < 6:
            urls["option_examples"].append({"date": as_of.isoformat(), "expiration": expiration.isoformat(), "urls": option_urls})
        if not rows:
            skipped_option_dates += 1
            continue
        option_fetch_dates += 1
        normalized_contracts.extend(rows)
        opt = option_features(
            rows,
            half_life_days=args.gamma_half_life_days,
            prev_rows=prev_option_rows,
            prev_feature=prev_option_feature,
            prev_spot=prev_option_spot,
        ) or {}
        iv_ann = _float(opt.get("iv_ann"))
        if iv_ann is not None:
            iv_history.append(iv_ann)
        iv_rank = None
        if iv_ann is not None and len(iv_history) >= 10:
            iv_rank = sum(1 for value in iv_history if value <= iv_ann) / len(iv_history)
        iv_hv = iv_ann / rv_ann if iv_ann is not None and rv_ann is not None and rv_ann > 0 else None
        bucket = iv_bucket(iv_hv, iv_rank)
        gamma_mult = gamma_tanh_multiplier(opt)
        gamma_v2_mult = gamma_v2_multiplier(opt)
        gamma_alpha = gamma_v2_alpha_fields(opt)
        feature_rows.append(
            {
                "date": as_of.isoformat(),
                "next_date": next_date.isoformat(),
                "expiry": expiration.isoformat(),
                "close": close,
                "next_close": next_close,
                "fwd_return": next_close / close - 1.0,
                "trend_score": score,
                "trend_on": trend_on,
                "mom5": trend_parts["mom5"],
                "mom20": trend_parts["mom20"],
                "ema20_gap": trend_parts["ema20_gap"],
                "ema200": ema200[idx],
                "ema200_gap": close / ema200[idx] - 1.0 if idx >= 199 and ema200[idx] > 0 else None,
                "ema200_ready": idx >= 199,
                "rv_ann": rv_ann,
                "iv_ann": iv_ann,
                "iv_hv": iv_hv,
                "iv_rank": iv_rank,
                "iv_bucket": bucket,
                "gamma_state": opt.get("gamma_state"),
                "gamma_center": opt.get("gamma_center"),
                "gamma_displacement": opt.get("gamma_displacement"),
                "gamma_net_ratio": opt.get("gamma_net_ratio"),
                "abs_gex": opt.get("abs_gex"),
                "max_abs_wall": opt.get("max_abs_wall"),
                "max_positive_wall": opt.get("max_positive_wall"),
                "max_negative_wall": opt.get("max_negative_wall"),
                "gamma_tanh_multiplier": gamma_mult,
                "gamma_v2_multiplier": gamma_v2_mult,
                "gamma_v2_alpha_score": gamma_alpha["gamma_v2_alpha_score"],
                "gamma_v2_entry_signal": gamma_alpha["gamma_v2_entry_signal"],
                "gamma_v2_management_signal": gamma_alpha["gamma_v2_management_signal"],
                "oi_change_net_ratio": opt.get("oi_change_net_ratio"),
                "oi_change_abs_gex": opt.get("oi_change_abs_gex"),
                "oi_change_contracts": opt.get("oi_change_contracts"),
                "volume_net_ratio": opt.get("volume_net_ratio"),
                "quote_pressure_ratio": opt.get("quote_pressure_ratio"),
                "dealer_pressure_proxy": opt.get("dealer_pressure_proxy"),
                "dealer_pressure_bucket": dealer_pressure_bucket(_float(opt.get("dealer_pressure_proxy"))),
                "wall_transition": opt.get("wall_transition"),
                "wall_transition_score": opt.get("wall_transition_score"),
                "put_call_oi": opt.get("put_call_oi"),
                "skew_ratio": opt.get("skew_ratio"),
                "n_contracts": opt.get("n_contracts"),
                "total_oi": opt.get("total_oi"),
            }
        )
        prev_option_rows = rows
        prev_option_feature = opt
        prev_option_spot = close

    if len(feature_rows) < 5:
        raise SystemExit(f"insufficient option feature rows for {ticker}: {len(feature_rows)}")

    strategy_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    for strategy in ("buy_hold", "ema200_below_buy", "ema200_above_buy", "trend_only", "gamma_tanh_only", "gamma_v2_entry_alpha", "ivhv_timing", "ivhv_gamma_tanh", "ivhv_band_timing", "ivhv_band_gamma_tanh", "ivhv_band_gamma_v2"):
        rows = simulate_strategy(feature_rows, strategy=strategy, cost_bps=args.cost_bps)
        strategy_rows.extend(rows)
        summary[strategy] = _stats(rows)

    diagnostics = {
        "iv_bucket_forward_returns": bucket_diagnostics(feature_rows, "iv_bucket"),
        "gamma_state_forward_returns": bucket_diagnostics(feature_rows, "gamma_state"),
        "iv_bucket_trend_on_forward_returns": bucket_diagnostics([row for row in feature_rows if row["trend_on"]], "iv_bucket"),
        "gamma_state_trend_on_forward_returns": bucket_diagnostics([row for row in feature_rows if row["trend_on"]], "gamma_state"),
        "dealer_pressure_trend_on_forward_returns": bucket_diagnostics([row for row in feature_rows if row["trend_on"]], "dealer_pressure_bucket"),
        "wall_transition_trend_on_forward_returns": bucket_diagnostics([row for row in feature_rows if row["trend_on"]], "wall_transition"),
    }
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "OnclickMedia free API",
        "config": {
            "ticker": ticker,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "min_dte": args.min_dte,
            "max_dte": args.max_dte,
            "hv_window": args.hv_window,
            "gamma_half_life_days": args.gamma_half_life_days,
            "cost_bps": args.cost_bps,
            "n_stock_rows": len(price_series),
            "split_adjustment_events": split_events,
            "n_option_fetch_dates": option_fetch_dates,
            "skipped_option_dates": skipped_option_dates,
            "n_contract_rows": len(normalized_contracts),
            "option_cache_hits": option_cache_hits,
            "stock_cached": stock_cached,
        },
        "urls": urls,
        "summary": summary,
        "diagnostics": diagnostics,
        "verdict": verdict(summary),
    }
    write_csv(run_dir / "features.csv", feature_rows)
    write_csv(run_dir / "strategy_daily.csv", strategy_rows)
    write_csv(run_dir / "normalized_options_chain_quotes.csv", normalized_contracts)
    (run_dir / "backtest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "backtest.md").write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
