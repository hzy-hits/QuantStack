"""Gamma spring model for US options positioning.

This is a classical spring/oscillator approximation, not a pricing model.
It translates option-chain gamma into stock-decision context:

- positive signed GEX near spot -> pinning / mean-reversion well
- negative signed GEX near spot -> acceleration risk
- weak gamma mass -> price likely dominated by news, trend, and liquidity

Dealer positioning is not directly observable. The sign convention here is the
standard report convention: calls positive, puts negative. The output is a stock
management overlay for exposure, stop, and chase/no-add decisions; it is not an
option-trading instruction and does not change source-evidence status.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

import duckdb


INDEX_CONTEXT = {"SPY", "QQQ", "SMH", "IWM", "DIA"}


def _finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in {float("inf"), float("-inf")}:
        return None
    return out


def _fmt_money_mm(value: float | None) -> str:
    if value is None:
        return "-"
    return f"${value / 1_000_000:.1f}mm"


def _fmt_pct(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.{digits}f}%"


def _fmt_band(value: Any) -> str:
    if not value or not isinstance(value, (list, tuple)) or len(value) < 2:
        return "-"
    lo = _finite_float(value[0])
    hi = _finite_float(value[1])
    if lo is None or hi is None:
        return "-"
    return f"{lo:.2f}-{hi:.2f}"


def _weighted_mean(items: list[tuple[float, float]]) -> float | None:
    cleaned = [(value, max(weight, 0.0)) for value, weight in items]
    denom = sum(weight for _value, weight in cleaned)
    if denom <= 0:
        return None
    return sum(value * weight for value, weight in cleaned) / denom


def _round_band(value: tuple[float, float] | list[float] | None) -> list[float] | None:
    if not value or len(value) < 2:
        return None
    lo = _finite_float(value[0])
    hi = _finite_float(value[1])
    if lo is None or hi is None:
        return None
    return [round(lo, 4), round(hi, 4)]


def _norm_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _bs_gamma(spot: float, strike: float, implied_volatility: float | None, days_to_exp: float | None) -> float | None:
    iv = _finite_float(implied_volatility)
    dte = _finite_float(days_to_exp)
    if spot <= 0 or strike <= 0 or iv is None or iv <= 0 or iv > 5.0 or dte is None or dte < 0:
        return None
    t = max(dte, 1.0) / 365.0
    vol_time = iv * math.sqrt(t)
    if vol_time <= 0:
        return None
    try:
        d1 = (math.log(spot / strike) + 0.5 * iv * iv * t) / vol_time
    except (ValueError, ZeroDivisionError):
        return None
    gamma = _norm_pdf(d1) / (spot * vol_time)
    return gamma if gamma > 0 and math.isfinite(gamma) else None


def _scenario_gamma(
    current_gamma: float | None,
    current_spot: float,
    scenario_spot: float,
    strike: float,
    implied_volatility: float | None,
    days_to_exp: float | None,
) -> tuple[float | None, str]:
    bs_gamma = _bs_gamma(scenario_spot, strike, implied_volatility, days_to_exp)
    if bs_gamma is not None:
        return bs_gamma, "bs_iv_repriced"
    gamma = _finite_float(current_gamma)
    if gamma is None or gamma <= 0:
        return None, "missing_gamma"
    if current_spot > 0 and scenario_spot > 0:
        return gamma * (current_spot / scenario_spot) ** 2, "static_gamma_fallback"
    return gamma, "static_gamma_fallback"


def _interpolate_zero(left_price: float, left_gex: float, right_price: float, right_gex: float) -> float | None:
    denom = right_gex - left_gex
    if abs(denom) <= 1e-12:
        return None
    weight = -left_gex / denom
    if weight < 0 or weight > 1:
        return None
    return left_price + weight * (right_price - left_price)


def _contiguous_intervals(points: list[dict[str, float]], predicate: Any) -> list[tuple[float, float]]:
    intervals: list[tuple[float, float]] = []
    start: float | None = None
    last: float | None = None
    for point in points:
        price = point["price"]
        if predicate(point):
            if start is None:
                start = price
            last = price
        elif start is not None and last is not None:
            intervals.append((start, last))
            start = None
            last = None
    if start is not None and last is not None:
        intervals.append((start, last))
    return intervals


def _select_interval(intervals: list[tuple[float, float]], spot: float) -> tuple[float, float] | None:
    if not intervals:
        return None
    for lo, hi in intervals:
        if lo <= spot <= hi:
            return lo, hi
    return min(intervals, key=lambda band: min(abs(spot - band[0]), abs(spot - band[1])))


@dataclass
class GammaSpringRow:
    symbol: str
    spot: float
    center_strike: float
    max_wall_strike: float
    wall_below: float | None
    wall_above: float | None
    net_gex_1pct: float
    abs_gex_1pct: float
    call_gex_1pct: float
    put_gex_1pct: float
    volume_gex_1pct: float
    total_oi: int
    total_volume: int
    displacement_pct: float
    well_width_pct: float
    net_gamma_ratio: float
    damping_ratio: float
    potential_energy: float
    max_positive_wall: float | None = None
    max_negative_wall: float | None = None
    oi_change_net_ratio: float | None = None
    volume_net_ratio: float | None = None
    skew_ratio: float | None = None
    dealer_pressure_proxy: float = 0.0
    dealer_pressure_bucket: str = "neutral_pressure"
    wall_transition: str = "NO_TRANSITION"
    wall_transition_score: float = 0.0
    gamma_v2_multiplier: float = 1.0
    management_signal: str = "hold_context_only"
    stiffness_percentile: float = 0.0
    state: str = "MIXED_GAMMA_FIELD"
    gex_curve_state: str = "UNKNOWN_GEX_CURVE"
    zero_gamma_levels: list[float] = field(default_factory=list)
    zero_gamma_band: tuple[float, float] | None = None
    positive_gex_pin_zone: tuple[float, float] | None = None
    negative_gex_accel_zone: tuple[float, float] | None = None
    call_wall_strike: float | None = None
    put_wall_strike: float | None = None
    gex_flip_distance_pct: float | None = None
    gex_curve_quality: str = "unknown"
    gex_curve_points: list[dict[str, float]] = field(default_factory=list)
    spot_source: str = "options_chain_current_price"
    spot_price_date: str | None = None
    current_net_gex_1pct: float = 0.0
    current_net_gamma_ratio: float = 0.0
    gex_flip_regime: str = "mixed_gamma"
    gex_flip_model_score: float = 0.5

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "spot": round(self.spot, 4),
            "center_strike": round(self.center_strike, 4),
            "max_wall_strike": round(self.max_wall_strike, 4),
            "wall_below": round(self.wall_below, 4) if self.wall_below is not None else None,
            "wall_above": round(self.wall_above, 4) if self.wall_above is not None else None,
            "net_gex_1pct": round(self.net_gex_1pct, 2),
            "abs_gex_1pct": round(self.abs_gex_1pct, 2),
            "call_gex_1pct": round(self.call_gex_1pct, 2),
            "put_gex_1pct": round(self.put_gex_1pct, 2),
            "volume_gex_1pct": round(self.volume_gex_1pct, 2),
            "total_oi": self.total_oi,
            "total_volume": self.total_volume,
            "displacement_pct": round(self.displacement_pct, 4),
            "well_width_pct": round(self.well_width_pct, 4),
            "net_gamma_ratio": round(self.net_gamma_ratio, 4),
            "damping_ratio": round(self.damping_ratio, 4),
            "potential_energy": round(self.potential_energy, 6),
            "max_positive_wall": round(self.max_positive_wall, 4) if self.max_positive_wall is not None else None,
            "max_negative_wall": round(self.max_negative_wall, 4) if self.max_negative_wall is not None else None,
            "oi_change_net_ratio": round(self.oi_change_net_ratio, 4) if self.oi_change_net_ratio is not None else None,
            "volume_net_ratio": round(self.volume_net_ratio, 4) if self.volume_net_ratio is not None else None,
            "skew_ratio": round(self.skew_ratio, 4) if self.skew_ratio is not None else None,
            "dealer_pressure_proxy": round(self.dealer_pressure_proxy, 4),
            "dealer_pressure_bucket": self.dealer_pressure_bucket,
            "wall_transition": self.wall_transition,
            "wall_transition_score": round(self.wall_transition_score, 4),
            "gamma_v2_multiplier": round(self.gamma_v2_multiplier, 4),
            "management_signal": self.management_signal,
            "stiffness_percentile": round(self.stiffness_percentile, 4),
            "state": self.state,
            "gex_curve_state": self.gex_curve_state,
            "zero_gamma_levels": [round(value, 4) for value in self.zero_gamma_levels],
            "zero_gamma_band": _round_band(self.zero_gamma_band),
            "positive_gex_pin_zone": _round_band(self.positive_gex_pin_zone),
            "negative_gex_accel_zone": _round_band(self.negative_gex_accel_zone),
            "call_wall_strike": round(self.call_wall_strike, 4) if self.call_wall_strike is not None else None,
            "put_wall_strike": round(self.put_wall_strike, 4) if self.put_wall_strike is not None else None,
            "gex_flip_distance_pct": round(self.gex_flip_distance_pct, 4) if self.gex_flip_distance_pct is not None else None,
            "gex_curve_quality": self.gex_curve_quality,
            "gex_curve_points": [
                {
                    "price": round(point["price"], 4),
                    "net_gex_1pct": round(point["net_gex_1pct"], 2),
                    "net_gamma_ratio": round(point["net_gamma_ratio"], 4),
                }
                for point in self.gex_curve_points
            ],
            "spot_source": self.spot_source,
            "spot_price_date": self.spot_price_date,
            "current_net_gex_1pct": round(self.current_net_gex_1pct, 2),
            "current_net_gamma_ratio": round(self.current_net_gamma_ratio, 4),
            "gex_flip_regime": self.gex_flip_regime,
            "gex_flip_model_score": round(self.gex_flip_model_score, 4),
        }


def _resolve_effective_date(con: duckdb.DuckDBPyConnection, as_of: date) -> date | None:
    try:
        row = con.execute(
            "SELECT MAX(as_of) FROM options_chain_quotes WHERE as_of <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()
    except duckdb.Error:
        return None
    if not row or not row[0]:
        return None
    value = row[0]
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _resolve_previous_date(con: duckdb.DuckDBPyConnection, effective: date) -> date | None:
    try:
        row = con.execute(
            "SELECT MAX(as_of) FROM options_chain_quotes WHERE as_of < CAST(? AS DATE)",
            [effective.isoformat()],
        ).fetchone()
    except duckdb.Error:
        return None
    if not row or not row[0]:
        return None
    value = row[0]
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _load_spot_overrides(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date | None,
) -> dict[str, dict[str, Any]]:
    if as_of is None or not symbols:
        return {}
    placeholders = ", ".join("?" for _ in symbols)
    try:
        rows = con.execute(
            f"""
            WITH latest AS (
                SELECT symbol, MAX(date) AS price_date
                FROM prices_daily
                WHERE symbol IN ({placeholders})
                  AND date <= CAST(? AS DATE)
                  AND close IS NOT NULL
                GROUP BY symbol
            )
            SELECT p.symbol, p.date, p.close
            FROM prices_daily p
            JOIN latest l ON l.symbol = p.symbol AND l.price_date = p.date
            """,
            [*symbols, as_of.isoformat()],
        ).fetchall()
    except duckdb.Error:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for symbol, price_date, close in rows:
        spot = _finite_float(close)
        if spot is None or spot <= 0:
            continue
        out[str(symbol).upper()] = {
            "spot": spot,
            "spot_source": "prices_daily_close",
            "spot_price_date": price_date.isoformat() if hasattr(price_date, "isoformat") else str(price_date),
        }
    return out


def _percentile_ranks(rows: list[GammaSpringRow]) -> None:
    if not rows:
        return
    ordered = sorted(rows, key=lambda r: r.abs_gex_1pct)
    denom = max(1, len(ordered) - 1)
    for idx, row in enumerate(ordered):
        row.stiffness_percentile = idx / denom


def _classify(row: GammaSpringRow) -> str:
    if row.stiffness_percentile <= 0.20 and row.abs_gex_1pct < 1_000_000:
        return "LOW_STIFFNESS"
    if row.net_gamma_ratio >= 0.25:
        if abs(row.displacement_pct) <= max(0.01, row.well_width_pct):
            return "PINNED_GAMMA_WELL"
        return "GAMMA_REVERSION_BAND"
    if row.net_gamma_ratio <= -0.25:
        return "NEGATIVE_GAMMA_ACCELERATOR"
    return "MIXED_GAMMA_FIELD"


def _pressure_bucket(value: float | None) -> str:
    if value is None:
        return "missing_pressure"
    if value <= -0.25:
        return "negative_pressure"
    if value >= 0.25:
        return "positive_pressure"
    return "neutral_pressure"


def _soft_tanh_gate(value: float, width: float) -> float:
    return 0.5 * (1.0 + math.tanh(value / max(width, 1e-6)))


def _gamma_v2_multiplier(row: GammaSpringRow) -> float:
    penalty = 0.0
    dealer = row.dealer_pressure_proxy
    if dealer < -0.25:
        penalty += 0.18 * _soft_tanh_gate((-dealer) - 0.25, 0.18)
    if row.wall_transition_score < -0.10:
        penalty += 0.20 * _soft_tanh_gate((-row.wall_transition_score) - 0.10, 0.18)
    if row.net_gamma_ratio >= 0.25 and row.displacement_pct > 0.05:
        penalty += 0.08 * _soft_tanh_gate(row.displacement_pct - 0.05, 0.03)
    if row.net_gamma_ratio <= -0.25 and dealer < 0:
        penalty += 0.10 * _soft_tanh_gate((-row.net_gamma_ratio) - 0.25, 0.15)
    return max(0.60, min(1.0, 1.0 - penalty))


def _management_signal(row: GammaSpringRow) -> str:
    if row.wall_transition_score <= -0.30:
        return "reduce_or_tighten_stop"
    if row.dealer_pressure_proxy <= -0.25:
        return "no_add_tighten_stop"
    if row.net_gamma_ratio >= 0.25 and row.displacement_pct > 0.05:
        return "do_not_chase_above_wall"
    if row.wall_transition_score >= 0.30 and row.dealer_pressure_proxy >= 0:
        return "breakout_hold_ok"
    if row.state == "PINNED_GAMMA_WELL":
        return "wall_pinning_watch"
    return "hold_context_only"


def _build_gex_curve(
    raw_rows: list[tuple[Any, ...]],
    *,
    spot: float,
    by_strike: dict[float, dict[str, float]],
) -> dict[str, Any]:
    if spot <= 0:
        return {
            "gex_curve_state": "UNKNOWN_GEX_CURVE",
            "zero_gamma_levels": [],
            "zero_gamma_band": None,
            "positive_gex_pin_zone": None,
            "negative_gex_accel_zone": None,
            "call_wall_strike": None,
            "put_wall_strike": None,
            "gex_flip_distance_pct": None,
            "gex_curve_quality": "missing_spot",
            "gex_curve_points": [],
        }

    curve: list[dict[str, float]] = []
    bs_used = 0
    fallback_used = 0
    for idx in range(41):
        scenario_spot = spot * (0.80 + idx * 0.01)
        net = 0.0
        total_abs = 0.0
        call_gex = 0.0
        put_gex = 0.0
        for (
            _contract_symbol,
            _current_price,
            option_type,
            strike,
            gamma,
            open_interest,
            _volume,
            implied_volatility,
            days_to_exp,
        ) in raw_rows:
            k = _finite_float(strike)
            oi = int(open_interest or 0)
            if k is None or k <= 0 or oi <= 0:
                continue
            scenario_gamma, quality = _scenario_gamma(
                _finite_float(gamma),
                spot,
                scenario_spot,
                k,
                _finite_float(implied_volatility),
                _finite_float(days_to_exp),
            )
            if scenario_gamma is None or scenario_gamma <= 0:
                continue
            if quality == "bs_iv_repriced":
                bs_used += 1
            else:
                fallback_used += 1
            sign = 1.0 if str(option_type).lower() == "call" else -1.0
            gex = scenario_gamma * 100.0 * scenario_spot * scenario_spot * 0.01 * oi
            signed = sign * gex
            net += signed
            total_abs += abs(gex)
            if sign > 0:
                call_gex += gex
            else:
                put_gex += gex
        curve.append(
            {
                "price": scenario_spot,
                "net_gex_1pct": net,
                "abs_gex_1pct": total_abs,
                "call_gex_1pct": call_gex,
                "put_gex_1pct": put_gex,
                "net_gamma_ratio": net / total_abs if total_abs > 0 else 0.0,
            }
        )

    zero_levels: list[float] = []
    for left, right in zip(curve, curve[1:]):
        left_gex = left["net_gex_1pct"]
        right_gex = right["net_gex_1pct"]
        if abs(left_gex) <= 1e-9:
            zero_levels.append(left["price"])
        elif left_gex * right_gex < 0:
            zero = _interpolate_zero(left["price"], left_gex, right["price"], right_gex)
            if zero is not None:
                zero_levels.append(zero)
    if curve and abs(curve[-1]["net_gex_1pct"]) <= 1e-9:
        zero_levels.append(curve[-1]["price"])
    zero_levels = sorted({round(level, 6) for level in zero_levels})

    positive_intervals = _contiguous_intervals(curve, lambda point: point["net_gex_1pct"] > 0)
    negative_intervals = _contiguous_intervals(curve, lambda point: point["net_gex_1pct"] < 0)
    positive_zone = _select_interval(positive_intervals, spot)
    negative_zone = _select_interval(negative_intervals, spot)
    nearest_zero = min(zero_levels, key=lambda level: abs(level - spot)) if zero_levels else None
    grid_step = spot * 0.01
    zero_band = (nearest_zero - 0.5 * grid_step, nearest_zero + 0.5 * grid_step) if nearest_zero else None
    flip_distance = (nearest_zero - spot) / spot if nearest_zero is not None and spot > 0 else None

    current_point = min(curve, key=lambda point: abs(point["price"] - spot)) if curve else {}
    current_ratio = float(current_point.get("net_gamma_ratio") or 0.0)
    current_net_gex = float(current_point.get("net_gex_1pct") or 0.0)
    near_zero = flip_distance is not None and abs(flip_distance) <= 0.015
    if near_zero:
        curve_state = "ZERO_GAMMA_TRANSITION"
    elif current_ratio >= 0.10:
        curve_state = "POSITIVE_GEX_PIN_ZONE"
    elif current_ratio <= -0.10:
        curve_state = "NEGATIVE_GEX_ACCEL_ZONE"
    else:
        curve_state = "MIXED_GEX_CURVE"
    if curve_state == "ZERO_GAMMA_TRANSITION":
        flip_regime = "near_flip_transition"
        flip_model_score = 0.50
    elif current_ratio >= 0.10 and (nearest_zero is None or spot >= nearest_zero):
        flip_regime = "positive_spring"
        flip_model_score = 0.66
    elif current_ratio <= -0.10:
        flip_regime = "negative_acceleration_unconfirmed"
        flip_model_score = 0.36
    elif current_ratio >= 0.10:
        flip_regime = "positive_gamma_below_flip"
        flip_model_score = 0.56
    else:
        flip_regime = "mixed_gamma"
        flip_model_score = 0.50

    call_wall_candidates = [(strike, value["call"]) for strike, value in by_strike.items() if value["call"] > 0]
    put_wall_candidates = [(strike, value["put"]) for strike, value in by_strike.items() if value["put"] > 0]
    call_wall = max(call_wall_candidates, key=lambda item: item[1])[0] if call_wall_candidates else None
    put_wall = max(put_wall_candidates, key=lambda item: item[1])[0] if put_wall_candidates else None
    total_quality = bs_used + fallback_used
    if total_quality <= 0:
        curve_quality = "no_valid_gex_curve"
    elif bs_used / total_quality >= 0.50:
        curve_quality = "bs_iv_repriced"
    else:
        curve_quality = "static_gamma_fallback"

    return {
        "gex_curve_state": curve_state,
        "zero_gamma_levels": zero_levels,
        "zero_gamma_band": zero_band,
        "positive_gex_pin_zone": positive_zone,
        "negative_gex_accel_zone": negative_zone,
        "call_wall_strike": call_wall,
        "put_wall_strike": put_wall,
        "gex_flip_distance_pct": flip_distance,
        "gex_curve_quality": curve_quality,
        "gex_curve_points": curve,
        "current_net_gex_1pct": current_net_gex,
        "current_net_gamma_ratio": current_ratio,
        "gex_flip_regime": flip_regime,
        "gex_flip_model_score": flip_model_score,
    }


def _build_row(
    symbol: str,
    raw_rows: list[tuple[Any, ...]],
    *,
    previous_raw_rows: list[tuple[Any, ...]] | None = None,
    previous_row: GammaSpringRow | None = None,
    spot_override: dict[str, Any] | None = None,
) -> GammaSpringRow | None:
    spots = [_finite_float(r[1]) for r in raw_rows]
    spots = [s for s in spots if s is not None and s > 0]
    if not spots:
        return None
    override_spot = _finite_float((spot_override or {}).get("spot"))
    spot = override_spot if override_spot is not None and override_spot > 0 else median(spots)
    spot_source = str((spot_override or {}).get("spot_source") or "options_chain_current_price")
    spot_price_date = (spot_override or {}).get("spot_price_date")

    by_strike: dict[float, dict[str, float]] = defaultdict(
        lambda: {"signed": 0.0, "abs": 0.0, "call": 0.0, "put": 0.0, "volume": 0.0, "oi": 0.0, "vol": 0.0}
    )
    total_abs = 0.0
    net = 0.0
    call_gex = 0.0
    put_gex = 0.0
    volume_gex = 0.0
    volume_signed_gex = 0.0
    volume_abs_gex = 0.0
    oi_change_signed_gex = 0.0
    oi_change_abs_gex = 0.0
    total_oi = 0
    total_volume = 0
    put_skew_items: list[tuple[float, float]] = []
    call_skew_items: list[tuple[float, float]] = []
    previous_by_contract = {
        str(r[0]): r for r in (previous_raw_rows or []) if r and r[0] is not None
    }

    for (
        contract_symbol,
        current_price,
        option_type,
        strike,
        gamma,
        open_interest,
        volume,
        implied_volatility,
        _days_to_exp,
    ) in raw_rows:
        k = _finite_float(strike)
        g = _finite_float(gamma)
        iv = _finite_float(implied_volatility)
        oi = int(open_interest or 0)
        vol = int(volume or 0)
        if k is None or k <= 0 or g is None or g <= 0 or oi <= 0:
            continue
        moneyness = k / spot
        if iv is not None and iv > 0 and str(option_type).lower() == "put" and 0.90 <= moneyness <= 0.98:
            put_skew_items.append((iv, max(oi, vol, 1.0)))
        if iv is not None and iv > 0 and str(option_type).lower() == "call" and 1.02 <= moneyness <= 1.10:
            call_skew_items.append((iv, max(oi, vol, 1.0)))
        sign = 1.0 if str(option_type).lower() == "call" else -1.0
        gex_unit = g * 100.0 * spot * spot * 0.01
        gex = gex_unit * oi
        signed = sign * gex
        flow = gex_unit * max(vol, 0)
        bucket = by_strike[k]
        bucket["signed"] += signed
        bucket["abs"] += abs(gex)
        bucket["volume"] += abs(flow)
        bucket["oi"] += oi
        bucket["vol"] += max(vol, 0)
        if sign > 0:
            bucket["call"] += gex
            call_gex += gex
        else:
            bucket["put"] += gex
            put_gex += gex
        net += signed
        total_abs += abs(gex)
        volume_gex += abs(flow)
        volume_signed_gex += sign * flow
        volume_abs_gex += abs(flow)
        total_oi += oi
        total_volume += max(vol, 0)
        previous = previous_by_contract.get(str(contract_symbol))
        if previous is not None:
            prev_oi = int(previous[5] or 0)
            oi_change = oi - prev_oi
            if oi_change:
                oi_change_signed_gex += sign * oi_change * gex_unit
                oi_change_abs_gex += abs(oi_change) * gex_unit

    if total_abs <= 0 or not by_strike:
        return None

    center = sum(k * v["abs"] for k, v in by_strike.items()) / total_abs
    variance = sum(v["abs"] * ((k - center) / center) ** 2 for k, v in by_strike.items()) / total_abs
    well_width = variance ** 0.5
    max_wall = max(by_strike.items(), key=lambda kv: kv[1]["abs"])[0]
    positive_walls = [(k, v) for k, v in by_strike.items() if v["signed"] > 0]
    negative_walls = [(k, v) for k, v in by_strike.items() if v["signed"] < 0]
    max_positive_wall = max(positive_walls, key=lambda kv: kv[1]["signed"])[0] if positive_walls else None
    max_negative_wall = min(negative_walls, key=lambda kv: kv[1]["signed"])[0] if negative_walls else None
    below = [kv for kv in by_strike.items() if kv[0] <= spot]
    above = [kv for kv in by_strike.items() if kv[0] >= spot]
    wall_below = max(below, key=lambda kv: kv[1]["abs"])[0] if below else None
    wall_above = max(above, key=lambda kv: kv[1]["abs"])[0] if above else None
    displacement = (spot - center) / center if center > 0 else 0.0
    net_ratio = net / total_abs if total_abs > 0 else 0.0
    damping = (volume_gex / total_abs) if total_abs > 0 else 0.0
    potential = 0.5 * (total_abs / 1_000_000.0) * displacement * displacement
    oi_change_net_ratio = oi_change_signed_gex / oi_change_abs_gex if oi_change_abs_gex > 0 else None
    volume_net_ratio = volume_signed_gex / volume_abs_gex if volume_abs_gex > 0 else None
    put_skew = _weighted_mean(put_skew_items)
    call_skew = _weighted_mean(call_skew_items)
    skew_ratio = put_skew / call_skew if put_skew and call_skew and call_skew > 0 else None
    skew_pressure = math.tanh(((skew_ratio or 1.05) - 1.05) / 0.20)
    dealer_pressure_proxy = max(
        -1.0,
        min(
            1.0,
            0.55 * (oi_change_net_ratio or 0.0)
            + 0.25 * (volume_net_ratio or 0.0)
            - 0.25 * skew_pressure,
        ),
    )
    wall_transition = "NO_TRANSITION"
    wall_transition_score = 0.0
    if previous_row is not None:
        prev_center_side = previous_row.spot - previous_row.center_strike
        cur_center_side = spot - center
        if prev_center_side <= 0 < cur_center_side:
            wall_transition = "CENTER_CROSS_UP"
            wall_transition_score += 0.45
        elif prev_center_side >= 0 > cur_center_side:
            wall_transition = "CENTER_CROSS_DOWN"
            wall_transition_score -= 0.45
        prev_wall_side = previous_row.spot - previous_row.max_wall_strike
        cur_wall_side = spot - max_wall
        if prev_wall_side <= 0 < cur_wall_side:
            wall_transition = "WALL_BREAK_UP" if wall_transition == "NO_TRANSITION" else f"{wall_transition}+WALL_BREAK_UP"
            wall_transition_score += 0.35
        elif prev_wall_side >= 0 > cur_wall_side:
            wall_transition = "WALL_BREAK_DOWN" if wall_transition == "NO_TRANSITION" else f"{wall_transition}+WALL_BREAK_DOWN"
            wall_transition_score -= 0.35
    wall_transition_score = max(-1.0, min(1.0, wall_transition_score))
    curve_fields = _build_gex_curve(raw_rows, spot=spot, by_strike=by_strike)
    if curve_fields["gex_curve_state"] == "NEGATIVE_GEX_ACCEL_ZONE":
        if wall_transition_score >= 0.25 and dealer_pressure_proxy >= 0.10:
            curve_fields["gex_flip_regime"] = "negative_acceleration_breakout"
            curve_fields["gex_flip_model_score"] = 0.62
        elif wall_transition_score <= -0.10 or dealer_pressure_proxy < 0:
            curve_fields["gex_flip_regime"] = "negative_acceleration_risk_off"
            curve_fields["gex_flip_model_score"] = 0.30

    row = GammaSpringRow(
        symbol=symbol,
        spot=spot,
        center_strike=center,
        max_wall_strike=max_wall,
        wall_below=wall_below,
        wall_above=wall_above,
        net_gex_1pct=net,
        abs_gex_1pct=total_abs,
        call_gex_1pct=call_gex,
        put_gex_1pct=put_gex,
        volume_gex_1pct=volume_gex,
        total_oi=total_oi,
        total_volume=total_volume,
        displacement_pct=displacement,
        well_width_pct=well_width,
        net_gamma_ratio=net_ratio,
        damping_ratio=damping,
        potential_energy=potential,
        max_positive_wall=max_positive_wall,
        max_negative_wall=max_negative_wall,
        oi_change_net_ratio=oi_change_net_ratio,
        volume_net_ratio=volume_net_ratio,
        skew_ratio=skew_ratio,
        dealer_pressure_proxy=dealer_pressure_proxy,
        dealer_pressure_bucket=_pressure_bucket(dealer_pressure_proxy),
        wall_transition=wall_transition,
        wall_transition_score=wall_transition_score,
        gex_curve_state=curve_fields["gex_curve_state"],
        zero_gamma_levels=curve_fields["zero_gamma_levels"],
        zero_gamma_band=curve_fields["zero_gamma_band"],
        positive_gex_pin_zone=curve_fields["positive_gex_pin_zone"],
        negative_gex_accel_zone=curve_fields["negative_gex_accel_zone"],
        call_wall_strike=curve_fields["call_wall_strike"],
        put_wall_strike=curve_fields["put_wall_strike"],
        gex_flip_distance_pct=curve_fields["gex_flip_distance_pct"],
        gex_curve_quality=curve_fields["gex_curve_quality"],
        gex_curve_points=curve_fields["gex_curve_points"],
        spot_source=spot_source,
        spot_price_date=str(spot_price_date) if spot_price_date else None,
        current_net_gex_1pct=curve_fields["current_net_gex_1pct"],
        current_net_gamma_ratio=curve_fields["current_net_gamma_ratio"],
        gex_flip_regime=curve_fields["gex_flip_regime"],
        gex_flip_model_score=curve_fields["gex_flip_model_score"],
    )
    row.gamma_v2_multiplier = _gamma_v2_multiplier(row)
    row.management_signal = _management_signal(row)
    return row


def build_gamma_spring_snapshot(
    us_db: Path,
    symbols: list[str],
    as_of: date,
    *,
    max_dte: int = 45,
) -> dict[str, Any]:
    syms = sorted({str(s).upper().strip() for s in symbols if str(s or "").strip()})
    if not us_db.exists() or not syms:
        return {"as_of": as_of.isoformat(), "effective_date": None, "rows": []}

    placeholders = ", ".join("?" for _ in syms)
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        effective = _resolve_effective_date(con, as_of)
        if effective is None:
            return {"as_of": as_of.isoformat(), "effective_date": None, "rows": []}
        previous_effective = _resolve_previous_date(con, effective)
        try:
            rows = con.execute(
                f"""
                SELECT contract_symbol, current_price, option_type, strike, gamma,
                       open_interest, volume, implied_volatility, days_to_exp, symbol
                FROM options_chain_quotes
                WHERE as_of = CAST(? AS DATE)
                  AND symbol IN ({placeholders})
                  AND days_to_exp BETWEEN 0 AND ?
                  AND gamma IS NOT NULL
                  AND strike IS NOT NULL
                  AND current_price IS NOT NULL
                  AND open_interest IS NOT NULL
                  AND open_interest > 0
                  AND option_type IN ('call', 'put')
                """,
                [effective.isoformat(), *syms, int(max_dte)],
            ).fetchall()
            previous_rows = []
            if previous_effective is not None:
                previous_rows = con.execute(
                    f"""
                    SELECT contract_symbol, current_price, option_type, strike, gamma,
                           open_interest, volume, implied_volatility, days_to_exp, symbol
                    FROM options_chain_quotes
                    WHERE as_of = CAST(? AS DATE)
                      AND symbol IN ({placeholders})
                      AND days_to_exp BETWEEN 0 AND ?
                      AND gamma IS NOT NULL
                      AND strike IS NOT NULL
                      AND current_price IS NOT NULL
                      AND open_interest IS NOT NULL
                      AND open_interest > 0
                      AND option_type IN ('call', 'put')
                    """,
                    [previous_effective.isoformat(), *syms, int(max_dte)],
                ).fetchall()
        except duckdb.Error:
            return {"as_of": as_of.isoformat(), "effective_date": effective.isoformat(), "rows": []}
        spot_overrides = _load_spot_overrides(con, syms, effective)
        previous_spot_overrides = _load_spot_overrides(con, syms, previous_effective)
    finally:
        con.close()

    grouped: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    previous_grouped: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility, days_to_exp, symbol in rows:
        grouped[str(symbol).upper()].append(
            (contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility, days_to_exp)
        )
    for contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility, days_to_exp, symbol in previous_rows:
        previous_grouped[str(symbol).upper()].append(
            (contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility, days_to_exp)
        )

    previous_summary = {
        sym: row
        for sym, raw in previous_grouped.items()
        if (row := _build_row(sym, raw, spot_override=previous_spot_overrides.get(sym))) is not None
    }
    out_rows = [
        row
        for sym, raw in grouped.items()
        if (
            row := _build_row(
                sym,
                raw,
                previous_raw_rows=previous_grouped.get(sym),
                previous_row=previous_summary.get(sym),
                spot_override=spot_overrides.get(sym),
            )
        )
        is not None
    ]
    _percentile_ranks(out_rows)
    for row in out_rows:
        row.state = _classify(row)
    out_rows.sort(
        key=lambda r: (
            0 if r.symbol in INDEX_CONTEXT else 1,
            -r.stiffness_percentile,
            -r.abs_gex_1pct,
        )
    )
    return {
        "as_of": as_of.isoformat(),
        "effective_date": effective.isoformat(),
        "previous_effective_date": previous_effective.isoformat() if previous_effective else None,
        "max_dte": int(max_dte),
        "sign_convention": "call_positive_put_negative",
        "contract": "stock management context only; v3 builds a GEX curve over S0*0.80..S0*1.20; dealer pressure is proxied from OI/volume/skew, not observed",
        "rows": [r.as_dict() for r in out_rows],
    }


def _state_text(state: str) -> str:
    return {
        "PINNED_GAMMA_WELL": "正 gamma 势能井,提示墙位/支撑阻力,不自动降 R",
        "GAMMA_REVERSION_BAND": "正 gamma 但价格偏离中心,关注回拉/突破确认",
        "NEGATIVE_GAMMA_ACCELERATOR": "负 gamma 加速器,跌破/突破后容易放大",
        "LOW_STIFFNESS": "gamma 约束弱,新闻和现货流更主导",
        "MIXED_GAMMA_FIELD": "多空 gamma 混合,看墙位而非单向判断",
    }.get(state, state)


def _management_text(signal: str) -> str:
    return {
        "reduce_or_tighten_stop": "减仓/收紧止损",
        "no_add_tighten_stop": "不加仓/止损收紧",
        "do_not_chase_above_wall": "不追高,等回踩",
        "breakout_hold_ok": "突破后可持有观察",
        "wall_pinning_watch": "墙位吸附,观察突破",
        "hold_context_only": "持有上下文",
    }.get(signal, signal)


def render_gamma_spring_section(payload: dict[str, Any], *, limit: int = 8) -> list[str]:
    snap = payload.get("gamma_spring") or {}
    rows = list(snap.get("rows") or [])
    lines = [
        "## US Gamma Spring v3 / GEX 区间状态机",
        "",
        "- 方法: calls 记正 gamma, puts 记负 gamma; v3 在 S0*0.80 到 S0*1.20 的 price grid 上重算 GEX curve,输出 zero-gamma、positive pin、negative acceleration 区间和 call/put wall。",
        "- 合同: 这是股票买卖管理和仓位上限信号,不是期权交易指令; dealer 仓位不可直接观测,proxy 只能用于风险解释和执行节奏。",
    ]
    if not rows:
        lines += ["- 今日无可用 gamma chain 数据。", ""]
        return lines
    eff = snap.get("effective_date") or "-"
    prev_eff = snap.get("previous_effective_date") or "-"
    lines.append(f"- 数据日 {eff}; 前一链日 {prev_eff}; Gamma v3 区间状态接入 Gamma entry alpha,仅在 AI universe 内生成股票入场和买卖管理信号。")
    lines.append("")
    lines += [
        "| Symbol | State | Curve | Flip regime | Spot | Zero γ | Pin zone | Accel zone | Call/Put wall | Dealer px | Mgmt |",
        "|---|---|---|---|---:|---|---|---|---|---:|---|",
    ]
    for row in rows[:limit]:
        call_wall = row.get("call_wall_strike") or row.get("max_positive_wall")
        put_wall = row.get("put_wall_strike") or row.get("max_negative_wall")
        wall_text = "{call}/{put}".format(
            call=f"{float(call_wall):.2f}" if call_wall is not None else "-",
            put=f"{float(put_wall):.2f}" if put_wall is not None else "-",
        )
        lines.append(
            "| {sym} | {state} | {curve} | {flip} | {spot:.2f} | {zero} | {pin} | {accel} | {wall} | {dealer} | {mgmt} |".format(
                sym=row.get("symbol"),
                state=row.get("state") or "-",
                curve=row.get("gex_curve_state") or "-",
                flip=row.get("gex_flip_regime") or "-",
                spot=float(row.get("spot") or 0.0),
                zero=_fmt_band(row.get("zero_gamma_band")) if row.get("zero_gamma_band") else (
                    ", ".join(f"{float(value):.2f}" for value in (row.get("zero_gamma_levels") or [])[:2]) or "-"
                ),
                pin=_fmt_band(row.get("positive_gex_pin_zone")),
                accel=_fmt_band(row.get("negative_gex_accel_zone")),
                wall=wall_text,
                dealer=_fmt_pct(row.get("dealer_pressure_proxy"), 0),
                mgmt=_management_text(str(row.get("management_signal") or "hold_context_only")),
            )
        )
    lines.append("")
    lines.append("Legend: Zero γ 是最近翻转带; pin zone 为正 GEX 区间,accel zone 为负 GEX 区间; quality 字段保存在 JSON,用于区分 BS IV repricing 和 static gamma fallback。")
    lines.append("")
    return lines
