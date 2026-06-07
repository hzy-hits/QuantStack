#!/usr/bin/env python3
"""Production ranker for current US opportunities.

This ranks the rows produced by Main Strategy V2 and makes the production
contract explicit: only the Alpha Factory-proven V2 stock sleeve can become an
Execution Alpha row. Legacy report buckets remain ranked watch until a sleeve
backtest promotes them.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from quant_bot.analytics import ai_infra_universe


STACK_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_opportunity_ranker"
US_ALPHA_FACTORY_EXECUTION_SLEEVE = "us_v2_stock_probe"
US_GAMMA_V2_ALPHA_SLEEVE = "us_gamma_v2_alpha"
US_ALPHA_FACTORY_EXECUTION_SLEEVES = {
    US_ALPHA_FACTORY_EXECUTION_SLEEVE,
    US_GAMMA_V2_ALPHA_SLEEVE,
    "us_theme_cluster_momentum",
    ai_infra_universe.PRODUCTION_ALPHA_SLEEVE_ID,
}


@dataclass(frozen=True)
class NewsRiskConfig:
    lookback_days: int = 14
    severe_threshold: float = 0.60
    severe_terms: tuple[str, ...] = (
        "fraud",
        "restatement",
        "accounting issue",
        "sec investigation",
        "doj investigation",
        "class action",
        "bankruptcy",
        "going concern",
        "delisting",
        "halted",
    )
    negative_terms: tuple[str, ...] = (
        "downgrade",
        "misses",
        "weak guidance",
        "guidance cut",
        "lawsuit",
        "probe",
        "investigation",
        "short report",
        "cuts target",
    )


@dataclass(frozen=True)
class RankerConfig:
    score_weights: dict[str, float] = field(
        default_factory=lambda: {
            "alpha_factory": 0.26,
            "gamma_v2_alpha": 0.28,
            "setup_quality": 0.18,
            "flow_options_quality": 0.22,
            "price_quality": 0.18,
            "supercycle_priority": 0.08,
            "ai_evidence": 0.06,
            "risk_penalty": -0.14,
            "headline_risk": -0.20,
        }
    )
    headline: NewsRiskConfig = field(default_factory=NewsRiskConfig)
    top_probe_count: int = 5
    secondary_probe_count: int = 10
    event_risk_zero_r: float = 0.60


DEFAULT_CONFIG = RankerConfig()
AI_EVIDENCE_TERMS = {
    "ai",
    "artificial intelligence",
    "datacenter",
    "data center",
    "accelerator",
    "gpu",
    "hbm",
    "memory",
    "optical",
    "cpo",
    "networking",
    "cloud",
    "inference",
    "nuclear",
    "power",
    "grid",
    "space",
    "satellite",
}
SUPPLY_LINK_TERMS = {
    "supplier",
    "supply",
    "customer",
    "contract",
    "order",
    "backlog",
    "design win",
    "capacity",
    "partnership",
    "deal",
}
NEGATIVE_SUPPLY_TERMS = {
    "cancelled purchase order",
    "canceled purchase order",
    "cancelled",
    "canceled",
    "cancel",
    "terminated",
    "termination",
    "lost",
    "losing",
    "risk losing",
    "cut",
    "cuts",
    "reduced",
    "reduction",
    "delay",
    "delayed",
    "breach",
    "breaches",
    "dispute",
    "sours",
    "soured",
    "delayed disclosure",
    "short seller",
    "lawsuit",
    "investigation",
    "downgrade",
    "withdrawn",
    "suspended",
    "halted",
}
US_COMPANY_ALIASES = {
    "AAOI": {"aaoi", "applied optoelectronics"},
    "AMD": {"amd", "advanced micro"},
    "AMZN": {"amazon", "aws", "amzn"},
    "ANET": {"arista", "anet"},
    "AVGO": {"broadcom", "avgo"},
    "CIEN": {"ciena", "cien"},
    "COHR": {"coherent", "cohr"},
    "DELL": {"dell"},
    "GOOGL": {"google", "alphabet", "googl"},
    "HPE": {"hewlett packard enterprise", "hpe"},
    "INTC": {"intel", "intc"},
    "LITE": {"lumentum", "lite"},
    "MRVL": {"marvell", "mrvl"},
    "MSFT": {"microsoft", "azure", "msft"},
    "MU": {"micron", "mu"},
    "NET": {"cloudflare", "net"},
    "NTAP": {"netapp", "ntap"},
    "NVDA": {"nvidia", "nvda"},
    "ORCL": {"oracle", "orcl"},
    "POET": {"poet"},
    "STX": {"seagate", "stx"},
    "WDC": {"western digital", "wdc"},
}


def term_in_text(term: str, text: str) -> bool:
    if not term:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(term.lower())}(?![a-z0-9])", text) is not None


def symbol_alias_in_text(symbol: str, text: str) -> bool:
    aliases = {symbol.lower()} | US_COMPANY_ALIASES.get(symbol.upper(), set())
    return any(alias and term_in_text(alias, text) for alias in aliases)


def as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value)
    return text[:10] if text else None


def round_or_none(value: Any, digits: int = 6) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def fmt_num(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    if not table_exists(con, table):
        return set()
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {str(row[0]) for row in rows}


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def placeholders(values: list[str]) -> str:
    return ",".join("?" for _ in values)


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


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


def latest_options(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "options_alpha"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        WITH latest AS (
            SELECT symbol, MAX(as_of) AS latest_date
            FROM options_alpha
            WHERE as_of <= CAST(? AS DATE)
              AND symbol IN ({placeholders(symbols)})
            GROUP BY symbol
        )
        SELECT oa.symbol, oa.as_of, oa.directional_edge, oa.vol_edge, oa.vrp_edge,
               oa.flow_edge, oa.liquidity_gate, oa.expression, oa.reason, oa.detail_json
        FROM options_alpha oa
        JOIN latest l ON l.symbol = oa.symbol AND l.latest_date = oa.as_of
        """,
        [as_of.isoformat(), *symbols],
    )
    return {normalize_symbol(row.get("symbol")): row for row in rows}


def _weighted_mean(items: list[tuple[float, float]]) -> float | None:
    denom = sum(max(weight, 0.0) for _value, weight in items)
    if denom <= 0:
        return None
    return sum(value * max(weight, 0.0) for value, weight in items) / denom


def _norm_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _bs_gamma_for_curve(spot: float, strike: float, implied_volatility: Any, days_to_exp: Any) -> float | None:
    iv = round_or_none(implied_volatility)
    dte = round_or_none(days_to_exp)
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


def _fallback_curve_gamma(current_gamma: Any, current_spot: float, scenario_spot: float) -> float | None:
    gamma = round_or_none(current_gamma)
    if gamma is None or gamma <= 0:
        return None
    if current_spot > 0 and scenario_spot > 0:
        return gamma * (current_spot / scenario_spot) ** 2
    return gamma


def _band_from_prices(prices: list[float]) -> list[float] | None:
    if not prices:
        return None
    return [round(min(prices), 4), round(max(prices), 4)]


def _gamma_v3_curve_fields(symbol_rows: list[dict[str, Any]], spot: float) -> dict[str, Any]:
    curve: list[dict[str, float]] = []
    bs_used = 0
    fallback_used = 0
    for idx in range(41):
        scenario_spot = spot * (0.80 + idx * 0.01)
        net = 0.0
        total_abs = 0.0
        for row in symbol_rows:
            strike = round_or_none(row.get("strike"))
            oi = int(row.get("open_interest") or 0)
            if strike is None or strike <= 0 or oi <= 0:
                continue
            gamma = _bs_gamma_for_curve(
                scenario_spot,
                strike,
                row.get("implied_volatility"),
                row.get("days_to_exp"),
            )
            if gamma is not None:
                bs_used += 1
            else:
                gamma = _fallback_curve_gamma(row.get("gamma"), spot, scenario_spot)
                if gamma is not None:
                    fallback_used += 1
            if gamma is None or gamma <= 0:
                continue
            sign = 1.0 if str(row.get("option_type") or "").lower() == "call" else -1.0
            gex = gamma * 100.0 * scenario_spot * scenario_spot * 0.01 * oi
            signed = sign * gex
            net += signed
            total_abs += abs(gex)
        curve.append(
            {
                "price": scenario_spot,
                "net_gex_1pct": net,
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
            denom = right_gex - left_gex
            if abs(denom) > 1e-12:
                weight = -left_gex / denom
                if 0 <= weight <= 1:
                    zero_levels.append(left["price"] + weight * (right["price"] - left["price"]))
    zero_levels = sorted({round(level, 4) for level in zero_levels})
    nearest_zero = min(zero_levels, key=lambda level: abs(level - spot)) if zero_levels else None
    flip_distance = (nearest_zero - spot) / spot if nearest_zero is not None and spot > 0 else None
    current = min(curve, key=lambda point: abs(point["price"] - spot)) if curve else {}
    current_ratio = float(current.get("net_gamma_ratio") or 0.0)
    current_net_gex = float(current.get("net_gex_1pct") or 0.0)
    if flip_distance is not None and abs(flip_distance) <= 0.015:
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
    quality_den = bs_used + fallback_used
    quality = "no_valid_gex_curve"
    if quality_den > 0:
        quality = "bs_iv_repriced" if bs_used / quality_den >= 0.50 else "static_gamma_fallback"
    return {
        "gamma_v3_curve_state": curve_state,
        "gamma_v3_zero_gamma_levels": zero_levels,
        "gamma_v3_zero_gamma_band": (
            [round(nearest_zero - spot * 0.005, 4), round(nearest_zero + spot * 0.005, 4)]
            if nearest_zero is not None
            else None
        ),
        "gamma_v3_positive_gex_pin_zone": _band_from_prices([point["price"] for point in curve if point["net_gex_1pct"] > 0]),
        "gamma_v3_negative_gex_accel_zone": _band_from_prices([point["price"] for point in curve if point["net_gex_1pct"] < 0]),
        "gamma_v3_gex_flip_distance_pct": round(flip_distance, 4) if flip_distance is not None else None,
        "gamma_v3_curve_quality": quality,
        "gamma_v3_current_net_gex_1pct": round(current_net_gex, 2),
        "gamma_v3_current_net_gamma_ratio": round(current_ratio, 4),
        "gamma_v3_flip_regime": flip_regime,
        "gamma_v3_flip_model_score": round(flip_model_score, 4),
    }


def _latest_price_spots(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: Any,
) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "prices_daily"):
        return {}
    try:
        rows = rows_as_dicts(
            con,
            f"""
            WITH latest AS (
                SELECT symbol, MAX(date) AS price_date
                FROM prices_daily
                WHERE symbol IN ({placeholders(symbols)})
                  AND date <= CAST(? AS DATE)
                  AND close IS NOT NULL
                GROUP BY symbol
            )
            SELECT p.symbol, p.date AS price_date, p.close
            FROM prices_daily p
            JOIN latest l ON l.symbol = p.symbol AND l.price_date = p.date
            """,
            [*symbols, as_iso(as_of)],
        )
    except duckdb.Error:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        close = round_or_none(row.get("close"))
        if close is None or close <= 0:
            continue
        out[normalize_symbol(row.get("symbol"))] = {
            "spot": close,
            "spot_source": "prices_daily_close",
            "spot_price_date": as_iso(row.get("price_date")),
        }
    return out


def latest_gamma_v2_alpha(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    *,
    max_dte: int = 45,
) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "options_chain_quotes"):
        return {}
    cols = table_columns(con, "options_chain_quotes")
    required = {"symbol", "as_of", "current_price", "contract_symbol", "option_type", "strike", "gamma", "open_interest"}
    if not required.issubset(cols):
        return {}
    volume_expr = "COALESCE(volume, 0)" if "volume" in cols else "0"
    iv_expr = "implied_volatility" if "implied_volatility" in cols else "NULL"
    dte_expr = "days_to_exp" if "days_to_exp" in cols else "NULL"
    dte_filter = "AND days_to_exp BETWEEN 1 AND ?" if "days_to_exp" in cols else ""
    latest_date = con.execute(
        f"""
        SELECT MAX(as_of)
        FROM options_chain_quotes
        WHERE as_of <= CAST(? AS DATE)
          AND symbol IN ({placeholders(symbols)})
          {dte_filter}
        """,
        [as_of.isoformat(), *symbols, *([max_dte] if dte_filter else [])],
    ).fetchone()[0]
    if latest_date is None:
        return {}
    previous_date = con.execute(
        f"""
        SELECT MAX(as_of)
        FROM options_chain_quotes
        WHERE as_of < CAST(? AS DATE)
          AND symbol IN ({placeholders(symbols)})
          {dte_filter}
        """,
        [latest_date, *symbols, *([max_dte] if dte_filter else [])],
    ).fetchone()[0]
    params: list[Any] = [latest_date, *symbols]
    if dte_filter:
        params.append(max_dte)
    rows = rows_as_dicts(
        con,
        f"""
        SELECT symbol, current_price, contract_symbol, option_type, strike, gamma,
               open_interest, {volume_expr} AS volume, {iv_expr} AS implied_volatility,
               {dte_expr} AS days_to_exp
        FROM options_chain_quotes
        WHERE as_of = CAST(? AS DATE)
          AND symbol IN ({placeholders(symbols)})
          {dte_filter}
        """,
        params,
    )
    prev_rows: list[dict[str, Any]] = []
    if previous_date is not None:
        prev_params: list[Any] = [previous_date, *symbols]
        if dte_filter:
            prev_params.append(max_dte)
        prev_rows = rows_as_dicts(
            con,
            f"""
            SELECT symbol, contract_symbol, open_interest
            FROM options_chain_quotes
            WHERE as_of = CAST(? AS DATE)
              AND symbol IN ({placeholders(symbols)})
              {dte_filter}
            """,
            prev_params,
        )
    prev_by_contract = {
        (normalize_symbol(row.get("symbol")), str(row.get("contract_symbol") or "")): int(row.get("open_interest") or 0)
        for row in prev_rows
    }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(normalize_symbol(row.get("symbol")), []).append(row)
    spot_overrides = _latest_price_spots(con, symbols, latest_date)
    out: dict[str, dict[str, Any]] = {}
    for symbol, symbol_rows in grouped.items():
        spots = [
            round_or_none(row.get("current_price"))
            for row in symbol_rows
            if (round_or_none(row.get("current_price")) or 0.0) > 0
        ]
        if not spots:
            continue
        spot = sorted(spots)[len(spots) // 2]
        if not spot or spot <= 0:
            continue
        spot_override = spot_overrides.get(symbol) or {}
        override_spot = round_or_none(spot_override.get("spot"))
        if override_spot is not None and override_spot > 0:
            spot = override_spot
        by_strike: dict[float, dict[str, float]] = {}
        total_abs = net = volume_signed = volume_abs = oi_change_signed = oi_change_abs = 0.0
        put_skew_items: list[tuple[float, float]] = []
        call_skew_items: list[tuple[float, float]] = []
        for row in symbol_rows:
            strike = round_or_none(row.get("strike"))
            gamma = round_or_none(row.get("gamma"))
            oi = int(row.get("open_interest") or 0)
            if strike is None or strike <= 0 or gamma is None or gamma <= 0 or oi <= 0:
                continue
            option_type = str(row.get("option_type") or "").lower()
            sign = 1.0 if option_type == "call" else -1.0
            volume = int(row.get("volume") or 0)
            gex_unit = gamma * 100.0 * spot * spot * 0.01
            gex = gex_unit * oi
            signed = sign * gex
            total_abs += abs(gex)
            net += signed
            flow = gex_unit * max(volume, 0)
            volume_signed += sign * flow
            volume_abs += abs(flow)
            bucket = by_strike.setdefault(strike, {"signed": 0.0, "abs": 0.0, "call": 0.0, "put": 0.0})
            bucket["signed"] += signed
            bucket["abs"] += abs(gex)
            if sign > 0:
                bucket["call"] += gex
            else:
                bucket["put"] += gex
            prev_oi = prev_by_contract.get((symbol, str(row.get("contract_symbol") or "")))
            if prev_oi is not None:
                oi_change = oi - prev_oi
                oi_change_signed += sign * oi_change * gex_unit
                oi_change_abs += abs(oi_change) * gex_unit
            iv = round_or_none(row.get("implied_volatility"))
            moneyness = strike / spot
            if iv and option_type == "put" and 0.90 <= moneyness <= 0.98:
                put_skew_items.append((iv, max(oi, volume, 1.0)))
            if iv and option_type == "call" and 1.02 <= moneyness <= 1.10:
                call_skew_items.append((iv, max(oi, volume, 1.0)))
        if total_abs <= 0 or not by_strike:
            continue
        curve_fields = _gamma_v3_curve_fields(symbol_rows, spot)
        center = sum(strike * value["abs"] for strike, value in by_strike.items()) / total_abs
        max_wall = max(by_strike.items(), key=lambda item: item[1]["abs"])[0]
        displacement = (spot - center) / center if center > 0 else 0.0
        net_ratio = net / total_abs
        oi_change_ratio = oi_change_signed / oi_change_abs if oi_change_abs > 0 else 0.0
        volume_ratio = volume_signed / volume_abs if volume_abs > 0 else 0.0
        put_skew = _weighted_mean(put_skew_items)
        call_skew = _weighted_mean(call_skew_items)
        skew_ratio = put_skew / call_skew if put_skew and call_skew and call_skew > 0 else None
        skew_pressure = math.tanh(((skew_ratio or 1.05) - 1.05) / 0.20)
        dealer_pressure = clamp(
            0.55 * oi_change_ratio + 0.25 * volume_ratio - 0.25 * skew_pressure,
            -1.0,
            1.0,
        )
        wall_transition_score = 0.0
        wall_transition = "NO_TRANSITION"
        if center and spot > center:
            wall_transition_score += 0.12
        if spot > max_wall:
            wall_transition_score += 0.18
        if spot < center:
            wall_transition_score -= 0.12
        if spot < max_wall:
            wall_transition_score -= 0.18
        wall_transition_score = clamp(wall_transition_score, -1.0, 1.0)
        if wall_transition_score >= 0.25:
            wall_transition = "WALL_BREAK_UP"
        elif wall_transition_score <= -0.25:
            wall_transition = "WALL_BREAK_DOWN"
        curve_state = str(curve_fields.get("gamma_v3_curve_state") or "MIXED_GEX_CURVE")
        flip_regime = str(curve_fields.get("gamma_v3_flip_regime") or "mixed_gamma")
        flip_part = float(curve_fields.get("gamma_v3_flip_model_score") or 0.50)
        if curve_state == "NEGATIVE_GEX_ACCEL_ZONE":
            if wall_transition_score >= 0.25 and dealer_pressure >= 0.10:
                flip_regime = "negative_acceleration_breakout"
                flip_part = 0.62
            elif wall_transition_score <= -0.10 or dealer_pressure < 0:
                flip_regime = "negative_acceleration_risk_off"
                flip_part = 0.30
        curve_fields["gamma_v3_flip_regime"] = flip_regime
        curve_fields["gamma_v3_flip_model_score"] = round(flip_part, 4)
        dealer_part = clamp(0.5 + 0.5 * dealer_pressure)
        wall_part = clamp(0.5 + wall_transition_score)
        gamma_score = clamp(0.38 * dealer_part + 0.27 * wall_part + 0.35 * flip_part)
        if flip_regime in {"negative_acceleration_risk_off", "near_flip_transition"}:
            gamma_score = min(gamma_score, 0.60)
        do_not_chase = net_ratio >= 0.25 and displacement > 0.08
        if do_not_chase:
            gamma_score = min(gamma_score, 0.58)
        entry_signal = (
            gamma_score >= 0.64
            and dealer_pressure >= -0.05
            and wall_transition_score >= -0.05
            and flip_regime not in {"negative_acceleration_risk_off", "near_flip_transition"}
            and not do_not_chase
        )
        management_signal = (
            "gamma_v2_entry_alpha" if entry_signal else
            "breakout_hold_ok" if flip_regime == "negative_acceleration_breakout" else
            "reduce_or_tighten_stop" if flip_regime == "negative_acceleration_risk_off" else
            "no_add_tighten_stop" if flip_regime == "near_flip_transition" else
            "do_not_chase_above_wall" if do_not_chase else
            "reduce_or_tighten_stop" if wall_transition_score <= -0.25 else
            "no_add_tighten_stop" if dealer_pressure <= -0.25 else
            "hold_context_only"
        )
        out[symbol] = {
            "gamma_v2_alpha_score": round(gamma_score * 100.0, 2),
            "gamma_v2_entry_signal": entry_signal,
            "gamma_v2_dealer_pressure_proxy": round(dealer_pressure, 4),
            "gamma_v2_wall_transition": wall_transition,
            "gamma_v2_wall_transition_score": round(wall_transition_score, 4),
            "gamma_v2_net_gamma_ratio": round(net_ratio, 4),
            "gamma_v2_displacement_pct": round(displacement, 4),
            "gamma_v2_center": round(center, 4),
            "gamma_v2_max_wall": round(max_wall, 4),
            "gamma_v2_management_signal": management_signal,
            "gamma_v2_effective_date": as_iso(latest_date),
            "gamma_v2_previous_effective_date": as_iso(previous_date),
            "gamma_v3_spot_source": spot_override.get("spot_source") or "options_chain_current_price",
            "gamma_v3_spot_price_date": spot_override.get("spot_price_date"),
            **curve_fields,
        }
    return out


def load_analysis_signals(
    con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date
) -> dict[str, dict[str, dict[str, Any]]]:
    """Per-symbol per-module analysis_daily snapshot for as_of.

    Returns {symbol: {module_name: {p_upside, p_downside, trend_prob, ...details}}}.
    Used by score_rows to fold broad-market momentum / breakout / mean-reversion
    signals into the AI-infra ranker's rank_score, so per-name conviction
    actually varies day-to-day with price action.
    """
    if not symbols or not table_exists(con, "analysis_daily"):
        return {}
    # US data lags 1 trading day (postmarket fetches prior close), so on
    # a fresh report-day analysis_daily often has nothing for as_of.
    # Fall back to the latest available date <= as_of for this symbol set.
    target_date = con.execute(
        f"""
        SELECT max(date) FROM analysis_daily
        WHERE date <= CAST(? AS DATE) AND symbol IN ({placeholders(symbols)})
        """,
        [as_of.isoformat(), *symbols],
    ).fetchone()[0]
    if target_date is None:
        return {}
    rows = con.execute(
        f"""
        SELECT symbol, module_name, p_upside, p_downside, trend_prob, details
        FROM analysis_daily
        WHERE date = CAST(? AS DATE) AND symbol IN ({placeholders(symbols)})
        """,
        [target_date, *symbols],
    ).fetchall()
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for sym, module, p_up, p_down, trend, details_json in rows:
        d = safe_json_loads(details_json) if details_json else {}
        d["analysis_signal_date"] = as_iso(target_date)
        if p_up is not None:
            d["p_upside"] = p_up
        if p_down is not None:
            d["p_downside"] = p_down
        if trend is not None:
            d["trend_prob"] = trend
        out.setdefault(normalize_symbol(sym), {})[str(module or "")] = d
    return out


# Regime-conditional internal weights for broad_signal sub-components.
# Right-side (momentum + breakout + overnight) vs left-side (mean_reversion)
# tilt matches REGIME_TILT_TABLE in generate_main_strategy_v2_report.py:
#   hedge        right=85 left=15  (default momentum-tilted)
#   wedge        right=70 left=30  (rates wedge — slight defense)
#   confirm      right=50 left=50  (downside confirmed — balanced)
#   press        right=30 left=70  (mean-reversion dominant)
#   capitulation right=15 left=85  (panic → left-side LEAPS window)
BROAD_REGIME_WEIGHTS: dict[str, dict[str, float]] = {
    "hedge":        {"momentum": 0.45, "breakout": 0.30, "mean_reversion": 0.15, "overnight": 0.10},
    "wedge":        {"momentum": 0.35, "breakout": 0.25, "mean_reversion": 0.30, "overnight": 0.10},
    "confirm":      {"momentum": 0.25, "breakout": 0.15, "mean_reversion": 0.50, "overnight": 0.10},
    "press":        {"momentum": 0.15, "breakout": 0.10, "mean_reversion": 0.70, "overnight": 0.05},
    "capitulation": {"momentum": 0.08, "breakout": 0.05, "mean_reversion": 0.85, "overnight": 0.02},
}


def broad_signal_score(
    modules: dict[str, dict[str, Any]],
    *,
    regime_state: str = "hedge",
) -> tuple[float, dict[str, float]]:
    """Combine analysis_daily modules into a 0-1 conviction proxy.

    Internal weights are now regime-conditional (BROAD_REGIME_WEIGHTS) so that
    in panic / capitulation tape the mean_reversion sub-score dominates, while
    in calm hedge tape the momentum_risk sub-score leads. When regime is
    unknown the table falls back to the hedge row.
    """
    state = str(regime_state or "hedge").lower()
    weights = BROAD_REGIME_WEIGHTS.get(state) or BROAD_REGIME_WEIGHTS["hedge"]
    breakdown: dict[str, float] = {"_regime_state": state}
    parts: list[tuple[float, float]] = []  # (weight, value)
    mom = modules.get("momentum_risk") or {}
    p_up = mom.get("p_upside")
    if isinstance(p_up, (int, float)):
        v = clamp(float(p_up))
        breakdown["momentum_p_upside"] = round(v * 100.0, 2)
        parts.append((weights["momentum"], v))
    br = modules.get("breakout") or {}
    br_score = br.get("breakout_score")
    if isinstance(br_score, (int, float)):
        v = clamp(float(br_score))
        breakdown["breakout"] = round(v * 100.0, 2)
        parts.append((weights["breakout"], v))
    mr = modules.get("mean_reversion") or {}
    mr_score = mr.get("reversion_score")
    if isinstance(mr_score, (int, float)):
        v = clamp(float(mr_score))
        direction = str(mr.get("reversion_direction") or "").lower()
        if direction.startswith("bullish"):
            breakdown["mean_reversion_bull"] = round(v * 100.0, 2)
            parts.append((weights["mean_reversion"], v))
        else:
            breakdown["mean_reversion_bear_headwind"] = round(v * 100.0, 2)
            parts.append((weights["mean_reversion"], 1.0 - v))   # bearish MR = headwind, invert
    oc = modules.get("overnight_continuation_alpha") or {}
    oc_pred = oc.get("p_continuation")
    if isinstance(oc_pred, (int, float)):
        v = clamp(float(oc_pred))
        breakdown["overnight_continuation"] = round(v * 100.0, 2)
        parts.append((weights["overnight"], v))
    if not parts:
        return 0.5, breakdown
    weight_sum = sum(w for w, _ in parts)
    return (sum(w * v for w, v in parts) / weight_sum), breakdown


def price_features(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "prices_daily"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        SELECT symbol, date, close, volume
        FROM prices_daily
        WHERE date <= CAST(? AS DATE)
          AND date >= CAST(? AS DATE)
          AND symbol IN ({placeholders(symbols)})
          AND close IS NOT NULL
        ORDER BY symbol, date
        """,
        [(as_of).isoformat(), (as_of - timedelta(days=45)).isoformat(), *symbols],
    )
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(normalize_symbol(row.get("symbol")), []).append(row)
    out: dict[str, dict[str, Any]] = {}
    for symbol, series in by_symbol.items():
        closes = [float(row["close"]) for row in series if row.get("close") is not None]
        if not closes:
            continue
        latest = closes[-1]
        price_as_of = as_iso(series[-1].get("date"))
        ret_5d = None if len(closes) < 6 or closes[-6] == 0 else (latest / closes[-6] - 1.0) * 100.0
        ret_20d = None if len(closes) < 21 or closes[-21] == 0 else (latest / closes[-21] - 1.0) * 100.0
        out[symbol] = {
            "price_as_of": price_as_of,
            "price_requested_date": as_of.isoformat(),
            "price_fallback_used": price_as_of != as_of.isoformat(),
            "close": round_or_none(latest, 4),
            "ret_5d_pct": round_or_none(ret_5d),
            "ret_20d_pct": round_or_none(ret_20d),
            "volume": round_or_none(series[-1].get("volume")),
        }
    return out


def recent_news(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date, config: NewsRiskConfig) -> dict[str, list[dict[str, Any]]]:
    if not symbols or not table_exists(con, "news_items"):
        return {}
    cols = table_columns(con, "news_items")
    symbol_col = "symbol" if "symbol" in cols else ""
    headline_col = "headline" if "headline" in cols else "title" if "title" in cols else ""
    published_col = "published_at" if "published_at" in cols else "date" if "date" in cols else ""
    summary_col = "summary" if "summary" in cols else ""
    source_col = "source" if "source" in cols else ""
    url_col = "url" if "url" in cols else ""
    if not symbol_col or not headline_col or not published_col:
        return {}
    summary_select = f"{summary_col} AS summary" if summary_col else "NULL AS summary"
    source_select = f"{source_col} AS source" if source_col else "NULL AS source"
    url_select = f"{url_col} AS url" if url_col else "NULL AS url"
    rows = rows_as_dicts(
        con,
        f"""
        SELECT {symbol_col} AS symbol, {headline_col} AS headline,
               {summary_select}, {source_select}, {url_select}, {published_col} AS published_at
        FROM news_items
        WHERE {published_col} >= CAST(? AS TIMESTAMP)
          AND {published_col} < CAST(? AS TIMESTAMP)
          AND {symbol_col} IN ({placeholders(symbols)})
        ORDER BY {published_col} DESC
        """,
        [
            (as_of - timedelta(days=config.lookback_days)).isoformat(),
            (as_of + timedelta(days=1)).isoformat(),
            *symbols,
        ],
    )
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(normalize_symbol(row.get("symbol")), []).append(row)
    # Enrich with news_scored if table exists (headline-agent output)
    if table_exists(con, "news_scored"):
        scored_rows = rows_as_dicts(
            con,
            f"""
            SELECT symbol, url, subject_match, sentiment, severity, event_type,
                   summary_zh
            FROM news_scored
            WHERE symbol IN ({placeholders(symbols)})
            """,
            list(symbols),
        )
        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for sr in scored_rows:
            by_key[(normalize_symbol(sr.get("symbol")), str(sr.get("url") or ""))] = {
                "subject_match": sr.get("subject_match"),
                "sentiment": sr.get("sentiment"),
                "severity": sr.get("severity"),
                "event_type": sr.get("event_type"),
                "summary_zh": sr.get("summary_zh"),
            }
        for sym, items in out.items():
            for item in items:
                key = (sym, str(item.get("url") or ""))
                if key in by_key:
                    item["scored"] = by_key[key]
    return out


def headline_risk_from_scored(items: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    """Read headline-agent (DeepSeek) scored items if available.

    Items here are news_items rows ENRICHED with a 'scored' subfield holding
    {subject_match, sentiment, severity, event_type, summary_zh, confidence}
    from the news_scored table. Falls back to keyword matching (caller) when
    no scored data is present for any item.

    Risk derivation (severity 0-3 int bucket → 0-1 risk float):
      - severity bucket: 0=irrelevant, 1=routine, 2=notable, 3=severe.
      - Map: 0→0.0, 1→0.0(noise), 2→0.6, 3→0.9.
      - subject_match=False items contribute zero risk.
      - sentiment='positive' contributes zero risk.
      - sentiment='neutral' × 0.5; sentiment='negative' × full.
    """
    have_any_scored = any(item.get("scored") for item in items)
    if not have_any_scored:
        return None
    SEV_BUCKET = {0: 0.0, 1: 0.0, 2: 0.6, 3: 0.9}
    risk = 0.0
    flags: list[str] = []
    latest_headline = ""
    latest_date = None
    sorted_items = sorted(
        items, key=lambda i: as_iso(i.get("published_at")) or "", reverse=True
    )
    for item in sorted_items:
        s = item.get("scored") or {}
        if not latest_headline and s.get("subject_match"):
            latest_headline = str(item.get("headline") or "")
            latest_date = as_iso(item.get("published_at"))
        if not s.get("subject_match"):
            continue
        sentiment = str(s.get("sentiment") or "neutral").lower()
        raw_sev = s.get("severity")
        # Accept both int (new schema) and float (legacy rows) just in case
        if isinstance(raw_sev, (int, float)):
            sev_int = int(raw_sev) if raw_sev > 1.0 else int(round(float(raw_sev) * 3))
            sev_int = max(0, min(3, sev_int))
        else:
            sev_int = 0
        if sentiment == "positive":
            continue
        sev_value = SEV_BUCKET.get(sev_int, 0.0)
        contribution = sev_value if sentiment == "negative" else sev_value * 0.5
        if contribution > risk:
            risk = contribution
        if sev_int >= 2:
            ev_type = str(s.get("event_type") or "other")
            flags.append(f"{sentiment}:{ev_type}")
    return {
        "headline_risk": round(risk, 4),
        "headline_flags": sorted(set(flags)),
        "latest_headline": latest_headline,
        "latest_headline_date": latest_date,
        "headline_source": "news_scored",
    }


def headline_risk(items: list[dict[str, Any]], config: NewsRiskConfig, symbol: str = "") -> dict[str, Any]:
    """Compute symbol-level headline risk with two precision improvements:

    1. Subject filter — severe/negative term must appear in the HEADLINE itself,
       AND the symbol must be in the headline (not just the summary or article
       body). This eliminates false positives from multi-ticker "tag list" articles
       (e.g. "ZoomInfo, GoDaddy... Shares Plummet" tagging MU because of sector
       grouping) and metaphor articles ("Burry's NVIDIA-Era Bloody Car Crash
       Warning" mentioning MU in body).

    2. Recency decay — if the most recent symbol-subject headline has NO negative
       terms and is newer than the offending severe hit, decay the risk score.
       Stale "bankruptcy" tags shouldn't outweigh fresh "UBS upgrade" news.
    """
    severe_risk = 0.0
    negative_risk = 0.0
    flags: list[str] = []
    latest_headline = ""
    latest_date = None
    severe_date: str | None = None
    sorted_items = sorted(
        items, key=lambda i: as_iso(i.get("published_at")) or "", reverse=True
    )
    for item in sorted_items:
        headline = str(item.get("headline") or "")
        summary = str(item.get("summary") or "")
        headline_l = headline.lower()
        full_text = f"{headline} {summary}".lower()
        # Filter 1: symbol must appear somewhere in headline/summary at all
        if symbol and not symbol_alias_in_text(symbol, full_text):
            continue
        # Track the most recent symbol-subject headline (symbol in headline)
        is_subject = (not symbol) or symbol_alias_in_text(symbol, headline_l)
        if is_subject and not latest_headline:
            latest_headline = headline
            latest_date = as_iso(item.get("published_at"))
        # Filter 2: only count severe/negative against symbol if it is the
        # subject (symbol in headline) AND the negative term is in the headline.
        # This rejects multi-ticker tag list articles and metaphor/body mentions.
        if not is_subject:
            continue
        severe_in_headline = [
            term for term in config.severe_terms if term_in_text(term, headline_l)
        ]
        negative_in_headline = [
            term for term in config.negative_terms if term_in_text(term, headline_l)
        ]
        item_date = as_iso(item.get("published_at"))
        if severe_in_headline:
            severe_risk = max(severe_risk, 0.82)
            flags.extend(severe_in_headline)
            if severe_date is None:
                severe_date = item_date
        elif negative_in_headline:
            negative_risk = max(negative_risk, 0.48)
            flags.extend(negative_in_headline)
    risk = max(severe_risk, negative_risk)
    # Recency decay: if latest symbol-subject headline is clean AND newer
    # than the severe hit, dampen the risk. Cap decay so risk stays ≥ 0.25 if
    # a severe hit was ever recorded (don't fully clear, just downgrade).
    if risk >= 0.6 and latest_headline and severe_date and latest_date:
        latest_l = latest_headline.lower()
        latest_clean = not (
            any(term_in_text(t, latest_l) for t in config.severe_terms)
            or any(term_in_text(t, latest_l) for t in config.negative_terms)
        )
        if latest_clean and latest_date > severe_date:
            risk = max(0.25, risk - 0.4)
    return {
        "headline_risk": round(risk, 4),
        "headline_flags": sorted(set(flags)),
        "latest_headline": latest_headline,
        "latest_headline_date": latest_date,
    }


def ai_news_evidence(items: list[dict[str, Any]], layer: str, symbol: str) -> dict[str, Any]:
    if not items:
        return {
            "ai_evidence_score": 0.0,
            "supplier_evidence_state": "missing_recent_news",
        }
    layer_terms = {term for term in str(layer or "").replace("_", " ").split() if len(term) >= 3}
    terms = AI_EVIDENCE_TERMS | layer_terms
    aliases = {symbol.lower()} | US_COMPANY_ALIASES.get(symbol.upper(), set())
    best: dict[str, Any] | None = None
    best_score = 0.0
    best_hits: list[str] = []
    negative_best: dict[str, Any] | None = None
    negative_score = 0.0
    negative_best_hits: list[str] = []
    for item in items:
        text = f"{item.get('headline') or ''} {item.get('summary') or ''}".lower()
        if not any(alias and term_in_text(alias, text) for alias in aliases):
            continue
        ai_hits = sorted(term for term in terms if term_in_text(term, text))
        supply_hits = sorted(term for term in SUPPLY_LINK_TERMS if term_in_text(term, text))
        negative_hits = sorted(term for term in NEGATIVE_SUPPLY_TERMS if term_in_text(term, text))
        if negative_hits and supply_hits:
            score = min(0.18, 0.08 + 0.02 * len(ai_hits) + 0.03 * len(supply_hits))
            if negative_best is None or score > negative_score:
                negative_best = item
                negative_score = score
                negative_best_hits = ai_hits[:6] + [f"supply:{term}" for term in supply_hits[:4]]
                negative_best_hits += [f"negative:{term}" for term in negative_hits[:6]]
            continue
        if not ai_hits and not supply_hits:
            continue
        score = min(1.0, 0.45 + 0.08 * len(ai_hits) + 0.14 * len(supply_hits))
        if score > best_score:
            best = item
            best_score = score
            best_hits = ai_hits[:6] + [f"supply:{term}" for term in supply_hits[:4]]
    if negative_best:
        best = negative_best
        best_score = negative_score
        best_hits = negative_best_hits
    if not best:
        return {
            "ai_evidence_score": 0.0,
            "supplier_evidence_state": "needs_primary_confirmation",
        }
    if any(hit.startswith("negative:") for hit in best_hits):
        state = "negative_supply_evidence"
    elif any(hit.startswith("supply:") for hit in best_hits):
        state = "source_linked_supply_evidence"
    else:
        state = "theme_news_only"
    headline = str(best.get("headline") or "")
    summary = str(best.get("summary") or "")
    headline_hit_count = sum(1 for hit in best_hits if hit.replace("supply:", "") in headline.lower())
    summary_hit_count = sum(1 for hit in best_hits if hit.replace("supply:", "") in summary.lower())
    display_text = summary if summary and summary_hit_count > headline_hit_count else headline
    return {
        "ai_evidence_score": round(best_score, 4),
        "supplier_evidence_state": state,
        "ai_evidence_headline": best.get("headline"),
        "ai_evidence_text": display_text[:240],
        "ai_evidence_source": best.get("source"),
        "ai_evidence_url": best.get("url"),
        "ai_evidence_date": as_iso(best.get("published_at")),
        "ai_evidence_hits": best_hits,
    }


def option_quality(row: dict[str, Any] | None) -> tuple[float, str]:
    if not row:
        return 50.0, "missing_options_alpha"
    expression = str(row.get("expression") or "").lower()
    liquidity = str(row.get("liquidity_gate") or "").lower()
    directional = round_or_none(row.get("directional_edge")) or 0.0
    vol_edge = round_or_none(row.get("vol_edge")) or 0.0
    flow_edge = round_or_none(row.get("flow_edge")) or 0.0
    liquidity_score = 1.0 if liquidity == "pass" else 0.35
    expression_score = 1.0 if expression in {"call_spread", "stock_long"} else 0.45
    edge_score = clamp((directional + max(vol_edge, 0.0) + max(flow_edge, 0.0)) / 1.5)
    score = (0.32 * liquidity_score + 0.30 * expression_score + 0.38 * edge_score) * 100.0
    reason = f"{expression or 'missing'}; liquidity={liquidity or '-'}; dir={directional:.2f}; vol={vol_edge:.2f}; flow={flow_edge:.2f}"
    return round(score, 2), reason


def setup_quality(row: dict[str, Any]) -> float:
    rr = round_or_none(row.get("rr_ratio"))
    expected = round_or_none(row.get("expected_move_pct"))
    confidence = str(row.get("signal_confidence") or "").upper()
    rr_score = 0.5 if rr is None else clamp(rr / 3.0)
    expected_score = 0.5 if expected is None else clamp(expected / 8.0)
    confidence_score = {"LOW": 0.75, "MODERATE": 0.55, "HIGH": 0.45}.get(confidence, 0.50)
    return 0.52 * rr_score + 0.28 * expected_score + 0.20 * confidence_score


def price_quality(row: dict[str, Any]) -> float:
    ret_5d = round_or_none(row.get("ret_5d_pct"))
    ret_20d = round_or_none(row.get("ret_20d_pct"))
    if ret_5d is None and ret_20d is None:
        return 0.5
    ret5_score = 0.5 if ret_5d is None else clamp((ret_5d + 4.0) / 12.0)
    ret20_score = 0.5 if ret_20d is None else clamp((ret_20d + 8.0) / 24.0)
    extension_penalty = 0.15 if (ret_5d is not None and ret_5d > 10.0) or (ret_20d is not None and ret_20d > 25.0) else 0.0
    return clamp(0.55 * ret5_score + 0.45 * ret20_score - extension_penalty)


def supercycle_priority_score(row: dict[str, Any]) -> float:
    layer = str(row.get("supercycle_layer") or "").strip()
    priority_raw = row.get("supercycle_priority")
    try:
        priority = int(priority_raw)
    except (TypeError, ValueError):
        priority = 9 if not layer else 3
    if priority <= 1:
        return 1.0
    if priority == 2:
        return 0.85
    if priority == 3:
        return 0.65
    return 0.25 if not layer else 0.45


def risk_penalty(row: dict[str, Any]) -> float:
    blockers = [str(item).lower() for item in (row.get("blockers") or []) if item]
    rr = round_or_none(row.get("rr_ratio"))
    penalty = 0.0
    if rr is not None and rr < 1.5:
        penalty += 0.25
    if row.get("stop") is None:
        penalty += 0.18
    for marker in ("stale", "already_paid", "exhaustion", "rr_below"):
        if any(marker in item for item in blockers):
            penalty += 0.16
    return clamp(penalty)


def enrich_rows(
    candidates: list[dict[str, Any]],
    options: dict[str, dict[str, Any]],
    gamma_alpha: dict[str, dict[str, Any]],
    prices: dict[str, dict[str, Any]],
    news: dict[str, list[dict[str, Any]]],
    config: RankerConfig,
    signals: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    signals = signals or {}
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = normalize_symbol(candidate.get("symbol"))
        if not symbol:
            continue
        option_row = options.get(symbol)
        option_score, option_reason = option_quality(option_row)
        gamma_row = gamma_alpha.get(symbol) or {}
        alpha_sleeve_id = candidate.get("alpha_sleeve_id")
        alpha_factory_role = candidate.get("alpha_factory_role") or ("execution_sleeve" if alpha_sleeve_id else "rank_only")
        prior_alpha_sleeve_id = alpha_sleeve_id
        if gamma_row.get("gamma_v2_entry_signal"):
            alpha_sleeve_id = US_GAMMA_V2_ALPHA_SLEEVE
            alpha_factory_role = "gamma_v2_entry_alpha"
        symbol_signals = signals.get(symbol, {})
        signal_dates = sorted(
            {
                str(module.get("analysis_signal_date"))
                for module in symbol_signals.values()
                if isinstance(module, dict) and module.get("analysis_signal_date")
            }
        )
        combined = {
            **(prices.get(symbol) or {}),
            **dict(candidate),
            "symbol": symbol,
            **gamma_row,
            "prior_alpha_sleeve_id": prior_alpha_sleeve_id,
            "alpha_sleeve_id": alpha_sleeve_id,
            "alpha_factory_role": alpha_factory_role,
            "options_quality": option_score,
            "flow_options_quality": option_score,
            "options_alpha_as_of": as_iso((option_row or {}).get("as_of")),
            "options_quality_reason": option_reason,
            "option_expression": (option_row or {}).get("expression") or candidate.get("option_expression"),
            # broad_modules carries today's momentum_risk / breakout /
            # mean_reversion details for this symbol. score_rows folds it
            # into rank_score so per-name conviction varies with price.
            "analysis_signal_date": signal_dates[-1] if signal_dates else None,
            "broad_modules": symbol_signals if symbol_signals else None,
        }
        sym_news = news.get(symbol) or []
        scored_risk = headline_risk_from_scored(sym_news, symbol)
        combined.update(scored_risk if scored_risk is not None else headline_risk(sym_news, config.headline, symbol))
        combined.update(ai_news_evidence(news.get(symbol) or [], str(combined.get("supercycle_layer") or ""), symbol))
        rows.append(combined)
    return rows


def _evidence_state_passes_gate(row: dict[str, Any]) -> bool:
    """Block tape-only promotion of `待原文核验` AI infra names.

    Methodology requires `原文已证明` or `合理推论` before a universe row
    can leave watch / research-only. Without that, even Alpha Factory
    sleeve membership keeps the row in ranked_watch.
    """
    state = str(row.get("ai_infra_evidence_state") or row.get("evidence_state") or "")
    if "原文已证明" in state or "合理推论" in state:
        return True
    if not row.get("ai_infra_universe") and not row.get("ai_infra_current_pool"):
        return True
    return False


def production_tier(rank: int, row: dict[str, Any], config: RankerConfig) -> tuple[str, str, str]:
    headline = round_or_none(row.get("headline_risk")) or 0.0
    if not _evidence_state_passes_gate(row):
        return (
            "ranked_watch",
            "evidence_state_pending_no_trade",
            "0R: ai_infra_evidence_state is 待原文核验 / empty; tape alone cannot promote pending-evidence names",
        )
    if headline >= config.event_risk_zero_r:
        return "event_risk_watch", "negative_headline_no_probe", "0R until event/news risk clears"
    if row.get("supplier_evidence_state") == "negative_supply_evidence":
        return "event_risk_watch", "negative_supply_no_trade", "0R until supply/order risk is resolved"
    if row.get("alpha_sleeve_id") not in US_ALPHA_FACTORY_EXECUTION_SLEEVES:
        return "ranked_watch", "rank_only_no_new_trade", "0R until Alpha Factory sleeve promotion"
    options_q = round_or_none(row.get("options_quality")) or 0.0
    if row.get("alpha_sleeve_id") == US_GAMMA_V2_ALPHA_SLEEVE:
        action = "buy_stock_with_gamma_v2_entry"
    else:
        action = "buy_stock_with_options_confirmation" if options_q >= 65.0 else "buy_stock_position"
    if rank <= config.top_probe_count:
        return "top_stock_trade", action, "0.50R/name; basket cap set by portfolio overlay"
    if rank <= config.secondary_probe_count:
        return "secondary_stock_trade", action, "0.25R/name after pullback/retest confirmation"
    return "active_watch", "prepare_order_but_wait_for_price", "0R default unless price confirms"


def score_rows(
    rows: list[dict[str, Any]],
    config: RankerConfig = DEFAULT_CONFIG,
    *,
    regime_state: str = "hedge",
) -> list[dict[str, Any]]:
    for row in rows:
        alpha_score = 1.0 if row.get("alpha_sleeve_id") in US_ALPHA_FACTORY_EXECUTION_SLEEVES else 0.15
        gamma_alpha = clamp((round_or_none(row.get("gamma_v2_alpha_score")) or 30.0) / 100.0)
        setup = setup_quality(row)
        options_q = clamp((round_or_none(row.get("flow_options_quality")) or 50.0) / 100.0)
        price = price_quality(row)
        supercycle = supercycle_priority_score(row)
        ai_evidence = clamp(round_or_none(row.get("ai_evidence_score")) or 0.0)
        penalty = risk_penalty(row)
        headline = clamp(round_or_none(row.get("headline_risk")) or 0.0)
        joint_signal = clamp(0.38 * price + 0.34 * options_q + 0.28 * setup)
        raw = (
            config.score_weights.get("alpha_factory", 0.0) * alpha_score
            + config.score_weights.get("gamma_v2_alpha", 0.0) * gamma_alpha
            + config.score_weights.get("setup_quality", 0.0) * setup
            + config.score_weights.get("flow_options_quality", 0.0) * options_q
            + config.score_weights.get("price_quality", 0.0) * price
            + config.score_weights.get("supercycle_priority", 0.0) * supercycle
            + config.score_weights.get("ai_evidence", 0.0) * ai_evidence
            + config.score_weights.get("risk_penalty", 0.0) * penalty
            + config.score_weights.get("headline_risk", 0.0) * headline
        )
        row["joint_signal_score"] = round(joint_signal * 100.0, 2)
        row["score_components"] = {
            "alpha_factory": round(alpha_score * 100.0, 2),
            "gamma_v2_alpha": round(gamma_alpha * 100.0, 2),
            "setup_quality": round(setup * 100.0, 2),
            "flow_options_quality": round(options_q * 100.0, 2),
            "price_quality": round(price * 100.0, 2),
            "supercycle_priority": round(supercycle * 100.0, 2),
            "ai_evidence": round(ai_evidence * 100.0, 2),
            "joint_price_options_news": round(joint_signal * 100.0, 2),
            "risk_penalty": round(penalty * 100.0, 2),
            "headline_risk": round(headline * 100.0, 2),
        }
        # Broad-market signal fold-in. analysis_daily already computes
        # momentum_risk / breakout / mean_reversion daily for every name
        # in the broad universe (incl. our AI-infra basket); read it,
        # combine into a 0-1 conviction proxy, and rebalance raw so 20%
        # of rank_score tracks today's price action. Backward-compat:
        # when broad_modules is absent (e.g. old tests), behavior unchanged.
        broad_modules = row.get("broad_modules")
        if broad_modules is not None:
            broad, breakdown = broad_signal_score(broad_modules, regime_state=regime_state)
            row["score_components"]["broad_signal"] = round(broad * 100.0, 2)
            row["score_components"]["broad_signal_breakdown"] = breakdown
            broad_weight = config.score_weights.get("broad_signal", 0.20)
            raw = raw * (1.0 - broad_weight) + broad_weight * broad
        row["rank_score"] = round(clamp(raw) * 100.0, 2)
    rows.sort(key=lambda row: (-(round_or_none(row.get("rank_score")) or 0.0), str(row.get("symbol") or "")))
    for idx, row in enumerate(rows, start=1):
        tier, action, size = production_tier(idx, row, config)
        row["rank"] = idx
        row["production_tier"] = tier
        row["production_action"] = action
        row["size_hint"] = size
    return rows


def public_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "rank",
        "rank_score",
        "production_tier",
        "production_action",
        "size_hint",
        "symbol",
        "state",
        "policy",
        "alpha_sleeve_id",
        "prior_alpha_sleeve_id",
        "alpha_factory_role",
        "entry",
        "stop",
        "target",
        "rr_ratio",
        "expected_move_pct",
        "option_expression",
        "options_alpha_as_of",
        "options_quality",
        "flow_options_quality",
        "options_quality_reason",
        "joint_signal_score",
        "gamma_v2_alpha_score",
        "gamma_v2_entry_signal",
        "gamma_v2_dealer_pressure_proxy",
        "gamma_v2_wall_transition",
        "gamma_v2_wall_transition_score",
        "gamma_v2_net_gamma_ratio",
        "gamma_v2_displacement_pct",
        "gamma_v2_center",
        "gamma_v2_max_wall",
        "gamma_v2_management_signal",
        "gamma_v2_effective_date",
        "gamma_v2_previous_effective_date",
        "gamma_v3_curve_state",
        "gamma_v3_zero_gamma_levels",
        "gamma_v3_zero_gamma_band",
        "gamma_v3_positive_gex_pin_zone",
        "gamma_v3_negative_gex_accel_zone",
        "gamma_v3_gex_flip_distance_pct",
        "gamma_v3_curve_quality",
        "gamma_v3_current_net_gex_1pct",
        "gamma_v3_current_net_gamma_ratio",
        "gamma_v3_flip_regime",
        "gamma_v3_flip_model_score",
        "gamma_v3_spot_source",
        "gamma_v3_spot_price_date",
        "trend_regime",
        "signal_confidence",
        "execution_mode",
        "theme_id",
        "theme_label",
        "theme_score",
        "member_rank",
        "ai_infra_universe",
        "ai_infra_asset_pool",
        "ai_infra_market_country",
        "ai_infra_bfs_depth",
        "ai_infra_module",
        "ai_infra_current_pool",
        "ai_infra_total_score",
        "ai_infra_score_bucket",
        "ai_infra_evidence_state",
        "ai_infra_counterevidence",
        "ai_infra_dependency_path",
        "ai_infra_dependency_edge",
        "ai_infra_verification_status",
        "supercycle_layer",
        "supercycle_priority",
        "supply_chain_role",
        "bottleneck_focus",
        "evidence_contract",
        "research_index",
        "ai_evidence_score",
        "supplier_evidence_state",
        "ai_evidence_headline",
        "ai_evidence_text",
        "ai_evidence_source",
        "ai_evidence_url",
        "ai_evidence_date",
        "ai_evidence_hits",
        "headline_risk",
        "headline_flags",
        "latest_headline_date",
        "latest_headline",
        "price_as_of",
        "price_requested_date",
        "price_fallback_used",
        "analysis_signal_date",
        "close",
        "ret_5d_pct",
        "ret_20d_pct",
        "volume",
        "blockers",
        "reason",
        "score_components",
    ]
    out = {key: row.get(key) for key in keys if key in row}
    for key, value in list(out.items()):
        if hasattr(value, "isoformat"):
            out[key] = value.isoformat()
        elif isinstance(value, float):
            out[key] = round_or_none(value)
    return out


def render_markdown(payload: dict[str, Any]) -> str:
    rows = payload.get("top_rows") or []
    lines = [
        f"# US Opportunity Ranker - {payload['as_of']}",
        "",
        "Production contract: `us_theme_cluster_momentum` and `us_v2_stock_probe` can emit stock-trade Execution Alpha. Price, news, and options/flow are scored together; options are decision evidence, not the traded instrument.",
        "",
        "| Rank | Symbol | Sleeve | Layer | Evidence | Tier | Action | Score | Joint | Headline | Options/Flow | R:R | Trend | Why |",
        "|---:|---|---|---|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        why = str(row.get("reason") or row.get("options_quality_reason") or "").replace("|", "/")
        if len(why) > 70:
            why = why[:67] + "..."
        lines.append(
            "| {rank} | {symbol} | {sleeve} | {layer} | {evidence} | {tier} | {action} | {score} | {joint} | {headline} | {options} | {rr} | {trend} | {why} |".format(
                rank=row.get("rank"),
                symbol=row.get("symbol") or "",
                sleeve=row.get("alpha_sleeve_id") or "rank_only",
                layer=row.get("supercycle_layer") or "-",
                evidence=row.get("supplier_evidence_state") or "-",
                tier=row.get("production_tier") or "",
                action=row.get("production_action") or "",
                score=fmt_num(row.get("rank_score")),
                joint=fmt_num(row.get("joint_signal_score"), 0),
                headline=fmt_num((round_or_none(row.get("headline_risk")) or 0.0) * 100.0, 0),
                options=fmt_num(row.get("flow_options_quality"), 0),
                rr=fmt_num(row.get("rr_ratio"), 2),
                trend=row.get("trend_regime") or "-",
                why=why,
            )
        )
    lines += [
        "",
        "## Operating Rule",
        "",
        "- Top sleeve rows become stock trades; options/flow quality only changes ranking and confirmation.",
        "- AI supercycle layer/priority is a ranking input; it does not by itself prove a supplier relationship.",
        "- News is used jointly with price and options/flow because US events can reprice immediately; options remain auxiliary evidence.",
        "- Strong theme-basket sleeve rows can become stock trades; legacy single-name rows remain ranked watch with 0R default size.",
        "- Event/news risk forces 0R even when the sleeve is otherwise valid.",
    ]
    return "\n".join(lines) + "\n"


def write_duckdb(path: Path, rows: list[dict[str, Any]], as_of: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS us_opportunity_ranker")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS us_opportunity_ranker (
                as_of DATE, rank INTEGER, symbol VARCHAR, rank_score DOUBLE,
                alpha_sleeve_id VARCHAR, alpha_factory_role VARCHAR,
                production_tier VARCHAR, production_action VARCHAR, size_hint VARCHAR,
                headline_risk DOUBLE, options_quality DOUBLE,
                rr_ratio DOUBLE, trend_regime VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute("DELETE FROM us_opportunity_ranker WHERE as_of = CAST(? AS DATE)", [as_of.isoformat()])
        records = [
            [
                as_of.isoformat(),
                row.get("rank"),
                row.get("symbol"),
                row.get("rank_score"),
                row.get("alpha_sleeve_id"),
                row.get("alpha_factory_role"),
                row.get("production_tier"),
                row.get("production_action"),
                row.get("size_hint"),
                row.get("headline_risk"),
                row.get("options_quality"),
                row.get("rr_ratio"),
                row.get("trend_regime"),
                json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
            ]
            for row in rows
        ]
        if records:
            con.executemany(
                """
                INSERT INTO us_opportunity_ranker VALUES (
                    CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                records,
            )
    finally:
        con.close()


def build_ranker_payload(
    *,
    as_of: date,
    candidates: list[dict[str, Any]],
    candidate_status: str,
    us_db: Path,
    source_report: str | None = None,
    top: int = 30,
    config: RankerConfig = DEFAULT_CONFIG,
    ai_infra_root: Path | None = None,
    ai_infra_mode: str = "off",
    regime_state: str = "hedge",
) -> dict[str, Any]:
    input_candidate_count = len(candidates)
    ai_infra_gate = None
    if ai_infra_mode != "off":
        include_all = ai_infra_mode in {"expand", "enforce_expand"}
        # Enforce modes execute the production basket → only evidence-confirmed
        # names (原文已证明 / 合理推论) qualify. Pure expand mode keeps the
        # research universe so the radar still surfaces 待原文核验 ideas for
        # operator review (they cannot be promoted by the secondary evidence
        # gate downstream).
        pool = "production" if ai_infra_mode in {"enforce", "enforce_expand"} else "research"
        candidates, gate = ai_infra_universe.merge_with_universe_candidates(
            candidates,
            market="US",
            ai_infra_root=ai_infra_root,
            include_all_universe=include_all,
            pool=pool,
        )
        ai_infra_gate = gate.as_dict()
        candidate_status = f"{candidate_status}+ai_infra_{ai_infra_mode}_{pool}"

    symbols = sorted({normalize_symbol(row.get("symbol")) for row in candidates if normalize_symbol(row.get("symbol"))})
    options: dict[str, dict[str, Any]] = {}
    gamma_alpha: dict[str, dict[str, Any]] = {}
    prices: dict[str, dict[str, Any]] = {}
    news: dict[str, list[dict[str, Any]]] = {}
    signals: dict[str, dict[str, dict[str, Any]]] = {}
    if us_db.exists() and symbols:
        con = duckdb.connect(str(us_db), read_only=True)
        try:
            options = latest_options(con, symbols, as_of)
            gamma_alpha = latest_gamma_v2_alpha(con, symbols, as_of)
            prices = price_features(con, symbols, as_of)
            news = recent_news(con, symbols, as_of, config.headline)
            signals = load_analysis_signals(con, symbols, as_of)
        finally:
            con.close()
    ranked = score_rows(
        enrich_rows(candidates, options, gamma_alpha, prices, news, config, signals),
        config,
        regime_state=regime_state,
    )
    public_rows = [public_row(row) for row in ranked]
    top_n = max(1, int(top or 30))
    return {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "production_opportunity_ranker",
        "candidate_status": candidate_status,
        "input_candidate_count": input_candidate_count,
        "candidate_count": len(candidates),
        "ranked_count": len(public_rows),
        "source_report": source_report,
        "us_db": str(us_db),
        "ai_infra_mode": ai_infra_mode,
        "ai_infra_root": str(ai_infra_root or ai_infra_universe.DEFAULT_AI_INFRA_ROOT),
        "ai_infra_gate": ai_infra_gate,
        "score_config": asdict(config),
        "score_weights": config.score_weights,
        "notes": [
            "Candidate universe is the ai_infra BFS workbench when ai_infra_mode is active.",
            "Alpha Factory sleeve membership is the execution contract.",
            "Gamma Spring v3 GEX curve/state machine runs under the `us_gamma_v2_alpha` compatibility sleeve inside the admitted AI universe.",
            "Options/flow quality controls expression choice, not legacy promotion.",
            "Headline/event risk forces 0R watch.",
        ],
        "production_basket": [row for row in public_rows if row.get("rank", 999) <= 10],
        "top_rows": public_rows[:top_n],
        "all_rows": public_rows,
    }


def write_ranker_outputs(payload: dict[str, Any], output_root: Path) -> Path:
    as_of = datetime.strptime(str(payload["as_of"]), "%Y-%m-%d").date()
    output_dir = output_root / as_of.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "us_opportunity_ranker.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "us_opportunity_ranker.md").write_text(render_markdown(payload), encoding="utf-8")
    write_duckdb(output_dir / "us_opportunity_ranker.duckdb", payload.get("all_rows") or [], as_of)
    return output_dir
