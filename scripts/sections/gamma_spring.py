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
from dataclasses import dataclass
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


def _weighted_mean(items: list[tuple[float, float]]) -> float | None:
    cleaned = [(value, max(weight, 0.0)) for value, weight in items]
    denom = sum(weight for _value, weight in cleaned)
    if denom <= 0:
        return None
    return sum(value * weight for value, weight in cleaned) / denom


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


def _build_row(
    symbol: str,
    raw_rows: list[tuple[Any, ...]],
    *,
    previous_raw_rows: list[tuple[Any, ...]] | None = None,
    previous_row: GammaSpringRow | None = None,
) -> GammaSpringRow | None:
    spots = [_finite_float(r[1]) for r in raw_rows]
    spots = [s for s in spots if s is not None and s > 0]
    if not spots:
        return None
    spot = median(spots)

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

    for contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility in raw_rows:
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
                       open_interest, volume, implied_volatility, symbol
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
                           open_interest, volume, implied_volatility, symbol
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
    finally:
        con.close()

    grouped: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    previous_grouped: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility, symbol in rows:
        grouped[str(symbol).upper()].append(
            (contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility)
        )
    for contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility, symbol in previous_rows:
        previous_grouped[str(symbol).upper()].append(
            (contract_symbol, current_price, option_type, strike, gamma, open_interest, volume, implied_volatility)
        )

    previous_summary = {
        sym: row
        for sym, raw in previous_grouped.items()
        if (row := _build_row(sym, raw)) is not None
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
        "contract": "0R stock management context only; dealer pressure is proxied from OI/volume/skew, not observed",
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
        "## US Gamma Spring v2 / 势能井买卖管理",
        "",
        "- 方法: calls 记正 gamma, puts 记负 gamma; GEX 按每 1% 标的移动估算。v2 增加 OI change / volume / put-call skew 的 dealer pressure proxy,以及 center / max wall transition。",
        "- 合同: 这是股票买卖管理和仓位上限信号,不是期权交易指令; dealer 仓位不可直接观测,proxy 只能用于风险解释和执行节奏。",
    ]
    if not rows:
        lines += ["- 今日无可用 gamma chain 数据。", ""]
        return lines
    eff = snap.get("effective_date") or "-"
    prev_eff = snap.get("previous_effective_date") or "-"
    lines.append(f"- 数据日 {eff}; 前一链日 {prev_eff}; Gamma v2 已接入 `us_gamma_v2_alpha`,可在 AI universe 内生成入场候选,并同步调管理动作/仓位上限。")
    lines.append("")
    lines += [
        "| Symbol | State | Spot | Center | Max wall | Net γ | Dealer px | Wall transition | v2 mult | Mgmt |",
        "|---|---|---:|---:|---:|---:|---:|---|---:|---|",
    ]
    for row in rows[:limit]:
        lines.append(
            "| {sym} | {state} | {spot:.2f} | {center:.2f} | {wall:.2f} | {net} | {dealer} | {transition} | {mult:.2f}x | {mgmt} |".format(
                sym=row.get("symbol"),
                state=row.get("state") or "-",
                spot=float(row.get("spot") or 0.0),
                center=float(row.get("center_strike") or 0.0),
                wall=float(row.get("max_wall_strike") or 0.0),
                net=_fmt_pct(row.get("net_gamma_ratio"), 0),
                dealer=_fmt_pct(row.get("dealer_pressure_proxy"), 0),
                transition=row.get("wall_transition") or "-",
                mult=float(row.get("gamma_v2_multiplier") or 1.0),
                mgmt=_management_text(str(row.get("management_signal") or "hold_context_only")),
            )
        )
    lines.append("")
    lines.append("Legend: Dealer px 为 OI/volume/skew proxy; v2 mult <1 表示管理层面降 exposure/不追高/收紧止损。")
    lines.append("")
    return lines
