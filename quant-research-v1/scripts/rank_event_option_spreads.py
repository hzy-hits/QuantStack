#!/usr/bin/env python3
"""Rank event-driven call debit spreads from stored option leg quotes."""
from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


DEFAULT_DB = Path(__file__).resolve().parents[1] / "data" / "quant.duckdb"


def _previous_weekday(day: date) -> date:
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def default_as_of(now: datetime | None = None) -> date:
    et_now = now.astimezone(ZoneInfo("America/New_York")) if now else datetime.now(ZoneInfo("America/New_York"))
    if et_now.weekday() >= 5:
        return _previous_weekday(et_now.date())
    if et_now.hour < 16 or (et_now.hour == 16 and et_now.minute < 15):
        return _previous_weekday(et_now.date())
    return et_now.date()


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def rn_prob_above(spot: float, strike: float, iv: float | None, days_to_exp: int | None) -> float | None:
    if spot <= 0 or strike <= 0 or not iv or iv <= 0 or not days_to_exp or days_to_exp <= 0:
        return None
    t = days_to_exp / 365.0
    vol_t = iv * math.sqrt(t)
    if vol_t <= 0:
        return None
    d2_like = (math.log(spot / strike) - 0.5 * iv * iv * t) / vol_t
    return norm_cdf(d2_like)


def round_tick(value: float, tick: float = 0.05) -> float:
    return round(round(value / tick) * tick, 2)


@dataclass(frozen=True)
class OptionLeg:
    strike: float
    bid: float
    ask: float
    mid: float
    volume: int
    open_interest: int
    implied_volatility: float | None
    delta: float | None
    theta: float | None
    days_to_exp: int | None

    @property
    def spread_pct(self) -> float:
        if self.mid <= 0:
            return 1.0
        return max(0.0, (self.ask - self.bid) / self.mid)


@dataclass(frozen=True)
class SpreadCandidate:
    symbol: str
    as_of: date
    expiry: str
    current_price: float
    long_strike: float
    short_strike: float
    style: str
    natural_debit: float
    mid_debit: float
    start_limit: float
    chase_limit: float
    width: float
    max_profit: float
    breakeven: float
    rr: float
    debit_width_pct: float
    net_delta: float | None
    net_theta: float | None
    p_be_rn: float | None
    p_short_rn: float | None
    target_price: float
    target_return: float
    scenario_return: float
    max_leg_spread_pct: float
    min_leg_oi: int
    total_volume: int
    score: float
    preference_score: float
    action: str


def payoff_return_at_expiry(price: float, long_strike: float, short_strike: float, debit: float) -> float:
    value = min(max(price - long_strike, 0.0), short_strike - long_strike)
    return (value / debit) - 1.0 if debit > 0 else -1.0


def classify_style(current_price: float, long_strike: float, rr: float, debit_width_pct: float) -> str:
    moneyness = (long_strike / current_price) - 1.0
    if moneyness <= 0.01 and debit_width_pct >= 0.30:
        return "higher_win"
    if moneyness >= 0.04 and rr >= 4.0:
        return "lotto"
    return "balanced"


def score_candidate(
    *,
    rr: float,
    debit_width_pct: float,
    max_leg_spread_pct: float,
    min_leg_oi: int,
    total_volume: int,
    net_delta: float | None,
    net_theta: float | None,
    mid_debit: float,
    width: float,
    target_price: float,
    breakeven: float,
    short_strike: float,
    scenario_return: float,
) -> float:
    scenario_score = clamp((scenario_return + 0.25) / 2.75)
    target_hit_score = clamp((target_price - breakeven) / max(short_strike - breakeven, 0.01))
    rr_score = clamp(rr / 4.0)
    debit_score = clamp((0.40 - debit_width_pct) / 0.30)
    spread_score = clamp((0.45 - max_leg_spread_pct) / 0.45)
    oi_score = clamp(math.log1p(min_leg_oi) / math.log1p(500))
    volume_score = clamp(math.log1p(total_volume) / math.log1p(500))
    liquidity_score = 0.45 * spread_score + 0.35 * oi_score + 0.20 * volume_score
    if net_delta is None:
        delta_score = 0.5
    else:
        delta_score = clamp(1.0 - abs(net_delta - 0.25) / 0.25)
    if net_theta is None or mid_debit <= 0:
        theta_score = 0.5
    else:
        theta_score = clamp(1.0 - abs(net_theta) / max(mid_debit, 0.01))
    if width <= 15:
        width_score = clamp((width - 5.0) / 10.0)
    else:
        width_score = clamp(1.0 - (width - 15.0) / 20.0)
    return 100.0 * (
        0.32 * scenario_score
        + 0.16 * target_hit_score
        + 0.15 * rr_score
        + 0.13 * liquidity_score
        + 0.09 * debit_score
        + 0.10 * width_score
        + 0.05 * delta_score
        + 0.05 * theta_score
    )


def action_for(candidate: SpreadCandidate) -> str:
    if candidate.mid_debit <= 0 or candidate.max_profit <= 0:
        return "skip_bad_quote"
    if candidate.min_leg_oi < 25 or candidate.max_leg_spread_pct > 0.60:
        return "watch_illiquid"
    if candidate.rr < 1.5 or candidate.debit_width_pct > 0.45:
        return "watch_too_expensive"
    if candidate.target_price < candidate.breakeven:
        return "watch_needs_bigger_move"
    if candidate.style == "balanced" and candidate.preference_score >= 60:
        return "main_candidate"
    if candidate.style == "higher_win" and candidate.preference_score >= 58:
        return "higher_win_candidate"
    if candidate.style == "lotto" and candidate.preference_score >= 55:
        return "lotto_candidate"
    return "watch"


def _parse_target_prices(values: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw in values:
        if "=" not in raw:
            continue
        symbol, value = raw.split("=", 1)
        out[symbol.strip().upper()] = float(value)
    return out


def _load_rows(con: duckdb.DuckDBPyConnection, symbol: str, expiry: str, as_of: date) -> tuple[date, list[dict]]:
    latest = con.execute(
        """
        SELECT MAX(as_of)
        FROM options_chain_quotes
        WHERE symbol = ? AND expiry = ? AND option_type = 'call' AND as_of <= CAST(? AS DATE)
        """,
        [symbol, expiry, as_of],
    ).fetchone()[0]
    if latest is None:
        return as_of, []
    rows = con.execute(
        """
        SELECT as_of, expiry, days_to_exp, current_price, strike, bid, ask, mid,
               volume, open_interest, implied_volatility, delta, theta
        FROM options_chain_quotes
        WHERE symbol = ? AND as_of = ? AND expiry = ? AND option_type = 'call'
        ORDER BY strike
        """,
        [symbol, latest, expiry],
    ).fetchall()
    keys = [
        "as_of",
        "expiry",
        "days_to_exp",
        "current_price",
        "strike",
        "bid",
        "ask",
        "mid",
        "volume",
        "open_interest",
        "implied_volatility",
        "delta",
        "theta",
    ]
    return latest, [dict(zip(keys, row)) for row in rows]


def rank_spreads(
    con: duckdb.DuckDBPyConnection,
    *,
    symbols: list[str],
    expiry: str,
    as_of: date,
    target_prices: dict[str, float] | None = None,
    target_move_pct: float = 0.07,
    min_width: float = 5.0,
    max_width: float = 25.0,
    strike_step: float = 5.0,
    max_long_otm_pct: float = 0.08,
    max_short_otm_pct: float = 0.16,
) -> list[SpreadCandidate]:
    target_prices = target_prices or {}
    candidates: list[SpreadCandidate] = []
    for symbol in symbols:
        latest_as_of, rows = _load_rows(con, symbol, expiry, as_of)
        if not rows:
            continue
        current_prices = [float(row["current_price"]) for row in rows if row.get("current_price")]
        if not current_prices:
            continue
        current_price = current_prices[0]
        target_price = target_prices.get(symbol, current_price * (1.0 + target_move_pct))
        legs: dict[float, OptionLeg] = {}
        for row in rows:
            bid = float(row.get("bid") or 0.0)
            ask = float(row.get("ask") or 0.0)
            mid = row.get("mid")
            if mid is None and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
            mid = float(mid or 0.0)
            if bid <= 0 or ask <= 0 or mid <= 0:
                continue
            strike = float(row["strike"])
            legs[strike] = OptionLeg(
                strike=strike,
                bid=bid,
                ask=ask,
                mid=mid,
                volume=int(row.get("volume") or 0),
                open_interest=int(row.get("open_interest") or 0),
                implied_volatility=float(row["implied_volatility"]) if row.get("implied_volatility") else None,
                delta=float(row["delta"]) if row.get("delta") is not None else None,
                theta=float(row["theta"]) if row.get("theta") is not None else None,
                days_to_exp=int(row["days_to_exp"]) if row.get("days_to_exp") is not None else None,
            )
        strikes = sorted(legs)
        for long_strike in strikes:
            if strike_step > 0 and abs((long_strike / strike_step) - round(long_strike / strike_step)) > 0.001:
                continue
            if long_strike < current_price * 0.97:
                continue
            if long_strike > current_price * (1.0 + max_long_otm_pct):
                continue
            for short_strike in strikes:
                if strike_step > 0 and abs((short_strike / strike_step) - round(short_strike / strike_step)) > 0.001:
                    continue
                width = short_strike - long_strike
                if width < min_width or width > max_width:
                    continue
                if short_strike > current_price * (1.0 + max_short_otm_pct):
                    continue
                long_leg = legs[long_strike]
                short_leg = legs[short_strike]
                natural_debit = long_leg.ask - short_leg.bid
                mid_debit = long_leg.mid - short_leg.mid
                if natural_debit <= 0 or mid_debit <= 0:
                    continue
                max_profit = width - mid_debit
                if max_profit <= 0:
                    continue
                rr = max_profit / mid_debit
                breakeven = long_strike + mid_debit
                debit_width_pct = mid_debit / width
                net_delta = (
                    None
                    if long_leg.delta is None or short_leg.delta is None
                    else long_leg.delta - short_leg.delta
                )
                net_theta = (
                    None
                    if long_leg.theta is None or short_leg.theta is None
                    else long_leg.theta - short_leg.theta
                )
                ivs = [x for x in [long_leg.implied_volatility, short_leg.implied_volatility] if x]
                avg_iv = sum(ivs) / len(ivs) if ivs else None
                p_be_rn = rn_prob_above(current_price, breakeven, avg_iv, long_leg.days_to_exp)
                p_short_rn = rn_prob_above(current_price, short_strike, avg_iv, long_leg.days_to_exp)
                mild_price = current_price + 0.50 * (target_price - current_price)
                high_price = current_price + 1.30 * (target_price - current_price)
                scenario_return = (
                    0.20 * payoff_return_at_expiry(mild_price, long_strike, short_strike, mid_debit)
                    + 0.35 * payoff_return_at_expiry(target_price, long_strike, short_strike, mid_debit)
                    + 0.45 * payoff_return_at_expiry(high_price, long_strike, short_strike, mid_debit)
                )
                target_return = payoff_return_at_expiry(target_price, long_strike, short_strike, mid_debit)
                max_leg_spread_pct = max(long_leg.spread_pct, short_leg.spread_pct)
                min_leg_oi = min(long_leg.open_interest, short_leg.open_interest)
                total_volume = long_leg.volume + short_leg.volume
                style = classify_style(current_price, long_strike, rr, debit_width_pct)
                score = score_candidate(
                    rr=rr,
                    debit_width_pct=debit_width_pct,
                    max_leg_spread_pct=max_leg_spread_pct,
                    min_leg_oi=min_leg_oi,
                    total_volume=total_volume,
                    net_delta=net_delta,
                    net_theta=net_theta,
                    mid_debit=mid_debit,
                    width=width,
                    target_price=target_price,
                    breakeven=breakeven,
                    short_strike=short_strike,
                    scenario_return=scenario_return,
                )
                style_bonus = {"balanced": 8.0, "higher_win": -2.0, "lotto": -6.0}.get(style, 0.0)
                candidate = SpreadCandidate(
                    symbol=symbol,
                    as_of=latest_as_of,
                    expiry=expiry,
                    current_price=current_price,
                    long_strike=long_strike,
                    short_strike=short_strike,
                    style=style,
                    natural_debit=natural_debit,
                    mid_debit=mid_debit,
                    start_limit=round_tick(mid_debit + 0.35 * (natural_debit - mid_debit)),
                    chase_limit=round_tick(min(natural_debit + 0.25, width * 0.30)),
                    width=width,
                    max_profit=max_profit,
                    breakeven=breakeven,
                    rr=rr,
                    debit_width_pct=debit_width_pct,
                    net_delta=net_delta,
                    net_theta=net_theta,
                    p_be_rn=p_be_rn,
                    p_short_rn=p_short_rn,
                    target_price=target_price,
                    target_return=target_return,
                    scenario_return=scenario_return,
                    max_leg_spread_pct=max_leg_spread_pct,
                    min_leg_oi=min_leg_oi,
                    total_volume=total_volume,
                    score=score,
                    preference_score=score + style_bonus,
                    action="",
                )
                candidates.append(candidate.__class__(**{**candidate.__dict__, "action": action_for(candidate)}))
    action_priority = {
        "main_candidate": 4,
        "higher_win_candidate": 3,
        "lotto_candidate": 3,
        "watch": 1,
        "watch_needs_bigger_move": 1,
        "watch_too_expensive": 0,
        "watch_illiquid": 0,
        "skip_bad_quote": -1,
    }
    return sorted(
        candidates,
        key=lambda item: (action_priority.get(item.action, 0), item.preference_score, item.score),
        reverse=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["BA", "BABA"])
    parser.add_argument("--expiry", default="2026-05-22")
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--target-move-pct", type=float, default=0.07)
    parser.add_argument("--target-price", action="append", default=[], help="Per-symbol target, e.g. BA=245.")
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument("--min-width", type=float, default=5.0)
    parser.add_argument("--max-width", type=float, default=25.0)
    parser.add_argument("--strike-step", type=float, default=5.0, help="Default keeps standard $5 strikes.")
    return parser.parse_args()


def fmt_pct(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def fmt_float(value: float | None, digits: int = 2) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def main() -> None:
    args = parse_args()
    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date() if args.as_of else default_as_of()
    symbols = sorted({symbol.strip().upper() for symbol in args.symbols if symbol.strip()})
    target_prices = _parse_target_prices(args.target_price)
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        ranked = rank_spreads(
            con,
            symbols=symbols,
            expiry=args.expiry,
            as_of=as_of,
            target_prices=target_prices,
            target_move_pct=args.target_move_pct,
            min_width=args.min_width,
            max_width=args.max_width,
            strike_step=args.strike_step,
        )
    finally:
        con.close()
    if not ranked:
        print("No valid call debit spreads found. Fetch option quotes first.")
        return
    print(
        "| Rank | Symbol | Spread | Style | Action | Score | Mid | Natural | Start limit | Chase cap | BE | R:R | "
        "Target | Target Ret | Scenario Ret | Net Delta | Net Theta | P(BE) | OI min | Vol | Max leg spread |"
    )
    print("|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for idx, row in enumerate(ranked[: args.top], 1):
        spread = f"{row.long_strike:.0f}/{row.short_strike:.0f}"
        print(
            f"| {idx} | {row.symbol} | {spread} | {row.style} | {row.action} | "
            f"{row.preference_score:.1f} | {row.mid_debit:.2f} | {row.natural_debit:.2f} | "
            f"{row.start_limit:.2f} | {row.chase_limit:.2f} | {row.breakeven:.2f} | "
            f"{row.rr:.2f} | {row.target_price:.2f} | {fmt_pct(row.target_return)} | "
            f"{fmt_pct(row.scenario_return)} | {fmt_float(row.net_delta, 2)} | "
            f"{fmt_float(row.net_theta, 3)} | {fmt_pct(row.p_be_rn)} | "
            f"{row.min_leg_oi} | {row.total_volume} | {fmt_pct(row.max_leg_spread_pct)} |"
        )


if __name__ == "__main__":
    main()
