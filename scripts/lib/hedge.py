"""Shared beta hedge math for current overlays and historical ledgers."""
from __future__ import annotations

import math
import statistics
from typing import Any

CN_BETA_HEDGE_RATIO = 0.70
US_BETA_HEDGE_RATIO = 0.50
CN_MARKET_BETA_FLOOR = 0.35
US_MARKET_BETA_FLOOR = 0.30
CN_HEDGE_BENCHMARKS = ("IM.CFX", "IC.CFX", "IF.CFX", "IH.CFX")
US_HEDGE_BENCHMARKS = ("SPY", "IWM", "QQQ", "DIA")


def corr(a: list[float], b: list[float], *, min_periods: int = 20) -> float | None:
    n = min(len(a), len(b))
    if n < min_periods:
        return None
    x = a[-n:]
    y = b[-n:]
    mx = statistics.fmean(x)
    my = statistics.fmean(y)
    vx = sum((v - mx) ** 2 for v in x)
    vy = sum((v - my) ** 2 for v in y)
    if vx <= 0 or vy <= 0:
        return None
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    return max(-1.0, min(1.0, cov / math.sqrt(vx * vy)))


def beta(asset: list[float], benchmark: list[float], *, min_periods: int = 20) -> float | None:
    n = min(len(asset), len(benchmark))
    if n < min_periods:
        return None
    x = asset[-n:]
    y = benchmark[-n:]
    mx = statistics.fmean(x)
    my = statistics.fmean(y)
    var_bench = sum((v - my) ** 2 for v in y)
    if var_bench <= 0:
        return None
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    return max(-5.0, min(5.0, cov / var_bench))


def returns_from_closes(values: list[float]) -> list[float]:
    returns: list[float] = []
    for prev, cur in zip(values, values[1:], strict=False):
        if prev and prev > 0 and cur is not None:
            returns.append(cur / prev - 1.0)
    return returns


def select_beta_hedge(
    market: str,
    asset_returns: list[float],
    benchmarks: dict[str, list[float]],
) -> tuple[str, float | None, float | None]:
    market_upper = str(market or "").upper()
    default = "IM.CFX" if market_upper == "CN" else "SPY"
    best: tuple[str, float, float, float] | None = None
    for instrument, benchmark_returns in benchmarks.items():
        beta_value = beta(asset_returns, benchmark_returns)
        corr_value = corr(asset_returns, benchmark_returns)
        if beta_value is None or corr_value is None:
            continue
        positive_beta = max(beta_value, 0.0)
        score = max(corr_value, 0.0) * positive_beta
        if best is None or score > best[3]:
            best = (instrument, positive_beta, corr_value, score)
    if best is None:
        return default, None, None
    if best[3] <= 0:
        return default, 0.0, best[2]
    return best[0], best[1], best[2]


def hedge_ratio_for_market(market: str) -> float:
    return CN_BETA_HEDGE_RATIO if str(market or "").upper() == "CN" else US_BETA_HEDGE_RATIO


def beta_floor_for_market(market: str) -> float:
    return CN_MARKET_BETA_FLOOR if str(market or "").upper() == "CN" else US_MARKET_BETA_FLOOR


def beta_for_size(market: str, beta_value: float | None) -> tuple[float, str]:
    floor = beta_floor_for_market(market)
    if beta_value is None:
        return 1.0, "fallback_beta_1"
    positive = max(beta_value, 0.0)
    if positive < floor:
        return floor, "market_beta_floor"
    return positive, "return_beta"


def hedge_notional_r(market: str, long_r: float, beta_value: float | None) -> tuple[float, float, str]:
    sized_beta, source = beta_for_size(market, beta_value)
    ratio = hedge_ratio_for_market(market)
    notional = min(float(long_r) * 0.90, float(long_r) * sized_beta * ratio) if long_r > 0 else 0.0
    return notional, sized_beta, source


def hedged_return_r(
    *,
    long_ret_pct: float,
    benchmark_ret_pct: float,
    long_r: float = 1.0,
    hedge_r: float | None = None,
    beta_value: float | None = None,
    hedge_ratio: float | None = None,
    hedge_cost_r: float = 0.0,
) -> float:
    """Return net R for a long book hedged with a short benchmark leg.

    If `hedge_r` is not supplied, size from `long_r * beta * hedge_ratio`.
    Positive benchmark returns hurt the short hedge, negative benchmark returns help.
    """
    if hedge_r is None:
        sized_beta = 1.0 if beta_value is None else max(float(beta_value), 0.0)
        ratio = 0.0 if hedge_ratio is None else float(hedge_ratio)
        hedge_r = float(long_r) * sized_beta * ratio
    long_return_r = float(long_r) * float(long_ret_pct) / 100.0
    beta_hedge_return_r = float(hedge_r) * float(benchmark_ret_pct) / 100.0
    return long_return_r - beta_hedge_return_r - float(hedge_cost_r)


def promoted_sleeve_lookup(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for row in rows:
        if str(row.get("status") or "").lower() != "promoted":
            continue
        out.add((str(row.get("market") or "").lower(), str(row.get("sleeve_id") or "")))
    return out
