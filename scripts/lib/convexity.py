"""Convexity classification — the system's lens on payoff shape.

Convexity philosophy (operator essay, 2026-05-18):
  duration  = you assume the world is linear
  convexity = you admit it is non-linear
  black swan = convexity erupting at the tail

A *good* trade is convex: downside is bounded/defined, upside is non-linear.
An *anti-convex* (concave) trade is the opposite: it wins small and often,
then loses everything once on a tail event — selling options, shorting
volatility, leveraged range-trading (XIV 2018, Optionsellers 2020).

This module classifies any expression string into one of:
  - convex      : bounded loss, non-linear upside (long options, debit
                  spreads, deep-OTM wings, LEAPS calls)
  - linear      : roughly symmetric, bounded both ways (stock long with a
                  stop, an ETF position)
  - anti_convex : the structurally-forbidden shape — capped gain, tail loss
  - none        : no position / not an executable expression

`assert_no_anticonvex` is the hard guardrail: the system must NEVER emit an
anti-convex expression. It is structural suicide, not a sizing question.
"""
from __future__ import annotations

from typing import Iterable

# Markers are matched case-insensitively as substrings. Order: anti_convex
# is checked first so an explicit "credit spread" / "covered call" is never
# mislabelled convex by the "spread" / "call" token.
ANTI_CONVEX_MARKERS: tuple[str, ...] = (
    "sell put", "sell call", "short put", "short call", "sell_put", "sell_call",
    "write call", "write put", "covered call", "naked option", "naked call",
    "naked put", "short straddle", "short strangle", "short_straddle",
    "credit spread", "iron condor", "short vol", "short volatility",
    "short gamma", "sell option", "sell_option", "sell premium",
    "卖期权", "卖出期权", "做空波动率", "做空波动", "卖看涨", "卖看跌",
    "反凸", "杠杆窄幅", "杠杆做窄幅",
)

CONVEX_MARKERS: tuple[str, ...] = (
    "buy put", "buy call", "long put", "long call", "buy_put", "buy_call",
    "put spread", "call spread", "put-spread", "call-spread", "debit spread",
    "leaps", "otm put", "otm call", "deep otm", "long vol", "long volatility",
    "long gamma", "victim put", "买入期权", "买 put", "买 call", "买看涨",
    "买看跌", "深虚值", "凸性", "put_spread", "call_spread",
)


def classify_convexity(expression: object) -> str:
    """Classify an expression string → convex / linear / anti_convex / none."""
    text = str(expression or "").strip().lower()
    if not text:
        return "none"
    # "no trade" / "rank only" style actions carry no position.
    if any(tok in text for tok in ("no_new_trade", "no_trade", "rank_only", "no new trade")):
        return "none"
    if any(m in text for m in ANTI_CONVEX_MARKERS):
        return "anti_convex"
    if any(m in text for m in CONVEX_MARKERS):
        return "convex"
    return "linear"


_LABEL = {
    "convex": "凸(下行锁死/上行非线性)",
    "linear": "线性(止损框住,对称)",
    "anti_convex": "反凸(尾部一次亏完 — 禁止)",
    "none": "—",
}


def convexity_label(expression: object) -> str:
    """Human-readable convexity label for an expression."""
    return _LABEL[classify_convexity(expression)]


class AntiConvexExpressionError(RuntimeError):
    """Raised when the system would emit a structurally-forbidden expression."""


def assert_no_anticonvex(expressions: Iterable[object]) -> None:
    """Hard guardrail: refuse any anti-convex expression.

    Selling options / shorting vol / leveraged range-trading wins small and
    blows up once on the tail. The system never suggests these — not as a
    preference, as a rule.
    """
    offenders = [
        str(e) for e in expressions
        if classify_convexity(e) == "anti_convex"
    ]
    if offenders:
        raise AntiConvexExpressionError(
            "anti-convex expression(s) blocked — selling premium / shorting "
            "vol / leveraged range-trading is forbidden (small-win / tail-ruin "
            f"payoff): {offenders}"
        )
