"""Entry-setup gate — should a production-pool name be ENTERED today?

The four universe gates (pool → evidence → sleeve → regime) all control
*membership*, not *timing*. Without an entry gate every production name
gets a mechanical buy-at-close plan every single day — which is exactly
the "每天都在推荐做多" complaint.

This gate adds the missing timing discipline. It does NOT decide whether a
name belongs in the book — membership already did that. It only decides
whether *today* is a sane moment to initiate / add:

  trend_broken — close below EMA50. Trend gone; do not start a new long.
  extended     — close too far above EMA20. Chasing; wait for a pullback.
  pullback     — above EMA50 and not extended. Trend intact, price has
                 come back to the moving average — a real entry setup.
  no_data      — not enough history; fail-open (do not block).

A held position is managed by its own risk plan — this gate is about
fresh entries / adds only.
"""
from __future__ import annotations

from dataclasses import dataclass

DEFAULT_EXTENDED_PCT = 8.0   # close > this far above EMA20 = chasing → wait


@dataclass(frozen=True)
class EntrySetup:
    has_setup: bool
    setup_type: str           # pullback | extended | trend_broken | no_data
    ext_pct: float | None     # close vs EMA20, in %
    reason: str


def ema(values: list[float], period: int) -> list[float]:
    """Causal EMA series aligned to `values`."""
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def classify_entry_setup(
    close: float | None,
    ema20: float | None,
    ema50: float | None,
    *,
    extended_pct: float = DEFAULT_EXTENDED_PCT,
) -> EntrySetup:
    """Pure classifier: today's close + EMA20/50 → entry-setup verdict."""
    if close is None or ema20 is None or ema50 is None or ema20 <= 0:
        return EntrySetup(True, "no_data", None, "缺均线数据 — 不拦截(fail-open)")
    ext = (close / ema20 - 1.0) * 100.0
    if close <= ema50:
        return EntrySetup(False, "trend_broken", ext,
                          "收于 EMA50 下,趋势破位 — 不开新仓")
    if ext > extended_pct:
        return EntrySetup(False, "extended", ext,
                          f"高于 EMA20 {ext:+.1f}%,追高 — 等回调")
    return EntrySetup(True, "pullback", ext,
                      f"站 EMA50、贴近 EMA20({ext:+.1f}%) — 回调到位,可进场")


def setup_from_closes(
    closes: list[float], *, extended_pct: float = DEFAULT_EXTENDED_PCT
) -> EntrySetup:
    """Convenience: classify from a close series ending on the eval day."""
    if len(closes) < 50:
        return EntrySetup(True, "no_data", None, "历史不足 50 日 — 不拦截")
    return classify_entry_setup(
        closes[-1], ema(closes, 20)[-1], ema(closes, 50)[-1],
        extended_pct=extended_pct,
    )
