"""
Breakout detection analysis.

Computes for each symbol:
  - squeeze_ratio:  current 20D Bollinger bandwidth / 60D average bandwidth
  - squeeze_score:  (1.0 - squeeze_ratio).clamp(0, 1)
  - vol_ratio:      today's volume / 20D average volume
  - volume_score:   ((vol_ratio - 1.0) / 1.5).clamp(0, 1)
  - range_score:    1.0 if close > 20D high or < 20D low,
                    0.6 if 10D break, 0.0 otherwise
  - vol_expansion:  (std5/std20 - 0.7) / 0.8, clamped [0, 1]
  - breakout_score: 0.30*squeeze + 0.25*volume + 0.25*range + 0.20*vol_expansion
  - breakout_direction: bullish_breakout | bearish_breakout | coiled | none

Stored in analysis_daily with module_name='breakout'.
"""
from __future__ import annotations

import json
from datetime import date

import duckdb
import numpy as np
import polars as pl
import structlog

log = structlog.get_logger()


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _bollinger_bandwidth(close: np.ndarray, period: int = 20, n_std: float = 2.0) -> float:
    """Bandwidth = (upper - lower) / SMA. Returns 0 if insufficient data."""
    if len(close) < period:
        return 0.0
    window = close[-period:]
    sma = float(np.mean(window))
    if sma < 1e-9:
        return 0.0
    std = float(np.std(window, ddof=1)) if len(window) > 1 else 0.0
    return float(2.0 * n_std * std / sma)


def run_breakout(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> pl.DataFrame:
    """
    Compute breakout detection signals for all symbols.
    Returns a DataFrame matching the analysis_daily schema.
    """
    lookback = 80  # need 60D rolling bandwidth + margin
    cutoff = as_of.strftime("%Y-%m-%d")

    price_data = con.execute(f"""
        SELECT symbol, date, adj_close, close, high, low, volume
        FROM prices_daily
        WHERE symbol IN ({','.join('?' * len(symbols))})
          AND date <= '{cutoff}'
        ORDER BY symbol, date
    """, symbols).fetchdf()

    if price_data.empty:
        log.warning("breakout_no_data")
        return pl.DataFrame()

    df = pl.from_pandas(price_data).sort(["symbol", "date"])

    results = []

    for sym in symbols:
        sdf = df.filter(pl.col("symbol") == sym)
        if len(sdf) < 21:  # need at least 20 bars for basic metrics
            continue

        ac = sdf["adj_close"].to_numpy().astype(float)
        hi = sdf["high"].to_numpy().astype(float)
        lo = sdf["low"].to_numpy().astype(float)
        vol = sdf["volume"].to_numpy().astype(float)

        current_close = float(ac[-1])

        # ── Squeeze ratio: current bandwidth / 60D average bandwidth ──
        current_bw = _bollinger_bandwidth(ac, period=20)
        # Compute rolling bandwidth over last 60 days
        if len(ac) >= 60:
            rolling_bws = []
            for i in range(len(ac) - 60, len(ac)):
                if i >= 20:
                    bw = _bollinger_bandwidth(ac[:i + 1], period=20)
                    rolling_bws.append(bw)
            avg_bw_60 = float(np.mean(rolling_bws)) if rolling_bws else current_bw
        else:
            avg_bw_60 = current_bw

        squeeze_ratio = current_bw / avg_bw_60 if avg_bw_60 > 1e-9 else 1.0
        squeeze_score = _clamp(1.0 - squeeze_ratio)

        # ── Volume score ──
        avg_vol_20 = float(np.mean(vol[-21:-1])) if len(vol) >= 21 else float(np.mean(vol[:-1]) + 1e-9)
        vol_ratio = float(vol[-1]) / avg_vol_20 if avg_vol_20 > 1e-9 else 1.0
        volume_score = _clamp((vol_ratio - 1.0) / 1.5)

        # ── Range score: price break above/below N-day high/low ──
        range_score = 0.0
        if len(hi) >= 21 and len(lo) >= 21:
            high_20d = float(np.max(hi[-21:-1]))
            low_20d = float(np.min(lo[-21:-1]))
            if current_close > high_20d or current_close < low_20d:
                range_score = 1.0
            elif len(hi) >= 11 and len(lo) >= 11:
                high_10d = float(np.max(hi[-11:-1]))
                low_10d = float(np.min(lo[-11:-1]))
                if current_close > high_10d or current_close < low_10d:
                    range_score = 0.6

        # ── Volatility expansion: std5/std20 ──
        if len(ac) >= 21:
            returns = np.diff(np.log(np.maximum(ac, 1e-9)))
            std5 = float(np.std(returns[-5:])) if len(returns) >= 5 else 0.0
            std20 = float(np.std(returns[-20:])) if len(returns) >= 20 else 1e-9
            vol_expansion = _clamp((std5 / max(std20, 1e-9) - 0.7) / 0.8)
        else:
            vol_expansion = 0.0

        # ── Composite breakout score ──
        breakout_score = (
            0.30 * squeeze_score
            + 0.25 * volume_score
            + 0.25 * range_score
            + 0.20 * vol_expansion
        )
        breakout_score = _clamp(breakout_score)

        # ── Direction classification ──
        # Determine if we're at the top or bottom of the range
        is_above_20d_high = len(hi) >= 21 and current_close > float(np.max(hi[-21:-1]))
        is_below_20d_low = len(lo) >= 21 and current_close < float(np.min(lo[-21:-1]))

        if breakout_score >= 0.3 and is_above_20d_high:
            direction = "bullish_breakout"
        elif breakout_score >= 0.3 and is_below_20d_low:
            direction = "bearish_breakout"
        elif squeeze_score >= 0.5 and breakout_score < 0.3:
            direction = "coiled"
        else:
            direction = "none"

        details = {
            "squeeze_ratio": round(squeeze_ratio, 4),
            "squeeze_score": round(squeeze_score, 4),
            "vol_ratio": round(vol_ratio, 3),
            "volume_score": round(volume_score, 4),
            "range_score": round(range_score, 2),
            "vol_expansion": round(vol_expansion, 4),
            "breakout_score": round(breakout_score, 4),
            "breakout_direction": direction,
            "current_bw": round(current_bw, 6),
            "avg_bw_60": round(avg_bw_60, 6),
        }

        results.append({
            "symbol": sym,
            "date": as_of,
            "module_name": "breakout",
            "trend_prob": None,
            "p_upside": None,
            "p_downside": None,
            "daily_risk_usd": None,
            "expected_move_pct": None,
            "z_score": None,
            "p_value_raw": None,
            "p_value_bonf": None,
            "strength_bucket": (
                "strong" if breakout_score >= 0.6
                else "moderate" if breakout_score >= 0.3
                else "weak" if breakout_score >= 0.15
                else "inconclusive"
            ),
            "regime": direction,
            "details": json.dumps(details),
        })

    if not results:
        return pl.DataFrame()

    log.info(
        "breakout_computed",
        n_symbols=len(results),
        n_bullish=sum(1 for r in results if r["regime"] == "bullish_breakout"),
        n_bearish=sum(1 for r in results if r["regime"] == "bearish_breakout"),
        n_coiled=sum(1 for r in results if r["regime"] == "coiled"),
    )

    return pl.DataFrame(results).with_columns(pl.col("date").cast(pl.Date))


def store_breakout(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Store breakout results into analysis_daily."""
    if df.is_empty():
        return 0
    con.register("analysis_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO analysis_daily
        SELECT
            symbol, date, module_name,
            trend_prob, p_upside, p_downside,
            daily_risk_usd, expected_move_pct,
            z_score, p_value_raw, p_value_bonf,
            strength_bucket, regime, details
        FROM analysis_updates
    """)
    con.unregister("analysis_updates")
    con.commit()
    return len(df)
