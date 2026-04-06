"""
Mean-reversion signal detection.

Computes for each symbol:
  - ma20_pct:   (close - SMA20) / SMA20 as percentage
  - ma20_z:     cross-sectional z-score of ma20_pct, clamped [-3, 3]
  - rsi_14:     standard RSI (14-period)
  - bb_position: (close - lower_band) / (upper_band - lower_band),
                 Bollinger bands with 2σ
  - reversion_score: weighted composite ∈ [0, 1]
      0.35 * rsi_extreme + 0.35 * bb_extreme + 0.30 * ma_extreme
  - reversion_direction: bullish_reversion | bearish_reversion | neutral

Stored in analysis_daily with module_name='mean_reversion'.
"""
from __future__ import annotations

import json
from datetime import date

import duckdb
import numpy as np
import polars as pl
import structlog

log = structlog.get_logger()


def _rsi(close: np.ndarray, period: int = 14) -> float:
    """Standard RSI over the last `period` bars."""
    if len(close) < period + 1:
        return 50.0  # neutral fallback
    deltas = np.diff(close[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss < 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def _bollinger_position(close: np.ndarray, period: int = 20, n_std: float = 2.0) -> float:
    """(close - lower_band) / (upper_band - lower_band). Returns 0.5 if bands collapse."""
    if len(close) < period:
        return 0.5
    window = close[-period:]
    sma = float(np.mean(window))
    std = float(np.std(window, ddof=1)) if len(window) > 1 else 0.0
    if std < 1e-12:
        return 0.5
    upper = sma + n_std * std
    lower = sma - n_std * std
    band_width = upper - lower
    if band_width < 1e-12:
        return 0.5
    return float((close[-1] - lower) / band_width)


def run_mean_reversion(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> pl.DataFrame:
    """
    Compute mean-reversion signals for all symbols.
    Returns a DataFrame matching the analysis_daily schema.
    """
    lookback = 60  # need enough bars for RSI + SMA20 + cross-sectional z
    cutoff = as_of.strftime("%Y-%m-%d")

    price_data = con.execute(f"""
        SELECT symbol, date, adj_close, close
        FROM prices_daily
        WHERE symbol IN ({','.join('?' * len(symbols))})
          AND date <= '{cutoff}'
        ORDER BY symbol, date
    """, symbols).fetchdf()

    if price_data.empty:
        log.warning("mean_reversion_no_data")
        return pl.DataFrame()

    df = pl.from_pandas(price_data).sort(["symbol", "date"])

    # Per-symbol raw metrics (before cross-sectional z)
    pending: list[dict] = []

    for sym in symbols:
        sdf = df.filter(pl.col("symbol") == sym)
        if len(sdf) < 21:  # need at least 20+1 bars for SMA20
            continue

        ac = sdf["adj_close"].to_numpy().astype(float)

        # SMA20 percentage deviation
        sma20 = float(np.mean(ac[-20:]))
        if sma20 < 1e-9:
            continue
        ma20_pct = float((ac[-1] - sma20) / sma20 * 100.0)

        # RSI 14
        rsi_14 = _rsi(ac, period=14)

        # Bollinger position
        bb_pos = _bollinger_position(ac, period=20, n_std=2.0)

        # Component extremes
        rsi_extreme = abs(rsi_14 - 50.0) / 50.0
        bb_extreme = min(abs(bb_pos - 0.5) * 2.0, 1.0)
        # ma_extreme placeholder — will use z-score after cross-sectional step

        pending.append({
            "symbol": sym,
            "ma20_pct": ma20_pct,
            "rsi_14": rsi_14,
            "bb_position": bb_pos,
            "rsi_extreme": rsi_extreme,
            "bb_extreme": bb_extreme,
        })

    if not pending:
        return pl.DataFrame()

    # Cross-sectional z-score of ma20_pct
    ma20_pcts = np.array([p["ma20_pct"] for p in pending])
    mean_ma = float(np.mean(ma20_pcts))
    std_ma = float(np.std(ma20_pcts) + 1e-9)

    results = []
    for p in pending:
        ma20_z = (p["ma20_pct"] - mean_ma) / std_ma
        ma20_z = max(-3.0, min(3.0, ma20_z))
        ma_extreme = min(abs(ma20_z) / 3.0, 1.0)

        # Signed reversion score ∈ [-1, +1]
        # Positive = oversold (expect up), Negative = overbought (expect down)
        # Previous: unsigned score mixed oversold+overbought → IC was negative
        rsi = p["rsi_14"]
        bb = p["bb_position"]

        rsi_signal = (50.0 - rsi) / 50.0          # +1 at RSI=0, -1 at RSI=100
        bb_signal = (0.5 - bb) * 2.0               # +1 at lower band, -1 at upper band
        ma_signal = -ma20_z / 3.0                   # positive when below MA

        reversion_score = (0.35 * rsi_signal + 0.35 * bb_signal + 0.30 * ma_signal)
        reversion_score = max(-1.0, min(1.0, reversion_score))

        # Direction from sign
        if reversion_score > 0.2:
            direction = "bullish_reversion"
        elif reversion_score < -0.2:
            direction = "bearish_reversion"
        else:
            direction = "neutral"

        details = {
            "ma20_pct": round(p["ma20_pct"], 3),
            "ma20_z": round(ma20_z, 4),
            "rsi_14": round(p["rsi_14"], 2),
            "bb_position": round(p["bb_position"], 4),
            "rsi_extreme": round(p["rsi_extreme"], 4),
            "bb_extreme": round(p["bb_extreme"], 4),
            "ma_extreme": round(ma_extreme, 4),
            "reversion_score": round(reversion_score, 4),
            "reversion_direction": direction,
        }

        results.append({
            "symbol": p["symbol"],
            "date": as_of,
            "module_name": "mean_reversion",
            "trend_prob": None,
            "p_upside": None,
            "p_downside": None,
            "daily_risk_usd": None,
            "expected_move_pct": None,
            "z_score": round(ma20_z, 4),
            "p_value_raw": None,
            "p_value_bonf": None,
            "strength_bucket": (
                "strong" if abs(reversion_score) >= 0.7
                else "moderate" if abs(reversion_score) >= 0.4
                else "weak" if abs(reversion_score) >= 0.2
                else "inconclusive"
            ),
            "regime": direction,
            "details": json.dumps(details),
        })

    log.info(
        "mean_reversion_computed",
        n_symbols=len(results),
        n_bullish=sum(1 for r in results if r["regime"] == "bullish_reversion"),
        n_bearish=sum(1 for r in results if r["regime"] == "bearish_reversion"),
    )

    return pl.DataFrame(results).with_columns(pl.col("date").cast(pl.Date))


def store_mean_reversion(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Store mean-reversion results into analysis_daily."""
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
