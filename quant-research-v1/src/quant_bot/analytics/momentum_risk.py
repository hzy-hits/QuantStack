"""
Momentum regime risk analysis.

Primary output: P(5D forward return > 0 | regime, vol_bucket)
computed from a 9-cell conditional probability table (CPT)
with a Beta-Binomial posterior. Prior: Beta(2, 2).

  regime:     trending | mean_reverting | noisy
              (from 20-bar lag-1 return autocorrelation)
  vol_bucket: low | mid | high
              (today's volume / 20-day avg volume, universe terciles)

The cross-sectional z_score is DESCRIPTIVE only — it ranks where today's
momentum sits vs the universe. It is NOT a sampling-distribution test
statistic and must not be converted to a p-value.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date

import duckdb
import numpy as np
import polars as pl
import structlog

from quant_bot.analytics.bayes import BetaPosterior, beta_binomial_update, strength_bucket

log = structlog.get_logger()


@dataclass
class MomentumCPT:
    """9-cell conditional probability table keyed by (regime, vol_bucket)."""
    cells: dict[tuple[str, str], tuple[int, int]] = field(default_factory=dict)
    vol_q1: float = 1.0   # 33rd percentile of universe relative volume
    vol_q2: float = 2.0   # 67th percentile


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
    """Average True Range over the last `period` bars."""
    if len(close) < period + 1:
        return float("nan")
    tr = np.maximum(
        high[-period:] - low[-period:],
        np.maximum(
            np.abs(high[-period:] - close[-period - 1:-1]),
            np.abs(low[-period:]  - close[-period - 1:-1]),
        ),
    )
    return float(np.mean(tr))


def _classify_regime(returns: np.ndarray, window: int = 20) -> str:
    """Classify market regime from lag-1 autocorrelation of recent returns."""
    if len(returns) < window:
        return "noisy"
    r = returns[-window:]
    if len(r) < 4:
        return "noisy"
    autocorr = float(np.corrcoef(r[:-1], r[1:])[0, 1])
    if np.isnan(autocorr):
        return "noisy"
    if autocorr > 0.15:
        return "trending"
    if autocorr < -0.10:
        return "mean_reverting"
    return "noisy"


def _build_cpt(df: pl.DataFrame) -> MomentumCPT:
    """
    Build 9-cell CPT from all historical price + volume data.

    For each (symbol, day) observation, classifies regime and vol_bucket,
    then records whether the 5-day forward return was positive (hit) or not.
    Returns raw counts (hits, n_obs) per cell — Beta-Binomial update
    is applied at query time via beta_binomial_update().
    """
    # First pass: collect all relative volumes to set universe-wide tercile thresholds
    rel_vols_all: list[float] = []
    for sym in df["symbol"].unique().to_list():
        sdf = df.filter(pl.col("symbol") == sym)
        vol = sdf["volume"].to_numpy().astype(float)
        for i in range(1, len(vol)):
            start = max(0, i - 20)
            avg = np.mean(vol[start:i])
            if avg > 0 and vol[i] > 0:
                rel_vols_all.append(vol[i] / avg)

    if len(rel_vols_all) < 30:
        log.warning("momentum_cpt_insufficient_volume_data", n=len(rel_vols_all))
        return MomentumCPT()

    vol_q1 = float(np.nanpercentile(rel_vols_all, 33.3))
    vol_q2 = float(np.nanpercentile(rel_vols_all, 66.7))

    # Second pass: classify each (symbol, day) and record 5-day forward outcome
    # EWMA-weighted: recent observations count more (half-life = 30 trading days)
    EWMA_HALF_LIFE = 30.0
    decay = np.log(0.5) / EWMA_HALF_LIFE  # negative

    cells: dict[tuple[str, str], list[float]] = {}  # (regime, vol_bucket) -> [weighted_hits, weighted_total]

    for sym in df["symbol"].unique().to_list():
        sdf = df.filter(pl.col("symbol") == sym)
        ac  = sdf["adj_close"].to_numpy().astype(float)
        vol = sdf["volume"].to_numpy().astype(float)

        if len(ac) < 27:   # 20 autocorr window + 1 lag + 5 forward + 1 buffer
            continue

        returns = np.diff(np.log(np.maximum(ac, 1e-9)))
        last_idx = len(ac) - 1

        for i in range(20, len(ac) - 5):
            # Regime: 20-bar rolling autocorrelation ending at index i (returns[i-20:i])
            r_win = returns[i - 20:i]
            if len(r_win) < 4:
                continue
            autocorr = float(np.corrcoef(r_win[:-1], r_win[1:])[0, 1])
            if np.isnan(autocorr):
                continue

            regime = (
                "trending"       if autocorr >  0.15 else
                "mean_reverting" if autocorr < -0.10 else
                "noisy"
            )

            # Vol bucket: today's volume vs prior 20-day average
            avg_vol = np.mean(vol[max(0, i - 20):i])
            if avg_vol <= 0:
                continue
            rel_vol = vol[i] / avg_vol
            vol_bucket = (
                "low"  if rel_vol < vol_q1 else
                "mid"  if rel_vol < vol_q2 else
                "high"
            )

            # 5-day forward return outcome
            fwd_ret = (ac[i + 5] / ac[i]) - 1.0
            hit = 1 if fwd_ret > 0 else 0

            # EWMA weight: recent observations count more
            age = float(last_idx - i - 5)  # bars from most recent outcome
            weight = float(np.exp(decay * age))

            key = (regime, vol_bucket)
            if key not in cells:
                cells[key] = [0.0, 0.0]
            cells[key][0] += hit * weight
            cells[key][1] += weight

    return MomentumCPT(
        cells={k: (int(round(v[0])), int(round(v[1]))) for k, v in cells.items()},
        vol_q1=vol_q1,
        vol_q2=vol_q2,
    )


def _cpt_lookup(cpt: MomentumCPT, regime: str, vol_bucket: str) -> BetaPosterior:
    """Look up a CPT cell and return the Beta-Binomial posterior."""
    hits, n_obs = cpt.cells.get((regime, vol_bucket), (0, 0))
    return beta_binomial_update(hits, n_obs)


def run_momentum_risk(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    momentum_windows: list[int] | None = None,
    atr_period: int = 14,
    ma_filter_window: int = 20,
) -> pl.DataFrame:
    """
    Compute momentum risk/probability analysis for all symbols.
    Returns a DataFrame matching the analysis_daily schema.
    """
    if momentum_windows is None:
        momentum_windows = [20, 60]

    lookback = max(momentum_windows) + atr_period + 5
    cutoff = as_of.strftime("%Y-%m-%d")

    price_data = con.execute(f"""
        SELECT symbol, date, adj_close, high, low, close, volume
        FROM prices_daily
        WHERE symbol IN ({','.join('?' * len(symbols))})
          AND date <= '{cutoff}'
        ORDER BY symbol, date
    """, symbols).fetchdf()

    if price_data.empty:
        log.warning("momentum_risk_no_data")
        return pl.DataFrame()

    df = pl.from_pandas(price_data).sort(["symbol", "date"])

    # Build CPT once from all loaded historical data
    cpt = _build_cpt(df)
    log.info(
        "momentum_cpt_built",
        n_cells=len(cpt.cells),
        vol_q1=round(cpt.vol_q1, 3),
        vol_q2=round(cpt.vol_q2, 3),
        total_obs=sum(v[1] for v in cpt.cells.values()),
    )

    # Per-symbol analysis
    pending: list[dict] = []   # accumulate before z-scoring

    for sym in symbols:
        sdf = df.filter(pl.col("symbol") == sym)
        if len(sdf) < lookback:
            log.debug("momentum_risk_insufficient_history", symbol=sym, rows=len(sdf))
            continue

        ac  = sdf["adj_close"].to_numpy()
        hi  = sdf["high"].to_numpy()
        lo  = sdf["low"].to_numpy()
        cl  = sdf["close"].to_numpy()
        vol = sdf["volume"].to_numpy().astype(float)

        # Momentum returns
        mom = {}
        for w in momentum_windows:
            mom[w] = (ac[-1] / ac[-w - 1]) - 1.0 if len(ac) > w else 0.0

        # Momentum deceleration: ratio of short-term to medium-term momentum
        # Low ratio (< 0.3) when 20D is strong = trend losing steam
        momentum_accel = None
        if 5 in momentum_windows and 20 in momentum_windows:
            mom_5 = mom.get(5, 0.0)
            mom_20 = mom.get(20, 0.0)
            if abs(mom_20) > 0.005:
                momentum_accel = mom_5 / mom_20

        # ATR and daily risk
        atr_val = _atr(hi, lo, cl, atr_period)
        daily_risk_usd = atr_val * ac[-1] / cl[-1] if cl[-1] > 0 else atr_val
        expected_move_pct = round(float(atr_val) / cl[-1] * 100, 2) if cl[-1] > 0 else None

        # Direction from SMA
        sma = float(np.mean(ac[-ma_filter_window:]))
        direction = 1 if ac[-1] > sma else -1

        # Regime from recent returns
        returns = np.diff(np.log(np.maximum(ac, 1e-9)))
        regime = _classify_regime(returns)

        # Current vol bucket using universe-wide thresholds from CPT
        avg_vol_20 = float(np.mean(vol[-21:-1])) if len(vol) >= 21 else float(np.mean(vol[:-1]) + 1e-9)
        today_rel_vol = float(vol[-1]) / avg_vol_20 if avg_vol_20 > 0 else 1.0
        vol_bucket = (
            "low"  if today_rel_vol < cpt.vol_q1 else
            "mid"  if today_rel_vol < cpt.vol_q2 else
            "high"
        )

        # CPT lookup → Beta-Binomial posterior
        posterior = _cpt_lookup(cpt, regime, vol_bucket)

        # P(5D return > 0 | regime, vol_bucket) — the primary probability output
        # p_upside == trend_prob by definition; direction is context, not a conditioning flip
        trend_prob = posterior.mean
        p_upside   = trend_prob
        p_downside = 1.0 - trend_prob

        # Composite score for cross-sectional ranking (descriptive, NOT a test stat)
        raw_score = sum(mom[w] * direction for w in momentum_windows) / len(momentum_windows)

        pending.append({
            "symbol": sym,
            "raw_score": raw_score,
            "trend_prob": trend_prob,
            "p_upside": p_upside,
            "p_downside": p_downside,
            "atr_val": atr_val,
            "daily_risk_usd": daily_risk_usd,
            "expected_move_pct": expected_move_pct,
            "regime": regime,
            "vol_bucket": vol_bucket,
            "today_rel_vol": today_rel_vol,
            "direction": direction,
            "mom": mom,
            "momentum_accel": momentum_accel,
            "posterior": posterior,
        })

    if not pending:
        return pl.DataFrame()

    # Cross-sectional z-score for ranking only
    raw_scores = np.array([p["raw_score"] for p in pending])
    mean_s = float(np.mean(raw_scores))
    std_s  = float(np.std(raw_scores) + 1e-9)

    results = []
    for p in pending:
        z_descriptive = (p["raw_score"] - mean_s) / std_s
        posterior: BetaPosterior = p["posterior"]
        bucket = strength_bucket(posterior)

        details = {
            **{f"mom_{w}d": round(float(p["mom"][w]) * 100, 2) for w in momentum_windows},
            "direction": p["direction"],
            "sma_above": p["direction"] == 1,
            "regime": p["regime"],
            "vol_bucket": p["vol_bucket"],
            "rel_vol": round(p["today_rel_vol"], 3),
            "cpt_n_obs": posterior.observations,
            "cpt_hits": posterior.hits,
            "ci_low": round(posterior.ci_low, 4),
            "ci_high": round(posterior.ci_high, 4),
            "z_descriptive": round(z_descriptive, 4),
            "z_note": "cross-sectional rank only, not a p-value",
            "momentum_accel": round(p["momentum_accel"], 3) if p["momentum_accel"] is not None else None,
        }

        results.append({
            "symbol": p["symbol"],
            "date": as_of,
            "module_name": "momentum_risk",
            "trend_prob": round(p["trend_prob"], 4),
            "p_upside": round(p["p_upside"], 4),
            "p_downside": round(p["p_downside"], 4),
            "daily_risk_usd": round(p["daily_risk_usd"], 2),
            "expected_move_pct": p["expected_move_pct"],
            "z_score": round(z_descriptive, 4),
            "p_value_raw": None,    # cross-sectional z is not a valid p-value
            "p_value_bonf": None,
            "strength_bucket": bucket,
            "regime": p["regime"],
            "details": json.dumps(details),
        })

    return pl.DataFrame(results).with_columns(pl.col("date").cast(pl.Date))


def store_analysis(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
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
