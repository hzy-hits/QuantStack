"""
Hidden Markov Model for market-level regime detection.

2-state Gaussian HMM fitted on a market feature set led by:
  - SPY daily log return
  - VIX level
  - S&P 500 breadth (daily advancer share)
  - HY credit spread (BAMLH0A0HYM2)
  - realized vol term structure (5d / 20d SPY realized vol)

Baum-Welch estimation via hmmlearn.

This is a MARKET-LEVEL overlay -- it runs parallel to the existing
per-symbol autocorrelation regime in momentum_risk.py.
It is NOT a drop-in replacement.

Latent states are unlabeled -- need mapping layer:
  - State with higher mean return = "bull"
  - State with lower mean return = "bear"
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger()


def fit_hmm_regime(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    lookback: int = 500,
) -> dict | None:
    """
    Fit a 2-state Gaussian HMM on a broader market feature set.

    Returns dict with regime label, probabilities, transition matrix,
    state means, 1-step-ahead forecast, and P(ret>0|state) estimates,
    or None if data insufficient or hmmlearn unavailable.
    """
    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError:
        log.warning("hmmlearn_not_installed", hint="pip install hmmlearn")
        return None

    as_of_str = as_of.strftime("%Y-%m-%d")

    history_buffer = 40

    # ── Load SPY adj_close ────────────────────────────────────────────────
    spy_df = con.execute("""
        SELECT date, adj_close
        FROM prices_daily
        WHERE symbol = 'SPY' AND date <= ?
        ORDER BY date DESC
        LIMIT ?
    """, [as_of_str, lookback + history_buffer + 1]).fetchdf()

    if spy_df.empty or len(spy_df) < 60:
        log.warning("hmm_insufficient_spy_data", rows=len(spy_df))
        return None

    spy_df = spy_df.sort_values("date").reset_index(drop=True)
    spy_df["date"] = pd.to_datetime(spy_df["date"])
    spy_df["adj_close"] = spy_df["adj_close"].astype(float)
    spy_df["log_return"] = np.log(np.maximum(spy_df["adj_close"], 1e-9)).diff()
    spy_df["rv_5d"] = spy_df["log_return"].rolling(5).std() * np.sqrt(252.0)
    spy_df["rv_20d"] = spy_df["log_return"].rolling(20).std() * np.sqrt(252.0)
    spy_df["rv_term_ratio"] = spy_df["rv_5d"] / spy_df["rv_20d"].replace(0.0, np.nan)

    spy_features = spy_df[["date", "log_return", "rv_term_ratio"]].copy()
    spy_features = spy_features.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
    if len(spy_features) < 60:
        log.warning("hmm_insufficient_spy_features", rows=len(spy_features))
        return None

    start_date = spy_features["date"].iloc[-lookback] if len(spy_features) > lookback else spy_features["date"].iloc[0]
    spy_features = spy_features[spy_features["date"] >= start_date].reset_index(drop=True)

    # ── Load VIX levels (^VIX from prices_daily, close column) ────────────
    vix_df = con.execute("""
        SELECT date, close
        FROM prices_daily
        WHERE symbol = '^VIX' AND date <= ?
        ORDER BY date DESC
        LIMIT ?
    """, [as_of_str, lookback + history_buffer + 1]).fetchdf()

    if vix_df.empty or len(vix_df) < 60:
        log.warning("hmm_insufficient_vix_data", rows=len(vix_df))
        return None

    vix_df = vix_df.sort_values("date").reset_index(drop=True)
    vix_df["date"] = pd.to_datetime(vix_df["date"])
    vix_df["vix_level"] = vix_df["close"].astype(float)
    vix_df = vix_df[["date", "vix_level"]]

    # ── Load breadth: S&P 500 daily advancer share ────────────────────────
    breadth_df = con.execute("""
        WITH sp AS (
            SELECT symbol
            FROM universe_constituents
            WHERE index_name = 'sp500'
        ),
        hist AS (
            SELECT p.symbol, p.date, p.adj_close,
                   LAG(p.adj_close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS prev_close
            FROM prices_daily p
            INNER JOIN sp ON p.symbol = sp.symbol
            WHERE p.date <= ?
        )
        SELECT date,
               AVG(CASE WHEN prev_close > 0 AND adj_close > prev_close THEN 1.0 ELSE 0.0 END) AS breadth_adv_share
        FROM hist
        WHERE prev_close IS NOT NULL
        GROUP BY date
        ORDER BY date
    """, [as_of_str]).fetchdf()
    if breadth_df.empty or len(breadth_df) < 60:
        log.warning("hmm_insufficient_breadth_data", rows=len(breadth_df))
        return None
    breadth_df["date"] = pd.to_datetime(breadth_df["date"])

    # ── Load HY credit spread (daily FRED) ────────────────────────────────
    hy_df = con.execute("""
        SELECT date, value
        FROM macro_daily
        WHERE series_id = 'BAMLH0A0HYM2' AND date <= ?
        ORDER BY date
    """, [as_of_str]).fetchdf()
    if hy_df.empty or len(hy_df) < 60:
        log.warning("hmm_insufficient_hy_data", rows=len(hy_df))
        return None
    hy_df["date"] = pd.to_datetime(hy_df["date"])
    hy_df["hy_spread"] = hy_df["value"].astype(float)
    hy_df = hy_df[["date", "hy_spread"]]

    # ── Align all features on SPY feature dates ───────────────────────────
    features_df = spy_features.merge(vix_df, on="date", how="inner")
    features_df = features_df.merge(breadth_df, on="date", how="inner")
    features_df = pd.merge_asof(
        features_df.sort_values("date"),
        hy_df.sort_values("date"),
        on="date",
        direction="backward",
        tolerance=pd.Timedelta(days=5),
    )
    features_df = features_df.dropna().reset_index(drop=True)

    if len(features_df) < 60:
        log.warning("hmm_insufficient_aligned_data", n=len(features_df))
        return None

    returns_arr = features_df["log_return"].to_numpy(dtype=float)
    vix_arr = features_df["vix_level"].to_numpy(dtype=float)
    breadth_arr = features_df["breadth_adv_share"].to_numpy(dtype=float)
    hy_arr = features_df["hy_spread"].to_numpy(dtype=float)
    rv_term_arr = features_df["rv_term_ratio"].to_numpy(dtype=float)

    raw_feature_map = {
        "spy_log_return": returns_arr,
        "vix_level": vix_arr,
        "breadth_adv_share": breadth_arr,
        "hy_spread": hy_arr,
        "rv_term_ratio": rv_term_arr,
    }
    stats: dict[str, tuple[float, float]] = {}
    standardized = []
    for name, arr in raw_feature_map.items():
        mean = float(arr.mean())
        std = float(arr.std()) + 1e-9
        stats[name] = (mean, std)
        standardized.append((arr - mean) / std)
    features = np.column_stack(standardized)

    # ── Fit 2-state GaussianHMM ──────────────────────────────────────────
    model = GaussianHMM(
        n_components=2,
        covariance_type="diag",
        n_iter=100,
        random_state=42,
    )

    try:
        model.fit(features)
    except Exception as e:
        log.warning("hmm_fit_failed", error=str(e))
        return None

    converged = model.monitor_.converged

    # ── Predict states and extract posteriors ─────────────────────────────
    states = model.predict(features)
    posteriors = model.predict_proba(features)

    # ── Map states to bull/bear ───────────────────────────────────────────
    # Un-standardize the means to get interpretable values
    state_ret_means = model.means_[:, 0] * stats["spy_log_return"][1] + stats["spy_log_return"][0]
    state_vix_means = model.means_[:, 1] * stats["vix_level"][1] + stats["vix_level"][0]
    state_breadth_means = (
        model.means_[:, 2] * stats["breadth_adv_share"][1] + stats["breadth_adv_share"][0]
    )
    state_hy_means = model.means_[:, 3] * stats["hy_spread"][1] + stats["hy_spread"][0]
    state_rv_term_means = (
        model.means_[:, 4] * stats["rv_term_ratio"][1] + stats["rv_term_ratio"][0]
    )

    # Bull state = higher mean return
    if state_ret_means[0] >= state_ret_means[1]:
        bull_idx, bear_idx = 0, 1
    else:
        bull_idx, bear_idx = 1, 0

    current_state = states[-1]
    regime = "bull" if current_state == bull_idx else "bear"
    p_bull = float(posteriors[-1, bull_idx])
    p_bear = float(posteriors[-1, bear_idx])

    # ── Transition matrix (reordered: bull first, bear second) ────────────
    raw_trans = model.transmat_
    trans = np.array([
        [raw_trans[bull_idx, bull_idx], raw_trans[bull_idx, bear_idx]],
        [raw_trans[bear_idx, bull_idx], raw_trans[bear_idx, bear_idx]],
    ])

    # ── 1-step-ahead forecast ─────────────────────────────────────────────
    # P(state tomorrow) = π_today · A
    pi_today = np.array([p_bull, p_bear])
    pi_tomorrow = pi_today @ trans  # [P(bull tomorrow), P(bear tomorrow)]
    p_bull_tomorrow = float(pi_tomorrow[0])

    # ── P(ret > 0 | state) from training data ────────────────────────────
    # Empirical fraction of positive-return days in each state
    bull_mask = states == bull_idx
    bear_mask = states == bear_idx
    n_bull = int(bull_mask.sum())
    n_bear = int(bear_mask.sum())

    # returns_arr is aligned with states
    p_ret_pos_given_bull = float(
        (returns_arr[bull_mask] > 0).sum() / n_bull
    ) if n_bull > 0 else 0.5
    p_ret_pos_given_bear = float(
        (returns_arr[bear_mask] > 0).sum() / n_bear
    ) if n_bear > 0 else 0.5

    # Full conditional forecast:
    # P(ret>0 tomorrow) = P(bull tomorrow) * P(ret>0|bull) + P(bear tomorrow) * P(ret>0|bear)
    p_ret_positive_tomorrow = (
        pi_tomorrow[0] * p_ret_pos_given_bull
        + pi_tomorrow[1] * p_ret_pos_given_bear
    )
    p_ret_positive_tomorrow = float(p_ret_positive_tomorrow)

    # ── Days in current regime (consecutive) ──────────────────────────────
    days_in_current = 0
    for s in reversed(states):
        if s == current_state:
            days_in_current += 1
        else:
            break

    # ── State means (un-standardized, interpretable) ──────────────────────
    state_means = {
        "bull": {
            "ret_mean_pct": round(float(state_ret_means[bull_idx]) * 100, 4),
            "vix_mean": round(float(state_vix_means[bull_idx]), 1),
            "breadth_mean_pct": round(float(state_breadth_means[bull_idx]) * 100, 1),
            "hy_spread_mean": round(float(state_hy_means[bull_idx]), 2),
            "rv_term_ratio": round(float(state_rv_term_means[bull_idx]), 3),
        },
        "bear": {
            "ret_mean_pct": round(float(state_ret_means[bear_idx]) * 100, 4),
            "vix_mean": round(float(state_vix_means[bear_idx]), 1),
            "breadth_mean_pct": round(float(state_breadth_means[bear_idx]) * 100, 1),
            "hy_spread_mean": round(float(state_hy_means[bear_idx]), 2),
            "rv_term_ratio": round(float(state_rv_term_means[bear_idx]), 3),
        },
    }

    log.info(
        "hmm_regime_fitted",
        regime=regime,
        p_bull=round(p_bull, 3),
        p_bull_tomorrow=round(p_bull_tomorrow, 3),
        p_ret_pos_tomorrow=round(p_ret_positive_tomorrow, 3),
        days_in_current=days_in_current,
        converged=converged,
        n_obs=len(features),
        feature_set=["SPY", "VIX", "breadth", "HY", "rv_term"],
    )

    return {
        "regime": regime,
        "p_bull": round(p_bull, 4),
        "p_bear": round(p_bear, 4),
        "p_bull_tomorrow": round(p_bull_tomorrow, 4),
        "p_ret_positive_tomorrow": round(p_ret_positive_tomorrow, 4),
        "p_ret_pos_given_bull": round(p_ret_pos_given_bull, 3),
        "p_ret_pos_given_bear": round(p_ret_pos_given_bear, 3),
        "state_means": state_means,
        "transition_matrix": [
            [round(trans[0, 0], 4), round(trans[0, 1], 4)],
            [round(trans[1, 0], 4), round(trans[1, 1], 4)],
        ],
        "days_in_current_regime": days_in_current,
        "model_converged": converged,
        "n_observations": len(features),
        "n_bull_days": n_bull,
        "n_bear_days": n_bear,
        "feature_set": [
            "spy_log_return",
            "vix_level",
            "breadth_adv_share",
            "hy_spread",
            "rv_term_ratio",
        ],
    }


# ── Forecast recording & resolution ──────────────────────────────────────────


def record_hmm_forecast(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    p_ret_positive_tomorrow: float,
) -> None:
    """Record an HMM forecast for next-day SPY return direction."""
    forecast_id = f"SPY_hmm_p_ret_pos_{as_of.isoformat()}_1d"
    as_of_str = as_of.strftime("%Y-%m-%d")

    # Find next trading day for resolution
    next_td = _next_trading_day(con, as_of)

    con.execute("""
        INSERT OR REPLACE INTO forecast_outcomes
            (forecast_id, symbol, module_name, forecast_date,
             horizon_days, resolution_date, p_forecast, outcome, brier_contrib)
        VALUES (?, 'SPY', 'hmm_regime', ?, 1, ?, ?, NULL, NULL)
    """, [forecast_id, as_of_str, next_td.strftime("%Y-%m-%d"),
          round(p_ret_positive_tomorrow, 6)])

    log.info("hmm_forecast_recorded", forecast_id=forecast_id, p=round(p_ret_positive_tomorrow, 4))


def resolve_hmm_forecasts(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> int:
    """Resolve past HMM forecasts whose resolution_date has arrived."""
    as_of_str = as_of.strftime("%Y-%m-%d")

    unresolved = con.execute("""
        SELECT forecast_id, forecast_date, resolution_date, p_forecast
        FROM forecast_outcomes
        WHERE module_name = 'hmm_regime'
          AND outcome IS NULL
          AND resolution_date <= ?
    """, [as_of_str]).fetchall()

    resolved_count = 0
    for fid, forecast_date, resolution_date, p_forecast in unresolved:
        f_date_str = str(forecast_date)
        r_date_str = str(resolution_date)

        # Get SPY return from forecast_date to the first trading day
        # on or after resolution_date (handles holidays gracefully)
        ret_row = con.execute("""
            SELECT p2.adj_close / p1.adj_close - 1.0 AS ret
            FROM prices_daily p1
            INNER JOIN (
                SELECT adj_close FROM prices_daily
                WHERE symbol = 'SPY' AND date >= ?
                ORDER BY date LIMIT 1
            ) p2 ON TRUE
            WHERE p1.symbol = 'SPY' AND p1.date = ?
              AND p1.adj_close > 0 AND p2.adj_close > 0
        """, [r_date_str, f_date_str]).fetchone()

        if ret_row is None or ret_row[0] is None:
            continue

        outcome = 1 if ret_row[0] > 0 else 0
        brier = (p_forecast - outcome) ** 2

        con.execute("""
            UPDATE forecast_outcomes
            SET outcome = ?, brier_contrib = ?
            WHERE forecast_id = ?
        """, [outcome, round(brier, 6), fid])
        resolved_count += 1

    if resolved_count > 0:
        log.info("hmm_forecasts_resolved", count=resolved_count)
    return resolved_count


def compute_hmm_calibration(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    lookback_days: int = 90,
) -> dict:
    """Compute Brier score and hit rate for resolved HMM forecasts."""
    cutoff = as_of - timedelta(days=lookback_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    rows = con.execute("""
        SELECT p_forecast, outcome
        FROM forecast_outcomes
        WHERE module_name = 'hmm_regime'
          AND outcome IS NOT NULL
          AND forecast_date >= ?
    """, [cutoff_str]).fetchall()

    if not rows:
        return {"n": 0, "brier_score": None, "hit_rate": None, "base_rate": None}

    n = len(rows)
    brier_total = 0.0
    hits = 0
    pos_outcomes = 0

    for p_forecast, outcome in rows:
        brier_total += (p_forecast - outcome) ** 2
        predicted_up = p_forecast > 0.5
        actual_up = outcome == 1
        if predicted_up == actual_up:
            hits += 1
        if outcome == 1:
            pos_outcomes += 1

    brier_score = brier_total / n
    hit_rate = hits / n
    base_rate = pos_outcomes / n
    # Climatological Brier: BS_clim = r * (1 - r)
    brier_clim = base_rate * (1 - base_rate) if n > 0 else None
    # Brier Skill Score: BSS = 1 - BS/BS_clim (>0 means better than climatology)
    brier_skill = (1 - brier_score / brier_clim) if brier_clim and brier_clim > 0 else None

    return {
        "n": n,
        "brier_score": round(brier_score, 4),
        "hit_rate": round(hit_rate, 4),
        "base_rate": round(base_rate, 4),
        "brier_clim": round(brier_clim, 4) if brier_clim is not None else None,
        "brier_skill_score": round(brier_skill, 4) if brier_skill is not None else None,
    }


def _next_trading_day(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> date:
    """Estimate the next trading day after as_of.

    First tries the DB (works for backfill runs where future data exists).
    Falls back to simple weekday skip (works for live runs where tomorrow
    hasn't been fetched yet).  Holidays may cause off-by-one, but the
    resolution step handles that gracefully — it resolves on the first
    available trading day on or after resolution_date.
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    row = con.execute("""
        SELECT MIN(date) AS next_date
        FROM prices_daily
        WHERE symbol = 'SPY' AND date > ?
    """, [as_of_str]).fetchone()
    if row and row[0] is not None:
        d = row[0]
        return d if isinstance(d, date) else date.fromisoformat(str(d))

    # Fallback: skip weekends
    next_d = as_of + timedelta(days=1)
    while next_d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        next_d += timedelta(days=1)
    return next_d
