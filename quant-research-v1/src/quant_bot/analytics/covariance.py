"""
Ledoit-Wolf shrinkage covariance estimation.

Uses T=250 (1 year) of daily log returns. Shrinkage is more robust
than MP hard eigenvalue truncation at high N/T ratios.
Computed in numpy (0.0004s), NOT DuckDB SQL (0.43s).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import duckdb
import numpy as np
import structlog
from sklearn.covariance import LedoitWolf

log = structlog.get_logger()


@dataclass
class CovarianceResult:
    """Container for Ledoit-Wolf covariance estimation results."""
    cov_matrix: np.ndarray          # (N, N) covariance of log returns
    corr_matrix: np.ndarray         # (N, N) correlation matrix
    symbols_aligned: list[str]      # symbols matching matrix rows/cols
    shrinkage_coef: float           # Ledoit-Wolf shrinkage intensity


def compute_covariance(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    lookback: int = 250,
    min_obs: int = 100,
) -> CovarianceResult | None:
    """
    Compute Ledoit-Wolf shrinkage covariance matrix.

    Steps:
    1. Load T=250 days of adj_close for all symbols from prices_daily
    2. Compute log returns: r_t = ln(P_t / P_{t-1})
    3. Build return matrix (T-1 x N) -- drop symbols with insufficient history
    4. Fit Ledoit-Wolf shrinkage estimator
    5. Return: covariance matrix, correlation matrix, symbol list (aligned)

    Returns CovarianceResult or None if insufficient data.
    """
    cutoff = as_of.strftime("%Y-%m-%d")
    start = (as_of - timedelta(days=int(lookback * 1.6))).strftime("%Y-%m-%d")

    placeholders = ", ".join(["?"] * len(symbols))
    rows = con.execute(f"""
        SELECT symbol, date, adj_close
        FROM prices_daily
        WHERE symbol IN ({placeholders})
          AND date BETWEEN '{start}' AND '{cutoff}'
        ORDER BY symbol, date
    """, symbols).fetchdf()

    if rows.empty:
        log.warning("covariance_no_price_data")
        return None

    # Pivot to date x symbol matrix
    pivot = rows.pivot_table(index="date", columns="symbol", values="adj_close")
    pivot = pivot.sort_index()

    # Keep only the last `lookback` trading days
    if len(pivot) > lookback:
        pivot = pivot.iloc[-lookback:]

    # Drop symbols with too many missing values
    min_required = min_obs
    valid_cols = pivot.columns[pivot.notna().sum() >= min_required]
    pivot = pivot[valid_cols]

    if pivot.shape[1] < 3:
        log.warning("covariance_too_few_symbols", n=pivot.shape[1])
        return None

    # Forward-fill small gaps then drop remaining NaNs
    pivot = pivot.ffill(limit=3)

    # Compute log returns
    prices = pivot.values
    # Guard against zero/negative prices
    prices = np.maximum(prices, 1e-9)
    log_returns = np.diff(np.log(prices), axis=0)

    # Drop rows (days) with any NaN
    mask = ~np.isnan(log_returns).any(axis=1)
    log_returns = log_returns[mask]

    if log_returns.shape[0] < min_obs:
        log.warning("covariance_insufficient_return_obs", T=log_returns.shape[0])
        return None

    symbols_aligned = list(valid_cols)
    n_symbols = len(symbols_aligned)
    T = log_returns.shape[0]

    # Fit Ledoit-Wolf
    lw = LedoitWolf()
    lw.fit(log_returns)
    cov_matrix = lw.covariance_
    shrinkage_coef = float(lw.shrinkage_)

    # Derive correlation matrix from covariance
    std_devs = np.sqrt(np.diag(cov_matrix))
    std_outer = np.outer(std_devs, std_devs)
    # Guard against division by zero
    std_outer = np.maximum(std_outer, 1e-15)
    corr_matrix = cov_matrix / std_outer
    # Clip to [-1, 1] for numerical safety
    np.clip(corr_matrix, -1.0, 1.0, out=corr_matrix)

    log.info(
        "covariance_computed",
        N=n_symbols,
        T=T,
        shrinkage=round(shrinkage_coef, 4),
    )

    return CovarianceResult(
        cov_matrix=cov_matrix,
        corr_matrix=corr_matrix,
        symbols_aligned=symbols_aligned,
        shrinkage_coef=shrinkage_coef,
    )
