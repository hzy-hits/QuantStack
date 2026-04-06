"""
Kalman filter for dynamic beta estimation.

State: beta(t) = beta(t-1) + eta, eta ~ N(0, Q)
Observation: r_A(t) = beta(t) * r_market(t) + eps, eps ~ N(0, R)

Fix R from observation residual variance.
Choose Q for 20-60 day effective smoothing.
Default: Q = R / 100 gives ~30-day responsiveness.
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import structlog

log = structlog.get_logger()


def _kalman_filter_beta(
    stock_returns: np.ndarray,
    market_returns: np.ndarray,
    q_ratio: float = 0.01,
) -> dict | None:
    """
    Run scalar Kalman filter for dynamic beta.

    Args:
        stock_returns: log returns of asset
        market_returns: log returns of benchmark
        q_ratio: Q/R ratio controlling smoothing (0.01 ~ 30-day responsiveness)

    Returns dict with {beta_current, beta_60d_mean, divergence, beta_std, betas_array}
    or None if insufficient data.
    """
    T = min(len(stock_returns), len(market_returns))
    if T < 80:
        return None

    y = stock_returns[-T:]
    x = market_returns[-T:]

    # Remove NaNs
    mask = ~(np.isnan(y) | np.isnan(x))
    y, x = y[mask], x[mask]
    T = len(y)
    if T < 80:
        return None

    # Initialize beta from OLS on first 60 observations
    init_n = min(60, T // 2)
    X_init = np.column_stack([np.ones(init_n), x[:init_n]])
    try:
        coeffs, _, _, _ = np.linalg.lstsq(X_init, y[:init_n], rcond=None)
    except np.linalg.LinAlgError:
        return None

    beta = coeffs[1]

    # Estimate R from OLS residuals
    residuals = y[:init_n] - (coeffs[0] + coeffs[1] * x[:init_n])
    R = float(np.var(residuals))
    if R < 1e-15:
        R = 1e-10

    # Q = R * q_ratio for desired smoothing
    Q = R * q_ratio

    # Initial state uncertainty
    P = R  # start with moderate uncertainty

    # Run Kalman filter
    betas = np.zeros(T)
    betas[0] = beta

    for t in range(1, T):
        # Predict
        beta_pred = beta
        P_pred = P + Q

        # Update (observation: y_t = beta * x_t + eps)
        x_t = x[t]
        y_t = y[t]

        # Innovation
        innovation = y_t - beta_pred * x_t
        S = x_t * x_t * P_pred + R  # innovation variance

        if abs(S) < 1e-15:
            # Market return near zero -- skip update
            betas[t] = beta_pred
            P = P_pred
            beta = beta_pred
            continue

        # Kalman gain
        K = P_pred * x_t / S

        # Update
        beta = beta_pred + K * innovation
        P = (1 - K * x_t) * P_pred

        betas[t] = beta

    # Summary statistics
    beta_current = float(betas[-1])

    # 60-day trailing mean
    lookback_60 = min(60, T)
    beta_60d_mean = float(np.mean(betas[-lookback_60:]))

    # Divergence from 60d mean
    divergence = beta_current - beta_60d_mean

    # Standard deviation of beta over last 60 days
    beta_std = float(np.std(betas[-lookback_60:]))

    return {
        "beta_current": round(beta_current, 4),
        "beta_60d_mean": round(beta_60d_mean, 4),
        "divergence": round(divergence, 4),
        "beta_std": round(beta_std, 4),
    }


def compute_kalman_betas(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    benchmark: str = "SPY",
    lookback: int = 250,
    q_ratio: float = 0.01,
) -> dict[str, dict]:
    """
    Compute Kalman-filtered dynamic betas for all symbols vs benchmark.

    For each symbol:
    1. Get log returns for symbol and benchmark
    2. Initialize beta from OLS on first 60 obs
    3. Run Kalman filter forward
    4. Record: current beta, 60D mean beta, divergence from mean

    Returns dict[symbol -> {beta_current, beta_60d_mean, divergence, beta_std}]
    """
    cutoff = as_of.strftime("%Y-%m-%d")
    start = (as_of - timedelta(days=int(lookback * 1.6))).strftime("%Y-%m-%d")

    # Load all prices
    all_syms = sorted(set(symbols + [benchmark]))
    placeholders = ", ".join(["?"] * len(all_syms))
    rows = con.execute(f"""
        SELECT symbol, date, adj_close
        FROM prices_daily
        WHERE symbol IN ({placeholders})
          AND date BETWEEN '{start}' AND '{cutoff}'
        ORDER BY symbol, date
    """, all_syms).fetchdf()

    if rows.empty:
        log.warning("kalman_no_price_data")
        return {}

    # Build per-symbol return arrays
    returns_map: dict[str, np.ndarray] = {}
    for sym, grp in rows.groupby("symbol", sort=False):
        prices = grp.sort_values("date")["adj_close"].values.astype(float)
        if len(prices) > lookback + 1:
            prices = prices[-(lookback + 1):]
        if len(prices) < 80:
            continue
        log_ret = np.diff(np.log(np.maximum(prices, 1e-9)))
        returns_map[sym] = log_ret

    if benchmark not in returns_map:
        log.warning("kalman_no_benchmark_data", benchmark=benchmark)
        return {}

    market_returns = returns_map[benchmark]

    results: dict[str, dict] = {}
    n_computed = 0

    for sym in symbols:
        if sym == benchmark:
            continue

        stock_returns = returns_map.get(sym)
        if stock_returns is None:
            continue

        kalman_result = _kalman_filter_beta(
            stock_returns, market_returns, q_ratio=q_ratio
        )
        if kalman_result is not None:
            results[sym] = kalman_result
            n_computed += 1

    log.info(
        "kalman_betas_computed",
        n_computed=n_computed,
        n_symbols=len(symbols),
        q_ratio=q_ratio,
    )

    return results


def store_kalman_betas(
    con: duckdb.DuckDBPyConnection,
    results: dict[str, dict],
    as_of: date,
) -> int:
    """Store Kalman beta results in DuckDB."""
    if not results:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")

    for sym, data in results.items():
        con.execute("""
            INSERT OR REPLACE INTO kalman_betas
                (symbol, beta_current, beta_60d_mean, divergence, beta_std, computed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            sym,
            data.get("beta_current"),
            data.get("beta_60d_mean"),
            data.get("divergence"),
            data.get("beta_std"),
            as_of_str,
        ])

    con.commit()
    log.info("kalman_betas_stored", n=len(results))
    return len(results)
