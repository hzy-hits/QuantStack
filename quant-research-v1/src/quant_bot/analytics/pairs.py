"""
Engle-Granger cointegration: within-sector pair detection.

T=250 gives adequate statistical power. Uses BH FDR control (q=0.05)
across all tested pairs to handle multiple comparisons.

OU fit on cointegrated spread: theta = -ln(b), mu = a/(1-b), half_life = ln(2)/theta
Guard: only valid when 0 < b < 1.
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import structlog
from statsmodels.tsa.stattools import adfuller

log = structlog.get_logger()


def _bh_fdr(pvalues: list[float], q: float = 0.05) -> list[bool]:
    """
    Benjamini-Hochberg FDR control.

    Given a list of p-values, returns a boolean list of the same length
    where True means the corresponding test is FDR-significant at level q.
    """
    m = len(pvalues)
    if m == 0:
        return []

    # Sort indices by p-value
    indexed = sorted(enumerate(pvalues), key=lambda x: x[1])
    significant = [False] * m

    # Find largest k where p_(k) <= k/m * q
    max_k = -1
    for rank_minus_1, (orig_idx, pval) in enumerate(indexed):
        k = rank_minus_1 + 1
        threshold = k / m * q
        if pval <= threshold:
            max_k = rank_minus_1

    # All tests with rank <= max_k are significant
    if max_k >= 0:
        for rank_minus_1 in range(max_k + 1):
            orig_idx = indexed[rank_minus_1][0]
            significant[orig_idx] = True

    return significant


def _get_sector_groups(con: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    """Get symbol-to-sector mapping, grouped by sector."""
    sector_groups: dict[str, list[str]] = {}

    # Try company_profile first (Phase 1 fundamentals)
    try:
        rows = con.execute("""
            SELECT DISTINCT symbol, sector
            FROM company_profile
            WHERE sector IS NOT NULL AND sector != ''
        """).fetchall()
        for sym, sector in rows:
            sector_groups.setdefault(sector, []).append(sym)
        if sector_groups:
            return sector_groups
    except Exception:
        pass

    # Fall back to universe_constituents
    try:
        rows = con.execute("""
            SELECT symbol, sector
            FROM universe_constituents
            WHERE sector IS NOT NULL AND sector != ''
        """).fetchall()
        for sym, sector in rows:
            sector_groups.setdefault(sector, []).append(sym)
    except Exception:
        pass

    return sector_groups


def _load_log_prices(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    lookback: int = 250,
) -> dict[str, np.ndarray]:
    """Load log prices for each symbol. Returns {symbol: log_price_array}."""
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
        return {}

    result: dict[str, np.ndarray] = {}
    for sym, grp in rows.groupby("symbol", sort=False):
        prices = grp.sort_values("date")["adj_close"].values.astype(float)
        # Take last `lookback` observations
        if len(prices) > lookback:
            prices = prices[-lookback:]
        if len(prices) < 60:
            continue
        log_prices = np.log(np.maximum(prices, 1e-9))
        result[sym] = log_prices

    return result


def _fit_ou(spread: np.ndarray) -> dict | None:
    """
    Fit OU process via AR(1) on the spread.

    spread_t = a + b * spread_{t-1} + eps
    theta = -ln(b), half_life = ln(2)/theta
    Guard: only valid when 0 < b < 1.
    """
    if len(spread) < 20:
        return None

    y = spread[1:]
    x = spread[:-1]

    # OLS: y = a + b*x
    X = np.column_stack([np.ones(len(x)), x])
    try:
        coeffs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return None

    a, b = coeffs[0], coeffs[1]

    # Guard: OU is only valid when 0 < b < 1
    if b <= 0 or b >= 1:
        return None

    theta = -np.log(b)
    half_life = np.log(2) / theta
    mu = a / (1 - b)

    # Current spread z-score
    residuals = y - (a + b * x)
    spread_std = float(np.std(residuals))
    if spread_std < 1e-10:
        return None

    current_z = float((spread[-1] - mu) / spread_std)

    return {
        "ou_theta": round(float(theta), 6),
        "ou_mu": round(float(mu), 6),
        "half_life_days": round(float(half_life), 2),
        "spread_zscore": round(current_z, 4),
        "ar1_b": round(float(b), 6),
    }


def find_cointegrated_pairs(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    lookback: int = 250,
    fdr_q: float = 0.05,
) -> list[dict]:
    """
    Find cointegrated pairs within sectors using Engle-Granger two-step.

    1. Group symbols by sector
    2. For each within-sector pair:
       a. OLS: y = alpha + beta*x + eps (log prices, T=250)
       b. ADF on residuals
    3. BH FDR control across all pairs
    4. For FDR-significant pairs, fit OU parameters

    Returns list of pair dicts with metadata.
    """
    sector_groups = _get_sector_groups(con)
    if not sector_groups:
        log.warning("cointegration_no_sector_data")
        return []

    # Filter to symbols in our universe
    symbol_set = set(symbols)

    # Load log prices for all equity symbols that have sector info
    all_sectored_syms = sorted(
        {s for syms in sector_groups.values() for s in syms if s in symbol_set}
    )
    if not all_sectored_syms:
        log.warning("cointegration_no_sectored_symbols")
        return []

    log_prices = _load_log_prices(con, all_sectored_syms, as_of, lookback)
    if not log_prices:
        log.warning("cointegration_no_price_data")
        return []

    # Generate all within-sector pairs and run Engle-Granger
    pair_tests: list[dict] = []  # accumulate before FDR

    for sector, sector_syms in sector_groups.items():
        # Filter to symbols with price data
        available = [s for s in sector_syms if s in log_prices]
        if len(available) < 2:
            continue

        # Limit sector size to avoid combinatorial explosion
        # Sort by available data length and take top 40
        available = sorted(available, key=lambda s: len(log_prices[s]), reverse=True)[:40]

        for i in range(len(available)):
            for j in range(i + 1, len(available)):
                sym_a, sym_b = available[i], available[j]
                y = log_prices[sym_a]
                x = log_prices[sym_b]

                # Align to same length (both already trimmed to lookback)
                T = min(len(y), len(x))
                if T < 60:
                    continue
                y = y[-T:]
                x = x[-T:]

                # Step 1: OLS regression y = alpha + beta * x
                X_mat = np.column_stack([np.ones(T), x])
                try:
                    coeffs, _, _, _ = np.linalg.lstsq(X_mat, y, rcond=None)
                except np.linalg.LinAlgError:
                    continue

                beta = coeffs[1]
                residuals = y - (coeffs[0] + beta * x)

                # Step 2: ADF test on residuals
                try:
                    adf_stat, adf_pvalue, _, _, _, _ = adfuller(
                        residuals, maxlag=int(np.ceil(T ** (1/3))), autolag="AIC"
                    )
                except Exception:
                    continue

                if np.isnan(adf_pvalue):
                    continue

                pair_tests.append({
                    "symbol_a": sym_a,
                    "symbol_b": sym_b,
                    "sector": sector,
                    "beta": float(beta),
                    "adf_pvalue": float(adf_pvalue),
                    "residuals": residuals,
                    "T": T,
                })

    if not pair_tests:
        log.info("cointegration_no_pairs_tested")
        return []

    # BH FDR control
    pvalues = [pt["adf_pvalue"] for pt in pair_tests]
    fdr_flags = _bh_fdr(pvalues, q=fdr_q)

    results: list[dict] = []
    n_significant = 0

    for pt, is_sig in zip(pair_tests, fdr_flags):
        if not is_sig:
            continue

        n_significant += 1

        # Fit OU parameters on the spread
        ou = _fit_ou(pt["residuals"])

        result = {
            "symbol_a": pt["symbol_a"],
            "symbol_b": pt["symbol_b"],
            "sector": pt["sector"],
            "beta": round(pt["beta"], 4),
            "adf_pvalue": round(pt["adf_pvalue"], 6),
            "fdr_significant": True,
        }

        if ou is not None:
            result.update({
                "ou_theta": ou["ou_theta"],
                "ou_mu": ou["ou_mu"],
                "half_life_days": ou["half_life_days"],
                "spread_zscore": ou["spread_zscore"],
            })
        else:
            result.update({
                "ou_theta": None,
                "ou_mu": None,
                "half_life_days": None,
                "spread_zscore": None,
            })

        results.append(result)

    log.info(
        "cointegration_complete",
        pairs_tested=len(pair_tests),
        fdr_significant=n_significant,
        sectors=len(sector_groups),
    )

    return results


def store_cointegrated_pairs(
    con: duckdb.DuckDBPyConnection,
    pairs: list[dict],
    as_of: date,
) -> int:
    """Store cointegrated pairs in DuckDB."""
    if not pairs:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")

    # Delete existing results for this date
    con.execute(
        "DELETE FROM cointegrated_pairs WHERE computed_at = ?",
        [as_of_str],
    )

    for p in pairs:
        con.execute("""
            INSERT INTO cointegrated_pairs
                (symbol_a, symbol_b, sector, beta, adf_pvalue,
                 ou_theta, ou_mu, half_life_days, spread_zscore,
                 fdr_significant, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            p["symbol_a"], p["symbol_b"], p["sector"],
            p["beta"], p["adf_pvalue"],
            p.get("ou_theta"), p.get("ou_mu"),
            p.get("half_life_days"), p.get("spread_zscore"),
            p.get("fdr_significant", True),
            as_of_str,
        ])

    con.commit()
    log.info("cointegrated_pairs_stored", n=len(pairs))
    return len(pairs)
