"""
Granger causality: F-test on restricted vs unrestricted VAR.
BIC for lag selection (not AIC -- AIC overfits at T=250).

Uses log returns (already stationary). BH FDR control across all tested pairs.
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import structlog
from statsmodels.tsa.stattools import grangercausalitytests

log = structlog.get_logger()


def _bh_fdr(pvalues: list[float], q: float = 0.05) -> list[bool]:
    """
    Benjamini-Hochberg FDR control.

    Returns boolean list where True = FDR-significant at level q.
    """
    m = len(pvalues)
    if m == 0:
        return []

    indexed = sorted(enumerate(pvalues), key=lambda x: x[1])
    significant = [False] * m

    max_k = -1
    for rank_minus_1, (orig_idx, pval) in enumerate(indexed):
        k = rank_minus_1 + 1
        threshold = k / m * q
        if pval <= threshold:
            max_k = rank_minus_1

    if max_k >= 0:
        for rank_minus_1 in range(max_k + 1):
            orig_idx = indexed[rank_minus_1][0]
            significant[orig_idx] = True

    return significant


def _get_sector_groups(con: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    """Get symbols grouped by sector."""
    sector_groups: dict[str, list[str]] = {}

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


def _load_log_returns(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    lookback: int = 250,
) -> dict[str, np.ndarray]:
    """Load log returns for each symbol. Returns {symbol: log_returns_array}."""
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
        if len(prices) > lookback + 1:
            prices = prices[-(lookback + 1):]
        if len(prices) < 60:
            continue
        log_returns = np.diff(np.log(np.maximum(prices, 1e-9)))
        result[sym] = log_returns

    return result


def _select_lag_by_bic(
    test_results: dict,
    max_lag: int,
) -> tuple[int, float, float]:
    """
    Select best lag by BIC from grangercausalitytests output.

    Returns (best_lag, f_stat, p_value) at the BIC-optimal lag.
    Uses the 'ssr_ftest' result from each lag.
    """
    best_lag = 1
    best_bic = float("inf")
    best_f = 0.0
    best_p = 1.0

    for lag in range(1, max_lag + 1):
        if lag not in test_results:
            continue

        lag_result = test_results[lag]
        # lag_result is a tuple: (test_dict, [restricted_ols, unrestricted_ols])
        tests, ols_objects = lag_result

        # Extract F-test result
        f_test = tests.get("ssr_ftest")
        if f_test is None:
            continue
        f_stat, p_value, df_denom, df_num = f_test

        # Compute BIC-like criterion from the unrestricted model
        # BIC = T * ln(RSS/T) + k * ln(T)
        try:
            unrestricted = ols_objects[1]
            T = unrestricted.nobs
            rss = unrestricted.ssr
            k = unrestricted.df_model + 1  # +1 for intercept
            bic = T * np.log(rss / T) + k * np.log(T)
        except Exception:
            # Fallback: use F-test p-value as proxy
            bic = p_value

        if bic < best_bic:
            best_bic = bic
            best_lag = lag
            best_f = float(f_stat)
            best_p = float(p_value)

    return best_lag, best_f, best_p


def find_granger_leaders(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    lookback: int = 250,
    max_lag: int = 5,
    fdr_q: float = 0.05,
) -> list[dict]:
    """
    Test Granger causality within sectors.

    For each sector pair (A, B):
    1. Get log returns (already stationary)
    2. Run grangercausalitytests(data, maxlag=max_lag)
    3. Select best lag by BIC
    4. Record F-statistic and p-value at best lag
    5. Apply BH FDR across all tests

    Returns list of significant leader-follower relationships.
    """
    sector_groups = _get_sector_groups(con)
    if not sector_groups:
        log.warning("granger_no_sector_data")
        return []

    symbol_set = set(symbols)
    all_sectored_syms = sorted(
        {s for syms in sector_groups.values() for s in syms if s in symbol_set}
    )
    if not all_sectored_syms:
        log.warning("granger_no_sectored_symbols")
        return []

    log_returns = _load_log_returns(con, all_sectored_syms, as_of, lookback)
    if not log_returns:
        log.warning("granger_no_return_data")
        return []

    # Test all within-sector directed pairs
    pair_tests: list[dict] = []

    for sector, sector_syms in sector_groups.items():
        available = [s for s in sector_syms if s in log_returns]
        if len(available) < 2:
            continue

        # Limit sector size to top 30 (by data length) to control runtime
        available = sorted(available, key=lambda s: len(log_returns[s]), reverse=True)[:30]

        for i in range(len(available)):
            for j in range(len(available)):
                if i == j:
                    continue

                leader, follower = available[i], available[j]
                r_leader = log_returns[leader]
                r_follower = log_returns[follower]

                # Align lengths
                T = min(len(r_leader), len(r_follower))
                if T < max_lag + 20:
                    continue
                r_l = r_leader[-T:]
                r_f = r_follower[-T:]

                # grangercausalitytests expects [y, x] where we test if x Granger-causes y
                # So if leader causes follower: [follower, leader]
                data = np.column_stack([r_f, r_l])

                try:
                    # verbose=False to suppress printed output
                    results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
                except Exception:
                    continue

                best_lag, f_stat, p_value = _select_lag_by_bic(results, max_lag)

                if np.isnan(p_value) or np.isinf(f_stat):
                    continue

                pair_tests.append({
                    "leader": leader,
                    "follower": follower,
                    "sector": sector,
                    "lag_days": best_lag,
                    "f_statistic": f_stat,
                    "p_value": p_value,
                })

    if not pair_tests:
        log.info("granger_no_pairs_tested")
        return []

    # BH FDR control
    pvalues = [pt["p_value"] for pt in pair_tests]
    fdr_flags = _bh_fdr(pvalues, q=fdr_q)

    results: list[dict] = []
    n_significant = 0

    for pt, is_sig in zip(pair_tests, fdr_flags):
        if not is_sig:
            continue

        n_significant += 1
        results.append({
            "leader": pt["leader"],
            "follower": pt["follower"],
            "lag_days": pt["lag_days"],
            "f_statistic": round(pt["f_statistic"], 4),
            "p_value": round(pt["p_value"], 6),
            "fdr_significant": True,
            "sector": pt["sector"],
        })

    log.info(
        "granger_complete",
        pairs_tested=len(pair_tests),
        fdr_significant=n_significant,
        sectors=len(sector_groups),
    )

    return results


def store_granger_pairs(
    con: duckdb.DuckDBPyConnection,
    pairs: list[dict],
    as_of: date,
) -> int:
    """Store Granger causality pairs in DuckDB."""
    if not pairs:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")

    # Delete existing results for this date
    con.execute(
        "DELETE FROM granger_pairs WHERE computed_at = ?",
        [as_of_str],
    )

    for p in pairs:
        con.execute("""
            INSERT INTO granger_pairs
                (leader, follower, lag_days, f_statistic, p_value,
                 fdr_significant, sector, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            p["leader"], p["follower"], p["lag_days"],
            p["f_statistic"], p["p_value"],
            p.get("fdr_significant", True),
            p.get("sector"),
            as_of_str,
        ])

    con.commit()
    log.info("granger_pairs_stored", n=len(pairs))
    return len(pairs)
