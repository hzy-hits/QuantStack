"""Load price signal layer data: cointegration, Granger, earnings CAR, Kalman beta."""
from __future__ import annotations

from datetime import date

import duckdb
import structlog

log = structlog.get_logger()


def load_cointegration(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, list[dict]]:
    """
    Load FDR-significant cointegrated pairs for current date.

    Returns {symbol: [{partner, half_life, spread_z, beta, sector, adf_pvalue}]}
    Each symbol appears as both symbol_a and symbol_b entries.
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        df = con.execute("""
            SELECT symbol_a, symbol_b, sector, beta, adf_pvalue,
                   ou_theta, ou_mu, half_life_days, spread_zscore
            FROM cointegrated_pairs
            WHERE computed_at = ?
              AND fdr_significant = TRUE
        """, [as_of_str]).fetchdf()
    except Exception:
        log.debug("cointegration_table_not_found", as_of=as_of_str)
        return {}

    if df.empty:
        return {}

    result: dict[str, list[dict]] = {}
    for _, row in df.iterrows():
        entry = {
            "beta": _safe_float(row.get("beta")),
            "adf_pvalue": _safe_float(row.get("adf_pvalue")),
            "half_life_days": _safe_float(row.get("half_life_days")),
            "spread_zscore": _safe_float(row.get("spread_zscore")),
            "ou_theta": _safe_float(row.get("ou_theta")),
            "sector": row.get("sector"),
        }

        # Add for symbol_a (partner = symbol_b)
        sym_a = row["symbol_a"]
        entry_a = {**entry, "partner": row["symbol_b"]}
        result.setdefault(sym_a, []).append(entry_a)

        # Add for symbol_b (partner = symbol_a)
        sym_b = row["symbol_b"]
        entry_b = {**entry, "partner": row["symbol_a"]}
        result.setdefault(sym_b, []).append(entry_b)

    log.debug("cointegration_loaded", symbols=len(result))
    return result


def load_granger(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, list[dict]]:
    """
    Load FDR-significant Granger causality pairs.

    Returns {symbol: [{role, counterpart, lag, f_statistic, sector}]}
    where role is 'leader' or 'follower'.
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        df = con.execute("""
            SELECT leader, follower, lag_days, f_statistic, p_value, sector
            FROM granger_pairs
            WHERE computed_at = ?
              AND fdr_significant = TRUE
        """, [as_of_str]).fetchdf()
    except Exception:
        log.debug("granger_table_not_found", as_of=as_of_str)
        return {}

    if df.empty:
        return {}

    result: dict[str, list[dict]] = {}
    for _, row in df.iterrows():
        leader = row["leader"]
        follower = row["follower"]

        # Leader entry
        leader_entry = {
            "role": "leader",
            "counterpart": follower,
            "lag_days": int(row["lag_days"]) if row["lag_days"] is not None else None,
            "f_statistic": _safe_float(row.get("f_statistic")),
            "sector": row.get("sector"),
        }
        result.setdefault(leader, []).append(leader_entry)

        # Follower entry
        follower_entry = {
            "role": "follower",
            "counterpart": leader,
            "lag_days": int(row["lag_days"]) if row["lag_days"] is not None else None,
            "f_statistic": _safe_float(row.get("f_statistic")),
            "sector": row.get("sector"),
        }
        result.setdefault(follower, []).append(follower_entry)

    log.debug("granger_loaded", symbols=len(result))
    return result


def load_earnings_car(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """
    Load most recent earnings CAR for each symbol.

    Returns {symbol: {car_1d, car_3d, car_5d, car_10d, pre_event_beta, event_date}}
    Only returns the most recent event per symbol.
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        df = con.execute("""
            SELECT symbol, event_date, car_1d, car_3d, car_5d, car_10d, pre_event_beta
            FROM earnings_car
            WHERE computed_at = ?
            ORDER BY symbol, event_date DESC
        """, [as_of_str]).fetchdf()
    except Exception:
        log.debug("earnings_car_table_not_found", as_of=as_of_str)
        return {}

    if df.empty:
        return {}

    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        sym = row["symbol"]
        if sym in result:
            continue  # only keep most recent event

        result[sym] = {
            "event_date": str(row["event_date"])[:10],
            "car_1d": _safe_float(row.get("car_1d")),
            "car_3d": _safe_float(row.get("car_3d")),
            "car_5d": _safe_float(row.get("car_5d")),
            "car_10d": _safe_float(row.get("car_10d")),
            "pre_event_beta": _safe_float(row.get("pre_event_beta")),
        }

    log.debug("earnings_car_loaded", symbols=len(result))
    return result


def load_kalman_betas(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """
    Load Kalman-filtered dynamic betas for the current date.

    Returns {symbol: {beta_current, beta_60d_mean, divergence, beta_std}}
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        df = con.execute("""
            SELECT symbol, beta_current, beta_60d_mean, divergence, beta_std
            FROM kalman_betas
            WHERE computed_at = ?
        """, [as_of_str]).fetchdf()
    except Exception:
        log.debug("kalman_betas_table_not_found", as_of=as_of_str)
        return {}

    if df.empty:
        return {}

    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        sym = row["symbol"]
        result[sym] = {
            "beta_current": _safe_float(row.get("beta_current")),
            "beta_60d_mean": _safe_float(row.get("beta_60d_mean")),
            "divergence": _safe_float(row.get("divergence")),
            "beta_std": _safe_float(row.get("beta_std")),
        }

    log.debug("kalman_betas_loaded", symbols=len(result))
    return result


def _safe_float(val) -> float | None:
    """Safely convert to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        import math
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None
