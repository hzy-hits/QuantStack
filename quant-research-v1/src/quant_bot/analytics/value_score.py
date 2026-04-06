"""
Cross-sectional value and quality scoring from Finnhub fundamentals.

For each symbol, computes:
  - Valuation percentiles (PE, PS, PB) within GICS sector
  - Quality composite (ROE, FCF yield, revenue growth) within sector
  - Combined valuation_score (0 = cheapest in sector, 1 = most expensive)
"""
from __future__ import annotations

from datetime import date

import duckdb
import numpy as np
import structlog

log = structlog.get_logger()


def _percentile_rank(values: list[float | None], target: float | None, lower_is_better: bool = True) -> float | None:
    """
    Compute percentile rank of target within values.

    If lower_is_better=True, rank 0.0 means cheapest (lowest value).
    Returns None if target is None or insufficient data.
    """
    if target is None:
        return None
    clean = [v for v in values if v is not None and np.isfinite(v)]
    if len(clean) < 3:
        return None
    rank = sum(1 for v in clean if v <= target) / len(clean)
    return round(rank, 4)


def _normalize_01(values: list[float | None], target: float | None) -> float | None:
    """Normalize target to [0, 1] within values. Higher = better."""
    if target is None:
        return None
    clean = [v for v in values if v is not None and np.isfinite(v)]
    if len(clean) < 3:
        return None
    lo, hi = min(clean), max(clean)
    if hi == lo:
        return 0.5
    return round((target - lo) / (hi - lo), 4)


def compute_value_scores(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> dict[str, dict]:
    """
    Compute cross-sectional value and quality scores for symbols.

    Reads from company_profile table. Percentile ranks are computed
    within each GICS sector (using the Finnhub sector field).

    Returns:
        {symbol: {
            pe_pct, ps_pct, pb_pct,      # 0=cheapest in sector
            quality_score,                 # 0-1, higher=better
            valuation_score,              # mean(pe_pct, ps_pct, pb_pct), 0=cheapest
            sector,
            sector_peers_count,
        }}
    """
    if not symbols:
        return {}

    # Load latest fundamentals for requested symbols
    placeholders = ", ".join(["?"] * len(symbols))
    try:
        rows = con.execute(f"""
            SELECT symbol, sector, pe_ttm, ps_ratio, pb_ratio,
                   roe, fcf_yield, revenue_growth
            FROM company_profile
            WHERE symbol IN ({placeholders})
              AND as_of = (
                  SELECT MAX(as_of) FROM company_profile cp2
                  WHERE cp2.symbol = company_profile.symbol AND cp2.as_of <= ?
              )
        """, symbols + [as_of]).fetchall()
    except duckdb.CatalogException:
        log.warning("value_score_no_profile_table")
        return {}

    if not rows:
        log.info("value_score_no_data", symbols=len(symbols))
        return {}

    # Build per-symbol data and group by sector
    sym_data = {}
    sector_groups: dict[str, list[str]] = {}
    for row in rows:
        sym, sector = row[0], row[1] or "Unknown"
        sym_data[sym] = {
            "sector": sector,
            "pe_ttm": row[2],
            "ps_ratio": row[3],
            "pb_ratio": row[4],
            "roe": row[5],
            "fcf_yield": row[6],
            "revenue_growth": row[7],
        }
        sector_groups.setdefault(sector, []).append(sym)

    # Compute percentile ranks within each sector
    results = {}
    for sector, sector_syms in sector_groups.items():
        peer_data = [sym_data[s] for s in sector_syms]
        pe_vals = [d["pe_ttm"] for d in peer_data]
        ps_vals = [d["ps_ratio"] for d in peer_data]
        pb_vals = [d["pb_ratio"] for d in peer_data]
        roe_vals = [d["roe"] for d in peer_data]
        fcf_vals = [d["fcf_yield"] for d in peer_data]
        rev_vals = [d["revenue_growth"] for d in peer_data]

        for sym in sector_syms:
            d = sym_data[sym]
            pe_pct = _percentile_rank(pe_vals, d["pe_ttm"], lower_is_better=True)
            ps_pct = _percentile_rank(ps_vals, d["ps_ratio"], lower_is_better=True)
            pb_pct = _percentile_rank(pb_vals, d["pb_ratio"], lower_is_better=True)

            # Quality composite: 0.4*ROE + 0.3*FCF_yield + 0.3*revenue_growth
            roe_n = _normalize_01(roe_vals, d["roe"])
            fcf_n = _normalize_01(fcf_vals, d["fcf_yield"])
            rev_n = _normalize_01(rev_vals, d["revenue_growth"])

            quality_parts = []
            quality_weights = []
            if roe_n is not None:
                quality_parts.append(0.4 * roe_n)
                quality_weights.append(0.4)
            if fcf_n is not None:
                quality_parts.append(0.3 * fcf_n)
                quality_weights.append(0.3)
            if rev_n is not None:
                quality_parts.append(0.3 * rev_n)
                quality_weights.append(0.3)

            quality_score = None
            if quality_weights:
                quality_score = round(sum(quality_parts) / sum(quality_weights), 4)

            # Valuation score = mean of available percentiles
            val_parts = [p for p in [pe_pct, ps_pct, pb_pct] if p is not None]
            valuation_score = round(sum(val_parts) / len(val_parts), 4) if val_parts else None

            results[sym] = {
                "pe_pct": pe_pct,
                "ps_pct": ps_pct,
                "pb_pct": pb_pct,
                "quality_score": quality_score,
                "valuation_score": valuation_score,
                "sector": sector,
                "sector_peers_count": len(sector_syms),
            }

    log.info("value_scores_computed", scored=len(results),
             sectors=len(sector_groups))
    return results
