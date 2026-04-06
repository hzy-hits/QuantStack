"""
Screen and universe-summary helpers for the report bundle.

Moved from ``bundle.py`` to keep the orchestrator thin.
"""
from __future__ import annotations

import duckdb


def _build_dip_screen_section(items: list[dict]) -> dict:
    """Package scored DYP candidates into the bundle payload format."""
    if not items:
        return {"summary": {"eligible_count": 0, "rendered_count": 0}, "items": []}

    label_counts = {}
    flag_count = 0
    for it in items:
        label = it.get("label", "UNKNOWN")
        label_counts[label] = label_counts.get(label, 0) + 1
        if it.get("flags"):
            flag_count += 1

    return {
        "summary": {
            "rendered_count": len(items),
            "label_counts": label_counts,
            "items_with_flags": flag_count,
        },
        "items": items,
    }


def _universe_summary(con: duckdb.DuckDBPyConnection, as_of_str: str) -> dict:
    """
    Quick snapshot of all symbols — gives Claude the full market picture
    without burying the notable items. Grouped by asset class.
    """
    rows = con.execute("""
        WITH latest AS (
            SELECT symbol, adj_close, date,
                   LAG(adj_close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_1d
            FROM prices_daily WHERE date <= ?
        )
        SELECT l.symbol, l.adj_close, l.prev_1d,
               a.regime, a.z_score, a.strength_bucket
        FROM latest l
        LEFT JOIN analysis_daily a
            ON a.symbol = l.symbol AND a.date = ? AND a.module_name = 'momentum_risk'
        WHERE l.date = (SELECT MAX(date) FROM prices_daily WHERE date <= ? AND close IS NOT NULL)
    """, [as_of_str, as_of_str, as_of_str]).fetchdf()

    result = {}
    for _, r in rows.iterrows():
        sym = r["symbol"]
        ac = r["adj_close"]
        prev = r["prev_1d"]
        ret_1d = round((ac / prev - 1) * 100, 2) if prev and prev > 0 else None
        result[sym] = {
            "ret_1d_pct": ret_1d,
            "regime": r.get("regime"),
            "z_score": round(float(r["z_score"]), 2) if r["z_score"] is not None else None,
            "strength": r.get("strength_bucket"),
        }
    return result
