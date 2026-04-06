"""Load options_sentiment data for the notable items pipeline."""
from __future__ import annotations

from datetime import date

import duckdb
import structlog

log = structlog.get_logger()


def load_sentiment(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, dict]:
    """
    Load options_sentiment for current date.

    Returns {symbol: {vrp, pc_ratio_z, skew_z, iv_ann, rv_ann, pc_ratio_raw, skew_raw}}
    """
    as_of_str = as_of.strftime("%Y-%m-%d")
    try:
        df = con.execute("""
            SELECT symbol, vrp, iv_ann, rv_ann, vrp_z,
                   pc_ratio_z, skew_z, pc_ratio_raw, skew_raw
            FROM options_sentiment
            WHERE as_of = ?
        """, [as_of_str]).fetchdf()
    except Exception:
        log.debug("sentiment_table_not_found", as_of=as_of_str)
        return {}

    if df.empty:
        return {}

    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        entry = {}
        for col in ["vrp", "iv_ann", "rv_ann", "vrp_z",
                     "pc_ratio_z", "skew_z", "pc_ratio_raw", "skew_raw"]:
            val = row.get(col)
            if val is not None:
                import math
                fval = float(val)
                entry[col] = fval if math.isfinite(fval) else None
            else:
                entry[col] = None
        result[row["symbol"]] = entry

    log.debug("sentiment_loaded", symbols=len(result))
    return result
