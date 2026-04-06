"""Price context, ATR, and benchmark return loading."""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import structlog

from ._common import _safe

log = structlog.get_logger()


def load_price_context(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
):
    """Load price context DataFrame for the given symbols as of the given date."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    sym_placeholders = ",".join("?" * len(symbols))

    price_ctx = con.execute(f"""
        WITH latest AS (
            SELECT symbol,
                   adj_close,
                   close,
                   volume,
                   date,
                   LAG(adj_close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_1d,
                   LAG(adj_close, 5) OVER (PARTITION BY symbol ORDER BY date) AS prev_5d,
                   LAG(adj_close, 21) OVER (PARTITION BY symbol ORDER BY date) AS prev_21d,
                   LAG(volume, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_vol,
                   AVG(volume) OVER (
                       PARTITION BY symbol ORDER BY date
                       ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                   ) AS avg_vol_20d,
                   MAX(adj_close) OVER (
                       PARTITION BY symbol ORDER BY date
                       ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
                   ) AS high_52w
            FROM prices_daily
            WHERE symbol IN ({sym_placeholders})
              AND date <= ?
        )
        SELECT * FROM latest
        WHERE date = (SELECT MAX(date) FROM prices_daily WHERE date <= ? AND close IS NOT NULL)
    """, symbols + [as_of_str, as_of_str]).fetchdf()

    return price_ctx


def load_atr(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, float]:
    """Load 14-day ATR map for all symbols."""
    as_of_str = as_of.strftime("%Y-%m-%d")
    atr_data = con.execute("""
        SELECT symbol,
               AVG(high - low) AS atr_14
        FROM prices_daily
        WHERE date > ? AND date <= ?
        GROUP BY symbol
    """, [(as_of - timedelta(days=20)).strftime("%Y-%m-%d"), as_of_str]).fetchdf()
    return dict(zip(atr_data["symbol"], atr_data["atr_14"]))


def get_benchmark_return(price_ctx, benchmark: str) -> float:
    """Extract 1-day benchmark return from price context DataFrame."""
    bench_rows = price_ctx[price_ctx["symbol"] == benchmark]
    bench_ret_1d = 0.0
    if not bench_rows.empty:
        r = bench_rows.iloc[0]
        if _safe(r.get("prev_1d")) and r["adj_close"] and r["prev_1d"] > 0:
            bench_ret_1d = (r["adj_close"] / r["prev_1d"] - 1.0) * 100.0
    return bench_ret_1d
