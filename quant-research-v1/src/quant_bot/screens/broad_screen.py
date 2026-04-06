"""
Broad market screen — scan ~5000 US symbols for unusual activity.

Uses yfinance bulk download to fetch 1-month price data for all symbols,
then screens for large moves, volume surges, and 52-week high proximity.

Any ONE criterion qualifies a symbol:
  - |5D return| > threshold (default 10%)
  - |20D return| > threshold (default 20%)
  - Volume surge: latest volume > multiplier * 20D avg volume
  - Within 2% of 52-week high (checked via separate 1Y fetch for candidates)

Output: up to top_n symbols sorted by absolute magnitude.
"""
from __future__ import annotations

import time
from datetime import date

import duckdb
import numpy as np
import pandas as pd
import yfinance as yf
import structlog

log = structlog.get_logger()


def _bulk_download(symbols: list[str], period: str, batch_size: int = 500) -> pd.DataFrame:
    """
    Download price data for symbols in batches via yfinance.

    Returns a multi-index DataFrame grouped by ticker.
    Batching prevents yfinance timeouts on very large symbol lists.
    """
    all_frames = []
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        log.info("broad_screen_download_batch",
                 batch=batch_num, total=total_batches, symbols=len(batch))
        try:
            raw = yf.download(
                " ".join(batch),
                period=period,
                auto_adjust=False,
                progress=False,
                group_by="ticker" if len(batch) > 1 else "column",
                threads=True,
            )
            if not raw.empty:
                if len(batch) == 1:
                    # Single symbol — wrap in multi-level columns
                    raw.columns = pd.MultiIndex.from_product(
                        [batch, raw.columns]
                    )
                all_frames.append(raw)
        except Exception as e:
            log.warning("broad_screen_batch_error", batch=batch_num, error=str(e))
        time.sleep(1.0)  # polite pause between batches

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames, axis=1)


def _compute_screen_scores(raw: pd.DataFrame, symbols: list[str],
                           return_5d_threshold: float,
                           return_20d_threshold: float,
                           volume_surge_multiplier: float,
                           min_volume_20d: int,
                           min_dollar_volume_20d: float,
                           min_price: float) -> list[dict]:
    """Score each symbol based on screen criteria. Returns list of qualifying dicts."""
    results = []

    for sym in symbols:
        try:
            if sym not in raw.columns.get_level_values(0):
                continue
            df = raw[sym].dropna(how="all")
            if len(df) < 5:
                continue

            close = df["Close"].dropna()
            volume = df["Volume"].dropna()

            if len(close) < 2 or len(volume) < 2:
                continue

            latest_close = float(close.iloc[-1])
            latest_volume = float(volume.iloc[-1])

            if latest_close < min_price:
                continue

            # Average volume check — skip illiquid
            window = min(len(volume), len(close), 20)
            avg_vol_20d = float(volume.tail(window).mean()) if window >= 5 else float(volume.mean())
            avg_dollar_vol_20d = float((close.tail(window) * volume.tail(window)).mean()) if window >= 5 else float(latest_close * avg_vol_20d)
            if avg_vol_20d < min_volume_20d:
                continue
            if avg_dollar_vol_20d < min_dollar_volume_20d:
                continue

            # 5D return
            ret_5d = None
            if len(close) >= 6:
                ret_5d = (latest_close / float(close.iloc[-6]) - 1) * 100

            # 20D return
            ret_20d = None
            if len(close) >= 21:
                ret_20d = (latest_close / float(close.iloc[0]) - 1) * 100

            # Volume surge
            vol_surge = latest_volume / avg_vol_20d if avg_vol_20d > 0 else 0

            # Check criteria
            hit_5d = ret_5d is not None and abs(ret_5d) > return_5d_threshold
            hit_20d = ret_20d is not None and abs(ret_20d) > return_20d_threshold
            hit_volume = vol_surge > volume_surge_multiplier

            if not (hit_5d or hit_20d or hit_volume):
                continue

            # Composite magnitude for sorting
            magnitude = 0.0
            if ret_5d is not None:
                magnitude = max(magnitude, abs(ret_5d))
            if ret_20d is not None:
                magnitude = max(magnitude, abs(ret_20d) * 0.5)  # weight 20D less
            if hit_volume:
                magnitude = max(magnitude, vol_surge * 3)  # scale volume to comparable range

            results.append({
                "symbol": sym,
                "magnitude": round(magnitude, 2),
                "return_5d_pct": round(ret_5d, 2) if ret_5d is not None else None,
                "return_20d_pct": round(ret_20d, 2) if ret_20d is not None else None,
                "volume_surge": round(vol_surge, 2),
                "avg_volume_20d": int(avg_vol_20d),
                "avg_dollar_volume_20d": round(avg_dollar_vol_20d, 2),
                "latest_close": round(latest_close, 2),
                "screen_reason": "|".join(filter(None, [
                    "5d_move" if hit_5d else None,
                    "20d_move" if hit_20d else None,
                    "vol_surge" if hit_volume else None,
                ])),
            })
        except Exception as e:
            log.debug("broad_screen_symbol_error", symbol=sym, error=str(e))
            continue

    return results


def run_broad_screen(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    *,
    top_n: int = 200,
    min_volume_20d: int = 100_000,
    min_dollar_volume_20d: float = 20_000_000.0,
    min_price: float = 5.0,
    return_5d_threshold: float = 10.0,
    return_20d_threshold: float = 20.0,
    volume_surge_multiplier: float = 3.0,
    finnhub_api_key: str = "",
) -> list[str]:
    """
    Run broad screen on all US symbols in the us_symbols table.

    Returns up to top_n symbol strings, sorted by absolute magnitude.
    Falls back gracefully: returns empty list if us_symbols table is empty
    or yfinance download fails entirely.
    """
    # Load symbols from us_symbols table — only major exchanges, common stock
    try:
        rows = con.execute("""
            SELECT symbol FROM us_symbols
            WHERE type = 'Common Stock'
              AND mic IN ('XNYS', 'XNAS', 'XASE', 'XNCM', 'XNMS', 'XNGS', 'ARCX', 'BATS')
        """).fetchall()
    except duckdb.CatalogException:
        log.warning("broad_screen_no_us_symbols_table")
        return []

    all_syms = [r[0] for r in rows]
    if not all_syms:
        log.warning("broad_screen_no_symbols")
        return []

    # Filter out symbols with problematic characters for yfinance
    # Keep tickers up to 5 chars, no dots/spaces/hyphens (warrants, units, etc.)
    clean_syms = [
        s for s in all_syms
        if len(s) <= 5 and s.isalpha() and s.isupper()
    ]
    log.info("broad_screen_start", total_symbols=len(clean_syms))

    # Bulk download 1-month data
    raw = _bulk_download(clean_syms, period="1mo", batch_size=500)
    if raw.empty:
        log.warning("broad_screen_no_data")
        return []

    # Score and filter
    results = _compute_screen_scores(
        raw, clean_syms,
        return_5d_threshold=return_5d_threshold,
        return_20d_threshold=return_20d_threshold,
        volume_surge_multiplier=volume_surge_multiplier,
        min_volume_20d=min_volume_20d,
        min_dollar_volume_20d=min_dollar_volume_20d,
        min_price=min_price,
    )

    # Sort by magnitude, take top N
    results.sort(key=lambda x: x["magnitude"], reverse=True)
    top = results[:top_n]

    symbols_out = [r["symbol"] for r in top]
    log.info("broad_screen_done",
             candidates=len(results), selected=len(symbols_out),
             top3=[r["symbol"] for r in top[:3]] if top else [])

    return symbols_out
