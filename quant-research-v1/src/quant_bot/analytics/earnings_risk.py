"""
Earnings event risk analysis.

Primary output: P(5D excess return vs SPY > 0 | surprise_quintile)
computed from historical post-earnings drift, using a Beta-Binomial
posterior with Beta(2, 2) prior.

  surprise_quintile: 1 (worst miss) → 5 (biggest beat) — from earnings_calendar.surprise_pct
  If surprise_pct is None (upcoming event), surprise_unknown=True is flagged
  and the estimate falls back to the global (all-quintile) hit rate.

expected_move_pct: historical avg absolute stock move (not excess) over the
5 days following past earnings events.

Pre-event regime is recorded as context in the details dict but is NOT yet
used as a conditioning variable (too few per-symbol events to split further).
"""
from __future__ import annotations

from collections import defaultdict
import json
from datetime import date, timedelta

import duckdb
import numpy as np
import polars as pl
import structlog

from quant_bot.analytics.bayes import BetaPosterior, beta_binomial_update, strength_bucket

log = structlog.get_logger()


def _surprise_quintile(pct: float | None) -> int:
    """Map surprise % to quintile 1 (worst) to 5 (best). 0 = unknown."""
    if pct is None:
        return 0
    if pct <= -10:
        return 1
    elif pct <= -3:
        return 2
    elif pct <= 3:
        return 3
    elif pct <= 10:
        return 4
    return 5


def _pre_event_regime(prices: np.ndarray, event_idx: int, window: int = 20) -> str:
    """Classify the market regime in the 20 bars before an earnings event."""
    start = max(0, event_idx - window)
    r = prices[start:event_idx]
    if len(r) < 4:
        return "noisy"
    returns = np.diff(np.log(np.maximum(r, 1e-9)))
    if len(returns) < 3:
        return "noisy"
    autocorr = float(np.corrcoef(returns[:-1], returns[1:])[0, 1])
    if np.isnan(autocorr):
        return "noisy"
    if autocorr > 0.15:
        return "trending"
    if autocorr < -0.10:
        return "mean_reverting"
    return "noisy"


def _as_date(value: object) -> date:
    """Normalize DB/pandas date-like values to plain date."""
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if hasattr(value, "date"):
        return value.date()
    return value  # type: ignore[return-value]


def _post_earnings_excess(
    sym_prices: np.ndarray,
    sym_event_idx: int,
    bench_prices: np.ndarray,
    bench_event_idx: int,
    horizon: int = 5,
) -> tuple[float, float]:
    """
    Returns (stock_abs_move, excess_return) at `horizon` days after event.
    stock_abs_move: |stock_return| (for expected_move_pct)
    excess_return: stock_return - bench_return (for p_upside direction)
    Both are fractions (not %). Returns (nan, nan) if data insufficient.
    """
    sym_end = sym_event_idx + horizon
    bench_end = bench_event_idx + horizon
    if (sym_end >= len(sym_prices) or sym_event_idx < 1
            or bench_end >= len(bench_prices) or bench_event_idx < 1):
        return float("nan"), float("nan")

    stock_ret = (sym_prices[sym_end] / sym_prices[sym_event_idx]) - 1.0
    bench_ret = (bench_prices[bench_end] / bench_prices[bench_event_idx]) - 1.0
    return abs(float(stock_ret)), float(stock_ret - bench_ret)


def run_earnings_risk(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    benchmark: str,
    as_of: date,
    lookback_days: int = 730,
    min_history: int = 4,
    event_window_days: int = 7,
) -> pl.DataFrame:
    """
    For each symbol with an earnings event within event_window_days of as_of,
    compute risk/probability metrics from historical surprise→drift patterns.

    Returns a DataFrame matching the analysis_daily schema.
    """
    window_start = (as_of - timedelta(days=event_window_days)).strftime("%Y-%m-%d")
    window_end   = (as_of + timedelta(days=event_window_days)).strftime("%Y-%m-%d")
    cutoff       = as_of.strftime("%Y-%m-%d")
    history_start = (as_of - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    sector_map: dict[str, str] = {}
    rows = con.execute("SELECT symbol, sector FROM universe_constituents").fetchall()
    for sym, sector in rows:
        sector_map[sym] = sector or "Unknown"

    # Find events near as_of
    events = con.execute(f"""
        SELECT symbol, report_date, estimate_eps, actual_eps, surprise_pct
        FROM earnings_calendar
        WHERE symbol IN ({','.join('?' * len(symbols))})
          AND report_date BETWEEN '{window_start}' AND '{window_end}'
        ORDER BY symbol, report_date DESC
    """, symbols).fetchall()

    if not events:
        return pl.DataFrame()

    hist_rows = con.execute(f"""
        SELECT symbol, report_date, surprise_pct, actual_eps, estimate_eps
        FROM earnings_calendar
        WHERE actual_eps IS NOT NULL
          AND report_date BETWEEN '{history_start}' AND '{cutoff}'
        ORDER BY symbol, report_date
    """).fetchall()

    # Load prices for analysis window
    all_syms = sorted(set([e[0] for e in events] + [r[0] for r in hist_rows] + [benchmark]))
    price_rows = con.execute(f"""
        SELECT symbol, date, adj_close
        FROM prices_daily
        WHERE symbol IN ({','.join('?' * len(all_syms))})
          AND date BETWEEN '{history_start}' AND '{cutoff}'
        ORDER BY symbol, date
    """, all_syms).fetchdf()

    if price_rows.empty:
        return pl.DataFrame()

    price_rows = price_rows.sort_values(["symbol", "date"])
    price_index: dict[str, tuple[list[date], np.ndarray, dict[date, int]]] = {}
    for sym, sym_df in price_rows.groupby("symbol", sort=False):
        dates = [_as_date(d) for d in sym_df["date"].tolist()]
        prices = sym_df["adj_close"].to_numpy()
        price_index[sym] = (dates, prices, {d: i for i, d in enumerate(dates)})

    # Benchmark index
    bench_data = price_index.get(benchmark)
    if bench_data is None:
        return pl.DataFrame()
    _, bench_prices, bench_date_to_idx = bench_data

    hist_by_symbol: dict[str, list[tuple[date, float | None, float | None, float | None]]] = defaultdict(list)
    symbol_abs_moves: dict[str, list[float]] = defaultdict(list)
    symbol_excess_all: dict[str, list[float]] = defaultdict(list)
    symbol_excess_by_quintile: dict[str, dict[int, list[float]]] = defaultdict(
        lambda: {q: [] for q in range(1, 6)}
    )
    global_hits: dict[int, int] = {q: 0 for q in range(1, 6)}
    global_obs: dict[int, int] = {q: 0 for q in range(1, 6)}
    sector_hits: dict[tuple[str, int], int] = defaultdict(int)
    sector_obs: dict[tuple[str, int], int] = defaultdict(int)

    for hist_sym, h_date_raw, h_surp, h_actual, h_est in hist_rows:
        h_date = _as_date(h_date_raw)
        hist_by_symbol[hist_sym].append((h_date, h_surp, h_actual, h_est))

        q = _surprise_quintile(h_surp)
        if q == 0:
            continue

        sym_data = price_index.get(hist_sym)
        if sym_data is None:
            continue

        _, sym_prices, sym_date_to_idx = sym_data
        si = sym_date_to_idx.get(h_date)
        bi = bench_date_to_idx.get(h_date)
        if si is None or bi is None:
            continue

        abs_move, excess = _post_earnings_excess(sym_prices, si, bench_prices, bi, horizon=5)
        if np.isnan(abs_move):
            continue

        symbol_abs_moves[hist_sym].append(abs_move)
        symbol_excess_all[hist_sym].append(excess)
        symbol_excess_by_quintile[hist_sym][q].append(excess)

        global_obs[q] += 1
        if excess > 0:
            global_hits[q] += 1

        sector = sector_map.get(hist_sym, "Unknown") or "Unknown"
        sector_obs[(sector, q)] += 1
        if excess > 0:
            sector_hits[(sector, q)] += 1

    results = []

    for sym, report_date_raw, estimate_eps, actual_eps, surprise_pct in events:
        sym_data = price_index.get(sym)
        if sym_data is None:
            continue

        sym_dates, sym_prices, sym_date_to_idx = sym_data
        if len(sym_dates) < min_history * 20:
            log.debug("earnings_risk_insufficient_history", symbol=sym)
            continue

        hist = hist_by_symbol.get(sym, [])

        if len(hist) < min_history:
            log.debug("earnings_risk_too_few_events", symbol=sym, events=len(hist))
            continue

        all_abs_moves = symbol_abs_moves.get(sym, [])
        all_excess = symbol_excess_all.get(sym, [])
        excess_by_quintile = symbol_excess_by_quintile.get(sym, {q: [] for q in range(1, 6)})

        # expected_move_pct — stock's absolute move (not excess), in percent
        expected_move_pct = float(np.mean(all_abs_moves)) * 100.0 if all_abs_moves else None

        # Determine surprise quintile for the current event
        current_q = _surprise_quintile(surprise_pct)
        surprise_unknown = current_q == 0

        if surprise_unknown:
            # Upcoming event with no actual yet — use global (all-quintile) hit rate
            hits_use   = sum(1 for e in all_excess if e > 0)
            n_obs_use  = len(all_excess)
            n_q_events = 0
            sector_p = None
            global_p = None
            sector_n = 0
            global_n = 0
            p_upside_raw = beta_binomial_update(hits_use, n_obs_use).mean
        else:
            q_excess = excess_by_quintile.get(current_q, [])
            hits_use   = sum(1 for e in q_excess if e > 0)
            n_obs_use  = len(q_excess)
            n_q_events = n_obs_use
            sector = sector_map.get(sym, "Unknown") or "Unknown"
            sector_n = sector_obs[(sector, current_q)]
            global_n = global_obs[current_q]
            sector_p = beta_binomial_update(
                sector_hits[(sector, current_q)],
                sector_n,
            ).mean
            global_p = beta_binomial_update(global_hits[current_q], global_n).mean
            symbol_p = beta_binomial_update(hits_use, n_obs_use).mean

            w_sym = n_obs_use
            w_sec = sector_n * 0.3
            w_glo = global_n * 0.1
            w_total = w_sym + w_sec + w_glo
            if w_total == 0:
                p_upside_raw = 0.5
            else:
                p_upside_raw = (
                    (w_sym * symbol_p) + (w_sec * sector_p) + (w_glo * global_p)
                ) / w_total

        # Symbol-level posterior still drives evidence quality / strength bucket.
        posterior: BetaPosterior = beta_binomial_update(hits_use, n_obs_use)
        p_upside   = round(p_upside_raw, 4)
        p_downside = round(1.0 - p_upside_raw, 4)
        bucket     = strength_bucket(posterior)

        # Pre-event regime for today's event date (context only)
        rd_obj = _as_date(report_date_raw)
        cur_si = sym_date_to_idx.get(rd_obj)
        pre_regime = _pre_event_regime(sym_prices, cur_si) if cur_si is not None else "unknown"

        details = {
            "report_date":        str(report_date_raw),
            "surprise_pct":       surprise_pct,
            "surprise_quintile":  current_q,
            "surprise_unknown":   surprise_unknown,
            "n_historical_events": len(hist),
            "n_quintile_events":  n_q_events,
            "n_global_events":    len(all_excess),
            "is_past_event":      actual_eps is not None,
            "actual_eps":         actual_eps,
            "estimate_eps":       estimate_eps,
            "pre_event_regime":   pre_regime,
            "ci_low":             round(posterior.ci_low, 4),
            "ci_high":            round(posterior.ci_high, 4),
            "cpt_hits":           posterior.hits,
            "cpt_n_obs":          posterior.observations,
            "pooling": {
                "symbol_p": round(posterior.mean, 4),
                "sector_p": round(sector_p, 4) if sector_p is not None else None,
                "global_p": round(global_p, 4) if global_p is not None else None,
                "symbol_n": n_obs_use,
                "sector_n": sector_n,
                "global_n": global_n,
                "blended_p": round(p_upside_raw, 4),
            },
        }

        results.append({
            "symbol":           sym,
            "date":             as_of,
            "module_name":      "earnings_risk",
            "trend_prob":       None,
            "p_upside":         p_upside,
            "p_downside":       p_downside,
            "daily_risk_usd":   None,
            "expected_move_pct": round(expected_move_pct, 2) if expected_move_pct is not None else None,
            "z_score":          None,          # no cross-sectional z for event-driven module
            "p_value_raw":      None,          # Beta-Binomial posterior replaces frequentist test
            "p_value_bonf":     None,
            "strength_bucket":  bucket,
            "regime":           "event_driven",
            "details":          json.dumps(details),
        })

    return pl.DataFrame(results).with_columns(pl.col("date").cast(pl.Date)) if results else pl.DataFrame()


def store_analysis(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0
    con.register("analysis_updates", df.to_arrow())
    con.execute("""
        INSERT OR REPLACE INTO analysis_daily
        SELECT
            symbol, date, module_name,
            trend_prob, p_upside, p_downside,
            daily_risk_usd, expected_move_pct,
            z_score, p_value_raw, p_value_bonf,
            strength_bucket, regime, details
        FROM analysis_updates
    """)
    con.unregister("analysis_updates")
    con.commit()
    return len(df)
