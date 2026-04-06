"""
Event study: Cumulative Abnormal Returns (CAR) around earnings.

AR(t) = r_stock(t) - beta * r_market(t)
CAR(0,T) = sum AR(t) for t in [0, T]

Uses pre-event beta (estimated from [-60, -5] window before event).
Market benchmark: SPY.
"""
from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import structlog

log = structlog.get_logger()


def _estimate_alpha_beta(
    stock_returns: np.ndarray,
    market_returns: np.ndarray,
) -> tuple[float, float] | None:
    """OLS: r_stock = alpha + beta * r_market + eps. Returns (alpha, beta) or None."""
    if len(stock_returns) < 20 or len(market_returns) < 20:
        return None

    T = min(len(stock_returns), len(market_returns))
    y = stock_returns[-T:]
    x = market_returns[-T:]

    # Remove any NaN
    mask = ~(np.isnan(y) | np.isnan(x))
    y, x = y[mask], x[mask]
    if len(y) < 20:
        return None

    X = np.column_stack([np.ones(len(x)), x])
    try:
        coeffs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return None

    return float(coeffs[0]), float(coeffs[1])


def compute_earnings_car(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    benchmark: str = "SPY",
    lookback_events: int = 8,
    event_window_days: int = 60,
    pre_event_window: int = 60,
    pre_event_gap: int = 5,
) -> list[dict]:
    """
    Compute CAR for recent earnings events.

    For each symbol with earnings in the last event_window_days:
    1. Get earnings event dates from earnings_calendar
    2. For each event:
       a. Estimate beta from [-60, -5] pre-event window
       b. Compute AR(t) = r_stock(t) - beta * r_market(t)
       c. Compute CAR for [0,1], [0,3], [0,5], [0,10] windows
    3. Return list of CAR dicts.
    """
    cutoff = as_of.strftime("%Y-%m-%d")
    event_start = (as_of - timedelta(days=event_window_days)).strftime("%Y-%m-%d")
    # Need price data going back further for pre-event beta estimation
    price_start = (as_of - timedelta(days=event_window_days + pre_event_window + 30)).strftime("%Y-%m-%d")

    placeholders = ", ".join(["?"] * len(symbols))

    # Get recent earnings events
    events = con.execute(f"""
        SELECT symbol, report_date
        FROM earnings_calendar
        WHERE symbol IN ({placeholders})
          AND report_date BETWEEN '{event_start}' AND '{cutoff}'
          AND actual_eps IS NOT NULL
        ORDER BY symbol, report_date DESC
    """, symbols).fetchall()

    if not events:
        log.info("event_study_no_events")
        return []

    # Get unique symbols with events + benchmark
    event_syms = sorted(set([e[0] for e in events] + [benchmark]))

    # Load price data
    sym_placeholders = ", ".join(["?"] * len(event_syms))
    price_rows = con.execute(f"""
        SELECT symbol, date, adj_close
        FROM prices_daily
        WHERE symbol IN ({sym_placeholders})
          AND date BETWEEN '{price_start}' AND '{cutoff}'
        ORDER BY symbol, date
    """, event_syms).fetchdf()

    if price_rows.empty:
        log.warning("event_study_no_price_data")
        return []

    # Build price index: {symbol: (dates_list, prices_array, date_to_idx)}
    price_index: dict[str, tuple[list[date], np.ndarray, dict[date, int]]] = {}
    for sym, grp in price_rows.groupby("symbol", sort=False):
        grp = grp.sort_values("date")
        dates = []
        for d in grp["date"].tolist():
            if isinstance(d, date) and not hasattr(d, "hour"):
                dates.append(d)
            elif hasattr(d, "date"):
                dates.append(d.date())
            else:
                dates.append(d)
        prices = grp["adj_close"].values.astype(float)
        date_to_idx = {d: i for i, d in enumerate(dates)}
        price_index[sym] = (dates, prices, date_to_idx)

    bench_data = price_index.get(benchmark)
    if bench_data is None:
        log.warning("event_study_no_benchmark_data", benchmark=benchmark)
        return []

    bench_dates, bench_prices, bench_date_to_idx = bench_data
    bench_returns = np.diff(np.log(np.maximum(bench_prices, 1e-9)))

    results: list[dict] = []

    # Process each symbol's events (limit to most recent lookback_events)
    sym_event_count: dict[str, int] = {}

    for sym, event_date_raw in events:
        # Limit events per symbol
        sym_event_count[sym] = sym_event_count.get(sym, 0) + 1
        if sym_event_count[sym] > lookback_events:
            continue

        sym_data = price_index.get(sym)
        if sym_data is None:
            continue

        sym_dates, sym_prices, sym_date_to_idx = sym_data
        sym_returns = np.diff(np.log(np.maximum(sym_prices, 1e-9)))

        # Normalize event date
        if isinstance(event_date_raw, date) and not hasattr(event_date_raw, "hour"):
            event_date = event_date_raw
        elif hasattr(event_date_raw, "date"):
            event_date = event_date_raw.date()
        else:
            event_date = event_date_raw

        # Find event index in price data (nearest trading day on or after)
        event_idx = sym_date_to_idx.get(event_date)
        if event_idx is None:
            # Try next few days (event might be on weekend)
            for offset in range(1, 5):
                shifted = event_date + timedelta(days=offset)
                event_idx = sym_date_to_idx.get(shifted)
                if event_idx is not None:
                    break
        if event_idx is None:
            continue

        bench_event_idx = bench_date_to_idx.get(event_date)
        if bench_event_idx is None:
            for offset in range(1, 5):
                shifted = event_date + timedelta(days=offset)
                bench_event_idx = bench_date_to_idx.get(shifted)
                if bench_event_idx is not None:
                    break
        if bench_event_idx is None:
            continue

        # Estimate pre-event beta from [-60, -5] returns window
        pre_start = max(0, event_idx - pre_event_window - 1)
        pre_end = max(0, event_idx - pre_event_gap - 1)
        bench_pre_start = max(0, bench_event_idx - pre_event_window - 1)
        bench_pre_end = max(0, bench_event_idx - pre_event_gap - 1)

        if pre_end <= pre_start or bench_pre_end <= bench_pre_start:
            continue

        stock_pre_returns = sym_returns[pre_start:pre_end]
        market_pre_returns = bench_returns[bench_pre_start:bench_pre_end]

        ab = _estimate_alpha_beta(stock_pre_returns, market_pre_returns)
        if ab is None:
            continue
        alpha, beta = ab

        # Compute CAR for different windows
        car_windows = {1: None, 3: None, 5: None, 10: None}

        for horizon in car_windows:
            # Compute AR for each day in [event, event+horizon] using return indices
            # event_idx is in price space; return index = event_idx - 1 gives
            # the return from day event_idx-1 to event_idx.
            # For post-event returns, we want returns starting at event_idx.
            ret_start = event_idx  # return index: price[event_idx+1]/price[event_idx]
            ret_end = min(event_idx + horizon, len(sym_returns))
            bench_ret_start = bench_event_idx
            bench_ret_end = min(bench_event_idx + horizon, len(bench_returns))

            actual_horizon = min(ret_end - ret_start, bench_ret_end - bench_ret_start)
            if actual_horizon < 1:
                continue

            stock_post = sym_returns[ret_start:ret_start + actual_horizon]
            market_post = bench_returns[bench_ret_start:bench_ret_start + actual_horizon]

            # AR(t) = r_stock(t) - (alpha + beta * r_market(t))
            ar = stock_post - (alpha + beta * market_post)
            car = float(np.sum(ar))

            car_windows[horizon] = round(car * 100, 4)  # convert to percent

        result = {
            "symbol": sym,
            "event_date": event_date,
            "car_1d": car_windows[1],
            "car_3d": car_windows[3],
            "car_5d": car_windows[5],
            "car_10d": car_windows[10],
            "pre_event_beta": round(beta, 4),
        }
        results.append(result)

    log.info(
        "event_study_complete",
        events_processed=len(results),
        symbols=len(sym_event_count),
    )

    return results


def store_earnings_car(
    con: duckdb.DuckDBPyConnection,
    cars: list[dict],
    as_of: date,
) -> int:
    """Store earnings CAR results in DuckDB."""
    if not cars:
        return 0

    as_of_str = as_of.strftime("%Y-%m-%d")

    for car in cars:
        event_date_str = car["event_date"]
        if isinstance(event_date_str, date):
            event_date_str = event_date_str.strftime("%Y-%m-%d")
        else:
            event_date_str = str(event_date_str)[:10]

        con.execute("""
            INSERT OR REPLACE INTO earnings_car
                (symbol, event_date, car_1d, car_3d, car_5d, car_10d,
                 pre_event_beta, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            car["symbol"], event_date_str,
            car["car_1d"], car["car_3d"], car["car_5d"], car["car_10d"],
            car["pre_event_beta"],
            as_of_str,
        ])

    con.commit()
    log.info("earnings_car_stored", n=len(cars))
    return len(cars)
