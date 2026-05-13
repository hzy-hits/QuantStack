"""Historical beta-hedged portfolio ledger for alpha sleeves."""
from __future__ import annotations

import json
import math
import statistics
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

import run_main_strategy_v2_backtest as v2
from lib import hedge as hedge_lib

from .base import Sleeve, daily_series, rows_as_dicts, table_exists


def _return_series_from_closes(rows: list[dict[str, Any]], date_key: str, close_key: str) -> dict[str, float]:
    by_symbol: dict[str, list[tuple[str, float]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or row.get("ts_code") or "").upper()
        dt = v2.as_iso(row.get(date_key))
        close = v2.round_or_none(row.get(close_key))
        if symbol and dt and close is not None and close > 0:
            by_symbol.setdefault(symbol, []).append((dt, float(close)))

    out: dict[str, float] = {}
    # This helper is only used after filtering by one symbol at a time in SQL.
    for values in by_symbol.values():
        values.sort(key=lambda item: item[0])
        for (prev_dt, prev_close), (cur_dt, cur_close) in zip(values, values[1:], strict=False):
            if prev_close > 0:
                out[cur_dt] = (cur_close / prev_close - 1.0) * 100.0
    return out


def load_us_hedge_returns(us_db: Path, start: date, as_of: date) -> dict[str, dict[str, float]]:
    if not us_db.exists():
        return {}
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        if not table_exists(con, "prices_daily"):
            return {}
        placeholders = ",".join("?" for _ in hedge_lib.US_HEDGE_BENCHMARKS)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT symbol, date, adj_close AS close
            FROM prices_daily
            WHERE UPPER(symbol) IN ({placeholders})
              AND date >= CAST(? AS DATE)
              AND date <= CAST(? AS DATE)
              AND adj_close > 0
            ORDER BY symbol, date
            """,
            [*hedge_lib.US_HEDGE_BENCHMARKS, (start - timedelta(days=90)).isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()

    out: dict[str, dict[str, float]] = {}
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]).upper(), []).append(row)
    for symbol, symbol_rows in by_symbol.items():
        out[symbol] = _return_series_from_closes(symbol_rows, "date", "close")
    return out


def load_cn_hedge_returns(cn_db: Path, start: date, as_of: date) -> dict[str, dict[str, float]]:
    if not cn_db.exists():
        return {}
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not table_exists(con, "fut_daily"):
            return {}
        placeholders = ",".join("?" for _ in hedge_lib.CN_HEDGE_BENCHMARKS)
        rows = rows_as_dicts(
            con,
            f"""
            SELECT ts_code, trade_date, close
            FROM fut_daily
            WHERE UPPER(ts_code) IN ({placeholders})
              AND trade_date >= CAST(? AS DATE)
              AND trade_date <= CAST(? AS DATE)
              AND close > 0
            ORDER BY ts_code, trade_date
            """,
            [*hedge_lib.CN_HEDGE_BENCHMARKS, (start - timedelta(days=90)).isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()

    out: dict[str, dict[str, float]] = {}
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["ts_code"]).upper(), []).append({"symbol": row["ts_code"], **row})
    for symbol, symbol_rows in by_symbol.items():
        out[symbol] = _return_series_from_closes(symbol_rows, "trade_date", "close")
    return out


def _aligned_values(asset: dict[str, float], benchmark: dict[str, float]) -> tuple[list[float], list[float]]:
    dates = sorted(set(asset) & set(benchmark))
    return [asset[dt] for dt in dates], [benchmark[dt] for dt in dates]


def summarize_r_series(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "n": 0,
            "avg_r": None,
            "lcb80_r": None,
            "win_rate": None,
            "total_r": None,
            "max_drawdown_r": None,
        }
    avg = statistics.fmean(values)
    if len(values) == 1:
        lcb = avg
    else:
        lcb = avg - v2.LCB80_Z * statistics.stdev(values) / math.sqrt(len(values))
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)
    return {
        "n": len(values),
        "avg_r": v2.round_or_none(avg, 8),
        "lcb80_r": v2.round_or_none(lcb, 8),
        "win_rate": v2.round_or_none(sum(1 for value in values if value > 0) / len(values), 6),
        "total_r": v2.round_or_none(sum(values), 8),
        "max_drawdown_r": v2.round_or_none(max_dd, 8),
    }


def daily_rows_from_ledger(ledger_rows: list[dict[str, Any]], as_of: date) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, float]] = {}
    for row in ledger_rows:
        for key in [
            (row["return_date"], row["market"], row["sleeve_id"]),
            (row["return_date"], row["market"], "ALL"),
            (row["return_date"], "ALL", "ALL"),
        ]:
            bucket = grouped.setdefault(
                key,
                {
                    "long_return_r": 0.0,
                    "beta_hedge_return_r": 0.0,
                    "hedge_cost_r": 0.0,
                    "net_return_r": 0.0,
                    "gross_long_r": 0.0,
                    "hedge_notional_r": 0.0,
                    "net_beta_r": 0.0,
                },
            )
            for metric in bucket:
                bucket[metric] += float(row.get(metric) or 0.0)

    out: list[dict[str, Any]] = []
    for (return_date, market, sleeve_id), values in sorted(grouped.items()):
        out.append(
            {
                "as_of": as_of.isoformat(),
                "return_date": return_date,
                "market": market,
                "sleeve_id": sleeve_id,
                "benchmark": "MIXED" if sleeve_id == "ALL" else "",
                **{k: v2.round_or_none(v, 8) for k, v in values.items()},
                "detail_json": "{}",
            }
        )
    return out


def build_portfolio_hedged_backtest(
    sleeves: list[Sleeve],
    us_db: Path,
    cn_db: Path,
    start: date,
    as_of: date,
) -> dict[str, Any]:
    us_benchmarks = load_us_hedge_returns(us_db, start, as_of)
    cn_benchmarks = load_cn_hedge_returns(cn_db, start, as_of)
    benchmark_lookup = {"US": us_benchmarks, "CN": cn_benchmarks}
    ledger_rows: list[dict[str, Any]] = []

    eligible = [s for s in sleeves if s.money_status in {"money_candidate", "stock_trade"}]
    for sleeve in eligible:
        market = sleeve.market.upper()
        sleeve_returns = daily_series(sleeve.rows)
        if not sleeve_returns:
            continue
        benchmarks = benchmark_lookup.get(market) or {}
        best_instrument = "IM.CFX" if market == "CN" else "SPY"
        best_beta = None
        best_corr = None
        if benchmarks:
            best_score = None
            for instrument, benchmark_series in benchmarks.items():
                asset_values, benchmark_values = _aligned_values(sleeve_returns, benchmark_series)
                beta_value = hedge_lib.beta(asset_values, benchmark_values)
                corr_value = hedge_lib.corr(asset_values, benchmark_values)
                if beta_value is None or corr_value is None:
                    continue
                score = max(beta_value, 0.0) * max(corr_value, 0.0)
                if best_score is None or score > best_score:
                    best_score = score
                    best_instrument = instrument
                    best_beta = max(beta_value, 0.0)
                    best_corr = corr_value
        hedge_r, beta_size, beta_source = hedge_lib.hedge_notional_r(market, 1.0, best_beta)
        benchmark_series = benchmarks.get(best_instrument, {})
        for return_date, long_ret_pct in sorted(sleeve_returns.items()):
            hedge_ret_pct = benchmark_series.get(return_date)
            missing_hedge = hedge_ret_pct is None
            hedge_ret_pct = 0.0 if hedge_ret_pct is None else float(hedge_ret_pct)
            long_return_r = float(long_ret_pct) / 100.0
            beta_hedge_return_r = hedge_r * hedge_ret_pct / 100.0
            net_return_r = long_return_r - beta_hedge_return_r
            detail = {
                "beta_source": beta_source,
                "beta_corr": best_corr,
                "missing_hedge_return": missing_hedge,
                "proxy_note": "daily sleeve return is hedged by same-date benchmark close-to-close return",
            }
            ledger_rows.append(
                {
                    "as_of": as_of.isoformat(),
                    "signal_date": return_date,
                    "entry_date": return_date,
                    "exit_date": return_date,
                    "return_date": return_date,
                    "market": market,
                    "sleeve_id": sleeve.sleeve_id,
                    "symbol_or_basket": sleeve.sleeve_id,
                    "long_r": 1.0,
                    "hedge_instrument": best_instrument,
                    "hedge_r": v2.round_or_none(hedge_r, 8),
                    "beta": v2.round_or_none(beta_size, 8),
                    "beta_raw": v2.round_or_none(best_beta, 8),
                    "beta_corr": v2.round_or_none(best_corr, 8),
                    "long_ret_pct": v2.round_or_none(long_ret_pct, 8),
                    "hedge_ret_pct": v2.round_or_none(hedge_ret_pct, 8),
                    "long_return_r": v2.round_or_none(long_return_r, 8),
                    "beta_hedge_return_r": v2.round_or_none(beta_hedge_return_r, 8),
                    "hedge_cost_r": 0.0,
                    "net_return_r": v2.round_or_none(net_return_r, 8),
                    "gross_long_r": 1.0,
                    "hedge_notional_r": v2.round_or_none(hedge_r, 8),
                    "net_beta_r": v2.round_or_none(max(1.0 - hedge_r, 0.0), 8),
                    "reason_json": json.dumps(detail, sort_keys=True),
                }
            )

    daily_rows = daily_rows_from_ledger(ledger_rows, as_of)
    all_daily = [float(row["net_return_r"]) for row in daily_rows if row["market"] == "ALL" and row["sleeve_id"] == "ALL"]
    unhedged_daily = [
        float(row["long_return_r"]) for row in daily_rows if row["market"] == "ALL" and row["sleeve_id"] == "ALL"
    ]
    hedge_daily = [
        float(row["beta_hedge_return_r"]) for row in daily_rows if row["market"] == "ALL" and row["sleeve_id"] == "ALL"
    ]
    return {
        "ledger": ledger_rows,
        "daily": daily_rows,
        "summary": {
            "eligible_sleeves": [s.sleeve_id for s in eligible],
            "net": summarize_r_series(all_daily),
            "unhedged": summarize_r_series(unhedged_daily),
            "hedge_leg": summarize_r_series(hedge_daily),
        },
    }
