#!/usr/bin/env python3
"""
Historical backtest for the daily probability signal pipeline.

The script reconstructs what the pipeline would have signaled on each
historical trade date by:
  1. Re-running momentum_risk and earnings_risk on the historical cutoff
  2. Rebuilding the notable-item payload from the same filtering code
  3. Re-classifying items into long/short/neutral signals
  4. Measuring 5-trading-day forward returns for each emitted signal

Outputs:
  - signal-level CSV
  - date-level strategy CSV
  - JSON summary with aggregate metrics and sanity-window diagnostics

Examples:
    python scripts/backtest_signals.py
    python scripts/backtest_signals.py --start 2025-01-01 --end 2025-12-31
    python scripts/backtest_signals.py --cost-bps 20 --output-prefix reports/backtests/q1_2025
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import structlog

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from quant_bot.analytics.earnings_risk import run_earnings_risk
from quant_bot.analytics.momentum_risk import run_momentum_risk
from quant_bot.config.settings import Settings
from quant_bot.filtering.notable import build_notable_items
from quant_bot.signals.classify import classify_all

MACRO_LABELS = {
    "FEDFUNDS": "Fed Funds Rate (%)",
    "DGS10": "10Y Treasury Yield (%)",
    "BAMLH0A0HYM2": "HY Credit Spread (bps)",
    "VIXCLS": "VIX - Market Fear Index",
    "T10Y2Y": "10Y-2Y Yield Spread (recession indicator)",
    "UNRATE": "Unemployment Rate (%)",
    "CPIAUCSL": "CPI YoY Inflation Rate (%)",
}

SOURCE_VIEWS = [
    "prices_daily",
    "earnings_calendar",
    "universe_constituents",
    "macro_daily",
    "news_items",
    "sec_filings",
    "index_changes",
    "options_snapshot",
    "options_analysis",
]

SANITY_WINDOWS = {
    "2020_covid_crash": ("2020-02-20", "2020-04-15"),
    "2022_rate_hike_cycle": ("2022-03-16", "2022-12-31"),
    "2024_ai_rally": ("2024-03-07", "2024-08-31"),
}


def _patch_polars_from_pandas() -> None:
    """
    polars.from_pandas() uses a multiprocessing-backed conversion path in this
    environment, which fails under the sandbox. Route conversion through Arrow
    instead so the existing analytics modules remain reusable.
    """
    original = pl.from_pandas

    def _from_pandas_via_arrow(
        data: pd.DataFrame,
        *,
        schema_overrides: dict[str, Any] | None = None,
        rechunk: bool = True,
        nan_to_null: bool = True,
        include_index: bool = False,
    ) -> pl.DataFrame:
        del nan_to_null  # Arrow preserves nullability directly.
        df = data.reset_index() if include_index else data
        table = pa.Table.from_pandas(df, preserve_index=False)
        out = pl.from_arrow(table, rechunk=rechunk)
        if schema_overrides:
            out = out.cast(schema_overrides, strict=False)
        return out

    if getattr(original, "__name__", "") != "_from_pandas_via_arrow":
        pl.from_pandas = _from_pandas_via_arrow  # type: ignore[assignment]


def _configure_structlog(min_level: str = "warning") -> None:
    level_map = {
        "debug": 10,
        "info": 20,
        "warning": 30,
        "error": 40,
    }
    current = structlog.get_config()
    structlog.configure(
        processors=current["processors"],
        context_class=current["context_class"],
        logger_factory=current["logger_factory"],
        cache_logger_on_first_use=current["cache_logger_on_first_use"],
        wrapper_class=structlog.make_filtering_bound_logger(level_map[min_level]),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest daily signal accuracy from historical pipeline reconstruction.",
    )
    parser.add_argument("--config", type=str, default="config.yaml")
    parser.add_argument("--db", type=str, default=None, help="Override DuckDB path from config.")
    parser.add_argument("--start", type=str, default=None, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end", type=str, default=None, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument("--benchmark", type=str, default=None, help="Override benchmark symbol.")
    parser.add_argument("--max-items", type=int, default=50, help="Final notable items per day.")
    parser.add_argument("--candidate-pool", type=int, default=120, help="First-pass candidate pool size.")
    parser.add_argument(
        "--cost-bps",
        type=float,
        default=10.0,
        help="Round-trip transaction cost in basis points for long/short signals.",
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default=None,
        help="Path prefix for outputs. Script writes *_signals.csv, *_daily.csv, *_summary.json.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=20,
        help="Print progress every N processed trade dates.",
    )
    return parser.parse_args()


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, np.floating)) and math.isnan(float(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: Any, digits: int = 4) -> float | None:
    value_f = _safe_float(value)
    if value_f is None:
        return None
    return round(value_f, digits)


def _analysis_table_schema() -> str:
    return """
        CREATE TEMP TABLE analysis_daily (
            symbol            VARCHAR NOT NULL,
            date              DATE NOT NULL,
            module_name       VARCHAR NOT NULL,
            trend_prob        DOUBLE,
            p_upside          DOUBLE,
            p_downside        DOUBLE,
            daily_risk_usd    DOUBLE,
            expected_move_pct DOUBLE,
            z_score           DOUBLE,
            p_value_raw       DOUBLE,
            p_value_bonf      DOUBLE,
            strength_bucket   VARCHAR,
            regime            VARCHAR,
            details           VARCHAR
        )
    """


def _append_analysis(con: duckdb.DuckDBPyConnection, df) -> int:
    if df.is_empty():
        return 0
    con.register("analysis_updates", df.to_arrow())
    con.execute(
        """
        INSERT INTO analysis_daily
        SELECT
            symbol, date, module_name,
            trend_prob, p_upside, p_downside,
            daily_risk_usd, expected_move_pct,
            z_score, p_value_raw, p_value_bonf,
            strength_bucket, regime, details
        FROM analysis_updates
        """
    )
    con.unregister("analysis_updates")
    return len(df)


def _make_working_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    escaped = str(db_path).replace("'", "''")
    con.execute(f"ATTACH '{escaped}' AS src (READ_ONLY)")
    for table_name in SOURCE_VIEWS:
        try:
            con.execute(f"CREATE VIEW {table_name} AS SELECT * FROM src.{table_name}")
        except duckdb.Error:
            continue
    con.execute(_analysis_table_schema())
    return con


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(result and result[0])


def _table_coverage(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    date_col: str,
) -> dict[str, Any]:
    if not _table_exists(con, table_name):
        return {"table": table_name, "available": False}

    row = con.execute(
        f"SELECT MIN({date_col}), MAX({date_col}), COUNT(*) FROM {table_name}"
    ).fetchone()
    return {
        "table": table_name,
        "available": True,
        "min_date": str(row[0]) if row and row[0] is not None else None,
        "max_date": str(row[1]) if row and row[1] is not None else None,
        "rows": int(row[2]) if row else 0,
    }


def _build_market_context(con: duckdb.DuckDBPyConnection, as_of: date) -> dict[str, Any]:
    as_of_str = as_of.isoformat()
    context: dict[str, Any] = {"macro": {}, "vix": {}}

    try:
        vix_row = con.execute(
            """
            SELECT adj_close, date
            FROM prices_daily
            WHERE symbol = '^VIX' AND date <= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            [as_of_str],
        ).fetchone()
        if vix_row and vix_row[0] is not None:
            context["vix"] = {
                "level": float(vix_row[0]),
                "as_of": str(vix_row[1]),
            }
    except duckdb.Error:
        pass

    try:
        macro_df = con.execute(
            """
            SELECT m.series_id, m.series_name, m.value, m.date
            FROM macro_daily m
            INNER JOIN (
                SELECT series_id, MAX(date) AS max_date
                FROM macro_daily
                WHERE date <= ?
                GROUP BY series_id
            ) latest
              ON m.series_id = latest.series_id
             AND m.date = latest.max_date
            """,
            [as_of_str],
        ).fetchdf()
        for _, row in macro_df.iterrows():
            series_id = row["series_id"]
            label = MACRO_LABELS.get(series_id) or row.get("series_name") or series_id
            value = _safe_float(row["value"])
            context["macro"][label] = {
                "value": value,
                "as_of": str(row["date"]),
                "series_id": series_id,
            }
    except duckdb.Error:
        pass

    return context


def _load_trade_dates(
    con: duckdb.DuckDBPyConnection,
    benchmark: str,
    start: date | None,
    end: date | None,
) -> list[date]:
    cal_df = con.execute(
        """
        WITH cal AS (
            SELECT date,
                   LEAD(date, 5) OVER (ORDER BY date) AS fwd_5d_date
            FROM prices_daily
            WHERE symbol = ?
            ORDER BY date
        )
        SELECT date
        FROM cal
        WHERE fwd_5d_date IS NOT NULL
        ORDER BY date
        """,
        [benchmark],
    ).fetchdf()

    if cal_df.empty:
        raise ValueError(f"No benchmark calendar found for {benchmark}")

    dates = [pd.Timestamp(d).date() for d in cal_df["date"].tolist()]
    if start is not None:
        dates = [d for d in dates if d >= start]
    if end is not None:
        dates = [d for d in dates if d <= end]
    return dates


def _load_symbol_calendar_and_forward_returns(
    con: duckdb.DuckDBPyConnection,
    benchmark: str,
) -> tuple[dict[date, list[str]], dict[tuple[date, str], float], dict[date, float]]:
    price_df = con.execute(
        """
        SELECT
            symbol,
            date,
            adj_close,
            LEAD(adj_close, 5) OVER (PARTITION BY symbol ORDER BY date) AS fwd_adj_close_5d
        FROM prices_daily
        WHERE adj_close IS NOT NULL
        ORDER BY date, symbol
        """
    ).fetchdf()

    if price_df.empty:
        raise ValueError("prices_daily is empty")

    price_df["date"] = pd.to_datetime(price_df["date"]).dt.date
    price_df["fwd_return_5d"] = np.where(
        price_df["fwd_adj_close_5d"].notna() & (price_df["adj_close"] > 0),
        (price_df["fwd_adj_close_5d"] / price_df["adj_close"]) - 1.0,
        np.nan,
    )

    symbols_by_date = (
        price_df.groupby("date", sort=True)["symbol"]
        .apply(lambda s: sorted(set(s.tolist())))
        .to_dict()
    )

    signal_returns = {
        (row.date, row.symbol): float(row.fwd_return_5d)
        for row in price_df.itertuples(index=False)
        if not pd.isna(row.fwd_return_5d)
    }

    benchmark_rows = price_df[price_df["symbol"] == benchmark]
    benchmark_returns = {
        row.date: float(row.fwd_return_5d)
        for row in benchmark_rows.itertuples(index=False)
        if not pd.isna(row.fwd_return_5d)
    }

    return symbols_by_date, signal_returns, benchmark_returns


def _flatten_signal_row(
    as_of: date,
    rank: int,
    item: dict[str, Any],
    eligible_count: int,
    fwd_return_5d: float | None,
    benchmark_fwd_return_5d: float | None,
    cost_bps: float,
) -> dict[str, Any]:
    signal = item.get("signal", {})
    momentum = item.get("momentum", {})
    earnings = item.get("earnings_risk", {})
    options = item.get("options", {})
    side = 1 if signal.get("direction") == "long" else -1 if signal.get("direction") == "short" else 0

    signed_return = None
    net_signed_return = None
    is_hit = None
    if fwd_return_5d is not None:
        if side == 0:
            signed_return = 0.0
            net_signed_return = 0.0
        else:
            cost = cost_bps / 10_000.0
            signed_return = side * fwd_return_5d
            net_signed_return = signed_return - cost
            is_hit = signed_return > 0

    excess_return = None
    if fwd_return_5d is not None and benchmark_fwd_return_5d is not None:
        excess_return = fwd_return_5d - benchmark_fwd_return_5d

    row = {
        "date": as_of.isoformat(),
        "rank": rank,
        "symbol": item["symbol"],
        "eligible_universe_size": eligible_count,
        "score": _round_or_none(item.get("score")),
        "primary_reason": item.get("primary_reason"),
        "sub_score_magnitude": _round_or_none(item.get("sub_scores", {}).get("magnitude"), 3),
        "sub_score_event": _round_or_none(item.get("sub_scores", {}).get("event"), 3),
        "sub_score_momentum": _round_or_none(item.get("sub_scores", {}).get("momentum"), 3),
        "sub_score_options": _round_or_none(item.get("sub_scores", {}).get("options"), 3),
        "sub_score_cross_asset": _round_or_none(item.get("sub_scores", {}).get("cross_asset"), 3),
        "price": _round_or_none(item.get("price"), 4),
        "ret_1d_pct": _round_or_none(item.get("ret_1d_pct"), 2),
        "ret_5d_pct": _round_or_none(item.get("ret_5d_pct"), 2),
        "ret_21d_pct": _round_or_none(item.get("ret_21d_pct"), 2),
        "rel_volume": _round_or_none(item.get("rel_volume"), 2),
        "atr": _round_or_none(item.get("atr"), 2),
        "direction": signal.get("direction"),
        "confidence": signal.get("confidence"),
        "signal_type": signal.get("signal_type"),
        "direction_score": _round_or_none(signal.get("direction_score"), 3),
        "macro_gate": _round_or_none(signal.get("macro_gate"), 2),
        "sources_aligned": ",".join(signal.get("sources_aligned", [])),
        "sources_conflicting": ",".join(signal.get("sources_conflicting", [])),
        "sources_neutral": ",".join(signal.get("sources_neutral", [])),
        "source_details_json": json.dumps(signal.get("source_details", {}), sort_keys=True),
        "momentum_regime": momentum.get("regime"),
        "momentum_trend_prob": _round_or_none(momentum.get("trend_prob"), 4),
        "momentum_p_upside": _round_or_none(momentum.get("p_upside"), 4),
        "momentum_p_downside": _round_or_none(momentum.get("p_downside"), 4),
        "momentum_z_score": _round_or_none(momentum.get("z_score"), 4),
        "momentum_strength_bucket": momentum.get("strength_bucket"),
        "momentum_daily_risk_usd": _round_or_none(momentum.get("daily_risk_usd"), 2),
        "momentum_mom_20d_pct": _round_or_none(momentum.get("mom_20d"), 2),
        "momentum_mom_60d_pct": _round_or_none(momentum.get("mom_60d"), 2),
        "momentum_cpt_n_obs": momentum.get("cpt_n_obs"),
        "momentum_cpt_hits": momentum.get("cpt_hits"),
        "momentum_ci_low": _round_or_none(momentum.get("ci_low"), 4),
        "momentum_ci_high": _round_or_none(momentum.get("ci_high"), 4),
        "earnings_p_upside": _round_or_none(earnings.get("p_upside"), 4),
        "earnings_p_downside": _round_or_none(earnings.get("p_downside"), 4),
        "earnings_expected_move_pct": _round_or_none(earnings.get("expected_move_pct"), 2),
        "earnings_strength_bucket": earnings.get("strength_bucket"),
        "earnings_surprise_quintile": earnings.get("surprise_quintile"),
        "earnings_n_obs": earnings.get("n_obs"),
        "earnings_pre_event_regime": earnings.get("pre_event_regime"),
        "options_expected_move_pct": _round_or_none(options.get("expected_move_pct"), 2),
        "options_atm_iv_pct": _round_or_none(options.get("atm_iv_pct"), 4),
        "options_put_call_ratio": _round_or_none(options.get("put_call_ratio"), 4),
        "options_iv_skew": _round_or_none(options.get("iv_skew"), 4),
        "options_liquidity_score": options.get("liquidity_score"),
        "options_flow_intensity": _round_or_none(options.get("flow_intensity"), 3),
        "events_json": json.dumps(item.get("events", []), sort_keys=True),
        "forward_return_5d": _round_or_none(fwd_return_5d, 6),
        "forward_return_5d_pct": _round_or_none(None if fwd_return_5d is None else fwd_return_5d * 100.0, 4),
        "benchmark_forward_return_5d": _round_or_none(benchmark_fwd_return_5d, 6),
        "benchmark_forward_return_5d_pct": _round_or_none(
            None if benchmark_fwd_return_5d is None else benchmark_fwd_return_5d * 100.0,
            4,
        ),
        "excess_return_5d": _round_or_none(excess_return, 6),
        "excess_return_5d_pct": _round_or_none(None if excess_return is None else excess_return * 100.0, 4),
        "side": side,
        "actionable": side != 0 and fwd_return_5d is not None,
        "is_hit": is_hit,
        "signed_return_5d": _round_or_none(signed_return, 6),
        "signed_return_5d_pct": _round_or_none(
            None if signed_return is None else signed_return * 100.0,
            4,
        ),
        "net_signed_return_5d": _round_or_none(net_signed_return, 6),
        "net_signed_return_5d_pct": _round_or_none(
            None if net_signed_return is None else net_signed_return * 100.0,
            4,
        ),
    }
    return row


def _group_metrics(df: pd.DataFrame, group_col: str) -> list[dict[str, Any]]:
    if df.empty:
        return []

    records: list[dict[str, Any]] = []
    for key, group in df.groupby(group_col, dropna=False):
        actionable = group[group["actionable"] == True]  # noqa: E712
        record = {
            group_col: key,
            "n_signals": int(len(group)),
            "n_actionable": int(len(actionable)),
            "hit_rate": _round_or_none(actionable["is_hit"].mean(), 4),
            "avg_forward_return_5d_pct": _round_or_none(actionable["forward_return_5d_pct"].mean(), 4),
            "avg_signed_return_5d_pct": _round_or_none(actionable["signed_return_5d_pct"].mean(), 4),
            "avg_net_signed_return_5d_pct": _round_or_none(actionable["net_signed_return_5d_pct"].mean(), 4),
            "total_signed_return_5d_pct": _round_or_none(actionable["signed_return_5d_pct"].sum(), 4),
            "total_net_signed_return_5d_pct": _round_or_none(actionable["net_signed_return_5d_pct"].sum(), 4),
        }
        records.append(record)
    return records


def _build_daily_strategy(signal_df: pd.DataFrame) -> pd.DataFrame:
    if signal_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for as_of, group in signal_df.groupby("date", sort=True):
        actionable = group[group["actionable"] == True]  # noqa: E712
        rows.append(
            {
                "date": as_of,
                "n_signals": int(len(group)),
                "n_actionable": int(len(actionable)),
                "n_long": int((group["direction"] == "long").sum()),
                "n_short": int((group["direction"] == "short").sum()),
                "n_neutral": int((group["direction"] == "neutral").sum()),
                "hit_rate": _round_or_none(actionable["is_hit"].mean(), 4),
                "avg_forward_return_5d_pct": _round_or_none(actionable["forward_return_5d_pct"].mean(), 4),
                "avg_signed_return_5d_pct": _round_or_none(actionable["signed_return_5d_pct"].mean(), 4),
                "avg_net_signed_return_5d_pct": _round_or_none(actionable["net_signed_return_5d_pct"].mean(), 4),
                "total_signed_return_5d_pct": _round_or_none(actionable["signed_return_5d_pct"].sum(), 4),
                "total_net_signed_return_5d_pct": _round_or_none(actionable["net_signed_return_5d_pct"].sum(), 4),
            }
        )
    return pd.DataFrame(rows)


def _annualized_trade_sharpe(return_series_pct: pd.Series) -> float | None:
    clean = return_series_pct.dropna()
    if len(clean) < 2:
        return None
    returns = clean / 100.0
    std = float(returns.std(ddof=1))
    if std == 0:
        return None
    return float((returns.mean() / std) * math.sqrt(252.0 / 5.0))


def _momentum_cluster_metrics(signal_df: pd.DataFrame) -> dict[str, Any]:
    actionable = signal_df[(signal_df["actionable"] == True) & signal_df["momentum_trend_prob"].notna()]  # noqa: E712
    if actionable.empty:
        return {
            "n_actionable_with_momentum": 0,
            "n_clustered_53_55": 0,
            "share_clustered_53_55": None,
        }

    cluster = actionable[actionable["momentum_trend_prob"].between(0.53, 0.55, inclusive="both")]
    return {
        "n_actionable_with_momentum": int(len(actionable)),
        "n_clustered_53_55": int(len(cluster)),
        "share_clustered_53_55": _round_or_none(len(cluster) / len(actionable), 4),
        "avg_signed_return_5d_pct": _round_or_none(cluster["signed_return_5d_pct"].mean(), 4),
        "avg_net_signed_return_5d_pct": _round_or_none(cluster["net_signed_return_5d_pct"].mean(), 4),
        "hit_rate": _round_or_none(cluster["is_hit"].mean(), 4),
    }


def _cpt_support_metrics(signal_df: pd.DataFrame) -> dict[str, Any]:
    obs = pd.to_numeric(signal_df["momentum_cpt_n_obs"], errors="coerce").dropna()
    if obs.empty:
        return {"n": 0}
    return {
        "n": int(len(obs)),
        "median_n_obs": _round_or_none(obs.median(), 2),
        "p10_n_obs": _round_or_none(obs.quantile(0.10), 2),
        "p25_n_obs": _round_or_none(obs.quantile(0.25), 2),
        "pct_lt_25_obs": _round_or_none((obs < 25).mean(), 4),
        "pct_lt_50_obs": _round_or_none((obs < 50).mean(), 4),
    }


def _window_metrics(signal_df: pd.DataFrame, window_name: str, start_s: str, end_s: str) -> dict[str, Any]:
    start = _parse_date(start_s)
    end = _parse_date(end_s)
    assert start is not None and end is not None

    if signal_df.empty:
        return {
            "window": window_name,
            "start": start_s,
            "end": end_s,
            "status": "no_signals",
        }

    window_df = signal_df[
        (signal_df["date"] >= start.isoformat()) & (signal_df["date"] <= end.isoformat())
    ]

    if window_df.empty:
        return {
            "window": window_name,
            "start": start_s,
            "end": end_s,
            "status": "unavailable_in_local_db",
        }

    actionable = window_df[window_df["actionable"] == True]  # noqa: E712
    return {
        "window": window_name,
        "start": start_s,
        "end": end_s,
        "status": "ok",
        "n_dates": int(window_df["date"].nunique()),
        "n_signals": int(len(window_df)),
        "n_actionable": int(len(actionable)),
        "direction_counts": {
            k: int(v) for k, v in window_df["direction"].value_counts(dropna=False).to_dict().items()
        },
        "confidence_counts": {
            k: int(v) for k, v in window_df["confidence"].value_counts(dropna=False).to_dict().items()
        },
        "hit_rate": _round_or_none(actionable["is_hit"].mean(), 4),
        "avg_signed_return_5d_pct": _round_or_none(actionable["signed_return_5d_pct"].mean(), 4),
        "avg_net_signed_return_5d_pct": _round_or_none(actionable["net_signed_return_5d_pct"].mean(), 4),
        "top_symbols": [
            {"symbol": symbol, "count": int(count)}
            for symbol, count in window_df["symbol"].value_counts().head(10).items()
        ],
        "top_signal_types": [
            {"signal_type": signal_type, "count": int(count)}
            for signal_type, count in window_df["signal_type"].value_counts().head(10).items()
        ],
    }


def _classify_window_status(
    signal_df: pd.DataFrame,
    window_name: str,
    start_s: str,
    end_s: str,
    local_start: str | None,
    local_end: str | None,
    tested_start: str,
    tested_end: str,
) -> dict[str, Any]:
    start = _parse_date(start_s)
    end = _parse_date(end_s)
    assert start is not None and end is not None

    local_start_d = _parse_date(local_start)
    local_end_d = _parse_date(local_end)
    tested_start_d = _parse_date(tested_start)
    tested_end_d = _parse_date(tested_end)
    assert tested_start_d is not None and tested_end_d is not None

    if local_start_d is None or local_end_d is None:
        return {
            "window": window_name,
            "start": start_s,
            "end": end_s,
            "status": "local_price_coverage_unknown",
        }

    if end < local_start_d or start > local_end_d:
        return {
            "window": window_name,
            "start": start_s,
            "end": end_s,
            "status": "unavailable_in_local_db",
        }

    if end < tested_start_d or start > tested_end_d:
        return {
            "window": window_name,
            "start": start_s,
            "end": end_s,
            "status": "outside_requested_range",
        }

    return _window_metrics(signal_df, window_name, start_s, end_s)


def _render_summary_tables(summary: dict[str, Any]) -> None:
    print("\nBacktest coverage")
    for table_name, info in summary["coverage"]["tables"].items():
        if not info.get("available"):
            print(f"  {table_name}: missing")
            continue
        print(
            f"  {table_name}: {info['min_date']} -> {info['max_date']} "
            f"({info['rows']} rows)"
        )

    overall = summary["overall"]
    print("\nOverall")
    print(
        f"  dates tested: {overall['dates_tested']} | signals: {overall['n_signals']} | "
        f"actionable: {overall['n_actionable']}"
    )
    print(
        f"  hit rate: {overall['hit_rate']} | "
        f"avg signed 5D return: {overall['avg_signed_return_5d_pct']}% gross / "
        f"{overall['avg_net_signed_return_5d_pct']}% net"
    )
    print(
        f"  total signed 5D return: {overall['total_signed_return_5d_pct']}% gross / "
        f"{overall['total_net_signed_return_5d_pct']}% net"
    )
    print(
        f"  entry-date trade Sharpe: {overall['trade_sharpe_gross']} gross / "
        f"{overall['trade_sharpe_net']} net"
    )

    for title, records, key in [
        ("Hit Rate by Confidence", summary["by_confidence"], "confidence"),
        ("Hit Rate by Direction", summary["by_direction"], "direction"),
        ("Average Return by Signal Type", summary["by_signal_type"], "signal_type"),
    ]:
        if not records:
            continue
        df = pd.DataFrame(records)
        cols = [
            key,
            "n_actionable",
            "hit_rate",
            "avg_signed_return_5d_pct",
            "avg_net_signed_return_5d_pct",
        ]
        print(f"\n{title}")
        print(df[cols].to_string(index=False))

    cluster = summary["momentum_cluster_53_55"]
    print("\nMomentum probability cluster")
    print(
        f"  share in 0.53-0.55 bucket: {cluster.get('share_clustered_53_55')} | "
        f"avg signed 5D return: {cluster.get('avg_signed_return_5d_pct')}% gross / "
        f"{cluster.get('avg_net_signed_return_5d_pct')}% net"
    )

    cpt = summary["momentum_cpt_support"]
    print("\nMomentum CPT support")
    print(
        f"  median n_obs: {cpt.get('median_n_obs')} | p10: {cpt.get('p10_n_obs')} | "
        f"% <25 obs: {cpt.get('pct_lt_25_obs')} | % <50 obs: {cpt.get('pct_lt_50_obs')}"
    )

    print("\nSanity windows")
    for window in summary["sanity_windows"]:
        if window["status"] != "ok":
            print(f"  {window['window']}: {window['status']} ({window['start']} -> {window['end']})")
            continue
        print(
            f"  {window['window']}: signals={window['n_signals']} actionable={window['n_actionable']} "
            f"hit_rate={window['hit_rate']} avg_signed_5d={window['avg_signed_return_5d_pct']}%"
        )

    if summary["notes"]:
        print("\nNotes")
        for note in summary["notes"]:
            print(f"  - {note}")


def main() -> None:
    args = parse_args()
    _patch_polars_from_pandas()
    _configure_structlog("warning")
    cfg = Settings.load(args.config)

    db_path = Path(args.db) if args.db else cfg.db_path_abs
    benchmark = args.benchmark or cfg.universe.benchmark
    start = _parse_date(args.start)
    end = _parse_date(args.end)

    source_con = duckdb.connect(str(db_path), read_only=True)
    work_con = _make_working_connection(db_path)

    try:
        price_cov = _table_coverage(source_con, "prices_daily", "date")
        analysis_cov = _table_coverage(source_con, "analysis_daily", "date")
        earnings_cov = _table_coverage(source_con, "earnings_calendar", "report_date")
        options_cov = _table_coverage(source_con, "options_snapshot", "as_of")
        options_analysis_cov = _table_coverage(source_con, "options_analysis", "as_of")

        trade_dates = _load_trade_dates(source_con, benchmark, start, end)
        if not trade_dates:
            raise ValueError("No trade dates available in the requested range with 5D forward prices.")

        symbols_by_date, forward_returns, benchmark_forward_returns = _load_symbol_calendar_and_forward_returns(
            source_con,
            benchmark,
        )

        signal_rows: list[dict[str, Any]] = []

        for idx, as_of in enumerate(trade_dates, start=1):
            eligible_symbols = list(symbols_by_date.get(as_of, []))
            if benchmark not in eligible_symbols:
                continue

            work_con.execute("DELETE FROM analysis_daily")

            mom_df = run_momentum_risk(
                work_con,
                eligible_symbols,
                as_of,
                momentum_windows=cfg.signals.momentum_windows,
                atr_period=cfg.signals.atr_period,
                ma_filter_window=cfg.signals.ma_filter_window,
            )
            earn_df = run_earnings_risk(
                work_con,
                eligible_symbols,
                benchmark,
                as_of,
                lookback_days=cfg.signals.earnings_lookback_days,
                min_history=cfg.signals.earnings_min_history,
            )

            _append_analysis(work_con, mom_df)
            _append_analysis(work_con, earn_df)

            candidates = build_notable_items(
                work_con,
                as_of,
                eligible_symbols,
                benchmark=benchmark,
                max_items=args.candidate_pool,
            )
            candidate_syms = [row["symbol"] for row in candidates]
            pass2_syms = candidate_syms if benchmark in candidate_syms else candidate_syms + [benchmark]
            notable = build_notable_items(
                work_con,
                as_of,
                pass2_syms,
                benchmark=benchmark,
                max_items=args.max_items,
            )
            classify_all(notable, _build_market_context(work_con, as_of))

            bench_fwd = benchmark_forward_returns.get(as_of)
            for rank, item in enumerate(notable, start=1):
                fwd = forward_returns.get((as_of, item["symbol"]))
                signal_rows.append(
                    _flatten_signal_row(
                        as_of=as_of,
                        rank=rank,
                        item=item,
                        eligible_count=len(eligible_symbols),
                        fwd_return_5d=fwd,
                        benchmark_fwd_return_5d=bench_fwd,
                        cost_bps=args.cost_bps,
                    )
                )

            if args.progress_every > 0 and idx % args.progress_every == 0:
                print(
                    f"Processed {idx}/{len(trade_dates)} trade dates "
                    f"({trade_dates[0]} -> {trade_dates[-1]})"
                )

        signal_df = pd.DataFrame(signal_rows)
        if signal_df.empty:
            raise ValueError("The backtest produced no signal rows.")

        for col in ["actionable", "is_hit"]:
            if col in signal_df.columns:
                signal_df[col] = signal_df[col].astype("boolean")

        actionable_df = signal_df[signal_df["actionable"] == True]  # noqa: E712
        daily_df = _build_daily_strategy(signal_df)

        summary = {
            "config": {
                "db_path": str(db_path),
                "benchmark": benchmark,
                "start": trade_dates[0].isoformat(),
                "end": trade_dates[-1].isoformat(),
                "cost_bps": args.cost_bps,
                "max_items": args.max_items,
                "candidate_pool": args.candidate_pool,
                "momentum_windows": cfg.signals.momentum_windows,
                "atr_period": cfg.signals.atr_period,
                "ma_filter_window": cfg.signals.ma_filter_window,
                "earnings_lookback_days": cfg.signals.earnings_lookback_days,
                "earnings_min_history": cfg.signals.earnings_min_history,
            },
            "coverage": {
                "tables": {
                    "prices_daily": price_cov,
                    "analysis_daily": analysis_cov,
                    "earnings_calendar": earnings_cov,
                    "options_snapshot": options_cov,
                    "options_analysis": options_analysis_cov,
                }
            },
            "overall": {
                "dates_tested": int(signal_df["date"].nunique()),
                "n_signals": int(len(signal_df)),
                "n_actionable": int(len(actionable_df)),
                "avg_signals_per_day": _round_or_none(len(signal_df) / signal_df["date"].nunique(), 2),
                "hit_rate": _round_or_none(actionable_df["is_hit"].mean(), 4),
                "avg_forward_return_5d_pct": _round_or_none(actionable_df["forward_return_5d_pct"].mean(), 4),
                "avg_signed_return_5d_pct": _round_or_none(actionable_df["signed_return_5d_pct"].mean(), 4),
                "avg_net_signed_return_5d_pct": _round_or_none(actionable_df["net_signed_return_5d_pct"].mean(), 4),
                "total_signed_return_5d_pct": _round_or_none(actionable_df["signed_return_5d_pct"].sum(), 4),
                "total_net_signed_return_5d_pct": _round_or_none(actionable_df["net_signed_return_5d_pct"].sum(), 4),
                "trade_sharpe_gross": _round_or_none(_annualized_trade_sharpe(daily_df["avg_signed_return_5d_pct"]), 4),
                "trade_sharpe_net": _round_or_none(_annualized_trade_sharpe(daily_df["avg_net_signed_return_5d_pct"]), 4),
            },
            "by_confidence": _group_metrics(
                signal_df[signal_df["confidence"].isin(["HIGH", "MODERATE", "LOW"])],
                "confidence",
            ),
            "by_direction": _group_metrics(
                signal_df[signal_df["direction"].isin(["long", "short"])],
                "direction",
            ),
            "by_signal_type": _group_metrics(signal_df, "signal_type"),
            "momentum_cluster_53_55": _momentum_cluster_metrics(signal_df),
            "momentum_cpt_support": _cpt_support_metrics(signal_df),
            "sanity_windows": [
                _classify_window_status(
                    signal_df=signal_df,
                    window_name=name,
                    start_s=window_start,
                    end_s=window_end,
                    local_start=price_cov.get("min_date"),
                    local_end=price_cov.get("max_date"),
                    tested_start=trade_dates[0].isoformat(),
                    tested_end=trade_dates[-1].isoformat(),
                )
                for name, (window_start, window_end) in SANITY_WINDOWS.items()
            ],
            "notes": [],
        }

        notes = summary["notes"]
        prices_start = price_cov.get("min_date")
        if prices_start and prices_start > SANITY_WINDOWS["2020_covid_crash"][0]:
            notes.append(
                f"COVID crash sanity window is unavailable locally: prices_daily starts on {prices_start}."
            )
        if prices_start and prices_start > SANITY_WINDOWS["2022_rate_hike_cycle"][0]:
            notes.append(
                f"2022 rate-hike sanity window is unavailable locally: prices_daily starts on {prices_start}."
            )
        if earnings_cov.get("available"):
            earnings_start = earnings_cov.get("min_date")
            prices_end = price_cov.get("max_date")
            if earnings_start and prices_end and earnings_start > prices_end:
                notes.append(
                    f"earnings_calendar begins on {earnings_start} while prices_daily ends on {prices_end}; "
                    "historical earnings-driven signals are not represented in this backtest."
                )
        if options_cov.get("available"):
            notes.append(
                f"options_snapshot coverage is sparse ({options_cov.get('min_date')} -> "
                f"{options_cov.get('max_date')}); most historical signals are effectively price/event-only."
            )

        output_prefix = (
            Path(args.output_prefix)
            if args.output_prefix
            else ROOT / "reports" / "backtests" / f"signals_backtest_{trade_dates[0]}_{trade_dates[-1]}"
        )
        output_prefix.parent.mkdir(parents=True, exist_ok=True)

        signals_path = output_prefix.with_name(output_prefix.name + "_signals.csv")
        daily_path = output_prefix.with_name(output_prefix.name + "_daily.csv")
        summary_path = output_prefix.with_name(output_prefix.name + "_summary.json")

        signal_df.to_csv(signals_path, index=False)
        daily_df.to_csv(daily_path, index=False)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

        _render_summary_tables(summary)
        print(f"\nWrote signal-level CSV: {signals_path}")
        print(f"Wrote date-level CSV:   {daily_path}")
        print(f"Wrote summary JSON:     {summary_path}")

    finally:
        work_con.close()
        source_con.close()


if __name__ == "__main__":
    main()
