"""China A-share alpha sleeve builders."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

import run_main_strategy_v2_backtest as v2

from .base import Sleeve, make_sleeve, rows_as_dicts, table_exists
from .cn_tape_leadership import CN_TAPE_SLEEVE_ID, query_cn_tape_leadership_returns

try:
    import pandas as pd
except ImportError:  # pragma: no cover - the repo already depends on pandas for factor-lab.
    pd = None  # type: ignore[assignment]


def query_cn_forecast_returns(cn_db: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    if not cn_db.exists():
        return []
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not table_exists(con, "forecast") or not table_exists(con, "prices"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH events AS (
                SELECT ts_code, ann_date, forecast_type, p_change_min, p_change_max
                FROM forecast
                WHERE ann_date >= CAST(? AS DATE)
                  AND ann_date <= CAST(? AS DATE)
            ),
            joined AS (
                SELECT e.ts_code, e.ann_date, e.forecast_type, e.p_change_min, e.p_change_max,
                       p.trade_date AS price_date, p.close,
                       ROW_NUMBER() OVER (
                           PARTITION BY e.ts_code, e.ann_date, e.forecast_type
                           ORDER BY p.trade_date
                       ) AS rn
                FROM events e
                JOIN prices p
                  ON p.ts_code = e.ts_code
                 AND p.trade_date > e.ann_date
                 AND p.close > 0
            ),
            entry AS (
                SELECT ts_code, ann_date, forecast_type, p_change_min, p_change_max,
                       price_date AS entry_date, close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT ts_code, ann_date, forecast_type, price_date AS exit_date, close AS exit_close
                FROM joined
                WHERE rn = 6
            )
            SELECT e.ann_date AS report_date,
                   e.ts_code AS symbol,
                   e.forecast_type,
                   e.p_change_min,
                   e.p_change_max,
                   e.entry_date,
                   x.exit_date,
                   (x.exit_close / e.entry_close - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x
              ON x.ts_code = e.ts_code
             AND x.ann_date = e.ann_date
             AND x.forecast_type = e.forecast_type
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()


def query_cn_cb_returns(cn_db: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    if not cn_db.exists():
        return []
    con = duckdb.connect(str(cn_db), read_only=True)
    try:
        if not table_exists(con, "cb_daily"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH signals AS (
                SELECT ts_code, trade_date, close AS signal_close, cb_value, cb_over_rate
                FROM cb_daily
                WHERE trade_date >= CAST(? AS DATE)
                  AND trade_date <= CAST(? AS DATE)
                  AND close > 0
                  AND cb_over_rate IS NOT NULL
            ),
            joined AS (
                SELECT s.ts_code, s.trade_date, s.cb_value, s.cb_over_rate,
                       p.trade_date AS price_date, p.close,
                       ROW_NUMBER() OVER (
                           PARTITION BY s.ts_code, s.trade_date
                           ORDER BY p.trade_date
                       ) AS rn
                FROM signals s
                JOIN cb_daily p
                  ON p.ts_code = s.ts_code
                 AND p.trade_date > s.trade_date
                 AND p.close > 0
            ),
            entry AS (
                SELECT ts_code, trade_date, cb_value, cb_over_rate,
                       price_date AS entry_date, close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT ts_code, trade_date, price_date AS exit_date, close AS exit_close
                FROM joined
                WHERE rn = 6
            )
            SELECT e.trade_date AS report_date,
                   e.ts_code AS symbol,
                   e.cb_value,
                   e.cb_over_rate,
                   e.entry_date,
                   x.exit_date,
                   (x.exit_close / e.entry_close - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x
              ON x.ts_code = e.ts_code
             AND x.trade_date = e.trade_date
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()


def load_cn_log_overlay_rows(cn_db: Path, start: date, as_of: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    if pd is None:
        return [], [], "missing pandas"
    try:
        from quant_bot.analytics import cn_log_denoise_backtest as cn_log
    except ImportError as exc:
        return [], [], f"missing cn log module: {exc}"

    strategy = cn_log.load_strategy_rows(cn_db, start, as_of)
    if strategy.empty:
        return [], [], "no strategy rows"
    symbols = sorted(strategy["symbol"].dropna().astype(str).unique())
    prices = cn_log.load_prices(cn_db, symbols, start - timedelta(days=140), as_of)
    features = cn_log.compute_log_features(prices)
    merged = cn_log.merge_strategy_with_features(strategy, features)
    if "feature_lag_days" in merged.columns:
        merged = merged[pd.to_numeric(merged["feature_lag_days"], errors="coerce") <= 7]
    merged = cn_log.add_holding_days(cn_log.dedupe_strategy_rows(merged))
    all_oversold = merged[merged["strategy_family"] == "oversold_contrarian"].copy()
    ev_positive = all_oversold[
        (all_oversold["alpha_state"] == "positive_ev_setup")
        | (pd.to_numeric(all_oversold["ev_lcb_80_pct"], errors="coerce") > 0)
    ].copy()
    residual = ev_positive[pd.to_numeric(ev_positive["denoise_residual_zscore"], errors="coerce") <= -1.5].copy()
    deep_log = all_oversold[pd.to_numeric(all_oversold["log_return_20d_pct"], errors="coerce") <= -20.0].copy()
    return records_from_df(residual), records_from_df(deep_log), "ok"


def records_from_df(frame: Any) -> list[dict[str, Any]]:
    if pd is None or frame is None or frame.empty:
        return []
    out = frame.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    out = out.replace({float("nan"): None})
    records: list[dict[str, Any]] = []
    for row in out.to_dict(orient="records"):
        records.append(
            {
                "report_date": v2.as_iso(row.get("report_date")),
                "symbol": row.get("symbol"),
                "return_pct": v2.round_or_none(row.get("return_pct")),
            }
        )
    return records


def build_cn_sleeves(cn_db: Path, start: date, as_of: date, min_money_n: int) -> list[Sleeve]:
    sleeves: list[Sleeve] = []
    cn_rows, cn_status = v2.load_cn_strategy_rows(cn_db, start, as_of)
    cn_oversold_all = [row for row in cn_rows if row.get("strategy_family") == "oversold_contrarian"]
    cn_oversold_ev = [
        row
        for row in cn_oversold_all
        if row.get("alpha_state") == "positive_ev_setup" or (v2.round_or_none(row.get("ev_lcb_80_pct")) or 0.0) > 0.0
    ]
    residual_rows, deep_log_rows, log_status = load_cn_log_overlay_rows(cn_db, start, as_of)
    tape_rows, tape_status = query_cn_tape_leadership_returns(cn_db, start, as_of)
    forecast_rows = query_cn_forecast_returns(cn_db, start, as_of)
    positive_forecast = [
        row
        for row in forecast_rows
        if str(row.get("forecast_type") or "") in {"预增", "扭亏", "略增", "续盈"}
    ]
    negative_forecast = [
        row
        for row in forecast_rows
        if str(row.get("forecast_type") or "") in {"预减", "首亏", "略减", "续亏"}
    ]
    cb_rows = query_cn_cb_returns(cn_db, start, as_of)
    cb_low_premium = [row for row in cb_rows if (v2.round_or_none(row.get("cb_over_rate")) or 999.0) <= 5.0]
    cb_high_premium = [row for row in cb_rows if (v2.round_or_none(row.get("cb_over_rate")) or -999.0) >= 30.0]

    sleeves.append(
        make_sleeve(
            sleeve_id="cn_oversold_ev_positive",
            market="cn",
            label="CN oversold_contrarian EV-positive",
            signal_rule="oversold_contrarian with positive EV LCB80 / positive_ev_setup",
            horizon="T+1 to T+5 lifecycle",
            data_status=cn_status,
            role="money",
            notes="Current CN mainline; still clipped by T+1, high-vol, no-chase, and book overlay.",
            rows=cn_oversold_ev,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_oversold_residual_z_action",
            market="cn",
            label="CN EV+ residual_z <= -1.5 action overlay",
            signal_rule="EV-positive oversold plus causal log residual stretch <= -1.5",
            horizon="T+1 to T+5 lifecycle",
            data_status=log_status,
            role="overlay",
            notes="Report action overlay in opportunity mode; EV fields are context, not a hard gate.",
            rows=residual_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_oversold_deep_log20_setup",
            market="cn",
            label="CN all oversold log20 <= -20 setup overlay",
            signal_rule="all oversold_contrarian with causal 20D log return <= -20%",
            horizon="T+1 to T+5 lifecycle",
            data_status=log_status,
            role="overlay",
            notes="Setup overlay in opportunity mode; use for pullback/retest priority.",
            rows=deep_log_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id=CN_TAPE_SLEEVE_ID,
            market="cn",
            label="CN tape leadership continuation",
            signal_rule="price-first 5D leadership + volume expansion + positive money flow + industry/sector confirmation; news is lagging label only",
            horizon="T+1 to T+5 continuation",
            data_status=tape_status,
            role="money",
            notes="HF-style tape sleeve: price/volume/flow/linked board lead; anti-chase caps exclude overheated 5D/20D extension.",
            rows=tape_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_forecast_positive_event",
            market="cn",
            label="CN positive earnings forecast event diagnostic",
            signal_rule="预增/扭亏/略增/续盈, next tradable close to T+5 exit",
            horizon="T+5",
            data_status="diagnostic_only_forecast_table",
            role="research",
            notes="Needs announcement timestamp, eligibility, and event payoff table before money use.",
            rows=positive_forecast,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_forecast_negative_raw_long",
            market="cn",
            label="CN negative forecast raw-long avoid diagnostic",
            signal_rule="预减/首亏/略减/续亏, raw long return for avoid/risk study",
            horizon="T+5",
            data_status="diagnostic_only_forecast_table",
            role="research",
            notes="Risk/avoid diagnostic; A-share long-only report should not short from this.",
            rows=negative_forecast,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_cb_low_premium_rv",
            market="cn",
            label="CN convertible low-premium RV diagnostic",
            signal_rule="cb_daily cb_over_rate <= 5%, next tradable close to T+5 exit",
            horizon="T+5",
            data_status="diagnostic_only_cb_daily",
            role="research",
            notes="Needs terms, forced redemption, putback, conversion window, and liquidity fields before money use.",
            rows=cb_low_premium,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="cn_cb_high_premium_raw_long",
            market="cn",
            label="CN convertible high-premium raw-long risk diagnostic",
            signal_rule="cb_daily cb_over_rate >= 30%, raw long return for risk study",
            horizon="T+5",
            data_status="diagnostic_only_cb_daily",
            role="research",
            notes="Risk/avoid diagnostic; high premium can be sentiment exposure, not alpha proof.",
            rows=cb_high_premium,
            min_money_n=min_money_n,
        )
    )
    return sleeves
