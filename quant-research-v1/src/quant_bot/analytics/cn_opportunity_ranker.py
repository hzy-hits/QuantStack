#!/usr/bin/env python3
"""Rank current A-share opportunities with the data already available locally.

This is the production ranking layer for current A-share rows. It keeps every
oversold candidate ranked, while Alpha Factory-proven sleeve members and
qualified observed-lifecycle rows can move from watchlist into probe tiers.
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_V2_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "cn_opportunity_ranker"
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb"
CN_ALPHA_FACTORY_EXECUTION_SLEEVE = "cn_oversold_ev_positive"
CN_OBSERVED_LIFECYCLE_SLEEVE = "cn_observed_lifecycle_prob"
CN_EXECUTION_ALPHA_STATE = "positive_ev_setup"


@dataclass(frozen=True)
class NewsRiskConfig:
    lookback_days: int = 21
    severe_threshold: float = 0.60
    negative_threshold: float = 0.40
    severe_event_types: tuple[str, ...] = ("regulatory", "governance", "accounting", "fraud", "litigation")
    earnings_event_types: tuple[str, ...] = ("earnings", "forecast")
    severe_metric_drop_pct: float = -30.0
    severe_structured_risk: float = 0.82
    negative_structured_risk: float = 0.55
    sentiment_base: float = 0.30
    sentiment_slope: float = 0.45
    fallback_severe_terms: tuple[str, ...] = (
        "财务造假",
        "造假",
        "虚假陈述",
        "虚假记载",
        "重大遗漏",
        "会计差错",
        "调低去年业绩",
        "大幅调低",
        "下修",
        "业绩变脸",
        "同比下降",
        "同比下滑",
        "暴跌",
        "腰斩",
        "留置",
        "立案",
        "违纪违法",
        "问询函",
        "监管",
        "处罚",
    )
    fallback_negative_terms: tuple[str, ...] = (
        "低开",
        "亏损",
        "下滑",
        "下降",
        "减持",
        "风险",
        "被查",
        "信任危机",
    )


@dataclass(frozen=True)
class RankerConfig:
    """Central scoring contract for the production CN ranker.

    These numbers are defaults, not hidden gates. Keep experiments in a config
    file or backtest artifact, then promote a full config into this contract.
    """

    score_weights: dict[str, float] = field(
        default_factory=lambda: {
            "strategy_ev": 0.20,
            "observed_lifecycle": 0.22,
            "tushare_flow": 0.22,
            "oversold_reversion": 0.10,
            "execution_quality": 0.16,
            "limit_heat": 0.09,
            "liquidity": 0.08,
            "factor_lab": 0.05,
            "sector_heat": 0.02,
            "risk_penalty": -0.08,
            "falling_knife": -0.08,
            "headline_risk": -0.20,
        }
    )
    headline: NewsRiskConfig = field(default_factory=NewsRiskConfig)
    top_probe_count: int = 5
    secondary_probe_count: int = 10
    active_watch_count: int = 20
    event_risk_zero_r: float = 0.60
    falling_knife_zero_r: float = 72.0


DEFAULT_CONFIG = RankerConfig()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank current CN opportunities for production probing.")
    parser.add_argument("--date", required=True, help="Report date, YYYY-MM-DD.")
    parser.add_argument("--v2-root", type=Path, default=DEFAULT_V2_ROOT)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--config", type=Path, default=None, help="Optional JSON scoring config override.")
    parser.add_argument("--top", type=int, default=30, help="Number of rows to keep in the headline table.")
    return parser.parse_args()


def load_ranker_config(path: Path | None) -> RankerConfig:
    if not path:
        return DEFAULT_CONFIG
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"bad CN ranker config {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"bad CN ranker config {path}: expected JSON object")

    base = asdict(DEFAULT_CONFIG)
    for key, value in raw.items():
        if key == "headline" and isinstance(value, dict):
            base["headline"].update(value)
        else:
            base[key] = value
    base["headline"] = NewsRiskConfig(**base["headline"])
    return RankerConfig(**base)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value)
    return text[:10] if text else None


def round_or_none(value: Any, digits: int = 6) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def safe_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def nested_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def first_number(*values: Any) -> float | None:
    for value in values:
        parsed = round_or_none(value)
        if parsed is not None:
            return parsed
    return None


def alpha_factory_sleeve_id(row: dict[str, Any]) -> str | None:
    family = str(row.get("policy") or row.get("strategy_family") or "")
    action = str(row.get("action_intent") or "")
    if not action and str(row.get("state") or "") == "Execution Alpha":
        action = "TRADE"
    alpha_state = str(row.get("alpha_state") or "")
    lcb80 = first_number(row.get("ev_lcb80_pct"), row.get("ev_lcb_80_pct"))
    if (
        family == "oversold_contrarian"
        and action == "TRADE"
        and (alpha_state == CN_EXECUTION_ALPHA_STATE or (lcb80 is not None and lcb80 > 0.0))
    ):
        return CN_ALPHA_FACTORY_EXECUTION_SLEEVE
    return None


def is_special_treatment_name(value: Any) -> bool:
    text = str(value or "").upper().strip()
    return text.startswith("*ST") or text.startswith("ST") or text.startswith("S*ST") or text.startswith("退市")


def fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:+.{digits}f}%"


def fmt_num(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def placeholders(values: list[str]) -> str:
    return ",".join("?" for _ in values)


def normalize_cn_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    return f"{digits}.SH" if digits.startswith(("6", "9")) else f"{digits}.SZ"


def load_v2_candidates(v2_root: Path, as_of: date) -> tuple[list[dict[str, Any]], str, str | None]:
    path = v2_root / as_of.isoformat() / "main_strategy_v2_backtest.json"
    if not path.exists():
        return [], "missing_v2_report", str(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [], f"bad_v2_report:{exc}", str(path)
    rows = []
    for row in (payload.get("cn") or {}).get("current") or []:
        symbol = normalize_cn_symbol(row.get("symbol"))
        if not symbol:
            continue
        copied = dict(row)
        copied["symbol"] = symbol
        rows.append(copied)
    return rows, "ok", str(path)


def load_db_candidates(db_path: Path, as_of: date) -> tuple[list[dict[str, Any]], str]:
    if not db_path.exists():
        return [], "missing_cn_db"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return [], "missing_strategy_model_dataset"
        latest_row = con.execute(
            "SELECT MAX(report_date) FROM strategy_model_dataset WHERE report_date <= CAST(? AS DATE)",
            [as_of.isoformat()],
        ).fetchone()
        latest = latest_row[0] if latest_row else None
        if latest is None:
            return [], "no_strategy_rows"
        latest_iso = as_iso(latest) or as_of.isoformat()
        rows = rows_as_dicts(
            con,
            """
            SELECT
                m.report_date AS as_of, m.symbol,
                COALESCE(sb.name, '') AS name,
                COALESCE(sb.industry, '') AS industry,
                m.strategy_family AS policy,
                m.alpha_state,
                m.action_intent,
                m.ev_pct,
                m.ev_lcb_80_pct AS ev_lcb80_pct,
                m.risk_unit_pct,
                m.features_json,
                m.detail_json,
                m.planned_entry,
                m.reference_close
            FROM strategy_model_dataset m
            LEFT JOIN stock_basic sb ON sb.ts_code = m.symbol
            WHERE m.report_date = CAST(? AS DATE)
              AND m.evaluation_date = (
                  SELECT MAX(evaluation_date)
                  FROM strategy_model_dataset
                  WHERE report_date = CAST(? AS DATE)
              )
              AND m.selection_status IN ('selected', 'exploration')
            ORDER BY
              CASE m.action_intent WHEN 'TRADE' THEN 0 WHEN 'SETUP' THEN 1 WHEN 'OBSERVE' THEN 2 ELSE 3 END,
              COALESCE(m.ev_norm_lcb_80, m.ev_norm_score, -999) DESC,
              m.symbol
            LIMIT 200
            """,
            [latest_iso, latest_iso],
        )
        for row in rows:
            row["symbol"] = normalize_cn_symbol(row.get("symbol"))
            row.setdefault("state", "Execution Alpha" if row.get("action_intent") == "TRADE" else "Positive EV Setup")
        return rows, "ok"
    finally:
        con.close()


def latest_symbol_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    date_col: str,
    select_sql: str,
    symbols: list[str],
    as_of: date,
) -> list[dict[str, Any]]:
    if not symbols or not table_exists(con, table):
        return []
    sql = f"""
        WITH latest AS (
            SELECT ts_code, MAX({date_col}) AS latest_date
            FROM {table}
            WHERE {date_col} <= CAST(? AS DATE)
              AND ts_code IN ({placeholders(symbols)})
            GROUP BY ts_code
        )
        {select_sql}
    """
    return rows_as_dicts(con, sql, [as_of.isoformat(), *symbols])


def load_strategy_details(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "strategy_model_dataset"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        SELECT
            m.symbol,
            m.features_json,
            m.detail_json,
            m.strategy_key,
            m.action_intent,
            m.alpha_state,
            m.ev_norm_score,
            m.ev_norm_lcb_80,
            m.p_fill,
            m.mu_ret_pct,
            m.tail_loss_pct,
            m.planned_entry,
            m.reference_close
        FROM strategy_model_dataset m
        WHERE m.report_date = (
            SELECT MAX(report_date)
            FROM strategy_model_dataset
            WHERE report_date <= CAST(? AS DATE)
        )
          AND m.evaluation_date = (
            SELECT MAX(evaluation_date)
            FROM strategy_model_dataset
            WHERE report_date = (
                SELECT MAX(report_date)
                FROM strategy_model_dataset
                WHERE report_date <= CAST(? AS DATE)
            )
        )
          AND m.symbol IN ({placeholders(symbols)})
        """,
        [as_of.isoformat(), as_of.isoformat(), *symbols],
    )
    return {normalize_cn_symbol(row.get("symbol")): row for row in rows}


def load_daily_basic(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    rows = latest_symbol_rows(
        con,
        table="daily_basic",
        date_col="trade_date",
        symbols=symbols,
        as_of=as_of,
        select_sql="""
        SELECT
            d.ts_code AS symbol,
            d.trade_date AS daily_basic_as_of,
            d.turnover_rate,
            d.volume_ratio,
            d.pe_ttm,
            d.pb,
            d.total_mv,
            d.circ_mv
        FROM daily_basic d
        JOIN latest l ON l.ts_code = d.ts_code AND l.latest_date = d.trade_date
        """,
    )
    return {normalize_cn_symbol(row.get("symbol")): row for row in rows}


def load_prices(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    rows = latest_symbol_rows(
        con,
        table="prices",
        date_col="trade_date",
        symbols=symbols,
        as_of=as_of,
        select_sql="""
        SELECT
            p.ts_code AS symbol,
            p.trade_date AS price_as_of,
            p.close,
            p.pct_chg,
            p.amount
        FROM prices p
        JOIN latest l ON l.ts_code = p.ts_code AND l.latest_date = p.trade_date
        """,
    )
    return {normalize_cn_symbol(row.get("symbol")): row for row in rows}


def load_moneyflow(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    daily_basic: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows = latest_symbol_rows(
        con,
        table="moneyflow",
        date_col="trade_date",
        symbols=symbols,
        as_of=as_of,
        select_sql="""
        SELECT
            m.ts_code AS symbol,
            m.trade_date AS moneyflow_as_of,
            m.net_mf_amount,
            m.net_mf_vol,
            (COALESCE(m.buy_lg_amount, 0) + COALESCE(m.buy_elg_amount, 0)
             - COALESCE(m.sell_lg_amount, 0) - COALESCE(m.sell_elg_amount, 0)) AS large_net_amount,
            (COALESCE(m.buy_elg_amount, 0) - COALESCE(m.sell_elg_amount, 0)) AS extra_large_net_amount
        FROM moneyflow m
        JOIN latest l ON l.ts_code = m.ts_code AND l.latest_date = m.trade_date
        """,
    )
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = normalize_cn_symbol(row.get("symbol"))
        circ_mv = round_or_none((daily_basic.get(symbol) or {}).get("circ_mv"))
        net = round_or_none(row.get("net_mf_amount"))
        large = round_or_none(row.get("large_net_amount"))
        extra = round_or_none(row.get("extra_large_net_amount"))
        row["net_mf_pct_circ_mv"] = None if circ_mv in (None, 0) or net is None else net / circ_mv * 100.0
        row["large_net_pct_circ_mv"] = None if circ_mv in (None, 0) or large is None else large / circ_mv * 100.0
        row["extra_large_net_pct_circ_mv"] = None if circ_mv in (None, 0) or extra is None else extra / circ_mv * 100.0
        out[symbol] = row
    return out


def load_margin(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "margin_detail"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        WITH hist AS (
            SELECT
                ts_code,
                trade_date,
                rzye,
                rzmre,
                rzche,
                rqye,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM margin_detail
            WHERE trade_date <= CAST(? AS DATE)
              AND ts_code IN ({placeholders(symbols)})
        )
        SELECT
            latest.ts_code AS symbol,
            latest.trade_date AS margin_as_of,
            latest.rzye,
            latest.rzmre,
            latest.rzche,
            latest.rqye,
            (latest.rzye - prior.rzye) / NULLIF(ABS(prior.rzye), 0) * 100.0 AS rzye_5d_delta_pct,
            (latest.rzmre - latest.rzche) / NULLIF(ABS(latest.rzye), 0) * 100.0 AS margin_buy_minus_repay_pct
        FROM hist latest
        LEFT JOIN hist prior ON prior.ts_code = latest.ts_code AND prior.rn = 6
        WHERE latest.rn = 1
        """,
        [as_of.isoformat(), *symbols],
    )
    return {normalize_cn_symbol(row.get("symbol")): row for row in rows}


def load_analytics(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "analytics"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        WITH latest AS (
            SELECT
                ts_code,
                module,
                metric,
                value,
                as_of,
                ROW_NUMBER() OVER (
                    PARTITION BY ts_code, module, metric
                    ORDER BY as_of DESC
                ) AS rn
            FROM analytics
            WHERE as_of <= CAST(? AS DATE)
              AND ts_code IN ({placeholders(symbols)})
              AND (
                (module = 'flow' AND metric IN ('information_score', 'large_flow_z', 'margin_z', 'tape_z', 'market_vol_z'))
                OR (module = 'lab_factor' AND metric = 'lab_composite')
                OR (module = 'mean_reversion' AND metric IN ('reversion_score', 'rsi_14'))
                OR (module = 'momentum' AND metric = 'trend_prob')
                OR (module = 'breakout' AND metric = 'breakout_score')
                OR (module = 'continuation_vs_fade' AND metric = 'continuation_score')
              )
        )
        SELECT
            ts_code AS symbol,
            MAX(as_of) AS analytics_as_of,
            MAX(CASE WHEN module = 'flow' AND metric = 'information_score' THEN value END) AS flow_information_score,
            MAX(CASE WHEN module = 'flow' AND metric = 'large_flow_z' THEN value END) AS flow_large_flow_z,
            MAX(CASE WHEN module = 'flow' AND metric = 'margin_z' THEN value END) AS flow_margin_z,
            MAX(CASE WHEN module = 'flow' AND metric = 'tape_z' THEN value END) AS flow_tape_z,
            MAX(CASE WHEN module = 'flow' AND metric = 'market_vol_z' THEN value END) AS flow_market_vol_z,
            MAX(CASE WHEN module = 'lab_factor' AND metric = 'lab_composite' THEN value END) AS lab_composite,
            MAX(CASE WHEN module = 'mean_reversion' AND metric = 'reversion_score' THEN value END) AS mean_reversion_score,
            MAX(CASE WHEN module = 'mean_reversion' AND metric = 'rsi_14' THEN value END) AS db_rsi_14,
            MAX(CASE WHEN module = 'momentum' AND metric = 'trend_prob' THEN value END) AS trend_prob,
            MAX(CASE WHEN module = 'breakout' AND metric = 'breakout_score' THEN value END) AS breakout_score,
            MAX(CASE WHEN module = 'continuation_vs_fade' AND metric = 'continuation_score' THEN value END) AS continuation_score_db
        FROM latest
        WHERE rn = 1
        GROUP BY ts_code
        """,
        [as_of.isoformat(), *symbols],
    )
    return {normalize_cn_symbol(row.get("symbol")): row for row in rows}


def load_limit_predictions(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "limit_up_model_predictions"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        WITH latest AS (
            SELECT
                symbol,
                MAX(as_of) AS latest_date
            FROM limit_up_model_predictions
            WHERE as_of <= CAST(? AS DATE)
              AND symbol IN ({placeholders(symbols)})
            GROUP BY symbol
        )
        SELECT
            p.symbol,
            p.as_of AS limit_model_as_of,
            p.p_limit_up,
            p.p_touch_limit,
            p.p_failed_board,
            p.ev_after_cost_pct AS limit_ev_after_cost_pct,
            p.ev_lcb_80_pct AS limit_ev_lcb80_pct,
            p.probability_decile,
            p.model_state,
            p.decision_state
        FROM limit_up_model_predictions p
        JOIN latest l ON l.symbol = p.symbol AND l.latest_date = p.as_of
        """,
        [as_of.isoformat(), *symbols],
    )
    return {normalize_cn_symbol(row.get("symbol")): row for row in rows}


def load_sector_flow(con: duckdb.DuckDBPyConnection, industries: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    industries = sorted({item for item in industries if item})
    if not industries or not table_exists(con, "sector_fund_flow"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        WITH latest AS (
            SELECT
                sector_name,
                MAX(trade_date) AS latest_date
            FROM sector_fund_flow
            WHERE trade_date <= CAST(? AS DATE)
              AND sector_name IN ({placeholders(industries)})
            GROUP BY sector_name
        )
        SELECT
            f.sector_name AS industry,
            f.trade_date AS sector_flow_as_of,
            f.pct_chg AS sector_pct_chg,
            f.main_net_in AS sector_main_net_in,
            f.main_net_pct AS sector_main_net_pct,
            f.super_net_in AS sector_super_net_in,
            f.big_net_in AS sector_big_net_in
        FROM sector_fund_flow f
        JOIN latest l ON l.sector_name = f.sector_name AND l.latest_date = f.trade_date
        """,
        [as_of.isoformat(), *industries],
    )
    return {str(row.get("industry") or ""): row for row in rows}


def parse_news_date(value: Any) -> date | None:
    text = as_iso(value)
    if not text:
        return None
    try:
        return parse_date(text)
    except ValueError:
        return None


def parse_key_metrics(value: Any) -> dict[str, Any]:
    parsed = safe_json_loads(value)
    return parsed if isinstance(parsed, dict) else {}


def metric_event_risk(metrics: dict[str, Any], config: NewsRiskConfig) -> tuple[float, list[str]]:
    risk = 0.0
    flags: list[str] = []
    for key, value in metrics.items():
        key_text = str(key)
        parsed = round_or_none(value)
        if parsed is None:
            continue
        is_profit_or_revenue = any(token in key_text for token in ["净利润", "营收", "收入", "利润"])
        is_yoy = any(token in key_text.lower() for token in ["同比", "yoy", "增速"])
        if is_profit_or_revenue and parsed < 0:
            risk = max(risk, config.negative_structured_risk)
            flags.append(f"negative_metric:{key_text}")
        if is_profit_or_revenue and is_yoy and parsed <= config.severe_metric_drop_pct:
            risk = max(risk, config.severe_structured_risk)
            flags.append(f"severe_metric_drop:{key_text}={parsed:g}")
    return risk, flags


def display_news_text(row: dict[str, Any]) -> str:
    headline = str(row.get("headline") or row.get("title") or "")
    summary = str(row.get("summary_one_line") or "")
    content = str(row.get("content") or "")
    if summary and (not headline or not any(token in headline for token in ["五粮液", "造假", "财报", "业绩", "净利"])):
        return summary
    return headline or content[:160]


def score_headline_rows(rows: list[dict[str, Any]], as_of: date, config: NewsRiskConfig) -> dict[str, Any]:
    risk = 0.0
    flags: list[str] = []
    latest_date: date | None = None
    latest_headline = ""
    latest_risk_date: date | None = None
    latest_risk_headline = ""
    best_risk = 0.0
    best_risk_date: date | None = None
    best_risk_headline = ""
    hit_rows: list[dict[str, Any]] = []

    for row in rows:
        news_date = parse_news_date(row.get("news_date") or row.get("published_at") or row.get("publish_time"))
        display_text = display_news_text(row)
        if news_date and (latest_date is None or news_date > latest_date):
            latest_date = news_date
            latest_headline = display_text
        days_old = (as_of - news_date).days if news_date else 99
        recency = 1.0 if days_old <= 3 else 0.85 if days_old <= 7 else 0.65 if days_old <= 21 else 0.35
        text = " ".join(
            str(row.get(key) or "")
            for key in ["headline", "title", "summary_one_line", "content", "event_type", "sentiment"]
        )
        severe_hits = [term for term in config.fallback_severe_terms if term in text]
        negative_hits = [term for term in config.fallback_negative_terms if term in text]
        event_type = str(row.get("event_type") or "").lower()
        sentiment = str(row.get("sentiment") or "").lower()
        confidence = round_or_none(row.get("sentiment_confidence")) or 0.0
        relevance = round_or_none(row.get("relevance")) or 0.5
        metrics = parse_key_metrics(row.get("key_metrics"))

        row_risk = 0.0
        metric_risk, metric_flags = metric_event_risk(metrics, config)
        if metric_risk:
            row_risk = max(row_risk, metric_risk * recency)
            flags.extend(metric_flags)
        if event_type in config.severe_event_types and sentiment in {"negative", "bearish"}:
            row_risk = max(row_risk, config.severe_structured_risk * recency)
            flags.append(f"structured_event:{event_type}")
        if event_type in config.earnings_event_types and sentiment == "negative":
            row_risk = max(row_risk, config.negative_structured_risk * recency)
            flags.append(f"negative_{event_type}")
        if severe_hits:
            row_risk = max(row_risk, config.severe_structured_risk * recency)
            flags.extend(severe_hits)
        if negative_hits:
            row_risk = max(row_risk, config.negative_structured_risk * recency)
            flags.extend(negative_hits)
        if sentiment == "negative":
            row_risk = max(
                row_risk,
                (config.sentiment_base + config.sentiment_slope * clamp(confidence) * clamp(relevance)) * recency,
            )
            flags.append("negative_sentiment")
        if row_risk > 0:
            if news_date and (latest_risk_date is None or news_date > latest_risk_date):
                latest_risk_date = news_date
                latest_risk_headline = display_text
            if row_risk > best_risk:
                best_risk = row_risk
                best_risk_date = news_date
                best_risk_headline = display_text
            hit_rows.append(
                {
                    "date": news_date.isoformat() if news_date else None,
                    "headline": display_text[:160],
                    "risk": round(row_risk, 4),
                }
            )
        risk = max(risk, row_risk)

    clean_flags = list(dict.fromkeys(flag for flag in flags if flag))
    if risk >= config.severe_threshold:
        level = "severe_event"
    elif risk >= config.negative_threshold:
        level = "negative_event"
    elif risk > 0:
        level = "mild_event"
    else:
        level = "clear"
    return {
        "headline_risk": round(clamp(risk), 6),
        "headline_risk_level": level,
        "headline_flags": clean_flags[:10],
        "latest_headline_date": (best_risk_date or latest_risk_date or latest_date).isoformat() if (best_risk_date or latest_risk_date or latest_date) else None,
        "latest_headline": (best_risk_headline or latest_risk_headline or latest_headline)[:160],
        "headline_hits": hit_rows[:5],
    }


def load_news_risk(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
    config: NewsRiskConfig,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    lookback_start = (as_of - timedelta(days=config.lookback_days)).isoformat()

    if table_exists(con, "news_enriched"):
        rows = rows_as_dicts(
            con,
            f"""
            WITH parsed AS (
                SELECT
                    ts_code AS symbol,
                    TRY_CAST(SUBSTR(published_at, 1, 10) AS DATE) AS news_date,
                    headline,
                    event_type,
                    sentiment,
                    sentiment_confidence,
                    relevance,
                    key_metrics,
                    summary_one_line
                FROM news_enriched
                WHERE ts_code IN ({placeholders(symbols)})
            )
            SELECT *
            FROM parsed
            WHERE news_date >= CAST(? AS DATE)
              AND news_date <= CAST(? AS DATE)
            """,
            [*symbols, lookback_start, as_of.isoformat()],
        )
        for row in rows:
            by_symbol.setdefault(normalize_cn_symbol(row.get("symbol")), []).append(row)

    if table_exists(con, "stock_news"):
        rows = rows_as_dicts(
            con,
            f"""
            WITH parsed AS (
                SELECT
                    ts_code AS symbol,
                    TRY_CAST(SUBSTR(publish_time, 1, 10) AS DATE) AS news_date,
                    title,
                    content,
                    source,
                    url
                FROM stock_news
                WHERE ts_code IN ({placeholders(symbols)})
            )
            SELECT *
            FROM parsed
            WHERE news_date >= CAST(? AS DATE)
              AND news_date <= CAST(? AS DATE)
            """,
            [*symbols, lookback_start, as_of.isoformat()],
        )
        for row in rows:
            by_symbol.setdefault(normalize_cn_symbol(row.get("symbol")), []).append(row)

    return {
        symbol: score_headline_rows(rows, as_of, config)
        for symbol, rows in by_symbol.items()
        if rows
    }


def load_market_data(
    db_path: Path,
    symbols: list[str],
    industry_by_symbol: dict[str, str],
    as_of: date,
    config: RankerConfig = DEFAULT_CONFIG,
) -> dict[str, dict[str, Any]]:
    if not db_path.exists() or not symbols:
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        daily = load_daily_basic(con, symbols, as_of)
        out: dict[str, dict[str, Any]] = {symbol: dict(daily.get(symbol) or {}) for symbol in symbols}
        sources = [
            load_prices(con, symbols, as_of),
            load_moneyflow(con, symbols, as_of, daily),
            load_margin(con, symbols, as_of),
            load_analytics(con, symbols, as_of),
            load_limit_predictions(con, symbols, as_of),
            load_news_risk(con, symbols, as_of, config.headline),
        ]
        sector = load_sector_flow(con, list(industry_by_symbol.values()), as_of)
        for source in sources:
            for symbol, row in source.items():
                out.setdefault(symbol, {}).update(row)
        for symbol, row in out.items():
            industry = str((row.get("industry") or industry_by_symbol.get(symbol) or "")).strip()
            row["industry"] = industry
            if industry and industry in sector:
                row.update(sector[industry])
        return out
    finally:
        con.close()


def percentile_scores(rows: list[dict[str, Any]], key: str, *, higher_better: bool = True, missing: float = 0.5) -> list[float]:
    values: list[tuple[int, float]] = []
    for idx, row in enumerate(rows):
        value = round_or_none(row.get(key))
        if value is not None:
            values.append((idx, value))
    scores = [missing for _ in rows]
    if not values:
        return scores
    if len(values) == 1:
        scores[values[0][0]] = 1.0
        return scores
    values.sort(key=lambda item: item[1])
    for rank, (idx, _) in enumerate(values):
        pct = rank / (len(values) - 1)
        scores[idx] = pct if higher_better else 1.0 - pct
    return scores


def add_percentiles(rows: list[dict[str, Any]]) -> None:
    specs = {
        "ev_pct": True,
        "ev_lcb80_pct": True,
        "expected_r_t3": True,
        "lcb80_r_t3": True,
        "p_win_t1": True,
        "p_hit_1r_t3": True,
        "p_stop_t3": False,
        "observed_lifecycle_score": True,
        "strategy_samples": True,
        "negative_residual": True,
        "negative_log20": True,
        "rsi_14": False,
        "mean_reversion_score": True,
        "flow_information_score": True,
        "flow_large_flow_z": True,
        "flow_margin_z": True,
        "net_mf_pct_circ_mv": True,
        "large_net_pct_circ_mv": True,
        "rzye_5d_delta_pct": True,
        "setup_score": True,
        "entry_quality_score": True,
        "shadow_alpha_prob": True,
        "stale_chase_risk": False,
        "fade_risk": False,
        "p_touch_limit": True,
        "p_limit_up": True,
        "limit_ev_after_cost_pct": True,
        "p_failed_board": False,
        "turnover_rate": True,
        "volume_ratio": True,
        "amount": True,
        "circ_mv": True,
        "lab_composite": True,
        "continuation_score_db": True,
        "sector_main_net_pct": True,
        "sector_pct_chg": True,
        "risk_unit_pct": False,
        "downside_stress": False,
        "haar_noise_energy": False,
    }
    for key, higher_better in specs.items():
        scores = percentile_scores(rows, key, higher_better=higher_better)
        for row, score in zip(rows, scores, strict=False):
            row[f"{key}_rank"] = score


def weighted(values: list[tuple[float | None, float]]) -> float:
    total = 0.0
    weight_sum = 0.0
    for value, weight in values:
        if value is None:
            value = 0.5
        total += clamp(float(value)) * weight
        weight_sum += weight
    if weight_sum <= 0:
        return 0.5
    return clamp(total / weight_sum)


def inverse_rank(value: Any) -> float | None:
    parsed = round_or_none(value)
    if parsed is None:
        return None
    return 1.0 - clamp(parsed)


def enrich_base_rows(
    candidates: list[dict[str, Any]],
    market: dict[str, dict[str, Any]],
    model_details: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = normalize_cn_symbol(candidate.get("symbol"))
        if not symbol:
            continue
        model = model_details.get(symbol) or {}
        features = safe_json_loads(candidate.get("features_json") or model.get("features_json"))
        detail = safe_json_loads(candidate.get("detail_json") or model.get("detail_json"))
        nested = features.get("details") if isinstance(features.get("details"), dict) else {}
        combined = {
            **(market.get(symbol) or {}),
            "symbol": symbol,
            "name": candidate.get("name") or "",
            "industry": candidate.get("industry") or (market.get(symbol) or {}).get("industry") or "",
            "state": candidate.get("state") or "Execution Alpha",
            "policy": candidate.get("policy") or detail.get("strategy_family") or candidate.get("strategy_family") or "",
            "alpha_state": candidate.get("alpha_state") or model.get("alpha_state"),
            "action_intent": candidate.get("action_intent") or model.get("action_intent"),
            "strategy_key": candidate.get("strategy_key") or model.get("strategy_key"),
            "lifecycle_action": candidate.get("lifecycle_action") or "",
            "execution_mode": candidate.get("execution_mode") or detail.get("execution_mode") or features.get("execution_mode"),
            "observation_entry_zone": candidate.get("observation_entry_zone"),
            "handling_line": candidate.get("handling_line"),
            "first_target": candidate.get("first_target"),
            "time_exit": candidate.get("time_exit"),
            "reason": candidate.get("reason") or "",
            "ev_pct": first_number(candidate.get("ev_pct"), model.get("ev_pct")),
            "ev_lcb80_pct": first_number(candidate.get("ev_lcb80_pct"), candidate.get("ev_lcb_80_pct"), model.get("ev_lcb_80_pct")),
            "p_win_t1": first_number(candidate.get("p_win_t1")),
            "p_hit_1r_t3": first_number(candidate.get("p_hit_1r_t3")),
            "p_stop_t3": first_number(candidate.get("p_stop_t3")),
            "expected_r_t3": first_number(candidate.get("expected_r_t3")),
            "lcb80_r_t3": first_number(candidate.get("lcb80_r_t3")),
            "observed_probability_n": first_number(candidate.get("observed_probability_n")),
            "observed_probability_t3_n": first_number(candidate.get("observed_probability_t3_n")),
            "observed_probability_bucket": candidate.get("observed_probability_bucket"),
            "observed_probability_source": candidate.get("observed_probability_source"),
            "observed_lifecycle_tier": candidate.get("observed_lifecycle_tier"),
            "observed_lifecycle_qualified": bool(candidate.get("observed_lifecycle_qualified")),
            "observed_lifecycle_sleeve_id": candidate.get("observed_lifecycle_sleeve_id"),
            "observed_lifecycle_reason": candidate.get("observed_lifecycle_reason"),
            "suggested_hold_days": first_number(candidate.get("suggested_hold_days")),
            "risk_unit_pct": first_number(candidate.get("risk_unit_pct"), model.get("risk_unit_pct")),
            "strategy_samples": first_number(candidate.get("strategy_samples")),
            "strategy_fills": first_number(candidate.get("strategy_fills")),
            "denoise_residual_zscore": first_number(candidate.get("denoise_residual_zscore")),
            "log_return_20d_pct": first_number(candidate.get("log_return_20d_pct"), nested.get("ret_20d"), features.get("ret_20d")),
            "fft_signal_to_noise": first_number(candidate.get("fft_signal_to_noise")),
            "haar_noise_energy": first_number(candidate.get("haar_noise_energy")),
            "rsi_14": first_number(candidate.get("rsi_14"), features.get("rsi_14"), nested.get("rsi_14"), (market.get(symbol) or {}).get("db_rsi_14")),
            "setup_score": first_number(features.get("setup_score"), detail.get("setup_score"), nested.get("setup_score")),
            "entry_quality_score": first_number(nested.get("entry_quality_score")),
            "execution_score": first_number(nested.get("execution_score")),
            "shadow_alpha_prob": first_number(
                features.get("shadow_alpha_prob"),
                features.get("shadow_option_alpha_prob"),
                detail.get("shadow_alpha_prob"),
                nested.get("shadow_option_alpha_prob"),
                nested_get(nested, "shadow_option_alpha", "shadow_alpha_prob"),
            ),
            "stale_chase_risk": first_number(features.get("stale_chase_risk"), detail.get("stale_chase_risk"), nested.get("stale_chase_risk")),
            "fade_risk": first_number(features.get("fade_risk"), detail.get("fade_risk"), nested_get(nested, "shadow_option_alpha", "fade_risk")),
            "downside_stress": first_number(features.get("downside_stress"), detail.get("downside_stress"), nested.get("downside_stress")),
        }
        if combined.get("denoise_residual_zscore") is not None:
            combined["negative_residual"] = -float(combined["denoise_residual_zscore"])
        if combined.get("log_return_20d_pct") is not None:
            combined["negative_log20"] = -float(combined["log_return_20d_pct"])
        alpha_sleeve = candidate.get("alpha_sleeve_id") or alpha_factory_sleeve_id(combined)
        combined["alpha_sleeve_id"] = alpha_sleeve
        if alpha_sleeve:
            combined["execution_source"] = "alpha_factory_sleeve"
            combined["alpha_factory_role"] = "execution_sleeve"
        elif combined.get("observed_lifecycle_qualified"):
            combined["execution_source"] = "observed_lifecycle_prob"
            combined["alpha_factory_role"] = "observed_lifecycle_execution"
        else:
            combined["execution_source"] = "rank_only"
            combined["alpha_factory_role"] = "rank_only"
        rows.append(combined)
    return rows


def production_tier(rank: int, row: dict[str, Any], config: RankerConfig = DEFAULT_CONFIG) -> tuple[str, str, str]:
    headline_risk = round_or_none(row.get("headline_risk")) or 0.0
    falling_knife = round_or_none(row.get("falling_knife_score")) or 0.0
    alpha_sleeve = str(row.get("alpha_sleeve_id") or "")
    observed_qualified = bool(row.get("observed_lifecycle_qualified"))
    if is_special_treatment_name(row.get("name")):
        return (
            "special_treatment_watch",
            "special_treatment_no_probe",
            "0R until a dedicated ST/restructuring sleeve exists; ordinary oversold lifecycle buckets do not apply",
        )
    if headline_risk >= config.event_risk_zero_r:
        return (
            "event_risk_watch",
            "negative_headline_no_probe",
            "0R until financial/event headline clears and price stabilizes",
        )
    if falling_knife >= config.falling_knife_zero_r:
        return (
            "falling_knife_watch",
            "wait_for_flow_reversal",
            "0R default; only manual tiny after positive flow reversal",
        )
    if alpha_sleeve != CN_ALPHA_FACTORY_EXECUTION_SLEEVE and not observed_qualified:
        return (
            "ranked_watch",
            "rank_only_no_new_trade",
            "0R until Alpha Factory sleeve membership or observed lifecycle probability is present",
        )
    if alpha_sleeve != CN_ALPHA_FACTORY_EXECUTION_SLEEVE and observed_qualified:
        observed_tier = str(row.get("observed_lifecycle_tier") or "")
        if observed_tier == "observed_micro_probe" and rank <= config.secondary_probe_count:
            return (
                "observed_lifecycle_micro_probe",
                "pullback_observed_micro_probe",
                "0.02R/name after pullback/flow confirmation; observed mean positive but LCB weak",
            )
        if observed_tier == "observed_micro_probe":
            return (
                "observed_lifecycle_watch",
                "wait_for_rank_or_price_confirmation",
                "0R default; micro-probe evidence is not top-ranked",
            )
        if rank <= config.top_probe_count:
            return (
                "observed_lifecycle_probe",
                "planned_entry_observed_probe",
                "0.05R/name; observed historical analogs are positive; no chase above entry zone",
            )
        if rank <= config.secondary_probe_count:
            return (
                "observed_lifecycle_secondary",
                "pullback_observed_micro_probe",
                "0.03R/name after pullback/flow confirmation",
            )
        return (
            "observed_lifecycle_watch",
            "wait_for_rank_or_price_confirmation",
            "0R default; probability is positive but rank is not high enough",
        )
    if rank <= config.top_probe_count:
        return (
            "top_probe",
            "planned_entry_probe",
            "0.20R/name; top-5 basket <=1.00R; no chase above entry zone",
        )
    if rank <= config.secondary_probe_count:
        return (
            "secondary_probe",
            "pullback_or_intraday_confirmation_probe",
            "0.10R/name; use only after planned-entry/pullback touch",
        )
    if rank <= config.active_watch_count:
        return (
            "active_watch",
            "prepare_order_but_wait_for_price",
            "0.05R optional micro-probe after price confirms",
        )
    return (
        "bench_ranked",
        "watch_for_rotation",
        "no default size; keep ranked for tomorrow comparison",
    )


def score_rows(rows: list[dict[str, Any]], config: RankerConfig = DEFAULT_CONFIG) -> list[dict[str, Any]]:
    if not rows:
        return []
    add_percentiles(rows)
    for row in rows:
        probability_decile = round_or_none(row.get("probability_decile"))
        decile_score = None if probability_decile is None else clamp(probability_decile / 10.0)
        strategy_ev = weighted(
            [
                (row.get("ev_pct_rank"), 0.70),
                (row.get("ev_lcb80_pct_rank"), 0.20),
                (row.get("strategy_samples_rank"), 0.10),
            ]
        )
        observed_lifecycle = weighted(
            [
                (row.get("observed_lifecycle_score_rank"), 0.25),
                (row.get("expected_r_t3_rank"), 0.25),
                (row.get("lcb80_r_t3_rank"), 0.25),
                (row.get("p_hit_1r_t3_rank"), 0.12),
                (row.get("p_stop_t3_rank"), 0.08),
                (row.get("p_win_t1_rank"), 0.05),
            ]
        )
        oversold_reversion = weighted(
            [
                (row.get("negative_residual_rank"), 0.42),
                (row.get("negative_log20_rank"), 0.26),
                (row.get("rsi_14_rank"), 0.20),
                (row.get("mean_reversion_score_rank"), 0.12),
            ]
        )
        tushare_flow = weighted(
            [
                (row.get("flow_information_score_rank"), 0.32),
                (row.get("flow_large_flow_z_rank"), 0.18),
                (row.get("net_mf_pct_circ_mv_rank"), 0.22),
                (row.get("large_net_pct_circ_mv_rank"), 0.16),
                (row.get("rzye_5d_delta_pct_rank"), 0.12),
            ]
        )
        execution_quality = weighted(
            [
                (row.get("setup_score_rank"), 0.25),
                (row.get("entry_quality_score_rank"), 0.20),
                (row.get("shadow_alpha_prob_rank"), 0.20),
                (row.get("stale_chase_risk_rank"), 0.20),
                (row.get("fade_risk_rank"), 0.15),
            ]
        )
        limit_heat = weighted(
            [
                (row.get("p_touch_limit_rank"), 0.30),
                (row.get("p_limit_up_rank"), 0.22),
                (decile_score, 0.18),
                (row.get("limit_ev_after_cost_pct_rank"), 0.15),
                (row.get("p_failed_board_rank"), 0.15),
            ]
        )
        liquidity = weighted(
            [
                (row.get("amount_rank"), 0.36),
                (row.get("turnover_rate_rank"), 0.26),
                (row.get("volume_ratio_rank"), 0.20),
                (row.get("circ_mv_rank"), 0.18),
            ]
        )
        factor_lab = weighted(
            [
                (row.get("lab_composite_rank"), 0.60),
                (row.get("mean_reversion_score_rank"), 0.25),
                (row.get("continuation_score_db_rank"), 0.15),
            ]
        )
        sector_heat = weighted(
            [
                (row.get("sector_main_net_pct_rank"), 0.65),
                (row.get("sector_pct_chg_rank"), 0.35),
            ]
        )
        risk_penalty = weighted(
            [
                (inverse_rank(row.get("risk_unit_pct_rank")), 0.25),
                (inverse_rank(row.get("downside_stress_rank")), 0.25),
                (inverse_rank(row.get("p_failed_board_rank")), 0.20),
                (inverse_rank(row.get("fade_risk_rank")), 0.20),
                (inverse_rank(row.get("haar_noise_energy_rank")), 0.10),
            ]
        )
        headline_risk = clamp(float(round_or_none(row.get("headline_risk")) or 0.0))
        falling_knife = weighted(
            [
                (row.get("negative_log20_rank"), 0.30),
                (1.0 - tushare_flow, 0.30),
                (1.0 - execution_quality, 0.15),
                (headline_risk, 0.25),
            ]
        )
        row["falling_knife_score"] = round(falling_knife * 100.0, 2)
        raw = (
            config.score_weights["strategy_ev"] * strategy_ev
            + config.score_weights["observed_lifecycle"] * observed_lifecycle
            + config.score_weights["tushare_flow"] * tushare_flow
            + config.score_weights["oversold_reversion"] * oversold_reversion
            + config.score_weights["execution_quality"] * execution_quality
            + config.score_weights["limit_heat"] * limit_heat
            + config.score_weights["liquidity"] * liquidity
            + config.score_weights["factor_lab"] * factor_lab
            + config.score_weights["sector_heat"] * sector_heat
            + config.score_weights["risk_penalty"] * risk_penalty
            + config.score_weights["falling_knife"] * falling_knife
            + config.score_weights["headline_risk"] * headline_risk
        )
        row["score_components"] = {
            "strategy_ev": round(strategy_ev * 100.0, 2),
            "observed_lifecycle": round(observed_lifecycle * 100.0, 2),
            "tushare_flow": round(tushare_flow * 100.0, 2),
            "oversold_reversion": round(oversold_reversion * 100.0, 2),
            "execution_quality": round(execution_quality * 100.0, 2),
            "limit_heat": round(limit_heat * 100.0, 2),
            "liquidity": round(liquidity * 100.0, 2),
            "factor_lab": round(factor_lab * 100.0, 2),
            "sector_heat": round(sector_heat * 100.0, 2),
            "risk_penalty": round(risk_penalty * 100.0, 2),
            "falling_knife": round(falling_knife * 100.0, 2),
            "headline_risk": round(headline_risk * 100.0, 2),
        }
        row["rank_score"] = round(clamp(raw) * 100.0, 2)

    rows.sort(
        key=lambda row: (
            -(round_or_none(row.get("rank_score")) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    for idx, row in enumerate(rows, start=1):
        tier, action, size = production_tier(idx, row, config)
        row["rank"] = idx
        row["production_tier"] = tier
        row["production_action"] = action
        row["size_hint"] = size
    return rows


def public_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "rank",
        "rank_score",
        "production_tier",
        "production_action",
        "size_hint",
        "symbol",
        "name",
        "industry",
        "state",
        "policy",
        "alpha_sleeve_id",
        "alpha_factory_role",
        "execution_source",
        "alpha_state",
        "action_intent",
        "execution_mode",
        "lifecycle_action",
        "ev_pct",
        "ev_lcb80_pct",
        "p_win_t1",
        "p_hit_1r_t3",
        "p_stop_t3",
        "expected_r_t3",
        "lcb80_r_t3",
        "observed_probability_n",
        "observed_probability_t3_n",
        "observed_probability_source",
        "observed_probability_bucket",
        "observed_lifecycle_tier",
        "observed_lifecycle_qualified",
        "observed_lifecycle_sleeve_id",
        "observed_lifecycle_reason",
        "suggested_hold_days",
        "risk_unit_pct",
        "strategy_samples",
        "strategy_fills",
        "denoise_residual_zscore",
        "log_return_20d_pct",
        "rsi_14",
        "flow_information_score",
        "flow_large_flow_z",
        "net_mf_pct_circ_mv",
        "large_net_pct_circ_mv",
        "rzye_5d_delta_pct",
        "p_touch_limit",
        "p_limit_up",
        "p_failed_board",
        "probability_decile",
        "turnover_rate",
        "volume_ratio",
        "circ_mv",
        "amount",
        "lab_composite",
        "mean_reversion_score",
        "sector_main_net_pct",
        "sector_pct_chg",
        "headline_risk",
        "headline_risk_level",
        "headline_flags",
        "latest_headline_date",
        "latest_headline",
        "headline_hits",
        "falling_knife_score",
        "observation_entry_zone",
        "handling_line",
        "first_target",
        "time_exit",
        "reason",
        "score_components",
    ]
    out = {key: row.get(key) for key in keys if key in row}
    for key, value in list(out.items()):
        if hasattr(value, "isoformat"):
            out[key] = value.isoformat()
        elif isinstance(value, float):
            out[key] = round_or_none(value)
    return out


def render_markdown(payload: dict[str, Any]) -> str:
    rows = payload.get("top_rows") or []
    lines = [
        f"# CN Opportunity Ranker - {payload['as_of']}",
        "",
        "生产版：Alpha Factory sleeve 和 `cn_observed_lifecycle_prob` 都可以产出小仓/微仓 Execution Alpha；其他 oversold 只做 ranked watch。财报造假、虚假陈述、留置调查等事件风险会直接降级为 0R 观察。",
        "",
        "## Data",
        "",
        f"- candidates: {payload.get('candidate_count', 0)}",
        f"- source_report: `{payload.get('source_report') or '-'}`",
        f"- cn_db: `{payload.get('cn_db') or '-'}`",
        "- Tushare fields used: daily_basic, moneyflow, margin_detail, sector_fund_flow, limit_up_model_predictions",
        "- News fields used: news_enriched, stock_news",
        "",
        "## Production Basket",
        "",
        "| Rank | Symbol | Name | Source | Tier | Action | Size | Entry | ExpR | LCBR | n | Score |",
        "|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|",
    ]
    basket_rows = [
        row
        for row in rows
        if (
            "probe" in str(row.get("production_tier") or "")
            or "probe" in str(row.get("production_action") or "")
        )
        and not str(row.get("size_hint") or "").startswith("0R")
    ]
    for row in basket_rows[:10]:
        lines.append(
            "| {rank} | {symbol} | {name} | {source} | {tier} | {action} | {size} | {entry} | {expr} | {lcbr} | {n} | {score:.2f} |".format(
                rank=row.get("rank"),
                symbol=row.get("symbol") or "",
                name=row.get("name") or "",
                source=row.get("alpha_sleeve_id") or row.get("observed_lifecycle_sleeve_id") or "rank_only",
                tier=row.get("production_tier") or "",
                action=row.get("production_action") or "",
                size=row.get("size_hint") or "",
                entry=row.get("observation_entry_zone") or "-",
                expr=fmt_num(row.get("expected_r_t3"), 2),
                lcbr=fmt_num(row.get("lcb80_r_t3"), 2),
                n=row.get("observed_probability_n") or "-",
                score=round_or_none(row.get("rank_score")) or 0.0,
            )
        )
    if not basket_rows:
        lines.append("| - | - | - | - | no production basket today | - | 0R | - | - | - | - | 0.00 |")
    lines += [
        "",
        "## Top Ranked",
        "",
        "| Rank | Symbol | Name | Industry | Score | Source | Tier | ExpR | LCBR | Hit | Stop | n | Headline | Knife | EV | LCB80 | Flow | Reason |",
        "|---:|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        tier = str(row.get("production_tier") or "")
        if tier == "special_treatment_watch":
            reason_value = row.get("size_hint") or row.get("reason")
        elif tier.startswith("observed_lifecycle"):
            reason_value = row.get("observed_lifecycle_reason") or row.get("size_hint") or row.get("reason")
        else:
            reason_value = row.get("reason") or row.get("size_hint")
        reason = str(reason_value or "").replace("|", "/")
        if len(reason) > 70:
            reason = reason[:67] + "..."
        lines.append(
            "| {rank} | {symbol} | {name} | {industry} | {score:.2f} | {source} | {tier} | {expr} | {lcbr} | {hit} | {stop} | {n} | {headline} | {knife} | {ev} | {lcb} | {flow} | {reason} |".format(
                rank=row.get("rank"),
                symbol=row.get("symbol") or "",
                name=row.get("name") or "",
                industry=row.get("industry") or "",
                score=round_or_none(row.get("rank_score")) or 0.0,
                source=row.get("alpha_sleeve_id") or row.get("observed_lifecycle_sleeve_id") or "rank_only",
                tier=row.get("production_tier") or "",
                expr=fmt_num(row.get("expected_r_t3"), 2),
                lcbr=fmt_num(row.get("lcb80_r_t3"), 2),
                hit=fmt_num(None if row.get("p_hit_1r_t3") is None else float(row.get("p_hit_1r_t3")) * 100.0, 0),
                stop=fmt_num(None if row.get("p_stop_t3") is None else float(row.get("p_stop_t3")) * 100.0, 0),
                n=row.get("observed_probability_n") or "-",
                headline=fmt_num((round_or_none(row.get("headline_risk")) or 0.0) * 100.0, 0),
                knife=fmt_num(row.get("falling_knife_score"), 0),
                ev=fmt_pct(row.get("ev_pct")),
                lcb=fmt_pct(row.get("ev_lcb80_pct")),
                flow=fmt_num(row.get("flow_information_score"), 2),
                reason=reason,
            )
        )
    lines += [
        "",
        "## Score Weights",
        "",
        "| Component | Weight |",
        "|---|---:|",
    ]
    for key, value in (payload.get("score_weights") or {}).items():
        lines.append(f"| {key} | {value:+.2f} |")
    lines += [
        "",
        "## Operating Rule",
        "",
        "- `cn_oversold_ev_positive` remains the proven Alpha Factory sleeve.",
        "- `cn_observed_lifecycle_prob` rows can become tiny observed probes when historical analog probability is positive.",
        "- Top 5 sleeve rows: planned-entry probe; top-5 basket default <= 1R.",
        "- Rank 6-10 sleeve rows: only after pullback or intraday confirmation.",
        "- Non-sleeve oversold rows remain ranked watch with 0R default size.",
        "- This is a ranker, not a broker adapter; order execution still needs live price and account checks.",
    ]
    return "\n".join(lines) + "\n"


def write_duckdb(path: Path, rows: list[dict[str, Any]], as_of: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS cn_opportunity_ranker")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cn_opportunity_ranker (
                as_of DATE,
                rank INTEGER,
                symbol VARCHAR,
                name VARCHAR,
                industry VARCHAR,
                rank_score DOUBLE,
                alpha_sleeve_id VARCHAR,
                observed_lifecycle_sleeve_id VARCHAR,
                execution_source VARCHAR,
                alpha_factory_role VARCHAR,
                production_tier VARCHAR,
                production_action VARCHAR,
                size_hint VARCHAR,
                expected_r_t3 DOUBLE,
                lcb80_r_t3 DOUBLE,
                p_win_t1 DOUBLE,
                p_hit_1r_t3 DOUBLE,
                p_stop_t3 DOUBLE,
                observed_probability_n INTEGER,
                ev_pct DOUBLE,
                ev_lcb80_pct DOUBLE,
                denoise_residual_zscore DOUBLE,
                log_return_20d_pct DOUBLE,
                flow_information_score DOUBLE,
                net_mf_pct_circ_mv DOUBLE,
                p_touch_limit DOUBLE,
                turnover_rate DOUBLE,
                score_components_json VARCHAR
            )
            """
        )
        con.execute("DELETE FROM cn_opportunity_ranker WHERE as_of = CAST(? AS DATE)", [as_of.isoformat()])
        con.executemany(
            """
            INSERT INTO cn_opportunity_ranker VALUES (
                CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                [
                    as_of.isoformat(),
                    row.get("rank"),
                    row.get("symbol"),
                    row.get("name"),
                    row.get("industry"),
                    row.get("rank_score"),
                    row.get("alpha_sleeve_id"),
                    row.get("observed_lifecycle_sleeve_id"),
                    row.get("execution_source"),
                    row.get("alpha_factory_role"),
                    row.get("production_tier"),
                    row.get("production_action"),
                    row.get("size_hint"),
                    row.get("expected_r_t3"),
                    row.get("lcb80_r_t3"),
                    row.get("p_win_t1"),
                    row.get("p_hit_1r_t3"),
                    row.get("p_stop_t3"),
                    row.get("observed_probability_n"),
                    row.get("ev_pct"),
                    row.get("ev_lcb80_pct"),
                    row.get("denoise_residual_zscore"),
                    row.get("log_return_20d_pct"),
                    row.get("flow_information_score"),
                    row.get("net_mf_pct_circ_mv"),
                    row.get("p_touch_limit"),
                    row.get("turnover_rate"),
                    json.dumps(row.get("score_components") or {}, ensure_ascii=False, sort_keys=True),
                ]
                for row in rows
            ],
        )
    finally:
        con.close()


def build_ranker_payload(
    *,
    as_of: date,
    candidates: list[dict[str, Any]],
    candidate_status: str,
    cn_db: Path,
    source_report: str | None = None,
    top: int = 30,
    config: RankerConfig = DEFAULT_CONFIG,
) -> dict[str, Any]:
    symbols = sorted({normalize_cn_symbol(row.get("symbol")) for row in candidates if normalize_cn_symbol(row.get("symbol"))})
    industry_by_symbol = {
        normalize_cn_symbol(row.get("symbol")): str(row.get("industry") or "")
        for row in candidates
        if normalize_cn_symbol(row.get("symbol"))
    }

    market: dict[str, dict[str, Any]] = {}
    model_details: dict[str, dict[str, Any]] = {}
    if cn_db.exists() and symbols:
        con = duckdb.connect(str(cn_db), read_only=True)
        try:
            model_details = load_strategy_details(con, symbols, as_of)
        finally:
            con.close()
        market = load_market_data(cn_db, symbols, industry_by_symbol, as_of, config)

    base_rows = enrich_base_rows(candidates, market, model_details)
    ranked = score_rows(base_rows, config)
    public_rows = [public_row(row) for row in ranked]
    top_n = max(1, int(top or 30))
    payload = {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "production_opportunity_ranker",
        "candidate_status": candidate_status,
        "candidate_count": len(candidates),
        "ranked_count": len(public_rows),
        "source_report": source_report,
        "cn_db": str(cn_db),
        "score_config": asdict(config),
        "score_weights": config.score_weights,
        "notes": [
            "Alpha Factory sleeve membership is the execution contract; broad oversold rows are rank-only.",
            "Headline/event risk and falling-knife risk can force 0R action tier.",
            "Designed for 200-point Tushare daily data: no minute-data or top_list dependency.",
        ],
        "production_basket": [row for row in public_rows if row.get("rank", 999) <= 10],
        "top_rows": public_rows[:top_n],
        "all_rows": public_rows,
    }
    return payload


def write_ranker_outputs(payload: dict[str, Any], output_root: Path) -> Path:
    as_of = parse_date(str(payload["as_of"]))
    output_dir = output_root / as_of.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "cn_opportunity_ranker.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "cn_opportunity_ranker.md").write_text(render_markdown(payload), encoding="utf-8")
    write_duckdb(output_dir / "cn_opportunity_ranker.duckdb", payload.get("all_rows") or [], as_of)
    return output_dir


def run(args: argparse.Namespace) -> dict[str, Any]:
    as_of = parse_date(args.date)
    config = load_ranker_config(args.config)
    candidates, candidate_status, source_report = load_v2_candidates(args.v2_root, as_of)
    if not candidates:
        candidates, candidate_status = load_db_candidates(args.cn_db, as_of)
        source_report = None
    payload = build_ranker_payload(
        as_of=as_of,
        candidates=candidates,
        candidate_status=candidate_status,
        cn_db=args.cn_db,
        source_report=source_report,
        top=args.top,
        config=config,
    )
    write_ranker_outputs(payload, args.output_root)
    return payload


def main() -> None:
    payload = run(parse_args())
    top = payload.get("top_rows") or []
    print(
        json.dumps(
            {
                "as_of": payload["as_of"],
                "ranked_count": payload["ranked_count"],
                "top5": [
                    {
                        "rank": row.get("rank"),
                        "symbol": row.get("symbol"),
                        "name": row.get("name"),
                        "score": row.get("rank_score"),
                        "tier": row.get("production_tier"),
                    }
                    for row in top[:5]
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
