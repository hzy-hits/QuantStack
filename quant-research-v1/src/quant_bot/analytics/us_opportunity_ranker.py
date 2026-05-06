#!/usr/bin/env python3
"""Production ranker for current US opportunities.

This ranks the rows produced by Main Strategy V2 and makes the production
contract explicit: only the Alpha Factory-proven V2 stock sleeve can become an
Execution Alpha row. Legacy report buckets remain ranked watch until a sleeve
backtest promotes them.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "us_opportunity_ranker"
US_ALPHA_FACTORY_EXECUTION_SLEEVE = "us_v2_stock_probe"


@dataclass(frozen=True)
class NewsRiskConfig:
    lookback_days: int = 14
    severe_threshold: float = 0.60
    severe_terms: tuple[str, ...] = (
        "fraud",
        "restatement",
        "accounting issue",
        "sec investigation",
        "doj investigation",
        "class action",
        "bankruptcy",
        "going concern",
        "delisting",
        "halted",
    )
    negative_terms: tuple[str, ...] = (
        "downgrade",
        "misses",
        "weak guidance",
        "guidance cut",
        "lawsuit",
        "probe",
        "investigation",
        "short report",
        "cuts target",
    )


@dataclass(frozen=True)
class RankerConfig:
    score_weights: dict[str, float] = field(
        default_factory=lambda: {
            "alpha_factory": 0.30,
            "setup_quality": 0.22,
            "flow_options_quality": 0.22,
            "price_quality": 0.12,
            "risk_penalty": -0.14,
            "headline_risk": -0.20,
        }
    )
    headline: NewsRiskConfig = field(default_factory=NewsRiskConfig)
    top_probe_count: int = 5
    secondary_probe_count: int = 10
    event_risk_zero_r: float = 0.60


DEFAULT_CONFIG = RankerConfig()


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


def table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    if not table_exists(con, table):
        return set()
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {str(row[0]) for row in rows}


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def placeholders(values: list[str]) -> str:
    return ",".join("?" for _ in values)


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


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


def latest_options(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "options_alpha"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        WITH latest AS (
            SELECT symbol, MAX(as_of) AS latest_date
            FROM options_alpha
            WHERE as_of <= CAST(? AS DATE)
              AND symbol IN ({placeholders(symbols)})
            GROUP BY symbol
        )
        SELECT oa.symbol, oa.as_of, oa.directional_edge, oa.vol_edge, oa.vrp_edge,
               oa.flow_edge, oa.liquidity_gate, oa.expression, oa.reason, oa.detail_json
        FROM options_alpha oa
        JOIN latest l ON l.symbol = oa.symbol AND l.latest_date = oa.as_of
        """,
        [as_of.isoformat(), *symbols],
    )
    return {normalize_symbol(row.get("symbol")): row for row in rows}


def price_features(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date) -> dict[str, dict[str, Any]]:
    if not symbols or not table_exists(con, "prices_daily"):
        return {}
    rows = rows_as_dicts(
        con,
        f"""
        SELECT symbol, date, close, volume
        FROM prices_daily
        WHERE date <= CAST(? AS DATE)
          AND date >= CAST(? AS DATE)
          AND symbol IN ({placeholders(symbols)})
          AND close IS NOT NULL
        ORDER BY symbol, date
        """,
        [(as_of).isoformat(), (as_of - timedelta(days=45)).isoformat(), *symbols],
    )
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(normalize_symbol(row.get("symbol")), []).append(row)
    out: dict[str, dict[str, Any]] = {}
    for symbol, series in by_symbol.items():
        closes = [float(row["close"]) for row in series if row.get("close") is not None]
        if not closes:
            continue
        latest = closes[-1]
        ret_5d = None if len(closes) < 6 or closes[-6] == 0 else (latest / closes[-6] - 1.0) * 100.0
        ret_20d = None if len(closes) < 21 or closes[-21] == 0 else (latest / closes[-21] - 1.0) * 100.0
        out[symbol] = {
            "price_as_of": as_iso(series[-1].get("date")),
            "close": round_or_none(latest, 4),
            "ret_5d_pct": round_or_none(ret_5d),
            "ret_20d_pct": round_or_none(ret_20d),
            "volume": round_or_none(series[-1].get("volume")),
        }
    return out


def recent_news(con: duckdb.DuckDBPyConnection, symbols: list[str], as_of: date, config: NewsRiskConfig) -> dict[str, list[dict[str, Any]]]:
    if not symbols or not table_exists(con, "news_items"):
        return {}
    cols = table_columns(con, "news_items")
    symbol_col = "symbol" if "symbol" in cols else ""
    headline_col = "headline" if "headline" in cols else "title" if "title" in cols else ""
    published_col = "published_at" if "published_at" in cols else "date" if "date" in cols else ""
    summary_col = "summary" if "summary" in cols else ""
    source_col = "source" if "source" in cols else ""
    if not symbol_col or not headline_col or not published_col:
        return {}
    summary_select = f"{summary_col} AS summary" if summary_col else "NULL AS summary"
    source_select = f"{source_col} AS source" if source_col else "NULL AS source"
    rows = rows_as_dicts(
        con,
        f"""
        SELECT {symbol_col} AS symbol, {headline_col} AS headline,
               {summary_select}, {source_select}, {published_col} AS published_at
        FROM news_items
        WHERE {published_col} >= CAST(? AS TIMESTAMP)
          AND {published_col} < CAST(? AS TIMESTAMP)
          AND {symbol_col} IN ({placeholders(symbols)})
        ORDER BY {published_col} DESC
        """,
        [
            (as_of - timedelta(days=config.lookback_days)).isoformat(),
            (as_of + timedelta(days=1)).isoformat(),
            *symbols,
        ],
    )
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(normalize_symbol(row.get("symbol")), []).append(row)
    return out


def headline_risk(items: list[dict[str, Any]], config: NewsRiskConfig) -> dict[str, Any]:
    risk = 0.0
    flags: list[str] = []
    latest_headline = ""
    latest_date = None
    for item in items:
        headline = str(item.get("headline") or "")
        if not latest_headline:
            latest_headline = headline
            latest_date = as_iso(item.get("published_at"))
        text = f"{headline} {item.get('summary') or ''}".lower()
        severe_hits = [term for term in config.severe_terms if term in text]
        negative_hits = [term for term in config.negative_terms if term in text]
        if severe_hits:
            risk = max(risk, 0.82)
            flags.extend(severe_hits)
        elif negative_hits:
            risk = max(risk, 0.48)
            flags.extend(negative_hits)
    return {
        "headline_risk": round(risk, 4),
        "headline_flags": sorted(set(flags)),
        "latest_headline": latest_headline,
        "latest_headline_date": latest_date,
    }


def option_quality(row: dict[str, Any] | None) -> tuple[float, str]:
    if not row:
        return 50.0, "missing_options_alpha"
    expression = str(row.get("expression") or "").lower()
    liquidity = str(row.get("liquidity_gate") or "").lower()
    directional = round_or_none(row.get("directional_edge")) or 0.0
    vol_edge = round_or_none(row.get("vol_edge")) or 0.0
    flow_edge = round_or_none(row.get("flow_edge")) or 0.0
    liquidity_score = 1.0 if liquidity == "pass" else 0.35
    expression_score = 1.0 if expression in {"call_spread", "stock_long"} else 0.45
    edge_score = clamp((directional + max(vol_edge, 0.0) + max(flow_edge, 0.0)) / 1.5)
    score = (0.32 * liquidity_score + 0.30 * expression_score + 0.38 * edge_score) * 100.0
    reason = f"{expression or 'missing'}; liquidity={liquidity or '-'}; dir={directional:.2f}; vol={vol_edge:.2f}; flow={flow_edge:.2f}"
    return round(score, 2), reason


def setup_quality(row: dict[str, Any]) -> float:
    rr = round_or_none(row.get("rr_ratio"))
    expected = round_or_none(row.get("expected_move_pct"))
    confidence = str(row.get("signal_confidence") or "").upper()
    rr_score = 0.5 if rr is None else clamp(rr / 3.0)
    expected_score = 0.5 if expected is None else clamp(expected / 8.0)
    confidence_score = {"LOW": 0.75, "MODERATE": 0.55, "HIGH": 0.45}.get(confidence, 0.50)
    return 0.52 * rr_score + 0.28 * expected_score + 0.20 * confidence_score


def price_quality(row: dict[str, Any]) -> float:
    ret_5d = round_or_none(row.get("ret_5d_pct"))
    ret_20d = round_or_none(row.get("ret_20d_pct"))
    if ret_5d is None and ret_20d is None:
        return 0.5
    ret5_score = 0.5 if ret_5d is None else clamp((ret_5d + 4.0) / 12.0)
    ret20_score = 0.5 if ret_20d is None else clamp((ret_20d + 8.0) / 24.0)
    extension_penalty = 0.15 if (ret_5d is not None and ret_5d > 10.0) or (ret_20d is not None and ret_20d > 25.0) else 0.0
    return clamp(0.55 * ret5_score + 0.45 * ret20_score - extension_penalty)


def risk_penalty(row: dict[str, Any]) -> float:
    blockers = [str(item).lower() for item in (row.get("blockers") or []) if item]
    rr = round_or_none(row.get("rr_ratio"))
    penalty = 0.0
    if rr is not None and rr < 1.5:
        penalty += 0.25
    if row.get("stop") is None:
        penalty += 0.18
    for marker in ("stale", "already_paid", "exhaustion", "rr_below"):
        if any(marker in item for item in blockers):
            penalty += 0.16
    return clamp(penalty)


def enrich_rows(
    candidates: list[dict[str, Any]],
    options: dict[str, dict[str, Any]],
    prices: dict[str, dict[str, Any]],
    news: dict[str, list[dict[str, Any]]],
    config: RankerConfig,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = normalize_symbol(candidate.get("symbol"))
        if not symbol:
            continue
        option_row = options.get(symbol)
        option_score, option_reason = option_quality(option_row)
        combined = {
            **(prices.get(symbol) or {}),
            **dict(candidate),
            "symbol": symbol,
            "alpha_sleeve_id": candidate.get("alpha_sleeve_id"),
            "alpha_factory_role": candidate.get("alpha_factory_role") or ("execution_sleeve" if candidate.get("alpha_sleeve_id") else "rank_only"),
            "options_quality": option_score,
            "flow_options_quality": option_score,
            "options_quality_reason": option_reason,
            "option_expression": (option_row or {}).get("expression") or candidate.get("option_expression"),
        }
        combined.update(headline_risk(news.get(symbol) or [], config.headline))
        rows.append(combined)
    return rows


def production_tier(rank: int, row: dict[str, Any], config: RankerConfig) -> tuple[str, str, str]:
    headline = round_or_none(row.get("headline_risk")) or 0.0
    if headline >= config.event_risk_zero_r:
        return "event_risk_watch", "negative_headline_no_probe", "0R until event/news risk clears"
    if row.get("alpha_sleeve_id") != US_ALPHA_FACTORY_EXECUTION_SLEEVE:
        return "ranked_watch", "rank_only_no_new_trade", "0R until Alpha Factory sleeve promotion"
    options_q = round_or_none(row.get("options_quality")) or 0.0
    action = "option_or_stock_probe" if options_q >= 65.0 else "stock_probe"
    if rank <= config.top_probe_count:
        return "top_probe", action, "0.25R/name; basket cap set by portfolio overlay"
    if rank <= config.secondary_probe_count:
        return "secondary_probe", action, "0.10R/name after pullback/retest confirmation"
    return "active_watch", "prepare_order_but_wait_for_price", "0R default unless price confirms"


def score_rows(rows: list[dict[str, Any]], config: RankerConfig = DEFAULT_CONFIG) -> list[dict[str, Any]]:
    for row in rows:
        alpha_score = 1.0 if row.get("alpha_sleeve_id") == US_ALPHA_FACTORY_EXECUTION_SLEEVE else 0.15
        setup = setup_quality(row)
        options_q = clamp((round_or_none(row.get("flow_options_quality")) or 50.0) / 100.0)
        price = price_quality(row)
        penalty = risk_penalty(row)
        headline = clamp(round_or_none(row.get("headline_risk")) or 0.0)
        raw = (
            config.score_weights["alpha_factory"] * alpha_score
            + config.score_weights["setup_quality"] * setup
            + config.score_weights["flow_options_quality"] * options_q
            + config.score_weights["price_quality"] * price
            + config.score_weights["risk_penalty"] * penalty
            + config.score_weights["headline_risk"] * headline
        )
        row["score_components"] = {
            "alpha_factory": round(alpha_score * 100.0, 2),
            "setup_quality": round(setup * 100.0, 2),
            "flow_options_quality": round(options_q * 100.0, 2),
            "price_quality": round(price * 100.0, 2),
            "risk_penalty": round(penalty * 100.0, 2),
            "headline_risk": round(headline * 100.0, 2),
        }
        row["rank_score"] = round(clamp(raw) * 100.0, 2)
    rows.sort(key=lambda row: (-(round_or_none(row.get("rank_score")) or 0.0), str(row.get("symbol") or "")))
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
        "state",
        "policy",
        "alpha_sleeve_id",
        "alpha_factory_role",
        "entry",
        "stop",
        "target",
        "rr_ratio",
        "expected_move_pct",
        "option_expression",
        "options_quality",
        "flow_options_quality",
        "options_quality_reason",
        "trend_regime",
        "signal_confidence",
        "execution_mode",
        "headline_risk",
        "headline_flags",
        "latest_headline_date",
        "latest_headline",
        "ret_5d_pct",
        "ret_20d_pct",
        "blockers",
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
        f"# US Opportunity Ranker - {payload['as_of']}",
        "",
        "Production contract: only `us_v2_stock_probe` can emit Execution Alpha. Legacy HIGH/MOD rows remain ranked watch until Alpha Factory promotes them.",
        "",
        "| Rank | Symbol | Sleeve | Tier | Action | Score | Headline | Options/Flow | R:R | Trend | Why |",
        "|---:|---|---|---|---|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        why = str(row.get("reason") or row.get("options_quality_reason") or "").replace("|", "/")
        if len(why) > 70:
            why = why[:67] + "..."
        lines.append(
            "| {rank} | {symbol} | {sleeve} | {tier} | {action} | {score} | {headline} | {options} | {rr} | {trend} | {why} |".format(
                rank=row.get("rank"),
                symbol=row.get("symbol") or "",
                sleeve=row.get("alpha_sleeve_id") or "rank_only",
                tier=row.get("production_tier") or "",
                action=row.get("production_action") or "",
                score=fmt_num(row.get("rank_score")),
                headline=fmt_num((round_or_none(row.get("headline_risk")) or 0.0) * 100.0, 0),
                options=fmt_num(row.get("flow_options_quality"), 0),
                rr=fmt_num(row.get("rr_ratio"), 2),
                trend=row.get("trend_regime") or "-",
                why=why,
            )
        )
    lines += [
        "",
        "## Operating Rule",
        "",
        "- Top sleeve rows can be probed as stock or option/stock expression depending on options quality.",
        "- Legacy rows and non-sleeve rows are ranked watch with 0R default size.",
        "- Event/news risk forces 0R even when the sleeve is otherwise valid.",
    ]
    return "\n".join(lines) + "\n"


def write_duckdb(path: Path, rows: list[dict[str, Any]], as_of: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS us_opportunity_ranker")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS us_opportunity_ranker (
                as_of DATE, rank INTEGER, symbol VARCHAR, rank_score DOUBLE,
                alpha_sleeve_id VARCHAR, alpha_factory_role VARCHAR,
                production_tier VARCHAR, production_action VARCHAR, size_hint VARCHAR,
                headline_risk DOUBLE, options_quality DOUBLE,
                rr_ratio DOUBLE, trend_regime VARCHAR, payload_json VARCHAR
            )
            """
        )
        con.execute("DELETE FROM us_opportunity_ranker WHERE as_of = CAST(? AS DATE)", [as_of.isoformat()])
        con.executemany(
            """
            INSERT INTO us_opportunity_ranker VALUES (
                CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                [
                    as_of.isoformat(),
                    row.get("rank"),
                    row.get("symbol"),
                    row.get("rank_score"),
                    row.get("alpha_sleeve_id"),
                    row.get("alpha_factory_role"),
                    row.get("production_tier"),
                    row.get("production_action"),
                    row.get("size_hint"),
                    row.get("headline_risk"),
                    row.get("options_quality"),
                    row.get("rr_ratio"),
                    row.get("trend_regime"),
                    json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
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
    us_db: Path,
    source_report: str | None = None,
    top: int = 30,
    config: RankerConfig = DEFAULT_CONFIG,
) -> dict[str, Any]:
    symbols = sorted({normalize_symbol(row.get("symbol")) for row in candidates if normalize_symbol(row.get("symbol"))})
    options: dict[str, dict[str, Any]] = {}
    prices: dict[str, dict[str, Any]] = {}
    news: dict[str, list[dict[str, Any]]] = {}
    if us_db.exists() and symbols:
        con = duckdb.connect(str(us_db), read_only=True)
        try:
            options = latest_options(con, symbols, as_of)
            prices = price_features(con, symbols, as_of)
            news = recent_news(con, symbols, as_of, config.headline)
        finally:
            con.close()
    ranked = score_rows(enrich_rows(candidates, options, prices, news, config), config)
    public_rows = [public_row(row) for row in ranked]
    top_n = max(1, int(top or 30))
    return {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "production_opportunity_ranker",
        "candidate_status": candidate_status,
        "candidate_count": len(candidates),
        "ranked_count": len(public_rows),
        "source_report": source_report,
        "us_db": str(us_db),
        "score_config": asdict(config),
        "score_weights": config.score_weights,
        "notes": [
            "Alpha Factory sleeve membership is the execution contract.",
            "Options/flow quality controls expression choice, not legacy promotion.",
            "Headline/event risk forces 0R watch.",
        ],
        "production_basket": [row for row in public_rows if row.get("rank", 999) <= 10],
        "top_rows": public_rows[:top_n],
        "all_rows": public_rows,
    }


def write_ranker_outputs(payload: dict[str, Any], output_root: Path) -> Path:
    as_of = datetime.strptime(str(payload["as_of"]), "%Y-%m-%d").date()
    output_dir = output_root / as_of.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "us_opportunity_ranker.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (output_dir / "us_opportunity_ranker.md").write_text(render_markdown(payload), encoding="utf-8")
    write_duckdb(output_dir / "us_opportunity_ranker.duckdb", payload.get("all_rows") or [], as_of)
    return output_dir
