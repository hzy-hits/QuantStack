"""US theme-cluster basket sleeve and guardrails."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import yaml


US_THEME_SLEEVE_ID = "us_theme_cluster_momentum"
THEME_METADATA_FIELDS = (
    "supercycle_layer",
    "supercycle_priority",
    "supply_chain_role",
    "bottleneck_focus",
    "evidence_contract",
    "research_index",
)


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name=?
            """,
            [table],
        ).fetchone()[0]
        > 0
    )


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def _round_or_none(value: Any, digits: int = 6) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return round(parsed, digits)


def _fmt_num(value: Any, digits: int = 2) -> str:
    parsed = _round_or_none(value, digits)
    return "-" if parsed is None else f"{parsed:.{digits}f}"


def _fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = _round_or_none(value, digits)
    return "-" if parsed is None else f"{parsed:+.{digits}f}%"


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(item).strip() for item in value if str(item or "").strip())
    if isinstance(value, dict):
        return yaml.safe_dump(value, allow_unicode=True, default_flow_style=True).strip()
    return str(value).strip()


def _theme_priority(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 3
    return max(1, min(parsed, 9))


def theme_breadth(member_returns_pct: dict[str, float]) -> float:
    if not member_returns_pct:
        return 0.0
    winners = sum(1 for value in member_returns_pct.values() if float(value) > 0.0)
    return winners / len(member_returns_pct)


def is_promotable_theme_basket(
    members: list[str],
    member_returns_pct: dict[str, float],
    *,
    min_members: int = 3,
    min_positive_members: int = 2,
    min_breadth: float = 0.60,
) -> bool:
    unique_members = {str(member).upper() for member in members if str(member or "").strip()}
    positive_members = {
        str(symbol).upper()
        for symbol, value in member_returns_pct.items()
        if str(symbol).upper() in unique_members and float(value) > 0.0
    }
    if len(unique_members) < min_members:
        return False
    if len(positive_members) < min_positive_members:
        return False
    scoped_returns = {symbol: member_returns_pct[symbol] for symbol in member_returns_pct if symbol.upper() in unique_members}
    return theme_breadth(scoped_returns) >= min_breadth


def theme_payload_is_promotable(payload: dict[str, Any]) -> bool:
    return is_promotable_theme_basket(
        list(payload.get("members") or []),
        dict(payload.get("member_returns_pct") or {}),
    )


def load_us_theme_seed_map(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    themes: list[dict[str, Any]] = []
    for item in payload.get("themes") or []:
        members = [str(symbol).upper() for symbol in item.get("members") or [] if str(symbol or "").strip()]
        if len(set(members)) < 2:
            continue
        themes.append(
            {
                "theme_id": str(item.get("theme_id") or "").strip(),
                "label": str(item.get("label") or item.get("theme_id") or "").strip(),
                "benchmark": str(item.get("benchmark") or "SPY").upper(),
                "inception_date": str(item.get("inception_date") or "1900-01-01")[:10],
                "aliases": [_as_text(alias) for alias in item.get("aliases") or [] if _as_text(alias)],
                "supercycle_layer": _as_text(item.get("supercycle_layer")),
                "supercycle_priority": _theme_priority(item.get("supercycle_priority")),
                "supply_chain_role": _as_text(item.get("supply_chain_role")),
                "bottleneck_focus": _as_text(item.get("bottleneck_focus")),
                "evidence_contract": _as_text(item.get("evidence_contract")),
                "research_index": _as_text(item.get("research_index")),
                "members": sorted(set(members)),
            }
        )
    return [theme for theme in themes if theme["theme_id"]]


def _seed_rows(themes: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for theme in themes:
        for symbol in theme.get("members") or []:
            rows.append(
                (
                    theme["theme_id"],
                    theme["label"],
                    symbol,
                    theme["benchmark"],
                    theme["inception_date"],
                    theme.get("supercycle_layer") or "",
                    _theme_priority(theme.get("supercycle_priority")),
                    theme.get("supply_chain_role") or "",
                    theme.get("bottleneck_focus") or "",
                    theme.get("evidence_contract") or "",
                    theme.get("research_index") or "",
                )
            )
    return rows


def _create_seed_table(con: duckdb.DuckDBPyConnection, themes: list[dict[str, Any]]) -> None:
    con.execute(
        """
        CREATE TEMP TABLE theme_seed (
            theme_id VARCHAR,
            theme_label VARCHAR,
            symbol VARCHAR,
            benchmark VARCHAR,
            inception_date DATE,
            supercycle_layer VARCHAR,
            supercycle_priority INTEGER,
            supply_chain_role VARCHAR,
            bottleneck_focus VARCHAR,
            evidence_contract VARCHAR,
            research_index VARCHAR
        )
        """
    )
    rows = _seed_rows(themes)
    if rows:
        con.executemany(
            "INSERT INTO theme_seed VALUES (?, ?, ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?)",
            rows,
        )


def _has_us_theme_tables(con: duckdb.DuckDBPyConnection) -> bool:
    return table_exists(con, "prices_daily")


def query_us_theme_cluster_returns(
    us_db: Path,
    start: date,
    as_of: date,
    seed_map_path: Path,
) -> tuple[list[dict[str, Any]], str]:
    themes = load_us_theme_seed_map(seed_map_path)
    if not themes:
        return [], "missing theme seed map"
    if not us_db.exists():
        return [], "missing"
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        if not _has_us_theme_tables(con):
            return [], "missing prices_daily"
        _create_seed_table(con, themes)
        start_pad = start - timedelta(days=80)
        has_options = table_exists(con, "options_alpha")
        options_join = (
            """
            LEFT JOIN options_alpha oa
              ON oa.symbol = f.symbol
             AND oa.as_of = f.report_date
            """
            if has_options
            else ""
        )
        options_select = (
            """
                SUM(CASE
                    WHEN oa.expression IN ('call_spread', 'stock_long')
                     AND COALESCE(oa.directional_edge, 0) > 0
                     AND COALESCE(oa.flow_edge, 0) >= 0
                    THEN 1 ELSE 0 END) AS options_confirm_members,
                COUNT(oa.symbol) AS options_seen_members,
            """
            if has_options
            else """
                0::INTEGER AS options_confirm_members,
                0::INTEGER AS options_seen_members,
            """
        )
        rows = rows_as_dicts(
            con,
            f"""
            WITH member_prices AS (
                SELECT
                    s.theme_id,
                    s.theme_label,
                    s.supercycle_layer,
                    s.supercycle_priority,
                    s.symbol,
                    s.benchmark,
                    p.date AS report_date,
                    p.adj_close,
                    p.volume,
                    LAG(p.adj_close, 3) OVER (PARTITION BY s.theme_id, s.symbol ORDER BY p.date) AS close_3d_ago,
                    LAG(p.adj_close, 10) OVER (PARTITION BY s.theme_id, s.symbol ORDER BY p.date) AS close_10d_ago,
                    AVG(p.volume) OVER (
                        PARTITION BY s.theme_id, s.symbol
                        ORDER BY p.date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS avg_volume_20
                FROM theme_seed s
                JOIN prices_daily p ON p.symbol = s.symbol
                WHERE p.date >= CAST(? AS DATE)
                  AND p.date <= CAST(? AS DATE)
                  AND p.date >= s.inception_date
                  AND p.adj_close > 0
            ),
            features AS (
                SELECT
                    *,
                    (adj_close / NULLIF(close_3d_ago, 0) - 1.0) * 100.0 AS ret_3d_pct,
                    (adj_close / NULLIF(close_10d_ago, 0) - 1.0) * 100.0 AS ret_10d_pct,
                    volume / NULLIF(avg_volume_20, 0) AS volume_ratio
                FROM member_prices
            ),
            theme_day AS (
                SELECT
                    f.theme_id,
                    MIN(f.theme_label) AS theme_label,
                    MIN(f.supercycle_layer) AS supercycle_layer,
                    MIN(f.supercycle_priority) AS supercycle_priority,
                    f.report_date,
                    COUNT(*) AS members_with_price,
                    AVG(f.ret_3d_pct) AS avg_ret_3d_pct,
                    AVG(f.ret_10d_pct) AS avg_ret_10d_pct,
                    AVG(f.volume_ratio) AS avg_volume_ratio,
                    AVG(CASE WHEN f.ret_3d_pct > 0 THEN 1.0 ELSE 0.0 END) AS ret_3d_breadth,
                    SUM(CASE WHEN f.ret_3d_pct > 0 THEN 1 ELSE 0 END) AS positive_members,
                    {options_select}
                    STRING_AGG(
                        CASE WHEN f.ret_3d_pct > 0 THEN f.symbol ELSE NULL END,
                        ','
                        ORDER BY f.ret_3d_pct DESC
                    ) AS positive_member_list,
                    STRING_AGG(f.symbol, ',' ORDER BY f.ret_3d_pct DESC) AS member_rank_list,
                    (
                        COALESCE(AVG(f.ret_3d_pct), 0) * 0.35
                        + COALESCE(AVG(f.ret_10d_pct), 0) * 0.20
                        + COALESCE(AVG(f.volume_ratio), 0) * 1.40
                        + COALESCE(AVG(CASE WHEN f.ret_3d_pct > 0 THEN 1.0 ELSE 0.0 END), 0) * 4.0
                    ) AS theme_score
                FROM features f
                {options_join}
                WHERE f.report_date >= CAST(? AS DATE)
                  AND f.report_date <= CAST(? AS DATE)
                  AND f.ret_3d_pct IS NOT NULL
                  AND f.volume_ratio IS NOT NULL
                GROUP BY f.theme_id, f.report_date
            ),
            signals AS (
                SELECT
                    *,
                    CASE
                        WHEN options_seen_members > 0 AND options_confirm_members >= 1 THEN 'full_confirm'
                        WHEN options_seen_members > 0 THEN 'proxy_confirm'
                        ELSE 'price_volume_proxy'
                    END AS confirm_quality
                FROM theme_day
                WHERE members_with_price >= 3
                  AND positive_members >= 2
                  AND avg_ret_10d_pct <= 45.0
                  AND avg_volume_ratio >= 1.10
                  AND (
                        (
                            ret_3d_breadth >= 0.60
                            AND avg_ret_3d_pct >= 2.0
                            AND (
                                options_confirm_members >= 1
                                OR avg_ret_3d_pct >= 3.5
                            )
                        )
                     OR (
                            positive_members >= 4
                            AND options_confirm_members >= 1
                            AND avg_ret_3d_pct >= 5.0
                        )
                  )
            ),
            joined AS (
                SELECT
                    s.*,
                    p.symbol,
                    p.date AS price_date,
                    p.adj_close,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.theme_id, s.report_date, p.symbol
                        ORDER BY p.date
                    ) AS rn
                FROM signals s
                JOIN theme_seed seed ON seed.theme_id = s.theme_id
                JOIN prices_daily p
                  ON p.symbol = seed.symbol
                 AND p.date > s.report_date
                 AND p.adj_close > 0
            ),
            entry AS (
                SELECT theme_id, report_date, symbol, price_date AS entry_date, adj_close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT theme_id, report_date, symbol, price_date AS exit_date, adj_close AS exit_close
                FROM joined
                WHERE rn = 4
            ),
            basket_returns AS (
                SELECT
                    e.theme_id,
                    e.report_date,
                    MIN(e.entry_date) AS entry_date,
                    MAX(x.exit_date) AS exit_date,
                    AVG((x.exit_close / NULLIF(e.entry_close, 0) - 1.0) * 100.0) AS return_pct,
                    COUNT(*) AS priced_members
                FROM entry e
                JOIN exit x
                  ON x.theme_id = e.theme_id
                 AND x.report_date = e.report_date
                 AND x.symbol = e.symbol
                GROUP BY e.theme_id, e.report_date
            )
            SELECT
                s.report_date,
                s.theme_id AS symbol,
                s.theme_id,
                s.theme_label,
                s.supercycle_layer,
                s.supercycle_priority,
                s.confirm_quality,
                s.members_with_price,
                s.positive_members,
                s.ret_3d_breadth,
                s.avg_ret_3d_pct,
                s.avg_ret_10d_pct,
                s.avg_volume_ratio,
                s.options_confirm_members,
                s.options_seen_members,
                s.positive_member_list,
                s.member_rank_list,
                s.theme_score,
                b.entry_date,
                b.exit_date,
                b.priced_members,
                b.return_pct
            FROM signals s
            JOIN basket_returns b ON b.theme_id = s.theme_id AND b.report_date = s.report_date
            ORDER BY s.report_date, s.theme_score DESC, s.theme_id
            """,
            [start_pad.isoformat(), as_of.isoformat(), start.isoformat(), as_of.isoformat()],
        )
        full = sum(1 for row in rows if row.get("confirm_quality") == "full_confirm")
        proxy = len(rows) - full
        return rows, f"ok themes={len(themes)} full_confirm={full} proxy_confirm={proxy}"
    finally:
        con.close()


def query_us_theme_current_candidates(
    us_db: Path,
    as_of: date,
    seed_map_path: Path,
    *,
    top_themes: int = 3,
    top_members: int = 5,
) -> list[dict[str, Any]]:
    themes = load_us_theme_seed_map(seed_map_path)
    if not themes or not us_db.exists():
        return []
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        if not _has_us_theme_tables(con):
            return []
        _create_seed_table(con, themes)
        start_pad = as_of - timedelta(days=80)
        price_row = con.execute(
            """
            SELECT MAX(p.date)
            FROM prices_daily p
            JOIN theme_seed s ON s.symbol = p.symbol
            WHERE p.date <= CAST(? AS DATE)
            """,
            [as_of.isoformat()],
        ).fetchone()
        price_as_of = price_row[0] if price_row else None
        if price_as_of is None:
            return []
        if hasattr(price_as_of, "isoformat"):
            price_as_of_text = price_as_of.isoformat()
        else:
            price_as_of_text = str(price_as_of)[:10]
        option_as_of_text: str | None = None
        has_options = table_exists(con, "options_alpha")
        if has_options:
            option_row = con.execute(
                "SELECT MAX(as_of) FROM options_alpha WHERE as_of <= CAST(? AS DATE)",
                [as_of.isoformat()],
            ).fetchone()
            option_as_of = option_row[0] if option_row else None
            if option_as_of is None:
                has_options = False
            elif hasattr(option_as_of, "isoformat"):
                option_as_of_text = option_as_of.isoformat()
            else:
                option_as_of_text = str(option_as_of)[:10]
        options_join = (
            """
            LEFT JOIN options_alpha oa
              ON oa.symbol = f.symbol
             AND oa.as_of = CAST(? AS DATE)
            """
            if has_options
            else ""
        )
        option_fields = (
            """
                CASE
                    WHEN oa.expression IN ('call_spread', 'stock_long')
                     AND COALESCE(oa.directional_edge, 0) > 0
                     AND COALESCE(oa.flow_edge, 0) >= 0
                    THEN 1 ELSE 0 END AS option_confirm,
                oa.expression AS option_expression,
            """
            if has_options
            else """
                0::INTEGER AS option_confirm,
                NULL::VARCHAR AS option_expression,
            """
        )
        rows = rows_as_dicts(
            con,
            f"""
            WITH member_prices AS (
                SELECT
                    s.theme_id,
                    s.theme_label,
                    s.supercycle_layer,
                    s.supercycle_priority,
                    s.supply_chain_role,
                    s.bottleneck_focus,
                    s.evidence_contract,
                    s.research_index,
                    s.symbol,
                    p.date AS report_date,
                    p.adj_close,
                    p.volume,
                    LAG(p.adj_close, 3) OVER (PARTITION BY s.theme_id, s.symbol ORDER BY p.date) AS close_3d_ago,
                    LAG(p.adj_close, 10) OVER (PARTITION BY s.theme_id, s.symbol ORDER BY p.date) AS close_10d_ago,
                    AVG(p.volume) OVER (
                        PARTITION BY s.theme_id, s.symbol
                        ORDER BY p.date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS avg_volume_20
                FROM theme_seed s
                JOIN prices_daily p ON p.symbol = s.symbol
                WHERE p.date >= CAST(? AS DATE)
                  AND p.date <= CAST(? AS DATE)
                  AND p.adj_close > 0
            ),
            features AS (
                SELECT
                    *,
                    (adj_close / NULLIF(close_3d_ago, 0) - 1.0) * 100.0 AS ret_3d_pct,
                    (adj_close / NULLIF(close_10d_ago, 0) - 1.0) * 100.0 AS ret_10d_pct,
                    volume / NULLIF(avg_volume_20, 0) AS volume_ratio
                FROM member_prices
                WHERE report_date = CAST(? AS DATE)
            ),
            enriched AS (
                SELECT
                    f.*,
                    {option_fields}
                    (
                        COALESCE(f.ret_3d_pct, 0) * 0.45
                        + COALESCE(f.ret_10d_pct, 0) * 0.15
                        + COALESCE(f.volume_ratio, 0) * 1.35
                    ) AS member_score
                FROM features f
                {options_join}
            ),
            theme_day AS (
                SELECT
                    theme_id,
                    COUNT(*) AS members_with_price,
                    AVG(ret_3d_pct) AS avg_ret_3d_pct,
                    AVG(ret_10d_pct) AS avg_ret_10d_pct,
                    AVG(volume_ratio) AS avg_volume_ratio,
                    AVG(CASE WHEN ret_3d_pct > 0 THEN 1.0 ELSE 0.0 END) AS ret_3d_breadth,
                    SUM(CASE WHEN ret_3d_pct > 0 THEN 1 ELSE 0 END) AS positive_members,
                    SUM(option_confirm) AS options_confirm_members,
                    STRING_AGG(symbol, ',' ORDER BY member_score DESC) AS ranked_members,
                    (
                        COALESCE(AVG(ret_3d_pct), 0) * 0.35
                        + COALESCE(AVG(ret_10d_pct), 0) * 0.20
                        + COALESCE(AVG(volume_ratio), 0) * 1.40
                        + COALESCE(AVG(CASE WHEN ret_3d_pct > 0 THEN 1.0 ELSE 0.0 END), 0) * 4.0
                    ) AS theme_score
                FROM enriched
                WHERE ret_3d_pct IS NOT NULL AND volume_ratio IS NOT NULL
                GROUP BY theme_id
            ),
            passed_themes AS (
                SELECT *
                FROM theme_day
                WHERE members_with_price >= 3
                  AND positive_members >= 2
                  AND avg_ret_10d_pct <= 45.0
                  AND avg_volume_ratio >= 1.10
                  AND (
                        (
                            ret_3d_breadth >= 0.60
                            AND avg_ret_3d_pct >= 2.0
                            AND (
                                options_confirm_members >= 1
                                OR avg_ret_3d_pct >= 3.5
                            )
                        )
                     OR (
                            positive_members >= 4
                            AND options_confirm_members >= 1
                            AND avg_ret_3d_pct >= 5.0
                        )
                  )
                ORDER BY theme_score DESC
                LIMIT ?
            ),
            ranked_members AS (
                SELECT
                    e.*,
                    p.theme_score,
                    p.avg_ret_3d_pct,
                    p.avg_volume_ratio,
                    p.ret_3d_breadth,
                    p.options_confirm_members,
                    ROW_NUMBER() OVER (PARTITION BY e.theme_id ORDER BY e.member_score DESC, e.symbol) AS member_rank
                FROM enriched e
                JOIN passed_themes p ON p.theme_id = e.theme_id
                WHERE e.ret_3d_pct > 0
            )
            SELECT *
            FROM ranked_members
            WHERE member_rank <= ?
            ORDER BY theme_score DESC, theme_id, member_rank
            """,
            [
                start_pad.isoformat(),
                price_as_of_text,
                price_as_of_text,
                *([option_as_of_text] if has_options and option_as_of_text else []),
                top_themes,
                top_members,
            ],
        )
        candidates: list[dict[str, Any]] = []
        for row in rows:
            price = _round_or_none(row.get("adj_close")) or 0.0
            theme_label = str(row.get("theme_label") or row.get("theme_id") or "").strip()
            layer = str(row.get("supercycle_layer") or "").strip()
            role = str(row.get("supply_chain_role") or "").strip()
            candidates.append(
                {
                    "market": "us",
                    "as_of": as_of.isoformat(),
                    "signal_price_date": price_as_of_text,
                    "options_as_of": option_as_of_text,
                    "symbol": str(row.get("symbol") or "").upper(),
                    "name": "",
                    "state": "Execution Alpha",
                    "policy": f"theme_cluster_momentum:{row.get('theme_id')}",
                    "alpha_sleeve_id": US_THEME_SLEEVE_ID,
                    "alpha_factory_role": "execution_sleeve",
                    "execution_source": "alpha_factory_sleeve",
                    "entry": _round_or_none(price, 4),
                    "stop": _round_or_none(price * 0.94 if price else None, 4),
                    "target": _round_or_none(price * 1.10 if price else None, 4),
                    "rr_ratio": 1.67,
                    "expected_move_pct": _round_or_none(row.get("avg_ret_3d_pct"), 4),
                    "time_exit": "3 sessions / theme breadth deterioration",
                    "option_expression": row.get("option_expression") or "stock_long",
                    "trend_regime": "theme_cluster_momentum",
                    "signal_confidence": "HIGH" if int(row.get("option_confirm") or 0) else "MODERATE",
                    "execution_mode": "buy_stock_with_theme_and_options_confirmation",
                    "primary_reason": (
                        f"{theme_label}: "
                        f"layer={layer or '-'}, role={role or '-'}, "
                        f"breadth={_fmt_pct((row.get('ret_3d_breadth') or 0) * 100.0)}, "
                        f"avg3d={_fmt_pct(row.get('avg_ret_3d_pct'))}, "
                        f"vol={_fmt_num(row.get('avg_volume_ratio'), 2)}, "
                        f"options_confirm={row.get('options_confirm_members')}; "
                        f"price_date={price_as_of_text}, options_date={option_as_of_text or '-'}"
                    ),
                    "blockers": [],
                    "pullback_price": _round_or_none(price * 0.985 if price else None, 4),
                    "theme_id": row.get("theme_id"),
                    "theme_label": theme_label,
                    "theme_score": _round_or_none(row.get("theme_score"), 4),
                    "supercycle_layer": layer,
                    "supercycle_priority": row.get("supercycle_priority"),
                    "supply_chain_role": role,
                    "bottleneck_focus": row.get("bottleneck_focus"),
                    "evidence_contract": row.get("evidence_contract"),
                    "research_index": row.get("research_index"),
                    "member_rank": row.get("member_rank"),
                    "reason": (
                        "AI supercycle theme basket is strong; stock is a liquid member expression of the basket. "
                        "Supplier/customer claims still require news or filing confirmation before final narrative."
                    ),
                }
            )
        return candidates
    finally:
        con.close()
