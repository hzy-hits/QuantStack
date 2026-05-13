"""US alpha sleeve builders."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

import run_main_strategy_v2_backtest as v2

from .base import Sleeve, make_sleeve, rows_as_dicts, table_exists
from .us_theme_cluster import US_THEME_SLEEVE_ID, query_us_theme_cluster_returns


STACK_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_US_THEME_SEED_MAP = STACK_ROOT / "data" / "us_theme_seed_map.yaml"


def query_us_sec_filing_returns(us_db: Path, start: date, as_of: date) -> list[dict[str, Any]]:
    if not us_db.exists():
        return []
    con = duckdb.connect(str(us_db), read_only=True)
    try:
        if not table_exists(con, "sec_filings") or not table_exists(con, "prices_daily"):
            return []
        return rows_as_dicts(
            con,
            """
            WITH events AS (
                SELECT symbol, accession_number, filed_date,
                       LOWER(COALESCE(description, '') || ' ' || COALESCE(items, '')) AS event_text
                FROM sec_filings
                WHERE filed_date >= CAST(? AS DATE)
                  AND filed_date <= CAST(? AS DATE)
                  AND form_type = '8-K'
            ),
            joined AS (
                SELECT e.symbol, e.accession_number, e.filed_date, e.event_text,
                       p.date AS price_date, p.adj_close AS close,
                       ROW_NUMBER() OVER (
                           PARTITION BY e.symbol, e.accession_number
                           ORDER BY p.date
                       ) AS rn
                FROM events e
                JOIN prices_daily p
                  ON p.symbol = e.symbol
                 AND p.date > e.filed_date
                 AND p.adj_close > 0
            ),
            entry AS (
                SELECT symbol, accession_number, filed_date, event_text, price_date AS entry_date, close AS entry_close
                FROM joined
                WHERE rn = 1
            ),
            exit AS (
                SELECT symbol, accession_number, price_date AS exit_date, close AS exit_close
                FROM joined
                WHERE rn = 4
            )
            SELECT e.filed_date AS report_date,
                   e.symbol,
                   e.accession_number,
                   e.event_text,
                   e.entry_date,
                   x.exit_date,
                   (x.exit_close / e.entry_close - 1.0) * 100.0 AS return_pct
            FROM entry e
            JOIN exit x
              ON x.symbol = e.symbol
             AND x.accession_number = e.accession_number
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()



def build_us_sleeves(us_db: Path, start: date, as_of: date, min_money_n: int) -> list[Sleeve]:
    sleeves: list[Sleeve] = []
    us_rows, us_status = v2.load_us_rows(us_db, start, as_of)
    us_v2 = v2.rows_with_return_cost([row for row in us_rows if v2.is_us_v2_policy(row)], v2.US_STOCK_ROUNDTRIP_COST_PCT)
    us_legacy = [row for row in us_rows if v2.is_us_legacy_policy(row)]
    option_ledger = v2.build_option_shadow_ledger(us_db, start, as_of) if us_db.exists() else {"rows": []}
    option_rows = [
        {"report_date": row.get("report_date"), "symbol": row.get("symbol"), "return_pct": row.get("return_pct")}
        for row in option_ledger.get("rows", [])
        if row.get("resolved") and row.get("long_expression") and row.get("return_pct") is not None
    ]
    filing_rows = query_us_sec_filing_returns(us_db, start, as_of)
    material_filing_rows = [
        row
        for row in filing_rows
        if "item 1.01" in str(row.get("event_text") or "")
        or "material definitive agreement" in str(row.get("event_text") or "")
    ]
    theme_rows, theme_status = query_us_theme_cluster_returns(us_db, start, as_of, DEFAULT_US_THEME_SEED_MAP)

    sleeves.append(
        make_sleeve(
            sleeve_id="us_v2_stock_probe",
            market="us",
            label="US V2 stock-only trade net",
            signal_rule="LOW/core/executable_now/trending, underlying 3-session return minus stock cost",
            horizon="3 sessions",
            data_status=us_status,
            role="probe",
            notes="Stock trade sleeve; options/flow are auxiliary ranking evidence, not the traded instrument.",
            rows=us_v2,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="us_legacy_high_mod",
            market="us",
            label="US legacy HIGH/MOD baseline",
            signal_rule="legacy core long HIGH/MODERATE executable_now",
            horizon="3 sessions",
            data_status=us_status,
            role="baseline",
            notes="Baseline only; not a fresh-entry policy.",
            rows=us_legacy,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="us_option_shadow_long",
            market="us",
            label="US option shadow long expressions",
            signal_rule="V2 rows with stock_long/call_spread expression marked by bid/ask or proxy",
            horizon="3 sessions",
            data_status=f"resolved={option_ledger.get('resolved_count', 0)} unresolved={option_ledger.get('unresolved_count', 0)}",
            role="shadow",
            notes="Shadow-only until resolved sample, LCB80, liquidity, and live slippage pass.",
            rows=option_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id=US_THEME_SLEEVE_ID,
            market="us",
            label="US theme cluster momentum basket",
            signal_rule="theme basket breadth + 3D/10D price strength + volume expansion + options/flow confirmation",
            horizon="3 sessions equal-weight basket",
            data_status=theme_status,
            role="money",
            notes="Basket sleeve: theme-level evidence first, then strongest member stocks get R; options/flow confirms but options are not the traded instrument.",
            rows=theme_rows,
            min_money_n=min_money_n,
        )
    )
    sleeves.append(
        make_sleeve(
            sleeve_id="us_sec_8k_material_agreement",
            market="us",
            label="US SEC 8-K material-agreement diagnostic",
            signal_rule="8-K Item 1.01 / material definitive agreement, next tradable close to 3-session exit",
            horizon="3 sessions",
            data_status="diagnostic_only_sec_summary",
            role="research",
            notes="Needs document parser/payoff table before becoming event alpha.",
            rows=material_filing_rows,
            min_money_n=min_money_n,
        )
    )
    return sleeves
