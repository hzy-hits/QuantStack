#!/usr/bin/env python3
"""Export promoted Factor Lab factors as auditable sleeve return streams."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.dsl.compute import compute_factor
from src.dsl.parser import parse
from src.mining.batch_mine import CONFIGS
from src.mining.contracts import ensure_contract_tables
from src.mining.daily_pipeline import init_db
from src.paths import FACTOR_LAB_DB


HORIZON_DAYS = 5


def _orient_for_direction(factor_df: pd.DataFrame, direction: str | None) -> pd.DataFrame:
    if (direction or "long").lower() != "short":
        return factor_df
    out = factor_df.copy()
    out["factor_value"] = -out["factor_value"]
    return out


def _normalize_prices(prices: pd.DataFrame, market: str) -> pd.DataFrame:
    cfg = CONFIGS[market]
    out = prices.copy()
    top_n = cfg.get("universe_top_n")
    if top_n and "market_cap" in out.columns:
        out["_mcap_rank"] = out.groupby("trade_date")["market_cap"].rank(
            ascending=False, method="first", na_option="bottom"
        )
        out = out[out["_mcap_rank"] <= top_n].drop(columns=["_mcap_rank"]).reset_index(drop=True)
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    return out.dropna(subset=["trade_date"])


def _forward_returns_from_prices(prices: pd.DataFrame) -> pd.DataFrame:
    sorted_prices = prices.sort_values(["ts_code", "trade_date"]).copy()
    sorted_prices["fwd_5d"] = (
        sorted_prices.groupby("ts_code")["close"].shift(-HORIZON_DAYS)
        / sorted_prices["close"]
        - 1.0
    )
    return sorted_prices[["ts_code", "trade_date", "fwd_5d"]].dropna(subset=["fwd_5d"])


def _row_detail(valid: pd.DataFrame, bucket: str) -> str:
    return json.dumps(
        {
            "method": "oriented_top_quintile_5d_forward_return",
            "bucket": bucket,
            "horizon_days": HORIZON_DAYS,
            "n_names": int(len(valid)),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def _compute_factor_sleeve_rows(
    *,
    market: str,
    factor_id: str,
    factor_name: str,
    sleeve_id: str,
    report_contract: str,
    money_readiness: str,
    direction: str,
    factor_df: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    start: str,
    as_of: str,
    cost_per_trade: float,
) -> list[dict[str, Any]]:
    merged = factor_df.merge(fwd_returns, on=["ts_code", "trade_date"], how="inner")
    merged = merged.dropna(subset=["factor_value", "fwd_5d"])
    if merged.empty:
        return []

    start_ts = pd.Timestamp(start)
    as_of_ts = pd.Timestamp(as_of)
    merged = merged[(merged["trade_date"] >= start_ts) & (merged["trade_date"] <= as_of_ts)]
    if merged.empty:
        return []

    rows: list[dict[str, Any]] = []
    daily_cost_pct = float(cost_per_trade) * 100.0 / HORIZON_DAYS
    diagnostic_cost_pct = daily_cost_pct * 2.0

    for trade_date, group in sorted(merged.groupby("trade_date"), key=lambda item: item[0]):
        valid = group.dropna(subset=["factor_value", "fwd_5d"]).copy()
        if len(valid) < 25:
            continue
        try:
            valid["quintile"] = pd.qcut(valid["factor_value"], 5, labels=False, duplicates="drop") + 1
        except ValueError:
            continue
        if valid["quintile"].nunique() < 5:
            continue

        top = valid[valid["quintile"] == 5]
        bottom = valid[valid["quintile"] == 1]
        if top.empty or bottom.empty:
            continue

        top_gross_daily_pct = float(top["fwd_5d"].mean()) * 100.0 / HORIZON_DAYS
        rows.append(
            {
                "return_date": pd.Timestamp(trade_date).date().isoformat(),
                "market": market,
                "factor_id": factor_id,
                "sleeve_id": sleeve_id,
                "factor_name": factor_name,
                "report_contract": report_contract,
                "money_readiness": money_readiness,
                "direction": direction,
                "bucket": "top_quintile_long",
                "gross_return_pct": top_gross_daily_pct,
                "daily_return_pct": top_gross_daily_pct,
                "cost_adjusted_return_pct": top_gross_daily_pct - daily_cost_pct,
                "n_names": int(len(valid)),
                "top_bucket_count": int(len(top)),
                "bottom_bucket_count": int(len(bottom)),
                "cost_pct": daily_cost_pct,
                "method": "oriented_top_quintile_5d_forward_return",
                "detail_json": _row_detail(valid, "top_quintile_long"),
            }
        )

        long_short_daily_pct = float(top["fwd_5d"].mean() - bottom["fwd_5d"].mean()) * 100.0 / HORIZON_DAYS
        rows.append(
            {
                "return_date": pd.Timestamp(trade_date).date().isoformat(),
                "market": market,
                "factor_id": factor_id,
                "sleeve_id": sleeve_id,
                "factor_name": factor_name,
                "report_contract": "research_only",
                "money_readiness": "research_only",
                "direction": direction,
                "bucket": "long_short_diagnostic",
                "gross_return_pct": long_short_daily_pct,
                "daily_return_pct": long_short_daily_pct,
                "cost_adjusted_return_pct": long_short_daily_pct - diagnostic_cost_pct,
                "n_names": int(len(valid)),
                "top_bucket_count": int(len(top)),
                "bottom_bucket_count": int(len(bottom)),
                "cost_pct": diagnostic_cost_pct,
                "method": "oriented_long_short_5d_forward_return_research_only",
                "detail_json": _row_detail(valid, "long_short_diagnostic"),
            }
        )

    return rows


def _load_promoted(con: duckdb.DuckDBPyConnection, market: str) -> list[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT factor_id, formula, name, direction, sleeve_id, report_contract,
               money_readiness, ic_7d, ic_14d, ic_30d
        FROM factor_registry
        WHERE market=? AND status='promoted'
        ORDER BY composite_score DESC NULLS LAST, promoted_at DESC NULLS LAST
        """,
        [market],
    )
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def export_market(market: str, start: str, as_of: str) -> int:
    init_db()
    cfg = CONFIGS[market]
    lab_con = duckdb.connect(str(FACTOR_LAB_DB))
    try:
        ensure_contract_tables(lab_con)
        promoted = _load_promoted(lab_con, market)
    finally:
        lab_con.close()

    if not promoted:
        print(f"  No promoted factors for {market}")
        return 0

    price_con = duckdb.connect(cfg["db_path"], read_only=True)
    try:
        prices = price_con.execute(cfg["sql"]).fetchdf()
    finally:
        price_con.close()
    prices = _normalize_prices(prices, market)
    fwd_returns = _forward_returns_from_prices(prices)
    cost = 0.003 if market == "cn" else 0.001

    all_rows: list[dict[str, Any]] = []
    for factor in promoted:
        try:
            factor_df = compute_factor(
                parse(str(factor["formula"])),
                prices,
                sym_col="ts_code",
                date_col="trade_date",
            )
            direction = str(factor.get("direction") or "long").lower()
            if direction not in {"long", "short"}:
                direction = "long"
            factor_df = _orient_for_direction(factor_df, direction)
            rows = _compute_factor_sleeve_rows(
                market=market,
                factor_id=str(factor["factor_id"]),
                factor_name=str(factor.get("name") or factor["factor_id"]),
                sleeve_id=str(factor.get("sleeve_id") or "daily_price_overlay"),
                report_contract=str(factor.get("report_contract") or "research_only"),
                money_readiness=str(factor.get("money_readiness") or "research_only"),
                direction=direction,
                factor_df=factor_df,
                fwd_returns=fwd_returns,
                start=start,
                as_of=as_of,
                cost_per_trade=cost,
            )
            all_rows.extend(rows)
            print(f"    {factor.get('name') or factor['factor_id']}: {len(rows)} sleeve rows")
        except Exception as exc:
            print(f"    {factor.get('name') or factor.get('factor_id')}: failed - {exc}")

    if not all_rows:
        return 0

    lab_con = duckdb.connect(str(FACTOR_LAB_DB))
    try:
        ensure_contract_tables(lab_con)
        lab_con.execute(
            """
            DELETE FROM factor_sleeve_returns
            WHERE market=? AND return_date >= CAST(? AS DATE) AND return_date <= CAST(? AS DATE)
            """,
            [market, start, as_of],
        )
        lab_con.executemany(
            """
            INSERT OR REPLACE INTO factor_sleeve_returns (
                return_date, market, factor_id, sleeve_id, factor_name,
                report_contract, money_readiness, direction, bucket,
                gross_return_pct, daily_return_pct, cost_adjusted_return_pct,
                n_names, top_bucket_count, bottom_bucket_count, cost_pct,
                method, detail_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                [
                    row["return_date"],
                    row["market"],
                    row["factor_id"],
                    row["sleeve_id"],
                    row["factor_name"],
                    row["report_contract"],
                    row["money_readiness"],
                    row["direction"],
                    row["bucket"],
                    row["gross_return_pct"],
                    row["daily_return_pct"],
                    row["cost_adjusted_return_pct"],
                    row["n_names"],
                    row["top_bucket_count"],
                    row["bottom_bucket_count"],
                    row["cost_pct"],
                    row["method"],
                    row["detail_json"],
                ]
                for row in all_rows
            ],
        )
    finally:
        lab_con.close()

    print(f"  Wrote {len(all_rows)} Factor Lab sleeve return rows for {market}")
    return len(all_rows)


def run(market: str, start: str, as_of: str | None = None) -> int:
    as_of = as_of or date.today().isoformat()
    markets = ["cn", "us"] if market == "all" else [market]
    return sum(export_market(mkt, start, as_of) for mkt in markets)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Factor Lab promoted factor sleeve returns.")
    parser.add_argument("--market", choices=["cn", "us", "all"], default="all")
    parser.add_argument("--start", default="2026-03-01")
    parser.add_argument("--date", "--as-of", dest="as_of", default=None)
    args = parser.parse_args()
    count = run(args.market, args.start, args.as_of)
    print(f"Exported {count} Factor Lab sleeve return rows")


if __name__ == "__main__":
    main()
