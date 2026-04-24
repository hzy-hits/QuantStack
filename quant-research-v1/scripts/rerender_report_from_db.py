#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quant_bot.analytics.hmm_regime import compute_hmm_calibration, fit_hmm_regime
from quant_bot.analytics.news_quality import assess_news_quality
from quant_bot.analytics.overnight_gate import run_overnight_gate, store_overnight_gate
from quant_bot.analytics.report_review import refresh_report_review, store_report_decisions
from quant_bot.analytics.risk_params import compute_risk_params
from quant_bot.analytics.scorecard import compute_scorecard
from quant_bot.config.settings import Settings
from quant_bot.filtering.notable import build_notable_items
from quant_bot.reporting.bundle import (
    build_report_bundle,
    compute_headline_gate,
    compute_options_extremes,
)
from quant_bot.reporting.paths import payload_path
from quant_bot.reporting.render import render_payload_md
from quant_bot.signals.classify import classify_all
from quant_bot.storage.db import connect_write, snapshot_path
from quant_bot.universe.builder import build_universe


ANALYSIS_MODULES = (
    "momentum_risk",
    "earnings_risk",
    "mean_reversion",
    "breakout",
    "overnight_gate",
)


def _load_symbols(con, as_of, benchmark: str) -> list[str]:
    as_of_str = as_of.strftime("%Y-%m-%d")
    placeholders = ",".join(f"'{module}'" for module in ANALYSIS_MODULES)
    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM analysis_daily
        WHERE date = ?
          AND module_name IN ({placeholders})
        ORDER BY symbol
        """,
        [as_of_str],
    ).fetchall()
    symbols = [row[0] for row in rows if row and row[0]]
    if benchmark not in symbols:
        symbols.append(benchmark)
    return symbols


def _build_core_symbols(universe: dict[str, list[str]], benchmark: str) -> set[str]:
    core_symbols = {
        symbol
        for bucket, symbols in universe.items()
        if not bucket.startswith("_") and bucket != "broad_screen"
        for symbol in symbols
    }
    core_symbols.add(benchmark)
    return core_symbols


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-render a US payload from an existing report DB snapshot."
    )
    parser.add_argument("--date", required=True, help="Trade date, YYYY-MM-DD")
    parser.add_argument("--session", choices=["pre", "post"], required=True)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--refresh-overnight-gate",
        action="store_true",
        help="Recompute overnight_gate rows in the report DB before rendering.",
    )
    args = parser.parse_args()

    cfg = Settings.load(args.config)
    as_of = datetime.strptime(args.date, "%Y-%m-%d").date()
    benchmark = cfg.universe.benchmark
    report_db_path = snapshot_path(cfg.report_db_path_abs, as_of, args.session)
    if not report_db_path.exists():
        raise FileNotFoundError(f"Report DB not found: {report_db_path}")

    if args.refresh_overnight_gate:
        with connect_write(report_db_path) as write_con:
            candidate_syms = _load_symbols(write_con, as_of, benchmark)
            overnight_df = run_overnight_gate(write_con, candidate_syms, as_of)
            n_rows = store_overnight_gate(write_con, overnight_df)
            write_con.execute("CHECKPOINT")
        print(f"Refreshed overnight_gate rows: {n_rows}")

    with connect_write(report_db_path) as report_con:
        universe = build_universe(
            report_con,
            scan_sp500=cfg.universe.scan.sp500,
            scan_nasdaq100=cfg.universe.scan.nasdaq100,
            include_sector_etfs=cfg.universe.asset_classes.sector_etfs,
            include_semi_etfs=cfg.universe.asset_classes.semi_etfs,
            include_biotech_etfs=cfg.universe.asset_classes.biotech_etfs,
            include_china_internet_etfs=cfg.universe.asset_classes.china_internet_etfs,
            include_innovation_etfs=cfg.universe.asset_classes.innovation_etfs,
            include_bond_etfs=cfg.universe.asset_classes.bond_etfs,
            include_commodities=cfg.universe.asset_classes.commodities,
            include_international=cfg.universe.asset_classes.international,
            include_volatility=cfg.universe.asset_classes.volatility,
            include_crypto_etfs=cfg.universe.asset_classes.crypto_etfs,
            watchlist=cfg.universe.watchlist,
            constituent_refresh_days=cfg.data.constituent_refresh_days,
            benchmark=benchmark,
            broad_screen_hits=None,
        )
        core_symbols = _build_core_symbols(universe, benchmark)
        selection_policy = cfg.selection.model_dump()
        candidate_syms = _load_symbols(report_con, as_of, benchmark)
        ranked_items = build_notable_items(
            report_con,
            as_of,
            candidate_syms,
            benchmark=benchmark,
            max_items=max(len(candidate_syms), cfg.output.max_notable_items),
            core_symbols=core_symbols,
            selection_policy=selection_policy,
        )
        notable = ranked_items[: cfg.output.max_notable_items]
        bundle = build_report_bundle(
            report_con,
            as_of,
            notable,
            benchmark=benchmark,
            universe=universe,
            dividend_dip_screen=[],
        )
        classify_all(ranked_items, bundle.get("market_context"))
        for item in bundle["notable_items"]:
            sig_conf = (item.get("signal") or {}).get("confidence")
            if sig_conf in {"HIGH", "MODERATE"}:
                item["risk_params"] = compute_risk_params(item)
        bundle["options_extremes"] = compute_options_extremes(bundle["notable_items"])
        bundle["shared_catalysts"] = assess_news_quality(bundle["notable_items"], as_of)
        bundle["scorecard"] = compute_scorecard(report_con, as_of)
        hmm_result = fit_hmm_regime(report_con, as_of)
        if hmm_result:
            try:
                hmm_result["calibration"] = compute_hmm_calibration(report_con, as_of)
            except Exception:
                pass
            bundle["hmm_regime"] = hmm_result
        bundle["headline_gate"] = compute_headline_gate(bundle)
        selected_symbols = {item["symbol"] for item in notable}
        store_report_decisions(
            report_con,
            as_of,
            args.session,
            ranked_items,
            selected_symbols,
            bundle["headline_gate"],
        )
        bundle["report_review"] = refresh_report_review(report_con, as_of, args.session)
        report_con.execute("CHECKPOINT")

    output_path = payload_path("reports", as_of, args.session)
    render_payload_md(bundle, output_path)

    lane_counts: dict[str, int] = {}
    for item in notable:
        lane = item.get("report_bucket") or "unknown"
        lane_counts[lane] = lane_counts.get(lane, 0) + 1

    print(f"Payload rendered: {output_path}")
    print(f"Selected items: {len(notable)}")
    print(f"Lane counts: {lane_counts}")
    print(f"Headline gate: {bundle['headline_gate']['mode']}")


if __name__ == "__main__":
    main()
