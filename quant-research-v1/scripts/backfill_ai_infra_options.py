"""One-shot: fetch current options chains for the AI-infra universe.

The daily pipeline only fetches options for the day's notable candidates +
watchlist, so AI-infra production-basket names (AMZN/GOOGL/ORCL/TSM/CRWV ...)
injected at report time often have no chain → empty iv/vrp/tenor in the
per-stock options verdict.

run_daily.py is now fixed forward (it adds the AI-infra universe to the
options fetch list). This script does the immediate one-shot catch-up: fetch
current CBOE delayed quotes for every options-eligible AI-infra name and
recompute their VRP / sentiment, so the verdict is complete now rather than
next trading day.

Note: CBOE CDN serves current delayed quotes only — this captures the latest
snapshot, it cannot recreate a specific past day's chain.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_bot.analytics import ai_infra_universe  # noqa: E402
from quant_bot.analytics.variance_premium import compute_vrp, store_vrp  # noqa: E402
from quant_bot.analytics.sentiment_ewma import (  # noqa: E402
    compute_sentiment_ewma,
    store_sentiment,
)
from quant_bot.data_ingestion.options import (  # noqa: E402
    fetch_options_snapshot_with_quotes,
    is_options_eligible,
    upsert_options,
    upsert_options_analysis,
    upsert_options_chain_quotes,
)
from quant_bot.data_ingestion.prices import fetch_and_store_prices  # noqa: E402
from quant_bot.storage.db import connect_write  # noqa: E402

DEFAULT_DB = REPO_ROOT / "data" / "quant.duckdb"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        default=None, help="Report date to tag the snapshot (default: today).")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help="Canonical research DuckDB the report reads.")
    parser.add_argument("--max-expiries", type=int, default=12)
    args = parser.parse_args()
    as_of = args.as_of or date.today()

    if not args.db.exists():
        print(f"error: db missing at {args.db}", file=sys.stderr)
        return 2

    symbols = sorted(
        s for s in ai_infra_universe.records_by_symbol("US", pool="research")
        if is_options_eligible(s)
    )
    if not symbols:
        print("error: no options-eligible AI-infra US symbols resolved", file=sys.stderr)
        return 1
    print(f"fetching options chains for {len(symbols)} AI-infra US names "
          f"(max_expiries={args.max_expiries}) ...")

    snapshot_df, analysis_df, chain_df = fetch_options_snapshot_with_quotes(
        symbols, as_of, max_expiries=args.max_expiries
    )
    fetched = (
        sorted(set(chain_df["symbol"].to_list()))
        if not chain_df.is_empty() else []
    )
    missing = sorted(set(symbols) - set(fetched))
    print(f"fetched chains: {len(fetched)} / {len(symbols)} | "
          f"chain rows: {len(chain_df)}")
    if missing:
        print(f"no chain (illiquid / 403 / no listed options): {', '.join(missing)}")

    con = connect_write(str(args.db))
    try:
        n1 = upsert_options(con, snapshot_df)
        n2 = upsert_options_analysis(con, analysis_df)
        n3 = upsert_options_chain_quotes(con, chain_df)
        print(f"upserted: snapshot={n1} analysis={n2} chain_quotes={n3}")

        # Price backfill — VRP / sentiment need price history (realised vol).
        # AI-infra ADRs (TSM/ASML/ASX/ABB ...) the S&P/Nasdaq scan misses have
        # no prices_daily rows, which blocks their options verdict AND their
        # stock trade plan. yfinance can backfill history (unlike CBOE).
        missing_prices = [
            s for s in symbols
            if con.execute(
                "SELECT COUNT(*) FROM prices_daily WHERE symbol = ? AND date <= ?",
                [s, as_of.isoformat()],
            ).fetchone()[0] == 0
        ]
        if missing_prices:
            print(f"price backfill ({len(missing_prices)}): {', '.join(missing_prices)}")
            try:
                n_px = fetch_and_store_prices(con, missing_prices, init=True)
                print(f"  fetched {n_px} price rows")
            except Exception as exc:  # noqa: BLE001
                print(f"  warn: price backfill failed ({exc})")

        # Recompute VRP + sentiment for the fetched names so iv/vrp/skew/pc
        # land in options_sentiment for the 逐票复核 verdict.
        target = fetched or symbols
        vrp_rows = compute_vrp(con, target, as_of)
        store_vrp(con, vrp_rows, as_of)
        ewma_rows = compute_sentiment_ewma(con, target, as_of)
        store_sentiment(con, ewma_rows, as_of)
        print(f"recomputed: vrp={len(vrp_rows)} sentiment_ewma={len(ewma_rows)}")
    finally:
        con.close()

    print(f"AI-infra options backfill complete for as-of {as_of.isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
