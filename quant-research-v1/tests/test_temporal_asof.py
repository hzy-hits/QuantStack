from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from quant_bot.filtering._events_loader import load_news  # noqa: E402
from quant_bot.reporting._bundle_tier1 import Tier1Builder  # noqa: E402


class TemporalAsOfTests(unittest.TestCase):
    def test_polymarket_context_uses_snapshots_available_as_of_trade_date(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            con.execute(
                """
                CREATE TABLE prices_daily (
                    symbol VARCHAR,
                    date DATE,
                    adj_close DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE polymarket_events (
                    market_id VARCHAR,
                    question VARCHAR,
                    p_yes DOUBLE,
                    p_no DOUBLE,
                    volume_usd DOUBLE,
                    end_date TIMESTAMP,
                    category VARCHAR,
                    fetched_at TIMESTAMP,
                    fetch_date DATE
                )
                """
            )
            con.execute(
                """
                INSERT INTO polymarket_events VALUES
                ('m1', 'Will rates fall?', 0.30, 0.70, 50000, '2026-12-31', 'macro', '2026-04-23 05:00:00', '2026-04-23'),
                ('m1', 'Will rates fall?', 0.40, 0.60, 50000, '2026-12-31', 'macro', '2026-04-24 05:00:00', '2026-04-24'),
                ('m1', 'Will rates fall?', 0.80, 0.20, 50000, '2026-12-31', 'macro', '2026-04-25 05:00:00', '2026-04-25')
                """
            )

            events = Tier1Builder(con, "2026-04-24", {}).build_polymarket()

            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["p_yes"], 0.40)
            self.assertEqual(events[0]["p_yes_delta"], 0.10)
            self.assertTrue(str(events[0]["fetched_at"]).startswith("2026-04-24"))
        finally:
            con.close()

    def test_symbol_news_loader_excludes_after_asof_news(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            con.execute(
                """
                CREATE TABLE news_items (
                    symbol VARCHAR,
                    headline VARCHAR,
                    summary VARCHAR,
                    source VARCHAR,
                    published_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                INSERT INTO news_items VALUES
                ('AAA', 'Known on trade date', 'ok', 'wire', '2026-04-24 20:00:00'),
                ('AAA', 'Future leak', 'bad', 'wire', '2026-04-25 01:00:00')
                """
            )

            news = load_news(con, date(2026, 4, 24))

            headlines = [row["headline"] for row in news.get("AAA", [])]
            self.assertEqual(headlines, ["Known on trade date"])
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
