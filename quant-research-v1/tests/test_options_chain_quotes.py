from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
import polars as pl


SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_ROOT))

from quant_bot.data_ingestion.options import upsert_options_chain_quotes  # noqa: E402


class OptionsChainQuoteTests(unittest.TestCase):
    def test_upsert_options_chain_quotes_persists_bid_ask_legs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            con = duckdb.connect(str(Path(tmpdir) / "quotes.duckdb"))
            df = pl.DataFrame(
                [
                    {
                        "symbol": "AAPL",
                        "as_of": date(2026, 4, 1),
                        "expiry": "2026-04-17",
                        "days_to_exp": 16,
                        "current_price": 200.0,
                        "contract_symbol": "AAPL260417C00200000",
                        "option_type": "call",
                        "strike": 200.0,
                        "bid": 5.1,
                        "ask": 5.3,
                        "mid": 5.2,
                        "last_price": 5.15,
                        "volume": 120,
                        "open_interest": 340,
                        "implied_volatility": 0.32,
                        "delta": 0.52,
                        "gamma": 0.02,
                        "theta": -0.04,
                        "vega": 0.21,
                        "source": "cboe_delayed",
                    }
                ],
                infer_schema_length=None,
            ).with_columns(pl.col("as_of").cast(pl.Date))

            self.assertEqual(upsert_options_chain_quotes(con, df), 1)
            row = con.execute(
                """
                SELECT symbol, option_type, strike, bid, ask, implied_volatility
                FROM options_chain_quotes
                WHERE contract_symbol = 'AAPL260417C00200000'
                """
            ).fetchone()
            con.close()

            self.assertEqual(row, ("AAPL", "call", 200.0, 5.1, 5.3, 0.32))


if __name__ == "__main__":
    unittest.main()
