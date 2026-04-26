from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from quant_bot.analytics.options_alpha import compute_options_alpha, store_options_alpha


class OptionsAlphaTests(unittest.TestCase):
    def test_compute_options_alpha_filters_stale_expiry_and_selects_expression(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute(
                """
                CREATE TABLE options_analysis (
                    symbol VARCHAR,
                    as_of DATE,
                    expiry VARCHAR,
                    days_to_exp INTEGER,
                    current_price DOUBLE,
                    range_68_low DOUBLE,
                    range_68_high DOUBLE,
                    range_95_low DOUBLE,
                    range_95_high DOUBLE,
                    atm_iv DOUBLE,
                    iv_skew DOUBLE,
                    put_call_vol_ratio DOUBLE,
                    bias_signal VARCHAR,
                    liquidity_score VARCHAR,
                    chain_width INTEGER,
                    avg_spread_pct DOUBLE,
                    unusual_strikes VARCHAR,
                    PRIMARY KEY (symbol, as_of, expiry)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE options_sentiment (
                    symbol VARCHAR,
                    as_of DATE,
                    pc_ratio_z DOUBLE,
                    skew_z DOUBLE,
                    vrp DOUBLE,
                    iv_ann DOUBLE,
                    rv_ann DOUBLE,
                    vrp_z DOUBLE,
                    pc_ratio_raw DOUBLE,
                    skew_raw DOUBLE,
                    PRIMARY KEY (symbol, as_of)
                )
                """
            )
            unusual_calls = json.dumps(
                [{"type": "call", "strike": 105, "volume": 12000, "vol_oi_ratio": 12.0}]
            )
            con.execute(
                """
                INSERT INTO options_analysis
                VALUES
                ('ALFA', DATE '2026-04-24', '2024-01-19', 30, 100, 95, 105, 90, 110,
                 0.30, 0.90, 0.30, 'bullish', 'good', 20, 5.0, ?),
                ('ALFA', DATE '2026-04-24', '2026-05-15', 21, 100, 95, 105, 90, 110,
                 0.30, 0.90, 0.30, 'bullish', 'good', 20, 5.0, ?)
                """,
                [unusual_calls, unusual_calls],
            )
            con.execute(
                """
                INSERT INTO options_sentiment
                VALUES ('ALFA', DATE '2026-04-24', -2.0, -1.5, -0.05, 0.30, 0.40, NULL, 0.3, 0.9)
                """
            )

            rows = compute_options_alpha(con, ["ALFA"], date(2026, 4, 24))
            stored = store_options_alpha(con, rows, date(2026, 4, 24))

            self.assertEqual(stored, 1)
            self.assertEqual(rows[0]["symbol"], "ALFA")
            self.assertEqual(rows[0]["expression"], "call_spread")
            detail = json.loads(rows[0]["detail_json"])
            self.assertEqual(detail["expiry"], "2026-05-15")
            self.assertGreater(rows[0]["directional_edge"], 0.45)
            self.assertGreater(rows[0]["vol_edge"], 0.10)


if __name__ == "__main__":
    unittest.main()
