from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_ROOT = REPO_ROOT.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "rank_event_option_spreads.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("rank_event_option_spreads", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ranker = _load_module()


class EventOptionSpreadRankerTests(unittest.TestCase):
    def test_ba_china_order_fixture_prefers_balanced_235_250(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "options.duckdb"
            con = duckdb.connect(str(db))
            con.execute(
                """
                CREATE TABLE options_chain_quotes (
                    symbol VARCHAR, as_of DATE, expiry VARCHAR, days_to_exp INTEGER,
                    current_price DOUBLE, contract_symbol VARCHAR, option_type VARCHAR,
                    strike DOUBLE, bid DOUBLE, ask DOUBLE, mid DOUBLE, last_price DOUBLE,
                    volume BIGINT, open_interest BIGINT, implied_volatility DOUBLE,
                    delta DOUBLE, gamma DOUBLE, theta DOUBLE, vega DOUBLE, source VARCHAR
                )
                """
            )
            rows = [
                (230, 5.60, 7.20, 6.40, 140, 639, 0.3463, 0.5242, -0.2076),
                (235, 4.20, 4.80, 4.50, 111, 1117, 0.3383, 0.4046, -0.1989),
                (240, 2.66, 3.10, 2.88, 330, 391, 0.3361, 0.2938, -0.1744),
                (245, 1.33, 1.93, 1.63, 80, 486, 0.3263, 0.2016, -0.1411),
                (250, 0.79, 1.10, 0.945, 88, 529, 0.3277, 0.1330, -0.1073),
                (255, 0.38, 0.75, 0.565, 24, 422, 0.3353, 0.0857, -0.0784),
            ]
            for strike, bid, ask, mid, vol, oi, iv, delta, theta in rows:
                con.execute(
                    """
                    INSERT INTO options_chain_quotes
                    VALUES ('BA', '2026-05-06', '2026-05-22', 16, 229.75,
                            ?, 'call', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, 'fixture')
                    """,
                    [
                        f"BA260522C{int(strike * 1000):08d}",
                        strike,
                        bid,
                        ask,
                        mid,
                        mid,
                        vol,
                        oi,
                        iv,
                        delta,
                        theta,
                    ],
                )
            ranked = ranker.rank_spreads(
                con,
                symbols=["BA"],
                expiry="2026-05-22",
                as_of=date(2026, 5, 6),
                target_prices={"BA": 245.0},
            )
            con.close()

            self.assertGreaterEqual(len(ranked), 1)
            top = ranked[0]
            self.assertEqual((top.long_strike, top.short_strike), (235.0, 250.0))
            self.assertEqual(top.action, "main_candidate")
            self.assertAlmostEqual(top.start_limit, 3.70)
            self.assertAlmostEqual(top.chase_limit, 4.25)


if __name__ == "__main__":
    unittest.main()
