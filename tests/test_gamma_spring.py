from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from sections.gamma_spring import build_gamma_spring_snapshot, render_gamma_spring_section  # noqa: E402


def _seed(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE options_chain_quotes (
                symbol VARCHAR,
                as_of DATE,
                expiry VARCHAR,
                days_to_exp INTEGER,
                current_price DOUBLE,
                contract_symbol VARCHAR,
                option_type VARCHAR,
                strike DOUBLE,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                last_price DOUBLE,
                volume BIGINT,
                open_interest BIGINT,
                implied_volatility DOUBLE,
                delta DOUBLE,
                gamma DOUBLE,
                theta DOUBLE,
                vega DOUBLE,
                source VARCHAR
            )
            """
        )
        rows = [
            # Positive call gamma dominates -> pinned/reversion state.
            ("PIN", "call", 100.0, 0.050, 10000, 10),
            ("PIN", "call", 105.0, 0.030, 7000, 8),
            ("PIN", "put", 95.0, 0.010, 1000, 5),
            # Put gamma dominates -> negative gamma accelerator.
            ("ACC", "put", 100.0, 0.050, 10000, 20),
            ("ACC", "put", 95.0, 0.030, 7000, 10),
            ("ACC", "call", 105.0, 0.010, 1000, 3),
        ]
        for idx, (sym, typ, strike, gamma, oi, vol) in enumerate(rows):
            con.execute(
                """
                INSERT INTO options_chain_quotes VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, ?, NULL, NULL, 'test')
                """,
                [
                    sym,
                    date(2026, 5, 29),
                    "2026-06-19",
                    20,
                    100.0,
                    f"{sym}{idx}",
                    typ,
                    strike,
                    vol,
                    oi,
                    gamma,
                ],
            )
    finally:
        con.close()


class GammaSpringTests(unittest.TestCase):
    def test_build_snapshot_classifies_positive_and_negative_gamma(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            _seed(db)
            snap = build_gamma_spring_snapshot(db, ["PIN", "ACC"], date(2026, 5, 31), max_dte=45)
            by_symbol = {row["symbol"]: row for row in snap["rows"]}

            self.assertEqual(snap["effective_date"], "2026-05-29")
            self.assertGreater(by_symbol["PIN"]["net_gamma_ratio"], 0)
            self.assertIn(by_symbol["PIN"]["state"], {"PINNED_GAMMA_WELL", "GAMMA_REVERSION_BAND"})
            self.assertLess(by_symbol["ACC"]["net_gamma_ratio"], 0)
            self.assertEqual(by_symbol["ACC"]["state"], "NEGATIVE_GAMMA_ACCELERATOR")

    def test_render_section_is_report_safe_context(self) -> None:
        payload = {
            "gamma_spring": {
                "effective_date": "2026-05-29",
                "rows": [
                    {
                        "symbol": "PIN",
                        "state": "PINNED_GAMMA_WELL",
                        "spot": 100.0,
                        "center_strike": 100.5,
                        "displacement_pct": -0.005,
                        "abs_gex_1pct": 1_000_000.0,
                        "net_gamma_ratio": 0.8,
                        "wall_below": 100.0,
                        "wall_above": 105.0,
                        "damping_ratio": 0.2,
                    }
                ],
            }
        }
        text = "\n".join(render_gamma_spring_section(payload))
        self.assertIn("US Gamma Spring", text)
        self.assertIn("股票买卖管理", text)
        self.assertIn("不是期权交易指令", text)
        self.assertIn("PINNED_GAMMA_WELL", text)


if __name__ == "__main__":
    unittest.main()
