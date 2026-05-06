from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = REPO_ROOT / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import cn_observed_lifecycle_prob as observed  # noqa: E402


def _make_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE strategy_model_dataset (
            report_date DATE, evaluation_date DATE, symbol VARCHAR,
            selection_status VARCHAR, strategy_family VARCHAR, strategy_key VARCHAR,
            action_intent VARCHAR, alpha_state VARCHAR, features_json VARCHAR,
            detail_json VARCHAR, fill_status VARCHAR, fill_date DATE, fill_price DOUBLE,
            exit_date DATE, exit_price DOUBLE, realized_ret_pct DOUBLE,
            max_favorable_pct DOUBLE, max_adverse_pct DOUBLE, risk_unit_pct DOUBLE,
            ev_pct DOUBLE, ev_lcb_80_pct DOUBLE, ev_norm_score DOUBLE,
            ev_norm_lcb_80 DOUBLE
        )
        """
    )
    features = json.dumps(
        {
            "execution_mode": "wait_pullback",
            "rsi_14": 27.0,
            "ret_20d": -14.0,
            "ret_5d": -6.0,
            "fade_risk": 0.25,
            "stale_chase_risk": 0.2,
            "setup_score": 0.7,
            "flow_conflict_flag": False,
            "market_p_high_vol": 0.9,
        }
    )
    for idx in range(30):
        day = date(2026, 3, idx + 1).isoformat()
        con.execute(
            """
            INSERT INTO strategy_model_dataset
            VALUES (?, ?, ?, 'selected', 'oversold_contrarian', 'k',
                    'TRADE', 'research_setup', ?, '{}', 'filled_open',
                    ?, 10, ?, 10.4, 1.2, 2.6, -0.4, 2.0, 0.1, -0.2, 50, 20)
            """,
            [day, day, f"000{idx:03d}.SZ", features, day, day],
        )
    con.close()


class CnObservedLifecycleProbTests(unittest.TestCase):
    def test_current_row_receives_observed_probability_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cn.duckdb"
            _make_db(db_path)
            current = [
                {
                    "symbol": "000999.SZ",
                    "strategy_family": "oversold_contrarian",
                    "features_json": json.dumps(
                        {
                            "execution_mode": "wait_pullback",
                            "rsi_14": 27.0,
                            "ret_20d": -14.0,
                            "ret_5d": -6.0,
                            "fade_risk": 0.25,
                            "stale_chase_risk": 0.2,
                            "setup_score": 0.7,
                            "flow_conflict_flag": False,
                            "market_p_high_vol": 0.9,
                        }
                    ),
                }
            ]
            payload = observed.build_probability_payload(
                db_path=db_path,
                start=date(2026, 3, 1),
                as_of=date(2026, 4, 1),
                current_rows=current,
            )
            row = payload["rows"][0]

            self.assertEqual(payload["historical_n"], 30)
            self.assertEqual(row["observed_probability_source"], "exact_state")
            self.assertGreater(row["p_hit_1r_t3"], row["p_stop_t3"])
            self.assertGreater(row["expected_r_t3"], 0)
            self.assertGreater(row["lcb80_r_t3"], 0)
            self.assertTrue(row["observed_lifecycle_qualified"])
            self.assertEqual(row["observed_lifecycle_sleeve_id"], observed.OBSERVED_LIFECYCLE_SLEEVE)


if __name__ == "__main__":
    unittest.main()
