from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.mining import daily_pipeline
from src.mining import export_to_pipeline
from src.mining import export_sleeve_returns


class DailyPipelineFeedbackTests(unittest.TestCase):
    def test_init_db_adds_contract_ledger_sleeve_and_health_tables_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "factor_lab.duckdb"
            with mock.patch.object(daily_pipeline, "FACTOR_LAB_DB", db_path):
                daily_pipeline.init_db()
                daily_pipeline.init_db()

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                registry_cols = {row[1] for row in con.execute("PRAGMA table_info('factor_registry')").fetchall()}
                self.assertIn("sleeve_id", registry_cols)
                self.assertIn("report_contract", registry_cols)
                self.assertIn("money_readiness", registry_cols)
                for table in ("factor_experiment_ledger", "factor_sleeve_returns", "factor_health_daily"):
                    row = con.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
                        [table],
                    ).fetchone()
                    self.assertEqual(row[0], 1)
            finally:
                con.close()

    def test_short_direction_orients_factor_values_before_health_checks(self) -> None:
        raw = pd.DataFrame(
            {
                "ts_code": ["AAA", "BBB"],
                "trade_date": pd.to_datetime(["2026-04-23", "2026-04-23"]),
                "factor_value": [1.0, -2.0],
            }
        )

        oriented = daily_pipeline._orient_factor_values_for_direction(raw, "short")

        self.assertEqual(oriented["factor_value"].tolist(), [-1.0, 2.0])
        self.assertEqual(raw["factor_value"].tolist(), [1.0, -2.0])

    def test_export_short_direction_orients_rolling_factor_frame(self) -> None:
        raw = pd.DataFrame(
            {
                "ts_code": ["AAA", "BBB"],
                "trade_date": pd.to_datetime(["2026-04-23", "2026-04-23"]),
                "factor_value": [0.25, 0.75],
            }
        )

        oriented = export_to_pipeline._orient_factor_values_for_direction(raw, "short")

        self.assertEqual(oriented["factor_value"].tolist(), [-0.25, -0.75])
        self.assertEqual(raw["factor_value"].tolist(), [0.25, 0.75])

    def test_export_direction_uses_weighted_ic_before_stored_default(self) -> None:
        self.assertEqual(
            export_to_pipeline._resolve_direction("long", -0.04, -0.01, 0.01),
            "short",
        )
        self.assertEqual(
            export_to_pipeline._resolve_direction("short", 0.03, 0.01, -0.01),
            "long",
        )

    def test_load_report_feedback_prefers_algorithm_postmortem_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pipeline.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute(
                """
                CREATE TABLE algorithm_postmortem (
                    symbol VARCHAR,
                    label VARCHAR,
                    evaluation_date DATE,
                    feedback_action VARCHAR,
                    feedback_weight DOUBLE
                )
                """
            )
            con.executemany(
                """
                INSERT INTO algorithm_postmortem
                VALUES (?, ?, CURRENT_DATE, ?, ?)
                """,
                [
                    ("AAA", "missed_alpha", "boost_recall", 1.0),
                    ("BBB", "right_but_no_fill", "penalize_stale_chase", 0.8),
                    ("CCC", "false_positive_executable", "penalize_false_positive", 0.7),
                    ("DDD", "won_and_executable", "reward_executable_capture", 0.5),
                ],
            )
            con.close()

            with (
                mock.patch.object(daily_pipeline, "QUANT_US_DB", db_path),
                mock.patch.object(daily_pipeline, "QUANT_US_REPORT_DB", Path(tmpdir) / "none.duckdb"),
            ):
                feedback = daily_pipeline._load_report_feedback("us")

        self.assertEqual(feedback["missed_alpha"], {"AAA": 1.0})
        self.assertEqual(feedback["stale"], {"BBB": 0.8})
        self.assertEqual(feedback["false_positive"], {"CCC": 0.7})
        self.assertEqual(feedback["captured"], {"DDD": 0.5})

    def test_load_report_feedback_skips_observation_only_postmortem_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pipeline.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute(
                """
                CREATE TABLE algorithm_postmortem (
                    symbol VARCHAR,
                    label VARCHAR,
                    evaluation_date DATE,
                    feedback_action VARCHAR,
                    feedback_weight DOUBLE,
                    action_label VARCHAR,
                    detail_json VARCHAR
                )
                """
            )
            con.executemany(
                """
                INSERT INTO algorithm_postmortem
                VALUES (?, ?, CURRENT_DATE, ?, ?, ?, ?)
                """,
                [
                    ("OBS1", "stale_chase", "penalize_stale_chase", 1.0, "OBSERVE", "{}"),
                    (
                        "OBS2",
                        "false_positive_executable",
                        "penalize_false_positive",
                        1.0,
                        "TRADE_NOW",
                        '{"action_intent":"OBSERVE"}',
                    ),
                    ("TRADE", "won_and_executable", "reward_executable_capture", 0.5, "TRADE_NOW", "{}"),
                ],
            )
            con.close()

            with (
                mock.patch.object(daily_pipeline, "QUANT_US_DB", db_path),
                mock.patch.object(daily_pipeline, "QUANT_US_REPORT_DB", Path(tmpdir) / "none.duckdb"),
            ):
                feedback = daily_pipeline._load_report_feedback("us")

        self.assertEqual(feedback["stale"], {})
        self.assertEqual(feedback["false_positive"], {})
        self.assertEqual(feedback["captured"], {"TRADE": 0.5})

    def test_report_feedback_records_shadow_alpha_overlap_aliases(self) -> None:
        candidate = {
            "factor_id": "demo",
            "direction": "long",
            "composite_score": 1.0,
            "rank_score": 1.0,
            "sigreg_n_eff_shrinkage": 0.5,
            "_latest_values": pd.Series(
                {
                    "AAA": 3.0,
                    "BBB": 2.0,
                    "CCC": 1.0,
                }
            ),
        }

        with mock.patch.object(
            daily_pipeline,
            "_load_report_feedback",
            return_value={
                "missed_alpha": {"AAA": 1.0},
                "stale": {"BBB": 1.0},
                "false_positive": {},
                "captured": {"CCC": 1.0},
            },
        ):
            adjusted = daily_pipeline._apply_report_feedback([candidate], "us")

        detail = adjusted[0]["report_feedback_detail"]
        self.assertIn("missed_alpha_overlap", detail)
        self.assertIn("stale_chase_overlap", detail)
        self.assertEqual(detail["stale_chase_overlap"], detail["stale_overlap"])
        self.assertIn("captured_overlap", detail)
        self.assertEqual(detail["captured_overlap"], detail["capture_overlap"])
        self.assertLessEqual(adjusted[0]["report_feedback_multiplier"], 1.20)

    def test_compute_factor_sleeve_rows_generates_top_quintile_daily_return(self) -> None:
        symbols = [f"S{i:02d}" for i in range(30)]
        factor_df = pd.DataFrame(
            {
                "ts_code": symbols,
                "trade_date": pd.Timestamp("2026-05-01"),
                "factor_value": list(range(30)),
            }
        )
        fwd_returns = pd.DataFrame(
            {
                "ts_code": symbols,
                "trade_date": pd.Timestamp("2026-05-01"),
                "fwd_5d": [0.0] * 24 + [0.10] * 6,
            }
        )

        rows = export_sleeve_returns._compute_factor_sleeve_rows(
            market="cn",
            factor_id="f1",
            factor_name="demo",
            sleeve_id="daily_price_overlay",
            report_contract="research_only",
            money_readiness="research_only",
            direction="long",
            factor_df=factor_df,
            fwd_returns=fwd_returns,
            start="2026-05-01",
            as_of="2026-05-01",
            cost_per_trade=0.003,
        )

        top = [row for row in rows if row["bucket"] == "top_quintile_long"][0]
        self.assertAlmostEqual(top["gross_return_pct"], 2.0)
        self.assertAlmostEqual(top["cost_adjusted_return_pct"], 1.94)
        self.assertEqual(top["top_bucket_count"], 6)


if __name__ == "__main__":
    unittest.main()
