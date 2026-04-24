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

from quant_bot.analytics.algorithm_postmortem import (  # noqa: E402
    build_algorithm_postmortem_summary,
    materialize_algorithm_postmortem,
)
from quant_bot.storage.db import DDL  # noqa: E402


class AlgorithmPostmortemTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = duckdb.connect(":memory:")
        self.con.execute(DDL)

    def tearDown(self) -> None:
        self.con.close()

    def _insert_decision(
        self,
        symbol: str,
        *,
        selection_status: str = "selected",
        report_bucket: str = "core",
        direction: str = "long",
        execution_mode: str | None = "executable_now",
        headline_mode: str = "normal",
        rr_ratio: float | None = 2.0,
        expected_move_pct: float = 4.0,
        details_json: str = "{}",
    ) -> None:
        self.con.execute(
            """
            INSERT INTO report_decisions (
                report_date, session, symbol, selection_status, rank_order,
                report_bucket, signal_direction, signal_confidence, headline_mode, execution_mode,
                entry_price, reference_price, rr_ratio, expected_move_pct, details_json
            )
            VALUES ('2026-04-20', 'pre', ?, ?, 1, ?, ?, 'HIGH', ?, ?, 100, 100, ?, ?, ?)
            """,
            [
                symbol,
                selection_status,
                report_bucket,
                direction,
                headline_mode,
                execution_mode,
                rr_ratio,
                expected_move_pct,
                details_json,
            ],
        )

    def _insert_outcome(
        self,
        symbol: str,
        *,
        selection_status: str = "selected",
        next_open: float = 100.0,
        next_close: float = 103.0,
        hold_3d_close: float = 106.0,
        next_close_ret_pct: float = 3.0,
        hold_3d_ret_pct: float = 6.0,
        move_consumed_ratio: float = 0.2,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO report_outcomes (
                report_date, session, symbol, selection_status, evaluation_date,
                next_trade_date, entry_price, reference_price, next_open, next_close,
                hold_3d_close, next_open_ret_pct, next_close_ret_pct, hold_3d_ret_pct,
                move_consumed_ratio, alpha_remaining_pct, data_ready
            )
            VALUES (
                '2026-04-20', 'pre', ?, ?, '2026-04-24', '2026-04-21',
                100, 100, ?, ?, ?, 0, ?, ?, ?, 5, TRUE
            )
            """,
            [
                symbol,
                selection_status,
                next_open,
                next_close,
                hold_3d_close,
                next_close_ret_pct,
                hold_3d_ret_pct,
                move_consumed_ratio,
            ],
        )

    def test_trade_now_uses_next_open_fill_and_3d_exit(self) -> None:
        self._insert_decision("AAA")
        self._insert_outcome("AAA", next_open=101.0, hold_3d_close=106.0)

        inserted = materialize_algorithm_postmortem(self.con, date(2026, 4, 24))

        self.assertEqual(inserted, 1)
        row = self.con.execute(
            """
            SELECT action_label, action_intent, executable, ROUND(realized_pnl_pct, 3), label, fill_quality
            FROM algorithm_postmortem
            WHERE symbol = 'AAA'
            """
        ).fetchone()
        self.assertEqual(row, ("TRADE_NOW", "TRADE", True, 4.95, "won_and_executable", "captured"))

    def test_do_not_chase_is_not_counted_as_executable_capture(self) -> None:
        self._insert_decision("LATE", execution_mode="do_not_chase")
        self._insert_outcome("LATE", hold_3d_ret_pct=8.0, move_consumed_ratio=1.2)

        materialize_algorithm_postmortem(self.con, date(2026, 4, 24))

        row = self.con.execute(
            """
            SELECT action_label, executable, stale_chase, no_fill_reason, label
            FROM algorithm_postmortem
            WHERE symbol = 'LATE'
            """
        ).fetchone()
        self.assertEqual(row, ("DO_NOT_CHASE", False, True, "do_not_chase", "stale_chase"))

    def test_non_core_selected_item_is_observation_not_trade_feedback(self) -> None:
        self._insert_decision("OBS", report_bucket="event_tape", execution_mode="executable_now")
        self._insert_outcome("OBS", hold_3d_ret_pct=6.0, move_consumed_ratio=1.2)

        materialize_algorithm_postmortem(self.con, date(2026, 4, 24))

        row = self.con.execute(
            """
            SELECT action_label, action_intent, executable, stale_chase, no_fill_reason, label, feedback_action
            FROM algorithm_postmortem
            WHERE symbol = 'OBS'
            """
        ).fetchone()
        self.assertEqual(
            row,
            (
                "OBSERVE",
                "OBSERVE",
                False,
                False,
                "report_bucket_observation",
                "observed_alpha",
                None,
            ),
        )

    def test_uncertain_headline_core_item_is_observation(self) -> None:
        self._insert_decision("UNC", report_bucket="core", headline_mode="uncertain")
        self._insert_outcome("UNC", hold_3d_ret_pct=6.0)

        materialize_algorithm_postmortem(self.con, date(2026, 4, 24))

        row = self.con.execute(
            """
            SELECT action_label, action_intent, label, feedback_action
            FROM algorithm_postmortem
            WHERE symbol = 'UNC'
            """
        ).fetchone()
        self.assertEqual(row, ("OBSERVE", "OBSERVE", "observed_alpha", None))

    def test_blocked_main_signal_gate_overrides_trade_default(self) -> None:
        self._insert_decision(
            "GATED",
            details_json=(
                '{"main_signal_gate": {"status": "blocked", "role": "directional_observation", '
                '"action_intent": "OBSERVE", "blockers": ["headline_gate_range"]}}'
            ),
        )
        self._insert_outcome("GATED", hold_3d_ret_pct=6.0)

        materialize_algorithm_postmortem(self.con, date(2026, 4, 24))

        row = self.con.execute(
            """
            SELECT action_label, action_source, action_intent, label, feedback_action
            FROM algorithm_postmortem
            WHERE symbol = 'GATED'
            """
        ).fetchone()
        self.assertEqual(row, ("OBSERVE", "main_signal_gate", "OBSERVE", "observed_alpha", None))

    def test_ignored_follow_through_becomes_missed_alpha(self) -> None:
        self._insert_decision("MISS", selection_status="ignored")
        self._insert_outcome("MISS", selection_status="ignored", hold_3d_ret_pct=5.0)

        materialize_algorithm_postmortem(self.con, date(2026, 4, 24))
        summary = build_algorithm_postmortem_summary(self.con, date(2026, 4, 24), "pre")

        row = self.con.execute(
            "SELECT action_label, label, feedback_action FROM algorithm_postmortem WHERE symbol = 'MISS'"
        ).fetchone()
        self.assertEqual(row, ("WAIT", "missed_alpha", "boost_recall"))
        self.assertEqual(summary["missed_alpha_count"], 1)
        self.assertTrue(summary["calibration_buckets"])


if __name__ == "__main__":
    unittest.main()
