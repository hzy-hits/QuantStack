from __future__ import annotations

import unittest
from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from src import data_readiness


class DataReadinessTests(unittest.TestCase):
    def test_expected_us_data_date_uses_new_york_calendar_day(self):
        now = datetime(2026, 4, 1, 4, 35, tzinfo=ZoneInfo("Asia/Shanghai"))

        expected = data_readiness.expected_us_data_date(now)

        self.assertEqual(expected, date(2026, 3, 31))

    @patch("src.data_readiness.latest_trade_date", return_value=date(2026, 3, 30))
    def test_market_data_ready_fails_when_latest_date_is_stale(self, _latest_trade_date):
        ready, latest, expected = data_readiness.market_data_ready(
            "us",
            expected_date=date(2026, 3, 31),
        )

        self.assertFalse(ready)
        self.assertEqual(latest, date(2026, 3, 30))
        self.assertEqual(expected, date(2026, 3, 31))

    @patch("src.data_readiness.latest_trade_date", return_value=date(2026, 3, 31))
    def test_market_data_ready_passes_when_latest_date_matches_expected(self, _latest_trade_date):
        ready, latest, expected = data_readiness.market_data_ready(
            "us",
            expected_date=date(2026, 3, 31),
        )

        self.assertTrue(ready)
        self.assertEqual(latest, date(2026, 3, 31))
        self.assertEqual(expected, date(2026, 3, 31))

    @patch("src.data_readiness.latest_trade_date", return_value=date(2026, 5, 15))
    def test_monday_gap_tolerated_with_staleness(self, _latest):
        # 2026-05-18 is a Monday; the freshest US close is Friday 05-15.
        # With a staleness budget the Monday run is still ready.
        ready, latest, expected = data_readiness.market_data_ready(
            "us", expected_date=date(2026, 5, 18), max_staleness_days=5,
        )
        self.assertTrue(ready)
        self.assertEqual(latest, date(2026, 5, 15))

    @patch("src.data_readiness.latest_trade_date", return_value=date(2026, 5, 15))
    def test_genuine_multi_day_outage_still_fails(self, _latest):
        # Data 14 days stale is a real outage — staleness budget must not hide it.
        ready, _latest_date, _expected = data_readiness.market_data_ready(
            "us", expected_date=date(2026, 5, 29), max_staleness_days=5,
        )
        self.assertFalse(ready)


if __name__ == "__main__":
    unittest.main()
