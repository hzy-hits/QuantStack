"""Tests for the missed-task catch-up runner's cron logic."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "ops" / "catch_up.py"


def _load_module():
    if "catch_up" in sys.modules:
        return sys.modules["catch_up"]
    spec = importlib.util.spec_from_file_location("catch_up", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["catch_up"] = module
    spec.loader.exec_module(module)
    return module


class CronFieldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_star_expands_full_range(self) -> None:
        self.assertEqual(self.m._parse_field("*", 0, 5), {0, 1, 2, 3, 4, 5})

    def test_range(self) -> None:
        self.assertEqual(self.m._parse_field("1-5", 0, 7), {1, 2, 3, 4, 5})

    def test_comma_list(self) -> None:
        self.assertEqual(self.m._parse_field("12,27,42,57", 0, 59), {12, 27, 42, 57})


class CronMatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_weekday_match(self) -> None:
        # 2026-05-19 is a Tuesday.
        self.assertTrue(self.m._matches("0 10 * * 1-5", datetime(2026, 5, 19, 10, 0)))

    def test_weekend_no_match(self) -> None:
        # 2026-05-23 is a Saturday — weekday-only schedule must not match.
        self.assertFalse(self.m._matches("0 10 * * 1-5", datetime(2026, 5, 23, 10, 0)))

    def test_wrong_minute_no_match(self) -> None:
        self.assertFalse(self.m._matches("0 10 * * 1-5", datetime(2026, 5, 19, 10, 1)))

    def test_sunday_is_zero(self) -> None:
        # 2026-05-24 is a Sunday; cron dow 0 = Sunday.
        self.assertTrue(self.m._matches("0 0 * * 0", datetime(2026, 5, 24, 0, 0)))

    def test_reboot_or_malformed_never_matches(self) -> None:
        self.assertFalse(self.m._matches("@reboot", datetime(2026, 5, 19, 10, 0)))


class MostRecentFireTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_finds_earlier_today(self) -> None:
        fire = self.m.most_recent_fire("0 10 * * 1-5", datetime(2026, 5, 19, 12, 0))
        self.assertEqual(fire, datetime(2026, 5, 19, 10, 0))

    def test_before_todays_slot_reaches_back(self) -> None:
        # 09:00 on Tuesday — today's 10:00 slot has not happened; the most
        # recent fire is the prior weekday (Monday 2026-05-18).
        fire = self.m.most_recent_fire("0 10 * * 1-5", datetime(2026, 5, 19, 9, 0))
        self.assertEqual(fire, datetime(2026, 5, 18, 10, 0))


if __name__ == "__main__":
    unittest.main()
