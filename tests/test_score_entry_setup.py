"""Tests for the entry-setup gate classifier."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_entry_setup.py"


def _load_module():
    if "score_entry_setup" in sys.modules:
        return sys.modules["score_entry_setup"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_entry_setup", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class EntrySetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_pullback_near_ema20_above_ema50_is_setup(self) -> None:
        d = self.m.classify_entry_setup(close=101.0, ema20=100.0, ema50=95.0)
        self.assertTrue(d.has_setup)
        self.assertEqual(d.setup_type, "pullback")

    def test_below_ema50_is_trend_broken_no_setup(self) -> None:
        d = self.m.classify_entry_setup(close=94.0, ema20=100.0, ema50=95.0)
        self.assertFalse(d.has_setup)
        self.assertEqual(d.setup_type, "trend_broken")

    def test_far_above_ema20_is_extended_no_setup(self) -> None:
        d = self.m.classify_entry_setup(close=120.0, ema20=100.0, ema50=95.0)
        self.assertFalse(d.has_setup)
        self.assertEqual(d.setup_type, "extended")

    def test_pullback_below_ema20_but_above_ema50_is_setup(self) -> None:
        # Below EMA20 yet holding EMA50 is the classic buy-the-dip zone.
        d = self.m.classify_entry_setup(close=96.0, ema20=100.0, ema50=95.0)
        self.assertTrue(d.has_setup)
        self.assertEqual(d.setup_type, "pullback")

    def test_extended_threshold_is_configurable(self) -> None:
        d = self.m.classify_entry_setup(close=110.0, ema20=100.0, ema50=95.0,
                                        extended_pct=15.0)
        self.assertTrue(d.has_setup)  # +10% is within a 15% tolerance

    def test_missing_data_fails_open(self) -> None:
        d = self.m.classify_entry_setup(close=100.0, ema20=None, ema50=95.0)
        self.assertTrue(d.has_setup)          # do not block on missing data
        self.assertEqual(d.setup_type, "no_data")

    def test_setup_from_short_series_fails_open(self) -> None:
        d = self.m.setup_from_closes([100.0] * 10)
        self.assertTrue(d.has_setup)
        self.assertEqual(d.setup_type, "no_data")

    def test_setup_from_closes_uptrend_pullback(self) -> None:
        # 60 rising closes, last one a small dip → above EMA50, near EMA20.
        closes = [100.0 + i for i in range(60)]
        closes[-1] = closes[-2] - 1.0
        d = self.m.setup_from_closes(closes)
        self.assertEqual(d.setup_type, "pullback")
        self.assertTrue(d.has_setup)

    def test_ema_is_causal_length_preserved(self) -> None:
        e = self.m.ema([1.0, 2.0, 3.0, 4.0], 2)
        self.assertEqual(len(e), 4)
        self.assertEqual(e[0], 1.0)


if __name__ == "__main__":
    unittest.main()
