"""Tests for the ingestion freshness gate (pure logic)."""
from __future__ import annotations

import datetime as dt
import importlib
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]


def _load():
    sys.path.insert(0, str(STACK_ROOT / "ops"))
    return importlib.import_module("freshness_gate")


SOURCES = [
    {"source": "tushare", "criticality": "critical", "max_staleness_days": 3},
    {"source": "akshare", "criticality": "optional", "max_staleness_days": 7},
]
TODAY = dt.date(2026, 6, 27)


class FreshnessGateTests(unittest.TestCase):
    def setUp(self):
        self.mod = _load()

    def test_all_fresh_passes(self):
        state = {
            "tushare": {"status": "ok", "as_of": dt.date(2026, 6, 26)},
            "akshare": {"status": "ok", "as_of": dt.date(2026, 6, 25)},
        }
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertTrue(ok)
        self.assertEqual(crit, [])
        self.assertEqual(opt, [])

    def test_critical_missing_fails(self):
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, {}, TODAY)
        self.assertFalse(ok)
        self.assertIn("tushare", crit)

    def test_critical_too_old_fails(self):
        state = {"tushare": {"status": "ok", "as_of": dt.date(2026, 6, 20)}}  # 7d > 3d
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertFalse(ok)
        self.assertIn("tushare", crit)

    def test_critical_error_status_fails(self):
        state = {"tushare": {"status": "error", "as_of": dt.date(2026, 6, 26)}}
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertFalse(ok)
        self.assertIn("tushare", crit)

    def test_optional_stale_still_passes(self):
        state = {"tushare": {"status": "ok", "as_of": dt.date(2026, 6, 26)}}  # akshare missing
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertTrue(ok)          # optional staleness does not fail the gate
        self.assertIn("akshare", opt)


if __name__ == "__main__":
    unittest.main()
