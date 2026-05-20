"""Tests for the held→target position delta hint in the daily report."""
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load_module():
    if "v2_report" in sys.modules:
        return sys.modules["v2_report"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("v2_report", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["v2_report"] = module
    spec.loader.exec_module(module)
    return module


class PositionDeltaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_fresh_entry_no_decoration(self) -> None:
        # No holding → Action stays clean (already says 买入).
        self.assertEqual(self.m.position_delta_text(0.0, 0.044), "")
        self.assertEqual(self.m.position_delta_text(None, 0.125), "")

    def test_trim_on_press_day(self) -> None:
        # The canonical PRESS scenario: held 0.125 → target 0.044 (R=0.35x).
        text = self.m.position_delta_text(0.125, 0.044)
        self.assertIn("减", text)
        self.assertIn("-65%", text)
        self.assertIn("0.125", text)
        self.assertIn("0.044", text)

    def test_add_when_target_grows(self) -> None:
        text = self.m.position_delta_text(0.044, 0.125)
        self.assertIn("加", text)
        self.assertIn("+", text)

    def test_steady_when_essentially_equal(self) -> None:
        text = self.m.position_delta_text(0.125, 0.126)   # < 0.005 delta
        self.assertIn("持稳", text)

    def test_full_exit(self) -> None:
        # target → 0 still produces a 减 hint (note: row would normally
        # be filtered out of actionable before reaching here, but the
        # function is defensive).
        text = self.m.position_delta_text(0.125, 0.0)
        self.assertIn("减", text)
        self.assertIn("-100%", text)


class ManualHoldingsLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_missing_file_returns_empty(self) -> None:
        original = self.m.MANUAL_HOLDINGS_PATH
        try:
            self.m.MANUAL_HOLDINGS_PATH = Path("/tmp/__definitely_not_a_file__.yaml")
            self.assertEqual(self.m._load_manual_holdings(), {"US": {}, "CN": {}})
        finally:
            self.m.MANUAL_HOLDINGS_PATH = original

    def test_parses_us_and_cn_sections(self) -> None:
        original = self.m.MANUAL_HOLDINGS_PATH
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
                f.write("us:\n  AMZN: 0.125\n  nvda: 0.05\ncn:\n  002518.SZ: 0.075\n")
                tmp = f.name
            self.m.MANUAL_HOLDINGS_PATH = Path(tmp)
            h = self.m._load_manual_holdings()
            self.assertEqual(h["US"]["AMZN"], 0.125)
            self.assertEqual(h["US"]["NVDA"], 0.05)            # case-normalized
            self.assertEqual(h["CN"]["002518.SZ"], 0.075)
        finally:
            self.m.MANUAL_HOLDINGS_PATH = original

    def test_malformed_yaml_returns_empty(self) -> None:
        original = self.m.MANUAL_HOLDINGS_PATH
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
                f.write("us:\n  - this is a list, not a map\n")
                tmp = f.name
            self.m.MANUAL_HOLDINGS_PATH = Path(tmp)
            h = self.m._load_manual_holdings()
            self.assertEqual(h, {"US": {}, "CN": {}})         # safe degrade
        finally:
            self.m.MANUAL_HOLDINGS_PATH = original


if __name__ == "__main__":
    unittest.main()
