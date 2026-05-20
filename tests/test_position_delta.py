"""Tests for the held→target position delta hint + virtual holdings ledger.

Virtual holdings are self-maintained: today's actionable sizes become
tomorrow's "held" — the operator doesn't input positions by hand.
"""
from __future__ import annotations

import importlib.util
import json
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
        self.assertEqual(self.m.position_delta_text(0.0, 0.044), "")
        self.assertEqual(self.m.position_delta_text(None, 0.125), "")

    def test_trim_on_press_day(self) -> None:
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
        text = self.m.position_delta_text(0.125, 0.126)
        self.assertIn("持稳", text)

    def test_full_exit(self) -> None:
        text = self.m.position_delta_text(0.125, 0.0)
        self.assertIn("减", text)
        self.assertIn("-100%", text)


class VirtualHoldingsLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()
        self._orig_path = self.m.VIRTUAL_HOLDINGS_PATH
        self._tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        self._tmp.close()
        self.m.VIRTUAL_HOLDINGS_PATH = Path(self._tmp.name)
        Path(self._tmp.name).unlink()  # start absent

    def tearDown(self) -> None:
        self.m.VIRTUAL_HOLDINGS_PATH = self._orig_path
        if Path(self._tmp.name).exists():
            Path(self._tmp.name).unlink()

    def test_missing_file_returns_empty(self) -> None:
        self.assertEqual(self.m._load_virtual_holdings(), {"US": {}, "CN": {}})

    def test_persist_then_load_roundtrip(self) -> None:
        actionable = [
            {"market": "US", "symbol": "AMZN", "size_r": 0.125},
            {"market": "US", "symbol": "NVDA", "size_r": 0.044},
            {"market": "CN", "symbol": "600183.SH", "size_r": 0.05},
        ]
        self.m._persist_virtual_holdings(actionable, {"US": {}, "CN": {}}, "2026-05-20")
        loaded = self.m._load_virtual_holdings()
        self.assertEqual(loaded["US"]["AMZN"], 0.125)
        self.assertEqual(loaded["US"]["NVDA"], 0.044)
        self.assertEqual(loaded["CN"]["600183.SH"], 0.05)

    def test_orphan_positions_persist(self) -> None:
        # Yesterday held AMZN. Today's actionable doesn't include AMZN.
        # The held should remain in the book.
        prior = {"US": {"AMZN": 0.125}, "CN": {}}
        self.m._persist_virtual_holdings(
            [{"market": "US", "symbol": "NVDA", "size_r": 0.125}],
            prior, "2026-05-20",
        )
        loaded = self.m._load_virtual_holdings()
        self.assertEqual(loaded["US"]["AMZN"], 0.125)   # orphan kept
        self.assertEqual(loaded["US"]["NVDA"], 0.125)   # new added

    def test_historical_run_does_not_stomp_newer_state(self) -> None:
        # Persist 2026-05-20 first; then re-run for 2026-05-15 must NOT
        # overwrite the newer ledger.
        self.m._persist_virtual_holdings(
            [{"market": "US", "symbol": "AMZN", "size_r": 0.125}],
            {"US": {}, "CN": {}}, "2026-05-20",
        )
        self.m._persist_virtual_holdings(
            [{"market": "US", "symbol": "AMZN", "size_r": 0.044}],   # would overwrite
            {"US": {}, "CN": {}}, "2026-05-15",                       # but date older
        )
        loaded = self.m._load_virtual_holdings()
        self.assertEqual(loaded["US"]["AMZN"], 0.125)   # newer state preserved

    def test_malformed_file_returns_empty(self) -> None:
        Path(self._tmp.name).write_text("{ not valid json", encoding="utf-8")
        self.assertEqual(self.m._load_virtual_holdings(), {"US": {}, "CN": {}})


if __name__ == "__main__":
    unittest.main()
