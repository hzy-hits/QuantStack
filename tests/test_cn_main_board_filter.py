"""Tests for the CN main-board (主板) filter on the daily actionable top-5."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_main_strategy_v2_backtest.py"


def _load_module():
    if "run_main_strategy_v2_backtest" in sys.modules:
        return sys.modules["run_main_strategy_v2_backtest"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "run_main_strategy_v2_backtest", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MainBoardFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_shanghai_main_board(self) -> None:
        for s in ("600183.SH", "601138.SH", "603078.SH", "605117.SH"):
            self.assertTrue(self.m._is_cn_main_board(s), s)

    def test_shenzhen_main_board(self) -> None:
        for s in ("000021.SZ", "001309.SZ", "002518.SZ", "003816.SZ"):
            self.assertTrue(self.m._is_cn_main_board(s), s)

    def test_star_board_excluded(self) -> None:
        for s in ("688082.SH", "688627.SH", "689009.SH"):
            self.assertFalse(self.m._is_cn_main_board(s), s)

    def test_chinext_excluded(self) -> None:
        for s in ("300975.SZ", "301308.SZ", "301611.SZ"):
            self.assertFalse(self.m._is_cn_main_board(s), s)

    def test_bse_and_blank_excluded(self) -> None:
        self.assertFalse(self.m._is_cn_main_board("830799.BJ"))
        self.assertFalse(self.m._is_cn_main_board(""))
        self.assertFalse(self.m._is_cn_main_board(None))

    def test_works_without_exchange_suffix(self) -> None:
        self.assertTrue(self.m._is_cn_main_board("600183"))
        self.assertFalse(self.m._is_cn_main_board("688082"))


if __name__ == "__main__":
    unittest.main()
