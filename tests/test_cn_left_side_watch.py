"""Tests for the CN left-side (oversold) watch section in the daily report."""
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
    sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("run_main_strategy_v2_backtest", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CnLeftSideWatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_empty_when_no_oversold_rows(self) -> None:
        payload = {
            "cn_opportunity_ranker": {
                "all_rows": [
                    {"symbol": "600584.SH", "strategy_family": "cn_tape_leadership_continuation"},
                    {"symbol": "002902.SZ", "alpha_sleeve_id": "cn_tape_leadership_continuation"},
                ]
            }
        }
        rows = self.module.cn_left_side_watch_rows(payload)
        self.assertEqual(rows, [])
        section = "\n".join(self.module.render_cn_left_side_watch_section(payload))
        self.assertIn("左侧观察池", section)
        self.assertIn("没有输出", section)

    def test_surfaces_oversold_contrarian_family(self) -> None:
        payload = {
            "cn_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "600519.SH",
                        "name": "贵州茅台",
                        "strategy_family": "oversold_contrarian",
                        "ev_lcb80_pct": 2.5,
                        "pct_chg": -0.5,
                        "ret_5d_pct": -8.0,
                        "ai_infra_current_pool": "雷达池",
                        "state": "active_watch",
                    },
                    {
                        "symbol": "002902.SZ",
                        "alpha_sleeve_id": "cn_tape_leadership_continuation",
                    },
                ]
            }
        }
        rows = self.module.cn_left_side_watch_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "600519.SH")
        section = "\n".join(self.module.render_cn_left_side_watch_section(payload))
        self.assertIn("600519.SH", section)
        self.assertIn("贵州茅台", section)
        self.assertIn("+2.50%", section)

    def test_sleeve_id_matches_cn_oversold_prefix(self) -> None:
        payload = {
            "cn_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "600000.SH",
                        "name": "浦发银行",
                        "alpha_sleeve_id": "cn_oversold_ev_positive",
                        "ev_lcb80_pct": 1.2,
                    },
                    {
                        "symbol": "601318.SH",
                        "name": "中国平安",
                        "alpha_sleeve_id": "cn_oversold_residual_z_action",
                        "ev_lcb80_pct": 0.8,
                    },
                ]
            }
        }
        rows = self.module.cn_left_side_watch_rows(payload)
        # Highest EV first
        self.assertEqual([r["symbol"] for r in rows], ["600000.SH", "601318.SH"])

    def test_dedupes_by_symbol(self) -> None:
        payload = {
            "cn_opportunity_ranker": {
                "all_rows": [
                    {"symbol": "600519.SH", "strategy_family": "oversold_contrarian", "ev_lcb80_pct": 1.0},
                    {"symbol": "600519.SH", "alpha_sleeve_id": "cn_oversold_ev_positive", "ev_lcb80_pct": 2.0},
                ]
            }
        }
        rows = self.module.cn_left_side_watch_rows(payload)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
