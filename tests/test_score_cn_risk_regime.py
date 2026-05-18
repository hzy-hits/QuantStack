"""Tests for the CN-native risk regime classifier."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_cn_risk_regime.py"


def _load_module():
    if "score_cn_risk_regime" in sys.modules:
        return sys.modules["score_cn_risk_regime"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_cn_risk_regime", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _sig(**kw) -> dict:
    base = {
        "gem_above_ema20": True, "gem_above_ema50": True,
        "hs300_above_ema50": True,
        "north_20d_sum": 5_000_000.0,   # net inflow
        "margin_chg_20d_pct": 6.0,      # leverage building
        "us_move_level": 70.0, "us_move_chg_20d": -3.0,
    }
    base.update(kw)
    return base


class CnRegimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_healthy_tape_and_flow_is_hedge(self) -> None:
        d = self.m.classify_cn_regime(_sig())
        self.assertEqual(d.state, "hedge")
        self.assertEqual(d.r_multiplier, 1.0)

    def test_gem_below_ema50_is_press(self) -> None:
        d = self.m.classify_cn_regime(_sig(gem_above_ema50=False, gem_above_ema20=False))
        self.assertEqual(d.state, "press")
        self.assertEqual(d.r_multiplier, 0.0)
        self.assertFalse(d.new_adds_allowed)

    def test_hs300_below_ema50_is_press(self) -> None:
        d = self.m.classify_cn_regime(_sig(hs300_above_ema50=False))
        self.assertEqual(d.state, "press")

    def test_gem_lost_ema20_holds_ema50_is_confirm(self) -> None:
        d = self.m.classify_cn_regime(_sig(gem_above_ema20=False, gem_above_ema50=True))
        self.assertEqual(d.state, "confirm")
        self.assertEqual(d.r_multiplier, 0.4)

    def test_northbound_outflow_is_wedge(self) -> None:
        d = self.m.classify_cn_regime(_sig(north_20d_sum=-2_000_000.0))
        self.assertEqual(d.state, "wedge")
        self.assertEqual(d.r_multiplier, 0.6)

    def test_us_move_wedge_transmits_to_cn(self) -> None:
        # US MOVE >= 80 and rising → rates wedge transmits via northbound.
        d = self.m.classify_cn_regime(_sig(us_move_level=88.0, us_move_chg_20d=12.0))
        self.assertEqual(d.state, "wedge")

    def test_outflow_plus_margin_derisk_is_confirm(self) -> None:
        d = self.m.classify_cn_regime(_sig(north_20d_sum=-2_000_000.0, margin_chg_20d_pct=-5.0))
        self.assertEqual(d.state, "confirm")

    def test_press_beats_wedge_when_both_trigger(self) -> None:
        d = self.m.classify_cn_regime(_sig(gem_above_ema50=False, north_20d_sum=-9e6))
        self.assertEqual(d.state, "press")

    def test_missing_signals_default_to_hedge(self) -> None:
        d = self.m.classify_cn_regime({})
        self.assertEqual(d.state, "hedge")
        self.assertEqual(d.r_multiplier, 1.0)

    # ── PRESS hysteresis (EMA50-break confirmation lag) ──────────────────

    def test_one_day_below_ema50_is_confirm_not_press(self) -> None:
        d = self.m.classify_cn_regime(_sig(
            gem_above_ema50=False, gem_above_ema20=False,
            gem_ema50_streak=-1, hs300_ema50_streak=8,
        ))
        self.assertEqual(d.state, "confirm")
        self.assertEqual(d.r_multiplier, 0.4)
        self.assertTrue(d.signals["tape_breaking"])

    def test_three_days_below_ema50_confirms_press(self) -> None:
        d = self.m.classify_cn_regime(_sig(
            gem_above_ema50=False, gem_ema50_streak=-3, hs300_ema50_streak=8,
        ))
        self.assertEqual(d.state, "press")
        self.assertEqual(d.r_multiplier, 0.0)

    def test_hs300_three_day_break_confirms_press(self) -> None:
        d = self.m.classify_cn_regime(_sig(
            hs300_above_ema50=False, gem_ema50_streak=8, hs300_ema50_streak=-4,
        ))
        self.assertEqual(d.state, "press")

    def test_long_streak_above_ema50_is_hedge(self) -> None:
        d = self.m.classify_cn_regime(_sig(
            gem_ema50_streak=15, hs300_ema50_streak=20,
        ))
        self.assertEqual(d.state, "hedge")


if __name__ == "__main__":
    unittest.main()
