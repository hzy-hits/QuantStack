"""Tests for the Hedge/Wedge/Confirm/Press risk-regime engine."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_risk_regime_engine.py"


def _load_module():
    if "score_risk_regime_engine" in sys.modules:
        return sys.modules["score_risk_regime_engine"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_risk_regime_engine", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _wedge(tlt: float | None = None, hyg: float | None = None) -> list[dict]:
    rows = []
    if tlt is not None:
        rows.append({"symbol": "TLT", "ret_20d_pct": tlt})
    if hyg is not None:
        rows.append({"symbol": "HYG", "ret_20d_pct": hyg})
    return rows


def _confirm(**kw) -> dict:
    base = {
        "ai_book_vs_tlt_corr_20d": 0.4,
        "fear_greed_score": 50.0,
        "smh_above_ema20": True,
        "smh_above_ema50": True,
        "trendline_break": False,
        # Calm cross-asset volatility baseline (no MOVE-driven wedge).
        "move_level": 70.0,
        "move_chg_20d": -3.0,
        "vix_level": 18.0,
        "move_vix_ratio": 3.9,
    }
    base.update(kw)
    return base


class RegimeClassifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_calm_tape_is_hedge_full_size(self) -> None:
        d = self.m.classify_regime(_wedge(tlt=0.5, hyg=0.3), _confirm(), [])
        self.assertEqual(d.state, "hedge")
        self.assertEqual(d.r_multiplier, 1.0)
        self.assertTrue(d.new_adds_allowed)

    def test_rates_drawdown_triggers_wedge(self) -> None:
        d = self.m.classify_regime(_wedge(tlt=-3.2, hyg=0.0), _confirm(), [])
        self.assertEqual(d.state, "wedge")
        self.assertEqual(d.r_multiplier, 0.6)
        self.assertTrue(d.new_adds_allowed)

    def test_negative_corr_alone_triggers_wedge(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=0.1), _confirm(ai_book_vs_tlt_corr_20d=-0.62), []
        )
        self.assertEqual(d.state, "wedge")

    def test_credit_stress_alone_triggers_wedge(self) -> None:
        d = self.m.classify_regime(_wedge(tlt=0.0, hyg=-1.5), _confirm(), [])
        self.assertEqual(d.state, "wedge")

    def test_lost_ema20_but_holds_ema50_is_confirm(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=0.0),
            _confirm(smh_above_ema20=False, smh_above_ema50=True),
            [],
        )
        self.assertEqual(d.state, "confirm")
        self.assertEqual(d.r_multiplier, 0.4)
        self.assertTrue(d.new_adds_allowed)

    def test_extreme_greed_plus_wedge_is_confirm(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=-3.0),
            _confirm(fear_greed_score=82.0),
            [],
        )
        self.assertEqual(d.state, "confirm")

    def test_extreme_greed_without_wedge_stays_hedge(self) -> None:
        # Greed alone is not a reason to cut size — don't fight the tape.
        d = self.m.classify_regime(
            _wedge(tlt=0.5), _confirm(fear_greed_score=85.0), []
        )
        self.assertEqual(d.state, "hedge")

    def test_lost_ema50_is_press_and_freezes_adds(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=0.0),
            _confirm(smh_above_ema20=False, smh_above_ema50=False),
            [{"symbol": "AAOI"}],
        )
        self.assertEqual(d.state, "press")
        self.assertEqual(d.r_multiplier, 0.0)
        self.assertFalse(d.new_adds_allowed)
        self.assertIn("AAOI", d.victim_action)

    def test_trendline_break_is_press_even_above_ema50(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=0.0),
            _confirm(smh_above_ema50=True, trendline_break=True),
            [],
        )
        self.assertEqual(d.state, "press")

    def test_press_beats_wedge_when_both_trigger(self) -> None:
        # Severity order: PRESS > CONFIRM > WEDGE > HEDGE.
        d = self.m.classify_regime(
            _wedge(tlt=-5.0),  # would be wedge
            _confirm(smh_above_ema50=False),  # but tape broken
            [],
        )
        self.assertEqual(d.state, "press")

    def test_move_above_80_and_rising_triggers_wedge(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=0.5),  # TLT calm
            _confirm(move_level=92.0, move_chg_20d=14.0),
            [],
        )
        self.assertEqual(d.state, "wedge")
        self.assertTrue(d.signals["move_wedge"])
        self.assertIn("MOVE", d.rationale)

    def test_move_above_80_but_falling_does_not_trigger_wedge(self) -> None:
        # MOVE elevated but declining → wedge easing, not biting.
        d = self.m.classify_regime(
            _wedge(tlt=0.5),
            _confirm(move_level=92.0, move_chg_20d=-6.0),
            [],
        )
        self.assertEqual(d.state, "hedge")
        self.assertFalse(d.signals["move_wedge"])

    def test_move_below_80_is_bond_calm(self) -> None:
        d = self.m.classify_regime(
            _wedge(tlt=0.5),
            _confirm(move_level=79.0, move_chg_20d=5.0),
            [],
        )
        self.assertEqual(d.state, "hedge")
        self.assertFalse(d.signals["move_wedge"])

    def test_move_vix_ratio_above_6_triggers_wedge(self) -> None:
        # Rates vol dominates the cross-asset stress picture.
        d = self.m.classify_regime(
            _wedge(tlt=0.5),
            _confirm(move_level=78.0, move_chg_20d=-1.0, move_vix_ratio=6.4),
            [],
        )
        self.assertEqual(d.state, "wedge")
        self.assertTrue(d.signals["move_wedge"])
        self.assertIn("MOVE/VIX", d.rationale)

    def test_missing_move_data_does_not_crash_or_trigger(self) -> None:
        conf = _confirm()
        for k in ("move_level", "move_chg_20d", "vix_level", "move_vix_ratio"):
            conf.pop(k, None)
        d = self.m.classify_regime(_wedge(tlt=0.5), conf, [])
        self.assertEqual(d.state, "hedge")
        self.assertFalse(d.signals["move_wedge"])

    def test_capitulation_triggers_fifth_state(self) -> None:
        cap = {"capitulation": True, "fired_count": 4,
               "fired_signals": ["vix_peak_rollover", "hy_oas_peak",
                                 "put_call_extreme", "volume_exhaustion"]}
        d = self.m.classify_regime(
            _wedge(tlt=-5.0),  # wedge biting
            _confirm(smh_above_ema50=False),  # tape broken
            [],
            capitulation=cap,
        )
        # CAPITULATION overrides PRESS — selling exhausted, flip to convex long.
        self.assertEqual(d.state, "capitulation")
        self.assertEqual(d.r_multiplier, 1.0)
        self.assertTrue(d.new_adds_allowed)
        self.assertIn("LEAPS call", d.victim_action)

    def test_capitulation_below_threshold_does_not_trigger(self) -> None:
        cap = {"capitulation": False, "fired_count": 2, "fired_signals": []}
        d = self.m.classify_regime(
            _wedge(tlt=0.5), _confirm(smh_above_ema50=False), [], capitulation=cap,
        )
        self.assertEqual(d.state, "press")  # falls through to normal precedence

    def test_no_capitulation_arg_is_backward_compatible(self) -> None:
        d = self.m.classify_regime(_wedge(tlt=0.5), _confirm(), [])
        self.assertEqual(d.state, "hedge")

    def test_missing_signals_default_to_hedge(self) -> None:
        d = self.m.classify_regime([], {}, [])
        self.assertEqual(d.state, "hedge")
        self.assertEqual(d.r_multiplier, 1.0)
        self.assertFalse(d.signals["move_wedge"])


if __name__ == "__main__":
    unittest.main()
