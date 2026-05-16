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

    def test_missing_signals_default_to_hedge(self) -> None:
        d = self.m.classify_regime([], {}, [])
        self.assertEqual(d.state, "hedge")
        self.assertEqual(d.r_multiplier, 1.0)


if __name__ == "__main__":
    unittest.main()
