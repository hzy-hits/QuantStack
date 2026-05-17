"""Tests for the capitulation radar — the 5-signal bottom detector."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_capitulation_radar.py"


def _load_module():
    if "score_capitulation_radar" in sys.modules:
        return sys.modules["score_capitulation_radar"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_capitulation_radar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CapitulationSignalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_vix_peak_rollover(self) -> None:
        # Touched 45, now back to 24 → fired.
        fired = self.m._sig_vix([20, 30, 45, 38, 30, 24])
        self.assertTrue(fired["fired"])
        # Never panicked → not fired.
        calm = self.m._sig_vix([18, 19, 22, 25, 20, 18])
        self.assertFalse(calm["fired"])
        # Panicked but still elevated → not fired.
        still_high = self.m._sig_vix([20, 45, 50, 44, 41, 38])
        self.assertFalse(still_high["fired"])

    def test_hy_oas_peak(self) -> None:
        # Spiked to 6.5, now 5.6 (off peak by 0.9) → fired.
        fired = self.m._sig_hy_oas([3.0, 4.5, 6.5, 6.2, 5.8, 5.6])
        self.assertTrue(fired["fired"])
        # Calm throughout → not fired.
        calm = self.m._sig_hy_oas([2.8, 2.9, 3.0, 2.9, 2.8, 2.76])
        self.assertFalse(calm["fired"])
        # Still at the peak → not fired.
        at_peak = self.m._sig_hy_oas([3.0, 4.0, 6.0, 6.3, 6.4, 6.5])
        self.assertFalse(at_peak["fired"])

    def test_put_call_extreme(self) -> None:
        self.assertTrue(self.m._sig_put_call(1.35)["fired"])
        self.assertFalse(self.m._sig_put_call(0.53)["fired"])
        self.assertFalse(self.m._sig_put_call(None)["fired"])

    def test_volume_exhaustion(self) -> None:
        # Peak early, then 3 contracting bars draining below 0.7×peak → fired.
        fired = self.m._sig_volume([100, 200, 1000, 800, 600, 400, 300, 200])
        self.assertTrue(fired["fired"])
        # Volume rising into the latest bar → not exhausted.
        rising = self.m._sig_volume([100, 200, 300, 400, 500, 600, 700, 900])
        self.assertFalse(rising["fired"])

    def test_high_beta_leadership(self) -> None:
        # Highest-beta names lead by a wide margin → dash-for-trash → fired.
        rows = [(0.5, 0.1), (0.6, 0.0), (0.7, 0.3), (0.8, 0.5),
                (1.8, 6.0), (1.9, 7.0), (2.0, 8.0), (2.2, 9.0)]
        self.assertTrue(self.m._sig_beta_leadership(rows)["fired"])
        # Low-beta defensives lead → not a capitulation rebound.
        rows2 = [(0.5, 8.0), (0.6, 7.0), (0.7, 6.0), (0.8, 5.0),
                 (1.8, 0.5), (1.9, 0.0), (2.0, -0.5), (2.2, -1.0)]
        self.assertFalse(self.m._sig_beta_leadership(rows2)["fired"])


class CapitulationAggregateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_three_signals_trigger_capitulation(self) -> None:
        result = self.m.evaluate_capitulation(
            vix_series=[20, 45, 50, 38, 28, 24],          # signal 1 ✓
            oas_series=[3.0, 6.5, 6.4, 6.0, 5.6, 5.5],    # signal 2 ✓
            pc_median=1.4,                                 # signal 3 ✓
            spy_vol=[100, 200, 300, 400, 500, 600, 700, 900],  # not exhausted
            beta_rows=[(0.5, 1.0)] * 4 + [(2.0, 1.5)] * 4,      # no lead
        )
        self.assertEqual(result["fired_count"], 3)
        self.assertTrue(result["capitulation"])

    def test_two_signals_do_not_trigger(self) -> None:
        result = self.m.evaluate_capitulation(
            vix_series=[20, 45, 50, 38, 28, 24],          # signal 1 ✓
            oas_series=[2.8, 2.9, 3.0, 2.9, 2.8, 2.76],   # calm
            pc_median=1.4,                                 # signal 3 ✓
            spy_vol=[100, 200, 300, 400, 500, 600, 700, 900],
            beta_rows=[(0.5, 1.0)] * 4 + [(2.0, 1.5)] * 4,
        )
        self.assertEqual(result["fired_count"], 2)
        self.assertFalse(result["capitulation"])

    def test_calm_market_is_zero(self) -> None:
        result = self.m.evaluate_capitulation(
            vix_series=[18, 19, 20, 22, 19, 18],
            oas_series=[2.8, 2.9, 3.0, 2.9, 2.8, 2.76],
            pc_median=0.53,
            spy_vol=[300, 320, 310, 305, 315, 320, 318, 322],
            beta_rows=[(0.5, 1.0)] * 4 + [(2.0, 1.2)] * 4,
        )
        self.assertEqual(result["fired_count"], 0)
        self.assertFalse(result["capitulation"])


if __name__ == "__main__":
    unittest.main()
