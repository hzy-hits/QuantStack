from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load():
    scripts_dir = str(REPO_ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)  # engine does `from lib.radar_io import ...`
    spec = importlib.util.spec_from_file_location(
        "regime_under_test", REPO_ROOT / "scripts" / "score_risk_regime_engine.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


eng = _load()


class ContinuousRegimeTest(unittest.TestCase):
    def test_smoothstep_bounds(self) -> None:
        self.assertEqual(eng._smoothstep(None, 0.35, 0.5), 0.0)
        self.assertEqual(eng._smoothstep(0.30, 0.35, 0.5), 0.0)
        self.assertEqual(eng._smoothstep(0.60, 0.35, 0.5), 1.0)
        self.assertAlmostEqual(eng._smoothstep(0.425, 0.35, 0.5), 0.5, places=2)

    def test_2026_06_10_cliff_day_gets_dampened(self) -> None:
        # 当天真实信号:corr 0.446 / MOVE 77 升 / TLT -0.51% / HYG -0.45%
        pressure = eng.wedge_pressure(corr=0.446, move_level=77.0, move_rising=True,
                                      tlt_ret_20d=-0.51, hyg_ret_20d=-0.45)
        self.assertGreater(pressure, 0.30)
        mult = eng.continuous_multiplier(state="hedge", base_multiplier=1.0, pressure=pressure)
        self.assertLess(mult, 0.90)
        self.assertGreaterEqual(mult, 0.60)

    def test_calm_day_stays_full_size(self) -> None:
        pressure = eng.wedge_pressure(corr=0.10, move_level=60.0, move_rising=False,
                                      tlt_ret_20d=1.2, hyg_ret_20d=0.5)
        self.assertLess(pressure, 0.05)
        self.assertAlmostEqual(
            eng.continuous_multiplier(state="hedge", base_multiplier=1.0, pressure=pressure),
            1.0, places=2)

    def test_non_hedge_states_unchanged(self) -> None:
        self.assertEqual(eng.continuous_multiplier(state="wedge", base_multiplier=0.60, pressure=0.9), 0.60)


if __name__ == "__main__":
    unittest.main()
