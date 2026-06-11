from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "backtest_promotion_history.py"


def _load():
    spec = importlib.util.spec_from_file_location("promo_bt_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bt = _load()


class PromotionAlphaAlertTest(unittest.TestCase):
    def test_alert_fires_on_negative_ir_with_enough_n(self) -> None:
        trailing_5d = [("2026-W22", {"n": 65, "mean_active_pct": -0.68, "hit_rate_pct": 41.5, "ir": -0.13}),
                       ("2026-W23", {"n": 75, "mean_active_pct": -1.15, "hit_rate_pct": 38.7, "ir": -0.21})]
        alert = bt.promotion_alpha_alert(trailing_5d)
        self.assertIsNotNone(alert)
        self.assertEqual(alert["level"], "warning")
        self.assertEqual(alert["week"], "2026-W23")

    def test_no_alert_on_small_sample_or_positive_ir(self) -> None:
        self.assertIsNone(bt.promotion_alpha_alert([("2026-W23", {"n": 12, "ir": -0.5,
                                                                  "mean_active_pct": -2.0, "hit_rate_pct": 30.0})]))
        self.assertIsNone(bt.promotion_alpha_alert([("2026-W23", {"n": 80, "ir": 0.05,
                                                                  "mean_active_pct": 0.2, "hit_rate_pct": 51.0})]))


if __name__ == "__main__":
    unittest.main()
