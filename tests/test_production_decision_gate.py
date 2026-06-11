from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load():
    spec = importlib.util.spec_from_file_location("gen_msv2_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gen = _load()


def _payload(plan: dict) -> dict:
    return {
        "portfolio_risk_overlay": {
            "rows": [{
                "market": "US", "symbol": "ABB", "name": "ABB Ltd",
                "final_r": 0.0441, "state": "actionable",
            }]
        },
        "us_trade_plan": {"ABB": plan},
        "profit_guardrails": [],
        "gamma_spring": {"rows": []},
    }


class MissingPriceGateTest(unittest.TestCase):
    def _run(self, plan: dict) -> dict:
        with mock.patch.object(gen, "evaluate_us_execution_gate",
                               return_value={"allowed": True, "top_blocker": "", "top_warning": ""}), \
             mock.patch.object(gen, "_load_virtual_holdings", return_value={}):
            return gen.build_production_decision_summary(_payload(plan))

    def test_missing_price_plan_is_blocked_not_actionable(self) -> None:
        decision = self._run({
            "status": "missing_price", "entry": None, "stop": None, "target": None,
            "latest_date": None,
            "rule": "missing US prices_daily close; no mechanical stock plan",
        })
        actionable_syms = [r.get("symbol") for r in decision.get("actionable") or []]
        watch = decision.get("watch") or []
        self.assertNotIn("ABB", actionable_syms)
        self.assertTrue(any(r.get("symbol") == "ABB" and r.get("state") == "execution_blocked_0r"
                            for r in watch))

    def test_ok_plan_stays_actionable(self) -> None:
        decision = self._run({
            "status": "ok", "entry": 52.1, "stop": 48.97, "target": 57.31,
            "latest_date": "2026-06-09", "rule": "entry=close; stop=-6%; target=+10%",
        })
        actionable_syms = [r.get("symbol") for r in decision.get("actionable") or []]
        self.assertIn("ABB", actionable_syms)


if __name__ == "__main__":
    unittest.main()
