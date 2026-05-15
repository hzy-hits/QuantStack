"""Tests for suppressing unavailable CN signal families in daily reports."""
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
    spec = importlib.util.spec_from_file_location("run_main_strategy_v2_backtest", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CNUnavailableSignalsReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_limit_up_rows_do_not_become_watch_or_no_trade_blockers(self) -> None:
        payload = {
            "as_of": "2026-05-15",
            "portfolio_risk_overlay": {"rows": []},
            "profit_readiness": {"rows": []},
            "option_shadow_ledger": {},
            "limit_up": {
                "current": [
                    {
                        "symbol": "002918.SZ",
                        "name": "蒙娜丽莎",
                    }
                ]
            },
            "cn_opportunity_ranker": {"all_rows": []},
            "us_opportunity_ranker": {"all_rows": []},
            "source_review_calendar": {"cn": {"rows": []}},
        }

        payload["pipeline_requirements_audit"] = self.module.build_pipeline_requirements_audit(payload)
        summary = self.module.build_production_decision_summary(payload)

        self.assertNotIn("limit_up_radar", {row.get("state") for row in summary["watch"]})
        self.assertNotIn("Limit-up", {row.get("area") for row in summary["no_trade"]})
        self.assertEqual(
            payload["pipeline_requirements_audit"]["summary"]["top_blocker"],
            "CN AI-infra production sleeve",
        )

    def test_cn_standalone_report_does_not_show_left_side_section(self) -> None:
        payload = {
            "as_of": "2026-05-15",
            "cn": {"current": [], "sector_narrative_screen": []},
            "limit_up": {"current": [{"symbol": "002918.SZ", "name": "蒙娜丽莎"}]},
            "production_decision_summary": {
                "summary": {"cn_r": 0.0, "beta_hedge_r": 0.0, "net_beta_r": 0.0},
                "actionable": [],
                "watch": [],
            },
            "ai_supercycle_evidence_ledger": {"rows": []},
            "ai_supercycle_value_radar": {"cn": {"rows": []}},
            "earnings_calendar": {"cn": {"rows": [], "status": "ok"}},
            "source_review_calendar": {"cn": {"rows": [], "status": "ok"}},
            "benchmark_attribution": {"cn": {}, "ai_book": {"cn": {"rows": [], "basket_size": 0}}},
        }

        text = self.module.render_cn_standalone_report(payload)

        self.assertNotIn("A股左侧观察池", text)
        self.assertNotIn("limit_up_radar", text)
        self.assertNotIn("9:25", text)
        self.assertNotIn("9:35", text)
        self.assertIn("非 AI-infra broad-market 信号不生成 R，也不能阻拦 AI-infra sleeve", text)


if __name__ == "__main__":
    unittest.main()
