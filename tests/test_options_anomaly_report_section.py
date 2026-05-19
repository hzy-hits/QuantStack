"""Tests for the options anomaly section in the main daily report."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load_module():
    if "generate_main_strategy_v2_report" in sys.modules:
        return sys.modules["generate_main_strategy_v2_report"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("generate_main_strategy_v2_report", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class OptionsAnomalyReportSectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_section_has_clean_rows_and_ai_infra_context(self) -> None:
        payload = {
            "options_anomaly_rows": [
                {
                    "symbol": "MU",
                    "spot_close": "798.33",
                    "far_otm_call_volume": "24940",
                    "far_otm_put_volume": "4302",
                    "far_otm_call_vol_oi_ratio": "0.715",
                    "far_otm_put_vol_oi_ratio": "0.240",
                    "pc_ratio_z": "-0.70",
                    "skew_z": "-1.10",
                    "short_squeeze_score": "35833.9",
                    "selling_pressure_score": "3578.66",
                },
                {
                    "symbol": "CSCO",
                    "spot_close": "58.12",
                    "far_otm_call_volume": "9531",
                    "far_otm_put_volume": "520",
                    "far_otm_call_vol_oi_ratio": "1.31",
                    "far_otm_put_vol_oi_ratio": "0.06",
                    "pc_ratio_z": "-1.20",
                    "skew_z": "-0.40",
                    "short_squeeze_score": "12516.46",
                    "selling_pressure_score": "739.0",
                },
            ],
            "production_decision_summary": {
                "actionable": [
                    {
                        "market": "US",
                        "symbol": "MU",
                        "size_r": 0.25,
                        "action": "buy_stock_with_options_confirmation",
                        "evidence_state": "原文已证明",
                    }
                ],
                "watch": [],
            },
            "us_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "MU",
                        "ai_infra_module": "HBM memory",
                        "ai_infra_evidence_state": "原文已证明",
                        "production_action": "buy_stock_with_options_confirmation",
                        "production_tier": "top_stock_trade",
                    }
                ]
            },
            "source_review_calendar": {
                "us": {
                    "rows": [
                        {
                            "ticker": "CSCO",
                            "primary_ticker": "CSCO",
                            "module": "AI networking",
                            "current_pool": "候选池",
                            "readiness_tier": "pending_human_review",
                        }
                    ]
                }
            },
        }

        text = "\n".join(self.module.render_options_anomaly_section(payload, top_n=2))

        self.assertIn("| MU | 798.33 | 24,940 | 0.71 | 4,302 |", text)
        self.assertNotIn("| MU | 798.33 |\n| MU |", text)
        self.assertIn("Related AI-infra names to watch", text)
        self.assertIn("| MU | S 35,834 / P 3,579 | production 0.25R | HBM memory |", text)
        self.assertIn("| CSCO | S 12,516 / P 739 | source-review/候选池 | AI networking |", text)
        self.assertIn("期权异动不能单独生成 R", text)

    def test_tenor_section_maps_weekly_signals_to_ai_infra_status(self) -> None:
        payload = {
            "options_tenor_signals": [
                {
                    "symbol": "CSCO",
                    "pattern": "gamma_trap",
                    "score": 256.4,
                    "guidance": "本周远 OTM call 成交远超月度",
                }
            ],
            "source_review_calendar": {
                "us": {
                    "rows": [
                        {
                            "ticker": "CSCO",
                            "primary_ticker": "CSCO",
                            "module": "AI networking",
                            "current_pool": "候选池",
                            "readiness_tier": "pending_human_review",
                        }
                    ]
                }
            },
        }

        text = "\n".join(self.module.render_options_tenor_section(payload, top_n=3))

        self.assertIn("临期 / weekly 异动映射", text)
        self.assertIn("| CSCO | gamma_trap | 256.4 | source-review/候选池 |", text)
        self.assertIn("source review first", text)

    def test_production_summary_uses_us_trade_plan_fallback(self) -> None:
        payload = {
            "portfolio_risk_overlay": {
                "rows": [
                    {
                        "market": "US",
                        "symbol": "AMZN",
                        "final_r": 0.5,
                        "state": "Execution Alpha",
                    }
                ]
            },
            "us_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "AMZN",
                        "production_action": "buy_stock_position",
                        "production_tier": "top_stock_trade",
                    }
                ]
            },
            "us_trade_plan": {
                "AMZN": {
                    "status": "ok",
                    "entry": 100.0,
                    "stop": 94.0,
                    "target": 110.0,
                    "latest_date": "2026-05-14",
                    "rule": "entry=latest close; stop=-6%; target=+10%",
                }
            },
        }

        summary = self.module.build_production_decision_summary(payload)
        action = summary["actionable"][0]

        self.assertEqual(action["entry"], 100.0)
        self.assertIn("stop 94.00", action["risk_plan"])
        self.assertIn("target 110.00", action["risk_plan"])


if __name__ == "__main__":
    unittest.main()
