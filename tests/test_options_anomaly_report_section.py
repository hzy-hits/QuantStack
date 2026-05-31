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
        self.assertIn("P/C raw", text)
        self.assertNotIn("| MU | 798.33 |\n| MU |", text)
        self.assertIn("Related AI-infra names to watch", text)
        self.assertIn("| MU | S 35,834 / P 3,579 | production 0.25R | HBM memory |", text)
        self.assertIn("| CSCO | S 12,516 / P 739 | source-review/候选池 | AI networking |", text)
        self.assertIn("0R context", text)

    def test_tenor_section_maps_weekly_signals_to_ai_infra_status(self) -> None:
        payload = {
            "options_tenor_signals": [
                {
                    "symbol": "CSCO",
                    "pattern": "gamma_trap",
                    "score": 256.4,
                    "guidance": "本周远 OTM call 成交远超月度；leaps context 只观察",
                    "evidence": {"tenors": ["weekly", "leaps"], "ratios": [3.0, 8.0]},
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

        self.assertIn("AI-infra 映射", text)
        self.assertIn("| CSCO | gamma_trap | 256.4 | source-review/候选池 |", text)
        self.assertIn("source-review queue: 0R", text)
        self.assertIn("LEAPS", text)
        self.assertIn("weekly 3.0x / LEAPS 8.0x", text)

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

    def test_us_failed_ev_gate_blocks_execution_r(self) -> None:
        payload = {
            "as_of": "2026-05-25",
            "strategy_alpha_bulletin": {
                "ev_status": {"us": "failed"},
                "selected_policies": {"us": None},
                "evaluated_through": {"us": "2026-05-22"},
            },
            "portfolio_risk_overlay": {
                "rows": [
                    {"market": "US", "symbol": "AMZN", "final_r": 0.5, "state": "Execution Alpha"}
                ],
                "summary": {},
            },
            "us_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "AMZN",
                        "name": "Amazon",
                        "production_action": "buy_stock_position",
                        "production_tier": "top_stock_trade",
                    }
                ]
            },
        }

        summary = self.module.build_production_decision_summary(payload)

        self.assertEqual(summary["actionable"], [])
        self.assertEqual(summary["summary"]["us_r"], 0)
        self.assertIn("US stable alpha gate not passed", summary["summary"]["top_blocker"])
        self.assertEqual(summary["watch"][0]["symbol"], "AMZN")
        gate_rows = [r for r in summary["no_trade"] if r["area"] == "US execution gate"]
        self.assertEqual(gate_rows[0]["status"], "0R")

    def test_cn_probability_pick_must_be_actionable(self) -> None:
        payload = {
            "production_decision_summary": {
                "actionable": [
                    {
                        "market": "CN",
                        "symbol": "000988.SZ",
                        "name": "华工科技",
                        "size_r": 0.0933,
                        "risk_plan": "handle 160; target 184",
                    }
                ]
            },
            "cn_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "300655.SZ",
                        "name": "晶瑞电材",
                        "rank_score": 92,
                        "informed_flow_score": 95,
                        "score_components": {"tushare_flow": 90, "narrative_fit": 100},
                    },
                    {
                        "symbol": "000988.SZ",
                        "name": "华工科技",
                        "rank_score": 72,
                        "informed_flow_score": 80,
                        "score_components": {"tushare_flow": 78, "narrative_fit": 100},
                    },
                ]
            },
            "cn_shadow_full": {},
        }

        text = "\n".join(self.module.render_cn_probability_picks_section(payload))

        self.assertIn("000988.SZ 华工科技", text)
        self.assertNotIn("300655.SZ 晶瑞电材", text)
        self.assertIn("0.0933R", text)

    def test_us_probability_section_uses_production_size_and_options_context_only(self) -> None:
        payload = {
            "as_of": "2026-05-25",
            "production_decision_summary": {
                "summary": {
                    "us_execution_gate": {"allowed": True, "reasons": []}
                },
                "actionable": [
                    {
                        "market": "US",
                        "symbol": "AXTI",
                        "size_r": 0.05,
                        "risk_plan": "stop 132; target 155",
                    }
                ],
            },
            "us_opportunity_ranker": {
                "all_rows": [
                    {
                        "symbol": "AXTI",
                        "rank": 7,
                        "rank_score": 70.4,
                        "ai_infra_evidence_state": "原文已证明",
                    }
                ]
            },
            "options_verdicts": {"AXTI": {"iv_rank_pct": 15, "pc_ratio_z": 0.2, "skew_z": 0.1}},
            "options_tenor_signals": [
                {
                    "symbol": "AXTI",
                    "pattern": "gamma_trap",
                    "score": 866,
                    "evidence": {"weekly_far_otm_call": 2000, "monthly_far_otm_call": 20},
                },
                {
                    "symbol": "AXTI",
                    "pattern": "insider_tilt_long_dated_calls",
                    "score": 80,
                    "evidence": {"tenors": ["leaps"], "ratios": [6]},
                },
            ],
        }

        actions = payload["production_decision_summary"]["actionable"]
        us_gate = payload["production_decision_summary"]["summary"]["us_execution_gate"]
        text = "\n".join(
            self.module.render_us_probability_picks_section(
                payload,
                actions=actions,
                us_gate=us_gate,
            )
        )

        self.assertIn("0.05R", text)
        self.assertIn("0R", text)
        self.assertNotIn("仓位:1R", text)
        self.assertNotIn("0DTE", text)
        self.assertIn("LEAPS/远月", text)
        self.assertNotIn("≤ 0.3R", text)
        self.assertNotIn("打法:", text)

    def test_us_overlay_caps_theme_momentum_and_sets_next_session_review(self) -> None:
        overlay = self.module.build_portfolio_risk_overlay(
            {
                "current": [
                    {
                        "state": "Execution Alpha",
                        "symbol": "RGTI",
                        "name": "RGTI",
                        "policy": "us_theme_cluster_momentum",
                    }
                ]
            },
            {"current": [], "metrics": {"v2": {}}},
            {},
            [
                {"market": "US", "profit_state": "stock_trade"},
                {"market": "CN", "profit_state": "no_current_setup"},
            ],
            Path("/tmp/nonexistent_us.duckdb"),
            Path("/tmp/nonexistent_cn.duckdb"),
            self.module.parse_date("2026-05-25"),
            risk_regime={"state": "hedge", "r_multiplier": 1.0},
            cn_risk_regime={"state": "hedge", "r_multiplier": 1.0},
        )

        row = overlay["rows"][0]
        self.assertEqual(row["final_r"], 0.125)
        self.assertEqual(row["time_exit"], "next session review; no mechanical 3D-5D hold")
        self.assertIn("theme_momentum_3d5d_decay_cap", row["risk_reasons"])

    def test_realized_horizon_edge_section_marks_bad_us_3d5d_review_only(self) -> None:
        payload = {
            "report_action_backtest_summary": {
                "by_mode_market": {
                    "contract_gated:US": {
                        "horizons": {
                            "1": {"n": 25, "weighted_avg": 0.0028, "median": -0.015, "win_rate": 0.36},
                            "3": {"n": 25, "weighted_avg": -0.019, "median": -0.016, "win_rate": 0.40},
                            "5": {"n": 21, "weighted_avg": -0.060, "median": -0.054, "win_rate": 0.24},
                        }
                    }
                }
            }
        }

        text = "\n".join(self.module.render_realized_horizon_edge_section(payload, "US"))

        self.assertIn("US Realized Horizon Edge", text)
        self.assertIn("3D", text)
        self.assertIn("review-only", text)
        self.assertIn("no mechanical 3D/5D hold", text)


if __name__ == "__main__":
    unittest.main()
