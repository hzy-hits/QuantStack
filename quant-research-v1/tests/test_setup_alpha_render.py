import unittest

from quant_bot.reporting.render import (
    render_action_plan_summary,
    render_my_book_overlay,
    render_setup_alpha_summary,
    render_strategy_ev_guidance,
)


def _item(symbol: str, **overrides):
    base = {
        "symbol": symbol,
        "price": 100.0,
        "ret_1d_pct": 1.0,
        "ret_5d_pct": 3.0,
        "ret_21d_pct": 6.0,
        "score": 0.5,
        "report_bucket": "CORE BOOK",
        "signal": {"direction": "long", "confidence": "HIGH"},
        "main_signal_gate": {"status": "pass", "role": "main_signal", "blockers": []},
        "sub_scores": {"event": 0.20, "lab_factor": 0.10, "options": 0.20},
        "execution_gate": {
            "action": "still_actionable",
            "support_score": 0.55,
            "pullback_price": 97.0,
            "effective_stretch_score": 0.20,
        },
        "options": {"expected_move_pct": 5.0, "cone_position_68": 0.55},
        "risk_params": {"entry": 100.0, "stop": 94.0, "target": 112.0, "rr_ratio": 2.0, "expected_move_pct": 12.0},
        "fundamentals": {"company_name": f"{symbol} Corp"},
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged = dict(base[key])
            merged.update(value)
            base[key] = merged
        else:
            base[key] = value
    return base


class SetupAlphaRenderTests(unittest.TestCase):
    def test_my_book_overlay_is_rendered_when_available(self):
        from pathlib import Path
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "reports" / "2026-04-24_payload_post.md"
            overlay_path = (
                output_path.parent
                / "review_dashboard"
                / "my_book_overlay"
                / "2026-04-24"
                / "my_book_overlay_us.md"
            )
            overlay_path.parent.mkdir(parents=True)
            overlay_path.write_text("## My Book Overlay\n\n- No ticket, no trade.", encoding="utf-8")

            rendered = "\n".join(render_my_book_overlay({"meta": {"trade_date": "2026-04-24"}}, output_path))

        self.assertIn("## My Book Overlay", rendered)
        self.assertIn("No ticket, no trade", rendered)

    def test_strategy_ev_guidance_separates_signal_strength_from_ev(self):
        text = "\n".join(
            render_strategy_ev_guidance(
                {
                    "_alpha_bulletin": {
                        "ev_status": {"us": "failed"},
                        "selected_policies": {"us": None},
                        "research_policies": {"us": "us:core:long:low:executable_now:trending:h3"},
                        "stability": {
                            "us": [
                                {
                                    "policy_id": "us:core:long:low:executable_now:trending:h3",
                                    "fills": 46,
                                    "avg_trade_pct": 2.56,
                                    "ev_lower_confidence_pct": 0.81,
                                    "strict_win_rate": 0.652,
                                    "max_drawdown_pct": -6.89,
                                    "top1_winner_contribution": 0.113,
                                    "stability_score": 2.09,
                                    "eligible": False,
                                    "fail_reasons": ["policy_confidence_not_high_mod"],
                                },
                                {
                                    "policy_id": "us:core:long:high_mod:executable_now:noisy:h3",
                                    "fills": 84,
                                    "avg_trade_pct": -0.40,
                                    "ev_lower_confidence_pct": -2.05,
                                    "strict_win_rate": 0.524,
                                    "max_drawdown_pct": -12.12,
                                    "top1_winner_contribution": 0.116,
                                    "stability_score": 0.0,
                                    "eligible": False,
                                    "fail_reasons": ["avg_trade_pct<=0.4"],
                                },
                            ]
                        },
                    }
                }
            )
        )

        self.assertIn("## Strategy EV Guidance", text)
        self.assertIn("Positive EV Setup / Recall only", text)
        self.assertIn("Do not promote; historical EV is weak", text)
        self.assertIn("+2.6%", text)
        self.assertIn("-0.4%", text)

    def test_stable_gate_failed_blocks_gate_pass_plan(self):
        text = "\n".join(
            render_action_plan_summary(
                {
                    "notable_items": [
                        _item(
                            "PWR",
                            stable_alpha_context={
                                "status": "blocked_alpha",
                                "ev_status": "failed",
                                "reason": "stable EV gate not passed",
                                "policy_metrics": {
                                    "fills": 84,
                                    "avg_trade_pct": -0.40,
                                    "ev_lower_confidence_pct": -2.05,
                                },
                            },
                        )
                    ]
                }
            )
        )

        self.assertIn("## Action Plan Ledger", text)
        self.assertIn("| Gate-pass plan | 0 |", text)
        self.assertIn("PWR / PWR Corp", text)
        self.assertIn("stable EV gate not passed", text)

    def test_positive_ev_recall_is_setup_not_execution(self):
        text = "\n".join(
            render_action_plan_summary(
                {
                    "notable_items": [
                        _item(
                            "LOWEV",
                            signal={"confidence": "LOW"},
                            main_signal_gate={
                                "status": "blocked",
                                "role": "notability_only",
                                "blockers": ["confidence_low"],
                            },
                            ret_1d_pct=1.0,
                            ret_5d_pct=3.0,
                            ret_21d_pct=6.0,
                            stable_alpha_context={
                                "status": "positive_ev_recall",
                                "ev_status": "failed",
                                "reason": "positive-EV research policy",
                                "policy_metrics": {
                                    "fills": 46,
                                    "avg_trade_pct": 2.56,
                                    "ev_lower_confidence_pct": 0.81,
                                },
                            },
                        )
                    ]
                }
            )
        )

        self.assertIn("| Gate-pass plan | 0 |", text)
        self.assertIn("| Setup / wait plan | 1 |", text)
        self.assertIn("LOWEV / LOWEV Corp", text)
        self.assertIn("positive-EV recall policy", text)

    def test_action_plan_summary_keeps_price_plan_and_company_name(self):
        text = "\n".join(
            render_action_plan_summary(
                {
                    "notable_items": [
                        _item("PLAN"),
                        _item(
                            "BLOCK",
                            main_signal_gate={
                                "status": "blocked",
                                "role": "directional_observation",
                                "blockers": ["rr_below_1_5"],
                            },
                            risk_params={"entry": 100.0, "stop": 94.0, "target": 104.0, "rr_ratio": 0.67, "expected_move_pct": 4.0},
                        ),
                    ]
                }
            )
        )

        self.assertIn("## Action Plan Ledger", text)
        self.assertIn("PLAN / PLAN Corp", text)
        self.assertIn("$100.00", text)
        self.assertIn("$94.00", text)
        self.assertIn("$112.00", text)
        self.assertIn("BLOCK / BLOCK Corp", text)
        self.assertIn("R:R below execution floor", text)

    def test_render_separates_early_pullback_breakout_and_chase(self):
        text = "\n".join(
            render_setup_alpha_summary(
                {
                    "notable_items": [
                        _item("EARLY"),
                        _item(
                            "PULL",
                            execution_gate={
                                "action": "wait_pullback",
                                "support_score": 0.60,
                                "pullback_price": 91.0,
                            },
                        ),
                        _item(
                            "BREAK",
                            ret_1d_pct=4.0,
                            ret_5d_pct=10.0,
                            ret_21d_pct=22.0,
                            execution_gate={
                                "action": "still_actionable",
                                "support_score": 0.70,
                                "effective_stretch_score": 0.45,
                            },
                            sub_scores={"event": 0.75, "lab_factor": 0.20, "options": 0.65},
                        ),
                        _item(
                            "HOT",
                            ret_1d_pct=12.0,
                            ret_5d_pct=21.0,
                            ret_21d_pct=42.0,
                            execution_gate={
                                "action": "do_not_chase",
                                "support_score": 0.30,
                                "effective_stretch_score": 0.90,
                            },
                        ),
                    ]
                }
            )
        )

        self.assertIn("## Setup Alpha / Anti-Chase", text)
        self.assertIn("### Early Accumulation", text)
        self.assertIn("EARLY", text)
        self.assertIn("### Pullback / Reset", text)
        self.assertIn("PULL", text)
        self.assertIn("### Breakout Acceptance", text)
        self.assertIn("BREAK", text)
        self.assertIn("### Blocked Chase / Priced-In", text)
        self.assertIn("HOT", text)

    def test_confirmed_breakout_is_not_blocked_by_extension_alone(self):
        text = "\n".join(
            render_setup_alpha_summary(
                {
                    "notable_items": [
                        _item(
                            "BREAK",
                            ret_1d_pct=4.0,
                            ret_5d_pct=10.0,
                            ret_21d_pct=22.0,
                            execution_gate={
                                "action": "still_actionable",
                                "support_score": 0.70,
                                "effective_stretch_score": 0.45,
                            },
                            sub_scores={"event": 0.75, "lab_factor": 0.20, "options": 0.65},
                        )
                    ]
                }
            )
        )

        breakout_section = text.split("### Breakout Acceptance", 1)[1].split(
            "### Post-Event Second Day", 1
        )[0]
        blocked_section = text.split("### Blocked Chase / Priced-In", 1)[1]

        self.assertIn("BREAK", breakout_section)
        self.assertNotIn("BREAK", blocked_section)


if __name__ == "__main__":
    unittest.main()
