import unittest

from quant_bot.reporting.render import render_setup_alpha_summary


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
        "sub_scores": {"event": 0.20, "lab_factor": 0.10, "options": 0.20},
        "execution_gate": {
            "action": "still_actionable",
            "support_score": 0.55,
            "pullback_price": 97.0,
            "effective_stretch_score": 0.20,
        },
        "options": {"expected_move_pct": 5.0, "cone_position_68": 0.55},
        "risk_params": {},
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
