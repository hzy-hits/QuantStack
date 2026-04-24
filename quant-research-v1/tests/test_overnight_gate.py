from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class OvernightGateFormulaTests(unittest.TestCase):
    def test_trend_aligned_small_gap_gets_more_support(self) -> None:
        from quant_bot.analytics.overnight_gate import (
            _compute_continuation_probability,
            _compute_support_score,
            _discipline_support_score,
            _support_regime_bonus,
            _trend_alignment_score,
        )

        trend_alignment = _trend_alignment_score(
            gap_dir=1,
            trend_prob=0.5496,
            trend_regime="trending",
        )
        discipline_support = _discipline_support_score(
            gap_dir=1,
            gap_vs_expected_move=0.014,
            cone_position_68=0.353,
        )
        regime_bonus = _support_regime_bonus(
            gap_dir=1,
            trend_regime="trending",
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            flow_intensity=0.0,
            bias_support=1.0,
        )
        support_score = _compute_support_score(
            flow_intensity=0.0,
            iv_delta=0.312,
            skew_delta=0.243,
            pc_delta=0.553,
            bias_support=1.0,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            sentiment_support=1.0,
            regime_bonus=regime_bonus,
        )
        p_continue = _compute_continuation_probability(
            support_score=support_score,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            stretch_score=0.0,
            trend_regime="trending",
            gap_dir=1,
        )

        self.assertGreaterEqual(trend_alignment, 0.65)
        self.assertGreaterEqual(discipline_support, 0.80)
        self.assertGreaterEqual(support_score, 0.45)
        self.assertGreaterEqual(p_continue, 0.54)

    def test_eventless_noisy_name_stays_subcritical(self) -> None:
        from quant_bot.analytics.overnight_gate import (
            _compute_continuation_probability,
            _compute_support_score,
            _discipline_support_score,
            _support_regime_bonus,
            _trend_alignment_score,
        )

        trend_alignment = _trend_alignment_score(
            gap_dir=1,
            trend_prob=0.5411,
            trend_regime="noisy",
        )
        discipline_support = _discipline_support_score(
            gap_dir=1,
            gap_vs_expected_move=0.037,
            cone_position_68=0.465,
        )
        regime_bonus = _support_regime_bonus(
            gap_dir=1,
            trend_regime="noisy",
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            flow_intensity=0.0,
            bias_support=0.0,
        )
        support_score = _compute_support_score(
            flow_intensity=0.0,
            iv_delta=None,
            skew_delta=None,
            pc_delta=None,
            bias_support=0.0,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            sentiment_support=0.0,
            regime_bonus=regime_bonus,
        )
        p_continue = _compute_continuation_probability(
            support_score=support_score,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            stretch_score=0.0,
            trend_regime="noisy",
            gap_dir=1,
        )

        self.assertLessEqual(support_score, 0.35)
        self.assertLess(p_continue, 0.50)


class OvernightContinuationAlphaTests(unittest.TestCase):
    def test_high_stretch_gate_prefers_do_not_chase(self) -> None:
        from quant_bot.analytics.overnight_continuation_alpha import CalibrationStats, _score_current

        stats = CalibrationStats()
        for _ in range(8):
            stats.add("alpha_already_paid", "2026-04-01")
        for _ in range(4):
            stats.add("continuation", "2026-04-02")

        scored = _score_current(
            current={
                "gate": {
                    "action": "executable_now",
                    "p_continue": 0.57,
                    "p_fade": 0.42,
                    "support_score": 0.50,
                    "discipline_support": 0.45,
                    "trend_alignment": 0.55,
                    "effective_stretch_score": 0.86,
                    "gap_vs_expected_move": 1.18,
                },
                "options": {"liquidity_score": "fair"},
            },
            stats=stats,
        )

        self.assertEqual(scored["advice"], "do_not_chase")
        self.assertGreaterEqual(scored["paid_risk"], 0.62)

    def test_supported_low_stretch_gate_can_continue(self) -> None:
        from quant_bot.analytics.overnight_continuation_alpha import CalibrationStats, _score_current

        stats = CalibrationStats()
        for _ in range(10):
            stats.add("continuation", "2026-04-03")
        for _ in range(2):
            stats.add("fade", "2026-04-04")

        scored = _score_current(
            current={
                "gate": {
                    "action": "executable_now",
                    "p_continue": 0.66,
                    "p_fade": 0.24,
                    "support_score": 0.68,
                    "discipline_support": 0.74,
                    "trend_alignment": 0.71,
                    "effective_stretch_score": 0.18,
                    "gap_vs_expected_move": 0.32,
                },
                "options": {"liquidity_score": "good"},
                "lab_factor": {"composite": 0.20},
            },
            stats=stats,
        )

        self.assertEqual(scored["advice"], "continue")
        self.assertGreaterEqual(scored["entry_quality"], 0.56)


if __name__ == "__main__":
    unittest.main()
