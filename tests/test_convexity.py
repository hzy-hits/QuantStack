"""Tests for the convexity classifier + anti-convex guardrail."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "scripts"))

from lib.convexity import (  # noqa: E402
    AntiConvexExpressionError,
    assert_no_anticonvex,
    classify_convexity,
    convexity_label,
)


class ConvexityClassifierTests(unittest.TestCase):
    def test_long_options_are_convex(self) -> None:
        for expr in ("buy OTM put", "long call", "LEAPS call", "victim put",
                     "TLT put-spread", "深虚值 call", "buy_put"):
            self.assertEqual(classify_convexity(expr), "convex", expr)

    def test_selling_premium_is_anti_convex(self) -> None:
        for expr in ("sell put", "covered call", "short straddle",
                     "credit spread", "short vol", "卖期权", "iron condor",
                     "naked call"):
            self.assertEqual(classify_convexity(expr), "anti_convex", expr)

    def test_stock_long_is_linear(self) -> None:
        for expr in ("buy_stock_position", "buy_stock_with_options_confirmation",
                     "计划买入", "buy stock"):
            self.assertEqual(classify_convexity(expr), "linear", expr)

    def test_no_trade_actions_are_none(self) -> None:
        for expr in ("rank_only_no_new_trade", "evidence_state_pending_no_trade",
                     "no_trade", "", None):
            self.assertEqual(classify_convexity(expr), "none", str(expr))

    def test_anti_convex_checked_before_convex(self) -> None:
        # "credit spread" contains "spread" — must still resolve anti_convex.
        self.assertEqual(classify_convexity("put credit spread"), "anti_convex")
        # "covered call" contains "call" — must still resolve anti_convex.
        self.assertEqual(classify_convexity("covered call write"), "anti_convex")

    def test_credit_structures_cannot_bypass_via_wording(self) -> None:
        # Codex review P0: these all contain a convex token (put/call spread)
        # but are credit / premium-selling structures — must be anti_convex.
        for expr in ("bull put spread", "bear call spread", "credit put spread",
                     "cash-secured put", "cash secured put", "short premium",
                     "sell straddle", "sell strangle", "iron butterfly",
                     "ratio spread", "信用价差", "备兑开仓"):
            self.assertEqual(classify_convexity(expr), "anti_convex", expr)

    def test_label_is_human_readable(self) -> None:
        self.assertIn("凸", convexity_label("buy put"))
        self.assertIn("线性", convexity_label("buy_stock_position"))
        self.assertIn("反凸", convexity_label("sell put"))


class AntiConvexGuardrailTests(unittest.TestCase):
    def test_clean_expressions_pass(self) -> None:
        # Must not raise.
        assert_no_anticonvex([
            "buy_stock_position", "victim put 60-DTE", "TLT put-spread",
            "rank_only_no_new_trade",
        ])

    def test_anti_convex_expression_raises(self) -> None:
        with self.assertRaises(AntiConvexExpressionError):
            assert_no_anticonvex(["buy_stock_position", "sell covered call"])

    def test_error_message_names_the_offender(self) -> None:
        try:
            assert_no_anticonvex(["short straddle on NVDA"])
        except AntiConvexExpressionError as exc:
            self.assertIn("short straddle on NVDA", str(exc))
        else:
            self.fail("expected AntiConvexExpressionError")

    def test_empty_iterable_passes(self) -> None:
        assert_no_anticonvex([])


if __name__ == "__main__":
    unittest.main()
