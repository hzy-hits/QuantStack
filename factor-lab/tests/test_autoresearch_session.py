from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.autoresearch.session_state import ensure_session_files
from src.agent.loop import FactorSession
from src.agent.prompts import build_system_prompt, parse_agent_response


class AutoresearchSessionTests(unittest.TestCase):
    def test_ensure_session_files_creates_pi_style_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = ensure_session_files("us", goal="Optimize US factor mining loop", root=root)

            self.assertTrue(paths.session_doc.exists())
            self.assertTrue(paths.benchmark_script.exists())
            self.assertTrue(paths.checks_script.exists())
            self.assertTrue(paths.log_file.exists())

            context_text = paths.session_doc.read_text(encoding="utf-8")
            benchmark_text = paths.benchmark_script.read_text(encoding="utf-8")
            checks_text = paths.checks_script.read_text(encoding="utf-8")

            self.assertIn("Optimize US factor mining loop", context_text)
            self.assertIn("METRIC is_ic_ir", benchmark_text)
            self.assertIn("--eval-composite --market \"$MARKET\"", checks_text)
            self.assertNotIn(">/dev/null", checks_text)

    def test_system_prompt_embeds_resumable_session_context(self) -> None:
        prompt = build_system_prompt(
            market="cn",
            session_context="# Session Notes\nTry volume-stability families first.",
        )
        self.assertIn("Resumable Session Context", prompt)
        self.assertIn("Try volume-stability families first.", prompt)

    def test_system_prompt_requires_ai_infra_quant_fund_inputs(self) -> None:
        prompt = build_system_prompt(market="us")

        self.assertIn("ai_infra/data/global_universe_v2.jsonl", prompt)
        self.assertIn("CDS/credit spreads", prompt)
        self.assertIn("options IV/skew/VRP/flow", prompt)
        self.assertIn("beta hedge return", prompt)
        self.assertIn("portfolio risk attribution", prompt)

    def test_agent_response_parser_accepts_new_contract_fields(self) -> None:
        parsed = parse_agent_response(
            """
            HYPOTHESIS: Forced unwind after crowded volume spikes.
            FORMULA: rank(-ret_5d)
            DIRECTION: long
            NAME: reversal_overlay
            SLEEVE: Daily Price Overlay
            MISPRICING_SOURCE: stale forced sellers
            FORCED_COUNTERPARTY: liquidation flow
            DATA_REQUIREMENTS: ["prices"]
            FAILURE_MODE: crowded
            REPORT_CONTRACT: action_overlay
            """
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.sleeve_id, "daily_price_overlay")
        self.assertEqual(parsed.report_contract, "action_overlay")
        self.assertEqual(parsed.mispricing_source, "stale forced sellers")

    def test_agent_response_parser_keeps_old_format_research_only(self) -> None:
        parsed = parse_agent_response(
            """
            HYPOTHESIS: Old format.
            FORMULA: rank(close)
            DIRECTION: long
            NAME: old_format
            """
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.sleeve_id, "daily_price_overlay")
        self.assertEqual(parsed.report_contract, "research_only")

    def test_summary_does_not_promote_oos_pass_that_checks_reverted(self) -> None:
        session = FactorSession.__new__(FactorSession)
        session.session_id = "test-session"
        session.market = "cn"
        session.budget = 3
        session.branch_context = {"session_branch": "test-branch"}
        session.experiments = [
            {
                "name": "reverted_oos_pass",
                "formula": "rank(close)",
                "is_ic": 0.04,
                "is_ic_ir": 0.3,
                "is_sharpe": 1.2,
                "gates_passed": True,
                "decision": "revert",
            }
        ]

        summary = session._build_summary(
            [
                {
                    "name": "reverted_oos_pass",
                    "formula": "rank(close)",
                    "is_ic": 0.04,
                    "oos_pass": True,
                    "decision": "revert",
                }
            ]
        )

        self.assertIn("## 结论", summary)
        self.assertIn("OOS 通过：1/1", summary)
        self.assertIn("keep：0/1", summary)
        self.assertIn("为什么 OOS PASS 但不 keep", summary)
        self.assertIn("## 是否进入主系统", summary)
        self.assertIn("否。本轮没有因子同时通过 OOS 和 checks", summary)
        self.assertIn("公式（不截断）", summary)
        self.assertIn("rank(close)", summary)
        self.assertNotIn("reverted_oos_pass (id=", summary)


if __name__ == "__main__":
    unittest.main()
