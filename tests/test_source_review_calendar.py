"""Unit tests for the AI Infra source-review calendar wiring."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load_module():
    """Import generate_main_strategy_v2_report without executing __main__."""
    if "generate_main_strategy_v2_report" in sys.modules:
        return sys.modules["generate_main_strategy_v2_report"]
    sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("generate_main_strategy_v2_report", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CSV_HEADER = (
    "rank,priority_tier,ticker,company,market_country,asset_pool,bfs_depth,module,"
    "current_pool,total_score,score_bucket,verification_status,source_priority,"
    "primary_sources_to_find,metrics_to_verify,upgrade_conditions,downgrade_conditions,"
    "evidence_state,counterevidence,dependency_path,dependency_edge,etf_clue,smart_money_clue\n"
)


def _row(rank: int, ticker: str, asset_pool: str, tier: str = "P0_first_batch") -> str:
    return (
        f"{rank},{tier},{ticker},Company{rank},US,{asset_pool},D2,Module{rank},"
        "候选,100,core_review,pending_original_source_verification,Find docs,"
        "filings,metrics,upgrade conditions,downgrade conditions,evidence,"
        "counter,path,edge,etf,smart\n"
    )


class SourceReviewCalendarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_buckets_split_by_asset_pool(self) -> None:
        with TemporaryDirectory() as tmp:
            queue_path = Path(tmp) / "queue.csv"
            queue_path.write_text(
                CSV_HEADER
                + _row(1, "NVDA", "美国资产池")
                + _row(2, "002463.SZ", "中国资产池")
                + _row(3, "2330.TW", "卫星资产池")
                + _row(4, "AAPL", "美国资产池", tier="P3"),
                encoding="utf-8",
            )
            result = self.module.build_source_review_calendar(queue_path=queue_path)
            us_tickers = {row["ticker"] for row in result["us"]["rows"]}
            cn_tickers = {row["ticker"] for row in result["cn"]["rows"]}
            self.assertEqual(us_tickers, {"NVDA", "2330.TW", "AAPL"})
            self.assertEqual(cn_tickers, {"002463.SZ"})

    def test_focus_symbol_is_prioritized_above_tier(self) -> None:
        with TemporaryDirectory() as tmp:
            queue_path = Path(tmp) / "queue.csv"
            queue_path.write_text(
                CSV_HEADER
                + _row(1, "NVDA", "美国资产池", tier="P0_first_batch")
                + _row(2, "AMD", "美国资产池", tier="P0_first_batch")
                + _row(3, "AAOI", "美国资产池", tier="P3"),
                encoding="utf-8",
            )
            result = self.module.build_source_review_calendar(
                queue_path=queue_path,
                focus_symbols=["AAOI"],
            )
            us = result["us"]
            self.assertEqual(us["focus_match_count"], 1)
            self.assertEqual(us["rows"][0]["ticker"], "AAOI")

    def test_missing_queue_returns_status(self) -> None:
        with TemporaryDirectory() as tmp:
            queue_path = Path(tmp) / "missing.csv"
            result = self.module.build_source_review_calendar(queue_path=queue_path)
            self.assertEqual(result["us"]["status"], "missing_queue")
            self.assertEqual(result["cn"]["status"], "missing_queue")

    def test_renderer_produces_table_header(self) -> None:
        payload = {
            "source_review_calendar": {
                "us": {
                    "status": "ok",
                    "scope": "all_symbols",
                    "focus_symbol_count": 0,
                    "focus_match_count": 0,
                    "queue_path": "ai_infra/reports/source_verification_queue_v1.csv",
                    "rows": [
                        {
                            "priority_tier": "P0_first_batch",
                            "primary_ticker": "NVDA",
                            "ticker": "NVDA",
                            "company": "NVIDIA",
                            "bfs_depth": "D1",
                            "module": "GPU",
                            "verification_status": "pending_original_source_verification",
                            "upgrade_conditions": "original sources prove sustained demand",
                            "market_context_notes": "[options-flow-alert 2026-05-13: squeeze_score=9000]",
                        }
                    ],
                },
                "cn": {"status": "ok", "rows": []},
            }
        }
        lines = self.module.render_source_review_calendar_section(payload, "US")
        rendered = "\n".join(lines)
        self.assertIn("AI Infra Source Review Calendar (US)", rendered)
        self.assertIn("| NVDA |", rendered)
        # Renderer surfaces readiness_tier instead of verification_status; the
        # raw verification field still ships through the JSON artifact.
        self.assertIn("Readiness", rendered)
        self.assertIn("Market Context", rendered)
        self.assertIn("options-flow-alert", rendered)


if __name__ == "__main__":
    unittest.main()
