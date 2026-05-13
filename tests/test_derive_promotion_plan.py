"""Tests for the readiness → promotion plan deriver."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "derive_promotion_plan_from_readiness.py"


def _load_module():
    if "derive_promotion_plan_from_readiness" in sys.modules:
        return sys.modules["derive_promotion_plan_from_readiness"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("derive_promotion_plan_from_readiness", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


READINESS_HEADERS = [
    "rank",
    "ticker",
    "company",
    "asset_pool",
    "market_country",
    "bfs_depth",
    "priority_tier",
    "module",
    "verification_status",
    "evidence_signals",
    "evidence_score",
    "readiness_tier",
    "missing_fields",
    "evidence_state",
    "counterevidence",
]


QUEUE_HEADERS = [
    "rank",
    "priority_tier",
    "ticker",
    "company",
    "market_country",
    "asset_pool",
    "bfs_depth",
    "module",
    "current_pool",
    "total_score",
    "score_bucket",
    "verification_status",
    "source_priority",
    "primary_sources_to_find",
    "metrics_to_verify",
    "upgrade_conditions",
    "downgrade_conditions",
    "evidence_state",
    "counterevidence",
    "dependency_path",
    "dependency_edge",
    "etf_clue",
    "smart_money_clue",
]


def _write(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _readiness(**overrides: object) -> dict[str, str]:
    row = {
        "rank": "1",
        "ticker": "NVDA",
        "company": "NVIDIA",
        "asset_pool": "美国资产池",
        "market_country": "US",
        "bfs_depth": "D1",
        "priority_tier": "P0_first_batch",
        "module": "GPU",
        "verification_status": "pending_original_source_verification",
        "evidence_signals": "evidence_proved",
        "evidence_score": "1.00",
        "readiness_tier": "ready_for_promotion",
        "missing_fields": "",
        "evidence_state": "原文已证明",
        "counterevidence": "ASIC替代",
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def _queue(rank: str = "1") -> dict[str, str]:
    return {
        "rank": rank,
        "priority_tier": "P0_first_batch",
        "ticker": "NVDA",
        "company": "NVIDIA",
        "market_country": "US",
        "asset_pool": "美国资产池",
        "bfs_depth": "D1",
        "module": "GPU",
        "current_pool": "核心池",
        "total_score": "100",
        "score_bucket": "core_review",
        "verification_status": "pending_original_source_verification",
        "source_priority": "Find filings",
        "primary_sources_to_find": "10-K",
        "metrics_to_verify": "revenue, margin",
        "upgrade_conditions": "AI demand confirmed",
        "downgrade_conditions": "margin pressure",
        "evidence_state": "原文已证明",
        "counterevidence": "ASIC替代",
        "dependency_path": "tokens -> GPU",
        "dependency_edge": "客户边",
        "etf_clue": "SMH",
        "smart_money_clue": "13F",
    }


class PromotionPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_recommendation_mapping_covers_each_tier(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            readiness_path = root / "readiness.csv"
            queue_path = root / "queue.csv"
            _write(
                readiness_path,
                READINESS_HEADERS,
                [
                    _readiness(readiness_tier="ready_for_promotion"),
                    _readiness(rank="2", ticker="AVGO", readiness_tier="evidence_partial"),
                    _readiness(rank="3", ticker="COHR", readiness_tier="pending_human_review"),
                    _readiness(rank="4", ticker="ZZZ", readiness_tier="blocked_by_counterevidence"),
                    _readiness(rank="5", ticker="NOSRC", readiness_tier="g0_blocked"),
                    _readiness(rank="6", ticker="UNK", readiness_tier="unscored"),
                ],
            )
            _write(queue_path, QUEUE_HEADERS, [_queue(rank=str(i)) for i in range(1, 7)])
            plan = self.module.build_plan(readiness_path, queue_path)
            counts = Counter(line.recommendation for line in plan)
            self.assertEqual(counts["promote_now"], 1)
            self.assertEqual(counts["watch_with_review"], 1)
            self.assertEqual(counts["research_only"], 1)
            self.assertEqual(counts["reject_until_resolved"], 1)
            self.assertEqual(counts["gate_g0_no_promotion"], 1)
            self.assertEqual(counts["needs_template_fill"], 1)

    def test_render_markdown_lists_recommendation_sections(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            readiness_path = root / "readiness.csv"
            queue_path = root / "queue.csv"
            _write(readiness_path, READINESS_HEADERS, [_readiness()])
            _write(queue_path, QUEUE_HEADERS, [_queue()])
            plan = self.module.build_plan(readiness_path, queue_path)
            md = self.module.render_markdown(plan, "2026-05-13")
            self.assertIn("AI Infra Promotion Plan", md)
            self.assertIn("promote_now", md)
            self.assertIn("NVDA", md)


if __name__ == "__main__":
    unittest.main()
