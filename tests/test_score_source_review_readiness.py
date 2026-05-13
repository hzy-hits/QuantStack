"""Tests for the AI Infra source-review readiness scorer."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_source_review_readiness.py"


def _load_module():
    if "score_source_review_readiness" in sys.modules:
        return sys.modules["score_source_review_readiness"]
    spec = importlib.util.spec_from_file_location("score_source_review_readiness", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


HEADERS = [
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


def _write_queue(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in HEADERS})


def _base_row(**overrides) -> dict[str, str]:
    row = {
        "rank": "1",
        "priority_tier": "P0_first_batch",
        "ticker": "EXM",
        "company": "Example Co",
        "market_country": "US",
        "asset_pool": "美国资产池",
        "bfs_depth": "D2",
        "module": "HBM",
        "current_pool": "核心池",
        "total_score": "100",
        "score_bucket": "core_review",
        "verification_status": "pending_original_source_verification",
        "source_priority": "Find 10-K",
        "primary_sources_to_find": "10-K / earnings call",
        "metrics_to_verify": "HBM revenue, capacity, margins",
        "upgrade_conditions": "Original sources prove AI HBM demand",
        "downgrade_conditions": "HBM oversupply, ASP decline",
        "evidence_state": "原文已证明: data center segment strong; 具体客户仍需核验",
        "counterevidence": "ASIC替代",
        "dependency_path": "GPU -> HBM -> revenue",
        "dependency_edge": "BOM边",
        "etf_clue": "SMH",
        "smart_money_clue": "13F",
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


class ReadinessScorerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_ready_for_promotion_when_proved_and_complete(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_base_row()])
            rows = self.module.score_queue(queue)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].readiness_tier, "ready_for_promotion")
            self.assertGreaterEqual(rows[0].evidence_score, 0.95)

    def test_g0_blocked_when_no_primary_source(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_base_row(primary_sources_to_find="-")])
            rows = self.module.score_queue(queue)
            self.assertEqual(rows[0].readiness_tier, "g0_blocked")
            self.assertIn("no_primary_source", rows[0].evidence_signals)

    def test_pending_when_only_pending_state(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_base_row(evidence_state="待原文核验: AI server revenue")])
            rows = self.module.score_queue(queue)
            self.assertEqual(rows[0].readiness_tier, "pending_human_review")

    def test_blocked_by_counterevidence_when_many_items_and_partial(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(
                queue,
                [
                    _base_row(
                        evidence_state="合理推论: AI mix likely",
                        counterevidence="ASIC替代,毛利率下行,出口管制,客户集中",
                    )
                ],
            )
            rows = self.module.score_queue(queue)
            self.assertEqual(rows[0].readiness_tier, "blocked_by_counterevidence")

    def test_evidence_partial_when_partial_state_present(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(
                queue,
                [
                    _base_row(
                        evidence_state="合理推论 + 待原文核验: customer concentration",
                        counterevidence="单客户集中",
                    )
                ],
            )
            rows = self.module.score_queue(queue)
            self.assertEqual(rows[0].readiness_tier, "evidence_partial")

    def test_write_outputs_emits_csv_and_md(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_base_row(), _base_row(ticker="OTH", primary_sources_to_find="-")])
            out_csv = Path(tmp) / "out.csv"
            out_md = Path(tmp) / "out.md"
            rows = self.module.score_queue(queue)
            self.module.write_outputs(rows, out_csv, out_md, queue)
            self.assertTrue(out_csv.exists())
            self.assertTrue(out_md.exists())
            with out_csv.open("r", encoding="utf-8") as handle:
                csv_rows = list(csv.DictReader(handle))
            self.assertEqual({row["ticker"] for row in csv_rows}, {"EXM", "OTH"})
            md = out_md.read_text(encoding="utf-8")
            self.assertIn("ready_for_promotion", md)
            self.assertIn("g0_blocked", md)


if __name__ == "__main__":
    unittest.main()
