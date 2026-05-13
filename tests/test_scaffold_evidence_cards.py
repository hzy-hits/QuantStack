"""Tests for the evidence-card draft scaffolder."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "scaffold_evidence_cards_from_readiness.py"


def _load_module():
    if "scaffold_evidence_cards_from_readiness" in sys.modules:
        return sys.modules["scaffold_evidence_cards_from_readiness"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("scaffold_evidence_cards_from_readiness", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _base_queue_row(**overrides: object) -> dict[str, str]:
    row = {
        "rank": "1",
        "priority_tier": "P0_first_batch",
        "ticker": "NVDA",
        "company": "NVIDIA",
        "market_country": "US",
        "asset_pool": "美国资产池",
        "bfs_depth": "D1",
        "module": "GPU/CUDA",
        "current_pool": "核心池",
        "total_score": "100",
        "score_bucket": "core_review",
        "verification_status": "pending_original_source_verification",
        "source_priority": "Find filings",
        "primary_sources_to_find": "10-K, earnings call, investor deck",
        "metrics_to_verify": "data center revenue, gross margin, customer concentration",
        "upgrade_conditions": "Customer disclosures, supply-chain transmission",
        "downgrade_conditions": "Margin compression, export controls",
        "evidence_state": "原文已证明: data center segment strong; 具体客户仍需核验",
        "counterevidence": "ASIC替代, 毛利率下行",
        "dependency_path": "Token demand → GPU cluster → HBM",
        "dependency_edge": "客户边",
        "etf_clue": "SMH",
        "smart_money_clue": "13F",
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def _base_readiness_row(**overrides: object) -> dict[str, str]:
    row = {
        "rank": "1",
        "ticker": "NVDA",
        "company": "NVIDIA",
        "asset_pool": "美国资产池",
        "market_country": "US",
        "bfs_depth": "D1",
        "priority_tier": "P0_first_batch",
        "module": "GPU/CUDA",
        "verification_status": "pending_original_source_verification",
        "evidence_signals": "evidence_proved",
        "evidence_score": "1.00",
        "readiness_tier": "ready_for_promotion",
        "missing_fields": "",
        "evidence_state": "原文已证明: data center segment strong",
        "counterevidence": "ASIC替代",
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


class ScaffoldEvidenceCardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_only_eligible_tiers_produce_drafts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue = root / "queue.csv"
            readiness = root / "readiness.csv"
            _write_csv(queue, QUEUE_HEADERS, [_base_queue_row()])
            _write_csv(
                readiness,
                READINESS_HEADERS,
                [
                    _base_readiness_row(readiness_tier="ready_for_promotion"),
                    _base_readiness_row(ticker="DGXX", rank="2", company="DataGo", readiness_tier="pending_human_review"),
                    _base_readiness_row(ticker="AVGO", rank="3", company="Broadcom", readiness_tier="evidence_partial"),
                ],
            )
            queue_rows = self.module.load_queue(queue)
            specs = self.module.collect_specs(self.module.load_readiness(readiness), queue_rows)
            tickers = {spec.primary_ticker for spec in specs}
            self.assertEqual(tickers, {"NVDA", "AVGO"})

    def test_render_card_includes_evidence_anchors_and_counters(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue_path = root / "queue.csv"
            readiness_path = root / "readiness.csv"
            _write_csv(queue_path, QUEUE_HEADERS, [_base_queue_row()])
            _write_csv(readiness_path, READINESS_HEADERS, [_base_readiness_row()])
            specs = self.module.collect_specs(
                self.module.load_readiness(readiness_path),
                self.module.load_queue(queue_path),
            )
            md = self.module.render_card(specs[0], "2026-05-13")
            self.assertIn("# Evidence Card Draft — NVIDIA (NVDA)", md)
            self.assertIn("Revenue / segment revenue", md)
            self.assertIn("data center revenue", md)
            self.assertIn("ASIC替代", md)
            self.assertIn("Token demand → GPU cluster → HBM", md)

    def test_write_drafts_creates_files_and_index(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue_path = root / "queue.csv"
            readiness_path = root / "readiness.csv"
            _write_csv(
                queue_path,
                QUEUE_HEADERS,
                [_base_queue_row(), _base_queue_row(rank="2", ticker="2330.TW / TSM", company="TSMC", market_country="台湾", asset_pool="卫星资产池", bfs_depth="D2", module="Foundry")],
            )
            _write_csv(
                readiness_path,
                READINESS_HEADERS,
                [
                    _base_readiness_row(),
                    _base_readiness_row(
                        ticker="2330.TW / TSM",
                        company="TSMC",
                        rank="2",
                        bfs_depth="D2",
                        asset_pool="卫星资产池",
                        market_country="台湾",
                        readiness_tier="ready_for_promotion",
                    ),
                ],
            )
            specs = self.module.collect_specs(
                self.module.load_readiness(readiness_path),
                self.module.load_queue(queue_path),
            )
            out_dir = root / "drafts"
            written = self.module.write_drafts(specs, out_dir, "2026-05-13")
            names = sorted(p.name for p in written)
            self.assertEqual(names, ["NVDA.md", "TSM.md"])
            index = (out_dir / "INDEX.md").read_text(encoding="utf-8")
            self.assertIn("NVIDIA", index)
            self.assertIn("TSMC", index)


if __name__ == "__main__":
    unittest.main()
