"""Tests for the AI Infra satellite-pool section in the daily report."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_main_strategy_v2_backtest.py"

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


def _load_module():
    if "run_main_strategy_v2_backtest" in sys.modules:
        return sys.modules["run_main_strategy_v2_backtest"]
    sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("run_main_strategy_v2_backtest", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _row(rank: int, ticker: str, region: str, asset_pool: str, depth: str = "D3", **overrides) -> dict[str, str]:
    base = {
        "rank": str(rank),
        "priority_tier": "P0_first_batch",
        "ticker": ticker,
        "company": f"Company{rank}",
        "market_country": region,
        "asset_pool": asset_pool,
        "bfs_depth": depth,
        "module": "Module",
        "current_pool": "候选",
        "total_score": "100",
        "score_bucket": "core_review",
        "verification_status": "pending_original_source_verification",
        "source_priority": "find filings",
        "primary_sources_to_find": "10-K",
        "metrics_to_verify": "revenue",
        "upgrade_conditions": "AI demand confirmed",
        "downgrade_conditions": "demand evaporates",
        "evidence_state": "待原文核验",
        "counterevidence": "competition",
        "dependency_path": "GPU -> HBM",
        "dependency_edge": "客户边",
        "etf_clue": "SMH",
        "smart_money_clue": "13F",
    }
    base.update({key: str(value) for key, value in overrides.items()})
    return base


def _write_queue(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in HEADERS})


class SatellitePoolReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_only_satellite_pool_rows_kept_and_region_labeled(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(
                queue,
                [
                    _row(1, "NVDA", "US", "美国资产池"),
                    _row(2, "002463.SZ", "A股主板", "中国资产池"),
                    _row(3, "2330.TW", "台湾", "卫星资产池", depth="D2"),
                    _row(4, "6857.T", "日本", "卫星资产池"),
                    _row(5, "BESI.AS", "欧洲", "卫星资产池"),
                    _row(6, "000660.KS", "韩国", "卫星资产池", depth="D2"),
                ],
            )
            report = self.module.build_satellite_pool_report(queue_path=queue)
            self.assertEqual(report["total_rows"], 4)
            tickers = {row["primary_ticker"] for row in report["rows"]}
            self.assertEqual(tickers, {"2330.TW", "6857.T", "BESI.AS", "000660.KS"})
            self.assertEqual(report["region_counts"]["Taiwan"], 1)
            self.assertEqual(report["region_counts"]["Japan"], 1)
            self.assertEqual(report["region_counts"]["Europe"], 1)
            self.assertEqual(report["region_counts"]["Korea"], 1)
            self.assertEqual(report["depth_counts"]["D2"], 2)

    def test_renderer_emits_region_headers_and_readiness(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(
                queue,
                [
                    _row(
                        1,
                        "2330.TW",
                        "台湾",
                        "卫星资产池",
                        depth="D2",
                        evidence_state="原文已证明: quarterly results 公开披露",
                    ),
                    _row(2, "6857.T", "日本", "卫星资产池"),
                ],
            )
            report = self.module.build_satellite_pool_report(queue_path=queue)
            payload = {"satellite_pool_report": report}
            rendered = "\n".join(self.module.render_satellite_pool_report_section(payload))
            self.assertIn("AI Infra Satellite Pool", rendered)
            self.assertIn("### Taiwan", rendered)
            self.assertIn("### Japan", rendered)
            self.assertIn("ready_for_promotion", rendered)
            self.assertIn("2330.TW", rendered)

    def test_missing_queue_returns_status(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "absent.csv"
            report = self.module.build_satellite_pool_report(queue_path=queue)
            self.assertEqual(report["status"], "missing_queue")
            self.assertEqual(report["rows"], [])


if __name__ == "__main__":
    unittest.main()
