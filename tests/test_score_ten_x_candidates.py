"""Tests for the AI Infra 10x candidate scorer."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_ten_x_candidates.py"


def _load_module():
    if "score_ten_x_candidates" in sys.modules:
        return sys.modules["score_ten_x_candidates"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_ten_x_candidates", SCRIPT_PATH)
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


def _row(rank: int, ticker: str, depth: str, counter: str = "competition", **overrides) -> dict[str, str]:
    base = {
        "rank": str(rank),
        "priority_tier": "P0_first_batch",
        "ticker": ticker,
        "company": f"Company{rank}",
        "market_country": "US",
        "asset_pool": "美国资产池",
        "bfs_depth": depth,
        "module": "Module",
        "current_pool": "候选",
        "total_score": "100",
        "score_bucket": "core_review",
        "verification_status": "pending_original_source_verification",
        "source_priority": "Find filings",
        "primary_sources_to_find": "10-K",
        "metrics_to_verify": "revenue",
        "upgrade_conditions": "AI demand confirmed",
        "downgrade_conditions": "demand evaporates",
        "evidence_state": "待原文核验",
        "counterevidence": counter,
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


class TenXCandidateScorerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_filter_keeps_d2_d3_under_cap(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(
                queue,
                [
                    _row(1, "SMALL", "D3"),     # 8B → kept
                    _row(2, "BIG", "D3"),       # 80B → filtered (too large)
                    _row(3, "DEEP", "D5"),      # depth not in eligible set
                    _row(4, "NOSRC", "D3", primary_sources_to_find="-"),  # g0_blocked filtered
                    _row(5, "MANYCNT", "D3", counter="a,b,c,d,e"),       # too many counters
                ],
            )
            cache = {
                "SMALL": {"ticker": "SMALL", "market_cap": 8_000_000_000.0, "fetched_at": "2026-05-13T00:00:00+00:00"},
                "BIG": {"ticker": "BIG", "market_cap": 80_000_000_000.0, "fetched_at": "2026-05-13T00:00:00+00:00"},
                "DEEP": {"ticker": "DEEP", "market_cap": 1_000_000_000.0, "fetched_at": "2026-05-13T00:00:00+00:00"},
                "NOSRC": {"ticker": "NOSRC", "market_cap": 5_000_000_000.0, "fetched_at": "2026-05-13T00:00:00+00:00"},
                "MANYCNT": {"ticker": "MANYCNT", "market_cap": 5_000_000_000.0, "fetched_at": "2026-05-13T00:00:00+00:00"},
            }
            candidates, _ = self.module.collect_candidates(
                queue,
                cache=cache,
                fetch=False,
                cap_ceiling=50_000_000_000.0,
                max_counter_items=3,
            )
            tickers = [c.primary_ticker for c in candidates]
            self.assertEqual(tickers, ["SMALL"])
            self.assertGreater(candidates[0].elasticity_score, 70)

    def test_alias_prefers_adr_token(self) -> None:
        self.assertEqual(self.module._primary_ticker_for("2330.TW / TSM"), "TSM")
        self.assertEqual(self.module._primary_ticker_for("3711.TW / ASX"), "ASX")
        self.assertEqual(self.module._primary_ticker_for("0522.HK"), "0522.HK")
        self.assertEqual(self.module._primary_ticker_for("NVDA"), "NVDA")

    def test_missing_mcap_kept_with_note(self) -> None:
        with TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue.csv"
            _write_queue(queue, [_row(1, "002463.SZ", "D3")])
            candidates, _ = self.module.collect_candidates(
                queue,
                cache={},
                fetch=False,
                cap_ceiling=50_000_000_000.0,
                max_counter_items=3,
            )
            self.assertEqual(len(candidates), 1)
            self.assertIsNone(candidates[0].market_cap)
            self.assertEqual(candidates[0].mcap_bucket, "unknown")
            self.assertIn("offline", candidates[0].notes)

    def test_render_markdown_emits_section_headers(self) -> None:
        candidates = [
            self.module.TenXCandidate(
                rank=1,
                primary_ticker="EXM",
                ticker_field="EXM",
                company="Example",
                asset_pool="美国资产池",
                market_country="US",
                bfs_depth="D3",
                module="probe card",
                priority_tier="P0_first_batch",
                readiness_tier="pending_human_review",
                readiness_score=0.85,
                market_cap=5_000_000_000.0,
                elasticity_score=80.5,
                elasticity_signals=["depth_D3=35"],
                counter_items=1,
                mcap_bucket="<5B micro",
                notes="",
                evidence_state="待原文核验",
                counterevidence="competition",
                primary_sources_to_find="10-K",
                metrics_to_verify="revenue",
                upgrade_conditions="AI demand confirmed",
            )
        ]
        md = self.module.render_markdown(candidates, "2026-05-13", 50_000_000_000.0)
        self.assertIn("# AI Infra 10x Candidate Radar", md)
        self.assertIn("EXM", md)
        self.assertIn("Top Elasticity", md)


if __name__ == "__main__":
    unittest.main()
