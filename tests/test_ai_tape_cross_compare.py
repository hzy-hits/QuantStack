"""Tests for the AI tape cross-compare page."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "build_ai_tape_cross_compare.py"


def _load_module():
    if "build_ai_tape_cross_compare" in sys.modules:
        return sys.modules["build_ai_tape_cross_compare"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("build_ai_tape_cross_compare", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


TEN_X_FIELDS = [
    "rank",
    "primary_ticker",
    "ticker_field",
    "company",
    "asset_pool",
    "market_country",
    "bfs_depth",
    "module",
    "priority_tier",
    "readiness_tier",
    "readiness_score",
    "market_cap_usd",
    "mcap_bucket",
    "counter_items",
    "elasticity_score",
    "elasticity_signals",
    "notes",
    "evidence_state",
    "counterevidence",
    "primary_sources_to_find",
    "metrics_to_verify",
    "upgrade_conditions",
    "ema_cross_state",
    "ema_slope_5d_pct",
    "ema_dist_close_ema21_pct",
]


MR_FIELDS = [
    "rank",
    "symbol",
    "company_name",
    "sector",
    "market_cap_usd_b",
    "latest_close",
    "ret_5d_pct",
    "ret_20d_pct",
    "ema21",
    "ema50",
    "slope_ema21_5d_pct",
    "dist_close_ema21_pct",
    "dist_close_ema50_pct",
    "in_ai_universe",
    "is_mean_reversion_candidate",
    "reasons",
    "next_earnings_date",
    "days_to_earnings",
    "earnings_block",
    "pe_ttm",
    "ps_ratio",
    "ev_ebitda",
    "sector_pe_median",
    "sector_ps_median",
    "valuation_signal",
]


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


class AiTapeCrossCompareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_leader_filter_keeps_only_bull_rising(self) -> None:
        with TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "ten_x.csv"
            _write_csv(
                csv_path,
                TEN_X_FIELDS,
                [
                    {
                        "primary_ticker": "AAOI",
                        "company": "Applied Optoelectronics",
                        "asset_pool": "美国资产池",
                        "bfs_depth": "D2-D3",
                        "market_cap_usd": "15000000000",
                        "ema_cross_state": "bull",
                        "ema_slope_5d_pct": "5.81",
                        "ema_dist_close_ema21_pct": "18.89",
                        "readiness_tier": "pending_human_review",
                        "elasticity_score": "61.5",
                    },
                    {
                        "primary_ticker": "FOO",
                        "ema_cross_state": "bear",
                        "ema_slope_5d_pct": "-1.0",
                    },
                    {
                        "primary_ticker": "BAR",
                        "ema_cross_state": "bull",
                        "ema_slope_5d_pct": "0.2",  # below 0.5 threshold
                    },
                ],
            )
            leaders = self.module._load_leaders(csv_path)
            self.assertEqual({l.ticker for l in leaders}, {"AAOI"})

    def test_laggard_filter_keeps_ai_universe_candidates(self) -> None:
        with TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "mr.csv"
            _write_csv(
                csv_path,
                MR_FIELDS,
                [
                    {
                        "rank": "69",
                        "symbol": "ANET",
                        "company_name": "Arista",
                        "sector": "Communications",
                        "market_cap_usd_b": "171.8",
                        "is_mean_reversion_candidate": "yes",
                        "in_ai_universe": "yes",
                        "reasons": "lagging_market_5d",
                        "ret_5d_pct": "-16.3",
                        "dist_close_ema21_pct": "-7.8",
                        "slope_ema21_5d_pct": "-4.9",
                        "valuation_signal": "rich_vs_sector",
                    },
                    {
                        "rank": "28",
                        "symbol": "BAC",
                        "is_mean_reversion_candidate": "yes",
                        "in_ai_universe": "no",
                    },
                    {
                        "rank": "1",
                        "symbol": "NVDA",
                        "is_mean_reversion_candidate": "no",
                        "in_ai_universe": "yes",
                    },
                ],
            )
            laggards = self.module._load_laggards(csv_path, ai_only=True)
            self.assertEqual({l.symbol for l in laggards}, {"ANET"})

    def test_render_markdown_emits_both_sections(self) -> None:
        leader = self.module.LeaderRow(
            ticker="AAOI",
            company="Applied Optoelectronics",
            asset_pool="美国资产池",
            bfs_depth="D2-D3",
            market_cap_b=15.0,
            cross_state="bull",
            slope_5d_pct=5.81,
            dist_close_ema21_pct=18.89,
            readiness_tier="pending_human_review",
            elasticity_score=61.5,
        )
        laggard = self.module.LaggardRow(
            rank=69,
            symbol="ANET",
            company="Arista",
            sector="Communications",
            market_cap_b=171.8,
            ret_5d_pct=-16.3,
            dist_close_ema21_pct=-7.8,
            slope_ema21_5d_pct=-4.9,
            valuation_signal="rich_vs_sector",
            next_earnings_date="",
            reasons="lagging_market_5d",
        )
        md = self.module.render_markdown([leader], [laggard], "2026-05-13")
        self.assertIn("AI Tape Cross-Compare", md)
        self.assertIn("AI Tape Leaders", md)
        self.assertIn("AI Mean-Reversion", md)
        self.assertIn("AAOI", md)
        self.assertIn("ANET", md)


if __name__ == "__main__":
    unittest.main()
