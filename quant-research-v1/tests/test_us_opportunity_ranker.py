from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = REPO_ROOT / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import us_opportunity_ranker as ranker  # noqa: E402


def _write_ai_infra_universe(root: Path, rows: list[dict]) -> None:
    path = root / "data" / "global_universe_v2.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


class UsOpportunityRankerTests(unittest.TestCase):
    def test_gamma_v2_alpha_can_create_ai_universe_entry_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ai_root = root / "ai"
            _write_ai_infra_universe(
                ai_root,
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "ALPH",
                        "company": "Alpha Compute",
                        "bfs_depth": "D3",
                        "module": "AI accelerator supply chain",
                        "current_pool": "P0",
                        "total_score": 90,
                        "evidence_state": "原文已证明: AI accelerator supplier",
                    }
                ],
            )
            db = root / "us.duckdb"
            con = duckdb.connect(str(db))
            try:
                con.execute(
                    """
                    CREATE TABLE options_chain_quotes (
                        symbol VARCHAR,
                        as_of DATE,
                        days_to_exp INTEGER,
                        current_price DOUBLE,
                        contract_symbol VARCHAR,
                        option_type VARCHAR,
                        strike DOUBLE,
                        volume BIGINT,
                        open_interest BIGINT,
                        implied_volatility DOUBLE,
                        gamma DOUBLE
                    )
                    """
                )
                rows = [
                    ("ALPH", "2026-05-28", 20, 99.0, "ALPHC100", "call", 100.0, 120, 5_000, 0.32, 0.05),
                    ("ALPH", "2026-05-28", 20, 99.0, "ALPHP95", "put", 95.0, 80, 1_500, 0.36, 0.02),
                    ("ALPH", "2026-05-29", 20, 101.0, "ALPHC100", "call", 100.0, 2_000, 12_000, 0.31, 0.05),
                    ("ALPH", "2026-05-29", 20, 101.0, "ALPHC105", "call", 105.0, 1_000, 4_000, 0.33, 0.03),
                    ("ALPH", "2026-05-29", 20, 101.0, "ALPHP95", "put", 95.0, 50, 1_000, 0.35, 0.02),
                ]
                con.executemany("INSERT INTO options_chain_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            finally:
                con.close()

            payload = ranker.build_ranker_payload(
                as_of=ranker.date(2026, 5, 29),
                candidates=[],
                candidate_status="unit",
                us_db=db,
                top=10,
                ai_infra_root=ai_root,
                ai_infra_mode="enforce_expand",
            )
            row = payload["all_rows"][0]
            self.assertEqual(row["symbol"], "ALPH")
            self.assertEqual(row["alpha_sleeve_id"], ranker.US_GAMMA_V2_ALPHA_SLEEVE)
            self.assertEqual(row["alpha_factory_role"], "gamma_v2_entry_alpha")
            self.assertEqual(row["production_action"], "buy_stock_with_gamma_v2_entry")
            self.assertTrue(row["gamma_v2_entry_signal"])
            self.assertGreater(row["gamma_v2_alpha_score"], 64.0)
            self.assertIn(row["gamma_v3_curve_state"], {"POSITIVE_GEX_PIN_ZONE", "ZERO_GAMMA_TRANSITION"})
            self.assertEqual(row["gamma_v3_curve_quality"], "bs_iv_repriced")
            self.assertIn("gamma_v3_flip_regime", row)

    def test_ai_infra_expand_uses_all_us_symbols_not_only_p0(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ai_root = Path(tmpdir)
            _write_ai_infra_universe(
                ai_root,
                [
                    # All three rows carry evidence_state so they clear the
                    # production-pool gate that enforce_expand now applies.
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D3",
                        "module": "800G optical",
                        "current_pool": "P0",
                        "total_score": 91,
                        "evidence_state": "原文已证明: datacom transceiver mix and AI exposure",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "CRDO",
                        "company": "Credo",
                        "bfs_depth": "D3",
                        "module": "scale-up connectivity",
                        "current_pool": "P0",
                        "total_score": 87,
                        "evidence_state": "原文已证明: AEC retimer revenue ramp",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "NBIS",
                        "company": "Nebius",
                        "bfs_depth": "D4",
                        "module": "NeoCloud",
                        "current_pool": "P1",
                        "total_score": 73,
                        "evidence_state": "合理推论: GPU cluster financing and customer pipeline",
                    },
                ],
            )

            payload = ranker.build_ranker_payload(
                as_of=ranker.date(2026, 5, 12),
                candidates=[{"symbol": "COHR"}, {"symbol": "ZZZ"}],
                candidate_status="unit",
                us_db=ai_root / "missing.duckdb",
                top=10,
                ai_infra_root=ai_root,
                ai_infra_mode="enforce_expand",
            )
            symbols = {row["symbol"] for row in payload["all_rows"]}

            self.assertEqual(symbols, {"COHR", "CRDO", "NBIS"})
            self.assertEqual(payload["ai_infra_gate"]["retained_candidate_count"], 1)
            self.assertEqual(payload["ai_infra_gate"]["added_universe_count"], 2)
            self.assertIn("ZZZ", payload["ai_infra_gate"]["excluded_symbols"])
            self.assertTrue(all(row.get("ai_infra_universe") for row in payload["all_rows"]))
            self.assertTrue(any(row.get("ai_infra_current_pool") == "P1" for row in payload["all_rows"]))
            self.assertTrue(
                all(row.get("ai_infra_universe") for row in payload.get("production_basket") or []),
                "production_basket must only contain ai_infra_universe members",
            )

    def test_broad_signal_shifts_rank_score_when_modules_present(self) -> None:
        # Same name evaluated three ways. broad_modules folds 20% of
        # analysis_daily momentum/breakout/MR into rank_score, so the
        # bullish day should rank higher than the bearish day, and both
        # should differ from the no-broad-modules baseline.
        base = {
            "symbol": "AMZN",
            "alpha_sleeve_id": "us_theme_cluster_momentum",
            "rr_ratio": 2.0,
            "expected_move_pct": 6.0,
            "flow_options_quality": 70.0,
            "supercycle_priority": 1,
            "ai_evidence_score": 0.8,
        }
        baseline = ranker.score_rows([dict(base)])[0]
        bull = ranker.score_rows([{**base, "broad_modules": {
            "momentum_risk": {"p_upside": 0.75},
            "breakout": {"breakout_score": 0.6},
            "mean_reversion": {"reversion_score": 0.4, "reversion_direction": "bullish"},
        }}])[0]
        bear = ranker.score_rows([{**base, "broad_modules": {
            "momentum_risk": {"p_upside": 0.20},
            "breakout": {"breakout_score": 0.1},
            "mean_reversion": {"reversion_score": 0.8, "reversion_direction": "bearish"},
        }}])[0]
        # No broad_modules → behaves like before (no broad_signal key).
        self.assertNotIn("broad_signal", baseline["score_components"])
        # With modules → score_components shows broad_signal + breakdown.
        self.assertIn("broad_signal", bull["score_components"])
        self.assertIn("broad_signal_breakdown", bull["score_components"])
        # Bull > Bear on rank_score, by a meaningful margin.
        self.assertGreater(bull["rank_score"], bear["rank_score"] + 5.0)

    def test_supercycle_priority_is_scored_and_public(self) -> None:
        rows = ranker.score_rows(
            [
                {
                    "symbol": "NVDA",
                    "alpha_sleeve_id": "us_theme_cluster_momentum",
                    "rr_ratio": 2.0,
                    "expected_move_pct": 6.0,
                    "signal_confidence": "MODERATE",
                    "flow_options_quality": 70.0,
                    "supercycle_layer": "ai_compute_accelerators",
                    "supercycle_priority": 1,
                    "supply_chain_role": "accelerator and AI server ecosystem",
                    "bottleneck_focus": "compute capacity",
                    "ai_evidence_score": 0.8,
                    "supplier_evidence_state": "source_linked_supply_evidence",
                }
            ]
        )
        public = ranker.public_row(rows[0])
        self.assertEqual(rows[0]["score_components"]["supercycle_priority"], 100.0)
        self.assertEqual(rows[0]["score_components"]["ai_evidence"], 80.0)
        self.assertEqual(public["supercycle_layer"], "ai_compute_accelerators")
        self.assertEqual(public["supercycle_priority"], 1)
        self.assertEqual(public["supply_chain_role"], "accelerator and AI server ecosystem")
        self.assertEqual(public["supplier_evidence_state"], "source_linked_supply_evidence")

    def test_ai_news_evidence_requires_company_alias(self) -> None:
        evidence = ranker.ai_news_evidence(
            [
                {
                    "headline": "Seagate is sold out through 2027 as AI reshapes hard drive demand",
                    "summary": "The story is about a competitor, not the target company.",
                    "source": "Yahoo",
                    "published_at": "2026-05-11T12:00:00",
                }
            ],
            "ai_memory_storage",
            "WDC",
        )
        self.assertEqual(evidence["supplier_evidence_state"], "needs_primary_confirmation")

        matched = ranker.ai_news_evidence(
            [
                {
                    "headline": "Western Digital sees AI-driven HDD demand surge",
                    "summary": "Western Digital discusses cloud customer demand and capacity.",
                    "source": "Yahoo",
                    "published_at": "2026-05-11T12:00:00",
                }
            ],
            "ai_memory_storage",
            "WDC",
        )
        self.assertEqual(matched["supplier_evidence_state"], "source_linked_supply_evidence")

    def test_headline_risk_requires_company_alias_for_display_and_risk(self) -> None:
        result = ranker.headline_risk(
            [
                {
                    "headline": "Arista Networks: Bridging the Gap To $200",
                    "summary": "Arista is not the target ticker.",
                    "published_at": "2026-05-11T15:52:20",
                },
                {
                    "headline": "Stifel is Bullish on Coherent Corp. (COHR)",
                    "summary": "The note references Ciena as part of the optical equipment basket.",
                    "published_at": "2026-05-11T13:24:44",
                },
                {
                    "headline": "Did Fresh Analyst Coverage and AI Optics Buzz Shift Ciena's Narrative?",
                    "summary": "Ciena demand is tied to AI optical networking.",
                    "published_at": "2026-05-08T13:16:19",
                },
            ],
            ranker.NewsRiskConfig(),
            "CIEN",
        )
        self.assertIn("Ciena", result["latest_headline"])

        unrelated_negative = ranker.headline_risk(
            [
                {
                    "headline": "Competitor faces lawsuit after customer cancellations",
                    "summary": "No target company alias is present.",
                    "published_at": "2026-05-11T12:00:00",
                }
            ],
            ranker.NewsRiskConfig(),
            "CIEN",
        )
        self.assertEqual(unrelated_negative["headline_risk"], 0.0)
        self.assertEqual(unrelated_negative["latest_headline"], "")

    def test_ai_news_evidence_uses_word_boundaries_for_negative_terms(self) -> None:
        evidence = ranker.ai_news_evidence(
            [
                {
                    "headline": "Western Digital executives highlight AI capacity demand",
                    "summary": "Western Digital says cloud customer demand and storage capacity remain strong.",
                    "source": "Newswire",
                    "published_at": "2026-05-11T12:00:00",
                }
            ],
            "ai_memory_storage",
            "WDC",
        )
        self.assertEqual(evidence["supplier_evidence_state"], "source_linked_supply_evidence")
        self.assertNotIn("negative:cut", evidence["ai_evidence_hits"])

    def test_ai_news_evidence_treats_cancelled_orders_as_negative_supply(self) -> None:
        evidence = ranker.ai_news_evidence(
            [
                {
                    "headline": "POET says Marvell cancelled all Celestial AI-related purchase orders",
                    "summary": "POET disclosed a customer dispute after AI networking orders were cancelled.",
                    "source": "Newswire",
                    "published_at": "2026-05-11T12:00:00",
                }
            ],
            "ai_networking_optical_cpo",
            "POET",
        )
        self.assertEqual(evidence["supplier_evidence_state"], "negative_supply_evidence")
        self.assertLess(evidence["ai_evidence_score"], 0.30)
        self.assertIn("negative:cancelled", evidence["ai_evidence_hits"])

    def test_negative_supply_evidence_cannot_receive_production_r(self) -> None:
        rows = ranker.score_rows(
            [
                {
                    "symbol": "POET",
                    "alpha_sleeve_id": "us_theme_cluster_momentum",
                    "rr_ratio": 2.0,
                    "expected_move_pct": 8.0,
                    "signal_confidence": "HIGH",
                    "flow_options_quality": 80.0,
                    "supercycle_layer": "ai_networking_optical_cpo",
                    "supercycle_priority": 1,
                    "ai_evidence_score": 0.08,
                    "supplier_evidence_state": "negative_supply_evidence",
                }
            ]
        )
        self.assertEqual(rows[0]["production_tier"], "event_risk_watch")
        self.assertEqual(rows[0]["production_action"], "negative_supply_no_trade")


if __name__ == "__main__":
    unittest.main()
