from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from lib.hedge import hedged_return_r  # noqa: E402
from sleeves.cn_tape_leadership import causal_news_score  # noqa: E402
from sleeves.portfolio_hedge import daily_rows_from_ledger  # noqa: E402
from sleeves.promotions import UnpromotedSleeveError, assert_sleeve_promoted  # noqa: E402
from sleeves.us_theme_cluster import is_promotable_theme_basket, load_us_theme_seed_map  # noqa: E402


class PhaseGuardrailTests(unittest.TestCase):
    def test_future_news_does_not_enter_candidate_score(self) -> None:
        score = causal_news_score(
            [
                {"published_at": "2026-03-15T16:01:00", "sentiment": "positive"},
                {"published_at": "2026-03-15T14:30:00", "sentiment": "negative"},
            ],
            "2026-03-15T15:00:00",
        )
        self.assertEqual(score, -1.0)

    def test_hedge_sign_market_up_and_down(self) -> None:
        up = hedged_return_r(long_ret_pct=5.0, benchmark_ret_pct=5.0, beta_value=1.0, hedge_ratio=0.5)
        self.assertAlmostEqual(up, 0.025)
        down_unhedged = -0.05
        down = hedged_return_r(long_ret_pct=-5.0, benchmark_ret_pct=-5.0, beta_value=1.0, hedge_ratio=0.5)
        self.assertGreater(down, down_unhedged)
        self.assertAlmostEqual(down, -0.025)

    def test_single_name_fake_theme_cannot_promote(self) -> None:
        self.assertFalse(is_promotable_theme_basket(["RKLB"], {"RKLB": 12.0}))
        self.assertTrue(is_promotable_theme_basket(["RKLB", "LUNR", "ASTS"], {"RKLB": 5.0, "LUNR": 1.0, "ASTS": -0.5}))

    def test_us_theme_seed_map_preserves_supercycle_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "themes.yaml"
            path.write_text(
                """
version: 2
themes:
  - theme_id: ai_networking_optical_cpo
    label: AI networking
    benchmark: QQQ
    inception_date: 2026-03-01
    supercycle_layer: ai_networking_optical_cpo
    supercycle_priority: 1
    supply_chain_role: optical module
    bottleneck_focus: 800G optics
    evidence_contract: confirm design-win evidence
    research_index: optical bottleneck monitor
    members: [COHR, LITE, ANET]
""",
                encoding="utf-8",
            )
            theme = load_us_theme_seed_map(path)[0]
            self.assertEqual(theme["supercycle_layer"], "ai_networking_optical_cpo")
            self.assertEqual(theme["supercycle_priority"], 1)
            self.assertEqual(theme["supply_chain_role"], "optical module")
            self.assertEqual(theme["bottleneck_focus"], "800G optics")

    def test_ledger_daily_aggregate_matches_ledger_sum(self) -> None:
        ledger = [
            {
                "return_date": "2026-05-01",
                "market": "US",
                "sleeve_id": "a",
                "long_return_r": 0.05,
                "beta_hedge_return_r": 0.02,
                "hedge_cost_r": 0.0,
                "net_return_r": 0.03,
                "gross_long_r": 1.0,
                "hedge_notional_r": 0.5,
                "net_beta_r": 0.5,
            },
            {
                "return_date": "2026-05-01",
                "market": "CN",
                "sleeve_id": "b",
                "long_return_r": -0.01,
                "beta_hedge_return_r": -0.02,
                "hedge_cost_r": 0.0,
                "net_return_r": 0.01,
                "gross_long_r": 1.0,
                "hedge_notional_r": 0.7,
                "net_beta_r": 0.3,
            },
        ]
        daily = daily_rows_from_ledger(ledger, date(2026, 5, 8))
        global_row = next(row for row in daily if row["market"] == "ALL" and row["sleeve_id"] == "ALL")
        self.assertAlmostEqual(global_row["net_return_r"], 0.04)
        self.assertAlmostEqual(global_row["long_return_r"], 0.04)
        self.assertAlmostEqual(global_row["beta_hedge_return_r"], 0.0)

    def test_unpromoted_sleeve_cannot_receive_r(self) -> None:
        promoted_rows = [{"market": "us", "sleeve_id": "us_theme_cluster_momentum", "status": "watch"}]
        with self.assertRaises(UnpromotedSleeveError):
            assert_sleeve_promoted(market="us", sleeve_id="us_theme_cluster_momentum", promoted_rows=promoted_rows)
        assert_sleeve_promoted(
            market="us",
            sleeve_id="us_theme_cluster_momentum",
            promoted_rows=[{"market": "us", "sleeve_id": "us_theme_cluster_momentum", "status": "promoted"}],
        )


if __name__ == "__main__":
    unittest.main()
