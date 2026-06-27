"""Tests for the CN 0R Ranked Watch radar section."""
from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    name = "sections.cn_ranked_watch"
    if name in sys.modules:
        return sys.modules[name]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    return importlib.import_module(name)


def _payload():
    return {
        "cn_opportunity_ranker": {
            "all_rows": [
                {"symbol": "688535.SH", "name": "科创甲", "production_tier": "active_watch",
                 "rank": 20, "rank_score": 70.7, "pct_chg": 1.2,
                 "ev_lcb80_pct": 0.5, "size_hint": "0R", "reason": "wait for price"},
                {"symbol": "688233.SH", "name": "科创乙", "production_tier": "active_watch",
                 "rank": 18, "rank_score": 70.87, "pct_chg": -2.0,
                 "ev_lcb80_pct": 0.3, "size_hint": "0R", "reason": "prepare"},
                {"symbol": "600519.SH", "name": "主板丙", "production_tier": "active_watch",
                 "rank": 15, "rank_score": 72.0, "pct_chg": 0.5,
                 "ev_lcb80_pct": 0.4, "size_hint": "0R", "reason": "watch"},
                {"symbol": "688019.SH", "name": "bench科创", "production_tier": "bench_ranked",
                 "rank": 50, "rank_score": 60.1},
                {"symbol": "600000.SH", "name": "可执行", "production_tier": "top_stock_trade",
                 "rank": 1, "rank_score": 80.0},
            ]
        }
    }


class CnRankedWatchRadarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_board_label(self) -> None:
        b = self.module.board_label
        self.assertEqual(b("688981.SH"), "科创板")
        self.assertEqual(b("300750.SZ"), "创业板")
        self.assertEqual(b("301236.SZ"), "创业板")
        self.assertEqual(b("600519.SH"), "主板")
        self.assertEqual(b("000001.SZ"), "主板")
        self.assertEqual(b("830799.BJ"), "北交所")
        self.assertEqual(b("920819.BJ"), "北交所")

    def test_only_active_watch_rows_rank_ascending(self) -> None:
        rows = self.module.cn_ranked_watch_rows(_payload())
        self.assertEqual([r["symbol"] for r in rows], ["600519.SH", "688233.SH", "688535.SH"])

    def test_render_flags_star_and_excludes_non_active_watch(self) -> None:
        md = "\n".join(self.module.render_cn_ranked_watch_radar_section(_payload()))
        self.assertIn("## 0R 观察雷达 (Ranked Watch)", md)
        self.assertIn("★科创板", md)        # 688 flagged
        self.assertIn("688233.SH", md)
        self.assertIn("688535.SH", md)
        self.assertIn("600519.SH", md)       # mainboard active_watch present
        self.assertNotIn("688019.SH", md)    # bench_ranked excluded
        self.assertNotIn("600000.SH", md)    # executable excluded

    def test_empty_pool_placeholder(self) -> None:
        md = "\n".join(self.module.render_cn_ranked_watch_radar_section(
            {"cn_opportunity_ranker": {"all_rows": []}}))
        self.assertIn("## 0R 观察雷达 (Ranked Watch)", md)
        self.assertIn("没有 active_watch 0R 候选", md)

    def test_missing_ranker_key_is_safe(self) -> None:
        md = "\n".join(self.module.render_cn_ranked_watch_radar_section({}))
        self.assertIn("没有 active_watch 0R 候选", md)


if __name__ == "__main__":
    unittest.main()
