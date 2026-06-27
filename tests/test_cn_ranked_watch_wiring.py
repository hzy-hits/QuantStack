"""Wiring tests: radar section is rendered by cn_daily and sliced by the CN narrator."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]


class CnRankedWatchWiringTests(unittest.TestCase):
    def test_cn_daily_imports_and_inserts_radar(self) -> None:
        src = (STACK_ROOT / "scripts" / "reports" / "cn_daily.py").read_text(encoding="utf-8")
        self.assertIn("from sections.cn_ranked_watch import render_cn_ranked_watch_radar_section", src)
        self.assertIn("render_cn_ranked_watch_radar_section(payload)", src)

    def test_narrator_structural_headers_include_radar_key(self) -> None:
        src = (STACK_ROOT / "scripts" / "agents" / "run_cn_narrator.py").read_text(encoding="utf-8")
        m = re.search(r"_STRUCTURAL_HEADERS\s*=\s*\[(.*?)\]", src, re.DOTALL)
        self.assertIsNotNone(m, "_STRUCTURAL_HEADERS list not found")
        self.assertIn("0R 观察雷达", m.group(1))


if __name__ == "__main__":
    unittest.main()
