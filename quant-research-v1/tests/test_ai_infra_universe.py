from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = REPO_ROOT / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import ai_infra_universe as universe  # noqa: E402


def _write_universe(root: Path, rows: list[dict]) -> None:
    path = root / "data" / "global_universe_v2.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


class AiInfraUniverseTests(unittest.TestCase):
    def test_us_universe_keeps_us_rows_and_satellite_adr_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D3",
                        "module": "800G optical",
                        "current_pool": "P0",
                    },
                    {
                        "asset_pool": "海外卫星池",
                        "market_country": "TW",
                        "ticker": "2330.TW / TSM",
                        "company": "TSMC",
                        "bfs_depth": "D2-D3",
                        "module": "CoWoS foundry",
                        "current_pool": "卫星观察",
                    },
                    {
                        "asset_pool": "中国A股资产池",
                        "market_country": "CN",
                        "ticker": "300308.SZ",
                        "company": "中际旭创",
                        "bfs_depth": "D3",
                        "module": "CPO optical",
                        "current_pool": "候选池",
                    },
                ],
            )

            by_symbol = universe.records_by_symbol("US", root)

            self.assertIn("COHR", by_symbol)
            self.assertIn("TSM", by_symbol)
            self.assertNotIn("2330.TW", by_symbol)
            self.assertNotIn("300308.SZ", by_symbol)

    def test_merge_filters_non_ai_and_adds_every_market_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D3",
                        "module": "optical CPO",
                        "current_pool": "P0",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "NBIS",
                        "company": "Nebius",
                        "bfs_depth": "D4",
                        "module": "NeoCloud",
                        "current_pool": "P1",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "BAD",
                        "company": "Bad Excluded",
                        "bfs_depth": "D5",
                        "module": "none",
                        "current_pool": "排除池",
                    },
                ],
            )

            rows, gate = universe.merge_with_universe_candidates(
                [{"symbol": "COHR"}, {"symbol": "ZZZ"}],
                market="US",
                ai_infra_root=root,
                include_all_universe=True,
            )
            symbols = {row["symbol"] for row in rows}

            self.assertEqual(symbols, {"COHR", "NBIS"})
            self.assertEqual(gate.raw_candidate_count, 2)
            self.assertEqual(gate.retained_candidate_count, 1)
            self.assertEqual(gate.added_universe_count, 1)
            self.assertEqual(gate.excluded_symbols, ("ZZZ",))
            self.assertTrue(all(row["ai_infra_universe"] for row in rows))


if __name__ == "__main__":
    unittest.main()
