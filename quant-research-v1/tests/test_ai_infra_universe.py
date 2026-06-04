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


    def test_production_pool_filters_to_evidence_confirmed_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "NVDA",
                        "company": "NVIDIA",
                        "bfs_depth": "D1",
                        "module": "GPU",
                        "current_pool": "核心池",
                        "evidence_state": "原文已证明: data center segment strong",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "MRVL",
                        "company": "Marvell",
                        "bfs_depth": "D1-D3",
                        "module": "Custom silicon",
                        "current_pool": "候选池",
                        # Pure 合理推论 head, no pending flag → production.
                        "evidence_state": "合理推论: AI revenue mix is structural",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "AVGO",
                        "company": "Broadcom",
                        "bfs_depth": "D1-D2",
                        "module": "Custom ASIC",
                        "current_pool": "核心池",
                        # 合理推论 token present, but head still flags
                        # 待原文核验 → research-only (codex P0 case).
                        "evidence_state": "合理推论+待原文核验: ASIC mix",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "AMD",
                        "company": "AMD",
                        "bfs_depth": "D1",
                        "module": "GPU",
                        "current_pool": "核心/候选池",
                        "evidence_state": "原文需核验: Instinct segment metrics",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "FOO",
                        "company": "Foo",
                        "bfs_depth": "D5",
                        "module": "speculative",
                        "current_pool": "雷达池",
                        "evidence_state": "证据不足: unverified",
                    },
                ],
            )

            research = universe.records_by_symbol("US", root, pool="research")
            production = universe.records_by_symbol("US", root, pool="production")

            self.assertEqual(set(research), {"NVDA", "MRVL", "AVGO", "AMD", "FOO"})
            # 原文已证明 (NVDA) and pure 合理推论 (MRVL) clear the gate.
            # AVGO's head still flags 待原文核验 → research-only.
            self.assertEqual(set(production), {"NVDA", "MRVL"})

            # merge_with_universe_candidates threads pool param into the gate.
            rows, gate = universe.merge_with_universe_candidates(
                [{"symbol": "AMD"}],
                market="US",
                ai_infra_root=root,
                include_all_universe=True,
                pool="production",
            )
            symbols = {row["symbol"] for row in rows}
            self.assertEqual(symbols, {"NVDA", "MRVL"})
            self.assertEqual(gate.pool, "production")
            # AMD candidate is dropped because it's not production-grade.
            self.assertIn("AMD", gate.excluded_symbols)

    def test_production_pool_rejects_unknown_pool_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_universe(root, [])
            with self.assertRaises(ValueError):
                universe.records_by_symbol("US", root, pool="bogus")

    def test_market_context_and_off_bfs_rows_do_not_enter_ai_universe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "HYG",
                        "company": "iShares iBoxx High Yield Corporate Bond ETF",
                        "bfs_depth": "D4",
                        "module": "credit context",
                        "current_pool": "候选池",
                        "evidence_state": "合理推论: credit conditions",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "BA",
                        "company": "Boeing",
                        "bfs_depth": "—(off-BFS)",
                        "module": "民航",
                        "current_pool": "候选池",
                        "evidence_state": "合理推论: policy rotation",
                        "counterevidence": "出 AI-infra mandate;不可进生产池;不生成 R",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D2-D3",
                        "module": "AI optics",
                        "current_pool": "候选池",
                        "evidence_state": "合理推论: datacom AI mix",
                    },
                ],
            )

            by_symbol = universe.records_by_symbol("US", root, pool="research")

            self.assertIn("COHR", by_symbol)
            self.assertNotIn("HYG", by_symbol)
            self.assertNotIn("BA", by_symbol)


if __name__ == "__main__":
    unittest.main()
