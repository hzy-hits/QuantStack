from __future__ import annotations

import csv
import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.autoresearch.ai_infra_context import (
    apply_ai_infra_filter,
    build_ai_infra_session_context,
    market_symbols,
)
from src.mining.daily_pipeline import _apply_factor_lab_universe
from src.mining.export_to_pipeline import _apply_export_universe


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class AiInfraContextTests(unittest.TestCase):
    def test_market_symbols_include_full_us_pool_and_adr_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_jsonl(
                root / "data" / "global_universe_v2.jsonl",
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D2-D3",
                        "module": "800G optics",
                        "current_pool": "候选池",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "NBIS",
                        "company": "Nebius",
                        "bfs_depth": "D1-D4",
                        "module": "NeoCloud",
                        "current_pool": "雷达/候选池",
                    },
                    {
                        "asset_pool": "卫星资产池",
                        "market_country": "台湾",
                        "ticker": "2330.TW / TSM",
                        "company": "TSMC",
                        "bfs_depth": "D2",
                        "module": "CoWoS foundry",
                        "current_pool": "核心池",
                    },
                    {
                        "asset_pool": "中国资产池",
                        "market_country": "A股主板",
                        "ticker": "600000.SH",
                        "company": "Unit CN",
                        "bfs_depth": "D3",
                        "module": "Power grid",
                        "current_pool": "候选池",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "WRAP",
                        "company": "Excluded Wrapper",
                        "bfs_depth": "D4",
                        "module": "AI wrapper",
                        "current_pool": "排除池",
                    },
                ],
            )

            self.assertEqual(market_symbols("us", root), {"COHR", "NBIS", "TSM"})
            self.assertEqual(market_symbols("cn", root), {"600000.SH"})

    def test_session_context_says_queue_is_not_full_universe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_jsonl(
                root / "data" / "global_universe_v2.jsonl",
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D2-D3",
                        "module": "800G optics",
                        "current_pool": "候选池",
                    },
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "NBIS",
                        "company": "Nebius",
                        "bfs_depth": "D1-D4",
                        "module": "NeoCloud",
                        "current_pool": "雷达/候选池",
                    },
                ],
            )
            _write_csv(
                root / "reports" / "us_alpha_mining_queue_v1.csv",
                [
                    {"priority": "P0_us_alpha", "ticker": "COHR", "cluster": "optics_connectivity"},
                ],
            )

            context = build_ai_infra_session_context("us", root)

            self.assertIn("AI Infra Universe Context", context)
            self.assertIn("2 US tradable symbols", context)
            self.assertIn("US alpha queue is the first review queue, not the full universe", context)
            self.assertIn("NBIS", context)
            self.assertIn("filings/transcripts/news", context)
            self.assertIn("long alpha return minus beta hedge return", context)

    def test_apply_filter_respects_env_toggle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_jsonl(
                root / "data" / "global_universe_v2.jsonl",
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D2-D3",
                        "module": "800G optics",
                        "current_pool": "候选池",
                    }
                ],
            )
            df = pd.DataFrame({"symbol": ["COHR", "SPY"], "close": [1.0, 2.0]})

            old = os.environ.get("FACTOR_LAB_AI_INFRA_ONLY")
            try:
                os.environ["FACTOR_LAB_AI_INFRA_ONLY"] = "1"
                filtered = apply_ai_infra_filter(df, market="us", symbol_col="symbol", root=root)
                self.assertEqual(filtered["symbol"].tolist(), ["COHR"])

                os.environ["FACTOR_LAB_AI_INFRA_ONLY"] = "0"
                unfiltered = apply_ai_infra_filter(df, market="us", symbol_col="symbol", root=root)
                self.assertEqual(unfiltered["symbol"].tolist(), ["COHR", "SPY"])
            finally:
                if old is None:
                    os.environ.pop("FACTOR_LAB_AI_INFRA_ONLY", None)
                else:
                    os.environ["FACTOR_LAB_AI_INFRA_ONLY"] = old

    def test_daily_and_export_pipelines_use_ai_infra_universe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_jsonl(
                root / "data" / "global_universe_v2.jsonl",
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "COHR",
                        "company": "Coherent",
                        "bfs_depth": "D2-D3",
                        "module": "800G optics",
                        "current_pool": "候选池",
                    }
                ],
            )
            prices = pd.DataFrame(
                {
                    "ts_code": ["COHR", "SPY"],
                    "trade_date": ["2026-05-12", "2026-05-12"],
                    "close": [1.0, 2.0],
                    "market_cap": [1.0, 999.0],
                }
            )
            old_root = os.environ.get("FACTOR_LAB_AI_INFRA_ROOT")
            old_only = os.environ.get("FACTOR_LAB_AI_INFRA_ONLY")
            try:
                os.environ["FACTOR_LAB_AI_INFRA_ROOT"] = str(root)
                os.environ["FACTOR_LAB_AI_INFRA_ONLY"] = "1"
                mined = _apply_factor_lab_universe("us", prices, {"universe_top_n": 1})
                exported = _apply_export_universe("us", prices, {"sym_col": "ts_code", "universe_top_n": 1})

                self.assertEqual(mined["ts_code"].tolist(), ["COHR"])
                self.assertEqual(exported["ts_code"].tolist(), ["COHR"])
            finally:
                if old_root is None:
                    os.environ.pop("FACTOR_LAB_AI_INFRA_ROOT", None)
                else:
                    os.environ["FACTOR_LAB_AI_INFRA_ROOT"] = old_root
                if old_only is None:
                    os.environ.pop("FACTOR_LAB_AI_INFRA_ONLY", None)
                else:
                    os.environ["FACTOR_LAB_AI_INFRA_ONLY"] = old_only


if __name__ == "__main__":
    unittest.main()
