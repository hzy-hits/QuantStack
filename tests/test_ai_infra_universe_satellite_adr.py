"""Verify satellite ADR aliases land in the US AI Infra universe gate."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))

from quant_bot.analytics import ai_infra_universe as universe  # noqa: E402


def _write_universe(root: Path, rows: list[dict]) -> None:
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "global_universe_v2.jsonl"
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")


class SatelliteAdrAliasTests(unittest.TestCase):
    def test_asml_local_ticker_extracts_us_adr(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "卫星资产池",
                        "market_country": "欧洲",
                        "ticker": "ASML.AS",
                        "company": "ASML",
                        "bfs_depth": "D4",
                        "module": "EUV lithography",
                    }
                ],
            )
            us_records = universe.records_by_symbol("US", ai_infra_root=root)
            self.assertIn("ASML", us_records)
            self.assertEqual(us_records["ASML"]["company"], "ASML")

    def test_aliased_satellite_keeps_adr_token(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "卫星资产池",
                        "market_country": "台湾",
                        "ticker": "2330.TW / TSM",
                        "company": "TSMC",
                        "bfs_depth": "D2",
                    },
                    {
                        "asset_pool": "卫星资产池",
                        "market_country": "日本",
                        "ticker": "6857.T",  # no ADR alias and not in SATELLITE_US_ADRS → excluded.
                        "company": "Advantest",
                        "bfs_depth": "D3",
                    },
                ],
            )
            us_records = universe.records_by_symbol("US", ai_infra_root=root)
            self.assertIn("TSM", us_records)
            self.assertNotIn("6857.T", us_records)
            self.assertNotIn("ATEYY", us_records)  # OTC ADR intentionally excluded.

    def test_us_pool_records_unchanged(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_universe(
                root,
                [
                    {
                        "asset_pool": "美国资产池",
                        "market_country": "US",
                        "ticker": "NVDA",
                        "company": "NVIDIA",
                        "bfs_depth": "D1",
                    }
                ],
            )
            us_records = universe.records_by_symbol("US", ai_infra_root=root)
            self.assertIn("NVDA", us_records)


if __name__ == "__main__":
    unittest.main()
