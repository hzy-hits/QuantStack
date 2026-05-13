from __future__ import annotations

import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "ai_infra" / "scripts" / "generate_bfs_supply_chain_discovery_queue.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("generate_bfs_supply_chain_discovery_queue", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = _load_module()


class AiInfraBfsDiscoveryQueueTests(unittest.TestCase):
    def test_build_task_uses_original_name_for_us_and_chinese_name_for_a_share_seed(self) -> None:
        overrides = {"NVDA": "英伟达"}
        us_task = generator.build_task(
            {
                "ticker": "NVDA",
                "company": "NVIDIA",
                "market_country": "US",
                "asset_pool": "美国资产池",
                "bfs_depth": "D1",
                "module": "GPU/CUDA",
                "current_pool": "核心池",
            },
            1,
            overrides,
        )
        cn_task = generator.build_task(
            {
                "ticker": "002463.SZ",
                "company": "沪电股份",
                "market_country": "A股主板",
                "asset_pool": "中国资产池",
                "bfs_depth": "D3",
                "module": "PCB / substrate",
                "current_pool": "候选池",
            },
            2,
            overrides,
        )

        self.assertEqual(us_task["seed_company_zh"], "NVIDIA")
        self.assertEqual(cn_task["seed_company_zh"], "沪电股份")
        self.assertIn("NVIDIA", us_task["agent_prompt"])

    def test_write_csv_includes_seed_company_zh_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queue.csv"
            generator.write_csv(
                path,
                [
                    {
                        "task_id": "DISC-0001",
                        "priority": "P0_expand_now",
                        "theme": "compute_gpu_asic",
                        "region_bucket": "US",
                        "seed_ticker": "NVDA",
                        "seed_company": "NVIDIA",
                        "seed_company_zh": "NVIDIA",
                        "market_country": "US",
                        "asset_pool": "美国资产池",
                        "bfs_depth": "D1",
                        "module": "GPU",
                        "current_pool": "核心池",
                        "source_targets": "10-K",
                        "extraction_keywords": "GPU",
                        "expansion_goal": "find suppliers",
                        "agent_prompt": "Seed NVDA / NVIDIA",
                    }
                ],
            )
            with path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["seed_company_zh"], "NVIDIA")


if __name__ == "__main__":
    unittest.main()
