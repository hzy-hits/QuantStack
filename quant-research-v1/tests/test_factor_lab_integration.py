from __future__ import annotations

import importlib.util
import sys
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
BUILD_AGENT_CONTEXT_PATH = REPO_ROOT / "scripts" / "build_agent_context.py"
RUN_FULL_PATH = REPO_ROOT / "scripts" / "run_full.sh"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build_agent_context = _load_module("build_agent_context", BUILD_AGENT_CONTEXT_PATH)


class BuildAgentContextTests(unittest.TestCase):
    def test_compact_items_preserves_factor_lab_trailing_block(self) -> None:
        structural_text = textwrap.dedent(
            """
            # Structural Payload

            ### 1. AAA
            lane: CORE BOOK
            signal: **HIGH**

            ### 2. BBB
            lane: CORE BOOK
            signal: **WATCH**

            ## Factor Lab Independent Trading Signal

            symbol list here
            """
        ).strip()

        compact, _ = build_agent_context._compact_items(
            structural_text,
            max_items=1,
            title="结构信号",
        )

        self.assertIn("### 1. AAA", compact)
        self.assertIn("## Factor Lab Independent Trading Signal", compact)
        self.assertIn("symbol list here", compact)


class FactorLabReportSyncTests(unittest.TestCase):
    def test_sync_replaces_placeholder_factor_lab_section_with_signal_block(self) -> None:
        from quant_bot.reporting.factor_lab import sync_factor_lab_signal_section

        report_text = textwrap.dedent(
            """
            # 市场日报 — 2026-04-01

            **Factor Lab 选股**

            本期为因子研究实验报告，非个股选股清单。

            ---

            **接下来看什么**

            未来3天关注宏观事件。
            """
        ).strip()

        structural_text = textwrap.dedent(
            """
            ## Factor Lab Independent Trading Signal

            以下是独立选股清单。

            AAA 10.0 9.0 12.0
            BBB 20.0 18.0 24.0
            """
        ).strip()

        synced = sync_factor_lab_signal_section(report_text, structural_text)

        self.assertIn("**Factor Lab 选股**", synced)
        self.assertIn("AAA 10.0 9.0 12.0", synced)
        self.assertIn("BBB 20.0 18.0 24.0", synced)
        self.assertNotIn("本期为因子研究实验报告，非个股选股清单。", synced)


class RunFullOrderingTests(unittest.TestCase):
    def test_run_full_refreshes_us_factor_lab_before_import(self) -> None:
        run_full_text = RUN_FULL_PATH.read_text(encoding="utf-8")
        refresh_marker = 'bash scripts/daily_factors.sh --market us'
        import_marker = 'src.mining.export_to_pipeline --market us --date "$DATE"'

        self.assertIn(refresh_marker, run_full_text)
        self.assertIn(import_marker, run_full_text)
        self.assertLess(run_full_text.index(refresh_marker), run_full_text.index(import_marker))


if __name__ == "__main__":
    unittest.main()
