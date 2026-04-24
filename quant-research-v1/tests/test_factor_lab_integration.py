from __future__ import annotations

import importlib.util
import sys
import textwrap
import tempfile
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

    def test_build_contexts_prefers_session_specific_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir)
            out_dir = reports_dir / "out"
            date = "2026-04-14"

            (reports_dir / f"{date}_payload_macro_pre.md").write_text(
                "# Macro\n\npre-session macro\n",
                encoding="utf-8",
            )
            (reports_dir / f"{date}_payload_structural_pre.md").write_text(
                "# Structural\n\n### 1. AAA [x]\nlane: CORE BOOK\nsignal: **HIGH**\n",
                encoding="utf-8",
            )
            (reports_dir / f"{date}_payload_news_pre.md").write_text(
                "# News\n\n### 1. AAA [x]\n**Recent News:**\nheadline\n",
                encoding="utf-8",
            )
            (reports_dir / f"{date}_payload_macro.md").write_text(
                "# Macro\n\nlegacy macro should not win\n",
                encoding="utf-8",
            )

            build_agent_context.build_contexts(reports_dir, date, "pre", out_dir)

            macro_text = (out_dir / "macro.md").read_text(encoding="utf-8")
            self.assertIn("pre-session macro", macro_text)
            self.assertNotIn("legacy macro should not win", macro_text)

    def test_compact_items_prioritizes_tactical_continuation_before_event_tape(self) -> None:
        structural_text = textwrap.dedent(
            """
            # Structural Payload

            ### 1. AAA
            lane: CORE BOOK
            signal: **HIGH**

            ### 2. BBB
            lane: TACTICAL CONTINUATION
            signal: **MODERATE**

            ### 3. CCC
            lane: TACTICAL EVENT TAPE
            signal: **HIGH**
            """
        ).strip()

        compact, selected = build_agent_context._compact_items(
            structural_text,
            max_items=2,
            title="结构信号",
        )

        self.assertEqual(selected, ["AAA", "BBB"])
        self.assertIn("TACTICAL_CONTINUATION 1", compact)
        self.assertNotIn("### 3. CCC", compact)


class TacticalContinuationSelectionTests(unittest.TestCase):
    def test_selection_metadata_creates_tactical_continuation_lane(self) -> None:
        from quant_bot.filtering.notable import _selection_metadata

        item = {
            "symbol": "QBTS",
            "score": 0.61,
            "price": 18.5,
            "avg_dollar_volume_20d": 55_000_000.0,
            "fundamentals": {"market_cap_musd": 3_800.0},
            "options": {"liquidity_score": "good"},
            "lab_factor": {"is_confirming": True},
            "sub_scores": {"event": 0.22, "magnitude": 0.71},
            "ret_5d_pct": 17.4,
            "execution_gate": {
                "action": "wait_pullback",
                "gap_pct": 4.2,
                "p_continue": 0.66,
                "support_score": 0.64,
                "effective_stretch_score": 0.49,
                "regime": "continue",
                "trend_regime": "trending",
            },
        }

        selection = _selection_metadata(
            item,
            core_symbols=set(),
            selection_policy={
                "core_min_market_cap_musd": 2_000.0,
                "core_min_price": 5.0,
                "core_min_dollar_volume_20d": 20_000_000.0,
            },
        )

        self.assertEqual(selection["lane"], "tactical_continuation")
        self.assertTrue(selection["tactical_continuation"])

    def test_selection_metadata_keeps_pullback_valid_mean_reversion_name(self) -> None:
        from quant_bot.filtering.notable import _selection_metadata

        item = {
            "symbol": "POOL",
            "score": 0.57,
            "price": 26.5,
            "avg_dollar_volume_20d": 42_000_000.0,
            "fundamentals": {"market_cap_musd": 2_900.0},
            "options": {"liquidity_score": "fair"},
            "lab_factor": {"is_confirming": False},
            "sub_scores": {"event": 0.18, "magnitude": 0.63},
            "ret_1d_pct": 1.4,
            "ret_5d_pct": 8.9,
            "mean_reversion": {
                "reversion_score": 0.62,
                "reversion_direction": 1.0,
            },
            "execution_gate": {
                "action": "wait_pullback",
                "gap_pct": 1.1,
                "p_continue": 0.52,
                "support_score": 0.60,
                "effective_stretch_score": 0.79,
                "regime": "fade",
                "trend_regime": "mean_reverting",
                "max_chase_gap_pct": 2.6,
                "pullback_price": 25.7,
            },
        }

        selection = _selection_metadata(
            item,
            core_symbols=set(),
            selection_policy={
                "core_min_market_cap_musd": 2_000.0,
                "core_min_price": 5.0,
                "core_min_dollar_volume_20d": 20_000_000.0,
            },
        )

        self.assertEqual(selection["lane"], "tactical_continuation")
        self.assertTrue(selection["tactical_continuation"])

    def test_selection_metadata_does_not_promote_weak_pullback_reset(self) -> None:
        from quant_bot.filtering.notable import _selection_metadata

        item = {
            "symbol": "SNAP",
            "score": 0.54,
            "price": 18.2,
            "avg_dollar_volume_20d": 48_000_000.0,
            "fundamentals": {"market_cap_musd": 3_400.0},
            "options": {"liquidity_score": "fair"},
            "lab_factor": {"is_confirming": False},
            "sub_scores": {"event": 0.22, "magnitude": 0.59},
            "ret_1d_pct": 0.7,
            "ret_5d_pct": 7.1,
            "mean_reversion": {
                "reversion_score": 0.44,
                "reversion_direction": 1.0,
            },
            "execution_gate": {
                "action": "wait_pullback",
                "gap_pct": 0.8,
                "p_continue": 0.48,
                "support_score": 0.50,
                "effective_stretch_score": 0.81,
                "regime": "fade",
                "trend_regime": "mean_reverting",
                "max_chase_gap_pct": 2.2,
                "pullback_price": 17.9,
            },
        }

        selection = _selection_metadata(
            item,
            core_symbols=set(),
            selection_policy={
                "core_min_market_cap_musd": 2_000.0,
                "core_min_price": 5.0,
                "core_min_dollar_volume_20d": 20_000_000.0,
            },
        )

        self.assertNotEqual(selection["lane"], "tactical_continuation")
        self.assertFalse(selection["tactical_continuation"])


class RenderPayloadTests(unittest.TestCase):
    def test_render_notable_items_includes_tactical_continuation_bucket(self) -> None:
        from quant_bot.reporting.render import _render_notable_items

        bundle = {
            "notable_items": [
                {
                    "symbol": "AAA",
                    "report_bucket": "core",
                    "report_score": 0.92,
                    "score": 0.92,
                    "primary_reason": "core setup",
                    "sub_scores": {},
                    "signal": {
                        "confidence": "LOW",
                        "direction": "bullish",
                        "signal_type": "trend",
                        "direction_score": 0.41,
                    },
                },
                {
                    "symbol": "BBB",
                    "report_bucket": "tactical_continuation",
                    "report_score": 0.71,
                    "score": 0.71,
                    "primary_reason": "continuation still intact",
                    "sub_scores": {},
                    "signal": {
                        "confidence": "LOW",
                        "direction": "bullish",
                        "signal_type": "trend",
                        "direction_score": 0.33,
                    },
                },
                {
                    "symbol": "CCC",
                    "report_bucket": "event_tape",
                    "report_score": 0.61,
                    "score": 0.61,
                    "primary_reason": "event burst",
                    "sub_scores": {},
                    "signal": {
                        "confidence": "LOW",
                        "direction": "bullish",
                        "signal_type": "event",
                        "direction_score": 0.22,
                    },
                },
            ]
        }

        rendered = "\n".join(_render_notable_items(bundle))

        self.assertIn("### Tactical Continuation", rendered)
        self.assertLess(
            rendered.index("### Tactical Continuation"),
            rendered.index("### Tactical Event Tape"),
        )
        self.assertIn("BBB", rendered)

    def test_nontrend_headline_gate_suppresses_order_shaped_risk_params(self) -> None:
        from quant_bot.reporting.render import _render_notable_items

        bundle = {
            "headline_gate": {"mode": "uncertain"},
            "notable_items": [
                {
                    "symbol": "AAA",
                    "report_bucket": "core",
                    "report_score": 0.92,
                    "score": 0.92,
                    "price": 100.0,
                    "primary_reason": "core setup",
                    "sub_scores": {},
                    "signal": {
                        "confidence": "HIGH",
                        "direction": "bullish",
                        "signal_type": "trend",
                        "direction_score": 0.41,
                    },
                    "execution_gate": {
                        "action": "executable_now",
                        "gap_vs_expected_move": 0.12,
                        "pullback_price": 98.0,
                    },
                    "risk_params": {
                        "entry": 100.0,
                        "stop": 95.0,
                        "target": 112.0,
                        "rr_ratio": 2.4,
                        "execution_mode": "executable_now",
                    },
                }
            ],
        }

        rendered = "\n".join(_render_notable_items(bundle))

        self.assertIn("Execution guard", rendered)
        self.assertIn("Do not turn any lane below into a buy list", rendered)
        self.assertIn("observation only", rendered)
        self.assertNotIn("| Entry |", rendered)
        self.assertNotIn("| Stop (2-ATR) |", rendered)
        self.assertNotIn("| Target |", rendered)
        self.assertNotIn("still actionable at current levels", rendered)


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

            状态: FRESH。策略输出交易日 2026-04-20，可作为独立实验附录展示，但不得覆盖主系统结论。

            数据截止: 2026-04-20 (请求日期 2026-04-21 无更新交易数据)

            依据: d2_3_516

            怎么操作:
              1. 明天开盘买入下面 2 只
              2. 持有 4 个交易日 (到 ~2026-04-26 全卖)

              # 代码           名称              买入价       止损       止盈     仓位
            ──────────────────────────────────────────────────────────────
              1 AAA          ALPHA         10.00     9.00    12.00  18.2%
              2 BBB          BETA          20.00    18.00    24.00  16.4%

            数据清洗: 已剔除 3 个异常候选
            """
        ).strip()

        synced = sync_factor_lab_signal_section(report_text, structural_text)

        self.assertIn("**Factor Lab 选股**", synced)
        self.assertIn("| `AAA` | ALPHA | 10.00 | 9.00 | 12.00 | 18.2% | 强度#1 |", synced)
        self.assertIn("| `BBB` | BETA | 20.00 | 18.00 | 24.00 | 16.4% | 强度#2 |", synced)
        self.assertIn("当前因子：`d2_3_516`。", synced)
        self.assertIn("数据清洗: 已剔除 3 个异常候选", synced)
        self.assertNotIn("怎么操作:", synced)
        self.assertNotIn("本期为因子研究实验报告，非个股选股清单。", synced)

    def test_unavailable_factor_lab_block_does_not_render_candidate_table(self) -> None:
        from quant_bot.reporting.factor_lab import sync_factor_lab_signal_section

        report_text = textwrap.dedent(
            """
            # 市场日报 — 2026-04-23

            **Factor Lab 选股**

            旧内容

            ---

            **接下来看什么**
            """
        ).strip()
        structural_text = textwrap.dedent(
            """
            ## Factor Lab Research Candidates

            状态: UNAVAILABLE。候选输出失败或缺少交易日信息，忽略其方向性结论。

            怎么操作:
              1. 明天开盘买入下面 2 只

              # 代码           名称              买入价       止损       止盈     仓位
              1 AAA          ALPHA         10.00     9.00    12.00  18.2%
            """
        ).strip()

        synced = sync_factor_lab_signal_section(report_text, structural_text)

        self.assertIn("状态: UNAVAILABLE", synced)
        self.assertIn("本期不展示 Factor Lab 候选表", synced)
        self.assertNotIn("| `AAA` |", synced)
        self.assertNotIn("怎么操作:", synced)


class RunFullOrderingTests(unittest.TestCase):
    def test_run_full_refreshes_us_factor_lab_before_import(self) -> None:
        run_full_text = RUN_FULL_PATH.read_text(encoding="utf-8")
        refresh_marker = 'bash scripts/daily_factors.sh --market us'
        import_marker = 'src.mining.export_to_pipeline --market us --date "$DATE"'

        self.assertIn(refresh_marker, run_full_text)
        self.assertIn(import_marker, run_full_text)
        self.assertLess(run_full_text.index(refresh_marker), run_full_text.index(import_marker))

    def test_run_full_passes_session_to_run_daily_and_uses_session_payloads(self) -> None:
        run_full_text = RUN_FULL_PATH.read_text(encoding="utf-8")

        self.assertIn('python scripts/run_daily.py --date "$DATE" --session "$SESSION"', run_full_text)
        self.assertIn('PAYLOAD="reports/${DATE}_payload_${SESSION}.md"', run_full_text)
        self.assertIn('python scripts/split_payload.py --date "$DATE" --session "$SESSION"', run_full_text)


if __name__ == "__main__":
    unittest.main()
