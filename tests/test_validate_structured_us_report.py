from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "agents" / "run_us_narrator.py"


def _load():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    sys.path.insert(0, str(REPO_ROOT / "scripts" / "agents"))
    spec = importlib.util.spec_from_file_location("us_narrator_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


nar = _load()

_TABLE = "| 指标 | 值 |\n|---|---|\n| x | y |\n"
_PROD_TABLE = "| Symbol | Decision | Size |\n|---|---|---|\n| KLAC | 正式执行 | 0.0176R |\n"
AS_OF = "2026-06-10"


def _valid_report(prod_table: str = _TABLE) -> str:
    return "\n".join([
        f"# 美股量化日报 — {AS_OF}",
        "## 策略主线", "正文。",
        "## 市场结构", "US Realized Horizon Edge 历史持有周期复盘。", _TABLE,
        "## 交易计划", "Production candidates 正式执行表。", prod_table,
        "## 风险与反证", "IV/HV 与 Gamma v3 证据,Congressional 无 artifact。", _TABLE,
        "## 催化与复核", _TABLE,
        "## 附注", "不构成投资建议。",
    ])


class StructuredReportGuardTest(unittest.TestCase):
    def test_clean_report_passes(self) -> None:
        nar.validate_structured_us_report(_valid_report(), AS_OF, None)

    def test_extra_h2_section_rejected(self) -> None:
        text = _valid_report() + "\n## 今日概率最优\n临时段落。"
        with self.assertRaisesRegex(RuntimeError, "unexpected H2"):
            nar.validate_structured_us_report(text, AS_OF, None)

    def test_emoji_rejected(self) -> None:
        text = _valid_report().replace("正文。", "正文 ✓🎯。")
        with self.assertRaisesRegex(RuntimeError, "emoji"):
            nar.validate_structured_us_report(text, AS_OF, None)

    def test_internal_field_name_rejected(self) -> None:
        text = _valid_report().replace("正文。", "stable_alpha_gate 未放行,payload 显示……")
        with self.assertRaisesRegex(RuntimeError, "internal"):
            nar.validate_structured_us_report(text, AS_OF, None)

    def test_executed_symbol_listed_as_not_executed_rejected(self) -> None:
        text = _valid_report(prod_table=_PROD_TABLE) + \
            "\n\n**以下仅观察,不执行**:KLAC 突破后不追,等待回踩。\n"
        with self.assertRaisesRegex(RuntimeError, "仅观察/不执行"):
            nar.validate_structured_us_report(text, AS_OF, None)

    def test_observation_wording_for_non_executed_symbol_ok(self) -> None:
        text = _valid_report(prod_table=_PROD_TABLE) + \
            "\n\n**以下仅观察,不执行**:AVGO 失守 EMA50,等收复。\n"
        nar.validate_structured_us_report(text, AS_OF, None)

    def test_table_separator_column_mismatch_rejected(self) -> None:
        bad = "| 列1 | 列2 | 列3 |\n|---|---|\n| a | b | c |\n"
        text = _valid_report() + "\n" + bad
        with self.assertRaisesRegex(RuntimeError, "separator"):
            nar.validate_structured_us_report(text, AS_OF, None)


if __name__ == "__main__":
    unittest.main()
