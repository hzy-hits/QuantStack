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

_TABLE = "| A | B |\n|---|---|\n| x | y |\n"
AS_OF = "2026-06-10"


def _valid_report() -> str:
    return "\n".join([
        f"# 美股量化日报 — {AS_OF}",
        "## 策略主线", "正文。",
        "## 市场结构", "US Realized Horizon Edge 历史持有周期复盘。", _TABLE,
        "## 交易计划", "Production candidates 正式执行表。", _TABLE,
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


if __name__ == "__main__":
    unittest.main()
