"""Tests for the CN AI-evidence verifier (Tushare fina_mainbz based)."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "verify_cn_ai_evidence.py"


def _load_module():
    if "verify_cn_ai_evidence" in sys.modules:
        return sys.modules["verify_cn_ai_evidence"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("verify_cn_ai_evidence", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SegmentClassifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_ai_direct_segments(self) -> None:
        for seg in ("算力", "数据中心产品", "智算中心", "高速光组件与光模块",
                    "IDC业务", "数据中心电源", "AI服务器"):
            self.assertEqual(self.m.classify_segment(seg), "direct", seg)

    def test_ai_adjacent_segments(self) -> None:
        for seg in ("服务器", "电源产品", "覆铜板", "云计算", "光通信",
                    "高速连接器", "存储设备"):
            self.assertEqual(self.m.classify_segment(seg), "adjacent", seg)

    def test_unrelated_segments(self) -> None:
        for seg in ("白酒", "中央空调", "冷冻冷藏", "房地产", "", None):
            self.assertEqual(self.m.classify_segment(seg), "none", str(seg))


class EvidenceDecisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_material_direct_segment_is_proven(self) -> None:
        # 「算力」40% of total → 原文已证明 with the real number.
        segs = [("算力", 4.0e8), ("其他", 6.0e8)]
        state, summary = self.m.decide_evidence("20251231", segs)
        self.assertTrue(state.startswith("原文已证明"))
        self.assertEqual(summary["verdict"], "原文已证明")
        self.assertIn("算力", state)
        self.assertIn("40%", state)

    def test_adjacent_material_is_reasonable_inference(self) -> None:
        # 「电源产品」33%, no direct segment → 合理推论 (head pure, passes gate).
        segs = [("电源产品", 3.3e8), ("工业自动化", 6.7e8)]
        state, summary = self.m.decide_evidence("20251231", segs)
        self.assertTrue(state.startswith("合理推论"))
        self.assertEqual(summary["verdict"], "合理推论")
        # Must NOT produce 合理推论+待原文核验 (that would fail the gate).
        self.assertNotIn("待原文核验", state)

    def test_no_ai_segment_is_pending(self) -> None:
        segs = [("中央空调", 5.0e8), ("冷冻冷藏", 5.0e8)]
        state, summary = self.m.decide_evidence("20251231", segs)
        self.assertTrue(state.startswith("待原文核验"))
        self.assertEqual(summary["verdict"], "待原文核验")

    def test_tiny_direct_segment_is_reasonable_not_proven(self) -> None:
        # direct segment present but only 3% → not material → 合理推论.
        segs = [("算力", 0.03e8), ("白酒", 0.97e8)]
        state, summary = self.m.decide_evidence("20251231", segs)
        self.assertEqual(summary["verdict"], "合理推论")

    def test_empty_segments_is_pending(self) -> None:
        state, summary = self.m.decide_evidence("20251231", [])
        self.assertTrue(state.startswith("待原文核验"))
        self.assertEqual(summary["verdict"], "待原文核验")


class RewritableGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_auto_stamp_and_defaults_are_rewritable(self) -> None:
        self.assertTrue(self.m._rewritable(""))
        self.assertTrue(self.m._rewritable("原文已证明: source-review ready_for_promotion 2026-05-15"))
        self.assertTrue(self.m._rewritable("待原文核验: HBM sold-out"))
        self.assertTrue(self.m._rewritable("原文需核验: segment metrics"))

    def test_verified_evidence_is_not_rewritten(self) -> None:
        # Once the verifier has written real evidence, a re-run must skip it.
        self.assertFalse(self.m._rewritable(
            "原文已证明: 20251231 主营分部「算力」收入6.2亿，AI直接分部占比19%"
        ))
        self.assertFalse(self.m._rewritable(
            "合理推论: 20251231 主营分部「电源产品」占比33%，AI 敞口需逐项拆分"
        ))


if __name__ == "__main__":
    unittest.main()
