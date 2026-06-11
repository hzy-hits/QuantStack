from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load():
    spec = importlib.util.spec_from_file_location("gen_msv2_rab_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gen = _load()


class ReportActionBacktestFallbackTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())

    def _write(self, day: str, payload: dict) -> None:
        d = self.root / day
        d.mkdir(parents=True)
        (d / "report_action_backtest_summary.json").write_text(
            json.dumps(payload), encoding="utf-8")

    def test_exact_date_preferred(self) -> None:
        self._write("2026-06-10", {"us": {"horizons": {"1": {"n": 167}}}})
        self._write("2026-06-11", {"us": {"horizons": {"1": {"n": 170}}}})
        with mock.patch.object(gen, "REPORT_ACTION_BACKTEST_ROOT", self.root):
            out = gen.load_report_action_backtest_summary("2026-06-11")
        self.assertEqual(out["us"]["horizons"]["1"]["n"], 170)
        self.assertEqual(out.get("source_as_of"), "2026-06-11")

    def test_morning_falls_back_to_latest_prior_artifact(self) -> None:
        # 早报在 12:25 的回测任务之前发车——必须回退到最近一份并标注来源日期
        self._write("2026-06-09", {"us": {"horizons": {"1": {"n": 150}}}})
        self._write("2026-06-10", {"us": {"horizons": {"1": {"n": 167}}}})
        with mock.patch.object(gen, "REPORT_ACTION_BACKTEST_ROOT", self.root):
            out = gen.load_report_action_backtest_summary("2026-06-11")
        self.assertIsNotNone(out)
        self.assertEqual(out["us"]["horizons"]["1"]["n"], 167)
        self.assertEqual(out.get("source_as_of"), "2026-06-10")

    def test_no_artifact_at_or_before_date_returns_none(self) -> None:
        self._write("2026-06-10", {"us": {}})
        with mock.patch.object(gen, "REPORT_ACTION_BACKTEST_ROOT", self.root):
            self.assertIsNone(gen.load_report_action_backtest_summary("2026-06-08"))


if __name__ == "__main__":
    unittest.main()
