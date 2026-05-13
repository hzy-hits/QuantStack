from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def load_module():
    path = ROOT / "scripts" / "send_production_decision_report.py"
    spec = importlib.util.spec_from_file_location("send_production_decision_report", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MarketReportScopeTest(unittest.TestCase):
    def test_us_report_rejects_cn_content(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "us.md"
            path.write_text("# 美股量化日报 - 2026-05-11\n\n| CN | 002185.SZ |\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "cross-market content"):
                module.validate_market_report_scope(path, "us")

    def test_us_report_accepts_clean_us_content(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "us.md"
            path.write_text("# 美股量化日报 - 2026-05-11\n\n| US | COHR |\n", encoding="utf-8")
            module.validate_market_report_scope(path, "us")

    def test_prod_delivery_rejects_combined_report(self) -> None:
        module = load_module()
        argv = [
            "send_production_decision_report.py",
            "--date",
            "2026-05-11",
            "--market",
            "all",
            "--delivery-mode",
            "prod",
            "--delivery-dry-run",
        ]
        with mock.patch("sys.argv", argv), self.assertRaises(SystemExit) as cm:
            module.main()
        self.assertIn("prod delivery requires --market cn or --market us", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
