from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "validate_main_strategy_v2_reports.py"


def _load_module():
    if "validate_main_strategy_v2_reports" in sys.modules:
        return sys.modules["validate_main_strategy_v2_reports"]
    spec = importlib.util.spec_from_file_location("validate_main_strategy_v2_reports", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_report(report_dir: Path, payload: dict, cn_report: str, us_report: str) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "main_strategy_v2_backtest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (report_dir / "cn_daily_report.md").write_text(cn_report, encoding="utf-8")
    (report_dir / "us_daily_report.md").write_text(us_report, encoding="utf-8")


class MainStrategyV2ReportValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def _clean_payload(self) -> dict:
        return {
            "as_of": "2026-05-25",
            "strategy_alpha_bulletin": {
                "ev_status": {"us": "passed"},
                "selected_policies": {"us": "us_theme_cluster_momentum"},
            },
            "us_market_data_status": {"stock_data_current": True, "latest_date": "2026-05-25"},
            "report_action_backtest_summary": {"by_mode_market": {"contract_gated:US": {}, "contract_gated:CN": {}}},
            "production_decision_summary": {
                "summary": {"us_r": 0.125, "us_action_count": 1},
                "actionable": [
                    {"market": "US", "symbol": "AXTI", "size_r": 0.125},
                    {"market": "CN", "symbol": "000988.SZ", "size_r": 0.09},
                ],
            },
        }

    def test_clean_report_passes(self) -> None:
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                self._clean_payload(),
                "## CN Realized Horizon Edge\n\n### 🥇 个股 → **000988.SZ 华工科技**\n",
                "## US Realized Horizon Edge\n\n### 🥇 股票 → **AXTI**\n- 仓位:按 Production Decision 执行 **0.125R**\n",
            )

            failures = self.module.validate_report_dir(report_dir)

            self.assertEqual(failures, [])

    def test_ev_failed_with_us_r_fails(self) -> None:
        payload = self._clean_payload()
        payload["strategy_alpha_bulletin"] = {
            "ev_status": {"us": "failed"},
            "selected_policies": {"us": None},
        }
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                payload,
                "## CN Realized Horizon Edge\n",
                "## US Realized Horizon Edge\n",
            )

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("us_ev_gate_failed_with_execution_r", codes)

    def test_old_options_trade_language_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                self._clean_payload(),
                "## CN Realized Horizon Edge\n",
                "## US Realized Horizon Edge\n- 仓位:1R\n- 打法:本周 0DTE call\n- 仓位 ≤ 0.3R\n",
            )

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("forbidden_option_1r_sizing", codes)
            self.assertIn("forbidden_option_tactic_label", codes)
            self.assertIn("forbidden_short_option_size", codes)

    def test_options_data_terms_are_allowed_when_marked_context(self) -> None:
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                self._clean_payload(),
                "## CN Realized Horizon Edge\n",
                "## US Realized Horizon Edge\n| Term | Value |\n|---|---|\n| LEAPS ratio | 6.2x |\n| 0DTE bucket | unavailable |\n| PMCC | context label only |\n",
            )

            failures = self.module.validate_report_dir(report_dir)

            self.assertEqual(failures, [])

    def test_probability_pick_must_be_actionable(self) -> None:
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                self._clean_payload(),
                "## CN Realized Horizon Edge\n\n### 🥇 个股 → **300655.SZ 晶瑞电材**\n",
                "## US Realized Horizon Edge\n\n### 🥇 股票 → **AXTI**\n",
            )

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("cn_probability_pick_not_actionable", codes)


if __name__ == "__main__":
    unittest.main()
