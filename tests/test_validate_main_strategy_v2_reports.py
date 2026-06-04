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
            "us": {"current_date": "2026-05-25"},
            "strategy_alpha_bulletin": {
                "ev_status": {"us": "passed"},
                "selected_policies": {"us": "us_theme_cluster_momentum"},
            },
            "us_market_data_status": {
                "as_of": "2026-05-25",
                "stock_data_current": True,
                "latest_date": "2026-05-25",
                "prices_daily_latest_date": "2026-05-25",
                "effective_us_market_date": "2026-05-25",
                "options_analysis_latest_as_of": "2026-05-25",
                "options_chain_latest_as_of": "2026-05-25",
                "options_sentiment_latest_as_of": "2026-05-25",
                "state": "current",
                "is_previous_session": False,
            },
            "gamma_spring": {"effective_date": "2026-05-25"},
            "fear_greed": {"source": "cnn", "score": 50.0, "rating": "neutral"},
            "risk_regime": {"signals": {"fear_greed_score": 50.0}},
            "bubble_hedge": {"confirmation": {"fear_greed_score": 50.0}},
            "report_dates": {
                "report_label_date": "2026-05-25",
                "effective_us_market_date": "2026-05-25",
                "prices_effective_date": "2026-05-25",
                "report_decisions_effective_date": "2026-05-25",
                "gamma_effective_date": "2026-05-25",
                "fear_greed_source": "cnn",
            },
            "report_action_backtest_summary": {"by_mode_market": {"contract_gated:US": {}, "contract_gated:CN": {}}},
            "production_decision_summary": {
                "summary": {"us_r": 0.125, "us_action_count": 1},
                "actionable": [
                    {
                        "market": "US",
                        "symbol": "AXTI",
                        "size_r": 0.125,
                        "evidence_state": "原文已证明: fixture",
                    },
                    {
                        "market": "CN",
                        "symbol": "000988.SZ",
                        "size_r": 0.09,
                        "evidence_state": "原文已证明: fixture",
                    },
                ],
            },
        }

    def _clean_cn_report(self, extra: str = "") -> str:
        return "## CN Realized Horizon Edge\n\n### 🥇 个股 → **000988.SZ 华工科技**\n" + extra

    def _clean_us_report(self, extra: str = "") -> str:
        return """## 数据校准

| Item | Value |
|---|---|
| 报告标签日期 | 2026-05-25 |
| US 收盘价数据截至 | 2026-05-25 |
| US 候选/执行数据日期 | 2026-05-25 |
| US 期权链/Gamma 有效日 | 2026-05-25 |
| Fear & Greed | CNN 50.0 (neutral) |

## US Realized Horizon Edge

### 🥇 股票 → **AXTI**
- 仓位:按 Production Decision 执行 **0.125R**
""" + extra

    def test_clean_report_passes(self) -> None:
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                self._clean_payload(),
                self._clean_cn_report(),
                self._clean_us_report(),
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
                self._clean_cn_report(),
                self._clean_us_report(),
            )

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("us_ev_gate_failed_with_execution_r", codes)

    def test_old_options_trade_language_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(
                report_dir,
                self._clean_payload(),
                self._clean_cn_report(),
                self._clean_us_report("\n- 仓位:1R\n- 打法:本周 0DTE call\n- 仓位 ≤ 0.3R\n"),
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
                self._clean_cn_report(),
                self._clean_us_report("\n| Term | Value |\n|---|---|\n| LEAPS ratio | 6.2x |\n| 0DTE bucket | unavailable |\n| PMCC | context label only |\n"),
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
                self._clean_us_report(),
            )

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("cn_probability_pick_not_actionable", codes)

    def test_previous_session_us_report_must_say_not_same_day_close(self) -> None:
        payload = self._clean_payload()
        payload["as_of"] = "2026-05-26"
        payload["us"]["current_date"] = "2026-05-25"
        payload["us_market_data_status"].update(
            {
                "as_of": "2026-05-26",
                "state": "previous_session",
                "is_previous_session": True,
                "prices_daily_latest_date": "2026-05-25",
                "effective_us_market_date": "2026-05-25",
            }
        )
        payload["report_dates"].update(
            {
                "report_label_date": "2026-05-26",
                "effective_us_market_date": "2026-05-25",
                "prices_effective_date": "2026-05-25",
                "report_decisions_effective_date": "2026-05-25",
                "gamma_effective_date": "2026-05-25",
            }
        )
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-26"
            _write_report(
                report_dir,
                payload,
                self._clean_cn_report(),
                self._clean_us_report().replace("报告标签日期 | 2026-05-25", "报告标签日期 | 2026-05-26"),
            )

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("us_report_missing_previous_session_caveat", codes)

    def test_fear_greed_mismatch_fails(self) -> None:
        payload = self._clean_payload()
        payload["risk_regime"]["signals"]["fear_greed_score"] = 71.0
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(report_dir, payload, self._clean_cn_report(), self._clean_us_report())

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("risk_regime_fear_greed_mismatch", codes)

    def test_forbidden_evidence_marker_on_actionable_fails(self) -> None:
        payload = self._clean_payload()
        payload["production_decision_summary"]["actionable"][0]["evidence_state"] = "合理推论+待原文核验"
        with TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "2026-05-25"
            _write_report(report_dir, payload, self._clean_cn_report(), self._clean_us_report())

            codes = {failure.code for failure in self.module.validate_report_dir(report_dir)}

            self.assertIn("production_actionable_forbidden_evidence_marker", codes)


if __name__ == "__main__":
    unittest.main()
