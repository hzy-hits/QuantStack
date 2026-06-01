from __future__ import annotations

import importlib.util
import json
from datetime import date
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

    def test_market_report_path_is_agent_only_for_us_cn(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.object(module, "STACK_ROOT", Path(tmpdir)):
            self.assertEqual(
                module.report_path("2026-05-31", "us"),
                Path(tmpdir)
                / "reports"
                / "review_dashboard"
                / "main_strategy_v2"
                / "2026-05-31"
                / "us_daily_report_agent.md",
            )
            self.assertEqual(
                module.report_path("2026-05-31", "cn"),
                Path(tmpdir)
                / "reports"
                / "review_dashboard"
                / "main_strategy_v2"
                / "2026-05-31"
                / "cn_daily_report_agent.md",
            )

    def test_fresh_agent_report_requires_codex_metadata(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "us_daily_report_agent.md"
            as_of = date.today().isoformat()
            report.write_text(structured_us_report(as_of), encoding="utf-8")
            self.assertFalse(module._is_fresh_codex_agent_report(report, as_of))
            report.with_name(report.name + ".meta.json").write_text(
                json.dumps({"as_of": as_of, "backend": "codex"}),
                encoding="utf-8",
            )
            self.assertTrue(module._is_fresh_codex_agent_report(report, as_of))

    def test_us_agent_report_requires_structured_tables(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "us_daily_report_agent.md"
            as_of = date.today().isoformat()
            report.write_text(f"# 美股量化日报 — {as_of}\n\n## 一句话\n纯段落。\n", encoding="utf-8")
            report.with_name(report.name + ".meta.json").write_text(
                json.dumps({"as_of": as_of, "backend": "codex"}),
                encoding="utf-8",
            )
            self.assertFalse(module._is_fresh_codex_agent_report(report, as_of))

    def test_disabled_narrator_refuses_fallback_delivery(self) -> None:
        module = load_module()
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            mock.patch.object(module, "STACK_ROOT", Path(tmpdir)),
            mock.patch.dict(
                "os.environ",
                {"QUANT_DISABLE_US_NARRATOR": "1", "QUANT_NARRATOR_BACKEND": "codex"},
                clear=False,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "refusing to send a fallback report"):
                module._ensure_narrator("2026-05-31", "us")

    def test_successful_narrator_must_write_verified_agent_report(self) -> None:
        module = load_module()
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            mock.patch.object(module, "STACK_ROOT", Path(tmpdir)),
            mock.patch.dict("os.environ", {"QUANT_NARRATOR_BACKEND": "codex"}, clear=False),
        ):
            narrator = Path(tmpdir) / "scripts" / "agents" / "run_us_narrator.py"
            narrator.parent.mkdir(parents=True, exist_ok=True)
            narrator.write_text("import sys\nsys.exit(0)\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "did not produce a verified agent report"):
                module._ensure_narrator("2026-05-31", "us")

def structured_us_report(as_of: str) -> str:
    table = (
        "| Symbol | Decision | Size | Entry | Risk | Hedge | Why |\n"
        "|---|---|---:|---|---|---|---|\n"
        "| NVDA | Production long | 0.01R | 100 | stop 94 | SPY | Gamma + IV/HV |\n"
    )
    return f"""# 美股量化日报 — {as_of}

## 一句话
Production 候选保留,IV/HV 与 Gamma 共同约束。

## 市场状态
| Metric | Value |
|---|---|
| VIX | 15 |

## 今日交易清单
{table}

## 观察与风险
| Symbol | Status | Reason | Next check |
|---|---|---|---|
| AMD | watch | IV/HV high | next open |

| Symbol | IV/HV | IV rank | Context | Action |
|---|---:|---:|---|---|
| NVDA | 1.0 | 50% | healthy | hold |

| Symbol | Gamma state | Dealer proxy | Wall | Management |
|---|---|---:|---|---|
| NVDA | Gamma | +10% | wall | hold |

## 催化与复核
| Item | Date | Impact | Review |
|---|---|---|---|
| earnings | {as_of} | medium | review |

## 附注
options / news 仅作为股票决策证据,不是这份报告的交易标的。不构成投资建议。
"""


if __name__ == "__main__":
    unittest.main()
