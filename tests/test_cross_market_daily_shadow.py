from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "agents" / "run_cross_market_daily_shadow.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_cross_market_daily_shadow", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def artifact(module, market: str, report_date: str, tmp_path: Path):
    payload = {
        "as_of": report_date,
        "production_decision_summary": {
            "summary": {
                "us_r": 0.125,
                "us_action_count": 1,
                "cn_r": 0.05,
                "cn_action_count": 1,
            },
            "actionable": [
                {"market": "US", "symbol": "NVDA", "size_r": 0.125, "evidence_state": "原文已证明"},
                {"market": "CN", "symbol": "000063.SZ", "size_r": 0.05, "evidence_state": "原文已证明"},
            ],
        },
    }
    report_dir = tmp_path / report_date
    report_dir.mkdir(parents=True, exist_ok=True)
    md_path = report_dir / f"{market}_daily_report.md"
    md_path.write_text(f"# {market} report\n\nfixture", encoding="utf-8")
    return module.MarketArtifact(
        market=market,
        report_date=report_date,
        report_dir=report_dir,
        payload=payload,
        markdown=md_path.read_text(encoding="utf-8"),
        markdown_path=md_path,
    )


def test_pm_packet_keeps_us_to_cn_causality(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    packet = module.build_packet("pm", cn, us)

    assert packet["causal_direction"] == "US -> CN"
    assert packet["lead_market"] == "US"
    assert packet["target_market"] == "CN"
    assert packet["cn_role"] == "feedback_only"
    assert "不反向约束美股" in packet["thesis"]
    assert packet["agent_operating_mode"]["mode"] == "heuristic_tool_use"
    assert packet["data_boundary"]["fetch_workers"].startswith("Own data collection")
    assert any(tool["name"] == "select_cross_market_transmission" for tool in packet["tool_manifest"])
    assert packet["style_brief"]["reference_url"].startswith("https://boist.org/")


def test_pm_report_does_not_claim_cn_guides_us(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    report = module.deterministic_report(module.build_packet("pm", cn, us))

    assert "A股盘后 → 美股盘前" not in report
    assert "A股盘后反馈 + 美股盘前" in report
    assert "因果方向固定为: US -> CN" in report
    assert "不得反向升降美股仓位" in report


def test_validator_rejects_cn_to_us_framing() -> None:
    module = load_module()

    failures = module.validate_shadow_report("# 跨市场晚报\n\nCN -> US\nA股\n美股\n", "pm")

    assert any("CN -> US" in item for item in failures)


def test_agent_prompt_is_heuristic_not_fixed_template(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    system, user = module.build_agent_messages(module.build_packet("am", cn, us))

    assert "MCP/skill-like 工具面" in system
    assert "不是章节模板" in system
    assert "结构必须覆盖" not in system
    assert "coverage_checklist" in user
    assert "tool_manifest" in user
