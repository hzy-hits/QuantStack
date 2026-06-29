from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest import mock


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
    (report_dir / "main_strategy_v2_backtest.json").write_text(
        module.json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
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
    assert any(tool["name"] == "finance-search.quant_stack_spine_triage" for tool in packet["tool_manifest"])
    assert packet["style_brief"]["reference_url"].startswith("https://boist.org/")


def test_am_uses_previous_cn_context_when_target_day_payload_is_missing(tmp_path: Path) -> None:
    module = load_module()
    artifact(module, "cn", "2026-06-26", tmp_path)

    cn, note = module.load_cn_context_artifact(tmp_path, "am", "2026-06-29")

    assert cn.report_date == "2026-06-26"
    assert note is not None
    assert "2026-06-29" in note
    assert "2026-06-26" in note


def test_am_saturday_uses_friday_cn_context(tmp_path: Path) -> None:
    module = load_module()
    artifact(module, "cn", "2026-06-26", tmp_path)

    cn, note = module.load_cn_context_artifact(tmp_path, "am", "2026-06-27")

    assert cn.report_date == "2026-06-26"
    assert note is not None


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


def test_validator_rejects_delivery_failure_language() -> None:
    module = load_module()

    failures = module.validate_shadow_report("# 跨市场晚报\n\nA股\n美股\nvalidator | 未通过\n", "pm")

    assert any("validator | 未通过" in item for item in failures)


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


def test_hermes_prompt_retires_legacy_narrator_templates(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    prompt = module.build_hermes_prompt(module.build_packet("pm", cn, us))

    assert "Hermes lead editor agent" in prompt
    assert "finance-search MCP" in prompt
    assert "不要使用 quant-research-v1/prompts" in prompt
    assert "coverage_checklist 是验收清单,不是章节模板" in prompt
    assert "不得把 A股盘后反馈写成会指导美股盘前或美股策略" in prompt
    assert "CN -> US" not in prompt
    assert "数据缺口/待补证据" in prompt
    assert "不要写成生产运行错误" in prompt
    assert "投递失败" not in prompt


def test_call_hermes_agent_uses_hermes_skill(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    completed = mock.Mock(returncode=0, stdout="# 跨市场早报 — 2026-06-29\n\n美股影响A股。", stderr="")
    with mock.patch.object(module.subprocess, "run", return_value=completed) as run:
        report = module.call_hermes_agent(
            packet,
            timeout=30,
            hermes_bin="/home/ubuntu/.local/bin/hermes",
            model="",
            provider="",
            max_turns=8,
        )

    cmd = run.call_args.args[0]
    assert cmd[0] == "/home/ubuntu/.local/bin/hermes"
    assert cmd[1:4] == ["chat", "-Q", "-q"]
    assert "--skills" in cmd
    assert "quant-stack-cross-market-daily" in cmd
    assert "--max-turns" in cmd
    assert "--source" in cmd
    assert "quant-stack-cron" in cmd
    assert report.startswith("# 跨市场早报")
    assert packet["_agent_backend"] == "hermes"


def test_fallback_report_uses_legacy_backend_only_after_primary_failure(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    with mock.patch.object(module, "call_agent", return_value="# 跨市场早报 — 2026-06-29\n\n美股 A股"):
        report, backend = module.fallback_report(packet, "auto", 30, RuntimeError("hermes down"))

    assert report.startswith("# 跨市场早报")
    assert backend.startswith("fallback:")
    assert "hermes down" in packet["_agent_primary_error"]
