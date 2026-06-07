from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_us_narrator_guard_requires_structured_tables() -> None:
    text = (ROOT / "scripts" / "agents" / "run_us_narrator.py").read_text(encoding="utf-8")

    assert "Codex 结构化研报" in text
    assert "至少包含 4 张 Markdown 表格" in text
    assert "Production candidates" in text
    assert "Watch / 0R context" in text
    assert "IV/HV 表" in text
    assert "Gamma v3 表" in text
    assert "Congressional Trading" in text
    assert "政策资金流" in text
    assert "数据校准行" in text
    assert "不是当日美股已收盘数据" in text
    assert "策略叙事底稿" in text
    assert "策略主线" in text
    assert "市场结构" in text
    assert "交易计划" in text
    assert "风险与反证" in text
    assert "不新增仓位风险(0R)" in text
    assert "build_layout_skeleton" in text
    assert "build_strategy_story_brief" in text
    assert "validate_structured_us_report" in text
    assert "版式骨架" in text
    assert "不使用 emoji" in text
    assert "narrator:us:repair" in text
    assert "max_tokens=4500" in text


def test_us_merge_prompt_keeps_markdown_tables() -> None:
    text = (ROOT / "quant-research-v1" / "prompts" / "us-merge-agent.md").read_text(encoding="utf-8")

    assert "Production candidates Markdown 表" in text
    assert "Watch / 0R context 表" in text
    assert "IV/HV 表" in text
    assert "Gamma v3 表" in text
    assert "Congressional Trading / 政策资金流表" in text
    assert "结构化 Markdown 表格" in text
    assert "数据校准" in text
    assert "US 收盘价数据截至" in text
    assert "策略叙事底稿是最高叙事锚点" in text
    assert "策略主线" in text
    assert "市场结构" in text
    assert "交易计划" in text
    assert "风险与反证" in text
    assert "正式执行" in text
    assert "观察候选" in text
    assert "模型诊断" in text
    assert "不新增仓位风险(0R)" in text
    assert "政策资金流是催化还是预警" in text
