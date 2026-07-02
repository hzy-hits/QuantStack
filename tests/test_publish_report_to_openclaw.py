from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "publish_report_to_openclaw.py"


def load_module():
    spec = importlib.util.spec_from_file_location("publish_report_to_openclaw", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_reply_destinations_fan_out_accounts_and_targets() -> None:
    module = load_module()
    args = argparse.Namespace(
        reply_channel="openclaw-weixin",
        reply_account="912f45c70aa5-im-bot,86fb46c4a557-im-bot",
        reply_to="o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
    )

    assert module.reply_destinations(args) == [
        {
            "reply_channel": "openclaw-weixin",
            "reply_account": "912f45c70aa5-im-bot",
            "reply_to": "o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat",
        },
        {
            "reply_channel": "openclaw-weixin",
            "reply_account": "86fb46c4a557-im-bot",
            "reply_to": "o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
        },
    ]


def test_reply_destinations_reject_mismatched_lists() -> None:
    module = load_module()
    args = argparse.Namespace(
        reply_channel="openclaw-weixin",
        reply_account="acct-1,acct-2",
        reply_to="target-1,target-2,target-3",
    )

    try:
        module.reply_destinations(args)
    except ValueError as exc:
        assert "--reply-account" in str(exc)
    else:
        raise AssertionError("expected mismatched fan-out lists to fail")


def test_message_destinations_fan_out_accounts_and_targets() -> None:
    module = load_module()
    args = argparse.Namespace(
        message_channel="openclaw-weixin",
        message_account="acct-1,acct-2",
        message_target="target-1,target-2",
        reply_channel="",
        reply_account="",
        reply_to="",
    )

    assert module.message_destinations(args) == [
        {"message_channel": "openclaw-weixin", "message_account": "acct-1", "message_target": "target-1"},
        {"message_channel": "openclaw-weixin", "message_account": "acct-2", "message_target": "target-2"},
    ]


def test_message_destinations_can_reuse_reply_config() -> None:
    module = load_module()
    args = argparse.Namespace(
        message_channel="",
        message_account="",
        message_target="",
        reply_channel="openclaw-weixin",
        reply_account="acct-1,acct-2",
        reply_to="target-1,target-2",
    )

    assert module.message_destinations(args) == [
        {"message_channel": "openclaw-weixin", "message_account": "acct-1", "message_target": "target-1"},
        {"message_channel": "openclaw-weixin", "message_account": "acct-2", "message_target": "target-2"},
    ]


def test_notify_agent_fans_out_remote_calls(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    prompt = tmp_path / "prompt.txt"
    prompt.write_text("report ready", encoding="utf-8")
    args = argparse.Namespace(
        remote_user="ivena",
        remote_host="100.109.146.30",
        identity_file="",
        timeout=60,
        dry_run=False,
        openclaw_bin="openclaw",
        agent="main",
        agent_session_key="",
        kind="cross_market_daily",
        slot="pm",
        agent_timeout=180,
        agent_deliver=True,
        reply_channel="openclaw-weixin",
        reply_account="acct-1,acct-2",
        reply_to="target-1,target-2",
    )
    manifest = {"remote_dir": "/tmp/openclaw", "kind": "cross_market_daily", "slot": "pm"}
    remote_calls: list[list[str]] = []

    monkeypatch.setattr(module, "run", lambda *a, **kw: subprocess.CompletedProcess([], 0, "", ""))

    def fake_remote_script(*call_args, **_kwargs):
        remote_calls.append(call_args[4])
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(module, "run_remote_python_script", fake_remote_script)

    deliveries = module.notify_agent(args, manifest, prompt)

    assert [call[5:8] for call in remote_calls] == [
        ["openclaw-weixin", "acct-1", "target-1"],
        ["openclaw-weixin", "acct-2", "target-2"],
    ]
    assert len(deliveries) == 2


def test_send_message_fans_out_direct_openclaw_messages(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    report = tmp_path / "report.md"
    report.write_text(
        """# Hermes 跨市场早报

## 跨市场主线
美股半导体和 AI 主线继续给 A股风险预算，但 A股仍按本域候选管线等待确认。

## 宏观事件与产业新闻
- 美联储利率路径仍是全球风险资产的核心变量（Reuters / 2026-07-01T00:00:00Z）
- DRAM 现货价格继续牵动亚洲半导体链（TrendForce / 2026-07-01T02:49:02Z）

## SEC 13F 机构持仓快照
过去 12 小时本地新增 1 个 13F 持仓文件；13F 有季度滞后，只作为机构仓位线索，不当作实时资金流。

| Manager | Filing/Report | Holdings | 新增Top5 | 增持Top5 | 减持Top5 |
|---|---|---:|---|---|---|
| TEST MANAGER | 2026-06-30 / 2026-03-31 | 3 | NEW AI CO($200.0M) | APPLE INC($75.0M) | OLD CO($-50.0M) |
""",
        encoding="utf-8",
    )
    args = argparse.Namespace(
        openclaw_bin="openclaw",
        timeout=60,
        dry_run=False,
        report_path=report,
        message_channel="openclaw-weixin",
        message_account="acct-1,acct-2",
        message_target="target-1,target-2",
        reply_channel="",
        reply_account="",
        reply_to="",
    )
    manifest = {
        "title": "Hermes 跨市场早报",
        "kind": "cross_market_daily",
        "slot": "am",
        "date": "2026-07-01",
        "remote_report_path": "/tmp/report.md",
        "remote_dir": "/tmp/openclaw",
    }
    remote_calls: list[dict[str, object]] = []

    def fake_remote_script(*call_args, **_kwargs):
        remote_calls.append({"script": call_args[3], "argv": call_args[4]})
        return subprocess.CompletedProcess(
            [],
            0,
            '{"messageId":"openclaw-weixin:test","contextToken":"present","response":{"ret":0}}\n',
            "",
        )

    monkeypatch.setattr(module, "run_remote_python_script", fake_remote_script)

    deliveries = module.send_message(args, manifest)

    assert [item["message_account"] for item in deliveries] == ["acct-1", "acct-2"]
    assert [item["message_transport"] for item in deliveries] == ["weixin-direct-api", "weixin-direct-api"]
    assert [item["message_id"] for item in deliveries] == ["openclaw-weixin:test", "openclaw-weixin:test"]
    assert [item["context_token"] for item in deliveries] == ["present", "present"]
    assert [call["script"] for call in remote_calls] == [
        "openclaw_send_weixin_direct.py",
        "openclaw_send_weixin_direct.py",
    ]
    assert [call["argv"][1] for call in remote_calls] == ["acct-1", "acct-2"]
    assert [call["argv"][2] for call in remote_calls] == ["target-1", "target-2"]
    pushed = remote_calls[0]["argv"][3]
    assert "报告解读:" in pushed
    assert "最新新闻:" in pushed
    assert "13F 机构仓位:" in pushed
    assert "TEST MANAGER" in pushed
    assert "/tmp/report.md" in pushed


def test_send_message_keeps_openclaw_cli_for_non_weixin_channels(monkeypatch) -> None:
    module = load_module()
    args = argparse.Namespace(
        openclaw_bin="openclaw",
        timeout=60,
        dry_run=False,
        message_channel="slack",
        message_account="",
        message_target="#reports",
        reply_channel="",
        reply_account="",
        reply_to="",
    )
    manifest = {
        "title": "Hermes 跨市场早报",
        "kind": "cross_market_daily",
        "slot": "am",
        "date": "2026-07-01",
        "remote_report_path": "/tmp/report.md",
        "remote_dir": "/tmp/openclaw",
    }
    remote_calls: list[dict[str, object]] = []

    def fake_remote_script(*call_args, **_kwargs):
        remote_calls.append({"script": call_args[3], "argv": call_args[4]})
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(module, "run_remote_python_script", fake_remote_script)

    deliveries = module.send_message(args, manifest)

    assert deliveries == [
        {
            "message_channel": "slack",
            "message_account": "",
            "message_target": "#reports",
            "message_transport": "openclaw-cli",
        }
    ]
    assert remote_calls[0]["script"] == "openclaw_send_message.py"
    argv = remote_calls[0]["argv"]
    assert argv[argv.index("--channel") + 1] == "slack"
    assert argv[argv.index("--target") + 1] == "#reports"


def test_deliver_new_event_all_mode_keeps_message_when_agent_fails(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    report = tmp_path / "report.md"
    prompt = tmp_path / "prompt.txt"
    report.write_text("# Hermes 跨市场早报\n\n今天的报告。", encoding="utf-8")
    args = argparse.Namespace(mode="all", report_path=report)
    manifest = {
        "title": "Hermes 跨市场早报",
        "kind": "cross_market_daily",
        "slot": "am",
        "date": "2026-07-02",
        "remote_report_path": "/tmp/report.md",
    }
    calls: list[str] = []

    def fake_send_message(_args, _manifest):
        calls.append("message")
        return [{"message_channel": "openclaw-weixin"}]

    def fake_notify_agent(_args, _manifest, _prompt_path):
        calls.append("agent")
        raise TimeoutError("agent timed out after 180 seconds")

    monkeypatch.setattr(module, "send_message", fake_send_message)
    monkeypatch.setattr(module, "notify_agent", fake_notify_agent)

    agent_deliveries, message_deliveries, agent_error = module.deliver_new_event(args, manifest, prompt)

    assert calls == ["message", "agent"]
    assert agent_deliveries == []
    assert message_deliveries == [{"message_channel": "openclaw-weixin"}]
    assert "agent timed out" in agent_error
    assert prompt.exists()


def test_write_prompt_tells_agent_deliver_to_send_visible_summary(tmp_path: Path) -> None:
    module = load_module()
    report = tmp_path / "report.md"
    report.write_text(
        "# Hermes 跨市场早报\n\n"
        "MSFT/GOOGL/AVGO 期权观察。\n\n"
        "## 宏观事件与产业新闻\n"
        "- AI demand fuels investors' portfolios（CNBC / 2026-07-01T04:08:00Z）\n",
        encoding="utf-8",
    )
    manifest = {
        "kind": "cross_market_daily",
        "slot": "am",
        "date": "2026-07-01",
        "title": "Hermes 跨市场早报 - 2026-07-01",
        "remote_report_path": "/home/ivena/.openclaw/quant-stack/reports/2026-07-01/cross_market_daily_am/report.md",
        "remote_packet_path": "/home/ivena/.openclaw/quant-stack/reports/2026-07-01/cross_market_daily_am/packet.json",
        "remote_meta_path": "/home/ivena/.openclaw/quant-stack/reports/2026-07-01/cross_market_daily_am/meta.json",
    }

    prompt = module.write_prompt(manifest, report)

    assert "--deliver" in prompt
    assert "最终回复必须是一条可见摘要" in prompt
    assert "最新新闻/13F 摘要" in prompt
    assert "通知摘要候选" in prompt
    assert "不要回答“没有需要发送的新内容”" in prompt
