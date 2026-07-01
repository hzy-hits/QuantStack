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


def test_write_prompt_tells_agent_deliver_to_send_visible_summary(tmp_path: Path) -> None:
    module = load_module()
    report = tmp_path / "report.md"
    report.write_text("# Hermes 跨市场早报\n\nMSFT/GOOGL/AVGO 期权观察。", encoding="utf-8")
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
    assert "不要回答“没有需要发送的新内容”" in prompt
