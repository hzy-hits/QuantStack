#!/usr/bin/env python3
"""Publish generated QuantStack reports to an OpenClaw inbox on another host."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


DEFAULT_REMOTE_ROOT = "/home/ivena/.openclaw/quant-stack"
DEFAULT_OPENCLAW_BIN = "/home/ivena/.local/bin/openclaw"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a report into a remote OpenClaw inbox.")
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--kind", default=os.environ.get("QUANT_OPENCLAW_KIND", "cross_market_daily"))
    parser.add_argument("--slot", default=os.environ.get("QUANT_OPENCLAW_SLOT", ""))
    parser.add_argument("--date", default=os.environ.get("QUANT_OPENCLAW_DATE", ""))
    parser.add_argument("--title", default=os.environ.get("QUANT_OPENCLAW_TITLE", ""))
    parser.add_argument("--packet-path", type=Path, default=None)
    parser.add_argument("--meta-path", type=Path, default=None)
    parser.add_argument("--remote-host", default=os.environ.get("QUANT_OPENCLAW_HOST", "100.109.146.30"))
    parser.add_argument("--remote-user", default=os.environ.get("QUANT_OPENCLAW_USER", "ivena"))
    parser.add_argument("--remote-root", default=os.environ.get("QUANT_OPENCLAW_REMOTE_ROOT", DEFAULT_REMOTE_ROOT))
    parser.add_argument("--identity-file", default=os.environ.get("QUANT_OPENCLAW_IDENTITY_FILE", ""))
    parser.add_argument(
        "--mode",
        choices=["file", "agent", "message", "all"],
        default=os.environ.get("QUANT_OPENCLAW_MODE", "file"),
        help="file copies artifacts; agent also notifies an OpenClaw agent; message sends via a channel.",
    )
    parser.add_argument("--openclaw-bin", default=os.environ.get("QUANT_OPENCLAW_BIN", DEFAULT_OPENCLAW_BIN))
    parser.add_argument("--agent", default=os.environ.get("QUANT_OPENCLAW_AGENT", "main"))
    parser.add_argument("--agent-session-key", default=os.environ.get("QUANT_OPENCLAW_AGENT_SESSION_KEY", ""))
    parser.add_argument("--agent-timeout", type=int, default=int(os.environ.get("QUANT_OPENCLAW_AGENT_TIMEOUT", "180")))
    parser.add_argument(
        "--agent-deliver",
        action="store_true",
        default=os.environ.get("QUANT_OPENCLAW_AGENT_DELIVER", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--reply-channel", default=os.environ.get("QUANT_OPENCLAW_REPLY_CHANNEL", ""))
    parser.add_argument("--reply-account", default=os.environ.get("QUANT_OPENCLAW_REPLY_ACCOUNT", ""))
    parser.add_argument("--reply-to", default=os.environ.get("QUANT_OPENCLAW_REPLY_TO", ""))
    parser.add_argument("--message-channel", default=os.environ.get("QUANT_OPENCLAW_MESSAGE_CHANNEL", ""))
    parser.add_argument("--message-target", default=os.environ.get("QUANT_OPENCLAW_MESSAGE_TARGET", ""))
    parser.add_argument(
        "--allow-duplicate-event",
        action="store_true",
        default=os.environ.get("QUANT_OPENCLAW_ALLOW_DUPLICATE_EVENT", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("QUANT_OPENCLAW_SSH_TIMEOUT", "60")))
    return parser.parse_args()


def run(cmd: list[str], *, timeout: int, dry_run: bool = False) -> subprocess.CompletedProcess[str]:
    if dry_run:
        print("dry-run:", " ".join(cmd))
        return subprocess.CompletedProcess(cmd, 0, "", "")
    result = subprocess.run(cmd, check=False, text=True, capture_output=True, timeout=timeout)
    if result.returncode != 0:
        tail = ((result.stderr or "") + "\n" + (result.stdout or "")).strip()[-1600:]
        raise RuntimeError(f"command failed exit={result.returncode}: {tail}")
    return result


def ssh_base(args: argparse.Namespace) -> list[str]:
    cmd = ["ssh"]
    if args.identity_file:
        cmd.extend(["-i", args.identity_file])
    cmd.extend([
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        f"{args.remote_user}@{args.remote_host}",
    ])
    return cmd


def scp_base(args: argparse.Namespace) -> list[str]:
    cmd = ["scp"]
    if args.identity_file:
        cmd.extend(["-i", args.identity_file])
    cmd.extend(["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"])
    return cmd


def shell_join(argv: list[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in argv)


def ssh_command(
    args: argparse.Namespace,
    remote_argv: list[str],
    *,
    timeout: int,
    dry_run: bool = False,
) -> subprocess.CompletedProcess[str]:
    return run(ssh_base(args) + [shell_join(remote_argv)], timeout=timeout, dry_run=dry_run)


def run_remote_python_script(
    args: argparse.Namespace,
    code: str,
    remote_dir: PurePosixPath,
    script_name: str,
    argv: list[str],
    *,
    timeout: int,
    dry_run: bool = False,
) -> subprocess.CompletedProcess[str]:
    remote_script = remote_dir / script_name
    with tempfile.TemporaryDirectory(prefix="openclaw-remote-script-") as tmp:
        local_script = Path(tmp) / script_name
        local_script.write_text(code.strip() + "\n", encoding="utf-8")
        run(
            scp_base(args) + [str(local_script), f"{args.remote_user}@{args.remote_host}:{remote_script}"],
            timeout=timeout,
            dry_run=dry_run,
        )
    return ssh_command(args, ["python3", str(remote_script), *argv], timeout=timeout, dry_run=dry_run)


def remote_report_dir(args: argparse.Namespace) -> PurePosixPath:
    date_part = args.date or datetime.now(timezone.utc).date().isoformat()
    name = args.kind + (f"_{args.slot}" if args.slot else "")
    return PurePosixPath(args.remote_root) / "reports" / date_part / name


def first_line(path: Path) -> str:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                return line.strip().lstrip("#").strip()
    except FileNotFoundError:
        return ""
    return ""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def default_session_key(kind: str, slot: str) -> str:
    if kind == "cross_market_daily" and slot == "am":
        return "agent:main:briefing-morning"
    if kind == "cross_market_daily" and slot == "pm":
        return "agent:main:briefing-close"
    if "weekly" in kind:
        return "agent:main:briefing-weekly"
    return f"agent:main:{kind}" + (f"-{slot}" if slot else "")


def build_manifest(args: argparse.Namespace, remote_dir: PurePosixPath, copied: dict[str, str]) -> dict[str, Any]:
    title = args.title or first_line(args.report_path)
    report_sha = file_sha256(args.report_path)
    event_basis = {
        "kind": args.kind,
        "slot": args.slot,
        "date": args.date,
        "title": title,
        "source_report_sha256": report_sha,
    }
    event_id = hashlib.sha256(
        json.dumps(event_basis, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return {
        "schema": "quant-stack.openclaw_report.v1",
        "event_id": event_id,
        "kind": args.kind,
        "slot": args.slot,
        "date": args.date,
        "title": title,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "source_host": socket.gethostname(),
        "source_report_path": str(args.report_path),
        "source_report_sha256": report_sha,
        "remote_dir": str(remote_dir),
        "remote_report_path": copied["report"],
        "remote_packet_path": copied.get("packet", ""),
        "remote_meta_path": copied.get("meta", ""),
        "agent_backend": "hermes" if args.kind.startswith("cross_market") else "",
        "openclaw_agent": args.agent,
        "openclaw_session_key": args.agent_session_key or default_session_key(args.kind, args.slot),
    }


def copy_artifacts(args: argparse.Namespace) -> tuple[PurePosixPath, dict[str, str]]:
    if not args.report_path.exists():
        raise FileNotFoundError(args.report_path)

    remote_dir = remote_report_dir(args)
    ssh_command(args, ["mkdir", "-p", str(remote_dir), str(PurePosixPath(args.remote_root) / "inbox")], timeout=args.timeout, dry_run=args.dry_run)

    files: list[tuple[str, Path]] = [("report", args.report_path)]
    if args.packet_path and args.packet_path.exists():
        files.append(("packet", args.packet_path))
    if args.meta_path and args.meta_path.exists():
        files.append(("meta", args.meta_path))

    copied: dict[str, str] = {}
    for key, path in files:
        remote_path = remote_dir / path.name
        run(
            scp_base(args) + [str(path), f"{args.remote_user}@{args.remote_host}:{remote_path}"],
            timeout=args.timeout,
            dry_run=args.dry_run,
        )
        copied[key] = str(remote_path)
    return remote_dir, copied


def install_manifest(args: argparse.Namespace, remote_manifest: str) -> bool:
    code = r"""
import json
import pathlib
import sys

manifest_path = pathlib.Path(sys.argv[1])
remote_root = pathlib.Path(sys.argv[2])
allow_duplicate = sys.argv[3] == "1"
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
inbox = remote_root / "inbox"
inbox.mkdir(parents=True, exist_ok=True)
line = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
events_path = inbox / "events.jsonl"
event_id = manifest.get("event_id")
seen = False
if event_id and events_path.exists() and not allow_duplicate:
    with events_path.open("r", encoding="utf-8") as fh:
        for existing in fh:
            try:
                if json.loads(existing).get("event_id") == event_id:
                    seen = True
                    break
            except json.JSONDecodeError:
                continue
if not seen or allow_duplicate:
    with events_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
(inbox / "latest.json").write_text(line + "\n", encoding="utf-8")
suffix = manifest.get("kind", "report")
if manifest.get("slot"):
    suffix += "_" + manifest["slot"]
(inbox / f"latest_{suffix}.json").write_text(line + "\n", encoding="utf-8")
print(json.dumps({"latest": str(inbox / "latest.json"), "duplicate": seen and not allow_duplicate}, ensure_ascii=False))
"""
    result = run_remote_python_script(
        args,
        code,
        PurePosixPath(remote_manifest).parent,
        "openclaw_install_manifest.py",
        [remote_manifest, args.remote_root, "1" if args.allow_duplicate_event else "0"],
        timeout=args.timeout,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        return True
    try:
        payload = json.loads((result.stdout or "").strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return True
    return not bool(payload.get("duplicate"))


def write_prompt(manifest: dict[str, Any], report_path: Path) -> str:
    excerpt = ""
    try:
        text = report_path.read_text(encoding="utf-8")
        excerpt = text[:1800].rstrip()
    except FileNotFoundError:
        pass
    return (
        "Hermes/QuantStack 已生成一份报告，并已放入树莓派本机 OpenClaw inbox。\n"
        "请把它当作 Oracle Hermes 上游 subagent 的产物登记到 briefing session；不要重写全文，"
        "只保留一句状态、3 条要点和本机报告路径，供后续对话调用。\n\n"
        f"- kind: {manifest.get('kind')}\n"
        f"- slot: {manifest.get('slot')}\n"
        f"- date: {manifest.get('date')}\n"
        f"- title: {manifest.get('title')}\n"
        f"- report: {manifest.get('remote_report_path')}\n"
        f"- packet: {manifest.get('remote_packet_path')}\n"
        f"- meta: {manifest.get('remote_meta_path')}\n\n"
        "报告开头摘录：\n"
        f"{excerpt}"
    )


def notify_agent(args: argparse.Namespace, manifest: dict[str, Any], prompt_path: Path) -> None:
    remote_prompt = PurePosixPath(manifest["remote_dir"]) / prompt_path.name
    run(
        scp_base(args) + [str(prompt_path), f"{args.remote_user}@{args.remote_host}:{remote_prompt}"],
        timeout=args.timeout,
        dry_run=args.dry_run,
    )
    session_key = args.agent_session_key or default_session_key(args.kind, args.slot)
    code = r"""
import pathlib
import subprocess
import sys

openclaw, agent, session_key, timeout_s, deliver, reply_channel, reply_account, reply_to, prompt_path = sys.argv[1:10]
message = pathlib.Path(prompt_path).read_text(encoding="utf-8")
cmd = [openclaw, "agent", "--agent", agent, "--session-key", session_key, "--message", message, "--timeout", timeout_s, "--json"]
if deliver == "1":
    cmd.append("--deliver")
    if reply_channel:
        cmd.extend(["--reply-channel", reply_channel])
    if reply_account:
        cmd.extend(["--reply-account", reply_account])
    if reply_to:
        cmd.extend(["--reply-to", reply_to])
result = subprocess.run(cmd, text=True, capture_output=True)
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
sys.exit(result.returncode)
"""
    run_remote_python_script(
        args,
        code,
        PurePosixPath(manifest["remote_dir"]),
        "openclaw_notify_agent.py",
        [
            args.openclaw_bin,
            args.agent,
            session_key,
            str(args.agent_timeout),
            "1" if args.agent_deliver else "0",
            args.reply_channel,
            args.reply_account,
            args.reply_to,
            str(remote_prompt),
        ],
        timeout=max(args.timeout, args.agent_timeout + 15),
        dry_run=args.dry_run,
    )


def send_message(args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    if not args.message_channel or not args.message_target:
        raise ValueError("--message-channel and --message-target are required for mode=message/all")
    message = (
        f"{manifest.get('title')}\n"
        f"{manifest.get('kind')} {manifest.get('slot')} {manifest.get('date')}\n"
        f"{manifest.get('remote_report_path')}"
    )
    cmd = [
        args.openclaw_bin,
        "message",
        "send",
        "--channel",
        args.message_channel,
        "--target",
        args.message_target,
        "--message",
        message,
        "--media",
        manifest["remote_report_path"],
        "--force-document",
        "--json",
    ]
    code = "import subprocess,sys; sys.exit(subprocess.run(sys.argv[1:]).returncode)"
    run_remote_python_script(
        args,
        code,
        PurePosixPath(manifest["remote_dir"]),
        "openclaw_send_message.py",
        cmd,
        timeout=args.timeout,
        dry_run=args.dry_run,
    )


def main() -> int:
    args = parse_args()
    with tempfile.TemporaryDirectory(prefix="openclaw-publish-") as tmp:
        manifest_path = Path(tmp) / "manifest.json"
        prompt_path = Path(tmp) / "openclaw_prompt.txt"
        remote_dir, copied = copy_artifacts(args)
        manifest = build_manifest(args, remote_dir, copied)
        manifest["remote_manifest_path"] = str(remote_dir / manifest_path.name)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        run(
            scp_base(args) + [str(manifest_path), f"{args.remote_user}@{args.remote_host}:{remote_dir / manifest_path.name}"],
            timeout=args.timeout,
            dry_run=args.dry_run,
        )
        is_new_event = install_manifest(args, manifest["remote_manifest_path"])
        if args.mode in {"agent", "all"} and is_new_event:
            prompt_path.write_text(write_prompt(manifest, args.report_path), encoding="utf-8")
            notify_agent(args, manifest, prompt_path)
        if args.mode in {"message", "all"} and is_new_event:
            send_message(args, manifest)
        print(json.dumps({"ok": True, "new_event": is_new_event, "manifest": manifest}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
