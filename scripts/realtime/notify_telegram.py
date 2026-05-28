"""Send formatted advisor message to Telegram.

Setup:
  1. In Telegram, message @BotFather, /newbot, follow prompts.
     Save the BOT_TOKEN it gives you.
  2. Message your new bot anything (start chat).
  3. Visit https://api.telegram.org/bot<BOT_TOKEN>/getUpdates
     to find your chat_id (look for "chat":{"id": ...}).
  4. Set environment variables or write to scripts/realtime/telegram.yaml:
       TELEGRAM_BOT_TOKEN=<token>
       TELEGRAM_CHAT_ID=<chat_id>

Test:
  python3 scripts/realtime/notify_telegram.py --test
  → should ping "test message from quant-stack" to your Telegram
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import yaml

STACK_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = STACK_ROOT / "scripts" / "realtime" / "telegram.yaml"

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _load_credentials() -> tuple[str, str]:
    """Resolve credentials from env var first, then yaml file."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        return token, chat_id
    if CONFIG_PATH.exists():
        cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
        token = token or cfg.get("bot_token")
        chat_id = chat_id or str(cfg.get("chat_id") or "")
    if not token or not chat_id:
        raise SystemExit(
            "Telegram credentials missing.\n"
            "Set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID env vars, or write to:\n"
            f"  {CONFIG_PATH}\n"
            "with: \n"
            "  bot_token: 1234:ABC...\n"
            "  chat_id: 1234567890"
        )
    return token, str(chat_id)


def _fmt_advisor(setup: dict[str, Any], advisor: dict[str, Any]) -> str:
    """Format advisor JSON into a readable Telegram message (uses HTML mode)."""
    lines: list[str] = []
    lines.append(f"🔔 <b>{setup.get('type', '')}</b> · {setup.get('symbol', '')}")
    lines.append(f"<i>{setup.get('summary', '')}</i>")
    lines.append("")
    lines.append(f"📊 Regime: <b>{advisor.get('regime_now', '—')}</b>")
    lines.append(f"💡 {advisor.get('summary', '')}")
    lines.append("")
    ideas = advisor.get("trade_ideas") or []
    for i, idea in enumerate(ideas, start=1):
        lines.append(f"<b>Idea #{i}:</b> {idea.get('direction')} · {idea.get('instrument_family')}")
        lines.append(f"  instrument: <b>{idea.get('instrument')}</b> {idea.get('expiry_window', '')}")
        lines.append(f"  strike: {idea.get('strike_logic', '')}")
        lines.append(f"  entry: {idea.get('entry', '')}")
        lines.append(f"  stop: {idea.get('stop_logic', '')}")
        lines.append(f"  target: {idea.get('target_logic', '')}")
        lines.append(f"  risk: <b>{idea.get('max_risk_R', '')}R</b> · conf {idea.get('confidence_label', '')}")
        reasoning = idea.get("reasoning") or ""
        if reasoning:
            lines.append(f"  why: {reasoning}")
        lines.append("")
    risks = advisor.get("risk_notes") or []
    if risks:
        lines.append("⚠️ <b>Risk notes:</b>")
        for r in risks:
            lines.append(f"  · {r}")
    bans = advisor.get("do_not_do") or []
    if bans:
        lines.append("")
        lines.append("🚫 <b>Do NOT:</b>")
        for b in bans:
            lines.append(f"  · {b}")
    lines.append("")
    lines.append(f"<i>{datetime.utcnow().isoformat(timespec='seconds')}Z · 不构成投资建议</i>")
    return "\n".join(lines)


def send_message(text: str, *, parse_mode: str = "HTML") -> bool:
    token, chat_id = _load_credentials()
    url = TELEGRAM_API.format(token=token)
    try:
        r = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode,
                  "disable_web_page_preview": True},
            timeout=15,
        )
        if r.status_code != 200:
            print(f"[telegram] HTTP {r.status_code}: {r.text[:200]}", file=sys.stderr)
            return False
        return True
    except requests.RequestException as e:
        print(f"[telegram] request failed: {e}", file=sys.stderr)
        return False


def send_advisor(setup: dict[str, Any], advisor: dict[str, Any]) -> bool:
    text = _fmt_advisor(setup, advisor)
    return send_message(text)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="Send a test message")
    ap.add_argument("--setup-file", help="Setup JSON file")
    ap.add_argument("--advisor-file", help="Advisor JSON file")
    args = ap.parse_args()
    if args.test:
        ok = send_message("✅ test message from quant-stack realtime advisor")
        print("sent" if ok else "FAILED")
        return
    if not args.setup_file or not args.advisor_file:
        raise SystemExit("Need --setup-file and --advisor-file (or --test)")
    setup = json.loads(Path(args.setup_file).read_text())
    advisor = json.loads(Path(args.advisor_file).read_text())
    ok = send_advisor(setup, advisor)
    print("sent" if ok else "FAILED")


if __name__ == "__main__":
    main()
