#!/usr/bin/env python3
"""Generate and send the Main Strategy V2 production decision report.

This is the cron-facing delivery hook for the cross-market production decision
report under reports/review_dashboard/main_strategy_v2/{date}/.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


STACK_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_ROOT = STACK_ROOT / "quant-research-v1"
QUANT_V1_SRC = QUANT_V1_ROOT / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.delivery.gmail import send_report_email  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send Main Strategy V2 production decision report.")
    parser.add_argument("--date", required=True, help="Report date, YYYY-MM-DD.")
    parser.add_argument("--start", default="2026-03-01", help="Backtest start date.")
    parser.add_argument("--session", default="morning", help="Session label for subject, e.g. morning/evening/pre/post.")
    parser.add_argument("--delivery-mode", choices=["test", "prod"], default=os.environ.get("QUANT_DELIVERY_MODE", "test"))
    parser.add_argument("--test-recipient", default=os.environ.get("QUANT_TEST_RECIPIENT"))
    parser.add_argument("--skip-generate", action="store_true", help="Send an already-generated report.")
    parser.add_argument("--delivery-dry-run", action="store_true", help="Generate and resolve recipients, but skip Gmail.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen; skip generation and Gmail.")
    return parser.parse_args()


def _split_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _list_config_recipients(value: object) -> list[str]:
    if isinstance(value, str):
        return _split_recipients(value)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _reporting_config() -> dict[str, Any]:
    path = QUANT_V1_ROOT / "config.yaml"
    if not path.exists():
        return {}
    cfg = yaml.safe_load(path.read_text()) or {}
    reporting = cfg.get("reporting", {})
    return reporting if isinstance(reporting, dict) else {}


def _resolve_test_recipients(test_recipient: str | None) -> tuple[list[str], str]:
    override = _split_recipients(test_recipient) or _split_recipients(os.environ.get("QUANT_TEST_RECIPIENT"))
    if override:
        return override, "override"

    reporting = _reporting_config()
    configured = _list_config_recipients(reporting.get("test_recipients"))
    if not configured:
        configured = _list_config_recipients(reporting.get("test_recipient"))
    if configured:
        return configured, "config.reporting.test_recipients"

    raise SystemExit(
        "Test delivery needs --test-recipient, QUANT_TEST_RECIPIENT, or reporting.test_recipients."
    )


def _prod_recipient_count() -> int:
    return len(_list_config_recipients(_reporting_config().get("recipients")))


def report_path(as_of: str) -> Path:
    return STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of / "main_strategy_v2_backtest.md"


def report_json_path(as_of: str) -> Path:
    return STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of / "main_strategy_v2_backtest.json"


def generate_report(as_of: str, start: str) -> None:
    subprocess.run(
        [
            sys.executable,
            str(STACK_ROOT / "scripts" / "run_main_strategy_v2_backtest.py"),
            "--date",
            as_of,
            "--start",
            start,
        ],
        cwd=STACK_ROOT,
        check=True,
    )


def subject_for(as_of: str, session: str) -> str:
    normalized = session.lower()
    if normalized in {"pre", "morning"}:
        label = "盘前"
    elif normalized in {"post", "evening"}:
        label = "盘后"
    else:
        label = session
    return f"量化{label}生产决策 — {as_of}"


def load_headline(as_of: str) -> str:
    path = report_json_path(as_of)
    if not path.exists():
        return "-"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "-"
    return (((payload.get("production_decision_summary") or {}).get("summary") or {}).get("headline")) or "-"


def main() -> None:
    args = parse_args()
    path = report_path(args.date)
    subject = subject_for(args.date, args.session)

    if args.dry_run:
        print(f"Dry run: would generate {path}")
    elif not args.skip_generate:
        generate_report(args.date, args.start)

    if not args.dry_run and not path.exists():
        raise FileNotFoundError(f"production decision report not found: {path}")

    if args.delivery_mode == "test":
        recipients, source = _resolve_test_recipients(args.test_recipient)
        send_to = recipients[0]
        send_bcc = recipients[1:]
        effective_subject = f"[TEST] {subject}"
        delivery_note = f"test recipients from {source}: {len(recipients)}"
    else:
        send_to = None
        send_bcc = None
        effective_subject = subject
        delivery_note = f"prod recipients from config.reporting.recipients: {_prod_recipient_count()}"

    print(f"Report: {path}")
    print(f"Subject: {effective_subject}")
    print(f"Delivery: {args.delivery_mode} ({delivery_note})")
    print(f"Headline: {load_headline(args.date)}")

    if args.dry_run or args.delivery_dry_run:
        print("Gmail send skipped")
        return

    msg_ids = send_report_email(
        report_path=path,
        chart_paths=[],
        to=send_to,
        bcc=send_bcc,
        subject=effective_subject,
        credentials_path=QUANT_V1_ROOT / "credentials.json",
        token_path=QUANT_V1_ROOT / "token.json",
        config_path=str(QUANT_V1_ROOT / "config.yaml"),
    )
    print(f"Sent production decision report: {len(msg_ids)} message(s) {','.join(msg_ids)}")


if __name__ == "__main__":
    main()
