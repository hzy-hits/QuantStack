#!/usr/bin/env python3
"""
Send the daily report email with inline charts.

Usage:
    # Send Chinese post-market report (default)
    python scripts/send_report.py --send --date 2026-03-09 --session post

    # Send pre-market report
    python scripts/send_report.py --send --date 2026-03-09 --session pre

    # Draft mode (review before sending)
    python scripts/send_report.py --date 2026-03-09

    # Override recipient (skips config)
    python scripts/send_report.py --send --to someone@example.com
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quant_bot.delivery.gmail import send_report_email, create_report_draft

SESSION_LABELS = {
    "post": "盘后",
    "pre": "盘前",
    "weekly": "周报",
}


def _find_charts(as_of: str, session: str) -> list[Path]:
    charts_dir = Path("reports") / "charts" / as_of / session
    if not charts_dir.exists():
        charts_dir = Path("reports") / "charts" / as_of
    if not charts_dir.exists():
        return []
    return sorted(charts_dir.glob("*.png"))


def _split_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _reporting_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        return {}
    cfg = yaml.safe_load(path.read_text()) or {}
    reporting = cfg.get("reporting", {})
    return reporting if isinstance(reporting, dict) else {}


def _list_config_recipients(value: object) -> list[str]:
    if isinstance(value, str):
        return _split_recipients(value)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _resolve_test_recipients(args: argparse.Namespace) -> tuple[list[str], str]:
    override = _split_recipients(args.test_recipient) or _split_recipients(
        os.environ.get("QUANT_TEST_RECIPIENT")
    )
    if override:
        return override, "override"

    reporting = _reporting_config()
    configured = _list_config_recipients(reporting.get("test_recipients"))
    if not configured:
        configured = _list_config_recipients(reporting.get("test_recipient"))
    if configured:
        return configured, "config.reporting.test_recipients"

    if args.to:
        return [args.to], "--to"

    raise SystemExit(
        "Test delivery needs a recipient. Set --test-recipient, "
        "QUANT_TEST_RECIPIENT, reporting.test_recipients in config.yaml, "
        "or --to for an explicit one-off test."
    )


def main():
    parser = argparse.ArgumentParser(description="Send daily report email")
    parser.add_argument("--date", type=str, default=None, help="Report date (default: today)")
    parser.add_argument("--session", choices=["post", "pre", "weekly"], default="post",
                        help="Session: post (post-market), pre (pre-market), or weekly")
    parser.add_argument("--lang", choices=["zh", "en", "both"], default="zh",
                        help="Which report(s) to send (default: zh)")
    parser.add_argument("--send", action="store_true", help="Send directly (default: draft)")
    parser.add_argument("--to", type=str, default=None,
                        help="Override recipient (skips config list)")
    parser.add_argument("--delivery-mode", choices=["test", "prod"],
                        default=os.environ.get("QUANT_DELIVERY_MODE", "test"),
                        help="test sends only to the test recipient; prod uses config recipients")
    parser.add_argument("--test-recipient", type=str, default=None,
                        help="Comma-separated test recipient override")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print resolved delivery targets without calling Gmail")
    args = parser.parse_args()

    if args.date:
        as_of = args.date
    else:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        as_of = str(datetime.now(ZoneInfo("America/New_York")).date())
    session = args.session
    session_label = SESSION_LABELS[session]
    chart_paths = _find_charts(as_of, session)

    reports: list[tuple[Path, str]] = []

    if args.lang in ("zh", "both"):
        # Try session-specific filename first, fall back to legacy
        if session == "weekly":
            p = Path("reports") / f"{as_of}_report_weekly_zh.md"
        else:
            p = Path("reports") / f"{as_of}_report_zh_{session}.md"
            if not p.exists():
                p = Path("reports") / f"{as_of}_report_zh.md"
        if p.exists():
            label = "周报" if session == "weekly" else f"日报·{session_label}"
            reports.append((p, f"量化研究{label} — {as_of}"))
        else:
            print(f"Warning: no Chinese report found for {as_of} ({session})")

    if args.lang in ("en", "both"):
        p = Path("reports") / f"{as_of}_report_claude.md"
        if p.exists():
            reports.append((p, f"Quant Research Report — {as_of}"))
        else:
            print(f"Warning: {p} not found")

    if not reports:
        print("Error: No reports found", file=sys.stderr)
        sys.exit(1)

    print(f"Date: {as_of} ({session_label})")
    print(f"Charts: {len(chart_paths)} files")
    print(f"Mode: {'SEND' if args.send else 'DRAFT'}")
    print(f"Delivery mode: {args.delivery_mode.upper()}")
    print()

    for report_path, subject in reports:
        send_to = args.to
        send_bcc: list[str] | None = None
        delivery_note = "config.reporting.recipients"
        effective_subject = subject
        if args.delivery_mode == "test":
            recipients, source = _resolve_test_recipients(args)
            send_to = recipients[0]
            send_bcc = recipients[1:]
            delivery_note = source
            effective_subject = f"[TEST] {subject}"
        elif args.to:
            delivery_note = "--to override"

        print(f"{'Sending' if args.send else 'Drafting'}: {report_path.name}")
        print(f"  Delivery: {args.delivery_mode} ({delivery_note})")
        if args.delivery_mode == "test":
            print(f"  Recipients: {', '.join([send_to, *(send_bcc or [])])}")

        if args.dry_run:
            print("  Dry run: Gmail call skipped")
            continue

        if args.send:
            msg_ids = send_report_email(
                report_path=report_path,
                chart_paths=chart_paths,
                to=send_to,
                bcc=send_bcc,
                subject=effective_subject,
            )
            print(f"  Sent successfully ({len(msg_ids)} message)")
        else:
            draft_id = create_report_draft(
                report_path=report_path,
                chart_paths=chart_paths,
                to=send_to or "",
                bcc=send_bcc,
                subject=effective_subject,
            )
            print(f"  Draft created: {draft_id}")

    print("\nDone.")


if __name__ == "__main__":
    main()
