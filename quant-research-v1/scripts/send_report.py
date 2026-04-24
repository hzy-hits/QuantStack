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
import sys
from datetime import date
from pathlib import Path

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
    print()

    for report_path, subject in reports:
        print(f"{'Sending' if args.send else 'Drafting'}: {report_path.name}")

        if args.send:
            msg_ids = send_report_email(
                report_path=report_path,
                chart_paths=chart_paths,
                to=args.to,
                subject=subject,
            )
            print(f"  Sent successfully ({len(msg_ids)} message)")
        else:
            draft_id = create_report_draft(
                report_path=report_path,
                chart_paths=chart_paths,
                to=args.to or "",
                subject=subject,
            )
            print(f"  Draft created: {draft_id}")

    print("\nDone.")


if __name__ == "__main__":
    main()
