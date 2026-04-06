#!/usr/bin/env python3
"""Send a pipeline failure alert email.

Usage:
    python scripts/send_alert.py --to user@example.com --subject "..." --body "..."
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quant_bot.delivery.gmail import send_alert_email


def main():
    parser = argparse.ArgumentParser(description="Send pipeline alert email")
    parser.add_argument("--to", required=True, help="Recipient email")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--body", required=True, help="Email body text")
    args = parser.parse_args()

    send_alert_email(to=args.to, subject=args.subject, body=args.body)


if __name__ == "__main__":
    main()
