#!/usr/bin/env python3
"""Send A-share research report via Gmail API with inline chart images.

Usage:
    python scripts/send_email.py reports/2026-03-13_report_zh.md
    python scripts/send_email.py reports/2026-03-13_report_zh.md --charts reports/charts/2026-03-13/

Requires:
    - credentials.json (OAuth client ID) in project root
    - Recipients read from config.yaml -> reporting.recipients
    - pip install google-auth google-auth-oauthlib google-api-python-client
      pyyaml markdown
"""

import os
import sys
import argparse
import base64
import re
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from datetime import datetime

import yaml
import markdown
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]
PROJECT_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_FILE = PROJECT_DIR / "credentials.json"
TOKEN_FILE = PROJECT_DIR / "token.json"

# Chart display order and Chinese titles
CHART_TITLES = {
    "index_trends": "主要指数走势",
    "volatility": "沪深300波动率",
    "notable_items": "信号强度排名",
    "info_score_breakdown": "信息得分分解",
    "sector_flow": "行业资金流向",
    "fund_flow_trend": "资金流向趋势",
}

CHART_ORDER = [
    "index_trends",
    "volatility",
    "notable_items",
    "info_score_breakdown",
    "sector_flow",
    "fund_flow_trend",
]

# Map charts to report sections (insert after matching h2)
CHART_SECTION_MAP = {
    "index_trends": "市场状态",
    "volatility": "市场状态",
    "notable_items": "高置信信号",
    "info_score_breakdown": "主题观察",
    "sector_flow": "主题观察",
    "fund_flow_trend": "风险地图",
}


def load_config(*, required: bool = True):
    config_path = PROJECT_DIR / "config.yaml"
    if not config_path.exists():
        if required:
            print(f"ERROR: {config_path} not found.")
            sys.exit(1)
        return {}
    with open(config_path) as f:
        return yaml.safe_load(f)


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


def resolve_recipients(
    cfg: dict,
    *,
    delivery_mode: str,
    test_recipient: str | None,
) -> tuple[list[str], str]:
    reporting = cfg.get("reporting", {}) if isinstance(cfg, dict) else {}
    if not isinstance(reporting, dict):
        reporting = {}

    if delivery_mode == "prod":
        recipients = _list_config_recipients(reporting.get("recipients"))
        return recipients, "config.reporting.recipients"

    override = _split_recipients(test_recipient) or _split_recipients(
        os.environ.get("QUANT_TEST_RECIPIENT")
    )
    if override:
        return override, "override"

    configured = _list_config_recipients(reporting.get("test_recipients"))
    if not configured:
        configured = _list_config_recipients(reporting.get("test_recipient"))
    if configured:
        return configured, "config.reporting.test_recipients"

    prod = [r for r in _list_config_recipients(reporting.get("recipients")) if r != "you@example.com"]
    if prod:
        return [prod[0]], "first configured production recipient"

    raise SystemExit(
        "Test delivery needs a recipient. Set --test-recipient, "
        "QUANT_TEST_RECIPIENT, or reporting.test_recipients in config.yaml."
    )


def get_gmail_service():
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                print(f"ERROR: {CREDENTIALS_FILE} not found.")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), SCOPES
            )
            print("\n=== Gmail OAuth 认证 ===")
            print("请在浏览器中打开下方链接完成授权（授权后页面会自动关闭）：\n")
            creds = flow.run_local_server(port=0, open_browser=False)
        TOKEN_FILE.write_text(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def _chart_img_html(cid: str, title: str) -> str:
    """Build inline chart HTML block."""
    return (
        f'<div style="margin:16px 0">'
        f'<p style="font-size:13px;color:#636e72;font-weight:600;margin-bottom:4px">{title}</p>'
        f'<img src="cid:{cid}" alt="{title}" '
        f'style="max-width:100%;height:auto;border-radius:8px;'
        f'box-shadow:0 2px 8px rgba(0,0,0,0.1)">'
        f'</div>\n'
    )


def _insert_charts_into_html(html_body: str, chart_paths: list[Path], cid_map: dict[str, str]) -> str:
    """Insert chart images after their matching report sections."""
    if not chart_paths or not cid_map:
        return html_body

    # Group charts by section
    section_charts: dict[str, list[str]] = {}
    unmatched: list[str] = []

    for cp in chart_paths:
        stem = cp.stem
        cid = cid_map.get(str(cp))
        if not cid:
            continue
        title = CHART_TITLES.get(stem, stem.replace("_", " ").title())
        img_html = _chart_img_html(cid, title)

        section_keyword = CHART_SECTION_MAP.get(stem)
        if section_keyword:
            section_charts.setdefault(section_keyword, []).append(img_html)
        else:
            unmatched.append(img_html)

    # Find h2 positions
    h2_pattern = re.compile(r'<h2[^>]*>(.*?)</h2>', re.IGNORECASE | re.DOTALL)
    h2_matches = list(h2_pattern.finditer(html_body))

    if not h2_matches:
        # No sections — append all at end
        for charts in section_charts.values():
            html_body += "".join(charts)
        html_body += "".join(unmatched)
        return html_body

    # Insert after matching sections (work backwards to preserve positions)
    insertions: list[tuple[int, str]] = []

    for keyword, charts_list in section_charts.items():
        matched_idx = None
        for i, m in enumerate(h2_matches):
            if keyword in m.group(1):
                matched_idx = i
                break

        if matched_idx is None:
            unmatched.extend(charts_list)
            continue

        # Insert before next h2 or at end
        if matched_idx + 1 < len(h2_matches):
            insert_pos = h2_matches[matched_idx + 1].start()
        else:
            insert_pos = len(html_body)

        insertions.append((insert_pos, "".join(charts_list)))

    insertions.sort(key=lambda x: x[0], reverse=True)
    for pos, snippet in insertions:
        html_body = html_body[:pos] + snippet + html_body[pos:]

    if unmatched:
        html_body += "".join(unmatched)

    return html_body


def build_email(report_path: str, recipients: list[str], chart_paths: list[Path] | None = None, subject: str | None = None) -> dict:
    """Build MIME message with inline charts."""
    report = Path(report_path)
    if not report.exists():
        print(f"ERROR: Report file not found: {report_path}")
        sys.exit(1)

    md_content = report.read_text(encoding="utf-8")
    chart_paths = chart_paths or []

    # Build CID map
    cid_map: dict[str, str] = {}
    for i, cp in enumerate(chart_paths):
        cid_map[str(cp)] = f"chart_{i}_{cp.stem}"

    # Convert markdown to HTML
    html_content = markdown.markdown(
        md_content,
        extensions=["tables", "fenced_code", "toc"],
    )

    # Insert charts into HTML
    html_content = _insert_charts_into_html(html_content, chart_paths, cid_map)

    # Wrap in styled template
    html_body = f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{
    font-family: -apple-system, "PingFang SC", "Microsoft YaHei", "Helvetica Neue", sans-serif;
    max-width: 900px;
    margin: 0 auto;
    padding: 20px;
    line-height: 1.8;
    color: #1a1a2e;
    background-color: #f8f9fa;
}}
.container {{
    max-width: 860px;
    margin: 0 auto;
    padding: 24px;
    background: #ffffff;
    border-radius: 8px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.05);
}}
h1 {{ color: #1a1a2e; border-bottom: 3px solid #c41e3a; padding-bottom: 8px; font-size: 22px; }}
h2 {{ color: #c41e3a; margin-top: 28px; border-bottom: 1px solid #e0e0e0; padding-bottom: 6px; font-size: 18px; }}
h3 {{ color: #2d3436; margin-top: 20px; font-size: 15px; }}
table {{
    border-collapse: collapse;
    width: 100%;
    margin: 12px 0;
    font-size: 13px;
}}
th {{
    background-color: #1a1a2e;
    color: #ffffff;
    padding: 8px 10px;
    text-align: left;
    font-weight: 600;
}}
td {{
    padding: 6px 10px;
    border-bottom: 1px solid #e8e8e8;
}}
tr:nth-child(even) {{ background-color: #f5f6fa; }}
code {{
    background: #f0f1ff;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 12px;
}}
pre {{
    background: #f5f5f5;
    padding: 12px;
    border-radius: 4px;
    overflow-x: auto;
}}
strong {{ color: #c41e3a; }}
blockquote {{
    border-left: 4px solid #c41e3a;
    margin: 12px 0;
    padding: 8px 16px;
    background: #fff5f5;
    color: #555;
    font-size: 13px;
}}
hr {{
    border: none;
    border-top: 1px solid #e0e0e0;
    margin: 20px 0;
}}
.footer {{
    margin-top: 32px;
    padding-top: 16px;
    border-top: 1px solid #e0e0e0;
    font-size: 11px;
    color: #b2bec3;
    text-align: center;
}}
</style>
</head>
<body>
<div class="container">
{html_content}
<div class="footer">
    Generated by QuantResearcher-CN &middot; 不构成投资建议
</div>
</div>
</body>
</html>"""

    date_str = report.stem.split("_")[0] if "_" in report.stem else datetime.now().strftime("%Y-%m-%d")

    # Build multipart/related message (required for CID images)
    msg = MIMEMultipart("related")
    msg["Subject"] = subject or f"A股量化研究日报 — {date_str}"
    msg["Bcc"] = ", ".join(recipients)

    # Alternative part: plain text + HTML
    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(md_content, "plain", "utf-8"))
    alt_part.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt_part)

    # Inline chart images
    for cp in chart_paths:
        cid = cid_map.get(str(cp))
        if not cid or not cp.exists():
            continue
        with open(cp, "rb") as f:
            img = MIMEImage(f.read(), _subtype="png")
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=cp.name)
        msg.attach(img)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return {"raw": raw}


def send(
    report_path: str,
    recipients: list[str],
    chart_dir: str | None = None,
    subject: str | None = None,
    *,
    delivery_mode: str = "test",
    recipient_source: str = "",
    dry_run: bool = False,
):
    """Send the report email via Gmail API."""
    if not recipients:
        print("WARNING: No recipients configured in config.yaml -> reporting.recipients")
        return

    # Find chart PNGs
    chart_paths: list[Path] = []
    if chart_dir:
        chart_dir_p = Path(chart_dir)
        if chart_dir_p.is_dir():
            # Sort by CHART_ORDER
            for stem in CHART_ORDER:
                p = chart_dir_p / f"{stem}.png"
                if p.exists():
                    chart_paths.append(p)
            # Add any remaining charts not in CHART_ORDER
            for p in sorted(chart_dir_p.glob("*.png")):
                if p not in chart_paths:
                    chart_paths.append(p)

    print(f"  Report:     {report_path}")
    print(f"  Charts:     {len(chart_paths)} images")
    print(f"  Mode:       {delivery_mode}")
    if recipient_source:
        print(f"  Source:     {recipient_source}")
    print(f"  Recipients: {', '.join(recipients)}")
    if dry_run:
        print("  Dry run: Gmail call skipped")
        return

    service = get_gmail_service()
    message = build_email(report_path, recipients, chart_paths, subject=subject)

    result = service.users().messages().send(userId="me", body=message).execute()
    msg_id = result.get("id", "unknown")
    print(f"  Sent successfully. Message ID: {msg_id}")


def main():
    parser = argparse.ArgumentParser(description="Send A-share daily report email")
    parser.add_argument("report_path")
    parser.add_argument("--charts", dest="chart_dir", default=None)
    parser.add_argument("--subject", dest="subject_override", default=None)
    parser.add_argument("--delivery-mode", choices=["test", "prod"],
                        default=os.environ.get("QUANT_DELIVERY_MODE", "test"),
                        help="test sends only to the test recipient; prod uses config recipients")
    parser.add_argument("--test-recipient", default=None,
                        help="Comma-separated test recipient override")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print resolved delivery targets without calling Gmail")
    args = parser.parse_args()

    report_path = args.report_path
    chart_dir = args.chart_dir
    subject_override = args.subject_override

    # Auto-detect chart dir from report date
    if not chart_dir:
        report_name = Path(report_path).stem
        date_str = report_name.split("_")[0]
        auto_dir = PROJECT_DIR / "reports" / "charts" / date_str
        if auto_dir.is_dir():
            chart_dir = str(auto_dir)
            print(f"  Auto-detected charts: {chart_dir}")

    has_test_override = bool(
        _split_recipients(args.test_recipient)
        or _split_recipients(os.environ.get("QUANT_TEST_RECIPIENT"))
    )
    cfg = load_config(required=args.delivery_mode == "prod" or not has_test_override)
    recipients, source = resolve_recipients(
        cfg,
        delivery_mode=args.delivery_mode,
        test_recipient=args.test_recipient,
    )
    if args.delivery_mode == "test":
        if subject_override:
            subject_override = f"[TEST] {subject_override}"
        else:
            date_str = Path(report_path).stem.split("_")[0]
            subject_override = f"[TEST] A股量化研究日报 — {date_str}"
    send(
        report_path,
        recipients,
        chart_dir,
        subject=subject_override,
        delivery_mode=args.delivery_mode,
        recipient_source=source,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
