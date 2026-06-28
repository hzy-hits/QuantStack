"""
Gmail delivery with inline chart images.

Converts markdown report → styled HTML email → sends via Gmail API
with chart PNGs embedded as CID inline attachments.

Usage:
    from quant_bot.delivery.gmail import send_report_email, create_report_draft

    # Send directly
    send_report_email(
        report_md=Path("reports/2026-03-08_report.md"),
        chart_paths=[Path("reports/charts/2026-03-08/sector_performance.png"), ...],
        to="you@example.com",
        subject="Quant Research Report — 2026-03-08",
    )

    # Or create draft
    create_report_draft(
        report_md=Path("reports/2026-03-08_report.md"),
        chart_paths=[...],
        to="you@example.com",
        subject="Quant Research Report — 2026-03-08",
    )
"""
from __future__ import annotations

import base64
import mimetypes
import os
import re
import time
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.policy import SMTP
from email.utils import formataddr


EMAIL_POLICY = SMTP.clone(max_line_length=998)


def _format_sender(name: str | None, addr: str) -> str:
    """Build an RFC 5322 From value with an RFC 2047-encoded display name.

    Sending `From: me` lets Gmail fill the display name server-side; when the
    account name is non-ASCII (e.g. 黄振宇) some delivery paths surface it as
    raw UTF-8 bytes → mojibake (é»„æŒ¯å®‡). Setting the header ourselves with an
    explicitly encoded name removes that dependency.
    """
    if not addr or addr == "me":
        return addr or "me"
    if not name:
        return addr
    try:
        name.encode("ascii")
        return formataddr((name, addr))
    except UnicodeEncodeError:
        return formataddr((str(Header(name, "utf-8").encode()), addr))


def _encode_subject(subject: str) -> str:
    """RFC 2047 encode any non-ASCII subject so Gmail/mail clients render
    Chinese (and em-dash etc.) correctly instead of mojibake.

    ASCII-only subjects pass through unchanged so existing English headers
    are not touched.
    """
    try:
        subject.encode("ascii")
        return subject
    except UnicodeEncodeError:
        return Header(subject, "utf-8").encode()
from pathlib import Path

import markdown
from premailer import transform

import yaml
import structlog

log = structlog.get_logger()

# ── OAuth constants ──────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]
GMAIL_API_RETRY_ATTEMPTS = 5
GMAIL_API_RETRY_INITIAL_DELAY_SECONDS = 3.0


def _is_retryable_gmail_error(exc: Exception) -> bool:
    status = getattr(getattr(exc, "resp", None), "status", None)
    if status in {408, 429, 500, 502, 503, 504}:
        return True

    text = repr(exc).lower()
    retry_markers = (
        "ssl",
        "eof occurred",
        "timeout",
        "timed out",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "max retries exceeded",
        "remote end closed connection",
        "broken pipe",
    )
    return any(marker in text for marker in retry_markers)


def _execute_gmail_call(label: str, func, *, attempts: int = GMAIL_API_RETRY_ATTEMPTS):
    delay = GMAIL_API_RETRY_INITIAL_DELAY_SECONDS
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:
            if attempt >= attempts or not _is_retryable_gmail_error(exc):
                raise
            log.warning(
                "gmail_api_retry",
                label=label,
                attempt=attempt,
                attempts=attempts,
                delay_seconds=delay,
                error=str(exc),
            )
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def load_recipients(config_path: str = "config.yaml") -> list[str]:
    """Load recipient list from config.yaml → reporting.recipients.
    NEVER hallucinate email addresses — always read from config."""
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}. Cannot determine recipients.")
    cfg = yaml.safe_load(p.read_text())
    recipients = cfg.get("reporting", {}).get("recipients", [])
    if not recipients or recipients == ["you@example.com"]:
        raise ValueError("No recipients configured. Edit config.yaml → reporting.recipients")
    return recipients


def load_sender_name(config_path: str = "config.yaml") -> str | None:
    """Optional sender display name from config.yaml → reporting.sender_name."""
    p = Path(config_path)
    if not p.exists():
        return None
    try:
        cfg = yaml.safe_load(p.read_text()) or {}
    except yaml.YAMLError:
        return None
    name = (cfg.get("reporting", {}) or {}).get("sender_name")
    return str(name).strip() or None if name else None


def _read_key_value_file(path: Path | str | None) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    values: dict[str, str] = {}
    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _resend_config_value(name: str, config_path: str = "config.yaml") -> str:
    """Resolve Resend config without requiring secrets in git.

    Resolution order:
    1. Process env, e.g. RESEND_API_KEY / RESEND_FROM_EMAIL.
    2. RESEND_ENV_FILE key-value file, e.g. Multica's /home/ubuntu/apps/multica/.env.
    3. RESEND_API_KEY_FILE key-value file, matching ops-bot's secret-file style.
    4. reporting.<lowercase name> in quant config for non-secret values.
    5. Oracle compatibility fallback: /home/ubuntu/apps/multica/.env if present.
    """
    direct = os.environ.get(name, "").strip()
    if direct:
        return direct

    for env_name in ("RESEND_ENV_FILE", "RESEND_API_KEY_FILE"):
        value = _read_key_value_file(os.environ.get(env_name)).get(name, "").strip()
        if value:
            return value

    reporting: dict = {}
    p = Path(config_path)
    if p.exists():
        try:
            cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            reporting = cfg.get("reporting", {}) if isinstance(cfg.get("reporting"), dict) else {}
        except yaml.YAMLError:
            reporting = {}
    config_value = str(reporting.get(name.lower()) or reporting.get(name) or "").strip()
    if config_value:
        return config_value

    oracle_multica_env = Path("/home/ubuntu/apps/multica/.env")
    return _read_key_value_file(oracle_multica_env).get(name, "").strip()
TOKEN_PATH = Path("token.json")
CREDENTIALS_PATH = Path("credentials.json")


# ── HTML email template ──────────────────────────────────────────────────────
EMAIL_CSS = """
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    line-height: 1.6;
    color: #1a1a2e;
    background-color: #f8f9fa;
    margin: 0;
    padding: 0;
}
.container {
    max-width: 800px;
    margin: 0 auto;
    padding: 24px;
    background: #ffffff;
}
h1 {
    color: #1a1a2e;
    border-bottom: 3px solid #3742fa;
    padding-bottom: 8px;
    font-size: 22px;
}
h2 {
    color: #1a1a2e;
    border-bottom: 1px solid #e0e0e0;
    padding-bottom: 6px;
    margin-top: 28px;
    font-size: 18px;
}
h3 {
    color: #2d3436;
    margin-top: 20px;
    font-size: 15px;
}
table {
    border-collapse: collapse;
    width: 100%;
    margin: 12px 0;
    font-size: 13px;
}
th {
    background-color: #1a1a2e;
    color: #ffffff;
    padding: 8px 10px;
    text-align: left;
    font-weight: 600;
}
td {
    padding: 6px 10px;
    border-bottom: 1px solid #e8e8e8;
}
tr:nth-child(even) {
    background-color: #f5f6fa;
}
blockquote {
    border-left: 4px solid #3742fa;
    margin: 12px 0;
    padding: 8px 16px;
    background: #f0f1ff;
    color: #555;
    font-size: 13px;
}
code {
    background: #f0f1ff;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 12px;
}
strong {
    color: #1a1a2e;
}
hr {
    border: none;
    border-top: 1px solid #e0e0e0;
    margin: 20px 0;
}
img.chart {
    max-width: 100%;
    height: auto;
    border-radius: 8px;
    margin: 12px 0;
    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}
.chart-title {
    font-size: 13px;
    color: #636e72;
    margin-bottom: 4px;
    font-weight: 600;
}
.footer {
    margin-top: 32px;
    padding-top: 16px;
    border-top: 1px solid #e0e0e0;
    font-size: 11px;
    color: #b2bec3;
    text-align: center;
}
ul, ol {
    padding-left: 24px;
}
li {
    margin-bottom: 4px;
}
"""

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>{css}</style>
</head>
<body>
<div class="container">
{body}
<div class="footer">
    Generated by quant-research-v1 &middot; Not financial advice
</div>
</div>
</body>
</html>"""


# ── Markdown → HTML conversion ──────────────────────────────────────────────

CHART_TITLES = {
    "sector_performance": "Sector Performance",
    "notable_items": "Top Notable Items",
    "cross_asset": "Cross-Asset Dashboard",
    "dyp_candidates": "Dividend Yield Dip Candidates",
    "index_trends": "Major Index Trends (60D)",
    "vix_trend": "VIX — Market Fear Gauge",
    "top_movers": "Top Movers — Winners & Losers",
}

# Map chart stems to the h2 heading they should appear AFTER.
# Charts are inserted after the first <h2> whose text contains the keyword.
# If no match, the chart goes to a fallback section at the end.
CHART_SECTION_MAP = {
    "sector_performance": "Market Context",
    "index_trends": "Market Context",
    "vix_trend": "Market Context",
    "notable_items": "HIGH CONFIDENCE",
    "top_movers": "MODERATE",
    "cross_asset": "Cross-Asset",
    "dyp_candidates": "Dividend",
}


def _chart_img_html(cid: str, title: str) -> str:
    """Build inline chart HTML block."""
    return (
        f'<p style="font-size:13px;color:#636e72;font-weight:600;'
        f'margin-bottom:4px;margin-top:16px">{title}</p>\n'
        f'<img src="cid:{cid}" alt="{title}" '
        f'style="max-width:100%;height:auto;border-radius:8px;'
        f'margin:0 0 20px 0;box-shadow:0 2px 8px rgba(0,0,0,0.1)">\n'
    )


def _insert_charts_after_sections(
    html_body: str,
    chart_paths: list[Path],
    chart_cid_map: dict[str, str],
) -> str:
    """
    Insert chart images after their matching report sections.
    Charts are placed after the LAST element before the next <h2>,
    i.e. at the end of the section they belong to.
    Unmatched charts go to a fallback section at the bottom.
    """
    if not chart_paths or not chart_cid_map:
        return html_body

    # Build insertion map: section_keyword → list of chart HTML snippets
    section_charts: dict[str, list[str]] = {}
    unmatched: list[str] = []

    for cp in chart_paths:
        cid = chart_cid_map.get(str(cp))
        if not cid:
            continue
        stem = Path(cp).stem
        title = CHART_TITLES.get(stem, stem.replace("_", " ").title())
        img_html = _chart_img_html(cid, title)

        keyword = CHART_SECTION_MAP.get(stem)
        if keyword:
            section_charts.setdefault(keyword, []).append(img_html)
        else:
            unmatched.append(img_html)

    # Find all <h2> positions to determine section boundaries
    h2_pattern = re.compile(r'<h2[^>]*>(.*?)</h2>', re.IGNORECASE | re.DOTALL)
    h2_matches = list(h2_pattern.finditer(html_body))

    if not h2_matches:
        # No sections found — append everything at the end
        for charts in section_charts.values():
            html_body += "".join(charts)
        html_body += "".join(unmatched)
        return html_body

    # For each section keyword, find where it matches and insert charts
    # Work backwards so insertion positions stay valid
    insertions: list[tuple[int, str]] = []  # (position, html_to_insert)

    for keyword, charts_html_list in section_charts.items():
        # Find the h2 that contains this keyword
        matched_idx = None
        for i, m in enumerate(h2_matches):
            if keyword.lower() in m.group(1).lower():
                matched_idx = i
                break

        if matched_idx is None:
            unmatched.extend(charts_html_list)
            continue

        # Insert position = start of next <h2>, or end of body
        if matched_idx + 1 < len(h2_matches):
            # Find the <hr> or whitespace just before the next h2
            next_h2_start = h2_matches[matched_idx + 1].start()
            # Look for preceding <hr> tag to insert before it
            preceding = html_body[max(0, next_h2_start - 80):next_h2_start]
            hr_pos = preceding.rfind('<hr')
            if hr_pos >= 0:
                insert_pos = max(0, next_h2_start - 80) + hr_pos
            else:
                insert_pos = next_h2_start
        else:
            insert_pos = len(html_body)

        insertions.append((insert_pos, "".join(charts_html_list)))

    # Sort insertions by position descending so we don't shift indices
    insertions.sort(key=lambda x: x[0], reverse=True)
    for pos, html_snippet in insertions:
        html_body = html_body[:pos] + html_snippet + html_body[pos:]

    # Append any unmatched charts at the end
    if unmatched:
        html_body += "".join(unmatched)

    return html_body


def _md_to_html(
    md_text: str,
    chart_cid_map: dict[str, str],
    chart_paths: list[Path],
) -> str:
    """
    Convert markdown to styled HTML email.

    1. Replace any ![title](path) in the markdown with CID images.
    2. Insert remaining charts inline after their matching report sections.
    3. Any unmatched charts go to the bottom.
    """
    # Replace markdown image refs if present
    already_placed = set()

    def _replace_image(match):
        alt = match.group(1)
        img_path = match.group(2)
        for path_str, cid in chart_cid_map.items():
            if Path(img_path).name == Path(path_str).name:
                already_placed.add(path_str)
                return (
                    f'<p class="chart-title">{alt}</p>\n'
                    f'<img src="cid:{cid}" alt="{alt}" class="chart"'
                    f' style="max-width:100%;height:auto;border-radius:8px;'
                    f'margin:12px 0;box-shadow:0 2px 8px rgba(0,0,0,0.1)">'
                )
        return f"*[Chart: {alt}]*"

    md_text = re.sub(r'!\[([^\]]*)\]\(([^)]+)\)', _replace_image, md_text)

    # Convert markdown to HTML
    html_body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code"],
    )

    # Insert charts that weren't already placed via markdown refs
    remaining = [cp for cp in chart_paths if str(cp) not in already_placed]
    html_body = _insert_charts_after_sections(html_body, remaining, chart_cid_map)

    # Wrap in template with inline styles
    full_html = HTML_TEMPLATE.format(css=EMAIL_CSS, body=html_body)

    # Inline CSS for email client compatibility
    try:
        full_html = transform(full_html, remove_classes=False)
    except Exception as e:
        log.warning("premailer_failed", error=str(e))

    return full_html


# ── MIME message building ────────────────────────────────────────────────────

def build_email_message(
    to: str,
    subject: str,
    html: str,
    chart_paths: list[Path],
    chart_cid_map: dict[str, str],
    sender: str = "me",
    bcc: list[str] | None = None,
) -> MIMEMultipart:
    """Build a MIME message; structure depends on whether inline images exist.

    When there ARE inline charts, use the RFC 2387 nested structure:
        multipart/related
        ├── multipart/alternative
        │   ├── text/plain
        │   └── text/html (with cid: refs)
        └── image/png (Content-ID)

    When there are NO charts, the top level is `multipart/alternative`
    directly. A `multipart/related` wrapper with zero related parts is what
    several Chinese mail clients (QQ Mail / 163 / Foxmail) mishandle — they
    fail to descend into the inner alternative and render the raw base64 body
    as 乱码. Only wrap in `related` when an image actually needs relating.
    """
    # Resolve which charts are real, attachable images first.
    images: list[tuple[MIMEImage, str]] = []
    for chart_path in chart_paths:
        path = Path(chart_path)
        if not path.exists():
            continue
        cid = chart_cid_map.get(str(path))
        if not cid:
            continue
        mime_type, _ = mimetypes.guess_type(str(path))
        if not mime_type or not mime_type.startswith("image/"):
            mime_type = "image/png"
        with open(path, "rb") as f:
            img = MIMEImage(f.read(), _subtype=mime_type.split("/")[1])
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=path.name)
        images.append((img, path.name))

    # Plain-text fallback (strip tags roughly).
    plain = re.sub(r'<[^>]+>', '', html)
    plain = re.sub(r'\n{3,}', '\n\n', plain).strip()
    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(plain[:5000], "plain", "utf-8"))
    alt_part.attach(MIMEText(html, "html", "utf-8"))

    if images:
        msg: MIMEMultipart = MIMEMultipart("related")
        msg.attach(alt_part)
        for img, _name in images:
            msg.attach(img)
    else:
        # No inline images → the alternative IS the message.
        msg = alt_part

    msg["Subject"] = _encode_subject(subject)
    msg["From"] = sender
    if to:
        msg["To"] = to
    elif bcc:
        msg["To"] = "undisclosed-recipients:;"
    if bcc:
        msg["Bcc"] = ", ".join(bcc)

    return msg


# ── Gmail API helpers ────────────────────────────────────────────────────────

def _get_gmail_service(
    credentials_path: Path = CREDENTIALS_PATH,
    token_path: Path = TOKEN_PATH,
):
    """Authenticate and return Gmail API service object."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                _execute_gmail_call("token_refresh", lambda: creds.refresh(Request()))
            except Exception as e:
                log.error("gmail_token_refresh_failed", error=str(e))
                # Only discard the token on explicit invalid/revoked states.
                # Transient transport errors (SSL, timeout, EOF) should not wipe
                # the shared token file for every downstream pipeline.
                error_text = str(e).lower()
                if (
                    "invalid_grant" in error_text
                    or "revoked" in error_text
                    or "expired or revoked" in error_text
                ):
                    token_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Gmail token refresh failed: {e}\n"
                    "Re-run manually to re-authenticate if the token is truly invalid:\n"
                    "  uv run python scripts/send_report.py --date YYYY-MM-DD"
                ) from e
        else:
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Gmail credentials not found at {credentials_path}.\n"
                    "Download OAuth 2.0 Client ID JSON from Google Cloud Console → APIs → Credentials.\n"
                    "Save as credentials.json in project root."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            print("\n=== Gmail OAuth 认证 ===")
            print("请在浏览器中打开下方链接完成授权（授权后页面会自动关闭）：\n")
            creds = flow.run_local_server(port=0, open_browser=False)

        token_path.write_text(creds.to_json())
        log.info("gmail_token_saved", path=str(token_path))

    return build("gmail", "v1", credentials=creds)


def _encode_message(msg: MIMEMultipart) -> dict:
    """Encode MIME message for Gmail API."""
    raw = base64.urlsafe_b64encode(msg.as_bytes(policy=EMAIL_POLICY)).decode("utf-8")
    return {"raw": raw}


# ── Public API ───────────────────────────────────────────────────────────────

def prepare_email(
    report_path: Path | str,
    chart_paths: list[Path] | None = None,
    to: str = "",
    subject: str = "",
    bcc: list[str] | None = None,
    sender: str = "me",
) -> tuple[MIMEMultipart, dict[str, str]]:
    """
    Prepare email message from a markdown report file + chart images.
    Returns (MIME message, cid_map).
    """
    report_path = Path(report_path)
    md_text = report_path.read_text(encoding="utf-8")
    chart_paths = chart_paths or []

    # Build CID map: path_str → content_id
    chart_cid_map: dict[str, str] = {}
    for i, cp in enumerate(chart_paths):
        cid = f"chart_{i}_{Path(cp).stem}"
        chart_cid_map[str(cp)] = cid

    # Convert markdown → HTML with embedded CID references + appended charts
    html = _md_to_html(md_text, chart_cid_map, chart_paths)

    # Build MIME message
    msg = build_email_message(
        to, subject, html, chart_paths, chart_cid_map, sender=sender, bcc=bcc
    )

    return msg, chart_cid_map


def send_report_email(
    report_path: Path | str,
    chart_paths: list[Path] | None = None,
    to: str | None = None,
    subject: str = "",
    bcc: list[str] | None = None,
    credentials_path: Path = CREDENTIALS_PATH,
    token_path: Path = TOKEN_PATH,
    config_path: str = "config.yaml",
) -> list[str]:
    """
    Send a markdown report as a styled HTML email with inline charts.
    Recipients are read from config.yaml by default. Returns list of message IDs.
    """
    if to:
        direct_to = to
        bcc_recipients: list[str] = bcc or []
        log.info("direct_recipient_override", has_bcc=bool(bcc_recipients))
    else:
        direct_to = ""
        bcc_recipients = load_recipients(config_path)
        log.info("recipients_from_config", count=len(bcc_recipients))

    import socket
    service = _get_gmail_service(credentials_path, token_path)
    msg_ids = []

    # Resolve the authenticated account address once — used both as the
    # visible To on Bcc sends AND as the explicit From address so we never
    # rely on Gmail filling an unencoded non-ASCII display name.
    account_addr = ""
    try:
        profile = _execute_gmail_call(
            "profile_lookup",
            lambda: service.users().getProfile(userId="me").execute(),
            attempts=3,
        )
        account_addr = profile.get("emailAddress", "")
    except Exception as e:
        log.warning("gmail_profile_lookup_failed", error=str(e))

    # Gmail API rejects an empty/placeholder To header on single-message Bcc sends.
    visible_to = direct_to or account_addr

    # Explicit, RFC 2047-encoded From — fixes garbled sender name (é»„æŒ¯å®‡).
    sender = _format_sender(load_sender_name(config_path), account_addr or "me")

    # Set socket-level timeout to prevent indefinite hangs on Gmail API calls
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(60)  # 60s per API call
    try:
        msg, _ = prepare_email(
            report_path,
            chart_paths,
            visible_to,
            subject,
            bcc=bcc_recipients,
            sender=sender,
        )
        result = _execute_gmail_call(
            "message_send",
            lambda: service.users().messages().send(
                userId="me", body=_encode_message(msg)
            ).execute(),
        )
        msg_id = result.get("id", "")
        msg_ids.append(msg_id)
        log.info(
            "gmail_sent",
            to=visible_to or "undisclosed-recipients",
            bcc_count=len(bcc_recipients),
            message_id=msg_id,
        )
    finally:
        socket.setdefaulttimeout(old_timeout)

    log.info("gmail_send_complete", messages=len(msg_ids), bcc_count=len(bcc_recipients))
    return msg_ids


def send_report_email_resend(
    report_path: Path | str,
    chart_paths: list[Path] | None = None,
    to: str | None = None,
    subject: str = "",
    bcc: list[str] | None = None,
    config_path: str = "config.yaml",
) -> list[str]:
    """Send a markdown report through Resend.

    This is intentionally API-compatible with send_report_email for the daily
    pipeline. It supports Multica's RESEND_ENV_FILE style and ops-bot's
    RESEND_API_KEY_FILE style; no secret is read from git-tracked files.
    """
    import requests

    if to:
        direct_to = [to]
        bcc_recipients: list[str] = bcc or []
        log.info("resend_direct_recipient_override", has_bcc=bool(bcc_recipients))
    else:
        direct_to = []
        bcc_recipients = load_recipients(config_path)
        log.info("resend_recipients_from_config", count=len(bcc_recipients))

    api_key = _resend_config_value("RESEND_API_KEY", config_path)
    from_email = _resend_config_value("RESEND_FROM_EMAIL", config_path)
    if not api_key:
        raise ValueError("RESEND_API_KEY is not configured")
    if not from_email:
        raise ValueError("RESEND_FROM_EMAIL is not configured")

    report_path = Path(report_path)
    md_text = report_path.read_text(encoding="utf-8")
    chart_paths = chart_paths or []
    chart_cid_map: dict[str, str] = {
        str(cp): f"chart_{idx}_{Path(cp).stem}"
        for idx, cp in enumerate(chart_paths)
    }
    html = _md_to_html(md_text, chart_cid_map, chart_paths)

    payload: dict[str, object] = {
        "from": from_email,
        "subject": subject,
        "html": html,
        "text": md_text[:12000],
        "tags": [
            {"name": "app", "value": "quant-stack"},
            {"name": "provider", "value": "resend"},
        ],
    }
    if direct_to:
        payload["to"] = direct_to
        if bcc_recipients:
            payload["bcc"] = bcc_recipients
    else:
        visible_to = _resend_config_value("RESEND_VISIBLE_TO", config_path) or from_email
        payload["to"] = [visible_to]
        payload["bcc"] = bcc_recipients

    timeout = float(os.environ.get("RESEND_TIMEOUT_SECONDS", "30"))
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )
    if resp.status_code < 200 or resp.status_code >= 300:
        body = (resp.text or "")[:800].replace(api_key, "re_***REDACTED***")
        raise RuntimeError(f"Resend returned HTTP {resp.status_code}: {body}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"Resend returned non-JSON response: {resp.text[:300]}") from exc
    msg_id = str(data.get("id") or "")
    if not msg_id:
        raise RuntimeError(f"Resend response missing id: {data}")
    log.info(
        "resend_sent",
        to_count=len(direct_to) or 1,
        bcc_count=len(bcc_recipients),
        message_id=msg_id,
    )
    return [msg_id]


def _resolve_sender(service, config_path: str) -> str:
    """Build an RFC 2047-encoded From value from the account + config name.

    Shared by every send path so a non-ASCII display name (黄振宇) never
    leaks raw — see `_format_sender`.
    """
    account_addr = ""
    try:
        profile = _execute_gmail_call(
            "profile_lookup",
            lambda: service.users().getProfile(userId="me").execute(),
            attempts=3,
        )
        account_addr = profile.get("emailAddress", "")
    except Exception as e:  # noqa: BLE001
        log.warning("gmail_profile_lookup_failed", error=str(e))
    return _format_sender(load_sender_name(config_path), account_addr or "me")


def create_report_draft(
    report_path: Path | str,
    chart_paths: list[Path] | None = None,
    to: str = "",
    subject: str = "",
    bcc: list[str] | None = None,
    credentials_path: Path = CREDENTIALS_PATH,
    token_path: Path = TOKEN_PATH,
    config_path: str = "config.yaml",
) -> str:
    """
    Create a Gmail draft with the report as styled HTML + inline charts.
    Returns the draft ID.
    """
    import socket
    service = _get_gmail_service(credentials_path, token_path)
    sender = _resolve_sender(service, config_path)
    msg, _ = prepare_email(report_path, chart_paths, to, subject, bcc=bcc, sender=sender)

    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(60)
    try:
        result = _execute_gmail_call(
            "draft_create",
            lambda: service.users().drafts().create(
                userId="me", body={"message": _encode_message(msg)}
            ).execute(),
        )
    finally:
        socket.setdefaulttimeout(old_timeout)

    draft_id = result.get("id", "")
    log.info("gmail_draft_created", to=to, bcc_count=len(bcc or []), draft_id=draft_id)
    return draft_id


def send_alert_email(
    to: str,
    subject: str,
    body: str,
    credentials_path: Path = CREDENTIALS_PATH,
    token_path: Path = TOKEN_PATH,
    config_path: str = "config.yaml",
) -> str:
    """Send a plain-text alert email (for pipeline failure notifications)."""
    import socket

    service = _get_gmail_service(credentials_path, token_path)
    msg = MIMEText(body, "plain", "utf-8")
    msg["To"] = to
    msg["Subject"] = _encode_subject(subject)
    msg["From"] = _resolve_sender(service, config_path)
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(30)
    try:
        result = _execute_gmail_call(
            "alert_send",
            lambda: service.users().messages().send(
                userId="me", body=_encode_message(msg)
            ).execute(),
        )
    finally:
        socket.setdefaulttimeout(old_timeout)

    msg_id = result.get("id", "")
    log.info("gmail_alert_sent", to=to, subject=subject, message_id=msg_id)
    return msg_id
