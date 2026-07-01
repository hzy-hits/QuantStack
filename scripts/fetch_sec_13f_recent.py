#!/usr/bin/env python3
"""Fetch recent SEC 13F information-table files into the local source cache."""
from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = Path(os.environ.get("QUANT_SEC_13F_OUTPUT_DIR", "/home/ubuntu/research/finance/sources/sec_13f"))
FALLBACK_OUTPUT_DIR = ROOT / "quant-research-v1" / "data" / "sec_13f"
SEC_CURRENT_13F_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F-HR&owner=include&count={count}&output=atom"
DEFAULT_USER_AGENT = os.environ.get("SEC_USER_AGENT") or os.environ.get("QUANT_SEC_USER_AGENT") or "QuantStack research contact 13502448752hzy@gmail.com"
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


@dataclass(frozen=True)
class CurrentFiling:
    form: str
    manager: str
    cik: str
    accession: str
    filing_date: str
    updated_at: datetime
    index_url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download recent SEC 13F-HR information tables.")
    parser.add_argument("--lookback-hours", type=float, default=float(os.environ.get("QUANT_SEC_13F_LOOKBACK_HOURS", "12")))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--feed-count", type=int, default=int(os.environ.get("QUANT_SEC_13F_FEED_COUNT", "100")))
    parser.add_argument("--limit-filings", type=int, default=int(os.environ.get("QUANT_SEC_13F_FETCH_LIMIT", "20")))
    parser.add_argument("--sleep-seconds", type=float, default=float(os.environ.get("QUANT_SEC_13F_SLEEP_SECONDS", "0.2")))
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser.parse_args()


def ensure_output_dir(path: Path) -> Path:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except OSError:
        FALLBACK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return FALLBACK_OUTPUT_DIR


def fetch_text(url: str, *, user_agent: str, timeout: int = 20) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "identity",
            "Accept": "application/atom+xml,text/html,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_atom_datetime(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def text_of(entry: ET.Element, name: str) -> str:
    node = entry.find(f"atom:{name}", ATOM_NS)
    return (node.text or "").strip() if node is not None else ""


def parse_feed(xml_text: str, *, lookback_hours: float) -> list[CurrentFiling]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)
    root = ET.fromstring(xml_text)
    filings: list[CurrentFiling] = []
    for entry in root.findall("atom:entry", ATOM_NS):
        updated = parse_atom_datetime(text_of(entry, "updated"))
        if updated is None or updated < cutoff or updated > now + timedelta(minutes=10):
            continue
        title = text_of(entry, "title")
        form_match = re.match(r"([A-Z0-9/-]+)\s+-\s+", title)
        form = form_match.group(1) if form_match else "13F-HR"
        manager_match = re.search(r"-\s+(.*?)\s+\((\d{10})\)\s+\(Filer\)", title)
        manager = manager_match.group(1).strip() if manager_match else title
        cik = manager_match.group(2).lstrip("0") if manager_match else ""
        link = entry.find("atom:link[@rel='alternate']", ATOM_NS)
        if link is None:
            link = entry.find("atom:link", ATOM_NS)
        index_url = link.attrib.get("href", "") if link is not None else ""
        summary = html_lib.unescape(text_of(entry, "summary"))
        accession_match = re.search(r"AccNo:\s*</b>\s*([0-9-]+)", summary, flags=re.IGNORECASE)
        filing_date_match = re.search(r"Filed:\s*</b>\s*(\d{4}-\d{2}-\d{2})", summary, flags=re.IGNORECASE)
        accession = accession_match.group(1) if accession_match else ""
        filings.append(
            CurrentFiling(
                form=form,
                manager=manager,
                cik=cik,
                accession=accession,
                filing_date=filing_date_match.group(1) if filing_date_match else "",
                updated_at=updated,
                index_url=index_url,
            )
        )
    return filings


def strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_lib.unescape(value))).strip()


def extract_report_date(index_html: str) -> str:
    match = re.search(
        r"<div\s+class=\"infoHead\">\s*Period of Report\s*</div>\s*<div\s+class=\"info\">\s*(\d{4}-\d{2}-\d{2})\s*</div>",
        index_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return match.group(1) if match else ""


def extract_information_table_links(index_html: str, index_url: str) -> list[str]:
    links: list[str] = []
    for row in re.findall(r"<tr\b.*?</tr>", index_html, flags=re.IGNORECASE | re.DOTALL):
        row_text = strip_tags(row).lower()
        if "information table" not in row_text and "infotable" not in row_text and "informationtable" not in row_text:
            continue
        for href in re.findall(r"href=\"([^\"]+\.xml)\"", row, flags=re.IGNORECASE):
            if "/xsl" in href.lower():
                continue
            links.append(urljoin(index_url, html_lib.unescape(href)))
    seen: set[str] = set()
    out: list[str] = []
    for link in links:
        if link not in seen:
            seen.add(link)
            out.append(link)
    return out


def safe_name(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return text.strip("_") or "filing"


def write_filing(
    filing: CurrentFiling,
    *,
    output_dir: Path,
    info_links: list[str],
    index_html: str,
    user_agent: str,
    sleep_seconds: float,
) -> dict[str, Any]:
    cik_part = filing.cik or "unknown_cik"
    accession_part = filing.accession.replace("-", "") or safe_name(filing.index_url.rsplit("/", 1)[-1])
    dest = output_dir / cik_part / accession_part
    dest.mkdir(parents=True, exist_ok=True)
    downloaded: list[str] = []
    for link in info_links:
        filename = safe_name(link.rsplit("/", 1)[-1])
        target = dest / filename
        if not target.exists():
            text = fetch_text(link, user_agent=user_agent)
            target.write_text(text, encoding="utf-8")
            time.sleep(max(0.0, sleep_seconds))
        downloaded.append(str(target))
    metadata = {
        "company": {"title": filing.manager},
        "manager": filing.manager,
        "cik": filing.cik,
        "accession_number": filing.accession,
        "form": filing.form,
        "filing_date": filing.filing_date,
        "report_date": extract_report_date(index_html),
        "url": filing.index_url,
        "filing_url": filing.index_url,
        "accepted_at": filing.updated_at.isoformat(),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "information_table_files": downloaded,
    }
    (dest / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "manager": filing.manager,
        "cik": filing.cik,
        "accession": filing.accession,
        "updated_at": filing.updated_at.isoformat(),
        "report_date": metadata["report_date"],
        "file_count": len(downloaded),
        "dir": str(dest),
    }


def fetch_recent_13f(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = ensure_output_dir(args.output_dir.expanduser())
    feed_url = SEC_CURRENT_13F_URL.format(count=max(1, args.feed_count))
    feed = fetch_text(feed_url, user_agent=args.user_agent)
    filings = parse_feed(feed, lookback_hours=args.lookback_hours)[: max(0, args.limit_filings)]
    downloaded = []
    errors = []
    for filing in filings:
        try:
            index_html = fetch_text(filing.index_url, user_agent=args.user_agent)
            info_links = extract_information_table_links(index_html, filing.index_url)
            if not info_links:
                errors.append({"accession": filing.accession, "error": "no information table link found"})
                continue
            downloaded.append(
                write_filing(
                    filing,
                    output_dir=output_dir,
                    info_links=info_links,
                    index_html=index_html,
                    user_agent=args.user_agent,
                    sleep_seconds=args.sleep_seconds,
                )
            )
            time.sleep(max(0.0, args.sleep_seconds))
        except Exception as exc:  # best-effort fetcher; report per-filing failures.
            errors.append({"accession": filing.accession, "error": str(exc)[-500:]})
    return {
        "schema": "quant_stack.sec_13f_fetch.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_hours": args.lookback_hours,
        "feed_url": feed_url,
        "output_dir": str(output_dir),
        "candidate_count": len(filings),
        "downloaded_count": len(downloaded),
        "downloaded": downloaded,
        "errors": errors,
    }


def main() -> int:
    args = parse_args()
    print(json.dumps(fetch_recent_13f(args), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
