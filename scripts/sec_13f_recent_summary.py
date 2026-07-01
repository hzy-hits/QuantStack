#!/usr/bin/env python3
"""Summarize recently added SEC 13F information table files.

The script is intentionally read-only. It scans local SEC source directories,
selects files modified within a recent window, parses 13F information tables,
and compares each recent filing with the previous local 13F from the same
manager when available.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIRS = (
    "/home/ubuntu/research/finance/sources/sec_13f",
    "/home/ubuntu/research/finance/sources/sec",
    str(ROOT / "quant-research-v1" / "data" / "sec_13f"),
    str(ROOT / "quant-research-v1" / "data" / "sec"),
)
SUPPORTED_SUFFIXES = {".xml", ".txt", ".html", ".htm", ".md"}
MAX_SNIFF_BYTES = 256_000


@dataclass(frozen=True)
class Holding:
    issuer: str
    title: str
    cusip: str
    value_usd: float
    shares: float
    put_call: str = ""

    @property
    def key(self) -> str:
        return (self.cusip or self.issuer).upper()


@dataclass(frozen=True)
class Filing:
    file_path: Path
    file_mtime: datetime
    manager: str
    manager_key: str
    cik: str
    accession: str
    filing_date: str
    report_date: str
    form: str
    url: str
    holdings: list[Holding]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize local SEC 13F files added in a recent window.")
    parser.add_argument("--lookback-hours", type=float, default=float(os.environ.get("QUANT_SEC_13F_LOOKBACK_HOURS", "12")))
    parser.add_argument(
        "--source-dirs",
        default=os.environ.get("QUANT_SEC_13F_SOURCE_DIRS", ",".join(DEFAULT_SOURCE_DIRS)),
        help="Comma-separated directories to scan.",
    )
    parser.add_argument("--limit-filings", type=int, default=int(os.environ.get("QUANT_SEC_13F_LIMIT_FILINGS", "8")))
    parser.add_argument("--top-n", type=int, default=int(os.environ.get("QUANT_SEC_13F_TOP_N", "5")))
    parser.add_argument("--json", action="store_true", help="Print JSON instead of markdown.")
    return parser.parse_args()


def utc_from_timestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def parse_source_dirs(value: str) -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for raw in value.split(","):
        text = raw.strip()
        if not text:
            continue
        path = Path(text).expanduser()
        key = str(path)
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def first_text(elem: ET.Element, *names: str) -> str:
    wanted = {name.lower() for name in names}
    for child in elem.iter():
        if local_name(child.tag) in wanted and child.text:
            text = child.text.strip()
            if text:
                return text
    return ""


def number(value: str) -> float:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def clean_xml(text: str) -> str:
    text = re.sub(r"<\?xml[^>]*\?>", "", text, count=1, flags=re.IGNORECASE).strip()
    return text


def extract_xml_blocks(text: str) -> list[str]:
    blocks = re.findall(r"<XML>(.*?)</XML>", text, flags=re.IGNORECASE | re.DOTALL)
    candidates = blocks or [text]
    out: list[str] = []
    for block in candidates:
        lower = block.lower()
        if "infotable" not in lower and "informationtable" not in lower:
            continue
        info_match = re.search(
            r"(<(?:\w+:)?informationTable\b.*?</(?:\w+:)?informationTable>)",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if info_match:
            out.append(info_match.group(1))
            continue
        table_chunks = re.findall(
            r"(<(?:\w+:)?infoTable\b.*?</(?:\w+:)?infoTable>)",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if table_chunks:
            out.append("<informationTable>" + "\n".join(table_chunks) + "</informationTable>")
            continue
        out.append(block)
    return out


def parse_xml_holdings(xml_text: str) -> list[Holding]:
    xml_text = clean_xml(xml_text)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    raw_rows: list[tuple[str, str, str, float, float, str]] = []
    for elem in root.iter():
        if local_name(elem.tag) != "infotable":
            continue
        raw_rows.append(
            (
                first_text(elem, "nameOfIssuer"),
                first_text(elem, "titleOfClass"),
                first_text(elem, "cusip").upper(),
                number(first_text(elem, "value")),
                number(first_text(elem, "sshPrnamt", "shrsOrPrnAmt")),
                first_text(elem, "putCall"),
            )
        )
    value_scale = infer_13f_value_scale(raw_rows)
    holdings: list[Holding] = []
    for issuer, title, cusip, raw_value, shares, put_call in raw_rows:
        holdings.append(
            Holding(
                issuer=issuer,
                title=title,
                cusip=cusip,
                value_usd=raw_value * value_scale,
                shares=shares,
                put_call=put_call,
            )
        )
    return [row for row in holdings if row.issuer or row.cusip]


def infer_13f_value_scale(raw_rows: list[tuple[str, str, str, float, float, str]]) -> float:
    implied_prices = [
        raw_value / shares
        for _issuer, _title, _cusip, raw_value, shares, _put_call in raw_rows
        if raw_value > 0 and shares > 0
    ]
    if not implied_prices:
        return 1000.0
    raw_price = median(implied_prices)
    if 1.0 <= raw_price <= 5000.0:
        return 1.0
    if 1.0 <= raw_price * 1000.0 <= 5000.0:
        return 1000.0
    return 1000.0


def parse_holdings_file(path: Path) -> list[Holding]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    holdings: list[Holding] = []
    for block in extract_xml_blocks(text):
        holdings.extend(parse_xml_holdings(block))
    deduped: dict[str, Holding] = {}
    for row in holdings:
        key = "|".join([row.key, row.title.upper(), row.put_call.upper()])
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = row
        else:
            deduped[key] = Holding(
                issuer=existing.issuer or row.issuer,
                title=existing.title or row.title,
                cusip=existing.cusip or row.cusip,
                value_usd=existing.value_usd + row.value_usd,
                shares=existing.shares + row.shares,
                put_call=existing.put_call or row.put_call,
            )
    return list(deduped.values())


def load_nearby_metadata(path: Path) -> dict[str, Any]:
    for parent in [path.parent, *list(path.parents)[:3]]:
        metadata = parent / "metadata.json"
        if metadata.exists():
            try:
                data = json.loads(metadata.read_text(encoding="utf-8"))
            except Exception:
                continue
            return data if isinstance(data, dict) else {}
    return {}


def infer_metadata(path: Path) -> dict[str, str]:
    metadata = load_nearby_metadata(path)
    company = metadata.get("company")
    if isinstance(company, dict):
        manager = str(company.get("title") or company.get("name") or "")
    else:
        manager = str(company or "")
    manager = manager or str(metadata.get("manager") or "")
    if not manager:
        manager = path.parent.parent.name if path.parent.parent != path.parent else path.parent.name
    accession = str(metadata.get("accession_number") or metadata.get("accession") or path.parent.name)
    form = str(metadata.get("form") or metadata.get("form_type") or "")
    return {
        "manager": manager,
        "cik": str(metadata.get("cik") or ""),
        "accession": accession,
        "filing_date": str(metadata.get("filing_date") or ""),
        "report_date": str(metadata.get("report_date") or ""),
        "form": form,
        "url": str(metadata.get("url") or metadata.get("filing_url") or ""),
    }


def maybe_13f_file(path: Path, *, recent: bool) -> bool:
    if path.suffix.lower() not in SUPPORTED_SUFFIXES:
        return False
    name = path.name.lower()
    if any(marker in name for marker in ("13f", "infotable", "informationtable", "form13")):
        return True
    if not recent:
        return False
    try:
        sample = path.read_text(encoding="utf-8", errors="replace")[:MAX_SNIFF_BYTES].lower()
    except OSError:
        return False
    return "infotable" in sample or "informationtable" in sample or "13f-hr" in sample


def scan_files(source_dirs: list[Path], cutoff: datetime) -> list[Path]:
    paths: list[Path] = []
    seen: set[str] = set()
    for root in source_dirs:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            try:
                mtime = utc_from_timestamp(path.stat().st_mtime)
            except OSError:
                continue
            if not maybe_13f_file(path, recent=mtime >= cutoff):
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            paths.append(path)
    return sorted(paths, key=lambda item: item.stat().st_mtime, reverse=True)


def load_filings(paths: list[Path]) -> list[Filing]:
    filings: list[Filing] = []
    for path in paths:
        holdings = parse_holdings_file(path)
        if not holdings:
            continue
        meta = infer_metadata(path)
        manager = meta["manager"] or path.parent.name
        manager_key = (meta["cik"] or manager).strip().lower()
        try:
            mtime = utc_from_timestamp(path.stat().st_mtime)
        except OSError:
            continue
        filings.append(
            Filing(
                file_path=path,
                file_mtime=mtime,
                manager=manager,
                manager_key=manager_key,
                cik=meta["cik"],
                accession=meta["accession"],
                filing_date=meta["filing_date"],
                report_date=meta["report_date"],
                form=meta["form"] or "13F",
                url=meta["url"],
                holdings=holdings,
            )
        )
    return filings


def compact_holding(row: Holding, **extra: Any) -> dict[str, Any]:
    out = {
        "issuer": row.issuer,
        "title": row.title,
        "cusip": row.cusip,
        "value_usd": round(row.value_usd, 2),
        "shares": round(row.shares, 4),
    }
    if row.put_call:
        out["put_call"] = row.put_call
    out.update(extra)
    return out


def filing_total_value(filing: Filing) -> float:
    return sum(row.value_usd for row in filing.holdings)


def summarize_diff(current: Filing, previous: Filing | None, *, top_n: int) -> dict[str, Any]:
    cur_by_key = {row.key: row for row in current.holdings}
    prev_by_key = {row.key: row for row in previous.holdings} if previous else {}

    new_rows = [row for key, row in cur_by_key.items() if key not in prev_by_key]
    increased = []
    decreased = []
    for key, row in cur_by_key.items():
        prev = prev_by_key.get(key)
        if prev is None:
            continue
        value_delta = row.value_usd - prev.value_usd
        shares_delta = row.shares - prev.shares
        if value_delta > 0 or shares_delta > 0:
            increased.append((row, prev, value_delta, shares_delta))
        elif value_delta < 0 or shares_delta < 0:
            decreased.append((row, prev, value_delta, shares_delta))
    for key, prev in prev_by_key.items():
        if key in cur_by_key:
            continue
        sold = Holding(
            issuer=prev.issuer,
            title=prev.title,
            cusip=prev.cusip,
            value_usd=0.0,
            shares=0.0,
            put_call=prev.put_call,
        )
        decreased.append((sold, prev, -prev.value_usd, -prev.shares))

    new_rows.sort(key=lambda row: row.value_usd, reverse=True)
    increased.sort(key=lambda item: (item[2], item[3]), reverse=True)
    decreased.sort(key=lambda item: (item[2], item[3]))

    def change_row(row: Holding, prev: Holding, value_delta: float, shares_delta: float) -> dict[str, Any]:
        return compact_holding(
            row,
            previous_value_usd=round(prev.value_usd, 2),
            value_delta_usd=round(value_delta, 2),
            previous_shares=round(prev.shares, 4),
            shares_delta=round(shares_delta, 4),
        )

    return {
        "baseline_found": previous is not None,
        "baseline": filing_identity(previous) if previous else None,
        "new_positions_top5": [compact_holding(row) for row in new_rows[:top_n]],
        "increases_top5": [change_row(*item) for item in increased[:top_n]],
        "decreases_top5": [change_row(*item) for item in decreased[:top_n]],
    }


def filing_identity(filing: Filing | None) -> dict[str, Any] | None:
    if filing is None:
        return None
    return {
        "manager": filing.manager,
        "cik": filing.cik,
        "accession": filing.accession,
        "filing_date": filing.filing_date,
        "report_date": filing.report_date,
        "file_path": str(filing.file_path),
        "file_mtime": filing.file_mtime.isoformat(),
    }


def summarize_recent(source_dirs: list[Path], lookback_hours: float, limit_filings: int, top_n: int) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)
    all_filings = load_filings(scan_files(source_dirs, cutoff))
    by_manager: dict[str, list[Filing]] = {}
    for filing in all_filings:
        by_manager.setdefault(filing.manager_key, []).append(filing)
    for rows in by_manager.values():
        rows.sort(key=lambda item: (item.report_date or "", item.filing_date or "", item.file_mtime.isoformat()))

    recent = [filing for filing in all_filings if filing.file_mtime >= cutoff]
    recent.sort(key=lambda item: item.file_mtime, reverse=True)
    out_filings = []
    for filing in recent[:limit_filings]:
        history = by_manager.get(filing.manager_key, [])
        older = [row for row in history if row.file_path != filing.file_path and row.file_mtime < filing.file_mtime]
        previous = older[-1] if older else None
        diff = summarize_diff(filing, previous, top_n=top_n)
        out_filings.append(
            {
                **(filing_identity(filing) or {}),
                "form": filing.form,
                "url": filing.url,
                "holding_count": len(filing.holdings),
                "total_value_usd": round(filing_total_value(filing), 2),
                **diff,
            }
        )

    return {
        "schema": "quant_stack.sec_13f_recent.v1",
        "generated_at": now.isoformat(),
        "lookback_hours": lookback_hours,
        "cutoff": cutoff.isoformat(),
        "source_dirs": [str(path) for path in source_dirs],
        "available_filing_count": len(all_filings),
        "recent_file_count": len(recent),
        "filings": out_filings,
        "note": "13F values are normalized to USD; parser auto-detects whether an information table reports raw USD or thousands of USD.",
    }


def money(value: Any) -> str:
    try:
        number_value = float(value)
    except (TypeError, ValueError):
        return "-"
    sign = "-" if number_value < 0 else ""
    number_value = abs(number_value)
    if number_value >= 1_000_000_000:
        return f"{sign}${number_value / 1_000_000_000:.2f}B"
    if number_value >= 1_000_000:
        return f"{sign}${number_value / 1_000_000:.1f}M"
    if number_value >= 1_000:
        return f"{sign}${number_value / 1_000:.1f}K"
    return f"{sign}${number_value:.0f}"


def compact_list(rows: list[dict[str, Any]], *, change_key: str = "") -> str:
    if not rows:
        return "-"
    parts = []
    for row in rows:
        label = row.get("issuer") or row.get("cusip") or "-"
        value = money(row.get(change_key) if change_key else row.get("value_usd"))
        parts.append(f"{label}({value})")
    return "; ".join(parts)


def render_markdown(payload: dict[str, Any]) -> str:
    filings = payload.get("filings") if isinstance(payload.get("filings"), list) else []
    if not filings:
        return "## SEC 13F 机构持仓快照\n\n过去窗口内没有发现本地新增 13F 持仓文件。"
    lines = [
        "## SEC 13F 机构持仓快照",
        f"过去 {payload.get('lookback_hours')} 小时本地新增 {payload.get('recent_file_count')} 个 13F 持仓文件；13F 有季度滞后，只作为机构仓位线索。",
        "",
        "| Manager | Filing/Report | Holdings | 新增Top5 | 增持Top5 | 减持Top5 |",
        "|---|---|---:|---|---|---|",
    ]
    for filing in filings:
        filing_label = " / ".join(part for part in [filing.get("filing_date"), filing.get("report_date")] if part) or "-"
        lines.append(
            "| {manager} | {date} | {count} | {new} | {inc} | {dec} |".format(
                manager=str(filing.get("manager") or "-").replace("|", "/"),
                date=filing_label.replace("|", "/"),
                count=filing.get("holding_count") or 0,
                new=compact_list(filing.get("new_positions_top5") or []).replace("|", "/"),
                inc=compact_list(filing.get("increases_top5") or [], change_key="value_delta_usd").replace("|", "/"),
                dec=compact_list(filing.get("decreases_top5") or [], change_key="value_delta_usd").replace("|", "/"),
            )
        )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    source_dirs = parse_source_dirs(args.source_dirs)
    payload = summarize_recent(source_dirs, args.lookback_hours, args.limit_filings, args.top_n)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(render_markdown(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
