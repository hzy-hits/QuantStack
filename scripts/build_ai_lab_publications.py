#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

import yaml


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED = STACK_ROOT / "data" / "ai_lab_quality_seed.yaml"
DEFAULT_OUTPUT = STACK_ROOT / "data" / "ai_lab_publications.csv"
DEFAULT_CONFERENCES = {"NEURIPS", "ICML", "ICLR", "CVPR"}
PRESENTATION_BONUS_TERMS = {"oral", "spotlight", "notable", "award", "best paper"}
MANUAL_ALIASES = {
    "GOOGL": ["google", "google llc", "deepmind", "google brain", "google ai", "google research", "alphabet"],
    "META": ["meta", "fair", "meta ai", "facebook ai", "facebook research", "pytorch"],
    "MSFT": ["microsoft", "microsoft research", "azure ai", "msr"],
    "AMZN": ["amazon", "amazon science", "aws ai", "alexa ai", "amazon agi"],
    "NVDA": ["nvidia", "nvidia corporation", "nvidia research", "nvidia ai", "cuda"],
    "ADBE": ["adobe research"],
    "IBM": ["ibm research"],
}


def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def term_in_text(term: str, text: str) -> bool:
    if not term or not text:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(normalize_text(term))}(?![a-z0-9])", text) is not None


def load_seed(seed_path: Path = DEFAULT_SEED) -> tuple[set[str], dict[str, dict[str, Any]]]:
    payload = yaml.safe_load(seed_path.read_text(encoding="utf-8")) or {}
    conferences = {str(item).upper() for item in payload.get("conference_scope") or DEFAULT_CONFERENCES}
    companies: dict[str, dict[str, Any]] = {}
    for item in payload.get("companies") or []:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        terms = {
            item.get("company"),
            symbol if len(symbol) >= 4 else "",
            *(item.get("labs") or []),
            *(item.get("stack_aliases") or []),
            *MANUAL_ALIASES.get(symbol, []),
        }
        aliases = sorted({normalize_text(term) for term in terms if normalize_text(term)})
        companies[symbol] = {
            "symbol": symbol,
            "company": item.get("company"),
            "aliases": aliases,
        }
    return conferences, companies


def row_text(row: dict[str, Any]) -> str:
    fields = [
        "affiliations",
        "affiliation",
        "institutions",
        "institution",
        "authors",
        "author_affiliations",
        "company",
        "lab",
        "title",
    ]
    return normalize_text(" ".join(str(row.get(field) or "") for field in fields))


def row_conference(row: dict[str, Any]) -> str:
    raw = row.get("conference") or row.get("venue") or row.get("booktitle") or ""
    text = normalize_text(raw).upper()
    if "NEURIPS" in text or "NIPS" in text or "NEURAL INFORMATION PROCESSING SYSTEMS" in text:
        return "NeurIPS"
    if "ICML" in text or "INTERNATIONAL CONFERENCE ON MACHINE LEARNING" in text:
        return "ICML"
    if "ICLR" in text or "LEARNING REPRESENTATIONS" in text:
        return "ICLR"
    if "CVPR" in text or "COMPUTER VISION AND PATTERN RECOGNITION" in text:
        return "CVPR"
    return str(raw or "").strip()


def row_year(row: dict[str, Any]) -> str:
    raw = str(row.get("year") or row.get("publication_year") or row.get("date") or "").strip()
    match = re.search(r"(20\d{2}|19\d{2})", raw)
    return match.group(1) if match else raw


def row_is_accepted(row: dict[str, Any]) -> bool:
    status = normalize_text(row.get("status") or row.get("decision") or row.get("acceptance") or "")
    if not status:
        return True
    if any(term in status for term in ["reject", "withdraw", "desk reject"]):
        return False
    return any(term in status for term in ["accept", "poster", "oral", "spotlight", "published", "conference"]) or status == "1"


def row_oral_spotlight(row: dict[str, Any]) -> int:
    text = normalize_text(
        " ".join(
            str(row.get(field) or "")
            for field in ["presentation_type", "track", "session", "award", "decision", "status"]
        )
    )
    return 1 if any(term in text for term in PRESENTATION_BONUS_TERMS) else 0


def matched_symbols(text: str, companies: dict[str, dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for symbol, item in companies.items():
        if any(term_in_text(alias, text) for alias in item["aliases"]):
            out.append(symbol)
    return out


def unwrap_value(value: Any) -> Any:
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def as_text_list(value: Any) -> list[str]:
    value = unwrap_value(value)
    if value is None:
        return []
    if isinstance(value, list):
        return [str(unwrap_value(item) or "").strip() for item in value if str(unwrap_value(item) or "").strip()]
    if isinstance(value, dict):
        return [str(item).strip() for item in value.values() if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"[;|]", text) if part.strip()]


def flatten_json_record(record: dict[str, Any]) -> dict[str, Any]:
    content = record.get("content") if isinstance(record.get("content"), dict) else {}
    if content:
        title = unwrap_value(content.get("title")) or record.get("title")
        venue = (
            unwrap_value(content.get("venue"))
            or unwrap_value(content.get("venueid"))
            or unwrap_value(content.get("conference"))
            or record.get("venue")
            or record.get("venueid")
        )
        year = unwrap_value(content.get("year")) or unwrap_value(content.get("publication_year")) or record.get("year")
        authors = as_text_list(content.get("authors") or record.get("authors"))
        affiliations = (
            as_text_list(content.get("affiliations"))
            or as_text_list(content.get("institutions"))
            or as_text_list(content.get("author_affiliations"))
        )
        decision = unwrap_value(content.get("decision")) or unwrap_value(content.get("status")) or record.get("decision")
        return {
            "title": title,
            "venue": venue,
            "year": year,
            "status": decision or venue or "accepted",
            "presentation_type": venue,
            "authors": "; ".join(authors),
            "affiliations": "; ".join(affiliations),
            "source": "openreview_json",
        }

    if "authorships" in record:
        institutions: list[str] = []
        authors: list[str] = []
        for authorship in record.get("authorships") or []:
            author = authorship.get("author") if isinstance(authorship, dict) else {}
            if isinstance(author, dict) and author.get("display_name"):
                authors.append(str(author.get("display_name")))
            for institution in authorship.get("institutions") or []:
                if isinstance(institution, dict) and institution.get("display_name"):
                    institutions.append(str(institution.get("display_name")))
        location = record.get("primary_location") if isinstance(record.get("primary_location"), dict) else {}
        source = location.get("source") if isinstance(location.get("source"), dict) else {}
        return {
            "title": record.get("title"),
            "venue": source.get("display_name") or record.get("venue") or record.get("host_venue"),
            "year": record.get("publication_year") or record.get("year"),
            "status": "accepted",
            "presentation_type": record.get("presentation_type") or record.get("type"),
            "authors": "; ".join(authors),
            "affiliations": "; ".join(sorted(set(institutions))),
            "source": "openalex_json",
        }

    if isinstance(record.get("authors"), list):
        affiliations: list[str] = []
        authors: list[str] = []
        for author in record.get("authors") or []:
            if not isinstance(author, dict):
                continue
            if author.get("name"):
                authors.append(str(author.get("name")))
            affiliations.extend(as_text_list(author.get("affiliations") or author.get("affiliation")))
        return {
            "title": record.get("title"),
            "venue": record.get("venue") or record.get("conference"),
            "year": record.get("year") or record.get("publication_year"),
            "status": record.get("status") or "accepted",
            "presentation_type": record.get("presentation_type") or record.get("track") or record.get("venue"),
            "authors": "; ".join(authors),
            "affiliations": "; ".join(sorted(set(affiliations))),
            "source": "semantic_scholar_json",
        }

    return record


def iter_publication_rows(input_path: Path) -> list[dict[str, Any]]:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        with input_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(flatten_json_record(json.loads(line)))
        return rows
    if suffix == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            raw_rows = payload.get("results") or payload.get("works") or payload.get("notes") or payload.get("data") or []
        else:
            raw_rows = payload
        return [flatten_json_record(row) for row in raw_rows if isinstance(row, dict)]
    raise ValueError(f"unsupported publication input format: {input_path}")


def aggregate_publications(input_path: Path, seed_path: Path = DEFAULT_SEED) -> list[dict[str, Any]]:
    conferences, companies = load_seed(seed_path)
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in iter_publication_rows(input_path):
        conference = row_conference(row)
        if conference.upper() not in conferences:
            continue
        if not row_is_accepted(row):
            continue
        year = row_year(row)
        if not year:
            continue
        text = row_text(row)
        symbols = matched_symbols(text, companies)
        for symbol in symbols:
            key = (symbol, conference, year)
            bucket = buckets.setdefault(
                key,
                {
                    "symbol": symbol,
                    "conference": conference,
                    "year": year,
                    "accepted_count": 0,
                    "oral_spotlight_count": 0,
                    "source": str(input_path),
                },
            )
            bucket["accepted_count"] += 1
            bucket["oral_spotlight_count"] += row_oral_spotlight(row)
    return sorted(buckets.values(), key=lambda item: (item["symbol"], item["conference"], item["year"]))


def write_publications(rows: list[dict[str, Any]], output_path: Path = DEFAULT_OUTPUT) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "conference", "year", "accepted_count", "oral_spotlight_count", "source"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build data/ai_lab_publications.csv from a local accepted-paper affiliation CSV/JSON/JSONL export."
    )
    parser.add_argument("--input", required=True, type=Path, help="Raw paper export CSV, JSON, or JSONL.")
    parser.add_argument("--seed", default=DEFAULT_SEED, type=Path)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    args = parser.parse_args()

    rows = aggregate_publications(args.input, args.seed)
    write_publications(rows, args.output)
    print(f"AI lab publications written: {args.output} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
