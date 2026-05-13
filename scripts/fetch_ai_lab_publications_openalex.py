#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
STACK_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_ai_lab_publications import DEFAULT_SEED, load_seed  # noqa: E402


OPENALEX_API = "https://api.openalex.org"
DEFAULT_OUTPUT = STACK_ROOT / "data" / "ai_lab_publications_openalex_raw.jsonl"
CONFERENCE_SOURCE_QUERIES = {
    "NeurIPS": "Neural Information Processing Systems",
    "ICML": "International Conference on Machine Learning",
    "ICLR": "International Conference on Learning Representations",
    "CVPR": "Computer Vision and Pattern Recognition",
}
AFFILIATION_ALIAS_EXCLUDES = {
    "alphabet",
    "google",
    "meta",
    "facebook",
    "microsoft",
    "amazon",
    "aws",
    "nvidia",
    "adobe",
    "ibm",
}


def api_get_json(url: str, timeout: int = 30) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "quant-stack-ai-lab-index/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def build_url(endpoint: str, params: dict[str, Any], mailto: str | None = None) -> str:
    clean = {key: value for key, value in params.items() if value not in (None, "")}
    if mailto:
        clean["mailto"] = mailto
    return f"{OPENALEX_API}/{endpoint.lstrip('/')}?{urllib.parse.urlencode(clean)}"


def normalize(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("&", " and ").split())


def source_matches(conference: str, display_name: str) -> bool:
    conf = conference.upper()
    text = normalize(display_name)
    if conf == "NEURIPS":
        return "neural information processing systems" in text or "neurips" in text or "nips" in text
    if conf == "ICML":
        return "international conference on machine learning" in text or "icml" in text
    if conf == "ICLR":
        return "learning representations" in text or "iclr" in text
    if conf == "CVPR":
        return "computer vision and pattern recognition" in text or "cvpr" in text
    return normalize(conference) in text


def resolve_source_ids(
    conferences: set[str],
    *,
    mailto: str | None = None,
    max_sources_per_conference: int = 4,
) -> dict[str, list[str]]:
    resolved: dict[str, list[str]] = {}
    for conference in sorted(conferences):
        query = CONFERENCE_SOURCE_QUERIES.get(conference, conference)
        url = build_url("sources", {"search": query, "per-page": 10}, mailto)
        payload = api_get_json(url)
        source_ids: list[str] = []
        for item in payload.get("results") or []:
            source_id = str(item.get("id") or "").strip()
            display_name = str(item.get("display_name") or "")
            if source_id and source_matches(conference, display_name):
                source_ids.append(source_id)
            if len(source_ids) >= max_sources_per_conference:
                break
        resolved[conference] = source_ids
    return resolved


def affiliation_terms(companies: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for symbol, item in companies.items():
        aliases = [str(alias).strip() for alias in item.get("aliases") or [] if str(alias).strip()]
        filtered = [
            alias
            for alias in aliases
            if len(alias) >= 4
            and alias not in AFFILIATION_ALIAS_EXCLUDES
            and not any(term in alias for term in ["gemini", "llama", "copilot", "cuda", "react", "jax"])
        ]
        out[symbol] = sorted(set(filtered))[:10]
    return out


def fetch_works(
    *,
    source_id: str,
    year: int,
    affiliation: str,
    mailto: str | None = None,
    per_page: int = 200,
    max_pages: int = 2,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = "*"
    for _ in range(max_pages):
        filters = ",".join(
            [
                f"publication_year:{year}",
                f"primary_location.source.id:{source_id}",
                f"raw_affiliation_strings.search:{affiliation}",
            ]
        )
        url = build_url(
            "works",
            {
                "filter": filters,
                "per-page": per_page,
                "cursor": cursor,
                "select": "id,display_name,title,publication_year,primary_location,authorships",
            },
            mailto,
        )
        payload = api_get_json(url)
        page_rows = payload.get("results") or []
        rows.extend(page_rows)
        cursor = ((payload.get("meta") or {}).get("next_cursor") or "").strip()
        if not page_rows or not cursor:
            break
    return rows


def fetch_works_by_search(
    *,
    conference: str,
    year: int,
    affiliation: str,
    mailto: str | None = None,
    per_page: int = 200,
    max_pages: int = 1,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = "*"
    query = f"{affiliation} {CONFERENCE_SOURCE_QUERIES.get(conference, conference)} {year}"
    for _ in range(max_pages):
        url = build_url(
            "works",
            {
                "search": query,
                "filter": f"publication_year:{year},raw_affiliation_strings.search:{affiliation}",
                "per-page": per_page,
                "cursor": cursor,
                "select": "id,display_name,title,publication_year,primary_location,authorships",
            },
            mailto,
        )
        payload = api_get_json(url)
        page_rows = payload.get("results") or []
        rows.extend(page_rows)
        cursor = ((payload.get("meta") or {}).get("next_cursor") or "").strip()
        if not page_rows or not cursor:
            break
    return rows


def fetch_publications(
    *,
    seed: Path = DEFAULT_SEED,
    years: list[int],
    output: Path = DEFAULT_OUTPUT,
    mailto: str | None = None,
    symbols: set[str] | None = None,
    conferences_filter: set[str] | None = None,
    fallback_search: bool = False,
    max_pages: int = 2,
    sleep_seconds: float = 0.1,
) -> int:
    conferences, companies = load_seed(seed)
    if conferences_filter:
        conferences = {conference for conference in conferences if conference.upper() in conferences_filter}
    if symbols:
        companies = {symbol: item for symbol, item in companies.items() if symbol.upper() in symbols}
    source_ids = resolve_source_ids(conferences, mailto=mailto)
    terms = affiliation_terms(companies)
    output.parent.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    written = 0
    with output.open("w", encoding="utf-8") as handle:
        for conference in sorted(conferences):
            conference_source_ids = source_ids.get(conference) or []
            for year in years:
                for symbol, aliases in sorted(terms.items()):
                    for affiliation in aliases:
                        rows: list[dict[str, Any]] = []
                        for source_id in conference_source_ids:
                            rows.extend(
                                fetch_works(
                                    source_id=source_id,
                                    year=year,
                                    affiliation=affiliation,
                                    mailto=mailto,
                                    max_pages=max_pages,
                                )
                            )
                        source_id_context = ";".join(conference_source_ids)
                        if not rows and fallback_search:
                            rows = fetch_works_by_search(
                                conference=conference,
                                year=year,
                                affiliation=affiliation,
                                mailto=mailto,
                                max_pages=max(1, min(max_pages, 2)),
                            )
                            source_id_context = "fallback_search"
                        for row in rows:
                            work_id = str(row.get("id") or "")
                            if not work_id or work_id in seen:
                                continue
                            seen.add(work_id)
                            row["_ai_lab_fetch_context"] = {
                                "conference": conference,
                                "source_id": source_id_context,
                                "year": year,
                                "symbol_hint": symbol,
                                "affiliation_query": affiliation,
                            }
                            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                            written += 1
                        if sleep_seconds > 0:
                            time.sleep(sleep_seconds)
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch raw OpenAlex works for AI-lab top-conference publication indexing."
    )
    parser.add_argument("--seed", default=DEFAULT_SEED, type=Path)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--symbols", nargs="*", help="Optional symbol subset, e.g. GOOGL META NVDA.")
    parser.add_argument("--conferences", nargs="*", help="Optional conference subset, e.g. NeurIPS ICML ICLR CVPR.")
    parser.add_argument("--fallback-search", action="store_true", help="Use broad works search if source+affiliation filters return no rows.")
    parser.add_argument("--mailto", default=os.environ.get("OPENALEX_MAILTO"))
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument("--sleep-seconds", type=float, default=0.1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    written = fetch_publications(
        seed=args.seed,
        years=args.years,
        output=args.output,
        mailto=args.mailto,
        symbols={item.upper() for item in args.symbols or []} or None,
        conferences_filter={item.upper() for item in args.conferences or []} or None,
        fallback_search=args.fallback_search,
        max_pages=args.max_pages,
        sleep_seconds=args.sleep_seconds,
    )
    print(f"OpenAlex AI lab publication raw JSONL written: {args.output} rows={written}")
    print("Next: python3 scripts/build_ai_lab_publications.py --input", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
