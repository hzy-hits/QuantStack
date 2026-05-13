#!/usr/bin/env python3
"""Fetch AI-infra source documents and extract structured evidence rows."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPANSION_CANDIDATES = STACK_ROOT / "ai_infra" / "reports" / "expansion_candidates_v1.csv"
DEFAULT_CACHE_DIR = STACK_ROOT / "ai_infra" / "data" / "source_cache"
DEFAULT_REPORTS_DIR = STACK_ROOT / "ai_infra" / "reports"
DEFAULT_SEC_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"
SEC_FORMS = ("10-K", "10-Q", "8-K", "S-1", "20-F", "6-K")

TASK_FIELDS = [
    "as_of",
    "symbol",
    "company",
    "source_url",
    "source_type",
    "published_at",
    "status",
    "raw_path",
    "text_path",
    "error",
]

EVIDENCE_FIELDS = [
    "as_of",
    "company",
    "symbol",
    "source_url",
    "source_type",
    "published_at",
    "evidence_type",
    "claim",
    "source_excerpt_locator",
    "ai_module",
    "relationship_type",
    "counterparty",
    "financial_translation",
    "confidence",
]

AI_MODULE_TERMS = [
    ("ai_compute_accelerators", ["gpu", "accelerator", "asic", "xpu", "cuda", "ai chip", "tpu"]),
    ("ai_memory_storage", ["hbm", "dram", "nand", "ssd", "storage", "memory"]),
    ("ai_networking_optical_cpo", ["optical", "optics", "cpo", "transceiver", "800g", "1.6t", "ethernet", "networking", "silicon photonics"]),
    ("ai_datacenter_edge_infra", ["data center", "datacenter", "edge ai", "ai server", "rack", "cloud", "gpu-as-a-service"]),
    ("ai_power_nuclear_grid", ["power", "cooling", "thermal", "grid", "switchgear", "transformer", "nuclear", "ppa"]),
    ("ai_chip_equipment_materials_packaging", ["cowos", "advanced packaging", "packaging", "test", "metrology", "eda", "substrate", "wafer", "material"]),
]

EVIDENCE_TERMS = [
    ("segment_revenue", ["revenue", "sales", "segment", "data center revenue", "ai revenue"]),
    ("orders_backlog", ["order", "orders", "backlog", "rpo", "book-to-bill", "contract"]),
    ("capex_capacity", ["capex", "capital expenditure", "capacity", "expansion", "mw", "megawatt"]),
    ("margin_cash_flow", ["gross margin", "operating margin", "free cash flow", "fcf", "cash flow"]),
    ("customer_concentration", ["customer concentration", "major customer", "customer accounted", "customer represented"]),
    ("qualification_roadmap", ["qualified", "qualification", "design win", "roadmap", "ramp", "shipping", "shipment"]),
]

RELATIONSHIP_TERMS = [
    ("supplier", ["supplier", "supplies", "supply agreement", "supply"]),
    ("customer", ["customer", "customer agreement", "customer contract"]),
    ("contract_or_order", ["contract", "order", "purchase order", "backlog"]),
    ("partnership", ["partner", "partnership", "collaboration", "collaborate"]),
    ("capacity_or_qualification", ["qualified", "design win", "capacity", "ramp"]),
]

ORIGINAL_SOURCE_HINTS = {
    "sec_filing",
    "10-k",
    "10-q",
    "8-k",
    "20-f",
    "6-k",
    "annual_report",
    "quarterly_report",
    "official_press_release",
    "official_customer_announcement",
    "investor_presentation",
    "earnings_transcript",
    "exchange_announcement",
    "company_product_page",
    "product_page",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch original sources and extract AI-infra evidence rows.")
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--expansion-candidates", type=Path, default=DEFAULT_EXPANSION_CANDIDATES)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--max-docs", type=int, default=80)
    parser.add_argument("--sec-db", type=Path, default=DEFAULT_SEC_DB)
    parser.add_argument("--sec-forms", default=",".join(SEC_FORMS), help="Comma-separated SEC form types to add as source tasks.")
    parser.add_argument("--sec-per-symbol", type=int, default=2, help="Latest SEC filings to add per candidate symbol.")
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def source_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:20]


def normalize_symbol(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().upper())


def table_exists(con: Any, table: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name) = lower(?)",
            [table],
        ).fetchone()[0]
    )


def sec_source_rows(candidate_rows: list[dict[str, str]], sec_db: Path, forms: list[str], per_symbol: int) -> list[dict[str, str]]:
    symbols = sorted({normalize_symbol(row.get("symbol")) for row in candidate_rows if normalize_symbol(row.get("symbol"))})
    if not symbols or not sec_db.exists() or per_symbol <= 0:
        return []
    try:
        import duckdb  # type: ignore
    except ImportError:
        return []

    forms = [form.strip().upper() for form in forms if form.strip()]
    if not forms:
        return []
    placeholders_symbols = ",".join(["?"] * len(symbols))
    placeholders_forms = ",".join(["?"] * len(forms))
    con = duckdb.connect(str(sec_db), read_only=True)
    try:
        if not table_exists(con, "sec_filings"):
            return []
        query = f"""
            SELECT symbol, form_type, filed_date, description, items, filing_url
            FROM sec_filings
            WHERE upper(symbol) IN ({placeholders_symbols})
              AND upper(form_type) IN ({placeholders_forms})
              AND coalesce(filing_url, '') <> ''
            ORDER BY symbol, filed_date DESC, form_type
        """
        cur = con.execute(query, [*symbols, *forms])
        cols = [item[0] for item in cur.description]
        raw_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()

    base_by_symbol: dict[str, dict[str, str]] = {}
    for row in candidate_rows:
        symbol = normalize_symbol(row.get("symbol"))
        if symbol and symbol not in base_by_symbol:
            base_by_symbol[symbol] = row

    counts: dict[str, int] = {}
    out: list[dict[str, str]] = []
    for raw in raw_rows:
        symbol = normalize_symbol(raw.get("symbol"))
        if counts.get(symbol, 0) >= per_symbol:
            continue
        base = dict(base_by_symbol.get(symbol) or {})
        form_type = str(raw.get("form_type") or "").strip().upper()
        base.update(
            {
                "symbol": symbol,
                "company_name": base.get("company_name") or base.get("company") or symbol,
                "source_url": str(raw.get("filing_url") or "").strip(),
                "source_type": f"sec_filing_{form_type.lower()}",
                "source_date": str(raw.get("filed_date") or "")[:10],
                "candidate_reason": base.get("candidate_reason")
                or f"SEC {form_type} original-source task: {raw.get('description') or raw.get('items') or ''}",
            }
        )
        out.append(base)
        counts[symbol] = counts.get(symbol, 0) + 1
    return out


def strip_html(text: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"[ \t\r\f\v]+", " ", text)


def decode_bytes(payload: bytes, content_type: str = "") -> str:
    charset_match = re.search(r"charset=([\w.-]+)", content_type or "", re.I)
    encodings = [charset_match.group(1)] if charset_match else []
    encodings.extend(["utf-8", "latin-1"])
    for encoding in encodings:
        try:
            return payload.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return payload.decode("utf-8", errors="replace")


def fetch_url(url: str, timeout: float) -> tuple[bytes, str]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "file":
        path = Path(urllib.request.url2pathname(parsed.path))
        return path.read_bytes(), "text/plain"
    if parsed.scheme in {"", None}:
        path = Path(url)
        if path.exists():
            return path.read_bytes(), "text/plain"
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported source_url scheme: {parsed.scheme}")
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "quant-stack-ai-infra-source-review/1.0 research-contact local@example.invalid",
            "Accept": "text/html,text/plain,application/json,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - controlled research URLs
        content_type = response.headers.get("content-type", "")
        return response.read(), content_type


def clean_sentence(value: str) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    return text[:700]


def split_sentences(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text)
    pieces = re.split(r"(?<=[.!?。！？])\s+", text)
    return [clean_sentence(piece) for piece in pieces if len(piece.strip()) >= 30]


def find_first_term(terms: list[tuple[str, list[str]]], text: str) -> str:
    lower = text.lower()
    for label, needles in terms:
        if any(needle in lower for needle in needles):
            return label
    return ""


def matched_labels(terms: list[tuple[str, list[str]]], text: str) -> list[str]:
    lower = text.lower()
    return [label for label, needles in terms if any(needle in lower for needle in needles)]


def is_original_source_type(source_type: str) -> bool:
    lowered = source_type.lower().strip()
    if lowered.endswith("_review_candidate") or "news" in lowered or lowered in {"factor_lab_hypothesis", "candidate_missing_source_type"}:
        return False
    return any(hint in lowered for hint in ORIGINAL_SOURCE_HINTS)


def infer_financial_translation(evidence_types: list[str]) -> str:
    mapping = {
        "segment_revenue": "revenue_or_segment_mix",
        "orders_backlog": "orders_backlog_or_rpo",
        "capex_capacity": "capex_capacity_or_power_translation",
        "margin_cash_flow": "margin_cash_flow_translation",
        "customer_concentration": "customer_quality_or_concentration_risk",
        "qualification_roadmap": "qualification_ramp_or_design_win",
    }
    translated = [mapping[label] for label in evidence_types if label in mapping]
    return ";".join(dict.fromkeys(translated))


def confidence_for(source_type: str, ai_module: str, financial_translation: str, evidence_count: int) -> str:
    if not is_original_source_type(source_type):
        return "low"
    if ai_module and financial_translation and evidence_count >= 3:
        return "high"
    if ai_module and financial_translation:
        return "medium"
    return "low"


def extract_evidence(row: dict[str, str], text_path: Path, text: str, as_of: str) -> list[dict[str, str]]:
    source_type = row.get("source_type") or ""
    sentences = split_sentences(text)
    extracted: list[dict[str, str]] = []
    for index, sentence in enumerate(sentences, start=1):
        ai_module = find_first_term(AI_MODULE_TERMS, sentence) or row.get("ai_module", "")
        evidence_types = matched_labels(EVIDENCE_TERMS, sentence)
        relationship_type = find_first_term(RELATIONSHIP_TERMS, sentence)
        if not ai_module or not (evidence_types or relationship_type):
            continue
        financial_translation = infer_financial_translation(evidence_types)
        evidence_type = ";".join(evidence_types) if evidence_types else "relationship_evidence"
        confidence = confidence_for(source_type, ai_module, financial_translation, len(extracted) + 1)
        extracted.append(
            {
                "as_of": as_of,
                "company": row.get("company_name") or row.get("company") or row.get("symbol") or "",
                "symbol": (row.get("symbol") or "").upper(),
                "source_url": row.get("source_url") or "",
                "source_type": source_type,
                "published_at": row.get("source_date") or row.get("published_at") or "",
                "evidence_type": evidence_type,
                "claim": sentence,
                "source_excerpt_locator": f"{text_path}#sentence-{index}",
                "ai_module": ai_module,
                "relationship_type": relationship_type,
                "counterparty": row.get("counterparty") or "",
                "financial_translation": financial_translation,
                "confidence": confidence,
            }
        )
        if len(extracted) >= 12:
            break
    return extracted


def ingest_rows(rows: list[dict[str, str]], cache_dir: Path, as_of: str, timeout: float, max_docs: int) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    raw_dir = cache_dir / "raw"
    text_dir = cache_dir / "text"
    raw_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)
    tasks: list[dict[str, str]] = []
    evidence: list[dict[str, str]] = []
    fetched_text_by_url: dict[str, tuple[Path, str]] = {}
    for row in rows:
        url = str(row.get("source_url") or "").strip()
        task = {
            "as_of": as_of,
            "symbol": str(row.get("symbol") or "").upper(),
            "company": row.get("company_name") or row.get("company") or "",
            "source_url": url,
            "source_type": row.get("source_type") or "",
            "published_at": row.get("source_date") or "",
            "status": "",
            "raw_path": "",
            "text_path": "",
            "error": "",
        }
        if not url:
            task["status"] = "missing_source_url"
            tasks.append(task)
            continue
        if url in fetched_text_by_url:
            text_path, extracted_text = fetched_text_by_url[url]
            task["status"] = "duplicate_source_url_reused"
            task["text_path"] = str(text_path)
            evidence.extend(extract_evidence(row, text_path, extracted_text, as_of))
            tasks.append(task)
            continue
        if len(fetched_text_by_url) >= max_docs:
            task["status"] = "skipped_max_docs"
            tasks.append(task)
            continue
        key = source_key(url)
        raw_path = raw_dir / f"{key}.raw"
        text_path = text_dir / f"{key}.txt"
        task["raw_path"] = str(raw_path)
        task["text_path"] = str(text_path)
        try:
            payload, content_type = fetch_url(url, timeout)
            raw_path.write_bytes(payload)
            decoded = decode_bytes(payload, content_type)
            extracted_text = strip_html(decoded) if "<html" in decoded[:500].lower() or "<body" in decoded[:2000].lower() else decoded
            text_path.write_text(extracted_text, encoding="utf-8", errors="replace")
            task["status"] = "fetched"
            fetched_text_by_url[url] = (text_path, extracted_text)
            evidence.extend(extract_evidence(row, text_path, extracted_text, as_of))
        except (OSError, urllib.error.URLError, ValueError) as exc:
            task["status"] = "fetch_error"
            task["error"] = str(exc)[:500]
        tasks.append(task)
    return tasks, evidence


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_markdown(path: Path, tasks: list[dict[str, str]], evidence: list[dict[str, str]], as_of: str) -> None:
    status_counts: dict[str, int] = {}
    for row in tasks:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    lines = [
        f"# AI Infra Source Evidence Extracts - {as_of}",
        "",
        "Source ingestion does not promote companies. Evidence here feeds the promotion gate.",
        "",
        f"- source tasks: {len(tasks)}",
        f"- evidence rows: {len(evidence)}",
        f"- generated_at: {datetime.now().astimezone().isoformat(timespec='seconds')}",
        "",
        "## Task Status",
        "",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(f"- {status}: {count}")
    lines.extend(
        [
            "",
            "## Evidence Preview",
            "",
            "| Symbol | Source Type | Confidence | Module | Evidence | Claim |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in evidence[:50]:
        claim = str(row.get("claim") or "").replace("|", "/")[:140]
        lines.append(
            f"| {row.get('symbol')} | {row.get('source_type')} | {row.get('confidence')} | "
            f"{row.get('ai_module')} | {row.get('evidence_type')} | {claim} |"
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    candidate_rows = read_csv(args.expansion_candidates)
    sec_rows = sec_source_rows(
        candidate_rows,
        sec_db=args.sec_db,
        forms=[item.strip() for item in str(args.sec_forms).split(",")],
        per_symbol=args.sec_per_symbol,
    )
    rows = [*sec_rows, *candidate_rows]
    tasks, evidence = ingest_rows(rows, args.cache_dir, args.as_of, args.timeout, args.max_docs)
    task_path = args.reports_dir / "source_document_tasks_v1.csv"
    evidence_path = args.reports_dir / "source_evidence_extracts_v1.csv"
    md_path = args.reports_dir / "source_evidence_extracts_v1.md"
    write_csv(task_path, TASK_FIELDS, tasks)
    write_csv(evidence_path, EVIDENCE_FIELDS, evidence)
    write_markdown(md_path, tasks, evidence, args.as_of)
    print(f"AI-infra source document tasks written: {task_path} rows={len(tasks)}")
    print(f"AI-infra source evidence extracts written: {evidence_path} rows={len(evidence)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
