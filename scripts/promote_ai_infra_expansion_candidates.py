#!/usr/bin/env python3
"""Promote source-confirmed AI-infra expansion candidates into the universe."""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATES = STACK_ROOT / "ai_infra" / "reports" / "expansion_candidates_v1.csv"
DEFAULT_EVIDENCE = STACK_ROOT / "ai_infra" / "reports" / "source_evidence_extracts_v1.csv"
DEFAULT_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"
DEFAULT_REPORTS_DIR = STACK_ROOT / "ai_infra" / "reports"

PROMOTED_FIELDS = [
    "as_of",
    "symbol",
    "company_name",
    "market",
    "ai_module",
    "source_url",
    "source_type",
    "source_date",
    "confidence",
    "evidence_state",
    "financial_translation",
    "universe_row",
]

VALID_CONFIDENCE = {"high", "medium"}
PROMOTABLE_STATES = {"source_confirmed"}
DISALLOWED_SOURCE_TYPE_TERMS = {"news", "review_candidate", "factor_lab_hypothesis", "candidate_missing_source_type"}
ORIGINAL_SOURCE_TERMS = {
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
    parser = argparse.ArgumentParser(description="Promote reviewed AI-infra expansion candidates.")
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--candidates", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--apply", action="store_true", help="Append promoted rows to global_universe_v2.jsonl.")
    parser.add_argument("--rebuild", action="store_true", help="After --apply, rebuild universe DB and queues.")
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def normalize_symbol(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().upper())


def load_universe_symbols(path: Path) -> set[str]:
    symbols: set[str] = set()
    if not path.exists():
        return symbols
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        symbol = normalize_symbol(row.get("ticker") or row.get("symbol"))
        if symbol:
            symbols.add(symbol)
    return symbols


def is_http_url(value: Any) -> bool:
    return bool(re.match(r"^https?://", str(value or "").strip(), re.I))


def is_original_source_type(value: Any) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    if any(term in lowered for term in DISALLOWED_SOURCE_TYPE_TERMS):
        return False
    return any(term in lowered for term in ORIGINAL_SOURCE_TERMS)


def market_to_asset_pool(market: str) -> str:
    normalized = market.upper()
    if normalized in {"CN", "CHINA", "A"}:
        return "中国资产池"
    if normalized in {"US", "USA"}:
        return "美国资产池"
    return "卫星资产池"


def financial_evidence_by_key(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    out: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in rows:
        symbol = normalize_symbol(row.get("symbol"))
        url = str(row.get("source_url") or "").strip()
        if not symbol or not url:
            continue
        out.setdefault((symbol, url), []).append(row)
    return out


def best_evidence(candidate: dict[str, str], evidence_index: dict[tuple[str, str], list[dict[str, str]]]) -> dict[str, str] | None:
    symbol = normalize_symbol(candidate.get("symbol"))
    url = str(candidate.get("source_url") or "").strip()
    rows = evidence_index.get((symbol, url), [])
    valid = [
        row
        for row in rows
        if str(row.get("confidence") or "").lower() in VALID_CONFIDENCE
        and is_original_source_type(row.get("source_type"))
        and str(row.get("ai_module") or "").strip()
        and str(row.get("financial_translation") or "").strip()
    ]
    if not valid:
        return None
    valid.sort(
        key=lambda row: (
            0 if str(row.get("confidence") or "").lower() == "high" else 1,
            -len(str(row.get("financial_translation") or "")),
        )
    )
    return valid[0]


def universe_row(candidate: dict[str, str], evidence: dict[str, str], as_of: str) -> dict[str, str]:
    symbol = normalize_symbol(candidate.get("symbol"))
    company = str(candidate.get("company_name") or candidate.get("company") or symbol).strip()
    market = str(candidate.get("market") or evidence.get("market") or "US").strip().upper()
    module = str(candidate.get("ai_module") or evidence.get("ai_module") or "").strip()
    reason = str(candidate.get("candidate_reason") or evidence.get("claim") or "").strip()
    bfs_seed = str(candidate.get("bfs_seed") or candidate.get("counterparty") or evidence.get("counterparty") or "").strip()
    bfs_depth = str(candidate.get("bfs_depth_estimate") or "D3-D4").strip()
    financial_translation = str(evidence.get("financial_translation") or "").strip()
    source_type = str(candidate.get("source_type") or evidence.get("source_type") or "").strip()
    return {
        "asset_pool": market_to_asset_pool(market),
        "market_country": market,
        "ticker": symbol,
        "company": company,
        "mcap_bucket": "",
        "bfs_depth": bfs_depth,
        "module": module,
        "dependency_path": f"{bfs_seed or 'AI infra source review'} -> {symbol}: {reason[:220]}",
        "dependency_edge": "source-confirmed expansion candidate",
        "overseas_bottleneck": module,
        "up_downstream": str(candidate.get("counterparty") or evidence.get("counterparty") or "").strip(),
        "evidence_state": (
            f"原文已证明: expansion source confirmed on {as_of}; "
            f"{financial_translation}; source_type={source_type}"
        ),
        "etf_clue": "",
        "smart_money_clue": "",
        "counterevidence": "",
        "current_pool": "候选池",
        "trading_reach": "",
    }


def validate_candidate(
    candidate: dict[str, str],
    evidence_index: dict[tuple[str, str], list[dict[str, str]]],
    existing_symbols: set[str],
    as_of: str,
) -> tuple[dict[str, str] | None, list[str]]:
    errors: list[str] = []
    symbol = normalize_symbol(candidate.get("symbol"))
    if not symbol:
        errors.append("missing_symbol")
    if symbol in existing_symbols:
        errors.append("already_in_universe")
    if str(candidate.get("evidence_state") or "").strip().lower() not in PROMOTABLE_STATES:
        errors.append("not_source_confirmed")
    if str(candidate.get("confidence") or "").strip().lower() not in VALID_CONFIDENCE:
        errors.append("confidence_not_high_or_medium")
    if not is_http_url(candidate.get("source_url")):
        errors.append("source_url_not_http")
    if not is_original_source_type(candidate.get("source_type")):
        errors.append("source_type_not_original")
    if not str(candidate.get("ai_module") or "").strip():
        errors.append("missing_ai_module")
    if str(candidate.get("counterevidence") or "").strip():
        errors.append("counterevidence_present")
    evidence = best_evidence(candidate, evidence_index)
    if evidence is None:
        errors.append("missing_matching_financial_evidence")
    if errors:
        return None, errors
    row = universe_row(candidate, evidence or {}, as_of)
    promoted = {
        "as_of": as_of,
        "symbol": symbol,
        "company_name": str(candidate.get("company_name") or symbol),
        "market": str(candidate.get("market") or row["market_country"]),
        "ai_module": str(candidate.get("ai_module") or ""),
        "source_url": str(candidate.get("source_url") or ""),
        "source_type": str(candidate.get("source_type") or ""),
        "source_date": str(candidate.get("source_date") or ""),
        "confidence": str(candidate.get("confidence") or ""),
        "evidence_state": str(candidate.get("evidence_state") or ""),
        "financial_translation": str((evidence or {}).get("financial_translation") or ""),
        "universe_row": json.dumps(row, ensure_ascii=False, sort_keys=True),
    }
    return promoted, []


def promote_rows(
    candidates: list[dict[str, str]],
    evidence_rows: list[dict[str, str]],
    existing_symbols: set[str],
    as_of: str,
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    evidence_index = financial_evidence_by_key(evidence_rows)
    promoted: list[dict[str, str]] = []
    rejected: list[dict[str, Any]] = []
    seen_promoted: set[str] = set()
    for index, candidate in enumerate(candidates, start=2):
        row, errors = validate_candidate(candidate, evidence_index, existing_symbols | seen_promoted, as_of)
        if row is None:
            rejected.append({"line": index, "symbol": normalize_symbol(candidate.get("symbol")), "errors": errors, "row": candidate})
            continue
        promoted.append(row)
        seen_promoted.add(row["symbol"])
    promoted.sort(key=lambda item: (item["market"], item["symbol"]))
    return promoted, rejected


def write_promoted_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Backup curated promoted CSV before rewrite (Codex review 2026-05-14).
    if path.exists():
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROMOTED_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in PROMOTED_FIELDS})


def append_universe(universe_path: Path, rows: list[dict[str, str]]) -> int:
    if not rows:
        return 0
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_universe_symbols(universe_path)
    append_rows = []
    for row in rows:
        symbol = row["symbol"]
        if symbol in existing:
            continue
        append_rows.append(json.loads(row["universe_row"]))
        existing.add(symbol)
    if not append_rows:
        return 0
    with universe_path.open("a", encoding="utf-8") as handle:
        if universe_path.exists() and universe_path.stat().st_size > 0:
            handle.write("\n")
        handle.write("\n".join(json.dumps(row, ensure_ascii=False, sort_keys=False) for row in append_rows))
        handle.write("\n")
    return len(append_rows)


def rebuild_ai_infra() -> None:
    commands = [
        [sys.executable, "scripts/build_universe_system.py"],
        [sys.executable, "scripts/generate_source_verification_queue.py"],
        [sys.executable, "scripts/generate_us_alpha_mining_queue.py"],
    ]
    ai_infra_root = STACK_ROOT / "ai_infra"
    for command in commands:
        subprocess.run(command, cwd=ai_infra_root, check=True)


def main() -> int:
    args = parse_args()
    candidates = read_csv(args.candidates)
    evidence_rows = read_csv(args.evidence)
    existing_symbols = load_universe_symbols(args.universe)
    promoted, rejected = promote_rows(candidates, evidence_rows, existing_symbols, args.as_of)
    promoted_path = args.reports_dir / "expansion_candidates_promoted_v1.csv"
    rejects_path = args.reports_dir / "expansion_candidates_rejected_v1.json"
    write_promoted_csv(promoted_path, promoted)
    rejects_path.write_text(json.dumps(rejected, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    appended = 0
    if args.apply:
        appended = append_universe(args.universe, promoted)
        if args.rebuild and appended:
            rebuild_ai_infra()
    print(
        f"AI-infra expansion promotion plan written: {promoted_path} "
        f"promoted={len(promoted)} rejected={len(rejected)} applied={appended} generated_at={datetime.now().isoformat(timespec='seconds')}"
    )
    print(f"Rejects: {rejects_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
