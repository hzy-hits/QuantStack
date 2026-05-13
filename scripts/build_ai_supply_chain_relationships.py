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
DEFAULT_OUTPUT = STACK_ROOT / "data" / "ai_supply_chain_relationships.yaml"
REQUIRED_FIELDS = {
    "relationship_id",
    "market",
    "primary_symbol",
    "layer",
    "relationship_type",
    "supply_chain_role",
    "source_type",
    "source_url",
    "confidence",
}
VALID_CONFIDENCE = {"high", "medium"}


def split_symbols(value: Any) -> list[str]:
    text = str(value or "")
    parts = re.split(r"[,\s;|]+", text)
    return sorted({part.strip().upper() for part in parts if part.strip()})


def normalize_row(raw: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    row = {key: str(value or "").strip() for key, value in raw.items()}
    missing = sorted(field for field in REQUIRED_FIELDS if not row.get(field))
    confidence = row.get("confidence", "").lower()
    if confidence and confidence not in VALID_CONFIDENCE:
        missing.append("confidence_not_high_or_medium")
    if row.get("source_url") and not re.match(r"^https?://", row["source_url"]):
        missing.append("source_url_not_http")
    review_state = row.get("review_state", "").lower()
    if review_state and review_state != "source_confirmed":
        missing.append("review_state_not_source_confirmed")
    if row.get("source_type", "").lower().endswith("_review_candidate"):
        missing.append("source_type_still_review_candidate")
    if missing:
        return None, missing

    symbols = split_symbols(row.get("symbols"))
    for field in ["primary_symbol", "counterparty_symbol", "customer_symbol"]:
        symbol = row.get(field)
        if symbol:
            symbols.append(symbol.upper())
    symbols = sorted(set(symbols))
    normalized = {
        "relationship_id": row["relationship_id"],
        "as_of": row.get("as_of") or row.get("source_date"),
        "market": row["market"].upper(),
        "primary_symbol": row["primary_symbol"].upper(),
        "counterparty_symbol": row.get("counterparty_symbol", "").upper() or None,
        "customer_symbol": row.get("customer_symbol", "").upper() or None,
        "symbols": symbols,
        "layer": row["layer"],
        "relationship_type": row["relationship_type"],
        "supply_chain_role": row["supply_chain_role"],
        "bottleneck_focus": row.get("bottleneck_focus") or "",
        "source_name": row.get("source_name") or row["source_type"],
        "source_type": row["source_type"],
        "source_url": row["source_url"],
        "source_date": row.get("source_date") or row.get("as_of"),
        "confidence": confidence,
        "notes": row.get("notes") or "",
    }
    return {key: value for key, value in normalized.items() if value not in ("", None, [])}, []


def load_relationship_rows(input_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen: set[str] = set()
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        for idx, raw in enumerate(csv.DictReader(handle), start=2):
            row, errors = normalize_row(raw)
            if row is None:
                rejected.append({"line": idx, "errors": errors, "row": raw})
                continue
            relationship_id = str(row["relationship_id"])
            if relationship_id in seen:
                rejected.append({"line": idx, "errors": ["duplicate_relationship_id"], "row": raw})
                continue
            seen.add(relationship_id)
            valid.append(row)
    valid.sort(key=lambda item: (str(item.get("market")), str(item.get("primary_symbol")), str(item.get("relationship_id"))))
    return valid, rejected


def write_relationships(rows: list[dict[str, Any]], output_path: Path) -> None:
    payload = {
        "version": 1,
        "contract": (
            "Only relationships backed by a source_url and source_type may be used as "
            "source-linked AI supercycle evidence. This file is a research ledger, not a trade instruction."
        ),
        "relationships": rows,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build data/ai_supply_chain_relationships.yaml from a source-confirmed relationship CSV."
    )
    parser.add_argument("--input", required=True, type=Path, help="Raw source-confirmed relationship CSV.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--rejects", default=None, type=Path, help="Optional JSON file for rejected rows.")
    args = parser.parse_args()

    rows, rejected = load_relationship_rows(args.input)
    write_relationships(rows, args.output)
    if args.rejects:
        args.rejects.parent.mkdir(parents=True, exist_ok=True)
        args.rejects.write_text(json.dumps(rejected, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(f"AI supply-chain relationships written: {args.output} rows={len(rows)} rejected={len(rejected)}")
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
