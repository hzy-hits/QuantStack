#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_ai_supply_chain_relationships import normalize_row  # noqa: E402


SOURCE_CONFIRMED_FIELDS = [
    "relationship_id",
    "as_of",
    "market",
    "primary_symbol",
    "counterparty_symbol",
    "customer_symbol",
    "symbols",
    "layer",
    "relationship_type",
    "supply_chain_role",
    "bottleneck_focus",
    "source_name",
    "source_type",
    "source_url",
    "source_date",
    "confidence",
    "notes",
    "review_state",
]


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name("ai_supply_chain_relationships_source_confirmed.csv")


def stringify_symbols(value: Any) -> str:
    if isinstance(value, list):
        return ";".join(str(item).strip().upper() for item in value if str(item).strip())
    return str(value or "").strip()


def promote_rows(input_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    promoted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen: set[str] = set()
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        for line_no, raw in enumerate(csv.DictReader(handle), start=2):
            review_state = str(raw.get("review_state") or "").strip().lower()
            if review_state != "source_confirmed":
                rejected.append({"line": line_no, "errors": ["not_source_confirmed"], "row": raw})
                continue
            normalized, errors = normalize_row(raw)
            if normalized is None:
                rejected.append({"line": line_no, "errors": errors, "row": raw})
                continue
            relationship_id = str(normalized.get("relationship_id") or "")
            if relationship_id in seen:
                rejected.append({"line": line_no, "errors": ["duplicate_relationship_id"], "row": raw})
                continue
            seen.add(relationship_id)
            out = {field: "" for field in SOURCE_CONFIRMED_FIELDS}
            for key, value in normalized.items():
                out[key] = stringify_symbols(value) if key == "symbols" else value
            out["review_state"] = "source_confirmed"
            promoted.append(out)
    promoted.sort(key=lambda item: (str(item.get("market")), str(item.get("primary_symbol")), str(item.get("relationship_id"))))
    return promoted, rejected


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SOURCE_CONFIRMED_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in SOURCE_CONFIRMED_FIELDS})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Promote source-confirmed AI supply-chain relationship candidate rows into a builder-ready CSV."
    )
    parser.add_argument("--input", required=True, type=Path, help="Reviewed candidate CSV.")
    parser.add_argument("--output", default=None, type=Path, help="Builder-ready source-confirmed CSV.")
    parser.add_argument("--rejects", default=None, type=Path, help="Optional JSON file with rejected/unpromoted rows.")
    args = parser.parse_args()

    output = args.output or default_output_path(args.input)
    promoted, rejected = promote_rows(args.input)
    write_csv(promoted, output)
    if args.rejects:
        args.rejects.parent.mkdir(parents=True, exist_ok=True)
        args.rejects.write_text(json.dumps(rejected, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(f"AI supply-chain promoted candidate CSV written: {output} promoted={len(promoted)} rejected={len(rejected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
