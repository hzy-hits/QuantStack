#!/usr/bin/env python3
"""Generate the AI-infra expansion-candidate lane.

This queue is the intake for companies that are not yet in
``ai_infra/data/global_universe_v2.jsonl``.  Rows here are research candidates
only: they must be backed by original-source evidence and promoted before the
quant rankers or alpha queues may use them.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_UNIVERSE = STACK_ROOT / "ai_infra" / "data" / "global_universe_v2.jsonl"
DEFAULT_MANUAL = STACK_ROOT / "ai_infra" / "data" / "expansion_candidates.manual.csv"
DEFAULT_NAME_ZH_OVERRIDES = STACK_ROOT / "ai_infra" / "data" / "company_name_zh_overrides.csv"
DEFAULT_DISCOVERY_JSON = (
    STACK_ROOT
    / "factor-lab"
    / "reports"
    / "autoresearch_exports"
    / "ai_supply_chain"
    / "ai_supply_chain_discovery.json"
)
DEFAULT_RELATIONSHIP_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_supply_chain_candidates"
DEFAULT_REPORTS_DIR = STACK_ROOT / "ai_infra" / "reports"

OUTPUT_FIELDS = [
    "rank",
    "as_of",
    "symbol",
    "company_name",
    "company_name_zh",
    "market",
    "ai_module",
    "discovered_from",
    "candidate_reason",
    "bfs_seed",
    "bfs_depth_estimate",
    "source_url",
    "source_type",
    "source_date",
    "evidence_state",
    "confidence",
    "reviewer_action",
    "relationship_type",
    "counterparty",
    "financial_translation",
    "review_state",
    "candidate_score",
    "source_name",
    "source_table",
    "counterevidence",
]

COMMON_FALSE_TICKERS = {
    "AI",
    "BFS",
    "CEO",
    "CFO",
    "COO",
    "CPU",
    "DCF",
    "EBITDA",
    "ETF",
    "FCF",
    "GPU",
    "HBM",
    "IR",
    "LLM",
    "OOS",
    "PASS",
    "RPO",
    "SEC",
    "US",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate AI-infra source-review expansion candidates.")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="As-of date, YYYY-MM-DD.")
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--manual-csv", type=Path, default=DEFAULT_MANUAL)
    parser.add_argument("--name-zh-overrides", type=Path, default=DEFAULT_NAME_ZH_OVERRIDES)
    parser.add_argument("--discovery-json", type=Path, default=DEFAULT_DISCOVERY_JSON)
    parser.add_argument("--relationship-root", type=Path, default=DEFAULT_RELATIONSHIP_ROOT)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    return parser.parse_args()


def normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    return re.sub(r"\s+", "", text)


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


def load_name_zh_overrides(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        return {
            str(row.get("symbol") or "").strip().upper(): str(row.get("company_zh") or "").strip()
            for row in csv.DictReader(handle)
            if str(row.get("symbol") or "").strip() and str(row.get("company_zh") or "").strip()
        }


def has_cjk(value: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", value or ""))


def company_name_zh_for(row: dict[str, str], overrides: dict[str, str]) -> str:
    symbol = normalize_symbol(row.get("symbol"))
    company = str(row.get("company_name") or row.get("company") or symbol).strip()
    market = infer_market(symbol, row.get("market"))
    if market == "US":
        return company
    if symbol in overrides:
        return overrides[symbol]
    if has_cjk(company):
        return company
    return company


def split_symbols(value: Any) -> list[str]:
    text = str(value or "")
    parts = re.split(r"[,\s;|]+", text)
    return sorted({normalize_symbol(part) for part in parts if normalize_symbol(part)})


def symbols_from_text(value: Any) -> list[str]:
    text = str(value or "")
    raw = re.findall(r"\b[A-Z]{1,5}(?:\.[A-Z]{1,3})?\b|\b\d{6}\.(?:SZ|SH|BJ)\b", text)
    return sorted({symbol for symbol in (normalize_symbol(item) for item in raw) if symbol not in COMMON_FALSE_TICKERS})


def infer_market(symbol: str, fallback: Any = "") -> str:
    text = str(fallback or "").strip().upper()
    if text:
        if text in {"CN", "CHINA", "中国", "A"}:
            return "CN"
        if text in {"US", "USA", "美国"}:
            return "US"
        if text in {"HK", "HONGKONG", "香港"}:
            return "HK"
        return text
    if re.search(r"\.(SZ|SH|BJ)$", symbol):
        return "CN"
    if symbol.endswith(".HK"):
        return "HK"
    return "US"


def clean_text(value: Any, limit: int = 500) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def candidate_score(row: dict[str, str]) -> float:
    source_type = str(row.get("source_type") or "").lower()
    module = str(row.get("ai_module") or "").lower()
    relationship_type = str(row.get("relationship_type") or "").lower()
    market = str(row.get("market") or "").upper()
    bfs_depth = str(row.get("bfs_depth_estimate") or "")

    score = 0.0
    if any(term in source_type for term in ["sec_filing", "10-k", "10-q", "8-k", "20-f", "6-k", "s-1"]):
        score += 30
    elif any(term in source_type for term in ["official", "investor", "annual_report", "earnings_transcript"]):
        score += 34
    elif "news_review_candidate" in source_type:
        score += 10
    elif "factor_lab_hypothesis" in source_type:
        score += 6

    if any(term in module for term in ["power", "grid", "cooling", "thermal", "datacenter", "data center"]):
        score += 18
    elif any(term in module for term in ["optical", "cpo", "network", "hbm", "memory", "packaging", "test", "eda"]):
        score += 16
    elif any(term in module for term in ["compute", "accelerator", "gpu", "asic"]):
        score += 14
    elif module:
        score += 8

    if any(term in relationship_type for term in ["contract", "order", "supplier", "customer", "agreement"]):
        score += 16
    elif any(term in relationship_type for term in ["partnership", "deployment", "integration", "qualification"]):
        score += 10

    if row.get("financial_translation"):
        score += 16
    if market in {"US", "CN", "HK"}:
        score += 8
    if re.search(r"D[0-3]", bfs_depth):
        score += 8
    elif bfs_depth:
        score += 4

    if not row.get("source_url"):
        score -= 15
    if "news_review_candidate" in source_type:
        score -= 8
    if "factor_lab_hypothesis" in source_type:
        score -= 10
    return round(max(0.0, min(100.0, score)), 2)


def latest_relationship_candidate_csv(root: Path, as_of: str) -> Path | None:
    exact = root / as_of / "ai_supply_chain_relationship_candidates.csv"
    if exact.exists():
        return exact
    if not root.exists():
        return None
    dated = sorted(path for path in root.iterdir() if path.is_dir())
    for directory in reversed(dated):
        csv_path = directory / "ai_supply_chain_relationship_candidates.csv"
        if csv_path.exists():
            return csv_path
    return None


def default_candidate(row: dict[str, Any], as_of: str) -> dict[str, str]:
    out = {field: "" for field in OUTPUT_FIELDS}
    for field in OUTPUT_FIELDS:
        if field in row:
            out[field] = clean_text(row.get(field), limit=1200)
    out["as_of"] = as_of
    out["symbol"] = normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("primary_symbol"))
    out["company_name"] = clean_text(row.get("company_name") or row.get("company") or out["symbol"])
    out["company_name_zh"] = clean_text(row.get("company_name_zh"))
    out["market"] = infer_market(out["symbol"], row.get("market"))
    out["ai_module"] = clean_text(row.get("ai_module") or row.get("layer") or row.get("ai_supercycle_layer"))
    out["source_url"] = clean_text(row.get("source_url") or row.get("url"))
    out["source_type"] = clean_text(row.get("source_type") or "candidate_missing_source_type")
    out["source_date"] = clean_text(row.get("source_date") or row.get("published_at") or row.get("as_of"))
    out["evidence_state"] = "pending_original_source_verification"
    out["confidence"] = clean_text(row.get("confidence") or "unreviewed")
    out["review_state"] = clean_text(row.get("review_state") or "needs_human_source_review")
    out["reviewer_action"] = clean_text(
        row.get("reviewer_action")
        or "open_original_source; fill evidence card; set source_confirmed only after primary-source verification"
    )
    return out


def manual_candidates(path: Path, as_of: str, universe_symbols: set[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for raw in csv.DictReader(handle):
            item = default_candidate(raw, as_of)
            if not item["symbol"] or item["symbol"] in universe_symbols:
                continue
            item["discovered_from"] = item["discovered_from"] or f"manual:{path}"
            item["candidate_reason"] = item["candidate_reason"] or "manual source-review expansion candidate"
            rows.append(item)
    return rows


def relationship_candidates(path: Path | None, as_of: str, universe_symbols: set[str]) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for raw in csv.DictReader(handle):
            source_symbols = split_symbols(raw.get("symbols"))
            source_symbols.extend(split_symbols(raw.get("primary_symbol")))
            source_symbols.extend(split_symbols(raw.get("counterparty_symbol")))
            source_symbols = sorted({symbol for symbol in source_symbols if symbol and symbol not in universe_symbols})
            for symbol in source_symbols:
                item = default_candidate({**raw, "symbol": symbol}, as_of)
                item["discovered_from"] = f"relationship_candidate:{raw.get('relationship_id') or path.parent.name}"
                item["candidate_reason"] = clean_text(raw.get("evidence_text") or raw.get("headline"))
                item["bfs_seed"] = clean_text(raw.get("primary_symbol") or raw.get("counterparty_symbol"))
                item["bfs_depth_estimate"] = "D3-D4"
                item["relationship_type"] = clean_text(raw.get("relationship_type"))
                item["counterparty"] = clean_text(raw.get("counterparty_symbol") if symbol != raw.get("counterparty_symbol") else raw.get("primary_symbol"))
                rows.append(item)
    return rows


def factor_lab_candidates(path: Path, as_of: str, universe_symbols: set[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    rows: list[dict[str, str]] = []
    for raw in payload.get("rows") or []:
        explicit_symbols: list[str] = []
        for field in ["symbol", "ticker", "primary_symbol", "company_symbol"]:
            explicit_symbols.extend(split_symbols(raw.get(field)))
        for symbol in sorted(set(explicit_symbols)):
            if symbol in universe_symbols:
                continue
            item = default_candidate({"symbol": symbol, "market": raw.get("market")}, as_of)
            item["discovered_from"] = f"factor_lab_autoresearch:{raw.get('session_id') or 'unknown'}"
            item["candidate_reason"] = clean_text(raw.get("supply_chain_hypothesis") or raw.get("data_requirements"))
            item["ai_module"] = clean_text(raw.get("ai_supercycle_layer"))
            item["bfs_seed"] = clean_text(raw.get("forced_counterparty"))
            item["bfs_depth_estimate"] = "D3-D5"
            item["candidate_score"] = clean_text(raw.get("score"))
            item["source_type"] = "factor_lab_hypothesis"
            item["confidence"] = "unreviewed"
            rows.append(item)
    return rows


def dedupe(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    best: dict[tuple[str, str, str], dict[str, str]] = {}
    def score(row: dict[str, str]) -> float:
        try:
            return float(row.get("candidate_score") or 0.0)
        except ValueError:
            return 0.0

    for row in rows:
        row["candidate_score"] = str(candidate_score(row))
        key = (
            row.get("symbol", ""),
            row.get("source_url", ""),
            row.get("candidate_reason", "")[:120],
        )
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        if score(row) > score(current):
            best[key] = row
    ordered = sorted(
        best.values(),
        key=lambda item: (
            -score(item),
            str(item.get("market") or ""),
            str(item.get("symbol") or ""),
            str(item.get("source_date") or ""),
            str(item.get("discovered_from") or ""),
        ),
    )
    for index, row in enumerate(ordered, start=1):
        row["rank"] = str(index)
    return ordered


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in OUTPUT_FIELDS})


def render_markdown(rows: list[dict[str, str]], as_of: str) -> str:
    lines = [
        f"# AI Infra Expansion Candidates - {as_of}",
        "",
        "This is a source-review intake lane. Rows here are not eligible for alpha queues, rankers, reports, or trading candidates until `evidence_state=source_confirmed` and promotion succeeds.",
        "",
        f"- rows: {len(rows)}",
        f"- generated_at: {datetime.now().astimezone().isoformat(timespec='seconds')}",
        "",
        "| Rank | Symbol | Market | Module | Evidence State | Confidence | Source | Reason |",
        "|---:|---|---|---|---|---|---|---|",
    ]
    for row in rows[:80]:
        source = row.get("source_type") or "-"
        url = row.get("source_url") or ""
        if url.startswith("http"):
            source = f"[{source}]({url})"
        reason = clean_text(row.get("candidate_reason"), 120).replace("|", "/")
        lines.append(
            f"| {row.get('rank')} | {row.get('symbol')} | {row.get('market')} | "
            f"{clean_text(row.get('ai_module'), 40).replace('|', '/')} | {row.get('evidence_state')} | "
            f"{row.get('confidence')} | {source} | {reason} |"
        )
    lines.extend(
        [
            "",
            "## Promotion Gate",
            "",
            "- Do not add a row to `global_universe_v2.jsonl` from this file directly.",
            "- Fetch and read the original source, then write structured evidence to `source_evidence_extracts_v1.csv`.",
            "- Promote only with `source_confirmed`, high/medium confidence, original `source_type`, AI module, and financial translation evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def build_candidates(args: argparse.Namespace) -> list[dict[str, str]]:
    universe_symbols = load_universe_symbols(args.universe)
    name_zh_overrides = load_name_zh_overrides(getattr(args, "name_zh_overrides", DEFAULT_NAME_ZH_OVERRIDES))
    relationship_csv = latest_relationship_candidate_csv(args.relationship_root, args.as_of)
    rows: list[dict[str, str]] = []
    rows.extend(manual_candidates(args.manual_csv, args.as_of, universe_symbols))
    rows.extend(relationship_candidates(relationship_csv, args.as_of, universe_symbols))
    rows.extend(factor_lab_candidates(args.discovery_json, args.as_of, universe_symbols))
    for row in rows:
        if not row.get("company_name_zh"):
            row["company_name_zh"] = company_name_zh_for(row, name_zh_overrides)
    return dedupe(rows)


def main() -> int:
    args = parse_args()
    rows = build_candidates(args)
    csv_path = args.reports_dir / "expansion_candidates_v1.csv"
    md_path = args.reports_dir / "expansion_candidates_v1.md"
    write_csv(csv_path, rows)
    md_path.write_text(render_markdown(rows, args.as_of) + "\n", encoding="utf-8")
    print(f"AI-infra expansion candidates written: {csv_path} rows={len(rows)}")
    print(f"Review brief: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
