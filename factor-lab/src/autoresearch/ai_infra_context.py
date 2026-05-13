from __future__ import annotations

import csv
import json
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from src.paths import FACTOR_LAB_ROOT


DEFAULT_AI_INFRA_ROOT = FACTOR_LAB_ROOT.parent / "ai_infra"
UNIVERSE_PATH = Path("data/global_universe_v2.jsonl")
US_ALPHA_QUEUE_PATH = Path("reports/us_alpha_mining_queue_v1.csv")


def ai_infra_enabled() -> bool:
    text = os.environ.get("FACTOR_LAB_AI_INFRA_ONLY", "1").strip().lower()
    return text not in {"0", "false", "no", "off"}


def ai_infra_root() -> Path:
    return Path(os.environ.get("FACTOR_LAB_AI_INFRA_ROOT") or DEFAULT_AI_INFRA_ROOT).expanduser().resolve()


def load_universe_records(root: Path | None = None) -> list[dict[str, Any]]:
    path = (root or ai_infra_root()) / UNIVERSE_PATH
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"bad AI infra universe JSONL line {line_no}: {exc}") from exc
            if isinstance(row, dict):
                rows.append(row)
    return rows


def load_us_alpha_queue(root: Path | None = None) -> list[dict[str, str]]:
    path = (root or ai_infra_root()) / US_ALPHA_QUEUE_PATH
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def split_tickers(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip().upper() for part in re.split(r"\s*/\s*|[,，;；]+", text) if part.strip()]


def _is_exchange_suffixed(symbol: str) -> bool:
    return bool(re.search(r"\.[A-Z]{1,4}$", symbol))


def normalize_cn_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    return f"{digits}.SH" if digits.startswith(("6", "9")) else f"{digits}.SZ"


def is_excluded_record(record: dict[str, Any]) -> bool:
    text = " ".join(
        str(record.get(key) or "")
        for key in ["current_pool", "score_bucket", "evidence_state", "counterevidence"]
    )
    return "排除" in text or str(record.get("score_bucket") or "").lower() == "exclude"


def symbols_for_record(record: dict[str, Any], market: str) -> list[str]:
    market = market.upper()
    raw_symbols = split_tickers(record.get("ticker"))
    if market == "US":
        if record.get("market_country") == "US" or record.get("asset_pool") == "美国资产池":
            return [symbol for symbol in raw_symbols if symbol]
        return [
            symbol
            for symbol in raw_symbols
            if symbol and not _is_exchange_suffixed(symbol) and re.fullmatch(r"[A-Z][A-Z0-9.-]{0,6}", symbol)
        ]
    if market == "CN":
        return [
            normalized
            for normalized in (normalize_cn_symbol(symbol) for symbol in raw_symbols)
            if normalized.endswith((".SZ", ".SH"))
        ]
    return []


def records_by_symbol(market: str, root: Path | None = None) -> dict[str, dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for record in load_universe_records(root):
        if is_excluded_record(record):
            continue
        for symbol in symbols_for_record(record, market):
            by_symbol.setdefault(symbol.upper(), record)
    return by_symbol


def market_symbols(market: str, root: Path | None = None) -> set[str]:
    return set(records_by_symbol(market, root))


def apply_ai_infra_filter(
    df: pd.DataFrame,
    *,
    market: str,
    symbol_col: str,
    root: Path | None = None,
) -> pd.DataFrame:
    if not ai_infra_enabled() or df.empty or symbol_col not in df.columns:
        return df
    symbols = market_symbols(market, root)
    if not symbols:
        return df
    filtered = df[df[symbol_col].astype(str).str.upper().isin(symbols)].copy()
    return filtered.reset_index(drop=True)


def _compact_counts(rows: list[dict[str, Any]], field: str, limit: int = 8) -> str:
    counts = Counter(str(row.get(field) or "unknown") for row in rows)
    if not counts:
        return "-"
    return ", ".join(f"{key}={value}" for key, value in counts.most_common(limit))


def _priority_queue_summary(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "US alpha queue not loaded."
    by_priority = Counter(str(row.get("priority") or "unknown") for row in rows)
    p0 = [str(row.get("ticker") or "").upper() for row in rows if str(row.get("priority") or "") == "P0_us_alpha"]
    p1 = [str(row.get("ticker") or "").upper() for row in rows if str(row.get("priority") or "") == "P1_verify"]
    p2 = [str(row.get("ticker") or "").upper() for row in rows if str(row.get("priority") or "") == "P2_large_cap_context"]
    return (
        f"queue rows={len(rows)} ({', '.join(f'{k}={v}' for k, v in sorted(by_priority.items()))}); "
        f"P0={', '.join(p0) or '-'}; P1={', '.join(p1) or '-'}; P2={', '.join(p2) or '-'}"
    )


def build_ai_infra_session_context(market: str, root: Path | None = None) -> str:
    root = root or ai_infra_root()
    records = load_universe_records(root)
    if not records:
        return ""

    by_symbol = records_by_symbol(market, root)
    market_records = list({id(record): record for record in by_symbol.values()}.values())
    symbols = sorted(by_symbol)
    queue_rows = load_us_alpha_queue(root) if market.lower() == "us" else []
    queue_symbols = {str(row.get("ticker") or "").upper() for row in queue_rows}
    missing_from_queue = [symbol for symbol in symbols if symbol not in queue_symbols] if market.lower() == "us" else []

    lines = [
        "## AI Infra Universe Context",
        f"- Upstream root: `{root}`.",
        f"- Contract: mine the AI-infra BFS universe first; broad-market ideas are allowed only as source-review expansion candidates.",
        f"- Loaded universe rows: {len(records)} total; {len(symbols)} {market.upper()} tradable symbols after exclusions/aliases.",
        f"- {market.upper()} symbols: {', '.join(symbols) if symbols else '-'}",
        f"- Asset pools: {_compact_counts(market_records, 'asset_pool')}",
        f"- BFS depths: {_compact_counts(market_records, 'bfs_depth')}",
        f"- Current pools: {_compact_counts(market_records, 'current_pool')}",
    ]
    if market.lower() == "us":
        lines += [
            f"- US alpha queue is the first review queue, not the full universe: {_priority_queue_summary(queue_rows)}",
            f"- US symbols outside the current alpha queue to keep mining: {', '.join(missing_from_queue) if missing_from_queue else '-'}",
        ]
    lines += [
        "- Research method from `ai_infra` md docs: start at LLM demand, map D1-D3 bottlenecks, require original-source evidence, keep D4-D5 as radar unless it can block D0-D2.",
        "- Factor Lab should search for price/volume behavior that exposes AI-infra leadership, accumulation, lifecycle pullbacks or bottleneck repricing; do not hard-code tickers.",
        "- Put missing filings/transcripts/news, backlog/capex/margin/FCF, credit/CDS, options IV/skew/VRP/flow and option-leg ledger needs in DATA_REQUIREMENTS.",
        "- Portfolio readiness must be evaluated as long alpha return minus beta hedge return with residual risk attribution.",
    ]
    return "\n".join(lines)
