"""AI Infra universe bridge for market rankers.

The `ai_infra/` workbench is the upstream research universe. Rankers may still
use market data, options, tape and news to decide timing, but candidate
membership should come from this source when the AI-infra mandate is active.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


STACK_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_AI_INFRA_ROOT = STACK_ROOT / "ai_infra"
UNIVERSE_PATH = Path("data/global_universe_v2.jsonl")


@dataclass(frozen=True)
class UniverseGateResult:
    market: str
    raw_candidate_count: int
    retained_candidate_count: int
    added_universe_count: int
    universe_symbol_count: int
    excluded_symbols: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "raw_candidate_count": self.raw_candidate_count,
            "retained_candidate_count": self.retained_candidate_count,
            "added_universe_count": self.added_universe_count,
            "universe_symbol_count": self.universe_symbol_count,
            "excluded_symbols": list(self.excluded_symbols),
            "contract": "ai_infra_universe_only",
        }


def normalize_us_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


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


def split_tickers(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = re.split(r"\s*/\s*|[,，;；]+", text)
    return [part.strip().upper() for part in parts if part.strip()]


def _is_exchange_suffixed(symbol: str) -> bool:
    return bool(re.search(r"\.[A-Z]{1,4}$", symbol))


def load_records(ai_infra_root: Path | None = None) -> list[dict[str, Any]]:
    root = ai_infra_root or DEFAULT_AI_INFRA_ROOT
    path = root / UNIVERSE_PATH
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


def is_excluded_record(record: dict[str, Any]) -> bool:
    text = " ".join(
        str(record.get(key) or "")
        for key in ["current_pool", "score_bucket", "evidence_state", "counterevidence"]
    )
    return "排除" in text or str(record.get("score_bucket") or "").lower() == "exclude"


def market_symbols_for_record(record: dict[str, Any], market: str) -> list[str]:
    market = market.upper()
    raw_symbols = split_tickers(record.get("ticker"))
    if market == "US":
        if record.get("market_country") == "US" or record.get("asset_pool") == "美国资产池":
            return [normalize_us_symbol(symbol) for symbol in raw_symbols]
        # Satellite rows sometimes include a US ADR alias, for example
        # `2330.TW / TSM`. Keep the plain ADR token, not the local exchange leg.
        return [
            normalize_us_symbol(symbol)
            for symbol in raw_symbols
            if symbol and not _is_exchange_suffixed(symbol) and re.fullmatch(r"[A-Z][A-Z0-9.-]{0,6}", symbol)
        ]
    if market == "CN":
        out = []
        for symbol in raw_symbols:
            normalized = normalize_cn_symbol(symbol)
            if normalized.endswith((".SZ", ".SH")):
                out.append(normalized)
        return out
    return []


def records_by_symbol(market: str, ai_infra_root: Path | None = None) -> dict[str, dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for record in load_records(ai_infra_root):
        if is_excluded_record(record):
            continue
        for symbol in market_symbols_for_record(record, market):
            by_symbol.setdefault(symbol, record)
    return by_symbol


def _depth_numbers(depth: Any) -> list[int]:
    return [int(value) for value in re.findall(r"D(\d+)", str(depth or ""))]


def priority_from_depth(depth: Any) -> int:
    values = _depth_numbers(depth)
    if not values:
        return 5
    low = min(values)
    high = max(values)
    if high <= 3:
        return 1 if low <= 2 else 2
    if high <= 4:
        return 3
    return 4


def layer_from_record(record: dict[str, Any]) -> str:
    text = " ".join(
        str(record.get(key) or "").lower()
        for key in ["module", "dependency_path", "dependency_edge", "overseas_bottleneck", "up_downstream"]
    )
    if any(token in text for token in ["hbm", "dram", "nand", "ssd", "storage", "memory"]):
        return "ai_memory_storage"
    if any(token in text for token in ["optical", "optic", "cpo", "photonic", "laser", "network", "ethernet", "serdes", "retimer", "connectivity"]):
        return "ai_networking_optical_cpo"
    if any(token in text for token in ["power", "thermal", "cooling", "switchgear", "electric", "nuclear", "grid", "firm power"]):
        return "ai_power_nuclear_grid"
    if any(token in text for token in ["cloud", "neocloud", "gpu-as-a-service", "data center developer", "powered land"]):
        return "ai_datacenter_edge_infra"
    if any(token in text for token in ["gpu", "cuda", "asic", "accelerator", "server/rack", "ai server", "tpu"]):
        return "ai_compute_accelerators"
    if any(token in text for token in ["packaging", "cowos", "substrate", "eda", "test", "probe", "metrology", "inspection", "foundry", "molding", "dicing"]):
        return "ai_chip_equipment_materials_packaging"
    if any(token in text for token in ["gemini", "openai", "anthropic", "model", "llm"]):
        return "ai_labs_cloud_models"
    return "ai_labs_cloud_models" if str(record.get("bfs_depth") or "").startswith("D0") else "ai_chip_equipment_materials_packaging"


def enrich_candidate(candidate: dict[str, Any], record: dict[str, Any], *, market: str, symbol: str) -> dict[str, Any]:
    layer = layer_from_record(record)
    priority = priority_from_depth(record.get("bfs_depth"))
    out = dict(candidate)
    out["symbol"] = symbol
    out.setdefault("name", record.get("company") or "")
    out.setdefault("policy", "ai_infra_bfs_universe")
    out.setdefault("state", "AI Infra Universe Watch")
    out.setdefault("execution_source", "ai_infra_universe")
    out.setdefault("alpha_factory_role", "ai_infra_rank_only")
    out.setdefault("reason", "AI Infra BFS universe member; rank by price, flow, news, options and risk before any R.")
    if market.upper() == "CN":
        out.setdefault("narrative_group", "ai_infra")
    out["ai_infra_universe"] = True
    out["ai_infra_asset_pool"] = record.get("asset_pool")
    out["ai_infra_market_country"] = record.get("market_country")
    out["ai_infra_bfs_depth"] = record.get("bfs_depth")
    out["ai_infra_module"] = record.get("module")
    out["ai_infra_current_pool"] = record.get("current_pool")
    out["ai_infra_total_score"] = record.get("total_score")
    out["ai_infra_score_bucket"] = record.get("score_bucket")
    out["ai_infra_evidence_state"] = record.get("evidence_state")
    out["ai_infra_counterevidence"] = record.get("counterevidence")
    out["ai_infra_dependency_path"] = record.get("dependency_path")
    out["ai_infra_dependency_edge"] = record.get("dependency_edge")
    out["ai_infra_verification_status"] = "pending_original_source_verification"
    out.setdefault("supercycle_layer", layer)
    out.setdefault("supercycle_priority", priority)
    out.setdefault("supply_chain_role", record.get("module") or "")
    out.setdefault("bottleneck_focus", record.get("overseas_bottleneck") or record.get("dependency_edge") or "")
    out.setdefault("evidence_contract", "source_review_required")
    out.setdefault("research_index", "ai_infra/global_universe_v2")
    return out


def candidate_from_record(record: dict[str, Any], *, market: str, symbol: str) -> dict[str, Any]:
    return enrich_candidate(
        {
            "symbol": symbol,
            "name": record.get("company") or "",
            "policy": "ai_infra_bfs_universe",
            "state": "AI Infra Universe Watch",
            "execution_source": "ai_infra_universe",
            "alpha_factory_role": "ai_infra_rank_only",
        },
        record,
        market=market,
        symbol=symbol,
    )


def filter_and_enrich_candidates(
    candidates: Iterable[dict[str, Any]],
    *,
    market: str,
    ai_infra_root: Path | None = None,
) -> tuple[list[dict[str, Any]], UniverseGateResult]:
    by_symbol = records_by_symbol(market, ai_infra_root)
    retained: list[dict[str, Any]] = []
    excluded: list[str] = []
    normalizer = normalize_cn_symbol if market.upper() == "CN" else normalize_us_symbol
    raw = list(candidates)
    for candidate in raw:
        symbol = normalizer(candidate.get("symbol"))
        record = by_symbol.get(symbol)
        if not symbol:
            continue
        if not record:
            excluded.append(symbol)
            continue
        retained.append(enrich_candidate(candidate, record, market=market, symbol=symbol))
    return retained, UniverseGateResult(
        market=market.upper(),
        raw_candidate_count=len(raw),
        retained_candidate_count=len(retained),
        added_universe_count=0,
        universe_symbol_count=len(by_symbol),
        excluded_symbols=tuple(sorted(set(excluded))),
    )


def merge_with_universe_candidates(
    candidates: Iterable[dict[str, Any]],
    *,
    market: str,
    ai_infra_root: Path | None = None,
    include_all_universe: bool = True,
) -> tuple[list[dict[str, Any]], UniverseGateResult]:
    by_symbol = records_by_symbol(market, ai_infra_root)
    retained, base_gate = filter_and_enrich_candidates(candidates, market=market, ai_infra_root=ai_infra_root)
    seen = {str(row.get("symbol") or "").upper() for row in retained}
    added = []
    if include_all_universe:
        for symbol, record in sorted(by_symbol.items()):
            if symbol not in seen:
                added.append(candidate_from_record(record, market=market, symbol=symbol))
    return retained + added, UniverseGateResult(
        market=market.upper(),
        raw_candidate_count=base_gate.raw_candidate_count,
        retained_candidate_count=len(retained),
        added_universe_count=len(added),
        universe_symbol_count=len(by_symbol),
        excluded_symbols=base_gate.excluded_symbols,
    )
