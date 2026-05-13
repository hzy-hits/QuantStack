from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


FACTOR_LAB_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNTIME_ROOT = FACTOR_LAB_ROOT / "runtime" / "autoresearch"
DEFAULT_EXPORT_ROOT = FACTOR_LAB_ROOT / "reports" / "autoresearch_exports" / "ai_supply_chain"

AI_SUPERCYCLE_KEYWORDS = {
    "ai",
    "artificial intelligence",
    "accelerator",
    "gpu",
    "tpu",
    "hbm",
    "memory",
    "storage",
    "optical",
    "cpo",
    "fiber",
    "networking",
    "cloud",
    "datacenter",
    "data center",
    "inference",
    "semiconductor",
    "packaging",
    "testing",
    "substrate",
    "material",
    "power",
    "grid",
    "nuclear",
    "space",
    "satellite",
    "supply",
    "supplier",
    "customer",
    "contract",
    "capacity",
    "bottleneck",
    "pytorch",
    "lab",
}
GENERIC_RELATIONSHIP_KEYWORDS = {
    "supply",
    "supplier",
    "customer",
    "contract",
    "capacity",
    "bottleneck",
}
STRONG_AI_SUPERCYCLE_KEYWORDS = AI_SUPERCYCLE_KEYWORDS - GENERIC_RELATIONSHIP_KEYWORDS


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_bool_text(value: Any, default: bool = True) -> bool:
    text = str(value if value is not None else "").strip().lower()
    if text in {"false", "0", "no", "n"}:
        return False
    if text in {"true", "1", "yes", "y"}:
        return True
    return default


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _joined_text(row: dict[str, Any]) -> str:
    fields = [
        row.get("name"),
        row.get("sleeve_id"),
        row.get("hypothesis"),
        row.get("mispricing_source"),
        row.get("forced_counterparty"),
        row.get("data_requirements"),
        row.get("failure_mode"),
        row.get("ai_supercycle_layer"),
        row.get("supply_chain_hypothesis"),
    ]
    return " ".join(str(field or "").lower() for field in fields)


def is_ai_supply_chain_candidate(row: dict[str, Any]) -> bool:
    layer = str(row.get("ai_supercycle_layer") or "").strip().lower()
    if layer and layer != "none":
        return True
    hypothesis = str(row.get("supply_chain_hypothesis") or "").strip().lower()
    if hypothesis and hypothesis != "none":
        return True
    text = _joined_text(row)
    return any(
        re.search(rf"(?<![a-z0-9_]){re.escape(keyword)}(?![a-z0-9_])", text)
        for keyword in STRONG_AI_SUPERCYCLE_KEYWORDS
    )


def candidate_score(row: dict[str, Any]) -> float:
    score = 0.0
    if str(row.get("gates") or "").upper() == "PASS" or row.get("gates_passed"):
        score += 18.0
    if str(row.get("oos") or "").upper() == "PASS":
        score += 16.0
    if str(row.get("checks_status") or "").lower() in {"passed", "pass"}:
        score += 12.0
    if str(row.get("status") or "").lower() in {"candidate", "kept", "keep"}:
        score += 10.0
    if str(row.get("decision") or "").lower() == "keep":
        score += 10.0
    if str(row.get("report_contract") or "") in {"fresh_buy_gate", "action_overlay", "setup_overlay"}:
        score += 8.0
    score += min(18.0, abs(_as_float(row.get("is_ic_ir"))) * 24.0)
    if str(row.get("ai_supercycle_layer") or "").strip().lower() not in {"", "none"}:
        score += 12.0
    if str(row.get("supply_chain_hypothesis") or "").strip().lower() not in {"", "none"}:
        score += 8.0
    return round(min(score, 100.0), 2)


def normalize_discovery_row(row: dict[str, Any], source_log: Path | None = None) -> dict[str, Any]:
    relationship_required = _as_bool_text(row.get("relationship_evidence_required"), default=True)
    hypothesis = str(row.get("supply_chain_hypothesis") or "").strip()
    if not hypothesis or hypothesis.lower() == "none":
        evidence_state = "factor_hypothesis_only"
    elif relationship_required:
        evidence_state = "needs_source_confirmation"
    else:
        evidence_state = "claims_source_available_review_required"
    return {
        "market": str(row.get("market") or "").lower(),
        "session_id": row.get("session_id"),
        "name": row.get("name"),
        "formula": row.get("formula"),
        "sleeve_id": row.get("sleeve_id"),
        "ai_supercycle_layer": row.get("ai_supercycle_layer") or "none",
        "supply_chain_hypothesis": hypothesis,
        "relationship_evidence_required": relationship_required,
        "evidence_state": evidence_state,
        "score": candidate_score(row),
        "gates": row.get("gates") or ("PASS" if row.get("gates_passed") else "FAIL"),
        "oos": row.get("oos"),
        "checks_status": row.get("checks_status"),
        "decision": row.get("decision"),
        "status": row.get("status"),
        "is_ic": row.get("is_ic"),
        "is_ic_ir": row.get("is_ic_ir"),
        "is_sharpe": row.get("is_sharpe"),
        "report_contract": row.get("report_contract") or "research_only",
        "money_readiness": row.get("money_readiness") or "research_only",
        "mispricing_source": row.get("mispricing_source"),
        "forced_counterparty": row.get("forced_counterparty"),
        "data_requirements": row.get("data_requirements"),
        "failure_mode": row.get("failure_mode"),
        "source_log": str(source_log) if source_log else row.get("source_log"),
        "contract_note": "Discovery queue only. Supplier/customer facts must be promoted through ai_supply_chain_relationships.yaml with source_url/source_type/confidence.",
    }


def discover_ai_supply_chain_rows(log_paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for path in log_paths:
        for raw in load_jsonl(path):
            if not is_ai_supply_chain_candidate(raw):
                continue
            row = normalize_discovery_row(raw, path)
            key = (str(row.get("market")), str(row.get("session_id")), str(row.get("name")))
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    rows.sort(
        key=lambda item: (
            -(item.get("score") or 0.0),
            str(item.get("market") or ""),
            str(item.get("name") or ""),
        )
    )
    return rows


def default_log_paths(runtime_root: Path = DEFAULT_RUNTIME_ROOT) -> list[Path]:
    return sorted(runtime_root.glob("*/autoresearch.jsonl"))


def render_discovery_markdown(payload: dict[str, Any], limit: int = 80) -> str:
    rows = payload.get("rows") or []
    summary = payload.get("summary") or {}
    lines = [
        "# Factor Lab AI Supply Chain Discovery Queue",
        "",
        "This is a research queue, not a source-confirmed relationship ledger. Rows here must be verified through official filings, press releases, transcripts or reliable news before they can enter `data/ai_supply_chain_relationships.yaml`.",
        "",
        f"- rows: {summary.get('rows', 0)}",
        f"- needs_source_confirmation: {summary.get('needs_source_confirmation', 0)}",
        f"- factor_hypothesis_only: {summary.get('factor_hypothesis_only', 0)}",
        "",
    ]
    if not rows:
        lines += ["- No AI supply-chain discovery rows.", ""]
        return "\n".join(lines).rstrip() + "\n"
    lines += [
        "| Score | Market | Name | Layer | Evidence | IC_IR | Contract | Hypothesis / data required |",
        "|---:|---|---|---|---|---:|---|---|",
    ]
    for row in rows[:limit]:
        hypothesis = row.get("supply_chain_hypothesis") or row.get("data_requirements") or "-"
        lines.append(
            f"| {row.get('score')} | {row.get('market')} | {row.get('name')} | "
            f"{row.get('ai_supercycle_layer') or 'none'} | {row.get('evidence_state')} | "
            f"{_as_float(row.get('is_ic_ir')):.3f} | {row.get('report_contract')} | "
            f"{str(hypothesis)[:160]} |"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_discovery_payload(log_paths: list[Path] | None = None) -> dict[str, Any]:
    paths = log_paths or default_log_paths()
    rows = discover_ai_supply_chain_rows(paths)
    by_state: dict[str, int] = {}
    by_market: dict[str, int] = {}
    for row in rows:
        by_state[row["evidence_state"]] = by_state.get(row["evidence_state"], 0) + 1
        market = row.get("market") or "unknown"
        by_market[market] = by_market.get(market, 0) + 1
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source_logs": [str(path) for path in paths],
        "summary": {
            "rows": len(rows),
            "needs_source_confirmation": by_state.get("needs_source_confirmation", 0),
            "factor_hypothesis_only": by_state.get("factor_hypothesis_only", 0),
            "claims_source_available_review_required": by_state.get("claims_source_available_review_required", 0),
            "by_market": by_market,
        },
        "rows": rows,
    }


def export_discovery_bundle(
    output_dir: Path = DEFAULT_EXPORT_ROOT,
    log_paths: list[Path] | None = None,
) -> dict[str, Path]:
    payload = build_discovery_payload(log_paths)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "ai_supply_chain_discovery.json"
    md_path = output_dir / "ai_supply_chain_discovery.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_discovery_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
