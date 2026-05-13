#!/usr/bin/env python3
"""Verify AI-supercycle research readiness across local artifacts.

The verifier is intentionally conservative. It separates "screening universe is
wired" from "relationship evidence is actually loaded", so final reports do not
turn missing source data into a false sense of readiness. The AI-lab publication
index is auxiliary and reported as a warning when absent.
"""
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml


STACK_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_LAYERS = {
    "ai_labs_cloud_models",
    "ai_compute_accelerators",
    "ai_memory_storage",
    "ai_networking_optical_cpo",
    "ai_datacenter_edge_infra",
    "ai_chip_equipment_materials_packaging",
    "ai_power_nuclear_grid",
    "space_connectivity_datacenter",
    "hard_assets_energy_heavy",
    "excluded_consumer",
}
REQUIRED_US_THEME_LAYERS = REQUIRED_LAYERS - {"hard_assets_energy_heavy", "excluded_consumer"}
REQUIRED_THEME_MEMBERS = {
    "ai_labs_cloud_models": {"GOOGL", "META", "MSFT", "AMZN", "NET"},
    "ai_compute_accelerators": {"NVDA", "AMD", "AVGO", "SMCI", "DELL"},
    "ai_memory_storage": {"MU", "WDC", "STX"},
    "ai_networking_optical_cpo": {"ANET", "COHR", "LITE"},
    "ai_datacenter_edge_infra": {"NET", "VRT", "ETN"},
    "ai_chip_equipment_materials_packaging": {"ASML", "AMAT", "TER", "AXTI"},
    "ai_power_nuclear_grid": {"CEG", "VST", "SMR", "OKLO"},
    "space_connectivity_datacenter": {"RKLB", "ASTS", "LUNR"},
}
REQUIRED_RELATIONSHIP_SYMBOLS = {"AXTI", "NET", "CEG", "DELL", "MU"}
TOP_CONFERENCES = {"NeurIPS", "ICML", "ICLR", "CVPR"}
REQUIRED_RESEARCH_THEMES = {
    "hbm_structural_supercycle",
    "cowos_advanced_packaging_bottleneck",
    "ai_hbm_test_metrology",
    "optical_cpo_800g_1p6t",
    "rack_scale_connectivity",
    "custom_asic_xpu_supply_chain",
    "neocloud_unit_economics_credit_risk",
    "ai_server_odm_liquid_cooled_rack",
    "ai_power_cooling_transformer",
    "ai_essd_nand_storage_controller",
    "silicon_photonics_inp_laser",
    "semiconductor_materials_gases_vacuum_cleanroom",
    "ai_datacenter_energy_grid",
    "ai_infra_bear_case_dashboard",
}
REQUIRED_THEME_DEEP_DIVES = {
    "hbm_structural_supercycle": "docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md",
    "cowos_advanced_packaging_bottleneck": "docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md",
}
REQUIRED_AI_INFRA_FILES = {
    "ai_infra/START_HERE.md",
    "ai_infra/README.md",
    "ai_infra/docs/llm-dependency-bfs-framework.md",
    "ai_infra/docs/research-checklist.md",
    "ai_infra/docs/source-evidence-template.md",
    "ai_infra/data/global_universe_v2.jsonl",
    "ai_infra/reports/source_verification_queue_v1.csv",
    "ai_infra/reports/us_alpha_mining_queue_v1.csv",
    "ai_infra/reports/core_candidates.csv",
    "ai_infra/scripts/build_universe_system.py",
}
REQUIRED_AI_INFRA_EXPANSION_FILES = {
    "ai_infra/scripts/generate_expansion_candidates.py",
    "scripts/ingest_ai_source_documents.py",
    "scripts/promote_ai_infra_expansion_candidates.py",
    "ai_infra/reports/expansion_candidates_v1.csv",
    "ai_infra/reports/source_document_tasks_v1.csv",
    "ai_infra/reports/source_evidence_extracts_v1.csv",
}
REQUIRED_AI_INFRA_METHOD_FILES = {
    "ai_infra/scripts/generate_bfs_supply_chain_discovery_queue.py",
    "ai_infra/docs/company-financials-market-options-methodology.md",
    "ai_infra/reports/bfs_supply_chain_discovery_queue_v1.csv",
    "ai_infra/reports/bfs_supply_chain_discovery_queue_v1.md",
    "ai_infra/reports/financials_market_options_pro_delta_v1.md",
    "ai_infra/data/company_name_zh_overrides.csv",
}
AI_INFRA_ALPHA_ELIGIBLE_EXPANSION_STATES = {
    "source_confirmed",
    "universe_promoted",
    "alpha_queue_eligible",
}
DISALLOWED_EXPANSION_SOURCE_TYPE_TERMS = {
    "news",
    "review_candidate",
    "factor_lab_hypothesis",
    "candidate_missing_source_type",
}
ORIGINAL_EXPANSION_SOURCE_TYPE_TERMS = {
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
REQUIRED_AI_INFRA_QUEUE_SYMBOLS = {"COHR", "FN", "MOD", "RMBS", "ALAB", "CRDO", "FORM", "LITE", "MTSI", "ONTO", "PSTG", "TER"}
REQUIRED_AI_INFRA_QUANT_DOCS = {
    "docs/AI_INFRA_QUANT_FUND_INTEGRATION.md",
}
REQUIRED_FACTOR_LAB_AI_INFRA_FILES = {
    "factor-lab/src/autoresearch/ai_infra_context.py",
}
REQUIRED_FACTOR_LAB_AI_INFRA_PROMPT_MARKERS = {
    "ai_infra/data/global_universe_v2.jsonl",
    "CDS/credit spreads",
    "options IV/skew/VRP/flow",
    "beta hedge return",
    "portfolio risk attribution",
}
REQUIRED_FACTOR_LAB_AI_INFRA_SCRIPT_MARKERS = {
    "FACTOR_LAB_AI_INFRA_ONLY",
    "FACTOR_LAB_AI_INFRA_ROOT",
}


@dataclass
class ReadinessCheck:
    check_id: str
    status: str
    summary: str
    evidence: dict[str, Any] = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _latest_candidate_dir(root: Path, as_of: str) -> Path | None:
    base = root / "reports" / "review_dashboard" / "ai_supply_chain_candidates"
    exact = base / as_of
    if exact.exists():
        return exact
    if not base.exists():
        return None
    dated = sorted(path for path in base.iterdir() if path.is_dir())
    return dated[-1] if dated else None


def check_taxonomy(root: Path) -> ReadinessCheck:
    path = root / "data" / "ai_supercycle_taxonomy.yaml"
    payload = _load_yaml(path)
    layers = {str(row.get("layer_id") or "") for row in payload.get("layers") or []}
    missing = sorted(REQUIRED_LAYERS - layers)
    return ReadinessCheck(
        check_id="taxonomy_layers",
        status="pass" if not missing else "fail",
        summary=f"{len(layers)} taxonomy layers loaded",
        evidence={"path": str(path), "layers": sorted(layers)},
        blockers=[f"missing layers: {', '.join(missing)}"] if missing else [],
    )


def check_us_theme_map(root: Path) -> ReadinessCheck:
    path = root / "data" / "us_theme_seed_map.yaml"
    payload = _load_yaml(path)
    themes = payload.get("themes") or []
    by_layer: dict[str, set[str]] = {}
    for theme in themes:
        layer = str(theme.get("supercycle_layer") or theme.get("theme_id") or "").strip()
        members = {str(item).upper() for item in theme.get("members") or []}
        by_layer.setdefault(layer, set()).update(members)
    missing_layers = sorted(REQUIRED_US_THEME_LAYERS - set(by_layer))
    missing_members: list[str] = []
    for layer, required in REQUIRED_THEME_MEMBERS.items():
        missing = sorted(required - by_layer.get(layer, set()))
        if missing:
            missing_members.append(f"{layer}: {', '.join(missing)}")
    blockers = []
    if missing_layers:
        blockers.append(f"missing theme layers: {', '.join(missing_layers)}")
    if missing_members:
        blockers.append("missing expected members: " + "; ".join(missing_members))
    return ReadinessCheck(
        check_id="us_theme_universe",
        status="pass" if not blockers else "fail",
        summary=f"{len(themes)} US AI-supercycle themes loaded",
        evidence={
            "path": str(path),
            "layers": sorted(by_layer),
            "member_counts": {layer: len(members) for layer, members in sorted(by_layer.items())},
        },
        blockers=blockers,
    )


def check_ai_infra_research_themes(root: Path) -> ReadinessCheck:
    path = root / "data" / "ai_infra_research_themes.yaml"
    payload = _load_yaml(path)
    themes = payload.get("themes") or []
    by_id = {str(theme.get("theme_id") or ""): theme for theme in themes}
    missing = sorted(REQUIRED_RESEARCH_THEMES - set(by_id))
    incomplete = []
    for theme_id, theme in by_id.items():
        required_fields = ["title", "research_question", "company_pool", "key_metrics", "disconfirming_evidence"]
        missing_fields = [field for field in required_fields if not theme.get(field)]
        if missing_fields:
            incomplete.append(f"{theme_id}: missing {', '.join(missing_fields)}")
        expected_deep_dive = REQUIRED_THEME_DEEP_DIVES.get(theme_id)
        deep_dive_doc = str(theme.get("deep_dive_doc") or "").strip()
        if expected_deep_dive and deep_dive_doc != expected_deep_dive:
            incomplete.append(f"{theme_id}: missing deep_dive_doc {expected_deep_dive}")
        if deep_dive_doc and not (root / deep_dive_doc).exists():
            incomplete.append(f"{theme_id}: deep_dive_doc not found: {deep_dive_doc}")
    blockers = []
    if missing:
        blockers.append(f"missing research themes: {', '.join(missing)}")
    if incomplete:
        blockers.extend(incomplete[:8])
    return ReadinessCheck(
        check_id="ai_infra_research_mainlines",
        status="pass" if not blockers else "fail",
        summary=f"{len(themes)} AI-infra research themes loaded",
        evidence={"path": str(path), "theme_ids": sorted(by_id)},
        blockers=blockers,
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _is_original_expansion_source_type(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    if any(term in lowered for term in DISALLOWED_EXPANSION_SOURCE_TYPE_TERMS):
        return False
    return any(term in lowered for term in ORIGINAL_EXPANSION_SOURCE_TYPE_TERMS)


def check_ai_infra_workbench(root: Path) -> ReadinessCheck:
    missing_files = sorted(path for path in REQUIRED_AI_INFRA_FILES if not (root / path).exists())
    universe_path = root / "ai_infra" / "data" / "global_universe_v2.jsonl"
    universe_rows = 0
    if universe_path.exists():
        universe_rows = sum(1 for line in universe_path.read_text(encoding="utf-8").splitlines() if line.strip())

    source_queue = _read_csv_rows(root / "ai_infra" / "reports" / "source_verification_queue_v1.csv")
    alpha_queue = _read_csv_rows(root / "ai_infra" / "reports" / "us_alpha_mining_queue_v1.csv")
    core_rows = _read_csv_rows(root / "ai_infra" / "reports" / "core_candidates.csv")
    alpha_symbols = {str(row.get("ticker") or "").upper() for row in alpha_queue}
    missing_alpha_symbols = sorted(REQUIRED_AI_INFRA_QUEUE_SYMBOLS - alpha_symbols)

    blockers = []
    if missing_files:
        blockers.append(f"missing ai_infra files: {', '.join(missing_files[:8])}")
    if universe_rows < 100:
        blockers.append(f"ai_infra universe too small: {universe_rows} rows")
    if len(source_queue) < 100:
        blockers.append(f"source verification queue too small: {len(source_queue)} rows")
    if len(alpha_queue) < 20:
        blockers.append(f"US alpha mining queue too small: {len(alpha_queue)} rows")
    if len(core_rows) < 50:
        blockers.append(f"core candidate queue too small: {len(core_rows)} rows")
    if missing_alpha_symbols:
        blockers.append(f"US alpha queue missing P0 symbols: {', '.join(missing_alpha_symbols)}")

    return ReadinessCheck(
        check_id="ai_infra_workbench",
        status="pass" if not blockers else "fail",
        summary=(
            f"{universe_rows} universe rows, {len(source_queue)} source-review rows, "
            f"{len(alpha_queue)} US alpha rows, {len(core_rows)} core candidates"
        ),
        evidence={
            "root": str(root / "ai_infra"),
            "required_files": sorted(REQUIRED_AI_INFRA_FILES),
            "us_alpha_symbols": sorted(alpha_symbols),
        },
        blockers=blockers,
    )


def check_ai_infra_expansion_lane(root: Path) -> ReadinessCheck:
    missing_files = sorted(path for path in REQUIRED_AI_INFRA_EXPANSION_FILES if not (root / path).exists())
    expansion_path = root / "ai_infra" / "reports" / "expansion_candidates_v1.csv"
    evidence_path = root / "ai_infra" / "reports" / "source_evidence_extracts_v1.csv"
    alpha_path = root / "ai_infra" / "reports" / "us_alpha_mining_queue_v1.csv"
    expansion_rows = _read_csv_rows(expansion_path)
    evidence_rows = _read_csv_rows(evidence_path)
    alpha_symbols = {str(row.get("ticker") or "").strip().upper() for row in _read_csv_rows(alpha_path)}
    evidence_keys = {
        (str(row.get("symbol") or "").strip().upper(), str(row.get("source_url") or "").strip())
        for row in evidence_rows
        if str(row.get("confidence") or "").strip().lower() in {"high", "medium"}
        and str(row.get("ai_module") or "").strip()
        and str(row.get("financial_translation") or "").strip()
        and _is_original_expansion_source_type(str(row.get("source_type") or ""))
    }

    blockers = []
    if missing_files:
        blockers.append(f"missing AI-infra expansion lane files: {', '.join(missing_files)}")

    invalid_confirmed: list[str] = []
    unconfirmed_in_alpha: list[str] = []
    states: dict[str, int] = {}
    for row in expansion_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        state = str(row.get("evidence_state") or "").strip().lower()
        confidence = str(row.get("confidence") or "").strip().lower()
        source_url = str(row.get("source_url") or "").strip()
        source_type = str(row.get("source_type") or "").strip()
        states[state or "missing"] = states.get(state or "missing", 0) + 1
        if symbol in alpha_symbols and state not in AI_INFRA_ALPHA_ELIGIBLE_EXPANSION_STATES:
            unconfirmed_in_alpha.append(symbol)
        if state == "source_confirmed":
            row_errors = []
            if not source_url.startswith(("http://", "https://")):
                row_errors.append("source_url")
            if confidence not in {"high", "medium"}:
                row_errors.append("confidence")
            if not str(row.get("ai_module") or "").strip():
                row_errors.append("ai_module")
            if not _is_original_expansion_source_type(source_type):
                row_errors.append("source_type")
            if (symbol, source_url) not in evidence_keys:
                row_errors.append("financial_evidence")
            if row_errors:
                invalid_confirmed.append(f"{symbol or 'missing_symbol'}: {', '.join(row_errors)}")

    if unconfirmed_in_alpha:
        blockers.append(
            "unconfirmed expansion candidates present in US alpha queue: "
            + ", ".join(sorted(set(unconfirmed_in_alpha))[:20])
        )
    if invalid_confirmed:
        blockers.append("invalid source_confirmed expansion rows: " + "; ".join(invalid_confirmed[:8]))

    return ReadinessCheck(
        check_id="ai_infra_expansion_lane",
        status="pass" if not blockers else "fail",
        summary=(
            f"{len(expansion_rows)} expansion candidates, {len(evidence_rows)} evidence extracts, "
            f"{len(evidence_keys)} promotable evidence keys"
        ),
        evidence={
            "required_files": sorted(REQUIRED_AI_INFRA_EXPANSION_FILES),
            "expansion_path": str(expansion_path),
            "evidence_path": str(evidence_path),
            "states": states,
        },
        blockers=blockers,
    )


def check_ai_infra_method_stack(root: Path) -> ReadinessCheck:
    missing_files = sorted(path for path in REQUIRED_AI_INFRA_METHOD_FILES if not (root / path).exists())
    queue_path = root / "ai_infra" / "reports" / "bfs_supply_chain_discovery_queue_v1.csv"
    queue_rows = _read_csv_rows(queue_path)
    queue_fields = set(queue_rows[0]) if queue_rows else set()
    missing_fields = sorted({"seed_ticker", "seed_company", "seed_company_zh", "theme", "source_targets"} - queue_fields)
    missing_zh = [
        str(row.get("seed_ticker") or "")
        for row in queue_rows
        if str(row.get("region_bucket") or "") in {"US", "china_hk"}
        and not str(row.get("seed_company_zh") or "").strip()
    ]
    blockers = []
    if missing_files:
        blockers.append(f"missing AI-infra method files: {', '.join(missing_files)}")
    if len(queue_rows) < 100:
        blockers.append(f"BFS discovery queue too small: {len(queue_rows)} rows")
    if missing_fields:
        blockers.append(f"BFS discovery queue missing fields: {', '.join(missing_fields)}")
    if missing_zh:
        blockers.append(f"BFS discovery queue missing Chinese names: {', '.join(missing_zh[:20])}")
    return ReadinessCheck(
        check_id="ai_infra_bfs_discovery_method_stack",
        status="pass" if not blockers else "fail",
        summary=f"{len(queue_rows)} BFS discovery tasks with Chinese-name field",
        evidence={
            "required_files": sorted(REQUIRED_AI_INFRA_METHOD_FILES),
            "queue_path": str(queue_path),
            "fields": sorted(queue_fields),
        },
        blockers=blockers,
    )


def check_ai_infra_quant_contract(root: Path) -> ReadinessCheck:
    missing_docs = sorted(path for path in REQUIRED_AI_INFRA_QUANT_DOCS if not (root / path).exists())
    missing_files = sorted(path for path in REQUIRED_FACTOR_LAB_AI_INFRA_FILES if not (root / path).exists())
    prompt_path = root / "factor-lab" / "src" / "agent" / "prompts.py"
    prompt_text = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""
    missing_prompt_markers = sorted(
        marker for marker in REQUIRED_FACTOR_LAB_AI_INFRA_PROMPT_MARKERS if marker not in prompt_text
    )
    script_paths = [
        root / "factor-lab" / "scripts" / "autoresearch.sh",
        root / "factor-lab" / "scripts" / "daily_factors.sh",
    ]
    missing_script_markers: list[str] = []
    missing_scripts: list[str] = []
    for script_path in script_paths:
        script_text = script_path.read_text(encoding="utf-8") if script_path.exists() else ""
        if not script_path.exists():
            missing_scripts.append(str(script_path))
            continue
        missing = sorted(marker for marker in REQUIRED_FACTOR_LAB_AI_INFRA_SCRIPT_MARKERS if marker not in script_text)
        if missing:
            missing_script_markers.append(f"{script_path.name}: {', '.join(missing)}")
    blockers = []
    if missing_docs:
        blockers.append(f"missing AI-infra quant docs: {', '.join(missing_docs)}")
    if missing_files:
        blockers.append(f"missing Factor Lab AI-infra bridge files: {', '.join(missing_files)}")
    if not prompt_path.exists():
        blockers.append(f"missing Factor Lab prompt: {prompt_path}")
    elif missing_prompt_markers:
        blockers.append(f"Factor Lab prompt missing AI-infra quant markers: {', '.join(missing_prompt_markers)}")
    if missing_scripts:
        blockers.append(f"missing Factor Lab scripts: {', '.join(missing_scripts)}")
    if missing_script_markers:
        blockers.append(f"Factor Lab scripts missing AI-infra markers: {', '.join(missing_script_markers)}")
    return ReadinessCheck(
        check_id="ai_infra_quant_fund_contract",
        status="pass" if not blockers else "fail",
        summary="AI-infra quant-fund contract, Factor Lab bridge, prompt and autoresearch defaults checked",
        evidence={
            "docs": sorted(REQUIRED_AI_INFRA_QUANT_DOCS),
            "bridge_files": sorted(REQUIRED_FACTOR_LAB_AI_INFRA_FILES),
            "factor_lab_prompt": str(prompt_path),
            "required_prompt_markers": sorted(REQUIRED_FACTOR_LAB_AI_INFRA_PROMPT_MARKERS),
            "factor_lab_scripts": [str(path) for path in script_paths],
            "required_script_markers": sorted(REQUIRED_FACTOR_LAB_AI_INFRA_SCRIPT_MARKERS),
        },
        blockers=blockers,
    )


def check_relationship_ledger(root: Path) -> ReadinessCheck:
    path = root / "data" / "ai_supply_chain_relationships.yaml"
    payload = _load_yaml(path)
    rows = payload.get("relationships") or []
    invalid: list[str] = []
    symbols: set[str] = set()
    layers: set[str] = set()
    for idx, row in enumerate(rows, start=1):
        source_url = str(row.get("source_url") or "")
        source_type = str(row.get("source_type") or "")
        confidence = str(row.get("confidence") or "").lower()
        if not source_url.startswith(("http://", "https://")):
            invalid.append(f"row {idx}: missing http source_url")
        if not source_type:
            invalid.append(f"row {idx}: missing source_type")
        if confidence not in {"high", "medium"}:
            invalid.append(f"row {idx}: confidence not high/medium")
        for field in ["primary_symbol", "counterparty_symbol", "customer_symbol"]:
            if row.get(field):
                symbols.add(str(row[field]).upper())
        for symbol in row.get("symbols") or []:
            symbols.add(str(symbol).upper())
        if row.get("layer"):
            layers.add(str(row["layer"]))
    missing_symbols = sorted(REQUIRED_RELATIONSHIP_SYMBOLS - symbols)
    blockers = invalid[:8]
    if missing_symbols:
        blockers.append(f"missing source-confirmed symbols: {', '.join(missing_symbols)}")
    status = "pass" if rows and not blockers else "fail"
    return ReadinessCheck(
        check_id="source_confirmed_relationship_ledger",
        status=status,
        summary=f"{len(rows)} source-confirmed relationship rows",
        evidence={"path": str(path), "symbols": sorted(symbols), "layers": sorted(layers)},
        blockers=blockers,
    )


def check_relationship_candidate_queue(root: Path, as_of: str) -> ReadinessCheck:
    directory = _latest_candidate_dir(root, as_of)
    if directory is None:
        return ReadinessCheck(
            check_id="relationship_candidate_queue",
            status="warn",
            summary="no local relationship candidate queue found",
            evidence={},
            blockers=["run research.ai_supply_chain_candidates"],
        )
    csv_path = directory / "ai_supply_chain_relationship_candidates.csv"
    if not csv_path.exists():
        return ReadinessCheck(
            check_id="relationship_candidate_queue",
            status="warn",
            summary=f"candidate directory exists but CSV is missing: {directory.name}",
            evidence={"directory": str(directory)},
            blockers=["candidate extractor did not write CSV"],
        )
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    review_states: dict[str, int] = {}
    source_types: dict[str, int] = {}
    for row in rows:
        review_states[str(row.get("review_state") or "missing")] = review_states.get(str(row.get("review_state") or "missing"), 0) + 1
        source_types[str(row.get("source_type") or "missing")] = source_types.get(str(row.get("source_type") or "missing"), 0) + 1
    status = "pass" if rows else "warn"
    return ReadinessCheck(
        check_id="relationship_candidate_queue",
        status=status,
        summary=f"{len(rows)} unreviewed relationship candidates in {directory.name}",
        evidence={"csv": str(csv_path), "review_states": review_states, "source_types": source_types},
        blockers=[] if rows else ["candidate queue is empty"],
    )


def check_ai_lab_publications(root: Path) -> ReadinessCheck:
    seed = root / "data" / "ai_lab_quality_seed.yaml"
    publication_path = root / "data" / "ai_lab_publications.csv"
    seed_payload = _load_yaml(seed)
    if not seed_payload:
        return ReadinessCheck(
            check_id="ai_lab_publication_index",
            status="warn",
            summary="AI lab seed missing",
            evidence={"seed": str(seed)},
            blockers=["optional lab-quality seed missing"],
        )
    if not publication_path.exists():
        return ReadinessCheck(
            check_id="ai_lab_publication_index",
            status="warn",
            summary="optional top-conference publication dataset is not loaded",
            evidence={
                "seed": str(seed),
                "publication_path": str(publication_path),
                "conference_scope": seed_payload.get("conference_scope") or [],
            },
            blockers=[
                "optional: load data/ai_lab_publications.csv via scripts/build_ai_lab_publications.py",
                "lab quality remains auxiliary until publication data is loaded",
            ],
        )
    with publication_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    conferences = {str(row.get("conference") or "") for row in rows}
    symbols = {str(row.get("symbol") or "").upper() for row in rows if row.get("symbol")}
    missing_confs = sorted(TOP_CONFERENCES - conferences)
    blockers = [f"missing conference counts: {', '.join(missing_confs)}"] if missing_confs else []
    return ReadinessCheck(
        check_id="ai_lab_publication_index",
        status="pass" if rows and not blockers else "warn",
        summary=f"{len(rows)} normalized AI lab publication rows loaded",
        evidence={"publication_path": str(publication_path), "conferences": sorted(conferences), "symbols": sorted(symbols)},
        blockers=blockers,
    )


def check_current_report(root: Path, as_of: str) -> ReadinessCheck:
    path = root / "reports" / "review_dashboard" / "main_strategy_v2" / as_of / "main_strategy_v2_backtest.json"
    if not path.exists():
        return ReadinessCheck(
            check_id="daily_report_ai_artifacts",
            status="warn",
            summary=f"main strategy payload missing for {as_of}",
            evidence={"path": str(path)},
            blockers=["run scripts/run_main_strategy_v2_backtest.py for this date"],
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    ai_evidence = (
        payload.get("ai_supercycle_evidence_ledger")
        or payload.get("ai_supercycle_evidence")
        or {}
    ).get("rows") or []
    value_radar = (payload.get("ai_supercycle_value_radar") or {}).get("rows") or []
    layer_rows = (payload.get("ai_supercycle_layer_attribution") or {}).get("rows") or []
    cn_layers = {str(row.get("layer")) for row in layer_rows if str(row.get("market") or "").upper() == "CN"}
    us_layers = {str(row.get("layer")) for row in layer_rows if str(row.get("market") or "").upper() == "US"}
    missing = []
    if not ai_evidence:
        missing.append("ai_supercycle_evidence rows missing")
    if not value_radar:
        missing.append("ai_supercycle_value_radar rows missing")
    if not layer_rows:
        missing.append("ai_supercycle_layer_attribution rows missing")
    if "ai_networking_optical_cpo" not in cn_layers:
        missing.append("CN optical/CPO attribution missing")
    if "ai_chip_equipment_materials_packaging" not in cn_layers:
        missing.append("CN chip/materials attribution missing")
    return ReadinessCheck(
        check_id="daily_report_ai_artifacts",
        status="pass" if not missing else "fail",
        summary=f"report payload has {len(ai_evidence)} evidence rows, {len(value_radar)} value-radar rows, {len(layer_rows)} layer rows",
        evidence={"path": str(path), "cn_layers": sorted(cn_layers), "us_layers": sorted(us_layers)},
        blockers=missing,
    )


def build_readiness(root: Path, as_of: str) -> dict[str, Any]:
    checks = [
        check_taxonomy(root),
        check_us_theme_map(root),
        check_ai_infra_research_themes(root),
        check_ai_infra_workbench(root),
        check_ai_infra_method_stack(root),
        check_ai_infra_expansion_lane(root),
        check_ai_infra_quant_contract(root),
        check_relationship_ledger(root),
        check_relationship_candidate_queue(root, as_of),
        check_ai_lab_publications(root),
        check_current_report(root, as_of),
    ]
    failures = [check for check in checks if check.status == "fail"]
    warnings = [check for check in checks if check.status == "warn"]
    return {
        "as_of": as_of,
        "ready": not failures,
        "status": "ready" if not failures and not warnings else ("ready_with_warnings" if not failures else "blocked"),
        "summary": {
            "checks": len(checks),
            "pass": sum(1 for check in checks if check.status == "pass"),
            "warn": len(warnings),
            "fail": len(failures),
            "hard_blockers": [blocker for check in failures for blocker in check.blockers],
            "warnings": [blocker for check in warnings for blocker in check.blockers],
        },
        "checks": [asdict(check) for check in checks],
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# AI Supercycle Readiness - {payload.get('as_of')}",
        "",
        f"- status: {payload.get('status')}",
        f"- checks: {payload.get('summary', {}).get('checks', 0)}",
        f"- pass/warn/fail: {payload.get('summary', {}).get('pass', 0)}/"
        f"{payload.get('summary', {}).get('warn', 0)}/{payload.get('summary', {}).get('fail', 0)}",
        "",
        "| Check | Status | Summary | Blockers |",
        "|---|---|---|---|",
    ]
    for check in payload.get("checks") or []:
        blockers = "; ".join(str(item) for item in check.get("blockers") or []) or "-"
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | "
            f"{str(check.get('summary') or '').replace('|', '/')} | {blockers.replace('|', '/')} |"
        )
    lines.append("")
    if payload.get("summary", {}).get("hard_blockers"):
        lines += ["## Hard Blockers", ""]
        for blocker in payload["summary"]["hard_blockers"]:
            lines.append(f"- {blocker}")
        lines.append("")
    if payload.get("summary", {}).get("warnings"):
        lines += ["## Warnings", ""]
        for warning in payload["summary"]["warnings"]:
            lines.append(f"- {warning}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(root: Path, payload: dict[str, Any]) -> Path:
    output_dir = root / "reports" / "review_dashboard" / "ai_supercycle_readiness" / str(payload["as_of"])
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "ai_supercycle_readiness.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "ai_supercycle_readiness.md").write_text(render_markdown(payload), encoding="utf-8")
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify AI-supercycle readiness artifacts.")
    parser.add_argument("--root", type=Path, default=STACK_ROOT)
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--strict", action="store_true", help="Return non-zero if any hard blocker remains.")
    args = parser.parse_args()

    root = args.root.resolve()
    payload = build_readiness(root, args.as_of)
    output_dir = write_outputs(root, payload)
    print(
        f"AI supercycle readiness written: {output_dir} "
        f"status={payload['status']} pass/warn/fail="
        f"{payload['summary']['pass']}/{payload['summary']['warn']}/{payload['summary']['fail']}"
    )
    if args.strict and payload["summary"]["fail"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
