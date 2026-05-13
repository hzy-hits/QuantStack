from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "verify_ai_supercycle_readiness.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("verify_ai_supercycle_readiness", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


readiness = _load_module()


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_ai_infra_workbench(root: Path) -> None:
    for relative_path in readiness.REQUIRED_AI_INFRA_FILES:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".csv" or path.suffix == ".jsonl":
            continue
        path.write_text("unit ai infra artifact\n", encoding="utf-8")
    universe_rows = [
        {
            "asset_pool": "美国资产池",
            "market_country": "US",
            "ticker": f"UNIT{i:03d}",
            "company": f"Unit {i}",
            "bfs_depth": "D2-D3",
            "module": "unit module",
            "current_pool": "候选池",
        }
        for i in range(120)
    ]
    (root / "ai_infra" / "data" / "global_universe_v2.jsonl").write_text(
        "\n".join(yaml.safe_dump(row, allow_unicode=True).replace("\n", " ") for row in universe_rows),
        encoding="utf-8",
    )
    _write_csv(
        root / "ai_infra" / "reports" / "source_verification_queue_v1.csv",
        ["ticker", "company"],
        [{"ticker": f"UNIT{i:03d}", "company": f"Unit {i}"} for i in range(120)],
    )
    required_symbols = sorted(readiness.REQUIRED_AI_INFRA_QUEUE_SYMBOLS)
    alpha_rows = [{"ticker": symbol, "company": symbol} for symbol in required_symbols]
    alpha_rows += [{"ticker": f"ALPHA{i:03d}", "company": f"Alpha {i}"} for i in range(20)]
    _write_csv(root / "ai_infra" / "reports" / "us_alpha_mining_queue_v1.csv", ["ticker", "company"], alpha_rows)
    _write_csv(
        root / "ai_infra" / "reports" / "core_candidates.csv",
        ["ticker", "company"],
        [{"ticker": f"CORE{i:03d}", "company": f"Core {i}"} for i in range(70)],
    )
    for relative_path in readiness.REQUIRED_AI_INFRA_EXPANSION_FILES:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".py":
            path.write_text("# unit expansion lane script\n", encoding="utf-8")
    _write_csv(
        root / "ai_infra" / "reports" / "expansion_candidates_v1.csv",
        [
            "symbol",
            "company_name",
            "market",
            "ai_module",
            "source_url",
            "source_type",
            "evidence_state",
            "confidence",
        ],
        [],
    )
    _write_csv(
        root / "ai_infra" / "reports" / "source_document_tasks_v1.csv",
        ["symbol", "source_url", "source_type", "status"],
        [],
    )
    _write_csv(
        root / "ai_infra" / "reports" / "source_evidence_extracts_v1.csv",
        ["symbol", "source_url", "source_type", "confidence", "ai_module", "financial_translation"],
        [],
    )
    for relative_path in readiness.REQUIRED_AI_INFRA_METHOD_FILES:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix in {".py", ".md"}:
            path.write_text("unit ai infra method artifact\n", encoding="utf-8")
    _write_csv(
        root / "ai_infra" / "reports" / "bfs_supply_chain_discovery_queue_v1.csv",
        ["seed_ticker", "seed_company", "seed_company_zh", "theme", "source_targets", "region_bucket"],
        [
            {
                "seed_ticker": f"BFS{i:03d}",
                "seed_company": f"BFS {i}",
                "seed_company_zh": f"BFS中文{i}",
                "theme": "compute_gpu_asic",
                "source_targets": "10-K",
                "region_bucket": "US",
            }
            for i in range(120)
        ],
    )
    _write_csv(
        root / "ai_infra" / "data" / "company_name_zh_overrides.csv",
        ["symbol", "company_zh", "name_source"],
        [{"symbol": "NVDA", "company_zh": "英伟达", "name_source": "unit"}],
    )


def _make_minimal_root(root: Path, *, with_publications: bool) -> None:
    layers = [{"layer_id": layer} for layer in sorted(readiness.REQUIRED_LAYERS)]
    _write_yaml(root / "data" / "ai_supercycle_taxonomy.yaml", {"layers": layers})
    _write_yaml(
        root / "data" / "us_theme_seed_map.yaml",
        {
            "themes": [
                {
                    "theme_id": layer,
                    "supercycle_layer": layer,
                    "members": sorted(members),
                }
                for layer, members in readiness.REQUIRED_THEME_MEMBERS.items()
            ]
        },
    )
    deep_dive_path = root / "docs" / "AI_INFRA_HBM_COWOS_DEEP_DIVE.md"
    deep_dive_path.parent.mkdir(parents=True, exist_ok=True)
    deep_dive_path.write_text("# Unit HBM CoWoS deep dive\n", encoding="utf-8")
    (root / "docs" / "AI_INFRA_QUANT_FUND_INTEGRATION.md").write_text(
        "# Unit AI Infra Quant Fund Integration\n",
        encoding="utf-8",
    )
    prompt_path = root / "factor-lab" / "src" / "agent" / "prompts.py"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(
        "\n".join(
            [
                "ai_infra/data/global_universe_v2.jsonl",
                "CDS/credit spreads",
                "options IV/skew/VRP/flow",
                "beta hedge return",
                "portfolio risk attribution",
            ]
        ),
        encoding="utf-8",
    )
    bridge_path = root / "factor-lab" / "src" / "autoresearch" / "ai_infra_context.py"
    bridge_path.parent.mkdir(parents=True, exist_ok=True)
    bridge_path.write_text("unit ai infra bridge\n", encoding="utf-8")
    autoresearch_script = root / "factor-lab" / "scripts" / "autoresearch.sh"
    autoresearch_script.parent.mkdir(parents=True, exist_ok=True)
    autoresearch_script.write_text(
        "FACTOR_LAB_AI_INFRA_ONLY\nFACTOR_LAB_AI_INFRA_ROOT\n",
        encoding="utf-8",
    )
    daily_factors_script = root / "factor-lab" / "scripts" / "daily_factors.sh"
    daily_factors_script.write_text(
        "FACTOR_LAB_AI_INFRA_ONLY\nFACTOR_LAB_AI_INFRA_ROOT\n",
        encoding="utf-8",
    )
    research_themes = []
    for theme_id in sorted(readiness.REQUIRED_RESEARCH_THEMES):
        theme = {
            "theme_id": theme_id,
            "title": theme_id,
            "research_question": "unit research question",
            "company_pool": ["UNIT"],
            "key_metrics": ["metric"],
            "disconfirming_evidence": ["risk"],
        }
        expected_deep_dive = readiness.REQUIRED_THEME_DEEP_DIVES.get(theme_id)
        if expected_deep_dive:
            theme["deep_dive_doc"] = expected_deep_dive
        research_themes.append(theme)
    _write_yaml(
        root / "data" / "ai_infra_research_themes.yaml",
        {"themes": research_themes},
    )
    _make_ai_infra_workbench(root)
    _write_yaml(
        root / "data" / "ai_supply_chain_relationships.yaml",
        {
            "relationships": [
                {
                    "relationship_id": f"rel_{symbol.lower()}",
                    "market": "US",
                    "primary_symbol": symbol,
                    "symbols": [symbol],
                    "layer": "ai_chip_equipment_materials_packaging",
                    "source_type": "official_press_release",
                    "source_url": f"https://example.com/{symbol.lower()}",
                    "confidence": "high",
                }
                for symbol in sorted(readiness.REQUIRED_RELATIONSHIP_SYMBOLS)
            ]
        },
    )
    _write_yaml(
        root / "data" / "ai_lab_quality_seed.yaml",
        {
            "conference_scope": sorted(readiness.TOP_CONFERENCES),
            "companies": [{"symbol": "GOOGL", "company": "Alphabet"}],
        },
    )
    candidate_dir = root / "reports" / "review_dashboard" / "ai_supply_chain_candidates" / "2026-05-12"
    candidate_dir.mkdir(parents=True)
    with (candidate_dir / "ai_supply_chain_relationship_candidates.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["relationship_id", "review_state", "source_type"])
        writer.writeheader()
        writer.writerow(
            {
                "relationship_id": "candidate_1",
                "review_state": "needs_human_source_review",
                "source_type": "news_review_candidate",
            }
        )
    report_dir = root / "reports" / "review_dashboard" / "main_strategy_v2" / "2026-05-12"
    report_dir.mkdir(parents=True)
    (report_dir / "main_strategy_v2_backtest.json").write_text(
        """
{
  "ai_supercycle_evidence_ledger": {"rows": [{"symbol": "AXTI"}]},
  "ai_supercycle_value_radar": {"rows": [{"symbol": "AXTI"}]},
  "ai_supercycle_layer_attribution": {
    "rows": [
      {"market": "CN", "layer": "ai_networking_optical_cpo"},
      {"market": "CN", "layer": "ai_chip_equipment_materials_packaging"},
      {"market": "US", "layer": "ai_labs_cloud_models"}
    ]
  }
}
""".strip(),
        encoding="utf-8",
    )
    if with_publications:
        with (root / "data" / "ai_lab_publications.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["symbol", "conference", "year", "accepted_count", "oral_spotlight_count", "source"],
            )
            writer.writeheader()
            for conference in sorted(readiness.TOP_CONFERENCES):
                writer.writerow(
                    {
                        "symbol": "GOOGL",
                        "conference": conference,
                        "year": "2025",
                        "accepted_count": "1",
                        "oral_spotlight_count": "0",
                        "source": "unit",
                    }
                )


class AiSupercycleReadinessTests(unittest.TestCase):
    def test_missing_lab_publications_is_only_an_auxiliary_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _make_minimal_root(root, with_publications=False)

            payload = readiness.build_readiness(root, "2026-05-12")

            self.assertTrue(payload["ready"])
            self.assertEqual(payload["summary"]["fail"], 0)
            self.assertEqual(payload["summary"]["warn"], 1)
            warnings = payload["summary"]["warnings"]
            self.assertTrue(any("ai_lab_publications.csv" in warning for warning in warnings))

    def test_full_minimal_artifacts_are_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _make_minimal_root(root, with_publications=True)

            payload = readiness.build_readiness(root, "2026-05-12")

            self.assertTrue(payload["ready"])
            self.assertEqual(payload["summary"]["fail"], 0)
            output_dir = readiness.write_outputs(root, payload)
            self.assertTrue((output_dir / "ai_supercycle_readiness.json").exists())
            self.assertTrue((output_dir / "ai_supercycle_readiness.md").exists())

    def test_unconfirmed_expansion_candidate_in_alpha_queue_blocks_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _make_minimal_root(root, with_publications=True)
            with (root / "ai_infra" / "reports" / "us_alpha_mining_queue_v1.csv").open(
                "a", encoding="utf-8", newline=""
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=["ticker", "company"])
                writer.writerow({"ticker": "DGXX", "company": "DGXX"})
            _write_csv(
                root / "ai_infra" / "reports" / "expansion_candidates_v1.csv",
                [
                    "symbol",
                    "company_name",
                    "market",
                    "ai_module",
                    "source_url",
                    "source_type",
                    "evidence_state",
                    "confidence",
                ],
                [
                    {
                        "symbol": "DGXX",
                        "company_name": "DGXX",
                        "market": "US",
                        "ai_module": "ai_datacenter_edge_infra",
                        "source_url": "https://example.com/dgxx",
                        "source_type": "news_review_candidate",
                        "evidence_state": "pending_original_source_verification",
                        "confidence": "unreviewed",
                    }
                ],
            )

            payload = readiness.build_readiness(root, "2026-05-12")

            self.assertFalse(payload["ready"])
            blockers = payload["summary"]["hard_blockers"]
            self.assertTrue(any("unconfirmed expansion candidates" in blocker for blocker in blockers))


if __name__ == "__main__":
    unittest.main()
