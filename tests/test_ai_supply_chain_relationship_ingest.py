from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_ai_supply_chain_relationships.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_ai_supply_chain_relationships", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ingest = _load_module()


class AiSupplyChainRelationshipIngestTests(unittest.TestCase):
    def test_builds_only_source_linked_relationships(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "relationships.csv"
            out = root / "relationships.yaml"
            raw.write_text(
                "relationship_id,as_of,market,primary_symbol,counterparty_symbol,symbols,layer,relationship_type,supply_chain_role,source_type,source_url,source_date,confidence\n"
                "rel_good,2026-01-01,US,DELL,NVDA,DELL;NVDA,ai_compute_accelerators,official_partner,Dell integrates NVIDIA systems,official_press_release,https://example.com/dell,2026-01-01,high\n"
                "rel_missing_source,2026-01-01,US,AAOI,HYPER,AAOI;HYPER,ai_networking_optical_cpo,rumor,AAOI may supply modules,news,,2026-01-01,high\n"
                "rel_low_conf,2026-01-01,US,POET,HYPER,POET;HYPER,ai_networking_optical_cpo,rumor,POET may supply modules,news,https://example.com/poet,2026-01-01,low\n",
                encoding="utf-8",
            )
            rows, rejected = ingest.load_relationship_rows(raw)
            ingest.write_relationships(rows, out)

            payload = yaml.safe_load(out.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["relationships"]), 1)
            self.assertEqual(payload["relationships"][0]["relationship_id"], "rel_good")
            self.assertEqual(payload["relationships"][0]["symbols"], ["DELL", "NVDA"])
            self.assertEqual(len(rejected), 2)

    def test_candidate_rows_must_be_source_confirmed_before_ingest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "relationships.csv"
            raw.write_text(
                "relationship_id,as_of,market,primary_symbol,counterparty_symbol,symbols,layer,relationship_type,supply_chain_role,source_type,source_url,source_date,confidence,review_state\n"
                "rel_candidate,2026-01-01,US,NET,NVDA,NET;NVDA,ai_datacenter_edge_infra,partnership_candidate,Cloudflare edge AI candidate,news_review_candidate,https://example.com/net,2026-01-01,high,source_confirmed\n"
                "rel_unreviewed,2026-01-01,US,GLW,NVDA,GLW;NVDA,ai_networking_optical_cpo,partnership_candidate,Corning optical candidate,news,https://example.com/glw,2026-01-01,high,needs_human_source_review\n"
                "rel_confirmed,2026-01-01,US,DELL,NVDA,DELL;NVDA,ai_compute_accelerators,official_partner,Dell integrates NVIDIA systems,official_press_release,https://example.com/dell,2026-01-01,high,source_confirmed\n",
                encoding="utf-8",
            )

            rows, rejected = ingest.load_relationship_rows(raw)

            self.assertEqual([row["relationship_id"] for row in rows], ["rel_confirmed"])
            errors = {item["row"]["relationship_id"]: item["errors"] for item in rejected}
            self.assertIn("source_type_still_review_candidate", errors["rel_candidate"])
            self.assertIn("review_state_not_source_confirmed", errors["rel_unreviewed"])


if __name__ == "__main__":
    unittest.main()
