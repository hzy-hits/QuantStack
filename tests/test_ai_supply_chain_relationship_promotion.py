from __future__ import annotations

import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "promote_ai_supply_chain_relationship_candidates.py"
BUILD_SCRIPT_PATH = REPO_ROOT / "scripts" / "build_ai_supply_chain_relationships.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


promoter = _load_module(SCRIPT_PATH, "promote_ai_supply_chain_relationship_candidates")
builder = _load_module(BUILD_SCRIPT_PATH, "build_ai_supply_chain_relationships")


class AiSupplyChainRelationshipPromotionTests(unittest.TestCase):
    def test_promotes_only_source_confirmed_builder_ready_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            reviewed = root / "reviewed.csv"
            promoted_csv = root / "source_confirmed.csv"
            reviewed.write_text(
                "relationship_id,as_of,market,primary_symbol,counterparty_symbol,customer_symbol,symbols,layer,relationship_type,supply_chain_role,bottleneck_focus,source_name,source_type,source_url,source_date,confidence,notes,review_state\n"
                "rel_good,2026-05-12,US,GLW,NVDA,,GLW;NVDA,ai_networking_optical_cpo,official_partner,Corning expands optical fiber with NVIDIA,AI optical fiber,Corning press release,official_press_release,https://example.com/glw,2026-05-11,high,verified source,source_confirmed\n"
                "rel_unreviewed,2026-05-12,US,NET,NVDA,,NET;NVDA,ai_datacenter_edge_infra,partnership_candidate,Cloudflare candidate,edge AI,News,news_review_candidate,https://example.com/net,2026-05-11,unreviewed,not checked,needs_human_source_review\n"
                "rel_bad_type,2026-05-12,US,DELL,NVDA,,DELL;NVDA,ai_compute_accelerators,partnership_candidate,Dell candidate,AI server,News,news_review_candidate,https://example.com/dell,2026-05-11,high,still candidate,source_confirmed\n",
                encoding="utf-8",
            )

            rows, rejected = promoter.promote_rows(reviewed)
            promoter.write_csv(rows, promoted_csv)

            self.assertEqual([row["relationship_id"] for row in rows], ["rel_good"])
            errors = {item["row"]["relationship_id"]: item["errors"] for item in rejected}
            self.assertIn("not_source_confirmed", errors["rel_unreviewed"])
            self.assertIn("source_type_still_review_candidate", errors["rel_bad_type"])

            builder_rows, builder_rejected = builder.load_relationship_rows(promoted_csv)
            self.assertEqual([row["relationship_id"] for row in builder_rows], ["rel_good"])
            self.assertEqual(builder_rejected, [])

            with promoted_csv.open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            self.assertEqual(csv_rows[0]["review_state"], "source_confirmed")
            self.assertEqual(csv_rows[0]["symbols"], "GLW;NVDA")


if __name__ == "__main__":
    unittest.main()
