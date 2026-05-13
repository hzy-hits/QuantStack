from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PATH = REPO_ROOT / "ai_infra" / "scripts" / "generate_expansion_candidates.py"
INGEST_PATH = REPO_ROOT / "scripts" / "ingest_ai_source_documents.py"
PROMOTE_PATH = REPO_ROOT / "scripts" / "promote_ai_infra_expansion_candidates.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = _load_module(GENERATOR_PATH, "generate_expansion_candidates")
ingest = _load_module(INGEST_PATH, "ingest_ai_source_documents")
promoter = _load_module(PROMOTE_PATH, "promote_ai_infra_expansion_candidates")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class AiInfraExpansionLaneTests(unittest.TestCase):
    def test_manual_candidate_becomes_pending_source_review_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            universe = root / "global_universe_v2.jsonl"
            universe.write_text(
                json.dumps({"ticker": "NVDA", "company": "NVIDIA"}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            manual = root / "manual.csv"
            _write_csv(
                manual,
                [
                    "symbol",
                    "company_name",
                    "market",
                    "ai_module",
                    "candidate_reason",
                    "source_url",
                    "source_type",
                ],
                [
                    {
                        "symbol": "DGXX",
                        "company_name": "DGXX Inc.",
                        "market": "US",
                        "ai_module": "ai_datacenter_edge_infra",
                        "candidate_reason": "possible AI data center infrastructure supplier",
                        "source_url": "https://example.com/dgxx",
                        "source_type": "news_review_candidate",
                    },
                    {
                        "symbol": "NVDA",
                        "company_name": "NVIDIA",
                        "market": "US",
                        "ai_module": "ai_compute_accelerators",
                        "candidate_reason": "already in universe",
                        "source_url": "https://example.com/nvda",
                        "source_type": "official_press_release",
                    },
                ],
            )

            args = argparse.Namespace(
                as_of="2026-05-13",
                universe=universe,
                manual_csv=manual,
                discovery_json=root / "missing.json",
                relationship_root=root / "missing_relationships",
                reports_dir=root,
            )
            rows = generator.build_candidates(args)

            self.assertEqual([row["symbol"] for row in rows], ["DGXX"])
            self.assertEqual(rows[0]["evidence_state"], "pending_original_source_verification")
            self.assertEqual(rows[0]["review_state"], "needs_human_source_review")
            self.assertEqual(rows[0]["confidence"], "unreviewed")

    def test_ingest_source_extracts_structured_evidence_without_promoting(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "dgxx_source.txt"
            source.write_text(
                "DGXX announced that its AI data center platform generated data center revenue, "
                "new customer orders, backlog growth, and capacity expansion for GPU cloud customers.",
                encoding="utf-8",
            )
            candidates = root / "expansion_candidates_v1.csv"
            _write_csv(
                candidates,
                [
                    "symbol",
                    "company_name",
                    "source_url",
                    "source_type",
                    "source_date",
                    "ai_module",
                    "counterparty",
                ],
                [
                    {
                        "symbol": "DGXX",
                        "company_name": "DGXX Inc.",
                        "source_url": source.as_uri(),
                        "source_type": "official_press_release",
                        "source_date": "2026-05-12",
                        "ai_module": "ai_datacenter_edge_infra",
                        "counterparty": "GPU cloud customers",
                    }
                ],
            )

            tasks, evidence = ingest.ingest_rows(
                ingest.read_csv(candidates),
                cache_dir=root / "cache",
                as_of="2026-05-13",
                timeout=1.0,
                max_docs=10,
            )

            self.assertEqual(tasks[0]["status"], "fetched")
            self.assertTrue(evidence)
            self.assertEqual(evidence[0]["symbol"], "DGXX")
            self.assertIn(evidence[0]["confidence"], {"medium", "high"})
            self.assertIn("revenue_or_segment_mix", evidence[0]["financial_translation"])
            self.assertNotEqual(evidence[0].get("evidence_state"), "source_confirmed")

    def test_ingest_adds_local_sec_filing_source_tasks_for_candidate_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sec_db = root / "quant.duckdb"
            con = duckdb.connect(str(sec_db))
            con.execute(
                """
                CREATE TABLE sec_filings (
                    symbol VARCHAR,
                    form_type VARCHAR,
                    filed_date DATE,
                    description VARCHAR,
                    items VARCHAR,
                    filing_url VARCHAR
                )
                """
            )
            con.execute(
                """
                INSERT INTO sec_filings VALUES
                ('DGXX', '10-K', DATE '2026-05-10', 'Annual report',
                 'AI data center revenue and backlog disclosure',
                 'https://www.sec.gov/Archives/dgxx-10k.htm'),
                ('DGXX', 'S-1', DATE '2026-05-01', 'Registration statement',
                 'AI data center capacity expansion disclosure',
                 'https://www.sec.gov/Archives/dgxx-s1.htm'),
                ('OTHER', '10-K', DATE '2026-05-10', 'Other annual report',
                 'not a candidate',
                 'https://www.sec.gov/Archives/other-10k.htm')
                """
            )
            con.close()
            rows = [{"symbol": "DGXX", "company_name": "DGXX Inc.", "ai_module": "ai_datacenter_edge_infra"}]

            sec_rows = ingest.sec_source_rows(rows, sec_db, ["10-K", "S-1"], per_symbol=2)

            self.assertEqual([row["source_type"] for row in sec_rows], ["sec_filing_10-k", "sec_filing_s-1"])
            self.assertTrue(all(row["source_url"].startswith("https://www.sec.gov/") for row in sec_rows))

    def test_promoter_requires_source_confirmed_original_evidence_before_universe_append(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            universe = root / "global_universe_v2.jsonl"
            universe.write_text(
                json.dumps({"ticker": "NVDA", "company": "NVIDIA"}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            candidates = [
                {
                    "symbol": "DGXX",
                    "company_name": "DGXX Inc.",
                    "market": "US",
                    "ai_module": "ai_datacenter_edge_infra",
                    "candidate_reason": "AI data center platform revenue and backlog",
                    "bfs_seed": "GPU cloud",
                    "bfs_depth_estimate": "D3-D4",
                    "source_url": "https://example.com/dgxx-official",
                    "source_type": "official_press_release",
                    "source_date": "2026-05-12",
                    "evidence_state": "source_confirmed",
                    "confidence": "high",
                    "counterparty": "GPU cloud customers",
                },
                {
                    "symbol": "NOPE",
                    "company_name": "Nope Inc.",
                    "market": "US",
                    "ai_module": "ai_datacenter_edge_infra",
                    "source_url": "https://example.com/nope",
                    "source_type": "news_review_candidate",
                    "evidence_state": "pending_original_source_verification",
                    "confidence": "unreviewed",
                },
            ]
            evidence = [
                {
                    "symbol": "DGXX",
                    "source_url": "https://example.com/dgxx-official",
                    "source_type": "official_press_release",
                    "confidence": "medium",
                    "ai_module": "ai_datacenter_edge_infra",
                    "financial_translation": "revenue_or_segment_mix;orders_backlog_or_rpo",
                    "claim": "Official source links AI data center revenue, orders, and backlog.",
                }
            ]

            promoted, rejected = promoter.promote_rows(
                candidates,
                evidence,
                promoter.load_universe_symbols(universe),
                "2026-05-13",
            )
            appended = promoter.append_universe(universe, promoted)

            self.assertEqual([row["symbol"] for row in promoted], ["DGXX"])
            self.assertEqual(appended, 1)
            errors = {item["symbol"]: item["errors"] for item in rejected}
            self.assertIn("not_source_confirmed", errors["NOPE"])
            lines = [json.loads(line) for line in universe.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(lines[-1]["ticker"], "DGXX")
            self.assertEqual(lines[-1]["current_pool"], "候选池")
            self.assertIn("原文已证明", lines[-1]["evidence_state"])


if __name__ == "__main__":
    unittest.main()
