from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "extract_ai_supply_chain_relationship_candidates.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("extract_ai_supply_chain_relationship_candidates", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extractor = _load_module()


class AiSupplyChainRelationshipCandidateExtractorTests(unittest.TestCase):
    def _seed_path(self, root: Path) -> Path:
        seed = root / "seed.yaml"
        seed.write_text(
            """
version: 1
themes:
  - theme_id: ai_datacenter_edge_infra
    supercycle_layer: ai_datacenter_edge_infra
    supply_chain_role: edge AI inference network
    bottleneck_focus: edge inference capacity
    members: [NET]
  - theme_id: ai_compute_accelerators
    supercycle_layer: ai_compute_accelerators
    supply_chain_role: GPU and accelerator platform
    bottleneck_focus: accelerator supply
    members: [NVDA]
""",
            encoding="utf-8",
        )
        return seed

    def test_us_news_extracts_partner_candidate_without_promoting(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            us_db = root / "us.duckdb"
            con = duckdb.connect(str(us_db))
            con.execute(
                """
                CREATE TABLE news_items (
                    symbol VARCHAR,
                    headline VARCHAR,
                    summary VARCHAR,
                    source VARCHAR,
                    url VARCHAR,
                    published_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                INSERT INTO news_items VALUES
                ('NET', 'Cloudflare partners with NVIDIA to expand Workers AI inference',
                 'Cloudflare said the collaboration deploys NVIDIA GPUs across its edge AI network.',
                 'Official', 'https://example.com/net-nvda', TIMESTAMP '2026-05-11 10:00:00'),
                ('NET', 'Money supply and liquidity supply improved for risk assets',
                 'This macro note is not a company supply-chain relationship.',
                 'Macro', 'https://example.com/noise', TIMESTAMP '2026-05-11 11:00:00')
                """
            )
            con.close()

            rows = extractor.extract_candidates(
                us_db=us_db,
                cn_db=root / "missing_cn.duckdb",
                as_of=date(2026, 5, 12),
                lookback_days=7,
                seed_path=self._seed_path(root),
            )

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["market"], "US")
            self.assertEqual(row["primary_symbol"], "NET")
            self.assertEqual(row["counterparty_symbol"], "NVDA")
            self.assertEqual(row["review_state"], "needs_human_source_review")
            self.assertEqual(row["confidence"], "unreviewed")
            self.assertEqual(row["source_url"], "https://example.com/net-nvda")
            self.assertIn("partner", row["relationship_terms"])

    def test_cn_stock_news_extracts_local_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cn_db = root / "cn.duckdb"
            con = duckdb.connect(str(cn_db))
            con.execute(
                """
                CREATE TABLE stock_news (
                    ts_code VARCHAR,
                    publish_time VARCHAR,
                    title VARCHAR,
                    content VARCHAR,
                    source VARCHAR,
                    url VARCHAR
                )
                """
            )
            con.execute(
                """
                INSERT INTO stock_news VALUES
                ('002281.SZ', '2026-05-11 09:30:00',
                 '光迅科技中标AI数据中心光模块订单',
                 '公司光通信产品进入数据中心客户供应链，订单用于AI算力集群。',
                 '本地新闻', 'https://example.com/cn-optical')
                """
            )
            con.close()

            rows = extractor.extract_candidates(
                us_db=root / "missing_us.duckdb",
                cn_db=cn_db,
                as_of=date(2026, 5, 12),
                lookback_days=7,
                seed_path=self._seed_path(root),
            )

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["market"], "CN")
            self.assertEqual(row["primary_symbol"], "002281.SZ")
            self.assertEqual(row["source_url"], "https://example.com/cn-optical")
            self.assertEqual(row["source_date"], "2026-05-11")
            self.assertEqual(row["layer"], "ai_networking_optical_cpo")
            self.assertEqual(row["review_state"], "needs_human_source_review")

    def test_sec_material_agreement_for_seed_symbol_becomes_review_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            us_db = root / "us.duckdb"
            con = duckdb.connect(str(us_db))
            con.execute(
                """
                CREATE TABLE sec_filings (
                    symbol VARCHAR,
                    cik VARCHAR,
                    accession_number VARCHAR,
                    form_type VARCHAR,
                    filed_date DATE,
                    items VARCHAR,
                    description VARCHAR,
                    filing_url VARCHAR,
                    fetched_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                INSERT INTO sec_filings VALUES
                ('NET', '0001477333', '0000000000-26-000001', '8-K', DATE '2026-05-11',
                 '["Item 1.01 — Material Definitive Agreement"]',
                 'Item 1.01 — Material Definitive Agreement',
                 'https://www.sec.gov/example-net', TIMESTAMP '2026-05-11 12:00:00'),
                ('ZZZZ', '0000000000', '0000000000-26-000002', '8-K', DATE '2026-05-11',
                 '["Item 1.01 — Material Definitive Agreement"]',
                 'Item 1.01 — Material Definitive Agreement',
                 'https://www.sec.gov/example-zzzz', TIMESTAMP '2026-05-11 12:00:00')
                """
            )
            con.close()

            rows = extractor.extract_candidates(
                us_db=us_db,
                cn_db=root / "missing_cn.duckdb",
                as_of=date(2026, 5, 12),
                lookback_days=7,
                seed_path=self._seed_path(root),
            )

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["primary_symbol"], "NET")
            self.assertEqual(row["source_table"], "sec_filings")
            self.assertEqual(row["source_type"], "sec_filing_review_candidate")
            self.assertEqual(row["relationship_type"], "sec_material_agreement_candidate")
            self.assertEqual(row["confidence"], "unreviewed")

    def test_write_outputs_keeps_review_contract_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rows = [
                {
                    "relationship_id": "us_net_20260511_x",
                    "as_of": "2026-05-12",
                    "market": "US",
                    "primary_symbol": "NET",
                    "counterparty_symbol": "NVDA",
                    "customer_symbol": "",
                    "symbols": "NET;NVDA",
                    "layer": "ai_datacenter_edge_infra",
                    "relationship_type": "partnership_candidate",
                    "supply_chain_role": "edge AI inference network",
                    "bottleneck_focus": "edge inference capacity",
                    "source_name": "Official",
                    "source_type": "news_review_candidate",
                    "source_url": "https://example.com",
                    "source_date": "2026-05-11",
                    "confidence": "unreviewed",
                    "notes": "review required",
                    "review_state": "needs_human_source_review",
                    "candidate_score": 0.7,
                    "ai_terms": "ai",
                    "relationship_terms": "partners",
                    "counterparty_terms": "NVDA:nvidia",
                    "headline": "Cloudflare partners with NVIDIA",
                    "evidence_text": "Cloudflare partners with NVIDIA",
                    "source_table": "news_items",
                }
            ]
            csv_path, json_path, md_path = extractor.write_outputs(rows, root, date(2026, 5, 12))

            self.assertTrue(csv_path.exists())
            self.assertTrue(md_path.exists())
            text = csv_path.read_text(encoding="utf-8")
            self.assertIn("review_state", text)
            self.assertIn("unreviewed", text)
            payload = json_path.read_text(encoding="utf-8")
            self.assertIn("must not be inserted", payload)
            brief = md_path.read_text(encoding="utf-8")
            self.assertIn("Highest-Priority Checks", brief)
            self.assertIn("source_confirmed", brief)
            self.assertIn("deduplicates", brief)


if __name__ == "__main__":
    unittest.main()
