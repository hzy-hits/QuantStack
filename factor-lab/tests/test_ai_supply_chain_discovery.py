from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.agent.prompts import parse_agent_response
from src.autoresearch.ai_supply_chain import build_discovery_payload, export_discovery_bundle


class AiSupplyChainDiscoveryTests(unittest.TestCase):
    def test_agent_response_parses_ai_supply_chain_fields(self) -> None:
        parsed = parse_agent_response(
            """
HYPOTHESIS: Persistent volume in CPO names may reveal AI infra accumulation.
FORMULA: rank(ts_mean(volume_ratio,20))
DIRECTION: long
NAME: cpo_relvol_accumulation
SLEEVE: ai_infra_participation
MISPRICING_SOURCE: Slow recognition of optical bottlenecks.
FORCED_COUNTERPARTY: Passive/rebalance sellers.
DATA_REQUIREMENTS: ["official supplier/customer confirmation"]
FAILURE_MODE: Breaks when the theme becomes crowded.
REPORT_CONTRACT: research_only
AI_SUPERCYCLE_LAYER: ai_networking_optical_cpo
SUPPLY_CHAIN_HYPOTHESIS: optical suppliers to hyperscale AI datacenter buildouts
RELATIONSHIP_EVIDENCE_REQUIRED: true
"""
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.ai_supercycle_layer, "ai_networking_optical_cpo")
        self.assertIn("optical suppliers", parsed.supply_chain_hypothesis)
        self.assertEqual(parsed.relationship_evidence_required, "true")

    def test_discovery_export_marks_relationships_as_source_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            log_path = root / "autoresearch.jsonl"
            log_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "session_id": "sess",
                                "market": "us",
                                "name": "cpo_relvol_accumulation",
                                "formula": "rank(ts_mean(volume_ratio,20))",
                                "sleeve_id": "ai_infra_participation",
                                "ai_supercycle_layer": "ai_networking_optical_cpo",
                                "supply_chain_hypothesis": "optical supplier to hyperscaler AI clusters",
                                "relationship_evidence_required": "true",
                                "is_ic_ir": 0.5,
                                "gates": "PASS",
                                "oos": "PASS",
                                "checks_status": "passed",
                                "decision": "candidate",
                                "status": "candidate",
                                "report_contract": "research_only",
                            }
                        ),
                        json.dumps(
                            {
                                "session_id": "sess",
                                "market": "us",
                                "name": "generic_liquidity_supply_noise",
                                "formula": "rank(close)",
                                "mispricing_source": "liquidity supply from rebalancers, not infrastructure exposure",
                                "is_ic_ir": 0.1,
                                "gates": "FAIL",
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_discovery_payload([log_path])
            self.assertEqual(payload["summary"]["rows"], 1)
            row = payload["rows"][0]
            self.assertEqual(row["evidence_state"], "needs_source_confirmation")
            self.assertGreater(row["score"], 50)

            bundle = export_discovery_bundle(output_dir=root / "out", log_paths=[log_path])
            self.assertTrue(bundle["json"].exists())
            self.assertTrue(bundle["markdown"].exists())
            self.assertIn("needs_source_confirmation", bundle["markdown"].read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
