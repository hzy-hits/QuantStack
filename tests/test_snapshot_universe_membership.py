"""Tests for the PIT universe membership snapshot builder."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "snapshot_universe_membership.py"


def _load_module():
    if "snapshot_universe_membership" in sys.modules:
        return sys.modules["snapshot_universe_membership"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("snapshot_universe_membership", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MembersFromContentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_proven_name_is_production_pending_is_not(self) -> None:
        content = (
            '{"market_country":"US","ticker":"NVDA",'
            '"evidence_state":"原文已证明: data center strong"}\n'
            '{"market_country":"US","ticker":"AVGO",'
            '"evidence_state":"合理推论+待原文核验: needs source"}\n'
        )
        members = self.m.members_from_content(content)
        self.assertIn("NVDA", members["US"])      # proven head → production
        self.assertNotIn("AVGO", members["US"])   # pending flag → research only

    def test_excluded_record_dropped(self) -> None:
        content = (
            '{"market_country":"US","ticker":"FOO",'
            '"evidence_state":"原文已证明: ok","current_pool":"排除"}\n'
        )
        members = self.m.members_from_content(content)
        self.assertNotIn("FOO", members["US"])

    def test_markets_split(self) -> None:
        content = (
            '{"market_country":"CN","ticker":"002475.SZ",'
            '"evidence_state":"原文已证明: ok"}\n'
        )
        members = self.m.members_from_content(content)
        self.assertIn("002475.SZ", members["CN"])
        self.assertEqual(members["US"], [])


if __name__ == "__main__":
    unittest.main()
