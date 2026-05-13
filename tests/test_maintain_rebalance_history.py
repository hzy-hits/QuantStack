"""Tests for the rebalance history ledger maintainer."""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "maintain_rebalance_history.py"


def _load_module():
    if "maintain_rebalance_history" in sys.modules:
        return sys.modules["maintain_rebalance_history"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("maintain_rebalance_history", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _suggestion(as_of: str) -> dict:
    return {
        "as_of": as_of,
        "summary": {"total_add_pct": 5.0, "total_rotate_in_pct": 0.0, "total_trim_pct": -2.0},
        "leaders": [
            {"ticker": "AAOI", "company": "Applied Optoelectronics", "action": "add", "tilt_pct": 2.5, "rationale": "bull rising"},
            {"ticker": "NVDA", "company": "NVIDIA", "action": "add", "tilt_pct": 2.5, "rationale": "bull rising"},
        ],
        "rotate_in": [],
        "trim": [
            {"ticker": "ANET", "company": "Arista", "action": "trim", "tilt_pct": -2.0, "rationale": "rich valuation"},
        ],
    }


def _write_suggestion(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class RebalanceHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_first_run_creates_history(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            suggestion_path = root / "rebalance_suggestion.json"
            history = root / "history.csv"
            summary = root / "summary.md"
            _write_suggestion(suggestion_path, _suggestion("2026-05-13"))
            payload = self.module._load_suggestion(suggestion_path)
            new_rows = self.module._suggestion_rows(payload)
            cache: dict = {}
            self.module._merge_rows(cache, new_rows)
            self.module._write_history(history, cache)
            summary.write_text(self.module.render_summary(cache, "2026-05-13"), encoding="utf-8")
            with history.open("r", encoding="utf-8") as h:
                rows = list(csv.DictReader(h))
            self.assertEqual({r["ticker"] for r in rows}, {"AAOI", "NVDA", "ANET"})
            md = summary.read_text(encoding="utf-8")
            self.assertIn("Last 30 Suggestions", md)
            self.assertIn("AAOI", md)

    def test_operator_edits_preserved_on_rerun(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            suggestion_path = root / "rebalance_suggestion.json"
            history = root / "history.csv"
            _write_suggestion(suggestion_path, _suggestion("2026-05-13"))
            payload = self.module._load_suggestion(suggestion_path)
            cache: dict = {}
            self.module._merge_rows(cache, self.module._suggestion_rows(payload))
            # Simulate operator filling executed_tilt_pct + notes for AAOI.
            key = ("2026-05-13", "AAOI", "add")
            cache[key]["executed_tilt_pct"] = "+1.50"
            cache[key]["executed_at"] = "2026-05-14T09:30"
            cache[key]["notes"] = "partial fill"
            self.module._write_history(history, cache)

            # Re-run: maintainer should not overwrite executed_* / notes.
            cache2 = self.module._load_history(history)
            self.module._merge_rows(cache2, self.module._suggestion_rows(payload))
            self.module._write_history(history, cache2)

            with history.open("r", encoding="utf-8") as h:
                rows = {(r["as_of"], r["ticker"], r["action"]): r for r in csv.DictReader(h)}
            self.assertEqual(rows[key]["executed_tilt_pct"], "+1.50")
            self.assertEqual(rows[key]["notes"], "partial fill")

    def test_render_summary_flags_drift_at_least_1pct(self) -> None:
        history = {
            ("2026-05-13", "AAA", "add"): {
                "as_of": "2026-05-13",
                "ticker": "AAA",
                "company": "AAA Corp",
                "action": "add",
                "suggested_tilt_pct": "+2.50",
                "rationale": "bull",
                "executed_tilt_pct": "+1.20",
                "executed_at": "2026-05-14",
                "notes": "risk cap",
            },
            ("2026-05-13", "BBB", "add"): {
                "as_of": "2026-05-13",
                "ticker": "BBB",
                "company": "BBB Corp",
                "action": "add",
                "suggested_tilt_pct": "+2.50",
                "rationale": "bull",
                "executed_tilt_pct": "+2.30",  # drift only 0.20%, below threshold
                "executed_at": "2026-05-14",
                "notes": "",
            },
        }
        md = self.module.render_summary(history, "2026-05-13")
        self.assertIn("Significant Drift Rows", md)
        self.assertIn("AAA", md)
        # BBB should not show up in the drift section (gap < 1%).
        # Slice the drift section out to assert.
        drift_section = md.split("Significant Drift Rows")[1]
        self.assertNotIn("| BBB ", drift_section)

    def test_missing_suggestion_does_not_crash(self) -> None:
        with TemporaryDirectory() as tmp:
            self.assertIsNone(self.module._load_suggestion(Path(tmp) / "absent.json"))


if __name__ == "__main__":
    unittest.main()
