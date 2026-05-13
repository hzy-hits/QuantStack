"""Tests for the rebalance execution recorder CLI + auto-accept mode."""
from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
RECORDER = STACK_ROOT / "scripts" / "record_rebalance_execution.py"
MAINTAINER = STACK_ROOT / "scripts" / "maintain_rebalance_history.py"


def _load_recorder():
    if "record_rebalance_execution" in sys.modules:
        return sys.modules["record_rebalance_execution"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("record_rebalance_execution", RECORDER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SUGGESTION = {
    "as_of": "2026-05-13",
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


def _seed_history(tmp: Path, *, auto_accept: bool = False) -> tuple[Path, Path]:
    suggestion_path = tmp / "rebalance_suggestion.json"
    suggestion_path.parent.mkdir(parents=True, exist_ok=True)
    suggestion_path.write_text(json.dumps(SUGGESTION), encoding="utf-8")
    history = tmp / "history.csv"
    summary = tmp / "summary.md"
    args = [
        sys.executable,
        str(MAINTAINER),
        "--as-of",
        "2026-05-13",
        "--suggestion-json",
        str(suggestion_path),
        "--history-csv",
        str(history),
        "--summary-md",
        str(summary),
        "--no-backup",
    ]
    if auto_accept:
        args.append("--auto-accept")
    subprocess.run(args, capture_output=True, text=True, check=True)
    return history, summary


def _read(history: Path) -> dict[str, dict[str, str]]:
    with history.open("r", encoding="utf-8") as h:
        return {row["ticker"]: row for row in csv.DictReader(h)}


class RebalanceRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_recorder()

    def test_auto_accept_fills_executed_columns(self) -> None:
        with TemporaryDirectory() as tmp:
            history, summary = _seed_history(Path(tmp), auto_accept=True)
            rows = _read(history)
            self.assertEqual(rows["AAOI"]["executed_tilt_pct"], "+2.50")
            self.assertEqual(rows["NVDA"]["executed_tilt_pct"], "+2.50")
            self.assertEqual(rows["ANET"]["executed_tilt_pct"], "-2.00")
            for row in rows.values():
                self.assertIn("auto-accept", row["notes"])

    def test_accept_subset_only_marks_listed(self) -> None:
        with TemporaryDirectory() as tmp:
            history, summary = _seed_history(Path(tmp))
            result = subprocess.run(
                [
                    sys.executable,
                    str(RECORDER),
                    "--as-of",
                    "2026-05-13",
                    "--history-csv",
                    str(history),
                    "--summary-md",
                    str(summary),
                    "--accept",
                    "AAOI",
                    "--no-backup",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
            rows = _read(history)
            self.assertEqual(rows["AAOI"]["executed_tilt_pct"], "+2.50")
            self.assertEqual(rows["NVDA"]["executed_tilt_pct"], "")
            self.assertEqual(rows["ANET"]["executed_tilt_pct"], "")

    def test_override_wins_and_reject_writes_zero(self) -> None:
        with TemporaryDirectory() as tmp:
            history, summary = _seed_history(Path(tmp))
            result = subprocess.run(
                [
                    sys.executable,
                    str(RECORDER),
                    "--as-of",
                    "2026-05-13",
                    "--history-csv",
                    str(history),
                    "--summary-md",
                    str(summary),
                    "--accept-all",
                    "--override",
                    "AAOI=+1.5",
                    "--reject",
                    "ANET",
                    "--notes",
                    "earnings risk",
                    "--no-backup",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
            rows = _read(history)
            self.assertEqual(rows["AAOI"]["executed_tilt_pct"], "+1.50")
            self.assertIn("override", rows["AAOI"]["notes"])
            self.assertEqual(rows["NVDA"]["executed_tilt_pct"], "+2.50")  # accept-all
            self.assertEqual(rows["ANET"]["executed_tilt_pct"], "0.00")
            self.assertIn("reject", rows["ANET"]["notes"])
            for row in rows.values():
                self.assertIn("earnings risk", row["notes"])

    def test_unknown_ticker_warns_but_does_not_crash(self) -> None:
        with TemporaryDirectory() as tmp:
            history, summary = _seed_history(Path(tmp))
            result = subprocess.run(
                [
                    sys.executable,
                    str(RECORDER),
                    "--as-of",
                    "2026-05-13",
                    "--history-csv",
                    str(history),
                    "--summary-md",
                    str(summary),
                    "--accept",
                    "DGXX",
                    "--no-backup",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("DGXX", result.stderr)
            self.assertIn("ignored", result.stderr)

    def test_no_action_flag_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            history, summary = _seed_history(Path(tmp))
            result = subprocess.run(
                [
                    sys.executable,
                    str(RECORDER),
                    "--as-of",
                    "2026-05-13",
                    "--history-csv",
                    str(history),
                    "--summary-md",
                    str(summary),
                    "--no-backup",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("at least one of", result.stderr)


if __name__ == "__main__":
    unittest.main()
