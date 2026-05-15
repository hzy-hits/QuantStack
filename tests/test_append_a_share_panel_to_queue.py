"""Tests for the A-share BFS panel appender."""
from __future__ import annotations

import csv
import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "append_a_share_panel_to_queue.py"


def _load_module():
    if "append_a_share_panel_to_queue" in sys.modules:
        return sys.modules["append_a_share_panel_to_queue"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("append_a_share_panel_to_queue", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_queue(path: Path, rows: list[dict[str, str]], module) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=module.FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            full = {key: "" for key in module.FIELDS}
            full.update(row)
            writer.writerow(full)


class TierDerivationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_d1_proven_lands_in_p0_core_review(self) -> None:
        tier, pool, score, bucket = self.module._tier_for("原文已证明: data", "D1", "agent")
        self.assertEqual(tier, "P0_first_batch")
        self.assertEqual(pool, "核心候选")
        self.assertEqual(bucket, "core_review")

    def test_d2_proven_lands_in_p1(self) -> None:
        tier, _, _, _ = self.module._tier_for("原文已证明: split", "D2", "agent")
        self.assertEqual(tier, "P1_d1_d3_followup")

    def test_three_hit_bumps_tier_up_one_step(self) -> None:
        # Default 待原文核验 + D3 = P3; with 3-hit it bumps to P2.
        tier_default, *_ = self.module._tier_for("待原文核验", "D3", "agent")
        tier_threehit, *_ = self.module._tier_for("待原文核验", "D3", "3-hit")
        self.assertEqual(tier_default, "P3_deep_radar")
        self.assertEqual(tier_threehit, "P2_radar_if_blocks_d2")

    def test_one_hit_demotes_tier_one_step(self) -> None:
        # 原文已证明 + D1 = P0; with 1-hit it demotes to P1.
        tier_one_hit, *_ = self.module._tier_for("原文已证明", "D1", "1-hit")
        self.assertEqual(tier_one_hit, "P1_d1_d3_followup")


class AppendMainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def _run_main(self, queue: Path, *extra: str) -> int:
        argv = sys.argv[:]
        sys.argv = ["append_a_share_panel_to_queue.py", "--queue", str(queue), "--no-backup", *extra]
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = self.module.main()
            self.last_stdout = buf.getvalue()
            return rc
        finally:
            sys.argv = argv

    def test_returns_two_when_queue_missing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "missing.csv"
            rc = self._run_main(queue)
            self.assertEqual(rc, 2)

    def test_dry_run_does_not_modify_queue(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            _seed_queue(queue, [], self.module)
            before = queue.read_text(encoding="utf-8")
            rc = self._run_main(queue, "--dry-run")
            self.assertEqual(rc, 0)
            self.assertEqual(queue.read_text(encoding="utf-8"), before)
            self.assertIn(f"panel size: {len(self.module.PANEL)}", self.last_stdout)

    def test_append_is_idempotent_on_second_run(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            _seed_queue(queue, [], self.module)
            rc1 = self._run_main(queue)
            self.assertEqual(rc1, 0)
            with queue.open() as fh:
                rows_after_first = list(csv.DictReader(fh))
            rc2 = self._run_main(queue)
            self.assertEqual(rc2, 0)
            with queue.open() as fh:
                rows_after_second = list(csv.DictReader(fh))
            self.assertEqual(len(rows_after_first), len(rows_after_second))
            # second run reports nothing to add
            self.assertIn("nothing to add", self.last_stdout)

    def test_skips_tickers_already_present_in_queue(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            existing_ticker = self.module.PANEL[0]["ticker"]
            _seed_queue(
                queue,
                [{"rank": "1", "ticker": existing_ticker, "company": "preexisting"}],
                self.module,
            )
            rc = self._run_main(queue, "--dry-run")
            self.assertEqual(rc, 0)
            self.assertIn("already in queue: 1", self.last_stdout)


if __name__ == "__main__":
    unittest.main()
