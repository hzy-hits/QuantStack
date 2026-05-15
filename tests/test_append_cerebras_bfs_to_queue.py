"""Tests for the Cerebras BFS appender."""
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
SCRIPT_PATH = STACK_ROOT / "scripts" / "append_cerebras_bfs_to_queue.py"


def _load_module():
    if "append_cerebras_bfs_to_queue" in sys.modules:
        return sys.modules["append_cerebras_bfs_to_queue"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("append_cerebras_bfs_to_queue", SCRIPT_PATH)
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


class CerebrasAppenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def _run_main(self, queue: Path, *extra: str) -> tuple[int, str]:
        argv = sys.argv[:]
        sys.argv = ["append_cerebras_bfs_to_queue.py", "--queue", str(queue), "--no-backup", *extra]
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = self.module.main()
            return rc, buf.getvalue()
        finally:
            sys.argv = argv

    def test_returns_two_when_queue_missing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "missing.csv"
            rc, _ = self._run_main(queue)
            self.assertEqual(rc, 2)

    def test_dry_run_does_not_modify_queue(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            _seed_queue(queue, [], self.module)
            before = queue.read_text(encoding="utf-8")
            rc, out = self._run_main(queue, "--dry-run")
            self.assertEqual(rc, 0)
            self.assertEqual(queue.read_text(encoding="utf-8"), before)
            self.assertIn("dry-run", out)

    def test_idempotent_appends_then_skips_on_rerun(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            _seed_queue(queue, [], self.module)
            rc1, out1 = self._run_main(queue)
            self.assertEqual(rc1, 0)
            with queue.open() as fh:
                rows1 = list(csv.DictReader(fh))
            self.assertEqual(len(rows1), len(self.module.CANDIDATES))

            rc2, out2 = self._run_main(queue)
            self.assertEqual(rc2, 0)
            with queue.open() as fh:
                rows2 = list(csv.DictReader(fh))
            self.assertEqual(len(rows1), len(rows2))
            self.assertIn("No new tickers", out2)

    def test_existing_primary_ticker_is_skipped(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            existing = self.module.CANDIDATES[0]["ticker"].split("/")[0].strip()
            _seed_queue(
                queue,
                [{"rank": "9999", "ticker": existing, "company": "x"}],
                self.module,
            )
            rc, out = self._run_main(queue, "--dry-run")
            self.assertEqual(rc, 0)
            # Confirm dry-run output omits the seeded ticker from "to add" list.
            self.assertNotIn(f"+ rank=10000 ticker={existing:20}", out)

    def test_appended_rows_carry_default_verification_status(self) -> None:
        with TemporaryDirectory() as tmpdir:
            queue = Path(tmpdir) / "queue.csv"
            _seed_queue(queue, [], self.module)
            rc, _ = self._run_main(queue)
            self.assertEqual(rc, 0)
            with queue.open() as fh:
                rows = list(csv.DictReader(fh))
            self.assertGreater(len(rows), 0)
            for row in rows:
                self.assertEqual(row["verification_status"], self.module.TPL_VERIFICATION_DEFAULT)


if __name__ == "__main__":
    unittest.main()
