from __future__ import annotations

import sys
import unittest
from pathlib import Path


STACK_ROOT = Path(__file__).resolve().parents[1]
OPS_ROOT = STACK_ROOT / "ops"
if str(OPS_ROOT) not in sys.path:
    sys.path.insert(0, str(OPS_ROOT))

from tasklib import materialize_task  # noqa: E402


class TasklibTests(unittest.TestCase):
    def test_us_daily_tasks_pass_date_override_to_run_full(self) -> None:
        postmarket = materialize_task("us.postmarket", "2026-06-26")
        premarket = materialize_task("us.premarket", "2026-06-26")

        self.assertEqual(postmarket["command"], ["./scripts/run_full.sh", "--prod", "2026-06-26"])
        self.assertEqual(
            premarket["command"],
            ["./scripts/run_full.sh", "--prod", "--premarket", "2026-06-26"],
        )


if __name__ == "__main__":
    unittest.main()
