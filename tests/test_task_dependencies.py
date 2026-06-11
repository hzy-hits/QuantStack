from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_under_test", REPO_ROOT / "ops" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tasklib = _load("tasklib")
CST = timezone(timedelta(hours=8))


class DependencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.state_dir = Path(tempfile.mkdtemp())

    def _write_success(self, task_id: str, finished: datetime) -> None:
        (self.state_dir / f"{task_id}.last_success.json").write_text(
            json.dumps({"task_id": task_id, "finished_at": finished.isoformat()}), encoding="utf-8")

    def test_unmet_when_dep_has_no_success_today(self) -> None:
        now = datetime(2026, 6, 10, 12, 25, tzinfo=CST)
        self._write_success("research.bubble_hedge_radar", now - timedelta(days=1))
        task = {"task_id": "research.risk_regime_engine",
                "depends_on": ["research.bubble_hedge_radar"],
                "schedule": "17 12 * * 1-5"}
        registry = {"research.bubble_hedge_radar": {"task_id": "research.bubble_hedge_radar",
                                                    "schedule": "14 12 * * 1-5"}}
        unmet = tasklib.unmet_dependencies(task, registry=registry, state_dir=self.state_dir, now=now)
        self.assertEqual(unmet, ["research.bubble_hedge_radar"])

    def test_met_when_dep_succeeded_today(self) -> None:
        now = datetime(2026, 6, 10, 12, 25, tzinfo=CST)
        self._write_success("research.bubble_hedge_radar", now - timedelta(minutes=10))
        task = {"task_id": "research.risk_regime_engine",
                "depends_on": ["research.bubble_hedge_radar"],
                "schedule": "17 12 * * 1-5"}
        registry = {"research.bubble_hedge_radar": {"task_id": "research.bubble_hedge_radar",
                                                    "schedule": "14 12 * * 1-5"}}
        self.assertEqual(tasklib.unmet_dependencies(task, registry=registry,
                                                    state_dir=self.state_dir, now=now), [])

    def test_dep_not_scheduled_today_counts_as_met(self) -> None:
        # 周六:dep 只在工作日跑,不该阻塞周末任务
        now = datetime(2026, 6, 13, 10, 30, tzinfo=CST)  # Saturday
        task = {"task_id": "weekly.us", "depends_on": ["research.bubble_hedge_radar"],
                "schedule": "30 9 * * 6"}
        registry = {"research.bubble_hedge_radar": {"task_id": "research.bubble_hedge_radar",
                                                    "schedule": "14 12 * * 1-5"}}
        self.assertEqual(tasklib.unmet_dependencies(task, registry=registry,
                                                    state_dir=self.state_dir, now=now), [])


if __name__ == "__main__":
    unittest.main()
