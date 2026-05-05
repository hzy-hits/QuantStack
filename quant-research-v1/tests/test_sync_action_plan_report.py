import importlib.util
from pathlib import Path
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "sync_action_plan_report.py"
spec = importlib.util.spec_from_file_location("sync_action_plan_report", SCRIPT_PATH)
sync_action_plan_report = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(sync_action_plan_report)


class SyncActionPlanReportTests(unittest.TestCase):
    def test_build_action_plan_snippet_translates_price_plan(self):
        structural = """
## Action Plan Ledger

### Setup / Wait Plans

| Symbol / Company | Direction | Confidence | Entry / Review | Stop / Invalid | Target | R:R | Exp move | Time exit | State reason |
|------------------|-----------|------------|----------------|----------------|--------|-----|----------|-----------|--------------|
| MPWR / Monolithic Power Systems Inc | long | MODERATE | $1614.41 | $1510.93 | $1921.15 | 2.96 | +19.0% | 3 sessions / next catalyst | event known; require second-day acceptance |

### Blocked / No-Chase Plans

| Symbol / Company | Direction | Confidence | Entry / Review | Stop / Invalid | Target | R:R | Exp move | Time exit | State reason |
|------------------|-----------|------------|----------------|----------------|--------|-----|----------|-----------|--------------|
| PWR / Quanta Services Inc | long | MODERATE | $727.77 | $694.39 | $798.22 | 2.11 | +9.7% | 3 sessions / next catalyst | move already paid / stale chase risk; stale chase / already paid |

---

## Setup Alpha / Anti-Chase
"""
        snippet = sync_action_plan_report.build_action_plan_snippet(structural)

        self.assertIn("### 价格计划", snippet)
        self.assertIn("MPWR / Monolithic Power Systems Inc", snippet)
        self.assertIn("$1510.93", snippet)
        self.assertIn("事件已公开；只看第二日承接", snippet)
        self.assertIn("PWR / Quanta Services Inc", snippet)
        self.assertIn("涨幅已兑现/追高风险", snippet)
        self.assertNotIn("<!--", snippet)


if __name__ == "__main__":
    unittest.main()
