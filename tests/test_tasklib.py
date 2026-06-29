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

        self.assertEqual(
            postmarket["command"],
            ["./scripts/run_full.sh", "--prod", "--delivery-dry-run", "2026-06-26"],
        )
        self.assertEqual(
            premarket["command"],
            ["./scripts/run_full.sh", "--prod", "--delivery-dry-run", "--premarket", "2026-06-26"],
        )
        self.assertFalse(postmarket["sends_email"])
        self.assertFalse(premarket["sends_email"])

    def test_legacy_cn_daily_tasks_do_not_send_email_by_default(self) -> None:
        morning = materialize_task("cn.morning", "2026-06-26")
        evening = materialize_task("cn.evening", "2026-06-26")

        self.assertNotIn("--send-reports", morning["command"])
        self.assertNotIn("--send-reports", evening["command"])
        self.assertFalse(morning["sends_email"])
        self.assertFalse(evening["sends_email"])

    def test_cross_market_tasks_use_hermes_with_fallback(self) -> None:
        morning = materialize_task("daily.cross_market_am", "2026-06-26")
        evening = materialize_task("daily.cross_market_pm", "2026-06-26")

        self.assertEqual(morning["schedule"], "30 7 * * 1-6")
        self.assertEqual(evening["schedule"], "30 18 * * 1-5")
        self.assertEqual(morning["command"][0], "quant-research-v1/.venv/bin/python")
        self.assertEqual(evening["command"][0], "quant-research-v1/.venv/bin/python")
        self.assertIn("--agent-backend", morning["command"])
        self.assertIn("hermes", morning["command"])
        self.assertIn("--fallback-backend", evening["command"])
        self.assertIn("auto", evening["command"])
        self.assertIn("us.postmarket", morning["depends_on"])
        self.assertIn("research.main_strategy_v2_report", evening["depends_on"])
        self.assertEqual(morning["env"]["HERMES_BIN"], "/home/ubuntu/.local/bin/hermes")
        self.assertEqual(evening["env"]["HERMES_BIN"], "/home/ubuntu/.local/bin/hermes")
        self.assertEqual(morning["env"]["CROSS_MARKET_REVIEW_PROVIDER"], "deepseek")
        self.assertEqual(morning["env"]["CROSS_MARKET_REVIEW_MODEL"], "deepseek-v3")
        self.assertEqual(evening["env"]["CROSS_MARKET_REVIEW_PROVIDER"], "deepseek")
        self.assertEqual(evening["env"]["CROSS_MARKET_REVIEW_MODEL"], "deepseek-v3")
        self.assertEqual(morning["env"]["CROSS_MARKET_SEND_EMAIL"], "1")
        self.assertEqual(morning["env"]["QUANT_EMAIL_PROVIDER"], "resend")
        self.assertEqual(evening["env"]["RESEND_ENV_FILE"], "/home/ubuntu/apps/multica/.env")
        self.assertTrue(morning["sends_email"])
        self.assertTrue(evening["sends_email"])


if __name__ == "__main__":
    unittest.main()
