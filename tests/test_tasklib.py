from __future__ import annotations

import io
import sys
import unittest
from pathlib import Path


STACK_ROOT = Path(__file__).resolve().parents[1]
OPS_ROOT = STACK_ROOT / "ops"
if str(OPS_ROOT) not in sys.path:
    sys.path.insert(0, str(OPS_ROOT))

from run_task import Tee  # noqa: E402
from tasklib import materialize_task  # noqa: E402


class BrokenPipeStream:
    def write(self, _text: str) -> None:
        raise BrokenPipeError()

    def flush(self) -> None:
        raise BrokenPipeError()


class TasklibTests(unittest.TestCase):
    def test_legacy_us_daily_report_tasks_are_manual_only(self) -> None:
        postmarket = materialize_task("us.postmarket", "2026-06-26")
        premarket = materialize_task("us.premarket", "2026-06-26")

        self.assertFalse(postmarket["schedule"])
        self.assertFalse(premarket["schedule"])
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

    def test_legacy_cn_daily_report_tasks_are_manual_only(self) -> None:
        morning = materialize_task("cn.morning", "2026-06-26")
        evening = materialize_task("cn.evening", "2026-06-26")

        self.assertFalse(morning["schedule"])
        self.assertFalse(evening["schedule"])
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
        self.assertNotIn("us.postmarket", morning["depends_on"])
        self.assertIn("research.main_strategy_v2_report", evening["depends_on"])
        self.assertEqual(morning["env"]["HERMES_BIN"], "/home/ubuntu/.local/bin/hermes")
        self.assertEqual(evening["env"]["HERMES_BIN"], "/home/ubuntu/.local/bin/hermes")
        self.assertEqual(morning["env"]["CROSS_MARKET_REVIEW_PROVIDER"], "deepseek")
        self.assertEqual(morning["env"]["CROSS_MARKET_REVIEW_MODEL"], "deepseek-v4-pro")
        self.assertEqual(evening["env"]["CROSS_MARKET_REVIEW_PROVIDER"], "deepseek")
        self.assertEqual(evening["env"]["CROSS_MARKET_REVIEW_MODEL"], "deepseek-v4-pro")
        self.assertEqual(morning["env"]["CROSS_MARKET_SEND_EMAIL"], "1")
        self.assertEqual(morning["env"]["QUANT_EMAIL_PROVIDER"], "resend")
        self.assertEqual(morning["env"]["QUANT_EMAIL_FALLBACK_PROVIDER"], "gmail")
        self.assertEqual(morning["env"]["QUANT_DELIVERY_MODE"], "prod")
        self.assertEqual(evening["env"]["QUANT_DELIVERY_MODE"], "prod")
        self.assertNotIn("QUANT_TEST_RECIPIENT", morning["env"])
        self.assertNotIn("QUANT_TEST_RECIPIENT", evening["env"])
        self.assertEqual(evening["env"]["RESEND_ENV_FILE"], "/home/ubuntu/apps/multica/.env")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_PUBLISH"], "1")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_MODE"], "all")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_HOST"], "100.109.146.30")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_USER"], "ivena")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_IDENTITY_FILE"], "/home/ubuntu/.ssh/id_ed25519_quant_pi")
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_PUBLISH"], "1")
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_MODE"], "all")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_AGENT_DELIVER"], "0")
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_AGENT_DELIVER"], "0")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_REPLY_CHANNEL"], "openclaw-weixin")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_REPLY_ACCOUNT"], "912f45c70aa5-im-bot,86fb46c4a557-im-bot")
        self.assertEqual(
            morning["env"]["QUANT_OPENCLAW_REPLY_TO"],
            "o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
        )
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_MESSAGE_CHANNEL"], "openclaw-weixin")
        self.assertEqual(morning["env"]["QUANT_OPENCLAW_MESSAGE_ACCOUNT"], "912f45c70aa5-im-bot,86fb46c4a557-im-bot")
        self.assertEqual(
            morning["env"]["QUANT_OPENCLAW_MESSAGE_TARGET"],
            "o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
        )
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_REPLY_CHANNEL"], "openclaw-weixin")
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_REPLY_ACCOUNT"], "912f45c70aa5-im-bot,86fb46c4a557-im-bot")
        self.assertEqual(
            evening["env"]["QUANT_OPENCLAW_REPLY_TO"],
            "o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
        )
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_MESSAGE_CHANNEL"], "openclaw-weixin")
        self.assertEqual(evening["env"]["QUANT_OPENCLAW_MESSAGE_ACCOUNT"], "912f45c70aa5-im-bot,86fb46c4a557-im-bot")
        self.assertEqual(
            evening["env"]["QUANT_OPENCLAW_MESSAGE_TARGET"],
            "o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
        )
        self.assertTrue(morning["sends_email"])
        self.assertTrue(evening["sends_email"])

    def test_only_cross_market_tasks_send_scheduled_reports(self) -> None:
        task_ids = [
            "us.premarket",
            "us.postmarket",
            "cn.morning",
            "cn.evening",
            "daily.cross_market_am",
            "daily.cross_market_pm",
            "weekly.us",
            "weekly.cn",
        ]
        active_senders = [
            task_id
            for task_id in task_ids
            if materialize_task(task_id, "2026-06-26")["schedule"]
            and materialize_task(task_id, "2026-06-26")["sends_email"]
        ]
        self.assertEqual(active_senders, ["daily.cross_market_am", "daily.cross_market_pm"])

    def test_legacy_weekly_reports_are_manual_only(self) -> None:
        us = materialize_task("weekly.us", "2026-06-26")
        cn = materialize_task("weekly.cn", "2026-06-26")

        self.assertFalse(us["schedule"])
        self.assertFalse(cn["schedule"])
        self.assertFalse(us["sends_email"])
        self.assertFalse(cn["sends_email"])

    def test_legacy_report_watchdog_is_manual_only(self) -> None:
        reboot = materialize_task("us.watchdog.reboot", "2026-06-26")
        interval = materialize_task("us.watchdog", "2026-06-26")

        self.assertFalse(reboot["schedule"])
        self.assertFalse(interval["schedule"])
        self.assertFalse(reboot["sends_email"])
        self.assertFalse(interval["sends_email"])

    def test_run_task_tee_keeps_file_log_when_stdout_pipe_breaks(self) -> None:
        log = io.StringIO()
        tee = Tee([BrokenPipeStream(), log])

        tee.write("still log this\n")

        self.assertEqual(log.getvalue(), "still log this\n")
        self.assertEqual(tee.files, [log])


if __name__ == "__main__":
    unittest.main()
