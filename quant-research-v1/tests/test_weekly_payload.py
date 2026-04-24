from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WEEKLY_PAYLOAD_PATH = REPO_ROOT / "scripts" / "weekly_payload.py"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


weekly_payload = _load_module("weekly_payload", WEEKLY_PAYLOAD_PATH)


class WeeklyPayloadTests(unittest.TestCase):
    def test_collect_weekly_daily_reports_requires_all_trading_day_posts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir)
            for trading_day in ["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"]:
                (reports_dir / f"{trading_day}_report_zh_post.md").write_text(
                    "# 市场日报\n\ncontent\n" * 40,
                    encoding="utf-8",
                )

            with self.assertRaisesRegex(RuntimeError, "2026-04-17_report_zh_post.md"):
                weekly_payload.collect_weekly_daily_reports(
                    date(2026, 4, 17),
                    reports_dir,
                )

    def test_build_weekly_daily_digest_lists_all_found_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir)
            for trading_day in ["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]:
                (reports_dir / f"{trading_day}_report_zh_post.md").write_text(
                    (
                        f"# 市场日报 — {trading_day}\n\n"
                        "**一句话总结**：test summary.\n\n"
                        "**今天大盘**\nline a\nline b\n"
                        "extra detail\n" * 40
                    ),
                    encoding="utf-8",
                )

            digest_lines = weekly_payload.build_weekly_daily_digest(
                date(2026, 4, 17),
                reports_dir,
            )
            digest = "\n".join(digest_lines)

            self.assertIn("Reports found: 5/5", digest)
            self.assertIn("2026-04-13_report_zh_post.md", digest)
            self.assertIn("2026-04-17_report_zh_post.md", digest)
            self.assertIn("一句话总结", digest)


if __name__ == "__main__":
    unittest.main()
