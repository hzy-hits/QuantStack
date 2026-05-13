"""Tests for the promotion-history ledger maintainer."""
from __future__ import annotations

import csv
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = STACK_ROOT / "scripts" / "maintain_promotion_history.py"


PLAN_HEADERS = [
    "rank",
    "primary_ticker",
    "ticker_field",
    "company",
    "asset_pool",
    "market_country",
    "bfs_depth",
    "module",
    "priority_tier",
    "readiness_tier",
    "readiness_score",
    "recommendation",
    "rationale",
    "primary_sources_to_find",
    "upgrade_conditions",
    "counterevidence",
]


def _write_plan(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in PLAN_HEADERS})


def _row(ticker: str, tier: str = "ready_for_promotion", rec: str = "promote_now") -> dict[str, str]:
    return {
        "rank": "1",
        "primary_ticker": ticker,
        "ticker_field": ticker,
        "company": f"{ticker} Inc",
        "asset_pool": "美国资产池",
        "market_country": "US",
        "bfs_depth": "D2",
        "module": "GPU",
        "priority_tier": "P0",
        "readiness_tier": tier,
        "readiness_score": "1.000",
        "recommendation": rec,
        "rationale": "evidence_state contains 原文已证明",
        "primary_sources_to_find": "10-K",
        "upgrade_conditions": "AI demand",
        "counterevidence": "ASIC",
    }


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False)


class PromotionHistoryTests(unittest.TestCase):
    def test_first_run_writes_header_and_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            history = root / "history.csv"
            _write_plan(plan, [_row("NVDA"), _row("AVGO", tier="evidence_partial", rec="watch_with_review")])
            result = _run(["--plan", str(plan), "--history-csv", str(history), "--as-of", "2026-05-13", "--no-backup"])
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
            with history.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual({row["primary_ticker"] for row in rows}, {"NVDA", "AVGO"})

    def test_idempotent_for_same_date(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            history = root / "history.csv"
            _write_plan(plan, [_row("NVDA"), _row("TSM")])
            _run(["--plan", str(plan), "--history-csv", str(history), "--as-of", "2026-05-13", "--no-backup"])
            # Re-run with same date — should not duplicate
            result = _run(["--plan", str(plan), "--history-csv", str(history), "--as-of", "2026-05-13", "--no-backup"])
            self.assertEqual(result.returncode, 0)
            with history.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)

    def test_separate_dates_accumulate(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            history = root / "history.csv"
            _write_plan(plan, [_row("NVDA")])
            _run(["--plan", str(plan), "--history-csv", str(history), "--as-of", "2026-05-13", "--no-backup"])
            # New plan day with updated tier
            _write_plan(plan, [_row("NVDA", tier="evidence_partial", rec="watch_with_review")])
            _run(["--plan", str(plan), "--history-csv", str(history), "--as-of", "2026-05-14", "--no-backup"])
            with history.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            dates = sorted({row["as_of"] for row in rows})
            self.assertEqual(dates, ["2026-05-13", "2026-05-14"])
            self.assertEqual(
                {(row["as_of"], row["readiness_tier"]) for row in rows},
                {("2026-05-13", "ready_for_promotion"), ("2026-05-14", "evidence_partial")},
            )

    def test_missing_plan_returns_2(self) -> None:
        with TemporaryDirectory() as tmp:
            result = _run([
                "--plan",
                str(Path(tmp) / "absent.csv"),
                "--history-csv",
                str(Path(tmp) / "history.csv"),
                "--as-of",
                "2026-05-13",
                "--no-backup",
            ])
            self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
