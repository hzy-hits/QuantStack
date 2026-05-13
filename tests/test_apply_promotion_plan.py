"""Tests for the readiness → expansion_candidates_promoted closed loop."""
from __future__ import annotations

import csv
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = STACK_ROOT / "scripts" / "apply_promotion_plan.py"


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


def _plan_row(ticker: str, rec: str = "promote_now", **overrides: object) -> dict[str, str]:
    row = {
        "rank": "1",
        "primary_ticker": ticker,
        "ticker_field": ticker,
        "company": f"{ticker} Inc",
        "asset_pool": "美国资产池",
        "market_country": "US",
        "bfs_depth": "D2",
        "module": "GPU",
        "priority_tier": "P0_first_batch",
        "readiness_tier": "ready_for_promotion",
        "readiness_score": "1.000",
        "recommendation": rec,
        "rationale": "evidence_state contains 原文已证明",
        "primary_sources_to_find": "10-K",
        "upgrade_conditions": "AI demand confirmed",
        "counterevidence": "ASIC",
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False)


class ApplyPromotionPlanTests(unittest.TestCase):
    def test_dry_run_does_not_write(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            promoted = root / "promoted.csv"
            promoted.write_text("", encoding="utf-8")
            _write_plan(plan, [_plan_row("NVDA"), _plan_row("AVGO", rec="watch_with_review")])
            result = _run(["--plan", str(plan), "--promoted", str(promoted), "--as-of", "2026-05-13"])
            self.assertEqual(result.returncode, 0)
            self.assertIn("NVDA", result.stdout)
            self.assertIn("Dry run", result.stdout)
            self.assertEqual(promoted.read_text(encoding="utf-8"), "")

    def test_confirm_appends_promote_now_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            promoted = root / "promoted.csv"
            promoted.write_text("", encoding="utf-8")
            _write_plan(
                plan,
                [
                    _plan_row("NVDA"),
                    _plan_row("TSM", asset_pool="卫星资产池", market_country="台湾"),
                    _plan_row("AVGO", rec="watch_with_review"),
                ],
            )
            result = _run(
                [
                    "--plan",
                    str(plan),
                    "--promoted",
                    str(promoted),
                    "--as-of",
                    "2026-05-13",
                    "--confirm",
                ]
            )
            self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
            with promoted.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            tickers = {row["symbol"] for row in rows}
            self.assertEqual(tickers, {"NVDA", "TSM"})
            markets = {row["symbol"]: row["market"] for row in rows}
            self.assertEqual(markets["TSM"], "Satellite")

    def test_tickers_filter_restricts_subset(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            promoted = root / "promoted.csv"
            promoted.write_text("", encoding="utf-8")
            _write_plan(plan, [_plan_row("NVDA"), _plan_row("TSM"), _plan_row("AVGO")])
            result = _run(
                [
                    "--plan",
                    str(plan),
                    "--promoted",
                    str(promoted),
                    "--as-of",
                    "2026-05-13",
                    "--tickers",
                    "NVDA",
                    "--confirm",
                ]
            )
            self.assertEqual(result.returncode, 0)
            with promoted.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row["symbol"] for row in rows], ["NVDA"])

    def test_existing_symbols_are_skipped(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = root / "plan.csv"
            promoted = root / "promoted.csv"
            promoted.write_text(
                "as_of,symbol,company_name,market,ai_module,source_url,source_type,source_date,confidence,evidence_state,financial_translation,universe_row\n"
                "2026-04-01,NVDA,NVIDIA,US,GPU,manual,seed,2026-04-01,high,seed,initial seed,\n",
                encoding="utf-8",
            )
            _write_plan(plan, [_plan_row("NVDA"), _plan_row("TSM")])
            result = _run(
                [
                    "--plan",
                    str(plan),
                    "--promoted",
                    str(promoted),
                    "--as-of",
                    "2026-05-13",
                    "--confirm",
                ]
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("NVDA", result.stdout)  # listed as already promoted
            with promoted.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            tickers = [row["symbol"] for row in rows]
            self.assertEqual(tickers, ["NVDA", "TSM"])

    def test_missing_plan_returns_exit_code_2(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = _run([
                "--plan",
                str(root / "missing.csv"),
                "--promoted",
                str(root / "promoted.csv"),
                "--as-of",
                "2026-05-13",
            ])
            self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
