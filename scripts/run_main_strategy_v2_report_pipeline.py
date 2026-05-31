#!/usr/bin/env python3
"""Run the production Main Strategy V2 daily report pipeline.

Order is intentional:
1. Build the realized report-action backtest summary for the report date.
2. Generate the daily report, which reads that summary into the payload.
3. Validate the generated daily reports against production contracts.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
DEFAULT_BACKTEST_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2_report_backtest"
DEFAULT_CN_DB = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn_report.duckdb"
DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant_report.duckdb"
if not DEFAULT_US_DB.exists():
    DEFAULT_US_DB = STACK_ROOT / "quant-research-v1" / "data" / "quant.duckdb"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Main Strategy V2 report backtest -> render -> validate.")
    parser.add_argument("--date", default=None, help="Report date YYYY-MM-DD. Defaults to local today.")
    parser.add_argument("--start", default="2025-01-01", help="Backtest start date passed to the report generator.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--backtest-root", type=Path, default=DEFAULT_BACKTEST_ROOT)
    parser.add_argument("--us-db", type=Path, default=DEFAULT_US_DB)
    parser.add_argument("--cn-db", type=Path, default=DEFAULT_CN_DB)
    parser.add_argument("--promotion-db", type=Path, default=None)
    parser.add_argument(
        "--ai-infra-mode",
        choices=["off", "enforce", "expand", "enforce_expand"],
        default=None,
    )
    parser.add_argument("--skip-backtest", action="store_true")
    parser.add_argument("--skip-validate", action="store_true")
    return parser.parse_args()


def run_command(cmd: list[str]) -> None:
    print("+ " + " ".join(str(part) for part in cmd), flush=True)
    subprocess.run([str(part) for part in cmd], cwd=STACK_ROOT, check=True)


def generator_command(args: argparse.Namespace, report_date: str) -> list[str]:
    cmd = [
        sys.executable,
        "scripts/generate_main_strategy_v2_report.py",
        "--date",
        report_date,
        "--start",
        args.start,
        "--output-root",
        str(args.output_root),
        "--us-db",
        str(args.us_db),
        "--cn-db",
        str(args.cn_db),
    ]
    if args.promotion_db is not None:
        cmd.extend(["--promotion-db", str(args.promotion_db)])
    if args.ai_infra_mode is not None:
        cmd.extend(["--ai-infra-mode", args.ai_infra_mode])
    return cmd


def backtest_command(args: argparse.Namespace, report_date: str) -> list[str]:
    return [
        sys.executable,
        "scripts/backtest_main_strategy_v2_daily_reports.py",
        "--report-root",
        str(args.output_root),
        "--cn-db",
        str(args.cn_db),
        "--us-db",
        str(args.us_db),
        "--before-date",
        report_date,
        "--price-through-date",
        report_date,
        "--output-dir",
        str(args.backtest_root / report_date),
        "--quiet",
    ]


def validator_command(args: argparse.Namespace, report_date: str) -> list[str]:
    return [
        sys.executable,
        "scripts/validate_main_strategy_v2_reports.py",
        "--date",
        report_date,
        "--report-root",
        str(args.output_root),
    ]


def main() -> None:
    args = parse_args()
    report_date = args.date or date.today().isoformat()
    if not args.skip_backtest:
        run_command(backtest_command(args, report_date))
    run_command(generator_command(args, report_date))
    if not args.skip_validate:
        run_command(validator_command(args, report_date))
    print(f"Main Strategy V2 pipeline complete: {args.output_root / report_date}")


if __name__ == "__main__":
    main()
