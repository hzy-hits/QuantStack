#!/usr/bin/env python3
"""Validate Main Strategy V2 daily reports against production contracts."""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"

FORBIDDEN_DAILY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("forbidden_option_1r_sizing", re.compile(r"仓位\s*:\s*1R", re.IGNORECASE)),
    ("forbidden_option_tactic_label", re.compile(r"打法\s*[:：]")),
    ("forbidden_short_option_size", re.compile(r"(?:≤|<=)\s*0\.3R", re.IGNORECASE)),
    ("forbidden_option_capital_instruction", re.compile(r"资金占股票仓位", re.IGNORECASE)),
    ("forbidden_old_leaps_structure", re.compile(r"12-18\s*月\s*ATM\s*call", re.IGNORECASE)),
    ("forbidden_old_weekly_option_structure", re.compile(r"本周末或下周到期\s*OTM\s*call", re.IGNORECASE)),
)
OLD_US_HOLD_PHRASES = (
    "3个交易日或下个催化前复核",
    "3 sessions / next catalyst",
)
CN_NON_AI_EXECUTION_POLLUTION = ("600519.SH", "贵州茅台")
US_SINGLE_NAME_R_CAP = 0.125


@dataclass(frozen=True)
class ValidationFailure:
    code: str
    detail: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Main Strategy V2 report artifacts.")
    parser.add_argument("--date", required=True, help="Report date YYYY-MM-DD.")
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--allow-missing-horizon-edge", action="store_true")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"missing report json: {path}") from None
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid report json: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"report json is not an object: {path}")
    return payload


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise SystemExit(f"missing report markdown: {path}") from None


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def market_actionables(payload: dict[str, Any], market: str) -> list[dict[str, Any]]:
    wanted = market.upper()
    rows = ((payload.get("production_decision_summary") or {}).get("actionable") or [])
    return [row for row in rows if str(row.get("market") or "").upper() == wanted]


def actionable_symbols(payload: dict[str, Any], market: str) -> set[str]:
    return {
        str(row.get("symbol") or "").upper()
        for row in market_actionables(payload, market)
        if row.get("symbol")
    }


def summary_us_exposure(payload: dict[str, Any]) -> tuple[float, int]:
    summary = ((payload.get("production_decision_summary") or {}).get("summary") or {})
    us_r = as_float(summary.get("us_r"))
    us_action_count = as_int(summary.get("us_action_count"))
    if us_action_count <= 0:
        us_action_count = len(market_actionables(payload, "US"))
    return us_r, us_action_count


def validate_forbidden_daily_copy(files: dict[str, str]) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    for filename, text in files.items():
        for code, pattern in FORBIDDEN_DAILY_PATTERNS:
            match = pattern.search(text)
            if match:
                failures.append(
                    ValidationFailure(code, f"{filename} contains `{match.group(0)}`")
                )
    return failures


def validate_us_gate(payload: dict[str, Any]) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    us_r, us_action_count = summary_us_exposure(payload)
    bulletin = payload.get("strategy_alpha_bulletin") or {}
    ev_status = str(((bulletin.get("ev_status") or {}).get("us")) or "").lower()
    selected_policy = (bulletin.get("selected_policies") or {}).get("us")
    gate_passed = ev_status == "passed" and bool(selected_policy)
    if not gate_passed and (us_r > 0.000001 or us_action_count > 0):
        failures.append(
            ValidationFailure(
                "us_ev_gate_failed_with_execution_r",
                f"ev_status.us={ev_status or '-'} selected_policy={selected_policy or '-'} "
                f"but us_r={us_r:.4f}, us_action_count={us_action_count}",
            )
        )

    data_status = payload.get("us_market_data_status") or {}
    if data_status.get("stock_data_current") is False and (us_r > 0.000001 or us_action_count > 0):
        failures.append(
            ValidationFailure(
                "us_stale_stock_data_with_execution_r",
                f"stock_data_current=false latest={data_status.get('latest_date') or '-'} "
                f"as_of={data_status.get('as_of') or payload.get('as_of') or '-'} "
                f"but us_r={us_r:.4f}, us_action_count={us_action_count}",
            )
        )
    return failures


def validate_us_size_and_horizon(payload: dict[str, Any], us_report: str) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    for row in market_actionables(payload, "US"):
        size_r = as_float(row.get("size_r"))
        symbol = str(row.get("symbol") or "-").upper()
        if size_r > US_SINGLE_NAME_R_CAP + 0.000001:
            failures.append(
                ValidationFailure(
                    "us_single_name_r_above_cap",
                    f"{symbol} size_r={size_r:.4f} exceeds {US_SINGLE_NAME_R_CAP:.3f}R cap",
                )
            )
    for phrase in OLD_US_HOLD_PHRASES:
        if phrase in us_report:
            failures.append(
                ValidationFailure("old_us_hold_horizon_phrase", f"us_daily_report.md contains `{phrase}`")
            )
    return failures


def validate_probability_picks(payload: dict[str, Any], cn_report: str, us_report: str) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    cn_actionable = actionable_symbols(payload, "CN")
    us_actionable = actionable_symbols(payload, "US")
    cn_picks = {
        match.group(1).upper()
        for match in re.finditer(r"###\s+🥇\s+个股\s+→\s+\*\*([0-9]{6}\.(?:SZ|SH))\b", cn_report)
    }
    us_picks = {
        match.group(1).upper()
        for match in re.finditer(r"###\s+🥇\s+股票\s+→\s+\*\*([A-Z][A-Z0-9.\-]*)\*\*", us_report)
    }
    for symbol in sorted(cn_picks - cn_actionable):
        failures.append(
            ValidationFailure(
                "cn_probability_pick_not_actionable",
                f"{symbol} appears in CN probability pick but is not production actionable",
            )
        )
    for symbol in sorted(us_picks - us_actionable):
        failures.append(
            ValidationFailure(
                "us_probability_pick_not_actionable",
                f"{symbol} appears in US probability pick but is not production actionable",
            )
        )
    return failures


def validate_cn_daily_scope(cn_report: str) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    for token in CN_NON_AI_EXECUTION_POLLUTION:
        if token in cn_report:
            failures.append(
                ValidationFailure("cn_non_ai_daily_pollution", f"cn_daily_report.md contains `{token}`")
            )
    return failures


def validate_horizon_edge(
    payload: dict[str, Any],
    cn_report: str,
    us_report: str,
    allow_missing: bool,
) -> list[ValidationFailure]:
    if allow_missing:
        return []
    failures: list[ValidationFailure] = []
    if not payload.get("report_action_backtest_summary"):
        failures.append(
            ValidationFailure(
                "missing_report_action_backtest_summary",
                "main_strategy_v2_backtest.json has no report_action_backtest_summary",
            )
        )
    if "CN Realized Horizon Edge" not in cn_report:
        failures.append(
            ValidationFailure("missing_cn_realized_horizon_edge", "cn_daily_report.md lacks CN Realized Horizon Edge")
        )
    if "US Realized Horizon Edge" not in us_report:
        failures.append(
            ValidationFailure("missing_us_realized_horizon_edge", "us_daily_report.md lacks US Realized Horizon Edge")
        )
    return failures


def validate_report_dir(report_dir: Path, allow_missing_horizon_edge: bool = False) -> list[ValidationFailure]:
    payload = load_json(report_dir / "main_strategy_v2_backtest.json")
    cn_report = read_text(report_dir / "cn_daily_report.md")
    us_report = read_text(report_dir / "us_daily_report.md")
    failures: list[ValidationFailure] = []
    failures.extend(validate_forbidden_daily_copy({
        "cn_daily_report.md": cn_report,
        "us_daily_report.md": us_report,
    }))
    failures.extend(validate_us_gate(payload))
    failures.extend(validate_us_size_and_horizon(payload, us_report))
    failures.extend(validate_probability_picks(payload, cn_report, us_report))
    failures.extend(validate_cn_daily_scope(cn_report))
    failures.extend(validate_horizon_edge(payload, cn_report, us_report, allow_missing_horizon_edge))
    return failures


def main() -> None:
    args = parse_args()
    report_dir = args.report_root / args.date
    failures = validate_report_dir(report_dir, allow_missing_horizon_edge=args.allow_missing_horizon_edge)
    if failures:
        print(f"Main Strategy V2 report validation failed for {report_dir}:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure.code}: {failure.detail}", file=sys.stderr)
        raise SystemExit(1)
    print(f"Main Strategy V2 report validation passed: {report_dir}")


if __name__ == "__main__":
    main()
