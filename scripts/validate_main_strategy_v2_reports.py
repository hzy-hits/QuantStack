#!/usr/bin/env python3
"""Validate Main Strategy V2 daily reports against production contracts."""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import date
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
PRODUCTION_FORBIDDEN_EVIDENCE_MARKERS = (
    "不可进生产池",
    "不生成 R",
    "不生成R",
    "AI 敞口待原文核验",
    "待原文核验",
    "原文需核验",
    "pending_original_source_verification",
    "证据不足",
)
MARKET_CONTEXT_SYMBOLS = {
    "SPY", "QQQ", "SMH", "DIA", "IWM",
    "TLT", "IEF", "SHY",
    "HYG", "LQD", "JNK",
    "SOXX", "XLK", "VGT", "AIQ", "SOXQ", "KWEB",
}


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


def read_text_optional(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def as_optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def market_actionables(payload: dict[str, Any], market: str) -> list[dict[str, Any]]:
    wanted = market.upper()
    rows = ((payload.get("production_decision_summary") or {}).get("actionable") or [])
    return [row for row in rows if str(row.get("market") or "").upper() == wanted]


def parse_ymd(value: Any) -> date | None:
    text = as_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _num_text_variants(value: Any) -> set[str]:
    text = as_text(value)
    out = {text} if text else set()
    parsed = as_optional_float(value)
    if parsed is not None:
        out.add(f"{parsed:.0f}")
        out.add(f"{parsed:.1f}")
        out.add(f"{parsed:.2f}")
    return {item for item in out if item}


def _contains_any(text: str, variants: set[str]) -> bool:
    return any(variant in text for variant in variants)


def us_lineage_values(payload: dict[str, Any]) -> dict[str, Any]:
    status = payload.get("us_market_data_status") or {}
    us = payload.get("us") or {}
    gamma = payload.get("gamma_spring") or {}
    fg = payload.get("fear_greed") or {}
    report_date = as_text(payload.get("as_of")) or "-"
    latest_stock = (
        as_text(status.get("effective_us_market_date"))
        or as_text(status.get("prices_daily_latest_date"))
        or as_text(status.get("latest_date"))
        or as_text(us.get("current_date"))
        or "-"
    )
    candidate_date = as_text(us.get("current_date")) or latest_stock
    gamma_date = (
        as_text(gamma.get("effective_date"))
        or as_text(status.get("options_chain_latest_as_of"))
        or "-"
    )
    state = as_text(status.get("state")) or "-"
    is_previous_session = bool(status.get("is_previous_session")) or state == "previous_session"
    return {
        "report_date": report_date,
        "latest_stock": latest_stock,
        "candidate_date": candidate_date,
        "gamma_date": gamma_date,
        "state": state,
        "is_previous_session": is_previous_session,
        "status": status,
        "gamma": gamma,
        "fear_greed": fg,
    }


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
    summary = ((payload.get("production_decision_summary") or {}).get("summary") or {})
    execution_gate = summary.get("us_execution_gate")
    if (
        isinstance(execution_gate, dict)
        and execution_gate.get("allowed") is False
        and (us_r > 0.000001 or us_action_count > 0)
    ):
        failures.append(
            ValidationFailure(
                "us_hard_gate_failed_with_execution_r",
                f"top_blocker={execution_gate.get('top_blocker') or '-'} "
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


def validate_us_payload_lineage(payload: dict[str, Any]) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    values = us_lineage_values(payload)
    status = values["status"]
    gamma = values["gamma"]
    fg = values["fear_greed"]
    report_date = values["report_date"]
    latest_stock = values["latest_stock"]
    candidate_date = values["candidate_date"]
    gamma_date = values["gamma_date"]

    status_as_of = as_text(status.get("as_of"))
    if status_as_of and status_as_of != report_date:
        failures.append(
            ValidationFailure(
                "us_market_status_as_of_mismatch",
                f"us_market_data_status.as_of={status_as_of} but payload.as_of={report_date}",
            )
        )
    prices_latest = as_text(status.get("prices_daily_latest_date"))
    effective_latest = as_text(status.get("effective_us_market_date"))
    if prices_latest and effective_latest and prices_latest != effective_latest:
        failures.append(
            ValidationFailure(
                "us_effective_date_mismatch",
                f"effective_us_market_date={effective_latest} but prices_daily_latest_date={prices_latest}",
            )
        )
    if candidate_date != "-" and latest_stock != "-" and candidate_date != latest_stock:
        failures.append(
            ValidationFailure(
                "us_candidate_date_mismatch",
                f"us.current_date={candidate_date} but effective stock date={latest_stock}",
            )
        )
    chain_latest = as_text(status.get("options_chain_latest_as_of"))
    gamma_effective = as_text(gamma.get("effective_date"))
    if chain_latest and gamma_effective and chain_latest != gamma_effective:
        failures.append(
            ValidationFailure(
                "us_gamma_chain_date_mismatch",
                f"gamma_spring.effective_date={gamma_effective} but options_chain_latest_as_of={chain_latest}",
            )
        )
    report_dt = parse_ymd(report_date)
    for field in (
        "prices_daily_latest_date",
        "effective_us_market_date",
        "options_analysis_latest_as_of",
        "options_chain_latest_as_of",
        "options_sentiment_latest_as_of",
        "market_quotes_latest_as_of",
    ):
        value = as_text(status.get(field))
        value_dt = parse_ymd(value)
        if report_dt and value_dt and value_dt > report_dt:
            failures.append(
                ValidationFailure(
                    "us_lineage_date_after_report_date",
                    f"us_market_data_status.{field}={value} is after report as_of={report_date}",
                )
            )
    gamma_dt = parse_ymd(gamma_date)
    if report_dt and gamma_dt and gamma_dt > report_dt:
        failures.append(
            ValidationFailure(
                "us_gamma_date_after_report_date",
                f"gamma effective date={gamma_date} is after report as_of={report_date}",
            )
        )
    latest_dt = parse_ymd(latest_stock)
    if values["is_previous_session"]:
        if values["state"] != "previous_session":
            failures.append(
                ValidationFailure(
                    "us_previous_session_state_mismatch",
                    f"is_previous_session=true but state={values['state']}",
                )
            )
        if report_dt and latest_dt and latest_dt >= report_dt:
            failures.append(
                ValidationFailure(
                    "us_previous_session_date_not_previous",
                    f"is_previous_session=true but effective date={latest_stock} and as_of={report_date}",
                )
            )

    fg_score = as_optional_float(fg.get("score"))
    regime_fg = as_optional_float(((payload.get("risk_regime") or {}).get("signals") or {}).get("fear_greed_score"))
    bubble_fg = as_optional_float(
        ((payload.get("bubble_hedge") or {}).get("confirmation") or {}).get("fear_greed_score")
    )
    if fg_score is not None and regime_fg is not None and abs(fg_score - regime_fg) > 0.05:
        failures.append(
            ValidationFailure(
                "risk_regime_fear_greed_mismatch",
                f"fear_greed.score={fg_score:.2f} but risk_regime.signals.fear_greed_score={regime_fg:.2f}",
            )
        )
    if fg_score is not None and bubble_fg is not None and abs(fg_score - bubble_fg) > 0.05:
        failures.append(
            ValidationFailure(
                "bubble_hedge_fear_greed_mismatch",
                f"fear_greed.score={fg_score:.2f} but bubble_hedge.confirmation.fear_greed_score={bubble_fg:.2f}",
            )
        )
    report_dates = payload.get("report_dates") or {}
    if not isinstance(report_dates, dict) or not report_dates:
        failures.append(
            ValidationFailure("missing_report_dates_lineage", "main_strategy_v2_backtest.json has no report_dates")
        )
    else:
        expected = {
            "report_label_date": report_date,
            "effective_us_market_date": latest_stock,
            "prices_effective_date": as_text(status.get("prices_daily_latest_date")) or "-",
            "report_decisions_effective_date": candidate_date,
            "gamma_effective_date": gamma_date,
            "fear_greed_source": as_text(fg.get("source")) or "-",
        }
        for key, value in expected.items():
            actual = as_text(report_dates.get(key)) or "-"
            if value != "-" and actual != value:
                failures.append(
                    ValidationFailure(
                        "report_dates_lineage_mismatch",
                        f"report_dates.{key}={actual} but expected {value}",
                    )
                )
    return failures


def validate_us_ranker_lineage(payload: dict[str, Any]) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    values = us_lineage_values(payload)
    report_date = values["report_date"]
    latest_stock = values["latest_stock"]
    rows = ((payload.get("us_opportunity_ranker") or {}).get("all_rows") or [])
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        if symbol in MARKET_CONTEXT_SYMBOLS and row.get("ai_infra_universe"):
            failures.append(
                ValidationFailure(
                    "market_context_symbol_in_ai_universe_ranker",
                    f"{symbol} is ETF/benchmark/hedge context but appears as AI universe ranker row",
                )
            )
        price_as_of = as_text(row.get("price_as_of"))
        if row.get("close") is not None or row.get("ret_5d_pct") is not None or row.get("ret_20d_pct") is not None:
            if not price_as_of:
                failures.append(
                    ValidationFailure("us_ranker_missing_price_as_of", f"{symbol} has price features without price_as_of")
                )
            elif price_as_of != latest_stock:
                fallback_disclosed = bool(row.get("price_fallback_used"))
                trade_tier = "trade" in str(row.get("production_tier") or "").lower()
                if not fallback_disclosed or trade_tier:
                    failures.append(
                        ValidationFailure(
                            "us_ranker_price_as_of_mismatch",
                            f"{symbol} price_as_of={price_as_of} but effective_us_market_date={latest_stock}; "
                            f"fallback_disclosed={fallback_disclosed} production_tier={row.get('production_tier') or '-'}",
                        )
                    )
        score_components = row.get("score_components") or {}
        if isinstance(score_components, dict) and score_components.get("broad_signal") is not None:
            analysis_date = as_text(row.get("analysis_signal_date"))
            if not analysis_date:
                failures.append(
                    ValidationFailure(
                        "us_ranker_missing_analysis_signal_date",
                        f"{symbol} has broad_signal but no analysis_signal_date",
                    )
                )
            elif parse_ymd(analysis_date) and parse_ymd(report_date) and parse_ymd(analysis_date) > parse_ymd(report_date):
                failures.append(
                    ValidationFailure(
                        "us_ranker_analysis_date_after_report_date",
                        f"{symbol} analysis_signal_date={analysis_date} after report as_of={report_date}",
                    )
                )
        option_reason = str(row.get("options_quality_reason") or "")
        if option_reason and "missing_options_alpha" not in option_reason:
            option_date = as_text(row.get("options_alpha_as_of"))
            if not option_date:
                failures.append(
                    ValidationFailure(
                        "us_ranker_missing_options_alpha_as_of",
                        f"{symbol} has options_alpha quality without options_alpha_as_of",
                    )
                )
            elif parse_ymd(option_date) and parse_ymd(report_date) and parse_ymd(option_date) > parse_ymd(report_date):
                failures.append(
                    ValidationFailure(
                        "us_ranker_options_alpha_after_report_date",
                        f"{symbol} options_alpha_as_of={option_date} after report as_of={report_date}",
                    )
                )
    return failures


def validate_options_artifact_lineage(payload: dict[str, Any]) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    as_of = as_text(payload.get("as_of")) or "-"
    status = payload.get("us_market_data_status") or {}
    expected_options_date = as_text(status.get("options_chain_latest_as_of")) or as_text(status.get("options_sentiment_latest_as_of"))
    for payload_key, label in (
        ("options_anomaly_rows", "options_anomaly"),
        ("options_tenor_signals", "options_tenor"),
    ):
        rows = payload.get(payload_key) or []
        if not rows:
            continue
        for idx, row in enumerate(rows[:20]):
            requested = as_text(row.get("requested_date"))
            source_date = as_text(row.get("source_date"))
            if requested != as_of:
                failures.append(
                    ValidationFailure(
                        f"{label}_requested_date_mismatch",
                        f"{payload_key}[{idx}].requested_date={requested or '-'} but payload.as_of={as_of}",
                    )
                )
            if not source_date:
                failures.append(
                    ValidationFailure(f"{label}_missing_source_date", f"{payload_key}[{idx}] has no source_date")
                )
            elif expected_options_date and source_date != expected_options_date:
                failures.append(
                    ValidationFailure(
                        f"{label}_source_date_mismatch",
                        f"{payload_key}[{idx}].source_date={source_date} but options effective date={expected_options_date}",
                    )
                )
            if "fallback_used" not in row:
                failures.append(
                    ValidationFailure(f"{label}_missing_fallback_used", f"{payload_key}[{idx}] has no fallback_used")
                )
            if not as_text(row.get("source_path")):
                failures.append(
                    ValidationFailure(f"{label}_missing_source_path", f"{payload_key}[{idx}] has no source_path")
                )
    verdicts = payload.get("options_verdicts") or {}
    for symbol, row in list(verdicts.items())[:50]:
        if not isinstance(row, dict):
            continue
        eff = as_text(row.get("effective_date"))
        if not eff:
            failures.append(
                ValidationFailure("options_verdict_missing_effective_date", f"{symbol} verdict has no effective_date")
            )
        elif expected_options_date and eff != expected_options_date:
            failures.append(
                ValidationFailure(
                    "options_verdict_effective_date_mismatch",
                    f"{symbol} verdict effective_date={eff} but options effective date={expected_options_date}",
                )
            )
    return failures


def validate_us_report_text_against_payload(
    payload: dict[str, Any],
    text: str,
    label: str = "us_report",
) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    values = us_lineage_values(payload)
    fg = values["fear_greed"]
    required_pairs = (
        ("报告标签日期", values["report_date"]),
        ("US 收盘价数据截至", values["latest_stock"]),
        ("US 候选/执行数据日期", values["candidate_date"]),
        ("Gamma 有效日", values["gamma_date"]),
    )
    for marker, expected in required_pairs:
        if expected and expected != "-" and (marker not in text or expected not in text):
            failures.append(
                ValidationFailure(
                    "us_report_data_calibration_missing",
                    f"{label} does not preserve `{marker}`={expected}",
                )
            )
    if values["is_previous_session"] and "不是当日美股已收盘数据" not in text:
        failures.append(
            ValidationFailure(
                "us_report_missing_previous_session_caveat",
                f"{label} must explicitly say `不是当日美股已收盘数据`",
            )
        )
    report_date = values["report_date"]
    latest_stock = values["latest_stock"]
    if values["is_previous_session"] and report_date != latest_stock and f"数据日: {report_date}" in text:
        failures.append(
            ValidationFailure(
                "us_report_uses_label_date_as_data_date",
                f"{label} contains `数据日: {report_date}` while US effective date is {latest_stock}",
            )
        )
    fg_source = (as_text(fg.get("source")) or "").lower()
    fg_score = fg.get("score")
    if fg_source:
        if "Fear" not in text and "Greed" not in text:
            failures.append(
                ValidationFailure("us_report_missing_fear_greed", f"{label} omits Fear/Greed source")
            )
        if fg_source == "cnn" and "CNN" not in text and "cnn" not in text:
            failures.append(
                ValidationFailure("us_report_missing_cnn_fear_greed_source", f"{label} omits CNN source")
            )
        if fg_source == "proxy":
            if "Internal Fear/Greed proxy" not in text:
                failures.append(
                    ValidationFailure(
                        "us_report_missing_proxy_fear_greed_source",
                        f"{label} must say Internal Fear/Greed proxy when source=proxy",
                    )
                )
            if "CNN Fear" in text or "CNN F&G" in text:
                failures.append(
                    ValidationFailure(
                        "us_report_proxy_mislabeled_as_cnn",
                        f"{label} labels proxy Fear/Greed as CNN",
                    )
                )
    if fg_score is not None and not _contains_any(text, _num_text_variants(fg_score)):
        failures.append(
            ValidationFailure(
                "us_report_missing_fear_greed_score",
                f"{label} omits Fear/Greed score={fg_score}",
            )
        )
    return failures


def validate_production_actionable_contract(payload: dict[str, Any]) -> list[ValidationFailure]:
    failures: list[ValidationFailure] = []
    for row in ((payload.get("production_decision_summary") or {}).get("actionable") or []):
        size_r = as_float(row.get("size_r"))
        tier = str(row.get("tier") or row.get("production_tier") or "")
        if size_r <= 0 and "trade" not in tier:
            continue
        symbol = str(row.get("symbol") or "<unknown>").upper()
        text = " ".join(str(value or "") for value in row.values())
        found = [marker for marker in PRODUCTION_FORBIDDEN_EVIDENCE_MARKERS if marker in text]
        if found:
            failures.append(
                ValidationFailure(
                    "production_actionable_forbidden_evidence_marker",
                    f"{symbol} actionable row contains forbidden marker(s): {', '.join(found)}",
                )
            )
        evidence = as_text(row.get("ai_infra_evidence_state") or row.get("evidence_state"))
        if not evidence:
            failures.append(
                ValidationFailure(
                    "production_actionable_missing_evidence_state",
                    f"{symbol} actionable row has no evidence_state / ai_infra_evidence_state",
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
    as_of = as_text(payload.get("as_of")) or report_dir.name
    optional_us_reports: dict[str, str] = {}
    agent_us = read_text_optional(report_dir / "us_daily_report_agent.md")
    if agent_us is not None:
        optional_us_reports["us_daily_report_agent.md"] = agent_us
    try:
        report_dir.relative_to(DEFAULT_REPORT_ROOT)
        include_delivery_report = True
    except ValueError:
        include_delivery_report = False
    if include_delivery_report:
        legacy_us_path = STACK_ROOT / "quant-research-v1" / "reports" / f"{as_of}_report_zh_post.md"
        legacy_us = read_text_optional(legacy_us_path)
        if legacy_us is not None:
            optional_us_reports[str(legacy_us_path.relative_to(STACK_ROOT))] = legacy_us
    failures: list[ValidationFailure] = []
    failures.extend(validate_forbidden_daily_copy({
        "cn_daily_report.md": cn_report,
        "us_daily_report.md": us_report,
        **optional_us_reports,
    }))
    failures.extend(validate_us_gate(payload))
    failures.extend(validate_us_payload_lineage(payload))
    failures.extend(validate_us_ranker_lineage(payload))
    failures.extend(validate_options_artifact_lineage(payload))
    failures.extend(validate_us_report_text_against_payload(payload, us_report, "us_daily_report.md"))
    for label, text in optional_us_reports.items():
        failures.extend(validate_us_report_text_against_payload(payload, text, label))
    failures.extend(validate_production_actionable_contract(payload))
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
