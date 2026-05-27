"""Format / parse helpers — Phase A.1 of REFACTOR_PLAN.md.

Extracted from scripts/generate_main_strategy_v2_report.py — behavior
preserved bit-for-bit. All pure functions, no DB access.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime
from typing import Any


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)[:10]


def round_or_none(value: Any, digits: int = 6) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def fmt_pct(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:+.{digits}f}%"


def fmt_num(value: Any, digits: int = 2) -> str:
    parsed = round_or_none(value, digits)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def fmt_bool(value: bool) -> str:
    return "yes" if value else "no"


def safe_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def fmt_r(value: Any) -> str:
    parsed = round_or_none(value, 4)
    if parsed is None:
        return "-"
    text = f"{parsed:.4f}".rstrip("0").rstrip(".")
    return f"{text}R"


def clean_table_text(value: Any, limit: int = 120) -> str:
    text = str(value or "-").replace("\n", " ").replace("|", "/").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def fmt_rate_pct(value: Any) -> str:
    parsed = round_or_none(value)
    if parsed is None:
        return "-"
    if abs(parsed) <= 1.5:
        parsed *= 100.0
    return f"{parsed:+.2f}%"


def symbol_key(value: Any) -> str:
    """Normalize a ticker symbol (uppercase, stripped). Was _symbol_key in main."""
    return str(value or "").upper().strip()


def report_safe_options_context(value: Any, limit: int = 120) -> str:
    """Strip option-pitching language (打法/指引/追入) from options context text."""
    text = str(value or "-").replace("\n", " ").replace("|", "/").strip()
    replacements = {
        "打法:": "",
        "打法：": "",
        "指引：": "",
        "指引:": "",
        "加仓": "提高关注",
        "小仓": "小样本关注",
        "追入": "追高",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


ACTION_LABELS = {
    "buy_planned_entry": "计划买入",
    "buy_pullback_or_intraday_confirmation": "回踩/盘中确认再买",
    "buy_only_if_intraday_relative_strength_confirms": "只在盘中相对强度确认后买",
    "buy_planned_entry_observed": "左侧计划买入",
    "buy_pullback_observed": "左侧回踩买入",
    "buy_stock_with_options_confirmation": "股票买入，期权/flow 确认",
    "buy_stock_position": "股票买入",
    "prepare_order_but_wait_for_price": "准备观察，等价格确认",
    "rank_only_no_new_trade": "只观察，不开新仓",
}


def action_label(value: Any) -> str:
    text = str(value or "-")
    return ACTION_LABELS.get(text, text)


def display_tenor_name(value: Any) -> str:
    """Map raw tenor key (weekly/monthly/...) to display label (LEAPS for long_dated)."""
    mapping = {
        "weekly": "weekly",
        "biweekly": "biweekly",
        "monthly": "monthly",
        "quarterly": "quarterly",
        "half_year": "half-year",
        "leaps": "LEAPS",
        "long_dated": "LEAPS",
    }
    text = str(value or "").strip()
    return mapping.get(text.lower(), text or "-")
