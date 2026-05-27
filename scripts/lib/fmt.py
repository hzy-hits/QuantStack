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
