"""Shared constants and helpers for the filtering subpackage."""
from __future__ import annotations

import json
import math
from typing import Any

import numpy as np

MAX_ITEMS = 30
MIN_ITEMS = 8

# Composite score weights (sum to 1.0)
W_MAGNITUDE = 0.15  # big move today vs ATR
W_EVENT     = 0.18  # earnings / 8-K / FOMC
W_MOMENTUM  = 0.10  # trend strength
W_OPTIONS   = 0.12  # options pricing
W_CROSS     = 0.10  # idiosyncratic vs broad move
W_LAB       = 0.35  # Factor Lab rolling best-factor signal


def _clamp01(x: float) -> float:
    return max(0.0, min(float(x), 1.0))


def _weighted_average_available(items: list[tuple[float, float | None]]) -> float | None:
    """Weighted average over non-None items, renormalized."""
    usable = [(w, v) for w, v in items if v is not None]
    if not usable:
        return None
    denom = sum(w for w, _ in usable)
    return sum(w * v for w, v in usable) / denom if denom else None


def _parse_unusual(raw) -> list[dict]:
    """Parse unusual_strikes from JSON string or passthrough list."""
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, json.JSONDecodeError):
        return []


def _safe(v, default=None):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return default
    return v
