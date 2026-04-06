"""Shared formatting helpers used by all render sub-modules."""
from __future__ import annotations


def _fmt_pct(v, dp=1) -> str:
    if v is None:
        return "\u2014"
    return f"{v:+.{dp}f}%"


def _fmt_val(v, dp=2) -> str:
    if v is None:
        return "\u2014"
    return f"{v:.{dp}f}"


def _fmt_p(v) -> str:
    if v is None:
        return "\u2014"
    return f"{v:.4f}"


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"
