#!/usr/bin/env python3
"""Prune accumulated per-session DuckDB snapshots to a retention window.

SAFE: only deletes files matching quant_{research,report}_YYYY-MM-DD_{pre,post}.duckdb
under quant-research-v1/data/. Canonical DBs never match. Dry-run by default.
"""
from __future__ import annotations

import argparse
import datetime
import os
import re
from pathlib import Path

SNAPSHOT_RE = re.compile(
    r"^quant_(?:research|report)_(\d{4})-(\d{2})-(\d{2})_(?:pre|post)\.duckdb$"
)


def classify_snapshots(
    names: list[str], today: datetime.date, keep_days: int
) -> tuple[list[str], list[str]]:
    cutoff = today - datetime.timedelta(days=keep_days)
    keep: list[str] = []
    delete: list[str] = []
    for name in names:
        m = SNAPSHOT_RE.match(name)
        if not m:
            continue
        d = datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        (keep if d >= cutoff else delete).append(name)
    return keep, delete
