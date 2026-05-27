"""DuckDB connection + small SQL helpers shared by the main report and
extracted section modules.

Extracted from scripts/generate_main_strategy_v2_report.py (Phase A.0 of
REFACTOR_PLAN.md). Behavior preserved bit-for-bit — main script imports
these instead of re-defining them.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import duckdb


def _connect_ro(db_path, retries: int = 8, backoff: float = 20.0):
    """Read-only DuckDB connect with retry on a transient writer lock.

    The CN report reads quant_cn_report.duckdb while quant-cn's
    review-backfill may briefly hold a write lock (DuckDB is single-writer
    and a writer blocks cross-process readers). Retry rather than crash the
    whole report; a pathological long lock still fails and retries next cron.
    """
    for attempt in range(retries):
        try:
            return duckdb.connect(str(db_path), read_only=True)
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or attempt == retries - 1:
                raise
            time.sleep(backoff)
    raise RuntimeError(f"unreachable: _connect_ro({db_path})")


def placeholders(values: list[Any]) -> str:
    return ",".join("?" for _ in values)


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]
