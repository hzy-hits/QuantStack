#!/usr/bin/env python3
"""
Compact canonical DuckDB files by rewriting them with only live data
(reclaims dead space; DuckDB never shrinks a file in place).

Method: COPY FROM DATABASE src -> fresh dst, verify table set + per-table row
counts match, then atomically replace. Original moved to a backup dir (same
filesystem -> instant rename), never deleted by this script.

SAFETY: requires NO active writer (DuckDB single-writer). Stop cron first.

Usage:
  python3 ops/compact_db.py                 # compact the canonical set
  python3 ops/compact_db.py path/to.duckdb  # compact specific file(s)
  BACKUP_DIR=/home/ivena/migration_backups/orig python3 ops/compact_db.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(os.environ.get("QUANT_STACK_ROOT", Path(__file__).resolve().parents[1]))
BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", "/home/ivena/migration_backups/orig"))

CANONICAL = [
    "quant-research-v1/data/quant.duckdb",
    "quant-research-v1/data/quant_report.duckdb",
    "quant-research-cn/data/quant_cn.duckdb",
    "quant-research-cn/data/quant_cn_report.duckdb",
    "quant-research-cn/data/quant_cn_research.duckdb",
    "data/strategy_backtest_history.duckdb",
]


def human(n: int) -> str:
    f = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if f < 1024 or u == "TB":
            return f"{f:.1f}{u}"
        f /= 1024
    return f"{f:.1f}TB"


def table_counts(db: Path) -> dict[str, int]:
    c = duckdb.connect(str(db), read_only=True)
    try:
        tabs = [r[0] for r in c.execute(
            "select table_name from information_schema.tables where table_schema='main'"
        ).fetchall()]
        out = {}
        for t in tabs:
            out[t] = c.execute(f'select count(*) from "{t}"').fetchone()[0]
        return out
    finally:
        c.close()


def compact(rel_or_abs: str) -> bool:
    src = Path(rel_or_abs)
    if not src.is_absolute():
        src = ROOT / src
    if not src.exists():
        print(f"  SKIP (missing): {src}")
        return True
    before = src.stat().st_size
    print(f"\n# {src.relative_to(ROOT) if str(src).startswith(str(ROOT)) else src}  ({human(before)})")

    src_counts = table_counts(src)
    print(f"  tables: {len(src_counts)}, total rows: {sum(src_counts.values())}")

    dst = src.with_suffix(".duckdb.compact")
    if dst.exists():
        dst.unlink()

    con = duckdb.connect()  # in-memory control connection
    try:
        con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
        con.execute(f"ATTACH '{dst}' AS dst")
        con.execute("COPY FROM DATABASE src TO dst")
        con.execute("DETACH src")
        con.execute("DETACH dst")
    finally:
        con.close()

    dst_counts = table_counts(dst)
    if dst_counts != src_counts:
        print("  !! VERIFY FAILED — table/row mismatch; leaving original untouched")
        only_src = {k: src_counts[k] for k in src_counts if dst_counts.get(k) != src_counts[k]}
        print(f"     mismatches: {only_src}")
        dst.unlink(missing_ok=True)
        return False

    after = dst.stat().st_size
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    bak = BACKUP_DIR / (src.name + ".orig")
    os.replace(src, bak)          # move original aside (same fs)
    os.replace(dst, src)          # promote compacted file
    print(f"  OK  {human(before)} -> {human(after)}  (saved {human(before - after)});"
          f" original -> {bak}")
    return True


def main() -> int:
    targets = sys.argv[1:] or CANONICAL
    print(f"root: {ROOT}\nbackup dir (originals): {BACKUP_DIR}")
    ok = True
    total_before = total_after = 0
    for t in targets:
        p = (ROOT / t) if not Path(t).is_absolute() else Path(t)
        b = p.stat().st_size if p.exists() else 0
        res = compact(t)
        ok = ok and res
        a = p.stat().st_size if p.exists() else 0
        total_before += b
        total_after += a
    print(f"\n=== total: {human(total_before)} -> {human(total_after)} "
          f"(saved {human(total_before - total_after)}) ===")
    print("ALL_OK" if ok else "SOME_FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
