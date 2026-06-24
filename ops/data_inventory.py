#!/usr/bin/env python3
"""
Read-only data inventory for the portability refactor (spec 阶段 0, step 1).

Classifies every *.duckdb under the stack root into:
  - canonical  : live hot DBs that must migrate (compacted)
  - snapshot   : dated session copies (quant_*_YYYY-MM-DD_*.duckdb) -> prune/cold-archive
  - drop       : factor_lab.duckdb (decision A: archive small tables, then drop)
  - other      : anything unclassified (review manually)

SAFE: filesystem stat only by default (no DB opens, no writes). Pass --rows to
additionally open each DB read-only and count tables/rows (skips on lock).

Usage:
  python3 ops/data_inventory.py
  python3 ops/data_inventory.py --rows          # also count rows (may hit write locks)
  QUANT_STACK_ROOT=/path python3 ops/data_inventory.py
"""
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

CANONICAL = {
    "quant-research-v1/data/quant.duckdb",
    "quant-research-v1/data/quant_report.duckdb",
    "quant-research-cn/data/quant_cn.duckdb",
    "quant-research-cn/data/quant_cn_report.duckdb",
    "quant-research-cn/data/quant_cn_research.duckdb",
    "data/strategy_backtest_history.duckdb",
}
DROP = {"factor-lab/data/factor_lab.duckdb"}
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def human(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if f < 1024 or unit == "TB":
            return f"{f:.1f}{unit}"
        f /= 1024
    return f"{f:.1f}TB"


def classify(rel: str) -> str:
    if rel in DROP:
        return "drop"
    if rel in CANONICAL:
        return "canonical"
    name = Path(rel).name
    if DATE_RE.search(name):
        return "snapshot"
    return "other"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", action="store_true", help="open each DB read-only and count tables/rows")
    args = ap.parse_args()

    root = Path(os.environ.get("QUANT_STACK_ROOT", Path(__file__).resolve().parents[1]))
    dbs = sorted(p for p in root.rglob("*.duckdb") if ".git" not in p.parts)

    rows = []
    for p in dbs:
        rel = str(p.relative_to(root))
        size = p.stat().st_size
        rows.append((classify(rel), rel, size))

    buckets: dict[str, list] = {"canonical": [], "snapshot": [], "drop": [], "other": []}
    for cat, rel, size in rows:
        buckets[cat].append((rel, size))

    print(f"# Data inventory — root: {root}\n")
    for cat in ("canonical", "drop", "snapshot", "other"):
        items = sorted(buckets[cat], key=lambda x: -x[1])
        total = sum(s for _, s in items)
        print(f"## {cat}  ({len(items)} files, {human(total)})")
        for rel, size in items:
            print(f"  {human(size):>9}  {rel}")
        print()

    canon = sum(s for _, s in buckets["canonical"])
    snap = sum(s for _, s in buckets["snapshot"])
    drop = sum(s for _, s in buckets["drop"])
    other = sum(s for _, s in buckets["other"])
    grand = canon + snap + drop + other
    print("## Summary")
    print(f"  canonical (migrate, compact): {human(canon)}")
    print(f"  drop (factor-lab, archive small tables then delete): {human(drop)}")
    print(f"  snapshot (prune / cold-archive to NAS): {human(snap)}")
    print(f"  other (REVIEW): {human(other)}")
    print(f"  --- grand total: {human(grand)} ---")
    print(f"  est. hot footprint to migrate (canonical only, pre-compaction): {human(canon)}")

    if args.rows:
        print("\n## row counts (read-only; skips on lock)")
        try:
            import duckdb  # noqa
        except ImportError:
            print("  duckdb not importable; skipping")
            return 0
        import duckdb
        for _, rel, _ in rows:
            p = root / rel
            try:
                c = duckdb.connect(str(p), read_only=True)
                tabs = [r[0] for r in c.execute("show tables").fetchall()]
                total_rows = 0
                for t in tabs:
                    try:
                        total_rows += c.execute(f'select count(*) from "{t}"').fetchone()[0]
                    except Exception:
                        pass
                c.close()
                print(f"  {rel}: {len(tabs)} tables, ~{total_rows} rows")
            except Exception as e:
                print(f"  {rel}: SKIP ({str(e)[:60]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
