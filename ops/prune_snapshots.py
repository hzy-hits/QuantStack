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


def human(n: int) -> str:
    f = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if f < 1024 or u == "TB":
            return f"{f:.1f}{u}"
        f /= 1024
    return f"{f:.1f}TB"


def prune_dir(
    data_dir: Path, today: datetime.date, keep_days: int, apply: bool
) -> tuple[list[Path], int]:
    names = [p.name for p in data_dir.iterdir() if p.is_file()]
    _, to_delete = classify_snapshots(names, today=today, keep_days=keep_days)
    paths = [data_dir / n for n in sorted(to_delete)]
    total = sum(p.stat().st_size for p in paths)
    if apply:
        for p in paths:
            p.unlink()
    return paths, total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-days", type=int, default=7)
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run)")
    ap.add_argument("--data-dir", default=None)
    args = ap.parse_args()

    root = Path(os.environ.get("QUANT_STACK_ROOT", Path(__file__).resolve().parents[1]))
    data_dir = Path(args.data_dir) if args.data_dir else root / "quant-research-v1" / "data"
    today = datetime.datetime.now().date()  # CLI only; core logic stays pure

    paths, total = prune_dir(data_dir, today=today, keep_days=args.keep_days, apply=args.apply)
    mode = "DELETED" if args.apply else "would delete (dry-run)"
    print(f"snapshot prune ({data_dir}) keep_days={args.keep_days}: {mode} {len(paths)} files, {human(total)}")
    for p in paths:
        print(f"  {p.name}")
    if not args.apply and paths:
        print("re-run with --apply to delete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
