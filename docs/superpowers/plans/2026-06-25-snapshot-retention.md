# Snapshot Retention & Pruning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim the ~44GB of accumulated per-session DuckDB snapshots under `quant-research-v1/data/` by adding a tested retention/prune tool and a scheduled cron task that keeps only a recent window.

**Architecture:** A pure classification function (`classify_snapshots`) decides keep-vs-delete from filenames + a retention window + an injected `today` (no wall-clock in core logic, so it's testable). A thin CLI (`ops/prune_snapshots.py`) wraps it with `--dry-run` default and `--apply` to delete. A `data.prune_snapshots` task in `ops/tasks.yaml` runs it daily. Canonical live DBs are never touched — only dated session snapshots matching `quant_{research,report}_YYYY-MM-DD_{pre,post}.duckdb`.

**Tech Stack:** Python 3.11 (stdlib only), pytest, DuckDB single-writer (irrelevant here — we only stat/delete files), `ops/run_task.py` + `tasks.yaml` cron registry, `ops/render_cron.py`.

## Global Constraints

- **Never delete canonical DBs**: `quant.duckdb`, `quant_report.duckdb`, `quant_cn*.duckdb`, `data/strategy_backtest_history.duckdb`, `factor-lab/data/*` are out of scope and must never match the prune set.
- **Scope = session snapshots only**: filenames matching `quant_(research|report)_YYYY-MM-DD_(pre|post).duckdb` under `quant-research-v1/data/`. The `reports/review_dashboard/**/<date>/*.duckdb` component DBs are explicitly NOT in scope (may be read for backtest/PIT).
- **Retention window**: keep snapshots with date `>= today - KEEP_DAYS`; default `KEEP_DAYS = 7`.
- **Dry-run by default**: deletion only with explicit `--apply`.
- **No wall-clock in core logic**: `today` is a parameter (tests inject a fixed date).
- **Stack root** resolves from `QUANT_STACK_ROOT` env or two parents up from the script (matches `ops/data_inventory.py`).
- **Reversibility**: this deletes disposable debug snapshots (canonical is separate); there is no undo, so the CLI prints the full delete list and total bytes before acting, and `--apply` is required.

---

## Task 1: Pure snapshot classifier

**Files:**
- Create: `ops/prune_snapshots.py`
- Test: `tests/ops/test_prune_snapshots.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `SNAPSHOT_RE` (compiled regex), `classify_snapshots(names: list[str], today: datetime.date, keep_days: int) -> tuple[list[str], list[str]]` returning `(keep, delete)` — `delete` = snapshot names strictly older than `today - keep_days`; non-snapshot names are ignored (never in either list... they go to `keep`? No — ignored entirely, returned in neither). Define: returns `(keep, delete)` where both contain ONLY snapshot-matching names; non-matching names are dropped.

- [ ] **Step 1: Write the failing test**

```python
# tests/ops/test_prune_snapshots.py
import datetime
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "prune_snapshots", Path(__file__).resolve().parents[2] / "ops" / "prune_snapshots.py"
)
prune = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(prune)


def test_classify_keeps_recent_deletes_old_ignores_nonsnapshots():
    today = datetime.date(2026, 6, 25)
    names = [
        "quant_research_2026-06-24_pre.duckdb",   # 1 day old -> keep
        "quant_report_2026-06-24_post.duckdb",    # keep
        "quant_research_2026-06-10_post.duckdb",  # 15 days -> delete
        "quant_report_2026-06-01_pre.duckdb",     # delete
        "quant.duckdb",                           # canonical -> ignored
        "quant_report.duckdb",                    # canonical -> ignored
        "random_notes.txt",                       # ignored
    ]
    keep, delete = prune.classify_snapshots(names, today=today, keep_days=7)
    assert set(keep) == {
        "quant_research_2026-06-24_pre.duckdb",
        "quant_report_2026-06-24_post.duckdb",
    }
    assert set(delete) == {
        "quant_research_2026-06-10_post.duckdb",
        "quant_report_2026-06-01_pre.duckdb",
    }
    # canonical + non-snapshot never appear
    for n in ("quant.duckdb", "quant_report.duckdb", "random_notes.txt"):
        assert n not in keep and n not in delete


def test_boundary_exactly_keep_days_is_kept():
    today = datetime.date(2026, 6, 25)
    names = ["quant_research_2026-06-18_pre.duckdb"]  # exactly 7 days
    keep, delete = prune.classify_snapshots(names, today=today, keep_days=7)
    assert keep == ["quant_research_2026-06-18_pre.duckdb"]
    assert delete == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/ivena/coding/quant-stack && python3 -m pytest tests/ops/test_prune_snapshots.py -v`
Expected: FAIL — `ModuleNotFoundError`/`AttributeError` (file or `classify_snapshots` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# ops/prune_snapshots.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/ivena/coding/quant-stack && python3 -m pytest tests/ops/test_prune_snapshots.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add ops/prune_snapshots.py tests/ops/test_prune_snapshots.py
git commit -m "feat(ops): pure snapshot-retention classifier + tests"
```

---

## Task 2: CLI (dry-run default, --apply deletes)

**Files:**
- Modify: `ops/prune_snapshots.py` (add `human`, `iter_snapshot_dir`, `main`)
- Test: `tests/ops/test_prune_snapshots.py` (add a tmp-dir integration test)

**Interfaces:**
- Consumes: `classify_snapshots` (Task 1), `SNAPSHOT_RE`.
- Produces: `prune_dir(data_dir: Path, today: datetime.date, keep_days: int, apply: bool) -> tuple[list[Path], int]` returning `(deleted_or_would_delete_paths, total_bytes)`; deletes only when `apply=True`. `main()` CLI entry with `--keep-days` (default 7), `--apply`, `--data-dir` (default `<stack_root>/quant-research-v1/data`).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/ops/test_prune_snapshots.py
def test_prune_dir_dry_run_keeps_files(tmp_path):
    for n in ("quant_research_2026-06-01_pre.duckdb",
              "quant_research_2026-06-24_pre.duckdb",
              "quant.duckdb"):
        (tmp_path / n).write_bytes(b"x" * 10)
    today = datetime.date(2026, 6, 25)
    paths, total = prune.prune_dir(tmp_path, today=today, keep_days=7, apply=False)
    # would delete only the old snapshot
    assert [p.name for p in paths] == ["quant_research_2026-06-01_pre.duckdb"]
    assert total == 10
    # dry-run: nothing actually deleted
    assert (tmp_path / "quant_research_2026-06-01_pre.duckdb").exists()


def test_prune_dir_apply_deletes(tmp_path):
    (tmp_path / "quant_research_2026-06-01_pre.duckdb").write_bytes(b"x" * 10)
    (tmp_path / "quant.duckdb").write_bytes(b"x" * 10)
    today = datetime.date(2026, 6, 25)
    paths, total = prune.prune_dir(tmp_path, today=today, keep_days=7, apply=True)
    assert not (tmp_path / "quant_research_2026-06-01_pre.duckdb").exists()
    assert (tmp_path / "quant.duckdb").exists()  # canonical untouched
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/ivena/coding/quant-stack && python3 -m pytest tests/ops/test_prune_snapshots.py -v`
Expected: FAIL — `AttributeError: module 'prune_snapshots' has no attribute 'prune_dir'`.

- [ ] **Step 3: Write minimal implementation** (append to `ops/prune_snapshots.py`)

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/ivena/coding/quant-stack && python3 -m pytest tests/ops/test_prune_snapshots.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Dry-run against real data (no deletion)**

Run: `cd /home/ivena/coding/quant-stack && python3 ops/prune_snapshots.py`
Expected: prints "would delete (dry-run) N files, ~44GB" listing `quant_research_2026-06-XX_*` / `quant_report_2026-06-XX_*` older than 7 days; canonical DBs absent from the list.

- [ ] **Step 6: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add ops/prune_snapshots.py tests/ops/test_prune_snapshots.py
git commit -m "feat(ops): snapshot prune CLI (dry-run default, --apply)"
```

---

## Task 3: Schedule it + document the policy

**Files:**
- Modify: `ops/tasks.yaml` (add `data.prune_snapshots` task)
- Modify: `ops/crontab.quant-stack` (regenerated by render_cron)
- Create: `docs/DATA_RETENTION.md`

**Interfaces:**
- Consumes: `ops/prune_snapshots.py` CLI (Task 2).
- Produces: a registered `data.prune_snapshots` cron task; a retention policy doc.

- [ ] **Step 1: Add the task to `ops/tasks.yaml`** (insert a new task block; follow the existing schema, e.g. the `research.*` blocks). Place it in the `watchdog`/maintenance area:

```yaml
  data.prune_snapshots:
    group: maintenance
    schedule: "40 7 * * 1-5"
    cwd: .
    command: ["python3", "ops/prune_snapshots.py", "--apply", "--keep-days", "7"]
    log: ops/logs/data.prune_snapshots.log
    lock: ops/state/data.prune_snapshots.lock
    timeout_minutes: 10
    sends_email: false
    market: all
    session: maintenance
    owner: ops
    outputs: []
```

- [ ] **Step 2: Verify tasks.yaml still parses + lists the new task**

Run:
```bash
cd /home/ivena/coding/quant-stack
python3 -c "import yaml; d=yaml.safe_load(open('ops/tasks.yaml')); assert 'data.prune_snapshots' in d['tasks']; print('tasks:', len(d['tasks']))"
python3 ops/run_task.py --list | grep data.prune_snapshots
```
Expected: prints the task count and the line `data.prune_snapshots`.

- [ ] **Step 3: Regenerate the crontab**

Run:
```bash
cd /home/ivena/coding/quant-stack
python3 ops/render_cron.py --output ops/crontab.quant-stack
grep -c data.prune_snapshots ops/crontab.quant-stack
```
Expected: `1`.

- [ ] **Step 4: Write `docs/DATA_RETENTION.md`**

```markdown
# Data Retention Policy

## Per-session DuckDB snapshots (`quant-research-v1/data/`)
- Files: `quant_{research,report}_YYYY-MM-DD_{pre,post}.duckdb` — disposable debug/replay copies.
- Policy: keep the most recent **7 days**; older are deleted daily by `data.prune_snapshots`
  (`07:40 Mon-Fri`, runs `ops/prune_snapshots.py --apply --keep-days 7`).
- Canonical DBs (`quant.duckdb`, `quant_report.duckdb`, `quant_cn*.duckdb`,
  `data/strategy_backtest_history.duckdb`, `factor-lab/data/*`) are never pruned.

## Canonical history (future, hot/cold — see portability spec)
- Hot: recent 6-12 months stay in the live DuckDB on the compute host.
- Cold: older months export to partitioned Parquet on the NAS cold lake (lands with
  the Oracle/NAS/Pi phase). Not yet active.

## Component dashboards (`reports/review_dashboard/**/<date>/*.duckdb`)
- Out of scope for the snapshot pruner (may be read for backtest/PIT). Revisit separately.
```

- [ ] **Step 5: Install the regenerated crontab + run once to reclaim now**

Run:
```bash
cd /home/ivena/coding/quant-stack
crontab ops/crontab.quant-stack
python3 ops/prune_snapshots.py --apply --keep-days 7
df -h / | awk 'NR==1 || /\//{print}' | head -2
```
Expected: deletes the old session snapshots (~44GB reclaimed); available space jumps ~44GB.

- [ ] **Step 6: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add ops/tasks.yaml ops/crontab.quant-stack docs/DATA_RETENTION.md
git commit -m "feat(ops): schedule daily snapshot prune + retention policy doc"
```

---

## Self-Review

- **Spec coverage:** Implements portability spec 阶段 0 item 3 (snapshot retention) — the local half (prune old session snapshots + cron + policy). The cold-Parquet-to-NAS archival of *canonical* old months is explicitly out of scope here (hardware-gated, lands in the Oracle/NAS/Pi plan); `DATA_RETENTION.md` notes that boundary.
- **Placeholder scan:** No TBD/TODO; every step has concrete code/commands.
- **Type consistency:** `classify_snapshots(names, today, keep_days) -> (keep, delete)` and `prune_dir(data_dir, today, keep_days, apply) -> (paths, total)` used consistently across tasks; `SNAPSHOT_RE` shared.
- **Safety:** canonical DBs can't match `SNAPSHOT_RE` (require `_YYYY-MM-DD_{pre,post}` infix); dry-run default; `--apply` gated; tmp-dir tests prove canonical files survive `apply=True`.
