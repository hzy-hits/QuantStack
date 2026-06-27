# CN Ingestion Decoupling — Plan 2: Gate + Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the CN ingestion freshness gate + source registry, thread a `--skip-fetch` flag through the Rust orchestrator, and register fetch-worker / consolidate / gate cron tasks — running them in SHADOW (independent of the live report path), with the live `cn.morning/evening` flip held as a separate promote step.

**Architecture:** A source registry (`ops/fetch_sources.yaml`) + a pure `evaluate_freshness` function in `ops/freshness_gate.py` read `fetch_state` from the CN hot DB and classify each source fresh/stale by criticality. The orchestrator gains a `--skip-fetch` flag (default OFF) forwarded to `quant-cn run`. New `tasks.yaml` entries run the fetch workers → consolidate → gate on cron, populating staging + hot's `fetch_state` WITHOUT touching `cn.morning/evening` yet. Task 4 (the live flip) is gated on shadow validation and is NOT auto-executed.

**Tech Stack:** Python 3.11 (gate + registry; pytest under `quant-research-v1/.venv`), Rust 2021 (quant-stack-cli, root workspace — `cargo build --release`), `ops/tasks.yaml` + `ops/render_cron.py` cron registry.

## Global Constraints

- Plan 1 (data path) is DONE: `fetch_state` table, `quant-cn fetch --source --staging`, `scripts/consolidate_raw.py` all exist and are tested. This plan consumes them.
- `--skip-fetch` already exists on `quant-cn run` (CN Rust) and on the new `quant-cn fetch`. The orchestrator flag defaults OFF — adding it changes NO behavior until a task passes it (Task 4).
- Freshness criticality/max-staleness POLICY lives ONLY in `ops/fetch_sources.yaml` (not in `fetch_state`).
- The gate is fail-closed by EXIT CODE (non-zero on critical-stale) + an OPTIONAL operator email (guarded behind `--alert` + `QUANT_OPERATOR_EMAIL`; never sends in shadow/tests). Do NOT modify `ops/run_task.py` to add alerting (invasive; affects all tasks).
- Tasks 1–3 are SHADOW (zero change to `cn.morning/evening`). Task 4 is the live flip — execute ONLY after explicit go-ahead following shadow validation.
- CN hot DB from repo root: `quant-research-cn/data/quant_cn.duckdb`. quant-cn worker tasks run with `cwd: quant-research-cn`.
- crontab is generated: never hand-edit; run `python3 ops/render_cron.py --output ops/crontab.quant-stack` after task changes.

---

### Task 1: Source registry + freshness gate (pure logic + tests)

**Files:**
- Create: `ops/fetch_sources.yaml`
- Create: `ops/freshness_gate.py`
- Test: `tests/test_freshness_gate.py`

**Interfaces:**
- Produces: `evaluate_freshness(sources: list[dict], state_by_fetcher: dict[str, dict], today: datetime.date) -> tuple[bool, list[str], list[str]]` returning `(ok, critical_stale, optional_stale)`; CLI `python3 ops/freshness_gate.py --market cn [--alert]` (exit 0 fresh, exit 1 critical-stale).

- [ ] **Step 1: Write the registry** — create `ops/fetch_sources.yaml`:

```yaml
# Per-source ingestion policy for the freshness gate.
# criticality: critical → fail-closed when stale; optional → tolerate (note only).
# max_staleness_days: data `as_of` older than today-N (calendar days) counts stale.
cn:
  - source: tushare
    criticality: critical
    max_staleness_days: 3
  - source: akshare
    criticality: optional
    max_staleness_days: 7
```

- [ ] **Step 2: Write the failing test** — create `tests/test_freshness_gate.py`:

```python
"""Tests for the ingestion freshness gate (pure logic)."""
from __future__ import annotations

import datetime as dt
import importlib
import sys
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]


def _load():
    sys.path.insert(0, str(STACK_ROOT / "ops"))
    return importlib.import_module("freshness_gate")


SOURCES = [
    {"source": "tushare", "criticality": "critical", "max_staleness_days": 3},
    {"source": "akshare", "criticality": "optional", "max_staleness_days": 7},
]
TODAY = dt.date(2026, 6, 27)


class FreshnessGateTests(unittest.TestCase):
    def setUp(self):
        self.mod = _load()

    def test_all_fresh_passes(self):
        state = {
            "tushare": {"status": "ok", "as_of": dt.date(2026, 6, 26)},
            "akshare": {"status": "ok", "as_of": dt.date(2026, 6, 25)},
        }
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertTrue(ok)
        self.assertEqual(crit, [])
        self.assertEqual(opt, [])

    def test_critical_missing_fails(self):
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, {}, TODAY)
        self.assertFalse(ok)
        self.assertIn("tushare", crit)

    def test_critical_too_old_fails(self):
        state = {"tushare": {"status": "ok", "as_of": dt.date(2026, 6, 20)}}  # 7d > 3d
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertFalse(ok)
        self.assertIn("tushare", crit)

    def test_critical_error_status_fails(self):
        state = {"tushare": {"status": "error", "as_of": dt.date(2026, 6, 26)}}
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertFalse(ok)
        self.assertIn("tushare", crit)

    def test_optional_stale_still_passes(self):
        state = {"tushare": {"status": "ok", "as_of": dt.date(2026, 6, 26)}}  # akshare missing
        ok, crit, opt = self.mod.evaluate_freshness(SOURCES, state, TODAY)
        self.assertTrue(ok)          # optional staleness does not fail the gate
        self.assertIn("akshare", opt)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_freshness_gate.py -q 2>&1 | tail -8`
Expected: FAIL — `ModuleNotFoundError: No module named 'freshness_gate'`.

- [ ] **Step 4: Write `ops/freshness_gate.py`:**

```python
"""CN ingestion freshness gate.

Reads ops/fetch_sources.yaml (policy) + fetch_state from the CN hot DB (actual),
classifies each source fresh/stale by criticality. Exit 0 if no critical source
is stale; exit 1 (fail-closed) otherwise. On critical-stale, optionally emails an
operator (only with --alert AND QUANT_OPERATOR_EMAIL set; never in shadow/tests).

Usage:
    python3 ops/freshness_gate.py --market cn
    python3 ops/freshness_gate.py --market cn --alert
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import yaml

STACK_ROOT = Path(__file__).resolve().parents[1]
REGISTRY = STACK_ROOT / "ops" / "fetch_sources.yaml"
CN_HOT = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"


def evaluate_freshness(sources, state_by_fetcher, today):
    """Return (ok, critical_stale, optional_stale).

    A source is stale if: no fetch_state row, status != 'ok', as_of missing, or
    as_of older than today - max_staleness_days. ok == no critical source stale.
    """
    critical_stale, optional_stale = [], []
    for s in sources:
        row = state_by_fetcher.get(s["source"])
        as_of = row.get("as_of") if row else None
        stale = (
            row is None
            or row.get("status") != "ok"
            or as_of is None
            or (today - as_of).days > int(s["max_staleness_days"])
        )
        if stale:
            if s["criticality"] == "critical":
                critical_stale.append(s["source"])
            else:
                optional_stale.append(s["source"])
    return (len(critical_stale) == 0, critical_stale, optional_stale)


def _load_state(hot_path):
    import duckdb
    if not Path(hot_path).exists():
        return {}
    con = duckdb.connect(str(hot_path), read_only=True)
    try:
        try:
            rows = con.execute(
                "SELECT fetcher, status, as_of FROM fetch_state WHERE market='cn'"
            ).fetchall()
        except duckdb.CatalogException:
            return {}  # table not created yet (no worker has run)
        return {r[0]: {"status": r[1], "as_of": r[2]} for r in rows}
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["cn"])
    ap.add_argument("--alert", action="store_true", help="email operator on critical-stale")
    ap.add_argument("--hot", default=str(CN_HOT))
    args = ap.parse_args()

    sources = (yaml.safe_load(REGISTRY.read_text(encoding="utf-8")) or {}).get(args.market, [])
    state = _load_state(args.hot)
    today = dt.datetime.now().date()
    ok, critical_stale, optional_stale = evaluate_freshness(sources, state, today)

    print(f"freshness {args.market}: ok={ok} critical_stale={critical_stale} optional_stale={optional_stale}")
    if not ok and args.alert:
        operator = os.environ.get("QUANT_OPERATOR_EMAIL", "").strip()
        if operator:
            try:
                sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))
                from quant_bot.delivery.gmail import send_alert_email
                send_alert_email(
                    operator,
                    f"[检修] CN 取数关键源过期: {', '.join(critical_stale)}",
                    f"freshness gate fail-closed at {today}. critical_stale={critical_stale}. "
                    f"CN report suppressed. Check fetch workers + consolidate.",
                )
                print(f"alert sent to {operator}")
            except Exception as e:  # noqa: BLE001 — alert must never crash the gate
                print(f"alert send failed: {e}", file=sys.stderr)
        else:
            print("QUANT_OPERATOR_EMAIL unset; skipping alert email", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_freshness_gate.py -q 2>&1 | tail -6`
Expected: PASS (5 passed).

- [ ] **Step 6: Smoke — run the gate against the real CN hot DB**

Run: `cd /home/ivena/coding/quant-stack && quant-research-v1/.venv/bin/python ops/freshness_gate.py --market cn; echo "exit=$?"`
Expected: prints a `freshness cn: ok=... critical_stale=[...] ...` line. Likely `ok=False critical_stale=['tushare']` + `exit=1` because no worker has populated `fetch_state` in production hot yet — that is the correct fail-closed signal pre-shadow. (No `--alert`, so no email.)

- [ ] **Step 7: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add ops/fetch_sources.yaml ops/freshness_gate.py tests/test_freshness_gate.py
git commit -m "feat(cn): freshness gate + source registry (fail-closed on critical-stale)"
```

---

### Task 2: Orchestrator `--skip-fetch` flag (default OFF, forwarded to quant-cn run)

**Files:**
- Modify: `crates/quant-stack-cli/src/main.rs` (`DailyArgs` struct ~line 119-152; `run_producers` call ~line 336-343; `run_producers` fn ~line 417-441)

**Interfaces:**
- Consumes: existing `DailyArgs`, `run_producers`, `cn_quant_command`.
- Produces: `quant-stack daily --skip-fetch` flag; when set, the CN producer is invoked as `quant-cn run --date D --skip-fetch`.

- [ ] **Step 1: Add the `skip_fetch` field to `DailyArgs`** — in `crates/quant-stack-cli/src/main.rs`, inside `struct DailyArgs`, after the `run_producers` field add:

```rust
    #[arg(long)]
    skip_fetch: bool,
```

- [ ] **Step 2: Thread it into the `run_producers` call** — change the call site (currently):

```rust
    if args.run_producers {
        run_producers(
            &stack_root,
            &args.date,
            &args.session,
            &markets,
            args.dry_run,
        )?;
    }
```

to:

```rust
    if args.run_producers {
        run_producers(
            &stack_root,
            &args.date,
            &args.session,
            &markets,
            args.dry_run,
            args.skip_fetch,
        )?;
    }
```

- [ ] **Step 3: Accept + apply the flag in `run_producers`** — change the signature and the CN branch:

```rust
fn run_producers(
    stack_root: &Path,
    date: &str,
    session: &str,
    markets: &[String],
    dry_run: bool,
    cn_skip_fetch: bool,
) -> Result<()> {
```

and in the CN branch replace:

```rust
        cmd.arg("run").arg("--date").arg(date).current_dir(&cn_root);
```

with:

```rust
        cmd.arg("run").arg("--date").arg(date);
        if cn_skip_fetch {
            cmd.arg("--skip-fetch");
        }
        cmd.current_dir(&cn_root);
```

- [ ] **Step 4: Build the root workspace**

Run: `cd /home/ivena/coding/quant-stack && cargo build --release -p quant-stack-cli 2>&1 | tail -3`
Expected: `Finished release`, no errors.

- [ ] **Step 5: Smoke — dry-run shows the flag forwarded**

Run:
```bash
cd /home/ivena/coding/quant-stack
./target/release/quant-stack daily --date 2026-06-26 --markets cn --session morning \
  --run-producers --skip-fetch --dry-run 2>&1 | grep -i "cn producer\|skip-fetch"
```
Expected: the dry-run printout of the CN producer command includes `--skip-fetch`. (Without `--skip-fetch`, it must NOT appear — verify by re-running without the flag.)

- [ ] **Step 6: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add crates/quant-stack-cli/src/main.rs
git commit -m "feat(orchestrator): daily --skip-fetch forwards to quant-cn run (default off)"
```

---

### Task 3: Register fetch-worker / consolidate / gate cron tasks (SHADOW)

**Files:**
- Modify: `ops/tasks.yaml` (add 4 new tasks; do NOT touch `cn.morning`/`cn.evening`)
- Modify: `ops/crontab.quant-stack` (regenerated)

**Interfaces:**
- Consumes: `quant-cn fetch` (Plan 1), `scripts/consolidate_raw.py` (Plan 1), `ops/freshness_gate.py` (Task 1).
- Produces: cron tasks `cn.fetch.tushare`, `cn.fetch.akshare`, `cn.consolidate`, `cn.freshness_gate` — all independent of the report pipeline.

- [ ] **Step 1: Add the four tasks** — in `ops/tasks.yaml`, add (place near the `cn.*` tasks; keep the existing `cn.morning`/`cn.evening` UNCHANGED):

```yaml
  cn.fetch.tushare:
    group: cn
    schedule: "0 16 * * 1-5"
    cwd: quant-research-cn
    command:
      - ./target/release/quant-cn
      - fetch
      - --source
      - tushare
      - --date
      - "{cst_date}"
      - --staging
      - data/staging/cn_tushare.duckdb
    log: ops/logs/cn.fetch.tushare.log
    lock: ops/state/cn.fetch.tushare.lock
    timeout_minutes: 30
    sends_email: false
    market: cn
    session: ingest
    owner: cn_ingest
    outputs:
      - quant-research-cn/data/staging/cn_tushare.duckdb

  cn.fetch.akshare:
    group: cn
    schedule: "5 16 * * 1-5"
    cwd: quant-research-cn
    command:
      - ./target/release/quant-cn
      - fetch
      - --source
      - akshare
      - --date
      - "{cst_date}"
      - --staging
      - data/staging/cn_akshare.duckdb
    log: ops/logs/cn.fetch.akshare.log
    lock: ops/state/cn.fetch.akshare.lock
    timeout_minutes: 20
    sends_email: false
    market: cn
    session: ingest
    owner: cn_ingest
    outputs:
      - quant-research-cn/data/staging/cn_akshare.duckdb

  cn.consolidate:
    group: cn
    schedule: "30 16 * * 1-5"
    cwd: .
    command:
      - python3
      - scripts/consolidate_raw.py
      - --market
      - cn
    log: ops/logs/cn.consolidate.log
    lock: ops/state/cn.consolidate.lock
    timeout_minutes: 15
    sends_email: false
    market: cn
    session: ingest
    owner: cn_ingest
    outputs:
      - quant-research-cn/data/quant_cn.duckdb

  cn.freshness_gate:
    group: cn
    schedule: "25 8 * * 1-5"
    cwd: .
    command:
      - python3
      - ops/freshness_gate.py
      - --market
      - cn
    log: ops/logs/cn.freshness_gate.log
    lock: ops/state/cn.freshness_gate.lock
    timeout_minutes: 5
    sends_email: false
    market: cn
    session: ingest
    owner: cn_ingest
```

- [ ] **Step 2: Validate the registry parses + new tasks present**

Run:
```bash
cd /home/ivena/coding/quant-stack
python3 - <<'PY'
import yaml
d = yaml.safe_load(open("ops/tasks.yaml"))
tasks = d.get("tasks", d)
for t in ["cn.fetch.tushare", "cn.fetch.akshare", "cn.consolidate", "cn.freshness_gate"]:
    assert t in tasks, f"missing {t}"
    print("ok:", t, tasks[t]["schedule"])
# cn.morning/evening MUST be unchanged (no skip-fetch, no depends_on yet)
m = tasks["cn.morning"]
assert "--skip-fetch" not in m["command"], "cn.morning must NOT be flipped in Plan 2"
assert "depends_on" not in m, "cn.morning must NOT depend on the gate yet"
print("cn.morning unchanged: OK")
PY
```
Expected: prints `ok:` for all 4 new tasks + `cn.morning unchanged: OK`.

- [ ] **Step 3: Regenerate the crontab**

Run:
```bash
cd /home/ivena/coding/quant-stack
python3 ops/render_cron.py --output ops/crontab.quant-stack 2>&1 | tail -2
grep -c "cn.fetch.tushare\|cn.fetch.akshare\|cn.consolidate\|cn.freshness_gate" ops/crontab.quant-stack
```
Expected: the crontab regenerates and the grep count is `4` (each new task has a cron line).

- [ ] **Step 4: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add ops/tasks.yaml ops/crontab.quant-stack
git commit -m "feat(cn): register fetch-worker + consolidate + freshness-gate cron tasks (shadow)"
```

- [ ] **Step 5: Install the crontab (operator action — surface, do not auto-run)**

The new cron lines take effect only after `crontab ops/crontab.quant-stack` is installed. This is an operator step (installing crontab is outward-facing). REPORT to the operator with the diff; do not auto-install. After install, the workers run post-close (16:00 CST), consolidate at 16:30, gate at 08:25 — all in shadow.

---

### Task 4: PROMOTE — flip cn.morning/evening to --skip-fetch + gate dependency (HELD)

> **DO NOT EXECUTE until shadow validation passes:** for ≥3 trading days, confirm `cn.fetch.tushare` + `cn.consolidate` populate hot `fetch_state` with `status='ok'` and a current `as_of`, and `cn.freshness_gate` exits 0. Only then flip the live pipeline.

**Files:**
- Modify: `ops/tasks.yaml` (`cn.morning`, `cn.evening`)
- Modify: `ops/crontab.quant-stack` (regenerated)

- [ ] **Step 1: Add `--skip-fetch` + gate dependency** — to BOTH `cn.morning` and `cn.evening`, append `--skip-fetch` to the `command:` list (after `--stack-root "{stack_root}"`) and add:

```yaml
    depends_on:
      - cn.freshness_gate
```

(cn.evening should depend on a second evening gate schedule — add a `"55 17 * * 1-5"` schedule to `cn.freshness_gate` as a list, or a sibling `cn.freshness_gate.evening` task. Decide at promote time based on shadow behavior.)

- [ ] **Step 2: Regenerate crontab + validate**, then commit. (Full steps authored at promote time, after shadow data informs the evening-gate choice.)

**Rollback:** remove `--skip-fetch` + `depends_on` from `cn.morning/evening`, regenerate crontab → back to inline fetch. Worker/consolidate/gate tasks keep running harmlessly.

---

## Self-Review

- **Spec coverage:** 组件4 新鲜度门 → Task 1 (registry + gate, fail-closed exit + optional alert). `--skip-fetch` 转发 → Task 2. 常驻 worker + consolidate 上 cron → Task 3. 源注册表 → Task 1 (`fetch_sources.yaml`). The live flip (组件4 pipeline side) → Task 4, held for shadow→promote.
- **Placeholder scan:** none — every code step shows full code; every command has expected output. Task 4 Step 2 is intentionally deferred (its evening-gate detail depends on observed shadow behavior) and is explicitly marked HELD, not a silent gap.
- **Type consistency:** `evaluate_freshness(sources, state_by_fetcher, today) -> (bool, list, list)` defined in Task 1 and used identically in its test. `--skip-fetch` flag name identical across Rust orchestrator (Task 2), `quant-cn run` (Plan 1), and tasks.yaml (Task 4). `fetch_state` columns (`fetcher`, `status`, `as_of`) read in the gate match Plan 1's schema.
- **Safety:** Tasks 1–3 add only new files + new independent cron tasks; `cn.morning/evening` are asserted unchanged (Task 3 Step 2). No live report behavior changes until Task 4. Gate sends email only with `--alert` + `QUANT_OPERATOR_EMAIL` (never in shadow). Crontab install is surfaced to the operator, not auto-run.
