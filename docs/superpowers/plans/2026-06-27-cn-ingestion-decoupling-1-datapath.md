# CN Ingestion Decoupling — Plan 1: Data Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make CN fetch write to per-source staging DuckDBs and add a serial `consolidate` step that merges staging → hot, tracked by a `fetch_state` watermark table — the mechanism only, no pipeline wiring yet.

**Architecture:** Add a `fetch_state` table to the CN schema + a `record_fetch_state` storage helper. Extend the existing `quant-cn fetch` subcommand with `--source {tushare|akshare|all}` and `--staging <path>` so each source-worker writes its own `staging/{source}.duckdb` and records its watermark. A new Python `scripts/consolidate_raw.py --market cn` takes the hot-DB write lock and `ATTACH`-merges each staging DB into hot via `INSERT OR REPLACE`. This plan delivers a manually-runnable decoupled ingest; the orchestrator `--skip-fetch` + freshness-gate wiring is Plan 2.

**Tech Stack:** Rust 2021 (`quant-cn` crate at `quant-research-cn/`, EXCLUDED from root workspace — build/test via `--manifest-path quant-research-cn/Cargo.toml`), duckdb-rs, chrono, clap, tokio. Python 3.11 + duckdb (consolidate script + pytest under `quant-research-v1/.venv/bin/python`).

## Global Constraints

- Build/test the CN crate with `--manifest-path quant-research-cn/Cargo.toml` (package `quant-cn`); do NOT `cd` (avoids permission prompts).
- `fetch_state` is pure STATE (no policy): columns `market, fetcher, as_of, status, row_count, fetched_at, error`, PK `(market, fetcher)`. Criticality / max-staleness POLICY lives in a registry in Plan 2, NOT this table.
- `consolidate` is the ONLY writer to the hot DB; it takes the fcntl exclusive lock (`connect_write`). Workers write only their own staging DB and never touch hot.
- `INSERT OR REPLACE` for all upserts (DuckDB idempotency); merge only tables present in BOTH staging and hot, skipping empty staging tables.
- staging schema == hot schema (both via `storage::init_schema` / same `CREATE_TABLES`), so `SELECT *` column alignment holds.
- Do NOT modify analytics, `run` pipeline body, narration, delivery, or `tasks.yaml` in this plan. `Command::Fetch` is the only CLI surface touched.
- CN hot DB (live config): `quant-research-cn/data/quant_cn.duckdb`. quant-cn runs with cwd=quant-research-cn (config + data are cwd-relative).

---

### Task 1: `fetch_state` table + `record_fetch_state` storage helper

**Files:**
- Modify: `quant-research-cn/src/storage/schema.rs` (append to `CREATE_TABLES` const)
- Modify: `quant-research-cn/src/storage/mod.rs` (add `record_fetch_state` + inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `init_schema(&Connection) -> Result<()>`, `CREATE_TABLES: &str`.
- Produces: `storage::record_fetch_state(con: &Connection, market: &str, fetcher: &str, as_of: chrono::NaiveDate, status: &str, rows: usize, error: Option<&str>) -> anyhow::Result<()>`; `fetch_state` table.

- [ ] **Step 1: Write the failing test** — append to the END of `quant-research-cn/src/storage/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn fetch_state_table_exists_and_upsert_is_idempotent() {
        let con = Connection::open_in_memory().unwrap();
        init_schema(&con).unwrap();
        let d = chrono::NaiveDate::from_ymd_opt(2026, 6, 26).unwrap();
        record_fetch_state(&con, "cn", "tushare", d, "ok", 100, None).unwrap();
        // same PK again with a different row_count → must REPLACE, not duplicate
        record_fetch_state(&con, "cn", "tushare", d, "ok", 150, None).unwrap();
        let (n, rows): (i64, i64) = con
            .query_row(
                "SELECT count(*), max(row_count) FROM fetch_state \
                 WHERE market='cn' AND fetcher='tushare'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(n, 1, "upsert must not duplicate the (market,fetcher) PK");
        assert_eq!(rows, 150, "latest write must win");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --manifest-path quant-research-cn/Cargo.toml fetch_state_table_exists_and_upsert_is_idempotent 2>&1 | tail -15`
Expected: FAIL to compile — `record_fetch_state` not found and `fetch_state` table does not exist.

- [ ] **Step 3: Add the `fetch_state` table** — in `quant-research-cn/src/storage/schema.rs`, append this `CREATE TABLE` inside the `CREATE_TABLES` const string (just before its closing `";`):

```sql
CREATE TABLE IF NOT EXISTS fetch_state (
    market      VARCHAR NOT NULL,
    fetcher     VARCHAR NOT NULL,
    as_of       DATE,
    status      VARCHAR,
    row_count   BIGINT DEFAULT 0,
    fetched_at  TIMESTAMP DEFAULT current_timestamp,
    error       VARCHAR,
    PRIMARY KEY (market, fetcher)
);
```

- [ ] **Step 4: Add the `record_fetch_state` helper** — in `quant-research-cn/src/storage/mod.rs`, add (near the other `pub fn`s; ensure `use chrono::NaiveDate;` and `use anyhow::Result;` and `use duckdb::Connection;` are in scope — they are used elsewhere in this file):

```rust
/// Upsert a per-(market,fetcher) ingestion watermark row.
pub fn record_fetch_state(
    con: &Connection,
    market: &str,
    fetcher: &str,
    as_of: NaiveDate,
    status: &str,
    rows: usize,
    error: Option<&str>,
) -> Result<()> {
    con.execute(
        "INSERT OR REPLACE INTO fetch_state \
         (market, fetcher, as_of, status, row_count, fetched_at, error) \
         VALUES (?, ?, ?, ?, ?, current_timestamp, ?)",
        duckdb::params![market, fetcher, as_of.to_string(), status, rows as i64, error],
    )?;
    Ok(())
}
```

(If `NaiveDate` / `Result` / `Connection` are not already imported at the top of `mod.rs`, add `use anyhow::Result;`, `use chrono::NaiveDate;`, `use duckdb::Connection;`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --manifest-path quant-research-cn/Cargo.toml fetch_state_table_exists_and_upsert_is_idempotent 2>&1 | tail -15`
Expected: PASS (1 passed).

- [ ] **Step 6: Confirm the crate still builds**

Run: `cargo build --release --manifest-path quant-research-cn/Cargo.toml 2>&1 | tail -3`
Expected: `Finished release` with no errors.

- [ ] **Step 7: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add quant-research-cn/src/storage/schema.rs quant-research-cn/src/storage/mod.rs
git commit -m "feat(cn): fetch_state watermark table + record_fetch_state helper"
```

---

### Task 2: `quant-cn fetch --source --staging` routes each source to its own staging DB

**Files:**
- Modify: `quant-research-cn/src/main.rs` (`Command::Fetch` variant ~line 47-50; its handler ~line 225-233)

**Interfaces:**
- Consumes: `storage::open`, `storage::record_fetch_state` (Task 1), `fetcher::tushare::fetch_all`, `fetcher::akshare::fetch_all`, `config::resolve_date`, `cfg.data.raw_path()`.
- Produces: CLI `quant-cn fetch [--date D] [--source tushare|akshare|all] [--staging PATH]`. When `--staging` given, writes data + a `fetch_state` row into that DB; else writes to hot raw DB (legacy behavior preserved when no flags).

- [ ] **Step 1: Extend the `Command::Fetch` variant** — in `quant-research-cn/src/main.rs`, replace:

```rust
    /// Fetch data only (no analytics)
    Fetch {
        #[arg(long)]
        date: Option<String>,
    },
```

with:

```rust
    /// Fetch data only (no analytics). With --staging, writes to a per-source staging DB.
    Fetch {
        #[arg(long)]
        date: Option<String>,
        /// Which source to fetch: tushare | akshare | all (default all)
        #[arg(long, default_value = "all")]
        source: String,
        /// Optional staging DB path; if set, fetch writes here instead of the hot raw DB
        #[arg(long)]
        staging: Option<String>,
    },
```

- [ ] **Step 2: Replace the `Command::Fetch` handler** — replace the existing handler block (currently):

```rust
        Command::Fetch { date } => {
            let as_of = config::resolve_date(date.as_deref())?;
            let raw_db = storage::open(cfg.data.raw_path())?;
            let (t, a) = tokio::join!(
                fetcher::tushare::fetch_all(&raw_db, &cfg, as_of),
                fetcher::akshare::fetch_all(&raw_db, &cfg, as_of),
            );
            info!(tushare = ?t, akshare = ?a, "fetch complete");
        }
```

with:

```rust
        Command::Fetch { date, source, staging } => {
            let as_of = config::resolve_date(date.as_deref())?;
            let target = staging.as_deref().unwrap_or(cfg.data.raw_path());
            let db = storage::open(target)?;
            info!(%as_of, source = %source, target, "fetch-only start");

            if source == "tushare" || source == "all" {
                let res = fetcher::tushare::fetch_all(&db, &cfg, as_of).await;
                let rows = *res.as_ref().unwrap_or(&0);
                let err = res.as_ref().err().map(|e| e.to_string());
                let status = if res.is_ok() { "ok" } else { "error" };
                storage::record_fetch_state(&db, "cn", "tushare", as_of, status, rows, err.as_deref())?;
                info!(tushare = ?res, "tushare fetch recorded");
            }
            if source == "akshare" || source == "all" {
                let res = fetcher::akshare::fetch_all(&db, &cfg, as_of).await;
                let rows = *res.as_ref().unwrap_or(&0);
                let err = res.as_ref().err().map(|e| e.to_string());
                let status = if res.is_ok() { "ok" } else { "error" };
                storage::record_fetch_state(&db, "cn", "akshare", as_of, status, rows, err.as_deref())?;
                info!(akshare = ?res, "akshare fetch recorded");
            }
        }
```

(Note: sequential awaits replace the old `tokio::join!` — per-source workers each invoke a single `--source`, so parallelism within one call is unnecessary. `--source all` runs them in sequence, which is acceptable.)

- [ ] **Step 3: Build the crate**

Run: `cargo build --release --manifest-path quant-research-cn/Cargo.toml 2>&1 | tail -3`
Expected: `Finished release`, no errors.

- [ ] **Step 4: Smoke — fetch akshare into a staging DB and verify staging + fetch_state**

(AKShare is fast and degrades to 0 rows if its localhost bridge is down — it still exercises routing + `fetch_state`. cwd must be `quant-research-cn` so config/data resolve.)

Run:
```bash
cd /home/ivena/coding/quant-stack/quant-research-cn
mkdir -p data/staging
./target/release/quant-cn fetch --source akshare --date 2026-06-26 --staging data/staging/cn_akshare.duckdb 2>&1 | tail -5
../quant-research-v1/.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect("data/staging/cn_akshare.duckdb", read_only=True)
print("fetch_state rows:", con.execute(
    "SELECT market, fetcher, status, row_count FROM fetch_state").fetchall())
PY
```
Expected: a `fetch_state` row `('cn','akshare', <'ok'|'error'>, <n>)` exists in the staging DB; the staging file was created separately from the hot DB. If status is 'error' due to bridge being down, that is acceptable for this smoke (routing + watermark are what we verify).

- [ ] **Step 5: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add quant-research-cn/src/main.rs
git commit -m "feat(cn): quant-cn fetch --source/--staging writes per-source staging + fetch_state"
```

---

### Task 3: `scripts/consolidate_raw.py --market cn` merges staging → hot under the write lock

**Files:**
- Create: `scripts/consolidate_raw.py`
- Test: `tests/test_consolidate_raw.py`

**Interfaces:**
- Consumes: `quant_bot.storage.db.connect_write` (exclusive fcntl lock on the hot DB).
- Produces: `consolidate_cn(hot_path: str, staging_paths: list[str]) -> int` (rows merged); CLI `python3 scripts/consolidate_raw.py --market cn [--hot PATH] [--staging-dir DIR]`.

- [ ] **Step 1: Write the failing test** — create `tests/test_consolidate_raw.py`:

```python
"""Tests for the staging→hot consolidate step (CN)."""
from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]


def _load():
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    return importlib.import_module("consolidate_raw")


def _make_db(path: Path, prices_rows, fetch_row):
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE prices (ts_code VARCHAR, trade_date DATE, close DOUBLE, "
                "PRIMARY KEY (ts_code, trade_date))")
    con.execute("CREATE TABLE fetch_state (market VARCHAR, fetcher VARCHAR, as_of DATE, "
                "status VARCHAR, row_count BIGINT, fetched_at TIMESTAMP, error VARCHAR, "
                "PRIMARY KEY (market, fetcher))")
    for r in prices_rows:
        con.execute("INSERT OR REPLACE INTO prices VALUES (?, ?, ?)", r)
    if fetch_row:
        con.execute("INSERT OR REPLACE INTO fetch_state "
                    "(market, fetcher, as_of, status, row_count, fetched_at, error) "
                    "VALUES (?, ?, ?, ?, ?, current_timestamp, NULL)", fetch_row)
    con.close()


class ConsolidateCnTests(unittest.TestCase):
    def setUp(self):
        self.mod = _load()
        self.tmp = STACK_ROOT / "tests" / "_tmp_consolidate"
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.hot = self.tmp / "hot.duckdb"
        self.stg = self.tmp / "cn_tushare.duckdb"
        for p in (self.hot, self.stg, Path(str(self.hot) + ".lock")):
            if p.exists():
                p.unlink()
        # hot has one existing (stale) row that staging will overwrite
        _make_db(self.hot, [("600519.SH", "2026-06-26", 1.0)], None)
        _make_db(self.stg, [("600519.SH", "2026-06-26", 1700.0), ("688981.SH", "2026-06-26", 90.0)],
                 ("cn", "tushare", "2026-06-26", "ok", 2))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_merge_upserts_and_records_watermark(self):
        merged = self.mod.consolidate_cn(str(self.hot), [str(self.stg)])
        self.assertEqual(merged, 3)  # 2 prices + 1 fetch_state row
        con = duckdb.connect(str(self.hot), read_only=True)
        # existing row overwritten, new row inserted
        self.assertEqual(con.execute(
            "SELECT close FROM prices WHERE ts_code='600519.SH'").fetchone()[0], 1700.0)
        self.assertEqual(con.execute("SELECT count(*) FROM prices").fetchone()[0], 2)
        self.assertEqual(con.execute(
            "SELECT row_count FROM fetch_state WHERE fetcher='tushare'").fetchone()[0], 2)
        con.close()

    def test_missing_staging_is_skipped(self):
        merged = self.mod.consolidate_cn(str(self.hot), [str(self.tmp / "nope.duckdb")])
        self.assertEqual(merged, 0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_consolidate_raw.py -q 2>&1 | tail -15`
Expected: FAIL — `ModuleNotFoundError: No module named 'consolidate_raw'`.

- [ ] **Step 3: Write `scripts/consolidate_raw.py`:**

```python
"""Consolidate per-source staging DuckDBs into the hot DB (single writer).

Each fetch worker writes its own staging/{source}.duckdb. This script takes the
hot DB's exclusive write lock and merges every matching table via INSERT OR
REPLACE, including the fetch_state watermark. It is the ONLY writer to hot.

Usage:
    python3 scripts/consolidate_raw.py --market cn
    python3 scripts/consolidate_raw.py --market cn --hot <path> --staging-dir <dir>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))

from quant_bot.storage.db import connect_write  # noqa: E402

CN_HOT_DEFAULT = STACK_ROOT / "quant-research-cn" / "data" / "quant_cn.duckdb"
CN_STAGING_DIR_DEFAULT = STACK_ROOT / "quant-research-cn" / "data" / "staging"
CN_SOURCES = ["cn_tushare.duckdb", "cn_akshare.duckdb"]


def consolidate_cn(hot_path: str, staging_paths: list[str]) -> int:
    """Merge each existing staging DB into hot via INSERT OR REPLACE. Returns rows merged."""
    con = connect_write(hot_path)  # exclusive fcntl lock on hot
    try:
        hot_tables = {
            r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        total = 0
        for i, sp in enumerate(staging_paths):
            if not Path(sp).exists():
                continue
            alias = f"s{i}"
            con.execute(f"ATTACH '{sp}' AS {alias} (READ_ONLY)")
            try:
                stg_tables = {
                    r[0] for r in con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_catalog=?",
                        [alias],
                    ).fetchall()
                }
                for t in sorted(hot_tables & stg_tables):
                    n = con.execute(f"SELECT count(*) FROM {alias}.{t}").fetchone()[0]
                    if n == 0:
                        continue
                    con.execute(f"INSERT OR REPLACE INTO main.{t} SELECT * FROM {alias}.{t}")
                    total += n
            finally:
                con.execute(f"DETACH {alias}")
        return total
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["cn"])
    ap.add_argument("--hot", default=str(CN_HOT_DEFAULT))
    ap.add_argument("--staging-dir", default=str(CN_STAGING_DIR_DEFAULT))
    args = ap.parse_args()
    staging = [str(Path(args.staging_dir) / name) for name in CN_SOURCES]
    merged = consolidate_cn(args.hot, staging)
    print(f"consolidate cn: merged {merged} rows from {len([s for s in staging if Path(s).exists()])} staging DB(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `quant-research-v1/.venv/bin/python -m pytest tests/test_consolidate_raw.py -q 2>&1 | tail -15`
Expected: PASS (2 passed).

- [ ] **Step 5: End-to-end smoke — worker → consolidate → hot**

(Uses the staging DB produced in Task 2 Step 4; merges it into a throwaway hot copy to avoid touching production.)

Run:
```bash
cd /home/ivena/coding/quant-stack/quant-research-cn
cp data/quant_cn.duckdb /tmp/claude-1000/-home-ivena/781799a0-6e2a-4979-8b7c-8dfc235853c0/scratchpad/hot_test.duckdb
../quant-research-v1/.venv/bin/python ../scripts/consolidate_raw.py --market cn \
  --hot /tmp/claude-1000/-home-ivena/781799a0-6e2a-4979-8b7c-8dfc235853c0/scratchpad/hot_test.duckdb \
  --staging-dir data/staging 2>&1 | tail -3
../quant-research-v1/.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect("/tmp/claude-1000/-home-ivena/781799a0-6e2a-4979-8b7c-8dfc235853c0/scratchpad/hot_test.duckdb", read_only=True)
print("fetch_state in hot:", con.execute("SELECT market, fetcher, status FROM fetch_state").fetchall())
PY
```
Expected: prints `consolidate cn: merged N rows ...` and the throwaway hot DB now contains the `fetch_state` row(s) from staging.

- [ ] **Step 6: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add scripts/consolidate_raw.py tests/test_consolidate_raw.py
git commit -m "feat(cn): consolidate_raw.py merges per-source staging into hot under write lock"
```

---

## Self-Review

- **Spec coverage (this plan = the data-path slice of the CN spec):**
  - 组件1 `fetch_state` 水位表 → Task 1 (state-only columns; criticality/max-staleness deferred to Plan 2 registry — a deliberate refinement of the spec's table, noted in Global Constraints).
  - 组件2 每源 fetch worker(直写 staging,无锁) → Task 2 (`--source`/`--staging`; writes only the staging DB + its `fetch_state` row; never locks hot).
  - 组件3 consolidate(唯一热库写者,flock 串行) → Task 3 (`connect_write` exclusive lock; ATTACH + INSERT OR REPLACE; merges `fetch_state` too).
  - 组件4 新鲜度门 / 组件5 锁纪律 (pipeline side) / 源注册表 / `--skip-fetch` 转发 → **Plan 2** (explicitly out of scope here).
- **Placeholder scan:** none — every code step shows full code; every run step gives an exact command + expected output.
- **Type consistency:** `record_fetch_state(con, market, fetcher, as_of: NaiveDate, status, rows: usize, error: Option<&str>)` defined in Task 1 and called identically in Task 2. `consolidate_cn(hot_path, staging_paths) -> int` defined in Task 3 and used in its test. `fetch_state` columns `(market, fetcher, as_of, status, row_count, fetched_at, error)` identical across schema (Task 1), helper (Task 1), and the test fixture (Task 3).
- **Decoupling check:** worker writes staging only (no hot lock); consolidate is sole hot writer under fcntl lock — matches the single-writer discipline. Plan delivers a manually-runnable decoupled ingest; nothing in the live pipeline changes yet (no `tasks.yaml`, no `run` body edits).
