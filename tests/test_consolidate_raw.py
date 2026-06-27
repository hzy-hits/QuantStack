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
