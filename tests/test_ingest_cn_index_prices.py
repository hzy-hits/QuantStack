"""Tests for ingest_cn_index_prices.py.

Network paths (AKShare) are not exercised — those would require network
access and aren't reliable in CI. Instead we test the pure-Python pieces:
spec resolution, the upsert math (pre_close / change / pct_chg derivation),
and CLI error paths.
"""
from __future__ import annotations

import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stderr
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "ingest_cn_index_prices.py"


def _load_module():
    if "ingest_cn_index_prices" in sys.modules:
        return sys.modules["ingest_cn_index_prices"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("ingest_cn_index_prices", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CnIndexIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_resolve_specs_drops_unknown_codes_and_keeps_known(self) -> None:
        m = self.module
        specs = m._resolve_specs(["000001.SH", "BOGUS.XX", "399006.SZ"])
        canonicals = [s.canonical for s in specs]
        self.assertEqual(canonicals, ["000001.SH", "399006.SZ"])

    def test_resolve_specs_returns_empty_for_only_unknown(self) -> None:
        m = self.module
        self.assertEqual(m._resolve_specs(["BAD.XX"]), [])

    def test_main_returns_two_when_db_missing(self) -> None:
        m = self.module
        argv = sys.argv[:]
        sys.argv = ["ingest_cn_index_prices.py", "--cn-db", "/nope/missing.duckdb"]
        try:
            err = io.StringIO()
            with redirect_stderr(err):
                rc = m.main()
            self.assertEqual(rc, 2)
            self.assertIn("CN db missing", err.getvalue())
        finally:
            sys.argv = argv

    def test_upsert_derives_pre_close_change_and_pct_chg(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "cn.duckdb"
            con = duckdb.connect(str(db))
            try:
                m._ensure_prices_table(con)
                spec = m.IndexSpec("000001.SH", "sh000001", "test")
                df = pd.DataFrame(
                    {
                        "date": [date(2026, 5, 11), date(2026, 5, 12), date(2026, 5, 13)],
                        "open": [3500.0, 3510.0, 3540.0],
                        "high": [3520.0, 3550.0, 3560.0],
                        "low": [3490.0, 3500.0, 3530.0],
                        "close": [3510.0, 3540.0, 3550.0],
                        "volume": [1.0e9, 1.1e9, 1.2e9],
                    }
                )
                inserted = m._upsert_index_rows(con, spec, df, date(2026, 5, 13), 30)
                self.assertEqual(inserted, 3)
                rows = con.execute(
                    "SELECT trade_date, close, pre_close, change, ROUND(pct_chg, 4) "
                    "FROM prices WHERE ts_code = ? ORDER BY trade_date",
                    ["000001.SH"],
                ).fetchall()
                self.assertEqual(rows[0][2], None)  # first row: no pre_close
                self.assertEqual(rows[1][1], 3540.0)
                self.assertEqual(rows[1][2], 3510.0)
                self.assertAlmostEqual(rows[1][3], 30.0, places=3)
                # 30/3510*100 ≈ 0.8547
                self.assertAlmostEqual(rows[1][4], 0.8547, places=3)
            finally:
                con.close()

    def test_upsert_is_idempotent_under_replace(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "cn.duckdb"
            con = duckdb.connect(str(db))
            try:
                m._ensure_prices_table(con)
                spec = m.IndexSpec("000001.SH", "sh000001", "test")
                df = pd.DataFrame(
                    {
                        "date": [date(2026, 5, 12), date(2026, 5, 13)],
                        "open": [3510.0, 3540.0],
                        "high": [3550.0, 3560.0],
                        "low": [3500.0, 3530.0],
                        "close": [3540.0, 3550.0],
                        "volume": [1.0e9, 1.0e9],
                    }
                )
                m._upsert_index_rows(con, spec, df, date(2026, 5, 13), 30)
                m._upsert_index_rows(con, spec, df, date(2026, 5, 13), 30)
                count = con.execute(
                    "SELECT COUNT(*) FROM prices WHERE ts_code = ?", ["000001.SH"]
                ).fetchone()[0]
                self.assertEqual(count, 2)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
