"""Tests for ingest_satellite_index_prices.py.

Network-dependent yfinance paths are bypassed — we exercise pure helpers
(`_is_nan`, `_maybe_float`, `_maybe_int`) and the upsert routine with a
mock pandas frame, plus the missing-db CLI error path.
"""
from __future__ import annotations

import importlib.util
import io
import math
import sys
import unittest
from contextlib import redirect_stderr
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "ingest_satellite_index_prices.py"


def _load_module():
    if "ingest_satellite_index_prices" in sys.modules:
        return sys.modules["ingest_satellite_index_prices"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("ingest_satellite_index_prices", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SatelliteIngestHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_is_nan_catches_floating_nan_only(self) -> None:
        m = self.module
        self.assertTrue(m._is_nan(float("nan")))
        self.assertFalse(m._is_nan(None))
        self.assertFalse(m._is_nan(0.0))
        self.assertFalse(m._is_nan("string"))

    def test_maybe_float_returns_none_for_invalid(self) -> None:
        m = self.module
        self.assertIsNone(m._maybe_float(None))
        self.assertIsNone(m._maybe_float(float("nan")))
        self.assertEqual(m._maybe_float(3.5), 3.5)

    def test_maybe_int_returns_none_for_invalid(self) -> None:
        m = self.module
        self.assertIsNone(m._maybe_int(None))
        self.assertIsNone(m._maybe_int(float("nan")))
        self.assertEqual(m._maybe_int(42), 42)
        self.assertEqual(m._maybe_int(42.7), 42)


class SatelliteIngestMainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_main_returns_two_when_db_missing(self) -> None:
        m = self.module
        argv = sys.argv[:]
        sys.argv = ["ingest_satellite_index_prices.py", "--us-db", "/nope/missing.duckdb"]
        try:
            err = io.StringIO()
            with redirect_stderr(err):
                rc = m.main()
            self.assertEqual(rc, 2)
            self.assertIn("US db missing", err.getvalue())
        finally:
            sys.argv = argv

    def test_upsert_inserts_and_replaces_on_pk_collision(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "us.duckdb"
            con = duckdb.connect(str(db))
            try:
                m._ensure_prices_daily(con)
                spec = m.SatelliteIndex("^TWII", "^TWII", "TAIEX test", "Taiwan")
                df_v1 = pd.DataFrame(
                    {
                        "Open": [22000.0, 22050.0],
                        "High": [22100.0, 22200.0],
                        "Low": [21900.0, 21950.0],
                        "Close": [22050.0, 22150.0],
                        "Adj Close": [22050.0, 22150.0],
                        "Volume": [1_000_000, 1_500_000],
                    },
                    index=pd.to_datetime([date(2026, 5, 12), date(2026, 5, 13)]),
                )
                self.assertEqual(m._upsert(con, spec, df_v1, date(2026, 5, 13)), 2)
                # second run with adjusted close — PK should replace.
                df_v2 = pd.DataFrame(
                    {
                        "Open": [22000.0],
                        "High": [22100.0],
                        "Low": [21900.0],
                        "Close": [22100.0],  # adjusted
                        "Adj Close": [22100.0],
                        "Volume": [1_000_000],
                    },
                    index=pd.to_datetime([date(2026, 5, 12)]),
                )
                m._upsert(con, spec, df_v2, date(2026, 5, 13))
                rows = con.execute(
                    "SELECT date, close FROM prices_daily WHERE symbol=? ORDER BY date",
                    ["^TWII"],
                ).fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][1], 22100.0)  # replaced
                self.assertEqual(rows[1][1], 22150.0)  # preserved
            finally:
                con.close()

    def test_upsert_skips_future_dates(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "us.duckdb"
            con = duckdb.connect(str(db))
            try:
                m._ensure_prices_daily(con)
                spec = m.SatelliteIndex("EWJ", "EWJ", "EWJ ETF", "Japan")
                df = pd.DataFrame(
                    {
                        "Open": [70.0, 71.0],
                        "High": [70.5, 71.5],
                        "Low": [69.5, 70.5],
                        "Close": [70.2, 71.2],
                        "Adj Close": [70.2, 71.2],
                        "Volume": [10_000, 11_000],
                    },
                    index=pd.to_datetime([date(2026, 5, 13), date(2026, 5, 14)]),
                )
                inserted = m._upsert(con, spec, df, date(2026, 5, 13))
                self.assertEqual(inserted, 1)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
