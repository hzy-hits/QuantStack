"""Tests for ingest_wedge_instrument_prices.py.

The yfinance network path is not exercised. We verify the script's CLI
contract: missing-db returns 2, the prices_daily schema is created on
success, and the bundle of wedge symbols covers the framework's required
rates / credit / banks / housing buckets.
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

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "ingest_wedge_instrument_prices.py"


def _load_module():
    if "ingest_wedge_instrument_prices" in sys.modules:
        return sys.modules["ingest_wedge_instrument_prices"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("ingest_wedge_instrument_prices", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class WedgeIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_symbol_universe_covers_all_required_buckets(self) -> None:
        symbols = set(self.module.WEDGE_SYMBOLS)
        # rates
        self.assertTrue({"TLT", "IEF", "SHY", "TBT", "^TNX"}.issubset(symbols))
        # credit
        self.assertTrue({"HYG", "JNK", "LQD"}.issubset(symbols))
        # banks (Canadian + XLF)
        self.assertTrue({"XLF", "BMO", "RY", "TD", "BNS", "CM"}.issubset(symbols))
        # housing
        self.assertTrue({"XHB", "ITB"}.issubset(symbols))

    def test_ingest_returns_two_when_db_missing(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            err = io.StringIO()
            with redirect_stderr(err):
                rc = m._ingest(Path(tmpdir) / "nope.duckdb", date(2026, 5, 13))
            self.assertEqual(rc, 2)
            self.assertIn("US db missing", err.getvalue())

    def test_main_returns_two_when_db_missing(self) -> None:
        m = self.module
        argv = sys.argv[:]
        sys.argv = ["ingest_wedge_instrument_prices.py", "--us-db", "/nope/missing.duckdb"]
        try:
            err = io.StringIO()
            with redirect_stderr(err):
                rc = m.main()
            self.assertEqual(rc, 2)
        finally:
            sys.argv = argv

    def test_ingest_creates_prices_daily_table_on_first_run(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "us.duckdb"
            # Pre-create an empty DB so the existence check passes,
            # but DON'T create the table — the ingest call should make it.
            duckdb.connect(str(db)).close()
            # Patch yfinance with a stub that returns an empty frame so we
            # avoid the network call and exercise the schema-init path only.
            import pandas as pd

            class _StubTicker:
                def history(self, **_kwargs):  # noqa: D401
                    return pd.DataFrame()

            class _StubYf:
                Ticker = staticmethod(lambda symbol: _StubTicker())

            sys.modules["yfinance"] = _StubYf  # type: ignore[assignment]
            try:
                rc = m._ingest(db, date(2026, 5, 13))
            finally:
                sys.modules.pop("yfinance", None)
            # All symbols returned empty → rc = 1 (every fetch is a "failure").
            self.assertEqual(rc, 1)
            con = duckdb.connect(str(db))
            try:
                tables = {row[0] for row in con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                ).fetchall()}
                self.assertIn("prices_daily", tables)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
