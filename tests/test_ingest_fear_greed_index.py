"""Tests for the Fear & Greed ingest script (proxy path only)."""
from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "ingest_fear_greed_index.py"


def _load_module():
    if "ingest_fear_greed_index" in sys.modules:
        return sys.modules["ingest_fear_greed_index"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("ingest_fear_greed_index", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed(path: Path, symbol_series: dict[str, list[tuple[date, float]]]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE prices_daily (symbol VARCHAR, date DATE, close DOUBLE)")
        for sym, points in symbol_series.items():
            for d, close in points:
                con.execute("INSERT INTO prices_daily VALUES (?, ?, ?)", [sym, d.isoformat(), close])
    finally:
        con.close()


class FearGreedProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_rating_thresholds(self) -> None:
        self.assertEqual(self.module._rating_for(10), "Extreme Fear")
        self.assertEqual(self.module._rating_for(30), "Fear")
        self.assertEqual(self.module._rating_for(50), "Neutral")
        self.assertEqual(self.module._rating_for(70), "Greed")
        self.assertEqual(self.module._rating_for(90), "Extreme Greed")

    def test_proxy_produces_score_when_vix_and_spy_present(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            as_of = date(2026, 5, 13)
            # SPY rising trend (greedy)
            spy = [(as_of - timedelta(days=i), 600.0 + (300 - i) * 0.4) for i in range(300, 0, -1)]
            vix = [(as_of - timedelta(days=i), 15.0 + (i % 20) * 0.3) for i in range(300, 0, -1)]
            _seed(db, {"SPY": spy, "^VIX": vix})
            data = self.module._compute_proxy(db, as_of)
            self.assertIsNotNone(data)
            self.assertEqual(data["source"], "proxy")
            self.assertGreaterEqual(data["score"], 0)
            self.assertLessEqual(data["score"], 100)
            self.assertIn("vix", data["components"])
            self.assertIn("spy_vs_ema50", data["components"])
            self.assertIn("spy_5d_return", data["components"])

    def test_proxy_returns_none_when_data_missing(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            con = duckdb.connect(str(db))
            con.execute("CREATE TABLE prices_daily (symbol VARCHAR, date DATE, close DOUBLE)")
            con.close()
            self.assertIsNone(self.module._compute_proxy(db, date(2026, 5, 13)))


if __name__ == "__main__":
    unittest.main()
