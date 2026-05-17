"""Tests for the capitulation convex radar (upside mirror of victim_put)."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_capitulation_convex_radar.py"


def _load_module():
    if "score_capitulation_convex_radar" in sys.modules:
        return sys.modules["score_capitulation_convex_radar"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_capitulation_convex_radar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class OversoldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_oversold_via_ema50_distance(self) -> None:
        oversold, metrics = self.m._is_oversold(80.0, 100.0, 100.0)
        self.assertTrue(oversold)
        self.assertEqual(metrics["px_vs_ema50_pct"], -20.0)

    def test_oversold_via_drawdown(self) -> None:
        # 5% below EMA50 (not oversold by EMA) but 30% off the 60d high.
        oversold, metrics = self.m._is_oversold(70.0, 73.5, 100.0)
        self.assertTrue(oversold)
        self.assertEqual(metrics["drawdown_60d_pct"], -30.0)

    def test_not_oversold_near_highs(self) -> None:
        oversold, _ = self.m._is_oversold(98.0, 100.0, 100.0)
        self.assertFalse(oversold)

    def test_ema_basic(self) -> None:
        self.assertIsNone(self.m._ema([1, 2, 3], 50))
        self.assertIsNotNone(self.m._ema(list(range(60)), 50))


def _seed_prices(con, symbol, closes, start=date(2026, 1, 1)):
    con.execute(
        "CREATE TABLE IF NOT EXISTS prices_daily (symbol VARCHAR, date DATE, "
        "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT, adj_close DOUBLE)"
    )
    for i, c in enumerate(closes):
        d = date.fromordinal(start.toordinal() + i)
        con.execute("INSERT INTO prices_daily VALUES (?,?,?,?,?,?,?,?)",
                    [symbol, d.isoformat(), c, c, c, c, 1_000_000, c])


class ConvexValueBuyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def test_oversold_production_name_is_flagged(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "u.duckdb"
            con = duckdb.connect(str(db))
            # 70 days: high near 200 early, crashes to ~120 → deep drawdown.
            closes = [200.0] * 35 + [c for c in range(195, 125, -2)]
            _seed_prices(con, "AAOI", closes[:70])
            con.close()
            con = duckdb.connect(str(db), read_only=True)
            try:
                production = {"AAOI": {"company": "Applied Opto",
                                       "evidence_state": "原文已证明: x"}}
                rows = self.m.build_convex_value_buys(con, production, date(2026, 3, 11))
            finally:
                con.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "AAOI")
            self.assertEqual(rows[0]["convexity"], "convex")

    def test_name_near_highs_is_not_flagged(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "u.duckdb"
            con = duckdb.connect(str(db))
            _seed_prices(con, "NVDA", [100.0 + i * 0.1 for i in range(70)])
            con.close()
            con = duckdb.connect(str(db), read_only=True)
            try:
                production = {"NVDA": {"company": "NVIDIA",
                                       "evidence_state": "原文已证明: x"}}
                rows = self.m.build_convex_value_buys(con, production, date(2026, 3, 11))
            finally:
                con.close()
            self.assertEqual(rows, [])


class LeapsCallTests(unittest.TestCase):
    def setUp(self) -> None:
        self.m = _load_module()

    def _seed_chain(self, con, rows):
        con.execute(
            "CREATE TABLE options_chain_quotes (symbol VARCHAR, as_of DATE, "
            "expiry VARCHAR, days_to_exp INTEGER, current_price DOUBLE, "
            "contract_symbol VARCHAR, option_type VARCHAR, strike DOUBLE, "
            "bid DOUBLE, ask DOUBLE, mid DOUBLE, last_price DOUBLE, "
            "volume BIGINT, open_interest BIGINT, implied_volatility DOUBLE, delta DOUBLE)"
        )
        for r in rows:
            con.execute(
                "INSERT INTO options_chain_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", r)

    def test_long_dated_call_in_delta_band_is_picked(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "u.duckdb"
            con = duckdb.connect(str(db))
            self._seed_chain(con, [
                # LEAPS call, delta 0.40, liquid → picked
                ("MU", date(2026, 5, 15), "2027-01-15", 245, 100.0,
                 "MU270115C00100000", "call", 100.0, 9.0, 9.4, 9.2, 9.1,
                 500, 800, 0.55, 0.40),
                # short-dated call → excluded (DTE < 221)
                ("MU", date(2026, 5, 15), "2026-06-19", 35, 100.0,
                 "MU260619C00100000", "call", 100.0, 4.0, 4.2, 4.1, 4.0,
                 900, 900, 0.60, 0.45),
                # LEAPS but delta too high (deep ITM) → excluded
                ("MU", date(2026, 5, 15), "2027-01-15", 245, 100.0,
                 "MU270115C00060000", "call", 60.0, 42.0, 43.0, 42.5, 42.0,
                 100, 300, 0.50, 0.85),
            ])
            con.close()
            con = duckdb.connect(str(db), read_only=True)
            try:
                calls = self.m.find_leaps_calls(con, "MU", date(2026, 5, 15))
            finally:
                con.close()
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["contract_symbol"], "MU270115C00100000")
            self.assertEqual(calls[0]["convexity"], "convex")
            self.assertEqual(calls[0]["premium_est"], 9.2)


if __name__ == "__main__":
    unittest.main()
