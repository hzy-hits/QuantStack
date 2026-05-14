"""Tests for the multi-tenor options radar."""
from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_options_tenor_radar.py"


def _load_module():
    if "score_options_tenor_radar" in sys.modules:
        return sys.modules["score_options_tenor_radar"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_options_tenor_radar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_db(path: Path, chain: list[tuple]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE options_chain_quotes (
                symbol VARCHAR, as_of DATE, expiry DATE, days_to_exp INTEGER,
                current_price DOUBLE, contract_symbol VARCHAR, option_type VARCHAR,
                strike DOUBLE, bid DOUBLE, ask DOUBLE, mid DOUBLE, last_price DOUBLE,
                volume BIGINT, open_interest BIGINT, implied_volatility DOUBLE,
                delta DOUBLE, gamma DOUBLE, theta DOUBLE
            )
            """
        )
        for row in chain:
            con.execute(
                "INSERT INTO options_chain_quotes (symbol, as_of, days_to_exp, current_price, "
                "option_type, strike, volume, open_interest, delta) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
    finally:
        con.close()


def _chain(symbol: str, dte: int, opt_type: str, strike: float, spot: float,
            volume: int, oi: int, delta: float | None,
            as_of: date = date(2026, 5, 13)) -> tuple:
    return (symbol, as_of, dte, spot, opt_type, strike, volume, oi, delta)


class OptionsTenorRadarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_gamma_trap_signal_when_weekly_dominates_monthly(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            universe = Path(tmp) / "u.jsonl"
            universe.write_text(json.dumps({"ticker": "NVDA"}) + "\n", encoding="utf-8")
            # Weekly far-OTM calls dominant (5 DTE)
            chain = [
                _chain("NVDA", 5, "call", 1000.0, 800.0, 6000, 100, 0.10),
                _chain("NVDA", 5, "put",  700.0, 800.0,  300, 500, -0.10),
                # Monthly far-OTM call - tiny relative to weekly
                _chain("NVDA", 30, "call", 1000.0, 800.0, 100, 500, 0.10),
                _chain("NVDA", 30, "put",  700.0, 800.0, 100, 500, -0.10),
            ]
            _seed_db(db, chain)
            target, buckets, _ = self.module.collect_buckets(
                db, {"NVDA"}, as_of=date(2026, 5, 13),
            )
            signals = self.module.detect_cross_tenor_signals(buckets)
            patterns = {s.pattern for s in signals}
            self.assertIn("gamma_trap", patterns)
            gt = next(s for s in signals if s.pattern == "gamma_trap")
            self.assertEqual(gt.symbol, "NVDA")
            self.assertGreater(gt.score, 10)

    def test_bullish_conviction_stack_across_three_tenors(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            universe = Path(tmp) / "u.jsonl"
            universe.write_text(json.dumps({"ticker": "MU"}) + "\n", encoding="utf-8")
            chain = [
                # weekly: call 600, put 200 (ratio 3.0)
                _chain("MU", 5, "call", 850.0, 800.0, 600, 100, 0.30),
                _chain("MU", 5, "put", 750.0, 800.0, 200, 100, -0.30),
                # biweekly: call 400, put 150 (ratio 2.67)
                _chain("MU", 15, "call", 850.0, 800.0, 400, 100, 0.30),
                _chain("MU", 15, "put", 750.0, 800.0, 150, 100, -0.30),
                # monthly: call 300, put 100 (ratio 3.0)
                _chain("MU", 35, "call", 850.0, 800.0, 300, 100, 0.30),
                _chain("MU", 35, "put", 750.0, 800.0, 100, 100, -0.30),
            ]
            _seed_db(db, chain)
            _, buckets, _ = self.module.collect_buckets(
                db, {"MU"}, as_of=date(2026, 5, 13),
            )
            signals = self.module.detect_cross_tenor_signals(buckets)
            patterns = {s.pattern for s in signals if s.symbol == "MU"}
            self.assertIn("bullish_conviction_stack", patterns)

    def test_bearish_stack_when_puts_dominate(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            chain = [
                _chain("FOO", 5, "call", 110.0, 100.0, 100, 100, 0.30),
                _chain("FOO", 5, "put", 90.0, 100.0, 300, 100, -0.30),
                _chain("FOO", 15, "call", 110.0, 100.0, 100, 100, 0.30),
                _chain("FOO", 15, "put", 90.0, 100.0, 250, 100, -0.30),
                _chain("FOO", 35, "call", 110.0, 100.0, 100, 100, 0.30),
                _chain("FOO", 35, "put", 90.0, 100.0, 200, 100, -0.30),
            ]
            _seed_db(db, chain)
            _, buckets, _ = self.module.collect_buckets(
                db, {"FOO"}, as_of=date(2026, 5, 13),
            )
            signals = self.module.detect_cross_tenor_signals(buckets)
            patterns = {s.pattern for s in signals if s.symbol == "FOO"}
            self.assertIn("bearish_stack", patterns)

    def test_insider_tilt_long_dated_calls(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            chain = [
                # Tiny weekly far-OTM call
                _chain("BAR", 5, "call", 120.0, 100.0, 50, 100, 0.10),
                _chain("BAR", 5, "put", 80.0, 100.0, 50, 100, -0.10),
                # Heavy quarterly + half_year far-OTM call (long horizon)
                _chain("BAR", 80, "call", 120.0, 100.0, 600, 100, 0.10),
                _chain("BAR", 80, "put", 80.0, 100.0, 100, 100, -0.10),
                _chain("BAR", 180, "call", 120.0, 100.0, 600, 100, 0.10),
                _chain("BAR", 180, "put", 80.0, 100.0, 100, 100, -0.10),
            ]
            _seed_db(db, chain)
            _, buckets, _ = self.module.collect_buckets(
                db, {"BAR"}, as_of=date(2026, 5, 13),
            )
            signals = self.module.detect_cross_tenor_signals(buckets)
            patterns = {s.pattern for s in signals if s.symbol == "BAR"}
            self.assertIn("insider_tilt_long_dated_calls", patterns)

    def test_tenor_buckets_are_routed_by_dte(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            chain = [
                _chain("ZZZ", 3, "call", 110.0, 100.0, 500, 100, 0.30),
                _chain("ZZZ", 15, "call", 110.0, 100.0, 500, 100, 0.30),
                _chain("ZZZ", 30, "call", 110.0, 100.0, 500, 100, 0.30),
                _chain("ZZZ", 75, "call", 110.0, 100.0, 500, 100, 0.30),
                _chain("ZZZ", 180, "call", 110.0, 100.0, 500, 100, 0.30),
            ]
            _seed_db(db, chain)
            _, buckets, _ = self.module.collect_buckets(
                db, {"ZZZ"}, as_of=date(2026, 5, 13),
            )
            tenors = {b.tenor for b in buckets}
            self.assertEqual(tenors, {"weekly", "biweekly", "monthly", "quarterly", "half_year"})


if __name__ == "__main__":
    unittest.main()
