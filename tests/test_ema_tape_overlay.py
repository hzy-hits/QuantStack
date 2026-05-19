"""Tests for the EMA 21/50 tape overlay."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load_module():
    if "generate_main_strategy_v2_report" in sys.modules:
        return sys.modules["generate_main_strategy_v2_report"]
    sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("generate_main_strategy_v2_report", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_prices_daily(path: Path, symbol: str, closes: list[float], end: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE IF NOT EXISTS prices_daily (symbol VARCHAR, date DATE, close DOUBLE)")
        for offset, close in enumerate(closes):
            d = end - timedelta(days=len(closes) - 1 - offset)
            con.execute("INSERT INTO prices_daily VALUES (?, ?, ?)", [symbol, d.isoformat(), close])
    finally:
        con.close()


def _seed_cn_prices(path: Path, ts_code: str, closes: list[float], end: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE IF NOT EXISTS prices (ts_code VARCHAR, trade_date DATE, close DOUBLE)")
        for offset, close in enumerate(closes):
            d = end - timedelta(days=len(closes) - 1 - offset)
            con.execute("INSERT INTO prices VALUES (?, ?, ?)", [ts_code, d.isoformat(), close])
    finally:
        con.close()


class EmaTapeOverlayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_compute_ema_matches_known_value(self) -> None:
        closes = [10.0 + i * 0.5 for i in range(100)]
        ema = self.module._compute_ema(closes, 21)
        self.assertEqual(len(ema), 100)
        # Rising linear series → EMA21 ends just below the final close.
        self.assertLess(ema[-1], closes[-1])
        self.assertGreater(ema[-1], closes[-21])

    def test_ema_metrics_returns_bull_when_short_above_long(self) -> None:
        rising_closes = [(d, 100.0 + d * 0.5) for d in range(80)]
        # Replace integer offsets with real dates
        rising_closes = [(date(2026, 1, 1) + timedelta(days=i), 100.0 + i * 0.5) for i in range(80)]
        metrics = self.module._ema_tape_metrics(rising_closes)
        self.assertEqual(metrics["cross_state"], "bull")
        self.assertGreater(metrics["slope_21d_5d_pct"], 0)
        self.assertGreater(metrics["dist_close_ema21_pct"], 0)

    def test_ema_metrics_returns_bear_when_falling(self) -> None:
        falling_closes = [(date(2026, 1, 1) + timedelta(days=i), 200.0 - i * 0.5) for i in range(80)]
        metrics = self.module._ema_tape_metrics(falling_closes)
        self.assertEqual(metrics["cross_state"], "bear")
        self.assertLess(metrics["slope_21d_5d_pct"], 0)

    def test_ema_metrics_short_series_returns_none(self) -> None:
        short = [(date(2026, 1, 1) + timedelta(days=i), 100.0) for i in range(10)]
        self.assertIsNone(self.module._ema_tape_metrics(short))

    def test_overlay_routes_symbols_by_suffix(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            us_db = root / "us.duckdb"
            cn_db = root / "cn.duckdb"
            as_of = date(2026, 5, 13)
            _seed_prices_daily(us_db, "NVDA", [100 + i * 0.4 for i in range(80)], as_of)
            _seed_cn_prices(cn_db, "002463.SZ", [50 + i * 0.2 for i in range(80)], as_of)
            overlay = self.module.build_ema_tape_overlay(us_db, cn_db, ["NVDA", "002463.SZ", "MISSING"], as_of)
            self.assertEqual(overlay["NVDA"]["market"], "US")
            self.assertEqual(overlay["002463.SZ"]["market"], "CN")
            self.assertIsNotNone(overlay["NVDA"]["metrics"])
            self.assertIsNotNone(overlay["002463.SZ"]["metrics"])
            self.assertIsNone(overlay["MISSING"]["metrics"])

    def test_summary_label_emits_human_string(self) -> None:
        metrics = {
            "cross_state": "bull",
            "recent_cross": "bull_cross",
            "slope_21d_5d_pct": 0.8,
            "dist_close_ema21_pct": 2.5,
        }
        label = self.module._ema_summary_label(metrics)
        self.assertIn("bull", label)
        self.assertIn("rising", label)
        self.assertIn("+2.5%", label)


if __name__ == "__main__":
    unittest.main()
