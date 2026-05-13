"""Tests for the promotion-history backtest."""
from __future__ import annotations

import csv
import importlib.util
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

STACK_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = STACK_ROOT / "scripts" / "backtest_promotion_history.py"


def _load_module():
    if "backtest_promotion_history" in sys.modules:
        return sys.modules["backtest_promotion_history"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("backtest_promotion_history", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


HISTORY_HEADERS = [
    "as_of",
    "primary_ticker",
    "ticker_field",
    "company",
    "asset_pool",
    "market_country",
    "bfs_depth",
    "module",
    "priority_tier",
    "readiness_tier",
    "readiness_score",
    "recommendation",
    "rationale",
    "counterevidence",
]


def _write_history(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in HISTORY_HEADERS})


def _hist_row(as_of: str, ticker: str, rec: str = "promote_now") -> dict[str, str]:
    return {
        "as_of": as_of,
        "primary_ticker": ticker,
        "ticker_field": ticker,
        "company": f"{ticker} Inc",
        "asset_pool": "美国资产池",
        "market_country": "US",
        "bfs_depth": "D2",
        "module": "AI infra",
        "priority_tier": "P0",
        "readiness_tier": "ready_for_promotion",
        "readiness_score": "1.000",
        "recommendation": rec,
        "rationale": "evidence_state contains 原文已证明",
        "counterevidence": "ASIC",
    }


def _seed_prices(path: Path, series: dict[str, list[tuple[date, float]]]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE prices_daily (symbol VARCHAR, date DATE, close DOUBLE)")
        for symbol, points in series.items():
            for d, close in points:
                con.execute("INSERT INTO prices_daily VALUES (?, ?, ?)", [symbol, d.isoformat(), close])
    finally:
        con.close()


class PromotionBacktestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_aggregate_returns_when_forward_data_exists(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = root / "history.csv"
            us_db = root / "us.duckdb"
            anchor = date(2026, 1, 5)
            # Ticker outperforms SPY at +0.5%/d vs +0.2%/d
            ticker_series = [(anchor + timedelta(days=i), 100.0 * (1.005 ** i)) for i in range(80)]
            spy_series = [(anchor + timedelta(days=i), 600.0 * (1.002 ** i)) for i in range(80)]
            _seed_prices(us_db, {"AIINC": ticker_series, "SPY": spy_series})
            _write_history(history, [_hist_row(anchor.isoformat(), "AIINC")])
            rows = self.module.backtest(history, us_db)
            self.assertEqual(len(rows), 1)
            metrics_5d = rows[0].returns["5d"]
            self.assertIsNotNone(metrics_5d["ticker_ret_pct"])
            self.assertIsNotNone(metrics_5d["spy_ret_pct"])
            self.assertIsNotNone(metrics_5d["active_ret_pct"])
            self.assertGreater(metrics_5d["active_ret_pct"], 0)  # ticker beat SPY

    def test_missing_forward_data_returns_nones(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = root / "history.csv"
            us_db = root / "us.duckdb"
            # Only one bar — no forward window available
            anchor = date(2026, 5, 13)
            _seed_prices(us_db, {"AIINC": [(anchor, 100.0)], "SPY": [(anchor, 600.0)]})
            _write_history(history, [_hist_row(anchor.isoformat(), "AIINC")])
            rows = self.module.backtest(history, us_db)
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0].returns["5d"]["ticker_ret_pct"])
            self.assertIsNone(rows[0].returns["20d"]["active_ret_pct"])

    def test_only_promote_now_rows_used(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            history = root / "history.csv"
            us_db = root / "us.duckdb"
            anchor = date(2026, 1, 5)
            series = [(anchor + timedelta(days=i), 100.0 + i) for i in range(80)]
            spy = [(anchor + timedelta(days=i), 600.0 + i) for i in range(80)]
            _seed_prices(us_db, {"AIINC": series, "WAITCO": series, "SPY": spy})
            _write_history(
                history,
                [
                    _hist_row(anchor.isoformat(), "AIINC", rec="promote_now"),
                    _hist_row(anchor.isoformat(), "WAITCO", rec="watch_with_review"),
                ],
            )
            rows = self.module.backtest(history, us_db)
            self.assertEqual({r.primary_ticker for r in rows}, {"AIINC"})

    def test_aggregate_summary_computes_hit_rate_and_ir(self) -> None:
        sample_returns = [
            {"5d": {"active_ret_pct": 1.0}},
            {"5d": {"active_ret_pct": 2.0}},
            {"5d": {"active_ret_pct": -1.0}},
            {"5d": {"active_ret_pct": None}},
        ]
        # Build minimal BacktestRow shells for aggregator.
        backtest_rows = [
            self.module.BacktestRow(
                as_of="2026-01-05",
                primary_ticker=f"T{i}",
                company="",
                asset_pool="",
                readiness_tier="",
                base_close=None,
                spy_base_close=None,
                returns=ret,
            )
            for i, ret in enumerate(sample_returns)
        ]
        agg = self.module._aggregate(backtest_rows, 5)
        self.assertEqual(agg["n"], 3)
        self.assertAlmostEqual(agg["mean_active_pct"], (1.0 + 2.0 - 1.0) / 3.0, places=2)
        self.assertAlmostEqual(agg["hit_rate_pct"], 200.0 / 3.0, places=1)
        self.assertIsNotNone(agg["ir"])


if __name__ == "__main__":
    unittest.main()
