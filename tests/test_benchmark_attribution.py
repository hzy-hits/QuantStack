"""Unit tests for the benchmark snapshot wired into the daily report."""
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


def _seed_us_prices(path: Path, symbol: str, base: float, days: int, as_of: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS prices_daily (symbol VARCHAR, date DATE, close DOUBLE)"
        )
        for offset in range(days, 0, -1):
            d = as_of - timedelta(days=offset - 1)
            close = base * (1.0 + 0.001 * (days - offset))
            con.execute(
                "INSERT INTO prices_daily VALUES (?, ?, ?)",
                [symbol, d.isoformat(), close],
            )
    finally:
        con.close()


def _seed_cn_prices(path: Path, ts_code: str, base: float, days: int, as_of: date) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE IF NOT EXISTS prices (ts_code VARCHAR, trade_date DATE, close DOUBLE)")
        for offset in range(days, 0, -1):
            d = as_of - timedelta(days=offset - 1)
            close = base * (1.0 + 0.002 * (days - offset))
            con.execute(
                "INSERT INTO prices VALUES (?, ?, ?)",
                [ts_code, d.isoformat(), close],
            )
    finally:
        con.close()


class BenchmarkAttributionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_build_returns_rows_per_market(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            us_db = root / "us.duckdb"
            cn_db = root / "cn.duckdb"
            as_of = date(2026, 5, 13)
            _seed_us_prices(us_db, "SPY", 600.0, 120, as_of)
            _seed_us_prices(us_db, "QQQ", 500.0, 120, as_of)
            _seed_us_prices(us_db, "^TWII", 40000.0, 120, as_of)
            _seed_us_prices(us_db, "EWT", 90.0, 120, as_of)
            _seed_cn_prices(cn_db, "000300.SH", 4000.0, 120, as_of)

            data = self.module.build_benchmark_attribution(us_db, cn_db, as_of)
            us_rows = {row["symbol"]: row for row in data["us"]["rows"]}
            cn_rows = {row["symbol"]: row for row in data["cn"]["rows"]}
            sat_rows = {row["symbol"]: row for row in data["satellite"]["rows"]}

            self.assertEqual(us_rows["SPY"]["status"], "ok")
            self.assertEqual(us_rows["QQQ"]["status"], "ok")
            self.assertEqual(us_rows["SMH"]["status"], "missing_data")
            self.assertIn("SMH", data["us"]["missing"])
            self.assertEqual(cn_rows["000300.SH"]["status"], "ok")
            self.assertEqual(cn_rows["399001.SZ"]["status"], "missing_data")
            self.assertEqual(sat_rows["^TWII"]["status"], "ok")
            self.assertEqual(sat_rows["EWT"]["status"], "ok")
            self.assertEqual(sat_rows["^N225"]["status"], "missing_data")
            self.assertIsNotNone(us_rows["SPY"]["ret_5d_pct"])
            self.assertIsNotNone(us_rows["SPY"]["ret_ytd_pct"])

    def test_ai_book_attribution_computes_alpha_beta_ir(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            us_db = root / "us.duckdb"
            cn_db = root / "cn.duckdb"
            as_of = date(2026, 5, 13)
            _seed_us_prices(us_db, "SPY", 600.0, 120, as_of)
            _seed_us_prices(us_db, "QQQ", 500.0, 120, as_of)
            _seed_us_prices(us_db, "SMH", 540.0, 120, as_of)
            _seed_us_prices(us_db, "IWM", 280.0, 120, as_of)
            _seed_us_prices(us_db, "DIA", 480.0, 120, as_of)
            _seed_us_prices(us_db, "AIONE", 100.0, 120, as_of)

            data = self.module.build_benchmark_attribution(
                us_db,
                cn_db,
                as_of,
                us_basket=["AIONE"],
                cn_basket=[],
            )
            us_book = (data.get("ai_book") or {}).get("us") or {}
            self.assertEqual(us_book["status"], "ok")
            rows = {(row["benchmark"], row["window"]): row for row in us_book["rows"]}
            self.assertIn(("SPY", "20d"), rows)
            spy20 = rows[("SPY", "20d")]
            self.assertGreater(spy20["n"], 10)
            self.assertIsNotNone(spy20["beta"])
            self.assertIsNotNone(spy20["information_ratio"])
            cn_book = (data.get("ai_book") or {}).get("cn") or {}
            self.assertEqual(cn_book["status"], "no_basket")

    def test_ai_book_risk_block_includes_drawdown_atr_corr(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            us_db = root / "us.duckdb"
            cn_db = root / "cn.duckdb"
            as_of = date(2026, 5, 13)
            _seed_us_prices(us_db, "SPY", 600.0, 120, as_of)
            _seed_us_prices(us_db, "AI1", 100.0, 120, as_of)
            _seed_us_prices(us_db, "AI2", 120.0, 120, as_of)

            data = self.module.build_benchmark_attribution(
                us_db,
                cn_db,
                as_of,
                us_basket=["AI1", "AI2"],
                cn_basket=[],
            )
            risk = ((data.get("ai_book") or {}).get("us") or {}).get("risk") or {}
            self.assertIn("max_drawdown_20d_pct", risk)
            self.assertIn("avg_atr20_pct", risk)
            self.assertIn("pairwise_corr_20d", risk)
            self.assertGreaterEqual(risk["pairwise_corr_20d"].get("n_pairs") or 0, 1)

    def test_renderer_handles_empty_ai_book(self) -> None:
        payload = {
            "benchmark_attribution": {
                "ai_book": {
                    "us": {"status": "no_basket", "rows": [], "basket_size": 0},
                }
            }
        }
        rendered = "\n".join(self.module.render_ai_book_attribution_section(payload, "US"))
        self.assertIn("US AI Book vs Benchmark", rendered)
        self.assertIn("production basket 为空", rendered)

    def test_satellite_renderer_uses_satellite_section(self) -> None:
        payload = {
            "benchmark_attribution": {
                "us": {"rows": [], "missing": []},
                "cn": {"rows": [], "missing": []},
                "satellite": {
                    "rows": [
                        {
                            "symbol": "^TWII",
                            "label": "^TWII (TAIEX 台湾加权)",
                            "status": "ok",
                            "latest_close": 41000.0,
                            "latest_date": "2026-05-13",
                            "ret_1d_pct": -0.5,
                            "ret_5d_pct": 1.2,
                            "ret_20d_pct": 5.4,
                            "ret_60d_pct": 12.3,
                            "ret_ytd_pct": 40.0,
                        }
                    ],
                    "missing": [],
                },
            }
        }
        rendered = "\n".join(self.module.render_benchmark_attribution_section(payload, "SATELLITE"))
        self.assertIn("Satellite Benchmark Snapshot", rendered)
        self.assertIn("^TWII (TAIEX 台湾加权)", rendered)
        self.assertIn("+40.00%", rendered)

    def test_render_includes_label_and_pct(self) -> None:
        payload = {
            "benchmark_attribution": {
                "us": {
                    "rows": [
                        {
                            "symbol": "SPY",
                            "label": "SPY (S&P 500)",
                            "status": "ok",
                            "latest_close": 600.0,
                            "latest_date": "2026-05-13",
                            "ret_1d_pct": 0.5,
                            "ret_5d_pct": 1.2,
                            "ret_20d_pct": 3.4,
                            "ret_60d_pct": 6.7,
                            "ret_ytd_pct": 8.1,
                        }
                    ],
                    "missing": [],
                },
                "cn": {"rows": [], "missing": []},
            }
        }
        rendered = "\n".join(self.module.render_benchmark_attribution_section(payload, "US"))
        self.assertIn("US Benchmark Snapshot", rendered)
        self.assertIn("SPY (S&P 500)", rendered)
        self.assertIn("+1.20%", rendered)


if __name__ == "__main__":
    unittest.main()
