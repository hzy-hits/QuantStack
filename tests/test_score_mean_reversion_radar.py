"""Tests for the US top-100 mean-reversion radar."""
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
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_mean_reversion_radar.py"


def _load_module():
    if "score_mean_reversion_radar" in sys.modules:
        return sys.modules["score_mean_reversion_radar"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_mean_reversion_radar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_db(path: Path, profiles: list[tuple[str, float]], price_series: dict[str, list[tuple[date, float]]]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE company_profile (
                symbol VARCHAR, as_of DATE, company_name VARCHAR, sector VARCHAR,
                market_cap DOUBLE
            )
            """
        )
        for symbol, mcap in profiles:
            con.execute(
                "INSERT INTO company_profile VALUES (?, ?, ?, ?, ?)",
                [symbol, "2026-05-13", f"{symbol} Corp", "Sector", mcap],
            )
        con.execute("CREATE TABLE prices_daily (symbol VARCHAR, date DATE, close DOUBLE)")
        for symbol, series in price_series.items():
            for d, close in series:
                con.execute("INSERT INTO prices_daily VALUES (?, ?, ?)", [symbol, d.isoformat(), close])
    finally:
        con.close()


def _trend(start: float, change_per_day: float, n: int, end: date) -> list[tuple[date, float]]:
    return [(end - timedelta(days=n - 1 - i), start + change_per_day * i) for i in range(n)]


class MeanReversionRadarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_lagging_stock_flagged_when_market_up(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "us.duckdb"
            ai_universe = root / "universe.jsonl"
            ai_universe.write_text(
                json.dumps({"ticker": "LAG", "company": "Laggard Inc"}) + "\n",
                encoding="utf-8",
            )
            as_of = date(2026, 5, 13)
            # Linear price growth yields shrinking percent returns; bump the
            # benchmark daily delta so SPY 5d return crosses the +1% threshold.
            spy = _trend(600, 2.0, 80, as_of)
            qqq = _trend(500, 2.0, 80, as_of)
            # LAG falling fast; price below EMA21 and lagging the market.
            lag = _trend(120, -1.5, 80, as_of)
            # WIN tracking market.
            win = _trend(100, 1.5, 80, as_of)
            _seed_db(
                db,
                profiles=[("LAG", 50_000.0), ("WIN", 40_000.0)],
                price_series={"LAG": lag, "WIN": win, "SPY": spy, "QQQ": qqq},
            )
            rows = self.module.build_radar(us_db=db, ai_universe_path=ai_universe, as_of=as_of, top_n=10)
            by_symbol = {r.symbol: r for r in rows}
            self.assertIn("LAG", by_symbol)
            self.assertTrue(by_symbol["LAG"].is_mean_reversion_candidate)
            self.assertFalse(by_symbol["WIN"].is_mean_reversion_candidate)
            self.assertTrue(by_symbol["LAG"].in_ai_universe)
            self.assertFalse(by_symbol["WIN"].in_ai_universe)

    def test_render_markdown_includes_candidate_section(self) -> None:
        sample = self.module.RadarRow(
            rank=1,
            symbol="LAG",
            company_name="Laggard Inc",
            sector="Banking",
            market_cap_b=200.0,
            latest_close=80.0,
            ret_5d_pct=-3.5,
            ret_20d_pct=-6.0,
            ema21=85.0,
            ema50=90.0,
            slope_ema21_5d_pct=-1.5,
            dist_close_ema21_pct=-5.9,
            dist_close_ema50_pct=-11.1,
            in_ai_universe=True,
            is_mean_reversion_candidate=True,
            reasons=["lagging_market_5d:-3.5%", "below_ema21:-5.9%"],
        )
        md = self.module.render_markdown([sample], "2026-05-13")
        self.assertIn("US Top-100 Mean-Reversion Radar", md)
        self.assertIn("LAG", md)
        self.assertIn("lagging_market_5d", md)


if __name__ == "__main__":
    unittest.main()
