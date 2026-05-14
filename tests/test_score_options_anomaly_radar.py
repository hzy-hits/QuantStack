"""Tests for the US options anomaly radar."""
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
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_options_anomaly_radar.py"


def _load_module():
    if "score_options_anomaly_radar" in sys.modules:
        return sys.modules["score_options_anomaly_radar"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_options_anomaly_radar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_db(path: Path, chain: list[tuple], sentiment: list[tuple]) -> None:
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
                "INSERT INTO options_chain_quotes (symbol, as_of, expiry, days_to_exp, current_price, "
                "contract_symbol, option_type, strike, volume, open_interest, delta) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
        con.execute(
            """
            CREATE TABLE options_sentiment (
                symbol VARCHAR, as_of DATE, pc_ratio_z DOUBLE, skew_z DOUBLE,
                vrp DOUBLE, iv_ann DOUBLE, rv_ann DOUBLE, vrp_z DOUBLE,
                pc_ratio_raw DOUBLE, skew_raw DOUBLE, computed_at TIMESTAMP
            )
            """
        )
        for row in sentiment:
            con.execute(
                "INSERT INTO options_sentiment (symbol, as_of, pc_ratio_raw, pc_ratio_z, skew_z) VALUES (?, ?, ?, ?, ?)",
                row,
            )
    finally:
        con.close()


class OptionsAnomalyRadarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_far_otm_call_volume_drives_squeeze_score(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "us.duckdb"
            universe = root / "ai.jsonl"
            universe.write_text(json.dumps({"ticker": "NVDA"}) + "\n", encoding="utf-8")
            as_of = date(2026, 5, 13)
            chain = [
                # Near-money call (NOT far-OTM) — should not contribute to squeeze
                ("NVDA", as_of, date(2026, 6, 1), 20, 800.0, "NVDA-1", "call", 805.0, 500, 1000, 0.50),
                # Far-OTM call: delta 0.10 → squeeze contributor
                ("NVDA", as_of, date(2026, 6, 1), 20, 800.0, "NVDA-2", "call", 900.0, 5000, 100, 0.10),
                # Far-OTM put: delta -0.10 → pressure contributor
                ("NVDA", as_of, date(2026, 6, 1), 20, 800.0, "NVDA-3", "put", 700.0, 800, 200, -0.10),
            ]
            sentiment = [("NVDA", as_of, 0.40, -0.80, -1.50)]  # pc_z low bullish, skew z low (calls bid)
            _seed_db(db, chain, sentiment)
            target, rows = self.module.build_radar(
                us_db=db, ai_universe_path=universe, as_of=as_of,
            )
            self.assertEqual(target, as_of)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row.symbol, "NVDA")
            self.assertEqual(row.far_otm_call_volume, 5000)
            self.assertEqual(row.far_otm_put_volume, 800)
            self.assertGreater(row.short_squeeze_score, row.selling_pressure_score)

    def test_below_min_volume_filtered_out(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "us.duckdb"
            universe = root / "ai.jsonl"
            universe.write_text(json.dumps({"ticker": "FOO"}) + "\n", encoding="utf-8")
            as_of = date(2026, 5, 13)
            chain = [
                ("FOO", as_of, date(2026, 6, 1), 20, 100.0, "F-1", "call", 120.0, 50, 100, 0.10),
                ("FOO", as_of, date(2026, 6, 1), 20, 100.0, "F-2", "put", 80.0, 30, 100, -0.12),
            ]
            _seed_db(db, chain, [])
            _, rows = self.module.build_radar(us_db=db, ai_universe_path=universe, as_of=as_of, min_total_volume=200)
            self.assertEqual(rows, [])

    def test_delta_missing_fallback_to_strike_pct(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "us.duckdb"
            universe = root / "ai.jsonl"
            universe.write_text(json.dumps({"ticker": "BAR"}) + "\n", encoding="utf-8")
            as_of = date(2026, 5, 13)
            chain = [
                # delta None, strike > spot * 1.05 → counts as far-OTM call
                ("BAR", as_of, date(2026, 6, 1), 30, 100.0, "B-1", "call", 110.0, 600, 200, None),
                # delta None, strike < spot * 0.95 → counts as far-OTM put
                ("BAR", as_of, date(2026, 6, 1), 30, 100.0, "B-2", "put", 90.0, 300, 150, None),
            ]
            _seed_db(db, chain, [])
            _, rows = self.module.build_radar(us_db=db, ai_universe_path=universe, as_of=as_of)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].far_otm_call_volume, 600)
            self.assertEqual(rows[0].far_otm_put_volume, 300)

    def test_render_markdown_handles_empty(self) -> None:
        md = self.module.render_markdown([], "2026-05-13")
        self.assertIn("US Options Anomaly Radar", md)
        self.assertIn("无符合阈值", md)


if __name__ == "__main__":
    unittest.main()
