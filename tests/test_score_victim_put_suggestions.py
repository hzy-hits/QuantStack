"""Tests for the bubble-hedge victim → OTM put suggestion script."""
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
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_victim_put_suggestions.py"


def _load_module():
    if "score_victim_put_suggestions" in sys.modules:
        return sys.modules["score_victim_put_suggestions"]
    sys.path.insert(0, str(STACK_ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("score_victim_put_suggestions", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_db(path: Path, rows: list[dict]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE options_chain_quotes (
                symbol VARCHAR, as_of DATE, expiry VARCHAR, days_to_exp INTEGER,
                current_price DOUBLE, contract_symbol VARCHAR, option_type VARCHAR,
                strike DOUBLE, bid DOUBLE, ask DOUBLE, mid DOUBLE, last_price DOUBLE,
                volume BIGINT, open_interest BIGINT, implied_volatility DOUBLE,
                delta DOUBLE, gamma DOUBLE, theta DOUBLE
            )
            """
        )
        for r in rows:
            con.execute(
                """
                INSERT INTO options_chain_quotes
                (symbol, as_of, expiry, days_to_exp, current_price, contract_symbol,
                 option_type, strike, bid, ask, mid, last_price, volume,
                 open_interest, implied_volatility, delta, gamma, theta)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    r["symbol"], r["as_of"], r["expiry"], r["days_to_exp"],
                    r["current_price"], r["contract_symbol"], r["option_type"],
                    r["strike"], r.get("bid"), r.get("ask"), r.get("mid"),
                    r.get("last_price"), r.get("volume", 0),
                    r.get("open_interest", 0), r.get("implied_volatility"),
                    r.get("delta"), r.get("gamma"), r.get("theta"),
                ],
            )
    finally:
        con.close()


def _seed_bubble_hedge(root: Path, as_of: str, victims: list[dict]) -> None:
    d = root / as_of
    d.mkdir(parents=True, exist_ok=True)
    (d / "bubble_hedge.json").write_text(
        json.dumps({"as_of": as_of, "victims": victims}, ensure_ascii=False),
        encoding="utf-8",
    )


class VictimPutSuggestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()

    def test_premium_helper_prefers_mid_then_midpoint_then_last(self) -> None:
        m = self.module
        self.assertEqual(m._premium({"mid": 4.0, "bid": 3.5, "ask": 4.5, "last_price": 4.1}), 4.0)
        self.assertEqual(m._premium({"mid": None, "bid": 3.5, "ask": 4.5, "last_price": 4.1}), 4.0)
        self.assertEqual(m._premium({"mid": None, "bid": None, "ask": None, "last_price": 4.1}), 4.1)
        self.assertIsNone(m._premium({"mid": None, "bid": None, "ask": None, "last_price": None}))

    def test_picks_liquid_otm_put_in_primary_dte_window(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db = tmp / "u.duckdb"
            radar = tmp / "radar"
            _seed_db(
                db,
                [
                    # The "good" target: DTE 45, delta -0.25, OI 1000
                    dict(
                        symbol="VICTIM", as_of=date(2026, 5, 13), expiry="2026-06-26",
                        days_to_exp=45, current_price=100.0,
                        contract_symbol="VICTIM260626P00090000", option_type="put",
                        strike=90.0, bid=2.5, ask=2.7, mid=2.6, last_price=2.55,
                        volume=300, open_interest=1000, implied_volatility=0.55, delta=-0.25,
                    ),
                    # Too far OTM (delta out of band) — should NOT show up
                    dict(
                        symbol="VICTIM", as_of=date(2026, 5, 13), expiry="2026-06-26",
                        days_to_exp=45, current_price=100.0,
                        contract_symbol="VICTIM260626P00050000", option_type="put",
                        strike=50.0, bid=0.05, ask=0.10, mid=0.08, last_price=0.07,
                        volume=10, open_interest=500, implied_volatility=1.20, delta=-0.02,
                    ),
                    # ATM — also out of band (delta too negative)
                    dict(
                        symbol="VICTIM", as_of=date(2026, 5, 13), expiry="2026-06-26",
                        days_to_exp=45, current_price=100.0,
                        contract_symbol="VICTIM260626P00100000", option_type="put",
                        strike=100.0, bid=4.0, ask=4.2, mid=4.1, last_price=4.05,
                        volume=200, open_interest=900, implied_volatility=0.50, delta=-0.50,
                    ),
                    # Illiquid (OI < 50) — must be filtered out
                    dict(
                        symbol="VICTIM", as_of=date(2026, 5, 13), expiry="2026-06-26",
                        days_to_exp=45, current_price=100.0,
                        contract_symbol="VICTIM260626P00085000", option_type="put",
                        strike=85.0, bid=1.2, ask=1.5, mid=1.35, last_price=1.4,
                        volume=2, open_interest=10, implied_volatility=0.60, delta=-0.22,
                    ),
                ],
            )
            _seed_bubble_hedge(
                radar, "2026-05-13",
                [{"symbol": "VICTIM", "convex_score": 80, "reasons": ["test"]}],
            )

            con = duckdb.connect(str(db), read_only=True)
            try:
                result = m._suggest_for_victim(con, "VICTIM", date(2026, 5, 13))
            finally:
                con.close()
            contracts = result["contracts"]
            self.assertEqual(len(contracts), 1)
            self.assertEqual(contracts[0]["contract_symbol"], "VICTIM260626P00090000")
            self.assertEqual(result["dte_window"], "30-60d")
            self.assertIsNone(result["note"])
            self.assertEqual(contracts[0]["premium_est"], 2.6)
            self.assertEqual(contracts[0]["cost_pct_of_spot"], 2.6)

    def test_falls_back_to_shortest_chain_when_target_window_empty(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db = tmp / "u.duckdb"
            _seed_db(
                db,
                [
                    # Only DTE 16 available — script must warn AND still suggest
                    dict(
                        symbol="SHORT", as_of=date(2026, 5, 13), expiry="2026-05-29",
                        days_to_exp=16, current_price=200.0,
                        contract_symbol="SHORT260529P00180000", option_type="put",
                        strike=180.0, bid=4.0, ask=4.4, mid=4.2, last_price=4.1,
                        volume=50, open_interest=500, implied_volatility=0.65, delta=-0.22,
                    ),
                ],
            )
            con = duckdb.connect(str(db), read_only=True)
            try:
                result = m._suggest_for_victim(con, "SHORT", date(2026, 5, 13))
            finally:
                con.close()
            self.assertEqual(len(result["contracts"]), 1)
            self.assertIn("fallback", result["dte_window"])
            self.assertIsNotNone(result["note"])  # LEAPS warning emitted

    def test_no_chain_data_returns_empty_contracts_with_note(self) -> None:
        m = self.module
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db = tmp / "u.duckdb"
            _seed_db(db, [])
            con = duckdb.connect(str(db), read_only=True)
            try:
                result = m._suggest_for_victim(con, "GHOST", date(2026, 5, 13))
            finally:
                con.close()
            self.assertEqual(result["contracts"], [])
            self.assertIn("no chain data", result["note"])


if __name__ == "__main__":
    unittest.main()
