"""Tests for the per-stock options verdict (逐票复核 options read)."""
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_ROOT = REPO_ROOT.parent
SCRIPT_PATH = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"


def _load_module():
    if "generate_main_strategy_v2_report" in sys.modules:
        return sys.modules["generate_main_strategy_v2_report"]
    spec = importlib.util.spec_from_file_location("generate_main_strategy_v2_report", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


v2 = _load_module()


def _seed(path: Path, sentiment: list[tuple], chain: list[tuple]) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE options_sentiment (symbol VARCHAR, as_of DATE, "
            "pc_ratio_z DOUBLE, skew_z DOUBLE, vrp DOUBLE, iv_ann DOUBLE, rv_ann DOUBLE)"
        )
        for row in sentiment:
            con.execute("INSERT INTO options_sentiment VALUES (?,?,?,?,?,?,?)", row)
        con.execute(
            "CREATE TABLE options_chain_quotes (symbol VARCHAR, as_of DATE, "
            "days_to_exp INTEGER, volume BIGINT)"
        )
        for row in chain:
            con.execute("INSERT INTO options_chain_quotes VALUES (?,?,?,?)", row)
    finally:
        con.close()


class OptionsVerdictTests(unittest.TestCase):
    def test_full_verdict_all_four_readings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            as_of = date(2026, 5, 15)
            _seed(
                db,
                # call-heavy (pcz -1.5), elevated skew (+1.5), IV 60% rich (vrp +0.1)
                [("MRVL", as_of, -1.5, 1.5, 0.10, 0.60, 0.50)],
                # heavy long-dated (LEAPS) volume → conviction long
                [("MRVL", as_of, 5, 1000), ("MRVL", as_of, 300, 2000)],
            )
            out = v2.build_options_verdicts(db, ["MRVL"], as_of)
            verdict = out["MRVL"]["verdict"]
            self.assertIn("call 偏多", verdict)
            self.assertIn("IV 60%·贵", verdict)
            self.assertIn("下行恐惧高", verdict)
            self.assertIn("信仰久期长", verdict)

    def test_cheap_iv_and_calm_skew(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            as_of = date(2026, 5, 15)
            _seed(
                db,
                # put-heavy, calm skew, IV cheap (vrp -0.1)
                [("NVDA", as_of, 1.4, -1.4, -0.10, 0.35, 0.45)],
                [("NVDA", as_of, 3, 5000)],  # all weekly → tactical
            )
            out = v2.build_options_verdicts(db, ["NVDA"], as_of)
            verdict = out["NVDA"]["verdict"]
            self.assertIn("put 偏空", verdict)
            self.assertIn("IV 35%·便宜", verdict)
            self.assertIn("下行恐惧低", verdict)
            self.assertIn("信仰久期短", verdict)

    def test_missing_sentiment_degrades_gracefully(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            as_of = date(2026, 5, 15)
            _seed(db, [], [])
            out = v2.build_options_verdicts(db, ["GHOST"], as_of)
            # No sentiment row → only the "定位 n/a" placeholder, still emitted.
            self.assertEqual(out.get("GHOST", {}).get("verdict"), "定位 n/a")

    def test_partial_sentiment_omits_missing_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "us.duckdb"
            as_of = date(2026, 5, 15)
            # iv/vrp NULL → expected-move part omitted; neutral positioning
            _seed(db, [("AMZN", as_of, -0.3, 1.5, None, None, None)], [])
            verdict = v2.build_options_verdicts(db, ["AMZN"], as_of)["AMZN"]["verdict"]
            self.assertIn("定位中性", verdict)
            self.assertIn("下行恐惧高", verdict)
            self.assertNotIn("IV", verdict)

    def test_missing_db_returns_empty(self) -> None:
        out = v2.build_options_verdicts(Path("/nope/missing.duckdb"), ["NVDA"], date(2026, 5, 15))
        self.assertEqual(out, {})


if __name__ == "__main__":
    unittest.main()
