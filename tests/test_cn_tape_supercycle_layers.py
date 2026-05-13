from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from sleeves.cn_tape_leadership import (  # noqa: E402
    query_cn_tape_current_candidates,
    query_cn_tape_leadership_returns,
)


def _make_cn_tape_db(path: Path) -> tuple[date, date]:
    signal_date = date(2026, 5, 12)
    first_date = signal_date - timedelta(days=25)
    final_date = signal_date + timedelta(days=6)
    dates = [first_date + timedelta(days=idx) for idx in range((final_date - first_date).days + 1)]
    symbols = [
        ("002281.SZ", "光迅科技", "元器件"),
        ("002185.SZ", "华天科技", "半导体"),
        ("600900.SH", "长江电力", "水力发电"),
        ("600519.SH", "贵州茅台", "白酒"),
    ]
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE prices (
            ts_code VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, pre_close DOUBLE, pct_chg DOUBLE, amount DOUBLE
        )
        """
    )
    con.execute("CREATE TABLE stock_basic (ts_code VARCHAR, name VARCHAR, industry VARCHAR)")
    con.execute("CREATE TABLE daily_basic (ts_code VARCHAR, trade_date DATE, circ_mv DOUBLE)")
    con.execute(
        """
        CREATE TABLE moneyflow (
            ts_code VARCHAR, trade_date DATE, net_mf_amount DOUBLE,
            buy_lg_amount DOUBLE, buy_elg_amount DOUBLE,
            sell_lg_amount DOUBLE, sell_elg_amount DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE sector_fund_flow (
            trade_date DATE, sector_name VARCHAR, main_net_in DOUBLE, main_net_pct DOUBLE
        )
        """
    )
    for symbol, name, industry in symbols:
        con.execute("INSERT INTO stock_basic VALUES (?, ?, ?)", [symbol, name, industry])
        for idx, trade_date in enumerate(dates):
            if trade_date == signal_date:
                close = 12.0
                pct_chg = 4.0
                amount = 3000.0
                net = 60.0
                buy_lg = 50.0
                buy_elg = 35.0
                sell_lg = 10.0
                sell_elg = 5.0
            else:
                close = 10.0 + idx * 0.05
                pct_chg = 0.4
                amount = 1000.0
                net = 5.0
                buy_lg = 5.0
                buy_elg = 4.0
                sell_lg = 3.0
                sell_elg = 2.0
            pre_close = close / (1.0 + pct_chg / 100.0)
            con.execute(
                "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [symbol, trade_date, pre_close, close * 1.01, close * 0.99, close, pre_close, pct_chg, amount],
            )
            con.execute("INSERT INTO daily_basic VALUES (?, ?, ?)", [symbol, trade_date, 10_000.0])
            con.execute(
                "INSERT INTO moneyflow VALUES (?, ?, ?, ?, ?, ?, ?)",
                [symbol, trade_date, net, buy_lg, buy_elg, sell_lg, sell_elg],
            )
    for trade_date in dates:
        for industry in {row[2] for row in symbols}:
            con.execute("INSERT INTO sector_fund_flow VALUES (?, ?, ?, ?)", [trade_date, industry, 1_000_000.0, 1.0])
    con.close()
    return signal_date, final_date


class CnTapeSupercycleLayerTests(unittest.TestCase):
    def test_current_candidates_get_granular_supercycle_layers_and_exclude_consumer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cn.duckdb"
            signal_date, _ = _make_cn_tape_db(db_path)

            rows = query_cn_tape_current_candidates(db_path, signal_date, top=10)
            by_symbol = {str(row.get("symbol")): row for row in rows}

            self.assertEqual(by_symbol["002281.SZ"]["supercycle_layer"], "ai_networking_optical_cpo")
            self.assertEqual(by_symbol["002185.SZ"]["supercycle_layer"], "ai_chip_equipment_materials_packaging")
            self.assertEqual(by_symbol["600900.SH"]["supercycle_layer"], "ai_power_nuclear_grid")
            self.assertEqual(by_symbol["600900.SH"]["narrative_group"], "ai_infra")
            self.assertNotIn("600519.SH", by_symbol)

    def test_historical_returns_preserve_granular_cn_layer_for_attribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cn.duckdb"
            signal_date, final_date = _make_cn_tape_db(db_path)

            rows, status = query_cn_tape_leadership_returns(db_path, signal_date, final_date)
            by_symbol = {str(row.get("symbol")): row for row in rows}

            self.assertIn("ok", status)
            self.assertEqual(by_symbol["002281.SZ"]["supercycle_layer"], "ai_networking_optical_cpo")
            self.assertEqual(by_symbol["002185.SZ"]["supercycle_layer"], "ai_chip_equipment_materials_packaging")
            self.assertEqual(by_symbol["600900.SH"]["supercycle_layer"], "ai_power_nuclear_grid")
            self.assertNotIn("600519.SH", by_symbol)


if __name__ == "__main__":
    unittest.main()
