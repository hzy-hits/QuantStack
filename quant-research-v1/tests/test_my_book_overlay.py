from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import duckdb


STACK_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_my_book_overlay.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_my_book_overlay", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


overlay = _load_module()


def _write_activity_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "交易,Header,DataDiscriminator,资产分类,货币,代码,日期/时间,数量,交易价格,收盘价格,收益,佣金/税,基础,已实现的损益,按市值计算的损益,代码",
                "交易,Data,Order,股票,USD,OLD,\"2026-03-15, 09:30:00\",2,100,130,-200,-1,201,0,60,O",
                "交易,Data,Order,股票,USD,RUN,\"2026-04-20, 09:30:00\",10,100,112,-1000,-1,1001,0,120,O",
                "交易,Data,Order,股票,USD,MSFT,\"2026-04-29, 09:30:00\",10,100,101,-1000,-1,1001,0,10,O",
                "交易,Data,Order,股票,USD,NOPE,\"2026-04-30, 09:30:00\",5,20,19,-100,-1,101,0,-5,O",
                "已实现和未实现的表现总结,Header,资产分类,代码,费用调整,已实现的 短期利润,已实现的 短期损失,已实现的 长期利润,已实现的 长期损失,已实现的 总数,未实现的 短期利润,未实现的 短期损失,未实现的 长期利润,未实现的 长期损失,未实现的 总数,总数,代码",
                "已实现和未实现的表现总结,Data,股票,OLD,0,0,0,0,0,0,60,0,0,0,60,60,",
                "已实现和未实现的表现总结,Data,股票,RUN,0,0,0,0,0,0,120,0,0,0,120,120,",
                "已实现和未实现的表现总结,Data,股票,MSFT,0,0,0,0,0,0,10,0,0,0,10,10,",
                "已实现和未实现的表现总结,Data,股票,NOPE,0,0,0,0,0,0,0,-5,0,0,-5,-5,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_us_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE report_decisions (
                report_date DATE,
                session VARCHAR,
                symbol VARCHAR,
                selection_status VARCHAR,
                rank_order INTEGER,
                report_bucket VARCHAR,
                signal_direction VARCHAR,
                signal_confidence VARCHAR,
                execution_mode VARCHAR,
                rr_ratio DOUBLE,
                primary_reason VARCHAR,
                details_json VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO report_decisions VALUES
            (DATE '2026-04-29', 'post', 'MSFT', 'selected', 1, 'core', 'long',
             'HIGH', 'executable_now', 2.0, 'test',
             '{"execution_gate":{"trend_regime":"trending"}}')
            """
        )
    finally:
        con.close()


class MyBookOverlayTests(unittest.TestCase):
    def test_overlay_marks_probe_ticket_and_no_ticket_violation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            csv_path = root / "activity.csv"
            db_path = root / "quant.duckdb"
            output_root = root / "reports" / "review_dashboard" / "my_book_overlay"
            alpha_root = root / "reports" / "review_dashboard" / "strategy_backtest"
            alpha_dir = alpha_root / "2026-05-01"
            alpha_dir.mkdir(parents=True)
            (alpha_dir / "alpha_bulletin.json").write_text(
                """
                {
                  "recall_alpha": [
                    {
                      "market": "us",
                      "symbol": "MSFT",
                      "section": "recall_alpha",
                      "policy_id": "us:core:long:high_mod:executable_now:trending:h3",
                      "reason": "Positive EV Setup: V2 main strategy has positive EV evidence"
                    }
                  ],
                  "execution_alpha": [],
                  "blocked_alpha": []
                }
                """,
                encoding="utf-8",
            )
            _write_activity_csv(csv_path)
            _write_us_db(db_path)

            payload = overlay.run(
                Namespace(
                    date="2026-05-01",
                    activity_csv=csv_path,
                    us_db=db_path,
                    output_root=output_root,
                    alpha_root=alpha_root,
                    ticket_lookback_days=5,
                    max_new_names_per_week=3,
                    max_single_name_positions=6,
                    max_hold_days=30,
                )
            )

            rows = {row["symbol"]: row for row in payload["rows"]}
            self.assertEqual(rows["MSFT"]["ticket_state"], "Positive EV Setup")
            self.assertEqual(rows["MSFT"]["action"], "hold_probe_only")
            self.assertEqual(rows["MSFT"]["management_state"], "hold_winner")
            self.assertEqual(rows["OLD"]["management_state"], "runner_2r")
            self.assertEqual(rows["OLD"]["management_action"], "hold_runner_trim_to_half_max")
            self.assertEqual(rows["OLD"]["max_exit_fraction_now"], "50%")
            self.assertEqual(rows["RUN"]["management_state"], "runner_1r")
            self.assertEqual(rows["RUN"]["management_action"], "hold_runner_trim_one_third_max")
            self.assertEqual(rows["RUN"]["fresh_entry_action"], "no_new_buy")
            self.assertEqual(rows["NOPE"]["ticket_state"], "No Report Support")
            self.assertEqual(rows["NOPE"]["violation"], "new_buy_without_trade_ticket")
            self.assertEqual(rows["NOPE"]["management_state"], "exit_or_reduce_loser")
            self.assertEqual(payload["summary"]["runner_1r_positions"], 1)
            self.assertEqual(payload["summary"]["runner_2r_positions"], 1)
            self.assertTrue((output_root / "2026-05-01" / "my_book_overlay_us.md").exists())
            self.assertTrue((output_root / "2026-05-01" / "personal_alpha_research.md").exists())
            self.assertTrue((output_root / "2026-05-01" / "personal_alpha_research.json").exists())


if __name__ == "__main__":
    unittest.main()
