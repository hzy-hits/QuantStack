from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_ROOT = REPO_ROOT.parent
SRC_ROOT = REPO_ROOT / "src"
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_strategy_backtest_report.py"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _load_gate_module():
    spec = importlib.util.spec_from_file_location("run_strategy_backtest_report", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate_module()


def _make_history_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE algorithm_postmortem (
            report_date DATE,
            session VARCHAR,
            symbol VARCHAR,
            selection_status VARCHAR,
            evaluation_date DATE,
            direction VARCHAR,
            executable BOOLEAN,
            realized_pnl_pct DOUBLE,
            best_possible_ret_pct DOUBLE,
            stale_chase BOOLEAN,
            no_fill_reason VARCHAR,
            label VARCHAR,
            report_bucket VARCHAR,
            headline_mode VARCHAR,
            action_intent VARCHAR,
            calibration_bucket VARCHAR,
            detail_json VARCHAR
        )
        """
    )
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
            headline_mode VARCHAR,
            composite_score DOUBLE,
            execution_mode VARCHAR,
            rr_ratio DOUBLE,
            primary_reason VARCHAR,
            details_json VARCHAR
        )
        """
    )
    gate_json = json.dumps(
        {"main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"}}
    )
    start = date(2026, 3, 23)
    for idx in range(25):
        report_date = start + timedelta(days=idx)
        symbol = f"T{idx:02d}"
        ret = 0.8
        con.execute(
            """
            INSERT INTO algorithm_postmortem
            VALUES (?, 'post', ?, 'selected', ?, 'long', TRUE, ?, ?, FALSE, NULL,
                    'won_and_executable', 'core', 'trend', 'TRADE', 'stable', ?)
            """,
            [report_date.isoformat(), symbol, (report_date + timedelta(days=3)).isoformat(), ret, ret, gate_json],
        )
        con.execute(
            """
            INSERT INTO report_decisions
            VALUES (?, 'post', ?, 'selected', ?, 'core', 'long', 'HIGH', 'trend',
                    0.9, 'executable_now', 2.0, 'main system', ?)
            """,
            [report_date.isoformat(), symbol, idx + 1, gate_json],
        )

    # This row is inside lookback but beyond the completed horizon cutoff and must be excluded.
    con.execute(
        """
        INSERT INTO algorithm_postmortem
        VALUES ('2026-04-23', 'post', 'LEAK', 'selected', '2026-04-26', 'long',
                TRUE, 99.0, 99.0, FALSE, NULL, 'won_and_executable',
                'core', 'trend', 'TRADE', 'stable', ?)
        """,
        [gate_json],
    )
    con.execute(
        """
        INSERT INTO report_decisions
        VALUES ('2026-04-23', 'post', 'LEAK', 'selected', 1, 'core', 'long',
                'HIGH', 'trend', 0.9, 'executable_now', 2.0, 'main system', ?)
        """,
        [gate_json],
    )
    con.close()


class StrategyBacktestGateTests(unittest.TestCase):
    def test_auto_select_excludes_unfinished_horizon_dates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "market.duckdb"
            _make_history_db(db_path)

            rows, evaluated_through = gate.load_evaluated_trades(
                db_path,
                "us",
                date(2026, 4, 24),
                lookback_days=40,
                horizon_days=3,
            )

            self.assertEqual(evaluated_through, "2026-04-16")
            self.assertNotIn("LEAK", {row["symbol"] for row in rows})

            candidates = gate.build_policy_candidates(rows, "us", horizon_days=3, lookback_days=40)
            selected, reason = gate.select_champion(candidates, previous_policy_id=None)

            self.assertIsNotNone(selected)
            self.assertIn("highest stability score", reason)
            self.assertTrue(any(c["eligible"] for c in candidates))

    def test_evaluation_date_is_checked_against_as_of_not_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "market.duckdb"
            _make_history_db(db_path)
            con = duckdb.connect(str(db_path))
            con.execute("UPDATE algorithm_postmortem SET evaluation_date = DATE '2026-04-24'")
            con.execute(
                """
                INSERT INTO algorithm_postmortem
                VALUES ('2026-04-21', 'post', 'FUTURE_EVAL', 'selected', '2026-04-25',
                        'long', TRUE, 1.0, 1.0, FALSE, NULL, 'won_and_executable',
                        'core', 'trend', 'TRADE', 'stable', '{}')
                """
            )
            con.execute(
                """
                INSERT INTO report_decisions
                VALUES ('2026-04-21', 'post', 'FUTURE_EVAL', 'selected', 1, 'core',
                        'long', 'HIGH', 'trend', 0.9, 'executable_now', 2.0,
                        'main system', '{}')
                """
            )
            con.close()

            rows, _ = gate.load_evaluated_trades(
                db_path,
                "us",
                date(2026, 4, 24),
                lookback_days=40,
                horizon_days=3,
            )

            symbols = {row["symbol"] for row in rows}
            self.assertIn("T00", symbols)
            self.assertNotIn("FUTURE_EVAL", symbols)

    def test_outcome_loader_falls_back_when_postmortem_table_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "market.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute(
                """
                CREATE TABLE algorithm_postmortem (
                    report_date DATE,
                    session VARCHAR,
                    symbol VARCHAR,
                    selection_status VARCHAR,
                    evaluation_date DATE,
                    direction VARCHAR,
                    executable BOOLEAN,
                    realized_pnl_pct DOUBLE
                )
                """
            )
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
                    composite_score DOUBLE,
                    details_json VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE report_outcomes (
                    report_date DATE,
                    session VARCHAR,
                    symbol VARCHAR,
                    selection_status VARCHAR,
                    evaluation_date DATE,
                    hold_3d_ret_pct DOUBLE,
                    data_ready BOOLEAN
                )
                """
            )
            con.execute(
                """
                INSERT INTO report_decisions
                VALUES ('2026-04-21', 'post', 'OUTCOME_ONLY', 'selected', 1,
                        'core', 'long', 'HIGH', 'executable_now', 0.9, '{}')
                """
            )
            con.execute(
                """
                INSERT INTO report_outcomes
                VALUES ('2026-04-21', 'post', 'OUTCOME_ONLY', 'selected',
                        '2026-04-24', 1.25, TRUE)
                """
            )
            con.close()

            rows, _ = gate.load_evaluated_trades(
                db_path,
                "us",
                date(2026, 4, 24),
                lookback_days=10,
                horizon_days=3,
            )

            self.assertEqual([row["symbol"] for row in rows], ["OUTCOME_ONLY"])
            self.assertEqual(rows[0]["return_pct"], 1.25)

    def test_outcome_loader_falls_back_when_postmortem_has_no_fills(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "market.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute(
                """
                CREATE TABLE algorithm_postmortem (
                    report_date DATE,
                    session VARCHAR,
                    symbol VARCHAR,
                    selection_status VARCHAR,
                    evaluation_date DATE,
                    direction VARCHAR,
                    executable BOOLEAN,
                    realized_pnl_pct DOUBLE
                )
                """
            )
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
                    composite_score DOUBLE,
                    details_json VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE report_outcomes (
                    report_date DATE,
                    session VARCHAR,
                    symbol VARCHAR,
                    selection_status VARCHAR,
                    evaluation_date DATE,
                    hold_3d_ret_pct DOUBLE,
                    data_ready BOOLEAN
                )
                """
            )
            con.execute(
                """
                INSERT INTO algorithm_postmortem
                VALUES ('2026-04-21', 'post', 'OBSERVED', 'selected',
                        '2026-04-24', 'long', FALSE, NULL)
                """
            )
            con.execute(
                """
                INSERT INTO report_decisions
                VALUES ('2026-04-21', 'post', 'OBSERVED', 'selected', 1,
                        'core', 'long', 'HIGH', 'executable_now', 0.9, '{}')
                """
            )
            con.execute(
                """
                INSERT INTO report_outcomes
                VALUES ('2026-04-21', 'post', 'OBSERVED', 'selected',
                        '2026-04-24', 0.75, TRUE)
                """
            )
            con.close()

            rows, _ = gate.load_evaluated_trades(
                db_path,
                "us",
                date(2026, 4, 24),
                lookback_days=10,
                horizon_days=3,
            )

            self.assertEqual([row["symbol"] for row in rows], ["OBSERVED"])
            self.assertEqual(rows[0]["return_pct"], 0.75)
            self.assertTrue(rows[0]["executable"])

    def test_top_winner_concentration_blocks_policy(self) -> None:
        rows = []
        for idx in range(20):
            row = {
                "market": "us",
                "policy_id": "us:core:long:high_mod:executable_now:h3",
                "policy_label": "test",
                "report_date": f"2026-03-{idx + 1:02d}",
                "return_pct": 10.0 if idx == 0 else 0.5,
                "executable": True,
            }
            rows.append(row)

        candidate = gate.evaluate_policy(
            "us",
            rows[0]["policy_id"],
            "test",
            rows,
            horizon_days=3,
            lookback_days=30,
        )

        self.assertFalse(candidate["eligible"])
        self.assertIn("top1_winner_contribution>0.45", candidate["fail_reasons"])

    def test_low_confidence_policy_cannot_be_execution_champion(self) -> None:
        rows = []
        for idx in range(25):
            rows.append(
                {
                    "market": "us",
                    "policy_id": "us:core:long:low:executable_now:h3",
                    "policy_label": "low confidence",
                    "report_date": f"2026-03-{idx + 1:02d}",
                    "return_pct": 1.0,
                    "executable": True,
                }
            )

        candidate = gate.evaluate_policy(
            "us",
            rows[0]["policy_id"],
            "low confidence",
            rows,
            horizon_days=3,
            lookback_days=30,
        )

        self.assertFalse(candidate["eligible"])
        self.assertIn("policy_confidence_not_high_mod", candidate["fail_reasons"])

    def test_champion_challenger_requires_15_percent_margin(self) -> None:
        previous = {
            "policy_id": "old",
            "eligible": True,
            "stability_score": 1.0,
            "fills": 30,
        }
        weak_challenger = {
            "policy_id": "new",
            "eligible": True,
            "stability_score": 1.1,
            "fills": 30,
        }
        strong_challenger = {**weak_challenger, "stability_score": 1.16}

        selected, _ = gate.select_champion([previous, weak_challenger], "old")
        self.assertEqual(selected, "old")

        selected, _ = gate.select_champion([previous, strong_challenger], "old")
        self.assertEqual(selected, "new")

    def test_bulletin_demotes_current_candidates_without_stable_policy(self) -> None:
        current = {
            "market": "us",
            "symbol": "ALFA",
            "policy_id": "us:core:long:high_mod:executable_now:h3",
            "policy_label": "US core long high/mod executable now 3D",
            "report_bucket": "core",
            "signal_direction": "long",
            "signal_confidence": "HIGH",
            "execution_mode": "executable_now",
            "details_json": json.dumps(
                {"main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"}}
            ),
        }

        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"us": "2026-04-21"},
            {"us": None},
            {"us": []},
            {"us": [current]},
        )
        rendered = gate.render_market_bulletin_md(bulletin, "us")

        self.assertEqual(bulletin["ev_status"]["us"], "failed")
        self.assertEqual(bulletin["execution_alpha"], [])
        self.assertIn("- ev_status: `failed`", rendered)
        self.assertIn("stable gate evaluated", rendered)
        self.assertIn("EV unknown", rendered)
        self.assertIn("### Equity Execution Alpha", rendered)
        self.assertIn("### Options / Shadow Options Alpha", rendered)
        self.assertIn("### Recall Alpha", rendered)
        self.assertIn("### Blocked / Out-of-scope Alpha", rendered)
        self.assertNotIn("buy", rendered.lower())

    def test_bulletin_surfaces_stable_theme_rotation_as_tactical_alpha(self) -> None:
        current = {
            "market": "cn",
            "symbol": "600861.SH",
            "policy_id": "cn:theme_rotation:long:high_mod:executable_now:h2",
            "policy_label": "CN theme rotation long high/mod executable now 2D",
            "report_bucket": "THEME ROTATION",
            "signal_direction": "bullish",
            "signal_confidence": "MODERATE",
            "execution_mode": "executable",
            "details_json": json.dumps({"main_signal_gate": {"headline_mode": "range"}}),
        }
        stable_non_core = {
            "policy_id": "cn:theme_rotation:long:high_mod:executable_now:h2",
            "policy_label": "CN theme rotation long high/mod executable now 2D",
            "eligible": False,
            "selected": False,
            "stability_score": 0.83,
            "fills": 475,
            "active_buckets": 20,
            "avg_trade_pct": 0.80,
            "strict_win_rate": 0.56,
            "max_drawdown_pct": -4.33,
            "top1_winner_contribution": 0.03,
            "fail_reasons": ["policy_bucket_not_core"],
        }

        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"cn": "2026-04-22"},
            {"cn": None},
            {"cn": [stable_non_core]},
            {"cn": [current]},
        )
        rendered = gate.render_market_bulletin_md(bulletin, "cn")

        self.assertEqual(bulletin["ev_status"]["cn"], "failed")
        self.assertEqual(bulletin["execution_alpha"], [])
        self.assertEqual(len(bulletin["tactical_alpha"]), 1)
        self.assertIn("Tactical / Theme Rotation Alpha", rendered)
        self.assertIn("600861.SH", rendered)

    def test_strategy_report_surfaces_ev_status(self) -> None:
        report = gate.strategy_report_md(
            date(2026, 4, 24),
            {"us": "2026-04-21"},
            {"us": [], "cn": []},
            {"us": None, "cn": "cn:core:long:high_mod:executable_now:h2"},
            {"us": "failed", "cn": "passed"},
        )

        self.assertIn("| US | 2026-04-21 | `failed` | `none` | 0 / 0 |", report)
        self.assertIn(
            "| CN | - | `passed` | `cn:core:long:high_mod:executable_now:h2` | 0 / 0 |",
            report,
        )


class AlphaBulletinRenderTests(unittest.TestCase):
    def test_us_renderer_includes_alpha_bulletin_before_notable_items(self) -> None:
        from quant_bot.reporting.render import render_alpha_bulletin

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "reports" / "2026-04-24_payload_post.md"
            bulletin_path = (
                output_path.parent
                / "review_dashboard"
                / "strategy_backtest"
                / "2026-04-24"
                / "alpha_bulletin_us.md"
            )
            bulletin_path.parent.mkdir(parents=True)
            bulletin_path.write_text(
                "### Equity Execution Alpha\n\n- `ALFA` -- stable candidate.\n\n### Options / Shadow Options Alpha\n\n- None.\n\n### Recall Alpha\n\n- None.\n\n### Blocked / Out-of-scope Alpha\n\n- None.\n",
                encoding="utf-8",
            )

            lines = render_alpha_bulletin({"meta": {"trade_date": "2026-04-24"}}, output_path)

        rendered = "\n".join(lines)
        self.assertIn("### Equity Execution Alpha", rendered)
        self.assertIn("### Options / Shadow Options Alpha", rendered)
        self.assertIn("### Recall Alpha", rendered)
        self.assertIn("### Blocked / Out-of-scope Alpha", rendered)


class PipelineHookTests(unittest.TestCase):
    def test_us_daily_emits_alpha_bulletin_before_payload_render(self) -> None:
        run_daily = (REPO_ROOT / "scripts" / "run_daily.py").read_text(encoding="utf-8")

        self.assertIn("def emit_stable_alpha_bulletin", run_daily)
        self.assertIn("QUANT_STACK_BIN", run_daily)
        self.assertIn('"alpha"', run_daily)
        self.assertIn('"evaluate"', run_daily)
        self.assertIn("step_alpha_bulletin", run_daily)
        self.assertLess(run_daily.index("step_alpha_bulletin"), run_daily.index("        render_payload_md("))

    def test_cn_pipeline_emits_bulletin_before_final_structural_render(self) -> None:
        daily_pipeline = (STACK_ROOT / "quant-research-cn" / "scripts" / "daily_pipeline.sh").read_text(
            encoding="utf-8"
        )
        gate_marker = "alpha evaluate"
        render_marker = './target/release/quant-cn render --date "$DATE" 2>&1'

        self.assertIn(gate_marker, daily_pipeline)
        self.assertIn("QUANT_STACK_BIN", daily_pipeline)
        self.assertIn("review-backfill", daily_pipeline)
        self.assertIn("QUANT_CN_REVIEW_BACKFILL_TIMING", daily_pipeline)
        self.assertIn("Review history backfill deferred until after email", daily_pipeline)
        self.assertIn("Post-email review maintenance", daily_pipeline)
        self.assertIn("run_strategy_backtest_report.py", daily_pipeline)
        self.assertEqual(daily_pipeline.count(render_marker), 2)
        self.assertLess(daily_pipeline.index(gate_marker), daily_pipeline.rindex(render_marker))
        self.assertIn("## Factor Lab research prior / recall lead", daily_pipeline)
        self.assertIn("研究候选清单如下（仅 recall lead，进入主书前仍需 gate）", daily_pipeline)
        self.assertNotIn('cat "$FACTOR_TMP"', daily_pipeline)

    def test_delivery_mode_defaults_to_test_in_us_and_cn_wrappers(self) -> None:
        run_full = (REPO_ROOT / "scripts" / "run_full.sh").read_text(encoding="utf-8")
        cn_pipeline = (
            STACK_ROOT / "quant-research-cn" / "scripts" / "daily_pipeline.sh"
        ).read_text(encoding="utf-8")
        cn_agents = (STACK_ROOT / "quant-research-cn" / "scripts" / "run_agents.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn('DELIVERY_MODE="${QUANT_DELIVERY_MODE:-test}"', run_full)
        self.assertIn('--delivery-mode "$DELIVERY_MODE"', run_full)
        self.assertIn('DELIVERY_MODE="${QUANT_DELIVERY_MODE:-test}"', cn_pipeline)
        self.assertIn('QUANT_DELIVERY_MODE="$DELIVERY_MODE"', cn_pipeline)
        self.assertIn('--delivery-mode "${QUANT_DELIVERY_MODE:-test}"', cn_agents)
        self.assertIn("--test-recipient", run_full)
        self.assertIn("--test-recipient", cn_agents)


if __name__ == "__main__":
    unittest.main()
