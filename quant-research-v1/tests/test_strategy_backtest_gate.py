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
SCRIPT_PATH = STACK_ROOT / "scripts" / "score_strategy_stability_gate.py"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _load_gate_module():
    spec = importlib.util.spec_from_file_location("score_strategy_stability_gate", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate_module()


def _positive_us_pulse() -> dict:
    return {
        "market": "us",
        "symbol": "__US__",
        "section": "recent_alpha_pulse",
        "reason": "recent US alpha pulse is positive",
        "blockers": [],
        "details": {
            "basis": "test basis",
            "core_weighted_avg_next_pct": 1.25,
            "lane_summary": [],
            "leaders": [],
            "drags": [],
        },
    }


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
        {
            "main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"},
            "execution_gate": {"trend_regime": "trending"},
        }
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
            VALUES (?, 'post', ?, 'selected', ?, 'core', 'long', 'LOW', 'trend',
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
                'LOW', 'trend', 0.9, 'executable_now', 2.0, 'main system', ?)
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
                        'long', 'LOW', 'trend', 0.9, 'executable_now', 2.0,
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

    def test_paper_trades_are_preferred_when_execution_outcomes_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "market.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute(
                """
                CREATE TABLE paper_trades (
                    report_date DATE,
                    session VARCHAR,
                    symbol VARCHAR,
                    selection_status VARCHAR,
                    action_intent VARCHAR,
                    evaluation_date DATE,
                    exit_date DATE,
                    fill_status VARCHAR,
                    realized_ret_pct DOUBLE,
                    max_favorable_pct DOUBLE,
                    label VARCHAR,
                    detail_json VARCHAR
                )
                """
            )
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
            detail = json.dumps(
                {
                    "report_bucket": "CORE BOOK",
                    "signal_confidence": "HIGH",
                    "execution_mode": "executable",
                }
            )
            con.execute(
                """
                INSERT INTO paper_trades
                VALUES ('2026-04-21', 'daily', 'PAPER', 'selected', 'TRADE',
                        '2026-04-24', '2026-04-23', 'filled_open',
                        1.70, 2.20, 'won', ?)
                """,
                [detail],
            )
            con.execute(
                """
                INSERT INTO algorithm_postmortem
                VALUES ('2026-04-21', 'daily', 'PAPER', 'selected',
                        '2026-04-24', 'long', TRUE, -9.0)
                """
            )
            con.execute(
                """
                INSERT INTO report_decisions
                VALUES ('2026-04-21', 'daily', 'PAPER', 'selected', 1,
                        'core', 'long', 'HIGH', 'executable_now', 0.9, '{}')
                """
            )
            con.close()

            rows, _ = gate.load_evaluated_trades(
                db_path,
                "cn",
                date(2026, 4, 24),
                lookback_days=10,
                horizon_days=2,
            )

            self.assertEqual([row["symbol"] for row in rows], ["PAPER"])
            self.assertEqual(rows[0]["return_pct"], 1.7)
            self.assertTrue(rows[0]["executable"])

    def test_top_winner_concentration_blocks_policy(self) -> None:
        rows = []
        for idx in range(20):
            row = {
                "market": "us",
                "policy_id": "us:core:long:high_mod:executable_now:trending:h3",
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

    def test_policy_evidence_metrics_show_statistical_support(self) -> None:
        rows = []
        for idx in range(25):
            rows.append(
                {
                    "market": "us",
                    "policy_id": "us:core:long:high_mod:executable_now:trending:h3",
                    "policy_label": "stable positive edge",
                    "report_date": f"2026-03-{idx + 1:02d}",
                    "return_pct": 0.8,
                    "executable": True,
                }
            )

        candidate = gate.evaluate_policy(
            "us",
            rows[0]["policy_id"],
            "stable positive edge",
            rows,
            horizon_days=3,
            lookback_days=30,
        )

        self.assertEqual(candidate["ev_probability_positive"], 1.0)
        self.assertEqual(candidate["ev_lower_confidence_pct"], 0.8)
        self.assertEqual(candidate["fills_required_for_95_lcb"], 25)

    def test_noisy_positive_policy_can_clear_lcb80_but_still_fail_sample_gate(self) -> None:
        rows = []
        returns = [1.2, -0.2, 0.9, -0.1, 1.0, -0.3, 1.1, -0.4]
        for idx, ret in enumerate(returns):
            rows.append(
                {
                    "market": "cn",
                    "policy_id": "cn:core:long:high_mod:executable_now:h2",
                    "policy_label": "noisy positive edge",
                    "report_date": f"2026-03-{idx + 1:02d}",
                    "return_pct": ret,
                    "executable": True,
                }
            )

        candidate = gate.evaluate_policy(
            "cn",
            rows[0]["policy_id"],
            "noisy positive edge",
            rows,
            horizon_days=2,
            lookback_days=30,
        )

        self.assertGreater(candidate["ev_probability_positive"], 0.5)
        self.assertGreater(candidate["ev_lower_confidence_pct"], 0.0)
        self.assertFalse(candidate["eligible"])
        self.assertIn("fills<50", candidate["fail_reasons"])

    def test_low_confidence_core_policy_is_v2_execution_scope(self) -> None:
        rows = []
        for idx in range(25):
            rows.append(
                {
                    "market": "us",
                    "policy_id": "us:core:long:low:executable_now:trending:h3",
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

        self.assertTrue(candidate["eligible"])
        self.assertEqual(candidate["fail_reasons"], [])

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
            "policy_id": "us:core:long:high_mod:executable_now:trending:h3",
            "policy_label": "US core long high/mod executable now 3D",
            "report_bucket": "core",
            "signal_direction": "long",
            "signal_confidence": "HIGH",
            "execution_mode": "executable_now",
            "details_json": json.dumps(
                {
                    "main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"},
                    "execution_gate": {"trend_regime": "trending"},
                }
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
        self.assertIn("stable EV gate not passed", rendered)
        self.assertIn("### Equity Execution Alpha", rendered)
        self.assertIn("### Options / Shadow Options Alpha", rendered)
        self.assertIn("### Positive EV Setup", rendered)
        self.assertIn("### Legacy / Blocked Alpha", rendered)
        self.assertNotIn("buy", rendered.lower())

    def test_us_options_blocked_keeps_core_equity_execution_stock_only(self) -> None:
        current = {
            "market": "us",
            "symbol": "ALFA",
            "policy_id": "us:core:long:low:executable_now:trending:h3",
            "policy_label": "US core long low executable now trending 3D",
            "report_bucket": "core",
            "signal_direction": "long",
            "signal_confidence": "LOW",
            "selection_status": "ignored",
            "execution_mode": "executable_now",
            "details_json": json.dumps(
                {
                    "main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"},
                    "execution_gate": {"trend_regime": "trending"},
                }
            ),
        }
        options_row = {
            "market": "us",
            "symbol": "ALFA",
            "source": "real_options",
            "expression": "blocked",
            "reason": "no clean options edge",
            "details": {
                "directional_edge": 0.12,
                "vol_edge": -0.05,
                "flow_edge": 0.01,
                "liquidity_gate": "pass",
            },
        }

        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"us": "2026-04-21"},
            {"us": "us:core:long:low:executable_now:trending:h3"},
            {"us": []},
            {"us": [current]},
            {"us": [options_row]},
            {"us": [_positive_us_pulse()]},
        )
        rendered = gate.render_market_bulletin_md(bulletin, "us")

        self.assertEqual([row["symbol"] for row in bulletin["execution_alpha"]], ["ALFA"])
        self.assertEqual(bulletin["recall_alpha"], [])
        self.assertEqual(bulletin["core_options_cross"][0]["tier"], "stock_only_unconfirmed")
        self.assertEqual(bulletin["execution_candidates"][0]["action"], "stock_only_probe_if_other_gates_pass")
        self.assertEqual(bulletin["options_alpha"], [])
        self.assertIn("Execution Candidates", rendered)
        self.assertIn("Core + Options Cross", rendered)
        self.assertIn("stock_only_unconfirmed", rendered)

    def test_execution_candidates_promote_confirmed_core_options_pulse(self) -> None:
        current = {
            "market": "us",
            "symbol": "ALFA",
            "policy_id": "us:core:long:low:executable_now:trending:h3",
            "policy_label": "US core long low executable now trending 3D",
            "report_bucket": "core",
            "signal_direction": "long",
            "signal_confidence": "LOW",
            "selection_status": "selected",
            "execution_mode": "executable_now",
            "rr_ratio": 1.8,
            "details_json": json.dumps(
                {
                    "main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"},
                    "execution_gate": {"trend_regime": "trending", "effective_stretch_score": 0.12},
                    "overnight_alpha": {"alpha_already_paid_risk": 0.18},
                }
            ),
        }
        options_row = {
            "market": "us",
            "symbol": "ALFA",
            "source": "real_options",
            "expression": "call_spread",
            "reason": "bullish direction with cheap convexity",
            "details": {
                "directional_edge": 0.42,
                "vol_edge": 0.18,
                "flow_edge": 0.20,
                "liquidity_gate": "pass",
            },
        }

        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"us": "2026-04-21"},
            {"us": "us:core:long:low:executable_now:trending:h3"},
            {"us": []},
            {"us": [current]},
            {"us": [options_row]},
            {"us": [_positive_us_pulse()]},
        )
        rendered = gate.render_market_bulletin_md(bulletin, "us")

        self.assertEqual([row["symbol"] for row in bulletin["execution_alpha"]], ["ALFA"])
        self.assertEqual(bulletin["execution_candidates"][0]["action"], "execute_option_confirmed_probe")
        self.assertIn("execute_option_confirmed_probe", rendered)

    def test_us_options_direction_conflict_demotes_core_execution(self) -> None:
        current = {
            "market": "us",
            "symbol": "BRAV",
            "policy_id": "us:core:long:low:executable_now:trending:h3",
            "policy_label": "US core long low executable now trending 3D",
            "report_bucket": "core",
            "signal_direction": "long",
            "signal_confidence": "LOW",
            "selection_status": "selected",
            "execution_mode": "executable_now",
            "details_json": json.dumps(
                {
                    "main_signal_gate": {"status": "pass", "role": "main_signal", "action_intent": "TRADE"},
                    "execution_gate": {"trend_regime": "trending"},
                }
            ),
        }
        options_row = {
            "market": "us",
            "symbol": "BRAV",
            "source": "real_options",
            "expression": "put_spread",
            "reason": "bearish direction with acceptable/cheap convexity",
            "details": {
                "directional_edge": -0.56,
                "vol_edge": 0.22,
                "flow_edge": -0.30,
                "liquidity_gate": "pass",
            },
        }

        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"us": "2026-04-21"},
            {"us": "us:core:long:low:executable_now:trending:h3"},
            {"us": []},
            {"us": [current]},
            {"us": [options_row]},
            {"us": [_positive_us_pulse()]},
        )

        self.assertEqual(bulletin["execution_alpha"], [])
        self.assertEqual([row["symbol"] for row in bulletin["recall_alpha"]], ["BRAV"])
        self.assertEqual(bulletin["core_options_cross"][0]["tier"], "core_options_conflict")
        self.assertEqual(bulletin["execution_candidates"][0]["action"], "do_not_promote_conflict")
        self.assertIn("core/options direction conflict", bulletin["recall_alpha"][0]["blockers"])

    def test_recent_alpha_pulse_renders_pending_as_unresolved_not_failed(self) -> None:
        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"cn": "2026-04-22"},
            {"cn": None},
            {"cn": []},
            {"cn": []},
            {},
            {
                "cn": [
                    {
                        "market": "cn",
                        "symbol": "__CN__",
                        "section": "recent_alpha_pulse",
                        "reason": "recent CN pulse is pending",
                        "blockers": [],
                        "details": {
                            "basis": "test basis",
                            "state_summary": [
                                {
                                    "report_date": "2026-04-24",
                                    "alpha_state": "positive_ev_setup",
                                    "fill_status": "pending",
                                    "n": 1,
                                    "avg_realized_pct": None,
                                    "avg_best_pct": None,
                                    "win_rate": None,
                                }
                            ],
                            "best_realized_or_favorable": [],
                            "pending_watch": [
                                {
                                    "report_date": "2026-04-24",
                                    "symbol": "002773.SZ",
                                    "ev_norm_lcb_80": 51.1534,
                                }
                            ],
                        },
                    }
                ]
            },
            learning_queue_by_market={
                "cn": [
                    {
                        "market": "cn",
                        "symbol": "__MISSED_ALPHA__",
                        "section": "learning_queue",
                        "label": "missed_alpha",
                        "reason": "Find recall features that appeared before missed winners; do not invent unrelated factors.",
                        "blockers": [],
                        "details": {
                            "n": 2,
                            "avg_best_ret_pct": 3.2,
                            "positive_best_rate": 1.0,
                            "examples": [
                                {
                                    "report_date": "2026-04-23",
                                    "symbol": "600000.SH",
                                    "best_ret_pct": 4.2,
                                }
                            ],
                        },
                    }
                ]
            },
        )

        rendered = gate.render_market_bulletin_md(bulletin, "cn")

        self.assertIn("### Recent Alpha Pulse", rendered)
        self.assertIn("| 2026-04-24 | positive_ev_setup | pending | 1 | - | - | - |", rendered)
        self.assertIn("`002773.SZ` 51.1534", rendered)
        self.assertIn("### Learning Queue", rendered)
        self.assertIn("`missed_alpha`", rendered)

    def test_cn_do_not_chase_shadow_setup_is_not_execution_alpha(self) -> None:
        current = {
            "market": "cn",
            "symbol": "002393.SZ",
            "policy_id": "cn:oversold_contrarian:long:ev_positive:planned_entry:na:h2",
            "policy_label": "CN oversold contrarian EV-positive planned entry 2D",
            "strategy_family": "oversold_contrarian",
            "report_bucket": "oversold_contrarian",
            "action_intent": "TRADE",
            "alpha_state": "positive_ev_setup",
            "signal_direction": "long",
            "signal_confidence": "EV_POSITIVE",
            "execution_mode": "do_not_chase",
            "execution_rule": "do_not_chase",
            "ev_lcb_80_pct": 0.8,
            "ev_norm_lcb_80": 50.0,
            "features_json": json.dumps(
                {
                    "shadow_alpha_prob": 0.42,
                    "entry_quality_score": 0.22,
                    "stale_chase_risk": 0.72,
                    "execution_mode": "do_not_chase",
                }
            ),
            "details_json": "{}",
        }
        pulse = {
            "market": "cn",
            "symbol": "__CN__",
            "section": "recent_alpha_pulse",
            "reason": "recent CN shadow-option pulse has realized alpha",
            "blockers": [],
            "details": {
                "basis": "test basis",
                "filled_avg_realized_pct": 1.2,
                "filled_avg_best_pct": 2.1,
                "state_summary": [],
                "best_realized_or_favorable": [],
                "pending_watch": [],
            },
        }

        bulletin = gate.build_bulletin(
            date(2026, 4, 24),
            {"cn": "2026-04-22"},
            {"cn": "cn:oversold_contrarian:long:ev_positive:planned_entry:na:h2"},
            {"cn": []},
            {"cn": [current]},
            {},
            {"cn": [pulse]},
        )

        self.assertEqual(bulletin["execution_alpha"], [])
        self.assertEqual(bulletin["execution_candidates"][0]["action"], "do_not_chase_wait_reset")
        self.assertEqual([row["symbol"] for row in bulletin["recall_alpha"]], ["002393.SZ"])
        self.assertIn("stale_chase_or_do_not_chase", bulletin["recall_alpha"][0]["blockers"])

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
                "### Equity Execution Alpha\n\n- `ALFA` -- stable candidate.\n\n### Options / Shadow Options Alpha\n\n- None.\n\n### Positive EV Setup\n\n- None.\n\n### Legacy / Blocked Alpha\n\n- None.\n",
                encoding="utf-8",
            )

            lines = render_alpha_bulletin({"meta": {"trade_date": "2026-04-24"}}, output_path)

        rendered = "\n".join(lines)
        self.assertIn("### Equity Execution Alpha", rendered)
        self.assertIn("### Options / Shadow Options Alpha", rendered)
        self.assertIn("### Positive EV Setup", rendered)
        self.assertIn("### Legacy / Blocked Alpha", rendered)


class PipelineHookTests(unittest.TestCase):
    def test_us_daily_emits_alpha_bulletin_before_payload_render(self) -> None:
        run_daily = (REPO_ROOT / "scripts" / "run_daily.py").read_text(encoding="utf-8")

        self.assertIn("def emit_stable_alpha_bulletin", run_daily)
        self.assertIn("score_strategy_stability_gate.py", run_daily)
        self.assertIn('"--lookback-days"', run_daily)
        self.assertIn('"60"', run_daily)
        self.assertIn("def emit_my_book_overlay", run_daily)
        self.assertIn("run_my_book_overlay.py", run_daily)
        self.assertIn("QUANT_USER_ACTIVITY_CSV", run_daily)
        self.assertIn("step_alpha_bulletin", run_daily)
        self.assertIn("step_my_book_overlay", run_daily)
        self.assertLess(run_daily.index("step_alpha_bulletin"), run_daily.index("        render_payload_md("))
        self.assertLess(run_daily.index("step_my_book_overlay"), run_daily.index("        render_payload_md("))

    def test_cn_pipeline_emits_bulletin_before_final_structural_render(self) -> None:
        daily_pipeline = (STACK_ROOT / "quant-research-cn" / "scripts" / "daily_pipeline.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("canonical state machine", daily_pipeline)
        self.assertIn("daily", daily_pipeline)
        self.assertIn("--markets cn", daily_pipeline)
        self.assertIn("--run-producers", daily_pipeline)
        self.assertIn("--with-narrative", daily_pipeline)
        self.assertIn("--send-reports", daily_pipeline)
        self.assertIn("QUANT_STACK_BIN", daily_pipeline)
        self.assertNotIn("review-backfill --date-from", daily_pipeline)
        self.assertNotIn('quant-cn render --date "$DATE"', daily_pipeline)
        self.assertNotIn("score_strategy_stability_gate.py", daily_pipeline)
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
