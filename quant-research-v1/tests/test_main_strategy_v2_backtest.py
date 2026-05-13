from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_ROOT = REPO_ROOT.parent
SCRIPT_PATH = STACK_ROOT / "scripts" / "run_main_strategy_v2_backtest.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_main_strategy_v2_backtest", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


v2 = _load_module()


def _make_us_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE report_decisions (
            report_date DATE, session VARCHAR, symbol VARCHAR, selection_status VARCHAR,
            rank_order INTEGER, report_bucket VARCHAR, signal_direction VARCHAR,
            signal_confidence VARCHAR, headline_mode VARCHAR, execution_mode VARCHAR,
            entry_price DOUBLE, reference_price DOUBLE, stop_price DOUBLE,
            target_price DOUBLE, rr_ratio DOUBLE, expected_move_pct DOUBLE,
            primary_reason VARCHAR, details_json VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE report_outcomes (
            report_date DATE, session VARCHAR, symbol VARCHAR, selection_status VARCHAR,
            evaluation_date DATE, hold_3d_ret_pct DOUBLE, data_ready BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE options_alpha (
            symbol VARCHAR, as_of DATE, directional_edge DOUBLE, vol_edge DOUBLE,
            vrp_edge DOUBLE, flow_edge DOUBLE, liquidity_gate VARCHAR,
            expression VARCHAR, reason VARCHAR, detail_json VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE options_analysis (
            symbol VARCHAR, as_of DATE, expiry VARCHAR, days_to_exp INTEGER,
            current_price DOUBLE, range_68_low DOUBLE, range_68_high DOUBLE,
            range_95_low DOUBLE, range_95_high DOUBLE, atm_iv DOUBLE,
            iv_skew DOUBLE, put_call_vol_ratio DOUBLE, bias_signal VARCHAR,
            liquidity_score VARCHAR, chain_width INTEGER, avg_spread_pct DOUBLE,
            unusual_strikes VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE earnings_calendar (
            symbol VARCHAR, report_date DATE, fiscal_period VARCHAR,
            estimate_eps DOUBLE, actual_eps DOUBLE, surprise_pct DOUBLE
        )
        """
    )
    con.execute("CREATE TABLE us_symbols (symbol VARCHAR, name VARCHAR, type VARCHAR, exchange VARCHAR, mic VARCHAR)")
    con.execute("INSERT INTO us_symbols VALUES ('NVDA', 'NVIDIA Corporation', 'Common Stock', 'US', 'XNAS')")
    con.execute("INSERT INTO us_symbols VALUES ('V211', 'V2 Test Corp', 'Common Stock', 'US', 'XNAS')")
    con.execute("INSERT INTO earnings_calendar VALUES ('NVDA', '2026-04-01', 'Q1 2026', 1.20, NULL, NULL)")
    con.execute("INSERT INTO earnings_calendar VALUES ('V211', '2026-04-01', 'Q1 2026', 0.42, NULL, NULL)")
    trending = json.dumps({"execution_gate": {"trend_regime": "trending"}})
    noisy = json.dumps({"execution_gate": {"trend_regime": "noisy"}})
    for idx in range(12):
        day = f"2026-03-{idx + 2:02d}"
        con.execute(
            """
            INSERT INTO report_decisions
            VALUES (?, 'post', ?, 'selected', ?, 'core', 'long', 'LOW', 'uncertain',
                    'executable_now', 100, 100, 95, 110, 2.0, 5.0, 'v2', ?)
            """,
            [day, f"V2{idx}", idx + 1, trending],
        )
        con.execute(
            "INSERT INTO report_outcomes VALUES (?, 'post', ?, 'selected', ?, 1.0, TRUE)",
            [day, f"V2{idx}", "2026-03-31"],
        )
        con.execute(
            "INSERT INTO options_alpha VALUES (?, ?, 0.5, 0.2, 0.1, 0.1, 'pass', 'call_spread', 'ok', ?)",
            [f"V2{idx}", day, json.dumps({"expiry": "2026-04-17"})],
        )
        con.execute(
            """
            INSERT INTO options_analysis
            VALUES (?, ?, '2026-04-17', 30, 100, 94, 106, 89, 112,
                    0.35, 1.0, 0.8, 'bullish', 'good', 20, 0.08, NULL)
            """,
            [f"V2{idx}", day],
        )
    for idx in range(12):
        day = f"2026-03-{idx + 2:02d}"
        con.execute(
            """
            INSERT INTO report_decisions
            VALUES (?, 'post', ?, 'selected', ?, 'core', 'long', 'HIGH', 'uncertain',
                    'executable_now', 100, 100, 95, 110, 2.0, 5.0, 'legacy', ?)
            """,
            [day, f"LEG{idx}", idx + 20, noisy],
        )
        con.execute(
            "INSERT INTO report_outcomes VALUES (?, 'post', ?, 'selected', ?, -0.5, TRUE)",
            [day, f"LEG{idx}", "2026-03-31"],
        )
    con.close()


def _make_cn_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE stock_basic (ts_code VARCHAR, name VARCHAR, industry VARCHAR)")
    con.execute("INSERT INTO stock_basic VALUES ('000001.SZ', '平安银行', '银行'), ('000002.SZ', '万科A', '地产')")
    con.execute(
        """
        CREATE TABLE disclosure_date (
            ts_code VARCHAR, ann_date VARCHAR, end_date DATE,
            pre_date VARCHAR, actual_date VARCHAR, modify_date VARCHAR
        )
        """
    )
    con.execute(
        "INSERT INTO disclosure_date VALUES ('000001.SZ', NULL, '2026-03-31', '2026-04-01', NULL, NULL)"
    )
    con.execute(
        "INSERT INTO disclosure_date VALUES ('000002.SZ', NULL, '2026-03-31', '2026-04-02', NULL, NULL)"
    )
    con.execute(
        """
        CREATE TABLE strategy_model_dataset (
            evaluation_date DATE, report_date DATE, session VARCHAR, symbol VARCHAR,
            selection_status VARCHAR, strategy_family VARCHAR, strategy_key VARCHAR,
            execution_rule VARCHAR, action_intent VARCHAR, alpha_state VARCHAR,
            features_json VARCHAR, reference_close DOUBLE, planned_entry DOUBLE,
            fill_status VARCHAR, fill_date DATE, fill_price DOUBLE, exit_date DATE,
            exit_price DOUBLE, realized_ret_pct DOUBLE, max_favorable_pct DOUBLE,
            max_adverse_pct DOUBLE, p_fill DOUBLE, mu_ret_pct DOUBLE, tail_loss_pct DOUBLE,
            ev_pct DOUBLE, ev_lcb_80_pct DOUBLE, ev_lcb_95_pct DOUBLE,
            risk_unit_pct DOUBLE, ev_norm_score DOUBLE, ev_norm_lcb_80 DOUBLE,
            ev_norm_lcb_95 DOUBLE, detail_json VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE limit_up_model_predictions (
            as_of DATE, symbol VARCHAR, board_scope VARCHAR, p_limit_up DOUBLE,
            p_touch_limit DOUBLE, p_failed_board DOUBLE, ev_after_cost_pct DOUBLE,
            ev_lcb_80_pct DOUBLE, probability_decile INTEGER, model_state VARCHAR,
            decision_state VARCHAR, detail_json VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE limit_up_model_performance (
            as_of DATE, train_start DATE, train_end DATE, model_state VARCHAR,
            train_samples INTEGER, train_positives INTEGER, auc DOUBLE, brier DOUBLE,
            top_decile_hit_rate DOUBLE, top_decile_lift DOUBLE,
            failed_board_rate DOUBLE, avg_next_ret_pct DOUBLE
        )
        """
    )
    features = json.dumps({"market_p_high_vol": 0.4, "execution_mode": "wait_pullback"})
    for idx in range(25):
        day = f"2026-03-{idx + 2:02d}"
        con.execute(
            """
            INSERT INTO strategy_model_dataset
            VALUES ('2026-04-01', ?, 'daily', '000001.SZ', 'exploration',
                    'oversold_contrarian', 'oversold|x', 'next_open_or_pullback',
                    'TRADE', 'positive_ev_setup', ?, 10, 10, 'filled_open',
                    ?, 10, ?, 10.2, 1.2, 2.0, -0.8, 0.9, 1.0, 1.2, 0.8,
                    0.4, 0.1, 2.0, 60, 55, 50, '{}')
            """,
            [day, features, day, day],
        )
        con.execute(
            """
            INSERT INTO strategy_model_dataset
            VALUES ('2026-04-01', ?, 'daily', '000002.SZ', 'selected',
                    'structural_core', 'struct|x', 'next_open_or_pullback',
                    'TRADE', 'blocked_negative_ev', ?, 10, 10, 'filled_open',
                    ?, 10, ?, 9.8, -0.8, 1.0, -1.5, 0.7, -0.5, 1.2, -0.4,
                    -0.7, -1.0, 2.0, 40, 35, 30, '{}')
            """,
            [day, features, day, day],
        )
    con.execute(
        "INSERT INTO limit_up_model_predictions VALUES ('2026-04-01', '000001.SZ', 'mainboard_10cm', 0.2, 0.35, 0.1, 1.1, 0.2, 10, 'trained', 'limit_up_candidate', '{}')"
    )
    con.execute(
        "INSERT INTO limit_up_model_performance VALUES ('2026-04-01', '2026-03-01', '2026-03-31', 'trained', 100, 5, 0.61, 0.02, 0.12, 2.4, 0.08, 1.1)"
    )
    con.close()


class MainStrategyV2BacktestTests(unittest.TestCase):
    def test_us_earnings_calendar_uses_original_company_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "us.duckdb"
            con = duckdb.connect(str(path))
            con.execute(
                """
                CREATE TABLE earnings_calendar (
                    symbol VARCHAR, report_date DATE, fiscal_period VARCHAR,
                    estimate_eps DOUBLE, actual_eps DOUBLE, surprise_pct DOUBLE
                )
                """
            )
            con.execute("CREATE TABLE us_symbols (symbol VARCHAR, name VARCHAR, type VARCHAR, exchange VARCHAR, mic VARCHAR)")
            con.execute("INSERT INTO us_symbols VALUES ('NVDA', 'NVIDIA Corporation', 'Common Stock', 'US', 'XNAS')")
            con.execute("INSERT INTO earnings_calendar VALUES ('NVDA', '2026-04-01', 'Q1 2026', 1.20, NULL, NULL)")
            con.close()

            calendar = v2.build_us_earnings_calendar(
                path,
                v2.parse_date("2026-04-01"),
                focus_symbols=["NVDA"],
            )

            self.assertEqual(calendar["rows"][0]["symbol"], "NVDA")
            self.assertEqual(calendar["rows"][0]["display_name"], "NVIDIA Corporation")

    def test_ai_lab_quality_index_scores_loaded_publication_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            seed = root / "seed.yaml"
            pubs = root / "pubs.csv"
            seed.write_text(
                """
version: 1
conference_scope: [NeurIPS, ICML, ICLR, CVPR]
companies:
  - symbol: GOOGL
    company: Alphabet
    labs: [Google DeepMind]
    supercycle_layer: ai_labs_cloud_models
    data_status: data_required
""",
                encoding="utf-8",
            )
            pubs.write_text(
                "symbol,conference,year,accepted_count,oral_spotlight_count,source\n"
                "GOOGL,NeurIPS,2025,10,2,unit\n"
                "GOOGL,ICML,2025,5,1,unit\n",
                encoding="utf-8",
            )
            index = v2.build_ai_lab_quality_index(seed, pubs)
            row = index["rows"][0]
            self.assertEqual(index["status"], "scored")
            self.assertTrue(index["publication_dataset_loaded"])
            self.assertEqual(row["paper_count_total"], 15)
            self.assertEqual(row["oral_spotlight_count"], 3)
            self.assertGreater(row["lab_quality_score"], 0)

    def test_ai_supply_chain_relationship_loader_requires_source_link(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "relationships.yaml"
            path.write_text(
                """
version: 1
relationships:
  - relationship_id: rel_good
    as_of: 2026-01-01
    market: US
    primary_symbol: DELL
    counterparty_symbol: NVDA
    symbols: [DELL, NVDA]
    layer: ai_compute_accelerators
    relationship_type: ai_factory_partner
    supply_chain_role: Dell AI server platform with NVIDIA acceleration.
    source_name: Dell press release
    source_type: official_press_release
    source_url: https://example.com/dell-nvidia
    source_date: 2026-01-01
    confidence: high
  - relationship_id: rel_bad
    market: US
    primary_symbol: AAOI
    counterparty_symbol: HYPER
    symbols: [AAOI, HYPER]
    layer: ai_networking_optical_cpo
    relationship_type: rumored_supplier
    confidence: low
""",
                encoding="utf-8",
            )
            ledger = v2.build_ai_supply_chain_relationships(path)
            self.assertEqual(ledger["summary"]["rows"], 2)
            self.assertEqual(ledger["summary"]["source_linked"], 1)
            by_id = {row["relationship_id"]: row for row in ledger["rows"]}
            self.assertEqual(by_id["rel_good"]["evidence_state"], "source_linked_relationship")
            self.assertEqual(by_id["rel_bad"]["evidence_state"], "invalid_missing_source")

    def test_us_value_radar_requires_source_linked_evidence_for_deep_dive(self) -> None:
        payload = {
            "as_of": "2026-04-01",
            "generated_at": "2026-04-01T12:00:00",
            "ai_supercycle_evidence_ledger": {
                "rows": [
                    {
                        "market": "US",
                        "symbol": "AAOI",
                        "name": "Applied Optoelectronics",
                        "layer": "ai_networking_optical_cpo",
                        "priority": 1,
                        "supply_chain_role": "optical module supplier",
                        "bottleneck_focus": "datacenter optical links",
                        "evidence_state": "theme_news_only",
                        "evidence_score": 0.95,
                        "rank_score": 100.0,
                    },
                    {
                        "market": "US",
                        "symbol": "POET",
                        "name": "POET Technologies",
                        "layer": "ai_networking_optical_cpo",
                        "priority": 1,
                        "supply_chain_role": "optical module supplier",
                        "bottleneck_focus": "datacenter optical links",
                        "evidence_state": "negative_supply_evidence",
                        "evidence_score": 0.08,
                        "rank_score": 100.0,
                    },
                ]
            },
        }
        radar = v2.build_ai_supercycle_value_radar(payload, Path("/tmp/nonexistent-us.duckdb"), v2.parse_date("2026-04-01"))
        by_symbol = {row["symbol"]: row for row in radar["rows"]}
        self.assertEqual(by_symbol["AAOI"]["research_priority"], "evidence_first")
        self.assertEqual(by_symbol["POET"]["research_priority"], "avoid_until_resolved")
        self.assertIn("negative supply/order evidence", "; ".join(by_symbol["POET"]["blockers"]))

    def test_us_value_radar_carries_ai_lab_quality_score(self) -> None:
        payload = {
            "as_of": "2026-04-01",
            "generated_at": "2026-04-01T12:00:00",
            "ai_supercycle_evidence_ledger": {
                "rows": [
                    {
                        "market": "US",
                        "symbol": "GOOGL",
                        "name": "Alphabet",
                        "layer": "ai_labs_cloud_models",
                        "priority": 1,
                        "supply_chain_role": "AI lab, cloud and model distribution",
                        "evidence_state": "source_linked_supply_evidence",
                        "evidence_score": 0.9,
                        "rank_score": 70.0,
                    }
                ]
            },
            "ai_lab_quality_index": {
                "rows": [
                    {
                        "symbol": "GOOGL",
                        "data_status": "scored",
                        "lab_quality_score": 88.0,
                        "publication_sources": ["unit"],
                    }
                ]
            },
        }
        radar = v2.build_ai_supercycle_value_radar(payload, Path("/tmp/nonexistent-us.duckdb"), v2.parse_date("2026-04-01"))
        row = radar["rows"][0]
        self.assertEqual(row["lab_quality_score"], 88.0)
        self.assertEqual(row["lab_quality_status"], "scored")
        self.assertIn("unit", row["lab_quality_sources"])

    def test_source_linked_relationships_create_research_seed_evidence_rows(self) -> None:
        payload = {
            "as_of": "2026-04-01",
            "generated_at": "2026-04-01T12:00:00",
            "us_opportunity_ranker": {"all_rows": []},
            "cn_opportunity_ranker": {"all_rows": []},
            "ai_supply_chain_relationships": {
                "rows": [
                    {
                        "market": "US",
                        "primary_symbol": "NET",
                        "symbols": ["NET", "NVDA"],
                        "layer": "ai_datacenter_edge_infra",
                        "relationship_type": "edge_ai_inference_partner",
                        "supply_chain_role": "Cloudflare deploys NVIDIA GPUs at the edge.",
                        "source_name": "Cloudflare press release",
                        "source_url": "https://example.com/cloudflare",
                        "source_date": "2023-09-27",
                        "source_linked": True,
                    }
                ]
            },
        }
        ledger = v2.build_ai_supercycle_evidence_ledger(payload)
        row = ledger["rows"][0]
        self.assertEqual(row["symbol"], "NET")
        self.assertEqual(row["candidate_tier"], "relationship_research_seed")
        self.assertEqual(row["action"], "research_only_no_trade")
        self.assertEqual(row["evidence_state"], "source_linked_supply_evidence")

    def test_v2_report_writes_required_outputs_and_states(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            us_db = root / "us.duckdb"
            cn_db = root / "cn.duckdb"
            out = root / "reports"
            _make_us_db(us_db)
            _make_cn_db(cn_db)

            args = type(
                "Args",
                (),
                {
                    "date": "2026-04-01",
                    "start": "2026-03-01",
                    "output_root": out,
                    "us_db": us_db,
                    "cn_db": cn_db,
                },
            )()
            payload = v2.run(args)
            output_dir = out / payload["as_of"]
            text = (output_dir / "main_strategy_v2_backtest.md").read_text(encoding="utf-8")
            us_daily = (output_dir / "us_daily_report.md").read_text(encoding="utf-8")
            cn_daily = (output_dir / "cn_daily_report.md").read_text(encoding="utf-8")

            self.assertTrue((output_dir / "main_strategy_v2_backtest.json").exists())
            self.assertTrue((output_dir / "main_strategy_v2_backtest.duckdb").exists())
            self.assertTrue((output_dir / "strategy_direction.md").exists())
            self.assertTrue((output_dir / "strategy_direction.json").exists())
            self.assertTrue((output_dir / "portfolio_risk_overlay.md").exists())
            self.assertTrue((output_dir / "portfolio_risk_overlay.json").exists())
            self.assertTrue((output_dir / "option_shadow_ledger.md").exists())
            self.assertTrue((output_dir / "option_shadow_ledger.json").exists())
            self.assertTrue((output_dir / "earnings_calendar.md").exists())
            self.assertTrue((output_dir / "earnings_calendar.json").exists())
            self.assertTrue((output_dir / "ai_supercycle_evidence.md").exists())
            self.assertTrue((output_dir / "ai_supercycle_evidence.json").exists())
            self.assertTrue((output_dir / "ai_supply_chain_relationships.md").exists())
            self.assertTrue((output_dir / "ai_supply_chain_relationships.json").exists())
            self.assertTrue((output_dir / "ai_lab_quality_index.md").exists())
            self.assertTrue((output_dir / "ai_lab_quality_index.json").exists())
            self.assertTrue((output_dir / "ai_supercycle_value_radar.md").exists())
            self.assertTrue((output_dir / "ai_supercycle_value_radar.json").exists())
            self.assertTrue((output_dir / "ai_supercycle_layer_attribution.md").exists())
            self.assertTrue((output_dir / "ai_supercycle_layer_attribution.json").exists())
            self.assertTrue((output_dir / "cn_lifecycle_research.md").exists())
            self.assertTrue((output_dir / "cn_lifecycle_research.json").exists())
            self.assertTrue((output_dir / "profit_readiness.md").exists())
            self.assertTrue((output_dir / "profit_readiness.json").exists())
            self.assertTrue((output_dir / "pipeline_requirements_audit.md").exists())
            self.assertTrue((output_dir / "pipeline_requirements_audit.json").exists())
            self.assertTrue((output_dir / "us_opportunity_ranker.md").exists())
            self.assertTrue((output_dir / "us_opportunity_ranker.json").exists())
            self.assertTrue((output_dir / "us_opportunity_ranker.duckdb").exists())
            self.assertTrue((output_dir / "cn_opportunity_ranker.md").exists())
            self.assertTrue((output_dir / "cn_opportunity_ranker.json").exists())
            self.assertTrue((output_dir / "cn_opportunity_ranker.duckdb").exists())
            self.assertIn("## 今日交易决策 / Production Decision", text)
            self.assertIn("## 美股财报日历", text)
            self.assertIn("## A股财报披露日历", text)
            self.assertIn("V211", text)
            self.assertIn("平安银行", text)
            self.assertIn("## 美股财报日历", us_daily)
            self.assertIn("V2 Test Corp", us_daily)
            self.assertIn("## A股财报披露日历", cn_daily)
            self.assertIn("平安银行", cn_daily)
            self.assertIn("R 口径: 全部是归一化风险单位，不换算 RMB", text)
            self.assertIn("归一化 R 占用", text)
            self.assertIn("### 可交易 / Actionable", text)
            self.assertIn("### 只能观察 / Watch", text)
            self.assertIn("### 禁止碰 / 0R / 未闭环", text)
            self.assertIn("## 赚钱优先裁决 / Profit Guardrails", text)
            self.assertIn("## 赚钱落地缺口 / Profit Readiness", text)
            self.assertIn("## 管线需求审计 / Pipeline Requirements Audit", text)
            self.assertIn("## AI Supercycle Evidence Ledger", text)
            self.assertIn("## AI Supply Chain Relationship Ledger", text)
            self.assertIn("## AI Lab Quality Index", text)
            self.assertIn("## AI Supercycle 10x Value Radar", text)
            self.assertIn("## AI Supercycle Layer Attribution", text)
            self.assertIn("## 策略方向裁决 / Strategy Direction", text)
            self.assertIn("## 组合风险覆盖 / Portfolio Risk Overlay", text)
            self.assertIn("## US Option Shadow PnL Ledger", text)
            self.assertIn("## US Missed Alpha / Winner Hold Radar", text)
            self.assertIn("## 美股生产排序 / US Production Ranker", text)
            self.assertIn("## A 股生产排序 / CN Production Ranker", text)
            self.assertIn("## A 股生命周期研究 / CN Lifecycle", text)
            self.assertIn("## Adjustment Rules", text)
            self.assertIn("不是永久固化", text)
            self.assertIn("## 策略新鲜期 / Freshness", text)
            self.assertIn("Execution Alpha", text)
            self.assertIn("Ranked Watch", text)
            self.assertIn("Limit-Up Radar", text)
            self.assertIn("profit_guardrails", payload)
            self.assertIn("production_decision_summary", payload)
            self.assertIn("strategy_direction", payload)
            self.assertIn("portfolio_risk_overlay", payload)
            self.assertIn("option_shadow_ledger", payload)
            self.assertIn("earnings_calendar", payload)
            self.assertEqual(payload["earnings_calendar"]["us"]["rows"][0]["symbol"], "V211")
            self.assertEqual(payload["earnings_calendar"]["cn"]["rows"][0]["symbol"], "000001.SZ")
            self.assertIn("lifecycle", payload["cn"])
            self.assertIn("observed_lifecycle_prob", payload["cn"])
            self.assertIn("profit_readiness", payload)
            self.assertIn("pipeline_requirements_audit", payload)
            self.assertIn("ai_supercycle_evidence_ledger", payload)
            self.assertIn("ai_supply_chain_relationships", payload)
            self.assertIn("ai_lab_quality_index", payload)
            self.assertIn("ai_supercycle_value_radar", payload)
            self.assertIn("ai_supercycle_layer_attribution", payload)
            self.assertIn("missed_alpha_radar", payload["us"])
            self.assertIn("us_opportunity_ranker", payload)
            self.assertIn("cn_opportunity_ranker", payload)
            self.assertIn("p_win_t1", payload["cn"]["current"][0])
            self.assertIn("expected_r_t3", payload["cn"]["current"][0])
            self.assertGreaterEqual(len(payload["us"]["missed_alpha_radar"]), 1)
            self.assertGreaterEqual(len(payload["profit_readiness"]["rows"]), 1)
            self.assertGreaterEqual(payload["production_decision_summary"]["summary"]["cn_action_count"], 1)
            self.assertGreaterEqual(payload["production_decision_summary"]["summary"]["us_action_count"], 1)
            self.assertGreaterEqual(payload["profit_readiness"]["summary"]["money_ready_lines"], 2)
            self.assertEqual(payload["strategy_direction"][0]["strategy_family"], "oversold_contrarian")
            self.assertEqual(payload["cn"]["lifecycle"]["policy"]["state"], "opportunity_lifecycle")
            self.assertIn("lifecycle_action", payload["cn"]["current"][0])
            self.assertIn("v2_stock_only_net", payload["us"]["metrics"])
            self.assertIn("Stock-only bridge", text)
            self.assertGreater(payload["option_shadow_ledger"]["resolved_count"], 0)
            self.assertGreater(payload["us"]["metrics"]["v2"]["lcb80_pct"], 0)
            self.assertGreater(payload["us"]["metrics"]["v2_stock_only_net"]["lcb80_pct"], 0)
            self.assertLess(payload["us"]["metrics"]["legacy"]["lcb80_pct"], 0)
            con = duckdb.connect(str(output_dir / "main_strategy_v2_backtest.duckdb"), read_only=True)
            try:
                count = con.execute("SELECT COUNT(*) FROM strategy_direction").fetchone()[0]
                overlay_count = con.execute("SELECT COUNT(*) FROM portfolio_risk_overlay").fetchone()[0]
                ledger_count = con.execute("SELECT COUNT(*) FROM option_shadow_ledger").fetchone()[0]
                lifecycle_count = con.execute("SELECT COUNT(*) FROM cn_lifecycle_research").fetchone()[0]
                readiness_count = con.execute("SELECT COUNT(*) FROM profit_readiness").fetchone()[0]
                audit_count = con.execute("SELECT COUNT(*) FROM pipeline_requirements_audit").fetchone()[0]
                ai_evidence_count = con.execute("SELECT COUNT(*) FROM ai_supercycle_evidence").fetchone()[0]
                ai_relationship_count = con.execute("SELECT COUNT(*) FROM ai_supply_chain_relationships").fetchone()[0]
                ai_lab_count = con.execute("SELECT COUNT(*) FROM ai_lab_quality_index").fetchone()[0]
                value_radar_count = con.execute("SELECT COUNT(*) FROM ai_supercycle_value_radar").fetchone()[0]
                layer_attribution_count = con.execute("SELECT COUNT(*) FROM ai_supercycle_layer_attribution").fetchone()[0]
                us_ranker_count = con.execute("SELECT COUNT(*) FROM us_opportunity_ranker").fetchone()[0]
                cn_ranker_count = con.execute("SELECT COUNT(*) FROM cn_opportunity_ranker").fetchone()[0]
            finally:
                con.close()
            self.assertGreaterEqual(count, 4)
            self.assertGreaterEqual(overlay_count, 1)
            self.assertGreaterEqual(ledger_count, 1)
            self.assertGreaterEqual(lifecycle_count, 1)
            self.assertGreaterEqual(readiness_count, 1)
            self.assertGreaterEqual(audit_count, 1)
            self.assertGreaterEqual(ai_evidence_count, 0)
            self.assertGreaterEqual(ai_relationship_count, 0)
            self.assertGreaterEqual(ai_lab_count, 1)
            self.assertGreaterEqual(value_radar_count, 0)
            self.assertGreaterEqual(layer_attribution_count, 0)
            self.assertGreaterEqual(us_ranker_count, 1)
            self.assertGreaterEqual(cn_ranker_count, 1)


if __name__ == "__main__":
    unittest.main()
