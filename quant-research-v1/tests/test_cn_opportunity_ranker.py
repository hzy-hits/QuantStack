from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_ROOT = REPO_ROOT.parent
QUANT_V1_SRC = REPO_ROOT / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import cn_opportunity_ranker as ranker  # noqa: E402


def _write_ai_infra_universe(root: Path, rows: list[dict]) -> None:
    path = root / "data" / "global_universe_v2.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")


def _make_v2_report(root: Path) -> None:
    out = root / "2026-05-06"
    out.mkdir(parents=True)
    payload = {
        "as_of": "2026-05-06",
        "cn": {
            "current": [
                {
                    "symbol": "000001.SZ",
                    "name": "强机会",
                    "industry": "银行",
                    "state": "Execution Alpha",
                    "policy": "oversold_contrarian",
                    "ev_pct": 2.0,
                    "ev_lcb80_pct": 0.2,
                    "risk_unit_pct": 2.0,
                    "strategy_samples": 20,
                    "strategy_fills": 12,
                    "denoise_residual_zscore": -2.2,
                    "log_return_20d_pct": -22.0,
                    "rsi_14": 22.0,
                    "observation_entry_zone": "10.00-10.05",
                    "handling_line": 9.7,
                    "first_target": 10.3,
                    "reason": "synthetic strong opportunity",
                },
                {
                    "symbol": "000002.SZ",
                    "name": "弱机会",
                    "industry": "地产",
                    "state": "Execution Alpha",
                    "policy": "oversold_contrarian",
                    "ev_pct": -1.0,
                    "ev_lcb80_pct": -2.0,
                    "risk_unit_pct": 4.0,
                    "strategy_samples": 3,
                    "strategy_fills": 1,
                    "denoise_residual_zscore": -0.2,
                    "log_return_20d_pct": -2.0,
                    "rsi_14": 55.0,
                    "observation_entry_zone": "20.00-20.05",
                    "handling_line": 19.1,
                    "first_target": 20.9,
                    "reason": "synthetic weak opportunity",
                },
            ]
        },
    }
    (out / "main_strategy_v2_backtest.json").write_text(json.dumps(payload), encoding="utf-8")


def _make_cn_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE strategy_model_dataset (
            report_date DATE, evaluation_date DATE, symbol VARCHAR, features_json VARCHAR,
            detail_json VARCHAR, strategy_key VARCHAR, action_intent VARCHAR,
            alpha_state VARCHAR, ev_norm_score DOUBLE, ev_norm_lcb_80 DOUBLE,
            p_fill DOUBLE, mu_ret_pct DOUBLE, tail_loss_pct DOUBLE,
            planned_entry DOUBLE, reference_close DOUBLE
        )
        """
    )
    strong_features = json.dumps(
        {
            "setup_score": 0.9,
            "shadow_alpha_prob": 0.8,
            "stale_chase_risk": 0.1,
            "fade_risk": 0.1,
            "downside_stress": 0.1,
            "details": {"entry_quality_score": 0.9, "execution_score": 0.85},
        }
    )
    weak_features = json.dumps(
        {
            "setup_score": 0.2,
            "shadow_alpha_prob": 0.2,
            "stale_chase_risk": 0.9,
            "fade_risk": 0.8,
            "downside_stress": 0.8,
            "details": {"entry_quality_score": 0.2, "execution_score": 0.25},
        }
    )
    con.execute(
        "INSERT INTO strategy_model_dataset VALUES ('2026-05-06','2026-05-06','000001.SZ', ?, '{}', 'strong', 'TRADE', 'positive_ev_setup', 90, 80, 0.8, 2.0, -1.0, 10, 10)",
        [strong_features],
    )
    con.execute(
        "INSERT INTO strategy_model_dataset VALUES ('2026-05-06','2026-05-06','000002.SZ', ?, '{}', 'weak', 'TRADE', 'blocked_negative_ev', 10, 5, 0.2, -1.0, -3.0, 20, 20)",
        [weak_features],
    )
    con.execute(
        """
        CREATE TABLE daily_basic (
            ts_code VARCHAR, trade_date DATE, turnover_rate DOUBLE, volume_ratio DOUBLE,
            pe_ttm DOUBLE, pb DOUBLE, total_mv DOUBLE, circ_mv DOUBLE
        )
        """
    )
    con.execute("INSERT INTO daily_basic VALUES ('000001.SZ','2026-05-06',5,2,10,1,100000,80000)")
    con.execute("INSERT INTO daily_basic VALUES ('000002.SZ','2026-05-06',0.5,0.8,30,3,200000,160000)")
    con.execute(
        """
        CREATE TABLE prices (
            ts_code VARCHAR, trade_date DATE, close DOUBLE, pct_chg DOUBLE, amount DOUBLE
        )
        """
    )
    con.execute("INSERT INTO prices VALUES ('000001.SZ','2026-05-06',10,1,90000)")
    con.execute("INSERT INTO prices VALUES ('000002.SZ','2026-05-06',20,-1,10000)")
    con.execute(
        """
        CREATE TABLE moneyflow (
            ts_code VARCHAR, trade_date DATE, net_mf_amount DOUBLE, net_mf_vol DOUBLE,
            buy_lg_amount DOUBLE, buy_elg_amount DOUBLE, sell_lg_amount DOUBLE, sell_elg_amount DOUBLE
        )
        """
    )
    con.execute("INSERT INTO moneyflow VALUES ('000001.SZ','2026-05-06',2000,100,3000,1000,500,200)")
    con.execute("INSERT INTO moneyflow VALUES ('000002.SZ','2026-05-06',-2000,-100,500,200,3000,1000)")
    con.execute(
        """
        CREATE TABLE margin_detail (
            ts_code VARCHAR, trade_date DATE, rzye DOUBLE, rzmre DOUBLE,
            rzche DOUBLE, rqye DOUBLE
        )
        """
    )
    for idx in range(6):
        day = f"2026-04-{30 - idx:02d}"
        con.execute("INSERT INTO margin_detail VALUES ('000001.SZ', ?, ?, 100, 20, 1)", [day, 1000 + idx * 20])
        con.execute("INSERT INTO margin_detail VALUES ('000002.SZ', ?, ?, 20, 100, 1)", [day, 1000 - idx * 20])
    con.execute(
        """
        CREATE TABLE analytics (
            ts_code VARCHAR, as_of DATE, module VARCHAR, metric VARCHAR, value DOUBLE, detail VARCHAR
        )
        """
    )
    analytics_rows = [
        ("000001.SZ", "flow", "information_score", 0.9),
        ("000001.SZ", "flow", "large_flow_z", 2.0),
        ("000001.SZ", "flow", "margin_z", 1.0),
        ("000001.SZ", "lab_factor", "lab_composite", 0.8),
        ("000001.SZ", "mean_reversion", "reversion_score", 0.9),
        ("000002.SZ", "flow", "information_score", 0.1),
        ("000002.SZ", "flow", "large_flow_z", -2.0),
        ("000002.SZ", "flow", "margin_z", -1.0),
        ("000002.SZ", "lab_factor", "lab_composite", 0.2),
        ("000002.SZ", "mean_reversion", "reversion_score", 0.1),
    ]
    for symbol, module, metric, value in analytics_rows:
        con.execute("INSERT INTO analytics VALUES (?, '2026-05-06', ?, ?, ?, NULL)", [symbol, module, metric, value])
    con.execute(
        """
        CREATE TABLE limit_up_model_predictions (
            as_of DATE, symbol VARCHAR, p_limit_up DOUBLE, p_touch_limit DOUBLE,
            p_failed_board DOUBLE, ev_after_cost_pct DOUBLE, ev_lcb_80_pct DOUBLE,
            probability_decile INTEGER, model_state VARCHAR, decision_state VARCHAR
        )
        """
    )
    con.execute("INSERT INTO limit_up_model_predictions VALUES ('2026-05-06','000001.SZ',0.2,0.35,0.02,1.0,0.3,10,'trained','candidate')")
    con.execute("INSERT INTO limit_up_model_predictions VALUES ('2026-05-06','000002.SZ',0.01,0.02,0.3,-1.0,-2.0,1,'trained','heat_only')")
    con.execute(
        """
        CREATE TABLE sector_fund_flow (
            trade_date DATE, sector_name VARCHAR, pct_chg DOUBLE, main_net_in DOUBLE,
            main_net_pct DOUBLE, super_net_in DOUBLE, big_net_in DOUBLE
        )
        """
    )
    con.execute("INSERT INTO sector_fund_flow VALUES ('2026-05-06','银行',1,1000000,2,500000,500000)")
    con.execute("INSERT INTO sector_fund_flow VALUES ('2026-05-06','地产',-1,-1000000,-2,-500000,-500000)")
    con.execute(
        """
        CREATE TABLE news_enriched (
            ts_code VARCHAR, published_at VARCHAR, headline VARCHAR, event_type VARCHAR,
            sentiment VARCHAR, sentiment_confidence DOUBLE, relevance DOUBLE,
            key_entities VARCHAR, key_metrics VARCHAR, summary_one_line VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO news_enriched
        VALUES ('000002.SZ', '2026-05-06', '弱机会大幅调低去年业绩并被质疑虚假陈述',
                'earnings', 'negative', 0.95, 1.0, '[]', '{}', '严重财报事件')
        """
    )
    con.close()


class CnOpportunityRankerTests(unittest.TestCase):
    def test_ai_infra_expand_filters_cn_candidates_and_keeps_bfs_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ai_root = Path(tmpdir) / "ai_infra"
            _write_ai_infra_universe(
                ai_root,
                [
                    {
                        "asset_pool": "中国A股资产池",
                        "market_country": "CN",
                        "ticker": "300308.SZ",
                        "company": "中际旭创",
                        "bfs_depth": "D3",
                        "module": "CPO optical",
                        "current_pool": "核心候选",
                        "total_score": 92,
                        # Production-grade evidence so enforce_expand keeps the row
                        # under the production pool gate (added 2026-05-15).
                        "evidence_state": "原文已证明: 800G CPO product roadmap and customer wins",
                    },
                    {
                        "asset_pool": "中国A股资产池",
                        "market_country": "CN",
                        "ticker": "002281.SZ",
                        "company": "光迅科技",
                        "bfs_depth": "D3-D4",
                        "module": "光模块",
                        "current_pool": "候选池",
                        "total_score": 84,
                        "evidence_state": "合理推论: optical module revenue mix",
                    },
                ],
            )

            payload = ranker.build_ranker_payload(
                as_of=ranker.date(2026, 5, 12),
                candidates=[{"symbol": "002281.SZ", "name": "光迅科技"}, {"symbol": "600519.SH", "name": "消费票"}],
                candidate_status="unit",
                cn_db=ai_root / "missing.duckdb",
                top=10,
                ai_infra_root=ai_root,
                ai_infra_mode="enforce_expand",
            )
            by_symbol = {row["symbol"]: row for row in payload["all_rows"]}

            self.assertEqual(set(by_symbol), {"002281.SZ", "300308.SZ"})
            self.assertEqual(payload["ai_infra_gate"]["retained_candidate_count"], 1)
            self.assertEqual(payload["ai_infra_gate"]["added_universe_count"], 1)
            self.assertIn("600519.SH", payload["ai_infra_gate"]["excluded_symbols"])
            self.assertEqual(by_symbol["002281.SZ"]["ai_infra_bfs_depth"], "D3-D4")
            self.assertEqual(by_symbol["300308.SZ"]["ai_infra_module"], "CPO optical")
            self.assertTrue(all(row.get("ai_infra_universe") for row in payload["all_rows"]))
            self.assertTrue(
                all(row.get("ai_infra_universe") for row in payload.get("production_basket") or []),
                "production_basket must only contain ai_infra_universe members",
            )

    def test_ai_infra_right_side_tape_leadership_can_become_execution_sleeve(self) -> None:
        rows = ranker.score_rows(
            [
                {
                    "symbol": "601179.SH",
                    "name": "中国西电",
                    "ai_infra_universe": True,
                    "ai_infra_evidence_state": "原文已证明: AI/data-center transformer revenue",
                    "narrative_group": "ai_infra",
                    "supercycle_layer": "ai_power_nuclear_grid",
                    "supercycle_priority": 2,
                    "pct_chg": 2.5,
                    "ret_5d": 9.0,
                    "ret_20d": 18.0,
                    "volume_ratio": 2.0,
                    "turnover_rate": 3.0,
                    "flow_volume_confirmation": 1.7,
                    "flow_information_score": 0.92,
                    "flow_tape_z": 2.0,
                    "flow_large_flow_z": 1.8,
                    "net_mf_pct_circ_mv": 0.7,
                    "large_net_pct_circ_mv": 0.45,
                    "extra_large_net_pct_circ_mv": 0.25,
                    "rzye_5d_delta_pct": 1.2,
                    "sector_main_net_pct": 1.0,
                    "sector_pct_chg": 1.3,
                    "p_touch_limit": 0.35,
                    "p_limit_up": 0.12,
                    "p_failed_board": 0.02,
                    "amount": 500000,
                    "circ_mv": 2000000,
                }
            ]
        )

        self.assertEqual(rows[0]["alpha_sleeve_id"], ranker.CN_TAPE_LEADERSHIP_SLEEVE)
        self.assertEqual(rows[0]["execution_source"], "ai_infra_tape_leadership_runtime")
        self.assertEqual(rows[0]["production_tier"], "top_stock_trade")
        self.assertEqual(rows[0]["production_action"], "buy_planned_entry")

    def test_pending_evidence_blocks_tape_promotion(self) -> None:
        """待原文核验 names cannot be promoted to top_stock_trade by tape alone.

        Codex review 2026-05-14: 603690.SH was hitting top_stock_trade despite
        ai_infra_evidence_state being pending. The secondary evidence gate must
        force ranked_watch regardless of tape strength.
        """
        rows = ranker.score_rows(
            [
                {
                    "symbol": "603690.SH",
                    "name": "中国西电（pending）",
                    "ai_infra_universe": True,
                    "ai_infra_evidence_state": "待原文核验: AI revenue mix",
                    "narrative_group": "ai_infra",
                    "supercycle_layer": "ai_power_nuclear_grid",
                    "supercycle_priority": 2,
                    "pct_chg": 2.5, "ret_5d": 9.0, "ret_20d": 18.0,
                    "volume_ratio": 2.0, "turnover_rate": 3.0,
                    "flow_volume_confirmation": 1.7, "flow_information_score": 0.92,
                    "flow_tape_z": 2.0, "flow_large_flow_z": 1.8,
                    "net_mf_pct_circ_mv": 0.7, "large_net_pct_circ_mv": 0.45,
                    "extra_large_net_pct_circ_mv": 0.25, "rzye_5d_delta_pct": 1.2,
                    "sector_main_net_pct": 1.0, "sector_pct_chg": 1.3,
                    "p_touch_limit": 0.35, "p_limit_up": 0.12,
                    "p_failed_board": 0.02, "amount": 500000, "circ_mv": 2000000,
                }
            ]
        )
        self.assertEqual(rows[0]["production_tier"], "ranked_watch")
        self.assertEqual(rows[0]["production_action"], "evidence_state_pending_no_trade")

    def test_cn_supercycle_profile_prioritizes_ai_infra_over_consumer(self) -> None:
        semiconductor = ranker.cn_supercycle_profile({"industry": "半导体", "name": "先进封测"})
        consumer = ranker.cn_supercycle_profile({"industry": "白酒", "name": "消费票"})

        self.assertEqual(semiconductor["supercycle_priority"], 1)
        self.assertEqual(semiconductor["supercycle_layer"], "ai_chip_equipment_materials_packaging")
        self.assertGreater(ranker.cn_narrative_fit({"industry": "半导体"}), ranker.cn_narrative_fit({"industry": "白酒"}))
        self.assertEqual(consumer["supercycle_layer"], "excluded_consumer")

    def test_ranker_outputs_production_tiers_without_hard_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            v2_root = root / "main_strategy_v2"
            db_path = root / "cn.duckdb"
            output_root = root / "ranker"
            _make_v2_report(v2_root)
            _make_cn_db(db_path)

            args = type(
                "Args",
                (),
                {
                    "date": "2026-05-06",
                    "v2_root": v2_root,
                    "cn_db": db_path,
                    "output_root": output_root,
                    "config": None,
                    "top": 10,
                },
            )()
            payload = ranker.run(args)
            rows = payload["all_rows"]

            self.assertEqual(payload["candidate_count"], 2)
            self.assertEqual(rows[0]["symbol"], "000001.SZ")
            self.assertEqual(rows[0]["production_tier"], "top_stock_trade")
            self.assertEqual(rows[0]["production_action"], "buy_planned_entry")
            self.assertIn("price_first_signal", rows[0]["score_components"])
            self.assertIn("informed_flow", rows[0]["score_components"])
            self.assertNotIn("research_only", rows[0]["production_tier"])
            self.assertEqual(rows[1]["symbol"], "000002.SZ")
            self.assertEqual(rows[1]["production_tier"], "event_risk_watch")
            self.assertEqual(rows[1]["production_action"], "negative_headline_no_probe")
            self.assertTrue((output_root / "2026-05-06" / "cn_opportunity_ranker.json").exists())
            self.assertTrue((output_root / "2026-05-06" / "cn_opportunity_ranker.md").exists())
            self.assertTrue((output_root / "2026-05-06" / "cn_opportunity_ranker.duckdb").exists())
            text = (output_root / "2026-05-06" / "cn_opportunity_ranker.md").read_text(encoding="utf-8")
            self.assertIn("cn_observed_lifecycle_prob", text)
            self.assertNotIn("只有 Alpha Factory 已证明 sleeve 可以产出 Execution Alpha", text)


if __name__ == "__main__":
    unittest.main()
