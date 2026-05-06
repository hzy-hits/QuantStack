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
            self.assertEqual(rows[0]["production_tier"], "top_probe")
            self.assertEqual(rows[0]["production_action"], "planned_entry_probe")
            self.assertNotIn("research_only", rows[0]["production_tier"])
            self.assertEqual(rows[1]["symbol"], "000002.SZ")
            self.assertEqual(rows[1]["production_tier"], "event_risk_watch")
            self.assertEqual(rows[1]["production_action"], "negative_headline_no_probe")
            self.assertTrue((output_root / "2026-05-06" / "cn_opportunity_ranker.json").exists())
            self.assertTrue((output_root / "2026-05-06" / "cn_opportunity_ranker.md").exists())
            self.assertTrue((output_root / "2026-05-06" / "cn_opportunity_ranker.duckdb").exists())


if __name__ == "__main__":
    unittest.main()
