from __future__ import annotations

import json
import sys
import unittest
from datetime import date
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class OvernightGateFormulaTests(unittest.TestCase):
    def test_trend_aligned_small_gap_gets_more_support(self) -> None:
        from quant_bot.analytics.overnight_gate import (
            _compute_continuation_probability,
            _compute_support_score,
            _discipline_support_score,
            _support_regime_bonus,
            _trend_alignment_score,
        )

        trend_alignment = _trend_alignment_score(
            gap_dir=1,
            trend_prob=0.5496,
            trend_regime="trending",
        )
        discipline_support = _discipline_support_score(
            gap_dir=1,
            gap_vs_expected_move=0.014,
            cone_position_68=0.353,
        )
        regime_bonus = _support_regime_bonus(
            gap_dir=1,
            trend_regime="trending",
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            flow_intensity=0.0,
            bias_support=1.0,
        )
        support_score = _compute_support_score(
            flow_intensity=0.0,
            iv_delta=0.312,
            skew_delta=0.243,
            pc_delta=0.553,
            bias_support=1.0,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            sentiment_support=1.0,
            regime_bonus=regime_bonus,
        )
        p_continue = _compute_continuation_probability(
            support_score=support_score,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            stretch_score=0.0,
            trend_regime="trending",
            gap_dir=1,
        )

        self.assertGreaterEqual(trend_alignment, 0.65)
        self.assertGreaterEqual(discipline_support, 0.80)
        self.assertGreaterEqual(support_score, 0.45)
        self.assertGreaterEqual(p_continue, 0.54)

    def test_eventless_noisy_name_stays_subcritical(self) -> None:
        from quant_bot.analytics.overnight_gate import (
            _compute_continuation_probability,
            _compute_support_score,
            _discipline_support_score,
            _support_regime_bonus,
            _trend_alignment_score,
        )

        trend_alignment = _trend_alignment_score(
            gap_dir=1,
            trend_prob=0.5411,
            trend_regime="noisy",
        )
        discipline_support = _discipline_support_score(
            gap_dir=1,
            gap_vs_expected_move=0.037,
            cone_position_68=0.465,
        )
        regime_bonus = _support_regime_bonus(
            gap_dir=1,
            trend_regime="noisy",
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            flow_intensity=0.0,
            bias_support=0.0,
        )
        support_score = _compute_support_score(
            flow_intensity=0.0,
            iv_delta=None,
            skew_delta=None,
            pc_delta=None,
            bias_support=0.0,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            sentiment_support=0.0,
            regime_bonus=regime_bonus,
        )
        p_continue = _compute_continuation_probability(
            support_score=support_score,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            stretch_score=0.0,
            trend_regime="noisy",
            gap_dir=1,
        )

        self.assertLessEqual(support_score, 0.35)
        self.assertLess(p_continue, 0.50)


class OvernightContinuationAlphaTests(unittest.TestCase):
    def test_high_stretch_gate_prefers_do_not_chase(self) -> None:
        from quant_bot.analytics.overnight_continuation_alpha import CalibrationStats, _score_current

        stats = CalibrationStats()
        for _ in range(8):
            stats.add("alpha_already_paid", "2026-04-01")
        for _ in range(4):
            stats.add("continuation", "2026-04-02")

        scored = _score_current(
            current={
                "gate": {
                    "action": "executable_now",
                    "p_continue": 0.57,
                    "p_fade": 0.42,
                    "support_score": 0.50,
                    "discipline_support": 0.45,
                    "trend_alignment": 0.55,
                    "effective_stretch_score": 0.86,
                    "gap_vs_expected_move": 1.18,
                },
                "options": {"liquidity_score": "fair"},
            },
            stats=stats,
        )

        self.assertEqual(scored["advice"], "do_not_chase")
        self.assertGreaterEqual(scored["paid_risk"], 0.62)

    def test_supported_low_stretch_gate_can_continue(self) -> None:
        from quant_bot.analytics.overnight_continuation_alpha import CalibrationStats, _score_current

        stats = CalibrationStats()
        for _ in range(10):
            stats.add("continuation", "2026-04-03")
        for _ in range(2):
            stats.add("fade", "2026-04-04")

        scored = _score_current(
            current={
                "gate": {
                    "action": "executable_now",
                    "p_continue": 0.66,
                    "p_fade": 0.24,
                    "support_score": 0.68,
                    "discipline_support": 0.74,
                    "trend_alignment": 0.71,
                    "effective_stretch_score": 0.18,
                    "gap_vs_expected_move": 0.32,
                },
                "options": {"liquidity_score": "good"},
                "lab_factor": {"composite": 0.20},
            },
            stats=stats,
        )

        self.assertEqual(scored["advice"], "continue")
        self.assertGreaterEqual(scored["entry_quality"], 0.56)


class OvernightGateMarketQuoteTests(unittest.TestCase):
    def _db(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE prices_daily (
                symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume BIGINT, adj_close DOUBLE
            );
            CREATE TABLE analysis_daily (
                symbol VARCHAR, date DATE, module_name VARCHAR,
                trend_prob DOUBLE, p_upside DOUBLE, p_downside DOUBLE,
                daily_risk_usd DOUBLE, expected_move_pct DOUBLE,
                z_score DOUBLE, p_value_raw DOUBLE, p_value_bonf DOUBLE,
                strength_bucket VARCHAR, regime VARCHAR, details VARCHAR
            );
            CREATE TABLE options_analysis (
                symbol VARCHAR, as_of DATE, expiry VARCHAR, days_to_exp INTEGER,
                current_price DOUBLE, range_68_low DOUBLE, range_68_high DOUBLE,
                range_95_low DOUBLE, range_95_high DOUBLE, atm_iv DOUBLE,
                iv_skew DOUBLE, put_call_vol_ratio DOUBLE, bias_signal VARCHAR,
                liquidity_score VARCHAR, chain_width INTEGER, avg_spread_pct DOUBLE,
                unusual_strikes VARCHAR
            );
            CREATE TABLE options_snapshot (
                symbol VARCHAR, as_of DATE, expiry VARCHAR, days_to_exp INTEGER,
                atm_iv DOUBLE, expected_move_pct DOUBLE, put_call_vol_ratio DOUBLE
            );
            CREATE TABLE market_quotes (
                symbol VARCHAR, as_of DATE, session VARCHAR, quote_time TIMESTAMP,
                regular_market_price DOUBLE, premarket_price DOUBLE,
                postmarket_price DOUBLE, last_price DOUBLE, previous_close DOUBLE,
                active_price DOUBLE, active_price_source VARCHAR, currency VARCHAR,
                source VARCHAR, raw_json VARCHAR
            );
            """
        )
        return con

    def test_premarket_quote_overrides_stale_options_reference(self) -> None:
        from quant_bot.analytics.overnight_gate import run_overnight_gate

        con = self._db()
        as_of = date(2026, 6, 1)
        con.execute(
            "INSERT INTO prices_daily VALUES ('ABC', '2026-05-29', 100, 102, 98, 100, 1000000, 100)"
        )
        con.execute(
            """
            INSERT INTO analysis_daily VALUES
            ('ABC', '2026-06-01', 'momentum_risk',
             0.62, 0.60, 0.30, 3.0, NULL, NULL, NULL, NULL,
             'strong', 'trending', '{}')
            """
        )
        con.execute(
            """
            INSERT INTO options_analysis VALUES
            ('ABC', '2026-06-01', '2026-06-19', 18,
             101, 94, 106, 88, 112, 0.40, 0.90, 0.60,
             'bullish', 'good', 20, 0.03, NULL)
            """
        )
        con.execute(
            """
            INSERT INTO options_snapshot VALUES
            ('ABC', '2026-06-01', '2026-06-19', 18, 40, 5.0, 0.60)
            """
        )
        con.execute(
            """
            INSERT INTO market_quotes VALUES
            ('ABC', '2026-06-01', 'pre', '2026-06-01 12:55:00',
             100.5, 108.0, NULL, 108.0, 100.0, 108.0,
             'premarket_price', 'USD', 'yfinance_yahoo_delayed', '{}')
            """
        )

        df = run_overnight_gate(con, ["ABC"], as_of, session="pre")
        row = df.to_dicts()[0]
        details = json.loads(row["details"])

        self.assertEqual(details["ref_price"], 108.0)
        self.assertEqual(details["ref_price_source"], "market_quotes.premarket_price")
        self.assertEqual(details["quote_session"], "pre")
        self.assertAlmostEqual(details["gap_pct"], 8.0)
        self.assertAlmostEqual(row["daily_risk_usd"], 8.0)


if __name__ == "__main__":
    unittest.main()
