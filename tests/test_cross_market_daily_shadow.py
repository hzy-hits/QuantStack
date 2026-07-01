from __future__ import annotations

import argparse
import importlib.util
import sys
import types
from datetime import date
from pathlib import Path
from unittest import mock

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "agents" / "run_cross_market_daily_shadow.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_cross_market_daily_shadow", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def disable_live_leaps_fallback(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_LIVE_LEAPS_IV_FALLBACK", "0")


def artifact(module, market: str, report_date: str, tmp_path: Path):
    payload = {
        "as_of": report_date,
        "production_decision_summary": {
            "summary": {
                "us_r": 0.125,
                "us_action_count": 1,
                "cn_r": 0.05,
                "cn_action_count": 1,
            },
            "actionable": [
                {"market": "US", "symbol": "NVDA", "size_r": 0.125, "evidence_state": "原文已证明"},
                {"market": "CN", "symbol": "000063.SZ", "size_r": 0.05, "evidence_state": "原文已证明"},
                {"market": "CN", "symbol": "688981.SH", "size_r": 0.04, "evidence_state": "原文已证明"},
            ],
        },
    }
    report_dir = tmp_path / report_date
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "main_strategy_v2_backtest.json").write_text(
        module.json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    md_path = report_dir / f"{market}_daily_report.md"
    md_path.write_text(f"# {market} report\n\nfixture", encoding="utf-8")
    return module.MarketArtifact(
        market=market,
        report_date=report_date,
        report_dir=report_dir,
        payload=payload,
        markdown=md_path.read_text(encoding="utf-8"),
        markdown_path=md_path,
    )


def test_pm_packet_keeps_us_to_cn_causality(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    packet = module.build_packet("pm", cn, us)

    assert packet["causal_direction"] == "US -> CN"
    assert packet["lead_market"] == "US"
    assert packet["target_market"] == "CN"
    assert packet["cn_role"] == "feedback_only"
    assert "不反向约束美股" in packet["thesis"]
    assert packet["agent_operating_mode"]["mode"] == "heuristic_tool_use"
    assert packet["data_boundary"]["fetch_workers"].startswith("Own data collection")
    assert any(tool["name"] == "select_cross_market_transmission" for tool in packet["tool_manifest"])
    assert any(tool["name"] == "finance-search.quant_stack_spine_triage" for tool in packet["tool_manifest"])
    assert any(tool["name"] == "finance-search.get_market_snapshot" for tool in packet["tool_manifest"])
    assert any(tool["name"] == "finance-search.search_news" for tool in packet["tool_manifest"])
    assert any(tool["name"] == "finance-search.quant_stack_ranker" for tool in packet["tool_manifest"])
    assert any(tool["name"] == "finance-search.quant_stack_sec_13f_recent" for tool in packet["tool_manifest"])
    assert "external_context_requirements" in packet
    assert "科创板" in module.json.dumps(packet["cn_universe_requirement"], ensure_ascii=False)
    assert packet["style_brief"]["reference_url"].startswith("https://boist.org/")


def test_cn_summary_mixes_star_candidates_into_cn_pipeline(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    cn.payload["cn_opportunity_ranker"] = {
        "all_rows": [
            {"symbol": "688535.SH", "name": "华海诚科", "production_tier": "active_watch", "rank": 20, "rank_score": 70.7},
            {"symbol": "688233.SH", "name": "神工股份", "production_tier": "active_watch", "rank": 18, "rank_score": 70.87},
            {"symbol": "688019.SH", "name": "安集科技", "production_tier": "bench_ranked", "rank": 50, "rank_score": 60.1},
            {"symbol": "600519.SH", "name": "主板样本", "production_tier": "active_watch", "rank": 1, "rank_score": 99.0},
        ]
    }

    summary = module.summarize_artifact(cn)

    symbols = [row["symbol"] for row in summary["pipeline_candidates"]]
    assert symbols[0] == "600519.SH"
    assert "688233.SH" in symbols
    assert "688535.SH" in symbols
    star = next(row for row in summary["pipeline_candidates"] if row["symbol"] == "688233.SH")
    assert star["board"] == "科创板"
    assert star["pipeline_stage"] == "active_watch"


def test_cn_pipeline_section_renders_star_inside_a_share_pipeline(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    cn.payload["cn_opportunity_ranker"] = {
        "all_rows": [
            {
                "symbol": "688233.SH",
                "name": "神工股份",
                "production_tier": "active_watch",
                "rank": 20,
                "rank_score": 70.18,
                "observation_entry_zone": "194.33-198.22",
                "handling_line": "回落后重新站回入场区",
            }
        ]
    }
    packet = module.build_packet("am", cn, us)

    section = module.render_cn_pipeline_section(packet)

    assert "## A股执行与候选管线" in section
    assert "688233.SH" in section
    assert "神工股份" in section
    assert "观察候选" in section
    assert "194.33-198.22" in section
    assert "## A股科创板候选管线" not in section


def test_us_summary_includes_compact_option_context(tmp_path: Path) -> None:
    module = load_module()
    us = artifact(module, "us", "2026-06-29", tmp_path)
    us.payload["production_decision_summary"]["actionable"].append(
        {"market": "US", "symbol": "NVDA", "size_r": 0.05, "evidence_state": "期权确认"}
    )
    us.payload["gamma_spring"] = {
        "effective_date": "2026-06-26",
        "sign_convention": "calls_positive_puts_negative",
        "rows": [
            {
                "symbol": "SPY",
                "state": "MIXED_GAMMA_FIELD",
                "gex_curve_state": "NEGATIVE_GEX_ACCEL_ZONE",
                "gex_flip_regime": "negative_acceleration_risk_off",
                "spot": 728.99,
                "spot_price_date": "2026-06-26",
                "zero_gamma_band": [742.0432, 749.3331],
                "positive_gex_pin_zone": [750.8597, 874.788],
                "negative_gex_accel_zone": [583.192, 743.5698],
                "call_wall_strike": 733,
                "put_wall_strike": 732,
                "dealer_pressure_proxy": 0.2077,
                "management_signal": "reduce_or_tighten_stop",
            },
            {"symbol": "NVDA", "state": "ZERO_GAMMA_TRANSITION", "spot": 192.53},
        ],
    }
    us.payload["option_shadow_ledger"] = {
        "status": "ok",
        "rows_with_legs": 1676,
        "all_real_bid_ask_resolved_count": 1676,
        "all_real_bid_ask_unresolved_count": 429,
        "summary": {
            "overall_long": {"lcb80_pct": -44.78},
            "all_options_alpha_real_bid_ask": {"lcb80_pct": -72.75, "win_rate": 0.1282},
        },
    }
    us.payload["options_anomaly_rows"] = []
    us.payload["options_tenor_signals"] = []
    us.payload["options_verdicts"] = {
        "NVDA": {
            "effective_date": "2026-06-26",
            "verdict": "定位中性",
            "iv_ann": 0.2798,
            "iv_rank_pct": 12.0,
            "iv_rank_n": 35,
            "iv_hv": 0.594,
            "pc_ratio_z": 0.1245,
            "skew_z": -0.1612,
        }
    }
    us.payload["long_dated_iv_history"] = {
        "NVDA": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-26",
            "atm_iv": 0.30,
            "avg_dte": 370,
            "rank_pct": 12.0,
            "rank_n": 35,
            "coverage_status": "ok",
        }
    }

    summary = module.summarize_artifact(us)
    option_context = summary["option_context"]

    assert option_context["gamma_effective_date"] == "2026-06-26"
    assert option_context["gamma_rows"][0]["symbol"] == "SPY"
    assert option_context["gamma_rows"][0]["zero_gamma_band"] == [742.04, 749.33]
    assert option_context["option_shadow_ledger"]["all_options_alpha_lcb80_pct"] == -72.75
    assert option_context["options_anomaly_radar"]["status"] == "no_trigger"
    assert option_context["options_tenor_radar"]["status"] == "no_signal"
    assert option_context["options_verdicts"][0]["symbol"] == "NVDA"
    assert option_context["options_attention_watchlist"][0]["symbol"] == "NVDA"
    assert "LEAPS IV" in option_context["options_attention_watchlist"][0]["reason"]


def test_us_options_attention_section_includes_otm_skew_and_leaps_iv(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    us.payload["options_anomaly_rows"] = [
        {
            "symbol": "DLR",
            "as_of": "2026-06-29",
            "far_otm_put_volume": 1200,
            "far_otm_put_vol_oi_ratio": 2.4,
            "pc_ratio_z": 1.8,
            "skew_z": 3.2,
            "selling_pressure_score": 188,
        }
    ]
    us.payload["options_tenor_signals"] = [
        {
            "symbol": "ALAB",
            "pattern": "insider_tilt_long_dated_calls",
            "score": 42,
            "guidance": "长端远 OTM call 占主导",
            "evidence": {"long_horizon_far_otm_call": 2000, "weekly_far_otm_call": 100},
        }
    ]
    us.payload["options_verdicts"] = {
        "DLR": {
            "effective_date": "2026-06-29",
            "verdict": "put 偏空 | 信仰久期中",
            "iv_ann": 0.2464,
            "iv_rank_pct": 16,
            "iv_rank_n": 35,
            "iv_hv": 0.8355,
            "pc_ratio_z": 1.8,
            "skew_z": 3.2,
        },
        "ALAB": {
            "effective_date": "2026-06-29",
            "verdict": "定位中性 | 信仰久期长(远月堆积)",
            "iv_ann": 0.9971,
            "iv_rank_pct": 20,
            "iv_rank_n": 35,
            "iv_hv": 0.9332,
            "pc_ratio_z": 0.1,
            "skew_z": 1.6,
        },
    }
    us.payload["long_dated_iv_history"] = {
        "DLR": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.25,
            "avg_dte": 365,
            "rank_pct": 16,
            "rank_n": 35,
            "coverage_status": "ok",
        },
        "ALAB": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.99,
            "avg_dte": 390,
            "rank_pct": 20,
            "rank_n": 35,
            "coverage_status": "ok",
        },
    }
    packet = module.build_packet("am", cn, us)

    section = module.render_us_options_attention_section(packet)

    assert "## 美股期权关注标的（OTM skew / LEAPS IV）" in section
    assert "DLR" in section
    assert "ALAB" in section
    assert "远 OTM 异常" in section
    assert "LEAPS tenor 异动" in section
    assert "0R context" in section


def test_us_options_attention_does_not_promote_low_iv_rank_with_short_history(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    us.payload["options_verdicts"] = {
        "BTBT": {
            "effective_date": "2026-06-29",
            "verdict": "定位中性 | IV 110%·贵(IV>实际波动)·IV/HV 1.18x | 下行恐惧高(put skew 抬升)",
            "iv_ann": 1.0989,
            "iv_rank_pct": 0,
            "iv_rank_n": 10,
            "iv_hv": 1.1764,
            "pc_ratio_z": -0.1355,
            "skew_z": 1.3744,
        },
        "RDDT": {
            "effective_date": "2026-06-29",
            "verdict": "定位中性 | IV 69%·便宜(IV<实际波动)·IV/HV 0.86x | 信仰久期中",
            "iv_ann": 0.6927,
            "iv_rank_pct": 0,
            "iv_rank_n": 6,
            "iv_hv": 0.8627,
            "pc_ratio_z": 0.4637,
            "skew_z": 0.4458,
        },
        "NVDA": {
            "effective_date": "2026-06-29",
            "verdict": "定位中性",
            "iv_ann": 0.2798,
            "iv_rank_pct": 12,
            "iv_rank_n": 40,
            "iv_hv": 0.594,
            "pc_ratio_z": 0.1245,
            "skew_z": -0.1612,
        },
    }
    us.payload["long_dated_iv_history"] = {
        "BTBT": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 1.10,
            "avg_dte": 570,
            "rank_pct": 0,
            "rank_n": 10,
            "coverage_status": "short_history",
        },
        "RDDT": {
            "tenor": "远月",
            "effective_date": "2026-06-29",
            "atm_iv": 0.69,
            "avg_dte": 207,
            "rank_pct": 0,
            "rank_n": 6,
            "coverage_status": "short_history",
        },
        "NVDA": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.28,
            "avg_dte": 365,
            "rank_pct": 12,
            "rank_n": 40,
            "coverage_status": "ok",
        },
    }
    packet = module.build_packet("am", cn, us)
    watch = packet["us"]["option_context"]["options_attention_watchlist"]
    by_symbol = {row["symbol"]: row for row in watch}

    assert "BTBT" in by_symbol
    assert "OTM skew 偏离" in by_symbol["BTBT"]["reason"]
    assert "LEAPS IV 低位" not in by_symbol["BTBT"]["reason"]
    assert "RDDT" not in by_symbol
    assert "NVDA" in by_symbol
    assert "LEAPS IV 低位" in by_symbol["NVDA"]["reason"]

    section = module.render_us_options_attention_section(packet)
    assert "N=10，仅参考" in section
    assert "LEAPS/远月 IV rank 来自逐合约链" in section


def test_live_long_dated_iv_snapshot_is_current_chain_not_history_rank() -> None:
    module = load_module()

    class FakeChain:
        def __init__(self, rows):
            self.rows = rows
            self.empty = not rows

        def iterrows(self):
            return iter(enumerate(self.rows))

    snapshot = module.select_live_long_dated_iv_snapshot(
        "MSFT",
        {
            "2027-07-16": (
                FakeChain([
                    {"strike": 490, "impliedVolatility": 0.31, "volume": 5, "openInterest": 10},
                    {"strike": 500, "impliedVolatility": 0.25, "volume": 7, "openInterest": 20},
                ]),
                FakeChain([
                    {"strike": 500, "impliedVolatility": 0.27, "volume": 11, "openInterest": 30},
                ]),
                501.0,
            ),
            "2027-02-19": (
                FakeChain([{"strike": 500, "impliedVolatility": 0.20}]),
                FakeChain([{"strike": 500, "impliedVolatility": 0.22}]),
                501.0,
            ),
        },
        date(2026, 6, 29),
    )

    assert snapshot is not None
    assert snapshot["symbol"] == "MSFT"
    assert snapshot["tenor"] == "LEAPS"
    assert snapshot["rank_pct"] is None
    assert snapshot["rank_n"] == 1
    assert snapshot["coverage_status"] == "live_only"
    assert snapshot["source"] == "cboe_live"
    assert snapshot["atm_iv"] == 0.26


def test_long_dated_iv_history_uses_live_fallback_when_db_missing(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    calls: list[tuple[list[str], str | None]] = []

    def fake_live(symbols, as_of):
        calls.append((symbols, as_of))
        return {"MSFT": {"symbol": "MSFT", "rank_pct": None, "rank_n": 1, "coverage_status": "live_only"}}

    monkeypatch.setattr(module, "fetch_live_long_dated_iv_snapshots", fake_live)

    out = module.fetch_long_dated_iv_history(
        ["MSFT", "AAPL"],
        "2026-06-29",
        db_path=tmp_path / "missing.duckdb",
        live_fallback_symbols=["MSFT", "AVGO"],
    )

    assert out["MSFT"]["coverage_status"] == "live_only"
    assert calls == [(["MSFT"], "2026-06-29")]


def test_live_long_iv_fallback_symbols_lists_tech_growth_universe(monkeypatch) -> None:
    module = load_module()
    monkeypatch.setenv("QUANT_LIVE_LEAPS_IV_FALLBACK", "1")
    monkeypatch.delenv("QUANT_LIVE_LEAPS_IV_SYMBOLS", raising=False)
    monkeypatch.setenv("QUANT_LIVE_LEAPS_IV_MAX_SYMBOLS", "40")

    symbols = module.live_long_iv_fallback_symbols({})
    universe = module.tech_growth_option_universe({"MSFT", "GOOGL", "AVGO"})
    groups = {row["group"]: row for row in universe}

    assert {"MSFT", "GOOGL", "AVGO", "NVDA", "SMH", "QQQ", "VGT", "CRWV", "NBIS", "VRT"} <= set(symbols)
    assert "Hyperscaler/平台" in groups
    assert "AI 半导体/设备" in groups
    assert groups["Hyperscaler/平台"]["covered_symbols"] == ["MSFT", "GOOGL"]
    assert groups["AI 半导体/设备"]["covered_symbols"] == ["AVGO"]


def test_us_options_attention_section_includes_tech_live_pool_and_spy_quadrant(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    us.payload["options_verdicts"] = {
        "SPY": {
            "effective_date": "2026-06-29",
            "iv_hv": 1.0,
            "pc_ratio_z": 1.2,
            "skew_z": 0.1,
        }
    }
    us.payload["long_dated_iv_history"] = {
        "MSFT": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.24,
            "avg_dte": 380,
            "rank_pct": None,
            "rank_n": 1,
            "coverage_status": "live_only",
            "source": "cboe_live",
        },
        "GOOGL": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.27,
            "avg_dte": 370,
            "rank_pct": None,
            "rank_n": 1,
            "coverage_status": "live_only",
            "source": "cboe_live",
        },
        "AVGO": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.33,
            "avg_dte": 390,
            "rank_pct": None,
            "rank_n": 1,
            "coverage_status": "live_only",
            "source": "cboe_live",
        },
        "NVDA": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.31,
            "avg_dte": 365,
            "rank_pct": 18,
            "rank_n": 40,
            "coverage_status": "ok",
        },
    }
    packet = module.build_packet("am", cn, us)
    packet["finance_search_prefetch"] = {
        "market_rows": [
            {"symbol": "SPY", "label": "SPY", "date": "2026-06-29", "change_pct": 0.62},
        ],
    }

    section = module.render_us_options_attention_section(packet)

    assert "科技成长 LEAPS 覆盖池" in section
    assert "MSFT" in section
    assert "GOOGL" in section
    assert "AVGO" in section
    assert "NVDA" in section
    assert "科技成长 LEAPS 观察池" in section
    assert "本地历史不足，不能判定低位/高位" in section
    assert "SPY 象限：SPY(2026-06-29) +0.62%" in section
    assert "上涨但 put/call 偏高" in section


def test_spy_quadrant_uses_index_option_sentiment_when_payload_lacks_spy(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    us.payload["options_verdicts"] = {}
    us.payload["long_dated_iv_history"] = {
        "NVDA": {
            "tenor": "LEAPS",
            "effective_date": "2026-06-29",
            "atm_iv": 0.31,
            "avg_dte": 365,
            "rank_pct": 18,
            "rank_n": 40,
            "coverage_status": "ok",
        },
    }

    monkeypatch.setattr(
        module,
        "fetch_option_sentiment_context",
        lambda symbols, as_of: {
            "SPY": {
                "effective_date": "2026-06-29",
                "verdict": "指数期权情绪背景",
                "pc_ratio_z": -1.1,
                "skew_z": 0.2,
            }
        },
    )

    packet = module.build_packet("am", cn, us)
    packet["finance_search_prefetch"] = {
        "market_rows": [
            {"symbol": "SPY", "label": "SPY", "date": "2026-06-29", "change_pct": 0.72},
        ],
    }

    section = module.render_us_options_attention_section(packet)

    assert "SPY 象限：SPY(2026-06-29) +0.72%" in section
    assert "上涨且 put/call 偏低" in section


def test_cboe_quote_day_prefers_last_trade_time() -> None:
    module = load_module()

    assert module.cboe_quote_day(
        {"data": {"last_trade_time": "2026-06-30T15:59:59"}, "timestamp": "2026-07-01 00:39:28"},
        date(2026, 6, 29),
    ) == date(2026, 6, 30)


def test_am_uses_previous_cn_context_when_target_day_payload_is_missing(tmp_path: Path) -> None:
    module = load_module()
    artifact(module, "cn", "2026-06-26", tmp_path)

    cn, note = module.load_cn_context_artifact(tmp_path, "am", "2026-06-29")

    assert cn.report_date == "2026-06-26"
    assert note is not None
    assert "2026-06-29" in note
    assert "2026-06-26" in note


def test_am_saturday_uses_friday_cn_context(tmp_path: Path) -> None:
    module = load_module()
    artifact(module, "cn", "2026-06-26", tmp_path)

    cn, note = module.load_cn_context_artifact(tmp_path, "am", "2026-06-27")

    assert cn.report_date == "2026-06-26"
    assert note is not None


def test_us_context_falls_back_to_latest_frozen_artifact(tmp_path: Path) -> None:
    module = load_module()
    artifact(module, "us", "2026-06-26", tmp_path)

    us, note = module.load_us_context_artifact(tmp_path, "2026-06-29")

    assert us.report_date == "2026-06-26"
    assert note is not None
    assert "2026-06-29" in note
    assert "2026-06-26" in note


def test_pm_report_does_not_claim_cn_guides_us(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    report = module.deterministic_report(module.build_packet("pm", cn, us))

    assert "A股盘后 → 美股盘前" not in report
    assert "A股盘后反馈 + 美股盘前" in report
    assert "因果方向固定为: US -> CN" in report
    assert "不得反向升降美股仓位" in report


def test_validator_rejects_cn_to_us_framing() -> None:
    module = load_module()

    failures = module.validate_shadow_report("# 跨市场晚报\n\nCN -> US\nA股\n美股\n", "pm")

    assert any("CN -> US" in item for item in failures)


def test_validator_rejects_delivery_failure_language() -> None:
    module = load_module()

    failures = module.validate_shadow_report("# 跨市场晚报\n\nA股\n美股\nvalidator | 未通过\n", "pm")

    assert any("validator | 未通过" in item for item in failures)


def test_public_delivery_rejects_tool_log_language() -> None:
    module = load_module()

    failures = module.validate_shadow_report("# 跨市场晚报\n\nA股\n美股\nMCP snapshot\n", "pm", public_delivery=True)

    assert any("MCP" in item for item in failures)


def test_public_delivery_cron_marker_does_not_reject_micron() -> None:
    module = load_module()

    base = (
        "# 跨市场晚报\n\n"
        "A股 美股 期权 Gamma 黄金 WTI原油 标普期货(2026-06-30) "
        "日经225(2026-06-30) KOSPI(2026-06-30) 德国DAX(2026-06-30) 688233.SH "
    )
    micron_failures = module.validate_shadow_report(base + "Micron 带动芯片链。", "pm", public_delivery=True)
    cron_failures = module.validate_shadow_report(base + "cron 状态不应出现在正文。", "pm", public_delivery=True)

    assert not any("cron" in item for item in micron_failures)
    assert any("cron" in item for item in cron_failures)


def test_public_delivery_rejects_diff_artifacts() -> None:
    module = load_module()

    failures = module.validate_shadow_report("# 跨市场晚报\n\nA股\n美股\n+## diff block\n", "pm", public_delivery=True)

    assert any("diff artifact" in item for item in failures)


def test_public_delivery_rejects_prompt_leakage_and_missing_data_list() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        "# 跨市场早报\n\n美股影响A股。\n\n以下是我的思维过程: 数据缺口/待补证据。",
        "am",
        public_delivery=True,
    )

    assert any("思维过程" in item for item in failures)
    assert any("数据缺口" in item for item in failures)


def test_public_delivery_rejects_internal_research_jargon() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股。AI Infra universe production执行层 source evidence 原文验证状态。"
            "BFS universe member; rank by price, flow, news, options and risk before any R."
        ),
        "am",
        public_delivery=True,
    )

    assert any("AI Infra" in item for item in failures)
    assert any("production" in item for item in failures)
    assert any("source evidence" in item for item in failures)
    assert any("原文验证" in item for item in failures)
    assert any("BFS universe" in item for item in failures)
    assert any("rank by price" in item for item in failures)


def test_public_delivery_rejects_reviewer_note_markers() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场晚报\n\n"
            "美股期货里标普期货(2026-06-29)给出下一轮风险线。黄金、WTI原油作为风险温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、DAX(2026-06-29)展示非美大盘。"
            "688233.SH神工股份覆盖A股半导体候选管线。"
            "\n\n主要改动：删除了事实清单外的新闻。"
        ),
        "pm",
        public_delivery=True,
    )

    assert any("主要改动" in item for item in failures)
    assert any("事实清单" in item for item in failures)


def test_public_delivery_requires_global_context_markers() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        "# 跨市场早报\n\n美股影响A股。黄金和WTI原油存在波动。科创板688样本在观察。",
        "am",
        public_delivery=True,
    )

    assert any("US equity futures" in item for item in failures)
    assert any("non-US country/region indices" in item for item in failures)


def test_public_delivery_accepts_required_global_context_markers() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场晚报\n\n"
            "美股期货里标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "期权Gamma显示指数仓位仍需收紧止损。"
            "科创50(2026-06-29)和688233.SH神工股份覆盖A股半导体候选管线。A股和美股合并复盘。"
        ),
        "pm",
        public_delivery=True,
    )

    assert failures == []


def test_public_delivery_rejects_missing_us_options_context() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场晚报\n\n"
            "美股期货里标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "科创50(2026-06-29)和688233.SH神工股份覆盖A股半导体候选管线。"
        ),
        "pm",
        public_delivery=True,
    )

    assert any("options/Gamma" in item for item in failures)


def test_public_delivery_rejects_missing_us_action_tickers(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股。标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "期权Gamma没有新增异常信号，但仍约束美股股票仓位。"
            "科创50(2026-06-29)和688233.SH神工股份覆盖A股半导体候选管线。"
        ),
        "am",
        public_delivery=True,
        packet=packet,
    )

    assert any("missing public US action ticker" in item for item in failures)


def test_public_delivery_rejects_missing_us_options_watch_tickers(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    us.payload["options_verdicts"] = {
        "DLR": {
            "effective_date": "2026-06-29",
            "verdict": "put 偏空 | 信仰久期中",
            "iv_rank_pct": 16,
            "iv_hv": 0.8355,
            "pc_ratio_z": 1.2,
            "skew_z": 3.4,
        }
    }
    packet = module.build_packet("am", cn, us)

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "NVDA 仍在美股执行清单里。"
            "美股影响A股。标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "期权Gamma没有新增异常信号，但仍约束美股股票仓位。"
            "科创50(2026-06-29)和688233.SH神工股份覆盖A股半导体候选管线。"
        ),
        "am",
        public_delivery=True,
        packet=packet,
    )

    assert any("missing public US options watch ticker" in item for item in failures)


def test_sec_13f_section_renders_recent_holding_changes() -> None:
    module = load_module()
    packet = {
        "sec_13f_recent": {
            "lookback_hours": 12,
            "recent_file_count": 1,
            "filings": [
                {
                    "manager": "TEST MANAGER",
                    "filing_date": "2026-06-30",
                    "report_date": "2026-03-31",
                    "holding_count": 3,
                    "new_positions_top5": [{"issuer": "NEW AI CO", "cusip": "999999999", "value_usd": 200_000_000}],
                    "increases_top5": [{"issuer": "APPLE INC", "cusip": "037833100", "value_delta_usd": 75_000_000}],
                    "decreases_top5": [{"issuer": "OLD CO", "cusip": "000000001", "value_delta_usd": -50_000_000}],
                }
            ],
        }
    }

    section = module.render_sec_13f_section(packet)

    assert "SEC 13F 机构持仓快照" in section
    assert "TEST MANAGER" in section
    assert "NEW AI CO" in section
    assert "APPLE INC" in section
    assert "OLD CO" in section
    assert "季度滞后" in section


def test_public_delivery_rejects_standalone_star_candidate_table() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场晚报\n\n"
            "美股期货里标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "A股管线里688233.SH神工股份仍是观察候选。"
            "\n\n| 科创板候选 | 状态 |\n|---|---|\n| 688233.SH | 观察 |"
        ),
        "pm",
        public_delivery=True,
    )

    assert any("standalone table" in item for item in failures)


def test_public_delivery_rejects_india_index_and_undated_index_lines() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股。标普期货和纳指期货给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经、KOSPI、恒生、DAX和印度Sensex展示非美大盘方向。"
            "688233.SH神工股份覆盖A股半导体。"
        ),
        "am",
        public_delivery=True,
    )

    assert any("forbidden India index marker" in item for item in failures)
    assert any("missing returned date" in item for item in failures)


def test_public_delivery_allows_single_news_context_marker_without_market_quote() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股。标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "新闻背景里提到纳指期货曾受利率预期影响。"
            "期权Gamma没有新增异常信号，但仍约束美股股票仓位。"
            "科创50(2026-06-29)和688233.SH神工股份覆盖A股半导体候选管线。"
        ),
        "am",
        public_delivery=True,
    )

    assert failures == []


def test_public_delivery_rejects_star_as_thermometer_without_concrete_ticker() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股。标普期货(2026-06-29)和纳指期货(2026-06-29)给出下一轮风险线。"
            "黄金、WTI原油同时作为避险和能源温度。"
            "日经225(2026-06-29)、KOSPI(2026-06-29)、恒生(2026-06-27)和DAX(2026-06-29)展示非美大盘方向。"
            "科创板只做温度计，不进入A股候选管线。"
        ),
        "am",
        public_delivery=True,
    )

    assert any("688xxx.SH" in item for item in failures)
    assert any("thermometer" in item for item in failures)


def test_public_delivery_rejects_split_single_market_reports() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        "# 跨市场晚报\n\n## 美股报告\n美股\n\n## A股报告\nA股",
        "pm",
        public_delivery=True,
    )

    assert any("美股报告" in item for item in failures)
    assert any("A股报告" in item for item in failures)


def test_clean_hermes_stdout_removes_session_id_and_code_fence() -> None:
    module = load_module()

    text = module.clean_hermes_stdout(
        "\nsession_id: 20260629_085722_8ca7b6\n```markdown\n# 跨市场早报\n\n美股影响A股。\n```\n"
    )

    assert text.startswith("# 跨市场早报")
    assert "session_id:" not in text
    assert "```" not in text


def test_normalize_public_report_trims_reviewer_preamble_and_translates_jargon() -> None:
    module = load_module()

    text = module.normalize_public_report_text(
        (
            "审稿意见: draft 需要修改。\n# 跨市场早报\n\n"
            "packet 里的 AI Infra universe 使用 beta hedge、source evidence、JSON 和 MCP。"
        ),
        "am",
    )

    assert text.startswith("# 跨市场早报")
    assert "审稿" not in text
    assert "draft" not in text
    assert "AI Infra" not in text
    assert "beta hedge" not in text
    assert "source evidence" not in text
    assert "packet" not in text
    assert "JSON" not in text
    assert "MCP" not in text


def test_normalize_public_report_removes_reviewer_change_notes() -> None:
    module = load_module()

    text = module.normalize_public_report_text(
        (
            "# 跨市场晚报\n\n"
            "美股影响A股。\n\n"
            "主要改动：\n\n"
            "1. 删除了事实清单外的新闻。\n"
            "2. 修正了标题。\n\n"
            "## A股执行\n"
            "688233.SH神工股份进入候选管线。"
        ),
        "pm",
    )

    assert "主要改动" not in text
    assert "事实清单" not in text
    assert "修正了标题" not in text
    assert "## A股执行" in text
    assert "688233.SH" in text


def test_normalize_public_report_strips_diff_artifacts() -> None:
    module = load_module()

    text = module.normalize_public_report_text(
        (
            "+# 跨市场晚报\n"
            "+\n"
            "+## 宏观数据温度计\n"
            "+旧表格。\n"
            "+## 正文\n"
            "+美股影响A股，A股不反向约束美股。"
        ),
        "pm",
    )

    assert text.startswith("# 跨市场晚报")
    assert all(not line.startswith("+") for line in text.splitlines())
    assert "旧表格" in text


def test_normalize_public_report_removes_duplicate_top_level_title() -> None:
    module = load_module()

    text = module.normalize_public_report_text(
        (
            "# 跨市场早报：先看美股\n\n"
            "宏观温度。\n\n"
            "# 跨市场早报\n\n"
            "正文继续。"
        ),
        "am",
    )

    assert text.count("# 跨市场早报") == 1
    assert "正文继续" in text


def test_normalize_public_report_strips_reviewer_delete_diff_artifacts() -> None:
    module = load_module()

    text = module.normalize_public_report_text(
        (
            "# 跨市场早报\n\n"
            "美股影响A股，A股只接受美股风险约束。\n\n"
            "-| Ticker | 动作 |\n"
            "-|---|---|\n"
            "-| NVDA | 删除旧表 |\n"
            "-## 美股期权关注标的（OTM skew / LEAPS IV）\n"
            "-这里是被审稿删除的旧段。\n"
            "-- Reuters：旧新闻。\n"
            "新的正文继续保留。\n\n"
            "… omitted 164 diff line(s) across 1 additional file(s)/section(s)\n"
            "  ┊ review diff\n"
            "a//home/ubuntu/quant-stack/report.md → b//home/ubuntu/quant-stack/report.md\n"
            "@@ -82,7 +82,7 @@\n"
            " 1. 旧上下文\n"
            "-4. 被删除的宏观条件。\n"
            "4. 新宏观条件。\n\n"
            "## 附表：其他跨市场数据\n"
            "| 类别 | 指标 |\n"
            "|---|---|\n"
            "| 美股期货 | 标普期货 |\n"
        ),
        "am",
    )

    assert text.startswith("# 跨市场早报")
    assert text.count("# 跨市场早报") == 1
    assert not any(line.startswith("-|") for line in text.splitlines())
    assert not any(line.startswith("-##") for line in text.splitlines())
    assert "被审稿删除的旧段" not in text
    assert "-- Reuters" not in text
    assert "review diff" not in text
    assert "@@" not in text
    assert "omitted 164 diff" not in text
    assert "新的正文继续保留" in text
    assert "## 附表：其他跨市场数据" in text


def test_public_delivery_rejects_reviewer_diff_artifacts() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股，标普期货(2026-06-29)和纳指期货(2026-06-29)修复。"
            "黄金(2026-06-29)、WTI原油(2026-06-29)、日经225(2026-06-29)、"
            "KOSPI(2026-06-29)、恒生(2026-06-29)、DAX(2026-06-29)。"
            "期权Gamma约束美股仓位。688233.SH进入A股候选管线。\n"
            "-## 可核验宏观与产业 headlines\n"
        ),
        "am",
        public_delivery=True,
    )

    assert any("diff artifact" in item for item in failures)


def test_public_delivery_rejects_duplicate_top_level_title() -> None:
    module = load_module()

    failures = module.validate_shadow_report(
        (
            "# 跨市场早报\n\n"
            "美股影响A股，标普期货(2026-06-29)和纳指期货(2026-06-29)修复。"
            "黄金(2026-06-29)、WTI原油(2026-06-29)、日经225(2026-06-29)、"
            "KOSPI(2026-06-29)、恒生(2026-06-29)、DAX(2026-06-29)。"
            "期权Gamma约束美股仓位。688233.SH进入A股候选管线。\n\n"
            "# 跨市场早报\n\n"
            "重复标题。"
        ),
        "am",
        public_delivery=True,
    )

    assert any("duplicate top-level title" in item for item in failures)


def test_normalize_public_report_repairs_plain_title() -> None:
    module = load_module()

    text = module.normalize_public_report_text("跨市场早报｜期货反弹但A股不追高\n\n美股影响A股。", "am")

    assert text.startswith("# 跨市场早报：期货反弹但A股不追高")


def test_compact_market_snapshot_rows_labels_futures_and_country_indices() -> None:
    module = load_module()

    assert "^BSESN" not in module.GLOBAL_MARKET_SNAPSHOT_SYMBOLS
    assert "印度Sensex" not in module.GLOBAL_MARKET_LABELS.values()

    rows = module.compact_market_snapshot_rows(
        {
            "used_symbols": ["ES=F", "NQ=F", "^N225", "^GDAXI", "GC=F", "CL=F"],
            "symbols": {
                "ES=F": {"ok": True, "date": "2026-06-29", "close": 7000, "change_pct": 0.5, "source": "yfinance"},
                "NQ=F": {"ok": True, "date": "2026-06-29", "close": 25000, "change_pct": 0.8, "source": "yfinance"},
                "^N225": {"ok": True, "date": "2026-06-29", "close": 41000, "change_pct": -0.2, "source": "yfinance"},
                "^GDAXI": {"ok": True, "date": "2026-06-29", "close": 18000, "change_pct": 0.1, "source": "yfinance"},
                "GC=F": {"ok": True, "date": "2026-06-29", "close": 3300, "change_pct": 0.3, "source": "yfinance"},
                "CL=F": {"ok": True, "date": "2026-06-29", "close": 70, "change_pct": 1.0, "source": "yfinance"},
            },
        }
    )

    labels = {row["label"] for row in rows}
    assert {"标普期货", "纳指期货", "日本日经225", "德国DAX", "黄金期货", "WTI原油期货"} <= labels
    assert all("2026-06-29" in row["display"] for row in rows)


def test_market_snapshot_section_filters_india_and_keeps_dates(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-26", tmp_path)
    packet = module.build_packet("am", cn, us)
    packet["finance_search_prefetch"] = {
        "market_rows": [
            {"symbol": "^GSPC", "label": "标普500", "date": "2026-06-26", "close": 6088.9, "change_pct": -0.05},
            {"symbol": "000001.SS", "label": "上证指数", "date": "2026-06-29", "close": 3300, "change_pct": 0.3},
            {"symbol": "^KS11", "label": "韩国KOSPI", "date": "2026-06-29", "close": 3000, "change_pct": -1.56},
            {"symbol": "^N225", "label": "日本日经225", "date": "2026-06-29", "close": 41000, "change_pct": -1.04},
            {"symbol": "ES=F", "label": "标普期货", "date": "2026-06-29", "close": 7000, "change_pct": 0.5},
            {"symbol": "^GDAXI", "label": "德国DAX", "date": "2026-06-26", "close": 18000, "change_pct": -1.2},
            {"symbol": "^BSESN", "label": "印度Sensex", "date": "2026-06-26", "close": 80000, "change_pct": 1.0},
        ]
    }

    section = module.render_market_snapshot_section(packet)
    tail = module.render_market_tail_section(packet)

    assert "## 宏观数据温度计" in section
    assert "标普500 | 2026-06-26" in section
    assert "上证指数 | 2026-06-29" in section
    assert "韩国KOSPI | 2026-06-29" in section
    assert "日本日经225 | 2026-06-29" in section
    assert "标普期货" not in section
    assert "德国DAX" not in section
    assert "标普期货 | 2026-06-29" in tail
    assert "德国DAX | 2026-06-26" in tail
    assert "印度" not in section
    assert "Sensex" not in section
    assert "印度" not in tail
    assert "Sensex" not in tail


def test_market_snapshot_dates_are_annotated_and_inserted_for_public_report(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-26", tmp_path)
    us.payload["options_verdicts"] = {
        "DLR": {
            "effective_date": "2026-06-26",
            "verdict": "put 偏空 | 信仰久期中",
            "iv_ann": 0.2464,
            "iv_rank_pct": 16,
            "iv_hv": 0.8355,
            "pc_ratio_z": 1.1,
            "skew_z": 3.9,
        }
    }
    packet = module.build_packet("am", cn, us)
    packet["finance_search_prefetch"] = {
        "generated_at": "2026-06-29T12:00:00Z",
        "lookback_hours": 12,
        "market_rows": [
            {"symbol": "^GSPC", "label": "标普500", "date": "2026-06-26", "close": 6088.9, "change_pct": -0.05},
            {"symbol": "^VIX", "label": "VIX波动率", "date": "2026-06-26", "close": 18.41, "change_pct": -2.54},
            {"symbol": "ES=F", "label": "标普期货", "date": "2026-06-29", "close": 6120, "change_pct": 0.4},
            {"symbol": "NQ=F", "label": "纳指期货", "date": "2026-06-29", "close": 22400, "change_pct": 0.35},
            {"symbol": "^GDAXI", "label": "德国DAX", "date": "2026-06-26", "close": 18000, "change_pct": -1.29},
            {"symbol": "^N225", "label": "日本日经225", "date": "2026-06-29", "close": 41000, "change_pct": -1.04},
            {"symbol": "^KS11", "label": "韩国KOSPI", "date": "2026-06-29", "close": 3000, "change_pct": -1.56},
            {"symbol": "^HSI", "label": "香港恒生", "date": "2026-06-26", "close": 24000, "change_pct": -1.76},
            {"symbol": "000001.SS", "label": "上证指数", "date": "2026-06-26", "close": 3300, "change_pct": -2.26},
            {"symbol": "000688.SS", "label": "科创50", "date": "2026-06-29", "close": 950, "change_pct": 0.2},
            {"symbol": "GC=F", "label": "黄金期货", "date": "2026-06-29", "close": 4074, "change_pct": -0.12},
            {"symbol": "CL=F", "label": "WTI原油期货", "date": "2026-06-29", "close": 70.12, "change_pct": 1.29},
        ],
        "news_items": [
            {"title": "Fed officials keep rate-cut timing in focus", "source": "Reuters", "published_at": "2026-06-29T08:30:00Z"},
            {"title": "AI chip supply chain leads Asia trading", "source": "NewsNow", "published_at": "2026-06-28T20:00:00Z"},
        ],
    }
    packet["cn"]["pipeline_candidates"] = [
        {
            "symbol": "688233.SH",
            "name": "神工股份",
            "pipeline_stage": "active_watch",
            "rank": 20,
            "rank_score": 70.18,
            "reason": "AI Infra BFS universe member; rank by price, flow, news, options and risk before any R.; 等待A股本域价格和量能确认",
        }
    ]
    report = (
        "# 跨市场早报：测试\n\n"
        "美股影响A股，标普500 -0.05%，VIX收低，标普期货和纳指期货修复。"
        "德国DAX、日本日经225、KOSPI、恒生同步给出压力。"
        "期权Gamma要求美股股票仓位收紧止损。"
        "A股看上证指数和科创50，688233.SH神工股份进入候选管线。黄金和WTI原油作为风险温度。"
        "\n\n## 全球市场温度：模型草稿\n\n"
        "| 资产/指数 | 返回日期 | 读数 |\n"
        "|---|---:|---:|\n"
        "| VIX波动率 | 2026-06-26 | 18.41 |\n\n"
        "| 科创板候选 | 状态 |\n"
        "|---|---|\n"
        "| 688233.SH | 观察 |\n\n"
        "科创板只做温度计，不进入A股候选管线。"
    )

    report = module.annotate_market_snapshot_dates(report, packet)
    report = module.ensure_market_snapshot_section(report, packet)
    report = module.ensure_us_action_section(report, packet)
    report = module.ensure_us_options_attention_section(report, packet)
    report = module.ensure_cn_pipeline_section(report, packet)
    report = module.ensure_cn_pipeline_language(report, packet)
    report = module.ensure_execution_diary_sections(report, packet)
    report = module.normalize_public_report_text(report, "am")
    report = module.strip_diff_artifact_markers(report)
    report = module.strip_duplicate_report_titles(report, "am")
    failures = module.validate_shadow_report(report, "am", public_delivery=True, packet=packet)

    assert "标普500(2026-06-26)" in report
    assert "VIX(2026-06-26)收低" in report
    assert "VIX(2026-06-26)波动率" not in report
    assert "DAX(2026-06-26)" in report
    assert report.index("## 宏观数据温度计") < report.index("美股影响A股")
    assert "## 宏观数据温度计" in report
    assert report.count("## 宏观数据温度计") == 1
    assert "## 宏观事件与产业新闻" in report
    assert "美联储利率路径仍是全球风险资产的核心变量" in report
    assert "AI 和半导体链条仍是跨市场风险偏好的主线" not in report
    assert "Fed officials keep rate-cut timing in focus" not in report
    assert "## 美股执行标的" in report
    assert "NVDA" in report
    assert "## 美股期权关注标的" in report
    assert "DLR" in report
    assert report.index("## 美股执行标的") < report.index("## 美股期权关注标的")
    assert "## A股执行与候选管线" in report
    assert "## A股科创板候选管线" not in report
    assert "688233.SH" in report
    assert "## 跨市场主线" in report
    assert "## 传导到A股" in report
    assert "## 今天的执行剧本" in report
    assert "## 失效条件和下一步检查" in report
    assert "AI Infra" not in report
    assert "BFS universe" not in report
    assert "rank by price" not in report
    assert "等待A股本域价格和量能确认" in report
    assert "## 附表：其他跨市场数据" in report
    assert "资产/指数" not in report
    assert "| 科创板候选 |" not in report
    assert "科创板只做温度计" not in report
    assert report.index("## 宏观数据温度计") < report.index("## 宏观事件与产业新闻")
    assert report.index("## 宏观事件与产业新闻") < report.index("## 美股执行标的")
    assert report.rfind("## 附表：其他跨市场数据") > report.index("688233.SH")
    assert failures == []


def test_macro_headlines_keep_only_recent_timestamped_news() -> None:
    module = load_module()
    section = module.render_macro_headline_section(
        {
            "finance_search_prefetch": {
                "generated_at": "2026-06-30T12:00:00Z",
                "lookback_hours": 12,
                "news_items": [
                    {
                        "title": "Fed officials keep rate-cut timing in focus",
                        "source": "Reuters",
                        "published_at": "2026-06-30T08:00:00Z",
                    },
                    {
                        "title": "AI chip supply chain leads Asia trading",
                        "source": "NewsNow",
                        "published_at": "2026-06-29T23:30:00Z",
                    },
                    {
                        "title": "Gold and silver fall as Fed risk rises",
                        "source": "Wire",
                        "published_at": "2026-06-30",
                    },
                ],
            }
        }
    )

    assert "美联储利率路径仍是全球风险资产的核心变量" in section
    assert "AI 和半导体链条仍是跨市场风险偏好的主线" not in section
    assert "美联储利率重定价压过避险需求" not in section


def test_execution_diary_sections_restore_compact_reviewer_output(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    cn.payload["cn_opportunity_ranker"] = {
        "all_rows": [
            {
                "symbol": "688233.SH",
                "name": "神工股份",
                "production_tier": "active_watch",
                "rank": 20,
                "rank_score": 70.18,
                "observation_entry_zone": "194.33-198.22",
                "handling_line": "回落后重新站回入场区",
                "reason": "AI Infra BFS universe member; 等待A股本域价格和量能确认",
            }
        ]
    }
    us.payload["options_verdicts"] = {
        "NVDA": {"effective_date": "2026-06-29", "iv_rank_pct": 18, "iv_hv": 0.8, "skew_z": 1.4}
    }
    packet = module.build_packet("pm", cn, us)
    packet["finance_search_prefetch"] = {
        "market_rows": [
            {"symbol": "^GSPC", "label": "标普500", "date": "2026-06-29", "change_pct": 0.42},
            {"symbol": "000001.SS", "label": "上证指数", "date": "2026-06-29", "change_pct": -0.21},
            {"symbol": "^KS11", "label": "韩国KOSPI", "date": "2026-06-29", "change_pct": 1.22},
            {"symbol": "^N225", "label": "日本日经225", "date": "2026-06-29", "change_pct": 0.88},
            {"symbol": "ES=F", "label": "标普期货", "date": "2026-06-29", "change_pct": 0.18},
        ]
    }
    report = (
        "# 跨市场晚报\n\n"
        "## 宏观数据温度计\n"
        "已有温度计。\n\n"
        "## 美股执行标的\n"
        "NVDA 在执行池。\n\n"
        "## A股执行与候选管线\n"
        "688233.SH神工股份在观察。\n\n"
        "## 附表：其他跨市场数据\n"
        "尾表。"
    )

    enriched = module.ensure_execution_diary_sections(report, packet)

    assert "## 跨市场主线" in enriched
    assert "## 传导到A股" in enriched
    assert "## 今天的执行剧本" in enriched
    assert "## 失效条件和下一步检查" in enriched
    assert "NVDA" in enriched
    assert "688233.SH神工股份" in enriched
    assert "标普500(2026-06-29)" in enriched
    assert enriched.index("## 失效条件和下一步检查") < enriched.index("## 附表：其他跨市场数据")


def test_execution_diary_sections_respect_natural_reviewer_headings(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)
    report = (
        "# 跨市场早报\n\n"
        "## 今天的交易逻辑：反弹是门，纪律是锁\n"
        "已有主线。\n\n"
        "## A股盘前：守住入场区\n"
        "已有执行。\n\n"
        "## 失效条件\n"
        "- 已有失效条件。\n\n"
        "## 附表：其他跨市场数据\n"
        "尾表。"
    )

    enriched = module.ensure_execution_diary_sections(report, packet)

    assert enriched.count("## 跨市场主线") == 0
    assert enriched.count("## 传导到A股") == 0
    assert enriched.count("## 今天的执行剧本") == 0
    assert enriched.count("## 失效条件和下一步检查") == 0
    assert "已有主线" in enriched


def test_public_delivery_rejects_thin_report_when_packet_is_available(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)
    report = (
        "# 跨市场早报\n\n"
        "## 宏观数据温度计\n"
        "标普期货(2026-06-29)和纳指期货(2026-06-29)给出风险线。\n\n"
        "## 宏观事件与产业新闻\n"
        "AI 和半导体链条仍是主线。\n\n"
        "## 美股执行标的\n"
        "NVDA 期权 Gamma 约束仓位。\n\n"
        "## 美股期权关注标的（OTM skew / LEAPS IV）\n"
        "期权 Gamma 只影响股票节奏。\n\n"
        "## A股执行与候选管线\n"
        "688981.SH进入A股候选管线。\n\n"
        "## 附表：其他跨市场数据\n"
        "黄金(2026-06-29)、WTI原油(2026-06-29)、日经225(2026-06-29)、"
        "KOSPI(2026-06-29)、恒生(2026-06-29)、德国DAX(2026-06-29)。"
    )

    failures = module.validate_shadow_report(report, "am", public_delivery=True, packet=packet)

    assert any("public report too thin" in item for item in failures)
    assert any("missing public narrative section" in item for item in failures)


def test_managed_market_sections_strip_common_agent_heading_variants(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-26", tmp_path)
    packet = module.build_packet("pm", cn, us)
    packet["finance_search_prefetch"] = {
        "generated_at": "2026-06-29T12:00:00Z",
        "lookback_hours": 12,
        "market_rows": [
            {"symbol": "^GSPC", "label": "标普500", "date": "2026-06-26", "close": 6088.9, "change_pct": -0.05},
            {"symbol": "^N225", "label": "日本日经225", "date": "2026-06-29", "close": 41000, "change_pct": -1.04},
            {"symbol": "^KS11", "label": "韩国KOSPI", "date": "2026-06-29", "close": 3000, "change_pct": -1.56},
            {"symbol": "000001.SS", "label": "上证指数", "date": "2026-06-29", "close": 3300, "change_pct": 0.2},
            {"symbol": "ES=F", "label": "标普期货", "date": "2026-06-29", "close": 7000, "change_pct": 0.5},
        ],
        "news_items": [{"title": "Fed rate risk remains in focus", "source": "Reuters", "published_at": "2026-06-29T09:00:00Z"}],
    }
    report = (
        "# 跨市场晚报\n\n"
        "美股影响A股。\n\n"
        "## 顶部宏观数据温度计\n"
        "旧顶部表。\n\n"
        "## 宏观温度计\n"
        "旧宏观温度计。\n\n"
        "## 宏观与产业 headlines\n"
        "- 幻觉新闻。\n\n"
        "## 可核验宏观与产业 headlines\n"
        "- 旧英文新闻。\n\n"
        "## 科创板不是温度计，是下一轮候选管线\n"
        "旧科创段。\n\n"
        "## 附表：外围资产与风险参考\n"
        "旧外围表。\n\n"
        "## 市场主线\n"
        "正文保留。\n\n"
        "## 附表：全球风险与跨资产读数\n"
        "旧尾表。"
    )

    report = module.ensure_market_snapshot_section(report, packet)
    report = module.ensure_cn_pipeline_language(report, packet)

    assert "旧顶部表" not in report
    assert "旧宏观温度计" not in report
    assert "幻觉新闻" not in report
    assert "旧英文新闻" not in report
    assert "可核验宏观与产业" not in report
    assert "旧科创段" not in report
    assert "旧外围表" not in report
    assert "旧尾表" not in report
    assert "正文保留" in report
    assert "## 宏观数据温度计" in report
    assert "## 宏观事件与产业新闻" in report
    assert "## 附表：其他跨市场数据" in report


def test_agent_prompt_is_heuristic_not_fixed_template(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    system, user = module.build_agent_messages(module.build_packet("am", cn, us))

    assert "MCP/skill-like 工具面" in system
    assert "不是章节模板" in system
    assert "结构必须覆盖" not in system
    assert "coverage_checklist" in user
    assert "tool_manifest" in user


def test_hermes_prompt_retires_legacy_narrator_templates(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)

    prompt = module.build_hermes_prompt(module.build_packet("pm", cn, us))

    assert "Hermes lead editor agent" in prompt
    assert "finance-search MCP" in prompt
    assert "get_market_snapshot" in prompt
    assert "newsnow_radar" in prompt
    assert "search_news" in prompt
    assert "quant_stack_ranker" in prompt
    assert "美股期货" in prompt
    assert "顶部只放美股大盘、A股大盘、KOSPI、日经225" in prompt
    assert "报告尾部附表" in prompt
    assert "科创板/688xxx" in prompt
    assert "不要使用 quant-research-v1/prompts" in prompt
    assert "coverage_checklist 是验收清单,不是章节模板" in prompt
    assert "不得把 A股盘后反馈写成会指导美股盘前或美股策略" in prompt
    assert "CN -> US" not in prompt
    assert "自然省略" in prompt
    assert "数据缺口/待补证据" not in prompt
    assert "投递失败" not in prompt


def test_hermes_reviewer_prompt_requires_one_merged_public_report(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    prompt = module.build_hermes_review_prompt(packet, "# 跨市场早报\n\nMCP snapshot")

    assert "二审编辑" in prompt
    assert "输出一封合并日报" in prompt
    assert "不要出现 MCP" in prompt
    assert "prompt" in prompt
    assert "思维过程" in prompt
    assert "缺口清单" in prompt
    assert "只输出最终 markdown" in prompt


def test_call_hermes_agent_uses_hermes_skill(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    completed = mock.Mock(
        returncode=0,
        stdout="session_id: 20260629_085722_8ca7b6\n# 跨市场早报 — 2026-06-29\n\n美股影响A股。",
        stderr="",
    )
    with mock.patch.object(module.subprocess, "run", return_value=completed) as run:
        report = module.call_hermes_agent(
            packet,
            timeout=30,
            hermes_bin="/home/ubuntu/.local/bin/hermes",
            model="",
            provider="",
            max_turns=8,
        )

    cmd = run.call_args.args[0]
    assert cmd[0] == "/home/ubuntu/.local/bin/hermes"
    assert cmd[1:4] == ["chat", "-Q", "-q"]
    assert "--skills" in cmd
    assert "quant-stack-cross-market-daily" in cmd
    assert "--max-turns" in cmd
    assert "--source" in cmd
    assert "quant-stack-cron" in cmd
    assert report.startswith("# 跨市场早报")
    assert packet["_agent_backend"] == "hermes"


def test_call_hermes_reviewer_uses_review_source(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    completed = mock.Mock(
        returncode=0,
        stdout="session_id: 20260629_085722_8ca7b6\n# 跨市场早报 — 2026-06-29\n\n编辑后的一封合并日报。",
        stderr="",
    )
    with mock.patch.object(module.subprocess, "run", return_value=completed) as run:
        report = module.call_hermes_reviewer(
            packet,
            "# 跨市场早报\n\nMCP snapshot",
            timeout=30,
            hermes_bin="/home/ubuntu/.local/bin/hermes",
            model="deepseek-v4-pro",
            provider="deepseek",
            max_turns=6,
        )

    cmd = run.call_args.args[0]
    assert cmd[0] == "/home/ubuntu/.local/bin/hermes"
    assert "quant-stack-reviewer" in cmd
    assert "--model" in cmd
    assert "deepseek-v4-pro" in cmd
    assert "--provider" in cmd
    assert "deepseek" in cmd
    assert report.startswith("# 跨市场早报")
    assert packet["_reviewer_backend"] == "hermes"
    assert packet["_reviewer_provider"] == "deepseek"


def test_call_hermes_reviewer_falls_back_to_writer_model(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    failed = mock.Mock(returncode=1, stdout="", stderr="deepseek down")
    succeeded = mock.Mock(returncode=0, stdout="# 跨市场早报 — 2026-06-29\n\n默认 reviewer 接管。", stderr="")
    with mock.patch.object(module.subprocess, "run", side_effect=[failed, succeeded]) as run:
        report = module.call_hermes_reviewer_with_fallback(
            packet,
            "# 跨市场早报\n\nMCP snapshot",
            timeout=30,
            hermes_bin="/home/ubuntu/.local/bin/hermes",
            review_model="deepseek-v4-pro",
            review_provider="deepseek",
            fallback_model="",
            fallback_provider="",
            max_turns=6,
        )

    assert run.call_count == 2
    assert report.startswith("# 跨市场早报")
    assert "deepseek down" in packet["_reviewer_primary_error"]


def test_resend_delivery_falls_back_to_gmail(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    monkeypatch.setenv("CROSS_MARKET_DELIVERY_STATE_DIR", str(tmp_path / "delivery_state"))
    report = tmp_path / "cross_market.md"
    report.write_text("# 跨市场早报\n", encoding="utf-8")
    packet = {"slot": "am", "target_cn_date": "2026-06-29", "cn": {"report_date": "2026-06-29"}}

    gmail_mod = types.ModuleType("quant_bot.delivery.gmail")
    calls: list[tuple[str, dict]] = []

    def fail_resend(**kwargs):
        calls.append(("resend", kwargs))
        raise RuntimeError("resend down")

    def send_gmail(**kwargs):
        calls.append(("gmail", kwargs))
        return ["gmail-id"]

    gmail_mod.send_report_email_resend = fail_resend
    gmail_mod.send_report_email = send_gmail
    quant_bot = types.ModuleType("quant_bot")
    quant_bot.__path__ = []
    delivery = types.ModuleType("quant_bot.delivery")
    delivery.__path__ = []

    args = argparse.Namespace(
        send_email=True,
        delivery_dry_run=False,
        email_provider="resend",
        email_fallback_provider="gmail",
        delivery_mode="prod",
        test_recipient="",
    )
    with mock.patch.dict(
        sys.modules,
        {
            "quant_bot": quant_bot,
            "quant_bot.delivery": delivery,
            "quant_bot.delivery.gmail": gmail_mod,
        },
    ):
        ids = module.send_email_if_requested(report, packet, args)

    assert ids == ["gmail-id"]
    assert [name for name, _ in calls] == ["resend", "gmail"]
    assert calls[0][1]["to"] is None
    assert calls[0][1]["bcc"] is None
    assert calls[1][1]["credentials_path"].name == "credentials.json"
    assert calls[1][1]["token_path"].name == "token.json"
    records = list((tmp_path / "delivery_state").glob("*.json"))
    assert len(records) == 1
    assert module.json.loads(records[0].read_text(encoding="utf-8"))["status"] == "sent"


def test_delivery_ledger_skips_duplicate_send(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    monkeypatch.setenv("CROSS_MARKET_DELIVERY_STATE_DIR", str(tmp_path / "delivery_state"))
    report = tmp_path / "cross_market.md"
    report.write_text("# 跨市场早报\n", encoding="utf-8")
    packet = {"slot": "am", "target_cn_date": "2026-06-29", "cn": {"report_date": "2026-06-29"}}

    gmail_mod = types.ModuleType("quant_bot.delivery.gmail")
    calls: list[dict] = []

    def send_resend(**kwargs):
        calls.append(kwargs)
        return [f"resend-{len(calls)}"]

    gmail_mod.send_report_email_resend = send_resend
    gmail_mod.send_report_email = mock.Mock(return_value=["gmail-id"])
    quant_bot = types.ModuleType("quant_bot")
    quant_bot.__path__ = []
    delivery = types.ModuleType("quant_bot.delivery")
    delivery.__path__ = []
    args = argparse.Namespace(
        send_email=True,
        delivery_dry_run=False,
        email_provider="resend",
        email_fallback_provider="gmail",
        delivery_mode="test",
        test_recipient="first@example.com,second@example.com",
    )
    with mock.patch.dict(
        sys.modules,
        {
            "quant_bot": quant_bot,
            "quant_bot.delivery": delivery,
            "quant_bot.delivery.gmail": gmail_mod,
        },
    ):
        first = module.send_email_if_requested(report, packet, args)
        second = module.send_email_if_requested(report, packet, args)

    assert first == ["resend-1"]
    assert second == []
    assert len(calls) == 1


def test_openclaw_publish_invokes_helper(tmp_path: Path) -> None:
    module = load_module()
    report = tmp_path / "cross_market_am_shadow.md"
    report.write_text("# 跨市场早报 — 2026-06-30\n\n美股 A股", encoding="utf-8")
    (tmp_path / "cross_market_am_shadow_packet.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "cross_market_am_shadow.meta.json").write_text("{}\n", encoding="utf-8")
    packet = {"slot": "am", "target_cn_date": "2026-06-30", "cn": {"report_date": "2026-06-30"}}
    args = argparse.Namespace(
        publish_openclaw=True,
        delivery_dry_run=False,
        openclaw_mode="agent",
        openclaw_host="100.109.146.30",
        openclaw_user="ivena",
        openclaw_root="/home/ivena/.openclaw/quant-stack",
        openclaw_identity_file="/home/ubuntu/.ssh/id_ed25519_quant_pi",
        openclaw_agent="main",
        openclaw_agent_session_key="",
        openclaw_agent_timeout=180,
        openclaw_agent_deliver=True,
        openclaw_reply_channel="openclaw-weixin",
        openclaw_reply_account="912f45c70aa5-im-bot,86fb46c4a557-im-bot",
        openclaw_reply_to="o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat",
        openclaw_message_channel="",
        openclaw_message_account="",
        openclaw_message_target="",
        openclaw_allow_duplicate_event=False,
        openclaw_required=True,
    )

    completed = mock.Mock(returncode=0, stdout='{"ok": true}\n', stderr="")
    with mock.patch.object(module.subprocess, "run", return_value=completed) as run:
        module.publish_openclaw_if_requested(report, packet, args)

    cmd = run.call_args.args[0]
    assert str(module.ROOT / "scripts" / "publish_report_to_openclaw.py") in cmd
    assert cmd[cmd.index("--kind") + 1] == "cross_market_daily"
    assert cmd[cmd.index("--slot") + 1] == "am"
    assert cmd[cmd.index("--date") + 1] == "2026-06-30"
    assert cmd[cmd.index("--mode") + 1] == "agent"
    assert "--agent-deliver" in cmd
    assert cmd[cmd.index("--reply-channel") + 1] == "openclaw-weixin"
    assert cmd[cmd.index("--reply-account") + 1] == "912f45c70aa5-im-bot,86fb46c4a557-im-bot"
    assert (
        cmd[cmd.index("--reply-to") + 1]
        == "o9cq801qjkqxtXS-B8BAuJEzUM0A@im.wechat,o9cq80-w8F7HxwCfvSJdoF-vN2os@im.wechat"
    )
    assert cmd[cmd.index("--packet-path") + 1].endswith("cross_market_am_shadow_packet.json")
    assert cmd[cmd.index("--meta-path") + 1].endswith("cross_market_am_shadow.meta.json")


def test_openclaw_publish_skips_external_call_on_delivery_dry_run(tmp_path: Path) -> None:
    module = load_module()
    report = tmp_path / "cross_market_pm_shadow.md"
    report.write_text("# 跨市场晚报 — 2026-06-30\n", encoding="utf-8")
    args = argparse.Namespace(publish_openclaw=True, delivery_dry_run=True, openclaw_mode="agent")

    with mock.patch.object(module.subprocess, "run") as run:
        module.publish_openclaw_if_requested(report, {"slot": "pm", "cn": {"report_date": "2026-06-30"}}, args)

    run.assert_not_called()


def test_output_snapshot_restores_failed_validation_artifacts(tmp_path: Path) -> None:
    module = load_module()
    output_dir = tmp_path / "reports"
    output_dir.mkdir()
    paths = module.output_paths(output_dir, "am")
    paths["report"].write_text("# old report\n", encoding="utf-8")
    paths["meta"].write_text('{"old": true}\n', encoding="utf-8")

    snapshot = module.snapshot_existing_outputs(output_dir, "am")
    paths["report"].write_text("# failed report\n", encoding="utf-8")
    paths["meta"].unlink()
    paths["packet"].write_text('{"failed": true}\n', encoding="utf-8")

    module.restore_output_snapshot(snapshot)

    assert paths["report"].read_text(encoding="utf-8") == "# old report\n"
    assert paths["meta"].read_text(encoding="utf-8") == '{"old": true}\n'
    assert not paths["packet"].exists()


def test_write_outputs_allows_output_dir_outside_repo(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path / "artifacts")
    us = artifact(module, "us", "2026-06-26", tmp_path / "artifacts")
    packet = module.build_packet("am", cn, us)

    path = module.write_outputs(tmp_path / "outside", packet, "# 跨市场早报\n\n美股 A股", agent_backend="test")

    assert path.exists()
    trajectory = (tmp_path / "outside" / "cross_market_am_shadow_trajectory.jsonl").read_text(encoding="utf-8")
    assert str(path) in trajectory


def test_fallback_report_uses_legacy_backend_only_after_primary_failure(tmp_path: Path) -> None:
    module = load_module()
    cn = artifact(module, "cn", "2026-06-29", tmp_path)
    us = artifact(module, "us", "2026-06-29", tmp_path)
    packet = module.build_packet("am", cn, us)

    with mock.patch.object(module, "call_agent", return_value="# 跨市场早报 — 2026-06-29\n\n美股 A股"):
        report, backend = module.fallback_report(packet, "auto", 30, RuntimeError("hermes down"))

    assert report.startswith("# 跨市场早报")
    assert backend.startswith("fallback:")
    assert "hermes down" in packet["_agent_primary_error"]
