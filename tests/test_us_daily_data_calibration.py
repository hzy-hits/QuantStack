from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
US_SRC = ROOT / "quant-research-v1" / "src"
if str(US_SRC) not in sys.path:
    sys.path.insert(0, str(US_SRC))

from reports.us_daily import render_us_data_calibration_section  # noqa: E402
from sections.left_side import render_us_left_side_section  # noqa: E402


def test_us_data_calibration_marks_previous_session() -> None:
    lines = render_us_data_calibration_section(
        {
            "as_of": "2026-06-04",
            "us": {"current_date": "2026-06-03"},
            "us_market_data_status": {
                "state": "previous_session",
                "prices_daily_latest_date": "2026-06-03",
                "options_analysis_latest_as_of": "2026-06-03",
                "options_sentiment_latest_as_of": "2026-06-03",
                "market_quotes_latest_as_of": "2026-06-03",
                "market_quotes_latest_session": "post",
                "market_quotes_latest_quote_time": "2026-06-03T21:24:27",
                "is_previous_session": True,
            },
            "gamma_spring": {"effective_date": "2026-06-03"},
            "fear_greed": {"source": "cnn", "score": 54.0, "rating": "neutral"},
        }
    )
    text = "\n".join(lines)

    assert "## 数据校准" in text
    assert "| 报告标签日期 | 2026-06-04 |" in text
    assert "| US 收盘价数据截至 | 2026-06-03 |" in text
    assert "| US 盘前/盘后 quote 最新 | 2026-06-03 / post / 2026-06-03T21:24:27 |" in text
    assert "不能读成 `2026-06-04` 已收盘结果" in text


def test_us_left_side_uses_effective_market_date(tmp_path: Path) -> None:
    for day, symbol in [("2026-06-03", "RIGHT"), ("2026-06-04", "WRONG")]:
        out_dir = tmp_path / day
        out_dir.mkdir()
        (out_dir / "mean_reversion_radar.csv").write_text(
            "\n".join(
                [
                    "symbol,company_name,ret_5d_pct,ret_20d_pct,dist_close_ema21_pct,dist_close_ema50_pct,is_mean_reversion_candidate,in_ai_universe,reasons,valuation_signal",
                    f"{symbol},{symbol} Corp,-1,-2,-3,-4,yes,yes,below_ema21,cheap",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    lines = render_us_left_side_section(
        {
            "as_of": "2026-06-04",
            "risk_regime": {"state": "wedge", "r_multiplier": 0.6},
            "us_market_data_status": {
                "effective_us_market_date": "2026-06-03",
                "prices_daily_latest_date": "2026-06-03",
            },
        },
        us_mean_reversion_root=tmp_path,
    )
    text = "\n".join(lines)

    assert "数据日: 2026-06-03" in text
    assert "RIGHT" in text
    assert "WRONG" not in text
