"""
Overnight execution gate for post-close / pre-open decision support.

This module does not try to predict the full next-day path from scratch.
It answers a narrower question:

  "Given the latest options structure and the reference price already implied
   by the chain, is the move still executable, should we wait for pullback,
   or has the overnight gap already consumed the edge?"

Outputs one row per symbol into ``analysis_daily`` with:
  - trend_prob: continuation probability proxy
  - p_upside: continuation probability
  - p_downside: fade probability
  - daily_risk_usd: overnight stretch in dollars
  - expected_move_pct: max chase gap threshold (%)
  - z_score: gap_vs_expected_move
  - strength_bucket / regime
  - details: JSON payload consumed by filtering/reporting
"""
from __future__ import annotations

import json
import math
from datetime import date
from typing import Any

import duckdb
import polars as pl
import structlog

from quant_bot.config.strategy_params import get_us_strategy_param_section, load_us_strategy_params
from quant_bot.filtering._common import _parse_unusual

log = structlog.get_logger()


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _sign(x: float | None) -> int:
    if x is None:
        return 0
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _safe_float(v, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        out = float(v)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def _section(params: dict[str, Any] | None, name: str) -> dict[str, Any]:
    if not isinstance(params, dict):
        return {}
    value = params.get(name)
    return value if isinstance(value, dict) else {}


def _p(params: dict[str, Any] | None, key: str, default: float) -> float:
    value = _safe_float((params or {}).get(key), default)
    return default if value is None else value


def _trend_alignment_score(
    *,
    gap_dir: int,
    trend_prob: float | None,
    trend_regime: str | None,
    params: dict[str, Any] | None = None,
) -> float:
    p = _section(params, "trend_alignment")
    if gap_dir == 0:
        return _p(p, "neutral_no_gap", 0.5)
    if trend_prob is None:
        return _p(p, "missing_trend", 0.35)

    signed_edge = gap_dir * (trend_prob - 0.5)
    base = _clamp01(0.5 + signed_edge / _p(p, "signed_edge_scale", 0.25))
    if signed_edge <= 0.0:
        return base

    regime_bonus = 0.0
    if trend_regime == "trending":
        regime_bonus = _p(p, "trending_bonus", 0.03)
    elif trend_regime == "noisy":
        regime_bonus = _p(p, "noisy_bonus", 0.02)
    elif trend_regime == "mean_reverting" and abs(trend_prob - 0.5) <= _p(
        p, "mean_reverting_neutral_band", 0.08
    ):
        regime_bonus = _p(p, "mean_reverting_bonus", 0.025)
    return _clamp01(base + regime_bonus)


def _discipline_support_score(
    *,
    gap_dir: int,
    gap_vs_expected_move: float | None,
    cone_position_68: float | None,
    params: dict[str, Any] | None = None,
) -> float:
    p = _section(params, "discipline")
    if gap_dir == 0:
        return _p(p, "no_gap_support", 0.55)

    if gap_vs_expected_move is None:
        gap_comfort = _p(p, "missing_gap_comfort", 0.60)
    else:
        gap_comfort = 1.0 - _clamp01(
            ((gap_vs_expected_move or 0.0) - _p(p, "gap_offset", 0.15))
            / _p(p, "gap_span", 0.85)
        )

    if cone_position_68 is None:
        cone_comfort = _p(p, "missing_cone_comfort", 0.60)
    elif gap_dir > 0:
        cone_comfort = 1.0 - _clamp01(
            (cone_position_68 - _p(p, "cone_upper_mid", 0.58)) / _p(p, "cone_span", 0.32)
        )
    else:
        cone_comfort = 1.0 - _clamp01(
            (_p(p, "cone_lower_mid", 0.42) - cone_position_68) / _p(p, "cone_span", 0.32)
        )

    return min(
        _clamp01(
            _p(p, "gap_weight", 0.60) * gap_comfort
            + _p(p, "cone_weight", 0.40) * cone_comfort
        ),
        _p(p, "cap", 0.85),
    )


def _support_regime_bonus(
    *,
    gap_dir: int,
    trend_regime: str | None,
    trend_alignment: float,
    discipline_support: float,
    flow_intensity: float,
    bias_support: float,
    params: dict[str, Any] | None = None,
) -> float:
    p = _section(params, "support_regime_bonus")
    if gap_dir == 0:
        return 0.0
    if trend_regime == "trending" and trend_alignment >= _p(p, "trending_alignment_min", 0.62):
        return _p(p, "trending_bonus", 0.03)
    if (
        trend_regime == "noisy"
        and discipline_support >= _p(p, "noisy_discipline_min", 0.74)
        and (
            flow_intensity >= _p(p, "noisy_flow_min", 0.30)
            or bias_support >= _p(p, "noisy_bias_min", 0.75)
        )
    ):
        return _p(p, "noisy_bonus", 0.02)
    if (
        trend_regime == "mean_reverting"
        and trend_alignment >= _p(p, "mean_reverting_alignment_min", 0.55)
        and discipline_support >= _p(p, "mean_reverting_discipline_min", 0.72)
    ):
        return _p(p, "mean_reverting_bonus", 0.035)
    return 0.0


def _compute_support_score(
    *,
    flow_intensity: float,
    iv_delta: float | None,
    skew_delta: float | None,
    pc_delta: float | None,
    bias_support: float,
    trend_alignment: float,
    discipline_support: float,
    sentiment_support: float,
    regime_bonus: float,
    params: dict[str, Any] | None = None,
) -> float:
    p = _section(params, "support_weights")
    support_score = (
        _p(p, "flow_intensity", 0.20) * flow_intensity
        + _p(p, "iv_delta", 0.12) * (iv_delta or 0.0)
        + _p(p, "skew_delta", 0.08) * (skew_delta or 0.0)
        + _p(p, "pc_delta", 0.06) * (pc_delta or 0.0)
        + _p(p, "bias_support", 0.10) * bias_support
        + _p(p, "trend_alignment", 0.18) * trend_alignment
        + _p(p, "discipline_support", 0.14) * discipline_support
        + _p(p, "sentiment_support", 0.12) * sentiment_support
        + regime_bonus
    )
    return _clamp01(support_score)


def _compute_continuation_probability(
    *,
    support_score: float,
    trend_alignment: float,
    discipline_support: float,
    stretch_score: float,
    trend_regime: str | None,
    gap_dir: int,
    params: dict[str, Any] | None = None,
) -> float:
    p = _section(params, "continuation_probability")
    regime_bonus = 0.0
    if gap_dir != 0:
        if trend_regime == "trending" and trend_alignment >= _p(p, "trending_alignment_min", 0.62):
            regime_bonus = _p(p, "trending_bonus", 0.03)
        elif trend_regime == "mean_reverting" and discipline_support >= _p(
            p, "mean_reverting_discipline_min", 0.72
        ):
            regime_bonus = _p(p, "mean_reverting_bonus", 0.035)
        elif (
            trend_regime == "noisy"
            and support_score >= _p(p, "noisy_support_min", 0.48)
            and discipline_support >= _p(p, "noisy_discipline_min", 0.74)
        ):
            regime_bonus = _p(p, "noisy_bonus", 0.02)
    return _clamp01(
        _p(p, "base", 0.14)
        + _p(p, "support_score", 0.50) * support_score
        + _p(p, "trend_alignment", 0.12) * trend_alignment
        + _p(p, "discipline_support", 0.08) * discipline_support
        + regime_bonus
        + _p(p, "stretch_score", -0.22) * stretch_score
    )


def _continuation_relief(
    *,
    gap_dir: int,
    trend_dir: int,
    trend_regime: str | None,
    p_continue: float,
    support_score: float,
    trend_alignment: float,
    discipline_support: float,
    params: dict[str, Any] | None = None,
) -> float:
    p = _section(params, "continuation_relief")
    if gap_dir == 0:
        return 0.0

    relief = 0.0
    if trend_regime == "trending" and trend_dir == gap_dir:
        relief += _p(p, "trend_match_bonus", 0.10)
    elif trend_alignment >= _p(p, "alignment_min", 0.65):
        relief += _p(p, "alignment_bonus", 0.06)
    if p_continue >= _p(p, "p_continue_min", 0.60):
        relief += (
            min(
                (p_continue - _p(p, "p_continue_min", 0.60))
                / _p(p, "p_continue_span", 0.20),
                1.0,
            )
            * _p(p, "p_continue_bonus", 0.08)
        )
    if support_score >= _p(p, "support_min", 0.52):
        relief += (
            min(
                (support_score - _p(p, "support_min", 0.52)) / _p(p, "support_span", 0.28),
                1.0,
            )
            * _p(p, "support_bonus", 0.06)
        )
    if discipline_support >= _p(p, "discipline_min", 0.75):
        relief += (
            min(
                (discipline_support - _p(p, "discipline_min", 0.75))
                / _p(p, "discipline_span", 0.10),
                1.0,
            )
            * _p(p, "discipline_bonus", 0.03)
        )
    return min(relief, _p(p, "cap", 0.22))


def _nearest_options_current(
    con: duckdb.DuckDBPyConnection,
    as_of_str: str,
):
    try:
        return con.execute(
            """
            WITH nearest AS (
                SELECT symbol, MIN(days_to_exp) AS min_exp
                FROM options_analysis
                WHERE as_of = ?
                GROUP BY symbol
            )
            SELECT
                oa.symbol,
                oa.current_price,
                oa.range_68_low,
                oa.range_68_high,
                oa.range_95_low,
                oa.range_95_high,
                oa.atm_iv,
                oa.iv_skew,
                oa.bias_signal,
                oa.put_call_vol_ratio,
                oa.unusual_strikes,
                oa.days_to_exp,
                os.expected_move_pct
            FROM options_analysis oa
            INNER JOIN nearest n
                ON oa.symbol = n.symbol
               AND oa.days_to_exp = n.min_exp
               AND oa.as_of = ?
            LEFT JOIN options_snapshot os
                ON oa.symbol = os.symbol
               AND oa.as_of = os.as_of
               AND oa.expiry = os.expiry
            """
            ,
            [as_of_str, as_of_str],
        ).fetchdf()
    except Exception:
        return None


def _historical_options_context(
    con: duckdb.DuckDBPyConnection,
    as_of_str: str,
    params: dict[str, Any] | None = None,
):
    try:
        as_of_dt = date.fromisoformat(as_of_str)
        p = _section(params, "historical_context")
        lo = as_of_dt.fromordinal(as_of_dt.toordinal() - int(_p(p, "lo_days", 10))).strftime(
            "%Y-%m-%d"
        )
        hi = as_of_dt.fromordinal(as_of_dt.toordinal() - int(_p(p, "hi_days", 5))).strftime(
            "%Y-%m-%d"
        )
        return con.execute(
            """
            WITH hist_dates AS (
                SELECT symbol,
                       as_of AS hist_as_of,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol
                           ORDER BY ABS(DATE_DIFF('day', as_of, CAST(? AS DATE))) ASC,
                                    as_of DESC
                       ) AS rn
                FROM options_analysis
                WHERE as_of BETWEEN ? AND ?
            ),
            hist_nearest AS (
                SELECT h.symbol,
                       h.hist_as_of,
                       MIN(oa.days_to_exp) AS min_exp
                FROM hist_dates h
                JOIN options_analysis oa
                  ON oa.symbol = h.symbol
                 AND oa.as_of = h.hist_as_of
                WHERE h.rn = 1
                GROUP BY h.symbol, h.hist_as_of
            )
            SELECT
                oa.symbol,
                hn.hist_as_of,
                oa.atm_iv,
                oa.iv_skew,
                oa.put_call_vol_ratio,
                oa.unusual_strikes
            FROM hist_nearest hn
            JOIN options_analysis oa
              ON oa.symbol = hn.symbol
             AND oa.as_of = hn.hist_as_of
             AND oa.days_to_exp = hn.min_exp
            """
            ,
            [as_of_str, lo, hi],
        ).fetchdf()
    except Exception:
        return None


def run_overnight_gate(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> pl.DataFrame:
    """
    Compute overnight execution gate for the supplied symbols.
    """
    if not symbols:
        return pl.DataFrame()

    strategy_params = load_us_strategy_params()
    gate_params = get_us_strategy_param_section("overnight_gate")
    price_params = _section(gate_params, "price_context")
    atr_window = int(_p(price_params, "atr_window", 14))
    prior_range_end = int(_p(price_params, "prior_range_window", 20)) + 1
    as_of_str = as_of.strftime("%Y-%m-%d")
    placeholders = ",".join("?" * len(symbols))

    price_df = con.execute(
        f"""
        WITH ranked AS (
            SELECT symbol,
                   date,
                   close,
                   high,
                   low,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
            FROM prices_daily
            WHERE symbol IN ({placeholders})
              AND date <= ?
        )
        SELECT
            symbol,
            MAX(CASE WHEN rn = 1 THEN close END) AS last_close,
            AVG(CASE WHEN rn BETWEEN 1 AND {atr_window} THEN high - low END) AS atr_14,
            MAX(CASE WHEN rn BETWEEN 2 AND {prior_range_end} THEN high END) AS high_20d,
            MIN(CASE WHEN rn BETWEEN 2 AND {prior_range_end} THEN low END) AS low_20d
        FROM ranked
        GROUP BY symbol
        """,
        symbols + [as_of_str],
    ).fetchdf()
    if price_df.empty:
        log.warning("overnight_gate_no_price_data", as_of=as_of_str)
        return pl.DataFrame()

    price_map = {r["symbol"]: r for _, r in price_df.iterrows()}

    mom_df = con.execute(
        f"""
        SELECT symbol, trend_prob, regime, daily_risk_usd
        FROM analysis_daily
        WHERE date = ?
          AND module_name = 'momentum_risk'
          AND symbol IN ({placeholders})
        """,
        [as_of_str] + symbols,
    ).fetchdf()
    mom_map = {r["symbol"]: r for _, r in mom_df.iterrows()} if not mom_df.empty else {}

    try:
        sent_df = con.execute(
            f"""
            SELECT symbol, vrp, pc_ratio_z, skew_z
            FROM options_sentiment
            WHERE as_of = ?
              AND symbol IN ({placeholders})
            """,
            [as_of_str] + symbols,
        ).fetchdf()
    except Exception:
        sent_df = None
    sent_map = {r["symbol"]: r for _, r in sent_df.iterrows()} if sent_df is not None and not sent_df.empty else {}

    cur_opts_df = _nearest_options_current(con, as_of_str)
    cur_opts_map = {r["symbol"]: r for _, r in cur_opts_df.iterrows()} if cur_opts_df is not None and not cur_opts_df.empty else {}

    hist_opts_df = _historical_options_context(con, as_of_str, gate_params)
    hist_opts_map = {r["symbol"]: r for _, r in hist_opts_df.iterrows()} if hist_opts_df is not None and not hist_opts_df.empty else {}

    rows: list[dict] = []

    for sym in symbols:
        px = price_map.get(sym)
        if px is None:
            continue

        last_close = _safe_float(px.get("last_close"))
        if not last_close or last_close <= 0:
            continue

        mom = mom_map.get(sym)
        sent = sent_map.get(sym)
        cur_opts = cur_opts_map.get(sym)
        hist_opts = hist_opts_map.get(sym)
        mom_row = mom if mom is not None else {}
        sent_row = sent if sent is not None else {}
        cur_opts_row = cur_opts if cur_opts is not None else {}
        hist_opts_row = hist_opts if hist_opts is not None else {}

        ref_price = _safe_float(cur_opts_row.get("current_price"))
        if not ref_price or ref_price <= 0:
            ref_price = last_close

        atr_usd = _safe_float(mom_row.get("daily_risk_usd")) or _safe_float(px.get("atr_14")) or 0.0
        atr_pct = (atr_usd / last_close * 100.0) if atr_usd and last_close > 0 else None

        gap_pct = ((ref_price / last_close) - 1.0) * 100.0 if last_close > 0 else 0.0
        gap_dir = _sign(gap_pct)

        expected_move_pct = _safe_float(cur_opts_row.get("expected_move_pct"))
        gap_vs_expected_move = (
            abs(gap_pct) / expected_move_pct
            if expected_move_pct and expected_move_pct > 0
            else None
        )
        gap_vs_atr = abs(ref_price - last_close) / atr_usd if atr_usd and atr_usd > 0 else None

        range_68_low = _safe_float(cur_opts_row.get("range_68_low"))
        range_68_high = _safe_float(cur_opts_row.get("range_68_high"))
        cone_position_68 = None
        if (
            range_68_low is not None
            and range_68_high is not None
            and range_68_high > range_68_low
        ):
            cone_position_68 = (ref_price - range_68_low) / (range_68_high - range_68_low)

        atm_iv = _safe_float(cur_opts_row.get("atm_iv"))
        hist_iv = _safe_float(hist_opts_row.get("atm_iv"))
        delta_params = _section(gate_params, "delta_features")
        iv_delta = None
        if atm_iv and hist_iv and hist_iv > 0:
            iv_delta = abs(
                math.log((atm_iv + _p(delta_params, "iv_epsilon", 1e-6)) / hist_iv)
            ) / math.log(_p(delta_params, "iv_log_base", 2.0))
            iv_delta = _clamp01(iv_delta)

        cur_skew = _safe_float(cur_opts_row.get("iv_skew"))
        hist_skew = _safe_float(hist_opts_row.get("iv_skew"))
        skew_delta = None
        if cur_skew is not None and hist_skew is not None:
            skew_delta = _clamp01(
                abs(cur_skew - hist_skew) / _p(delta_params, "skew_delta_scale", 0.35)
            )

        cur_pc = _safe_float(cur_opts_row.get("put_call_vol_ratio"))
        hist_pc = _safe_float(hist_opts_row.get("put_call_vol_ratio"))
        pc_delta = None
        if cur_pc is not None and hist_pc is not None:
            pc_offset = _p(delta_params, "pc_offset", 0.25)
            pc_delta = _clamp01(
                abs(math.log((cur_pc + pc_offset) / (hist_pc + pc_offset)))
                / math.log(_p(delta_params, "pc_log_base", 3.0))
            )

        unusual = _parse_unusual(cur_opts_row.get("unusual_strikes"))
        total_vol = sum(_safe_float(u.get("volume"), 0.0) or 0.0 for u in unusual)
        max_vol_oi = max((_safe_float(u.get("vol_oi_ratio"), 0.0) or 0.0 for u in unusual), default=0.0)
        flow_params = _section(gate_params, "flow")
        flow_intensity = _clamp01(
            _p(flow_params, "volume_weight", 0.55)
            * min(
                math.log10(max(total_vol, 1.0))
                / math.log10(max(_p(flow_params, "volume_norm", 50_000.0), 10.0)),
                1.0,
            )
            + _p(flow_params, "ratio_weight", 0.45)
            * min(max_vol_oi / max(_p(flow_params, "vol_oi_norm", 40.0), 1.0), 1.0)
        ) if unusual else 0.0

        bias_signal = cur_opts_row.get("bias_signal") or "neutral"
        bias_dir = 1 if bias_signal == "bullish" else -1 if bias_signal == "bearish" else 0
        sentiment_params = _section(gate_params, "sentiment")
        bias_support = _p(sentiment_params, "neutral_support", 0.50)
        if gap_dir == 0:
            bias_support = _p(sentiment_params, "neutral_support", 0.50)
        elif bias_dir == gap_dir:
            bias_support = _p(sentiment_params, "aligned_bias_support", 1.0)
        elif bias_dir == -gap_dir:
            bias_support = _p(sentiment_params, "opposed_bias_support", 0.0)

        trend_prob = _safe_float(mom_row.get("trend_prob"))
        trend_regime = mom_row.get("regime") or "unknown"
        trend_dir = (
            1
            if (trend_prob is not None and trend_prob > _p(sentiment_params, "trend_dir_upper", 0.56))
            else -1
            if (trend_prob is not None and trend_prob < _p(sentiment_params, "trend_dir_lower", 0.44))
            else 0
        )
        trend_neutral = _p(sentiment_params, "trend_neutral_probability", 0.50)
        trend_scale = _p(sentiment_params, "trend_support_scale", 2.0)
        if gap_dir == 0:
            trend_support = (
                abs((trend_prob or trend_neutral) - trend_neutral) * trend_scale
                if trend_prob is not None
                else 0.0
            )
        elif trend_dir == gap_dir:
            trend_support = (
                abs((trend_prob or trend_neutral) - trend_neutral) * trend_scale
                if trend_prob is not None
                else 0.0
            )
        else:
            trend_support = 0.0
        trend_support = _clamp01(trend_support)
        trend_alignment = _trend_alignment_score(
            gap_dir=gap_dir,
            trend_prob=trend_prob,
            trend_regime=trend_regime,
            params=gate_params,
        )

        pc_ratio_z = _safe_float(sent_row.get("pc_ratio_z"))
        skew_z = _safe_float(sent_row.get("skew_z"))
        sentiment_support = _p(sentiment_params, "neutral_support", 0.50)
        vote_z = _p(sentiment_params, "vote_z_threshold", 0.50)
        vote_weight = _p(sentiment_params, "vote_weight", 0.50)
        if gap_dir > 0:
            bullish_votes = 0.0
            if pc_ratio_z is not None and pc_ratio_z < -vote_z:
                bullish_votes += vote_weight
            if skew_z is not None and skew_z < -vote_z:
                bullish_votes += vote_weight
            sentiment_support = bullish_votes
        elif gap_dir < 0:
            bearish_votes = 0.0
            if pc_ratio_z is not None and pc_ratio_z > vote_z:
                bearish_votes += vote_weight
            if skew_z is not None and skew_z > vote_z:
                bearish_votes += vote_weight
            sentiment_support = bearish_votes
        discipline_support = _discipline_support_score(
            gap_dir=gap_dir,
            gap_vs_expected_move=gap_vs_expected_move,
            cone_position_68=cone_position_68,
            params=gate_params,
        )
        regime_support_bonus = _support_regime_bonus(
            gap_dir=gap_dir,
            trend_regime=trend_regime,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            flow_intensity=flow_intensity,
            bias_support=bias_support,
            params=gate_params,
        )

        support_score = _compute_support_score(
            flow_intensity=flow_intensity,
            iv_delta=iv_delta,
            skew_delta=skew_delta,
            pc_delta=pc_delta,
            bias_support=bias_support,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            sentiment_support=max(
                sentiment_support,
                _p(sentiment_params, "trend_support_weight", 0.50) * trend_support,
            ),
            regime_bonus=regime_support_bonus,
            params=gate_params,
        )

        stretch_params = _section(gate_params, "stretch")
        gap_consumed = _clamp01(
            ((gap_vs_expected_move or 0.0) - _p(stretch_params, "gap_consumed_offset", 0.55))
            / _p(stretch_params, "gap_consumed_span", 0.60)
        )
        gap_vs_atr_norm = _clamp01(
            ((gap_vs_atr or 0.0) - _p(stretch_params, "gap_atr_offset", 0.75))
            / _p(stretch_params, "gap_atr_span", 0.75)
        )
        cone_stretch = 0.0
        if cone_position_68 is not None:
            if gap_dir >= 0:
                cone_stretch = _clamp01(
                    (cone_position_68 - _p(stretch_params, "cone_upper", 0.78))
                    / _p(stretch_params, "cone_span", 0.18)
                )
            else:
                cone_stretch = _clamp01(
                    (_p(stretch_params, "cone_lower", 0.22) - cone_position_68)
                    / _p(stretch_params, "cone_span", 0.18)
                )

        stretch_score = _clamp01(
            _p(stretch_params, "gap_weight", 0.45) * gap_consumed
            + _p(stretch_params, "atr_weight", 0.30) * gap_vs_atr_norm
            + _p(stretch_params, "cone_weight", 0.25) * cone_stretch
        )

        p_continue = _compute_continuation_probability(
            support_score=support_score,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            stretch_score=stretch_score,
            trend_regime=trend_regime,
            gap_dir=gap_dir,
            params=gate_params,
        )
        fade_params = _section(gate_params, "fade_probability")
        p_fade = _clamp01(
            _p(fade_params, "base", 0.10)
            + _p(fade_params, "stretch_score", 0.62) * stretch_score
            + _p(fade_params, "bias_gap", 0.16) * (1.0 - bias_support)
            + _p(fade_params, "trend_alignment_gap", 0.08) * (1.0 - trend_alignment)
            + _p(fade_params, "discipline_gap", 0.04) * (1.0 - discipline_support)
        )

        max_chase_gap_pct = None
        chase_params = _section(gate_params, "chase_gap")
        if expected_move_pct and expected_move_pct > 0:
            max_chase_gap_pct = max(
                _p(chase_params, "min_pct", 0.75),
                min(
                    _p(chase_params, "max_pct", 4.0),
                    expected_move_pct
                    * (
                        _p(chase_params, "expected_move_base", 0.55)
                        + _p(chase_params, "expected_move_support", 0.20) * support_score
                    ),
                ),
            )
        elif atr_pct and atr_pct > 0:
            max_chase_gap_pct = max(
                _p(chase_params, "min_pct", 0.75),
                min(
                    _p(chase_params, "max_pct", 4.0),
                    atr_pct
                    * (
                        _p(chase_params, "atr_base", 0.80)
                        + _p(chase_params, "atr_support", 0.20) * support_score
                    ),
                ),
            )

        gap_abs = abs(ref_price - last_close)
        pullback_price = ref_price
        if gap_abs > 0:
            if gap_dir >= 0:
                pullback_price = ref_price - _p(chase_params, "pullback_gap_fraction", 0.35) * gap_abs
            else:
                pullback_price = ref_price + _p(chase_params, "pullback_gap_fraction", 0.35) * gap_abs

        continuation_relief = _continuation_relief(
            gap_dir=gap_dir,
            trend_dir=trend_dir,
            trend_regime=trend_regime,
            p_continue=p_continue,
            support_score=support_score,
            trend_alignment=trend_alignment,
            discipline_support=discipline_support,
            params=gate_params,
        )
        effective_stretch_score = _clamp01(stretch_score - continuation_relief)
        action_params = _section(gate_params, "action")
        wait_gap_threshold = _p(action_params, "wait_gap_base", 0.65) + _p(
            action_params, "wait_gap_relief", 0.35
        ) * continuation_relief
        do_not_chase_gap_threshold = _p(
            action_params, "do_not_chase_gap_base", 1.00
        ) + _p(action_params, "do_not_chase_gap_relief", 0.45) * continuation_relief

        if effective_stretch_score >= _p(action_params, "do_not_chase_stretch", 0.78) or (
            (gap_vs_expected_move or 0.0) > do_not_chase_gap_threshold
            and support_score < _p(action_params, "do_not_chase_support_max", 0.48)
        ):
            action = "do_not_chase"
            regime = "stretched"
        elif effective_stretch_score >= _p(action_params, "wait_stretch", 0.50) or (
            (gap_vs_expected_move or 0.0) > wait_gap_threshold
            and p_continue < _p(action_params, "wait_p_continue_max", 0.68)
        ):
            action = "wait_pullback"
            regime = "fade" if p_fade >= p_continue else "neutral"
        elif p_continue >= _p(action_params, "continue_min", 0.58):
            action = "executable_now"
            regime = "continue"
        elif p_fade >= _p(action_params, "fade_min", 0.58):
            action = "wait_pullback"
            regime = "fade"
        else:
            action = "executable_now"
            regime = "neutral"

        strength_bucket = (
            "strong" if action == "executable_now" and p_continue >= _p(action_params, "strong_continue_min", 0.65)
            else "moderate" if action != "do_not_chase" and p_continue >= _p(action_params, "moderate_continue_min", 0.52)
            else "weak" if action == "wait_pullback"
            else "inconclusive"
        )

        details = {
            "ref_price": round(ref_price, 4),
            "last_close": round(last_close, 4),
            "gap_pct": round(gap_pct, 3),
            "gap_vs_expected_move": round(gap_vs_expected_move, 3) if gap_vs_expected_move is not None else None,
            "gap_vs_atr": round(gap_vs_atr, 3) if gap_vs_atr is not None else None,
            "cone_position_68": round(cone_position_68, 3) if cone_position_68 is not None else None,
            "atm_iv": round(atm_iv, 4) if atm_iv is not None else None,
            "iv_delta": round(iv_delta, 3) if iv_delta is not None else None,
            "skew_delta": round(skew_delta, 3) if skew_delta is not None else None,
            "pc_delta": round(pc_delta, 3) if pc_delta is not None else None,
            "flow_intensity": round(flow_intensity, 3),
            "bias_signal": bias_signal,
            "trend_prob": round(trend_prob, 4) if trend_prob is not None else None,
            "trend_regime": trend_regime,
            "vrp": round(_safe_float(sent_row.get("vrp"), 0.0) or 0.0, 6),
            "pc_ratio_z": round(pc_ratio_z, 3) if pc_ratio_z is not None else None,
            "skew_z": round(skew_z, 3) if skew_z is not None else None,
            "trend_alignment": round(trend_alignment, 3),
            "discipline_support": round(discipline_support, 3),
            "regime_support_bonus": round(regime_support_bonus, 3),
            "support_score": round(support_score, 3),
            "stretch_score": round(stretch_score, 3),
            "effective_stretch_score": round(effective_stretch_score, 3),
            "continuation_relief": round(continuation_relief, 3),
            "action": action,
            "max_chase_gap_pct": round(max_chase_gap_pct, 3) if max_chase_gap_pct is not None else None,
            "pullback_price": round(pullback_price, 4),
            "param_source": strategy_params.get("_source", "built_in_default"),
            "param_provenance": gate_params.get("provenance", "legacy_heuristic"),
        }

        rows.append(
            {
                "symbol": sym,
                "date": as_of,
                "module_name": "overnight_gate",
                "trend_prob": round(p_continue, 4),
                "p_upside": round(p_continue, 4),
                "p_downside": round(p_fade, 4),
                "daily_risk_usd": round(gap_abs, 4),
                "expected_move_pct": round(max_chase_gap_pct, 4) if max_chase_gap_pct is not None else None,
                "z_score": round(gap_vs_expected_move, 4) if gap_vs_expected_move is not None else None,
                "p_value_raw": None,
                "p_value_bonf": None,
                "strength_bucket": strength_bucket,
                "regime": regime,
                "details": json.dumps(details),
            }
        )

    if not rows:
        return pl.DataFrame()

    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


def store_overnight_gate(con: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Store overnight execution-gate results into analysis_daily."""
    if df.is_empty():
        return 0
    con.register("analysis_updates", df.to_arrow())
    con.execute(
        """
        INSERT OR REPLACE INTO analysis_daily
        SELECT
            symbol, date, module_name,
            trend_prob, p_upside, p_downside,
            daily_risk_usd, expected_move_pct,
            z_score, p_value_raw, p_value_bonf,
            strength_bucket, regime, details
        FROM analysis_updates
        """
    )
    con.unregister("analysis_updates")
    con.commit()
    return len(df)
