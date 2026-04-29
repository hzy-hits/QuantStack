"""Render per-item data tables: price, momentum, options + cone + unusual activity."""
from __future__ import annotations

from typing import Any

from ._render_fmt import _fmt_p, _fmt_pct, _fmt_val

_DASH = "\u2014"


def render_item_data(item: dict, compact: bool = False) -> list[str]:
    """Return lines for the data-table portion of a single notable item."""
    if compact:
        return _price_summary(item)

    lines: list[str] = []

    # Price + momentum (merged table)
    lines += _price_momentum_table(item)

    # Options
    lines += _options_section(item)

    # Sentiment (VRP, EWMA z-scores)
    lines += _sentiment_section(item)

    # Price signals (cointegration, Granger, earnings CAR, Kalman beta)
    lines += _price_signals_section(item)

    return lines


# -- private helpers --------------------------------------------------------


def _price_summary(item: dict) -> list[str]:
    """One-line price summary for compact (LOW tier) items."""
    return [
        f"**Price:** ${_fmt_val(item.get('price'), 2)} | "
        f"1D {_fmt_pct(item.get('ret_1d_pct'))} | "
        f"5D {_fmt_pct(item.get('ret_5d_pct'))} | "
        f"21D {_fmt_pct(item.get('ret_21d_pct'))} | "
        f"52W high {_fmt_pct(item.get('pct_from_52w_high'))} | "
        f"RVOL {_fmt_val(item.get('rel_volume'), 2)}\u00d7 | "
        f"ATR ${_fmt_val(item.get('atr'), 2)}",
        "",
    ]


def _price_momentum_table(item: dict) -> list[str]:
    """Merged price + momentum table (saves one table header)."""
    mom = item.get("momentum") or {}
    mean_reversion = item.get("mean_reversion") or {}
    gate = item.get("execution_gate") or {}
    overnight_alpha = item.get("overnight_alpha") or {}
    headline_mode = str(item.get("_headline_mode") or "unknown").lower()
    report_session = str(item.get("_report_session") or "").lower()
    lines = [
        "**Price & Momentum:**",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Price | ${_fmt_val(item.get('price'), 2)} |",
        f"| Return 1D | {_fmt_pct(item.get('ret_1d_pct'))} |",
        f"| Return 5D | {_fmt_pct(item.get('ret_5d_pct'))} |",
        f"| Return 21D | {_fmt_pct(item.get('ret_21d_pct'))} |",
        f"| From 52W High | {_fmt_pct(item.get('pct_from_52w_high'))} |",
        f"| Relative Volume | {_fmt_val(item.get('rel_volume'), 2)}\u00d7 avg |",
        f"| ATR (14D) | ${_fmt_val(item.get('atr'), 2)} |",
    ]
    if mean_reversion:
        lines += [
            f"| RSI (14D) | {_fmt_val(mean_reversion.get('rsi_14'), 1)} |",
            f"| Bollinger position | {_fmt_val(mean_reversion.get('bb_position'), 3)} |",
            f"| Mean-reversion read | {mean_reversion.get('reversion_direction', _DASH)} / {_fmt_val(mean_reversion.get('reversion_score'), 3)} |",
        ]
    if gate and report_session == "post":
        lines += [
            "| Execution reference | Regular close; overnight/pre-open gate hidden for post-market consistency |",
        ]
    elif gate:
        lines += [
            f"| Live reference price | ${_fmt_val(gate.get('ref_price'), 2)} |",
            f"| Overnight gap vs last close | {_fmt_pct(gate.get('gap_pct'), 2)} |",
            f"| Stretch vs implied move | {_fmt_val(gate.get('gap_vs_expected_move'), 2)}\u00d7 |",
            f"| Execution read | {_execution_phrase(gate.get('action'), headline_mode=headline_mode)} |",
        ]
    if overnight_alpha and report_session != "post":
        calibration = overnight_alpha.get("calibration") or {}
        interval = calibration.get("continuation_hit_rate_interval")
        interval_text = (
            f"{interval[0]:.2f}-{interval[1]:.2f}"
            if isinstance(interval, list) and len(interval) == 2
            else _DASH
        )
        lines += [
            (
                f"| 隔夜延续判断 | {_overnight_alpha_phrase(overnight_alpha.get('advice'))}"
                f" (n={calibration.get('sample_count', 0)}, hit CI={interval_text},"
                f" latest={calibration.get('latest_sample_date') or _DASH}) |"
            ),
        ]
    if mom:
        regime = mom.get("regime", _DASH)
        strength = mom.get("strength_bucket", _DASH)
        lines += [
            f"| Momentum Regime | {regime} |",
            f"| Trend P(continues) | {_fmt_val(mom.get('trend_prob'), 4)} |",
            f"| P(Upside) | {_fmt_val(mom.get('p_upside'), 4)} |",
            f"| P(Downside) | {_fmt_val(mom.get('p_downside'), 4)} |",
            f"| Momentum 20D | {_fmt_pct(mom.get('mom_20d'), 2)} |",
            f"| Momentum 5D | {_fmt_pct(mom.get('mom_5d'), 2)} |",
            f"| Momentum 60D | {_fmt_pct(mom.get('mom_60d'), 2)} |",
            f"| Momentum Accel (5D/20D) | {_fmt_val(mom.get('momentum_accel'), 3)} |",
            f"| Z-Score (universe) | {_fmt_val(mom.get('z_score'), 3)} |",
            f"| Bonferroni p-value | {_fmt_p(mom.get('p_value_bonf'))} |",
            f"| Strength bucket | {strength} |",
            f"| Daily risk (ATR $) | ${_fmt_val(mom.get('daily_risk_usd'), 2)} |",
        ]
    lines.append("")
    return lines


def _execution_phrase(action: str | None, *, headline_mode: str = "unknown") -> str:
    mapping = {
        "executable_now": "Still actionable at current levels",
        "wait_pullback": "Not actionable at current price; wait for a pullback reset",
        "do_not_chase": "Move looks spent here; stand down and do not chase",
    }
    phrase = mapping.get(action or "", "No execution read")
    if headline_mode != "trend":
        phrase += "; headline mode is advisory context"
    return phrase


def _overnight_alpha_phrase(advice: str | None) -> str:
    mapping = {
        "continue": "继续，但只作为延续诊断",
        "wait_pullback": "等回落，不追当前价",
        "do_not_chase": "不追，疑似 alpha 已兑现",
    }
    return mapping.get(advice or "", "等回落，样本不足")


def _options_section(item: dict) -> list[str]:
    opts = item.get("options", {})
    if not opts or not any(v is not None for v in opts.values()):
        return []

    # Opt 3: IV=0% collapse — skip entire options block
    atm_iv = opts.get("atm_iv_pct")
    if atm_iv is None or atm_iv == 0:
        return ["期权数据: 不可用", ""]

    lines: list[str] = [
        "**Options Market Data:**",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| ATM Implied Vol (annualized) | {_fmt_val(opts.get('atm_iv_pct'), 1)}% |",
        f"| Market-implied expected move | \u00b1{_fmt_val(opts.get('expected_move_pct'), 1)}% |",
        f"| Put/Call volume ratio | {_fmt_val(opts.get('put_call_ratio'), 2)} |",
    ]

    # Enhanced analysis fields
    if opts.get("proxy_source"):
        lines.append(f"| Options source | **PROXY** via {opts['proxy_source']} |")
    if opts.get("iv_skew") is not None:
        lines.append(f"| IV Skew (5% OTM put/call) | {_fmt_val(opts.get('iv_skew'), 3)} |")
    if opts.get("iv_delta") is not None:
        lines.append(f"| IV Delta (7D repricing) | {_fmt_val(opts.get('iv_delta'), 3)} |")
    if opts.get("iv_change_pct_pts_7d") is not None:
        lines.append(f"| ATM IV change (7D) | {_fmt_pct(opts.get('iv_change_pct_pts_7d'), 1)} |")
    if opts.get("put_call_ratio_delta_7d") is not None:
        lines.append(f"| P/C ratio delta (7D) | {_fmt_val(opts.get('put_call_ratio_delta_7d'), 3)} |")
    if opts.get("skew_delta_7d") is not None:
        lines.append(f"| Skew delta (7D) | {_fmt_val(opts.get('skew_delta_7d'), 4)} |")
    if opts.get("bias_signal"):
        lines.append(f"| Directional bias | {opts['bias_signal'].upper()} |")
    if opts.get("liquidity_score"):
        lines.append(f"| Chain liquidity | {opts['liquidity_score']} (width={opts.get('chain_width', '?')}, spread={_fmt_val(opts.get('avg_spread_pct'), 1)}%) |")
    if opts.get("history_status") and opts["history_status"] != "available":
        lines.append(f"| IV history | {opts['history_status']} |")
    lines.append("")

    # Probability cone
    lines += _probability_cone(opts)

    # Unusual activity
    lines += _unusual_activity(opts)

    return lines


def _probability_cone(opts: dict) -> list[str]:
    cone = opts.get("probability_cone")
    if not cone or not cone.get("range_68") or not cone.get("range_95"):
        return []
    r68 = cone["range_68"]
    r95 = cone["range_95"]

    # Opt 1: degenerate cone (all bounds collapsed) → one line
    if r68[0] == r68[1] == r95[0] == r95[1]:
        return ["概率锥: 退化（IV不可用）", ""]

    exp = cone.get("expiry", "?")
    dte = cone.get("days_to_exp", "?")
    lines = [
        f"**Options-Implied Probability Cone** (expiry {exp}, {dte}D):",
        f"*Risk-neutral lognormal ranges \u2014 NOT real-world odds. For context only.*",
        "",
        f"| Probability | Low | High |",
        f"|-------------|-----|------|",
        f"| 68% (1\u03c3) | ${_fmt_val(r68[0], 2)} | ${_fmt_val(r68[1], 2)} |",
        f"| 95% (2\u03c3) | ${_fmt_val(r95[0], 2)} | ${_fmt_val(r95[1], 2)} |",
    ]
    cp68 = opts.get("cone_position_68")
    if cp68 is not None:
        lines.append(f"| Current cone position | {cp68:.2f} (0=low, 1=high) |")
    lines.append("")
    return lines


def _unusual_activity(opts: dict) -> list[str]:
    unusual = opts.get("unusual_activity", [])
    if not unusual:
        return []
    lines = [
        "**Unusual Options Activity:**",
        "",
        "| Strike | Type | Volume | OI | Vol/OI |",
        "|--------|------|--------|----|--------|",
    ]
    for u in unusual:
        ratio_str = f"{u['vol_oi_ratio']}\u00d7" if u.get('vol_oi_ratio') else "new"
        lines.append(
            f"| ${u['strike']:.0f} | {u['type']} | "
            f"{u['volume']:,} | {u.get('open_interest', 0):,} | {ratio_str} |"
        )
    lines.append("")
    return lines


def _sentiment_section(item: dict) -> list[str]:
    """Render sentiment data: VRP, EWMA put/call ratio z-score, skew z-score."""
    sent = item.get("sentiment", {})
    if not sent or not any(v is not None for v in sent.values()):
        return []

    lines = [
        "**Sentiment (Options-Derived):**",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
    ]
    if sent.get("vrp") is not None:
        lines.append(f"| Variance Risk Premium (VRP) | {_fmt_val(sent['vrp'], 4)} |")
    if sent.get("iv_rv_ratio") is not None:
        lines.append(f"| IV / RV ratio | {_fmt_val(sent['iv_rv_ratio'], 2)} |")
    if sent.get("pc_ratio_z") is not None:
        lines.append(f"| Put/Call ratio EWMA z-score | {_fmt_val(sent['pc_ratio_z'], 2)} |")
    if sent.get("pc_ratio_raw") is not None:
        lines.append(f"| Put/Call ratio (raw) | {_fmt_val(sent['pc_ratio_raw'], 3)} |")
    if sent.get("skew_z") is not None:
        lines.append(f"| IV Skew EWMA z-score | {_fmt_val(sent['skew_z'], 2)} |")
    if sent.get("skew_raw") is not None:
        lines.append(f"| IV Skew (raw) | {_fmt_val(sent['skew_raw'], 3)} |")
    lines.append("")
    return lines


def _price_signals_section(item: dict) -> list[str]:
    """Render price signal layer: cointegration, Granger, earnings CAR, Kalman beta."""
    ps = item.get("price_signals", {})
    if not ps:
        return []

    lines: list[str] = []

    # Cointegration partners
    coint = ps.get("cointegration")
    if coint:
        lines += [
            "**Cointegration Partners:**",
            "",
            "| Partner | Spread Z | Half-Life | Beta | ADF p-value |",
            "|---------|----------|-----------|------|-------------|",
        ]
        pairs = coint if isinstance(coint, list) else [coint]
        for p in pairs:
            lines.append(
                f"| {p.get('partner', _DASH)} "
                f"| {_fmt_val(p.get('spread_zscore'), 2)} "
                f"| {_fmt_val(p.get('half_life_days'), 1)}d "
                f"| {_fmt_val(p.get('beta'), 3)} "
                f"| {_fmt_val(p.get('adf_pvalue'), 4)} |"
            )
        lines.append("")

    # Granger causality leaders/followers
    granger = ps.get("granger")
    if granger:
        lines += [
            "**Granger Causality:**",
            "",
            "| Role | Counterpart | Lag | F-statistic |",
            "|------|-------------|-----|-------------|",
        ]
        pairs = granger if isinstance(granger, list) else [granger]
        for g in pairs:
            lines.append(
                f"| {g.get('role', _DASH)} "
                f"| {g.get('counterpart', _DASH)} "
                f"| {g.get('lag_days', _DASH)}d "
                f"| {_fmt_val(g.get('f_statistic'), 2)} |"
            )
        lines.append("")

    # Earnings CAR
    car = ps.get("earnings_car")
    if car:
        lines += [
            "**Earnings Cumulative Abnormal Return (CAR):**",
            "",
            f"| Window | CAR |",
            f"|--------|-----|",
            f"| 1D | {_fmt_pct(car.get('car_1d'))} |",
            f"| 3D | {_fmt_pct(car.get('car_3d'))} |",
            f"| 5D | {_fmt_pct(car.get('car_5d'))} |",
            f"| 10D | {_fmt_pct(car.get('car_10d'))} |",
            f"| Pre-event beta | {_fmt_val(car.get('pre_event_beta'), 3)} |",
            f"| Event date | {car.get('event_date', _DASH)} |",
            "",
        ]

    # Opt 2: Kalman beta — skip if divergence < 0.01 (no useful signal)
    kalman = ps.get("kalman_beta")
    if kalman:
        divergence = kalman.get("divergence")
        if divergence is None or abs(divergence) >= 0.01:
            lines += [
                "**Kalman Dynamic Beta:**",
                "",
                f"| Field | Value |",
                f"|-------|-------|",
                f"| Current beta | {_fmt_val(kalman.get('beta_current'), 4)} |",
                f"| 60D mean beta | {_fmt_val(kalman.get('beta_60d_mean'), 4)} |",
                f"| Divergence (current - 60D mean) | {_fmt_val(kalman.get('divergence'), 4)} |",
                f"| Beta std (60D) | {_fmt_val(kalman.get('beta_std'), 4)} |",
                "",
            ]

    return lines
