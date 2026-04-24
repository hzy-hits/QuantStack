"""
Shadow alpha layer for overnight continuation/fade diagnostics.

The module is intentionally explainable and diagnostic-only.  It reuses the
existing overnight execution gate, options context, and report postmortem
labels to answer a narrow question for the report layer:

    continue now, wait for a pullback, or do not chase.

Rows are stored in ``analysis_daily`` with
``module_name='overnight_continuation_alpha'``.  The probability-shaped columns
are compatibility fields for downstream loaders; report copy should use the
advice text and calibration sample counts from ``details``.
"""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import duckdb
import polars as pl


MODULE_NAME = "overnight_continuation_alpha"
LOOKBACK_DAYS = 90
PRIOR_N = 12.0


@dataclass
class CalibrationStats:
    n: int = 0
    latest_sample_date: str | None = None
    labels: Counter[str] = field(default_factory=Counter)

    def add(self, label_group: str, sample_date: str | None) -> None:
        self.n += 1
        self.labels[label_group] += 1
        if sample_date and (self.latest_sample_date is None or sample_date > self.latest_sample_date):
            self.latest_sample_date = sample_date


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def _parse_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _query_df(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> Any:
    try:
        return con.execute(sql, params).fetchdf()
    except Exception:
        return None


def _label_group(label: str | None) -> str:
    if label in {"captured", "missed_alpha"}:
        return "continuation"
    if label in {"alpha_already_paid", "good_signal_bad_timing", "stale_chase"}:
        return "alpha_already_paid"
    if label == "false_positive":
        return "fade"
    if label in {"flat_edge", "ignored_ok"}:
        return "executable"
    return "unresolved"


def _bucket_for(*, gate: dict[str, Any], has_event: bool) -> str:
    action = str(gate.get("action") or "unknown")
    stretch = _safe_float(gate.get("effective_stretch_score"))
    if stretch is None:
        stretch = _safe_float(gate.get("stretch_score"), 0.0) or 0.0
    support = _safe_float(gate.get("support_score"), 0.5) or 0.5

    stretch_bucket = "low_stretch" if stretch < 0.35 else "mid_stretch" if stretch < 0.65 else "high_stretch"
    support_bucket = "weak_support" if support < 0.45 else "ok_support" if support < 0.60 else "strong_support"
    event_bucket = "event" if has_event else "no_event"
    return f"{action}|{stretch_bucket}|{support_bucket}|{event_bucket}"


def _wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float] | None:
    if n <= 0:
        return None
    phat = successes / n
    denom = 1.0 + z * z / n
    centre = phat + z * z / (2.0 * n)
    margin = z * math.sqrt((phat * (1.0 - phat) + z * z / (4.0 * n)) / n)
    return (_clamp01((centre - margin) / denom), _clamp01((centre + margin) / denom))


def _shrunk_rate(successes: int, n: int, prior: float = 0.50) -> float:
    return (successes + prior * PRIOR_N) / (n + PRIOR_N) if n > 0 else prior


def _calibration_payload(stats: CalibrationStats) -> dict[str, Any]:
    continuation_hits = int(stats.labels.get("continuation", 0))
    stale_hits = int(stats.labels.get("alpha_already_paid", 0))
    fade_hits = int(stats.labels.get("fade", 0))
    interval = _wilson_interval(continuation_hits, stats.n)
    return {
        "sample_count": stats.n,
        "continuation_hit_rate": round(continuation_hits / stats.n, 4) if stats.n else None,
        "continuation_hit_rate_interval": (
            [round(interval[0], 4), round(interval[1], 4)] if interval else None
        ),
        "stale_chase_rate": round(stale_hits / stats.n, 4) if stats.n else None,
        "fade_rate": round(fade_hits / stats.n, 4) if stats.n else None,
        "latest_sample_date": stats.latest_sample_date,
        "label_counts": dict(stats.labels),
    }


def _load_history(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
) -> dict[str, CalibrationStats]:
    cutoff = as_of - timedelta(days=LOOKBACK_DAYS)
    df = _query_df(
        con,
        """
        SELECT
            p.symbol,
            p.label,
            CAST(p.evaluation_date AS VARCHAR) AS evaluation_date,
            d.execution_mode,
            d.details_json
        FROM alpha_postmortem p
        LEFT JOIN report_decisions d
          ON d.report_date = p.report_date
         AND d.session = p.session
         AND d.symbol = p.symbol
         AND d.selection_status = p.selection_status
        WHERE p.evaluation_date >= ?
          AND p.evaluation_date < ?
        """,
        [cutoff.strftime("%Y-%m-%d"), as_of.strftime("%Y-%m-%d")],
    )
    stats: dict[str, CalibrationStats] = defaultdict(CalibrationStats)
    overall = stats["_overall"]
    if df is None or df.empty:
        return stats

    for _, row in df.iterrows():
        label_group = _label_group(row.get("label"))
        if label_group == "unresolved":
            continue
        details = _parse_json(row.get("details_json"))
        gate = _parse_json(details.get("execution_gate"))
        if not gate:
            gate = {"action": row.get("execution_mode") or "unknown"}
        has_event = bool(details.get("headline_gate_reasons")) or bool(details.get("events"))
        bucket = _bucket_for(gate=gate, has_event=has_event)
        sample_date = str(row.get("evaluation_date"))[:10] if row.get("evaluation_date") else None
        stats[bucket].add(label_group, sample_date)
        overall.add(label_group, sample_date)
    return stats


def _load_current_rows(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}

    as_of_str = as_of.strftime("%Y-%m-%d")
    placeholders = ",".join("?" * len(symbols))
    rows: dict[str, dict[str, Any]] = {sym: {"symbol": sym} for sym in symbols}

    gate_df = _query_df(
        con,
        f"""
        SELECT symbol, trend_prob, p_downside, z_score, regime, details
        FROM analysis_daily
        WHERE date = ?
          AND module_name = 'overnight_gate'
          AND symbol IN ({placeholders})
        """,
        [as_of_str] + symbols,
    )
    if gate_df is not None and not gate_df.empty:
        for _, row in gate_df.iterrows():
            details = _parse_json(row.get("details"))
            gate = {
                "p_continue": _safe_float(row.get("trend_prob")),
                "p_fade": _safe_float(row.get("p_downside")),
                "gap_vs_expected_move": _safe_float(row.get("z_score")),
                "regime": row.get("regime"),
                **details,
            }
            rows.setdefault(row["symbol"], {"symbol": row["symbol"]})["gate"] = gate

    opts_df = _query_df(
        con,
        f"""
        WITH nearest AS (
            SELECT symbol, MIN(days_to_exp) AS min_exp
            FROM options_analysis
            WHERE as_of = ?
              AND symbol IN ({placeholders})
            GROUP BY symbol
        )
        SELECT
            oa.symbol,
            oa.atm_iv,
            oa.iv_skew,
            oa.put_call_vol_ratio,
            oa.bias_signal,
            oa.liquidity_score,
            oa.days_to_exp,
            os.expected_move_pct
        FROM options_analysis oa
        INNER JOIN nearest n
          ON oa.symbol = n.symbol
         AND oa.days_to_exp = n.min_exp
         AND oa.as_of = ?
        LEFT JOIN options_snapshot os
          ON os.symbol = oa.symbol
         AND os.as_of = oa.as_of
         AND os.expiry = oa.expiry
        """,
        [as_of_str] + symbols + [as_of_str],
    )
    if opts_df is not None and not opts_df.empty:
        for _, row in opts_df.iterrows():
            rows.setdefault(row["symbol"], {"symbol": row["symbol"]})["options"] = row.to_dict()

    lab_df = _query_df(
        con,
        f"""
        SELECT symbol, trend_prob, details
        FROM analysis_daily
        WHERE date = ?
          AND module_name = 'lab_factor'
          AND symbol IN ({placeholders})
        """,
        [as_of_str] + symbols,
    )
    if lab_df is not None and not lab_df.empty:
        for _, row in lab_df.iterrows():
            rows.setdefault(row["symbol"], {"symbol": row["symbol"]})["lab_factor"] = {
                "composite": _safe_float(row.get("trend_prob"), 0.0) or 0.0,
                "details": _parse_json(row.get("details")),
            }

    event_df = _query_df(
        con,
        f"""
        SELECT symbol, COUNT(*) AS event_count
        FROM earnings_calendar
        WHERE symbol IN ({placeholders})
          AND report_date BETWEEN ? AND ?
        GROUP BY symbol
        """,
        symbols
        + [
            (as_of - timedelta(days=1)).strftime("%Y-%m-%d"),
            (as_of + timedelta(days=2)).strftime("%Y-%m-%d"),
        ],
    )
    if event_df is not None and not event_df.empty:
        for _, row in event_df.iterrows():
            rows.setdefault(row["symbol"], {"symbol": row["symbol"]})["event_count"] = int(row.get("event_count") or 0)

    news_df = _query_df(
        con,
        f"""
        SELECT symbol, COUNT(*) AS news_count
        FROM news_items
        WHERE symbol IN ({placeholders})
          AND published_at >= ?
        GROUP BY symbol
        """,
        symbols + [(as_of - timedelta(days=3)).strftime("%Y-%m-%d")],
    )
    if news_df is not None and not news_df.empty:
        for _, row in news_df.iterrows():
            rows.setdefault(row["symbol"], {"symbol": row["symbol"]})["news_count"] = int(row.get("news_count") or 0)

    return rows


def _score_current(
    *,
    current: dict[str, Any],
    stats: CalibrationStats,
) -> dict[str, Any]:
    gate = current.get("gate") or {}
    options = current.get("options") or {}
    lab = current.get("lab_factor") or {}

    p_gate_continue = _safe_float(gate.get("p_continue"), 0.50) or 0.50
    p_gate_fade = _safe_float(gate.get("p_fade"), 0.35) or 0.35
    support = _safe_float(gate.get("support_score"), 0.50) or 0.50
    discipline = _safe_float(gate.get("discipline_support"), 0.50) or 0.50
    trend_alignment = _safe_float(gate.get("trend_alignment"), 0.50) or 0.50
    effective_stretch = _safe_float(gate.get("effective_stretch_score"))
    if effective_stretch is None:
        effective_stretch = _safe_float(gate.get("stretch_score"), 0.0) or 0.0
    gap_vs_move = _safe_float(gate.get("gap_vs_expected_move"), 0.0) or 0.0

    continuation_hits = int(stats.labels.get("continuation", 0))
    stale_hits = int(stats.labels.get("alpha_already_paid", 0))
    fade_hits = int(stats.labels.get("fade", 0))
    hist_continue = _shrunk_rate(continuation_hits, stats.n, prior=0.50)
    hist_stale = _shrunk_rate(stale_hits, stats.n, prior=0.25)
    hist_fade = _shrunk_rate(fade_hits, stats.n, prior=0.25)

    lab_composite = abs(_safe_float(lab.get("composite"), 0.0) or 0.0)
    event_count = int(current.get("event_count") or 0)
    news_count = int(current.get("news_count") or 0)
    event_boost = 0.035 if event_count or news_count >= 2 else 0.0

    option_liquidity = str(options.get("liquidity_score") or "")
    liquidity_adj = 0.025 if option_liquidity == "good" else -0.025 if option_liquidity == "poor" else 0.0

    continuation_score = _clamp01(
        0.38 * p_gate_continue
        + 0.18 * support
        + 0.12 * trend_alignment
        + 0.17 * hist_continue
        + 0.06 * lab_composite
        + event_boost
        + liquidity_adj
        - 0.16 * effective_stretch
        - 0.10 * hist_stale
    )
    fade_score = _clamp01(
        0.42 * p_gate_fade
        + 0.22 * hist_fade
        + 0.18 * effective_stretch
        + 0.10 * (1.0 - support)
        + 0.08 * (1.0 - discipline)
    )
    paid_risk = _clamp01(
        0.40 * effective_stretch
        + 0.30 * hist_stale
        + 0.18 * _clamp01((gap_vs_move - 0.65) / 0.55)
        + 0.12 * (1.0 - discipline)
    )
    entry_quality = _clamp01(
        0.38 * continuation_score
        + 0.24 * discipline
        + 0.18 * support
        + 0.12 * (1.0 - paid_risk)
        + 0.08 * (1.0 - fade_score)
    )

    gate_action = gate.get("action") or "unknown"
    if gate_action == "do_not_chase" or paid_risk >= 0.62:
        advice = "do_not_chase"
        regime = "alpha_already_paid"
    elif gate_action == "wait_pullback" or paid_risk >= 0.44 or entry_quality < 0.52:
        advice = "wait_pullback"
        regime = "conditional"
    elif continuation_score >= 0.55 and entry_quality >= 0.56:
        advice = "continue"
        regime = "continuation"
    else:
        advice = "wait_pullback"
        regime = "mixed"

    return {
        "continuation_score": continuation_score,
        "fade_score": fade_score,
        "paid_risk": paid_risk,
        "entry_quality": entry_quality,
        "advice": advice,
        "regime": regime,
    }


def run_overnight_continuation_alpha(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> pl.DataFrame:
    """Run daily shadow inference for the supplied symbols."""
    if not symbols:
        return pl.DataFrame()

    history = _load_history(con, as_of)
    current_rows = _load_current_rows(con, symbols, as_of)
    overall = history.get("_overall", CalibrationStats())
    rows: list[dict[str, Any]] = []

    for sym in symbols:
        current = current_rows.get(sym) or {"symbol": sym}
        gate = current.get("gate") or {}
        if not gate:
            continue
        has_event = bool(current.get("event_count") or current.get("news_count"))
        bucket = _bucket_for(gate=gate, has_event=has_event)
        stats = history.get(bucket)
        stats_source = "bucket"
        if stats is None or stats.n < 8:
            stats = overall
            stats_source = "overall" if overall.n else "prior"

        score = _score_current(current=current, stats=stats)
        calibration = _calibration_payload(stats)
        details = {
            "advice": score["advice"],
            "display_label": {
                "continue": "继续",
                "wait_pullback": "等回落",
                "do_not_chase": "不追",
            }.get(score["advice"], "等回落"),
            "reason_codes": [
                code
                for code, enabled in [
                    ("gate_do_not_chase", gate.get("action") == "do_not_chase"),
                    ("gate_wait_pullback", gate.get("action") == "wait_pullback"),
                    ("stretch_high", score["paid_risk"] >= 0.55),
                    ("continuation_supported", score["continuation_score"] >= 0.55),
                    ("fade_risk_visible", score["fade_score"] >= 0.48),
                    ("event_or_news", has_event),
                ]
                if enabled
            ],
            "bucket": bucket,
            "stats_source": stats_source,
            "continuation_score": round(score["continuation_score"], 4),
            "fade_score": round(score["fade_score"], 4),
            "alpha_already_paid_risk": round(score["paid_risk"], 4),
            "entry_quality": round(score["entry_quality"], 4),
            "gate_action": gate.get("action"),
            "gate_p_continue": gate.get("p_continue"),
            "gate_p_fade": gate.get("p_fade"),
            "gap_pct": gate.get("gap_pct"),
            "gap_vs_expected_move": gate.get("gap_vs_expected_move"),
            "support_score": gate.get("support_score"),
            "effective_stretch_score": gate.get("effective_stretch_score", gate.get("stretch_score")),
            "calibration": calibration,
            "event_count": int(current.get("event_count") or 0),
            "news_count": int(current.get("news_count") or 0),
        }

        rows.append(
            {
                "symbol": sym,
                "date": as_of,
                "module_name": MODULE_NAME,
                "trend_prob": round(score["continuation_score"], 4),
                "p_upside": round(score["entry_quality"], 4),
                "p_downside": round(max(score["fade_score"], score["paid_risk"]), 4),
                "daily_risk_usd": _safe_float(gate.get("stretch_usd")) or _safe_float(gate.get("gap_abs")) or 0.0,
                "expected_move_pct": _safe_float(gate.get("max_chase_gap_pct")),
                "z_score": _safe_float(gate.get("gap_vs_expected_move")),
                "p_value_raw": None,
                "p_value_bonf": None,
                "strength_bucket": (
                    "strong"
                    if score["advice"] == "continue" and score["entry_quality"] >= 0.62
                    else "moderate"
                    if score["advice"] != "do_not_chase"
                    else "weak"
                ),
                "regime": score["regime"],
                "details": json.dumps(details, ensure_ascii=True, sort_keys=True),
            }
        )

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


def store_overnight_continuation_alpha(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
) -> int:
    """Store shadow alpha diagnostics into analysis_daily."""
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
