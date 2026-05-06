#!/usr/bin/env python3
"""Observed lifecycle probabilities for current A-share candidates.

The goal is deliberately narrower than a new strategy model: given today's
candidate state, find historical rows with similar observable states and
estimate whether the next T+1/T+3 lifecycle is worth a probe.
"""
from __future__ import annotations

import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb


LCB80_Z = 1.2816
OBSERVED_LIFECYCLE_SLEEVE = "cn_observed_lifecycle_prob"


@dataclass(frozen=True)
class ObservedLifecycleConfig:
    min_bucket_n: int = 24
    min_t3_n: int = 12
    prior_n: int = 8
    hit_r_multiple: float = 1.0
    stop_r_multiple: float = 1.0
    strong_lcb_r: float = 0.0
    micro_expected_r: float = 0.12
    micro_hit_stop_spread: float = 0.06
    max_probe_stop_prob: float = 0.55


DEFAULT_CONFIG = ObservedLifecycleConfig()

BUCKET_LEVELS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "exact_state",
        (
            "execution_mode_bucket",
            "rsi_bucket",
            "ret20_bucket",
            "ret5_bucket",
            "fade_bucket",
            "setup_bucket",
            "flow_bucket",
            "market_vol_bucket",
        ),
    ),
    (
        "core_price_flow",
        ("execution_mode_bucket", "rsi_bucket", "ret20_bucket", "fade_bucket", "setup_bucket", "flow_bucket"),
    ),
    ("price_flow", ("rsi_bucket", "ret20_bucket", "ret5_bucket", "flow_bucket")),
    ("risk_flow", ("rsi_bucket", "ret20_bucket", "fade_bucket", "stale_bucket", "flow_bucket")),
    ("baseline_oversold", ("strategy_family",)),
)


def parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def as_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value)
    return text[:10] if text else None


def round_or_none(value: Any, digits: int = 6) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def safe_json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def nested_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def first_number(*values: Any) -> float | None:
    for value in values:
        parsed = round_or_none(value)
        if parsed is not None:
            return parsed
    return None


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0])


def rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]


def feature_bundle(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    features = safe_json_loads(row.get("features_json"))
    detail = safe_json_loads(row.get("detail_json"))
    nested = features.get("details") if isinstance(features.get("details"), dict) else {}
    return features, detail, nested


def feature_value(row: dict[str, Any], key: str) -> Any:
    features, detail, nested = feature_bundle(row)
    if key in row and row.get(key) is not None:
        return row.get(key)
    if key in features:
        return features.get(key)
    if key in detail:
        return detail.get(key)
    return nested.get(key)


def bool_bucket(value: Any, true_label: str, false_label: str) -> str:
    text = str(value).lower()
    if text in {"true", "1", "yes"}:
        return true_label
    if text in {"false", "0", "no"}:
        return false_label
    return "unknown"


def numeric_bucket(value: float | None, cuts: tuple[tuple[float, str], ...], default: str) -> str:
    if value is None:
        return "unknown"
    for upper, label in cuts:
        if value <= upper:
            return label
    return default


def risk_bucket(value: float | None, *, low: float, high: float) -> str:
    if value is None:
        return "unknown"
    if value < low:
        return "low"
    if value < high:
        return "mid"
    return "high"


def hold_days(row: dict[str, Any]) -> int | None:
    fill = as_iso(row.get("fill_date"))
    exit_ = as_iso(row.get("exit_date"))
    if not fill or not exit_:
        return None
    try:
        return max(0, (parse_date(exit_) - parse_date(fill)).days)
    except ValueError:
        return None


def hold_bucket(days: int | None) -> str:
    if days is None:
        return "pending"
    if days <= 1:
        return "T+1"
    if days == 2:
        return "T+2"
    if days == 3:
        return "T+3"
    if days <= 5:
        return "T+4-T+5"
    return ">T+5"


def observation_state(row: dict[str, Any]) -> dict[str, Any]:
    features, detail, nested = feature_bundle(row)
    ret20 = first_number(
        row.get("log_return_20d_pct"),
        features.get("ret_20d"),
        detail.get("ret_20d"),
        nested.get("ret_20d"),
    )
    ret5 = first_number(features.get("ret_5d"), detail.get("ret_5d"), nested.get("ret_5d"))
    rsi = first_number(row.get("rsi_14"), features.get("rsi_14"), detail.get("rsi_14"), nested.get("rsi_14"))
    setup = first_number(features.get("setup_score"), detail.get("setup_score"), nested.get("setup_score"))
    fade = first_number(features.get("fade_risk"), detail.get("fade_risk"), nested.get("fade_risk"))
    stale = first_number(features.get("stale_chase_risk"), detail.get("stale_chase_risk"), nested.get("stale_chase_risk"))
    market_high_vol = first_number(
        features.get("market_p_high_vol"),
        detail.get("market_p_high_vol"),
        nested_get(nested, "market_vol", "p_high_vol"),
    )
    flow_conflict = (
        row.get("flow_conflict_flag")
        if row.get("flow_conflict_flag") is not None
        else features.get("flow_conflict_flag", detail.get("flow_conflict_flag"))
    )
    execution_mode = str(
        row.get("execution_mode")
        or features.get("execution_mode")
        or detail.get("execution_mode")
        or nested.get("execution_mode")
        or "unknown"
    )
    state = {
        "strategy_family": str(row.get("strategy_family") or row.get("policy") or "oversold_contrarian"),
        "execution_mode_bucket": execution_mode,
        "rsi_bucket": str(features.get("rsi_bucket") or detail.get("rsi_bucket") or numeric_bucket(
            rsi,
            ((25.0, "rsi_deep"), (35.0, "rsi_oversold"), (45.0, "rsi_low")),
            "rsi_neutral",
        )),
        "ret20_bucket": numeric_bucket(
            ret20,
            ((-20.0, "deep_drawdown_20d"), (-10.0, "pullback_20d"), (-5.0, "shallow_pullback")),
            "no_20d_pullback",
        ),
        "ret5_bucket": numeric_bucket(
            ret5,
            ((-12.0, "fast_crash_5d"), (-5.0, "pullback_5d"), (5.0, "flat_5d")),
            "bounce_5d",
        ),
        "fade_bucket": risk_bucket(fade, low=0.35, high=0.70),
        "stale_bucket": risk_bucket(stale, low=0.35, high=0.65),
        "setup_bucket": numeric_bucket(setup, ((0.40, "setup_weak"), (0.65, "setup_mixed")), "setup_strong"),
        "flow_bucket": bool_bucket(flow_conflict, "flow_conflict", "flow_clean"),
        "market_vol_bucket": numeric_bucket(
            market_high_vol,
            ((0.45, "market_low_vol"), (0.85, "market_mid_vol")),
            "market_high_vol",
        ),
        "rsi_14": rsi,
        "ret_20d_pct": ret20,
        "ret_5d_pct": ret5,
        "setup_score": setup,
        "fade_risk": fade,
        "stale_chase_risk": stale,
        "market_p_high_vol": market_high_vol,
    }
    return state


def key_for(state: dict[str, Any], fields: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(state.get(field) or "unknown" for field in fields)


def load_historical_rows(db_path: Path, start: date, as_of: date) -> tuple[list[dict[str, Any]], str]:
    if not db_path.exists():
        return [], "missing_cn_db"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "strategy_model_dataset"):
            return [], "missing_strategy_model_dataset"
        rows = rows_as_dicts(
            con,
            """
            SELECT
                report_date, evaluation_date, symbol, selection_status,
                strategy_family, strategy_key, action_intent, alpha_state,
                features_json, detail_json, fill_status, fill_date, fill_price,
                exit_date, exit_price, realized_ret_pct, max_favorable_pct,
                max_adverse_pct, risk_unit_pct, ev_pct, ev_lcb_80_pct,
                ev_norm_score, ev_norm_lcb_80
            FROM strategy_model_dataset
            WHERE report_date >= CAST(? AS DATE)
              AND report_date < CAST(? AS DATE)
              AND strategy_family = 'oversold_contrarian'
              AND action_intent = 'TRADE'
              AND realized_ret_pct IS NOT NULL
            ORDER BY report_date, symbol
            """,
            [start.isoformat(), as_of.isoformat()],
        )
    finally:
        con.close()

    best: dict[tuple[str, str], tuple[tuple[float, float, float], dict[str, Any]]] = {}
    for row in rows:
        report_date = as_iso(row.get("report_date")) or ""
        symbol = str(row.get("symbol") or "")
        if not report_date or not symbol:
            continue
        selection_score = 1.0 if str(row.get("selection_status") or "") == "selected" else 0.0
        lcb = round_or_none(row.get("ev_norm_lcb_80")) or -999.0
        ev = round_or_none(row.get("ev_norm_score")) or -999.0
        score = (selection_score, lcb, ev)
        key = (report_date, symbol)
        existing = best.get(key)
        if existing is None or score > existing[0]:
            best[key] = (score, row)
    out = [row for _, row in best.values()]
    return out, "ok"


def row_return_r(row: dict[str, Any]) -> float | None:
    ret = round_or_none(row.get("realized_ret_pct"))
    risk = round_or_none(row.get("risk_unit_pct"))
    if ret is None:
        return None
    if risk is None or risk <= 0:
        risk = 1.0
    return ret / risk


def row_hit_1r(row: dict[str, Any], config: ObservedLifecycleConfig) -> bool | None:
    mfe = round_or_none(row.get("max_favorable_pct"))
    risk = round_or_none(row.get("risk_unit_pct"))
    if mfe is None:
        return None
    if risk is None or risk <= 0:
        risk = 1.0
    return mfe >= config.hit_r_multiple * risk


def row_stop_1r(row: dict[str, Any], config: ObservedLifecycleConfig) -> bool | None:
    mae = round_or_none(row.get("max_adverse_pct"))
    risk = round_or_none(row.get("risk_unit_pct"))
    if mae is None:
        return None
    if risk is None or risk <= 0:
        risk = 1.0
    return mae <= -config.stop_r_multiple * risk


def shrink_prob(successes: int, n: int, *, prior_p: float, prior_n: int) -> float | None:
    if n <= 0:
        return None
    return (successes + prior_p * prior_n) / (n + prior_n)


def mean_lcb(values: list[float]) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean, mean, None
    std = statistics.stdev(values)
    lcb = mean - LCB80_Z * std / math.sqrt(len(values))
    return mean, lcb, std


def best_hold_bucket(rows: list[dict[str, Any]]) -> tuple[str | None, int | None]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        ret_r = row_return_r(row)
        if ret_r is None:
            continue
        grouped.setdefault(hold_bucket(hold_days(row)), []).append(ret_r)
    order_days = {"T+1": 1, "T+2": 2, "T+3": 3, "T+4-T+5": 5, ">T+5": 5, "pending": None}
    best: tuple[float, str] | None = None
    for bucket, values in grouped.items():
        if bucket == "pending" or not values:
            continue
        _, lcb, _ = mean_lcb(values)
        score = -999.0 if lcb is None else lcb
        if best is None or score > best[0]:
            best = (score, bucket)
    if best is None:
        return None, None
    return best[1], order_days.get(best[1])


def compute_metrics(
    rows: list[dict[str, Any]],
    *,
    baseline_win: float,
    baseline_hit: float,
    baseline_stop: float,
    config: ObservedLifecycleConfig,
) -> dict[str, Any]:
    sample_n = len(rows)
    t1_rows = [row for row in rows if (hold_days(row) or 99) <= 1]
    t3_rows = [row for row in rows if (hold_days(row) or 99) <= 3]
    t3_metric_rows = t3_rows if len(t3_rows) >= config.min_t3_n else rows
    ret_r = [ret for row in t3_metric_rows if (ret := row_return_r(row)) is not None]
    mean_r, lcb_r, std_r = mean_lcb(ret_r)
    t1_returns = [ret for row in t1_rows if (ret := row_return_r(row)) is not None]
    p_win_t1 = shrink_prob(
        sum(1 for value in t1_returns if value > 0.0),
        len(t1_returns),
        prior_p=baseline_win,
        prior_n=config.prior_n,
    )
    hit_values = [hit for row in t3_metric_rows if (hit := row_hit_1r(row, config)) is not None]
    stop_values = [stop for row in t3_metric_rows if (stop := row_stop_1r(row, config)) is not None]
    p_hit = shrink_prob(sum(1 for value in hit_values if value), len(hit_values), prior_p=baseline_hit, prior_n=config.prior_n)
    p_stop = shrink_prob(sum(1 for value in stop_values if value), len(stop_values), prior_p=baseline_stop, prior_n=config.prior_n)
    best_bucket, suggested_hold = best_hold_bucket(rows)
    score = 0.0
    if mean_r is not None:
        score += max(-1.0, min(1.0, mean_r)) * 45.0
    if lcb_r is not None:
        score += max(-1.0, min(1.0, lcb_r)) * 35.0
    if p_hit is not None and p_stop is not None:
        score += (p_hit - p_stop) * 30.0
    if p_win_t1 is not None:
        score += (p_win_t1 - 0.5) * 20.0
    return {
        "observed_probability_n": sample_n,
        "observed_probability_t1_n": len(t1_returns),
        "observed_probability_t3_n": len(ret_r),
        "p_win_t1": round_or_none(p_win_t1, 6),
        "p_hit_1r_t3": round_or_none(p_hit, 6),
        "p_stop_t3": round_or_none(p_stop, 6),
        "expected_r_t3": round_or_none(mean_r, 6),
        "lcb80_r_t3": round_or_none(lcb_r, 6),
        "std_r_t3": round_or_none(std_r, 6),
        "observed_lifecycle_score": round_or_none(score, 4),
        "best_observed_hold_bucket": best_bucket,
        "suggested_hold_days": suggested_hold,
        "t3_metric_source": "t3_subset" if len(t3_rows) >= config.min_t3_n else "all_matching_rows",
    }


def observed_tier(metrics: dict[str, Any], config: ObservedLifecycleConfig) -> tuple[str, bool, str]:
    expected = round_or_none(metrics.get("expected_r_t3"))
    lcb = round_or_none(metrics.get("lcb80_r_t3"))
    hit = round_or_none(metrics.get("p_hit_1r_t3"))
    stop = round_or_none(metrics.get("p_stop_t3"))
    n = int(metrics.get("observed_probability_n") or 0)
    if expected is None or hit is None or stop is None or n <= 0:
        return "observed_no_data", False, "missing observed analogs"
    if lcb is not None and lcb > config.strong_lcb_r and stop <= config.max_probe_stop_prob:
        return "observed_probe", True, "positive observed T+3 LCB"
    if expected >= config.micro_expected_r and (hit - stop) >= config.micro_hit_stop_spread:
        return "observed_micro_probe", True, "positive observed expected R with hit/stop spread"
    if expected > 0.0:
        return "observed_watch_positive_mean", False, "positive mean but weak LCB/spread"
    return "observed_watch_negative_mean", False, "negative observed expected R"


def build_groups(history: list[dict[str, Any]]) -> tuple[dict[str, dict[tuple[Any, ...], list[dict[str, Any]]]], list[dict[str, Any]]]:
    enriched: list[dict[str, Any]] = []
    groups: dict[str, dict[tuple[Any, ...], list[dict[str, Any]]]] = {name: {} for name, _ in BUCKET_LEVELS}
    for row in history:
        state = observation_state(row)
        copied = {**row, "observed_state": state}
        enriched.append(copied)
        for name, fields in BUCKET_LEVELS:
            groups[name].setdefault(key_for(state, fields), []).append(copied)
    return groups, enriched


def baseline_priors(history: list[dict[str, Any]], config: ObservedLifecycleConfig) -> tuple[float, float, float]:
    if not history:
        return 0.5, 0.45, 0.45
    returns = [ret for row in history if (ret := row_return_r(row)) is not None]
    win = sum(1 for ret in returns if ret > 0.0) / len(returns) if returns else 0.5
    hit_values = [hit for row in history if (hit := row_hit_1r(row, config)) is not None]
    stop_values = [stop for row in history if (stop := row_stop_1r(row, config)) is not None]
    hit = sum(1 for value in hit_values if value) / len(hit_values) if hit_values else 0.45
    stop = sum(1 for value in stop_values if value) / len(stop_values) if stop_values else 0.45
    return win, hit, stop


def match_row(
    row: dict[str, Any],
    groups: dict[str, dict[tuple[Any, ...], list[dict[str, Any]]]],
    *,
    baseline_win: float,
    baseline_hit: float,
    baseline_stop: float,
    config: ObservedLifecycleConfig,
) -> dict[str, Any]:
    state = observation_state(row)
    selected_name = "baseline_oversold"
    selected_fields = ("strategy_family",)
    selected_key = key_for(state, selected_fields)
    selected_rows: list[dict[str, Any]] = []
    for name, fields in BUCKET_LEVELS:
        key = key_for(state, fields)
        candidates = groups.get(name, {}).get(key) or []
        if len(candidates) >= config.min_bucket_n or name == "baseline_oversold":
            selected_name = name
            selected_fields = fields
            selected_key = key
            selected_rows = candidates
            break
    metrics = compute_metrics(
        selected_rows,
        baseline_win=baseline_win,
        baseline_hit=baseline_hit,
        baseline_stop=baseline_stop,
        config=config,
    )
    tier, qualified, reason = observed_tier(metrics, config)
    if selected_name == "baseline_oversold":
        tier = "observed_baseline_watch"
        qualified = False
        reason = "baseline oversold fallback is diagnostic only"
    out = {
        **metrics,
        "observed_probability_source": selected_name,
        "observed_probability_bucket": "|".join(str(item) for item in selected_key),
        "observed_probability_fields": list(selected_fields),
        "observed_state": state,
        "observed_lifecycle_tier": tier,
        "observed_lifecycle_qualified": qualified,
        "observed_lifecycle_reason": reason,
    }
    if qualified:
        out["observed_lifecycle_sleeve_id"] = OBSERVED_LIFECYCLE_SLEEVE
    return out


def build_probability_payload(
    *,
    db_path: Path,
    start: date,
    as_of: date,
    current_rows: list[dict[str, Any]],
    config: ObservedLifecycleConfig = DEFAULT_CONFIG,
) -> dict[str, Any]:
    history, status = load_historical_rows(db_path, start, as_of)
    groups, enriched_history = build_groups(history)
    baseline_win, baseline_hit, baseline_stop = baseline_priors(enriched_history, config)
    rows: list[dict[str, Any]] = []
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in current_rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        metrics = match_row(
            row,
            groups,
            baseline_win=baseline_win,
            baseline_hit=baseline_hit,
            baseline_stop=baseline_stop,
            config=config,
        )
        public = {
            "symbol": symbol,
            "name": row.get("name") or "",
            "strategy_family": row.get("strategy_family") or row.get("policy"),
            "alpha_state": row.get("alpha_state"),
            **metrics,
        }
        rows.append(public)
        by_symbol[symbol] = metrics
    qualified = [row for row in rows if row.get("observed_lifecycle_qualified")]
    return {
        "as_of": as_of.isoformat(),
        "start": start.isoformat(),
        "status": status,
        "historical_n": len(history),
        "current_n": len(current_rows),
        "qualified_n": len(qualified),
        "config": asdict(config),
        "baseline_priors": {
            "p_win": round_or_none(baseline_win, 6),
            "p_hit_1r": round_or_none(baseline_hit, 6),
            "p_stop_1r": round_or_none(baseline_stop, 6),
        },
        "rows": rows,
        "by_symbol": by_symbol,
    }
