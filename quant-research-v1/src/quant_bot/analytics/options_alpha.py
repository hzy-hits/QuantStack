"""Daily options alpha layer.

This module turns raw options context into a small set of expression-aware
signals.  It is deliberately separate from the equity gate: options can confirm
or reshape an idea, but they must build their own history before they can be
treated as execution alpha.
"""
from __future__ import annotations

import json
import math
from datetime import date
from typing import Any

import duckdb
import structlog

from quant_bot.config.strategy_params import get_us_strategy_param_section, load_us_strategy_params

log = structlog.get_logger()


DDL = """
CREATE TABLE IF NOT EXISTS options_alpha (
    symbol            VARCHAR NOT NULL,
    as_of             DATE NOT NULL,
    directional_edge  DOUBLE,
    vol_edge          DOUBLE,
    vrp_edge          DOUBLE,
    flow_edge         DOUBLE,
    liquidity_gate    VARCHAR,
    expression        VARCHAR,
    reason            VARCHAR,
    detail_json       VARCHAR,
    computed_at       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (symbol, as_of)
);
"""


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return default
    return fval if math.isfinite(fval) else default


def _parse_unusual(raw: Any) -> list[dict[str, Any]]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    try:
        parsed = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return []
    return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []


def _flow_direction(unusual: list[dict[str, Any]], params: dict[str, Any]) -> tuple[float, float]:
    call_vol = 0.0
    put_vol = 0.0
    max_ratio = 0.0
    for item in unusual:
        volume = max(_finite(item.get("volume")), 0.0)
        max_ratio = max(max_ratio, _finite(item.get("vol_oi_ratio")))
        typ = str(item.get("type") or item.get("call_put") or "").lower()
        if typ.startswith("c"):
            call_vol += volume
        elif typ.startswith("p"):
            put_vol += volume
    total = call_vol + put_vol
    if total <= 0:
        return 0.0, 0.0
    signed = (call_vol - put_vol) / total
    volume_norm = _finite(params.get("flow_volume_norm"), 50_000.0)
    ratio_norm = _finite(params.get("flow_vol_oi_norm"), 50.0)
    volume_score = min(math.log10(max(total, 1.0)) / math.log10(max(volume_norm, 10.0)), 1.0)
    ratio_score = min(max_ratio / max(ratio_norm, 1.0), 1.0) if max_ratio > 0 else 0.0
    volume_weight = _finite(params.get("flow_volume_weight"), 0.70)
    ratio_weight = _finite(params.get("flow_ratio_weight"), 0.30)
    strength = volume_weight * volume_score + ratio_weight * ratio_score
    return _clamp(signed), _clamp(strength, 0.0, 1.0)


def _liquidity_gate(row: dict[str, Any], params: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    quality = str(row.get("liquidity_score") or "unknown").lower()
    spread = _finite(row.get("avg_spread_pct"), 999.0)
    width = _finite(row.get("chain_width"), 0.0)
    days = _finite(row.get("days_to_exp"), 0.0)

    if quality == "poor":
        reasons.append("poor liquidity")
    if spread > _finite(params.get("max_spread_pct"), 25.0):
        reasons.append("wide spread")
    if width < _finite(params.get("min_chain_width"), 6.0):
        reasons.append("thin chain")
    if days < _finite(params.get("min_days_to_exp"), 3.0):
        reasons.append("expiry too near")

    return ("pass" if not reasons else "fail"), reasons


def _directional_edge(row: dict[str, Any], flow_edge: float, params: dict[str, Any]) -> float:
    bias = str(row.get("bias_signal") or "neutral").lower()
    bias_score = 0.0
    if bias == "bullish":
        bias_score = 1.0
    elif bias == "bearish":
        bias_score = -1.0

    pc_z = _finite(row.get("pc_ratio_z"))
    skew_z = _finite(row.get("skew_z"))
    pc_component = -math.tanh(pc_z / _finite(params.get("pc_z_scale"), 2.5)) if pc_z else 0.0
    skew_component = -math.tanh(skew_z / _finite(params.get("skew_z_scale"), 2.5)) if skew_z else 0.0
    weights = params.get("direction_weights") if isinstance(params.get("direction_weights"), dict) else {}

    return _clamp(
        _finite(weights.get("bias"), 0.35) * bias_score
        + _finite(weights.get("pc"), 0.25) * pc_component
        + _finite(weights.get("skew"), 0.25) * skew_component
        + _finite(weights.get("flow"), 0.15) * flow_edge
    )


def _vrp_edge(row: dict[str, Any], params: dict[str, Any]) -> float:
    vrp_z = row.get("vrp_z")
    if vrp_z is not None:
        return _clamp(-math.tanh(_finite(vrp_z) / _finite(params.get("vrp_z_scale"), 2.5)))
    return _clamp(-math.tanh(_finite(row.get("vrp")) / _finite(params.get("vrp_raw_scale"), 0.20)))


def _vol_edge(row: dict[str, Any], vrp_edge: float, flow_strength: float, params: dict[str, Any]) -> float:
    iv = _finite(row.get("iv_ann"))
    rv = _finite(row.get("rv_ann"))
    cheapness = 0.0
    if iv > 0 and rv > 0:
        cheapness = _clamp(math.tanh((rv / iv - 1.0) / _finite(params.get("cheapness_scale"), 0.35)))
    weights = params.get("vol_weights") if isinstance(params.get("vol_weights"), dict) else {}
    return _clamp(
        _finite(weights.get("vrp"), 0.65) * vrp_edge
        + _finite(weights.get("cheapness"), 0.25) * cheapness
        + _finite(weights.get("flow"), 0.10) * flow_strength
    )


def _expression(
    directional_edge: float,
    vol_edge: float,
    liquidity_gate: str,
    liquidity_reasons: list[str],
    row: dict[str, Any],
    params: dict[str, Any],
) -> tuple[str, str]:
    if liquidity_gate != "pass":
        return "blocked", "; ".join(liquidity_reasons)

    atm_iv = _finite(row.get("atm_iv")) * (100.0 if _finite(row.get("atm_iv")) <= 3.0 else 1.0)
    if atm_iv < _finite(params.get("min_atm_iv_pct"), 5.0):
        return "blocked", "ATM IV implausibly low"

    directional_threshold = _finite(params.get("directional_edge_threshold"), 0.45)
    vol_threshold = _finite(params.get("vol_edge_threshold"), 0.10)
    vol_wait_threshold = _finite(params.get("vol_edge_wait_threshold"), 0.55)

    if directional_edge >= directional_threshold:
        if vol_edge >= vol_threshold:
            return "call_spread", "bullish direction with acceptable/cheap convexity"
        return "stock_long", "bullish direction but listed options appear overpaid"
    if directional_edge <= -directional_threshold:
        if vol_edge >= vol_threshold:
            return "put_spread", "bearish direction with acceptable/cheap convexity"
        return "wait", "bearish direction but listed puts appear overpaid"
    if vol_edge >= vol_wait_threshold:
        return "wait", "volatility edge exists but direction is not clean"
    return "blocked", "no clean options edge"


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def compute_options_alpha(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str],
    as_of: date,
) -> list[dict[str, Any]]:
    """Compute expression-aware options alpha rows for direct option chains."""
    ensure_schema(con)
    if not symbols:
        return []
    strategy_params = load_us_strategy_params()
    params = get_us_strategy_param_section("options_alpha")
    as_of_str = as_of.isoformat()
    symbol_list = sorted(set(symbols))

    placeholders = ",".join("?" for _ in symbol_list)
    sql_params: list[Any] = [
        as_of_str,
        *symbol_list,
        _finite(params.get("min_days_to_exp"), 3.0),
        _finite(params.get("max_days_to_exp"), 120.0),
        as_of_str,
    ]
    sql = f"""
        WITH ranked_analysis AS (
            SELECT
                oa.*,
                ROW_NUMBER() OVER (
                    PARTITION BY oa.symbol
                    ORDER BY oa.days_to_exp ASC
                ) AS rn
            FROM options_analysis oa
            WHERE oa.as_of = CAST(? AS DATE)
              AND oa.symbol IN ({placeholders})
              AND oa.days_to_exp BETWEEN ? AND ?
              AND (
                    TRY_CAST(oa.expiry AS DATE) IS NULL
                 OR TRY_CAST(oa.expiry AS DATE) >= CAST(? AS DATE)
              )
        )
        SELECT
            oa.symbol,
            oa.expiry,
            oa.days_to_exp,
            oa.current_price,
            oa.atm_iv,
            oa.iv_skew,
            oa.put_call_vol_ratio,
            oa.bias_signal,
            oa.liquidity_score,
            oa.chain_width,
            oa.avg_spread_pct,
            oa.unusual_strikes,
            os.pc_ratio_z,
            os.skew_z,
            os.vrp,
            os.iv_ann,
            os.rv_ann,
            os.vrp_z
        FROM ranked_analysis oa
        LEFT JOIN options_sentiment os
          ON os.symbol = oa.symbol AND os.as_of = oa.as_of
        WHERE oa.rn = 1
    """
    try:
        df = con.execute(sql, sql_params).fetchdf()
    except Exception as exc:
        log.warning("options_alpha_query_failed", error=str(exc))
        return []
    if df.empty:
        return []

    rows: list[dict[str, Any]] = []
    for record in df.to_dict("records"):
        unusual = _parse_unusual(record.get("unusual_strikes"))
        flow_dir, flow_strength = _flow_direction(unusual, params)
        flow_edge = _clamp(flow_dir * flow_strength)
        directional = _directional_edge(record, flow_edge, params)
        vrp = _vrp_edge(record, params)
        vol = _vol_edge(record, vrp, flow_strength, params)
        liquidity, liquidity_reasons = _liquidity_gate(record, params)
        expression, reason = _expression(directional, vol, liquidity, liquidity_reasons, record, params)
        detail = {
            "expiry": str(record.get("expiry")),
            "days_to_exp": record.get("days_to_exp"),
            "current_price": record.get("current_price"),
            "atm_iv": record.get("atm_iv"),
            "iv_skew": record.get("iv_skew"),
            "put_call_vol_ratio": record.get("put_call_vol_ratio"),
            "bias_signal": record.get("bias_signal"),
            "liquidity_score": record.get("liquidity_score"),
            "chain_width": record.get("chain_width"),
            "avg_spread_pct": record.get("avg_spread_pct"),
            "pc_ratio_z": record.get("pc_ratio_z"),
            "skew_z": record.get("skew_z"),
            "vrp": record.get("vrp"),
            "iv_ann": record.get("iv_ann"),
            "rv_ann": record.get("rv_ann"),
            "flow_strength": round(flow_strength, 4),
            "unusual_count": len(unusual),
            "role": "options_alpha_direct_binding",
            "param_source": strategy_params.get("_source", "built_in_default"),
            "param_provenance": params.get("provenance", "legacy_heuristic"),
        }
        rows.append(
            {
                "symbol": str(record["symbol"]),
                "as_of": as_of_str,
                "directional_edge": round(directional, 4),
                "vol_edge": round(vol, 4),
                "vrp_edge": round(vrp, 4),
                "flow_edge": round(flow_edge, 4),
                "liquidity_gate": liquidity,
                "expression": expression,
                "reason": reason,
                "detail_json": json.dumps(detail, ensure_ascii=False, default=str),
            }
        )
    return rows


def store_options_alpha(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    as_of: date,
) -> int:
    ensure_schema(con)
    as_of_str = as_of.isoformat()
    con.execute("DELETE FROM options_alpha WHERE as_of = CAST(? AS DATE)", [as_of_str])
    if not rows:
        con.commit()
        return 0
    for row in rows:
        con.execute(
            """
            INSERT OR REPLACE INTO options_alpha
                (symbol, as_of, directional_edge, vol_edge, vrp_edge, flow_edge,
                 liquidity_gate, expression, reason, detail_json)
            VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["symbol"],
                row["as_of"],
                row["directional_edge"],
                row["vol_edge"],
                row["vrp_edge"],
                row["flow_edge"],
                row["liquidity_gate"],
                row["expression"],
                row["reason"],
                row["detail_json"],
            ],
        )
    con.commit()
    log.info("options_alpha_stored", rows=len(rows))
    return len(rows)
