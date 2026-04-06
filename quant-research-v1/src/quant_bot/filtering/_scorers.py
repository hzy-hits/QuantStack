"""Non-options scoring functions, composite score, and item assembly."""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from ._common import (
    _safe,
    W_MAGNITUDE,
    W_EVENT,
    W_MOMENTUM,
    W_OPTIONS,
    W_CROSS,
    W_LAB,
)


def score_magnitude(
    ret_1d: float | None,
    ac: float,
    atr: float,
    rel_volume: float,
) -> float:
    """Score 1: How big is today's move vs ATR?"""
    if ret_1d is not None and atr > 0:
        move_usd = abs(ret_1d / 100.0 * ac)
        magnitude_ratio = move_usd / atr  # 1.0 = exactly 1 ATR
        # Also boost if unusual volume
        vol_boost = min(rel_volume / 2.0, 1.0)  # caps at 2x avg vol
        magnitude_score = min(magnitude_ratio * (1.0 + vol_boost * 0.3), 3.0) / 3.0
    else:
        magnitude_score = 0.0
    return magnitude_score


def score_event(
    sym: str,
    as_of: date,
    earn_map: dict[str, list],
    filing_map: dict[str, list],
    index_change_map: dict[str, list[dict]],
) -> tuple[float, list]:
    """Score 2: Event proximity. Returns (event_score, events_list)."""
    event_score = 0.0
    events: list[dict] = []

    # Check if 8-K Item 2.02 (earnings results) already filed for this symbol
    has_item_2_02 = False
    if sym in filing_map:
        for filing in filing_map[sym]:
            items = filing.get("items") or []
            if isinstance(items, str):
                items = [items]
            if any("2.02" in str(item) for item in items):
                has_item_2_02 = True
                break

    if sym in earn_map:
        for ev in earn_map[sym]:
            report_date = date.fromisoformat(str(ev["report_date"])[:10])
            days_to_earnings = (report_date - as_of).days

            # Skip "upcoming" earnings if 8-K Item 2.02 already filed
            # (earnings already reported — flagging as upcoming is misleading)
            if has_item_2_02 and days_to_earnings >= 0:
                # Mark as already reported instead
                events.append({
                    "type": "earnings_reported",
                    "days_offset": days_to_earnings,
                    "estimate_eps": _safe(ev.get("estimate_eps")),
                    "actual_eps": _safe(ev.get("actual_eps")),
                    "surprise_pct": _safe(ev.get("surprise_pct")),
                    "note": "8-K Item 2.02 already filed",
                })
                continue

            proximity = 1.0 - abs(days_to_earnings) / 7.0
            event_score = max(event_score, proximity)
            events.append({
                "type": "earnings",
                "days_offset": days_to_earnings,
                "estimate_eps": _safe(ev.get("estimate_eps")),
                "actual_eps": _safe(ev.get("actual_eps")),
                "surprise_pct": _safe(ev.get("surprise_pct")),
            })
    if sym in filing_map:
        event_score = max(event_score, 0.8)  # recent 8-K always notable
        events.append({"type": "8-K_filing", "filings": filing_map[sym]})
    if sym in index_change_map:
        for ic_item in index_change_map[sym]:
            try:
                days_ago = (as_of - date.fromisoformat(ic_item["date"])).days
            except Exception:
                days_ago = 30
            recency = max(0.0, 1.0 - days_ago / 14.0)
            base = 0.90 if ic_item["type"] == "add" else 0.85
            event_score = max(event_score, base * (0.7 + 0.3 * recency))
            events.append({
                "type": f"index_{ic_item['type']}",
                "index": ic_item["index"],
                "date": ic_item["date"],
                "days_ago": days_ago,
            })

    return event_score, events


def score_momentum(mom_row) -> tuple[float, dict]:
    """Score 3: Momentum extremity. Returns (momentum_score, mom_payload)."""
    momentum_score = 0.0
    mom_payload: dict[str, Any] = {}
    if mom_row is not None:
        z = _safe(mom_row.get("z_score"), 0.0)
        momentum_score = min(abs(z) / 3.0, 1.0)  # z=3 -> score=1
        mom_payload = {
            "regime": mom_row.get("regime"),
            "trend_prob": _safe(mom_row.get("trend_prob")),
            "p_upside": _safe(mom_row.get("p_upside")),
            "p_downside": _safe(mom_row.get("p_downside")),
            "z_score": _safe(mom_row.get("z_score")),
            "p_value_bonf": _safe(mom_row.get("p_value_bonf")),
            "strength_bucket": mom_row.get("strength_bucket"),
            "daily_risk_usd": _safe(mom_row.get("daily_risk_usd")),
            "expected_move_pct": _safe(mom_row.get("expected_move_pct")),
        }
        if mom_row.get("details"):
            mom_payload.update(json.loads(mom_row["details"]))
    return momentum_score, mom_payload


def score_cross_asset(ret_1d: float | None, bench_ret_1d: float) -> float:
    """Score 5: Cross-asset -- idiosyncratic vs market-wide. High = NOT explained by market."""
    cross_score = 0.0
    if ret_1d is not None:
        idiosyncratic = abs(ret_1d - bench_ret_1d)
        cross_score = min(idiosyncratic / 5.0, 1.0)  # 5% idio -> score=1
    return cross_score


def composite_score(
    magnitude: float,
    event: float,
    momentum: float,
    options: float,
    cross: float,
    lab: float = 0.0,
) -> float:
    """Weighted composite notability score."""
    return (
        W_MAGNITUDE * magnitude +
        W_EVENT     * event +
        W_MOMENTUM  * momentum +
        W_OPTIONS   * options +
        W_CROSS     * cross +
        W_LAB       * lab
    )


def build_earnings_risk_payload(er) -> dict:
    """Build earnings risk payload from earnings risk row."""
    if er is None:
        return {}
    er_details = json.loads(er["details"]) if er.get("details") else {}
    return {
        "p_upside":          _safe(er.get("p_upside")),
        "p_downside":        _safe(er.get("p_downside")),
        "expected_move_pct": _safe(er.get("expected_move_pct")),
        "strength_bucket":   er.get("strength_bucket"),
        "surprise_quintile": er_details.get("surprise_quintile"),
        "surprise_unknown":  er_details.get("surprise_unknown", False),
        "n_obs":             er_details.get("n_quintile_events"),
        "ci_low":            er_details.get("ci_low"),
        "ci_high":           er_details.get("ci_high"),
        "pre_event_regime":  er_details.get("pre_event_regime"),
    }


def assemble_item(
    sym: str,
    score: float,
    sub_scores: dict[str, float],
    primary_reason: str,
    ac: float,
    ret_1d: float | None,
    ret_5d: float | None,
    ret_21d: float | None,
    pct_from_52w_high: float | None,
    rel_volume: float,
    atr: float,
    mom_payload: dict,
    earn_risk_payload: dict,
    opts_payload: dict,
    events: list,
    news: list,
    filings: list,
    index_changes: list,
    sentiment: dict | None = None,
    price_signals: dict | None = None,
) -> dict:
    """Assemble the final item dict for one symbol."""
    item = {
        "symbol": sym,
        "score": round(score, 4),
        "primary_reason": primary_reason,
        "sub_scores": {k: round(v, 3) for k, v in sub_scores.items()},
        # Price context
        "price": round(ac, 2),
        "ret_1d_pct": round(ret_1d, 2) if ret_1d is not None else None,
        "ret_5d_pct": round(ret_5d, 2) if ret_5d is not None else None,
        "ret_21d_pct": round(ret_21d, 2) if ret_21d is not None else None,
        "pct_from_52w_high": round(pct_from_52w_high, 2) if pct_from_52w_high is not None else None,
        "rel_volume": round(rel_volume, 2),
        "atr": round(atr, 2),
        # Analysis
        "momentum": mom_payload,
        "earnings_risk": earn_risk_payload,
        "options": opts_payload,
        # Sentiment (VRP + EWMA z-scores)
        "sentiment": sentiment if sentiment else {},
        # Price signal layer (cointegration, Granger, CAR, Kalman beta)
        "price_signals": price_signals if price_signals else {},
        # Catalysts
        "events": events,
        # News and filings (latest 3 each)
        "news": news,
        "sec_filings": filings,
        "index_changes": index_changes,
    }
    return item
