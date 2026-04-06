"""
Notable items filter — the most important module.

Takes the full universe (~550 symbols) and returns up to 50 items
ranked by a composite notability score.

Philosophy:
  - We are NOT picking stocks or making strategy decisions
  - We are identifying what is WORTH ATTENTION today
  - Claude then reasons about WHY it is notable using news + filings + macro

Scoring dimensions (each 0-1, then weighted composite):
  1. magnitude_score   — how large is today's move vs ATR baseline?
  2. event_score       — is there a catalyst (earnings, 8-K, FOMC)?
  3. momentum_score    — is there an extreme trend (z-score magnitude)?
  4. cross_asset_score — is this part of a broad market move or idiosyncratic?
  5. options_score     — IV level + 7D IV delta + flow intensity (with novelty)

Output: list of up to MAX_ITEMS dicts, each containing all data
the agent needs to write about that item.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import duckdb
import structlog

from ._common import MAX_ITEMS, _safe
from ._price_loader import load_price_context, load_atr, get_benchmark_return
from ._events_loader import (
    load_momentum_analysis,
    load_mean_reversion,
    load_breakout,
    load_lab_factor,
    load_earnings_risk,
    load_earnings_events,
    load_filings,
    load_index_changes,
    load_news,
)
from ._options_loader import (
    load_options_current,
    load_options_history,
    apply_proxy_mapping,
)
from ._options_scorer import score_options
from ._scorers import (
    score_magnitude,
    score_event,
    score_momentum,
    score_cross_asset,
    composite_score,
    build_earnings_risk_payload,
    assemble_item,
)
from ._ranking import rank_and_select, annotate_shared_options_groups
from ._sentiment_loader import load_sentiment
from ._price_signals_loader import (
    load_cointegration,
    load_granger,
    load_earnings_car,
    load_kalman_betas,
)

log = structlog.get_logger()


def _load_company_profiles(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    """Load the latest available company profile per symbol."""
    if not symbols:
        return {}

    rows = con.execute(f"""
        WITH latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of DESC) AS rn
            FROM company_profile
            WHERE symbol IN ({",".join("?" * len(symbols))})
              AND as_of <= ?
        )
        SELECT symbol, company_name, sector, industry, market_cap
        FROM latest
        WHERE rn = 1
    """, symbols + [as_of.strftime("%Y-%m-%d")]).fetchdf()

    if rows.empty:
        return {}

    return {
        r["symbol"]: {
            "company_name": r.get("company_name"),
            "sector": r.get("sector"),
            "industry": r.get("industry"),
            "market_cap": _safe(r.get("market_cap")),
        }
        for _, r in rows.iterrows()
    }


def _liquidity_factor(opts_payload: dict) -> float:
    liq = (opts_payload or {}).get("liquidity_score")
    if liq == "good":
        return 1.0
    if liq == "fair":
        return 0.8
    if liq == "poor":
        return 0.35
    return 0.55


def _selection_metadata(
    item: dict[str, Any],
    *,
    core_symbols: set[str],
    selection_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    """Compute report-lane assignment and tradability penalty."""
    policy = selection_policy or {}
    min_market_cap = float(policy.get("core_min_market_cap_musd", 2_000.0))
    min_price = float(policy.get("core_min_price", 5.0))
    min_dollar_volume = float(policy.get("core_min_dollar_volume_20d", 20_000_000.0))

    price = float(item.get("price") or 0.0)
    avg_dollar_volume = float(item.get("avg_dollar_volume_20d") or 0.0)
    market_cap = item.get("fundamentals", {}).get("market_cap_musd")
    has_market_cap = market_cap is not None
    options = item.get("options") or {}
    options_liquidity = options.get("liquidity_score")
    has_liquid_options = options_liquidity in {"good", "fair"}
    has_lab_factor = bool(item.get("lab_factor"))
    named_core = item["symbol"] in core_symbols

    price_factor = min(max(price / max(min_price, 1.0), 0.15), 1.0)
    dollar_volume_factor = min(max(avg_dollar_volume / max(min_dollar_volume, 1.0), 0.10), 1.0)
    if has_market_cap:
        market_cap_factor = min(max(float(market_cap) / max(min_market_cap, 1.0), 0.10), 1.0)
    else:
        market_cap_factor = 1.0
    options_factor = _liquidity_factor(options)
    confirmation_factor = 1.0 if (has_liquid_options or has_lab_factor) else 0.65

    tradability_score = round(
        0.20 * price_factor
        + 0.35 * dollar_volume_factor
        + 0.25 * market_cap_factor
        + 0.10 * options_factor
        + 0.10 * confirmation_factor,
        3,
    )

    passes_core_floor = (
        price >= min_price
        and avg_dollar_volume >= min_dollar_volume
        and (not has_market_cap or float(market_cap) >= min_market_cap)
    )

    if passes_core_floor and (named_core or has_liquid_options or has_lab_factor or (has_market_cap and float(market_cap) >= min_market_cap * 2.5)):
        lane = "core"
        lane_reason = "tradable core-book candidate"
    elif item["score"] >= 0.45 or item["sub_scores"].get("event", 0.0) >= 0.65 or item["sub_scores"].get("magnitude", 0.0) >= 0.75:
        lane = "event_tape"
        lane_reason = "anomaly/event tape candidate"
    else:
        lane = "appendix"
        lane_reason = "low-priority radar candidate"

    report_score = round(item["score"] * (0.35 + 0.65 * tradability_score), 4)

    penalties = []
    if price < min_price:
        penalties.append("low_price")
    if avg_dollar_volume < min_dollar_volume:
        penalties.append("low_dollar_volume")
    if has_market_cap and float(market_cap) < min_market_cap:
        penalties.append("small_cap")
    if options_liquidity == "poor":
        penalties.append("poor_options")
    if not has_liquid_options and not has_lab_factor:
        penalties.append("weak_secondary_confirmation")

    return {
        "lane": lane,
        "lane_reason": lane_reason,
        "tradability_score": tradability_score,
        "report_score": report_score,
        "penalties": penalties,
        "named_core": named_core,
        "has_liquid_options": has_liquid_options,
        "has_lab_factor": has_lab_factor,
    }


# ── Main entry point ────────────────────────────────────────────────────────

def build_notable_items(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    symbols: list[str],
    benchmark: str = "SPY",
    max_items: int = MAX_ITEMS,
    core_symbols: set[str] | None = None,
    selection_policy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Main entry point. Returns ranked list of notable items for the payload.
    """
    # 7-day history window for IV delta
    hist_target_str = (as_of - timedelta(days=7)).strftime("%Y-%m-%d")
    hist_lo_str = (as_of - timedelta(days=10)).strftime("%Y-%m-%d")
    hist_hi_str = (as_of - timedelta(days=5)).strftime("%Y-%m-%d")

    # ── Load all data ──────────────────────────────────────────────────────
    price_ctx = load_price_context(con, symbols, as_of)
    if price_ctx.empty:
        log.warning("notable_no_price_data", as_of=as_of.strftime("%Y-%m-%d"))
        return []

    atr_map = load_atr(con, as_of)
    bench_ret_1d = get_benchmark_return(price_ctx, benchmark)

    mom_map = load_momentum_analysis(con, as_of)
    mr_map = load_mean_reversion(con, as_of)
    bo_map = load_breakout(con, as_of)
    lab_map = load_lab_factor(con, as_of)
    earn_risk_map = load_earnings_risk(con, as_of)
    earn_map = load_earnings_events(con, as_of)
    filing_map = load_filings(con, as_of)
    index_change_map = load_index_changes(con, as_of)
    news_map = load_news(con, as_of)

    opts_map, opts_analysis_map = load_options_current(con, as_of)
    opts_hist_map, opts_analysis_hist_map = load_options_history(
        con, as_of, hist_target_str, hist_lo_str, hist_hi_str,
    )
    apply_proxy_mapping(opts_map, opts_analysis_map, opts_hist_map, opts_analysis_hist_map)

    sentiment_map = load_sentiment(con, as_of)
    profile_map = _load_company_profiles(con, as_of, symbols)

    # Price signal layer
    coint_map = load_cointegration(con, as_of)
    granger_map = load_granger(con, as_of)
    car_map = load_earnings_car(con, as_of)
    kalman_map = load_kalman_betas(con, as_of)

    # ── Score each symbol ──────────────────────────────────────────────────
    scored = []

    for _, row in price_ctx.iterrows():
        sym = row["symbol"]
        if sym == benchmark:
            continue

        ac = _safe(row.get("adj_close"), 0.0)
        prev1 = _safe(row.get("prev_1d"))
        prev5 = _safe(row.get("prev_5d"))
        prev21 = _safe(row.get("prev_21d"))
        vol = _safe(row.get("volume"), 0.0)
        avg_vol = _safe(row.get("avg_vol_20d"), 1.0)
        high52 = _safe(row.get("high_52w"), ac)

        # Returns
        ret_1d  = ((ac / prev1 - 1.0) * 100.0) if prev1 and prev1 > 0 else None
        ret_5d  = ((ac / prev5 - 1.0) * 100.0) if prev5 and prev5 > 0 else None
        ret_21d = ((ac / prev21 - 1.0) * 100.0) if prev21 and prev21 > 0 else None
        pct_from_52w_high = ((ac / high52 - 1.0) * 100.0) if high52 and high52 > 0 else None
        rel_volume = vol / avg_vol if avg_vol > 0 else 1.0

        atr = _safe(atr_map.get(sym), ac * 0.015)  # fallback: 1.5% of price
        avg_dollar_volume_20d = float(avg_vol * ac) if avg_vol and ac else 0.0

        # Score each dimension
        magnitude_sc = score_magnitude(ret_1d, ac, atr, rel_volume)
        event_sc, events = score_event(sym, as_of, earn_map, filing_map, index_change_map)
        momentum_sc, mom_payload = score_momentum(mom_map.get(sym))
        options_sc, iv_ratio_level, opts_payload = score_options(
            sym,
            opts_map.get(sym),
            opts_hist_map.get(sym),
            opts_analysis_map.get(sym),
            opts_analysis_hist_map.get(sym),
            atr,
            ac,
        )
        cross_sc = score_cross_asset(ret_1d, bench_ret_1d)

        # Lab factor score: abs(composite) as notability signal (strong signal in either direction = notable)
        lab_payload_for_score: dict = lab_map.get(sym, {})
        lab_sc = min(abs(lab_payload_for_score.get("lab_composite", 0.0)), 1.0)

        score = composite_score(magnitude_sc, event_sc, momentum_sc, options_sc, cross_sc, lab_sc)

        sub_scores = {
            "magnitude": magnitude_sc,
            "event": event_sc,
            "momentum": momentum_sc,
            "options": options_sc,
            "cross_asset": cross_sc,
            "lab_factor": lab_sc,
        }
        primary_reason = max(sub_scores, key=sub_scores.get)

        earn_risk_payload = build_earnings_risk_payload(earn_risk_map.get(sym))

        # Assemble price signals for this symbol
        price_signals: dict = {}
        if sym in coint_map:
            price_signals["cointegration"] = coint_map[sym]
        if sym in granger_map:
            price_signals["granger"] = granger_map[sym]
        if sym in car_map:
            price_signals["earnings_car"] = car_map[sym]
        if sym in kalman_map:
            price_signals["kalman_beta"] = kalman_map[sym]

        # Assemble mean-reversion payload
        mr_payload: dict = {}
        mr_row = mr_map.get(sym)
        if mr_row is not None:
            mr_details = json.loads(mr_row["details"]) if mr_row.get("details") else {}
            mr_payload = {
                "reversion_score": mr_details.get("reversion_score"),
                "reversion_direction": mr_details.get("reversion_direction"),
                "rsi_14": mr_details.get("rsi_14"),
                "bb_position": mr_details.get("bb_position"),
                "ma20_pct": mr_details.get("ma20_pct"),
                "ma20_z": mr_details.get("ma20_z"),
                "strength_bucket": mr_row.get("strength_bucket"),
            }

        # Assemble breakout payload
        bo_payload: dict = {}
        bo_row = bo_map.get(sym)
        if bo_row is not None:
            bo_details = json.loads(bo_row["details"]) if bo_row.get("details") else {}
            bo_payload = {
                "breakout_score": bo_details.get("breakout_score"),
                "breakout_direction": bo_details.get("breakout_direction"),
                "squeeze_score": bo_details.get("squeeze_score"),
                "volume_score": bo_details.get("volume_score"),
                "range_score": bo_details.get("range_score"),
                "vol_expansion": bo_details.get("vol_expansion"),
                "strength_bucket": bo_row.get("strength_bucket"),
            }

        # Factor Lab data
        lab_payload: dict = lab_map.get(sym, {})

        item = assemble_item(
            sym=sym,
            score=score,
            sub_scores=sub_scores,
            primary_reason=primary_reason,
            ac=ac,
            ret_1d=ret_1d,
            ret_5d=ret_5d,
            ret_21d=ret_21d,
            pct_from_52w_high=pct_from_52w_high,
            rel_volume=rel_volume,
            atr=atr,
            mom_payload=mom_payload,
            earn_risk_payload=earn_risk_payload,
            opts_payload=opts_payload,
            events=events,
            news=news_map.get(sym, [])[:3],
            filings=filing_map.get(sym, [])[:3],
            index_changes=index_change_map.get(sym, []),
            sentiment=sentiment_map.get(sym),
            price_signals=price_signals if price_signals else None,
        )

        profile = profile_map.get(sym, {})
        item["fundamentals"] = {
            "company_name": profile.get("company_name"),
            "sector": profile.get("sector"),
            "industry": profile.get("industry"),
            "market_cap_musd": _safe(profile.get("market_cap")),
        }
        item["avg_dollar_volume_20d"] = round(avg_dollar_volume_20d, 2)

        # Attach reversion/breakout data to item
        if mr_payload:
            item["mean_reversion"] = mr_payload
        if bo_payload:
            item["breakout"] = bo_payload
        if lab_payload:
            item["lab_factor"] = lab_payload

        selection = _selection_metadata(
            item,
            core_symbols=core_symbols or set(),
            selection_policy=selection_policy,
        )
        item["selection"] = selection
        item["report_bucket"] = selection["lane"]
        item["report_score"] = selection["report_score"]

        scored.append(item)

    # ── Rank, inject events, annotate shared options ───────────────────────
    top = rank_and_select(scored, max_items, selection_policy=selection_policy)
    annotate_shared_options_groups(top)

    log.info(
        "notable_items_selected",
        total_universe=len(scored),
        selected=len(top),
        event_driven=len([s for s in top if s["sub_scores"]["event"] > 0.5]),
        core_book=len([s for s in top if s.get("report_bucket") == "core"]),
        event_tape=len([s for s in top if s.get("report_bucket") == "event_tape"]),
    )

    return top
