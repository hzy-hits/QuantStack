"""
Assemble the validated report bundle.

The bundle is what Claude receives. It contains everything the agent needs:
  - market_context: regime, macro (human-labeled), benchmark, Fed/FOMC
  - notable_items: pre-filtered, ranked, enriched with news + filings + options
  - polymarket: crowd probability for macro events
  - universe_summary: all ~60 symbols at a glance (regime, returns)
  - coverage: data quality report

Claude reads this and writes the narrative. Claude never sees raw DB queries.
Claude never touches arithmetic. Every number here is computed upstream.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import duckdb
import structlog

from ._bundle_extremes import compute_options_extremes as compute_options_extremes  # noqa: F401 — re-export
from ._bundle_screens import _build_dip_screen_section, _universe_summary
from ._bundle_tier1 import Tier1Builder

log = structlog.get_logger()

# Human-readable labels for FRED series IDs
FRED_LABELS = {
    "FEDFUNDS":     "Fed Funds Rate (%)",
    "DGS10":        "10Y Treasury Yield (%)",
    "BAMLH0A0HYM2": "HY Credit Spread (%-pts, ×100=bps)",
    "VIXCLS":       "VIX — Market Fear Index",
    "T10Y2Y":       "10Y-2Y Yield Spread (recession indicator)",
    "UNRATE":       "Unemployment Rate (%)",
    "CPIAUCSL":     "CPI YoY Inflation Rate (%)",
}


def _latest_macro(con: duckdb.DuckDBPyConnection, as_of_str: str) -> dict:
    """Pull latest macro value per series, as-of the trade date (no leakage)."""
    rows = con.execute("""
        SELECT m.series_id, m.value, m.date
        FROM macro_daily m
        INNER JOIN (
            SELECT series_id, MAX(date) AS max_date
            FROM macro_daily
            WHERE date <= ?
            GROUP BY series_id
        ) latest ON m.series_id = latest.series_id AND m.date = latest.max_date
    """, [as_of_str]).fetchdf()

    result = {}
    if rows.empty:
        return result

    for _, r in rows.iterrows():
        sid = r["series_id"]
        label = FRED_LABELS.get(sid, sid)
        result[label] = {
            "value": round(float(r["value"]), 4) if r["value"] is not None else None,
            "as_of": str(r["date"]),
            "series_id": sid,
        }
    return result


def _polymarket_events(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return top 10 Polymarket finance events by volume."""
    try:
        rows = con.execute("""
            SELECT question, p_yes, p_no, volume_usd, end_date, category
            FROM polymarket_events
            WHERE p_yes IS NOT NULL
            ORDER BY volume_usd DESC NULLS LAST
            LIMIT 10
        """).fetchdf()
    except Exception:
        return []

    result = []
    for _, r in rows.iterrows():
        result.append({
            "question": r["question"],
            "p_yes": round(float(r["p_yes"]), 3) if r["p_yes"] is not None else None,
            "p_no": round(float(r["p_no"]), 3) if r["p_no"] is not None else None,
            "volume_usd": round(float(r["volume_usd"])) if r["volume_usd"] else None,
            "end_date": str(r["end_date"]) if r["end_date"] else None,
            "category": r["category"],
        })
    return result


def _regime_summary(con: duckdb.DuckDBPyConnection, as_of_str: str) -> dict:
    """Compute dominant regime across full universe."""
    rows = con.execute("""
        SELECT regime, COUNT(*) AS cnt
        FROM analysis_daily
        WHERE date = ? AND module_name = 'momentum_risk'
        GROUP BY regime
    """, [as_of_str]).fetchdf()

    if rows.empty:
        return {"label": "unknown", "confidence": 0.0, "confidence_label": "low", "counts": {}}

    counts = dict(zip(rows["regime"], rows["cnt"].astype(int)))
    total = sum(counts.values())
    dominant = max(counts, key=counts.get)
    confidence = round(counts[dominant] / total, 2) if total > 0 else 0.0

    return {
        "label": dominant,
        "regime_prevalence_pct": confidence,  # fraction of universe in dominant regime, NOT a probability
        "prevalence_label": "high" if confidence >= 0.65 else "moderate" if confidence >= 0.50 else "low",
        "counts": counts,
        "total_analyzed": total,
    }


def build_report_bundle(
    con: duckdb.DuckDBPyConnection,
    as_of: date,
    notable_items: list[dict],
    benchmark: str = "SPY",
    universe: dict | None = None,
    dividend_dip_screen: list[dict] | None = None,
) -> dict:
    """
    Assemble the complete payload for Claude.
    notable_items comes pre-built from filtering/notable.py.
    universe dict (from builder.py) is used for Tier 1 breadth computation.
    """
    as_of_str = as_of.strftime("%Y-%m-%d")

    # Tier 1 mandatory market context (indices, VIX, sectors, breadth, Polymarket)
    tier1 = Tier1Builder(con, as_of_str, universe or {}).build()
    polymarket = tier1["polymarket_events"]

    # Macro — human-labeled, no leakage
    macro = _latest_macro(con, as_of_str)

    # Regime across full universe
    regime = _regime_summary(con, as_of_str)

    # Universe snapshot (all symbols, brief)
    universe_summary = _universe_summary(con, as_of_str)

    # Data coverage report
    n_with_prices  = len(universe_summary)
    n_with_news    = len([s for s in notable_items if s.get("news")])
    n_with_filings = len([s for s in notable_items if s.get("sec_filings")])
    n_with_options = len([
        s for s in notable_items
        if s.get("options") and s["options"].get("atm_iv_pct")
    ])
    n_options_eligible = len([
        s for s in notable_items
        if "=" not in s["symbol"] and not s["symbol"].startswith("^")
    ])
    n_with_proxy_options = len([
        s for s in notable_items
        if s.get("options") and s["options"].get("proxy_source")
    ])

    # Market news (MARKET symbol = general headlines)
    try:
        news_start = (as_of - timedelta(days=3)).strftime("%Y-%m-%d 00:00:00")
        news_end = (as_of + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        market_news = con.execute("""
            SELECT headline, source, published_at
            FROM news_items
            WHERE symbol = 'MARKET'
              AND published_at >= ?
              AND published_at < ?
            ORDER BY published_at DESC
            LIMIT 5
        """, [news_start, news_end]).fetchdf()
        market_headlines = [
            {"headline": r["headline"], "source": r["source"], "published_at": str(r["published_at"])}
            for _, r in market_news.iterrows()
        ] if not market_news.empty else []
    except Exception:
        market_headlines = []

    from datetime import datetime as _dt

    return {
        "meta": {
            "trade_date": as_of_str,
            "generated_at": _dt.now().isoformat(timespec="minutes"),
            "benchmark": benchmark,
            "total_universe_size": len(universe_summary),
            "notable_items_count": len(notable_items),
            "data_freshness": {
                "prices": as_of_str,
                "macro": max((v["as_of"] for v in macro.values()), default="unknown"),
                "news": f"last 3 days through {as_of_str}",
                "filings": "last 7 days",
            },
        },
        "market_context": {
            # Tier 1 — guaranteed market context per Agents.md contract
            **tier1,
            # Additional context
            "regime": regime,
            "macro": macro,
            "market_headlines": market_headlines,
        },
        "notable_items": notable_items,
        "options_extremes": {},  # populated after classify_all() in run_daily.py
        "dividend_dip_screen": _build_dip_screen_section(dividend_dip_screen or []),
        "universe_summary": universe_summary,
        "coverage": {
            "symbols_with_price_data": n_with_prices,
            "notable_items_with_news": n_with_news,
            "notable_items_with_filings": n_with_filings,
            "notable_items_with_options": n_with_options,
            "notable_items_with_proxy_options": n_with_proxy_options,
            "notable_items_options_eligible": n_options_eligible,
            "polymarket_events_tracked": len(polymarket),
            "tier1_missing_fields":  len(tier1["uncertainty"]["missing"]),
            "tier1_stale_fields":    len(tier1["uncertainty"]["stale"]),
        },
        "_rendering": {
            "pct_dp": 1,
            "ratio_dp": 2,
            "pvalue_sigfigs": 3,
        },
    }


def compute_headline_gate(bundle: dict[str, Any]) -> dict[str, Any]:
    """
    Determine how strongly the report is allowed to frame the market direction.

    Modes:
      - trend: enough calibrated directional edge to headline a directional view
      - range: state can be discussed, but not as a bull/bear headline
      - uncertain: insufficient predictive edge; describe uncertainty explicitly
    """
    hmm = bundle.get("hmm_regime") or {}
    cal = hmm.get("calibration") or {}
    fear_greed = (bundle.get("market_context") or {}).get("fear_greed") or {}
    fg_inputs = fear_greed.get("inputs") or {}

    p_ret = hmm.get("p_ret_positive_tomorrow")
    days = int(hmm.get("days_in_current_regime") or 0)
    cal_n = int(cal.get("n") or 0)
    brier_skill = cal.get("brier_skill_score")
    edge = abs(float(p_ret) - 0.5) if p_ret is not None else None

    reasons: list[str] = []
    mode = "range"

    if p_ret is None:
        mode = "uncertain"
        reasons.append("HMM next-day probability unavailable")
    else:
        if cal_n < 20:
            reasons.append(f"resolved calibration sample too small (n={cal_n})")
        if edge is not None and edge < 0.03:
            reasons.append(f"next-day edge weak (|P(up)-0.5|={edge:.3f})")
        if brier_skill is not None and brier_skill <= 0:
            reasons.append(f"no calibration edge vs climatology (BSS={brier_skill:.3f})")
        if days < 3:
            reasons.append(f"regime age too short ({days}d)")

        if cal_n < 20 or edge is None or edge < 0.03 or (brier_skill is not None and brier_skill <= 0):
            mode = "uncertain"
        elif edge >= 0.06 and (brier_skill is None or brier_skill > 0) and days >= 3:
            mode = "trend"
        else:
            mode = "range"

    if not reasons:
        if mode == "trend":
            reasons.append("directional edge, calibration, and regime age all clear threshold")
        elif mode == "range":
            reasons.append("directional edge present but not strong enough for a bull/bear headline")
        else:
            reasons.append("insufficient predictive edge for a directional headline")

    bias = "neutral"
    if p_ret is not None:
        if p_ret >= 0.53:
            bias = "bullish"
        elif p_ret <= 0.47:
            bias = "bearish"

    return {
        "mode": mode,
        "bias": bias if mode != "uncertain" else "neutral",
        "allow_directional_regime": mode == "trend",
        "reporting_rule": (
            "Use trend framing only if the full macro evidence panel confirms it; HMM alone is not a bull/bear referee."
            if mode == "trend"
            else "Do not headline bull/bear from HMM alone; describe the market from Fear/Greed, VIX, RSI, breadth, rates, and credit."
        ),
        "inputs": {
            "p_ret_positive_tomorrow": round(float(p_ret), 4) if p_ret is not None else None,
            "edge_vs_coinflip": round(edge, 4) if edge is not None else None,
            "calibration_n": cal_n,
            "brier_score": cal.get("brier_score"),
            "brier_skill_score": brier_skill,
            "regime_days": days,
            "hmm_regime": hmm.get("regime"),
            "fear_greed_score": fear_greed.get("score"),
            "fear_greed_label": fear_greed.get("label"),
            "spy_rsi_14": fg_inputs.get("spy_rsi_14"),
        },
        "reasons": reasons,
    }
