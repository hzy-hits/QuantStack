"""
Options-extremes extraction for the report bundle.

Moved from ``bundle.py`` to keep the orchestrator thin.
"""
from __future__ import annotations


def compute_options_extremes(notable_items: list[dict], top_n: int = 5) -> dict:
    """
    Extract top-N most extreme bullish and bearish options positioning.
    Ranked by options signed_score from the classify signal.
    """
    scored = []
    for item in notable_items:
        opts = item.get("options", {})
        sig = item.get("signal", {})
        src_details = sig.get("source_details", {})
        opt_src = src_details.get("options", {})
        opt_score = opt_src.get("signed_score", 0.0)
        pc_ratio = opts.get("put_call_ratio")
        if pc_ratio is None and not opts:
            continue  # no options data at all

        # Summarize unusual flow
        unusual = opts.get("unusual_activity", [])
        call_vol = sum(u.get("volume", 0) for u in unusual if u.get("type") == "call")
        put_vol = sum(u.get("volume", 0) for u in unusual if u.get("type") == "put")
        if call_vol + put_vol > 0:
            flow_pct_calls = round(call_vol / (call_vol + put_vol) * 100)
            flow_summary = f"{flow_pct_calls}% calls / {100 - flow_pct_calls}% puts"
            total_unusual_vol = call_vol + put_vol
        else:
            flow_summary = None
            total_unusual_vol = 0

        scored.append({
            "symbol": item.get("symbol"),
            "options_score": round(opt_score, 3),
            "put_call_ratio": round(pc_ratio, 3) if pc_ratio is not None else None,
            "iv_skew": opts.get("iv_skew"),
            "bias_signal": opts.get("bias_signal"),
            "atm_iv_pct": opts.get("atm_iv_pct"),
            "iv_data_quality": opts.get("iv_data_quality"),
            "unusual_flow_summary": flow_summary,
            "unusual_total_volume": total_unusual_vol,
            "ret_1d_pct": item.get("ret_1d_pct"),
            "confidence": sig.get("confidence"),
        })

    # Sort by options_score: most positive (bullish) and most negative (bearish)
    bullish = sorted(
        [s for s in scored if s["options_score"] > 0.05],
        key=lambda x: x["options_score"], reverse=True,
    )[:top_n]
    bearish = sorted(
        [s for s in scored if s["options_score"] < -0.05],
        key=lambda x: x["options_score"],
    )[:top_n]

    return {"bullish": bullish, "bearish": bearish}
