"""Cross-asset divergence detection."""
from __future__ import annotations

from typing import Any

from ._asset_buckets import CROSS_ASSET_REF_MAP


def _cross_asset_divergence(item: dict, market_context: dict | None) -> dict[str, Any]:
    """Check if item's 1D return diverges from its reference assets."""
    if not market_context:
        return {"divergence_detected": False}

    symbol = item.get("symbol", "")
    refs = CROSS_ASSET_REF_MAP.get(symbol)
    if not refs:
        return {"divergence_detected": False}

    # Get item's 1D return
    item_ret = item.get("ret_1d_pct")
    if item_ret is None:
        return {"divergence_detected": False}

    # Get reference asset returns from universe_summary in market_context
    uni = market_context.get("universe_summary", {})
    indices = market_context.get("indices", {})
    sectors = market_context.get("sectors", {})

    ref_rets = {}
    for ref_sym in refs:
        # Check indices, sectors, and universe_summary for the reference return
        ret = None
        for source in [indices, sectors, uni]:
            entry = source.get(ref_sym, {})
            if isinstance(entry, dict):
                ret = entry.get("ret_1d_pct") or entry.get("change_pct")
            if ret is not None:
                break
        if ret is not None:
            ref_rets[ref_sym] = ret

    if not ref_rets:
        return {"divergence_detected": False}

    # Divergence: item positive but ALL refs negative, or vice versa
    item_positive = item_ret > 0.5  # threshold to avoid noise
    all_refs_opposite = all(
        (r < -0.5) if item_positive else (r > 0.5)
        for r in ref_rets.values()
    )

    return {
        "divergence_detected": all_refs_opposite and len(ref_rets) > 0,
        "ref_returns": ref_rets,
        "item_ret_1d": round(item_ret, 2),
    }
