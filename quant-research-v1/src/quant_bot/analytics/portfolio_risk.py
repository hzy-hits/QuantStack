"""
Portfolio risk summary for HIGH-signal items.
"""
from __future__ import annotations

from typing import Any

import structlog

log = structlog.get_logger()


def compute_portfolio_risk(
    high_items: list[dict],
    clusters: dict[str, Any] | None,
    corr_matrix: Any = None,
    symbols_aligned: list[str] | None = None,
) -> dict[str, Any]:
    """
    For all HIGH-signal items:
    1. Net directional tilt: sum of directions (are we net long or balanced?)
    2. Herfindahl index: concentration in few sectors/clusters
    3. Natural hedges: identify opposing items in same cluster
    4. Independent bet count: from clusters

    Returns: {
        net_tilt: float,  # positive = net long
        herfindahl: float,  # 0-1, higher = more concentrated
        n_independent_bets: int,
        natural_hedges: list[tuple],
        sector_concentration: dict[sector -> count],
    }
    """
    if not high_items:
        return {
            "net_tilt": 0.0,
            "herfindahl": 0.0,
            "n_independent_bets": 0,
            "natural_hedges": [],
            "sector_concentration": {},
        }

    # ── Net directional tilt ────────────────────────────────────────────────
    net_tilt = 0.0
    for item in high_items:
        sig = item.get("signal", {})
        direction = sig.get("direction", "neutral")
        score = sig.get("direction_score", 0.0)
        if direction == "long":
            net_tilt += abs(score)
        elif direction == "short":
            net_tilt -= abs(score)

    # ── Sector concentration (Herfindahl) ───────────────────────────────────
    sector_counts: dict[str, int] = {}
    for item in high_items:
        # Try to get sector from various sources
        sector = None
        mom = item.get("momentum") or {}
        if mom.get("sector"):
            sector = mom["sector"]
        elif item.get("sector"):
            sector = item["sector"]
        else:
            # Infer from asset bucket if signal is present
            sig = item.get("signal", {})
            sector = sig.get("macro_asset_bucket", "unknown")
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    n = len(high_items)
    # Herfindahl index: sum of squared market shares
    if n > 0:
        herfindahl = sum((count / n) ** 2 for count in sector_counts.values())
    else:
        herfindahl = 0.0

    # ── Independent bets from clusters ──────────────────────────────────────
    n_independent = n  # default: each item is independent
    if clusters:
        symbol_cluster = clusters.get("symbol_cluster", {})
        # Count unique clusters among HIGH items
        high_clusters = set()
        for item in high_items:
            sym = item.get("symbol", "")
            cid = symbol_cluster.get(sym)
            if cid is not None:
                high_clusters.add(cid)
            else:
                # Symbol not in correlation matrix -- count as independent
                high_clusters.add(f"_unclustered_{sym}")
        n_independent = len(high_clusters)

    # ── Natural hedges: opposing directions in same cluster ─────────────────
    natural_hedges: list[dict] = []
    if clusters:
        symbol_cluster = clusters.get("symbol_cluster", {})
        # Group HIGH items by cluster
        cluster_items: dict[int, list[dict]] = {}
        for item in high_items:
            sym = item.get("symbol", "")
            cid = symbol_cluster.get(sym)
            if cid is not None:
                cluster_items.setdefault(cid, []).append(item)

        for cid, c_items in cluster_items.items():
            if len(c_items) < 2:
                continue
            longs = [i for i in c_items if i.get("signal", {}).get("direction") == "long"]
            shorts = [i for i in c_items if i.get("signal", {}).get("direction") == "short"]
            if longs and shorts:
                for l_item in longs:
                    for s_item in shorts:
                        natural_hedges.append({
                            "long": l_item["symbol"],
                            "short": s_item["symbol"],
                            "cluster_id": cid,
                        })

    result = {
        "net_tilt": round(net_tilt, 3),
        "herfindahl": round(herfindahl, 3),
        "n_independent_bets": n_independent,
        "natural_hedges": natural_hedges,
        "sector_concentration": sector_counts,
    }

    log.info(
        "portfolio_risk_computed",
        high_items=len(high_items),
        net_tilt=result["net_tilt"],
        n_independent=n_independent,
        hedges=len(natural_hedges),
    )

    return result
