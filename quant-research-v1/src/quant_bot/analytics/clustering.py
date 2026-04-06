"""
Correlation-based clustering using Ledoit-Wolf covariance.
Groups correlated assets to identify independent bets.

Symbols with corr > 0.7 in the same cluster count as 1 independent bet.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import structlog
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

log = structlog.get_logger()


def compute_clusters(
    corr_matrix: np.ndarray,
    symbols: list[str],
    corr_threshold: float = 0.7,
) -> dict[str, Any]:
    """
    Hierarchical clustering from correlation matrix.

    1. Convert correlation matrix to distance: d = (1 - corr) / 2
    2. Hierarchical clustering (average linkage)
    3. Cut tree at threshold (corr > 0.7 = same cluster)
    4. Label each cluster by dominant sector/theme

    Returns:
        {
            clusters: dict[int, list[str]],      # cluster_id -> symbols
            symbol_cluster: dict[str, int],       # symbol -> cluster_id
            n_independent_bets: int,              # number of clusters
        }
    """
    n = len(symbols)
    if n < 2:
        # Degenerate case: everything is its own cluster
        clusters = {0: list(symbols)} if symbols else {}
        symbol_cluster = {s: 0 for s in symbols}
        return {
            "clusters": clusters,
            "symbol_cluster": symbol_cluster,
            "n_independent_bets": len(clusters),
        }

    # Distance = (1 - corr) / 2: 0 = perfectly correlated, 0.5 = uncorrelated, 1 = anti-correlated
    # Using signed correlation so anti-correlated assets (hedges) are far apart,
    # not clustered together as with |corr|.
    dist_matrix = (1.0 - corr_matrix) / 2.0
    # Ensure diagonal is exactly 0 and matrix is symmetric
    np.fill_diagonal(dist_matrix, 0.0)
    dist_matrix = (dist_matrix + dist_matrix.T) / 2.0
    # Clip small negative values from floating-point noise
    dist_matrix = np.clip(dist_matrix, 0.0, 2.0)

    # Convert to condensed distance vector for scipy
    condensed = squareform(dist_matrix, checks=False)

    # Average linkage clustering
    Z = linkage(condensed, method="average")

    # Cut tree: distance threshold = (1 - corr_threshold) / 2
    # e.g. corr_threshold=0.7 -> distance=0.15
    dist_cut = (1.0 - corr_threshold) / 2.0
    labels = fcluster(Z, t=dist_cut, criterion="distance")

    # Build cluster mapping
    clusters: dict[int, list[str]] = {}
    symbol_cluster: dict[str, int] = {}
    for sym, label in zip(symbols, labels):
        cid = int(label)
        clusters.setdefault(cid, []).append(sym)
        symbol_cluster[sym] = cid

    n_clusters = len(clusters)

    log.info(
        "clustering_complete",
        n_symbols=n,
        n_clusters=n_clusters,
        corr_threshold=corr_threshold,
        largest_cluster=max(len(v) for v in clusters.values()) if clusters else 0,
    )

    return {
        "clusters": clusters,
        "symbol_cluster": symbol_cluster,
        "n_independent_bets": n_clusters,
    }
