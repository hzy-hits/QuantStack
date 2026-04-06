"""
News quality assessment: freshness decay, dedup, attribution.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any

import structlog

log = structlog.get_logger()


def _headline_words(headline: str) -> set[str]:
    """Extract lowercase word tokens from a headline."""
    return set(re.findall(r"[a-z0-9]+", headline.lower()))


def _jaccard_similarity(a: set[str], b: set[str]) -> float:
    """Jaccard similarity between two word sets."""
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


def _parse_date(ts: Any, as_of: date) -> date | None:
    """Try to parse a timestamp/date string to a date object."""
    if ts is None:
        return None
    s = str(ts)[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def assess_news_quality(
    items: list[dict],
    as_of: date | None = None,
) -> list[dict]:
    """
    For each notable item's news list:
    1. Freshness decay: age_days -> decay = exp(-ln(2) * age / 3) (3-day half-life)
    2. Dedup: Jaccard similarity on headline words. If >0.6 sim, keep newest only.
    3. Attribution: flag if same news appears on multiple symbols (shared catalyst)

    Modifies items in-place: adds news_quality field to each item.
    Returns list of shared catalysts (news appearing on 3+ symbols).
    """
    if as_of is None:
        from zoneinfo import ZoneInfo
        as_of = datetime.now(ZoneInfo("America/New_York")).date()

    # ── Pass 1: build headline -> symbol mapping for attribution ──────────
    headline_symbols: dict[str, list[str]] = {}
    for item in items:
        sym = item.get("symbol", "?")
        for n in item.get("news", []):
            headline = n.get("headline", "")
            if headline:
                headline_symbols.setdefault(headline, []).append(sym)

    # Also check for similar (not identical) headlines across symbols
    all_headlines: list[tuple[str, str, set[str]]] = []  # (headline, symbol, words)
    for item in items:
        sym = item.get("symbol", "?")
        for n in item.get("news", []):
            headline = n.get("headline", "")
            if headline:
                words = _headline_words(headline)
                all_headlines.append((headline, sym, words))

    # Build similarity clusters: headlines with Jaccard > 0.6 across symbols
    shared_groups: dict[str, set[str]] = {}  # representative_headline -> symbols
    for i, (h1, s1, w1) in enumerate(all_headlines):
        for j in range(i + 1, len(all_headlines)):
            h2, s2, w2 = all_headlines[j]
            if s1 == s2:
                continue
            if _jaccard_similarity(w1, w2) > 0.6:
                # Group by the first headline seen
                key = h1
                if key not in shared_groups:
                    shared_groups[key] = {s1}
                shared_groups[key].add(s2)

    # Shared catalysts: headlines touching 3+ symbols
    shared_catalysts = [
        {"headline": headline, "symbols": sorted(syms)}
        for headline, syms in shared_groups.items()
        if len(syms) >= 3
    ]

    # ── Pass 2: per-item news quality ─────────────────────────────────────
    for item in items:
        news_list = item.get("news", [])
        if not news_list:
            item["news_quality"] = {"count": 0, "deduped_count": 0, "avg_freshness": None}
            continue

        # Add freshness decay to each news item
        for n in news_list:
            pub_date = _parse_date(n.get("published_at"), as_of)
            if pub_date is not None:
                age_days = max(0, (as_of - pub_date).days)
                n["_freshness_decay"] = round(math.exp(-math.log(2) * age_days / 3.0), 3)
                n["_age_days"] = age_days
            else:
                n["_freshness_decay"] = 0.5  # unknown age -> moderate penalty
                n["_age_days"] = None

            # Attribution: is this a shared catalyst?
            headline = n.get("headline", "")
            syms_with_same = headline_symbols.get(headline, [])
            n["_shared_with"] = [s for s in syms_with_same if s != item.get("symbol")]

        # Dedup within this item's news
        deduped: list[dict] = []
        seen_word_sets: list[set[str]] = []
        for n in sorted(news_list, key=lambda x: x.get("_freshness_decay", 0), reverse=True):
            words = _headline_words(n.get("headline", ""))
            is_dup = False
            for seen in seen_word_sets:
                if _jaccard_similarity(words, seen) > 0.6:
                    is_dup = True
                    break
            if not is_dup:
                deduped.append(n)
                seen_word_sets.append(words)

        freshness_values = [n["_freshness_decay"] for n in deduped if "_freshness_decay" in n]
        avg_freshness = round(sum(freshness_values) / len(freshness_values), 3) if freshness_values else None

        item["news_quality"] = {
            "count": len(news_list),
            "deduped_count": len(deduped),
            "avg_freshness": avg_freshness,
            "has_shared_catalyst": any(n.get("_shared_with") for n in news_list),
        }

    log.info(
        "news_quality_assessed",
        items=len(items),
        shared_catalysts=len(shared_catalysts),
    )

    return shared_catalysts
