"""Ranking, event injection, and shared options group annotation."""
from __future__ import annotations

import structlog

from quant_bot.data_ingestion.options import OPTIONS_PROXY_MAP

log = structlog.get_logger()


def _rank_score(item: dict) -> float:
    return float(
        item.get("selection_rank_score")
        or item.get("report_score")
        or item.get("score")
        or 0.0
    )


def rank_and_select(
    scored: list[dict],
    max_items: int,
    selection_policy: dict | None = None,
) -> list[dict]:
    """
    Rank scored items by composite score, inject high-event items,
    and return the top items.
    """
    scored.sort(key=_rank_score, reverse=True)

    if not selection_policy:
        top = scored[:max_items]
        top_syms = {s["symbol"] for s in top}

        # Inject high-event items by replacing lowest-score non-event items
        event_extras = [
            s for s in scored[max_items:]
            if s["sub_scores"]["event"] > 0.5
            and s["symbol"] not in top_syms
            and (s.get("selection") or {}).get("execution_action") != "do_not_chase"
        ][:5]
        for extra in event_extras:
            replaceable = [
                (i, s) for i, s in enumerate(top)
                if s["sub_scores"]["event"] <= 0.3
            ]
            if not replaceable:
                break
            worst_idx, _ = min(replaceable, key=lambda x: _rank_score(x[1]))
            top[worst_idx] = extra
        top.sort(key=_rank_score, reverse=True)
        return top

    selected: list[dict] = []
    seen: set[str] = set()

    def _take(items: list[dict], limit: int) -> None:
        nonlocal selected
        for item in items:
            if len(selected) >= max_items or len([x for x in selected if x.get("report_bucket") == item.get("report_bucket")]) >= limit:
                continue
            if item["symbol"] in seen:
                continue
            selected.append(item)
            seen.add(item["symbol"])

    core_items = sorted(
        [x for x in scored if x.get("report_bucket") == "core"],
        key=lambda x: (_rank_score(x), x.get("report_score", x["score"]), x["score"]),
        reverse=True,
    )
    tactical_items = sorted(
        [x for x in scored if x.get("report_bucket") == "tactical_continuation"],
        key=lambda x: (_rank_score(x), x.get("report_score", x["score"]), x["score"]),
        reverse=True,
    )
    event_tape_items = sorted(
        [x for x in scored if x.get("report_bucket") == "event_tape"],
        key=lambda x: (_rank_score(x), x.get("report_score", x["score"]), x["score"]),
        reverse=True,
    )
    appendix_items = sorted(
        [x for x in scored if x.get("report_bucket") == "appendix"],
        key=lambda x: (_rank_score(x), x.get("report_score", x["score"]), x["score"]),
        reverse=True,
    )

    _take(core_items, min(selection_policy.get("core_max_items", max_items), max_items))
    _take(
        tactical_items,
        min(selection_policy.get("tactical_continuation_max_items", 0), max_items),
    )
    _take(event_tape_items, min(selection_policy.get("event_tape_max_items", 0), max_items))
    _take(appendix_items, min(selection_policy.get("appendix_max_items", 0), max_items))

    if len(selected) < max_items:
        remaining = sorted(
            [x for x in scored if x["symbol"] not in seen],
            key=lambda x: (_rank_score(x), x.get("report_score", x["score"]), x["score"]),
            reverse=True,
        )
        for item in remaining[: max_items - len(selected)]:
            selected.append(item)
            seen.add(item["symbol"])

    return selected[:max_items]


def annotate_shared_options_groups(top: list[dict]) -> None:
    """
    Annotate items that share the same options chain via proxy mapping.
    Mutates items in place.
    """
    top_map = {item["symbol"]: item for item in top}
    shared_groups: dict[str, list[str]] = {}
    for proxy_sym, source_sym in OPTIONS_PROXY_MAP.items():
        if proxy_sym in top_map and source_sym in top_map:
            shared_groups.setdefault(source_sym, [source_sym])
            if proxy_sym not in shared_groups[source_sym]:
                shared_groups[source_sym].append(proxy_sym)

    for source_sym, members in shared_groups.items():
        group = list(dict.fromkeys(members))
        for member in group:
            item = top_map[member]
            opts_dict = item.setdefault("options", {})
            others = [s for s in group if s != member]
            opts_dict["shared_options_group"] = group
            opts_dict["shared_options_with"] = others
            if member == source_sym:
                opts_dict["shared_options_note"] = (
                    f"{', '.join(others)} reuse this same options chain as proxies; "
                    f"treat these symbols as sharing one derivatives dataset."
                )
            else:
                opts_dict["shared_options_note"] = (
                    f"Shares the same options data as {source_sym}; "
                    f"this is proxy-derived, not independent."
                )
