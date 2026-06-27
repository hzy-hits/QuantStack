"""0R Ranked Watch 雷达 section (Phase D).

Surfaces ranker rows that landed in the 0R `active_watch` tier — names the
ranker scored and ranked but that did NOT clear the execution line
(prepare-order-but-wait-for-price). 科创板 (688) rows are flagged with ★.

Pure rendering: no I/O, no DB, no payload mutation. Reads
payload["cn_opportunity_ranker"]["all_rows"] only.
"""
from __future__ import annotations

from typing import Any

from lib.fmt import clean_table_text, fmt_num, fmt_pct

RADAR_LIMIT = 20


def board_label(symbol: str) -> str:
    """A-share board from ticker digit-prefix (digits before any market suffix)."""
    digits = "".join(ch for ch in str(symbol) if ch.isdigit())
    if digits.startswith("688"):
        return "科创板"
    if digits.startswith(("300", "301")):
        return "创业板"
    if digits.startswith(("4", "8", "920")):
        return "北交所"
    return "主板"


def cn_ranked_watch_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Ranker rows in the 0R active_watch tier, rank-ascending (None ranks last)."""
    ranker = payload.get("cn_opportunity_ranker") or {}
    rows = [
        row
        for row in (ranker.get("all_rows") or [])
        if str(row.get("production_tier")) == "active_watch"
    ]
    rows.sort(key=lambda r: (r.get("rank") is None, r.get("rank") or 9_999))
    return rows


def render_cn_ranked_watch_radar_section(payload: dict[str, Any]) -> list[str]:
    rows = cn_ranked_watch_rows(payload)
    lines = [
        "## 0R 观察雷达 (Ranked Watch)",
        "",
        "这一档是 ranker 已排名、但未达执行线的 0R 候选(prepare but wait);★ 为科创板。不占资金,仅观察。",
        "",
    ]
    if not rows:
        lines += [
            "今天没有 active_watch 0R 候选(名字要么进了可交易名单,要么落到 bench)。",
            "",
        ]
        return lines
    lines += [
        "| Rank | Symbol | Name | 板 | Score | 1D | EV LCB80 | Size | Reason |",
        "|---:|---|---|---|---:|---:|---:|---|---|",
    ]
    for row in rows[:RADAR_LIMIT]:
        board = board_label(row.get("symbol") or "")
        board_cell = f"★{board}" if board == "科创板" else board
        ev = row.get("ev_lcb80_pct")
        pct = row.get("pct_chg")
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("rank")) if row.get("rank") is not None else "-",
                    str(row.get("symbol") or "-"),
                    clean_table_text(row.get("name") or "-", 22),
                    board_cell,
                    fmt_num(row.get("rank_score"), 2),
                    fmt_pct(pct) if pct is not None else "-",
                    fmt_pct(ev) if ev is not None else "-",
                    str(row.get("size_hint") or "0R"),
                    clean_table_text(str(row.get("reason") or "-"), 60),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines
