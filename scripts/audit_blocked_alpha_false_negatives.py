#!/usr/bin/env python3
"""Audit blocked alpha candidates that later moved strongly.

The report is diagnostic only: it identifies which blockers most often appear
on candidates labelled as missed/right-but-no-fill/observed alpha.
"""

from __future__ import annotations

import argparse
import collections
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb


BAD_LABELS = {"missed_alpha", "right_but_no_fill", "observed_alpha", "stale_chase"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-db", default="data/strategy_backtest_history.duckdb")
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--window-days", type=int, default=60)
    parser.add_argument("--min-best-pct", type=float, default=5.0)
    parser.add_argument(
        "--output-dir",
        default="reports/review_dashboard/blocked_alpha_false_negative",
    )
    return parser.parse_args()


def safe_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def blocker_tags(source_json: str | None) -> tuple[list[str], dict[str, Any]]:
    row = safe_json(source_json)
    details = safe_json(row.get("details_json"))
    gate = details.get("execution_gate") or details.get("main_signal_gate") or {}
    gate = gate if isinstance(gate, dict) else {}
    tags: list[str] = []
    for blocker in gate.get("blockers") or []:
        text = str(blocker)
        if "headline" not in text.lower():
            tags.append(text)
    bucket = str(row.get("report_bucket") or "").lower().replace(" ", "_")
    execution = str(row.get("execution_mode") or gate.get("execution_mode") or "").lower()
    no_fill = str(row.get("no_fill_reason") or "").lower()
    rr = row.get("rr_ratio")
    if bucket in {"radar", "appendix", "theme_rotation", "event_tape"}:
        tags.append("strategy/out-of-scope")
    if execution == "wait_pullback":
        tags.append("execution_wait_pullback")
    if execution == "do_not_chase":
        tags.append("stale_chase_or_do_not_chase")
    try:
        if rr is not None and float(rr) < 1.5:
            tags.append("rr_below_1_5")
    except (TypeError, ValueError):
        pass
    if "risk_reward" in no_fill:
        tags.append("rr_below_1_5")
    if "not_actionable" in no_fill:
        tags.append("execution_wait_pullback")
    if not tags:
        tags.append("other")
    out: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if tag and tag not in seen:
            out.append(tag)
            seen.add(tag)
    return out, row


def fmt_pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}%"


def main() -> None:
    args = parse_args()
    db_path = Path(args.history_db)
    con = duckdb.connect(str(db_path), read_only=True)
    if args.as_of:
        as_of_s = str(args.as_of)
        start = date.fromisoformat(as_of_s) - timedelta(days=args.window_days)
        rows = con.execute(
            """
            SELECT as_of, market, report_date, symbol, policy_id, label, data_ready, return_pct, source_json
            FROM candidate_outcomes
            WHERE as_of = CAST(? AS DATE)
              AND report_date >= CAST(? AS DATE)
            """,
            [as_of_s, start.isoformat()],
        ).fetchall()
        as_of_by_market = {
            market: as_of_s
            for (market,) in con.execute(
                "SELECT DISTINCT market FROM candidate_outcomes WHERE as_of = CAST(? AS DATE)",
                [as_of_s],
            ).fetchall()
        }
    else:
        rows = con.execute(
            """
            WITH latest AS (
              SELECT market, max(as_of) AS as_of
              FROM candidate_outcomes
              GROUP BY market
            )
            SELECT c.as_of, c.market, c.report_date, c.symbol, c.policy_id, c.label,
                   c.data_ready, c.return_pct, c.source_json
            FROM candidate_outcomes c
            JOIN latest l
              ON c.market = l.market
             AND c.as_of = l.as_of
            WHERE c.report_date >= l.as_of - ?
            """,
            [args.window_days],
        ).fetchall()
        as_of_by_market = {
            market: str(as_of)
            for market, as_of in con.execute(
                "SELECT market, max(as_of) FROM candidate_outcomes GROUP BY market"
            ).fetchall()
        }
        max_as_of = max(date.fromisoformat(value) for value in as_of_by_market.values())
        as_of_s = max_as_of.isoformat()
        start = max_as_of - timedelta(days=args.window_days)

    label_counts: collections.Counter[tuple[str, str]] = collections.Counter()
    blocker_counts: collections.Counter[tuple[str, str]] = collections.Counter()
    blocker_best: dict[tuple[str, str], list[float]] = collections.defaultdict(list)
    examples: list[dict[str, Any]] = []

    for row_as_of, market, report_date, symbol, policy_id, label, _ready, return_pct, source_json in rows:
        if label not in BAD_LABELS:
            continue
        tags, row = blocker_tags(source_json)
        label_counts[(market, label)] += 1
        best = row.get("best_possible_ret_pct")
        try:
            best_f = float(best) if best is not None else None
        except (TypeError, ValueError):
            best_f = None
        for tag in tags:
            blocker_counts[(market, tag)] += 1
            if best_f is not None:
                blocker_best[(market, tag)].append(best_f)
        if best_f is not None and best_f >= args.min_best_pct:
            examples.append(
                {
                    "best_possible_ret_pct": round(best_f, 3),
                    "as_of": str(row_as_of),
                    "market": market,
                    "report_date": str(report_date),
                    "symbol": symbol,
                    "label": label,
                    "policy_id": policy_id,
                    "blockers": tags,
                    "rr_ratio": row.get("rr_ratio"),
                    "composite_score": row.get("composite_score"),
                    "no_fill_reason": row.get("no_fill_reason"),
                    "return_pct": return_pct,
                }
            )

    out_root = Path(args.output_dir) / as_of_s
    out_root.mkdir(parents=True, exist_ok=True)
    json_path = out_root / "blocked_alpha_false_negative.json"
    md_path = out_root / "blocked_alpha_false_negative.md"

    summary_rows = []
    for (market, blocker), count in blocker_counts.most_common():
        values = blocker_best[(market, blocker)]
        avg_best = sum(values) / len(values) if values else None
        summary_rows.append(
            {
                "market": market,
                "blocker": blocker,
                "count": count,
                "avg_best_possible_ret_pct": None if avg_best is None else round(avg_best, 3),
            }
        )
    examples = sorted(examples, key=lambda row: row["best_possible_ret_pct"], reverse=True)[:50]
    payload = {
        "as_of": as_of_s,
        "as_of_by_market": as_of_by_market,
        "window_start": start.isoformat(),
        "window_days": args.window_days,
        "bad_labels": sorted(BAD_LABELS),
        "label_counts": [
            {"market": market, "label": label, "count": count}
            for (market, label), count in label_counts.most_common()
        ],
        "blocker_summary": summary_rows,
        "examples": examples,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lines = [
        f"# Blocked Alpha False Negative Audit - {as_of_s}",
        "",
        f"Window: `{start.isoformat()}` to `{as_of_s}`; latest as_of by market: `{as_of_by_market}`; labels: `{', '.join(sorted(BAD_LABELS))}`.",
        "",
        "## Label Counts",
        "",
        "| Market | Label | Count |",
        "|---|---|---:|",
    ]
    for row in payload["label_counts"]:
        lines.append(f"| {row['market']} | {row['label']} | {row['count']} |")
    lines.extend(["", "## Blocker Concentration", "", "| Market | Blocker | Count | Avg Best Possible Return |", "|---|---|---:|---:|"])
    for row in summary_rows[:30]:
        lines.append(
            f"| {row['market']} | {row['blocker']} | {row['count']} | {fmt_pct(row['avg_best_possible_ret_pct'])} |"
        )
    lines.extend(["", "## Top Examples", "", "| Market | Date | Symbol | Label | Best Possible | Blockers |", "|---|---|---|---|---:|---|"])
    for row in examples[:25]:
        lines.append(
            "| {market} | {date} | {symbol} | {label} | {best} | {blockers} |".format(
                market=row["market"],
                date=row["report_date"],
                symbol=row["symbol"],
                label=row["label"],
                best=fmt_pct(row["best_possible_ret_pct"]),
                blockers=", ".join(row["blockers"]),
            )
        )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"wrote {md_path}")
    print(f"wrote {json_path}")


if __name__ == "__main__":
    main()
