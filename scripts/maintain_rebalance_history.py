"""Append daily rebalance suggestions to a long-running history ledger.

The cross-compare page emits `rebalance_suggestion.json` each day. This
maintainer:

1. Loads `ai_infra/reports/rebalance_history.csv` (creates it if missing).
2. Appends every row from today's suggestion that is not already present,
   keyed by `(as_of, ticker, action)`.
3. **Preserves operator-edited columns** — `executed_tilt_pct`,
   `executed_at`, `notes` — when re-running for an existing date. This is
   the only way to surface drift between what was suggested and what was
   actually traded.
4. Renders `rebalance_history_summary.md` next to the CSV with:
   - last 30 rows in chronological order
   - per-ticker aggregate (sum of suggested tilts vs sum of executed tilts)
   - drift signal: rows where suggested-vs-executed gap > 1%

This script is *append-only* on the CSV. It never deletes rows.
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STACK_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CROSS_COMPARE_ROOT = STACK_ROOT / "reports" / "review_dashboard" / "ai_tape_cross_compare"
DEFAULT_HISTORY = STACK_ROOT / "ai_infra" / "reports" / "rebalance_history.csv"
DEFAULT_SUMMARY = STACK_ROOT / "ai_infra" / "reports" / "rebalance_history_summary.md"

HISTORY_FIELDS = (
    "as_of",
    "ticker",
    "company",
    "action",            # add | rotate_in | trim
    "suggested_tilt_pct",
    "rationale",
    # Operator-edited columns — never overwritten by the maintainer.
    "executed_tilt_pct",
    "executed_at",
    "notes",
)


def _load_history(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return {
            (
                (row.get("as_of") or "").strip(),
                (row.get("ticker") or "").strip().upper(),
                (row.get("action") or "").strip(),
            ): row
            for row in csv.DictReader(handle)
        }


def _load_suggestion(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _suggestion_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    as_of = payload.get("as_of") or ""
    for action in ("leaders", "rotate_in", "trim"):
        for item in payload.get(action) or []:
            action_label = item.get("action") or action.rstrip("s")
            tilt = item.get("tilt_pct")
            try:
                tilt_text = f"{float(tilt):+.2f}" if tilt is not None else ""
            except (TypeError, ValueError):
                tilt_text = ""
            out.append(
                {
                    "as_of": as_of,
                    "ticker": (item.get("ticker") or "").upper(),
                    "company": item.get("company") or "",
                    "action": action_label,
                    "suggested_tilt_pct": tilt_text,
                    "rationale": item.get("rationale") or "",
                    "executed_tilt_pct": "",
                    "executed_at": "",
                    "notes": "",
                }
            )
    return out


def _merge_rows(
    history: dict[tuple[str, str, str], dict[str, str]],
    new_rows: list[dict[str, str]],
) -> tuple[int, int]:
    added = 0
    refreshed = 0
    for row in new_rows:
        key = (row["as_of"], row["ticker"], row["action"])
        existing = history.get(key)
        if existing is None:
            history[key] = row
            added += 1
            continue
        # Preserve operator-edited fields when the maintainer re-runs.
        merged = dict(existing)
        for field in ("suggested_tilt_pct", "rationale", "company"):
            if row.get(field):
                if merged.get(field) != row[field]:
                    refreshed += 1
                merged[field] = row[field]
        history[key] = merged
    return added, refreshed


def _write_history(path: Path, rows: dict[tuple[str, str, str], dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows.values(), key=lambda r: (r.get("as_of") or "", r.get("ticker") or "", r.get("action") or ""))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDS)
        writer.writeheader()
        for row in ordered:
            writer.writerow({key: row.get(key, "") for key in HISTORY_FIELDS})


def _parse_pct(value: str | None) -> float | None:
    if not value:
        return None
    text = str(value).replace("%", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def render_summary(history: dict[tuple[str, str, str], dict[str, str]], as_of: str) -> str:
    rows = sorted(history.values(), key=lambda r: (r.get("as_of") or "", r.get("ticker") or "", r.get("action") or ""))
    by_ticker: dict[str, dict[str, float | int]] = {}
    drift_rows: list[tuple[str, str, str, float, float, float]] = []
    for row in rows:
        ticker = row.get("ticker") or ""
        action = row.get("action") or ""
        suggested = _parse_pct(row.get("suggested_tilt_pct")) or 0.0
        executed = _parse_pct(row.get("executed_tilt_pct"))
        bucket = by_ticker.setdefault(
            ticker,
            {"n_suggestions": 0, "n_executions": 0, "sum_suggested": 0.0, "sum_executed": 0.0},
        )
        bucket["n_suggestions"] += 1
        bucket["sum_suggested"] += suggested
        if executed is not None:
            bucket["n_executions"] += 1
            bucket["sum_executed"] += executed
            drift = suggested - executed
            if abs(drift) >= 1.0:
                drift_rows.append((row.get("as_of") or "", ticker, action, suggested, executed, drift))

    lines: list[str] = [
        f"# AI Rebalance Suggestion vs Execution Ledger - {as_of}",
        "",
        "- 数据源: `ai_tape_cross_compare/<date>/rebalance_suggestion.json` → `rebalance_history.csv`.",
        "- 状态: maintainer 只追加新行；`executed_tilt_pct` / `executed_at` / `notes` 由操作员手填，maintainer 不覆盖。",
        "- 用法: 看建议 vs 实际差距，发现「系统说加但没加」/「系统说减但没减」的执行漂移。",
        "",
        f"- 历史行数: {len(rows)}",
        f"- 跟踪 ticker 数: {len(by_ticker)}",
        f"- 显著漂移 (|diff| ≥ 1%): {len(drift_rows)}",
        "",
        "## Last 30 Suggestions",
        "",
        "| As-of | Ticker | Action | Suggested | Executed | Notes |",
        "|---|---|---|---:|---:|---|",
    ]
    for row in rows[-30:]:
        executed = _parse_pct(row.get("executed_tilt_pct"))
        executed_text = f"{executed:+.2f}%" if executed is not None else "-"
        suggested = _parse_pct(row.get("suggested_tilt_pct"))
        suggested_text = f"{suggested:+.2f}%" if suggested is not None else "-"
        lines.append(
            f"| {row.get('as_of') or '-'} | {row.get('ticker') or '-'} | {row.get('action') or '-'} | "
            f"{suggested_text} | {executed_text} | {(row.get('notes') or '')[:60]} |"
        )
    if not rows:
        lines.append("| - | - | _暂无 rebalance 建议历史_ | - | - | - |")
    lines.append("")

    lines += [
        "## Per-Ticker Cumulative",
        "",
        "| Ticker | # Suggestions | # Executions | Σ Suggested | Σ Executed | Cumulative Drift |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    if not by_ticker:
        lines.append("| - | - | - | - | - | - |")
    for ticker in sorted(by_ticker):
        bucket = by_ticker[ticker]
        drift = bucket["sum_suggested"] - bucket["sum_executed"]
        lines.append(
            "| "
            + " | ".join(
                [
                    ticker,
                    str(int(bucket["n_suggestions"])),
                    str(int(bucket["n_executions"])),
                    f"{bucket['sum_suggested']:+.2f}%",
                    f"{bucket['sum_executed']:+.2f}%" if bucket["n_executions"] else "-",
                    f"{drift:+.2f}%" if bucket["n_executions"] else "_no_execution_",
                ]
            )
            + " |"
        )
    lines.append("")

    if drift_rows:
        lines += [
            "## Significant Drift Rows (|diff| ≥ 1%)",
            "",
            "| As-of | Ticker | Action | Suggested | Executed | Drift |",
            "|---|---|---|---:|---:|---:|",
        ]
        for as_of_text, ticker, action, suggested, executed, drift in drift_rows[-30:]:
            lines.append(
                f"| {as_of_text} | {ticker} | {action} | "
                f"{suggested:+.2f}% | {executed:+.2f}% | {drift:+.2f}% |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--cross-compare-root", type=Path, default=DEFAULT_CROSS_COMPARE_ROOT)
    parser.add_argument("--suggestion-json", type=Path, default=None)
    parser.add_argument("--history-csv", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    suggestion_path = args.suggestion_json or (args.cross_compare_root / as_of / "rebalance_suggestion.json")
    payload = _load_suggestion(suggestion_path)
    if payload is None:
        print(f"warn: no rebalance suggestion at {suggestion_path}; nothing to append", file=sys.stderr)
        # Still re-render summary if history exists.
        history = _load_history(args.history_csv)
        if history:
            args.summary_md.parent.mkdir(parents=True, exist_ok=True)
            args.summary_md.write_text(render_summary(history, as_of), encoding="utf-8")
        return 0

    history = _load_history(args.history_csv)
    if args.history_csv.exists() and not args.no_backup:
        shutil.copy2(args.history_csv, args.history_csv.with_suffix(args.history_csv.suffix + ".bak"))

    new_rows = _suggestion_rows(payload)
    added, refreshed = _merge_rows(history, new_rows)
    _write_history(args.history_csv, history)
    args.summary_md.parent.mkdir(parents=True, exist_ok=True)
    args.summary_md.write_text(render_summary(history, as_of), encoding="utf-8")
    print(
        f"Rebalance history updated: {args.history_csv}; new={added}, refreshed={refreshed}, total_rows={len(history)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
