#!/usr/bin/env python3
"""Consolidate autoresearch runtime logs into an auditable rollup."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.paths import FACTOR_LAB_ROOT


def parse_date(raw: str | None) -> date | None:
    return date.fromisoformat(raw) if raw else None


def load_runs(path: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    if not path.exists():
        return runs
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            runs.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return runs


def run_date(run: dict[str, Any]) -> date | None:
    ts = str(run.get("ts", ""))
    if len(ts) < 10:
        return None
    try:
        return date.fromisoformat(ts[:10])
    except ValueError:
        return None


def filter_runs(
    runs: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    filtered = []
    for run in runs:
        d = run_date(run)
        if d is None:
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        filtered.append(run)
    return filtered


def dedupe_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    unique = []
    for run in sorted(runs, key=lambda r: (str(r.get("ts", "")), str(r.get("name", "")))):
        key = (
            run.get("session_id"),
            run.get("name"),
            run.get("formula"),
            run.get("ts"),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(run)
    return unique


def decision(run: dict[str, Any]) -> str:
    if run.get("decision"):
        return str(run["decision"])
    if run.get("oos") == "PASS":
        return "keep"
    if run.get("gates") == "PASS":
        return "candidate"
    return "revert"


def formula_themes(formula: str) -> list[str]:
    lower = formula.lower()
    themes = []
    if "amount/circ_market_cap" in lower or "circ_market_cap" in lower:
        themes.append("float-flow")
    if "turnover" in lower:
        themes.append("turnover")
    if "volume_ratio" in lower or "volume" in lower:
        themes.append("volume")
    if "ts_corr" in lower and "shift" in lower:
        themes.append("anti-autocorr")
    if "ts_std" in lower or "stability" in lower:
        themes.append("stability")
    if "ts_min" in lower and "ts_max" in lower:
        themes.append("corridor/evenness")
    if "pb" in lower or "pe" in lower:
        themes.append("value")
    if not themes:
        themes.append("other")
    return themes


def load_journal_sessions(market: str) -> list[dict[str, Any]]:
    journal = FACTOR_LAB_ROOT / "research_journal.md"
    if not journal.exists():
        return []
    sessions: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in journal.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("## Session ") and f"— {market.upper()}" in line:
            if current:
                sessions.append(current)
            parts = line.removeprefix("## Session ").split(" — ", 1)
            current = {"session_id": parts[0].strip(), "market": market}
            continue
        if current is None:
            continue
        stripped = line.strip()
        if stripped.startswith("- Timestamp:"):
            current["timestamp"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- Experiments run:"):
            current["experiments"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- OOS passed:"):
            current["oos_passed"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- Passing factors:"):
            current["passing_factors"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("## "):
            sessions.append(current)
            current = None
    if current:
        sessions.append(current)
    return sessions


def filter_journal_sessions(
    sessions: list[dict[str, Any]],
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    filtered = []
    for session in sessions:
        raw = str(session.get("timestamp", ""))
        try:
            d = date.fromisoformat(raw[:10])
        except ValueError:
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        filtered.append(session)
    return filtered


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def render_rollup(
    runs: list[dict[str, Any]],
    market: str,
    start: date | None,
    end: date | None,
    journal_sessions: list[dict[str, Any]] | None = None,
) -> str:
    runs = dedupe_runs(runs)
    journal_sessions = journal_sessions or []
    sessions = sorted({str(run.get("session_id", "")) for run in runs if run.get("session_id")})
    journal_only = [s for s in journal_sessions if s.get("session_id") not in set(sessions)]
    decisions = Counter(decision(run) for run in runs)
    gates_pass = sum(1 for run in runs if run.get("gates") == "PASS")
    oos_pass = sum(1 for run in runs if run.get("oos") == "PASS")
    checks_failed = sum(1 for run in runs if run.get("checks_status") == "failed")
    theme_counts = Counter()
    for run in runs:
        if run.get("gates") == "PASS":
            theme_counts.update(formula_themes(str(run.get("formula", ""))))

    per_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        per_session[str(run.get("session_id", "unknown"))].append(run)

    top_candidates = sorted(
        [run for run in runs if run.get("gates") == "PASS"],
        key=lambda run: safe_float(run.get("is_ic_ir")),
        reverse=True,
    )[:20]

    period = f"{start or 'begin'} to {end or 'end'}"
    lines = [
        f"# Autoresearch Rollup — {market.upper()}",
        "",
        f"- Period: {period}",
        f"- Detailed JSONL sessions: {len(sessions)}",
        f"- Journal-only earlier sessions: {len(journal_only)}",
        f"- Detailed experiments: {len(runs)}",
        f"- IS gates pass: {gates_pass}",
        f"- OOS pass: {oos_pass}",
        f"- Decisions: keep={decisions['keep']}, candidate={decisions['candidate']}, revert={decisions['revert']}",
        f"- Checks failed after OOS/gates: {checks_failed}",
        "",
        "## What Repeated",
        "",
    ]
    for theme, count in theme_counts.most_common(8):
        lines.append(f"- {theme}: {count} gate-pass occurrences")

    lines.extend(
        [
            "",
            "## Top Gate-Pass Candidates",
            "",
            "| # | Name | IC | IC_IR | Sharpe | Gates | OOS | Decision | Formula |",
            "|---|------|----|-------|--------|-------|-----|----------|---------|",
        ]
    )
    for idx, run in enumerate(top_candidates, 1):
        formula = str(run.get("formula", "")).replace("|", "\\|")
        lines.append(
            f"| {idx} | {run.get('name', '')} | {run.get('is_ic', '')} | "
            f"{run.get('is_ic_ir', '')} | {run.get('is_sharpe', '')} | "
            f"{run.get('gates', '')} | {run.get('oos', '')} | {decision(run)} | `{formula}` |"
        )

    lines.extend(["", "## Session Summary", ""])
    lines.extend(["| Session | Date | Runs | Gates Pass | OOS Pass | Candidates | Reverts |", "|---|---:|---:|---:|---:|---:|---:|"])
    for session_id in sessions:
        sruns = per_session[session_id]
        first_date = run_date(sruns[0])
        lines.append(
            f"| {session_id} | {first_date} | {len(sruns)} | "
            f"{sum(1 for r in sruns if r.get('gates') == 'PASS')} | "
            f"{sum(1 for r in sruns if r.get('oos') == 'PASS')} | "
            f"{sum(1 for r in sruns if decision(r) == 'candidate')} | "
            f"{sum(1 for r in sruns if decision(r) == 'revert')} |"
        )

    if journal_only:
        lines.extend(["", "## Earlier Journal Sessions", ""])
        lines.extend(["| Session | Timestamp | Experiments | OOS | Passing factors |", "|---|---|---:|---|---|"])
        for session in journal_only:
            lines.append(
                f"| {session.get('session_id', '')} | {session.get('timestamp', '')} | "
                f"{session.get('experiments', '')} | {session.get('oos_passed', '')} | "
                f"{session.get('passing_factors', '')} |"
            )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The dominant CN discoveries are not classic momentum; they are stability/evenness and anti-autocorrelation structures around turnover, amount, and float-normalized flow.",
            "- Many factors pass IS gates but fail final checks or OOS. They should remain research priors until strategy EV converts them into executable paper-trade performance.",
            "- This rollup is research evidence only. It does not override the execution gate or paper-trade EV layer.",
            "",
        ]
    )
    return "\n".join(lines)


def append_journal(journal: Path, rollup_path: Path, market: str, runs: list[dict[str, Any]]) -> None:
    runs = dedupe_runs(runs)
    text = journal.read_text(encoding="utf-8") if journal.exists() else "# Factor Research Journal\n"
    marker = f"## Autoresearch Rollup — {market.upper()} 2026-04"
    if marker in text:
        return
    gates_pass = sum(1 for run in runs if run.get("gates") == "PASS")
    oos_pass = sum(1 for run in runs if run.get("oos") == "PASS")
    resolved_rollup = rollup_path.resolve()
    try:
        rollup_label = str(resolved_rollup.relative_to(FACTOR_LAB_ROOT))
    except ValueError:
        rollup_label = str(resolved_rollup)
    entry = (
        f"\n{marker}\n"
        f"- Consolidated experiments: {len(runs)}\n"
        f"- IS gates pass: {gates_pass}\n"
        f"- OOS pass: {oos_pass}\n"
        f"- Rollup: `{rollup_label}`\n"
        "- Main repeated motif: stability/evenness + anti-autocorrelation on turnover, amount, and float-normalized flow.\n"
    )
    journal.write_text(text.rstrip() + "\n" + entry, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Consolidate autoresearch logs.")
    parser.add_argument("--market", default="cn")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--rewrite-log", action="store_true")
    parser.add_argument("--append-journal", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    log_path = FACTOR_LAB_ROOT / "runtime" / "autoresearch" / args.market / "autoresearch.jsonl"
    start = parse_date(args.start)
    end = parse_date(args.end)
    all_runs = load_runs(log_path)
    filtered = filter_runs(all_runs, start=start, end=end)
    unique_filtered = dedupe_runs(filtered)

    if args.rewrite_log:
        outside = [run for run in all_runs if run not in filtered]
        merged = dedupe_runs(outside + unique_filtered)
        log_path.write_text(
            "\n".join(json.dumps(run, ensure_ascii=False, sort_keys=True) for run in merged) + "\n",
            encoding="utf-8",
        )

    output = args.output or (
        FACTOR_LAB_ROOT
        / "research_rollups"
        / f"autoresearch_{args.market}_{(start or date.today()).strftime('%Y%m')}.md"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    journal_sessions = filter_journal_sessions(
        load_journal_sessions(args.market),
        start=start,
        end=end,
    )
    output.write_text(
        render_rollup(unique_filtered, args.market, start, end, journal_sessions),
        encoding="utf-8",
    )
    if args.append_journal:
        append_journal(FACTOR_LAB_ROOT / "research_journal.md", output, args.market, unique_filtered)
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
