"""One-shot CLI for recording rebalance execution decisions.

This is the antidote to "having to hand-edit `rebalance_history.csv` every day".

Examples:

    # Accept every suggestion as-is.
    python3 scripts/record_rebalance_execution.py --as-of 2026-05-13 --accept-all

    # Accept some tickers at suggested tilts; reject the rest implicitly.
    python3 scripts/record_rebalance_execution.py \
        --as-of 2026-05-13 --accept NVDA AAOI

    # Override specific tilts (decimal percentage, sign required).
    python3 scripts/record_rebalance_execution.py \
        --as-of 2026-05-13 --override AAOI=+1.5 --override ANET=-1.0

    # Explicitly reject (records executed_tilt_pct = 0 so summary stops nagging).
    python3 scripts/record_rebalance_execution.py \
        --as-of 2026-05-13 --reject NVDA --notes "earnings risk"

Rules:
- The recorder only touches `executed_tilt_pct`, `executed_at`, and `notes` —
  never the suggested side of the row.
- For `--accept-all` and `--accept`, executed_tilt_pct copies the
  suggested_tilt_pct verbatim.
- `--override` and `--reject` win over `--accept-all` when they touch the
  same ticker.
- After writing the CSV, the script re-renders `rebalance_history_summary.md`
  by delegating to `maintain_rebalance_history.render_summary`.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "scripts"))

from maintain_rebalance_history import (  # noqa: E402
    DEFAULT_HISTORY,
    DEFAULT_SUMMARY,
    HISTORY_FIELDS,
    _load_history,
    _write_history,
    render_summary,
)


def _parse_override(spec: str) -> tuple[str, float]:
    if "=" not in spec:
        raise argparse.ArgumentTypeError(f"override must be TICKER=PCT (got {spec!r})")
    ticker, value = spec.split("=", 1)
    ticker = ticker.strip().upper()
    text = value.strip().replace("%", "")
    try:
        pct = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid percentage in {spec!r}: {exc}") from exc
    return ticker, pct


def _fmt_pct(value: float) -> str:
    return f"{value:+.2f}"


def _apply(
    history: dict[tuple[str, str, str], dict[str, str]],
    as_of: str,
    *,
    accept_all: bool,
    accept: set[str],
    overrides: dict[str, float],
    rejects: set[str],
    notes: str,
    executed_at: str,
) -> tuple[int, list[str]]:
    """Mutate history in place and return (touched_count, warnings)."""
    touched = 0
    warnings: list[str] = []
    keys_for_date = [key for key in history if key[0] == as_of]
    if not keys_for_date:
        warnings.append(f"no rebalance_history rows for as_of={as_of}; run maintain_rebalance_history first")
        return 0, warnings

    tickers_for_date = {key[1] for key in keys_for_date}
    for unknown in (accept | set(overrides) | rejects) - tickers_for_date:
        warnings.append(f"ticker {unknown} not in suggestion for {as_of}; ignored")

    for key in keys_for_date:
        as_of_text, ticker, action = key
        row = history[key]
        decision: tuple[str, str] | None = None  # (executed_tilt_pct, source_label)
        if ticker in overrides:
            decision = (_fmt_pct(overrides[ticker]), f"override:{overrides[ticker]:+.2f}")
        elif ticker in rejects:
            decision = ("0.00", "reject")
        elif ticker in accept or accept_all:
            suggested = row.get("suggested_tilt_pct") or ""
            decision = (suggested, "accept")
        if decision is None:
            continue
        executed_value, label = decision
        row["executed_tilt_pct"] = executed_value
        row["executed_at"] = executed_at
        # Merge notes: keep prior text, append new label and optional message.
        prior = (row.get("notes") or "").strip()
        suffix = label if not notes else f"{label}; {notes}"
        if prior and label not in prior:
            row["notes"] = f"{prior}; {suffix}"
        else:
            row["notes"] = suffix
        touched += 1
    return touched, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=None, help="Date to record. Defaults to today (Asia/Shanghai).")
    parser.add_argument("--history-csv", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--accept-all", action="store_true", help="Mark every suggestion for the date as executed at suggested tilt.")
    parser.add_argument("--accept", nargs="*", default=[], help="Tickers to accept at suggested tilt.")
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        type=_parse_override,
        help="Override executed tilt: TICKER=+1.5 (sign required). Can repeat.",
    )
    parser.add_argument("--reject", nargs="*", default=[], help="Tickers explicitly not executed (executed_tilt_pct=0).")
    parser.add_argument("--notes", default="", help="Free-text note appended to every touched row.")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    if not (args.accept_all or args.accept or args.override or args.reject):
        parser.error("at least one of --accept-all / --accept / --override / --reject is required")

    cst = datetime.now(timezone(timedelta(hours=8)))
    as_of = args.as_of or cst.date().isoformat()
    executed_at = cst.strftime("%Y-%m-%dT%H:%M")

    if not args.history_csv.exists():
        print(
            f"error: {args.history_csv} does not exist; run maintain_rebalance_history first",
            file=sys.stderr,
        )
        return 2

    history = _load_history(args.history_csv)
    if not args.no_backup:
        shutil.copy2(args.history_csv, args.history_csv.with_suffix(args.history_csv.suffix + ".bak"))

    accept_set = {t.strip().upper() for t in args.accept if t and t.strip()}
    overrides = {ticker: pct for ticker, pct in args.override}
    rejects = {t.strip().upper() for t in args.reject if t and t.strip()}

    touched, warnings = _apply(
        history,
        as_of,
        accept_all=args.accept_all,
        accept=accept_set,
        overrides=overrides,
        rejects=rejects,
        notes=args.notes.strip(),
        executed_at=executed_at,
    )
    for warning in warnings:
        print(f"warn: {warning}", file=sys.stderr)

    _write_history(args.history_csv, history)
    args.summary_md.parent.mkdir(parents=True, exist_ok=True)
    args.summary_md.write_text(render_summary(history, as_of), encoding="utf-8")
    print(f"Rebalance execution recorded for {as_of}: touched={touched} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
