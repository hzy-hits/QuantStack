"""Build a point-in-time AI-infra universe membership ledger.

The backtest holds today's production pool fixed across ~2 years — pure
survivorship bias. The universe file is only days old and carries no
per-symbol entry dates, so this bias *cannot* be fixed retroactively.

What this CAN do: capture membership point-in-time from now on. It rebuilds
`ai_infra/data/universe_membership_history.jsonl` from every git commit that
touched the universe file, plus the live working tree. Run it daily — each
universe edit becomes a dated, immutable snapshot, and the backtest reads
the snapshot in effect on each historical day instead of today's pool.

Coverage today is near-zero (the universe file is ~days old); the value is
that it grows. Membership classification uses the *current* production-pool
gate (`ai_infra_universe.is_production_grade`) applied to each historical
file — i.e. "by today's rules, who was in the file then".
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

STACK_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics import ai_infra_universe as aiu  # noqa: E402

UNIVERSE_REL = "ai_infra/data/global_universe_v2.jsonl"
DEFAULT_LEDGER = STACK_ROOT / "ai_infra" / "data" / "universe_membership_history.jsonl"
MARKETS = ("US", "CN")


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(STACK_ROOT), *args],
        capture_output=True, text=True, check=True,
    ).stdout


def _universe_commits() -> list[tuple[str, str]]:
    """[(commit, date_iso)] oldest-first for every commit touching the file."""
    out = _git("log", "--follow", "--format=%H %ad", "--date=short", "--", UNIVERSE_REL)
    rows: list[tuple[str, str]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        commit, day = line.split()
        rows.append((commit, day))
    return list(reversed(rows))  # oldest first


def members_from_content(content: str) -> dict[str, list[str]]:
    """Production-pool symbols per market for a universe-file snapshot.

    The content is written to a throwaway ai_infra root so the real
    `records_by_symbol` production gate runs unchanged on it.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "data").mkdir()
        (root / "data" / "global_universe_v2.jsonl").write_text(content, encoding="utf-8")
        return {
            mkt: sorted(aiu.records_by_symbol(mkt, root, pool="production"))
            for mkt in MARKETS
        }


def _rows(snapshot_date: str, source: str, members: dict[str, list[str]]) -> list[dict[str, Any]]:
    return [
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "market": mkt,
            "pool": "production",
            "count": len(members[mkt]),
            "symbols": members[mkt],
        }
        for mkt in MARKETS
    ]


def build_ledger() -> list[dict[str, Any]]:
    """Full ledger: one (date, market) row per git snapshot + a live row."""
    rows: list[dict[str, Any]] = []
    last_members: dict[str, list[str]] | None = None
    for commit, day in _universe_commits():
        members = members_from_content(_git("show", f"{commit}:{UNIVERSE_REL}"))
        rows += _rows(day, f"git:{commit[:9]}", members)
        last_members = members

    # Live working tree — captures uncommitted universe edits. Recorded only
    # when it actually differs from the newest git snapshot (no noise rows).
    live_path = STACK_ROOT / UNIVERSE_REL
    if live_path.exists():
        live = members_from_content(live_path.read_text(encoding="utf-8"))
        if last_members is None or any(live[m] != last_members[m] for m in MARKETS):
            rows += _rows(date.today().isoformat(), "worktree", live)

    rows.sort(key=lambda r: (r["snapshot_date"], r["market"]))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    args = parser.parse_args()

    rows = build_ledger()
    args.ledger.parent.mkdir(parents=True, exist_ok=True)
    with args.ledger.open("w", encoding="utf-8") as handle:
        handle.write(f"# generated {datetime.now().isoformat(timespec='seconds')} "
                     f"by snapshot_universe_membership.py\n")
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    dates = sorted({r["snapshot_date"] for r in rows})
    print(f"universe membership ledger → {args.ledger}")
    print(f"  {len(dates)} snapshot date(s): {dates[0]} .. {dates[-1]}")
    for row in rows:
        if row["snapshot_date"] == dates[-1]:
            print(f"  latest {row['market']}: {row['count']} names")
    return 0


if __name__ == "__main__":
    sys.exit(main())
