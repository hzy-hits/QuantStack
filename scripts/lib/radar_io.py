"""Shared I/O harness for the score_* radar scripts.

Every radar does the same plumbing: resolve an as-of date (defaulting to
today in CST), then write {name}.json + {name}.md under
reports/review_dashboard/{radar}/{as_of}/. That was ~12 lines of
identical boilerplate copy-pasted into each script — here it is once.

This harness covers ONLY the I/O plumbing. Each radar keeps its own
(genuinely different) scoring logic — there is nothing to share there.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

CST = timezone(timedelta(hours=8))


def resolve_as_of(arg: str | None) -> tuple[date, str]:
    """Return (as_of_date, as_of_text); default to the current CST date."""
    text = arg or datetime.now(CST).date().isoformat()
    return date.fromisoformat(text), text  # fromisoformat also validates


def write_radar_outputs(
    output_root: Path,
    as_of_text: str,
    name: str,
    payload: dict[str, Any],
    markdown: str,
) -> Path:
    """Write {name}.json + {name}.md under output_root/{as_of_text}/.

    JSON is sorted-keys / indent-2 / unicode-preserving — the convention
    the radars already converged on. Returns the output directory.
    """
    out_dir = output_root / as_of_text
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{name}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (out_dir / f"{name}.md").write_text(markdown, encoding="utf-8")
    return out_dir
