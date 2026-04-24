from __future__ import annotations

from datetime import date
from pathlib import Path


def normalize_session(session: str | None) -> str:
    value = (session or "post").strip().lower()
    if value not in {"pre", "post"}:
        raise ValueError(f"unsupported session: {session}")
    return value


def payload_path(reports_dir: str | Path, as_of: date | str, session: str) -> Path:
    reports_root = Path(reports_dir)
    as_of_str = as_of.isoformat() if isinstance(as_of, date) else str(as_of)
    return reports_root / f"{as_of_str}_payload_{normalize_session(session)}.md"


def split_payload_path(
    reports_dir: str | Path,
    as_of: date | str,
    session: str,
    section: str,
) -> Path:
    reports_root = Path(reports_dir)
    as_of_str = as_of.isoformat() if isinstance(as_of, date) else str(as_of)
    return reports_root / f"{as_of_str}_payload_{section}_{normalize_session(session)}.md"


def charts_dir(reports_dir: str | Path, as_of: date | str, session: str) -> Path:
    reports_root = Path(reports_dir)
    as_of_str = as_of.isoformat() if isinstance(as_of, date) else str(as_of)
    return reports_root / "charts" / as_of_str / normalize_session(session)
