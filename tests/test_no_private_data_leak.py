#!/usr/bin/env python3
"""Lightweight private-data leak test for public-ready files."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

PRIVATE_PATTERNS = [
    "api_key",
    "secret",
    "cookie",
    "portfolio_value",
    "broker token",
]

# These patterns are allowed in private notes, but should not appear in public docs.
PUBLIC_PATHS = [
    ROOT / "README.md",
    ROOT / "DISCLAIMER.md",
    ROOT / "START_HERE.md",
    ROOT / "docs",
    ROOT / "scripts",
    ROOT / "tests",
]

ALLOWLIST_FILES = {
    "docs/data-security-rules.md",
    "docs/public-private-boundary.md",
    "docs/github-repo-operating-model.md",
    "tests/test_no_private_data_leak.py",
}


def iter_files(path: Path):
    if path.is_file():
        yield path
        return
    for file in path.rglob("*"):
        if file.is_file() and ".git" not in file.parts:
            yield file


def main() -> None:
    failures: list[str] = []
    for path in PUBLIC_PATHS:
        if not path.exists():
            continue
        for file in iter_files(path):
            rel = str(file.relative_to(ROOT))
            if rel in ALLOWLIST_FILES:
                continue
            if file.suffix.lower() not in {".md", ".py", ".txt", ".toml", ".yml", ".yaml", ".json", ""}:
                continue
            text = file.read_text(encoding="utf-8", errors="ignore").lower()
            for pattern in PRIVATE_PATTERNS:
                if pattern.lower() in text:
                    failures.append(f"{rel} contains {pattern!r}")
    if failures:
        raise SystemExit("Private-data leak check failed:\n" + "\n".join(failures))
    print("private-data leak check passed")


if __name__ == "__main__":
    main()
