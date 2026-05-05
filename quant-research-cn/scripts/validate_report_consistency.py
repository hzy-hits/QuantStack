#!/usr/bin/env python3
"""Patch report facts that must match deterministic payload values."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover - production env has duckdb
    duckdb = None


RSI_PAYLOAD_RE = re.compile(
    r"沪深300 RSI14=(?P<hs300>[^，\n]+)，"
    r"上证50 RSI14=(?P<sz50>[^，\n]+)，"
    r"创业板 RSI14=(?P<gem>[^，\n]+)"
)

RSI_REPORT_RE = re.compile(
    r"沪深300 RSI14=[^，。；]+、上证50(?: RSI14)?=[^，。；]+、创业板(?: RSI14)?=[^，。；]+，[^。；\n]*"
)

BOLD_A_SHARE_CODE_RE = re.compile(r"\*\*(?P<code>\d{6}\.(?:SZ|SH))\*\*")
PAYLOAD_TABLE_NAME_RE = re.compile(
    r"^\|\s*(?P<code>\d{6}\.(?:SZ|SH))\s*\|\s*(?P<name>[^|\n]+?)\s*\|",
    re.MULTILINE,
)
PAYLOAD_BOLD_NAME_RE = re.compile(
    r"\*\*(?P<code>\d{6}\.(?:SZ|SH))\s+(?P<name>[\u4e00-\u9fffA-Za-z0-9＊*Ａ-Ｚａ-ｚ]+)\*\*"
)


def _parse_float(raw: str) -> float | None:
    try:
        return float(raw.strip().rstrip("%"))
    except ValueError:
        return None


def _rsi_state(values: list[float]) -> str:
    high = max(values)
    low = min(values)
    if high >= 78.0:
        return "已经进入高温区"
    if high >= 70.0:
        return "偏热，需要降低追价容忍度"
    if low >= 55.0:
        return "偏暖但未到极端过热"
    if high <= 45.0:
        return "偏冷，适合关注恐慌修复而不是追涨"
    return "处在中性区间"


def sync_market_rsi(report_path: Path, macro_payload_path: Path) -> bool:
    payload = macro_payload_path.read_text(encoding="utf-8")
    match = RSI_PAYLOAD_RE.search(payload)
    if not match:
        raise SystemExit(f"Cannot find market RSI inputs in {macro_payload_path}")

    hs300 = match.group("hs300").strip()
    sz50 = match.group("sz50").strip()
    gem = match.group("gem").strip()
    numeric = [_parse_float(v) for v in (hs300, sz50, gem)]
    if any(v is None for v in numeric):
        return False

    replacement = (
        f"沪深300 RSI14={hs300}、上证50={sz50}、创业板={gem}，"
        f"{_rsi_state([v for v in numeric if v is not None])}"
    )

    report = report_path.read_text(encoding="utf-8")
    patched, count = RSI_REPORT_RE.subn(replacement, report, count=1)
    if count == 0:
        return False
    if patched != report:
        report_path.write_text(patched, encoding="utf-8")
        return True
    return False


def load_stock_names(report_db_path: Path) -> dict[str, str]:
    if duckdb is None or not report_db_path.exists():
        return {}
    try:
        con = duckdb.connect(str(report_db_path), read_only=True)
    except Exception as exc:
        print(f"Stock name sync skipped: cannot open {report_db_path}: {exc}")
        return {}
    try:
        rows = con.execute(
            "SELECT ts_code, name FROM stock_basic WHERE name IS NOT NULL AND name <> ''"
        ).fetchall()
    finally:
        con.close()
    return {str(code): str(name) for code, name in rows}


def load_payload_stock_names(payload_paths: list[Path]) -> dict[str, str]:
    names: dict[str, str] = {}
    for path in payload_paths:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in PAYLOAD_TABLE_NAME_RE.finditer(text):
            name = match.group("name").strip()
            if name and name not in {"名称", "-"}:
                names.setdefault(match.group("code"), name)
        for match in PAYLOAD_BOLD_NAME_RE.finditer(text):
            names.setdefault(match.group("code"), match.group("name").strip())
    return names


def sync_stock_code_names(
    report_path: Path,
    report_db_path: Path | None,
    name_payload_paths: list[Path],
) -> bool:
    stock_names = load_payload_stock_names(name_payload_paths)
    if report_db_path is None:
        default_db = report_path.parents[1] / "data" / "quant_cn_report.duckdb"
        report_db_path = default_db
    stock_names.update(load_stock_names(report_db_path))
    if not stock_names:
        return False

    report = report_path.read_text(encoding="utf-8")

    def repl(match: re.Match[str]) -> str:
        code = match.group("code")
        name = stock_names.get(code)
        if not name:
            return match.group(0)
        return f"**{code} {name}**"

    patched = BOLD_A_SHARE_CODE_RE.sub(repl, report)
    for code, name in stock_names.items():
        patched = patched.replace(f"**{code} {name}** {name}", f"**{code} {name}**")
        patched = patched.replace(f"**{code} {name}**（{name}）", f"**{code} {name}**")
        patched = patched.replace(f"**{code} {name}**({name})", f"**{code} {name}**")

    if patched != report:
        report_path.write_text(patched, encoding="utf-8")
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--macro-payload", type=Path, required=True)
    parser.add_argument("--report-db", type=Path)
    parser.add_argument("--name-payload", type=Path, action="append", default=[])
    args = parser.parse_args()

    changed = sync_market_rsi(args.report, args.macro_payload)
    names_changed = sync_stock_code_names(args.report, args.report_db, args.name_payload)
    if changed:
        print(f"Report consistency patched: {args.report}")
    elif names_changed:
        print(f"Report stock names patched: {args.report}")
    else:
        print(f"Report consistency checked: {args.report}")


if __name__ == "__main__":
    main()
