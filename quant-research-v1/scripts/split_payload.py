#!/usr/bin/env python3
"""
Split the daily payload into 3 focused files for parallel agent analysis.

Usage:
    python scripts/split_payload.py                      # today's date
    python scripts/split_payload.py --date 2026-03-09

Output:
    reports/{date}_payload_macro.md      (~30KB)
    reports/{date}_payload_structural.md (~90KB)
    reports/{date}_payload_news.md       (~65KB)
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path


def split_payload(payload_path: Path, output_dir: Path) -> dict[str, Path]:
    """Split a payload file into macro, structural, and news components."""
    text = payload_path.read_text(encoding="utf-8")
    lines = text.split("\n")
    stem = payload_path.stem.replace("_payload", "")

    # --- Validate required sections ---
    notable_start = None
    universe_start = None

    for i, line in enumerate(lines):
        if line.startswith("## Notable Items"):
            notable_start = i
        if line.startswith("## Universe Summary"):
            universe_start = i

    if notable_start is None:
        raise ValueError(f"Missing '## Notable Items' section in {payload_path}")
    if universe_start is None:
        raise ValueError(f"Missing '## Universe Summary' section in {payload_path}")

    # --- Detect additional section boundaries for routing ---
    scorecard_start = None
    portfolio_risk_start = None
    shared_catalysts_start = None
    options_extremes_start = None
    dividend_start = None
    coverage_start = None
    charts_start = None

    for i, line in enumerate(lines):
        if line.startswith("## Algorithm Scorecard"):
            scorecard_start = i
        elif line.startswith("## Portfolio Risk Summary"):
            portfolio_risk_start = i
        elif line.startswith("## Shared Catalysts"):
            shared_catalysts_start = i
        elif line.startswith("## Options Extremes") or line.startswith("## Top Bullish") or line.startswith("## Top Bearish"):
            if options_extremes_start is None:
                options_extremes_start = i
        elif line.startswith("## Dividend"):
            dividend_start = i
        elif line.startswith("## Data Coverage") or line.startswith("## Coverage"):
            coverage_start = i
        elif line.startswith("## Charts"):
            charts_start = i

    # --- File 1: Macro context ---
    # Market context (header through end of context), HMM regime, scorecard,
    # portfolio risk, shared catalysts, options extremes, universe summary
    macro_lines: list[str] = lines[:notable_start]

    # Append scorecard (macro analysts need to reference algorithm accuracy)
    # Append portfolio risk, shared catalysts, options extremes — all are macro-level
    # These sections live between context and notable items, so they're already in macro_lines

    # Append universe summary
    if universe_start:
        macro_lines.append("\n---\n")
        macro_lines.extend(lines[universe_start:])

    # --- File 2 & 3: Per-item structural vs news ---
    structural_lines = [
        f"# Structural Analysis Data — {stem}\n",
        "Per-item quantitative data: regime, momentum, options, probability cones.\n",
    ]
    news_lines = [
        f"# News & Events Data — {stem}\n",
        "Per-item news headlines, SEC filings, earnings events.\n",
    ]

    # Find each item block by ### N. SYMBOL pattern
    item_pattern = re.compile(r"^### \d+\. (\S+) \[")
    item_starts = [
        (i, lines[i]) for i in range(len(lines)) if item_pattern.match(lines[i])
    ]
    if not item_starts:
        raise ValueError(f"No notable items found (expected '### N. SYMBOL [' pattern) in {payload_path}")

    for idx, (start, header) in enumerate(item_starts):
        end = item_starts[idx + 1][0] if idx + 1 < len(item_starts) else len(lines)
        block = lines[start:end]

        struct_block: list[str] = []
        news_block: list[str] = []
        current_type = "struct"

        for line in block:
            # Detect section switches within each item
            if any(
                line.startswith(prefix)
                for prefix in [
                    "**Signal:",
                    "**Sources:",
                    "**Notability score:",
                    "**Price & Returns:",
                    "**Price & Momentum:",
                    "**Price:**",
                    "**Momentum Risk Analysis:",
                    "**Options Market Data:",
                    "**Options-Implied Probability Cone",
                    "**Unusual Options Activity:",
                    "**Cross-Asset Context",
                    "**Risk Parameters",
                    "**Contradiction Analysis:",
                    "**Exhaustion Flags:",
                    "**Sub-scores:",
                    "**Sentiment (Options-Derived):",
                    "**Cointegration Partners:",
                    "**Granger Causality:",
                    "**Earnings Cumulative Abnormal Return",
                    "**Kalman Dynamic Beta:",
                    "期权数据:",
                    "概率锥:",
                ]
            ):
                current_type = "struct"
            elif any(
                line.startswith(prefix)
                for prefix in [
                    "**Recent News:",
                    "**News:**",
                    "**SEC Filings",
                    "**Earnings",
                    "*News:",
                    "*Some news is shared",
                ]
            ):
                current_type = "news"

            if current_type == "struct":
                struct_block.append(line)
            else:
                news_block.append(line)

        structural_lines.extend(struct_block)
        structural_lines.append("---\n")

        if news_block:
            news_lines.append(header)
            news_lines.extend(news_block)
            news_lines.append("---\n")

    # Write output files atomically (temp → rename)
    import os
    import tempfile
    paths = {}
    temp_files: list[tuple[Path, Path]] = []
    for suffix, content in [
        ("macro", macro_lines),
        ("structural", structural_lines),
        ("news", news_lines),
    ]:
        out_path = output_dir / f"{stem}_payload_{suffix}.md"
        # Write to temp file in same directory (ensures same filesystem for rename)
        fd, tmp = tempfile.mkstemp(dir=output_dir, suffix=f"_{suffix}.tmp")
        os.close(fd)  # close fd immediately; write_text opens its own handle
        tmp_path = Path(tmp)
        try:
            tmp_path.write_text("\n".join(content), encoding="utf-8")
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        temp_files.append((tmp_path, out_path))
        paths[suffix] = out_path

    # All written successfully — atomically promote all at once
    for tmp_path, out_path in temp_files:
        tmp_path.rename(out_path)
        size_kb = out_path.stat().st_size / 1024
        content_lines = out_path.read_text().count("\n")
        print(f"  {out_path.stem.split('_payload_')[1]}: {out_path.name} ({size_kb:.1f}KB, {content_lines} lines)")

    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Split payload for parallel agents")
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()

    if args.date:
        as_of = args.date
    else:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        as_of = str(datetime.now(ZoneInfo("America/New_York")).date())
    payload_path = Path("reports") / f"{as_of}_payload.md"

    if not payload_path.exists():
        print(f"Error: {payload_path} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Splitting {payload_path}:")
    split_payload(payload_path, payload_path.parent)
    print("Done.")


if __name__ == "__main__":
    main()
