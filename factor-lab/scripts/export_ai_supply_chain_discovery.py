#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.autoresearch.ai_supply_chain import DEFAULT_EXPORT_ROOT, export_discovery_bundle


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Factor Lab AI supply-chain discovery queue.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--log", action="append", type=Path, default=None, help="Optional autoresearch.jsonl path.")
    args = parser.parse_args()

    bundle = export_discovery_bundle(output_dir=args.output_dir, log_paths=args.log)
    print(f"AI supply-chain discovery markdown: {bundle['markdown']}")
    print(f"AI supply-chain discovery json: {bundle['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
