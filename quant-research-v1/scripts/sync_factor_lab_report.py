#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from quant_bot.reporting.factor_lab import sync_factor_lab_signal_section


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', required=True)
    parser.add_argument('--structural', required=True)
    args = parser.parse_args()

    report_path = Path(args.report)
    structural_path = Path(args.structural)

    report_text = report_path.read_text(encoding='utf-8', errors='replace')
    structural_text = structural_path.read_text(encoding='utf-8', errors='replace')
    synced = sync_factor_lab_signal_section(report_text, structural_text)

    if synced != report_text:
        report_path.write_text(synced, encoding='utf-8')
        print(f'Synced Factor Lab signal into {report_path}')
    else:
        print('No Factor Lab signal sync changes needed')


if __name__ == '__main__':
    main()
