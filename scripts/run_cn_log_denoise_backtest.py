#!/usr/bin/env python3
"""Compatibility CLI for CN log-denoise diagnostics."""
from __future__ import annotations

import sys
from pathlib import Path


STACK_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_SRC = STACK_ROOT / "quant-research-v1" / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.analytics.cn_log_denoise_backtest import main  # noqa: E402


if __name__ == "__main__":
    main()
