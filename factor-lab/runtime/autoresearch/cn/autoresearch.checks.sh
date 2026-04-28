#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/ivena/coding/quant-stack/factor-lab"
cd "$ROOT"

MARKET="cn"
uv run python eval_factor.py --show-registry --market "$MARKET" >/dev/null
uv run python eval_factor.py --eval-composite --market "$MARKET" >/dev/null
