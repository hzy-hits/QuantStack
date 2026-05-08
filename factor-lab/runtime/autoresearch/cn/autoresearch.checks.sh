#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/ivena/coding/quant-stack/factor-lab"
cd "$ROOT"

MARKET="cn"
echo "== show-registry =="
uv run python eval_factor.py --show-registry --market "$MARKET"
echo "== eval-composite =="
uv run python eval_factor.py --eval-composite --market "$MARKET"
