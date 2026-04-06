#!/bin/bash
set -euo pipefail

STACK_DIR="$(cd "$(dirname "$0")" && pwd)"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT="${1:-$HOME/quant-stack-${STAMP}.tar.gz}"

tar -chzf "$OUT" -C "$STACK_DIR" \
    factor-lab \
    quant-research-cn \
    quant-research-v1 \
    env.sh \
    README.md

echo "Created migration archive: $OUT"
