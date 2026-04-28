#!/bin/bash
# Precompute CN review history and stable alpha bulletin outside the report email path.
#
# Intended for early-morning cron. It does not run agents or send email.
#
# Usage:
#   ./scripts/precompute_alpha.sh
#   ./scripts/precompute_alpha.sh 2026-04-27

set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
STACK_ROOT="${QUANT_STACK_ROOT:-$(cd "$PROJ_DIR/.." && pwd)}"
DATE="${1:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}"
# This job is meant to run before the email path, so default to a full
# evidence window. The daily email pipeline keeps its smaller post-email
# maintenance window for latency.
REVIEW_BACKFILL_DAYS="${QUANT_CN_REVIEW_BACKFILL_DAYS:-90}"

cd "$PROJ_DIR"

echo "=========================================="
echo "  A-Share Alpha Precompute"
echo "  Date:  $DATE"
echo "  Start: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
echo "=========================================="

if [[ ! -x target/release/quant-cn ]]; then
    echo "  Building quant-cn release binary..."
    cargo build --release
fi

if [[ "$REVIEW_BACKFILL_DAYS" =~ ^[0-9]+$ ]] && [[ "$REVIEW_BACKFILL_DAYS" -gt 0 ]]; then
    REVIEW_FROM="$("$PYTHON_BIN" - "$DATE" "$REVIEW_BACKFILL_DAYS" <<'PY'
import sys
from datetime import date, timedelta

as_of = date.fromisoformat(sys.argv[1])
days = int(sys.argv[2])
print((as_of - timedelta(days=days)).isoformat())
PY
)"
    echo "  Review backfill window: ${REVIEW_FROM} -> ${DATE} (${REVIEW_BACKFILL_DAYS} calendar days)"
    ./target/release/quant-cn review-backfill --date-from "$REVIEW_FROM" --date-to "$DATE"
else
    echo "  Review backfill skipped (QUANT_CN_REVIEW_BACKFILL_DAYS=${REVIEW_BACKFILL_DAYS})"
fi

if [[ -n "${QUANT_STACK_BIN:-}" ]]; then
    ALPHA_BIN=("$QUANT_STACK_BIN")
elif [[ -x "$STACK_ROOT/target/release/quant-stack" ]]; then
    ALPHA_BIN=("$STACK_ROOT/target/release/quant-stack")
elif [[ -x "$STACK_ROOT/target/debug/quant-stack" ]]; then
    ALPHA_BIN=("$STACK_ROOT/target/debug/quant-stack")
else
    ALPHA_BIN=(cargo run --quiet --manifest-path "$STACK_ROOT/Cargo.toml" --bin quant-stack --)
fi

echo "  Emitting stable alpha bulletin..."
"${ALPHA_BIN[@]}" alpha evaluate \
    --date "$DATE" \
    --lookback-days 30 \
    --auto-select \
    --emit-bulletin \
    --history-db "$STACK_ROOT/data/strategy_backtest_history.duckdb" \
    --output-root "$PROJ_DIR/reports/review_dashboard/strategy_backtest" \
    --us-db "$STACK_ROOT/quant-research-v1/data/quant.duckdb" \
    --cn-db "$PROJ_DIR/data/quant_cn_report.duckdb" \
    --no-project-copies

echo "  Precompute complete: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
