#!/bin/bash
# Compatibility wrapper for the A-share daily pipeline.
#
# The canonical state machine is the root Rust command:
#   quant-stack daily --markets cn --run-producers --with-narrative --send-reports ...
#
# This wrapper intentionally contains no producer/render/agent/email logic. Keeping
# the old shell workflow here would create a second cron path that can bypass the
# shared alpha gate and report-model checks.

set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STACK_ROOT="${QUANT_STACK_ROOT:-$(cd "$PROJ_DIR/.." && pwd)}"

DATE=""
SLOT=""
DELIVERY_MODE="${QUANT_DELIVERY_MODE:-test}"
TEST_RECIPIENT="${QUANT_TEST_RECIPIENT:-}"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
        morning|evening|daily)
            SLOT="$arg"
            ;;
        --prod)
            DELIVERY_MODE="prod"
            ;;
        --test)
            DELIVERY_MODE="test"
            ;;
        --delivery-mode=*)
            DELIVERY_MODE="${arg#--delivery-mode=}"
            ;;
        --delivery-mode)
            shift
            if [[ $# -eq 0 ]]; then
                echo "ERROR: --delivery-mode requires test or prod"
                exit 1
            fi
            DELIVERY_MODE="$1"
            ;;
        --test-recipient=*)
            TEST_RECIPIENT="${arg#--test-recipient=}"
            ;;
        --test-recipient)
            shift
            if [[ $# -eq 0 ]]; then
                echo "ERROR: --test-recipient requires an email"
                exit 1
            fi
            TEST_RECIPIENT="$1"
            ;;
        --delivery-dry-run|--dry-run)
            EXTRA_ARGS+=("$arg")
            ;;
        [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])
            DATE="$arg"
            ;;
        *)
            echo "ERROR: Unknown argument '$arg'"
            echo "Usage: ./scripts/daily_pipeline.sh [--test|--prod|--delivery-dry-run|--dry-run|--test-recipient=email] [morning|evening|daily] [YYYY-MM-DD]"
            exit 1
            ;;
    esac
    shift
done

DATE="${DATE:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}"
SLOT="${SLOT:-$(TZ=Asia/Shanghai date +%H | awk '{print ($1 < 12) ? "morning" : "evening"}')}"

ARGS=(
    daily
    --stack-root "$STACK_ROOT"
    --date "$DATE"
    --markets cn
    --session "$SLOT"
    --run-producers
    --with-narrative
    --send-reports
    --delivery-mode "$DELIVERY_MODE"
)

if [[ -n "$TEST_RECIPIENT" ]]; then
    ARGS+=(--test-recipient "$TEST_RECIPIENT")
fi
ARGS+=("${EXTRA_ARGS[@]}")
export QUANT_DELIVERY_MODE="$DELIVERY_MODE"

if [[ -n "${QUANT_STACK_BIN:-}" ]]; then
    exec "$QUANT_STACK_BIN" "${ARGS[@]}"
fi

if [[ -x "$STACK_ROOT/target/release/quant-stack" ]]; then
    exec "$STACK_ROOT/target/release/quant-stack" "${ARGS[@]}"
fi

if [[ -x "$STACK_ROOT/target/debug/quant-stack" ]]; then
    exec "$STACK_ROOT/target/debug/quant-stack" "${ARGS[@]}"
fi

cd "$STACK_ROOT"
exec cargo run --quiet --bin quant-stack -- "${ARGS[@]}"
