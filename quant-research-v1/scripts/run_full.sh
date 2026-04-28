#!/bin/bash
# Compatibility wrapper for the US full daily pipeline.
#
# The actual state machine lives in Rust:
#   quant-stack us-daily --stack-root <repo> ...
#
# Usage:
#   ./scripts/run_full.sh                    # today, post-market (default)
#   ./scripts/run_full.sh --premarket        # today, pre-market
#   ./scripts/run_full.sh 2026-03-09         # specific date, post-market
#   ./scripts/run_full.sh --skip-data        # reuse existing payload inputs
#   ./scripts/run_full.sh --test             # test delivery only
#   ./scripts/run_full.sh --prod             # production delivery
#   ./scripts/run_full.sh --test-recipient=email@example.com

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STACK_ROOT="${QUANT_STACK_ROOT:-$(cd "$PROJECT_DIR/.." && pwd)}"
DELIVERY_MODE="${QUANT_DELIVERY_MODE:-test}"
TEST_RECIPIENT="${QUANT_TEST_RECIPIENT:-}"
ARGS=(us-daily --stack-root "$STACK_ROOT")

while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
        --skip-data)
            ARGS+=(--skip-data)
            ;;
        --premarket)
            ARGS+=(--premarket)
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
        --delivery-dry-run)
            ARGS+=(--delivery-dry-run)
            ;;
        --dry-run)
            ARGS+=(--dry-run)
            ;;
        --no-retry)
            ARGS+=(--no-retry)
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
        --*)
            echo "ERROR: Unknown option '$arg'"
            echo "Usage: run_full.sh [--premarket|--skip-data|--test|--prod|--test-recipient=email] [YYYY-MM-DD]"
            exit 1
            ;;
        *)
            if [[ "$arg" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                ARGS+=("$arg")
            else
                echo "ERROR: Invalid date format '$arg' (expected YYYY-MM-DD)"
                exit 1
            fi
            ;;
    esac
    shift
done

ARGS+=(--delivery-mode "$DELIVERY_MODE")
if [[ -n "$TEST_RECIPIENT" ]]; then
    ARGS+=(--test-recipient "$TEST_RECIPIENT")
fi

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
