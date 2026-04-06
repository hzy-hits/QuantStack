#!/bin/bash
# Full daily pipeline: data → split → agents → merge → email
#
# Usage:
#   ./scripts/run_full.sh                    # today, post-market (default)
#   ./scripts/run_full.sh --premarket        # today, pre-market (full data + analysis)
#   ./scripts/run_full.sh 2026-03-09         # specific date, post-market
#   ./scripts/run_full.sh --skip-data        # re-analyze only (no data fetch, manual use)
#   ./scripts/run_full.sh --skip-data --premarket  # re-analyze pre-market session
#
# On failure: sends alert email to admin, then retries once.
#
# Two sessions per trading day (both fetch fresh data):
#   post = after market close: closing prices, full-day news, final options
#   pre  = before market open: overnight news, pre-market/futures prices, fresh Polymarket
#
# Reports: {date}_report_zh_post.md / {date}_report_zh_pre.md
# Each session references the most recent prior report for hypothesis validation.
#
# Cron examples (UTC+8):
#   盘后 (ET 17:00 = UTC+8 05:00 EDT / 06:00 EST):
#     0 5 * * 2-6 cd $HOME/coding/python/quant-research-v1 && ./scripts/run_full.sh >> logs/cron_postmarket.log 2>&1
#   盘前 (ET 08:00 = UTC+8 20:00 EDT / 21:00 EST):
#     0 21 * * 1-5 cd $HOME/coding/python/quant-research-v1 && ./scripts/run_full.sh --premarket >> logs/cron_premarket.log 2>&1

set -uo pipefail  # no -e: we handle errors ourselves for retry

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCK_FILE="/tmp/quant-research-pipeline.lock"
ALERT_TO="13502448752hzy@gmail.com"
DATA_TIMEOUT=3600   # 60 min for data pipeline (802 symbols, fundamentals ~21min + prices ~5min + options + Rust)
SPLIT_TIMEOUT=60    # 1 min for split
AGENT_TIMEOUT=2400  # 40 min for all agents + merge (4 agents ~14 min + merge ~15 min)
EMAIL_TIMEOUT=120   # 2 min for email
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
CODEX_BIN="${CODEX_BIN:-codex}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

resolve_repo_dir() {
    for candidate in "$@"; do
        if [[ -n "${candidate:-}" && -d "$candidate" ]]; then
            (cd "$candidate" && pwd)
            return 0
        fi
    done
    return 1
}

STACK_ROOT="${QUANT_STACK_ROOT:-}"
FACTOR_LAB_ROOT="${FACTOR_LAB_ROOT:-$(resolve_repo_dir \
    "${STACK_ROOT:+$STACK_ROOT/factor-lab}" \
    "$PROJECT_DIR/../factor-lab" \
    "$PROJECT_DIR/../../python/factor-lab" \
    "")}"

if [[ -z "$FACTOR_LAB_ROOT" ]]; then
    echo "ERROR: factor-lab repo not found. Set FACTOR_LAB_ROOT or QUANT_STACK_ROOT."
    exit 1
fi

cd "$PROJECT_DIR"
mkdir -p logs

# ── Helper: send failure alert ──────────────────────────────────────────────
send_alert() {
    local step="$1"
    local attempt="$2"
    local detail="$3"
    local subject="[Quant Pipeline FAILED] $DATE ($SESSION) — $step (attempt $attempt)"
    local body="Pipeline failed at step: $step
Date: $DATE
Session: $SESSION
Attempt: $attempt
Time: $(date '+%Y-%m-%d %H:%M:%S %Z')

Error detail:
$detail

Log file: logs/cron_${SESSION}market.log"

    echo "  Sending failure alert to $ALERT_TO ..."
    uv run python scripts/send_alert.py \
        --to "$ALERT_TO" \
        --subject "$subject" \
        --body "$body" 2>/dev/null || echo "  WARNING: Failed to send alert email"
}

# ── Pre-flight: ensure required binaries exist in PATH ────────────────────
for bin in uv timeout; do
    command -v "$bin" >/dev/null 2>&1 || {
        echo "ERROR: '$bin' not found in PATH. Cron PATH may differ from interactive shell."
        echo "  Current PATH: $PATH"
        exit 1
    }
done
if ! command -v "$CLAUDE_BIN" >/dev/null 2>&1 && ! command -v "$CODEX_BIN" >/dev/null 2>&1; then
    echo "ERROR: neither '$CLAUDE_BIN' nor '$CODEX_BIN' is available in PATH."
    echo "  Current PATH: $PATH"
    exit 1
fi

# ── Lock: prevent concurrent runs ──────────────────────────────────────────
if [ -f "$LOCK_FILE" ]; then
    LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null)
    if kill -0 "$LOCK_PID" 2>/dev/null; then
        echo "ERROR: Pipeline already running (PID $LOCK_PID). Exiting."
        exit 1
    else
        echo "WARNING: Stale lock file (PID $LOCK_PID not running). Removing."
        rm -f "$LOCK_FILE"
    fi
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

# Parse args
DATE=""
SKIP_DATA=false
SESSION="post"
for arg in "$@"; do
    case $arg in
        --skip-data) SKIP_DATA=true ;;
        --premarket) SESSION="pre" ;;
        --*) echo "ERROR: Unknown option '$arg'"; echo "Usage: run_full.sh [--premarket|--skip-data] [YYYY-MM-DD]"; exit 1 ;;
        *)
            if [[ "$arg" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                DATE="$arg"
            else
                echo "ERROR: Invalid date format '$arg' (expected YYYY-MM-DD)"
                exit 1
            fi
            ;;
    esac
done

if [ -z "$DATE" ]; then
    # Use NY timezone for market date
    DATE=$(TZ=America/New_York date +%Y-%m-%d)
fi

# ── Main pipeline function ──────────────────────────────────────────────────
run_pipeline() {
    local attempt="$1"

    echo "=========================================="
    echo "Quant Research Pipeline — $DATE ($SESSION) [attempt $attempt]"
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "=========================================="

    # ── Find previous report for hypothesis validation ──────────────────────
    CURRENT_REPORT="reports/${DATE}_report_zh_${SESSION}.md"
    PREV_REPORT=""
    for f in $(ls -t reports/*_report*.md 2>/dev/null); do
        [ "$f" = "$CURRENT_REPORT" ] && continue
        echo "$f" | grep -q '_report_codex' && continue
        PREV_REPORT="$f"
        echo "Previous report found: $PREV_REPORT"
        break
    done
    if [ -z "$PREV_REPORT" ]; then
        echo "No previous report found (first run or no prior reports)"
    fi

    # ── 1. Data pipeline ────────────────────────────────────────────────────
    if [ "$SKIP_DATA" = false ]; then
        echo ""
        echo "[1/4] Running data pipeline ($SESSION)..."
        if ! timeout "$DATA_TIMEOUT" uv run python scripts/run_daily.py --date "$DATE" 2>&1 | tail -20; then
            return 1
        fi
    else
        echo ""
        echo "[1/4] Skipping data pipeline (--skip-data)"
    fi

    # ── 1.5 Refresh same-day US Factor Lab outputs ──────────────────────────
    echo ""
    if [ "$SESSION" = "post" ]; then
        echo "[1.5/4] Refreshing same-day US Factor Lab outputs..."
        (cd "$FACTOR_LAB_ROOT" && FACTOR_LAB_US_EXPECTED_DATE="$DATE" bash scripts/daily_factors.sh --market us) 2>&1 || echo "  Factor Lab US refresh failed (non-fatal)"
    else
        echo "[1.5/4] Skipping same-day US Factor Lab refresh for premarket session"
    fi

    # ── 1.6 Import Factor Lab promoted factors ──────────────────────────────
    echo ""
    echo "[1.6/4] Importing Factor Lab factors..."
    (cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" -m src.mining.export_to_pipeline --market us --date "$DATE") 2>&1 || echo "  Factor Lab import failed (non-fatal)"

    PAYLOAD="reports/${DATE}_payload.md"
    if [ ! -s "$PAYLOAD" ]; then
        echo "ERROR: $PAYLOAD not found or empty"
        return 1
    fi
    echo "  Payload: $(wc -c < "$PAYLOAD") bytes"

    # ── 2. Split payload ────────────────────────────────────────────────────
    echo ""
    echo "[2/4] Splitting payload..."
    if ! timeout "$SPLIT_TIMEOUT" uv run python scripts/split_payload.py --date "$DATE"; then
        return 2
    fi
    for split_f in macro structural news; do
        sf="reports/${DATE}_payload_${split_f}.md"
        if [ ! -s "$sf" ]; then
            echo "ERROR: Split file $sf is empty or missing"
            return 2
        fi
    done

    # Append Factor Lab trading signal to structural payload (AFTER split)
    STRUCT_FILE="reports/${DATE}_payload_structural.md"
    if [ -f "$STRUCT_FILE" ]; then
        echo "" >> "$STRUCT_FILE"
        echo "## Factor Lab Independent Trading Signal" >> "$STRUCT_FILE"
        echo "" >> "$STRUCT_FILE"
        echo "以下是 Factor Lab 基于滚动最优因子的独立选股建议。" >> "$STRUCT_FILE"
        echo "请将此选股列表整合到最终报告的 Core Book 或独立章节中。" >> "$STRUCT_FILE"
        echo "每只股票附带入场价、止损价、止盈价。" >> "$STRUCT_FILE"
        echo "" >> "$STRUCT_FILE"
        (cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" scripts/run_strategy.py --market us --today --date "$DATE") >> "$STRUCT_FILE" 2>&1 || echo "  Factor Lab signal injection failed"
        echo "" >> "$STRUCT_FILE"
        echo "请在最终研报中包含上述 Factor Lab 选股清单及入场/止损/止盈参数。" >> "$STRUCT_FILE"
        echo "  Factor Lab signal injected into structural payload"
    fi

    # ── 3. Run agents ───────────────────────────────────────────────────────
    echo ""
    echo "[3/4] Running 4 analysis agents (EN) + merge (ZH) [session=$SESSION]..."

    AGENT_ARGS=("$DATE" "$SESSION")
    if [ -n "$PREV_REPORT" ]; then
        AGENT_ARGS+=("$PREV_REPORT")
    fi
    if ! timeout "$AGENT_TIMEOUT" "$PROJECT_DIR/scripts/run_agents.sh" "${AGENT_ARGS[@]}"; then
        return 3
    fi

    ZH_REPORT="reports/${DATE}_report_zh_${SESSION}.md"
    if [ ! -s "$ZH_REPORT" ]; then
        echo "  ERROR: $ZH_REPORT not generated or empty"
        return 3
    fi
    echo "  Chinese report: $ZH_REPORT ($(wc -c < "$ZH_REPORT") bytes)"

    # ── 4. Send email ───────────────────────────────────────────────────────
    echo ""
    echo "[4/4] Sending email..."
    if ! timeout "$EMAIL_TIMEOUT" uv run python scripts/send_report.py --send --date "$DATE" --session "$SESSION" --lang zh; then
        return 4
    fi

    echo ""
    echo "=========================================="
    echo "Pipeline complete — $DATE ($SESSION)"
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "=========================================="
    return 0
}

# Map return codes to step names
step_name() {
    case $1 in
        1) echo "data pipeline" ;;
        2) echo "split payload" ;;
        3) echo "agent analysis" ;;
        4) echo "email send" ;;
        *) echo "unknown (code $1)" ;;
    esac
}

# ── Run with retry ──────────────────────────────────────────────────────────
run_pipeline 1
FAIL_CODE=$?
if [ "$FAIL_CODE" -eq 0 ]; then
    exit 0
fi

FAILED_STEP=$(step_name $FAIL_CODE)
echo ""
echo "PIPELINE FAILED at step: $FAILED_STEP"

# Send alert about first failure
send_alert "$FAILED_STEP" 1 "First attempt failed. Retrying in 60 seconds..."

# Wait before retry
echo "Retrying in 60 seconds..."
sleep 60

# Retry once
echo ""
echo "═══════════════════════════════════════════"
echo "  RETRY ATTEMPT"
echo "═══════════════════════════════════════════"

run_pipeline 2
FAIL_CODE2=$?
if [ "$FAIL_CODE2" -eq 0 ]; then
    # Retry succeeded — send success notification
    echo ""
    echo "Retry succeeded!"
    uv run python scripts/send_alert.py \
        --to "$ALERT_TO" \
        --subject "[Quant Pipeline RECOVERED] $DATE ($SESSION)" \
        --body "Pipeline recovered on retry (attempt 2).
Date: $DATE
Session: $SESSION
Time: $(date '+%Y-%m-%d %H:%M:%S %Z')
First failure was at step: $FAILED_STEP" 2>/dev/null || true
    exit 0
fi

FAILED_STEP2=$(step_name $FAIL_CODE2)
echo ""
echo "PIPELINE FAILED AGAIN at step: $FAILED_STEP2"

# Send final failure alert
send_alert "$FAILED_STEP2" 2 "Both attempts failed. Manual intervention required.
First failure: $FAILED_STEP
Second failure: $FAILED_STEP2"

exit 1
