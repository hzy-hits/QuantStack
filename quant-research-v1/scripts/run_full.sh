#!/bin/bash
# Full daily pipeline: data → split → agents → merge → email
#
# Usage:
#   ./scripts/run_full.sh                    # today, post-market (default)
#   ./scripts/run_full.sh --premarket        # today, pre-market (full data + analysis)
#   ./scripts/run_full.sh 2026-03-09         # specific date, post-market
#   ./scripts/run_full.sh --skip-data        # re-analyze only (no data fetch, manual use)
#   ./scripts/run_full.sh --skip-data --premarket  # re-analyze pre-market session
#   ./scripts/run_full.sh --prod             # send to full config recipients
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
#   盘前 (fixed UTC+8 20:00; ET 08:00 EDT / 07:00 EST):
#     0 20 * * 1-5 cd $HOME/coding/python/quant-research-v1 && ./scripts/run_full.sh --premarket >> logs/cron_premarket.log 2>&1

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
DELIVERY_MODE="${QUANT_DELIVERY_MODE:-test}"
TEST_RECIPIENT="${QUANT_TEST_RECIPIENT:-}"
for arg in "$@"; do
    case $arg in
        --skip-data) SKIP_DATA=true ;;
        --premarket) SESSION="pre" ;;
        --prod) DELIVERY_MODE="prod" ;;
        --test) DELIVERY_MODE="test" ;;
        --test-recipient=*) TEST_RECIPIENT="${arg#--test-recipient=}" ;;
        --*) echo "ERROR: Unknown option '$arg'"; echo "Usage: run_full.sh [--premarket|--skip-data|--test|--prod|--test-recipient=email] [YYYY-MM-DD]"; exit 1 ;;
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
    echo "Delivery mode: $DELIVERY_MODE"
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "=========================================="

    # ── Find previous report for hypothesis validation ──────────────────────
    CURRENT_REPORT="reports/${DATE}_report_zh_${SESSION}.md"
    PREV_REPORT="$("$PYTHON_BIN" - "$PROJECT_DIR" "$DATE" "$SESSION" <<'PY'
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

project_dir = Path(sys.argv[1])
as_of = date.fromisoformat(sys.argv[2])
session = sys.argv[3]
session_rank = {"pre": 0, "post": 1}
current_key = (as_of.isoformat(), session_rank.get(session, 1))

reports: list[tuple[tuple[str, int], Path]] = []
for path in (project_dir / "reports").glob("*_report_zh_*.md"):
    if "_report_codex" in path.name:
        continue
    stem = path.stem
    parts = stem.split("_report_zh_")
    if len(parts) != 2:
        continue
    report_date, report_session = parts
    if report_session not in session_rank:
        continue
    reports.append(((report_date, session_rank[report_session]), path))

reports.sort()
previous = [path for key, path in reports if key < current_key]
print(previous[-1] if previous else "", end="")
PY
)"
    if [ -n "$PREV_REPORT" ]; then
        echo "Previous report found: $PREV_REPORT"
    fi
    if [ -z "$PREV_REPORT" ]; then
        echo "No previous report found (first run or no prior reports)"
    fi

    # ── 1. Data pipeline ────────────────────────────────────────────────────
    if [ "$SKIP_DATA" = false ]; then
        echo ""
        echo "[1/4] Running data pipeline ($SESSION)..."
        if ! PYTHONUNBUFFERED=1 timeout "$DATA_TIMEOUT" uv run python scripts/run_daily.py --date "$DATE" --session "$SESSION"; then
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

    PAYLOAD="reports/${DATE}_payload_${SESSION}.md"
    if [ ! -s "$PAYLOAD" ]; then
        echo "ERROR: $PAYLOAD not found or empty"
        return 1
    fi
    echo "  Payload: $(wc -c < "$PAYLOAD") bytes"

    # ── 2. Split payload ────────────────────────────────────────────────────
    echo ""
    echo "[2/4] Splitting payload..."
    if ! timeout "$SPLIT_TIMEOUT" uv run python scripts/split_payload.py --date "$DATE" --session "$SESSION"; then
        return 2
    fi
    for split_f in macro structural news; do
        sf="reports/${DATE}_payload_${split_f}_${SESSION}.md"
        if [ ! -s "$sf" ]; then
            echo "ERROR: Split file $sf is empty or missing"
            return 2
        fi
    done

    # Append Factor Lab research candidates to structural payload (AFTER split)
    STRUCT_FILE="reports/${DATE}_payload_structural_${SESSION}.md"
    if [ -f "$STRUCT_FILE" ]; then
        FACTOR_TMP="$(mktemp)"
        FACTOR_STATUS="missing"
        FACTOR_TRADE_DATE=""
        FACTOR_AGE_DAYS=""
        if (cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" scripts/run_strategy.py --market us --today --date "$DATE") > "$FACTOR_TMP" 2>&1; then
            FACTOR_TRADE_DATE="$("$PYTHON_BIN" - "$FACTOR_TMP" <<'PY'
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(errors="ignore")
m = re.search(r"数据截止:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
print(m.group(1) if m else "")
PY
)"
            if [ -n "$FACTOR_TRADE_DATE" ]; then
                FACTOR_AGE_DAYS="$("$PYTHON_BIN" - "$DATE" "$FACTOR_TRADE_DATE" <<'PY'
import sys
from datetime import date

as_of = date.fromisoformat(sys.argv[1])
trade = date.fromisoformat(sys.argv[2])
print((as_of - trade).days)
PY
)"
                if [ "${FACTOR_AGE_DAYS:-999}" -le 3 ]; then
                    FACTOR_STATUS="fresh"
                else
                    FACTOR_STATUS="stale"
                fi
            else
                FACTOR_STATUS="fresh"
            fi
        else
            echo "  Factor Lab signal injection failed"
        fi

        echo "" >> "$STRUCT_FILE"
        echo "## Factor Lab Research Candidates" >> "$STRUCT_FILE"
        echo "" >> "$STRUCT_FILE"
        echo "以下是 Factor Lab 的研究候选，不是独立交易指令。" >> "$STRUCT_FILE"
        echo "它不能决定 Headline Gate、今日大盘结论或主书方向；只有通过主系统方向、execution gate、流动性和追价过滤后，才能进入主书。" >> "$STRUCT_FILE"
        if [ "$FACTOR_STATUS" = "stale" ]; then
            echo "状态: STALE。候选输出使用的最新交易日为 ${FACTOR_TRADE_DATE}，较报告日 ${DATE} 滞后 ${FACTOR_AGE_DAYS} 天。只允许放在附录，不得作为主报告确认信号。" >> "$STRUCT_FILE"
        elif [ "$FACTOR_STATUS" = "fresh" ]; then
            if [ -n "$FACTOR_TRADE_DATE" ]; then
                echo "状态: FRESH。候选输出交易日 ${FACTOR_TRADE_DATE}，可作为研究附录展示，但不得覆盖主系统结论。" >> "$STRUCT_FILE"
            else
                echo "状态: FRESH。未发现明显日期滞后，可作为研究附录展示，但不得覆盖主系统结论。" >> "$STRUCT_FILE"
            fi
        else
            echo "状态: UNAVAILABLE。候选输出失败或缺少交易日信息，忽略其方向性结论。" >> "$STRUCT_FILE"
        fi
        echo "每只股票附带参考价、风控线、观察上沿和研究权重。" >> "$STRUCT_FILE"
        echo "" >> "$STRUCT_FILE"
        if [ -s "$FACTOR_TMP" ]; then
            cat "$FACTOR_TMP" >> "$STRUCT_FILE"
        fi
        echo "" >> "$STRUCT_FILE"
        echo "最终研报只需保留状态说明和紧凑表格，不要复述整段“使用方式”说明；它不得主导 headline 或主书排序。" >> "$STRUCT_FILE"
        rm -f "$FACTOR_TMP"
        echo "  Factor Lab candidates injected into structural payload"
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
    SEND_ARGS=(--send --date "$DATE" --session "$SESSION" --lang zh --delivery-mode "$DELIVERY_MODE")
    if [[ -n "$TEST_RECIPIENT" ]]; then
        SEND_ARGS+=(--test-recipient "$TEST_RECIPIENT")
    fi
    if ! timeout "$EMAIL_TIMEOUT" uv run python scripts/send_report.py "${SEND_ARGS[@]}"; then
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
FAIL_CODE=0
run_pipeline 1 || FAIL_CODE=$?
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

FAIL_CODE2=0
run_pipeline 2 || FAIL_CODE2=$?
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
