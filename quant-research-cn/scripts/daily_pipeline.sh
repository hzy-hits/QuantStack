#!/bin/bash
# Full daily pipeline: fetch → enrich → analytics → filter → render → agents → email
#
# Designed for cron. Logs everything to reports/logs/{date}_{slot}.log
#
# Usage:
#   ./scripts/daily_pipeline.sh          # auto-detect date (Shanghai time)
#   ./scripts/daily_pipeline.sh morning  # morning slot (8:00 AM label)
#   ./scripts/daily_pipeline.sh evening  # evening slot (6:00 PM label)

set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
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
    "$PROJ_DIR/../factor-lab" \
    "$PROJ_DIR/../../python/factor-lab" \
)}"

if [[ -z "$FACTOR_LAB_ROOT" ]]; then
    echo "ERROR: factor-lab repo not found. Set FACTOR_LAB_ROOT or QUANT_STACK_ROOT."
    exit 1
fi

DATE="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
SLOT="${1:-$(TZ=Asia/Shanghai date +%H | awk '{print ($1 < 12) ? "morning" : "evening"}')}"
LOG_DIR="$PROJ_DIR/reports/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/${DATE}_${SLOT}.log"

exec > >(tee -a "$LOG") 2>&1

echo "=========================================="
echo "  A-Share Daily Pipeline"
echo "  Date:  $DATE"
echo "  Slot:  $SLOT"
echo "  Start: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
echo "=========================================="
echo ""

cd "$PROJ_DIR"

# ── Step 0: Ensure binary is built ────────────────────────────────────────────
echo "[0/6] Checking binary..."
if [[ ! -x target/release/quant-cn ]]; then
    echo "  Building release binary..."
    cargo build --release 2>&1
fi

# ── Step 1: Start AKShare bridge (if not running) ────────────────────────────
echo "[1/6] AKShare bridge..."
if ! curl -sf http://localhost:8321/health >/dev/null 2>&1; then
    echo "  Starting AKShare bridge..."
    cd bridge && "$PYTHON_BIN" -m uvicorn akshare_bridge:app --host 0.0.0.0 --port 8321 &
    BRIDGE_PID=$!
    cd "$PROJ_DIR"
    sleep 3
    if curl -sf http://localhost:8321/health >/dev/null 2>&1; then
        echo "  Bridge started (PID $BRIDGE_PID)"
    else
        echo "  Bridge failed to start — continuing without AKShare data"
        kill $BRIDGE_PID 2>/dev/null || true
        BRIDGE_PID=""
    fi
else
    echo "  Bridge already running"
    BRIDGE_PID=""
fi

# ── Step 2: Fetch + Enrich + Analytics + Filter + Render ─────────────────────
echo "[2/6] Running pipeline (fetch → analyze → filter → render)..."
./target/release/quant-cn run 2>&1

# ── Step 2.5: Import Factor Lab promoted factors ──────────────────────────────
echo "[2.5/6] Importing Factor Lab factors..."
(cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" -m src.mining.export_to_pipeline --market cn --date "$DATE") 2>&1 || echo "  Factor Lab import failed (non-fatal)"

echo "[3/6] Enriching news with DeepSeek..."
./target/release/quant-cn enrich 2>&1 || echo "  Enrich failed (non-fatal)"

# Re-render after enrichment
echo "[4/6] Re-rendering payloads..."
./target/release/quant-cn render 2>&1

# Append Factor Lab trading signal AFTER render (so it doesn't get overwritten)
STRUCT_PAYLOAD="reports/${DATE}_payload_structural.md"
if [ -f "$STRUCT_PAYLOAD" ]; then
    echo "" >> "$STRUCT_PAYLOAD"
    echo "## Factor Lab Independent Trading Signal" >> "$STRUCT_PAYLOAD"
    echo "" >> "$STRUCT_PAYLOAD"
    echo "以下是 Factor Lab 基于滚动最优因子的独立选股建议。" >> "$STRUCT_PAYLOAD"
    echo "请将此选股列表整合到最终报告的 Core Book 或独立章节中。" >> "$STRUCT_PAYLOAD"
    echo "每只股票附带入场价、止损价、止盈价。" >> "$STRUCT_PAYLOAD"
    echo "" >> "$STRUCT_PAYLOAD"
    (cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" scripts/run_strategy.py --market cn --today --date "$DATE") >> "$STRUCT_PAYLOAD" 2>&1 || echo "  Factor Lab signal failed"
    echo "" >> "$STRUCT_PAYLOAD"
    echo "请在最终研报中包含上述 Factor Lab 选股清单及入场/止损/止盈参数。" >> "$STRUCT_PAYLOAD"
fi

# ── Step 3: Generate charts ──────────────────────────────────────────────────
echo "[5/8] Generating charts..."
"$PYTHON_BIN" scripts/generate_charts.py --date "$DATE" 2>&1 || echo "  Charts failed (non-fatal)"

# ── Step 4: Verify payload files ─────────────────────────────────────────────
echo "[6/8] Checking payload files..."
PAYLOADS_OK=1
for f in macro structural events; do
    payload="reports/${DATE}_payload_${f}.md"
    if [[ -s "$payload" ]]; then
        echo "  $payload ($(wc -c < "$payload") bytes)"
    else
        echo "  MISSING: $payload"
        PAYLOADS_OK=0
    fi
done

if [[ "$PAYLOADS_OK" -eq 0 ]]; then
    echo "  ERROR: Payload files incomplete. Aborting agent run."
    exit 1
fi

# ── Step 5: Run multi-agent report ───────────────────────────────────────────
echo "[7/8] Running multi-agent analysis..."

# Pass previous day's report for hypothesis validation (if exists)
PREV_DATE="$(TZ=Asia/Shanghai date -d "$DATE - 1 day" +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)"
PREV_REPORT="reports/${PREV_DATE}_report_zh.md"
if [[ -f "$PREV_REPORT" ]]; then
    echo "  Using previous report: $PREV_REPORT"
    SEND_EMAIL=1 bash scripts/run_agents.sh "$DATE" "$PREV_REPORT"
else
    SEND_EMAIL=1 bash scripts/run_agents.sh "$DATE"
fi

# ── Cleanup ──────────────────────────────────────────────────────────────────
if [[ -n "${BRIDGE_PID:-}" ]]; then
    echo "  Stopping AKShare bridge (PID $BRIDGE_PID)..."
    kill "$BRIDGE_PID" 2>/dev/null || true
fi

echo ""
echo "=========================================="
echo "  Pipeline complete: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
echo "  Report: reports/${DATE}_report_zh.md"
echo "=========================================="
