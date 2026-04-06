#!/bin/bash
# Run 4 analysis agents in parallel using claude -p (stdin), then merge.
#
# Usage:
#   ./scripts/run_agents.sh                                                 # today (Shanghai time)
#   ./scripts/run_agents.sh 2026-03-13                                      # specific date
#   ./scripts/run_agents.sh 2026-03-13 reports/2026-03-12_report_zh.md      # with previous report
#
# Args:
#   $1 = date (optional, defaults to today Shanghai time)
#   $2 = previous report path (optional, for hypothesis validation)
#
# Environment:
#   SEND_EMAIL=1  — trigger email delivery after report generation

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
DATE="${1:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}"
PREV_REPORT="${2:-}"

AGENT_TIMEOUT=600   # 10 min per specialist agent (quant prompt 44KB needs more time)
MERGE_TIMEOUT=1200  # 20 min for merge agent
MIN_AGENT_BYTES=100
MIN_MERGE_BYTES=200

CLAUDE_BIN="${CLAUDE_BIN:-claude}"
CODEX_BIN="${CODEX_BIN:-codex}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPORTS_DIR="$PROJ_DIR/reports"
PROMPTS_DIR="$PROJ_DIR/prompts"
OUT_DIR="$REPORTS_DIR/agents-${DATE}"
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
QUANT_US_ROOT="${QUANT_US_ROOT:-$(resolve_repo_dir \
    "${STACK_ROOT:+$STACK_ROOT/quant-research-v1}" \
    "$PROJ_DIR/../quant-research-v1" \
    "$PROJ_DIR/../../python/quant-research-v1" \
)}"

if [[ -z "$FACTOR_LAB_ROOT" ]]; then
    echo "ERROR: factor-lab repo not found. Set FACTOR_LAB_ROOT or QUANT_STACK_ROOT."
    exit 1
fi
if [[ -z "$QUANT_US_ROOT" ]]; then
    echo "ERROR: quant-research-v1 repo not found. Set QUANT_US_ROOT or QUANT_STACK_ROOT."
    exit 1
fi

# ── Pre-flight checks ────────────────────────────────────────────────────────
for bin in "$TIMEOUT_BIN" python3; do
    command -v "$bin" >/dev/null 2>&1 || { echo "ERROR: '$bin' not found in PATH"; exit 1; }
done
CLAUDE_AVAILABLE=0
CODEX_AVAILABLE=0
if command -v "$CLAUDE_BIN" >/dev/null 2>&1; then
    CLAUDE_AVAILABLE=1
fi
if command -v "$CODEX_BIN" >/dev/null 2>&1; then
    CODEX_AVAILABLE=1
fi
if [[ "$CLAUDE_AVAILABLE" -eq 0 && "$CODEX_AVAILABLE" -eq 0 ]]; then
    echo "ERROR: neither '$CLAUDE_BIN' nor '$CODEX_BIN' is available in PATH"
    exit 1
fi

for f in macro structural events; do
    payload="$REPORTS_DIR/${DATE}_payload_${f}.md"
    if [[ ! -s "$payload" ]]; then
        echo "ERROR: Missing or empty: $payload"
        exit 1
    fi
done

# Clean stale outputs from previous runs
rm -rf "$OUT_DIR/outputs" "$OUT_DIR/logs"
mkdir -p "$OUT_DIR/outputs" "$OUT_DIR/logs" "$OUT_DIR/prompts"
mkdir -p "$OUT_DIR/context"


run_agent_with_fallback() {
    local agent_name="$1"
    local prompt_file="$2"
    local output_file="$3"
    local log_file="$4"
    local timeout_secs="$5"
    local min_bytes="$6"

    : > "$log_file"

    if [[ "$CLAUDE_AVAILABLE" -eq 1 ]]; then
        echo "[claude] starting ${agent_name}" >> "$log_file"
        if CLAUDECODE="" "$TIMEOUT_BIN" "$timeout_secs" "$CLAUDE_BIN" -p --output-format text \
            < "$prompt_file" > "$output_file" 2>>"$log_file"; then
            local size
            size=$(wc -c < "$output_file" 2>/dev/null || echo 0)
            if [[ "$size" -ge "$min_bytes" ]]; then
                echo "[claude] success (${size} bytes)" >> "$log_file"
                echo "  [OK] ${agent_name} done (${size} bytes, backend=claude)"
                return 0
            fi
            echo "[claude] output too small (${size} bytes < ${min_bytes})" >> "$log_file"
        else
            local rc=$?
            echo "[claude] failed (exit ${rc})" >> "$log_file"
        fi
    fi

    if [[ "$CODEX_AVAILABLE" -eq 1 ]]; then
        echo "[codex] starting ${agent_name}" >> "$log_file"
        if "$TIMEOUT_BIN" "$timeout_secs" "$CODEX_BIN" exec \
            --sandbox read-only \
            --color never \
            --skip-git-repo-check \
            -C "$PROJ_DIR" \
            -o "$output_file" \
            - < "$prompt_file" >> "$log_file" 2>&1; then
            local size
            size=$(wc -c < "$output_file" 2>/dev/null || echo 0)
            if [[ "$size" -ge "$min_bytes" ]]; then
                echo "[codex] success (${size} bytes)" >> "$log_file"
                echo "  [OK] ${agent_name} done (${size} bytes, backend=codex)"
                return 0
            fi
            echo "[codex] output too small (${size} bytes < ${min_bytes})" >> "$log_file"
        else
            local rc=$?
            echo "[codex] failed (exit ${rc})" >> "$log_file"
        fi
    fi

    echo "  [FAIL] ${agent_name} failed on all available backends"
    return 1
}

echo "=== A-Share Agent Pipeline ==="
echo "  Date:    $DATE"
echo "  Project: $PROJ_DIR"
echo "  Output:  $OUT_DIR"
[[ -n "$PREV_REPORT" ]] && echo "  Prev:    $PREV_REPORT"
echo ""

python3 "$PROJ_DIR/scripts/build_agent_context.py" \
    --date "$DATE" \
    --reports-dir "$REPORTS_DIR" \
    --out-dir "$OUT_DIR/context"

# ── Build previous report context ────────────────────────────────────────────
PREV_CONTEXT=""
if [[ -n "$PREV_REPORT" ]] && [[ -f "$PREV_REPORT" ]]; then
    PREV_CONTEXT="

--- 上期报告（用于假设验证）---
$(cat "$PREV_REPORT")
--- 上期报告结束 ---

如果上期报告存在，先用2-3句话简述上期预测的兑现情况，再进行今日分析。"
fi

# ── Assemble prompts using Python (handles large payloads safely) ────────────
# This avoids bash string substitution issues with special characters in payloads.
python3 << 'PYEOF'
import sys
from pathlib import Path

date = sys.argv[1] if len(sys.argv) > 1 else ""
PYEOF

python3 - "$DATE" "$PREV_REPORT" "$PROJ_DIR" "$OUT_DIR" "$QUANT_US_ROOT" << 'BUILDEOF'
import sys
from pathlib import Path

date = sys.argv[1]
prev_report = sys.argv[2]
proj_dir = Path(sys.argv[3])
out_dir = Path(sys.argv[4])
us_root = Path(sys.argv[5])

reports_dir = proj_dir / "reports"
prompts_dir = proj_dir / "prompts"

# Read compacted agent contexts
macro_payload = (out_dir / "context" / "macro.md").read_text()
structural_payload = (out_dir / "context" / "structural.md").read_text()
events_payload = (out_dir / "context" / "events.md").read_text()

# Read US macro payload for cross-reference (if available)
us_macro_path = us_root / "reports" / f"{date}_payload_macro.md"
us_macro_payload = ""
if us_macro_path.exists():
    us_text = us_macro_path.read_text()
    # Truncate to key sections (first ~3000 chars to avoid bloat)
    lines = us_text.split("\n")
    truncated = "\n".join(lines[:120])
    us_macro_payload = f"""--- 美股宏观数据（同日，仅供跨市场参考）---
{truncated}
--- 美股宏观数据结束 ---"""
    print(f"  US macro payload found ({len(us_macro_payload)} chars)")
else:
    us_macro_payload = "(美股宏观数据不可用)"
    print(f"  US macro payload not found at {us_macro_path}")

# Read previous report if provided
prev_context = ""
if prev_report and Path(prev_report).exists():
    prev_text = Path(prev_report).read_text()
    prev_context = f"""
--- 上期报告（用于假设验证）---
{prev_text}
--- 上期报告结束 ---

如果上期报告存在，先用2-3句话简述上期预测的兑现情况，再进行今日分析。"""

# Read prompt templates
templates = {}
for name in ["macro-analyst", "quant-analyst", "event-analyst", "risk-analyst", "merge-agent"]:
    templates[name] = (prompts_dir / f"{name}.md").read_text()

# Assemble each agent prompt
replacements_per_agent = {
    "macro-analyst": {
        "{payload_macro}": macro_payload,
        "{payload_us_macro}": us_macro_payload,
        "{prev_context}": prev_context,
    },
    "quant-analyst": {
        "{payload_structural}": structural_payload,
        "{prev_context}": prev_context,
    },
    "event-analyst": {
        "{payload_events}": events_payload,
        "{prev_context}": prev_context,
    },
    "risk-analyst": {
        "{payload_structural}": structural_payload,
        "{payload_macro}": macro_payload,
        "{prev_context}": prev_context,
    },
}

for agent_name, replacements in replacements_per_agent.items():
    content = templates[agent_name]
    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)
    out_path = out_dir / "prompts" / f"{agent_name}.txt"
    out_path.write_text(content)
    print(f"  Built prompt: {agent_name} ({len(content)} chars)")

print("  All 4 agent prompts assembled.")
BUILDEOF

echo ""

# ── Launch 4 agents in parallel ──────────────────────────────────────────────
echo "  Launching 4 agents in parallel..."

(
run_agent_with_fallback \
    "macro-analyst" \
    "$OUT_DIR/prompts/macro-analyst.txt" \
    "$OUT_DIR/outputs/macro-analyst.md" \
    "$OUT_DIR/logs/macro-analyst.log" \
    "$AGENT_TIMEOUT" \
    "$MIN_AGENT_BYTES"
) &
PID_MACRO=$!

(
run_agent_with_fallback \
    "quant-analyst" \
    "$OUT_DIR/prompts/quant-analyst.txt" \
    "$OUT_DIR/outputs/quant-analyst.md" \
    "$OUT_DIR/logs/quant-analyst.log" \
    "$AGENT_TIMEOUT" \
    "$MIN_AGENT_BYTES"
) &
PID_QUANT=$!

(
run_agent_with_fallback \
    "event-analyst" \
    "$OUT_DIR/prompts/event-analyst.txt" \
    "$OUT_DIR/outputs/event-analyst.md" \
    "$OUT_DIR/logs/event-analyst.log" \
    "$AGENT_TIMEOUT" \
    "$MIN_AGENT_BYTES"
) &
PID_EVENT=$!

(
run_agent_with_fallback \
    "risk-analyst" \
    "$OUT_DIR/prompts/risk-analyst.txt" \
    "$OUT_DIR/outputs/risk-analyst.md" \
    "$OUT_DIR/logs/risk-analyst.log" \
    "$AGENT_TIMEOUT" \
    "$MIN_AGENT_BYTES"
) &
PID_RISK=$!

# ── Wait for all 4 ──────────────────────────────────────────────────────────
echo "  Waiting for agents (PIDs: $PID_MACRO $PID_QUANT $PID_EVENT $PID_RISK)..."
FAIL=0
wait $PID_MACRO || { echo "  [FAIL] macro-analyst (check $OUT_DIR/logs/macro-analyst.log)"; FAIL=1; }
wait $PID_QUANT || { echo "  [FAIL] quant-analyst (check $OUT_DIR/logs/quant-analyst.log)"; FAIL=1; }
wait $PID_EVENT || { echo "  [FAIL] event-analyst (check $OUT_DIR/logs/event-analyst.log)"; FAIL=1; }
wait $PID_RISK  || { echo "  [FAIL] risk-analyst (check $OUT_DIR/logs/risk-analyst.log)"; FAIL=1; }

if [[ "$FAIL" -eq 1 ]]; then
    echo ""
    echo "  Some agents failed. Check logs in $OUT_DIR/logs/"
    echo "  Aborting — will not merge partial agent outputs."
    exit 1
fi

# ── Verify outputs are non-empty (≥100 bytes) + retry on failure ─────────────
MIN_BYTES=100
RETRY_LIST=()
for f in macro-analyst quant-analyst event-analyst risk-analyst; do
    fsize=$(wc -c < "$OUT_DIR/outputs/$f.md" 2>/dev/null || echo 0)
    if [[ "$fsize" -lt "$MIN_BYTES" ]]; then
        echo "  WARN: $f.md too small (${fsize} bytes < ${MIN_BYTES}) — will retry"
        RETRY_LIST+=("$f")
    fi
done

# Retry failed agents sequentially (one at a time to avoid concurrency issues)
for f in "${RETRY_LIST[@]}"; do
    echo "  Retrying $f..."
    run_agent_with_fallback \
        "$f" \
        "$OUT_DIR/prompts/$f.txt" \
        "$OUT_DIR/outputs/$f.md" \
        "$OUT_DIR/logs/$f.log" \
        "$AGENT_TIMEOUT" \
        "$MIN_AGENT_BYTES" || true
    fsize=$(wc -c < "$OUT_DIR/outputs/$f.md" 2>/dev/null || echo 0)
    if [[ "$fsize" -lt "$MIN_BYTES" ]]; then
        echo "  ERROR: $f still failed after retry (${fsize} bytes)"
    else
        echo "  [OK] $f retry succeeded (${fsize} bytes)"
    fi
done

# Final check
MISSING=0
for f in macro-analyst quant-analyst event-analyst risk-analyst; do
    fsize=$(wc -c < "$OUT_DIR/outputs/$f.md" 2>/dev/null || echo 0)
    if [[ "$fsize" -lt "$MIN_BYTES" ]]; then
        echo "  ERROR: $f.md is empty or too small (${fsize} bytes)"
        MISSING=1
    fi
done
if [[ "$MISSING" -eq 1 ]]; then
    echo "  Aborting merge — cannot merge with missing agent outputs."
    exit 1
fi

echo ""
echo "  All 4 agents completed successfully."

# ── Agent 5: Merge (sequential) ──────────────────────────────────────────────
echo "  Building merge prompt..."

# Assemble merge prompt using Python for safe substitution
python3 - "$DATE" "$PREV_REPORT" "$PROJ_DIR" "$OUT_DIR" << 'MERGEEOF'
import sys
from pathlib import Path

date = sys.argv[1]
prev_report = sys.argv[2]
proj_dir = Path(sys.argv[3])
out_dir = Path(sys.argv[4])

reports_dir = proj_dir / "reports"
prompts_dir = proj_dir / "prompts"

template = (prompts_dir / "merge-agent.md").read_text()

# Read all agent outputs
macro_output = (out_dir / "outputs" / "macro-analyst.md").read_text()
quant_output = (out_dir / "outputs" / "quant-analyst.md").read_text()
event_output = (out_dir / "outputs" / "event-analyst.md").read_text()
risk_output = (out_dir / "outputs" / "risk-analyst.md").read_text()

# Read compact cross-check context instead of the full raw payload
full_payload = (out_dir / "context" / "merge_crosscheck.md").read_text()

# Build previous report context for merge
prev_context = ""
if prev_report and Path(prev_report).exists():
    prev_text = Path(prev_report).read_text()
    prev_context = f"""
--- 上一份日报 ---
{prev_text}
--- 上一份日报结束 ---

请在「上期信号记分卡」部分严格评判：每个上期HIGH信号的预测方向与实际走势。信号错就是错，不要包装成"风险警告验证"。"""

replacements = {
    "{macro_output}": macro_output,
    "{quant_output}": quant_output,
    "{event_output}": event_output,
    "{risk_output}": risk_output,
    "{full_payload}": full_payload,
    "{prev_context}": prev_context,
    "{date}": date,
}

for k, v in replacements.items():
    template = template.replace(k, v)

out_path = out_dir / "prompts" / "merge-report.txt"
out_path.write_text(template)
print(f"  Merge prompt assembled ({len(template)} chars)")
MERGEEOF

echo "  Running merge agent..."

run_agent_with_fallback \
    "merge-report" \
    "$OUT_DIR/prompts/merge-report.txt" \
    "$OUT_DIR/outputs/merge-report.md" \
    "$OUT_DIR/logs/merge-report.log" \
    "$MERGE_TIMEOUT" \
    "$MIN_MERGE_BYTES"

# ── Copy final report ────────────────────────────────────────────────────────
FINAL_REPORT="$REPORTS_DIR/${DATE}_report_zh.md"
if [[ -s "$OUT_DIR/outputs/merge-report.md" ]]; then
    cp "$OUT_DIR/outputs/merge-report.md" "$FINAL_REPORT"
    echo ""
    echo "=== Report ready ==="
    echo "  $FINAL_REPORT"
    echo "  Size: $(wc -c < "$FINAL_REPORT") bytes"
    echo "  Chars: $(wc -m < "$FINAL_REPORT") chars"

    # Append Factor Lab experiment report section (non-fatal)
    echo "  Appending Factor Lab section..."
    if ! "$PYTHON_BIN" "$FACTOR_LAB_ROOT/scripts/generate_factor_report.py" \
        --date "$DATE" \
        --append-to "$FINAL_REPORT"; then
        echo "  Factor Lab section append failed (non-fatal)"
    fi
    cd "$PROJ_DIR"
else
    echo ""
    echo "  ERROR: merge-report.md is empty. Check $OUT_DIR/logs/merge-report.log"
    exit 1
fi

# ── Optional: email delivery ─────────────────────────────────────────────────
if [[ "${SEND_EMAIL:-}" == "1" ]]; then
    echo ""
    echo "  Sending email (with charts if available)..."
    CHART_DIR="$REPORTS_DIR/charts/$DATE"
    if [[ -d "$CHART_DIR" ]]; then
        python3 "$PROJ_DIR/scripts/send_email.py" "$FINAL_REPORT" --charts "$CHART_DIR"
    else
        python3 "$PROJ_DIR/scripts/send_email.py" "$FINAL_REPORT"
    fi
    echo "  Email sent."
fi

echo ""
echo "=== Done ==="
