#!/bin/bash
set -uo pipefail

STACK_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$STACK_DIR/env.sh"

TODAY_CN="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
TODAY_US="$(TZ=America/New_York date +%Y-%m-%d)"

FAILS=0
WARNS=0

section() {
    echo ""
    echo "== $1 =="
}

pass() {
    echo "[OK] $1"
}

warn() {
    echo "[WARN] $1"
    WARNS=$((WARNS + 1))
}

fail() {
    echo "[FAIL] $1"
    FAILS=$((FAILS + 1))
}

require_path() {
    local path="$1"
    local label="$2"
    if [[ -e "$path" ]]; then
        pass "$label: $path"
    else
        fail "$label missing: $path"
    fi
}

require_command() {
    local command_name="$1"
    local label="$2"
    if [[ "$command_name" == */* ]]; then
        if [[ -x "$command_name" ]]; then
            pass "$label: $command_name"
        else
            fail "$label missing or not executable: $command_name"
        fi
    elif command -v "$command_name" >/dev/null 2>&1; then
        pass "$label: $(command -v "$command_name")"
    else
        fail "$label missing from PATH: $command_name"
    fi
}

latest_file() {
    "$PYTHON_BIN" - "$@" <<'PY'
import glob
import os
import sys

files = []
for pattern in sys.argv[1:]:
    files.extend(path for path in glob.glob(pattern) if os.path.isfile(path))
if files:
    print(max(files, key=os.path.getmtime))
PY
}

parse_field() {
    local file="$1"
    local prefix="$2"
    "$PYTHON_BIN" - "$file" "$prefix" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
prefix = sys.argv[2]
for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
    if line.startswith(prefix):
        print(line[len(prefix):].strip())
        break
PY
}

section "Paths"
require_path "$FACTOR_LAB_ROOT" "factor-lab root"
require_path "$QUANT_CN_ROOT" "quant-research-cn root"
require_path "$QUANT_US_ROOT" "quant-research-v1 root"
require_command "$PYTHON_BIN" "python bin"

section "Agent Defaults"
echo "backend=$FACTOR_LAB_AGENT_BACKEND"
echo "codex_model=$FACTOR_LAB_CODEX_MODEL"
echo "reasoning=$FACTOR_LAB_CODEX_REASONING_EFFORT"
if [[ "$FACTOR_LAB_AGENT_BACKEND" == "codex" ]]; then
    pass "Codex is the default backend"
else
    warn "Default backend is not codex"
fi
if [[ "$FACTOR_LAB_CODEX_MODEL" == "gpt-5.4" && "$FACTOR_LAB_CODEX_REASONING_EFFORT" == "xhigh" ]]; then
    pass "Codex default is gpt-5.4 xhigh"
else
    warn "Codex defaults are not gpt-5.4 xhigh"
fi

section "Factor Lab Smoke"
tmp_report="$(mktemp /tmp/factorlab-report.XXXXXX.md)"
if "$PYTHON_BIN" "$FACTOR_LAB_ROOT/scripts/generate_factor_report.py" --date "$TODAY_CN" --append-to "$tmp_report" >/tmp/factorlab-smoke-report.log 2>&1; then
    pass "generate_factor_report.py succeeded for $TODAY_CN"
else
    fail "generate_factor_report.py failed for $TODAY_CN"
    sed -n '1,40p' /tmp/factorlab-smoke-report.log
fi

cn_signal="$("$PYTHON_BIN" "$FACTOR_LAB_ROOT/scripts/run_strategy.py" --market cn --today --date "$TODAY_CN" 2>/tmp/factorlab-cn-signal.log | sed -n '1,12p')"
if [[ $? -eq 0 ]]; then
    pass "CN run_strategy smoke passed"
    echo "$cn_signal"
else
    fail "CN run_strategy smoke failed"
    sed -n '1,40p' /tmp/factorlab-cn-signal.log
fi

us_signal="$("$PYTHON_BIN" "$FACTOR_LAB_ROOT/scripts/run_strategy.py" --market us --today --date "$TODAY_US" 2>/tmp/factorlab-us-signal.log | sed -n '1,12p')"
if [[ $? -eq 0 ]]; then
    pass "US run_strategy smoke passed"
    echo "$us_signal"
else
    fail "US run_strategy smoke failed"
    sed -n '1,40p' /tmp/factorlab-us-signal.log
fi

section "Autoresearch Artifacts"
latest_cn_auto="$(latest_file "$FACTOR_LAB_ROOT/reports/autoresearch_cn_*.md")"
latest_us_auto="$(latest_file "$FACTOR_LAB_ROOT/reports/autoresearch_us_*.md")"

if [[ -n "${latest_cn_auto:-}" ]]; then
    cn_runs="$(parse_field "$latest_cn_auto" "- Experiments run:")"
    echo "CN latest: $(basename "$latest_cn_auto") | experiments=$cn_runs"
    if [[ "${cn_runs:-0}" -gt 0 ]]; then
        pass "CN autoresearch produced experiments"
    else
        warn "CN autoresearch latest run has 0 experiments"
    fi
else
    pass "No CN autoresearch report found; autoresearch is manual-only"
fi

if [[ -n "${latest_us_auto:-}" ]]; then
    us_runs="$(parse_field "$latest_us_auto" "- Experiments run:")"
    echo "US latest: $(basename "$latest_us_auto") | experiments=$us_runs"
    if [[ "${us_runs:-0}" -gt 0 ]]; then
        pass "US autoresearch produced experiments"
    else
        warn "US autoresearch latest run has 0 experiments"
    fi
else
    pass "No US autoresearch report found; autoresearch is manual-only"
fi

section "CN Reports"
latest_cn_report="$(latest_file \
    "$QUANT_CN_ROOT/reports/*_report_zh_evening.md" \
    "$QUANT_CN_ROOT/reports/*_report_zh_morning.md" \
    "$QUANT_CN_ROOT/reports/*_report_weekly_zh.md")"
if [[ -n "${latest_cn_report:-}" ]]; then
    pass "CN final report exists: $(basename "$latest_cn_report")"
    ls -lh "$latest_cn_report"
    pass "CN legacy report presence checked; dashboard validator owns report contract"
else
    fail "No CN final report found"
fi

section "US Reports"
latest_us_report="$(latest_file "$QUANT_US_ROOT/reports/*_report_zh_*.md")"
if [[ -n "${latest_us_report:-}" ]]; then
    pass "US final report exists: $(basename "$latest_us_report")"
    ls -lh "$latest_us_report"
    pass "US legacy report presence checked; dashboard validator owns report contract"
else
    fail "No US final report found"
fi

section "Main Strategy V2"
latest_dashboard_json="$(latest_file "$QUANT_STACK_ROOT/reports/review_dashboard/main_strategy_v2/*/main_strategy_v2_backtest.json")"
if [[ -n "${latest_dashboard_json:-}" ]]; then
    dashboard_dir="$(dirname "$latest_dashboard_json")"
    dashboard_date="$(basename "$dashboard_dir")"
    pass "Main Strategy V2 dashboard exists: $dashboard_date"
    require_path "$dashboard_dir/cn_daily_report.md" "dashboard CN report"
    require_path "$dashboard_dir/us_daily_report.md" "dashboard US report"
    if "$PYTHON_BIN" "$QUANT_STACK_ROOT/scripts/validate_main_strategy_v2_reports.py" --date "$dashboard_date" >/tmp/main-strategy-v2-validate.log 2>&1; then
        pass "Main Strategy V2 validator passed for $dashboard_date"
    else
        fail "Main Strategy V2 validator failed for $dashboard_date"
        sed -n '1,80p' /tmp/main-strategy-v2-validate.log
    fi
else
    fail "No Main Strategy V2 dashboard JSON found"
fi

section "Summary"
echo "fails=$FAILS warnings=$WARNS"
if [[ "$FAILS" -gt 0 ]]; then
    exit 1
fi
