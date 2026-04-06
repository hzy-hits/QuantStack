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

latest_file() {
    local pattern="$1"
    ls -1t $pattern 2>/dev/null | head -n 1
}

parse_field() {
    local file="$1"
    local prefix="$2"
    python3 - "$file" "$prefix" <<'PY'
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

payload_signal_date() {
    local file="$1"
    python3 - "$file" <<'PY'
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8", errors="replace")
needle = "## Factor Lab Independent Trading Signal"
idx = text.find(needle)
if idx < 0:
    sys.exit(1)
match = re.search(r"(A股|美股)\s+—\s+(\d{4}-\d{2}-\d{2})", text[idx:])
if not match:
    sys.exit(1)
print(match.group(2))
PY
}

section "Paths"
require_path "$FACTOR_LAB_ROOT" "factor-lab root"
require_path "$QUANT_CN_ROOT" "quant-research-cn root"
require_path "$QUANT_US_ROOT" "quant-research-v1 root"
require_path "$PYTHON_BIN" "python bin"

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

section "Autoresearch"
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
    fail "No CN autoresearch report found"
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
    fail "No US autoresearch report found"
fi

section "CN Reports"
latest_cn_report="$(latest_file "$QUANT_CN_ROOT/reports/*_report_zh.md")"
latest_cn_payload="$(latest_file "$QUANT_CN_ROOT/reports/*_payload_structural.md")"
if [[ -n "${latest_cn_report:-}" ]]; then
    pass "CN final report exists: $(basename "$latest_cn_report")"
    ls -lh "$latest_cn_report"
    if grep -q 'Factor Lab 因子实验报告' "$latest_cn_report"; then
        pass "CN final report includes Factor Lab experiment section"
    else
        warn "CN final report missing Factor Lab experiment section"
    fi
else
    fail "No CN final report found"
fi

if [[ -n "${latest_cn_payload:-}" ]]; then
    pass "CN structural payload exists: $(basename "$latest_cn_payload")"
    cn_payload_date="$(basename "$latest_cn_payload" | cut -d_ -f1)"
    cn_signal_date="$(payload_signal_date "$latest_cn_payload" 2>/dev/null || true)"
    if [[ -n "${cn_signal_date:-}" ]]; then
        echo "CN payload signal date=$cn_signal_date payload_date=$cn_payload_date"
        if [[ "$cn_signal_date" == "$cn_payload_date" ]]; then
            pass "CN payload signal date matches payload date"
        else
            warn "CN payload signal date is stale"
        fi
    else
        warn "CN payload missing parsable Factor Lab signal block"
    fi
else
    fail "No CN structural payload found"
fi

section "US Reports"
latest_us_report="$(latest_file "$QUANT_US_ROOT/reports/*_report_zh_*.md")"
latest_us_payload="$(latest_file "$QUANT_US_ROOT/reports/*_payload_structural.md")"
if [[ -n "${latest_us_report:-}" ]]; then
    pass "US final report exists: $(basename "$latest_us_report")"
    ls -lh "$latest_us_report"
    if grep -q 'Factor Lab 因子实验报告' "$latest_us_report"; then
        pass "US final report includes Factor Lab experiment section"
    else
        warn "US final report missing Factor Lab experiment section"
    fi
else
    fail "No US final report found"
fi

if [[ -n "${latest_us_payload:-}" ]]; then
    pass "US structural payload exists: $(basename "$latest_us_payload")"
    us_payload_date="$(basename "$latest_us_payload" | cut -d_ -f1)"
    us_signal_date="$(payload_signal_date "$latest_us_payload" 2>/dev/null || true)"
    if [[ -n "${us_signal_date:-}" ]]; then
        echo "US payload signal date=$us_signal_date payload_date=$us_payload_date"
        if [[ "$us_signal_date" == "$us_payload_date" ]]; then
            pass "US payload signal date matches payload date"
        else
            warn "US payload signal date is stale"
        fi
    else
        warn "US payload missing parsable Factor Lab signal block"
    fi
else
    fail "No US structural payload found"
fi

section "Summary"
echo "fails=$FAILS warnings=$WARNS"
if [[ "$FAILS" -gt 0 ]]; then
    exit 1
fi
