#!/bin/bash
# Full daily pipeline: fetch → enrich → analytics → filter → render → agents → email
#
# Designed for cron. Logs everything to reports/logs/{date}_{slot}.log
#
# Usage:
#   ./scripts/daily_pipeline.sh                      # auto-detect date (Shanghai time)
#   ./scripts/daily_pipeline.sh morning              # morning slot (8:30 AM label)
#   ./scripts/daily_pipeline.sh evening              # evening slot (6:00 PM label)
#   ./scripts/daily_pipeline.sh morning 2026-04-16   # rerun specific date/slot
#   ./scripts/daily_pipeline.sh 2026-04-16 morning   # same as above

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

DATE=""
SLOT=""
for arg in "$@"; do
    case "$arg" in
        morning|evening)
            SLOT="$arg"
            ;;
        [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])
            DATE="$arg"
            ;;
        *)
            echo "ERROR: Unknown argument '$arg'"
            echo "Usage: ./scripts/daily_pipeline.sh [morning|evening] [YYYY-MM-DD]"
            exit 1
            ;;
    esac
done

DATE="${DATE:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}"
SLOT="${SLOT:-$(TZ=Asia/Shanghai date +%H | awk '{print ($1 < 12) ? "morning" : "evening"}')}"
LOG_DIR="$PROJ_DIR/reports/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/${DATE}_${SLOT}.log"
BRIDGE_PID=""
if [[ "$SLOT" == "morning" ]]; then
    SLOT_LABEL_CN="盘前"
elif [[ "$SLOT" == "evening" ]]; then
    SLOT_LABEL_CN="盘后"
else
    SLOT_LABEL_CN="$SLOT"
fi

cleanup() {
    # Ensure the background bridge never keeps the cron shell alive after failures.
    if [[ -n "${BRIDGE_PID:-}" ]]; then
        echo "  Stopping AKShare bridge (PID $BRIDGE_PID)..."
        kill "$BRIDGE_PID" 2>/dev/null || true
        wait "$BRIDGE_PID" 2>/dev/null || true
        BRIDGE_PID=""
    fi
}

trap cleanup EXIT

find_previous_report() {
    "$PYTHON_BIN" - "$PROJ_DIR" "$DATE" "$SLOT" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

project_dir = Path(sys.argv[1])
date = sys.argv[2]
slot = sys.argv[3]
rank = {"morning": 0, "evening": 1}
current_key = (date, rank.get(slot, 1))
reports: list[tuple[tuple[str, int], Path]] = []

for path in (project_dir / "reports").glob("*_report_zh_*.md"):
    stem = path.stem
    if "_report_zh_" not in stem:
        continue
    report_date, report_slot = stem.split("_report_zh_", 1)
    if report_slot not in rank:
        continue
    reports.append(((report_date, rank[report_slot]), path))

# Legacy evening reports from before slot-aware output existed.
for path in (project_dir / "reports").glob("*_report_zh.md"):
    report_date = path.stem.split("_report_zh", 1)[0]
    reports.append(((report_date, rank["evening"]), path))

reports.sort()
previous = [path for key, path in reports if key < current_key]
print(previous[-1] if previous else "", end="")
PY
}

annotate_and_snapshot_payloads() {
    "$PYTHON_BIN" - "$PROJ_DIR" "$DATE" "$SLOT" "$SLOT_LABEL_CN" <<'PY'
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import duckdb

project_dir = Path(sys.argv[1])
date = sys.argv[2]
slot = sys.argv[3]
slot_label = sys.argv[4]
reports_dir = project_dir / "reports"
db_path = project_dir / "data" / "quant_cn_report.duckdb"

latest_trade_date = "unknown"
try:
    con = duckdb.connect(str(db_path), read_only=True)
    latest = con.execute(
        """
        SELECT MAX(trade_date)
        FROM prices
        WHERE ts_code IN ('000300.SH', '000016.SH', '399006.SZ')
          AND trade_date <= CAST(? AS DATE)
        """,
        [date],
    ).fetchone()[0]
    con.close()
    latest_trade_date = str(latest) if latest is not None else "unknown"
except Exception as exc:
    latest_trade_date = f"unknown ({exc})"

if slot == "morning":
    meaning = (
        "盘前报告：价格/资金数据按最新可用收盘解释，重点是隔夜事件、今日触发条件、"
        "开盘后确认与撤销规则；不要把它写成收盘复盘。"
    )
else:
    meaning = (
        "盘后报告：应以今日收盘、全天资金流、事件兑现和早盘假设复盘为主；"
        "不要把早盘条件原样复制。"
    )

block = "\n".join(
    [
        "## 报告时段",
        f"- Slot: {slot} / {slot_label}",
        f"- 报告日期: {date}",
        f"- 最新指数价格交易日: {latest_trade_date}",
        f"- 解释: {meaning}",
        "",
        "---",
        "",
    ]
)

for section in ("macro", "structural", "events"):
    src = reports_dir / f"{date}_payload_{section}.md"
    if not src.exists():
        continue
    text = src.read_text(encoding="utf-8", errors="replace")
    if "## 报告时段" not in text[:1000]:
        src.write_text(block + text, encoding="utf-8")
    dst = reports_dir / f"{date}_payload_{section}_{slot}.md"
    shutil.copy2(src, dst)
    print(f"  Snapshot payload: {dst}")
PY
}

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
fi

# ── Step 2: Fetch + Enrich + Analytics + Filter + Render ─────────────────────
echo "[2/6] Running pipeline (fetch → analyze → filter → render)..."
./target/release/quant-cn run --date "$DATE" 2>&1

# ── Step 2.5: Import Factor Lab promoted factors ──────────────────────────────
echo "[2.5/6] Importing Factor Lab factors..."
(cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" -m src.mining.export_to_pipeline --market cn --date "$DATE") 2>&1 || echo "  Factor Lab import failed (non-fatal)"

# `quant-cn run` already performed enrichment. Only refresh payloads here so we
# preserve the same-day research snapshot and its analytics state.
echo "[3/6] Re-rendering payloads..."
./target/release/quant-cn render --date "$DATE" 2>&1

# Append Factor Lab research candidates AFTER render (so they don't get overwritten)
STRUCT_PAYLOAD="reports/${DATE}_payload_structural.md"
if [ -f "$STRUCT_PAYLOAD" ]; then
    FACTOR_TMP="$(mktemp)"
    FACTOR_STATUS="missing"
    FACTOR_TRADE_DATE=""
    FACTOR_AGE_DAYS=""
    if (cd "$FACTOR_LAB_ROOT" && "$PYTHON_BIN" scripts/run_strategy.py --market cn --today --date "$DATE") > "$FACTOR_TMP" 2>&1; then
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
        echo "  Factor Lab signal failed"
    fi

    echo "" >> "$STRUCT_PAYLOAD"
    echo "## Factor Lab Research Candidates" >> "$STRUCT_PAYLOAD"
    echo "" >> "$STRUCT_PAYLOAD"
    echo "以下是 Factor Lab 的研究候选，不是独立交易指令。" >> "$STRUCT_PAYLOAD"
    echo "它不能决定 Headline Gate、今日市场主方向或主书排序；只有通过主系统方向、execution gate、流动性和追价过滤后，才能进入主书。" >> "$STRUCT_PAYLOAD"
    if [ "$FACTOR_STATUS" = "stale" ]; then
        echo "状态: STALE。候选输出使用的最新交易日为 ${FACTOR_TRADE_DATE}，较报告日 ${DATE} 滞后 ${FACTOR_AGE_DAYS} 天。只允许放在附录，不得作为主书确认信号。" >> "$STRUCT_PAYLOAD"
    elif [ "$FACTOR_STATUS" = "fresh" ]; then
        if [ -n "$FACTOR_TRADE_DATE" ]; then
            echo "状态: FRESH。候选输出交易日 ${FACTOR_TRADE_DATE}，可作为研究附录展示，但不得覆盖主系统结论。" >> "$STRUCT_PAYLOAD"
        else
            echo "状态: FRESH。未发现明显日期滞后，可作为研究附录展示，但不得覆盖主系统结论。" >> "$STRUCT_PAYLOAD"
        fi
    else
        echo "状态: UNAVAILABLE。候选输出失败或缺少交易日信息，忽略其方向性结论。" >> "$STRUCT_PAYLOAD"
    fi
    echo "每只股票附带参考价、风控线、观察上沿和研究权重。" >> "$STRUCT_PAYLOAD"
    echo "" >> "$STRUCT_PAYLOAD"
    if [ -s "$FACTOR_TMP" ]; then
        cat "$FACTOR_TMP" >> "$STRUCT_PAYLOAD"
    fi
    echo "" >> "$STRUCT_PAYLOAD"
    echo "请在最终研报中完整展示上述清单，但明确标注为研究附录，不得让其主导 headline 或主书排序。" >> "$STRUCT_PAYLOAD"
    rm -f "$FACTOR_TMP"
fi

echo "[4/8] Annotating + snapshotting $SLOT payloads..."
annotate_and_snapshot_payloads

# ── Step 3: Generate charts ──────────────────────────────────────────────────
echo "[5/8] Generating charts..."
"$PYTHON_BIN" scripts/generate_charts.py --date "$DATE" 2>&1 || echo "  Charts failed (non-fatal)"
if [[ -d "reports/charts/$DATE" ]]; then
    mkdir -p "reports/charts/$DATE/$SLOT"
    find "reports/charts/$DATE" -maxdepth 1 -type f -name '*.png' -exec cp {} "reports/charts/$DATE/$SLOT/" \;
fi

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

# Pass the immediate previous slot report for hypothesis validation (if exists).
PREV_REPORT="$(find_previous_report)"
if [[ -f "$PREV_REPORT" ]]; then
    echo "  Using previous report: $PREV_REPORT"
    SEND_EMAIL=1 bash scripts/run_agents.sh "$DATE" "$SLOT" "$PREV_REPORT"
else
    SEND_EMAIL=1 bash scripts/run_agents.sh "$DATE" "$SLOT"
fi

echo ""
echo "=========================================="
echo "  Pipeline complete: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
echo "  Report: reports/${DATE}_report_zh_${SLOT}.md"
echo "=========================================="
