#!/bin/bash
# A-share weekly report: aggregate week data → payload → agent → email
#
# Usage:
#   ./scripts/weekly_pipeline.sh                    # current week
#   ./scripts/weekly_pipeline.sh 2026-03-15         # week ending on this date
#
# Cron: Saturday 10:00 CST (UTC 02:00)
#   0 2 * * 6 cd $PROJ_CN && bash scripts/weekly_pipeline.sh >> reports/logs/cron_weekly.log 2>&1

set -uo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DATE="${1:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}"
LOG_DIR="$PROJ_DIR/reports/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/${DATE}_weekly.log"

exec > >(tee -a "$LOG") 2>&1

echo "=========================================="
echo "  A-Share Weekly Report Pipeline"
echo "  Week ending: $DATE"
echo "  Start: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
echo "=========================================="
echo ""

cd "$PROJ_DIR"

AGENT_TIMEOUT=600  # 10 min

# ── 1. Generate weekly payload ────────────────────────────────────────────
echo "[1/3] Generating weekly payload..."
"$PYTHON_BIN" scripts/weekly_payload.py --date "$DATE"

PAYLOAD="reports/${DATE}_weekly_payload.md"
if [[ ! -s "$PAYLOAD" ]]; then
    echo "ERROR: $PAYLOAD not found or empty"
    exit 1
fi
echo "  Payload: $(wc -c < "$PAYLOAD") bytes"

# ── 2. Run agent for weekly narrative ─────────────────────────────────────
echo ""
echo "[2/3] Running weekly synthesis agent..."

WEEKLY_REPORT="reports/${DATE}_report_weekly_zh.md"

# Find most recent daily report for context
PREV_DAILY=""
for f in $(ls -t reports/*_report_zh.md 2>/dev/null | head -5); do
    echo "$f" | grep -q '_weekly_' && continue
    PREV_DAILY="$f"
    break
done

CONTEXT=""
if [[ -n "$PREV_DAILY" ]]; then
    echo "  Using latest daily report for context: $PREV_DAILY"
    CONTEXT="

--- 最近一期日报（供参考）---
$(head -100 "$PREV_DAILY")
"
fi

PROMPT="$(cat "$PAYLOAD")
$CONTEXT

---

请基于以上周度数据撰写一份A股周度研究总结。

要求：
1. 开头3句话概括本周市场（指数表现、主线逻辑、关键变化）
2. 市场回顾：指数表现、板块轮动、成交量变化
3. 资金面：北向资金周度流向、融资余额变化、主力资金动向
4. 个股亮点：本周涨跌幅前5，简述驱动因素
5. 事件驱动：业绩预告、限售解禁、政策面变化
6. 下周展望：关注的风险事件、潜在催化剂、限售解禁预览
7. HMM模型状态：本周regime变化、P(bull)趋势

输出中文，约2000-3000字。语气专业但易读。
所有数字来自上方数据，不可编造。"

echo "$PROMPT" | timeout "$AGENT_TIMEOUT" claude -p --output-format text > "$WEEKLY_REPORT" 2>/dev/null

if [[ ! -s "$WEEKLY_REPORT" ]]; then
    echo "ERROR: Weekly report generation failed"
    exit 1
fi
echo "  Report: $WEEKLY_REPORT ($(wc -c < "$WEEKLY_REPORT") bytes)"

# ── 3. Send email ─────────────────────────────────────────────────────────
echo ""
echo "[3/3] Sending weekly report email..."

"$PYTHON_BIN" scripts/send_email.py "$WEEKLY_REPORT" \
    --charts "reports/charts/$DATE/" \
    --subject "A股周报 — $DATE" \
    2>&1 || echo "  Email send failed (non-fatal)"

echo ""
echo "=========================================="
echo "  Weekly pipeline complete: $(TZ=Asia/Shanghai date '+%Y-%m-%d %H:%M:%S CST')"
echo "  Report: $WEEKLY_REPORT"
echo "=========================================="
