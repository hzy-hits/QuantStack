#!/bin/bash
# Weekly report pipeline: aggregate week's data → payload → agent → email
#
# Usage:
#   ./scripts/run_weekly.sh                    # current week
#   ./scripts/run_weekly.sh 2026-03-15         # week ending on this date
#
# Cron: Saturday 07:00 CST (UTC+8)
#   0 7 * * 6 cd $PROJ && ./scripts/run_weekly.sh >> logs/cron_weekly.log 2>&1

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"
mkdir -p logs

DATE="${1:-$(TZ=America/New_York date +%Y-%m-%d)}"
AGENT_TIMEOUT=600  # 10 min for weekly synthesis

echo "=========================================="
echo "Weekly Report Pipeline — week ending $DATE"
echo "$(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=========================================="

# ── 1. Generate weekly payload ────────────────────────────────────────────
echo ""
echo "[1/3] Generating weekly payload..."
if ! uv run python scripts/weekly_payload.py --date "$DATE"; then
    echo "ERROR: Weekly payload generation failed"
    exit 1
fi

PAYLOAD="reports/${DATE}_weekly_payload.md"
if [ ! -s "$PAYLOAD" ]; then
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
for f in $(ls -t reports/*_report_zh_*.md 2>/dev/null | head -5); do
    echo "$f" | grep -q '_weekly_' && continue
    PREV_DAILY="$f"
    break
done

CONTEXT=""
if [ -n "$PREV_DAILY" ]; then
    echo "  Using latest daily report for context: $PREV_DAILY"
    CONTEXT="

--- LATEST DAILY REPORT (for reference) ---
$(head -100 "$PREV_DAILY")
"
fi

# Single agent: weekly synthesis (no need for 4-agent split)
PROMPT="$(cat "$PAYLOAD")
$CONTEXT

---

请基于以上周度数据撰写一份A股/美股周度研究总结。

要求：
1. 开头3句话概括本周市场（指数表现、主线逻辑、关键变化）
2. 市场回顾：指数、板块轮动、资金流向
3. 宏观环境：本周发布的宏观数据、利率变化、Polymarket概率变化
4. 个股亮点：本周涨跌幅前5，简述原因
5. 风险提示：下周关注的风险事件
6. 下周展望：预期关注点、潜在催化剂

输出中文，约2000-3000字。语气专业但易读。"

echo "$PROMPT" | timeout "$AGENT_TIMEOUT" claude -p --output-format text > "$WEEKLY_REPORT" 2>/dev/null

if [ ! -s "$WEEKLY_REPORT" ]; then
    echo "ERROR: Weekly report generation failed"
    exit 1
fi
echo "  Report: $WEEKLY_REPORT ($(wc -c < "$WEEKLY_REPORT") bytes)"

# ── 3. Send email ─────────────────────────────────────────────────────────
echo ""
echo "[3/3] Sending weekly report email..."
uv run python scripts/send_report.py --send --date "$DATE" --session weekly --lang zh 2>&1 || {
    echo "  WARNING: send_report.py doesn't support --session weekly yet."
    echo "  Falling back to direct email..."
    # Fallback: use send_alert.py with the report content
    SUBJECT="[Quant Weekly] 美股周报 — $DATE"
    BODY="$(cat "$WEEKLY_REPORT")"
    uv run python scripts/send_alert.py --subject "$SUBJECT" --body "$BODY" 2>&1 || {
        echo "  Email send failed (non-fatal)"
    }
}

echo ""
echo "=========================================="
echo "Weekly pipeline complete — $DATE"
echo "$(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=========================================="
