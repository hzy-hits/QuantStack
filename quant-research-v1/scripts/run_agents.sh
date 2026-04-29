#!/bin/bash
# Run 4 analysis agents in parallel, then merge
#
# Usage:
#   ./scripts/run_agents.sh 2026-03-09 post
#   ./scripts/run_agents.sh 2026-03-09 pre reports/2026-03-08_report_zh_post.md
#
# Args:
#   $1 = date (required)
#   $2 = session: "post" or "pre" (required)
#   $3 = previous report path (optional)

set -euo pipefail

DATE="${1:?Usage: run_agents.sh DATE SESSION [PREV_REPORT]}"
SESSION="${2:?Usage: run_agents.sh DATE SESSION [PREV_REPORT]}"
PREV_REPORT="${3:-}"

AGENT_TIMEOUT=360  # 6 min per agent (larger universe = bigger payload)
MERGE_TIMEOUT=1200  # 20 min for merge (4 agent outputs + previous report + precision rules)
MIN_AGENT_BYTES=100
MIN_MERGE_BYTES=200

CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
QUANT_AGENT_BACKEND="${QUANT_AGENT_BACKEND:-codex}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
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

if [ "$SESSION" = "pre" ]; then
    SESSION_LABEL_CN="盘前"
SESSION_CONTEXT="--- 本期报告时段 ---
Session: pre / 盘前。
写作要求：本期必须突出 overnight delta：上一份盘后之后新增/移除的名字、隔夜新闻、盘前价差、期权/宏观变化。价格层若仍是上一交易日收盘，要明确说明这是盘前限制；不要把上一份盘后复述成一份新报告。
As-of 约束：本报告只能使用 ${DATE} 盘前可知的信息；禁止使用 ${DATE} 之后发布、结算或验证的事实。
--- 本期报告时段结束 ---"
else
    SESSION_LABEL_CN="盘后"
SESSION_CONTEXT="--- 本期报告时段 ---
Session: post / 盘后。
写作要求：本期必须突出 full-session delta：当日收盘、全天新闻、期权/波动率和盘前假设兑现情况。不要把盘前的触发条件原样保留；必须裁决哪些已触发、作废、升级或降级。
As-of 约束：本报告只能使用 ${DATE} 盘后当时可知的信息；未来日程可以写成“待发生催化剂”，但禁止写入 ${DATE} 之后才知道的结果、行情或验证。
--- 本期报告时段结束 ---"
fi

cd "$PROJECT_DIR"

# Pre-flight checks
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
for section in macro structural news; do
    required_f="reports/${DATE}_payload_${section}_${SESSION}.md"
    legacy_f="reports/${DATE}_payload_${section}.md"
    if [ -s "$required_f" ]; then
        continue
    fi
    [ -s "$legacy_f" ] || { echo "ERROR: Required file missing or empty: $required_f"; exit 1; }
done

OUT_DIR="run-agents-${DATE}-${SESSION}"
# Clean stale outputs from previous runs of same date/session
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

    try_claude_agent() {
        [[ "$CLAUDE_AVAILABLE" -eq 1 ]] || return 1
        echo "[claude] starting ${agent_name}" >> "$log_file"
        if CLAUDECODE="" "$TIMEOUT_BIN" "$timeout_secs" "$CLAUDE_BIN" -p --output-format text \
            < "$prompt_file" > "$output_file" 2>>"$log_file"; then
            local size
            size=$(wc -c < "$output_file" 2>/dev/null || echo 0)
            if [[ "$size" -ge "$min_bytes" ]]; then
                echo "[claude] success (${size} bytes)" >> "$log_file"
                echo "  ✓ ${agent_name} done (${size} bytes, backend=claude)"
                return 0
            fi
            echo "[claude] output too small (${size} bytes < ${min_bytes})" >> "$log_file"
        else
            local rc=$?
            echo "[claude] failed (exit ${rc})" >> "$log_file"
        fi
        return 1
    }

    try_codex_agent() {
        [[ "$CODEX_AVAILABLE" -eq 1 ]] || return 1
        echo "[codex] starting ${agent_name} model=${CODEX_MODEL}" >> "$log_file"
        if "$TIMEOUT_BIN" "$timeout_secs" "$CODEX_BIN" exec \
            --model "$CODEX_MODEL" \
            --sandbox read-only \
            --color never \
            --skip-git-repo-check \
            -C "$PROJECT_DIR" \
            -o "$output_file" \
            - < "$prompt_file" >> "$log_file" 2>&1; then
            local size
            size=$(wc -c < "$output_file" 2>/dev/null || echo 0)
            if [[ "$size" -ge "$min_bytes" ]]; then
                echo "[codex] success (${size} bytes)" >> "$log_file"
                echo "  ✓ ${agent_name} done (${size} bytes, backend=codex)"
                return 0
            fi
            echo "[codex] output too small (${size} bytes < ${min_bytes})" >> "$log_file"
        else
            local rc=$?
            echo "[codex] failed (exit ${rc})" >> "$log_file"
        fi
        return 1
    }

    if [[ "$QUANT_AGENT_BACKEND" == "claude" ]]; then
        try_claude_agent && return 0
        try_codex_agent && return 0
    else
        try_codex_agent && return 0
        try_claude_agent && return 0
    fi

    echo "  ✗ ${agent_name} failed on all available backends"
    return 1
}

build_carry_forward_context() {
    local report_path="$1"
    if [[ -z "$report_path" || ! -f "$report_path" ]]; then
        return 0
    fi
    "$PYTHON_BIN" - "$report_path" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8", errors="replace")
section_re = re.compile(
    r"(?ms)^###\s+(.+?)\n(.*?)(?=^###\s+|^##\s+|^\*\*风险与展望\*\*|^\*\*附注|^---\s*$|\Z)"
)
symbol_re = re.compile(r"\*\*([A-Z][A-Z0-9.\-]{0,9})\*\*|`([A-Z][A-Z0-9.\-]{0,9})`")
ignored = {
    "AI", "AIFF", "API", "ATM", "ATR", "BSS", "CPI", "DTE", "EPS", "ETF", "GDP",
    "HMM", "HY", "IV", "PDT", "PMI", "Q1", "Q2", "Q3", "Q4", "RVOL", "SPY",
    "T1", "T2", "VIX", "WTI",
}
wanted_sections = [
    "做多",
    "可执行机会",
    "条件式延续观察",
    "风险回避",
    "继续跟踪",
    "观望",
]
groups: dict[str, list[str]] = {name: [] for name in wanted_sections}
for title, body in section_re.findall(text):
    matched = next((name for name in wanted_sections if name in title), None)
    if not matched:
        continue
    if "Factor Lab" in title:
        continue
    for a, b in symbol_re.findall(body):
        sym = (a or b).strip().upper()
        if sym in ignored or len(sym) < 2:
            continue
        if sym not in groups[matched]:
            groups[matched].append(sym)

lines = []
for name in wanted_sections:
    symbols = groups.get(name) or []
    if symbols:
        lines.append(f"- {name}: {', '.join(symbols[:12])}")
if not lines:
    sys.exit(0)

print("--- 延续跟踪清单（从上一份日报自动提取，必须交代去向）---")
print("\n".join(lines))
print("--- 延续跟踪清单结束 ---")
print(
    "写作要求：上述股票不能在本期静默消失。若今日仍有新证据，写入对应栏目；"
    "若今日 payload 未保留或证据转弱，也要在信号记分卡、风险回避或继续跟踪中用一句话说明“移除/降级/等待”的原因。"
)
PY
}

clean_previous_report_context() {
    local report_path="$1"
    "$PYTHON_BIN" - "$report_path" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8", errors="replace")
text = re.sub(r"(?ms)\n---\n\n\*\*Factor Lab 选股\*\*.*?(?=\n---\n\n## Factor Lab 因子实验报告|\Z)", "", text)
text = re.sub(r"(?ms)\n---\n\n## Factor Lab 因子实验报告.*\Z", "", text)
text = re.sub(r"(?ms)^### Factor Lab\n.*?(?=^## |\Z)", "", text)
print(text.strip())
PY
}

# Build previous report context
PREV_CONTEXT=""
PREV_CONTEXT="

${SESSION_CONTEXT}"
if [ -n "$PREV_REPORT" ] && [ -f "$PREV_REPORT" ]; then
    PREV_CARRY_FORWARD="$(build_carry_forward_context "$PREV_REPORT")"
    PREV_REPORT_CONTEXT="$(clean_previous_report_context "$PREV_REPORT")"
    PREV_CONTEXT="
${SESSION_CONTEXT}
--- PREVIOUS REPORT (for hypothesis validation) ---
${PREV_REPORT_CONTEXT}
--- END PREVIOUS REPORT ---

${PREV_CARRY_FORWARD}

If the previous report exists above, START by briefly noting which predictions played out and which didn't (2-3 sentences). Then proceed with today's analysis."
    PREV_CONTEXT="${PREV_CONTEXT}

Use the previous report for two things:
1. scorekeeping: clearly mark what played out and what failed;
2. continuity: if a risk, watchlist name, or house view still stands, keep the same framing unless today's data clearly overturns it.
Do NOT blindly inherit yesterday's directional narrative; only carry forward what still has fresh evidence behind it."
fi

echo "  Launching 4 agents in parallel..."

python3 "$PROJECT_DIR/scripts/build_agent_context.py" \
    --date "$DATE" \
    --session "$SESSION" \
    --reports-dir "$PROJECT_DIR/reports" \
    --out-dir "$OUT_DIR/context"

# Write prompts to temp files (avoids ARG_MAX for large payloads)

# ── Agent 1: Macro ─────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/macro-analyst.txt" <<PROMPT
你是美股宏观提取器。你的任务不是写故事，而是从 payload 里提取“今天能不能下主方向判断”的结构化证据。

阅读下面数据，输出约 450-650 字的**中文结构化提取**。不要写成散文。
严格 as-of：只使用 payload 和上一份报告中截至 ${DATE} 可知的信息；不要用模型常识补入 ${DATE} 之后的结果。

--- TODAY'S MACRO DATA ---
$(cat "$OUT_DIR/context/macro.md")
--- END DATA ---
${PREV_CONTEXT}

先读 Headline Gate。它是市场叙事上下文，不是个股执行门禁：
- 如果 Headline mode = TREND，你可以承认市场存在方向偏置。
- 如果 Headline mode = RANGE / UNCERTAIN，你必须明确写“不能 headline 成牛/熊”，但不能仅凭 headline 否决通过执行约束的个股 alpha。
- HMM 只作为模型证据，不能单独决定牛/熊；必须结合 Internal Fear/Greed、VIX、SPY RSI、市场宽度、利率和信用。

输出格式严格遵守：

## Headline Gate
- mode: [trend|range|uncertain]
- directional_regime_allowed: [true|false]
- rule: [摘录 payload 的 reporting_rule 或同义压缩]
- calibration_support: [强|一般|弱；附 n 和 Brier / hit rate 的粗粒度结论]
- key_reasons: [最多2条]

## Regime
- Internal Fear/Greed: [score + label + VIX/SPY RSI/breadth/credit 的关键输入]
- HMM: [model state + 证据强弱 + P(ret>0) 的大致区间；明确它不是牛熊裁判]
- Breadth: [上涨/下跌分布、sector breadth、advance/decline]
- 核心结论: [趋势 / 震荡 / 不确定；必须来自 Fear/Greed、RSI、宽度、利率/信用的合成证据，而不是 HMM 单独决定]

## Rates / Credit / Vol
| 指标 | 当前值 | 变化/滞后 | 解释 |
|------|--------|-----------|------|
必须覆盖：Fed Funds、10Y、10Y-2Y、HY spread、VIX、CPI。

## Cross-Asset
- Polymarket: [概率 + 成交量 + 时间截面属性]
- Options Extremes: [最极端看多/看空结构透露什么]
- 2条跨市场信号：例如油、黄金、利差、美元、VIX 的组合

## Contradictions
- OBSERVED: ...
  MOST LIKELY: ...
  UNVERIFIED: ...
- OBSERVED: ...
  MOST LIKELY: ...
  UNVERIFIED: ...

## 判断
恰好 3 句话：
- 每句必须包含至少 1 个数字
- 至少 1 句必须直接回答“今天能不能 headline 成牛/熊”
- 至少 1 句必须说明 HMM 为什么只能作为辅助证据

规则：
- 只能引用 payload 中已有数字
- 概率禁止写成 1.00 / 0.00，用“约55%”或“接近抛硬币”这类表达
- 宏观数据如果滞后超过 14 天，必须标注参考期和滞后
- 区分模型概率、历史基率、风险中性概率
- regime day-count 只能作背景，不能当作“模型已稳定”的证据
- 样本量 < 30 时，不要堆三位小数；优先说明“样本偏少、证据弱”
- 不给交易建议
PROMPT

# ── Agent 2: Quant ─────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/quant-analyst.txt" <<PROMPT
你是美股量化提取器。你的任务是回答“今天哪些名字还能做，哪些已经被 overnight move 或结构冲突吃掉了 alpha”。

阅读下面数据，输出约 550-750 字的**中文结构化提取**。不要写散文，不要给主观大段评论。
严格 as-of：只使用 payload 和上一份报告中截至 ${DATE} 可知的信息；不要用 ${DATE} 之后的行情、新闻或结果解释今天。

--- TODAY'S STRUCTURAL DATA ---
$(cat "$OUT_DIR/context/structural.md")
--- END DATA ---
${PREV_CONTEXT}

先读 Headline Gate。它是市场叙事上下文，不是个股执行门禁：
- 如果 Headline mode != TREND，不得把 Core Book 写成单边市场主线。
- 如果 Headline mode = UNCERTAIN，优先输出触发条件、回踩条件和“不要追价”的名字，但允许保留满足执行硬规则的单名股 alpha。

再读 Report Postmortem（如果 payload 里有）。这不是背景噪音，而是交易复盘硬约束：
- 你必须明确回答近期主问题更像：漏 alpha、追晚了、判断错了、还是 edge 太薄
- 如果 postmortem 明确写 “The bigger problem is arriving after the move is already paid” 或同义结论，今天就不能把高开后已消耗 expected move 的名字写成还能做
- 如果 postmortem 明确写 missed alpha 偏高，要点名系统更容易错过 follow-through，而不是假装一切只是 headline 问题

再读 Setup Alpha / Anti-Chase。反追高不是反趋势：
- Early accumulation / Pullback / Post-event second day 只能写成 setup、回踩复核或次日承接观察，不得直接升为“可直接做”
- Breakout Acceptance 代表“已经涨了但趋势/事件/期权确认仍支持 follow-through”，不得机械打成 stale chase
- Blocked Chase / Priced-in 必须进入风险回避或不追价，不得升入可直接做

执行层硬规则：
- 只有同时满足 Execution read = Still actionable、R:R >= 1.20、且没有重大冲突的名字，才允许归入“可直接做”
- 如果 Execution read = Prefer pullback entry / Do not chase the gap，一律归入“等回踩/不追价”
- 如果 Headline mode != TREND，不得把“可直接做”偷换成市场主线；但 CORE BOOK 名字若同时满足 Execution read = Still actionable、R:R >= 1.50、且没有被新闻/风险层明确判定为“already-done / priced in / 不该追”，仍可保留为 1-2 个单名股 alpha

输出格式严格遵守：

## Headline Gate
- mode: [trend|range|uncertain]
- direction_allowed: [true|false]
- rule: [摘录]
- trend_quality: [用一句话概括 trend_prob / calibration / concentration]

## Report Review
- primary_issue: [missed_alpha|late|wrong|thin_edge|capturing|mixed|insufficient]
- verdict: [用一句话翻译 payload 的 Verdict]
- implication_today: [今天最该防什么]
- evidence: [selected_reviewed / ignored_reviewed / capture / stale / ignored-alpha / false-positive / flat-edge，用粗粒度百分比，不要三位小数]

## Clusters
- [主题/篮子]: [N] 个名字 ≈ [M] 个独立赌注
- [主题/篮子]: ...

## 可直接做（最多 3 个）
| 代码 | lane | 方向 | execution | gap_vs_move | entry | stop | target | R:R | 关键支持 | 关键冲突 |
|------|------|------|-----------|-------------|-------|------|--------|-----|----------|----------|
如果没有符合规则的名字，明确写：“今天没有可直接做的多头。”

## 等回踩 / 不追价
| 代码 | execution | gap_vs_move | R:R | 为什么现在不能追 |
|------|-----------|-------------|-----|------------------|

## Tactical Event Tape
- 只写 2-4 个最值得留意的事件型名字
- 必须标注它们是“战术观察”，不是主书

## Exhaustion / Already-Done
| 代码 | 信号 | 证据 |
|------|------|------|
优先列出 20D 涨幅过大、cone_position 靠近顶部、execution gate 不支持追价的名字

## Judgment
恰好 3 句话：
- 每句必须包含至少 1 个数字
- 至少 1 句必须直接回答“今天有没有可直接做的多头”
- 至少 1 句必须解释 overnight / execution 为什么让部分 HIGH 失效

规则：
- 只用 payload 数字
- 概率与风险中性区间不能混用
- 不给主观大段叙事，不给投资建议
PROMPT

# ── Agent 3: News ──────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/news-analyst.txt" <<PROMPT
你是美股事件提取器。你的任务是回答“催化剂是不是还新鲜、是不是已经被价格吃掉、有没有自我否定”。

阅读下面数据，输出约 450-650 字的**中文结构化提取**。不要写散文。
严格 as-of：未来事件只能写成“待发生/待验证”，不得写事件结果或 ${DATE} 之后才发布的事实。

--- TODAY'S NEWS & EVENTS DATA ---
$(cat "$OUT_DIR/context/news.md")
--- END DATA ---
${PREV_CONTEXT}

输出格式严格遵守：

## 主题
- [主题1]: [一句话，带数字或时间]
- [主题2]: ...
- [主题3]: ...

## Core Catalysts
| 代码 | 催化剂 | 新鲜度 | 持续性 | 是否已被定价 | 是否自我否定 | 近端风险 |
|------|--------|--------|--------|--------------|--------------|----------|

## Already Priced / Exhausted
- [代码]: [为什么新闻已老化、被 3+ 来源广泛传播、或价格已先走完]
- [代码]: ...

## Tactical Event Tape
- [代码]: [事件型机会还是纯噪声，是否只能战术观察]
- [代码]: ...

## Data Quality Warnings
- [代码]: [误归因 / 单一来源 / 新闻太旧 / 根本没新闻]
- [代码]: ...

## Judgment
恰好 3 句话：
- 每句至少 1 个数字
- 至少 1 句必须说明哪些催化剂已经被 price in
- 至少 1 句必须说明哪些名字“ headline 很响，但信息优势为零 ”

规则：
- 只能引用具体 headline / filing 描述
- 单一来源必须点名是单一来源
- 不要因为有 headline 就默认还有 alpha
- 不给投资建议
PROMPT

# ── Agent 4: Risk ──────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/risk-analyst.txt" <<PROMPT
你是美股风险提取器。你的任务是回答“今天如果硬做，会怎么错”。
账户约束：**long-only**。你可以讨论下行风险，但不能把任何内容写成“做空机会”。
当你写到 bear / downside / 下行情景时，含义只能是：空仓、减仓、回避、等待更好买点，不能是反手做空。

阅读下面数据，输出约 450-650 字的**中文结构化提取**。不要写散文，不要替交易找理由。
严格 as-of：只评估 ${DATE} 当时可知的风险，不得引用 ${DATE} 之后的行情或事件结果。

--- TODAY'S STRUCTURAL DATA ---
$(cat "$OUT_DIR/context/structural.md")
--- END STRUCTURAL DATA ---

--- TODAY'S MACRO DATA ---
$(cat "$OUT_DIR/context/macro.md")
--- END MACRO DATA ---

先读 Headline Gate。如果 Headline mode != TREND，必须优先强调集中度、执行失效和“不值得追”的理由。

输出格式严格遵守：

## Concentration
- 方向暴露: [多/空占比]
- 主题集中: [N 个名字 ≈ M 个独立赌注]
- 最危险的单一暴露: [一句话]

## Execution Blocks
| 代码 | execution | gap_vs_move | R:R | 主要风险 | 失效条件 |
|------|-----------|-------------|-----|----------|----------|
优先列出那些看起来最像“报告里会被误写成可以买，但实际不该追”的名字。

## Portfolio Warnings
- 净方向
- 是否存在自然对冲
- 如果全部执行，最可能一起出错的 2 个因子

## Scenarios
- 上行确认触发: [1-2个具体条件]
- 下行风险触发: [1-2个具体条件；只代表减仓/空仓/回避]
- 震荡区间: [最可能的混乱区间]

## Judgment
恰好 3 句话：
- 每句至少 1 个数字
- 至少 1 句必须说明“今天最大的风险不是看错方向，而是用错入场时点”
- 如果 gate != TREND，至少 1 句必须点明“不能把这套书单当成趋势主书”

规则：
- 失效条件必须具体可观察
- 情景概率如需出现，只能写主观估计
- 不给投资建议
PROMPT

# Launch all 4 agents reading from prompt files via stdin
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
    "news-analyst" \
    "$OUT_DIR/prompts/news-analyst.txt" \
    "$OUT_DIR/outputs/news-analyst.md" \
    "$OUT_DIR/logs/news-analyst.log" \
    "$AGENT_TIMEOUT" \
    "$MIN_AGENT_BYTES"
) &
PID_NEWS=$!

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

# Wait for all 4 to finish
echo "  Waiting for agents (PIDs: $PID_MACRO $PID_QUANT $PID_NEWS $PID_RISK)..."
FAIL=0
wait $PID_MACRO || { echo "  ✗ macro-analyst failed"; FAIL=1; }
wait $PID_QUANT || { echo "  ✗ quant-analyst failed"; FAIL=1; }
wait $PID_NEWS  || { echo "  ✗ news-analyst failed"; FAIL=1; }
wait $PID_RISK  || { echo "  ✗ risk-analyst failed"; FAIL=1; }

if [ "$FAIL" -eq 1 ]; then
    echo "  Some agents failed. Check logs in $OUT_DIR/logs/"
    echo "  Aborting — will not merge partial agent outputs"
    exit 1
fi

# Verify outputs exist — abort if any are empty
MISSING=0
for f in macro-analyst quant-analyst news-analyst risk-analyst; do
    if [ ! -s "$OUT_DIR/outputs/$f.md" ]; then
        echo "  ERROR: $f.md is empty or missing"
        MISSING=1
    fi
done
if [ "$MISSING" -eq 1 ]; then
    echo "  Aborting merge — cannot merge with missing agent outputs"
    exit 1
fi

# ── Agent 5: Merge (sequential) ───────────────────────────────────────────
echo "  Running merge agent..."

PREV_MERGE_CONTEXT=""
PREV_MERGE_CONTEXT="

${SESSION_CONTEXT}"
if [ -n "$PREV_REPORT" ] && [ -f "$PREV_REPORT" ]; then
    PREV_CARRY_FORWARD="$(build_carry_forward_context "$PREV_REPORT")"
    PREV_REPORT_CONTEXT="$(clean_previous_report_context "$PREV_REPORT")"
    PREV_MERGE_CONTEXT="
${SESSION_CONTEXT}
--- 上一份日报 ---
${PREV_REPORT_CONTEXT}
--- END ---

${PREV_CARRY_FORWARD}

请在「上期信号记分卡」部分严格评判：每个上期HIGH信号的预测方向与实际走势。信号错就是错，不要包装成\"风险警告验证\"。"
    PREV_MERGE_CONTEXT="${PREV_MERGE_CONTEXT}

上一份日报不只用于记分卡，也用于保持版式和 house view 的连续性，但不能覆盖本期 session delta：
- 如果主问题、主要风险、继续跟踪名单今天仍成立，可以沿用同一套层级，但必须先写清楚本期新增/移除/触发/作废的变化。
- 如果盘前到盘后、或前一天到今天，核心判断没有被新数据推翻，就写“结论未变，但新增证据是...”而不是复述上一份正文。
- 只有当今天出现更硬的新证据时，才允许推翻上一份日报的主线，并明确写出为什么改口。"
fi

cat > "$OUT_DIR/prompts/merge-report.txt" <<PROMPT
你是美股日报叙事官。请阅读四个分析师的**结构化提取**，写成一篇清晰、连贯、说人话的中文市场日报。
严格 as-of：这是一份 ${DATE} ${SESSION_LABEL_CN} 日报，只能写当时已经可知的信息。未来日程只能作为待发生催化剂；禁止使用 ${DATE} 之后的结果、行情、新闻验证或模型外事实。

写作风格要求（极其重要）：
- 写作风格：像顶级对冲基金的晨会纪要。冷静、精准、有攻击性。每句话都有信息量。
- 数字驱动：每个判断必须附数字。不说"资金流出明显"，说"净流出8.5亿，连续3天"。
- Headline Gate 只约束市场 headline 强度；HMM 不能单独决定牛/熊。是否能写成强单边，必须由 Fear/Greed、SPY RSI、VIX、宽度、利率/信用和事件共同支持；它不是个股执行否决器。
- 有观点，但不要强行选边：如果 "Headline mode = RANGE/UNCERTAIN"，就直接写震荡/等待确认/暂不下主方向，并给触发条件。
- 禁用词：综合考量、谨慎乐观、值得关注、密切跟踪、不确定性较大。
- 上次错了一句话说清楚，不要包装。
- 全文1200-1500字。精炼是能力。
- Factor Lab 属于独立实验附录，由 pipeline 另行追加；正文不要重写完整清单。
- 不得出现逻辑打架：如果前文写“今天没有好机会”，后文就不能再硬塞“值得买的”。
- 如果分析师的 Report Review 已经说明近期主问题是漏 alpha / 追晚了 / 判断错了 / edge 太薄，你必须在正文里明确说出来，不能只埋在记分卡里。
- 优先清晰裁决：四个分析师冲突时，先保留能落成交易地图的那个结论；只有在数字证据不足时才默认保守。
- 禁止把弱证据写成强结论：样本小、Brier 接近 0.25、或 edge 靠近 0 时，要直接写“证据弱”，不要用三位小数制造确定感。
- Herfindahl 只能解释为“行业/主题集中度”，不能写成“所有仓位都压在同一个名字”。
- regime_duration 只能作为背景信息，不能单独证明趋势稳定。
- 账户约束：**long-only**。全文只能给出做多、空仓、减仓、回避、等待确认这几类动作；不得给出做空、反手空、空头仓位、对冲空单这类执行建议。
- 如果写下行情景，它的交易含义只能是“不做多/减仓/等更低风险买点”，不能写成可以从下跌中获利的动作。
- 交易地图要稳定成三层：先写“可执行机会”，再写“条件式延续观察”，最后写“继续跟踪”；不要把大量 veto 全堆进“风险回避”。
- “风险回避”只留最关键的 3-6 个名字，每个一句话；空间优先让给可执行层和继续跟踪层。
- 盘前、盘后、前后两天如果主判断没有被新证据推翻，就保持同一套 section 顺序、口径和主问题表述；但每份报告必须先写本期 session delta，不能只复述上一份。
- 如果 prompt 中出现“延续跟踪清单”，清单里的股票必须有去向：继续保留、降级、移除或等待，都要给一句理由，不能让昨天提到的名字静默消失。

--- 宏观分析师输出 ---
$(cat "$OUT_DIR/outputs/macro-analyst.md")

--- 量化结构分析师输出 ---
$(cat "$OUT_DIR/outputs/quant-analyst.md")

--- 新闻事件分析师输出 ---
$(cat "$OUT_DIR/outputs/news-analyst.md")

--- 风险分析师输出 ---
$(cat "$OUT_DIR/outputs/risk-analyst.md")
${PREV_MERGE_CONTEXT}

输出要求：
1. 结构（严格按这个来，标题和小标题照抄）：
   - "# 市场日报 — ${DATE}（${SESSION_LABEL_CN}）"
   - "## 一句话"
     一句话，不超过30字。Headline Gate 只约束市场叙事强度。
   - "## 信号记分卡"
     上次 HIGH 信号对还是错。没有可追踪就写“本期无可追踪信号”。结尾必须补一句：近期主问题到底是“漏 alpha”“追晚了”“判断错了”还是“edge 太薄”。
   - "## 今日市场"
     一段连贯叙事，必须回答“今天能不能下主方向判断”。必须包含：Internal Fear/Greed、SPY RSI、联邦基金利率、10年美债收益率、10Y-2Y利差、高收益利差、VIX，并说明 HMM 只能作为辅助证据。
   - "## 交易地图"
     - "### 做多"：只允许写真正还能做的名字。
     - "### Setup Alpha"：先写 Early/Pullback/Post-event/Breakout Acceptance 的系统分组结论；Breakout Acceptance 是条件式突破承接，不等于追高。
     - "### 条件式延续观察"：只写仍有延续 edge 的名字，最多2只；明确写“小仓位、硬止损、不追高超过 max chase gap”。
     - "### 风险回避"：不该追、不该加、该降风险的名字，最多 3-6 个。
     - "### 继续跟踪"：值得跟踪但今天不做的名字，优先写 2-4 个带催化剂或等待位的名字。
     - "### Factor Lab"：只保留一句状态说明 + 一张紧凑表格（代码/名称/参考价/风控线/观察上沿/研究权重/备注）；必须标注为研究附录，不得写成交易指令。
   - "## 风险与展望"
     集中度警告 + 三情景 + 未来3天看什么。三情景必须是“震荡 / 上行确认 / 下行风险”，其中“下行风险”只代表 long-only 账户该回避或减仓。
   - "## 附注"
     一行风险提示。
2. “做多” 的硬规则：
   - 只有同时满足 execution = Still actionable、R:R >= 1.20、没有重大冲突、并且没有被新闻/风险分析师判定为“已 price in / 不该追”的名字，才允许进入“做多”
   - 如果 Headline Gate != TREND，“做多”仍可保留通过执行硬规则的条件式单名股机会，但必须同时满足 execution = Still actionable、R:R >= 1.35、没有重大冲突，且没有被风险/新闻分析师明确判成“already-done / priced in / 不该追”；它们只能写成条件式个股 alpha，不能上升成市场主方向。如果没有这种机会，直接写：“今天没有可直接做的多头，别追价。”
   - 不允许出现“前文说今天没机会，后文又列出两个买点”的矛盾
3. 延续候选里的名字可以写进“条件式延续观察”，但不能偷换成市场主方向；若 Headline Gate != TREND，它们默认只是 continuation 战术仓位。
4. 事件驱动候选默认是“战术观察”，不是主书。除非四个分析师都支持，否则不要抬进“做多”。不要仅凭“IV 高”或“接近 52 周新高”就把已经满足上面硬规则的名字打回继续跟踪；必须给出明确的数值型 exhaustion / priced-in 证据。
5. 四个分析师有冲突时，必须裁决，只保留一个结论，并说明为什么另一个不成立。
6. 标题：# 市场日报 — ${DATE}（${SESSION_LABEL_CN}）
7. 末尾：*AI分析，仅供参考，不构成投资建议。*
8. 全文控制在1500字以内。宁可少写也不要废话。
9. 正文禁止直接出现内部桶名或英文 lane 名。不要写 CORE BOOK、TACTICAL CONTINUATION、event tape、appendix；统一改写成对外表述，如“保留名单”“条件式延续观察”“继续跟踪名单”。
10. 语言规则（极其重要，严格遵守）：
   - 输出必须是**流畅的中文**，不是中英混杂。读起来应该像一篇中文研报，不像把英文片段塞进中文句子。
   - **只有以下内容保留英文**：股票/ETF代码（SPY, NIO等）、缩写指标名（R:R, P/C, IV, ATM, VIX, HMM, EPS, DTE, ATR, EWMA）、数据库字段名（trend_prob, p_upside等）
   - **必须翻译为中文的**：所有描述性词汇和短语。例如：
     - bear regime → 熊市状态
     - forced liquidation → 强制平仓
     - degenerate cone → 退化概率锥
     - mean-reverting → 均值回归
     - trending → 趋势态
     - noisy → 震荡态
     - flight-to-safety → 避险
     - exhaustion → 动能耗竭
     - conviction → 置信度
     - chasing → 追高
     - edge → 优势/胜率
     - catalyst → 催化剂
     - breadth → 市场宽度
     - entry/stop/target → 入场/止损/目标
     - upgrade/downgrade → 上调/下调
     - single-factor bet → 单因子押注
     - dead-cat bounce → 死猫反弹
     - gap down → 跳空下跌
   - 如果一个句子翻译后读起来别扭，就整句重写成自然中文，不要逐词翻译
11. 所有股票/ETF代码用粗体标记
12. 每个写进“做多”的名字必须附失效条件和风险参数；做不到就不要写进“做多”
13. Tactical Event Tape 如果主要由小盘/高波动标的构成，必须明确写成“战术观察”
14. 专业直白，有观点。只用分析师提供的数字，不编造。
15. 如果没有符合条件的多头机会，就明确写“今天没有可直接做的多头，别追价”
16. 禁止因为 HMM 的 state 或 P(bull) 单独把全文主线写成“熊市延续”或“牛市确认”；必须由 Fear/Greed、SPY RSI、VIX、宽度、利率/信用共同支持。不能仅凭 headline 否决通过执行硬规则的个股 alpha。
17. 如果 Report Review 说主问题是“追晚了”或“edge 太薄”，不要把当天的失败解释成“判断正确，只是市场太快”。那仍然是今天不能追的理由。
18. 如果 Report Review 说主问题是“漏 alpha”，就要在交易地图或风险与展望里明确写出：系统更容易漏掉 follow-through，而不是只会追已经走完的名字。
19. 这是 long-only 日报。禁止写“做空”“反手空”“空头对冲可以做”“双杀可空”这类可执行空头语言；如果市场偏弱，只能写成“空仓/减仓/回避/等待更优买点”。
20. 精度与不确定性规则（强制）：
   - 禁止使用 P=1.00 或 P=0.00。优先使用“约55%”“接近抛硬币”“证据偏弱”这种粗粒度表述
   - 引用概率或 z 值时标注样本量；样本量 < 30 时，不要堆三位小数
   - 宏观数据'Ref Period'距交易日>14天时，必须注明滞后："CPI 2.66%（1月数据，滞后约2个月）"
   - 区分三类概率——不得混用：
     (1) 模型概率：算法计算（HMM、Beta-Binomial）
     (2) 历史基率：历史数据中的频率（trend_prob、p_upside）
     (3) 风险中性概率：期权价格隐含（非真实世界概率）
   - 样本量小于30时必须标注。"基于8次历史盈利事件"比"上行概率 62.5%"更诚实
   - 引用 Polymarket 数据时注明抓取时间和成交量——概率是时间截面快照
   - 如果 HMM 校准数据存在，讨论状态置信度时必须引用 Brier 分数和命中率，并明确它不是牛熊裁判
   - 不得把 regime day-count 当作稳定性证明；最多写成“状态刚切换/已持续一段时间”
   - 情景概率必须标注为主观估计，不是计算值
PROMPT

run_agent_with_fallback \
    "merge-report" \
    "$OUT_DIR/prompts/merge-report.txt" \
    "$OUT_DIR/outputs/merge-report.md" \
    "$OUT_DIR/logs/merge-report.log" \
    "$MERGE_TIMEOUT" \
    "$MIN_MERGE_BYTES"

# Copy to reports/
ZH_REPORT="reports/${DATE}_report_zh_${SESSION}.md"
if [ -s "$OUT_DIR/outputs/merge-report.md" ]; then
    cp "$OUT_DIR/outputs/merge-report.md" "$ZH_REPORT"
    echo "  Final report: $ZH_REPORT"

    echo "  Syncing Factor Lab stock list..."
    if ! PYTHONPATH="$PROJECT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON_BIN" \
        "$PROJECT_DIR/scripts/sync_factor_lab_report.py" \
        --report "$ZH_REPORT" \
        --structural "$PROJECT_DIR/reports/${DATE}_payload_structural_${SESSION}.md"; then
        echo "  Factor Lab stock list sync failed (non-fatal)"
    fi

    # Append Factor Lab experiment report section (non-fatal)
    echo "  Appending Factor Lab section..."
    if ! "$PYTHON_BIN" "$FACTOR_LAB_ROOT/scripts/generate_factor_report.py" \
        --date "$DATE" \
        --append-to "$OLDPWD/$ZH_REPORT"; then
        echo "  Factor Lab section append failed (non-fatal)"
    fi
    cd "$OLDPWD"
fi
