#!/bin/bash
# Run 4 analysis agents in parallel using claude -p (stdin), then merge
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

CLAUDE_BIN="${CLAUDE_BIN:-claude}"
CODEX_BIN="${CODEX_BIN:-codex}"
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
for required_f in "reports/${DATE}_payload_macro.md" "reports/${DATE}_payload_structural.md" "reports/${DATE}_payload_news.md"; do
    [ -s "$required_f" ] || { echo "ERROR: Required file missing or empty: $required_f"; exit 1; }
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

    if [[ "$CLAUDE_AVAILABLE" -eq 1 ]]; then
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
    fi

    if [[ "$CODEX_AVAILABLE" -eq 1 ]]; then
        echo "[codex] starting ${agent_name}" >> "$log_file"
        if "$TIMEOUT_BIN" "$timeout_secs" "$CODEX_BIN" exec \
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
    fi

    echo "  ✗ ${agent_name} failed on all available backends"
    return 1
}

# Build previous report context
PREV_CONTEXT=""
if [ -n "$PREV_REPORT" ] && [ -f "$PREV_REPORT" ]; then
    PREV_CONTEXT="

--- PREVIOUS REPORT (for hypothesis validation) ---
$(cat "$PREV_REPORT")
--- END PREVIOUS REPORT ---

If the previous report exists above, START by briefly noting which predictions played out and which didn't (2-3 sentences). Then proceed with today's analysis."
fi

echo "  Launching 4 agents in parallel..."

python3 "$PROJECT_DIR/scripts/build_agent_context.py" \
    --date "$DATE" \
    --reports-dir "$PROJECT_DIR/reports" \
    --out-dir "$OUT_DIR/context"

# Write prompts to temp files (avoids ARG_MAX for large payloads)

# ── Agent 1: Macro ─────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/macro-analyst.txt" <<PROMPT
You are a senior macro strategist at a quantitative research firm.

Read the data below and output analysis (800 words max, English).

--- TODAY'S MACRO DATA ---
$(cat "$OUT_DIR/context/macro.md")
--- END DATA ---
${PREV_CONTEXT}

## Macro Regime Assessment
What kind of market environment are we in? Use VIX, SPY, sector performance, breadth data.

## Rates, Inflation & Credit
What do Fed Funds, 10Y yield, credit spreads, and the yield curve signal?

## Narrative Drivers
What themes dominate the headlines?

## Polymarket Signals
What are participants betting on?

## Options Extremes
What do the most extreme bullish and bearish option positions reveal?

## Universe Breadth
What does the advance/decline distribution tell us about market health?

## Key Contradictions
Identify 2-3 data points that contradict each other (e.g. VIX elevated but credit calm). For each:
- OBSERVED: what the data shows
- MOST LIKELY: your best interpretation (label as inference)
- UNVERIFIED: what would confirm/deny

## Precision & Uncertainty Rules (MANDATORY)
- Never present model probabilities as P=1.00 or P=0.00. Use "P≈1.00" or "near-certainty" — models have estimation error.
- When citing any probability or z-score, note the sample size: "P(bull)=0.98 (n=487 obs)".
- For macro data where 'Ref Period' is >14 days before trade date, note the lag: "CPI 2.66% (Jan data, ~2-month lag)".
- Distinguish three probability types — never conflate:
  (1) Model probability: computed by algorithm (HMM, Beta-Binomial)
  (2) Historical base rate: frequency in past data (trend_prob, p_upside)
  (3) Risk-neutral probability: implied by options prices (NOT real-world odds)
- If HMM calibration data is present, cite Brier score and hit rate when discussing regime confidence.
- Polymarket probabilities are snapshots — note the fetched timestamp and volume when citing.

Rules: All numbers must come from the data. Be direct and opinionated. Distinguish facts from inferences. No investment advice.
PROMPT

# ── Agent 2: Quant ─────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/quant-analyst.txt" <<PROMPT
You are a senior quantitative analyst specializing in regime detection and options flow.

Read the data below and output analysis (1000 words max, English).

This is a compact structural context: highest-signal items are expanded, lower-signal items are only summarized in counts.
The structural payload is split into three lanes:
- Core Book = main report candidates, higher tradability / confirmation
- Tactical Event Tape = high-volatility or smaller event-driven names
- Appendix / Radar = background anomalies only, not main thesis material

--- TODAY'S STRUCTURAL DATA ---
$(cat "$OUT_DIR/context/structural.md")
--- END DATA ---
${PREV_CONTEXT}

If previous report exists, START with signal scorecard: for each prior HIGH, state predicted direction, actual move, verdict (CORRECT/WRONG/INCONCLUSIVE). Do NOT reframe wrong calls as "risk warning validated".

## Thematic Baskets
BEFORE individual items, identify 3-5 correlated clusters. Items in the same basket are NOT independent bets.

## Core Book
Start with the Core Book lane. Cover at most 6 symbols/baskets in total across Core Book + Tactical Event Tape.
If the core set is thin but cleaner than the event tape, say that explicitly instead of padding with noisy names.

For each Core Book HIGH item:
- Signal logic: how regime + momentum + options align (2 sentences)
- Time frame: holding period assumption (1D/3D/1W/2W) based on cone expiry or momentum window
- Risk parameters from probability cone: entry / stop (68% boundary against) / target (68% boundary with) / R:R
- Invalidation trigger: one specific, observable condition that kills the thesis
- Three layers: OBSERVED (data fact) / MOST LIKELY (inference) / UNVERIFIED (needs confirmation)

## Tactical Event Tape
Treat event-tape names as tactical and lower-capacity by default. If a name is a small/mid-cap anomaly, say so plainly.
Only elevate an Event Tape name if the data is materially cleaner than the Core Book.

## Exhaustion & Reflexivity Checks
For each Core Book HIGH item, check Exhaustion Flags and assess if the move is "already done".

## MODERATE Items
Group by thematic basket. Directional lean, key uncertainty, upgrade/downgrade conditions.

## Appendix / Radar
Only mention the most informative divergences or watchlist names from Appendix / Radar. Keep it brief.

## Precision & Uncertainty Rules (MANDATORY)
- Never present model probabilities as P=1.00 or P=0.00. Use "P≈1.00" — models have estimation error.
- When citing probabilities, note sample size: "trend_prob=0.62 (n=45 obs in this regime×vol_bucket cell)".
- State n_obs or sample size when it's small (<30). "Based on 8 historical events" is more honest than "P=0.625".
- Distinguish: model probability (HMM, Beta-Binomial) vs historical base rate (trend_prob) vs risk-neutral probability (options-implied). Never conflate.
- Options cone boundaries are risk-neutral ranges, NOT real-world odds. Say "options market prices X" not "there is a 68% chance".
- If HMM calibration is present, cite Brier/hit rate when discussing regime confidence.

Rules: All numbers from the data. Distinguish facts from inferences. No investment advice.
PROMPT

# ── Agent 3: News ──────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/news-analyst.txt" <<PROMPT
You are a senior event-driven analyst covering catalysts, filings, and information asymmetry.

Read the data below and output analysis (1000 words max, English).

This is a compact event context aligned to the highest-priority symbols from the structural signal set.
Respect the report lanes: Core Book first, Tactical Event Tape second, Appendix / Radar only as a watchlist.

--- TODAY'S NEWS & EVENTS DATA ---
$(cat "$OUT_DIR/context/news.md")
--- END DATA ---
${PREV_CONTEXT}

## Core Narrative Themes
Distill 3-5 dominant themes from all news and filings.

## Core Book Catalyst Analysis
For each Core Book HIGH item:
- Catalyst: cite specific headline/filing
- Freshness: <24h (fresh), 1-3 days (aging), >3 days (stale)
- Persistence: one-time event or structural (multi-week)?
- Event risks: upcoming earnings, regulatory, macro events

## Catalyst Reflexivity
For each Core Book HIGH item, assess:
- Is this catalyst already widely priced in (reported by 3+ sources, >2 days old)?
- Does the headline contain its own negation?
- One-time catalysts exhaust faster than structural ones.

## Tactical Event Tape
For event-tape names, prioritize freshness, rumor risk, and liquidity/capacity caveats over long narrative expansion.

## News Attribution Warnings
Flag items where: headlines about a DIFFERENT company (misattributed), news >3 days old driving current signal, no news for event-driven signals.

## MODERATE Item Event Risks
Group by theme. What news/filings drive each? Near-term earnings risk?

## Data Quality Warnings
Which items have missing/stale data? Which rely on low-quality sources?

## Precision & Uncertainty Rules (MANDATORY)
- When earnings guidance is mentioned, verify direction: "raised guidance" vs "lowered guidance" — do NOT assume from beat/miss alone.
- Polymarket probabilities are point-in-time snapshots. Cite fetched timestamp and volume when referencing.
- Macro data (CPI, unemployment) is lagged — cite the reference period, not the trade date.
- If a catalyst comes from only 1 source, flag as "single-source, unverified".

Rules: Cite specific headlines and filing descriptions. Do not fabricate. No investment advice.
PROMPT

# ── Agent 4: Risk ──────────────────────────────────────────────────────────
cat > "$OUT_DIR/prompts/risk-analyst.txt" <<PROMPT
You are a senior risk manager at a quantitative research firm. Your job is to stress-test signals and quantify risk — NOT to find trades.

Read the data below and output analysis (800 words max, English).

This is a compact risk context: use it to find the dominant correlated bets, not to exhaustively enumerate every low-signal name.
Treat Core Book and Tactical Event Tape as different capacity/risk buckets.

--- TODAY'S STRUCTURAL DATA ---
$(cat "$OUT_DIR/context/structural.md")
--- END STRUCTURAL DATA ---

--- TODAY'S MACRO DATA ---
$(cat "$OUT_DIR/context/macro.md")
--- END MACRO DATA ---

## Correlation Clusters
Group ALL notable items into correlated baskets (same sector, same macro factor, shared options chain). State: "these N items represent ~M independent bets."

## Risk Parameters for HIGH Signals
Start with Core Book HIGH signals. Only include Event Tape HIGH signals if they are among the top tactical names.
For each selected HIGH item, extract from probability cone:
| Symbol | Direction | Entry | Stop (68% boundary against) | Target (68% boundary with) | R:R | DTE |
If no cone data, state "no cone available".

## Invalidation Triggers
For each HIGH signal, ONE specific observable condition that kills the thesis. Be concrete (price levels, breadth thresholds), not vague ("if macro deteriorates").

## Scenario Analysis
For the dominant macro theme, THREE scenarios:
1. Bull case: what confirms risk-on extension
2. Bear case: what triggers reversal
3. Messy middle (MOST LIKELY): partial resolution, continued ambiguity

## Portfolio-Level Warnings
- Which signals create concentrated exposure to a single factor if all executed?
- Net directional tilt across HIGH+MODERATE?
- Any natural hedges within the signal set?

## Precision & Uncertainty Rules (MANDATORY)
- Never present model probabilities as P=1.00 or P=0.00. Use "P≈1.00" — estimation error is real.
- Scenario probabilities must be labeled as subjective estimates, not computed values.
- When citing HMM regime duration statistics, note: transition probabilities are in-sample estimates, not forward guarantees. "Cumulative flip probability" assumes constant transition rate (Markov property) — flag this assumption.
- If HMM calibration metrics are available, cite them. If Brier score ≥ 0.25 (no better than climatology), downgrade all regime-based claims.
- Distinguish correlation (observed co-movement) from causation in cluster analysis.

Rules: All numbers from the data. Be conservative and skeptical. No investment advice.
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
if [ -n "$PREV_REPORT" ] && [ -f "$PREV_REPORT" ]; then
    PREV_MERGE_CONTEXT="

--- 上一份日报 ---
$(cat "$PREV_REPORT")
--- END ---

请在「上期信号记分卡」部分严格评判：每个上期HIGH信号的预测方向与实际走势。信号错就是错，不要包装成\"风险警告验证\"。"
fi

cat > "$OUT_DIR/prompts/merge-report.txt" <<PROMPT
你是一个说人话的投资助手。请阅读四个分析师的英文分析，写成一篇普通人能看懂的中文市场日报。

写作风格要求（极其重要）：
- 写作风格：像顶级对冲基金的晨会纪要。冷静、精准、有攻击性。每句话都有信息量。
- 数字驱动：每个判断必须附数字。不说"资金流出明显"，说"净流出8.5亿，连续3天"。
- 有立场：选一个方向，给理由，给失效条件。不要两面讨好。
- 禁用词：综合考量、谨慎乐观、值得关注、密切跟踪、不确定性较大。
- 上次错了一句话说清楚，不要包装。
- 全文1200-1500字。精炼是能力。
- Factor Lab 选股清单必须完整列出。

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
1. 结构（简洁，每部分说重点）：
   - **一句话总结**：今天市场怎样？该做什么？（一句话，不超过30字）
   - **上次对不对**：上次推荐的股票涨了还是跌了？赚了多少亏了多少？直说对错。
   - **今天大盘**：涨跌原因 + 关键利率数据。必须包含：联邦基金利率、10年美债收益率、10Y-2Y利差、高收益利差、VIX，标注相比前一天的变化方向和幅度（如"10Y: 4.33% ↓6bp"）
   - **值得买的**（如果有）：每只写清楚——为什么买、买入价、止损价、止盈价、持多少天。没有就写”今天没有好机会，别动”
   - **要小心的**：哪些股票有风险？在跌的？有坏消息的？
   - **Factor Lab 选股**：如果payload里有Factor Lab的选股清单，原样列出所有股票的代码、名称、买入价、止损、止盈、仓位。这是独立的量化选股建议，必须完整展示。
   - **接下来看什么**：未来3天关注什么事件
2. 四个分析师有冲突的话，直接选一个最靠谱的，说清楚为什么另一个不对
3. 标题：# 市场日报 — ${DATE}
4. 末尾：*AI分析，仅供参考，不构成投资建议。*
4.5. 全文控制在1500字以内。宁可少写也不要废话。
5. 语言规则（极其重要，严格遵守）：
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
6. 所有股票/ETF代码用粗体标记
7. 每个HIGH信号必须附失效条件和风险参数——没有就不完整
7.5. Tactical Event Tape 如果主要由小盘/高波动标的构成，必须明确写出“战术观察”而不是“主报告主线”。
8. 专业直白，有观点。只用分析师提供的数字，不编造。
8.5. 如果没有HIGH信号，就明确写“今日无HIGH信号”，不要强行拔高MODERATE。
9. 精度与不确定性规则（强制）：
   - 禁止使用 P=1.00 或 P=0.00。使用"P≈1.00"或"极高概率"——模型存在估计误差
   - 引用概率或z值时标注样本量："P(牛市)≈0.998（n=487观测，模型已收敛）"
   - 宏观数据'Ref Period'距交易日>14天时，必须注明滞后："CPI 2.66%（1月数据，滞后约2个月）"
   - 区分三类概率——不得混用：
     (1) 模型概率：算法计算（HMM、Beta-Binomial）
     (2) 历史基率：历史数据中的频率（trend_prob、p_upside）
     (3) 风险中性概率：期权价格隐含（非真实世界概率）
   - 样本量小于30时必须标注。"基于8次历史盈利事件"比"P(上行)=0.625"更诚实
   - 引用 Polymarket 数据时注明抓取时间和成交量——概率是时间截面快照
   - 如果 HMM 校准数据存在，讨论状态置信度时必须引用 Brier 分数和命中率
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
        --structural "$PROJECT_DIR/reports/${DATE}_payload_structural.md"; then
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
