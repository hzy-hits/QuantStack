# Report Pipeline Redesign: Extractor → Narrator Architecture

**Date**: 2026-04-09
**Scope**: CN (quant-research-cn) + US (quant-research-v1) multi-agent report pipelines
**Goal**: Fix report quality — eliminate contradictions, create coherent narratives, cut bloat

## Problem Summary

The current pipeline produces reports with:
- Contradictions between sections (4 independent analysts each form opinions, merge can't reconcile)
- Checklist-style output (10 mandatory sections, no narrative thread)
- Factor Lab content duplicated 2-3 times (payload injection + agent output + post-append)
- 87KB merge input for a 1500-word output (60:1 compression ratio, impossible to do well)
- Scorecard mostly "inconclusive" (evaluating 3-day signals after 1 day)
- Same data-gap warnings repeated daily (10Y yield missing, northbound null)
- US agents write English, merge translates + synthesizes simultaneously

## Architecture Change

```
OLD:  payload(40KB) → 4 analysts(each 800-1200w prose) → merge(87KB input) → report
NEW:  payload(40KB) → 4 extractors(each ~500w structured) → narrator(~6KB input) → report
```

**Key principle**: Specialists extract facts. One narrator tells the story.

## Extractor Design

### Shared Rules (all 4 extractors)

- Output language: **Chinese** (both CN and US pipelines)
- Output format: structured markdown with fixed headers and tables
- Each extractor ends with a `## 判断` block: **exactly 3 sentences**, each backed by a number from the payload
- No narrative prose, no hedging language, no "综合考量/谨慎乐观/值得关注"
- If data is missing, write `[缺失]` in the cell — do not analyze the gap
- Do not form trade recommendations — only the narrator does that
- Total output target: 400-500 words per extractor

### Macro Extractor

**Input**: `{date}_payload_macro.md` + US macro payload (if available) + previous report one-liner
**Output schema**:

```markdown
## Regime
- state: [bull|bear|range]
- P(bull): [value], n=[obs], duration=[days]
- Brier: [value], hit_rate: [pct], n=[obs]
- p_ret_positive: [value]

## Rates
| 指标 | 值 | 变化 |
|------|-----|------|
| Shibor隔夜 | x% | Δ±xbp |
| 10Y国债 | x% 或 [缺失] | |
| LPR_1Y | x% | 不变/Δ |

## Macro
| 指标 | 值 | 参考期 | 滞后 |
|------|-----|--------|------|
| CPI | x% | YYYY-MM | ~N月 |
| PPI | x% | YYYY-MM | ~N月 |
| PMI | x | YYYY-MM | ~N月 |
| M2 | x% | YYYY-MM | ~N月 |

## Funds
- 融资余额: [value] (截至[date], 缺失区间[if any])
- 北向: [value 或 null]
- 流入TOP3: [行业+金额] x3
- 流出TOP3: [行业+金额] x3

## Cross-Market
- 黄金: Au(T+D) [pct], Ag(T+D) [pct]
- 原油: SC主力 [pct]
- US(if available): SPY [pct], VIX [value], HY spread [value](Δ[change])

## Gate
- multiplier: [value]
- vol_state: [low|elevated|high]
- yield_state: [normal|inverted]

## 判断
[3 sentences max, each with a number. Domain: macro/regime/cross-market interpretation.]
```

### Quant Extractor

**Input**: `{date}_payload_structural.md` + previous report one-liner
**Output schema**:

```markdown
## Core Book
| 代码 | 名称 | 方向 | composite | regime | 5D% | 20D% | trend_prob | info_score | 资金方向 | 冲突 |
|------|------|------|-----------|--------|-----|------|------------|------------|----------|------|
(one row per CORE item, max ~12 rows)

## Composite拆解 (仅CORE HIGH)
For each HIGH item:
- magnitude: [value]
- information: [value] (驱动: [top 2 components])
- momentum/reversion/breakout: [value] (当前regime权重)
- event: [value]
- cross_asset: [value]

## Theme Rotation
- [主题名]: [N]信号 ≈ [M]独立赌注
- 方向: [N多/N空]

## Regime Distribution
- trending: [pct], noisy: [pct], mean_reverting: [pct]
- trend_prob range: [min]-[max] (span=[value])

## Exhaustion Flags
| 代码 | 信号 |
|------|------|
(stocks where 20D return extreme + trend_prob diverges + turnover spike)

## 判断
[3 sentences max. Domain: signal quality, discriminative power, key conflicts.]
```

### Event Extractor

**Input**: `{date}_payload_events.md` + previous report one-liner
**Output schema**:

```markdown
## Earnings
| 代码 | 名称 | 类型 | p_change范围 | p_upside | 样本量 |
|------|------|------|-------------|----------|--------|

## Unlock (未来30天)
| 日期 | 代码 | 名称 | unlock_ratio | 类型 |
|------|------|------|-------------|------|

## Catalysts
### 新鲜 (<24h)
| 代码 | 催化剂 | 来源数 | confidence |
### 已消化 (>3天)
| 代码 | 催化剂 | 状态 |

## Shareholder Actions
| 代码 | 行为 | 规模 | 主体 |

## Calendar (未来7-30天)
| 日期 | 事件 | 代码 |

## 判断
[3 sentences max. Domain: catalyst freshness, earnings surprise direction, unlock pressure.]
```

### Risk Extractor

**Input**: `{date}_payload_structural.md` + `{date}_payload_macro.md` + previous report one-liner
**Output schema**:

```markdown
## Concentration
- 方向: [N多/N空] ([pct]多头, [>70%则标注警戒])
- 行业: [cluster描述], ≈[M]独立赌注
- 因子: [主要暴露]

## Leverage
| 代码 | 融资余额 | 融资5D变化 | 风险等级 |

## Invalidation (每个CORE HIGH)
| 代码 | 失效条件 |
|------|----------|
(具体、可观察、可量化的条件)

## Scenarios
- bull触发: [1-2 conditions]
- bear触发: [1-2 conditions]
- range特征: [1-2 observations]

## 判断
[3 sentences max. Domain: what could go wrong, biggest unhedged risk, concentration danger.]
```

## Narrator (Merge Agent) Design

### Input (~6KB total)

| Source | ~Size | Content |
|--------|-------|---------|
| 4 extractions | 2000w | Structured data + 4×3 judgment sentences |
| Core payload digest | 1500w | Core Book raw data for cross-validation |
| Previous scorecard | 200w | Previous one-liner + matured signal results only |
| Factor Lab signal | 300w | Stock table (code/price/position) from payload |

### Narrator responsibilities

- **Owns**: opinion formation, direction calls, narrative construction, contradiction resolution
- **Does not own**: number computation, data extraction
- If two extractors' `## 判断` conflict, narrator must pick one side and state why
- Every claim must trace to a number in the extractions or core payload

### Output format (6 sections, 1000-1500 words)

```markdown
# 市场日报 — {date}

## 一句话
（30字以内。方向+理由+行动。）

## 信号记分卡
（仅已到期信号。CORRECT/WRONG/脱出。没有到期的写"本期无到期信号"。
 禁止"待验"。）

## 今日市场
（regime + 利率 + 资金面 + 跨市场。一段连贯叙事，5-8句。
 不是分项罗列，是一个有逻辑的段落。）

## 交易地图
分三栏：

### 做多
每只：一句话逻辑 + 入场/止损/止盈 + 失效条件

### 做空
每只：同上

### 观望
不做但值得跟踪的，一句话说为什么不做

### Factor Lab
选股表嵌在此处（代码/名称/买入价/止损/止盈/仓位）。
如有与主系统的交叉确认，标注。

## 风险与展望
集中度警告 + 三情景(各2句) + 未来3-7天关键催化剂/事件。

## 附注
一行永久性disclaimer："10Y国债、北向资金持续缺失；宏观数据滞后见括号标注。不构成投资建议。"
```

### Language rules (unchanged from current, applied to narrator only)

- Output: fluent Chinese, reads like a professional research note
- English preserved only for: stock codes, abbreviation indicators (R:R, P/C, IV, HMM, EPS, VIX, ATR, EWMA, ETF, LPR, Shibor, PMI, CPI, PPI), database field names (trend_prob, composite_score, etc.)
- Forbidden: 综合考量, 谨慎乐观, 值得关注, 密切跟踪, 不确定性较大
- Tone: hedge fund morning call — cold, precise, aggressive, data-driven
- Every judgment backed by a number
- Stock codes in bold

### Precision rules (unchanged, applied to narrator only)

- No P=1.00 or P=0.00 — use P≈1.00
- Probabilities must include sample size
- Macro data must include reference period and lag
- Distinguish: model probability / historical base rate / composite score
- Small samples (n<30) must be flagged
- Scenario probabilities labeled as subjective estimates
- Every HIGH signal must have an invalidation condition

## Factor Lab: Single Path

```
BEFORE: payload injection → agent sees it → merge writes it → pipeline appends raw → experiment report appends
AFTER:  payload injection → extractor extracts it → narrator integrates into "交易地图" → done
```

### Files to change

**CN pipeline (`quant-research-cn`):**
- `scripts/run_agents.sh` lines 456-463: **DELETE** the `generate_factor_report.py --append-to` call
- `scripts/daily_pipeline.sh` lines 99-111: KEEP (this is the single injection point into structural payload)

**US pipeline (`quant-research-v1`):**
- `scripts/run_agents.sh` lines ~555-558: **DELETE** the `generate_factor_report.py --append-to` call
- `scripts/run_full.sh` lines 212-225: KEEP (single injection point into structural payload)

**Factor Lab experiment report** (session stats, OOS results, research notes): removed from daily reports entirely. This is internal factor-lab bookkeeping, not reader-facing content.

## Scorecard Logic

```
for each signal in previous_report:
    if days_held >= holding_period:
        → CORRECT / WRONG (with return %)
    elif signal exited notable list:
        → 脱出 (last known price if available)
    else:
        → skip (do not mention in report)

if no signals matured:
    → "本期无到期信号"
```

No more "待验（1日）". Scorecard is either informative or absent.

## Context Assembly Changes

**`build_agent_context.py`** (both pipelines) needs a new output: `merge_digest.md`

This is a compact version of core payload data (~1500 words) containing:
- Core Book items with raw numbers (for narrator cross-validation)
- Factor Lab signal table
- Previous report one-liner and matured signals

The existing `macro.md`, `structural.md`, `events.md` context files continue to feed extractors. The new `merge_digest.md` feeds the narrator alongside the 4 extractions.

## Files Changed (complete list)

| File | Pipeline | Change |
|------|----------|--------|
| `prompts/macro-analyst.md` | CN | Rewrite: analyst → extractor |
| `prompts/quant-analyst.md` | CN | Rewrite: analyst → extractor |
| `prompts/event-analyst.md` | CN | Rewrite: analyst → extractor |
| `prompts/risk-analyst.md` | CN | Rewrite: analyst → extractor |
| `prompts/merge-agent.md` | CN | Rewrite: 10 sections → 6, narrator role |
| `scripts/run_agents.sh` | CN | Delete Factor Lab append (lines 456-463) |
| `scripts/run_agents.sh` | US | Rewrite 4 extractor heredocs + merge heredoc, all Chinese |
| `scripts/run_agents.sh` | US | Delete Factor Lab post-append (lines ~555-558) |
| `scripts/build_agent_context.py` | CN | Add `merge_digest.md` output |
| `scripts/build_agent_context.py` | US | Add `merge_digest.md` output (if separate file) |

**Not changed**: Rust code, analytics modules, fetchers, DuckDB schema, Factor Lab code, email scripts.

## Success Criteria

1. Merge agent input < 8KB (down from 50-87KB)
2. Final report 1000-1500 words (no section padding)
3. Factor Lab appears exactly once in the report
4. No "待验" in scorecard
5. No multi-paragraph data quality analysis (one-line disclaimer)
6. No leaked pipeline instructions in final output
7. Contradictions between extractors resolved by narrator with stated reasoning
8. US and CN reports use identical section structure
