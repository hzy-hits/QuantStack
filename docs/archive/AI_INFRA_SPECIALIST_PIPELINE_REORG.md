# AI Infra Specialist Pipeline Reorg

> **ARCHIVED 2026-06-10** — 已完成:AI-infra 专业户改造已落地(universe/证据门/research.* 任务群均在生产)。现状见 docs/ARCHITECTURE.md。

Date: 2026-05-13

## Goal

Turn Quant Stack into an AI-infra specialist fund pipeline instead of a broad
market screener.

The pipeline may still fetch broad market data for benchmark, liquidity,
macro and beta context. It must not rank, size or report non-AI names as
production candidates unless they are benchmark or hedge instruments.

## Agent Entry

Future agents should start from `AGENTS.md`. That file turns this reorg plan and
the `ai_infra` method documents into concrete operating rules, read order,
promotion flow and common commands.

## Source Method Documents

This reorg is based on these `ai_infra` method documents:

- `ai_infra/docs/fund-management-philosophy.md`: AI Infra BFS fund philosophy;
  start from LLM demand instead of stock stories.
- `ai_infra/docs/llm-dependency-bfs-framework.md`: BFS depth D0-D5, dependency
  edges and A-share mapping.
- `ai_infra/docs/company-financials-market-options-methodology.md`: evidence,
  financials, K-line and options layers; evidence first, market second, options
  third.
- `ai_infra/docs/research-checklist.md`: company research checklist.
- `ai_infra/docs/source-evidence-template.md`: source evidence card template.
- `ai_infra/docs/credit-financing-evidence-card-template.md`: credit and
  financing refutation layer.
- `ai_infra/docs/firm-power-evidence-card-template.md`: firm power and grid
  refutation layer.

## Operating Contract

```text
ai_infra source review -> AI universe -> market data/features -> sleeves ->
portfolio allocation -> benchmark attribution -> final report
```

The hard boundary is the AI universe:

- Production candidates must come from `ai_infra/data/global_universe_v2.jsonl`
  or a source-reviewed promotion output.
- Expansion candidates from Factor Lab, BFS discovery or news/filing extraction
  remain research-only until the source-review path promotes them.
- Broad market scans can discover anomalies, but outside-universe names can only
  enter `ai_infra/reports/expansion_candidates_v1.csv`.

## Data Scope

### Tradable/Research Universe

Use these as the source of truth:

- `ai_infra/data/global_universe_v2.jsonl`
- `ai_infra/reports/source_verification_queue_v1.csv`
- `ai_infra/reports/us_alpha_mining_queue_v1.csv`
- `ai_infra/reports/expansion_candidates_v1.csv`
- `data/ai_supply_chain_relationships.yaml`
- `data/ai_supercycle_taxonomy.yaml`
- `data/us_theme_seed_map.yaml`

### Allowed Market Context

Benchmarks and hedges are allowed even though they are not AI companies:

| Market | Benchmarks | Purpose |
| --- | --- | --- |
| US | `SPY`, `QQQ`, `SMH` | broad beta, growth beta, semiconductor beta |
| CN | `000001.SH`, `399001.SZ`, `399006.SZ`, `000300.SH` | 上证、深成指、创业板、沪深300 context |
| CN hedge/proxy | `510300.SH`, `510500.SH`, futures if available | beta hedge and drawdown attribution |

These instruments can appear in reports only as benchmark, hedge, beta or
relative-performance context. They cannot become stock-selection candidates.

## Strategy Lanes

### 1. K-Line / Tape Timing

Purpose: decide entry timing, trend quality, pullback/retest and stop behavior
inside the AI universe.

US:

- Main sleeve: `us_theme_cluster_momentum`.
- Inputs: price, volume, relative strength, AI layer breadth, options/flow as
  confirmation.
- Output: stock-only long candidate or ranked watch.

CN:

- Main sleeve: `cn_tape_leadership_continuation`.
- Inputs: price, amount ratio, money flow, sector co-move, AI-supercycle layer,
  T+1/T+3 lifecycle evidence.
- Output: positive-EV setup, ranked watch or blocked risk.

Rule: K-line/tape cannot prove a supplier/customer relationship. It only times a
source-reviewed or universe-approved company.

### 2. Options / Convexity

US options are a confirmation and expression-quality layer:

- IV rank, skew, VRP, put/call flow and bid/ask quality can upgrade or haircut
  stock sizing.
- Option-leg PnL must stay in the shadow/options ledger until it has enough
  real bid/ask history.
- Options cannot create a non-universe candidate.

CN shadow option is risk/convexity modeling only. It is not executable
single-name option trading.

### 3. Factor Lab

Factor Lab is a research engine, not the final trader.

Allowed outputs:

- AI-infra factor hypothesis.
- Price/tape prior.
- `DATA_REQUIREMENTS`.
- Source-review expansion candidate.
- Sleeve-return ledger for later promotion.

Not allowed:

- Directly adding non-reviewed companies to production rankers.
- Treating price-only formulas as proof of an AI supply-chain relationship.

### 4. Financials / Source Evidence

Factor Lab and `ai_infra` should read filings, earnings transcripts, official
press releases and source-linked news to fill:

- segment revenue and AI exposure,
- backlog/orders/RPO,
- capex and capacity,
- gross margin and inventory,
- customer concentration,
- debt, leases and credit stress,
- source-confirmed supplier/customer relationship,
- counterevidence.

Only source-confirmed evidence can enter `data/ai_supply_chain_relationships.yaml`.

## Portfolio Allocation

Allocation should be layer-aware, not just ticker-ranked.

Required dimensions:

- AI layer: compute, HBM/storage, networking, optical/CPO, packaging/test,
  power/cooling/grid, neocloud/data center, industrial capex, hard assets.
- Market: US vs CN.
- Sleeve: US theme cluster, CN tape leadership, options shadow, Factor Lab
  research, source-review-only.
- Risk: beta, sector concentration, correlation cluster, liquidity, event,
  earnings, credit and evidence quality.

Suggested sizing stack:

```text
base sleeve score
* evidence quality multiplier
* tape/entry quality multiplier
* options/flow confirmation multiplier
* portfolio concentration haircut
* benchmark beta hedge adjustment
= final stock R
```

No row should receive production R unless:

- it is inside the AI universe or source-reviewed promotion output,
- it has a valid sleeve/state,
- benchmark-relative and beta-hedged performance can be measured,
- the final report can explain why it is not merely a story stock.

## Benchmark Evaluation

Every sleeve and production report should be judged in three ways:

1. Absolute return: raw long-only return after costs.
2. Relative return: return minus benchmark return.
3. Hedged alpha: stock basket return plus/minus beta hedge return.

Required benchmark tables:

| Scope | Benchmarks | Metrics |
| --- | --- | --- |
| US AI book | `SPY`, `QQQ`, `SMH` | excess return, beta, alpha, drawdown, hit rate, information ratio |
| CN AI book | `000001.SH`, `399001.SZ`, `399006.SZ`, `000300.SH` | excess return, beta, alpha, drawdown, hit rate, information ratio |
| Cross-market | US AI book vs CN AI book | correlation, active dates, contribution by layer |
| Sleeve level | own benchmark basket | LCB80, LCB95, win rate, max drawdown, turnover, concentration |

Minimum report output:

- AI book daily return.
- Benchmark daily return.
- AI excess return vs each benchmark.
- Hedged residual return.
- Rolling 20D/60D drawdown.
- Top/bottom contribution by AI layer.
- Whether the current strategy beat `SPY/QQQ/SMH` or CN indices on the same
  window.

## Report Contract

The final daily report should have this shape:

1. AI book state.
2. Production candidates.
3. Watch/research-only candidates.
4. Earnings/source-review calendar.
5. Benchmark attribution vs `SPY/QQQ/SMH` or CN indices.
6. Portfolio risk and hedge state.
7. Open data requirements.

The report should not include broad-market non-AI candidates except benchmarks,
hedges and macro context.

## Cleanup Plan

### Phase 0: Freeze Broad Market Behavior

- Rename broad-market screens as legacy/context.
- Keep broad price ingestion for benchmarks and liquidity.
- Prevent broad screens from feeding production rankers directly.

### Phase 1: Enforce AI Universe Everywhere

- US producer: apply `ai_infra` universe before candidate ranking.
- CN producer: apply `ai_infra` universe and taxonomy before ranker output.
- Factor Lab: keep `FACTOR_LAB_AI_INFRA_ONLY=1` as default.
- Main Strategy V2: assert production rows are AI-universe or benchmark/hedge.

### Phase 2: Benchmark Attribution

- Add a dedicated benchmark attribution artifact under
  `reports/review_dashboard/main_strategy_v2/<date>/`.
- Persist benchmark comparison rows into the report DuckDB.
- Render benchmark attribution in US/CN standalone daily reports.

### Phase 3: Portfolio Allocator

- Make final R depend on sleeve score, evidence quality, tape quality,
  options/flow confirmation and benchmark beta hedge.
- Add layer caps and correlation-cluster caps.
- Report why each name got its final R.

### Phase 4: Source-Review Automation

- Automate filings/transcript/source reading into evidence cards.
- Promote only `source_confirmed` rows into the relationship ledger.
- Keep all unresolved new names in source-review expansion queues.

### Phase 5: Publish Clean QuantStack Branch

Do not push the current messy worktree directly to `main`.

Recommended flow:

1. Create a branch such as `ai-infra-specialist-pipeline`.
2. Commit in logical slices:
   - AI-infra imports and source-review queues.
   - Universe/ranker enforcement.
   - benchmark attribution.
   - report rendering.
   - ops/task registry.
   - docs/tests.
3. Run readiness and market-report tests.
4. Generate an `ops/review_packet.sh` packet.
5. Push the branch to `origin` and open a PR.

## Acceptance Checklist

- `scripts/verify_ai_supercycle_readiness.py --strict` has zero failures.
- US/CN standalone reports contain no non-AI production stock candidates.
- Benchmark attribution exists for `SPY`, `QQQ`, `SMH` and CN indices.
- Every production row has AI layer, sleeve id, evidence state and risk plan.
- Every new ticker discovered outside the universe lands in source-review
  expansion, not production ranker.
- Factor Lab exports research candidates separately from execution candidates.
- Review packet clearly shows changed code, reports and test commands.
