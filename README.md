# Quant Stack

Production-grade quantitative research platform covering **US equities** and **Chinese A-shares**, with an AI-driven alpha factor discovery lab. Its core rule is simple: **the program computes, the agent narrates**. Every output is an explicit `P(event | conditions)` with horizon, conditioning set, and sample size. No black-box scores. No free-form agent arithmetic. ~40k lines of Python + Rust, running daily in production. The stack now includes shadow-running option alpha diagnostics for both markets and a Factor Lab feedback loop for postmortem-driven factor weighting.

## Production Snapshot

- **US market**: ~748 symbols, twice daily reports (pre-market + post-market)
- **CN market**: 300+ A-shares from CSI300/SSE50, daily reports
- **Data layer**: 8 external APIs, async Rust fetchers, DuckDB-backed audit trail
- **Analysis layer**: 15 US modules + 10 CN modules under one probability-first framework, including shadow option alpha diagnostics
- **Agent layer**: LLMs narrate structured payloads but never modify the numbers

## Why This Exists

In quant systems, polished output is cheap and statistical honesty is expensive.

The hard part is not generating a report. The hard part is keeping the whole stack auditable while it runs every day across heterogeneous APIs, market deadlines, and LLM involvement. Quant Stack exists to make that constraint explicit:

- programs compute the numbers
- models and assumptions stay legible
- agents narrate facts instead of improvising arithmetic

## Thesis

- **Probability first**: reports are built from conditional probabilities, not opaque "signals"
- **Agents are narrators, not calculators**: LLMs read structured payloads and write reports, but never change the numbers
- **Anti-overfit by design**: holdouts stay hidden from agents, sessions are budget-capped, and promotion requires hard gates

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Quant Stack                                    │
│                                                                         │
│  ┌──────────────┐   ┌───────────────────┐   ┌───────────────────────┐  │
│  │  Factor Lab   │   │ Quant-Research-V1 │   │  Quant-Research-CN    │  │
│  │  (Python)     │   │ (Python + Rust)   │   │  (Rust)               │  │
│  │              │   │                   │   │                       │  │
│  │  DSL Engine  │   │  US Equities      │   │  A-Share Pipeline     │  │
│  │  Walk-Fwd    │──▶│  Pipeline         │   │                       │  │
│  │  Backtest    │   │                   │   │  Tushare Pro          │  │
│  │  Agent Loop  │──▶│  Finnhub / FRED   │   │  AKShare              │  │
│  │              │   │  SEC Edgar        │   │  DeepSeek LLM         │  │
│  │  factor_lab  │   │  Polymarket       │   │                       │  │
│  │  .duckdb     │   │  quant.duckdb     │   │  quant_cn.duckdb      │  │
│  └──────┬───────┘   └────────┬──────────┘   └──────────┬────────────┘  │
│         │                    │                          │               │
│         │    lab_composite   │     4 Parallel Agents    │               │
│         └───────────────────▶│     (Claude API)         │               │
│                              ▼                          ▼               │
│                    ┌──────────────────────────────────────┐             │
│                    │     Daily Reports (Email / MD)       │             │
│                    │     30 Notable Items × 2 Markets     │             │
│                    └──────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────┘
```

## System Components

### Factor Lab — AI-Driven Alpha Factor Discovery

Factor discovery is where LLM curiosity is most useful and statistical discipline is most necessary. Factor Lab lets agents generate hypotheses quickly, but only inside a search space narrow enough to audit and a validation stack strict enough to reject most ideas.

**Core Design:**

- **Factor DSL** — agents propose formulas, not code. ~35 operators, bounded depth, and whitelisted windows keep search expressive enough for ideas but narrow enough to audit
- **Walk-Forward Backtest** — validation is expanding-window, not full-sample. Agents see in-sample metrics, but out-of-sample results collapse to PASS/FAIL so the holdout cannot be gamed
- **5-Gate Anti-Overfit System** — predictive power is necessary but not sufficient; promotion also requires stability, tradeability, monotonicity, and low redundancy
- **Agent Loop** — Claude/Codex can iterate quickly, but a hard 50-experiment budget prevents brute-force multiple testing
- **Factor Registry** — promoted factors are tracked like production assets, with daily metrics, promotion history, and auto-retirement on decay
- **Feedback Loop** — Candidate postmortem details capture captured/missed/stale overlaps and feed shrunken multiplier selection instead of producing a second trade list

**Metrics:** IC, IC_IR, quintile spread, portfolio turnover, Sharpe ratio of long-short basket.

| | |
|---|---|
| Language | Python (~9.5k LOC) |
| Storage | DuckDB |
| GPU | Optional — cuDF/cuML/cuPy for acceleration (800-symbol backtest: 2s GPU vs 3min CPU) |

---

### Quant-Research-V1 — US Equities Pipeline

Automated US equities research pipeline delivering analyst-grade probability reports twice daily. The operating problem is not just "compute features"; it is to keep reports fresh and auditable despite heterogeneous APIs, market deadlines, and LLM involvement.

**Pipeline (2x daily):**

```
8 APIs → Rust fetcher (async) → DuckDB → 15 Analytics Modules → 2-Pass Filter → Agent Synthesis → Email
```

**Data Sources:** Finnhub (news/quotes), FRED (macro), SEC Edgar (8-K filings), Polymarket (crowd probabilities), yfinance (OHLCV), CBOE (options)

**Core Analytics Modules:**

| Module | Method |
|---|---|
| Momentum Risk | P(5D ret > 0 \| regime, vol_bucket) via 9-cell CPT + Beta-Binomial |
| Earnings Risk | P(5D excess > 0 \| surprise quintile) with Beta(2,2) prior |
| HMM Regime | 2-state Gaussian HMM on SPY with Brier calibration |
| Options | IV ratio, P/C skew, probability cones, unusual activity |
| Overnight Gate | Open/gap execution gate with event and option context |
| Overnight Continuation Alpha | Shadow continuation/fade calibration with sample count, hit-rate interval, latest sample date |
| Report Review | Postmortem storage for report decisions and feedback labels |
| Variance Premium | IV² - RV² |
| Macro Gate | 3x3 VIX × yield curve matrix |
| + 9 more | Kalman beta, Granger causality, sector rotation, pairs, ... |

**Filtering:** 748 symbols → 120 candidates → 30 notable items (HIGH / MOD / WATCH / LOW)

**Agent Synthesis:** 4 specialist analysts (Claude API) read structured payloads in parallel, then a merge agent assembles the final report. The agents never compute numbers; they only narrate pre-computed facts.

| | |
|---|---|
| Language | Python + Rust fetcher (~18k LOC) |
| Universe | ~748 symbols (S&P 500 + ETFs + watchlists) |
| Schedule | 2x daily (pre-market + post-market) |

---

### Quant-Research-CN — A-Share Pipeline

Chinese A-share quantitative research pipeline, fully implemented in Rust. The goal was to own the full execution path in one language while handling A-share-specific microstructure, data vendor constraints, and daily production scheduling.

**Pipeline (daily):**

```
Tushare Pro + AKShare → DuckDB → 10 Analytics → 2-Pass Filter → Payload → Agent → Report
```

**A-Share Specific Modules:**

| Module | Description |
|---|---|
| Flow Score | Northbound + margin + block trades + hot money (龙虎榜) + turnover |
| Announcement | 业绩预告 type → surprise → Beta-Binomial posterior |
| Share Unlock | 限售解禁 risk buckets and timing windows |
| iVIX | Model-free implied variance from 300ETF options |
| Sector Rotation | Industry momentum + flow rotation with CSI300 weighting |
| Vol HMM | 2-state HMM on log-variance (Parkinson / Garman-Klass / Yang-Zhang) |
| Open Execution Gate | A-share open/gap chase gate for do-not-chase and entry-quality diagnostics |
| Continuation/Fade | Shadow postmortem labels for captured, missed, stale, and false-positive setups |
| Shadow Option Alpha Calibration | Shadow option calibration buckets for report-only stale-chase and entry-quality diagnostics |
| + 4 more | Momentum, HMM regime, macro gate, realized volatility |

**Flow Score Weights:** large_flow (30%), northbound (18%), margin (15%), block trades (10%), hot money (8%), insider (7%), event_clock (7%), market_vol (5%)

| | |
|---|---|
| Language | Rust (~12.5k LOC) |
| Runtime | tokio async, rate-limited with exponential backoff |
| LLM | DeepSeek for structured news/announcement extraction |

---

## Mathematical Foundation

All three components share a unified 5-axiom probability framework:

1. **Conditional Probability** — P(r>0 \| state) ≠ P(r>0). Regime classification removes non-stationarity
2. **Bayesian Updating** — Beta(2,2) prior + observed outcomes → posterior credible intervals
3. **Latent States (HMM)** — 2-state Gaussian model with Brier score calibration
4. **Multi-Source Fusion** — 5-dimensional composite: magnitude + event + momentum + options/flow + cross-asset
5. **Finite Attention** — Saturation clamp + two-pass filter → top 30 actionable items

Every output is **P(event | conditions)** with explicit horizon, conditioning set, and sample size. No black-box scores.

## Tech Stack

| Layer | Technology |
|---|---|
| **Languages** | Python 3.11+, Rust 2021 edition |
| **Storage** | DuckDB (OLAP, 15-20 tables per pipeline) |
| **Analytics** | polars, scipy, hmmlearn, statsmodels, statrs, nalgebra |
| **Async I/O** | tokio, reqwest (Rust); aiohttp (Python) |
| **LLM** | Claude API (agent synthesis), DeepSeek (structured extraction), Codex (factor mining) |
| **GPU** | Optional cuDF/cuML/cuPy acceleration |
| **Delivery** | Gmail API (OAuth2, HTML + inline charts) |
| **Tooling** | uv (Python), cargo (Rust), cron scheduling |

## Key Engineering Decisions

- **Probability-first**: No buy/sell signals — pure P(X \| Y) outputs with Bayesian credible intervals
- **Rust/Python split**: I/O-heavy fetching in Rust (async, single binary), analytics in Python (ecosystem)
- **Agent isolation**: LLM agents narrate pre-computed facts; they never modify numbers
- **Anti-overfit by design**: OOS holdout hidden from agent, hard experiment budget, 5-gate promotion
- **Idempotent pipelines**: Same date = same results. Full audit trail in DuckDB
- **Cross-market abstraction**: Same mathematical framework for US and China, only data sources differ

## Project Structure

```
quant-stack/
├── env.sh                    # Environment setup
├── smoke-check.sh            # Integration health check
├── package-stack.sh           # Migration packaging
├── factor-lab/               # Alpha factor discovery (Python)
│   ├── src/
│   │   ├── dsl/              # Factor expression DSL (parser, operators, compute)
│   │   ├── evaluate/         # IC, quintile, turnover, signal regression
│   │   ├── backtest/         # Walk-forward engine + 5-gate system
│   │   ├── agent/            # LLM-driven discovery loop
│   │   ├── mining/           # Batch mining + daily pipeline
│   │   └── paper/            # Paper trading tracker
│   └── scripts/              # Automation (autoresearch, daily, weekly)
├── quant-research-v1/        # US equities pipeline (Python + Rust)
│   ├── rust/src/             # Async data fetcher (Finnhub, FRED, SEC, Polymarket)
│   ├── src/quant_bot/
│   │   └── analytics/        # 15 analytics modules
│   └── scripts/              # Pipeline orchestration + email delivery
└── quant-research-cn/        # A-share pipeline (Rust)
    ├── src/
    │   ├── fetcher/          # Tushare Pro + AKShare bridges
    │   ├── analytics/        # 10 analytics modules
    │   └── enrichment/       # DeepSeek LLM integration
    └── scripts/              # Daily/weekly pipeline + email
```

## Production Status

This system runs daily in production, covering:
- **US market**: ~748 symbols, 2x daily reports (pre-market + post-market)
- **CN market**: 300+ A-shares from CSI300/SSE50, daily reports
- **Factor Lab**: Continuous autonomous research sessions, 30+ experiments per run
- **Shadow Alpha Feedback**: US overnight continuation and CN shadow option calibration write diagnostics only during the observation window; they do not directly override primary ranking weights

## Repository Structure

This is the umbrella repository for the Quant Stack platform. The three sub-projects are maintained in separate private repositories, and this repository stores synchronized source snapshots for browsing, backup, and migration. Generated reports, runtime databases, local config, and other operational artifacts are excluded.

## License

MIT
