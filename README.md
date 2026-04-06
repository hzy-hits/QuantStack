# Quant Stack

Production-grade quantitative research platform covering **US equities** and **Chinese A-shares**, with an AI-driven alpha factor discovery lab. ~40k lines of Python + Rust, running daily in production.

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

## Components

### Factor Lab — AI-Driven Alpha Factor Discovery

An automated factor mining platform where LLM agents iteratively propose, evaluate, and promote alpha factors through a strict anti-overfit pipeline.

**Core Design:**

- **Factor DSL** — ~35 operators (time-series, cross-sectional, utility) for expressing alpha factors. Example: `rank(delta(ts_mean(close, 5), 10))`
- **Walk-Forward Backtest** — Expanding-window validation with separate IS/OOS periods. Agent only sees IS metrics; OOS results are binary PASS/FAIL to prevent reverse-engineering
- **5-Gate Anti-Overfit System** — IC > 0.02, IC_IR > 0.3, turnover < 40%, monotonicity > 0.8, correlation < 0.7 with existing factors
- **Agent Loop** — Claude/Codex-driven iterative discovery with budget cap (50 experiments/session)
- **Factor Registry** — DuckDB-backed repository with daily metrics, auto-retirement (rolling 60-day IC < 0.01), and promotion tracking

**Metrics:** IC, IC_IR, quintile spread, portfolio turnover, Sharpe ratio of long-short basket.

| | |
|---|---|
| Language | Python (~9.5k LOC) |
| Storage | DuckDB |
| GPU | Optional — cuDF/cuML/cuPy for acceleration (800-symbol backtest: 2s GPU vs 3min CPU) |

---

### Quant-Research-V1 — US Equities Pipeline

Automated US equities research pipeline delivering analyst-grade probability reports twice daily.

**Pipeline (2x daily):**

```
8 APIs → Rust fetcher (async) → DuckDB → 15 Analytics Modules → 2-Pass Filter → Agent Synthesis → Email
```

**Data Sources:** Finnhub (news/quotes), FRED (macro), SEC Edgar (8-K filings), Polymarket (crowd probabilities), yfinance (OHLCV), CBOE (options)

**15 Analytics Modules:**

| Module | Method |
|---|---|
| Momentum Risk | P(5D ret > 0 \| regime, vol_bucket) via 9-cell CPT + Beta-Binomial |
| Earnings Risk | P(5D excess > 0 \| surprise quintile) with Beta(2,2) prior |
| HMM Regime | 2-state Gaussian HMM on SPY with Brier calibration |
| Options | IV ratio, P/C skew, probability cones, unusual activity |
| Variance Premium | IV² - RV² |
| Macro Gate | 3x3 VIX × yield curve matrix |
| + 9 more | Kalman beta, Granger causality, sector rotation, pairs, ... |

**Filtering:** 748 symbols → 120 candidates → 30 notable items (HIGH / MOD / WATCH / LOW)

**Agent Synthesis:** 4 specialist analysts (Claude API) read structured payload in parallel, merge agent synthesizes final report.

| | |
|---|---|
| Language | Python + Rust fetcher (~18k LOC) |
| Universe | ~748 symbols (S&P 500 + ETFs + watchlists) |
| Schedule | 2x daily (pre-market + post-market) |

---

### Quant-Research-CN — A-Share Pipeline

Chinese A-share quantitative research pipeline, fully implemented in Rust.

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

## Repository Structure

This is the umbrella repository for the Quant Stack platform. The three sub-projects are maintained in separate private repositories.

## License

MIT
