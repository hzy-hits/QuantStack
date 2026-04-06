# quant-research-cn — Agent Code Map

> This file is the **primary navigation guide** for any agent (Claude, Codex, or human) working on this codebase.
> It maps every module to its purpose, axiom, data dependencies, and implementation status.

## Philosophy (NON-NEGOTIABLE)

- Program computes probabilities → filters → raw Markdown payload
- Agent reads payload → writes narrative → human reads report
- **NO** trading signals. **NO** buy/sell/hold. **NO** execution.
- Agents **NEVER** touch arithmetic. Every number computed upstream in Rust.
- DeepSeek does **EXTRACTION ONLY** — never reasoning or prediction.
- See `spec.md` for the full binding specification.

## File Map

```
quant-research-cn/
├── spec.md                    # BINDING design specification (read first)
├── CLAUDE.md                  # THIS FILE — agent navigation guide
├── Cargo.toml                 # Dependencies: tokio, reqwest, duckdb, statrs, nalgebra, polars
├── config.example.yaml        # Template config (copy → config.yaml, fill API keys)
├── config.yaml                # GITIGNORED — live config with API keys
│
├── docs/
│   ├── AXIOMS.md              # 5 mathematical axioms with A-share instantiations
│   ├── DESIGN.md              # Architecture diagram, pipeline phases, DuckDB schema
│   └── DATA_SOURCES.md        # Tushare/AKShare API reference, code formats, rate limits
│
├── src/
│   ├── main.rs                # CLI entry: Init | Run | Fetch | Analyze | Enrich | Render
│   ├── config.rs              # YAML config loading, date resolution (Shanghai TZ)
│   │
│   ├── storage/
│   │   ├── mod.rs             # open(path) → DuckDB Connection, init_schema()
│   │   └── schema.rs          # 15 CREATE TABLE statements (see spec.md §5.2)
│   │
│   ├── fetcher/
│   │   ├── mod.rs             # Module exports
│   │   ├── http.rs            # Shared retry logic: 3 retries, exp backoff, 30s timeout
│   │   ├── tushare.rs         # [COMPLETE] 9 Tushare endpoints, 500ms rate limit
│   │   └── akshare.rs         # [PARTIAL] HTTP bridge client, only northbound done
│   │
│   ├── enrichment/
│   │   ├── mod.rs             # Module exports
│   │   ├── llm.rs             # [COMPLETE] DeepSeek client, Semaphore concurrency
│   │   └── news.rs            # [COMPLETE] Async concurrent news extraction
│   │
│   ├── analytics/
│   │   ├── mod.rs             # run_all() orchestrator — runs 8 compute modules; rv supports vol_hmm
│   │   ├── bayes.rs           # [COMPLETE] Beta-Binomial updater + unit tests (Axiom 2)
│   │   ├── momentum.rs        # [COMPLETE] CPT 9-cell + Beta-Binomial (502 lines, Axiom 1)
│   │   ├── announcement.rs    # [COMPLETE] 业绩预告 surprise categories (273 lines, Axiom 2)
│   │   ├── flow.rs            # [COMPLETE] 9-component information score + iVIX model-free variance (1192 lines, Axiom 4)
│   │   ├── hmm.rs             # [COMPLETE] 2-state Gaussian HMM, Baum-Welch + Viterbi, Brier calibration (512 lines, Axiom 3)
│   │   ├── rv.rs              # [COMPLETE] Realized volatility: Parkinson, Garman-Klass, Yang-Zhang (167 lines)
│   │   ├── vol_hmm.rs         # [COMPLETE] 2-state Gaussian HMM on log-variance, vol regime detection (417 lines, Axiom 3)
│   │   ├── sector_rotation.rs # [COMPLETE] Industry-level momentum + flow rotation (162 lines)
│   │   ├── unlock.rs          # [COMPLETE] 限售解禁 risk buckets (305 lines, Axiom 2)
│   │   └── macro_gate.rs      # [COMPLETE] 3×3 vol×yield gate matrix (381 lines, Axiom 4)
│   │
│   ├── filtering/
│   │   ├── mod.rs             # Module exports
│   │   └── notable.rs         # [COMPLETE] Two-pass filter: universe → 120 → 30 (461 lines, Axiom 5)
│   │
│   └── reporting/
│       ├── mod.rs             # Module exports
│       └── render.rs          # [COMPLETE] Markdown payload renderer (2003 lines)
│
├── .claude/
│   ├── workflows/             # Multi-step agent workflows (see below)
│   └── skills/                # Reusable agent skills (see below)
│
├── data/                      # DuckDB files (gitignored)
├── reports/                   # Generated payload files
└── logs/                      # Runtime logs
```

## Module → Axiom → Data Dependency Map

| Module | Axiom | Reads From (DuckDB) | Writes To | Status |
|--------|-------|-------------------|-----------|--------|
| `fetcher/tushare.rs` | — | (external API) | prices, daily_basic, forecast, margin_detail, block_trade, top_list, share_unlock, macro_cn, index_weight | COMPLETE |
| `fetcher/akshare.rs` | — | (HTTP bridge) | northbound_flow, news_items | PARTIAL |
| `enrichment/news.rs` | — | news_items OR forecast | news_enriched | COMPLETE |
| `analytics/momentum.rs` | Axiom 1 | prices | analytics (module=momentum) | COMPLETE |
| `analytics/announcement.rs` | Axiom 2 | forecast | analytics (module=announcement) | COMPLETE |
| `analytics/flow.rs` | Axiom 4 | northbound_flow, margin_detail, block_trade, top_list, daily_basic | analytics (module=flow) | COMPLETE |
| `analytics/hmm.rs` | Axiom 3 | prices (benchmark) | analytics (module=hmm), hmm_forecasts | COMPLETE |
| `analytics/rv.rs` | Axiom 3 | — (pure OHLC estimators) | in-memory variance/volatility series for `vol_hmm` | COMPLETE |
| `analytics/vol_hmm.rs` | Axiom 3 | prices (benchmark OHLC), `analytics/rv.rs` | analytics (module=vol_hmm) | COMPLETE |
| `analytics/sector_rotation.rs` | Axiom 4 | analytics, daily_basic | analytics (module=sector_rotation) | COMPLETE |
| `analytics/unlock.rs` | Axiom 2 | share_unlock, prices | analytics (module=unlock) | COMPLETE |
| `analytics/macro_gate.rs` | Axiom 4 | macro_cn, analytics | analytics (values multiplied by gate) | COMPLETE |
| `filtering/notable.rs` | Axiom 5 | analytics, daily_basic | (in-memory Vec) | COMPLETE |
| `reporting/render.rs` | — | analytics, notable items | reports/{date}_payload.md | COMPLETE |

## Signal Mapping (US Pipeline → A-share)

| US Module | A-share Module | What Changed |
|-----------|---------------|-------------|
| `options_score` (IV + P/C + skew) | `flow_score` (large_flow+northbound+margin+block+hot+insider+event_clock+market_vol) | Market microstructure is completely different |
| `earnings_risk` (surprise quintile) | `announcement_risk` (业绩预告类型) | Chinese pre-announcements are categorical, not continuous |
| `sentiment_ewma` (P/C ratio EWMA) | `leverage_ewma` (融资余额 EWMA) | Margin balance replaces options sentiment |
| `momentum_risk` | `momentum` | Identical — CPT + Beta-Binomial is market-agnostic |
| `hmm_regime` | `hmm` | Identical algo, benchmark: 沪深300 instead of SPY |
| (none) | `unlock_risk` | A-share unique: 限售解禁 |
| (none) | `hot_money` | A-share unique: 龙虎榜游资 |

Current `flow_score` weights: `large_flow` 0.30, `northbound` 0.18, `margin` 0.15, `block` 0.10, `hot` 0.08, `insider` 0.07, `event_clock` 0.07, `market_vol` 0.05. Total = 1.00; tape abnormality + iVIX are blended separately.

## Key Patterns for Agents

### When modifying analytics modules:
1. Read `spec.md` §3 for the axiom this module implements
2. Read `analytics/bayes.rs` — it's the shared Beta-Binomial engine
3. Every `compute()` function: reads from DuckDB → computes → writes to `analytics` table
4. Output format: `INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)`

### When modifying fetcher modules:
1. Tushare date format is `YYYYMMDD` — use `ts_date()` to convert to `YYYY-MM-DD`
2. All fetchers follow: HTTP request → parse JSON → INSERT INTO DuckDB → return row count
3. Rate limit: 500ms between Tushare calls (enforced in `tushare.rs`)

### When modifying enrichment:
1. DeepSeek = extraction only (spec.md §8). Never change the system prompt to ask for reasoning.
2. Concurrency controlled by `tokio::Semaphore` in `llm.rs`
3. Failed parses are logged and skipped — never retry with modified prompt

### DuckDB gotchas:
- `duckdb::Connection` is `!Send` — cannot cross `.await` boundaries in spawned tasks
- Use `INSERT OR REPLACE INTO` for idempotent upserts (DuckDB supports this)
- Date columns are DATE type — strings must be `YYYY-MM-DD` format

## Build & Run

```bash
cargo build --release
cp config.example.yaml config.yaml     # fill tushare_token + deepseek_key
./target/release/quant-cn init         # first time: create schema
./target/release/quant-cn run          # full pipeline
./target/release/quant-cn fetch        # data only
./target/release/quant-cn analyze      # analytics only
./target/release/quant-cn enrich       # DeepSeek enrichment only
./target/release/quant-cn render       # render payload only
```

## Code Style

- Rust 2021 edition, `rustfmt` standard
- Errors: `anyhow::Result` everywhere (application code)
- Async: `tokio` for HTTP I/O, sync for DuckDB + analytics
- Logging: `tracing` with structured fields (`info!`, `warn!`)
- Every probability output MUST include: horizon, conditioning set, sample size
