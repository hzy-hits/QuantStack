# quant-research-cn

A-share quantitative research pipeline — pure Rust.

Computes cross-sectional and market analytics from Chinese stock market data, filters by information density, and renders a structured Markdown payload for agent-driven analysis.

## What This Does

```
Tushare Pro + AKShare → DuckDB → Analytics (10 modules) → Filter → Payload → Agent → Report
```

- Fetches daily A-share data (prices, fundamentals, flow, announcements, macro)
- Computes 10 analytics modules spanning momentum, flow, regime, volatility, and Bayesian features
- Filters 300+ stocks down to 30 notable items
- Renders Markdown payload for Claude/agent to narrate in Chinese

**Not a trading system.** No signals, no execution, no portfolio management.

## Quick Start

```bash
# Build
cargo build --release

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml: fill in tushare_token (required), deepseek_key (optional)

# Initialize database
./target/release/quant-cn init

# Run full pipeline
./target/release/quant-cn run

# Or run individual phases
./target/release/quant-cn fetch --date 2026-03-12
./target/release/quant-cn enrich
./target/release/quant-cn analyze
./target/release/quant-cn render
```

## Daily Pipeline

```bash
# Full daily pipeline (cron target)
./scripts/daily_pipeline.sh          # auto-detect morning/evening
./scripts/daily_pipeline.sh morning  # morning slot
./scripts/daily_pipeline.sh evening  # evening slot
```

## Requirements

- **Rust 2021 edition** (1.70+)
- **Tushare Pro** account (200 RMB/year, 2000 credits) — required for data
- **DeepSeek API key** — optional, for news enrichment
- **AKShare Python bridge** — optional, for 北向资金/news supplementary data

## Data Sources

| Source | Type | Cost | Data |
|--------|------|------|------|
| Tushare Pro | Structured API | 200 RMB/year | Prices, PE/PB, 业绩预告, 融资融券, 大宗交易, 龙虎榜, 限售解禁 |
| AKShare | Web scraping (Python bridge) | Free | 北向资金, news, macro |
| CBOE-style | 300ETF Options | Free (via Tushare opt_daily) | iVIX model-free implied vol |
| DeepSeek | LLM extraction | Per-token | Structured news annotations |

## Project Structure

```
src/
├── main.rs              # CLI: init / run / fetch / analyze / enrich / render
├── config.rs            # YAML config + date resolution
├── storage/             # DuckDB schema (15 tables)
├── fetcher/             # Tushare (9 endpoints) + AKShare bridge
├── enrichment/          # DeepSeek async concurrent extraction
├── analytics/           # 10 modules: momentum, announcement, flow, hmm, vol_hmm, rv, unlock, macro_gate, sector_rotation, bayes
├── filtering/           # Two-pass: 300+ → 120 → 30 notable items
└── reporting/           # Markdown payload renderer
```

## Mathematical Foundation

10 analytics modules, adapted for A-share market structure:

1. **Momentum** — multi-horizon trend and breakout persistence
2. **Announcement** — event-conditioned response around disclosures and notices
3. **Flow** — financing, block trade, northbound, and leaderboard flow features
4. **HMM** — 2-state Gaussian HMM on 沪深300
5. **Volatility Regime** — 2-state HMM on log-variance
6. **Realized Volatility** — Parkinson, Garman-Klass, Yang-Zhang estimators
7. **Unlock** — conditional impact around share unlock schedules
8. **Macro Gate** — macro/liquidity filters that gate cross-sectional scoring
9. **Sector Rotation** — industry-level momentum + flow rotation
10. **Bayes** — Beta-Binomial and CPT updates; **iVIX** — VIX-style model-free variance from 300ETF options

See `docs/AXIOMS.md` for full mathematical specification.

## Configuration

See `config.example.yaml` for all options. Key sections:

- `api` — Tushare token, DeepSeek key
- `universe` — which indices to scan (CSI300, SSE50, etc.) + manual watchlist
- `signals` — momentum windows, ATR period, flow EWMA halflife
- `enrichment` — DeepSeek concurrency and model selection
- `reporting` — Claude model for narrative generation

## Related Projects

- US pipeline: `quant-research-v1` (Python + Rust fetcher)
- Same philosophy, different market microstructure
- Signal mapping: `spec.md` §4

## License

Private research tool. Not for redistribution.
