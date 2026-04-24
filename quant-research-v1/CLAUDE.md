# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Core Philosophy

**The program computes probabilities. The agent writes narrative. Never mix these.**

- Program collects data + runs math → filters notable items → structured Markdown payload
- Agent (you) reads payload → reasons + writes human narrative → human reads report
- NO strategy execution. NO buy/sell signals. NO auto-trading.
- Claude/agents NEVER touch arithmetic. Every number is computed upstream and validated before the agent sees it.
- Config defines SCOPE (what asset classes to scan), NOT specific tickers. The program decides what's compelling.

**Primary output language is probability**, not price or return. Every output number should be expressible as `P(event | conditions)` with explicit horizon, conditioning set, sample size, and ideally confidence bounds.

## Setup

```bash
# First time
cp config.example.yaml config.yaml   # fill in API keys
pip install -e ".[dev]"

# Build Rust fetcher
cd rust && cargo build --release
```

Config is loaded from `config.yaml` (never committed). Keys needed: `finnhub_key`, `fred_key`, `anthropic_key`, `sec_user_agent`.

## Running the Pipeline

```bash
# First-time run (fetches 2yr history)
python scripts/run_daily.py --init

# Normal daily run
python scripts/run_daily.py

# Debug: skip Rust fetcher
python scripts/run_daily.py --skip-rust

# Backfill a specific date
python scripts/run_daily.py --date 2026-03-07
```

Output: `reports/{date}_payload_{session}.md` — feed to any agent:
```bash
claude < reports/2026-03-07_payload_post.md
```

## Tests and Lint

```bash
# Lint
ruff check src/ scripts/

# Tests
pytest
pytest tests/test_analytics.py          # single file
pytest tests/test_analytics.py::test_atr  # single test
pytest --cov=quant_bot                  # with coverage

# Rust
cd rust && cargo test
cd rust && cargo clippy
```

## Architecture

### Pipeline Flow (run_daily.py)

```
1. build_universe()          → resolve S&P 500 + Nasdaq 100 + broad screen + ETFs (~748 symbols)
2. Rust quant-fetcher all    → news (Finnhub), macro (FRED), SEC 8-K, Polymarket, earnings
3. fetch_and_store_prices()  → yfinance OHLCV for all symbols
4. fetch_options_snapshot()  → CBOE CDN delayed quotes (5 concurrent workers, 403 blacklist)
5. run_momentum_risk()       → P(trend continues | regime, vol_bucket), ATR, z-score
   run_earnings_risk()       → P(upside drift | surprise quintile)
6. build_notable_items()     → composite score → top 10-20
7. build_report_bundle()     → assemble all data
8. render_payload_md()       → reports/{date}_payload_{session}.md
```

### Rust / Python Split

**Rust** (`rust/`): rate-limited HTTP + DuckDB bulk writes
- `src/fetcher/finnhub.rs` — company news + earnings calendar (60 req/s, 20ms delay)
- `src/fetcher/fred.rs` — 7 FRED macro series (200ms delay)
- `src/fetcher/sec_edgar.rs` — 8-K filings via CIK lookup (333ms delay = 3 req/s)
- `src/fetcher/polymarket.rs` — crowd probabilities, Gamma API (no key needed), PK(market_id, fetch_date) for historical snapshots, sports/noise exclusion filter
- `src/storage/duckdb.rs` — table init + INSERT OR REPLACE

**Python** (`src/quant_bot/`): yfinance (Python-only) + analytics + filtering + rendering
- `universe/builder.py` — Wikipedia scraping, 7-day DuckDB cache (`universe_constituents`)
- `data_ingestion/prices.py` — yfinance OHLCV
- `data_ingestion/options.py` — CBOE CDN delayed quotes (5 workers, 403 blacklist), ATM IV, expected move, P/C ratio, skew, unusual activity
- `analytics/momentum_risk.py` — ATR, lag-1 autocorr regime, trend hit rate, Bonferroni z-score
- `analytics/earnings_risk.py` — post-earnings drift by surprise quintile
- `filtering/notable.py` — composite score, top 10-20 items
- `reporting/bundle.py` — assemble payload data
- `reporting/render.py` — write Markdown payload file

### Storage

See [docs/PIPELINE_STORAGE.md](docs/PIPELINE_STORAGE.md) for the current `raw / research / report / dev`
snapshot model and session-aware artifact naming.

Execution timing and post-gap handling are documented in
[docs/EXECUTION_ALPHA.md](docs/EXECUTION_ALPHA.md).

Canonical latest DB remains `data/quant.duckdb`, but daily report work now stages session-specific
research/report snapshots.

Key tables:
- `prices_daily` — OHLCV + adj_close, PK (symbol, date)
- `analysis_daily` — probability outputs, PK (symbol, date, module_name). `module_name` is `'momentum_risk'` or `'earnings_risk'`
- `earnings_calendar` — EPS history + surprise %, PK (symbol, report_date)
- `macro_daily` — FRED series, PK (date, series_id). Note: column is `series_id`, NOT `series_name`
- `forecast_outcomes` — Brier scoring table for calibration tracking
- `run_log` — pipeline governance log

### Notability Score Weights

```python
W_MAGNITUDE  = 0.30  # today's move vs ATR
W_EVENT      = 0.25  # earnings ±7d, 8-K recency
W_MOMENTUM   = 0.20  # |z_score| magnitude
W_OPTIONS    = 0.15  # implied vol vs historical vol
W_CROSS      = 0.10  # idiosyncratic vs SPY
```

### Universe (dynamic, ~748 symbols)

Determined at runtime by config scope flags — never hardcode symbols:
- S&P 500 constituents: Wikipedia, cached 7 days
- Sector ETFs (11): XLK XLF XLE XLV XLI XLU XLRE XLY XLP XLB XLC
- Bond ETFs (5): TLT IEF SHY HYG LQD
- Commodities (5): GLD SLV GC=F CL=F NG=F
- International (4): EEM EFA FXI VWO
- Volatility (2): ^VIX UVXY

## Agent Philosophy

See **[docs/Agents.md](docs/Agents.md)** for the full agent philosophy, how to read the payload, and the recommended system prompt to prepend when feeding the payload to any LLM.

The short version: the program computes, the agent narrates. The agent reads `reports/{date}_payload_{session}.md` and explains the facts in plain English. It never invents numbers, never fetches data, and never makes buy/sell decisions.

## Mandatory Market Context (Tier 1)

Every payload includes these fixed symbols regardless of the notability filter — the agent needs this backdrop to contextualize any individual item. Never remove these from `bundle.py`:

- **Indices**: SPY, QQQ, IWM, DIA — performance + YTD
- **Fear/greed**: `^VIX` from `prices_daily` (NOT FRED — FRED has 1-day lag), put/call ratio, % above 200MA/50MA
- **Sectors**: all 11 ETFs (XLK through XLC) — always present, never filtered
- **Rates**: 10Y yield, 10Y-2Y spread, HY credit spread from `macro_daily` (column: `series_id`), TLT, HYG from prices
- **Commodities**: GLD, CL=F
- **Polymarket**: top 10 crowd probability markets, 2-day freshness filter, Δ tracking, `end_date` + `fetched_at` columns

## Known Bugs (as of 2026-03-07)

(updated 2026-03-13 — 2 bugs fixed this session)

Confirmed by 6-task codex review (`review-v2-20260307-1244/`). Not yet fixed.

### Crashes / Wrong Numbers (fix first)

1. **`expected_move_pct`** (`momentum_risk.py:184`): `atr_val / 1.0 * 100` → should be `atr_val / cl[-1] * 100`
2. **`binom_test` removed** (`earnings_risk.py:177`): `stats.binom_test` → `stats.binomtest(k, n, 0.5, alternative="two-sided").pvalue`
3. **`series_name` column missing** (`bundle.py:34`): `macro_daily` has `series_id`, not `series_name` — breaks fresh DBs
4. **`build_notable_items` ignores `symbols` arg** (`notable.py:66`): scans entire DB — stale/out-of-scope symbols leak in
5. **Options subquery mis-correlated** (`notable.py:170`): "nearest expiry" selects global min `days_to_exp`, not per-symbol min
6. **SEC 8-K dedup key** (`rust/storage/duckdb.rs:28`): PK is `(symbol, form_type, filed_date)` — multiple same-day 8-Ks overwrite each other; need `accession_number` in key
7. **SEC `filing_url` wrong** (`rust/fetcher/sec_edgar.rs:134`): URL constructed incorrectly, doesn't resolve on sec.gov

### Probability Architecture (core redesign)

9. **`trend_prob` ignores `regime`** (`momentum_risk.py:55`): conditions only on prior return sign, not the computed regime. Replace with CPT lookup keyed by `(regime, vol_bucket)`
10. **`earnings_risk` never reaches payload** (`notable.py:106`): `build_notable_items` only loads `momentum_risk`; earnings probabilities computed but not delivered
11. **Earnings hard-codes `regime = "event_driven"`** (`earnings_risk.py:226`): compute actual pre-event regime from price data
12. **No Beta-Binomial updater exists**: create `analytics/bayes.py` — spec in `QUANT_BOT_DESIGN.md` Part V and Part XVI
13. **Earnings silently substitutes q=3 when surprise unknown** (`earnings_risk.py:167`): use L1/L2 posterior instead + flag `surprise_unknown: true`
14. **`regime.confidence` is prevalence not probability** (`bundle.py:88`): rename to `regime_prevalence_pct`
15. **`earnings_risk` stock/benchmark alignment** (`earnings_risk.py:41`, `155`): `event_idx` used for both arrays without alignment enforcement

### Data Quality

16. **S&P refresh is non-transactional** (`builder.py:145`): DELETE before validate — Wikipedia failure leaves empty cache; wrap in transaction
18. **Config controls not implemented**: `min_price`, `min_avg_volume_shares`, `max_notable_items`, `russell2000` in config but not applied in code
19. **Universe summary too large** (`render.py:248`): 500 symbols × 11 tokens ≈ 5.5k tokens; compress to top/bottom 10 + breadth counts

## Probability Design Constraints

When adding or modifying analytics:
- Every exported probability must be `P(event | conditions)` — event definition, horizon, conditioning set, and `n_obs` must all be stored or inferable from `details` JSON
- `p_upside`/`p_downside` mean different things in `momentum_risk` vs `earnings_risk` — never conflate them in the render layer; label which module they came from
- `regime` must be used as a conditioning variable in `trend_prob`, not ignored after being computed
- `magnitude_score` should be a tail probability `P(|move| >= today | symbol, regime)`, not a scaled ATR multiple
- Bonferroni `n_tests` must be consistent across both modules — either both use actual tests run, or both use `universe_size × n_modules`
- Cross-sectional z-scores are descriptive, not inferential — never convert them to p-values via `norm.sf`
- `regime_prevalence_pct` (formerly `regime.confidence`) is fraction of universe in dominant regime — not a posterior probability
