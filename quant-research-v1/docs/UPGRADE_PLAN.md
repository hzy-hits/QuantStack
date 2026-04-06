# Quant Research System Upgrade Plan (v2 — post-review)

## Current State

The system scans S&P 500 (~500 stocks) + fixed ETFs, computes momentum/options/news signals, dumps 170KB raw data to LLM agents who do all analysis + narrative writing.

**Core gaps:**
- Only sees 500 stocks (misses OKLO, RKLB, AAOI, etc.)
- No fundamental valuation ("is this cheap or expensive?")
- No lead-lag relationship detection ("who moves first?")
- LLM does arithmetic that algorithms should do
- Options history only 7 days (can't model mean-reversion)

## Target Architecture: Four-Layer Signal Fusion

```
Layer 1: Universe (WHO to look at)
  Finnhub all US ~5000 → broad_screen → 200 active
  + S&P 500 + watchlist + ETFs → ~750 symbols

Layer 2: Signal Generation (four independent signal sources)

  A. Price/Momentum Signal
     - HMM regime → P(bull), P(bear) as market-level overlay (NOT replacing per-symbol regime)
     - trend_prob → P(5D return > 0 | regime, vol_bucket) [existing]
     - Cointegration spread z-score → how far from equilibrium (T=250)
     - Granger causality → who leads whom, by how many days (T=250)
     - Event study CAR → how much information is digested
     - Output: direction + confidence + time_frame

  B. Options/Forward-Looking Signal
     - VRP = IV² - RV² → fear premium level (no historical standardization until 6mo accumulated)
     - P/C ratio EWMA z-score → sentiment deviation (replaces OU; more robust with short history)
     - IV skew EWMA z-score → tail risk pricing vs recent history
     - Unusual flow + flow novelty → smart money positioning [existing]
     - Implied density (SPY/QQQ only) → market-level tail risk from Breeden-Litzenberger
     - Output: direction + confidence + expected_move

  C. Fundamental/Valuation Signal
     - PE/PS/PB vs sector percentile → cheap or expensive (cross-sectional, no history needed)
     - FCF yield + ROE → quality score
     - Analyst revision momentum → expectations rising or falling (needs historical snapshots)
     - Output: valuation_score + quality_score

  D. Event/Catalyst Signal
     - Earnings proximity + surprise history
     - 8-K filing type + freshness scoring
     - News dedup + attribution + exponential decay
     - Output: catalyst_score + freshness + persistence

Layer 3: Signal Fusion (algorithm, NOT LLM)

  Consistency check:
    A↑ + B↑ + C=cheap + D=catalyst → HIGH conviction
    A↑ + B↓                        → contradiction, flag
    A↑ + C=expensive               → momentum chasing, risk flag
    A=none + C=cheap + D=catalyst  → value play, different strategy

  Ledoit-Wolf covariance → clustering → "N signals = M independent bets"
  Kalman dynamic beta → divergence quantification
  Risk params → all pre-computed (entry/stop/target/R:R/half_life)

  Output: ~35KB pre-computed insights

Layer 4: Narrative (the ONLY thing LLM does)

  Input: 35KB refined payload
  LLM judges:
    - How to interpret contradictions between signals
    - Conviction assessment given macro context
    - Synthesized narrative
  Output: research report
```

## Mathematical Methods

### Price Layer (T=250, 1-year price history)
- **Log returns**: r(t) = ln(P(t)/P(t-1)) for additivity
- **Covariance denoising**: Ledoit-Wolf shrinkage (NOT hard MP eigenvalue truncation — more stable at high N/T)
- **Cointegration**: Engle-Granger two-step (OLS → ADF on residuals), within-sector pairs only
  - T=250 gives adequate power; apply BH false discovery rate control (q=0.05) across all pairs
  - OU fit on cointegrated spread → θ = -ln(b), μ = a/(1-b), half_life = ln(2)/θ
  - Guard: only valid when 0 < b < 1; discard non-stationary fits
  - Stability check: re-test quarterly, drop pairs that lose significance
- **Granger causality**: F-test on restricted vs unrestricted VAR, BIC for lag selection (not AIC)
  - T=250 with lag=1-2 gives reasonable power for strong relationships
  - Must difference non-stationary series first (or use VECM for cointegrated pairs)
  - BH FDR control across all tested pairs
- **Kalman filter**: Dynamic beta estimation, state β(t) = β(t-1) + η, observation r_A(t) = β(t)·r_B(t) + ε
  - Fix R from observation residual variance, choose Q for 20-60 day effective smoothing
  - Pooled calibration across similar pairs (not per-pair free fitting with T=250)
- **Event study**: AR(t) = r(t) - β·r_market(t), CAR(0,T) = Σ AR(t)
  - Requires pre/post market timing from earnings (add to schema)
- **HMM regime**: 2-state Gaussian HMM on [SPY return, VIX level], Baum-Welch estimation
  - Market-level signal ONLY — runs parallel to existing per-symbol autocorrelation regime
  - NOT a drop-in replacement; provides P(bull_market) as additional context

### Options Layer (progressive — depends on accumulated history)
- **VRP**: Variance Risk Premium = IV² - 20D Realized Vol² (IV² - RV² is typically positive)
  - Use ATM IV as proxy initially; upgrade to model-free OTM strip variance later
  - RV: close-to-close initially; upgrade to Yang-Zhang when OHLC available
  - First 6 months: report raw VRP level only (no z-score — insufficient history to standardize)
  - After 6 months: z-score against rolling 6-month mean
- **EWMA z-score on P/C ratio** (replaces OU process — more robust with limited history)
  - z = (X - EWMA_μ) / EWMA_σ, with span=20 days
  - No parametric model assumptions; works from Day 1 of accumulation
  - Upgrade path: after 120+ days, optionally fit regime-switching AR for better tail handling
- **EWMA z-score on IV skew**: Same framework as P/C ratio
- **Implied density (SPY/QQQ ONLY)**: Breeden-Litzenberger q(K) = e^(rT)·∂²C/∂K²
  - Requires dense strike coverage (20+ liquid strikes) — only index products qualify
  - Fit smooth IV smile (cubic spline with no-arbitrage constraints) before differentiating
  - Output: tail probabilities P(move > ±X%) as market-level risk indicator
- **Flow scoring**: Existing flow_intensity + novelty (already implemented)

### Fundamental Layer (cross-sectional — no history needed for core metrics)
- **Relative valuation**: PE percentile within GICS sector (immediate from first snapshot)
- **Quality composite**: Weighted ROE + FCF yield + margin stability
- **Analyst revision momentum**: Δ(consensus EPS estimate) over 30/90 days
  - Requires storing historical snapshots (not just latest) — table needs `as_of` column
  - Not available until 30+ days of fundamental snapshots accumulated

### Fusion Layer
- **Ledoit-Wolf clustering**: Shrinkage-cleaned covariance → hierarchical clustering
- **Contradiction matrix**: Boolean logic on 4 source directions
- **Portfolio risk**: Herfindahl index on factor exposure, net directional tilt

## Implementation Phases

### Phase 1: Infrastructure Expansion + Payload Compression Design
**Goal**: See more stocks, know their fundamentals, start accumulating options history, design compact payload schema.

| Module | File | Lines | Description |
|--------|------|-------|-------------|
| US symbols fetch | `data_ingestion/symbols.py` | ~80 | Finnhub `/stock/symbol?exchange=US` → us_symbols table, weekly |
| Company profiling | `data_ingestion/fundamentals.py` | ~150 | Finnhub `stock/profile2` + `stock/metric` for post-screen universe (~750), weekly |
| Broad screen | `screens/broad_screen.py` | ~120 | yfinance bulk prices for 5000 → 5D/20D extremes + volume surge → top 200 |
| Value scoring | `analytics/value_score.py` | ~80 | PE/PS/PB vs sector percentile, quality composite |
| Options history | `data_ingestion/options.py` (modify) | ~30 | Expand fetch to all ~750 research symbols (not just daily candidates); 60D retention with pruning |
| Raw chain storage | `data_ingestion/options.py` (modify) | ~40 | Store strike-level data for SPY/QQQ (needed for B-L later) |
| Fundamental snapshots | `data_ingestion/fundamentals.py` | ~30 | Store with `as_of` date for revision momentum tracking |
| Pipeline wiring | `scripts/run_daily.py` (modify) | ~50 | New flow: symbol_master → screen → research_universe → existing pipeline |
| Payload v2 schema | `reporting/render_v2.py` | ~60 | Define compact payload contract BEFORE new signals add more raw data |

**New DB tables:**
- `us_symbols (symbol PK, name, type, exchange, fetched_at)`
- `company_profile (symbol, as_of, sector, industry, market_cap, pe_fwd, ps_ratio, pb_ratio, ev_ebitda, roe, fcf_yield, revenue_growth, analyst_target, analyst_count, recommendation, pe_vs_sector_pct, PK(symbol, as_of))`

**Key decisions (from Codex review):**
- Use Finnhub `stock/profile2` + `stock/metric` instead of yfinance `.info` (avoids 429 bans at scale)
- Fundamentals fetched for post-screen ~750 symbols only, NOT all 5000
- Pipeline flow changes: symbol_master → bulk price screen → research universe → detailed fetchers
- `company_profile` keyed by `(symbol, as_of)` not just `symbol` — enables revision momentum
- Options fetched for all ~750 research symbols daily (not just candidates) to avoid survivorship bias
- DATA_TIMEOUT in run_full.sh must increase (current pipeline already runs 21-24 min)

**Runtime**: Symbols fetch ~5s, bulk screen prices ~7min, fundamentals ~8min (weekly via Finnhub at 60req/min for 750), broad screen ~10s

### Phase 2: Price Signal Layer
**Goal**: Detect lead-lag relationships and mean-reverting pairs using T=250 price history.

| Module | File | Lines | Description |
|--------|------|-------|-------------|
| Log returns + covariance | `analytics/covariance.py` | ~100 | Log returns, Ledoit-Wolf shrinkage (not MP hard truncation) |
| Cointegration | `analytics/pairs.py` | ~200 | Engle-Granger within sector, OU fit, half-life, BH FDR control |
| Granger causality | `analytics/granger.py` | ~120 | Sector bellwethers → peers, F-test, BIC lag selection, BH FDR |
| Event study | `analytics/event_study.py` | ~120 | CAPM beta, AR, CAR post-earnings (needs pre/post market flag) |
| Kalman beta | `analytics/kalman_beta.py` | ~80 | Dynamic beta, fixed Q/R from pooled calibration |

**New DB tables:**
- `cointegrated_pairs (symbol_a, symbol_b, beta, adf_pvalue, ou_theta, ou_mu, half_life_days, spread_zscore, fdr_significant BOOL, computed_at)`
- `granger_pairs (leader, follower, lag_days, f_statistic, p_value, fdr_significant BOOL, sector, computed_at)`
- `earnings_car (symbol, event_date, pre_post_market VARCHAR, car_1d, car_3d, car_5d, car_10d, computed_at)`

**Key decisions (from Codex review):**
- T=250 (1 year), not T=60 — dramatically improves statistical power
- Ledoit-Wolf shrinkage instead of MP eigenvalue truncation (more robust at high N/T)
- BH FDR control on all pair tests (4950 pairs per sector → ~250 false positives without it)
- BIC for Granger lag selection (AIC overfits with T=250, lag=1-2 only)
- Series must be differenced before Granger (or VECM for cointegrated pairs)
- Cointegration pairs re-tested quarterly; drop unstable ones
- earnings_car needs pre/post market timing flag (currently missing from schema)
- Covariance computed in pandas/numpy (0.0004s), NOT DuckDB SQL (0.43s)

**Runtime**: Covariance ~1s, cointegration ~3min, Granger ~2min, event study ~30s

### Phase 3: Options Signal Layer Upgrade (progressive)
**Goal**: Extract forward-looking signals from options. Progressive rollout — no hard "wait 30 days" gate.

**Phase 3a — Immediate (Day 1):**
| Module | File | Lines | Description |
|--------|------|-------|-------------|
| VRP calculation | `analytics/variance_premium.py` | ~60 | IV² - 20D RV², raw level only (no z-score yet) |
| EWMA sentiment | `analytics/sentiment_ewma.py` | ~80 | EWMA z-score on P/C ratio + IV skew (replaces OU) |

**Phase 3b — After 30 days accumulation:**
| Module | File | Lines | Description |
|--------|------|-------|-------------|
| Implied density | `analytics/implied_density.py` | ~80 | B-L on SPY/QQQ only (requires raw chain from Phase 1) |

**Phase 3c — After 6 months accumulation:**
- VRP z-score standardization (rolling 6-month mean/std)
- Optional: regime-switching AR upgrade for P/C ratio (if EWMA proves insufficient)

**New DB tables:**
- `options_sentiment (symbol, as_of, pc_ratio_z, skew_z, vrp, computed_at)`

**Key decisions (from Codex review):**
- EWMA z-score replaces OU process (no parametric assumptions, works from Day 1)
- VRP sign: IV² - RV² is typically POSITIVE (premium for bearing vol risk)
- B-L restricted to SPY/QQQ (individual stocks lack strike density)
- B-L requires smooth IV smile fit before differentiation (cubic spline + no-arb constraints)
- No Kelly sizing — this is a research system, not an execution engine

**Runtime**: VRP ~5s, EWMA ~5s, implied density ~30s (2 symbols only)

### Phase 4: Algorithmic Pre-Processing Layer
**Goal**: Move computation from LLM to algorithm. Compress payload 170KB → 35KB.

| Module | File | Lines | Description |
|--------|------|-------|-------------|
| Covariance clustering | `analytics/clustering.py` | ~100 | Ledoit-Wolf covariance → hierarchical clustering |
| Risk params | `analytics/risk_params.py` | ~60 | Cone → entry/stop/target/R:R extraction |
| Contradictions | `analytics/contradictions.py` | ~80 | 4-source direction conflict detection |
| Scorecard | `analytics/scorecard.py` | ~100 | Parse prior HIGH signals, compare to actual prices |
| News quality | `analytics/news_quality.py` | ~80 | Freshness decay, Jaccard dedup, attribution check |
| Portfolio risk | `analytics/portfolio_risk.py` | ~60 | Net tilt, HHI concentration, natural hedges |
| Payload v2 render | `reporting/render_v2.py` | ~200 | Compact payload using schema from Phase 1 |

**Runtime**: All <10s (pure computation on existing data)

### Phase 5: Model Upgrades
**Goal**: Better regime detection, new agent prompts for judgment-only workflow.

| Module | File | Lines | Description |
|--------|------|-------|-------------|
| HMM regime | `analytics/hmm_regime.py` | ~100 | 2-state GaussianHMM, market-level P(bull) overlay |
| Agent prompts | `agents.yaml` + `run_agents.sh` | ~200 | Rewrite for 35KB payload, judgment-only |

**Key decisions (from Codex review):**
- HMM is a market-level overlay, NOT replacing per-symbol autocorrelation regime
- HMM latent states are unlabeled — need mapping layer to interpret as bull/bear
- hmmlearn needs compatibility testing with NumPy 2.4.2 before use

**Dependencies**: Phase 4 must complete first (agents need new payload format).

## Total Scope

| Phase | New Code | Modified | New Tables | New Data Sources | Can Parallelize |
|-------|----------|----------|------------|------------------|-----------------|
| 1 | ~590 lines | ~80 lines | 2 | Finnhub symbols + profile/metric | — |
| 2 | ~620 lines | ~30 lines | 3 | None (T=250 from existing prices) | After Phase 1 |
| 3a | ~140 lines | ~20 lines | 1 | None (immediate) | After Phase 1 |
| 3b | ~80 lines | ~10 lines | 0 | None (needs 30D options) | Day 30+ |
| 4 | ~680 lines | ~100 lines | 0 | None | After Phase 2 |
| 5 | ~300 lines | ~50 lines | 0 | None | After Phase 4 |
| **Total** | **~2410 lines** | **~290 lines** | **6 tables** | **1 source** | |

## Key Dependencies & Timing

```
Day 1────── Phase 1 start (infrastructure + payload schema design)
             Options history begins accumulating for ALL 750 symbols
             Phase 3a starts (VRP level + EWMA z-scores — works from Day 1)
Day 5────── Phase 1 done, Phase 2 starts
Day 10───── Phase 2 done, Phase 4 starts
Day 15───── Phase 4 done, Phase 5 starts
Day 30+──── Phase 3b starts (B-L on SPY/QQQ, needs 30D raw chain)
Day 180+─── Phase 3c (VRP z-score standardization with 6mo history)
```

Phase 3a (EWMA z-scores) has NO wait — works from Day 1 with even a few data points.
Phase 3b (B-L implied density) needs 30 days of raw chain for SPY/QQQ only.
Phase 3c (VRP standardization) is a gradual upgrade, not a hard gate.

## Payload Size Comparison

```
Current (170KB):
  Tier 1 raw macro tables ........................ 30KB
  Tier 2: 50 items × all raw fields ............. 130KB
  Options extremes + universe summary ............  10KB

Target (35KB):
  Pre-computed insights:
    Correlation clusters + independent bet count .. 1KB
    Risk parameter table (all HIGH signals) ....... 0.5KB
    Contradiction matrix .......................... 1KB
    Prior signal scorecard ........................ 0.5KB
    News attribution warnings ..................... 0.5KB
    Portfolio risk summary ........................ 0.5KB
    Scenario quantification table ................. 1KB
    Cointegration/Granger alerts .................. 1KB
    VRP + sentiment EWMA extremes ................. 0.5KB
  Context (retained but compressed):
    Macro snapshot ................................ 3KB
    HIGH items: key fields only ................... 8KB
    MODERATE items: one-line each ................. 3KB
    Catalyst list (deduped) ...................... 3KB
    Fundamental highlights (value plays) .......... 2KB
  Appendix:
    SPY/QQQ implied tail probabilities ............ 1KB
    Dynamic beta divergence table ................. 1KB
```

## Payload v2 Format: Signals + Confidence + Scorecard

### Design Principle

The payload gives the LLM **raw signals with uncertainty**, not black-box conclusions.
The LLM's job is judgment under uncertainty — it needs to see:
1. What each signal says (direction + magnitude)
2. How confident we are (CI width, FDR pass/fail, sample size)
3. Whether signals agree or contradict
4. How accurate the algorithm has been historically (scorecard)

### Per-Item Format (HIGH/MODERATE signals)

```markdown
### AAOI — Applied Optoelectronics [HIGH]

Sources: momentum↑ options↑ value=cheap catalyst=yes
Contradictions: NONE

| Signal | Value | Confidence | Detail |
|--------|-------|------------|--------|
| trend_prob | 0.62 | CI 0.48-0.76 (weak) | regime=trending, vol=mid |
| PE_pct | 15% | 43 sector peers | cheaper than 85% of sector |
| PS_pct | 8% | 43 sector peers | very cheap on revenue |
| quality | 0.45 | ROE=12% FCF_yield=3.2% | below median quality |
| VRP | +3.2 | raw level (no z yet) | elevated fear premium |
| pc_ratio_z | +1.8σ | EWMA span=20d | put-heavy sentiment |
| skew_z | +0.9σ | EWMA span=20d | mild tail bid |
| granger | led by LITE | lag=1d p=0.003 fdr=pass | information flows from LITE |
| cointegration | with COHR | half_life=8d z=+2.1 fdr=pass | spread stretched 2σ above |
| catalyst | DOE permit news | 2d ago, decay=0.67 | fresh, not yet priced in |

Risk params: entry=18.5 stop=16.2 target=22.0 R:R=2.1
Cluster: [AAOI, LITE, COHR] — counts as 1 independent bet (corr>0.7)
Dynamic beta vs XLK: 1.35 (Kalman), divergence=+0.15 from 60d mean
```

### Per-Item Format (MODERATE — compressed one-liner)

```markdown
| Symbol | Signal | trend_prob | PE_pct | VRP | Key flag |
|--------|--------|-----------|--------|-----|----------|
| PLTR | momentum↑ | 0.58 (0.44-0.72) | 92% (expensive) | +1.1 | momentum vs expensive → risk |
| IONQ | catalyst | 0.51 (0.38-0.64) | — (pre-rev) | N/A | IBM partnership news (1d) |
```

### Scorecard Section (algorithm track record)

```markdown
## Algorithm Scorecard (trailing 20 trading days)

### Signal Accuracy
| Signal Type | Calls | Correct | Accuracy | Notes |
|-------------|-------|---------|----------|-------|
| HIGH bullish | 12 | 8 | 67% | 5D forward return > 0 |
| HIGH bearish | 5 | 3 | 60% | 5D forward return < 0 |
| Cointegration z>2 | 8 | 6 | 75% | spread reverted within half_life |
| Granger lead-lag | 15 | 9 | 60% | follower moved in predicted direction |
| VRP elevated | 7 | 5 | 71% | IV declined toward RV within 10D |

### Recent Misses (learn from failures)
| Date | Symbol | Signal | Predicted | Actual | Why wrong |
|------|--------|--------|-----------|--------|-----------|
| 03-07 | RKLB | HIGH bull | trend_prob=0.68 | -4.2% 5D | earnings miss overrode momentum |
| 03-05 | SMR | coint z=2.3 | mean revert | spread widened +1.2σ | regime break (policy news) |

### Confidence Calibration
| Stated CI | Observed hit rate | Calibration |
|-----------|-------------------|-------------|
| >0.65 trend_prob | 71% (n=24) | slightly overconfident |
| >0.55 trend_prob | 58% (n=48) | well calibrated |
| FDR-pass pairs | 72% stable at 30d | reasonable |
```

### Why This Works

1. **LLM sees uncertainty** — wide CI = "don't trust this signal blindly"
2. **LLM sees contradictions** — "momentum↑ but expensive" → it can weigh the tradeoff
3. **LLM sees track record** — if algorithm is 60% on Granger, it calibrates trust accordingly
4. **LLM sees failures** — specific examples of what went wrong → learns patterns
5. **No black box** — every number has a source, confidence interval, and FDR status
6. **Self-correcting** — scorecard degrades visibly when algorithm underperforms → LLM naturally becomes more cautious

## Codex Review Log

This plan was reviewed by 3 parallel Codex agents (2026-03-10). Key changes from v1:
- **T=60 → T=250** for all price-based methods (cointegration, Granger, covariance)
- **Ledoit-Wolf shrinkage** replaces MP hard eigenvalue truncation
- **EWMA z-score** replaces OU process for P/C ratio and IV skew
- **B-L restricted to SPY/QQQ** (individual stocks lack strike density)
- **Finnhub profile2/metric** replaces yfinance .info (avoids 429 rate limit bans)
- **BH FDR control** added for all multiple-testing scenarios
- **HMM as overlay** not replacement for per-symbol regime
- **company_profile keyed by (symbol, as_of)** for revision tracking
- **Options fetched for all 750 symbols** daily (not just candidates) to avoid survivorship bias
- **Kelly sizing removed** — research system, not execution engine
- **Payload schema designed in Phase 1** (not after signals bloat the data)
- **Phase 3 split into 3a/3b/3c** — progressive rollout, no hard 30-day gate
