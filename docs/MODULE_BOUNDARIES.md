# Quant Stack Module Boundaries

This document is the operating map for the combined US, A-share, Factor Lab, and
shared alpha stack. The intended boundary is:

```text
market producer -> review ledger -> shared alpha gate -> report model -> narrative/delivery
```

The producer computes facts. Quant Stack decides whether a setup is mature enough
to appear as execution alpha. The report explains the state and must not promote
research-only evidence into a trading ticket.

## Shared Control Plane

### `crates/quant-stack-core`

Owner of reusable alpha maturity logic.

Responsibilities:

- Load evaluated trades from `algorithm_postmortem` or
  `report_decisions`/`report_outcomes`.
- Evaluate policy-level stability without future leakage.
- Select a champion policy only when fills, active buckets, average return,
  median return, win rate, drawdown, and concentration gates pass.
- Build the alpha bulletin sections used by both markets:
  `execution_alpha`, `recall_alpha`, `options_alpha`, and `blocked_alpha`.
- Persist the report model into `daily_report_model`.

Important files:

- `src/alpha/source.rs`: market DB adapters and options/shadow-options candidate
  loaders.
- `src/alpha/policy.rs`: policy thresholds, stability score, champion selection,
  and hysteresis.
- `src/alpha/bulletin.rs`: converts evaluated trades and option candidates into
  report sections.
- `src/alpha/storage.rs`: writes maturity metrics, selected trades, bulletin
  rows, execution gate rows, and report model JSON.
- `src/report_model.rs`: renders a market/session-specific JSON contract for
  downstream reports.

Non-responsibilities:

- No broker integration.
- No account sizing.
- No market-specific feature engineering beyond reading market-produced ledger
  tables.

### `crates/quant-stack-cli`

Owner of daily orchestration and compatibility entry points.

Responsibilities:

- `alpha evaluate`: run shared maturity evaluation.
- `daily`: coordinate cross-market producer, alpha gate, report model, narrative,
  delivery, and review maintenance.
- `us-daily`: Rust state machine for the US daily pipeline and the preferred
  replacement for `quant-research-v1/scripts/run_full.sh`.

Design rule: shell scripts may remain as compatibility wrappers, but the cron
source of truth should move toward the Rust state machine.

## US Producer: `quant-research-v1`

Purpose: compute US facts and write a session-specific report snapshot.

### Ingestion and Universe

Files:

- `data_ingestion/*.py`
- `screens/*.py`
- `universe/builder.py`

Responsibilities:

- Maintain broad US symbol coverage, price history, macro series, earnings,
  filings, options snapshots, dividends, and fundamentals.
- Build a research universe before expensive per-symbol analytics.
- Store snapshots in the raw/research/report DuckDB model described in
  `quant-research-v1/docs/PIPELINE_STORAGE.md`.

### Analytics

Files: `analytics/*.py`

Main module families:

- Price/risk: `momentum_risk.py`, `mean_reversion.py`, `breakout.py`,
  `covariance.py`, `pairs.py`, `granger.py`, `kalman_beta.py`,
  `event_study.py`.
- Options: `options_alpha.py`, `variance_premium.py`,
  `sentiment_ewma.py`.
- Execution timing: `overnight_gate.py`,
  `overnight_continuation_alpha.py`.
- Valuation and catalysts: `value_score.py`, `earnings_risk.py`,
  `news_quality.py`.
- Review: `algorithm_postmortem.py`, `report_review.py`, `scorecard.py`.
- Portfolio context: `clustering.py`, `portfolio_risk.py`,
  `risk_params.py`, `contradictions.py`.

Output contract:

- Analytics write structured rows into `analysis_daily`, `options_alpha`,
  `algorithm_postmortem`, and related tables.
- These rows are facts. They are not trade instructions until they pass the
  shared gate or are explicitly labeled research-only.

### Filtering and Reporting

Files:

- `filtering/notable.py` and loaders under `filtering/_*.py`.
- `signals/classify.py`.
- `reporting/*.py`.

Responsibilities:

- Rank and classify candidates from computed signals.
- Inject execution gate state into candidate records.
- Render payload and final Markdown.
- Keep Factor Lab candidates visibly separated as research priors.

Important guardrail: `overnight_gate` can downgrade a stale move to
`wait_pullback` or `do_not_chase`; the report must not treat the prior close as
the only entry reference.

## A-share Producer: `quant-research-cn`

Purpose: compute A-share facts in Rust and write a report snapshot.

### Fetching and Storage

Files:

- `fetcher/tushare/*`
- `fetcher/akshare.rs`
- `bridge/akshare_bridge.py`
- `storage/*.rs`

Responsibilities:

- Fetch Tushare/AKShare market, fundamental, flow, macro, event, and option-like
  reference data.
- Store the raw/research/report snapshot model described in
  `quant-research-cn/docs/PIPELINE_STORAGE.md`.
- Keep rendering read-only; analytics such as shortlist shadow pricing must be
  materialized before render.

### Analytics

Files: `analytics/*.rs`

Main module families:

- Market state: `momentum.rs`, `hmm.rs`, `vol_hmm.rs`, `macro_gate.rs`,
  `price_features.rs`.
- Events and flow: `announcement.rs`, `flow.rs`, `flow_audit.rs`,
  `unlock.rs`, `headline_gate.rs`.
- Setup/execution: `setup_alpha.rs`, `continuation_vs_fade.rs`,
  `open_execution_gate.rs`.
- Risk/convexity: `shadow_option.rs`,
  `shadow_option_alpha_calibration.rs`, `rv.rs`.
- Tactical/radar: `breakout.rs`, `mean_reversion.rs`,
  `sector_rotation.rs`, `limit_move_radar.rs`, `limit_up_model.rs`.
- Review: `paper_trade_ev.rs`, `algorithm_postmortem.rs`,
  `report_review.rs`.

Output contract:

- Analytics write into `analytics`, `paper_trades`, `report_decisions`,
  `report_outcomes`, and `algorithm_postmortem`.
- `shadow_option` is a risk and convexity model for A-shares, not a claim that
  single-name options are tradeable.
- `open_execution_gate` is a next-open threshold model until auction/minute-bar
  data is added.

### Filtering and Reporting

Files:

- `filtering/notable.rs`
- `reporting/render.rs`

Responsibilities:

- Build notables from already materialized analytics.
- Render market state, action map, EV audit, candidate tables, Factor Lab
  appendix, and risk scenarios.
- Preserve T+1 constraints and call out when a line is a review condition rather
  than a same-day executable stop.

## Factor Lab

Purpose: discover and maintain research factors without letting them override
the main execution gate.

Module families:

- DSL: `dsl/parser.py`, `dsl/operators.py`, `dsl/compute.py`.
- Evaluation: `evaluate/*.py`, `backtest/*.py`.
- Mining: `mining/daily_pipeline.py`, `mining/batch_mine.py`,
  `mining/export_to_pipeline.py`.
- Strategy board: `strategy/rolling_best.py`.
- Autoresearch: `autoresearch/*.py`, `agent/*.py`.
- Paper tracking: `paper/tracker.py`.

Promotion rule:

```text
research-only -> exported signal -> report appendix -> shared gate/review evidence -> possible execution sleeve
```

Factor Lab may rank opportunities and provide priors, but the report must keep
them separate unless the relevant market pipeline and shared alpha gate have
promoted the evidence.

## Options and Shadow Options Boundary

US:

- `options_alpha` may produce real listed-options candidates when liquidity and
  expression constraints pass.
- The current system remains a research system. It should not compute live
  account sizing or pretend to manage an options book.

A-shares:

- `shadow_option` and `shadow_option_alpha` estimate convexity, downside room,
  chase risk, and entry quality.
- They are risk discounts and timing context for stock candidates, not tradeable
  options instructions.

Shared report rule:

- Options/shadow-options evidence belongs in `options_alpha` or risk sections
  unless the equity execution gate also supports a real stock ticket.

