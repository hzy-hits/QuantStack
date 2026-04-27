# quant-research-cn

A-share research producer for Quant Stack, implemented in Rust. It owns China
market data ingestion, analytics, report filtering, payload rendering, agent
orchestration, and Gmail delivery. The root `quant-stack` CLI consumes its
review ledger for shared alpha maturity and daily bulletins.

`Execution Alpha` is a research execution candidate. It is not an order ticket.
A-share T+1, limit-up/limit-down, and liquidity constraints must still be
handled by the human execution layer.

## Pipeline

```text
Tushare + AKShare + DeepSeek
        |
        v
DuckDB -> analytics -> notable filter -> report_decisions/report_outcomes
        |
        v
Quant Stack alpha gate -> payload markdown -> agents -> Chinese report -> Gmail
        |
        v
post-email review-backfill -> algorithm_postmortem history
```

The producer remains A-share-specific. The post-producer alpha gate is shared
with the US system. Historical review maintenance is deliberately outside the
email critical path; morning reports should consume precomputed history instead
of rebuilding it before sending.

## Quick Start

```bash
cp config.example.yaml config.yaml
# Fill api.tushare_token and optional DeepSeek/Gmail fields.

cargo build --release

# Full daily pipeline; default delivery mode is test.
./scripts/daily_pipeline.sh evening 2026-04-24 --test

# Rebuild review history from existing data.
./target/release/quant-cn review-backfill \
  --date-from 2026-03-23 \
  --date-to 2026-04-24

# Cron-friendly maintenance: backfill recent history and refresh alpha bulletin,
# without agents or email.
./scripts/precompute_alpha.sh 2026-04-24
```

Production delivery is explicit:

```bash
./scripts/daily_pipeline.sh --prod evening YYYY-MM-DD
```

## Delivery Modes

`scripts/daily_pipeline.sh`, `scripts/run_agents.sh`, and `scripts/send_email.py`
all support:

- `--test`: send only to one test recipient.
- `--prod`: send to full `reporting.recipients`.
- `--test-recipient=email@example.com`: override the test target.
- `QUANT_DELIVERY_MODE=test|prod`
- `QUANT_TEST_RECIPIENT=email@example.com`

Dry-run recipient resolution:

```bash
python3 scripts/send_email.py reports/2026-04-24_report_zh_evening.md \
  --delivery-mode test \
  --dry-run
```

## Headline Gate Policy

Headline/news state is advisory context only. It is rendered in the report so a
human can understand the tape, but it must not demote a candidate from
`CORE BOOK` by itself.

The report lane is determined by structural signal quality, direction,
execution score, strategy scope, and A-share-specific execution constraints.

Current lanes:

| Lane | Meaning |
|---|---|
| `CORE BOOK` | main report candidate eligible for shared `Execution Alpha` if stable |
| `THEME ROTATION` | theme/flow/rotation candidate, useful for recall or tactical context |
| `RADAR` | low confidence, neutral/bearish, no-fill, stale chase, or out-of-scope |

## Review Backfill

Historical `algorithm_postmortem` must not be built before execution analytics
exist. `review-backfill` now checks and fills these modules for each review
date before materializing the review ledger:

- `setup_alpha`
- `continuation_vs_fade`
- `open_execution_gate`

This prevents historical rows from collapsing into all `OBSERVE/WAIT` because
of missing `execution_score`.

`daily_pipeline.sh` defaults to `QUANT_CN_REVIEW_BACKFILL_TIMING=post-email`.
That keeps historical maintenance from delaying the report. Use
`QUANT_CN_REVIEW_BACKFILL_TIMING=pre-alpha` only for manual backfills where
latency is not important. Use `QUANT_CN_REVIEW_BACKFILL_DAYS=0` to skip it.

## Analytics

| Module | Purpose |
|---|---|
| `momentum` | multi-horizon return and trend context |
| `flow` | northbound, margin, block trade, hot money, turnover |
| `announcement` | event-conditioned response around disclosures |
| `unlock` | lockup expiration risk |
| `setup_alpha` | structure/setup score |
| `continuation_vs_fade` | follow-through vs fade diagnostics |
| `open_execution_gate` | chase, pullback, and entry-quality constraints |
| `vol_hmm` | limit-adjusted volatility regime diagnostics |
| `shadow_option` | A-share shadow option risk/convexity features |
| `shadow_option_alpha` | report-only shadow option alpha calibration |
| `macro_gate` | market-wide context |

A-share shadow options are not real single-name option trades. They are used for
risk correction, stale-chase detection, and convexity context.

## Shared Alpha Gate

After the producer has rendered/reviewed a date:

```bash
cd ..
target/release/quant-stack alpha evaluate \
  --date 2026-04-24 \
  --lookback-days 30 \
  --auto-select \
  --emit-bulletin
```

The CN candidate becomes `Equity Execution Alpha` only if:

- selected policy is the stable champion,
- policy scope is core/high-or-moderate/executable-now,
- rolling history passes CN stability thresholds,
- execution gate passes,
- headline context is not treated as a hard blocker.

## Project Layout

```text
quant-research-cn/
├── src/
│   ├── analytics/
│   ├── enrichment/
│   ├── fetcher/
│   ├── filtering/
│   ├── reporting/
│   └── storage/
├── scripts/
│   ├── daily_pipeline.sh
│   ├── run_agents.sh
│   └── send_email.py
├── data/                    # DuckDB, gitignored
└── reports/                 # payloads, reports, charts, gitignored
```

## Verification

```bash
cargo test filtering::notable
cargo test reporting filtering analytics
cargo build --release
```

Root integration smoke:

```bash
cd ..
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets cn \
  --session post \
  --send-reports \
  --delivery-mode test \
  --delivery-dry-run
```

## License

Private research tool. Not for redistribution.
