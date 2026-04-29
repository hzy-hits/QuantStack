# quant-research-v1

US equities research producer for Quant Stack. It ingests market data, computes
structured analytics, renders the US daily payload/report, and sends Gmail
delivery. The root `quant-stack` CLI consumes its review ledger for shared alpha
maturity and bulletin generation.

The operating rule is:

**Python/Rust compute; agents narrate; Quant Stack promotes only stable alpha.**

`Execution Alpha` in the report is a research execution candidate, not an order.

## Pipeline

```text
Finnhub / FRED / SEC / Polymarket / yfinance / CBOE
        |
        v
Rust + Python ingestion -> DuckDB -> analytics -> report_decisions
        |
        v
payload markdown -> agents -> Chinese report -> Gmail
        |
        v
Quant Stack stable alpha gate -> alpha_bulletin_us.md
```

The full daily runner is now the Rust `quant-stack us-daily` state machine.
`scripts/run_full.sh` remains for cron/watchdog compatibility, but it only
normalizes legacy flags and delegates to `quant-stack us-daily`.

The report renderer reads `alpha_bulletin_us.md` when present and skips it when
missing, so the producer can still run while the shared gate is being repaired.

## Quick Start

```bash
cp config.example.yaml config.yaml
uv sync

# Build the async Rust fetcher.
cd rust
cargo build --release
cd ..

# Generate/re-render a daily report.
uv run python scripts/run_daily.py --date 2026-04-24 --session post

# Full wrapper, default delivery mode is test.
./scripts/run_full.sh 2026-04-24 --test
```

Equivalent direct Rust entrypoint:

```bash
../target/release/quant-stack us-daily \
  --stack-root .. \
  --session post \
  --delivery-mode test \
  --test-recipient you@example.com \
  2026-04-24
```

Production send is explicit:

```bash
./scripts/run_full.sh 2026-04-24 --prod
```

## Delivery Modes

The delivery wrapper and `send_report.py` support the same contract as the root
CLI:

```bash
# Test send: one test recipient only.
uv run python scripts/send_report.py \
  --send \
  --date 2026-04-24 \
  --session post \
  --lang zh \
  --delivery-mode test

# Dry-run recipient resolution.
uv run python scripts/send_report.py \
  --send \
  --date 2026-04-24 \
  --session post \
  --lang zh \
  --delivery-mode test \
  --dry-run

# Production recipient list.
uv run python scripts/send_report.py \
  --send \
  --date YYYY-MM-DD \
  --session post \
  --lang zh \
  --delivery-mode prod
```

`QUANT_TEST_RECIPIENT` can override the default test recipient. Production uses
`reporting.recipients` in `config.yaml`.

## Analytics

Core modules include:

| Module | Purpose |
|---|---|
| `momentum_risk` | conditional return probability by regime/vol bucket |
| `earnings_risk` | event-conditioned upside probability |
| `hmm_regime` | SPY latent state and calibration diagnostics |
| `overnight_continuation_alpha` | post-close continuation/fade diagnostics |
| `options_alpha` | directional, vol, VRP, flow, liquidity gate, expression choice |
| `algorithm_postmortem` | captured/missed/stale/false-positive review labels |
| `factor_lab` | research priors and recall leads; never direct sizing |
| `reporting.render` | payload and report markdown with alpha bulletin injection |

Options alpha emits:

- `directional_edge`
- `vol_edge`
- `vrp_edge`
- `flow_edge`
- `liquidity_gate`
- expression: `stock_long`, `call_spread`, `put_spread`, `wait`, or `blocked`

## Strategy Parameters

US execution/risk thresholds now follow the same provenance pattern as CN. The
runtime loads, in order:

- `QUANT_US_STRATEGY_PARAMS`
- `config/strategy_params.generated.yaml`
- `config/strategy_params.yaml`
- `../factor-lab/runtime/strategy_calibration/us/strategy_params.generated.yaml`

The wired runtime sections are `risk_params`, `options_alpha`,
`overnight_gate`, and `overnight_continuation_alpha`. Defaults remain
conservative legacy heuristics unless the calibration artifact can prove an OOS
improvement. The US calibration job reads `algorithm_postmortem`,
`alpha_postmortem`, `analysis_daily`, and `options_alpha`, then only activates a
candidate parameter set when the walk-forward OOS EV80 lower-confidence bound is
positive and better than the current default.

Generate the US artifact:

```bash
cd ..
python factor-lab/scripts/calibrate_strategy_params.py --market us
```

The same command is called by `factor-lab/scripts/weekly_maintenance.py` for
both CN and US. If the data is not strong enough, the artifact still refreshes
but keeps `provenance: legacy_heuristic`; that is intentional and prevents a
single noisy week from rewriting production gates.

## Shared Alpha Gate

The shared gate lives in the root repo:

```bash
cd ..
target/release/quant-stack alpha evaluate \
  --date 2026-04-24 \
  --lookback-days 30 \
  --auto-select \
  --emit-bulletin
```

For US, the core champion policy is selected only if historical rows pass the
US stability thresholds. Headline state is advisory context only. Factor Lab is
rendered as `research prior / recall lead`; only historical
`won_and_executable` postmortem rows can be described as captured alpha.

## Project Layout

```text
quant-research-v1/
├── rust/                    # async fetcher
├── scripts/
│   ├── run_full.sh          # cron-friendly wrapper
│   ├── run_daily.py         # producer and renderer
│   ├── run_agents.sh        # agent synthesis
│   └── send_report.py       # Gmail delivery
├── src/quant_bot/
│   ├── analytics/
│   ├── filtering/
│   ├── reporting/
│   ├── signals/
│   └── storage/
├── tests/
├── data/                    # DuckDB, gitignored
└── reports/                 # payloads, reports, charts, gitignored
```

## Verification

```bash
python -m unittest discover tests
python -m unittest tests/test_strategy_backtest_gate.py
python -m unittest tests/test_options_alpha.py
```

Root integration smoke:

```bash
cd ..
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us \
  --session post \
  --send-reports \
  --delivery-mode test \
  --delivery-dry-run
```

## License

Private research tool. Not for redistribution.
