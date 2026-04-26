# Quant Stack

Quant Stack is the shared control plane for the US equities and China A-share
research systems. The rule is deliberately narrow:

**market producers compute facts, Quant Stack judges alpha maturity, reports
narrate the result.**

The system is not a broker and does not place orders. `Execution Alpha` means a
candidate passed historical stability and execution constraints for the daily
research bulletin; position sizing and live trading remain outside this repo.

## Current Shape

```text
US producer               CN producer
quant-research-v1         quant-research-cn
     |                         |
     v                         v
report_decisions + outcomes + algorithm_postmortem
     |                         |
     +----------- quant-stack core -----------+
                 stable alpha gate
                 champion/challenger selection
                 options/shadow-options alpha
                 report model + bulletin
                              |
                              v
                  daily markdown + Gmail delivery
```

The producer layer remains market-specific because raw inputs are different:
US uses Finnhub/FRED/SEC/Polymarket/yfinance/CBOE; A-share uses
Tushare/AKShare, local calendars, flow data, announcements, unlocks, and
ETF-option-derived shadow diagnostics.

Everything after the producer boundary is shared:

```text
review ledger -> stable alpha gate -> alpha bulletin -> report model -> delivery
```

## Daily Commands

Run the shared alpha gate and write both market bulletins:

```bash
target/release/quant-stack alpha evaluate \
  --date 2026-04-24 \
  --lookback-days 30 \
  --auto-select \
  --emit-bulletin
```

Run the post-producer daily control plane:

```bash
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us,cn \
  --session post \
  --lookback-days 30
```

Send test email to the configured test recipient only:

```bash
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us,cn \
  --session post \
  --send-reports \
  --delivery-mode test
```

Production delivery is explicit:

```bash
target/release/quant-stack daily \
  --date YYYY-MM-DD \
  --markets us,cn \
  --session post \
  --send-reports \
  --delivery-mode prod
```

Use `--delivery-dry-run` before any delivery change. Test mode resolves to one
test recipient by default; prod uses `reporting.recipients` from each market
config.

## Alpha Bulletin

The shared bulletin has the same sections for both markets:

- `Equity Execution Alpha`: selected champion policy plus execution gate pass.
- `Tactical / Theme Rotation Alpha`: stable non-core theme/rotation candidates;
  visible for research, not treated as core execution.
- `Options / Shadow Options Alpha`: US real options expressions and A-share
  shadow-option risk/convexity diagnostics.
- `Recall Alpha`: research priors and recall leads that did not pass execution.
- `Blocked / Out-of-scope Alpha`: explicit blocker reasons such as EV unknown,
  no fill, stale chase, low R:R, strategy scope, or execution score failure.

Headline/news state is context only. It is included in the report so a human can
read the market tape, but it is not allowed to veto a candidate by itself.

## Stability Gate

The gate evaluates rolling historical outcomes by policy. Selection only uses
completed evaluation windows:

```text
report_date <= as_of - horizon
evaluation_date <= as_of
```

This avoids incomplete outcomes and future leakage. Policies must pass market
thresholds before they can become the daily champion.

Initial thresholds:

| Market | Fills | Active Buckets | Avg Trade | Median | Strict Win | Max DD | Top Winner Share |
|---|---:|---:|---:|---:|---:|---:|---:|
| US | 20 | 10 | > 0.40% | >= 0 | > 45% | > -25% | <= 45% |
| CN | 50 | 15 | > 0.30% | >= 0 | > 43% | > -8% | <= 25% |

Champion/challenger hysteresis prevents daily churn: if the incumbent is still
eligible, a challenger must beat its stability score by 15% before replacement.

## A-Share Review Repair

The A-share review ledger needs execution analytics to exist before historical
postmortems are rebuilt. `quant-cn review-backfill` now ensures these modules
are present for each review date:

- `setup_alpha`
- `continuation_vs_fade`
- `open_execution_gate`

This prevents historical `algorithm_postmortem` from collapsing into all
`OBSERVE/WAIT` rows because of missing `execution_score`.

## Project Layout

```text
quant-stack/
├── crates/
│   ├── quant-stack-core      # shared alpha gate, bulletin, report model
│   ├── quant-stack-cli       # root daily control plane
│   └── quant-stack-py        # thin PyO3 bindings for tests/notebooks/legacy Python
├── scripts/
│   └── run_strategy_backtest_report.py  # Python fallback/compat gate
├── quant-research-v1/        # US producer, report, agents, delivery
├── quant-research-cn/        # A-share producer, report, agents, delivery
└── factor-lab/               # research factor discovery
```

## Verification

Useful checks after changes:

```bash
python -m unittest quant-research-v1/tests/test_strategy_backtest_gate.py
cargo build -p quant-stack-cli --release
cargo test -p quant-stack-core --lib
(cd quant-research-cn && cargo test filtering::notable)
```

Full alpha smoke for the latest validated date:

```bash
target/release/quant-stack daily \
  --date 2026-04-24 \
  --markets us,cn \
  --session post \
  --send-reports \
  --delivery-mode test \
  --delivery-dry-run
```

## Production Notes

- Default delivery mode is `test`; prod must be explicit.
- `QUANT_DELIVERY_MODE=prod` enables full recipient lists in wrapper scripts.
- `QUANT_TEST_RECIPIENT=email@example.com` overrides the test recipient.
- Daily pipeline order matters: producer -> review backfill -> alpha bulletin ->
  final render -> delivery.

## License

Private research tool. Not for redistribution.
