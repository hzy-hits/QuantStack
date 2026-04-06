# Signal Builder Research Note

- Date: 2026-03-25
- Scope: US/CN promoted-factor signal construction, SigReg role, and discrepancy vs earlier Claude summary
- Authoring context: current local repo state after voting-composite fix and SigReg penalty integration

## What Was Compared

Three signal builders were compared under one protocol:

1. Single-factor top picks
2. IC_IR-weighted composite
3. Fixed voting composite

Two evaluation modes were used:

1. `walk_forward_backtest` IS folds in [walk_forward.py](/home/ivena/coding/python/factor-lab/src/backtest/walk_forward.py)
2. Full-period replay from `2024-03-25` to `2026-03-24`

The full-period replay uses the same quintile long-short construction as the backtest engine:

- daily `Q5 - Q1` return
- cost-adjusted by average turnover
- annualized Sharpe from daily long-short return divided by 5-day horizon

This is still a hindsight replay using the current promoted-factor snapshot, not a strict live replay.

## Coverage

- US replay window: `2024-03-25` to `2026-03-24`, `503` trading days
- CN replay window: `2024-03-25` to `2026-03-24`, `483` trading days

## Full-Period Replay Results

Snapshot note:

- US snapshot used here had `23` promoted factors
- CN snapshot used here had `13` promoted factors
- US changed from `20` to `23` promoted because a same-day smoke test ran `daily_pipeline`

### US

- Single-factor top 3 average: `Sharpe -0.632`, `CumRet -17.44%`
- Best single factor: `rsv_inv_40`, `Sharpe -0.302`, `CumRet -11.16%`
- IC_IR-weighted composite: `Sharpe -1.862`, `CumRet -59.99%`
- Fixed voting composite: `Sharpe +1.189`, `CumRet +57.97%`

Other voting stats:

- `avg_ic = -0.011`
- `avg_ic_ir = -0.080`
- `avg_turnover = 0.2584`
- `positive_day_pct = 56.3%`

Interpretation:

- Fixed voting is materially better than the other two US builders on this hindsight full replay.
- But its IC and IC_IR remain negative, so the return seems to come from extreme-bucket spread rather than stable full cross-sectional ranking.

### CN

- Single-factor top 3 average: `Sharpe -0.269`, `CumRet -2.86%`
- Best single factor: `range_vol_20`, `Sharpe +0.429`, `CumRet +4.28%`
- IC_IR-weighted composite: `Sharpe -1.315`, `CumRet -18.45%`
- Fixed voting composite: `Sharpe -2.083`, `CumRet -25.77%`

Other voting stats:

- `avg_ic = +0.0074`
- `avg_ic_ir = +0.046`
- `avg_turnover = 0.3407`
- `positive_day_pct = 44.5%`

Interpretation:

- Current CN voting composite still degrades signal quality.
- CN alpha, if any, is still at the single-factor fragment level rather than in the current composite builder.

## Why This Differs From Earlier Claude Output

The earlier Claude statement about `Sharpe 2.1` referred to:

- report: [autoresearch_us_20260324.md](/home/ivena/coding/python/factor-lab/reports/autoresearch_us_20260324.md#L43)
- factor: `sign_persistence_reversal_60d`
- metrics on that line: `IC=-0.0324`, `IC_IR=-0.278`, `Sharpe=2.101`, `FAIL`

That does **not** mean the production strategy Sharpe is `2.1`.

There are four separate reasons for the discrepancy:

1. Different object

- Claude cited a single candidate factor
- This research evaluated signal builders built from the promoted pool

2. Different metric

- The candidate report logs a single-factor IS walk-forward `Q5 - Q1` Sharpe
- This research also evaluated full-period replay Sharpe across the whole promoted snapshot

3. Different directional meaning

- A factor can show positive `Q5 - Q1` spread Sharpe while having negative `IC` and `IC_IR`
- That means the extreme tails are making money, but the whole cross-section is not ranked correctly

4. Different snapshot and code state

- The earlier Claude report is from `2026-03-25 00:12` and predates the voting-composite fix
- Today’s replay used the current promoted-factor snapshot and the fixed voting logic

So the outputs are not contradictory. They are answering different questions with different inputs.

## What “Extreme-Bucket Alpha” Means Here

In the current US voting result:

- the strategy makes money by separating the most extreme long bucket from the most extreme short bucket
- but the full ordering from low score to high score is not monotonic and not consistently rank-predictive

That is why:

- Sharpe can be positive
- while `IC` and `IC_IR` remain near zero or negative

Practical meaning:

- useful for concentrated extreme selection
- weak evidence for broad rank-based portfolio construction

## What SigReg Is And Is Not Doing

SigReg is now wired in as a penalty layer in [daily_pipeline.py](/home/ivena/coding/python/factor-lab/src/mining/daily_pipeline.py#L295) and as an extra health input in [step4_health_check](/home/ivena/coding/python/factor-lab/src/mining/daily_pipeline.py#L793).

It currently affects:

- redundancy penalty via multi-collinearity `R²`
- diversity penalty via marginal change in factor-set diversity score
- health penalty via IC-series stability and regime-change detection

It does **not** create alpha by itself.

What SigReg is useful for:

- avoiding duplicate factors entering the pool
- down-ranking factors that are linear combinations of existing ones
- flagging or retiring factors whose IC behavior looks unstable

What SigReg does not solve:

- a weak underlying factor pool
- a bad composite-construction rule
- market-specific alpha decay

Bottom line:

- SigReg is useful as a secondary constraint and hygiene layer
- SigReg is not a primary alpha source
- If the composite logic is wrong, SigReg will not rescue it

## Current Bottom-Line View

### US

- There may be usable alpha in the fixed voting composite
- But it currently looks like sparse tail alpha, not stable rank alpha
- This should stay in research / paper mode until confirmed by stricter live-style replay

### CN

- Current composite builders are not producing convincing strategy-level alpha
- The best evidence is still isolated single factors, and even those are weak

## Recommended Next Steps

1. Freeze daily promoted snapshots and factor weights
2. Run a true live-style replay using only information available on each date
3. Decompose US voting PnL by month, factor contribution, and bucket behavior
4. Do not promote CN voting composite to main strategy in its current form
5. Keep SigReg as a penalty/health layer, not as an alpha claim
