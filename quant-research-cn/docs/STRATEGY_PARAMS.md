# Strategy Parameters

Every numeric threshold in the A-share strategy path should have provenance.

## Provenance

- `market_rule`: Exchange or account rule, such as T+1, 10cm/20cm/ST limits, board eligibility.
- `statistical`: Comes from a defined estimator or confidence bound. Example: one-sided 80% normal z = `1.2816`.
- `cost_assumption`: Explicit trading cost, slippage, or fill-cost assumption.
- `calibrated_walk_forward`: Selected by out-of-sample walk-forward EV/risk performance.
- `legacy_heuristic`: Old scoring constant kept only as a default until calibration replaces it.

## EV80 LCB

`EV80 LCB` is the one-sided 80% lower confidence bound of strategy EV:

```text
ev_lcb_80 = ev_pct - 1.2816 * max(realized_std_pct, risk_unit_pct) / sqrt(fills)
```

It is not win rate. It is a conservative haircut on expected return. A strategy with positive average EV can still fail release if the sample is small or the returns are too volatile.

Example:

```text
EV = +0.40%
risk unit = 2.00%
fills = 25
EV80 LCB = 0.40 - 1.2816 * 2.00 / sqrt(25) = -0.11%
```

The average is positive, but the lower confidence bound is still negative, so the strategy should stay in research/setup rather than Execution Alpha.

## Calibration

Weekend maintenance runs:

```bash
python factor-lab/scripts/calibrate_strategy_params.py --market cn
```

The generated artifact is written to:

```text
factor-lab/runtime/strategy_calibration/cn/strategy_params.generated.yaml
quant-research-cn/config/strategy_params.generated.yaml
```

The current artifact records strategy EV, limit-up model diagnostics, and the list of remaining legacy heuristic constants. Runtime code should only consume calibrated parameters after OOS EV LCB improves versus the current production default.
