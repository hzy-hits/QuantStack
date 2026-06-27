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

> ⚠️ DECOMMISSIONED 2026-06-24 — factor-lab 已退役;以下为历史记录,不反映现状。详见 docs/DECISIONS.md。

factor-lab calibration is retired (the `calibrate_strategy_params.py` job and its `factor-lab/runtime/strategy_calibration/...` artifacts no longer run). Strategy params now come from local config / env only:

```text
QUANT_CN_STRATEGY_PARAMS
config/strategy_params.generated.yaml
quant-research-cn/config/strategy_params.generated.yaml
```

When no calibrated artifact is present, built-in defaults remain effective.
