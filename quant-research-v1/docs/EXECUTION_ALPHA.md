# Execution Alpha Layer

## Problem

The original US pipeline was strong on `state` and `risk`, but weak on `execution timing`.
This created a recurring failure mode:

- the report anchored on prior close
- overnight gaps consumed most of the expected move
- the report still looked mechanically bullish or bearish even when the trade was already stretched

## Design

The execution-aware layer does **not** replace the main research stack. It sits on top of it.

1. `momentum_risk / earnings_risk / sentiment / options`
   Produce directional context and background probabilities.
2. `overnight_gate`
   Decides whether the next session is still executable.
3. `notable / classify / risk_params / reporting`
   Consume the gate and stop treating stale close as the only valid entry reference.

## `overnight_gate`

Location: `src/quant_bot/analytics/overnight_gate.py`

Stored in `analysis_daily` with `module_name='overnight_gate'`.

Key inputs:

- latest close and recent range from `prices_daily`
- current option reference price from `options_analysis.current_price`
- expected move from the nearest options snapshot
- recent options context: IV / skew / put-call ratio history
- options sentiment and momentum risk outputs

Key outputs:

- `action`: `executable_now | wait_pullback | do_not_chase`
- `ref_price`
- `gap_pct`
- `gap_vs_expected_move`
- `gap_vs_atr`
- `cone_position_68`
- `pullback_price`
- `max_chase_gap_pct`

## Downstream effects

- `filtering/notable.py`
  Lowers tradability when the move is stretched.
- `signals/classify.py`
  Adds execution-aware exhaustion flags.
- `analytics/risk_params.py`
  Uses `pullback_price` instead of blindly using prior close when the gate says to wait.
- reporting
  Shows whether the name is executable now, needs pullback, or should not be chased.

## First-version constraints

- There is still no dedicated extended-hours trade table.
- The gate currently uses `options_analysis.current_price` as the best live reference.
- This is an execution filter, not a standalone alpha model.

## Next upgrades

- Add a true `overnight_continuation` model
- Separate `event gap` from `non-event gap`
- Add gamma/expected-move overshoot structure when the options data quality is good enough
