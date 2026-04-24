# Execution Alpha Layer

## Problem

The CN pipeline already had direction, risk, and `shadow option` work, but it still missed the
last mile:

- many names only became "interesting" after a closing surge
- the next session often opened too high to chase
- the report could still sound constructive even when the entry was already poor

## Design

The execution-aware layer is split into three modules and sits on top of the existing research
stack.

1. `shadow_option`
   Keeps doing risk and convexity estimation.
2. `setup_alpha`
   Looks for pre-breakout structure.
3. `continuation_vs_fade`
   Estimates whether a strong move is more likely to continue or mean-revert.
4. `open_execution_gate`
   Decides whether the next session is still executable.

## Modules

### `setup_alpha`

Location: `src/analytics/setup_alpha.rs`

Goal: detect names that are being prepared before the obvious breakout.

Main features:

- recent volatility compression
- close location inside the 20-day range
- quiet volume build
- flow confirmation
- shadow downside room

Outputs:

- `setup_score`
- `setup_direction`
- `close_location_20d`
- `compression_score`
- `volume_build`

### `continuation_vs_fade`

Location: `src/analytics/continuation_vs_fade.rs`

Goal: distinguish follow-through from exhaustion.

Main features:

- breakout strength
- information score
- setup score
- recent return pressure
- band position / stretch
- shadow vol and downside stress

Outputs:

- `continuation_score`
- `fade_risk`
- `continuation_direction`

### `open_execution_gate`

Location: `src/analytics/open_execution_gate.rs`

Goal: decide whether the next open is still actionable.

Main features:

- ATR-like daily range
- setup + continuation quality
- fade risk
- recent move stretch
- shadow vol / downside stress

Outputs:

- `execution_score`
- `max_chase_gap_pct`
- `pullback_trigger_pct`
- detail fields:
  - `execution_mode`
  - `pullback_price`

## Downstream effects

- `filtering/notable.rs`
  Uses these signals in shortlist ranking, convergence classification, and report bucket routing.
- `reporting/render.rs`
  Shows setup, continuation/fade, and next-open execution guidance inside each notable item.

## Shadow option relationship

`shadow_option` remains the risk engine, not the timing engine.

It answers:

- how expensive the downside convexity is
- whether the name has entered a high-volatility chase zone
- where the implied floor and stress levels are

The new execution layer answers:

- whether the structure existed before the move
- whether the next session should be chased, faded, or bought only on pullback

## First-version constraints

- There is no auction/minute bar table in the current core pipeline.
- `open_execution_gate` is therefore a pre-open threshold model, not a true intraday execution model.
- The next upgrade should incorporate `09:15-09:25` auction and `09:30-09:45` open behavior.
