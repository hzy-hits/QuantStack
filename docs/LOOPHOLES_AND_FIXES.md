# Loopholes and Fixes

This is the implementation checklist for the "do not trust the strategy blindly"
loop. The target is not 100% market certainty; that is impossible. The target is
100% process enforcement for the parts this repo controls:

```text
facts -> shared alpha/report model -> final report -> delivery
```

## Implemented Fixes

### 1. US `us-daily` could bypass the shared gate

Loophole:

- `quant-stack us-daily` could run data, Factor Lab, agents, report, and
  delivery without first writing the shared alpha/report model for that date.

Fix:

- `us-daily` now runs the shared US alpha gate after Factor Lab import.
- It materializes the US report model from the root history DB.
- It injects `Shared Report Model Status` into the structural payload before
  agents run.
- It prepends `Shared Report Model Status` into the final report if the merge
  agent omitted it.
- Missing report model is fatal outside dry-run.

Evidence:

- `target/release/quant-stack us-daily --dry-run ...` emits
  `shared_alpha_evaluated` and `shared_report_model_written`.

### 2. CN shell cron path could bypass Rust state machine

Loophole:

- `quant-research-cn/scripts/daily_pipeline.sh` contained a full independent
  producer/render/alpha/agent/email workflow.
- Some alpha failures were non-fatal, which allowed a report to continue without
  enforcing the shared model.

Fix:

- `daily_pipeline.sh` is now a compatibility wrapper only.
- It delegates to `quant-stack daily --markets cn --run-producers
  --with-narrative --send-reports`.
- The root Rust state machine owns producer, Factor Lab import, alpha evaluate,
  report model writing, CN render, agents, delivery, and review maintenance.

Evidence:

- `bash -n quant-research-cn/scripts/daily_pipeline.sh` passes.
- `QUANT_STACK_BIN=.../target/release/quant-stack
  quant-research-cn/scripts/daily_pipeline.sh --dry-run ...` enters Rust
  states `alpha_evaluated`, `report_model_written`,
  `cn_payloads_finalized`, and `delivery_ready`.

### 3. Watchdog cron still called shell workflows

Loophole:

- The watchdog default tasks invoked `bash scripts/run_full.sh` and
  `bash scripts/daily_pipeline.sh`.

Fix:

- US pre/post tasks now call `target/release/quant-stack us-daily`.
- CN morning/evening tasks now call `target/release/quant-stack daily`.
- Shell wrappers remain for manual compatibility, not as the canonical cron
  target.

Evidence:

- `python3 -m py_compile
  quant-research-v1/src/quant_bot/orchestration/watchdog.py` passes.

### 4. Final reports could omit shared report model state

Loophole:

- Agent output could mention alpha status in prose but omit the exact shared
  `ev_status`, selected policy, evaluated-through date, and section counts.

Fix:

- Root `daily` enforces `Shared Report Model Status` before CN delivery and
  before US delivery when using the root sender.
- `us-daily` enforces the same block before US delivery.
- The status block is inserted from `reports/{date}_report_model_*_post.json`
  or the review-dashboard fallback.
- Missing report model is fatal outside dry-run.

Evidence:

- `cargo test -p quant-stack-cli --bin quant-stack` covers report model fallback
  and final report prepending.

## Remaining Non-100% Areas

These cannot be made factually 100% certain by code alone:

- Future alpha profitability.
- Slippage, gaps, limit-up/limit-down, and execution availability.
- LLM narrative judgment quality beyond deterministic pre-send constraints.
- External data completeness and vendor outages.

Process fixes still worth doing:

1. Add full bulletin fixture tests for execution/tactical/options/recall/blocked
   classification.
2. Add a no-LLM final report renderer for the `Shared Report Model Status`,
   scorecard, and action map sections.
3. Persist missed-alpha cause buckets instead of relying on prose.
4. Add A-share auction/minute data before upgrading `open_execution_gate` beyond
   review language.
5. Add CI that rejects cron/watchdog configs pointing at non-Rust daily entry
   points.

