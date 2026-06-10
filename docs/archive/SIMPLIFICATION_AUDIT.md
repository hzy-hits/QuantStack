# Simplification and Overdesign Audit

> **ARCHIVED 2026-06-10** — 2026-05-08 的点状审计,结论已消化进后续重构(REFACTOR_PLAN/PROJECT_CONSOLIDATION)。

This audit identifies which parts of the stack should remain modular, which are
overdesigned, and which simplifications are available but not fully used.

## Keep

### Shared alpha maturity gate

Keep `crates/quant-stack-core`.

Reason: both markets need one place where "interesting" becomes "executable".
The gate prevents LLM narrative, headlines, and Factor Lab priors from becoming
trade instructions without historical evidence.

Required improvement:

- Keep extending unit tests beyond the current policy threshold, hysteresis, and
  market/session filtering coverage into full bulletin section classification.

### Market-specific producers

Keep US Python and CN Rust producers separate.

Reason: US and A-share data sources, sessions, calendars, instruments, and
execution constraints are materially different. Forcing one producer would add
abstraction without reducing trading risk.

### Factor Lab as research-only

Keep Factor Lab, but keep it downstream of the main report contract.

Reason: it discovers useful priors, but current reports show its own composite
IC_IR can be small and unstable. It should influence ranking and recall, not
override execution alpha.

## Simplify

### 1. Duplicate orchestration paths

Current state:

- Root Rust `daily` and `us-daily` exist.
- US `scripts/run_full.sh` remains.
- CN `scripts/daily_pipeline.sh` remains.
- Watchdog still references shell scripts.

Risk:

- Two paths can send different reports or run different gates.
- Fixes land in one path and miss the other.

Decision:

- Make Rust state machines the canonical cron entry.
- Keep shell scripts as wrappers only.
- Add a CI or smoke check that wrapper command construction matches Rust
  defaults.

### 2. Report model not always visibly enforced

Current state:

- Root `daily_report_model` exists and has rows.
- CN 2026-05-07 has shared gate evidence.
- US 2026-05-07 now has shared root gate evidence after running the explicit
  dual-market `alpha evaluate` command; the report still needs to embed that
  status visibly.

Risk:

- The report may rely on legacy local logic while the shared gate is intended to
  be the source of truth.

Decision:

- Final render should read or embed `report_model_{market}_{session}.json`.
- If absent, fail closed or visibly mark "shared gate unavailable; no formal
  execution alpha".

### 3. Options and shadow-options scope

Current state:

- US has real `options_alpha` rows.
- CN has `shadow_option` and `shadow_option_alpha`.
- Reports mostly label these correctly, but both can be misunderstood as account
  trading systems.

Risk:

- Overdesign if option modules start managing positions, sizing, or a book
  before fills and PnL history exist.

Decision:

- US options: evidence and expression candidates only.
- CN shadow options: risk/convexity discount only.
- No Kelly sizing, no live account sizing, no automatic option book management
  in this repo.

### 4. Legacy strategy families

Current state:

- Legacy HIGH/MOD, structural core, low/core/trending, opportunity rankers, and
  observed lifecycle models coexist.

Risk:

- Reports can sound as if several independent systems agree when they are
  variants of the same historical ledger.

Decision:

- Keep legacy families as baselines.
- Only Alpha Factory-proven or shared-gate-selected sleeves can receive
  production sizing language.
- Reports should include an "independent bet count" or cluster note whenever
  multiple names come from the same theme.

### 5. Factor Lab appenders

Current state:

- US and CN have separate Factor Lab injection/sync code paths.
- Root CLI also contains Factor Lab handling.

Risk:

- Inconsistent wording, stale date detection, and appendix placement.

Decision:

- Extract a shared text contract:
  `status`, `trade_date`, `age_days`, `research_only`, candidate table.
- Both markets render that contract in their local language/style.

## Candidate Removals or Freezes

Do not delete immediately; freeze until usage is proven.

| Area | Recommendation | Reason |
|---|---|---|
| `quant-stack-py` | Keep thin; avoid expanding | Useful for tests/notebooks, but can become a second API surface. |
| Shell orchestration | Wrapper only | Rust state machines are easier to make deterministic. |
| US legacy HIGH/MOD | Baseline only | Useful comparison, not a production sleeve without current gate proof. |
| CN `limit_up_model` | Radar only | Needs 9:25/9:35 auction/open data before execution language. |
| Option book logic | Do not add | Objective is research reports, not brokerage automation. |

## Target Module Shape

```text
quant-stack-core
  alpha maturity, bulletin, report model

quant-stack-cli
  canonical orchestration, state machine, delivery mode control

quant-research-v1
  US ingestion -> analytics -> report snapshot -> narrative rendering

quant-research-cn
  CN ingestion -> analytics -> report snapshot -> narrative rendering

factor-lab
  factor discovery -> research priors -> exported evidence
```

The integration rule is one-way:

```text
producers write facts -> shared gate reads facts -> reports narrate shared gate
```

No module should bypass this path to create a formal trade ticket.

## Next Implementation Backlog

1. Add `quant-stack-core` fixture tests for full bulletin section
   classification.
2. Add a production pre-send check for report model presence by market/session.
3. Make US final report embed the shared report model status.
4. Refactor Factor Lab appendix parsing into one shared contract.
5. Move watchdog commands from shell scripts to root Rust state machines.
6. Add persisted missed-alpha cause buckets.
7. Add auction/minute data before upgrading CN execution gates beyond review
   language.
