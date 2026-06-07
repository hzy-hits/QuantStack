# Factor Family OOS Failure Review - 2026-06-07

Scope: historical Factor Lab / autoresearch logs only. No new factor search was run.

Inputs:

- `factor-lab/experiments.jsonl`
- local runtime logs `factor-lab/runtime/autoresearch/cn/autoresearch.jsonl`
- local runtime logs `factor-lab/runtime/autoresearch/us/autoresearch.jsonl`

Review definition:

- True OOS failure = `gates=PASS` and `oos=FAIL`.
- Process failure = `oos=PASS` but checks/status/decision still prevent keep or promotion.
- IS-only rejects are not counted as OOS failures.

## Funnel Summary

| Metric | Count |
|---|---:|
| Total experiment rows reviewed | 2,465 |
| CN rows | 1,908 |
| US rows | 557 |
| IS gate pass rows | 696 |
| OOS-tested rows | 184 |
| OOS pass | 139 |
| OOS fail | 45 |
| OOS pass but not keep / checks failed or reverted | 89 |

The main conclusion is not that every factor idea is useless. The failure is concentrated in repeated families and in the handoff from OOS pass to reproducible keep/promotion.

## OOS Failure By Family

| Family | Market | OOS fail | OOS tested | Fail rate |
|---|---|---:|---:|---:|
| `reversal_recovery` | CN | 13 | 19 | 68.4% |
| `valuation_value` | CN | 5 | 15 | 33.3% |
| `correlation_divergence` | CN | 2 | 9 | 22.2% |
| `flow_absorption_liquidity` | US | 3 | 14 | 21.4% |
| `intraday_close_gap` | US | 3 | 14 | 21.4% |
| `flow_absorption_liquidity` | CN | 19 | 107 | 17.8% |

The raw fail count is largest in CN flow/amount/liquidity because autoresearch spent most attempts there. The highest actual fail rate is CN reversal/recovery.

## Recent Sessions

| Session | Market | Rows | IS pass | OOS pass/tested | OOS fails |
|---|---|---:|---:|---:|---|
| `20260602_060002_fd206b` | CN | 16 | 0 | 0/0 | - |
| `20260602_140002_0d2b0f` | CN | 20 | 3 | 2/3 | `stable_amount_pullback_20` |
| `20260602_145619_176062` | US | 20 | 2 | 2/2 | - |
| `20260603_060002_b50f3d` | CN | 22 | 2 | 0/2 | `weekly_log_amount_evenness_20`, `weekly_log_amount_stability_20` |
| `20260603_140002_270be5` | CN | 20 | 0 | 0/0 | - |
| `20260603_145509_614d50` | US | 14 | 0 | 0/0 | - |
| `20260604_060002_35b979` | CN | 22 | 2 | 0/2 | `stable_amount_reaccel_20`, `amount_stability_pullback_20` |
| `20260604_140002_7b9360` | CN | 17 | 2 | 2/2 | - |
| `20260604_145705_492d65` | US | 21 | 2 | 0/2 | `reclaim_relvol_accel_exp_10_20`, `gap_absorb_upper_close_10_40` |
| `20260605_060002_c506a5` | CN | 22 | 1 | 1/1 | - |
| `20260605_140003_315ee3` | CN | 19 | 1 | 0/1 | `amount_stability_pullback_20` |
| `20260605_145901_215228` | US | 21 | 1 | 0/1 | `upper_close_gap_absorb_20_40` |

Recent pattern: CN has repeated failures in `amount_stability_pullback` / weekly log amount stability variants. US has moved from one successful close-location/relative-volume cluster on 2026-06-02 to failed upper-close/gap-absorb variants on 2026-06-04 and 2026-06-05.

## Findings

1. CN reversal/recovery is not production-ready.
   The family passes IS with attractive IC/Sharpe on pullback and re-acceleration variants, then fails OOS frequently. This includes `leader_pullback_turnfloor_20`, `leader_turnfloor_efficiency_20`, `leader_turnover_efficiency_20`, `stable_amt_pullback_20`, `amount_stability_pullback_20`, and `stable_amount_reaccel_20`. The family should be quarantined unless a regime filter is added before OOS.

2. Flow/amount/liquidity is over-mined.
   It dominates the search set: 1,443 of 2,465 rows. It is not dead, but the search is producing many near-duplicates around amount stability, float-adjusted turnover, weekly amount smoothness, and participation floors. There were 437 IS-pass rows in this family that were not OOS-tested, which means the top-3 OOS funnel is overwhelmed by variants instead of evaluating distinct hypotheses.

3. CN value + flow needs separation from pullback value.
   `pb_float_amount_absorption_20` and `cheap_locked_float` passed OOS in multiple recent sessions, but `stable_amount_pullback_20` and related pullback/value variants failed. Treat "cheap with absorption" as a different family from "cheap or stable after pullback"; do not generalize one success to the other.

4. US intraday/close/gap family is regime-sensitive.
   `close_loc_relvol_stepup_10_40` and `close_loc_relvol_accel_10_40` passed on 2026-06-02, but `gap_absorb_upper_close_10_40`, `reclaim_relvol_accel_exp_10_20`, and `upper_close_gap_absorb_20_40` failed later in the same week. The family likely needs market regime, overnight gap, news, and options/Gamma context before promotion. Daily OHLC alone is too coarse for the stated intraday sponsorship hypothesis.

5. OOS pass is not enough in the current pipeline.
   89 rows show OOS pass but still not keep or promotion because checks failed or decision reverted. This is a healthy brake, but it also means autoresearch can report "OOS PASS" while no reproducible factor enters the system. Reports must separate "OOS PASS" from "kept/promoted".

## Quarantine Decisions

Do not promote or keep new variants from these templates without a manual review note and a family-level OOS rerun:

- CN `*_pullback_*`, `*_reaccel_*`, `leader_*turn*`, `stable_amt_pullback_20`, `amount_stability_pullback_20`.
- CN `weekly_log_amount_*`, `weekly_amount_change_stability_20`, and similar amount-smoothness clones.
- US `upper_close_gap_absorb_*`, `gap_absorb_upper_close_*`, `reclaim_relvol_accel_exp_*` until a regime filter exists.
- Generic flow/amount variants that are only algebraic rewrites of an existing family member and do not carry a new data source or counterparty story.

## Required Follow-Ups

1. Add a factor-family registry: `family_id`, parent hypothesis, market, allowed transforms, quarantine status, and canonical representative.
2. Change the OOS funnel from top-3 by IS IC to top-1 per family plus a family-level dedupe/correlation gate.
3. Report session summaries as `OOS pass / kept`, not just `OOS pass`.
4. For US intraday/gap factors, require at least one of: intraday volume-curve data, verified news timestamp context, or options/Gamma regime context before promotion.
5. For CN reversal/recovery, require explicit regime conditioning and beta/industry attribution before another promotion attempt.

Current operating decision: keep autoresearch manual-only. The historical record does not justify scheduled autonomous factor generation.
