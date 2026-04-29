# Autoresearch Rollup — CN

- Period: 2026-04-01 to 2026-04-30
- Detailed JSONL sessions: 6
- Journal-only earlier sessions: 11
- Detailed experiments: 150
- IS gates pass: 102
- OOS pass: 17
- Decisions: keep=0, candidate=84, revert=66
- Checks failed after OOS/gates: 17

## What Repeated

- anti-autocorr: 68 gate-pass occurrences
- float-flow: 45 gate-pass occurrences
- stability: 41 gate-pass occurrences
- turnover: 33 gate-pass occurrences
- corridor/evenness: 16 gate-pass occurrences
- volume: 11 gate-pass occurrences
- value: 2 gate-pass occurrences

## Top Gate-Pass Candidates

| # | Name | IC | IC_IR | Sharpe | Gates | OOS | Decision | Formula |
|---|------|----|-------|--------|-------|-----|----------|---------|
| 1 | anti_autocorr_turnover_stability_20 | 0.0403 | 0.497 | 3.701 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(-ts_std(turnover_rate,20)/ts_mean(turnover_rate,20))` |
| 2 | anti_autocorr_float_flow_smooth_20 | 0.0399 | 0.495 | 3.724 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(amount/circ_market_cap,20)/ts_std(amount/circ_market_cap,20))` |
| 3 | anti_autocorr_float_stability_20 | 0.0399 | 0.495 | 3.724 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(amount/circ_market_cap,20)/ts_std(amount/circ_market_cap,20))` |
| 4 | anti_autocorr_turnover_corridor_20 | 0.0383 | 0.49 | 4.135 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(turnover_rate,20)/ts_max(turnover_rate,20))` |
| 5 | anti_autocorr_turnover_corridor_20 | 0.0383 | 0.49 | 4.135 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(turnover_rate,20)/ts_max(turnover_rate,20))` |
| 6 | anti_autocorr_float_corridor_20 | 0.0379 | 0.485 | 4.003 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(amount/circ_market_cap,20)/ts_max(amount/circ_market_cap,20))` |
| 7 | anti_autocorr_weekly_turnover_evenness_20 | 0.0372 | 0.479 | 3.253 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(ts_sum(turnover_rate,5),20)/ts_max(ts_sum(turnover_rate,5),20))` |
| 8 | anti_autocorr20_weekly_float_evenness | 0.0372 | 0.479 | 3.218 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(ts_sum(amount/circ_market_cap,5),20)/ts_max(ts_sum(amount/circ_market_cap,5),20))` |
| 9 | anti_autocorr_amount_peak_balance_20 | 0.0366 | 0.475 | 2.167 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(amount,20)/ts_max(amount,20))` |
| 10 | anti_autocorr_weekly_amount_evenness_20 | 0.0367 | 0.473 | 3.934 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(ts_sum(amount,5),20)/ts_max(ts_sum(amount,5),20))` |
| 11 | anti_autocorr_weekly_amount_evenness_20 | 0.0367 | 0.473 | 3.934 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(ts_sum(amount,5),20)/ts_max(ts_sum(amount,5),20))` |
| 12 | anti_autocorr_float_evenness_20 | 0.0336 | 0.46 | 2.602 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(amount/circ_market_cap,20)/ts_max(amount/circ_market_cap,20))` |
| 13 | anti_autocorr_float_peak_balance_20 | 0.0336 | 0.46 | 2.602 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_mean(amount/circ_market_cap,20)/ts_max(amount/circ_market_cap,20))` |
| 14 | anti_autocorr_floatflow_evenness_20 | 0.0336 | 0.46 | 2.602 | PASS |  | candidate | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * rank(ts_mean(amount / circ_market_cap, 20) / ts_max(amount / circ_market_cap, 20))` |
| 15 | anti_autocorr_turnover_growth_stability_20 | 0.0398 | 0.456 | 3.198 | PASS | PASS | revert | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * rank(-ts_std(pct_change(turnover_rate, 5), 20))` |
| 16 | anti_autocorr_weekly_turnover_corridor_20 | 0.0379 | 0.454 | 3.641 | PASS |  | candidate | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(ts_sum(turnover_rate,5),20)/ts_max(ts_sum(turnover_rate,5),20))` |
| 17 | anti_autocorr_weekly_turnover_corridor_20 | 0.0379 | 0.454 | 3.641 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(ts_sum(turnover_rate,5),20)/ts_max(ts_sum(turnover_rate,5),20))` |
| 18 | anti_autocorr_weekly_turnover_corridor_20 | 0.0379 | 0.454 | 3.641 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(ts_sum(turnover_rate,5),20)/ts_max(ts_sum(turnover_rate,5),20))` |
| 19 | anti_autocorr_weekly_float_corridor_20 | 0.0378 | 0.452 | 3.54 | PASS | PASS | revert | `rank(-ts_corr(ret_1d,shift(ret_1d,1),20))*rank(ts_min(ts_sum(amount/circ_market_cap,5),20)/ts_max(ts_sum(amount/circ_market_cap,5),20))` |
| 20 | anti_autocorr_amtflow_smooth_20 | 0.0434 | 0.45 | 2.914 | PASS | PASS | revert | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * rank(-ts_mean(abs(pct_change(amount, 5)), 20))` |

## Session Summary

| Session | Date | Runs | Gates Pass | OOS Pass | Candidates | Reverts |
|---|---:|---:|---:|---:|---:|---:|
| 20260427_060002_ded4b4 | 2026-04-27 | 23 | 15 | 3 | 12 | 11 |
| 20260427_140002_cda48d | 2026-04-27 | 29 | 20 | 2 | 17 | 12 |
| 20260428_060002_dc8b1a | 2026-04-28 | 25 | 20 | 3 | 17 | 8 |
| 20260428_140002_78c607 | 2026-04-28 | 24 | 21 | 3 | 18 | 6 |
| 20260429_060001_1474fc | 2026-04-29 | 24 | 13 | 3 | 10 | 14 |
| 20260429_140002_505112 | 2026-04-29 | 25 | 13 | 3 | 10 | 15 |

## Earlier Journal Sessions

| Session | Timestamp | Experiments | OOS | Passing factors |
|---|---|---:|---|---|
| 20260417_060002_0396c3 | 2026-04-17 06:56 | 27 | 3/3 | float_quiet_absorb_20, amount_compress_absorption_20_60, log_amount_change_stability_20 |
| 20260417_140002_bdb741 | 2026-04-17 14:56 | 22 | 3/3 | autocorr_smooth_amount_20, turnover_flow_smooth_20, turnover_stability_20 |
| 20260420_060002_e676ef | 2026-04-20 06:57 | 20 | 3/3 | autocorr_smooth_turnover_20, turnover_stability_20, anti_autocorr_amount_compress_20_60 |
| 20260420_140003_bf56e4 | 2026-04-20 14:55 | 22 | 3/3 | autocorr_smooth_turnover_20, turnover_stability_20, autocorr_amount_floor_20 |
| 20260421_060002_c0a60a | 2026-04-21 06:57 | 22 | 3/3 | quiet_trend_turnover_regime_20_120, smooth_turnover_flow_20, turnover_stability_20 |
| 20260421_140002_0b99f7 | 2026-04-21 14:57 | 23 | 3/3 | quiet_weekly_amount_flow_20, smooth_turnover_flow_20, weekly_amount_even_turnover |
| 20260422_060001_ddb78d | 2026-04-22 06:57 | 19 | 2/3 | anti_autocorr_amount_corridor_20, smooth_turnover_flow_20 |
| 20260422_140002_107fce | 2026-04-22 14:57 | 21 | 3/3 | steady_flow_underreaction_20, anti_autocorr_amount_corridor_20, float_even_absorption_20 |
| 20260423_060002_a3c36a | 2026-04-23 06:55 | 19 | 2/3 | float_even_absorption_20, anti_autocorr_amtflow_smooth_20 |
| 20260423_140002_85b7e5 | 2026-04-23 14:57 | 19 | 3/3 | anti_autocorr_float_value_stability_20, anti_autocorr_float_flow_smooth_20, turnover_stability_20 |
| 20260424_060002_bcd025 | 2026-04-24 06:55 | 20 | 3/3 | anti_autocorr_amount_corridor_20, anti_autocorr_float_value_stability_20, log_amount_change_stability_20_5 |

## Interpretation

- The dominant CN discoveries are not classic momentum; they are stability/evenness and anti-autocorrelation structures around turnover, amount, and float-normalized flow.
- Many factors pass IS gates but fail final checks or OOS. They should remain research priors until strategy EV converts them into executable paper-trade performance.
- This rollup is research evidence only. It does not override the execution gate or paper-trade EV layer.
