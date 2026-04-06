# Factor Lab Session Report

- Session ID: 20260330_140002_8edd00
- Market: CN
- Budget: 30 experiments
- Experiments run: 30
- Date: 2026-03-30 14:48

## All Experiments

| # | Name | Formula | IC | IC_IR | Sharpe | Gates |
|---|------|---------|-----|-------|--------|-------|
| 1 | turnover_compression_60 | `rank(-turnover_rate / (ts_mean(turnover_` | 0.0293 | 0.208 | -4.567 | PASS |
| 2 | quiet_value_accumulation | `rank(-pb) * rank(-delta(turnover_rate, 2` | 0.0187 | 0.120 | -3.032 | FAIL |
| 3 | turnover_ret_decorr_20 | `rank(-ts_corr(turnover_rate, ret_1d, 20)` | 0.0206 | 0.142 | -2.506 | FAIL |
| 4 | pe_momentum_20 | `rank(-pct_change(pe_ttm, 20))` | 0.0044 | 0.025 | -3.093 | FAIL |
| 5 | value_lowvol_20 | `rank(-pb) * rank(-ts_std(ret_1d, 20))` | 0.0380 | 0.184 | 0.052 | FAIL |
| 6 | ret5d_turn_decorr_20 | `rank(-ts_corr(ret_5d, turnover_rate, 20)` | 0.0353 | 0.221 | -0.226 | PASS |
| 7 | turnover_platykurt_20 | `rank(-ts_kurt(turnover_rate, 20))` | 0.0062 | 0.079 | -2.280 | FAIL |
| 8 | close_to_high_20 | `rank(ts_mean(close / high, 20))` | 0.0358 | 0.166 | -1.268 | FAIL |
| 9 | close_location_value_10 | `rank(ts_mean((close - low) / (high - low` | 0.0042 | 0.023 | -2.814 | FAIL |
| 10 | turnover_impact_decorr_20 | `rank(-ts_corr(turnover_rate, abs(ret_1d)` | 0.0096 | 0.132 | -0.446 | FAIL |
| 11 | vol_neg_skew_20 | `rank(-ts_skew(volume, 20))` | 0.0163 | 0.188 | -1.381 | FAIL |
| 12 | ret_sign_vol_decorr_20 | `rank(-ts_corr(sign(ret_1d), volume, 20))` | 0.0229 | 0.151 | -2.244 | FAIL |
| 13 | amihud_illiq_20 | `rank(ts_mean(abs(ret_1d) / (amount + 1),` | -0.0127 | -0.107 | -1.151 | FAIL |
| 14 | ret_autocorr_neg_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0263 | 0.348 | 3.282 | PASS |
| 15 | overnight_gap_reversal_20 | `rank(-ts_mean(open / shift(close, 1), 20` | -0.0049 | -0.069 | -4.197 | FAIL |
| 16 | max_ret_lottery_20 | `rank(-ts_max(ret_1d, 20))` | 0.0399 | 0.239 | 0.115 | PASS |
| 17 | signed_volume_pressure_20 | `rank(ts_mean(sign(ret_1d) * volume, 20) ` | 0.0009 | 0.006 | -0.580 | FAIL |
| 18 | variance_ratio_mr_20_________rationale_for_this_specific_proposal_________classic_microstructure_signal____the_variance_ratio_test__lo___mackinlay_1988__is_well_established__if_returns_were_a_random_walk___std_ret_5d___should_scale_as__sqrt_5____std_ret_1d____stocks_where_daily_vol_is_high_relative_to_weekly_vol_have_returns_that_cancel_out___textbook_mean_reversion______independent_from_existing_factors____promoted_factors_are_mostly_price_volume_decorrelation_and_skewness__this_captures_return_autocorrelation__structure__via_volatility_scaling__which_is_orthogonal______low_correlation_with_ret_autocorr_neg_20____that_factor_measures_lag_1_return_correlation_directly__this_measures_multi_period_variance_scaling__related_economic_phenomenon__different_measurement_approach______clean_formula____depth_3__only_53_characters__uses_only__ret_1d__and__ret_5d____minimal_overfitting_risk_ | `rank(ts_std(ret_1d, 20) / (ts_std(ret_5d` | -0.0054 | -0.001 | -0.915 | FAIL |
| 19 | turnover_rank_fade_10 | `rank(-delta(rank(turnover_rate), 10))` | 0.0315 | 0.299 | -6.402 | FAIL |
| 20 | price_path_efficiency_20 | `rank(delta(close, 20) / (ts_sum(high - l` | -0.0149 | -0.080 | -0.553 | FAIL |
| 21 | weekly_autocorr_neg_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 5), ` | -0.0090 | -0.095 | -5.542 | FAIL |
| 22 | amount_uniformity_20 | `rank(-ts_max(amount, 20) / ts_mean(amoun` | 0.0331 | 0.337 | 1.204 | PASS |
| 23 | autocorr_amt_stable_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0414 | 0.496 | 4.103 | PASS |
| 24 | autocorr_turn_stable_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0413 | 0.350 | 2.278 | PASS |
| 25 | autocorr_tight_range_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0442 | 0.345 | 1.062 | PASS |
| 26 | autocorr_cheap_ps_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0080 | 0.078 | -1.525 | FAIL |
| 27 | autocorr_quiet_vol_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0333 | 0.304 | -2.537 | PASS |
| 28 | autocorr_low_turnover_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0370 | 0.277 | -0.187 | PASS |
| 29 | autocorr_lowvol_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0418 | 0.345 | 0.942 | PASS |
| 30 | autocorr_fading_turn_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), ` | 0.0296 | 0.267 | -3.160 | PASS |

## OOS Results (Top 3 by IS IC)

| Name | Formula | IS IC | OOS |
|------|---------|-------|-----|
| autocorr_tight_range_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * ran` | 0.0442 | PASS |
| autocorr_lowvol_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * ran` | 0.0418 | PASS |
| autocorr_amt_stable_20 | `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * ran` | 0.0414 | PASS |

## Summary

- Factors passing OOS: 3/3
- Candidates for promotion:
  - autocorr_tight_range_20 (id=5a4bc06ab855): `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * rank(-ts_mean(high / low - 1, 10))`
  - autocorr_lowvol_20 (id=bc30e5f3b616): `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * rank(-ts_std(ret_1d, 20))`
  - autocorr_amt_stable_20 (id=af870f66bea0): `rank(-ts_corr(ret_1d, shift(ret_1d, 1), 20)) * rank(-ts_std(amount, 20) / ts_mean(amount, 20))`

---
*Generated by Factor Lab agent loop.*