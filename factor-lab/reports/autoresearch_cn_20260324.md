# Factor Lab Session Report

- Session ID: 20260324_220002_38b3bd
- Market: CN
- Budget: 50 experiments
- Experiments run: 39
- Date: 2026-03-24 22:59

## All Experiments

| # | Name | Formula | IC | IC_IR | Sharpe | Gates |
|---|------|---------|-----|-------|--------|-------|
| 1 | money_flow_persistence_20 | `rank(ts_count(net_mf_amount, 20) / 20)` | 0.0000 | 0.000 | 0.000 | FAIL |
| 2 | turnover_stability_20 | `rank(-ts_std(turnover_rate, 20) / (ts_me` | 0.0000 | 0.000 | 0.000 | FAIL |
| 3 | range_volume_decorr_20 | `rank(-ts_corr(high - low, volume, 20))` | 0.0254 | 0.291 | 0.076 | PASS |
| 4 | amihud_illiquidity_20 | `rank(ts_mean(abs(ret_1d) / turnover_rate` | 0.0000 | 0.000 | 0.000 | FAIL |
| 5 | turnover_mean_reversion_ratio | `rank(-ts_mean(turnover_rate, 5) / ts_mea` | 0.0000 | 0.000 | 0.000 | FAIL |
| 6 | oversold_attention_reversal | `rank(-ret_5d) * rank(turnover_rate)` | 0.0000 | 0.000 | 0.000 | FAIL |
| 7 | ret_vol_decorr_20 | `rank(-ts_corr(ret_1d, volume, 20))` | 0.0209 | 0.146 | 0.656 | FAIL |
| 8 | smart_flow_momentum_10 | `rank(decay_linear(net_mf_amount, 10))` | 0.0000 | 0.000 | 0.000 | FAIL |
| 9 | return_skew_lottery_20 | `ts_skew(ret_1d, 20)` | 0.0282 | 0.262 | 2.737 | PASS |
| 10 | close_location_value_20 | `ts_mean((close - low) / clamp(high - low` | 0.0092 | 0.043 | -1.022 | FAIL |
| 11 | panic_reversal_5d | `rank(-ret_5d) * rank(ts_mean(turnover_ra` | 0.0000 | 0.000 | 0.000 | FAIL |
| 12 | return_autocorr_reversal_20 | `rank(ts_corr(ret_1d, shift(ret_1d, 1), 2` | 0.0263 | 0.348 | 3.983 | PASS |
| 13 | price_path_efficiency_20 | `rank(abs(ret_20d) / ts_sum(abs(ret_1d), ` | -0.0031 | -0.029 | -5.002 | FAIL |
| 14 | low_volume_reversal_5d | `rank(-ret_5d) * rank(-volume_ratio)` | 0.0263 | 0.160 | -3.843 | FAIL |
| 15 | vol_surprise_ratio_5_20 | `rank(ts_std(ret_1d, 5) / ts_std(ret_1d, ` | 0.0075 | 0.060 | -4.383 | FAIL |
| 16 | obv_price_divergence_20 | `rank(ts_rank(obv, 20) - ts_rank(close, 2` | 0.0000 | 0.000 | 0.000 | FAIL |
| 17 | risk_adjusted_momentum_20 | `rank(ret_20d / atr_14)` | 0.0000 | 0.000 | 0.000 | FAIL |
| 18 | vol_term_structure_ratio | `ts_std(ret_1d, 10) / ts_std(ret_1d, 60)` | 0.0245 | 0.199 | 1.348 | FAIL |
| 19 | return_kurtosis_lottery_20 | `rank(ts_kurt(ret_1d, 20))` | 0.0092 | 0.119 | -2.948 | FAIL |
| 20 | volume_spike_concentration_20 | `rank(ts_max(volume, 20) / ts_mean(volume` | 0.0312 | 0.353 | 0.237 | PASS |
| 21 | vwap_close_premium_20 | `rank(ts_mean(close / vwap - 1, 20))` | 0.0000 | 0.000 | 0.000 | FAIL |
| 22 | amount_weighted_momentum_20 | `rank(ts_mean(ret_1d * amount, 20) / ts_m` | 0.0000 | 0.000 | 0.000 | FAIL |
| 23 | turnover_vol_ratio_20 | `rank(ts_mean(turnover_rate, 20) / ts_std` | 0.0000 | 0.000 | 0.000 | FAIL |
| 24 | volume_confirmed_momentum_20 | `rank(ts_corr(ret_1d, turnover_rate, 20))` | 0.0000 | 0.000 | 0.000 | FAIL |
| 25 | skew_volume_accumulation_20 | `rank(ts_skew(ret_1d, 20)) * rank(pct_cha` | 0.0000 | 0.000 | 0.000 | FAIL |
| 26 | return_concentration_ratio_20 | `rank(ts_max(abs(ret_1d), 20) / ts_sum(ab` | 0.0113 | 0.163 | -2.117 | FAIL |
| 27 | atr_mean_reversion_20 | `rank(-(close - ts_mean(close, 20)) / atr` | 0.0000 | 0.000 | 0.000 | FAIL |
| 28 | amount_return_divergence_20 | `rank(-ts_corr(ret_1d, amount, 20))` | 0.0000 | 0.000 | 0.000 | FAIL |
| 29 | volume_return_sign_corr_20 | `rank(ts_corr(sign(ret_1d), volume, 20))` | 0.0229 | 0.151 | 0.935 | FAIL |
| 30 | value_momentum_interaction | `rank(-pb) * rank(ret_20d)` | 0.0000 | 0.000 | 0.000 | FAIL |
| 31 | downside_volume_ratio_20 | `rank(ts_mean(if_then(ret_1d < 0, volume,` | 0.0000 | 0.000 | 0.000 | FAIL |
| 32 | volume_attention_trend_10 | `rank(delta(ts_mean(volume, 5), 10))` | 0.0346 | 0.269 | 1.369 | PASS |
| 33 | volume_accumulation_reversal_10 | `rank(delta(ts_mean(volume, 5), 10)) * ra` | -0.0116 | -0.057 | -0.277 | FAIL |
| 34 | volume_skew_reversal_20 | `ts_skew(volume, 20)` | 0.0163 | 0.187 | -1.381 | FAIL |
| 35 | volume_accumulation_dip_buying | `rank(delta(ts_mean(volume, 5), 5)) * ran` | 0.0017 | 0.014 | -3.083 | FAIL |
| 36 | vol_compression_ratio_5_60 | `rank(ts_std(ret_1d, 5) / ts_std(ret_1d, ` | 0.0143 | 0.108 | -3.152 | FAIL |
| 37 | turnover_attention_decay_10 | `rank(decay_linear(delta(ts_mean(turnover` | 0.0000 | 0.000 | 0.000 | FAIL |
| 38 | volume_attention_acceleration_5 | `rank(delta(ts_mean(volume_ratio, 5), 5))` | -0.0126 | -0.109 | -3.995 | FAIL |
| 39 | lottery_attention_interaction | `rank(ts_skew(ret_1d, 20)) * rank(delta(t` | 0.0374 | 0.303 | 2.856 | PASS |

## OOS Results (Top 3 by IS IC)

| Name | Formula | IS IC | OOS |
|------|---------|-------|-----|
| lottery_attention_interaction | `rank(ts_skew(ret_1d, 20)) * rank(delta(ts_mean(vol` | 0.0374 | PASS |
| volume_attention_trend_10 | `rank(delta(ts_mean(volume, 5), 10))` | 0.0346 | PASS |
| volume_spike_concentration_20 | `rank(ts_max(volume, 20) / ts_mean(volume, 20))` | 0.0312 | PASS |

## Summary

- Factors passing OOS: 3/3
- Candidates for promotion:
  - lottery_attention_interaction (id=6d5eefa7b7b3): `rank(ts_skew(ret_1d, 20)) * rank(delta(ts_mean(volume, 10), 5))`
  - volume_attention_trend_10 (id=e73f9fdfca7b): `rank(delta(ts_mean(volume, 5), 10))`
  - volume_spike_concentration_20 (id=60e9700ed44b): `rank(ts_max(volume, 20) / ts_mean(volume, 20))`

---
*Generated by Factor Lab agent loop.*