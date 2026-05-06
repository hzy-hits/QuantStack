/// Momentum risk — CPT + Beta-Binomial (Axioms 1 + 2)
///
/// Identical logic to US pipeline:
///   regime (autocorrelation) x vol_bucket (tercile) -> 9-cell CPT
///   Each cell: Beta-Binomial posterior -> P(5D return > 0 | cell)
///
/// This module is market-agnostic — works on any price series.
use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use serde_json::json;
use tracing::{info, warn};

use super::bayes::BetaBinomial;
use crate::config::Settings;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Regime {
    Trending,
    MeanReverting,
    Noisy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VolBucket {
    Low,
    Mid,
    High,
}

/// Key for the 9-cell CPT (regime x vol_bucket).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CellKey {
    regime: Regime,
    vol_bucket: VolBucket,
}

/// Per-stock intermediate result before CPT lookup.
struct StockFeatures {
    ts_code: String,
    regime: Regime,
    vol_ratio: f64,
    log_features: LogFeatures,
    // These are set after cross-sectional tercile assignment:
    vol_bucket: Option<VolBucket>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LogFeatures {
    log_return_1d_pct: Option<f64>,
    log_return_5d_pct: Option<f64>,
    log_return_20d_pct: Option<f64>,
    log_trend_slope_20d_pct: Option<f64>,
    denoised_log_slope_10d_pct: Option<f64>,
    log_return_vol_norm_20d: Option<f64>,
    denoise_residual_zscore: Option<f64>,
    fft_low_freq_power: Option<f64>,
    fft_high_freq_power: Option<f64>,
    fft_signal_to_noise: Option<f64>,
    haar_trend_energy: Option<f64>,
    haar_noise_energy: Option<f64>,
    log_feature_window: f64,
}

// ---------------------------------------------------------------------------
// Classification helpers
// ---------------------------------------------------------------------------

/// Classify regime from lag-1 autocorrelation of returns.
pub fn classify_regime(autocorr: f64) -> Regime {
    if autocorr > 0.15 {
        Regime::Trending
    } else if autocorr < -0.10 {
        Regime::MeanReverting
    } else {
        Regime::Noisy
    }
}

/// Classify vol bucket from current vol ratio (today's vol / 20d avg).
/// Tercile thresholds computed cross-sectionally.
pub fn classify_vol_bucket(vol_ratio: f64, tercile_low: f64, tercile_high: f64) -> VolBucket {
    if vol_ratio <= tercile_low {
        VolBucket::Low
    } else if vol_ratio >= tercile_high {
        VolBucket::High
    } else {
        VolBucket::Mid
    }
}

// ---------------------------------------------------------------------------
// Math helpers
// ---------------------------------------------------------------------------

/// Lag-1 autocorrelation of a slice.  Returns 0.0 for slices shorter than 3.
fn lag1_autocorrelation(series: &[f64]) -> f64 {
    let n = series.len();
    if n < 3 {
        return 0.0;
    }
    let mean: f64 = series.iter().sum::<f64>() / n as f64;
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..n {
        let d = series[i] - mean;
        den += d * d;
        if i > 0 {
            num += (series[i] - mean) * (series[i - 1] - mean);
        }
    }
    if den.abs() < 1e-15 {
        return 0.0;
    }
    num / den
}

/// Compute daily log-returns from a close-price series.
fn log_returns(closes: &[f64]) -> Vec<f64> {
    closes.windows(2).map(|w| (w[1] / w[0]).ln()).collect()
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn stddev(values: &[f64]) -> Option<f64> {
    let mu = mean(values)?;
    let var = values
        .iter()
        .map(|v| {
            let d = *v - mu;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    let sigma = var.sqrt();
    if sigma.is_finite() && sigma > 1e-12 {
        Some(sigma)
    } else {
        None
    }
}

fn tail_slope(values: &[f64], window: usize) -> Option<f64> {
    if values.len() < window || window < 2 {
        return None;
    }
    let y = &values[values.len() - window..];
    if y.iter().any(|v| !v.is_finite()) {
        return None;
    }
    let x_mean = (window as f64 - 1.0) / 2.0;
    let y_mean = mean(y)?;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, value) in y.iter().enumerate() {
        let dx = i as f64 - x_mean;
        num += dx * (*value - y_mean);
        den += dx * dx;
    }
    if den <= 0.0 {
        None
    } else {
        Some(num / den)
    }
}

fn ema(values: &[f64], span: usize) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }
    let alpha = 2.0 / (span as f64 + 1.0);
    let mut out = Vec::with_capacity(values.len());
    let mut prev = values[0];
    out.push(prev);
    for value in values.iter().skip(1) {
        prev = alpha * *value + (1.0 - alpha) * prev;
        out.push(prev);
    }
    out
}

fn log_return_pct(log_prices: &[f64], days: usize) -> Option<f64> {
    if log_prices.len() <= days {
        return None;
    }
    Some((log_prices[log_prices.len() - 1] - log_prices[log_prices.len() - 1 - days]) * 100.0)
}

fn spectral_energy(log_rets: &[f64], window: usize) -> (Option<f64>, Option<f64>, Option<f64>) {
    if log_rets.len() < 8 {
        return (None, None, None);
    }
    let n = window.min(log_rets.len());
    if n < 8 {
        return (None, None, None);
    }
    let mut tail = log_rets[log_rets.len() - n..].to_vec();
    if tail.iter().any(|v| !v.is_finite()) {
        return (None, None, None);
    }
    let mu = mean(&tail).unwrap_or(0.0);
    for value in &mut tail {
        *value -= mu;
    }

    let mut powers: Vec<f64> = Vec::new();
    for k in 1..=(n / 2) {
        let mut re = 0.0;
        let mut im = 0.0;
        for (t, value) in tail.iter().enumerate() {
            let angle = -2.0 * std::f64::consts::PI * k as f64 * t as f64 / n as f64;
            re += *value * angle.cos();
            im += *value * angle.sin();
        }
        powers.push(re * re + im * im);
    }
    if powers.is_empty() {
        return (None, None, None);
    }
    let total = powers.iter().sum::<f64>();
    if total <= 1e-18 {
        return (Some(0.0), Some(0.0), None);
    }
    let split = (powers.len() / 3).max(1);
    let low = powers[..split].iter().sum::<f64>();
    let high = powers[split..].iter().sum::<f64>();
    (
        Some(low / total),
        Some(high / total),
        Some(low / (high + 1e-12)),
    )
}

fn haar_energy(log_rets: &[f64], window: usize) -> (Option<f64>, Option<f64>) {
    if log_rets.len() < 8 {
        return (None, None);
    }
    let mut n = window.min(log_rets.len());
    n = 1usize << ((usize::BITS - 1 - n.leading_zeros()) as usize);
    if n < 8 {
        return (None, None);
    }
    let mut cur = log_rets[log_rets.len() - n..].to_vec();
    if cur.iter().any(|v| !v.is_finite()) {
        return (None, None);
    }
    let mu = mean(&cur).unwrap_or(0.0);
    for value in &mut cur {
        *value -= mu;
    }
    let total = cur.iter().map(|v| v * v).sum::<f64>();
    if total <= 1e-18 {
        return (Some(0.0), Some(0.0));
    }

    let inv_sqrt2 = 1.0 / 2.0_f64.sqrt();
    let mut detail_energies: Vec<f64> = Vec::new();
    while cur.len() >= 2 {
        let mut next = Vec::with_capacity(cur.len() / 2);
        let mut detail_energy = 0.0;
        for pair in cur.chunks_exact(2) {
            let avg = (pair[0] + pair[1]) * inv_sqrt2;
            let detail = (pair[0] - pair[1]) * inv_sqrt2;
            next.push(avg);
            detail_energy += detail * detail;
        }
        detail_energies.push(detail_energy);
        cur = next;
    }
    let noise = detail_energies.iter().take(2).sum::<f64>();
    let trend = detail_energies.iter().skip(2).sum::<f64>();
    (Some(trend / total), Some(noise / total))
}

fn compute_log_features(closes: &[f64]) -> LogFeatures {
    if closes.len() < 2 {
        return LogFeatures::default();
    }
    let log_prices: Vec<f64> = closes.iter().map(|close| (*close).max(1e-9).ln()).collect();
    let log_rets = log_returns(closes);
    let ema_log = ema(&log_prices, 5);
    let residuals: Vec<f64> = log_prices
        .iter()
        .zip(ema_log.iter())
        .map(|(price, smooth)| price - smooth)
        .collect();
    let residual_z = if residuals.len() >= 20 {
        stddev(&residuals[residuals.len() - 20..])
            .map(|sigma| residuals[residuals.len() - 1] / sigma)
    } else {
        None
    };
    let vol_norm = if log_rets.len() >= 20 {
        stddev(&log_rets[log_rets.len() - 20..]).map(|sigma| log_rets[log_rets.len() - 1] / sigma)
    } else {
        None
    };
    let (fft_low, fft_high, fft_snr) = spectral_energy(&log_rets, 32);
    let (haar_trend, haar_noise) = haar_energy(&log_rets, 32);

    LogFeatures {
        log_return_1d_pct: log_return_pct(&log_prices, 1),
        log_return_5d_pct: log_return_pct(&log_prices, 5),
        log_return_20d_pct: log_return_pct(&log_prices, 20),
        log_trend_slope_20d_pct: tail_slope(&log_prices, 20).map(|v| v * 100.0),
        denoised_log_slope_10d_pct: tail_slope(&ema_log, 10).map(|v| v * 100.0),
        log_return_vol_norm_20d: vol_norm,
        denoise_residual_zscore: residual_z,
        fft_low_freq_power: fft_low,
        fft_high_freq_power: fft_high,
        fft_signal_to_noise: fft_snr,
        haar_trend_energy: haar_trend,
        haar_noise_energy: haar_noise,
        log_feature_window: 32usize.min(log_rets.len()) as f64,
    }
}

/// Compute 5-day forward log-return starting at index `i`.
/// Returns `None` if there are not enough bars ahead.
fn forward_return_5d(closes: &[f64], i: usize) -> Option<f64> {
    if i + 5 >= closes.len() {
        return None;
    }
    Some((closes[i + 5] / closes[i]).ln())
}

/// Compute cross-sectional tercile thresholds (33rd and 67th percentile).
fn terciles(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (1.0, 1.0);
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let lo_idx = n / 3;
    let hi_idx = 2 * n / 3;
    (sorted[lo_idx], sorted[hi_idx])
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let fwd_horizon = cfg.signals.momentum_windows.first().copied().unwrap_or(5) as usize;
    let date_str = as_of.to_string();

    // ------------------------------------------------------------------
    // 1. Query price history
    // ------------------------------------------------------------------
    let mut stmt = db.prepare(
        "SELECT ts_code, CAST(trade_date AS VARCHAR) AS trade_date, close, vol
         FROM prices
         WHERE trade_date <= ?
         ORDER BY ts_code, trade_date",
    )?;

    // Collect rows grouped by ts_code.
    // Each entry: (trade_date_str, close, vol)
    let mut stock_map: HashMap<String, Vec<(String, f64, f64)>> = HashMap::new();

    let rows = stmt.query_map([&date_str], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<f64>>(3)?,
        ))
    })?;

    for r in rows {
        let (ts_code, trade_date, close_opt, vol_opt) = r?;
        // Skip rows with missing close or vol
        if let (Some(close), Some(vol)) = (close_opt, vol_opt) {
            if close > 0.0 {
                stock_map
                    .entry(ts_code)
                    .or_default()
                    .push((trade_date, close, vol));
            }
        }
    }

    info!(symbols = stock_map.len(), "momentum: loaded price history");

    // ------------------------------------------------------------------
    // 2. Per-stock: compute regime and vol_ratio
    // ------------------------------------------------------------------
    let mut features: Vec<StockFeatures> = Vec::new();

    // We also accumulate the CPT: for each cell, record (wins, losses) from
    // historical 5D forward returns.  We use the ENTIRE cross-section history
    // to build the CPT, not just today's classification.
    //
    // Approach: for each stock's history, at each bar t (where we have 20
    // trailing bars and `fwd_horizon` forward bars), compute the cell and
    // the outcome.  Accumulate into a global CPT map.
    // EWMA-weighted CPT: recent observations weighted more than distant ones.
    // Half-life of 120 trading days (~6 months): an observation 120 days ago
    // has half the weight of today's observation.
    const EWMA_HALF_LIFE: f64 = 30.0; // ~6 weeks: effective n ≈ 40-80, responsive to recent regime
    let decay = (0.5_f64).ln() / EWMA_HALF_LIFE; // negative

    let mut cpt_counts: HashMap<CellKey, (f64, f64)> = HashMap::new();

    // We need cross-sectional vol_ratio terciles for today.  First pass:
    // collect today's vol_ratio per stock, then compute terciles.
    // Meanwhile, also build historical CPT with a *per-snapshot* approach:
    // since computing true cross-sectional terciles for every historical date
    // is expensive, we use a simpler heuristic for the historical CPT —
    // per-stock vol_ratio terciles over the stock's own history.
    //
    // For today's classification we use true cross-sectional terciles.

    const MIN_BARS: usize = 10; // minimum bars for feature classification
    const MIN_HISTORY: usize = 120; // minimum bars for CPT training contribution

    for (ts_code, bars) in &stock_map {
        let n = bars.len();
        if n < MIN_BARS {
            continue;
        }

        let closes: Vec<f64> = bars.iter().map(|b| b.1).collect();
        let vols: Vec<f64> = bars.iter().map(|b| b.2).collect();
        let log_rets = log_returns(&closes);

        // -- Today's features (use last 20 bars) --
        let tail_rets = if log_rets.len() >= MIN_BARS {
            &log_rets[log_rets.len() - MIN_BARS..]
        } else {
            &log_rets[..]
        };
        let autocorr = lag1_autocorrelation(tail_rets);
        let regime = classify_regime(autocorr);

        // Vol ratio: today's vol / 20-day average vol
        let recent_vols = if vols.len() >= MIN_BARS {
            &vols[vols.len() - MIN_BARS..]
        } else {
            &vols[..]
        };
        let avg_vol: f64 = recent_vols.iter().sum::<f64>() / recent_vols.len() as f64;
        let today_vol = *vols.last().unwrap();
        let vol_ratio = if avg_vol > 0.0 {
            today_vol / avg_vol
        } else {
            1.0
        };

        features.push(StockFeatures {
            ts_code: ts_code.clone(),
            regime,
            vol_ratio,
            log_features: compute_log_features(&closes),
            vol_bucket: None, // filled in after tercile computation
        });

        // -- Historical CPT contribution --
        // Only if we have enough history for meaningful statistics.
        if n < MIN_HISTORY {
            continue;
        }

        // Collect vol_ratios for the stock's own history to assign per-stock
        // vol buckets (for historical CPT building).
        let mut hist_vol_ratios: Vec<f64> = Vec::new();
        for t in MIN_BARS..n {
            let vol_window = &vols[t - MIN_BARS..t];
            let avg_v: f64 = vol_window.iter().sum::<f64>() / MIN_BARS as f64;
            if avg_v > 0.0 {
                hist_vol_ratios.push(vols[t] / avg_v);
            } else {
                hist_vol_ratios.push(1.0);
            }
        }
        let (vr_lo, vr_hi) = terciles(&hist_vol_ratios);

        // Now iterate again and accumulate EWMA-weighted CPT counts.
        // Weight = exp(decay * age), where age = distance from most recent bar.
        let last_bar_idx = n - 1;
        for t in MIN_BARS..n {
            // Need fwd_horizon bars ahead of t for outcome.
            if t + fwd_horizon >= n {
                break;
            }

            // Regime at bar t: lag-1 autocorrelation over [t-20..t]
            let ret_window = &log_rets[t - MIN_BARS..t];
            let ac = lag1_autocorrelation(ret_window);
            let reg = classify_regime(ac);

            // Vol bucket at bar t
            let vr_idx = t - MIN_BARS; // index into hist_vol_ratios
            let vb = classify_vol_bucket(hist_vol_ratios[vr_idx], vr_lo, vr_hi);

            // 5D forward return outcome
            let fwd = (closes[t + fwd_horizon] / closes[t]).ln();
            let win = fwd > 0.0;

            // EWMA weight: recent observations count more
            let age = (last_bar_idx - t - fwd_horizon) as f64; // age in bars from most recent outcome
            let weight = (decay * age).exp(); // 1.0 for most recent, 0.5 at half-life

            let key = CellKey {
                regime: reg,
                vol_bucket: vb,
            };
            let entry = cpt_counts.entry(key).or_insert((0.0, 0.0));
            if win {
                entry.0 += weight;
            } else {
                entry.1 += weight;
            }
        }
    }

    // ------------------------------------------------------------------
    // 3. Cross-sectional terciles for today's vol_ratio
    // ------------------------------------------------------------------
    let today_vol_ratios: Vec<f64> = features.iter().map(|f| f.vol_ratio).collect();
    let (tercile_low, tercile_high) = terciles(&today_vol_ratios);

    for feat in &mut features {
        feat.vol_bucket = Some(classify_vol_bucket(
            feat.vol_ratio,
            tercile_low,
            tercile_high,
        ));
    }

    info!(
        cells = cpt_counts.len(),
        tercile_low = format!("{:.3}", tercile_low),
        tercile_high = format!("{:.3}", tercile_high),
        "momentum: CPT built"
    );

    // ------------------------------------------------------------------
    // 4. Beta-Binomial update per cell → posterior for each stock
    // ------------------------------------------------------------------
    let bb = BetaBinomial::new();
    let as_of_str = as_of.to_string();
    let mut rows_written: usize = 0;

    // Prepare INSERT statement
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO analytics
            (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, 'momentum', ?, ?, ?)",
    )?;

    for feat in &features {
        let vol_bucket = match feat.vol_bucket {
            Some(vb) => vb,
            None => continue,
        };

        let key = CellKey {
            regime: feat.regime,
            vol_bucket,
        };
        let (wins, losses) = cpt_counts.get(&key).copied().unwrap_or((0.0, 0.0));

        let posterior = bb.update_weighted(wins, losses);

        let regime_label = match feat.regime {
            Regime::Trending => "trending",
            Regime::MeanReverting => "mean_reverting",
            Regime::Noisy => "noisy",
        };
        let vol_label = match vol_bucket {
            VolBucket::Low => "low",
            VolBucket::Mid => "mid",
            VolBucket::High => "high",
        };
        let regime_f64 = match feat.regime {
            Regime::Trending => 0.0,
            Regime::MeanReverting => 1.0,
            Regime::Noisy => 2.0,
        };
        let vol_bucket_f64 = match vol_bucket {
            VolBucket::Low => 0.0,
            VolBucket::Mid => 1.0,
            VolBucket::High => 2.0,
        };

        let detail = json!({
            "regime": regime_label,
            "vol_bucket": vol_label,
            "ewma_wins": wins,
            "ewma_losses": losses,
            "half_life": EWMA_HALF_LIFE as u32,
            "ci_low": posterior.ci_low,
            "ci_high": posterior.ci_high,
            "log_return_1d_pct": feat.log_features.log_return_1d_pct,
            "log_return_5d_pct": feat.log_features.log_return_5d_pct,
            "log_return_20d_pct": feat.log_features.log_return_20d_pct,
            "log_trend_slope_20d_pct": feat.log_features.log_trend_slope_20d_pct,
            "denoised_log_slope_10d_pct": feat.log_features.denoised_log_slope_10d_pct,
            "log_return_vol_norm_20d": feat.log_features.log_return_vol_norm_20d,
            "denoise_residual_zscore": feat.log_features.denoise_residual_zscore,
            "fft_low_freq_power": feat.log_features.fft_low_freq_power,
            "fft_high_freq_power": feat.log_features.fft_high_freq_power,
            "fft_signal_to_noise": feat.log_features.fft_signal_to_noise,
            "haar_trend_energy": feat.log_features.haar_trend_energy,
            "haar_noise_energy": feat.log_features.haar_noise_energy,
            "log_feature_window": feat.log_features.log_feature_window,
        })
        .to_string();

        let mut metrics: Vec<(&str, f64, &str)> = vec![
            ("trend_prob", posterior.mean, &detail),
            ("trend_prob_ci_low", posterior.ci_low, &detail),
            ("trend_prob_ci_high", posterior.ci_high, &detail),
            ("trend_prob_n", posterior.n as f64, &detail),
            ("regime", regime_f64, regime_label),
            ("vol_bucket", vol_bucket_f64, vol_label),
        ];
        if let Some(v) = feat.log_features.log_return_1d_pct {
            metrics.push(("log_return_1d_pct", v, &detail));
        }
        if let Some(v) = feat.log_features.log_return_5d_pct {
            metrics.push(("log_return_5d_pct", v, &detail));
        }
        if let Some(v) = feat.log_features.log_return_20d_pct {
            metrics.push(("log_return_20d_pct", v, &detail));
        }
        if let Some(v) = feat.log_features.log_trend_slope_20d_pct {
            metrics.push(("log_trend_slope_20d_pct", v, &detail));
        }
        if let Some(v) = feat.log_features.denoised_log_slope_10d_pct {
            metrics.push(("denoised_log_slope_10d_pct", v, &detail));
        }
        if let Some(v) = feat.log_features.log_return_vol_norm_20d {
            metrics.push(("log_return_vol_norm_20d", v, &detail));
        }
        if let Some(v) = feat.log_features.denoise_residual_zscore {
            metrics.push(("denoise_residual_zscore", v, &detail));
        }
        if let Some(v) = feat.log_features.fft_low_freq_power {
            metrics.push(("fft_low_freq_power", v, &detail));
        }
        if let Some(v) = feat.log_features.fft_high_freq_power {
            metrics.push(("fft_high_freq_power", v, &detail));
        }
        if let Some(v) = feat.log_features.fft_signal_to_noise {
            metrics.push(("fft_signal_to_noise", v, &detail));
        }
        if let Some(v) = feat.log_features.haar_trend_energy {
            metrics.push(("haar_trend_energy", v, &detail));
        }
        if let Some(v) = feat.log_features.haar_noise_energy {
            metrics.push(("haar_noise_energy", v, &detail));
        }
        metrics.push((
            "log_feature_window",
            feat.log_features.log_feature_window,
            &detail,
        ));

        for (metric, value, det) in &metrics {
            insert_stmt.execute(duckdb::params![feat.ts_code, as_of_str, metric, value, det,])?;
            rows_written += 1;
        }
    }

    if rows_written == 0 {
        warn!("momentum: no rows written (insufficient data?)");
    }

    info!(
        rows = rows_written,
        symbols = features.len(),
        windows = ?cfg.signals.momentum_windows,
        "momentum CPT computed"
    );
    Ok(rows_written)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_regime() {
        assert!(matches!(classify_regime(0.20), Regime::Trending));
        assert!(matches!(classify_regime(-0.15), Regime::MeanReverting));
        assert!(matches!(classify_regime(0.05), Regime::Noisy));
        // Boundary cases
        assert!(matches!(classify_regime(0.15), Regime::Noisy)); // not strictly >
        assert!(matches!(classify_regime(-0.10), Regime::Noisy)); // not strictly <
    }

    #[test]
    fn test_classify_vol_bucket() {
        assert!(matches!(classify_vol_bucket(0.5, 0.8, 1.2), VolBucket::Low));
        assert!(matches!(classify_vol_bucket(1.0, 0.8, 1.2), VolBucket::Mid));
        assert!(matches!(
            classify_vol_bucket(1.5, 0.8, 1.2),
            VolBucket::High
        ));
    }

    #[test]
    fn test_lag1_autocorrelation() {
        // Perfect positive autocorrelation: monotonically increasing
        let mono: Vec<f64> = (0..30).map(|i| i as f64).collect();
        let ac = lag1_autocorrelation(&mono);
        assert!(ac > 0.85, "monotonic should have high autocorr, got {ac}");

        // Short series
        assert_eq!(lag1_autocorrelation(&[1.0, 2.0]), 0.0);
        assert_eq!(lag1_autocorrelation(&[]), 0.0);

        // Alternating series: strong negative autocorrelation
        let alt: Vec<f64> = (0..30)
            .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
            .collect();
        let ac_alt = lag1_autocorrelation(&alt);
        assert!(
            ac_alt < -0.85,
            "alternating should be strongly negative, got {ac_alt}"
        );
    }

    #[test]
    fn test_log_returns() {
        let closes = vec![100.0, 105.0, 110.25];
        let rets = log_returns(&closes);
        assert_eq!(rets.len(), 2);
        assert!((rets[0] - (1.05f64).ln()).abs() < 1e-10);
    }

    #[test]
    fn test_compute_log_features_percentage_scale() {
        let closes: Vec<f64> = (0..40).map(|i| 100.0 * 1.01f64.powi(i)).collect();
        let features = compute_log_features(&closes);
        assert!((features.log_return_1d_pct.unwrap() - 1.01f64.ln() * 100.0).abs() < 1e-10);
        assert!((features.log_return_20d_pct.unwrap() - 1.01f64.ln() * 20.0 * 100.0).abs() < 1e-10);
        assert!(features.denoised_log_slope_10d_pct.unwrap() > 0.0);
        assert!(features.fft_low_freq_power.is_some());
        assert_eq!(features.log_feature_window, 32.0);
    }

    #[test]
    fn test_compute_log_features_chop_noise() {
        let closes: Vec<f64> = (0..40)
            .map(|i| if i % 2 == 0 { 102.0 } else { 98.0 })
            .collect();
        let features = compute_log_features(&closes);
        assert!(features.haar_noise_energy.unwrap() > 0.5);
    }

    #[test]
    fn test_forward_return_5d() {
        let closes = vec![100.0, 101.0, 102.0, 103.0, 104.0, 110.0, 115.0];
        let fwd = forward_return_5d(&closes, 0);
        assert!(fwd.is_some());
        assert!((fwd.unwrap() - (110.0f64 / 100.0).ln()).abs() < 1e-10);

        // Not enough bars
        assert!(forward_return_5d(&closes, 3).is_none());
    }

    #[test]
    fn test_terciles() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let (lo, hi) = terciles(&vals);
        assert_eq!(lo, 4.0); // vals[3]
        assert_eq!(hi, 7.0); // vals[6]
    }

    #[test]
    fn test_terciles_empty() {
        let (lo, hi) = terciles(&[]);
        assert_eq!(lo, 1.0);
        assert_eq!(hi, 1.0);
    }
}
