/// Volatility HMM — 2-state Gaussian HMM on log-variance.
///
/// Complements the return-HMM (hmm.rs) by detecting volatility regimes:
///   State 0 (low_vol): market in quiet/calm regime
///   State 1 (high_vol): market in stressed/turbulent regime
///
/// Uses Garman-Klass log-variance as observation (better Gaussian fit than raw vol).
/// Outputs: P(high_vol), P(high_vol_tomorrow), rv_gk_20d, vol_regime_duration.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::{info, warn};

use crate::config::Settings;
use super::rv;

const MODULE: &str = "vol_hmm";
const MARKET_CODE: &str = "_MARKET";
const MAX_ITER: usize = 100;
const CONVERGENCE_EPS: f64 = 1e-6;

/// 2-state Gaussian HMM parameters (reused structure from hmm.rs)
struct GaussianHMM {
    pi: [f64; 2],
    trans: [[f64; 2]; 2],
    mu: [f64; 2],
    sigma: [f64; 2],
}

pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let benchmark = &cfg.universe.benchmark;
    let date_str = as_of.to_string();

    // Query benchmark OHLC (latest 500 bars, DESC then reverse for correct order)
    let mut stmt = db.prepare(
        "SELECT trade_date, open, high, low, close
         FROM prices
         WHERE ts_code = ? AND trade_date <= ?
         ORDER BY trade_date DESC
         LIMIT 500"
    )?;

    let mut data: Vec<(String, f64, f64, f64, f64)> = stmt
        .query_map(duckdb::params![benchmark, date_str], |row| {
            Ok((
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, Option<f64>>(1).unwrap_or(None).unwrap_or(0.0),
                row.get::<_, Option<f64>>(2).unwrap_or(None).unwrap_or(0.0),
                row.get::<_, Option<f64>>(3).unwrap_or(None).unwrap_or(0.0),
                row.get::<_, Option<f64>>(4).unwrap_or(None).unwrap_or(0.0),
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Reverse to chronological order (we queried DESC)
    data.reverse();

    if data.len() < 100 {
        info!(n = data.len(), "insufficient OHLC data for vol_hmm, skipping");
        return Ok(0);
    }

    // Build OHLC bars and log-variance observations
    let bars: Vec<(f64, f64, f64, f64)> = data.iter()
        .map(|(_, o, h, l, c)| (*o, *h, *l, *c))
        .filter(|(o, h, l, c)| *o > 0.0 && *h > 0.0 && *l > 0.0 && *c > 0.0)
        .collect();

    if bars.len() < 100 {
        info!(n = bars.len(), "insufficient valid OHLC bars for vol_hmm");
        return Ok(0);
    }

    let log_vars = rv::log_variance_series(&bars);

    // Also compute current 20-day realized vol (GK)
    let rv_gk_20d = rv::rolling_gk_vol(&bars, 20);

    // Initialize HMM via percentile split on log-variance
    let mut hmm = initialize_percentile(&log_vars);

    // Baum-Welch EM
    let mut prev_ll = f64::NEG_INFINITY;
    let mut converged = false;

    for iter in 0..MAX_ITER {
        let (ll, alpha) = forward(&log_vars, &hmm);
        let beta = backward(&log_vars, &hmm);
        let (gamma, xi) = compute_gamma_xi(&log_vars, &alpha, &beta, &hmm);

        if (ll - prev_ll).abs() < CONVERGENCE_EPS {
            converged = true;
            info!(iterations = iter + 1, log_likelihood = format!("{:.4}", ll), "vol_hmm converged");
            break;
        }
        prev_ll = ll;

        m_step(&mut hmm, &log_vars, &gamma, &xi);
    }

    if !converged {
        warn!("vol_hmm did not converge after {} iterations", MAX_ITER);
    }

    // Ensure state 0 = low_vol (lower mean log-var), state 1 = high_vol
    if hmm.mu[0] > hmm.mu[1] {
        hmm.pi.swap(0, 1);
        hmm.mu.swap(0, 1);
        hmm.sigma.swap(0, 1);
        let t00 = hmm.trans[0][0];
        let t01 = hmm.trans[0][1];
        let t10 = hmm.trans[1][0];
        let t11 = hmm.trans[1][1];
        hmm.trans = [[t11, t10], [t01, t00]];
    }

    // Final forward pass → posterior at T
    let (_, alpha) = forward(&log_vars, &hmm);
    let t = log_vars.len() - 1;
    let alpha_sum: f64 = alpha[t].iter().sum();
    let p_low_vol = if alpha_sum > 0.0 { alpha[t][0] / alpha_sum } else { 0.5 };
    let p_high_vol = 1.0 - p_low_vol;

    // 1-step-ahead: P(high_vol tomorrow)
    let p_high_vol_tomorrow = p_low_vol * hmm.trans[0][1] + p_high_vol * hmm.trans[1][1];

    // Regime duration via Viterbi
    let regime_duration = compute_regime_duration(&log_vars, &hmm);
    let current_regime = if p_high_vol > 0.5 { "high_vol" } else { "low_vol" };

    // Convert log-var means to approximate annualized vol for interpretability
    let vol_state0 = (hmm.mu[0].exp() * 252.0).sqrt() * 100.0; // low_vol regime typical vol
    let vol_state1 = (hmm.mu[1].exp() * 252.0).sqrt() * 100.0; // high_vol regime typical vol

    info!(
        p_high_vol = format!("{:.3}", p_high_vol),
        p_high_vol_tomorrow = format!("{:.3}", p_high_vol_tomorrow),
        rv_gk_20d = format!("{:.2}", rv_gk_20d),
        regime = current_regime,
        duration = regime_duration,
        vol_low = format!("{:.1}%", vol_state0),
        vol_high = format!("{:.1}%", vol_state1),
        n = log_vars.len(),
        "vol_hmm regime computed"
    );

    // Write analytics rows
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let detail = format!(
        r#"{{"benchmark":"{}","n":{},"converged":{},"mu_logvar":[{:.4},{:.4}],"sigma_logvar":[{:.4},{:.4}],"vol_approx":[{:.1},{:.1}],"regime":"{}","duration":{}}}"#,
        benchmark,
        log_vars.len(),
        converged,
        hmm.mu[0], hmm.mu[1],
        hmm.sigma[0], hmm.sigma[1],
        vol_state0, vol_state1,
        current_regime,
        regime_duration,
    );

    // p_high_vol — current probability of being in high-vol regime
    insert.execute(duckdb::params![
        MARKET_CODE, date_str, MODULE, "p_high_vol", p_high_vol, &detail,
    ])?;

    // p_high_vol_tomorrow — 1-step-ahead forecast
    insert.execute(duckdb::params![
        MARKET_CODE, date_str, MODULE, "p_high_vol_tomorrow", p_high_vol_tomorrow, None::<String>,
    ])?;

    // rv_gk_20d — current 20-day realized vol (GK, annualized %)
    insert.execute(duckdb::params![
        MARKET_CODE, date_str, MODULE, "rv_gk_20d", rv_gk_20d, None::<String>,
    ])?;

    // vol_regime_duration
    insert.execute(duckdb::params![
        MARKET_CODE, date_str, MODULE, "vol_regime_duration", regime_duration as f64, None::<String>,
    ])?;

    info!("vol_hmm analytics written");
    Ok(1)
}

/// Initialize via percentile split: bottom 60% = low_vol, top 40% = high_vol.
fn initialize_percentile(observations: &[f64]) -> GaussianHMM {
    let mut sorted = observations.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let split = (sorted.len() as f64 * 0.6) as usize;
    let low: &[f64] = &sorted[..split];
    let high: &[f64] = &sorted[split..];

    let mu_low = low.iter().sum::<f64>() / low.len() as f64;
    let mu_high = high.iter().sum::<f64>() / high.len() as f64;

    let sigma_low = if low.len() > 1 {
        let var = low.iter().map(|x| (x - mu_low).powi(2)).sum::<f64>() / (low.len() - 1) as f64;
        var.sqrt().max(0.01)
    } else { 1.0 };

    let sigma_high = if high.len() > 1 {
        let var = high.iter().map(|x| (x - mu_high).powi(2)).sum::<f64>() / (high.len() - 1) as f64;
        var.sqrt().max(0.01)
    } else { 1.0 };

    GaussianHMM {
        pi: [0.6, 0.4],
        trans: [[0.95, 0.05], [0.10, 0.90]], // low_vol more persistent
        mu: [mu_low, mu_high],
        sigma: [sigma_low, sigma_high],
    }
}

// ════════════════════════════════════════════════════════════════════════════
// HMM algorithms (same as hmm.rs — extracted for independence)
// ════════════════════════════════════════════════════════════════════════════

fn gaussian_pdf(x: f64, mu: f64, sigma: f64) -> f64 {
    let z = (x - mu) / sigma;
    (-0.5 * z * z).exp() / (sigma * (2.0 * std::f64::consts::PI).sqrt())
}

fn forward(observations: &[f64], hmm: &GaussianHMM) -> (f64, Vec<[f64; 2]>) {
    let n = observations.len();
    let mut alpha = vec![[0.0f64; 2]; n];
    let mut log_likelihood = 0.0;

    for k in 0..2 {
        alpha[0][k] = hmm.pi[k] * gaussian_pdf(observations[0], hmm.mu[k], hmm.sigma[k]);
    }
    let scale: f64 = alpha[0].iter().sum();
    if scale > 0.0 {
        for k in 0..2 { alpha[0][k] /= scale; }
        log_likelihood += scale.ln();
    }

    for t in 1..n {
        let mut scale_t = 0.0;
        for j in 0..2 {
            let mut sum = 0.0;
            for i in 0..2 {
                sum += alpha[t - 1][i] * hmm.trans[i][j];
            }
            alpha[t][j] = sum * gaussian_pdf(observations[t], hmm.mu[j], hmm.sigma[j]);
            scale_t += alpha[t][j];
        }
        if scale_t > 0.0 {
            for k in 0..2 { alpha[t][k] /= scale_t; }
            log_likelihood += scale_t.ln();
        }
    }

    (log_likelihood, alpha)
}

fn backward(observations: &[f64], hmm: &GaussianHMM) -> Vec<[f64; 2]> {
    let n = observations.len();
    let mut beta = vec![[0.0f64; 2]; n];
    beta[n - 1] = [1.0, 1.0];

    for t in (0..n - 1).rev() {
        let mut scale_t = 0.0;
        for i in 0..2 {
            let mut sum = 0.0;
            for j in 0..2 {
                sum += hmm.trans[i][j]
                    * gaussian_pdf(observations[t + 1], hmm.mu[j], hmm.sigma[j])
                    * beta[t + 1][j];
            }
            beta[t][i] = sum;
            scale_t += beta[t][i];
        }
        if scale_t > 0.0 {
            for k in 0..2 { beta[t][k] /= scale_t; }
        }
    }

    beta
}

fn compute_gamma_xi(
    observations: &[f64],
    alpha: &[[f64; 2]],
    beta: &[[f64; 2]],
    hmm: &GaussianHMM,
) -> (Vec<[f64; 2]>, Vec<[[f64; 2]; 2]>) {
    let n = observations.len();
    let mut gamma = vec![[0.0f64; 2]; n];
    let mut xi = vec![[[0.0f64; 2]; 2]; n - 1];

    for t in 0..n {
        let mut denom = 0.0;
        for k in 0..2 {
            gamma[t][k] = alpha[t][k] * beta[t][k];
            denom += gamma[t][k];
        }
        if denom > 0.0 {
            for k in 0..2 { gamma[t][k] /= denom; }
        }
    }

    for t in 0..n - 1 {
        let mut denom = 0.0;
        for i in 0..2 {
            for j in 0..2 {
                xi[t][i][j] = alpha[t][i]
                    * hmm.trans[i][j]
                    * gaussian_pdf(observations[t + 1], hmm.mu[j], hmm.sigma[j])
                    * beta[t + 1][j];
                denom += xi[t][i][j];
            }
        }
        if denom > 0.0 {
            for i in 0..2 {
                for j in 0..2 {
                    xi[t][i][j] /= denom;
                }
            }
        }
    }

    (gamma, xi)
}

fn m_step(
    hmm: &mut GaussianHMM,
    observations: &[f64],
    gamma: &[[f64; 2]],
    xi: &[[[f64; 2]; 2]],
) {
    let n = observations.len();

    let g0_sum: f64 = gamma[0].iter().sum();
    if g0_sum > 0.0 {
        for k in 0..2 {
            hmm.pi[k] = (gamma[0][k] / g0_sum).max(1e-10);
        }
    }

    for i in 0..2 {
        let gamma_sum: f64 = gamma[..n - 1].iter().map(|g| g[i]).sum();
        if gamma_sum > 0.0 {
            for j in 0..2 {
                let xi_sum: f64 = xi.iter().map(|x| x[i][j]).sum();
                hmm.trans[i][j] = (xi_sum / gamma_sum).max(1e-10);
            }
            let row_sum: f64 = hmm.trans[i].iter().sum();
            if row_sum > 0.0 {
                for j in 0..2 { hmm.trans[i][j] /= row_sum; }
            }
        }
    }

    for k in 0..2 {
        let gamma_sum: f64 = gamma.iter().map(|g| g[k]).sum();
        if gamma_sum > 1e-10 {
            let weighted_sum: f64 = gamma.iter().zip(observations.iter())
                .map(|(g, &x)| g[k] * x)
                .sum();
            hmm.mu[k] = weighted_sum / gamma_sum;

            let weighted_var: f64 = gamma.iter().zip(observations.iter())
                .map(|(g, &x)| g[k] * (x - hmm.mu[k]).powi(2))
                .sum();
            hmm.sigma[k] = (weighted_var / gamma_sum).sqrt().max(0.01);
        }
    }
}

fn compute_regime_duration(observations: &[f64], hmm: &GaussianHMM) -> u32 {
    let n = observations.len();
    if n == 0 { return 0; }

    let mut viterbi = vec![[0.0f64; 2]; n];
    let mut path = vec![[0usize; 2]; n];

    for k in 0..2 {
        viterbi[0][k] = hmm.pi[k].ln()
            + gaussian_pdf(observations[0], hmm.mu[k], hmm.sigma[k]).max(1e-300).ln();
    }

    for t in 1..n {
        for j in 0..2 {
            let emission = gaussian_pdf(observations[t], hmm.mu[j], hmm.sigma[j]).max(1e-300).ln();
            let (best_i, best_val) = (0..2)
                .map(|i| (i, viterbi[t-1][i] + hmm.trans[i][j].max(1e-300).ln()))
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .unwrap();
            viterbi[t][j] = best_val + emission;
            path[t][j] = best_i;
        }
    }

    let mut states = vec![0usize; n];
    states[n-1] = if viterbi[n-1][0] > viterbi[n-1][1] { 0 } else { 1 };
    for t in (0..n-1).rev() {
        states[t] = path[t+1][states[t+1]];
    }

    let current = states[n-1];
    let mut duration = 0u32;
    for t in (0..n).rev() {
        if states[t] == current {
            duration += 1;
        } else {
            break;
        }
    }

    duration
}
