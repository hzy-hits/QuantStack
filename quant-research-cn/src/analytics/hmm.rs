/// HMM regime detection — Axiom 3 (Latent States).
///
/// 2-state Gaussian HMM on benchmark (沪深300) daily returns.
/// State 0 (bull): μ > 0, lower σ
/// State 1 (bear): μ < 0, higher σ
///
/// Outputs: P(bull), P(r > 0 tomorrow), regime_duration, brier_score
///
/// Pure Rust HMM implementation using Baum-Welch EM.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use tracing::{info, warn};

use crate::config::Settings;

const MODULE: &str = "hmm";
const MARKET_CODE: &str = "_MARKET";
const MAX_ITER: usize = 100;
const CONVERGENCE_EPS: f64 = 1e-6;

/// 2-state Gaussian HMM parameters
struct GaussianHMM {
    /// Initial state probabilities [2]
    pi: [f64; 2],
    /// Transition matrix [2][2]: trans[i][j] = P(state_j | state_i)
    trans: [[f64; 2]; 2],
    /// Emission means [2]
    mu: [f64; 2],
    /// Emission std devs [2]
    sigma: [f64; 2],
}

pub fn compute(db: &Connection, cfg: &Settings, as_of: NaiveDate) -> Result<usize> {
    let benchmark = &cfg.universe.benchmark;
    let date_str = as_of.to_string();

    // Query benchmark returns (latest 500, DESC then reverse for chronological)
    let mut stmt = db.prepare(
        "SELECT trade_date, pct_chg
         FROM prices
         WHERE ts_code = ? AND trade_date <= ?
         ORDER BY trade_date DESC
         LIMIT 500",
    )?;

    let mut data: Vec<(String, f64)> = stmt
        .query_map(duckdb::params![benchmark, date_str], |row| {
            Ok((
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, Option<f64>>(1).unwrap_or(None).unwrap_or(0.0),
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Reverse to chronological order (we queried DESC to get latest 500)
    data.reverse();

    let returns: Vec<f64> = data.iter().map(|(_, r)| *r).collect();

    if returns.len() < 100 {
        info!(
            n = returns.len(),
            benchmark = benchmark,
            "insufficient data for HMM, skipping"
        );
        return Ok(0);
    }

    // Initialize HMM via K-means (split returns into positive/negative clusters)
    let mut hmm = initialize_kmeans(&returns);

    // Baum-Welch EM
    let mut prev_ll = f64::NEG_INFINITY;
    let mut converged = false;

    for iter in 0..MAX_ITER {
        // E-step: forward-backward
        let (ll, alpha) = forward(&returns, &hmm);
        let beta = backward(&returns, &hmm);
        let (gamma, xi) = compute_gamma_xi(&returns, &alpha, &beta, &hmm);

        // Check convergence
        if (ll - prev_ll).abs() < CONVERGENCE_EPS {
            converged = true;
            info!(
                iterations = iter + 1,
                log_likelihood = format!("{:.4}", ll),
                "HMM converged"
            );
            break;
        }
        prev_ll = ll;

        // M-step: update parameters
        m_step(&mut hmm, &returns, &gamma, &xi);
    }

    if !converged {
        warn!("HMM did not converge after {} iterations", MAX_ITER);
    }

    // Ensure state 0 = higher mean, state 1 = lower mean
    if hmm.mu[0] < hmm.mu[1] {
        hmm.pi.swap(0, 1);
        hmm.mu.swap(0, 1);
        hmm.sigma.swap(0, 1);
        let t00 = hmm.trans[0][0];
        let t01 = hmm.trans[0][1];
        let t10 = hmm.trans[1][0];
        let t11 = hmm.trans[1][1];
        hmm.trans = [[t11, t10], [t01, t00]];
    }

    // Final forward pass to get posterior at T
    let (_, alpha) = forward(&returns, &hmm);
    let t = returns.len() - 1;
    let alpha_sum: f64 = alpha[t].iter().sum();
    let p_state0 = if alpha_sum > 0.0 {
        alpha[t][0] / alpha_sum
    } else {
        0.5
    };

    // Semantic label: based on emission mean AND volatility
    let label_state0 = if hmm.mu[0] > 0.2 && hmm.sigma[0] > 1.5 {
        "rally"
    } else if hmm.mu[0] > 0.0 {
        "bull"
    } else {
        "bear"
    };
    let label_state1 = if hmm.mu[1] < -0.1 {
        "bear"
    } else if hmm.mu[1] < 0.1 {
        "consolidation"
    } else {
        "quiet_bull"
    };

    // 1-step-ahead: P(state0 tomorrow) = p_state0 × A[0][0] + p_state1 × A[1][0]
    let p_state0_tomorrow = p_state0 * hmm.trans[0][0] + (1.0 - p_state0) * hmm.trans[1][0];

    // P(r > 0 tomorrow) = sum_k P(state=k tomorrow) × P(r>0 | state=k)
    let p_pos_0 = p_positive_given_state(hmm.mu[0], hmm.sigma[0]);
    let p_pos_1 = p_positive_given_state(hmm.mu[1], hmm.sigma[1]);
    let p_ret_positive = p_state0_tomorrow * p_pos_0 + (1.0 - p_state0_tomorrow) * p_pos_1;

    // P(bull) = probability of being in a bullish regime
    let p_bull = if hmm.mu[0] > 0.0 && hmm.mu[1] > 0.0 {
        // Both states have positive mean → market is overall bullish
        // P(bull) ≈ 1 - small adjustment for how close to coin-flip p_ret_positive is
        0.999 - (0.5 - p_ret_positive).abs().min(0.499)
    } else if hmm.mu[0] > 0.0 && hmm.mu[1] <= 0.0 {
        p_state0
    } else if hmm.mu[0] <= 0.0 && hmm.mu[1] > 0.0 {
        1.0 - p_state0
    } else {
        // Both negative means → overall bearish
        0.001 + (p_ret_positive * 0.1)
    };
    let p_bull = p_bull.clamp(0.001, 0.999);

    // Regime duration: count consecutive days in current regime
    let regime_duration = compute_regime_duration(&returns, &hmm);
    let current_regime = if p_state0 > 0.5 {
        label_state0
    } else {
        label_state1
    };

    info!(
        p_bull = format!("{:.3}", p_bull),
        p_ret_positive = format!("{:.3}", p_ret_positive),
        regime = current_regime,
        duration = regime_duration,
        mu_bull = format!("{:.4}", hmm.mu[0]),
        mu_bear = format!("{:.4}", hmm.mu[1]),
        n = returns.len(),
        "HMM regime computed"
    );

    // Write analytics rows
    let mut insert_stmt = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let detail = format!(
        r#"{{"benchmark":"{}","n":{},"converged":{},"mu":[{:.4},{:.4}],"sigma":[{:.4},{:.4}],"regime":"{}","duration":{},"state_labels":["{}","{}"],"p_state0":{:.4}}}"#,
        benchmark,
        returns.len(),
        converged,
        hmm.mu[0],
        hmm.mu[1],
        hmm.sigma[0],
        hmm.sigma[1],
        current_regime,
        regime_duration,
        label_state0,
        label_state1,
        p_state0,
    );

    // P(bull)
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "p_bull",
        p_bull,
        &detail,
    ])?;

    // P(r > 0 tomorrow)
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "p_ret_positive",
        p_ret_positive,
        None::<String>,
    ])?;

    // regime_duration
    insert_stmt.execute(duckdb::params![
        MARKET_CODE,
        date_str,
        MODULE,
        "regime_duration",
        regime_duration as f64,
        None::<String>,
    ])?;

    // Store 1-step forecast for Brier score tracking
    let forecast_id = format!("{}_{}", date_str, benchmark);
    db.execute(
        "INSERT OR REPLACE INTO hmm_forecasts (forecast_id, as_of, horizon, p_predicted, actual, resolved)
         VALUES (?, ?, '1D', ?, NULL, FALSE)",
        duckdb::params![forecast_id, date_str, p_ret_positive],
    )?;

    // Resolve previous day's forecast if we have today's return
    if returns.len() >= 2 {
        let yesterday_return = returns[returns.len() - 1];
        let actual = if yesterday_return > 0.0 { 1 } else { 0 };
        // Find yesterday's date from data
        if data.len() >= 2 {
            let yesterday_date = &data[data.len() - 2].0;
            let prev_id = format!("{}_{}", yesterday_date, benchmark);
            let _ = db.execute(
                "UPDATE hmm_forecasts SET actual = ?, resolved = TRUE WHERE forecast_id = ? AND resolved = FALSE",
                duckdb::params![actual, prev_id],
            );
        }
    }

    // Compute Brier score from resolved forecasts
    let brier = db
        .query_row(
            "SELECT AVG((p_predicted - actual) * (p_predicted - actual))
         FROM hmm_forecasts WHERE resolved = TRUE",
            [],
            |row| row.get::<_, Option<f64>>(0),
        )
        .unwrap_or(None);

    if let Some(bs) = brier {
        insert_stmt.execute(duckdb::params![
            MARKET_CODE,
            date_str,
            MODULE,
            "brier_score",
            bs,
            None::<String>,
        ])?;
        info!(
            brier_score = format!("{:.4}", bs),
            "HMM Brier score computed"
        );
    }

    Ok(1)
}

/// Initialize HMM via simple K-means (split by sign of returns).
fn initialize_kmeans(returns: &[f64]) -> GaussianHMM {
    let pos: Vec<f64> = returns.iter().filter(|&&r| r >= 0.0).cloned().collect();
    let neg: Vec<f64> = returns.iter().filter(|&&r| r < 0.0).cloned().collect();

    let mu_bull = if pos.is_empty() {
        0.5
    } else {
        pos.iter().sum::<f64>() / pos.len() as f64
    };
    let mu_bear = if neg.is_empty() {
        -0.5
    } else {
        neg.iter().sum::<f64>() / neg.len() as f64
    };

    let sigma_bull = if pos.len() > 1 {
        let var = pos.iter().map(|r| (r - mu_bull).powi(2)).sum::<f64>() / (pos.len() - 1) as f64;
        var.sqrt().max(0.01)
    } else {
        1.0
    };

    let sigma_bear = if neg.len() > 1 {
        let var = neg.iter().map(|r| (r - mu_bear).powi(2)).sum::<f64>() / (neg.len() - 1) as f64;
        var.sqrt().max(0.01)
    } else {
        1.0
    };

    let p_bull = pos.len() as f64 / returns.len() as f64;

    GaussianHMM {
        pi: [p_bull, 1.0 - p_bull],
        trans: [[0.95, 0.05], [0.05, 0.95]], // sticky regimes
        mu: [mu_bull, mu_bear],
        sigma: [sigma_bull, sigma_bear],
    }
}

/// Gaussian PDF
fn gaussian_pdf(x: f64, mu: f64, sigma: f64) -> f64 {
    let z = (x - mu) / sigma;
    (-0.5 * z * z).exp() / (sigma * (2.0 * std::f64::consts::PI).sqrt())
}

/// Forward algorithm — returns log-likelihood and scaled α matrix.
fn forward(observations: &[f64], hmm: &GaussianHMM) -> (f64, Vec<[f64; 2]>) {
    let n = observations.len();
    let mut alpha = vec![[0.0f64; 2]; n];
    let mut log_likelihood = 0.0;

    // Initialization
    for k in 0..2 {
        alpha[0][k] = hmm.pi[k] * gaussian_pdf(observations[0], hmm.mu[k], hmm.sigma[k]);
    }
    let scale: f64 = alpha[0].iter().sum();
    if scale > 0.0 {
        for k in 0..2 {
            alpha[0][k] /= scale;
        }
        log_likelihood += scale.ln();
    }

    // Induction
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
            for k in 0..2 {
                alpha[t][k] /= scale_t;
            }
            log_likelihood += scale_t.ln();
        }
    }

    (log_likelihood, alpha)
}

/// Backward algorithm — returns scaled β matrix.
fn backward(observations: &[f64], hmm: &GaussianHMM) -> Vec<[f64; 2]> {
    let n = observations.len();
    let mut beta = vec![[0.0f64; 2]; n];

    // Initialization
    beta[n - 1] = [1.0, 1.0];

    // Induction (backwards)
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
            for k in 0..2 {
                beta[t][k] /= scale_t;
            }
        }
    }

    beta
}

/// Compute gamma (state posteriors) and xi (transition posteriors).
fn compute_gamma_xi(
    observations: &[f64],
    alpha: &[[f64; 2]],
    beta: &[[f64; 2]],
    hmm: &GaussianHMM,
) -> (Vec<[f64; 2]>, Vec<[[f64; 2]; 2]>) {
    let n = observations.len();
    let mut gamma = vec![[0.0f64; 2]; n];
    let mut xi = vec![[[0.0f64; 2]; 2]; n - 1];

    // Gamma
    for t in 0..n {
        let mut denom = 0.0;
        for k in 0..2 {
            gamma[t][k] = alpha[t][k] * beta[t][k];
            denom += gamma[t][k];
        }
        if denom > 0.0 {
            for k in 0..2 {
                gamma[t][k] /= denom;
            }
        }
    }

    // Xi
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

/// M-step: update HMM parameters from gamma and xi.
fn m_step(hmm: &mut GaussianHMM, observations: &[f64], gamma: &[[f64; 2]], xi: &[[[f64; 2]; 2]]) {
    let n = observations.len();

    // Update initial probabilities
    let g0_sum: f64 = gamma[0].iter().sum();
    if g0_sum > 0.0 {
        for k in 0..2 {
            hmm.pi[k] = (gamma[0][k] / g0_sum).max(1e-10);
        }
    }

    // Update transition matrix
    for i in 0..2 {
        let gamma_sum: f64 = gamma[..n - 1].iter().map(|g| g[i]).sum();
        if gamma_sum > 0.0 {
            for j in 0..2 {
                let xi_sum: f64 = xi.iter().map(|x| x[i][j]).sum();
                hmm.trans[i][j] = (xi_sum / gamma_sum).max(1e-10);
            }
            // Normalize
            let row_sum: f64 = hmm.trans[i].iter().sum();
            if row_sum > 0.0 {
                for j in 0..2 {
                    hmm.trans[i][j] /= row_sum;
                }
            }
        }
    }

    // Update emission means and variances
    for k in 0..2 {
        let gamma_sum: f64 = gamma.iter().map(|g| g[k]).sum();
        if gamma_sum > 1e-10 {
            // Mean
            let weighted_sum: f64 = gamma
                .iter()
                .zip(observations.iter())
                .map(|(g, &x)| g[k] * x)
                .sum();
            hmm.mu[k] = weighted_sum / gamma_sum;

            // Variance
            let weighted_var: f64 = gamma
                .iter()
                .zip(observations.iter())
                .map(|(g, &x)| g[k] * (x - hmm.mu[k]).powi(2))
                .sum();
            hmm.sigma[k] = (weighted_var / gamma_sum).sqrt().max(0.01);
        }
    }
}

/// P(r > 0 | state k) using Gaussian CDF approximation.
fn p_positive_given_state(mu: f64, sigma: f64) -> f64 {
    // P(X > 0) = 1 - Φ(-μ/σ) = Φ(μ/σ)
    let z = mu / sigma;
    standard_normal_cdf(z)
}

/// Approximation of standard normal CDF.
fn standard_normal_cdf(x: f64) -> f64 {
    // Abramowitz & Stegun approximation (max error 7.5e-8)
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    0.5 * (1.0 + sign * y)
}

/// Count consecutive days in current regime via Viterbi.
fn compute_regime_duration(returns: &[f64], hmm: &GaussianHMM) -> u32 {
    let n = returns.len();
    if n == 0 {
        return 0;
    }

    // Viterbi decoding
    let mut viterbi = vec![[0.0f64; 2]; n];
    let mut path = vec![[0usize; 2]; n];

    // Initialize
    for k in 0..2 {
        viterbi[0][k] = hmm.pi[k].ln()
            + gaussian_pdf(returns[0], hmm.mu[k], hmm.sigma[k])
                .max(1e-300)
                .ln();
    }

    // Forward
    for t in 1..n {
        for j in 0..2 {
            let emission = gaussian_pdf(returns[t], hmm.mu[j], hmm.sigma[j])
                .max(1e-300)
                .ln();
            let (best_i, best_val) = (0..2)
                .map(|i| (i, viterbi[t - 1][i] + hmm.trans[i][j].max(1e-300).ln()))
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .unwrap();
            viterbi[t][j] = best_val + emission;
            path[t][j] = best_i;
        }
    }

    // Backtrack
    let mut states = vec![0usize; n];
    states[n - 1] = if viterbi[n - 1][0] > viterbi[n - 1][1] {
        0
    } else {
        1
    };
    for t in (0..n - 1).rev() {
        states[t] = path[t + 1][states[t + 1]];
    }

    // Count consecutive from end
    let current = states[n - 1];
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
