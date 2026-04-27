/// Volatility HMM — 2-state Gaussian HMM on log-variance.
///
/// Complements the return-HMM (hmm.rs) by detecting volatility regimes:
///   State 0 (low_vol): market in quiet/calm regime
///   State 1 (high_vol): market in stressed/turbulent regime
///
/// Uses cross-sectional limit-censored Tobit log-variance as observation.
/// Outputs: P(high_vol), P(high_vol_tomorrow), rv_tobit_20d, vol_regime_duration.
use anyhow::Result;
use chrono::NaiveDate;
use duckdb::Connection;
use std::collections::BTreeMap;
use tracing::{info, warn};

use super::rv;
use crate::config::Settings;

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

    let points = load_market_censored_vol_points(db, as_of)?;
    if points.len() < 100 {
        info!(
            n = points.len(),
            "insufficient censored market vol data for vol_hmm, skipping"
        );
        return Ok(0);
    }

    let log_vars = rv::log_tobit_variance_series(&points);
    let rv_tobit_20d = rv::rolling_tobit_vol(&points, 20);
    let rv_raw_20d = rv::rolling_raw_cross_section_vol(&points, 20);
    let latest = points.last().expect("points checked non-empty");
    let recent_n: usize = points.iter().rev().take(20).map(|point| point.n).sum();
    let recent_limit_up: usize = points
        .iter()
        .rev()
        .take(20)
        .map(|point| point.limit_up)
        .sum();
    let recent_limit_down: usize = points
        .iter()
        .rev()
        .take(20)
        .map(|point| point.limit_down)
        .sum();
    let censor_ratio_20d = if recent_n == 0 {
        0.0
    } else {
        (recent_limit_up + recent_limit_down) as f64 / recent_n as f64
    };

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
            info!(
                iterations = iter + 1,
                log_likelihood = format!("{:.4}", ll),
                "vol_hmm converged"
            );
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
    let p_low_vol = if alpha_sum > 0.0 {
        alpha[t][0] / alpha_sum
    } else {
        0.5
    };
    let p_high_vol = 1.0 - p_low_vol;

    // 1-step-ahead: P(high_vol tomorrow)
    let p_high_vol_tomorrow = p_low_vol * hmm.trans[0][1] + p_high_vol * hmm.trans[1][1];

    // Regime duration via Viterbi
    let regime_duration = compute_regime_duration(&log_vars, &hmm);
    let current_regime = if p_high_vol > 0.5 {
        "high_vol"
    } else {
        "low_vol"
    };

    // Convert log-var means to approximate annualized vol for interpretability
    let vol_state0 = (hmm.mu[0].exp() * 252.0).sqrt() * 100.0; // low_vol regime typical vol
    let vol_state1 = (hmm.mu[1].exp() * 252.0).sqrt() * 100.0; // high_vol regime typical vol

    info!(
        p_high_vol = format!("{:.3}", p_high_vol),
        p_high_vol_tomorrow = format!("{:.3}", p_high_vol_tomorrow),
        rv_tobit_20d = format!("{:.2}", rv_tobit_20d),
        rv_raw_20d = format!("{:.2}", rv_raw_20d),
        censor_ratio_20d = format!("{:.4}", censor_ratio_20d),
        regime = current_regime,
        duration = regime_duration,
        vol_low = format!("{:.1}%", vol_state0),
        vol_high = format!("{:.1}%", vol_state1),
        n = log_vars.len(),
        "vol_hmm regime computed"
    );

    db.execute(
        "DELETE FROM analytics
         WHERE ts_code = ?
           AND as_of = CAST(? AS DATE)
           AND module = ?",
        duckdb::params![MARKET_CODE, &date_str, MODULE],
    )?;

    // Write analytics rows
    let mut insert = db.prepare(
        "INSERT OR REPLACE INTO analytics (ts_code, as_of, module, metric, value, detail)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?;

    let detail = format!(
        r#"{{"source":"cross_section_limit_tobit","benchmark":"{}","n":{},"latest_date":"{}","latest_cross_section_n":{},"latest_limit_up":{},"latest_limit_down":{},"latest_censor_ratio":{:.6},"censor_ratio_20d":{:.6},"rv_tobit_20d":{:.4},"rv_raw_20d":{:.4},"converged":{},"mu_logvar":[{:.4},{:.4}],"sigma_logvar":[{:.4},{:.4}],"vol_approx":[{:.1},{:.1}],"regime":"{}","duration":{}}}"#,
        benchmark,
        log_vars.len(),
        latest.date,
        latest.n,
        latest.limit_up,
        latest.limit_down,
        latest.censor_ratio(),
        censor_ratio_20d,
        rv_tobit_20d,
        rv_raw_20d,
        converged,
        hmm.mu[0],
        hmm.mu[1],
        hmm.sigma[0],
        hmm.sigma[1],
        vol_state0,
        vol_state1,
        current_regime,
        regime_duration,
    );

    // p_high_vol — current probability of being in high-vol regime
    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "p_high_vol",
        p_high_vol,
        &detail,
    ])?;

    // p_high_vol_tomorrow — 1-step-ahead forecast
    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "p_high_vol_tomorrow",
        p_high_vol_tomorrow,
        None::<String>,
    ])?;

    // rv_tobit_20d — main A-share vol input: limit-censored cross-section vol.
    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "rv_tobit_20d",
        rv_tobit_20d,
        None::<String>,
    ])?;

    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "rv_raw_20d",
        rv_raw_20d,
        None::<String>,
    ])?;

    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "limit_censor_ratio_20d",
        censor_ratio_20d,
        None::<String>,
    ])?;

    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "limit_up_count_20d",
        recent_limit_up as f64,
        None::<String>,
    ])?;

    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "limit_down_count_20d",
        recent_limit_down as f64,
        None::<String>,
    ])?;

    // vol_regime_duration
    insert.execute(duckdb::params![
        MARKET_CODE,
        &date_str,
        MODULE,
        "vol_regime_duration",
        regime_duration as f64,
        None::<String>,
    ])?;

    info!("vol_hmm analytics written");
    Ok(1)
}

fn load_market_censored_vol_points(
    db: &Connection,
    as_of: NaiveDate,
) -> Result<Vec<rv::CensoredVolPoint>> {
    let mut grouped: BTreeMap<String, Vec<rv::CensoredReturn>> = BTreeMap::new();
    let mut stmt = db.prepare(
        "WITH recent_dates AS (
             SELECT DISTINCT trade_date
             FROM prices
             WHERE trade_date <= CAST(? AS DATE)
             ORDER BY trade_date DESC
             LIMIT 520
         )
         SELECT CAST(p.trade_date AS VARCHAR),
                p.ts_code,
                COALESCE(sb.name, ''),
                p.pct_chg,
                p.high,
                p.low,
                p.close
         FROM prices p
         INNER JOIN recent_dates rd ON p.trade_date = rd.trade_date
         INNER JOIN stock_basic sb ON p.ts_code = sb.ts_code
         WHERE COALESCE(sb.list_status, 'L') = 'L'
           AND p.pct_chg IS NOT NULL
           AND p.high IS NOT NULL
           AND p.low IS NOT NULL
           AND p.close IS NOT NULL
           AND p.close > 0
         ORDER BY p.trade_date, p.ts_code",
    )?;

    let rows = stmt.query_map(duckdb::params![as_of.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, f64>(3)?,
            row.get::<_, f64>(4)?,
            row.get::<_, f64>(5)?,
            row.get::<_, f64>(6)?,
        ))
    })?;

    for row in rows {
        let (date, ts_code, name, pct_chg, high, low, close) = row?;
        if high <= 0.0 || low <= 0.0 || close <= 0.0 || high < low {
            continue;
        }
        if let Some(obs) = rv::censored_return_from_pct(&ts_code, &name, pct_chg, high, low, close)
        {
            grouped.entry(date).or_default().push(obs);
        }
    }

    let mut points = Vec::with_capacity(grouped.len());
    for (date, observations) in grouped {
        if let Some(point) = rv::daily_censored_vol_point(date, &observations) {
            points.push(point);
        }
    }
    Ok(points)
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
    } else {
        1.0
    };

    let sigma_high = if high.len() > 1 {
        let var = high.iter().map(|x| (x - mu_high).powi(2)).sum::<f64>() / (high.len() - 1) as f64;
        var.sqrt().max(0.01)
    } else {
        1.0
    };

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
        for k in 0..2 {
            alpha[0][k] /= scale;
        }
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
            for k in 0..2 {
                alpha[t][k] /= scale_t;
            }
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
            for k in 0..2 {
                beta[t][k] /= scale_t;
            }
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
            for k in 0..2 {
                gamma[t][k] /= denom;
            }
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

fn m_step(hmm: &mut GaussianHMM, observations: &[f64], gamma: &[[f64; 2]], xi: &[[[f64; 2]; 2]]) {
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
                for j in 0..2 {
                    hmm.trans[i][j] /= row_sum;
                }
            }
        }
    }

    for k in 0..2 {
        let gamma_sum: f64 = gamma.iter().map(|g| g[k]).sum();
        if gamma_sum > 1e-10 {
            let weighted_sum: f64 = gamma
                .iter()
                .zip(observations.iter())
                .map(|(g, &x)| g[k] * x)
                .sum();
            hmm.mu[k] = weighted_sum / gamma_sum;

            let weighted_var: f64 = gamma
                .iter()
                .zip(observations.iter())
                .map(|(g, &x)| g[k] * (x - hmm.mu[k]).powi(2))
                .sum();
            hmm.sigma[k] = (weighted_var / gamma_sum).sqrt().max(0.01);
        }
    }
}

fn compute_regime_duration(observations: &[f64], hmm: &GaussianHMM) -> u32 {
    let n = observations.len();
    if n == 0 {
        return 0;
    }

    let mut viterbi = vec![[0.0f64; 2]; n];
    let mut path = vec![[0usize; 2]; n];

    for k in 0..2 {
        viterbi[0][k] = hmm.pi[k].ln()
            + gaussian_pdf(observations[0], hmm.mu[k], hmm.sigma[k])
                .max(1e-300)
                .ln();
    }

    for t in 1..n {
        for j in 0..2 {
            let emission = gaussian_pdf(observations[t], hmm.mu[j], hmm.sigma[j])
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

    let mut states = vec![0usize; n];
    states[n - 1] = if viterbi[n - 1][0] > viterbi[n - 1][1] {
        0
    } else {
        1
    };
    for t in (0..n - 1).rev() {
        states[t] = path[t + 1][states[t + 1]];
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ApiConfig, AssetClassConfig, DataConfig, EnrichmentConfig, FilterConfig, MacroConfig,
        OutputConfig, ReportingConfig, RuntimeConfig, ScanConfig, Settings, SignalsConfig,
        UniverseConfig,
    };
    use chrono::Duration;

    fn test_settings() -> Settings {
        Settings {
            api: ApiConfig {
                tushare_token: String::new(),
                deepseek_key: String::new(),
            },
            runtime: RuntimeConfig {
                timezone: "Asia/Shanghai".to_string(),
                random_seed: 1,
            },
            universe: UniverseConfig {
                benchmark: "000300.SH".to_string(),
                scan: ScanConfig {
                    csi300: true,
                    csi500: false,
                    csi1000: false,
                    sse50: false,
                },
                asset_classes: AssetClassConfig {
                    sector_etfs: false,
                    bond_etfs: false,
                    commodity_etfs: false,
                    cross_border: false,
                },
                watchlist: Vec::new(),
                filters: FilterConfig {
                    min_avg_volume_shares: 0,
                    min_price: 0.0,
                },
            },
            output: OutputConfig {
                max_notable_items: 10,
                min_notable_items: 1,
            },
            data: DataConfig {
                db_path: String::new(),
                raw_db_path: String::new(),
                research_db_path: String::new(),
                report_db_path: String::new(),
                dev_db_path: String::new(),
                use_dev_for_research: false,
                constituent_refresh_days: 7,
            },
            signals: SignalsConfig {
                momentum_windows: vec![5, 20],
                atr_period: 14,
                ma_filter_window: 120,
                flow_ewma_halflife: 10,
                unlock_lookahead_days: 30,
            },
            reporting: ReportingConfig {
                anthropic_model: String::new(),
                anthropic_temperature: 0.0,
                max_tokens: 0,
                recipients: Vec::new(),
            },
            r#macro: MacroConfig::default(),
            enrichment: EnrichmentConfig::default(),
        }
    }

    fn init_test_db(db: &Connection) -> Result<()> {
        db.execute_batch(
            "CREATE TABLE prices (
                 ts_code VARCHAR NOT NULL,
                 trade_date DATE NOT NULL,
                 open DOUBLE,
                 high DOUBLE,
                 low DOUBLE,
                 close DOUBLE,
                 pre_close DOUBLE,
                 change DOUBLE,
                 pct_chg DOUBLE,
                 vol DOUBLE,
                 amount DOUBLE,
                 adj_factor DOUBLE,
                 PRIMARY KEY (ts_code, trade_date)
             );
             CREATE TABLE stock_basic (
                 ts_code VARCHAR NOT NULL PRIMARY KEY,
                 symbol VARCHAR,
                 name VARCHAR,
                 area VARCHAR,
                 industry VARCHAR,
                 market VARCHAR,
                 list_date VARCHAR,
                 list_status VARCHAR
             );
             CREATE TABLE analytics (
                 ts_code VARCHAR NOT NULL,
                 as_of DATE NOT NULL,
                 module VARCHAR NOT NULL,
                 metric VARCHAR NOT NULL,
                 value DOUBLE,
                 detail VARCHAR,
                 PRIMARY KEY (ts_code, as_of, module, metric)
             );",
        )?;
        Ok(())
    }

    #[test]
    fn compute_replaces_stale_gk_metric_with_tobit_metrics() -> Result<()> {
        let db = Connection::open_in_memory()?;
        init_test_db(&db)?;

        db.execute_batch("BEGIN TRANSACTION")?;
        {
            let mut insert_stock = db.prepare(
                "INSERT INTO stock_basic
                 (ts_code, symbol, name, area, industry, market, list_date, list_status)
                 VALUES (?, ?, ?, '', '', '主板', '20200101', 'L')",
            )?;
            for stock_idx in 0..50 {
                let ts_code = format!("{:06}.SH", 600000 + stock_idx);
                insert_stock.execute(duckdb::params![
                    &ts_code,
                    format!("{:06}", 600000 + stock_idx),
                    format!("测试{stock_idx}")
                ])?;
            }
        }

        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        {
            let mut insert_price = db.prepare(
                "INSERT INTO prices
                 (ts_code, trade_date, open, high, low, close, pct_chg)
                 VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?)",
            )?;
            for day in 0..100 {
                let trade_date = start + Duration::days(day);
                let date_str = trade_date.to_string();
                for stock_idx in 0..50 {
                    let ts_code = format!("{:06}.SH", 600000 + stock_idx);
                    let base = 10.0 + stock_idx as f64 * 0.05 + day as f64 * 0.01;
                    let pct_chg = if day % 17 == 0 && stock_idx % 10 == 0 {
                        10.0
                    } else if day % 19 == 0 && stock_idx % 15 == 0 {
                        -10.0
                    } else {
                        (stock_idx as f64 % 9.0 - 4.0) * 0.35 + (day as f64 % 7.0 - 3.0) * 0.18
                    };
                    let close = base * (1.0 + pct_chg / 100.0);
                    let (high, low) = if pct_chg >= 9.8 {
                        (close, base.min(close) * 0.99)
                    } else if pct_chg <= -9.8 {
                        (base.max(close) * 1.01, close)
                    } else {
                        (base.max(close) * 1.01, base.min(close) * 0.99)
                    };
                    insert_price.execute(duckdb::params![
                        &ts_code, &date_str, base, high, low, close, pct_chg
                    ])?;
                }
            }
        }
        db.execute_batch("COMMIT")?;

        let as_of = start + Duration::days(99);
        let as_of_str = as_of.to_string();
        db.execute(
            "INSERT INTO analytics (ts_code, as_of, module, metric, value, detail)
             VALUES ('_MARKET', CAST(? AS DATE), 'vol_hmm', 'rv_gk_20d', 12.3, NULL)",
            duckdb::params![&as_of_str],
        )?;

        let written = compute(&db, &test_settings(), as_of)?;
        assert_eq!(written, 1);

        let stale_gk: i64 = db.query_row(
            "SELECT COUNT(*) FROM analytics
             WHERE ts_code = '_MARKET'
               AND as_of = CAST(? AS DATE)
               AND module = 'vol_hmm'
               AND metric = 'rv_gk_20d'",
            duckdb::params![&as_of_str],
            |row| row.get(0),
        )?;
        assert_eq!(stale_gk, 0);

        let rv_tobit: f64 = db.query_row(
            "SELECT value FROM analytics
             WHERE ts_code = '_MARKET'
               AND as_of = CAST(? AS DATE)
               AND module = 'vol_hmm'
               AND metric = 'rv_tobit_20d'",
            duckdb::params![&as_of_str],
            |row| row.get(0),
        )?;
        assert!(rv_tobit > 0.0);

        let source: String = db.query_row(
            "SELECT json_extract_string(detail, '$.source')
             FROM analytics
             WHERE ts_code = '_MARKET'
               AND as_of = CAST(? AS DATE)
               AND module = 'vol_hmm'
               AND metric = 'p_high_vol'",
            duckdb::params![&as_of_str],
            |row| row.get(0),
        )?;
        assert_eq!(source, "cross_section_limit_tobit");

        Ok(())
    }
}
