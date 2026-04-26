/// Realized volatility estimators from OHLC data.
///
/// Provides Parkinson, Garman-Klass, and Yang-Zhang estimators that are
/// more efficient than close-to-close volatility, especially for intraday.
///
/// All estimators return daily VARIANCE (not std dev). Caller annualizes.

/// Parkinson (1980) — uses High/Low range.
/// var_t = [ln(H/L)]^2 / (4 * ln(2))
pub fn parkinson_var(high: f64, low: f64) -> f64 {
    if high <= 0.0 || low <= 0.0 || high < low {
        return 0.0;
    }
    let hl = (high / low).ln();
    hl * hl / (4.0 * 2.0_f64.ln())
}

/// Garman-Klass (1980) — uses OHLC.
/// var_t = 0.5 * [ln(H/L)]^2 - (2*ln2 - 1) * [ln(C/O)]^2
pub fn garman_klass_var(open: f64, high: f64, low: f64, close: f64) -> f64 {
    if open <= 0.0 || high <= 0.0 || low <= 0.0 || close <= 0.0 || high < low {
        return 0.0;
    }
    let hl = (high / low).ln();
    let co = (close / open).ln();
    let val = 0.5 * hl * hl - (2.0 * 2.0_f64.ln() - 1.0) * co * co;
    val.max(0.0) // ensure non-negative
}

/// Yang-Zhang (2000) — combines overnight, open-to-close, and Rogers-Satchell.
/// Most efficient for OHLC with overnight jumps (A-shares have gaps).
///
/// Takes a slice of (open, high, low, close) tuples.
/// Returns annualized volatility (%) or 0.0 if insufficient data.
pub fn yang_zhang_vol(bars: &[(f64, f64, f64, f64)], annualize_factor: f64) -> f64 {
    let n = bars.len();
    if n < 3 {
        return 0.0;
    }

    // overnight returns: ln(O_t / C_{t-1})
    let mut overnight = Vec::with_capacity(n - 1);
    // open-to-close (intraday) returns: ln(C_t / O_t) — NOT close-to-close
    let mut oc = Vec::with_capacity(n - 1);
    // Rogers-Satchell variance per bar (only for bars with prior close)
    let mut rs_vars = Vec::with_capacity(n - 1);

    for i in 1..n {
        let (o, h, l, c) = bars[i];
        let prev_c = bars[i - 1].3;
        if o <= 0.0 || h <= 0.0 || l <= 0.0 || c <= 0.0 || prev_c <= 0.0 {
            continue;
        }
        overnight.push((o / prev_c).ln());
        oc.push((c / o).ln());
        // Rogers-Satchell: ln(H/C)*ln(H/O) + ln(L/C)*ln(L/O)
        let hc = (h / c).ln();
        let ho = (h / o).ln();
        let lc = (l / c).ln();
        let lo = (l / o).ln();
        rs_vars.push(hc * ho + lc * lo);
    }

    if overnight.len() < 2 {
        return 0.0;
    }

    let nn = overnight.len() as f64;

    // Variance of overnight returns
    let on_mean = overnight.iter().sum::<f64>() / nn;
    let var_overnight = overnight.iter().map(|r| (r - on_mean).powi(2)).sum::<f64>() / (nn - 1.0);

    // Variance of open-to-close (intraday) returns
    let oc_mean = oc.iter().sum::<f64>() / nn;
    let var_oc = oc.iter().map(|r| (r - oc_mean).powi(2)).sum::<f64>() / (nn - 1.0);

    // Mean Rogers-Satchell variance
    let var_rs = rs_vars.iter().sum::<f64>() / nn;

    // Yang-Zhang: k = 0.34 / (1.34 + (n+1)/(n-1))
    let k = 0.34 / (1.34 + (nn + 1.0) / (nn - 1.0));
    let var_yz = var_overnight + k * var_oc + (1.0 - k) * var_rs;

    if var_yz <= 0.0 {
        return 0.0;
    }

    // Annualize and convert to percentage
    (var_yz * annualize_factor).sqrt() * 100.0
}

/// Rolling realized volatility using Garman-Klass estimator.
/// Returns annualized vol (%) for the latest `window` bars.
pub fn rolling_gk_vol(bars: &[(f64, f64, f64, f64)], window: usize) -> f64 {
    if bars.len() < window || window < 2 {
        return 0.0;
    }

    let recent = &bars[bars.len() - window..];
    let sum_var: f64 = recent
        .iter()
        .map(|(o, h, l, c)| garman_klass_var(*o, *h, *l, *c))
        .sum();

    let avg_var = sum_var / window as f64;
    if avg_var <= 0.0 {
        return 0.0;
    }

    // Annualize: sqrt(252 * avg_daily_var) * 100
    (252.0 * avg_var).sqrt() * 100.0
}

/// Compute log-variance series from OHLC bars (Garman-Klass).
/// Returns ln(var_t + eps) for each bar — suitable as HMM observation.
pub fn log_variance_series(bars: &[(f64, f64, f64, f64)]) -> Vec<f64> {
    let eps = 1e-10;
    bars.iter()
        .map(|(o, h, l, c)| {
            let var = garman_klass_var(*o, *h, *l, *c);
            (var + eps).ln()
        })
        .collect()
}

/// Whether an observed daily return is uncensored or clipped by the exchange limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CensorSide {
    None,
    Left,
    Right,
}

/// One daily stock return observation for a censored-normal volatility estimate.
///
/// Values are decimal returns, not percentage points.  For example +10% is 0.10.
#[derive(Debug, Clone, Copy)]
pub struct CensoredReturn {
    pub observed: f64,
    pub lower: f64,
    pub upper: f64,
    pub side: CensorSide,
}

/// Daily cross-sectional market volatility point from limit-censored returns.
#[derive(Debug, Clone)]
pub struct CensoredVolPoint {
    pub date: String,
    pub tobit_var: f64,
    pub raw_var: f64,
    pub n: usize,
    pub limit_up: usize,
    pub limit_down: usize,
}

impl CensoredVolPoint {
    pub fn censor_ratio(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            (self.limit_up + self.limit_down) as f64 / self.n as f64
        }
    }
}

/// Exchange daily price limit by board.
///
/// This intentionally uses simple code/name rules so the estimator can run even
/// when a separate limit-up table is unavailable.
pub fn price_limit_pct(ts_code: &str, name: &str) -> f64 {
    let code = ts_code.split('.').next().unwrap_or(ts_code);
    let upper_name = name.to_uppercase();
    if upper_name.contains("ST") {
        5.0
    } else if ts_code.ends_with(".BJ") || code.starts_with('4') || code.starts_with('8') {
        30.0
    } else if code.starts_with("300") || code.starts_with("301") || code.starts_with("688") {
        20.0
    } else {
        10.0
    }
}

/// Infer whether a bar closed at its daily exchange limit.
pub fn infer_censor_side(
    ts_code: &str,
    name: &str,
    pct_chg: f64,
    high: f64,
    low: f64,
    close: f64,
) -> CensorSide {
    if close <= 0.0 {
        return CensorSide::None;
    }
    let limit = price_limit_pct(ts_code, name);
    let tolerance = if limit <= 5.0 { 0.12 } else { 0.20 };
    let at_high = high > 0.0 && close >= high * 0.999;
    let at_low = low > 0.0 && close <= low * 1.001;
    if pct_chg >= limit - tolerance && at_high {
        CensorSide::Right
    } else if pct_chg <= -limit + tolerance && at_low {
        CensorSide::Left
    } else {
        CensorSide::None
    }
}

pub fn censored_return_from_pct(
    ts_code: &str,
    name: &str,
    pct_chg: f64,
    high: f64,
    low: f64,
    close: f64,
) -> Option<CensoredReturn> {
    if !pct_chg.is_finite() {
        return None;
    }
    let limit = price_limit_pct(ts_code, name) / 100.0;
    Some(CensoredReturn {
        observed: pct_chg / 100.0,
        lower: -limit,
        upper: limit,
        side: infer_censor_side(ts_code, name, pct_chg, high, low, close),
    })
}

pub fn sample_variance(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    Some(var.max(0.0))
}

/// Estimate latent variance from censored-normal observations via Tobit EM.
///
/// Right-censored observations contribute E[r | r >= upper]; left-censored
/// observations contribute E[r | r <= lower].  Uncensored observations remain
/// exact.  This lifts volatility on limit-up/limit-down days where observed
/// returns understate latent pressure.
pub fn tobit_variance(observations: &[CensoredReturn]) -> Option<f64> {
    if observations.len() < 5 {
        return None;
    }

    let raw: Vec<f64> = observations.iter().map(|obs| obs.observed).collect();
    let uncensored: Vec<f64> = observations
        .iter()
        .filter(|obs| obs.side == CensorSide::None)
        .map(|obs| obs.observed)
        .collect();

    let mut mu = if uncensored.len() >= 3 {
        uncensored.iter().sum::<f64>() / uncensored.len() as f64
    } else {
        raw.iter().sum::<f64>() / raw.len() as f64
    };
    let mut var = sample_variance(&raw).unwrap_or(1e-4).clamp(1e-6, 0.25);

    for _ in 0..30 {
        let sigma = var.sqrt().max(1e-4);
        let mut sum_y = 0.0;
        let mut sum_y2 = 0.0;

        for obs in observations {
            let (ey, ey2) = match obs.side {
                CensorSide::None => (obs.observed, obs.observed * obs.observed),
                CensorSide::Right => {
                    let a = (obs.upper - mu) / sigma;
                    let tail = (1.0 - normal_cdf(a)).max(1e-12);
                    let lambda = normal_pdf(a) / tail;
                    let mean = mu + sigma * lambda;
                    let cond_var = var * (1.0 + a * lambda - lambda * lambda).max(1e-8);
                    (mean, cond_var + mean * mean)
                }
                CensorSide::Left => {
                    let b = (obs.lower - mu) / sigma;
                    let prob = normal_cdf(b).max(1e-12);
                    let lambda = normal_pdf(b) / prob;
                    let mean = mu - sigma * lambda;
                    let cond_var = var * (1.0 - b * lambda - lambda * lambda).max(1e-8);
                    (mean, cond_var + mean * mean)
                }
            };
            sum_y += ey;
            sum_y2 += ey2;
        }

        let n = observations.len() as f64;
        let next_mu = sum_y / n;
        let next_var = (sum_y2 / n - next_mu * next_mu).clamp(1e-8, 0.25);
        if (next_mu - mu).abs() < 1e-8 && (next_var - var).abs() < 1e-10 {
            var = next_var;
            break;
        }
        mu = next_mu;
        var = next_var;
    }

    Some(var)
}

pub fn daily_censored_vol_point(
    date: String,
    observations: &[CensoredReturn],
) -> Option<CensoredVolPoint> {
    if observations.len() < 50 {
        return None;
    }
    let raw: Vec<f64> = observations.iter().map(|obs| obs.observed).collect();
    let raw_var = sample_variance(&raw)?;
    let tobit_var = tobit_variance(observations)?.max(raw_var);
    let limit_up = observations
        .iter()
        .filter(|obs| obs.side == CensorSide::Right)
        .count();
    let limit_down = observations
        .iter()
        .filter(|obs| obs.side == CensorSide::Left)
        .count();
    Some(CensoredVolPoint {
        date,
        tobit_var,
        raw_var,
        n: observations.len(),
        limit_up,
        limit_down,
    })
}

pub fn log_tobit_variance_series(points: &[CensoredVolPoint]) -> Vec<f64> {
    let eps = 1e-10;
    points
        .iter()
        .map(|point| (point.tobit_var.max(eps)).ln())
        .collect()
}

pub fn rolling_tobit_vol(points: &[CensoredVolPoint], window: usize) -> f64 {
    if points.len() < window || window < 2 {
        return 0.0;
    }
    let recent = &points[points.len() - window..];
    let avg_var = recent.iter().map(|point| point.tobit_var).sum::<f64>() / window as f64;
    if avg_var <= 0.0 {
        0.0
    } else {
        (252.0 * avg_var).sqrt() * 100.0
    }
}

pub fn rolling_raw_cross_section_vol(points: &[CensoredVolPoint], window: usize) -> f64 {
    if points.len() < window || window < 2 {
        return 0.0;
    }
    let recent = &points[points.len() - window..];
    let avg_var = recent.iter().map(|point| point.raw_var).sum::<f64>() / window as f64;
    if avg_var <= 0.0 {
        0.0
    } else {
        (252.0 * avg_var).sqrt() * 100.0
    }
}

fn normal_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

fn normal_cdf(x: f64) -> f64 {
    // Abramowitz-Stegun approximation; enough for Tobit tail weights here.
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let z = x.abs() / 2.0_f64.sqrt();
    0.5 * (1.0 + sign * erf_approx(z))
}

fn erf_approx(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let poly = (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t;
    1.0 - poly * (-x * x).exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parkinson() {
        // H=110, L=90 → ln(110/90) ≈ 0.2007
        let var = parkinson_var(110.0, 90.0);
        assert!(var > 0.01 && var < 0.02, "parkinson_var={}", var);
    }

    #[test]
    fn test_garman_klass() {
        let var = garman_klass_var(100.0, 110.0, 90.0, 105.0);
        assert!(var > 0.0, "gk_var should be positive");
    }

    #[test]
    fn test_rolling_gk_vol() {
        let bars: Vec<(f64, f64, f64, f64)> = (0..20)
            .map(|i| {
                let base = 100.0 + i as f64;
                (base, base + 2.0, base - 2.0, base + 0.5)
            })
            .collect();
        let vol = rolling_gk_vol(&bars, 20);
        assert!(vol > 0.0, "rolling_gk_vol should be positive: {}", vol);
    }

    #[test]
    fn test_yang_zhang() {
        let bars: Vec<(f64, f64, f64, f64)> = (0..20)
            .map(|i| {
                let base = 100.0 + i as f64 * 0.5;
                (base, base + 3.0, base - 2.0, base + 1.0)
            })
            .collect();
        let vol = yang_zhang_vol(&bars, 252.0);
        assert!(vol > 0.0, "yang_zhang should be positive: {}", vol);
    }

    #[test]
    fn test_price_limit_rules() {
        assert_eq!(price_limit_pct("600000.SH", "浦发银行"), 10.0);
        assert_eq!(price_limit_pct("300750.SZ", "宁德时代"), 20.0);
        assert_eq!(price_limit_pct("688001.SH", "华兴源创"), 20.0);
        assert_eq!(price_limit_pct("000001.SZ", "*ST测试"), 5.0);
        assert_eq!(price_limit_pct("830000.BJ", "北交所测试"), 30.0);
    }

    #[test]
    fn test_infer_censor_side_requires_close_at_limit_edge() {
        assert_eq!(
            infer_censor_side("600000.SH", "浦发银行", 10.01, 11.0, 10.0, 11.0),
            CensorSide::Right
        );
        assert_eq!(
            infer_censor_side("600000.SH", "浦发银行", 10.01, 11.5, 10.0, 11.0),
            CensorSide::None
        );
        assert_eq!(
            infer_censor_side("600000.SH", "浦发银行", -9.98, 10.0, 9.0, 9.0),
            CensorSide::Left
        );
    }

    #[test]
    fn test_tobit_variance_lifts_limit_censored_returns() {
        let mut observations = Vec::new();
        for value in [-0.01, -0.004, 0.0, 0.003, 0.007, 0.012, -0.006, 0.004] {
            observations.push(CensoredReturn {
                observed: value,
                lower: -0.10,
                upper: 0.10,
                side: CensorSide::None,
            });
        }
        for _ in 0..4 {
            observations.push(CensoredReturn {
                observed: 0.10,
                lower: -0.10,
                upper: 0.10,
                side: CensorSide::Right,
            });
        }
        let raw = sample_variance(&observations.iter().map(|obs| obs.observed).collect::<Vec<_>>())
            .unwrap();
        let tobit = tobit_variance(&observations).unwrap();
        assert!(tobit >= raw, "tobit={tobit}, raw={raw}");
    }
}
