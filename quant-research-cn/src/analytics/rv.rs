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
}
