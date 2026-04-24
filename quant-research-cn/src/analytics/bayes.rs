/// Beta-Binomial Bayesian updater — identical to US pipeline.
///
/// Prior: Beta(α₀, β₀) — default Beta(2, 2)
/// Posterior: Beta(α₀ + wins, β₀ + losses)
/// Output: posterior mean, 95% credible interval
use statrs::distribution::{Beta, ContinuousCDF};

pub struct BetaBinomial {
    pub alpha: f64,
    pub beta: f64,
}

pub struct Posterior {
    pub mean: f64,
    pub ci_low: f64,
    pub ci_high: f64,
    pub alpha: f64,
    pub beta: f64,
    pub n: usize,
}

impl BetaBinomial {
    /// Default prior: Beta(2, 2) — mildly informative
    pub fn new() -> Self {
        Self {
            alpha: 2.0,
            beta: 2.0,
        }
    }

    pub fn with_prior(alpha: f64, beta: f64) -> Self {
        Self { alpha, beta }
    }

    /// Update with observed wins and losses, return posterior.
    pub fn update(&self, wins: usize, losses: usize) -> Posterior {
        self.update_weighted(wins as f64, losses as f64)
    }

    /// Update with fractional (EWMA-weighted) wins and losses.
    pub fn update_weighted(&self, wins: f64, losses: f64) -> Posterior {
        let a = self.alpha + wins;
        let b = self.beta + losses;
        let n = (wins + losses).round() as usize;

        let mean = a / (a + b);

        // 95% credible interval via Beta quantile function
        let (ci_low, ci_high) = if n > 0 {
            let dist = Beta::new(a, b).unwrap();
            (dist.inverse_cdf(0.025), dist.inverse_cdf(0.975))
        } else {
            (0.0, 1.0)
        };

        Posterior {
            mean,
            ci_low,
            ci_high,
            alpha: a,
            beta: b,
            n,
        }
    }
}

impl Default for BetaBinomial {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniform_prior() {
        let bb = BetaBinomial::with_prior(1.0, 1.0);
        let p = bb.update(7, 3);
        // Beta(1+7, 1+3) = Beta(8, 4) → mean = 8/12 ≈ 0.6667
        assert!((p.mean - 0.6667).abs() < 0.01);
        assert!(p.ci_low < p.mean);
        assert!(p.ci_high > p.mean);
    }

    #[test]
    fn test_default_prior_no_data() {
        let bb = BetaBinomial::new();
        let p = bb.update(0, 0);
        assert!((p.mean - 0.5).abs() < 1e-10);
    }
}
