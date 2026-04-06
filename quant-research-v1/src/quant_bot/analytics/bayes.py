"""
Beta-Binomial Bayesian updater.

Central probability primitive used by momentum_risk and earnings_risk.

Usage:
    posterior = beta_binomial_update(hits=14, observations=20)
    print(posterior.mean)    # 0.6 (shrunk toward 0.5 by prior)
    print(posterior.ci_low)  # 5th percentile
    print(posterior.ci_high) # 95th percentile

Prior Beta(2, 2) has mean 0.5 and is weakly informative — with n=0
observations you get mean=0.5, 90% CI roughly [0.16, 0.84]. As n grows
the data dominates the prior.
"""
from __future__ import annotations

from dataclasses import dataclass

from scipy.stats import beta as beta_dist


@dataclass(frozen=True)
class BetaPosterior:
    hits: int
    observations: int
    alpha_post: float
    beta_post: float
    mean: float       # posterior mean = alpha_post / (alpha_post + beta_post)
    ci_low: float     # 5th percentile of posterior Beta distribution
    ci_high: float    # 95th percentile of posterior Beta distribution

    @property
    def ci_width(self) -> float:
        return self.ci_high - self.ci_low

    @property
    def is_low_sample(self) -> bool:
        return self.observations < 10

    def to_dict(self) -> dict:
        return {
            "hits": self.hits,
            "observations": self.observations,
            "mean": round(self.mean, 4),
            "ci_low": round(self.ci_low, 4),
            "ci_high": round(self.ci_high, 4),
        }


def beta_binomial_update(
    hits: int,
    observations: int,
    alpha0: float = 2.0,
    beta0: float = 2.0,
) -> BetaPosterior:
    """
    Update a Beta(alpha0, beta0) prior with `hits` successes in
    `observations` Bernoulli trials.

    Returns the posterior Beta(alpha0+hits, beta0+observations-hits)
    with mean and 90% credible interval.

    Args:
        hits:         number of successes (0 <= hits <= observations)
        observations: total number of trials
        alpha0:       prior alpha (default 2.0 — weakly informative)
        beta0:        prior beta  (default 2.0 — weakly informative)
    """
    if observations < 0:
        raise ValueError(f"observations must be >= 0, got {observations}")
    if hits < 0 or hits > observations:
        raise ValueError(f"hits must satisfy 0 <= hits <= observations, got {hits}/{observations}")

    alpha_post = alpha0 + hits
    beta_post = beta0 + (observations - hits)
    mean = alpha_post / (alpha_post + beta_post)
    ci_low, ci_high = beta_dist.ppf([0.05, 0.95], alpha_post, beta_post)

    return BetaPosterior(
        hits=hits,
        observations=observations,
        alpha_post=alpha_post,
        beta_post=beta_post,
        mean=float(mean),
        ci_low=float(ci_low),
        ci_high=float(ci_high),
    )


def strength_bucket(posterior: BetaPosterior) -> str:
    """
    Classify a posterior into strong/moderate/weak/inconclusive.

    Uses the 90% credible interval relative to 0.5:
      strong:       CI excludes 0.5 AND nearest CI bound is >2% from 0.5
      moderate:     CI excludes 0.5 (directional, but close to 0.5)
      weak:         CI overlaps 0.5 but mean is >5% from 0.5
      inconclusive: CI straddles 0.5 closely or too few observations
    """
    if posterior.observations < 5:
        return "inconclusive"

    ci_excludes_half = posterior.ci_low > 0.5 or posterior.ci_high < 0.5

    if ci_excludes_half:
        # How far is the nearest CI bound from 0.5?
        if posterior.ci_low > 0.5:
            margin = posterior.ci_low - 0.5
        else:
            margin = 0.5 - posterior.ci_high
        if margin >= 0.02:
            return "strong"
        return "moderate"

    # CI overlaps 0.5 — check if mean is far enough
    if abs(posterior.mean - 0.5) >= 0.05:
        return "weak"

    return "inconclusive"
