#!/usr/bin/env python3
"""
Generate mathematical algorithm diagrams for the quant research system.
Derives the entire screening pipeline from 5 foundational axioms.
Outputs: docs/math_*.png
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import numpy as np

# Color palette
C = {
    "axiom": "#2D3436", "axiom_bg": "#DFE6E9",
    "derive": "#0984E3", "derive_bg": "#D6EAF8",
    "formula": "#6C5CE7", "formula_bg": "#EDE7F6",
    "output": "#00B894", "output_bg": "#D5F5E3",
    "data": "#E17055", "data_bg": "#FDEBD0",
    "arrow": "#636E72", "bg": "#FAFAFA", "text": "#2D3436",
    "accent": "#FD79A8",
}

def _box(ax, x, y, w, h, text, color, bg, fs=9, bold=False):
    box = FancyBboxPatch((x-w/2, y-h/2), w, h, boxstyle="round,pad=0.15",
        facecolor=bg, edgecolor=color, linewidth=1.5, alpha=0.95, zorder=2)
    ax.add_patch(box)
    ax.text(x, y, text, ha="center", va="center", fontsize=fs,
            color=color, fontweight="bold" if bold else "normal", zorder=3,
            fontfamily="monospace")

def _arr(ax, x1, y1, x2, y2, c=None, lw=1.2):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=c or C["arrow"], lw=lw), zorder=1)


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 1: Axiomatic Derivation Tree
# ══════════════════════════════════════════════════════════════════════════
def fig1():
    fig, ax = plt.subplots(figsize=(22, 30))
    ax.set_xlim(0, 22); ax.set_ylim(0, 30)
    ax.set_aspect("equal"); ax.axis("off")
    fig.patch.set_facecolor(C["bg"])

    ax.text(11, 29.2, "Five Axioms -> Complete Screening Pipeline",
            ha="center", fontsize=18, fontweight="bold", color=C["text"],
            fontfamily="monospace")

    # ── Axioms ─────────────────────────────────────────────────────────
    axioms = [
        ("AXIOM 1\nConditional Probability\nP(r>0|state) != P(r>0)\nState matters", 2.5),
        ("AXIOM 2\nBayesian Updating\nBeta(a0+hits, b0+miss)\nEvidence -> belief", 7),
        ("AXIOM 3\nLatent States\nMarket has hidden\nbull/bear regimes", 11.5),
        ("AXIOM 4\nMulti-Source Info\nPrice+Options+News+\nEarnings each signal", 16),
        ("AXIOM 5\nFinite Attention\nMust rank & filter\nBounded rationality", 20),
    ]
    for text, x in axioms:
        _box(ax, x, 27.5, 3.5, 2.0, text, C["axiom"], C["axiom_bg"], fs=8, bold=True)

    # ── Layer 1: Derived Concepts ──────────────────────────────────────
    y1 = 24.0

    _box(ax, 2.5, y1, 4.0, 1.8,
         "State Classification\n--------------------\nautocorr_20 -> regime\n"
         "trending(>0.15)\nmean_rev(<-0.10)\nnoisy(else)",
         C["derive"], C["derive_bg"], fs=7)
    _arr(ax, 2.5, 26.5, 2.5, y1+0.9)

    _box(ax, 7, y1, 4.0, 1.8,
         "9-Cell CPT\n--------------------\nP(5D ret>0 | regime, vol)\n"
         "Beta(2,2) prior\nposterior mean = a/(a+b)",
         C["derive"], C["derive_bg"], fs=7)
    _arr(ax, 2.5, 26.5, 7, y1+0.9)
    _arr(ax, 7, 26.5, 7, y1+0.9)

    _box(ax, 11.5, y1, 4.0, 1.8,
         "Hidden Markov Model\n--------------------\n2-state GaussianHMM\n"
         "features: [SPY ret, VIX]\nBaum-Welch EM (100 iter)",
         C["derive"], C["derive_bg"], fs=7)
    _arr(ax, 11.5, 26.5, 11.5, y1+0.9)

    _box(ax, 16, y1, 4.0, 1.8,
         "5-Dim Signal Extract\n--------------------\nmagnitude (|dp|/ATR)\n"
         "event (days to catalyst)\nmomentum (z-score)\n"
         "options (IV/flow)\ncross-asset (idio ret)",
         C["derive"], C["derive_bg"], fs=6.5)
    _arr(ax, 16, 26.5, 16, y1+0.9)

    _box(ax, 20, y1, 3.5, 1.8,
         "Weighted Scoring\n--------------------\nS = sum(wi * si)\n"
         "si in [0,1]\nsum(wi) = 1\nSaturation: min(raw/T, 1)",
         C["derive"], C["derive_bg"], fs=7)
    _arr(ax, 20, 26.5, 20, y1+0.9)

    # ── Layer 2: Core Formulas ─────────────────────────────────────────
    y2 = 20.5

    _box(ax, 2, y2, 4.2, 2.2,
         "Regime Classification\n====================\n"
         "autocorr = corr(\n  r[t-20:t-1], r[t-19:t])\n\n"
         "vol_bucket:\n  rel_vol = V[t]/avg(V,20)\n  tercile(q33, q67)",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 2.5, y1-0.9, 2, y2+1.1)

    _box(ax, 7, y2, 4.2, 2.2,
         "Beta-Binomial Update\n====================\n"
         "a_post = 2 + hits\nb_post = 2 + (n - hits)\n\n"
         "P = a/(a+b)\nCI_90 = [ppf(0.05), ppf(0.95)]\n"
         "Strength: CI excludes 0.5?",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 7, y1-0.9, 7, y2+1.1)

    _box(ax, 12, y2, 4.5, 2.2,
         "HMM Forward Prediction\n====================\n"
         "pi_tmr = pi_today @ A\nA = transition matrix\n\n"
         "P(r>0) = pi_1*P(r>0|bull)\n       + pi_2*P(r>0|bear)\n"
         "P(r>0|Sj) = empirical freq",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 11.5, y1-0.9, 12, y2+1.1)

    _box(ax, 17.5, y2, 5.0, 2.2,
         "Composite Score Formula\n====================\n"
         "S = 0.30*mag + 0.25*event\n  + 0.20*mom + 0.15*opt\n  + 0.10*cross\n\n"
         "Each si = min(raw/threshold, 1.0)\n"
         "Prevents any single signal > 1",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 16, y1-0.9, 17.5, y2+1.1)
    _arr(ax, 20, y1-0.9, 17.5, y2+1.1)

    # ── Layer 3: Sub-formulas ──────────────────────────────────────────
    y3 = 16.5

    _box(ax, 1.5, y3, 3.8, 2.0,
         "Earnings Drift\n--------------\nexcess = r_stock - r_bench\n"
         "surprise -> quintile\nHierarchical pooling:\n"
         "w_sym*p + 0.3w*p_sec\n+ 0.1w*p_global",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 2, y2-1.1, 1.5, y3+1.0)

    _box(ax, 5.8, y3, 3.8, 2.0,
         "Kalman Filter Beta\n--------------\n"
         "beta_t = beta_{t-1} + eta (Q)\n"
         "r_stock = beta*r_mkt + eps (R)\n"
         "K = P*x/(x^2*P + R)\n"
         "beta = beta_pred + K*innov",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 7, y2-1.1, 5.8, y3+1.0)

    _box(ax, 10.2, y3, 3.8, 2.0,
         "Engle-Granger Coint.\n--------------\n"
         "log(pA) = a + b*log(pB) + e\n"
         "ADF(residuals) -> p-val\n"
         "OU: theta = -ln(b)\n"
         "half_life = ln(2)/theta\n"
         "z = (spread - mu)/sigma",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 7, y2-1.1, 10.2, y3+1.0)

    _box(ax, 14.5, y3, 3.8, 2.0,
         "Options Score Detail\n--------------\n"
         "iv_ratio = IV_daily/hist_vol\n"
         "iv_delta = max(abs, rel)\n"
         "flow = 0.5*vol + 0.3*V/OI\n"
         "     + 0.2*breadth\n"
         "proxy: 50% haircut",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 17.5, y2-1.1, 14.5, y3+1.0)

    _box(ax, 18.8, y3, 3.8, 2.0,
         "Auxiliary Metrics\n--------------\n"
         "VRP = IV^2 - RV^2\n"
         "  RV = std(ln r, 20d)*sqrt252\n\n"
         "Sentiment EWMA z-score:\n"
         "  z = (X_today - EWMAmu)\n"
         "      / EWMAstd",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 17.5, y2-1.1, 18.8, y3+1.0)

    # ── Layer 4: Granger + Covariance ─────────────────────────────────
    y4 = 13.0

    _box(ax, 3, y4, 4.5, 1.8,
         "Granger Causality\n--------------\n"
         "Restricted:  yt = c + sum(gj*y_{t-j})\n"
         "Unrestr: + sum(bj*x_{t-j})\n"
         "F-test on restricted vs full\n"
         "BIC lag select, BH FDR q=0.05",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 1.5, y3-1.0, 3, y4+0.9)

    _box(ax, 8.5, y4, 4.5, 1.8,
         "Ledoit-Wolf Covariance\n--------------\n"
         "S_hat = (1-lam)*S_sample\n"
         "      + lam*S_target\n"
         "lam in [0,1] optimal shrink\n"
         "-> correlation matrix NxN",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 10.2, y3-1.0, 8.5, y4+0.9)

    _box(ax, 14, y4, 4.5, 1.8,
         "Macro Gate Matrix\n--------------\n"
         "3x3: VIX x 10Y-2Y spread\n"
         "  calm(<20)/elevated/panic(>30)\n"
         "  x normal/flat/inverted\n"
         "-> multiplier [0.7 .. 1.3]",
         C["accent"], "#FFF0F5", fs=6.5)
    _arr(ax, 14.5, y3-1.0, 14, y4+0.9)

    _box(ax, 19, y4, 4, 1.8,
         "Brier Calibration\n--------------\n"
         "BS = (1/N)*sum((pi-oi)^2)\n"
         "BS_clim = r*(1-r)\n"
         "BSS = 1 - BS/BS_clim\n"
         "BSS>0 -> beats baseline",
         C["formula"], C["formula_bg"], fs=6.5)
    _arr(ax, 12, y2-1.1, 19, y4+0.9)

    # ── Layer 5: Output Pipeline ──────────────────────────────────────
    y5 = 9.5

    _box(ax, 6, y5, 7, 1.5,
         "PASS 1: ~800 symbols -> 120 candidates\n"
         "composite_score > threshold, macro multiplier applied",
         C["output"], C["output_bg"], fs=8.5, bold=True)

    _box(ax, 16, y5, 7, 1.5,
         "PASS 2: 120 -> Top 30 notable items\n"
         "Options enrichment, re-score, extreme floor >= 0.65",
         C["output"], C["output_bg"], fs=8.5, bold=True)

    for x_src in [3, 8.5, 14, 19]:
        _arr(ax, x_src, y4-0.9, 6, y5+0.75, lw=0.8)
        _arr(ax, x_src, y4-0.9, 16, y5+0.75, lw=0.8)

    _arr(ax, 9.5, y5, 12.5, y5, lw=2.5, c=C["output"])

    # ── Layer 6: Final Outputs ────────────────────────────────────────
    y6 = 7.0

    outs = [
        ("trend_prob\nP(5D r>0|regime,vol)\nBeta posterior mean", 3),
        ("p_upside\nP(5D excess>0)\nhierarchical pooling", 7.5),
        ("HMM regime\nP(bull/bear)\nP(r>0 tomorrow)", 12),
        ("composite_score\nweighted 5-dim\nS in [0, 1]", 16.5),
        ("Brier Score\ncalibration metric\nBSS vs climatology", 20.5),
    ]
    for text, x in outs:
        _box(ax, x, y6, 3.5, 1.5, text, C["output"], C["output_bg"], fs=7.5, bold=True)

    _arr(ax, 6, y5-0.75, 3, y6+0.75)
    _arr(ax, 6, y5-0.75, 7.5, y6+0.75)
    _arr(ax, 16, y5-0.75, 12, y6+0.75)
    _arr(ax, 16, y5-0.75, 16.5, y6+0.75)
    _arr(ax, 16, y5-0.75, 20.5, y6+0.75)

    # ── Constraints Box ───────────────────────────────────────────────
    y7 = 4.5

    _box(ax, 5, y7, 8.5, 2.5,
         "THREE PROBABILITY TYPES (never mix)\n"
         "================================\n"
         "1. Model probability: HMM posterior\n"
         "   (estimation error, calibrate w/ Brier)\n"
         "2. Historical base rate: CPT trend_prob\n"
         "   (frequency-based, Beta-Binomial CI)\n"
         "3. Risk-neutral: options-implied IV\n"
         "   (market price, not real-world prob)",
         C["accent"], "#FFF0F5", fs=7, bold=True)

    _box(ax, 17, y7, 8, 2.5,
         "KEY INVARIANTS\n"
         "================================\n"
         "* All si in [0,1] (saturation clamp)\n"
         "* Beta prior symmetric: a0=b0=2\n"
         "* No signal -> P = 0.5 (prior)\n"
         "* Proxy data -> 50% haircut\n"
         "* P=1.00 / P=0.00 forbidden\n"
         "* Sample n<30 must be flagged\n"
         "* Brier < 0.25 -> better than coin",
         C["accent"], "#FFF0F5", fs=7, bold=True)

    # ── Derivation chain ──────────────────────────────────────────────
    ax.text(11, 1.8,
        "DERIVATION CHAIN:\n"
        "A1 (Conditional Prob) -> State classification -> CPT -> trend_prob\n"
        "A2 (Bayesian Update)  -> Beta-Binomial -> posterior mean -> credible interval\n"
        "A3 (Latent States)    -> HMM -> transition matrix -> forward predict -> Brier calibration\n"
        "A4 (Multi-Source Info) -> 5-dim signals -> weighted scoring -> options/momentum/event/mag/idio\n"
        "A5 (Finite Attention)  -> saturation clamp -> two-pass filter -> Top 30",
        ha="center", va="center", fontsize=8, color=C["text"],
        fontfamily="monospace", linespacing=1.6,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#F8F9FA", edgecolor="#BDC3C7"))

    plt.tight_layout(pad=0.5)
    plt.savefig("docs/math_axiom_tree.png", dpi=180, bbox_inches="tight", facecolor=C["bg"])
    plt.close()
    print("  ok: docs/math_axiom_tree.png")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 2: Probability Pipeline (3 panels)
# ══════════════════════════════════════════════════════════════════════════
def fig2():
    fig, axes = plt.subplots(3, 1, figsize=(20, 22))
    fig.patch.set_facecolor(C["bg"])

    # ── Panel A: CPT ──────────────────────────────────────────────────
    ax = axes[0]
    ax.set_xlim(0, 20); ax.set_ylim(0, 7)
    ax.set_aspect("equal"); ax.axis("off")
    ax.text(10, 6.5, "A. Conditional Probability Table (CPT) -- from Axiom 1 + 2",
            ha="center", fontsize=13, fontweight="bold", color=C["text"], fontfamily="monospace")

    _box(ax, 2.5, 4, 3.5, 1.5,
         "Prior: Beta(2, 2)\nE[P] = 0.5\nWeak, symmetric\n'no information' start",
         C["axiom"], C["axiom_bg"], fs=8, bold=True)

    _box(ax, 7.5, 5, 4, 1.0,
         "Regime: autocorr_20\ntrending(>0.15) / mean_rev(<-0.10) / noisy",
         C["derive"], C["derive_bg"], fs=7.5)
    _box(ax, 7.5, 3, 4, 1.0,
         "Vol bucket: rel_vol = V/avg_20\nlow(<q33) / mid / high(>q67)",
         C["derive"], C["derive_bg"], fs=7.5)

    _arr(ax, 4.25, 4.3, 5.5, 5.0)
    _arr(ax, 4.25, 3.7, 5.5, 3.0)

    _box(ax, 13, 4, 5.5, 2.5,
         "9-Cell CPT Grid\n"
         "         low_vol  mid_vol  high_vol\n"
         "trending   P11      P12      P13\n"
         "noisy      P21      P22      P23\n"
         "mean_rev   P31      P32      P33\n\n"
         "Each cell: Beta(2+hits, 2+miss)",
         C["formula"], C["formula_bg"], fs=7.5)
    _arr(ax, 9.5, 5.0, 10.25, 4.5)
    _arr(ax, 9.5, 3.0, 10.25, 3.5)

    _box(ax, 18.5, 4, 2.5, 1.8,
         "OUTPUT:\ntrend_prob\n= a/(a+b)\n= P(5D r>0\n| regime,vol)",
         C["output"], C["output_bg"], fs=7.5, bold=True)
    _arr(ax, 15.75, 4, 17.25, 4)

    ax.text(10, 1.0,
         "Update rule: observe (regime=trending, vol=high) and 5D return > 0\n"
         "-> hits += 1, n += 1 in cell (trending, high)\n"
         "posterior mean = (2+hits)/(4+n) -> converges to true conditional probability",
         ha="center", fontsize=8, color=C["arrow"], fontfamily="monospace")

    # ── Panel B: HMM ─────────────────────────────────────────────────
    ax = axes[1]
    ax.set_xlim(0, 20); ax.set_ylim(0, 7)
    ax.set_aspect("equal"); ax.axis("off")
    ax.text(10, 6.5, "B. Hidden Markov Model (HMM) -- from Axiom 3",
            ha="center", fontsize=13, fontweight="bold", color=C["text"], fontfamily="monospace")

    _box(ax, 3, 4.5, 2.8, 1.2, "BULL state S1\nmean_ret > 0\nlow VIX",
         "#27AE60", "#D5F5E3", fs=8, bold=True)
    _box(ax, 3, 2.5, 2.8, 1.2, "BEAR state S2\nmean_ret < 0\nhigh VIX",
         "#E74C3C", "#FADBD8", fs=8, bold=True)

    # Self-loop arrows (text only, simplified)
    ax.annotate("P(bull->bull)", xy=(4.8, 5.2), fontsize=7, color="#27AE60",
                fontfamily="monospace")
    ax.annotate("P(bear->bear)", xy=(4.8, 1.8), fontsize=7, color="#E74C3C",
                fontfamily="monospace")
    _arr(ax, 3.5, 3.9, 3.5, 3.1, c="#E74C3C")  # bull->bear
    _arr(ax, 2.5, 3.1, 2.5, 3.9, c="#27AE60")  # bear->bull
    ax.text(1.5, 3.5, "P(switch)", fontsize=6.5, color=C["arrow"], fontfamily="monospace")

    _box(ax, 8.5, 3.5, 4, 1.5,
         "Observations (standardized)\nx1 = (ln_ret_SPY - mu) / sigma\n"
         "x2 = (VIX - mu) / sigma",
         C["data"], C["data_bg"], fs=7.5)
    _arr(ax, 4.4, 3.5, 6.5, 3.5)

    _box(ax, 14, 4.8, 4.5, 1.3,
         "1-Step Forecast\npi_tmr = [p_bull, p_bear] @ A\n"
         "A = [[P11,P12],[P21,P22]]",
         C["formula"], C["formula_bg"], fs=7.5)
    _arr(ax, 10.5, 3.8, 11.75, 4.5)

    _box(ax, 14, 2.5, 5, 1.3,
         "Conditional Return Forecast\nP(r>0) = pi1*P(r>0|bull) + pi2*P(r>0|bear)\n"
         "P(r>0|Sj) = empirical fraction in training",
         C["formula"], C["formula_bg"], fs=7.5)
    _arr(ax, 10.5, 3.2, 11.5, 2.8)

    _box(ax, 19, 3.5, 1.8, 2.5,
         "OUTPUT:\nregime\np_bull\np_bear\nP(r>0)\nBrier",
         C["output"], C["output_bg"], fs=7.5, bold=True)
    _arr(ax, 16.25, 4.8, 18.1, 4.0)
    _arr(ax, 16.5, 2.5, 18.1, 3.0)

    # ── Panel C: Scoring ──────────────────────────────────────────────
    ax = axes[2]
    ax.set_xlim(0, 20); ax.set_ylim(0, 7)
    ax.set_aspect("equal"); ax.axis("off")
    ax.text(10, 6.5, "C. Composite Scoring Function -- from Axiom 4 + 5",
            ha="center", fontsize=13, fontweight="bold", color=C["text"], fontfamily="monospace")

    signals = [
        ("MAGNITUDE\nw=0.30\n|dp|/ATR\nx(1+vol_boost)", 2, "#E74C3C"),
        ("EVENT\nw=0.25\nf(days_to)\nearnings/8-K", 6, "#F39C12"),
        ("MOMENTUM\nw=0.20\n|z|/3.0\ncross-sect z", 10, "#3498DB"),
        ("OPTIONS\nw=0.15\n0.4*iv_r+\n0.3*iv_d+0.3*flow", 14, "#9B59B6"),
        ("CROSS-ASSET\nw=0.10\n|r_stock-r_mkt|\n/ 5%", 18, "#1ABC9C"),
    ]
    for text, x, col in signals:
        _box(ax, x, 4.2, 3.2, 1.8, text, col, "#FAFAFA", fs=7)

    ax.text(10, 2.7,
         "Saturation: si = min(raw / threshold, 1.0)  <--  Axiom 5: no single signal dominates",
         ha="center", fontsize=9, color=C["accent"], fontfamily="monospace", fontweight="bold")

    _box(ax, 10, 1.5, 14, 1.0,
         "S = 0.30*s_mag + 0.25*s_event + 0.20*s_mom + 0.15*s_opt + 0.10*s_cross   in [0, 1]",
         C["output"], C["output_bg"], fs=10, bold=True)

    for x in [2, 6, 10, 14, 18]:
        _arr(ax, x, 3.3, 10, 2.0, lw=1)

    plt.tight_layout(pad=1.0)
    plt.savefig("docs/math_probability_pipeline.png", dpi=180, bbox_inches="tight", facecolor=C["bg"])
    plt.close()
    print("  ok: docs/math_probability_pipeline.png")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 3: Data Flow DAG
# ══════════════════════════════════════════════════════════════════════════
def fig3():
    fig, ax = plt.subplots(figsize=(22, 14))
    ax.set_xlim(0, 22); ax.set_ylim(0, 14)
    ax.set_aspect("equal"); ax.axis("off")
    fig.patch.set_facecolor(C["bg"])

    ax.text(11, 13.5, "Data Flow: Raw Sources -> Analytics -> Scoring -> Payload",
            ha="center", fontsize=16, fontweight="bold", color=C["text"], fontfamily="monospace")

    # Data sources
    sources = [
        ("yfinance\nOHLCV prices", 2),
        ("CBOE\nOptions chains", 5.5),
        ("Finnhub\nNews + Earnings", 9),
        ("FRED\n7 Macro series", 12.5),
        ("SEC EDGAR\n8-K filings", 16),
        ("Polymarket\nCrowd probs", 19.5),
    ]
    for text, x in sources:
        _box(ax, x, 12, 2.8, 1.2, text, C["data"], C["data_bg"], fs=8, bold=True)

    # Analytics
    analytics = [
        ("momentum_risk\n9-cell CPT\ntrend_prob", 1.5, 9),
        ("bayes.py\nBeta(2,2)\nposterior", 4.5, 9),
        ("hmm_regime\n2-state HMM\nP(bull/bear)", 7.5, 9),
        ("earnings_risk\nsurprise Q\nhierarch pool", 10.5, 9),
        ("kalman_beta\ndynamic beta\nstate-space", 13.5, 9),
        ("variance_prem\nVRP=IV^2-RV^2", 16.5, 9),
        ("sentiment\nEWMA z-score\nPC+skew", 19.5, 9),
    ]
    for text, x, y in analytics:
        _box(ax, x, y, 2.6, 1.5, text, C["derive"], C["derive_bg"], fs=7)

    # Source -> analytics arrows
    _arr(ax, 2, 11.4, 1.5, 9.75)
    _arr(ax, 2, 11.4, 4.5, 9.75)
    _arr(ax, 2, 11.4, 7.5, 9.75)
    _arr(ax, 2, 11.4, 13.5, 9.75)
    _arr(ax, 5.5, 11.4, 16.5, 9.75)
    _arr(ax, 5.5, 11.4, 19.5, 9.75)
    _arr(ax, 9, 11.4, 10.5, 9.75)

    # Structural analytics
    struct = [
        ("covariance\nLedoit-Wolf\nshrinkage", 3, 6.5),
        ("pairs\nEngle-Granger\ncoint + OU", 7, 6.5),
        ("granger\nVAR F-test\nlead-lag", 11, 6.5),
    ]
    for text, x, y in struct:
        _box(ax, x, y, 2.8, 1.3, text, C["formula"], C["formula_bg"], fs=7)

    _arr(ax, 2, 11.4, 3, 7.15)
    _arr(ax, 2, 11.4, 7, 7.15)
    _arr(ax, 2, 11.4, 11, 7.15)

    # Scoring
    _box(ax, 15, 6.5, 4.5, 1.5,
         "composite_score\n= 0.30*mag + 0.25*event\n+ 0.20*mom + 0.15*opt + 0.10*cross",
         C["output"], C["output_bg"], fs=7.5, bold=True)

    _box(ax, 20, 6.5, 3, 1.3, "Macro Gate\n3x3 matrix\nVIX x spread",
         C["accent"], "#FFF0F5", fs=7.5)

    for x_src in [1.5, 4.5, 7.5, 10.5, 13.5, 16.5, 19.5]:
        _arr(ax, x_src, 8.25, 15, 7.25, lw=0.8)
    _arr(ax, 12.5, 11.4, 20, 7.15)

    # Filtering
    _box(ax, 8, 4, 5, 1.3,
         "Pass 1: ~800 -> 120 candidates\nFull scan, macro multiplier",
         C["output"], C["output_bg"], fs=8, bold=True)
    _box(ax, 16, 4, 5, 1.3,
         "Pass 2: 120 -> Top 30\nOptions enrichment, repricing floor",
         C["output"], C["output_bg"], fs=8, bold=True)

    _arr(ax, 15, 5.75, 8, 4.65)
    _arr(ax, 20, 5.85, 16, 4.65)
    _arr(ax, 10.5, 4, 13.5, 4, lw=2)
    for x in [3, 7, 11]:
        _arr(ax, x, 5.85, 8, 4.65, lw=0.8)

    # Final
    _box(ax, 11, 2, 10, 1.2,
         "PAYLOAD: trend_prob + p_upside + HMM regime + composite_score + VRP + pairs + beta + options",
         C["output"], C["output_bg"], fs=8.5, bold=True)
    _arr(ax, 8, 3.35, 11, 2.6, lw=1.5)
    _arr(ax, 16, 3.35, 11, 2.6, lw=1.5)

    plt.tight_layout(pad=0.5)
    plt.savefig("docs/math_data_flow.png", dpi=180, bbox_inches="tight", facecolor=C["bg"])
    plt.close()
    print("  ok: docs/math_data_flow.png")


if __name__ == "__main__":
    print("Generating diagrams...")
    fig1()
    fig2()
    fig3()
    print("Done.")
