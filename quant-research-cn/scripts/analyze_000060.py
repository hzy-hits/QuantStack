#!/usr/bin/env python3
"""
中金岭南 (000060.SZ) 深度统计分析
Distribution Fitting · Shadow Option · Low-Point Estimation
"""

import duckdb
import numpy as np
import pandas as pd
from scipy import stats
from scipy.optimize import minimize_scalar, minimize
import warnings
warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════════════════
# 0. Load Data
# ═══════════════════════════════════════════════════════════════════════════
conn = duckdb.connect("data/quant_cn.duckdb", read_only=True)
df = conn.execute("""
    SELECT p.trade_date, p.open, p.high, p.low, p.close, p.vol, p.amount,
           p.adj_factor, p.pct_chg,
           b.pe_ttm, b.pb, b.turnover_rate, b.total_mv, b.circ_mv
    FROM prices p
    LEFT JOIN daily_basic b ON p.ts_code = b.ts_code AND p.trade_date = b.trade_date
    WHERE p.ts_code = '000060.SZ'
    ORDER BY p.trade_date
""").fetchdf()
conn.close()

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.set_index("trade_date")

# Adjusted close
if df["adj_factor"].notna().any():
    latest_adj = df["adj_factor"].dropna().iloc[-1]
    df["adj_close"] = df["close"] * df["adj_factor"] / latest_adj
else:
    df["adj_close"] = df["close"]

# Log returns
df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
df["ret"] = df["close"].pct_change()
df = df.dropna(subset=["log_ret"])

N = len(df)
current_price = df["close"].iloc[-1]
current_date = df.index[-1]

print("=" * 80)
print(f"  中金岭南 (000060.SZ) 深度分析  |  截至 {current_date.date()}")
print(f"  当前价格: ¥{current_price:.2f}  |  数据量: {N} 个交易日")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════════════════
# 1. 基础统计
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§1  基础统计描述")
print("─" * 80)

rets = df["log_ret"].values
print(f"  日均收益率 (log):  {rets.mean()*100:.4f}%")
print(f"  日波动率 (std):    {rets.std()*100:.4f}%")
print(f"  年化收益率:        {rets.mean()*252*100:.2f}%")
print(f"  年化波动率:        {rets.std()*np.sqrt(252)*100:.2f}%")
print(f"  偏度 (Skewness):   {stats.skew(rets):.4f}")
print(f"  峰度 (Kurtosis):   {stats.kurtosis(rets):.4f} (excess)")
print(f"  最大日跌幅:        {rets.min()*100:.2f}%")
print(f"  最大日涨幅:        {rets.max()*100:.2f}%")

# Historical price range
print(f"\n  价格范围:")
print(f"    历史最高: ¥{df['high'].max():.2f} ({df['high'].idxmax().date()})")
print(f"    历史最低: ¥{df['low'].min():.2f} ({df['low'].idxmin().date()})")
print(f"    当前距最高: {(current_price/df['high'].max()-1)*100:.1f}%")
print(f"    当前距最低: {(current_price/df['low'].min()-1)*100:.1f}%")

# Rolling realized vol
for w in [20, 60, 120]:
    rv = df["log_ret"].rolling(w).std() * np.sqrt(252)
    if rv.dropna().shape[0] > 0:
        print(f"  {w}日年化波动率:    {rv.iloc[-1]*100:.2f}%")

# ═══════════════════════════════════════════════════════════════════════════
# 2. 分布拟合与检验
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§2  收益率分布拟合与检验 (Goodness-of-Fit)")
print("─" * 80)

distributions = {
    "Normal":        stats.norm,
    "Student-t":     stats.t,
    "Skew-Normal":   stats.skewnorm,
    "Laplace":       stats.laplace,
    "Logistic":      stats.logistic,
    "Gen. Hyperbolic (NIG)": stats.norminvgauss,
    "Gen. Extreme Value": stats.genextreme,
}

fit_results = {}
print(f"\n  {'分布':<28} {'AIC':>10} {'BIC':>10} {'KS p值':>10} {'AD stat':>10}")
print(f"  {'─'*28} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")

for name, dist in distributions.items():
    try:
        params = dist.fit(rets)
        # Log-likelihood
        ll = np.sum(dist.logpdf(rets, *params))
        k = len(params)
        aic = 2 * k - 2 * ll
        bic = k * np.log(N) - 2 * ll

        # KS test
        ks_stat, ks_p = stats.kstest(rets, dist.cdf, args=params)

        # Anderson-Darling (only for norm)
        if name == "Normal":
            ad_result = stats.anderson(rets, "norm")
            ad_stat = ad_result.statistic
        else:
            # Manual AD
            cdf_vals = np.sort(dist.cdf(rets, *params))
            cdf_vals = np.clip(cdf_vals, 1e-10, 1 - 1e-10)
            n = len(cdf_vals)
            i = np.arange(1, n + 1)
            ad_stat = -n - (1/n) * np.sum((2*i - 1) * (np.log(cdf_vals) + np.log(1 - cdf_vals[::-1])))

        fit_results[name] = {
            "params": params, "aic": aic, "bic": bic,
            "ks_p": ks_p, "ad_stat": ad_stat, "dist": dist
        }

        print(f"  {name:<28} {aic:>10.1f} {bic:>10.1f} {ks_p:>10.4f} {ad_stat:>10.4f}")
    except Exception as e:
        print(f"  {name:<28} {'FAILED':>10}  ({str(e)[:40]})")

# Best fit by BIC
if fit_results:
    best_name = min(fit_results, key=lambda k: fit_results[k]["bic"])
    print(f"\n  ★ 最佳拟合 (BIC): {best_name}")
    best = fit_results[best_name]
    print(f"    参数: {best['params']}")

# Jarque-Bera test for normality
jb_stat, jb_p = stats.jarque_bera(rets)
print(f"\n  Jarque-Bera 正态性检验: stat={jb_stat:.2f}, p={jb_p:.6f}")
if jb_p < 0.05:
    print("  → 拒绝正态分布假设 (p<0.05), 收益率呈厚尾分布")

# Shapiro-Wilk (subsample if >5000)
sw_stat, sw_p = stats.shapiro(rets[:5000])
print(f"  Shapiro-Wilk 检验:       stat={sw_stat:.6f}, p={sw_p:.6f}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. 尾部风险分析 (Tail Risk)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§3  尾部风险分析 (VaR / CVaR / EVT)")
print("─" * 80)

# Historical VaR / CVaR
for conf in [0.95, 0.99]:
    var_h = np.percentile(rets, (1 - conf) * 100)
    cvar_h = rets[rets <= var_h].mean()
    print(f"\n  Historical {conf*100:.0f}% VaR:  {var_h*100:.2f}% (日)  → 价格 ¥{current_price * np.exp(var_h):.2f}")
    print(f"  Historical {conf*100:.0f}% CVaR: {cvar_h*100:.2f}% (日)  → 价格 ¥{current_price * np.exp(cvar_h):.2f}")

# Parametric VaR (best-fit distribution)
if fit_results and best_name in fit_results:
    best_d = fit_results[best_name]
    dist_obj = best_d["dist"]
    params = best_d["params"]
    for conf in [0.95, 0.99]:
        var_p = dist_obj.ppf(1 - conf, *params)
        print(f"  Parametric ({best_name}) {conf*100:.0f}% VaR: {var_p*100:.2f}% → ¥{current_price * np.exp(var_p):.2f}")

# Extreme Value Theory - Block Maxima
print(f"\n  极值理论 (EVT) — Generalized Pareto Distribution:")
# Use negative returns for loss tail
losses = -rets
threshold = np.percentile(losses, 90)  # Top 10% losses
exceedances = losses[losses > threshold] - threshold
if len(exceedances) > 20:
    gpd_params = stats.genpareto.fit(exceedances, floc=0)
    xi, loc, sigma = gpd_params
    print(f"    GPD shape (ξ): {xi:.4f}, scale (σ): {sigma:.6f}")
    print(f"    尾部指数: {'厚尾 (heavy tail)' if xi > 0 else '薄尾 (thin tail)'}")

    # EVT-based VaR
    n_total = len(losses)
    n_exceed = len(exceedances)
    for conf in [0.95, 0.99, 0.999]:
        q = 1 - conf
        var_evt = threshold + (sigma / xi) * ((n_total / n_exceed * q) ** (-xi) - 1)
        print(f"    EVT {conf*100:.1f}% VaR: {var_evt*100:.2f}% → 价格 ¥{current_price * np.exp(-var_evt):.2f}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. 影子期权分析 (Shadow Option / Implied Floor)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§4  影子期权分析 (Shadow / Synthetic Option Pricing)")
print("─" * 80)

sigma_annual = rets.std() * np.sqrt(252)
r = 0.015  # 无风险利率 ~1.5% (中国国债)

def black_scholes_put(S, K, T, r, sigma):
    """Black-Scholes European put price."""
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    put = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)
    return put

def black_scholes_greeks(S, K, T, r, sigma):
    """Compute put Greeks."""
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    delta = stats.norm.cdf(d1) - 1  # put delta
    gamma = stats.norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * stats.norm.pdf(d1) * np.sqrt(T) / 100  # per 1% vol
    theta = (-(S * stats.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * stats.norm.cdf(-d2)) / 252
    return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta}

S = current_price

# Use multiple vol estimates
rv_20 = df["log_ret"].rolling(20).std().iloc[-1] * np.sqrt(252)
rv_60 = df["log_ret"].rolling(60).std().iloc[-1] * np.sqrt(252)
rv_120 = df["log_ret"].rolling(120).std().iloc[-1] * np.sqrt(252)

vol_estimates = {
    "20日RV": rv_20,
    "60日RV": rv_60,
    "120日RV": rv_120,
    "全样本": sigma_annual,
}

print(f"\n  当前价 S = ¥{S:.2f}")
print(f"  无风险利率 r = {r*100:.1f}%")

for vol_name, sigma_est in vol_estimates.items():
    print(f"\n  ── {vol_name} σ = {sigma_est*100:.1f}% ──")
    for T_months, T in [(1, 1/12), (3, 3/12), (6, 6/12), (12, 1.0)]:
        # ATM put
        put_atm = black_scholes_put(S, S, T, r, sigma_est)

        # OTM puts at various strikes
        for pct in [0.90, 0.85, 0.80]:
            K = S * pct
            put_price = black_scholes_put(S, K, T, r, sigma_est)
            greeks = black_scholes_greeks(S, K, T, r, sigma_est)
            print(f"    T={T_months:>2}M | K=¥{K:.2f} ({pct*100:.0f}%) | "
                  f"Put=¥{put_price:.3f} | Δ={greeks['delta']:.3f} | "
                  f"Γ={greeks['gamma']:.4f}")

# Break-even floors: where put cost equals premium collected
print(f"\n  ── 影子看跌期权隐含底部 (Implied Floor) ──")
print(f"  概念: 假设持有者用{sigma_annual*100:.0f}%波动率对冲, 不同期限的保护性看跌期权暗示的底部价位")
for T_months, T in [(1, 1/12), (3, 3/12), (6, 6/12), (12, 1.0)]:
    # Find K where P(K)/S = cost_threshold (e.g., 5% premium)
    # Also find the delta-neutral hedge point

    # 1-sigma downside
    floor_1s = S * np.exp(-sigma_annual * np.sqrt(T))
    floor_2s = S * np.exp(-2 * sigma_annual * np.sqrt(T))
    floor_3s = S * np.exp(-3 * sigma_annual * np.sqrt(T))

    # Probability of touching these levels (barrier approximation)
    p_1s = 2 * stats.norm.cdf(-sigma_annual * np.sqrt(T) / (sigma_annual * np.sqrt(T)))  # ~31.7%
    # More accurate: P(min(S_t) < K) for GBM
    def prob_touch(K, S, mu, sigma, T):
        """Probability that GBM touches K before T (first passage time)."""
        if K >= S:
            return 1.0
        a = np.log(K / S)
        b = (mu - 0.5 * sigma**2) * T
        c = sigma * np.sqrt(T)
        if c == 0:
            return 0.0
        p = stats.norm.cdf((a - b) / c) + np.exp(2 * a * (mu - 0.5*sigma**2) / sigma**2) * stats.norm.cdf((a + b) / c)
        return min(p, 1.0)

    mu = rets.mean() * 252
    p1 = prob_touch(floor_1s, S, mu, sigma_annual, T)
    p2 = prob_touch(floor_2s, S, mu, sigma_annual, T)
    p3 = prob_touch(floor_3s, S, mu, sigma_annual, T)

    print(f"  T={T_months:>2}M: 1σ底=¥{floor_1s:.2f}(P触及={p1:.1%}) | "
          f"2σ底=¥{floor_2s:.2f}(P={p2:.1%}) | "
          f"3σ底=¥{floor_3s:.2f}(P={p3:.1%})")

# ═══════════════════════════════════════════════════════════════════════════
# 5. HMM 状态分析 (2-state regime)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§5  隐马尔可夫模型 (HMM) 状态分析")
print("─" * 80)

def fit_hmm(data, n_states=2, max_iter=200, tol=1e-6):
    """Fit Gaussian HMM via Baum-Welch."""
    n = len(data)

    # K-means init
    sorted_data = np.sort(data)
    cut = n // n_states
    means = np.array([sorted_data[i*cut:(i+1)*cut].mean() for i in range(n_states)])
    stds = np.array([sorted_data[i*cut:(i+1)*cut].std() for i in range(n_states)])
    stds = np.maximum(stds, 1e-6)

    # Uniform transitions and start
    trans = np.full((n_states, n_states), 1/n_states)
    pi = np.full(n_states, 1/n_states)

    for iteration in range(max_iter):
        # E-step: forward-backward
        # Emission probabilities
        B = np.zeros((n_states, n))
        for s in range(n_states):
            B[s] = stats.norm.pdf(data, means[s], stds[s]) + 1e-300

        # Forward
        alpha = np.zeros((n_states, n))
        alpha[:, 0] = pi * B[:, 0]
        alpha[:, 0] /= alpha[:, 0].sum() + 1e-300
        scale = np.zeros(n)
        scale[0] = alpha[:, 0].sum()

        for t in range(1, n):
            alpha[:, t] = B[:, t] * (trans.T @ alpha[:, t-1])
            s = alpha[:, t].sum()
            if s > 0:
                alpha[:, t] /= s
            scale[t] = s

        # Backward
        beta = np.zeros((n_states, n))
        beta[:, -1] = 1.0
        for t in range(n-2, -1, -1):
            beta[:, t] = trans @ (B[:, t+1] * beta[:, t+1])
            s = beta[:, t].sum()
            if s > 0:
                beta[:, t] /= s

        # Posteriors
        gamma = alpha * beta
        gamma_sum = gamma.sum(axis=0, keepdims=True)
        gamma_sum = np.maximum(gamma_sum, 1e-300)
        gamma /= gamma_sum

        # M-step
        new_means = np.zeros(n_states)
        new_stds = np.zeros(n_states)
        new_pi = gamma[:, 0]

        for s in range(n_states):
            w = gamma[s]
            ws = w.sum()
            if ws > 0:
                new_means[s] = np.dot(w, data) / ws
                new_stds[s] = np.sqrt(np.dot(w, (data - new_means[s])**2) / ws)
                new_stds[s] = max(new_stds[s], 1e-6)

        # Transition update
        new_trans = np.zeros((n_states, n_states))
        for t in range(n-1):
            for i in range(n_states):
                for j in range(n_states):
                    new_trans[i, j] += alpha[i, t] * trans[i, j] * B[j, t+1] * beta[j, t+1]
        for i in range(n_states):
            row_sum = new_trans[i].sum()
            if row_sum > 0:
                new_trans[i] /= row_sum

        # Convergence check
        if np.max(np.abs(new_means - means)) < tol:
            break

        means, stds, trans, pi = new_means, new_stds, new_trans, new_pi

    # Sort states: state 0 = lower mean (bearish)
    order = np.argsort(means)
    means = means[order]
    stds = stds[order]
    trans = trans[np.ix_(order, order)]
    gamma = gamma[order]

    return means, stds, trans, gamma, iteration+1

hmm_means, hmm_stds, hmm_trans, hmm_gamma, hmm_iters = fit_hmm(rets)

print(f"  收敛迭代数: {hmm_iters}")
print(f"\n  状态参数:")
state_labels = ["熊市/下跌", "牛市/上涨"]
for s in range(2):
    ann_mu = hmm_means[s] * 252
    ann_sig = hmm_stds[s] * np.sqrt(252)
    print(f"    状态 {s} ({state_labels[s]}):")
    print(f"      日均收益: {hmm_means[s]*100:.4f}% (年化 {ann_mu*100:.1f}%)")
    print(f"      日波动率: {hmm_stds[s]*100:.4f}% (年化 {ann_sig*100:.1f}%)")

print(f"\n  转移矩阵:")
print(f"    P(熊→熊)={hmm_trans[0,0]:.4f}  P(熊→牛)={hmm_trans[0,1]:.4f}")
print(f"    P(牛→熊)={hmm_trans[1,0]:.4f}  P(牛→牛)={hmm_trans[1,1]:.4f}")

# Current state
p_bear = hmm_gamma[0, -1]
p_bull = hmm_gamma[1, -1]
print(f"\n  当前状态概率:")
print(f"    P(熊市) = {p_bear:.4f}")
print(f"    P(牛市) = {p_bull:.4f}")
current_state = "熊市/下跌" if p_bear > p_bull else "牛市/上涨"
print(f"    → 当前判定: {current_state}")

# Expected duration in current state
if p_bear > p_bull:
    exp_duration = 1 / (1 - hmm_trans[0, 0])
    bear_vol = hmm_stds[0] * np.sqrt(252)
    bear_mu = hmm_means[0] * 252
    print(f"    熊市预期持续: {exp_duration:.0f} 个交易日 ({exp_duration/21:.1f} 个月)")
    print(f"    熊市条件下的底部估计 (1/2/3σ from current):")
    for nsig in [1, 2, 3]:
        for T_months, T in [(1, 21), (3, 63), (6, 126)]:
            floor = current_price * np.exp(bear_mu * T/252 - nsig * hmm_stds[0] * np.sqrt(T))
            print(f"      {nsig}σ, {T_months}M: ¥{floor:.2f}")
else:
    exp_duration = 1 / (1 - hmm_trans[1, 1])
    print(f"    牛市预期持续: {exp_duration:.0f} 个交易日")

# ═══════════════════════════════════════════════════════════════════════════
# 6. 支撑位分析 (Technical Support Levels)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§6  技术支撑位分析")
print("─" * 80)

# Key moving averages
for w in [5, 10, 20, 60, 120, 250]:
    if len(df) >= w:
        ma = df["close"].rolling(w).mean().iloc[-1]
        dist = (current_price / ma - 1) * 100
        print(f"  MA{w:>3}: ¥{ma:.2f} (偏离 {dist:+.1f}%)")

# Fibonacci retracement from recent high to recent low
recent = df.tail(120)
swing_high = recent["high"].max()
swing_low = recent["low"].min()
swing_high_date = recent["high"].idxmax()
swing_low_date = recent["low"].idxmin()

print(f"\n  近120日波段: 高=¥{swing_high:.2f}({swing_high_date.date()}) 低=¥{swing_low:.2f}({swing_low_date.date()})")
fib_levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
rng = swing_high - swing_low
print(f"  Fibonacci 回撤位 (从高到低):")
for f in fib_levels:
    level = swing_high - rng * f
    dist = (current_price / level - 1) * 100
    marker = " ◄ 当前" if abs(dist) < 2 else ""
    print(f"    {f*100:>5.1f}%: ¥{level:.2f} ({dist:+.1f}%){marker}")

# Volume-weighted price levels (VWAP-like support)
df["vwap_cum"] = (df["amount"] * 1000).cumsum() / (df["vol"] * 100).cumsum()
print(f"\n  累计VWAP: ¥{df['vwap_cum'].iloc[-1]:.2f}")

# ═══════════════════════════════════════════════════════════════════════════
# 7. 估值底分析 (Valuation Floor)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§7  估值底分析 (PB/PE Implied Floor)")
print("─" * 80)

pb = df["pb"].dropna()
pe = df["pe_ttm"].dropna()

if len(pb) > 0:
    current_pb = pb.iloc[-1]
    pb_pctiles = [5, 10, 25, 50]
    print(f"  当前 PB: {current_pb:.3f}")
    print(f"  PB 分位数 (近{len(pb)}日):")
    for p in pb_pctiles:
        pb_val = np.percentile(pb, p)
        implied_price = current_price * (pb_val / current_pb) if current_pb != 0 else 0
        print(f"    {p:>3}%分位: PB={pb_val:.3f} → 隐含价格 ¥{implied_price:.2f}")

if len(pe) > 0:
    pe_valid = pe[(pe > 0) & (pe < 200)]
    if len(pe_valid) > 0:
        current_pe = pe.iloc[-1]
        print(f"\n  当前 PE_TTM: {current_pe:.2f}")
        print(f"  PE 分位数 (有效数据{len(pe_valid)}日):")
        for p in pb_pctiles:
            pe_val = np.percentile(pe_valid, p)
            implied_price = current_price * (pe_val / current_pe) if current_pe != 0 else 0
            print(f"    {p:>3}%分位: PE={pe_val:.2f} → 隐含价格 ¥{implied_price:.2f}")

# ═══════════════════════════════════════════════════════════════════════════
# 8. Monte Carlo 模拟底部分布
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§8  Monte Carlo 模拟 — 底部价格分布")
print("─" * 80)

np.random.seed(42)
n_sims = 50000

# Use best-fit distribution for simulation
if best_name == "Student-t":
    df_t, loc_t, scale_t = fit_results["Student-t"]["params"]
    sim_label = f"Student-t(df={df_t:.1f})"
elif best_name == "Skew-Normal":
    a_sn, loc_sn, scale_sn = fit_results["Skew-Normal"]["params"]
    sim_label = f"Skew-Normal(a={a_sn:.2f})"
else:
    sim_label = "Empirical Bootstrap"

print(f"  模拟方法: {sim_label}")
print(f"  模拟次数: {n_sims:,}")

for T_months, T_days in [(1, 21), (3, 63), (6, 126), (12, 252)]:
    # Simulate paths
    if best_name in fit_results:
        best_d = fit_results[best_name]
        sim_rets = best_d["dist"].rvs(*best_d["params"], size=(n_sims, T_days))
    else:
        # Bootstrap
        sim_rets = np.random.choice(rets, size=(n_sims, T_days), replace=True)

    # Cumulative returns
    cum_rets = np.cumsum(sim_rets, axis=1)

    # Terminal prices
    terminal_prices = current_price * np.exp(cum_rets[:, -1])

    # Minimum prices along each path
    running_min = np.minimum.accumulate(cum_rets, axis=1)
    min_prices = current_price * np.exp(running_min[:, -1])

    print(f"\n  ── {T_months}个月 ({T_days}日) ──")
    print(f"    终端价格分布:")
    for p in [5, 10, 25, 50, 75]:
        prc = np.percentile(terminal_prices, p)
        print(f"      {p:>3}%分位: ¥{prc:.2f} ({(prc/current_price-1)*100:+.1f}%)")

    print(f"    路径最低价分布 (★关键):")
    for p in [5, 10, 25, 50]:
        prc = np.percentile(min_prices, p)
        print(f"      {p:>3}%分位: ¥{prc:.2f} ({(prc/current_price-1)*100:+.1f}%)")

    # Probability of falling below key levels
    key_levels = [6.0, 5.5, 5.0, 4.5, 4.0]
    for level in key_levels:
        if level < current_price:
            p_below = (min_prices < level).mean()
            if p_below > 0.001:
                print(f"      P(触及¥{level:.1f}) = {p_below:.1%}")

# ═══════════════════════════════════════════════════════════════════════════
# 9. 综合低点估算
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§9  ★ 综合低点估算汇总")
print("─" * 80)

estimates = []

# From VaR
var_99 = np.percentile(rets, 1)
estimates.append(("99% Historical VaR (日)", current_price * np.exp(var_99)))

# From EVT
if len(exceedances) > 20:
    var_999_evt = threshold + (sigma / xi) * ((n_total / n_exceed * 0.001) ** (-xi) - 1)
    estimates.append(("99.9% EVT VaR (日)", current_price * np.exp(-var_999_evt)))

# From HMM bear state
if p_bear > 0.5:
    bear_floor_3m = current_price * np.exp(hmm_means[0] * 63 - 2 * hmm_stds[0] * np.sqrt(63))
    estimates.append(("HMM 熊市 2σ (3M)", bear_floor_3m))

# From Monte Carlo
mc_3m_10pct = np.percentile(min_prices, 10)  # last sim was 12M, recalc 3M
sim_rets_3m = fit_results[best_name]["dist"].rvs(*fit_results[best_name]["params"], size=(n_sims, 63))
cum_3m = np.cumsum(sim_rets_3m, axis=1)
min_3m = current_price * np.exp(np.minimum.accumulate(cum_3m, axis=1)[:, -1])
estimates.append(("Monte Carlo 3M 10%分位", np.percentile(min_3m, 10)))
estimates.append(("Monte Carlo 3M 25%分位", np.percentile(min_3m, 25)))

# From PB floor
if len(pb) > 0 and current_pb != 0:
    pb_10 = np.percentile(pb, 10)
    estimates.append(("PB 10%分位", current_price * pb_10 / current_pb))

# From Fibonacci
estimates.append(("Fibonacci 78.6%回撤", swing_high - rng * 0.786))
estimates.append(("Fibonacci 100%回撤", swing_low))

# From shadow option
floor_1s_3m = current_price * np.exp(-sigma_annual * np.sqrt(0.25))
floor_2s_3m = current_price * np.exp(-2 * sigma_annual * np.sqrt(0.25))
estimates.append(("影子期权 1σ (3M)", floor_1s_3m))
estimates.append(("影子期权 2σ (3M)", floor_2s_3m))

# Sort by price
estimates.sort(key=lambda x: x[1], reverse=True)

print(f"\n  {'模型/方法':<30} {'估算价格':>10} {'距当前':>10}")
print(f"  {'─'*30} {'─'*10} {'─'*10}")
for name, price in estimates:
    dist = (price / current_price - 1) * 100
    print(f"  {name:<30} ¥{price:>8.2f} {dist:>+9.1f}%")

print(f"\n  当前价: ¥{current_price:.2f}")
print(f"\n  ★ 核心结论:")
prices_only = [p for _, p in estimates]
median_floor = np.median(prices_only)
print(f"    各模型估算底部中位数: ¥{median_floor:.2f} ({(median_floor/current_price-1)*100:+.1f}%)")
conservative = np.percentile(prices_only, 25)
print(f"    保守估计 (25%分位):   ¥{conservative:.2f} ({(conservative/current_price-1)*100:+.1f}%)")
extreme = min(prices_only)
print(f"    极端情况 (最低估计):  ¥{extreme:.2f} ({(extreme/current_price-1)*100:+.1f}%)")

print("\n" + "=" * 80)
print("  注意: 以上均为概率估算,不构成任何投资建议。")
print("  所有模型假设历史分布对未来有一定预测力,市场可能出现超预期事件。")
print("=" * 80)
