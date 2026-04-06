#!/usr/bin/env python3
"""
中金岭南 (000060.SZ) 量价深度分析
Volume-Price Microstructure · Flow · Regime · Divergence
"""

import duckdb
import numpy as np
import pandas as pd
from scipy import stats, signal
from scipy.optimize import minimize
import warnings
warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════════════════
# 0. Load Data
# ═══════════════════════════════════════════════════════════════════════════
conn = duckdb.connect("data/quant_cn.duckdb", read_only=True)
df = conn.execute("""
    SELECT p.trade_date, p.open, p.high, p.low, p.close, p.vol, p.amount,
           p.pre_close, p.pct_chg, p.adj_factor,
           b.pe_ttm, b.pb, b.turnover_rate, b.total_mv, b.circ_mv
    FROM prices p
    LEFT JOIN daily_basic b ON p.ts_code = b.ts_code AND p.trade_date = b.trade_date
    WHERE p.ts_code = '000060.SZ'
    ORDER BY p.trade_date
""").fetchdf()
conn.close()

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.set_index("trade_date")
df["ret"] = df["close"].pct_change()
df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
df["vol_shares"] = df["vol"] * 100        # tushare vol is in 手(100股)
df["amount_yuan"] = df["amount"] * 1000   # tushare amount is in 千元

N = len(df)
current_price = df["close"].iloc[-1]
current_date = df.index[-1]

print("=" * 80)
print(f"  中金岭南 (000060.SZ) 量价深度分析  |  截至 {current_date.date()}")
print(f"  当前价: ¥{current_price:.2f}  |  数据: {N} 交易日")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════════════════
# 1. Volume Profile (成交量分布 / 筹码分布)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§1  Volume Profile — 成交量价格分布 (筹码分布)")
print("─" * 80)
print("  概念: 将历史成交量按价格区间分配,找到成交密集区 (POC/VAH/VAL)")

for lookback_label, lookback in [("近60日", 60), ("近120日", 120), ("全样本", N)]:
    sub = df.tail(lookback).dropna(subset=["close"])
    if len(sub) < 10:
        continue

    price_min, price_max = sub["low"].min(), sub["high"].max()
    n_bins = 50
    bins = np.linspace(price_min, price_max, n_bins + 1)
    bin_centers = (bins[:-1] + bins[1:]) / 2
    vol_profile = np.zeros(n_bins)

    # Distribute each day's volume across its H-L range
    for _, row in sub.iterrows():
        lo, hi, v = row["low"], row["high"], row["vol_shares"]
        if hi <= lo or np.isnan(v):
            continue
        mask = (bin_centers >= lo) & (bin_centers <= hi)
        n_hit = mask.sum()
        if n_hit > 0:
            vol_profile[mask] += v / n_hit

    # Key levels
    poc_idx = np.argmax(vol_profile)
    poc_price = bin_centers[poc_idx]

    # Value Area: 70% of volume around POC
    total_vol = vol_profile.sum()
    target = total_vol * 0.70
    accumulated = vol_profile[poc_idx]
    lo_idx, hi_idx = poc_idx, poc_idx
    while accumulated < target and (lo_idx > 0 or hi_idx < n_bins - 1):
        expand_lo = vol_profile[lo_idx - 1] if lo_idx > 0 else 0
        expand_hi = vol_profile[hi_idx + 1] if hi_idx < n_bins - 1 else 0
        if expand_lo >= expand_hi and lo_idx > 0:
            lo_idx -= 1
            accumulated += expand_lo
        elif hi_idx < n_bins - 1:
            hi_idx += 1
            accumulated += expand_hi
        else:
            lo_idx -= 1
            accumulated += expand_lo

    vah = bin_centers[hi_idx]  # Value Area High
    val = bin_centers[lo_idx]  # Value Area Low

    print(f"\n  ── {lookback_label} ──")
    print(f"    POC (最大成交密集价): ¥{poc_price:.2f} (距当前 {(current_price/poc_price-1)*100:+.1f}%)")
    print(f"    VAH (价值区上沿):     ¥{vah:.2f} (距当前 {(current_price/vah-1)*100:+.1f}%)")
    print(f"    VAL (价值区下沿):     ¥{val:.2f} (距当前 {(current_price/val-1)*100:+.1f}%)")

    # Low Volume Nodes (支撑/阻力稀疏区)
    avg_vol = vol_profile.mean()
    lvn_mask = vol_profile < avg_vol * 0.3
    if lvn_mask.any():
        lvn_prices = bin_centers[lvn_mask]
        near_current = lvn_prices[(lvn_prices < current_price) & (lvn_prices > current_price * 0.8)]
        if len(near_current) > 0:
            print(f"    低成交量节点 (下方LVN): ¥{near_current.min():.2f}~¥{near_current.max():.2f}")
            print(f"    → 价格穿越LVN时往往加速,注意此区间可能快速下穿")

# ═══════════════════════════════════════════════════════════════════════════
# 2. Anchored VWAP + Bands
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§2  锚定 VWAP + 标准差带")
print("─" * 80)
print("  概念: 从关键日期锚定的VWAP,±1/2σ带构成动态支撑阻力")

# Compute VWAP from anchor points
anchors = {
    "近期高点 (2026-01-29)": "2026-01-29",
    "近期低点 (2025-11-24)": "2025-11-24",
    "年初 (2026-01-02)":     "2026-01-02",
    "半年前":                 str((current_date - pd.Timedelta(days=180)).date()),
}

for label, anchor_str in anchors.items():
    anchor_date = pd.Timestamp(anchor_str)
    sub = df.loc[anchor_date:]
    if len(sub) < 5:
        continue

    cum_vol = sub["vol_shares"].cumsum()
    cum_pv = (sub["amount_yuan"]).cumsum()  # amount already = price * vol
    vwap = cum_pv / cum_vol

    # VWAP bands: std of (price - vwap) weighted by volume
    dev = sub["close"] - vwap
    sq_dev_cum = (dev**2 * sub["vol_shares"]).cumsum()
    vwap_std = np.sqrt(sq_dev_cum / cum_vol)

    v = vwap.iloc[-1]
    s = vwap_std.iloc[-1]
    print(f"\n  ── 锚定: {label} ──")
    print(f"    VWAP:    ¥{v:.2f} (距当前 {(current_price/v-1)*100:+.1f}%)")
    print(f"    +1σ带:   ¥{v+s:.2f}")
    print(f"    -1σ带:   ¥{v-s:.2f} (距当前 {(current_price/(v-s)-1)*100:+.1f}%)")
    print(f"    -2σ带:   ¥{v-2*s:.2f} (距当前 {(current_price/(v-2*s)-1)*100:+.1f}%)")

# ═══════════════════════════════════════════════════════════════════════════
# 3. OBV + Chaikin Money Flow + MFI
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§3  资金流向指标 (OBV / CMF / MFI)")
print("─" * 80)

# OBV
df["obv"] = (np.sign(df["ret"].fillna(0)) * df["vol_shares"].fillna(0)).cumsum()
obv_20ma = df["obv"].rolling(20).mean()
obv_current = df["obv"].iloc[-1]
obv_ma = obv_20ma.iloc[-1]
obv_trend = "↑ 资金流入" if obv_current > obv_ma else "↓ 资金流出"
print(f"\n  OBV (On-Balance Volume):")
print(f"    当前 OBV: {obv_current/1e6:.1f}M 股")
print(f"    20日均线: {obv_ma/1e6:.1f}M 股")
print(f"    趋势: {obv_trend}")

# OBV divergence detection
recent_60 = df.tail(60)
price_slope = np.polyfit(range(len(recent_60)), recent_60["close"].values, 1)[0]
obv_slope = np.polyfit(range(len(recent_60)), recent_60["obv"].values, 1)[0]
if price_slope < 0 and obv_slope > 0:
    print(f"    ★ 底背离信号: 价格下跌但OBV上升 → 潜在见底")
elif price_slope > 0 and obv_slope < 0:
    print(f"    ⚠ 顶背离信号: 价格上涨但OBV下降 → 上涨动力不足")
else:
    div_type = "同向" if (price_slope > 0) == (obv_slope > 0) else "弱背离"
    print(f"    OBV与价格: {div_type}")

# Chaikin Money Flow (20日)
clv = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"]).replace(0, np.nan)
clv = clv.fillna(0)
mf_vol = clv * df["vol_shares"]
cmf_20 = mf_vol.rolling(20).sum() / df["vol_shares"].rolling(20).sum()
print(f"\n  Chaikin Money Flow (20日):")
print(f"    CMF = {cmf_20.iloc[-1]:.4f}")
if cmf_20.iloc[-1] > 0.05:
    print(f"    → 资金净流入 (买方主导)")
elif cmf_20.iloc[-1] < -0.05:
    print(f"    → 资金净流出 (卖方主导)")
else:
    print(f"    → 资金流中性")

# Money Flow Index (14日)
tp = (df["high"] + df["low"] + df["close"]) / 3
raw_mf = tp * df["vol_shares"]
mf_pos = raw_mf.where(tp > tp.shift(1), 0)
mf_neg = raw_mf.where(tp < tp.shift(1), 0)
mf_ratio = mf_pos.rolling(14).sum() / mf_neg.rolling(14).sum().replace(0, np.nan)
mfi = 100 - 100 / (1 + mf_ratio)
print(f"\n  Money Flow Index (14日):")
print(f"    MFI = {mfi.iloc[-1]:.1f}")
if mfi.iloc[-1] < 20:
    print(f"    ★ 超卖区间 (MFI < 20) → 量价配合的超卖信号")
elif mfi.iloc[-1] > 80:
    print(f"    ⚠ 超买区间 (MFI > 80)")
else:
    print(f"    正常区间")

# ═══════════════════════════════════════════════════════════════════════════
# 4. Amihud Illiquidity + Kyle's Lambda
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§4  流动性微观结构 (Amihud / Kyle's Lambda / Roll Spread)")
print("─" * 80)

# Amihud illiquidity: |r| / dollar_volume
df["amihud"] = df["log_ret"].abs() / (df["amount_yuan"] / 1e6)  # per million yuan
amihud_20 = df["amihud"].rolling(20).mean()
amihud_60 = df["amihud"].rolling(60).mean()

print(f"\n  Amihud 非流动性指标 (|return| / 成交额):")
print(f"    20日均值: {amihud_20.iloc[-1]:.6f}")
print(f"    60日均值: {amihud_60.iloc[-1]:.6f}")
print(f"    全样本均值: {df['amihud'].mean():.6f}")
amihud_pctile = stats.percentileofscore(df["amihud"].dropna(), df["amihud"].iloc[-1])
print(f"    当前分位: {amihud_pctile:.0f}%")
if amihud_20.iloc[-1] > amihud_60.iloc[-1] * 1.5:
    print(f"    ⚠ 短期流动性恶化 (20日 > 60日 × 1.5)")
elif amihud_20.iloc[-1] < amihud_60.iloc[-1] * 0.7:
    print(f"    ★ 短期流动性改善 (20日 < 60日 × 0.7)")

# Kyle's Lambda: regress |price change| on sqrt(volume)
# ΔP = λ * sqrt(V) + ε
valid = df.dropna(subset=["log_ret", "vol_shares"])
y = valid["log_ret"].abs().values
x = np.sqrt(valid["vol_shares"].values)
# Rolling 60-day Kyle's lambda
kyle_lambdas = []
for i in range(60, len(valid)):
    y_w = y[i-60:i]
    x_w = x[i-60:i]
    slope, _, _, _, _ = stats.linregress(x_w, y_w)
    kyle_lambdas.append(slope)
kyle_series = pd.Series(kyle_lambdas, index=valid.index[60:])
print(f"\n  Kyle's Lambda (价格冲击系数):")
print(f"    当前 λ: {kyle_series.iloc[-1]:.8f}")
print(f"    60日前: {kyle_series.iloc[-60] if len(kyle_series)>60 else float('nan'):.8f}")
print(f"    含义: 每多交易√V股,价格变动 λ×√V")
lambda_pctile = stats.percentileofscore(kyle_series.dropna(), kyle_series.iloc[-1])
print(f"    当前分位: {lambda_pctile:.0f}%")

# Roll's implicit spread
# Roll spread = 2 * sqrt(-cov(r_t, r_{t-1})) when cov < 0
rets_valid = df["log_ret"].dropna()
roll_spreads = []
for i in range(20, len(rets_valid)):
    window = rets_valid.iloc[i-20:i]
    cov = window.autocorr(lag=1) * window.var()
    if cov < 0:
        roll_spreads.append(2 * np.sqrt(-cov))
    else:
        roll_spreads.append(0)
roll_series = pd.Series(roll_spreads, index=rets_valid.index[20:])
print(f"\n  Roll's 隐含价差 (Implied Spread):")
print(f"    当前: {roll_series.iloc[-1]*10000:.1f} bps")
print(f"    20日均值: {roll_series.tail(20).mean()*10000:.1f} bps")
print(f"    含义: 越大=流动性越差,买卖价差越宽")

# ═══════════════════════════════════════════════════════════════════════════
# 5. Volume-Weighted Return Asymmetry
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§5  量价非对称性分析 (Volume-Return Asymmetry)")
print("─" * 80)
print("  概念: 下跌日放量/上涨日缩量 = 抛压; 反之 = 吸筹")

valid_df = df.dropna(subset=["ret", "vol_shares"])

# Up vs down volume
up_days = valid_df[valid_df["ret"] > 0]
down_days = valid_df[valid_df["ret"] < 0]
flat_days = valid_df[valid_df["ret"] == 0]

print(f"\n  全样本统计:")
print(f"    上涨日: {len(up_days)}天, 均量 {up_days['vol_shares'].mean()/1e6:.1f}M")
print(f"    下跌日: {len(down_days)}天, 均量 {down_days['vol_shares'].mean()/1e6:.1f}M")
print(f"    上涨/下跌 成交量比: {up_days['vol_shares'].mean()/down_days['vol_shares'].mean():.3f}")

for lookback_label, lookback in [("近20日", 20), ("近60日", 60)]:
    sub = valid_df.tail(lookback)
    up = sub[sub["ret"] > 0]
    dn = sub[sub["ret"] < 0]
    if len(up) > 0 and len(dn) > 0:
        ratio = up["vol_shares"].mean() / dn["vol_shares"].mean()
        print(f"\n  {lookback_label}:")
        print(f"    上涨{len(up)}天均量: {up['vol_shares'].mean()/1e6:.1f}M")
        print(f"    下跌{len(dn)}天均量: {dn['vol_shares'].mean()/1e6:.1f}M")
        print(f"    量比: {ratio:.3f}")
        if ratio < 0.8:
            print(f"    ★ 下跌放量/上涨缩量 → 抛压沉重")
        elif ratio > 1.2:
            print(f"    ★ 上涨放量/下跌缩量 → 资金吸筹迹象")
        else:
            print(f"    量比接近1.0 → 多空均衡")

# Volume-return correlation dynamics
print(f"\n  量价相关性 (滚动60日):")
vr_corr = df["ret"].rolling(60).corr(df["vol_shares"])
print(f"    当前 corr(ret, vol): {vr_corr.iloc[-1]:.4f}")
print(f"    正相关=追涨杀跌, 负相关=逆向交易/做市主导")

# Asymmetric volume-return correlation
abs_ret_vol_corr = df["ret"].abs().rolling(60).corr(df["vol_shares"])
print(f"    当前 corr(|ret|, vol): {abs_ret_vol_corr.iloc[-1]:.4f}")
print(f"    高值=波动伴随放量(情绪交易), 低值=波动不需要量(流动性差)")

# ═══════════════════════════════════════════════════════════════════════════
# 6. Volume Regime HMM (成交量状态识别)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§6  成交量 HMM 状态 (Volume Regime Detection)")
print("─" * 80)
print("  概念: 用HMM识别 低量(蛰伏) / 中量(正常) / 高量(活跃) 三种状态")

log_vol = np.log(df["vol_shares"].dropna().values + 1)

def fit_hmm_3state(data, max_iter=200, tol=1e-6):
    n = len(data)
    n_states = 3
    # K-means init
    sorted_d = np.sort(data)
    cut = n // 3
    means = np.array([sorted_d[:cut].mean(), sorted_d[cut:2*cut].mean(), sorted_d[2*cut:].mean()])
    stds = np.array([sorted_d[:cut].std(), sorted_d[cut:2*cut].std(), sorted_d[2*cut:].std()])
    stds = np.maximum(stds, 0.01)
    trans = np.full((3, 3), 1/3)
    pi = np.full(3, 1/3)

    for it in range(max_iter):
        B = np.zeros((3, n))
        for s in range(3):
            B[s] = stats.norm.pdf(data, means[s], stds[s]) + 1e-300
        alpha = np.zeros((3, n))
        alpha[:, 0] = pi * B[:, 0]
        s_sum = alpha[:, 0].sum()
        if s_sum > 0: alpha[:, 0] /= s_sum
        for t in range(1, n):
            alpha[:, t] = B[:, t] * (trans.T @ alpha[:, t-1])
            s_sum = alpha[:, t].sum()
            if s_sum > 0: alpha[:, t] /= s_sum
        beta = np.zeros((3, n))
        beta[:, -1] = 1.0
        for t in range(n-2, -1, -1):
            beta[:, t] = trans @ (B[:, t+1] * beta[:, t+1])
            s_sum = beta[:, t].sum()
            if s_sum > 0: beta[:, t] /= s_sum
        gamma = alpha * beta
        g_sum = gamma.sum(axis=0, keepdims=True)
        gamma /= np.maximum(g_sum, 1e-300)

        new_means = np.zeros(3)
        new_stds = np.zeros(3)
        for s in range(3):
            w = gamma[s]
            ws = w.sum()
            if ws > 0:
                new_means[s] = np.dot(w, data) / ws
                new_stds[s] = np.sqrt(np.dot(w, (data - new_means[s])**2) / ws)
                new_stds[s] = max(new_stds[s], 0.01)
        new_trans = np.zeros((3, 3))
        for t in range(n-1):
            for i in range(3):
                for j in range(3):
                    new_trans[i,j] += alpha[i,t]*trans[i,j]*B[j,t+1]*beta[j,t+1]
        for i in range(3):
            rs = new_trans[i].sum()
            if rs > 0: new_trans[i] /= rs
        if np.max(np.abs(new_means - means)) < tol:
            break
        means, stds, trans, pi = new_means, new_stds, new_trans, gamma[:, 0]

    order = np.argsort(means)
    return means[order], stds[order], trans[np.ix_(order,order)], gamma[order], it+1

vol_means, vol_stds, vol_trans, vol_gamma, vol_iters = fit_hmm_3state(log_vol)
state_names = ["低量(蛰伏)", "中量(正常)", "高量(活跃)"]

print(f"\n  收敛: {vol_iters} 次迭代")
for s in range(3):
    daily_vol = np.exp(vol_means[s]) / 1e6
    print(f"  状态{s} {state_names[s]}: 日均量≈{daily_vol:.1f}M股 (log: μ={vol_means[s]:.2f}, σ={vol_stds[s]:.2f})")

print(f"\n  当前状态概率:")
for s in range(3):
    print(f"    P({state_names[s]}) = {vol_gamma[s, -1]:.4f}")
current_vol_state = state_names[np.argmax(vol_gamma[:, -1])]
print(f"    → {current_vol_state}")

# Recent state transitions
print(f"\n  近10日成交量状态序列:")
for i in range(-10, 0):
    date = df.index[i]
    vol = df["vol_shares"].iloc[i]
    state = state_names[np.argmax(vol_gamma[:, i])]
    print(f"    {date.date()}: {vol/1e6:.1f}M股 → {state}")

# ═══════════════════════════════════════════════════════════════════════════
# 7. Wyckoff / Volume Spread Analysis (VSA)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§7  Wyckoff 量价分析 (Volume Spread Analysis)")
print("─" * 80)
print("  概念: 通过K线实体/影线/成交量关系识别机构行为")

df["spread"] = df["high"] - df["low"]
df["body"] = abs(df["close"] - df["open"])
df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
df["vol_ma20"] = df["vol_shares"].rolling(20).mean()
df["spread_ma20"] = df["spread"].rolling(20).mean()

# Classify recent bars
print(f"\n  近20日 Wyckoff 信号:")
recent = df.tail(20).dropna()
for _, row in recent.iterrows():
    date = _.date() if hasattr(_, 'date') else _
    vol_rel = row["vol_shares"] / row["vol_ma20"] if row["vol_ma20"] > 0 else 1
    spread_rel = row["spread"] / row["spread_ma20"] if row["spread_ma20"] > 0 else 1
    is_up = row["close"] > row["open"]
    close_pos = (row["close"] - row["low"]) / row["spread"] if row["spread"] > 0 else 0.5

    signals = []
    # Climactic action
    if vol_rel > 2.0 and not is_up and close_pos < 0.3:
        signals.append("卖出高潮(Selling Climax)")
    elif vol_rel > 2.0 and is_up and close_pos > 0.7:
        signals.append("需求涌入(Demand Surge)")
    # No demand / no supply
    elif vol_rel < 0.5 and spread_rel < 0.5 and is_up:
        signals.append("无需求(No Demand)")
    elif vol_rel < 0.5 and spread_rel < 0.5 and not is_up:
        signals.append("无供给(No Supply)★")
    # Stopping volume
    elif vol_rel > 1.5 and spread_rel < 0.7 and not is_up:
        signals.append("止跌量(Stopping Volume)★")
    elif vol_rel > 1.5 and spread_rel < 0.7 and is_up:
        signals.append("止涨量(Upthrust)")
    # Spring / upthrust
    if row["lower_wick"] > row["body"] * 2 and not is_up and vol_rel > 1.0:
        signals.append("弹簧(Spring)★")
    if row["upper_wick"] > row["body"] * 2 and is_up and vol_rel > 1.0:
        signals.append("假突破(Upthrust)")

    if signals:
        print(f"    {date}: ¥{row['close']:.2f} | 量比={vol_rel:.1f}x | "
              f"{'▲' if is_up else '▼'} | {', '.join(signals)}")

# Accumulation/Distribution score
print(f"\n  累积/派发 (Accumulation/Distribution) 判断:")
recent_60 = df.tail(60).dropna()
# Count Wyckoff accumulation signals
accum_signals = 0
distrib_signals = 0
for _, row in recent_60.iterrows():
    vol_rel = row["vol_shares"] / row["vol_ma20"] if row["vol_ma20"] > 0 else 1
    spread_rel = row["spread"] / row["spread_ma20"] if row["spread_ma20"] > 0 else 1
    is_up = row["close"] > row["open"]
    close_pos = (row["close"] - row["low"]) / row["spread"] if row["spread"] > 0 else 0.5

    # Accumulation signs
    if vol_rel > 1.5 and not is_up and close_pos > 0.5:  # High vol down bar closing near high
        accum_signals += 1
    if vol_rel < 0.5 and not is_up:  # Low vol down bar = no supply
        accum_signals += 1
    if row["lower_wick"] > row["body"] * 1.5:  # Long lower wick
        accum_signals += 1

    # Distribution signs
    if vol_rel > 1.5 and is_up and close_pos < 0.5:  # High vol up bar closing near low
        distrib_signals += 1
    if vol_rel < 0.5 and is_up:  # Low vol up bar = no demand
        distrib_signals += 1
    if row["upper_wick"] > row["body"] * 1.5:  # Long upper wick
        distrib_signals += 1

print(f"    近60日 吸筹信号计数: {accum_signals}")
print(f"    近60日 派发信号计数: {distrib_signals}")
if accum_signals > distrib_signals * 1.3:
    print(f"    ★ 吸筹迹象明显 (accumulation)")
elif distrib_signals > accum_signals * 1.3:
    print(f"    ⚠ 派发迹象明显 (distribution)")
else:
    print(f"    多空交织,暂无明确判断")

# ═══════════════════════════════════════════════════════════════════════════
# 8. Volume-Synchronized Price of Return (VPIN)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§8  VPIN — 量同步知情交易概率")
print("─" * 80)
print("  概念: 将成交量分桶(而非时间),用tick-rule估算知情交易者占比")

# Volume bucketing
bucket_size = df["vol_shares"].mean() * 1  # 1 average day per bucket
valid = df.dropna(subset=["ret", "vol_shares"]).copy()

# Approximate buy/sell classification using tick rule (close vs prev close)
valid["buy_vol"] = valid["vol_shares"].where(valid["ret"] > 0, 0)
valid["sell_vol"] = valid["vol_shares"].where(valid["ret"] < 0, 0)
# Split flat days
flat_mask = valid["ret"] == 0
valid.loc[flat_mask, "buy_vol"] = valid.loc[flat_mask, "vol_shares"] / 2
valid.loc[flat_mask, "sell_vol"] = valid.loc[flat_mask, "vol_shares"] / 2

# Better approximation: use close position within bar
close_pos = (valid["close"] - valid["low"]) / (valid["high"] - valid["low"]).replace(0, 0.5)
close_pos = close_pos.clip(0, 1)
valid["buy_vol_bp"] = valid["vol_shares"] * close_pos
valid["sell_vol_bp"] = valid["vol_shares"] * (1 - close_pos)

# Rolling VPIN (20-day buckets)
window = 20
buy_sum = valid["buy_vol_bp"].rolling(window).sum()
sell_sum = valid["sell_vol_bp"].rolling(window).sum()
total_sum = valid["vol_shares"].rolling(window).sum()
vpin = (buy_sum - sell_sum).abs() / total_sum

print(f"\n  当前 VPIN (20日): {vpin.iloc[-1]:.4f}")
print(f"  60日均值:         {vpin.tail(60).mean():.4f}")
print(f"  全样本均值:       {vpin.mean():.4f}")
vpin_pctile = stats.percentileofscore(vpin.dropna(), vpin.iloc[-1])
print(f"  当前分位:         {vpin_pctile:.0f}%")
if vpin.iloc[-1] > vpin.quantile(0.8):
    print(f"  ⚠ VPIN偏高 → 知情交易活跃 (信息不对称加剧)")
elif vpin.iloc[-1] < vpin.quantile(0.2):
    print(f"  VPIN偏低 → 交易以噪音/散户为主")

# ═══════════════════════════════════════════════════════════════════════════
# 9. Turnover Rate Analysis (换手率分析)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§9  换手率分析 (Turnover Regime)")
print("─" * 80)

turnover = df["turnover_rate"].dropna()
if len(turnover) > 0:
    current_tr = turnover.iloc[-1]
    print(f"\n  当前换手率: {current_tr:.2f}%")
    print(f"  20日均值:   {turnover.tail(20).mean():.2f}%")
    print(f"  60日均值:   {turnover.tail(60).mean():.2f}%")
    print(f"  历史分位:   {stats.percentileofscore(turnover, current_tr):.0f}%")

    # Cumulative turnover (estimate holding period)
    cum_tr_20 = turnover.tail(20).sum()
    cum_tr_60 = turnover.tail(60).sum()
    print(f"\n  20日累计换手: {cum_tr_20:.1f}%")
    print(f"  60日累计换手: {cum_tr_60:.1f}%")
    if cum_tr_20 > 100:
        print(f"  → 20日内流通盘已换手超过1次 (筹码快速交换)")
    if cum_tr_60 > 300:
        print(f"  → 60日内流通盘换手超过3次 (极高活跃度)")

    # 换手率-收益率关系
    tr_ret_corr = df["turnover_rate"].rolling(60).corr(df["ret"])
    print(f"\n  换手率-收益率 60日相关系数: {tr_ret_corr.iloc[-1]:.4f}")
    print(f"    正相关=追涨杀跌交易模式, 负相关=逆向交易模式")

# ═══════════════════════════════════════════════════════════════════════════
# 10. Volume-Time Analysis (量时钟)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§10  量时钟分析 (Volume Clock)")
print("─" * 80)
print("  概念: 以成交量(而非日历时间)为时间轴,消除低活跃期噪音")

valid = df.dropna(subset=["log_ret", "vol_shares"]).copy()
cum_vol = valid["vol_shares"].cumsum()
avg_daily_vol = valid["vol_shares"].mean()

# Create volume-time bars (each bar = 1 average day's volume)
vol_bar_size = avg_daily_vol
vol_bars = []
current_bar = {"open": None, "high": -np.inf, "low": np.inf,
               "close": None, "vol": 0, "n_days": 0, "start": None}

for date, row in valid.iterrows():
    if current_bar["open"] is None:
        current_bar["open"] = row["open"]
        current_bar["start"] = date
    current_bar["high"] = max(current_bar["high"], row["high"])
    current_bar["low"] = min(current_bar["low"], row["low"])
    current_bar["close"] = row["close"]
    current_bar["vol"] += row["vol_shares"]
    current_bar["n_days"] += 1

    if current_bar["vol"] >= vol_bar_size:
        vol_bars.append(current_bar.copy())
        current_bar = {"open": None, "high": -np.inf, "low": np.inf,
                       "close": None, "vol": 0, "n_days": 0, "start": None}

vol_bars_df = pd.DataFrame(vol_bars)
if len(vol_bars_df) > 10:
    vol_bars_df["ret"] = vol_bars_df["close"].pct_change()

    print(f"\n  量时钟统计: {len(vol_bars_df)} 个量柱 (每柱≈{avg_daily_vol/1e6:.1f}M股)")
    print(f"  每柱平均天数: {vol_bars_df['n_days'].mean():.1f} 天")
    print(f"  量柱收益率 std: {vol_bars_df['ret'].std()*100:.2f}%")

    # Recent volume bars
    print(f"\n  最近5个量柱:")
    for _, bar in vol_bars_df.tail(5).iterrows():
        days = bar["n_days"]
        ret = bar["ret"] if not np.isnan(bar["ret"]) else 0
        print(f"    {bar['start'].date()} ({days:.0f}天): "
              f"O={bar['open']:.2f} H={bar['high']:.2f} L={bar['low']:.2f} C={bar['close']:.2f} "
              f"ret={ret*100:+.1f}%")

    # Volume clock acceleration/deceleration
    recent_bars = vol_bars_df.tail(10)
    older_bars = vol_bars_df.tail(20).head(10)
    if len(recent_bars) > 0 and len(older_bars) > 0:
        recent_pace = recent_bars["n_days"].mean()
        older_pace = older_bars["n_days"].mean()
        if recent_pace < older_pace * 0.7:
            print(f"\n  ★ 量时钟加速: 最近柱均{recent_pace:.1f}天 vs 之前{older_pace:.1f}天")
            print(f"    → 交易活跃度显著上升")
        elif recent_pace > older_pace * 1.3:
            print(f"\n  量时钟减速: 最近柱均{recent_pace:.1f}天 vs 之前{older_pace:.1f}天")
            print(f"    → 交易活跃度下降")

# ═══════════════════════════════════════════════════════════════════════════
# 11. Price Impact & Information Content
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§11  价格冲击 & 信息含量分析")
print("─" * 80)

# Permanent vs transient price impact (Hasbrouck decomposition simplified)
# Use autocorrelation structure: transient impact reverses, permanent doesn't
valid = df.dropna(subset=["log_ret"]).copy()
rets = valid["log_ret"]

ac1 = rets.autocorr(lag=1)
ac2 = rets.autocorr(lag=2)
ac5 = rets.autocorr(lag=5)

print(f"\n  收益率自相关:")
print(f"    lag-1: {ac1:.4f} {'(均值回复)' if ac1 < -0.05 else '(趋势)' if ac1 > 0.05 else '(无显著)'}")
print(f"    lag-2: {ac2:.4f}")
print(f"    lag-5: {ac5:.4f}")

# Variance ratio test (Lo-MacKinlay)
# VR(q) = Var(q-period ret) / (q * Var(1-period ret))
for q in [5, 10, 20]:
    q_rets = rets.rolling(q).sum().dropna()
    vr = q_rets.var() / (q * rets.var())
    # Z-statistic under null
    nq = len(rets)
    z = (vr - 1) * np.sqrt(nq * q) / np.sqrt(2 * (2*q - 1) * (q - 1) / (3*q))
    p_val = 2 * (1 - stats.norm.cdf(abs(z)))
    label = ""
    if p_val < 0.05:
        label = " ★均值回复" if vr < 1 else " ★趋势延续"
    print(f"    VR({q:>2}): {vr:.4f} (z={z:.2f}, p={p_val:.4f}){label}")

# Volume-informed price discovery
# How much of price change is "informed" (volume-driven) vs noise
vol_log = np.log(valid["vol_shares"] + 1)
abs_ret = rets.abs()

# Rolling R² of |ret| ~ log(vol)
r2_series = []
window = 60
for i in range(window, len(valid)):
    y = abs_ret.iloc[i-window:i].values
    x = vol_log.iloc[i-window:i].values
    if np.std(x) > 0 and np.std(y) > 0:
        r = np.corrcoef(x, y)[0, 1]
        r2_series.append(r**2)
    else:
        r2_series.append(0)
r2_s = pd.Series(r2_series, index=valid.index[window:])
print(f"\n  价格发现的成交量信息含量 (R² of |ret| ~ log(vol)):")
print(f"    当前 R²: {r2_s.iloc[-1]:.4f}")
print(f"    60日均值: {r2_s.tail(60).mean():.4f}")
print(f"    含义: R²越高=成交量驱动定价越强; 越低=价格由信息/情绪驱动")

# ═══════════════════════════════════════════════════════════════════════════
# 12. Entropy Analysis (量价信息熵)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§12  量价信息熵 (Volume-Price Entropy)")
print("─" * 80)
print("  概念: 高熵=不确定性大(随机); 低熵=有序(趋势或机构控盘)")

# Volume entropy: Shannon entropy of volume distribution
def rolling_entropy(series, window, n_bins=10):
    result = []
    for i in range(window, len(series)):
        w = series.iloc[i-window:i]
        counts, _ = np.histogram(w, bins=n_bins)
        probs = counts / counts.sum()
        probs = probs[probs > 0]
        ent = -np.sum(probs * np.log2(probs))
        result.append(ent)
    return pd.Series(result, index=series.index[window:])

vol_entropy = rolling_entropy(df["vol_shares"].dropna(), 60)
ret_entropy = rolling_entropy(df["log_ret"].dropna(), 60)

if len(vol_entropy) > 0:
    print(f"\n  成交量熵 (60日窗口):")
    print(f"    当前: {vol_entropy.iloc[-1]:.4f} bits")
    print(f"    全样本均值: {vol_entropy.mean():.4f}")
    ent_pctile = stats.percentileofscore(vol_entropy, vol_entropy.iloc[-1])
    print(f"    分位: {ent_pctile:.0f}%")
    if ent_pctile > 80:
        print(f"    → 成交量分布均匀(高熵),交易混乱,缺乏主力")
    elif ent_pctile < 20:
        print(f"    ★ 成交量分布集中(低熵),可能有主力控盘")

if len(ret_entropy) > 0:
    print(f"\n  收益率熵 (60日窗口):")
    print(f"    当前: {ret_entropy.iloc[-1]:.4f} bits")
    ent_pctile = stats.percentileofscore(ret_entropy, ret_entropy.iloc[-1])
    print(f"    分位: {ent_pctile:.0f}%")

# ═══════════════════════════════════════════════════════════════════════════
# 13. 综合量价信号总结
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§13  ★ 综合量价信号总结")
print("─" * 80)

signals = []

# 1. Volume profile position
print(f"\n  信号汇总:")

# OBV divergence
if price_slope < 0 and obv_slope > 0:
    signals.append(("OBV底背离", "★ 看多", "价格下跌但资金在流入"))
elif price_slope < 0 and obv_slope < 0:
    signals.append(("OBV同向下跌", "看空", "量价齐跌"))

# CMF
if cmf_20.iloc[-1] > 0.05:
    signals.append(("CMF > +0.05", "★ 看多", "买方主导"))
elif cmf_20.iloc[-1] < -0.05:
    signals.append(("CMF < -0.05", "看空", "卖方主导"))

# MFI
if mfi.iloc[-1] < 20:
    signals.append(("MFI < 20", "★ 超卖", "量价配合的超卖"))
elif mfi.iloc[-1] < 30:
    signals.append(("MFI < 30", "偏超卖", "接近超卖区"))

# VPIN
if vpin.iloc[-1] > vpin.quantile(0.8):
    signals.append(("VPIN高位", "⚠ 警惕", "知情交易活跃"))

# Volume asymmetry
sub_20 = valid.tail(20)
up_20 = sub_20[sub_20["ret"] > 0]
dn_20 = sub_20[sub_20["ret"] < 0]
if len(up_20) > 0 and len(dn_20) > 0:
    vr = up_20["vol_shares"].mean() / dn_20["vol_shares"].mean()
    if vr > 1.2:
        signals.append(("上涨放量", "★ 看多", "资金在上涨时积极参与"))
    elif vr < 0.8:
        signals.append(("下跌放量", "看空", "抛压沉重"))

# Wyckoff
if accum_signals > distrib_signals * 1.3:
    signals.append(("Wyckoff吸筹", "★ 看多", f"吸筹{accum_signals}次 vs 派发{distrib_signals}次"))
elif distrib_signals > accum_signals * 1.3:
    signals.append(("Wyckoff派发", "看空", f"派发{distrib_signals}次 vs 吸筹{accum_signals}次"))

# Variance ratio
for q in [5, 10, 20]:
    q_rets = rets.rolling(q).sum().dropna()
    vr = q_rets.var() / (q * rets.var())
    if vr < 0.8:
        signals.append((f"VR({q})<0.8", "★ 均值回复", "超跌有修复概率"))
        break

# Liquidity
if amihud_20.iloc[-1] > amihud_60.iloc[-1] * 1.5:
    signals.append(("流动性恶化", "⚠ 警惕", "短期Amihud显著上升"))

print(f"\n  {'信号':<20} {'方向':>8} {'说明'}")
print(f"  {'─'*20} {'─'*8} {'─'*40}")
bullish = 0
bearish = 0
for name, direction, note in signals:
    print(f"  {name:<20} {direction:>8} {note}")
    if "看多" in direction or "超卖" in direction or "均值回复" in direction:
        bullish += 1
    elif "看空" in direction:
        bearish += 1

print(f"\n  多头信号: {bullish}  |  空头信号: {bearish}")
if bullish > bearish + 1:
    print(f"  → 量价结构偏多,存在筑底迹象")
elif bearish > bullish + 1:
    print(f"  → 量价结构偏空,下跌趋势可能延续")
else:
    print(f"  → 多空交织,等待更明确的量价信号")

print("\n" + "=" * 80)
print("  注意: 量价分析为辅助判断工具,不构成投资建议。")
print("=" * 80)
