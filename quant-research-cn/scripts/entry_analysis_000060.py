#!/usr/bin/env python3
"""
中金岭南 (000060.SZ) 入场点位分析
Entry Point Analysis · Risk-Reward · Optimal Zones
"""

import duckdb
import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import argrelextrema
import warnings
warnings.filterwarnings("ignore")

conn = duckdb.connect("data/quant_cn.duckdb", read_only=True)
df = conn.execute("""
    SELECT p.trade_date, p.open, p.high, p.low, p.close, p.vol, p.amount,
           p.pre_close, p.pct_chg,
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
df["vol_shares"] = df["vol"] * 100
df["amount_yuan"] = df["amount"] * 1000
df["atr"] = pd.concat([
    df["high"] - df["low"],
    (df["high"] - df["close"].shift(1)).abs(),
    (df["low"] - df["close"].shift(1)).abs()
], axis=1).max(axis=1).rolling(14).mean()

N = len(df)
S = df["close"].iloc[-1]

print("=" * 80)
print(f"  中金岭南 (000060.SZ) 入场点位分析  |  截至 {df.index[-1].date()}")
print(f"  当前价: ¥{S:.2f}  |  14日ATR: ¥{df['atr'].iloc[-1]:.2f} ({df['atr'].iloc[-1]/S*100:.1f}%)")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════════════════
# 1. 历史关键价位 (Local Extrema Detection)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§1  历史关键转折点 (Support/Resistance via Local Extrema)")
print("─" * 80)

# Find local minima/maxima at different scales
for order_label, order in [("短期(10日)", 10), ("中期(20日)", 20), ("长期(40日)", 40)]:
    close = df["close"].values
    low = df["low"].values
    high = df["high"].values

    local_min_idx = argrelextrema(low, np.less_equal, order=order)[0]
    local_max_idx = argrelextrema(high, np.greater_equal, order=order)[0]

    # Filter to recent
    recent_min = [(df.index[i], low[i]) for i in local_min_idx if i > N - 250]
    recent_max = [(df.index[i], high[i]) for i in local_max_idx if i > N - 250]

    print(f"\n  ── {order_label} 级别 ──")
    if recent_min:
        # Sort by price, show nearest to current
        recent_min.sort(key=lambda x: x[1])
        print(f"    支撑位 (近1年局部低点):")
        for date, price in recent_min[:6]:
            dist = (S / price - 1) * 100
            marker = " ◄◄ 最近" if abs(dist) < 5 else ""
            print(f"      ¥{price:.2f} ({date.date()}) 距当前 {dist:+.1f}%{marker}")

    if recent_max:
        recent_max.sort(key=lambda x: x[1], reverse=True)
        print(f"    阻力位 (近1年局部高点):")
        for date, price in recent_max[:4]:
            dist = (S / price - 1) * 100
            print(f"      ¥{price:.2f} ({date.date()}) 距当前 {dist:+.1f}%")

# ═══════════════════════════════════════════════════════════════════════════
# 2. 超跌反弹概率 (Mean Reversion from Drawdown)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§2  超跌反弹统计 (Drawdown → Bounce Probability)")
print("─" * 80)

# Rolling max & drawdown
df["rolling_max"] = df["close"].cummax()
df["drawdown"] = df["close"] / df["rolling_max"] - 1
current_dd = df["drawdown"].iloc[-1]

print(f"  当前回撤: {current_dd*100:.1f}% (距历史最高)")

# Find all historical drawdown episodes > 15%
dd_threshold = -0.15
in_dd = False
dd_episodes = []

for i in range(len(df)):
    dd = df["drawdown"].iloc[i]
    if dd <= dd_threshold and not in_dd:
        in_dd = True
        dd_start = i
        dd_trough = dd
        dd_trough_idx = i
    elif in_dd:
        if dd < dd_trough:
            dd_trough = dd
            dd_trough_idx = i
        if dd > dd_threshold * 0.5:  # recovered to half the threshold
            in_dd = False
            dd_episodes.append({
                "start": df.index[dd_start],
                "trough_date": df.index[dd_trough_idx],
                "trough_price": df["close"].iloc[dd_trough_idx],
                "max_dd": dd_trough,
                "recovery_days": i - dd_trough_idx,
                "bounce_5d": df["close"].iloc[min(dd_trough_idx+5, len(df)-1)] / df["close"].iloc[dd_trough_idx] - 1,
                "bounce_10d": df["close"].iloc[min(dd_trough_idx+10, len(df)-1)] / df["close"].iloc[dd_trough_idx] - 1,
                "bounce_20d": df["close"].iloc[min(dd_trough_idx+20, len(df)-1)] / df["close"].iloc[dd_trough_idx] - 1,
            })

if dd_episodes:
    print(f"\n  历史大回撤 (>{abs(dd_threshold)*100:.0f}%) 统计: {len(dd_episodes)} 次")
    print(f"  {'谷底日期':<14} {'最大回撤':>8} {'5日反弹':>8} {'10日反弹':>8} {'20日反弹':>8}")
    print(f"  {'─'*14} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")
    for ep in dd_episodes:
        print(f"  {ep['trough_date'].date()!s:<14} {ep['max_dd']*100:>7.1f}% "
              f"{ep['bounce_5d']*100:>+7.1f}% {ep['bounce_10d']*100:>+7.1f}% {ep['bounce_20d']*100:>+7.1f}%")

    avg_5d = np.mean([e["bounce_5d"] for e in dd_episodes])
    avg_10d = np.mean([e["bounce_10d"] for e in dd_episodes])
    avg_20d = np.mean([e["bounce_20d"] for e in dd_episodes])
    print(f"\n  均值:                          {avg_5d*100:>+7.1f}% {avg_10d*100:>+7.1f}% {avg_20d*100:>+7.1f}%")
    win_5d = np.mean([1 for e in dd_episodes if e["bounce_5d"] > 0])
    win_10d = np.mean([1 for e in dd_episodes if e["bounce_10d"] > 0])
    win_20d = np.mean([1 for e in dd_episodes if e["bounce_20d"] > 0])
    print(f"  胜率:                          {win_5d*100:>7.0f}% {win_10d*100:>7.0f}% {win_20d*100:>7.0f}%")

# Conditional drawdown analysis
print(f"\n  当前类似回撤幅度 ({current_dd*100:.0f}%±5pp) 的历史后续表现:")
similar = [e for e in dd_episodes if abs(e["max_dd"] - current_dd) < 0.05]
if similar:
    for ep in similar:
        print(f"    {ep['trough_date'].date()}: 回撤{ep['max_dd']*100:.1f}% → "
              f"5d={ep['bounce_5d']*100:+.1f}% 10d={ep['bounce_10d']*100:+.1f}% 20d={ep['bounce_20d']*100:+.1f}%")
else:
    print(f"    无完全匹配的历史区间,使用所有回撤统计")

# ═══════════════════════════════════════════════════════════════════════════
# 3. 连续下跌后的反弹概率
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§3  连续下跌统计 (Consecutive Down Days → Bounce)")
print("─" * 80)

# Count current consecutive down streak
streak = 0
for i in range(len(df) - 1, -1, -1):
    if df["ret"].iloc[i] < 0:
        streak += 1
    else:
        break

print(f"  当前连续下跌: {streak} 天")

# Historical stats for streaks
df["is_down"] = df["ret"] < 0
all_streaks = {}
current_streak_len = 0
for i in range(1, len(df)):
    if df["is_down"].iloc[i]:
        current_streak_len += 1
    else:
        if current_streak_len > 0:
            key = current_streak_len
            # Record what happened after the streak
            fwd = {}
            for h in [1, 3, 5, 10, 20]:
                if i + h < len(df):
                    fwd[h] = df["close"].iloc[i + h] / df["close"].iloc[i] - 1
            if key not in all_streaks:
                all_streaks[key] = []
            all_streaks[key].append(fwd)
        current_streak_len = 0

print(f"\n  连续下跌天数 → 后续表现统计:")
print(f"  {'天数':>4} {'次数':>6} {'次日':>8} {'3日':>8} {'5日':>8} {'10日':>8} {'20日':>8}")
print(f"  {'─'*4} {'─'*6} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

for s in sorted(all_streaks.keys()):
    if s < 2 or s > 10:
        continue
    episodes = all_streaks[s]
    n = len(episodes)
    avgs = {}
    for h in [1, 3, 5, 10, 20]:
        vals = [e[h] for e in episodes if h in e]
        avgs[h] = np.mean(vals) * 100 if vals else float("nan")
    marker = " ◄" if s == streak else ""
    print(f"  {s:>4} {n:>6} {avgs[1]:>+7.2f}% {avgs[3]:>+7.2f}% {avgs[5]:>+7.2f}% "
          f"{avgs[10]:>+7.2f}% {avgs[20]:>+7.2f}%{marker}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. RSI + Bollinger Band 超卖区分析
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§4  超卖指标综合 (RSI / Bollinger / Williams %R)")
print("─" * 80)

# RSI
delta = df["close"].diff()
gain = delta.where(delta > 0, 0)
loss = (-delta).where(delta < 0, 0)
for period in [6, 14]:
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    current_rsi = rsi.iloc[-1]
    print(f"  RSI({period}): {current_rsi:.1f}", end="")
    if current_rsi < 20:
        print(" ★★ 极度超卖")
    elif current_rsi < 30:
        print(" ★ 超卖")
    elif current_rsi > 70:
        print(" ⚠ 超买")
    else:
        print(" 正常")

# Bollinger Bands
for period in [20, 60]:
    ma = df["close"].rolling(period).mean()
    std = df["close"].rolling(period).std()
    upper = ma + 2 * std
    lower = ma - 2 * std
    bb_pct = (df["close"] - lower) / (upper - lower)
    print(f"\n  Bollinger Band ({period}日):")
    print(f"    上轨: ¥{upper.iloc[-1]:.2f} | 中轨: ¥{ma.iloc[-1]:.2f} | 下轨: ¥{lower.iloc[-1]:.2f}")
    print(f"    %B: {bb_pct.iloc[-1]:.4f}", end="")
    if bb_pct.iloc[-1] < 0:
        print(f" ★ 跌破下轨 (极端超卖)")
    elif bb_pct.iloc[-1] < 0.2:
        print(f" ★ 接近下轨")
    else:
        print(f" 正常")

# Williams %R
for period in [14, 28]:
    hh = df["high"].rolling(period).max()
    ll = df["low"].rolling(period).min()
    wr = -100 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)
    print(f"  Williams %R ({period}日): {wr.iloc[-1]:.1f}", end="")
    if wr.iloc[-1] < -80:
        print(" ★ 超卖区")
    elif wr.iloc[-1] > -20:
        print(" ⚠ 超买区")
    else:
        print("")

# ═══════════════════════════════════════════════════════════════════════════
# 5. 估值百分位入场框架
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§5  估值百分位入场框架 (PB/PE Historical Percentile)")
print("─" * 80)

pb = df["pb"].dropna()
pe = df["pe_ttm"].dropna()

if len(pb) > 0:
    current_pb = pb.iloc[-1]
    pb_pct = stats.percentileofscore(pb, current_pb)
    print(f"\n  PB = {current_pb:.3f} (历史分位: {pb_pct:.0f}%)")

    # PB-based zones
    zones = [
        (0, 10, "★★ 极度低估 — 激进入场"),
        (10, 25, "★ 低估 — 积极入场"),
        (25, 50, "合理偏低 — 正常建仓"),
        (50, 75, "合理偏高 — 观望"),
        (75, 90, "偏高 — 减仓"),
        (90, 100, "极度高估 — 回避"),
    ]
    for lo, hi, label in zones:
        pb_lo = np.percentile(pb, lo)
        pb_hi = np.percentile(pb, hi)
        price_lo = S * pb_lo / current_pb
        price_hi = S * pb_hi / current_pb
        marker = " ◄ 当前" if lo <= pb_pct < hi else ""
        print(f"    {lo:>3}-{hi:>3}%: PB={pb_lo:.3f}~{pb_hi:.3f} → ¥{price_lo:.2f}~¥{price_hi:.2f} {label}{marker}")

if len(pe) > 0:
    pe_valid = pe[(pe > 0) & (pe < 200)]
    if len(pe_valid) > 0:
        current_pe = pe.iloc[-1]
        pe_pct = stats.percentileofscore(pe_valid, current_pe)
        print(f"\n  PE_TTM = {current_pe:.2f} (历史分位: {pe_pct:.0f}%)")

# ═══════════════════════════════════════════════════════════════════════════
# 6. 风险收益比分析 (Risk-Reward at Different Entry Levels)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§6  风险收益比分析 (R:R at Different Entry Levels)")
print("─" * 80)

# Define potential targets and stops
atr = df["atr"].iloc[-1]
sigma_20 = df["log_ret"].rolling(20).std().iloc[-1] * np.sqrt(252)

# Target: bounce to VWAP / MA60 / recent POC
targets = {
    "MA60": df["close"].rolling(60).mean().iloc[-1],
    "MA20": df["close"].rolling(20).mean().iloc[-1],
    "前密集区POC": 7.00,  # from volume profile
    "Fib 50%": 7.12,
}

# Stop: below recent low, 2 ATR, key support break
stops = {
    "2×ATR止损": S - 2 * atr,
    "近120日最低": df["low"].tail(120).min(),
    "PB 5%分位": S * np.percentile(pb, 5) / pb.iloc[-1] if len(pb) > 0 else S * 0.8,
}

print(f"\n  入场价: ¥{S:.2f}")
print(f"  {'目标位':<16} {'价格':>8} {'盈利':>8}")
print(f"  {'─'*16} {'─'*8} {'─'*8}")
for name, target in sorted(targets.items(), key=lambda x: x[1]):
    gain = (target / S - 1) * 100
    print(f"  {name:<16} ¥{target:>7.2f} {gain:>+7.1f}%")

print(f"\n  {'止损位':<16} {'价格':>8} {'亏损':>8}")
print(f"  {'─'*16} {'─'*8} {'─'*8}")
for name, stop in sorted(stops.items(), key=lambda x: x[1], reverse=True):
    loss = (stop / S - 1) * 100
    print(f"  {name:<16} ¥{stop:>7.2f} {loss:>+7.1f}%")

print(f"\n  风险收益比矩阵 (R:R = 收益/风险):")
print(f"  {'':>16}", end="")
for t_name in targets:
    print(f"  {t_name:>12}", end="")
print()
print(f"  {'':>16}", end="")
for _ in targets:
    print(f"  {'─'*12}", end="")
print()

for s_name, stop in stops.items():
    risk = abs(S - stop)
    print(f"  {s_name:<16}", end="")
    for t_name, target in targets.items():
        reward = target - S
        if risk > 0 and reward > 0:
            rr = reward / risk
            marker = "★" if rr >= 2.0 else " "
            print(f"  {rr:>10.2f}:1{marker}", end="")
        else:
            print(f"  {'N/A':>12}", end="")
    print()

# ═══════════════════════════════════════════════════════════════════════════
# 7. 分批入场模拟 (DCA vs Lump Sum at Different Levels)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§7  分批入场策略模拟")
print("─" * 80)

# Based on Monte Carlo from previous analysis
np.random.seed(42)
n_sims = 30000

# Fit Student-t for simulation
t_params = stats.t.fit(df["log_ret"].dropna().values)
df_t, loc_t, scale_t = t_params

# Simulate 60 trading day paths
T = 60
sim_rets = stats.t.rvs(df_t, loc=loc_t, scale=scale_t, size=(n_sims, T))
cum_rets = np.cumsum(sim_rets, axis=1)
price_paths = S * np.exp(cum_rets)

strategies = {
    "一次性全仓": None,
    "3次均分 (每20日)": [0, 20, 40],
    "5次均分 (每12日)": [0, 12, 24, 36, 48],
    "跌5%加仓": "dip_5pct",
    "跌10%加仓": "dip_10pct",
}

print(f"\n  模拟期限: {T}个交易日 (约3个月)")
print(f"  模拟次数: {n_sims:,}")
print(f"  收益率分布: Student-t (df={df_t:.1f})")
print(f"\n  {'策略':<20} {'均成本':>8} {'60日收益':>10} {'最大回撤':>10} {'胜率':>8} {'Sharpe':>8}")
print(f"  {'─'*20} {'─'*8} {'─'*10} {'─'*10} {'─'*8} {'─'*8}")

for strat_name, config in strategies.items():
    avg_costs = []
    terminal_rets = []
    max_dds = []

    for sim in range(n_sims):
        path = price_paths[sim]

        if config is None:
            # Lump sum
            avg_cost = S
            terminal_ret = path[-1] / S - 1
        elif isinstance(config, list):
            # Fixed schedule DCA
            prices_at_entry = [S if d == 0 else path[d-1] for d in config if d < T]
            avg_cost = np.mean(prices_at_entry)
            terminal_ret = path[-1] / avg_cost - 1
        elif config == "dip_5pct":
            # Buy on 5% dips
            entries = [S]
            for d in range(1, T):
                if path[d] < entries[-1] * 0.95 and len(entries) < 5:
                    entries.append(path[d])
            avg_cost = np.mean(entries)
            terminal_ret = path[-1] / avg_cost - 1
        elif config == "dip_10pct":
            entries = [S]
            for d in range(1, T):
                if path[d] < entries[-1] * 0.90 and len(entries) < 5:
                    entries.append(path[d])
            avg_cost = np.mean(entries)
            terminal_ret = path[-1] / avg_cost - 1

        # Max drawdown from avg cost
        running_dd = np.minimum.accumulate(path) / avg_cost - 1
        max_dd = running_dd.min()

        avg_costs.append(avg_cost)
        terminal_rets.append(terminal_ret)
        max_dds.append(max_dd)

    avg_costs = np.array(avg_costs)
    terminal_rets = np.array(terminal_rets)
    max_dds = np.array(max_dds)

    win_rate = (terminal_rets > 0).mean()
    sharpe = terminal_rets.mean() / terminal_rets.std() if terminal_rets.std() > 0 else 0

    print(f"  {strat_name:<20} ¥{avg_costs.mean():>7.2f} {terminal_rets.mean()*100:>+9.2f}% "
          f"{max_dds.mean()*100:>9.1f}% {win_rate*100:>7.1f}% {sharpe:>7.3f}")

# ═══════════════════════════════════════════════════════════════════════════
# 8. 波动率调整后的入场区间 (Volatility-Adjusted Entry Zones)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§8  波动率调整入场区间 (Vol-Adjusted Entry Zones)")
print("─" * 80)

# Current vol regime
rv_20 = df["log_ret"].rolling(20).std().iloc[-1] * np.sqrt(252)
rv_60 = df["log_ret"].rolling(60).std().iloc[-1] * np.sqrt(252)
rv_full = df["log_ret"].std() * np.sqrt(252)

print(f"\n  波动率环境:")
print(f"    20日RV: {rv_20*100:.1f}%  (当前)")
print(f"    60日RV: {rv_60*100:.1f}%")
print(f"    全样本:  {rv_full*100:.1f}%")
vol_ratio = rv_20 / rv_full
print(f"    波动率比: {vol_ratio:.2f}x (>1.5 = 高波动环境)")

# In high-vol regimes, widen entry zones; in low-vol, tighten
print(f"\n  高波动率 → 入场区间需更宽 (等更深的折扣)")
print(f"  低波动率 → 入场区间可更窄")

# Regime-adjusted entry zones using vol-scaled ATR
for vol_name, vol_est in [("当前vol", rv_20), ("正常vol", rv_full)]:
    daily_move = S * vol_est / np.sqrt(252)
    print(f"\n  ── 基于{vol_name} ({vol_est*100:.0f}%) ──")
    print(f"    预期日波动: ±¥{daily_move:.2f}")
    for n_days in [5, 10, 20]:
        expected_range = S * vol_est * np.sqrt(n_days/252)
        low_end = S * np.exp(-vol_est * np.sqrt(n_days/252))
        # 1.5σ and 2σ levels
        low_15 = S * np.exp(-1.5 * vol_est * np.sqrt(n_days/252))
        low_20 = S * np.exp(-2.0 * vol_est * np.sqrt(n_days/252))
        print(f"    {n_days:>2}日: 1σ=¥{low_end:.2f} | 1.5σ=¥{low_15:.2f} | 2σ=¥{low_20:.2f}")

# ═══════════════════════════════════════════════════════════════════════════
# 9. Kelly Criterion (最优仓位)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§9  Kelly Criterion 最优仓位 (基于历史胜率)")
print("─" * 80)

# Forward return statistics at current-like conditions
# Use conditional: similar drawdown + similar vol regime
rets_fwd = {}
for h in [5, 10, 20, 60]:
    fwd = df["close"].shift(-h) / df["close"] - 1
    rets_fwd[h] = fwd

# Condition: drawdown > 20%
dd_mask = df["drawdown"] < -0.20
if dd_mask.sum() > 20:
    print(f"\n  条件: 回撤>20% 时入场 ({dd_mask.sum()} 个样本)")
    print(f"  {'持有期':>8} {'胜率':>8} {'均盈':>10} {'均亏':>10} {'Kelly%':>10} {'期望值':>10}")
    print(f"  {'─'*8} {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")
    for h in [5, 10, 20, 60]:
        fwd = rets_fwd[h][dd_mask].dropna()
        if len(fwd) < 10:
            continue
        wins = fwd[fwd > 0]
        losses = fwd[fwd < 0]
        p_win = len(wins) / len(fwd) if len(fwd) > 0 else 0
        avg_win = wins.mean() if len(wins) > 0 else 0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 1
        # Kelly: f* = p/a - q/b where p=win prob, q=loss prob, a=avg loss, b=avg win
        if avg_loss > 0 and avg_win > 0:
            kelly = p_win / avg_loss - (1 - p_win) / avg_win
            kelly = max(0, min(kelly, 1))
            expected = p_win * avg_win - (1 - p_win) * avg_loss
        else:
            kelly = 0
            expected = 0
        print(f"  {h:>6}日 {p_win*100:>7.1f}% {avg_win*100:>+9.2f}% {-avg_loss*100:>9.2f}% "
              f"{kelly*100:>9.1f}% {expected*100:>+9.3f}%")
else:
    print(f"  回撤>20%的样本不足,使用全样本")

# Full sample (unconditional)
print(f"\n  无条件 (全样本, {N} 日):")
print(f"  {'持有期':>8} {'胜率':>8} {'均盈':>10} {'均亏':>10} {'Kelly%':>10} {'期望值':>10}")
print(f"  {'─'*8} {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")
for h in [5, 10, 20, 60]:
    fwd = rets_fwd[h].dropna()
    wins = fwd[fwd > 0]
    losses = fwd[fwd < 0]
    p_win = len(wins) / len(fwd)
    avg_win = wins.mean()
    avg_loss = abs(losses.mean())
    if avg_loss > 0 and avg_win > 0:
        kelly = p_win / avg_loss - (1 - p_win) / avg_win
        kelly = max(0, min(kelly, 1))
        expected = p_win * avg_win - (1 - p_win) * avg_loss
    else:
        kelly = expected = 0
    print(f"  {h:>6}日 {p_win*100:>7.1f}% {avg_win*100:>+9.2f}% {-avg_loss*100:>9.2f}% "
          f"{kelly*100:>9.1f}% {expected*100:>+9.3f}%")

# ═══════════════════════════════════════════════════════════════════════════
# 10. 量价背离入场信号 (Volume-Price Divergence Scanner)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§10  量价背离扫描 (Bottom Divergence Detection)")
print("─" * 80)

# Look for price making lower lows but volume/RSI/MFI making higher lows
recent = df.tail(60).copy()

# Price lower lows
price_lows = []
for order in [5, 10]:
    idx = argrelextrema(recent["low"].values, np.less_equal, order=order)[0]
    for i in idx:
        price_lows.append((recent.index[i], recent["low"].iloc[i], recent["vol_shares"].iloc[i]))

if len(price_lows) >= 2:
    price_lows.sort(key=lambda x: x[0])
    print(f"\n  近60日局部低点:")
    for date, price, vol in price_lows:
        print(f"    {date.date()}: ¥{price:.2f}, 成交量 {vol/1e6:.1f}M")

    # Check last two lows
    if len(price_lows) >= 2:
        p1_date, p1_price, p1_vol = price_lows[-2]
        p2_date, p2_price, p2_vol = price_lows[-1]
        if p2_price < p1_price and p2_vol < p1_vol:
            print(f"\n  ★★ 量价底背离: 价格创新低 (¥{p1_price:.2f}→¥{p2_price:.2f})")
            print(f"      但成交量萎缩 ({p1_vol/1e6:.1f}M→{p2_vol/1e6:.1f}M)")
            print(f"      → 卖压衰竭,潜在见底信号")
        elif p2_price < p1_price and p2_vol > p1_vol:
            print(f"\n  ⚠ 价量齐跌加速: 新低+放量 → 恐慌性抛售,可能尚未见底")
        elif p2_price > p1_price:
            print(f"\n  价格未创新低,形成更高低点 (higher low) → 结构转强")

# ═══════════════════════════════════════════════════════════════════════════
# 11. 综合入场建议框架
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 80)
print("§11  ★ 综合入场分析框架")
print("─" * 80)

# Collect all signals into scoring
score = 0
max_score = 0
details = []

# RSI oversold
delta = df["close"].diff()
gain = delta.where(delta > 0, 0)
loss_s = (-delta).where(delta < 0, 0)
avg_gain = gain.rolling(14).mean()
avg_loss = loss_s.rolling(14).mean()
rs = avg_gain / avg_loss.replace(0, np.nan)
rsi14 = 100 - 100 / (1 + rs)
rsi_val = rsi14.iloc[-1]

max_score += 2
if rsi_val < 20:
    score += 2; details.append(f"RSI14={rsi_val:.0f} ★★ 极度超卖 (+2)")
elif rsi_val < 30:
    score += 1; details.append(f"RSI14={rsi_val:.0f} ★ 超卖 (+1)")
else:
    details.append(f"RSI14={rsi_val:.0f} 正常 (0)")

# Drawdown depth
max_score += 2
if current_dd < -0.30:
    score += 2; details.append(f"回撤{current_dd*100:.0f}% ★★ 深度超跌 (+2)")
elif current_dd < -0.20:
    score += 1; details.append(f"回撤{current_dd*100:.0f}% ★ 较大回撤 (+1)")
else:
    details.append(f"回撤{current_dd*100:.0f}% 正常 (0)")

# PB percentile
max_score += 2
if len(pb) > 0:
    pb_pct = stats.percentileofscore(pb, pb.iloc[-1])
    if pb_pct < 10:
        score += 2; details.append(f"PB {pb_pct:.0f}%分位 ★★ 极低估值 (+2)")
    elif pb_pct < 25:
        score += 1; details.append(f"PB {pb_pct:.0f}%分位 ★ 偏低估值 (+1)")
    else:
        details.append(f"PB {pb_pct:.0f}%分位 估值正常 (0)")

# Volume-price divergence (positive)
max_score += 1
if price_slope < 0 and obv_slope > 0 if 'obv_slope' in dir() else False:
    score += 1; details.append("OBV底背离 (+1)")

# Bollinger below lower band
bb_lower = df["close"].rolling(20).mean().iloc[-1] - 2 * df["close"].rolling(20).std().iloc[-1]
max_score += 1
if S < bb_lower:
    score += 1; details.append(f"跌破BB下轨 ¥{bb_lower:.2f} ★ (+1)")
else:
    details.append(f"在BB下轨 ¥{bb_lower:.2f} 之上 (0)")

# Consecutive decline
max_score += 1
if streak >= 5:
    score += 1; details.append(f"连跌{streak}日 ★ 均值回复概率高 (+1)")
else:
    details.append(f"连跌{streak}日 (0)")

# Vol regime (high vol = deeper discount needed)
max_score += 1
if vol_ratio > 1.5:
    details.append(f"⚠ 高波动环境 (vol ratio={vol_ratio:.1f}x) → 需要更大安全边际")
    score -= 0.5  # Penalty
else:
    details.append(f"波动率正常 (vol ratio={vol_ratio:.1f}x)")

# Volume at lows declining (selling exhaustion)
max_score += 1
vol_20_avg = df["vol_shares"].tail(20).mean()
vol_60_avg = df["vol_shares"].tail(60).mean()
if vol_20_avg < vol_60_avg * 0.7:
    score += 1; details.append("近20日缩量 → 抛压减轻 (+1)")
elif vol_20_avg > vol_60_avg * 1.5:
    details.append("近20日放量 → 可能恐慌/换手 (0)")
else:
    details.append("成交量正常 (0)")

print(f"\n  入场评分: {score:.1f} / {max_score} ({score/max_score*100:.0f}%)")
print()
for d in details:
    print(f"    {d}")

# Interpretation
print(f"\n  综合判定:")
pct = score / max_score
if pct >= 0.7:
    print(f"    ★★★ 强入场信号 — 多项指标指向超卖区域,统计上看反弹概率较高")
    print(f"    建议: 分批入场 (3-5次), 初始仓位 20-30%")
elif pct >= 0.5:
    print(f"    ★★ 中等入场信号 — 部分指标超卖,但需等待更多确认")
    print(f"    建议: 小仓试探 (10-15%), 等待放量阳线确认再加仓")
elif pct >= 0.3:
    print(f"    ★ 弱入场信号 — 下跌趋势中,尚无明确见底迹象")
    print(f"    建议: 观望为主, 可设定价格预警在关键支撑位")
else:
    print(f"    观望 — 当前不符合入场条件")

# Key price levels summary
print(f"\n  ── 关键价位速查 ──")
print(f"    当前价:        ¥{S:.2f}")
print(f"    BB下轨(20日):  ¥{bb_lower:.2f}")
bb_lower60 = df["close"].rolling(60).mean().iloc[-1] - 2 * df["close"].rolling(60).std().iloc[-1]
print(f"    BB下轨(60日):  ¥{bb_lower60:.2f}")
print(f"    60日POC:       ¥7.00 (成交密集区)")
print(f"    MA120:         ¥{df['close'].rolling(120).mean().iloc[-1]:.2f}")
print(f"    MA250:         ¥{df['close'].rolling(250).mean().iloc[-1]:.2f}")
if len(pb) > 0:
    print(f"    PB 10%分位:    ¥{S * np.percentile(pb, 10) / pb.iloc[-1]:.2f}")
    print(f"    PB 25%分位:    ¥{S * np.percentile(pb, 25) / pb.iloc[-1]:.2f}")
print(f"    2×ATR止损:     ¥{S - 2*atr:.2f}")

print("\n" + "=" * 80)
print("  以上分析基于历史数据的统计规律,不构成投资建议。")
print("  市场存在系统性风险,个股可能出现超出历史范围的极端走势。")
print("=" * 80)
