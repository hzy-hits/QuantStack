# Quant Stack — Architecture Reference

Single source of truth for **what runs where**. Read this before asking
"是不是混了 / 老代码还在不 / cron 跑了啥".

Date: 2026-05-20 · Maintainer: operator + Claude Code

---

## 1. 顶层视图

AI 基础设施专业户量化基金。一套 BFS-D0~D5 依赖图驱动的窄主题股票池
(US + CN 双市场),用同一个 Rust orchestrator 出日更决策报告。架构分
四层:

```
┌─ ① 数据层(共享 DuckDB,read-shared / single-writer per cron) ─┐
│  quant.duckdb · quant_cn.duckdb · factor_lab.duckdb           │
└────────────────────────────────────────────────────────────────┘
            ↑                                          ↑
┌─ ② 主编排(唯一一个 Rust 二进制) ──────────────────────────────┐
│  ./target/release/quant-stack daily ...                       │
│  US 入口: us.premarket / us.postmarket(→ run_full.sh exec 它) │
│  CN 入口: cn.morning / cn.evening(直接调)                     │
│  → 出 R-based AI-infra 决策报告,发邮件                         │
└────────────────────────────────────────────────────────────────┘
            ↑ 读各种 component 产物
┌─ ③ 组件生产 task(~36 个 research.*,Python) ─────────────────┐
│  ingest · radars · regime · evidence cards · universe         │
│  → 写 reports/review_dashboard/{component}/{date}/             │
└────────────────────────────────────────────────────────────────┘
            +
┌─ ④ 周边 ──────────────────────────────────────────────────────┐
│  factor-lab(独立研究层)· paper · cn.precompute_alpha         │
│  · us.watchdog · ops.catch_up                                  │
└────────────────────────────────────────────────────────────────┘
```

**没有"老/新管线并行跑"的问题。** `us.premarket` / `us.postmarket` 看着
像老,实际只是入口 shell,exec 进同一个 Rust orchestrator。

---

## 2. 数据层

| DB | 路径 | 关键表 | 写入者 |
|---|---|---|---|
| US prices/options/news | `quant-research-v1/data/quant.duckdb` | `prices_daily`、`options_*`、`analysis_daily`、`macro_daily`、`news`、`run_log` | `us.postmarket` / `us.premarket`(via Rust + Python fetchers) |
| CN prices/factor/flow | `quant-research-cn/data/quant_cn.duckdb` | `prices`、`daily_basic`、`fina_indicator`、`income/balancesheet/cashflow`、`moneyflow`、`northbound_flow`、`margin_detail`、`opt_basic/opt_daily`、`top_list` | Rust fetchers + `ingest_cn_*` Python scripts |
| Factor Lab | `factor-lab/data/factor_lab.duckdb` | `factor_registry`、`factor_experiment_ledger`、`factor_weights`、`factor_money_gate_daily`、`paper_returns`、`pipeline_runs` | `factor.*.daily` + `autoresearch.*` |
| Backtest history | `data/strategy_backtest_history.duckdb` | strategy-gate champion ledger | `score_strategy_stability_gate.py`(via run_daily) |

**单写多读**:每张 DB 一次只能一个进程写(DuckDB 文件锁)。`run_task.py`
用 `flock` 保证同一 task 不并发;不同 task 写不同表时无冲突。

---

## 3. 主编排:Rust `quant-stack daily`

**唯一的日更决策入口。** US 和 CN 走同一份代码,只是 cron 时点与
`--session` / `--markets` 不同。

```
                 cron 时点                cwd              入口
us.premarket     20:00 工作日             quant-research-v1  run_full.sh → exec quant-stack daily --markets us --session pre
us.postmarket    05:00 周二-周六          quant-research-v1  run_full.sh → exec quant-stack daily --markets us --session post
cn.morning       08:30 工作日             .                  quant-stack daily --markets cn --session morning
cn.evening       18:00 工作日             .                  quant-stack daily --markets cn --session evening
```

**它做什么**:拉数据(via 内置或外部 fetcher)→ 读 `reports/review_dashboard/{component}/{date}/` 各组件产物 → 合成
R-based 决策报告 → 写 `reports/review_dashboard/main_strategy_v2/{date}/{cn,us}_daily_report.md` → 发邮件(via
`scripts/send_production_decision_report.py`)。

**为啥 `us.premarket` 的 cwd 是 `quant-research-v1` 而 `cn.morning` 是 `.`** —— 历史包袱。
`run_full.sh` 还住在老 US 仓目录,但里面就是 `exec "$STACK_ROOT/target/release/quant-stack"`,完全等价。

---

## 4. 组件生产 task(`research.*`)

36 个 Python 脚本,各自负责日报某一片(雷达/regime/证据卡/universe)。
每个写到 `reports/review_dashboard/{component_name}/{date}/`,主编排读它们。

按时间窗分:

```
06:00-08:00  数据 ingest            cn_flow / cn_index / satellite_index / wedge_instruments / fear_greed
09:00        US 因子日跑            factor.us.daily(→ daily_factors.sh us)
04:00        CN 因子日跑            factor.cn.daily
10:00 / 14:00 autoresearch           autoresearch.all.midday / .afternoon(Factor Mining 模式仅在 hour ∉ [9,12])
                                    autoresearch.cn.morning(06:00,Factor Mining)
11:00-12:00  AI-infra 研究           bfs_discovery / expansion_candidates / source_ingest / promotion_plan /
                                    source_review_readiness / cn_ai_evidence_verify / production_universe_refresh /
                                    evidence_card_drafts / ai_supercycle_readiness / ai_tape_cross_compare
12:07-12:22  雷达批                  options_anomaly / options_tenor / mean_reversion / ten_x / victim_put /
                                    bubble_hedge / capitulation / capitulation_convex / risk_regime / cn_risk_regime /
                                    options_anomaly_alerts
12:25        主报告                  main_strategy_v2_report ← 读以上所有组件产物
12:27        审计                    production_basket_audit
12:50        PIT 账本                universe_membership_snapshot
周六 10:30   AI-infra 回测            ai_infra_strategy_backtest
```

**全部跑 cwd `.`**(除了 `ai_infra_bfs_discovery_queue` 在 `ai_infra`、`ai_infra_expansion_candidates` 在 `.`
调 `ai_infra/scripts/generate_expansion_candidates.py`)。

---

## 5. 周边

### Factor Lab(独立研究层)

| Task | 时点 | 作用 |
|---|---|---|
| `factor.cn.daily` | 04:00 工作日 | CN 因子挖掘(bootstrap + money gate) |
| `factor.us.daily` | 09:00 周二-周六 | US 因子挖掘 |
| `autoresearch.cn.morning` | 06:00 工作日 | CN 因子假设生成(LLM agent,Factor Mining 模式) |
| `autoresearch.all.midday` | 10:00 工作日 | Strategy Optimization 模式(网格搜索 + SigReg,**不产 autoresearch.jsonl**) |
| `autoresearch.all.afternoon` | 14:00 工作日 | Factor Mining 模式(US + CN 因子假设,产 jsonl) |
| `factor.maintenance.weekly` | 周六 08:17 | 因子健康度维护 |
| `paper.record/.evaluate/.report` | 04:33 / 07:47 / 07:53 周二-周六 | 纸面交易簿 |

**产出注入主管线**:promoted 因子(`factor_registry`)→ `export_to_pipeline` →
`analytics`(CN) / `analysis_daily`(US)的 `lab_factor` 列。
**当前状态**:14 个 promoted,**全部 `research_only`,无一过 `fresh_buy_gate + money_ready`**,
所以 `lab_factor` 自 05-06 起每日被正确清空(闸门做对了事,**不是 bug**)。

### CN 预处理

`cn.precompute_alpha`(07:20 工作日,cwd `quant-research-cn`)——
旧的 CN 因子特征预算 shell。还在用,未被主 orchestrator 取代。

### Watchdog & Catch-up

- `us.watchdog`:12,27,42,57 * * * *(15 分钟一次)。监控并触发漏跑的 US 任务。
- `us.watchdog.reboot`:@reboot。WSL 启动后跑一次。
- `ops.catch_up`:5,20,35,50 * * * *(每 15 分钟,与 watchdog 错开)。
  WSL2 cron 是 best-effort —— 机器睡了 cron 不补跑。catch-up 检测
  *今天* 该跑但 `last_success` 还是昨天的 task,通过 `run_task.sh` 补跑。
  **范围**:只补 `research / factor / autoresearch / paper`,不碰 `cn.*` / `us.*` / `weekly.*`(重型,跑晚了更糟)。

---

## 6. 二进制和构建

| 二进制 | 工程路径 | 构建命令 | 谁在用 |
|---|---|---|---|
| `./target/release/quant-stack` | `crates/quant-stack-cli/`(根 workspace) | `cargo build --release`(在仓根) | 主 orchestrator(所有 daily 入口) |
| `quant-research-v1/rust/target/release/quant-fetcher` | `quant-research-v1/rust/`(独立 Cargo,**根 workspace 已 exclude**) | `cd quant-research-v1/rust && cargo build --release` | US 数据 fetcher(Finnhub/FRED/SEC/Polymarket),被 run_full.sh 等调用 |
| `factor-lab/rust-bootstrap/target/release/...` | `factor-lab/rust-bootstrap/`(独立 Cargo) | `cd factor-lab/rust-bootstrap && cargo build --release` | factor-lab 内部 |

**改 Rust 要记住**:看文件路径前缀就知道 build 哪个工程。
根 workspace 的 `Cargo.toml` 已经 exclude 了 quant-research-v1/rust 和
factor-lab/rust-bootstrap,所以仓根 `cargo build` 不会编它们。

---

## 7. Universe & 证据门

| 文件 | 作用 |
|---|---|
| `ai_infra/data/global_universe_v2.jsonl` | AI-infra universe 唯一来源(US ~57 / CN ~96 records) |
| `ai_infra/data/universe_membership_history.jsonl` | PIT 成员账本,每日 12:50 由 `snapshot_universe_membership.py` 追加 |
| `ai_infra/reports/source_verification_queue_v1.csv` | 待源审队列 |
| `quant-research-v1/src/quant_bot/analytics/ai_infra_universe.py` | universe 加载 + `is_production_grade` 判定 |

**证据门**(`is_production_grade`):evidence_state 头(冒号前)含
`原文已证明` → production;含 `合理推论` 且无 pending flag(`待原文核验` /
`原文需核验` / `证据不足`)→ production;其余 → 仅研究池。

**Tushare 验证**:`scripts/verify_cn_ai_evidence.py` 拉 fina_mainbz,
按 `AI_DIRECT_KW` / `AI_ADJACENT_KW` 判 direct/adjacent/none,自动升级
`待原文核验` → `合理推论` / `原文已证明`(若达 material share)。
**对操作员手动写的 `合理推论` 不再触碰**(避免覆盖人工决定)。

---

## 8. Regime 引擎

`score_risk_regime_engine.py`(US) + `score_cn_risk_regime.py`(CN-native)
共享同一份 R 乘数表 + 状态机:

| State | 触发 | R | 新加仓 |
|---|---|---|---|
| HEDGE | tape 健康 + wedge 未咬 | **1.00** | 满仓 |
| WEDGE | TLT 20d ≤ -2% / SMH↔TLT corr ≥ 0.5 / HYG 20d ≤ -1% / MOVE ≥ 80↑ | **0.60** | 减码 |
| CONFIRM | SMH 失守 EMA20 但仍站 EMA50 / 1-2 日破 EMA50(fresh break)/ extreme greed + wedge | **0.40** | 冻结追高 |
| PRESS | 连续 ≥3 日收于 EMA50 下(滞回)/ 显式 trendline break | **0.35** | 冻结新加仓,保留防御核心 |
| CAPITULATION | 抄底雷达 ≥3/5 | **1.00** | 翻多凸性 |

CN 版本结构相同,tape 用创业板/沪深300 EMA,flow 用北向 20d + 两融趋势,
wedge 保留 US MOVE 层(经北向传导)。

---

## 9. 关键产物与去向

```
reports/review_dashboard/
  ├─ main_strategy_v2/{date}/         ← 主日报(US/CN markdown + opportunity_ranker)
  ├─ risk_regime/{date}/              ← US regime 决策 + R
  ├─ cn_risk_regime/{date}/           ← CN regime 决策 + R
  ├─ bubble_hedge_radar/{date}/       ← wedge/victim/confirmation layers
  ├─ capitulation_radar/{date}/       ← 抄底 5 信号 + convex_longs
  ├─ us_options_anomaly_radar/{date}/ ← US 远 OTM 异常(注:date = 最新 US trade day)
  ├─ us_options_tenor_radar/{date}/   ← 多 tenor 期权信号
  ├─ us_mean_reversion_radar/{date}/  ← US 左侧超卖雷达
  ├─ ai_infra_ten_x_radar/{date}/     ← ten-x 候选
  ├─ ai_infra_promotion_plan/{date}/  ← 晋级计划
  ├─ ai_infra_evidence_card_drafts/{date}/  ← 证据卡草稿
  ├─ ai_infra_backtest/{date}/        ← 周六回测
  └─ ...
```

`reports/` 整个被 `.gitignore` —— 不进 git。

---

## 10. 操作速查

```bash
# 重新生成 + 安装 crontab
python3 ops/render_cron.py --output ops/crontab.quant-stack
crontab ops/crontab.quant-stack

# 手动跑一个 task(走 flock + 日志 + state 写入)
ops/run_task.sh research.bubble_hedge_radar

# 手动补跑今天漏掉的(等不到下次 catch-up 触发)
python3 ops/catch_up.py             # 实跑
python3 ops/catch_up.py --dry-run   # 只看会跑啥

# 重建 Rust orchestrator(改了 crates/* 后)
cargo build --release

# 重建 Rust fetcher(改了 quant-research-v1/rust/* 后)
cd quant-research-v1/rust && cargo build --release

# 手动跑日更报告(不发邮件,产物落 review_dashboard/main_strategy_v2/{date}/)
python3 scripts/generate_main_strategy_v2_report.py --date 2026-05-20 --ai-infra-mode enforce_expand

# 跑 AI-infra 回测(2024-06 起,~1-2 分钟)
python3 scripts/run_ai_infra_strategy_backtest.py --start 2024-06-01 --end 2026-05-20

# CN 证据门验证(Tushare fina_mainbz,~30s,会改 universe 文件)
python3 scripts/verify_cn_ai_evidence.py             # 实跑
python3 scripts/verify_cn_ai_evidence.py --dry-run   # 只预览

# 给新加 universe 的 CN 标的回补价格历史
python3 scripts/backfill_cn_prices.py

# 刷新 PIT 成员账本(每日 12:50 cron,手跑也行)
python3 scripts/snapshot_universe_membership.py
```

---

## 11. 已知约束 & 易踩坑

- **WSL2 cron 是 best-effort** —— Windows 睡/WSL idle 关机时不跑,**不补跑**。
  `ops.catch_up` 醒来后 15 分钟内会补,但前提是它装进了 live crontab
  (`crontab ops/crontab.quant-stack`)。
- **US prices/options 滞后 1 个交易日** —— `us.postmarket` 早 5 点拉的是**前一日**收盘。
  日报的"期权读数" loader 已有 `_latest_dated_subdir` 回退,会读最新可用日期,不再 n/a。
- **CN 周一缺口** —— 周一 NY 日历昨天=周日,无数据。`check_us_ready` fallback
  有 `--max-staleness-days 5`,容忍周末/假期/周一缺口。
- **Tushare rate-limit** —— `fina_mainbz` 一分钟限次,脚本里 0.8s sleep + 60s 回退。
- **factor-lab feature 饿死** —— `_compute_basic_features` 静默丢 `n<60` 的标的。
  loop.py 启动时打印覆盖率,覆盖不足一半发 `⚠️ FEATURE STARVATION` 大字警告。
- **CN 新加 universe 标的需要 backfill** —— Rust fetcher 只往前拉,新名字默认只有 ~45 行。
  跑 `scripts/backfill_cn_prices.py`。
- **DuckDB 文件锁** —— 同一 DB 一个写者。手动跑可能撞上正在跑的 cron task,
  报 `Conflicting lock`。等它跑完或读 `last_success` 状态。
- **改 Rust 要记得 build 对的工程** —— 根 workspace 不含 quant-research-v1/rust,
  改 fetcher 要 `cd` 进去 build。

---

## 12. 历史包袱(留着没废,但不影响主链)

- `quant-research-v1/scripts/run_daily.py` —— Python 后备日更,**未在 cron**,
  被 Rust orchestrator 取代。如果完全确认不被用可删,目前留着。
- `scripts/run_alpha_sleeve_backtest.py` —— Alpha sleeve 工程的回测,
  休眠状态,绑着 `docs/ALPHA_SLEEVE_ENGINEERING_PLAN.md`。
- 老的 `quant-research-v1/scripts/run_full.sh` —— 就是个 exec shell,只剩"找正确的 quant-stack 二进制"的逻辑。

---

## 13. 演进:还能改但不紧急

按价值/风险排序:

1. **正式标记/删除 `run_daily.py`**(`# DEPRECATED — Rust quant-stack canonical`)。
2. **合并 `quant-research-v1/rust` 进根 workspace** —— 减少"改 Rust 要 build 哪个"的混淆。~50 文件迁移,
   要重写 `Cargo.toml` 依赖路径,不紧急。
3. **`cn.precompute_alpha` 移进 Rust orchestrator** 或显式标注是预处理依赖。

**不建议做的**:改 cron task 名字(纯装饰,破历史)、把 cwd 全统一成 `.`
(动 N 个 sh)、把 factor-lab 物理合进主仓(独立研究节奏,保持边界对)。
