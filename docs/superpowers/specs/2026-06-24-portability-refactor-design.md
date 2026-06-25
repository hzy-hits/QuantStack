# Quant-Stack 可移植性重构 + Oracle 迁移 — 设计 Spec

Date: 2026-06-24 · 状态:待评审 · 作者:operator + Claude

## 目标

把生产 quant-stack 从 WSL2 上"能跑但臃肿、绑死本机"的状态,重构成**瘦身、可移植、以 GitHub 为源、数据热库在 Oracle 计算机、冷历史 Parquet 落 NAS、Pi 当 broker/watchdog** 的状态。两条主线:

1. **数据 + 部署可移植性**(低风险,解决当前真痛点):数据瘦身/压实、去 WSL 化、GitHub 当部署源、迁到 Oracle。
2. **有限的架构收敛**(中风险,刻意框死):只收敛两条 Rust 入口 + 去 WSL 化,**不动** 36 个 research 脚本的组织。

## 明确不在本 spec 范围内(避免范围蔓延)

- **agent-native / MCP / Hermes 层** —— 这是后续独立 spec。本次只做"让系统瘦、可移植、跑在 Oracle 上",不加新 agent 能力。
- **重写任何确定性 compute**(分析模块、证据门、regime、main_strategy_v2)—— 它们成熟且正确,本次只搬不改。
- **修已记录的分析 bug**(earnings_risk 未 surface、HMM calibration 语义等)—— 记录在案,另开。

## 已锁定决策

| 决策 | 选择 | 依据 |
|---|---|---|
| factor-lab 子系统 | **从部署中移除(方案 A)** | 当前对生产日报零贡献(promoted 全 `research_only`,无一过 money gate,`lab_factor` 每日清空);主链已 `is_dir()` 解耦;daily autoresearch 早已停用 |
| factor_lab.duckdb 12GB | **不迁,归档小表后丢弃** | 12GB 是 DuckDB 死空间;实际数据仅几万行;`paper_returns`(37)+`factor_registry`(56)归档为 parquet |
| 部署源 | **GitHub(已在 `github.com/hzy-hits/QuantStack`)** | 代码已 push,密钥从未入历史,`.gitignore` 已正确白名单 curated 种子 |
| 数据迁移方式 | **EXPORT DATABASE → parquet → IMPORT** | 一举两得:压实死空间 + 跨 DuckDB 版本/架构安全 |
| 架构收敛范围 | **仅两条 Rust 入口合一 + 去 WSL 化 + 取数/计算解耦** | "有限思考";36 脚本重排收益低、破历史 |
| 数据层引擎 | **DuckDB(热)+ Parquet(冷)分层,不换 Postgres** | 单机 OLAP 最佳命中区;热库小而快、冷历史落分区 Parquet、DuckDB 原生 `read_parquet` 可扫;**拓展靠分层不靠换引擎** |
| 部署拓扑 | **Oracle 热算(本地 hot DuckDB)/ NAS 冷 Parquet 历史湖 / Pi broker+watchdog** | 活 DuckDB 只在算的机器(单写者最佳形态);NAS 只存 Parquet 不跑活库;Pi 家侧 always-on 发起归档+巡检 |
| NAS | **群晖 DS220+**(Celeron J4025 双核 x86_64 / 2–6GB / Docker) | **冷 Parquet 历史湖 + 密钥/报告归档**(存储重 CPU 轻,正合 DS220+);不跑活 DuckDB、不跑重计算 |
| 树莓派 | **Pi(家侧 always-on,低功耗)** | broker:把 Oracle 滚出的老分区归档成 Parquet 推 NAS;watchdog:巡检 Oracle/任务新鲜度,挂了告警。不跑 DuckDB/计算 |

## 现状关键事实(来自 2026-06-24 并行深读,见 workflow wf_99329de6-7ea)

- 系统 95% 确定性;LLM 仅叙述(codex 主 + DeepSeek `deepseek-v4-pro` 回退)。**本次重构不触碰这条边界。**
- canonical DuckDB:`quant-research-v1/data/quant.duckdb`、`quant_report.duckdb`、`quant-research-cn/data/quant_cn_report.duckdb`、`data/strategy_backtest_history.duckdb`(factor_lab.duckdb 本次移除)。
- `quant-research-v1/data/` 累积了**几十个按日 session 快照**(`quant_research_2026-06-XX_{pre,post}.duckdb`、`quant_report_2026-06-XX_*`,每个 ~1.5GB)——可丢/冷藏,非 live。
- 不可再生的 JSONL 账本:`ai_infra/data/universe_membership_history.jsonl`(PIT 账本)、`ai_infra/data/virtual_holdings.json`(gitignored)。
- 两条 Rust 入口:`quant-stack daily`(老,通用/CN)vs `quant-stack us-daily`(新,带 retry/lock/告警)。cron `us.*` 实际走 us-daily(经 `run_full.sh`)。
- WSL 专有逻辑:邮件任务的 7897 端口自动代理注入(`_default_gateway_ip()`);`ops/catch_up.py` 的"WSL 睡眠不补跑"假设。
- 本地 DuckDB 版本 **1.4.4**。

## 硬约束(每阶段都要守)

- **DuckDB 单写者**:迁移/压实必须在无 cron 写入的窗口做;读用 `read_only=True`。
- **证据门 / money gate / 反捏造契约**:本次不碰逻辑,但任何脚本改动不得绕过(`docs/AI_SUPERCYCLE_PIPELINE_CONTRACT.md`、`docs/REPORT_DELIVERY_CONTRACT.md`)。
- **永不外发** `main_strategy_v2_backtest.md`(内部"厨房小票")。
- **改 Rust**:`quant-stack` + `quant-fetcher` 在根 workspace,仓根 `cargo build --release` 一次全编。
- **发布纪律**:不直接推 main;走专门分支 + `ops/review_packet.sh`。

---

## 数据生命周期与拓展性(hot/cold 分层)

**为什么不怕涨**:数据是 EOD 日频 + 有界 universe(~1150 标的),增长**线性于时间**——7.7GB ≈ 2 年 → ~3-5GB/年(期权链为大头),10 年也 ~40-50GB,DuckDB 单机长期够用。**结构性地小,不是侥幸。**

**拓展靠分层,不靠换引擎:**
- **热(hot)** = 最近 6-12 个月工作集 → DuckDB,小而快,在 Oracle。每日跑批只碰热库,**永远小**(与总历史无关)。
- **冷(cold)** = 全部历史 → 按 `年/月` 分区的 **Parquet**,放 NAS。DuckDB 原生 `read_parquet('cold/year=*/month=*')`,**只加载碰到的分区**(列存 + 分区裁剪),回测时临时扫,不污染热库。
- 这套能扩到几百 GB ~ TB 级**不用上服务端 DB**——也是 DuckDB 作为长期选择的依据。

**机制**:滚动窗口——超热窗的月份由 Pi 定期从 Oracle 导出成 Parquet 分区、推 NAS 冷湖,并从热库 drop。日常读热库;回测按需 `read_parquet` 冷湖(频繁则 Pi 把所需分区拉到 Oracle 本地再扫)。

**唯一悬崖**:从 EOD 改成 **intraday/tick**(量跳 100-1000×)→ 届时再评估(分区 Parquet 湖能扛很多,极端上专用 TSDB);明确触发下的未来决定,现在不预建(YAGNI)。

## 取数解耦:常驻 ingestion worker

**原则**:报告流水线**不取数**,只读已备好的数据 + 查新鲜度;取数是独立后台 worker 持续喂。**取数时间与出报告时间彻底脱钩。**

- **一源一 worker,各自节奏**:Finnhub / FRED / SEC / CBOE / yfinance / Tushare / 北向… 各按自己限速慢拉,不在流水线时刻一锅端。
- **幂等 + 增量 + 续跑**:`INSERT OR REPLACE` by PK;`fetch_state` 水位表(source / last_fetched / last_success)只拉新/过期;死了下次接着跑。**把一次猛拉摊成连续小拉,限速不再是 deadline。**
- **DuckDB 单写者 → 各写 raw + 串行合并**:每 worker 写自己的 raw(per-source Parquet / 小 duckdb);一个 flock 串行 `consolidate` 合进热库;流水线只读已合并热库。绕开写竞争,天然接 raw→热→冷 Parquet。
- **日内 + 收盘双 cadence**:日内轮询(期权/quote 给执行门,`*/30`,照搬现有 `intraday.index_refresh`)+ 收盘 EOD 拉满,共喂同一 raw。
- **跑在哪**:主力 worker → Oracle `systemd service/timer`(常驻 + 限速 + 续跑);个别狠 API → 家侧 Pi 拉后推 Oracle。
- **流水线改动(就一处)**:`run_daily.py` 第 1 步从"现拉 quant-fetcher"→"校验 raw 就绪 + 够新鲜"(沿用 `--max-staleness-days` 容差);够新鲜直接算,缺关键源走健康门(抑制 + 检修)。

```
[各源 worker:常驻/限速/续跑] → 各写 raw → [flock 串行 consolidate] → 热 DuckDB
                                                                        ↑ 只读
                              [报告流水线:到点只读热库 → 算 → 叙述 → 投递]  ← 不取数、快
```

## 阶段 0 — 数据瘦身(先做,解锁迁移)

**产物**:`ops/data_inventory.py`(分类)、`ops/compact_db.sh`(EXPORT/IMPORT 压实)、`ops/archive_factor_lab.py`(归档)、一份 `docs/DATA_RETENTION.md`。

1. **盘点 + 分类**:扫所有 `*.duckdb`,标记 canonical / 按日快照 / 可弃。输出体积报告 + 压实后预估。
2. **压实 canonical 库**:对每个 canonical 库,无写窗口内 `EXPORT DATABASE '<tmp>' (FORMAT PARQUET)` → 新建库 `IMPORT DATABASE` → 校验行数一致 → 原子替换。预期总体积从几十~上百 GB 降到个位数 GB。
3. **保留 + 热/冷策略**:按日 session 快照 `quant_*_2026-XX-XX_*.duckdb` 仅留最近 N 天(建议 7),其余删;canonical 走滚动窗口——超热窗(建议 6-12 月)的历史导出 Parquet 分区 → NAS 冷湖、从热库 drop。写进 `docs/DATA_RETENTION.md` + cron 清理/归档任务(进 `tasks.yaml`)。
4. **factor-lab 移除(方案 A)**:
   - 归档:`paper_returns` + `factor_registry` → `ai_infra/archive/factor_lab_final_2026-06-24.parquet`(几十 KB)。
   - 删 cron:`tasks.yaml` 移除 `factor.cn.daily`、`factor.us.daily`、`paper.record`、`paper.evaluate`、`paper.report`、`factor.maintenance.weekly`;`render_cron.py` 重生成。
   - 主链:`crates/quant-stack-cli/src/main.rs` 与 `us_daily.rs` 的 FactorLab refresh/import 步骤——因已 `is_dir()` 守卫,**移除 factor-lab 目录即自动跳过**;本阶段额外把相关 step 显式标记为 deprecated/删除,避免日志噪声。
   - `verify_ai_supercycle_readiness.py:425` 对 `daily_factors.sh` 的引用要清理。
   - 12GB 的 `factor_lab.duckdb` 归档后不迁、删除。

**验证**:压实后每个 canonical 库 `count(*)` 与压实前一致;一次完整 `us.postmarket` + `cn.evening` dry-run 跑通且报告无 factor-lab 报错;`smoke-check.sh` 通过。
**回滚**:压实是"新建+替换",保留原文件至验证通过;factor-lab 归档后再删。

---

## 阶段 1 — GitHub 当部署源

**现状**:代码已在 `github.com/hzy-hits/QuantStack` main,密钥干净。本阶段把它正式确立为部署源。

1. **审计 `.gitignore` 完整性**:确认 data/密钥/快照全屏蔽、curated 种子(`global_universe_v2.jsonl`、taxonomy、seed map)仍 tracked。补一条 `*.parquet` 归档的取舍。
2. **部署拉取模型**:Oracle 通过 `git pull` 取代码;**数据与密钥永不走 git**(走阶段 2 的迁移脚本 + 手工 provision)。写 `deploy/README.md` 说明"代码 from git / 数据 from 迁移 / 密钥 from 手工"。
3. **可选 CI**:仅 lint + `cargo build --release` + 单元测试的 GitHub Action(不碰数据、不发邮件)。本阶段标为 optional。

**验证**:在干净目录 `git clone` + provision 后,`cargo build --release` 成功,`smoke-check.sh`(无数据模式)通过。

---

## 阶段 2 — Oracle 热算 / NAS 冷湖 / Pi broker

活 DuckDB **只在 Oracle**(单写者最佳形态);NAS 只存 Parquet 冷历史;Pi 家侧发起归档 + 巡检。

### 2a — Oracle(Ampere A1)= ingest + 热算 + 投递

1. **Provision**(`deploy/oracle/provision.sh`,幂等):uv+Python3.11、Rust(aarch64)、DuckDB 1.4.4(版本对齐)、**codex CLI + claude CLI 无头认证**(narrator)、DeepSeek key;`config.yaml`/`credentials.json`/`token.json` 手工 provision + `chmod 600`(CN auth 软链重建)。
2. **迁热数据**:阶段 0 压实的 canonical(~7.7GB)`EXPORT DATABASE` → Oracle `IMPORT`;不可再生 JSONL 账本一并迁。**Oracle 是活库唯一写者**,WSL 停写,不双写。
3. **取数**:收盘后直连 API 拉 EOD(无 deadline,限速可接受,如 cboe 429/success≥95%);**若某 API 特别狠**,只让那一个在家侧(Pi)拉、结果推 Oracle——不为它把整个数据层搬回家。
4. **去 WSL 化**:禁 7897 代理(`QUANT_DISABLE_AUTO_PROXY=1` 或删 `_default_gateway_ip()`);`ops/catch_up.py` 去"WSL 睡眠"假设、留崩溃兜底;校验时区(Asia/Shanghai)。

### 2b — NAS 冷 Parquet 湖 + Pi broker/watchdog

1. **冷湖**:NAS 存按 `年/月` 分区的 Parquet 历史 + 密钥/报告归档。**只存 Parquet,不跑活 DuckDB。**
2. **Pi broker**(家侧 always-on,家→云 outbound 穿 NAT):定期从 Oracle 导出超热窗的老分区 → Parquet → 推 NAS;并把 Oracle 报告/快照拉回 NAS 冷藏。
3. **Pi watchdog**:巡检 Oracle 可达性 + `ops/state/*.last_success.json` 新鲜度 + NAS 同步;异常**独立于 Gmail** 告警(Telegram/ntfy)。

**验证**:Oracle `ops/run_task.sh research.bubble_hedge_radar` 跑通;完整 `us.postmarket` + `cn.evening`(test)出报告;narrator codex 跑通 + DeepSeek fallback;Pi 能把一个老分区归档到 NAS 且被 Oracle `read_parquet` 读到。
**回滚**:WSL 实例只读可用,直到 Oracle 链路连续稳定 N 天。

---

## 阶段 3 — 架构收敛(有限)

**只动三块,且先调研再决定(不盲目重写)。**

1. **取数/计算解耦(见 §取数解耦:常驻 ingestion worker)+ 热/冷写出**:fetch 剥成常驻 worker(各写 raw → flock 串行合并热库);`run_daily.py` 第 1 步改为"校验 raw 就绪 + 新鲜",compute 读热库、不内联取数。加滚动归档:超热窗月份导 Parquet 分区供 Pi 推 NAS 冷湖。
2. **两条 Rust 入口合一**:先产出一份 `daily` vs `us-daily` 的差异分析(retry/lock/告警/session 处理),确认 `daily` 是否还有活引用;若 us-daily 是事实标准,把 CN 路径也收敛到同一入口,`daily` 标记 deprecated 后删除。**这一步带显式"调研→评审→再改"门,不在本 spec 里预设结论。**
3. **去 WSL 化收尾**:与阶段 2 重叠的清理统一收口,确保代码里无 WSL 专有分支。

**明确不做**:重排 36 个 research 脚本目录、统一所有 cwd、把 factor-lab 物理并入(已移除)。

**验证**:合一后 `us.*` 与 `cn.*` 全链路 dry-run 与合一前报告逐项对齐(diff 无实质差异)。

---

## 调度模型决策(阶段 2 内)

沿用 `ops/render_cron.py` 生成 crontab(`ops.catch_up` 在真 Linux 仍作崩溃兜底),**暂不换 systemd timer**——render_cron 已是 single source of truth,换 systemd 是额外动土,留作后续可选。

## 风险

- **压实/迁移期间撞 cron 写锁** → 在无写窗口 + 停 cron 下做。
- **DuckDB 版本不齐导致直接 copy 失败** → 用 EXPORT/IMPORT 规避;Oracle 装 1.4.4。
- **Rust 入口合一引入回归** → 阶段 3 带 dry-run 逐项对齐 + 回滚分支。
- **遗漏不可再生账本** → 阶段 0 盘点明确标注"不可再生",迁移清单强制覆盖。
- **Pi→NAS 冷归档失败/滞后** → Pi 带成功标记 + 重试,watchdog 告警;冷湖滞后不影响日常(只动热库),仅回测历史受限。
- **Oracle 直连 API 限速** → EOD 无 deadline、慢拉可接受;个别狠 API 改家侧(Pi)拉后推 Oracle。
- **热库丢失** → 近期可由冷 Parquet 湖 + 不可再生账本重建;canonical 仍定期备份到 NAS。

## 待评审后转实现

评审通过 → 用 writing-plans 出逐任务实现计划(每步含确切命令/校验/回滚)。
