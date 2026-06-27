# 取数解耦(常驻 fetch worker + consolidate + 新鲜度门)— Design Spec

Date: 2026-06-27 · 状态:待评审 · 作者:operator + Claude

## 目标

把**收盘 EOD 取数**从报告流水线里剥出来:各源由独立常驻 worker 持续慢拉、各写自己的
staging 库;一个串行 `consolidate` 作业把 staging 合进热库;报告流水线**不取数**,只读已合并
热库 + 查新鲜度。**取数时间与出报告时间彻底脱钩**,流水线不再因取数攥着写锁跑满全程。

落地在 WSL(现状环境),形态对齐后续 Oracle 迁移(systemd timer 取代 cron,逻辑不变)。

## 范围

**做**:EOD 取数解耦,两个市场。共享架构(fetch_state 水位表 + per-source DuckDB staging +
consolidate + 新鲜度门 + 锁纪律)一份 spec;实施拆两个 plan:**CN 先(已有 `--skip-fetch`,最快
跑通)→ US 后(需新加 `--skip-fetch` + 拆长写锁)**。

**不做(YAGNI,显式排除防蔓延)**:
- **intraday 解耦** —— `intraday.index_refresh`(`*/30` 盘中拉 CBOE 链写 raw)**本来就是独立
  cron worker**,已是目标形态,不碰。本次只动收盘 EOD 内联取数。
- **冷湖 Parquet 归档 / 滚动窗口导出** —— 属迁移阶段(Pi 把超热窗分区导 Parquet 推 NAS)。
  本次 staging 用 DuckDB,冷湖仍是 Parquet,由迁移阶段单独做;staging 格式不阻碍它。
- **per-source Parquet staging** —— 已评估否决(CN Rust 写 DuckDB,改 Parquet 要重做 schema 映射
  + INSERT OR REPLACE 读-并合,活多收益小);选 per-source DuckDB。
- 两条 Rust 入口合一、去 WSL 化收尾(portability 阶段 3 的另两块,独立处理)。

## 现状关键事实(2026-06-27 并行深读,带位置)

### US(Python `quant-research-v1`)
- 入口 `scripts/run_daily.py:401`;**内联取数 stage 2a-5**:fundamentals(Finnhub)`:514`、
  prices(yfinance)`:538`、Rust `quant-fetcher`(Finnhub news/earnings、FRED、SEC EDGAR、
  Polymarket)`:716`、options(CBOE)`:784`、market_quotes(Yahoo)`:792`。
- 写热库 `data/quant.duckdb`;**写锁(fcntl.flock,`storage/db.py:262`)在整条管线全程持有**。
- **无 `--skip-fetch` 模式**(需新加)。新鲜度仅 `refresh_days` 粗检(`fundamentals.py:34` 等),
  无水位表。
- fetcher 模块化:`src/quant_bot/data_ingestion/{prices,options,fundamentals,market_quotes,…}.py`
  + `crates/quant-fetcher`。

### CN(Rust `quant-research-cn` / `quant-cn`)
- 入口 `src/main.rs:167` `Command::Run`;`fetch_all` `:171`(Tushare 全量 + AKShare 北向),
  **`--skip-fetch` 已存在**。
- 写热库 `data/quant_cn.duckdb`(config);Tushare 500ms 限速;**无 fcntl 锁 → 并发写会损库**。
- fetcher 模块化:`src/fetcher/tushare/{prices,flow,fundamental,event,macro_cn,market}.rs` + `akshare.rs`;
  orchestrator `tushare/mod.rs:136 fetch_all`。

### 共享
- **已解耦取数任务范例(模板)**:`intraday.index_refresh`、`research.cn_index_ingest`、
  `research.cn_flow_signals`、`research.satellite_index_ingest` —— 都已是"独立 cron 写 raw + 各自 lock"。
- `INSERT OR REPLACE` 幂等贯穿两市;`ops/run_task.py` + `tasks.yaml` 的 `lock:` 字段已是 fcntl.flock。
- `tasks.yaml` 全为 cron(无 service/timer);crontab 由 `ops/render_cron.py` 生成,不手改。

## 设计

### 拓扑与数据流
```
每源 worker(cron,限速/增量/续跑,无锁并行)
   └─ 各写 staging/{source}.duckdb ─────────────┐
                                                 ▼
                       consolidate 作业(cron,fcntl.flock 串行,唯一热库写者)
                          ATTACH 各 staging → INSERT OR REPLACE 进热库 → 更新 fetch_state.consolidated
                                                 │
报告流水线(到点) ── 查 fetch_state 新鲜度 ──────┤
   ├─ 够新鲜 → 快照 raw→research(短读)→ 算 → 叙述 → 投递
   └─ 关键源超期 → fail-closed:不出报告,发检修邮件给 operator(+ 前端 agent sink)
```

### 组件 1 — `fetch_state` 水位表(两市各一份,建在各自热库)
DDL:
```sql
CREATE TABLE IF NOT EXISTS fetch_state (
  source       VARCHAR NOT NULL,   -- 'us.prices' / 'cn.tushare.moneyflow' …
  scope        VARCHAR NOT NULL DEFAULT '*',  -- 细分(如某指数/某 universe 分片),默认 '*'
  criticality  VARCHAR NOT NULL DEFAULT 'optional',  -- 'critical' | 'optional'
  max_staleness_days INTEGER NOT NULL DEFAULT 3,
  last_fetched_date    DATE,        -- worker 拉到的最新交易日
  last_fetched_at      TIMESTAMP,   -- worker 上次成功落 staging 的时刻
  last_consolidated_date DATE,      -- 已并入热库的最新交易日
  last_consolidated_at   TIMESTAMP,
  status       VARCHAR,             -- 'ok' | 'error' | 'stale'
  rows_written BIGINT DEFAULT 0,
  error        VARCHAR,
  PRIMARY KEY (source, scope)
);
```
worker 更新 `last_fetched_*` + status;consolidate 更新 `last_consolidated_*`。
`criticality` / `max_staleness_days` 是每源策略,初值由一份源注册表 seed(见组件 4)。

### 组件 2 — 每源 fetch worker(无锁并行,增量,续跑)
- **CN**:把 `fetch_all` 拆出 fetch-only 子命令 `quant-cn fetch --source <name> --staging <path>`
  (复用现有 `tushare/*.rs`、`akshare.rs` 写路径,只把目标连接从热库换成 `staging/{source}.duckdb`);
  每源一个 `tasks.yaml` cron 任务,各自 lock,收盘后慢拉。
- **US**:把 `run_daily.py` stage 2a-5 抽成 `scripts/ingest_us.py --source <name> --staging <path>`
  (复用现有 `data_ingestion.*` 模块 + `quant-fetcher`,目标库换 staging);每源一个 cron 任务。
- 增量:worker 读 `fetch_state.last_fetched_date` 决定拉取窗口(只拉新/过期);`INSERT OR REPLACE`
  幂等;进程死了下次接着拉。worker **只写自己的 staging,不碰热库,不抢锁**。
- 写完更新 `fetch_state`(可写在 staging 内的镜像表,consolidate 时一并并入;或直接短锁写热库的
  fetch_state——MVP 取后者:worker 仅对 `fetch_state` 一行做短事务,避免读写竞态)。

### 组件 3 — `consolidate` 作业(唯一热库写者,fcntl.flock 串行)
- `scripts/consolidate_raw.py --market us|cn`:取热库写锁(fcntl.flock,`storage/db.py` 同款)→
  逐个 `ATTACH 'staging/{source}.duckdb' AS s; INSERT OR REPLACE INTO <hot>.<table> SELECT * FROM s.<table>;`
  → 更新 `fetch_state.last_consolidated_*` → 释放锁。幂等、可重入。
- 调度:每个 pipeline 窗口前若干分钟各排一次(如 US `*/... `、CN 早/晚盘前);亦可在 pipeline
  step 0 以幂等方式自调用一次兜底。consolidate 是**唯一**热库写者,堵住 CN Rust 当前的无锁并发损库隐患。

### 组件 4 — 新鲜度门(报告流水线 step 1 改造)
- **CN**:`cn.morning/evening` 命令加 `--skip-fetch`;compute 前调用共享新鲜度检查。
- **US**:给 `run_daily.py` 新加 `--skip-fetch` 模式(目前没有,内联 stage 2a-5 全跳过);
  同样先做新鲜度检查;并把"全程写锁"改成"仅 raw→research 快照时短锁"。
- **策略(已定默认)**:每源按 `fetch_state.criticality` + `max_staleness_days` 判定:
  - **critical 源超期** → **fail-closed**:不 compute、不投递报告,改发**检修邮件给 operator**
    (走与 Gmail 报告独立的告警路径;对齐"服务异常发检修不发报告"铁律)。
  - **optional 源超期** → 照常 compute,报告里记一句容差/降级说明。
- 源注册表(`ops/fetch_sources.yaml` 或 seed 进 fetch_state):列每源的 criticality + max_staleness +
  对应 worker 任务名,供新鲜度门、consolidate、巡检共用单一真相源。

### 组件 5 — 锁纪律
- **热库写锁**:仅 `consolidate` 持有(fcntl.flock)。worker 写各自 staging、零锁。pipeline 仅在
  raw→research 快照那一刻短暂读 raw(与 consolidate 互斥用同一把锁)。
- CN Rust 取数从此不直写热库 → 当前"无锁并发损库"隐患消除。

## 拆分交付
- 本 spec = 共享设计。实施两 plan:
  1. **CN plan(先)**:fetch_state(CN 库)+ `quant-cn fetch` 子命令 + CN staging + `consolidate_raw.py --market cn`
     + `cn.morning/evening` 加 `--skip-fetch` + 新鲜度门 + CN 源注册。`--skip-fetch` 已存在,最快验证模式。
  2. **US plan(后)**:fetch_state(US 库)+ `scripts/ingest_us.py` + US staging + `consolidate_raw.py --market us`
     + `run_daily.py` 新加 `--skip-fetch` + 拆长写锁 + 新鲜度门 + US 源注册。

## 测试
- worker 单测:增量窗口(读水位只拉新)、`INSERT OR REPLACE` 幂等(重复跑不重复行)、续跑(中断后接着拉)。
- consolidate 单测:两个假 staging.duckdb → ATTACH 合并 → 热库行数/PK 正确;重入幂等;锁互斥(并发两个 consolidate 一个等待)。
- 新鲜度门单测:critical-stale → 返回 fail-closed 信号(不 compute、产检修事件);optional-stale → 通过 + 容差标记;全新鲜 → 通过。
- 冒烟(test 模式不发邮件):跑通一个市场 EOD —— worker 落 staging → consolidate 进热库 → pipeline `--skip-fetch` 读热库出报告。
- 回归:CN 现有 `cn.evening`(test)报告内容在解耦前后逐字段不劣化。

## 回滚
- 管线去掉 `--skip-fetch`(US 去掉新模式)即回到内联取数;worker/consolidate 任务可留着空跑无副作用。
- staging 库可删(下次 worker 重建)。fetch_state 是新表,不影响既有表。

## 风险
- **worker 与 consolidate 之间的 fetch_state 竞态**:MVP 用 worker 对 fetch_state 单行短事务规避;
  若仍抖,退化为"fetch_state 镜像写进 staging,consolidate 时统一并入"。
- **首跑/补数窗口**:新源 staging 为空 → consolidate 无行可并 → 新鲜度门按 critical 判 fail-closed(预期行为,提示先补数)。
- **CN Rust fetch-only 子命令的 cwd/config**:`quant-cn` 仍需 cwd=quant-research-cn 读 config.yaml + data/;staging 路径相对 cwd,plan 里固定。
- **US 拆长写锁**:run_daily.py 当前依赖"全程持锁"的隐式串行;拆锁后要确认 compute 阶段不再写 raw(只写 research),plan 里逐 stage 核。

## 自查
- 范围聚焦单一能力(EOD 取数解耦),显式排除 intraday/冷湖/入口合一,防蔓延。
- 命名一致:`fetch_state`、`staging/{source}.duckdb`、`consolidate_raw.py`、`--skip-fetch`、
  `criticality∈{critical,optional}` 全文一致。
- 单写者纪律:consolidate 是热库唯一写者,worker 零锁,pipeline 只读 + 短快照锁——与 DuckDB 单写者约束自洽。
- 与 portability spec 一致:本 spec 实现其 §取数解耦 + 阶段 3.1 的取数/计算解耦;冷湖归档留迁移阶段。
