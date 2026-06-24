# Quant-Stack 可移植性重构 + Oracle 迁移 — 设计 Spec

Date: 2026-06-24 · 状态:待评审 · 作者:operator + Claude

## 目标

把生产 quant-stack 从 WSL2 上"能跑但臃肿、绑死本机"的状态,重构成**瘦身、可移植、以 GitHub 为源、数据层在 NAS 取数发快照、计算在 Oracle ARM** 的状态。两条主线:

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
| 数据层引擎 | **DuckDB 全程保留,不换 Postgres** | 单机 OLAP 最佳命中区;换 Postgres = 重写全部查询 + 行存反而慢;Parquet 原生导出留好出口 |
| 部署拓扑 | **NAS 取数 + 发快照 / Oracle 拉快照计算**(快照拉取,非跨网共享 live 库) | DuckDB 单写者、不支持网络共享读写;取数 I/O 密集适合弱 NAS,重算交 Oracle |
| NAS | **群晖 DS220+**(Celeron J4025 双核 x86_64 / 2–6GB / Docker) | 当数据家 + 轻量 Docker ingest worker + 备份;不跑重计算 |

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

## 阶段 0 — 数据瘦身(先做,解锁迁移)

**产物**:`ops/data_inventory.py`(分类)、`ops/compact_db.sh`(EXPORT/IMPORT 压实)、`ops/archive_factor_lab.py`(归档)、一份 `docs/DATA_RETENTION.md`。

1. **盘点 + 分类**:扫所有 `*.duckdb`,标记 canonical / 按日快照 / 可弃。输出体积报告 + 压实后预估。
2. **压实 canonical 库**:对每个 canonical 库,无写窗口内 `EXPORT DATABASE '<tmp>' (FORMAT PARQUET)` → 新建库 `IMPORT DATABASE` → 校验行数一致 → 原子替换。预期总体积从几十~上百 GB 降到个位数 GB。
3. **快照保留策略**:`quant_*_2026-XX-XX_*.duckdb` 仅保留最近 N 天(建议 7),其余 tar 进 OCI Object Storage 冷藏或删除;写进 `DATA_RETENTION.md` + 加一个 cron 清理任务(进 `tasks.yaml`)。
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

## 阶段 2 — NAS + Oracle 落地(快照拉取拓扑)

数据层不跨网共享 live DuckDB。**NAS 取数→发布冻结只读快照→Oracle 拉快照计算。** 接缝:NAS 交付 raw 表(取来的:prices/options/news/macro/flow),Oracle 跑 derived 表(算出来的:analysis_daily 等)。

### 2a — NAS(群晖 DS220+)立 ingest worker + 快照发布

1. **Docker ingest worker**:Container Manager 跑一个容器(x86_64,匹配 DS220+),内含 x86 build 的 `quant-fetcher`(Finnhub/FRED/SEC)+ Python fetchers + AKShare bridge。收盘后慢速拉 EOD(限速无所谓),写 NAS 卷上的 DuckDB raw 表。
2. **快照发布**:ingest 完成后冻结一份只读快照(`EXPORT DATABASE` parquet 或拷一份 `.duckdb`),放到 Oracle 可拉取的目录。NAS 是 raw 库的**唯一写者**。
3. **备份家**:canonical 库 + 不可再生 JSONL 账本 + parquet 归档统一在 NAS 留存(NAS 本职)。

### 2b — Oracle(Ampere A1)拉快照 + 重计算

1. **Provision**(`deploy/oracle/provision.sh`,幂等):uv+Python3.11、Rust(aarch64)、DuckDB 1.4.4(版本对齐)、**codex CLI + claude CLI 无头认证**(narrator)、DeepSeek key(`api.deepseek_key`)。
2. **拉快照**:`rsync` 从 NAS 拉冻结快照到本地 → 计算阶段**直接读本地库,不碰任何接口**(核心诉求)。derived 结果写 Oracle 本地 canonical 库。
3. **去 WSL 化**:禁用 7897 代理自动注入(`QUANT_DISABLE_AUTO_PROXY=1` 或删 `_default_gateway_ip()`);`ops/catch_up.py` 去掉"WSL 睡眠"假设、保留崩溃兜底;校验 host 时区(Asia/Shanghai)。
4. **密钥**:`config.yaml`/`credentials.json`/`token.json` 手工 provision,`chmod 600`,绝不入 git。

**写者边界**:NAS 写 raw,Oracle 写 derived,**各写各的库,不双写同一库**。

**验证**:NAS worker 产出 raw 快照且行数/日期范围达标(用 `quant-research-cn/.claude/skills` 的 verify-data 思路);Oracle 拉快照后 `ops/run_task.sh research.bubble_hedge_radar` 跑通;完整 `us.postmarket` + `cn.evening`(test 模式)出报告;narrator codex 跑通 + 验证 DeepSeek fallback。
**回滚**:WSL 实例保持只读可用,直到 NAS+Oracle 链路连续稳定 N 天。

---

## 阶段 3 — 架构收敛(有限)

**只动三块,且先调研再决定(不盲目重写)。**

1. **取数/计算解耦**:把现 pipeline 内的 fetch 步正式剥离为独立的 ingest 阶段(产物即 raw 快照),供 2a 的 NAS worker 承载。compute 入口改为"读已就绪的 raw 快照",不再内联取数。
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
- **NAS→Oracle 快照同步失败/陈旧** → rsync 带成功标记 + Oracle 计算前校验快照日期;失败则告警并复用上一份(宁可用昨日数据出报告,也不静默用半截快照)。
- **DS220+ 算力/内存不足跑 ingest** → worker 仅做 I/O 密集取数(双核够);若 AKShare bridge 内存吃紧,限并发 + 分批。

## 待评审后转实现

评审通过 → 用 writing-plans 出逐任务实现计划(每步含确切命令/校验/回滚)。
