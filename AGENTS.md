# Quant Stack Agent Operating Manual

版本日期：2026-05-13

本文件是后续 agent 进入本仓库时的顶层入口。先读这里，再读被引用的详细文档。

## 一句话目标

Quant Stack 要收敛成一套 AI Infra 专门基金研究管线，而不是泛市场股票扫描器：

```text
ai_infra 原文研究 / BFS 发现
  -> source-review 晋级
  -> AI universe / relationship ledger
  -> 美股和 A 股量化特征、K线、期权/影子期权
  -> sleeve 排序和组合权重
  -> 对 SPY / QQQ / SMH / 上证 / 深成指等 benchmark 做归因
  -> 每日报告
```

核心原则：研究从 LLM 需求和 AI 基础设施依赖出发；交易候选只能来自已晋级的 AI universe；Factor Lab 和 autoresearch 负责发现和验证，不直接把新公司塞进交易候选。

## Agent 必读顺序

1. 本文件：仓库目标、边界和执行规则。
2. `CLAUDE_HANDOFF.md`：给 Claude/其他 agent 的当前仓库状态、验证结果和接手提示。
3. `docs/AI_INFRA_SPECIALIST_PIPELINE_REORG.md`：AI Infra 专门基金改造方案。
4. `docs/AI_SUPERCYCLE_PIPELINE_CONTRACT.md`：生产候选、source evidence、报告约束。
5. `docs/AI_INFRA_QUANT_FUND_INTEGRATION.md`：`ai_infra` 与量化系统的集成关系。
6. `docs/MODULE_BOUNDARIES.md`：US producer、CN producer、Factor Lab、shared gate、reporting 的职责边界。
7. `docs/PROJECT_CONSOLIDATION_PLAN.md`：当前不要乱搬目录，先用 root ops/control plane 收敛。
8. `ai_infra/START_HERE.md` 和 `ai_infra/docs/README.md`：`ai_infra` 的研究入口。

`ai_infra` 方法论文档：

| 文件 | 用途 |
| --- | --- |
| `ai_infra/docs/fund-management-philosophy.md` | AI Infra BFS 基金哲学：从 LLM 需求出发，不从股票故事出发。 |
| `ai_infra/docs/llm-dependency-bfs-framework.md` | BFS 深度 D0-D5、边关系和 A 股映射规则。 |
| `ai_infra/docs/company-financials-market-options-methodology.md` | 财报、K线、期权三层研究方法，Evidence first。 |
| `ai_infra/docs/research-checklist.md` | 公司研究 checklist。 |
| `ai_infra/docs/source-evidence-template.md` | 原文证据卡模板。 |
| `ai_infra/docs/credit-financing-evidence-card-template.md` | NeoCloud、数据中心、重资产公司的融资和信用反证。 |
| `ai_infra/docs/firm-power-evidence-card-template.md` | 电力、并网、firm power 反证。 |

## 不可违反的边界

1. 本项目不是泛市场选股系统。 broad market 数据可以用于 benchmark、liquidity、macro、beta、hedge，但不能把非 AI 公司作为 production stock candidate。
2. 生产股票候选必须来自 `ai_infra/data/global_universe_v2.jsonl` 或 source-reviewed promotion output。
3. Factor Lab、BFS、news/filing extraction 发现的新公司只能先进入 source-review expansion queue。
4. 没有原文证据，不能写成 source-confirmed supplier/customer relationship。
5. K线只能说明 pricing、trend、crowding、risk，不能证明公司有 AI 收入、订单、客户或 backlog。
6. 期权只能说明 volatility、event pricing、liquidity、crowding clue，不能提升 evidence status。
7. 任何新 ticker 进入交易候选前，必须先有 evidence card / source registry / relationship ledger 或 universe 晋级记录。
8. 美股报告里的公司名保持原始英文名；A 股和港股可以显示中文名。
9. 每日报告必须区分 production candidate、watch/research-only candidate、benchmark/hedge/context。
10. 本仓库不是 broker；rebalance / execution ledger 只记录研究建议、paper/intended tilt 或人工确认状态，不代表真实下单。
11. 不要把当前凌乱 worktree 直接推到 `main`。需要发布时，建专门分支、分逻辑提交、生成 review packet。

## 模块职责

### `ai_infra/`

上游研究工作台，负责：

- 从 OpenAI、Anthropic、Google DeepMind、Meta、xAI、DeepSeek 等 D0 需求源头开始做 BFS。
- 维护 D1-D5 的 AI 基础设施链条、边关系、反证层。
- 生成 source review queue、BFS supply chain discovery queue、expansion candidates。
- 维护 evidence card、source registry、relationship ledger、global universe。
- 输出可以被量化系统消费的 AI universe 和研究队列。

`ai_infra` 不负责直接给交易 R，不负责绕过证据流程。

### `factor-lab/`

研究和因子发现层，负责：

- 读取财报、公告、原文、新闻、产业链资料，生成 hypothesis。
- 发现新的上下游公司，例如 DGXX 这类当前不在 universe 的名字。
- 写 `DATA_REQUIREMENTS`、source-review expansion candidate、research priority。
- 对 AI universe 内公司做 factor hypothesis 和 sleeve-return 研究。

Factor Lab 不能把未 source-reviewed 的公司直接加入 ranker、日报 production 候选或真实仓位建议。

### `quant-research-v1/`

美股 producer，负责：

- 只在 AI universe 或已晋级 promotion output 内做候选排序。
- 计算 price/volume、relative strength、options、event、fundamental-derived features。
- 对 `SPY`、`QQQ`、`SMH` 做相对表现、beta、alpha、drawdown、hit rate 归因。
- 输出美股 AI sleeve 候选、blocked reasons、报告 payload。

### `quant-research-cn/`

A 股 producer，负责：

- 将海外 AI Infra 瓶颈映射到 A 股和港股公司。
- 使用 A 股市场数据、资金流、公告、板块联动、T+1/T+3 生命周期证据。
- 对 `000001.SH`、`399001.SZ`、`399006.SZ`、`000300.SH` 做 benchmark 归因。
- 输出 CN AI sleeve 候选、blocked reasons、报告 payload。

### `crates/` 和 root `ops/`

共享控制面，负责：

- shared alpha gate、review ledger、report model、bulletin。
- 任务注册、cron 渲染、root wrapper、review packet。
- 确保 US/CN producer 的输出进入同一套 alpha maturity 和报告结构。

## 新公司发现与晋级流程

任何新公司，不管来自 ChatGPT Pro、新闻、财报、筛选器、K线异动还是人工发现，都走同一路径：

```text
new ticker / company lead
  -> ai_infra expansion candidate
  -> source-review queue
  -> evidence card + original sources + counterevidence
  -> relationship ledger / global universe promotion
  -> market data and factor computation
  -> sleeve ranking
  -> production candidate or watch/research-only
```

例子：DGXX / dgxx 当前若不在 `global_universe_v2.jsonl`、US alpha queue、source verification queue，就只能作为 source-review expansion candidate。补完 evidence card 和原文来源后，才能进入量化扫描。

最低晋级字段：

- ticker、company name、exchange、country、currency。
- AI module、BFS depth、dependency edge、edge type。
- source status：primary / primary-ish / secondary-only / pending。
- evidence claims：客户、产品、订单、收入、backlog、产能、技术路线、现金流。
- counterevidence：库存、毛利率、债务、客户集中、融资、电力、监管、竞争。
- promotion decision：promote / watch / reject / need source。

## 量化与组合规则

量化扫描只回答 AI universe 内公司的排序、时点、风险和组合权重，不负责证明产业链关系。

推荐 sizing stack：

```text
base sleeve score
* evidence quality multiplier
* tape / entry quality multiplier
* options / flow confirmation multiplier
* portfolio concentration haircut
* benchmark beta hedge adjustment
= final stock R
```

每个 production row 至少要有：

- ticker、company name、market。
- AI layer / module / BFS depth。
- sleeve id 和 signal state。
- evidence status。
- tape / K-line quality。
- options 或 shadow-options state，若无数据需标记 `NO_OPTIONS_DATA` 或 `LOW_QUALITY_OPTIONS_DATA`。
- benchmark relative return 和 beta/hedged attribution。
- blocked reason 或 risk plan。

## Benchmark 与指数规则

允许进入系统的非 AI 标的是 benchmark、hedge、macro context：

| 市场 | 标的 | 用途 |
| --- | --- | --- |
| US | `SPY` | broad beta benchmark |
| US | `QQQ` | growth / Nasdaq benchmark |
| US | `SMH` | semiconductor beta benchmark |
| CN | `000001.SH` | 上证指数 context |
| CN | `399001.SZ` | 深成指 context |
| CN | `399006.SZ` | 创业板指 context |
| CN | `000300.SH` | 沪深300 context |
| CN proxy | `510300.SH`, `510500.SH`, futures if available | hedge / drawdown attribution |

这些标的可以出现在 benchmark table、hedge table、risk table，不能成为股票 production candidate。

每日报告至少要回答：

- AI book 是否跑赢对应 benchmark。
- 超额收益来自哪个 AI layer / sleeve / ticker。
- 当前回撤、beta、相关性、集中度是否可接受。
- 是否需要 hedge 或降权。

## 每日报告合同

日报应该固定为以下结构：

1. AI book state。
2. Production candidates。
3. Watch / research-only candidates。
4. Earnings calendar 和 source-review calendar。
5. Benchmark attribution vs `SPY` / `QQQ` / `SMH` 或 CN indices。
6. Portfolio risk、hedge state、concentration。
7. Open `DATA_REQUIREMENTS`。

报告不能把 broad-market non-AI 名字写成候选，除非明确标注为 benchmark、hedge 或 macro context。

## 常用命令

生成 AI Infra BFS 供应链 discovery queue：

```bash
python3 ai_infra/scripts/generate_bfs_supply_chain_discovery_queue.py
```

生成 expansion candidates：

```bash
python3 ai_infra/scripts/generate_expansion_candidates.py
```

检查 AI supercycle readiness：

```bash
python3 scripts/verify_ai_supercycle_readiness.py --as-of 2026-05-13 --strict
```

运行 Main Strategy V2 report：

```bash
python3 scripts/run_main_strategy_v2_backtest.py --as-of 2026-05-13
```

核心 Python smoke tests：

```bash
python3 -m unittest \
  quant-research-v1.tests.test_main_strategy_v2_backtest \
  tests.test_ai_infra_bfs_discovery_queue \
  tests.test_ai_infra_expansion_lane \
  tests.test_send_production_decision_report
```

更多当前 specialist smoke tests、source-review loop、rebalance ledger 命令见 `CLAUDE_HANDOFF.md`。

共享控制面示例：

```bash
target/release/quant-stack daily \
  --date 2026-05-13 \
  --markets us,cn \
  --session post \
  --run-producers \
  --with-narrative \
  --delivery-mode test \
  --delivery-dry-run
```

## Agent 做不同任务时怎么动手

### 添加新公司

不要直接改 ranker。先更新 `ai_infra` queue / evidence / source review，再生成候选队列。只有晋级后，才改 universe 或 relationship ledger。

### 改量化策略

先确认输入 universe 已被 AI scope 过滤。策略可以改 ranking、sleeve score、risk budget、benchmark attribution，但不能扩大股票候选范围。

### 改 Factor Lab

输出应落在 research hypothesis、`DATA_REQUIREMENTS`、source-review candidates、factor backtest artifact。不要输出 production R。

### 改日报

保持 production / watch / research-only / benchmark / hedge 五类清楚分开。财报日历和 source-review calendar 要进入每日报告。

### 发布到 QuantStack

先整理 dirty worktree，建立 `ai-infra-specialist-pipeline` 这类专门分支，按逻辑提交：

1. `ai_infra` imports / queues / evidence。
2. universe / ranker enforcement。
3. benchmark attribution。
4. report rendering。
5. ops/task registry。
6. docs/tests。

发布前运行 readiness、market report smoke tests，并生成 review packet。

## 完成定义

一次改动只有满足下面条件，才算真正完成：

- 没有把非 AI 公司放进 production stock candidates。
- 新 ticker 走了 source-review 或明确保持 research-only。
- 报告里能解释 evidence、tape、options、benchmark、risk 的角色。
- 美股用英文原名，A 股/港股可用中文名。
- 有必要的测试或 smoke command，不能运行时说明原因。
- 如果改了管线入口或 agent 规则，同步更新本文件或相关顶层文档。
