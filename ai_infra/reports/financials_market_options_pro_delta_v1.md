# Financials / K-line / Options Pro Delta v1

状态：已整合进稳定方法论  
来源：`notes/2026-05-13-chatgpt-pro-company-financials-market-options-methodology.md`  
边界：研究流程和本地工程计划，不是投资建议、买卖建议、目标价或实际仓位建议。

## 核心采纳

这次 Pro 输出最有价值的地方不是具体公司结论，而是把项目从“原文核验队列”升级成三层研究系统：

1. `Evidence / fundamentals`：证明公司是否真的在 AI Infra 链条里赚钱。
2. `Market behavior / K-line`：观察市场是否定价、拥挤和风险。
3. `Options / event risk`：识别波动、事件、流动性和尾部风险。

采纳为硬规则：

- Evidence 是硬门槛。
- K-line 只做 pricing / crowding / risk。
- Options 只做 volatility / event pricing / liquidity / crowding clue。
- Market 和 options 不能把弱证据公司升级为核心研究池。
- 所有分数都是 `research priority score`，不是交易信号。

稳定文档已落到：

- `docs/company-financials-market-options-methodology.md`

## 对现有系统的影响

| 现有模块 | 变化 |
| --- | --- |
| `global_universe_v2.jsonl` | 仍作为 seed universe，不直接变成组合 |
| `ai_infra_universe.sqlite` | 下一步从 universe 表扩展到 source、evidence、financial、price、options、refutation |
| `source_verification_queue_v1` | 继续作为原文核验入口 |
| `evidence/batch1` | 每张 card 需要新增 market behavior 和 options risk 区块 |
| `research_mvp_plan_v1.md` | 下一步应从 9 家 evidence MVP 扩展为 10-12 家三层样板 |
| `scores` | 从单一 universe priority 扩展为 evidence_score + market_score + risk_score |

## P0 Engineering Tasks

| 顺序 | 模块 | 产出 | 验收 |
| ---: | --- | --- | --- |
| 1 | `security_master` | 统一 company_id、ticker、exchange、country、currency、CIK/ISIN/ADR/local mapping | 146 家全部有 company_id；重复 ticker 可区分 |
| 2 | `source_registry` | 公司原文来源登记，含 source_id、type、period、url、hash、status | 样板公司至少 8 家有 primary source |
| 3 | `financials_extractor` | `financial_metrics.csv` + `evidence_claims.jsonl` | 财务数字必须有 source_id 和 quote/location |
| 4 | `company_card` | 增强版 company card | 原文已证明、合理推论、待核验、反证四栏齐全 |
| 5 | `price_feature_builder` | `price_daily` + `price_features` | RS、beta、RV、drawdown、liquidity 可复算 |
| 6 | `options_feature_builder` | `option_chain_snapshots` + `options_features` | 无期权数据标 `NO_OPTIONS_DATA`，低质量标 `LOW_QUALITY_OPTIONS_DATA` |
| 7 | `research_scores` | evidence/market/risk 三层 score | gate rules 保证 market/options 不覆盖 evidence |
| 8 | `refutation_dashboard` | company + module 反证信号 | 每家公司至少 3 个反证，每个核心模块至少 5 个反证 |

## P0 Sample Company Set

样板不是推荐，只是为了测试 pipeline 覆盖面。

| 模块 | 样板 | 为什么适合 MVP |
| --- | --- | --- |
| HBM / memory | MU | US-listed，SEC，期权，HBM/DRAM/eSSD 字段可测 |
| Foundry / advanced packaging | TSM | 20-F/IR，CoWoS/advanced packaging 交叉验证 |
| Custom ASIC / networking | AVGO | ASIC、networking、CPO、SEC、期权 |
| Optics / laser | COHR | datacom / AI optics，SEC，期权 |
| AI server / systems | SMCI 或 DELL | AI server、inventory、margin、customer risk |
| Power / cooling | VRT | data center power/cooling、backlog、orders、SEC、期权 |
| EDA / IP | CDNS 或 SNPS | RPO、EDA demand、AI chip design exposure |
| Connectivity | ALAB | AI connectivity、SEC、期权 |
| Storage / eSSD | PSTG 或 WDC/SNDK | enterprise storage / NAND / eSSD 区分 |
| NeoCloud | NBIS 或 CRWV | utilization、backlog、debt、lease、customer concentration |
| Non-US equipment/test | Advantest 或 TOWA | 测试 non-US source workflow 和 `NO_OPTIONS_DATA` 分支 |
| OSAT / packaging | AMKR | advanced packaging、CapEx、margin 传导 |

## Scoring Delta

旧评分主要来自 JSONL 字段和 BFS/pool 分层。新评分拆成三层：

| 层 | 权重 | 作用 |
| --- | ---: | --- |
| Evidence score | 60 | 判断 AI Infra 相关性、收入/订单/毛利/现金流传导和反证 |
| Market behavior score | 20 | 判断 evidence event 后市场是否验证、是否拥挤、流动性如何 |
| Risk budget score | 20 | 判断 beta、相关性、期权/事件、drawdown、数据质量和流动性风险 |

硬门槛不变：

- 没有 primary source，不能进核心结论。
- 只有 AI 叙事，没有收入/订单/客户/产品证据，不能进核心研究池。
- D5 无强证据只能 theme watch。
- 强反证触发时降级。

## Schema Delta

建议在现有 SQLite 后续增加：

```text
security_master
source_registry
evidence_claims
financial_metrics
price_daily
price_features
option_chain_snapshots
options_features
research_scores
refutation_signals
```

第一版不需要一次性全部做完。最小可行顺序：

```text
security_master -> source_registry -> financial_metrics/evidence_claims -> price_features -> company_card -> research_scores
```

Options 可以只覆盖 US liquid sample names；无数据时明确标注，不阻塞 evidence MVP。

## 下一步建议

1. 先把 `source_verification_batch1.csv` 中的 24 家映射到 `security_master`。
2. 选 10-12 家样板跑 `source_registry`。
3. 为 MU、VRT、COHR、TSM、TOWA 各做一张增强版 company card。
4. 写 `price_feature_builder`，先只接日频 OHLCV 和 benchmark。
5. 期权只做 US liquid names 的 `NO_OPTIONS_DATA / LOW_QUALITY_OPTIONS_DATA / OPTIONS_OK` 三态。
6. 生成 `research_scores_v2.csv`，验证 gate 是否能阻止弱证据 + 强 K-line 的误升级。

## 仍未采纳

| 建议 | 原因 |
| --- | --- |
| 直接接 IBKR | 当前阶段是 research OS，不接交易和实盘 |
| 全量 options analytics | 免费数据不稳定，先做样板和风险标签 |
| 用 K-line 或期权生成方向信号 | 违反 evidence-first 原则 |
| 自动给仓位 | 当前不是组合引擎阶段 |
