# ChatGPT Pro Prompt: Company Financials + K-line + Options Research Methodology

你是 AI Infra 研究系统的方法论顾问。目标不是投资建议，不给买卖建议、不做目标价、不输出实际仓位。

我们正在构建一个 `source-backed AI Infra research OS / fund research engine`。研究起点是 D0 LLM demand，沿 D1-D5 BFS 找 AI Infra 产业链公司。现在问题是：**对一个候选公司，应该如何系统研究财报、K线和期权数据，并把它们整合成可复用的本地研究 pipeline？**

## 当前项目原则

- 财报/原文证据决定：公司是否真的处在 AI Infra 产业链中、是否有收入/订单/产能/毛利/现金流传导。
- K线和期权数据决定：市场如何定价、拥挤程度、波动风险、事件风险和组合风险。
- 不允许用 K线/期权替代基本面证据。
- 不允许用“AI 概念”替代公司原文。
- 所有结论必须分为：原文已证明 / 合理推论 / 待原文核验 / 主要反证。

## 请设计完整研究框架

### 1. Company financials research workflow

对每家公司如何读财报？请给出标准流程：

- 先定位 BFS depth 和 dependency edge。
- 找哪些原文：10-K/20-F/10-Q、annual report、earnings release、call transcript、investor deck、product page、customer/supplier cross-disclosure。
- 逐项抽取哪些字段：
  - revenue / segment revenue
  - order / backlog / RPO / bookings
  - gross margin / operating margin
  - CapEx / capacity expansion
  - inventory / receivables / working capital
  - FCF / OCF / interest expense / debt / lease liabilities
  - customer concentration
  - AI/data center/HBM/CoWoS/optics/power/cooling 相关原文字段
  - guidance / management commentary
  - technology roadmap
  - counterevidence

请按不同模块给差异化财报指标：

- HBM / memory vendor
- HBM equipment / test / probe / substrate / materials
- optics / CPO / silicon photonics
- AI server / ODM
- NeoCloud / AI data center developer
- power / cooling / grid equipment
- EDA / IP / custom ASIC
- storage / eSSD

### 2. K-line / price-volume research

K线数据应该怎么用？请设计一套非交易建议的市场行为研究框架：

- relative strength vs SPY / QQQ / SMH / SOXX / AIQ / GRID
- rolling beta to SPY / QQQ / SMH
- realized volatility: 20/60/120 day
- max drawdown / drawdown duration
- trend / moving averages / breakout vs mean reversion
- volume / dollar volume / liquidity
- earnings event gap
- post-earnings drift
- abnormal volume
- downside gap risk
- correlation with module basket

请说明哪些信号可用于：

- 观察市场是否开始验证基本面；
- 判断拥挤和过热；
- 控制组合风险；
- 发现财报前后事件机会；
- 但不能作为基本面证明。

### 3. Options data research

如果公司有期权数据，应该怎么研究？

请覆盖：

- implied volatility
- IV rank / IV percentile
- term structure
- skew / put-call skew
- put/call open interest
- volume / open interest
- earnings implied move
- realized vol vs implied vol
- gamma exposure / large OI strike clustering
- liquidity and bid-ask spread
- event risk before earnings / product event / investor day

请说明：

- 哪些期权指标是风险温度计；
- 哪些是事件定价；
- 哪些只能作为 smart money / crowding clue；
- 如何避免把期权流误读成基本面证据；
- 小账户研究时如何只把期权用于风险预算，而不是赌博。

### 4. Evidence + market + portfolio integration

请设计一个三层研究系统：

```text
Layer 1: Evidence / fundamental truth
Layer 2: Market behavior / pricing and crowding
Layer 3: Portfolio construction / risk budget
```

输出：

- scoring model
- 数据库 schema
- CSV / JSONL fields
- company card 模板
- quarterly update workflow
- downgrade / upgrade rules
- refutation dashboard

### 5. Local engineering pipeline

请设计本地脚本/agent pipeline。要求 Python 标准库 MVP 优先，不接 IBKR，不自动交易。

Pipeline stages:

1. `security_master`
2. `source_registry`
3. `financials_extractor`
4. `evidence_card_writer`
5. `price_feature_builder`
6. `options_feature_builder`
7. `risk_model_builder`
8. `portfolio_research_dashboard`
9. `refutation_dashboard`

每个 stage 给：

- input
- output
- schema
- agent prompt
- failure mode
- test plan

### 6. Practical MVP

我们现在有 146 条 AI Infra universe。请给一个 2 周 MVP 计划：

- 第 1-3 天做什么；
- 第 4-7 天做什么；
- 第 8-14 天做什么；
- 哪些公司先做样板；
- 财报、K线、期权分别先接哪些免费/公开数据源；
- 什么结果算 MVP 成功。

## 输出约束

- 不要输出买入/卖出建议。
- 不要输出目标价。
- 不要给实际仓位。
- 可以给研究字段、评分、风险预算框架，但必须声明是 research priority，不是投资建议。
- 所有涉及事实数据都必须回到原文或公开数据源核验。
