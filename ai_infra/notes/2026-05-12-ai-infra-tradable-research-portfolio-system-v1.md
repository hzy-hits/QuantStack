# AI Infra 可交易研究与组合系统 v1

日期：2026-05-12  
状态：研究系统设计，不是投资建议或买卖建议  
用途：把 AI Infra / LLM Dependency BFS 研究转成可跟踪、可回测、可组合、可风控的主题基金框架。

## 0. 核心判断

下一步不能只继续读新闻。要把研究拆成三层：

1. **事实层**：财报、订单、backlog、CapEx、毛利率、FCF、产能、客户、技术路线。
2. **资金层**：ETF 持仓、N-PORT、13F、13D/13G、期权 IV/OI、成交量、价格动量、相对强弱。
3. **组合层**：beta、factor exposure、相关性、波动率、最大回撤、仓位上限、delta/gamma/vega/theta、再平衡规则。

目标不是预测每一只股票，而是构建一个 `AI Infra BFS 主题基金`：

```text
D0 LLM demand
  -> D1 GPU / cloud / ASIC
  -> D2 HBM / CoWoS / optics / power / cooling
  -> D3 equipment / test / substrate / optical components / rack power
  -> D4-D5 radar only
```

投资上优先关注 `D1-D3`，但组合上要控制单一叙事、单一行业、单一国家、单一流动性和单一估值因子风险。

## 1. 真实东西怎么挖

### 1.1 财报与原文

每家公司先建 evidence card：

| 维度 | 原文出处 | 要抽取什么 |
| --- | --- | --- |
| 收入 | 10-K / 20-F / 年报 / 季报 | AI/data center/HPC/semiconductor segment revenue |
| 订单 | earnings release / call / backlog | backlog、RPO、book-to-bill、客户长约 |
| 产能 | investor presentation / capex plan | CoWoS、HBM、光模块、液冷、变压器、PCB/CCL 产能 |
| 毛利 | financial statements | 毛利率是否随 AI mix 提升 |
| FCF | cash flow statement | 收入增长是否转成现金 |
| 库存 | balance sheet | 是否提前备货、是否有周期高点风险 |
| 客户 | annual report / risk factors | 客户集中、客户是谁、是否可替代 |
| 技术 | product page / whitepaper | 是否真对应 HBM、800G/1.6T、CPO、液冷、AI server PCB |

只要没有原文，不写“已证明”，只能写“待原文核验”。

### 1.2 新闻怎么用

新闻只做触发器，不做结论。

| 新闻类型 | 用途 | 处理方式 |
| --- | --- | --- |
| 大客户订单 | 触发原文核验 | 回到公告 / 8-K / call transcript |
| 扩产新闻 | 判断供给瓶颈 | 查 CapEx、设备订单、交期 |
| 新产品发布 | 判断技术路线 | 查规格、客户认证、量产时间 |
| 价格上涨 | 判断供需 | 查 ASP、合同价、毛利率 |
| 分析师报告 | 生成问题 | 不当事实 |

新闻质量排序：

1. 公司公告 / 交易所公告。
2. 公司 earnings call。
3. 客户或供应商交叉披露。
4. 行业协会 / 标准组织。
5. 媒体和券商摘要。

## 2. 聪明钱怎么跟

“聪明钱”不能只看某个大佬买了什么。要拆成几类资金。

### 2.1 ETF 资金

ETF 是最容易系统化的资金流。要看：

- 该公司是否被哪些 ETF 持有。
- 持有权重。
- ETF 自身 AUM。
- AUM-weighted dollars：`ETF AUM * stock weight`。
- 权重变化。
- 是否进入新 ETF / 被剔除。
- 是否被多个主题 ETF 同时持有。

对 `<100B` 中小公司，ETF 覆盖常见特征：

- 大盘半导体 ETF 可能只给很小权重。
- 等权半导体 ETF 比市值加权 ETF 更容易给中小公司较高权重。
- 主题 ETF 可能覆盖更小、更偏门公司，但流动性和费用更差。
- 电力/电网/机器人/AI 主题 ETF 会覆盖到 D3-D5，但 AI 纯度会下降。

ETF 覆盖强度指标：

```text
ETF_coverage_score =
  ETF_count
  + AUM_weighted_exposure
  + recent_weight_change
  + thematic_purity
  + liquidity_score
```

### 2.2 13F / 13D / 13G

13F 用于看机构季度持仓，但有缺陷：

- 季度披露，滞后。
- 主要是 long equity，不能完整看到 short、hedge、option details。
- 不能把 13F 买入直接当未来上涨信号。

可用指标：

| 指标 | 含义 |
| --- | --- |
| new holders | 新机构进入 |
| top holder concentration | 是否被少数机构拥挤持有 |
| QoQ shares change | 季度增减仓 |
| high-quality holders | 是否有长期产业型基金 / 主动管理基金 |
| overlap with AI ETF | 是否同时被 ETF 和主动机构买入 |

13D/13G 更适合跟大股东/主动投资者，尤其是小中盘。

### 2.3 期权市场

期权不是只用来赌博方向。它是资金预期和风险价格的窗口。

需要跟：

| 指标 | 用途 |
| --- | --- |
| IV rank / IV percentile | 当前隐含波动率是否昂贵 |
| skew | 下行保护需求或上行追涨需求 |
| call/put OI | 资金偏向 |
| volume/OI spike | 异常交易 |
| term structure | 事件风险集中在哪个到期 |
| gamma exposure | 股价是否容易被做市商对冲放大 |
| option liquidity | 能不能真的交易，价差是否可接受 |

组合层要记录：

- delta：权益方向暴露。
- gamma：非线性风险。
- vega：波动率风险。
- theta：时间损耗。
- beta-adjusted delta：相对 QQQ/SPY/SMH 的真实风险。

## 3. ETF 覆盖池

以下不是推荐买入，只是 IBKR 里可以研究的覆盖工具。

### 3.1 AI / Robotics / AI software broader ETFs

| ETF | 用途 | 局限 |
| --- | --- | --- |
| AIQ | 广义 AI + technology，覆盖 AI 软件、平台、半导体、部分国际 AI 公司 | 可能偏大盘，AI Infra 纯度不一定高 |
| BOTZ | robotics / applied AI / automation | 工业、医疗机器人较多，不是纯 AI Infra |
| IRBO / ARTY / BAI | iShares AI / automation / future AI 系列 | 需下载 holdings 看是否覆盖 D2-D3 |
| CHAT | generative AI / AI theme | 可能偏应用和大盘平台 |

### 3.2 Semiconductor / AI hardware ETFs

| ETF | 用途 | 局限 |
| --- | --- | --- |
| SMH | 半导体龙头，NVDA/TSM/AVGO 权重高，适合作为 D1-D2 大盘 beta | 集中度高，较少覆盖小中盘 |
| SOXX | 半导体综合暴露 | 仍偏大中盘 |
| XSD | 等权半导体，更适合覆盖中小半导体 | AI 纯度要逐家公司查 |
| PSI | 动态半导体，可能有更多中盘暴露 | 因子和换仓规则要核验 |

### 3.3 Power / grid / infrastructure ETFs

| ETF | 用途 | 局限 |
| --- | --- | --- |
| GRID | smart grid / power infrastructure，对 D5 电网、电力设备有覆盖 | AI 数据中心直接性弱，需要防泛电网叙事 |
| PAVE | US infrastructure | 不是 AI Infra 专用 |
| XLU / VPU | 公用事业 beta | AI 相关性弱，更多是防守/利率资产 |

### 3.4 Nuclear / uranium / energy ETFs

| ETF | 用途 | 局限 |
| --- | --- | --- |
| URA / URNM | uranium / nuclear fuel 链 | D5 雷达，不应直接当 AI 核心 |
| NLR | nuclear energy broader | AI 数据中心传导路径长 |

### 3.5 怎么判断 ETF 是否真的覆盖小公司

不要只看 ETF 名字。要下载 holdings，做这个表：

| 公司 | 市值 | BFS depth | ETF | ETF weight | ETF AUM | AUM-weighted exposure | 是否新进 | 权重变化 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

公式：

```text
ETF dollars into stock = ETF AUM * stock weight
Coverage quality =
  number of ETFs holding
  + aggregate ETF dollars
  + weight percentile inside ETF
  + whether ETF is thematic pure-play
  - liquidity / expense / concentration penalty
```

## 4. K 线和价格数据怎么用

K 线不是用来替代基本面，而是判断市场是否已经开始定价。

### 4.1 价格信号

| 信号 | 含义 |
| --- | --- |
| relative strength vs QQQ / SMH / SOXX | 是否跑赢 AI beta |
| 20/50/200 日均线 | 趋势状态 |
| volume breakout | 是否有机构参与 |
| earnings gap | 财报信息是否被重估 |
| drawdown from high | 风险和拥挤度 |
| pair ratio | 相对产业链同类公司的强弱 |

### 4.2 不该怎么用

- 不因为涨了就证明基本面。
- 不因为跌了就认为便宜。
- 不用单日异动当长期结论。
- 不把小盘低流动性拉升当 smart money。

## 5. 组合怎么构造

目标不是“风险最小 alpha 最大”，这个不能保证。更现实的目标是：

> 在可承受最大回撤和波动率预算内，尽量提高对 D1-D3 真瓶颈的暴露，同时降低单一股票、单一 ETF、单一因子和单一叙事风险。

### 5.1 Core / Satellite

| 层 | 用途 | 工具 |
| --- | --- | --- |
| Core beta | 承接 AI Infra 大盘 beta | SMH / SOXX / AIQ / QQQ |
| Thematic beta | 覆盖半导体、光互连、电力、grid、robotics | XSD / GRID / BOTZ / selected ETFs |
| Alpha basket | 自建 D2-D3 高弹性股票篮子 | 10-30 只公司，等权或风险平价 |
| Options overlay | 控制下行、事件和波动率 | protective put、collar、call spread、cash-secured put |
| Cash / T-bills | 控制回撤和等待事件 | SGOV / BIL / cash |

### 5.2 自建主题基金的基本约束

建议先用约束而不是主观信心：

| 风险 | 约束例子 |
| --- | --- |
| 单股风险 | 单股初始 1-3%，高确信不超过 5% |
| 单一 ETF | 单 ETF 不超过 20-30% |
| 单一模块 | D1 / D2 / D3 任一模块不超过 35-40% |
| 小盘流动性 | 低流动性个股合计不超过 15-20% |
| 国家/币种 | 非美、ADR、日股、欧股单独设限 |
| 期权风险 | 总组合 beta-adjusted delta、vega、theta 设上限 |
| 最大回撤 | 先定义 -10%、-15%、-20% 哪个是不可承受线 |

### 5.3 权重方法

三种可选：

1. 等权：简单，但忽略波动差异。
2. 波动率倒数权重：低波动给更高权重。
3. 风险平价 + conviction tilt：基础按风险平价，再按 BFS/证据/反证评分微调。

基础公式：

```text
raw_weight_i = conviction_score_i / realized_volatility_i
final_weight_i = cap(raw_weight_i, single_name_limit)
```

其中 conviction_score 不来自感觉，而来自：

```text
conviction =
  BFS_score
  + evidence_score
  + bottleneck_score
  + financial_transmission_score
  - valuation_risk
  - crowding_risk
  - liquidity_risk
  - substitution_risk
```

## 6. Beta 和 Greeks 怎么抽

### 6.1 Equity beta

每只股票至少算：

- beta vs SPY。
- beta vs QQQ。
- beta vs SMH / SOXX。
- beta vs AI Infra custom basket。
- residual alpha：跑赢对应 beta 后的部分。

回归：

```text
stock_return = alpha + beta_spy * SPY + beta_qqq * QQQ + beta_smh * SMH + error
```

如果一只股票涨只是因为 `beta_smh` 很高，不一定有 alpha。

### 6.2 Theme beta

我们可以自己建一个 AI Infra benchmark：

```text
AI_INFRA_BETA =
  30% semis
  20% AI cloud / data center
  20% HBM / memory / packaging
  15% optical / networking
  15% power / cooling / grid
```

然后每只公司算：

```text
residual_return = stock_return - beta_to_AI_INFRA_BETA * AI_INFRA_BETA
```

这才是更接近 alpha 的东西。

### 6.3 Options Greeks

IBKR 可以看 option chain 和 Greeks；程序化可以通过 TWS / IB Gateway API 或第三方数据源。

组合层要聚合：

```text
portfolio_delta = sum(position_delta)
portfolio_gamma = sum(position_gamma)
portfolio_vega  = sum(position_vega)
portfolio_theta = sum(position_theta)
```

更重要的是：

```text
beta_adjusted_delta =
  option_delta * underlying_beta_to_QQQ_or_SMH
```

这样才能知道组合到底暴露在 AI beta 还是个股事件上。

## 7. 数据系统

### 7.1 表结构

建议本地建这些 CSV / SQLite 表：

```text
companies
  ticker, name, country, exchange, market_cap, bfs_depth, module, liquidity

evidence_cards
  ticker, source_type, source_url, report_period, metric, value, evidence_status, notes

etf_holdings
  date, etf, ticker, weight, shares, market_value, etf_aum

institutional_holdings
  quarter, manager, ticker, shares, market_value, change_qoq, source

prices
  date, ticker, open, high, low, close, volume, adj_close

options_snapshot
  date, ticker, expiry, strike, right, bid, ask, mid, iv, delta, gamma, vega, theta, volume, open_interest

portfolio_positions
  date, ticker, quantity, market_value, weight, beta_spy, beta_qqq, beta_smh, delta, gamma, vega, theta
```

### 7.2 每日 / 每周 / 每季节奏

每日：

- 价格、成交量、相对强弱。
- 期权 IV/OI/volume 异常。
- ETF 大额流入流出。

每周：

- ETF holdings 更新。
- 新闻触发器。
- K 线趋势和 drawdown。

每季：

- 财报原文。
- 13F。
- N-PORT。
- 证据卡片升级/降级。
- 组合再平衡。

## 8. 下一步实际动作

最有价值的顺序：

1. 生成 `A股映射 v2`，按 BFS + 边关系 + 评分重整。
2. 建一个美股/海外候选 universe，先 80-150 个 ticker。
3. 抓 ETF holdings：AIQ、BOTZ、CHAT、SMH、SOXX、XSD、GRID、URA/URNM 等。
4. 计算每只公司的 ETF 覆盖强度。
5. 抓价格数据，计算 beta / volatility / drawdown / relative strength。
6. 对有流动期权的 ticker 抓 option chain，计算 IV rank、skew、Greeks。
7. 建一个 paper portfolio，不实盘，先跑 4-8 周。
8. 再决定是否形成真钱组合。

## 9. 当前最适合先做的 MVP

先做一个本地 dashboard：

```text
输入：
  - BFS 候选公司列表
  - ETF holdings
  - 价格数据
  - 期权快照

输出：
  - 公司 BFS depth
  - ETF 覆盖强度
  - smart money proxy
  - beta vs SPY/QQQ/SMH
  - realized vol / drawdown
  - IV rank / skew
  - evidence score
  - portfolio suggested weight range
```

MVP 不需要自动下单，也不需要预测涨跌。它只负责回答：

1. 哪些公司是真正被 ETF / smart money 覆盖的 AI Infra 资产？
2. 哪些公司只是故事？
3. 当前价格风险是不是已经很高？
4. 如果要构建主题基金，哪些仓位会让组合过度暴露在 NVDA / SMH / QQQ beta 上？
5. 哪些股票有潜在 alpha，但流动性/期权/财报证据还不够？
