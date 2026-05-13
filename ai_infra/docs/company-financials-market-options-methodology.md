# Company Financials, K-line, and Options Research Methodology

状态：稳定方法论 v1  
来源：整合 ChatGPT Pro 输出与本项目既有 BFS / evidence / refutation 规则  
边界：研究优先级框架，不是投资建议、买卖建议、目标价或实际仓位建议。

## 核心原则

这套方法把公司研究拆成三个不可互相替代的层级：

| 层级 | 解决的问题 | 不能做什么 |
| --- | --- | --- |
| Evidence / fundamentals | 公司是否真的在 AI Infra 链条里赚钱，是否有收入、订单、客户、产能、毛利率、现金流传导 | 不能用 K线、期权、媒体、模型输出替代原文证据 |
| Market behavior / K-line | 市场是否开始定价、是否拥挤、风险和流动性如何 | 不能证明公司有 AI 收入、客户或 backlog |
| Options / event risk | 波动、事件、流动性、尾部风险和拥挤线索如何 | 不能证明基本面，也不能直接当作 smart money 真相 |

系统硬规则：

1. Evidence first.
2. Market second.
3. Options third.
4. No primary source, no core conclusion.
5. No AI revenue/order/customer/product evidence, no AI Infra proof.
6. K-line 只能说明 pricing、crowding、risk。
7. Options 只能说明 volatility、event pricing、liquidity、crowding clue。
8. 所有结论必须分为：原文已证明、合理推论、待原文核验、主要反证。
9. 所有评分都是 research priority，不是投资建议。
10. 每季度必须更新 evidence、market、options、refutation 四张表。

## Company Research Workflow

每家公司先写清楚一条可证伪假设：

```text
如果 [训练/推理/AI data center/ASIC/HBM/CoWoS/800G/电力] 需求持续增长，
那么 [公司] 会因为 [产品/客户认证/产能瓶颈/技术路线] 获得
[收入、订单、毛利率、现金流、ROIC] 的改善。
```

不合格：`公司有 AI exposure，股价强，所以可能受益。`

合格：`如果 HBM4 stack 高度和测试复杂度继续提升，那么 HBM memory tester / probe card 供应商可能因为测试时间、probe complexity 和客户认证门槛上升，获得订单、毛利率和 backlog 可见度改善。`

### Step 1: 定位 BFS 和 dependency edge

```json
{
  "company": "Example Co",
  "ticker": "EXM",
  "module": "HBM equipment",
  "bfs_depth": "D3",
  "dependency_edge": "LLM demand -> GPU/ASIC -> HBM capacity -> TCB/hybrid bonding tools -> company revenue",
  "edge_type": "direct_bottleneck | second_order_supplier | capacity_expansion | theme_mapping",
  "dependency_strength": 1,
  "primary_refutation": "HBM capacity expands but tool intensity falls"
}
```

`D1-D3` 是主战场；`D4-D5` 默认只做雷达，除非能证明会反向卡住 `D0-D2`。

### Step 2: 建 source checklist

| 来源类型 | 用途 | 状态标签 |
| --- | --- | --- |
| 10-K / 20-F / annual report | segment、客户集中、风险、CapEx、现金流、债务、长期披露 | primary |
| 10-Q / quarterly report | 最新季度收入、订单、库存、应收、债务、现金流 | primary |
| earnings release | 最新财务摘要、guidance、non-GAAP、管理层摘要 | primary |
| earnings call transcript | AI、客户、backlog、产能、margin 的解释 | primary-ish |
| investor deck / capital markets day | roadmap、TAM、产品架构、客户类型、长期模型 | primary-ish |
| product page / technical whitepaper | 产品是否真实服务 HBM、CPO、AI server、data center power | primary technical |
| customer / supplier cross-disclosure | 交叉验证客户、供应关系和技术路线 | high-value cross-check |
| industry standards | PCIe、CXL、UCIe、Ethernet、OCP、JEDEC 等路线验证 | supporting |
| media / broker / database / ChatGPT Pro | 只能作为线索 | secondary only |

接入数据源前必须重新核验官网和许可。美国公司 MVP 优先 SEC EDGAR；价格数据可以先用免费历史价格源做原型；期权免费源不稳定，先以 US liquid names 的风险温度计为目标，不承诺全量覆盖。

### Step 3: 抽取 financial transmission

| 模块 | 字段 | 研究问题 |
| --- | --- | --- |
| Revenue | total revenue, segment revenue, AI/data center revenue, product revenue | 收入是否已经进入财报，而不是只有 AI opportunity？ |
| Orders | backlog, RPO, bookings, book-to-bill, prepayment, long-term agreement | 需求是否有可见度？订单是否可取消？是否能转收入？ |
| Margin | gross margin, operating margin, product mix, yield, ASP | 需求上升是否带来议价权，而不是 pass-through？ |
| CapEx / capacity | CapEx, capacity expansion, tool delivery, lead time, utilization | 扩产是否支撑未来收入？是否带来折旧压力？ |
| Working capital | inventory, receivables, payables, cash conversion cycle | 是否提前备货？是否存在库存/应收风险？ |
| Cash / debt | OCF, FCF, debt, lease liabilities, interest expense, interest coverage | 增长是否转化为现金流？重资产公司是否靠杠杆堆收入？ |
| Customer | top customer %, named customers, geography, end-market | AI demand 是否来自 hyperscaler、GPU 厂、HBM 厂、OSAT、数据中心？ |
| AI keywords | AI, data center, HBM, CoWoS, CPO, SiPh, liquid cooling, power | 公司是否明确把产品映射到 AI Infra？ |
| Guidance | revenue guide, margin guide, CapEx guide, backlog conversion | 下一季度/年度预期如何？ |
| Roadmap | HBM3E/HBM4, 800G/1.6T, CPO, PCIe/CXL, liquid cooling | 技术路线是否延续，是否切换？ |
| Counterevidence | weak AI disclosure, margin compression, inventory rise, customer loss, ASP decline | 什么会推翻 thesis？ |

最低要求：不能只看收入。必须看毛利率和 FCF。重资产公司还要看折旧、库存、CapEx 回收周期；NeoCloud / data center developer 必须看利用率、折旧、融资成本和客户期限。

## Module-Specific Financial Fields

| 模块 | 核心问题 | 重点指标 |
| --- | --- | --- |
| HBM / memory vendor | AI 真实拉动、传统 memory 周期反转，还是二者混合？ | HBM revenue/mix、DRAM margin、ASP/bit shipment、HBM3E/HBM4 roadmap、wafer allocation、CapEx、inventory、customer concentration |
| HBM equipment / test / probe / substrate / materials | HBM 扩张是否传导到设备、测试、基板、材料订单和毛利？ | TCB/hybrid bonding orders、memory tester revenue、probe ASP、ABF layer count、advanced packaging inspection revenue、book-to-bill |
| Optics / CPO / SiPh | AI cluster 是否把光互连从通信周期变成数据中心算力周期？ | datacom revenue、800G/1.6T mix、customer concentration、laser capacity、CPO design win、gross margin、inventory |
| AI server / ODM | AI server revenue 是 GPU pass-through，还是系统集成价值提升？ | AI server revenue mix、gross margin、inventory、customer concentration、rack shipment、working capital、backlog |
| NeoCloud / AI data center developer | 高增长云平台，还是 GPU 租赁 + 高杠杆基础设施周期？ | contracted backlog/RPO、utilization、gross margin、CapEx、debt/lease、depreciation、secured power、customer concentration |
| Power / cooling / grid equipment | AI 数据中心瓶颈是否转向电力、变压器、UPS、PDU、switchgear、liquid cooling？ | backlog/orders、book-to-bill、lead time、data center revenue、gross margin、liquid cooling attach、rack kW |
| EDA / IP / custom ASIC | 自研 ASIC 是否把价值转向 EDA、IP、NRE、custom silicon？ | EDA recurring revenue、IP royalty、NRE revenue、tape-out count、RPO/deferred revenue、customer concentration |
| Storage / eSSD | AI 对 NAND/eSSD 是真实数据中心需求，还是传统 NAND 周期反弹？ | enterprise SSD revenue、eSSD TB shipment、QLC mix、controller revenue mix、PCIe Gen5/6、NAND ASP、inventory |

## K-line / Price-Volume Layer

K-line 只回答市场行为，不回答基本面真相。

Required OHLCV fields:

```text
date, open, high, low, close, adjusted_close, volume,
ticker, exchange, currency, split_adjusted, dividend_adjusted, source, source_date
```

| 特征 | 方法 | 用途 |
| --- | --- | --- |
| relative strength | stock return - benchmark return | 判断是否跑赢 SPY/QQQ/SMH/SOXX/AIQ/GRID 或内部 module basket |
| rolling beta | cov(stock, benchmark) / var(benchmark) | 判断系统性风险暴露 |
| realized volatility | std(log returns) * sqrt(252), window 20/60/120 | 风险预算、波动 regime |
| max drawdown | price / rolling max(price) - 1 | 下行风险 |
| drawdown duration | peak 到 recover 的交易日数 | 资金占用与心理压力 |
| trend | 20/50/100/200 日均线和 slope | 趋势强弱 |
| breakout | close > N-day high | 市场是否重新定价 |
| dollar volume | close * volume | 流动性和可研究性 |
| abnormal volume | volume / rolling median volume | 事件关注度 |
| earnings gap | post-earnings open/close vs pre-earnings close | 财报事件定价 |
| post-earnings drift | 财报后 1/5/20/60 日相对收益 | 市场是否持续验证 |
| module correlation | 与 HBM / optics / power / NeoCloud basket 的 rolling corr | 组合拥挤和主题 beta |

Market layer 可以标记：

- `market_validation`: 原文事件后相对强度、成交量和 drift 同步增强。
- `crowding_risk`: RS、RV、volume、correlation、price distance、IV 同时极端。
- `liquidity_risk`: dollar volume 太低、缺口大、成交不连续。
- `event_risk`: 财报或产品发布前异常波动。

它不能证明公司有 AI revenue、hyperscaler 客户、真实 backlog、毛利率改善，或处在 HBM / CPO / CoWoS / liquid cooling 真实链条中。

## Options Layer

期权数据是风险温度计、事件定价和拥挤线索。它不是基本面证据，也不是 smart money 真相。

Required option-chain fields:

```text
as_of_date, ticker, expiry, days_to_expiry, strike, call_put,
bid, ask, mid, last, volume, open_interest, implied_volatility,
delta, gamma, vega, theta, underlying_price, source, source_timestamp
```

| 特征 | 用途 |
| --- | --- |
| implied volatility | 市场预期波动 |
| IV rank / percentile | 当前 IV 是否处于历史高位 |
| term structure | 财报/产品发布事件风险 |
| skew | 下行保护需求 |
| put/call OI and volume | 持仓和交易热度线索 |
| earnings implied move | 财报隐含波动幅度 |
| RV vs IV | 市场预期是否高于历史波动 |
| gamma exposure proxy | 可能的 dealer hedging pressure |
| OI clustering | pin risk / crowded strikes |
| bid-ask spread | 期权流动性 |
| chain depth | 是否适合纳入期权风险研究 |

Rules:

1. `option_flow_status` 永远不能提升 `evidence_status`。
2. 没有原文证据时，期权异动只能标为 `MARKET_CLUE`。
3. put/call OI 不等于方向判断，必须结合价格、IV、delta、成交方向和历史背景。
4. 高 IV 不等于公司基本面好，只说明市场预期波动高。
5. 低 IV 不等于风险低，可能是市场低估事件，也可能是数据缺失。
6. 免费期权数据覆盖弱时，必须明确 `NO_OPTIONS_DATA` 或 `LOW_QUALITY_OPTIONS_DATA`，不能用缺失当低风险。

## Research Priority Scoring

总分 100，含义是研究优先级，不是交易信号。

### Hard gates

| Gate | 规则 |
| --- | --- |
| G0 | 没有 primary source，最高只能是 `pending_original_source_verification` |
| G1 | 只有 AI 叙事，没有收入/订单/客户/产品证据，不可进入核心研究池 |
| G2 | BFS depth = D5 且无客户/收入证据，只能做 theme watch |
| G3 | 反证直接推翻 thesis，降级到 refutation watch |
| G4 | 市场信号不能覆盖 evidence gate |

### Score layers

| 层 | 权重 | 维度 |
| --- | ---: | --- |
| Evidence score | 60 | BFS proximity、AI demand 原文证据、revenue / order / margin / CapEx / FCF 传导、roadmap、counterevidence |
| Market behavior score | 20 | evidence event 后相对强度、post-earnings drift、liquidity、abnormal volume、crowding penalty、drawdown / vol regime |
| Risk budget score | 20 | rolling beta、module correlation、options/event risk、downside gap、data quality、liquidity risk |

| Total score | Bucket |
| ---: | --- |
| 85-100 | Core research priority |
| 70-84 | High-priority watch |
| 55-69 | Thematic / validation needed |
| 35-54 | Source-needed / refutation-heavy |
| <35 | Low priority / theme mapping |

Bucket 只表示研究优先级。不能把它翻译为买入、卖出、目标价或实际仓位。

## Data Model Additions

当前 `companies / dependency_edges / research_signals / scores` 是 universe MVP。下一阶段建议扩展为：

| 表 | 用途 |
| --- | --- |
| `security_master` | ticker、交易所、国家、币种、CIK/ISIN、ADR/local mapping、module、BFS、dependency edge |
| `source_registry` | source_id、source_type、period、url、local_path、hash、priority、status |
| `evidence_claims` | claim、quote、source_location、metric、value、status、confidence、counterevidence |
| `financial_metrics` | revenue、margin、CapEx、OCF/FCF、debt、lease、segment metrics |
| `price_daily` | OHLCV 日频数据 |
| `price_features` | RS、beta、RV、drawdown、volume、gap、drift、correlation |
| `option_chain_snapshots` | 原始期权链快照 |
| `options_features` | IV rank、term structure、skew、put/call、implied move、liquidity |
| `research_scores` | evidence_score、market_score、risk_score、bucket、gate_status |
| `refutation_signals` | thesis、refutation_question、metric、threshold、latest_value、severity |

第一版不需要一次性全部做完。最小可行顺序：

```text
security_master -> source_registry -> financial_metrics/evidence_claims -> price_features -> company_card -> research_scores
```

Options 可以只覆盖 US liquid sample names；无数据时明确标注，不阻塞 evidence MVP。

## Quarterly Update Workflow

| 时间 | 动作 |
| --- | --- |
| T-10 to T-3 | 更新 price_features、options_features、历史 earnings gap；列本季度核验问题 |
| T | 抓 earnings release / 8-K / quarterly report；只抽取原文数字 |
| T+1 to T+3 | 加入 call transcript；抽取 guidance、customer、capacity、backlog、margin |
| T+3 to T+7 | 完成 evidence_claims、financial_metrics、company card、scoring、refutation dashboard |
| T+7 to T+14 | 交叉披露核验、同模块 basket 对比、季度 module note |

## MVP Implementation Order

两周 MVP 的目的不是建交易系统，而是把 146 条 universe 变成可持续迭代的研究系统。

### Day 1-3: 地基

- 清洗 146 universe，生成 `security_master`。
- 标准化 company_id、ticker、exchange、country、currency。
- 标记 module、BFS depth、dependency edge。
- 选 10-12 家样板公司。
- 接 source registry MVP。
- 接价格数据 MVP，先生成 RS、beta、RV、drawdown、volume。

成功标准：146 家全部有 company_id；样板公司至少 8 家有 source registry；多数公司有 price feature 或明确 `NO_PRICE_DATA`。

### Day 4-7: 样板公司卡

- 抽取样板公司的 annual / quarterly / earnings release。
- 生成 `financial_metrics` 和 `evidence_claims`。
- 补 AI-specific fields：HBM、CoWoS、optics、power、cooling、guidance、roadmap。
- 计算 earnings gap、post-earnings drift、module correlation。
- 对 US liquid sample names 接 options risk subset。
- 生成 company cards、research score、refutation dashboard v0。

成功标准：每张 card 至少 5 条 evidence claims；每张 card 至少 3 条 counterevidence；所有财务数字都有 source_id；K-line / options 只进入 market / risk section。

### Day 8-14: 扩到 universe dashboard

- 为 146 家生成 source coverage。
- 全量跑 K-line features。
- 生成流动性、drawdown、crowding risk labels。
- 跑 scoring gates。
- 生成 high-priority verification list。
- 完成 refutation dashboard。
- 输出 MVP README 和 next 30-day roadmap。

成功标准：146 家全部进入 security master；至少 10 家样板公司有完整 company card；所有 `原文已证明` 结论都有 source_id；所有 `合理推论` 都有 input evidence 和 assumption；所有高分公司都有反证；dashboard 不输出买卖建议、目标价或实际仓位。
