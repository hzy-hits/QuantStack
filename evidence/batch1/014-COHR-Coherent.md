# COHR Coherent evidence card

状态：first pass source-backed evidence card, scoped minimal

边界：这张卡只用于原文核验和研究分层，不是投资建议、买卖建议、目标价或仓位建议。

## 基本信息

| 字段 | 内容 |
| --- | --- |
| Rank | 14 |
| Priority tier | P0_first_batch |
| 公司 / 证券代码 | Coherent / COHR |
| 市场 / 资产池 | US / 美国资产池 |
| BFS depth | D2-D3 |
| 产业链模块 | 800G/1.6T optics + lasers |
| 当前分池 | 候选/核心候选 |
| Universe score | 100 / core_review |
| 核验状态 | first_pass_original_source_verified |

## 依赖链假设

| 字段 | 内容 |
| --- | --- |
| Dependency path | AI cluster scale-out → optical modules/lasers → datacom revenue |
| Dependency edge | 客户边+BOM边+技术边 |
| ETF clue | SMH? AI/semis/optics ETFs待查 |
| Smart money clue | 13F小中盘/AI optics discovery |

## 本轮优先核验

| 项目 | 内容 |
| --- | --- |
| Source priority | Find latest annual report, latest quarterly results, earnings call transcript, investor presentation, and official product/capacity pages first. |
| Primary sources to find | 10-K/20-F / quarterly results / earnings call / product qualification note / customer or hyperscaler reference |
| Metrics to verify | datacom AI mix; 800G/1.6T shipments; CPO/SiPh roadmap; customer concentration; ASP/gross margin |
| Upgrade conditions | Company filings show AI datacenter/datacom growth, qualified 800G/1.6T or CPO products, and margin durability. |
| Downgrade conditions | Growth is telecom cycle or one customer ramp, ASP declines dominate, or CPO timing is pushed out. |

## 原文来源登记

| 来源类型 | 链接 / 文件 | 发布日期 | 覆盖期间 | 备注 |
| --- | --- | --- | --- | --- |
| Quarterly report / 10-Q | https://www.sec.gov/Archives/edgar/data/820318/000082031826000013/iivi-20260331.htm | 2026-05-06 filed | Quarter ended 2026-03-31 | SEC EDGAR 原文；最新 10-Q。 |
| Earnings release | https://www.coherent.com/news/press-releases/third-quarter-fiscal-year-2026-results | 2026-05-06 | Q3 FY2026 | 公司官网原文；同日 8-K Exhibit 99.1 也收录。 |
| Investor presentation | https://www.coherent.com/content/dam/coherent/site/en/documents/investors/investor-presentations/2026/may-6/investor-presentation-20260506.pdf | 2026-05-06 | Q3 FY2026 | 公司官网 presentation；用于产品路线、capacity 和 segment mix 交叉核验。 |
| Financial releases index | https://www.coherent.com/company/investor-relations/financial-releases | 2026-05-12 accessed | IR source index | 用于确认 Q3 FY2026 release / webcast / presentation 均来自公司 IR。 |

## 原文证据

| 指标 | 原文位置 | 原文能证明什么 | 不能证明什么 | 口径备注 |
| --- | --- | --- | --- | --- |
| Revenue / segment revenue | Q3 FY2026 release; 10-Q Note 3 / Note 18 | Q3 FY2026 revenue was $1.8056B; Datacenter & Communications revenue was $1.3616B versus $968.7M in Q3 FY2025; Industrial was $444.0M versus $529.2M. | D&C 不是 AI-only 口径，不能直接拆成 hyperscaler / datacom / telecom。 | USD millions; quarter ended 2026-03-31. |
| Gross margin / profitability | Q3 FY2026 release | Q3 GAAP gross margin was 37.7%; non-GAAP gross margin was 39.6%. | 不能证明 800G/1.6T、CPO 或单客户产品毛利率。 | GAAP and non-GAAP; company reconciliation in release. |
| Customer / capacity evidence | 10-Q MD&A, “Agreements with NVIDIA” | 10-Q 披露公司与 NVIDIA 的 non-exclusive multiyear strategic agreement，包含 multibillion-dollar purchase commitment、future access/capacity rights；NVIDIA 同日投资 $2B，资金用于 R&D、future capacity and operations。 | 不能证明具体出货节奏、单价、订单取消条款或 NVIDIA 以外客户需求。 | 公司同时称正在扩 Sherman, Texas InP capacity 以应对客户需求和行业短缺。 |
| Datacenter growth attribution | 10-Q MD&A, Results of Operations | 10-Q 将 Q3 FY2026 revenue growth 中 D&C segment 增长 $393M / 41% 归因于 Datacenter business 的 AI datacenter demand，以及 Communications 中 data center interconnect、scale across 和 telecom demand。 | 仍不能拆 AI datacenter、DCI、scale-across、telecom 的单独收入。 | 这是 COHR 与 AI infra 连接最硬的公司原文之一。 |
| Product / roadmap evidence | Q3 FY2026 investor presentation | Presentation 把 next-gen AI datacenter 连接到 InP、CPO / silicon photonics、400G/lane、multi-rail optics，并列出 1.6T / 3.2T / 6.4T transceivers、DCI、OCS、optical components 等 growth engines。 | Presentation 是管理层路线图，不等于已确认订单或收入。 | 用来证明技术方向相关，不单独证明财务兑现。 |
| Capacity evidence | Q3 FY2026 investor presentation | Presentation 称公司计划到年末 double internal InP output，并到 2027 年再 more than double；同时提到 6-inch platform for EMLs, CW lasers and photodiodes。 | 不能证明扩产良率、客户锁定、资本回报或产能消化。 | 直接对应 InP / laser bottleneck 假设。 |
| Cash flow / working capital | 10-Q Liquidity and Capital Resources | 9M FY2026 operating cash flow was $10M versus $503M prior year；公司称下降主要由 inventories 增加以支持更高 revenue levels 带来的 working capital usage 驱动。 | 不能单独证明风险高；也可能是 AI 需求扩张期的前置备货。 | 这是后续反证重点：收入增长是否转为 FCF。 |
| CapEx / debt | 10-Q Liquidity and Capital Resources | 9M FY2026 additions to PP&E were $547M；March 31 2026 cash and equivalents were $1.593B，short-term investments $825M，total debt obligations $3.194B。 | 不能证明扩产回报；也不能证明债务风险已解除。 | 需要持续跟踪 capex、debt paydown、interest expense 和 capacity utilization。 |

## 结论分层

| 层级 | 内容 |
| --- | --- |
| 原文已证明 | Q3 FY2026 的收入增长主要落在 Datacenter & Communications；10-Q 明确将 Datacenter growth 与 AI datacenter demand 相连；presentation 给出 InP、CPO/SiPh、400G/lane、1.6T/3.2T/6.4T 等技术路线；公司披露 NVIDIA strategic agreement / $2B investment 线索。 |
| 合理推论 | COHR 可继续作为 AI optical / photonics 供应链核心候选，因为收入增长、毛利改善、D&C segment 增长、客户/产能线索和 InP/next-gen optics 路线在原文中互相支持。 |
| 待原文核验 | D&C 的 AI-only 占比、800G/1.6T/CPO 分产品收入、客户集中度、NVIDIA purchase commitment 的交付/取消条款、扩产良率与 capex 回报。 |
| 主要反证 | 单客户或少数客户拉动、产能扩张执行风险、inventory / working capital 吃掉 OCF、光模块 ASP/价格竞争、CPO 或硅光路线切换导致既有产品窗口缩短。 |

## 研究判断

| 维度 | 评分 1-5 | 依据 |
| --- | --- | --- |
| AI 需求相关度 | 5 | 10-Q 和 presentation 都把 Datacenter growth / next-gen optics 与 AI datacenter demand 连接。 |
| 供给瓶颈 | 4 | InP output、6-inch platform、capacity ramp 与 NVIDIA investment 支持瓶颈假设，但仍要看良率和客户锁定。 |
| 议价权 | 3 | 毛利率改善和 pricing optimization 是正面证据；光模块/组件链价格竞争仍是反证。 |
| 持续性 | 4 | 1.6T/3.2T/6.4T、CPO/SiPh、InP 路线支持多年代际升级；但产品窗口和路线切换需跟踪。 |
| 财务传导 | 3 | 收入和毛利改善明确，但 9M OCF 被库存和 working capital 压低，需继续验证 FCF。 |
| 技术护城河 | 4 | InP、EML/CW laser、photodiode、CPO/SiPh 和 end-to-end optical integration 是正面证据。 |
| 估值空间 |  | 未接行情、估值、同业比较，本轮不评分。 |
| 反证清晰度 | 5 | 客户集中、ASP、inventory/OCF、CPO timing、capacity execution 都可用原文和后续财报跟踪。 |

## 当前动作

- 当前动作：保持候选/核心候选。
- 原因：公司原文已经证明 D&C 收入高增、整体毛利改善、AI datacenter demand 表述、NVIDIA 采购承诺/投资、InP / next-gen optics 路线和 capacity expansion。
- 下一轮只核验：AI-only mix、客户集中、800G/1.6T/CPO 分产品口径、commitment 条款、inventory/OCF 是否改善。
