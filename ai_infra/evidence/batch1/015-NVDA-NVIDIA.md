# NVDA NVIDIA evidence card

状态：draft evidence card, pending original-source verification

边界：这张卡只用于原文核验和研究分层，不是投资建议、买卖建议、目标价或仓位建议。

## 基本信息

| 字段 | 内容 |
| --- | --- |
| Rank | 15 |
| Priority tier | P0_first_batch |
| 公司 / 证券代码 | NVIDIA / NVDA |
| 市场 / 资产池 | US / 美国资产池 |
| BFS depth | D1 |
| 产业链模块 | GPU/CUDA + networking + rack-scale systems |
| 当前分池 | 核心池 |
| Universe score | 100 / core_review |
| 核验状态 | pending_original_source_verification |

## 依赖链假设

| 字段 | 内容 |
| --- | --- |
| Dependency path | OpenAI/Anthropic/Gemini token demand → GPU cluster → HBM/CoWoS/networking/rack |
| Dependency edge | 客户边+BOM边+技术边+现金流边 |
| ETF clue | SMH/SOXX/XLK/QQQ/VGT/AIQ |
| Smart money clue | 13F核心大盘；期权流动性强；被动权重大 |

## 本轮优先核验

| 项目 | 内容 |
| --- | --- |
| Source priority | Find latest annual report, latest quarterly results, earnings call transcript, investor presentation, and official product/capacity pages first. |
| Primary sources to find | 10-K/20-F / quarterly results / earnings call / investor presentation / product roadmap |
| Metrics to verify | data center compute revenue; accelerator roadmap; networking/rack-scale systems; supply constraints; gross margin; customer concentration |
| Upgrade conditions | Original sources prove sustained AI compute demand, platform pull-through, and supply-chain transmission into D2/D3. |
| Downgrade conditions | Growth slows, supply constraints ease into oversupply, margins compress, or workload shifts weaken the platform moat. |

## 原文来源登记

| 来源类型 | 链接 / 文件 | 发布日期 | 覆盖期间 | 备注 |
| --- | --- | --- | --- | --- |
| Annual report / 10-K / 20-F | [NVIDIA SEC filings page](https://investor.nvidia.com/financial-info/sec-filings/default.aspx) | company IR page accessed 2026-05-12 | FY filings index | 官方 SEC filings 入口；下一轮需要直接打开 FY2026 10-K 原文 |
| Quarterly / annual earnings release | [NVIDIA FY2026 Q4 and fiscal 2026 earnings release](https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-fourth-quarter-and-fiscal-2026) | 2026-02-25 | Q4 FY2026 / FY2026 | 官方新闻稿，含 segment highlights、收入、毛利、资产负债表和现金流摘要 |
| Investor relations financial reports | [NVIDIA financial reports page](https://investor.nvidia.com/financial-info/financial-reports/default.aspx) | company IR page accessed 2026-05-12 | Latest report index | 官方财报入口，指向最新 earnings release |
| Earnings call transcript |  |  |  | 下一轮补官方 webcast / transcript 或公司 CFO commentary |
| Investor presentation |  |  |  | 下一轮补最新 investor presentation |
| Company product / technical page |  |  |  | 下一轮补 Blackwell / Rubin / NVLink / GB300 等官方产品资料 |
| Exchange filing / regulatory filing |  |  |  | 下一轮从 SEC filings 页补 10-K 原文 |
| Upstream/downstream cross-disclosure |  |  |  |  |

## 原文证据

| 指标 | 原文位置 | 原文能证明什么 | 不能证明什么 | 口径备注 |
| --- | --- | --- | --- | --- |
| Revenue / segment revenue | FY2026 earnings release, headline and Data Center highlights | FY2026 total revenue was $215.9B, +65% YoY; Q4 FY2026 revenue was $68.1B; Q4 Data Center revenue was $62.3B; FY2026 Data Center revenue was $193.7B, +68% YoY | 不能证明每个具体客户的采购额，也不能证明所有下游供应商按同比受益 | GAAP revenue, USD, NVIDIA fiscal year ending Jan. 25, 2026 |
| Gross margin / operating margin | FY2026 earnings release, Q4 and FY2026 summary tables | Q4 FY2026 GAAP / non-GAAP gross margin was 75.0% / 75.2%; FY2026 GAAP / non-GAAP gross margin was 71.1% / 71.3% | 不能拆分 Data Center 单独毛利，也不能证明毛利率未来持续 | 公司口径；FY2026 全年毛利率低于 FY2025 |
| CapEx / inventory / FCF | FY2026 earnings release balance sheet and cash-flow summary | Inventories were $21.4B at Jan. 25, 2026 versus $10.1B one year earlier; property and equipment net was $10.4B versus $6.3B | 不能直接证明 GPU 供给瓶颈，也不能证明 HBM/CoWoS 分项需求 | 需要下一轮用 10-K footnotes 和 cash-flow table补充 |
| Backlog / RPO / orders |  | 未在本轮官方 earnings release 摘要中找到可直接引用的 backlog/RPO | 不能用新闻稿里的客户合作替代订单/backlog | 下一轮查 10-K、CFO commentary 和 call Q&A |
| ASP / shipment / capacity |  | 未在本轮官方 earnings release 摘要中找到 GPU ASP、shipment 或 capacity 数据 | 不能推断单卡价格或出货量 | 下一轮只作为待核验，不用第三方估算进入结论 |
| Customer / product evidence | FY2026 earnings release, Data Center highlights | 原文列出 AWS、Google Cloud、Microsoft Azure、Oracle Cloud Infrastructure 将是 Vera Rubin-based instances 的首批部署云商之一；并披露与 Meta、Anthropic、CoreWeave 等合作线索 | 不能证明各客户采购金额、合同条款或取消条件 | 客户/合作为官方披露事实，但财务量化仍待核验 |
| Technical roadmap / qualification | FY2026 earnings release, Data Center highlights | 原文披露 Rubin platform、Blackwell Ultra、BlueField-4 等 data center roadmap / platform highlights | 不能证明每条产品线的量产良率、供给瓶颈或单位经济 | 需要补产品页和架构资料 |

## 结论分层

| 层级 | 内容 |
| --- | --- |
| 原文已证明 | NVIDIA 的 AI/Data Center 暴露已经直接体现在公司原文的收入规模：FY2026 Data Center revenue $193.7B，Q4 FY2026 Data Center revenue $62.3B。公司原文也证明 Q4 和 FY2026 仍保持高毛利率，并披露 Rubin / Blackwell / networking / cloud partner 等平台路线。 |
| 合理推论 | NVDA 是 `D1` 核心依赖：OpenAI/Anthropic/Gemini 等模型需求通过 GPU/CUDA/networking/rack-scale systems 传导到 HBM、CoWoS、networking、server/rack、电力和光互连。这个链条方向合理，但各下游公司受益幅度必须用各自原文单独证明。 |
| 待原文核验 | 具体客户采购金额、GPU 出货量、ASP、HBM/CoWoS allocation、GB300/Blackwell/Rubin 供给节奏、Data Center 单独毛利、China export control 影响。 |
| 主要反证 | FY2026 全年 GAAP/non-GAAP gross margin 低于 FY2025；inventory 同比显著上升，需要确认是供应链准备还是需求/产品切换风险。其他反证包括 ASIC 替代、客户 CapEx 放缓、出口管制、推理效率提升压低单位算力需求。 |

## 研究判断

| 维度 | 评分 1-5 | 依据 |
| --- | --- | --- |
| AI 需求相关度 | 5 | Data Center revenue 是公司收入主轴，且原文明确由 accelerated computing and AI platform shifts 驱动 |
| 供给瓶颈 | 4 | 供应链瓶颈方向合理，但本轮未用原文证明 HBM/CoWoS/GPU capacity 数量 |
| 议价权 | 5 | 高收入增长下仍保持高公司整体毛利率 |
| 持续性 | 4 | Rubin / Blackwell / cloud partner roadmap 支撑延续性，但客户 CapEx 和供给周期仍需跟踪 |
| 财务传导 | 5 | 收入、利润、毛利率均有官方数据支撑 |
| 技术护城河 | 5 | CUDA、GPU、networking、rack-scale systems 和产品路线均为 D1 核心 |
| 估值空间 |  | 本卡不做估值结论 |
| 反证清晰度 | 4 | 毛利率变化、inventory、出口管制、ASIC 替代和客户 CapEx 是可跟踪反证 |

## 当前动作

保持 `P0_first_batch / core_review`，但结论只限于“D1 核心依赖已被原文收入和产品路线支持”。下一轮必须补 FY2026 10-K、CFO commentary / call transcript、GB300/Blackwell/Rubin 官方产品资料，再更新客户、库存、供给和出口管制反证。

## 当前动作

- [ ] 找到最新 annual report / 10-K / 20-F 或交易所年报。
- [x] 找到最新季度 earnings release / investor presentation。
- [ ] 找到 earnings call transcript 或公司说明会材料。
- [ ] 核对收入、订单、backlog、产能、毛利率、客户关系和技术路线。
- [x] 写清楚升级 / 保持候选 / 降为雷达 / 排除的条件。
