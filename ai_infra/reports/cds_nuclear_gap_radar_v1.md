# CDS / Credit Risk and Nuclear Gap Radar v1

状态：gap radar, pending original-source verification  
日期：2026-05-12

边界：这是 AI Infra 研究框架补丁，不是投资建议、买卖建议、目标价或仓位建议。

## 结论

我们没有完全忘记核电，但覆盖很浅；CDS / 信用风险则基本没有真正建成模块。

- **CDS / credit risk**：应放进 `D5 融资约束 / 反证系统`，用于观察 NeoCloud、AI data center developer、IDC、utility、equipment leasing 和 GPU-backed financing 会不会被信用市场提前否定。
- **核电 / nuclear**：应放进 `D5 电力约束 / 远期供给雷达`，用于判断 AI 数据中心的 24/7 firm power、PPA、核电重启、SMR、uranium、enrichment 是否真的形成长期瓶颈。

这两个模块都很重要，但它们不是第一轮 MVP 的 `D1-D3` 主战场。正确位置是：**反证仪表盘 + gap radar + 资金/信用监控**。

## 1. CDS / 信用风险模块

### 为什么重要

AI Infra 正在从轻资产软件叙事转成重资产基建周期。NeoCloud、AI data center、IDC、powered land、GPU leasing、project finance 都依赖低成本资本和可持续融资。

如果信用市场先变坏，股价故事可能还没破，但债务和 CDS 已经开始反映：

- 客户合同质量不够；
- GPU 残值被下调；
- 利息费用吞掉毛利；
- backlog 不能转成现金流；
- 数据中心项目融资成本上升；
- 租赁义务和表外 JV 风险被重估。

### 应该跟踪什么

| 层级 | 指标 | 用途 |
| --- | --- | --- |
| 单公司信用 | CDS spread / bond spread / yield to maturity | 看市场是否开始给公司违约或再融资风险定价 |
| 债务结构 | debt maturity wall、secured debt、convertible notes、lease liabilities | 看融资期限和再融资压力 |
| 经营覆盖 | EBITDA / interest expense、OCF、FCF、CapEx commitments | 看收入增长是否真的能覆盖资本成本 |
| 合同质量 | take-or-pay、prepayment、termination rights、customer concentration | 看 backlog 是否能变成现金流 |
| 资产残值 | GPU residual value、depreciation policy、equipment useful life | 看 GPU 租赁模型会不会被技术迭代打穿 |
| 市场信用 beta | CDX IG/HY、HYG/LQD spreads、SOFR、high-yield issuance | 看宏观信用环境是否支持 AI infra 扩张 |

### 重点公司/资产

| 类别 | 现有 universe 代表 | 关注点 |
| --- | --- | --- |
| NeoCloud | CoreWeave、Nebius、Oracle、IREN、Applied Digital、Core Scientific、TeraWulf | backlog、debt、interest expense、utilization、GPU residual |
| Data center / IDC | GDS、Digital Realty / Equinix 后续待补 | debt cost、occupancy、pre-lease、capex/MW |
| Power / equipment | Vertiv、Eaton、GE Vernova、Schneider、Powell、Bloom Energy | backlog 是否可转收入，客户付款能力 |
| Hyperscaler | Microsoft、Amazon、Alphabet、Oracle、Meta | CapEx、depreciation、FCF、credit rating |
| Credit proxies | CDX IG/HY、HYG、LQD、IEF/TLT、SOFR | 信用市场环境，不是公司事实 |

### CDS 的现实限制

零售研究通常拿不到完整单名 CDS 数据，或者数据成本很高。因此 MVP 可以用代理指标：

1. 公司债价格、收益率、OAS；
2. 10-K / 10-Q 的 debt maturity、lease liabilities、interest expense；
3. 高收益债 ETF、投资级债 ETF、CDX 指数；
4. 可转债价格和隐含信用风险；
5. 股票期权 IV / skew 作为事件风险补充，但不能当基本面事实。

### CDS / 信用模块的结论规则

| 信号 | 解释 |
| --- | --- |
| 股价涨但债券跌 / spread 扩大 | 股票叙事可能强，信用市场不买账 |
| backlog 增长但 FCF 恶化 | 可能是重资产资本吞噬，而不是软件式增长 |
| interest expense 增速高于 gross profit | NeoCloud / IDC 风险上升 |
| customer concentration + debt maturity wall | 单一客户延期可能触发融资压力 |
| GPU useful life 假设过长 | 技术迭代会放大残值风险 |

## 2. 核电 / Nuclear 模块

### 为什么重要

AI 数据中心需要高负载、稳定、可预测的 24/7 电力。核电、SMR、核燃料、铀、HALEU、燃气轮机、输电和变压器，都是 D5 层的远端约束。

但核电的特殊问题是：周期很长、监管重、资本开支大，很多资产更像长期政策/能源选项，不一定能在 1-3 年内传导到 AI Infra 收入。

### 现有覆盖

当前 universe 已经有少量核电/能源雷达：

| 代码 | 公司 | 当前模块 | 当前定位 |
| --- | --- | --- | --- |
| LEU | Centrus Energy | Nuclear fuel / HALEU | D4-D5 雷达 |
| CCJ | Cameco | Uranium | D4-D5 雷达 |
| BE | Bloom Energy | On-site power / fuel cells | D4-D5 雷达/候选 |
| GEV | GE Vernova | Grid + gas turbines | D4 雷达/候选 |
| ENR.DE | Siemens Energy | Grid/power equipment | D4 雷达/候选 |

这还不够。核电链条至少应该作为第二阶段 gap radar 补以下类别。

### 待补核电/能源候选池

| 类别 | 候选线索 | 核验重点 |
| --- | --- | --- |
| Regulated / merchant nuclear power | Constellation、Vistra、Talen、NRG | 数据中心 PPA、nuclear capacity、power price、customer contracts |
| Nuclear fuel / uranium | Cameco、Kazatomprom、Centrus / LEU | uranium price、contract book、HALEU milestones、regulatory approvals |
| SMR / advanced nuclear | NuScale、Oklo、BWXT、Rolls-Royce SMR | licensing、project timeline、customer agreement、cash runway |
| Gas turbine / firm power | GE Vernova、Siemens Energy、Mitsubishi Heavy | turbine backlog、delivery time、data center demand |
| Grid / transmission | ABB、Schneider、Eaton、Powell、Quanta Services、Prysmian、Nexans | transformer/switchgear/backlog、grid interconnect bottleneck |
| Storage / backup | Fluence、Tesla Energy、Bloom Energy | duration、fuel cost、customer economics |

### 核电模块的结论规则

| 问题 | 判断 |
| --- | --- |
| 是否有数据中心客户合同 / PPA？ | 有才可能从叙事进入候选 |
| 项目是否能在 1-5 年内并网或供电？ | 否则更多是 long-duration option |
| 收入是否已经体现？ | 没体现则只做雷达，不做核心 |
| 监管 / licensing 是否可验证？ | 不可验证则降级 |
| 电价是否被客户接受？ | 决定云毛利和数据中心经济性 |
| 是否只是 uranium commodity beta？ | 如果是，只能作为能源雷达 |

## 3. 怎么纳入现有项目

### 不改主战场

`D1-D3` 仍然是主战场：

- GPU / TPU / ASIC / cloud；
- HBM / CoWoS / AI server / networking / optics / power cooling；
- HBM equipment / test / ABF / retimer / liquid cooling / electrical components。

### 新增两个雷达

| Radar | 作用 | 输出 |
| --- | --- | --- |
| Credit / CDS Radar | 监控 AI infra 是否被融资端反杀 | credit dashboard、debt maturity table、spread proxy |
| Nuclear / Firm Power Radar | 监控 AI data center 电力约束 | nuclear/firm power candidate pool、PPA/project evidence |

### 与 evidence card 的关系

信用和核电证据卡不应只看收入增长，而要重点看：

- 合同条款；
- 资产负债表；
- 债务到期；
- 监管许可；
- 项目时间；
- 客户信用；
- 资本成本。

## 4. MVP 排期建议

不要马上把 CDS 和核电塞进 Batch 1。先这样做：

1. 继续完成 NVIDIA 样板后的 3-5 张 D1-D3 evidence cards。
2. 新建 `Credit / CDS Radar v1`，先覆盖 CoreWeave、Nebius、Oracle、IREN、APLD、CORZ、GDS。
3. 新建 `Nuclear / Firm Power Radar v1`，先覆盖 CEG、VST、TLN、NRG、GEV、LEU、CCJ、OKLO、SMR、BWXT。
4. 只在它们能证明反向卡住 AI data center / cloud CapEx 时，才从 D5 雷达升级。

## 5. 当前判断

CDS/信用风险：确实漏了，应该补。它是 NeoCloud 和 AI data center 最重要的反证层之一。

核电：没有完全漏，但覆盖不够。当前只有 LEU、CCJ、BE、GEV 等少量雷达，需要补成完整 `firm power / nuclear / grid` 观察池。

两者都重要，但都不应该打断当前 D1-D3 原文核验 MVP。
