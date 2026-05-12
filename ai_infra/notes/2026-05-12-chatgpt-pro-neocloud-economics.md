# ChatGPT Pro 输出草稿：NeoCloud / GPU-as-a-Service 经济模型

状态：ChatGPT Pro 输出草稿，待原始出处逐条核验  
捕获时间：2026-05-12 12:46:12 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a02aaa3-ce60-83ea-a2c9-683dc30ca35b

> 使用边界：本文只作为研究线索和问题清单，不作为投资建议或买卖建议。涉及收入、订单、backlog、CapEx、毛利率、产能、ASP、客户关系、技术路线等实际结论，必须回到公司原始披露或监管文件核验后，才能进入主研究结论。

## 原始提问

主题6：NeoCloud / GPU-as-a-Service 经济模型研究。

请基于本项目已上传的 md 来源文件，建立 NeoCloud / GPU-as-a-Service 的单位经济模型，判断它是高增长云平台，还是 GPU 租赁 + 高杠杆基础设施周期。

严格要求：
1. 先列出需要核验的公司原始出处，不要直接把媒体/模型记忆当结论；
2. 区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”；
3. 不给买卖建议，不给目标价；
4. 不要把高 revenue backlog 直接等同为高质量现金流，必须拆 CapEx、debt、lease liabilities、depreciation、utilization、customer concentration、GPU residual value。

重点公司：
CoreWeave、Nebius、Oracle、Lambda、Crusoe、IREN、Applied Digital、Hut 8、TeraWulf、Core Scientific、DigitalOcean。

输出：原始出处清单、每 MW/每 GPU/每 rack 的收入成本折旧融资模型、backlog 到 revenue 到 FCF 的转化路径、GPU residual value 风险、客户集中和违约风险、反证指标。
展开

## ChatGPT Pro 输出

根据 2026-05-12 项目 md，NeoCloud / GPU-as-a-Service 已被放在“AI 云把 GPU、网络、电力、机房、融资打包成服务”的层级，项目文档明确要求跟踪 GPU fleet、contracted backlog、MW/GW secured power、CapEx、debt / lease liabilities、utilization、GPU depreciation、gross margin、customer concentration、RPO，并把融资成本、客户集中、GPU 残值、供给过剩、违约、交付延迟和“收入增长但 FCF 恶化”作为核心反证。

2026-05-12-ai-super-cycle-resea…

 下面的模型按项目 checklist 的原则处理：实际数据必须回到年报、10-K/20-F、10-Q、earnings release、earnings call、investor presentation、交易所公告和公司技术资料；媒体和模型输出只作为线索。

research-checklist

1. 先核验的公司原始出处清单
公司	必核原始出处	必核指标
CoreWeave	最近 10-K、10-Q、Q1 2026 earnings release、earnings presentation、call transcript、债务/租赁/客户集中 footnotes。CoreWeave IR 已列出 Q1 2026 press release、webcast、presentation、quarterly filing、transcript 和 outlook materials。
CoreWeave
	revenue backlog / RPO、top customer concentration、GPU purchase commitments、lease liabilities、debt maturity、interest expense、depreciation policy、utilization、contract term、take-or-pay / cancellation terms。
Nebius	FY2025 Form 20-F、Q4/FY2025 earnings release、shareholder letter、6-K、Microsoft commercial agreement 6-K、Q1 2026 release。Nebius IR 显示 FY2025/Q4 materials 已发布，Q1 2026 webcast 安排在 2026-05-13；因此截至 2026-05-12，Q1 2026 仍是下一步核验项。
Nebius
+1
	AI cloud revenue / ARR、customer advances、CapEx plan、funding gap、data center leases、GPU fleet、customer concentration、operating cash flow vs prepayments。
Oracle	FY2025 10-K、FY2026 10-Q、Q3 FY2026 earnings release / call。Oracle Q3 FY2026 release 披露 RPO、cloud revenue、cloud infrastructure revenue，并说明 Q3 RPO 增量大多与大规模 AI 合同相关，部分设备由客户预付款或客户自购 GPU 支持。
Oracle投资者关系
	OCI IaaS revenue、RPO conversion、customer prepayments、customer-owned GPU arrangements、CapEx、debt/equity financing、cloud gross margin、depreciation。
Lambda	私有公司：官方融资公告、credit facility documents、若有则核验 audited financials、lender presentation、customer contracts。Lambda 官方公告披露 2026-05-07 关闭 $1B senior secured credit facility，用于 NVIDIA AI accelerator infrastructure 和 data center capacity；2025 Series D 也有官方公告。
Lambda
+1
	GPU-backed debt、collateral、customer base、contracted revenue、utilization、GPU fleet cost、depreciation life、residual assumptions。
Crusoe	私有公司：官方融资/JV公告、Microsoft / Oracle / OpenAI 相关 campus contracts、credit facility、site-level lease/JV agreements。Crusoe 官方披露 Abilene 900MW Microsoft campus、预计全站约 2.1GW，并披露 Brookfield $750M credit facility 和 Abilene JV 线索。
Crusoe
+1
	MW under contract、customer identity、tenant credit、power plant / grid cost、project debt、JV waterfall、lease duration、take-or-pay、construction schedule。
IREN	10-Q、20-F、investor presentation、SEC filings、AI Cloud monthly updates、GPU financing documents。IREN 官网披露 AI Cloud / colocation / build-to-suit、>4.5GW secured power、810MW operational、2,100MW under construction 等数据，应回 SEC filing 核验口径。
IREN
	AI Cloud revenue vs Bitcoin mining revenue、GPU count、MW capacity、GPU financing、power cost、debt、depreciation、customer contracts。
Applied Digital	FY2025 10-K、FY2026 10-Q、lease footnotes、CoreWeave lease agreements、debt / finance lease footnotes。APLD 披露 250MW CoreWeave leases 预计约 $7B / 15 年，后续 150MW option，并在 10-Q 中列示 400MW CoreWeave + 200MW investment-grade hyperscaler lease 及 minimum contracted payments。
Applied Digital Corporation
+1
	MW leased、minimum lease payments、tenant credit、construction capex/MW、project financing、guarantees、lease classification、debt and preferred equity。
Hut 8	FY2025 10-K、Q1 2026 10-Q / 8-K、Beacon Point lease filing、project finance documents。Hut 8 披露 15-year 352MW IT AI data center lease、$9.8B base-term contract value、triple-net take-or-pay，并披露 $3.25B senior secured notes for River Bend。
美国证券交易委员会
	contracted MW、lease revenue/MW-year、tenant credit、loan-to-cost、non-recourse debt、construction risk、power pipeline conversion。
TeraWulf	10-K、10-Q、Note 8 / WULF Compute disclosure、HPC leases、site power disclosures。TeraWulf 10-Q 披露 WULF Compute 提供 GPU workload 的 HPC leasing / colocation / cloud / connectivity，并列出 Lake Mariner HPC leasing capacity。
TeraWulf Inc.
	Bitcoin mining vs HPC revenue, critical IT MW, long-term lease terms, customer identity, capex, debt, power cost, conversion cost。
Core Scientific	10-K、10-Q、CoreWeave agreements、customer-funded capex footnotes、HDC colocation segment。Core Scientific 披露 2025 CoreWeave 扩展约 590MW、$10.2B contracted revenue / 12 years，并披露某 70MW expansion 中 Core Scientific 负责 powered shell capex、CoreWeave 负责额外 capex；2026 Q1 又披露 colocation revenue、capex 中 CoreWeave funded portion。
Core Scientific, Inc.
+1
	colocation MW、customer-funded capex、average annualized colocation revenue、debt、impairment、Bitcoin exposure、customer concentration。
DigitalOcean	10-K、10-Q、Paperspace/GPU product disclosures、GPU pricing page、CapEx/depreciation footnotes。DigitalOcean 官网 GPU Droplets 价格页披露起价和 on-demand pricing，但该价格只可作为产品价格线索，不能直接代表 frontier H100/B200/GB300 economics。
数字海洋
	GPU revenue mix、SMB / developer customer churn、GPU utilization、capex intensity、server depreciation、gross margin by product if disclosed。
2. 结论分层：不要把 backlog 直接等同现金流
原文已证明

项目文件已证明研究方法：NeoCloud 必须拆 revenue backlog、CapEx、debt、lease liabilities、utilization、gross margin、interest expense、depreciation、GPU fleet、power secured、cluster size、network topology、time-to-power。

2026-05-12-ai-super-cycle-resea…

 项目 checklist 也明确要求对云和 NeoCloud 看利用率、折旧、融资成本和客户期限，不能只看收入增长。

research-checklist

官方原文还证明：部分公司确实已经进入长约 AI data center / colocation 或 GPU cloud 融资结构。例如 APLD、Hut 8、Core Scientific 披露了按 MW 计价的长期 AI/HPC leases；Lambda、Crusoe 披露了信用设施或项目融资；Oracle 披露了大规模 AI RPO 及客户预付款/客户自购 GPU 的融资结构。
Oracle投资者关系
+5
Applied Digital Corporation
+5
美国证券交易委员会
+5

合理推论

NeoCloud 不是一个单一商业模式，而是三类资产混合：

GPU-owning AI cloud：CoreWeave、Nebius、Lambda、IREN AI Cloud、DigitalOcean GPU / Paperspace。核心是 GPU 利用率、$/GPU-hour、depreciation、financing spread、residual value。

AI data center / powered shell / colocation：Applied Digital、Hut 8、Core Scientific、TeraWulf、Crusoe。核心是 $/MW-year、capex/MW、project debt、tenant credit、construction schedule。

Hyperscaler AI cloud / RPO platform：Oracle。核心是 RPO conversion、customer prepayment / customer-owned GPU、OCI margin、CapEx funding and depreciation。

基准判断：这个主题整体更像“高增长重资产 AI 基础设施 + GPU 租赁 + 项目融资周期”，而不是纯软件式高增长云平台。 只有当公司证明多租户高利用率、低单位融资成本、可持续 gross margin、GPU refresh 后仍有正 FCF、且客户不高度集中时，才可升级为“高质量云平台”判断。

待原文核验

逐家公司必须补齐：GPU fleet、GPU 型号、GPU capex/GPU、network/storage capex/GPU、depreciation life、estimated residual value、customer concentration、contract cancellation rights、take-or-pay coverage、lease liabilities、debt maturity schedule、interest rate、DSO/deferred revenue、customer prepayment、capex commitments、maintenance capex、utilization。

主要反证

最强反证不是“收入不增长”，而是：收入增长但 gross margin 不升；RPO/backlog 增长但交付 MW 滞后；capex/revenue 和 debt/EBITDA 上升；interest + depreciation 吃掉 gross profit；top customer 过度集中；GPU 二级市场残值下跌；客户预付款减少；contracted MW 无法按期 energize；以及 AI labs / hyperscaler 调整 CapEx。项目文档也把客户违约、融资收紧、GPU 残值下跌和复杂会计结构列为重资产 AI Infra 的核心风险。

2026-05-12-ai-super-cycle-resea…

3. 单位经济模型
3.1 每 GPU 模型：适合 CoreWeave / Nebius / Lambda / IREN AI Cloud / DigitalOcean

核心公式：

Revenue / GPU-year
= GPU hourly price × 8,760 × billable utilization
+ storage / networking / managed service attach

Cash COGS / GPU-year
= system kW per GPU × PUE × 8,760 × electricity price
+ data center lease / rent allocation
+ bandwidth / network transit
+ support / SRE / software
+ maintenance / spares

Depreciation / GPU-year
= (GPU server cost + allocated network/storage/rack capex - residual value) / useful life

Financing cost / GPU-year
= debt-funded GPU capex × interest rate
+ equipment lease interest
+ financing fees amortization

Pre-tax unit profit / GPU-year
= Revenue - Cash COGS - Depreciation - Financing cost - allocated SG&A

FCF / GPU-year
= Revenue cash collected
- Cash COGS
- cash interest
- maintenance capex
- tax
- refresh capex reserve
± working capital / customer prepayment

关键 break-even：

EBIT break-even utilization
= (Cash COGS + Depreciation + Financing cost + allocated SG&A)
  / (GPU hourly price × 8,760 + attach revenue per utilized GPU-year)

FCF break-even utilization
= (Cash COGS + cash interest + maintenance capex + refresh reserve + tax)
  / cash revenue per GPU-year

DigitalOcean 公开价格页只提供一个产品价格线索：GPU Droplets “starting at” $1.88/GPU-hour under multi-month commitment，on-demand from $0.76/GPU-hour。按 100% 利用率，这只是约 $6.7k–$16.5k/GPU-year 的低端公开价格带，不能外推到 H100/B200/GB300 frontier GPU。
数字海洋

3.2 每 rack 模型：适合 rack-scale GPU cloud / GB200/GB300 / H100/H200 clusters
Revenue / rack-year
= GPUs per rack × Revenue / GPU-year
+ rack-level network / storage / managed services

Rack power cost
= rack kW × 8,760 × PUE × electricity price

Rack depreciation
= (GPU servers + NVLink / InfiniBand / Ethernet fabric
   + storage + rack power/cooling allocation - residual value) / useful life

Rack financing
= rack capex × debt share × interest rate
+ lease liabilities interest

Rack FCF
= Revenue - power - facility opex - network opex - support
- cash interest - maintenance capex - refresh reserve

判断要点：GPU rack 的资产寿命通常短于 data center shell。若 GPU 折旧 3–5 年、但数据中心租约 12–15 年，真正风险在于第 2 轮 / 第 3 轮 GPU refresh 是否仍能以高利用率和高价格出租。若客户合同覆盖的是“capacity”而非“特定 GPU”，还要确认 refresh capex 由谁承担。

3.3 每 MW 模型：适合 Applied Digital / Hut 8 / Core Scientific / TeraWulf / Crusoe / IREN colocation

先区分三种 MW：

Utility MW = 从电网 / onsite generation 拿到的总电力
Facility MW = 机房和基础设施可用电力
Critical IT MW = 可用于服务器 / GPU 的 IT load

如果用 critical IT MW：

Revenue / MW-year
= contracted rent per MW-year
或 total contract value / contracted MW / contract years

Cash opex / MW-year
= power cost if not pass-through
+ facility O&M
+ property tax / insurance
+ site staff
+ repair / maintenance
+ water / cooling consumables

Depreciation / MW-year
= shell + MEP + substation + cooling capex per MW / useful life

Financing / MW-year
= project debt per MW × coupon
+ construction debt carry
+ lease interest

FCF / MW-year
= rent collected
- cash opex
- cash interest
- maintenance capex
- taxes
- equity contribution / JV distributions

如果用 utility MW，则：

Critical IT MW = Utility MW / PUE
Revenue / utility MW-year = Revenue / critical IT MW-year ÷ PUE

官方合同可给出几个“MW-year 收入密度”示例，但这些不是 FCF：

公司 / 合同线索	原文线索	推导收入密度
Applied Digital / CoreWeave	250MW critical IT load，约 $7B contracted revenue，约 15 年。
Applied Digital Corporation
	$7B / 250MW / 15 年 ≈ $1.87M/MW-year。
Hut 8 / Beacon Point	352MW IT AI data center lease，15 年，$9.8B base-term contract value，triple-net take-or-pay。
美国证券交易委员会
	$9.8B / 352MW / 15 年 ≈ $1.86M/MW-year。
Core Scientific / CoreWeave	CoreWeave contracted HPC infrastructure 约 590MW，$10.2B over 12-year contract terms。
Core Scientific, Inc.
	$10.2B / 590MW / 12 年 ≈ $1.44M/MW-year。

这些 MW-year 数字只说明“合同收入密度”，不说明现金流质量。必须继续扣除 capex/MW、debt cost、construction timing、tenant reimbursement、opex pass-through、maintenance capex、tax、lease accounting、以及是否存在 parent guarantee。

4. Backlog → Revenue → FCF 转化路径
1. Announced backlog / RPO
   ↓ 先核合同：binding? take-or-pay? cancellation? renewal? customer credit?
2. Funded backlog
   ↓ 先核资金：customer prepayment? project debt? vendor financing? equity? lease?
3. Built capacity
   ↓ 先核建设：permits, transformers, substations, liquid cooling, networking, GPU delivery
4. Energized / accepted capacity
   ↓ 先核 revenue recognition trigger：ready-for-service? customer acceptance? billable MW/GPU-hours?
5. GAAP revenue
   ↓ 扣 power, rent, payroll, network, maintenance, support
6. Gross profit / EBITDA
   ↓ 扣 depreciation, stock comp, SG&A, interest, lease expense
7. Operating cash flow
   ↓ 剥离 customer prepayments / deferred revenue / working capital timing
8. Free cash flow
   ↓ 扣 growth capex, maintenance capex, GPU refresh, debt amortization / project equity needs

Oracle 是一个典型提醒：其 FY2026 Q3 RPO 很高，但公司同时说明大规模 AI 合同相关设备中，部分由客户预付款让 Oracle 采购 GPU，或由客户自购 GPU 供应给 Oracle。
Oracle投资者关系
 这可以降低 Oracle 的资金压力，但不能自动证明高质量 FCF；仍要看 OCI revenue recognition、gross margin、depreciation、CapEx、debt/equity financing 和客户合同期限。

对 powered-shell / colocation 公司，backlog 转 FCF 的关键不是 GPU 利用率，而是：MW 是否按时交付、tenant 是否接收、project debt 是否低成本且 non-recourse、triple-net 是否真正把 power/O&M/property tax 传导给 tenant、以及 capex/MW 是否超支。Hut 8 的 triple-net take-or-pay 和 non-recourse project debt 是高质量合同结构的正面线索，但仍需核验债务条款、施工成本和收入确认。
美国证券交易委员会

5. GPU residual value 风险

GPU 残值风险主要出现在 owned GPU cloud，而不是纯 powered-shell landlord。

Remaining book value
= original GPU capex - accumulated depreciation

Residual stress loss
= max(0, remaining book value - market resale value)

Residual coverage ratio
= remaining contracted gross cash margin / remaining GPU book value

Refresh burden
= next-gen GPU capex needed to keep customer price/performance
  - customer-funded refresh
  - vendor financing

风险路径：

技术代际压缩残值：B200 / GB300 / future ASIC 价格性能提高，会压低 H100/H200 二手租赁价格。

供给放开压缩租金：NVIDIA 供给、hyperscaler 自建、ASIC / TPU / Trainium 增加，都可能降低 GPU-hour 价格。

合同期限错配：如果客户合同 1–3 年，但 GPU 折旧和融资假设 4–6 年，残值风险留在云商。

融资抵押品下跌：GPU-backed debt 的 collateral coverage 下降会触发 covenant / refinancing risk。

利用率与残值联动：利用率下降时，价格也往往下降；收入和抵押品价值同时受压。

反过来，如果合同是客户自购 GPU 或客户预付 GPU 采购，残值风险会部分转移给客户或被预付款覆盖。Oracle 披露的客户预付款 / 客户自购 GPU 结构就是需要重点拆解的例子。
Oracle投资者关系

6. 客户集中和违约风险

客户集中不能只看“客户是谁”，还要看合同结构：

风险项	高质量结构	低质量结构
客户信用	investment-grade hyperscaler / profitable platform	venture-backed AI lab / 单一融资事件驱动
合同	take-or-pay、不可早退、parent guarantee	可取消、短期、仅 reservation / MoU
资金	customer prepayment / customer-owned GPU / project debt matched to lease	公司先买 GPU，客户后付款
收入确认	ready-for-service 后稳定 over-time	依赖客户 ramp / usage
集中度	多客户、多站点、多产品	单客户、单站点、单 GPU generation

公司层面初步分类：

类型	公司	判断
AI cloud platform 候选	CoreWeave、Nebius、Lambda、Oracle、DigitalOcean	需要证明多租户利用率、平台软件/服务 attach、稳定 gross margin、低融资成本和 GPU refresh 后正 FCF。
GPU-owning / energy-integrated hybrid	IREN	需要拆 AI Cloud 与 Bitcoin mining，确认 AI revenue、GPU fleet、GPU financing、客户期限和 residual risk。
Powered-shell / colocation / AI data center developer	Applied Digital、Hut 8、Core Scientific、TeraWulf、Crusoe	更像项目融资基础设施资产；看 $/MW-year、capex/MW、debt cost、tenant credit、construction delivery，而不是按 SaaS 云平台估。

Core Scientific 还提供了一个重要当前状态：CoreWeave 收购 Core Scientific 的协议在 2025-10-30 因股东未批准而终止，两家公司继续商业合作；所以建模时应把 Core Scientific 继续作为独立 colocation / HDC 公司处理，而不是并入 CoreWeave。
CoreWeave
+1

7. 反证指标仪表盘
指标	红旗
Utilization	installed GPU 增长快于 billable GPU-hours；utilization 低于 break-even utilization；spot GPU price 下跌。
Revenue quality	backlog/RPO 增长但 deferred revenue、customer prepayment、cash collection 不增长；DSO 上升。
CapEx intensity	CapEx/revenue 持续上升；capex/MW 超预算；GPU refresh reserve 未计入。
Depreciation pressure	depreciation + interest > gross profit；延长折旧年限但实际 GPU 租价下降。
Debt / lease liabilities	debt maturity 与客户合同 cash flow 不匹配；floating-rate debt 暴露；lease liabilities 被低估。
Customer concentration	top 1 customer 占收入或 backlog 过半，且无 parent guarantee / prepayment / take-or-pay。
Residual value	二手 GPU 价格低于账面 residual assumption；lender collateral haircut 提高。
Construction delivery	contracted MW 增长但 energized MW 连续两个季度落后；transformer、substation、liquid cooling、network 交付延迟。
Margin quality	revenue 增长但 gross margin 不升；power / rent / support / network cost 吞掉收入。
Technology substitution	客户转向 ASIC / TPU / Trainium / internal cloud；GPU demand 从短缺转为价格竞争。
8. 当前研究判断

基准结论：NeoCloud / GPU-as-a-Service 不能整体按“高增长云平台”处理。 它更像一个分层的 AI 基建金融模型：上层是 GPU-hour / AI cloud revenue，中层是数据中心、电力、网络和冷却，下层是 debt、lease liabilities、customer prepayments、project finance 和 GPU residual value。

要把某家公司升级为“高增长云平台”，必须原文证明：利用率高、客户分散、软件/服务 attach 提升、gross margin 扩张、折旧和利息后仍能正 FCF、GPU refresh 不需要持续稀释或高成本债务。否则，即使 backlog 很高，也更应按“GPU 租赁 + 高杠杆基础设施周期”建模。
