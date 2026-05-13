# ChatGPT Pro Company Financials + K-line + Options Research Methodology

Status: ChatGPT Pro output, pending original-source verification
Conversation URL: https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a03edfe-dc28-83ea-b124-643a3345163e
Captured at: 2026-05-13

以下框架把候选公司研究拆成三个不可互相替代的层级：原始证据层决定“它是否真的在 AI Infra 链条里赚钱”；K线层决定“市场是否开始定价、是否拥挤、风险如何”；期权层决定“波动、事件和流动性风险如何预算”。 这不是买卖建议，不给目标价，不给实际仓位；所有评分都定义为 research priority score，不是投资建议。

项目文档中的底层原则应作为硬约束：先证明需求，再讨论弹性；先找反证，再给叙事；先看公司原始披露，再看二手摘要；涉及收入、segment revenue、AI/data center revenue、客户集中度、backlog、RPO、订单、产能、ASP、毛利率、CapEx、库存、FCF 等实际数据时，必须回到公司年报、10-K/20-F、季报、earnings release、call transcript、investor presentation、交易所公告、监管文件、公司官网技术资料、产业协会或客户/供应商交叉披露；媒体、数据库和 ChatGPT Pro 输出只能作为线索。

research-checklist

0. 总体研究架构
核心分层
D0: LLM demand / token demand
  ↓
D1: GPU / ASIC / AI accelerator demand
  ↓
D2: HBM / CoWoS / network / power / cooling bottlenecks
  ↓
D3: server / optics / testing / substrate / ODM / data center equipment
  ↓
D4: materials / EDA / IP / WFE / probe / packaging tools
  ↓
D5: theme-mapped / weakly linked companies

每家公司必须先回答：

如果 LLM 训练/推理需求持续增长，
那么 [公司/模块] 会因为 [瓶颈/议价权/技术壁垒/客户认证/产能约束]
获得 [收入 / 毛利率 / 订单 / 现金流 / ROIC] 的改善。

如果这句话写不清楚，公司先进入 待定位池，不要进入核心研究。

研究结论状态

所有结论只允许落入四类：

状态	含义	是否可进入核心判断
原文已证明	公司原始披露、监管文件、产品资料或客户/供应商交叉披露直接支持	可以
合理推论	原文证明 A + B，但 A→C 是产业链推导	可以，但必须单独标注
待原文核验	只有媒体、数据库、二手摘要、市场传闻或模型输出	不进入核心判断
主要反证	能够推翻或削弱 thesis 的事实、趋势或缺口	必须保留

这与项目已有证据卡片模板一致：原文证据必须记录指标、原文位置、能证明什么、不能证明什么和口径备注；结论层要分为原文已证明、合理推论、待原文核验、主要反证。

source-evidence-template

1. Company financials research workflow
1.1 公司研究标准流程
Step 1：定位 BFS depth 和 dependency edge

每家公司先生成：

JSON
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

BFS depth 的作用是防止“AI 概念泛化”。比如 HBM、先进封装、网络、电力属于更直接的瓶颈；服务器 ODM、液冷、光模块、测试设备属于配套扩张；材料、EDA、基板、气体、化学品属于上游制造；如果收入关联弱，只能标为主题映射。项目 checklist 已经定义了 S0-S5 分层：S1 核心算力，S2 直接瓶颈，S3 配套扩张，S4 上游制造，S5 主题映射。

research-checklist

Step 2：写一句话研究假设

模板：

如果 [训练/推理/AI data center/ASIC/HBM/CoWoS/800G/电力] 需求持续增长，
那么 [公司] 会因为 [产品/客户认证/产能瓶颈/技术路线] 获得
[收入、订单、毛利率、现金流、ROIC] 的改善。

不合格例子：

公司有 AI exposure，股价强，所以可能受益。

合格例子：

如果 HBM4 stack 高度和测试复杂度继续提升，那么 HBM memory tester / probe card 供应商会因为测试时间、probe complexity 和客户认证门槛上升，获得订单、毛利率和 backlog 可见度改善。
Step 3：建立 source checklist

按优先级抓取：

来源类型	用途	状态标签
10-K / 20-F / annual report	segment、客户集中、风险、CapEx、现金流、债务、长期披露	primary
10-Q / quarterly report	最新季度收入、订单、库存、应收、债务、现金流	primary
earnings release	最新财务摘要、guidance、non-GAAP、管理层摘要	primary
earnings call transcript	管理层对 AI、客户、backlog、产能、margin 的解释	primary-ish，但需警惕营销语言
investor deck / capital markets day	roadmap、TAM、产品架构、客户类型、长期模型	primary-ish
product page / technical whitepaper	产品是否真实服务 HBM、CPO、AI server、data center power 等	primary technical
customer/supplier cross-disclosure	交叉验证客户、供应关系和技术路线	high-value cross-check
行业协会/标准组织	PCIe、CXL、UCIe、Ethernet、OCP、JEDEC、Open Compute 等路线验证	supporting
媒体/券商/数据库	只作为线索	secondary only

美国公司优先接 SEC EDGAR：SEC 说明 EDGAR APIs 可访问 company submissions 和 extracted XBRL data，data.sec.gov 提供 JSON REST API，且不需要认证或 API key；当前 API 包括 submissions history 和 10-Q、10-K、8-K、20-F、40-F、6-K 等财报 XBRL 数据。
美国证券交易委员会
+1

Step 4：逐项抽取字段

每家公司形成 financial_metrics.csv 和 evidence_claims.jsonl。

模块	字段	研究问题	证据状态
Revenue	total revenue, segment revenue, AI/data center revenue, product revenue	收入是否已经进入财报，而不是只有口头 AI opportunity？	原文优先
Orders	backlog, RPO, bookings, book-to-bill, long-term agreement, prepayment	需求是否有可见度？订单是否可取消？是否转化为收入？	原文优先
Margin	gross margin, operating margin, product mix, yield, ASP	需求上升是否带来议价权，而不是低毛利 pass-through？	原文 + 推论
CapEx / capacity	CapEx, capacity expansion, tool delivery, lead time, utilization	扩产是否支撑未来收入？是否带来折旧压力？	原文优先
Working capital	inventory, receivables, payables, cash conversion cycle	是否提前备货？是否存在库存/应收风险？	原文优先
Cash / debt	OCF, FCF, debt, lease liabilities, interest expense, interest coverage	增长是否转为现金流？重资产公司是否靠杠杆堆收入？	原文优先
Customer	top customer %, named customers, geography, end-market	AI demand 是否来自 hyperscaler、GPU 厂、HBM 厂、OSAT、数据中心？	原文/交叉披露
AI keywords	AI, data center, HBM, CoWoS, CPO, silicon photonics, liquid cooling, power	公司是否明确把产品映射到 AI Infra？	原文，但不能单独证明收入
Guidance	revenue guide, margin guide, CapEx guide, backlog conversion	下一季度/年度管理层预期如何？	原文
Roadmap	HBM3E/HBM4, 800G/1.6T, CPO, GB rack, PCIe/CXL, liquid cooling	技术路线是否延续，是否切换？	原文/技术资料
Counterevidence	weak AI disclosure, margin compression, inventory rise, customer loss, ASP decline	什么会推翻 thesis？	必填

财务传导最低要求：不能只看收入；必须看毛利率和 FCF；重资产公司还要看折旧、库存、CapEx 回收周期；云和 NeoCloud 必须看利用率、折旧、融资成本和客户期限。

research-checklist

1.2 模块差异化财报指标
A. HBM / memory vendor

核心问题：这是 AI 真实拉动、传统 memory 周期反转，还是二者混合？

指标	重点
HBM revenue / HBM mix	HBM 占 DRAM revenue 比例、同比/环比、客户 qualification
DRAM segment margin	HBM 是否提升 blended gross margin
ASP / bit shipment	ASP 和 bit shipment 是否同步改善
HBM3E / HBM4 / HBM4E roadmap	代际切换是否提升 stack count、bandwidth、capacity
wafer allocation	HBM 是否挤出 commodity DRAM supply
CapEx	HBM capacity expansion 是否过快导致未来供给过剩
inventory	是 AI 拉动还是补库存
customer concentration	是否过度依赖单一 GPU/ASIC 客户
counterevidence	HBM ASP 下行、qualification 失败、供给追上、AI accelerator demand 放缓

项目文档已把存储拆成三类：真实 AI 拉动包括 HBM、HBM test、TCB/hybrid bonding、AI eSSD、server DRAM；传统周期反转包括 commodity DRAM、consumer NAND；普通 NAND/普通 SSD/无 enterprise 客户的概念股属于 AI 叙事映射风险。

2026-05-12-ai-super-cycle-resea…

B. HBM equipment / test / probe / substrate / materials

核心问题：memory vendor 的 HBM 扩张是否传导为设备、测试、基板、材料的订单和毛利？

子模块	关键指标
TCB / hybrid bonding equipment	orders, backlog, delivery lead time, HBM/advanced packaging customer split
molding / underfill / MUF	AI/HBM package related revenue, material mix, ASP, customer qualification
wafer thinning / dicing	advanced package exposure, tool utilization, shipment backlog
memory tester	memory tester revenue, HBM test demand commentary, tester capacity
probe card / socket	probe card ASP, HBM/GPU/advanced package exposure, top customer share
ABF / substrate	substrate ASP, layer count, AI package share, utilization, yield
inspection / metrology	advanced packaging inspection revenue, orders, gross margin, customer wins

必须区分：泛半导体复苏 vs HBM/CoWoS 特定拉动。项目研究地图中，HBM 设备/测试/材料/基板被列为第一优先级，原因是 HBM 本身由大公司主导，但更高弹性可能在 TC bonder、hybrid bonding、tester、probe、ABF、substrate、underfill、inspection 等二阶瓶颈。

2026-05-12-ai-super-cycle-resea…

C. Optics / CPO / silicon photonics

核心问题：AI cluster 是否把光互连从通信周期变成数据中心算力周期？

指标	重点
datacom revenue	telecom vs datacom 拆分，AI data center exposure
800G / 1.6T mix	高速产品占比、ASP、shipment
customer concentration	hyperscaler 或 switch ASIC 客户是否集中
laser capacity	InP laser、EML、DFB、CW laser capacity / yield
DSP / LPO / LRO	技术路线变化是否改变价值分配
CPO design win	是否只是 roadmap，还是进入客户 qualification / revenue
gross margin	高速 mix 是否改善毛利，还是被价格战吞掉
inventory	optics 周期容易库存波动
counterevidence	CPO 延后、pluggable 延寿、ASP 快速下滑、客户自研或供应商替换

项目地图给出的光互连验证指标包括 datacom revenue、800G/1.6T mix、laser capacity、gross margin、customer concentration，以及 CPO reliability、external light source、InP wafer supply 等技术指标。

2026-05-12-ai-super-cycle-resea…

D. AI server / ODM

核心问题：AI server revenue 是否只是 GPU pass-through，还是 ODM/系统集成商获得更高附加值？

指标	重点
AI server revenue mix	是否明确披露 AI server / rack-scale revenue
gross margin	收入增长是否同步提升毛利率
inventory	GPU / component inventory 是否过高
customer concentration	单一 hyperscaler 或 GPU platform 风险
rack shipments	liquid-cooled rack、GB rack、HGX/DGX 等系统级出货
pass-through ratio	GPU BOM pass-through 是否夸大收入、压低毛利
working capital	代工模式容易应收/库存吃现金
backlog	订单是否可见、是否可取消
counterevidence	低毛利组装、NVIDIA/云厂商内化、组件短缺导致递延、客户砍单
E. NeoCloud / AI data center developer

核心问题：这是高增长云平台，还是 GPU 租赁 + 高杠杆基础设施周期？

指标	重点
contracted backlog / RPO	合同期限、客户信用、是否 take-or-pay、是否可取消
revenue conversion	backlog 到 revenue 的转化速度
utilization	GPU / cluster / MW 利用率
gross margin	是否能覆盖 GPU 折旧、电力、运维、租赁
CapEx	GPU CapEx、data center CapEx、capex/MW、capex/GPU
debt / lease liabilities	融资结构、利息费用、lease duration
depreciation	GPU 残值和折旧年限假设
customer concentration	单一 AI lab 或 hyperscaler 风险
secured power	MW/GW、电力接入、time-to-power
counterevidence	融资成本上升、GPU 残值下跌、客户违约/延期、利用率不及预期

项目地图明确把 NeoCloud 视为“重资产、项目融资、客户集中”的新型基础设施资产，核心验证是 backlog 能否转化为高利用率、高毛利、低违约风险现金流。

2026-05-12-ai-super-cycle-resea…

F. Power / cooling / grid equipment

核心问题：AI 数据中心瓶颈是否从 GPU 转向电力、变压器、UPS、PDU、switchgear、liquid cooling、CDU、cold plate？

指标	重点
backlog / orders	数据中心相关 backlog 是否增长
book-to-bill	订单强度是否持续
lead time	transformer、switchgear、UPS、CDU 交期
data center revenue	是否拆出 data center end-market
gross margin	需求强是否带来 mix / pricing 改善
liquid cooling attach	cold plate、CDU、pump、heat exchanger 渗透
rack kW	高功率 rack 是否改变产品规格
working capital	长交期设备是否造成库存/应收压力
counterevidence	数据中心延期、电力接入成为瓶颈但设备不受益、液冷标准化导致 ASP 下行

项目研究地图把电力设备、液冷、热管理、变压器列为第一优先级之一，原因是 AI 数据中心从“买 GPU”变成“拿到电、把热带走、把 rack 点亮”。

2026-05-12-ai-super-cycle-resea…

G. EDA / IP / custom ASIC

核心问题：hyperscaler 自研 ASIC 是否把价值转向 EDA、IP、NRE、custom silicon、HBM/CoWoS 和 networking？

指标	重点
EDA recurring revenue / backlog	设计活动是否持续
IP royalty / licensing	SerDes、HBM PHY、CXL、UCIe、Arm IP
NRE revenue	custom ASIC 设计服务收入
tape-out count	是否有 AI ASIC 设计流片增长
customer concentration	单一 hyperscaler 或 ASIC 客户风险
gross margin	NRE、IP、license mix 对毛利影响
RPO / deferred revenue	多年软件和 IP 合同可见度
roadmap	HBM attach、CoWoS allocation、chiplet architecture
counterevidence	ASIC 设计失败、CUDA moat 太强、客户内化、EDA 增长被估值提前反映
H. Storage / eSSD

核心问题：AI 对 NAND/eSSD 的需求是真实数据中心需求，还是传统 NAND 周期反弹被包装成 AI？

指标	重点
enterprise SSD revenue	enterprise vs consumer 拆分
eSSD TB shipment	高容量 eSSD 是否增长
QLC mix	QLC enterprise qualification
controller revenue mix	enterprise controller vs consumer controller
PCIe Gen5/Gen6	高速接口路线
NAND ASP	ASP 上涨来自 AI demand 还是 supply cuts
inventory	NAND 周期库存风险
gross margin	高容量 enterprise mix 是否改善 margin
customer qualification	hyperscaler / storage appliance 认证
counterevidence	consumer recovery 误判为 AI、NAND ASP 来自供给收缩、利润被大客户压缩
2. K-line / price-volume research framework

K线只回答市场行为，不回答基本面真相。它可以告诉你“市场是否开始验证某个已被原文证明的 thesis”，但不能证明公司真的有 AI 收入、订单或客户。

2.1 基础数据

MVP 需要日频 OHLCV：

date, open, high, low, close, adjusted_close, volume

扩展字段：

ticker, exchange, currency, split_adjusted, dividend_adjusted, source, source_date

免费/公开数据源可以先用 Stooq 做 K线 MVP。Stooq 页面标注为 “Free Historical Market Data”，并列出 daily、hourly、5-minute 数据，以及 U.S.、U.K.、Japan、Hong Kong 等市场文件。
Stooq

2.2 价格行为特征
特征	公式/方法	用途
relative strength	stock_return - benchmark_return	判断是否跑赢 SPY/QQQ/SMH/SOXX/AIQ/GRID
rolling beta	cov(stock, benchmark) / var(benchmark)	判断系统性风险暴露
realized volatility	std(log_returns) * sqrt(252)，窗口 20/60/120	风险预算、波动 regime
max drawdown	price / rolling_max(price) - 1	下行风险
drawdown duration	从 peak 到 recover 的交易日数	资金占用/心理压力
trend	20/50/100/200 日均线、MA slope	趋势强弱
breakout	close > N-day high	市场是否重新定价
mean reversion	z-score vs moving average	过热/回撤风险
dollar volume	close * volume	流动性和可研究性
abnormal volume	volume / rolling median volume	事件关注度
earnings gap	post-earnings open/close vs pre-earnings close	财报事件定价
post-earnings drift	财报后 1/5/20/60 日相对收益	市场是否持续验证
downside gap risk	历史财报下跌 gap 分布	事件风险预算
module correlation	与 HBM basket / optics basket / power basket rolling corr	组合拥挤和主题 beta
2.3 benchmark 设计

每家公司至少对比：

Benchmark	用途
SPY	broad market beta
QQQ	growth / tech beta
SMH 或 SOXX	semiconductor beta
AIQ / BOTZ / 其他 AI ETF	AI theme beta
GRID / 工业电力 ETF	电力/电网链条
module basket	内部自建模块篮子，例如 HBM、optics、power、NeoCloud
2.4 哪些信号可用
A. 观察市场是否开始验证基本面

可用信号：

原文已证明订单/收入/毛利改善
+ 财报后相对强度上升
+ abnormal volume
+ post-earnings drift
+ 同模块 basket 同步走强

解释：市场可能开始重新定价。但前提是 原文证据已经成立。

B. 判断拥挤和过热

可用信号：

RS 连续极端上行
+ RV20 / RV120 快速升高
+ dollar volume 异常放大
+ 与 AI basket correlation 接近 1
+ price 远离 50/200 日均线
+ options IV rank 同步升高

解释：这只说明 crowded / hot，不说明 thesis 错，也不说明公司基本面强。

C. 控制组合风险

可用信号：

rolling beta to QQQ/SMH
rolling correlation with module basket
realized vol
max drawdown
liquidity
earnings gap distribution

输出不是仓位建议，而是：

research_risk_level = low | medium | high | event_high | liquidity_high
D. 发现财报前后事件窗口

可用信号：

historical earnings gap
post-earnings drift
pre-earnings abnormal volume
pre-earnings IV term structure

用途是风险预算和复盘，不是交易信号。

E. 明确不能做什么

K线不能证明：

公司有 AI revenue
公司拿到 hyperscaler 客户
公司 backlog 真实
公司毛利率会改善
公司处在 HBM / CPO / CoWoS / liquid cooling 真实链条中
3. Options data research framework

期权数据是 风险温度计 + 事件定价 + 拥挤线索。它不是基本面证据，也不是“smart money 真相”。

3.1 期权数据字段
as_of_date
ticker
expiry
days_to_expiry
strike
call_put
bid
ask
mid
last
volume
open_interest
implied_volatility
delta
gamma
vega
theta
underlying_price
source
source_timestamp

免费/公开期权数据较弱，MVP 要降低期望。Cboe 提供历史 options volume 下载，并说明其数据用于下载 Cboe 交易所的历史 options volume；同页也提供 Cboe volume 和 put/call ratio 数据，但明确有准确性免责。
Cboe Global Markets
 Nasdaq 页面提供股票 call/put option chain 信息说明，但实际页面可能出现 “Option Chain is currently not available”，因此只能作为辅助来源，不应当作为唯一来源。
纳斯达克
 Cboe 还提供 weekly options 可用列表和 CSV 下载，适合判断哪些标的存在周度期权，但不是完整 IV/OI 数据源。
Cboe Global Markets

3.2 期权研究特征
特征	定义	用途
implied volatility	option chain IV	市场预期波动
IV rank	(current IV - 52w min IV) / (52w max IV - 52w min IV)	当前 IV 是否处于历史高位
IV percentile	当前 IV 高于过去观察值的比例	更稳健的 IV 相对位置
term structure	near-term IV vs longer-term IV	财报/产品发布事件风险
skew	25-delta put IV - 25-delta call IV	下行保护需求
put/call OI	put open interest / call open interest	持仓结构线索
put/call volume	put volume / call volume	当日交易热度
earnings implied move	ATM straddle price / spot	财报隐含波动幅度
RV vs IV	realized vol vs implied vol	市场预期是否高于历史波动
gamma exposure proxy	Σ OI * gamma * contract_size * spot	可能的 dealer hedging pressure
OI clustering	大量 OI 集中在某些 strike	pin risk / crowded strikes
bid-ask spread	(ask - bid) / mid	期权流动性
chain depth	可交易 expiry/strike 数	是否适合纳入期权风险研究
3.3 期权指标分类
风险温度计
IV rank
IV percentile
RV vs IV
bid-ask spread
skew
gamma exposure proxy
OI strike clustering

用途：判断是否需要提高事件风险等级、降低研究信号置信度、标记流动性风险。

事件定价
earnings implied move
term structure inversion
near-dated IV spike
weekly option OI
post-event IV crush

用途：财报、investor day、产品发布、客户公告前后的风险预算。

smart money / crowding clue
unusual option volume
put/call volume
put/call OI
large OI strike
sweep/block prints

只能作为 clue。它可能来自对冲、做市、结构票据、covered call、protective put、dispersion trade、retail speculation，不能当作基本面证据。

3.4 避免误读期权流

必须写入系统规则：

Rule 1: option_flow_status 永远不能提升 evidence_status。
Rule 2: 没有原文证据时，期权异动只能标记为 MARKET_CLUE。
Rule 3: put/call OI 不等于方向判断，必须结合价格、IV、delta、成交方向和历史背景。
Rule 4: 高 IV 不等于公司基本面好，只说明市场预期波动高。
Rule 5: 低 IV 不等于风险低，可能是市场低估事件，也可能是数据缺失。
3.5 小账户研究原则

不输出具体仓位，但可以定义风险预算规则：

如果 earnings implied move > 账户可承受单事件亏损阈值：
    标记 event_risk = high
    不把财报前价格信号解读为基本面验证
    company_card 加入 downside_gap_risk

小账户只把期权用于：

1. 判断财报/产品发布前事件风险是否过高；
2. 判断标的期权是否流动性太差；
3. 判断市场是否已经把巨大波动计入；
4. 避免在高 IV、低流动性、宽价差环境中赌博；
5. 给研究排序打风险标签，而不是做方向下注。
4. Evidence + market + portfolio integration
4.1 三层系统
Layer 1: Evidence / fundamental truth
    判断公司是否真的在 AI Infra 链条中赚钱。

Layer 2: Market behavior / pricing and crowding
    判断市场是否开始定价、是否拥挤、是否存在事件风险。

Layer 3: Portfolio construction / risk budget
    判断研究优先级、组合相关性、波动预算、流动性风险。

Layer 1 是硬门槛。Layer 2 和 Layer 3 不能把弱证据公司升级为核心公司。

4.2 Scoring model：Research Priority Score

总分 100，但分数含义是 研究优先级，不是买入/卖出信号。

Hard gates
G0: 没有 primary source → max bucket = 待原文核验
G1: 只有 AI 叙事，没有收入/订单/客户/产品证据 → 不可进入核心研究池
G2: BFS depth = S5 且无客户/收入证据 → theme watch only
G3: 反证直接推翻 thesis → downgrade to refutation watch
G4: 市场信号不能覆盖 evidence gate
Layer 1：Evidence score，0-60
维度	分数
BFS proximity / dependency edge 清晰度	0-8
AI demand 原文证据	0-10
revenue / segment revenue 传导	0-8
order / backlog / RPO / bookings 可见度	0-8
margin / ASP / yield / mix 传导	0-8
CapEx / capacity / supply bottleneck	0-6
FCF / OCF / working capital 质量	0-6
technology roadmap 适配	0-3
counterevidence completeness	0-3
Layer 2：Market behavior score，0-20
维度	分数
relative strength after evidence event	0-5
post-earnings drift / market validation	0-4
liquidity / dollar volume	0-3
abnormal volume around verified source event	0-3
crowding penalty	-5 到 0
drawdown / volatility regime	0-5
Layer 3：Risk budget score，0-20
维度	分数
rolling beta manageable	0-4
module correlation not excessive	0-4
options/event risk transparent	0-4
downside gap history known	0-3
data quality/source coverage	0-3
liquidity risk	0-2
Bucket

沿用项目 checklist 的 1-5 分思想和研究池分层：AI 需求相关度、供给瓶颈、议价权、持续性、财务传导、技术护城河、基建周期位置、资产重估空间、反证清晰度等维度均需评分；原 checklist 把 40+ 定义为核心研究池、32-39 为重点观察池、24-31 为主题跟踪池、低于 24 暂不深挖。

research-checklist

本 pipeline 的 100 分版本可映射为：

85-100: Core research priority
70-84: High-priority watch
55-69: Thematic / validation needed
35-54: Source-needed / refutation-heavy
<35: Low priority / theme mapping
4.3 SQLite database schema
SQL
CREATE TABLE security_master (
    company_id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    ticker TEXT,
    exchange TEXT,
    country TEXT,
    currency TEXT,
    cik TEXT,
    isin TEXT,
    adr_ticker TEXT,
    local_ticker TEXT,
    module TEXT,
    bfs_depth TEXT,
    dependency_edge TEXT,
    status TEXT,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE source_registry (
    source_id TEXT PRIMARY KEY,
    company_id TEXT,
    source_type TEXT,
    fiscal_period TEXT,
    fiscal_year INTEGER,
    publication_date TEXT,
    report_date TEXT,
    url TEXT,
    local_path TEXT,
    content_hash TEXT,
    source_priority INTEGER,
    source_status TEXT,
    notes TEXT,
    FOREIGN KEY(company_id) REFERENCES security_master(company_id)
);

CREATE TABLE evidence_claims (
    claim_id TEXT PRIMARY KEY,
    company_id TEXT,
    source_id TEXT,
    metric TEXT,
    fiscal_period TEXT,
    value_text TEXT,
    value_num REAL,
    unit TEXT,
    currency TEXT,
    quote TEXT,
    source_location TEXT,
    evidence_status TEXT,
    inference_notes TEXT,
    confidence INTEGER,
    counterevidence_flag INTEGER,
    created_at TEXT,
    FOREIGN KEY(company_id) REFERENCES security_master(company_id),
    FOREIGN KEY(source_id) REFERENCES source_registry(source_id)
);

CREATE TABLE financial_metrics (
    metric_id TEXT PRIMARY KEY,
    company_id TEXT,
    fiscal_period TEXT,
    metric_name TEXT,
    value REAL,
    unit TEXT,
    currency TEXT,
    source_id TEXT,
    gaap_non_gaap TEXT,
    segment TEXT,
    notes TEXT
);

CREATE TABLE price_daily (
    company_id TEXT,
    date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume REAL,
    source TEXT,
    PRIMARY KEY(company_id, date)
);

CREATE TABLE price_features (
    company_id TEXT,
    as_of_date TEXT,
    rs_spy_60 REAL,
    rs_qqq_60 REAL,
    rs_smh_60 REAL,
    beta_spy_60 REAL,
    beta_qqq_60 REAL,
    rv_20 REAL,
    rv_60 REAL,
    rv_120 REAL,
    max_drawdown_252 REAL,
    drawdown_duration INTEGER,
    abnormal_volume_20 REAL,
    dollar_volume_20 REAL,
    module_corr_60 REAL,
    earnings_gap_last REAL,
    post_earnings_drift_20 REAL,
    PRIMARY KEY(company_id, as_of_date)
);

CREATE TABLE option_chain_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    company_id TEXT,
    as_of_date TEXT,
    expiry TEXT,
    days_to_expiry INTEGER,
    strike REAL,
    call_put TEXT,
    bid REAL,
    ask REAL,
    mid REAL,
    last REAL,
    volume INTEGER,
    open_interest INTEGER,
    implied_vol REAL,
    delta REAL,
    gamma REAL,
    source TEXT,
    data_quality TEXT
);

CREATE TABLE options_features (
    company_id TEXT,
    as_of_date TEXT,
    iv_atm_30d REAL,
    iv_rank_252 REAL,
    iv_percentile_252 REAL,
    term_structure_30_90 REAL,
    skew_25d REAL,
    put_call_oi REAL,
    put_call_volume REAL,
    earnings_implied_move REAL,
    rv_iv_spread REAL,
    gamma_exposure_proxy REAL,
    max_oi_strike REAL,
    avg_bid_ask_pct REAL,
    liquidity_score REAL,
    PRIMARY KEY(company_id, as_of_date)
);

CREATE TABLE research_scores (
    company_id TEXT,
    as_of_date TEXT,
    evidence_score REAL,
    market_score REAL,
    risk_score REAL,
    total_research_priority REAL,
    bucket TEXT,
    evidence_gate_status TEXT,
    downgrade_reason TEXT,
    upgrade_reason TEXT,
    PRIMARY KEY(company_id, as_of_date)
);

CREATE TABLE refutation_signals (
    signal_id TEXT PRIMARY KEY,
    company_id TEXT,
    thesis TEXT,
    refutation_question TEXT,
    metric_name TEXT,
    threshold_rule TEXT,
    latest_value TEXT,
    status TEXT,
    source_id TEXT,
    severity TEXT,
    updated_at TEXT
);
4.4 CSV / JSONL fields
security_master.csv
company_id,canonical_name,ticker,exchange,country,currency,cik,isin,adr_ticker,local_ticker,module,bfs_depth,dependency_edge,status
source_registry.jsonl
JSON
{
  "source_id": "MU_2026Q1_10Q",
  "company_id": "MU",
  "source_type": "10-Q",
  "fiscal_period": "2026Q1",
  "publication_date": "2026-01-01",
  "url": "...",
  "local_path": "sources/MU/2026Q1_10Q.html",
  "content_hash": "...",
  "source_priority": 1,
  "source_status": "downloaded"
}
evidence_claims.jsonl
JSON
{
  "claim_id": "MU_2026Q1_HBM_REVENUE_001",
  "company_id": "MU",
  "source_id": "MU_2026Q1_CALL",
  "metric": "HBM revenue commentary",
  "quote": "...",
  "source_location": "earnings call, prepared remarks",
  "evidence_status": "原文已证明",
  "inference_notes": "Does not prove long-term margin expansion.",
  "counterevidence_flag": 0
}
price_features.csv
company_id,as_of_date,rs_spy_60,rs_qqq_60,rs_smh_60,beta_spy_60,beta_qqq_60,rv_20,rv_60,rv_120,max_drawdown_252,drawdown_duration,abnormal_volume_20,dollar_volume_20,module_corr_60,earnings_gap_last,post_earnings_drift_20
options_features.csv
company_id,as_of_date,iv_atm_30d,iv_rank_252,iv_percentile_252,term_structure_30_90,skew_25d,put_call_oi,put_call_volume,earnings_implied_move,rv_iv_spread,gamma_exposure_proxy,max_oi_strike,avg_bid_ask_pct,liquidity_score
4.5 Company card 模板
Markdown
# Company Card: [Company / Ticker]

## 1. One-line thesis
如果 [D0 demand] 持续增长，[公司] 可能因为 [dependency edge] 获得 [financial transmission]。

## 2. Chain position
- Module:
- BFS depth:
- Dependency edge:
- Edge type:
- Upstream:
- Downstream:
- Customer evidence:

## 3. Source checklist
| Source | Period | Status | Link | Notes |
|---|---|---|---|---|

## 4. 原文已证明
| Claim | Source | Quote / Location | What it proves | What it does not prove |
|---|---|---|---|---|

## 5. 合理推论
| Inference | Inputs | Assumption | Confidence | Needed verification |
|---|---|---|---|---|

## 6. 待原文核验
| Claim | Current source | Needed primary source | Priority |
|---|---|---|---|

## 7. 主要反证
| Refutation | Metric | Source | Status | Severity |
|---|---|---|---|---|

## 8. Financial transmission
- Revenue:
- Segment revenue:
- Orders / backlog / RPO:
- Gross margin:
- Operating margin:
- CapEx / capacity:
- Inventory / receivables:
- OCF / FCF:
- Debt / lease / interest:
- Customer concentration:

## 9. Market behavior
- Relative strength:
- Rolling beta:
- Realized vol:
- Drawdown:
- Liquidity:
- Earnings gap:
- Correlation with module basket:

## 10. Options risk
- IV rank:
- Term structure:
- Skew:
- Put/call OI:
- Earnings implied move:
- Liquidity:
- Risk label:

## 11. Research priority score
- Evidence score:
- Market score:
- Risk score:
- Bucket:
- Gate status:

## 12. Next quarter questions
1.
2.
3.
4.6 Quarterly update workflow

项目 checklist 已把标准研究流程定义为：写假设、判断基建周期属性、列原始披露来源、确定产业链位置、找客户和订单证据、看毛利率/ASP/backlog/CapEx/FCF、列替代风险和周期风险、评分入池、写下一轮验证问题，并每季度更新。

research-checklist

落地成季度流程：

T-10 to T-3:
    更新 price_features、options_features、历史 earnings gap。
    列出本季度必须核验的问题。

T:
    抓 earnings release / 8-K / quarterly report。
    只抽取原文数字，不做结论扩展。

T+1 to T+3:
    加入 call transcript。
    抽取管理层 commentary、guidance、customer、capacity、backlog、margin。

T+3 to T+7:
    完成 evidence_claims。
    更新 financial_metrics。
    更新 company card。
    更新 scoring。
    更新 refutation dashboard。

T+7 to T+14:
    与客户/供应商交叉披露核验。
    与同模块 basket 对比。
    写 quarterly module note。
4.7 Research upgrade / downgrade rules

这不是买入/卖出升级降级，而是 research priority upgrade / downgrade。

Upgrade research priority

满足任意组合：

1. 原文首次明确 AI/data center/HBM/CPO/AI server/power 相关收入。
2. backlog / RPO / bookings 明确增长，并且可与 AI Infra edge 对应。
3. 毛利率或产品 mix 改善，且管理层解释与 AI/高端产品相关。
4. CapEx/capacity expansion 有客户需求或长约支撑。
5. 客户/供应商交叉披露验证 dependency edge。
6. 财报后 RS 和 post-earnings drift 支持市场开始验证。
7. 反证指标未恶化。
Downgrade research priority

触发任意强反证：

1. 公司只说 AI opportunity，但无收入、订单、客户、产品证据。
2. 收入增长来自消费电子、汽车、普通工业恢复，却被包装成 AI。
3. 毛利率不升反降，说明无议价权或 pass-through 属性强。
4. inventory / receivables / working capital 恶化。
5. backlog 可取消、客户集中且合同短。
6. CapEx 扩张导致 FCF 恶化且无可见订单支撑。
7. 技术路线改变，削弱公司产品价值量。
8. 期权 IV/股价拥挤但 evidence score 低。
4.8 Refutation dashboard

每家公司至少 3 个反证，每个模块至少 5 个反证。

Refutation theme	Leading indicator	Source	Red flag
AI CapEx 放缓	hyperscaler CapEx guide, RPO conversion, cloud margin	earnings / 10-Q	CapEx 下修或 RPO 转化放慢
GPU 供需松动	NVIDIA/AMD/ASIC supply commentary	earnings / product supply chain	交期缩短、价格压力
HBM 过剩	HBM ASP, vendor CapEx, inventory	memory vendor reports	ASP 下行 + 库存上升
CoWoS 瓶颈解除	advanced packaging capacity, substrate lead time	foundry / OSAT / substrate source	扩产快于需求
Optics 价格战	800G/1.6T ASP, datacom margin	optics earnings	出货增但毛利降
Power equipment 订单透支	backlog, book-to-bill, lead time	equipment earnings	backlog 转弱、交期缩短
NeoCloud 信用风险	utilization, debt, lease, interest expense, customer concentration	10-Q / earnings	收入涨但 FCF/interest coverage 恶化
Efficiency shock	tokens/W, tokens/$, model compression	technical sources	推理效率提升快于需求弹性
5. Local engineering pipeline

MVP 优先 Python 标准库：sqlite3, csv, json, pathlib, urllib.request, hashlib, datetime, statistics, math, re, html.parser。不接 IBKR，不自动交易，不下单。

Stage 1：security_master
项目	内容
Input	146 条 AI Infra universe：公司名、ticker、交易所、模块初始标签
Output	security_master.csv, SQLite security_master
Schema	company_id, canonical_name, ticker, exchange, country, currency, cik, isin, adr_ticker, local_ticker, module, bfs_depth, dependency_edge, status
Agent prompt	“请把以下公司标准化为唯一 company_id，识别本地 ticker/ADR/CIK/交易所，给出 AI Infra module、BFS depth 和 dependency edge。无法确认的字段标为 UNKNOWN，不要猜。”
Failure mode	ADR 与本地股混淆；ticker 重名；公司改名；退市；母子公司混淆；同一公司重复
Test plan	company_id 唯一；ticker+exchange 唯一；CIK 格式校验；手工抽查 20 家；所有 UNKNOWN 进入 review queue
Stage 2：source_registry
项目	内容
Input	security_master, source rules
Output	source_registry.jsonl, SQLite source_registry
Schema	source_id, company_id, source_type, fiscal_period, publication_date, url, local_path, content_hash, source_priority, source_status
Agent prompt	“为每家公司列最近年报、季报、earnings release、call transcript、investor deck、产品页、客户/供应商交叉披露。按 source priority 排序，只输出可核验来源。”
Failure mode	抓错公司；旧年报当新年报；IR 页面动态加载；transcript 来源不可核验；PDF 无法解析
Test plan	URL 可访问；hash 去重；source_type 合法；period 合法；每家公司至少 1 个 primary source；source date 不晚于当前日期
Stage 3：financials_extractor
项目	内容
Input	source_registry, downloaded filings / releases / transcripts
Output	financial_metrics.csv, evidence_claims.jsonl
Schema	metric, value, unit, currency, period, segment, GAAP/non-GAAP, quote, source_location
Agent prompt	“只从提供原文中抽取 revenue、segment revenue、orders/backlog/RPO/bookings、gross margin、operating margin、CapEx、inventory、receivables、OCF/FCF、debt、lease、interest、customer concentration、AI/data center/HBM/CoWoS/optics/power/cooling 相关字段。每条必须带 quote 和 source_location。无法证明则标为待原文核验。”
Failure mode	GAAP/non-GAAP 混用；单位 million/billion 错误；segment 口径变化；把管理层愿景当收入；XBRL tag mismatch
Test plan	数字单位校验；总收入与 XBRL/财报一致；每条 claim 必须有 source_id；没有 quote 的 claim reject；随机人工复核 10%
Stage 4：evidence_card_writer
项目	内容
Input	financial_metrics, evidence_claims, security_master
Output	company_cards/[company_id].md, company_cards.jsonl
Schema	thesis, BFS, sources, proven, inference, needs_verification, counterevidence, financial transmission
Agent prompt	“请按公司卡片模板写 evidence card。严格分为原文已证明、合理推论、待原文核验、主要反证。不得把 K线/期权/媒体摘要当基本面证据。”
Failure mode	过度总结；省略反证；把推论写成事实；没有说明原文不能证明什么
Test plan	每张卡必须有 ≥1 primary source；必须有 counterevidence；所有 原文已证明 必须映射 claim_id；禁止出现无 source 的财务数字
Stage 5：price_feature_builder
项目	内容
Input	OHLCV daily CSV, benchmark prices
Output	price_daily, price_features.csv
Schema	RS, beta, RV, drawdown, volume, liquidity, gap, drift, correlation
Agent prompt	不需要 LLM。解释层 prompt：“请基于 deterministic price features 解释市场行为，只能标记 market validation/crowding/risk，不能证明基本面。”
Failure mode	未复权；split/dividend 错误；benchmark 日期不一致；流动性太低；非交易日对齐错误
Test plan	日期交集校验；价格非负；RV 非负；beta 公式单元测试；SPY 对 SPY beta≈1；异常缺失告警
Stage 6：options_feature_builder
项目	内容
Input	option chain snapshots, earnings calendar, underlying price
Output	option_chain_snapshots, options_features.csv
Schema	IV rank, IV percentile, term structure, skew, put/call OI, implied move, RV/IV, GEX proxy, spread
Agent prompt	“请把期权链解释为风险温度计、事件定价或拥挤线索。不得把 option flow 解释为基本面证据。”
Failure mode	期权链陈旧；宽价差；缺 OI；IV 异常；adjusted options；低流动性小盘误判
Test plan	bid <= ask；mid > 0；IV > 0；spread 过宽标记 low_quality；expiry 合法；OI/volume 缺失不插值成事实
Stage 7：risk_model_builder
项目	内容
Input	evidence score, price features, options features
Output	research_scores, risk card, module correlation matrix
Schema	evidence_score, market_score, risk_score, total_research_priority, bucket, gate_status
Agent prompt	“根据三层系统生成 research priority score。Layer 2/3 不能覆盖 Layer 1 gate。输出 upgrade/downgrade reason 和主要反证。”
Failure mode	市场强势覆盖证据不足；过拟合；相关性矩阵不稳定；期权数据缺失当作低风险
Test plan	gate rules 单元测试；无 primary source 的公司不能进 core；缺期权数据标记 NO_OPTIONS_DATA；相关性窗口最小样本数校验
Stage 8：portfolio_research_dashboard
项目	内容
Input	research_scores, company_cards, price_features, options_features
Output	static HTML / Markdown / CSV dashboard
Schema	module, bucket, evidence score, market score, risk flags, next questions
Agent prompt	“生成研究 dashboard，只输出研究优先级、风险标签、证据缺口和下一步核验问题。不得输出买卖建议、目标价或实际仓位。”
Failure mode	dashboard 变成行情榜；忽略 source status；把高分解释为买入
Test plan	页面不得出现 buy/sell/target price/position；每个 high priority name 必须有 source_id 和 refutation_id
Stage 9：refutation_dashboard
项目	内容
Input	company cards, module thesis, evidence claims, market features
Output	refutation_signals.csv, refutation dashboard
Schema	thesis, refutation_question, metric, threshold_rule, latest_value, status, severity, source
Agent prompt	“为每家公司和模块写可证伪反证。优先寻找会推翻 thesis 的数据，而不是支持 thesis 的数据。”
Failure mode	只写支持证据；反证不可测；阈值含糊；红旗没有 source
Test plan	每家公司 ≥3 个反证；每个核心模块 ≥5 个反证；每个反证必须有 metric 或 source；每季更新状态
6. Practical MVP：146 条 universe 的 2 周计划
数据源优先级
财报 / 原文

第一批：

US-listed:
    SEC EDGAR submissions API
    SEC companyfacts XBRL
    company IR earnings release
    company IR annual/quarterly report
    investor presentation

第二批：

Non-US:
    company IR annual report
    exchange filings
    local regulator filings
    earnings presentation
    product / technology pages

SEC 是最适合 Python 标准库 MVP 的数据源，因为它直接提供 JSON API、无需 API key，并覆盖 submissions 和财务 XBRL 数据。
美国证券交易委员会
+1

K线

第一批：

Stooq daily OHLCV
Stooq benchmark: SPY, QQQ, SMH/SOXX if available, major indices

Stooq 免费历史数据支持 daily/hourly/5-min 文件，并覆盖美国、日本、香港、英国等市场，适合先跑 146 universe 的 K线特征。
Stooq

期权

第一批：

Cboe historical options volume / put-call ratios
Cboe available weekly options list
Nasdaq option chain page as lightweight reference
Manual CSV snapshot for top sample names

期权数据免费源不如财报和 K线稳定。MVP 不要承诺完整 options analytics；先对 US optionable、liquid names 做风险温度计。Cboe 历史页面支持按 symbol/product/month/year 下载历史 options volume，并提供 put/call ratio 数据但有准确性免责；Nasdaq 说明提供 call/put options information，但页面可用性不稳定。
Cboe Global Markets
+1

样板公司选择

样板不是推荐，只是为了覆盖模块和测试 pipeline。

优先选 10-12 家：

公司类型	样板	原因
HBM / memory vendor	MU	US-listed、SEC、期权、HBM/DRAM/eSSD 字段可测
Advanced packaging / foundry	TSM	20-F/IR、CoWoS/advanced packaging 交叉验证
Custom ASIC / networking	AVGO	ASIC、networking、CPO、SEC、期权
Optics / laser	COHR	datacom/AI optics、SEC、期权
AI server / systems	SMCI 或 DELL	AI server、inventory、margin、customer risk
Power / cooling	VRT	data center power/cooling、backlog、orders、SEC、期权
EDA / IP	CDNS 或 SNPS	RPO、EDA demand、AI chip design exposure
Connectivity / PCIe/CXL	ALAB	AI connectivity、high market attention、SEC、期权
Storage / eSSD	PSTG 或 WDC/SNDK	enterprise storage / NAND / eSSD 区分
NeoCloud	NBIS 或 CRWV	utilization、backlog、debt、lease、customer concentration
Non-US HBM equipment/test	Advantest 或 TOWA	测试 non-US source workflow，无完整 US options 也能测试 NO_OPTIONS_DATA
Advanced packaging / OSAT	AMKR	advanced packaging、customer/CapEx/margin 传导

选择逻辑：

1. 至少 6 家 US-listed，有 SEC + K线 + 期权。
2. 至少 2 家 non-US，测试 annual report / IR / local exchange。
3. 每个核心模块至少 1 家。
4. 至少 1 家无期权或低流动性，测试 pipeline 的缺失数据分支。
Day 1-3：建地基

目标：让 146 条 universe 进入统一数据库。

交付：

1. security_master.csv
2. module taxonomy
3. BFS depth / dependency edge 初版
4. source_registry.jsonl 初版
5. SQLite schema
6. price source downloader MVP
7. SEC companyfacts / submissions downloader MVP
8. evidence status enum

具体任务：

Day 1:
    - 清洗 146 universe。
    - 标准化 company_id, ticker, exchange, country, currency。
    - 标记 module 和 BFS depth。
    - 选 10-12 家样板公司。

Day 2:
    - 写 SQLite schema。
    - 接 SEC submissions/companyfacts。
    - 写 source_registry downloader。
    - 建 source hash 和 local archive。

Day 3:
    - 接 Stooq OHLCV。
    - 建 benchmark list。
    - 计算第一版 RS、beta、RV、drawdown、volume。
    - 写 deterministic unit tests。

成功标准：

146 家全部有 company_id。
样板公司至少 8 家有 source_registry。
146 家中至少 100 家有可用 K线或明确 NO_PRICE_DATA。
US-listed 样板公司能抓到 SEC submissions。
Day 4-7：做第一批公司卡和市场特征

目标：10-12 家样板公司跑通完整 evidence → market → risk。

交付：

1. financial_metrics.csv for sample names
2. evidence_claims.jsonl for sample names
3. company_card.md for sample names
4. price_features.csv for all available names
5. options_features.csv for liquid US sample names
6. first research_scores.csv

具体任务：

Day 4:
    - 样板公司最近 annual/quarterly/earnings release 抽取 revenue、margin、CapEx、cash/debt。
    - 每个 claim 强制 source_id。

Day 5:
    - 抽取 AI-specific fields：AI/data center/HBM/CoWoS/optics/power/cooling/guidance/roadmap。
    - 写原文已证明 / 合理推论 / 待核验 / 反证。

Day 6:
    - 计算 earnings gap、post-earnings drift、module correlation。
    - 接 Cboe / Nasdaq / manual options snapshot。
    - 计算 IV rank、term structure、skew、put/call、implied move 的可用子集。

Day 7:
    - 生成 company cards。
    - 生成 research score。
    - 生成 refutation dashboard v0。

成功标准：

每张 company card 至少 5 条 evidence claims。
每张 company card 至少 3 条 counterevidence。
所有财务数字都有 source_id。
K线/期权只进入 market/risk section，不进入 evidence section。
Day 8-14：扩展到 146 universe 的 MVP dashboard

目标：把系统从样板扩展成研究 OS 初版。

交付：

1. 146-company security dashboard
2. price feature dashboard for all available names
3. source coverage dashboard
4. evidence coverage dashboard
5. options risk dashboard for optionable/liquid US names
6. refutation dashboard
7. quarterly update runbook
8. data quality report

具体任务：

Day 8-9:
    - 为 146 家生成 source coverage。
    - 标记 SOURCE_OK / SOURCE_MISSING / NON_US_MANUAL / NEEDS_REVIEW。
    - 生成 source gaps。

Day 10-11:
    - 全量跑 K线 features。
    - 模块 basket correlation。
    - 流动性和 drawdown risk labels。

Day 12:
    - 全量跑 scoring gates。
    - 无 primary source 的公司自动限制 bucket。
    - 生成 high-priority verification list。

Day 13:
    - 完成 refutation dashboard。
    - 每个模块写 5 个反证。
    - 每个样板公司写 next quarter questions。

Day 14:
    - QA：随机复核 evidence claims。
    - 输出 MVP README。
    - 输出 next 30-day roadmap。

成功标准：

1. 146 家公司全部进入 security_master。
2. 至少 10 家样板公司有完整 company card。
3. 至少 100 家公司有 price_features 或明确 NO_PRICE_DATA。
4. US optionable 样板公司有 options_features 或明确 NO_OPTIONS_DATA。
5. 所有“原文已证明”结论都有 source_id。
6. 所有“合理推论”都有 input evidence 和 assumption。
7. 所有高分公司都有反证。
8. dashboard 不输出买入、卖出、目标价或实际仓位。
7. 最终 MVP 文件结构
ai_infra_research_os/
  data/
    universe_146.csv
    security_master.csv
    source_registry.jsonl
    financial_metrics.csv
    evidence_claims.jsonl
    price_daily/
    price_features.csv
    option_chain_snapshots/
    options_features.csv
    research_scores.csv
    refutation_signals.csv

  sources/
    SEC/
    IR/
    transcripts/
    product_pages/

  cards/
    company/
    module/
    quarterly/

  dashboards/
    index.html
    source_coverage.html
    evidence_dashboard.html
    market_dashboard.html
    options_risk_dashboard.html
    refutation_dashboard.html

  scripts/
    01_security_master.py
    02_source_registry.py
    03_financials_extractor.py
    04_evidence_card_writer.py
    05_price_feature_builder.py
    06_options_feature_builder.py
    07_risk_model_builder.py
    08_dashboard_builder.py
    09_refutation_dashboard.py

  tests/
    test_security_master.py
    test_source_registry.py
    test_financial_metrics.py
    test_price_features.py
    test_options_features.py
    test_scoring_gates.py

  README.md
  RUNBOOK.md
8. 最重要的系统规则
1. Evidence first.
2. Market second.
3. Options third.
4. No primary source, no core conclusion.
5. No AI revenue/order/customer/product evidence, no AI Infra proof.
6. K线只能说明 pricing / crowding / risk。
7. 期权只能说明 volatility / event pricing / liquidity / crowding clue。
8. 所有结论必须分为：原文已证明、合理推论、待原文核验、主要反证。
9. 所有评分都是 research priority，不是投资建议。
10. 每季度必须更新 evidence、market、options、refutation 四张表。

这个系统的目标不是预测短期涨跌，而是把 146 条 AI Infra universe 变成一个可持续迭代的、source-backed、可证伪的研究引擎。
