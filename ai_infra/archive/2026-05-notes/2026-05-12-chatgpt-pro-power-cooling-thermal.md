# ChatGPT Pro 输出草稿：电力设备 / 液冷 / 热管理

状态：ChatGPT Pro 输出草稿，待原始出处逐条核验  
捕获时间：2026-05-12 12:18:45 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a02a507-df18-83ea-a320-7258c29ac874  
项目：ai super cycle

## 使用边界

- 本文件是 ChatGPT Pro 对项目研究任务的输出草稿。
- 不作为投资建议、买卖建议或已验证结论。
- 所有涉及财报数字、订单、backlog、CapEx、产能、价格、客户关系、技术路线的内容，必须回到公司原始披露或权威原文核验后才能进入正式研究结论。
- 本文件保留模型输出中的可疑点，后续核验时应优先处理。

## 原始提问

```text
主题7：AI 数据中心电力设备 / 液冷 / 热管理研究。

请基于本项目已上传的 md 来源文件，研究 AI 数据中心瓶颈是否会从 GPU 转到电力设备、变压器、UPS、PDU、switchgear、liquid cooling、CDU、cold plate、power module。

严格要求：
1. 先列出需要核验的公司原始出处，不要直接把媒体/模型记忆当结论；
2. 区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”；
3. 不给买卖建议，不给目标价；
4. 不要把泛能源叙事等同于 AI 数据中心受益，必须拆 grid interconnect、substation、transformer、UPS/PDU、rack power、liquid cooling、thermal components。

重点公司：
Vertiv、Schneider、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta、Lite-On、AcBel、Vicor、MPS、Infineon、Fuji Electric、Mitsubishi Electric、Nidec、Munters、Alfa Laval、Modine、CoolIT、Boyd。

输出：原始出处清单、从 grid interconnect 到 rack power/cooling 的产业链地图、backlog/book-to-bill/lead time/gross margin 的验证方式、哪些环节是真瓶颈、哪些是主题映射、反证指标。
展开
```

## ChatGPT Pro 输出

基于 2026-05-12 项目 md，我把这次结论先定义为研究地图 + 待核验框架，不是公司事实结论。项目文件本身明确要求：所有收入、订单、backlog、CapEx、毛利率、产能、价格、出货、客户关系等数据，都只能先作为“待原文核验线索”，正式结论必须回到公司原始披露或权威原文；媒体、券商、数据库和模型输出只能作为线索。

research-checklist

 项目研究地图也把 AI Infra 定义成从 token 需求到算力、HBM、网络、数据中心、电力/能源/电网的多层约束系统，而不是单一 GPU 线性链条。

2026-05-12-ai-super-cycle-resea…

1. 先列需要核验的公司原始出处
1.1 需求源头和交叉验证原文

这些不是本主题公司，但必须先用来验证“AI 数据中心建设需求是否真实存在并继续扩张”，否则电力/液冷受益会变成泛叙事。

核验对象	必查原始出处	要证明什么	不能直接证明什么
Microsoft / Azure	最新 annual report、quarterly earnings、earnings call、investor presentation、CapEx / RPO / cloud margin 披露	AI infrastructure 是否继续拉动 CapEx、RPO、云收入和折旧压力	不能直接证明 Vertiv、Eaton、Schneider 等拿到订单
Alphabet / Google	最新 10-K/10-Q、earnings release/call、technical infrastructure CapEx 披露	AI technical infrastructure 是否持续扩张	不能直接推导到某个电力设备供应商
Meta	最新 10-K/10-Q、CapEx guidance、earnings call	AI data center / infrastructure spending 是否继续上修或维持高位	不能证明液冷渗透率
AWS / Amazon	10-K/10-Q、earnings call、AWS CapEx / capacity commentary	云端 AI 算力需求和数据中心容量约束	不能证明具体 UPS/PDU/变压器订单
NVIDIA / GB200 / GB300 / rack-scale systems	官方 product page、architecture material、investor presentation	AI rack 功率密度、液冷 rack、整机供电架构变化	不能单独证明某供应商收入增长

项目待核验清单已经把 Microsoft、Alphabet、Meta、AWS、NVIDIA、IEA/DOE data center power 等列为需要回原文验证的上游线索。

2026-05-12-ai-super-cycle-resea…

1.2 电力设备 / 液冷 / 热管理公司原始出处清单
环节	公司	必查原始出处	核心核验点
数据中心电源与热管理集成	Vertiv	10-K / annual report、quarterly earnings release、earnings call、investor presentation、product specs：UPS、PDU、switchgear、busway、thermal、CDU、liquid cooling	organic orders、backlog、book-to-bill、data center / hyperscaler revenue、liquid cooling attach、gross margin、working capital、lead time
电气化 / Secure Power / 数据中心配电	Schneider Electric	Universal Registration Document、quarterly sales / orders release、earnings call、Capital Markets Day、Secure Power / Energy Management product docs	data center order growth、Secure Power / Energy Management mix、backlog、margin、software/energy management 与硬件贡献拆分
UPS / switchgear / electrical distribution	Eaton	10-K、10-Q、quarterly earnings release/call、Electrical Americas / Global segment materials、investor day	data center orders、electrical backlog、book-to-bill、large project timing、margin mix、lead time
grid / substation / transformers	Siemens Energy	annual report、quarterly report、Grid Technologies segment disclosure、earnings call、transformer / switchgear product material	grid technology backlog、orders、transformer capacity/lead time、AI data center exposure是否可见
grid / electrification / transformers	ABB / Hitachi Energy	ABB annual report、quarterly results、Electrification segment disclosure；Hitachi / Hitachi Energy annual or integrated report、company technical docs	transformers、switchgear、substation orders、data center customer exposure、backlog quality
server / rack power, power shelf, cooling	Delta Electronics	annual report、quarterly investor presentation、monthly revenue、data center power/cooling product specs	AI server PSU、rack power、48V、cooling/CDU exposure、gross margin、customer concentration
server power	Lite-On	annual report、quarterly earnings / investor deck、monthly sales、cloud / server power product docs	data center PSU revenue mix、AI server exposure、margin uplift vs consumer/PC power
server / rack power	AcBel	annual report、quarterly investor deck、monthly revenue、server power product specs	AI server PSU / power shelf revenue、gross margin、working capital、customer concentration
high-density power modules	Vicor	10-K、10-Q、earnings call、power module product docs、customer / design-win commentary	48V / high-current power module design wins、AI rack relevance、customer concentration、inventory and margin
point-of-load / power modules	Monolithic Power Systems / MPS	10-K、10-Q、earnings call、investor presentation、data center power product docs	data center / AI server power revenue、module ASP、margin, attach rate, design wins
power semis：SiC/GaN/MOSFET/PMIC	Infineon	annual report、quarterly report、segment disclosure、SiC/GaN roadmap、server/data center power product docs	data center power semi exposure、SiC/GaN adoption、industrial vs AI data center mix
UPS / power electronics / semis	Fuji Electric	annual report、integrated report、quarterly results、power electronics / semiconductor / UPS product docs	data center UPS or power electronics exposure、orders、margin、AI specificity
UPS / power electronics / motors / cooling	Mitsubishi Electric	annual report、integrated report、quarterly results、FA / power systems / semiconductor / data center product docs	data center UPS / power systems / cooling exposure；AI vs industrial cycle
fans / motors / pumps / thermal motion components	Nidec	annual report、quarterly results、segment disclosure、fan/pump/motor product docs	data center cooling component exposure、AI rack liquid/air cooling relevance、margin
data center cooling / air handling	Munters	annual report、quarterly report、Data Center Technologies disclosure、orders/backlog, product specs	hyperscaler/data center orders、book-to-bill、gross margin、liquid/air cooling mix
heat exchangers / thermal transfer	Alfa Laval	annual report、quarterly report、data center / heat exchanger product docs、order commentary	data center heat exchanger orders、liquid cooling / heat rejection exposure、margin
thermal management / data center cooling	Modine	10-K、10-Q、earnings call、Climate Solutions / data center product docs	data center cooling orders、backlog、revenue conversion、gross margin
direct liquid cooling private supplier	CoolIT	company product specs、press releases、customer/OEM cross-disclosures、certifications、supply partnerships	CDU、cold plate、manifold qualification；因为私有公司财务披露有限，需靠客户交叉披露
cold plate / thermal components / private supplier	Boyd	company product specs、press releases、customer/OEM cross-disclosures、technical white papers	cold plate、TIM、thermal assembly、liquid cooling customer qualification；财务数据大概率需交叉验证

项目文档已经把本主题公司池和财报指标列为：Vertiv、Schneider、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta、Lite-On、AcBel、Vicor、MPS、Infineon、Fuji Electric、Mitsubishi Electric、Nidec、Munters、Alfa Laval、Modine；要看 backlog、book-to-bill、data center revenue、gross margin、order lead time、working capital，以及 rack kW、liquid cooling attach、transformer lead time、UPS efficiency、SiC/GaN adoption。

2026-05-12-ai-super-cycle-resea…

2. 从 grid interconnect 到 rack power / cooling 的产业链地图

项目文件明确把“数据中心 / 机电 / 冷却 → 电力 / 能源 / 电网”放在 AI Infra 物理瓶颈层，并指出数据中心从“买到 GPU”进入“拿到电、散掉热、点亮 rack、形成可用集群”的阶段。

2026-05-12-ai-super-cycle-resea…

层级	子环节	关键约束	代表公司 / 相关公司	AI 相关性判断
0. 需求源头	token 需求、frontier training、inference、agent workload	token 增长是否超过单位推理成本下降	OpenAI、Anthropic、Google、Meta、xAI、Microsoft、AWS 等，作为需求验证源	不是本主题供应商，但决定后续 CapEx 是否真实
1. 数据中心开发	land bank、permits、fiber、powered shell、colo、pre-lease	time-to-power、MW leased、MW under construction、permit / water / noise	Equinix、Digital Realty、Vantage、QTS、CoreWeave、Oracle、IREN 等用于交叉验证	证明需求是否进入“可建设项目”
2. grid interconnect	utility interconnection、transmission capacity、grid queue	并网排队、utility service agreement、substation readiness	utility、data center developers、Siemens Energy、Hitachi Energy、ABB 等	真物理瓶颈，但不等于所有电力设备公司直接受益
3. substation / high-voltage equipment	transformers、breakers、protection、HV/MV switchgear、power quality	transformer lead time、substation EPC、project backlog	Siemens Energy、ABB/Hitachi Energy、Schneider、Eaton、Mitsubishi Electric、Fuji Electric	可能是 S2 直接瓶颈，AI 归因需用数据中心订单验证
4. facility power chain	MV/LV switchgear、UPS、PDU、busway、STS、backup power interface	order backlog、book-to-bill、lead time、mission-critical qualification	Vertiv、Schneider、Eaton、ABB、Delta、Fuji Electric、Mitsubishi Electric	和 AI 数据中心更近；若订单/毛利/交期同步改善，是强瓶颈候选
5. rack power	power shelf、48V architecture、VRM、POL、BBU、power module、SiC/GaN	rack kW、conversion efficiency、density、thermal derating、design-in cycle	Delta、Lite-On、AcBel、Vicor、MPS、Infineon	与 AI rack 功率密度直接相关，是比泛电网更“AI-specific”的环节
6. direct liquid cooling	CDU、cold plate、manifold、quick disconnect、pump、valve、coolant loop、TIM	liquid cooling attach rate、CDU shipment、cold plate qualification、leak/reliability	Vertiv、CoolIT、Boyd、Modine、Delta、Munters、Alfa Laval	对高功率 AI rack 最直接，可能从 S3 配套变成局部 S2 瓶颈
7. facility heat rejection	chiller、dry cooler、heat exchanger、cooling tower、fan、air handler	heat rejection capacity、PUE/WUE、water constraint、ambient conditions	Munters、Alfa Laval、Modine、Nidec、Mitsubishi Electric	真需求存在，但更容易混入普通 HVAC / industrial cycle
8. power semiconductors / components	SiC/GaN、MOSFET、driver、controller、magnetics、capacitors	efficiency、density、thermal envelope、qualification	Infineon、MPS、Vicor、Fuji Electric、Mitsubishi Electric	需要证明进入 AI server / rack power BOM，否则只是泛功率半导体映射

项目文件对 3.14“电力设备、冷却、热管理”的拆分已经覆盖 UPS、PDU、switchgear、transformer、busway、power module、VRM、SiC/GaN、liquid cooling、CDU、cold plate、immersion、chiller、pump、fan、heat exchanger，并列出 backlog、book-to-bill、transformer lead time、liquid cooling attach rate、rack kW、gross margin、power conversion efficiency、customer concentration 作为关键指标。

2026-05-12-ai-super-cycle-resea…

 另一个模块图把机柜级电源定义为 48V、VRM、power module、BBU，把液冷/热管理定义为 cold plate、CDU、泵阀、TIM、浸没液，并分别给出 kW/rack、转换效率、订单、液冷渗透率、CDU 出货等验证指标。

2026-05-12-chatgpt-pro-module-m…

3. 初步判断：瓶颈会不会从 GPU 转到电力 / 变压器 / UPS / 液冷？

结论分层：

原文已证明

项目框架已把 AI Infra 定义为多层约束系统，电力/能源/电网、数据中心/机电/冷却位于 GPU、HBM、网络之后的物理交付层。

2026-05-12-ai-super-cycle-resea…

项目文件已把“电力设备、液冷、热管理、变压器”列为第一优先级方向之一，理由是 AI 数据中心从买 GPU 变成拿到电、把热带走、把 rack 点亮，且该链条供给扩张慢，很多公司原本不是科技叙事中心。

2026-05-12-ai-super-cycle-resea…

项目文件已明确要求，不能把实际数据直接当结论；所有收入、订单、backlog、CapEx、毛利率、客户关系等必须回公司原文核验。

research-checklist

合理推论

AI 数据中心瓶颈不是简单从 GPU 完全转移到电力设备，而是从“单一 GPU 供给约束”扩散成“GPU + HBM + 网络 + 数据中心机电 + 电力接入 + 冷却”的多瓶颈系统。真正的变化是：当 GPU 供应、HBM、CoWoS、网络逐步改善后，time-to-power、substation/transformer、UPS/PDU/switchgear、rack power、liquid cooling 可能成为决定集群上线速度的约束。

更细一点：

grid interconnect / utility queue 是最硬的物理约束之一，但它未必直接让设备商马上确认收入；它也可能导致数据中心项目延后，从而推迟 UPS/PDU/CDU 收入。

transformer / substation / switchgear 可能是真瓶颈，因为交付周期、认证、工程容量和制造扩产慢，但 AI 归因需要证明订单来自数据中心而非普通 grid upgrade。

UPS / PDU / busway / facility power 是数据中心建设必需品，若 backlog、book-to-bill、lead time 和 gross margin 同时改善，才可以从“配套扩张”上升为“瓶颈租金”。

rack power / 48V / VRM / power module / BBU 更接近 AI rack 本身，AI 相关性比泛电网设备更强，但必须验证是否进入 GB200/GB300 或同类高功率 AI server BOM。

liquid cooling / CDU / cold plate 对高功率 rack 最直接，可能是本主题中最“AI-specific”的热管理环节；但需要警惕标准化后 ASP 下行和竞争加剧。

heat exchanger / chiller / fan / pump / HVAC 需求真实，但更容易混入普通数据中心、工业 HVAC、楼宇改造周期，必须拆出 data center revenue 和 AI liquid-cooled rack exposure。

待原文核验

以下不能直接写成结论：

Vertiv、Schneider、Eaton 的 backlog 是否主要来自 AI data center，而不是普通数据中心、工业电气化或渠道补库存。

Siemens Energy、ABB/Hitachi Energy 的 transformer / grid backlog 是否有可识别的数据中心订单。

Delta、Lite-On、AcBel 的 AI server PSU / power shelf revenue 占比、毛利率和客户结构。

Vicor、MPS、Infineon 的 power module / SiC/GaN 是否有明确 AI rack design win，而不是泛 server / automotive / industrial。

Munters、Alfa Laval、Modine、Nidec 的增长是否来自 data center cooling，还是普通 HVAC / industrial recovery。

CoolIT、Boyd 因为私有属性，必须用 OEM、hyperscaler、server vendor、certification、product qualification 交叉披露验证。

主要反证

项目 checklist 要求每个主题都写反证，尤其要问：客户 CapEx 放缓是否最先砍掉该环节；电力并网卡住时哪些设备受益、哪些订单延迟；客户换供应商时护城河是否存在；毛利率不提升是否推翻瓶颈假设。

research-checklist

本主题最重要的反证是：

订单提前透支：2024-2026 的抢单让 backlog 高企，但后续 book-to-bill 回落到 1 以下。

电力接入而非设备成为真正瓶颈：utility queue / permit 卡住，导致设备订单延期，而非设备商持续受益。

毛利率不提升：收入增长但 gross margin 不升，说明只是工程交付/集成量增长，没有瓶颈租金。

液冷标准化和价格战：CDU、cold plate、manifold 标准化后竞争者增多，ASP 下行。

风冷延寿：更高效 GPU、低功耗推理、rear-door heat exchanger、高端风冷方案延长风冷生命周期。

AI CapEx 放缓：hyperscaler 或 NeoCloud 削减数据中心建设，power/cooling 订单取消或延后。

泛能源误判：核能、天然气、PPA、SMR、电网投资不等于某个数据中心设备商收入增长。项目文件也明确把“泛能源 / SMR / 核能叙事”放在更低优先级，强调没有 PPA、并网、许可、客户合同的主题容易只是股价叙事。

2026-05-12-ai-super-cycle-resea…

4. backlog / book-to-bill / lead time / gross margin 的验证方法
指标	怎么验证	强证据	弱证据 / 红旗
backlog	年报、季报、earnings release、earnings call；拆 segment、region、end-market	data center / hyperscaler backlog 明确增长，并披露 conversion schedule	只说 total backlog 创新高，但不拆数据中心
book-to-bill	orders ÷ revenue，最好按 segment 和 quarter 连续跟踪	连续多个季度 >1，且不是单一大单	单季度大单导致 book-to-bill 跳升，后续回落
lead time	公司 call、investor deck、客户交叉披露、产品交付周期	transformer / switchgear / UPS / CDU 交期明确拉长，且客户愿意提前锁单	媒体说“短缺”，公司原文不确认
gross margin	GAAP / non-GAAP gross margin、segment margin、price-cost commentary	revenue 增长 + gross margin 上行 + mix 改善	revenue 增长但 margin 持平/下降，说明低附加值交付
data center revenue	segment revenue、end-market revenue、customer commentary	data center / cloud / hyperscaler revenue 单独披露	只说“AI opportunity”，没有收入/订单
working capital	inventory、contract assets、payables、advance payments、FCF	backlog 增长同时 cash conversion 健康	inventory 快速上升、FCF 恶化、项目垫资重
CapEx / capacity	company capex plan、factory expansion、transformer/CDU capacity	扩产慢且有客户锁单/预付款	扩产容易，1-2 年后供给过剩
customer concentration	10-K/annual report、major customer disclosure、risk factors	多个 hyperscaler / colocation / OEM 客户	单一客户拉动，取消风险高

对瓶颈供应商，项目 checklist 明确要求看 ASP、毛利率、长约、客户认证和扩产约束；对重资产建设者，要看 CapEx 回收周期、利用率、融资成本和 FCF 拐点。

research-checklist

5. 哪些环节更像真瓶颈，哪些更像主题映射
5.1 更接近“真瓶颈”的环节
环节	初步瓶颈等级	原因	必须核验的原文信号
grid interconnect / utility queue	A 级物理瓶颈，但不一定是设备商利润瓶颈	决定数据中心能否通电；time-to-power 约束强	utility queue、interconnection agreement、MW energized、permit timeline
large transformer / substation / HV-MV switchgear	A/B	制造周期长、工程认证高、供给扩张慢	transformer lead time、Grid / Electrification backlog、data center project share
mission-critical UPS / PDU / busway / LV switchgear	B+	数据中心必需，客户认证和可靠性要求高	data center orders、backlog conversion、book-to-bill、gross margin
48V rack power / power shelf / VRM / BBU	B+/A-	与 AI rack 功率密度直接绑定，设计周期和效率要求高	rack kW、design win、AI server PSU revenue、power conversion efficiency
CDU / cold plate / liquid loop	B+/A-	高功率 AI rack 直接需求，液冷 attach 上升时弹性高	liquid cooling attach rate、CDU shipment、cold plate qualification、ASP / margin
power semis for AI rack	B	若进入 AI power BOM，价值量随功率密度上升	SiC/GaN / power module design wins、data center revenue mix
5.2 更像“主题映射”的环节
环节	为什么可能是映射	需要什么证据才能升级
泛能源 / 核能 / SMR / 天然气	AI 需要电，但能源项目周期长、监管重，和数据中心设备收入之间有多层传导	PPA、并网许可、客户合同、项目 COD、contracted MW
普通 HVAC / building cooling	数据中心只是 HVAC 的一个终端，AI 相关收入可能很小	data center cooling revenue、hyperscaler orders、liquid cooling exposure
普通 industrial power equipment	工业电气化、grid upgrade、renewables backlog 可能被误认为 AI	data center end-market order mix、customer identity、project references
普通 power semiconductors	汽车、工业、消费电子周期可能掩盖 AI server power	server / data center product line revenue、design win
私有液冷公司概念	技术相关性强，但财务透明度低	OEM qualification、customer shipment、repeat orders、gross margin proxy
6. 公司分层：当前应放在什么研究桶里
公司	产业链位置	初步分类	核心验证问题
Vertiv	UPS、PDU、thermal、liquid cooling、data center infrastructure	核心研究池候选	data center backlog、organic orders、liquid cooling mix、gross margin 是否同步上行
Schneider Electric	electrical distribution、Secure Power、energy management	核心/重点观察	Secure Power / data center orders 是否明确，AI vs 普通电气化如何拆
Eaton	switchgear、UPS、electrical distribution	核心/重点观察	Electrical backlog 和 data center exposure 是否同步提升，margin 是否证明议价权
Siemens Energy	grid technologies、transformer、substation	真物理瓶颈候选，但 AI 归因间接	Grid Technologies backlog 中数据中心项目占比、transformer lead time
ABB / Hitachi Energy	transformers、switchgear、electrification	真物理瓶颈候选，但 AI 归因间接	Electrification / Hitachi Energy orders 是否可追踪到 data center
Delta	server/rack power、thermal、cooling	AI-specific 重点观察	AI server PSU / power shelf / cooling revenue 和毛利率
Lite-On	server PSU / cloud power	重点观察	data center power revenue、AI server customer、margin
AcBel	server power	重点观察	AI server PSU 出货、客户集中、gross margin
Vicor	high-density power modules	高弹性但需强验证	48V / AI rack design win、客户集中、库存、毛利
MPS	power modules / POL	高质量观察	data center power revenue、AI accelerator / server design win、margin
Infineon	SiC/GaN/MOSFET/power semis	主题到核心之间	data center power semi exposure 是否足够大，还是汽车/工业主导
Fuji Electric	UPS、power electronics、semis	主题跟踪	data center revenue 和订单是否明确
Mitsubishi Electric	UPS/power systems/semis/cooling components	主题跟踪	AI data center exposure 是否可拆
Nidec	fans、motors、pumps	主题跟踪 / 组件观察	data center cooling component revenue、liquid cooling pump/fan exposure
Munters	data center cooling / air handling	重点观察	data center order backlog、book-to-bill、margin、液冷/风冷 mix
Alfa Laval	heat exchanger / thermal transfer	重点观察	data center heat exchanger orders、liquid cooling heat rejection exposure
Modine	data center thermal management	重点观察	data center cooling backlog、revenue conversion、gross margin
CoolIT	CDU / cold plate / liquid cooling	产品相关性高，财务透明度低	OEM qualification、hyperscaler or server vendor cross-disclosure
Boyd	cold plate / thermal components	产品相关性高，财务透明度低	customer qualification、repeat shipment、thermal BOM content
7. 未来 4 个季度反证仪表盘
维度	领先指标	反证阈值 / 负面信号
hyperscaler demand	CapEx guidance、AI infra commentary、RPO conversion、cloud gross margin	CapEx 下修、RPO conversion 放缓、AI infra 折旧压缩云毛利
data center project	MW leased、MW under construction、MW energized、pre-lease ratio、time-to-power	leased MW 增长但 energized MW 延迟，说明电力接入卡住
grid / transformer	transformer lead time、substation backlog、utility queue	lead time 开始明显缩短且价格/毛利不再改善
UPS / PDU / switchgear	book-to-bill、orders、segment backlog、gross margin	book-to-bill < 1、backlog conversion 放缓、margin 下行
rack power	rack kW、48V attach、power shelf shipment、server PSU ASP	AI server 出货强但 PSU/power module margin 不升
liquid cooling	liquid-cooled rack attach rate、CDU shipment、cold plate ASP	风冷延寿、液冷 attach 低于预期、ASP 快速下行
component suppliers	design wins、customer count、inventory、gross margin	单一客户取消、库存积压、design win 不转量产
cash flow	FCF、inventory、contract assets、advance payments	收入增长但 FCF 恶化，说明项目垫资或库存风险
technology substitution	tokens/W、推理成本、GPU utilization、ASIC adoption	推理效率大幅提升但总 token 需求没有补上，硬件需求放缓
8. 最终研究判断

当前项目 md 支持的判断是：AI 数据中心瓶颈正在从“只看 GPU”扩散到“电力接入 + 数据中心机电 + rack power + 液冷热管理”的多层物理瓶颈。 但这不是“所有电力、能源、冷却公司都受益”的结论。项目文件本身要求拆开 grid interconnect、substation、transformer、UPS/PDU、rack power、liquid cooling、thermal components，并用 backlog、book-to-bill、lead time、gross margin、data center revenue、working capital 去验证。

2026-05-12-ai-super-cycle-resea…

最优先深挖的顺序：

grid interconnect / transformer / substation switchgear：硬瓶颈，但 AI 归因要最谨慎。

UPS / PDU / busway / switchgear：数据中心直接设备，适合用 backlog + margin 验证。

rack power：48V / VRM / power module / BBU：比泛电网更 AI-specific。

liquid cooling：CDU / cold plate / manifold / pump / TIM：对高功率 AI rack 最直接。

heat exchanger / chiller / fan / HVAC components：真实需求存在，但最容易被普通 HVAC 周期污染。

泛能源 / 核能 / SMR / 天然气 / 储能：必须有 PPA、并网、许可、客户合同，否则只放主题跟踪，不进入核心判断。
