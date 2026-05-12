# ChatGPT Pro 输出草稿：非美材料 / 设备隐形冠军

状态：ChatGPT Pro 输出草稿，待原始出处逐条核验  
捕获时间：2026-05-12 12:46:15 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a02aac5-3b18-83ea-8d16-e29656bf4f27

> 使用边界：本文只作为研究线索和问题清单，不作为投资建议或买卖建议。涉及收入、订单、backlog、CapEx、毛利率、产能、ASP、客户关系、技术路线等实际结论，必须回到公司原始披露或监管文件核验后，才能进入主研究结论。

## 原始提问

主题8：非美材料 / 设备隐形冠军研究。

请基于本项目已上传的 md 来源文件，研究日本、韩国、台湾、欧洲、以色列 AI Infra 隐形供应链，重点找不是 AI 公司但被 AI 规格升级拉动的材料、设备、测试、真空、洁净、封装、基板公司。

严格要求：
1. 先列出需要核验的公司原始出处，不要直接把媒体/模型记忆当结论；
2. 区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”；
3. 不给买卖建议，不给目标价；
4. 不要泛化“半导体材料/设备 = AI受益”，必须对应 HBM、CoWoS、CPO/SiPh、AI server PCB/substrate、液冷/电力这些具体瓶颈。

重点方向：
- 日本：TOWA、Disco、Advantest、Lasertec、TEL、Screen、Ebara、Ibiden、Shinko、Ajinomoto、Resonac、Shin-Etsu、SUMCO、Organo、Kurita、Ferrotec
- 韩国：Hanmi、Leeno、ISC、TSE、Soulbrain、Dongjin、SK Materials、Wonik、Nextin
- 台湾：Unimicron、Nan Ya PCB、Kinsus、Chroma、MPI、WinWay、Delta、Lite-On、Alchip、GUC、Phison、Silicon Motion
- 欧洲：BESI、SUSS、VAT、Soitec、IQE、Aixtron、Sivers、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval
- 以色列：Camtek、Nova、Tower

输出：原始出处清单、按国家/地区分产业链位置、AI 相关收入证据、财报指标、潜在高弹性环节、反证指标。
展开

## ChatGPT Pro 输出

基于 2026-05-12 的项目 md，本版是主题8第一版研究底稿，不是公司原文核验后的最终结论。项目文件本身也要求：涉及收入、订单、backlog、CapEx、毛利率、产能、客户关系等实际数据，都只能先作为“待原文核验线索”，后续必须回到公司原始披露、交易所公告、监管文件或公司技术资料核验；ChatGPT Pro 输出和二手资料不能直接进入结论层。

2026-05-12-ai-super-cycle-resea…

0. 先列：需要核验的公司原始出处清单
0.1 统一核验口径

所有公司先抓以下原文，不用媒体、券商摘要或模型记忆替代：

原始出处类型	用来证明什么	不足
Annual report / 20-F / 有报	segment revenue、地域、客户集中、CapEx、库存、现金流	通常不会直接披露 AI 收入
Quarterly results / earnings release	最近订单、收入、毛利、指引、库存变化	口径可能变化，需要看脚注
Earnings call transcript / Q&A	AI、HBM、CoWoS、CPO、data center、liquid cooling 相关管理层口径	管理层描述不能替代订单证据
Investor presentation / capital markets day	技术路线、TAM、订单、产能规划	容易偏宣传，要和财报交叉
交易所公告 / 大客户公告	大额订单、长期供货、产能投资、客户认证	很多客户名可能不披露
公司官网技术资料 / product page	产品是否对应 HBM、CoWoS、CPO、SiPh、AI server power/cooling	技术可用不等于收入已确认
客户/供应商交叉披露	TSMC、SK hynix、Samsung、Micron、NVIDIA、Broadcom、hyperscaler 侧验证	需要确认供应链关系是否直接

这符合项目 checklist 的要求：先列原始披露、确定产业链位置、找客户与订单证据，再看毛利率、ASP、backlog、CapEx、FCF 和反证路径。

research-checklist

0.2 必须先核验的上游“需求锚点”原文

这些不是本主题公司，但决定主题8公司的 AI 相关性是否成立：

需求锚点	原始出处	需要证明的问题
NVIDIA GB200/GB300/NVL72 等平台	NVIDIA product page、architecture white paper、data center platform presentation	HBM 容量/stack、网络端口、电力/液冷规格是否升级
TSMC CoWoS / SoIC / advanced packaging	TSMC annual report、technology symposium、earnings call	CoWoS 扩产、interposer、substrate、hybrid bonding、test/inspection 是否仍是瓶颈
SK hynix / Samsung / Micron HBM	年报、季度财报、earnings call、HBM 技术材料	HBM3E/HBM4 qualification、HBM revenue、wafer allocation、12-high/16-high、测试时间
Broadcom / hyperscaler CPO/800G/1.6T	Broadcom IR、product release、hyperscaler network capex	CPO、silicon photonics、1.6T 是否进入真实采购
Hyperscaler CapEx / RPO	Microsoft、Alphabet、Meta、Amazon 原始财报	AI 数据中心 CapEx 是否持续，是否传导到电力/液冷/服务器 PCB
IEA/DOE/utility 数据中心电力资料	IEA、DOE、utility filings	数据中心电力与并网是否成为硬约束

项目地图已经把 NVIDIA GB300、TSMC CoWoS、SK hynix HBM、Broadcom CPO、hyperscaler CapEx、电力资料列为待核验原文锚点。

2026-05-12-ai-super-cycle-resea…

0.3 按公司需要抓的原始出处
地区	公司	必查原始出处
日本	TOWA	annual report、quarterly results、earnings call、molding/compression molding 产品资料、advanced packaging/HBM 相关客户或订单披露
日本	Disco	annual report、quarterly results、precision cutting/grinding/thinning 产品资料、AI/HBM/advanced packaging 需求口径
日本	Advantest	annual report、quarterly results、earnings call、SoC tester/memory tester breakdown、AI/HBM tester demand presentation
日本	Lasertec	annual report、quarterly results、EUV mask inspection / inspection 产品资料、订单与客户集中
日本	Tokyo Electron	annual report、quarterly results、WFE segment、etch/deposition/clean 对 DRAM/HBM/logic demand 的口径
日本	Screen	annual report、quarterly results、clean/wet process equipment、advanced node / memory customer demand
日本	Ebara	annual report、quarterly results、CMP、dry vacuum pump、plating、data center/fab infra exposure
日本	Ibiden	annual report、quarterly results、IC package substrate / ABF disclosure、HPC/AI package substrate commentary
日本	Shinko	annual report、quarterly results、IC package / flip-chip substrate disclosure、advanced package substrate
日本	Ajinomoto	annual report、ABF 技术资料、electronic materials segment、ABF capacity / high-end substrate demand
日本	Resonac	annual report、quarterly results、semiconductor materials / packaging materials / CMP / underfill / molding
日本	Shin-Etsu	annual report、quarterly results、silicon wafer、photoresist、materials segment
日本	SUMCO	annual report、quarterly results、300mm wafer demand、logic/memory capacity utilization
日本	Organo	annual report、quarterly results、UPW / water treatment orders、semiconductor fab project exposure
日本	Kurita	annual report、quarterly results、UPW、water treatment、semiconductor customer orders
日本	Ferrotec	annual report、quarterly results、vacuum feedthroughs、thermoelectric modules、quartz/ceramics、liquid cooling / thermal exposure
韩国	Hanmi	annual report、quarterly results、TC bonder / HBM bonding equipment、customer concentration
韩国	Leeno	annual report、quarterly results、test socket / probe pin revenue、HBM / AI chip test exposure
韩国	ISC	annual report、quarterly results、test socket product mix、AI/HBM socket demand
韩国	TSE	annual report、quarterly results、probe card / test interface / memory test exposure
韩国	Soulbrain	annual report、quarterly results、wet chemicals / etchants / precursor product mix
韩国	Dongjin	annual report、quarterly results、photoresist / wet chemical product mix
韩国	SK Materials	parent-company filings、materials segment、special gases / precursors / etch gas
韩国	Wonik	Wonik IPS / Wonik Materials annual and quarterly filings、WFE and gas/material split
韩国	Nextin	annual report、quarterly results、inspection equipment, memory/logic customer exposure
台湾	Unimicron	annual report、monthly revenue、quarterly presentation、ABF / IC substrate / AI server PCB split
台湾	Nan Ya PCB	annual report、monthly revenue、substrate / PCB product mix、AI server / high-end substrate commentary
台湾	Kinsus	annual report、monthly revenue、IC substrate / ABF / BT split、HPC/AI substrate
台湾	Chroma	annual report、quarterly results、semiconductor test / power electronics test / AI server test
台湾	MPI	annual report、monthly revenue、probe card / wafer probing / high frequency probing
台湾	WinWay	annual report、monthly revenue、probe card product mix、HPC/HBM/customer certification
台湾	Delta	annual report、quarterly results、power supply、thermal、data center solutions、AI server/rack exposure
台湾	Lite-On	annual report、quarterly results、power supply、optoelectronics、cloud/data center exposure
台湾	Alchip	annual report、quarterly results、NRE / ASIC design revenue、customer concentration、CoWoS/HBM attach
台湾	GUC	annual report、quarterly results、ASIC / NRE / turnkey revenue、advanced node and HPC exposure
台湾	Phison	annual report、monthly revenue、enterprise SSD/controller mix、PCIe Gen5/Gen6、QLC enterprise qualification
台湾	Silicon Motion	annual report / 20-F、quarterly results、enterprise SSD controller / data center product mix
欧洲	BESI	annual report、quarterly results、hybrid bonding / advanced packaging orders、datacenter/AI commentary
欧洲	SUSS	annual report、quarterly results、temporary bonding / lithography / advanced packaging demand
欧洲	VAT	annual report、quarterly results、vacuum valve orders、semiconductor capex split
欧洲	Soitec	annual report、quarterly results、SOI / SmartSiC / photonics substrate exposure
欧洲	IQE	annual report、quarterly results、InP / GaAs epitaxy、datacom/photonic exposure
欧洲	Aixtron	annual report、quarterly results、MOCVD tool orders、GaN/SiC/InP-related demand
欧洲	Sivers	annual report、quarterly results、photonics / laser / CPO design wins
欧洲	Infineon	annual report、quarterly results、power semiconductors、data center power exposure
欧洲	Schneider	annual report、quarterly results、secure power/data center backlog、orders、margin
欧洲	Siemens Energy	annual report、quarterly results、grid technologies、transformer/order backlog
欧洲	Munters	annual report、quarterly results、data center cooling / air treatment orders
欧洲	Alfa Laval	annual report、quarterly results、heat exchanger / data center cooling exposure
以色列	Camtek	20-F / annual report、quarterly results、advanced packaging inspection / metrology revenue, HBM/CoWoS exposure
以色列	Nova	20-F / annual report、quarterly results、metrology product revenue、advanced packaging / memory / logic customer exposure
以色列	Tower	20-F / annual report、quarterly results、silicon photonics / specialty foundry process disclosures
1. 本主题的一句话假设

如果 LLM 训练/推理和 AI 数据中心建设持续扩张，那么非美材料、设备、测试、真空、洁净、封装、基板、电源与热管理公司，会因为 HBM stack 高度上升、CoWoS/advanced packaging 复杂度上升、AI chip / HBM 测试时间拉长、800G/1.6T/CPO/SiPh 规格升级、AI server PCB/substrate 层数和材料升级、rack 级电力/液冷约束 获得收入、毛利率或订单弹性。

这个假设必须严格排除“泛半导体材料/设备 = AI 受益”的写法。项目文档明确要求避免材料泛化，只找 AI 封装、HBM、硅光、EUV、功率电源等直接提高用量或规格的材料/设备。

2026-05-12-ai-super-cycle-resea…

2. 产业链位置：按国家/地区拆分
2.1 日本
公司	产业链位置	对应 AI 瓶颈	证据状态	主要反证
TOWA	先进封装 molding / compression molding	HBM、CoWoS、AI package molding / encapsulation	合理推论 + 待原文核验：项目将 TOWA 放入 HBM/advanced packaging 设备池	订单来自普通封装复苏；molding ASP 不提升；客户扩产后议价下降
Disco	wafer thinning / dicing / grinding	HBM wafer thinning、advanced package singulation	合理推论 + 待原文核验	设备需求只是通用 WFE 后段复苏；客户良率提升降低单位设备需求
Advantest	SoC / memory tester	AI GPU/ASIC final test、HBM memory test	合理推论 + 待原文核验：项目主题3明确把 Advantest 放入 AI/HBM testing 池	测试时间缩短；tester 订单提前透支；memory cycle 下行
Lasertec	EUV/mask inspection、inspection	leading-edge GPU/ASIC/HBM DRAM 制程良率	合理推论 + 待原文核验	需求更多来自 EUV 通用扩产，不是 AI；订单周期性回落
Tokyo Electron	etch/deposition/clean WFE	advanced logic、DRAM/HBM 制造	合理推论 + 待原文核验	大盘 WFE beta，AI 收入不可拆；中国/存储周期影响更大
Screen	clean / wet process equipment	leading-edge logic、DRAM/HBM clean step	合理推论 + 待原文核验	普通晶圆厂 capex 周期主导；clean 设备不具备 AI 特异性
Ebara	CMP、vacuum pump、plating	CoWoS/RDL、logic/HBM manufacturing、fab infra	合理推论 + 待原文核验	CMP/vacuum 收入分散；难证明 AI 规格升级
Ibiden	ABF / package substrate	GPU/HBM package substrate、AI server substrate	合理推论 + 待原文核验	ABF 供需转松；CoWoS 瓶颈转移；客户压价
Shinko	package substrate	HPC/AI package substrate	合理推论 + 待原文核验	同上，且客户集中风险
Ajinomoto	ABF materials	ABF build-up film for high-end package substrate	合理推论 + 待原文核验	ABF 单价下滑；AI substrate 增长被 PC/consumer 拖累
Resonac	semiconductor / packaging materials	underfill、molding、CMP、advanced package materials	合理推论 + 待原文核验	材料收入分散；普通汽车/工业/存储周期主导
Shin-Etsu	silicon wafer、photoresist/materials	leading-edge logic、HBM DRAM wafer/materials	合理推论 + 待原文核验	wafer 大周期强于 AI 逻辑；客户议价
SUMCO	silicon wafer	advanced logic / memory wafer starts	合理推论 + 待原文核验	硅片供需下行；AI 对总 wafer demand 占比不够
Organo	UPW / water treatment	advanced fabs、HBM/CoWoS fab water infrastructure	合理推论 + 待原文核验	fab project 延后；UPW 订单不等于 AI
Kurita	UPW / water treatment	advanced fabs、data center / semiconductor water	合理推论 + 待原文核验	同上；工业水处理收入稀释 AI 暴露
Ferrotec	vacuum components、thermal / materials	fab vacuum parts、thermal modules、可能液冷/热管理	合理推论 + 待原文核验	收入结构复杂；新能源/工业/普通半导体拖累

项目地图把日本重点公司分成 HBM/先进封装设备、测试、EUV/检测、WFE、ABF/封装基板、硅片/材料、真空/气体/洁净/UPW 等方向，并要求验证收入是否真的来自 AI/HBM/CoWoS，而不是普通半导体复苏。

2026-05-12-ai-super-cycle-resea…

2.2 韩国
公司	产业链位置	对应 AI 瓶颈	证据状态	主要反证
Hanmi	HBM bonding / packaging equipment	TC bonder、HBM stacking、advanced packaging	合理推论 + 待原文核验	订单被 memory vendor 内化或转向其他设备；Samsung qualification 改变份额
Leeno	test pins / sockets	HBM、AI GPU/ASIC test interface	合理推论 + 待原文核验	socket ASP 不升；测试时间优化；客户集中
ISC	test sockets	AI SoC / HBM / high-end semiconductor test	合理推论 + 待原文核验	泛测试 beta；客户采购放缓
TSE	probe card / test interface	HBM wafer probe、memory test	合理推论 + 待原文核验	memory cycle 回落；HBM 良率提升降低测试需求
Soulbrain	wet chemicals / etchants / precursors	DRAM/HBM、advanced logic materials	合理推论 + 待原文核验	材料收入分散；普通半导体周期主导
Dongjin	photoresist / wet chemicals	DRAM/HBM、logic process chemicals	合理推论 + 待原文核验	无法拆 AI；本土替代/价格竞争
SK Materials	special gases / precursors	HBM DRAM / advanced logic etch & deposition gases	合理推论 + 待原文核验	需要确认主体与披露口径；客户 CapEx 波动
Wonik	WFE / gases/materials	DRAM/HBM WFE、special gases	合理推论 + 待原文核验	WFE 大周期；Samsung/Hynix CapEx 波动
Nextin	inspection	DRAM/logic inspection, yield	合理推论 + 待原文核验	检测设备订单周期；客户集中

项目地图把韩国方向明确拆成 HBM bonding/packaging equipment、前道设备、材料/气体/化学品、测试/探针，并特别提示 HBM 设备订单是否被头部厂商内化或重新分配，以及 HBM4 路线是否降低现有设备/材料价值量。

2026-05-12-ai-super-cycle-resea…

2.3 台湾
公司	产业链位置	对应 AI 瓶颈	证据状态	主要反证
Unimicron	ABF / IC substrate / PCB	GPU/HBM package substrate、AI server PCB	合理推论 + 待原文核验	TSMC 扩产后 substrate 议价下降；普通 PCB 周期拖累
Nan Ya PCB	substrate / high-end PCB	AI server PCB、IC substrate	合理推论 + 待原文核验	AI server PCB 占比低；layer/material ASP 不升
Kinsus	IC substrate / ABF	GPU/HPC substrate	合理推论 + 待原文核验	ABF 周期下行；客户集中
Chroma	ATE / power test	AI chip test、AI server power test	合理推论 + 待原文核验	普通电源/EV/industrial test 周期混淆
MPI	probe card / wafer probing	AI SoC/HBM wafer probe	合理推论 + 待原文核验	探针需求被大客户内化；测试时间下降
WinWay	probe card	HPC/HBM test	合理推论 + 待原文核验	客户集中、订单波动、认证失败
Delta	power supply / thermal / data center solutions	AI rack power、48V、liquid cooling、DC power	合理推论 + 待原文核验	电源是低毛利装配；数据中心延期；价格竞争
Lite-On	power supply / opto / cloud hardware	AI server PSU、rack power	合理推论 + 待原文核验	AI server PSU 占比不清；普通 consumer/PC 拖累
Alchip	ASIC design service	hyperscaler custom ASIC, CoWoS/HBM attach	合理推论 + 待原文核验	单一客户；tape-out 失败；NRE 与量产收入断层
GUC	ASIC / turnkey	custom ASIC, advanced node, CoWoS allocation	合理推论 + 待原文核验	客户集中；foundry/CoWoS 配额限制；毛利不提升
Phison	SSD controller / enterprise SSD	AI eSSD、PCIe Gen5/6、QLC enterprise controller	合理推论 + 待原文核验	consumer NAND 周期反弹被误判为 AI；enterprise 占比低
Silicon Motion	SSD controller	enterprise SSD / data center controller	合理推论 + 待原文核验	controller ASP 被压缩；缺 hyperscaler qualification

项目地图把台湾方向分成 CoWoS、OSAT/测试、CoWoS 相关设备、ATE/探针、基板/PCB、电源/散热、ASIC/IP、存储控制器，并提示低毛利 ODM 不一定捕获价值、TSMC 扩产后中游议价可能下降、单一客户或单一平台风险高。

2026-05-12-ai-super-cycle-resea…

2.4 欧洲
公司	产业链位置	对应 AI 瓶颈	证据状态	主要反证
BESI	hybrid bonding / advanced packaging equipment	HBM4 / hybrid bonding、2.5D/3D packaging	合理推论 + 待原文核验	hybrid bonding 量产节奏延后；订单一次性
SUSS	temporary bonding / lithography / advanced packaging	CoWoS/RDL/interposer、advanced package process	合理推论 + 待原文核验	AI module 需求只是描述，收入未落地
VAT	vacuum valves	WFE/fab vacuum infrastructure	合理推论 + 待原文核验	真空阀是半导体 capex beta，不是 AI 特异性
Soitec	SOI / engineered substrates	silicon photonics、RF-SOI、SmartSiC / power	合理推论 + 待原文核验	手机/RF 周期主导；SiPh revenue 不清
IQE	InP / GaAs epitaxy	800G/1.6T、CPO、laser / photonics material	合理推论 + 待原文核验	telecom/consumer/VCSEL 周期混淆；CPO 延后
Aixtron	MOCVD equipment	GaN/SiC/InP equipment, photonics/power	合理推论 + 待原文核验	SiC EV 周期拖累；AI optical/power 占比低
Sivers	photonics / laser / optical IC	CPO / external light source / SiPh	合理推论 + 待原文核验	design win 不转收入；客户集中；CPO 延后
Infineon	power semiconductor	AI server power conversion, PSU, VRM, data center power	合理推论 + 待原文核验	automotive/industrial cycle dominates；AI power 占比低
Schneider	secure power / electrical equipment	data center UPS、switchgear、power management	合理推论 + 待原文核验	大公司多业务稀释；订单提前透支
Siemens Energy	grid / transformers / power equipment	data center grid, transformer, substation	合理推论 + 待原文核验	电网周期广泛，不一定 AI；项目交付周期长
Munters	cooling / air treatment	data center cooling, possible liquid/evaporative cooling	合理推论 + 待原文核验	风冷延寿；冷却方案价格竞争
Alfa Laval	heat exchanger / thermal	liquid cooling heat exchange、data center thermal	合理推论 + 待原文核验	data center 占比低；工业热交换业务稀释

项目地图把欧洲方向分为 lithography/WFE、advanced packaging、真空/设备零部件、SOI/化合物半导体、硅光/激光、功率半导体、电力/数据中心设备、热管理，并提示欧洲小盘常见流动性弱、订单波动、客户集中和技术路线不确定，不能只看 AI/CPO 标签。

2026-05-12-ai-super-cycle-resea…

2.5 以色列
公司	产业链位置	对应 AI 瓶颈	证据状态	主要反证
Camtek	advanced packaging inspection / metrology	CoWoS、HBM、advanced package inspection	合理推论 + 待原文核验	advanced packaging 订单不是 AI；inspection 订单周期性
Nova	metrology	logic / memory / advanced packaging process control	合理推论 + 待原文核验	leading-edge capex beta；AI 收入不可拆
Tower	specialty foundry / silicon photonics	silicon photonics、CPO ecosystem、specialty process	合理推论 + 待原文核验	SiPh revenue 未放量；传统 analog/RF/industrial 周期主导

项目地图把以色列方向放在 advanced packaging inspection/metrology、specialty foundry、silicon photonics / optical connectivity，并要求用先进封装订单、HBM/CoWoS 客户、gross margin 与订单可见度验证，而不是只看公司官网标签。

2026-05-12-ai-super-cycle-resea…

3. AI 相关收入证据：分层判断
3.1 原文已证明

严格按项目要求，本轮没有公司原始财报逐条核验，因此不能写“某公司 AI 收入已证明”。目前原文已证明的只有项目研究框架层面的事实：

项目文档将 AI Infra 拆成 token demand → GPU/ASIC → HBM/DRAM/eSSD → 光互连 → 先进封装/测试/基板 → WFE/材料/真空/洁净 → 数据中心/冷却 → 电力/能源的多层约束系统。

2026-05-12-ai-super-cycle-resea…

项目优先级聚焦 HBM/CoWoS 设备测试材料、光互连/CPO/InP/激光、电力液冷、非美隐形供应链和小中市值。

2026-05-12-ai-super-cycle-resea…

项目文件明确指出 HBM 设备/测试/材料、HBM test、TCB/hybrid bonding、AI eSSD、server DRAM 属于更接近真实 AI 拉动的链条，而普通 NAND/普通 SSD 更容易是 AI 叙事映射。

2026-05-12-ai-super-cycle-resea…

3.2 合理推论

以下是可以从项目 md 推出的“合理推论”，但仍需公司原文验证：

具体瓶颈	合理推论	公司池
HBM 设备/材料/测试	HBM stack 高度、bonding、testing、substrate 复杂度上升，会拉动非 memory vendor 的设备/材料/测试公司	Hanmi、TOWA、Disco、Advantest、Leeno、ISC、TSE、Ibiden、Shinko、Ajinomoto、Resonac、BESI、SUSS、Camtek
CoWoS / 2.5D	瓶颈可能从 TSMC CoWoS 扩散到 substrate、interposer、设备、材料、测试	Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、BESI、SUSS、TOWA、Disco、Camtek、Nova
AI/HBM testing	AI die 大、封装复杂、HBM stack 多，可能提高 wafer probe、memory test、final test、probe card、inspection 价值量	Advantest、Chroma、MPI、WinWay、Leeno、ISC、TSE、Camtek、Nova
CPO/SiPh/1.6T	AI cluster 网络和功耗压力可能拉动 InP、SiPh、laser、photonics substrate、specialty foundry	IQE、Sivers、Soitec、Aixtron、Tower
AI server PCB/substrate	AI server / accelerator substrate 层数、低损耗材料、电源完整性要求升级	Unimicron、Nan Ya PCB、Kinsus、Ibiden、Shinko、Ajinomoto
电力/液冷	AI rack 功率密度上升，可能拉动 PSU、VRM、UPS、switchgear、CDU、cold plate、heat exchanger	Delta、Lite-On、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval、Ferrotec

项目模块图也把 HBM 外溢链、机柜级电源、液冷、AI 网络深水区、AI server ODM/PCB/CCL、Fab 基础设施/特气/真空/除害/洁净室、电网接入列为优先深挖主题。

2026-05-12-chatgpt-pro-module-m…

3.3 待原文核验

以下公司都不能在本轮写成“AI 收入已确认”，需要按原文核验：

直接待核验 AI/HBM/CoWoS 订单：Hanmi、TOWA、BESI、SUSS、Advantest、Camtek、Nova、Chroma、MPI、WinWay、Leeno、ISC、TSE。

待核验 AI substrate / ABF / PCB 收入占比：Ibiden、Shinko、Ajinomoto、Unimicron、Nan Ya PCB、Kinsus、Resonac。

待核验 CPO/SiPh 收入而非 design-win 叙事：Sivers、IQE、Soitec、Aixtron、Tower。

待核验 AI server power/cooling 而非普通工业/电气周期：Delta、Lite-On、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval、Ferrotec。

待核验材料/真空/洁净是否有 AI 规格升级：Shin-Etsu、SUMCO、Soulbrain、Dongjin、SK Materials、Wonik、VAT、Organo、Kurita、Ebara、Screen、TEL。

3.4 主要反证
反证类型	说明
泛半导体周期误判	收入增长来自普通 logic/memory capex recovery，不是 HBM、CoWoS、CPO、AI server
客户 CapEx 放缓	NVIDIA/HBM/CoWoS/hyperscaler capex 放缓，设备、基板、测试订单最先被砍
技术路线改变	HBM4/HBM4E、hybrid bonding、panel-level packaging、CPO 延后或 pluggable 延寿改变设备价值量
良率提升	HBM yield、test efficiency、CoWoS yield 改善，降低单位测试/inspection/设备需求
供给扩张	substrate、ABF、probe card、tester、cooling 产能扩张后 ASP 下行
毛利率不跟随	收入增加但 gross margin / operating margin 不升，说明只是低附加值放量
客户集中	单一 memory vendor、foundry、hyperscaler 或 ASIC 客户导致收入不可持续
库存/订单提前透支	backlog 高但取消、延迟或无法转收入；库存上升快于收入

项目 checklist 也把这些反证写入标准路径：客户 CapEx 放缓、NVIDIA 供给放开、HBM 供给过剩、ASIC 成功分流、推理成本下降、电力并网卡住、客户换供应商、毛利率不提升等。

research-checklist

4. 财报指标：每类公司要看什么
4.1 HBM / advanced packaging equipment

适用：TOWA、Disco、Hanmi、BESI、SUSS、TEL、Screen、Ebara、Aixtron。

指标	看什么	证明力
orders / backlog / book-to-bill	HBM、advanced packaging、hybrid bonding、molding、temporary bonding 订单是否增长	强
segment revenue	packaging equipment、back-end equipment、semiconductor equipment 是否增长	中
gross margin	AI/advanced packaging mix 上升是否带来毛利提升	强
customer concentration	是否绑定 SK hynix、Samsung、Micron、TSMC、ASE、Amkor 等	强，但要防集中风险
CapEx / inventory	是否提前备货或扩产	中
management commentary	是否明确 HBM、CoWoS、AI chip、hybrid bonding	中，需订单验证
4.2 Test / probe / inspection / metrology

适用：Advantest、Chroma、MPI、WinWay、Leeno、ISC、TSE、Camtek、Nova、Lasertec、Nextin。

指标	看什么	证明力
SoC tester revenue	GPU/ASIC test 是否贡献增量	强
memory tester revenue	HBM/DRAM tester 是否增长	强
probe card / socket revenue	HBM wafer probe、AI SoC probe card 是否上升	强
advanced packaging inspection revenue	CoWoS/HBM/2.5D inspection 是否增长	强
gross margin / ASP	test complexity 是否转化为价格权	强
backlog / orders	是否有可见订单	强
utilization / inventory	是否订单提前透支	反证

项目主题3明确提出要验证 AI 芯片复杂度是否让测试时间、测试设备、probe card 和 inspection 成为高弹性瓶颈，并给出 SoC tester revenue、memory tester revenue、probe card revenue、gross margin、orders/backlog 等财报指标。

2026-05-12-ai-super-cycle-resea…

4.3 Substrate / PCB / ABF / materials

适用：Ibiden、Shinko、Ajinomoto、Resonac、Unimicron、Nan Ya PCB、Kinsus、Soulbrain、Dongjin、SK Materials、Wonik、Shin-Etsu、SUMCO、Soitec、IQE。

指标	看什么	证明力
IC package substrate revenue	HPC/AI/ABF substrate 是否增长	强
ABF / build-up film demand	高层数、高端 substrate 是否拉动 Ajinomoto / substrate customers	中到强
low-loss PCB / AI server PCB	高速材料、层数、AI server board 是否提升 ASP	强
semiconductor materials revenue	是否来自 HBM/CoWoS/advanced logic/SiPh 而非泛材料	中
gross margin	高端材料 mix 是否带来 margin expansion	强
capacity expansion	是否为 ABF、advanced substrate、InP/SOI 等特定扩产	中
customer disclosures	TSMC / substrate makers / HBM vendor 交叉验证	强

项目文件强调，材料链不能泛化，只能找 AI 封装、HBM、硅光、EUV、功率电源直接提高用量或规格的材料。

2026-05-12-ai-super-cycle-resea…

4.4 CPO / SiPh / InP / photonics

适用：IQE、Sivers、Soitec、Aixtron、Tower。

指标	看什么	证明力
photonics / datacom revenue	是否已有 datacom / AI network revenue	强
InP / SOI / SiPh substrate revenue	是否受 800G/1.6T/CPO 牵引	中到强
design wins vs production revenue	design win 是否转化为量产收入	强
gross margin	photonics mix 是否改善利润率	强
customer concentration	hyperscaler、switch ASIC、optics module 客户	强但有风险
CPO schedule	CPO 是否延后，pluggable 是否延寿	关键反证

项目主题4要求拆解 800G、1.6T、CPO、硅光、外置光源、InP，并跟踪 datacom revenue、800G/1.6T mix、laser capacity、gross margin、customer concentration 等指标。

2026-05-12-ai-super-cycle-resea…

4.5 Power / liquid cooling / data center electrical

适用：Delta、Lite-On、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval、Ferrotec。

指标	看什么	证明力
data center revenue / orders	数据中心电力/冷却订单是否增长	强
backlog / book-to-bill	是否存在供给瓶颈	强
gross margin	是否有瓶颈租金，而非低毛利放量	强
liquid cooling attach rate	CDU、cold plate、heat exchanger 需求是否落地	强
rack kW / power density	AI rack 规格升级是否推动单机价值量	强
transformer / switchgear lead time	电网设备瓶颈是否传导到订单	强
working capital / inventory	是否提前备货或项目延期	反证

项目文件把电力设备、冷却、热管理拆成 UPS、PDU、switchgear、transformer、power module、VRM、SiC/GaN、liquid cooling、CDU、cold plate、heat exchanger 等，并给出 backlog、book-to-bill、transformer lead time、liquid cooling attach rate、rack kW、gross margin 等关键指标。

2026-05-12-ai-super-cycle-resea…

5. 潜在高弹性环节排序

这里不是买卖建议，也不做目标价；只是按“AI 需求相关度 × 供给瓶颈 × 收入弹性 × 原文可验证性”排序。

5.1 第一优先级：HBM 外溢链

环节：TC bonding / hybrid bonding、molding、wafer thinning/dicing、memory tester、probe card/socket、advanced packaging inspection、ABF/substrate。
公司池：Hanmi、TOWA、Disco、Advantest、Leeno、ISC、TSE、WinWay、MPI、Camtek、Nova、Ibiden、Shinko、Ajinomoto、Unimicron、Kinsus、BESI、SUSS。
为什么弹性高：HBM 本身由大 memory vendor 主导，但二阶设备/测试/材料公司收入基数可能更小，且 HBM stack、测试时间、封装复杂度上升可能提高单位价值量。项目文件也明确指出 HBM 设备/材料环节可能比 memory vendor 更有弹性。

2026-05-12-ai-super-cycle-resea…


关键反证：HBM4 扩产过快、Samsung/Micron 补上供给、HBM yield 提升、设备价值量被新路线压缩。

5.2 第一优先级：CoWoS / substrate / inspection

环节：ABF substrate、interposer、RDL、temporary bonding、hybrid bonding、inspection/metrology。
公司池：Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、BESI、SUSS、TOWA、Disco、Camtek、Nova。
为什么弹性高：CoWoS 瓶颈如果从 TSMC 转向 substrate、设备和 inspection，二阶供应商的收入弹性可能高于成熟 WFE 大盘。
关键反证：TSMC 扩产后瓶颈消失、substrate 产能过剩、foundry 吸收利润池、中游议价下降。

5.3 第二优先级：AI/HBM testing 与 probe ecosystem

环节：wafer probe、HBM memory test、GPU/ASIC final test、probe card、socket、burn-in、advanced package inspection。
公司池：Advantest、Chroma、MPI、WinWay、Leeno、ISC、TSE、Camtek、Nova、Lasertec、Nextin。
为什么弹性高：AI 芯片 die 大、封装贵、known-good-die 价值高，测试和良率管理的边际价值上升。
关键反证：测试时间被工艺优化压缩；tester 订单提前透支；memory/logic capex 下行。

5.4 第二优先级：CPO / SiPh / InP 深水区

环节：InP epitaxy、silicon photonics foundry、external light source、CPO packaging、photonics test。
公司池：IQE、Sivers、Soitec、Aixtron、Tower。
为什么弹性高：800G → 1.6T → CPO 如果成为 AI network 功耗/密度解决方案，会拉动小众光子材料和工艺供应商。
关键反证：CPO 延后、pluggable optics 延寿、LPO/LRO 路线变化、design win 不转量产。

5.5 第二优先级：AI server PCB / power / liquid cooling

环节：AI server PCB/CCL、PSU、48V、VRM、UPS、switchgear、CDU、cold plate、heat exchanger。
公司池：Unimicron、Nan Ya PCB、Kinsus、Delta、Lite-On、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval、Ferrotec。
为什么弹性高：AI Infra 从 GPU 采购转向“拿到电、散掉热、点亮 rack”，电力和冷却可能成为物理瓶颈。项目文档也把电力/液冷列为核心交叉点。

2026-05-12-ai-super-cycle-resea…


关键反证：数据中心延期、风冷延寿、冷却方案标准化后 ASP 下滑、真正瓶颈是并网许可而非设备。

5.6 第三优先级：泛 WFE / wafer / chemicals / vacuum / UPW

环节：WFE、硅片、wet chemicals、special gases、vacuum valves、UPW。
公司池：TEL、Screen、Ebara、Lasertec、Shin-Etsu、SUMCO、Soulbrain、Dongjin、SK Materials、Wonik、VAT、Organo、Kurita。
为什么不是第一优先级：它们很重要，但很多收入更像半导体大周期 beta。除非原文能证明订单来自 HBM、CoWoS、AI leading-edge、SiPh 或 data center power，否则不能进入核心池。
关键反证：AI 需求集中在少数 leading-edge fabs，材料公司收入分散；客户议价强；传统半导体周期拖累。

2026-05-12-ai-super-cycle-resea…

6. 反证仪表盘
层级	红旗指标	对应受影响公司
HBM	HBM ASP 下行、HBM sold-out 口径消失、HBM CapEx 下修、HBM4 qualification 延迟	Hanmi、TOWA、Disco、Advantest、Leeno、ISC、TSE、Ajinomoto、Ibiden、Shinko
CoWoS	TSMC CoWoS bottleneck 缓解、substrate lead time 缩短、interposer/substrate price 下行	Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、BESI、SUSS、Camtek、Nova
Testing	tester backlog 下降、memory tester utilization 下行、probe card ASP 不升、客户减少测试时间	Advantest、Chroma、MPI、WinWay、Leeno、ISC、TSE
CPO/SiPh	CPO 量产延后、1.6T pluggable 延寿、datacom revenue 不增、design win 不转 revenue	IQE、Sivers、Soitec、Aixtron、Tower
AI server PCB/power	AI server 出货增长但 PCB/PSU/cooling 毛利不升；库存快于收入	Unimicron、Nan Ya PCB、Kinsus、Delta、Lite-On
Data center power/cooling	backlog 高但交付延迟、book-to-bill 下滑、数据中心项目延期、并网许可卡住	Schneider、Siemens Energy、Munters、Alfa Laval、Infineon
Materials/vacuum/UPW	semiconductor materials revenue 增长但无 HBM/advanced package/SiPh 口径；毛利率不改善	Shin-Etsu、SUMCO、Soulbrain、Dongjin、SK Materials、Wonik、VAT、Organo、Kurita、Ebara
总需求	hyperscaler CapEx 下修、GPU utilization 下降、token 价格下降快于 token 量增长、NeoCloud 利用率/信用恶化	全链条
7. 结论：第一版研究池
核心研究池候选，优先做公司卡片

这些最贴近 HBM/CoWoS/testing/substrate，且可用公司原文验证：

日本：TOWA、Disco、Advantest、Ibiden、Shinko、Ajinomoto、Resonac

韩国：Hanmi、Leeno、ISC、TSE

台湾：Unimicron、Nan Ya PCB、Kinsus、Chroma、MPI、WinWay

欧洲：BESI、SUSS

以色列：Camtek、Nova

重点观察池

这些方向有较强逻辑，但需要更严格拆收入结构：

CPO/SiPh/InP：IQE、Sivers、Soitec、Aixtron、Tower

Power / cooling / electrical：Delta、Lite-On、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval

ASIC / storage controller：Alchip、GUC、Phison、Silicon Motion

主题跟踪池

这些很重要，但当前更容易被误判成“泛半导体 beta”：

WFE / inspection / clean / CMP：TEL、Screen、Ebara、Lasertec、Nextin

Materials / gases / wafer / UPW / vacuum：Shin-Etsu、SUMCO、Soulbrain、Dongjin、SK Materials、Wonik、VAT、Organo、Kurita、Ferrotec

最关键的下一步不是扩公司名单，而是逐家公司填证据卡：原文能证明什么、不能证明什么、哪些只是合理推论、哪些反证会推翻 AI 受益假设。
