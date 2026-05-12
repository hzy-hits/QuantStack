# ChatGPT Pro 输出草稿：A 股主板 AI Infra 映射总图 v1

状态：ChatGPT Pro output, pending original-source verification  
捕获时间：2026-05-12 14:26:42 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a02c2ec-71e4-83ea-a900-0bc31c7001dd

> 使用边界：本文只作为 A 股主板候选映射线索，不作为投资建议或买卖建议。涉及收入、订单、backlog、CapEx、毛利率、产能、ASP、客户关系、技术路线等实际结论，必须回到公司年报、半年报、交易所公告、投资者关系活动记录、公司官网技术资料或上下游原始披露核验后，才能进入主研究结论。

> 质量提示：这个 Pro 会话是在 D0-D5 LLM Dependency BFS 框架补入项目文档前发出的，因此后续需要再按 BFS 深度重整。A 股候选优先看 D1-D3；D4-D5 只做雷达；超过 D5 默认排除。

## 原始提问

会话标题：A股主板 AI Infra 映射总图 v1

请基于本项目已经完成的 AI Infra 海外产业链研究，把海外瓶颈环节映射到非科创板、非创业板 A 股主板公司。

重要前提：
1. 这不是投资建议，不给买卖建议，不给目标价；
2. 不要从 A 股热门概念出发；
3. 必须先列海外瓶颈环节，再映射 A 股公司；
4. 排除创业板 300xxx 和科创板 688xxx；
5. 每家公司都要标注“对应海外瓶颈 / 对应海外公司 / A 股产业链位置 / 需要核验的原始出处 / 财报指标 / 主要反证”；
6. 所有涉及收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线的判断，都只能作为待原文核验线索，必须回到年报、半年报、交易所公告、投资者关系活动记录、公司官网技术资料验证；
7. 不要把 A 股新闻稿或券商摘要当事实。

海外已形成的瓶颈主线：
- HBM / HBM equipment / HBM testing / substrate / materials；
- CoWoS / 2.5D / advanced packaging / OSAT / inspection；
- AI server PCB / CCL / high-speed switch PCB；
- 800G / 1.6T / CPO / silicon photonics / optical components；
- Scale-up fabric / custom ASIC / PCIe-CXL / high-speed interconnect；
- AI data center power / UPS / HVDC / transformer / switchgear / liquid cooling / thermal management；
- NeoCloud / GPU-as-a-Service / AI data center economics；
- storage supercycle：HBM 最纯，server DRAM 混合，NAND/eSSD 需要严格区分传统周期。

请输出：
1. A 股主板 AI Infra 映射总表；
2. 按海外瓶颈分层的候选池；
3. 每家公司最需要核验的原始出处；
4. 每家公司最可能的误判点；
5. 第一优先级、第二优先级、观察/排除池；
6. 下一步原始出处核验清单。

请特别注意：这是候选映射，不是买卖建议；输出时优先使用表格，并把“待原文核验”写清楚。
收起

## ChatGPT Pro 输出

A股主板 AI Infra 映射总图 v1

这是一张候选映射表，不是投资建议，不含买卖建议、目标价或估值判断。这里所有公司与 AI Infra 的关系都先按“待原文核验线索”处理；收入、订单、backlog、CapEx、毛利率、产能、客户关系、技术路线，必须回到公司年报、半年报、交易所公告、投资者关系活动记录、公司官网技术资料和上下游原始披露验证。项目原有 checklist 明确要求：先看公司原始披露，再看二手摘要；收入、订单、backlog、CapEx、毛利率、客户关系等均需原文校验，媒体、券商、数据库和 ChatGPT 输出只能作为线索。

research-checklist

本表按海外瓶颈倒推，不从 A 股热门概念出发。项目研究地图已把 AI Infra 拆成“算力集群 → GPU/ASIC → HBM/DRAM/NAND/eSSD → 网络/光互连 → 先进封装/测试/基板 → 材料/气体/洁净室 → 数据中心/冷却 → 电力/能源”的多层约束系统，并把优先交叉点放在 HBM/CoWoS 设备测试材料、光互连/CPO、电力液冷、非美隐形供应链和小中市值环节。

2026-05-12-ai-super-cycle-resea…

官方核验入口建议优先使用上交所、深交所披露系统与证监会年度报告格式要求：上交所提供“股票与存托凭证”入口，深交所提供“股票列表”和“上市公司公告/定期报告”入口，证监会已公布年度报告内容与格式准则。
中国证监会
+3
上海电力公司
+3
深圳证券交易所
+3

1. A股主板 AI Infra 映射总表 v1

筛选口径：保留沪深主板候选，剔除创业板 300/301、科创板 688/689、北交所。002 系列按深市主板处理；后续仍需用交易所股票列表逐一复核上市板块、简称变更、风险警示和退市状态。

公司 / 代码	对应海外瓶颈 / 对应海外公司	A股产业链位置	最需要核验的原始出处	财报指标	主要反证 / 最可能误判点	池
长电科技 600584	CoWoS / advanced packaging / OSAT；ASE、Amkor、TSMC advanced packaging	封测、SiP、先进封装候选	年报先进封装产品口径、募投/产能公告、IR 活动、官网封装技术资料	封测收入、先进封装占比、CapEx、毛利率、产能利用率、存货	普通消费/汽车封测恢复被误判为 AI；未能证明 AI/HPC/HBM 直接订单	第一优先级
通富微电 002156	OSAT / HPC packaging；ASE、Amkor、TSMC advanced packaging	封测、HPC/CPU/GPU 封装候选	年报客户结构、先进封装产品、重大客户/订单披露、IR 记录	封测收入、先进封装收入、毛利率、客户集中、存货	客户集中、PC/传统半导体周期复苏被包装为 AI	第一优先级
华天科技 002185	OSAT / advanced packaging；ASE、Amkor	封测、先进封装二阶候选	年报产品结构、先进封装产线、募投项目、官网技术资料	封测收入、产品 mix、毛利率、CapEx、产能利用率	封装层级不够高端；AI/HPC 客户证据不足	第二优先级
深科技 000021	HBM testing / memory packaging；SK hynix、Samsung、Micron、Powertech、KYEC	存储封测、电子制造服务	年报存储封测业务、客户披露、IR 活动、官网产品页	存储封测收入、毛利率、客户集中、产能、存货	普通 DRAM/NAND 封测周期被误判为 HBM 纯受益	第二优先级
北方华创 002371	HBM / DRAM / foundry WFE；AMAT、Lam、TEL、Kokusai	半导体前道设备	年报设备分类、客户/订单、募投、IR、产品技术资料	半导体设备收入、订单/合同负债、CapEx、毛利率、研发费用	更像国产 WFE beta；AI/HBM 直接暴露需证明	第二优先级
至纯科技 603690	Fab infrastructure / wet process / high-purity systems；Organo、Kurita、Ebara	半导体高纯工艺系统、清洗/湿法相关	年报半导体客户、订单、设备/系统收入、官网技术资料	半导体系统收入、订单、合同负债、毛利率、现金流	晶圆厂扩产通用受益，不一定是 AI/HBM/CoWoS 特定瓶颈	第二优先级
雅克科技 002409	HBM / CoWoS materials；Resonac、Namics、Merck、SK Materials	半导体材料、前驱体/电子材料候选	年报电子材料细分、客户认证、产能公告、官网产品规格	电子材料收入、毛利率、客户集中、CapEx、存货	LNG/显示/其他材料混入；AI 封装材料占比可能很低	第二优先级
江化微 603078	Wet chemicals / semiconductor materials；Fujifilm、Kanto、TOK	湿电子化学品	年报产品和客户、半导体收入占比、产能利用率、IR	半导体材料收入、毛利率、库存、产能	通用材料周期，不等于 AI 高端制程瓶颈	观察
有研新材 600206	Semiconductor materials / targets；JX Advanced Metals、Entegris	靶材、电子材料候选	年报电子材料分类、客户认证、产品规格、募投	电子材料收入、毛利率、CapEx、客户集中	材料泛化；不能证明 HBM/CoWoS/硅光直接拉动	观察
金海通 603061	AI/HBM test handling；Advantest、Teradyne、Chroma、MPI	半导体测试分选/handler 候选	年报产品、测试设备客户、IR、官网设备规格	测试设备收入、订单、毛利率、海外收入、研发	不是 ATE 核心 tester；HBM 测试暴露需原文证明	观察
文一科技 600520	Packaging equipment / molding；TOWA、ASMPT	封装模具/设备候选	年报业务结构、封装设备产品、客户、交易所问询回复	半导体设备收入、毛利率、订单、现金流	AI 概念风险高；主业/盈利质量可能弱，需严查	观察
沪电股份 002463	AI server PCB / high-speed switch PCB；Unimicron、Nan Ya PCB、AT&S、Tripod	高速 PCB、服务器/交换机 PCB 候选	年报产品结构、服务器/网络 PCB 收入、IR、官网高层数/高速板资料	企业通讯板收入、毛利率、ASP、产能、客户集中、存货	普通通信板/周期恢复被误判为 AI；价格战压毛利	第一优先级
深南电路 002916	AI server PCB / substrate；Ibiden、Shinko、Unimicron、Nan Ya PCB	PCB、封装基板、电子装联	年报 PCB/封装基板拆分、产能、客户、IR、官网技术资料	PCB/基板收入、毛利率、CapEx、产能利用率、存货	军工/通信/普通封装基板混入；ABF/HBM 基板需证明	第一优先级
生益科技 600183	High-speed CCL / low-loss materials；Panasonic、Rogers、ITEQ、Nan Ya Plastics	覆铜板、树脂材料、高速低损耗材料候选	年报高频高速材料收入、产品规格、客户认证、IR	CCL 收入、毛利率、产品 mix、库存、CapEx	普通 CCL 周期反转；AI 服务器 CCL 占比不清	第一优先级
兴森科技 002436	IC substrate / ABF candidate；Ibiden、Shinko、Kinsus、AT&S	IC 载板、PCB	年报 IC 载板收入、ABF/FC-BGA 产能、募投进度、IR	IC 载板收入、产能利用率、毛利率、CapEx、良率	ABF 高端量产/客户认证不充分；周期和折旧压力	第一优先级
华正新材 603186	High-speed CCL；Rogers、ITEQ、Panasonic	覆铜板、复合材料	年报高频高速材料、Dk/Df 产品规格、客户认证	高速材料收入、毛利率、研发、存货	AI 级低损耗材料占比可能低；传统 CCL 价格周期	第二优先级
景旺电子 603228	Server/network PCB；Tripod、Compeq、Unimicron	PCB	年报产品应用领域、服务器/通信占比、IR、官网技术资料	PCB 收入、毛利率、CapEx、存货、客户集中	消费电子/汽车占比混入；AI server 证据不足	第二优先级
奥士康 002913	Server/network PCB；Tripod、Compeq	PCB	年报应用领域、客户、产能、IR	PCB 收入、毛利率、存货、产能利用率	普通 PCB beta，不一定进入 AI server 高端链	观察
博敏电子 603936	PCB / substrate candidate；Unimicron、Kinsus	PCB、封装基板候选	年报产品结构、IC 载板/高端 PCB 进展、IR	PCB/基板收入、毛利率、CapEx、存货	高端载板量产和客户认证需证明	观察
光迅科技 002281	800G / 1.6T / optical components；Coherent、Lumentum、Fabrinet、InnoLight	光模块、光器件	年报数通/电信拆分、800G/1.6T 产品、客户认证、官网规格	数通收入、800G/1.6T mix、毛利率、客户集中、存货	电信光模块周期被误判为 AI 数通；ASP 下行	第一优先级
华工科技 000988	Optical modules / laser / components；Coherent、Lumentum、Fabrinet	光模块、激光加工、传感	年报光通信业务拆分、800G/1.6T 产品、IR、官网规格	光通信收入、毛利率、客户集中、研发、存货	激光加工/传感业务混入；AI 数通暴露需拆分	第一优先级
剑桥科技 603083	Optical transceiver / datacom；Fabrinet、Coherent、Lumentum	光模块、通信设备	年报数通产品、客户集中、IR、官网产品规格	光模块收入、毛利率、客户集中、存货、现金流	单客户/价格战；800G 订单可持续性需验证	第一优先级
中瓷电子 003031	Optical components / ceramic packaging / compound semi candidate；Coherent、IQE、Tower	光通信、电子陶瓷、相关器件候选	年报重组后业务拆分、光通信产品、官网规格、客户认证	光通信收入、毛利率、客户集中、研发	业务结构复杂；AI 数通/硅光直接证据不足	第二优先级
铭普光磁 002902	Optical module / magnetics；Coherent、Lumentum、TE Connectivity	光模块、磁性器件	年报光通信产品、客户、IR、官网规格	光通信收入、毛利率、存货、客户集中	通信/电源磁件混入；低毛利和概念化风险	观察
烽火通信 600498	Data center networking / optical comm；Cisco、Nokia、Arista、Ciena	通信设备、光通信系统	年报数通/云网业务、AI 数据中心客户、官网产品	通信设备收入、毛利率、订单、存货	运营商 CapEx 周期，不等于 AI cluster 网络	观察
亨通光电 600487	Fiber / optical cable / DC connectivity；Corning、Prysmian、TE Connectivity	光纤光缆、海缆、通信连接	年报光通信产品、数据中心客户、官网规格	光通信收入、毛利率、订单、海外收入	光纤不是 800G/1.6T 核心瓶颈；传统光纤周期	观察
长飞光纤 601869	Fiber / optical cable；Corning	光纤光缆	年报光纤光缆收入、数据中心应用、客户	光纤收入、价格、毛利率、库存	AI cluster 主要瓶颈在高速模块/激光/DSP，不是普通光纤	观察
紫光股份 000938	AI networking / data center switch；Arista、Cisco、Broadcom、NVIDIA Spectrum-X	数据中心网络、服务器、IT 基础设施	年报 H3C 业务、数据中心交换机/服务器、IR、官网产品	网络设备收入、服务器收入、毛利率、订单、存货	集成/渠道属性强；AI 网络芯片利润池可能不在公司	第二优先级
中兴通讯 000063	Data center networking / optical / server；Cisco、Arista、Nokia	通信设备、服务器、数据中心网络	年报运营商/政企业务拆分、数据中心产品、IR	政企业务收入、毛利率、订单、研发、海外收入	运营商设备周期、地缘/合规风险；AI 暴露需拆分	第二优先级
星网锐捷 002396	Ethernet switch / enterprise networking；Cisco、Arista	网络设备、交换机	年报企业网络/数据中心产品、客户、官网规格	网络设备收入、毛利率、存货、订单	园区/企业网络不等于 AI scale-out fabric	观察
立讯精密 002475	High-speed interconnect / cable / connector；Amphenol、TE Connectivity、Molex	连接器、线束、系统制造候选	年报通信/数据中心业务拆分、客户、官网产品规格	通信互连收入、毛利率、客户集中、CapEx	消费电子占比高；AI server interconnect 证据需原文确认	第二优先级
沃尔核材 002130	High-speed copper / cable / thermal materials candidate；Amphenol、TE、Credo AEC ecosystem	高速线缆、热缩/材料候选	年报高速通信线缆产品、客户认证、官网规格、IR	高速线缆收入、毛利率、订单、研发	概念映射风险；需证明高速铜缆进入 AI 数据中心	第二优先级
新亚电子 605277	High-speed cable / wire harness；Amphenol、TE、Molex	线缆、数据传输线材候选	年报数据传输产品、客户、官网规格	数据线缆收入、毛利率、客户集中、存货	普通线材不等于 AEC/DAC 高速互连瓶颈	观察
工业富联 601138	AI server ODM / rack-scale systems；Foxconn、Quanta/QCT、Wiwynn、Celestica	AI 服务器、云计算设备、系统制造	年报云计算收入、AI 服务器/机柜产品、客户、IR	云计算收入、毛利率、存货、客户集中、CapEx	收入大但毛利低；ODM 价值捕获弱于组件瓶颈	第一优先级
浪潮信息 000977	AI server / rack systems；Dell、HPE、Supermicro、Lenovo	服务器、AI 服务器	年报服务器产品、AI 服务器收入口径、客户、IR	服务器收入、毛利率、存货、现金流、客户集中	GPU 供应、低毛利、库存压力；收入弹性不等于利润弹性	第一优先级
神州数码 000034	Cloud / server distribution / IT integration；Oracle、VAR/integrator model	云和 IT 集成、服务器/分销	年报云业务、服务器/算力业务、客户合同、IR	云/信创收入、毛利率、现金流、存货	分销/集成毛利低；AI 基础设施利润池可能不在公司	观察
宝信软件 600845	Data center / industrial cloud；Equinix、Digital Realty、industrial DC operators	IDC、工业软件、云服务	年报 IDC 收入、机柜/MW、客户合同、CapEx	IDC 收入、折旧、利用率、CapEx、现金流	IDC 不是 GPUaaS；工业软件和传统 IDC 混入	第二优先级
数据港 603881	IDC / colocation；Equinix、Digital Realty、GDS	数据中心运营	年报机柜/MW、客户、合同期限、CapEx、折旧	IDC 收入、利用率、折旧、债务、现金流	批发 IDC 客户集中；未必拥有 AI GPU 经济性	第二优先级
云赛智联 600602	Local cloud / data center / IT services；regional cloud / colocation	云服务、数据中心、系统集成	年报云业务、算力/IDC 项目、客户合同、IR	云/IDC 收入、毛利率、CapEx、利用率	地方云/政企 IT 不等于 AI NeoCloud	观察
城地香江 603887	IDC construction / operation；Applied Digital、DataBank	数据中心建设与运营候选	年报 IDC 项目、合同、CapEx、债务、IR	IDC 收入、在建工程、折旧、债务、现金流	建设周期、融资压力、客户履约风险	观察
中贝通信 603220	AI compute services / communication services candidate；Applied Digital、Crusoe、CoreWeave model	通信服务、算力服务候选	年报算力业务、GPU/客户合同、融资租赁、公告	算力收入、CapEx、折旧、债务、利用率、客户集中	“算力租赁”概念风险；GPU 残值和融资成本可能吞噬利润	观察
科华数据 002335	UPS / HVDC / data center power；Vertiv、Eaton、Schneider、Delta	UPS、电源、数据中心电力	年报数据中心电源、UPS/HVDC 产品、订单、官网规格	数据中心电源收入、订单、毛利率、存货、现金流	新能源/储能业务混入；数据中心专用订单需验证	第一优先级
科士达 002518	UPS / data center power；Vertiv、Eaton、Delta	UPS、电源、数据中心基础设施	年报 UPS/数据中心产品、客户、IR、官网规格	UPS 收入、毛利率、订单、存货、现金流	光储业务周期混入；AI 数据中心客户证据不足	第一优先级
英维克 002837	Liquid cooling / thermal management；Vertiv、CoolIT、Boyd、Modine	数据中心温控、液冷、CDU/冷却候选	年报数据中心温控、液冷产品、客户、官网技术资料	数据中心温控收入、液冷占比、毛利率、订单、存货	传统温控/通信机柜混入；液冷渗透率不及预期	第一优先级
佳力图 603912	Data center cooling；Vertiv、Munters、Modine	数据中心空调/机房环境	年报数据中心客户、产品、订单、IR	数据中心温控收入、毛利率、订单、合同负债	传统 IDC 空调项目制；不一定受益高功率液冷 rack	观察
麦格米特 002851	Rack power / power modules candidate；Vicor、MPS、Delta、Lite-On	电源、电力电子、服务器电源候选	年报电源业务拆分、客户认证、官网产品规格	电源收入、毛利率、客户集中、研发、订单	家电/新能源/工业电源混入；AI rack 电源暴露需验证	第一优先级
川润股份 002272	Liquid cooling / thermal systems candidate；CoolIT、Alfa Laval、Boyd	液冷/润滑液压/热管理候选	年报液冷产品、客户、IR、官网资料	液冷收入、毛利率、订单、研发	工业液压/风电等业务混入；AI 数据中心证据弱	观察
盾安环境 002011	Thermal management / HVAC components；Modine、Daikin、Munters	制冷配件、热管理	年报数据中心热管理应用、客户、产品规格	热管理收入、毛利率、订单、客户	家电/汽车/HVAC 周期混入；AI 液冷直接性不足	观察
冰轮环境 000811	Industrial refrigeration / DC cooling candidate；Munters、Alfa Laval、Daikin	工业制冷、数据中心冷却候选	年报数据中心应用、订单、客户、官网产品	制冷收入、毛利率、订单、项目毛利	冷链/工业制冷周期，不一定是 AI 数据中心	观察
特变电工 600089	Transformer / grid / HVDC；Siemens Energy、Hitachi Energy、ABB	变压器、电力设备、输变电	年报输变电业务、海外订单、数据中心客户、公告	输变电收入、订单、毛利率、CapEx、现金流	电网/新能源周期为主；AI 数据中心订单需拆分	第一优先级
中国西电 601179	Transformer / switchgear / HVDC；Hitachi Energy、ABB、Siemens Energy	高压开关、变压器、电力设备	年报产品订单、客户、交付周期、公告	电力设备收入、订单、毛利率、合同负债	国网周期受益，不等于 AI 数据中心瓶颈	第一优先级
平高电气 600312	Switchgear / substation；ABB、Schneider、Eaton	高压开关、成套电气	年报开关设备订单、客户、公告、官网规格	开关设备收入、订单、毛利率、合同负债	主要跟随电网投资；数据中心客户需证明	第二优先级
许继电气 000400	HVDC / grid automation / switchgear；ABB、Siemens Grid	电网自动化、直流输电、配电	年报业务拆分、订单、客户、公告	电网设备收入、订单、毛利率、现金流	电网自动化周期，不是数据中心专属设备	第二优先级
思源电气 002028	Switchgear / substation / transformer components；Eaton、Schneider、ABB	输配电设备、变电站设备	年报产品分类、海外/国内订单、客户、官网规格	电力设备收入、订单、毛利率、海外收入	通用电力设备；AI DC 暴露需客户/项目证明	第一优先级
华明装备 002270	Transformer tap changer；Reinhausen、Hitachi Energy ecosystem	变压器分接开关、装备	年报分接开关收入、海外客户、产能、官网规格	分接开关收入、毛利率、海外收入、订单	变压器上游零部件，不等于数据中心直接供应	第一优先级
望变电气 603191	Transformer / electrical steel candidate；ABB、Siemens Energy ecosystem	变压器、取向硅钢/电气设备候选	年报业务拆分、客户、产能、公告	变压器/硅钢收入、毛利率、库存、订单	大宗材料/普通变压器周期；AI DC 项目证据不足	观察
伊戈尔 002922	Power transformer / power supply；Delta、Vicor、Eaton	电源变压器、电力电子	年报高频电源/服务器电源应用、客户、官网规格	电源收入、毛利率、客户集中、订单	光伏/工业电源混入；AI 数据中心订单需验证	第二优先级
白云电器 603861	Switchgear / power distribution；Eaton、Schneider	成套开关、电力配电	年报数据中心客户/项目、产品、订单	配电设备收入、订单、毛利率、合同负债	项目制和电网周期；AI 数据中心占比可能低	观察
国电南瑞 600406	Grid automation / power dispatch；Siemens Grid、ABB Grid	电网自动化、保护控制	年报电网自动化业务、调度/保护产品、订单	电网自动化收入、毛利率、订单、现金流	AI 数据中心用电增长未必直接转化为公司利润	第二优先级
兆易创新 603986	Storage supercycle / DRAM, NOR；Micron、Samsung、SK hynix	存储芯片、MCU	年报存储产品、DRAM/NOR收入、客户、库存	存储收入、毛利率、ASP、库存、CapEx	没有 HBM 纯暴露；普通存储周期反转被误判为 AI	观察
德明利 001309	eSSD / controller / storage module candidate；Phison、Silicon Motion、Pure Storage ecosystem	存储模组、SSD 候选	年报企业级 SSD/数据中心产品、客户、IR	SSD 收入、企业级占比、毛利率、库存、NAND 价格影响	消费 SSD/NAND 周期；企业级 AI 存储证据不足	观察
士兰微 600460	Power semis for rack power；Infineon、ST、Onsemi	功率半导体、模拟/分立器件	年报功率器件客户、数据中心电源设计、官网规格	功率器件收入、毛利率、产能、库存	EV/工业/消费功率周期，不一定进入 AI 电源	观察
斯达半导 603290	IGBT / SiC modules；Infineon、Mitsubishi Electric	IGBT/SiC 功率模块	年报应用领域、数据中心电源/UPS客户、官网规格	功率模块收入、毛利率、客户、库存	新能源车/光伏为主；AI 数据中心设计导入需证明	观察
新洁能 605111	MOSFET / power discrete；Infineon、Onsemi	MOSFET、功率器件	年报应用领域、服务器电源客户、官网规格	功率器件收入、毛利率、库存、产能	通用功率器件，议价权弱；AI 直接性低	观察
三安光电 600703	Compound semi / SiC / GaN / photonics candidate；IQE、Aixtron、Coherent ecosystem	化合物半导体、LED、SiC/GaN	年报化合物半导体收入、客户、产能、官网规格	化合物半导体收入、毛利率、CapEx、库存	LED 周期/新能源混入；硅光/InP/AI 数据中心证据不足	观察
闻泰科技 600745	Power discrete / Nexperia / electronics manufacturing；Nexperia、Onsemi	半导体分立器件、ODM	年报半导体产品应用、客户、地缘风险披露	半导体收入、毛利率、库存、现金流	消费电子和地缘风险；AI 数据中心暴露较弱	观察
2. 按海外瓶颈分层的候选池

项目原框架把第一优先级放在 HBM 设备/测试/材料/基板、光互连/CPO/InP/硅光、电力设备/液冷/热管理、Custom ASIC/scale-up connectivity，以及非美材料/设备隐形供应链；第二优先级包括 eSSD/enterprise NAND/controller、NeoCloud/AI data center developers、AI server ODM。

2026-05-12-ai-super-cycle-resea…

海外瓶颈层	海外主线 / 对应公司	A股主板候选池	研究判断
HBM / HBM equipment / testing / materials	SK hynix、Samsung、Micron；TOWA、ASMPT、BESI、SUSS、Advantest、Teradyne、Resonac、Ajinomoto	深科技、北方华创、至纯科技、雅克科技、江化微、有研新材、金海通、文一科技	A股主板没有纯 HBM 龙头，更多是二阶设备/材料/封测线索；必须严查是否只是国产半导体设备/材料 beta
CoWoS / 2.5D / OSAT / inspection	TSMC、ASE、Amkor、Ibiden、Shinko、Unimicron、Camtek、Nova	长电科技、通富微电、华天科技、深南电路、兴森科技	OSAT/基板是主板里相对可映射的方向；但 CoWoS/HBM 直接订单不能假设
AI server PCB / CCL / high-speed switch PCB	Unimicron、Nan Ya PCB、Kinsus、AT&S、Tripod、ITEQ	沪电股份、深南电路、生益科技、兴森科技、华正新材、景旺电子、奥士康、博敏电子	主板最清晰的映射之一；核心核验是 AI server / switch PCB 收入、低损耗材料、高层数板、毛利率是否同步改善
800G / 1.6T / CPO / optical components	Coherent、Lumentum、Broadcom、Marvell、Fabrinet、MACOM、Corning	光迅科技、华工科技、剑桥科技、中瓷电子、铭普光磁、烽火通信、亨通光电、长飞光纤	需严格区分 datacom 与 telecom；普通光纤/电信设备不能直接等同 AI 光互连
Scale-up fabric / custom ASIC / PCIe-CXL / high-speed interconnect	NVIDIA、Broadcom、Marvell、Astera、Credo、Arista、Cisco、Amphenol、TE	紫光股份、中兴通讯、星网锐捷、立讯精密、沃尔核材、新亚电子	A股主板缺少 CXL/retimer/SerDes 纯标的，更多是网络设备、连接器、线缆和系统集成
AI server / rack-scale systems	Supermicro、Dell、HPE、Quanta、Wiwynn、Foxconn、Celestica	工业富联、浪潮信息、神州数码	营收弹性可能大，但 ODM/集成毛利低；要看毛利率、存货、客户集中和现金流
AI data center power / UPS / HVDC / transformer / switchgear	Vertiv、Schneider、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta	科华数据、科士达、麦格米特、特变电工、中国西电、平高电气、许继电气、思源电气、华明装备、望变电气、伊戈尔、白云电器、国电南瑞	电力设备是真实物理瓶颈候选，但要区分电网投资、出口周期和数据中心专用订单
Liquid cooling / thermal management	Vertiv、CoolIT、Boyd、Munters、Alfa Laval、Modine、Daikin	英维克、佳力图、川润股份、盾安环境、冰轮环境	高功率 rack 是强逻辑，但 A股很多公司仍需证明液冷收入、客户和产品规格
NeoCloud / GPUaaS / AI DC economics	CoreWeave、Nebius、Crusoe、Applied Digital、IREN、Oracle	宝信软件、数据港、云赛智联、城地香江、中贝通信	重资产、融资、利用率和客户信用是核心；不能只看“算力”字样
Storage supercycle	HBM 最纯；server DRAM 次之；eSSD/NAND 需拆周期	兆易创新、德明利、深科技	项目 checklist 已要求拆分 HBM、服务器 DRAM、eSSD、NAND，普通 NAND/SSD 周期不能直接当 AI 结构性增长。

research-checklist

3. 每家公司最需要核验的原始出处

上表已逐家公司列出原始出处类型。按核验优先级，可以统一落成下面的检查模板。项目 checklist 要求每个研究对象先写一句话假设、确定产业链位置、找真实客户和订单证据，再看毛利率、ASP、backlog、CapEx、FCF 和反证。

research-checklist

公司类型	第一原始出处	第二原始出处	必须回到原文看的字段
PCB / CCL	年报“主营业务/产品/行业/客户”章节	IR 活动记录、官网产品规格	AI server / switch / high-speed material 收入是否单列；Dk/Df、层数、HDI/高频高速板；毛利率、产能、存货
光模块 / 光器件	年报光通信业务拆分	官网 800G/1.6T 产品页、IR 记录	datacom vs telecom；800G/1.6T 出货；客户集中；ASP/毛利率；CPO/LPO 是否只是研发
OSAT / 先进封装	年报封装产品结构	募投公告、官网先进封装技术资料	HPC/AI/先进封装收入；CoWoS-like / 2.5D / SiP / FCBGA 具体能力；CapEx、良率、客户
半导体材料 / 设备	年报产品收入和客户	官网产品规格、产能公告	是否进入 HBM/CoWoS/DRAM/foundry 客户；订单/合同负债；毛利率；认证周期
电力设备 / UPS / HVDC	年报产品与行业订单	重大合同公告、官网规格	数据中心客户/订单是否单列；backlog、book-to-bill、交期；毛利率；原材料成本
液冷 / 热管理	年报数据中心温控拆分	官网 CDU/cold plate/液冷产品页、IR	液冷收入、客户认证、rack kW 支持能力；毛利率；订单持续性
AI server / ODM	年报云计算/服务器业务	客户合同、IR、官网产品页	AI server/rack 收入占比；毛利率；存货；客户集中；GPU 供应约束
IDC / NeoCloud	年报 IDC/云收入	项目公告、客户合同、融资公告	MW/机柜、利用率、CapEx、折旧、债务、合同期限、客户信用
存储 / SSD / 控制器	年报存储产品拆分	客户/产品规格、IR	HBM/server DRAM/eSSD/consumer SSD 分拆；ASP、库存、毛利率；企业级客户
4. 每家公司最可能的误判点：横向反证框架
误判类型	最容易落入的公司池	需要的反证
把普通周期反转当成 AI 结构性增长	兆易创新、德明利、江化微、有研新材、华正新材、景旺电子、奥士康、博敏电子	收入和毛利率改善是否来自 AI 服务器/HBM/企业级客户，而不是库存去化、价格反弹或消费电子复苏
把低毛利集成当成瓶颈利润池	工业富联、浪潮信息、神州数码、紫光股份、中兴通讯、星网锐捷	收入增长是否传导到毛利率、经营现金流和 ROIC，而不是只增加存货和营收
把电网设备泛受益当成数据中心直接订单	特变电工、中国西电、平高电气、许继电气、思源电气、华明装备、国电南瑞	订单是否来自数据中心/云厂商/变电站扩容，而不是常规国网/南网/UHV 周期
把电信光通信当成 AI datacom	光迅科技、华工科技、剑桥科技、铭普光磁、烽火通信、亨通光电、长飞光纤	datacom 800G/1.6T 是否单列；客户是否为云厂商/数通客户；毛利率是否抗价格战
把普通散热当成高功率液冷	英维克、佳力图、川润股份、盾安环境、冰轮环境	是否有 CDU、cold plate、液冷机柜客户和收入；风冷项目不能直接归入 AI 液冷
把“算力租赁”当成 NeoCloud	数据港、云赛智联、城地香江、中贝通信、宝信软件	GPU 规模、利用率、客户合同、融资成本、折旧政策、GPU 残值是否可核验
把材料/设备国产替代当成 HBM/CoWoS 直接瓶颈	北方华创、至纯科技、雅克科技、江化微、有研新材、金海通、文一科技	客户是否与 HBM、先进封装、AI 芯片测试相关；订单和产品规格是否能对应海外瓶颈

项目 checklist 的反证要求包括：客户 CapEx 放缓、HBM 供给过剩、GPU 供给放开、云厂商 ASIC 成功、毛利率不提升、客户换供应商等路径。

research-checklist

5. 第一优先级、第二优先级、观察 / 排除池

这里的“优先级”是原始出处核验优先级，不是买卖优先级。

第一优先级：最值得先做原文核验
方向	公司
AI server PCB / high-speed switch PCB / CCL	沪电股份、深南电路、生益科技、兴森科技、华正新材
先进封装 / OSAT	长电科技、通富微电
光互连 / 800G / 1.6T	光迅科技、华工科技、剑桥科技
AI server / rack-scale	工业富联、浪潮信息
UPS / 数据中心电源 / rack power	科华数据、科士达、麦格米特
液冷 / 数据中心温控	英维克
变压器 / 开关 / 输配电	特变电工、中国西电、思源电气、华明装备

第一优先级的共同点：海外瓶颈清晰、A股主板有相对可落地的产品映射、财报和 IR 中较可能找到可核验字段。仍然必须先原文核验，不能直接升级为结论。

第二优先级：有映射，但需要更多拆分
方向	公司
封测 / 存储封测	华天科技、深科技
半导体设备 / 材料 / 高纯系统	北方华创、至纯科技、雅克科技、江化微、有研新材、金海通
PCB / CCL 补充	景旺电子、奥士康、博敏电子
网络设备 / 数据中心交换	紫光股份、中兴通讯
高速连接 / 铜缆 / 线材	立讯精密、沃尔核材、新亚电子
IDC / 数据中心	宝信软件、数据港
电力设备补充	平高电气、许继电气、国电南瑞、伊戈尔

第二优先级的共同点：业务和海外瓶颈有关，但 AI 直接收入、客户、产品规格或毛利传导不够清晰。

观察池：先防误判，后看证据
类型	公司
光通信但可能偏电信/普通光纤	铭普光磁、烽火通信、亨通光电、长飞光纤
数据中心/算力概念但经济模型待证	云赛智联、城地香江、中贝通信、神州数码
液冷/热管理泛化风险	佳力图、川润股份、盾安环境、冰轮环境
存储周期误判风险	兆易创新、德明利
功率半导体泛化风险	士兰微、斯达半导、新洁能、三安光电、闻泰科技
封装设备概念风险	文一科技
排除池：相关但不符合“非创业板、非科创板”条件

这些公司可能与 AI Infra 相关，但因板块规则先排除，不纳入主板表：中际旭创 300308、新易盛 300502、天孚通信 300394、胜宏科技 300476、精测电子 300567、长川科技 300604、申菱环境 301018、高澜股份 300499、润泽科技 300442、奥飞数据 300738、江波龙 301308、澜起科技 688008、华峰测控 688200、中微公司 688012、拓荆科技 688072、盛美上海 688082、华海清科 688120、源杰科技 688498、德科立 688205、佰维存储 688525。

6. 下一步原始出处核验清单
核验步骤	具体动作	产出
1. 主板身份复核	用上交所/深交所股票列表复核代码、板块、简称、风险警示状态	剔除非主板、退市风险或代码变更公司
2. 年报初筛	每家公司抓最近年报、半年报、一季报	主营结构、产品分类、客户集中、毛利率、CapEx、存货、现金流
3. IR 记录核验	搜索“投资者关系活动记录表”中 AI server、800G、1.6T、液冷、数据中心、先进封装、HBM、企业级 SSD 等关键词	只能作为管理层线索，不能替代财报数字
4. 官网技术资料	查公司官网产品规格：高速 PCB、低损耗 CCL、800G/1.6T 模块、CDU、UPS/HVDC、变压器、开关柜、封装设备等	确认产品是否能技术上对应海外瓶颈
5. 重大合同 / 募投 / 产能	查交易所公告中的项目投资、产线扩张、重大合同、客户集中、关联交易	判断是否有可持续订单和扩产约束
6. 海外瓶颈交叉核验	回到 NVIDIA、TSMC、SK hynix、Micron、Broadcom、Coherent、Lumentum、Vertiv、Eaton、Schneider、IEA/DOE 等原文	确认海外瓶颈是否仍存在，避免 A股映射建立在过期假设上
7. 财务传导验证	对每家公司建证据卡：原文已证明 / 合理推论 / 待核验 / 主要反证	不把收入增长直接等同于利润池迁移
8. 分池更新	用 1–5 分评分：AI 需求相关度、供给瓶颈、议价权、持续性、财务传导、技术护城河、反证清晰度等	第一优先级 / 第二优先级 / 观察 / 剔除动态更新

下一步最建议先核验三组，因为它们最容易形成“海外瓶颈 → A股主板产品 → 财报指标”的闭环：
沪电股份、深南电路、生益科技、兴森科技；光迅科技、华工科技、剑桥科技；科华数据、科士达、英维克、麦格米特、特变电工、中国西电、思源电气、华明装备。
