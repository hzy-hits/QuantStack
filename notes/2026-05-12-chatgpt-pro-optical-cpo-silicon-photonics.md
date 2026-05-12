# ChatGPT Pro 输出草稿：800G / 1.6T / CPO / Silicon Photonics

状态：ChatGPT Pro 输出草稿，待原始出处逐条核验  
捕获时间：2026-05-12 12:18:38 CST  
ChatGPT URL：https://chatgpt.com/g/g-p-6a0288a9fb1881919a965d3f9364be88-ai-super-cycle/c/6a02a4c5-2638-83ea-82d7-40b233dc8d21  
项目：ai super cycle

## 使用边界

- 本文件是 ChatGPT Pro 对项目研究任务的输出草稿。
- 不作为投资建议、买卖建议或已验证结论。
- 所有涉及财报数字、订单、backlog、CapEx、产能、价格、客户关系、技术路线的内容，必须回到公司原始披露或权威原文核验后才能进入正式研究结论。
- 本文件保留模型输出中的可疑点，后续核验时应优先处理。

## 原始提问

```text
主题4：800G / 1.6T / CPO / Silicon Photonics 光互连研究。

请基于本项目已上传的 md 来源文件，研究 AI cluster 是否让光互连从通信周期转为数据中心算力周期，并拆解 800G、1.6T、3.2T、CPO、LPO/LRO、silicon photonics、InP laser、external light source。

严格要求：
1. 先列出需要核验的公司原始出处，不要直接把媒体/模型记忆当结论；
2. 区分“原文已证明 / 合理推论 / 待原文核验 / 主要反证”；
3. 不给买卖建议，不给目标价；
4. 不要把所有光模块或通信公司都泛化为 AI Infra 受益，必须拆 datacom、telecom、AI cluster、CPO、laser/InP/SiPho 的真实证据。

重点公司：
Coherent、Lumentum、Broadcom、Marvell、Credo、MACOM、Semtech、InnoLight、中际旭创、Eoptolink、新易盛、Fabrinet、Sivers、IQE、Tower、GlobalFoundries、Ayar Labs、POET、Corning。

输出：原始出处清单、光互连技术路线、各环节瓶颈和供应商、非美小盘候选、CPO 延后或 pluggable 延寿的反证、财报验证指标。
展开
```

## ChatGPT Pro 输出

根据 2026-05-12 的项目 md，主题4已经被定义为“800G → 1.6T → CPO 光互连”，需要验证的问题是 AI cluster 是否让光模块从通信周期变成数据中心算力周期，以及 CPO 何时替代 pluggable；项目文件同时明确要求先列原始出处、区分事实/推论/待核验/反证、不推荐买卖也不给目标价。

2026-05-12-ai-super-cycle-resea…

 项目 checklist 还要求涉及收入、订单、backlog、ASP、产能、毛利率、客户关系等数据时，必须优先回到年报、10-K/20-F、季报、earnings call、investor presentation、交易所公告和公司技术资料，媒体和模型输出只能作为线索。

research-checklist

1. 必须先核验的公司原始出处清单
公司 / 主体	必须核验的原始出处	重点核验指标	当前证据状态
Coherent	10-K/annual report、OFC investor deck、InP/CPO/OCS 产品发布、earnings call	datacom revenue、800G/1.6T mix、InP/EML/CW laser 产能、ELS/CPO 订单、客户集中度、毛利率	官方技术披露已证明其 InP 组合覆盖 400mW CW laser for CPO/SiPh pluggables、200G EML for 1.6T、D-EML for 3.2T+；收入和订单仍需财报核验。
Coherent Inc

Lumentum	10-K、Cloud & Networking segment 披露、datacom product pages、earnings call	Cloud & Networking 收入、EML/DML/SiPh 产品 mix、1.6T module 出货、Japan InP fab utilization	官方产品页已证明其 1.6T OSFP 模块面向 AI/cloud data centers，8 electrical/8 optical lanes、212.5Gbps PAM4/lane、SiPh + DSP、典型 22W；财务贡献待核验。
Lumentum

Broadcom	10-K、semiconductor segment、AI networking/CPO press releases、OFC/analyst materials	switch ASIC、DSP、VCSEL/EML/CWL、CPO design win、AI networking revenue、CPO 是否进量产	官方 CPO 披露已证明 200G/lane CPO 面向 high-radix scale-up/scale-out，目标解决 foundation model 参数增长带来的 bandwidth/power/latency 问题。
Broadcom Inc.

Marvell	10-K、data center segment、optical DSP product release、earnings call	Ara/Nova DSP 出货、1.6T pluggable 客户、TRO/LRO/FRO mix、DSP ASP、custom silicon 与 optics 协同	官方披露称 Ara 1.6T 3nm platform 已 mass volume shipping to global customers，用于 hyperscaler/cloud 1.6T pluggable AI data centers；需用收入、客户和毛利率验证。
Marvell Technology

Credo	10-K/20-F、product release、investor presentation、customer concentration	Optical DSP、LRO、AEC、retimer、AI fabrics revenue、单客户依赖	官方披露 Cardinal 1.6T optical DSP 支持 full retimed 与 LRO，集成 EML/SiPh driver，并面向 AI fabrics；实际量产、客户和毛利需财报核验。
investors.credosemi.com

MACOM	10-K、Data Center 产品资料、laser/TIA/driver 产品发布、earnings call	CW laser、TIA、driver、photodiode、Data Center revenue、InP/GaAs 工艺产能	已有官网产品线索，但需核验 AI/datacom revenue 与 1.6T 真实订单；不能只因有 TIA/laser 就归入 AI 受益。
Semtech	10-K、Signal Integrity 产品发布、earnings call	FiberEdge TIA、MZM driver、LPO/LRO/CPO/NPO 产品收入、HieFo 收购后 laser-driver 能力	官方披露 224Gbps/lane TIA 与 driver 支持 LRO/LPO/XPO/NPO/CPO，目标为 800G/1.6T/3.2T AI/ML clusters 和 hyperscale data centers；财务贡献待核验。
Semtech

InnoLight / 中际旭创	A 股年报、季报、交易所公告、英文官网、客户/供应商交叉披露	800G/1.6T 出货、datacom vs telecom 收入、海外客户、泰国/海外产能、毛利率、库存	官网已说明 AI & Data Center Networking 是核心方案；Tower 官方披露与 InnoLight 扩大 SiPh 合作，面向 400G/800G、1.6T 和未来 3.2T 模块。
innolight.com
+1

Eoptolink / 新易盛	深交所年报/摘要、季报、投资者关系、产品页	800G/1.6T/LPO/LRO/CPO 收入、AI cluster 客户、产能、毛利率、现金流	官方产品页说明 1.6T OSFP 面向 AI clusters、cloud data centers、1.6T Ethernet；2025 年报摘要称 800G、1.6T 及更高速率光互联产品为核心产品与增长引擎，但需用完整年报口径验证利润和现金流。
Eoptolink
+1

Fabrinet	10-K、quarterly release、customer concentration、segment disclosure	optical communications revenue、datacom vs telecom、top customers、Thailand capacity、毛利率	主要是精密制造/代工与模块装配验证；收入增长若不伴随毛利改善，可能只是低附加值产能扩张。
Sivers	年报、interim report、Photonics segment、ELS/CW laser press releases	DFB/CW laser array 订单、CPO/ELS 客户、non-recurring vs recurring revenue、现金流	官方披露与 O-Net、Enablence 合作开发使用 Sivers laser arrays 的 ELS，用于 AI datacenters/HPC 的 CPO 架构；商业订单、规模和毛利仍待核验。
Sivers Semiconductors

IQE	annual report、interim results、Photonics revenue、InP data communications commentary	InP photonics revenue、capacity utilization、AI datacom 是否抵消 wireless/defense 周期	官方 H1 2025 披露 Photonics revenue £26.6m 基本持平，InP data communications for AI 表现较强但被其他业务拖累；这是“局部 AI 受益 + 公司整体仍有周期压力”的典型。
iqep.com

Tower	20-F、6-K、SiPho foundry product pages、NVIDIA/InnoLight 合作公告	SiPho revenue、capacity reservation、1.6T module 客户、gross margin、capex	官方披露与 NVIDIA 推进 AI infrastructure 1.6T optical modules，并称 SiPh 可提升数据率；还披露与 InnoLight 的 SiPho 方案进入 high-volume ramp。
Tower Semiconductor
+1

GlobalFoundries	20-F、investor deck、GF Fotonix 产品页、packaging/testing capability	GF Fotonix 收入、客户名单、45CLO/SiPh capacity、packaging/test 服务	官方资料称 silicon photonics 面向 data centers 更高 data rate 和 power efficiency，并强调 copper 在功耗、数据率、带宽上受限；财务分部贡献待核验。
GlobalFoundries

Ayar Labs	官网产品页、客户/投资方公告、技术白皮书	SuperNova ELS、TeraPHY optical I/O、客户验证、是否进入量产	官方称 SuperNova ELS 提供 up to 16 wavelengths、powering up to 16 ports，并与 TeraPHY 组合相对传统 pluggable optics + electrical SerDes 有带宽/延迟/能效优势；仍属私营公司，收入不可公开验证。
Ayar Labs

POET	20-F/40-F、Optical Interposer 产品页、客户订单、press releases	optical engine、interposer、800G/1.6T 设计导入、收入转化	官网仅能证明技术方向，必须核验 purchase orders、shipment、cash burn 和客户集中。
Corning	10-K、Optical Communications segment、NVIDIA/Meta 合作公告、capex	optical fiber/cable/connectivity 容量、AI datacenter 订单、长约、毛利率	官方与 NVIDIA 公告称扩产供应 hyperscale data centers 的 optical connectivity，用于 NVIDIA-accelerated computing at scale；这是 fiber/connectivity 进入 AI Infra 供应约束的强线索。
康宁
2. 初步判断：光互连是否从通信周期转为数据中心算力周期？

**原文已证明：**项目 md 已把“网络互连 / 光互连”放在 token 需求、算力集群、GPU/ASIC/HBM 之后的 AI Infra 物理链条中，并把 800G/1.6T ports、power/bit、CPO design win、hyperscaler qualification、fiber/connector backlog 作为关键验证指标。

2026-05-12-ai-super-cycle-resea…

 官方来源也已证明，Broadcom、Marvell、Lumentum、Semtech、Coherent、Tower、Sivers、Corning 等公司都在把 1.6T、CPO、SiPh、ELS、TIA/driver 或 optical connectivity 明确绑定到 AI datacenter / AI cluster / hyperscale 场景。
康宁
+6
Broadcom Inc.
+6
Marvell Technology
+6

**合理推论：**光互连正在从“电信运营商 capex + 通信设备周期”扩展为“AI cluster 算力基础设施周期”的一部分，但这不是所有光通信公司都自动升级为 AI Infra。真正进入 AI 算力周期的，是能被 GPU/ASIC cluster 的东西向流量、scale-out / scale-up fabric、rack-to-rack optical links、1.6T pluggable、LPO/LRO/TRO、CPO/ELS 和 SiPh 需求直接验证的环节。传统 telecom、5G fronthaul/backhaul、PON、metro/long-haul coherent 恢复，不能直接归为 AI cluster 受益。

**待原文核验：**每家公司 AI 相关收入占比、800G/1.6T 出货量、ASP、客户集中、hyperscaler 长单、laser/InP wafer capacity、CPO design win 是否可转收入、毛利率是否改善。项目 checklist 明确要求，收入增长必须与订单、客户、价格、产能和毛利率交叉验证，不能只看“AI opportunity”表述。

research-checklist

**主要反证：**CPO 量产延后、pluggable optics 生命周期拉长、LPO/LRO/TRO 继续降低功耗、铜缆/AEC 在短距连接中延寿、optics ASP 快速下行、AI capex 放缓、客户自研或内化 switch/NIC/optics、模块厂毛利率不升反降。项目 md 已把“CPO 量产延后、pluggable optics 生命周期更长、价格战、DSP 被 LPO/CPO 挤压、可靠性/可维护性问题”列为主题4核心反证。

2026-05-12-ai-super-cycle-resea…

3. 光互连技术路线：800G → 1.6T → 3.2T → CPO/Optical I/O
3.1 800G：当前主流放量层，仍以 pluggable 为核心

800G 的核心不是“新概念”，而是 AI cluster 规模扩大后，leaf-spine、spine-super spine、GPU/accelerator rack 间连接需要更高 port bandwidth。验证重点不是公司是否能做 800G，而是 800G 是否进入 AI datacenter 批量收入、毛利率是否高于传统 telecom、客户是否为 hyperscaler / AI cluster。

供应商分层：
模块：InnoLight / 中际旭创、Eoptolink / 新易盛、Coherent、Lumentum、Fabrinet 代工链。
DSP / retimer / SerDes：Broadcom、Marvell、Credo。
器件：Coherent、Lumentum、MACOM、Semtech。
光纤连接：Corning 等。

**状态判断：**800G 是“原文可验证空间最大”的阶段，因为产品、客户、收入、产能和毛利率最容易从财报中看到。不能把只有 telecom 800G coherent 或 DCI 800G 的公司，直接等同于 AI cluster 800G pluggable 受益。

3.2 1.6T：AI cluster datacom 进入第二主线

1.6T 的逻辑是 200G/lane、电气/光学接口升级、模块功耗下降与 switch radix 提升。Marvell 官方称 1.6T Ara 平台已 mass volume shipping to global customers，用于 hyperscaler/cloud 1.6T pluggable AI data centers；Lumentum 官方 1.6T OSFP 产品页也将其定位在 AI/cloud data centers，并给出 212.5Gbps PAM4/lane、SiPh + DSP 和 22W 典型功耗。
Marvell Technology
+1

供应商分层：
模块：Eoptolink、新易盛、InnoLight、中际旭创、Coherent、Lumentum。
DSP：Marvell、Broadcom、Credo。
TIA/driver：Semtech、MACOM、Coherent、Lumentum。
laser/EML/CW：Coherent、Lumentum、Sivers、MACOM。
SiPh foundry / PIC：Tower、GlobalFoundries、可能还有其他私有生态。

**状态判断：**1.6T 是本主题最关键的财报验证点。若 1.6T revenue、800G/1.6T mix、datacom gross margin、客户预付款/长单同步改善，光互连从通信周期向 AI cluster 算力周期迁移的证据会显著增强。

3.3 3.2T：更接近技术路线验证，而非多数公司当前收入主线

3.2T 当前更适合作为技术路线和研发资本开支指标，不应过早当作收入结论。Semtech 官方已经把 224Gbps/lane TIA/driver 的适用范围写到 800G、1.6T 和 3.2T optical engines；Coherent 官方披露 D-EML 使能 400G/lane performance for emerging pluggables at 3.2T and beyond。
Semtech
+1

**关键问题：**3.2T 会继续走 pluggable、NPO/XPO，还是加速 CPO？如果 3.2T 仍能通过高密度 pluggable/XPO/NPO 延续，那么 CPO 替代节奏会后移；如果 front-panel density、power/bit、热设计和可维护性过不了，CPO/ELS 的价值会提高。

3.4 LPO / LRO / TRO：pluggable 延寿的核心变量

LPO 移除模块内 DSP，目标是降低功耗和延迟，但链路预算、互操作性、诊断与系统级稳定性更难。LRO / TRO 是折中路线：保留部分 retiming，以减少功耗但保留可部署性。Credo 官方明确其 1.6T DSP 支持 full-retimed 和 LRO；Semtech 官方把 224G TIA/driver 定位于 LRO、LPO、XPO、NPO、CPO 全链路。
investors.credosemi.com
+1

结论分层：
原文已证明：LRO/LPO/TRO 已是供应商产品路线。
合理推论：它们会延长 pluggable optics 生命周期，构成 CPO 延后反证。
待核验：LPO/LRO 是否在 hyperscaler 大规模 production 网络稳定运行，以及是否真正改善 TCO。
主要反证：若 link flap、interop、diagnostics、field service 问题高于预期，客户可能回到 full DSP / TRO 或转向 CPO。

3.5 CPO / NPO / XPO：不是简单替代 pluggable，而是功耗密度瓶颈下的架构转移

Broadcom 官方 CPO 披露把 200G/lane CPO 直接绑定 high-radix scale-up/scale-out networks，并强调 scale-up domains exceeding 512 nodes、bandwidth/power/latency challenges。
Broadcom Inc.
 这说明 CPO 更可能先在极高带宽密度、低延迟、功耗受限的 AI fabric 中出现，而不是一次性替代所有 pluggable optics。

**CPO 的真实瓶颈：**光引擎与 switch/xPU 封装、光纤 attach、热设计、可靠性、现场维护、ELS 可替换性、系统级测试、良率、生态标准。CPO 会把价值从“模块外壳 + 前面板”迁移到 optical engine、ELS、SiPh PIC、InP CW laser、FAU、微透镜、封装测试和系统集成。

3.6 Silicon Photonics / InP laser / ELS：CPO 与 1.6T/3.2T 的深水区

GF 官方称 copper 在功耗、数据率和带宽上变得 prohibitive，SiPh 用于 data centers 更高 data rate 和 power efficiency；Tower 官方披露与 NVIDIA 推进 1.6T silicon photonics optical modules；Coherent 官方披露 InP CW laser、EML、photodiode 覆盖 CPO、SiPh pluggables 和 1.6T/3.2T。
GlobalFoundries
+2
Tower Semiconductor
+2

**技术判断：**SiPh 解决调制、波导、集成和规模制造问题，但高质量激光仍常依赖 InP/CW/DFB/EML 等化合物半导体；ELS 的意义是把热敏感 laser 从高热 switch/xPU package 中移出，提高可维护性和波长稳定性。Sivers 官方也明确称 ELS 将 laser 与 high-power processors 的极端热环境隔离，从而改善 wavelength stability、reliability 和 serviceability。
Sivers Semiconductors

4. 各环节瓶颈和供应商
环节	真实瓶颈	供应商 / 候选	证据分层
AI datacom pluggable modules	800G/1.6T 量产良率、功耗、客户认证、海外产能	InnoLight/中际旭创、Eoptolink/新易盛、Coherent、Lumentum、Fabrinet	新易盛/Eoptolink 官方已证明产品面向 AI clusters/cloud data centers；收入、ASP、客户集中需核验。
深证证券交易所
+1

DSP / retimed optics	200G/lane SerDes、功耗、延迟、DSP ASP、先进制程成本	Marvell、Broadcom、Credo	Marvell/Credo 已有 1.6T DSP 官方披露；需核验实际 revenue 和毛利。
Marvell Technology
+1

LPO/LRO/TRO analog chain	模拟信号完整性、系统级稳定性、互操作、诊断	Semtech、Credo、Marvell、MACOM、Coherent	Semtech/Credo 官方已证明 LPO/LRO/TRO 产品路线；是否大规模部署待核验。
Semtech
+1

TIA / driver / CDR / PMD IC	224G/448G lane 线性度、功耗、噪声、封装	Semtech、MACOM、Coherent、Broadcom	Semtech 224G TIA/driver 官方披露覆盖 800G/1.6T/3.2T 与 CPO/NPO；财务贡献待核验。
Semtech

InP EML / DML / CW laser	InP wafer capacity、良率、热稳定、波长稳定、高功率低噪声	Coherent、Lumentum、Sivers、MACOM、IQE 上游外延	Coherent、Sivers、IQE 已有 InP/laser/AI datacom 官方线索；订单与产能仍需核验。
Coherent Inc
+2
Sivers Semiconductors
+2

Silicon Photonics PIC / foundry	低损耗波导、调制器、fiber coupling、PDK、量产良率、packaging/test	Tower、GlobalFoundries、Ayar Labs、POET、Coherent/Lumentum 生态	Tower/GF 官方已证明 SiPh 面向 AI/data center 高速互连；收入和产能锁定需核验。
Tower Semiconductor
+1

External Light Source / ELS	激光阵列、wavelength locking、可替换性、热管理、fiber routing	Ayar Labs、Sivers、Coherent、Lumentum、O-Net/Enablence 生态	Ayar/Sivers 官方已证明 ELS 技术方案；商业量产仍是待核验。
Ayar Labs
+1

CPO optical engine / FAU / lens / PM fiber	封装、光纤 attach、可维护性、系统测试、良率	Broadcom、Coherent、Ayar Labs、POET、Fabrinet、Tower/GF 生态	Broadcom/Coherent/Ayar 官方已证明路线；量产节奏和服务模型是核心反证。
Broadcom Inc.
+2
Coherent Inc
+2

Fiber / cable / connectivity	AI DC 内部 fiber 数量、连接密度、产能、交期	Corning	Corning/NVIDIA 官方已证明 optical connectivity 已成为 AI infrastructure 扩产对象；需看 segment revenue、capex、长约毛利。
康宁
5. 公司拆分：不能把所有光通信公司都泛化成 AI Infra 受益
类型	公司	当前分类	需要特别防止的误判
AI datacom 直接暴露	InnoLight/中际旭创、Eoptolink/新易盛、Coherent、Lumentum	重点验证池	收入来自 800G/1.6T AI datacom 才算强证据；若主要来自 telecom/传统 DCI，不能等同 AI cluster。
AI networking silicon / DSP / switch optics	Broadcom、Marvell、Credo	重点验证池	DSP 可能被 LPO/LRO/CPO 挤压；AI networking 收入需拆 custom ASIC、switch silicon、DSP、optics。
模拟器件 / PMD IC	MACOM、Semtech	重点观察池	有 TIA/driver 不等于有 AI 收入；需验证 1.6T/3.2T design win 转收入。
制造/代工/装配	Fabrinet、Tower、GlobalFoundries	重点观察池	Fabrinet 可能收入增长但毛利不升；Tower/GF 需证明 SiPh revenue 和产能锁定，而不是泛 foundry beta。
非美小盘 / 上游材料器件	Sivers、IQE、POET	高弹性但高待核验池	press release 与技术路线不等于量产收入；必须看订单、现金流、客户集中、产能利用率。
fiber/connectivity	Corning	大盘基础设施验证池	fiber 紧缺是强线索，但需确认 AI data center 合同能否改善 Optical Communications segment 的收入和毛利。
6. 非美小盘候选池：只列研究候选，不构成买卖建议
候选	国家/地区	AI 光互连相关位置	看点	主要反证
Sivers	瑞典	DFB/CW laser array、ELS、CPO 光源	ELS 是 CPO 深水区，官方已披露与 O-Net/Enablence 的 AI datacenter ELS 合作。
Sivers Semiconductors
	是否有量产订单；客户是否只是 demo；现金流和产能是否支撑扩张。
IQE	英国	InP/GaAs 外延片，上游材料	H1 2025 已披露 InP data communications for AI 表现强，但 photonics revenue 基本持平、整体仍亏损。
iqep.com
	无法把 InP AI 增长转成整体利润；wireless/defense 周期拖累；产能利用率不足。
POET	加拿大 / 新加坡生态	Optical Interposer、optical engine、chiplet integration	若 800G/1.6T optical engine 批量导入，可受益于低成本封装集成。	当前更偏技术商业化早期；订单、收入、现金消耗和客户验证必须逐条核验。
Tower	以色列	SiPh foundry、1.6T optical module ecosystem	官方披露与 NVIDIA、InnoLight 合作，1.6T/3.2T SiPh 路线明确。
Tower Semiconductor
+1
	SiPh 收入占比不透明；foundry 业务整体受其他周期影响；客户集中和产能投资回收。
Eoptolink / 新易盛	中国	800G/1.6T pluggable、LPO/LRO/XPO/NPO/CPO 模块	官方披露 800G/1.6T 及更高速率产品为核心增长引擎，产品覆盖 AI cluster。
深证证券交易所
	估值/预期透支、客户集中、海外产能、贸易限制、ASP 下行、毛利率回落。
InnoLight / 中际旭创	中国	高速 datacom transceiver、SiPh 合作生态	官网与 Tower 合作披露支持 AI/DC 400G/800G、1.6T、未来 3.2T。
innolight.com
+1
	大客户依赖、地缘/出口管制、模块价格战、CPO 路线改变利润池。

项目文件也提醒，日韩欧小盘和隐形供应链必须回答它们处于直接瓶颈、二阶受益、周期反转还是主题映射，并且要特别防止“没有明确 AI 客户或 AI 收入拆分”“收入来自传统消费电子/汽车/工业恢复”“毛利率和订单没有改善”等误判。

README

7. CPO 延后或 pluggable 延寿的反证清单

1.6T pluggable 已足够好。 Marvell 官方称 1.6T pluggable connectivity 已用于 hyperscaler/cloud AI data centers；Lumentum 的 1.6T OSFP 产品页显示 full-retimed module 仍可覆盖 up to 500m，典型功耗 22W。若 TRO/LRO/LPO 继续下降到客户可接受功耗，CPO 替代会推迟。
Marvell Technology
+1

LPO/LRO/TRO 改善了 pluggable 的 TCO。 Credo 和 Semtech 的官方路线都覆盖 LRO/LPO/TRO/linear optics；如果系统厂能解决 link integrity、diagnostics 和互操作，pluggable 会继续延寿。
investors.credosemi.com
+1

CPO 可维护性仍未完全证明。 CPO 把 optical engine 靠近 switch/xPU，功耗密度和封装复杂度下降的同时，现场更换、故障隔离、fiber attach、系统测试更难。ELS 可缓解 laser 热和可替换性问题，但 ELS 本身也要证明可靠性、供电、fiber routing 和服务模型。

铜缆/AEC 在短距连接延寿。 机柜内、短 reach 连接如果仍能由 DAC/AEC/ACC 解决，光互连增量会集中在 rack-to-rack、switch-to-switch、scale-out fabric，而不是全域替换。

optics ASP 快速下降。 即使 800G/1.6T 出货增长，如果模块 ASP 跌速高于出货增长、毛利率不提升，模块厂可能只是“量增价跌”的通信周期。

AI cluster 网络没有成为真实 bottleneck。 若 GPU utilization、all-reduce、MoE routing、推理并发等瓶颈被软件/架构缓解，network port 需求增速可能低于预期。

客户内化与供应链重分配。 NVIDIA、Broadcom、hyperscaler 或大型 ODM 若把更多 optics / switch / NIC / CPO 价值内化，独立模块和器件公司利润池可能被压缩。

8. 财报验证指标：未来 4 个季度仪表盘
层级	必看指标	强证据	反证
模块厂	datacom revenue、800G/1.6T revenue mix、gross margin、inventory、customer concentration	800G/1.6T 占比上升，毛利率同步改善，库存周转健康	收入增但毛利率降；库存积压；客户取消/延后
DSP / SerDes / switch optics	optical DSP revenue、AI networking revenue、gross margin、tape-out/design win、advanced-node cost	1.6T DSP/LRO/TRO 出货进入收入，客户多元	LPO/CPO 压低 DSP ASP；大客户自研
TIA / driver / PMD	Signal Integrity / datacenter revenue、224G/448G design win、ASP、attach rate	1.6T/3.2T PMD IC revenue 明确增长	只有 demo，无 revenue；客户转向集成方案
InP / laser / ELS	InP wafer utilization、laser capacity、yield、长单、capex、gross margin	高功率 CW/DFB/EML 长单和产能锁定	产能过剩；良率改善导致单位价值下降；ELS 商业化推迟
SiPh foundry	SiPh revenue、capacity reservation、MPW/prototype 到量产转化、packaging/test 服务收入	1.6T/3.2T 客户进入量产，产能被锁定	SiPh 只是小收入；客户转向其他 foundry；良率问题
CPO / ELS / FAU	CPO design win、ELS shipment、field replaceability、RMA、system validation	从 demo 转 production deployment	CPO 可靠性/维护失败，客户继续使用 pluggable
fiber/connectivity	Optical Communications revenue、AI data center 合同、backlog、capacity expansion、capex 回收	长约、预付款、产能扩张、毛利率改善	只是短期抢货；fiber 价格回落；capex 造成 FCF 压力
9. 结论分层

原文已证明
AI Infra 项目文件已把光互连/CPO/InP/SiPh 列为第一优先级深挖方向，并把 800G/1.6T shipment、InP wafer capacity、laser yield、DSP ASP、power/bit、CPO design win、hyperscaler qualification、fiber/connector backlog 设为关键指标。

2026-05-12-ai-super-cycle-resea…

 Broadcom、Marvell、Lumentum、Semtech、Coherent、Sivers、Tower、GF、Ayar、Corning、Eoptolink 等官方资料均已证明 AI datacenter / AI cluster 正在进入其光互连产品叙事与技术路线。
Eoptolink
+9
Broadcom Inc.
+9
Marvell Technology
+9

合理推论
AI cluster 正在把一部分高速光互连从传统 telecom 周期迁移到 datacom / AI compute cluster 周期。最强传导链是：GPU/ASIC cluster 扩大 → scale-out/scale-up 网络端口增加 → 800G/1.6T pluggable 放量 → 200G/lane DSP/TIA/driver/laser/SiPh 升级 → CPO/ELS 在功耗密度压力下逐步出现。

待原文核验
每家公司是否真正受益，必须回到财报验证：AI datacom revenue、800G/1.6T 出货、毛利率、客户集中、backlog、产能锁定、InP/SiPh/ELS 订单、CPO production deployment。没有这些证据的公司，只能放在“主题跟踪池”，不能进入核心判断。

主要反证
CPO 延后、pluggable 延寿、LPO/LRO 成熟、铜缆/AEC 延寿、optics ASP 下跌、模块厂毛利率不升、AI capex 放缓、客户内化、自研 switch/NIC/optics、地缘限制和库存周期反转。项目 checklist 的处理方式是：结构性增长必须有多年订单、供给瓶颈和客户持续扩张；AI 叙事强但财报弱的，只能放入观察池。

research-checklist
