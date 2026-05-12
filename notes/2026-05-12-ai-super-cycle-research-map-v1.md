# AI Super Cycle 研究地图 v1

状态：研究框架草案，待原始出处逐条核验  
日期：2026-05-12  
项目：ai super cycle / AI Infra 产业链研究

## 0. 使用边界

这份文档用于沉淀 AI Infra super cycle 的研究框架、产业链地图、候选主题和后续 ChatGPT Pro 多会话研究任务。它不是投资建议，也不是买卖建议。

重要约束：

- 文中所有涉及实际数据、财报数字、订单、backlog、CapEx、毛利率、产能、价格、出货、客户关系的内容，先作为“待原文核验线索”。
- 后续进入正式结论前，必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或权威机构原文。
- ChatGPT Pro 输出只作为假设生成和研究拆解，不作为事实证据。
- 企业应用侧的软件模式不作为本项目重点。企业推理、存储、缓存等方向只在它们能转化为真实 AI Infra 需求时研究。

## 1. 核心判断

AI Infra 不是一条线，而是一个多层约束系统：

```text
token 需求
→ 算力集群
→ GPU / ASIC / CPU / DPU
→ HBM / DRAM / NAND / eSSD
→ 网络互连 / 光互连
→ 先进封装 / 测试 / 基板
→ 晶圆制造 / WFE / EDA / IP
→ 材料 / 气体 / 真空 / 洁净室
→ 数据中心 / 机电 / 冷却
→ 电力 / 能源 / 电网
→ 金融 / 监管 / 数据主权
```

真正可能出现指数弹性的环节，往往不是最显眼的模型公司或 GPU 本身，而是被 GPU 集群规模化放大、但供给侧扩产慢、客户认证强、单机价值量上升的瓶颈环节。

研究优先级的核心交叉点：

```text
HBM / CoWoS 设备测试材料
× 光互连 / CPO / InP / 激光
× 电力液冷
× 非美隐形供应链
× 小中市值
```

## 2. 事实基线：先看资本开支和物理瓶颈

AI Infra 已经从“模型发布 / GPU 订单”进入“资本开支 / 物理瓶颈”阶段。正式研究不能只看模型能力，而要同时看：

- 云厂商 CapEx、RPO、云毛利率、折旧和数据中心建设。
- GPU/ASIC 平台对 HBM、CoWoS、网络和电力的绑定。
- 存储、先进封装、光互连、电力设备是否从二阶受益变为一阶瓶颈。
- 数据中心从“买到 GPU”变成“拿到电、散掉热、点亮 rack、形成可用集群”。

当前待核验线索：

| 线索 | 需要核验的原文 |
| --- | --- |
| Microsoft FY26 Q3 cloud revenue、commercial RPO、AI infra 对云毛利率影响 | Microsoft investor relations FY26 Q3 earnings |
| Alphabet Q1 2026 CapEx 与 AI technical infrastructure 拆分 | Alphabet / Google investor relations Q1 2026 |
| Meta 2026 CapEx 指引上调与数据中心成本 | Meta investor relations 2026 guidance |
| AWS Q1 2026 revenue growth | Amazon investor relations Q1 2026 |
| NVIDIA GB300 NVL72 对 HBM3E、800G 网络和整机系统架构的绑定 | NVIDIA GB300 NVL72 product page / architecture materials |
| TSMC CoWoS 扩产和 2028 14-reticle CoWoS 路线 | TSMC annual report / technology symposium / investor materials |
| DRAM / NAND contract price forecast | TrendForce 原文 |
| SK hynix FY2025 业绩与 HBM / AI memory commentary | SK hynix FY2025 results |
| Broadcom CPO、LightCounting 800G/1.6T、IEA/DOE data center power | Broadcom、LightCounting、IEA、DOE 原文 |

## 3. AI Infra 产业链 16 层

### 3.1 LLM / Agent 需求层

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | chatbot、coding agent、enterprise copilot、AI search、AI video、AI voice、workflow agent、robotics / physical AI、sovereign AI |
| 和 infra 的关系 | 训练决定 frontier model 峰值算力需求；推理决定长期 token 流量、KV cache、显存、网络、存储和电力需求 |
| 代表公司 | OpenAI、Anthropic、Google、Meta、xAI、Mistral、Cohere、DeepSeek、Perplexity、Cursor/Anysphere、Harvey、Glean、ServiceNow、Salesforce、Adobe、Microsoft、Databricks |
| 关键指标 | token 量、API 收入、每百万 token 价格、agent loop 次数、上下文长度、推理成本/收入比、企业部署数、推理延迟 |
| 主要反证 | 模型能力提升但商业化慢；token 价格快速下跌；小模型/蒸馏/端侧推理吸收需求；企业 ROI 低；监管或版权限制 |

研究重点：不是从这里直接找 10 倍公开资产，而是跟踪 token 增长速度是否超过单位推理成本下降速度。

### 3.2 AI 软件栈、框架、编译器、推理引擎

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | PyTorch、JAX/XLA、CUDA、ROCm、Triton、TensorRT-LLM、vLLM、SGLang、Ray、Kubernetes、Slurm、Megatron、DeepSpeed、模型压缩、量化、MoE routing、KV cache 管理 |
| 和 infra 的关系 | 软件决定 GPU/ASIC 利用率、推理成本、显存占用、集群稳定性；效率提升既可能刺激需求，也可能降低单位硬件需求 |
| 代表公司 | NVIDIA、AMD、Google、Microsoft、Meta、Databricks、Anyscale、Snowflake、Cloudflare、MongoDB、Elastic、Hugging Face |
| 关键指标 | GPU utilization、MFU、tokens/s/GPU、batch size、prefill/decode 分离效率、KV cache 命中率、推理毛利、单位 token 能耗 |
| 主要反证 | 开源 commoditization；效率提升过快导致硬件需求低于预期；云厂商内部工具不外溢；软件公司捕获价值弱 |

研究重点：推理优化是反身性变量。关键问题是需求弹性是否大于效率提升。

### 3.3 AI 云、NeoCloud、GPU-as-a-Service

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | Hyperscaler AI cloud、GPU 租赁、bare metal、reserved capacity、serverless inference、AI factory、sovereign cloud、GPU-backed financing |
| 和 infra 的关系 | AI 云把 GPU、网络、电力、机房、融资打包成服务，是 AI Infra 的需求聚合器 |
| 代表公司 | AWS、Azure、GCP、Oracle、CoreWeave、Nebius、Lambda、Crusoe、IREN、Applied Digital、Hut 8、Core Scientific、OVHcloud、GDS |
| 关键指标 | GPU fleet、contracted backlog、MW/GW secured power、CapEx、debt/lease liabilities、utilization、GPU depreciation、gross margin、customer concentration、RPO |
| 主要反证 | 融资成本上升；客户集中；GPU 残值下跌；供给过剩；AI labs 违约/延期；电力/机房交付慢；收入增长但 FCF 恶化 |

研究重点：NeoCloud 更像高增长、重资产、项目融资、客户集中的新型基础设施资产。核心验证是 backlog 能否转化为高利用率、高毛利、低违约风险的现金流。

### 3.4 AI 服务器、rack-scale 系统、ODM/OEM

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | HGX/DGX/GB200/GB300 rack、AI server motherboard、power shelf、liquid-cooled rack、management controller、cabling、整机交付 |
| 和 infra 的关系 | GPU 不等于可用算力。AI 集群需要 rack 级交付、液冷集成、良率、客户定制和系统调试 |
| 代表公司 | Dell、HPE、Supermicro、Lenovo、Quanta/QCT、Wistron/Wiwynn、Foxconn、Inventec、Gigabyte、ASUS、Celestica、Jabil、Flex、Pegatron |
| 关键指标 | AI server revenue mix、GB200/GB300 出货、rack backlog、毛利率、存货、客户集中、液冷 rack 比例、交付周期 |
| 主要反证 | 低毛利代工属性；客户/平台切换；NVIDIA/云厂商直接整合；库存积压；组件短缺导致收入递延 |

研究重点：ODM/OEM 量大但利润率通常低。10 倍弹性更可能来自高附加值组件或系统集成瓶颈，而不是普通 AI server 组装。

### 3.5 加速器：GPU、TPU、ASIC、CPU、DPU/NIC

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | GPU、custom ASIC、TPU、Trainium/Inferentia、AI CPU、DPU、SmartNIC、RISC-V/Arm IP、chiplet |
| 和 infra 的关系 | AI 计算核心。训练看 FLOPS、HBM、网络；推理看 token economics、延迟、功耗、软件栈 |
| 代表公司 | NVIDIA、AMD、Broadcom、Marvell、Google TPU、AWS Trainium/Inferentia、Microsoft Maia、Meta MTIA、Cerebras、Groq、SambaNova、Tenstorrent、Intel、Arm、SiFive、Andes、Alchip、GUC、Faraday |
| 关键指标 | HBM 容量/带宽、FP8/FP4 性能、tokens/W、software adoption、customer wins、wafer allocation、CoWoS allocation、gross margin、ASIC NRE 与量产收入 |
| 主要反证 | CUDA moat 过强；ASIC 只服务单一客户；软件生态不足；HBM/封装无法配套；价格战；利用率不足 |

研究重点：GPU 龙头可能继续强，但 10 倍资产角度更值得挖 custom ASIC 供应链、chiplet IP、先进封装、scale-up fabric、HBM 绑定环节。

### 3.6 HBM、服务器 DRAM、NAND、eSSD、存储控制器

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | HBM3E/HBM4/HBM4E、DDR5/RDIMM/MRDIMM、LPDDR for server、CXL memory、enterprise SSD、QLC NAND、SSD controller、nearline HDD、object storage |
| 和 infra 的关系 | HBM 决定 GPU/ASIC 可用带宽；服务器 DRAM 支撑 CPU/inference 节点；eSSD 支撑数据加载、checkpoint、多模态数据、推理缓存 |
| 代表公司 | SK hynix、Samsung、Micron、Kioxia、Western Digital/SanDisk、Seagate、Phison、Silicon Motion、Marvell、Pure Storage、NetApp、Weka、VAST Data |
| 关键指标 | HBM bit shipment、HBM ASP、HBM yield、HBM 占 DRAM revenue、server DRAM mix、eSSD TB shipment、NAND ASP、controller ASP、QLC adoption、CXL adoption |
| 主要反证 | 传统存储周期反转被误认为 AI；HBM 产能扩张过快；NAND 价格暴涨抑制需求；eSSD 与 HDD 替代不及预期；大客户自研 controller |

研究重点：这是当前最重要的模块之一。必须拆开 HBM、server DRAM、eSSD、commodity NAND、controller，不能把所有存储上涨都归因于 AI。

### 3.7 网络互连

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | scale-up fabric、scale-out fabric、InfiniBand、Spectrum-X Ethernet、Ethernet switch ASIC、NIC/DPU、PCIe retimer、CXL switch、NVLink、UALink、AEC cable |
| 和 infra 的关系 | AI 集群瓶颈常在 GPU 间通信、all-reduce、MoE routing、推理并发和东西向流量 |
| 代表公司 | NVIDIA/Mellanox、Broadcom、Marvell、Arista、Cisco、Astera Labs、Credo、Alphawave Semi、Rambus、MACOM、TE Connectivity、Amphenol、Molex |
| 关键指标 | 800G/1.6T ports、switch radix、latency、packet loss、power/bit、AEC attach、PCIe/CXL generation、NVLink/UALink adoption、Arista AI cluster revenue |
| 主要反证 | 网络架构被单一厂商内化；以太网价格竞争；optics ASP 快速下降；GPU 利用率未受网络限制；客户自研 switch/NIC |

研究重点：百万卡级别集群下，网络是最可能出现价值量非线性增加的环节之一。

### 3.8 光模块、CPO、硅光、化合物半导体、激光

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | 800G/1.6T/3.2T optical transceiver、LPO/LRO、CPO、silicon photonics、InP laser、EML/DML/DFB/CW laser、TIA、DSP、photodiode、external light source、fiber connector |
| 和 infra 的关系 | AI 集群东西向流量巨大，铜连接受距离和功耗限制；800G→1.6T→CPO 是降低功耗和提升带宽密度的关键路径 |
| 代表公司 | Coherent、Lumentum、Broadcom、Marvell、Credo、MACOM、Semtech、InnoLight、中际旭创、Eoptolink、新易盛、Fabrinet、Sivers、IQE、Tower、GlobalFoundries Fotonix、Ayar Labs、POET、DustPhotonics、Corning |
| 关键指标 | 800G/1.6T shipment、InP wafer capacity、laser yield、DSP ASP、power/bit、CPO design win、hyperscaler qualification、fiber/connector backlog |
| 主要反证 | CPO 量产延后；pluggable optics 生命周期更长；价格战；DSP 被 LPO/CPO 挤压；可靠性/可维护性问题 |

研究重点：适合挖非美小盘和隐形供应链，尤其 InP、laser array、external light source、CPO bonding、光连接器、光测试。

### 3.9 先进封装

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | CoWoS-S/CoWoS-L、2.5D interposer、RDL interposer、SoIC、hybrid bonding、TC bonding、HBM stacking、underfill/MUF、molding、singulation、ABF substrate、glass substrate |
| 和 infra 的关系 | AI 芯片越来越依赖多 die、多 HBM stack、超大封装和高密度互连。封装从后段工艺变成系统性能核心 |
| 代表公司 | TSMC、ASE、Amkor、Samsung、Intel、JCET、Powertech、KYEC、Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、AT&S、Samsung Electro-Mechanics、LG Innotek、Ajinomoto、Resonac、Namics |
| 设备公司 | Hanmi Semiconductor、ASMPT、BESI、SUSS MicroTec、TOWA、Disco、Shibaura Mechatronics、Tazmo、Camtek、Nova、Onto Innovation、Advantest、Teradyne |
| 关键指标 | CoWoS capacity、HBM stack height、TCB/hybrid bonding orders、ABF 层数、substrate ASP、interposer size、yield、test time、probe card demand |
| 主要反证 | CoWoS 扩产后瓶颈消失；技术路径降低设备价值量；客户认证集中于少数龙头；封装价格被 foundry 吸收 |

研究重点：第一优先级方向。它同时满足需求确定、技术难、客户认证长、供给扩张慢、非美中小公司多。

### 3.10 测试、量测、探针、良率管理

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | GPU/ASIC final test、HBM memory test、wafer probe、probe card、burn-in、advanced packaging inspection、metrology、defect review、AI yield analytics |
| 和 infra 的关系 | AI 芯片 die 大、封装复杂、HBM stack 多，任何良率损失都极贵；HBM 测试时间和 probe complexity 上升 |
| 代表公司 | Advantest、Teradyne、Chroma、MPI、WinWay、FormFactor、Technoprobe、Camtek、Nova、Onto、KLA、Lasertec、Koh Young、Leeno、ISC、TSE、Nextin |
| 关键指标 | memory tester sales、SoC tester sales、probe card ASP、HBM test time、advanced packaging inspection revenue、book-to-bill、yield improvement |
| 主要反证 | 测试设备订单提前透支；客户内部化；测试时间被工艺优化压缩；memory cycle 下行导致 tester utilization 下降 |

研究重点：测试/量测是 AI Infra 里容易被低估的良率杠杆。必须拆收入结构，避免把所有半导体测试公司都映射成 HBM 受益。

### 3.11 晶圆制造、EDA、IP、WFE

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | leading-edge foundry、EUV/High-NA、deposition、etch、CMP、clean、metrology、mask inspection、EDA、verification、IP、HBM DRAM manufacturing |
| 和 infra 的关系 | GPU/ASIC/HBM 都依赖先进制程和高质量制造；custom ASIC 增长会拉动 EDA/IP/NRE；HBM 拉动 DRAM 先进节点和 EUV |
| 代表公司 | TSMC、Samsung Foundry、Intel Foundry、GlobalFoundries、UMC、SMIC、ASML、AMAT、Lam、KLA、TEL、ASM International、Kokusai、Screen、Ebara、Disco、Lasertec、JEOL、Horiba、ULVAC、Synopsys、Cadence、Siemens EDA、Ansys、Arm、Rambus、Alphawave、Andes |
| 关键指标 | leading-edge wafer starts、AI/HPC revenue mix、EUV shipment、WFE orders、EDA backlog、IP royalties、custom ASIC tape-out、mask complexity |
| 主要反证 | 半导体 CapEx 周期下行；export control；先进节点需求集中少数客户；EDA/IP 增长被估值提前反映 |

### 3.12 半导体材料、化工、气体、真空、洁净室

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | silicon wafer、SOI、InP/GaAs/GaN/SiC substrate、photoresist、CMP slurry、wet chemicals、precursor、etch gas、industrial gas、vacuum valve、cleanroom、FOUP/reticle pod、UPW |
| 和 infra 的关系 | AI 芯片先进制程、HBM、硅光、功率半导体和先进封装都会放大对高纯材料、气体、真空、洁净系统的需求 |
| 代表公司 | Shin-Etsu、SUMCO、GlobalWafers、Siltronic、Soitec、IQE、AXT、JX Advanced Metals、Mitsubishi Chemical、JSR、Tokyo Ohka、Fujifilm、Merck KGaA、Entegris、Resonac、Ajinomoto、ADEKA、Kanto Denka、Stella Chemifa、Soulbrain、Dongjin Semichem、SK Materials、Wonik Materials、Air Liquide、Linde、Nippon Sanso、VAT、CKD、SMC、Organo、Kurita |
| 关键指标 | wafer ASP、SOI/InP/GaAs substrate demand、EUV resist adoption、gas utilization、vacuum valve orders、cleanroom/UPW orders、materials gross margin |
| 主要反证 | AI 需求集中在少量 leading-edge fabs，材料公司收入分散；客户议价强；本土替代；传统半导体周期拖累 |

研究重点：避免材料泛化。只找 AI 封装、HBM、硅光、EUV、功率电源直接提高用量或规格的材料。

### 3.13 数据中心：土地、土建、机电、互联、许可

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | land bank、grid interconnect、substation、fiber route、colo、powered shell、modular data center、permits、water rights、noise/environment |
| 和 infra 的关系 | GPU 交付只是第一步。AI cluster 需要可用机房、电力、冷却和网络，time-to-power 成为核心约束 |
| 代表公司 | Equinix、Digital Realty、Vantage、QTS、CyrusOne、DataBank、NextDC、AirTrunk、GDS、Keppel DC、Applied Digital、IREN、Hut 8、TeraWulf、Core Scientific、Jacobs、Fluor |
| 关键指标 | MW leased、MW under construction、time-to-power、capex/MW、PUE/WUE、pre-lease ratio、utility queue、debt cost、customer credit quality |
| 主要反证 | 地方反对、水资源限制、并网延迟、建设成本超支、融资成本上升、客户取消或推迟 |

### 3.14 电力设备、冷却、热管理

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | UPS、PDU、switchgear、transformer、busway、power module、VRM、SiC/GaN power、liquid cooling、CDU、cold plate、immersion、chiller、pump、fan、heat exchanger |
| 和 infra 的关系 | AI rack 功率密度上升，传统风冷受限；数据中心扩张受制于变压器、并网、电力模块、冷却系统 |
| 代表公司 | Vertiv、Schneider Electric、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta Electronics、Lite-On、AcBel、Vicor、Monolithic Power、Infineon、Power Integrations、Navitas、Fuji Electric、Mitsubishi Electric、Nidec、Daikin、Munters、Alfa Laval、Modine、SPX、CoolIT、Boyd |
| 关键指标 | backlog、book-to-bill、transformer lead time、liquid cooling attach rate、rack kW、gross margin、power conversion efficiency、customer concentration |
| 主要反证 | 订单提前透支；冷却方案标准化后 ASP 下行；数据中心延期；供应商扩产导致瓶颈缓解；电力接入而非设备成为真正瓶颈 |

研究重点：AI Infra 从半导体扩散到工业和电网的核心方向。小型热管理、液冷组件、功率半导体、变压器/电力设备链条可能比大型电气集团更有弹性。

### 3.15 能源、电网、燃气、核能、储能

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | utility PPA、gas turbine、onsite power、fuel cell、battery storage、SMR、nuclear restart、uranium enrichment、transmission、substation |
| 和 infra 的关系 | AI 数据中心是高负荷、低容忍停机、交付时间敏感的电力用户。电力从成本项变成选址和扩张约束 |
| 代表公司 | Constellation、Vistra、Talen、NRG、GE Vernova、Siemens Energy、Mitsubishi Heavy、Bloom Energy、Cameco、Kazatomprom、Centrus/LEU、NuScale、Oklo、BWXT、Rolls-Royce SMR、Fluence、Tesla Energy、NextEra、Duke、Southern |
| 关键指标 | contracted MW/GW、PPA price、interconnect queue、gas turbine backlog、nuclear capacity factor、SMR licensing、fuel supply、transmission capex |
| 主要反证 | 电力项目周期太长，无法匹配 AI CapEx；地方反对；天然气/核能监管风险；数据中心需求波动；电价上涨侵蚀云毛利 |

### 3.16 金融、租赁、REIT、保险、GPU 残值

| 项目 | 内容 |
| --- | --- |
| 核心子环节 | project finance、GPU-backed lending、sale-leaseback、data center REIT、private credit、insurance、equipment leasing、off-balance-sheet JV |
| 和 infra 的关系 | AI Infra 是资本密集型产业。融资成本、资产残值、客户信用和租赁结构决定 NeoCloud/数据中心资产能否撑住扩张 |
| 代表公司 | Blackstone、Brookfield、KKR、Ares、Blue Owl、DigitalBridge、Macquarie、Apollo、Equinix、Digital Realty、CoreWeave、Oracle、Meta JV 结构相关方 |
| 关键指标 | cost of debt、lease duration、take-or-pay ratio、customer credit、GPU residual value、depreciation policy、interest coverage、off-balance-sheet liabilities |
| 主要反证 | AI 收入低于预期；债务市场收紧；GPU 残值下跌；客户违约；会计结构复杂导致风险被低估 |

研究重点：AI Infra 越重资产，越要研究信用结构。很多高增长云资产最终可能更像电信塔、航空租赁或 shale 资本周期，而不是软件公司。

## 4. 非美小盘 / 隐形供应链重点挖掘

### 4.1 日本

| 方向 | 候选公司 / 主题 |
| --- | --- |
| HBM / 先进封装设备 | TOWA、Disco、Tazmo、Shibaura Mechatronics、Tokyo Seimitsu/Accretech、Rorze、Tera Probe |
| 半导体测试 | Advantest、Tokyo Seimitsu、Yokogawa |
| EUV / 检测 / 量测 | Lasertec、JEOL、Horiba |
| WFE / 清洗 / 沉积 / 刻蚀 | Tokyo Electron、Screen、Ebara、Kokusai Electric、ULVAC |
| 封装基板 / ABF | Ibiden、Shinko Electric、Ajinomoto Fine-Techno、Resonac、Namics、Sumitomo Bakelite |
| 硅片 / 材料 | Shin-Etsu、SUMCO、JX Advanced Metals、Fujifilm、JSR、Tokyo Ohka、ADEKA、Kanto Denka、Stella Chemifa |
| 真空 / 气体 / 洁净 / UPW | ULVAC、Ebara、Organo、Kurita、CKD、SMC、Ferrotec |
| 电力 / 热管理 | Mitsubishi Electric、Fuji Electric、Nidec、Daikin、Meidensha |

核心问题：收入是否真的来自 AI/HBM/CoWoS，而不是普通半导体复苏；订单是否一次性；毛利率是否能随需求上升而扩张。

### 4.2 韩国

| 方向 | 候选公司 / 主题 |
| --- | --- |
| HBM / DRAM | SK hynix、Samsung Electronics |
| HBM bonding / packaging equipment | Hanmi Semiconductor、Hanwha Semitech、ASMPT 相关订单、SEMES |
| 前道设备 | Wonik IPS、Jusung Engineering、PSK、TES、HPSP、Nextin |
| 材料 / 气体 / 化学品 | Soulbrain、Dongjin Semichem、SK Materials、Wonik Materials、DNF、Foosung、Lake Materials |
| 测试 / 探针 | Leeno、ISC、TSE、Koh Young、Doosan Tesna |
| 封装 / OSAT | Hana Micron、SFA Semicon、Nepes、LB Semicon、Signetics |
| 基板 | Samsung Electro-Mechanics、LG Innotek、Simmtech、Daeduck Electronics |

核心问题：HBM 设备订单是否被头部厂商内化或重新分配；Samsung HBM qualification 是否改变供应链份额；HBM4 技术路径是否让现有设备/材料价值量下降。

### 4.3 台湾

| 方向 | 候选公司 / 主题 |
| --- | --- |
| Foundry / CoWoS | TSMC |
| OSAT / 测试 | ASE、SPIL、Powertech、KYEC、Sigurd、ChipMOS、Chipbond |
| CoWoS 相关设备 / 服务 | Marketech、Scientech、Topco Scientific、Gudeng、Foxsem |
| ATE / 探针 | Chroma、MPI、WinWay |
| 基板 / PCB | Unimicron、Nan Ya PCB、Kinsus、Tripod、Compeq |
| AI server ODM | Quanta/QCT、Wiwynn、Wistron、Foxconn、Inventec、Gigabyte、ASUS |
| 电源 / 散热 | Delta Electronics、Lite-On、AcBel、Auras、Asia Vital Components |
| ASIC / IP | Alchip、GUC、Faraday、Andes、eMemory、MediaTek |
| 存储控制器 | Phison、Silicon Motion |

核心问题：低毛利 ODM 不一定捕获价值；TSMC 扩产后中游议价下降；单一客户或单一平台风险高。

### 4.4 欧洲

| 方向 | 候选公司 / 主题 |
| --- | --- |
| Lithography / WFE | ASML、ASM International、BESI、SUSS MicroTec、Aixtron |
| Advanced packaging | BESI、SUSS MicroTec、ASMPT 欧洲客户链、LPKF |
| 真空 / 设备零部件 | VAT Group、Comet、Pfeiffer Vacuum |
| 硅片 / SOI / 化合物半导体 | Soitec、Siltronic、IQE、X-Fab、Aixtron |
| 硅光 / 激光 | Sivers Semiconductors、ams OSRAM、IQE |
| 功率半导体 | Infineon、STMicroelectronics、Nexperia |
| 电力 / 数据中心设备 | Schneider Electric、Siemens Energy、ABB、Legrand、Nexans、Prysmian、NKT |
| 热管理 | Munters、Alfa Laval、Aalberts |

核心问题：欧洲小盘常有流动性弱、订单波动大、客户集中、技术路线不确定等问题，不能只看 AI/CPO 标签。

### 4.5 以色列

| 方向 | 候选公司 / 主题 |
| --- | --- |
| Advanced packaging inspection / metrology | Camtek、Nova |
| Foundry / specialty process | Tower Semiconductor |
| 硅光 / 光连接 | DustPhotonics、ColorChip、Tower silicon photonics 相关生态 |
| 芯片 / 边缘 AI | Hailo、Valens、proteanTecs |
| 设计服务 / 安全 | 多数为 private，但与 hyperscaler、EDA、硅光生态相关 |

核心问题：用先进封装订单、HBM/CoWoS 客户、gross margin 和订单可见度验证，而不是只看公司官网标签。

## 5. 存储超级周期判断

核心判断：存储超级周期可能是 AI Infra 从 GPU 狭义周期扩散到全栈物理瓶颈周期的第一个明确信号，但不能把所有 memory 上涨都归因于 AI。

### 5.1 HBM：最纯的 AI 真实拉动

HBM 与 AI accelerator 强绑定。每一代 GPU/ASIC 都在增加 HBM 容量、带宽、stack 数或封装复杂度。HBM 不是普通 DRAM 的简单替代，而是高端 AI 芯片能否跑大模型训练和高吞吐推理的核心瓶颈。

受益链：

- Memory vendor：SK hynix、Samsung、Micron
- HBM 设备：Hanmi、ASMPT、BESI、SUSS、TOWA、Disco
- 测试：Advantest、Teradyne、Chroma、MPI、WinWay、Leeno、ISC、TSE
- 材料：underfill、MUF、molding compound、ABF、substrate、CMP、etch gas
- 封装：TSMC CoWoS、ASE、Amkor、Samsung/Intel advanced packaging

关键指标：

- HBM revenue 占 DRAM revenue 比例
- HBM3E/HBM4 qualification
- HBM wafer allocation
- HBM stack height：8-high、12-high、16-high
- TCB / hybrid bonding 订单
- HBM test time 与 tester capacity
- NVIDIA/AMD/ASIC 平台 attach rate

主要反证：

- HBM4/HBM4E 扩产过快导致 ASP 下滑
- Samsung/Micron 补上供给后竞争加剧
- AI accelerator 需求放缓
- 技术路线改变，降低某些设备/材料价值量
- HBM yield 提升导致单位设备需求下降

### 5.2 服务器 DRAM：AI + 供给挤出 + 传统周期反转

服务器 DRAM 受 AI 拉动，但逻辑更复杂：

- AI inference servers、CPU host servers、data preprocessing 都需要更多 DRAM。
- HBM 抢占先进 DRAM 产能，使 conventional DRAM 供给变紧。
- DDR5/RDIMM/MRDIMM、CXL memory 可能提高服务器内存价值量。
- 其中也包含存储下行后的传统周期修复。

验证指标：

- Server DRAM bit growth 是否显著高于 PC/mobile。
- 128GB/256GB RDIMM 渗透率。
- DDR5 ASP 与 bit shipment 是否同时上升。
- Memory vendor 是否把 wafer 从 consumer DRAM 转向 HBM/server。
- CXL memory 是否进入实际部署。

### 5.3 NAND / eSSD：真实需求存在，但更容易被误判

AI 对 NAND 的需求不如 HBM 直接，但在训练数据湖、checkpoint、模型权重存储、视频/多模态数据、推理缓存和高容量 enterprise SSD 方向有真实需求。

真实 AI 拉动：

- 高容量 eSSD：60TB、122TB、245TB
- QLC enterprise SSD
- PCIe Gen5/Gen6 SSD
- AI storage appliance
- 数据中心 storage controller

传统周期成分：

- NAND wafer start cuts
- 库存去化
- consumer SSD / phone recovery
- HDD shortage 推动替代

AI 叙事映射风险：

- 普通 consumer NAND 被包装成 AI。
- 没有 enterprise SSD 客户的 controller 公司被误认为 AI。
- NAND ASP 上涨来自供给收缩而非长期需求。
- eSSD 增长但利润被大客户压缩。

### 5.4 存储控制器

真 AI 相关：

- Enterprise SSD controller
- PCIe Gen5/Gen6 controller
- QLC endurance management
- Data center firmware
- AI storage appliance controller
- CXL memory controller

候选公司：Phison、Silicon Motion、Marvell、Microchip、Rambus、Innogrit、Maxio、Starblaze。

关键指标：

- enterprise controller revenue mix
- PCIe Gen5/Gen6 design wins
- QLC enterprise qualification
- hyperscaler qualification
- gross margin 是否提升
- 单盘容量提升带来的 controller ASP

### 5.5 封装 / 测试 / 材料：二阶弹性可能比 memory vendor 更高

HBM 本身由大公司主导，但 HBM 设备和材料环节可能更有 10 倍资产机会：

- TC bonder / hybrid bonder
- wafer thinning / dicing
- molding / underfill / MUF
- memory tester
- probe card / socket
- substrate / ABF
- inspection / metrology

### 5.6 存储超级周期三分法

| 成分 | 代表 | AI 真实性 | 研究优先级 |
| --- | --- | --- | --- |
| 真实 AI 拉动 | HBM、HBM test、TCB/hybrid bonding、AI eSSD、server DRAM | 高 | 最高 |
| 传统存储周期反转 | commodity DRAM、consumer NAND、库存修复 | 中 | 中等 |
| AI 叙事映射 | 普通 NAND、普通 SSD、无 enterprise 客户的概念股 | 低 | 谨慎 |

结论：值得挖的是 HBM 产能链、HBM 设备/测试/材料、服务器 DRAM 高容量化、enterprise SSD 与 controller、由 HBM 抢产能造成的 conventional memory squeeze。

## 6. 14 个连续研究主题

### 主题 1：HBM 结构性超级周期

| 项目 | 内容 |
| --- | --- |
| 研究问题 | HBM 是 2-3 年供需错配，还是进入 AI accelerator 的长期结构性内存标准？ |
| 公司池 | SK hynix、Samsung、Micron、Hanmi、ASMPT、Advantest、TOWA、BESI、SUSS、Ibiden、Shinko、Ajinomoto、Resonac、Camtek |
| 财报指标 | HBM revenue、DRAM margin、CapEx、HBM sold-out commentary、equipment backlog、tester revenue、substrate revenue |
| 技术指标 | HBM3E→HBM4→HBM4E、12-high/16-high、TCB vs hybrid bonding、MR-MUF/TC-NCF、HBM yield |
| 下一轮 prompt | 请把 HBM 产业链按 HBM3E/HBM4/HBM4E 拆成 vendor、设备、材料、测试、封装、基板，并找出最可能有供给瓶颈的非美公司。 |

### 主题 2：CoWoS / 2.5D / advanced packaging 产能瓶颈

| 项目 | 内容 |
| --- | --- |
| 研究问题 | CoWoS 扩产是否仍是 AI 芯片出货瓶颈？瓶颈从 TSMC 转向 substrate、interposer、设备还是测试？ |
| 公司池 | TSMC、ASE、Amkor、Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、AT&S、BESI、SUSS、ASMPT、TOWA、Disco、Camtek、Nova |
| 财报指标 | CoWoS revenue/capacity、OSAT advanced packaging revenue、substrate ASP、equipment orders、book-to-bill |
| 技术指标 | reticle size、interposer size、HBM stack count、RDL density、hybrid bonding adoption |
| 下一轮 prompt | 请做一张 CoWoS 供应链地图：TSMC 之外的基板、设备、材料、测试公司有哪些，哪些最可能成为下一个瓶颈？ |

### 主题 3：HBM/AI 芯片测试与量测

| 项目 | 内容 |
| --- | --- |
| 研究问题 | AI 芯片复杂度是否让测试时间、测试设备、probe card 和 inspection 成为高弹性瓶颈？ |
| 公司池 | Advantest、Teradyne、Chroma、MPI、WinWay、FormFactor、Technoprobe、Leeno、ISC、TSE、Camtek、Nova、Onto、KLA |
| 财报指标 | SoC tester revenue、memory tester revenue、probe card revenue、gross margin、orders/backlog |
| 技术指标 | HBM test time、known-good-die、wafer-level test、advanced packaging inspection、burn-in |
| 下一轮 prompt | 请把 AI/HBM 测试产业链从 wafer probe 到 final test 拆开，找出测试时间拉长带来的收入弹性。 |

### 主题 4：800G → 1.6T → CPO 光互连

| 项目 | 内容 |
| --- | --- |
| 研究问题 | AI cluster 是否让光模块从通信周期变成数据中心算力周期？CPO 何时替代 pluggable？ |
| 公司池 | Coherent、Lumentum、Broadcom、Marvell、Credo、MACOM、Semtech、InnoLight、中际旭创、Eoptolink、新易盛、Fabrinet、Sivers、IQE、Tower、Corning |
| 财报指标 | datacom revenue、800G/1.6T mix、laser capacity、gross margin、customer concentration |
| 技术指标 | power/bit、EML vs silicon photonics、LPO/LRO、CPO reliability、external light source、InP wafer supply |
| 下一轮 prompt | 请拆解 AI 光互连：800G/1.6T/CPO/硅光/外置光源/InP，各环节公司、瓶颈、验证指标和反证是什么？ |

### 主题 5：AI scale-up fabric 与 rack-scale connectivity

| 项目 | 内容 |
| --- | --- |
| 研究问题 | NVLink、UALink、PCIe/CXL、Ethernet/InfiniBand 谁在 rack-scale AI 里捕获价值？ |
| 公司池 | NVIDIA、Broadcom、Marvell、Astera Labs、Credo、Arista、Cisco、Rambus、Alphawave、Arm、AMD |
| 财报指标 | data center networking revenue、custom silicon revenue、PCIe/CXL product revenue、gross margin |
| 技术指标 | latency、bandwidth、radix、power/bit、scale-up nodes、CXL memory pooling、retimer/switch attach |
| 下一轮 prompt | 请比较 NVLink、UALink、PCIe/CXL、InfiniBand、Ethernet 在 AI 集群里的位置，并找出最可能被低估的连接芯片公司。 |

### 主题 6：Custom ASIC 与 XPU 供应链

| 项目 | 内容 |
| --- | --- |
| 研究问题 | Hyperscaler 自研 ASIC 是否会把价值从 GPU 转向 ASIC design、EDA/IP、HBM/CoWoS 和 networking？ |
| 公司池 | Broadcom、Marvell、Alchip、GUC、Faraday、Arm、Rambus、Synopsys、Cadence、TSMC、ASE、GlobalFoundries、AWS、Google、Microsoft、Meta |
| 财报指标 | ASIC revenue、NRE、design win、backlog、IP royalty、EDA backlog |
| 技术指标 | TPU/Trainium/Maia/MTIA roadmap、HBM attach、CoWoS allocation、chiplet architecture |
| 下一轮 prompt | 请按 Google TPU、AWS Trainium、Microsoft Maia、Meta MTIA 拆解 custom ASIC 供应链，哪些公开公司受益最大？ |

### 主题 7：NeoCloud 经济模型与信用风险

| 项目 | 内容 |
| --- | --- |
| 研究问题 | NeoCloud 是高增长云平台，还是 GPU 租赁 + 高杠杆基础设施周期？ |
| 公司池 | CoreWeave、Nebius、Oracle、Lambda、Crusoe、IREN、Applied Digital、Hut 8、TeraWulf、Core Scientific、DigitalOcean |
| 财报指标 | revenue backlog、CapEx、debt、lease liabilities、utilization、gross margin、interest expense、depreciation |
| 技术指标 | GPU fleet、power secured、cluster size、network topology、time-to-power |
| 下一轮 prompt | 请建立 NeoCloud 单位经济模型：每 MW、每 GPU、每 rack 的收入、成本、折旧、融资和反证指标。 |

### 主题 8：AI server ODM 与液冷 rack

| 项目 | 内容 |
| --- | --- |
| 研究问题 | Rack-scale AI 是否让 ODM 从低毛利组装商升级为系统集成 bottleneck？ |
| 公司池 | Quanta/QCT、Wiwynn、Wistron、Foxconn、Inventec、Gigabyte、Supermicro、Dell、HPE、Lenovo、Celestica、Jabil |
| 财报指标 | AI server revenue、gross margin、inventory、customer concentration、backlog |
| 技术指标 | GB200/GB300 qualification、liquid-cooled rack、power shelf、rack-level networking |
| 下一轮 prompt | 请比较台湾 AI server ODM：Quanta、Wiwynn、Wistron、Foxconn、Inventec、Gigabyte 的 AI 收入、毛利和客户风险。 |

### 主题 9：电力设备、变压器、UPS、PDU、液冷

| 项目 | 内容 |
| --- | --- |
| 研究问题 | AI 数据中心的真正瓶颈会不会从 GPU 转到电力设备和冷却？ |
| 公司池 | Vertiv、Schneider、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta、Lite-On、AcBel、Vicor、MPS、Infineon、Fuji Electric、Mitsubishi Electric、Nidec、Munters、Alfa Laval、Modine |
| 财报指标 | backlog、book-to-bill、data center revenue、gross margin、order lead time、working capital |
| 技术指标 | rack kW、liquid cooling attach、transformer lead time、UPS efficiency、SiC/GaN adoption |
| 下一轮 prompt | 请把 AI 数据中心电力与冷却链条从变压器到 rack 级 liquid cooling 拆开，列公司池、指标和反证。 |

### 主题 10：AI eSSD、NAND、存储控制器

| 项目 | 内容 |
| --- | --- |
| 研究问题 | AI 是否让 enterprise SSD 成为 NAND 超级周期的新核心，还是只是传统 NAND 反弹？ |
| 公司池 | Samsung、SK hynix/Solidigm、Micron、Kioxia、WDC/SanDisk、Phison、Silicon Motion、Marvell、Pure Storage、NetApp、Weka、VAST |
| 财报指标 | NAND ASP、eSSD revenue、controller mix、enterprise bit shipment、gross margin |
| 技术指标 | PCIe Gen5/6、QLC、122TB/245TB SSD、checkpoint bandwidth、多模态存储 |
| 下一轮 prompt | 请区分 AI 对 NAND/eSSD 的真实需求和传统存储周期反转，按公司列出验证指标。 |

### 主题 11：硅光、InP、激光与化合物半导体

| 项目 | 内容 |
| --- | --- |
| 研究问题 | 1.6T/CPO 是否会制造 InP laser、external light source、硅光 foundry 的新瓶颈？ |
| 公司池 | Sivers、IQE、Coherent、Lumentum、MACOM、Tower、GlobalFoundries、Ayar Labs、POET、DustPhotonics、Soitec、Aixtron |
| 财报指标 | photonics revenue、InP revenue、laser capacity、design win、gross margin |
| 技术指标 | DFB/EML/CW laser、external light source、SiPho integration、CPO attach、on-wafer test |
| 下一轮 prompt | 请深挖非美硅光/化合物半导体小盘：Sivers、IQE、Soitec、Aixtron、Tower、POET 的 AI 相关性和反证。 |

### 主题 12：半导体材料、气体、真空、洁净室

| 项目 | 内容 |
| --- | --- |
| 研究问题 | AI leading-edge、HBM、CoWoS、硅光是否让材料规格升级形成长期 ASP/volume 双击？ |
| 公司池 | Ajinomoto、Resonac、Shin-Etsu、SUMCO、GlobalWafers、Siltronic、Soitec、JSR、Tokyo Ohka、Fujifilm、Merck、Entegris、Soulbrain、Dongjin、SK Materials、VAT、Organo、Kurita |
| 财报指标 | semiconductor materials revenue、gross margin、capacity expansion、customer concentration |
| 技术指标 | ABF layer count、EUV resist、etch gas purity、UPW demand、vacuum valve orders |
| 下一轮 prompt | 请把 AI 半导体材料链条拆成 ABF、photoresist、slurry、gas、wafer、UPW、vacuum，并找非美小盘。 |

### 主题 13：AI 数据中心能源

| 项目 | 内容 |
| --- | --- |
| 研究问题 | AI 数据中心会不会重塑美国和全球电力负荷增长，并催生新型能源资产？ |
| 公司池 | GE Vernova、Siemens Energy、Mitsubishi Heavy、Bloom Energy、Constellation、Vistra、Talen、NRG、Cameco、Kazatomprom、LEU、NuScale、Oklo、BWXT、Fluence |
| 财报指标 | contracted capacity、PPA、turbine backlog、nuclear revenue、fuel supply、project pipeline |
| 技术指标 | interconnect queue、grid stability、24/7 clean power、SMR licensing、gas turbine delivery |
| 下一轮 prompt | 请分析 AI 数据中心电力：核能、SMR、天然气、燃料电池、储能、变压器，哪些是真瓶颈，哪些是叙事？ |

### 主题 14：AI Infra 反证系统

| 项目 | 内容 |
| --- | --- |
| 研究问题 | 什么情况会证明 AI Infra 资本开支过度？哪些指标最早预警？ |
| 公司池 | Hyperscalers、NeoCloud、NVIDIA、memory vendors、optical vendors、power equipment、data center REITs |
| 财报指标 | free cash flow、CapEx/sales、depreciation、gross margin、utilization、RPO conversion、customer concentration |
| 技术指标 | token price decline、tokens/W improvement、model distillation、small model adoption、GPU utilization |
| 下一轮 prompt | 请为 AI Infra 建立一个反证仪表盘：哪些财报和产业指标一旦变化，说明周期从结构性转为泡沫？ |

## 7. 优先级排序

### 第一优先级

1. HBM 设备、测试、材料、基板  
   原因：HBM 是最直接的 AI 内存瓶颈，但 memory vendor 大多已是大市值公司。更高弹性可能在 TC bonder、hybrid bonding、tester、probe、ABF、substrate、underfill、inspection。

2. 光互连 / CPO / InP / 激光 / 硅光  
   原因：AI cluster 规模扩大后，网络和功耗会成为非线性瓶颈。800G→1.6T→CPO 可能带来新的光子材料、激光、DSP、测试和封装需求。

3. 电力设备、液冷、热管理、变压器  
   原因：AI 数据中心从买 GPU 变成拿到电、把热带走、把 rack 点亮。该链条供给扩张慢，很多公司原本不是科技叙事中心。

4. Custom ASIC 供应链和 scale-up connectivity  
   原因：如果 hyperscaler 自研 ASIC 增速持续，价值会扩散到 ASIC design、EDA/IP、CoWoS、HBM、PCIe/CXL、network fabric。

5. 非美材料 / 设备隐形冠军  
   原因：日本、韩国、台湾、欧洲有大量不是 AI 公司但被 AI 规格升级拉动的公司，尤其材料、测试、真空、洁净、封装设备、基板。

### 第二优先级

6. eSSD / enterprise NAND / controller  
   有真实 AI 需求，但混入传统 NAND 周期概率高。

7. NeoCloud / AI data center developers  
   弹性大，但信用和融资风险同样大。

8. AI server ODM  
   需求大，但毛利低。除非液冷、rack-scale 集成、关键客户资格带来结构性毛利提升，否则利润弹性可能低于营收弹性。

### 第三优先级

9. 泛 memory 叙事  
   没有 HBM、server DRAM、eSSD、enterprise controller 暴露的公司，不应简单归入 AI Infra。

10. 泛能源 / SMR / 核能叙事  
   AI 确实需要电，但能源资产周期长、监管重。没有 PPA、并网、许可、客户合同的主题容易只是股价叙事。

11. 泛半导体设备大盘 beta  
   ASML、AMAT、Lam、KLA、TEL 等很重要，但多数是大市值、全周期暴露。10 倍弹性更可能在细分瓶颈小盘。

12. 低毛利 AI server 组装  
   收入可能爆发，但若毛利率无法提升、客户集中、存货压力大，利润弹性可能低于叙事。

## 8. 研究筛选公式

| 维度 | 问题 | 高分特征 |
| --- | --- | --- |
| 需求确定性 | 是否被 GPU/ASIC/HBM/AI DC 出货直接拉动？ | 与 HBM、CoWoS、CPO、液冷、电力强绑定 |
| 供给弹性 | 扩产是否慢？ | 设备复杂、材料认证长、客户切换难 |
| 单位价值量 | 每代 AI 芯片/每 rack 用量是否上升？ | HBM stack、光端口、液冷、测试时间、ABF 层数上升 |
| 议价权 | 是否有技术壁垒或寡头结构？ | 高份额、专利、认证、长期客户 |
| 市值弹性 | 公司当前收入/市值是否小于潜在 TAM？ | 小中盘、AI revenue 初期、订单刚开始反映 |
| 财报验证 | 是否已经在订单/收入/毛利中出现？ | backlog、book-to-bill、gross margin 同时改善 |
| 反证可跟踪 | 能否用 3-5 个指标及时否定？ | 有明确订单、产能、价格、客户指标 |

## 9. 待核验来源清单

| 编号 | 来源 | 链接 | 用途 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | Microsoft FY26 Q3 earnings | https://www.microsoft.com/en-us/investor/earnings/fy-2026-q3/press-release-webcast | Cloud revenue、RPO、AI infra 对毛利率影响 | 待原文核验 |
| 2 | NVIDIA GB300 NVL72 | https://www.nvidia.com/en-us/data-center/gb300-nvl72/ | GB300 系统架构、HBM、网络绑定 | 待原文核验 |
| 3 | TrendForce DRAM/NAND price forecast | https://www.trendforce.com/presscenter/news/20260331-12995.html | DRAM/NAND contract price 线索 | 待原文核验 |
| 4 | Reuters CoreWeave Q1 2026 | https://www.reuters.com/technology/coreweave-tops-quarterly-revenue-estimates-2026-05-07/ | NeoCloud capex、components cost、revenue | 二手来源，需回公司原文 |
| 5 | Google Ironwood TPU | https://blog.google/innovation-and-ai/infrastructure-and-cloud/google-cloud/ironwood-tpu-age-of-inference/ | TPU inference 架构 | 待原文核验 |
| 6 | Broadcom CPO | https://investors.broadcom.com/news-releases/news-release-details/broadcom-announces-third-generation-co-packaged-optics-cpo | CPO 技术和 AI 网络 | 待原文核验 |
| 7 | BESI Q4 2025 results | https://www.besi.com/investor-relations/press-releases/details/be-semiconductor-industries-nv-announces-q4-25-and-full-year-2025-results/ | 2.5D datacenter、photonics、hybrid bonding 订单 | 待原文核验 |
| 8 | Advantest forecast | https://www.advantest.com/en/investors/financial-highlights/forecast/ | AI-related semiconductor tester demand | 待原文核验 |
| 9 | Cadence FY2025 results | https://investor.cadence.com/news/news-details/2026/Cadence-Reports-Fourth-Quarter-and-Fiscal-Year-2025-Financial-Results/default.aspx | EDA / AI chip design demand | 待原文核验 |
| 10 | Ajinomoto ABF | https://www.ajinomoto.com/innovation/our_innovation/buildupfilm | ABF 技术资料 | 待原文核验 |
| 11 | OpenAI Stargate sites | https://openai.com/index/five-new-stargate-sites/ | Stargate data center capacity / investment 线索 | 待原文核验 |
| 12 | Vertiv Q4 2025 results | https://investors.vertiv.com/news/news-details/2026/Vertiv-Reports-Strong-Fourth-Quarter-with-Organic-Orders-Growth-of-252-and-Diluted-EPS-Growth-of-200-Adjusted-Diluted-EPS-37/default.aspx | Data center power / backlog | 待原文核验 |
| 13 | IEA data centre electricity | https://www.iea.org/news/data-centre-electricity-use-surged-in-2025-even-with-tightening-bottlenecks-driving-a-scramble-for-solutions | 数据中心电力需求 | 待原文核验 |
| 14 | TOWA official | https://www.towajapan.co.jp/en/ | TOWA 公司与设备定位 | 待原文核验 |
| 15 | SUSS 2025 results | https://www.suss.com/en/news/corporate-news/2026/suss-achieved-all-forecast-figures-in-the-2025-financial-year-and-hit-a-new-sales-record | AI chip modules / advanced packaging demand | 待原文核验 |
| 16 | Camtek official | https://www.camtek.com/ | Advanced packaging / inspection positioning | 待原文核验 |
| 17 | TrendForce enterprise SSD | https://www.trendforce.com/presscenter/news/20251205-12819.html | Enterprise SSD / NAND revenue 线索 | 待原文核验 |
| 18 | ASMPT 2025 results | https://www.asmpt.com/en/investor-relations/news-events/asmpt-announces-2025-annual-results/ | TCB / advanced packaging 线索 | 待原文核验 |

## 10. ChatGPT Pro 多会话研究方式

不要一次性把所有问题塞给一个会话。使用多个会话慢速推进，每个会话只负责一个方向：

1. HBM 结构性超级周期 agent
2. CoWoS / advanced packaging agent
3. HBM / AI chip testing agent
4. 光互连 / CPO / InP / 硅光 agent
5. Scale-up fabric / custom ASIC agent
6. NeoCloud 经济模型 agent
7. 电力设备 / 液冷 / 热管理 agent
8. 非美材料 / 设备隐形冠军 agent
9. 存储超级周期反证 agent
10. AI Infra 总反证仪表盘 agent

每个会话的输出要求：

- 先列原始出处清单，不要直接给结论。
- 把事实、推论、待核验分开。
- 按公司/模块列关键财报指标。
- 写出 3-5 个可证伪反证指标。
- 不推荐买卖，不给目标价。
- 输出适合回填到本仓库 markdown 的结构化内容。
