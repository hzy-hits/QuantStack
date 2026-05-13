# AI Infra Supercycle Research Mainlines

这份文档记录当前管线的主线研究框架。它回答一个问题：如果只围绕
AI infra supercycle 做顺势投资和长期 10x 候选挖掘，我们应该看哪些产业链、
哪些瓶颈、哪些公司池、哪些指标，以及哪些反证。

上游研究工作台是 `ai_infra/`。它包含 LLM-dependency BFS 框架、146 条全球
AI-infra universe、source verification queue、US alpha mining queue 和
evidence card 模板。本文是给生产管线读的主线摘要；当两者冲突时，
`ai_infra/` 的 BFS universe 和 evidence queue 是上游事实入口。

本文中的财报数字、行业预测、订单和 capex 描述来自研究输入，进入正式日报
或供应链证据前必须经过 source review。管线可以把它们当研究路线图，但不能
把未经复核的数字直接当作 `source_linked_supply_evidence`。

第一优先级的 HBM 与 CoWoS / advanced packaging 细节单独落在
`docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md`。那份文件是 source-review gated
研究备忘录：它可以指导 universe、调研队列和报告解释框架，但其中的公司关系、
订单、产能、财报数字在原始出处复核前不能升级为正式供应链证据。

## 0. Core Thesis

我们不应该每天全市场拉数据，然后从铁路、消费、地产、普通周期股里寻找随机
统计信号。当前主线是 AI infra supercycle：

- AI 是人类最先进的技术栈之一，硅基智能有指数增长可能。
- 投资流程应先限定在 AI infra 及直接邻近产业链，再用量化信号做择时。
- 右侧强趋势优先，尤其是主题强、价格强、成交放大、资金确认、供应链证据逐步
  兑现的股票。
- A股新闻经常滞后，所以 A股先看 tape/flow/sector breadth；新闻只做滞后标签。
- 美股可以把 price/news/options/flow 联合使用，但期权仍是股票决策证据。
- 消费、铁路、普通老经济行业默认不是主线；除非出现直接 AI infra 关系或独立
  sleeve 证明，否则不应消耗执行 R。

最终研究目标不是买“AI 概念”，而是找：

1. 谁控制瓶颈。
2. 谁承担 capex。
3. 谁拿走毛利。
4. 谁承担折旧。
5. 谁能把订单转化成交付、利用率、毛利和现金流。

## 1. From Gemini / ChatGPT / Claude Back To The Supply Chain

### 1.1 应用与分发层

用户看到的是 ChatGPT、Gemini、Claude、Copilot、Claude Code、Gemini in
Workspace、ChatGPT Enterprise、API、Agent、搜索增强、代码助手、企业知识库。

核心不是模型参数最多，而是：

- 用户入口：搜索、浏览器、手机、办公套件、开发者工具、企业 SaaS。
- 工作流嵌入：代码、客服、报告、数据分析、销售自动化、办公协作。
- 收费能力：订阅、API token、企业席位、按任务计费、云平台消耗。

代表公司池：

- OpenAI、Google、Anthropic、Microsoft、Salesforce、ServiceNow、Adobe、
  GitHub、Cursor、Perplexity、Notion、Databricks、Snowflake。

判断逻辑：

- 应用层长期空间大，但短期护城河分化剧烈。
- 真正有价值的是高频入口、私有数据、工作流闭环。
- 普通 AI wrapper 议价权弱，容易被模型、云平台或既有 SaaS 吸收。

### 1.2 模型层

这里包括 GPT、Gemini、Claude 等前沿模型本身：预训练、后训练、RLHF/RLAIF、
合成数据、推理模型、多模态、长上下文、工具调用、Agent、安全对齐和模型评测。

代表公司池：

- OpenAI、Google DeepMind、Anthropic、Meta、xAI、Mistral、DeepSeek、Cohere。

核心瓶颈：

- 高质量数据和合成数据管线。
- 训练算力与推理算力。
- 后训练能力：可控、可靠、会调用工具。
- 推理成本：同等能力下 cost per token 更低。
- 研究员、infra 工程师、系统优化团队。

判断逻辑：

- 前沿模型公司有品牌和能力溢价，但 capex、云成本和人才成本极高。
- 如果模型能力趋同，议价权会从模型本身转向分发入口、私有数据、工具链和云平台。

### 1.3 训练 / 推理平台层

这一层把模型变成企业可用服务：模型托管、API 网关、微调、RAG、向量数据库、
权限控制、安全审计、模型路由、推理加速、监控、计费和 Agent orchestration。

代表平台：

- Microsoft Azure AI / Azure AI Foundry。
- Google Vertex AI。
- AWS Bedrock / SageMaker。
- OpenAI API。
- Anthropic API。
- Databricks Mosaic AI。
- Snowflake Cortex。
- Hugging Face。
- LangChain / LlamaIndex 等工具生态。

核心瓶颈：

- 企业数据接入和权限治理。
- 推理调度效率：batching、KV cache、路由、小模型/大模型混合。
- 延迟、稳定性、SLA。
- 安全、合规、审计。

判断逻辑：

- 云平台和企业数据平台议价权强。
- 企业通常买的不是单一模型，而是模型 + 数据 + 安全 + 部署 + 合规。
- 这一层有强云锁定效应。

### 1.4 云与算力层

Gemini、ChatGPT、Claude 都依赖超大规模云基础设施：GPU/ASIC 集群、数据中心、
网络、电力、存储、调度系统和长期资本开支。

代表公司池：

- Google Cloud、Microsoft Azure、AWS、Oracle Cloud、CoreWeave、Crusoe、
  Lambda、Nebius。

核心瓶颈：

- GPU/ASIC 可得性。
- HBM 与先进封装供给。
- 数据中心 time to power。
- 集群利用率。
- 折旧压力和现金流。
- 大客户长期合约质量。

判断逻辑：

- 云层是 AI supercycle 的 capex 中枢，议价权强但不是无风险。
- 如果算力过度建设、模型推理效率快速提升，云厂商可能面临折旧和毛利压力。

### 1.5 GPU / AI 芯片层

这是当前 AI 产业链最核心的利润池之一。

代表公司池：

- GPU：NVIDIA、AMD。
- 云厂商自研 ASIC：Google TPU、AWS Trainium / Inferentia、Microsoft Maia。
- 定制 ASIC / 网络芯片：Broadcom、Marvell。
- 新型架构：Cerebras、Groq、SambaNova、Tenstorrent。

核心瓶颈：

- HBM 容量与带宽。
- CoWoS / 先进封装产能。
- Scale-up / scale-out 网络。
- CUDA / 软件生态。
- 推理阶段能效与成本。
- 大客户定制 ASIC 的开发周期和良率。

判断逻辑：

- NVIDIA 目前不只是卖芯片，而是卖 GPU + CUDA + NVLink/networking +
  软件生态 + rack-scale reference architecture。
- Google TPU、AWS Trainium、Broadcom 定制 ASIC、AMD 会分走部分推理和
  特定训练负载，但替代 NVIDIA 的难点不只是硬件，而是软件栈和开发者生态。

### 1.6 服务器、整机、网络设备与光模块层

GPU/ASIC 必须装进 AI server、rack、cluster，并通过高速网络连接。

代表公司池：

- 服务器 / 整机：Dell、HPE、Supermicro、Lenovo、Inspur、Quanta、Wiwynn、
  Foxconn、Inventec。
- 网络设备：Arista、Cisco、NVIDIA Mellanox、Broadcom、Juniper。
- 光模块 / 光通信：Coherent、Lumentum、Innolight、中际旭创、新易盛、
  Eoptolink、Fabrinet。
- 交换芯片 / DPU / NIC：Broadcom、Marvell、NVIDIA。

核心瓶颈：

- 800G / 1.6T 网络和光模块供应。
- 高功率 rack 集成。
- 液冷兼容设计。
- 交付能力和供应链管理。
- 大客户集中度。

判断逻辑：

- 纯服务器组装利润率通常低于 GPU、HBM、先进封装和网络芯片。
- 更有议价权的是网络设备、交换芯片、光模块、液冷 ready rack 设计和能快速交付整机集群的厂商。

### 1.7 数据中心、电力与冷却层

AI 数据中心正在从“机房地产”变成“电力、冷却和并网能力”的竞争。

代表公司池：

- 数据中心：Equinix、Digital Realty、QTS、Vantage、CyrusOne、CoreWeave、
  Oracle、AWS、Google、Microsoft。
- 电力设备：Schneider Electric、Eaton、ABB、Vertiv、Siemens Energy、GE Vernova。
- 冷却：Vertiv、Schneider、CoolIT、Asetek、Modine、Trane。
- 工程建设：Quanta Services、Fluor、Jacobs。
- 公用事业与电力开发：NextEra、Constellation、Dominion、AEP、Entergy。

核心瓶颈：

- 可获得电力，而不仅是土地。
- 并网排队。
- 变压器、开关设备、电缆交期。
- 液冷和热管理。
- 水资源。
- 地方监管和社区阻力。

判断逻辑：

- 未来 AI 算力扩张不只问有没有 GPU，还要问有没有 GW 级电力。
- 公用事业和电力项目监管重、建设周期长，投资收益模式不同于半导体。

### 1.8 半导体制造、先进封装、HBM、EDA、IP、设备与材料层

这是 AI 产业链最上游、壁垒最高的一组隐形收费站。

代表公司池：

- 晶圆代工：TSMC、Samsung Foundry、Intel Foundry。
- 先进封装：TSMC CoWoS / SoIC、ASE、Amkor、JCET、Ibiden、Unimicron。
- HBM：SK hynix、Samsung、Micron。
- EDA：Synopsys、Cadence、Siemens EDA。
- IP：Arm、Rambus、Imagination、Alphawave Semi。
- 半导体设备：ASML、Applied Materials、Lam Research、KLA、Tokyo Electron。
- 材料：Shin-Etsu、SUMCO、JSR、Tokyo Ohka、Entegris、DuPont、Linde、Air Liquide。

核心瓶颈：

- HBM3E / HBM4 产能与良率。
- CoWoS / 2.5D / 3D 封装产能。
- 先进制程良率。
- EUV / High-NA EUV 设备供给。
- EDA 和 IP 复杂度。
- 地缘政治和出口管制。

判断逻辑：

- TSMC、HBM 供应商、ASML、EDA/IP 厂商属于典型卖铲人和瓶颈资产。
- 这一层替代难、扩产慢、技术门槛高，议价权强，但仍有半导体周期和出口管制风险。

## 2. Three Ecosystem Archetypes

### 2.1 Gemini / Google：垂直整合最强

Google 的优势是模型、云、TPU、搜索、Android、Workspace、YouTube、广告和
数据中心基本都在自己体系内。

| Dimension | Gemini / Google |
|---|---|
| 用户入口 | Google Search、Android、Chrome、Workspace、YouTube、Google Cloud |
| 模型 | Google DeepMind / Gemini |
| 训练与推理 | TPU-first，GPU 补充 |
| 云 | Google Cloud |
| 芯片依赖 | 自研 TPU；背后仍依赖 Broadcom/TSMC/HBM/先进封装，也使用 NVIDIA GPU |
| 战略优势 | 垂直整合、分发入口强、数据中心经验深 |
| 主要风险 | 搜索商业模式被 AI 改写、capex 压力、TPU 生态外部化程度不如 CUDA |

一句话判断：Google 是三者中最像全栈 AI 工业公司的玩家。

### 2.2 ChatGPT / OpenAI / Microsoft：品牌最强，基础设施外部依赖更重

OpenAI 的优势是 ChatGPT 品牌、开发者生态、模型迭代速度和产品化能力。但相较
Google，OpenAI 不拥有同等级别的自有云和芯片供应链，因此必须通过 Microsoft、
Oracle、CoreWeave、Stargate 等方式扩张算力。

| Dimension | ChatGPT / OpenAI / Microsoft |
|---|---|
| 用户入口 | ChatGPT、OpenAI API、ChatGPT Enterprise、Microsoft Copilot、GitHub、Office、Azure |
| 模型 | OpenAI GPT / o 系列 |
| 训练与推理 | 主要依赖 Azure，同时扩展 Oracle、CoreWeave、Stargate 等容量 |
| 云 | Microsoft Azure 是核心；多云空间增大 |
| 芯片依赖 | 高度依赖 NVIDIA；Microsoft 推进 Maia 等自研 AI 芯片 |
| 战略优势 | 消费级 AI 品牌最强；开发者心智强；Microsoft 企业分发强 |
| 主要风险 | 算力供应、capex 融资、与 Microsoft 的利益分配、推理成本 |

一句话判断：OpenAI 是最强前台 AI 产品公司，但背后是资本密集型算力联盟。

### 2.3 Claude / Anthropic / AWS / Google：多云、多芯片对冲最明显

Anthropic 的特点是 AWS 是主要云和训练合作伙伴，但 Anthropic 同时积极使用
Google TPU、NVIDIA GPU，并通过多个云平台分发 Claude。

| Dimension | Claude / Anthropic / AWS / Google |
|---|---|
| 用户入口 | Claude、Claude Code、Anthropic API、AWS Bedrock、Google Vertex AI、Azure Foundry |
| 模型 | Claude 系列 |
| 训练与推理 | AWS Trainium 为核心之一，同时使用 Google TPU、NVIDIA GPU |
| 云 | AWS 是主要训练和云合作伙伴；同时多云分发 |
| 芯片依赖 | Trainium、TPU、NVIDIA 多路径 |
| 战略优势 | 多云、多芯片对冲；企业和安全定位清晰；Bedrock / Vertex 分发强 |
| 主要风险 | 战略投资人依赖、算力分配、商业化速度、与 OpenAI/Gemini 能力竞争 |

一句话判断：Anthropic 是三者中最明显采用供应链对冲策略的公司。

## 3. Sixteen-Layer AI Infra Map

AI infra 可以拆成 16 层：

1. LLM / Agent 需求。
2. AI 软件栈、框架、编译器、推理引擎。
3. AI 云、NeoCloud、GPU-as-a-Service。
4. AI 服务器、rack-scale 系统、ODM/OEM。
5. 加速器：GPU、TPU、ASIC、CPU、DPU/NIC。
6. HBM、服务器 DRAM、NAND、eSSD、存储控制器。
7. 网络互连：InfiniBand、Ethernet、NVLink、PCIe/CXL、UALink。
8. 光模块、CPO、硅光、化合物半导体、激光。
9. 先进封装：CoWoS、SoIC、hybrid bonding、TCB、interposer、substrate。
10. 测试、量测、探针、良率管理。
11. 晶圆制造、EDA、IP、WFE。
12. 半导体材料、化工、气体、真空、洁净室。
13. 数据中心：土地、土建、机电、互联、许可。
14. 电力设备、冷却、热管理。
15. 能源、电网、燃气、核能、储能。
16. 金融、租赁、REIT、保险、GPU 残值。

这些层不是平行叙事，而是从用户 token 需求倒推到 GPU/ASIC、HBM、封装、
网络、机房、电力和融资结构的连续链条。

## 4. Bottleneck And Bargaining Power Map

| Layer | 核心瓶颈 | 价值量与议价权判断 |
|---|---|---|
| 应用 / 分发 | 用户入口、工作流嵌入、数据权限、信任与合规 | 长期价值最大但分化剧烈；强入口 SaaS 和平台议价权高，普通 wrapper 低 |
| 模型 | 前沿能力、后训练、推理成本、数据、人才 | 品牌强但烧钱；能力趋同时议价权会下移到分发和上移到算力 |
| 训练 / 推理平台 | 企业部署、安全治理、模型路由、推理效率 | 云厂商和数据平台议价权强，可通过平台锁定客户 |
| 云与算力 | GPU/ASIC 集群、电力、利用率、折旧 | capex 中枢；规模优势强，但毛利和现金流受压 |
| GPU / AI 加速器 | HBM、封装、软件生态、网络、能效 | 当前利润池最集中；NVIDIA 最强，ASIC 逐步分流部分负载 |
| HBM / 内存 | HBM 产能、良率、先进封装配合 | 议价权上升；AI 推理和长上下文继续拉动带宽需求 |
| 先进封装 / 晶圆代工 | CoWoS、先进节点、良率、地缘风险 | 强瓶颈、强议价权；TSMC 是核心收费站 |
| 网络 / 光模块 | 800G/1.6T、交换芯片、集群可靠性 | AI cluster 越大，网络越关键；高端网络和光模块强于普通服务器 |
| 服务器 / Rack | 高功率整机交付、液冷、供应链 | 收入弹性大但毛利较低；强者靠交付速度和大客户关系 |
| 数据中心 / 电力 / 冷却 | time to power、并网、变压器、液冷、水资源 | 瓶颈快速上移；确定性提高，但建设周期和监管风险大 |
| EDA / IP / 设备 / 材料 | EUV、EDA 工具、IP、材料纯度 | 上游垄断/寡头环节，议价权强，周期性仍存在 |

## 5. Storage Supercycle View

存储超级周期可能是 AI infra 从 GPU 狭义周期扩散到全栈物理瓶颈周期的第一个
明确信号，但不能把所有 memory 上涨都归因于 AI。

### 5.1 HBM：最纯的 AI 拉动

HBM 与 AI accelerator 强绑定。每一代 GPU/ASIC 都在增加 HBM 容量、带宽、
stack 数或封装复杂度。

受益链：

- Memory vendor：SK hynix、Samsung、Micron。
- HBM 设备：Hanmi、ASMPT、BESI、SUSS、TOWA、Disco。
- 测试：Advantest、Teradyne、Chroma、MPI、WinWay、Leeno、ISC、TSE。
- 材料：underfill、MUF、molding compound、ABF、substrate、CMP、etch gas。
- 封装：TSMC CoWoS、ASE、Amkor、Samsung/Intel advanced packaging。

关键指标：

- HBM revenue 占 DRAM revenue 比例。
- HBM3E/HBM4 qualification。
- HBM wafer allocation。
- 8-high、12-high、16-high stack。
- TCB / hybrid bonding 订单。
- HBM test time 与 tester capacity。
- NVIDIA/AMD/ASIC 平台 attach rate。

主要反证：

- HBM4/HBM4E 扩产过快导致 ASP 下滑。
- Samsung/Micron 补上供给后竞争加剧。
- AI accelerator 需求放缓。
- 技术路线降低某些设备/材料价值量。
- HBM yield 提升导致单位设备需求下降。

结论：HBM 是存储超级周期里最像结构性 AI 周期的部分。

### 5.2 Server DRAM：AI + 供给挤出 + 传统周期反转

服务器 DRAM 受 AI 拉动，但逻辑更复杂：

- AI inference servers、CPU host servers、RAG、data preprocessing 需要更多 DRAM。
- HBM 抢占先进 DRAM 产能，使 conventional DRAM 供给变紧。
- DDR5/RDIMM/MRDIMM、CXL memory 可能提高服务器内存价值量。
- 其中也包含存储下行后的传统周期修复。

结论：server DRAM 是半结构性、半周期性。

### 5.3 NAND / eSSD：真实需求存在，但更容易误判

真实 AI 需求：

- 训练数据湖、checkpoint、模型权重存储。
- RAG / vector database / retrieval pipeline。
- 视频生成、多模态数据集。
- 推理缓存、日志、用户数据。
- 高容量 enterprise SSD 替代部分 HDD。

风险：

- consumer NAND 反弹被包装成 AI。
- 没有 enterprise SSD 客户的 controller 被误认为 AI。
- NAND ASP 上涨来自供给收缩而非长期需求。
- eSSD 增长但利润被大客户压缩。

结论：eSSD 值得研究，但 NAND 整体更容易混入传统周期。

### 5.4 Storage Research Priority

| Component | AI 真实性 | 研究优先级 |
|---|---|---|
| HBM、HBM test、TCB/hybrid bonding、AI eSSD、server DRAM | 高 | 最高 |
| commodity DRAM、consumer NAND、库存修复 | 中 | 中等 |
| 普通 NAND、普通 SSD、无 enterprise 客户的概念股 | 低 | 谨慎 |

## 6. Fourteen Continuous Research Themes

### Theme 1：HBM 结构性超级周期

研究问题：HBM 是 2-3 年供需错配，还是进入 AI accelerator 的长期结构性内存标准？

详细备忘录：`docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md`。

公司池：SK hynix、Samsung、Micron、Hanmi、ASMPT、Advantest、TOWA、BESI、
SUSS、Ibiden、Shinko、Ajinomoto、Resonac、Camtek。

核心指标：HBM revenue、DRAM margin、capex、HBM sold-out commentary、
equipment backlog、tester revenue、substrate revenue。

技术指标：HBM3E -> HBM4 -> HBM4E、12-high/16-high、TCB vs hybrid bonding、
MR-MUF/TC-NCF、HBM yield。

下一轮 prompt：

> 请把 HBM 产业链按 HBM3E/HBM4/HBM4E 拆成 vendor、设备、材料、测试、封装、基板，并找出最可能有供给瓶颈的非美公司。

### Theme 2：CoWoS / 2.5D / advanced packaging 产能瓶颈

研究问题：CoWoS 扩产是否仍是 AI 芯片出货瓶颈？瓶颈从 TSMC 转向 substrate、
interposer、设备还是测试？

详细备忘录：`docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md`。

公司池：TSMC、ASE、Amkor、Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、
AT&S、BESI、SUSS、ASMPT、TOWA、Disco、Camtek、Nova。

下一轮 prompt：

> 请做一张 CoWoS 供应链地图：TSMC 之外的基板、设备、材料、测试公司有哪些，哪些最可能成为下一个瓶颈？

### Theme 3：HBM/AI 芯片测试与量测

研究问题：AI 芯片复杂度是否让测试时间、测试设备、probe card 和 inspection 成为
高弹性瓶颈？

公司池：Advantest、Teradyne、Chroma、MPI、WinWay、FormFactor、Technoprobe、
Leeno、ISC、TSE、Camtek、Nova、Onto、KLA。

下一轮 prompt：

> 请把 AI/HBM 测试产业链从 wafer probe 到 final test 拆开，找出测试时间拉长带来的收入弹性。

### Theme 4：800G -> 1.6T -> CPO 光互连

研究问题：AI cluster 是否让光模块从通信周期变成数据中心算力周期？CPO 何时替代
pluggable？

公司池：Coherent、Lumentum、Broadcom、Marvell、Credo、MACOM、Semtech、
InnoLight、中际旭创、Eoptolink、新易盛、Fabrinet、Sivers、IQE、Tower、Corning。

下一轮 prompt：

> 请拆解 AI 光互连：800G/1.6T/CPO/硅光/外置光源/InP，各环节公司、瓶颈、验证指标和反证是什么？

### Theme 5：AI scale-up fabric 与 rack-scale connectivity

研究问题：NVLink、UALink、PCIe/CXL、Ethernet/InfiniBand 谁在 rack-scale AI 里捕获价值？

公司池：NVIDIA、Broadcom、Marvell、Astera Labs、Credo、Arista、Cisco、Rambus、
Alphawave、Arm、AMD。

下一轮 prompt：

> 请比较 NVLink、UALink、PCIe/CXL、InfiniBand、Ethernet 在 AI 集群里的位置，并找出最可能被低估的连接芯片公司。

### Theme 6：Custom ASIC 与 XPU 供应链

研究问题：Hyperscaler 自研 ASIC 是否会把价值从 GPU 转向 ASIC design、EDA/IP、
HBM/CoWoS 和 networking？

公司池：Broadcom、Marvell、Alchip、GUC、Faraday、Arm、Rambus、Synopsys、
Cadence、TSMC、ASE、AWS、Google、Microsoft、Meta。

下一轮 prompt：

> 请按 Google TPU、AWS Trainium、Microsoft Maia、Meta MTIA 拆解 custom ASIC 供应链，哪些公开公司受益最大？

### Theme 7：NeoCloud 经济模型与信用风险

研究问题：NeoCloud 是高增长云平台，还是 GPU 租赁 + 高杠杆基础设施周期？

公司池：CoreWeave、Nebius、Oracle、Lambda、Crusoe、IREN、Applied Digital、
Hut 8、TeraWulf、Core Scientific、DigitalOcean。

下一轮 prompt：

> 请建立 NeoCloud 单位经济模型：每 MW、每 GPU、每 rack 的收入、成本、折旧、融资和反证指标。

### Theme 8：AI server ODM 与液冷 rack

研究问题：Rack-scale AI 是否让 ODM 从低毛利组装商升级为系统集成 bottleneck？

公司池：Quanta/QCT、Wiwynn、Wistron、Foxconn、Inventec、Gigabyte、Supermicro、
Dell、HPE、Lenovo、Celestica、Jabil。

下一轮 prompt：

> 请比较台湾 AI server ODM：Quanta、Wiwynn、Wistron、Foxconn、Inventec、Gigabyte 的 AI 收入、毛利和客户风险。

### Theme 9：电力设备、变压器、UPS、PDU、液冷

研究问题：AI 数据中心的真正瓶颈会不会从 GPU 转到电力设备和冷却？

公司池：Vertiv、Schneider、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta、
Lite-On、AcBel、Vicor、MPS、Infineon、Fuji Electric、Mitsubishi Electric、
Nidec、Munters、Alfa Laval、Modine。

下一轮 prompt：

> 请把 AI 数据中心电力与冷却链条从变压器到 rack 级 liquid cooling 拆开，列公司池、指标和反证。

### Theme 10：AI eSSD、NAND、存储控制器

研究问题：AI 是否让 enterprise SSD 成为 NAND 超级周期的新核心，还是只是传统
NAND 反弹？

公司池：Samsung、SK hynix/Solidigm、Micron、Kioxia、WDC/SanDisk、Phison、
Silicon Motion、Marvell、Pure Storage、NetApp、Weka、VAST。

下一轮 prompt：

> 请区分 AI 对 NAND/eSSD 的真实需求和传统存储周期反转，按公司列出验证指标。

### Theme 11：硅光、InP、激光与化合物半导体

研究问题：1.6T/CPO 是否会制造 InP laser、external light source、硅光 foundry
的新瓶颈？

公司池：Sivers、IQE、Coherent、Lumentum、MACOM、Tower、GlobalFoundries、
Ayar Labs、POET、DustPhotonics、Soitec、Aixtron。

下一轮 prompt：

> 请深挖非美硅光/化合物半导体小盘：Sivers、IQE、Soitec、Aixtron、Tower、POET 的 AI 相关性和反证。

### Theme 12：半导体材料、气体、真空、洁净室

研究问题：AI leading-edge、HBM、CoWoS、硅光是否让材料规格升级形成长期
ASP/volume 双击？

公司池：Ajinomoto、Resonac、Shin-Etsu、SUMCO、GlobalWafers、Siltronic、
Soitec、JSR、Tokyo Ohka、Fujifilm、Merck、Entegris、Soulbrain、Dongjin、
SK Materials、VAT、Organo、Kurita。

下一轮 prompt：

> 请把 AI 半导体材料链条拆成 ABF、photoresist、slurry、gas、wafer、UPW、vacuum，并找非美小盘。

### Theme 13：AI 数据中心能源：gas、nuclear、SMR、fuel cell、grid

研究问题：AI 数据中心会不会重塑美国和全球电力负荷增长，并催生新型能源资产？

公司池：GE Vernova、Siemens Energy、Mitsubishi Heavy、Bloom Energy、
Constellation、Vistra、Talen、NRG、Cameco、Kazatomprom、LEU、NuScale、
Oklo、BWXT、Fluence。

下一轮 prompt：

> 请分析 AI 数据中心电力：核能、SMR、天然气、燃料电池、储能、变压器，哪些是真瓶颈，哪些是叙事？

### Theme 14：AI Infra 反证系统：效率、价格、ROI、融资

研究问题：什么情况会证明 AI infra capex 过度？哪些指标最早预警？

公司池：Hyperscalers、NeoCloud、NVIDIA、memory vendors、optical vendors、
power equipment、data center REITs。

下一轮 prompt：

> 请为 AI Infra 建立一个反证仪表盘：哪些财报和产业指标一旦变化，说明周期从结构性转为泡沫？

## 7. Priority For 10x / Index-Elasticity Hunting

### First Priority

1. HBM 设备、测试、材料、基板。
   - 逻辑：HBM 是最直接的 AI 内存瓶颈，memory vendor 已大市值化，更高弹性可能在
     TC bonder、hybrid bonding、tester、probe、ABF、substrate、underfill、inspection。
   - 反证：HBM 供给过快释放、设备订单一次性、HBM4 技术路线降低部分环节价值量。

2. 光互连 / CPO / InP / 激光 / 硅光。
   - 逻辑：AI cluster 扩大后网络和功耗是非线性瓶颈，800G -> 1.6T -> CPO
     可能带来光子材料、激光、DSP、测试和封装需求。
   - 反证：CPO 延后、中国光模块价格战、pluggable optics 生命周期延长、DSP 被
     LPO/CPO 部分替代。

3. 电力设备、液冷、热管理、变压器。
   - 逻辑：AI 数据中心从买 GPU 变成拿到电、把热带走、把 rack 点亮。
   - 反证：数据中心延期、订单提前透支、客户要求降价、电网许可而非设备成为真正瓶颈。

4. Custom ASIC 供应链和 scale-up connectivity。
   - 逻辑：hyperscaler 自研 ASIC 增速持续时，价值扩散到 ASIC design、EDA/IP、
     CoWoS、HBM、PCIe/CXL、network fabric。
   - 反证：ASIC 只替代部分 workload、软件生态不足、NVIDIA 系统级优势持续、
     单一客户议价强。

5. 非美材料/设备隐形冠军。
   - 逻辑：日本、韩国、台湾、欧洲有大量不是 AI 公司但被 AI 规格升级拉动的公司。
   - 反证：AI 收入占比太低、订单周期性、估值已反映、缺乏定价权。

### Second Priority

6. eSSD / enterprise NAND / controller。
   - 有真实 AI 需求，但混入传统 NAND 周期概率高。

7. NeoCloud / AI data center developers。
   - 弹性大，但信用和融资风险同样大。

8. AI server ODM。
   - 需求大，但毛利低，必须验证液冷/rack-scale 集成是否带来结构性毛利提升。

### Third Priority

9. 泛 memory 叙事。
10. 泛能源 / SMR / 核能叙事。
11. 泛半导体设备大盘 beta。
12. 低毛利 AI server 组装。

这些方向容易只是短期叙事，必须更严格要求 source evidence、订单、交付、毛利和
现金流验证。

## 8. Practical 10x Screening Formula

| Dimension | Question | High-Score Feature |
|---|---|---|
| 需求确定性 | 是否被 GPU/ASIC/HBM/AI DC 出货直接拉动？ | 与 HBM、CoWoS、CPO、液冷、电力强绑定 |
| 供给弹性 | 扩产是否慢？ | 设备复杂、材料认证长、客户切换难 |
| 单位价值量 | 每代 AI 芯片/每 rack 用量是否上升？ | HBM stack、光端口、液冷、测试时间、ABF 层数上升 |
| 议价权 | 是否有技术壁垒或寡头结构？ | 高份额、专利、认证、长期客户 |
| 市值弹性 | 当前收入/市值是否小于潜在 TAM？ | 小中盘、AI revenue 初期、订单刚开始反映 |
| 财报验证 | 是否已在订单/收入/毛利中出现？ | backlog、book-to-bill、gross margin 同时改善 |
| 反证可跟踪 | 能否用 3-5 个指标及时否定？ | 有明确订单、产能、价格、客户指标 |

最优先交叉点：

`HBM/CoWoS 设备测试材料 × 光互连/CPO × 电力液冷 × 非美隐形供应链 × 小中市值`

这几个方向最符合 AI infra 时代一起指数增长的资产特征，也最适合继续做深度拆解。

## 9. Report And Pipeline Requirements

日报、周报和 agent 输出应遵守：

1. 先说明当前 AI infra 哪些层在领涨。
2. 对每个候选，说明属于哪条主线和哪一层。
3. 区分 `theme_prior`、`price_flow_first_no_current_news`、`source_linked_supply_evidence`。
4. 对供应链关系必须给 source review 状态。
5. 对未确认关系只能写 research requirement，不能写成正式买入理由。
6. 对每个执行候选必须写 trade plan、invalidation、hedge 和 residual risk。
7. 所有用户输入的财报数字和行业预测必须进入 source-review 队列，复核后才可进入正式证据 ledger。

## 10. Next Research Backlog

优先连续研究：

1. HBM3E/HBM4/HBM4E vendor、设备、材料、测试、封装、基板。
2. CoWoS 供应链中 TSMC 之外的瓶颈。
3. AI/HBM 测试时间拉长带来的收入弹性。
4. 800G/1.6T/CPO/硅光/外置光源/InP。
5. NVLink、UALink、PCIe/CXL、InfiniBand、Ethernet 的价值分配。
6. TPU/Trainium/Maia/MTIA 的 custom ASIC 供应链。
7. NeoCloud 单位经济模型与信用风险。
8. AI 数据中心电力与冷却链条。
9. enterprise SSD / controller 与传统存储周期的区分。
10. AI infra 反证仪表盘。
