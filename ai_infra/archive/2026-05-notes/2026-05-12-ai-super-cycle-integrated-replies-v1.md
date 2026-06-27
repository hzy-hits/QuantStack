# AI Super Cycle 回复整合版 v1

日期：2026-05-12  
状态：整合稿，待原始出处核验  
项目：ai super cycle / AI Infra 产业链研究

> 使用边界：本文整合 ChatGPT Pro 输出、项目框架文档和本地整理结果，只作为研究地图、问题清单和核验路线，不是投资建议或买卖建议。所有涉及收入、订单、backlog、CapEx、毛利率、产能、ASP、客户关系和技术路线的内容，必须回到公司原始披露、交易所公告、监管文件、公司官网技术资料或上下游交叉披露核验。

## 1. 当前总判断

本项目应从“泛 AI Infra 分层”升级为 `LLM Dependency BFS`：

```text
D0 LLM 核心源头
  -> D1 模型强依赖一阶
  -> D2 一阶依赖的直接瓶颈
  -> D3 二阶瓶颈供应商
  -> D4-D5 深层雷达
```

研究主战场是 `D1-D3`，最深看到 `D5`。  
`D4-D5` 不应直接当核心资产结论，只作为雷达，除非能证明它反向卡住 `D0-D2`。

本轮 Pro 输出形成的共同结论是：

- AI Infra 不是单线 GPU 链，而是从 LLM token demand 向 GPU/TPU、HBM、CoWoS、网络、光互连、数据中心电力、冷却、测试、材料、基板、NeoCloud 融资结构扩散的多层约束系统。
- 公开市场里，很多最高弹性不一定在 `D1` 龙头，而可能在 `D2-D3` 的瓶颈供应商：HBM/CoWoS 设备测试材料、光互连/CPO/InP/硅光、电力液冷、高速 PCB/CCL、AI server rack power、部分非美设备/材料小中盘。
- NeoCloud 不能直接按软件云平台看，要按 GPU 租赁、重资产数据中心和项目融资周期拆解。高 backlog 不等于高质量 FCF。
- 存储超级周期可能是 AI Infra 扩散信号，但必须拆开 HBM、server DRAM、enterprise SSD、NAND、controller。HBM 最纯，NAND/eSSD 最容易混入传统周期。
- A 股映射可以启动，但只能做候选池和核验清单。A 股公司必须标注 BFS 深度，优先映射 `D1-D3`；`D4-D5` 只进观察池。

## 2. 文档入口

### 主框架文档

| 文档 | 用途 |
| --- | --- |
| [README.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/README.md) | 项目总入口 |
| [llm-dependency-bfs-framework.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/llm-dependency-bfs-framework.md) | D0-D5 BFS 主框架 |
| [research-checklist.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/research-checklist.md) | 原文核验、反证、评分基线 |
| [source-evidence-template.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/source-evidence-template.md) | 证据卡片模板 |
| [chatgpt-pro-agent-prompts.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/chatgpt-pro-agent-prompts.md) | ChatGPT Pro 多会话任务包 |

### Pro 输出和摘要

| 批次 | 主题 | 文件 |
| --- | --- | --- |
| 总图 | 初版模块补全 | [2026-05-12-chatgpt-pro-module-map-v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-module-map-v1.md) |
| 研究地图 | AI Super Cycle 研究地图 v1 | [2026-05-12-ai-super-cycle-research-map-v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-ai-super-cycle-research-map-v1.md) |
| 第一批 | HBM | [2026-05-12-chatgpt-pro-hbm-structural-super-cycle.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-hbm-structural-super-cycle.md) |
| 第一批 | CoWoS / Advanced Packaging | [2026-05-12-chatgpt-pro-cowos-advanced-packaging.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-cowos-advanced-packaging.md) |
| 第一批 | AI / HBM Testing | [2026-05-12-chatgpt-pro-ai-hbm-testing-metrology.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-ai-hbm-testing-metrology.md) |
| 第一批摘要 | HBM / CoWoS / Testing 摘要 | [2026-05-12-chatgpt-pro-results-summary.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-results-summary.md) |
| 第二批 | Optical / CPO / Silicon Photonics | [2026-05-12-chatgpt-pro-optical-cpo-silicon-photonics.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-optical-cpo-silicon-photonics.md) |
| 第二批 | Scale-up Fabric / Custom ASIC | [2026-05-12-chatgpt-pro-scaleup-fabric-custom-asic.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-scaleup-fabric-custom-asic.md) |
| 第二批 | Power / Cooling / Thermal | [2026-05-12-chatgpt-pro-power-cooling-thermal.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-power-cooling-thermal.md) |
| 第二批摘要 | 光互连 / Custom ASIC / 电力液冷摘要 | [2026-05-12-chatgpt-pro-batch2-results-summary.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-batch2-results-summary.md) |
| 第三批 | NeoCloud 经济模型 | [2026-05-12-chatgpt-pro-neocloud-economics.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-neocloud-economics.md) |
| 第三批 | 非美材料 / 设备隐形冠军 | [2026-05-12-chatgpt-pro-non-us-hidden-champions.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-non-us-hidden-champions.md) |
| 第三批 | 存储超级周期反证 | [2026-05-12-chatgpt-pro-storage-supercycle-refutation.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-storage-supercycle-refutation.md) |
| 第三批摘要 | NeoCloud / 非美隐形冠军 / 存储反证摘要 | [2026-05-12-chatgpt-pro-batch3-results-summary.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-batch3-results-summary.md) |
| A 股 | A 股主板映射 v1 | [2026-05-12-chatgpt-pro-a-share-mainboard-mapping-v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-a-share-mainboard-mapping-v1.md) |
| A 股 | 本地预筛观察池 | [2026-05-12-a-share-mainboard-ai-infra-watchlist.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-a-share-mainboard-ai-infra-watchlist.md) |
| A 股 | A 股映射启动方案 | [2026-05-12-a-share-mapping-launch-plan.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-a-share-mapping-launch-plan.md) |
| 建模 | BFS 结论建模提问稿 | [2026-05-12-chatgpt-pro-bfs-conclusion-modeling-prompt.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-bfs-conclusion-modeling-prompt.md) |
| 建模 | BFS 结论建模输出 | [2026-05-12-chatgpt-pro-bfs-conclusion-modeling.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-bfs-conclusion-modeling.md) |
| 运行记录 | Pro 会话日志 | [2026-05-12-chatgpt-pro-run-log.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-run-log.md) |
| 状态 | 模块推进状态 | [2026-05-12-ai-infra-module-status.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-ai-infra-module-status.md) |

## 3. D0-D5 统一归位

| BFS 深度 | 研究对象 | 已有回复覆盖 | 当前判断 |
| --- | --- | --- | --- |
| `D0` | OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI | 研究地图、BFS 框架 | 需求源头，不一定有直接公开标的，但决定 token demand、训练/推理节奏和 CapEx 方向 |
| `D1` | NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈 | Scale-up / Custom ASIC、NeoCloud、研究地图 | 核心一阶，必须跟踪模型路线、GPU/ASIC、云合同、推理软件效率 |
| `D2` | HBM、CoWoS、TSMC、AI server/rack、networking、800G/1.6T optics、data center power/cooling | HBM、CoWoS、Optical、Power、Storage | 直接瓶颈层，是 AI Infra 主线验证重点 |
| `D3` | HBM testing/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件 | Testing、Non-US hidden champions、Optical、Power、A 股映射 | 最高弹性区，尤其适合非美小中盘和 A 股主板候选映射 |
| `D4` | 化学品、气体、真空、洁净、衬底、精密加工 | Non-US hidden champions、研究地图 | 雷达区，必须证明反向卡住 D2/D3 才能升级 |
| `D5` | 能源、电网、土地、水、融资、监管 | Power、NeoCloud、研究地图 | 远端约束区，更多用于解释 bottleneck 和反证，不直接当核心结论 |

## 4. 模块整合结论

### 4.1 HBM

归位：`D2` 直接瓶颈；HBM 设备/测试/材料进入 `D3`。  
Pro 结论：HBM 可能已经从 2-3 年供需错配，转向高端 AI accelerator 的长期结构性内存标准。

需要核验：

- NVIDIA / AMD accelerator 的 HBM 容量、带宽、stack 数代际变化。
- JEDEC HBM4 标准。
- SK hynix、Samsung、Micron 的 HBM revenue、mix、ASP、margin、capacity、qualification。
- Hanmi、ASMPT、BESI、SUSS、TOWA、DISCO、Advantest、Teradyne 等 HBM 设备/测试收入和订单。

主要反证：

- HBM 供给过快释放，ASP 下行。
- HBM 设备订单一次性。
- HBM4 技术路线降低现有设备/材料价值量。

### 4.2 CoWoS / Advanced Packaging

归位：CoWoS / advanced packaging 为 `D2`；substrate、bonding、inspection、metrology 为 `D3`。  
Pro 结论：CoWoS 仍是一级瓶颈，但瓶颈不只在 TSMC，也可能扩散到 ABF/substrate、TCB/hybrid bonding、TBDB、dicing/grinding、molding、inspection/metrology。

需要核验：

- TSMC advanced packaging capacity tightness、CoWoS 扩产和 OSAT partner 表述。
- Ibiden、AT&S、Shinko、Unimicron、Nan Ya PCB、Kinsus 的 AI substrate / ABF / glass / T-glass 披露。
- ASMPT、BESI、SUSS、TOWA、DISCO、Camtek、Nova 的订单、backlog、AI/HBM/2.5D 收入口径。

主要反证：

- TSMC / OSAT 扩产后瓶颈缓解。
- 基板和设备只是传统半导体 beta。
- advanced packaging 订单无法传导到毛利率。

### 4.3 AI / HBM Testing

归位：`D3`。  
Pro 结论：AI/HBM 复杂度确实可能让测试、探针、量测/检测成为高弹性瓶颈，但不能把所有测试公司都映射为 AI 受益。

可用拆分：

- ATE：Advantest、Teradyne。
- Probe card / socket / interface：Technoprobe、FormFactor、MPI、WinWay、ISC、Leeno、TSE。
- Advanced packaging inspection / metrology：Camtek、Nova、Onto、KLA、Lasertec。

质量提示：

- 该 Pro 输出有 `insurance` 上下文污染，初始信任应降低。
- KLA、Nova、Onto、Lasertec 是宽口径 process control / metrology，不能直接等同 HBM 专用瓶颈。

### 4.4 Optical / CPO / Silicon Photonics

归位：800G/1.6T optics 为 `D2`；InP laser、ELS、SiPh、DSP/TIA/driver、CPO packaging 为 `D3`。  
Pro 结论：光互连正在从电信周期扩展为 AI cluster 算力基础设施周期，但必须证明它绑定东西向流量、scale-out/scale-up fabric、1.6T pluggable、LPO/LRO/TRO、CPO/ELS、SiPh。

需要核验：

- Coherent、Lumentum 的 InP / EML / CW laser / 1.6T 收入口径。
- Broadcom、Marvell、Credo、Semtech、MACOM 的 DSP、TIA/driver、LPO/LRO/CPO 和 AI networking 收入口径。
- Sivers、IQE、Tower、GlobalFoundries、Ayar Labs、POET 的 CPO / ELS / SiPh / InP 订单和现金流。
- Corning 的 AI data center 光连接长约和产能扩张。

主要反证：

- CPO 延后，pluggable optics 生命周期拉长。
- 铜缆/AEC 在短距连接中延寿。
- 光模块 ASP 下行，收入涨但毛利率不涨。

### 4.5 Scale-up Fabric / Custom ASIC

归位：GPU/TPU/ASIC 为 `D1`；networking / fabric / PCIe-CXL / retimer / SerDes / EDA/IP 为 `D2-D3`。  
Pro 结论：hyperscaler ASIC 不是简单替代 GPU，而是把价值从单卡扩散到 ASIC design、EDA/IP、HBM、CoWoS、SerDes、retimer/AEC、switch/NIC、rack power/cooling。

需要核验：

- Google TPU、AWS Trainium、Microsoft Maia、Meta MTIA 官方资料。
- Broadcom、Marvell custom silicon / XPU / AI networking 原文。
- Astera、Credo、Rambus、Alphawave、Arm、Synopsys、Cadence 的 PCIe/CXL/SerDes/HBM IP 暴露。
- Alchip、GUC、Faraday 的 NRE、turnkey、mass production 和客户集中。

主要反证：

- ASIC 需求主要内化在 hyperscaler，不外溢给公开供应链。
- CUDA / NVLink / NVIDIA full-stack 继续压倒开放生态。
- HBM、CoWoS、软件栈或工作负载限制 ASIC 放量。

### 4.6 Power / Cooling / Thermal

归位：data center power/cooling 为 `D2`；UPS、PDU、transformer、switchgear、CDU、cold plate、power module 为 `D3`；电网/能源为 `D5`。  
Pro 结论：AI 数据中心电力链条必须从需求源头、data center developer、grid interconnect、substation、transformer、switchgear、UPS/PDU、rack power、liquid cooling、heat rejection 分层验证。

需要核验：

- Vertiv、Schneider、Eaton 的 data center orders、backlog、book-to-bill、gross margin、liquid cooling attach。
- Siemens Energy、ABB/Hitachi Energy 的 grid / transformer / switchgear backlog 和 data center 订单。
- Delta、Lite-On、AcBel、Vicor、MPS、Infineon 的 AI server / rack power / 48V / power module 收入口径。
- Munters、Alfa Laval、Modine、CoolIT、Boyd 的 CDU/cold plate/heat exchanger 认证和订单。

主要反证：

- 真瓶颈是电网许可，不是设备供给。
- 订单提前透支，客户推迟建设。
- 液冷标准化后 ASP 下行。

### 4.7 NeoCloud / GPU-as-a-Service

归位：云算力 / GPUaaS 为 `D1`；data center economics、融资结构为 `D5` 约束。  
Pro 结论：NeoCloud 更像“高增长重资产 AI 基础设施 + GPU 租赁 + 项目融资周期”，不能直接按软件云平台估值。高 backlog 只是一层线索，必须看 funded backlog、交付 MW、GPU 利用率、客户预付款、折旧、利息、GPU 残值和 FCF。

需要核验：

- CoreWeave、Nebius、Oracle、Lambda、Crusoe、IREN、Applied Digital、Hut 8、TeraWulf、Core Scientific 的 10-K/20-F/10-Q、earnings release、presentation、call transcript。
- 每 GPU / rack / MW 的 revenue、CapEx、O&M、电力、折旧、利息、租赁义务和 FCF。
- backlog / RPO 是否有 take-or-pay、prepayment、parent guarantee、cancellation terms。

主要反证：

- 收入增长但 gross margin、operating cash flow、FCF 不改善。
- backlog 增长但 cash collection 不同步。
- debt / EBITDA、interest expense、depreciation 同时上升。
- GPU 残值下跌，客户从租赁转向自建。

### 4.8 Non-US Hidden Champions

归位：主要是 `D3-D4`，部分电力/能源公司在 `D5`。  
Pro 结论：非美小中盘不能只按“半导体 + AI 标签”入池，必须证明它直接卡在 HBM、CoWoS、advanced packaging、CPO/SiPh、AI server power/cooling、基板、测试、真空、洁净室或关键材料规格升级中。

优先国家/地区：

- 日本：TOWA、Disco、Tazmo、Shibaura、Advantest、Lasertec、Ibiden、Shinko、Ajinomoto、Resonac、Shin-Etsu、SUMCO、Organo、Kurita。
- 韩国：Hanmi、Leeno、ISC、TSE、Soulbrain、Dongjin、SK Materials、Wonik、HPSP、Nextin。
- 台湾：TSMC、ASE、Powertech、KYEC、Chroma、MPI、WinWay、Unimicron、Nan Ya PCB、Kinsus、Delta、Lite-On、Alchip、GUC、Faraday。
- 欧洲 / 以色列：BESI、SUSS、ASM International、VAT、Soitec、IQE、Aixtron、Sivers、Infineon、Schneider、Siemens Energy、Camtek、Nova、Tower。

主要反证：

- AI 收入无法拆出，收入来自消费电子、汽车、工业或普通半导体周期。
- 只有 design win / product page，没有订单、backlog、产能、认证或毛利率改善。

### 4.9 Storage Supercycle Refutation

归位：HBM 为 `D2`；HBM 设备测试材料为 `D3`；NAND/eSSD/controller 根据客户和收入证据归入 `D2-D4`。  
Pro 结论：存储超级周期可能是 AI Infra 从 GPU 向更宽物理供应链扩散的信号，但不能把所有 memory 价格上涨都归因于 AI。

拆分：

- HBM：最接近真实 AI 结构性瓶颈。
- Server DRAM：AI + 供给挤出 + 传统周期混合。
- Enterprise SSD / NAND：有 AI 数据管线需求，但最容易混入传统 NAND 周期。
- Controller：小而有弹性，但必须区分 enterprise / hyperscaler 与 consumer SSD。

主要反证：

- 上涨主要来自 wafer start cuts、库存去化、PC/mobile/consumer SSD 修复。
- HBM / server DRAM / eSSD / controller 没有独立收入和毛利率改善。
- memory vendor CapEx 过度扩张导致供给反转。

## 5. A 股映射整合

A 股主板映射已经有 v1，但它发出早于 D0-D5 BFS 框架补入，所以必须重整成 `A股映射 v2`。

### 5.1 A 股映射原则

- 不从 A 股概念出发，先从海外 D1-D3 瓶颈出发。
- 排除创业板 `300/301`、科创板 `688/689`、北交所。
- 002 系列按深市主板处理，但后续仍要用交易所列表复核板块、简称、风险警示。
- 每家公司必须标注：BFS depth、海外瓶颈、海外可比公司、A 股位置、原始出处、财报指标、主要反证。
- 只做候选池和核验优先级，不做买卖建议或目标价。

### 5.2 A 股候选按 BFS 重整

| BFS 深度 | A 股映射方向 | 代表候选 | 状态 |
| --- | --- | --- | --- |
| `D1` | AI server / rack-scale systems、网络设备、云/算力基础设施 | 工业富联、浪潮信息、紫光股份、中兴通讯、宝信软件、数据港 | 候选，需看毛利率、客户集中、现金流和项目经济模型 |
| `D2` | AI server PCB / CCL、高速交换机 PCB、光互连、数据中心电力/冷却 | 沪电股份、深南电路、生益科技、光迅科技、华工科技、剑桥科技、科华数据、科士达、英维克 | 主板映射最清晰的一组，优先核验 |
| `D3` | OSAT/先进封装、半导体设备/材料、液冷组件、电力设备关键部件、高速连接/线缆 | 长电科技、通富微电、兴森科技、北方华创、雅克科技、至纯科技、金海通、立讯精密、沃尔核材、麦格米特、思源电气、华明装备 | 高弹性候选，但需要严格拆 AI/HBM/CoWoS/数据中心收入 |
| `D4-D5` | 材料、化学品、功率器件、泛电网、能源、IDC 融资结构 | 江化微、有研新材、兆易创新、德明利、士兰微、斯达半导、新洁能、特变电工、中国西电、平高电气、国电南瑞 | 雷达/观察池，不能直接当核心结论 |

### 5.3 A 股原文核验优先级

第一组：AI server PCB / CCL  
候选：沪电股份、深南电路、生益科技、兴森科技、华正新材。  
核验：AI server / switch PCB 收入、低损耗材料、高层数板、毛利率、扩产、客户。

第二组：光互连 / 800G / 1.6T  
候选：光迅科技、华工科技、剑桥科技。  
核验：datacom vs telecom、800G/1.6T 出货、客户、ASP、毛利率、CPO/LPO 是否只是研发。

第三组：电力 / UPS / 液冷  
候选：科华数据、科士达、英维克、麦格米特、特变电工、中国西电、思源电气、华明装备。  
核验：数据中心订单、UPS/HVDC、rack power、液冷收入、backlog、交期、毛利率。

第四组：先进封装 / OSAT / 基板  
候选：长电科技、通富微电、华天科技、兴森科技、深南电路。  
核验：HPC/AI/advanced packaging 收入、CoWoS-like / 2.5D / SiP / FCBGA 能力、CapEx、良率、客户。

第五组：设备 / 材料 / 存储  
候选：北方华创、至纯科技、雅克科技、江化微、有研新材、金海通、深科技、兆易创新、德明利。  
核验：是否真实对应 HBM/CoWoS/DRAM/foundry/enterprise SSD，而不是国产半导体 beta 或普通存储周期。

## 6. 结论分层模板

后续每家公司都用下面四层结论，不直接写“好/坏”：

| 结论层 | 定义 | 示例句 |
| --- | --- | --- |
| 核心池 | `D1-D3`，且已有原文证据证明客户、订单、收入、产品或技术路线与 LLM 训练/推理强相关 | `D2 直接瓶颈，已有原文证据支持，进入核心池，继续跟踪毛利率和 FCF 传导。` |
| 候选池 | `D2-D4`，逻辑强但原文证据仍不足 | `D3 高弹性候选，但 AI/HBM/CoWoS 收入仍待原文核验。` |
| 雷达池 | `D4-D5`，可能是隐形约束但传导尚未证明 | `D4 雷达项，目前只能解释上游约束，不能作为核心 AI Infra 结论。` |
| 排除池 | `D6+` 或边关系不清，只靠主题映射 | `主题映射较强，但缺少客户、订单、收入和技术边证据，暂时排除。` |

## 7. BFS 结论建模补充

BFS 结论建模 Pro 输出已经抓取，核心规则可以直接进入 v2 工作流：

- 没有 BFS depth，不写公司结论。
- 没有原始出处，不写“原文已证明”。
- 没有 strong / medium dependency edge，不进核心池。
- 没有反证，不进入核心池。
- `D4-D5` 没有反向卡住 `D0-D2` 的证据，只能做雷达。
- 超过 `D5` 默认排除。

### 7.1 边关系模型

| 边类型 | 作用 | 强证据 |
| --- | --- | --- |
| 客户边 | 证明下游需求能传导到公司 | 客户披露、长约、订单、收入分部 |
| BOM 边 | 证明产品进入关键 BOM | 产品规格、供应链披露、客户认证 |
| 产能边 | 证明它可能形成瓶颈租金 | backlog、交期、扩产周期、产能锁定 |
| 技术边 | 证明它不是普通供应商，而是架构升级受益者 | 技术白皮书、标准、参数、路线图 |
| 现金流边 | 证明相关性能传到收入、毛利、FCF | revenue、gross margin、CapEx、FCF |
| 反证边 | 防止远端节点被叙事越级 | 风险因素、供应约束、客户表述 |

### 7.2 评分模型

Pro 输出建议使用 100 分或 5 星模型，核心维度包括：

- BFS depth 与路径完整性。
- 需求真实性。
- 供给瓶颈强度。
- 议价权 / 毛利率传导。
- 财务传导到 FCF。
- 技术替代风险。
- 客户集中风险。
- 证据质量。
- 反证清晰度。
- 10 倍弹性 / 指数增长可能性。

核心门槛：

- `85-100`：核心瓶颈或强 D3 高弹性候选。
- `70-84`：候选池，继续补原文证据。
- `50-69`：雷达池或弱候选。
- `<50`：排除池或仅主题映射。

### 7.3 A 股映射模型

A 股映射必须走这个方向：

```text
海外 D0-D3 瓶颈
  -> 具体产品/技术/BOM/产能卡点
  -> 中国主板公司是否真实供应
  -> 原始出处核验
  -> BFS depth 标注
  -> 评分
  -> 结论池
```

禁止路径：

```text
A 股 AI 概念
  -> 找故事
  -> 反向拼产业链
  -> 直接放入核心池
```

## 8. 统一证据卡片

```markdown
## 公司 / 模块

| 字段 | 内容 |
| --- | --- |
| BFS depth | D1 / D2 / D3 / D4 / D5 |
| 对应海外瓶颈 |  |
| 对应海外公司 |  |
| 上一层依赖 |  |
| 边关系 | 客户边 / BOM 边 / 产能边 / 技术边 / 现金流边 / 反证边 |
| 原始出处 | 年报 / 季报 / call / presentation / 公告 / 官网技术资料 |
| 原文已证明 |  |
| 合理推论 |  |
| 待原文核验 |  |
| 主要反证 |  |
| 财报指标 | revenue / gross margin / backlog / CapEx / inventory / FCF |
| 当前分池 | 核心池 / 候选池 / 雷达池 / 排除池 |
```

## 9. 下一步执行顺序

1. 启动 `AI Infra 全球可交易候选池深度研究`。  
   覆盖 A 股主板、美股/ADR、港股、欧洲、日本、韩国、台湾、以色列和其他可交易小中盘，输出全球 universe、ETF 覆盖、smart money 框架和组合构建模板。

2. 生成 `A股映射 v2`。  
   把 A 股 v1 候选公司按 `D1-D5` 重排，标注海外瓶颈、海外公司、边关系、证据等级、评分和反证。

3. 做第一轮原始出处核验。  
   优先三组：AI server PCB/CCL、光互连、数据中心电力/液冷。

4. 建立证据卡片。  
   每家公司至少记录：年报/半年报/一季报、IR 活动记录、官网技术资料、重大合同/产能公告、毛利率/库存/现金流。

5. 再启动总反证仪表盘。  
   重点监控：hyperscaler CapEx、cloud margin、RPO conversion、HBM ASP/capacity、CoWoS capacity、optical ASP、power equipment backlog、NeoCloud debt/utilization、token price/tokens per watt。

## 10. 需要特别防的误判

- 把普通半导体周期反转当作 AI 结构性增长。
- 把低毛利 ODM / 集成收入增长当作瓶颈利润池迁移。
- 把电网设备泛受益当作数据中心直接订单。
- 把电信光通信当作 AI datacom。
- 把普通散热当作高功率液冷。
- 把算力租赁项目公告当作 NeoCloud 正现金流模型。
- 把材料/设备国产替代当作 HBM/CoWoS 直接瓶颈。

## 11. 当前最重要的工作入口

下一步默认从这里开始：

1. [llm-dependency-bfs-framework.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/llm-dependency-bfs-framework.md)
2. [2026-05-12-ai-super-cycle-integrated-replies-v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-ai-super-cycle-integrated-replies-v1.md)
3. [2026-05-12-chatgpt-pro-bfs-conclusion-modeling.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-bfs-conclusion-modeling.md)
4. [2026-05-12-chatgpt-pro-a-share-mainboard-mapping-v1.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/notes/2026-05-12-chatgpt-pro-a-share-mainboard-mapping-v1.md)
5. [source-evidence-template.md](/Users/huangzhenyu/Desktop/05_学习项目/ai_infra/docs/source-evidence-template.md)
