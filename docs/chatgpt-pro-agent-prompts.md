# ChatGPT Pro 多会话研究任务包

项目：ai super cycle
用途：把 AI Infra 研究拆成多个 ChatGPT Pro 会话慢速推进，避免单会话过大、同步阻塞或打满限速。

## 全局系统约束

每个会话都要遵守：

```text
你是 AI Infra 产业链研究助手。目标不是给投资建议，也不是推荐买卖，而是帮助建立 source-backed research map。

所有涉及实际数据、财报数字、订单、backlog、CapEx、毛利率、产能、价格、出货、客户关系的内容，必须先列出需要核验的公司原始出处。公司原始出处包括 annual report、10-K/20-F、quarterly report、earnings release、earnings call transcript、investor presentation、交易所公告、公司官网技术资料。

请严格区分：
1. 原文已证明；
2. 合理推论；
3. 待原文核验；
4. 主要反证。

不要输出买入/卖出建议，不要给目标价。不要把媒体、券商摘要或模型记忆当成事实。先列 source checklist，再做产业链判断。
```

## 全局研究框架：LLM Dependency BFS

所有会话优先使用从 LLM Lab 出发的 BFS 分层：

- `D0`：LLM 核心需求源头，例如 OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI。
- `D1`：模型强依赖一阶，例如 NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈。
- `D2`：一阶依赖的直接瓶颈，例如 HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
- `D3`：二阶瓶颈供应商，例如 HBM equipment/test/probe、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、CXL/PCIe retimer、液冷组件、电力设备关键部件。
- `D4-D5`：更深层材料、设备、化学品、气体、真空、洁净、能源、电网、融资、监管，只作为雷达区。

研究重点放在 `D1-D3`，最深看到 `D5`。每家公司或环节都要说明 BFS 深度、与上一层的边关系、证据类型和主要反证。

## 会话 1：HBM 结构性超级周期

```text
请研究 HBM 是否已经从 2-3 年供需错配进入 AI accelerator 的长期结构性内存标准。

重点拆分：
- HBM3E / HBM4 / HBM4E
- 8-high / 12-high / 16-high
- memory vendors：SK hynix、Samsung、Micron
- equipment：Hanmi、ASMPT、BESI、SUSS、TOWA、Disco
- testing：Advantest、Teradyne、Chroma、MPI、WinWay、Leeno、ISC、TSE
- substrate/material：Ibiden、Shinko、Unimicron、Ajinomoto、Resonac

输出：
1. 原始出处清单；
2. 产业链地图；
3. 财报验证指标；
4. 技术路线变化；
5. 可能的 10 倍弹性环节；
6. 反证指标。
```

## 会话 2：CoWoS / 2.5D / Advanced Packaging

```text
请研究 CoWoS / 2.5D / advanced packaging 是否仍是 AI 芯片出货瓶颈，以及瓶颈是否会从 TSMC 转向 substrate、interposer、设备、材料、测试。

重点公司：
TSMC、ASE、Amkor、Ibiden、Shinko、Unimicron、Nan Ya PCB、Kinsus、AT&S、BESI、SUSS、ASMPT、TOWA、Disco、Camtek、Nova。

输出：
1. 原始出处清单；
2. CoWoS 供应链地图；
3. 各环节供给瓶颈；
4. HBM4/HBM4E 对封装设备和材料的影响；
5. 财报验证指标；
6. 反证指标。
```

## 会话 3：AI / HBM Testing

```text
请研究 AI 芯片复杂度是否让测试时间、测试设备、probe card、inspection/metrology 成为高弹性瓶颈。

重点公司：
Advantest、Teradyne、Chroma、MPI、WinWay、FormFactor、Technoprobe、Leeno、ISC、TSE、Camtek、Nova、Onto、KLA、Lasertec。

输出：
1. 原始出处清单；
2. 从 wafer probe 到 final test 的流程图；
3. HBM test time、known-good-die、advanced packaging inspection 的价值量变化；
4. 按公司列财报指标；
5. 哪些只是泛半导体测试 beta；
6. 反证指标。
```

## 会话 4：800G / 1.6T / CPO / Silicon Photonics

```text
请研究 AI cluster 是否让光互连从通信周期转为数据中心算力周期，并拆解 800G、1.6T、3.2T、CPO、LPO/LRO、silicon photonics、InP laser、external light source。

重点公司：
Coherent、Lumentum、Broadcom、Marvell、Credo、MACOM、Semtech、InnoLight、中际旭创、Eoptolink、新易盛、Fabrinet、Sivers、IQE、Tower、GlobalFoundries、Ayar Labs、POET、Corning。

输出：
1. 原始出处清单；
2. 光互连技术路线；
3. 各环节瓶颈和供应商；
4. 非美小盘候选；
5. CPO 延后或 pluggable 延寿的反证；
6. 财报验证指标。
```

## 会话 5：Scale-up Fabric / Custom ASIC

```text
请研究 hyperscaler 自研 ASIC、scale-up fabric、PCIe/CXL、NVLink、UALink、Ethernet/InfiniBand 如何改变 AI Infra 价值分配。

重点公司：
NVIDIA、Broadcom、Marvell、Astera Labs、Credo、Arista、Cisco、Rambus、Alphawave、Arm、AMD、Alchip、GUC、Faraday、Synopsys、Cadence、TSMC、ASE。

输出：
1. 原始出处清单；
2. Google TPU、AWS Trainium、Microsoft Maia、Meta MTIA 的供应链拆解；
3. GPU vs ASIC 价值转移；
4. rack-scale connectivity 价值链；
5. 财报验证指标；
6. 反证指标。
```

## 会话 6：NeoCloud 经济模型

```text
请建立 NeoCloud / GPU-as-a-Service 的单位经济模型，判断它是高增长云平台，还是 GPU 租赁 + 高杠杆基础设施周期。

重点公司：
CoreWeave、Nebius、Oracle、Lambda、Crusoe、IREN、Applied Digital、Hut 8、TeraWulf、Core Scientific、DigitalOcean。

输出：
1. 原始出处清单；
2. 每 MW、每 GPU、每 rack 的收入、成本、折旧、融资模型；
3. backlog 到 revenue 到 FCF 的转化路径；
4. GPU residual value 风险；
5. 客户集中和违约风险；
6. 反证指标。
```

## 会话 7：电力设备 / 液冷 / 热管理

```text
请研究 AI 数据中心瓶颈是否会从 GPU 转到电力设备、变压器、UPS、PDU、switchgear、liquid cooling、CDU、cold plate、power module。

重点公司：
Vertiv、Schneider、Eaton、Siemens Energy、ABB/Hitachi Energy、Delta、Lite-On、AcBel、Vicor、MPS、Infineon、Fuji Electric、Mitsubishi Electric、Nidec、Munters、Alfa Laval、Modine、CoolIT、Boyd。

输出：
1. 原始出处清单；
2. 从 grid interconnect 到 rack power/cooling 的产业链地图；
3. backlog、book-to-bill、lead time、gross margin 的验证方式；
4. 哪些环节是真瓶颈，哪些是主题映射；
5. 反证指标。
```

## 会话 8：非美材料 / 设备隐形冠军

```text
请研究日本、韩国、台湾、欧洲、以色列 AI Infra 隐形供应链，重点找不是 AI 公司但被 AI 规格升级拉动的材料、设备、测试、真空、洁净、封装、基板公司。

重点方向：
- 日本：TOWA、Disco、Advantest、Lasertec、TEL、Screen、Ebara、Ibiden、Shinko、Ajinomoto、Resonac、Shin-Etsu、SUMCO、Organo、Kurita、Ferrotec
- 韩国：Hanmi、Leeno、ISC、TSE、Soulbrain、Dongjin、SK Materials、Wonik、Nextin
- 台湾：Unimicron、Nan Ya PCB、Kinsus、Chroma、MPI、WinWay、Delta、Lite-On、Alchip、GUC、Phison、Silicon Motion
- 欧洲：BESI、SUSS、VAT、Soitec、IQE、Aixtron、Sivers、Infineon、Schneider、Siemens Energy、Munters、Alfa Laval
- 以色列：Camtek、Nova、Tower

输出：
1. 原始出处清单；
2. 按国家/地区分产业链位置；
3. AI 相关收入证据；
4. 财报指标；
5. 10 倍弹性可能性；
6. 反证指标。
```

## 会话 9：存储超级周期反证

```text
请判断存储超级周期是否是 AI Infra 指数增长扩散的开始，并严格区分：
1. 真实 AI 拉动；
2. 传统存储周期反转；
3. AI 叙事映射。

拆分：
HBM、server DRAM、commodity DRAM、enterprise SSD、NAND、SSD controller、CXL memory、HDD/object storage。

输出：
1. 原始出处清单；
2. 每个分支的 AI 真实性；
3. 财报验证指标；
4. 哪些公司最容易被误判；
5. 未来 4 个季度反证仪表盘。
```

## 会话 10：AI Infra 反证仪表盘

```text
请为 AI Infra super cycle 建立反证仪表盘。目标是判断周期何时从结构性基建扩张转向资本开支过度、泡沫或普通周期。

覆盖：
- hyperscaler CapEx / cloud margin / RPO conversion
- NeoCloud utilization / debt / backlog quality
- NVIDIA / GPU supply-demand
- HBM / DRAM / NAND pricing and capacity
- optical 800G/1.6T shipments and ASP
- power equipment backlog and lead time
- data center interconnection and permitting
- token price decline / tokens per watt / model efficiency

输出：
1. 领先指标；
2. 滞后指标；
3. 红线阈值；
4. 每季度更新表；
5. 对应原始出处。
```

## 会话 11：LLM Dependency BFS 结论建模

```text
请基于 D0-D5 LLM Dependency BFS 框架，生成一套 AI Infra 研究结论建模体系。

D0：LLM 核心源头，包括 OpenAI、Anthropic、Google DeepMind / Gemini、Meta、xAI。
D1：模型强依赖一阶，包括 NVIDIA GPU/CUDA、Google TPU、AMD GPU、Azure/AWS/GCP/Oracle/CoreWeave、训练/推理软件栈。
D2：一阶依赖的直接瓶颈，包括 HBM、CoWoS、TSMC leading-edge、AI server/rack、networking、800G/1.6T optics、data center power/cooling。
D3：二阶瓶颈供应商，包括 HBM test/equipment、ABF substrate、TCB/hybrid bonding、InP laser、SiPh、EDA/IP、retimer、液冷组件、电力设备关键部件。
D4-D5：深层雷达，包括材料、气体、化学品、真空、洁净、能源、电网、融资、监管；只做雷达，除非能证明反向卡住 D0-D2。

研究重点：主看 D1-D3，最深看到 D5。A 股映射也必须标注 BFS 深度，优先看 D1-D3，超过 D3 的公司只能进观察池，除非有强原文证据证明其反向卡住 D0-D2。

请输出：
1. company/module card 数据模型；
2. BFS 边关系模型：客户边、BOM 边、产能边、技术边、现金流边、反证边；
3. 结论分层模型：核心池、候选池、雷达池、排除池；
4. 评分模型：BFS depth、需求真实性、供给瓶颈、议价权、FCF 传导、技术替代、客户集中、证据质量、反证、10 倍弹性；
5. 标准结论句模板；
6. A 股主板映射规则；
7. 从 D0 到 D5 的执行流程；
8. 可复制到 markdown 的总图表格、公司卡片、证据卡片、A 股映射表、季度复盘表、排除池模板。

请严格区分原文已证明、合理推论、待原文核验、主要反证。不输出买卖建议，不给目标价。
```

## 后置会话：A 股主板映射

这个会话不要提前启动。必须等美股、日韩、欧洲 AI Infra 标的调研完成后，再基于已经形成的海外瓶颈链条做映射。

```text
请基于本项目已经完成的美股、日韩、欧洲 AI Infra 标的研究，把海外已验证的 AI Infra 瓶颈环节映射到非科创板、非创业板 A 股主板公司。

严格要求：
1. 不要从 A 股概念出发；
2. 先列海外已验证的瓶颈环节和代表公司；
3. 再找 A 股是否存在同环节、同客户、同技术路线或同供需逻辑；
4. 排除 300 创业板和 688 科创板；
5. 对每家 A 股公司标注：对应海外环节、对应海外公司、需要核验的原始出处、AI 相关收入证据、主要反证；
6. 不给买卖建议，不给目标价。
```
